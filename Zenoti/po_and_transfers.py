import os
import sys
import subprocess
import uuid
import pandas as pd
import pyodbc
from db_helper import get_connection
from dotenv import load_dotenv
from datetime import datetime
import re

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# ==================================
# Read Config
# ==================================
SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")
TABLE = os.getenv("TABLE_PO_AND_TRANSFERS")
CSV_FILE = os.getenv("CSV_FILE_PO_AND_TRANSFERS")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# ==================================
# Upsert Toggle
# ==================================
# OFF (default): every CSV row is appended, so re-running a file duplicates rows.
# ON: each row is matched on ref_no first -> UPDATE if it exists, INSERT if it does not.
# Set with UPSERT_PO_AND_TRANSFERS=on in the .env, or override per run with the
# --upsert / --no-upsert command line flag.
def _truthy(value):
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")

UPSERT = _truthy(os.getenv("UPSERT_PO_AND_TRANSFERS", "off"))
if "--upsert" in sys.argv:
    UPSERT = True
if "--no-upsert" in sys.argv:
    UPSERT = False

# ==================================
# API Chain Toggle
# ==================================
# After the CSV is loaded, optionally run purchase_order.py and transfer_order.py
# to refresh the detailed line-item tables straight from the Zenoti API, for the
# same date range the CSV covers. OFF by default so normal runs are unchanged.
# Set RUN_API_AFTER_PO_AND_TRANSFERS=on in the .env, or override per run with the
# --run-api / --no-run-api command line flag.
RUN_API_AFTER = _truthy(os.getenv("RUN_API_AFTER_PO_AND_TRANSFERS", "off"))
if "--run-api" in sys.argv:
    RUN_API_AFTER = True
if "--no-run-api" in sys.argv:
    RUN_API_AFTER = False

# ==================================
# Prune Toggle (CSV = source of truth)
# ==================================
# ON: after loading, delete rows whose ref_no is in the table but NOT in the CSV,
# limited to the CSV's own "Order on" date window so other periods are untouched.
# OFF by default. Set PRUNE_MISSING_PO_AND_TRANSFERS=on in the .env, or override
# per run with --prune / --no-prune. --prune-dry-run previews and deletes nothing.
PRUNE = _truthy(os.getenv("PRUNE_MISSING_PO_AND_TRANSFERS", "off"))
if "--prune" in sys.argv:
    PRUNE = True
if "--no-prune" in sys.argv:
    PRUNE = False
PRUNE_DRY_RUN = "--prune-dry-run" in sys.argv
if PRUNE_DRY_RUN:
    PRUNE = True            # dry-run implies running the prune step (preview only)

CSV_SOURCE = os.getenv("CSV_SOURCE", "local").lower()
if CSV_SOURCE == "gdrive":
    from gdrive_helper import get_csv_from_gdrive
    CSV_FILE = get_csv_from_gdrive(
        os.getenv("GDRIVE_FOLDER_PO_AND_TRANSFERS"),
        credentials_json=os.getenv("GDRIVE_CREDENTIALS_JSON"),
        credentials_file=os.getenv("GDRIVE_CREDENTIALS_FILE", "service_account.json"),
    )

# get_csv_from_gdrive returns None when the Drive folder holds no CSVs. Stop here
# with a clean exit so an empty folder is a no-op, not a failure: nothing is
# loaded, no DB connection is opened, and the API refresh below does not run
# either, since its date window comes from the CSV.
if not CSV_FILE:
    print("SKIP: No CSV file available for po_and_transfers. Exiting.")
    exit(0)

if not all([SERVER, DATABASE, DB_USER, DB_PASSWORD]):
    missing = [k for k, v in {"SERVER": SERVER, "DATABASE": DATABASE, "DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD}.items() if not v]
    raise ValueError(f"Missing environment variables in .env file: {', '.join(missing)}")

if not TABLE:
    raise ValueError("Missing environment variable in .env file: TABLE_PO_AND_TRANSFERS")

# ==================================
# Open Connection (retries transient failures via db_helper)
# ==================================
conn = get_connection()
cursor = conn.cursor()
# ==================================
# Get SQL Columns
# ==================================
# Use TABLE schema if provided, otherwise default to dbo.
table_schema = 'dbo'
table_name = TABLE
if '.' in TABLE:
    parts = TABLE.split('.', 1)
    table_schema = parts[0].strip(' []')
    table_name = parts[1].strip(' []')

sql = f"""
SELECT c.COLUMN_NAME,
       COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY
FROM INFORMATION_SCHEMA.COLUMNS AS c
WHERE c.TABLE_NAME = '{table_name}'
  AND c.TABLE_SCHEMA = '{table_schema}'
ORDER BY c.ORDINAL_POSITION
"""

cursor.execute(sql)
rows = cursor.fetchall()
sql_columns = [row[0] for row in rows]
identity_columns = {row[0] for row in rows if row[1] == 1}

if not sql_columns:
    raise ValueError(f"Table not found or has no columns: [{table_schema}].[{table_name}]")

print(f"Found {len(sql_columns)} SQL columns (identity: {', '.join(identity_columns) if identity_columns else 'none'})")

# Helper to normalize column names for matching between CSV and SQL
def normalize_col(col_name):
    return col_name.strip().strip('.,').replace(" ", "_").replace("-", "_").lower()

# Clean a CSV money/qty string ("1,350.52", "$82.50", "(45.00)") into a float.
def clean_number(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    negative = text.startswith('(') and text.endswith(')')
    text = re.sub(r'[^0-9.\-]', '', text)
    if text in ('', '-', '.', '-.'):
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return -abs(number) if negative else number

# Lookup from normalized SQL name -> actual SQL column name
sql_column_lookup = {normalize_col(c): c for c in sql_columns}

# ==================================
# Load CSV(s) and process one file at a time
# ==================================
# If CSV_FILE points to a directory, gather all .csv files in that directory.
if not CSV_FILE:
    raise ValueError("CSV_FILE_PO_AND_TRANSFERS must be set in the .env and point to a file or directory")

csv_paths = []
if os.path.isdir(CSV_FILE):
    for f in sorted(os.listdir(CSV_FILE)):
        if f.lower().endswith('.csv'):
            csv_paths.append(os.path.join(CSV_FILE, f))
elif os.path.isfile(CSV_FILE):
    csv_paths = [CSV_FILE]
else:
    raise ValueError(f"CSV_FILE path does not exist: {CSV_FILE}")

if not csv_paths:
    print(f"No CSV files found in: {CSV_FILE}")

# Precompute insert columns (exclude identity columns)
insert_columns = [c for c in sql_columns if c not in identity_columns]
table_qualified = f"[{table_schema}].[{table_name}]"
insert_sql = f"""
INSERT INTO {table_qualified}
({','.join(f'[{c}]' for c in insert_columns)})
VALUES
({','.join(['?'] * len(insert_columns))})
"""

# ==================================
# Upsert statements (only used when UPSERT is on)
# ==================================
# ref_no is the business key: it is unique and never blank in the source CSVs,
# so a single UPDATE ... WHERE ref_no = ? touches exactly one row.
KEY_COLUMN = sql_column_lookup.get("ref_no")
update_columns = []
update_sql = None
select_sql = None

if UPSERT:
    if not KEY_COLUMN:
        raise ValueError(f"Upsert mode needs a ref_no column in {table_qualified}")
    update_columns = [c for c in insert_columns if c != KEY_COLUMN]
    update_sql = f"""
UPDATE {table_qualified}
SET {','.join(f'[{c}] = ?' for c in update_columns)}
WHERE [{KEY_COLUMN}] = ?
"""
    select_sql = f"SELECT 1 FROM {table_qualified} WHERE [{KEY_COLUMN}] = ?"

print(f"Mode: {'UPSERT (match on ref_no)' if UPSERT else 'INSERT only (append)'}")
if PRUNE:
    print(f"Prune : {'DRY RUN (preview only)' if PRUNE_DRY_RUN else 'ON (delete CSV-window orphans)'}")

# Track the span of raised dates ("Order on") across every CSV processed, so the
# optional API refresh below can pull exactly the same period.
order_on_min = None
order_on_max = None

# Every ref_no seen across the CSV(s) - the "source of truth" set for pruning.
csv_ref_nos = set()

# Prepare cursor for fast inserts
cursor.fast_executemany = True
for csv_path in csv_paths:
    print(f"Processing CSV: {csv_path}")
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    print(f"Found {len(df):,} rows in {os.path.basename(csv_path)}")

    # ==================================
    # Align CSV Columns with SQL Headers (per-file)
    # ==================================
    MANUAL_MAP = {
        "REF #": "ref_no",
        "Order On": "order_on",
        "Deliver On": "deliver_on",
        "To": "order_to",
        "From": "order_from",
        "Status": "order_status",
        "Qty": "order_qty",
        "Value": "order_value",
        "Tax": "order_tax",
        "Ponotes": "order_ponotes",
        "Wait Time": "wait_time",
    }

    mapping = {}
    for csv_col in df.columns:
        # 1. Check for manual override first
        if csv_col in MANUAL_MAP:
            target = MANUAL_MAP[csv_col]
            mapping[csv_col] = sql_column_lookup.get(normalize_col(target), target)
            continue

        # 2. Otherwise, normalize CSV header for pattern matching
        norm_csv = normalize_col(csv_col)
        if norm_csv in sql_column_lookup:
            mapping[csv_col] = sql_column_lookup[norm_csv]

    # Apply the final column renames before SQL alignment
    df.rename(columns=mapping, inplace=True)

    # ==================================
    # Add Metadata Columns (per-file)
    # ==================================
    sql_cols_norm = [normalize_col(c) for c in sql_columns]
    now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    if "ingestion_timestamp" in sql_cols_norm:
        col_name = sql_columns[sql_cols_norm.index("ingestion_timestamp")]
        df[col_name] = now_ts

    if "date_inserted" in sql_cols_norm:
        col_name = sql_columns[sql_cols_norm.index("date_inserted")]
        df[col_name] = now_ts

    if "source_file" in sql_cols_norm:
        col_name = sql_columns[sql_cols_norm.index("source_file")]
        df[col_name] = os.path.basename(csv_path)

    if "row_index" in sql_cols_norm:
        col_name = sql_columns[sql_cols_norm.index("row_index")]
        df[col_name] = range(1, len(df) + 1)

    # ==================================
    # Create Missing Columns
    # ==================================
    for col in sql_columns:
        if col not in df.columns:
            df[col] = None

    # If row_id is required and not provided in CSV, generate GUIDs for missing values
    if 'row_id' in sql_cols_norm:
        real_row_id_col = sql_columns[sql_cols_norm.index('row_id')]
        if real_row_id_col not in df.columns:
            df[real_row_id_col] = None
        df[real_row_id_col] = df[real_row_id_col].where(df[real_row_id_col].notnull(), None)
        df[real_row_id_col] = df[real_row_id_col].apply(lambda x: str(uuid.uuid4()) if x is None else x)

    # Keep only columns that exist in SQL and do not include identities in the insert
    df = df[insert_columns]

    # Convert blanks to NULL
    df = df.replace("", None)

    # Remember every ref_no seen, for the optional prune step below.
    if KEY_COLUMN and KEY_COLUMN in df.columns:
        csv_ref_nos.update(
            str(v).strip() for v in df[KEY_COLUMN].tolist()
            if v is not None and str(v).strip()
        )

    # ==================================
    # Date/Time Type Conversion
    # ==================================
    # The CSV mixes formats ("01/02/2026 17:25" and "1/21/2026 11:17 AM"), so parse
    # per-value instead of letting pandas infer one format for the whole column.
    po_date_columns = {"order_on", "deliver_on"}
    for col in df.columns:
        if normalize_col(col) in po_date_columns:
            parsed = pd.to_datetime(df[col], errors='coerce', format='mixed')
            bad = df[col].notnull() & parsed.isnull()
            if bad.any():
                print(f"  Warning: {bad.sum()} unparseable value(s) in {col}, kept as NULL")
            # Remember the earliest / latest raised date for the optional API pull.
            if normalize_col(col) == "order_on":
                valid = parsed.dropna()
                if not valid.empty:
                    lo, hi = valid.min(), valid.max()
                    if order_on_min is None or lo < order_on_min:
                        order_on_min = lo
                    if order_on_max is None or hi > order_on_max:
                        order_on_max = hi
            df[col] = parsed.dt.strftime('%Y-%m-%d %H:%M:%S')
            df[col] = df[col].where(df[col].notnull(), None)

    # ==================================
    # Numeric Type Conversion
    # ==================================
    # Qty is int; Value/Tax are decimal(18,2). Strings arrive with thousands
    # separators, so strip them before casting.
    integer_columns = {"order_qty"}
    decimal_columns = {"order_value", "order_tax"}
    for col in df.columns:
        norm = normalize_col(col)
        if norm in integer_columns:
            df[col] = df[col].map(clean_number).round().astype('Int64')
        elif norm in decimal_columns:
            df[col] = df[col].map(clean_number).round(2).astype('Float64')
        else:
            df[col] = df[col].where(df[col].notnull(), None)

    # ==================================
    # Insert / Upsert Data for this CSV
    # ==================================
    data_to_insert = df.astype(object).where(df.notnull(), None).values.tolist()

    if UPSERT:
        key_idx = insert_columns.index(KEY_COLUMN)
        update_order = [insert_columns.index(c) for c in update_columns]
        inserted = updated = skipped = 0
        failed = False

        # One statement at a time: each row needs its own existence check.
        cursor.fast_executemany = False
        for i, row in enumerate(data_to_insert):
            key_value = row[key_idx]
            if key_value is None or str(key_value).strip() == "":
                skipped += 1
                print(f"  Warning: row {i + 2} has a blank ref_no, skipped")
                continue
            try:
                cursor.execute(select_sql, key_value)
                exists = cursor.fetchone() is not None
                if exists:
                    cursor.execute(update_sql, [row[j] for j in update_order] + [key_value])
                    updated += 1
                else:
                    cursor.execute(insert_sql, row)
                    inserted += 1
            except (pyodbc.DataError, pyodbc.ProgrammingError) as row_e:
                print(f"--- Error found in CSV {os.path.basename(csv_path)} row {i + 2} (ref_no {key_value}) ---")
                print(f"Data: {dict(zip(insert_columns, row))}")
                print(f"Error Details: {row_e}")
                failed = True
                break

        if failed:
            conn.rollback()
            print(f"Rolled back {os.path.basename(csv_path)}; no changes were saved.")
        else:
            conn.commit()
            print(
                f"Upserted {os.path.basename(csv_path)}: "
                f"{inserted:,} inserted, {updated:,} updated"
                + (f", {skipped:,} skipped" if skipped else "")
                + "."
            )
        cursor.fast_executemany = True
        continue

    try:
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()
        print(f"Inserted {len(df):,} rows successfully from {os.path.basename(csv_path)}.")
    except (pyodbc.DataError, pyodbc.ProgrammingError) as e:
        print(f"Data insertion failed for {os.path.basename(csv_path)}: {e}")
        conn.rollback()

        # Debugging: Loop through rows to find the exact record causing the overflow
        print("Searching for the problematic row...")
        cursor.fast_executemany = False
        for i, row in enumerate(data_to_insert):
            try:
                cursor.execute(insert_sql, row)
            except (pyodbc.DataError, pyodbc.ProgrammingError) as row_e:
                print(f"--- Error found in CSV {os.path.basename(csv_path)} row {i + 2} ---")
                if "Parameter" in str(row_e):
                    match = re.search(r"Parameter (\d+)", str(row_e))
                    if match:
                        param_idx = int(match.group(1)) - 1
                        col_name = insert_columns[param_idx]
                        print(f"Problematic Column: {col_name} (Index {param_idx})")
                print(f"Data: {dict(zip(insert_columns, row))}")
                print(f"Error Details: {row_e}")
                break
        conn.rollback()
        cursor.fast_executemany = True

# ==================================
# Optional: prune orders missing from the CSV (CSV = source of truth)
# ==================================
# Delete rows whose ref_no is in the table but NOT in the CSV, limited to the
# CSV's own "Order on" date window. Orphans are computed in Python so the exact
# list can be printed; the DELETE is scoped by BOTH ref_no and the date window,
# so no row outside the CSV's period can ever be removed.
ORDER_ON_COLUMN = sql_column_lookup.get("order_on")
if PRUNE:
    if not KEY_COLUMN:
        print("Prune skipped: table has no ref_no column.")
    elif not ORDER_ON_COLUMN:
        print("Prune skipped: table has no order_on column to scope the window.")
    elif order_on_min is None or order_on_max is None:
        print("Prune skipped: no usable 'Order on' dates found in the CSV(s).")
    elif not csv_ref_nos:
        print("Prune skipped: no ref_no values found in the CSV(s).")
    else:
        start_date, end_date = order_on_min.date(), order_on_max.date()
        cursor.execute(
            f"SELECT DISTINCT [{KEY_COLUMN}] FROM {table_qualified} "
            f"WHERE CAST([{ORDER_ON_COLUMN}] AS date) BETWEEN ? AND ?",
            start_date, end_date)
        db_refs = {str(r[0]).strip() for r in cursor.fetchall() if r[0] is not None}
        orphans = sorted(db_refs - csv_ref_nos)
        print(f"\nPrune scope {start_date}..{end_date}: {len(db_refs)} in SQL, "
              f"{len(csv_ref_nos)} in CSV, {len(orphans)} orphan(s).")
        if orphans:
            print("  ref_no(s): " + ", ".join(orphans[:20])
                  + ("..." if len(orphans) > 20 else ""))
        if not orphans:
            print("  Nothing to prune.")
        elif PRUNE_DRY_RUN:
            print("  --prune-dry-run: nothing deleted.")
        else:
            try:
                deleted = 0
                for i in range(0, len(orphans), 500):
                    chunk = orphans[i:i + 500]
                    ph = ",".join(["?"] * len(chunk))
                    cursor.execute(
                        f"DELETE FROM {table_qualified} WHERE [{KEY_COLUMN}] IN ({ph}) "
                        f"AND CAST([{ORDER_ON_COLUMN}] AS date) BETWEEN ? AND ?",
                        *chunk, start_date, end_date)
                    deleted += cursor.rowcount
                conn.commit()
                print(f"  Pruned {deleted:,} row(s) for {len(orphans)} orphan order(s).")
            except pyodbc.Error as e:
                conn.rollback()
                print(f"  Prune failed, rolled back: {e}")

# Close DB resources
cursor.close()
conn.close()


# ==================================
# Optional: refresh the API line-item tables for the same period
# ==================================
# When RUN_API_AFTER_PO_AND_TRANSFERS is on (or --run-api is passed), run
# purchase_order.py then transfer_order.py for the date range the CSV covered,
# writing with --replace so re-running the same range does not duplicate rows.
# --date-criteria 1 (raised) is pinned on purpose: the window above is derived
# from the raised "Order on" dates, so the API must filter on the raised date
# too for the pull to line up with the CSV listing.
# NOTE: both child scripts hit the live Zenoti API and write to the SAME
# production database this script just used.
if RUN_API_AFTER:
    if order_on_min is None or order_on_max is None:
        print("\nAPI refresh skipped: no usable 'Order on' dates found in the CSV(s).")
    else:
        start_arg = order_on_min.date().isoformat()
        end_arg = order_on_max.date().isoformat()
        script_dir = os.path.dirname(os.path.abspath(__file__))
        common = ["--start-date", start_arg, "--end-date", end_arg,
                  "--all-centers", "--replace", "--date-criteria", "1"]
        scripts = ["purchase_order.py", "transfer_order.py"]
        print(f"\nRefreshing Zenoti API data for {start_arg} to {end_arg} "
              "(--all-centers --replace, raised date)...")
        for name in scripts:
            print(f"\n=== Running {name} ===", flush=True)
            result = subprocess.run([sys.executable, os.path.join(script_dir, name)] + common)
            if result.returncode != 0:
                print(f"!! {name} exited with code {result.returncode}; "
                      "see its output above.")
        print("\nAPI refresh complete.")
