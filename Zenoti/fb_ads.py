import os
import uuid
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from datetime import datetime
import re


def log(step, msg=""):
    print(f"  ● {step:<10} {msg}", flush=True)

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# ==================================
# Read Config
# ==================================
SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")
TABLE = os.getenv("TABLE_FB_ADS")
CSV_FILE = os.getenv("CSV_FILE_FB_ADS")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

CSV_SOURCE = os.getenv("CSV_SOURCE", "local").lower()
if CSV_SOURCE == "gdrive":
    from gdrive_helper import get_csv_from_gdrive
    CSV_FILE = get_csv_from_gdrive(
        os.getenv("GDRIVE_FOLDER_FB_ADS"),
        credentials_json=os.getenv("GDRIVE_CREDENTIALS_JSON"),
        credentials_file=os.getenv("GDRIVE_CREDENTIALS_FILE", "service_account.json"),
    )

if not CSV_FILE:
    print("SKIP: No CSV file available for fb_ads. Exiting.")
    exit(0)

if not all([SERVER, DATABASE, DB_USER, DB_PASSWORD]):
    missing = [k for k, v in {"SERVER": SERVER, "DATABASE": DATABASE, "DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD}.items() if not v]
    raise ValueError(f"Missing environment variables in .env file: {', '.join(missing)}")

# ==================================
# Build Connection String
# ==================================
conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    # f"DRIVER={{SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
)


def ensure_connection(conn_str, timeout=5):
    """Attempt to open a new DB connection and verify it by running a lightweight query.

    Returns a tuple (conn, cursor) on success or raises ConnectionError on failure.
    """
    try:
        new_conn = pyodbc.connect(conn_str, timeout=timeout)
        new_cursor = new_conn.cursor()
        new_cursor.execute("SELECT 1")
        new_cursor.fetchone()
        return new_conn, new_cursor
    except Exception as e:
        raise ConnectionError(f"Unable to connect to database: {e}")


def check_and_reconnect(conn, conn_str, timeout=5):
    """Verify existing `conn` is alive; reconnect using `conn_str` if not.

    Returns (conn, cursor).
    """
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        return conn, cur
    except Exception:
        print("Lost DB connection — attempting to reconnect...")
        return ensure_connection(conn_str, timeout=timeout)


# establish initial connection
conn, cursor = ensure_connection(conn_str)
log("Connect", f"{DATABASE} on {SERVER}")

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

log("Schema", f"{len(sql_columns)} columns (identity: {', '.join(identity_columns) if identity_columns else 'none'})")

# Helper to normalize column names for matching between CSV and SQL
def normalize_col(col_name):
    return col_name.strip().strip('.,').replace(" ", "_").replace("-", "_").lower()

# Lookup from normalized SQL name -> actual SQL column name
sql_column_lookup = {normalize_col(c): c for c in sql_columns}

# ==================================
# Helper: Extract date from filename
# ==================================
def extract_date_from_filename(filename):
    match = re.match(r'^FBAds_(\d{4}-\d{2}-\d{2})\.csv$', filename, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

# ==================================
# Load CSV(s) and process one file at a time
# ==================================
if not CSV_FILE:
    raise ValueError("CSV_FILE_FB_ADS must be set in the .env and point to a file or directory")

csv_paths = []
if os.path.isdir(CSV_FILE):
    for f in sorted(os.listdir(CSV_FILE)):
        if f.lower().endswith('.csv'):
            if extract_date_from_filename(f):
                csv_paths.append(os.path.join(CSV_FILE, f))
            else:
                print(f"SKIPPED: '{f}' — filename must follow format FBAds_<YYYY-MM-DD>.csv")
elif os.path.isfile(CSV_FILE):
    fname = os.path.basename(CSV_FILE)
    if extract_date_from_filename(fname):
        csv_paths = [CSV_FILE]
    else:
        raise ValueError(f"Invalid filename: '{fname}' — filename must follow format FBAds_<YYYY-MM-DD>.csv")
else:
    raise ValueError(f"CSV_FILE_FB_ADS path does not exist: {CSV_FILE}")

if not csv_paths:
    print(f"No CSV files found in: {CSV_FILE}")

insert_columns = [c for c in sql_columns if c not in identity_columns]
table_qualified = f"[{table_schema}].[{table_name}]"

MATCH_KEY_NAMES = ["report_date", "campaign_id", "ad_group_name", "ad_name"]
match_key_cols = []
for key_name in MATCH_KEY_NAMES:
    sql_col = sql_column_lookup.get(key_name)
    if not sql_col:
        raise ValueError(f"Could not find '{key_name}' column in the SQL table. Cannot proceed with upsert logic.")
    match_key_cols.append(sql_col)

insert_sql = f"""
INSERT INTO {table_qualified}
({','.join(f'[{c}]' for c in insert_columns)})
VALUES
({','.join(['?'] * len(insert_columns))})
"""

for csv_path in csv_paths:
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    log("Load", f"{os.path.basename(csv_path)} → {len(df):,} rows")

    # ==================================
    # Align CSV Columns with SQL Headers (per-file)
    # ==================================
    MANUAL_MAP = {
        "Account: Account name": "account_name",
        "Campaign: Campaign Id": "campaign_id",
        "Campaign: Campaign name": "campaign_name",
        "Ad group: Ad group name": "ad_group_name",
        "Ad: Ad name": "ad_name",
        "Report: Date": "report_date",
        "Cost: Amount spend": "amount_spend",
        "Performance: Clicks": "clicks",
        "Cost: CPC": "cpc",
        "Clicks: CTR": "ctr",
        "Performance: Impressions": "impressions",
        "Performance: Reach": "reach",
    }

    mapping = {}
    for csv_col in df.columns:
        if csv_col in MANUAL_MAP:
            target = MANUAL_MAP[csv_col]
            mapping[csv_col] = sql_column_lookup.get(normalize_col(target), target)
            continue

        norm_csv = normalize_col(csv_col)
        if norm_csv in sql_column_lookup:
            mapping[csv_col] = sql_column_lookup[norm_csv]

    df.rename(columns=mapping, inplace=True)

    # ==================================
    # Add Metadata Columns (per-file)
    # ==================================
    sql_cols_norm = [normalize_col(c) for c in sql_columns]
    now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    # if "ingestion_timestamp" in sql_cols_norm:
    #     col_name = sql_columns[sql_cols_norm.index("ingestion_timestamp")]
    #     df[col_name] = now_ts

    if "date_inserted" in sql_cols_norm:
        col_name = sql_columns[sql_cols_norm.index("date_inserted")]
        df[col_name] = now_ts

    # if "source_file" in sql_cols_norm:
    #     col_name = sql_columns[sql_cols_norm.index("source_file")]
    #     df[col_name] = os.path.basename(csv_path)

    if "id" in sql_cols_norm:
        col_name = sql_columns[sql_cols_norm.index("id")]
        df[col_name] = range(1, len(df) + 1)

    for col in sql_columns:
        if col not in df.columns:
            df[col] = None

    if 'row_id' in [normalize_col(c) for c in sql_columns]:
        real_row_id_col = sql_columns[[normalize_col(c) for c in sql_columns].index('row_id')]
        if real_row_id_col not in df.columns:
            df[real_row_id_col] = None
        df[real_row_id_col] = df[real_row_id_col].where(df[real_row_id_col].notnull(), None)
        df[real_row_id_col] = df[real_row_id_col].apply(lambda x: str(uuid.uuid4()) if x is None else x)

    df = df[insert_columns]
    df = df.replace("", None)

    fb_ads_date_columns = {"report_date"}
    for col in df.columns:
        if normalize_col(col) in fb_ads_date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y/%m/%d')

    for col in [c for c in df.columns if normalize_col(c) != "ingestion_timestamp"]:
        test_numeric = pd.to_numeric(df[col], errors='coerce')
        if test_numeric.notnull().sum() == df[col].notnull().sum() and df[col].notnull().sum() > 0:
            if (test_numeric.dropna() % 1 == 0).all():
                df[col] = test_numeric.round().astype('Int64')
            else:
                df[col] = test_numeric
        else:
            df[col] = df[col].where(df[col].notnull(), None)

    # Ensure DB connection is alive before upsert
    conn, cursor = check_and_reconnect(conn, conn_str)

    # Fetch existing key combos from SQL
    key_select = ', '.join(f'[{c}]' for c in match_key_cols)
    cursor.execute(f"SELECT DISTINCT {key_select} FROM {table_qualified}")

    def _normalize_key(val):
        if val is None:
            return None
        if hasattr(val, 'strftime'):
            return val.strftime('%Y/%m/%d')
        s = str(val)
        if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
            return s.replace('-', '/')
        if '.' in s:
            try:
                f = float(s)
                if f == int(f):
                    return str(int(f))
            except (ValueError, OverflowError):
                pass
        return s

    existing_keys = {tuple(_normalize_key(v) for v in row) for row in cursor.fetchall()}

    total = len(df)

    # Filter out rows whose match keys already exist in DB
    key_col_indices = [insert_columns.index(c) for c in match_key_cols]
    all_rows = df.astype(object).where(df.notnull(), None).values.tolist()

    def _make_csv_key(row):
        return tuple(_normalize_key(row[i]) for i in key_col_indices)

    data_to_insert = [row for row in all_rows if _make_csv_key(row) not in existing_keys]
    skipped = total - len(data_to_insert)

    if not data_to_insert:
        log("Insert", f"Skipped: {skipped:,} | Inserted: 0")
        log("Failed", "0 rows")
        continue

    # Batch insert new rows only
    insert_count = len(data_to_insert)
    cursor.fast_executemany = True
    batch_size = 1000
    for start in range(0, insert_count, batch_size):
        end = min(start + batch_size, insert_count)
        batch = data_to_insert[start:end]
        try:
            cursor.executemany(insert_sql, batch)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    log("Insert", f"Skipped: {skipped:,} | Inserted: {insert_count:,}")
    log("Failed", "0 rows")

cursor.close()
conn.close()
log("Done")
