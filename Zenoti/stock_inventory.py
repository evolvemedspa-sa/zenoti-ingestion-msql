import os
import uuid
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from datetime import datetime
from zoneinfo import ZoneInfo
import re
from decimal import Decimal, InvalidOperation

# All date_inserted / ingestion_timestamp values are stamped in New York local
# time (EST/EDT, handled automatically) regardless of where this script runs.
NY_TZ = ZoneInfo("America/New_York")

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# ==================================
# Read Config
# ==================================
SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")
TABLE = os.getenv("TABLE_STOCK_INVENTORY")
CSV_FILE = os.getenv("CSV_FILE_STOCK_INVENTORY")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

CSV_SOURCE = os.getenv("CSV_SOURCE", "local").lower()
if CSV_SOURCE == "gdrive":
    from gdrive_helper import get_csv_from_gdrive
    CSV_FILE = get_csv_from_gdrive(
        os.getenv("GDRIVE_FOLDER_STOCK_INVENTORY"),
        credentials_json=os.getenv("GDRIVE_CREDENTIALS_JSON"),
        credentials_file=os.getenv("GDRIVE_CREDENTIALS_FILE", "service_account.json"),
    )

if not CSV_FILE:
    print("SKIP: No CSV file available for stock_inventory. Exiting.")
    exit(0)

if not all([SERVER, DATABASE, DB_USER, DB_PASSWORD]):
    missing = [k for k, v in {"SERVER": SERVER, "DATABASE": DATABASE, "DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD}.items() if not v]
    raise ValueError(f"Missing environment variables in .env file: {', '.join(missing)}")

# ==================================
# Build Connection String
# ==================================
conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
)

conn = pyodbc.connect(conn_str)
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
       COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY,
       c.DATA_TYPE,
       c.CHARACTER_MAXIMUM_LENGTH,
       c.NUMERIC_PRECISION,
       c.NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS AS c
WHERE c.TABLE_NAME = '{table_name}'
  AND c.TABLE_SCHEMA = '{table_schema}'
ORDER BY c.ORDINAL_POSITION
"""

cursor.execute(sql)
rows = cursor.fetchall()
sql_columns = [row[0] for row in rows]
identity_columns = {row[0] for row in rows if row[1] == 1}

# Column definitions drive the parameter bindings further down, so the declared
# precision/scale is read from the table rather than assumed.
column_meta = {
    row[0]: {
        "data_type": (row[2] or "").lower(),
        "char_len": row[3],
        "precision": row[4],
        "scale": row[5],
    }
    for row in rows
}

print(f"Found {len(sql_columns)} SQL columns (identity: {', '.join(identity_columns) if identity_columns else 'none'})")

# Helper to normalize column names for matching between CSV and SQL
def normalize_col(col_name):
    return col_name.strip().strip('.,').replace(" ", "_").replace("-", "_").lower()

# ==================================
# Inventory Date From Filename
# ==================================
# The stock snapshot date is not a column in the export - it only exists in the
# file name, e.g. "Current Stock_2026-07-29.csv" -> 2026-07-29. That value feeds
# the date_inventory column so a snapshot can be identified by the day it was
# taken, independently of date_inserted (the day it was loaded).
#
# Accepted patterns, first match wins:
#   2026-07-29  /  2026_07_29  /  20260729
# Returns 'YYYY-MM-DD' or None when the file name carries no valid date.
FILENAME_DATE_PATTERNS = [
    r'(\d{4})[-_](\d{1,2})[-_](\d{1,2})',
    r'(\d{4})(\d{2})(\d{2})',
]

def date_from_filename(path):
    name = os.path.splitext(os.path.basename(path))[0]
    for pattern in FILENAME_DATE_PATTERNS:
        for match in re.finditer(pattern, name):
            year, month, day = match.groups()
            try:
                return datetime(int(year), int(month), int(day)).strftime('%Y-%m-%d')
            except ValueError:
                # Digits matched but are not a real calendar date (e.g. 2026-13-40);
                # keep scanning in case a valid date appears later in the name.
                continue
    return None

# Lookup from normalized SQL name -> actual SQL column name
sql_column_lookup = {normalize_col(c): c for c in sql_columns}

# ==================================
# Load source file(s) and process one at a time
# ==================================
# CSV_FILE may point to a single file or to a directory. Both .csv and Excel
# workbooks are accepted - see read_source_file() for why .xlsx is the safer
# format for this particular export.
SUPPORTED_EXTENSIONS = ('.csv', '.xlsx', '.xlsm')

# Maps the headers as they appear in the Zenoti export to the SQL column names.
# Defined here rather than inside the per-file loop because the header names are
# also what find_header_row() looks for when locating the real header row.
MANUAL_MAP = {
    "Center Name": "center_name",
    "Product Code": "product_code",
    "Product Name": "product_name",
    "UOM": "uom",
    "Product Type": "product_type",
    "Brand": "brand",
    "Vendor": "vendor",
    "On-Hand Quantity": "on_hand_quantity",
    "Stock Cost (Perpetual Average)": "stock_cost_perpetual_avg",
    "Business Unit": "business_unit",
    "Configured Purchase Price": "configured_purchase_price",
    "Avg Price (Perpetual)": "avg_price_perpetual"
}
EXPECTED_HEADERS = frozenset(MANUAL_MAP)

# The Excel version of this report carries title rows above the real header:
#
#   row 1  Evolve Med Spa
#   row 2  Current Stock   As on : 28 Jul 2026 ...   Report Export Date : ...
#   row 3  (blank)
#   row 4  Center Name | Product Code | Product Name | ...   <- the real header
#
# Reading with the default header=0 takes "Evolve Med Spa" as the header, so no
# column matches MANUAL_MAP and every value silently loads as NULL. Rather than
# hard-coding skiprows (the .csv export has no preamble), scan the top of the
# sheet for the row that actually looks like the header.
HEADER_SCAN_ROWS = 25
MIN_HEADER_MATCHES = 3

def find_header_row(raw):
    """Return the index of the row that looks most like the export header."""
    best_idx, best_hits = None, 0
    for i in range(min(HEADER_SCAN_ROWS, len(raw))):
        values = {str(v).strip() for v in raw.iloc[i].tolist()}
        hits = len(values & EXPECTED_HEADERS)
        if hits > best_hits:
            best_idx, best_hits = i, hits
    return best_idx if best_hits >= MIN_HEADER_MATCHES else None

def strip_summary_rows(df):
    """Drop blank rows and the trailing 'Total:' line the export appends."""
    if df.empty:
        return df
    blank = df.map(lambda v: str(v).strip() == '').all(axis=1)
    first_col = df.iloc[:, 0].astype(str).str.strip()
    total_row = first_col.str.match(r'(?i)^total\b')
    if df.shape[1] > 1:
        # A real center could be named "Total ..."; a summary line never has a
        # product code next to it, so require the second column to be empty.
        total_row &= df.iloc[:, 1].astype(str).str.strip() == ''
    return df[~(blank | total_row)].reset_index(drop=True)

def read_source_file(path):
    """Read a .csv or .xlsx/.xlsm file into a DataFrame of plain strings.

    Everything is read as text on purpose. product_code holds 12-digit numeric
    codes such as 111100000029; letting the reader treat those as numbers is
    what produces the 1.11E+11 corruption seen in earlier loads.
    """
    ext = os.path.splitext(path)[1].lower()
    if ext == '.csv':
        raw = pd.read_csv(path, dtype=str, keep_default_na=False, header=None)
    else:
        # openpyxl returns real Python types for numeric cells, so dtype=str
        # alone is not enough - a cell holding the number 111100000029 arrives
        # as a float and str() would render it '1.11100000029e+11'. Reading with
        # no dtype and converting afterwards keeps full precision for integers.
        raw = pd.read_excel(path, sheet_name=0, dtype=object, header=None, engine='openpyxl')
        raw = raw.map(excel_cell_to_text)

    header_row = find_header_row(raw)
    if header_row is None:
        raise ValueError(
            f"Could not find the header row in {os.path.basename(path)}.\n"
            f"Looked at the first {HEADER_SCAN_ROWS} rows for at least "
            f"{MIN_HEADER_MATCHES} of: {', '.join(sorted(EXPECTED_HEADERS))}"
        )
    if header_row > 0:
        print(f"  Header found on row {header_row + 1}; skipped {header_row} preamble row(s)")

    # Blank header cells become unnamed_N so duplicate '' labels cannot collide.
    columns = []
    for i, value in enumerate(raw.iloc[header_row].tolist()):
        name = str(value).strip()
        columns.append(name if name else f"unnamed_{i}")

    df = raw.iloc[header_row + 1:].copy()
    df.columns = columns
    return strip_summary_rows(df.reset_index(drop=True))

def excel_cell_to_text(value):
    """Render one Excel cell as the text a CSV export would have contained."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ''
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, float) and value.is_integer():
        # 111100000029.0 -> '111100000029', not '1.11100000029e+11'
        return str(int(value))
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    return str(value).strip()

if not CSV_FILE:
    raise ValueError("CSV_FILE must be set in the .env and point to a file or directory")

csv_paths = []
if os.path.isdir(CSV_FILE):
    for f in sorted(os.listdir(CSV_FILE)):
        # Skip Excel lock files (~$name.xlsx) left behind by an open workbook.
        if f.startswith('~$'):
            continue
        if f.lower().endswith(SUPPORTED_EXTENSIONS):
            csv_paths.append(os.path.join(CSV_FILE, f))
elif os.path.isfile(CSV_FILE):
    if not CSV_FILE.lower().endswith(SUPPORTED_EXTENSIONS):
        raise ValueError(
            f"Unsupported file type: {CSV_FILE}\n"
            f"Supported extensions: {', '.join(SUPPORTED_EXTENSIONS)}"
        )
    csv_paths = [CSV_FILE]
else:
    raise ValueError(f"CSV_FILE path does not exist: {CSV_FILE}")

if not csv_paths:
    print(f"No .csv or .xlsx files found in: {CSV_FILE}")

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
# Parameter Bindings
# ==================================
# fast_executemany binds one fixed-width buffer per column, so every parameter
# needs an explicit size. A size of 0 means "use the driver maximum" only for
# "ODBC Driver 17 for SQL Server"; the legacy "{SQL Server}" driver in conn_str
# rejects it outright with:
#   HY104 Invalid precision value (0) (SQLBindParameter)
# Sizes are therefore read from the table definition and are never 0.

# varchar(max)/nvarchar(max) report CHARACTER_MAXIMUM_LENGTH = -1. The legacy
# driver cannot bind an unbounded buffer, so those get a fixed width instead.
MAX_TEXT_BINDING = 4000
# Columns that are not character types (datetime, date, int, uniqueidentifier)
# report no length, but their values still arrive as strings for SQL Server to
# convert. 64 covers the widest rendered form ('YYYY-MM-DD HH:MM:SS.mmm' at 23,
# a GUID at 36).
NON_TEXT_BINDING = 64

def text_binding_size(col_name):
    """Buffer width for a text-bound parameter. Always >= 1, never 0."""
    meta = column_meta.get(col_name)
    if meta is None:
        return MAX_TEXT_BINDING
    char_len = meta["char_len"]
    if char_len is None:
        return NON_TEXT_BINDING
    if char_len < 0:
        return MAX_TEXT_BINDING
    return max(1, min(int(char_len), MAX_TEXT_BINDING))

def decimal_binding(col_name):
    """(type, precision, scale) for a decimal column, taken from the table."""
    meta = column_meta.get(col_name) or {}
    precision = meta.get("precision") or 38
    scale = meta.get("scale")
    return (pyodbc.SQL_DECIMAL, min(int(precision), 38), int(scale if scale is not None else 10))

def build_input_sizes(columns, decimal_cols):
    """Bind true decimal columns as SQL_DECIMAL and everything else as text.

    Mixed text/numeric columns such as configured_purchase_price are absent from
    decimal_cols on purpose, so they bind as varchar at their declared length and
    keep values like "N/A" instead of being nulled out.
    """
    sizes = []
    for col_name in columns:
        if col_name in decimal_cols:
            sizes.append(decimal_binding(col_name))
        else:
            sizes.append((pyodbc.SQL_WVARCHAR, text_binding_size(col_name)))
    return sizes

for csv_path in csv_paths:
    print(f"Processing: {csv_path}")
    df = read_source_file(csv_path)
    print(f"Found {len(df):,} rows in {os.path.basename(csv_path)}")

    # ==================================
    # Align CSV Columns with SQL Headers (per-file)
    # ==================================
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

    # Stop rather than load garbage. If nothing mapped, the "Create Missing
    # Columns" step below would happily build an all-NULL frame and the insert
    # would report success - which is exactly how a bad header row slipped
    # through before.
    if not mapping:
        raise ValueError(
            f"No columns in {os.path.basename(csv_path)} matched the expected export "
            f"headers, so every value would load as NULL.\n"
            f"Found: {', '.join(str(c) for c in df.columns)}"
        )

    # ==================================
    # Add Metadata Columns (per-file)
    # ==================================
    sql_cols_norm = [normalize_col(c) for c in sql_columns]
    now_ts = datetime.now(NY_TZ).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    if "ingestion_timestamp" in sql_cols_norm:
        col_name = sql_columns[sql_cols_norm.index("ingestion_timestamp")]
        df[col_name] = now_ts

    if "date_inserted" in sql_cols_norm:
        col_name = sql_columns[sql_cols_norm.index("date_inserted")]
        df[col_name] = now_ts

    # date_inventory = the snapshot date parsed out of the CSV file name.
    if "date_inventory" in sql_cols_norm:
        col_name = sql_columns[sql_cols_norm.index("date_inventory")]
        inventory_date = date_from_filename(csv_path)
        if inventory_date is None:
            print(
                f"  Warning: no date found in file name '{os.path.basename(csv_path)}'"
                f" - date_inventory will be NULL for these rows."
                f" Expected something like 'Current Stock_2026-07-29.csv'."
            )
        else:
            print(f"  date_inventory: {inventory_date} (from file name)")
        df[col_name] = inventory_date

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
    if 'row_id' in [normalize_col(c) for c in sql_columns]:
        real_row_id_col = sql_columns[[normalize_col(c) for c in sql_columns].index('row_id')]
        if real_row_id_col not in df.columns:
            df[real_row_id_col] = None
        df[real_row_id_col] = df[real_row_id_col].where(df[real_row_id_col].notnull(), None)
        df[real_row_id_col] = df[real_row_id_col].apply(lambda x: str(uuid.uuid4()) if x is None else x)

    # Keep only columns that exist in SQL and do not include identities in the insert
    df = df[insert_columns]

    # Convert blanks to NULL
    df = df.replace("", None)

    # ==================================
    # Date/Time Type Conversion
    # ==================================
    # date_inserted is not read from the CSV - it is stamped above from the New
    # York clock and is already in 'YYYY-MM-DD HH:MM:SS.mmm' form, which the
    # datetime column accepts as-is. Reformatting it here would drop the time,
    # so it is deliberately left out of this set. date_inventory is likewise
    # stamped above from the file name and is already 'YYYY-MM-DD', which the
    # date column accepts as-is. Add any genuine CSV date columns to
    # stock_inventory_date_columns if the export ever gains one.
    stock_inventory_date_columns = set()
    for col in df.columns:
        if normalize_col(col) in stock_inventory_date_columns:
            # Coerce invalid dates to NaT (Not a Time), which will become NULL
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')

    # ==================================
    # Numeric Type Conversion (true decimal columns only)
    # ==================================
    # This function will convert numeric strings to Decimal to preserve precision,
    # but will leave non-numeric strings (like "N/A") as they are.
    def to_decimal_if_numeric(value):
        if value is None or isinstance(value, (int, float, Decimal)):
            return value
        try:
            # Using Decimal preserves precision for values like "2.68"
            return Decimal(value)
        except (InvalidOperation, TypeError, ValueError):
            # If it's not a valid number (e.g., "N/A"), return it as is.
            return value

    # NOTE: configured_purchase_price is intentionally NOT in this list.
    # That column is varchar(200) in SQL Server and must be able to store
    # the literal text "N/A" as well as formatted numeric strings like "280.00".
    # Binding it as SQL_DECIMAL (see sql_type_map below) silently nulls out any
    # row containing "N/A", so it gets its own text-preserving handling instead.
    numeric_cols = [
        "on_hand_quantity",
        "stock_cost_perpetual_avg",
        "avg_price_perpetual",
    ]

    # Resolve normalized names to actual DataFrame column names
    df_cols_norm_map = {normalize_col(c): c for c in df.columns}
    cols_to_convert = [df_cols_norm_map[n] for n in numeric_cols if n in df_cols_norm_map]

    for col in cols_to_convert:
        df[col] = df[col].apply(to_decimal_if_numeric)

    # ==================================
    # Mixed Text/Numeric Columns (e.g. configured_purchase_price)
    # ==================================
    # These columns are stored as varchar in SQL and can legitimately contain
    # either a formatted number ("280.00") or a text marker like "N/A".
    # Numbers are formatted to 2 decimal places as strings; "N/A"/"NA"/blank
    # pass through as the literal text "N/A" rather than becoming NULL.
    def format_price_text(value):
        if value is None:
            return None
        s = str(value).strip()
        if s.upper() in ("N/A", "NA", ""):
            return "N/A"
        try:
            return f"{Decimal(s):.2f}"  # e.g. "280" -> "280.00"
        except (InvalidOperation, TypeError, ValueError):
            return s  # unrecognized text, keep as-is rather than guessing

    price_text_cols = ["configured_purchase_price"]
    price_text_cols_resolved = [df_cols_norm_map[normalize_col(c)] for c in price_text_cols if normalize_col(c) in df_cols_norm_map]

    for col in price_text_cols_resolved:
        df[col] = df[col].apply(format_price_text)

    # ==================================
    # Insert new data (append-only)
    # ==================================
    # Every run appends the full CSV as a new snapshot; nothing is deleted, so
    # the table keeps a history of stock levels distinguishable by date_inserted.
    data_to_insert = df.astype(object).where(df.notnull(), None).values.tolist()
    total_rows = len(data_to_insert)

    try:
        print(f"Inserting {total_rows:,} new rows...")
        cursor.fast_executemany = True
        cursor.setinputsizes(build_input_sizes(df.columns, cols_to_convert))

        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()
        print(f"Successfully inserted {total_rows:,} rows from {os.path.basename(csv_path)}.")

    except pyodbc.Error as e:
        # pyodbc.Error, not just DataError/ProgrammingError: binding failures
        # (HY104 and friends) surface as the base class and would otherwise
        # escape as an unhandled traceback with the transaction left open.
        print(f"Data insertion failed for {os.path.basename(csv_path)}: {e}")
        conn.rollback()

        # Fallback to row-by-row insert to find the problematic record.
        # setinputsizes() persists on the cursor, so it is cleared first - those
        # bindings are themselves a candidate cause of the failure, and leaving
        # them in place would break every row in this loop too.
        print("Searching for the problematic row...")
        cursor.fast_executemany = False
        cursor.setinputsizes(None)
        failed_row = None
        for i, row in enumerate(data_to_insert):
            try:
                cursor.execute(insert_sql, row)
            except pyodbc.Error as row_e:
                failed_row = i
                print(f"--- Error found in CSV {os.path.basename(csv_path)} row {i + 2} ---")
                print(f"Data: {dict(zip(insert_columns, row))}")
                print(f"Error Details: {row_e}")
                break
        # Discard anything the debug loop managed to insert before the failure.
        conn.rollback()
        if failed_row is None:
            print(
                "  Every row inserted cleanly on its own, so the data is fine - the failure"
                " came from the parameter bindings or from fast_executemany itself."
            )

# Close DB resources
cursor.close()
conn.close()