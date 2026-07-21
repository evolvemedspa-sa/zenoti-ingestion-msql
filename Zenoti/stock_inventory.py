import os
import uuid
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from datetime import datetime
import re
from decimal import Decimal, InvalidOperation


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
# Load CSV(s) and process one file at a time
# ==================================
# If CSV_FILE points to a directory, gather all .csv files in that directory.
if not CSV_FILE:
    raise ValueError("CSV_FILE must be set in the .env and point to a file or directory")

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
    log("Load", f"No CSV files found in: {CSV_FILE}")

# Precompute insert columns (exclude identity columns)
insert_columns = [c for c in sql_columns if c not in identity_columns]
table_qualified = f"[{table_schema}].[{table_name}]"
insert_sql = f"""
INSERT INTO {table_qualified}
({','.join(f'[{c}]' for c in insert_columns)})
VALUES
({','.join(['?'] * len(insert_columns))})
"""
delete_sql = f"DELETE FROM {table_qualified}"

for csv_path in csv_paths:
    print(f"Processing CSV: {csv_path}")
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    log("Load", f"{os.path.basename(csv_path)} → {len(df):,} rows")

    # ==================================
    # Align CSV Columns with SQL Headers (per-file)
    # ==================================
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
    # Define which columns should be treated as dates.
    stock_inventory_date_columns = {"date_inserted"}
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
    # Delete existing data and insert new data
    # ==================================
    data_to_insert = df.astype(object).where(df.notnull(), None).values.tolist()
    total_rows = len(data_to_insert)

    try:
        # 1. Delete all existing rows from the table
        log("Delete", f"all rows from {table_qualified}...")
        cursor.execute(delete_sql)
        log("Delete", f"{cursor.rowcount:,} rows removed")

        # 2. Insert all new rows from the CSV
        log("Insert", f"{total_rows:,} new rows...")
        cursor.fast_executemany = True

        # Set input sizes for columns that are not numeric. This helps pyodbc's
        # fast_executemany mode handle None values correctly for string/text columns,
        # preventing "Invalid string or buffer length" errors.
        # We define a max length for VARCHAR columns; 0 means "max".
        # For decimals, we specify a precision (e.g., 38) and scale (e.g., 10).
        # Mixed text/numeric columns (like configured_purchase_price) are bound as
        # VARCHAR so both formatted numbers and "N/A" are stored correctly as text.
        sql_type_map = []
        for col_name in df.columns:
            if col_name in cols_to_convert:
                # Be explicit about decimal types with precision and scale.
                sql_type_map.append((pyodbc.SQL_DECIMAL, 38, 10))
            elif col_name in price_text_cols_resolved:
                # varchar(200) column - bind as wide varchar with an explicit
                # length matching the SQL column definition.
                sql_type_map.append((pyodbc.SQL_WVARCHAR, 200))
            else:
                # For all other columns, specify as wide varchar with max length.
                sql_type_map.append((pyodbc.SQL_WVARCHAR, 0))
        cursor.setinputsizes(sql_type_map)

        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()
        log("Insert", f"{total_rows:,} rows from {os.path.basename(csv_path)}")
        log("Failed", "0 rows")

    except (pyodbc.DataError, pyodbc.ProgrammingError) as e:
        print(f"Data insertion failed for {os.path.basename(csv_path)}: {e}")
        conn.rollback()

        # Fallback to row-by-row insert to find the problematic record
        print("Searching for the problematic row...")
        cursor.fast_executemany = False
        for i, row in enumerate(data_to_insert):
            try:
                cursor.execute(insert_sql, row)
            except (pyodbc.DataError, pyodbc.ProgrammingError) as row_e:
                print(f"--- Error found in CSV {os.path.basename(csv_path)} row {i + 2} ---")
                print(f"Data: {dict(zip(insert_columns, row))}")
                print(f"Error Details: {row_e}")
                break

# Close DB resources
cursor.close()
conn.close()