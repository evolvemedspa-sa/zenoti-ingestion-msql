import os
import uuid
import pandas as pd
import pyodbc
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
TABLE = os.getenv("TABLE_CASH")
CSV_FILE = os.getenv("CSV_FILE_CASH")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

CSV_SOURCE = os.getenv("CSV_SOURCE", "local").lower()
if CSV_SOURCE == "gdrive":
    from gdrive_helper import get_csv_from_gdrive
    CSV_FILE = get_csv_from_gdrive(
        os.getenv("GDRIVE_FOLDER_CASH"),
        credentials_json=os.getenv("GDRIVE_CREDENTIALS_JSON"),
        credentials_file=os.getenv("GDRIVE_CREDENTIALS_FILE", "service_account.json"),
    )

if not CSV_FILE:
    print("SKIP: No CSV file available for cash. Exiting.")
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
    # If you still need Trusted_Connection for other environments, consider conditional logic.
    # For this error, removing Trusted_Connection=yes and adding UID/PWD is the direct fix.
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

# Prepare cursor for fast inserts
cursor.fast_executemany = True

for csv_path in csv_paths:
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    print(f"Processing {len(df):,} rows from {os.path.basename(csv_path)}")

    # ==================================
    # Align CSV Columns with SQL Headers (per-file)
    # ==================================
    MANUAL_MAP = {
        "Item Type": "item_type",
        "Payment Date": "payment_date",
        "Sale Date": "sale_date",
        "Invoice No": "invoice_no",
        "Guest Code": "guest_code",
        "Guest Name": "guest_name",
        "Center Code": "center_code",
        "Center Name": "center_name",
        "Item Code": "item_code",
        "Item Name": "item_name",
        "Qty": "qty",
        "Sales Collected (Exc.Tax)": "sales_collected_exc_tax",
        "Tax Collected": "tax_collected",
        "Sales Collected (Inc.Tax)": "sales_collected_inc_tax",
        "Redeemed": "redeemed",
        "Collected to Date": "collected_as_on_date",
        "Collected": "collected",
        "Item Category": "item_category",
        "Item Subcategory": "item_sub_category",
        "Discount Name": "discount_name",
        "Discount": "discount",
        "Payment Type": "payment_type",
        "Sale Type": "sale_type",
        "Sold By": "sold_by",
        "Status": "status",
        "Invoice Notes": "invoice_notes",
        "Referral Source": "referral_source",
        "Member": "member",
        "First Visit": "first_visit",
        "Invoice Source": "invoice_source",
        "Vendor Name": "vendor_name",
        "Brand Name": "brand_name",
        "Invoice For": "invoice_for"
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
    currency_date_columns = {"payment_date", "sale_date"}
    for col in df.columns:
        if normalize_col(col) in currency_date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y/%m/%d')

    # ==================================
    # Numeric Type Conversion
    # ==================================
    for col in [c for c in df.columns if normalize_col(c) != "ingestion_timestamp"]:
        test_numeric = pd.to_numeric(df[col], errors='coerce')
        if test_numeric.notnull().sum() == df[col].notnull().sum() and df[col].notnull().sum() > 0:
            if (test_numeric.dropna() % 1 == 0).all():
                df[col] = test_numeric.round().astype('Int64')
            else:
                df[col] = test_numeric
        else:
            df[col] = df[col].where(df[col].notnull(), None)

    # ==================================
    # Insert Data for this CSV
    # ==================================
    data_to_insert = df.astype(object).where(df.notnull(), None).values.tolist()

    try:
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()
        print(f"Inserted {len(df):,} rows successfully from {os.path.basename(csv_path)}.")
    except (pyodbc.DataError, pyodbc.ProgrammingError) as e:
        print(f"Data insertion failed for {os.path.basename(csv_path)}: {e}")
        conn.rollback()

        print("Searching for problematic row...")
        cursor.fast_executemany = False
        for i, row in enumerate(data_to_insert):
            try:
                cursor.execute(insert_sql, row)
            except (pyodbc.DataError, pyodbc.ProgrammingError) as row_e:
                param_info = ""
                if "Parameter" in str(row_e):
                    match = re.search(r"Parameter (\d+)", str(row_e))
                    if match:
                        param_idx = int(match.group(1)) - 1
                        param_info = f" (column: {sql_columns[param_idx]})"
                print(f"Error at {os.path.basename(csv_path)} row {i + 2}{param_info}: {row_e}")
                break

# Close DB resources
cursor.close()
conn.close()