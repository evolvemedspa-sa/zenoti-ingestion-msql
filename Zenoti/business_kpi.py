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
TABLE = os.getenv("TABLE_BUSINESS_KPI")
CSV_FILE = os.getenv("CSV_FILE_BUSINESS_KPI")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

CSV_SOURCE = os.getenv("CSV_SOURCE", "local").lower()
if CSV_SOURCE == "gdrive":
    from gdrive_helper import get_csv_from_gdrive
    CSV_FILE = get_csv_from_gdrive(
        os.getenv("GDRIVE_FOLDER_BUSINESS_KPI"),
        credentials_json=os.getenv("GDRIVE_CREDENTIALS_JSON"),
        credentials_file=os.getenv("GDRIVE_CREDENTIALS_FILE", "service_account.json"),
    )

if not CSV_FILE:
    print("SKIP: No CSV file available for business_kpi. Exiting.")
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

print(f"Found {len(sql_columns)} SQL columns (identity: {', '.join(identity_columns) if identity_columns else 'none'})")

# Helper to normalize column names for matching between CSV and SQL
def normalize_col(col_name):
    return col_name.strip().strip('.,').replace(" ", "_").replace("-", "_").lower()

# Lookup from normalized SQL name -> actual SQL column name
sql_column_lookup = {normalize_col(c): c for c in sql_columns}

# ==================================
# Helper: Extract date from filename
# ==================================
def extract_date_from_filename(filename):
    match = re.match(r'^Business_KPI_(\d{4}-\d{2}-\d{2})\.csv$', filename, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

def normalize_time(val):
    if val is None or str(val).strip() == '':
        return None
    val = str(val).strip()
    if val == '0':
        return '0:00'
    parts = val.split(':')
    if len(parts) == 3:
        return f"{int(parts[0])}:{parts[1]}"
    if len(parts) == 2:
        return f"{int(parts[0])}:{parts[1]}"
    return val

# ==================================
# Load CSV(s) and process one file at a time
# ==================================
if not CSV_FILE:
    raise ValueError("CSV_FILE must be set in the .env and point to a file or directory")

csv_paths = []
if os.path.isdir(CSV_FILE):
    for f in sorted(os.listdir(CSV_FILE)):
        if f.lower().endswith('.csv'):
            if extract_date_from_filename(f):
                csv_paths.append(os.path.join(CSV_FILE, f))
            else:
                print(f"SKIPPED: '{f}' — filename must follow the format Business_KPI_<YYYY-MM-DD>.csv")
elif os.path.isfile(CSV_FILE):
    fname = os.path.basename(CSV_FILE)
    if extract_date_from_filename(fname):
        csv_paths = [CSV_FILE]
    else:
        raise ValueError(f"Invalid filename: '{fname}' — filename must follow the format Business_KPI_<YYYY-MM-DD>.csv")
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
    print(f"Processing CSV: {csv_path}")
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    print(f"Found {len(df):,} rows in {os.path.basename(csv_path)}")

    # ==================================
    # Align CSV Columns with SQL Headers (per-file)
    # ==================================
    MANUAL_MAP = {
        "Center Name": "center_name",
        "Unique Guest count": "unique_guest_count",
        "New Guest Count": "new_guest_count",
        "New Guest %": "new_guest_percentage",
        "Invoice Count": "invoice_count",
        "Online invoice count": "online_invoice_count",
        "Online invoice %": "online_invoice_percentage",
        "Rebooking Source Count": "rebooking_source_count",
        "Rebooking Source %": "rebooking_source_percentage",
        "Services with provider requests": "services_with_provider_requests_count",
        "Services with provider requests %": "services_with_provider_requests_percentage",
        "Service Sales Count": "service_sales_count",
        "Service Sales": "service_sales",
        "Average service sale per invoice": "average_service_sale_per_invoice",
        "Service Sales Invoice Count": "service_sales_invoice_count",
        "Product Sales Count": "product_sales_count",
        "Product Sales": "product_sales",
        "Average product sale per invoice": "average_product_sale_per_invoice",
        "Product Sales Invoice Count": "product_sales_invoice_count",
        "Package Sales Count": "package_sales_count",
        "Package Sales": "package_sales",
        "Average package sale per invoice": "average_package_sale_per_invoice",
        "Package Sales Invoice Count": "package_sales_invoice_count",
        "Membership Sales Count": "membership_sales_count",
        "Membership Sales": "membership_sales",
        "Average membership sale per invoice": "average_membership_sale_per_invoice",
        "Membership Sales Invoice Count": "membership_sales_invoice_count",
        "Gift Card Sales Count": "gift_card_sales_count",
        "Gift Card Sales": "gift_card_sales",
        "Prepaid Card Sales Count": "prepaid_card_sales_count",
        "Prepaid Card Sales": "prepaid_card_sales",
        "Total Sales": "total_sales",
        "Sale Count": "sale_count",
        "Refund Amount": "refund_amount",
        "Refund Quantity": "refund_quantity",
        "Open Invoices Count": "open_invoices_count",
        "Closed Invoices Count": "closed_invoices_count",
        "Center Utilization(By Employee)": "center_utilization_by_employee",
        "Available Hours": "available_hours",
        "Available hours (Providers)": "available_hours_providers",
        "Serviced Hours": "serviced_hours",
        "Booked Hours": "booked_hours",
        "Blocked Hours": "blocked_hours",
        "Gift card redeemed": "gift_card_redeemed",
        "Gift card redeemed (cross center)": "gift_card_redeemed_cross_center",
        "Package redeemed": "package_redeemed",
        "Package redeemed (cross center)": "package_redeemed_cross_center",
        "Membership redeemed": "membership_redeemed",
        "Membership redeemed (cross center)": "membership_redeemed_cross_center",
        "Discount": "discount",
        "Gift card online sales": "gift_card_online_sales",
        "Gift card in-store sales": "gift_card_instore_sales",
        "No-show fees": "no_show_fees",
        "Cancellation fees": "cancellation_fees",
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
    # Add business_kpi_date from filename
    # ==================================
    kpi_date = extract_date_from_filename(os.path.basename(csv_path))
    if kpi_date:
        if "business_kpi_date" in sql_column_lookup:
            col_name = sql_column_lookup["business_kpi_date"]
            df[col_name] = kpi_date
            print(f"Set business_kpi_date = {kpi_date} (from filename)")

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
    # Normalize Time Columns (e.g. 33:00:00 → 33:00, 0 → 0:00)
    # ==================================
    time_columns = {
        "available_hours", "available_hours_providers",
        "serviced_hours", "booked_hours", "blocked_hours"
    }
    for col in df.columns:
        if normalize_col(col) in time_columns:
            df[col] = df[col].apply(normalize_time)

    # ==================================
    # Date/Time Type Conversion
    # ==================================
    currency_date_columns = {"business_kpi_date"}
    for col in df.columns:
        if normalize_col(col) in currency_date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y/%m/%d')

    # ==================================
    # Numeric Type Conversion
    # ==================================
    skip_numeric = {
        "ingestion_timestamp", "date_inserted", "business_kpi_date",
        "available_hours", "available_hours_providers",
        "serviced_hours", "booked_hours", "blocked_hours",
        "new_guest_percentage", "online_invoice_percentage",
        "rebooking_source_percentage", "services_with_provider_requests_percentage",
        "center_utilization_by_employee", "center_name"
    }
    for col in [c for c in df.columns if normalize_col(c) not in skip_numeric]:
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

# Close DB resources
cursor.close()
conn.close()
