import os
import uuid
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from datetime import datetime
import re

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

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

if not all([SERVER, DATABASE, DB_USER, DB_PASSWORD]):
    missing = [k for k, v in {"SERVER": SERVER, "DATABASE": DATABASE, "DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD}.items() if not v]
    raise ValueError(f"Missing environment variables in .env file: {', '.join(missing)}")

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

def normalize_col(col_name):
    return col_name.strip().strip('.,').replace(" ", "_").replace("-", "_").lower()

sql_column_lookup = {normalize_col(c): c for c in sql_columns}

# ==================================
# Helper: Extract date from filename
# ==================================
def extract_date_from_filename(filename):
    match = re.match(r'^business_kpi_(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})\.csv$', filename, re.IGNORECASE)
    if match:
        return match.group(1), match.group(2)
    match = re.match(r'^Business_KPI_(\d{4}-\d{2}-\d{2})\.csv$', filename, re.IGNORECASE)
    if match:
        return match.group(1), match.group(1)
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
# Load CSV(s)
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
                print(f"SKIPPED: '{f}' — filename must follow format Business_KPI_<YYYY-MM-DD>.csv or business_kpi_<YYYY-MM-DD>_to_<YYYY-MM-DD>.csv")
elif os.path.isfile(CSV_FILE):
    fname = os.path.basename(CSV_FILE)
    if extract_date_from_filename(fname):
        csv_paths = [CSV_FILE]
    else:
        raise ValueError(f"Invalid filename: '{fname}' — filename must follow format Business_KPI_<YYYY-MM-DD>.csv or business_kpi_<YYYY-MM-DD>_to_<YYYY-MM-DD>.csv")
else:
    raise ValueError(f"CSV_FILE path does not exist: {CSV_FILE}")

if not csv_paths:
    print(f"No CSV files found in: {CSV_FILE}")

# Precompute column lists (exclude identity columns)
insert_columns = [c for c in sql_columns if c not in identity_columns]
table_qualified = f"[{table_schema}].[{table_name}]"

# Find the actual SQL column name for center_name (the match key)
CENTER_NAME_SQL_COL = sql_column_lookup.get("center_name")
if not CENTER_NAME_SQL_COL:
    raise ValueError("Could not find 'center_name' column in the SQL table. Cannot proceed with upsert logic.")

# Build UPDATE columns (exclude identity columns and the match key itself)
update_columns = [c for c in insert_columns if c != CENTER_NAME_SQL_COL]

update_sql = f"""
UPDATE {table_qualified}
SET {', '.join(f'[{c}] = ?' for c in update_columns)}
WHERE [{CENTER_NAME_SQL_COL}] = ?
"""

insert_sql = f"""
INSERT INTO {table_qualified}
({','.join(f'[{c}]' for c in insert_columns)})
VALUES
({','.join(['?'] * len(insert_columns))})
"""

for csv_path in csv_paths:
    print(f"\nProcessing CSV: {csv_path}")
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    print(f"Found {len(df):,} rows in {os.path.basename(csv_path)}")

    # ==================================
    # Column Mapping
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
        if csv_col in MANUAL_MAP:
            target = MANUAL_MAP[csv_col]
            mapping[csv_col] = sql_column_lookup.get(normalize_col(target), target)
            continue
        norm_csv = normalize_col(csv_col)
        if norm_csv in sql_column_lookup:
            mapping[csv_col] = sql_column_lookup[norm_csv]

    df.rename(columns=mapping, inplace=True)

    # ==================================
    # Add business_kpi_date from filename
    # ==================================
    date_result = extract_date_from_filename(os.path.basename(csv_path))
    if date_result:
        if date_result[0] != date_result[1]:
            kpi_date = f"{date_result[0]}_to_{date_result[1]}"
        else:
            kpi_date = date_result[0]
        if "business_kpi_date" in sql_column_lookup:
            col_name = sql_column_lookup["business_kpi_date"]
            df[col_name] = kpi_date
            print(f"Set business_kpi_date = {kpi_date} (from filename)")

    # ==================================
    # Add Metadata Columns
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

    if 'row_id' in [normalize_col(c) for c in sql_columns]:
        real_row_id_col = sql_columns[[normalize_col(c) for c in sql_columns].index('row_id')]
        if real_row_id_col not in df.columns:
            df[real_row_id_col] = None
        df[real_row_id_col] = df[real_row_id_col].where(df[real_row_id_col].notnull(), None)
        df[real_row_id_col] = df[real_row_id_col].apply(lambda x: str(uuid.uuid4()) if x is None else x)

    df = df[insert_columns]
    df = df.replace("", None)

    # ==================================
    # Normalize Time Columns
    # ==================================
    time_columns = {
        "available_hours", "available_hours_providers",
        "serviced_hours", "booked_hours", "blocked_hours"
    }
    for col in df.columns:
        if normalize_col(col) in time_columns:
            df[col] = df[col].apply(normalize_time)

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
    # Fetch existing center_names from SQL
    # ==================================
    cursor.execute(f"SELECT DISTINCT [{CENTER_NAME_SQL_COL}] FROM {table_qualified}")
    existing_centers = {row[0] for row in cursor.fetchall()}
    print(f"Found {len(existing_centers)} existing center(s) in SQL")

    # ==================================
    # Upsert: UPDATE existing, INSERT new
    # ==================================
    updated = 0
    inserted = 0
    errors = 0

    for i, row_series in df.iterrows():
        row_dict = row_series.to_dict()
        center_name_val = row_dict.get(CENTER_NAME_SQL_COL)

        # Convert pandas NA types to None for pyodbc
        row_values = []
        for c in insert_columns:
            v = row_dict[c]
            if pd.isna(v) if not isinstance(v, str) else False:
                row_values.append(None)
            else:
                row_values.append(v)

        if center_name_val in existing_centers:
            # UPDATE: values for SET clause + center_name for WHERE clause
            update_values = [row_values[insert_columns.index(c)] for c in update_columns]
            update_values.append(center_name_val)
            try:
                cursor.execute(update_sql, update_values)
                updated += 1
            except (pyodbc.DataError, pyodbc.ProgrammingError) as e:
                print(f"UPDATE failed for center '{center_name_val}' (row {i + 2}): {e}")
                errors += 1
        else:
            # INSERT new row
            try:
                cursor.execute(insert_sql, row_values)
                inserted += 1
                existing_centers.add(center_name_val)
            except (pyodbc.DataError, pyodbc.ProgrammingError) as e:
                print(f"INSERT failed for center '{center_name_val}' (row {i + 2}): {e}")
                errors += 1

    try:
        conn.commit()
        print(f"\nResults for {os.path.basename(csv_path)}:")
        print(f"  Updated: {updated:,} rows")
        print(f"  Inserted: {inserted:,} rows")
        if errors:
            print(f"  Errors: {errors:,} rows")
    except Exception as e:
        print(f"Commit failed: {e}")
        conn.rollback()

cursor.close()
conn.close()
print("\nDone.")
