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
TABLE = os.getenv("TABLE_EMPLOYEE_SALES")
CSV_FILE = os.getenv("CSV_FILE_EMPLOYEE_SALES")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

CSV_SOURCE = os.getenv("CSV_SOURCE", "local").lower()
if CSV_SOURCE == "gdrive":
    from gdrive_helper import get_csv_from_gdrive
    CSV_FILE = get_csv_from_gdrive(
        os.getenv("GDRIVE_FOLDER_EMPLOYEE_SALES"),
        credentials_json=os.getenv("GDRIVE_CREDENTIALS_JSON"),
        credentials_file=os.getenv("GDRIVE_CREDENTIALS_FILE", "service_account.json"),
    )

if not CSV_FILE:
    print("SKIP: No CSV file available for employee sales. Exiting.")
    exit(0)

if not TABLE:
    raise ValueError("Missing environment variable in .env file: TABLE_EMPLOYEE_SALES")

if not all([SERVER, DATABASE, DB_USER, DB_PASSWORD]):
    missing = [k for k, v in {"SERVER": SERVER, "DATABASE": DATABASE, "DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD}.items() if not v]
    raise ValueError(f"Missing environment variables in .env file: {', '.join(missing)}")

# ==================================
# Build Connection String
# ==================================
# Swap these two lines when testing locally: the legacy "SQL Server" driver is
# what is installed on the dev box, Driver 18 is what Railway has.
conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    # f"DRIVER={{SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
log("Connect", "Connected to Evolve Med Spa Server")

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
       c.CHARACTER_MAXIMUM_LENGTH,
       c.DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS AS c
WHERE c.TABLE_NAME = '{table_name}'
  AND c.TABLE_SCHEMA = '{table_schema}'
ORDER BY c.ORDINAL_POSITION
"""

cursor.execute(sql)
rows = cursor.fetchall()
sql_columns = [row[0] for row in rows]
identity_columns = {row[0] for row in rows if row[1] == 1}
column_char_length = {row[0]: row[2] for row in rows}
column_data_type = {row[0]: (row[3] or "").lower() for row in rows}
# CHARACTER_MAXIMUM_LENGTH is -1 for varchar(max)/nvarchar(max).
max_length_columns = [row[0] for row in rows if row[2] == -1 and row[0] not in identity_columns]

if not sql_columns:
    raise ValueError(f"Table {table_schema}.{table_name} not found in {DATABASE}")

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

# fast_executemany preallocates a buffer of rows x sum(declared column width)
# before sending anything. A varchar(max) column reports a 2 GB width, so a few
# thousand rows is enough to raise MemoryError. Turning fast_executemany off is
# not a fix either: the driver then binds every varchar(max) parameter with
# data-at-execution, one round trip per row, which effectively never finishes.
#
# setinputsizes pins each text parameter to the widest value actually present in
# the file, so the buffer stays small and fast_executemany remains usable.
CHAR_TYPES = {"char", "varchar", "nchar", "nvarchar", "text", "ntext"}
UNICODE_CHAR_TYPES = {"nchar", "nvarchar", "ntext"}
# Widest value the driver will bind as a non-MAX parameter.
MAX_BINDABLE_WIDTH = 4000
BATCH_SIZE = int(os.getenv("EMPLOYEE_SALES_BATCH_SIZE", "1000"))

if max_length_columns:
    log("Schema", f"MAX-width column(s): {', '.join(max_length_columns)}")


def measure_width(series):
    """Widest rendered value in a column, ignoring nulls."""
    lengths = series.map(lambda v: len(str(v)) if v is not None and pd.notnull(v) else 0)
    return int(lengths.max()) if len(lengths) else 0


def build_input_sizes(frame):
    """Bind widths for one file's parameters, in insert_columns order.

    Returns None when a value is too wide to bind explicitly - the caller then
    falls back to normal binding rather than risk a silent truncation.
    """
    sizes = []
    for col in insert_columns:
        data_type = column_data_type.get(col, "")
        if data_type not in CHAR_TYPES:
            # Numeric, date and datetime parameters bind at a fixed size already.
            # A bare None leaves this position at the driver default; an all-None
            # tuple is not the same thing and makes the driver raise HY104.
            sizes.append(None)
            continue

        declared = column_char_length.get(col)
        if declared is not None and declared > 0:
            width = declared
        else:
            # varchar(max): size to the data, never narrower than 1.
            width = max(measure_width(frame[col]), 1)

        if width > MAX_BINDABLE_WIDTH:
            return None

        sql_type = pyodbc.SQL_WVARCHAR if data_type in UNICODE_CHAR_TYPES else pyodbc.SQL_VARCHAR
        sizes.append((sql_type, width, 0))
    return sizes


use_fast = True

for csv_path in csv_paths:
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    log("Load", f"{os.path.basename(csv_path)} → {len(df):,} rows")

    # ==================================
    # Align CSV Columns with SQL Headers (per-file)
    # ==================================
    # Zenoti is inconsistent about spacing/parentheses in the "Employee Sales"
    # export headers, so every observed spelling is mapped explicitly here.
    MANUAL_MAP = {
        "Sale Center Code": "sale_center_code",
        "Center Code": "sale_center_code",
        "Sale Center": "sale_center",
        "Center Name": "sale_center",
        "Sale Date": "sale_date",
        "Employee Name": "employee_name",
        "Employee": "employee_name",
        "Employee Code": "employee_code",
        "Job": "job",
        "Job Role": "job",
        "Invoice No": "invoice_no",
        "Invoice No.": "invoice_no",
        "Invoice #": "invoice_no",
        "Item Code": "item_code",
        "Item Type": "item_type",
        "Item Name": "item_name",
        "Sale Type": "sale_type",
        "Sales": "sales",
        "Sales (Exc.Tax)": "sales",
        "Sales(Exc. Tax)": "sales",
        "Sales(Inc. Tax)": "sales_inc_tax",
        "Sales (Inc. Tax)": "sales_inc_tax",
        "Sales(Inc.Tax)": "sales_inc_tax",
        "Sales (Inc.Tax)": "sales_inc_tax",
        "Sales Inc Tax": "sales_inc_tax",
        "Split Commission": "split_commission",
        "Split Commission (%)": "split_commission",
        "Split Commission %": "split_commission",
        "Employee Sale Value": "employee_sale_value",
        "Employee Sales Value": "employee_sale_value",
        "Commissionable Discount": "commissionable_discount",
        "Payment Type": "payment_type",
        "Employee Sales Status": "employee_sales_status",
        "Status": "employee_sales_status",
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
        df[real_row_id_col] = df[real_row_id_col].where(df[real_row_id_col].notnull(), None)
        df[real_row_id_col] = df[real_row_id_col].apply(lambda x: str(uuid.uuid4()) if x is None else x)

    # Keep only columns that exist in SQL and do not include identities in the insert
    df = df[insert_columns]

    # Convert blanks to NULL
    df = df.replace("", None)

    # ==================================
    # Date/Time Type Conversion
    # ==================================
    # sale_date is a DATE column; sql_helper.py deletes on the same format.
    date_columns = {"sale_date"}
    for col in df.columns:
        if normalize_col(col) in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed').dt.strftime('%Y-%m-%d')

    # ==================================
    # Numeric Type Conversion
    # ==================================
    # Codes and invoice numbers stay text: they can be zero-padded or alphanumeric,
    # and coercing them to Int64 would silently drop leading zeros.
    text_columns = {
        "sale_center_code", "employee_code", "invoice_no", "item_code",
        "ingestion_timestamp", "date_inserted", "source_file",
    }
    for col in [c for c in df.columns if normalize_col(c) not in text_columns]:
        test_numeric = pd.to_numeric(df[col], errors='coerce')
        if test_numeric.notnull().sum() == df[col].notnull().sum() and df[col].notnull().sum() > 0:
            if (test_numeric.dropna() % 1 == 0).all():
                df[col] = test_numeric.round().astype('Int64')
            else:
                df[col] = test_numeric
        else:
            df[col] = df[col].where(df[col].notnull(), None)

    for col in [c for c in df.columns if normalize_col(c) in text_columns]:
        df[col] = df[col].where(df[col].notnull(), None)

    # ==================================
    # Insert Data for this CSV
    # ==================================
    data_to_insert = df.astype(object).where(df.notnull(), None).values.tolist()

    input_sizes = build_input_sizes(df)
    if input_sizes is None:
        # A value exceeds the explicit-bind ceiling; let the driver size it.
        log("Insert", "value wider than bind limit; using default binding")
        cursor.setinputsizes(None)
        cursor.fast_executemany = False
        use_fast = False
    else:
        cursor.setinputsizes(input_sizes)
        cursor.fast_executemany = use_fast

    def report_bad_row(chunk, offset, err):
        """Re-run a failed chunk one row at a time to name the offending row/column."""
        print(f"Data insertion failed for {os.path.basename(csv_path)}: {err}")
        conn.rollback()
        print("Searching for problematic row...")
        cursor.fast_executemany = False
        for i, row in enumerate(chunk):
            try:
                cursor.execute(insert_sql, row)
            except (pyodbc.DataError, pyodbc.ProgrammingError) as row_e:
                param_info = ""
                if "Parameter" in str(row_e):
                    match = re.search(r"Parameter (\d+)", str(row_e))
                    if match:
                        param_idx = int(match.group(1)) - 1
                        if 0 <= param_idx < len(insert_columns):
                            param_info = f" (column: {insert_columns[param_idx]})"
                # +2 converts a 0-based data index into a 1-based CSV line incl. header
                print(f"Error at {os.path.basename(csv_path)} row {offset + i + 2}{param_info}: {row_e}")
                break
        conn.rollback()
        cursor.fast_executemany = use_fast

    inserted = 0
    failed = False
    for start in range(0, len(data_to_insert), BATCH_SIZE):
        chunk = data_to_insert[start:start + BATCH_SIZE]
        try:
            cursor.executemany(insert_sql, chunk)
        except MemoryError:
            # Buffer allocation blew up even at this chunk size; retry the chunk
            # with normal parameter binding and stay in that mode for the rest.
            conn.rollback()
            cursor.fast_executemany = False
            use_fast = False
            log("Insert", f"MemoryError at row {start:,}; retrying without fast_executemany")
            try:
                cursor.executemany(insert_sql, chunk)
            except (pyodbc.DataError, pyodbc.ProgrammingError) as e:
                report_bad_row(chunk, start, e)
                failed = True
                break
        except (pyodbc.DataError, pyodbc.ProgrammingError) as e:
            report_bad_row(chunk, start, e)
            failed = True
            break

        conn.commit()
        inserted += len(chunk)
        log("Insert", f"{inserted:,} / {len(data_to_insert):,} rows")

    cursor.setinputsizes(None)

    if not failed:
        log("Insert", f"{inserted:,} rows")
        log("Failed", "0 rows")
    else:
        log("Insert", f"{inserted:,} rows committed before failure")

# Close DB resources
cursor.close()
conn.close()
log("Done")
