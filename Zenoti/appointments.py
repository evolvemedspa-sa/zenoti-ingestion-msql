import os
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
TABLE = os.getenv("TABLE_APPOINTMENTS")
CSV_FILE = os.getenv("CSV_FILE_APPOINTMENTS")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

CSV_SOURCE = os.getenv("CSV_SOURCE", "local").lower()
if CSV_SOURCE == "gdrive":
    from gdrive_helper import get_csv_from_gdrive
    CSV_FILE = get_csv_from_gdrive(
        os.getenv("GDRIVE_FOLDER_APPOINTMENTS"),
        credentials_json=os.getenv("GDRIVE_CREDENTIALS_JSON"),
        credentials_file=os.getenv("GDRIVE_CREDENTIALS_FILE", "service_account.json"),
    )

if not CSV_FILE:
    print("SKIP: No CSV file available for appointments. Exiting.")
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
log("Connect", f"{DATABASE} on {SERVER}")

# ==================================
# Get SQL Columns
# ==================================
sql = f"""
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{TABLE}'
ORDER BY ORDINAL_POSITION
"""

cursor.execute(sql)
sql_columns = [row[0] for row in cursor.fetchall()]

identity_sql = f"""
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{TABLE}' AND COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1
"""
cursor.execute(identity_sql)
identity_columns = [row[0] for row in cursor.fetchall()]

log("Schema", f"{len(sql_columns)} columns (identity: {', '.join(identity_columns) if identity_columns else 'none'})")


# ==================================
# Load CSV
# ==================================
df = pd.read_csv(
    CSV_FILE,
    dtype=str,
    keep_default_na=False
)
log("Load", f"{os.path.basename(CSV_FILE)} → {len(df):,} rows")

# ==================================
# Align CSV Columns with SQL Headers
# ==================================
# Use this dictionary to explicitly map CSV headers to SQL columns 
# when names are completely different or have special characters.
MANUAL_MAP = {
    "Appointment Date": "appointment_date",
    "Booked Date": "booked_date",
    "Invoice No": "invoice_no",
    "Guest Name": "guest_name",
    "Service Name": "service_name",
    "Center Name": "center_name",
    "Start Time": "start_time",
    "End Time": "end_time",
    "Scheduled Service Duration": "scheduled_service_duration",
    "Scheduled Service and Recovery Duration": "scheduled_service_and_recovery_duration",
    "Recovery Time": "recovery_time",
    "Providers": "providers",
    "Room": "room",
    "Status": "status",
    "Guest Code": "guest_code",
    "Gender": "gender",
    "Email": "email",
    "Add-on": "add_on",
    "Checkin Time": "checkin_time",
    "Default Service Duration": "default_service_duration",
    "Default Service and Recovery Duration": "default_service_and_recovery_duration",
    "Providerss Code": "providerss_code",
    "Request Type": "request_type",
    "Service Category": "service_category",
    "Service Subcategory": "service_subcategory",
    "Day Package": "day_package",
    "Room Category": "room_category",
    "Equipment": "equipment",
    "Rebooked": "rebooked",
    "Rebooking Source": "rebooking_source",
    "Booking Source": "booking_source",
    "UTM Source": "utm_source",
    "UTM Medium": "utm_medium",
    "Appointment Category": "appointment_category",
    "Business Unit": "business_unit",
    "Appointment Notes": "appointment_notes",
    "Booked By": "booked_by",
    "Modified by": "modified_by",
    "Modified On": "modified_on",
    "Before Appointment": "before_appointment",
    "After Appointment": "after_appointment",
    "Reason": "reason",
    "Actual Start Time": "actual_starttime",
    "Actual StartTime": "actual_starttime",
    "Actual End Time": "actual_endtime",
    "Actual EndTime": "actual_endtime",
    "First Visit": "first_visit",
    "Surprise Visit": "surprise_visit",
    "Actual Duration": "actual_duration"
}

mapping = {}
for csv_col in df.columns:
    # 1. Check for manual override first
    if csv_col in MANUAL_MAP:
        mapping[csv_col] = MANUAL_MAP[csv_col]
        continue

    # 2. Otherwise, normalize CSV header for pattern matching
    norm_csv = csv_col.strip().strip('.,').replace(" ", "_").replace("-", "_").lower()
    for sql_col in sql_columns:
        # Normalize SQL column name for comparison
        if sql_col.strip().strip('.,').replace(" ", "_").replace("-", "_").lower() == norm_csv:
            mapping[csv_col] = sql_col
            break

df.rename(columns=mapping, inplace=True)

# ==================================
# Add Metadata Columns
# ==================================
# Use normalized matching to find the exact column name in SQL for metadata
sql_cols_norm = [c.strip().strip('.,').replace(" ", "_").replace("-", "_").lower() for c in sql_columns]

if "ingestion_timestamp" in sql_cols_norm:
    col_name = sql_columns[sql_cols_norm.index("ingestion_timestamp")]
    df[col_name] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

if "source_file" in sql_cols_norm:
    col_name = sql_columns[sql_cols_norm.index("source_file")]
    df[col_name] = "csv"

# ==================================
# Create Missing Columns
# ==================================
for col in sql_columns:
    if col not in df.columns:
        df[col] = None

# Keep only columns that exist in SQL
df = df[sql_columns]

# Convert blanks to NULL
df = df.replace("", None)

# # ==================================
# # Date/Time Type Conversion
# # ==================================
# # Ensure specified date columns are in YYYY/MM/DD format for SQL DATE types
# date_columns_to_fix = ["Appointment_Date", "Booked_Date"]
# for col in date_columns_to_fix:
#     if col in df.columns:
#         # Convert to datetime objects and then format as strings
#         df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed').dt.strftime('%Y/%m/%d')

# # Ensure timestamp columns are in standard ISO format for SQL DATETIME types
# datetime_columns_to_fix = ["Actual_StartTime", "Actual_EndTime", "Modified_On", "Start_Time", "End_Time", "Checkin_Time"]
# for col in datetime_columns_to_fix:
#     if col in df.columns:
#         # SQL Server fast_executemany is strict; ISO format (YYYY-MM-DD HH:MM:SS) is safest.
#         df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed').dt.strftime('%Y-%m-%d %H:%M:%S')
# ==================================
# Date/Time Type Conversion
# ==================================
date_columns_to_fix = ["appointment_date", "booked_date"]
for col in date_columns_to_fix:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed').dt.strftime('%Y-%m-%d') # Use standard YYYY-MM-DD

datetime_columns_to_fix = ["actual_starttime", "actual_endtime", "modified_on", "start_time", "end_time", "checkin_time"]
for col in datetime_columns_to_fix:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed').dt.strftime('%Y-%m-%d %H:%M:%S')



# ==================================
# Numeric Type Conversion
# ==================================
for col in [c for c in df.columns if c != "ingestion_timestamp"]:
    # Test for pure numeric content
    # Convert to numeric; non-numeric values become NaN
    test_numeric = pd.to_numeric(df[col], errors='coerce')

    # We only convert the column if every non-null entry is successfully parsed as a number.
    # This prevents turning mixed text/number columns (like 'Center_Name') into floats.
    if test_numeric.notnull().sum() == df[col].notnull().sum() and df[col].notnull().sum() > 0:
        # Determine if it's an integer column (no decimals)
        if (test_numeric.dropna() % 1 == 0).all():
            # Use Int64 (nullable integer) to avoid the '.0' float issue
            df[col] = test_numeric.round().astype('Int64')
        else:
            df[col] = test_numeric
    else:
        # Keep as strings, ensuring nulls remain None
        df[col] = df[col].where(df[col].notnull(), None)

# ==================================
# Insert Data
# ==================================
insert_sql = f"""
INSERT INTO dbo.{TABLE}
({','.join(f'[{c}]' for c in sql_columns)})
VALUES
({','.join(['?'] * len(sql_columns))})
"""

# fast_executemany is highly performant but throws 'Numeric value out of range' 
# if Python strings/numbers exceed SQL column limits.
cursor.fast_executemany = True

# Convert NaN values (result of to_numeric) back to None for SQL NULL
# We cast to 'object' first to ensure Pandas NAType/nan are converted 
# to standard Python None, which pyodbc understands.
data_to_insert = df.astype(object).where(df.notnull(), None).values.tolist()

try:
    cursor.executemany(insert_sql, data_to_insert)
    conn.commit()
    log("Insert", f"{len(df):,} rows")
    log("Failed", "0 rows")
except (pyodbc.DataError, pyodbc.ProgrammingError) as e:
    print(f"Data insertion failed: {e}")
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
            print(f"Error at CSV row {i + 2}{param_info}: {row_e}")
            break
finally:
    cursor.close()
    conn.close()
    log("Done")