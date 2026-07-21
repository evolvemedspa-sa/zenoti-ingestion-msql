import os
import re
from datetime import datetime
import pyodbc
from dotenv import load_dotenv
from gdrive_helper import list_csv_filenames


def log(step, msg=""):
    print(f"  ● {step:<10} {msg}", flush=True)

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([SERVER, DATABASE, DB_USER, DB_PASSWORD]):
    missing = [k for k, v in {"SERVER": SERVER, "DATABASE": DATABASE, "DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD}.items() if not v]
    raise ValueError(f"Missing environment variables: {', '.join(missing)}")

conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
)

CSV_SOURCE = os.getenv("CSV_SOURCE", "local").lower()

TABLE_CONFIG = [
    {
        "table_env": "TABLE_APPOINTMENTS",
        "date_column": "appointment_date",
        "csv_prefix": "appointments",
        "folder_env": "GDRIVE_FOLDER_APPOINTMENTS",
        "date_format": "%Y-%m-%d",
    },
    {
        "table_env": "TABLE_CASH",
        "date_column": "payment_date",
        "csv_prefix": "sales-cash",
        "folder_env": "GDRIVE_FOLDER_CASH",
        "date_format": "%Y/%m/%d",
    },
    {
        "table_env": "TABLE_COG",
        "date_column": "transaction_date",
        "csv_prefix": "cost_of_goods",
        "folder_env": "GDRIVE_FOLDER_COG",
        "date_format": "%Y/%m/%d",
    },
    {
        "table_env": "TABLE_BLOCK_OUT",
        "date_column": "date",
        "csv_prefix": "attendance",
        "folder_env": "GDRIVE_FOLDER_BLOCK_OUT",
        "date_format": "%m/%d/%Y",
    },
    {
        "table_env": "TABLE_MEMBERSHIPS",
        "date_column": "sale_date",
        "csv_prefix": "memberships",
        "folder_env": "GDRIVE_FOLDER_MEMBERSHIPS",
        "date_format": "%Y/%m/%d",
    },
    {
        "table_env": "TABLE_STOCK_LEDGER",
        "date_column": "transaction_date",
        "csv_prefix": "stock_ledger",
        "folder_env": "GDRIVE_FOLDER_STOCK_LEDGER",
        "date_format": "%Y/%m/%d",
    },
]

DATE_RANGE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})")


def extract_date_range(filenames, prefix):
    for name in filenames:
        if name.lower().startswith(prefix.lower()):
            match = DATE_RANGE_PATTERN.search(name)
            if match:
                return match.group(1), match.group(2)
    return None, None


def delete_data_for_range(cursor, table, date_column, start_date, end_date):
    sql = f"DELETE FROM dbo.[{table}] WHERE [{date_column}] >= ? AND [{date_column}] <= ?"
    cursor.execute(sql, [start_date, end_date])
    return cursor.rowcount


def main():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    log("Connect", f"{DATABASE} on {SERVER}")

    credentials_json = os.getenv("GDRIVE_CREDENTIALS_JSON")
    credentials_file = os.getenv("GDRIVE_CREDENTIALS_FILE", "service_account.json")

    total_deleted = 0

    for config in TABLE_CONFIG:
        table = os.getenv(config["table_env"])
        if not table:
            print(f"  SKIP: {config['table_env']} not set")
            continue

        folder_id = os.getenv(config["folder_env"])

        if CSV_SOURCE == "gdrive" and folder_id:
            filenames = list_csv_filenames(
                folder_id,
                credentials_json=credentials_json,
                credentials_file=credentials_file,
            )
            if not filenames:
                print(f"  SKIP: No CSVs found in Google Drive folder for {config['table_env']}. No data will be deleted.")
                continue
        else:
            csv_path = os.getenv(f"CSV_FILE_{config['table_env'].replace('TABLE_', '')}")
            if csv_path and os.path.isdir(csv_path):
                filenames = os.listdir(csv_path)
            elif csv_path:
                filenames = [os.path.basename(csv_path)]
            else:
                print(f"  SKIP: No CSV source for {config['table_env']}")
                continue

        start_date, end_date = extract_date_range(filenames, config["csv_prefix"])

        if not start_date or not end_date:
            print(f"  SKIP: No date range in filenames for prefix '{config['csv_prefix']}'")
            continue

        date_fmt = config["date_format"]
        start_date = datetime.strptime(start_date, "%Y-%m-%d").strftime(date_fmt)
        end_date = datetime.strptime(end_date, "%Y-%m-%d").strftime(date_fmt)

        try:
            deleted = delete_data_for_range(cursor, table, config["date_column"], start_date, end_date)
            log("Delete", f"{table}: {deleted:,} rows ({start_date} to {end_date})")
            total_deleted += deleted
        except pyodbc.Error as e:
            print(f"  ERROR: DELETE failed for {table}: {e}")
            conn.rollback()
            raise

    conn.commit()
    log("Delete", f"Total: {total_deleted:,} rows committed")

    cursor.close()
    conn.close()
    log("Done")


if __name__ == "__main__":
    main()
