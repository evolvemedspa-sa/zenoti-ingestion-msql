import os
import pyodbc
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([SERVER, DATABASE, DB_USER, DB_PASSWORD]):
    missing = [k for k, v in {"SERVER": SERVER, "DATABASE": DATABASE, "DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD}.items() if not v]
    raise ValueError(f"Missing environment variables in .env file: {', '.join(missing)}")

conn_str = (
    f"DRIVER={{SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

TABLES = {
    "FB Ads": os.getenv("TABLE_FB_ADS"),
    "Google Ads": os.getenv("TABLE_GOOGLE_ADS"),
    "Business KPI": os.getenv("TABLE_BUSINESS_KPI"),
}


def parse_table(table_full):
    table_schema = 'dbo'
    table_name = table_full
    if '.' in table_full:
        parts = table_full.split('.', 1)
        table_schema = parts[0].strip(' []')
        table_name = parts[1].strip(' []')
    return table_schema, table_name


def check_table(label, table_full, cursor):
    print(f"\n{'=' * 60}")
    print(f"  {label}")
    print(f"{'=' * 60}")

    if not table_full:
        print(f"  SKIPPED: table env var not set\n")
        return

    table_schema, table_name = parse_table(table_full)
    table_qualified = f"[{table_schema}].[{table_name}]"

    cursor.execute(f"SELECT COUNT(*) FROM {table_qualified}")
    total = cursor.fetchone()[0]
    print(f"  Total rows: {total:,}")

    if total == 0:
        print("  (empty table)\n")
        return

    cursor.execute(f"""
        SELECT MIN(report_date), MAX(report_date)
        FROM {table_qualified}
        WHERE report_date IS NOT NULL
    """)
    row = cursor.fetchone()
    print(f"  Date range: {row[0]} to {row[1]}")

    cursor.execute(f"""
        SELECT account_name, COUNT(*) AS cnt
        FROM {table_qualified}
        GROUP BY account_name
        ORDER BY cnt DESC
    """)
    rows = cursor.fetchall()
    print(f"\n  Rows per account:")
    for r in rows:
        print(f"    {r[0] or '(null)'}: {r[1]:,}")

    cursor.execute(f"""
        SELECT report_date, COUNT(*) AS cnt
        FROM {table_qualified}
        GROUP BY report_date
        ORDER BY report_date DESC
    """)
    date_rows = cursor.fetchall()
    print(f"\n  Rows per date (latest 10):")
    for r in date_rows[:10]:
        print(f"    {r[0]}: {r[1]:,}")
    if len(date_rows) > 10:
        print(f"    ... ({len(date_rows) - 10} more dates)")

    print()


for label, table in TABLES.items():
    check_table(label, table, cursor)

cursor.close()
conn.close()
print("Done.")
