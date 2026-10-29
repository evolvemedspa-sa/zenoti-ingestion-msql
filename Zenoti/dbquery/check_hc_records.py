"""Read-only record checks for the ZConnect events and feedback tables.

Usage:
    python dbquery/check_hc_records.py
    python dbquery/check_hc_records.py events
    python dbquery/check_hc_records.py feedback
    python dbquery/check_hc_records.py --self-check
"""
import argparse
import os
import re
import sys

from dotenv import load_dotenv

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
load_dotenv(os.path.join(_ROOT, ".env"))

from db_helper import get_connection  # noqa: E402

REPORTS = {
    "events": {
        "env": "TABLE_HC_EVENTS",
        "default": "dbo.BRONZE_ZENOTI_EVENTS_REVIEWS",
        "date": "send_date",
        "key": ("centerid", "event_id", "send_date"),
    },
    "feedback": {
        "env": "TABLE_HC_FEEDBACK",
        "default": "dbo.BRONZE_ZENOTI_FEEDBACK",
        "date": "date",
        "key": ("center_id", "date"),
    },
}
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def qualified_table(value):
    """Validate a configured table identifier and return a quoted table name."""
    parts = value.split(".")
    if len(parts) == 1:
        schema, name = "dbo", parts[0]
    elif len(parts) == 2:
        schema, name = parts
    else:
        raise ValueError(f"invalid table name: {value!r}")
    if not _IDENTIFIER.fullmatch(schema) or not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"invalid table name: {value!r}")
    return schema, name, f"[{schema}].[{name}]"


def check_table(cursor, report, spec):
    """Print counts, date bounds, missing key counts, and duplicate-key groups."""
    configured = os.getenv(spec["env"]) or spec["default"]
    schema, name, table = qualified_table(configured)
    cursor.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        (schema, name),
    )
    columns = {row[0].lower() for row in cursor.fetchall()}
    if not columns:
        print(f"{report}: FAIL - table {configured} not found")
        return False

    required = {spec["date"].lower(), *(column.lower() for column in spec["key"])}
    missing = sorted(required - columns)
    if missing:
        print(f"{report}: FAIL - {configured} missing column(s): {', '.join(missing)}")
        return False

    date_column = spec["date"]
    key_columns = spec["key"]
    key_list = ", ".join(f"[{column}]" for column in key_columns)
    null_predicate = " OR ".join(f"[{column}] IS NULL" for column in key_columns)

    cursor.execute(
        f"SELECT COUNT_BIG(*), MIN([{date_column}]), MAX([{date_column}]), "
        f"SUM(CASE WHEN [{date_column}] IS NULL THEN CAST(1 AS bigint) "
        f"ELSE CAST(0 AS bigint) END), "
        f"SUM(CASE WHEN {null_predicate} THEN CAST(1 AS bigint) "
        f"ELSE CAST(0 AS bigint) END) FROM {table}"
    )
    total, earliest, latest, null_dates, null_keys = cursor.fetchone()

    cursor.execute(
        f"SELECT COUNT_BIG(*) FROM ("
        f"SELECT {key_list} FROM {table} "
        f"GROUP BY {key_list} HAVING COUNT_BIG(*) > 1"
        f") AS duplicate_keys"
    )
    duplicate_groups = cursor.fetchone()[0]

    print(f"{report}: {configured}")
    print(f"  rows: {total:,}; {date_column}: {earliest} .. {latest}")
    print(f"  NULL dates: {null_dates or 0:,}; rows with NULL key parts: {null_keys or 0:,}")
    print(f"  duplicate key groups ({', '.join(key_columns)}): {duplicate_groups:,}")
    if not total:
        print("  status: WARN - table is empty")
        return False
    if null_keys or duplicate_groups:
        print("  status: FAIL")
        return False
    print("  status: OK")
    return True


def self_check():
    assert qualified_table("BRONZE_ZENOTI_FEEDBACK") == (
        "dbo", "BRONZE_ZENOTI_FEEDBACK", "[dbo].[BRONZE_ZENOTI_FEEDBACK]"
    )
    assert qualified_table("dbo.BRONZE_ZENOTI_EVENTS_REVIEWS")[2] == \
        "[dbo].[BRONZE_ZENOTI_EVENTS_REVIEWS]"
    try:
        qualified_table("dbo.EVENTS; DROP TABLE EVENTS")
    except ValueError:
        pass
    else:
        raise AssertionError("unsafe table identifier was accepted")
    print("self-check OK")


def main():
    parser = argparse.ArgumentParser(description="Check stored ZConnect event and feedback records.")
    parser.add_argument("report", nargs="?", choices=("events", "feedback", "both"), default="both")
    parser.add_argument("--self-check", action="store_true", help="run offline checks only")
    args = parser.parse_args()
    if args.self_check:
        self_check()
        return 0

    selected = REPORTS if args.report == "both" else {args.report: REPORTS[args.report]}
    connection = get_connection()
    try:
        cursor = connection.cursor()
        results = [check_table(cursor, name, spec) for name, spec in selected.items()]
        cursor.close()
    finally:
        connection.close()
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
