"""Load a downloaded ZConnect events CSV into TABLE_HC_EVENTS.

Usage:
    python hc_ingestion/ingest_events.py path/to/zconnect_events.csv
    python hc_ingestion/ingest_events.py path/to/zconnect_events.csv --dry-run

The destination table must already exist and include centerid, event_id, and
send_date. The shared HC CSV loader validates SQL types and replaces rows by
that composite key. Set TABLE_HC_EVENTS in Zenoti/.env to select the destination.
"""
import argparse
import os
import subprocess
import sys


_HERE = os.path.dirname(os.path.abspath(__file__))
_IMPORTER = os.path.join(
    os.path.dirname(_HERE), "hc_sql_helper", "import_hc_report.py"
)


def main():
    parser = argparse.ArgumentParser(description="Load an events CSV into SQL Server.")
    parser.add_argument("csv", nargs="+", help="downloaded events CSV")
    parser.add_argument("--dry-run", action="store_true", help="validate without writing to SQL Server")
    parser.add_argument("--check-db", action="store_true", help="check the destination and CSV mapping")
    parser.add_argument("--verbose", action="store_true", help="print CSV columns and a sample row")
    args = parser.parse_args()

    csv_path = " ".join(args.csv)
    if not os.path.isfile(csv_path):
        parser.error(f"CSV file not found: {csv_path}")

    command = [
        sys.executable, _IMPORTER, "--report", "events", "--csv", csv_path
    ]
    if args.dry_run:
        command.append("--dry-run")
    if args.check_db:
        command.append("--check-db")
    if args.verbose:
        command.append("--verbose")
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    sys.exit(main())
