"""Load a downloaded ZConnect feedback-metrics CSV into TABLE_HC_FEEDBACK.

Usage:
    python hc_ingestion/ingest_feedback_metrics.py path/to/zconnect_feedbacks.csv
    python hc_ingestion/ingest_feedback_metrics.py path/to/zconnect_feedbacks.csv --dry-run

The shared HC CSV loader handles SQL type validation and replaces rows by
center_id/date. Set TABLE_HC_FEEDBACK in Zenoti/.env to select the destination.
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
    parser = argparse.ArgumentParser(description="Load a feedback-metrics CSV into SQL Server.")
    parser.add_argument("csv", nargs="+", help="downloaded feedback-metrics CSV")
    parser.add_argument("--dry-run", action="store_true", help="validate without writing to SQL Server")
    parser.add_argument("--check-db", action="store_true", help="check the destination and CSV mapping")
    parser.add_argument("--verbose", action="store_true", help="print CSV columns and a sample row")
    args = parser.parse_args()

    csv_path = " ".join(args.csv)
    if not os.path.isfile(csv_path):
        parser.error(f"CSV file not found: {csv_path}")

    command = [
        sys.executable, _IMPORTER, "--report", "feedback_metrics", "--csv", csv_path
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
