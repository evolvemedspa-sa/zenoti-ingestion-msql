"""Load detailed Zenoti feedback exports into SQL Server.

Uses TABLE_FEEDBACK_REPORT from Zenoti/.env, or the built-in destination.
Console output identifies the report as feedback_report and hides the
physical destination table name.

Runs beside run_all.py without importing anything from dbquery/. CSV_SOURCE
selects local files or Google Drive, matching the other pipeline reports.
The Drive folder is GDRIVE_FOLDER_FEEDBAC_REPORT (spelling is intentional).
Local input uses CSV_FILE_FEEDBACK_REPORT, or Feedback*.csv in sample csv/.
--csv selects a local file or directory regardless of CSV_SOURCE.

All 15 report columns are matched to the live table by normalized name.
Sale Date is parsed as a US calendar date; Rating is an integer from 1 to 5.
Blank cells become NULL, and First Visit / Member remain Yes/No text.
DATE_INSERTED uses the table's default, or a UTC load timestamp if needed.

The sample contains unescaped quotation marks inside one Guest Comments
field. A bounded recovery handles fully quoted records when all 15 field
boundaries are intact and every field except the comment parses strictly.
All CSVs are parsed before any database write. Other malformed records abort
the run before loading any file.

Reruns replace rows for the CSV's (Center Name, Invoice No) pairs. Other
invoices are retained. Identical duplicates in a file are collapsed; conflicting
records for one pair within a file are rejected. Each file's deletes and
inserts commit together. Files are processed in filename order.

Usage from Zenoti/:
    python feedback_report.py                # pipeline source from .env
    python feedback_report.py --dry-run       # validate source CSVs; no DB
    python feedback_report.py --check-db      # live schema validation; read-only
    python feedback_report.py --csv "sample csv" --dry-run
    python feedback_report.py --csv "C:/exports/Feedback.csv"
    python feedback_report.py --source local
    python feedback_report.py --table dbo.OTHER_FEEDBACK_TABLE --check-db
    python feedback_report.py --self-check    # parsing/transaction checks; no DB

Google Drive uses GDRIVE_CREDENTIALS_JSON or GDRIVE_CREDENTIALS_FILE through
the shared gdrive_helper. --dry-run may download Drive files; it never connects
to SQL Server. --csv ... --dry-run stays local.

Dependencies: python-dotenv and pyodbc (already in Zenoti/requirements.txt).
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import re
import sys
from contextlib import redirect_stdout
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import TypedDict

from dotenv import load_dotenv


SCRIPT_DIR = Path(__file__).resolve().parent
ZENOTI_DIR = SCRIPT_DIR
DEFAULT_LOCAL_SOURCE = ZENOTI_DIR / "sample csv"
DEFAULT_TABLE = "dbo.BRONZE_ZENOTI_FEEDBACK_REPORTS"
REPORT_LABEL = "feedback_report"
MAX_PARAMS = 2000  # Leave room below SQL Server's 2100-parameter limit.

SOURCE_COLUMNS = (
    "center_name", "invoice_no", "guest_name", "sale_date", "rating",
    "guest_comments", "tags", "providers", "appointment_status", "service",
    "category", "sub_category", "first_visit", "member", "source",
)
KEY_COLUMNS = ("center_name", "invoice_no")
AUDIT_COLUMNS = {"date_inserted", "ingestion_timestamp"}
TEXT_TYPES = {"char", "varchar", "nchar", "nvarchar", "text", "ntext"}
DATE_TYPES = {"date", "datetime", "datetime2", "smalldatetime"}
INTEGER_LIMITS = {
    "tinyint": (0, 255),
    "smallint": (-32768, 32767),
    "int": (-2147483648, 2147483647),
    "bigint": (-9223372036854775808, 9223372036854775807),
}


def normalize_col(name):
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")


def qualified_table(table):
    parts = table.strip().split(".")
    if len(parts) == 1:
        parts.insert(0, "dbo")
    if len(parts) != 2:
        raise ValueError("Use a table name or schema.table.")
    clean = []
    for part in parts:
        part = part.strip()
        if part.startswith("[") and part.endswith("]"):
            part = part[1:-1]
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", part):
            raise ValueError("Invalid SQL table identifier.")
        clean.append(part)
    return clean[0], clean[1], f"[{clean[0]}].[{clean[1]}]"


def quote_column(name):
    return "[" + name.replace("]", "]]") + "]"


def public_error(message: str, table: str) -> str:
    """Hide destination identifiers that an ODBC error may include."""
    variants = set()
    for target in {table, DEFAULT_TABLE, os.getenv("TABLE_FEEDBACK_REPORT") or DEFAULT_TABLE}:
        if not target.strip():
            continue
        variants.add(target)
        try:
            schema, name, qualified = qualified_table(target)
        except ValueError:
            continue
        variants.update((name, qualified, f"{schema}.{name}"))
    for variant in sorted(variants, key=len, reverse=True):
        pattern = r"(?<![A-Za-z0-9_])" + re.escape(variant) + r"(?![A-Za-z0-9_])"
        message = re.sub(pattern, REPORT_LABEL, message, flags=re.IGNORECASE)
    return message


class FeedbackValues(TypedDict):
    center_name: str
    invoice_no: str
    guest_name: str | None
    sale_date: date
    rating: int
    guest_comments: str | None
    tags: str | None
    providers: str | None
    appointment_status: str | None
    service: str | None
    category: str | None
    sub_category: str | None
    first_visit: str | None
    member: str | None
    source: str | None


@dataclass
class FeedbackRow:
    line: int
    values: FeedbackValues


@dataclass
class FeedbackCSV:
    rows: list[FeedbackRow] = field(default_factory=list)
    repaired_lines: list[int] = field(default_factory=list)
    blank_rows: int = 0
    duplicate_rows: int = 0


class RecordLines:
    """Track the physical lines consumed by one csv.reader record."""

    def __init__(self, stream):
        self.stream = iter(stream)
        self.parts = []

    def __iter__(self):
        return self

    def __next__(self):
        line = next(self.stream)
        self.parts.append(line)
        return line


def recover_comment(raw, headers):
    """Recover naked quotes in a comment without joining guessed CSV fields."""
    raw = raw.rstrip("\r\n")
    if not (raw.startswith('"') and raw.endswith('"')):
        raise ValueError("The malformed record is not fully quoted.")
    parts = raw[1:-1].split('","')
    if len(parts) != len(headers):
        raise ValueError("Cannot identify all 15 quoted field boundaries.")
    cells, repaired = [], False
    for name, part in zip(headers, parts):
        try:
            parsed = next(csv.reader(['"' + part + '"'], strict=True))
            if len(parsed) != 1:
                raise csv.Error("Extra fields inside a field.")
            cells.append(parsed[0])
        except csv.Error:
            if name != "guest_comments":
                raise ValueError(f"Malformed quotes outside Guest Comments: {name}.")
            cells.append(part.replace('""', '"'))
            repaired = True
    if not repaired:
        raise ValueError("The CSV error is not an unescaped quote in Guest Comments.")
    return cells


def clean_row(headers, cells, line) -> FeedbackValues:
    raw_values: dict[str, str | None] = {
        name: cell if cell.strip() else None
        for name, cell in zip(headers, cells)
    }

    def required_text(name: str) -> str:
        value = raw_values[name]
        if value is None:
            raise ValueError(f"Line {line}: {name} is required.")
        return value.strip()

    center_name = required_text("center_name")
    invoice_no = required_text("invoice_no")
    raw_date = required_text("sale_date")
    raw_rating = required_text("rating")
    sale_date = None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            sale_date = datetime.strptime(raw_date, fmt).date()
            break
        except ValueError:
            pass
    if sale_date is None:
        raise ValueError(f"Line {line}: Sale Date must be MM/DD/YYYY or YYYY-MM-DD.")
    try:
        rating = int(raw_rating)
    except ValueError:
        raise ValueError(f"Line {line}: Rating must be an integer from 1 to 5.")
    if not 1 <= rating <= 5:
        raise ValueError(f"Line {line}: Rating must be an integer from 1 to 5.")
    for name in ("first_visit", "member"):
        value = raw_values[name]
        if value is not None:
            flag = value.strip().lower()
            if flag not in {"yes", "no"}:
                raise ValueError(f"Line {line}: {name} must be Yes, No, or blank.")
            raw_values[name] = flag.title()
    return {
        "center_name": center_name,
        "invoice_no": invoice_no,
        "guest_name": raw_values["guest_name"],
        "sale_date": sale_date,
        "rating": rating,
        "guest_comments": raw_values["guest_comments"],
        "tags": raw_values["tags"],
        "providers": raw_values["providers"],
        "appointment_status": raw_values["appointment_status"],
        "service": raw_values["service"],
        "category": raw_values["category"],
        "sub_category": raw_values["sub_category"],
        "first_visit": raw_values["first_visit"],
        "member": raw_values["member"],
        "source": raw_values["source"],
    }


def read_feedback(stream):
    tracked = RecordLines(stream)
    reader = csv.reader(tracked, strict=True)
    try:
        headers = [normalize_col(h.lstrip("\ufeff")) for h in next(reader)]
    except StopIteration:
        raise ValueError("The CSV is empty.")
    except csv.Error as exc:
        raise ValueError(f"Invalid CSV header: {exc}") from exc
    if len(headers) != len(set(headers)):
        raise ValueError("Duplicate CSV headers after normalization.")
    missing = set(SOURCE_COLUMNS) - set(headers)
    extra = set(headers) - set(SOURCE_COLUMNS)
    if missing or extra:
        raise ValueError(
            f"CSV columns do not match Feedback.csv: "
            f"missing={sorted(missing)}, unexpected={sorted(extra)}."
        )

    report, seen = FeedbackCSV(), {}
    while True:
        tracked.parts.clear()
        line = reader.line_num + 1
        try:
            cells = next(reader)
        except StopIteration:
            break
        except csv.Error as exc:
            try:
                cells = recover_comment("".join(tracked.parts), headers)
            except ValueError as repair_error:
                raise ValueError(f"Line {line}: {exc}; {repair_error}") from exc
            report.repaired_lines.append(line)
        if not any(cell.strip() for cell in cells):
            report.blank_rows += 1
            continue
        if len(cells) != len(headers):
            raise ValueError(
                f"Line {line}: expected {len(headers)} fields, found {len(cells)}."
            )
        values = clean_row(headers, cells, line)
        # Be conservative about case variants of the same SQL Server key.
        key = (values["center_name"].casefold(), values["invoice_no"].casefold())
        previous = seen.get(key)
        if previous is not None:
            if previous.values != values:
                raise ValueError(
                    f"Lines {previous.line} and {line}: conflicting feedback for "
                    f"the same Center Name / Invoice No."
                )
            report.duplicate_rows += 1
            continue
        row = FeedbackRow(line, values)
        seen[key] = row
        report.rows.append(row)
    if not report.rows:
        raise ValueError("No feedback records found; nothing to ingest.")
    return report


def parse_csv(path):
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        return read_feedback(stream)


def csv_paths(source, feedback_only=False) -> list[Path]:
    path = Path(source).expanduser()
    if not path.is_absolute() and not path.exists():
        path = ZENOTI_DIR / path
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise FileNotFoundError(f"Feedback CSV source does not exist: {path}")
    paths = [p for p in path.iterdir()
             if p.is_file() and p.suffix.lower() == ".csv"
             and (not feedback_only or p.name.lower().startswith("feedback"))]
    return sorted(paths, key=lambda p: p.name.casefold())


def source_paths(args) -> list[Path]:
    if args.csv:
        return csv_paths(args.csv)
    source = (args.source or os.getenv("CSV_SOURCE", "local")).strip().lower()
    if args.gdrive_folder:
        source = "gdrive"
    if source == "local":
        configured = os.getenv("CSV_FILE_FEEDBACK_REPORT")
        return csv_paths(configured or DEFAULT_LOCAL_SOURCE, feedback_only=not configured)
    if source != "gdrive":
        raise ValueError("CSV_SOURCE must be local or gdrive.")
    folder = args.gdrive_folder or os.getenv("GDRIVE_FOLDER_FEEDBAC_REPORT")
    if not folder:
        raise ValueError("GDRIVE_FOLDER_FEEDBAC_REPORT is not set for feedback_report.")
    sys.path.insert(0, str(ZENOTI_DIR))
    from gdrive_helper import get_csv_from_gdrive

    # Shared Drive logs can include the folder name. Apply the same table
    # redaction before those lines reach run_all.py's console/file logs.
    drive_output = io.StringIO()
    try:
        with redirect_stdout(drive_output):
            downloaded = get_csv_from_gdrive(
                folder,
                credentials_json=os.getenv("GDRIVE_CREDENTIALS_JSON"),
                credentials_file=os.getenv("GDRIVE_CREDENTIALS_FILE", "service_account.json"),
            )
    finally:
        print(public_error(drive_output.getvalue(), args.table), end="", flush=True)
    return csv_paths(downloaded) if downloaded else []


@dataclass
class Column:
    name: str
    db_type: str
    length: int | None = None
    nullable: bool = True
    default: str | None = None
    identity: bool = False
    computed: bool = False


def table_columns(cursor, schema, name):
    cursor.execute("""
        SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
               c.IS_NULLABLE, c.COLUMN_DEFAULT,
               COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' +
                   QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, 'IsIdentity'),
               COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' +
                   QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, 'IsComputed')
        FROM INFORMATION_SCHEMA.COLUMNS AS c
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
    """, (schema, name))
    return [Column(r[0], r[1].lower(), r[2], r[3] == "YES", r[4],
                   r[5] == 1, r[6] == 1) for r in cursor.fetchall()]


def sql_value(value, column, line):
    prefix = f"Line {line}, column {column.name}"
    if value is None:
        if not column.nullable:
            raise ValueError(f"{prefix}: NULL is not allowed.")
        return None
    kind = column.db_type
    if kind in TEXT_TYPES:
        value = str(value)
        width = (len(value.encode("utf-16-le")) // 2
                 if kind in {"nchar", "nvarchar"} else len(value))
        if column.length is not None and column.length > 0 and width > column.length:
            raise ValueError(
                f"{prefix}: {width} characters exceeds the {column.length} limit."
            )
        return value
    if kind in DATE_TYPES and isinstance(value, date):
        if kind == "date":
            return value.date() if isinstance(value, datetime) else value
        return value if isinstance(value, datetime) else datetime.combine(value, time())
    if kind in INTEGER_LIMITS and isinstance(value, int):
        low, high = INTEGER_LIMITS[kind]
        if low <= value <= high:
            return value
    if kind == "bit" and value in {"Yes", "No"}:
        return value == "Yes"
    raise ValueError(f"{prefix}: unsupported value/type combination for {kind}.")


def prepare_rows(report, columns):
    writable = [c for c in columns
                if not c.identity and not c.computed
                and c.db_type not in {"timestamp", "rowversion"}]
    lookup = {normalize_col(c.name): c for c in writable}
    if len(lookup) != len(writable):
        raise ValueError("The table has ambiguous normalized column names.")
    missing = set(SOURCE_COLUMNS) - set(lookup)
    if missing:
        raise ValueError(f"Table is missing writable CSV columns: {sorted(missing)}.")
    insert_columns = []
    for column in writable:
        norm = normalize_col(column.name)
        if norm in SOURCE_COLUMNS:
            insert_columns.append(column)
        elif norm in AUDIT_COLUMNS:
            if column.default is None:
                insert_columns.append(column)
        elif not column.nullable and column.default is None:
            raise ValueError(f"Required table column has no CSV source: {column.name}.")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    data = []
    for row in report.rows:
        values = []
        for column in insert_columns:
            norm = normalize_col(column.name)
            value = now if norm in AUDIT_COLUMNS else row.values.get(norm)
            values.append(sql_value(value, column, row.line))
        data.append(tuple(values))
    return insert_columns, data


def key_batches(columns, data):
    indices = [next(i for i, c in enumerate(columns) if normalize_col(c.name) == key)
               for key in KEY_COLUMNS]
    predicate = "(" + " AND ".join(
        f"{quote_column(columns[i].name)} = ?" for i in indices
    ) + ")"
    size = MAX_PARAMS // len(indices)
    for start in range(0, len(data), size):
        batch = data[start:start + size]
        yield " OR ".join([predicate] * len(batch)), [
            row[i] for row in batch for i in indices
        ]


def write_rows(conn, cursor, table, columns, data):
    """Commit keyed replacement once; every failed write rolls back the load."""
    try:
        cursor.execute("SET XACT_ABORT ON")
        cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        deleted = 0
        for predicate, params in key_batches(columns, data):
            cursor.execute(f"DELETE FROM {table} WHERE {predicate}", params)
            deleted += max(0, cursor.rowcount)
        names = ", ".join(quote_column(c.name) for c in columns)
        marks = "(" + ", ".join(["?"] * len(columns)) + ")"
        size = min(1000, MAX_PARAMS // len(columns))
        for start in range(0, len(data), size):
            batch = data[start:start + size]
            cursor.execute(
                f"INSERT INTO {table} ({names}) VALUES "
                + ", ".join([marks] * len(batch)),
                [value for row in batch for value in row],
            )
            print(f"  inserted {start + len(batch):,} / {len(data):,}", flush=True)
        conn.commit()
        return deleted
    except Exception:
        conn.rollback()
        raise


def fetch_count(cursor) -> int:
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("SQL Server returned no row for a COUNT_BIG query.")
    return int(row[0])


def load_report(report, table, check_only=False):
    schema, name, qualified = qualified_table(table)
    # The local dry run and self-check do not import pyodbc or connect.
    sys.path.insert(0, str(ZENOTI_DIR))
    from db_helper import get_connection

    conn = get_connection()
    cursor = None
    try:
        cursor = conn.cursor()
        columns = table_columns(cursor, schema, name)
        if not columns:
            raise ValueError("Configured feedback destination does not exist or is not visible.")
        insert_columns, data = prepare_rows(report, columns)
        cursor.execute(f"SELECT COUNT_BIG(*) FROM {qualified}")
        total = fetch_count(cursor)
        matching = 0
        for predicate, params in key_batches(insert_columns, data):
            cursor.execute(f"SELECT COUNT_BIG(*) FROM {qualified} WHERE {predicate}", params)
            matching += fetch_count(cursor)
        print(f"  columns  {len(SOURCE_COLUMNS)} CSV columns mapped and validated")
        print(f"  stored   {total:,} row(s); {matching:,} match this file's invoices")
        if check_only:
            print("  check-db passed; no rows written.")
            return 0
        deleted = write_rows(conn, cursor, qualified, insert_columns, data)
        print(f"  wrote    {len(data):,} row(s); replaced {deleted:,} stored row(s)")
        return 0
    finally:
        if cursor is not None:
            cursor.close()
        conn.close()


def self_check():
    def export(rows):
        stream = io.StringIO(newline="")
        writer = csv.writer(stream, quoting=csv.QUOTE_ALL)
        writer.writerow(SOURCE_COLUMNS)
        writer.writerows(rows)
        return stream.getvalue()

    row = ["Example Center", "INV-1", "Example Guest", "9/4/2026", "5",
           'Excellent, including "detail".\nThank you.', "", "Example Provider",
           "Closed", "Service A,Service B", "Category", "Subcategory", "No", "Yes", "Web"]
    report = read_feedback(io.StringIO("\ufeff" + export([row, row]) + "\n"))
    assert len(report.rows) == 1 and report.duplicate_rows == 1
    assert report.blank_rows == 1
    assert report.rows[0].values["sale_date"] == date(2026, 9, 4)
    assert report.rows[0].values["rating"] == 5
    assert report.rows[0].values["guest_comments"] == row[5]
    assert report.rows[0].values["tags"] is None

    for index, name in ((0, "center_name"), (1, "invoice_no"),
                        (3, "sale_date"), (4, "rating")):
        missing = row.copy()
        missing[index] = " "
        try:
            read_feedback(io.StringIO(export([missing])))
        except ValueError as exc:
            assert f"{name} is required" in str(exc)
        else:
            raise AssertionError(f"Missing {name} must abort.")
    blank_flags = row.copy()
    blank_flags[12] = blank_flags[13] = ""
    optional = read_feedback(io.StringIO(export([blank_flags])))
    assert optional.rows[0].values["first_visit"] is None
    assert optional.rows[0].values["member"] is None

    broken = row.copy()
    broken[5] = 'Comment with "quoted words", commas, and Unicode \u2605.'
    raw = export([broken]).replace('""quoted words""', '"quoted words"')
    recovered = read_feedback(io.StringIO(raw))
    assert recovered.repaired_lines == [2]
    assert recovered.rows[0].values["guest_comments"] == broken[5]
    assert recovered.rows[0].values["service"] == broken[9]
    for invalid in ("bad date", "13/40/2026"):
        bad = row.copy()
        bad[3] = invalid
        try:
            read_feedback(io.StringIO(export([bad])))
        except ValueError:
            pass
        else:
            raise AssertionError("Invalid dates must abort.")
    conflict = row.copy()
    conflict[4] = "1"
    try:
        read_feedback(io.StringIO(export([row, conflict])))
    except ValueError:
        pass
    else:
        raise AssertionError("Conflicting invoice feedback must abort.")

    columns = [Column(name.upper(), "date" if name == "sale_date"
                      else "int" if name == "rating" else "nvarchar", -1)
               for name in SOURCE_COLUMNS]
    columns.append(Column("DATE_INSERTED", "datetime2", nullable=False,
                          default="(sysutcdatetime())"))
    insert_columns, data = prepare_rows(recovered, columns)
    assert len(insert_columns) == 15 and data[0][3] == date(2026, 9, 4)

    class FakeConnection:
        def __init__(self):
            self.committed = self.rolled_back = False

        def commit(self):
            self.committed = True

        def rollback(self):
            self.rolled_back = True

    class FakeCursor:
        rowcount = 1

        def __init__(self, fail=False):
            self.calls, self.fail = [], fail

        def execute(self, sql, params=()):
            self.calls.append((sql, params))
            assert len(params) <= MAX_PARAMS
            assert sql.count("?") == len(params)
            if self.fail and sql.startswith("INSERT"):
                raise RuntimeError("Simulated insert failure.")

    # Cross both DELETE and INSERT batch boundaries.
    conn, cursor = FakeConnection(), FakeCursor()
    with redirect_stdout(io.StringIO()):
        write_rows(conn, cursor, "[dbo].[example]", insert_columns, data * 1001)
    assert conn.committed and not conn.rolled_back
    inserts = [params for sql, params in cursor.calls if sql.startswith("INSERT")]
    deletes = [params for sql, params in cursor.calls if sql.startswith("DELETE")]
    assert sum(map(len, inserts)) == 1001 * 15
    assert list(map(len, deletes)) == [2000, 2]
    conn, cursor = FakeConnection(), FakeCursor(fail=True)
    try:
        write_rows(conn, cursor, "[dbo].[example]", insert_columns, data)
    except RuntimeError:
        pass
    else:
        raise AssertionError("The simulated failure must propagate.")
    assert conn.rolled_back and not conn.committed

    class FakeCountCursor:
        def __init__(self, row):
            self.row = row

        def fetchone(self):
            return self.row

    assert fetch_count(FakeCountCursor((3,))) == 3
    try:
        fetch_count(FakeCountCursor(None))
    except RuntimeError as exc:
        assert "COUNT_BIG" in str(exc)
    else:
        raise AssertionError("A missing count result must abort.")

    print("feedback_report self-check passed.")
    return 0


def main(argv=None):
    load_dotenv(ZENOTI_DIR / ".env")
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", help="local CSV file or directory; overrides CSV_SOURCE")
    parser.add_argument("--source", choices=("local", "gdrive"),
                        help="override CSV_SOURCE for this report")
    parser.add_argument("--gdrive-folder", help="override GDRIVE_FOLDER_FEEDBAC_REPORT")
    parser.add_argument("--table", default=os.getenv("TABLE_FEEDBACK_REPORT") or DEFAULT_TABLE,
                        help="target table (default: TABLE_FEEDBACK_REPORT from Zenoti/.env)")
    parser.add_argument("--dry-run", action="store_true", help="validate source CSVs; no DB")
    parser.add_argument("--check-db", action="store_true", help="validate live schema; read-only")
    parser.add_argument("--self-check", action="store_true", help="run built-in checks; no DB")
    args = parser.parse_args(argv)
    try:
        if args.self_check:
            return self_check()
        qualified_table(args.table)
        paths = source_paths(args)
        if not paths:
            print("SKIP: No feedback CSV files available; no rows written.")
            return 0
        # Validate every file before any earlier file can be committed.
        reports = [(path, parse_csv(path)) for path in paths]
        print(f"  report   {REPORT_LABEL}")
        print(f"  files    {len(reports):,}")
        dry_only = args.dry_run and not args.check_db
        for path, report in reports:
            dates = [row.values["sale_date"] for row in report.rows]
            print(f"  file     {path.resolve()}")
            print(f"  rows     {len(report.rows):,}; sale dates {min(dates)} .. {max(dates)}")
            if report.repaired_lines:
                print(f"  repaired unescaped comment quotes at line(s): "
                      + ", ".join(map(str, report.repaired_lines)))
            if report.blank_rows or report.duplicate_rows:
                print(f"  skipped  {report.blank_rows:,} blank and "
                      f"{report.duplicate_rows:,} identical duplicate record(s)")
            if not dry_only:
                status = load_report(report, args.table, check_only=args.check_db)
                if status != 0:
                    return status
        if args.dry_run and not args.check_db:
            print("  dry-run passed; no database connection made.")
        return 0
    except Exception as exc:
        print(f"ERROR: {public_error(str(exc), args.table)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
