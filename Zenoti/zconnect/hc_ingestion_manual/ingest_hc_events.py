"""Load the review CSV from zconnect/get_events.py into TABLE_HC_EVENTS.

    python Zenoti/zconnect/hc_ingestion_manual/ingest_hc_events.py --dry-run
    python Zenoti/zconnect/hc_ingestion_manual/ingest_hc_events.py --check-db
    python Zenoti/zconnect/hc_ingestion_manual/ingest_hc_events.py
    python Zenoti/zconnect/hc_ingestion_manual/ingest_hc_events.py --csv path/to/reviews.csv
    python Zenoti/zconnect/hc_ingestion_manual/ingest_hc_events.py --self-check

Defaults to the newest reviews_*.csv in Zenoti/zconnect/output/. Credentials
and TABLE_HC_EVENTS come from Zenoti/.env. --dry-run validates the CSV without
connecting; --check-db also validates the destination, without changing data.

Reviews are inserted or refreshed by review_key in one transaction. An older
event cannot replace a newer stored review. Repeated keys in the CSV keep the
latest event version. Other reviews in the table are retained. New rows receive
date_inserted = SYSUTCDATETIME(); updates preserve their original insertion
timestamp. No table schema is changed and the source CSV is retained.

The log's "refreshed" count includes existing reviews with equal event timestamps,
even when their values are unchanged. Re-fetching an overlapping date window
therefore commonly refreshes reviews that were already loaded.

Unicode text, whitespace, multiline comments, blank comments and JSON are
preserved. Optional empty identifiers, dates and ratings become SQL NULL.
ISO timestamps with offsets are converted to UTC for datetime2; timestamps
without offsets are stored as provided. The timeline HC loader uses a different
schema; use this command for the review output.
"""

from __future__ import annotations

import argparse
from collections import Counter
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, TextIO
from uuid import UUID


HERE = Path(__file__).resolve().parent
ZENOTI_DIR = HERE.parent.parent  # hc_ingestion_manual/ -> zconnect/ -> Zenoti/
# Import from the service root as well as the repository checkout: the
# zconnect Docker image copies Zenoti/'s contents directly into /app/.
sys.path.insert(0, str(ZENOTI_DIR))
from zconnect.get_events import COLUMNS


Row = dict[str, Any]
DATE_COLUMNS = {"review_date", "event_date"}
ID_COLUMNS = {"center_id", "guest_id"}
REQUIRED_COLUMNS = {"center_id", "review_key", "event_id", "metadata_index", "metadata_json"}
BATCH_SIZE = 200
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")


def qualified_table(value: str) -> tuple[str, str, str]:
    parts = value.strip().split(".")
    if len(parts) == 1:
        parts.insert(0, "dbo")
    if len(parts) != 2:
        raise ValueError("TABLE_HC_EVENTS must be a table or schema.table in the current database.")
    names: list[str] = []
    for part in parts:
        part = part.strip()
        if part.startswith("[") and part.endswith("]"):
            part = part[1:-1]
        if not IDENTIFIER.fullmatch(part):
            raise ValueError("TABLE_HC_EVENTS contains an invalid SQL identifier.")
        names.append(part)
    return names[0], names[1], f"[{names[0]}].[{names[1]}]"


def parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def read_reviews(handle: TextIO) -> tuple[list[Row], int, int]:
    """Parse/validate all CSV records and keep the latest version of each key."""
    reader = csv.DictReader(handle)
    if reader.fieldnames != list(COLUMNS):
        raise ValueError("CSV headers must exactly match zconnect/get_events.py's review columns.")
    reviews: dict[str, Row] = {}
    records = 0
    for records, raw in enumerate(reader, 1):
        if None in raw or any(value is None for value in raw.values()):
            raise ValueError(f"CSV record {records}: wrong number of fields.")
        row: Row = {}
        for column in COLUMNS:
            value = raw[column]
            if value is None:
                raise ValueError(f"CSV record {records}, column {column}: missing field.")
            try:
                if not value and column in REQUIRED_COLUMNS:
                    raise ValueError("a required value is blank")
                if column in ID_COLUMNS:
                    row[column] = str(UUID(value)) if value else None
                elif column == "review_key":
                    if not re.fullmatch(r"[0-9a-f]{64}", value):
                        raise ValueError("expected a lowercase SHA-256 review key")
                    row[column] = value
                elif column == "metadata_index":
                    if not value.isascii() or not value.isdigit() or int(value) > 2147483647:
                        raise ValueError("expected a nonnegative SQL int")
                    row[column] = int(value)
                elif column == "rating":
                    if value and value not in ("1", "2", "3", "4", "5"):
                        raise ValueError("expected a rating from 1 to 5 or blank")
                    row[column] = int(value) if value else None
                elif column in DATE_COLUMNS:
                    row[column] = parse_timestamp(value)
                elif column == "metadata_json":
                    if not isinstance(json.loads(value), (dict, list)):
                        raise ValueError("expected a JSON object or array")
                    row[column] = value
                else:
                    row[column] = value  # Keep text verbatim, including blank comments.
            except (ValueError, TypeError) as exc:
                raise ValueError(f"CSV record {records}, column {column}: {exc}") from None
        previous = reviews.get(row["review_key"])
        if previous is None or (row["event_date"] or datetime.min) > (previous["event_date"] or datetime.min):
            reviews[row["review_key"]] = row
    return [reviews[key] for key in sorted(reviews)], records, records - len(reviews)


@dataclass(frozen=True)
class ColumnInfo:
    kind: str
    length: int | None


def validate_destination(cursor: Any, schema: str, name: str, table: str, rows: list[Row]) -> None:
    cursor.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH "
        "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        (schema, name),
    )
    columns = {row[0].lower(): ColumnInfo(row[1].lower(), row[2]) for row in cursor.fetchall()}
    expected = {*COLUMNS, "date_inserted"}
    if set(columns) != expected:
        missing, extra = sorted(expected - set(columns)), sorted(set(columns) - expected)
        raise ValueError(f"{table} must have the review schema; missing: {missing}; extra: {extra}.")
    for column, info in columns.items():
        allowed = {"nvarchar", "nchar"}
        if column in ID_COLUMNS:
            allowed = {"uniqueidentifier", "nvarchar", "varchar", "char", "nchar"}
        elif column == "review_key":
            allowed = {"char", "varchar", "nchar", "nvarchar"}
        elif column in DATE_COLUMNS or column == "date_inserted":
            allowed = {"datetime2"}
        elif column in {"rating", "metadata_index"}:
            allowed = {"tinyint", "smallint", "int", "bigint"}
        if info.kind not in allowed:
            raise ValueError(f"{table}.{column} has incompatible type {info.kind}.")
    cursor.execute(
        "SELECT i.name FROM sys.indexes AS i "
        "JOIN sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
        "JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        "WHERE i.object_id = OBJECT_ID(?) AND i.is_unique = 1 AND i.has_filter = 0 "
        "AND ic.key_ordinal > 0 GROUP BY i.name "
        "HAVING COUNT(*) = 1 AND MAX(c.name) = 'review_key'", (table,),
    )
    if cursor.fetchone() is None:
        raise ValueError(f"{table} needs a primary key or unique index on review_key.")
    bounds = {"tinyint": (0, 255), "smallint": (-32768, 32767),
              "int": (-2147483648, 2147483647), "bigint": (-9223372036854775808, 9223372036854775807)}
    for record, row in enumerate(rows, 1):
        for column in COLUMNS:
            value, info = row[column], columns[column]
            if value is None:
                continue
            if isinstance(value, str) and info.length is not None and info.length > 0:
                length = len(value.encode("utf-16-le")) // 2 if info.kind in {"nvarchar", "nchar"} else len(value)
                if length > info.length:
                    raise ValueError(f"Review {record}, column {column}: exceeds SQL capacity ({info.length}).")
            if info.kind in bounds:
                if not isinstance(value, int) or not bounds[info.kind][0] <= value <= bounds[info.kind][1]:
                    raise ValueError(f"Review {record}, column {column}: exceeds SQL numeric capacity.")


def apply_reviews(cursor: Any, table: str, rows: list[Row]) -> tuple[int, int, int]:
    """Load typed JSON batches; the caller commits or rolls back all changes."""
    if not rows:
        return 0, 0, 0
    fields = ", ".join(f"[{column}]" for column in COLUMNS)
    cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
    stage_types = {"review_key": "char(64)", "metadata_index": "int", "rating": "int",
                   **{column: "uniqueidentifier" for column in ID_COLUMNS},
                   **{column: "datetime2(7)" for column in DATE_COLUMNS}}
    definitions = ", ".join(f"[{column}] {stage_types.get(column, 'nvarchar(max)')} "
                            f"{'NOT NULL' if column in REQUIRED_COLUMNS else 'NULL'} "
                            f"{'PRIMARY KEY' if column == 'review_key' else ''}" for column in COLUMNS)
    json_fields = ", ".join(f"[{column}] {stage_types.get(column, 'nvarchar(max)')} '$.{column}'"
                             for column in COLUMNS)
    assignments = ", ".join(f"t.[{column}] = s.[{column}]" for column in COLUMNS if column != "review_key")
    statement = f"""SET NOCOUNT ON;
        DECLARE @payload nvarchar(max) = ?;
        DECLARE @reviews TABLE ({definitions});
        DECLARE @updated int, @inserted int;
        INSERT INTO @reviews ({fields}) SELECT {fields} FROM OPENJSON(@payload) WITH ({json_fields});
        UPDATE t SET {assignments}
        FROM {table} AS t WITH (UPDLOCK, HOLDLOCK)
        JOIN @reviews AS s ON s.[review_key] = t.[review_key]
        WHERE t.[event_date] IS NULL OR s.[event_date] >= t.[event_date];
        SET @updated = @@ROWCOUNT;
        INSERT INTO {table} ({fields}, [date_inserted])
        SELECT {', '.join('s.[' + column + ']' for column in COLUMNS)}, SYSUTCDATETIME()
        FROM @reviews AS s
        WHERE NOT EXISTS (SELECT 1 FROM {table} AS t WITH (UPDLOCK, HOLDLOCK)
                          WHERE t.[review_key] = s.[review_key]);
        SET @inserted = @@ROWCOUNT;
        SELECT @inserted AS inserted, @updated AS updated;
    """
    inserted, updated = 0, 0
    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start:start + BATCH_SIZE]
        serializable = [{column: value.isoformat() if isinstance(value, datetime) else value
                         for column, value in row.items()} for row in batch]
        cursor.execute(statement, json.dumps(serializable, ensure_ascii=False))
        while cursor.description is None:
            if not cursor.nextset():
                raise RuntimeError("Review ingestion returned no row counts.")
        counts = cursor.fetchone()
        if counts is None:
            raise RuntimeError("Review ingestion returned empty row counts.")
        inserted += int(counts[0])
        updated += int(counts[1])
        while cursor.nextset():
            pass
    return inserted, updated, len(rows) - inserted - updated


def self_check() -> int:
    row: dict[str, str] = {column: "" for column in COLUMNS}
    row.update(center_id="00000000-0000-0000-0000-000000000001", review_key="a" * 64,
               event_id="12345678", metadata_index="0", rating="5", comments='  Great, "care"!\n\u2605  ',
               review_date="2023-02-23T19:33:25", event_date="2026-11-01T14:55:22.123",
               source="GOOGLE", metadata_json='{"review_text":"  Original text  "}')

    def fixture(records: list[dict[str, str]]) -> StringIO:
        handle = StringIO(newline="")
        writer: csv.DictWriter[str] = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(records)
        handle.seek(0)
        return handle

    older = dict(row, comments="old", event_date="2026-10-01T12:00:00")
    newer = dict(row, comments="latest", event_date="2026-11-02T12:00:00Z")
    native = dict(row, review_key="b" * 64, source="ZENOTI", comments="", rating="",
                  review_reply_status="False")
    parsed, total, duplicates = read_reviews(fixture([newer, older, row, native]))
    assert total == 4 and duplicates == 2 and len(parsed) == 2
    assert parsed[0]["comments"] == "latest" and parsed[1]["comments"] == ""
    assert parsed[1]["rating"] is None and parsed[1]["guest_id"] is None
    assert parsed[1]["review_reply_status"] == "False"
    fidelity, _, _ = read_reviews(fixture([row]))
    assert fidelity[0]["comments"] == row["comments"] and fidelity[0]["metadata_json"] == row["metadata_json"]
    assert parse_timestamp("2026-11-01T14:00:00-05:00") == datetime(2026, 11, 1, 19)
    for column, value in (("rating", "-1"), ("metadata_index", "-1"), ("center_id", "bad-id"),
                          ("event_id", ""), ("metadata_json", "oops"), ("event_date", "not-a-date")):
        try:
            read_reviews(fixture([dict(row, **{column: value})]))
        except ValueError:
            pass
        else:
            raise AssertionError(f"Invalid {column} was accepted")
    try:
        read_reviews(StringIO("centerid,send_date\nold,2026-11-01\n"))
    except ValueError:
        pass
    else:
        raise AssertionError("The old timeline CSV was accepted")
    for value in ("dbo.EVENTS; DELETE FROM x", "otherdb.dbo.EVENTS", "dbo.E'VENTS", ""):
        try:
            qualified_table(value)
        except ValueError:
            pass
        else:
            raise AssertionError("An unsafe SQL identifier was accepted")
    assert qualified_table("[dbo].[EVENTS]")[2] == "[dbo].[EVENTS]"
    print("ingest_hc_events self-check passed; no database connection or files written.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", type=Path, help="review CSV; default newest zconnect/output/reviews_*.csv")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="validate the CSV; no database connection")
    mode.add_argument("--check-db", action="store_true", help="validate CSV and destination; no writes")
    parser.add_argument("--self-check", action="store_true", help="offline checks; no database connection")
    args = parser.parse_args(argv)
    connection = None
    try:
        if args.self_check:
            return self_check()
        from dotenv import load_dotenv

        load_dotenv(ZENOTI_DIR / ".env")
        configured = os.getenv("TABLE_HC_EVENTS")
        if not configured:
            raise ValueError("Set TABLE_HC_EVENTS in Zenoti/.env to the review destination table.")
        schema, name, table = qualified_table(configured)
        path = args.csv
        if path is None:
            available = list((ZENOTI_DIR / "zconnect" / "output").glob("reviews_*.csv"))
            if not available:
                raise ValueError("No review CSV in zconnect/output; pass --csv or run get_events.py first.")
            path = max(available, key=lambda candidate: (candidate.stat().st_mtime_ns, candidate.name))
        with path.open(encoding="utf-8-sig", newline="") as handle:
            rows, records, duplicates = read_reviews(handle)
        print(f"CSV: {path.resolve()}")
        print(f"Target: {table}; {len(rows):,} reviews from {records:,} CSV records; {duplicates:,} repeated versions")
        print(f"Reviews by source: {dict(Counter(row['source'] or 'unknown' for row in rows))}")
        if args.dry_run:
            print("Dry-run passed; no database connection or writes.")
            return 0
        if not rows and not args.check_db:
            print("No reviews to ingest.")
            return 0
        sys.path.insert(0, str(ZENOTI_DIR))
        from db_helper import get_connection

        connection = get_connection()
        connection.autocommit = False
        cursor = connection.cursor()
        validate_destination(cursor, schema, name, table, rows)
        if args.check_db:
            cursor.execute(f"SELECT COUNT_BIG(*) FROM {table}")
            count_row = cursor.fetchone()
            if count_row is None:
                raise RuntimeError("Destination inspection returned no row count.")
            print(f"Destination check passed: {count_row[0]:,} existing rows; all CSV values fit. No writes.")
        else:
            inserted, updated, older = apply_reviews(cursor, table, rows)
            connection.commit()
            print(f"Ingested: {inserted:,} inserted, {updated:,} refreshed, {older:,} older versions skipped.")
        cursor.close()
        return 0
    except Exception as exc:
        if connection is not None:
            connection.rollback()
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    sys.exit(main())
