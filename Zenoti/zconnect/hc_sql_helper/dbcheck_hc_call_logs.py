"""Read-only check of what is actually stored in HC Call Logs.

For answering "did that load land?" and "is the loader really deduping?" without
reading the load log. Nothing here writes.

Worth being clear about what the loader does, because a load log reads as if it
re-ingests every time:

    wrote   11,069 row(s), 11,069 replaced   (key: CALL_ID)

"replaced" is the count of rows the DELETE matched, found by looking up
CALL_ID in the table. Every row was already there, so every row was found, so
every row was deleted and written back -- 11,069 in, 11,069 out. It is a
refresh of that window, not an append: the row count after the load is the row
count before it, plus anything genuinely new. This script is how you see that
from the table side rather than taking the log's word for it.

Three things it reports, in the order they would catch a problem:

  * duplicate rows. The key is CALL_ID and the table's primary key enforces it,
    so this is the check that the dedup mechanism is actually in place -- a
    table with no unique index on the key is one where every run appends.
  * day coverage. One row per call, bucketed by day, so a day loaded twice is
    visible as a doubled count and a missed day as a gap. Bucketed on
    TIMESTAMP_UTC, because that is what the API's start_date/end_date bound;
    TIMESTAMP_LOCAL is a display column and 22 rows in a real 16,784 sit on a
    different local day than UTC day.
  * with --csv, the CSV against the table. Which of the CSV's calls are not in
    the table (the load dropped them), day by day and CALL_ID by CALL_ID.

    python dbcheck_hc_call_logs.py                     # counts, days, dupes
    python dbcheck_hc_call_logs.py --csv call_logs.csv # also compare to a pull
    python dbcheck_hc_call_logs.py --days 60           # how many days to list
    python dbcheck_hc_call_logs.py --self-check        # no DB: the compare logic
"""
import argparse
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))          # hc_sql_helper/, this folder
_WORKDIR = os.path.dirname(_HERE)                            # zconnect/
_ROOT = os.path.dirname(_WORKDIR)                            # Zenoti/

sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "dbquery"))

import pyodbc  # noqa: E402
from db_helper import get_connection  # noqa: E402
import import_aesthetix_opportunity_history as base  # noqa: E402
import import_hc_report as ihr  # noqa: E402

# Both are datetimes on the row; UTC is the one the pull's window binds to.
DAY_COLUMNS = ("TIMESTAMP_UTC", "TIMESTAMP_LOCAL")


def day_counts(rows, column):
    """{YYYY-MM-DD: n} for one timestamp column of CSV rows (normalized keys)."""
    norm = base.normalize_col(column)
    counts = {}
    for row in rows:
        value = str(row.get(norm) or "")
        if len(value) >= 10:
            counts[value[:10]] = counts.get(value[:10], 0) + 1
    return counts


def split_keys(csv_ids, db_ids):
    """(in the CSV but not the table, in the table but not the CSV)."""
    csv_ids, db_ids = set(csv_ids), set(db_ids)
    return sorted(csv_ids - db_ids), sorted(db_ids - csv_ids)


def existing_ids(cursor, table, ids):
    """Which of `ids` are in the table, asked in batches that stay under the
    2100-parameter cap -- ihr.chunked is the same batching the loader uses."""
    found = set()
    for chunk in ihr.chunked(list(ids), 1):
        marks = ", ".join("?" * len(chunk))
        cursor.execute(f"SELECT CALL_ID FROM {table} WHERE CALL_ID IN ({marks})", chunk)
        found.update(row[0] for row in cursor.fetchall())
    return found


def _self_check():
    """The comparison is the part that would quietly report "all good" -- pin it."""
    rows = [{"timestamp_utc": "2026-09-01 10:00:00"}, {"timestamp_utc": "2026-09-01 23:00:00"},
            {"timestamp_utc": "2026-09-02 01:00:00"}, {"timestamp_utc": ""}, {}]
    assert day_counts(rows, "timestamp_utc") == {"2026-09-01": 2, "2026-09-02": 1}, \
        day_counts(rows, "timestamp_utc")

    # Both directions, and neither mistaken for the other.
    assert split_keys(["a", "b"], ["b", "c"]) == (["a"], ["c"])
    assert split_keys(["a"], ["a"]) == ([], [])
    assert split_keys([], []) == ([], [])

    # A batch boundary must not lose or invent a row.
    assert len(ihr.chunked(list(range(2500)), 1)) == 3
    print("self-check OK")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Read-only check of HC Call Logs.")
    parser.add_argument("--csv", default=None, help="a pulled call_logs CSV to compare against")
    parser.add_argument("--days", type=int, default=0, help="list at most this many days (0 = all)")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()

    if args.self_check:
        return _self_check()

    table = os.getenv(ihr.REPORTS["call_logs"]["env"]) or ihr.REPORTS["call_logs"]["table"]
    print(f"  server  {ihr.SERVER}")
    print(f"  db      {ihr.DATABASE}")
    print(f"  table   {table}")

    conn = get_connection()
    failed = 0
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT c.name FROM sys.columns AS c WHERE c.object_id = OBJECT_ID(?)",
            [table],
        )
        columns = {row[0].upper() for row in cursor.fetchall()}
        if "CALL_ID" not in columns:
            print("  FATAL   no CALL_ID column -- this is not the call logs table")
            return 1
        day_col = next((c for c in DAY_COLUMNS if c in columns), None)

        cursor.execute(f"SELECT COUNT(*), COUNT(DISTINCT CALL_ID) FROM {table}")
        counted = cursor.fetchone()
        # COUNT(*) always returns a row; the fallback is for the table going away
        # between the column read above and here.
        total, distinct = (counted[0], counted[1]) if counted else (0, 0)
        print(f"  rows    {total:,} row(s), {distinct:,} distinct CALL_ID"
              f"  ->  {total - distinct:,} duplicate row(s)")

        # What makes a rerun a refresh rather than an append.
        cursor.execute(
            "SELECT i.name FROM sys.indexes AS i JOIN sys.index_columns AS ic "
            "ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
            "WHERE i.object_id = OBJECT_ID(?) AND i.is_unique = 1 AND ic.is_included_column = 0 "
            "GROUP BY i.name HAVING COUNT(*) = 1 AND MAX(COL_NAME(ic.object_id, ic.column_id)) = 'CALL_ID'",
            [table],
        )
        unique_on_id = [row[0] for row in cursor.fetchall()]
        if unique_on_id:
            print(f"  key     CALL_ID, unique via {', '.join(unique_on_id)} -- a rerun replaces, never appends")
        else:
            print("  key     CALL_ID has no unique index: nothing stops two runs inserting the same call")

        days, undated = [], 0
        if day_col:
            cursor.execute(
                f"SELECT CONVERT(varchar(10), {day_col}, 23) AS d, COUNT(*) FROM {table} "
                f"GROUP BY CONVERT(varchar(10), {day_col}, 23) ORDER BY d"
            )
            days = [tuple(row) for row in cursor.fetchall()]
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {day_col} IS NULL")
            nulls = cursor.fetchone()
            undated = nulls[0] if nulls else 0
            span = f", {days[0][0]} .. {days[-1][0]}" if days else ""
            print(f"  days    {len(days)} day(s) in {day_col}{span}  ({undated:,} row(s) undated)")
            shown = days[-args.days:] if args.days else days
            for day, count in shown:
                print(f"          {day}  {count:>6,}")
            if len(shown) < len(days):
                print(f"          ... {len(days) - len(shown)} earlier day(s) not listed")
        else:
            print("  days    no timestamp column to bucket by")

        if not args.csv:
            print("\n  read-only. Pass --csv to compare against a pulled file.")
            return 0

        path = args.csv
        if not os.path.exists(os.path.join(_WORKDIR, path)) and not os.path.exists(path):
            print(f"\n  FAILED  no such CSV: {path}")
            return 1
        if not os.path.exists(path):
            path = os.path.join(_WORKDIR, path)
        csv_rows, _ = base.parse_csv(path)
        csv_ids = [str(row.get("call_id") or "").strip() for row in csv_rows]
        csv_ids = [c for c in csv_ids if c]
        print(f"\n  csv     {os.path.basename(path)}  {len(csv_rows):,} row(s)")

        if day_col:
            # Only the days the CSV itself covers: a day the CSV has none of is
            # not a missing day, it is a day this pull was never about.
            stored = dict(days)
            for day, count in sorted(day_counts(csv_rows, day_col).items()):
                have = stored.get(day, 0)
                mark = "OK  " if have == count else "DIFF"
                print(f"  {mark}    {day}  csv {count:>6,}  table {have:>6,}"
                      + ("" if have == count else f"  ({have - count:+,})"))

        in_db = existing_ids(cursor, table, csv_ids)
        missing, _ = split_keys(csv_ids, in_db)
        print(f"\n  csv in table   {len(csv_ids) - len(missing):,} of {len(csv_ids):,}"
              f"  ->  {len(missing):,} not stored")
        if missing:
            print(f"          e.g. {missing[:3]}")
        else:
            print("\n  OK      every call in the CSV is stored under its own CALL_ID")
    except pyodbc.Error as err:
        failed = 1
        print(f"\n  FAILED\n{ihr._sql_message(err)}")
    finally:
        conn.close()
    return failed


if __name__ == "__main__":
    sys.exit(main())
