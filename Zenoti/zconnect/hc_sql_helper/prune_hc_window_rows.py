"""One-off: remove the pre-daily HC rows, so the daily pull starts clean.

READ-ONLY unless you pass --apply. Run it once, when the two dashboard reports
switch to day grain, then delete it -- nothing imports it.

Why it is needed: until 2026-10-17 agent_metrics and location_performance stored
one row per agent/center per *window*, with start_date / end_date holding the
range asked for (a 7-day lookback run = one row stamped 2026-10-09..2026-10-15).
Both are now pulled one day at a time, so every row carries its own day in all
three date columns. The loader only ever deletes the keys it is inserting, so
the old window rows would sit in the table forever beside the daily ones -- and
a dashboard summing a month would count those overlapping windows as well.

There is nothing to reconstruct: a window row is a total for a range, not a set
of days, and the dashboard routes have no range cap (365 days came back fine),
so any window can simply be re-pulled at day grain.

    python prune_hc_window_rows.py              # report and print the SQL
    python prune_hc_window_rows.py --apply      # run it
    python prune_hc_window_rows.py --self-check # no DB: check the predicates
"""
import argparse
import os
import sys
import textwrap

_HERE = os.path.dirname(os.path.abspath(__file__))          # hc_sql_helper/, this folder
_WORKDIR = os.path.dirname(_HERE)                            # zconnect/
_ROOT = os.path.dirname(_WORKDIR)                            # Zenoti/

sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "dbquery"))

import pyodbc  # noqa: E402
from db_helper import get_connection  # noqa: E402
import import_hc_report as ihr  # noqa: E402  (its REPORTS table, and .env loading)

# "not a daily row", per report. agent_metrics carries `date`, so a daily row is
# one where all three date columns agree; location_performance has no `date`
# column, so start_date = end_date is the whole test. Anything else is what the
# window pulls left behind.
NOT_DAILY = {
    "agent_metrics": "NOT ([DATE] IS NOT NULL AND [START_DATE] = [DATE] AND [END_DATE] = [DATE])",
    "location_performance": "NOT ([START_DATE] = [END_DATE])",
}


def count_sql(table, predicate):
    return f"SELECT COUNT(*) FROM {table} WHERE {predicate}"


def delete_sql(table, predicate):
    return f"DELETE FROM {table} WHERE {predicate}"


def self_check():
    """Run the real predicate against a throwaway SQLite copy of the shape.

    The predicate is the part that deletes rows, so it is not read back and
    eyeballed: one daily row and one window row go in, the DELETE runs, and the
    daily row has to still be there. SQLite takes the same predicate once the
    brackets come off, which is why this needs no server.
    """
    import sqlite3

    for report, source in NOT_DAILY.items():
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE t (start_date TEXT, end_date TEXT, date TEXT)")
        conn.execute("INSERT INTO t VALUES ('2026-10-14', '2026-10-14', '2026-10-14')")
        if "date" in source:                      # the date-carrying report
            conn.execute("INSERT INTO t VALUES ('2026-10-09', '2026-10-15', '2026-10-14')")
            conn.execute("INSERT INTO t VALUES ('2026-10-09', '2026-10-15', NULL)")
        else:
            conn.execute("INSERT INTO t VALUES ('2026-10-09', '2026-10-15', NULL)")
        conn.execute(f"DELETE FROM t WHERE {source.replace('[', '').replace(']', '')}")
        rows = conn.execute("SELECT start_date, end_date, date FROM t").fetchall()
        conn.close()
        expected = [("2026-10-14", "2026-10-14", "2026-10-14")]
        assert rows == expected, (report, rows)

    # This one-off migration only targets the two dashboard reports that
    # changed from window grain to day grain. Feedback and events are not part
    # of that migration.
    assert set(NOT_DAILY) == {"agent_metrics", "location_performance"}, set(NOT_DAILY)
    assert set(NOT_DAILY) <= set(ihr.REPORTS), set(NOT_DAILY) - set(ihr.REPORTS)
    print("self-check OK")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Remove the pre-daily HC window rows.")
    parser.add_argument("--apply", action="store_true", help="run the DELETE rather than print it")
    parser.add_argument("--report", choices=sorted(NOT_DAILY), default=None)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()

    if args.self_check:
        return self_check()

    print(f"  server  {ihr.SERVER}")
    print(f"  db      {ihr.DATABASE}")
    print(f"  mode    {'APPLY, rows will be deleted' if args.apply else 'read-only, printing the SQL'}")

    conn = get_connection()
    failed = 0
    try:
        cursor = conn.cursor()
        for report in ([args.report] if args.report else sorted(NOT_DAILY)):
            spec = ihr.REPORTS[report]
            table = os.getenv(spec["env"]) or spec["table"]
            predicate = NOT_DAILY[report]
            cursor.execute(count_sql(table, predicate))
            stale = cursor.fetchone()[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total = cursor.fetchone()[0]
            print(f"\n  {spec['label']}  ({table})")
            print(f"          {total:,} row(s), of which {stale:,} not daily-shaped")
            if not stale:
                print("  OK      nothing to remove")
                continue
            if args.apply:
                cursor.execute(delete_sql(table, predicate))
                conn.commit()
                print(f"  deleted {cursor.rowcount:,} row(s)")
            else:
                print(textwrap.fill(delete_sql(table, predicate), width=100, initial_indent="  todo    ",
                                    subsequent_indent="          "))
        if not args.apply:
            print("\n  read-only: nothing was run. Add --apply to run the above.")
    except pyodbc.Error as err:
        failed = 1
        print(f"\n  FAILED\n{ihr._sql_message(err)}")
    finally:
        conn.close()
    return failed


if __name__ == "__main__":
    sys.exit(main())
