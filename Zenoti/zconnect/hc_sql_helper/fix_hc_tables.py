"""Bring the three HyperConnect tables in line with what the loader needs.

READ-ONLY unless you pass --apply. Without it this connects, works out what is
missing, and prints the exact SQL; with it, it runs that SQL. Nothing else ever
touches the schema -- import_hc_report.py only inserts rows.

What it fixes, and only these three things:

  * a key column the table does not have at all. The loader refuses to load
    without one (nothing would be recognised as already there, so every run
    would re-insert everything), so this is what unblocks it.
  * a key column whose type cannot hold the data. Two real cases, both AGENT_ID:
    on call logs it is uniqueidentifier and the voicebot agents come back as
    `agent_2401k7mc4rfbfvtvw1a3nbk6pvwd` -- 186 of 11069 rows; on agent metrics
    it is uniqueidentifier too and 64 of 1148 rows carry `agent_4001k7mbg...`.
    The type is checked before the ALTER, so a column that has already been
    widened, or one that is unexpectedly some third type, is reported and left
    alone. Where the column is part of an index the ALTER would be refused
    ("the index is dependent on column"), so that index is dropped first and
    recreated with the same name and columns straight afterwards -- see
    plan(). A column in the PRIMARY KEY is reported instead, never dropped.
  * a unique index on the key. The dedup in import_hc_report.py is an index seek
    per row with one and a full scan per row without, and the index is also the
    only thing stopping two overlapping runs from both inserting the same row.
    An existing unique index covering the same columns counts, whatever it is
    named.

Deliberately NOT fixed: a NOT NULL column the CSV cannot fill. That is a data
question about what the row should say, not a schema shape, and --check-db in
import_hc_report.py reports it.

    python fix_hc_tables.py                 # report and print the SQL
    python fix_hc_tables.py --report call_logs
    python fix_hc_tables.py --apply         # run it

    --apply         run the SQL rather than print it
    --report NAME   just this one: location_performance | agent_metrics | call_logs
    --self-check    no DB: exercise the plan builder

If --apply reports a permission error, the login can read the schema but not
change it; take the printed SQL to whoever can.
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
import import_aesthetix_opportunity_history as base  # noqa: E402
import import_hc_report as ihr  # noqa: E402  (its REPORTS table, and .env loading)

# Types for key columns that have to be added, hand-declared from the sample
# CSVs in ../../sample csv rather than inferred: start_date / end_date are
# YYYY-MM-DD, center_id is a GUID, and `date` is the ISO day an agent_metrics
# row covers. Nothing is inferred from the data, so a column can never come out
# with a type nobody chose.
ADD_TYPE = {
    "start_date": "date",
    "end_date": "date",
    "date": "date",
    "center_id": "uniqueidentifier",
    "centerid": "uniqueidentifier",
    "agent_id": "nvarchar(64)",
    "call_id": "nvarchar(64)",   # 'CA' + 32 hex, 34 chars -- not a GUID
    "event_id": "nvarchar(64)",
    "send_date": "datetime2",
}

# A column that exists but cannot hold what arrives. Only ever added here because
# a real load failed on it -- (type it must currently be, type to make it).
RETYPE = {
    "call_logs": {"agent_id": ("uniqueidentifier", "nvarchar(64)")},
    "agent_metrics": {"agent_id": ("uniqueidentifier", "nvarchar(64)")},
}


def index_columns(cursor, schema, name):
    """{index name: [key columns in order]} for the unique indexes on a table.

    Included columns are skipped: they are payload, not key, and would otherwise
    make a covering index look like a different key.
    """
    cursor.execute(
        "SELECT i.name, COL_NAME(ic.object_id, ic.column_id)\n"
        "FROM sys.indexes AS i\n"
        "JOIN sys.index_columns AS ic\n"
        "  ON ic.object_id = i.object_id AND ic.index_id = i.index_id\n"
        "WHERE i.object_id = OBJECT_ID(?) AND i.is_unique = 1 AND ic.is_included_column = 0\n"
        "ORDER BY i.name, ic.key_ordinal",
        [f"[{schema}].[{name}]"],
    )
    found = {}
    for index_name, column in cursor.fetchall():
        found.setdefault(index_name, []).append(column)
    return found


def primary_key_columns(cursor, schema, name):
    """{index name: [key columns]} for the primary key, if the table has one.

    Kept apart from index_columns so the "is the key indexed?" check still counts
    a primary key as covering -- but a retype never drops one.
    """
    cursor.execute(
        "SELECT i.name, COL_NAME(ic.object_id, ic.column_id)\n"
        "FROM sys.indexes AS i\n"
        "JOIN sys.index_columns AS ic\n"
        "  ON ic.object_id = i.object_id AND ic.index_id = i.index_id\n"
        "WHERE i.object_id = OBJECT_ID(?) AND i.is_primary_key = 1 AND ic.is_included_column = 0\n"
        "ORDER BY i.name, ic.key_ordinal",
        [f"[{schema}].[{name}]"],
    )
    found = {}
    for index_name, column in cursor.fetchall():
        found.setdefault(index_name, []).append(column)
    return found


def not_null_columns(cursor, schema, name):
    """Columns that are NOT NULL, so a retype can keep them that way.

    Nullability is not in _table_columns, and widening a column is no reason to
    quietly drop a constraint nobody asked to change.
    """
    cursor.execute(
        "SELECT c.name FROM sys.columns AS c\n"
        "WHERE c.object_id = OBJECT_ID(?) AND c.is_nullable = 0",
        [f"[{schema}].[{name}]"],
    )
    return {row[0] for row in cursor.fetchall()}


def duplicate_keys(cursor, table, key_cols):
    """(repeating key groups, surplus rows, one example) for keys already in the table.

    A unique index cannot be built over these, and they are exactly what the index
    exists to prevent -- so this is the answer to "why did CREATE UNIQUE INDEX
    fail", asked before it does instead of after.
    """
    cols = ", ".join(f"[{c}]" for c in key_cols)
    cursor.execute(
        f"SELECT COUNT(*), ISNULL(SUM(n), 0) FROM (\n"
        f"  SELECT COUNT(*) AS n FROM {table} GROUP BY {cols} HAVING COUNT(*) > 1\n"
        f") AS d"
    )
    groups, counted = cursor.fetchone()
    if not groups:
        return 0, 0, None
    cursor.execute(
        f"SELECT TOP 1 {cols}, COUNT(*) FROM {table}\n"
        f"GROUP BY {cols} HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC"
    )
    return groups, counted - groups, tuple(cursor.fetchone())


def plan(report, table, columns, col_types, indexes, primary_keys=None, not_null=None):
    """(statements, notes) to bring one table in line.

    `table` is passed in rather than read from REPORTS: the live name comes from
    the report's env var, and a plan that re-derived it here could aim at a
    different table than the one the columns were just read from. That is exactly
    how the first run of this script printed the right table and then ALTERed a
    name that did not exist.

    Pure, so the decision is testable without a server -- it is the part where a
    wrong answer silently writes schema. An empty `statements` means the table is
    already right. The two optional maps are read from the server by main() and
    left out entirely by the self-check that does not care about them.
    """
    spec = ihr.REPORTS[report]
    key = spec["key"]
    statements, notes = [], []
    primary_keys = primary_keys or {}
    not_null = not_null or set()

    by_norm = {base.normalize_col(c): c for c in columns}
    key_cols, missing = ihr.resolve_key(key, columns)

    for column in missing:
        kind = ADD_TYPE.get(base.normalize_col(column))
        if not kind:
            notes.append(f"no declared type for {column}; add it yourself")
            continue
        notes.append(f"add  {column} {kind}")
        statements.append(f"ALTER TABLE {table} ADD [{column}] {kind} NULL")
        key_cols.append(column)

    for column, (from_type, to_type) in RETYPE.get(report, {}).items():
        live = by_norm.get(base.normalize_col(column))
        if not live:
            continue
        if (col_types.get(live) or "").lower() != from_type.lower():
            notes.append(f"{live} is {col_types.get(live)}, not {from_type}; left alone")
            continue

        # A primary key cannot be dropped and put back the way a plain unique
        # index can -- SQL Server refuses the ALTER outright -- so say so and
        # change nothing rather than emit a statement that fails halfway.
        in_pk = [(name, cols) for name, cols in primary_keys.items() if live in cols]
        if in_pk:
            notes.append(f"{live} is in the primary key ({in_pk[0][0]}); widen it by hand, nothing planned")
            continue

        # An index over the column blocks ALTER COLUMN ("the index ... is
        # dependent on column"), so it comes off first and goes back on after,
        # same name, same columns. Dropped and recreated around one statement in
        # one plan: an index left off by a failed ALTER is put back by the next
        # run, and the load is correct either way.
        dependent = [(name, cols) for name, cols in indexes.items() if live in cols]
        for name, _ in dependent:
            notes.append(f"drop index {name} to retype {live}")
            statements.append(f"DROP INDEX [{name}] ON {table}")
        suffix = " NOT NULL" if live in not_null else " NULL"
        notes.append(f"widen {live} {from_type} -> {to_type}")
        statements.append(f"ALTER TABLE {table} ALTER COLUMN [{live}] {to_type}{suffix}")
        for name, cols in dependent:
            notes.append(f"recreate index {name} ({', '.join(cols)})")
            statements.append(
                f"CREATE UNIQUE INDEX [{name}] ON {table} ({', '.join(f'[{c}]' for c in cols)})"
            )

    # Same columns in any order counts as covered -- a key is a set.
    if key_cols:
        wanted = {base.normalize_col(c) for c in key_cols}
        if not any({base.normalize_col(c) for c in cols} == wanted for cols in indexes.values()):
            index_name = f"UX_{base._split_table(table)[1]}_KEY"
            notes.append(f"index {index_name} ({', '.join(key_cols)})")
            statements.append(
                f"CREATE UNIQUE INDEX [{index_name}] ON {table} "
                f"({', '.join(f'[{c}]' for c in key_cols)})"
            )

    return statements, notes


def self_check():
    """The plan builder decides DDL, so pin its shapes -- including that it is a
    no-op on a table that is already right, since that is what makes a second run
    safe."""
    # Literal names on purpose, not REPORTS[...]["table"]: a plan that reads the
    # name back out of the config would pass this test even if the config were
    # pointing at a table that does not exist, which is the bug this script
    # already shipped once.
    lp, cl = "dbo.BRONZE_ZENOTI_HC_LOCATION_PERFORMANCE", "dbo.BRONZE_ZENOTI_HC_CALL_LOGS"
    cols = ["CENTER_ID", "CENTER_NAME", "OVERALL_RATING"]
    types = {"CENTER_ID": "uniqueidentifier", "CENTER_NAME": "nvarchar", "OVERALL_RATING": "int"}

    # Nothing present: both key columns added, then indexed.
    st, notes = plan("location_performance", lp, cols, types, {})
    assert st == [
        f"ALTER TABLE {lp} ADD [start_date] date NULL",
        f"ALTER TABLE {lp} ADD [end_date] date NULL",
        f"CREATE UNIQUE INDEX [UX_BRONZE_ZENOTI_HC_LOCATION_PERFORMANCE_KEY] ON {lp} "
        f"([CENTER_ID], [start_date], [end_date])",
    ], st

    # Already complete: same columns, a differently-named unique index covering
    # them. Must plan nothing -- a second run cannot be allowed to add a second
    # index, or a second column.
    live = cols + ["START_DATE", "END_DATE"]
    live_types = dict(types, START_DATE="date", END_DATE="date")
    covered = {"whatever_name": ["END_DATE", "START_DATE", "CENTER_ID"]}
    st, notes = plan("location_performance", lp, live, live_types, covered)
    assert st == [], (st, notes)

    # A unique index on only part of the key does not count.
    st, _ = plan("location_performance", lp, live, live_types, {"idx": ["CENTER_ID"]})
    assert len(st) == 1 and st[0].startswith("CREATE UNIQUE INDEX"), st

    # The retype only fires on the type it was written for.
    st, notes = plan("call_logs", cl, ["CALL_ID", "AGENT_ID"],
                     {"CALL_ID": "nvarchar", "AGENT_ID": "uniqueidentifier"}, {"i": ["CALL_ID"]})
    assert st == [f"ALTER TABLE {cl} ALTER COLUMN [AGENT_ID] nvarchar(64) NULL"], st
    st, notes = plan("call_logs", cl, ["CALL_ID", "AGENT_ID"],
                     {"CALL_ID": "nvarchar", "AGENT_ID": "nvarchar"}, {"i": ["CALL_ID"]})
    assert st == [] and any("left alone" in n for n in notes), (st, notes)

    # A retype on an indexed column: the index comes off, the column is widened,
    # the index goes back -- in that order, because a CREATE before the ALTER is
    # exactly what SQL Server refuses.
    am = "dbo.BRONZE_ZENOTI_HC_AGENT_METRICS"
    am_cols = ["AGENT_ID", "CENTER_ID", "START_DATE", "END_DATE", "DATE"]
    am_types = dict.fromkeys(am_cols, "uniqueidentifier")
    am_types.update({"START_DATE": "date", "END_DATE": "date", "DATE": "date"})
    ux = {"UX_BRONZE_ZENOTI_HC_AGENT_METRICS_KEY": am_cols}
    st, notes = plan("agent_metrics", am, am_cols, am_types, ux, not_null={"AGENT_ID", "CENTER_ID"})
    assert st == [
        f"DROP INDEX [UX_BRONZE_ZENOTI_HC_AGENT_METRICS_KEY] ON {am}",
        f"ALTER TABLE {am} ALTER COLUMN [AGENT_ID] nvarchar(64) NOT NULL",
        f"CREATE UNIQUE INDEX [UX_BRONZE_ZENOTI_HC_AGENT_METRICS_KEY] ON {am} "
        f"([AGENT_ID], [CENTER_ID], [START_DATE], [END_DATE], [DATE])",
    ], st

    # Widening with no nullability handed in must not invent a NOT NULL.
    st, _ = plan("agent_metrics", am, am_cols, am_types, ux)
    assert st[1].endswith("nvarchar(64) NULL"), st

    # A column in the primary key is reported, never dropped: there is no way to
    # put a primary key back from here. The type still has to be the one the
    # retype was written for, or the guard above answers first and this never
    # reaches the primary-key branch.
    pk = {"PK_BRONZE_ZENOTI_HC_CALL_LOGS": ["CALL_ID", "AGENT_ID"]}
    st, notes = plan("call_logs", cl, ["CALL_ID", "AGENT_ID"],
                     {"CALL_ID": "nvarchar", "AGENT_ID": "uniqueidentifier"},
                     {"PK_BRONZE_ZENOTI_HC_CALL_LOGS": ["CALL_ID"]}, pk)
    assert st == [] and any("primary key" in n for n in notes), (st, notes)

    # Already widened: the second run is a no-op, index untouched.
    st, notes = plan("agent_metrics", am, am_cols, dict(am_types, AGENT_ID="nvarchar"), ux)
    assert st == [] and any("left alone" in n for n in notes), (st, notes)

    # A statement must name the table the columns were read from, never a second
    # name derived from the same config.
    st, _ = plan("call_logs", "dbo.SOMETHING_ELSE", ["CALL_ID", "AGENT_ID"],
                 {"CALL_ID": "nvarchar", "AGENT_ID": "uniqueidentifier"}, {"i": ["CALL_ID"]})
    assert st == ["ALTER TABLE dbo.SOMETHING_ELSE ALTER COLUMN [AGENT_ID] nvarchar(64) NULL"], st

    # Every report's key columns have a declared type, or the plan would tell the
    # user to add a column with no type at all.
    for report, spec in ihr.REPORTS.items():
        for column in spec["key"]:
            assert base.normalize_col(column) in ADD_TYPE, (report, column)

    events = ihr.REPORTS["events"]["table"]
    st, _ = plan("events", events, [], {}, {})
    assert st == [
        f"ALTER TABLE {events} ADD [centerid] uniqueidentifier NULL",
        f"ALTER TABLE {events} ADD [event_id] nvarchar(64) NULL",
        f"ALTER TABLE {events} ADD [send_date] datetime2 NULL",
        f"CREATE UNIQUE INDEX [UX_BRONZE_ZENOTI_EVENTS_REVIEWS_KEY] ON {events} "
        "([centerid], [event_id], [send_date])",
    ], st

    print("self-check OK")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Align the HyperConnect tables with the loader.")
    parser.add_argument("--apply", action="store_true", help="run the SQL rather than print it")
    parser.add_argument("--report", choices=sorted(ihr.REPORTS), default=None)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()

    if args.self_check:
        return self_check()

    print(f"  server  {ihr.SERVER}")
    print(f"  db      {ihr.DATABASE}")
    print(f"  mode    {'APPLY, changes will be made' if args.apply else 'read-only, printing the SQL'}")

    conn = get_connection()
    todo, failed = [], 0
    try:
        cursor = conn.cursor()
        for report in ([args.report] if args.report else sorted(ihr.REPORTS)):
            spec = ihr.REPORTS[report]
            table = os.getenv(spec["env"]) or spec["table"]
            schema, name = base._split_table(table)
            columns, _, col_types, _ = base._table_columns(cursor, schema, name)
            print(f"\n  {spec['label']}  ({table})")

            if not columns:
                print("  MISSING does not exist; create it out-of-band first")
                continue
            statements, notes = plan(report, table, columns, col_types, index_columns(cursor, schema, name),
                                     primary_key_columns(cursor, schema, name),
                                     not_null_columns(cursor, schema, name))
            print(f"          {len(columns)} column(s); key {', '.join(spec['key'])}")

            # Only worth asking about the data when the plan is to put a unique
            # index on it: that is the one statement existing duplicates can block.
            found, missing = ihr.resolve_key(spec["key"], columns)
            if not missing and any(s.startswith("CREATE UNIQUE INDEX") for s in statements):
                groups, surplus, example = duplicate_keys(cursor, table, found)
                if groups:
                    print(f"  dupes   {groups:,} key(s) already repeat, {surplus:,} row(s) beyond the "
                          f"first, e.g. {example}")
                    print("          a unique index cannot be built over these; the load itself is fine")

            if not statements:
                print("  OK      already matches what the loader needs")
                continue
            for note in notes:
                print(f"  todo    {note}")
            todo.extend((spec["label"], s) for s in statements)

        if not todo:
            print("\n  nothing to do")
            return 0

        print(f"\n  {len(todo)} statement(s)")
        if not args.apply:
            for _, statement in todo:
                print(textwrap.fill(statement, width=100, initial_indent="    ",
                                    subsequent_indent="    "))
            print("\n  read-only: nothing was run. Add --apply to run the above.")
            return 0

        # Committed one at a time, deliberately. The statements are independent --
        # the widen that unblocks a load has nothing to do with an index on
        # another table -- so aborting the rest because one hit existing
        # duplicates would turn one finding into three. Each is re-planned from
        # scratch on the next run, so what does not apply now applies later.
        for label, statement in todo:
            try:
                cursor.execute(statement)
                conn.commit()
                print(f"  applied {label}: {textwrap.shorten(statement, 80)}")
            except pyodbc.Error as err:
                conn.rollback()
                failed += 1
                print(f"  FAILED  {label}\n{ihr._sql_message(err)}")
        print(f"\n  {len(todo) - failed} applied, {failed} failed")
    finally:
        conn.close()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
