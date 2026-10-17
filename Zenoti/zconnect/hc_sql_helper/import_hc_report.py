"""Load the HyperConnect report CSVs into their Azure SQL tables.

Lives next to the scripts that produce the CSVs, because it is the second half of
the same job: sync_hc.py pulls, this loads. One table each:

    hc_location_performance_<start>_<end>.csv   ->  HC Location Performance Table
    hc_agent_metrics_<start>_<end>.csv          ->  HC Agent Metrics
    hc_call_logs_<start>_<end>.csv              ->  HC Call Logs

REPLACE THE WINDOW. Each load deletes the rows it is about to insert and then
inserts them, both in one transaction, so a re-pulled window is refreshed rather
than skipped. Each report declares a `key` -- the columns that say "this row is
this thing" -- and the two statements are per batch:

    DELETE FROM t WHERE EXISTS (SELECT 1 FROM (VALUES ...) AS s (...) WHERE s and t agree on every key column)
    INSERT INTO t (...) VALUES (...), (...), ...

Why delete rather than skip: the source keeps changing a row after the fact. A
call is indexed before its transcript lands, so `transcription_status` is
`inProgress` and `call_duration` 0 on the row as first pulled, and a load that
skips an existing key keeps those values forever. Delete-then-insert makes the
stored row whatever the latest pull says, which is what lets a daily run widen
its window over the last few days and actually correct them.

The delete is scoped by the payload, not by a date predicate: it removes exactly
the keys being inserted. A call_logs row has no window column at all -- its key
is `call_id` alone -- so a range-based delete would have to guess from
`timestamp_local` and would miss any row whose timestamp the source moved,
leaving the unique index to reject the insert. Bounded by the payload, the delete
can only ever touch a row it is about to write back, which also makes a truncated
pull safe: fewer rows come back, fewer rows are deleted, and what is not in the
payload is left alone rather than wiped and not refilled. A `--max-pages` cut-off
therefore costs freshness, never data. The trade is that a row the source has
stopped returning is left in place; this refreshes a window, it does not mirror
one.

Both statements are in the same transaction, so a failure rolls back to the rows
as they were -- there is no window in which a re-pulled window is missing. Each
replaced row gets a new identity value, since the identity column is not in
insert_columns.

GIVE EACH KEY A UNIQUE INDEX. That is what makes the delete an index seek instead
of a scan, and it is what bounds the blast radius: a key column is NULL on some
pulls by design (`date` on a plain agent pull), and the NULL-safe match treats
"all key columns NULL" as equal to any other all-NULL row. A unique index allows
only one such row, so the worst a garbage payload row can delete is that one.

The key columns have to exist in the table. That is checked before the first
statement and the load refuses if one is missing, naming it -- and that check
matters more now than it did under NOT EXISTS. With no key there is no WHERE
clause to build, so a load that proceeded would be a delete with nothing scoping
it.

WHICH DATES. The window comes from ZENOTI_START_DATE / ZENOTI_END_DATE -- the
same two variables the puller reads, so the file it wrote is named for exactly
that window and is found by name. Ask for a window with no matching file and
this says so instead of loading some other period's CSV, which would look like it
worked. With both variables unset there is no window to match, and the newest
matching file wins.

Columns are not fixed on either side: the CSVs are written from whatever the API
returned, and the tables are whatever they are. Nothing here hardcodes a column.
CSV headers and table column names are matched on their NORMALIZED form, and each
value is coerced by the target column's LIVE type read from INFORMATION_SCHEMA at
run time. That machinery is reused from dbquery/import_aesthetix_opportunity_history.py
rather than copied a second time.

Usage:
    python import_hc_report.py --report location_performance --check-db
    python import_hc_report.py --report location_performance --dry-run
    python import_hc_report.py --report agent_metrics
    python import_hc_report.py --report agent_metrics --csv path/to.csv
    python import_hc_report.py --self-check

    --report NAME   location_performance | agent_metrics | call_logs (required)
    --csv PATH      which CSV; default is the one named for the ZENOTI_* window
    --check-db      read-only: connect, report the table, mapping and pre-flight
    --dry-run       parse and report; no DB connection
    --verbose       also print the CSV's column list and its first row
    --self-check    no DB, no file: exercise the file-picking logic

Set TABLE_ZENOTI_HC_LOCATION_PERFORMANCE / TABLE_ZENOTI_HC_AGENT_METRICS /
TABLE_ZENOTI_HC_CALL_LOGS in .env to point at different tables; the names above
are the default.
"""

import glob
import os
import re
import sys
import textwrap
import time
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))          # hc_sql_helper/, this folder
_WORKDIR = os.path.dirname(_HERE)                            # zconnect/ -- where the pulls write their CSVs
_ROOT = os.path.dirname(_WORKDIR)                            # Zenoti/

# db_helper.py sits at the root; the shared CSV->table machinery sits in dbquery/.
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "dbquery"))

import pyodbc  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from db_helper import get_connection  # noqa: E402

# normalize_col, the type-aware coercers, _table_columns, _to_tuples,
# _validate_types, _mapping_report, _insert_batched. Importing rather than
# copying keeps one copy of that logic -- importing also runs its own
# load_dotenv and reads argv, both harmless here (its constants are unused).
import import_aesthetix_opportunity_history as base  # noqa: E402

# The real .env is two levels up (Zenoti/.env); a zconnect/.env, which is where
# login_headers.py writes HC_TOKEN, wins over it, and a .env beside this file
# would win over both. Searched nearest-first, and load_dotenv does not override
# what is already set.
for _dotenv in (".env", "env",
                os.path.join("..", ".env"), os.path.join("..", "env"),
                os.path.join("..", "..", ".env"), os.path.join("..", "..", "env")):
    load_dotenv(os.path.join(_HERE, _dotenv))

SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")

# `label` is what the log calls the table -- the BRONZE_ name is the loader's
# business, not the reader's, and the report it belongs to is what matters at a
# glance. `env` still resolves to the real name, which --check-db prints too.
# `prefix` is the filename stem a CSV for this report starts with, used both to
# find the file for a window and to find the newest when there is no window.
# Note hc_call_logs_ also front-matches call_logs.py --by agent / --by center
# rollups, whose shape differs. Only the no-window fallback could pick one of
# those; sync_hc always names the file, so the pipeline never can.
#
# `key` is what makes a row recognisable on a later run, in the normalized form
# the CSV and the table are both matched on. It is not a guess: each one is read
# off the sample CSVs in ../../sample csv, and each is the grain of the file.
#   call_logs            call_id is the API's own id for the call, one row each.
#                        Verified on 5715 sample rows: 5715 distinct, none blank.
#   agent_metrics        one row per agent per center per day. center_id is
#                        NOT redundant -- the pull walks every center in turn and
#                        8 agents come back under more than one (Jennifer Clark
#                        under 12), so agent_id alone keys 45 rows onto 76 and
#                        the other 31 would be dropped as duplicates. start_date,
#                        end_date and `date` are all the row's own day; the
#                        puller stamps it that way, so the key is exact at day
#                        grain and the NULL-safe comparison in _key_match only
#                        covers a row the API returned undated.
#   location_performance one row per center per day. The payload carries no
#                        period at all, so the day has to be stamped on the rows
#                        by the puller -- see location_performance.py.
REPORTS = {
    "location_performance": {
        "label": "HC Location Performance Table",
        "env": "TABLE_ZENOTI_HC_LOCATION_PERFORMANCE",
        "table": "dbo.BRONZE_ZENOTI_HC_LOCATION_PERFORMANCE",
        "prefix": "hc_location_performance_",
        "key": ("center_id", "start_date", "end_date"),
    },
    "agent_metrics": {
        "label": "HC Agent Metrics",
        "env": "TABLE_ZENOTI_HC_AGENT_METRICS",
        "table": "dbo.BRONZE_ZENOTI_HC_AGENT_METRICS",
        "prefix": "hc_agent_metrics_",
        "key": ("agent_id", "center_id", "start_date", "end_date", "date"),
    },
    "call_logs": {
        "label": "HC Call Logs",
        "env": "TABLE_ZENOTI_HC_CALL_LOGS",
        "table": "dbo.BRONZE_ZENOTI_HC_CALL_LOGS",
        "prefix": "hc_call_logs_",
        "key": ("call_id",),
    },
}

DRY_RUN = "--dry-run" in sys.argv
CHECK_DB = "--check-db" in sys.argv
VERBOSE = "--verbose" in sys.argv
SELF_CHECK = "--self-check" in sys.argv


def arg_value(name):
    """The value after `--name`, or None. A missing value (flag last on the
    line) is an error rather than silently eating the next flag."""
    if name not in sys.argv:
        return None
    index = sys.argv.index(name)
    if index + 1 >= len(sys.argv) or sys.argv[index + 1].startswith("--"):
        raise SystemExit(f"{name} needs a value")
    return sys.argv[index + 1]


def _note(label, items):
    """A labelled list, wrapped. Thirty-seven column names on one line is not a
    log, it is a wall."""
    head = f"  {label:<8}"
    print(textwrap.fill(", ".join(items), width=100,
                        initial_indent=head, subsequent_indent=" " * len(head)))


def _sample(row):
    """The first row as `key=value`, wrapped, each value trimmed.

    Trimmed on purpose: the agent payload nests dicts (sentiment, direct,
    transferred), and at full length they push everything else off the screen.
    Trimmed, the fact that they are dicts at all is what you see.
    """
    parts = []
    for key, value in row.items():
        text = str(value)
        parts.append(f"{key}={text[:38] + '~' if len(text) > 38 else text}")
    print(textwrap.fill("  ".join(parts), width=100,
                        initial_indent="  row     ",
                        subsequent_indent=" " * 10))


GUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}"
                     r"-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def bad_guids(dict_rows, insert_columns, col_types, limit=3):
    """Values that will not fit a uniqueidentifier column, as {col: (count, samples)}.

    The driver's own report of this names no column -- "Conversion failed when
    converting from a character string to uniqueidentifier" -- and it arrives
    after the whole load has been sent, so it costs the run and still leaves you
    working out which of 33 columns did it. Checked here instead, before the
    first INSERT.

    This is not hypothetical: agent_id is a GUID for the humans and
    `agent_2401k7mc4rfbfvtvw1a3nbk6pvwd` for the voicebots -- 186 of 11069 rows
    in one real pull. Nothing in the CSV marks which columns the table holds as
    uniqueidentifier, so the live type is the only thing to go on.
    """
    found = {}
    for col in insert_columns:
        if (col_types.get(col) or "").lower() != "uniqueidentifier":
            continue
        norm = base.normalize_col(col)
        samples = [str(row.get(norm)).strip() for row in dict_rows
                   if row.get(norm) not in (None, "") and not GUID_RE.match(str(row.get(norm)).strip())]
        if samples:
            found[col] = (len(samples), samples[:limit])
    return found


def _sql_message(err):
    """The server's own sentence out of pyodbc's tuple of everything.

    ('23000', "[23000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]
    Cannot insert the value NULL into column 'AGENT_ID', table '...'; column does
    not allow nulls. INSERT fails. (515)(SQLExecDirectW); [23000] [Microsoft]...
    The statement has been terminated.")

    The bracketed prefixes, the state codes and the repeat are driver
    boilerplate; what a reader needs is the one sentence in the middle.
    """
    text = re.sub(r"\[[^\]]*\]\s*", "", str(err)).split("(")[0].strip(" '\",;")
    return textwrap.fill(text or str(err), width=100,
                         initial_indent=" " * 10, subsequent_indent=" " * 10)


def requested_range(env=None):
    """(start, end) to load, from ZENOTI_START_DATE / ZENOTI_END_DATE.

    The same two variables the puller reads, so the file it wrote is named for
    exactly this window. Either one missing means there is no window to match,
    and (None, None) sends the caller to the newest file instead.
    """
    env = os.environ if env is None else env
    start, end = env.get("ZENOTI_START_DATE"), env.get("ZENOTI_END_DATE")
    return (start, end) if start and end else (None, None)


def latest_csv(prefix, folder):
    """Newest prefix-matching *.csv in `folder`, or None."""
    matches = glob.glob(os.path.join(folder, prefix + "*.csv"))
    return max(matches, key=os.path.getmtime) if matches else None


def csv_for_range(prefix, folder, start, end):
    """The CSV for the requested window, or the newest when none was asked for.

    A window with no file is an error, not a fallback to the newest: loading
    yesterday's CSV under today's dates looks exactly like a successful run.
    """
    if start and end:
        wanted = os.path.join(folder, f"{prefix}{start}_{end}.csv")
        if os.path.exists(wanted):
            return wanted
        others = sorted(os.path.basename(p) for p in glob.glob(os.path.join(folder, prefix + "*.csv")))
        raise SystemExit(
            f"no CSV for {start}..{end}: expected {os.path.basename(wanted)} in {folder}.\n"
            f"  Pull it first:  python sync_hc.py --start-date {start} --end-date {end}\n"
            f"  Or name the file:  --csv PATH"
            + (f"\n  (files present: {', '.join(others)})" if others else "")
        )
    return latest_csv(prefix, folder)


def resolve_key(key, insert_columns):
    """The report's declared key as the table actually spells those columns.

    Returns (key_cols, missing). Plain `=` would not do for the comparison, so
    the caller gets the table's own column names back to build the SQL with.
    """
    by_norm = {base.normalize_col(c): c for c in insert_columns}
    found = [by_norm[base.normalize_col(k)] for k in key
             if base.normalize_col(k) in by_norm]
    missing = [k for k in key if base.normalize_col(k) not in by_norm]
    return found, missing


def _key_match(key_cols):
    """SQL for "s and t are the same row", NULL-safe on every key column.

    Plain equality is not enough: a row the API hands back without a date keeps
    the window stamp and is NULL in `date` (`NULL = NULL` is unknown in SQL), so
    a plain `=` would make every re-run look new. Day-wise pulls are the norm --
    both dashboard reports are pulled daily -- so this is the rare path, not the
    usual one.
    """
    return " AND ".join(
        f"(t.[{c}] = s.[{c}] OR (t.[{c}] IS NULL AND s.[{c}] IS NULL))" for c in key_cols)


def drop_csv_dupes(dict_rows, key_cols):
    """Rows whose key already appeared earlier in the same CSV, dropped (the
    first is kept). The delete in delete_keys removes what is in the table, never
    what is in the batch, so two copies of one key inside a single pull would both
    be inserted -- and the unique index would reject the whole batch."""
    seen, out, dropped = set(), [], 0
    for row in dict_rows:
        marker = tuple(row.get(base.normalize_col(c)) for c in key_cols)
        if marker in seen:
            dropped += 1
            continue
        seen.add(marker)
        out.append(row)
    return out, dropped


# The two ceilings one statement has to stay under, and neither is the number to
# sit on. MAX_PARAMS: SQL Server's limit is 2100, and a real 11k-row call_logs
# load failed with "too many parameters ... maximum of 2100" on a DELETE carrying
# exactly 2100 -- while an insert at 2079 params had been loading fine for weeks.
# So the batch keeps clear of the number rather than landing on it; the exact
# off-by-one does not have to be understood for the load to work. MAX_ROWS: a
# VALUES list is a table value constructor, capped at 1000 rows, so a narrow table
# cannot be batched into that wall just because its parameter count is low.
MAX_PARAMS = 2000
MAX_ROWS = 1000


def chunked(data, params_per_row):
    """`data` in batches that stay under both per-statement ceilings.

    Both counts are pinned in _self_check, which is where the last version's
    mistake was: it asserted 2100 was the batch size, so it agreed with the code
    while the code was wrong.
    """
    size = max(1, min(MAX_ROWS, MAX_PARAMS // params_per_row))
    return [data[start:start + size] for start in range(0, len(data), size)]


def delete_keys(cursor, tbl, insert_columns, key_cols, data):
    """Delete every row of `tbl` whose key is one of the keys in `data`.

    Batched on the KEY columns only, so a wide table still deletes the full batch
    per round trip -- the parameter cap is what forces batching, not the payload's
    width. NULL-safe on the key, for the same reason the old NOT EXISTS was:
    `date` is NULL on a plain agent pull and `=` would never match it.

    The target is aliased `t` because that is what _key_match's SQL expects, and
    the key values are picked out of each row by POSITION -- insert_columns is the
    table's own column order, so the key is not necessarily at the front.
    """
    where = _key_match(key_cols)
    col_list = ", ".join(f"[{c}]" for c in key_cols)
    positions = [insert_columns.index(c) for c in key_cols]
    deleted = 0
    for batch in chunked(data, len(key_cols)):
        keys = [[row[i] for i in positions] for row in batch]
        rows = ", ".join("(" + ", ".join("?" * len(key_cols)) + ")" for _ in keys)
        cursor.execute(
            f"DELETE t FROM {tbl} AS t WHERE EXISTS (\n"
            f"  SELECT 1 FROM (VALUES {rows}) AS s ({col_list}) WHERE {where}\n"
            f")",
            [value for key in keys for value in key],
        )
        deleted += cursor.rowcount
    return deleted


def insert_rows(cursor, tbl, insert_columns, data):
    """Plain batched multi-row INSERT. Returns rows inserted.

    No WHERE clause: delete_keys has already removed anything that would collide,
    and drop_csv_dupes has removed anything that would collide with itself, so the
    unique index has nothing left to reject.
    """
    col_list = ", ".join(f"[{c}]" for c in insert_columns)
    batches = chunked(data, len(insert_columns))
    inserted = 0
    for start, batch in enumerate(batches):
        rows = ", ".join("(" + ", ".join("?" * len(insert_columns)) + ")" for _ in batch)
        cursor.execute(
            f"INSERT INTO {tbl} ({col_list}) VALUES {rows}",
            [value for row in batch for value in row],
        )
        inserted += cursor.rowcount
        # Worth having: the parameter cap is what sets the round-trip count, 185
        # of them for an 11k-row 33-column load, and elapsed time is dominated by
        # those. Silent, a slow load is indistinguishable from a hung one.
        if len(batches) > 1 and start % 50 == 49:
            print(f"          {min((start + 1) * len(batch), len(data)):,}/{len(data):,} sent, "
                  f"{inserted:,} inserted", flush=True)
    return inserted


def replace_rows(cursor, tbl, insert_columns, key_cols, data):
    """Delete the payload's keys, then insert the payload. Returns (deleted, inserted).

    Both are in the caller's transaction. All the deletes run before any insert:
    a key cannot appear twice in `data` (drop_csv_dupes saw to that), but doing it
    this way means that even if one did, it could not be deleted after being
    written.
    """
    deleted = delete_keys(cursor, tbl, insert_columns, key_cols, data)
    return deleted, insert_rows(cursor, tbl, insert_columns, data)


def load_report(conn, cursor, label, qualified, dict_rows, key):
    """Replace one CSV's worth of rows in one table, as its own transaction.

    Coercion, the key check and the type pre-flight all run before the first
    DELETE, so a mismatch aborts with the table untouched. Once the delete runs it
    and the insert are one transaction -- see the module docstring.
    """
    if not dict_rows:
        print("  skip    nothing to load, the CSV parsed to 0 rows")
        return

    schema, name = base._split_table(qualified)
    columns, identity, col_types, lengths = base._table_columns(cursor, schema, name)
    if not columns:
        raise SystemExit(f"  FAILED  {label} does not exist -- create it out-of-band first"
                         f"\n          {qualified}")
    insert_columns = [c for c in columns if c not in identity]
    tbl = f"[{schema}].[{name}]"

    # Before anything else: a key column the table does not have would compare
    # nothing and re-insert every row on every run, which reads as a clean load.
    key_cols, missing_key = resolve_key(key, insert_columns)
    if missing_key:
        raise SystemExit(
            f"  FAILED  {label} has no column for the key: {', '.join(missing_key)}\n"
            f"          Those columns are what say a row is already loaded, so without them\n"
            f"          every run re-inserts everything. Nothing was written.\n"
            f"          Add them to {qualified}, then rerun."
        )
    dict_rows, dropped = drop_csv_dupes(dict_rows, key_cols)
    if dropped:
        print(f"  dup     {dropped:,} row(s) repeated in the CSV, first kept")

    # Fill whichever load-time stamp column the table has.
    now_dt = datetime.now()
    for row in dict_rows:
        for audit in base.AUDIT_TIME_COLUMNS:
            row.setdefault(audit, now_dt)

    _, db_no_source, csv_unused = base._mapping_report(
        [k for k in dict_rows[0].keys()], insert_columns
    )
    # The audit columns were stamped on above, so the table not having them is
    # not a mapping gap to report -- listing them reads as if the CSV carried
    # columns nobody asked for.
    audit_norms = {base.normalize_col(a) for a in base.AUDIT_TIME_COLUMNS}
    gaps = [c for c in db_no_source if base.normalize_col(c) not in audit_norms]
    csv_unused = [c for c in csv_unused if base.normalize_col(c) not in audit_norms]
    if gaps:
        print(f"  warn    {len(gaps)} column(s) of {label} the CSV does not fill (they load as NULL)")
        _note("", gaps)
    if csv_unused:
        print(f"  drop    {len(csv_unused)} CSV column(s) with no column in {label}")
        _note("", csv_unused)

    bad = base._validate_types(dict_rows, insert_columns, col_types, lengths)
    # Same shape {col: (count, samples)}, so one report covers both a value too
    # long for its column and one that is not the right kind of thing at all.
    bad.update(bad_guids(dict_rows, insert_columns, col_types))
    if bad:
        for col, (count, samples) in bad.items():
            print(f"  {col} ({col_types.get(col)}): {count} value(s) that do not fit, e.g. {samples}")
        guids = [c for c in bad if (col_types.get(c) or "").lower() == "uniqueidentifier"]
        if guids:
            print(f"  hint    {', '.join(guids)} holds uniqueidentifier and the values above are "
                  f"not GUIDs.\n          Either widen it to nvarchar(64), or decide what those "
                  f"rows should say instead.")
        raise SystemExit("  FAILED  nothing written: the values above do not fit their live "
                         "columns.\n          Fix them and rerun.")

    data = base._to_tuples(dict_rows, insert_columns, col_types)

    # Delete and insert share this one transaction, so a failure anywhere leaves
    # the previous load of this window in place rather than a hole where it was.
    try:
        deleted, inserted = replace_rows(cursor, tbl, insert_columns, key_cols, data)
        conn.commit()
        note = f", {deleted:,} replaced" if deleted else " (all new)"
        print(f"  wrote   {inserted:,} row(s){note}   (key: {', '.join(key_cols)})", flush=True)
    except pyodbc.Error as err:
        conn.rollback()
        raise SystemExit(f"  FAILED  rolled back, nothing written\n{_sql_message(err)}")


def check_db(csv_path, spec):
    """Read-only connectivity + existence + mapping + pre-flight. No writes."""
    table = os.getenv(spec["env"]) or spec["table"]
    print(f"  server  {SERVER}")
    print(f"  db      {DATABASE}")
    conn = get_connection()
    try:
        cursor = conn.cursor()
        schema, name = base._split_table(table)
        columns, identity, col_types, lengths = base._table_columns(cursor, schema, name)
        if not columns:
            print(f"  MISSING {spec['label']} does not exist -- create it out-of-band first")
            print(f"          {table}")
            return
        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{name}]")
        counted = cursor.fetchone()
        print(f"  table   {spec['label']}  ({table})")
        # COUNT(*) always returns a row; the fallback is for the table going away
        # between the column read above and here, which is the one case where a
        # count of nothing is the honest answer.
        print(f"          {counted[0] if counted else 0:,} row(s), {len(columns)} column(s)")
        _note("", columns)
        insert_columns = [c for c in columns if c not in identity]

        # The key is what makes a rerun a no-op, so its absence is the finding
        # that matters most here.
        key_cols, missing_key = resolve_key(spec["key"], insert_columns)
        if missing_key:
            print(f"  FATAL   no column for the key: {', '.join(missing_key)}")
            print(f"          Without those, every run re-inserts every row. Add them to {table}.")
        else:
            print(f"  key     {', '.join(key_cols)}")
            print("          Index these, ideally UNIQUE -- the dedup is an index seek per row")
            print("          with one, a full scan per row without.")

        if not csv_path or not os.path.exists(csv_path):
            print(f"  (no CSV at {csv_path}; skipping the mapping cross-check)")
            return
        dict_rows, csv_norm_names = base.parse_csv(csv_path)
        _, db_no_source, csv_unused = base._mapping_report(csv_norm_names, insert_columns)
        print(f"  csv     {os.path.basename(csv_path)}  {len(dict_rows):,} row(s)")
        audit_norms = {base.normalize_col(a) for a in base.AUDIT_TIME_COLUMNS}
        gaps = [c for c in db_no_source if base.normalize_col(c) not in audit_norms]
        csv_unused = [c for c in csv_unused if base.normalize_col(c) not in audit_norms]
        if gaps:
            print(f"  warn    {len(gaps)} column(s) of {spec['label']} the CSV does not fill")
            _note("", gaps)
        if csv_unused:
            print(f"  drop    {len(csv_unused)} CSV column(s) with no table column")
            _note("", csv_unused)

        # A gap column that is also NOT NULL is fatal, not a warning -- the insert
        # is rejected. Split the two apart so the check ends in a verdict rather
        # than a list to interpret.
        cursor.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND IS_NULLABLE = 'NO'",
            [schema, name],
        )
        required = {row[0] for row in cursor.fetchall()}
        fatal = [c for c in gaps if c in required]

        bad = base._validate_types(dict_rows, insert_columns, col_types, lengths)
        bad.update(bad_guids(dict_rows, insert_columns, col_types))
        for col, (count, samples) in bad.items():
            print(f"  {col} ({col_types.get(col)}): {count} value(s) that do not fit, e.g. {samples}")
        guids = [c for c in bad if (col_types.get(c) or "").lower() == "uniqueidentifier"]

        print()
        if missing_key:
            print(f"  FATAL   {spec['label']} has no column for the key, so nothing is "
                  f"recognised as already loaded")
        if fatal:
            print(f"  FATAL   {len(fatal)} NOT NULL column(s) the CSV does not fill, "
                  f"so the insert would be rejected")
            _note("", fatal)
        if bad:
            print(f"  FATAL   {len(bad)} column(s) whose values do not fit"
                  + (f" ({', '.join(guids)} is uniqueidentifier and holds non-GUIDs)"
                     if guids else ""))
        if not fatal and not bad and not missing_key:
            print(f"  OK      all {len(dict_rows):,} row(s) map and fit; the load should succeed")
        cursor.close()
    finally:
        conn.close()


def self_check():
    """No DB, no network: pin the file-picking decisions, which are the part a
    wrong guess turns into a silently wrong load."""
    import tempfile

    # A window with no file must be an error, never a fallback to some other
    # period -- that is the failure that looks like success.
    with tempfile.TemporaryDirectory() as tmp:
        open(os.path.join(tmp, "hc_agent_metrics_2026-10-01_2026-10-15.csv"), "w").close()
        found = csv_for_range("hc_agent_metrics_", tmp, "2026-10-01", "2026-10-15")
        assert found is not None, "a file on disk for that window must be found"
        assert os.path.basename(found) == "hc_agent_metrics_2026-10-01_2026-10-15.csv"
        try:
            csv_for_range("hc_agent_metrics_", tmp, "2026-09-01", "2026-09-15")
        except SystemExit as exc:
            assert "2026-09-01" in str(exc) and "hc_agent_metrics_2026-09-01" in str(exc), exc
        else:
            raise AssertionError("a missing window fell back to another period's CSV")
        # No window asked for -> newest, and a prefix matching nothing -> None.
        assert csv_for_range("hc_agent_metrics_", tmp, None, None) is not None
        assert csv_for_range("hc_location_performance_", tmp, None, None) is None

    # Both variables are needed; half a window is no window.
    assert requested_range({}) == (None, None)
    assert requested_range({"ZENOTI_START_DATE": "2026-10-01"}) == (None, None)
    assert requested_range({"ZENOTI_END_DATE": "2026-10-15"}) == (None, None)
    assert requested_range({"ZENOTI_START_DATE": "2026-10-01",
                            "ZENOTI_END_DATE": "2026-10-15"}) == ("2026-10-01", "2026-10-15")

    # Each report needs its own label, env var, table and key: a copy-paste slip
    # would load one report over the other, log the wrong name, or -- worst --
    # leave a key that matches nothing, which re-inserts everything every run.
    assert all({"label", "env", "table", "prefix", "key"} <= set(spec) for spec in REPORTS.values())
    assert all(spec["key"] for spec in REPORTS.values()), "a report with no key cannot dedup"
    assert len({s["table"] for s in REPORTS.values()}) == len(REPORTS), "tables collide"
    assert len({s["label"] for s in REPORTS.values()}) == len(REPORTS), "labels collide"
    assert len({s["env"] for s in REPORTS.values()}) == len(REPORTS), "env vars collide"
    assert all(s["table"].startswith("dbo.") for s in REPORTS.values())
    # The fallback is only used when the env var is unset, so it has to name a
    # table that exists -- `dbo.TABLE_ZENOTI_HC_*` does not, and the schema
    # script that trusted this line aimed its ALTERs at nothing for a whole run.
    assert all(s["table"].startswith("dbo.BRONZE_ZENOTI_HC_") for s in REPORTS.values()), \
        [s["table"] for s in REPORTS.values()]

    # The key is matched in normalized form on both sides, and resolved to the
    # table's own spelling before the SQL is built.
    assert resolve_key(("call_id",), ["CALL_ID", "AGENT_NAME"]) == (["CALL_ID"], [])
    assert resolve_key(("call_id", "nope"), ["CALL_ID"]) == (["CALL_ID"], ["nope"])
    assert resolve_key(("start_date",), ["START_DATE"]) == (["START_DATE"], [])

    # NULL-safe: a plain `=` would make every re-run of a plain agent pull look
    # new, because `date` is NULL there and NULL = NULL is unknown in SQL.
    assert _key_match(["a"]) == "(t.[a] = s.[a] OR (t.[a] IS NULL AND s.[a] IS NULL))"
    joined = _key_match(["a", "b"])
    assert joined.count("t.[a]") == 2 and joined.count("t.[b]") == 2, joined

    # A key repeated inside one CSV is dropped; the delete only sees the table,
    # so both copies would otherwise insert and the unique index would refuse the
    # whole batch.
    rows, dropped = drop_csv_dupes(
        [{"call_id": "a", "v": 1}, {"call_id": "b", "v": 2}, {"call_id": "a", "v": 3}], ["call_id"])
    assert [r["v"] for r in rows] == [1, 2] and dropped == 1
    assert drop_csv_dupes([], ["call_id"]) == ([], 0)

    # The delete has to carry the key values and nothing else, picked out by
    # position -- the key is not at the front of the table's column order here, so
    # a slice off the front would delete on the wrong values, and a delete that
    # matches nothing looks exactly like a successful one.
    class Stub:
        def __init__(self, rowcount=11):
            self.calls = []
            self.rowcount = rowcount
        def execute(self, sql, params=()):
            self.calls.append((sql, list(params)))
    cursor = Stub()
    deleted = delete_keys(cursor, "dbo.T", ["NAME", "ID", "NOTE"], ["ID"], [("x", 1, "n"), ("y", 2, "m")])
    sql, params = cursor.calls[0]
    assert deleted == 11 and len(cursor.calls) == 1, cursor.calls
    assert sql.startswith("DELETE t FROM dbo.T AS t WHERE EXISTS"), sql
    assert "s.[ID]" in sql and "t.[ID]" in sql and "s.[NAME]" not in sql, sql
    assert params == [1, 2], params          # the key column's values, in row order

    # Deletes all run before any insert, so nothing can be deleted after it was
    # written. Both batched, on their own column counts.
    cursor = Stub(3)
    assert replace_rows(cursor, "dbo.T", ["NAME", "ID"], ["ID"], [("x", 1)]) == (3, 3)
    assert [sql.split()[0] for sql, _ in cursor.calls] == ["DELETE", "INSERT"], cursor.calls
    assert cursor.calls[1][0].startswith("INSERT INTO dbo.T ([NAME], [ID]) VALUES ("), cursor.calls[1][0]
    assert "WHERE" not in cursor.calls[1][0], "the insert must not re-check what the delete removed"

    # Both ceilings, asserted as a property over every batch rather than as one
    # expected size: the version of this that asserted `chunked(2100, 1) == [2100]`
    # agreed with a 2100-parameter DELETE, and a real load refused it.
    assert MAX_PARAMS < 2100, "the point of the constant is to stay under the limit"
    assert MAX_ROWS <= 1000, "a VALUES list is capped at 1000 rows"
    for per_row in (1, 33, 200):
        batches = chunked(list(range(3000)), per_row)
        assert sum(len(b) for b in batches) == 3000, "every row must land in some batch"
        for batch in batches:
            assert len(batch) <= MAX_ROWS, (per_row, len(batch))
            assert len(batch) * per_row <= MAX_PARAMS, (per_row, len(batch) * per_row)
    assert len(chunked(list(range(5000)), 1)) == 5, "a 1-column key batches on rows"
    assert len(chunked([], 33)) == 0

    # A value that will not convert to uniqueidentifier names no column in the
    # driver's error, so it is caught here -- with the shape that actually
    # occurs, a GUID column that is mostly GUIDs.
    ok = "5a61174b-da67-4dd9-b0e8-9dedd0050a98"
    rows = [{"agent_id": ok}, {"agent_id": ok.upper()}, {"agent_id": None}, {"agent_id": ""},
            {"agent_id": "agent_2401k7mc4rfbfvtvw1a3nbk6pvwd"}]
    assert bad_guids(rows, ["AGENT_ID"], {"AGENT_ID": "uniqueidentifier"}) == \
        {"AGENT_ID": (1, ["agent_2401k7mc4rfbfvtvw1a3nbk6pvwd"])}, "guid check changed"
    # A varchar column full of the same value is not a problem.
    assert bad_guids(rows, ["AGENT_ID"], {"AGENT_ID": "varchar"}) == {}
    assert bad_guids([{"agent_id": ok}], ["AGENT_ID"], {"AGENT_ID": "uniqueidentifier"}) == {}

    print("self-check OK")
    return 0


def main():
    if SELF_CHECK:
        raise SystemExit(self_check())

    report = arg_value("--report")
    if not report:
        raise SystemExit(f"--report is required. Choose one of: {', '.join(REPORTS)}")
    if report not in REPORTS:
        raise SystemExit(f"unknown report {report!r}; choose one of: {', '.join(REPORTS)}")
    spec = REPORTS[report]

    start, end = requested_range()
    # CSVs land beside the pull scripts, not beside this loader: --csv wins, and
    # the fallback looks in zconnect/ where a manual pull leaves its file.
    csv_path = arg_value("--csv") or csv_for_range(spec["prefix"], _WORKDIR, start, end)

    if CHECK_DB:
        check_db(csv_path, spec)
        return

    if not csv_path:
        raise SystemExit(
            f"  FAILED  no CSV for {report}: no {spec['prefix']}*.csv in {_WORKDIR}"
            f"\n          Run sync_hc.py first, or pass --csv PATH."
        )
    if not os.path.exists(csv_path):
        # Said separately from the above: a path that was given and is not there
        # is a different mistake from no path being given, and naming the file
        # that is actually missing is the whole answer.
        raise SystemExit(f"  FAILED  no such file: {csv_path}")

    print(f"  read    {os.path.basename(csv_path)}", flush=True)
    _t0 = time.monotonic()
    dict_rows, _ = base.parse_csv(csv_path)
    print(f"  parse   {len(dict_rows):,} row(s) in {time.monotonic() - _t0:.1f}s", flush=True)
    if dict_rows and VERBOSE:
        print(f"  cols    {len(dict_rows[0])} column(s) in the CSV")
        _note("", list(dict_rows[0]))
        _sample(dict_rows[0])

    if DRY_RUN:
        print(f"  write   {spec['label']}  on {DATABASE}  (replace the window)")
        print("\n  --dry-run: no database connection made, nothing written.")
        return

    print(f"  write   {spec['label']}  on {DATABASE}  (replace the window)")
    conn = get_connection()
    try:
        cursor = conn.cursor()
        load_report(conn, cursor, spec["label"],
                    os.getenv(spec["env"]) or spec["table"], dict_rows, spec["key"])
        cursor.close()
    finally:
        conn.close()
    print("  done")


if __name__ == "__main__":
    main()
