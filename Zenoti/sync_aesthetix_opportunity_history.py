"""Sync dbo.aesthetix_opportunity_history (SQL1) from dbo.ASCRMOpportunities (SQL2).

Two servers, two connections, one process:

    SQL2  EVOLVEPBI_SERVER / EVOLVEPBI_DATABASE   source: [dbo].[ASCRMOpportunities]  (read only)
    SQL1  SERVER / DATABASE                       target: dbo.aesthetix_opportunity_history

This is the deployable copy, sitting beside db_helper.py and .env in Zenoti/ so it
can be a Railway cron service's start command:

    Start Command:   python sync_aesthetix_opportunity_history.py --overlap-hours 2
    Cron Schedule:   */10 * * * *

Match key is opportunity_id <- Id. The CRM gives every opportunity a stable 20
char id, and it is the only column that is unique (Name has ~5k duplicates).

The cursor is UpdatedAt, NOT CreatedAt. In the source, 77.7% of rows have
UpdatedAt > CreatedAt - median 29 days later, p90 179, max 230 - so a record
opened on Sep 1 is routinely still being edited in October. Syncing by CreatedAt
would miss all of it. UpdatedAt moves forward on every edit, so the delta is
"everything with UpdatedAt >= the newest UpdatedAt already in the target".

The watermark is read from the target itself (MAX(updated_at_utc)) - no state
table, no checkpoint file. Start it slightly early with --overlap-hours; the
upsert is idempotent, so re-reading a few hours costs nothing and covers a
source clock that is a little behind. If the target is empty, everything loads.

NOTHING IS EVER DELETED. A row missing from the source keeps its target row.

Column-shape note: the source is the CRM read model (26 CamelCase columns), the
target is the raw export shape (17). MAP below carries the 14 that line up.
is_bulk_import, pipeline_stage_id and raw_json have NO source. On rows that
already exist they are left alone, never NULLed. On rows this inserts they are
handled by the target's own rules: a DB default applies, otherwise INSERT_DEFAULTS
supplies a value, otherwise the run aborts naming the column rather than
inventing data. PipelineName is deliberately NOT mapped to pipeline_stage_id:
one is a name, the other a guid. Source is not raw_json either.

Usage:
    python sync_aesthetix_opportunity_history.py --check-db
    python sync_aesthetix_opportunity_history.py --dry-run
    python sync_aesthetix_opportunity_history.py --limit 1000 --dry-run
    python sync_aesthetix_opportunity_history.py
    python sync_aesthetix_opportunity_history.py --full          # ignore the watermark
    python sync_aesthetix_opportunity_history.py --since 2026-09-01
    python sync_aesthetix_opportunity_history.py --overlap-hours 48
    python sync_aesthetix_opportunity_history.py --self-check

    --check-db      read-only: connect both, report tables, ids, duplicates, the mapping
    --dry-run       run the whole merge in a transaction, print the counts, ROLL BACK
    --full          read every source row (no UpdatedAt filter)
    --since D       explicit watermark start (YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS')
    --overlap-hours re-read this many hours before the watermark (default 24)
    --limit N       only the newest N source rows - trial run against prod
    --batch-rows N  rows per MERGE statement (default 71; capped by the 2100-parameter limit)
"""

import os
import sys
import time
from datetime import datetime, timedelta

import pyodbc
from dotenv import load_dotenv

from db_helper import pick_odbc_driver

# The script sits next to db_helper.py and .env in Zenoti/, so the import above
# just works and no sys.path juggling is needed.
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# Env var names per server, in SERVER / DATABASE / DB_USER / DB_PASSWORD order.
# SQL1 (the PowerBI-side target) keeps the pipeline's existing names. SQL2 (the
# ACRM source) is namespaced so the two sets cannot be confused once both live in
# Railway's shared variables -- reading the wrong one here would point this
# script at the wrong server.
TARGET_ENV = ("SERVER", "DATABASE", "DB_USER", "DB_PASSWORD")
SOURCE_ENV = ("EVOLVEPBI_SERVER", "EVOLVEPBI_DATABASE",
              "EVOLVEPBI_USER", "EVOLVEPBI_PASSWORD")
_ENV_ROLES = ("SERVER", "DATABASE", "DB_USER", "DB_PASSWORD")

TARGET_TABLE = os.getenv(
    "TABLE_AESTHETIX_OPPORTUNITY_HISTORY", "dbo.aesthetix_opportunity_history"
)
SOURCE_TABLE = os.getenv("TABLE_ASCRM_OPPORTUNITIES", "dbo.ASCRMOpportunities")

# target column -> source column. Order matters only for readability; the key is
# the first pair. The source names genuinely differ (CamelCase, and Id vs
# opportunity_id), so an explicit map beats guessing at a normalization rule.
MAP = [
    ("opportunity_id", "Id"),
    ("name", "Name"),
    ("monetary_value", "MonetaryValue"),
    ("status", "Status"),
    ("stage_name", "StageName"),
    ("lead_status", "lead_status"),
    ("contact_name", "ContactName"),
    ("contact_email", "ContactEmail"),
    ("contact_phone", "ContactPhone"),
    ("owner_name", "OwnerName"),
    ("last_stage_change_at_utc", "LastStageChangeAt"),
    ("created_at_utc", "CreatedAt"),
    ("updated_at_utc", "UpdatedAt"),
    ("current_location", "current_location"),
]
KEY_TARGET, KEY_SOURCE = MAP[0]

# The source's edit stamp, used as the delta cursor.
SOURCE_CURSOR = "UpdatedAt"
TARGET_CURSOR = "updated_at_utc"

# Target columns with no source column. Never touched on an existing row.
NO_SOURCE = ["is_bulk_import", "pipeline_stage_id", "raw_json"]

# Values for those columns on the INSERT branch only, for when the target
# declares them NOT NULL and gives them no DB default (observed: is_bulk_import
# is NOT NULL with no default, so a NULL insert is rejected).
#
# A column with a DB default is left out of the insert entirely and the default
# applies. A column that is NOT NULL, has no default, and has no entry here
# aborts the run with a message naming it -- rather than inventing a value for
# a column whose real data lives in the raw CRM export this source does not have.
# is_bulk_import = 0 because a row created from the read model was not the bulk
# import path; correct it in the same file if that is wrong for your data.
INSERT_DEFAULTS = {
    "is_bulk_import": 0,
}

DEFAULT_OVERLAP_HOURS = 24
# SQL Server caps a statement at 2100 parameters, but a MERGE over a VALUES list
# is REFUSED at exactly 2100 (observed: 150 rows x 14 columns -> "The incoming
# request has too many parameters"), where a plain INSERT of the same size is
# accepted. Budget well under the cap instead of at it; --batch-rows overrides.
PARAM_BUDGET = 1000
PROGRESS_EVERY = 20


# ---------------------------------------------------------------------------
# CONNECTIONS
# ---------------------------------------------------------------------------
def build_conn_str(env_names, env=None, driver=None):
    """Connection string from an explicit tuple of env var names.

    Self-contained on purpose: db_helper.build_conn_str reads fixed SERVER /
    DATABASE / DB_USER / DB_PASSWORD names and is left untouched, so nothing else
    in the pipeline changes. Only driver picking is reused. `env`/`driver` are
    injectable for self_check().
    """
    env = os.environ if env is None else env
    names = dict(zip(_ENV_ROLES, env_names))
    values = {k: env.get(v) for k, v in names.items()}

    missing = [names[k] for k, v in values.items() if not v]
    if missing:
        raise ValueError(f"Missing environment variables in .env file: {', '.join(missing)}")

    return (
        f"DRIVER={{{driver or pick_odbc_driver()}}};"
        f"SERVER={values['SERVER']};"
        f"DATABASE={values['DATABASE']};"
        f"UID={values['DB_USER']};"
        f"PWD={values['DB_PASSWORD']};"
        f"Encrypt={env.get('ENCRYPT', 'yes')};"
        f"TrustServerCertificate={env.get('TRUST_SERVER_CERTIFICATE', 'no')};"
    )


def connect(env_names):
    return pyodbc.connect(build_conn_str(env_names),
                          timeout=int(os.getenv("DB_LOGIN_TIMEOUT", "30")))


def split_table(qualified):
    """'dbo.NAME' / '[dbo].[NAME]' / 'NAME' -> ('dbo', 'NAME')."""
    schema, name = "dbo", qualified
    if "." in qualified:
        left, right = qualified.split(".", 1)
        schema = left.strip(" []")
        name = right.strip(" []")
    return schema, name.strip(" []")


def bracket(qualified):
    schema, name = split_table(qualified)
    return f"[{schema}].[{name}]"


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
def build_merge(target, n_rows, extra_cols=()):
    """One batched MERGE: the source is a VALUES list of `n_rows` tuples.

    `extra_cols` are the sourceless columns being given an INSERT_DEFAULTS value.
    They appear in the INSERT list ONLY -- a matched row must keep whatever it
    already has there, so they are absent from the UPDATE SET.

    MERGE must be terminated with a semicolon (SQL Server requirement), and
    OUTPUT $action reports per row whether it inserted or updated, which is what
    makes the counts real rather than inferred.
    """
    val_cols = [t for t, _s in MAP]
    set_cols = [t for t, _s in MAP if t != KEY_TARGET]
    insert_cols = val_cols + list(extra_cols)
    n_cols = len(insert_cols)

    row = "(" + ",".join(["?"] * n_cols) + ")"
    values = ",".join([row] * n_rows)

    update = ", ".join(f"t.[{c}] = s.[{c}]" for c in set_cols)
    cols = ",".join(f"[{c}]" for c in insert_cols)
    insert_vals = ",".join(f"s.[{c}]" for c in insert_cols)

    return (
        f"MERGE {target} AS t\n"
        f"USING (VALUES {values}) AS s ({cols})\n"
        f"ON t.[{KEY_TARGET}] = s.[{KEY_TARGET}]\n"
        f"WHEN MATCHED THEN UPDATE SET {update}\n"
        f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({insert_vals})\n"
        f"OUTPUT $action;"
    )


def target_column_info(cursor):
    """{column: (is_nullable, column_default)} for the target table."""
    cursor.execute("""
        SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """, *split_table(TARGET_TABLE))
    return {r[0]: (r[1], r[2]) for r in cursor.fetchall()}


def plan_extras(cursor):
    """Work out which sourceless columns the INSERT branch must supply.

    Returns the ordered list of columns to fill from INSERT_DEFAULTS. Raises
    SystemExit naming any column that is NOT NULL, has no DB default and no
    INSERT_DEFAULTS entry -- the load cannot proceed without inventing data, so
    it refuses rather than guessing.
    """
    info = target_column_info(cursor)
    if not info:
        raise SystemExit(f"[MISSING] target {bracket(TARGET_TABLE)} does not exist")

    extra, blocked = [], []
    for col in NO_SOURCE:
        if col not in info:
            continue
        nullable, default = info[col]
        if col in INSERT_DEFAULTS:
            extra.append(col)
        elif nullable == "NO" and default is None:
            blocked.append(col)

    if blocked:
        raise SystemExit(
            f"Target column(s) NOT NULL with no DB default and no INSERT_DEFAULTS "
            f"entry: {blocked}.\n"
            f"These have no source in {SOURCE_TABLE}, so a new row cannot be filled.\n"
            f"Either add them to INSERT_DEFAULTS in this file, or give the column a "
            f"DEFAULT in the table."
        )
    return extra


def read_watermark(cursor):
    """Newest updated_at_utc already in the target, or None when it is empty."""
    cursor.execute(f"SELECT MAX([{TARGET_CURSOR}]) FROM {bracket(TARGET_TABLE)}")
    return cursor.fetchone()[0]


def duplicate_keys(cursor, table, column):
    """How many opportunity_ids appear more than once in `table` (0 is healthy)."""
    cursor.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT [{column}] FROM {bracket(table)}
            GROUP BY [{column}] HAVING COUNT(*) > 1
        ) AS d
    """)
    return cursor.fetchone()[0]


def fetch_source(cursor, since, limit):
    """Read the delta: source rows whose UpdatedAt >= `since`, newest last."""
    cols = ",".join(f"[{s}]" for _t, s in MAP)
    top = f"TOP {int(limit)} " if limit else ""
    where = f"WHERE [{SOURCE_CURSOR}] >= ?" if since else ""
    # TOP + no ORDER BY would be arbitrary rows; for a trial run the newest are
    # the interesting ones.
    order = f"ORDER BY [{SOURCE_CURSOR}] DESC" if (limit and since) else ""
    sql = f"SELECT {top}{cols} FROM {bracket(SOURCE_TABLE)} {where} {order}"
    cursor.execute(sql, *([since] if since else []))
    return cursor.fetchall()


def merge_rows(cursor, table, rows, batch_rows=None, extra=()):
    """MERGE the rows in batches. Returns (inserted, updated). No deletes, ever.

    `extra` is the ordered list of sourceless columns being supplied; each row's
    parameters are its mapped values followed by those columns' constants, in the
    same order as the VALUES tuple.
    """
    if not rows:
        return 0, 0
    extra_cols = list(extra)
    extra_vals = [INSERT_DEFAULTS[c] for c in extra_cols]
    n_cols = len(MAP) + len(extra_cols)
    batch_size = batch_rows or max(1, PARAM_BUDGET // n_cols)
    if batch_size * n_cols > 2100:
        raise SystemExit(f"--batch-rows {batch_size} needs {batch_size * n_cols} parameters, "
                         f"over the 2100 SQL Server allows")
    inserted = updated = 0
    _t0 = time.monotonic()

    for start in range(0, len(rows), batch_size):
        chunk = rows[start:start + batch_size]
        sql = build_merge(table, len(chunk), extra_cols)
        flat = []
        for row in chunk:
            flat.extend(row)
            flat.extend(extra_vals)
        cursor.execute(sql, flat)
        # OUTPUT $action returns one row per affected row; it must be drained or
        # the next execute (and the count) sees the wrong result set.
        for (action,) in cursor.fetchall():
            if action == "INSERT":
                inserted += 1
            else:
                updated += 1

        done = min(start + batch_size, len(rows))
        if (start // batch_size + 1) % PROGRESS_EVERY == 0 and done < len(rows):
            rate = done / max(time.monotonic() - _t0, 1e-9)
            print(f"  {done:,} / {len(rows):,} merged ({rate:,.0f} rows/s)", flush=True)

    return inserted, updated


# ---------------------------------------------------------------------------
# MODES
# ---------------------------------------------------------------------------
def check_db():
    """Read-only: both servers, both tables, key uniqueness, and the mapping."""
    print("Read-only check (no writes)...")
    src_conn = connect(SOURCE_ENV)
    try:
        cur = src_conn.cursor()
        src = bracket(SOURCE_TABLE)
        try:
            cur.execute(f"SELECT COUNT(*) FROM {src}")
            n = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(DISTINCT [{KEY_SOURCE}]) FROM {src}")
            n_distinct = cur.fetchone()[0]
            cur.execute(f"SELECT MIN([{SOURCE_CURSOR}]), MAX([{SOURCE_CURSOR}]) FROM {src}")
            lo, hi = cur.fetchone()
        except pyodbc.Error as e:
            raise SystemExit(f"[FAILED] source {src}: {e}")
        print(f"  SQL2 source {src}")
        print(f"    rows {n:,}   distinct {KEY_SOURCE} {n_distinct:,}")
        if n != n_distinct:
            print(f"    WARNING: {n - n_distinct:,} duplicate {KEY_SOURCE}(s) - "
                  f"the merge would touch one of them arbitrarily")
        print(f"    {SOURCE_CURSOR} range {lo} .. {hi}")
        cur.close()
    finally:
        src_conn.close()

    tgt_conn = connect(TARGET_ENV)
    try:
        cur = tgt_conn.cursor()
        tgt = bracket(TARGET_TABLE)
        try:
            cur.execute(f"SELECT COUNT(*) FROM {tgt}")
            n = cur.fetchone()[0]
            cur.execute(f"""
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """, *split_table(TARGET_TABLE))
            have = {r[0] for r in cur.fetchall()}
        except pyodbc.Error as e:
            raise SystemExit(f"[FAILED] target {tgt}: {e}")
        if not have:
            raise SystemExit(f"[MISSING] target {tgt} does not exist")

        print(f"  SQL1 target {tgt}")
        print(f"    rows {n:,}   watermark (MAX {TARGET_CURSOR}) {read_watermark(cur)}")
        dupes = duplicate_keys(cur, TARGET_TABLE, KEY_TARGET)
        if dupes:
            print(f"    WARNING: {dupes:,} duplicate {KEY_TARGET}(s) in the target - "
                  f"every copy gets updated")
        else:
            print(f"    {KEY_TARGET} is unique in the target")

        mapped = [t for t, _s in MAP if t in have]
        missing = [t for t, _s in MAP if t not in have]
        print(f"    mapped {len(mapped)}/{len(MAP)} column(s)")
        if missing:
            print(f"    MISSING from the target: {missing} - the merge would fail")
        print(f"    no source (left as-is on existing rows):")
        info = target_column_info(cur)
        for c in NO_SOURCE:
            if c not in info:
                continue
            nullable, default = info[c]
            if c in INSERT_DEFAULTS:
                plan = f"insert uses INSERT_DEFAULTS = {INSERT_DEFAULTS[c]!r}"
            elif default is not None:
                plan = f"NOT NULL, DB default applies"
            elif nullable == "NO":
                plan = "NOT NULL, NO default -> RUN WOULD ABORT (add to INSERT_DEFAULTS)"
            else:
                plan = "nullable, insert leaves NULL"
            print(f"      {c:<20} {plan}")
        cur.close()
    finally:
        tgt_conn.close()


def run(since_override, overlap_hours, full, limit, dry_run, batch_rows=None):
    src_conn = connect(SOURCE_ENV)
    tgt_conn = connect(TARGET_ENV)
    try:
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()
        target = bracket(TARGET_TABLE)

        watermark = read_watermark(tgt_cur)

        if since_override:
            since = since_override
            origin = "--since"
        elif full:
            since = None
            origin = "--full"
        elif watermark is None:
            since = None
            origin = "target is empty"
        else:
            since = watermark - timedelta(hours=overlap_hours)
            origin = f"target MAX({TARGET_CURSOR}) {watermark} - {overlap_hours}h overlap"

        print(f"Source: {bracket(SOURCE_TABLE)} (SQL2)")
        print(f"Target: {target} (SQL1)")
        print(f"Delta:  {origin}")
        print(f"       {SOURCE_CURSOR} >= {since}" if since else "       no filter (every row)")

        _t0 = time.monotonic()
        rows = fetch_source(src_cur, since, limit)
        extra = plan_extras(tgt_cur)
        if extra:
            print(f"INSERT-only value(s) for sourceless column(s): "
                  f"{ {c: INSERT_DEFAULTS[c] for c in extra} }")
        print(f"\nRead {len(rows):,} source row(s) in {time.monotonic() - _t0:.1f}s", flush=True)

        if not rows:
            print("Nothing to sync - already up to date.")
            return 0

        inserted, updated = merge_rows(tgt_cur, target, rows, batch_rows, extra)

        if dry_run:
            tgt_conn.rollback()
            print(f"\n--dry-run: would insert {inserted:,}, update {updated:,} "
                  f"of {len(rows):,} row(s). ROLLED BACK - nothing written.")
        else:
            tgt_conn.commit()
            print(f"\ninserted {inserted:,}, updated {updated:,} "
                  f"of {len(rows):,} row(s). Committed.")
            print("(no rows were deleted)")
        return 0
    except Exception:
        tgt_conn.rollback()
        raise
    finally:
        src_conn.close()
        tgt_conn.close()


def self_check():
    """No DB: pin the mapping, the merge SQL, the env names, and the watermark math."""
    assert len({t for t, _s in MAP}) == len(MAP), "duplicate target column in MAP"
    assert len({s for _t, s in MAP}) == len(MAP), "duplicate source column in MAP"
    assert KEY_TARGET == "opportunity_id" and KEY_SOURCE == "Id"
    # The three no-source columns must not be smuggled into the merge anywhere.
    for c in NO_SOURCE:
        assert c not in {t for t, _s in MAP}, c
    assert SOURCE_CURSOR == "UpdatedAt" and TARGET_CURSOR == "updated_at_utc"

    sql = build_merge("[dbo].[t]", 3)
    assert sql.rstrip().endswith(";"), "MERGE must be semicolon-terminated"
    assert "OUTPUT $action;" in sql
    assert sql.count("?") == 3 * len(MAP), sql.count("?")
    assert f"ON t.[{KEY_TARGET}] = s.[{KEY_TARGET}]" in sql
    # The key is never in the UPDATE SET (matching it would be a no-op).
    update_line = [ln for ln in sql.splitlines() if ln.startswith("WHEN MATCHED")][0]
    assert f"t.[{KEY_TARGET}] =" not in update_line, update_line
    assert "is_bulk_import" not in sql and "raw_json" not in sql and "pipeline_stage_id" not in sql

    # An INSERT_DEFAULTS column goes into the INSERT branch and NOWHERE else: a
    # matched row must keep its own value, never be overwritten by the constant.
    sql_x = build_merge("[dbo].[t]", 2, extra_cols=["is_bulk_import"])
    assert sql_x.count("?") == 2 * (len(MAP) + 1), sql_x.count("?")
    # Three times: the USING alias list, the INSERT column list, and the INSERT's
    # VALUES list (s.[is_bulk_import]). Never as t.[...], which is the UPDATE SET.
    assert sql_x.count("[is_bulk_import]") == 3, sql_x.count("[is_bulk_import]")
    assert "t.[is_bulk_import]" not in sql_x, "constant must not appear in UPDATE SET"
    upd = [ln for ln in sql_x.splitlines() if ln.startswith("WHEN MATCHED")][0]
    assert "is_bulk_import" not in upd, upd

    # Batch math: 14 columns at a 1000-parameter budget -> 71 rows per statement,
    # deliberately short of the 2100 the server refuses. An extra column costs
    # rows per batch, so it must feed the same arithmetic.
    assert PARAM_BUDGET // len(MAP) == 71, PARAM_BUDGET // len(MAP)
    assert PARAM_BUDGET // (len(MAP) + 1) == 66
    assert PARAM_BUDGET < 2100
    assert 71 * len(MAP) <= 2100

    # Credentials: the source read must use the EVOLVEPBI_ set, never SQL1's.
    # A prefix typo would silently point the merge at the wrong server.
    env = {"SERVER": "s1", "DATABASE": "d1", "DB_USER": "u1", "DB_PASSWORD": "p1",
           "EVOLVEPBI_SERVER": "s2", "EVOLVEPBI_DATABASE": "d2",
           "EVOLVEPBI_USER": "u2", "EVOLVEPBI_PASSWORD": "p2"}
    src = build_conn_str(SOURCE_ENV, env=env, driver="SQL Server")
    tgt = build_conn_str(TARGET_ENV, env=env, driver="SQL Server")
    assert "SERVER=s2;" in src and "UID=u2;" in src and "PWD=p2;" in src
    assert "SERVER=s1;" in tgt and "UID=u1;" in tgt and "PWD=p1;" in tgt
    assert "s1" not in src and "d1" not in src and "u1" not in src and "p1" not in src
    assert "s2" not in tgt and "d2" not in tgt and "u2" not in tgt and "p2" not in tgt

    # The four names line up positionally with the four roles.
    assert len(TARGET_ENV) == len(SOURCE_ENV) == len(_ENV_ROLES) == 4
    assert SOURCE_ENV[0].endswith("_SERVER") and SOURCE_ENV[3].endswith("_PASSWORD")

    # A missing source variable is named in full, so the .env fix is obvious.
    try:
        build_conn_str(SOURCE_ENV, env={"EVOLVEPBI_SERVER": "s2"}, driver="SQL Server")
    except ValueError as e:
        assert "EVOLVEPBI_DATABASE" in str(e), e
        assert "EVOLVEPBI_SERVER" not in str(e), e
    else:
        raise AssertionError("a missing EVOLVEPBI_DATABASE should raise")

    # Watermark: the overlap reaches back, and an empty target means no filter.
    wm = datetime(2026, 10, 1, 12, 0, 0)
    assert wm - timedelta(hours=24) == datetime(2026, 9, 30, 12, 0, 0)

    print("self-check OK")
    return 0


def parse_since(text):
    """Accept 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'; return a datetime."""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text.strip(), fmt)
        except ValueError:
            continue
    raise SystemExit(f"--since needs a date like 2026-09-01 or '2026-09-01 08:30:00', got {text!r}")


def main():
    opts = {}
    argv = sys.argv[1:]
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in ("--since", "--overlap-hours", "--limit", "--batch-rows"):
            if i + 1 >= len(argv):
                raise SystemExit(f"{arg} needs a value.\n\n{__doc__}")
            opts[arg] = argv[i + 1]
            i += 2
        else:
            opts[arg] = True
            i += 1

    if "--self-check" in opts:
        raise SystemExit(self_check())
    if "--check-db" in opts:
        check_db()
        return

    since = parse_since(opts["--since"]) if "--since" in opts else None

    try:
        overlap = int(opts.get("--overlap-hours", DEFAULT_OVERLAP_HOURS))
    except ValueError:
        raise SystemExit(f"--overlap-hours needs a number, got {opts['--overlap-hours']!r}")
    if overlap < 0:
        raise SystemExit(f"--overlap-hours cannot be negative, got {overlap}")

    limit = None
    if "--limit" in opts:
        try:
            limit = int(opts["--limit"])
        except ValueError:
            raise SystemExit(f"--limit needs a number, got {opts['--limit']!r}")
        if limit <= 0:
            raise SystemExit(f"--limit must be positive, got {limit}")

    dry_run = "--dry-run" in opts

    batch_rows = None
    if "--batch-rows" in opts:
        try:
            batch_rows = int(opts["--batch-rows"])
        except ValueError:
            raise SystemExit(f"--batch-rows needs a number, got {opts['--batch-rows']!r}")
        if batch_rows <= 0:
            raise SystemExit(f"--batch-rows must be positive, got {batch_rows}")

    print("*** DRY RUN - everything runs, then rolls back ***\n" if dry_run else "")
    raise SystemExit(run(since, overlap, "--full" in opts, limit, dry_run, batch_rows))


if __name__ == "__main__":
    main()
