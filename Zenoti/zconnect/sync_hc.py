"""Run the HyperConnect reports and Zenoti reviews, then load their SQL tables.

Named sync_hc.py, not run_all.py, because ../run_all.py is the other pipeline and
the two never call each other: that one runs the Zenoti/BLVD loads and its SCRIPTS
list has no zconnect entry. Report pulls live here, and review loading is built
into this script. Deployed they are two services on two images -- ../Dockerfile for the
pipeline, zconnect/Dockerfile for this -- so a single `python run_all.py` at the
repo root cannot start this by mistake.

Steps
  1. login_headers.capture_token() logs in through a fresh browser and returns the
     value of the `authorization` header that starts `Bearer AN:evolvemedspa|`,
     minus the "Bearer " prefix.
  2. That value is exported as HC_TOKEN to each HyperConnect child script.
     Nothing is written to .env. _client.py's load_dotenv() does not override
     variables that are already set, so a stale HC_TOKEN in .env cannot win.
     get_events.py uses ZENOTI_API_KEY instead; --only events skips browser login.
  3. Every report in SCRIPTS runs, one after another, each as its own process.
     One failing does not stop the rest unless --stop-on-error is given.
  4. The reports that have an Azure SQL table (INGEST) are then loaded into it by
     import_hc_report.py in hc_sql_helper/, except get_events.py's review CSV, which
     is loaded directly here into TABLE_HC_EVENTS. The CSV hand-off
     between the two processes goes to a scratch directory and is deleted once the
     load succeeds -- no stale CSV is left to be read by mistake later. A FAILED
     load moves its file into this folder and prints the path. --no-ingest turns
     step 4 off, leaving report CSVs here and reviews in output/.

    python sync_hc.py                                             # T-7 .. T-1, the default
    python sync_hc.py --lookback 1                                # T-1 alone, the cheapest run
    python sync_hc.py --t-minus 2                                 # window ends at T-2
    python sync_hc.py --start-date 2026-10-01 --end-date 2026-10-15
    python sync_hc.py --skip location_performance
    python sync_hc.py --only location_performance --hc-extras   # also location --org --timeseries
    python sync_hc.py --only location_performance                # pull + load, no CSV left behind
    python sync_hc.py --only events                             # review pull + load, API key only
    python sync_hc.py --only events --no-ingest                  # review CSV in output/
    python sync_hc.py --no-ingest                                # pull only
    python sync_hc.py --headed                                   # watch the login
    python sync_hc.py --token "AN:evolvemedspa|..."              # skip the browser login
    python sync_hc.py --selftest

The schedule is set here rather than in the report scripts: with no dates given
this asks for the last --lookback days ending at T-1, --lookback defaulting to 7.
So a deployed run re-asks for the previous week and none of the day it runs on.
That is deliberate and it is what makes a daily run recover from its own gaps:
a call HyperConnect had not finished indexing when yesterday's run asked for it,
or a whole day one failed run never loaded, is still inside the window today. The
cost is the pull, roughly seven times a single day's, plus rewriting those days'
rows. --lookback 1 asks for T-1 alone when that cost matters more.

It also overrides ZENOTI_START_DATE / ZENOTI_END_DATE, so a stale pair in the
deploy environment cannot quietly freeze the window.

The window is UTC days, which is what the API bounds call timestamps by. So the
day before the container's today has ended in every timezone covered, and a call
in the local evening (20:00-24:00 Eastern) belongs to the next UTC day's window --
inside a 7-day window either way.

--start-date / --end-date / --center-id are forwarded to every script, so each of
them must accept those flags (the ones I could read do). Anything else goes after
`--`, e.g.  python sync_hc.py -- --some-flag   (appended to every script).
"""
import argparse
import builtins
from collections import Counter
import csv
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, TextIO
from uuid import UUID

from dotenv import load_dotenv

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)          # Zenoti/ -- service_log.py and gdrive_helper.py
load_dotenv(os.path.join(_ROOT, ".env"))

if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import service_log  # noqa: E402  (importable only once _ROOT is on the path)
from zconnect.get_events import COLUMNS as REVIEW_COLUMNS  # noqa: E402


def print(*args, **kwargs):
    """Every line this file emits, stamped with the wall clock.

    Shadowing the builtin on purpose: it is the single place that catches all of
    them, including the echoed child output, where the alternative is a stamp on
    each of two dozen call sites and a silent miss on the next one added. Same
    format as the other pipeline's log() in ../run_all.py, so both read alike.
    """
    builtins.print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}]", *args, **kwargs)

# Run order, and also what --only/--skip accept. Reports without a configured
# SQL destination still run and leave their CSV in this folder.
# call_logs last: it pages through a chunked date range, so it is by far the
# longest pull, and a slow or truncated one should not hold up the other tables.
SCRIPTS = (
    "agent_metrics",
    # "feedback_metrics",
    "location_performance",
    "events",
    "call_logs",
)

# Extra flags per script when --hc-extras is on.
HC_EXTRAS = {"location_performance": ["--org", "--timeseries"]}

# These reports use the shared Zenoti API key and need no browser bearer token.
API_KEY_SCRIPTS = {"events"}

# Reports that have an Azure SQL table, mapped to the shared importer's
# --report name (events is loaded directly by this script). An absent report is left
# as its CSV, which is its only output.
INGEST = {
    "location_performance": "location_performance",
    "agent_metrics": "agent_metrics",
    # "feedback_metrics": "feedback_metrics",
    "events": "events",
    "call_logs": "call_logs",
}

# The loader lives in hc_sql_helper/ with the other scripts that only touch
# Azure SQL -- no API, nothing to pull. The pull scripts stay here, beside the
# CSVs they write, and this is the one place the two halves meet.
IMPORTER = os.path.join(_HERE, "hc_sql_helper", "import_hc_report.py")

ReviewRow = dict[str, Any]
REVIEW_DATE_COLUMNS = {"review_date", "event_date"}
REVIEW_ID_COLUMNS = {"center_id", "guest_id"}
REVIEW_REQUIRED_COLUMNS = {"center_id", "review_key", "event_id", "metadata_index", "metadata_json"}
REVIEW_BATCH_SIZE = 200
REVIEW_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")


def build_command(name, start_date=None, end_date=None, center_id=None,
                  hc_extras=False, passthrough=()):
    """argv for one script."""
    filename = "get_events.py" if name == "events" else f"{name}.py"
    cmd = [sys.executable, os.path.join(_HERE, filename)]
    if start_date:
        cmd += ["--start-date", start_date]
    if end_date:
        cmd += ["--end-date", end_date]
    if center_id:
        cmd += ["--center-id", center_id]
    if hc_extras:
        cmd += HC_EXTRAS.get(name, [])
    cmd += list(passthrough)
    return cmd


def get_token(args):
    """--token or an HC_TOKEN already in the environment wins; otherwise log in."""
    if args.token:
        return args.token.removeprefix("Bearer ").strip()
    import login_headers  # imported lazily: needs playwright, --token does not

    return login_headers.capture_token(
        headed=args.headed, user=args.user, password=args.password
    )


def t_minus(days, today=None):
    """ISO date for T-`days`, i.e. one whole day per run and no half of another.

    `date.today()` is the container's clock, and Railway containers are UTC while
    the business day is Eastern. For the last few hours of each UTC day the two
    disagree, so a run scheduled there would ask for a day that is still going.
    Asking for the *previous* day keeps this correct at any hour: the day before
    the container's today has ended in every timezone the reporting covers.

    `today` is a parameter so the arithmetic can be pinned in --selftest.
    """
    return ((today or date.today()) - timedelta(days=days)).isoformat()


def requested_window(args, today=None):
    """(start, end) this run will ask every script for.

    Explicit dates win and are passed through as given -- that is the manual
    backfill path, where a half-specified window is the caller's business. With
    neither, it is the last `--lookback` days ending at T-`--t-minus`: the
    deployed case, and deliberately not a fallback to ZENOTI_START_DATE, so an
    env var left over from a backfill cannot pin a scheduled run to one old
    window forever.

    Asking for more than one day is what makes the run self-healing. Report
    loaders rewrite matching days; the review loader refreshes matching keys.
    What the extra days buy is the call HyperConnect had not finished indexing
    when the previous run asked for it, a day a failed run never loaded, and any
    field the source corrected after the first load. The window is UTC days --
    the API bounds call timestamps by that -- so it is the same set of rows
    whatever timezone the container keeps.
    """
    if args.start_date or args.end_date:
        return args.start_date, args.end_date
    # Capture the date once so a run crossing midnight cannot mix two days.
    today = today or date.today()
    return (t_minus(args.t_minus + args.lookback - 1, today), t_minus(args.t_minus, today))


def _status(code):
    """One vocabulary for every child, so the summary column lines up and a
    failure is greppable rather than a sentence."""
    return "ok" if code == 0 else f"FAILED (exit {code})"


def _short(cmd):
    """argv as a person reads it: the script, its flags, and any path cut to its
    basename. The interpreter's full path and the scratch directory are the same
    on every line and say nothing."""
    shown = [os.path.basename(cmd[1])]
    for arg in cmd[2:]:
        shown.append(os.path.basename(arg) if os.sep in arg else arg)
    return " ".join(shown)


def _run_logged(cmd, env=None, prefix="          "):
    """Run a child, echoing its output under `prefix`, and return its exit code.

    Line by line rather than subprocess.call: a pull spends seconds per center
    and buffering the lot would look like a hang. The child writes UTF-8 into a
    pipe (hence PYTHONIOENCODING in every env built here), and undecodable bytes
    are replaced rather than raising in the parent.
    """
    proc = subprocess.Popen(cmd, env=env, cwd=_HERE, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True,
                            encoding="utf-8", errors="replace", bufsize=1)
    stdout = proc.stdout
    # stdout=PIPE above makes this never None. The assert is what says so, and it
    # fails loudly here rather than hanging on a read that returns nothing.
    assert stdout is not None, "stdout=PIPE means there is a stream"
    with stdout:
        for line in stdout:
            print(prefix + line.rstrip(), flush=True)
    return proc.wait()


def run_script(name, token, csv_path=None, **options):
    """Run one script, supplying HC_TOKEN when needed. Returns (status, seconds)."""
    cmd = build_command(name, **options)
    path = cmd[1]
    if not os.path.exists(path):
        print(f"  pull    SKIPPED, {os.path.basename(path)} not found in this folder")
        return "missing", 0.0

    # Appended last so it beats a --csv that arrived via `--` passthrough:
    # argparse keeps the final occurrence, and sync_hc owns where the hand-off
    # file goes.
    if csv_path:
        cmd += ["--csv", csv_path]
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    if token is not None and name not in API_KEY_SCRIPTS:
        env["HC_TOKEN"] = token
    print(f"  pull    {_short(cmd)}")
    started = time.time()
    code = _run_logged(cmd, env=env)
    elapsed = time.time() - started
    status = _status(code)
    print(f"  pull    {status}  {elapsed:.1f}s")
    return status, elapsed


def qualified_review_table(value: str) -> tuple[str, str, str]:
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
        if not REVIEW_IDENTIFIER.fullmatch(part):
            raise ValueError("TABLE_HC_EVENTS contains an invalid SQL identifier.")
        names.append(part)
    return names[0], names[1], f"[{names[0]}].[{names[1]}]"


def parse_review_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def read_event_reviews(handle: TextIO) -> tuple[list[ReviewRow], int, int]:
    """Parse/validate all CSV records and keep the latest version of each key."""
    reader = csv.DictReader(handle)
    if reader.fieldnames != list(REVIEW_COLUMNS):
        raise ValueError("CSV headers must exactly match zconnect/get_events.py's review columns.")
    reviews: dict[str, ReviewRow] = {}
    records = 0
    for records, raw in enumerate(reader, 1):
        if None in raw or any(value is None for value in raw.values()):
            raise ValueError(f"CSV record {records}: wrong number of fields.")
        row: ReviewRow = {}
        for column in REVIEW_COLUMNS:
            value = raw[column]
            if value is None:
                raise ValueError(f"CSV record {records}, column {column}: missing field.")
            try:
                if not value and column in REVIEW_REQUIRED_COLUMNS:
                    raise ValueError("a required value is blank")
                if column in REVIEW_ID_COLUMNS:
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
                elif column in REVIEW_DATE_COLUMNS:
                    row[column] = parse_review_timestamp(value)
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
class ReviewColumnInfo:
    kind: str
    length: int | None


def validate_review_destination(cursor: Any, schema: str, name: str, table: str, rows: list[ReviewRow]) -> None:
    cursor.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH "
        "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        (schema, name),
    )
    columns = {row[0].lower(): ReviewColumnInfo(row[1].lower(), row[2]) for row in cursor.fetchall()}
    expected = {*REVIEW_COLUMNS, "date_inserted"}
    if set(columns) != expected:
        missing, extra = sorted(expected - set(columns)), sorted(set(columns) - expected)
        raise ValueError(f"{table} must have the review schema; missing: {missing}; extra: {extra}.")
    for column, info in columns.items():
        allowed = {"nvarchar", "nchar"}
        if column in REVIEW_ID_COLUMNS:
            allowed = {"uniqueidentifier", "nvarchar", "varchar", "char", "nchar"}
        elif column == "review_key":
            allowed = {"char", "varchar", "nchar", "nvarchar"}
        elif column in REVIEW_DATE_COLUMNS or column == "date_inserted":
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
        for column in REVIEW_COLUMNS:
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


def apply_event_reviews(cursor: Any, table: str, rows: list[ReviewRow]) -> tuple[int, int, int]:
    """Load typed JSON batches; the caller commits or rolls back all changes."""
    if not rows:
        return 0, 0, 0
    fields = ", ".join(f"[{column}]" for column in REVIEW_COLUMNS)
    cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
    stage_types = {"review_key": "char(64)", "metadata_index": "int", "rating": "int",
                   **{column: "uniqueidentifier" for column in REVIEW_ID_COLUMNS},
                   **{column: "datetime2(7)" for column in REVIEW_DATE_COLUMNS}}
    definitions = ", ".join(f"[{column}] {stage_types.get(column, 'nvarchar(max)')} "
                            f"{'NOT NULL' if column in REVIEW_REQUIRED_COLUMNS else 'NULL'} "
                            f"{'PRIMARY KEY' if column == 'review_key' else ''}" for column in REVIEW_COLUMNS)
    json_fields = ", ".join(f"[{column}] {stage_types.get(column, 'nvarchar(max)')} '$.{column}'"
                             for column in REVIEW_COLUMNS)
    assignments = ", ".join(f"t.[{column}] = s.[{column}]" for column in REVIEW_COLUMNS if column != "review_key")
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
        SELECT {', '.join('s.[' + column + ']' for column in REVIEW_COLUMNS)}, SYSUTCDATETIME()
        FROM @reviews AS s
        WHERE NOT EXISTS (SELECT 1 FROM {table} AS t WITH (UPDLOCK, HOLDLOCK)
                          WHERE t.[review_key] = s.[review_key]);
        SET @inserted = @@ROWCOUNT;
        SELECT @inserted AS inserted, @updated AS updated;
    """
    inserted, updated = 0, 0
    for start in range(0, len(rows), REVIEW_BATCH_SIZE):
        batch = rows[start:start + REVIEW_BATCH_SIZE]
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


def load_event_reviews(csv_path: str) -> None:
    """Validate and load this run's reviews in one transaction.

    Existing review_key values are refreshed for equal or newer event dates;
    older versions and reviews outside the CSV are retained. New rows get a
    UTC date_inserted, while refreshes keep their original insertion timestamp.
    """
    configured = os.getenv("TABLE_HC_EVENTS")
    if not configured:
        raise ValueError("Set TABLE_HC_EVENTS in Zenoti/.env to the review destination table.")
    schema, name, table = qualified_review_table(configured)
    path = Path(csv_path)
    with path.open(encoding="utf-8-sig", newline="") as handle:
        rows, records, duplicates = read_event_reviews(handle)
    print(f"          Target: {table}; {len(rows):,} reviews from {records:,} CSV records; "
          f"{duplicates:,} repeated versions")
    print(f"          Reviews by source: {dict(Counter(row['source'] or 'unknown' for row in rows))}")
    if not rows:
        print("          No reviews to ingest.")
        return

    from db_helper import get_connection  # lazy: --no-ingest and --selftest need no ODBC

    connection = get_connection()
    cursor = None
    try:
        connection.autocommit = False
        cursor = connection.cursor()
        validate_review_destination(cursor, schema, name, table, rows)
        inserted, updated, older = apply_event_reviews(cursor, table, rows)
        connection.commit()
        print(f"          Ingested: {inserted:,} inserted, {updated:,} refreshed, "
              f"{older:,} older versions skipped.")
    except Exception:
        connection.rollback()
        raise
    finally:
        try:
            if cursor is not None:
                cursor.close()
        finally:
            connection.close()


def build_ingest_command(name, csv_path):
    """argv that loads `csv_path` into `name`'s table."""
    if name == "events":
        raise ValueError("Review CSVs are loaded directly by load_event_reviews().")
    return [sys.executable, IMPORTER, "--report", INGEST[name], "--csv", csv_path]


def should_ingest(name):
    """Whether the report has a configured SQL destination."""
    return name in INGEST


def ingest_report(name, csv_path):
    """Load one report's CSV into its table. Returns (status, seconds).

    A failure here leaves the CSV in place and keeps a copy in this folder --
    the loader writes in one transaction rather than half-writing,
    so the file is what you need to see why.
    """
    started = time.time()
    if name == "events":
        print("  load    review CSV -> TABLE_HC_EVENTS (in sync_hc.py)")
        try:
            load_event_reviews(csv_path)
            code = 0
        except Exception as exc:
            print(f"          ERROR: {exc}")
            code = 1
    else:
        cmd = build_ingest_command(name, csv_path)
        print(f"  load    {_short(cmd)}")
        code = _run_logged(cmd, env=dict(os.environ, PYTHONIOENCODING="utf-8"))
    elapsed = time.time() - started
    status = _status(code)
    print(f"  load    {status}  {elapsed:.1f}s")
    return status, elapsed


def _selftest():
    # Named after real SCRIPTS entries, so the test drives the configuration
    # that actually runs rather than a script this folder no longer pulls.
    cmd = build_command("agent_metrics", "2026-10-01", "2026-10-15", "BEL", False, ["--x"])
    assert cmd[1].endswith("agent_metrics.py")
    assert cmd[2:] == ["--start-date", "2026-10-01", "--end-date", "2026-10-15",
                       "--center-id", "BEL", "--x"], cmd
    assert build_command("agent_metrics")[2:] == []
    assert build_command("location_performance", hc_extras=True)[2:] == ["--org", "--timeseries"]
    assert build_command("agent_metrics", hc_extras=True)[2:] == []
    assert "events" in SCRIPTS and should_ingest("events")
    assert Path(build_command("events")[1]).name == "get_events.py"
    assert "feedback_metrics" not in SCRIPTS and not should_ingest("feedback_metrics")
    assert build_command("events", "2026-11-01", "2026-11-16", "BEL", True)[2:] == [
        "--start-date", "2026-11-01", "--end-date", "2026-11-16", "--center-id", "BEL",
    ]

    class A:
        token = "Bearer AN:evolvemedspa|abc "
    assert get_token(A) == "AN:evolvemedspa|abc"

    # The window a deployed run asks for. It ends at T-1, not at today: a window
    # ending on the day the run happens asks for a day that is still going. That
    # and the month-end arithmetic are the mistakes that quietly leave gaps, so
    # both are pinned here.
    assert t_minus(1, date(2026, 11, 1)) == "2026-10-31", "T-1 must cross a month end"
    assert t_minus(2, date(2026, 3, 1)) == "2026-02-27", "T-2 must cross a month end"
    assert t_minus(0, date(2026, 10, 16)) == "2026-10-16"

    # The CLI default, read off the parser instead of restated here: a fixture
    # carrying its own default would keep passing after the flag changed.
    assert parse_args([]).lookback == 7, "a deployed run must re-ask for the week"
    assert parse_args(["--only", "events"]).only == ["events"]
    event_window = requested_window(parse_args(["--only", "events"]), date(2026, 11, 17))
    assert event_window == ("2026-11-10", "2026-11-16"), event_window
    assert build_command("events", *event_window)[2:] == [
        "--start-date", "2026-11-10", "--end-date", "2026-11-16",
    ], "events must receive the previous seven complete days"

    class W:
        def __init__(self, start=None, end=None, t=1, lookback=7):
            self.start_date, self.end_date = start, end
            self.t_minus, self.lookback = t, lookback
    assert requested_window(W()) == (t_minus(7), t_minus(1)), "a bare run must be T-7 .. T-1"
    assert requested_window(W(lookback=1)) == (t_minus(1), t_minus(1)), "lookback 1 is T-1 alone"
    assert requested_window(W(t=2)) == (t_minus(8), t_minus(2))
    # A wider lookback ends in the same place and reaches further back: the point
    # is to re-ask for days already loaded, so the end must not move with it.
    assert requested_window(W(lookback=3)) == (t_minus(3), t_minus(1))
    assert requested_window(W(t=2, lookback=3)) == (t_minus(4), t_minus(2))
    assert requested_window(W("2026-10-01", "2026-10-15")) == ("2026-10-01", "2026-10-15")
    # An explicit window must win outright: that is the backfill path, and dates
    # given there are not to be nudged by a day.
    assert requested_window(W("2026-10-01", "2026-10-15", t=5, lookback=7)) == ("2026-10-01", "2026-10-15")

    # The ingest step only runs for reports that have a table, and it must point
    # at the loader that accepts its CSV schema.
    assert set(INGEST) <= set(SCRIPTS), set(INGEST) - set(SCRIPTS)
    assert os.path.dirname(IMPORTER) == os.path.join(_HERE, "hc_sql_helper"), IMPORTER
    assert os.path.exists(IMPORTER), IMPORTER
    cmd = build_ingest_command("agent_metrics", r"C:\tmp\x.csv")
    assert cmd[1] == IMPORTER and cmd[2:] == ["--report", "agent_metrics", "--csv", r"C:\tmp\x.csv"], cmd
    cmd = build_ingest_command("call_logs", r"C:\tmp\call_logs.csv")
    assert cmd[2:] == ["--report", "call_logs", "--csv", r"C:\tmp\call_logs.csv"], cmd
    try:
        build_ingest_command("events", r"C:\tmp\reviews.csv")
    except ValueError:
        pass
    else:
        raise AssertionError("Reviews must never use the timeline subprocess loader")
    _selftest_review_csv()
    _selftest_sync()
    print("selftest ok")


def _selftest_review_csv() -> int:
    from io import StringIO

    row: dict[str, str] = {column: "" for column in REVIEW_COLUMNS}
    row.update(center_id="00000000-0000-0000-0000-000000000001", review_key="a" * 64,
               event_id="12345678", metadata_index="0", rating="5", comments='  Great, "care"!\n\u2605  ',
               review_date="2023-02-23T19:33:25", event_date="2026-11-01T14:55:22.123",
               source="GOOGLE", metadata_json='{"review_text":"  Original text  "}')

    def fixture(records: list[dict[str, str]]) -> StringIO:
        handle = StringIO(newline="")
        writer: csv.DictWriter[str] = csv.DictWriter(handle, fieldnames=REVIEW_COLUMNS)
        writer.writeheader()
        writer.writerows(records)
        handle.seek(0)
        return handle

    older = dict(row, comments="old", event_date="2026-10-01T12:00:00")
    newer = dict(row, comments="latest", event_date="2026-11-02T12:00:00Z")
    native = dict(row, review_key="b" * 64, source="ZENOTI", comments="", rating="",
                  review_reply_status="False")
    parsed, total, duplicates = read_event_reviews(fixture([newer, older, row, native]))
    assert total == 4 and duplicates == 2 and len(parsed) == 2
    assert parsed[0]["comments"] == "latest" and parsed[1]["comments"] == ""
    assert parsed[1]["rating"] is None and parsed[1]["guest_id"] is None
    assert parsed[1]["review_reply_status"] == "False"
    fidelity, _, _ = read_event_reviews(fixture([row]))
    assert fidelity[0]["comments"] == row["comments"] and fidelity[0]["metadata_json"] == row["metadata_json"]
    assert parse_review_timestamp("2026-11-01T14:00:00-05:00") == datetime(2026, 11, 1, 19)
    for column, value in (("rating", "-1"), ("metadata_index", "-1"), ("center_id", "bad-id"),
                          ("event_id", ""), ("metadata_json", "oops"), ("event_date", "not-a-date")):
        try:
            read_event_reviews(fixture([dict(row, **{column: value})]))
        except ValueError:
            pass
        else:
            raise AssertionError(f"Invalid {column} was accepted")
    try:
        read_event_reviews(StringIO("centerid,send_date\nold,2026-11-01\n"))
    except ValueError:
        pass
    else:
        raise AssertionError("The old timeline CSV was accepted")
    for value in ("dbo.EVENTS; DELETE FROM x", "otherdb.dbo.EVENTS", "dbo.E'VENTS", ""):
        try:
            qualified_review_table(value)
        except ValueError:
            pass
        else:
            raise AssertionError("An unsafe SQL identifier was accepted")
    assert qualified_review_table("[dbo].[EVENTS]")[2] == "[dbo].[EVENTS]"
    print("review CSV self-check passed; no database connection or files written.")
    return 0


def _selftest_sync():
    """Exercise orchestration without browser, API, SQL or lasting CSV writes."""
    from contextlib import redirect_stdout
    from io import StringIO
    from pathlib import Path
    from unittest.mock import patch

    from zconnect.get_events import COLUMNS, write_csv

    module = sys.modules[__name__]
    with tempfile.TemporaryDirectory(prefix="hc_sync_test_", dir=_HERE) as directory:
        # Child processes are simulated, but run_script still checks its path.
        for name in SCRIPTS:
            Path(directory, Path(build_command(name)[1]).name).touch()

        def scenario(argv, *, pull_code=0, load_code=0, publish=True):
            calls, exports = [], []

            def child(cmd, env: dict[str, str] | None = None, prefix="          "):
                calls.append(cmd)
                assert env is not None, "sync must supply the child environment"
                assert env["PYTHONIOENCODING"] == "utf-8"
                if cmd[1] == IMPORTER:
                    path = Path(cmd[cmd.index("--csv") + 1])
                    assert path.is_file(), "load must receive this run's CSV"
                    assert cmd[cmd.index("--report") + 1] != "events"
                    return load_code
                name = "events" if Path(cmd[1]).name == "get_events.py" else Path(cmd[1]).stem
                assert cmd[cmd.index("--start-date") + 1] == "2026-11-01"
                assert cmd[cmd.index("--end-date") + 1] == "2026-11-16"
                assert cmd[cmd.index("--center-id") + 1] == "BEL"
                if name == "events":
                    assert env.get("HC_TOKEN") == os.environ.get("HC_TOKEN")
                else:
                    assert env["HC_TOKEN"] == "test-token"
                if publish and (name != "events" or pull_code == 0):
                    path = (Path(cmd[cmd.index("--csv") + 1]) if "--csv" in cmd
                            else Path(directory, "output", "reviews_test.csv"))
                    if name == "events":
                        write_csv([], path)  # No reviews still produces a valid header-only CSV.
                    else:
                        path.write_text("test CSV", encoding="utf-8")
                    exports.append(path)
                return pull_code if name == "events" else 0

            def review_load(csv_path):
                calls.append(["inline_reviews", csv_path])
                path = Path(csv_path)
                assert path.is_file(), "review load must receive this run's CSV"
                assert path.read_text(encoding="utf-8-sig").strip() == ",".join(COLUMNS)
                if load_code:
                    raise RuntimeError("simulated review load failure")

            argv = [*argv, "--start-date", "2026-11-01", "--end-date", "2026-11-16",
                    "--center-id", "BEL"]
            with patch.object(module, "_HERE", directory), \
                    patch.object(module, "_run_logged", side_effect=child), \
                    patch.object(module, "load_event_reviews", side_effect=review_load), \
                    patch.object(module, "get_token", return_value="test-token") as login, \
                    redirect_stdout(StringIO()):
                code = run(parse_args(argv))
            return code, calls, exports, login.call_count

        code, calls, exports, logins = scenario(["--only", "events"])
        assert code == 0 and logins == 0 and len(calls) == 2
        assert calls[-1][0] == "inline_reviews" and not exports[0].exists()

        code, calls, exports, logins = scenario(["--only", "events", "--no-ingest"])
        assert code == 0 and logins == 0 and len(calls) == 1
        assert "--csv" not in calls[0] and exports[0].is_file()

        code, calls, _, logins = scenario(["--only", "events", "agent_metrics"])
        assert code == 0 and logins == 1
        assert calls[1][0] == "inline_reviews" and calls[3][1] == IMPORTER

        code, calls, _, _ = scenario(["--only", "events", "agent_metrics"], pull_code=1)
        assert code == 1 and len(calls) == 3 and calls[-1][1] == IMPORTER

        code, calls, exports, _ = scenario(
            ["--only", "events", "agent_metrics", "--stop-on-error"], load_code=1,
        )
        assert code == 1 and len(calls) == 2 and not exports[0].exists()
        assert Path(directory, "events.csv").is_file(), "failed review loads must keep their CSV"

        code, calls, _, _ = scenario(
            ["--only", "events", "agent_metrics", "--stop-on-error"], publish=False,
        )
        assert code == 1 and len(calls) == 1, "missing review CSV must fail and stop"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run HyperConnect reports and Zenoti reviews, then load SQL.")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD, forwarded to every script")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD, forwarded to every script")
    parser.add_argument("--center-id", default=None, help="UUID or code, forwarded to every script")
    parser.add_argument("--t-minus", type=int, default=1,
                        help="days back the default window ends; 1 = T-1 (default). "
                             "Ignored when --start-date/--end-date are given")
    parser.add_argument("--lookback", type=int, default=7,
                        help="days the default window spans, ending at --t-minus; 7 (default) "
                             "means T-7..T-1, 1 means T-1 alone. Days already loaded are "
                             "rewritten rather than trusted, so widening this only costs the pull")
    parser.add_argument("--only", nargs="+", choices=SCRIPTS, help="run just these scripts")
    parser.add_argument("--skip", nargs="+", choices=SCRIPTS, default=[], help="leave these out")
    parser.add_argument("--hc-extras", action="store_true",
                        help="also run location_performance with --org --timeseries")
    parser.add_argument("--stop-on-error", action="store_true", help="stop at the first failing script")
    parser.add_argument("--no-ingest", dest="ingest", action="store_false",
                        help="skip SQL loads; leave report CSVs here and review CSVs in output/")
    parser.add_argument("--token", default=None,
                        help="use this token (with or without 'Bearer ') instead of logging in")
    parser.add_argument("--headed", action="store_true", help="show the login browser")
    parser.add_argument("--user", default=None, help="login username, if not in .env")
    parser.add_argument("--password", default=None, help="login password, if not in .env")
    parser.add_argument("--selftest", action="store_true", help="run the offline check and exit")
    parser.add_argument("passthrough", nargs="*", help="extra flags for every script, after `--`")
    return parser.parse_args(argv)


def main():
    args = parse_args()
    if args.selftest:
        _selftest()
        return 0

    # The log is this service's only lasting output -- Railway's console scrolls
    # away -- so it starts before the first line and uploads from a finally: a
    # failed run's log is the one worth having.
    log_path = service_log.start("hc_ingestion_logs")
    try:
        return run(args)
    finally:
        service_log.upload(log_path)


def run(args):
    selected = [name for name in (args.only or SCRIPTS) if name not in args.skip]
    if not selected:
        print("nothing to run")
        return 0

    start_date, end_date = requested_window(args)
    window = f"{start_date} .. {end_date}"
    print(f"\n{'=' * 64}")
    print(f" HyperConnect sync   {window}")
    print(f" reports            {', '.join(selected)}")
    print(f" ingest             {'on' if args.ingest else 'off (--no-ingest)'}")
    print(f"{'=' * 64}")

    token = None
    if any(name not in API_KEY_SCRIPTS for name in selected):
        print("\nStep 1/3  Login")
        # The token itself is not printed: it is a live credential.
        token = get_token(args)
    else:
        print("\nStep 1/3  Using Zenoti API key; browser login not needed")

    print(f"\nStep 2/3  Pull and load  ({len(selected)} report(s))")
    # Ingested reports hand their CSV to the loader and nothing else, so it goes
    # to a scratch directory rather than this folder: a kept CSV goes stale and
    # then gets loaded by mistake. A failed load keeps its file -- see
    # ingest_report.
    scratch = tempfile.mkdtemp(prefix="hc_reports_") if args.ingest else None
    results = []
    for name in selected:
        print(f"\n  {name}")
        csv_path = os.path.join(scratch, f"{name}.csv") if scratch and should_ingest(name) else None
        status, elapsed = run_script(
            name, token, csv_path=csv_path,
            start_date=start_date, end_date=end_date, center_id=args.center_id,
            hc_extras=args.hc_extras, passthrough=args.passthrough,
        )
        results.append((name, status, elapsed))

        if status == "ok" and csv_path:
            if not os.path.exists(csv_path):
                if name == "events":
                    # The review exporter always publishes a header, even with
                    # zero reviews. A missing file is not a successful export.
                    print("  load    FAILED, review exporter did not produce its CSV")
                    results.append((f"{name} -> SQL", "FAILED (missing CSV)", 0.0))
                else:
                    # The other pullers write nothing when there are no rows.
                    print("  load    skipped, the API returned no rows")
            else:
                ingest_status, ingest_elapsed = ingest_report(name, csv_path)
                results.append((f"{name} -> SQL", ingest_status, ingest_elapsed))
                if ingest_status == "ok":
                    os.remove(csv_path)  # the CSV was only a hand-off
                else:
                    # Rescue it out of the scratch dir before the cleanup below
                    # wipes it: a refused load is exactly when the file matters.
                    kept = os.path.join(_HERE, os.path.basename(csv_path))
                    shutil.move(csv_path, kept)
                    print(f"  kept    {os.path.basename(kept)}  (load failed, file is in this folder)")

        if results[-1][1].startswith("FAILED") and args.stop_on_error:
            print("  stopped: --stop-on-error")
            break

    if scratch:
        # Failed loads were moved out above; what is left is the ingested CSVs.
        shutil.rmtree(scratch, ignore_errors=True)

    failed = [n for n, s, _ in results if s != "ok"]
    not_run = [n for n in selected if n not in {r[0] for r in results}]

    print("\nStep 3/3  Summary")
    width = max((len(n) for n, _, _ in results), default=0)
    for name, status, elapsed in results:
        print(f"  {name:<{width}}  {status:<16}{elapsed:7.1f}s")
    if not_run:
        print(f"  {'not run:':<{width}}  {', '.join(not_run)}")
    print(f"\n  {len(results) - len(failed)} ok, {len(failed)} failed")
    return 0 if not failed and not not_run else 1


if __name__ == "__main__":
    sys.exit(main())
