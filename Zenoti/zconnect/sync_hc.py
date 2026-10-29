"""Orchestrator for this folder only: log in once, capture the Bearer token, run
every HyperConnect report script and load the ones that have a table.

Named sync_hc.py, not run_all.py, because ../run_all.py is the other pipeline and
the two never call each other: that one runs the Zenoti/BLVD loads and its SCRIPTS
list has no zconnect entry, and this one never reaches outside this folder for its
own steps. Deployed they are two services on two images -- ../Dockerfile for the
pipeline, zconnect/Dockerfile for this -- so a single `python run_all.py` at the
repo root cannot start this by mistake.

Steps
  1. login_headers.capture_token() logs in through a fresh browser and returns the
     value of the `authorization` header that starts `Bearer AN:evolvemedspa|`,
     minus the "Bearer " prefix.
  2. That value is exported as HC_TOKEN in the environment of each child script.
     Nothing is written to .env. _client.py's load_dotenv() does not override
     variables that are already set, so a stale HC_TOKEN in .env cannot win.
  3. Every report in SCRIPTS runs, one after another, each as its own process.
     One failing does not stop the rest unless --stop-on-error is given.
  4. The reports that have an Azure SQL table (INGEST) are then loaded into it by
     import_hc_report.py in hc_sql_helper/. Their CSV is only a hand-off between
     the two processes, so it goes to a scratch directory and is deleted once the
     load succeeds -- no stale CSV is left to be read by mistake later. A FAILED
     load moves its file into this folder and prints the path. --no-ingest turns
     step 4 off, which leaves every CSV here instead.

    python sync_hc.py                                             # T-7 .. T-1, the default
    python sync_hc.py --lookback 1                                # T-1 alone, the cheapest run
    python sync_hc.py --t-minus 2                                 # window ends at T-2
    python sync_hc.py --start-date 2026-10-01 --end-date 2026-10-15
    python sync_hc.py --skip location_performance
    python sync_hc.py --only location_performance --hc-extras   # also location --org --timeseries
    python sync_hc.py --only location_performance                # pull + load, no CSV left behind
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
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import date, datetime, timedelta

from dotenv import load_dotenv

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)          # Zenoti/ -- service_log.py and gdrive_helper.py
load_dotenv(os.path.join(_ROOT, ".env"))

if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import service_log  # noqa: E402  (importable only once _ROOT is on the path)


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
# longest pull, and a slow or truncated one should not hold up the two small
# tables that have nothing to do with it.
SCRIPTS = ("agent_metrics", "feedback_metrics", "location_performance", "events", "call_logs")

# Extra flags per script when --hc-extras is on.
HC_EXTRAS = {"location_performance": ["--org", "--timeseries"]}

# Reports that have an Azure SQL table, mapped to import_hc_report.py's
# --report name. A report absent from here is pulled and left as its CSV, which
# is its only output. Adding a table to another report is a one-line edit here.
INGEST = {
    "location_performance": "location_performance",
    "agent_metrics": "agent_metrics",
    "feedback_metrics": "feedback_metrics",
    "events": "events",
    "call_logs": "call_logs",
}

# Do not attempt the SQL handoff unless its destination is configured.
INGEST_REQUIRED_ENV = {"events": ("TABLE_HC_EVENTS",)}

# The loader lives in hc_sql_helper/ with the other scripts that only touch
# Azure SQL -- no API, nothing to pull. The pull scripts stay here, beside the
# CSVs they write, and this is the one place the two halves meet.
IMPORTER = os.path.join(_HERE, "hc_sql_helper", "import_hc_report.py")


def build_command(name, start_date=None, end_date=None, center_id=None,
                  hc_extras=False, passthrough=()):
    """argv for one script."""
    cmd = [sys.executable, os.path.join(_HERE, f"{name}.py")]
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


def requested_window(args):
    """(start, end) this run will ask every script for.

    Explicit dates win and are passed through as given -- that is the manual
    backfill path, where a half-specified window is the caller's business. With
    neither, it is the last `--lookback` days ending at T-`--t-minus`: the
    deployed case, and deliberately not a fallback to ZENOTI_START_DATE, so an
    env var left over from a backfill cannot pin a scheduled run to one old
    window forever.

    Asking for more than one day is what makes the run self-healing, and the
    loader rewrites a day rather than skipping it, so an overlap is a refresh:
    what the extra days buy is the call HyperConnect had not finished indexing
    when the previous run asked for it, a day a failed run never loaded, and any
    field the source corrected after the first load. The window is UTC days --
    the API bounds call timestamps by that -- so it is the same set of rows
    whatever timezone the container keeps.
    """
    if args.start_date or args.end_date:
        return args.start_date, args.end_date
    return (t_minus(args.t_minus + args.lookback - 1), t_minus(args.t_minus))


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
    """Run one script with HC_TOKEN set. Returns (status, seconds)."""
    path = os.path.join(_HERE, f"{name}.py")
    if not os.path.exists(path):
        print(f"  pull    SKIPPED, {os.path.basename(path)} not found in this folder")
        return "missing", 0.0

    cmd = build_command(name, **options)
    # Appended last so it beats a --csv that arrived via `--` passthrough:
    # argparse keeps the final occurrence, and sync_hc owns where the hand-off
    # file goes.
    if csv_path:
        cmd += ["--csv", csv_path]
    env = dict(os.environ, HC_TOKEN=token, PYTHONIOENCODING="utf-8")
    print(f"  pull    {_short(cmd)}")
    started = time.time()
    code = _run_logged(cmd, env=env)
    elapsed = time.time() - started
    status = _status(code)
    print(f"  pull    {status}  {elapsed:.1f}s")
    return status, elapsed


def build_ingest_command(name, csv_path):
    """argv that loads `csv_path` into `name`'s table."""
    return [sys.executable, IMPORTER, "--report", INGEST[name], "--csv", csv_path]


def should_ingest(name, environ=None):
    """Whether the report has a configured SQL destination."""
    environ = os.environ if environ is None else environ
    return name in INGEST and all(
        environ.get(key) for key in INGEST_REQUIRED_ENV.get(name, ())
    )


def ingest_report(name, csv_path):
    """Load one report's CSV into its table. Returns (status, seconds).

    A failure here leaves the CSV in place and keeps a copy in this folder --
    the loader replaces the window in one transaction rather than half-writing,
    so the file is what you need to see why.
    """
    cmd = build_ingest_command(name, csv_path)
    print(f"  load    {_short(cmd)}")
    started = time.time()
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
    assert {"feedback_metrics", "events"} <= set(SCRIPTS)
    assert should_ingest("feedback_metrics", {})
    assert not should_ingest("events", {})
    assert should_ingest("events", {"TABLE_HC_EVENTS": "dbo.BRONZE_ZENOTI_EVENTS_REVIEWS"})

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
    argv = sys.argv
    sys.argv = ["sync_hc.py"]
    try:
        assert parse_args().lookback == 7, "a deployed run must re-ask for the week"
    finally:
        sys.argv = argv

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
    # at the loader in hc_sql_helper/ with that report's own --report name.
    assert set(INGEST) <= set(SCRIPTS), set(INGEST) - set(SCRIPTS)
    assert os.path.dirname(IMPORTER) == os.path.join(_HERE, "hc_sql_helper"), IMPORTER
    assert os.path.exists(IMPORTER), IMPORTER
    cmd = build_ingest_command("agent_metrics", r"C:\tmp\x.csv")
    assert cmd[1] == IMPORTER and cmd[2:] == ["--report", "agent_metrics", "--csv", r"C:\tmp\x.csv"], cmd
    print("selftest ok")


def parse_args():
    parser = argparse.ArgumentParser(description="Log in, then run every report script.")
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
                        help="do not load the reports that have a table; leave their CSVs in this folder")
    parser.add_argument("--token", default=None,
                        help="use this token (with or without 'Bearer ') instead of logging in")
    parser.add_argument("--headed", action="store_true", help="show the login browser")
    parser.add_argument("--user", default=None, help="login username, if not in .env")
    parser.add_argument("--password", default=None, help="login password, if not in .env")
    parser.add_argument("--selftest", action="store_true", help="run the offline check and exit")
    parser.add_argument("passthrough", nargs="*", help="extra flags for every script, after `--`")
    return parser.parse_args()


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

    print("\nStep 1/3  Login")
    # The token itself is not printed: login_headers already says it succeeded,
    # and the value is a live credential that would go to the deploy log.
    token = get_token(args)

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
                # write_csv writes nothing when there are no rows, so this is a
                # real "the API returned nothing", not a missing file.
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