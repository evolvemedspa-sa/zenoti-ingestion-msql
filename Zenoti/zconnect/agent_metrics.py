"""Agent Metrics.

POST https://hyperconnectapi.zenoti.com/dashboard/fetch_all_agent_data

One row per agent per day: call totals, average rating, average time to answer,
average call duration and sentiment. This is the endpoint behind the Agent
Metrics screen -- call_logs cannot produce the rating or time-to-answer columns
(guest_rating is null on every call row).

Verified against the Agent Metrics screen: Laura Cardinal came back
114 calls / 68 made / 46 received / 74 analyzed / 8.36 rating / 3.37s
time-to-answer / 83.35s duration / 58 positive / 1 negative, matching the UI.

The request body is exactly what the dashboard sends, with `day_wise` forced on:

    {"org_id": "...", "center_id": "<singular>", "agent_rating": null,
     "call_mode": 1, "call_sentiment": null, "call_type": null,
     "day_wise": true, "start_date": "20261015", "end_date": "20261015"}

Daily, not windowed, because that is the grain the client asked for: the
dashboard rolls days up to a month or a fortnight itself, and a windowed row
cannot be split back into days. day_wise=True is what makes the API return one
row per agent per day, and it carries every metric the aggregate row does --
verified 2026-10-17 against single-day pulls, per agent, exactly.

One consequence of daily rows: `start_date` / `end_date` are stamped with the
row's own day rather than the range asked for, so the row reads the same
however wide the window was. That keeps BRONZE_ZENOTI_HC_AGENT_METRICS's key
(agent_id, center_id, start_date, end_date, date) exact at day grain, so a
lookback run rewrites the day instead of stacking a second overlapping window
on top of it.

Traps worth knowing:
  * start_date / end_date are YYYYMMDD. An ISO string raises "invalid literal
    for int() with base 10"; an epoch timestamp parses but matches nothing and
    returns an empty list. Both int and str forms of YYYYMMDD work.
  * center_id is singular. Sending center_ids as a list raises a DynamoDB
    ValidationException that reads like a server fault.
  * A narrow window returns NOTHING, not an error. One day alone gave 0 agents
    on a center that has plenty over a fortnight, so give it room. Daily rows
    are not immune: an agent with no calls that day is simply absent.
  * No range cap observed -- 365 days returned fine, and with day_wise on that
    is 365 rows per agent rather than a wider row.
  * call_mode FILTERS. Measured across all 20 centers over a fortnight:
    omitted 73 agents, 0 -> 14, 1 -> 59, 2 -> 73, null -> 73. Unfiltered is the
    default here; --call-mode 1 reproduces the dashboard, which sends 1.
  * The day-wise rows and the aggregate row do not agree exactly: over one
    fortnight the day-wise rows summed to 499 calls where the aggregate said
    495. The daily column is the one the tables hold.

The output is already flat, column by column: _spread() walks the nested
`sentiment` / `direct` / `transferred` objects out to leaf columns, which is what
BRONZE_ZENOTI_HC_AGENT_METRICS is shaped for (direct_sentiment_Positive et al).
Without it those three cells would be Python dict reprs and 23 table columns
would load NULL.
"""
import argparse
import math
import os
import sys

import _client as zc

PATH = "/dashboard/fetch_all_agent_data"
MAX_ANSWER_TIME_SECONDS = 24 * 60 * 60


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch HyperConnect agent metrics.")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD; defaults to ZENOTI_START_DATE, else 7 days ago")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD; defaults to ZENOTI_END_DATE, else today")
    parser.add_argument("--center-id", default=None, help="one center (UUID or code); default is every center")
    parser.add_argument("--call-mode", type=int, default=None,
                        help="filters agents; unset (the default) returns everything, which is 73 agents "
                             "against 59 on a fortnight. The dashboard sends 1; pass --call-mode 1 to "
                             "reproduce the on-screen view exactly, or 0 for calls only (14)")
    parser.add_argument("--csv", default=None, help="output path")
    return parser.parse_args()


def _spread(mapping, prefix=""):
    """Flatten nested dicts to leaf columns, prefixing with the path taken.

    Recurses rather than spreading one level, so `direct.sentiment.Positive`
    becomes direct_sentiment_Positive instead of a Python dict repr in the cell.
    """
    row = {}
    for key, value in mapping.items():
        name = f"{prefix}{key}"
        if isinstance(value, dict):
            row.update(_spread(value, f"{name}_"))
        else:
            row[name] = value
    return row


def _agent_metrics(agent):
    return _spread({key: value for key, value in agent.items()
                    if key not in ("id", "name", "date")})


def _valid_answer_time(value):
    """Whether an average answer time is a plausible elapsed time in seconds."""
    if value is None:
        return True
    try:
        seconds = float(value)
    except (TypeError, ValueError, OverflowError):
        return True
    return math.isfinite(seconds) and 0 <= seconds <= MAX_ANSWER_TIME_SECONDS


def _invalid_answer_time_count(agent):
    return sum(
        not _valid_answer_time(value)
        for name, value in _agent_metrics(agent).items()
        if name.endswith("average_time_to_answer")
    )


def _flatten(agent, center_id, center_name, window_start, window_end):
    """One agent-day as a flat row, tagged with its center and its own day.

    Identifying columns come first so the CSV reads left to right in a sheet;
    the API's own field order has `name` near the end.

    start_date / end_date are the row's own day, not the queried window: with
    day_wise on, every row covers exactly one day, and labelling it with a
    seven-day range would make the row say something it does not mean. The
    window is only the fallback for a row the API returns without a date, which
    day_wise should never do -- main() counts those and says so.
    """
    day = zc.hc_iso_date(agent.get("date"))
    row = {
        "agent_name": agent.get("name"),
        "agent_id": agent.get("id"),
        "center_name": center_name,
        "center_id": center_id,
        "start_date": day or window_start,
        "end_date": day or window_end,
        "date": day,
    }
    metrics = _agent_metrics(agent)
    for name, value in metrics.items():
        if name.endswith("average_time_to_answer") and not _valid_answer_time(value):
            metrics[name] = None
    row.update(metrics)
    return row


def _body(center_id, start_date, end_date, call_mode):
    """The request body. day_wise is fixed true -- see the module docstring."""
    return {
        "org_id": zc.HC_ORG_ID,
        "center_id": center_id,
        "agent_rating": None,
        "call_mode": call_mode,
        "call_sentiment": None,
        "call_type": None,
        "day_wise": True,
        "start_date": zc.hc_yyyymmdd(start_date),
        "end_date": zc.hc_yyyymmdd(end_date),
    }


def fetch_center(center_id, center_name, args, start_date, end_date):
    """Every agent-day for one center. The response carries no paging fields, so
    this is a single call however wide the window."""
    payload = zc.hc_post(PATH, _body(center_id, start_date, end_date, args.call_mode))
    agents = payload.get("agents") or []
    invalid_answer_times = sum(_invalid_answer_time_count(agent) for agent in agents)
    if invalid_answer_times:
        print(f"  WARN    {center_name or center_id}: {invalid_answer_times} average answer "
              f"time value(s) outside 0..{MAX_ANSWER_TIME_SECONDS} seconds across metrics; "
              "exported as NULL")
    return [_flatten(agent, center_id, center_name, start_date, end_date)
            for agent in agents]


def main():
    args = parse_args()

    default_start, default_end = zc.default_date_range()
    start_date = args.start_date or default_start
    end_date = args.end_date or default_end

    if args.center_id:
        center_id = zc.resolve_center_id(args.center_id)
        centers = [(center_id, next((c.get("name") for c in zc.list_centers() if c["id"] == center_id), None))]
    else:
        centers = [(c["id"], c.get("name")) for c in zc.list_centers()]
    print(f"  {len(centers)} center(s)  {start_date} .. {end_date}")

    width = max(len(center_name or center_id) for center_id, center_name in centers)
    rows = []
    empty = []
    for center_id, center_name in centers:
        center_rows = fetch_center(center_id, center_name, args, start_date, end_date)
        rows.extend(center_rows)
        days = len({row["date"] for row in center_rows})
        print(f"  {center_name or center_id:<{width}}  {len(center_rows):>4} agent-day(s) over {days:>3} day(s)")
        if not center_rows:
            empty.append(center_name or center_id)

    # A row the API returned without a date keeps the window stamp, which is a
    # coarser label than the rest of the file carries. day_wise should make that
    # impossible, so say it rather than let mixed grain into the table.
    undated = sum(1 for row in rows if row["date"] is None)
    if undated:
        print(f"  WARN    {undated} row(s) came back with no date; stamped with the window instead")

    # One line about the empties rather than a note per center: an empty pull is
    # either a quiet window or a filter that cut everything, and both are read at
    # a glance from the count here.
    if empty and len(empty) < len(centers):
        print(f"  note    {len(empty)} center(s) returned no agent rows: {', '.join(empty)}")
    elif empty:
        print("  note    no center returned rows -- a narrow date range returns empty, not an error")

    path = args.csv or f"hc_agent_metrics_{start_date}_{end_date}.csv"
    if zc.write_csv(rows, zc.columns_of(rows), path):
        print(f"  wrote   {len(rows)} agent-day row(s)  {start_date} .. {end_date}  -> {os.path.basename(path)}")
    else:
        print(f"  wrote   nothing: no agent metrics between {start_date} and {end_date}")


if __name__ == "__main__":
    # _flatten() is the only non-obvious logic -- pin its shape down, and pin the
    # daily stamping with it: a row's own day, not the window asked for.
    assert _flatten(
        {"id": "a1", "name": "Jane", "calls": 3, "date": 20261003,
         "sentiment": {"Positive": 2, "Negative": 1},
         "direct": {"calls": 2, "sentiment": {"Positive": 1}}},
        "c1", "Center", "2026-10-01", "2026-10-15",
    ) == {
        "agent_name": "Jane", "agent_id": "a1", "center_name": "Center", "center_id": "c1",
        "start_date": "2026-10-03", "end_date": "2026-10-03", "date": "2026-10-03",
        "calls": 3, "sentiment_Positive": 2, "sentiment_Negative": 1,
        "direct_calls": 2, "direct_sentiment_Positive": 1,
    }, "flatten() output changed shape"

    # A row the API hands back without a date falls back to the window -- and
    # must not leak the string "None" into the date column.
    undated = _flatten({"id": "a1", "name": "Jane", "date": None}, "c1", "C", "2026-10-01", "2026-10-15")
    assert undated["date"] is None, undated
    assert (undated["start_date"], undated["end_date"]) == ("2026-10-01", "2026-10-15"), undated

    # The source can return an epoch-like value in a seconds metric; preserve
    # plausible values and leave impossible elapsed times NULL for SQL.
    answer_time = _flatten(
        {"id": "a1", "name": "Jane", "average_time_to_answer": 3.37},
        "c1", "C", "2026-10-01", "2026-10-15",
    )
    assert answer_time["average_time_to_answer"] == 3.37, answer_time
    bad_answer_time = _flatten(
        {"id": "a1", "name": "Jane", "average_time_to_answer": 1771703074.69},
        "c1", "C", "2026-10-01", "2026-10-15",
    )
    assert bad_answer_time["average_time_to_answer"] is None, bad_answer_time
    bad_direct_answer_time = _flatten(
        {"id": "a1", "name": "Jane",
         "direct": {"average_time_to_answer": 1771703074.69}},
        "c1", "C", "2026-10-01", "2026-10-15",
    )
    assert bad_direct_answer_time["direct_average_time_to_answer"] is None, bad_direct_answer_time
    assert _invalid_answer_time_count(
        {"average_time_to_answer": 3.37, "direct": {"average_time_to_answer": 1771703074.69}}
    ) == 1

    # Daily is not a flag any more: the body always asks for it, or the pull
    # silently returns one windowed row per agent and the table's day grain goes.
    body = _body("c1", "2026-10-03", "2026-10-05", None)
    assert body["day_wise"] is True, body
    assert (body["start_date"], body["end_date"]) == (20261003, 20261005), body

    assert zc.hc_yyyymmdd("2026-10-01") == 20261001
    try:
        main()
    except zc.ZConnectError as exc:
        sys.exit(f"error: {exc}")
