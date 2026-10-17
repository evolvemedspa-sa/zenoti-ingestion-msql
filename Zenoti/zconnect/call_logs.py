"""HyperConnect call logs, and the Agent Metrics / Location performance rollups.

POST https://hyperconnectapi.zenoti.com/features/v1/hcreports/call_logs

One row per call. The two dashboard screens are aggregations of exactly this
data, so there is no separate agent endpoint to find.

Field mapping, verified against the Agent Metrics screen (Laura Cardinal,
MTD: 114 calls / 68 made / 46 received / 74 analyzed / 58 positive / 1 negative
reproduced from this data):

  calls            every row
  calls made       call_type == 1   (outbound)
  calls received   call_type == 0   (inbound)
  calls analyzed   sentiment is not None   (NOT transcription_status)
  positive/negative/neutral   the sentiment value
  avg call duration           mean of call_duration (seconds)

`call_type == 2` is neither made nor received -- it is the bulk of the
agent-less rows (voicebot / missed), so it counts toward `calls` only.

Two Agent Metrics columns are NOT here: "Average rating" (guest_rating is null
on every row) and "Average time to answer" (no such field). Those come from
somewhere else -- capture the Agent Metrics screen's own XHR to find it.
"""
import argparse
import collections
import datetime
import os
import sys

import _client as zc

PATH = "/features/v1/hcreports/call_logs"
PAGE_SIZE_MAX = 200  # the API caps page_size here; larger values silently clamp

# `timestamp` is epoch milliseconds in UTC (e.g. 1790799734000 is
# 2026-09-30 20:22:14 UTC). The centres this org runs all sit at -04:00, so a
# readable column is derived with that offset; override with HC_UTC_OFFSET if
# you ever point this at another timezone.
HC_UTC_OFFSET = os.getenv("HC_UTC_OFFSET", "-04:00")


def parse_args():
    parser = argparse.ArgumentParser(description="Pull HyperConnect call logs.")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD; defaults to ZENOTI_START_DATE, else 7 days ago")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD; defaults to ZENOTI_END_DATE, else today")
    parser.add_argument("--center-id", default=None, help="one center (UUID or code); default is every center")
    parser.add_argument("--page-size", type=int, default=PAGE_SIZE_MAX, help="calls per page, max 200")
    parser.add_argument("--max-pages", type=int, default=200, help="safety cap per chunk")
    parser.add_argument("--by", choices=["raw", "agent", "center"], default="raw",
                        help="raw = one row per call; agent/center = the dashboard rollups")
    parser.add_argument("--csv", default=None, help="output path")
    return parser.parse_args()


def _offset_delta(text):
    """"-04:00" -> timedelta(hours=-4)."""
    sign = -1 if text.startswith("-") else 1
    hours, _, minutes = text.lstrip("+-").partition(":")
    return datetime.timedelta(hours=sign * int(hours), minutes=sign * int(minutes or 0))


def add_readable_time(rows, offset_text=HC_UTC_OFFSET):
    """Replace the raw epoch-ms `timestamp` with two ISO text columns.

    The API sends epoch milliseconds, which a spreadsheet renders as
    `1.79201E+12` -- unreadable, and wrong by seconds if that displayed text is
    copied back out. Both replacements are ISO 8601 text, so Excel leaves them
    alone. Nothing is lost: the UTC column carries the same instant the integer
    did, and sits in the same position.

    A value that will not parse keeps its original `timestamp` rather than
    becoming a guessed date.
    """
    delta = _offset_delta(offset_text)
    out = []
    for row in rows:
        converted = {}
        for key, value in row.items():
            if key != "timestamp":
                converted[key] = value
                continue
            try:
                moment = datetime.datetime.fromtimestamp(int(value) / 1000, datetime.timezone.utc)
            except (TypeError, ValueError):
                converted["timestamp"] = value
                continue
            converted["timestamp_utc"] = moment.strftime("%Y-%m-%d %H:%M:%S")
            converted["timestamp_local"] = (moment + delta).strftime("%Y-%m-%d %H:%M:%S")
        out.append(converted)
    return out


def fetch_chunk(center_ids, start, end, args):
    """Every call in one date window. Returns (rows, hit_page_cap)."""
    rows, token, page = [], None, 0
    while page < args.max_pages:
        body = {
            "org_id": zc.HC_ORG_ID,
            "start_date": start,
            "end_date": end,
            "center_ids": center_ids,
            "page_size": min(args.page_size, PAGE_SIZE_MAX),
        }
        if token:
            body["page_token"] = token
        payload = zc.hc_post(PATH, body)
        batch = payload.get("items") or []
        rows.extend(batch)
        page += 1
        token = payload.get("next_page_token")
        if not token or not batch:
            return rows, False
    return rows, True


def _call_type(row):
    return str(row.get("call_type"))


def _rollup(rows):
    """Counts shared by both rollups."""
    sentiment = collections.Counter(row.get("sentiment") for row in rows)
    durations = [float(row["call_duration"]) for row in rows
                 if row.get("call_duration") not in (None, "")]
    return {
        "calls": len(rows),
        "calls_made": sum(1 for row in rows if _call_type(row) == "1"),
        "calls_received": sum(1 for row in rows if _call_type(row) == "0"),
        "calls_analyzed": sum(1 for row in rows if row.get("sentiment")),
        "avg_call_duration_seconds": round(sum(durations) / len(durations)) if durations else "",
        "positive": sentiment.get("Positive", 0),
        "negative": sentiment.get("Negative", 0),
        "neutral": sentiment.get("Neutral", 0),
    }


def by_agent(rows):
    buckets = collections.defaultdict(list)
    for row in rows:
        buckets[row.get("agent_name") or "(no agent)"].append(row)
    out = [{"agent_name": name, **_rollup(group)} for name, group in buckets.items()]
    out.sort(key=lambda row: -row["calls"])
    return out


def by_center(rows):
    buckets = collections.defaultdict(list)
    names = {}
    for row in rows:
        key = row.get("center_id")
        names[key] = row.get("center_name")
        buckets[key].append(row)
    out = [
        {
            "center_name": names[key],
            "center_id": key,
            **_rollup(group),
            # call_category is the closest thing to the dashboard's "SmartBot
            # appointments"; unverified, so it is named plainly.
            "appointment_calls": sum(1 for row in group if row.get("call_category") == "appointment"),
        }
        for key, group in buckets.items()
    ]
    out.sort(key=lambda row: -row["calls"])
    return out


def main():
    args = parse_args()

    default_start, default_end = zc.default_date_range()
    start_date = args.start_date or default_start
    end_date = args.end_date or default_end

    if args.center_id:
        center_ids = [zc.resolve_center_id(args.center_id)]
        print("  centers 1 (--center-id)")
    else:
        # An empty list means "every center" to this route, and it returns all 15
        # HyperConnect-active locations -- including Scarsdale, NY, which serves
        # calls but is absent from /v1/centers. Passing the /v1/centers ids
        # instead missed Scarsdale and spent six calls on centers with no
        # HyperConnect activity at all.
        center_ids = []
        print("  centers all")

    rows, truncated = [], []
    for chunk_start, chunk_end in zc.date_chunks(start_date, end_date, zc.HC_MAX_SPAN_DAYS):
        chunk_rows, hit_cap = fetch_chunk(center_ids, chunk_start, chunk_end, args)
        rows.extend(chunk_rows)
        print(f"  {chunk_start} to {chunk_end}  {len(chunk_rows):>6,} call(s)")
        if hit_cap:
            truncated.append(f"{chunk_start}..{chunk_end}")

    if truncated:
        print(f"  WARN    hit --max-pages ({args.max_pages}) in {len(truncated)} chunk(s), so these "
              f"are incomplete -- raise --max-pages: {', '.join(truncated)}")

    if args.by == "agent":
        rows = by_agent(rows)
    elif args.by == "center":
        rows = by_center(rows)
    else:
        # Only the raw rows carry a timestamp; the rollups aggregate it away.
        rows = add_readable_time(rows)

    # Names are tagged with the source. --by agent used to write
    # hc_agent_metrics_*.csv, which silently clobbered agent_metrics.py's output
    # -- that one is the real Agent Metrics screen data (73 agents, with rating
    # and time-to-answer); this is a call-log approximation that has neither.
    default_name = f"hc_call_logs_{args.by}_{start_date}_{end_date}.csv" if args.by != "raw" \
        else f"hc_call_logs_{start_date}_{end_date}.csv"

    path = args.csv or default_name
    if zc.write_csv(rows, zc.columns_of(rows), path):
        print(f"  wrote   {len(rows):,} row(s) ({args.by})  {start_date} .. {end_date}"
              f"  -> {os.path.basename(path)}")
    else:
        print(f"  wrote   nothing: no calls between {start_date} and {end_date}")


def _self_check():
    """The decoded field mapping is the whole point of this file -- pin it down."""
    fixture = [
        {"call_type": 1, "sentiment": "Positive", "call_duration": "60"},
        {"call_type": 1, "sentiment": None, "call_duration": "120"},
        {"call_type": 0, "sentiment": "Negative", "call_duration": "30"},
        {"call_type": 2, "sentiment": "Neutral", "call_duration": ""},
    ]
    got = _rollup(fixture)
    assert got == {
        "calls": 4,
        "calls_made": 2,
        "calls_received": 1,
        "calls_analyzed": 3,
        "avg_call_duration_seconds": 70,
        "positive": 1,
        "negative": 1,
        "neutral": 1,
    }, got

    # The timestamp decode is the part a wrong guess would silently ruin --
    # epoch millis, UTC, replaced in place by two ISO text columns.
    assert add_readable_time([{"timestamp": "1790799734000"}])[0] == {
        "timestamp_utc": "2026-09-30 20:22:14", "timestamp_local": "2026-09-30 16:22:14"}
    assert add_readable_time([{"timestamp": "1790799734000"}], "00:00")[0]["timestamp_local"] == "2026-09-30 20:22:14"
    # Position is preserved: the pair lands where timestamp sat.
    assert list(add_readable_time([{"call_id": "c", "timestamp": "1790799734000", "agent_name": "A"}])[0]) == [
        "call_id", "timestamp_utc", "timestamp_local", "agent_name"]
    # A value that will not parse stays put rather than becoming a guessed date.
    assert add_readable_time([{"timestamp": ""}])[0] == {"timestamp": ""}
    assert add_readable_time([{"timestamp": "nonsense"}])[0] == {"timestamp": "nonsense"}
    assert add_readable_time([{}])[0] == {}


if __name__ == "__main__":
    _self_check()
    try:
        main()
    except zc.ZConnectError as exc:
        sys.exit(f"error: {exc}")
