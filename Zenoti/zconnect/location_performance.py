"""Location performance.

POST https://hyperconnectapi.zenoti.com/dashboard/fetch_centers_performance

One row per center per day, which is the grain the client asked for: the
dashboard rolls days up to a month or a fortnight itself, and a windowed total
cannot be split back into days.

This route has no day-wise switch -- it answers per-center totals for whatever
range it is given -- so a day at a time is the only way to get daily rows. One
call returns every center, so a seven-day lookback costs seven calls, each
returning all 15 active locations. Verified 2026-10-17: pulling three days one
at a time and summing them matches the same three days asked for as one window,
per numeric field (negative calls 10 = 10, appointments 1 = 1). Averages do not
add up, and are not supposed to: each row carries its own day's average rating.

Verified against the screen: Tribeca, NY came back overall_rating 10,
total_negative_calls 0, sb_total_appointments 0, google_rating 0 -- the #1 row.

Note the date format here is the opposite of the agent routes: ISO strings, not
YYYYMMDD integers. The two /dashboard handlers were written differently.

Two further routes sit on the same screen and are opt-in:

  --org        POST /dashboard/fetch_org_performance          the KPI cards above
               the table (org totals, each with a curr/prev pair), plus
               call_category and centers_per_feature counts.
  --timeseries POST /dashboard/fetch_org_level_call_volume_timeseries_plot
               the call-volume chart: incoming / outgoing / missed per day.

Both take ISO dates and ignore center_ids -- they are org-wide by definition.
"""
import argparse
import datetime
import os
import sys

import _client as zc

PATH = "/dashboard/fetch_centers_performance"
ORG_PATH = "/dashboard/fetch_org_performance"
TIMESERIES_PATH = "/dashboard/fetch_org_level_call_volume_timeseries_plot"
DEFAULT_PAGE_SIZE = 15


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch HyperConnect location performance.")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD; defaults to ZENOTI_START_DATE, else 7 days ago")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD; defaults to ZENOTI_END_DATE, else today")
    parser.add_argument("--center-id", default=None, help="one center (UUID or code); default is every center")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="rows per page, capped at 15 server-side")
    parser.add_argument("--sort-by", default="overall_rating",
                        choices=["overall_rating", "sb_revenue", "total_negative_calls",
                                 "sb_total_appointments", "google_rating"],
                        help="matches the screen's 'Quick sort by'")
    parser.add_argument("--sort-order", default="desc", choices=["asc", "desc"])
    parser.add_argument("--top", type=int, default=None, help="keep only the first N rows after sorting")
    parser.add_argument("--csv", default=None, help="output path")
    parser.add_argument("--org", action="store_true",
                        help="also pull the org-level KPI cards (active locations, ratings, revenue, "
                             "negative calls, call categories)")
    parser.add_argument("--timeseries", action="store_true",
                        help="also pull the org call-volume chart (incoming / outgoing / missed per day)")
    return parser.parse_args()


def _org_rows(metrics):
    """{metric, curr, prev} per KPI. overview values are {"curr": x, "prev": y};
    call_category and centers_per_feature are flat counts with no prev."""
    rows = []
    for group, value in metrics.items():
        for name, entry in (value or {}).items():
            if isinstance(entry, dict):
                rows.append({"metric": f"{group}.{name}", "curr": entry.get("curr"), "prev": entry.get("prev")})
            else:
                rows.append({"metric": f"{group}.{name}", "curr": entry, "prev": None})
    return rows


def fetch_org(start_date, end_date):
    """The org KPI cards above the center table."""
    payload = zc.hc_post(ORG_PATH, {
        "org_id": zc.HC_ORG_ID,
        "start_date": start_date,
        "end_date": end_date,
        "center_timezone": "-04:00",
        "type": 1,
    })
    return _org_rows(payload.get("org_metrics") or {})


def _timeseries_rows(payload):
    """{date, <label>: value} per day.

    The daily points are the leading x_axis values, one per data point; the
    trailing value is the range maximum (23:59:59 of the last day), so slice to
    the data length rather than taking the whole axis.
    """
    series = payload.get("data") or []
    if not series:
        return []
    points = (payload.get("x_axis") or {}).get("values") or []
    dates = points[: len(series[0].get("data") or [])]
    rows = []
    for index, day in enumerate(dates):
        local = datetime.datetime.fromtimestamp(day / 1000, datetime.timezone.utc)
        row = {"date": local.date().isoformat()}
        for entry in series:
            row[entry.get("label")] = (entry.get("data") or [])[index]
        rows.append(row)
    return rows


def fetch_timeseries(start_date, end_date):
    """The org call-volume chart: incoming / outgoing / missed per day."""
    payload = zc.hc_post(TIMESERIES_PATH, {
        "org_id": zc.HC_ORG_ID,
        "start_date": start_date,
        "end_date": end_date,
    })
    return _timeseries_rows(payload)


def fetch(args, start_date, end_date, center_ids):
    """Every active location in one call.

    There is no paging to do. page_size clamps at 15 server-side (20, 50 and 100
    all return 15) and that 15 is the complete set -- the org reports exactly 15
    active_locations, and the call returns those same 15. next_token comes back
    non-null but is not a cursor: feeding it back returns an empty page, so it is
    ignored rather than looped over. Six centers in /v1/centers are absent here
    (Corporate Center, Warehouse Center and four that are not live); Scarsdale is
    here but not in /v1/centers.
    """
    body = {
        "org_id": zc.HC_ORG_ID,
        "center_ids": center_ids,
        "start_date": start_date,
        "end_date": end_date,
        "next_token": None,
        "page_size": min(args.page_size, DEFAULT_PAGE_SIZE),
        "query_execution_id": None,
        "sort_by": args.sort_by,
        "sort_order": args.sort_order,
        "type": 1,
    }
    payload = zc.hc_post(PATH, body)
    return payload.get("center_metrics") or []


def _stamp(rows, day):
    """Label each row with the day it covers, in place.

    Both date columns get the day, so the row reads the same however wide the
    window was and the table's key (center_id, start_date, end_date) stays exact
    at day grain -- a lookback run rewrites the day instead of stacking another
    overlapping window on top of it. The payload carries no period at all, so
    without this a row is indistinguishable from the same center's row for any
    other day and the table could only hold one snapshot.
    """
    for row in rows:
        row["start_date"] = day
        row["end_date"] = day
    return rows


def fetch_daily(args, start_date, end_date, center_ids):
    """[(day, rows)] -- the window, one call per day.

    Days are inclusive at both ends, matching how the route treats its own
    range: three days asked for as a window return the same totals as the three
    single-day calls.
    """
    first = datetime.date.fromisoformat(start_date)
    last = datetime.date.fromisoformat(end_date)
    days = []
    while first <= last:
        day = first.isoformat()
        days.append((day, _stamp(fetch(args, day, day, center_ids), day)))
        first += datetime.timedelta(days=1)
    return days


def main():
    args = parse_args()

    default_start, default_end = zc.default_date_range()
    start_date = args.start_date or default_start
    end_date = args.end_date or default_end

    # An empty list means "every center" to this route, so it is the default.
    # That also picks up centers HyperConnect knows but /v1/centers does not --
    # Scarsdale, NY shows up on the dashboard but is absent from the Zenoti list.
    center_ids = [zc.resolve_center_id(args.center_id)] if args.center_id else []

    rows = []
    for day, day_rows in fetch_daily(args, start_date, end_date, center_ids):
        if args.top:
            day_rows = day_rows[: args.top]
        rows.extend(day_rows)
        print(f"  {day}  {len(day_rows):>3} center(s)")

    path = args.csv or f"hc_location_performance_{start_date}_{end_date}.csv"
    if zc.write_csv(rows, zc.columns_of(rows), path):
        print(f"  wrote   {len(rows)} center-day row(s)  {start_date} .. {end_date}  -> {os.path.basename(path)}")
    else:
        print(f"  wrote   nothing: no location performance between {start_date} and {end_date}")

    if args.org:
        org_rows = fetch_org(start_date, end_date)
        org_path = f"hc_org_performance_{start_date}_{end_date}.csv"
        if zc.write_csv(org_rows, zc.columns_of(org_rows), org_path):
            print(f"  wrote   {len(org_rows)} org metric(s)  -> {os.path.basename(org_path)}")

    if args.timeseries:
        series_rows = fetch_timeseries(start_date, end_date)
        series_path = f"hc_call_volume_timeseries_{start_date}_{end_date}.csv"
        if zc.write_csv(series_rows, zc.columns_of(series_rows), series_path):
            print(f"  wrote   {len(series_rows)} day(s)  -> {os.path.basename(series_path)}")


if __name__ == "__main__":
    # The two flatteners are the non-obvious bit -- pin their shape down, plus
    # the day stamping: a row carries its own day, never the window asked for.
    assert _org_rows({
        "overview": {"active_locations": {"curr": 15, "prev": 15}},
        "call_category": {"appointments": 1272},
    }) == [
        {"metric": "overview.active_locations", "curr": 15, "prev": 15},
        {"metric": "call_category.appointments", "curr": 1272, "prev": None},
    ], "_org_rows output changed shape"
    assert _timeseries_rows({
        "data": [{"label": "incoming_calls", "data": [99, 85]}],
        "x_axis": {"values": [1790812800000, 1790899200000, 1790985600000]},
    }) == [
        {"date": "2026-10-01", "incoming_calls": 99},
        {"date": "2026-10-02", "incoming_calls": 85},
    ], "_timeseries_rows output changed shape"

    # Every row of a day's pull carries that day, in both date columns.
    stamped = _stamp([{"center_id": "c1"}, {"center_id": "c2"}], "2026-10-14")
    assert all((r["start_date"], r["end_date"]) == ("2026-10-14", "2026-10-14") for r in stamped), stamped
    try:
        main()
    except zc.ZConnectError as exc:
        sys.exit(f"error: {exc}")
