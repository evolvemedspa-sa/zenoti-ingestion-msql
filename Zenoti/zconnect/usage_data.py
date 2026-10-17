"""Messaging usage per day for one center, or every center.

GET /v1/zconnect/financial/usage_data?center_id=..&channel=..&start_date=..&end_date=..

Fields returned (10, all flat): date, total_sent_count, twoway_count, mms_count,
notification_count, marketing_count, incoming_count, total_segments,
outbound_segments, inbound_segments.

`center_id` is mandatory -- without it the API answers CT026. `channel` is
optional; only channels this center actually uses return data, any other
answers CT028 "Data Not Found". The window caps at 32 days, chunked here.

The response carries no center field -- it is implied by the query -- so each
row is tagged with center_id and center_name. Every center is fetched by
default; pass --center-id to narrow to one.
"""
import argparse
import sys
import time

import _client as zc


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch ZConnect messaging usage data.")
    parser.add_argument("--center-id", default=None,
                        help="narrow to one center (UUID or code, e.g. BEL); omit to fetch every center")
    parser.add_argument("--channel", default=None, help="e.g. SMS; omit for all")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD; defaults to ZENOTI_START_DATE, else 7 days ago")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD; defaults to ZENOTI_END_DATE, else today")
    parser.add_argument("--csv", default=None, help="output path; defaults to zconnect_usage_<start>_<end>.csv")
    return parser.parse_args()


def fetch_center(center_id, center_name, args, start_date, end_date):
    rows = []
    for chunk_start, chunk_end in zc.date_chunks(start_date, end_date, zc.MAX_SPAN_DAYS["usage_data"]):
        # A chunk with no activity answers CT028, which the client returns as an
        # empty payload rather than raising.
        payload = zc.get(
            "/zconnect/financial/usage_data",
            center_id=center_id,
            channel=args.channel,
            start_date=chunk_start,
            end_date=chunk_end,
        )
        for day in payload.get("usage_data") or []:
            rows.append({"center_id": center_id, "center_name": center_name, **day})
        # One call per center per chunk, so keep the same pause the other
        # ZConnect scripts leave between calls -- this route throttles.
        time.sleep(zc.REQUEST_DELAY)
    return rows


def main():
    args = parse_args()

    default_start, default_end = zc.default_date_range()
    start_date = args.start_date or default_start
    end_date = args.end_date or default_end

    centers = zc.centers_to_fetch(args.center_id)
    if len(centers) > 1:
        print(f"{len(centers)} centers")

    rows = []
    for center_id, center_name in centers:
        center_rows = fetch_center(center_id, center_name, args, start_date, end_date)
        rows.extend(center_rows)
        sent = sum(row.get("total_sent_count") or 0 for row in center_rows)
        print(f"  {center_name or center_id}: {len(center_rows)} days, {sent} messages")

    stem = "zconnect_usage_all" if len(centers) > 1 else "zconnect_usage"
    path = args.csv or f"{stem}_{start_date}_{end_date}.csv"
    if zc.write_csv(rows, zc.columns_of(rows), path):
        sent = sum(row.get("total_sent_count") or 0 for row in rows)
        print(f"{len(rows)} days, {sent} messages across {len(centers)} center(s) -> {path}")
    else:
        print(f"no usage data between {start_date} and {end_date}")


if __name__ == "__main__":
    try:
        main()
    except zc.ZConnectError as exc:
        sys.exit(f"error: {exc}")
