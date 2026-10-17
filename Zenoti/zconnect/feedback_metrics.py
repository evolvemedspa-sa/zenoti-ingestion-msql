"""Feedback review metrics, one center or every center.

GET /v1/zconnect/centers/{center_id}/feedbacks/metrics

Fields returned per day: date, total_reviews, total_replies, ratings[]. The
nested ratings array is flattened to rating_1_count .. rating_5_count so the
CSV stays one row per date.

The API rejects the call unless both dates are present (CT060 / CT061) and caps
the window at 185 days, so longer ranges are chunked.

The response carries no center field -- it is implied by the URL -- so each row
is tagged with center_id and center_name ourselves. Every center is fetched by
default; pass --center-id to narrow to one.
"""
import argparse
import sys
import time

import _client as zc


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch ZConnect feedback metrics.")
    parser.add_argument("--center-id", default=None,
                        help="narrow to one center (UUID or code, e.g. BEL); omit to fetch every center")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD; defaults to ZENOTI_START_DATE, else 7 days ago")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD; defaults to ZENOTI_END_DATE, else today")
    parser.add_argument("--csv", default=None, help="output path")
    return parser.parse_args()


def flatten(metric, center_id, center_name):
    """One metric row, nested ratings spread into columns and tagged with its center."""
    row = {"center_id": center_id, "center_name": center_name}
    row.update({key: value for key, value in metric.items() if key != "ratings"})
    for entry in metric.get("ratings") or []:
        row[f"rating_{entry.get('rating')}_count"] = entry.get("count")
    return row


def fetch_center(center_id, center_name, start_date, end_date):
    rows = []
    for chunk_start, chunk_end in zc.date_chunks(start_date, end_date, zc.MAX_SPAN_DAYS["feedback_metrics"]):
        payload = zc.get(
            f"/zconnect/centers/{center_id}/feedbacks/metrics",
            start_date=chunk_start,
            end_date=chunk_end,
        )
        rows.extend(flatten(metric, center_id, center_name) for metric in payload.get("metrics") or [])
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
        center_rows = fetch_center(center_id, center_name, start_date, end_date)
        rows.extend(center_rows)
        reviews = sum(row.get("total_reviews") or 0 for row in center_rows)
        print(f"  {center_name or center_id}: {len(center_rows)} days, {reviews} reviews")

    stem = "zconnect_feedbacks_all" if len(centers) > 1 else "zconnect_feedbacks"
    path = args.csv or f"{stem}_{start_date}_{end_date}.csv"
    if zc.write_csv(rows, zc.columns_of(rows), path):
        reviews = sum(row.get("total_reviews") or 0 for row in rows)
        print(f"{len(rows)} rows, {reviews} reviews across {len(centers)} center(s) -> {path}")
    else:
        print(f"no feedback between {start_date} and {end_date}")


if __name__ == "__main__":
    # flatten() is the only non-obvious logic here -- one check that it holds.
    assert flatten({"date": "2026-10-13", "total_reviews": 2, "total_replies": 0,
                    "ratings": [{"rating": "5", "count": 2}]}, "cid", "A Center") == {
        "center_id": "cid", "center_name": "A Center", "date": "2026-10-13",
        "total_reviews": 2, "total_replies": 0, "rating_5_count": 2,
    }, "flatten() output changed shape"
    try:
        main()
    except zc.ZConnectError as exc:
        sys.exit(f"error: {exc}")
