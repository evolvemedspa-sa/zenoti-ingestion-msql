"""Center timeline events.

GET /v1/zconnect/centers/{center_id}/events

Appointments, feedback, bot interactions and other activity, newest first.
Fields returned (18, all flat):

  guest_id, guest_name, mobilephone, event_id, action_type, source, send_date,
  reference_id_1, reference_id_2, old_value, new_value, metadata,
  profile_image, centerid, center_name, category_type, unread, channel_medium

The route rejects a date window wider than 10 days, so longer ranges are
fetched in chunks.

`metadata` is a JSON string holding an array of objects, and its shape depends
on category_type -- a bare {"ItemType": n}, a call record (guest name/phone,
conference_name, recording flags), a guest/CRM block, or an appointment block.
It is written through verbatim by default; --expand-metadata spreads it into
metadata_<key> columns instead, one row per object (an event can carry several).
"""
import argparse
import json
import sys
import time

import _client as zc

# The 18 fields this route documents, in response order. Held explicitly so the
# CSV always carries all of them: columns_of() only emits keys it has actually
# seen, so a field the API omitted from every row in a given pull would quietly
# lose its column. Extra keys the API adds still come through, appended.
EVENT_FIELDS = (
    "guest_id", "guest_name", "mobilephone", "event_id", "action_type", "source",
    "send_date", "reference_id_1", "reference_id_2", "old_value", "new_value",
    "metadata", "profile_image", "centerid", "center_name", "category_type",
    "unread", "channel_medium",
)


def event_columns(rows):
    """EVENT_FIELDS, plus any key the API returned that is not already listed."""
    extra = [key for key in zc.columns_of(rows) if key not in EVENT_FIELDS]
    return list(EVENT_FIELDS) + extra


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch ZConnect center timeline events.")
    parser.add_argument("--center-id", default=None,
                        help="narrow to one center (UUID or code, e.g. BEL); omit to fetch every center")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD; defaults to ZENOTI_START_DATE, else 7 days ago")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD; defaults to ZENOTI_END_DATE, else today")
    parser.add_argument("--filter", default=None,
                        help="'feedback' narrows to the feedback feed (category_type 5); "
                             "unrecognised values are ignored by the API")
    parser.add_argument("--expand-metadata", action="store_true",
                        help="spread the metadata JSON into metadata_<key> columns")
    parser.add_argument("--size", type=int, default=200, help="records per page")
    parser.add_argument("--max-pages", type=int, default=50, help="page cap per chunk; warns if it truncates")
    parser.add_argument("--csv", default=None, help="output path; defaults to zconnect_events_<start>_<end>.csv")
    return parser.parse_args()


def expand_metadata(rows):
    """One row per metadata object, its keys spread as metadata_<key>.

    The array's shape varies by category_type (see the module docstring), so
    every key is spread rather than a fixed set picked out -- the union is wide
    and sparse, which is the honest shape of this field. An event with no usable
    metadata still yields one row, so nothing is silently dropped.
    """
    out = []
    for row in rows:
        raw = (row.get("metadata") or "").strip()
        objects = []
        if raw:
            try:
                parsed = json.loads(raw)
            except ValueError:
                parsed = None
            if isinstance(parsed, list):
                objects = [obj for obj in parsed if isinstance(obj, dict)]
            elif isinstance(parsed, dict):
                objects = [parsed]

        base = {key: value for key, value in row.items() if key != "metadata"}
        if not objects:
            out.append(base)
            continue
        for index, obj in enumerate(objects):
            expanded = dict(base)
            expanded["metadata_index"] = index
            for key, value in obj.items():
                expanded[f"metadata_{key}"] = value
            out.append(expanded)
    return out


def fetch_chunk(center_id, start, end, args):
    """All events in one date window. Returns (rows, hit_page_cap)."""
    rows, page, total = [], 1, None
    while page <= args.max_pages:
        payload = zc.get(
            f"/zconnect/centers/{center_id}/events",
            page=page,
            size=args.size,
            start_date=start,
            end_date=end,
            filter=args.filter,
        )
        batch = payload.get("events_data") or []
        rows.extend(batch)
        total = (payload.get("page_info") or {}).get("total") or total
        if not batch or (total is not None and len(rows) >= total):
            return rows, False
        page += 1
        time.sleep(zc.REQUEST_DELAY)
    return rows, True


def main():
    args = parse_args()

    default_start, default_end = zc.default_date_range()
    start_date = args.start_date or default_start
    end_date = args.end_date or default_end

    centers = zc.centers_to_fetch(args.center_id)
    if len(centers) > 1:
        print(f"{len(centers)} centers")

    rows, truncated = [], []
    for center_id, center_name in centers:
        center_rows, center_truncated = [], []
        for chunk_start, chunk_end in zc.date_chunks(start_date, end_date, zc.MAX_SPAN_DAYS["events"]):
            chunk_rows, hit_cap = fetch_chunk(center_id, chunk_start, chunk_end, args)
            center_rows.extend(chunk_rows)
            if hit_cap:
                center_truncated.append(f"{chunk_start}..{chunk_end}")
        rows.extend(center_rows)
        truncated.extend(center_truncated)
        print(f"  {center_name or center_id}: {len(center_rows)} events")

    if truncated:
        print(f"WARNING: hit --max-pages ({args.max_pages}) in {len(truncated)} chunk(s) -- results are "
              f"incomplete, raise --max-pages. Truncated: {', '.join(truncated)}")

    if args.expand_metadata:
        before = len(rows)
        rows = expand_metadata(rows)
        print(f"  metadata expanded: {before} events -> {len(rows)} rows")

    # Keep the filter, all-centers and metadata modes in the default name so
    # none of them overwrites a plain pull for the same range.
    parts = ["zconnect_events_all" if len(centers) > 1 else "zconnect_events"]
    if args.filter:
        parts.append(args.filter)
    if args.expand_metadata:
        parts.append("meta")
    path = args.csv or f"{'_'.join(parts)}_{start_date}_{end_date}.csv"
    # The expanded file's metadata_* columns are data-driven by nature, so only
    # the plain pull gets the guaranteed field list.
    columns = None if args.expand_metadata else event_columns(rows)
    if zc.write_csv(rows, columns or zc.columns_of(rows), path):
        print(f"{len(rows)} events across {len(centers)} center(s) ({start_date} to {end_date}) -> {path}")
    else:
        print(f"no events between {start_date} and {end_date}")


if __name__ == "__main__":
    # expand_metadata() is the only non-obvious logic -- pin its shape down.
    fixture = [
        {"event_id": "e1", "metadata": '[{"ItemType": 3}]'},
        {"event_id": "e2", "metadata": '[{"guest_name": "A"}, {"guest_name": "B"}]'},
        {"event_id": "e3", "metadata": ""},
        {"event_id": "e4", "metadata": '{"ItemType": 0}'},
        {"event_id": "e5", "metadata": "not json"},
    ]
    assert expand_metadata(fixture) == [
        {"event_id": "e1", "metadata_index": 0, "metadata_ItemType": 3},
        {"event_id": "e2", "metadata_index": 0, "metadata_guest_name": "A"},
        {"event_id": "e2", "metadata_index": 1, "metadata_guest_name": "B"},
        {"event_id": "e3"},
        {"event_id": "e4", "metadata_index": 0, "metadata_ItemType": 0},
        {"event_id": "e5"},
    ], "expand_metadata() output changed shape"

    # A pull missing some fields entirely must still emit all 18 columns.
    assert event_columns([{"event_id": "e1"}])[:18] == list(EVENT_FIELDS), "event_columns() dropped a field"
    assert event_columns([{"event_id": "e1", "brand_new": 1}])[-1] == "brand_new", "event_columns() dropped an extra key"

    try:
        main()
    except zc.ZConnectError as exc:
        sys.exit(f"error: {exc}")
