"""Export individual review ratings and comments from Zenoti to CSV.

Uses the verified GET /v1/zconnect/centers/{center_id}/events?filter=feedback
route and its external review metadata. Native Zenoti feedback events carry an
appointment group reference instead of review JSON; their ratings and comments
are retrieved through GET /v1/feedbacks?appointment_group_id=... using the same
API key. Both GOOGLE and ZENOTI rows are included in the export.

Credentials: ZENOTI_API_KEY and optional ZENOTI_BASE_URL in Zenoti/.env,
through the shared zconnect/_client.py client. No browser bearer token needed.
This script reads Zenoti and writes a local CSV; it does not load SQL Server.
The CSV is a different format from feedback_report.py's invoice-based export.

Defaults: all centers visible to the API key, 2000-01-01 through today.
ZENOTI_REVIEW_START_DATE / ZENOTI_REVIEW_END_DATE can override those dates.
The general ZENOTI_START_DATE / ZENOTI_END_DATE are intentionally not used:
an old pipeline window must not silently limit an all-review export.
Only reviews retained in the event feed can be retrieved; this route does not
guarantee the complete historical set shown on Google or other review sites.
Dates filter event activity, which can differ from the review's created_on.

Every page is fetched in inclusive 10-day windows. Submitted native feedback
(category 5, action 6, source 0) is fetched through reference_id_1. Invitations
and other non-review events are skipped, while ratings with blank comments stay.
Repeated reviews with the same center/source/date/reviewer keep their latest
event version. When those identity fields are incomplete, event ID and metadata
index are used instead. Native feedback uses appointment group/invoice identity.
Full review metadata is preserved in metadata_json, including native tags,
appointment dates and separate therapist/ambience/cleanliness ratings. Native
rating is service_rating; an unanswered service rating (-1) is left blank.
CSV publication happens only after every center/window finishes successfully.
Empty/non-JSON GET responses and temporary server failures are retried by the
shared client. Persistent failures report the center/window and event reference
so an unavailable historical feedback record can be investigated.
Event-feed failures also identify the requested page and received event count.
Non-JSON errors show the HTTP status, content type and body on a separate line.
HTTP 204 from GET /feedbacks means no details for that appointment group. It is
logged and treated as an empty lookup; other reviews continue to be exported.

From the repository root:
    python Zenoti/zconnect/get_events.py
    python Zenoti/zconnect/get_events.py --start-date 2026-01-01
    python Zenoti/zconnect/get_events.py --center-id cac21ac7-7925-418f-ae6e-1f3e8d45ca2b
    python Zenoti/zconnect/get_events.py --start-date 2026-11-01 --end-date 2026-11-16 --csv reviews.csv
    python Zenoti/zconnect/get_events.py --dry-run
    python Zenoti/zconnect/get_events.py --self-check

Default output: Zenoti/zconnect/output/reviews_<scope>_<dates>.csv.
This review export replaces the former timeline CSV format. The
timeline --filter and --expand-metadata options are no longer needed: reviews
always use filter=feedback and are normalized into the columns below. The
timeline SQL loader (hc_ingestion_manual/ingest_events_old.py) expects a different CSV schema.

The full default history needs many requests. Use your earliest relevant date
with --start-date to shorten it. --max-pages is optional; reaching it aborts
rather than publishing a truncated export. Dependencies: Zenoti/requirements.txt.
"""

from __future__ import annotations

import argparse
from collections import Counter
import csv
import hashlib
import json
import os
import sys
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4


HERE = Path(__file__).resolve().parent
ZENOTI_DIR = HERE.parent
DEFAULT_START = "2000-01-01"
WINDOW_DAYS = 10
COLUMNS = (
    "center_id", "center_name", "review_key", "event_id", "metadata_index",
    "guest_id", "guest_name", "reviewer_name", "rating", "comments",
    "review_date", "source", "review_reply", "review_reply_status",
    "review_reply_metadata", "event_date", "metadata_json",
)


def date_windows(start, end):
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first > last:
        raise ValueError("--start-date must be on or before --end-date.")
    while first <= last:
        window_end = min(first + timedelta(days=WINDOW_DAYS - 1), last)
        yield first.isoformat(), window_end.isoformat()
        first = window_end + timedelta(days=1)


def json_text(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def review_rows(event, center_id, center_name):
    """Extract actual review objects; feedback messages also use category 5."""
    if str(event.get("category_type")) != "5":
        return []
    raw = event.get("metadata")
    try:
        metadata = json.loads(raw) if isinstance(raw, str) else raw
    except ValueError:
        # Feedback-request messages return plain text rather than review JSON.
        if '"review_text"' in raw or '"rating"' in raw:
            raise ValueError("Malformed review metadata; export aborted.")
        return []
    objects = metadata if isinstance(metadata, list) else [metadata]
    rows = []
    for index, review in enumerate(objects):
        if not isinstance(review, dict):
            continue
        if "rating" not in review and "review_text" not in review:
            continue
        raw_rating = review.get("rating")
        try:
            rating = int(str(raw_rating))
        except (TypeError, ValueError) as exc:
            raise ValueError("Review rating must be an integer from 1 to 5.") from exc
        if not 1 <= rating <= 5:
            raise ValueError("Review rating must be an integer from 1 to 5.")
        if not event.get("event_id"):
            raise ValueError("A review event has no event_id; export aborted.")
        comments = review.get("review_text")
        if comments is not None and not isinstance(comments, str):
            raise ValueError("Review text must be a string or null.")
        source, created, reviewer = (
            review.get("source"), review.get("created_on"), review.get("reviewer_name")
        )
        # Reply/update events can repeat a review. Never use rating/comment text
        # as its identity: both may change. Fall back to event identity when the
        # metadata cannot identify the original review reliably.
        identity = (
            ["review", center_id, source, created, reviewer]
            if all(isinstance(value, str) and value.strip()
                   for value in (source, created, reviewer))
            else ["event", center_id, event["event_id"], index]
        )
        rows.append({
            "center_id": center_id,
            "center_name": event.get("center_name") or center_name,
            "review_key": hashlib.sha256(json_text(identity).encode("utf-8")).hexdigest(),
            "event_id": event["event_id"],
            "metadata_index": index,
            "guest_id": review.get("guest_id") or event.get("guest_id"),
            "guest_name": event.get("guest_name"),
            "reviewer_name": reviewer,
            "rating": rating,
            "comments": comments,
            "review_date": created,
            "source": source,
            "review_reply": review.get("review_reply"),
            "review_reply_status": review.get("review_reply_status"),
            "review_reply_metadata": review.get("review_reply_metadata"),
            "event_date": event.get("send_date"),
            "metadata_json": json_text(review),
        })
    return rows


def native_feedback_group(event):
    """The website loads submitted feedback through this event reference."""
    if (str(event.get("category_type")) == "5"
            and str(event.get("action_type")) == "6"
            and str(event.get("source")) == "0"):
        group = event.get("reference_id_1")
        if isinstance(group, str) and group.strip():
            return group.strip()
    return None


def native_feedback_rows(payload, event, center_id, center_name):
    """Normalize GET /feedbacks details without guessing an overall V1 rating."""
    if not isinstance(payload, dict):
        raise ValueError("Unexpected native feedback response; expected an object.")
    error = payload.get("error")
    if error:
        if isinstance(error, dict) and error.get("code") == "CT028":
            return []
        raise ValueError("Zenoti returned a native feedback API error.")
    feedback = payload.get("feedback")
    if not isinstance(feedback, list) or any(not isinstance(f, dict) for f in feedback):
        raise ValueError("Unexpected feedback; expected a list of native feedback objects.")
    if not event.get("event_id"):
        raise ValueError("A native feedback event has no event_id; export aborted.")
    rows = []
    for index, item in enumerate(feedback):
        raw_rating = item.get("service_rating")
        if raw_rating is None or str(raw_rating) == "-1":
            rating = None  # -1 marks an unanswered dimension, not a negative review.
        else:
            try:
                rating = int(str(raw_rating))
            except (TypeError, ValueError) as exc:
                raise ValueError("Native service rating must be 1 to 5, -1 or null.") from exc
            if not 1 <= rating <= 5:
                raise ValueError("Native service rating must be 1 to 5, -1 or null.")
        comments = item.get("notes")
        if comments is not None and not isinstance(comments, str):
            raise ValueError("Native feedback notes must be a string or null.")
        group = item.get("appointment_group_id") or native_feedback_group(event)
        identity = ["zenoti_feedback", center_id, group, item.get("invoice_id")]
        rows.append({
            "center_id": center_id,
            "center_name": item.get("center_name") or event.get("center_name") or center_name,
            "review_key": hashlib.sha256(json_text(identity).encode("utf-8")).hexdigest(),
            "event_id": event["event_id"],
            "metadata_index": index,
            "guest_id": item.get("guest_id") or event.get("guest_id"),
            "guest_name": item.get("guest_name") or event.get("guest_name"),
            "reviewer_name": item.get("guest_name") or event.get("guest_name"),
            "rating": rating,
            "comments": comments,
            "review_date": item.get("created_date"),
            "source": "ZENOTI",
            "review_reply": item.get("response"),
            "review_reply_status": item.get("is_responded"),
            "review_reply_metadata": None,
            "event_date": event.get("send_date"),
            "metadata_json": json_text(item),
        })
    return rows


def fetch_window(client, center_id, start, end, size, max_pages=0):
    """Yield every event, checking totals and detecting repeated API pages."""
    page, received, signatures = 1, 0, set()
    endpoint = f"/zconnect/centers/{center_id}/events"
    while True:
        try:
            payload = client.get(
                endpoint,
                start_date=start, end_date=end, filter="feedback", page=page, size=size,
            )
        except Exception as exc:
            raise RuntimeError(
                f"GET {endpoint} failed on page {page} "
                f"(size={size}, {received} events received): {exc}"
            ) from exc
        if not isinstance(payload, dict):
            raise ValueError("Unexpected feedback response; expected a JSON object.")
        error = payload.get("error")
        if error:
            if not isinstance(error, dict) or error.get("code") != "CT028":
                raise ValueError("Zenoti returned a feedback API error.")
            if received:
                raise ValueError("Zenoti stopped returning data before pagination finished.")
            return
        batch = payload.get("events_data")
        if not isinstance(batch, list) or any(not isinstance(e, dict) for e in batch):
            raise ValueError("Unexpected events_data; expected a list of event objects.")
        info = payload.get("page_info") or {}
        if not isinstance(info, dict):
            raise ValueError("Unexpected page_info; expected an object.")
        total = info.get("total")
        if total is not None:
            if isinstance(total, bool) or not str(total).isdigit():
                raise ValueError("Unexpected pagination total.")
            total = int(total)
        if not batch:
            if total is not None and received < total:
                raise ValueError("Empty page before the reported total; export is incomplete.")
            return
        signature = tuple(json_text(event) for event in batch)
        if signature in signatures:
            raise ValueError("Zenoti repeated a page; export is incomplete.")
        signatures.add(signature)
        received += len(batch)
        yield from batch
        # A short page is not a completion signal: the API may impose a size
        # below the requested one. Count all events, not just extracted reviews.
        if total is not None and received >= total:
            return
        if max_pages and page >= max_pages:
            raise ValueError(
                f"Reached --max-pages ({max_pages}) for {center_id}, {start}..{end}; "
                "no CSV published. Raise the cap or omit it."
            )
        page += 1
        time.sleep(client.REQUEST_DELAY)


def event_time(value):
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Unexpected review event date.") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def collect_reviews(client, centers, windows, size, max_pages):
    reviews = {}
    for center_id, center_name in centers:
        native_details = {}
        count_before, event_count = len(reviews), 0
        for index, (start, end) in enumerate(windows, 1):
            print(f"  {center_name or center_id}: {start}..{end} "
                  f"({index}/{len(windows)})", flush=True)
            # Clear context after each event so a page failure cannot be
            # incorrectly attributed to the preceding native feedback lookup.
            event, group = None, None
            try:
                for event in fetch_window(client, center_id, start, end, size, max_pages):
                    event_count += 1
                    group = native_feedback_group(event)
                    if group:
                        if group not in native_details:
                            time.sleep(client.REQUEST_DELAY)
                            native_details[group] = client.get("/feedbacks", appointment_group_id=group)
                        event_rows = native_feedback_rows(native_details[group], event, center_id, center_name)
                    else:
                        event_rows = review_rows(event, center_id, center_name)
                    for row in event_rows:
                        previous = reviews.get(row["review_key"])
                        if previous is None or event_time(row["event_date"]) > event_time(previous["event_date"]):
                            reviews[row["review_key"]] = row
                    event, group = None, None
            except Exception as exc:
                context = f"{center_name or center_id} (center_id={center_id}), {start}..{end}"
                if event is not None:
                    context += f", event_id={event.get('event_id')}"
                if group:
                    context += f", appointment_group_id={group}"
                raise RuntimeError(f"Review export failed for {context}: {exc}; no CSV published.") from exc
            time.sleep(client.REQUEST_DELAY)
        print(f"  {center_name or center_id}: {len(reviews) - count_before:,} reviews "
              f"from {event_count:,} feedback events", flush=True)
    return sorted(reviews.values(), key=lambda row: (
        str(row["center_name"] or row["center_id"]), str(row["review_date"] or ""),
        row["review_key"],
    ))


def write_csv(rows, destination):
    """Replace the destination only once the complete CSV has been written."""
    destination = Path(destination).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + "." + uuid4().hex + ".tmp")
    try:
        with temporary.open("x", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=COLUMNS)
            writer.writeheader()
            for row in rows:
                writer.writerow({key: json_text(value) if isinstance(value, (dict, list))
                                 else value for key, value in row.items()})
        temporary.replace(destination)
    finally:
        if temporary.exists():
            temporary.unlink()
    return destination


def self_check():
    """Offline checks for mixed feedback, pagination, updates and CSV fidelity."""
    from contextlib import redirect_stdout
    from io import StringIO

    review = {"source": "GOOGLE", "created_on": "2026-11-12T20:35:26",
              "rating": 5, "reviewer_name": "Example", "review_text": 'Great, "care"!\n\u2605'}
    original = {"event_id": "e1", "category_type": 5,
                "send_date": "2026-11-13T00:00:00Z", "metadata": json_text([review])}
    invitation = {"event_id": "invitation", "category_type": 5, "metadata": "Please rate your visit"}
    blank = dict(original, event_id="e2", metadata=json_text([
        dict(review, reviewer_name="Rating only", review_text="")]))
    updated = dict(original, event_id="reply", send_date="2026-11-14T00:00:00Z",
                   metadata=json_text([dict(review, review_reply="Thank you")]))

    class FakeClient:
        REQUEST_DELAY = 0

        def __init__(self, pages):
            self.pages, self.calls = pages, []

        def get(self, path, **params):
            self.calls.append(params)
            return self.pages[params["page"] - 1]

    client = FakeClient([
        {"events_data": [original, invitation], "page_info": {"total": 4}},
        {"events_data": [blank, updated], "page_info": {"total": 4}},
    ])
    with redirect_stdout(StringIO()):
        rows = collect_reviews(client, [("center", "Name")], [("2026-11-07", "2026-11-16")], 200, 0)
    assert len(rows) == 2 and len(client.calls) == 2
    assert all(call["filter"] == "feedback" for call in client.calls)
    assert any(row["comments"] == "" and row["rating"] == 5 for row in rows)
    assert any(row["event_id"] == "reply" and row["review_reply"] == "Thank you" for row in rows)
    assert list(date_windows("2026-10-01", "2026-10-21")) == [
        ("2026-10-01", "2026-10-10"), ("2026-10-11", "2026-10-20"),
        ("2026-10-21", "2026-10-21"),
    ]
    assert not review_rows(invitation, "center", "Name")
    # Native events contain plain text, so the website fetches their details.
    native_event = dict(invitation, event_id="native", action_type=6, source=0,
                        reference_id_1="group", send_date="2026-11-13T00:00:00Z")
    native_details = {"feedback": [{
        "appointment_group_id": "group", "invoice_id": "invoice",
        "service_rating": 4, "therapist_rating": -1, "notes": "Website feedback",
        "created_date": "2026-11-12T12:00:00", "guest_name": "Native guest",
        "tags": ["Cleanliness"], "is_responded": False, "response": None,
    }], "error": None}

    class NativeClient(FakeClient):
        def __init__(self):
            super().__init__([{"events_data": [original, native_event, dict(
                native_event, event_id="native-update", send_date="2026-11-14T00:00:00Z")],
                "page_info": {"total": 3}}])
            self.detail_calls = 0

        def get(self, path, **params):
            if path == "/feedbacks":
                assert params == {"appointment_group_id": "group"}
                self.detail_calls += 1
                return native_details
            return super().get(path, **params)

    native_client = NativeClient()
    with redirect_stdout(StringIO()):
        mixed = collect_reviews(native_client, [("center", "Name")],
                                [("2026-11-07", "2026-11-16")], 200, 0)
    assert Counter(row["source"] for row in mixed) == {"GOOGLE": 1, "ZENOTI": 1}
    native = next(row for row in mixed if row["source"] == "ZENOTI")
    assert native["rating"] == 4 and native["comments"] == "Website feedback"
    assert native["event_id"] == "native-update" and native_client.detail_calls == 1
    assert json.loads(native["metadata_json"])["tags"] == ["Cleanliness"]
    assert not native_feedback_group(invitation)
    unanswered = {"feedback": [dict(native_details["feedback"][0], service_rating=-1)]}
    assert native_feedback_rows(unanswered, native_event, "center", "Name")[0]["rating"] is None
    for bad_payload in ({"error": {"code": "CT004"}}, {"feedback": None}):
        try:
            native_feedback_rows(bad_payload, native_event, "center", "Name")
        except ValueError:
            pass
        else:
            raise AssertionError("Invalid native feedback must abort the export.")
    # No total: keep reading despite a page shorter than requested.
    no_total = FakeClient([{"events_data": [original]}, {"events_data": [blank]}, {"events_data": []}])
    assert len(list(fetch_window(no_total, "center", "2026-11-07", "2026-11-16", 200))) == 2
    for pages, cap in [
        ([{"events_data": [original], "page_info": {"total": 2}}, {"events_data": [], "page_info": {"total": 2}}], 0),
        ([{"events_data": [original]}, {"events_data": [original]}], 0),
        ([{"events_data": [original], "page_info": {"total": 2}}], 1),
        ([{"metrics": []}], 0),
        ([{"error": {"code": "CT004"}}], 0),
    ]:
        try:
            list(fetch_window(FakeClient(pages), "center", "2026-11-07", "2026-11-16", 200, cap))
        except ValueError:
            pass
        else:
            raise AssertionError("Incomplete/error response must fail.")
    assert list(fetch_window(FakeClient([{"events_data": None, "error": {"code": "CT028"}}]),
                             "center", "2026-11-07", "2026-11-16", 200)) == []
    with tempfile.TemporaryDirectory(dir=HERE) as directory:
        destination = Path(directory) / "reviews.csv"
        write_csv(rows, destination)
        with destination.open(encoding="utf-8-sig", newline="") as handle:
            loaded = list(csv.DictReader(handle))
        assert len(loaded) == 2 and any(row["comments"] == review["review_text"] for row in loaded)
        previous = destination.read_bytes()
        try:
            write_csv([{"unexpected_column": "bad"}], destination)
        except ValueError:
            pass
        else:
            raise AssertionError("Invalid CSV row must fail.")
        assert destination.read_bytes() == previous
        assert not list(Path(directory).glob("*.tmp"))
    print("events self-check passed; no API or database connection made.")
    return 0


def main(argv=None, *, output_dir=None):
    """Run the exporter, optionally overriding the default CSV output folder."""
    output_dir = Path(output_dir) if output_dir is not None else HERE / "output"
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--center-id", help="one center UUID/code; omit for all accessible centers")
    parser.add_argument("--start-date", help="YYYY-MM-DD; default 2000-01-01 or ZENOTI_REVIEW_START_DATE")
    parser.add_argument("--end-date", help="YYYY-MM-DD; default today or ZENOTI_REVIEW_END_DATE")
    parser.add_argument("--size", type=int, default=200, help="events per page (default 200)")
    parser.add_argument("--max-pages", type=int, default=0, help="page cap per window; 0 = unlimited")
    parser.add_argument("--csv", help=f"destination CSV; default {output_dir}/reviews_<scope>_<dates>.csv")
    parser.add_argument("--dry-run", action="store_true", help="show date window and scope; no API or CSV writes")
    parser.add_argument("--self-check", action="store_true", help="run offline checks only")
    args = parser.parse_args(argv)
    try:
        if args.self_check:
            return self_check()
        from dotenv import load_dotenv

        load_dotenv(ZENOTI_DIR / ".env")
        start = args.start_date or os.getenv("ZENOTI_REVIEW_START_DATE") or DEFAULT_START
        end = args.end_date or os.getenv("ZENOTI_REVIEW_END_DATE") or date.today().isoformat()
        windows = list(date_windows(start, end))
        if args.size < 1 or args.max_pages < 0:
            raise ValueError("--size must be positive; --max-pages must be zero or positive.")
        print(f"Review event dates: {start} .. {end}; {len(windows):,} windows per center", flush=True)
        print(f"Scope: {args.center_id or 'all centers visible to the API key'}", flush=True)
        if args.dry_run:
            print("Dry-run passed; no API requests, database connection or CSV writes.")
            return 0
        if __package__:
            from . import _client as client
        else:
            import _client as client

        centers = client.centers_to_fetch(args.center_id)
        if not centers:
            raise ValueError("No centers visible to the API key; no CSV published.")
        print(f"{len(centers)} center(s); at least {len(centers) * len(windows):,} API requests. "
              "Only reviews available in the event feed can be exported.", flush=True)
        rows = collect_reviews(client, centers, windows, args.size, args.max_pages)
        print(f"Reviews by source: {dict(Counter(row['source'] or 'unknown' for row in rows))}")
        scope = centers[0][0] if args.center_id else "all"
        destination = args.csv or output_dir / f"reviews_{scope}_{start}_{end}.csv"
        path = write_csv(rows, destination)
        print(f"Exported {len(rows):,} reviews with ratings and comments -> {path}")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
