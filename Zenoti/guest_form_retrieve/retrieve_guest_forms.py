"""Export guest form answers for guests with appointments in an inclusive date range.

Appointment API windows have exclusive end dates and a maximum of seven days.
Appointments are fetched once per center/window, without pagination parameters.
Guest-level answers are not tied to a specific appointment or submission date.
"""

import argparse
import json
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

if __package__:
    from ._shared import load_shared_environment, load_zconnect_module
else:
    from _shared import load_shared_environment, load_zconnect_module

load_shared_environment()
API_KEY = os.getenv("ZENOTI_API_KEY")
BASE_URL = os.getenv("ZENOTI_BASE_URL", "https://api.zenoti.com").rstrip("/")
if BASE_URL.endswith("/v1"):
    BASE_URL = BASE_URL[:-3]

DEFAULT_END_DATE = datetime.now().date()
START_DATE = os.getenv("ZENOTI_START_DATE") or (DEFAULT_END_DATE - timedelta(days=7)).isoformat()
END_DATE = os.getenv("ZENOTI_END_DATE") or DEFAULT_END_DATE.isoformat()
FORM_VERSION_NO = 0
PAGE_SIZE = 100
CONNECT_TIMEOUT = float(os.getenv("ZENOTI_CONNECT_TIMEOUT", "10"))
READ_TIMEOUT = float(os.getenv("ZENOTI_READ_TIMEOUT", "30"))
if CONNECT_TIMEOUT <= 0 or READ_TIMEOUT <= 0:
    raise RuntimeError("ZENOTI_CONNECT_TIMEOUT and ZENOTI_READ_TIMEOUT must be positive.")
REQUEST_DELAY = float(os.getenv("ZENOTI_REQUEST_DELAY", "0.4"))
MAX_RETRIES = int(os.getenv("ZENOTI_MAX_RETRIES", "5"))
RETRY_BACKOFF_BASE = float(os.getenv("ZENOTI_RETRY_BACKOFF", "5"))
RETRY_STATUS = (429, 500, 502, 503, 504)
APPOINTMENT_COLUMNS = [
    "center_id", "center_code", "center_name",
    "guest_first_name", "guest_last_name", "guest_email",
]
FORM_COLUMNS = ["is_form_submitted", "last_submitted_date", "is_form_expired"]

if not API_KEY:
    raise RuntimeError("ZENOTI_API_KEY is missing. Add it to Zenoti/.env.")

zc = load_zconnect_module("_client")
session = requests.Session()
session.headers.update({"Authorization": f"apikey {API_KEY}", "Accept": "application/json"})


def log(message):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}", flush=True)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Retrieve guest forms for guests with appointments in an inclusive date range."
    )
    parser.add_argument("--start-date", default=START_DATE, help="YYYY-MM-DD; defaults to ZENOTI_START_DATE, else 7 days ago")
    parser.add_argument("--end-date", default=END_DATE, help="YYYY-MM-DD, inclusive; defaults to ZENOTI_END_DATE, else today")
    return parser.parse_args(argv)


def parse_date(value):
    return datetime.strptime(value, "%Y-%m-%d").date()


def generate_date_chunks(start_date, end_date, days=7):
    """Convert inclusive dates into appointment windows with exclusive ends."""
    current, end = parse_date(start_date), parse_date(end_date)
    if current > end:
        raise ValueError("START_DATE must be on or before END_DATE.")
    if not 1 <= days <= 7:
        raise ValueError("Appointment date chunks must contain 1 to 7 days.")
    end += timedelta(days=1)
    while current < end:
        following = min(current + timedelta(days=days), end)
        yield current.isoformat(), following.isoformat()
        current = following


def retry_wait(response, attempt):
    """Use Retry-After when valid, otherwise exponential backoff."""
    if response is not None:
        try:
            return max(1.0, float(response.headers.get("Retry-After", "")))
        except ValueError:
            pass
    return RETRY_BACKOFF_BASE * 2 ** (attempt - 1)


def api_get(endpoint, params=None):
    """GET with retries for network errors, throttling and server errors."""
    url = f"{BASE_URL}{endpoint}"
    label = endpoint
    if isinstance(params, dict) and "page" in params:
        label += f" page {params['page']}"
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        response = None
        log(
            f"  GET {label}, attempt {attempt}/{MAX_RETRIES} "
            f"(connect/read timeouts {CONNECT_TIMEOUT:g}/{READ_TIMEOUT:g}s)"
        )
        started = time.monotonic()
        try:
            response = session.get(url, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        except requests.RequestException as exc:
            last_error = str(exc)
            log(f"  {type(exc).__name__} for {label} after {time.monotonic() - started:.1f}s: {exc}")
        else:
            log(f"  HTTP {response.status_code} for {label} after {time.monotonic() - started:.1f}s")
            if response.ok:
                try:
                    payload = response.json()
                except ValueError:
                    raise RuntimeError(f"Zenoti returned non-JSON response:\n{response.text[:1000]}")
                return zc.check_error(payload, response.status_code)
            if response.status_code not in RETRY_STATUS:
                raise RuntimeError(
                    f"HTTP {response.status_code}\nURL: {response.url}\nResponse: {response.text[:1000]}"
                )
            last_error = f"HTTP {response.status_code}: {response.text[:500]}"
        if attempt < MAX_RETRIES:
            wait = retry_wait(response, attempt)
            log(f"  {last_error} | attempt {attempt}/{MAX_RETRIES}; retry in {wait:.0f}s")
            time.sleep(wait)
    raise RuntimeError(last_error or "Unknown API error")


def paged_get(endpoint, params, extract):
    """Yield records while retaining the endpoint-specific response parser."""
    page = 1
    while True:
        payload = api_get(endpoint, {**params, "page": page, "size": PAGE_SIZE})
        batch = extract(payload)
        if not batch:
            return
        yield from batch
        if len(batch) < PAGE_SIZE:
            return
        page += 1
        time.sleep(REQUEST_DELAY)


def extract_centers(payload):
    return (payload.get("centers") or payload.get("requested_centers") or []) if isinstance(payload, dict) else []


def get_all_centers():
    log("FETCHING CENTERS")
    centers = {}
    for center in paged_get("/v1/centers", {}, extract_centers):
        if center.get("id"):
            centers[center["id"]] = {
                "center_id": center["id"],
                "center_code": center.get("code"),
                "center_name": center.get("name") or center.get("display_name"),
            }
    log(f"Centers found: {len(centers)}")
    return list(centers.values())


def extract_appointments(response):
    """Accept the appointment API's list and supported object wrappers."""
    if isinstance(response, list):
        return response
    if not isinstance(response, dict):
        return []
    items = response.get("appointments") or response.get("data") or response.get("items") or []
    if isinstance(items, dict):
        items = items.get("items") or items.get("appointments") or []
    return items if isinstance(items, list) else []


def get_appointments(center, start_date, end_date):
    """Fetch the complete appointment list for one exclusive-end date window."""
    params = {"center_id": center["center_id"], "start_date": start_date, "end_date": end_date}
    appointments = extract_appointments(api_get("/v1/appointments", params))
    log(f"  Appointment response: {len(appointments)} record(s)")
    rows = []
    for appointment in appointments:
        if not isinstance(appointment, dict):
            continue
        guest = appointment.get("guest")
        guest = guest if isinstance(guest, dict) else {}
        rows.append({
            **center,
            "guest_id": guest.get("id"),
            "guest_first_name": guest.get("first_name"),
            "guest_last_name": guest.get("last_name"),
            "guest_email": guest.get("email"),
            "appointment_id": appointment.get("appointment_id"),
            "appointment_segment_id": appointment.get("appointment_segment_id"),
        })
    return rows


def is_signature(value):
    return isinstance(value, str) and value.startswith("data:image")


def flatten_value(value):
    """Render answers as scalars, excluding image signatures."""
    if isinstance(value, dict):
        return ", ".join(key for key, flag in value.items() if flag is True and not is_signature(key))
    if isinstance(value, list):
        return ", ".join(str(item) for item in value if not is_signature(item))
    if isinstance(value, bool):
        return "true" if value else "false"
    return "" if value is None else str(value)


def parse_form_answers(raw):
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            return {}
    if not isinstance(raw, dict):
        return {}
    return {f"form_{key}": flatten_value(value) for key, value in raw.items() if not is_signature(value)}


def empty_form():
    return {"is_form_submitted": False, "last_submitted_date": None, "is_form_expired": None, "answers": {}}


def get_guest_forms(guest_id, version_no=FORM_VERSION_NO):
    if not guest_id:
        return empty_form()
    response = api_get(f"/v1/Guests/{guest_id}/Forms", {"version_no": version_no})
    if not isinstance(response, dict):
        return empty_form()
    return {
        "is_form_submitted": response.get("is_form_submitted", False),
        "last_submitted_date": response.get("last_submitted_date_in_utc"),
        "is_form_expired": response.get("is_expired"),
        "answers": parse_form_answers(response.get("data")),
    }


def fetch_forms_for_guests(guest_ids, errors):
    """Fetch each guest once and retain errors alongside empty fallback forms."""
    log(f"FETCHING GUEST FORMS | unique guests: {len(guest_ids)}")
    forms = {}
    for index, guest_id in enumerate(guest_ids, start=1):
        log(f"[{index}/{len(guest_ids)}] Guest: {guest_id}")
        try:
            forms[guest_id] = get_guest_forms(guest_id)
        except Exception as exc:
            log(f"  ERROR: {exc}")
            errors.append({"guest_id": guest_id, "error": str(exc)})
            forms[guest_id] = empty_form()
        time.sleep(REQUEST_DELAY)
    return forms


def build_dataframe(appointment_rows, forms):
    """Merge answers and deduplicate appointments only when their IDs exist."""
    merged = []
    for row in appointment_rows:
        form = forms.get(row.get("guest_id")) or empty_form()
        merged.append({
            **row,
            "is_form_submitted": form.get("is_form_submitted", False),
            "last_submitted_date": form.get("last_submitted_date"),
            "is_form_expired": form.get("is_form_expired"),
            **(form.get("answers") or {}),
        })
    df = pd.DataFrame(merged)
    if df.empty:
        return df
    if "appointment_id" in df.columns:
        subset = [key for key in ("appointment_id", "appointment_segment_id") if key in df.columns]
        identified = df["appointment_id"].notna() & df["appointment_id"].ne("")
        duplicates = identified & df.duplicated(subset=subset, keep="first")
        log(f"Removed {int(duplicates.sum())} duplicate row(s).")
        df = df.loc[~duplicates]
    columns = [key for key in APPOINTMENT_COLUMNS + FORM_COLUMNS if key in df.columns]
    return df[columns + sorted(key for key in df.columns if key.startswith("form_"))]


def save_csv(df, path):
    """Fall back to a timestamped filename when a spreadsheet locks the CSV."""
    try:
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return path
    except PermissionError:
        stem, extension = os.path.splitext(path)
        fallback = f"{stem}_{datetime.now():%Y%m%d_%H%M%S}{extension}"
        log(f"  {path} is locked. Writing {fallback}")
        df.to_csv(fallback, index=False, encoding="utf-8-sig")
        return fallback


def main(argv=None):
    args = parse_args(argv)
    chunks = list(generate_date_chunks(args.start_date, args.end_date))
    output_path = f"zenoti_guest_forms_{args.start_date}_{args.end_date}.csv"
    error_path = f"zenoti_guest_forms_errors_{args.start_date}_{args.end_date}.csv"
    started, errors = time.time(), []
    log(f"ZENOTI GUEST FORMS | {args.start_date} -> {args.end_date} (inclusive appointment dates)")
    log(f"version_no: {FORM_VERSION_NO}")
    log(f"Timeouts: connect {CONNECT_TIMEOUT:g}s / read {READ_TIMEOUT:g}s; {MAX_RETRIES} attempts per request")
    centers = get_all_centers()
    if not centers:
        raise RuntimeError("No centers were returned.")
    log(f"FETCHING APPOINTMENTS | date chunks: {len(chunks)}")
    appointment_rows = []
    for center in centers:
        name = center.get("center_name") or center["center_id"]
        for first, last in chunks:
            log(f"{name} | {first} -> {last}")
            try:
                rows = get_appointments(center, first, last)
                log(f"  {len(rows)} appointment(s)")
                appointment_rows.extend(rows)
            except Exception as exc:
                log(f"  ERROR: {exc}")
                errors.append({
                    "center_id": center["center_id"], "center_name": center.get("center_name"),
                    "start_date": first, "end_date": last, "error": str(exc),
                })
            time.sleep(REQUEST_DELAY)
    log(f"Total appointment rows: {len(appointment_rows)}")
    if not appointment_rows:
        if errors:
            log(f"Error log: {save_csv(pd.DataFrame(errors), error_path)}")
        log("No appointments retrieved. Nothing to write.")
        return

    guest_ids = sorted({row["guest_id"] for row in appointment_rows if row.get("guest_id")})
    forms = fetch_forms_for_guests(guest_ids, errors)
    df = build_dataframe(appointment_rows, forms)
    output_file = save_csv(df, output_path)
    error_file = save_csv(pd.DataFrame(errors), error_path) if errors else None
    submitted = sum(bool(value) for value in df["is_form_submitted"] if pd.notna(value))
    answer_columns = sum(key.startswith("form_") for key in df.columns)
    log(f"COMPLETE | centers: {len(centers)}; appointment rows: {len(df)}; unique guests: {len(guest_ids)}")
    log(f"Rows with a form: {submitted}; form answer columns: {answer_columns}; errors: {len(errors)}")
    log(f"Output: {output_file} | elapsed: {time.time() - started:.2f}s")
    if error_file:
        log(f"Error log: {error_file}")


if __name__ == "__main__":
    main()



