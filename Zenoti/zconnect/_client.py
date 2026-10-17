"""Shared Zenoti ZConnect API client.

ZConnect routes live on the same host and take the same `apikey` header as the
rest of the Zenoti REST API, so the credentials already in .env work here too.

The one trap: some failures come back as HTTP 200 with a populated `error`
object (e.g. CT028 "Data Not Found" from financial/usage_data). The status code
alone is not a success signal -- always go through `check_error`.
"""
import csv
import datetime
import os
import re
import sys
import time

import requests
from dotenv import load_dotenv

# Anchor the dotenv search to this file's folder, so the scripts find their
# credentials whatever directory they are run from. `.env` is the name the docs
# use; `env` was the name this folder shipped with, so both are accepted. Real
# environment variables win either way -- load_dotenv does not override.
#
# One level up is searched too, and that is the one that matters here: this
# folder has no .env, the repo keeps a single Zenoti/.env, and login_headers.py
# WRITES HC_TOKEN into the .env it finds. Reader and writer have to agree on the
# file or the token that script mints is never read back. Local file first, so a
# zconnect/.env would still win over the shared one.
_HERE = os.path.dirname(os.path.abspath(__file__))
for _dotenv_name in (".env", "env",
                     os.path.join("..", ".env"), os.path.join("..", "env")):
    load_dotenv(os.path.join(_HERE, _dotenv_name))

API_KEY = os.getenv("ZENOTI_API_KEY")

BASE_URL = os.getenv("ZENOTI_BASE_URL", "https://api.zenoti.com").rstrip("/")

# Endpoints below are written with their own /v1 prefix, so drop a trailing
# /v1 if the configured base URL already carries one.
if BASE_URL.endswith("/v1"):
    BASE_URL = BASE_URL[: -len("/v1")]

# The account hits HTTP 429 ("Account quota exceeded") under sustained load, so
# keep a pause between successive calls.
REQUEST_DELAY = float(os.getenv("ZENOTI_REQUEST_DELAY", "0.4"))

# Repeated or fast calls also get a bare 403 Forbidden -- the same URL and params
# answer 200 moments later, so it is throttling, not a permission problem.
# Retry both, backing off exponentially.
THROTTLE_STATUSES = {403, 429}

MAX_RETRIES = int(os.getenv("ZENOTI_MAX_RETRIES", "5"))

# First backoff in seconds; doubles each attempt (5, 10, 20, 40...).
RETRY_BACKOFF_BASE = float(os.getenv("ZENOTI_RETRY_BACKOFF", "5"))

if not API_KEY:
    raise SystemExit("ZENOTI_API_KEY is missing. Add it to your .env file.")

session = requests.Session()
session.headers.update(
    {
        "Authorization": f"apikey {API_KEY}",
        "Accept": "application/json",
    }
)


class ZConnectError(RuntimeError):
    """Raised when the API returns an `error` body, whatever the HTTP status."""


# Center IDs are UUIDs; anything else a caller passes is treated as a code.
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)


# CT028 "Data Not Found" is an empty result, not a failure: a range with no
# activity answers this instead of an empty list. Every other CT0xx (CT004
# invalid input, CT026 missing center, CT060/CT061 missing dates) is a real error.
EMPTY_RESULT_CODES = {"CT028"}


def check_error(payload, status_code):
    """Return payload, or raise ZConnectError if it carries an `error` object."""
    if isinstance(payload, dict):
        error = payload.get("error")
        if error:
            code = error.get("code", "?")
            if code in EMPTY_RESULT_CODES:
                return payload
            message = error.get("message", "unknown error")
            raise ZConnectError(f"{code}: {message} (HTTP {status_code})")
    return payload


def request(method, path, **kwargs):
    """Call {BASE_URL}/v1{path}. Path includes the /v1 prefix, e.g. "/centers".

    Retries two kinds of transient failure before giving up, with the same
    exponential backoff: a throttled response (403/429), and a connection that
    hangs or drops. The latter matters most on the multi-center sweeps -- under
    sustained load the API stops answering rather than returning 429, and an
    unretried ReadTimeout would abort the whole run partway through.
    """
    url = f"{BASE_URL}/v1{path}"
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = session.request(method, url, timeout=30, **kwargs)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            if attempt >= MAX_RETRIES:
                raise ZConnectError(
                    f"{type(exc).__name__} on {path} after {MAX_RETRIES + 1} attempts -- "
                    f"the API stopped responding; rerun, or raise ZENOTI_MAX_RETRIES"
                )
            wait = RETRY_BACKOFF_BASE * (2**attempt)
            print(
                f"{type(exc).__name__} on {path} -- retry {attempt + 1}/{MAX_RETRIES} in {wait:.0f}s",
                file=sys.stderr,
            )
            time.sleep(wait)
            continue

        if response.status_code in THROTTLE_STATUSES and attempt < MAX_RETRIES:
            wait = RETRY_BACKOFF_BASE * (2**attempt)
            print(
                f"throttled ({response.status_code}) on {path} -- retry "
                f"{attempt + 1}/{MAX_RETRIES} in {wait:.0f}s",
                file=sys.stderr,
            )
            time.sleep(wait)
            continue
        break

    try:
        payload = response.json()
    except ValueError:
        response.raise_for_status()
        raise ZConnectError(f"non-JSON response from {url}: {response.text[:200]}")
    response.raise_for_status()
    return check_error(payload, response.status_code)


def get(path, **params):
    """GET, dropping params left as None so the API sees defaults."""
    return request("GET", path, params={k: v for k, v in params.items() if v is not None})


def post(path, body):
    return request("POST", path, json=body)


def list_centers():
    """Every center this key can see.

    The response is keyed `centers`, but some scopes answer `requested_centers`
    instead, so accept either.
    """
    payload = request("GET", "/centers") or {}
    return payload.get("centers") or payload.get("requested_centers") or []


def describe_centers(centers):
    """One-line "CODE (Name)" list, for error messages and --list-centers."""
    return ", ".join(f"{c.get('code')} ({c.get('name')})" for c in centers) or "none"


def centers_to_fetch(center_id=None):
    """[(id, name)] -- every center this key can see, or just the one asked for.

    Running a script bare means every center; only an explicit --center-id
    narrows. ZENOTI_CENTER_ID is deliberately NOT consulted here: with it set in
    .env, a bare run would quietly fetch one center and look complete. Listing
    once and resolving against that same list keeps the single-center case to
    one call.
    """
    all_centers = list_centers()
    if center_id:
        resolved = resolve_center_id(center_id)
        name = next((center.get("name") for center in all_centers if center["id"] == resolved), None)
        return [(resolved, name)]
    return [(center["id"], center.get("name")) for center in all_centers]


def resolve_center_id(value=None):
    """Resolve a center UUID from a UUID, a center code like "BEL", or nothing.

    A UUID passes through untouched. Anything else is matched against the code
    (case-insensitively) of the centers this key can see. With no value, falls
    back to ZENOTI_CENTER_ID, then to the first visible center.
    """
    if value and _UUID_RE.match(value):
        return value

    value = value or os.getenv("ZENOTI_CENTER_ID")
    if value and _UUID_RE.match(value):
        return value

    centers = list_centers()
    if value:
        for center in centers:
            if (center.get("code") or "").lower() == value.lower():
                return center["id"]
        raise ZConnectError(f"no center matches {value!r} -- available: {describe_centers(centers)}")

    if not centers:
        raise ZConnectError("no centers visible to this API key; pass --center-id")
    return centers[0]["id"]


# Widest {start_date, end_date} span each GET route accepts; anything wider is
# rejected with HTTP 400 "CT004 Invalid input". Verified live: events broke at
# 11 days, usage_data at 33, feedback metrics at 186. Values below are the last
# span known to work. Days are counted inclusively -- that is how these routes
# count them, and how date_chunks() reads them.
MAX_SPAN_DAYS = {"events": 10, "usage_data": 32, "feedback_metrics": 185}


def date_chunks(start, end, max_span):
    """Yield (start, end) ISO date pairs covering [start, end], each at most
    `max_span` days long *counting both ends*. These routes reject a wider
    window outright, and they count inclusively -- call_logs answers a
    2026-09-01..2026-10-02 window with "date range cannot exceed 31 days
    (requested 32)", so a start plus 31 days is already one day too many.
    """
    first = datetime.date.fromisoformat(start)
    last = datetime.date.fromisoformat(end)
    while first <= last:
        chunk_end = min(first + datetime.timedelta(days=max_span - 1), last)
        yield first.isoformat(), chunk_end.isoformat()
        first = chunk_end + datetime.timedelta(days=1)


def default_date_range(days=7):
    """(start, end) ISO dates for an unspecified --start-date/--end-date pair.

    ZENOTI_START_DATE / ZENOTI_END_DATE win when set, matching the other scripts
    in this repo; otherwise fall back to the last `days` days ending today.
    Command-line arguments still take precedence over both.
    """
    today = datetime.date.today()
    return (
        os.getenv("ZENOTI_START_DATE") or (today - datetime.timedelta(days=days)).isoformat(),
        os.getenv("ZENOTI_END_DATE") or today.isoformat(),
    )


def write_csv(rows, columns, path):
    """Write a list of dicts to CSV. Returns row count; writes nothing if empty."""
    if not rows:
        return 0
    try:
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    except PermissionError:
        # Almost always the spreadsheet is still open, which Windows locks.
        raise ZConnectError(f"cannot write {path} -- it is open in another program (Excel?). Close it and rerun.")
    return len(rows)


def columns_of(rows):
    """Union of keys across rows, in first-seen order."""
    return list(dict.fromkeys(key for row in rows for key in row))


# ============================================================
# HYPERCONNECT
# ============================================================
# HyperConnect (the product formerly called ZConnect) serves its dashboard
# reports from a *different* host with its own auth: a session bearer token
# lifted from the dashboard, not the backend-app apikey. Everything under
# /features/v1 is ungated for the apikey, so this is the only way in.
#
# Get the token from the dashboard: F12 > Network > Fetch/XHR > any hcreports
# call > Request Headers > copy the value after "Bearer ". It expires, so it
# needs refreshing; put it in .env as HC_TOKEN.

HC_BASE_URL = os.getenv("HC_BASE_URL", "https://hyperconnectapi.zenoti.com")

HC_TOKEN = os.getenv("HC_TOKEN")

# Org id is stable per account; override in .env if you ever operate another org.
HC_ORG_ID = os.getenv("HC_ORG_ID", "837142f5-eaab-4f9e-8505-f90ecc5e5a69")

# call_logs rejects a window wider than 31 days with HTTP 400 ("date range
# cannot exceed 31 days"): 31 works, 32 fails. Chunk at 31, not 30 -- the extra
# day halves nothing but it is the true boundary, and documenting 30 cost a run.
# Both ends count, so this is the width date_chunks() must not exceed.
HC_MAX_SPAN_DAYS = 31

_hc_session = None


def hc_session():
    """Session for the HyperConnect host. Fails only when actually used, so
    importing this module does not require HC_TOKEN."""
    global _hc_session
    if _hc_session is None:
        if not HC_TOKEN:
            raise ZConnectError(
                "HC_TOKEN is missing. Open the HyperConnect dashboard, press F12 > Network > "
                "Fetch/XHR, pick any hcreports call, copy the 'Bearer ...' value from the "
                "authorization request header, and add HC_TOKEN=... to .env"
            )
        _hc_session = requests.Session()
        _hc_session.headers.update(
            {
                "Authorization": f"Bearer {HC_TOKEN}",
                "Content-Type": "application/json",
                "Accept": "*/*",
            }
        )
    return _hc_session


def _hc_body_error(payload):
    """Describe a failure some HyperConnect handlers hide inside HTTP 200.

    A few of them answer 200 with `{"statusCode": 500, ...}` instead of a real
    error status, so the status code alone says the call worked. Returns a
    readable string for those, else None.
    """
    if isinstance(payload, dict):
        status = payload.get("statusCode")
        if isinstance(status, int) and status >= 400:
            return f"{status}: {payload.get('message') or 'no message'}"
    return None


def hc_post(path, body):
    """POST to the HyperConnect host. `path` is everything after the host, e.g.
    "/features/v1/hcreports/call_logs" or "/dashboard/fetch_all_agent_data".

    HyperConnect does not use the `error` object shape the rest of Zenoti does,
    and answers an unmatched path with an AWS signature error rather than a 404,
    so each failure mode gets translated into something readable here.

    A hung connection is retried the same way the Zenoti routes retry it -- the
    agent sweep makes one call per center, so one timeout would otherwise take
    down the whole run.
    """
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = hc_session().post(f"{HC_BASE_URL}{path}", json=body, timeout=90)
            break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            if attempt >= MAX_RETRIES:
                raise ZConnectError(
                    f"{type(exc).__name__} on {path} after {MAX_RETRIES + 1} attempts -- "
                    f"the HyperConnect host stopped responding; rerun"
                )
            wait = RETRY_BACKOFF_BASE * (2**attempt)
            print(
                f"{type(exc).__name__} on {path} -- retry {attempt + 1}/{MAX_RETRIES} in {wait:.0f}s",
                file=sys.stderr,
            )
            time.sleep(wait)

    try:
        payload = response.json()
    except ValueError:
        raise ZConnectError(f"non-JSON response from {path}: {response.text[:200]}")

    if response.status_code == 200:
        # A 200 body can still carry the failure (see _hc_body_error).
        body_error = _hc_body_error(payload)
        if body_error:
            raise ZConnectError(f"{body_error} from {path} (HTTP 200)")
        return payload

    message = payload.get("message") if isinstance(payload, dict) else None
    if response.status_code == 401:
        raise ZConnectError(f"HC_TOKEN rejected (401) -- dashboard token expired, paste a fresh one")
    if response.status_code == 403 and message and "Invalid key=value pair" in message:
        raise ZConnectError(f"no such HyperConnect route: {path}")
    raise ZConnectError(f"{response.status_code} from {path}: {message or response.text[:200]}")


def hc_yyyymmdd(date_string):
    """Convert "YYYY-MM-DD" to the YYYYMMDD integer the /dashboard agent routes want.

    Those routes parse dates with int(), so an ISO string dies with
    "invalid literal for int() with base 10", and epoch millis parse happily but
    match no rows -- a silent empty result rather than an error.
    """
    return int(date_string.replace("-", ""))


def hc_iso_date(value):
    """Convert the YYYYMMDD integer those routes hand back into "YYYY-MM-DD".

    Inverse of hc_yyyymmdd. A None/"" date (a row the API did not date) passes
    through as None rather than becoming "None-No-N".
    """
    if value in (None, ""):
        return None
    text = str(value)
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


if __name__ == "__main__":
    # `python _client.py --centers` lists IDs to feed the other scripts.
    if "--centers" in sys.argv:
        for center in list_centers():
            print(f"{center.get('id')}  {(center.get('code') or ''):<8} {center.get('name')}")
        raise SystemExit(0)

    # Self-check: the error body financial/usage_data really returns on a miss.
    miss = {"usage_data": None, "error": {"code": "CT028", "message": "Data Not Found"}}
    assert check_error(miss, 200) is miss, "CT028 is an empty result, not an error"

    # A real failure must still raise.
    try:
        check_error({"metrics": None, "error": {"code": "CT004", "message": "Invalid input."}}, 400)
    except ZConnectError as exc:
        assert "CT004" in str(exc), exc
    else:
        raise AssertionError("check_error accepted a real CT004 error")

    ok = {"metrics": [{"date": "2026-10-13"}]}
    assert check_error(ok, 200) is ok
    assert columns_of([{"a": 1}, {"b": 2, "a": 3}]) == ["a", "b"]

    # A span wider than the route allows must be split, not sent as-is. Counted
    # inclusively: a 10-day chunk is 10 dates, not 11.
    assert list(date_chunks("2026-10-01", "2026-10-25", 10)) == [
        ("2026-10-01", "2026-10-10"),
        ("2026-10-11", "2026-10-20"),
        ("2026-10-21", "2026-10-25"),
    ]
    assert list(date_chunks("2026-10-01", "2026-10-01", 10)) == [("2026-10-01", "2026-10-01")]
    # The boundary that actually bit: a 46-day backfill must never emit a
    # 32-date chunk, which call_logs refuses with HTTP 400.
    for chunk_start, chunk_end in date_chunks("2026-09-01", "2026-10-16", HC_MAX_SPAN_DAYS):
        width = (datetime.date.fromisoformat(chunk_end) - datetime.date.fromisoformat(chunk_start)).days + 1
        assert width <= HC_MAX_SPAN_DAYS, (chunk_start, chunk_end, width)

    # A UUID passes straight through; a code is not mistaken for one.
    assert _UUID_RE.match("05b67e63-aca8-4015-838e-7f9bd223a0fe")
    assert not _UUID_RE.match("BEL")

    # The two date forms must round-trip.
    assert hc_yyyymmdd("2026-10-03") == 20261003
    assert hc_iso_date(20261003) == "2026-10-03"
    assert hc_iso_date(None) is None and hc_iso_date("") is None
    # A HyperConnect handler that fails inside an HTTP 200 body must not pass
    # for success.
    assert _hc_body_error({"statusCode": 500, "message": "boom"}) == "500: boom"
    assert _hc_body_error({"statusCode": 500}) == "500: no message"
    assert _hc_body_error({"agents": []}) is None
    assert _hc_body_error([1, 2]) is None

    print("client self-check ok")
