"""Export the Service Forms review report using the shared browser login helper.

The regional report uses inclusive date bounds and keeps every form record.
CLI examples and installation instructions are in this folder's README.md.
"""
import argparse
import datetime
import json
import os
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

import requests

if __package__:
    from ._shared import ZENOTI_ROOT, load_shared_environment, load_zconnect_module
else:
    from _shared import ZENOTI_ROOT, load_shared_environment, load_zconnect_module

load_shared_environment()
zc = load_zconnect_module("_client")

PATH = "/v1/reports/forms/review/flat_file"
DEFAULT_HOST = "https://apiamrs12.cu.zenoti.com"
DEFAULT_PAGE_SIZE = 300
DEFAULT_CHUNK_DAYS = 7
MAX_PAGES = 200
MAX_RETRIES = 5
RETRY_BACKOFF_SECONDS = 5

FIELD_CANDIDATES = {
    "Form Name": ["formname", "form", "formtitle", "name"],
    "Guest": ["guestname", "guestfullname", "guest", "customername"],
    "Review Status": ["reviewstatus", "reviewstate", "status", "isreviewed", "reviewed"],
    "Submit Status": ["submitstatus", "issubmitted", "formstatus", "submissionstatus"],
    "Appointment Date": ["appointmentdate", "appointmentstarttime", "appointmentdatetime",
                         "servicedate", "starttime"],
    "Invoice No": ["invoiceno", "invoicenumber", "invoicenum", "invoice"],
    "Service Center": ["centername", "servicecenter", "center"],
    "Guest Code": ["guestcode", "guestid"],
    "Service Name": ["servicename", "service"],
    "Provider Name": ["servicedby", "providername", "provider", "therapistname", "employeename", "therapist"],
    "Expiry Date": ["expirydate", "expirationdate", "expireson", "expiredate"],
    "Last Modified On": ["lastmodifiedon", "lastmodifieddate", "lastmodifiedat", "modifiedon",
                         "lastfilleddate"],
    "Enable for Review": ["enableforreview", "isenabledforreview", "isreviewenabled",
                          "enabledforreview", "reviewenabled", "isreviewneeded"],
    "Reviewed By": ["reviewedby", "reviewer", "reviewername", "reviewedbyname"],
    "Reviewed On": ["reviewedon", "revieweddate", "reviewedat", "reviewdate"],
    "Job": ["job", "jobname", "jobrole", "providerjob", "providerjobname"],
    "Appointment Status": ["appointmentstatus", "appointmentstatusname", "apptstatus"],
}
OUTPUT_COLUMNS = list(FIELD_CANDIDATES)

FALLBACK_CENTER_KEY = "_fallback_center_name"

CONTAINER_KEYS = ("form_reviews", "forms", "data", "items", "rows", "report", "records", "result")


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch the Zenoti Service Forms (Forms Review) report.")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD; defaults to ZENOTI_START_DATE, else 7 days ago")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD, inclusive; defaults to ZENOTI_END_DATE, else today")
    parser.add_argument("--center-id", default=None, help="one center (UUID or code); default is every center")
    parser.add_argument("--auth", default="login", choices=["auto", "web", "login"],
                        help="login (default): capture a fresh token through zconnect/login_headers.py; "
                             "web: reuse HC_TOKEN; auto: reuse it and log in if needed")
    parser.add_argument("--login", action="store_true",
                        help="log in through a browser first and use that fresh token (same as --auth login)")
    parser.add_argument("--host", default=os.getenv("ZENOTI_FORMS_HOST", DEFAULT_HOST),
                        help="API host the report is served from; the web UI uses "
                             f"{DEFAULT_HOST}. Try https://api.zenoti.com if the token is refused")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="rows per page; the UI sends 300")
    parser.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS,
                        help="split the date range into windows of this many days")
    parser.add_argument("--end-exclusive", action="store_true",
                        help="send end_date as the day AFTER each window, for a route that treats it as exclusive")
    parser.add_argument("--no-expand", action="store_true", help="omit expand=review_stats")
    parser.add_argument("--body-file", default=None,
                        help="JSON request body with center/date placeholders; "
                             "replaces the default captured browser payload")
    parser.add_argument("--csv", default=None, help="output path")
    return parser.parse_args()


def date_windows(start_date, end_date, days):
    """[(first, last)] inclusive ISO windows of at most `days` days covering the range."""
    first = datetime.date.fromisoformat(start_date)
    stop = datetime.date.fromisoformat(end_date)
    if first > stop:
        raise zc.ZConnectError(f"start date {start_date} is after end date {end_date}")
    step = max(1, days)
    windows = []
    while first <= stop:
        last = min(first + datetime.timedelta(days=step - 1), stop)
        windows.append((first.isoformat(), last.isoformat()))
        first = last + datetime.timedelta(days=1)
    return windows


def _fill(template, values):
    """Replace {center_id} {start_date} {end_date} in every string of a JSON template."""
    if isinstance(template, str):
        for key, value in values.items():
            template = template.replace("{" + key + "}", str(value))
        return template
    if isinstance(template, list):
        return [_fill(item, values) for item in template]
    if isinstance(template, dict):
        return {key: _fill(item, values) for key, item in template.items()}
    return template


def build_body(center_id, first, last, args, template: dict[str, Any] | None = None) -> dict[str, Any]:
    """Captured report body for all centers or one center and a date window."""
    send_end = last
    if args.end_exclusive:
        send_end = (datetime.date.fromisoformat(last) + datetime.timedelta(days=1)).isoformat()
    start_datetime = first + " 00:00:00"
    end_datetime = send_end + (" 00:00:00" if args.end_exclusive else " 23:59:59")
    values = {"center_id": center_id or "", "start_date": first, "end_date": send_end,
              "start_datetime": start_datetime, "end_datetime": end_datetime}
    if template is not None:
        return {key: _fill(value, values) for key, value in template.items()}
    return {
        "centers": {"ids": [center_id] if center_id else [], "is_all": not bool(center_id)},
        "start_date": start_datetime,
        "end_date": end_datetime,
        "appointment_statuses": [-1],
        "assignees": {"ids": [], "is_all": True},
        "categories": {"ids": [], "is_all": True},
        "enabled_for_review": ["-1"],
        "form_field_id": None,
        "form_field_type": None,
        "form_field_value": None,
        "forms": {"ids": [], "is_all": True},
        "is_new_report": True,
        "jobs": {"ids": [], "is_all": True},
        "operator_type": 1,
        "services": {"ids": [], "is_all": True},
        "statuses": [-1],
        "submit_statuses": [-1],
        "therapists": {"ids": [], "is_all": True},
    }


def report_scopes(args, template=None):
    """Only resolve API-key centers for an explicit center or center template."""
    uses_center_placeholder = template is not None and "{center_id}" in json.dumps(template)
    if args.center_id or uses_center_placeholder:
        return [{"id": cid, "name": name} for cid, name in zc.centers_to_fetch(args.center_id)]
    return [{"id": None, "name": "Body-file scope" if template is not None else "All centers"}]


def _mask(value):
    return f"****{value[-4:]}" if value and len(value) > 4 else "****"


AUTH_ORDER = ("web", "login")


def _web_token():
    """Return a stored admin token without its optional Bearer prefix."""
    raw = (os.getenv("ZENOTI_WEB_TOKEN") or os.getenv("HC_TOKEN") or os.getenv("ZENOTI_BEARER") or "").strip()
    if raw.lower().startswith("bearer "):
        raw = raw[7:].strip()
    return raw or None


def _login_helper():
    """Report missing dependencies separately from missing credentials."""
    try:
        return load_zconnect_module("login_headers")
    except ModuleNotFoundError as exc:
        requirements = ZENOTI_ROOT / "guest_form_retrieve" / "requirements.txt"
        interpreter = f'& "{sys.executable}"' if os.name == "nt" else f'"{sys.executable}"'
        raise zc.ZConnectError(
            f"Browser login dependency {exc.name!r} is not installed.\n"
            f"  Python interpreter: {sys.executable}\n"
            f'  Install dependencies: {interpreter} -m pip install -r "{requirements}"'
        ) from exc
    except ImportError as exc:
        raise zc.ZConnectError(f"Cannot load shared browser login helper: {exc}") from exc


def _can_login():
    """Whether the shared login helper has a username and password."""
    login_headers = _login_helper()
    user = any(os.getenv(n) for n in login_headers.USER_ENV_NAMES)
    password = any(os.getenv(n) for n in login_headers.PASSWORD_ENV_NAMES)
    return user and password


def _available(mode):
    return bool(_web_token()) if mode == "web" else _can_login()


def _browser_login():
    """Capture the shared browser token and keep it in this process's environment."""
    login_headers = _login_helper()
    print("  login   opening a browser to log in to Zenoti (this takes a moment)")
    try:
        token = login_headers.capture_token()
    except SystemExit as exc:
        raise zc.ZConnectError(f"browser login failed: {exc}")
    if not isinstance(token, str) or not token.strip():
        raise zc.ZConnectError("Browser login did not return a bearer token.")
    token = token.strip()
    os.environ["HC_TOKEN"] = token
    print("  login   fresh token captured for this run")
    return token


def _set_auth(state, mode):
    """Point `state` at one credential and say which, masked."""
    if mode == "web":
        token = _web_token()
        header, how = f"Bearer {token}", "stored token (HC_TOKEN)"
    elif mode == "login":
        token = _browser_login()
        header, how = f"Bearer {token}", "fresh browser login"
        state["tried"].add("web")      # the stored token is now superseded
    else:
        raise zc.ZConnectError(f"unsupported authentication mode: {mode}")
    state.update(mode=mode, header=header)
    state["tried"].add(mode)
    print(f"  auth    {mode} {_mask(token)} ({how})")


def _next_mode(state):
    """The next credential in AUTH_ORDER not yet tried, or None."""
    for mode in AUTH_ORDER:
        if mode not in state["tried"] and _available(mode):
            return mode
    return None


def make_auth(choice):
    """The starting auth state for --auth."""
    state = {"mode": None, "header": None, "tried": set(),
             "allow_fallback": choice == "auto"}
    if choice == "auto":
        first = _next_mode(state)
        if not first:
            raise zc.ZConnectError(
                "no token or login credentials: supply HC_TOKEN or configure the credentials "
                "used by zconnect/login_headers.py")
    else:
        if not _available(choice):
            if choice == "login":
                raise zc.ZConnectError(
                    f"--auth login requires a username and password. Set MINER_USER and MINER_PASSWORD "
                    f"in {ZENOTI_ROOT / '.env'} or the process environment.\n"
                    f"  Python interpreter: {sys.executable}"
                )
            raise zc.ZConnectError(f"--auth {choice} requires HC_TOKEN in {ZENOTI_ROOT / '.env'} or the process environment")
        first = choice
    _set_auth(state, first)
    return state


def _request_headers(state):
    headers = {"Authorization": state["header"], "Accept": "application/json"}
    if state["mode"] in ("web", "login"):
        headers.update(application_name="web", application_version="1")
    return headers


def post_page(host, params, body, state):
    """POST with transient retries, limited 403 retries and optional auth fallback."""
    url = host.rstrip("/") + PATH
    transient = 0       # 429 / 5xx / network retries used
    forbidden = 0       # 403s retried as possible throttling
    while True:
        try:
            response = requests.post(
                url, params=params, json=body, timeout=120,
                headers=_request_headers(state),
            )
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            transient += 1
            if transient >= MAX_RETRIES:
                raise zc.ZConnectError(f"{type(exc).__name__} on {PATH} after {MAX_RETRIES} attempts")
            wait = RETRY_BACKOFF_SECONDS * 2 ** (transient - 1)
            print(f"  {type(exc).__name__} -- waiting {wait:.0f}s (attempt {transient}/{MAX_RETRIES})")
            time.sleep(wait)
            continue

        code = response.status_code

        if code in (401, 403):
            if code == 403 and forbidden < 2:
                wait = RETRY_BACKOFF_SECONDS * 2 ** forbidden
                forbidden += 1
                print(f"  403 -- may be throttling, waiting {wait:.0f}s (retry {forbidden}/2)")
                time.sleep(wait)
                continue
            following = _next_mode(state) if state["allow_fallback"] else None
            if following:
                print(f"  {code} on {state['mode']} -- trying {following} instead")
                _set_auth(state, following)
                forbidden = 0
                continue
            raise zc.ZConnectError(
                f"{code} from {host}{PATH} using {state['mode']}: {response.text.strip()[:200]}\n"
                "  - the employee behind the token needs the 'Service Forms Report' permission "
                "(Security roles > Reports (V2) > Guest Engagement)\n"
                "  - or this host does not accept the token: try --host https://api.zenoti.com\n"
            )

        if code in (429, 500, 502, 503, 504):
            transient += 1
            if transient >= MAX_RETRIES:
                raise zc.ZConnectError(f"{code} from {PATH} after {MAX_RETRIES} attempts: {response.text.strip()[:200]}")
            try:
                wait = max(1.0, float(response.headers.get("Retry-After", "")))
            except ValueError:
                wait = RETRY_BACKOFF_SECONDS * 2 ** (transient - 1)
            print(f"  {code} -- waiting {wait:.0f}s (attempt {transient}/{MAX_RETRIES})")
            time.sleep(wait)
            continue

        if not response.ok:
            raise zc.ZConnectError(
                f"{code} from {host}{PATH}: {response.text.strip()[:300]}\n"
                f"  request body: {json.dumps(body)[:300]}\n"
                "  check the center scope and request filters against the successful browser payload; "
                "--body-file overrides the captured default"
            )

        try:
            payload = response.json()
        except ValueError:
            raise zc.ZConnectError(f"non-JSON response from {PATH}: {response.text[:200]}")
        return zc.check_error(payload, code)


def extract_records(payload):
    """(rows, container_name, other_top_level_keys) -- rows wherever they live."""
    if isinstance(payload, list):
        return payload, "<root list>", {}
    if not isinstance(payload, dict):
        return [], None, {}

    for key in CONTAINER_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            return value, key, {k: v for k, v in payload.items() if k != key}
        if isinstance(value, dict):
            for inner_key, inner in value.items():
                if isinstance(inner, list) and (not inner or isinstance(inner[0], dict)):
                    return inner, f"{key}.{inner_key}", {k: v for k, v in payload.items() if k != key}

    for key, value in payload.items():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return value, key, {k: v for k, v in payload.items() if k != key}
    return [], None, dict(payload)


def fetch_window(host, center, first, last, args, template, state):
    """Every row for one center and window; (rows, pages, extras)."""
    rows, pages, extras = [], 0, {}
    previous = None
    for page in range(1, MAX_PAGES + 1):
        params = {"page": page, "size": args.page_size}
        if not args.no_expand:
            params["expand"] = "review_stats"
        payload = post_page(host, params, build_body(center["id"], first, last, args, template), state)

        records, _, other = extract_records(payload)
        if page == 1:
            extras = other

        records = [r for r in records if isinstance(r, dict)]
        if not records:
            break

        signature = json.dumps(records, sort_keys=True, default=str)
        if signature == previous:
            print(f"  page {page} repeats page {page - 1}: the route ignores paging; stopping")
            break
        previous = signature

        if center["id"] is not None:
            for record in records:
                record[FALLBACK_CENTER_KEY] = center.get("name")
        rows.extend(records)
        pages += 1
        if len(records) < args.page_size:
            break
    else:
        print(f"  WARN    hit the {MAX_PAGES}-page cap for {center.get('name')} {first}..{last}")
    return rows, pages, extras


def normalise_key(key):
    return re.sub(r"[^a-z0-9]", "", str(key).lower())


def flatten_record(record, prefix=""):
    """Flatten nested objects and normalise response keys for column matching."""
    flat = {}
    for key, value in record.items():
        norm = normalise_key(key)
        if isinstance(value, dict):
            for nkey, nvalue in flatten_record(value, prefix + norm).items():
                flat.setdefault(nkey, nvalue)
            continue
        flat[prefix + norm] = value
        if prefix:
            flat.setdefault(norm, value)
    return flat


def _blank(value):
    return value is None or (isinstance(value, str) and not value.strip())


def _pick(flat, candidates):
    for candidate in candidates:
        if candidate in flat and not _blank(flat[candidate]):
            return flat[candidate], candidate
    return None, None


def render_value(column, value):
    """A CSV-friendly scalar; booleans become the words the report screen uses."""
    if value is None:
        return ""
    if isinstance(value, bool):
        words = {"Review Status": ("Reviewed", "Not Reviewed"),
                 "Submit Status": ("Submitted", "Not Submitted"),
                 "Enable for Review": ("Yes", "No")}.get(column, ("true", "false"))
        return words[0] if value else words[1]
    if isinstance(value, (list, dict)):
        return json.dumps(value, default=str)
    return str(value)


def _guest_name(flat):
    first = flat.get("guestfirstname") or flat.get("firstname")
    last = flat.get("guestlastname") or flat.get("lastname")
    parts = [str(p) for p in (first, last) if not _blank(p)]
    return " ".join(parts) if parts else None


@dataclass
class MappingCoverage:
    counts: Counter[str] = field(default_factory=Counter)
    used_keys: set[str] = field(default_factory=set)
    response_keys: set[str] = field(default_factory=set)


def map_record(record, coverage: MappingCoverage | None = None, sources=None):
    """One response row as the 17 output columns, in order."""
    flat = flatten_record(record)
    row = {}
    used = set()
    for column, candidates in FIELD_CANDIDATES.items():
        value, matched = _pick(flat, candidates)
        if value is None and column == "Guest":
            value = _guest_name(flat)
            matched = "guestfirstname+guestlastname" if value else None
        if value is not None:
            used.add(matched)
            if coverage is not None:
                coverage.counts[column] += 1
            if sources is not None:
                sources.setdefault(column, Counter())[matched] += 1
        row[column] = render_value(column, value)
    if not row["Service Center"]:
        row["Service Center"] = record.get(FALLBACK_CENTER_KEY) or ""
    if coverage is not None:
        coverage.used_keys.update(used)
        coverage.response_keys.update(flat)
    return row


def map_records(records):
    """(rows, coverage, sources) for a whole pull."""
    coverage = MappingCoverage()
    sources: dict[str, Counter[str]] = {}
    rows = [map_record(record, coverage, sources) for record in records]
    return rows, coverage, sources


def report_mapping(total, coverage: MappingCoverage, sources):
    """Print which columns matched which response key, and what was left over."""
    print("  field mapping (column <- response key, filled rows)")
    missing = []
    for column in OUTPUT_COLUMNS:
        hits = coverage.counts[column]
        if hits:
            key = sources[column].most_common(1)[0][0]
            print(f"    ok    {column:<20} <- {key:<28} {hits}/{total}")
        else:
            missing.append(column)
            print(f"    MISS  {column:<20} <- no matching key in the response")
    if missing:
        print(f"  WARN    {len(missing)} column(s) empty: {', '.join(missing)} -- inspect the response "
              "keys below and update FIELD_CANDIDATES")
    unused = sorted(coverage.response_keys - coverage.used_keys - {"fallbackcentername"})
    if unused:
        shown = ", ".join(unused[:50]) + (" ..." if len(unused) > 50 else "")
        print(f"  note    {len(unused)} response key(s) not mapped to any column: {shown}")


def _template(path):
    if not path:
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            template = json.load(handle)
    except (OSError, ValueError) as exc:
        raise zc.ZConnectError(f"cannot read --body-file {path}: {exc}")
    if not isinstance(template, dict):
        raise zc.ZConnectError(f"--body-file {path} must contain a JSON object")
    return template


def main():
    args = parse_args()

    default_start, default_end = zc.default_date_range()
    start_date = args.start_date or default_start
    end_date = args.end_date or default_end

    template = _template(args.body_file)
    centers = report_scopes(args, template)
    if not centers:
        raise zc.ZConnectError("no centers to pull")

    windows = date_windows(start_date, end_date, args.chunk_days)
    print(f"  {len(centers)} report scope(s) x {len(windows)} window(s)  {start_date} .. {end_date}")
    print(f"  host {args.host}  size {args.page_size}  "
          f"body {'from ' + args.body_file if template is not None else 'captured browser format'}")

    state = make_auth("login" if args.login else args.auth)
    records, failures = [], []
    width = max(len(c["name"] or c["id"]) for c in centers)
    started = time.time()
    total_jobs = len(centers) * len(windows)
    done = 0

    for center in centers:
        label = center["name"] or center["id"]
        for first, last in windows:
            done += 1
            t0 = time.time()
            try:
                rows, pages, extras = fetch_window(args.host, center, first, last, args, template, state)
            except zc.ZConnectError as exc:
                if "401" in str(exc) or "403" in str(exc):
                    raise
                failures.append((label, first, last, str(exc).splitlines()[0]))
                print(f"  FAIL    {label:<{width}}  {first}..{last}  {str(exc).splitlines()[0]}")
                continue
            elapsed = time.time() - started
            eta = elapsed / done * (total_jobs - done)
            print(f"  [{done}/{total_jobs}] {label:<{width}}  {first}..{last}  {len(rows):>5} form(s)  "
                  f"{pages} page(s)  {time.time() - t0:.1f}s  eta {eta:.0f}s")
            page_info = extras.get("page_info")
            api_total = page_info.get("total") if isinstance(page_info, dict) else None
            if api_total is not None:
                print(f"  counts  API total {api_total}; fetched {len(rows)}")
                if api_total != len(rows):
                    print(f"  WARN    fetched count differs from the API total for {label} {first}..{last}")
            if extras and done == 1:
                print(f"  note    non-row keys in the response (review_stats lives here?): {sorted(extras)}")
            records.extend(rows)

    if failures:
        print(f"  WARN    {len(failures)} window(s) failed and are missing from the output:")
        for label, first, last, reason in failures[:10]:
            print(f"            {label} {first}..{last}: {reason}")

    path = args.csv or f"zenoti_service_forms_{start_date}_{end_date}.csv"

    if not records:
        print(f"  wrote   nothing: no forms between {start_date} and {end_date}")
        return

    rows, coverage, sources = map_records(records)
    report_mapping(len(records), coverage, sources)
    print(f"  counts  fetched {len(records)}; CSV rows {len(rows)}")

    if zc.write_csv(rows, OUTPUT_COLUMNS, path):
        print(f"  wrote   {len(rows)} form row(s)  {start_date} .. {end_date}  -> {os.path.basename(path)}")
    for column in ("Review Status", "Submit Status", "Appointment Status"):
        counts = Counter(row[column] or "(blank)" for row in rows)
        print(f"  {column:<19} " + ", ".join(f"{value} {n}" for value, n in counts.most_common()))


if __name__ == "__main__":
    try:
        main()
    except zc.ZConnectError as exc:
        sys.exit(f"error: {exc}")
