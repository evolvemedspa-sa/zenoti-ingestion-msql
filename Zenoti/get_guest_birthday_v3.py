"""Standalone Zenoti export: center, guest ID, first/last name, and birthday -> CSV.

Requires Python 3.8+, pyodbc, and the SQL Server ODBC driver for database ingestion.
It has its own .env reader and HTTP client.

Credentials come from a .env file (next to this script, in its parent folder,
then the current folder) or from real environment variables, which take priority:

    ZENOTI_API_KEY=your-key            (required)
    ZENOTI_API_BASE=https://api.zenoti.com/v1   (optional)
    ZENOTI_CENTER_ID=uuid1,uuid2       (optional; default = all centers)

Endpoints (https://docs.zenoti.com):
    GET /v1/centers                                   list all centers
    GET /v1/guests?center_id=..&page=..&size=100      list guests of a center
    GET /v1/guests/{guest_id}                         details (name/birthday/created fallback)
Fields: personal_info.first_name, personal_info.last_name, personal_info.date_of_birth,
        created_date

Usage (default: guests CREATED two days ago through yesterday):
    python get_guest_birthday3.py                       # run on Oct 23 -> Oct 21 to Oct 22
    python get_guest_birthday3.py --from 2026-10-01 --to 2026-10-15   # date range, inclusive
    python get_guest_birthday3.py --from 2026-10-01     # from that date through yesterday
    python get_guest_birthday3.py --from 2026-01-01 --to 2026-03-31   # long range: auto-split
    python get_guest_birthday3.py --all --delay 1       # one-time full export of every guest
    python get_guest_birthday3.py --center-id UUID1,UUID2
    python get_guest_birthday3.py -v --log-file run.log # debug + log file
    python get_guest_birthday3.py --self-test

How the date window works: the window is a range of guest CREATION dates (calendar
days as Zenoti reports them for each center). Today is never included, because the
day is not finished yet. Zenoti can only be asked for guests "updated since" a date,
so for each window the script asks for guests updated since its start date and keeps
those whose created_date falls inside it (created_date comes from the guest list, or
from the guest details when the list omits it).

Long ranges: Zenoti does not accept a range longer than about a month, so a range
over --chunk-days (default 30) is split into consecutive chunks of at most that many
days. Chunks never overlap, a guest is written once, and details are fetched at most
once per guest. Progress is logged chunk by chunk.

After the guest list has been processed, any still-blank birthdays are confirmed
from the individual guest details endpoint. The guest list is fetched only once.
The CSV is written after confirmation completes. A local SQLite checkpoint is
saved beside the output; use --resume with the same --output and filters to continue
after an interruption. The checkpoint contains guest data and should be protected
like the CSV; it is removed after the export and SQL refresh both succeed.

If the guest list omits required name data, or a creation date needed for filtering,
the script fetches that guest's details during collection. Blank birthdays are
confirmed after collection; details already fetched are reused. Zenoti's
standard quota is 60 calls/minute for the whole organization, so for the big
one-time export (--all) use --delay 1. Progress streams to the console live with
timestamps; -v adds every API call and --log-file also saves the log (no guest
names or birthdays are logged). The CSV contains personal data; store it
securely. After a successful fetch and CSV export, fetched guest keys are deleted
and reinserted in TABLE_GUEST_DOB (dbo.BRONZE_ZENOTI_DOB by default) in one SQL
transaction. Existing rows for guests outside the fetched window are preserved.
Nothing is written back to Zenoti.
"""
import argparse
import csv
import json
import logging
import os
import sqlite3
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from email.message import Message
from pathlib import Path
from uuid import UUID

HERE = Path(__file__).resolve().parent
COLUMNS = ("center_id", "center_name", "guest_id", "first_name", "last_name", "date_of_birth")
PAGE_SIZE = 100          # documented maximum for /guests
MAX_PAGES = 5000
RETRY_STATUS = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 5
PROGRESS_EVERY = 25      # log a progress line every N detail lookups
DEFAULT_LAG_DAYS = 2     # two complete days ending yesterday; today is excluded
MAX_CHUNK_DAYS = 30      # longest single request window (Zenoti rejects about a month+)

logger = logging.getLogger("zenoti")


def setup_logging(verbose=False, log_file=None):
    """Stream timestamped logs to the console (flushed per line) and optionally a file."""
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.handlers.clear()
    logger.propagate = False
    formatter = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%H:%M:%S")
    console = logging.StreamHandler(sys.stdout)  # StreamHandler flushes after every record
    console.setFormatter(formatter)
    logger.addHandler(console)
    if log_file:
        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s"))
        logger.addHandler(file_handler)


# ---------------------------------------------------------------- .env reader
def load_env(paths):
    """Load KEY=VALUE lines; real environment variables are never overridden."""
    for path in paths:
        try:
            text = Path(path).read_text(encoding="utf-8-sig")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[7:].lstrip()
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            elif " #" in value:
                value = value.split(" #", 1)[0].rstrip()
            if key and key not in os.environ:
                os.environ[key] = value


# ---------------------------------------------------------------- helpers
def uuid_value(value):
    try:
        return str(UUID(value.strip()))
    except (ValueError, AttributeError) as exc:
        raise ValueError(f"Not a valid UUID: {value!r}") from exc


def parse_birthday(raw):
    """Return an ISO calendar date or ''. Keeps the date exactly as Zenoti sent it."""
    if raw is None or raw == "":
        return ""
    if not isinstance(raw, str):
        raise ValueError("Zenoti returned a non-string date_of_birth.")
    try:
        return datetime.fromisoformat(raw.strip().replace("Z", "+00:00")).date().isoformat()
    except ValueError as exc:
        raise ValueError(f"Invalid date_of_birth from Zenoti: {raw!r}") from exc


def parse_created(raw):
    """Return the creation calendar date (as sent, in center time) or None if missing/unreadable."""
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        return datetime.fromisoformat(raw.strip().replace("Z", "+00:00")).date()
    except ValueError:
        return None


def name_fields(info):
    """Return (first_name, last_name) from personal_info; '' for any that is missing."""
    if not isinstance(info, dict):
        return "", ""
    first, last = info.get("first_name"), info.get("last_name")
    return (first.strip() if isinstance(first, str) else "",
            last.strip() if isinstance(last, str) else "")


def needs_detail(info):
    """The list response is enough only if it carries the birthday and a name field."""
    return (not isinstance(info, dict) or "date_of_birth" not in info
            or not ("first_name" in info or "last_name" in info))


# ---------------------------------------------------------------- date window
def parse_cli_date(name, text):
    try:
        return datetime.strptime(text.strip(), "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"{name} must be a real date in YYYY-MM-DD format.") from None


def resolve_window(args, today=None):
    """Turn the CLI options into (start, end, label); start/end are dates, or None for --all.

    Default is the previous two complete days, through yesterday.
    Both ends are inclusive and today is never included.
    """
    today = today or date.today()
    if args.all:
        if args.date_from or args.date_to:
            raise ValueError("--all (full export) can't be combined with --from or --to.")
        return None, None, "full export of every guest (no date filter)"
    if args.date_to and not args.date_from:
        raise ValueError("--to needs --from.")
    yesterday = today - timedelta(days=1)
    if args.date_from:
        start = parse_cli_date("--from", args.date_from)
        end = parse_cli_date("--to", args.date_to) if args.date_to else yesterday
    else:
        start, end = today - timedelta(days=DEFAULT_LAG_DAYS), yesterday
    if end > today:
        raise ValueError("--to can't be in the future.")
    if end < start:
        raise ValueError(f"The end date ({end}) is before the start date ({start}). "
                         "Without --to the range ends yesterday, so --from must be before today.")
    return start, end, f"guests created {start} through {end}"


def split_range(start, end, max_days=MAX_CHUNK_DAYS):
    """Split an inclusive date range into consecutive chunks of at most max_days days."""
    chunks, current = [], start
    while current <= end:
        stop = min(current + timedelta(days=max_days - 1), end)
        chunks.append((current, stop))
        current = stop + timedelta(days=1)
    return chunks


def in_window(created, start, end):
    return start <= created <= end


# ---------------------------------------------------------------- HTTP client
class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None  # never follow redirects (would leak the API key)


def make_getter(base, key, delay, opener=None, sleep=time.sleep):
    """Return get(path, params) -> dict, with retries and strict response checks."""
    opener = opener or urllib.request.build_opener(_NoRedirect)
    authorization = key if key.lower().startswith("apikey ") else f"apikey {key}"

    def get(path, params=None):
        url = base + path + ("?" + urllib.parse.urlencode(params) if params else "")
        request = urllib.request.Request(url, method="GET", headers={
            "Authorization": authorization, "Accept": "application/json"})
        body = b""  # every loop path either sets this and breaks, or raises
        for attempt in range(1, MAX_ATTEMPTS + 1):
            sleep(delay)
            wait = 2.0 ** (attempt - 1)
            started = time.monotonic()
            try:
                with opener.open(request, timeout=60) as response:
                    body = response.read()
                logger.debug("GET %s%s -> OK in %.2fs", path, " " + str(params) if params else "",
                             time.monotonic() - started)
                break
            except urllib.error.HTTPError as exc:
                if exc.code in RETRY_STATUS and attempt < MAX_ATTEMPTS:
                    retry_after = exc.headers.get("Retry-After", "") if exc.headers else ""
                    try:  # Zenoti can send decimals such as "39.44"
                        wait = min(max(float(retry_after), 0.0), 60.0) if retry_after else wait
                    except ValueError:
                        pass
                    logger.warning("HTTP %s on %s; retry %d/%d in %.1fs", exc.code, path, attempt,
                                   MAX_ATTEMPTS - 1, wait)
                    sleep(wait)
                    continue
                raise ValueError(f"Zenoti returned HTTP {exc.code} for {path}. Check API access.") from None
            except (urllib.error.URLError, socket.timeout, ConnectionError) as exc:
                reason = getattr(exc, "reason", exc)  # the real cause (SSL, DNS, refused...)
                if attempt < MAX_ATTEMPTS:
                    logger.warning("%s on %s: %s; retry %d/%d in %.1fs", type(exc).__name__, path, reason,
                                   attempt, MAX_ATTEMPTS - 1, wait)
                    sleep(wait)
                    continue
                raise ValueError(f"Network error calling {path}: {reason}") from None
        try:
            payload = json.loads(body.decode("utf-8-sig"))
        except (ValueError, UnicodeDecodeError):
            raise ValueError("Zenoti returned a non-JSON response.") from None
        if not isinstance(payload, dict):
            raise ValueError("Zenoti returned an unexpected JSON response.")
        if payload.get("error"):
            err = payload["error"]
            code = err.get("code", "unknown") if isinstance(err, dict) else "unknown"
            raise ValueError(f"Zenoti reported API error {code} for {path}.")
        return payload

    return get


# ---------------------------------------------------------------- Zenoti logic
def format_duration(seconds):
    minutes, secs = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m{secs:02d}s" if hours else f"{minutes}m{secs:02d}s"


def list_centers(get):
    centers = get("/centers").get("centers")
    if not isinstance(centers, list):
        raise ValueError("Zenoti centers response has no centers array.")
    result = {c["id"]: c.get("name") or "" for c in centers if isinstance(c, dict) and c.get("id")}
    logger.info("Found %d center(s) on this account", len(result))
    return result


def list_guests(get, center_id, last_updated=None, start_page=1, on_page_complete=None):
    """Yield every guest of a center; fail instead of silently skipping pages."""
    seen = set()
    for page in range(start_page, MAX_PAGES + 1):
        params = {"center_id": center_id, "page": page, "size": PAGE_SIZE}
        if last_updated:
            params["last_updated"] = last_updated
        guests = get("/guests", params).get("guests")
        if not isinstance(guests, list):
            raise ValueError("Zenoti guest list response has no guests array.")
        logger.info("  page %d: fetched %d guest(s) (%d before this page)", page, len(guests), len(seen))
        added = 0
        for guest in guests:
            if not isinstance(guest, dict) or not guest.get("id"):
                raise ValueError("Zenoti returned a guest without an id.")
            if guest["id"] not in seen:
                seen.add(guest["id"])
                added += 1
                yield guest
        if guests and not added:
            raise ValueError("Zenoti repeated a page; stopped to avoid an incomplete export.")
        if on_page_complete:
            on_page_complete(page + 1)
        if len(guests) < PAGE_SIZE:
            return
    raise ValueError(f"More than {MAX_PAGES} pages for center {center_id}; use a shorter --from/--to range.")


def collect_rows(get, center_ids, names, chunks=None, on_row=None, details=None, checkpoint=None):
    """Return (rows, detail_lookups).

    chunks is a list of inclusive (start, end) creation-date windows, each short
    enough for Zenoti; None means every guest with no date filter. For each chunk
    the guest list is requested with last_updated=<chunk start> and only guests
    whose created_date lies inside the chunk are kept. created_date comes from the
    list or, when missing, from the guest details. A guest whose created_date can't
    be read is kept (and counted in a warning) rather than silently dropped.
    Each guest is written once and its details are fetched at most once.
    """
    rows = checkpoint.load_rows() if checkpoint else []
    fallbacks = 0
    unknown_created = set()
    details = details if details is not None else (checkpoint.load_details() if checkpoint else {})
    started = time.monotonic()

    def lookup(guest_id):
        nonlocal fallbacks
        if guest_id not in details:
            details[guest_id] = get(f"/guests/{uuid_value(guest_id)}")
            if checkpoint:
                checkpoint.save_detail(guest_id, details[guest_id])
            fallbacks += 1
            if fallbacks % PROGRESS_EVERY == 0:
                elapsed = time.monotonic() - started
                logger.info("  detail lookups: %d done (%.1f/s, %s elapsed)",
                            fallbacks, fallbacks / elapsed if elapsed else 0.0, format_duration(elapsed))
        return details[guest_id]

    windows = chunks or [None]
    state = checkpoint.get_state() if checkpoint else {
        "phase": "collect", "center_index": 0, "chunk_index": 0, "page": 1
    }
    if state["phase"] != "collect":
        return rows, fallbacks
    positions = [(center_index, chunk_index)
                 for center_index in range(len(center_ids))
                 for chunk_index in range(len(windows))]
    current_position = (state["center_index"], state["chunk_index"])
    start_position = positions.index(current_position) if current_position in positions else len(positions)
    for number, center_id in enumerate(center_ids, 1):
        name, before = names.get(center_id, ""), len(rows)
        logger.info("Center %d/%d: %s (%s)", number, len(center_ids), name or "unnamed", center_id)
        seen = {row["guest_id"] for row in rows if row["center_id"] == center_id}
        for index, window in enumerate(windows, 1):
            position = (number - 1, index - 1)
            if positions.index(position) < start_position:
                continue
            start, end = window or (None, None)
            chunk_before = len(rows)
            if window:
                logger.info(" Chunk %d/%d: guests created %s through %s", index, len(windows), start, end)

            def page_complete(next_page, position=position):
                if checkpoint is not None:
                    checkpoint.set_cursor(position[0], position[1], next_page)

            current_state = checkpoint.get_state() if checkpoint else None
            start_page = (current_state["page"]
                          if current_state and current_state["phase"] == "collect"
                          and (current_state["center_index"], current_state["chunk_index"]) == position
                          else 1)
            for guest in list_guests(
                    get, center_id, start.isoformat() if start else None, start_page,
                    on_page_complete=page_complete if checkpoint else None):
                guest_id = guest["id"]
                if guest_id in seen:
                    continue
                info = guest.get("personal_info")
                center_name = name or guest.get("center_name") or ""
                created = parse_created(guest.get("created_date")) if window else None
                if window and created and not in_window(created, start, end):
                    continue  # known to be outside this chunk: no lookup needed
                if needs_detail(info) or (window and created is None):
                    detail = lookup(guest_id)
                    if needs_detail(info):
                        info = detail.get("personal_info")
                    center_name = center_name or detail.get("center_name") or ""
                    if window and created is None:
                        created = parse_created(detail.get("created_date"))
                        if created is None:
                            unknown_created.add(guest_id)
                        elif not in_window(created, start, end):
                            continue
                seen.add(guest_id)
                first_name, last_name = name_fields(info)
                row = {
                    "center_id": center_id,
                    "center_name": center_name,
                    "guest_id": guest_id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "date_of_birth": parse_birthday(info.get("date_of_birth") if isinstance(info, dict) else None),
                }
                rows.append(row)
                if checkpoint:
                    checkpoint.save_row(row)
                if on_row:
                    on_row(row)  # written to disk immediately
            if window:
                logger.info(" Chunk %d/%d done: %d guest(s) kept", index, len(windows), len(rows) - chunk_before)
            if checkpoint:
                next_position = positions.index(position) + 1
                if next_position == len(positions):
                    checkpoint.set_phase("confirm")
                else:
                    next_center, next_chunk = positions[next_position]
                    checkpoint.set_cursor(next_center, next_chunk, 1)
        logger.info("Center done: %d guest(s) collected", len(rows) - before)
    if unknown_created:
        logger.warning("%d guest(s) had no readable created_date and were kept", len(unknown_created))
    return rows, fallbacks


def confirm_empty_birthdays(get, rows, details, checkpoint=None):
    """Confirm blank list birthdays using guest details, reusing prior lookups."""
    candidates = [row for row in rows if not row["date_of_birth"]]
    logger.info("Confirming birthdays for %d guest(s) with a blank birthday", len(candidates))
    lookups = birthdays_found = 0
    started = time.monotonic()
    for row in candidates:
        if checkpoint and checkpoint.is_confirmed(row):
            continue
        guest_id = row["guest_id"]
        if guest_id not in details:
            details[guest_id] = get(f"/guests/{uuid_value(guest_id)}")
            if checkpoint:
                checkpoint.save_detail(guest_id, details[guest_id])
            lookups += 1
            if lookups % PROGRESS_EVERY == 0:
                elapsed = time.monotonic() - started
                logger.info("  birthday confirmations: %d detail lookups done (%.1f/s, %s elapsed)",
                            lookups, lookups / elapsed if elapsed else 0.0, format_duration(elapsed))
        info = details[guest_id].get("personal_info")
        birthday = parse_birthday(info.get("date_of_birth") if isinstance(info, dict) else None)
        if birthday:
            row["date_of_birth"] = birthday
            birthdays_found += 1
        if checkpoint:
            checkpoint.save_confirmation(row)
    logger.info("Birthday confirmation complete: %d checked, %d birthday/birthdays found",
                len(candidates), birthdays_found)
    return lookups


class CsvStream:
    """Create the CSV up front and flush every row so the file grows live."""

    def __init__(self, output):
        self.count = 0
        # "x" = exclusive create: never overwrite an earlier export.
        self.file = output.open("x", newline="", encoding="utf-8-sig")
        self.writer = csv.DictWriter(self.file, fieldnames=COLUMNS)
        self.writer.writeheader()
        self.file.flush()

    def write(self, row):
        self.writer.writerow({  # block spreadsheet formula injection (names are user-entered)
            k: "'" + v if isinstance(v, str) and v.startswith(("=", "+", "-", "@", "\t", "\r", "\n")) else v
            for k, v in row.items()
        })
        self.file.flush()
        self.count += 1

    def close(self):
        self.file.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class ExportCheckpoint:
    """Persist collected rows, detail responses, and the next resumable position."""

    def __init__(self, path, config, resume=False):
        if resume and not path.is_file():
            raise ValueError(f"No checkpoint found at {path}.")
        if not resume and path.exists():
            raise ValueError(f"Checkpoint already exists at {path}; use --resume to continue it.")
        self.path = path
        self.db = sqlite3.connect(str(path))
        self.db.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS rows (center_id TEXT NOT NULL, guest_id TEXT NOT NULL, "
            "data TEXT NOT NULL, PRIMARY KEY (center_id, guest_id))")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS details (guest_id TEXT PRIMARY KEY, data TEXT NOT NULL)")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS confirmations (center_id TEXT NOT NULL, guest_id TEXT NOT NULL, "
            "PRIMARY KEY (center_id, guest_id))")
        config_json = json.dumps(config, sort_keys=True)
        saved_config = self.db.execute(
            "SELECT value FROM metadata WHERE key = 'config'").fetchone()
        if saved_config is None:
            if resume:
                self.db.close()
                raise ValueError("Checkpoint has no saved run configuration.")
            self.db.execute("INSERT INTO metadata VALUES ('config', ?)", (config_json,))
            for key, value in (("phase", "collect"), ("center_index", "0"),
                               ("chunk_index", "0"), ("page", "1")):
                self.db.execute("INSERT INTO metadata VALUES (?, ?)", (key, value))
            self.db.commit()
        elif saved_config[0] != config_json:
            self.db.close()
            raise ValueError("Checkpoint settings do not match this run. Use the same centers, date range, API base, and output path.")

    def get_state(self):
        values = dict(self.db.execute(
            "SELECT key, value FROM metadata WHERE key IN "
            "('phase', 'center_index', 'chunk_index', 'page')"))
        return {
            "phase": values["phase"],
            "center_index": int(values["center_index"]),
            "chunk_index": int(values["chunk_index"]),
            "page": int(values["page"]),
        }

    def _set_values(self, **values):
        self.db.executemany("UPDATE metadata SET value = ? WHERE key = ?",
                            [(str(value), key) for key, value in values.items()])
        self.db.commit()

    def set_cursor(self, center_index, chunk_index, page):
        self._set_values(phase="collect", center_index=center_index,
                         chunk_index=chunk_index, page=page)

    def set_phase(self, phase):
        self._set_values(phase=phase)

    def save_row(self, row):
        self.db.execute("INSERT OR IGNORE INTO rows VALUES (?, ?, ?)",
                        (row["center_id"], row["guest_id"], json.dumps(row)))

    def update_row(self, row):
        self.db.execute("UPDATE rows SET data = ? WHERE center_id = ? AND guest_id = ?",
                        (json.dumps(row), row["center_id"], row["guest_id"]))

    def is_confirmed(self, row):
        return self.db.execute(
            "SELECT 1 FROM confirmations WHERE center_id = ? AND guest_id = ?",
            (row["center_id"], row["guest_id"])).fetchone() is not None

    def save_confirmation(self, row):
        self.update_row(row)
        self.db.execute("INSERT OR IGNORE INTO confirmations VALUES (?, ?)",
                        (row["center_id"], row["guest_id"]))
        self.db.commit()

    def load_rows(self):
        return [json.loads(record[0]) for record in
                self.db.execute("SELECT data FROM rows ORDER BY rowid")]

    def save_detail(self, guest_id, detail):
        self.db.execute("INSERT OR REPLACE INTO details VALUES (?, ?)",
                        (guest_id, json.dumps(detail)))
        self.db.commit()

    def load_details(self):
        return {guest_id: json.loads(data) for guest_id, data in
                self.db.execute("SELECT guest_id, data FROM details")}

    def close(self):
        self.db.commit()
        self.db.close()


def write_csv_atomically(rows, output):
    """Write a complete CSV beside the destination, then publish it in one rename."""
    partial = output.with_name(output.name + ".partial")
    with partial.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: "'" + value if isinstance(value, str) and
                value.startswith(("=", "+", "-", "@", "\t", "\r", "\n")) else value
                for key, value in row.items()
            })
        file.flush()
        os.fsync(file.fileno())
    if output.exists():
        raise FileExistsError(f"Output already exists: {output}")
    os.rename(partial, output)


def ingest_rows(rows):
    """Atomically refresh fetched guest keys in the configured SQL Server table."""
    if not rows:
        logger.info("No guests fetched; SQL table left unchanged.")
        return 0

    table_name = os.getenv("TABLE_GUEST_DOB") or "BRONZE_ZENOTI_DOB"
    if not table_name.replace("_", "").isalnum():
        raise ValueError("TABLE_GUEST_DOB must be a simple SQL table name.")
    qualified_table = f"[dbo].[{table_name}]"
    expected_schema = {
        "center_id": ("varchar", 36),
        "center_name": ("varchar", 255),
        "guest_id": ("varchar", 36),
        "first_name": ("varchar", 255),
        "last_name": ("varchar", 255),
        "date_of_birth": ("varchar", 50),
    }

    unique_rows = {}
    for row in rows:
        unique_rows[(row["center_id"], row["guest_id"])] = row
    records = list(unique_rows.values())
    keys = list(unique_rows)
    batch_size = 1000
    from db_helper import get_connection

    connection = get_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """,
            "dbo",
            table_name,
        )
        actual_schema = {
            name: ((data_type or "").lower(), char_length)
            for name, data_type, char_length in cursor.fetchall()
        }
        if actual_schema != expected_schema:
            raise ValueError(
                f"{qualified_table} schema does not match the birthday export schema: "
                f"{actual_schema}"
            )

        delete_sql = (
            f"DELETE FROM {qualified_table} WHERE [center_id] = ? AND [guest_id] = ?"
        )
        for offset in range(0, len(keys), batch_size):
            cursor.executemany(delete_sql, keys[offset:offset + batch_size])

        columns = ", ".join(f"[{column}]" for column in COLUMNS)
        placeholders = ", ".join("?" for _ in COLUMNS)
        insert_sql = (
            f"INSERT INTO {qualified_table} ({columns}) VALUES ({placeholders})"
        )
        cursor.fast_executemany = True
        try:
            values = [
                tuple(row[column] or None for column in COLUMNS)
                for row in records
            ]
            for offset in range(0, len(values), batch_size):
                cursor.executemany(insert_sql, values[offset:offset + batch_size])
        finally:
            cursor.fast_executemany = False

        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    logger.info("Reconciled %s guest row(s) in %s.", f"{len(records):,}", qualified_table)
    return len(records)


# ---------------------------------------------------------------- self-test
def self_test():
    import io
    import tempfile

    records = []  # captured log messages (also keeps test output quiet)
    class Capture(logging.Handler):
        def emit(self, record): records.append(record.getMessage())
    setup_logging()
    logger.handlers[:] = [Capture()]

    assert parse_birthday("2000-02-29T23:30:00-05:00") == "2000-02-29"
    assert parse_birthday(None) == "" == parse_birthday("")
    try:
        parse_birthday("2001-02-29")
        raise SystemExit("invalid date accepted")
    except ValueError:
        pass
    assert parse_created("2026-10-21T09:00:00") == date(2026, 10, 21)
    assert parse_created(None) is None and parse_created("garbage") is None

    assert name_fields({"first_name": " Ann ", "last_name": "Lee"}) == ("Ann", "Lee")
    assert name_fields({"first_name": "Ann", "last_name": None}) == ("Ann", "")
    assert name_fields(None) == ("", "") == name_fields({})
    assert needs_detail(None) and needs_detail({"date_of_birth": "x"}) and needs_detail({"first_name": "A"})
    assert not needs_detail({"first_name": "A", "date_of_birth": None})

    # Date window: previous two complete days, excluding today.
    today = date(2026, 10, 23)
    def window(**options):
        base = {"all": False, "date_from": None, "date_to": None}
        return resolve_window(argparse.Namespace(**{**base, **options}), today)
    assert window()[:2] == (date(2026, 10, 21), date(2026, 10, 22)), "default must cover two days"
    assert window(date_from="2026-10-01")[:2] == (date(2026, 10, 1), date(2026, 10, 22))
    assert window(date_from="2026-10-01", date_to="2026-10-10")[:2] == (date(2026, 10, 1), date(2026, 10, 10))
    assert window(date_from="2026-10-22", date_to="2026-10-22")[:2] == (date(2026, 10, 22), date(2026, 10, 22))
    assert window(all=True)[:2] == (None, None)
    for options in ({"all": True, "date_from": "2026-10-01"}, {"date_to": "2026-10-10"},
                    {"date_from": "2026-10-10", "date_to": "2026-10-01"}, {"date_from": "2026-10-23"},
                    {"date_from": "2026-10-01", "date_to": "2026-10-24"},
                    {"date_from": "2026-02-30"}, {"date_from": "10/01/2026"}):
        try:
            window(**options)
            raise SystemExit(f"bad options accepted: {options}")
        except ValueError:
            pass

    # Long ranges are split into chunks of at most 30 days, with no gaps or overlaps
    assert split_range(date(2026, 10, 21), date(2026, 10, 22)) == [(date(2026, 10, 21), date(2026, 10, 22))]
    assert split_range(date(2026, 1, 1), date(2026, 3, 31)) == [
        (date(2026, 1, 1), date(2026, 1, 30)), (date(2026, 1, 31), date(2026, 3, 1)),
        (date(2026, 3, 2), date(2026, 3, 31))]
    assert len(split_range(date(2026, 1, 1), date(2026, 1, 31))) == 2          # 31 days > 30
    assert len(split_range(date(2026, 1, 1), date(2026, 1, 31), 31)) == 1
    assert split_range(date(2026, 5, 5), date(2026, 5, 5)) == [(date(2026, 5, 5), date(2026, 5, 5))]

    # .env reader: quotes, comments, export, no override of real env vars
    with tempfile.TemporaryDirectory() as d:
        env = Path(d) / ".env"
        env.write_text("# c\nexport T_A='one two'\nT_B=plain # note\nT_C=\"q\"\nT_KEEP=file\n", encoding="utf-8")
        os.environ["T_KEEP"] = "real"
        load_env([env, Path(d) / "missing.env"])
        assert (os.environ["T_A"], os.environ["T_B"], os.environ["T_C"], os.environ["T_KEEP"]) == \
            ("one two", "plain", "q", "real")

    # HTTP client: retry on 503, then success; hard failure on 403; bad JSON
    class Resp(io.BytesIO):
        def __enter__(self): return self
        def __exit__(self, *a): return None
    class FakeOpener:
        def __init__(self, script): self.script, self.calls = list(script), []
        def open(self, request, timeout=None):
            self.calls.append(request.full_url)
            item = self.script.pop(0)
            if isinstance(item, int):
                headers = Message()
                headers["Retry-After"] = "39.44"  # decimal, as Zenoti can send it
                raise urllib.error.HTTPError(request.full_url, item, "x", headers, None)
            if isinstance(item, Exception):
                raise item
            return Resp(item)
    slept = []
    opener = FakeOpener([503, b'{"ok": 1}'])
    get = make_getter("https://x/v1", "k", 0, opener, slept.append)
    assert get("/centers", {"a": 1}) == {"ok": 1} and len(opener.calls) == 2
    assert opener.calls[0] == "https://x/v1/centers?a=1"
    assert 39.44 in slept, slept  # decimal Retry-After honored
    for script, label in (([403], "403"), ([b"nope"], "json"), ([b'{"error": {"code": 9}}'], "error")):
        try:
            make_getter("https://x/v1", "k", 0, FakeOpener(script), slept.append)("/p")
            raise SystemExit(f"{label} not rejected")
        except ValueError:
            pass

    # Network errors surface the underlying reason
    boom = urllib.error.URLError("SSL: CERTIFICATE_VERIFY_FAILED")
    try:
        make_getter("https://x/v1", "k", 0, FakeOpener([boom] * MAX_ATTEMPTS), lambda _s: None)("/p")
        raise SystemExit("network error not raised")
    except ValueError as exc:
        assert "CERTIFICATE_VERIFY_FAILED" in str(exc), exc

    # Pagination + detail fallback + CSV safety (no date filter)
    def fake_get(path, params=None):
        params = params or {}
        if path == "/centers":
            return {"centers": [{"id": "c1", "name": "=Main"}]}
        if path == "/guests":
            if params.get("page") == 1:
                return {"guests": [{"id": f"g{i}", "personal_info": {
                    "first_name": "Ann", "last_name": "Lee", "date_of_birth": "1990-01-02"}}
                    for i in range(PAGE_SIZE)]}
            return {"guests": [{"id": str(UUID(int=1))}]}
        return {"personal_info": {"first_name": "Bo", "last_name": "Kay", "date_of_birth": "1985-06-07"}}
    names = list_centers(fake_get)
    streamed = []
    rows, fallbacks = collect_rows(fake_get, ["c1"], names, on_row=streamed.append)
    assert len(rows) == PAGE_SIZE + 1 and fallbacks == 1 and streamed == rows
    assert rows[-1]["date_of_birth"] == "1985-06-07" and rows[-1]["guest_id"] == str(UUID(int=1))
    assert (rows[-1]["first_name"], rows[-1]["last_name"]) == ("Bo", "Kay")
    blank_calls = []
    def blank_get(path, params=None):
        if path == "/guests":
            return {"guests": [{"id": str(UUID(int=2)), "personal_info": {
                "first_name": "Jo", "last_name": "Doe", "date_of_birth": None}}]}
        blank_calls.append(path)
        return {"personal_info": {"date_of_birth": "1992-04-03"}}
    details = {}
    blank_rows, blank_fallbacks = collect_rows(blank_get, ["c1"], {}, details=details)
    confirm_fallbacks = confirm_empty_birthdays(blank_get, blank_rows, details)
    assert blank_rows[0]["date_of_birth"] == "1992-04-03"
    assert blank_fallbacks == 0 and confirm_fallbacks == 1 and len(blank_calls) == 1
    assert confirm_empty_birthdays(blank_get, blank_rows, details) == 0
    with tempfile.TemporaryDirectory() as d:
        checkpoint_path = Path(d) / "export.sqlite"
        config = {"center_ids": ["A"], "output": "export.csv"}
        saved = {"center_id": "A", "center_name": "Center", "guest_id": str(UUID(int=10)),
                 "first_name": "First", "last_name": "Guest", "date_of_birth": "1990-01-01"}
        checkpoint = ExportCheckpoint(checkpoint_path, config)
        checkpoint.save_row(saved)
        checkpoint.set_cursor(0, 0, 2)
        checkpoint.close()

        checkpoint = ExportCheckpoint(checkpoint_path, config, resume=True)
        resumed_pages = []
        resume_info = {"first_name": "Resume", "last_name": "Guest",
                       "date_of_birth": "1990-01-01"}
        def resume_get(path, params=None):
            assert path == "/guests" and params is not None
            resumed_pages.append(params["page"])
            return {"guests": [
                {"id": str(UUID(int=10)), "personal_info": resume_info},
                {"id": str(UUID(int=11)), "personal_info": resume_info},
            ]}
        resumed_rows, _ = collect_rows(resume_get, ["A"], {"A": "Center"},
                                       checkpoint=checkpoint)
        assert resumed_pages == [2]
        assert sorted(row["guest_id"] for row in resumed_rows) == [
            str(UUID(int=10)), str(UUID(int=11))]
        assert checkpoint.get_state()["phase"] == "confirm"
        checkpoint.close()

        checkpoint = ExportCheckpoint(checkpoint_path, config, resume=True)
        assert checkpoint.get_state()["phase"] == "confirm" and len(checkpoint.load_rows()) == 2
        checkpoint.close()

        confirm_path = Path(d) / "confirm.sqlite"
        confirm_config = {"center_ids": ["A"], "output": "confirm.csv"}
        confirm_rows = [
            {"center_id": "A", "center_name": "Center", "guest_id": str(UUID(int=20)),
             "first_name": "First", "last_name": "Blank", "date_of_birth": ""},
            {"center_id": "A", "center_name": "Center", "guest_id": str(UUID(int=21)),
             "first_name": "Second", "last_name": "Blank", "date_of_birth": ""},
        ]
        checkpoint = ExportCheckpoint(confirm_path, confirm_config)
        for row in confirm_rows:
            checkpoint.save_row(row)
        checkpoint.set_phase("confirm")
        confirmation_calls = []
        def interrupted_confirm(path, params=None):
            confirmation_calls.append(path)
            if path.endswith(str(UUID(int=21))):
                raise ValueError("simulated interruption")
            return {"personal_info": {}}
        try:
            confirm_empty_birthdays(interrupted_confirm, confirm_rows, {}, checkpoint)
            raise SystemExit("expected simulated interruption")
        except ValueError as exc:
            assert "simulated interruption" in str(exc)
        checkpoint.close()

        checkpoint = ExportCheckpoint(confirm_path, confirm_config, resume=True)
        confirm_rows = checkpoint.load_rows()
        def resumed_confirm(path, params=None):
            confirmation_calls.append(path)
            return {"personal_info": {"date_of_birth": "1992-04-03"}}
        confirm_empty_birthdays(resumed_confirm, confirm_rows, checkpoint.load_details(), checkpoint)
        checkpoint.close()
        assert confirmation_calls == [
            f"/guests/{str(UUID(int=20))}", f"/guests/{str(UUID(int=21))}",
            f"/guests/{str(UUID(int=21))}"], confirmation_calls
        assert [row["date_of_birth"] for row in confirm_rows] == ["", "1992-04-03"]
    try:
        list(list_guests(lambda *a, **k: {"guests": [{"id": "x"}] * PAGE_SIZE}, "c1"))
        raise SystemExit("repeated page accepted")
    except ValueError:
        pass
    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / "o.csv"
        sink = CsvStream(out)
        sink.write(rows[0])
        with out.open(newline="", encoding="utf-8-sig") as f:   # readable before close = live
            data = list(csv.DictReader(f))
        assert len(data) == 1 and (data[0]["first_name"], data[0]["last_name"]) == ("Ann", "Lee")
        assert data[0]["center_name"] == "'=Main" and data[0]["date_of_birth"] == "1990-01-02"
        sink.close()
        try:
            CsvStream(out)
            raise SystemExit("overwrote file")
        except FileExistsError:
            pass

    # Chunked range: each chunk asks Zenoti with its own start date; one row per guest;
    # one detail lookup per guest even across chunks
    def uid(n):
        return str(UUID(int=n))
    full = {"first_name": "A", "last_name": "B", "date_of_birth": "1990-01-01"}
    asked, detail_calls = [], []
    def chunk_get(path, params=None):
        if path == "/guests":
            assert params is not None
            asked.append(params["last_updated"])
            return {"guests": [
                {"id": uid(1), "created_date": "2026-01-05T10:00:00", "personal_info": full},
                {"id": uid(2), "created_date": "2026-02-10T10:00:00", "personal_info": full},
                {"id": uid(3), "created_date": "2026-03-20T10:00:00", "personal_info": full},
                {"id": uid(4), "personal_info": full},                                  # no created_date
                {"id": uid(5), "created_date": "2025-12-01T10:00:00", "personal_info": full}]}  # before range
        detail_calls.append(path)
        return {"created_date": "2026-02-15T09:00:00", "personal_info": full}
    chunks = split_range(date(2026, 1, 1), date(2026, 3, 31))
    rows_c, fb_c = collect_rows(chunk_get, ["A"], {}, chunks=chunks)
    assert sorted(r["guest_id"] for r in rows_c) == [uid(1), uid(2), uid(3), uid(4)], rows_c
    assert asked == ["2026-01-01", "2026-01-31", "2026-03-02"], asked
    assert fb_c == 1 and len(detail_calls) == 1, "details fetched once per guest"
    # Short default window: a single chunk, guests outside it are dropped
    rows_s, _ = collect_rows(chunk_get, ["A"], {}, chunks=[(date(2026, 2, 1), date(2026, 2, 28))])
    assert sorted(r["guest_id"] for r in rows_s) == [uid(2), uid(4)], rows_s
    # Guest with no readable created_date anywhere is kept and reported
    del records[:]
    def nocreated_get(path, params=None):
        if path == "/guests":
            return {"guests": [{"id": uid(1), "personal_info": full}]}
        return {"personal_info": full}
    rows_n, _ = collect_rows(nocreated_get, ["A"], {}, chunks=[(date(2026, 1, 1), date(2026, 1, 2))])
    assert len(rows_n) == 1 and any("no readable created_date" in m for m in records), records

    # Logging: progress lines and retry warnings stream through the logger
    del records[:]
    collect_rows(fake_get, ["c1"], names)
    make_getter("https://x/v1", "k", 0, FakeOpener([503, b'{"ok": 1}']), lambda _s: None)("/p")
    assert any("page 1: fetched 100 guest(s)" in m for m in records), records
    assert any("Center done: 101" in m for m in records), records
    assert any("HTTP 503 on /p; retry 1/4" in m for m in records), records
    assert format_duration(3725) == "1h02m05s" and format_duration(65) == "1m05s"
    print("Self-test passed.")


# ---------------------------------------------------------------- main
def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--center-id", action="append", help="Center UUID(s), comma-separated; default: ZENOTI_CENTER_ID or all centers")
    parser.add_argument("--from", dest="date_from", metavar="YYYY-MM-DD", help="Range start (inclusive); default: two days ago")
    parser.add_argument("--to", dest="date_to", metavar="YYYY-MM-DD", help="Range end (inclusive); default: yesterday")
    parser.add_argument("--chunk-days", type=int, default=MAX_CHUNK_DAYS, help=f"Longest single request window in days; longer ranges are split (default {MAX_CHUNK_DAYS}, max 31)")
    parser.add_argument("--all", action="store_true", help="No date filter: one-time full export of every guest")
    parser.add_argument("--output", type=Path, help="New CSV path; existing files are never overwritten")
    parser.add_argument("--resume", action="store_true", help="Resume an interrupted export (requires the original --output and same filters)")
    parser.add_argument("--delay", type=float, default=0.4, help="Pause before each API call in seconds (default 0.4; use 1 to stay within Zenoti's 60 calls/min)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Also log every API call")
    parser.add_argument("--log-file", type=Path, help="Also append the log to this file")
    parser.add_argument("--self-test", action="store_true", help="Run offline checks and exit")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if not 0 <= args.delay < float("inf"):
        parser.error("--delay must be a finite, non-negative number.")
    if not 1 <= args.chunk_days <= 31:
        parser.error("--chunk-days must be between 1 and 31.")
    try:
        start, end, window_label = resolve_window(args)
    except ValueError as exc:
        parser.error(str(exc))
    chunks = split_range(start, end, args.chunk_days) if start else None

    env_files = list(dict.fromkeys([HERE / ".env", HERE.parent / ".env", Path.cwd() / ".env"]))
    try:
        setup_logging(args.verbose, args.log_file)
    except OSError as exc:
        parser.error(f"Cannot open log file: {exc}")
    load_env(env_files)
    key = (os.getenv("ZENOTI_API_KEY") or "").strip()
    if not key:
        searched = "\n".join(f"  {f}  ({'found' if f.is_file() else 'not found'})" for f in env_files)
        parser.error("ZENOTI_API_KEY is not set in the environment or any .env file. Searched:\n"
                     + searched + "\nThe .env needs a line like: ZENOTI_API_KEY=your-key")
    base = (os.getenv("ZENOTI_API_BASE") or os.getenv("ZENOTI_BASE_URL") or "https://api.zenoti.com/v1").strip().rstrip("/")
    parts = urllib.parse.urlsplit(base)
    if parts.scheme != "https" or not parts.hostname or parts.username or parts.password or parts.query or parts.fragment:
        parser.error("Zenoti API base must be an HTTPS URL without credentials, query, or fragment.")
    if not base.endswith("/v1"):
        base += "/v1"

    raw_centers = args.center_id or ([os.environ["ZENOTI_CENTER_ID"]] if os.getenv("ZENOTI_CENTER_ID") else [])
    try:
        center_ids = list(dict.fromkeys(uuid_value(v) for group in raw_centers for v in group.split(",") if v.strip()))
    except ValueError as exc:
        parser.error(str(exc))

    output = (args.output or HERE / f"guest_birthdays_{datetime.now():%Y%m%d_%H%M%S}.csv").resolve()
    checkpoint_path = output.with_name(output.name + ".checkpoint.sqlite")
    if output.exists() and not args.resume:
        parser.error(f"Output already exists: {output}. Choose a new --output path.")
    if not output.parent.is_dir():
        parser.error(f"Output directory does not exist: {output.parent}")
    if args.resume and not args.output:
        parser.error("--resume requires the original --output path.")

    logger.info("Window: %s", window_label)
    if chunks and len(chunks) > 1 and start is not None and end is not None:
        logger.info("Range is %d days; Zenoti limits a request to about a month, so it is split into %d chunks of up to %d days",
                    (end - start).days + 1, len(chunks), args.chunk_days)
    run_started = time.monotonic()
    get = make_getter(base, key, args.delay)
    try:
        names = list_centers(get)
        unknown = [c for c in center_ids if c not in names]
        if unknown:
            logger.warning("Not in your center list (names may be blank): %s", ", ".join(unknown))
        source = "--center-id / ZENOTI_CENTER_ID" if center_ids else "all centers on this account"
        center_ids = center_ids or list(names)
        if not center_ids:
            raise ValueError("No centers found for this API key.")
        logger.info("Centers to export (%s): %s", source, ", ".join(names.get(c) or "unnamed" for c in center_ids))
    except ValueError as exc:
        logger.error("%s No CSV was written.", exc)
        raise SystemExit(1) from exc

    logger.info("Starting export for %d center(s)", len(center_ids))
    config = {
        "version": 3,
        "api_base": base,
        "center_ids": center_ids,
        "chunks": [[chunk_start.isoformat(), chunk_end.isoformat()] for chunk_start, chunk_end in chunks]
        if chunks else None,
        "output": str(output),
    }
    try:
        checkpoint = ExportCheckpoint(checkpoint_path, config, resume=args.resume)
    except ValueError as exc:
        parser.error(str(exc))

    try:
        state = checkpoint.get_state()
        if output.exists():
            if args.resume and state["phase"] == "done":
                logger.info("Export was already completed; retrying its SQL ingestion.")
                rows = checkpoint.load_rows()
                checkpoint.close()
                checkpoint = None
                try:
                    ingest_rows(rows)
                except Exception as exc:
                    logger.error("SQL ingestion failed; export and checkpoint retained for retry: %s", exc)
                    raise SystemExit(1) from exc
                checkpoint_path.unlink()
                return
            raise ValueError(f"Output already exists: {output}. It will not be overwritten.")
        logger.info("Checkpoint: %s", checkpoint_path)
        if state["phase"] == "collect":
            rows, fallbacks = collect_rows(get, center_ids, names, chunks, checkpoint=checkpoint)
        else:
            rows, fallbacks = checkpoint.load_rows(), 0
        details = checkpoint.load_details()
        if checkpoint.get_state()["phase"] == "confirm":
            fallbacks += confirm_empty_birthdays(get, rows, details, checkpoint)
            checkpoint.set_phase("done")
        if checkpoint.get_state()["phase"] != "done":
            raise ValueError("Checkpoint did not reach a completed state; it has been kept for resume.")
        write_csv_atomically(rows, output)
    except ValueError as exc:
        logger.error("%s Checkpoint retained for --resume.", exc)
        raise SystemExit(1) from exc
    finally:
        if checkpoint is not None:
            checkpoint.close()

    with_dob = sum(bool(r["date_of_birth"]) for r in rows)
    logger.info("Exported %s guest(s); %s with birthdays, %s without.",
                f"{len(rows):,}", f"{with_dob:,}", f"{len(rows) - with_dob:,}")
    if fallbacks:
        logger.info("%s guest(s) needed a separate detail request.", f"{fallbacks:,}")
    try:
        ingest_rows(rows)
    except Exception as exc:
        logger.error("SQL ingestion failed; export retained: %s", exc)
        raise SystemExit(1) from exc
    checkpoint_path.unlink()
    logger.info("Finished in %s. CSV: %s", format_duration(time.monotonic() - run_started), output)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted. Checkpoint retained; rerun with --resume and the same --output and filters to continue.")
        raise SystemExit(130) from None
    except (OSError, EOFError) as exc:
        raise SystemExit(str(exc)) from exc