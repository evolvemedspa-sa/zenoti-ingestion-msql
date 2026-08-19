"""Fetch purchase orders from the Zenoti API and load them into SQL.

Port of the n8n "po" HTTP Request node: GET /v1/inventory/purchase_orders with
center_id, start_date and end_date as query parameters. Unlike the n8n node the
three parameters are dynamic here - pass them on the command line, or fall back
to the .env defaults.

The API nests three levels deep (orders -> partials -> line_items) while
BRONZE_ZENOTI_PURCHASE_ORDER is one row per product per order, so the partials
are merged per product before loading. See flatten_line_items() for why.

Usage
-----
  # asks for the Start Date and End Date, then loads into SQL
  python purchase_order.py

  # skip the prompts by passing the range
  python purchase_order.py --start-date 2026-07-29 --end-date 2026-07-29

  # a whole month across several centers
  python purchase_order.py --month 2026-07 --center-id id1,id2

  # see exactly what would be loaded, touch nothing
  python purchase_order.py --month 2026-07 --dry-run

  # re-run a range without duplicating rows already loaded for those orders
  python purchase_order.py --month 2026-07 --replace

  # skip SQL entirely and just write the CSV
  python purchase_order.py --month 2026-07 --no-load --csv
"""
import argparse
import calendar
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pyodbc
from db_helper import get_connection
import requests
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# ==================================
# Read Config
# ==================================
API_KEY = os.getenv("ZENOTI_API_KEY")
API_BASE = os.getenv("ZENOTI_API_BASE", "https://api.zenoti.com/v1").rstrip('/')
DEFAULT_CENTER_ID = os.getenv("ZENOTI_CENTER_ID")
CENTERS_FILE = os.getenv(
    "ZENOTI_CENTERS_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'centers.json')
)
OUTPUT_DIR = os.getenv(
    "PURCHASE_ORDER_OUTPUT_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'purchase_orders')
)

SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")
TABLE = os.getenv("TABLE_PURCHASE_ORDER", "BRONZE_ZENOTI_PURCHASE_ORDER")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

ENDPOINT = f"{API_BASE}/inventory/purchase_orders"

# Long ranges time out server-side, so a wide request is split into windows.
MAX_DAYS_PER_REQUEST = 31
PAGE_SIZE = 100
MAX_PAGES = 200
REQUEST_TIMEOUT = 120
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5

# date_inserted is stamped here rather than left to the server's getdate(),
# which runs in UTC; every other loader in this project stamps New York time.
NY_TZ = ZoneInfo("America/New_York")

# Which date start_date/end_date filter on: 1 = raised, 2 = delivered,
# 3 = completed, or "all" to fetch every criterion and de-duplicate by order
# number. The website "Orders Listing" is ordered by the raised date ("Order
# on"), so 1 (raised) is the default and the value that reproduces that screen.
# (transfer_order.py already sends this; the purchase-orders endpoint is the
# sibling inventory endpoint, so it takes the same parameter.)
DATE_CRITERIA = os.getenv("ZENOTI_PO_DATE_CRITERIA", "1")

CRITERIA_LABELS = {1: 'raised', 2: 'delivered', 3: 'completed'}


def parse_date_criteria(value):
    """Resolve a --date-criteria / ZENOTI_PO_DATE_CRITERIA setting to the list of
    criteria to fetch: [1], [2] or [3] for a single date, or [1, 2, 3] when the
    value is 'all' (or 0)."""
    text = str(value).strip().lower()
    if text in ('all', '0'):
        return [1, 2, 3]
    try:
        criterion = int(text)
    except ValueError:
        criterion = None
    if criterion not in (1, 2, 3):
        raise argparse.ArgumentTypeError(
            f"date-criteria must be 1, 2, 3, or 'all'; got {value!r}"
        )
    return [criterion]


# ==================================
# Command Line
# ==================================
def parse_ymd(text, flag):
    """Accept YYYY-MM-DD and reject anything else with a clear message."""
    try:
        return datetime.strptime(str(text).strip(), '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"{flag} must look like YYYY-MM-DD, got {text!r}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Fetch Zenoti purchase orders and load them into "
                    f"{TABLE} (one row per product per order)."
    )
    parser.add_argument(
        '--center-id', action='append', default=[],
        help="Center GUID to pull. Repeat the flag or comma-separate for several. "
             f"Omit it to loop every center in {os.path.basename(CENTERS_FILE)}."
    )
    parser.add_argument(
        '--all-centers', action='store_true',
        help=f"Loop every center in {os.path.basename(CENTERS_FILE)}. This is "
             "already the default when --center-id is omitted."
    )
    parser.add_argument(
        '--list-centers', action='store_true',
        help="Print the centers that would be pulled, then exit."
    )
    parser.add_argument(
        '--continue-on-error', action='store_true',
        help="Keep going if one center's fetch fails, instead of aborting the run."
    )
    parser.add_argument(
        '--start-date', type=lambda v: parse_ymd(v, '--start-date'),
        help="First day to fetch (YYYY-MM-DD). Prompted for if omitted."
    )
    parser.add_argument(
        '--end-date', type=lambda v: parse_ymd(v, '--end-date'),
        help="Last day to fetch, inclusive. Prompted for if omitted."
    )
    parser.add_argument(
        '--month',
        help="Shorthand for a whole calendar month (YYYY-MM). "
             "Overrides --start-date / --end-date."
    )
    parser.add_argument(
        '--date-criteria', type=parse_date_criteria, default=DATE_CRITERIA,
        metavar='{1,2,3,all}',
        help="Which date the range filters on: 1 = raised, 2 = delivered, "
             "3 = completed, or 'all' to fetch every criterion and de-duplicate "
             f"by order number. Default {DATE_CRITERIA!r}. The website Orders "
             "Listing is by raised date, so 1 reproduces that screen."
    )
    parser.add_argument(
        '--replace', action='store_true',
        help="Delete existing rows for the fetched ref_numbers before inserting, "
             "so re-running a range does not duplicate it."
    )
    parser.add_argument(
        '--no-load', action='store_true',
        help="Do not touch SQL. Implies --csv."
    )
    parser.add_argument(
        '--csv', action='store_true',
        help="Also write the rows to a CSV next to the load."
    )
    parser.add_argument(
        '--keep-empty-lines', action='store_true',
        help="Keep line items whose quantities are all zero. These are the "
             "roster padding Zenoti repeats in every partial shipment."
    )
    parser.add_argument(
        '--order-level-csv', action='store_true',
        help="Write the raw one-row-per-order CSV instead. Implies --no-load, "
             "since the SQL table is line-item grain."
    )
    parser.add_argument(
        '--output-dir', default=OUTPUT_DIR,
        help=f"Where to write CSVs. Default: {OUTPUT_DIR}"
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help="Print what would be loaded without writing a file or touching SQL."
    )
    parser.add_argument(
        '--raw-json', action='store_true',
        help="Also save the untouched API response."
    )
    return parser.parse_args(argv)


def load_centers_file(path=CENTERS_FILE):
    """Read centers.json into a list of (center_id, center_name) pairs.

    The file may carry extra keys such as start_date / end_date; those are
    ignored because the run's date range comes from the operator, not the file.
    """
    if not os.path.isfile(path):
        return []
    try:
        with open(path, encoding='utf-8') as fh:
            payload = json.load(fh)
    except (OSError, ValueError) as e:
        raise SystemExit(f"Could not read {path}: {e}")

    if isinstance(payload, dict):
        # Tolerate {"centers": [...]} as well as a bare list.
        payload = payload.get('centers') or payload.get('list') or []
    if not isinstance(payload, list):
        raise SystemExit(f"{path} must contain a list of centers.")

    centers, seen = [], set()
    for entry in payload:
        if isinstance(entry, str):
            center_id, center_name = entry.strip(), ''
        elif isinstance(entry, dict):
            center_id = _first_text(entry.get('center_id'), entry.get('id'))
            center_name = _first_text(entry.get('center_name'), entry.get('name'))
        else:
            continue
        if center_id and center_id not in seen:
            seen.add(center_id)
            centers.append((center_id, center_name))
    return centers


def resolve_centers(values, all_centers=False):
    """Decide which centers to pull, as a list of (center_id, center_name).

    Precedence: explicit --center-id, then --all-centers or centers.json, then
    ZENOTI_CENTER_ID. Running bare with a centers.json present loops every
    center in it, which is the normal case.
    """
    explicit = []
    for value in values:
        for part in str(value).split(','):
            part = part.strip()
            if part and part not in explicit:
                explicit.append(part)

    if explicit:
        # Label the ids from centers.json where possible so the log is readable.
        names = dict(load_centers_file())
        return [(cid, names.get(cid, '')) for cid in explicit]

    from_file = load_centers_file()
    if from_file:
        return from_file

    if all_centers:
        raise SystemExit(
            f"--all-centers needs a centers file. Expected it at {CENTERS_FILE} "
            f"(override with ZENOTI_CENTERS_FILE in the .env)."
        )
    if DEFAULT_CENTER_ID:
        return [(DEFAULT_CENTER_ID.strip(), '')]
    return []


def prompt_for_date(label, default=None):
    """Ask the operator for a date, re-asking until it parses.

    Falls back to the default when there is no console to ask on, so the script
    still works from a scheduler or from run_all.py.
    """
    suffix = f" [{default}]" if default else ""
    if not sys.stdin or not sys.stdin.isatty():
        if default is None:
            raise SystemExit(
                f"{label} is required. There is no console to prompt on, so pass "
                f"--start-date / --end-date (or --month) on the command line."
            )
        print(f"{label}: {default} (no console to prompt on)")
        return default

    while True:
        try:
            raw = input(f"Please enter the {label} (YYYY-MM-DD){suffix}: ").strip()
        except EOFError:
            if default is None:
                raise SystemExit(f"{label} is required.")
            return default
        if not raw and default is not None:
            return default
        try:
            return datetime.strptime(raw, '%Y-%m-%d').date()
        except ValueError:
            print(f"  {raw!r} is not a valid YYYY-MM-DD date. Try again.")


def resolve_range(args):
    """Turn the date flags into an inclusive (start, end) pair.

    Anything not supplied on the command line is asked for, so running the
    script bare walks the operator through the date range.
    """
    if args.month:
        try:
            anchor = datetime.strptime(args.month.strip(), '%Y-%m').date()
        except ValueError:
            raise SystemExit(f"--month must look like YYYY-MM, got {args.month!r}")
        last_day = calendar.monthrange(anchor.year, anchor.month)[1]
        return anchor, anchor.replace(day=last_day)

    start = args.start_date
    if start is None:
        start = prompt_for_date("Start Date", default=date.today())

    end = args.end_date
    if end is None:
        # Enter on its own repeats the start date, i.e. a single-day pull.
        while True:
            end = prompt_for_date("End Date", default=start)
            if end >= start:
                break
            print(f"  End Date {end} is before Start Date {start}. Try again.")
    elif end < start:
        raise SystemExit(f"--end-date {end} is before --start-date {start}")

    return start, end


def split_range(start, end, max_days=MAX_DAYS_PER_REQUEST):
    """Yield (window_start, window_end) chunks of at most max_days each."""
    cursor = start
    while cursor <= end:
        window_end = min(cursor + timedelta(days=max_days - 1), end)
        yield cursor, window_end
        cursor = window_end + timedelta(days=1)


# ==================================
# HTTP
# ==================================
def build_session():
    if not API_KEY:
        raise SystemExit("ZENOTI_API_KEY is not set in the .env file.")
    key = API_KEY.strip()
    # The .env may hold the bare key or the whole "apikey <key>" header value.
    authorization = key if key.lower().startswith('apikey ') else f"apikey {key}"
    session = requests.Session()
    session.headers.update({
        "Authorization": authorization,
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    return session


def request_with_retry(session, params):
    """GET one page, retrying throttling and server errors but not bad requests."""
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(ENDPOINT, params=params, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            last_error = f"{type(e).__name__}: {e}"
            if attempt == MAX_RETRIES:
                break
            print(f"  {last_error} - retrying in {RETRY_BACKOFF_SECONDS * attempt}s")
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
            continue

        if response.status_code == 200:
            try:
                return response.json()
            except ValueError:
                raise SystemExit(
                    f"Zenoti returned non-JSON for {params}:\n{response.text[:1000]}"
                )

        # 429 and 5xx are worth another try; 4xx means the request itself is wrong.
        if response.status_code == 429 or response.status_code >= 500:
            last_error = f"HTTP {response.status_code}: {response.text[:300]}"
            if attempt == MAX_RETRIES:
                break
            print(f"  {last_error} - retrying in {RETRY_BACKOFF_SECONDS * attempt}s")
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
            continue

        raise SystemExit(
            f"Zenoti rejected the request (HTTP {response.status_code}).\n"
            f"Params: {params}\nResponse: {response.text[:1000]}"
        )

    raise SystemExit(f"Gave up after {MAX_RETRIES} attempts. Last error - {last_error}")


LIST_KEYS = (
    'orders', 'purchase_orders', 'purchaseOrders', 'purchase_order',
    'list', 'items', 'data',
)


def extract_rows(payload):
    """Pull the list of purchase orders out of whatever envelope wraps them."""
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in LIST_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            return value
    # Unknown envelope: fall back to the first list of objects it contains.
    for value in payload.values():
        if isinstance(value, list) and (not value or isinstance(value[0], dict)):
            return value
    return []


def fetch_window(session, center_id, start, end, collect_raw=False,
                 date_criteria=1):
    """Fetch every page for one center and one date window."""
    rows, raw_pages = [], []
    previous_signature = None

    for page in range(1, MAX_PAGES + 1):
        params = {
            "center_id": center_id,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "date_criteria": date_criteria,
            "page": page,
            "size": PAGE_SIZE,
        }
        payload = request_with_retry(session, params)
        if collect_raw:
            raw_pages.append(payload)

        page_rows = extract_rows(payload)
        if not page_rows:
            break

        # If the endpoint ignores `page` it hands back the same block forever,
        # so stop as soon as a page repeats instead of looping to MAX_PAGES.
        signature = json.dumps(page_rows[0], sort_keys=True, default=str)[:500]
        if signature == previous_signature:
            break
        previous_signature = signature

        rows.extend(page_rows)
        print(f"    page {page}: {len(page_rows):,} row(s)")

        if len(page_rows) < PAGE_SIZE:
            break
    else:
        print(f"    Warning: stopped at the {MAX_PAGES}-page cap; there may be more.")

    return rows, raw_pages


# ==================================
# Line-item view (the SQL table's grain)
# ==================================
# Column order matches BRONZE_ZENOTI_PURCHASE_ORDER, minus the identity `id`
# and the stamped `date_inserted`.
LINE_ITEM_COLUMNS = [
    'vendor_name',
    'ref_number',
    'date_of_shipment',
    'date_of_delivery',
    'product_code',
    'vendor_part_number',
    'product_name',
    'retail_raised',
    'consumable_raised',
    'pending_retail_quantity',
    'pending_consumable_quantity',
    'mrp_usd',
    'unit_price_usd',
    'retail_received',
    'consumable_received',
    'discount_usd_or_pct',
    'ondelivery_price_usd',
    'total_price_usd',
    'tax',
    'total_tax',
    'notes',
]

INTEGER_COLUMNS = {
    'retail_raised', 'consumable_raised',
    'pending_retail_quantity', 'pending_consumable_quantity',
    'retail_received', 'consumable_received',
}
DECIMAL_COLUMNS = {
    'mrp_usd', 'unit_price_usd', 'ondelivery_price_usd',
    'total_price_usd', 'total_tax',
}

# delivered_discount_type as shown in the PO screen's "Discount ($ or %)" column.
DISCOUNT_TYPE_SUFFIX = {0: '', 1: '%', 2: '$'}


def _num(value):
    """Coerce an API number to float, treating blanks and nulls as zero."""
    if value in (None, ''):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _first_text(*values):
    """Return the first non-blank string among the arguments."""
    for value in values:
        if value not in (None, '') and str(value).strip():
            return str(value).strip()
    return ''


def _clean_timestamp(value):
    """Normalise an API timestamp to 'YYYY-MM-DD HH:MM:SS'.

    Both date columns are varchar in SQL, so this is cosmetic consistency with
    the other bronze tables rather than a type requirement. Anything that does
    not parse is passed through untouched instead of being dropped.
    """
    text = _first_text(value)
    if not text:
        return ''
    try:
        return datetime.fromisoformat(text).strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return text


def flatten_line_items(orders, keep_empty=False):
    """One row per product per purchase order, matching the PO detail screen.

    Zenoti repeats the order's full product roster inside every partial
    shipment, zeroing the quantities on the partials that did not carry that
    product. Order 4240 has 10 products across 2 partials and therefore 20
    line_item entries, but the screen shows 10 rows - so the partials are
    merged per product, summing quantities, rather than exploded.
    """
    rows = []
    for order in orders:
        merged = {}
        for partial in order.get('partials') or []:
            delivered_date = _clean_timestamp(partial.get('delivered_date'))
            for item in partial.get('line_items') or []:
                key = (
                    str(item.get('product_code') or ''),
                    str(item.get('product_name') or ''),
                )
                row = merged.get(key)
                if row is None:
                    row = {
                        'vendor_name': _first_text(order.get('vendor_name')),
                        'ref_number': _first_text(order.get('order_number')),
                        'date_of_shipment': _clean_timestamp(order.get('raised_date')),
                        'date_of_delivery': delivered_date,
                        'product_code': _first_text(item.get('product_code')),
                        'vendor_part_number': _first_text(item.get('vendor_product_part_number')),
                        'product_name': _first_text(item.get('product_name')),
                        'retail_raised': 0.0,
                        'consumable_raised': 0.0,
                        'mrp_usd': _num(item.get('mrp')),
                        'unit_price_usd': _num(item.get('ordered_unit_price')),
                        'retail_received': 0.0,
                        'consumable_received': 0.0,
                        'discount_usd_or_pct': '',
                        'ondelivery_price_usd': _num(item.get('delivered_unit_price')),
                        'total_price_usd': 0.0,
                        'tax': '',
                        'total_tax': 0.0,
                        'notes': '',
                    }
                    merged[key] = row
                elif delivered_date > (row['date_of_delivery'] or ''):
                    # Partials are not in chronological order, so compare rather
                    # than letting the last one seen win.
                    row['date_of_delivery'] = delivered_date

                retail_received = _num(item.get('delivered_retail_quantity'))
                consumable_received = _num(item.get('delivered_consumable_quantity'))
                row['retail_raised'] += _num(item.get('ordered_retail_quantity'))
                row['consumable_raised'] += _num(item.get('ordered_consumable_quantity'))
                row['retail_received'] += retail_received
                row['consumable_received'] += consumable_received
                row['total_price_usd'] += (
                    (retail_received + consumable_received)
                    * _num(item.get('delivered_unit_price'))
                )
                row['total_tax'] += _num(item.get('delivered_tax_amount'))

                # A padding row carries zeroed quantities but still repeats the
                # price and tax group, so only fill from a row that has a value.
                if not row['unit_price_usd']:
                    row['unit_price_usd'] = _num(item.get('ordered_unit_price'))
                if not row['ondelivery_price_usd']:
                    row['ondelivery_price_usd'] = _num(item.get('delivered_unit_price'))
                if not row['mrp_usd']:
                    row['mrp_usd'] = _num(item.get('mrp'))
                row['tax'] = _first_text(
                    row['tax'],
                    item.get('delivered_tax_group_name'),
                    item.get('ordered_tax_group_name'),
                )
                row['notes'] = _first_text(
                    row['notes'], item.get('notes'), partial.get('notes')
                )

                discount = _num(item.get('delivered_discount_value'))
                if discount:
                    suffix = DISCOUNT_TYPE_SUFFIX.get(
                        item.get('delivered_discount_type'), ''
                    )
                    row['discount_usd_or_pct'] = f"{discount:g}{suffix}"

        for row in merged.values():
            row['pending_retail_quantity'] = row['retail_raised'] - row['retail_received']
            row['pending_consumable_quantity'] = (
                row['consumable_raised'] - row['consumable_received']
            )
            if not row['discount_usd_or_pct']:
                row['discount_usd_or_pct'] = '0'
            rows.append(row)

    if not keep_empty:
        quantity_fields = ('retail_raised', 'consumable_raised',
                           'retail_received', 'consumable_received')
        rows = [r for r in rows if any(r[field] for field in quantity_fields)]

    if not rows:
        return pd.DataFrame(columns=LINE_ITEM_COLUMNS)

    df = pd.DataFrame(rows, columns=LINE_ITEM_COLUMNS)
    for col in INTEGER_COLUMNS:
        df[col] = df[col].round().astype('Int64')
    for col in DECIMAL_COLUMNS:
        df[col] = df[col].round(2).astype('Float64')
    return df


def to_order_dataframe(records):
    """Flatten to one row per purchase order - the --order-level-csv shape."""
    if not records:
        return pd.DataFrame()
    df = pd.json_normalize(records, sep='_')
    # Any leftover list/dict cells cannot go in a CSV cell as-is.
    return df.map(lambda v: json.dumps(v, default=str) if isinstance(v, (list, dict)) else v)


# ==================================
# SQL
# ==================================
def connect():
    if not all([SERVER, DATABASE, DB_USER, DB_PASSWORD]):
        missing = [
            k for k, v in {
                "SERVER": SERVER, "DATABASE": DATABASE,
                "DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD,
            }.items() if not v
        ]
        raise SystemExit(f"Missing environment variables in .env: {', '.join(missing)}")
    return get_connection()


def split_table_name(table):
    schema, name = 'dbo', table
    if '.' in table:
        schema, name = (part.strip(' []') for part in table.split('.', 1))
    return schema, name


def describe_table(cursor, schema, name):
    """Return (ordered column names, identity column set, type metadata)."""
    cursor.execute("""
        SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
               c.NUMERIC_PRECISION, c.NUMERIC_SCALE,
               COLUMNPROPERTY(
                   OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)),
                   c.COLUMN_NAME, 'IsIdentity'
               ) AS IS_IDENTITY
        FROM INFORMATION_SCHEMA.COLUMNS AS c
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
    """, schema, name)
    rows = cursor.fetchall()
    if not rows:
        raise SystemExit(f"Table not found: [{schema}].[{name}]")
    columns = [r[0] for r in rows]
    identities = {r[0] for r in rows if r[5] == 1}
    meta = {r[0]: {'type': r[1], 'length': r[2], 'precision': r[3], 'scale': r[4]}
            for r in rows}
    return columns, identities, meta


def input_size_for(info):
    """Map one SQL column's metadata to a pyodbc setinputsizes entry."""
    data_type = (info['type'] or '').lower()
    if data_type in ('decimal', 'numeric'):
        return (pyodbc.SQL_DECIMAL, info['precision'] or 18, info['scale'] or 2)
    if data_type in ('int', 'bigint', 'smallint', 'tinyint'):
        return (pyodbc.SQL_INTEGER, 0, 0)
    if data_type in ('datetime', 'datetime2', 'smalldatetime'):
        return (pyodbc.SQL_TYPE_TIMESTAMP, 0, 0)
    if data_type in ('varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext'):
        length = info['length']
        # -1 is varchar(max); pyodbc wants 0 for "no limit".
        return (pyodbc.SQL_WVARCHAR, 0 if length in (None, -1) else length, 0)
    return None


def load_to_sql(df, replace=False):
    """Insert the line-item rows into TABLE. Append-only unless replace=True."""
    schema, name = split_table_name(TABLE)
    qualified = f"[{schema}].[{name}]"

    conn = connect()
    cursor = conn.cursor()
    try:
        sql_columns, identities, meta = describe_table(cursor, schema, name)
        insert_columns = [c for c in sql_columns if c not in identities]

        missing = [c for c in LINE_ITEM_COLUMNS if c not in insert_columns]
        if missing:
            raise SystemExit(
                f"{qualified} is missing expected column(s): {', '.join(missing)}"
            )
        unexpected = [c for c in insert_columns
                      if c not in LINE_ITEM_COLUMNS and c != 'date_inserted']
        if unexpected:
            print(f"  Note: {', '.join(unexpected)} will be left to its column default")
            insert_columns = [c for c in insert_columns if c not in unexpected]

        out = df.copy()
        out['date_inserted'] = datetime.now(NY_TZ).strftime('%Y-%m-%d %H:%M:%S')
        out = out[insert_columns]

        if replace:
            refs = sorted({r for r in df['ref_number'].tolist() if r})
            if refs:
                placeholders = ','.join(['?'] * len(refs))
                cursor.execute(
                    f"SELECT COUNT(*) FROM {qualified} WHERE [ref_number] IN ({placeholders})",
                    *refs
                )
                doomed = cursor.fetchone()[0]
                print(f"--replace: deleting {doomed:,} existing row(s) for "
                      f"{len(refs)} ref_number(s)")
                cursor.execute(
                    f"DELETE FROM {qualified} WHERE [ref_number] IN ({placeholders})",
                    *refs
                )

        insert_sql = (
            f"INSERT INTO {qualified} "
            f"({','.join(f'[{c}]' for c in insert_columns)}) "
            f"VALUES ({','.join(['?'] * len(insert_columns))})"
        )

        data = out.astype(object).where(out.notnull(), None).values.tolist()
        if not data:
            print("Nothing to insert.")
            conn.rollback()
            return 0

        cursor.fast_executemany = True
        sizes = [input_size_for(meta[c]) for c in insert_columns]
        cursor.setinputsizes(sizes)

        print(f"Inserting {len(data):,} row(s) into {qualified}...")
        try:
            cursor.executemany(insert_sql, data)
        except (pyodbc.DataError, pyodbc.ProgrammingError) as e:
            conn.rollback()
            print(f"Bulk insert failed: {e}")
            print("Retrying row by row to find the offending record...")
            cursor.fast_executemany = False
            cursor.setinputsizes(sizes)
            for i, row in enumerate(data):
                try:
                    cursor.execute(insert_sql, row)
                except (pyodbc.DataError, pyodbc.ProgrammingError) as row_error:
                    print(f"--- Row {i + 1} rejected ---")
                    print(f"Data: {dict(zip(insert_columns, row))}")
                    print(f"Error: {row_error}")
                    conn.rollback()
                    raise SystemExit(1)
            conn.commit()
            print(f"Inserted {len(data):,} row(s) one at a time.")
            return len(data)

        conn.commit()
        print(f"Successfully inserted {len(data):,} row(s) into {qualified}.")
        return len(data)
    finally:
        cursor.close()
        conn.close()


# ==================================
# Main
# ==================================
def main(argv=None):
    args = parse_args(argv)
    if args.order_level_csv:
        args.no_load = True
        args.csv = True
    if args.no_load:
        args.csv = True

    centers = resolve_centers(args.center_id, all_centers=args.all_centers)
    if not centers:
        raise SystemExit(
            f"No center to query. Pass --center-id, add centers to "
            f"{CENTERS_FILE}, or set ZENOTI_CENTER_ID in the .env."
        )

    if args.list_centers:
        print(f"{len(centers)} center(s) from "
              f"{CENTERS_FILE if not args.center_id else 'the command line'}:")
        for i, (center_id, center_name) in enumerate(centers, 1):
            print(f"  {i:>2}. {center_id}  {center_name}")
        return 0

    start, end = resolve_range(args)
    session = build_session()

    criteria_list = args.date_criteria
    if len(criteria_list) > 1:
        criteria_desc = "all (" + ", ".join(
            f"{c}={CRITERIA_LABELS[c]}" for c in criteria_list
        ) + ")"
    else:
        only = criteria_list[0]
        criteria_desc = f"{only} ({CRITERIA_LABELS[only]})"

    print(f"\nEndpoint : {ENDPOINT}")
    print(f"Centers  : {len(centers)}")
    print(f"Range    : {start} to {end}")
    print(f"Filter   : date_criteria={criteria_desc}")
    print(f"Target   : {'(none - CSV only)' if args.no_load else TABLE}")

    all_records, all_raw = [], []
    failures = []
    per_center = []

    for i, (center_id, center_name) in enumerate(centers, 1):
        label = f"{center_name} ({center_id})" if center_name else center_id
        print(f"\n[{i}/{len(centers)}] {label}")
        center_records = []
        try:
            for window_start, window_end in split_range(start, end):
                if window_start != start or window_end != end:
                    print(f"  window {window_start} to {window_end}")
                for criteria in criteria_list:
                    if len(criteria_list) > 1:
                        print(f"    date_criteria={criteria} "
                              f"({CRITERIA_LABELS[criteria]})")
                    rows, raw_pages = fetch_window(
                        session, center_id, window_start, window_end,
                        collect_raw=args.raw_json,
                        date_criteria=criteria
                    )
                    # Stamp the request parameters on each row; the response does
                    # not echo which center or window produced it.
                    for row in rows:
                        if isinstance(row, dict):
                            row.setdefault('requested_center_id', center_id)
                            row.setdefault('requested_center_name', center_name)
                            row.setdefault('requested_start_date', window_start.isoformat())
                            row.setdefault('requested_end_date', window_end.isoformat())
                    center_records.extend(rows)
                    all_raw.extend(raw_pages)
        except SystemExit as e:
            # One bad center should not have to sink a 19-center run.
            if not args.continue_on_error:
                raise
            print(f"  FAILED: {e}")
            failures.append((label, str(e)))
            per_center.append((label, None))
            continue

        # Fetching several date criteria returns the same order more than once
        # (e.g. raised in one window, completed in another). Keep the first copy
        # of each, matched on order_number, so it is neither loaded nor counted
        # twice. Orders with no order_number can't be matched, so they're kept.
        if len(criteria_list) > 1:
            unique, seen = [], set()
            for record in center_records:
                number = record.get('order_number') if isinstance(record, dict) else None
                if number is not None and number in seen:
                    continue
                if number is not None:
                    seen.add(number)
                unique.append(record)
            duplicates = len(center_records) - len(unique)
            if duplicates:
                print(f"  de-duplicated {duplicates:,} order(s) seen under "
                      f"more than one date_criteria")
            center_records = unique

        all_records.extend(center_records)
        per_center.append((label, len(center_records)))
        print(f"  -> {len(center_records):,} order(s)")

    if len(centers) > 1:
        print(f"\n{'-' * 60}")
        print(f"{'Center':44} {'Orders':>8}")
        print(f"{'-' * 60}")
        for label, count in per_center:
            print(f"{label[:44]:44} {'FAILED' if count is None else f'{count:,}':>8}")
        print(f"{'-' * 60}")
        print(f"{'TOTAL':44} {len(all_records):>8,}")

    if failures:
        print(f"\n{len(failures)} center(s) failed:")
        for label, message in failures:
            print(f"  {label}: {message[:200]}")

    print(f"\nOrders fetched: {len(all_records):,}")
    if not all_records:
        print("The API returned no purchase orders for this request. Nothing to do.")
        return 0

    if args.order_level_csv:
        df = to_order_dataframe(all_records)
        stem = f"purchase_orders_{start}_to_{end}"
    else:
        df = flatten_line_items(all_records, keep_empty=args.keep_empty_lines)
        stem = f"purchase_orders_{start}_to_{end}_line_items"
        print(f"Line-item rows: {len(df):,} across "
              f"{df['ref_number'].nunique() if len(df) else 0} order(s)")
        if not args.keep_empty_lines:
            print("  (all-zero-quantity roster padding dropped; "
                  "--keep-empty-lines keeps it)")
        if len(df):
            print(f"  quantity {int((df.retail_received + df.consumable_received).sum()):,}"
                  f"  value {df.total_price_usd.sum():,.2f}")

    if df.empty:
        print("No rows to load after flattening.")
        return 0

    if args.dry_run:
        print("\n--dry-run: nothing written, nothing inserted.")
        with pd.option_context('display.width', 250, 'display.max_columns', 30):
            print(df.head(15).to_string(index=False))
        return 0

    if args.csv:
        os.makedirs(args.output_dir, exist_ok=True)
        csv_path = os.path.join(args.output_dir, f"{stem}.csv")
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"Wrote {len(df):,} rows -> {csv_path}")

    if args.raw_json:
        os.makedirs(args.output_dir, exist_ok=True)
        json_path = os.path.join(args.output_dir, f"{stem}.json")
        with open(json_path, 'w', encoding='utf-8') as fh:
            json.dump(all_raw, fh, indent=2, default=str)
        print(f"Wrote raw response -> {json_path}")

    if not args.no_load:
        load_to_sql(df, replace=args.replace)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
