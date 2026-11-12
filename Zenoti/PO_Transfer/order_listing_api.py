"""Export Zenoti orders in the Orders Listing CSV shape, without connecting to SQL.

Examples (run from the repository root):
  python Zenoti/order_listing_api.py --start-date 2026-11-06 --end-date 2026-11-11
  python Zenoti/order_listing_api.py --compare-csv Zenoti/old/orders_2026-11-10.csv --date-criteria 4
  python Zenoti/order_listing_api.py --from-json Zenoti/old/api_order_comparison/api_raised_orders.json --compare-csv Zenoti/old/orders_2026-11-10.csv

Default date criteria are ordered, raised, delivered and completed together, so
unraised CREATED orders and recent deliveries of older orders are included.
Repeated orders are consolidated before quantities or amounts are calculated.

The column mapping was checked against orders_2026-11-10.csv. That export uses
ordered_date for "Order on", delivered quantities and prices for Qty/Value,
ordered quantities and prices for CREATED orders, partial notes for "PO notes",
and calendar days for delivered purchase-order wait time. Date criterion 4
matches the CSV's order-date selection and includes CREATED orders such as PO5008.

These date-filtered exports contain updates, rather than a complete table
snapshot. The CSV loader's --prune option requires complete snapshot coverage
and must stay off when importing these files.
"""

import argparse
import csv
import hashlib
import json
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

import pandas as pd

if __package__:
    from . import api_client
else:
    import api_client

purchase_order = api_client.PURCHASE
transfer_order = api_client.TRANSFER


CSV_COLUMNS = (
    'REF #', 'Order on', 'Deliver on', 'To', 'From', 'Status', 'Qty',
    'Value', 'Tax', 'PO notes', 'Wait time',
)
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / 'data' / 'api_order_exports'
MONEY_PRECISION = Decimal('0.01')
CRITERIA_LABELS = {1: 'raised', 2: 'delivered', 3: 'completed', 4: 'ordered'}


def parse_date_criteria(value) -> list[int]:
    text = str(value).strip().lower()
    if text in ('all', '0'):
        return [1, 2, 3, 4]
    try:
        criterion = int(text)
    except ValueError as error:
        raise argparse.ArgumentTypeError('date-criteria must be 1, 2, 3, 4, or all') from error
    if criterion not in CRITERIA_LABELS:
        raise argparse.ArgumentTypeError('date-criteria must be 1, 2, 3, 4, or all')
    return [criterion]


def reference(value):
    """Match the CSV's unprefixed numeric reference without changing its value."""
    text = str(value if value is not None else '').strip()
    if text.upper().startswith(('PO', 'TO')) and text[2:].strip().isdigit():
        text = text[2:].strip()
    return text


def decimal(value):
    if value in (None, ''):
        return Decimal(0)
    try:
        result = Decimal(str(value).replace(',', '').replace('$', '').strip())
    except InvalidOperation as error:
        raise ValueError(f'Invalid numeric value: {value!r}') from error
    if not result.is_finite():
        raise ValueError(f'Non-finite numeric value: {value!r}')
    return result


def timestamp(value) -> datetime | None:
    if value in (None, ''):
        return None
    result = pd.to_datetime(str(value).strip(), format='mixed', errors='raise')
    if result.tzinfo is not None:
        result = result.tz_convert(purchase_order.NY_TZ).tz_localize(None)
    return result.to_pydatetime()


def csv_date(value):
    return value.strftime('%m/%d/%Y %H:%M') if value is not None else ''


def money(value):
    return format(value.quantize(MONEY_PRECISION, rounding=ROUND_HALF_UP), '.2f')


def read_csv(path):
    with Path(path).open(newline='', encoding='utf-8-sig') as handle:
        reader = csv.DictReader(handle)
        missing = set(CSV_COLUMNS) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f'{path} is missing column(s): {", ".join(sorted(missing))}')
        return list(reader)


def unique_rows(rows):
    result = {}
    for row in rows:
        ref = reference(row['REF #'])
        if not ref or ref in result:
            raise ValueError(f'Blank or repeated CSV reference: {ref!r}')
        result[ref] = row
    return result


def latest_delivery(order) -> datetime | None:
    dates: list[datetime] = []
    for partial in order.get('partials') or []:
        if not partial.get('delivered_date'):
            continue
        delivered = timestamp(partial['delivered_date'])
        if delivered is not None:
            dates.append(delivered)
    return max(dates, default=None)


def deduplicate(records):
    """Select one complete response per reference; never add duplicate shipments."""
    by_ref = {}
    for record in records:
        order = record['order']
        ref = reference(order.get('order_number'))
        if not ref:
            raise ValueError('An API order has no order_number; export stopped.')
        previous = by_ref.get(ref)
        if previous:
            previous_order = previous['order']
            if (record['source'] != previous['source']
                    or order.get('order_id') != previous_order.get('order_id')):
                raise ValueError(f'Reference {ref} identifies different API orders.')
            # Several criteria/windows can return the same order. Prefer a copy
            # with a later delivery if the order changed during the fetch.
            status_rank = {'CREATED': 0, 'RAISED': 1, 'PARTIALLY DELIVERED': 2,
                           'DELIVERED': 3, 'COMPLETED': 4}
            def version(value):
                return (latest_delivery(value) or datetime.min,
                        timestamp(value.get('closed_date')) or datetime.min,
                        status_rank.get(str(value.get('status') or '').upper(), -1))
            if version(order) <= version(previous_order):
                continue
        by_ref[ref] = record
    return by_ref


def order_row(record):
    order = record['order']
    partials = order.get('partials') or []
    items = [item for partial in partials for item in partial.get('line_items') or []]
    ordered = timestamp(order.get('ordered_date') or order.get('raised_date'))
    delivered = latest_delivery(order)
    status = str(order.get('status') or '').strip().upper()
    quantity_prefix = 'ordered' if status == 'CREATED' else 'delivered'
    quantity = total_value = total_tax = Decimal(0)
    for item in items:
        qty = (decimal(item.get(f'{quantity_prefix}_retail_quantity'))
               + decimal(item.get(f'{quantity_prefix}_consumable_quantity')))
        quantity += qty
        # delivered_unit_price already incorporates line-item discounts in the
        # verified export; applying delivered_discount_value again is incorrect.
        total_value += qty * decimal(item.get(f'{quantity_prefix}_unit_price'))
        total_tax += decimal(item.get(f'{quantity_prefix}_tax_amount'))
    if quantity != quantity.to_integral_value():
        raise ValueError(f'Order {order.get("order_number")} has a fractional Qty.')
    notes = list(dict.fromkeys(str(partial.get('notes') or '').strip()
                              for partial in partials
                              if str(partial.get('notes') or '').strip()))
    wait_time = ''
    if record['source'] == 'purchase' and status == 'DELIVERED' and ordered and delivered:
        wait_time = f'{(delivered.date() - ordered.date()).days} day(s)'
    return {
        'REF #': reference(order.get('order_number')),
        'Order on': csv_date(ordered),
        'Deliver on': csv_date(delivered),
        'To': str(order.get('center_name') or record.get('requested_center') or '').strip(),
        'From': str(order.get('vendor_name') or '').strip(),
        'Status': status,
        'Qty': str(int(quantity)),
        'Value': money(total_value),
        'Tax': money(total_tax),
        'PO notes': '\n'.join(notes),
        'Wait time': wait_time,
    }


def normalized(column, value):
    if column in ('Order on', 'Deliver on'):
        parsed = timestamp(value)
        return parsed.strftime('%Y-%m-%d %H:%M') if parsed is not None else ''
    if column in ('Qty', 'Value', 'Tax'):
        return decimal(value)
    if column == 'REF #':
        return reference(value)
    return str(value or '').strip().replace('\r\n', '\n')


def compare(baseline_rows, api_rows):
    baseline, current = unique_rows(baseline_rows), unique_rows(api_rows)
    shared = sorted(set(baseline) & set(current))
    stats = {column: {'matches': 0, 'differences': 0} for column in CSV_COLUMNS}
    differences, changed_refs = [], set()
    for ref in shared:
        for column in CSV_COLUMNS:
            before, after = baseline[ref][column], current[ref][column]
            equal = normalized(column, before) == normalized(column, after)
            stats[column]['matches' if equal else 'differences'] += 1
            if not equal:
                changed_refs.add(ref)
                differences.append({'REF #': ref, 'Column': column, 'CSV': before, 'API': after})
    missing = sorted(set(baseline) - set(current))
    return {
        'csv_rows': len(baseline), 'api_rows': len(current), 'matched_refs': len(shared),
        'fully_matching_rows': len(shared) - len(changed_refs),
        'changed_refs': sorted(changed_refs), 'missing_from_api': missing,
        'extra_api_refs': sorted(set(current) - set(baseline)),
        'column_comparison': stats, 'differences': differences,
        'missing_rows': [baseline[ref] for ref in missing],
    }


def fetch_job(job):
    source, module, center, start, end, criterion = job
    center_id, center_name = center
    rows, previous_signature = [], None
    with module.build_session() as session:
        for page in range(1, module.MAX_PAGES + 1):
            params = {'center_id': center_id, 'start_date': str(start), 'end_date': str(end),
                      'date_criteria': criterion, 'page': page, 'size': module.PAGE_SIZE}
            params['status'] = -1
            if source == 'transfer':
                params['show_delivery_details'] = 'true'
            payload = module.request_with_retry(session, params)
            page_rows = module.extract_rows(payload)
            if isinstance(payload, dict) and (payload.get('error') or payload.get('errors')):
                raise ValueError(f'Zenoti reported an error for {source}, {center_name}.')
            if not page_rows:
                return rows
            signature = json.dumps(page_rows, sort_keys=True, default=str)
            if signature == previous_signature:
                raise ValueError(f'Pagination repeated for {source}, {center_name}; export stopped.')
            previous_signature = signature
            rows.extend({'source': source, 'requested_center': center_name,
                         'date_criteria': criterion, 'order': order} for order in page_rows)
            total = payload.get('total_orders') if isinstance(payload, dict) else None
            if len(page_rows) < module.PAGE_SIZE or (total is not None and len(rows) >= int(total)):
                return rows
    raise ValueError(f'Pagination limit reached for {source}, {center_name}; export stopped.')


def job_cache_key(job):
    source, module, center, start, end, criterion = job
    return {
        'source': source, 'endpoint': module.ENDPOINT, 'center_id': center[0],
        'start_date': str(start), 'end_date': str(end), 'date_criteria': criterion,
        'page_size': module.PAGE_SIZE,
    }


def fetch_cached_job(job, cache_dir=None, resume=False):
    """Checkpoint a complete window; incomplete requests are never cached."""
    key = job_cache_key(job)
    cache_path = None
    if cache_dir is not None:
        digest = hashlib.sha256(json.dumps(key, sort_keys=True).encode('utf-8')).hexdigest()
        cache_path = cache_dir / f'{digest}.json'
        if resume and cache_path.exists():
            saved = json.loads(cache_path.read_text(encoding='utf-8'))
            if saved.get('job') != key or not isinstance(saved.get('records'), list):
                raise ValueError(f'Invalid request checkpoint: {cache_path}')
            return saved['records']
    # The existing HTTP helper retries individual pages. Retry the whole window
    # as well for rate limits/connectivity, since a historical pull is long.
    records = []
    for attempt in range(1, 4):
        try:
            records = fetch_job(job)
            break
        except (Exception, SystemExit) as error:
            message = str(error).lower()
            transient = any(term in message for term in
                            ('http 429', 'http 500', 'http 502', 'http 503',
                             'http 504', 'timeout', 'connectionerror'))
            if not transient or attempt == 3:
                raise
            delay = 30 * attempt
            reason = 'quota' if 'http 429' in message else 'transient error'
            criterion = CRITERIA_LABELS.get(job[5], str(job[5]))
            center = job[2][1] or 'configured center'
            print(f'  API      {reason}: {job[0]} | {center} | {criterion} '
                  f'| retry {attempt + 1}/3 in {delay}s', flush=True)
            time.sleep(delay)
    if cache_path is not None:
        temporary = cache_path.with_suffix('.tmp')
        temporary.write_text(json.dumps({'job': key, 'records': records}, default=str),
                             encoding='utf-8')
        temporary.replace(cache_path)
    return records


def fetch(start, end, criteria, centers, *, cache_dir=None, resume=False, workers=3):
    if workers < 1:
        raise ValueError('workers must be at least 1.')
    if cache_dir is not None:
        cache_dir = Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
    jobs = [(source, module, center, lo, hi, criterion)
            for source, module in (('purchase', purchase_order), ('transfer', transfer_order))
            for center in centers
            for lo, hi in purchase_order.split_range(start, end)
            for criterion in criteria]
    records, failures = [], []
    print(f'  API      {len(jobs):,} requests across {len(centers)} centers '
          f'| {api_client.MIN_REQUEST_INTERVAL_SECONDS:.1f}s minimum gap', flush=True)
    progress_every = max(1, (len(jobs) + 3) // 4)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_cached_job, job, cache_dir, resume): job for job in jobs}
        for count, future in enumerate(as_completed(futures), 1):
            job = futures[future]
            try:
                records.extend(future.result())
            except (Exception, SystemExit) as error:
                failure = (f'{job[0]}, {job[2][1]}, {job[3]}..{job[4]}, '
                           f'criterion {job[5]}: {error}')
                failures.append(failure)
                print(f'  Failed: {failure}', flush=True)
            if count % progress_every == 0 or count == len(jobs):
                print(f'  API      {count:,}/{len(jobs):,} requests complete', flush=True)
    if failures:
        raise ValueError('Fetch incomplete; no export written. Complete windows are '
                         'checkpointed when --cache-dir is used.\n' + '\n'.join(failures))
    return {'start_date': str(start), 'end_date': str(end), 'date_criteria': criteria,
            'fetched_at': datetime.now(purchase_order.NY_TZ).isoformat(),
            'records': records, 'failures': [], 'completed_jobs': len(jobs),
            'centers': [{'center_id': center_id, 'center_name': center_name}
                        for center_id, center_name in centers], 'resumed': resume}


def write_csv(path, columns, rows):
    with path.open('w', newline='', encoding='utf-8-sig') as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--start-date', type=lambda value: purchase_order.parse_ymd(value, '--start-date'))
    parser.add_argument('--end-date', type=lambda value: purchase_order.parse_ymd(value, '--end-date'))
    parser.add_argument('--date-criteria', type=parse_date_criteria, default='all', metavar='{1,2,3,4,all}',
                        help='1=raised, 2=delivered, 3=completed, 4=ordered; all includes all four.')
    parser.add_argument('--center-id', action='append', default=[])
    parser.add_argument('--compare-csv', type=Path, help='Compare every exported field with this existing CSV.')
    parser.add_argument('--from-json', type=Path, help='Replay a saved API snapshot; makes no API requests.')
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument('--cache-dir', type=Path, help='Save each completed API window for resuming a long pull.')
    parser.add_argument('--resume', action='store_true', help='Reuse complete windows in --cache-dir after an interrupted pull.')
    parser.add_argument('--workers', type=int, default=3, help='Concurrent API requests (default: 3).')
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    baseline = read_csv(args.compare_csv) if args.compare_csv else None
    if args.from_json:
        snapshot = json.loads(args.from_json.read_text(encoding='utf-8'))
        if snapshot.get('failures'):
            raise ValueError('The saved API snapshot contains failed requests.')
    else:
        dates: list[date] = []
        for row in baseline or []:
            if not row['Order on'].strip():
                continue
            ordered = timestamp(row['Order on'])
            if ordered is not None:
                dates.append(ordered.date())
        start = args.start_date or min(dates, default=None)
        end = args.end_date or max(dates, default=None)
        if start is None or end is None:
            raise ValueError('Supply --start-date and --end-date, or --compare-csv.')
        if end < start:
            raise ValueError('--end-date is before --start-date.')
        centers = purchase_order.resolve_centers(args.center_id, all_centers=not args.center_id)
        if not centers:
            raise ValueError('No Zenoti centers are configured.')
        if args.resume and args.cache_dir is None:
            raise ValueError('--resume requires --cache-dir.')
        snapshot = fetch(start, end, args.date_criteria, centers, cache_dir=args.cache_dir,
                         resume=args.resume, workers=args.workers)
    records = snapshot['records']
    selected = deduplicate(records)
    rows = [order_row(record) for record in selected.values()]
    rows.sort(key=lambda row: (timestamp(row['Order on']) or datetime.min, row['REF #']), reverse=True)
    report = compare(baseline, rows) if baseline is not None else None
    args.output_dir.mkdir(parents=True, exist_ok=True)
    stem = f'orders_{snapshot["start_date"]}_to_{snapshot["end_date"]}'
    csv_path = args.output_dir / f'{stem}.csv'
    raw_path = args.output_dir / f'{stem}_api.json'
    write_csv(csv_path, CSV_COLUMNS, rows)
    raw_path.write_text(json.dumps(snapshot, indent=2, default=str), encoding='utf-8')
    print(f'Wrote {len(rows)} orders: {csv_path}')
    print(f'Sources: {dict(Counter(record["source"] for record in selected.values()))}')
    print(f'Raw snapshot: {raw_path}')
    if report is not None:
        report['source_csv'] = str(args.compare_csv)
        report['api_snapshot'] = str(raw_path)
        report_path = args.output_dir / f'{stem}_comparison.json'
        report_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
        write_csv(args.output_dir / f'{stem}_differences.csv', ('REF #', 'Column', 'CSV', 'API'), report['differences'])
        print(f'CSV references found: {report["matched_refs"]}/{report["csv_rows"]}; '
              f'all fields match: {report["fully_matching_rows"]}; changed: {report["changed_refs"]}; '
              f'missing: {report["missing_from_api"]}')
        print(f'Comparison: {report_path}')
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, argparse.ArgumentTypeError) as error:
        raise SystemExit(str(error))
