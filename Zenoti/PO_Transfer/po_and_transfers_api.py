"""Fetch purchase/transfer orders and load the summary and optional product details.

This is one API-to-SQL entry point: no browser download or Google Drive is
needed. It uses the verified Orders Listing mapping in order_listing_api.py.
The local default is TABLE_PO_AND_TRANSFER_TEST, summary only. run_all.py selects
TABLE_PO_AND_TRANSFERS and both product-detail outputs for the Railway service.

Examples (from the repository root):
  python Zenoti/PO_Transfer/run_all.py --dry-run
  python Zenoti/PO_Transfer/run_all.py
  python Zenoti/PO_Transfer/run_all.py --table-env TABLE_PO_AND_TRANSFER_TEST --summary-only
  python Zenoti/PO_Transfer/run_all.py --start-date 2025-01-01 --end-date 2026-11-11

Default selection includes ordered, raised, delivered and completed dates.
This captures new orders and later deliveries of older orders. It is an update
feed: existing rows absent from the pull are retained. IDs survive updates,
and the whole SQL batch is committed only after its values are verified.
With no date parameters the inclusive window is the last 31 completed days in
America/New_York. CLI dates override PO_AND_TRANSFERS_START_DATE/END_DATE or
START_DATE/END_DATE variables. API JSON, CSV and checkpoints are saved locally.
--resume is for interrupted pulls; omit it when fetching fresh updates.
"""

import argparse
import json
import os
import shutil
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pyodbc

if __package__:
    from . import order_listing_api as listing
    from . import detail_sql, purchase_order as purchase_details, transfer_order as transfer_details
    from .api_client import PURCHASE as purchase_order
    from .db_helper import build_conn_str, get_connection
else:
    import order_listing_api as listing
    import detail_sql
    import purchase_order as purchase_details
    import transfer_order as transfer_details
    from api_client import PURCHASE as purchase_order
    from db_helper import build_conn_str, get_connection


TABLE_ENV = 'TABLE_PO_AND_TRANSFER_TEST'
OUTPUT_DIR = Path(__file__).resolve().parent / 'data' / 'api_order_ingestion'
FIELD_MAP = {
    'REF #': 'ref_no', 'Order on': 'order_on', 'Deliver on': 'deliver_on',
    'To': 'order_to', 'From': 'order_from', 'Status': 'order_status',
    'Qty': 'order_qty', 'Value': 'order_value', 'Tax': 'order_tax',
    'PO notes': 'order_ponotes', 'Wait time': 'wait_time',
}
LOAD_COLUMNS = tuple(FIELD_MAP.values()) + ('date_inserted',)
INTEGER_LIMITS = {
    'tinyint': (0, 255), 'smallint': (-32768, 32767),
    'int': (-2147483648, 2147483647),
    'bigint': (-9223372036854775808, 9223372036854775807),
}
TEXT_SQL_TYPES = {'char', 'varchar', 'nchar', 'nvarchar'}


def quote_identifier(value):
    return '[' + value.replace(']', ']]') + ']'


def target_table(table_env=TABLE_ENV, *, default=None):
    value = (os.environ.get(table_env) or default or '').strip()
    if not value:
        raise ValueError(f'{table_env} must name a SQL destination table.')
    schema, name = purchase_order.split_table_name(value)
    schema, name = schema.strip(' []'), name.strip(' []')
    if not schema or not name:
        raise ValueError(f'Invalid table name in {table_env}.')
    return quote_identifier(schema) + '.' + quote_identifier(name)


def describe_target(cursor, target):
    cursor.execute('''
        SELECT c.name, TYPE_NAME(c.user_type_id) AS data_type,
               CASE WHEN TYPE_NAME(c.user_type_id) IN ('nvarchar', 'nchar')
                         AND c.max_length <> -1 THEN c.max_length / 2
                    ELSE c.max_length END AS length,
               c.precision, c.scale, c.is_nullable, c.is_identity,
               c.is_computed, c.default_object_id
        FROM sys.columns AS c WHERE c.object_id = OBJECT_ID(?)
        ORDER BY c.column_id
    ''', target)
    names = [column[0] for column in cursor.description or []]
    metadata = {row[0].lower(): dict(zip(names, row)) for row in cursor.fetchall()}
    if not metadata:
        raise ValueError(f'Table not found: {target}')
    missing = [column for column in LOAD_COLUMNS if column not in metadata]
    if missing:
        raise ValueError(f'{target} is missing column(s): {", ".join(missing)}')
    for column in LOAD_COLUMNS:
        info = metadata[column]
        if info['is_identity'] or info['is_computed']:
            raise ValueError(f'{target}.{info["name"]} must be writable.')
    if metadata['ref_no']['data_type'] not in INTEGER_LIMITS and \
            metadata['ref_no']['data_type'] not in TEXT_SQL_TYPES:
        raise ValueError(f'{target}.ref_no must be an integer or character business key.')
    for column, info in metadata.items():
        if column in LOAD_COLUMNS or info['is_identity'] or info['is_computed']:
            continue
        if column == 'id':
            if info['data_type'] not in INTEGER_LIMITS:
                raise ValueError(f'{target}.id must be an integer or an identity column.')
        elif not info['is_nullable'] and not info['default_object_id']:
            raise ValueError(f'{target}.{info["name"]} needs a default or must allow NULL.')
    return metadata


def _fetch_sql_integer(cursor: pyodbc.Cursor) -> int:
    """Require the scalar result from a count or maximum-ID query."""
    row = cursor.fetchone()
    if row is None:
        raise ValueError('SQL aggregate query returned no row.')
    return int(row[0])


def inspect_target(target):
    connection = pyodbc.connect(build_conn_str(), timeout=15, readonly=True)
    connection.timeout = 60
    try:
        cursor = connection.cursor()
        try:
            metadata = describe_target(cursor, target)
        except ValueError as error:
            if str(error).startswith('Table not found:'):
                raise ValueError(
                    'Summary SQL destination was not found or is not accessible.') from error
            raise
        cursor.execute(f'SELECT COUNT_BIG(*) FROM {target}')
        count = _fetch_sql_integer(cursor)
        print(f'  SQL      summary ready | {count:,} existing rows '
              f'| {len(LOAD_COLUMNS)} fields', flush=True)
        info = metadata.get('id')
        if info and not info['is_identity'] and not info['is_computed']:
            print('  SQL      IDs preserved on update; new IDs allocated safely', flush=True)
        return {'target': target, 'existing_rows': count, 'columns': metadata}
    finally:
        connection.close()


def detail_targets(args):
    if not args.include_details:
        return {}
    if args.table_env != 'TABLE_PO_AND_TRANSFERS' and (
            args.purchase_table_env == 'TABLE_PURCHASE_ORDER'
            or args.transfer_table_env == 'TABLE_TRANSFER_ORDER'):
        raise ValueError('For the existing summary TEST table, use --summary-only. '
                         'To test all three tables, select separate test destinations '
                         'with --purchase-table-env and --transfer-table-env.')
    return {
        'purchase': target_table(args.purchase_table_env, default=(
            'dbo.BRONZE_ZENOTI_PURCHASE_ORDER'
            if args.purchase_table_env == 'TABLE_PURCHASE_ORDER' else None)),
        'transfer': target_table(args.transfer_table_env, default=(
            'dbo.BRONZE_ZENOTI_TRANSFER_ORDERS'
            if args.transfer_table_env == 'TABLE_TRANSFER_ORDER' else None)),
    }


def inspect_details(targets):
    if not targets:
        return
    connection = pyodbc.connect(build_conn_str(), timeout=15, readonly=True)
    connection.timeout = 60
    cursor = connection.cursor()
    try:
        for source, module in (('purchase', purchase_details), ('transfer', transfer_details)):
            target = targets[source]
            try:
                detail_sql.inspect_target(cursor, target, module.LINE_ITEM_COLUMNS)
            except ValueError as error:
                if str(error).startswith('Table not found:'):
                    raise ValueError(
                        f'{source.title()} detail SQL destination was not found or '
                        'is not accessible.') from error
                raise
            print(f'  SQL      {source} details ready | '
                  f'{len(module.LINE_ITEM_COLUMNS)} fields', flush=True)
    finally:
        cursor.close()
        connection.close()


def prepare_details(records):
    """Use the existing product mappings on the same deduplicated API responses."""
    grouped = {'purchase': [], 'transfer': []}
    for record in records:
        source = record['source']
        if source not in grouped:
            raise ValueError(f'Unknown API order source: {source!r}')
        order = dict(record['order'])
        order.setdefault('requested_center_name', record.get('requested_center') or '')
        grouped[source].append(order)
    result = {}
    for source, module in (('purchase', purchase_details), ('transfer', transfer_details)):
        orders = grouped[source]
        result[source] = {
            'rows': module.flatten_line_items(orders),
            'references': sorted({str(order['order_number']).strip() for order in orders}),
        }
    return result


def load_all_tables(rows, summary_target, details, targets):
    """Commit the summary and both detail replacements as one SQL transaction."""
    connection = get_connection()
    try:
        report = upsert_rows(rows, summary_target, connection=connection, commit=False)
        outputs = {}
        for source, module in (('purchase', purchase_details), ('transfer', transfer_details)):
            data = details[source]
            count = module.load_to_sql(
                data['rows'], replace=True, table=targets[source], connection=connection,
                commit=False, references=data['references'],
            )
            outputs[source] = {'target': targets[source], 'input_orders': len(data['references']),
                               'line_item_rows': count}
        connection.commit()
        report.update({'committed': True, 'details': outputs})
        print(f'  SQL      committed: {report["inserted"]:,} inserted, '
              f'{report["updated"]:,} updated; '
              f'{outputs["purchase"]["line_item_rows"]:,} purchase items, '
              f'{outputs["transfer"]["line_item_rows"]:,} transfer items', flush=True)
        return report
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def sql_rows(csv_rows, inserted_at=None, *, metadata=None):
    """Keep the CSV's verified minute precision, money, notes and blank semantics."""
    stamp = inserted_at or datetime.now(purchase_order.NY_TZ).replace(tzinfo=None)
    sql_types = {column: info['data_type'] for column, info in (metadata or {}).items()}
    if sql_types.get('date_inserted') in TEXT_SQL_TYPES:
        stamp = stamp.strftime('%Y-%m-%d %H:%M:%S')
    result, seen = [], set()
    for row in csv_rows:
        ref = listing.reference(row['REF #'])
        if not ref.isdigit():
            raise ValueError(f'Order reference is not numeric: {ref!r}')
        key = ref if sql_types.get('ref_no') in TEXT_SQL_TYPES else int(ref)
        if key in seen:
            raise ValueError(f'Repeated SQL order reference: {key}')
        seen.add(key)
        values = {'ref_no': key, 'date_inserted': stamp}
        for field, column in FIELD_MAP.items():
            if column == 'ref_no':
                continue
            value = row[field]
            if field in ('Order on', 'Deliver on'):
                parsed = listing.timestamp(value)
                values[column] = (parsed.strftime('%Y-%m-%d %H:%M:%S')
                                  if parsed is not None and sql_types.get(column) in TEXT_SQL_TYPES
                                  else parsed)
            elif field == 'Qty':
                quantity = listing.decimal(value)
                if quantity != quantity.to_integral_value():
                    raise ValueError(f'Order {key} has a fractional Qty.')
                values[column] = int(quantity)
            elif field in ('Value', 'Tax'):
                values[column] = listing.decimal(listing.money(listing.decimal(value)))
            else:
                values[column] = str(value or '').strip() or None
        result.append(tuple(values[column] for column in LOAD_COLUMNS))
    return result


def validate_values(rows, metadata):
    for values in rows:
        for column, value in zip(LOAD_COLUMNS, values):
            info = metadata[column]
            data_type = info['data_type']
            if value is None and not info['is_nullable']:
                raise ValueError(f'Order {values[0]} has NULL in required {column}.')
            if value is None:
                continue
            if data_type in INTEGER_LIMITS:
                minimum, maximum = INTEGER_LIMITS[data_type]
                if not minimum <= value <= maximum:
                    raise ValueError(f'Order {values[0]} exceeds the SQL range for {column}.')
            elif isinstance(value, str) and info['length'] > 0:
                if len(value.encode('utf-16-le')) // 2 > info['length']:
                    raise ValueError(f'Order {values[0]} exceeds the SQL length for {column}.')


def upsert_rows(csv_rows, target, *, connection=None, commit=True):
    """Stage typed values, update matching refs, and allocate IDs for new refs.

    An injected connection and commit=False allow an integration check that
    rolls back afterwards. Normal CLI loads own and close their connection.
    """
    csv_rows = list(csv_rows)
    owns_connection = connection is None
    if connection is None:
        connection = get_connection()
    connection.timeout = 120
    cursor = connection.cursor()
    try:
        cursor.execute('SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;')
        metadata = describe_target(cursor, target)
        rows = sql_rows(csv_rows, metadata=metadata)
        validate_values(rows, metadata)
        columns = [quote_identifier(metadata[column]['name']) for column in LOAD_COLUMNS]
        column_list = ','.join(columns)
        ref = columns[0]
        stage = '#ZenotiOrdersStage'
        cursor.execute(f'SELECT TOP (0) {column_list} INTO {stage} FROM {target}')
        # Bounded multi-row INSERTs avoid both SQL's 2,100-parameter limit and
        # legacy ODBC fast_executemany issues with local temporary tables.
        for offset in range(0, len(rows), 100):
            chunk = rows[offset:offset + 100]
            placeholders = ','.join('(' + ','.join('?' for _ in columns) + ')'
                                    for _ in chunk)
            cursor.execute(f'INSERT INTO {stage} ({column_list}) VALUES {placeholders}',
                           [value for row in chunk for value in row])

        # The test table may have no unique index. Hold the table lock through
        # verification/commit so concurrent runs cannot allocate duplicate IDs.
        cursor.execute(f'SELECT COUNT_BIG(*) FROM {target} WITH (TABLOCKX, HOLDLOCK)')
        before = _fetch_sql_integer(cursor)
        cursor.execute(f'SELECT TOP (1) {ref} FROM {target} GROUP BY {ref} '
                       f'HAVING COUNT_BIG(*) > 1 OR {ref} IS NULL')
        invalid = cursor.fetchone()
        if invalid is not None:
            raise ValueError(f'{target} has a blank or repeated ref_no: {invalid[0]!r}')
        cursor.execute(f'SELECT COUNT_BIG(*) FROM {stage} AS s '
                       f'JOIN {target} AS t ON t.{ref} = s.{ref}')
        updated = _fetch_sql_integer(cursor)
        inserted = len(rows) - updated
        manual_id = metadata.get('id')
        id_expression, id_columns, id_params = '', '', []
        if manual_id and not manual_id['is_identity'] and not manual_id['is_computed']:
            id_column = quote_identifier(manual_id['name'])
            cursor.execute(f'SELECT TOP (1) {id_column} FROM {target} '
                           f'GROUP BY {id_column} HAVING COUNT_BIG(*) > 1')
            if cursor.fetchone() is not None:
                raise ValueError(f'{target} has duplicate IDs; load stopped.')
            cursor.execute(f'SELECT COALESCE(MAX(CAST({id_column} AS bigint)), 0) FROM {target}')
            maximum_id = _fetch_sql_integer(cursor)
            if maximum_id + inserted > INTEGER_LIMITS[manual_id['data_type']][1]:
                raise ValueError(f'{target} cannot allocate {inserted} more IDs.')
            id_columns = id_column + ','
            id_expression = f'? + ROW_NUMBER() OVER (ORDER BY s.{ref}),'
            id_params = [maximum_id]

        if rows:
            assignments = ','.join(f't.{column} = s.{column}' for column in columns[1:])
            cursor.execute(f'UPDATE t SET {assignments} FROM {target} AS t '
                           f'JOIN {stage} AS s ON t.{ref} = s.{ref}')
            select_columns = ','.join(f's.{column}' for column in columns)
            cursor.execute(f'INSERT INTO {target} ({id_columns}{column_list}) '
                           f'SELECT {id_expression}{select_columns} FROM {stage} AS s '
                           f'LEFT JOIN {target} AS t ON t.{ref} = s.{ref} '
                           f'WHERE t.{ref} IS NULL', id_params)

        cursor.execute(f'SELECT COUNT_BIG(*) FROM {target}')
        after = _fetch_sql_integer(cursor)
        cursor.execute(f'SELECT COUNT_BIG(*) FROM (SELECT {column_list} FROM {stage} '
                       f'EXCEPT SELECT {column_list} FROM {target}) AS differences')
        differences = _fetch_sql_integer(cursor)
        if after != before + inserted or differences:
            raise ValueError('SQL verification failed; the entire load will be rolled back.')
        cursor.execute(f'DROP TABLE {stage}')
        if commit:
            connection.commit()
        result = {'target': target, 'input_orders': len(rows), 'inserted': inserted,
                  'updated': updated, 'rows_before': before, 'rows_after': after,
                  'verified_differences': differences, 'committed': commit}
        if commit:
            print(f'  SQL      committed: {inserted:,} inserted, {updated:,} updated; '
                  f'{after:,} total rows', flush=True)
        return result
    except BaseException:
        connection.rollback()
        raise
    finally:
        cursor.close()
        if owns_connection:
            connection.close()


def positive_int(value):
    try:
        number = int(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError('Value must be a positive integer.') from error
    if number < 1:
        raise argparse.ArgumentTypeError('Value must be at least 1.')
    return number


def calendar_timezone(value):
    try:
        return ZoneInfo(value)
    except (ValueError, ZoneInfoNotFoundError) as error:
        raise ValueError(f'Unknown calendar timezone: {value!r}') from error


def parse_calendar_date(value, flag, today: date):
    text = str(value).strip().lower()
    if text == 'yesterday':
        return today - timedelta(days=1)
    if text == 'today':
        return today
    return purchase_order.parse_ymd(value, flag)


def parse_end_date(value):
    zone = calendar_timezone(os.getenv('PO_AND_TRANSFERS_TIMEZONE') or 'America/New_York')
    return parse_calendar_date(value, '--end-date', datetime.now(zone).date())


def resolve_date_range(args, now=None):
    zone = calendar_timezone(args.timezone)
    # Resolve both relative dates from one instant, even if a run crosses midnight.
    current = now if now is not None else datetime.now(zone)
    if current.tzinfo is None:
        raise ValueError('The date resolver requires a timezone-aware instant.')
    today = current.astimezone(zone).date()
    end = (parse_calendar_date(args.end_date, '--end-date', today) if args.end_date
           else today - timedelta(days=1))
    start = (parse_calendar_date(args.start_date, '--start-date', today) if args.start_date
             else end - timedelta(days=args.lookback_days - 1))
    if end < start:
        raise ValueError('--end-date is before --start-date.')
    return start, end


def date_setting(name, fallback):
    return os.getenv(name) or os.getenv(fallback) or None


def running_on_railway(environ=None):
    values = os.environ if environ is None else environ
    return any(values.get(name) for name in
               ('RAILWAY_ENVIRONMENT', 'RAILWAY_PROJECT_ID', 'RAILWAY_SERVICE_ID'))


def validate_railway_window(start_date, end_date):
    """Require every deployed run to cover exactly 31 inclusive calendar days."""
    start = purchase_order.parse_ymd(start_date, 'start date')
    end = purchase_order.parse_ymd(end_date, 'end date')
    days = (end - start).days + 1
    if running_on_railway() and days != 31:
        raise ValueError(
            f'Railway runs require exactly 31 inclusive days; received {days}.')
    return days


def cleanup_run_files(output_dir, start_date, end_date):
    """Remove only this run's generated files and request checkpoints."""
    output_dir = Path(output_dir)
    stem = f'orders_{start_date}_to_{end_date}'
    names = (
        f'{stem}.csv', f'{stem}_api.json', f'{stem}_load.json',
        f'purchase_orders_{start_date}_to_{end_date}_line_items.csv',
        f'transfer_orders_{start_date}_to_{end_date}_line_items.csv',
    )
    removed = 0
    for name in names:
        path = output_dir / name
        if path.is_file():
            path.unlink()
            removed += 1
    requests_dir = output_dir / '_requests'
    if requests_dir.is_dir() and requests_dir.parent.resolve() == output_dir.resolve():
        shutil.rmtree(requests_dir)
        removed += 1
    try:
        output_dir.rmdir()
    except OSError:
        pass
    return removed


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--start-date', default=date_setting('PO_AND_TRANSFERS_START_DATE', 'START_DATE'),
                        help='YYYY-MM-DD, today or yesterday; defaults to the rolling-window start.')
    parser.add_argument('--end-date', default=date_setting('PO_AND_TRANSFERS_END_DATE', 'END_DATE'),
                        help='YYYY-MM-DD, today or yesterday; defaults to yesterday in --timezone.')
    parser.add_argument('--lookback-days', type=positive_int,
                        default=os.getenv('PO_AND_TRANSFERS_LOOKBACK_DAYS') or '31',
                        help='Inclusive rolling-window length (default: 31 completed days).')
    parser.add_argument('--timezone', default=os.getenv('PO_AND_TRANSFERS_TIMEZONE') or 'America/New_York',
                        help='Calendar timezone for relative dates (default: America/New_York).')
    parser.add_argument('--table-env', default=os.getenv('PO_AND_TRANSFERS_TABLE_ENV') or TABLE_ENV,
                        help='Name of the variable containing the SQL destination.')
    outputs = parser.add_mutually_exclusive_group()
    outputs.add_argument('--include-details', dest='include_details', action='store_true',
                         help='Also load purchase and transfer product details from the same pull.')
    outputs.add_argument('--summary-only', dest='include_details', action='store_false',
                         help='Load only the combined order-summary table, including single-table tests.')
    parser.set_defaults(include_details=str(os.getenv('PO_AND_TRANSFERS_INCLUDE_DETAILS') or 'false')
                        .strip().lower() in ('1', 'true', 'yes', 'on'))
    parser.add_argument('--purchase-table-env',
                        default=os.getenv('PO_AND_TRANSFERS_PURCHASE_TABLE_ENV') or 'TABLE_PURCHASE_ORDER',
                        help='Variable naming the purchase product-detail table.')
    parser.add_argument('--transfer-table-env',
                        default=os.getenv('PO_AND_TRANSFERS_TRANSFER_TABLE_ENV') or 'TABLE_TRANSFER_ORDER',
                        help='Variable naming the transfer product-detail table.')
    parser.add_argument('--date-criteria', type=listing.parse_date_criteria,
                        default=os.getenv('PO_AND_TRANSFERS_DATE_CRITERIA') or 'all')
    parser.add_argument('--center-id', action='append', default=[])
    parser.add_argument('--from-json', type=Path, help='Load a saved complete API snapshot without API calls.')
    parser.add_argument('--output-dir', type=Path,
                        help='Defaults to PO_AND_TRANSFERS_OUTPUT_DIR/<date range> or data/api_order_ingestion/<date range>.')
    parser.add_argument('--no-load', action='store_true', help='Export locally without connecting to SQL.')
    parser.add_argument('--check-table', action='store_true', help='Inspect the SQL destination; no API requests/writes.')
    parser.add_argument('--dry-run', action='store_true', help='Print resolved dates, centers and SQL destination; no API requests/writes.')
    parser.add_argument('--resume', action='store_true', help='Resume a previously interrupted pull using completed-request checkpoints.')
    parser.add_argument('--workers', type=positive_int, default=os.getenv('PO_AND_TRANSFERS_WORKERS') or '2',
                        help='Concurrent API requests (default: 2).')
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    target = target_table(args.table_env) if args.check_table or not args.no_load else None
    targets = detail_targets(args) if args.check_table or not args.no_load else {}
    if args.check_table:
        inspect_target(target)
        inspect_details(targets)
        return 0
    if args.workers < 1:
        raise ValueError('--workers must be at least 1.')
    if args.from_json:
        snapshot = json.loads(args.from_json.read_text(encoding='utf-8'))
        if snapshot.get('failures') or not isinstance(snapshot.get('records'), list):
            raise ValueError('The API snapshot is incomplete or invalid; nothing loaded.')
    else:
        args.start_date, args.end_date = resolve_date_range(args)
        snapshot = {'start_date': str(args.start_date), 'end_date': str(args.end_date)}
    validate_railway_window(snapshot['start_date'], snapshot['end_date'])
    centers = purchase_order.resolve_centers(args.center_id, all_centers=not args.center_id)
    criteria = ', '.join(listing.CRITERIA_LABELS[value] for value in args.date_criteria)
    print(f'  Window   {snapshot["start_date"]} .. {snapshot["end_date"]} '
          f'| {args.timezone}', flush=True)
    print(f'  Scope    {len(centers)} centers | {criteria}', flush=True)
    print(f'  Target   {"SQL summary + details" if target else "export only"}', flush=True)
    if args.include_details:
        print('  Details  purchase + transfer line items', flush=True)
    if args.dry_run:
        print('Dry run complete; no API requests or SQL connections made.', flush=True)
        return 0
    base_output_dir = Path(os.getenv('PO_AND_TRANSFERS_OUTPUT_DIR') or OUTPUT_DIR)
    output_dir = args.output_dir or base_output_dir / f'{snapshot["start_date"]}_to_{snapshot["end_date"]}'
    metadata = None
    if target is not None:
        metadata = inspect_target(target)['columns']
        inspect_details(targets)
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        if not args.from_json:
            snapshot = listing.fetch(args.start_date, args.end_date, args.date_criteria, centers,
                                     cache_dir=output_dir / '_requests', resume=args.resume,
                                     workers=args.workers)
        selected = listing.deduplicate(snapshot['records'])
        rows = [listing.order_row(record) for record in selected.values()]
        rows.sort(key=lambda row: (listing.timestamp(row['Order on']) or datetime.min,
                                  row['REF #']), reverse=True)
        # Validate values before saving outputs or beginning a SQL transaction.
        sql_rows(rows, metadata=metadata)
        details = prepare_details(selected.values()) if args.include_details else {}
        stem = f'orders_{snapshot["start_date"]}_to_{snapshot["end_date"]}'
        csv_path, raw_path = output_dir / f'{stem}.csv', output_dir / f'{stem}_api.json'
        listing.write_csv(csv_path, listing.CSV_COLUMNS, rows)
        raw_path.write_text(json.dumps(snapshot, indent=2, default=str), encoding='utf-8')
        sources = dict(Counter(record['source'] for record in selected.values()))
        print(f'  Orders   {len(rows):,} total | {sources.get("purchase", 0):,} purchase '
              f'| {sources.get("transfer", 0):,} transfer', flush=True)
        for source, data in details.items():
            detail_path = output_dir / f'{source}_orders_{snapshot["start_date"]}_to_{snapshot["end_date"]}_line_items.csv'
            data['rows'].to_csv(detail_path, index=False, encoding='utf-8-sig')
        if details:
            print(f'  Items    {len(details["purchase"]["rows"]):,} purchase '
                  f'| {len(details["transfer"]["rows"]):,} transfer', flush=True)
        if not running_on_railway():
            print(f'  Files    {output_dir}', flush=True)
        if target is not None:
            report = (load_all_tables(rows, target, details, targets)
                      if args.include_details else upsert_rows(rows, target))
            report.update({'start_date': snapshot['start_date'], 'end_date': snapshot['end_date'],
                           'sources': sources, 'api_snapshot': str(raw_path),
                           'source_csv': str(csv_path)})
            report_path = output_dir / f'{stem}_load.json'
            report_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
        return 0
    finally:
        if running_on_railway():
            try:
                removed = cleanup_run_files(
                    output_dir, snapshot['start_date'], snapshot['end_date'])
                print(f'  Cleanup  removed {removed} temporary output item(s)', flush=True)
            except Exception as error:
                print(f'  Cleanup  failed: {error}', flush=True)


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, argparse.ArgumentTypeError, pyodbc.Error) as error:
        raise SystemExit(str(error))
