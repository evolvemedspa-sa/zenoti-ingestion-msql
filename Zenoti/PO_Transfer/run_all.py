"""Railway entry point: fetch once and load the order summary plus both details."""

import os
import re
import sys
import time
from datetime import datetime, timezone

if __package__:
    from . import po_and_transfers_api as orders, service_log
else:
    import po_and_transfers_api as orders
    import service_log


def safe_error_message(error):
    """Remove configured or qualified SQL table names from deploy logs."""
    message = str(error)
    table_values = {
        value for name, value in os.environ.items()
        if name.startswith('TABLE_') and value
    }
    for value in sorted(table_values, key=len, reverse=True):
        message = re.sub(re.escape(value), '<SQL table>', message, flags=re.IGNORECASE)
        if '.' in value:
            schema, table = value.split('.', 1)
            quoted = f'[{schema.strip(" []")}].[{table.strip(" []")}]'
            message = re.sub(re.escape(quoted), '<SQL table>', message,
                             flags=re.IGNORECASE)
    message = re.sub(r'\[[^\]\r\n]+\]\.\[[^\]\r\n]+\]', '<SQL table>', message)
    message = re.sub(r'(?i)\bdbo\.[A-Za-z_][A-Za-z0-9_]*\b', '<SQL table>', message)
    return message


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    if not any(value == '--table-env' or value.startswith('--table-env=') for value in args):
        args += ['--table-env', os.getenv('PO_AND_TRANSFERS_TABLE_ENV') or 'TABLE_PO_AND_TRANSFERS']
    if not any(value in ('--include-details', '--summary-only') for value in args) \
            and not os.getenv('PO_AND_TRANSFERS_INCLUDE_DETAILS'):
        args += ['--include-details']
    started = time.monotonic()
    print(f'[START] Zenoti PO/Transfer ingestion | '
          f'{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC', flush=True)
    try:
        result = orders.main(args)
    except Exception as error:
        elapsed = time.monotonic() - started
        print(f'[FAILED] {type(error).__name__}: {safe_error_message(error)} ({elapsed:.1f}s)',
              file=sys.stderr, flush=True)
        return 1
    print(f'[DONE] Zenoti PO/Transfer ingestion ({time.monotonic() - started:.1f}s)', flush=True)
    return result


def cli(argv=None):
    """Run with a Drive-uploaded log on both success and failure."""
    log = service_log.start()
    try:
        return main(argv)
    finally:
        log.upload()
        log.close()


if __name__ == '__main__':
    raise SystemExit(cli())
