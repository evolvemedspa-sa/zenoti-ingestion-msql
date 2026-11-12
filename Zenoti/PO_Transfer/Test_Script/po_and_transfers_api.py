"""Compatibility entry point; Railway runs PO_Transfer/run_all.py."""
import argparse
import pyodbc
from PO_Transfer.po_and_transfers_api import (
    FIELD_MAP, INTEGER_LIMITS, LOAD_COLUMNS, OUTPUT_DIR, TABLE_ENV,
    calendar_timezone, date_setting, describe_target, inspect_target, listing,
    main, parse_args, parse_calendar_date, parse_end_date, positive_int,
    purchase_order, quote_identifier, resolve_date_range, sql_rows,
    target_table, upsert_rows, validate_values,
)

if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, argparse.ArgumentTypeError, pyodbc.Error) as error:
        raise SystemExit(str(error))
