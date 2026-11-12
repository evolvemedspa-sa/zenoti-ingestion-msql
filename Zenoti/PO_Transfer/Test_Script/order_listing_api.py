"""Compatibility entry point for the verified Orders Listing API exporter."""
import argparse
from PO_Transfer.order_listing_api import (
    CRITERIA_LABELS, CSV_COLUMNS, DEFAULT_OUTPUT_DIR, MONEY_PRECISION,
    compare, csv_date, decimal, deduplicate, fetch, fetch_cached_job, fetch_job,
    job_cache_key, latest_delivery, main, money, normalized, order_row,
    parse_args, parse_date_criteria, purchase_order, read_csv, reference,
    timestamp, transfer_order, unique_rows, write_csv,
)

if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, argparse.ArgumentTypeError) as error:
        raise SystemExit(str(error))
