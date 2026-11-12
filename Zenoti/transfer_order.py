"""Compatibility entry point; the transfer loader lives in PO_Transfer."""

from PO_Transfer.transfer_order import (
    DECIMAL_COLUMNS, INTEGER_COLUMNS, LINE_ITEM_COLUMNS,
    flatten_json, flatten_line_items, load_to_sql, main, to_order_dataframe,
)


if __name__ == '__main__':
    raise SystemExit(main())
