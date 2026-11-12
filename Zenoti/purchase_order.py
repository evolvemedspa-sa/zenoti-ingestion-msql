"""Compatibility entry point; the purchase loader lives in PO_Transfer."""

from PO_Transfer.purchase_order import (
    DECIMAL_COLUMNS, INTEGER_COLUMNS, LINE_ITEM_COLUMNS,
    flatten_line_items, load_to_sql, main, to_order_dataframe,
)


if __name__ == '__main__':
    raise SystemExit(main())
