"""Cover the production varchar schema as well as the typed summary TEST schema."""

import contextlib
import io
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from PO_Transfer import po_and_transfers_api as orders


PO2946 = {
    'REF #': '2946', 'Order on': '12/15/2025 15:54',
    'Deliver on': '11/09/2026 17:10', 'To': 'Destination', 'From': 'Supplier',
    'Status': 'DELIVERED', 'Qty': '4', 'Value': '44.00', 'Tax': '0.00',
    'PO notes': 'Final delivery', 'Wait time': '329 day(s)',
}


def schema_metadata(*, production):
    result = {}
    for column in ('id', *orders.LOAD_COLUMNS):
        data_type, length, precision, scale = 'varchar', -1, 0, 0
        if column in ('id', 'order_qty') or (column == 'ref_no' and not production):
            data_type, length, precision = 'int', 4, 10
        elif column == 'ref_no':
            length = 100
        elif column in ('order_value', 'order_tax'):
            data_type, length, precision, scale = 'decimal', 9, 18 if production else 12, 2
        elif column == 'date_inserted':
            data_type, length, precision, scale = ('datetime', 8, 23, 3) if production else ('datetime2', 8, 27, 7)
        elif column in ('order_on', 'deliver_on') and not production:
            data_type, length, precision, scale = 'datetime2', 8, 27, 7
        result[column] = {
            'name': column, 'data_type': data_type, 'length': length,
            'precision': precision, 'scale': scale, 'is_nullable': column != 'id',
            'is_identity': column == 'id' and production,
            'is_computed': False, 'default_object_id': 0,
        }
    return result


class SummarySchemaTests(unittest.TestCase):
    def test_both_real_reference_column_types_pass_schema_validation(self):
        for production in (True, False):
            expected = schema_metadata(production=production)
            names = list(expected['ref_no'])
            cursor = MagicMock()
            cursor.description = [(name,) for name in names]
            cursor.fetchall.return_value = [tuple(info[name] for name in names)
                                           for info in expected.values()]
            actual = orders.describe_target(cursor, '[dbo].[SummaryFixture]')
            self.assertEqual(actual, expected)

    def test_production_rows_keep_text_references_and_existing_date_format(self):
        metadata = schema_metadata(production=True)
        row = dict(PO2946, **{'REF #': '02946'})
        values = dict(zip(orders.LOAD_COLUMNS, orders.sql_rows(
            [row], inserted_at=datetime(2026, 11, 12, 8), metadata=metadata)[0]))
        self.assertEqual(values['ref_no'], '02946')
        self.assertEqual(values['order_on'], '2025-12-15 15:54:00')
        self.assertEqual(values['deliver_on'], '2026-11-09 17:10:00')
        self.assertEqual(values['order_status'], 'DELIVERED')
        orders.validate_values([tuple(values[column] for column in orders.LOAD_COLUMNS)], metadata)

    def test_test_table_rows_keep_integer_references_and_datetime_values(self):
        values = dict(zip(orders.LOAD_COLUMNS, orders.sql_rows(
            [PO2946], metadata=schema_metadata(production=False))[0]))
        self.assertEqual(values['ref_no'], 2946)
        self.assertEqual(values['order_on'], datetime(2025, 12, 15, 15, 54))
        self.assertEqual(values['deliver_on'], datetime(2026, 11, 9, 17, 10))

    def test_existing_production_reference_is_staged_as_text_and_counted_as_update(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value
        cursor.fetchone.side_effect = [(1,), None, (1,), (1,), (0,)]
        with patch.object(orders, 'describe_target', return_value=schema_metadata(production=True)), \
                contextlib.redirect_stdout(io.StringIO()):
            report = orders.upsert_rows(iter([PO2946]), '[dbo].[SummaryFixture]',
                                        connection=connection, commit=False)
        staged = next(call.args[1] for call in cursor.execute.call_args_list
                      if call.args[0].startswith('INSERT INTO #ZenotiOrdersStage'))
        values = dict(zip(orders.LOAD_COLUMNS, staged))
        self.assertEqual(values['ref_no'], '2946')
        self.assertEqual(values['deliver_on'], '2026-11-09 17:10:00')
        self.assertEqual(report['updated'], 1)
        self.assertEqual(report['inserted'], 0)
        self.assertEqual(report['rows_before'], report['rows_after'])
        connection.commit.assert_not_called()
        connection.rollback.assert_not_called()


if __name__ == '__main__':
    unittest.main()
