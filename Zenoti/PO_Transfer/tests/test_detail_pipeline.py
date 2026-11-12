"""Regression checks for the former chained loaders and their shared transaction."""

import contextlib
import copy
import csv
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pyodbc

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from PO_Transfer import detail_sql, order_listing_api as listing
from PO_Transfer import po_and_transfers_api as orders, run_all


def api_record(source, ref):
    def item(code, retail, consumable, price):
        return {
            'product_code': code, 'product_name': f'Product {code}',
            'ordered_retail_quantity': retail, 'delivered_retail_quantity': retail,
            'ordered_consumable_quantity': consumable,
            'delivered_consumable_quantity': consumable,
            'ordered_unit_price': price, 'delivered_unit_price': price,
            'delivered_tax_amount': 0,
        }
    return {
        'source': source, 'requested_center': 'Destination', 'date_criteria': 2,
        'order': {
            'order_id': f'{source}-{ref}', 'order_number': ref, 'status': 'DELIVERED',
            'ordered_date': '2025-12-15T09:00:00', 'raised_date': '2025-12-15T09:00:00',
            'center_name': 'Destination',
            'vendor_name': 'Supplier' if source == 'purchase' else 'Source center',
            'partials': [
                {'delivered_date': '2025-12-31T10:00:00', 'notes': 'First delivery',
                 'line_items': [item('SKU1', 2, 0, 12), item('SKU2', 0, 0, 8)]},
                {'delivered_date': '2026-11-09T17:10:00', 'notes': 'Final delivery',
                 'line_items': [item('SKU1', 1, 0, 12), item('SKU2', 0, 1, 8)]},
            ],
        },
    }


class DetailPipelineTests(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(os.environ)
        self.environment.start()
        for name in ('PO_AND_TRANSFERS_INCLUDE_DETAILS', 'PO_AND_TRANSFERS_TABLE_ENV',
                     'PO_AND_TRANSFERS_PURCHASE_TABLE_ENV', 'PO_AND_TRANSFERS_TRANSFER_TABLE_ENV'):
            os.environ.pop(name, None)

    def tearDown(self):
        self.environment.stop()

    def test_same_selected_orders_feed_summary_and_product_details(self):
        purchase = api_record('purchase', '2946')
        older = copy.deepcopy(purchase)
        older['order']['status'] = 'PARTIALLY DELIVERED'
        older['order']['partials'] = older['order']['partials'][:1]
        transfer = api_record('transfer', '7501')
        selected = listing.deduplicate([older, purchase, transfer, copy.deepcopy(purchase)])
        summary = [listing.order_row(record) for record in selected.values()]
        details = orders.prepare_details(selected.values())

        self.assertEqual(len(summary), 2)
        self.assertEqual(details['purchase']['references'], ['2946'])
        self.assertEqual(len(details['purchase']['rows']), 2)
        self.assertEqual(len(details['transfer']['rows']), 2)
        self.assertEqual(summary[0]['Status'], 'DELIVERED')
        self.assertEqual(summary[0]['Deliver on'], '11/09/2026 17:10')
        self.assertEqual(summary[0]['Value'], '44.00')
        self.assertEqual(details['purchase']['rows']['pending_retail_quantity'].sum(), 0)
        self.assertEqual(details['purchase']['rows']['pending_consumable_quantity'].sum(), 0)
        self.assertEqual(details['transfer']['rows']['transfer_status'].tolist(),
                         ['DELIVERED', 'DELIVERED'])

    def test_all_three_loads_share_one_commit(self):
        connection = MagicMock()
        order = []
        details = orders.prepare_details([
            api_record('purchase', '2946'), api_record('transfer', '7501')])
        targets = {'purchase': '[dbo].[Purchase]', 'transfer': '[dbo].[Transfer]'}
        def summary_load(*args, **kwargs):
            order.append('summary')
            return {'committed': False, 'inserted': 0, 'updated': 2}
        def purchase_load(*args, **kwargs):
            order.append('purchase')
            return 2
        def transfer_load(*args, **kwargs):
            order.append('transfer')
            return 2
        with patch.object(orders, 'get_connection', return_value=connection), \
                patch.object(orders, 'upsert_rows', side_effect=summary_load) as summary, \
                patch.object(orders.purchase_details, 'load_to_sql',
                             side_effect=purchase_load) as purchase, \
                patch.object(orders.transfer_details, 'load_to_sql',
                             side_effect=transfer_load) as transfer, \
                contextlib.redirect_stdout(io.StringIO()):
            report = orders.load_all_tables([], '[dbo].[Summary]', details, targets)
        self.assertIs(summary.call_args.kwargs['connection'], connection)
        self.assertFalse(summary.call_args.kwargs['commit'])
        for load, source in ((purchase, 'purchase'), (transfer, 'transfer')):
            self.assertIs(load.call_args.kwargs['connection'], connection)
            self.assertFalse(load.call_args.kwargs['commit'])
            self.assertTrue(load.call_args.kwargs['replace'])
            self.assertEqual(load.call_args.kwargs['references'], details[source]['references'])
            self.assertEqual(load.call_args.kwargs['table'], targets[source])
        connection.commit.assert_called_once_with()
        connection.rollback.assert_not_called()
        connection.close.assert_called_once_with()
        self.assertTrue(report['committed'])
        self.assertEqual(order, ['summary', 'purchase', 'transfer'])

    def test_transfer_failure_rolls_back_the_summary_and_purchase_load(self):
        connection = MagicMock()
        details = orders.prepare_details([
            api_record('purchase', '2946'), api_record('transfer', '7501')])
        with patch.object(orders, 'get_connection', return_value=connection), \
                patch.object(orders, 'upsert_rows', return_value={'committed': False}) as summary, \
                patch.object(orders.purchase_details, 'load_to_sql', return_value=2) as purchase, \
                patch.object(orders.transfer_details, 'load_to_sql', side_effect=pyodbc.Error('failed')):
            with self.assertRaises(pyodbc.Error):
                orders.load_all_tables([], '[dbo].[Summary]', details,
                                       {'purchase': '[dbo].[Purchase]', 'transfer': '[dbo].[Transfer]'})
        summary.assert_called_once()
        purchase.assert_called_once()
        connection.commit.assert_not_called()
        connection.rollback.assert_called_once_with()
        connection.close.assert_called_once_with()

    def test_snapshot_replay_exports_all_three_without_api_or_sql(self):
        snapshot = {'start_date': '2026-10-12', 'end_date': '2026-11-11', 'failures': [],
                    'records': [api_record('purchase', '2946'), api_record('transfer', '7501')]}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            saved = root / 'snapshot.json'
            saved.write_text(json.dumps(snapshot), encoding='utf-8')
            with patch.object(listing, 'fetch') as fetch, \
                    patch.object(orders, 'get_connection') as connect, \
                    patch.object(detail_sql, 'get_connection') as detail_connect, \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(run_all.main(['--from-json', str(saved), '--no-load',
                                               '--output-dir', str(root)]), 0)
            fetch.assert_not_called()
            connect.assert_not_called()
            detail_connect.assert_not_called()
            for name, count in (('orders_2026-10-12_to_2026-11-11.csv', 2),
                                ('purchase_orders_2026-10-12_to_2026-11-11_line_items.csv', 2),
                                ('transfer_orders_2026-10-12_to_2026-11-11_line_items.csv', 2)):
                with (root / name).open(encoding='utf-8-sig', newline='') as handle:
                    self.assertEqual(len(list(csv.DictReader(handle))), count)

    def test_summary_test_target_requires_explicit_detail_test_targets(self):
        os.environ['TABLE_PO_AND_TRANSFER_TEST'] = 'dbo.SummaryTest'
        with patch.object(listing, 'fetch') as fetch, \
                patch.object(orders, 'get_connection') as connect, \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(run_all.main(['--table-env', 'TABLE_PO_AND_TRANSFER_TEST', '--dry-run']), 1)
            self.assertEqual(run_all.main(['--table-env', 'TABLE_PO_AND_TRANSFER_TEST',
                                           '--summary-only', '--dry-run']), 0)
        fetch.assert_not_called()
        connect.assert_not_called()
        os.environ.update({'TABLE_PURCHASE_ORDER_TEST': 'dbo.PurchaseTest',
                           'TABLE_TRANSFER_ORDER_TEST': 'dbo.TransferTest'})
        args = orders.parse_args(['--include-details', '--table-env', 'TABLE_PO_AND_TRANSFER_TEST',
                                  '--purchase-table-env', 'TABLE_PURCHASE_ORDER_TEST',
                                  '--transfer-table-env', 'TABLE_TRANSFER_ORDER_TEST'])
        self.assertEqual(orders.detail_targets(args),
                         {'purchase': '[dbo].[PurchaseTest]', 'transfer': '[dbo].[TransferTest]'})

    def test_environment_can_select_summary_only_for_the_runner(self):
        os.environ['PO_AND_TRANSFERS_INCLUDE_DETAILS'] = 'false'
        with patch.object(orders, 'main', return_value=0) as load, \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(run_all.main([]), 0)
        self.assertNotIn('--include-details', load.call_args.args[0])
        self.assertFalse(orders.parse_args(load.call_args.args[0]).include_details)


class DetailSqlTests(unittest.TestCase):
    def test_input_sizes_omit_zero_precision_and_max_width_bindings(self):
        self.assertIsNone(detail_sql.input_size_for({
            'type': 'int', 'length': 4, 'precision': 10, 'scale': 0}))
        self.assertIsNone(detail_sql.input_size_for({
            'type': 'datetime2', 'length': 8, 'precision': None, 'scale': 7}))
        self.assertIsNone(detail_sql.input_size_for({
            'type': 'varchar', 'length': -1, 'precision': None, 'scale': None}))
        self.assertEqual(
            detail_sql.input_size_for({
                'type': 'decimal', 'length': 9, 'precision': 18, 'scale': 0}),
            (pyodbc.SQL_DECIMAL, 18, 0))
        self.assertEqual(
            detail_sql.input_size_for({
                'type': 'nvarchar', 'length': 80, 'precision': None, 'scale': None}),
            (pyodbc.SQL_WVARCHAR, 80, 0))

    def sql_fixture(self, counts):
        cursor = MagicMock()
        cursor.fetchone.side_effect = [(count,) for count in counts]
        connection = MagicMock()
        connection.cursor.return_value = cursor
        lookup = {'ref_number': 'ref_number', 'product_code': 'product_code'}
        metadata = {column: {'type': 'nvarchar', 'length': 100, 'precision': None, 'scale': None}
                    for column in lookup}
        return connection, cursor, patch.object(detail_sql, 'inspect_target',
                                                return_value=(lookup, metadata))

    def test_wide_backfill_replacement_stays_within_sql_parameter_limit(self):
        connection, cursor, metadata = self.sql_fixture([1500, 400, 200, 100, 801])
        frame = pd.DataFrame([{'ref_number': 'r0', 'product_code': 'SKU'}])
        references = [f'r{number}' for number in range(1101)]
        with metadata, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(detail_sql.load_line_items(
                frame, 'dbo.Details', frame.columns, replace=True, references=references,
                connection=connection, commit=False), 1)
        deletes = [call.args for call in cursor.execute.call_args_list
                   if call.args[0].startswith('DELETE FROM')]
        self.assertEqual(len(deletes), 3)
        self.assertTrue(all(len(args[1]) <= 500 for args in deletes))
        self.assertEqual({ref for args in deletes for ref in args[1]}, set(references))
        connection.commit.assert_not_called()
        connection.rollback.assert_not_called()

    def test_insert_failure_rolls_back_without_retrying_an_append(self):
        connection, cursor, metadata = self.sql_fixture([2, 2])
        def execute(sql, *params):
            if sql.startswith('INSERT INTO'):
                raise pyodbc.ProgrammingError('insert failed')
            return cursor
        cursor.execute.side_effect = execute
        frame = pd.DataFrame([{'ref_number': '2946', 'product_code': 'SKU'}])
        with metadata, self.assertRaises(pyodbc.ProgrammingError):
            detail_sql.load_line_items(frame, 'dbo.Details', frame.columns, replace=True,
                                       connection=connection, commit=False)
        inserts = [call for call in cursor.execute.call_args_list
                   if call.args[0].startswith('INSERT INTO')]
        self.assertEqual(len(inserts), 1)
        cursor.executemany.assert_not_called()
        connection.rollback.assert_called_once_with()
        connection.commit.assert_not_called()

    def test_detail_inserts_use_bounded_multi_row_batches(self):
        columns = [f'column_{number}' for number in range(22)]
        data = [[f'value-{row}-{column}' for column in range(len(columns))]
                for row in range(305)]
        cursor = MagicMock()

        detail_sql.insert_rows(cursor, '[dbo].[Details]', columns, data)

        inserts = [call for call in cursor.execute.call_args_list
                   if call.args[0].startswith('INSERT INTO')]
        expected_batch_size = detail_sql.SQL_PARAMETER_BUDGET // len(columns)
        self.assertEqual(len(inserts), 4)
        self.assertTrue(all(len(call.args[1]) <= detail_sql.SQL_PARAMETER_BUDGET
                            for call in inserts))
        self.assertEqual(sum(len(call.args[1]) // len(columns) for call in inserts), len(data))
        self.assertEqual(len(inserts[0].args[1]), expected_batch_size * len(columns))
        cursor.executemany.assert_not_called()

    def test_fetched_order_without_products_removes_its_old_detail_rows(self):
        connection, cursor, metadata = self.sql_fixture([4, 2, 2])
        frame = pd.DataFrame(columns=['ref_number', 'product_code'])
        with metadata, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(detail_sql.load_line_items(
                frame, 'dbo.Details', frame.columns, replace=True, references=['2946'],
                connection=connection, commit=False), 0)
        deletes = [call.args for call in cursor.execute.call_args_list
                   if call.args[0].startswith('DELETE FROM')]
        self.assertEqual(deletes[0][1], ['2946'])
        cursor.executemany.assert_not_called()
        connection.rollback.assert_not_called()


if __name__ == '__main__':
    unittest.main()
