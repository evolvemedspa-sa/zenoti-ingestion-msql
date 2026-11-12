"""Regression checks for scheduled dates, API boundaries and Railway failures."""

import ast
import contextlib
import io
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from PO_Transfer import api_client, order_listing_api as listing
from PO_Transfer import gdrive_helper, po_and_transfers_api as orders, run_all, service_log


DATE_VARIABLES = (
    'START_DATE', 'END_DATE', 'PO_AND_TRANSFERS_START_DATE', 'PO_AND_TRANSFERS_END_DATE',
    'PO_AND_TRANSFERS_LOOKBACK_DAYS', 'PO_AND_TRANSFERS_TIMEZONE',
    'PO_AND_TRANSFERS_WORKERS', 'PO_AND_TRANSFERS_TABLE_ENV',
    'PO_AND_TRANSFERS_INCLUDE_DETAILS', 'PO_AND_TRANSFERS_PURCHASE_TABLE_ENV',
    'PO_AND_TRANSFERS_TRANSFER_TABLE_ENV',
    'RAILWAY_ENVIRONMENT', 'RAILWAY_PROJECT_ID', 'RAILWAY_SERVICE_ID',
)


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(os.environ)
        self.environment.start()
        for name in DATE_VARIABLES:
            os.environ.pop(name, None)

    def tearDown(self):
        self.environment.stop()

    def test_new_york_calendar_before_utc_midnight_rollover(self):
        start, end = orders.resolve_date_range(orders.parse_args([]),
                                              datetime(2026, 11, 12, 0, 30, tzinfo=timezone.utc))
        self.assertEqual((start, end), (date(2026, 10, 11), date(2026, 11, 10)))
        self.assertEqual((end - start).days + 1, 31)

    def test_31_day_window_across_leap_month_and_dst(self):
        for instant in (datetime(2024, 3, 1, 18, tzinfo=timezone.utc),
                        datetime(2026, 11, 12, 18, tzinfo=timezone.utc)):
            start, end = orders.resolve_date_range(orders.parse_args([]), instant)
            self.assertEqual((end - start).days + 1, 31)
        self.assertEqual(end, date(2026, 11, 11))

    def test_railway_rejects_any_window_other_than_31_inclusive_days(self):
        os.environ['RAILWAY_ENVIRONMENT'] = 'production'
        self.assertEqual(
            orders.validate_railway_window('2026-10-12', '2026-11-11'), 31)
        with self.assertRaisesRegex(ValueError, 'exactly 31 inclusive days'):
            orders.validate_railway_window('2026-10-13', '2026-11-11')

    def test_dates_prefer_po_variables_then_cli(self):
        os.environ.update({'START_DATE': '2025-01-01', 'END_DATE': '2025-12-31',
                           'PO_AND_TRANSFERS_START_DATE': '2026-10-01',
                           'PO_AND_TRANSFERS_END_DATE': '2026-10-31'})
        now = datetime(2026, 11, 12, 18, tzinfo=timezone.utc)
        self.assertEqual(orders.resolve_date_range(orders.parse_args([]), now),
                         (date(2026, 10, 1), date(2026, 10, 31)))
        self.assertEqual(orders.resolve_date_range(orders.parse_args([
            '--start-date', '2026-11-01', '--end-date', 'yesterday']), now),
            (date(2026, 11, 1), date(2026, 11, 11)))

    def test_invalid_range_and_window_stop_before_work(self):
        with self.assertRaises(ValueError):
            orders.resolve_date_range(orders.parse_args([
                '--start-date', '2026-11-12', '--end-date', '2026-11-01']))
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            orders.parse_args(['--lookback-days', '0'])

    def test_dry_run_does_not_call_api_or_sql(self):
        os.environ['TABLE_PO_AND_TRANSFER_TEST'] = 'dbo.OrderTest'
        with patch.object(listing, 'fetch') as fetch, patch.object(orders, 'inspect_target') as inspect, \
                patch.object(orders, 'upsert_rows') as upsert, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(orders.main(['--dry-run', '--table-env', 'TABLE_PO_AND_TRANSFER_TEST']), 0)
        fetch.assert_not_called()
        inspect.assert_not_called()
        upsert.assert_not_called()

    def test_runner_selects_production_and_returns_failure(self):
        with patch.object(orders, 'main', side_effect=ValueError('incomplete API fetch')) as load, \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(run_all.main([]), 1)
        self.assertEqual(load.call_args.args[0], ['--table-env', 'TABLE_PO_AND_TRANSFERS',
                                                '--include-details'])

    def test_failure_log_redacts_qualified_table_names(self):
        os.environ['TABLE_PRIVATE'] = 'private_schema.RealOrderTable'
        message = run_all.safe_error_message(ValueError(
            'failed for private_schema.RealOrderTable and [private_schema].[RealOrderTable]'))
        self.assertNotIn('RealOrderTable', message)
        self.assertEqual(message, 'failed for <SQL table> and <SQL table>')

    def test_runner_allows_explicit_test_destination(self):
        with patch.object(orders, 'main', return_value=0) as load, \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(run_all.main(['--table-env', 'TABLE_PO_AND_TRANSFER_TEST',
                                           '--summary-only']), 0)
        self.assertEqual(load.call_args.args[0], ['--table-env', 'TABLE_PO_AND_TRANSFER_TEST',
                                                '--summary-only'])

    def test_cli_uploads_and_closes_log_on_success_and_failure(self):
        for outcome, expected in ((0, 0), (ValueError('failed'), 1)):
            session = MagicMock()
            with patch.object(run_all.service_log, 'start', return_value=session), \
                    patch.object(orders, 'main', side_effect=outcome if isinstance(outcome, Exception) else None,
                                 return_value=outcome if isinstance(outcome, int) else None), \
                    contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(run_all.cli([]), expected)
            session.upload.assert_called_once_with()
            session.close.assert_called_once_with()

    def test_log_upload_uses_parent_folder_and_expected_filename(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / service_log.LOG_FILENAME
            path.write_text('short useful log\n', encoding='utf-8')
            handle = MagicMock()
            session = service_log.LogSession(path, handle, sys.stdout, sys.stderr)
            with patch.dict(os.environ, {
                    'GDRIVE_PARENT_FOLDER': 'parent-folder-id',
                    'GDRIVE_CREDENTIALS_JSON': '{"credential": "placeholder"}',
                    'GDRIVE_CREDENTIALS_FILE': '',
                }, clear=False), \
                    patch.object(service_log, 'upload_file_to_gdrive',
                                 return_value='file-id') as upload, \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertTrue(session.upload())

            self.assertEqual(upload.call_args.args[1], 'parent-folder-id')
            self.assertEqual(upload.call_args.kwargs['credentials_json'],
                             '{"credential": "placeholder"}')

    def test_drive_helper_failure_is_reported_without_failing_job(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / service_log.LOG_FILENAME
            path.write_text('log\n', encoding='utf-8')
            session = service_log.LogSession(path, MagicMock(), sys.stdout, sys.stderr)
            with patch.dict(os.environ, {
                    'GDRIVE_PARENT_FOLDER': 'bad-or-unshared-folder',
                    'GDRIVE_CREDENTIALS_JSON': '{}',
                }, clear=False), \
                    patch.object(service_log, 'upload_file_to_gdrive',
                                 side_effect=ValueError('folder unavailable')), \
                    contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertFalse(session.upload())
            self.assertIn('folder unavailable', output.getvalue())

    def test_drive_helper_uses_oauth_fallback_for_inaccessible_service_account(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / service_log.LOG_FILENAME
            path.write_text('log\n', encoding='utf-8')
            response = MagicMock(status_code=404, text='not found')
            session = MagicMock()
            session.post.return_value = response
            with patch.object(gdrive_helper, '_get_authed_session', return_value=session), \
                    patch.object(gdrive_helper, '_upload_via_oauth',
                                 return_value='oauth-file-id') as fallback:
                result = gdrive_helper.upload_file_to_gdrive(
                    str(path), 'parent-folder', credentials_json='{}')
            self.assertEqual(result, 'oauth-file-id')
            fallback.assert_called_once_with(str(path), 'parent-folder')

    def test_drive_helper_can_use_oauth_without_service_account_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / service_log.LOG_FILENAME
            path.write_text('log\n', encoding='utf-8')
            with patch.object(gdrive_helper, '_upload_via_oauth',
                              return_value='oauth-file-id') as fallback:
                result = gdrive_helper.upload_file_to_gdrive(
                    str(path), 'parent-folder')
            self.assertEqual(result, 'oauth-file-id')
            fallback.assert_called_once_with(str(path), 'parent-folder')

    def test_railway_cleanup_removes_only_generated_run_files(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / '2026-10-12_to_2026-11-11'
            requests = output / '_requests'
            requests.mkdir(parents=True)
            (requests / 'checkpoint.json').write_text('{}', encoding='utf-8')
            generated = (
                'orders_2026-10-12_to_2026-11-11.csv',
                'orders_2026-10-12_to_2026-11-11_api.json',
                'purchase_orders_2026-10-12_to_2026-11-11_line_items.csv',
            )
            for name in generated:
                (output / name).write_text('test', encoding='utf-8')
            unrelated = output / 'keep.txt'
            unrelated.write_text('keep', encoding='utf-8')

            removed = orders.cleanup_run_files(output, '2026-10-12', '2026-11-11')

            self.assertEqual(removed, len(generated) + 1)
            self.assertTrue(unrelated.exists())
            self.assertFalse(requests.exists())
            self.assertTrue(all(not (output / name).exists() for name in generated))

    def test_api_jobs_send_criterion_and_delivery_detail_fields(self):
        for source, endpoint in (('purchase', api_client.PURCHASE), ('transfer', api_client.TRANSFER)):
            with patch.object(endpoint, 'build_session') as build, \
                    patch.object(endpoint, 'request_with_retry', return_value={'orders': [], 'total_orders': 0}) as request:
                listing.fetch_job((source, endpoint, ('center-id', 'Center'),
                                   date(2026, 10, 12), date(2026, 11, 11), 4))
                params = request.call_args.args[1]
                self.assertEqual(params['date_criteria'], 4)
                self.assertEqual(params['status'], -1)
                self.assertEqual(params['end_date'], '2026-11-11')
                if source == 'transfer':
                    self.assertEqual(params['show_delivery_details'], 'true')

    def test_api_requests_have_a_shared_minimum_gap(self):
        with patch.object(api_client, '_next_request_at', 0.0), \
                patch.object(api_client.time, 'monotonic', side_effect=[100.0, 100.25]), \
                patch.object(api_client.time, 'sleep') as sleep:
            api_client.wait_for_request_slot()
            api_client.wait_for_request_slot()
        sleep.assert_called_once_with(0.75)

    def test_fetch_progress_reads_throttle_from_api_helper(self):
        with patch.object(listing, 'ThreadPoolExecutor') as executor, \
                patch.object(listing, 'as_completed', return_value=[]), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            executor.return_value.__enter__.return_value.submit.return_value = MagicMock()
            snapshot = listing.fetch(
                date(2026, 10, 12), date(2026, 11, 11), [4],
                [('center-id', 'Center')], workers=1)
        self.assertIn('1.0s minimum gap', output.getvalue())
        self.assertEqual(snapshot['completed_jobs'], 2)

    def test_main_reports_runner_has_no_order_writers(self):
        path = Path(__file__).resolve().parents[2] / 'run_all.py'
        tree = ast.parse(path.read_text(encoding='utf-8'))
        scripts = next(ast.literal_eval(node.value) for node in tree.body
                       if isinstance(node, ast.Assign)
                       and any(isinstance(target, ast.Name) and target.id == 'SCRIPTS' for target in node.targets))
        self.assertIn('stock_inventory.py', scripts)
        self.assertFalse(any(script in scripts for script in (
            'po_and_transfers.py', 'po_and_transfers_api.py', 'purchase_order.py', 'transfer_order.py')))


if __name__ == '__main__':
    unittest.main()
