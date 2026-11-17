"""Offline checks for the review loader built into sync_hc.py."""

import csv
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
with patch("dotenv.load_dotenv"):
    from Zenoti.zconnect import sync_hc as sync


def review(**overrides):
    row = {column: "" for column in sync.REVIEW_COLUMNS}
    row.update(center_id="00000000-0000-0000-0000-000000000001", review_key="a" * 64,
               event_id="event", metadata_index="0", rating="5", source="GOOGLE",
               comments='  Great, "care"!\n\u2605  ', metadata_json='{"tags":["Cleanliness"]}',
               event_date="2026-11-16T14:00:00-05:00")
    row.update(overrides)
    return row


def csv_text(rows):
    handle = io.StringIO(newline="")
    writer = csv.DictWriter(handle, fieldnames=sync.REVIEW_COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    handle.seek(0)
    return handle


class ReviewIngestionTests(unittest.TestCase):
    def setUp(self):
        directory = self.enterContext(tempfile.TemporaryDirectory())
        self.path = Path(directory, "events.csv")
        self.path.write_text(csv_text([review()]).getvalue(), encoding="utf-8-sig", newline="")
        self.connection = Mock()
        self.connect = Mock(return_value=self.connection)
        self.enterContext(patch.dict(sys.modules, {"db_helper": Mock(get_connection=self.connect)}))
        self.enterContext(patch.dict(os.environ, {"TABLE_HC_EVENTS": "dbo.REVIEWS"}))
        self.validate = self.enterContext(patch.object(sync, "validate_review_destination"))
        self.apply = self.enterContext(patch.object(sync, "apply_event_reviews", return_value=(1, 0, 0)))
        self.output = self.enterContext(patch("sys.stdout", new_callable=io.StringIO))

    def test_success_commits_typed_rows_and_keeps_source_for_orchestrator_cleanup(self):
        original = self.path.read_bytes()
        sync.load_event_reviews(str(self.path))
        self.connection.commit.assert_called_once_with()
        self.connection.rollback.assert_not_called()
        self.connection.close.assert_called_once_with()
        self.connection.cursor.return_value.close.assert_called_once_with()
        self.assertFalse(self.connection.autocommit)
        validation_call = self.validate.call_args
        assert validation_call is not None
        cursor, schema, name, table, rows = validation_call.args
        self.assertEqual((schema, name, table), ("dbo", "REVIEWS", "[dbo].[REVIEWS]"))
        self.assertEqual(rows[0]["comments"], review()["comments"])
        self.assertEqual(rows[0]["event_date"].isoformat(), "2026-11-16T19:00:00")
        self.apply.assert_called_once_with(cursor, table, rows)
        self.assertEqual(self.path.read_bytes(), original)
        self.assertIn("1 inserted, 0 refreshed, 0 older versions skipped", self.output.getvalue())

    def test_schema_failure_rolls_back_before_any_updates(self):
        self.validate.side_effect = ValueError("wrong review schema")
        with self.assertRaisesRegex(ValueError, "wrong review schema"):
            sync.load_event_reviews(str(self.path))
        self.apply.assert_not_called()
        self.connection.commit.assert_not_called()
        self.connection.rollback.assert_called_once_with()
        self.connection.cursor.return_value.close.assert_called_once_with()
        self.connection.close.assert_called_once_with()
        self.assertTrue(self.path.is_file())

    def test_write_failure_rolls_back_and_closes_connection(self):
        self.apply.side_effect = RuntimeError("write failed")
        with self.assertRaisesRegex(RuntimeError, "write failed"):
            sync.load_event_reviews(str(self.path))
        self.connection.commit.assert_not_called()
        self.connection.rollback.assert_called_once_with()
        self.connection.close.assert_called_once_with()

    def test_invalid_csv_is_rejected_before_connecting(self):
        self.path.write_text("centerid,send_date\nold,2026-11-16\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "CSV headers"):
            sync.load_event_reviews(str(self.path))
        self.connect.assert_not_called()

    def test_empty_export_needs_no_database_connection(self):
        self.path.write_text(csv_text([]).getvalue(), encoding="utf-8-sig", newline="")
        sync.load_event_reviews(str(self.path))
        self.connect.assert_not_called()
        self.assertIn("No reviews to ingest", self.output.getvalue())

    def test_missing_table_setting_is_rejected_before_connecting(self):
        with patch.dict(os.environ, {"TABLE_HC_EVENTS": ""}):
            with self.assertRaisesRegex(ValueError, "Set TABLE_HC_EVENTS"):
                sync.load_event_reviews(str(self.path))
        self.connect.assert_not_called()

    def test_event_ingestion_runs_in_process_and_converts_failures_to_status(self):
        with patch.object(sync, "_run_logged") as subprocess:
            status, _ = sync.ingest_report("events", str(self.path))
            self.assertEqual(status, "ok")
            self.validate.side_effect = ValueError("wrong schema")
            status, _ = sync.ingest_report("events", str(self.path))
            self.assertEqual(status, "FAILED (exit 1)")
            subprocess.assert_not_called()
        self.assertTrue(self.path.is_file())
        self.assertIn("ERROR: wrong schema", self.output.getvalue())


class ReviewSqlTests(unittest.TestCase):
    def test_batches_preserve_unicode_and_protect_newer_reviews_and_insertion_dates(self):
        rows, _, _ = sync.read_event_reviews(csv_text([
            review(review_key="a" * 64), review(review_key="b" * 64), review(review_key="c" * 64),
        ]))
        cursor = Mock(description=[("inserted",), ("updated",)])
        cursor.fetchone.side_effect = [(1, 1), (0, 1)]
        cursor.nextset.return_value = False
        with patch.object(sync, "REVIEW_BATCH_SIZE", 2):
            self.assertEqual(sync.apply_event_reviews(cursor, "[dbo].[REVIEWS]", rows), (1, 2, 0))
        batches = cursor.execute.call_args_list[1:]
        self.assertEqual(len(batches), 2)
        statement, payload = batches[0].args
        self.assertIn("s.[review_key] = t.[review_key]", statement)
        self.assertIn("s.[event_date] >= t.[event_date]", statement)
        self.assertIn("SYSUTCDATETIME()", statement)
        update = statement.split("UPDATE t SET", 1)[1].split("FROM [dbo].[REVIEWS]", 1)[0]
        self.assertNotIn("date_inserted", update)
        self.assertEqual(json.loads(payload)[0]["comments"], review()["comments"])
        self.assertEqual(json.loads(payload)[0]["event_date"], "2026-11-16T19:00:00")

    def test_live_schema_checks_unique_key_and_unicode_capacity(self):
        columns = []
        for column in (*sync.REVIEW_COLUMNS, "date_inserted"):
            kind, length = "nvarchar", -1
            if column in sync.REVIEW_ID_COLUMNS:
                kind, length = "uniqueidentifier", None
            elif column == "review_key":
                kind, length = "char", 64
            elif column in (*sync.REVIEW_DATE_COLUMNS, "date_inserted"):
                kind, length = "datetime2", None
            elif column in ("rating", "metadata_index"):
                kind, length = "int", None
            columns.append((column, kind, length))
        rows, _, _ = sync.read_event_reviews(csv_text([review()]))
        cursor = Mock()
        cursor.fetchall.return_value = columns
        cursor.fetchone.return_value = ("unique_review_key",)
        sync.validate_review_destination(cursor, "dbo", "REVIEWS", "[dbo].[REVIEWS]", rows)
        self.assertIn("FROM INFORMATION_SCHEMA.COLUMNS", cursor.execute.call_args_list[0].args[0])
        cursor.fetchone.return_value = None
        with self.assertRaisesRegex(ValueError, "unique index on review_key"):
            sync.validate_review_destination(cursor, "dbo", "REVIEWS", "[dbo].[REVIEWS]", rows)
        cursor.fetchone.return_value = ("unique_review_key",)
        cursor.fetchall.return_value = [(name, kind, 1 if name == "comments" else length)
                                        for name, kind, length in columns]
        with self.assertRaisesRegex(ValueError, "comments: exceeds SQL capacity"):
            sync.validate_review_destination(cursor, "dbo", "REVIEWS", "[dbo].[REVIEWS]", rows)


if __name__ == "__main__":
    unittest.main()
