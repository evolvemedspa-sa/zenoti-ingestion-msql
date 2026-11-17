"""Offline regressions for historical review request failures."""

import csv
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import call, patch

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
# Do not read local credentials or depend on the user's retry settings.
with patch("dotenv.load_dotenv"), patch.dict(os.environ, {
    "ZENOTI_API_KEY": "offline-test-key",
    "ZENOTI_BASE_URL": "https://api.zenoti.com",
    "ZENOTI_MAX_RETRIES": "2",
    "ZENOTI_RETRY_BACKOFF": "5",
}):
    from Zenoti.zconnect import _client as client, get_events as events


def response(status=200, body='{"feedback": [], "error": null}', headers=None):
    result = requests.Response()
    result.status_code = status
    result._content = body.encode("utf-8")
    result._content_consumed = True
    result.headers.update(headers or {})
    result.url = "https://api.zenoti.com/v1/feedbacks"
    return result


class ReviewRequestTests(unittest.TestCase):
    def setUp(self):
        self.request = self.enterContext(patch.object(client.session, "request"))
        self.sleep = self.enterContext(patch.object(client.time, "sleep"))
        self.stderr = self.enterContext(patch("sys.stderr", new_callable=io.StringIO))
        self.enterContext(patch.object(client, "MAX_RETRIES", 2))
        self.enterContext(patch.object(client, "RETRY_BACKOFF_BASE", 5))

    def test_empty_and_html_successes_retry_the_identical_get(self):
        self.request.side_effect = [response(body=""), response(body="<html>Temporary failure</html>"),
                                    response(headers={"Content-Type": "application/json"})]
        self.assertEqual(client.get("/feedbacks", appointment_group_id="old-group"),
                         {"feedback": [], "error": None})
        expected = call("GET", "https://api.zenoti.com/v1/feedbacks", timeout=30,
                        params={"appointment_group_id": "old-group"})
        self.assertEqual(self.request.call_args_list, [expected] * 3)
        self.assertEqual(self.sleep.call_args_list, [call(5), call(10)])

    def test_persistent_empty_body_reports_http_details_after_bounded_retries(self):
        self.request.side_effect = [response(body="", headers={"Content-Type": "text/plain"})
                                    for _ in range(3)]
        with self.assertRaises(client.ZConnectError) as caught:
            client.get("/feedbacks", appointment_group_id="old-group")
        message = str(caught.exception)
        for detail in ("after 3 attempt(s)", "HTTP 200", "Content-Type=text/plain", "body=''"):
            self.assertIn(detail, message)
        self.assertEqual(self.request.call_count, 3)
        self.assertEqual(self.sleep.call_count, 2)

    def test_native_feedback_204_is_an_empty_lookup_without_json_parsing_or_retries(self):
        no_content = response(204, "", headers={"Content-Type": "application/json; charset=utf-8"})
        self.request.side_effect = [no_content]
        with patch.object(client, "MAX_RETRIES", 0), \
                patch.object(no_content, "json", wraps=no_content.json) as parse_json, \
                patch.object(no_content, "close") as close:
            self.assertEqual(client.get("/feedbacks", appointment_group_id="empty-group"),
                             {"feedback": [], "error": None})
            parse_json.assert_not_called()
            close.assert_called_once_with()
        self.assertEqual(self.request.call_count, 1)
        self.sleep.assert_not_called()

    def test_throttled_feedback_lookup_can_recover_with_204(self):
        self.request.side_effect = [response(429, headers={"Retry-After": "8"}), response(204, "")]
        self.assertEqual(client.get("/feedbacks", appointment_group_id="empty-group"),
                         {"feedback": [], "error": None})
        self.assertEqual(self.request.call_count, 2)
        self.sleep.assert_called_once_with(8)

    def test_204_on_other_routes_is_not_normalized_to_empty_feedback(self):
        self.request.side_effect = [response(204, "")]
        with patch.object(client, "MAX_RETRIES", 0):
            with self.assertRaisesRegex(client.ZConnectError, "HTTP 204"):
                client.get("/zconnect/centers/center/events", page=2)
        self.assertEqual(self.request.call_count, 1)

    def test_temporary_get_statuses_recover_with_json_or_html_bodies(self):
        for status in (403, 408, 429, 500, 502, 503, 504):
            for body in ('{"error": {"code": "temporary"}}', "<html>Unavailable</html>"):
                with self.subTest(status=status, body=body):
                    self.request.reset_mock()
                    self.sleep.reset_mock()
                    self.request.side_effect = [response(status, body), response()]
                    self.assertEqual(client.get("/feedbacks"), {"feedback": [], "error": None})
                    self.assertEqual(self.request.call_count, 2)
                    self.sleep.assert_called_once_with(5)

    def test_retry_after_seconds_date_and_invalid_values(self):
        for header, expected in (("25", 25), ("Wed, 01 Jan 2025 00:00:00 GMT", 20),
                                 ("invalid", 5), ("NaN", 5), ("-1", 5)):
            with self.subTest(header=header), patch.object(client.time, "time", return_value=1735689580):
                self.sleep.reset_mock()
                self.request.side_effect = [response(429, headers={"Retry-After": header}), response()]
                client.get("/feedbacks")
                self.sleep.assert_called_once_with(expected)

    def test_timeouts_still_retry(self):
        self.request.side_effect = [requests.exceptions.ReadTimeout(), response()]
        self.assertEqual(client.get("/feedbacks"), {"feedback": [], "error": None})
        self.sleep.assert_called_once_with(5)

    def test_real_api_errors_and_valid_empty_results_are_not_retried(self):
        for body in ('{"error": {"code": "CT004", "message": "Invalid input"}}',
                     '{"error": {"code": "CT028", "message": "Data Not Found"}}',
                     '{"feedback": []}'):
            with self.subTest(body=body):
                self.request.reset_mock()
                self.sleep.reset_mock()
                self.request.side_effect = [response(body=body)]
                if "CT004" in body:
                    with self.assertRaisesRegex(client.ZConnectError, "CT004"):
                        client.get("/feedbacks")
                else:
                    self.assertEqual(client.get("/feedbacks"), json.loads(body))
                self.assertEqual(self.request.call_count, 1)
                self.sleep.assert_not_called()

    def test_permanent_http_error_is_not_retried(self):
        self.request.side_effect = [response(401, "Access denied")]
        with self.assertRaisesRegex(client.ZConnectError, "HTTP 401"):
            client.get("/feedbacks")
        self.assertEqual(self.request.call_count, 1)
        self.sleep.assert_not_called()

    def test_new_retry_cases_do_not_repeat_posts(self):
        for status, body, error in ((200, "", client.ZConnectError),
                                    (503, '{"message": "Unavailable"}', requests.HTTPError)):
            with self.subTest(status=status):
                self.request.reset_mock()
                self.request.side_effect = [response(status, body)]
                with self.assertRaises(error):
                    client.post("/feedbacks", {"example": "write"})
                self.assertEqual(self.request.call_count, 1)
                self.sleep.assert_not_called()

    def test_zero_retries_fails_on_first_empty_response(self):
        self.request.side_effect = [response(body="")]
        with patch.object(client, "MAX_RETRIES", 0):
            with self.assertRaisesRegex(client.ZConnectError, "after 1 attempt"):
                client.get("/feedbacks")
        self.assertEqual(self.request.call_count, 1)
        self.sleep.assert_not_called()

    def test_failed_historical_detail_preserves_csv_and_identifies_record(self):
        event = {"event_id": "historical-event", "category_type": 5, "action_type": 6,
                 "source": 0, "reference_id_1": "historical-group", "metadata": "Feedback"}
        self.request.side_effect = [response(body=json.dumps({"events_data": [event],
                                                            "page_info": {"total": 1}}))] + [
            response(body="") for _ in range(3)]
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "existing.csv"
            destination.write_bytes(b"previous complete export")
            with patch.object(client, "centers_to_fetch", return_value=[("center", "Bel Air, MD")]), \
                    patch("sys.stdout", new_callable=io.StringIO):
                result = events.main(["--start-date", "2025-06-20", "--end-date", "2025-06-29",
                                      "--csv", str(destination)])
            self.assertEqual(result, 1)
            self.assertEqual(destination.read_bytes(), b"previous complete export")
            self.assertEqual(list(Path(directory).iterdir()), [destination])
        for detail in ("Bel Air, MD", "center_id=center", "2025-06-20..2025-06-29",
                       "event_id=historical-event", "appointment_group_id=historical-group",
                       "HTTP 200", "no CSV published"):
            self.assertIn(detail, self.stderr.getvalue())

    def test_empty_historical_feedback_does_not_abort_export_or_repeat_lookup(self):
        empty = {"event_id": "empty-feedback", "category_type": 5, "action_type": 6,
                 "source": 0, "reference_id_1": "empty-group"}
        google = {"event_id": "google", "category_type": 5, "metadata": json.dumps([{
            "source": "GOOGLE", "created_on": "2025-04-24T12:00:00Z", "rating": 5,
            "reviewer_name": "Example", "review_text": "Google review",
        }])}
        native = dict(empty, event_id="available-feedback", reference_id_1="available-group")
        feed = {"events_data": [empty, google, native, dict(empty, event_id="empty-update")],
                "page_info": {"total": 4}}
        details = {"feedback": [{"appointment_group_id": "available-group", "invoice_id": "invoice",
                                 "service_rating": 4, "notes": "Native feedback"}], "error": None}
        self.request.side_effect = [response(body=json.dumps(feed)), response(204, ""),
                                    response(body=json.dumps(details))]
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory, "reviews.csv")
            with patch.object(client, "centers_to_fetch", return_value=[("center", "Jersey City, NJ")]), \
                    patch("sys.stdout", new_callable=io.StringIO):
                result = events.main(["--start-date", "2025-04-21", "--end-date", "2025-04-30",
                                      "--csv", str(destination)])
            self.assertEqual(result, 0)
            with destination.open(encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
        self.assertEqual({row["source"] for row in rows}, {"GOOGLE", "ZENOTI"})
        self.assertEqual({row["comments"] for row in rows}, {"Google review", "Native feedback"})
        self.assertEqual(len(rows), 2)
        self.assertEqual(self.request.call_count, 3)
        self.assertNotIn("retry", self.stderr.getvalue())

    def test_page_failure_does_not_report_previous_native_event_as_the_cause(self):
        event = {"event_id": "previous-event", "category_type": 5, "action_type": 6,
                 "source": 0, "reference_id_1": "previous-group"}
        self.request.side_effect = [response(body=json.dumps({"events_data": [event]})), response()] + [
            response(body="") for _ in range(3)]
        with patch("sys.stdout", new_callable=io.StringIO):
            with self.assertRaises(RuntimeError) as caught:
                events.collect_reviews(client, [("center", "Name")],
                                       [("2025-06-20", "2025-06-29")], 200, 0)
        self.assertIn("2025-06-20..2025-06-29", str(caught.exception))
        self.assertIn("GET /zconnect/centers/center/events failed on page 2", str(caught.exception))
        self.assertIn("1 events received", str(caught.exception))
        self.assertIn("HTTP 200", str(caught.exception))
        self.assertNotIn("event_id=", str(caught.exception))
        self.assertNotIn("appointment_group_id=", str(caught.exception))

    def test_historical_window_recovers_from_non_json_without_skipping_reviews(self):
        event = {"event_id": "review", "category_type": 5,
                 "send_date": "2025-06-25T12:00:00Z", "metadata": json.dumps([{
                     "source": "GOOGLE", "created_on": "2025-06-24T12:00:00Z",
                     "rating": 5, "reviewer_name": "Example", "review_text": "Great care",
                 }])}
        self.request.side_effect = [response(body="<html>Temporary failure</html>"), response(
            body=json.dumps({"events_data": [event], "page_info": {"total": 1}}))]
        with patch("sys.stdout", new_callable=io.StringIO):
            rows = events.collect_reviews(client, [("center", "Bel Air, MD")],
                                          [("2025-06-20", "2025-06-29")], 200, 0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["comments"], "Great care")
        self.assertEqual(self.request.call_count, 2)
        self.assertEqual(self.request.call_args_list[0], self.request.call_args_list[1])

    def test_exhausted_non_json_page_reports_status_body_and_window(self):
        self.request.side_effect = [response(body="<html>Unavailable</html>", headers={
            "Content-Type": "text/html",
        }) for _ in range(3)]
        with patch("sys.stdout", new_callable=io.StringIO):
            with self.assertRaises(RuntimeError) as caught:
                events.collect_reviews(client, [("center", "Bel Air, MD")],
                                       [("2025-06-20", "2025-06-29")], 200, 0)
        message = str(caught.exception)
        for detail in ("Bel Air, MD", "2025-06-20..2025-06-29", "failed on page 1",
                       "after 3 attempt(s):\nHTTP 200", "Content-Type=text/html",
                       "body='<html>Unavailable</html>'", "no CSV published"):
            self.assertIn(detail, message)
        self.assertEqual(self.request.call_count, 3)


if __name__ == "__main__":
    unittest.main()
