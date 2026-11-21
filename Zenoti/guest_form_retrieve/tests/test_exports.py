"""Regression checks for export behavior; all network and browser calls are mocked."""

import io
import json
import os
import unittest
from contextlib import redirect_stdout
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

with patch.dict(os.environ, {"ZENOTI_API_KEY": "test-key"}), patch("dotenv.load_dotenv"):
    from Zenoti.guest_form_retrieve import retrieve_guest_forms as guest
    from Zenoti.guest_form_retrieve import service_forms as service


def response(payload=None, status=200, headers=None):
    result = Mock(status_code=status, ok=status < 400, headers=headers or {})
    result.json.return_value = payload
    result.text = json.dumps(payload)
    result.url = "https://example.test"
    return result


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.output = io.StringIO()
        self.enterContext(redirect_stdout(self.output))

    def test_date_boundaries(self):
        self.assertEqual(
            list(guest.generate_date_chunks("2026-11-05", "2026-11-12")),
            [("2026-11-05", "2026-11-12"), ("2026-11-12", "2026-11-13")],
        )
        self.assertEqual(
            list(guest.generate_date_chunks("2026-11-05", "2026-11-05")),
            [("2026-11-05", "2026-11-06")],
        )
        self.assertEqual(
            service.date_windows("2026-10-01", "2026-10-10", 7),
            [("2026-10-01", "2026-10-07"), ("2026-10-08", "2026-10-10")],
        )
        self.assertEqual(
            service.date_windows("2026-10-01", "2026-10-01", 7),
            [("2026-10-01", "2026-10-01")],
        )
        with self.assertRaises(ValueError):
            list(guest.generate_date_chunks("2026-11-06", "2026-11-05"))
        with self.assertRaises(service.zc.ZConnectError):
            service.date_windows("2026-11-06", "2026-11-05", 7)

    def test_report_body_and_templates(self):
        args = SimpleNamespace(end_exclusive=False)
        body = service.build_body(None, "2026-11-05", "2026-11-06", args)
        self.assertEqual(body["centers"], {"ids": [], "is_all": True})
        self.assertEqual(body["start_date"], "2026-11-05 00:00:00")
        self.assertEqual(body["end_date"], "2026-11-06 23:59:59")
        for key in ("statuses", "submit_statuses", "appointment_statuses"):
            self.assertEqual(body[key], [-1])
        self.assertEqual(body["enabled_for_review"], ["-1"])
        self.assertEqual(
            service.build_body("c1", "2026-11-05", "2026-11-06", args)["centers"],
            {"ids": ["c1"], "is_all": False},
        )
        args.end_exclusive = True
        self.assertEqual(
            service.build_body("c1", "2026-11-05", "2026-11-06", args)["end_date"],
            "2026-11-07 00:00:00",
        )
        args.end_exclusive = False
        template = {"centers": ["{center_id}"], "range": {"from": "{start_date}", "to": "{end_datetime}"}, "n": 5}
        self.assertEqual(
            service.build_body("c1", "2026-11-05", "2026-11-06", args, template),
            {"centers": ["c1"], "range": {"from": "2026-11-05", "to": "2026-11-06 23:59:59"}, "n": 5},
        )
        self.assertEqual(template["centers"], ["{center_id}"])

    def test_response_wrappers(self):
        rows = [{"a": 1}]
        for payload in (rows, {"appointments": rows}, {"data": {"items": rows}}):
            self.assertEqual(guest.extract_appointments(payload), rows)
        self.assertEqual(service.extract_records(rows), (rows, "<root list>", {}))
        self.assertEqual(
            service.extract_records({"forms": rows, "review_stats": {"x": 1}}),
            (rows, "forms", {"review_stats": {"x": 1}}),
        )
        self.assertEqual(service.extract_records({"report": {"rows": rows}})[1], "report.rows")
        self.assertEqual(
            service.extract_records({"form_reviews": [], "review_stats": [{"count": 1}]})[1],
            "form_reviews",
        )
        self.assertEqual(service.extract_records({"msg": "none"})[0], [])

    def test_answers_exclude_signatures(self):
        raw = json.dumps({
            "signature": "data:image/png;base64,abc", "consent": True,
            "choices": {"yes": True, "no": False}, "list": ["a", "data:image/png;base64,abc"],
            "empty": None,
        })
        self.assertEqual(
            guest.parse_form_answers(raw),
            {"form_consent": "true", "form_choices": "yes", "form_list": "a", "form_empty": ""},
        )
        self.assertEqual(guest.parse_form_answers("invalid JSON"), {})

    def test_centers_pagination(self):
        centers = [
            {"id": f"c{index}", "code": f"C{index}", "name": f"Center {index}"}
            for index in range(3)
        ]
        with patch.object(guest, "PAGE_SIZE", 2), patch.object(guest, "api_get", side_effect=[{"centers": centers[:2]}, {"requested_centers": centers[2:]}]) as get, patch.object(guest.time, "sleep"):
            rows = guest.get_all_centers()
        self.assertEqual([row["center_id"] for row in rows], ["c0", "c1", "c2"])
        self.assertEqual([call.args[1]["page"] for call in get.call_args_list], [1, 2])
        self.assertTrue(all(call.args[0] == "/v1/centers" and call.args[1]["size"] == 2 for call in get.call_args_list))

    def test_appointments_use_one_request_without_paging(self):
        center = {"center_id": "c1", "center_code": "C1", "center_name": "Center"}
        appointments = [
            {"appointment_id": f"a{index}", "guest": {"id": f"g{index}"}}
            for index in range(3)
        ]
        with patch.object(guest, "PAGE_SIZE", 2), patch.object(guest, "api_get", return_value=appointments) as get:
            rows = guest.get_appointments(center, "2026-11-05", "2026-11-06")
        self.assertEqual([row["guest_id"] for row in rows], ["g0", "g1", "g2"])
        get.assert_called_once_with(
            "/v1/appointments", {"center_id": "c1", "start_date": "2026-11-05", "end_date": "2026-11-06"}
        )

    def test_request_logs_before_io_and_uses_configured_timeouts(self):
        def send(url, **kwargs):
            self.assertIn("GET /v1/centers page 1, attempt 1/", output.call_args.args[0])
            self.assertTrue(output.call_args.kwargs["flush"])
            self.assertEqual(kwargs["timeout"], (3.5, 12.5))
            return response({"centers": []})

        with patch.object(guest, "CONNECT_TIMEOUT", 3.5), patch.object(guest, "READ_TIMEOUT", 12.5), patch.object(guest.time, "monotonic", side_effect=[100, 101.5]), patch("builtins.print") as output, patch.object(guest.session, "get", side_effect=send):
            self.assertEqual(guest.api_get("/v1/centers", {"page": 1}), {"centers": []})
        self.assertIn("HTTP 200 for /v1/centers page 1 after 1.5s", output.call_args.args[0])

    def test_timeout_configuration_rejects_nonpositive_values(self):
        source = Path(guest.__file__).read_text(encoding="utf-8")
        for connect, read in (("0", "30"), ("10", "-1")):
            environment = {"ZENOTI_API_KEY": "test-key", "ZENOTI_CONNECT_TIMEOUT": connect, "ZENOTI_READ_TIMEOUT": read}
            namespace = {"__name__": "timeout_validation", "__package__": guest.__package__, "__file__": guest.__file__}
            with self.subTest(connect=connect, read=read), patch.dict(os.environ, environment), patch("dotenv.load_dotenv"), self.assertRaisesRegex(RuntimeError, "must be positive"):
                exec(compile(source, guest.__file__, "exec"), namespace)

    def test_guest_transient_retries(self):
        results = [
            response(status=429, headers={"Retry-After": "2"}),
            guest.requests.Timeout("timeout"), response(status=503), response({"centers": []}),
        ]
        with patch.object(guest.session, "get", side_effect=results) as get, patch.object(guest.time, "sleep") as sleep, patch.object(guest, "RETRY_BACKOFF_BASE", 5):
            self.assertEqual(guest.api_get("/v1/centers"), {"centers": []})
        self.assertEqual(get.call_count, 4)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [2, 10, 20])
        self.assertTrue(all(call.kwargs["timeout"] == (guest.CONNECT_TIMEOUT, guest.READ_TIMEOUT) for call in get.call_args_list))

    def test_guest_api_error_is_not_an_empty_form(self):
        error = {"error": {"code": "CT004", "message": "Invalid input"}}
        with patch.object(guest.session, "get", return_value=response(error)):
            with self.assertRaises(guest.zc.ZConnectError):
                guest.get_guest_forms("g1")
        errors = []
        with patch.object(guest, "get_guest_forms", side_effect=RuntimeError("request failed")), patch.object(guest.time, "sleep"):
            forms = guest.fetch_forms_for_guests(["g1"], errors)
        self.assertEqual(errors, [{"guest_id": "g1", "error": "request failed"}])
        self.assertFalse(forms["g1"]["is_form_submitted"])

    def test_guest_deduplication_preserves_missing_ids(self):
        rows = [
            {"appointment_id": "a1", "appointment_segment_id": "s1", "guest_id": "g1"},
            {"appointment_id": "a1", "appointment_segment_id": "s1", "guest_id": "g1"},
            {"appointment_id": None, "appointment_segment_id": None, "guest_id": "g2"},
            {"appointment_id": None, "appointment_segment_id": None, "guest_id": "g3"},
        ]
        forms = {
            key: {"is_form_submitted": True, "answers": {"form_guest": key}}
            for key in ("g1", "g2", "g3")
        }
        df = guest.build_dataframe(rows, forms)
        self.assertEqual(df["form_guest"].tolist(), ["g1", "g2", "g3"])

    def test_guest_empty_run_retains_errors(self):
        center = {"center_id": "c1", "center_name": "Center"}
        with patch.object(guest, "get_all_centers", return_value=[center]), patch.object(guest, "get_appointments", side_effect=RuntimeError("request failed")), patch.object(guest, "save_csv") as save, patch.object(guest.time, "sleep"):
            guest.main(["--start-date", "2026-11-05", "--end-date", "2026-11-05"])
        self.assertEqual(save.call_count, 1)
        self.assertEqual(save.call_args.args[0]["error"].tolist(), ["request failed"])
        self.assertIn("_errors_", save.call_args.args[1])

    def test_guest_csv_lock_fallback(self):
        df = Mock()
        df.to_csv.side_effect = [PermissionError("locked"), None]
        path = guest.save_csv(df, "export.csv")
        self.assertNotEqual(path, "export.csv")
        self.assertTrue(path.startswith("export_") and path.endswith(".csv"))
        self.assertTrue(all(call.kwargs["encoding"] == "utf-8-sig" for call in df.to_csv.call_args_list))

    def test_service_mapping_and_diagnostics(self):
        record = {
            "form_name": "Consent", "guest": {"code": "G1", "first_name": "Ann", "last_name": "Lee"},
            "Review_Status": False, "isSubmitted": True, "AppointmentDate": "2026-10-03",
            "invoice": {"no": "INV-9"}, "center": {"name": "Tribeca"}, "ServiceName": "Treatment",
            "provider_name": "Demo Provider", "expiry_date": None, "enableForReview": True,
            "job": "Injector", "appointment_status": "Closed", "_fallback_center_name": "fallback",
        }
        rows, coverage, sources = service.map_records([record, deepcopy(record)])
        self.assertEqual(list(rows[0]), service.OUTPUT_COLUMNS)
        self.assertEqual(rows[0]["Guest"], "Ann Lee")
        self.assertEqual(rows[0]["Guest Code"], "G1")
        self.assertEqual(rows[0]["Review Status"], "Not Reviewed")
        self.assertEqual(rows[0]["Submit Status"], "Submitted")
        self.assertEqual(rows[0]["Enable for Review"], "Yes")
        self.assertEqual(rows[0]["Service Center"], "Tribeca")
        self.assertEqual(rows[0]["Expiry Date"], "")
        self.assertEqual(coverage.counts["Form Name"], 2)
        service.report_mapping(2, coverage, sources)
        self.assertIn("Reviewed On", self.output.getvalue())
        self.assertEqual(service.map_record({"_fallback_center_name": "Fallback"})["Service Center"], "Fallback")
        aliases = service.map_record({"status": "Flagged for Review", "serviced_by": "Demo Provider"})
        self.assertEqual(aliases["Review Status"], "Flagged for Review")
        self.assertEqual(aliases["Provider Name"], "Demo Provider")
        self.assertEqual(len(service.map_records([{"form_id": "f1"}, {"form_id": "f2"}])[0]), 2)

    def test_service_pagination_checks_the_whole_page(self):
        first = [{"form_name": "Same"}, {"form_name": "First page"}]
        second = [{"form_name": "Same"}, {"form_name": "Second page"}]
        args = SimpleNamespace(page_size=2, no_expand=False, end_exclusive=False)
        with patch.object(service, "post_page", side_effect=[deepcopy(first), deepcopy(second), []]) as post:
            rows, pages, _ = service.fetch_window("https://example.test", {"id": None}, "2026-11-05", "2026-11-06", args, None, {})
        self.assertEqual(len(rows), 4)
        self.assertEqual(pages, 2)
        self.assertEqual(post.call_count, 3)
        with patch.object(service, "post_page", side_effect=[deepcopy(first), deepcopy(first)]) as post:
            rows, pages, _ = service.fetch_window("https://example.test", {"id": None}, "2026-11-05", "2026-11-06", args, None, {})
        self.assertEqual(len(rows), 2)
        self.assertEqual(pages, 1)
        self.assertEqual(post.call_count, 2)

    def test_service_retries_and_auth_fallback(self):
        state = {"mode": "web", "header": "Bearer old", "tried": {"web"}, "allow_fallback": True}
        results = [
            response(status=429, headers={"Retry-After": "2"}), service.requests.Timeout("timeout"),
            response(status=403), response(status=403), response(status=401), response({"forms": []}),
        ]
        with patch.object(service.requests, "post", side_effect=results) as post, patch.object(service.time, "sleep") as sleep, patch.object(service, "_available", return_value=True), patch.object(service, "_browser_login", return_value="fresh"):
            self.assertEqual(service.post_page("https://example.test", {}, {}, state), {"forms": []})
        self.assertEqual(state["header"], "Bearer fresh")
        self.assertEqual(post.call_count, 6)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [2, 10, 5, 10])
        self.assertEqual(post.call_args.kwargs["headers"]["Authorization"], "Bearer fresh")

    def test_service_forbidden_is_bounded(self):
        state = {"mode": "login", "header": "Bearer fresh", "tried": {"web", "login"}, "allow_fallback": False}
        with patch.object(service.requests, "post", side_effect=[response(status=403) for _ in range(3)]) as post, patch.object(service.time, "sleep"):
            with self.assertRaises(service.zc.ZConnectError):
                service.post_page("https://example.test", {}, {}, state)
        self.assertEqual(post.call_count, 3)

    def test_missing_playwright_has_a_specific_error(self):
        missing = ModuleNotFoundError("No module named 'playwright'", name="playwright")
        with patch.object(service, "load_zconnect_module", side_effect=missing):
            with self.assertRaisesRegex(service.zc.ZConnectError, "dependency 'playwright'"):
                service.make_auth("login")


if __name__ == "__main__":
    unittest.main()
