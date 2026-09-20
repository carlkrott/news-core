"""Focused Run 8 generation/delivery queue-separation tests."""
from __future__ import annotations

import io
import json
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from news_container import ALLOWED_KINDS, DELIVERY_KIND_FORBIDDEN
from news_pipeline import jobs


class Run8QueueSplitTests(unittest.TestCase):
    def test_parser_separates_report_and_operator_delivery_arguments(self) -> None:
        report_argv = [
            "daily-report",
            "--db", "/tmp/state.db",
            "--artifact-root", "/tmp/artifacts",
            "--as-of-utc", "2026-09-20T12:00:00Z",
        ]
        report = jobs._parser().parse_args(report_argv)
        self.assertEqual(report.job, "daily-report")
        self.assertFalse(hasattr(report, "enable_live_delivery"))
        with self.assertRaises(SystemExit):
            jobs._parser().parse_args(report_argv + ["--enable-live-delivery"])

        delivery = jobs._parser().parse_args(
            [
                "daily-deliver",
                "--db", "/tmp/state.db",
                "--artifact-root", "/tmp/artifacts",
                "--report-id", "report-1",
            ]
        )
        self.assertEqual(delivery.job, "daily-deliver")
        self.assertEqual(delivery.report_id, "report-1")
        self.assertFalse(delivery.enable_live_delivery)

    def test_report_generation_job_never_calls_delivery(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = root / "state.db"
            db.touch()
            artifacts = root / "artifacts"
            artifacts.mkdir()
            args = Namespace(
                job="daily-report",
                db=str(db),
                artifact_root=str(artifacts),
                as_of_utc="2026-09-20T12:00:00Z",
                prior_upper_utc=None,
            )
            report_result = SimpleNamespace(
                report_id="report-1",
                generation_status="complete",
                was_replayed=False,
            )
            output = io.StringIO()
            with (
                patch(
                    "news_pipeline.report_builder.run_report",
                    return_value=report_result,
                ),
                patch(
                    "news_pipeline.delivery.deliver_report",
                    side_effect=AssertionError("generation called delivery"),
                ) as deliver,
                patch.object(jobs, "_resolve_report_prior", return_value=None),
                redirect_stdout(output),
            ):
                self.assertEqual(jobs._run_daily_report(args), 0)
            deliver.assert_not_called()
            payload = json.loads(output.getvalue())
            self.assertEqual(payload["report_id"], "report-1")
            self.assertEqual(payload["delivery_state"], "not_attempted")
            self.assertFalse(payload["network_used"])
            self.assertEqual(payload["message_count"], 0)

    def test_operator_delivery_job_never_generates_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = root / "state.db"
            db.touch()
            artifacts = root / "artifacts"
            artifacts.mkdir()
            args = Namespace(
                job="daily-deliver",
                db=str(db),
                artifact_root=str(artifacts),
                report_id="report-1",
                config=None,
                telegram_api_base="https://api.telegram.org",
                enable_live_delivery=False,
                retry_failed=False,
            )
            delivery_result = SimpleNamespace(
                report_id="report-1",
                state="dry_run",
                network_used=False,
                replayed=False,
                message_ids=(),
            )
            output = io.StringIO()
            with (
                patch(
                    "news_pipeline.delivery.deliver_report",
                    return_value=delivery_result,
                ) as deliver,
                patch(
                    "news_pipeline.report_builder.run_report",
                    side_effect=AssertionError("delivery called generation"),
                ) as generate,
                redirect_stdout(output),
            ):
                self.assertEqual(jobs._run_daily_deliver(args), 0)
            generate.assert_not_called()
            deliver.assert_called_once_with(
                str(db),
                str(artifacts),
                "report-1",
                enable_live=False,
                config_path=None,
                api_base="https://api.telegram.org",
                retry_failed=False,
            )
            payload = json.loads(output.getvalue())
            self.assertEqual(payload["job"], "daily-deliver")
            self.assertEqual(payload["delivery_state"], "dry_run")
            self.assertFalse(payload["network_used"])

    def test_public_worker_vocabulary_still_forbids_delivery(self) -> None:
        self.assertEqual(DELIVERY_KIND_FORBIDDEN, "delivery")
        self.assertNotIn("delivery", ALLOWED_KINDS)

    def test_daily_report_wrapper_contains_no_delivery_activation(self) -> None:
        wrapper = (
            Path(__file__).resolve().parents[2] / "bin" / "news-daily-report"
        ).read_text(encoding="utf-8")
        self.assertNotIn("NEWS_PHASE6_ENABLE_LIVE_DELIVERY", wrapper)
        self.assertNotIn("NEWS_PHASE6_RETRY_FAILED", wrapper)
        self.assertNotIn("ZEROCLAW_CONFIG", wrapper)
        self.assertNotIn("telegram-api-base", wrapper)


if __name__ == "__main__":
    unittest.main()
