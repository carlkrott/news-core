"""Run 9 disposable synthetic end-to-end rehearsal tests."""
from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from news_pipeline.synthetic_rehearsal import main, run_rehearsal

AS_OF = "2026-09-20T12:00:00Z"


class SyntheticRehearsalTests(unittest.TestCase):
    def test_first_replay_material_change_and_zero_delivery(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result = run_rehearsal(Path(temporary) / "run", as_of_utc=AS_OF)
        self.assertEqual(result.first_report_events, 1)
        self.assertEqual(result.first_subject_reports, 7)
        self.assertEqual(result.replay_event_versions_added, 0)
        self.assertEqual(result.replay_reports_added, 0)
        self.assertEqual(result.replay_subject_reports_added, 0)
        self.assertEqual(result.material_event_versions_added, 1)
        self.assertEqual(result.material_subject_reports_added, 7)
        self.assertEqual(result.updated_subjects, ("ai",))
        self.assertEqual(result.subject_delivery_attempts, 0)
        self.assertEqual(result.legacy_delivery_attempts, 0)
        self.assertEqual(result.live_delivery_attempts, 0)

    def test_negative_inputs_never_enter_reports_or_subject_outbox(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result = run_rehearsal(Path(temporary) / "run", as_of_utc=AS_OF)
        self.assertEqual(result.stale_rejection_count, 1)
        self.assertEqual(result.unverified_event_versions, 1)
        self.assertEqual(result.contaminated_model_fallbacks, 1)
        self.assertEqual(result.negative_report_event_count, 0)
        self.assertEqual(result.negative_subject_report_count, 0)
        self.assertEqual(result.delivery_state_counts["legacy_reports"], {"dry_run": 2})
        self.assertEqual(
            result.delivery_state_counts["subject_outbox"],
            {"prepared": 2, "skipped": 12},
        )

    def test_investigation_and_public_reports_are_replay_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as first_dir, tempfile.TemporaryDirectory() as second_dir:
            first = run_rehearsal(Path(first_dir) / "run", as_of_utc=AS_OF)
            second = run_rehearsal(Path(second_dir) / "run", as_of_utc=AS_OF)
        self.assertGreater(first.investigation_transport_calls_first, 0)
        self.assertEqual(
            first.investigation_transport_calls_after_replay,
            first.investigation_transport_calls_first,
        )
        self.assertEqual(first.public_quality_json, second.public_quality_json)
        public = json.loads(first.public_quality_json)
        encoded = first.public_quality_json
        self.assertEqual(public["schema"], "news-quality-audit-v1")
        for forbidden in (
            "https://", "fixture.example", "recipient", "message_ids",
            "Widget v2", "state.db", str(first_dir), str(second_dir),
        ):
            self.assertNotIn(forbidden, encoded)
        self.assertEqual(public["metrics"]["delivered_event_version_repeat_count"], 0)

    def test_cli_writes_receipt_and_refuses_nonempty_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "run"
            stdout, stderr = io.StringIO(), io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = main(["--work-root", str(root), "--as-of-utc", AS_OF])
            self.assertEqual(code, 0)
            self.assertEqual(stderr.getvalue(), "")
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["status"], "PASS")
            self.assertTrue((root / "rehearsal-receipt.json").is_file())
            self.assertTrue((root / "quality-report.json").is_file())
            self.assertFalse((root / "state.db-wal").exists())
            self.assertFalse((root / "state.db-shm").exists())
            before = (root / "rehearsal-receipt.json").read_bytes()

            stdout, stderr = io.StringIO(), io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = main(["--work-root", str(root), "--as-of-utc", AS_OF])
            self.assertNotEqual(code, 0)
            self.assertEqual(stdout.getvalue(), "")
            self.assertEqual(stderr.getvalue(), "synthetic rehearsal failed\n")
            self.assertEqual(before, (root / "rehearsal-receipt.json").read_bytes())

    def test_mid_rehearsal_failure_closes_database_and_checkpoints(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "run"
            with patch(
                "news_pipeline.synthetic_rehearsal._editorial_transport",
                side_effect=RuntimeError("synthetic failure"),
            ):
                with self.assertRaises(RuntimeError):
                    run_rehearsal(root, as_of_utc=AS_OF)
            self.assertTrue((root / "state.db").is_file())
            self.assertFalse((root / "state.db-wal").exists())
            self.assertFalse((root / "state.db-shm").exists())


if __name__ == "__main__":
    unittest.main()
