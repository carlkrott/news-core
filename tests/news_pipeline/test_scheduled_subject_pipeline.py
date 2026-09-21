from __future__ import annotations

import hashlib
import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from news_pipeline import jobs, report_builder
from news_pipeline.db import init_db
from news_pipeline.delivery_schema_v6 import migrate_v6
from news_pipeline.event_store import process_phase4
from news_pipeline.live_contracts import SourceRole
from news_pipeline.provenance import PublisherRule
from news_pipeline.quality_audit import audit_database
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5
from news_pipeline.schema_v7 import migrate_v7
from news_pipeline.schema_v8 import migrate_v8
from news_pipeline.schema_v9 import migrate_v9
from news_pipeline.schema_v10 import migrate_v10


AS_OF = "2026-09-08T09:00:00Z"
EVENT_AT = "2026-09-08T06:00:00Z"
APPLIED_AT = "2026-09-07T00:00:00Z"


class ScheduledSubjectPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.db_path = self.root / "state.db"
        self.artifact_root = self.root / "artifacts"
        self.artifact_root.mkdir()
        init_db(str(self.db_path))
        connection = sqlite3.connect(self.db_path, isolation_level=None)
        connection.execute("PRAGMA foreign_keys=ON")
        migrate_v3(connection, APPLIED_AT)
        migrate_v4(connection, APPLIED_AT)
        migrate_v5(connection, APPLIED_AT)
        migrate_v6(connection, APPLIED_AT)
        connection.execute(
            "INSERT INTO runs VALUES (?,?,?,?,?,?,?)",
            (
                "source-run",
                APPLIED_AT,
                None,
                "historical_replay",
                "observed_historical",
                None,
                None,
            ),
        )
        connection.execute(
            "INSERT INTO source_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "source-registry",
                "rss",
                "primary",
                "authority.example",
                '["ai"]',
                1,
                "[]",
                "[]",
                "[]",
                "[]",
                "[]",
                60,
                None,
                None,
                None,
                "a" * 64,
                APPLIED_AT,
            ),
        )
        migrate_v7(
            connection,
            APPLIED_AT,
            rules=(
                PublisherRule(
                    rule_id="authority-rule",
                    host="authority.example",
                    source_role=SourceRole.PRIMARY,
                    independence_group="authority",
                    categories=("ai",),
                    authority_entities=("Widget",),
                    audit_note="sanitized scheduled-path fixture",
                ),
            ),
        )
        migrate_v8(connection, APPLIED_AT)
        migrate_v9(connection, APPLIED_AT)
        migrate_v10(connection, APPLIED_AT)
        connection.execute(
            """INSERT INTO source_items(
                   source_item_id,source_id,external_id,category,original_url,canonical_url,
                   publisher,source_role,author_handle,retrieval_method,raw_content_hash,title,
                   body,raw,retrieved_at,published_at,updated_at,publication_evidence)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "primary-item",
                "source-registry",
                "primary-external",
                "ai",
                "https://authority.example/widget-v2",
                "https://authority.example/widget-v2",
                "Widget",
                "primary",
                None,
                "rss",
                "b" * 64,
                "Widget v2.0",
                "Widget v2.0 launched",
                "Widget v2.0 launched",
                EVENT_AT,
                EVENT_AT,
                None,
                "source",
            ),
        )
        connection.execute(
            "INSERT INTO decisions VALUES (?,?,?,?,?,?,?)",
            (
                "decision-primary",
                "source-run",
                None,
                "keep",
                json.dumps(
                    {
                        "source_item_id": "primary-item",
                        "matched_observation_ids": [],
                    }
                ),
                "2026-09-08T06:01:00Z",
                "phase3",
            ),
        )
        connection.close()
        phase4 = process_phase4(str(self.db_path), "2026-09-08T06:02:00Z")
        self.assertEqual(phase4.versions_appended, 1)

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _run_report(self) -> dict[str, object]:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = jobs.main(
                [
                    "daily-report",
                    "--db",
                    str(self.db_path),
                    "--artifact-root",
                    str(self.artifact_root),
                    "--as-of-utc",
                    AS_OF,
                ]
            )
        self.assertEqual(exit_code, 0, stdout.getvalue())
        return json.loads(stdout.getvalue())

    def test_scheduled_report_prepares_each_subject_without_delivery(self) -> None:
        payload = self._run_report()
        self.assertEqual(payload["generation_status"], "complete")
        self.assertEqual(payload["delivery_state"], "not_attempted")

        connection = sqlite3.connect(self.db_path)
        try:
            rows = connection.execute(
                """SELECT sr.subject_id,sr.story_count,o.state
                     FROM subject_reports sr
                     JOIN subject_delivery_outbox o USING(subject_report_id)
                     ORDER BY sr.subject_id"""
            ).fetchall()
            self.assertEqual(
                rows,
                [
                    ("ai", 1, "prepared"),
                    ("audio_engineering", 0, "skipped"),
                    ("fantasy_novel", 0, "skipped"),
                    ("hardware", 0, "skipped"),
                    ("our_setup", 0, "skipped"),
                    ("professional_av", 0, "skipped"),
                    ("world", 0, "skipped"),
                ],
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM subject_delivery_attempts"
                ).fetchone()[0],
                0,
            )
            report_rows = connection.execute(
                """SELECT subject_report_id,parent_report_id,subject_id,
                          content_sha256,story_count,created_at
                     FROM subject_reports ORDER BY subject_id"""
            ).fetchall()
            generation_rows = connection.execute(
                """SELECT sr.subject_id,g.mode,g.model_call_count,
                          g.cache_hit_count,g.fallback_count,g.malformed_count,
                          g.transport_error_count
                     FROM subject_reports sr
                     JOIN subject_generation_receipts g USING(subject_report_id)
                     ORDER BY sr.subject_id"""
            ).fetchall()
            self.assertEqual(
                generation_rows,
                [
                    ("ai", "fallback", 1, 0, 1, 0, 1),
                    ("audio_engineering", "empty", 0, 0, 0, 0, 0),
                    ("fantasy_novel", "empty", 0, 0, 0, 0, 0),
                    ("hardware", "empty", 0, 0, 0, 0, 0),
                    ("our_setup", "empty", 0, 0, 0, 0, 0),
                    ("professional_av", "empty", 0, 0, 0, 0, 0),
                    ("world", "empty", 0, 0, 0, 0, 0),
                ],
            )
        finally:
            connection.close()

        artifact_paths = sorted(
            (self.artifact_root / "subject-reports").glob("*.json")
        )
        self.assertEqual(len(artifact_paths), 7)
        artifacts: dict[str, dict[str, object]] = {}
        for path in artifact_paths:
            artifact = json.loads(path.read_text(encoding="utf-8"))
            artifacts[str(artifact["subject_report_id"])] = artifact
        for report_id, parent_id, subject_id, content_hash, count, created_at in report_rows:
            artifact = artifacts[report_id]
            self.assertEqual(
                set(artifact),
                {
                    "schema",
                    "subject_report_id",
                    "parent_report_id",
                    "subject_id",
                    "content_sha256",
                    "story_count",
                    "created_at",
                    "chunks",
                    "generation",
                },
            )
            self.assertEqual(artifact["schema"], "news-subject-report-v1")
            self.assertEqual(artifact["parent_report_id"], parent_id)
            self.assertEqual(artifact["subject_id"], subject_id)
            self.assertEqual(artifact["story_count"], count)
            self.assertEqual(artifact["created_at"], created_at)
            chunks = artifact["chunks"]
            self.assertIsInstance(chunks, list)
            rendered = "\n\n".join(chunks)
            self.assertEqual(
                hashlib.sha256(rendered.encode("utf-8")).hexdigest(),
                content_hash,
            )
            self.assertEqual(bool(chunks), count > 0)
            expected_generation = (
                {
                    "mode": "fallback",
                    "model_call_count": 1,
                    "cache_hit_count": 0,
                    "fallback_count": 1,
                    "malformed_count": 0,
                    "transport_error_count": 1,
                }
                if subject_id == "ai"
                else {
                    "mode": "empty",
                    "model_call_count": 0,
                    "cache_hit_count": 0,
                    "fallback_count": 0,
                    "malformed_count": 0,
                    "transport_error_count": 0,
                }
            )
            self.assertEqual(artifact["generation"], expected_generation)

        audit = audit_database(self.db_path)
        self.assertEqual(audit.model_fallback_count, 1)
        self.assertEqual(audit.model_malformed_count, 0)
        self.assertEqual(audit.transport_error_count, 1)
        self.assertNotIn("model_fallback_count", audit.unavailable_metrics)
        self.assertNotIn("model_malformed_count", audit.unavailable_metrics)
        self.assertNotIn("transport_error_count", audit.unavailable_metrics)
        self.assertEqual(audit.sources["model_fallback_count"], "db")
        self.assertEqual(audit.sources["model_malformed_count"], "db")
        self.assertEqual(audit.sources["transport_error_count"], "db")

    def test_complete_parent_replay_repairs_missing_subject_state(self) -> None:
        first = self._run_report()
        connection = sqlite3.connect(self.db_path, isolation_level=None)
        try:
            connection.execute("DELETE FROM subject_generation_receipts")
            connection.execute("DELETE FROM subject_delivery_outbox")
            connection.execute("DELETE FROM subject_reports")
        finally:
            connection.close()

        replay = self._run_report()
        self.assertEqual(replay["report_id"], first["report_id"])
        self.assertTrue(replay["was_replayed"])
        connection = sqlite3.connect(self.db_path)
        try:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM reports").fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM subject_reports"
                ).fetchone()[0],
                7,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM subject_delivery_attempts"
                ).fetchone()[0],
                0,
            )
        finally:
            connection.close()

    def test_parent_completion_failure_replays_from_precommitted_subject_artifacts(self) -> None:
        with patch.object(
            report_builder,
            "_persist_subject_reports",
            side_effect=RuntimeError("simulated post-parent persistence failure"),
            create=True,
        ):
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = jobs.main(
                    [
                        "daily-report",
                        "--db",
                        str(self.db_path),
                        "--artifact-root",
                        str(self.artifact_root),
                        "--as-of-utc",
                        AS_OF,
                    ]
                )
        self.assertEqual(exit_code, 1, stdout.getvalue())
        self.assertIn("simulated post-parent persistence failure", stdout.getvalue())
        self.assertEqual(
            len(list((self.artifact_root / "subject-reports").glob("*.json"))),
            7,
        )
        connection = sqlite3.connect(self.db_path)
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT generation_status FROM reports"
                ).fetchone(),
                ("complete",),
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM subject_reports").fetchone()[0],
                0,
            )
        finally:
            connection.close()

        replay = self._run_report()
        self.assertTrue(replay["was_replayed"])
        connection = sqlite3.connect(self.db_path)
        try:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM subject_reports").fetchone()[0],
                7,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM subject_generation_receipts"
                ).fetchone()[0],
                7,
            )
        finally:
            connection.close()


    def test_first_subject_artifact_failure_replays_from_durable_bundle(self) -> None:
        import news_pipeline.subject_artifacts as subject_artifacts

        original = subject_artifacts.write_subject_artifact
        calls = 0

        def fail_first(*args: object, **kwargs: object) -> object:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("simulated first subject artifact failure")
            return original(*args, **kwargs)

        stdout = io.StringIO()
        with (
            patch.object(subject_artifacts, "write_subject_artifact", side_effect=fail_first),
            redirect_stdout(stdout),
        ):
            exit_code = jobs.main(
                [
                    "daily-report",
                    "--db", str(self.db_path),
                    "--artifact-root", str(self.artifact_root),
                    "--as-of-utc", AS_OF,
                ]
            )
        self.assertEqual(exit_code, 1, stdout.getvalue())
        self.assertIn("simulated first subject artifact failure", stdout.getvalue())

        replay = self._run_report()
        self.assertEqual(replay["generation_status"], "complete")
        self.assertEqual(
            len(list((self.artifact_root / "subject-reports").glob("*.json"))),
            7,
        )

    def test_shadow_completion_failure_replays_from_durable_bundle(self) -> None:
        original = report_builder.BriefingLedger.complete_run
        calls = 0

        def fail_first_completion(
            ledger: object, *args: object, **kwargs: object
        ) -> object:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("simulated shadow completion failure")
            return original(ledger, *args, **kwargs)

        stdout = io.StringIO()
        with (
            patch.object(
                report_builder.BriefingLedger,
                "complete_run",
                side_effect=fail_first_completion,
                autospec=True,
            ),
            redirect_stdout(stdout),
        ):
            exit_code = jobs.main(
                [
                    "daily-report",
                    "--db", str(self.db_path),
                    "--artifact-root", str(self.artifact_root),
                    "--as-of-utc", AS_OF,
                ]
            )
        self.assertEqual(exit_code, 1, stdout.getvalue())
        self.assertIn("simulated shadow completion failure", stdout.getvalue())
        self.assertEqual(
            len(list((self.artifact_root / "subject-bundles").glob("*.json"))),
            1,
        )
        connection = sqlite3.connect(self.db_path)
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM shadow_briefing_runs"
                ).fetchone(),
                ("RUNNING",),
            )
        finally:
            connection.close()

        replay = self._run_report()
        self.assertEqual(replay["generation_status"], "complete")

    def test_partial_subject_artifact_publication_replays_exact_bundle(self) -> None:
        import news_pipeline.subject_artifacts as subject_artifacts

        original = subject_artifacts.write_subject_artifact
        calls = 0

        def fail_second(*args: object, **kwargs: object) -> object:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("simulated partial subject artifact failure")
            return original(*args, **kwargs)

        stdout = io.StringIO()
        with (
            patch.object(subject_artifacts, "write_subject_artifact", side_effect=fail_second),
            redirect_stdout(stdout),
        ):
            exit_code = jobs.main(
                [
                    "daily-report",
                    "--db", str(self.db_path),
                    "--artifact-root", str(self.artifact_root),
                    "--as-of-utc", AS_OF,
                ]
            )
        self.assertEqual(exit_code, 1, stdout.getvalue())
        self.assertIn("simulated partial subject artifact failure", stdout.getvalue())
        self.assertEqual(
            len(list((self.artifact_root / "subject-reports").glob("*.json"))),
            1,
        )

        replay = self._run_report()
        self.assertEqual(replay["generation_status"], "complete")
        self.assertEqual(
            len(list((self.artifact_root / "subject-reports").glob("*.json"))),
            7,
        )

    def test_unrelated_corrupt_subject_artifact_does_not_block_report(self) -> None:
        directory = self.artifact_root / "subject-reports"
        directory.mkdir()
        (directory / ("subject-report-" + "f" * 64 + ".json")).write_text(
            "{}", encoding="utf-8"
        )

        payload = self._run_report()
        self.assertEqual(payload["generation_status"], "complete")

    def test_daily_report_requires_schema_v10(self) -> None:
        connection = sqlite3.connect(self.db_path, isolation_level=None)
        try:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("DROP TABLE subject_generation_receipts")
            connection.execute("DELETE FROM schema_migrations WHERE version=10")
        finally:
            connection.close()

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = jobs.main(
                [
                    "daily-report",
                    "--db", str(self.db_path),
                    "--artifact-root", str(self.artifact_root),
                    "--as-of-utc", AS_OF,
                ]
            )
        self.assertEqual(exit_code, 1, stdout.getvalue())
        self.assertIn("schema v10 migration is required", stdout.getvalue())
        connection = sqlite3.connect(self.db_path)
        try:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM reports").fetchone()[0],
                0,
            )
        finally:
            connection.close()

    def test_bundle_write_failure_remains_running_and_replays(self) -> None:
        import news_pipeline.subject_artifact_bundle as subject_artifact_bundle

        original = subject_artifact_bundle.write_subject_artifact_bundle
        calls = 0

        def fail_first_bundle(*args: object, **kwargs: object) -> object:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError("simulated bundle write failure")
            return original(*args, **kwargs)

        stdout = io.StringIO()
        with (
            patch.object(
                subject_artifact_bundle,
                "write_subject_artifact_bundle",
                side_effect=fail_first_bundle,
            ),
            redirect_stdout(stdout),
        ):
            exit_code = jobs.main(
                [
                    "daily-report",
                    "--db", str(self.db_path),
                    "--artifact-root", str(self.artifact_root),
                    "--as-of-utc", AS_OF,
                ]
            )
        self.assertEqual(exit_code, 1, stdout.getvalue())
        connection = sqlite3.connect(self.db_path)
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM shadow_briefing_runs"
                ).fetchone(),
                ("RUNNING",),
            )
        finally:
            connection.close()
        self.assertFalse((self.artifact_root / "subject-bundles").exists())

        replay = self._run_report()
        self.assertEqual(replay["generation_status"], "complete")

    def test_bundle_story_counts_must_match_shadow_events(self) -> None:
        from news_pipeline.models import Subject
        from news_pipeline.subject_artifacts import (
            SubjectGeneration,
            build_subject_artifact_payload,
        )

        self._run_report()
        bundle_path = next((self.artifact_root / "subject-bundles").glob("*.json"))
        payload = json.loads(bundle_path.read_text(encoding="utf-8"))
        ai_index = next(
            index
            for index, artifact in enumerate(payload["artifacts"])
            if artifact["subject_id"] == "ai"
        )
        artifact = payload["artifacts"][ai_index]
        payload["artifacts"][ai_index] = build_subject_artifact_payload(
            parent_report_id=artifact["parent_report_id"],
            subject=Subject.AI,
            chunks=tuple(artifact["chunks"]),
            story_count=2,
            created_at=artifact["created_at"],
            generation=SubjectGeneration.from_mapping(artifact["generation"]),
        )
        bundle_path.write_text(
            json.dumps(
                payload,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = jobs.main(
                [
                    "daily-report",
                    "--db", str(self.db_path),
                    "--artifact-root", str(self.artifact_root),
                    "--as-of-utc", AS_OF,
                ]
            )
        self.assertEqual(exit_code, 1, stdout.getvalue())
        self.assertIn("story counts conflict with shadow events", stdout.getvalue())

if __name__ == "__main__":
    unittest.main()
