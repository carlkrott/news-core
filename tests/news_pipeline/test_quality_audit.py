"""Focused Run 9 read-only quality audit tests."""
from __future__ import annotations

import hashlib
import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from news_pipeline.db import init_db
from news_pipeline.delivery_schema_v6 import migrate_v6
from news_pipeline.quality_audit import (
    RECEIPT_FIELDS,
    ReceiptMetrics,
    audit_database,
    main,
    render_public_report,
)
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5
from news_pipeline.schema_v7 import migrate_v7
from news_pipeline.schema_v8 import migrate_v8
from news_pipeline.schema_v9 import migrate_v9

TS = "2026-09-20T12:00:00Z"


def _receipt() -> dict[str, int]:
    return {
        "candidates_returned": 5,
        "canonical_url_total": 5,
        "canonical_url_covered": 3,
        "canonical_url_missing": 1,
        "canonical_url_non_article": 1,
        "date_evidence_total": 5,
        "date_evidence_covered": 4,
        "date_evidence_source": 2,
        "date_evidence_metadata": 2,
        "date_evidence_missing": 1,
        "date_evidence_unparseable": 0,
        "stale_rejection_count": 2,
        "exact_duplicate_count": 1,
        "rewrite_count": 1,
        "distinct_event_count": 1,
        "material_update_count": 1,
        "subject_relevance_reject_count": 1,
        "cross_subject_collision_count": 1,
        "delivered_event_version_repeat_count": 1,
        "model_fallback_count": 1,
        "model_malformed_count": 1,
        "query_error_count": 0,
        "transport_error_count": 1,
    }


def _build_db(path: Path) -> None:
    init_db(str(path))
    connection = sqlite3.connect(path, isolation_level=None)
    connection.execute("PRAGMA foreign_keys=ON")
    migrate_v3(connection, TS)
    migrate_v4(connection, TS)
    migrate_v5(connection, TS)
    migrate_v6(connection, TS)
    migrate_v7(connection, TS)
    migrate_v8(connection, TS)
    migrate_v9(connection, TS)
    connection.execute(
        "INSERT INTO runs VALUES (?,?,?,?,?,?,?)",
        ("run-a", TS, TS, "historical_replay", "observed_historical", None, None),
    )
    connection.execute(
        "INSERT INTO source_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "source-a", "rss", "primary", "fixture.example", '["ai"]', 1,
            "[]", "[]", "[]", "[]", "[]", 60, None, None, None,
            "a" * 64, TS,
        ),
    )
    connection.execute(
        """INSERT INTO query_plans(
               query_plan_id,source_id,query_text,category,topic,entity,
               reason_selected,cooldown_seconds,max_rounds,created_at)
           VALUES(?,?,?,?,?,?,?,?,?,?)""",
        ("plan-a", "source-a", "fixture query", "ai", None, None, "fixture", 0, 1, TS),
    )
    connection.execute(
        """INSERT INTO query_attempts(
               attempt_id,query_plan_id,status,started_at,finished_at,returned_count,
               novel_count,verified_count,duplicate_count,stale_count,error_count,error,
               rate_limit_reset_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        ("attempt-a", "plan-a", "partial", TS, TS, 5, 2, 1, 1, 2, 1, "synthetic", None),
    )
    connection.execute(
        """INSERT INTO source_items(
               source_item_id,source_id,external_id,category,original_url,canonical_url,
               publisher,source_role,author_handle,retrieval_method,raw_content_hash,title,
               body,raw,retrieved_at,published_at,updated_at,publication_evidence)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            "item-a", "source-a", "external-a", "ai", "https://fixture.example/a",
            "https://fixture.example/a", "Fixture", "primary", None, "rss",
            "b" * 64, "Fixture title", "Fixture body", "Fixture raw", TS, TS, None, "source",
        ),
    )
    for index, semantic in enumerate(("rewrite", "distinct_event", "material_update"), start=1):
        connection.execute(
            "INSERT INTO decisions VALUES (?,?,?,?,?,?,?)",
            (
                f"decision-{index}", "run-a", None, "keep",
                json.dumps({"source_item_id": "item-a", "semantic_decision": semantic}),
                TS, "phase3",
            ),
        )
    connection.execute(
        """INSERT INTO investigations(
               investigation_id,candidate_id,feed_lane_id,query_plan_id,category,round_number,
               state,terminal_state,targeted_query_ids_json,created_at,updated_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        ("investigation-a", "item-a", "lane-a", "plan-a", "ai", 0, "pending", None, "[]", TS, TS),
    )
    connection.execute(
        """INSERT INTO source_item_provenance(
               source_item_id,normalized_publisher_host,effective_source_role,
               independence_group,matched_rule_id,authority_match,
               classification_timestamp,classification_reason)
           VALUES(?,?,?,?,?,?,?,?)""",
        ("item-a", "fixture.example", "discovery", "unknown", None, 0, TS, "unknown_publisher"),
    )
    connection.execute(
        "INSERT INTO events VALUES (?,?,?,?,?,?,?,?)",
        ("event-a", "run-a", "ai", TS, None, 0, 0, "complete"),
    )
    connection.execute(
        """INSERT INTO event_versions(
               event_id,version,material_change_reason,summary,verification_state,
               valid_from,superseded_at,verified_at)
           VALUES(?,?,?,?,?,?,?,?)""",
        ("event-a", 1, "distinct", "Fixture summary", "verified", TS, None, TS),
    )
    connection.execute(
        """INSERT INTO reports(
               report_id,window_start,window_end,generation_status,json_sha256,jsonl_sha256,
               markdown_sha256,manifest_sha256,delivery_state,delivery_id,created_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        ("report-a", "2026-09-13T07:00:00Z", "2026-09-20T07:00:00Z", "complete",
         "1" * 64, "2" * 64, "3" * 64, "4" * 64, "dry_run", None, TS),
    )
    connection.execute(
        "INSERT INTO report_events VALUES (?,?,?,?,?,?)",
        ("report-a", "event-a", 1, "ai", 0, "verified_event"),
    )
    connection.execute(
        """INSERT INTO report_deliveries(
               report_id,idempotency_key,channel,recipient_hash,content_sha256,state,
               current_attempt_id,created_at,updated_at)
           VALUES(?,?,?,?,?,?,?,?,?)""",
        ("report-a", "5" * 64, "telegram", "6" * 64, "3" * 64, "skipped", None, TS, TS),
    )
    connection.execute(
        """INSERT INTO subject_reports(
               subject_report_id,parent_report_id,subject_id,revision,content_sha256,
               story_count,created_at)
           VALUES(?,?,?,?,?,?,?)""",
        ("subject-a", "report-a", "ai", 1, "7" * 64, 1, TS),
    )
    connection.execute(
        """INSERT INTO subject_delivery_outbox(
               subject_report_id,idempotency_key,channel,recipient_hash,content_sha256,state,
               current_attempt_id,created_at,updated_at)
           VALUES(?,?,?,?,?,?,?,?,?)""",
        ("subject-a", "8" * 64, None, None, "7" * 64, "prepared", None, TS, TS),
    )
    connection.close()


class ReceiptMetricsTests(unittest.TestCase):
    def test_exact_shape_and_non_negative_values(self) -> None:
        metrics = ReceiptMetrics.from_mapping(_receipt())
        self.assertEqual(set(metrics.to_mapping()), set(RECEIPT_FIELDS))
        with self.assertRaisesRegex(ValueError, "exact keys"):
            ReceiptMetrics.from_mapping({**_receipt(), "unknown": 1})
        bad = _receipt()
        bad["rewrite_count"] = -1
        with self.assertRaisesRegex(ValueError, "non-negative"):
            ReceiptMetrics.from_mapping(bad)


class QualityAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "state.db"
        _build_db(self.path)

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_exact_database_and_receipt_aggregation(self) -> None:
        report = audit_database(self.path, ReceiptMetrics.from_mapping(_receipt()))
        self.assertEqual(report.candidates_returned, 5)
        self.assertEqual(report.canonical_url_coverage.to_mapping(), {
            "total": 5, "covered": 3, "missing": 1, "non_article": 1,
        })
        self.assertEqual(report.date_evidence_coverage.to_mapping(), {
            "total": 5, "covered": 4, "source": 2, "metadata": 2,
            "missing": 1, "unparseable": 0,
        })
        self.assertEqual((report.rewrite_count, report.distinct_event_count, report.material_update_count), (1, 1, 1))
        self.assertEqual(report.pending_investigation_count, 1)
        self.assertEqual(report.provenance_unknown_count, 1)
        self.assertEqual(report.event_versions_by_state, {"rejected": 0, "unverified": 0, "verified": 1, "watchlist": 0})
        self.assertEqual(report.report_events_by_subject, {"ai": 1})
        self.assertEqual(report.delivery_state_counts["legacy_reports"], {"dry_run": 1})
        self.assertEqual(report.delivery_state_counts["legacy_deliveries"], {"skipped": 1})
        self.assertEqual(report.delivery_state_counts["subject_outbox"], {"prepared": 1})
        self.assertEqual(report.delivery_state_counts["subject_attempts"], {})
        self.assertEqual(report.query_error_count, 1)
        self.assertEqual(report.transport_error_count, 1)
        self.assertEqual(report.integrity_check, "ok")
        self.assertEqual(report.foreign_key_violation_count, 0)

    def test_without_receipt_marks_receipt_only_metrics_unavailable(self) -> None:
        report = audit_database(self.path)
        self.assertIsNone(report.model_fallback_count)
        self.assertIsNone(report.model_malformed_count)
        self.assertIn("model_fallback_count", report.unavailable_metrics)
        self.assertEqual(report.candidates_returned, 5)
        self.assertEqual(report.sources["candidates_returned"], "db")

    def test_public_json_is_stable_and_contains_no_payload_values(self) -> None:
        report = audit_database(self.path, ReceiptMetrics.from_mapping(_receipt()))
        first = render_public_report(report)
        second = render_public_report(report)
        self.assertEqual(first, second)
        decoded = json.loads(first)
        self.assertEqual(decoded["schema"], "news-quality-audit-v1")
        forbidden = (
            "fixture.example", "Fixture body", "synthetic", str(self.path),
            "recipient", "message_ids", "https://",
        )
        self.assertTrue(all(value not in first for value in forbidden))

    def test_cli_is_read_only_and_malformed_receipt_fails_closed(self) -> None:
        receipt_path = Path(self.directory.name) / "receipt.json"
        receipt_path.write_text(json.dumps(_receipt()), encoding="utf-8")
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(["--db", str(self.path), "--receipts", str(receipt_path)])
        self.assertEqual(code, 0)
        self.assertEqual(stderr.getvalue(), "")
        json.loads(stdout.getvalue())
        self.assertEqual(before, hashlib.sha256(self.path.read_bytes()).hexdigest())

        receipt_path.write_text("{}", encoding="utf-8")
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(["--db", str(self.path), "--receipts", str(receipt_path)])
        self.assertNotEqual(code, 0)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(stderr.getvalue(), "quality audit failed\n")

    def test_foreign_key_violation_is_rejected(self) -> None:
        connection = sqlite3.connect(self.path, isolation_level=None)
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("DELETE FROM reports WHERE report_id='report-a'")
        connection.close()
        with self.assertRaisesRegex(ValueError, "foreign-key"):
            audit_database(self.path)


if __name__ == "__main__":
    unittest.main()
