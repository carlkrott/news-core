from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from news_pipeline.briefing_contracts import map_eligibility
from news_pipeline.db import init_db
from news_pipeline.delivery_schema_v6 import migrate_v6
from news_pipeline.event_store import EventWrite, append_event_version, process_phase4
from news_pipeline.event_contracts import (
    AdjudicationResult,
    EventCandidate,
    SemanticDecision,
    SemanticReasonCode,
)
from news_pipeline.models import Category, Subject, subject_for_category
from news_pipeline.provenance import PublisherRule
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5
from news_pipeline.schema_v7 import migrate_v7
from news_pipeline.live_contracts import SourceRole
from news_pipeline.contracts import CandidateArticle, FilterResult, DecisionCode, ReasonCode, TrustTier
from news_pipeline.policies import QueryPolicy


AS_OF = datetime(2026, 9, 8, 9, 0, 0, tzinfo=timezone.utc)


def _rule(rule_id: str, host: str, role: SourceRole, group: str, *, authority: tuple[str, ...] = ()) -> PublisherRule:
    return PublisherRule(
        rule_id=rule_id,
        host=host,
        source_role=role,
        independence_group=group,
        categories=("ai",),
        authority_entities=authority,
        audit_note="sanitized fixture rule",
    )


def _make_db(rules: tuple[PublisherRule, ...]) -> tuple[str, sqlite3.Connection]:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_db(path)
    con = sqlite3.connect(path, isolation_level=None)
    con.execute("PRAGMA foreign_keys=ON")
    migrate_v3(con, "2026-09-07T00:00:00Z")
    migrate_v4(con, "2026-09-07T00:00:01Z")
    migrate_v5(con, "2026-09-07T00:00:02Z")
    migrate_v6(con, "2026-09-07T00:00:03Z")
    con.execute(
        "INSERT INTO runs VALUES (?,?,?,?,?,?,?)",
        ("source-run", "2026-09-07T00:00:00Z", None, "historical_replay", "observed_historical", None, None),
    )
    con.execute(
        "INSERT INTO source_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "source-registry", "rss", "primary", "fixture.example", '["ai"]', 1,
            "[]", "[]", "[]", "[]", "[]", 60, None, None, None,
            "a" * 64, "2026-09-07T00:00:00Z",
        ),
    )
    migrate_v7(con, "2026-09-07T00:00:04Z", rules=rules)
    return path, con


def _insert_item(
    con: sqlite3.Connection,
    item_id: str,
    host: str,
    title: str,
    body: str,
    *,
    publisher: str | None = None,
) -> None:
    con.execute(
        """INSERT INTO source_items(
               source_item_id,source_id,external_id,category,original_url,canonical_url,
               publisher,source_role,author_handle,retrieval_method,raw_content_hash,title,
               body,raw,retrieved_at,published_at,updated_at,publication_evidence)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            item_id, "source-registry", item_id + "-external", "ai",
            f"https://{host}/{item_id}", f"https://{host}/{item_id}", publisher or host,
            "primary", None, "rss", "b" * 64, title, body, body,
            "2026-09-08T06:00:00Z", "2026-09-08T06:00:00Z", None, "source",
        ),
    )


def _insert_decision(con: sqlite3.Connection, decision_id: str, item_id: str) -> None:
    con.execute(
        "INSERT INTO decisions VALUES (?,?,?,?,?,?,?)",
        (
            decision_id,
            "source-run",
            None,
            "keep",
            json.dumps({"source_item_id": item_id, "matched_observation_ids": []}),
            "2026-09-08T06:01:00Z",
            "phase3",
        ),
    )


def _finish(con: sqlite3.Connection) -> None:
    con.commit()
    con.close()


class Run6PromotionTests(unittest.TestCase):
    def tearDown(self) -> None:
        path = getattr(self, "path", None)
        if path:
            os.unlink(path)

    def test_late_primary_provenance_promotes_to_report(self) -> None:
        self.path, con = _make_db(
            (_rule("primary-rule", "authority.example", SourceRole.PRIMARY, "authority", authority=("Widget",)),)
        )
        _insert_item(con, "primary-item", "authority.example", "Widget v2.0", "Widget v2.0 launched", publisher="Widget")
        _insert_decision(con, "decision-primary", "primary-item")
        _finish(con)

        report = process_phase4(self.path, "2026-09-08T06:02:00Z")
        self.assertEqual((report.selected, report.processed, report.versions_appended), (1, 1, 1))
        con = sqlite3.connect(self.path)
        try:
            self.assertEqual(
                con.execute(
                    "SELECT effective_source_role,independence_group,authority_match "
                    "FROM source_item_provenance WHERE source_item_id='primary-item'"
                ).fetchone(),
                ("primary", "authority", 1),
            )
            self.assertEqual(con.execute("SELECT DISTINCT status FROM claims").fetchone()[0], "verified")
            self.assertEqual(con.execute("SELECT verification_state FROM event_versions").fetchone()[0], "verified")
        finally:
            con.close()

    def test_two_independent_groups_verify_and_report_with_subject_section(self) -> None:
        rules = (
            _rule("neutral-rule", "neutral.example", SourceRole.NEUTRAL, "neutral-group"),
            _rule("specialist-rule", "specialist.example", SourceRole.SPECIALIST, "specialist-group"),
        )
        self.path, con = _make_db(rules)
        _insert_item(con, "neutral-item", "neutral.example", "Widget v2.1", "Widget v2.1 launched")
        _insert_item(con, "specialist-item", "specialist.example", "Widget v2.1", "Widget v2.1 launched")
        _insert_decision(con, "decision-neutral", "neutral-item")
        _insert_decision(con, "decision-specialist", "specialist-item")
        _finish(con)

        report = process_phase4(self.path, "2026-09-08T06:03:00Z")
        self.assertEqual((report.selected, report.processed), (2, 2))
        con = sqlite3.connect(self.path)
        try:
            states = con.execute(
                "SELECT verification_state FROM event_versions ORDER BY event_id,version"
            ).fetchall()
            self.assertEqual(states, [("unverified",), ("verified",)])
            self.assertEqual(con.execute("SELECT COUNT(*) FROM source_item_provenance").fetchone()[0], 2)
        finally:
            con.close()

        from news_pipeline.report_builder import run_report

        with tempfile.TemporaryDirectory() as artifacts:
            result = run_report(self.path, Path(artifacts), AS_OF)
            self.assertEqual(result.included_count, 1)
            self.assertIsNone(result.audit_discrepancy)
            con = sqlite3.connect(self.path)
            try:
                self.assertEqual(
                    con.execute(
                        "SELECT section,inclusion_reason FROM report_events "
                        "WHERE report_id=?",
                        (result.report_id,),
                    ).fetchone(),
                    (Subject.AI.value, "verified_event"),
                )
            finally:
                con.close()
            payload = json.loads(result.artifact_result.json_bytes)
            self.assertEqual(payload["items"][0]["subject_id"], Subject.AI.value)

    def test_v7_direct_verified_append_rejects_pending_claim(self) -> None:
        self.path, con = _make_db(
            (_rule("primary-rule", "authority.example", SourceRole.PRIMARY, "authority", authority=("Widget",)),)
        )
        _insert_item(con, "primary-item", "authority.example", "Widget v2.0", "Widget v2.0 launched", publisher="Widget")
        _insert_decision(con, "decision-primary", "primary-item")
        _finish(con)
        process_phase4(self.path, "2026-09-08T06:02:00Z")

        con = sqlite3.connect(self.path, isolation_level=None)
        try:
            claim_id = con.execute("SELECT claim_id FROM claims LIMIT 1").fetchone()[0]
            con.execute("UPDATE claims SET status='pending' WHERE claim_id=?", (claim_id,))
            with self.assertRaisesRegex(ValueError, "verified event writes require"):
                append_event_version(
                    con,
                    EventWrite(
                        event_id="bypass-event",
                        summary="Bypass",
                        material_change_reason="distinct",
                        verification_state="verified",
                        valid_from="2026-09-08T06:06:00Z",
                        claim_ids=(claim_id,),
                    ),
                )
            self.assertIsNone(
                con.execute("SELECT 1 FROM events WHERE id='bypass-event'").fetchone()
            )
        finally:
            con.close()

    def test_report_rechecks_v7_claim_status_before_promotion(self) -> None:
        self.path, con = _make_db(
            (_rule("primary-rule", "authority.example", SourceRole.PRIMARY, "authority", authority=("Widget",)),)
        )
        _insert_item(con, "primary-item", "authority.example", "Widget v2.0", "Widget v2.0 launched", publisher="Widget")
        _insert_decision(con, "decision-primary", "primary-item")
        _finish(con)
        process_phase4(self.path, "2026-09-08T06:02:00Z")
        con = sqlite3.connect(self.path, isolation_level=None)
        try:
            con.execute("UPDATE claims SET status='pending'")
        finally:
            con.close()

        from news_pipeline.report_builder import run_report

        with tempfile.TemporaryDirectory() as artifacts:
            result = run_report(self.path, Path(artifacts), AS_OF)
            self.assertEqual(result.included_count, 0)
            self.assertIsNone(result.audit_discrepancy)
            payload = json.loads(result.artifact_result.json_bytes)
            self.assertEqual(payload["health"], {"status": "healthy", "degraded": False, "reasons": []})

    def test_nonqualifying_state_is_typed_empty_report(self) -> None:
        self.path, con = _make_db(
            (_rule("discovery-rule", "discovery.example", SourceRole.DISCOVERY, "discovery"),)
        )
        _insert_item(con, "discovery-item", "discovery.example", "Widget v2.2", "Widget v2.2 launched")
        _insert_decision(con, "decision-discovery", "discovery-item")
        _finish(con)
        process_phase4(self.path, "2026-09-08T06:04:00Z")

        from news_pipeline.report_builder import run_report

        with tempfile.TemporaryDirectory() as artifacts:
            result = run_report(self.path, Path(artifacts), AS_OF)
            self.assertEqual(result.included_count, 0)
            self.assertIsNone(result.audit_discrepancy)
            payload = json.loads(result.artifact_result.json_bytes)
            self.assertEqual(payload["health"], {"status": "healthy", "degraded": False, "reasons": []})
            with sqlite3.connect(self.path) as check:
                self.assertEqual(
                    check.execute("SELECT COUNT(*) FROM report_events").fetchone()[0],
                    0,
                )

    def test_report_events_promotion_discrepancy_is_not_healthy_empty(self) -> None:
        from news_pipeline import report_builder

        self.path, con = _make_db(
            (_rule("primary-rule", "authority.example", SourceRole.PRIMARY, "authority", authority=("Widget",)),)
        )
        _insert_item(con, "primary-item", "authority.example", "Widget v2.0", "Widget v2.0 launched", publisher="Widget")
        _insert_decision(con, "decision-primary", "primary-item")
        _finish(con)
        process_phase4(self.path, "2026-09-08T06:05:00Z")

        original = report_builder._reconcile_report_events
        try:
            report_builder._reconcile_report_events = lambda con, report_id, items: None
            with tempfile.TemporaryDirectory() as artifacts:
                result = report_builder.run_report(self.path, Path(artifacts), AS_OF)
                self.assertEqual(result.included_count, 0)
                self.assertEqual(
                    result.audit_discrepancy,
                    report_builder.REPORT_EVENTS_PROMOTION_DISCREPANCY,
                )
                payload = json.loads(result.artifact_result.json_bytes)
                self.assertEqual(payload["health"]["reasons"], [report_builder.REPORT_EVENTS_PROMOTION_DISCREPANCY])
        finally:
            report_builder._reconcile_report_events = original


def _suppressed_input():
    category = Category.AUDIOVISUAL
    candidate = CandidateArticle(
        candidate_id="suppressed-candidate", category=category, query_group=category.value,
        title="Suppressed", snippet="Suppressed", original_url=None, canonical_url="https://fixture.example/suppressed",
        published_at="2026-09-08T06:00:00Z", published_evidence="source", observed_at="2026-09-08T06:00:00Z",
        evaluated_at="2026-09-08T06:00:00Z",
    )
    filtered = FilterResult(
        candidate=candidate, decision=DecisionCode.KEEP, reasons=(ReasonCode.OK_KEEP,),
        matched_article_ids=(), matched_observation_ids=(), trust_tier=TrustTier.UNKNOWN,
        evaluated_publication_time=None, ordinal=0,
    )
    policy = QueryPolicy(
        category=category, allowed_query_groups=(category.value,), recency=timedelta(days=7),
        missing_date_fallback=True, exact_title_lookback=timedelta(days=7),
        exact_url_lookback=timedelta(days=7), exact_identity_lookback=timedelta(days=7),
        cross_category_exact_url=True,
    )
    event_candidate = EventCandidate(candidate=candidate, filter_result=filtered, query_policy=policy)
    adjudication = AdjudicationResult(
        candidate_id=candidate.candidate_id, semantic_decision=SemanticDecision.distinct_event,
        phase2_decision=DecisionCode.KEEP, phase2_reasons=(ReasonCode.OK_KEEP,),
        semantic_reasons=(SemanticReasonCode.SUBJECT_COLLISION_SUPPRESSED,), cluster_id="event-suppressed",
        matched_candidate_ids=(candidate.candidate_id,), matched_history_ids=(), matched_observation_ids=(),
        fact_deltas=(), model_used=False, model_confidence=None, model_error_category=None, ordinal=0,
        event_version=1, subject_id=Subject.PROFESSIONAL_AV.value,
        subject_suppressed=True,
    )
    from news_pipeline.briefing_contracts import BriefingInput
    return BriefingInput(event_candidate=event_candidate, adjudication=adjudication)


class Run6EligibilityTests(unittest.TestCase):
    def test_suppressed_subject_is_excluded_with_typed_reason(self) -> None:
        result = map_eligibility(_suppressed_input())
        self.assertFalse(result.included)
        self.assertTrue(result.excluded)
        self.assertEqual(result.exclusion_reason, SemanticReasonCode.SUBJECT_COLLISION_SUPPRESSED)


if __name__ == "__main__":
    unittest.main()
