"""Bounded Run 4 admission and report-quality regressions."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

from news_pipeline.canonicalization import canonicalize_url, non_article_url_reason
from news_pipeline.contracts import DecisionCode, ReasonCode
from news_pipeline.db import init_db
from news_pipeline.event_contracts import EventCandidate, SemanticDecision, SemanticReasonCode
from news_pipeline.filtering import evaluate_candidates
from news_pipeline.models import CATEGORY_TO_SUBJECT, Category, Subject
from news_pipeline.phase3_api import evaluate_semantic_updates
from news_pipeline.policies import SourcePolicy, default_query_policies, query_policies_from_subject_policies
from news_pipeline.process_runner import candidate_from_source_item
from tests.news_pipeline.test_filtering import _candidate
from tests.news_pipeline.test_report_builder import _as_of, _insert_event_version, _make_db, _persist_db


class RouteQualityTests(unittest.TestCase):
    def test_article_route_classifier_rejects_exact_non_article_routes(self) -> None:
        cases = {
            "https://feed.example.com/": "home_page",
            "https://feed.example.com/home": "home_page",
            "https://feed.example.com/index.html": "index_page",
            "https://feed.example.com/news/index": "index_page",
            "https://feed.example.com/tag/audio": "tag_page",
            "https://feed.example.com/tags/audio": "tag_page",
            "https://feed.example.com/search?q=audio": "search_page",
        }
        for url, expected in cases.items():
            with self.subTest(url=url):
                self.assertEqual(non_article_url_reason(url), expected)

    def test_route_classifier_preserves_near_miss_article_paths(self) -> None:
        for url in (
            "https://feed.example.com/homepage",
            "https://feed.example.com/tagged/audio",
            "https://feed.example.com/searchlight",
            "https://feed.example.com/indexing",
            "https://feed.example.com/article/searchlight",
        ):
            with self.subTest(url=url):
                self.assertIsNone(non_article_url_reason(url))

    def test_canonical_equivalence_remains_stable(self) -> None:
        self.assertEqual(
            canonicalize_url("HTTPS://feed.example.com:443/story?utm_source=x&b=2&a=1#fragment"),
            "https://feed.example.com/story?a=1&b=2",
        )


class AdmissionQualityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "state.db")
        init_db(self.db_path)
        self.policies = default_query_policies()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_non_article_route_is_terminal_drop(self) -> None:
        result = evaluate_candidates(
            [_candidate(candidate_id="home", canonical_url="https://feed.example.com/")],
            self.db_path,
            SourcePolicy(),
            self.policies,
        )[0]
        self.assertEqual(result.decision, DecisionCode.DROP_NON_ARTICLE_URL)
        self.assertIn(ReasonCode.NON_ARTICLE_URL, result.reasons)
        self.assertFalse(result.audit_only)

    def test_missing_url_is_audit_only_pending_and_not_history_matched(self) -> None:
        result = evaluate_candidates(
            [_candidate(candidate_id="missing", original_url=None, canonical_url=None)],
            self.db_path,
            SourcePolicy(),
            self.policies,
        )[0]
        self.assertEqual(result.decision, DecisionCode.PENDING_MISSING_EVIDENCE)
        self.assertEqual(result.reasons, (ReasonCode.MISSING_URL,))
        self.assertTrue(result.audit_only)

    def test_audit_only_batch_update_cannot_reach_model(self) -> None:
        first = _candidate(candidate_id="first", original_url=None, canonical_url=None)
        second = _candidate(
            candidate_id="second",
            original_url=None,
            canonical_url=None,
            snippet="changed body",
        )
        filtered = evaluate_candidates([first, second], self.db_path, SourcePolicy(), self.policies)
        self.assertEqual(filtered[1].decision, DecisionCode.PENDING_POSSIBLE_UPDATE)
        self.assertTrue(filtered[1].audit_only)

        class Model:
            calls = 0

            def __call__(self, request):
                del request
                self.calls += 1
                raise AssertionError("audit-only candidate reached model")

        model = Model()
        result = evaluate_semantic_updates(
            (EventCandidate(candidate=second, filter_result=filtered[1], query_policy=self.policies[Category.AI]),),
            (),
            model,
            1,
        )[0]
        self.assertEqual(result.semantic_decision, SemanticDecision.pending_review)
        self.assertEqual(result.semantic_reasons, (SemanticReasonCode.PHASE2_MISSING_EVIDENCE,))
        self.assertEqual(model.calls, 0)

    def test_recency_statuses_are_explicit(self) -> None:
        cases = (
            (_candidate(candidate_id="fresh"), "fresh", "source"),
            (_candidate(candidate_id="fallback", published_at=None, published_evidence="missing"), "fresh", "observed_fallback"),
            (_candidate(candidate_id="missing", published_at=None, published_evidence="missing", observed_at=None), "missing", "missing"),
            (_candidate(candidate_id="invalid", published_at=None, published_evidence="unparseable"), "invalid", "unparseable"),
            (_candidate(candidate_id="future", published_at="2026-07-15T00:00:00Z"), "future_clamped", "source"),
        )
        for candidate, status, evidence in cases:
            with self.subTest(candidate=candidate.candidate_id):
                result = evaluate_candidates([candidate], self.db_path, SourcePolicy(), self.policies)[0]
                self.assertEqual(result.recency_status, status)
                self.assertEqual(result.date_evidence, evidence)

    def test_subject_policy_controls_recency_and_history_windows(self) -> None:
        policies = {
            subject: SimpleNamespace(subject=subject, recency_days=(2 if subject is Subject.AI else 9))
            for subject in Subject
        }
        resolved = query_policies_from_subject_policies(policies)
        self.assertEqual(resolved[Category.AI].recency, timedelta(days=2))
        self.assertEqual(resolved[Category.AI].exact_url_lookback, timedelta(days=2))
        self.assertEqual(resolved[Category.AI].exact_identity_lookback, timedelta(days=2))
        self.assertEqual(resolved[Category.AUDIOVISUAL].subject, Subject.PROFESSIONAL_AV)
        self.assertEqual(resolved[Category.AV_CORPORATE].recency, timedelta(days=9))
        self.assertEqual(CATEGORY_TO_SUBJECT[Category.AV_CORPORATE], Subject.PROFESSIONAL_AV)


class DateEvidenceAdapterTests(unittest.TestCase):
    def test_source_item_date_evidence_is_preserved(self) -> None:
        row = {
            "source_item_id": "item-1",
            "category": "ai",
            "title": "A story",
            "body": "Body",
            "original_url": "https://feed.example.com/story",
            "canonical_url": "https://feed.example.com/story",
            "published_at": None,
            "publication_evidence": "unparseable",
            "retrieved_at": "2026-09-20T00:00:00Z",
        }
        candidate = candidate_from_source_item(row, "2026-09-20T00:00:00Z")
        self.assertEqual(candidate.published_at, None)
        self.assertEqual(candidate.published_evidence, "unparseable")


class ReportUrlCompletenessTests(unittest.TestCase):
    def test_verified_event_without_canonical_source_url_is_excluded(self) -> None:
        conn = _make_db()
        _insert_event_version(
            conn,
            "ev-no-url",
            1,
            "No source URL",
            "verified",
            "2026-09-08T08:00:00Z",
            with_source=False,
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as handle:
            db_path = handle.name
        try:
            _persist_db(conn, db_path)
            with tempfile.TemporaryDirectory() as artifact_root:
                from news_pipeline.report_builder import run_report

                result = run_report(db_path, Path(artifact_root), _as_of(9))
                self.assertEqual(result.included_count, 0)
        finally:
            os.unlink(db_path)

    def test_replayed_report_link_without_canonical_source_url_is_excluded(self) -> None:
        conn = _make_db()
        _insert_event_version(
            conn,
            "ev-replay-no-url",
            1,
            "Replayed no source URL",
            "verified",
            "2026-09-08T08:00:00Z",
            with_source=False,
        )
        conn.execute(
            "INSERT INTO reports(report_id,window_start,window_end,generation_status,delivery_state,created_at) "
            "VALUES (?,?,?,?,?,?)",
            (
                "report-replay-no-url",
                "2026-09-07T08:00:00Z",
                "2026-09-08T08:00:00Z",
                "complete",
                "dry_run",
                "2026-09-08T08:01:00Z",
            ),
        )
        conn.execute(
            "INSERT INTO report_events(report_id,event_id,event_version,section,sort_order,inclusion_reason) "
            "VALUES (?,?,?,?,?,?)",
            (
                "report-replay-no-url",
                "ev-replay-no-url",
                1,
                "headline",
                0,
                "verified",
            ),
        )
        conn.commit()
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as handle:
            db_path = handle.name
        try:
            _persist_db(conn, db_path)
            with tempfile.TemporaryDirectory() as artifact_root:
                from news_pipeline.report_builder import _items_from_links

                check = sqlite3.connect(db_path)
                try:
                    self.assertEqual(_items_from_links(check, "report-replay-no-url"), [])
                finally:
                    check.close()
        finally:
            os.unlink(db_path)
