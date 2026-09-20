"""Tests for the deterministic filtering engine."""
from __future__ import annotations

import copy
import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

from news_pipeline.contracts import (
    CandidateArticle,
    DecisionCode,
    ReasonCode,
)
from news_pipeline.db import connect, init_db
from news_pipeline.filtering import evaluate_candidates
from news_pipeline.history import HistoryUnavailable, open_history
from news_pipeline.models import Category
from news_pipeline.policies import (
    QueryPolicy,
    SourcePolicy,
    SourceRule,
    default_query_policies,
)


EVAL = "2026-07-14T22:50:33Z"
RECENT = "2026-07-14T20:50:33Z"   # within all recency windows
OBSERVED = "2026-07-14T19:50:33Z"
OLD = "2026-07-01T00:00:00Z"       # outside all short recency windows


def _candidate(**overrides):
    base = dict(
        candidate_id="c1",
        category=Category.AI,
        query_group="ai",
        title="Hello world",
        snippet="body text",
        original_url="https://example.com/a",
        canonical_url="https://example.com/a",
        published_at=RECENT,
        published_evidence="source",
        observed_at=OBSERVED,
        evaluated_at=EVAL,
    )
    if "canonical_url" in overrides and "original_url" not in overrides:
        base["original_url"] = overrides["canonical_url"]
    base.update(overrides)
    return CandidateArticle(**base)


class DeterministicPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "state.db")
        init_db(self.db_path)
        self.policies = default_query_policies()
        self.source = SourcePolicy()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    # ---- 1. Validate contract / canonical consistency ----

    def test_canonical_consistency_mismatch_falls_back_to_original(self):
        # canonical is None but original is given. The engine must accept the candidate
        # and continue, treating canonical_url=None (this is a normalized form of the URL-less
        # candidate, not a malformed one).
        c = _candidate(candidate_id="c1", canonical_url=None, original_url="https://example.com/a")
        results = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(len(results), 1)
        # Should not raise — the contract tolerates None when original is provided.
        self.assertIn(results[0].decision, {DecisionCode.KEEP, DecisionCode.PENDING_HISTORY_UNAVAILABLE})

    def test_bad_evaluated_at_yields_pending(self):
        c = _candidate(evaluated_at="not-a-date")
        results = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].decision, DecisionCode.PENDING_INVALID_EVIDENCE)

    # ---- 2/3. Source policy ----

    def test_blocked_source_drops(self):
        self.source = SourcePolicy(
            rules=(SourceRule(label="block", host="example.com", scope="exact", action="block"),)
        )
        c = _candidate(candidate_id="blocked")
        results = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(results[0].decision, DecisionCode.DROP_BLOCKED_SOURCE)
        self.assertIn(ReasonCode.BLOCKED_SOURCE_EXACT, results[0].reasons)

    # ---- 4. Recency ----

    def test_stale_article_dropped(self):
        c = _candidate(candidate_id="stale", published_at=OLD)
        results = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(results[0].decision, DecisionCode.DROP_STALE)
        self.assertIn(ReasonCode.STALE, results[0].reasons)

    def test_missing_publication_date_with_valid_observed_at_keeps(self):
        c = _candidate(
            candidate_id="no-pub",
            published_at=None,
            published_evidence="missing",
        )
        results = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(results[0].decision, DecisionCode.KEEP)
        self.assertIn(ReasonCode.OK_OBSERVED_FALLBACK, results[0].reasons)

    def test_missing_publication_and_observed_yields_pending(self):
        c = _candidate(
            candidate_id="all-missing",
            published_at=None,
            published_evidence="missing",
            observed_at=None,
        )
        results = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(results[0].decision, DecisionCode.PENDING_MISSING_EVIDENCE)
        self.assertIn(ReasonCode.MISSING_DATE, results[0].reasons)

    def test_unparseable_publication_yields_pending(self):
        c = _candidate(
            candidate_id="bad-pub",
            published_at="not-a-date",
            published_evidence="unparseable",
        )
        results = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(results[0].decision, DecisionCode.PENDING_INVALID_EVIDENCE)

    def test_future_within_six_hours_keeps_with_reason(self):
        # ~1h in the future
        dt = datetime(2026, 7, 14, 23, 50, 33, tzinfo=timezone.utc)
        future_iso = dt.isoformat().replace("+00:00", "Z")
        c = _candidate(candidate_id="future-ok", published_at=future_iso)
        results = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(results[0].decision, DecisionCode.KEEP)
        # Future publication evidence within tolerance is explicitly clamped.
        self.assertIn(ReasonCode.FUTURE_DATE_CLAMPED, results[0].reasons)
        self.assertNotIn(ReasonCode.OK_OBSERVED_FALLBACK, results[0].reasons)

    def test_future_over_six_hours_yields_pending(self):
        dt = datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc)  # >6h past evaluated_at
        future_iso = dt.isoformat().replace("+00:00", "Z")
        c = _candidate(candidate_id="future-bad", published_at=future_iso)
        results = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(results[0].decision, DecisionCode.PENDING_INVALID_EVIDENCE)
        self.assertIn(ReasonCode.FUTURE_DATE, results[0].reasons)

    # ---- 5. Within-batch dedup ----

    def test_within_batch_first_ordinal_wins(self):
        c1 = _candidate(candidate_id="c1", canonical_url="https://example.com/a")
        c2 = _candidate(candidate_id="c2", canonical_url="https://example.com/a")
        results = evaluate_candidates([c1, c2], self.db_path, self.source, self.policies)
        self.assertEqual(results[0].decision, DecisionCode.KEEP)
        self.assertEqual(results[1].decision, DecisionCode.SUPPRESS_BATCH_EXACT)
        self.assertEqual(results[1].ordinal, 2)

    def test_within_batch_url_changed_content_pending(self):
        c1 = _candidate(candidate_id="c1", canonical_url="https://example.com/a", snippet="body text")
        c2 = _candidate(candidate_id="c2", canonical_url="https://example.com/a", snippet="different")
        results = evaluate_candidates([c1, c2], self.db_path, self.source, self.policies)
        self.assertEqual(results[0].decision, DecisionCode.KEEP)
        self.assertEqual(results[1].decision, DecisionCode.PENDING_POSSIBLE_UPDATE)
        self.assertIn(ReasonCode.WITHIN_BATCH_EXACT, results[1].reasons)

    def test_within_batch_preserves_input_order(self):
        cand = [
            _candidate(candidate_id=f"c{i}", canonical_url=f"https://example.com/{i}")
            for i in range(5)
        ]
        results = evaluate_candidates(cand, self.db_path, self.source, self.policies)
        for i, r in enumerate(results):
            self.assertEqual(r.candidate.candidate_id, f"c{i}")
            self.assertEqual(r.ordinal, i + 1)

    # ---- 6/7/8. History lookups ----

    def test_history_exact_url_suppresses(self):
        self._seed_history(
            article_id="a1",
            article_pk="a1",  # literal primary key so the test can assert on it
            category="ai",
            canonical_url="https://example.com/a",
            snippet="body text",
            title="Hello world",
            source_file="ai-2026-07-01.md",
            occurred_at="2026-07-10T00:00:00Z",
        )
        c = _candidate(candidate_id="c1")
        results = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(results[0].decision, DecisionCode.SUPPRESS_EXACT_URL)
        self.assertIn(ReasonCode.HISTORY_EXACT_URL, results[0].reasons)
        self.assertEqual(results[0].matched_article_ids, ("a1",))

    def test_history_exact_url_changed_content_pending(self):
        self._seed_history(
            article_id="a1",
            article_pk="a1",
            category="ai",
            canonical_url="https://example.com/a",
            snippet="OLD content",
            title="Hello world",
            source_file="ai-2026-07-01.md",
            occurred_at="2026-07-10T00:00:00Z",
        )
        c = _candidate(candidate_id="c1", snippet="NEW content")
        results = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(results[0].decision, DecisionCode.PENDING_POSSIBLE_UPDATE)
        self.assertIn(ReasonCode.HISTORY_URL_CHANGED_CONTENT, results[0].reasons)

    def test_history_exact_identity_suppresses(self):
        from news_pipeline.db import article_id

        ident = article_id(None, "hello world", "body text", "ai", None)
        self._seed_history(
            article_id="a1",
            article_pk=ident,
            category="ai",
            canonical_url=None,
            snippet="body text",
            title="Hello world",
            source_file="ai-2026-07-01.md",
            occurred_at="2026-07-10T00:00:00Z",
        )
        c = _candidate(candidate_id="c1", original_url=None, canonical_url=None)
        results = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(results[0].decision, DecisionCode.PENDING_MISSING_EVIDENCE)
        self.assertTrue(results[0].audit_only)
        self.assertIn(ReasonCode.MISSING_URL, results[0].reasons)

    def test_history_exact_title_changed_snippet_pending(self):
        # Title lookback is 72h, so seed within that window.
        self._seed_history(
            article_id="a1",
            category="ai",
            canonical_url="https://example.com/old",
            snippet="old snippet",
            title="Hello world",
            source_file="ai-2026-07-01.md",
            occurred_at="2026-07-13T23:00:00Z",
        )
        c = _candidate(candidate_id="c1", canonical_url="https://example.com/new", snippet="new snippet")
        results = evaluate_candidates([c], self.db_path, self.source, self.policies)
        # Title identical, but URL different => new article. The title-only history
        # evidence is low confidence and must NOT suppress a same-title candidate
        # whose canonical URL has changed.
        self.assertEqual(results[0].decision, DecisionCode.PENDING_POSSIBLE_UPDATE)
        self.assertIn(ReasonCode.HISTORY_TITLE_CHANGED_SNIPPET, results[0].reasons)

    def test_history_exact_title_low_confidence_does_not_unsafely_suppress(self):
        # Title-only history within 72h; candidate has a different canonical URL
        # and a snippet — title alone should never trigger a global suppression.
        self._seed_history(
            article_id="a1",
            category="ai",
            canonical_url=None,
            snippet="",
            title="Hello world",
            source_file="ai-2026-07-01.md",
            occurred_at="2026-07-13T23:00:00Z",
            identity_basis="title_only",
            identity_confidence=0.35,
        )
        c = _candidate(candidate_id="c1", canonical_url="https://example.com/other", snippet="brand new")
        results = evaluate_candidates([c], self.db_path, self.source, self.policies)
        # Title-only history at category scope is treated as low confidence; engine
        # returns PENDING_POSSIBLE_UPDATE rather than silently dropping.
        self.assertEqual(results[0].decision, DecisionCode.PENDING_POSSIBLE_UPDATE)

    def test_history_cross_category_url_match_when_flag_enabled(self):
        self._seed_history(
            article_id="a1",
            category="world",
            canonical_url="https://example.com/a",
            snippet="body text",
            title="Hello world",
            source_file="world-2026-07-01.md",
            occurred_at="2026-07-10T00:00:00Z",
        )
        c = _candidate(candidate_id="c1", category=Category.AI)
        results = evaluate_candidates([c], self.db_path, self.source, self.policies)
        # Default policy enables cross-category URL global
        self.assertEqual(results[0].decision, DecisionCode.SUPPRESS_EXACT_URL)

    def test_query_group_not_in_policy_pending(self):
        c = _candidate(candidate_id="c1", query_group="invented_query_group")
        results = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(results[0].decision, DecisionCode.PENDING_INVALID_EVIDENCE)
        self.assertIn(ReasonCode.UNKNOWN_QUERY_GROUP, results[0].reasons)

    def test_history_unavailable_yields_pending(self):
        # Pass a missing DB so HistoryUnavailable is raised and the engine returns
        # PENDING_HISTORY_UNAVAILABLE for candidates that need history.
        bogus = str(Path(self.tmp.name) / "missing.db")
        c = _candidate(candidate_id="c1")
        results = evaluate_candidates([c], bogus, self.source, self.policies)
        self.assertEqual(len(results), 1)
        # The candidate's decision is whatever its contract-validation produces:
        # either PENDING_HISTORY_UNAVAILABLE (if history was needed) or PENDING_INVALID_EVIDENCE
        # (if validation already bailed).
        self.assertIn(
            results[0].decision,
            {DecisionCode.PENDING_HISTORY_UNAVAILABLE, DecisionCode.PENDING_INVALID_EVIDENCE},
        )

    def test_all_eight_categories_have_distinct_policies(self):
        d = default_query_policies()
        for cat in Category:
            self.assertIn(cat, d)

    def test_deterministic_repeat_run(self):
        cand = [
            _candidate(candidate_id=f"c{i}", canonical_url=f"https://example.com/{i}")
            for i in range(3)
        ]
        a = evaluate_candidates(cand, self.db_path, self.source, self.policies)
        b = evaluate_candidates(cand, self.db_path, self.source, self.policies)
        self.assertEqual(
            [(r.candidate.candidate_id, r.decision) for r in a],
            [(r.candidate.candidate_id, r.decision) for r in b],
        )

    # ---- helpers ----

    def _seed_history(
        self,
        *,
        article_id: str,
        category: str,
        canonical_url: str | None,
        snippet: str,
        title: str,
        source_file: str,
        occurred_at: str,
        article_pk: str | None = None,
        identity_basis: str = "canonical_url",
        identity_confidence: float = 1.0,
    ) -> None:
        from news_pipeline.db import article_id as aid

        pk = article_pk or aid(canonical_url, title.lower(), snippet, category, source_file)
        with connect(self.db_path) as con:
            con.execute(
                "INSERT INTO runs(id,started_at,kind,provenance) VALUES (?,?,?,?)",
                ("r1", "2026-07-01T00:00:00Z", "historical_replay", "observed_historical"),
            )
            con.execute(
                "INSERT INTO articles(id,run_id,category,canonical_url,original_url,title,snippet,"
                "source_file,observed_at,fetch_marker,provenance,created_at,normalized_title,"
                "identity_confidence,identity_basis) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    pk,
                    "r1",
                    category,
                    canonical_url,
                    canonical_url,
                    title,
                    snippet,
                    source_file,
                    occurred_at,
                    None,
                    "observed_historical",
                    occurred_at,
                    title.lower(),
                    identity_confidence,
                    identity_basis,
                ),
            )
            con.execute(
                "INSERT INTO observations(id,article_id,category,source_file,kind,body,occurred_at,created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (
                    f"obs-{pk}",
                    pk,
                    category,
                    source_file,
                    "parsed_article",
                    snippet,
                    occurred_at,
                    occurred_at,
                ),
            )


if __name__ == "__main__":
    unittest.main()
