"""Phase 3 — Slice 3 clustering. RED until clustering exists."""
from __future__ import annotations

import unittest
from decimal import Decimal

from news_pipeline.clustering import (
    cluster_id_for_url,
    cluster_id_for_topic,
    combined_score,
    jaccard,
    score_pair,
    select_history_matches,
    tokenize,
)
from news_pipeline.contracts import (
    CandidateArticle,
    HistoryMatch,
    TrustTier,
)
from news_pipeline.event_contracts import ScoredHistoryMatch
from news_pipeline.models import Category
from news_pipeline.policies import QueryPolicy


def _candidate(
    cid: str = "cand-1", category: Category = Category.AI,
    title: str = "Title", snippet: str = "Snippet",
    url: str | None = "https://example.com/article",
) -> CandidateArticle:
    return CandidateArticle(
        candidate_id=cid, category=category, query_group=category.value,
        title=title, snippet=snippet, original_url=url, canonical_url=url,
        published_at="2026-07-01T00:00:00Z", published_evidence="source",
        observed_at="2026-07-01T00:00:00Z", evaluated_at="2026-07-01T00:00:00Z",
    )


def _history(
    article_id: str = "art-1", category: Category = Category.AI,
    occurred_at: str = "2026-06-01T00:00:00Z",
    title: str = "Title", snippet: str = "Snippet",
    url: str | None = None,
) -> HistoryMatch:
    return HistoryMatch(
        article_id=article_id, observation_id=f"obs-{article_id}",
        category=category, occurred_at=occurred_at, title=title, snippet=snippet,
        canonical_url=url, identity_basis="title_only",
    )


def _policy(category: Category, cross: bool = True) -> QueryPolicy:
    from datetime import timedelta
    return QueryPolicy(
        category=category, allowed_query_groups=(category.value,),
        recency=timedelta(days=7), missing_date_fallback=True,
        exact_title_lookback=timedelta(days=7),
        exact_url_lookback=timedelta(days=7),
        exact_identity_lookback=timedelta(days=7),
        cross_category_exact_url=cross,
    )


def _event(candidate: CandidateArticle):
    from datetime import timedelta
    policy = _policy(candidate.category)
    from news_pipeline.contracts import DecisionCode, ReasonCode, FilterResult
    flt = FilterResult(
        candidate=candidate, decision=DecisionCode.KEEP,
        reasons=(ReasonCode.OK_KEEP,),
        matched_article_ids=(), matched_observation_ids=(),
        trust_tier=TrustTier.UNKNOWN, evaluated_publication_time=None,
        ordinal=0,
    )
    from news_pipeline.event_contracts import EventCandidate
    return EventCandidate(candidate=candidate, filter_result=flt, query_policy=policy)


class TestClustering(unittest.TestCase):
    # 15
    def test_nfkc_casefold_unicode_tokens(self) -> None:
        # NFKC normalizes; casefold lowers case. diacritics stay so "Café"
        # becomes "café". Punctuation is dropped.
        t = tokenize("Café naïve résumé.")
        self.assertIn("café", t)
        self.assertIn("naïve", t)
        self.assertIn("résumé", t)
        # Sorted unique tuple
        self.assertEqual(tuple(sorted(set(t))), t)
        # Stopwords get filtered out
        s = tokenize("the quick brown fox")
        self.assertIn("quick", s)
        self.assertIn("brown", s)
        self.assertIn("fox", s)
        self.assertNotIn("the", s)

    # 16
    def test_punctuation_splits_hyphen_apostrophe_underscore(self) -> None:
        t = tokenize("foo-bar foo_bar can't don't")
        # Hyphens/underscores/apostrophes are non-letter/number, so they split
        self.assertIn("foo", t)
        self.assertIn("bar", t)
        self.assertIn("can", t)
        self.assertIn("don", t)
        # The full compounds should not be present
        self.assertNotIn("foo-bar", t)
        self.assertNotIn("foo_bar", t)
        self.assertNotIn("can\u0027t", t)

    # 17
    def test_jaccard_empty_and_threshold_boundaries(self) -> None:
        # Empty pair -> Decimal 0
        self.assertEqual(jaccard((), ()), Decimal("0"))
        # Identical -> 1
        self.assertEqual(jaccard(("alpha", "beta"), ("alpha", "beta")), Decimal("1"))
        # Disjoint -> 0
        self.assertEqual(jaccard(("a",), ("b",)), Decimal("0"))
        # 1/3 quantized to 6 decimals
        v = jaccard(("a", "b"), ("a", "c", "d"))
        # intersection={a}=1, union={a,b,c,d}=4 -> 0.25
        self.assertEqual(v, Decimal("0.250000"))

    # 18
    def test_combined_weight_and_round_half_even(self) -> None:
        # combined_score = 0.75 * title + 0.25 * snippet, 6 dp half-even
        v = combined_score(Decimal("0.6"), Decimal("0.4"))
        # 0.75*0.6 + 0.25*0.4 = 0.45 + 0.10 = 0.55
        self.assertEqual(v, Decimal("0.550000"))
        # Round-half-even at exactly .5 boundary: 0.625 input -> 0.625000
        v = combined_score(Decimal("0.5"), Decimal("1"))
        # 0.75*0.5 + 0.25*1.0 = 0.375 + 0.25 = 0.625
        self.assertEqual(v, Decimal("0.625000"))

    # 19
    def test_cross_category_url_policy_true_false(self) -> None:
        # Cross=true: an equal canonical URL across categories is an exact match.
        cand = _candidate(category=Category.AI)
        hist = _history(category=Category.WORLD, url=cand.canonical_url)
        event = _event(cand)
        out = select_history_matches(event, [hist])
        self.assertEqual(len(out), 1)
        self.assertTrue(out[0].exact_url)

        # Cross=false: same equal URL across categories is filtered out.
        cand2 = _candidate(category=Category.AI)
        event2 = _event(cand2)
        from news_pipeline.policies import QueryPolicy
        from datetime import timedelta
        # Build a candidate with non-cross-category policy
        cand2_x = CandidateArticle(
            candidate_id="cand-x", category=Category.AI, query_group="ai",
            title=cand.title, snippet=cand.snippet,
            original_url=cand.canonical_url, canonical_url=cand.canonical_url,
            published_at="2026-07-01T00:00:00Z", published_evidence="source",
            observed_at="2026-07-01T00:00:00Z",
            evaluated_at="2026-07-01T00:00:00Z",
        )
        from news_pipeline.contracts import DecisionCode, ReasonCode, FilterResult
        flt = FilterResult(
            candidate=cand2_x, decision=DecisionCode.KEEP,
            reasons=(ReasonCode.OK_KEEP,),
            matched_article_ids=(), matched_observation_ids=(),
            trust_tier=TrustTier.UNKNOWN, evaluated_publication_time=None,
            ordinal=0,
        )
        policy_x = QueryPolicy(
            category=Category.AI, allowed_query_groups=("ai",),
            recency=timedelta(days=7), missing_date_fallback=True,
            exact_title_lookback=timedelta(days=7),
            exact_url_lookback=timedelta(days=7),
            exact_identity_lookback=timedelta(days=7),
            cross_category_exact_url=False,
        )
        from news_pipeline.event_contracts import EventCandidate
        ev2 = EventCandidate(candidate=cand2_x, filter_result=flt, query_policy=policy_x)
        hist_x = _history(category=Category.WORLD, url=cand.canonical_url)
        out2 = select_history_matches(ev2, [hist_x])
        # No match expected: cross-category URL prohibited, lexical also false
        # because categories differ -> no candidates pass >=.70 within category
        self.assertEqual(len(out2), 0)

    # 20
    def test_lexical_matching_is_category_scoped(self) -> None:
        # Even with identical title, cross-category lexical must not match.
        cand = _candidate(category=Category.AI)
        hist = _history(category=Category.WORLD, title=cand.title,
                        snippet=cand.snippet)
        ev = _event(cand)
        out = select_history_matches(ev, [hist])
        self.assertEqual(len(out), 0)

    # 21
    def test_stable_tie_break_order(self) -> None:
        cand = _candidate(
            title="Same", snippet="Same",
            url="https://example.com/article",
        )
        # Three history rows with identical titles/snippets/category -> stable
        # tie-break: occurred_at asc, article_id asc.
        h_late = _history(article_id="b-late", occurred_at="2026-07-09T00:00:00Z",
                           title="Same", snippet="Same", url=None)
        h_mid = _history(article_id="a-mid", occurred_at="2026-07-05T00:00:00Z",
                          title="Same", snippet="Same", url=None)
        h_early = _history(article_id="c-early", occurred_at="2026-07-01T00:00:00Z",
                            title="Same", snippet="Same", url=None)
        ev = _event(cand)
        out = select_history_matches(ev, [h_late, h_mid, h_early])
        self.assertEqual(len(out), 3)
        ids = [m.match.article_id for m in out]
        # occurred_at ascending: 2026-07-01, 2026-07-05, 2026-07-09
        self.assertEqual(["c-early", "a-mid", "b-late"], ids)

    # 22
    def test_cluster_id_lengths_and_serialization(self) -> None:
        u = "https://example.com/path/article-12"
        cid_url = cluster_id_for_url(u)
        self.assertEqual(68, len(cid_url))  # "url|" + 64 hex
        self.assertTrue(cid_url.startswith("url|"))
        cid_topic = cluster_id_for_topic(Category.AI, "anchor-1")
        self.assertEqual(70, len(cid_topic))  # "topic|" + 64 hex
        self.assertTrue(cid_topic.startswith("topic|"))
        # Deterministic
        self.assertEqual(cluster_id_for_url(u), cluster_id_for_url(u))


if __name__ == "__main__":
    unittest.main()
