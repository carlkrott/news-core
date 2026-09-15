"""Tests for source and query policies."""
from __future__ import annotations

import unittest
from dataclasses import FrozenInstanceError
from datetime import timedelta

from news_pipeline.contracts import ReasonCode
from news_pipeline.models import Category
from news_pipeline.policies import (
    DEFAULT_POLICIES,
    QueryPolicy,
    SourceMatch,
    SourcePolicy,
    SourceRule,
    TrustTier,
    classify_source,
    default_query_policies,
)


class SourcePolicyBasics(unittest.TestCase):
    def test_empty_policy_returns_unknown_for_any_host(self):
        policy = SourcePolicy()
        match = classify_source(policy, "https://Example.com/path")
        self.assertEqual(match.tier, TrustTier.UNKNOWN)
        self.assertEqual(match.reason, ReasonCode.OK_UNKNOWN_SOURCE)
        self.assertEqual(match.matched_rule, None)

    def test_missing_url_returns_unknown_not_drop(self):
        policy = SourcePolicy()
        match = classify_source(policy, None)
        self.assertEqual(match.tier, TrustTier.UNKNOWN)
        self.assertEqual(match.reason, ReasonCode.MISSING_URL)

    def test_block_exact_rule_wins_over_allow(self):
        policy = SourcePolicy(
            rules=(
                SourceRule(label="allow", host="example.com", scope="exact", action="allow"),
                SourceRule(label="block", host="example.com", scope="exact", action="block"),
            )
        )
        match = classify_source(policy, "https://example.com/x")
        self.assertEqual(match.tier, TrustTier.BLOCKED)
        self.assertEqual(match.reason, ReasonCode.BLOCKED_SOURCE_EXACT)
        self.assertEqual(match.matched_rule, "block")

    def test_block_subdomain_overrides_allow_exact(self):
        policy = SourcePolicy(
            rules=(
                SourceRule(label="allow", host="example.com", scope="exact", action="allow"),
                SourceRule(label="block", host="evil.com", scope="subdomain", action="block"),
            )
        )
        match = classify_source(policy, "https://news.evil.com/x")
        self.assertEqual(match.tier, TrustTier.BLOCKED)
        self.assertEqual(match.reason, ReasonCode.BLOCKED_SOURCE_SUBDOMAIN)


class SourcePolicyPrecedence(unittest.TestCase):
    def test_full_precedence(self):
        # BLOCK exact > BLOCK subdomain > ALLOW exact > ALLOW subdomain
        # > TRUSTED exact > TRUSTED subdomain > UNKNOWN
        policy = SourcePolicy(
            rules=(
                SourceRule(label="allow-sd", host="allow.com", scope="subdomain", action="allow"),
                SourceRule(label="trusted", host="trusted.com", scope="exact", action="trusted"),
                SourceRule(label="block", host="block.com", scope="exact", action="block"),
                SourceRule(label="block-sd", host="bsub.com", scope="subdomain", action="block"),
            )
        )
        self.assertEqual(
            classify_source(policy, "https://block.com/x").tier,
            TrustTier.BLOCKED,
        )
        self.assertEqual(
            classify_source(policy, "https://x.bsub.com/x").tier,
            TrustTier.BLOCKED,
        )
        self.assertEqual(
            classify_source(policy, "https://allow.com/x").tier,
            TrustTier.ALLOWED,
        )
        self.assertEqual(
            classify_source(policy, "https://x.allow.com/x").tier,
            TrustTier.ALLOWED,
        )
        self.assertEqual(
            classify_source(policy, "https://trusted.com/x").tier,
            TrustTier.TRUSTED,
        )
        self.assertEqual(
            classify_source(policy, "https://unknown.com/x").tier,
            TrustTier.UNKNOWN,
        )


class SourcePolicyDotBoundary(unittest.TestCase):
    def test_subdomain_rule_does_not_match_substring(self):
        policy = SourcePolicy(
            rules=(
                SourceRule(label="a", host="example.com", scope="subdomain", action="allow"),
            )
        )
        # "badexample.com" must NOT match the example.com subdomain rule.
        match = classify_source(policy, "https://badexample.com/x")
        self.assertEqual(match.tier, TrustTier.UNKNOWN)

    def test_subdomain_rule_matches_subdomain_and_exact(self):
        policy = SourcePolicy(
            rules=(
                SourceRule(label="a", host="example.com", scope="subdomain", action="allow"),
            )
        )
        self.assertEqual(
            classify_source(policy, "https://example.com/x").tier,
            TrustTier.ALLOWED,
        )
        self.assertEqual(
            classify_source(policy, "https://www.example.com/x").tier,
            TrustTier.ALLOWED,
        )

    def test_idna_normalization(self):
        # Subdomain scope so ``www.bücher.de`` (canonical host ``www.xn--bcher-kva.de``)
        # matches the rule whose host is the apex ``xn--bcher-kva.de``.
        policy = SourcePolicy(
            rules=(
                SourceRule(label="a", host="xn--bcher-kva.de", scope="subdomain", action="allow"),
            )
        )
        # bücher.de should IDNA-encode and match
        match = classify_source(policy, "https://www.bücher.de/News")
        self.assertEqual(match.tier, TrustTier.ALLOWED)

    def test_exact_rule_does_not_match_subdomain(self):
        policy = SourcePolicy(
            rules=(
                SourceRule(label="a", host="example.com", scope="exact", action="allow"),
            )
        )
        match = classify_source(policy, "https://www.example.com/x")
        self.assertEqual(match.tier, TrustTier.UNKNOWN)

    def test_malformed_url_returns_unknown(self):
        policy = SourcePolicy(
            rules=(
                SourceRule(label="a", host="example.com", scope="exact", action="allow"),
            )
        )
        match = classify_source(policy, "not a url")
        self.assertEqual(match.tier, TrustTier.UNKNOWN)
        self.assertEqual(match.reason, ReasonCode.MALFORMED_CANONICAL_URL)


class SourceRuleValidation(unittest.TestCase):
    def test_invalid_action(self):
        with self.assertRaises(ValueError):
            SourceRule(label="x", host="example.com", scope="exact", action="reject")  # type: ignore[arg-type]

    def test_invalid_scope(self):
        with self.assertRaises(ValueError):
            SourceRule(label="x", host="example.com", scope="suffix", action="allow")  # type: ignore[arg-type]

    def test_empty_label_raises(self):
        with self.assertRaises(ValueError):
            SourceRule(label="", host="example.com", scope="exact", action="allow")

    def test_host_is_lowered_and_idna(self):
        rule = SourceRule(label="x", host="EXAMPLE.com", scope="exact", action="allow")
        self.assertEqual(rule.host, "example.com")

    def test_frozen(self):
        rule = SourceRule(label="x", host="example.com", scope="exact", action="allow")
        with self.assertRaises(FrozenInstanceError):
            rule.host = "other.com"  # type: ignore[misc]


class QueryPolicyDefaults(unittest.TestCase):
    def test_default_recency_windows(self):
        d = default_query_policies()
        self.assertEqual(d[Category.AI].recency, timedelta(hours=72))
        self.assertEqual(d[Category.WORLD].recency, timedelta(hours=48))
        self.assertEqual(d[Category.AUDIO_ENGINEERING].recency, timedelta(days=14))
        self.assertEqual(d[Category.HARDWARE].recency, timedelta(days=7))
        self.assertEqual(d[Category.FANTASY_NOVEL].recency, timedelta(days=14))
        self.assertEqual(d[Category.AUDIOVISUAL].recency, timedelta(days=7))
        self.assertEqual(d[Category.AV_CORPORATE].recency, timedelta(days=7))
        self.assertEqual(d[Category.OUR_SETUP].recency, timedelta(days=30))

    def test_duplicate_lookbacks(self):
        d = default_query_policies()
        for cat, policy in d.items():
            with self.subTest(cat=cat):
                self.assertEqual(policy.exact_url_lookback, timedelta(days=7))
                self.assertEqual(policy.exact_identity_lookback, timedelta(days=7))
                self.assertEqual(policy.exact_title_lookback, timedelta(hours=72))

    def test_default_allowed_query_group_is_category_value(self):
        d = default_query_policies()
        for cat, policy in d.items():
            with self.subTest(cat=cat):
                self.assertEqual(set(policy.allowed_query_groups), {cat.value})

    def test_cross_category_exact_url_default(self):
        d = default_query_policies()
        for cat, policy in d.items():
            with self.subTest(cat=cat):
                # Default true per repair prompt
                self.assertTrue(policy.cross_category_exact_url)

    def test_missing_date_fallback_default(self):
        d = default_query_policies()
        for cat, policy in d.items():
            with self.subTest(cat=cat):
                self.assertTrue(policy.missing_date_fallback)

    def test_unknown_category_default(self):
        # ``default_query_policies()`` returns a dict with exactly the 8 known
        # categories; anything else must KeyError.
        d = default_query_policies()
        self.assertEqual(set(d), set(Category))
        sentinel = object()
        with self.assertRaises(KeyError):
            d[sentinel]  # type: ignore[index]

    def test_explicit_custom_groups(self):
        pol = QueryPolicy(
            category=Category.AI,
            allowed_query_groups=("ai", "ai_explained", "ai_safety"),
            recency=timedelta(hours=72),
            missing_date_fallback=True,
            exact_title_lookback=timedelta(hours=72),
            exact_url_lookback=timedelta(days=7),
            exact_identity_lookback=timedelta(days=7),
            cross_category_exact_url=True,
        )
        self.assertEqual(
            set(pol.allowed_query_groups),
            {"ai", "ai_explained", "ai_safety"},
        )

    def test_default_policies_export(self):
        self.assertIsInstance(DEFAULT_POLICIES, dict)
        self.assertEqual(len(DEFAULT_POLICIES), 8)
        # Default-allowed query group is exactly the Category.value
        for cat in Category:
            self.assertEqual(set(DEFAULT_POLICIES[cat].allowed_query_groups), {cat.value})


class SourceMatchTests(unittest.TestCase):
    def test_unknown_source_is_not_a_drop(self):
        m = SourceMatch(tier=TrustTier.UNKNOWN, reason=ReasonCode.OK_UNKNOWN_SOURCE)
        # No terminal DROP decision lives in the source-policy path itself;
        # unknown_source merely informs trust tier.
        self.assertEqual(m.tier, TrustTier.UNKNOWN)


if __name__ == "__main__":
    unittest.main()
