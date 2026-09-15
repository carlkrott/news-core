"""Phase 3 — Slice 2 fact extraction. RED until fact_extraction exists."""
from __future__ import annotations

import unittest

from decimal import Decimal

from news_pipeline.event_contracts import FactKind
from news_pipeline.clustering import tokenize
from news_pipeline.fact_extraction import (
    canonical_decimal,
    extract_facts,
)


def _kinds(facts):
    return {tf.kind for tf in facts}


class TestFacts(unittest.TestCase):
    # 7 — addendum §3 currency & suffix
    def test_price_currency_and_suffix_normalization(self) -> None:
        facts = extract_facts("USD pricing announced", "First $10 then $10K then $5 million")
        units = {tf.unit for tf in facts.prices}
        self.assertEqual({"usd"}, units)
        vals = {tf.value_normalized for tf in facts.prices}
        self.assertIn("10", vals)
        self.assertIn("10000", vals)         # 10k
        self.assertIn("5000000", vals)       # 5 million

    # 8 — bounds and negative zero
    def test_price_bounds_and_negative_zero(self) -> None:
        # zero is invalid (must be > 0)
        f = extract_facts("boundary", "Price $0 and $1")
        vals = {tf.value_normalized for tf in f.prices}
        self.assertNotIn("0", vals)
        self.assertIn("1", vals)
        # canonical decimal format
        self.assertEqual("10", canonical_decimal(Decimal("10")))
        self.assertEqual("0.5", canonical_decimal(Decimal("0.50")))
        # negative zero rejected by canonical_decimal
        with self.assertRaises(ValueError):
            canonical_decimal(Decimal("-0"))

    # 9 — semver two and three components
    def test_semver_two_and_three_components(self) -> None:
        f = extract_facts("Versions shipped", "v1.2 and v3.4.5 also arrived v1.10")
        vals = {tf.value_normalized for tf in f.versions}
        self.assertIn("1.2", vals)
        self.assertIn("3.4.5", vals)
        self.assertIn("1.10", vals)

    # 10 — date requires year and validates calendar
    def test_date_requires_explicit_year_and_valid_calendar_date(self) -> None:
        f = extract_facts(
            "Date set",
            "ISO 2026-02-29 is invalid, 2026-02-28 valid, also January 2, 2026 ",
        )
        vals = {tf.value_normalized for tf in f.dates}
        self.assertIn("2026-02-28", vals)
        self.assertIn("2026-01-02", vals)
        self.assertNotIn("2026-02-29", vals)

    # 11 — percent bounds
    def test_percent_bounds(self) -> None:
        f = extract_facts(
            "Margin", "We saw 5% then 100% then 0% then 200% but 200 is out",
        )
        vals = {tf.value_normalized for tf in f.percentages}
        self.assertIn("5", vals)
        self.assertIn("100", vals)
        self.assertIn("0", vals)
        self.assertNotIn("200", vals)

    # 12 — contextual count units and free integers ignored
    def test_contextual_count_units_and_free_integer_ignored(self) -> None:
        f = extract_facts("Numbers", "1000 users and 42 alone with 250 subscribers")
        units = {tf.unit for tf in f.counts}
        self.assertIn("users", units)
        self.assertIn("subscribers", units)
        # Bare integer "42" should not be a count.
        all_vals = {tf.value_normalized for tf in f.counts}
        self.assertNotIn("42", all_vals)
        # Both counts normalized correctly.
        self.assertIn("1000", all_vals)
        self.assertIn("250", all_vals)

    # 13 — span precedence prevents double extraction
    def test_span_precedence_prevents_double_extraction(self) -> None:
        # The percent regex must not steal from a price; the price regex must
        # not steal from a date.
        f = extract_facts(
            "Promotion",
            "Offer $50 with 25% extra and a date 2026-01-01",
        )
        # We expect $50 (price) and "25%" (percent), and a date.
        price_evidences = " ".join(tf.evidence for tf in f.prices)
        self.assertNotIn("50%", price_evidences)
        # A date should be extracted independently of the price.
        date_vals = {tf.value_normalized for tf in f.dates}
        self.assertIn("2026-01-01", date_vals)
        # Percent must be present.
        percent_vals = {tf.value_normalized for tf in f.percentages}
        self.assertIn("25", percent_vals)

    # 14 — fact order and exact dedupe
    def test_fact_order_and_exact_dedupe(self) -> None:
        # Three identical "$10" mentions all inside a single surface (snippet)
        # should produce a single price fact, because context + evidence are
        # identical.
        f = extract_facts("Repeating", "First $10 then $10 again $10")
        prices = list(f.prices)
        ten_count = sum(1 for p in prices if p.value_normalized == "10")
        self.assertEqual(1, ten_count)
        # Each TypedFact must carry a FactKind constant.
        for tf in prices:
            self.assertEqual(tf.kind, FactKind.PRICE)
        # Sort key: (kind, unit, value, context, evidence) is preserved.
        keys = [(tf.kind.value, tf.unit, tf.value_normalized, tf.context, tf.evidence)
                for tf in prices]
        self.assertEqual(sorted(keys), keys)

# ------------------------------------------------------------------
    # F5 - Fact context always tokenize(title)[:8]
    # Parent finding 5: every extracted fact from title/snippet must carry
    # shared tokenize(title)[:8] context.
    # ------------------------------------------------------------------

    def test_F5_snippet_fact_uses_title_context(self):
        title = "Product alpha launch delta echo"
        snippet = "The price is $50"
        facts = extract_facts(title, snippet)
        # The snippet's price fact's context must come from the title.
        title_tokens = tokenize(title)[:8]
        # Verify the context tokens are title-only and the *exact* set.
        for tf in facts.prices:
            self.assertEqual(set(tf.context), set(title_tokens))
            self.assertEqual(sorted(tf.context), sorted(title_tokens))
            for tok in tf.context:
                self.assertIn(tok, title_tokens)

    def test_F5_title_fact_uses_title_context(self):
        title = "Beta pricing announced gamma"
        snippet = "completely different unrelated filler text"
        facts = extract_facts(title, snippet)
        title_tokens = tokenize(title)[:8]
        for tf in facts.prices:
            self.assertEqual(set(tf.context), set(title_tokens))

# ------------------------------------------------------------------
    # F6 - Count unit aliases normalize to canonical plural
    # Parent finding 6: every approved alias collapses to its canonical
    # plural. The test uses only "copies" (no singular "copy") since
    # copy is not an approved alias.
    # ------------------------------------------------------------------

    def test_F6_singular_and_plural_canonical_plural(self):
        # All six approved singular forms must produce canonical plural units.
        singular = extract_facts(
            "Headcount",
            "1000 user and 50 subscriber and 200 player and 10 employee and 5 unit and 7 job and 3 layoff",
        ).counts
        plural = extract_facts(
            "Headcount",
            "1000 users and 50 subscribers and 200 players and 10 employees and 5 units and 7 jobs and 3 layoffs",
        ).counts

        singular_pairs = sorted({(tf.unit, tf.value_normalized) for tf in singular})
        plural_pairs = sorted({(tf.unit, tf.value_normalized) for tf in plural})

        # All units in singular must be the canonical plural form.
        canonical_units = {"users", "subscribers", "players", "employees", "units", "jobs", "layoffs"}
        for unit, _ in singular_pairs:
            self.assertIn(unit, canonical_units, f"unit {unit!r} is not canonical plural")
            self.assertNotIn(unit.rstrip("s"), canonical_units)

        # Singular and plural sets must match exactly.
        self.assertEqual(singular_pairs, plural_pairs)

    def test_F6_copies_normalizes_only_copies(self):
        # "copies" is the canonical plural form; there is no singular
        # alias for it.
        facts = extract_facts("Distribution", "5000 copies shipped").counts
        self.assertEqual(len(facts), 1)
        self.assertEqual(facts[0].unit, "copies")
        self.assertEqual(facts[0].value_normalized, "5000")

# ------------------------------------------------------------------
    # F7 - PRICE_RE ending is Unicode-word boundary (?![\w.])
    # Parent finding 7: $100K is a valid price (suffix=K), $100Q is not,
    # and "abc$50" must not match because of the (?<!\w) prefix.
    # ------------------------------------------------------------------

    def test_F7_price_re_rejects_letter_suffix(self):
        facts = extract_facts("Boundary $100Q next $100K extra", "")
        prices = facts.prices
        vals = {tf.value_normalized for tf in prices}
        # $100K -> 100000; $100Q -> not a price.
        self.assertIn("100000", vals)
        self.assertNotIn("100", vals)

    def test_F7_price_re_starts_with_word_continuation_ignored(self):
        # "abc$50" must NOT match because of the (?<!\w) prefix boundary.
        prices = extract_facts("Strange abc$50 here", "").prices
        self.assertEqual(prices, ())

# ------------------------------------------------------------------
    # F8 - ISO dates enforce year 1900..2200 inclusive.
    # Parent finding 8: 1899 and 2201 are excluded; 1900 and 2200 are kept.
    # ------------------------------------------------------------------

    def test_F8_iso_dates_1899_2201_ignored(self):
        facts = extract_facts("Years", "Year 1899-06-15 then 2026-01-02 then 2201-12-31")
        vals = {tf.value_normalized for tf in facts.dates}
        self.assertNotIn("1899-06-15", vals)
        self.assertNotIn("2201-12-31", vals)
        self.assertIn("2026-01-02", vals)

    def test_F8_iso_date_1900_and_2200_kept(self):
        facts = extract_facts("Years", "Year 1900-01-01 and 2200-12-31")
        vals = {tf.value_normalized for tf in facts.dates}
        self.assertIn("1900-01-01", vals)
        self.assertIn("2200-12-31", vals)

if __name__ == "__main__":
    unittest.main()
