"""Phase 4 — Slice 4 renderer tests.

Twelve unittest methods for ``news_pipeline.briefing_renderer`` per the
Phase 4 supervisor addendum §4 and §6 (tests 56-67). Slice 4 owns only this
file plus ``news_pipeline.briefing_renderer``; every other Phase 4 file is
forbidden here, and no Phase 1-3 / wrapper / cron / service / DB file may
be touched.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import unittest

from news_pipeline import briefing_renderer
from news_pipeline.event_contracts import FactDelta, FactKind, SemanticDecision, SemanticReasonCode
from news_pipeline.models import Category


# ---------------------------------------------------------------------------
# Test fixtures — local renderer surface only
# ---------------------------------------------------------------------------


def _upper() -> datetime:
    return datetime(2026, 7, 15, 7, 0, tzinfo=timezone.utc)


def _record(
    candidate_id: str = "cand-1",
    category: Category = Category.AI,
    decision: SemanticDecision = SemanticDecision.distinct_event,
    title: str = "A meaningful headline",
    summary: str = "A meaningful summary",
    url: str | None = "https://example.test/article",
    semantic_reasons: tuple[SemanticReasonCode, ...] = (
        SemanticReasonCode.DISTINCT_EVENT,
    ),
    fact_deltas: tuple[FactDelta, ...] = (),
    ordinal: int = 0,
) -> briefing_renderer.RenderRecord:
    return briefing_renderer.RenderRecord(
        candidate_id=candidate_id,
        category=category,
        decision=decision,
        title=title,
        summary=summary,
        url=url,
        semantic_reasons=semantic_reasons,
        fact_deltas=fact_deltas,
        ordinal=ordinal,
    )


def _render(*records: briefing_renderer.RenderRecord, parse_mode=None):
    return briefing_renderer.render_briefing(
        records=records,
        upper_bound_utc=_upper(),
        parse_mode=parse_mode,
    )


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestBriefingRenderer(unittest.TestCase):
    # 56
    def test_utf16_counter_counts_astral_character_as_two(self) -> None:
        self.assertEqual(briefing_renderer.utf16_units("A"), 1)
        self.assertEqual(briefing_renderer.utf16_units("😀"), 2)
        self.assertEqual(briefing_renderer.utf16_units("A😀B"), 4)

    # 57
    def test_normalization_removes_controls_bidi_and_collapses_space(self) -> None:
        record = _record(
            title="  Hello\tworld\n\u202e\u0001  ",
            summary="\rSummary\u2066  with\tspaces\n\u0002here  ",
            url="https://example.test/article",
        )
        self.assertEqual(record.title, "Hello world")
        self.assertEqual(record.summary, "Summary with spaces here")

    # 58
    def test_url_requires_exact_scheme_netloc_and_no_controls(self) -> None:
        briefing_renderer.RenderRecord(
            candidate_id="cand-url-ok",
            category=Category.AI,
            decision=SemanticDecision.distinct_event,
            title="ok",
            summary="ok",
            url="https://example.test/path?x=1#frag",
            semantic_reasons=(SemanticReasonCode.DISTINCT_EVENT,),
            fact_deltas=(),
            ordinal=0,
        )
        for bad in (
            "HTTP://example.test/path",
            "https://",
            "https://?next=/x",
            "http:///path",
            "https://example .test/path",
            "https://example.test/path\nmore",
            "ftp://example.test/path",
        ):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    _record(url=bad)
        with self.assertRaises(ValueError):
            _record(candidate_id="x" * 257)

    # 59
    def test_new_and_update_record_grammar(self) -> None:
        facts = (
            FactDelta(
                kind=FactKind.PRICE,
                unit="  USD ",
                old_value="\t1\n",
                new_value="\r2 ",
                topic_gate=Decimal("0.5"),
            ),
        )
        result = _render(
            _record(
                candidate_id="cand-new",
                decision=SemanticDecision.distinct_event,
                title="  New headline\t",
                summary="New summary\n",
                url="https://example.test/new",
                semantic_reasons=(SemanticReasonCode.DISTINCT_EVENT,),
                fact_deltas=facts,
                ordinal=0,
            ),
            _record(
                candidate_id="cand-update",
                decision=SemanticDecision.material_update,
                title="Update headline",
                summary="Update summary",
                url=None,
                semantic_reasons=(
                    SemanticReasonCode.NUMERIC_REVISION,
                    SemanticReasonCode.SAME_FACTS,
                ),
                fact_deltas=(),
                ordinal=1,
            ),
        )
        self.assertEqual(result.status, briefing_renderer.RenderStatus.RENDERED)
        self.assertEqual(len(result.chunks), 1)
        self.assertEqual(
            result.chunks[0],
            "Morning briefing — 2026-07-15\n\n"
            "AI\n\n"
            "[NEW] New headline\n"
            "New summary\n"
            "https://example.test/new\n"
            "Reasons: DISTINCT_EVENT\n"
            "Facts: kind=price, unit=USD, old_value=1, new_value=2, topic_gate=0.5\n\n"
            "[UPDATE] Update headline\n"
            "Update summary\n"
            "(no URL)\n"
            "Reasons: NUMERIC_REVISION, SAME_FACTS\n"
            "Facts: none",
        )

    # 60
    def test_category_labels_and_declaration_order(self) -> None:
        records = tuple(
            _record(
                candidate_id=f"cand-{category.name.lower()}",
                category=category,
                title=f"Title {category.name}",
                summary=f"Summary {category.name}",
                url=None,
                semantic_reasons=(SemanticReasonCode.DISTINCT_EVENT,),
                fact_deltas=(),
                ordinal=index,
            )
            for index, category in enumerate(
                (
                    Category.OUR_SETUP,
                    Category.AV_CORPORATE,
                    Category.AUDIOVISUAL,
                    Category.FANTASY_NOVEL,
                    Category.HARDWARE,
                    Category.AUDIO_ENGINEERING,
                    Category.WORLD,
                    Category.AI,
                )
            )
        )
        result = _render(*records)
        paragraphs = result.chunks[0].split("\n\n")
        self.assertEqual(paragraphs[0], "Morning briefing — 2026-07-15")
        self.assertEqual(
            paragraphs[1::2],
            [
                "AI",
                "World",
                "Audio Engineering",
                "Hardware",
                "Fantasy Novel",
                "Audiovisual",
                "AV Corporate",
                "Our Setup",
            ],
        )

    # 61
    def test_sort_uses_ordinal_then_candidate_id(self) -> None:
        records = (
            _record(
                candidate_id="cand-b",
                category=Category.AI,
                title="Title B",
                summary="Summary B",
                url=None,
                ordinal=2,
            ),
            _record(
                candidate_id="cand-c",
                category=Category.AI,
                title="Title C",
                summary="Summary C",
                url=None,
                ordinal=1,
            ),
            _record(
                candidate_id="cand-a",
                category=Category.AI,
                title="Title A",
                summary="Summary A",
                url=None,
                ordinal=1,
            ),
        )
        result = _render(*records)
        paragraphs = result.chunks[0].split("\n\n")
        record_paragraphs = paragraphs[2:]
        self.assertEqual(
            [p.split("\n", 1)[0] for p in record_paragraphs],
            ["[NEW] Title A", "[NEW] Title C", "[NEW] Title B"],
        )

    # 62
    def test_fact_delta_rendering_uses_declared_fields(self) -> None:
        facts = (
            FactDelta(
                kind=FactKind.VERSION,
                unit=" release ",
                old_value=" 1.0 \t",
                new_value="\n2.0",
                topic_gate=Decimal("0.75"),
            ),
            FactDelta(
                kind=FactKind.COUNT,
                unit=" items ",
                old_value=" 5 ",
                new_value=" 6 ",
                topic_gate=Decimal("1"),
            ),
        )
        result = _render(
            _record(
                candidate_id="cand-facts",
                category=Category.AI,
                title="Facts headline",
                summary="Facts summary",
                url=None,
                fact_deltas=facts,
            )
        )
        self.assertIn(
            "Facts: kind=version, unit=release, old_value=1.0, new_value=2.0, topic_gate=0.75, "
            "kind=count, unit=items, old_value=5, new_value=6, topic_gate=1",
            result.chunks[0],
        )

    # 63
    def test_chunks_never_exceed_4000_utf16_units(self) -> None:
        records = tuple(
            _record(
                candidate_id=f"cand-{index}",
                category=Category.AI,
                title=f"Headline {index}",
                summary="s" * 180,
                url=None,
                ordinal=index,
            )
            for index in range(18)
        )
        result = _render(*records)
        self.assertGreater(len(result.chunks), 1)
        for chunk in result.chunks:
            self.assertLessEqual(briefing_renderer.utf16_units(chunk), 4000)

    # 64
    def test_chunks_split_only_at_complete_paragraphs_and_repeat_header(self) -> None:
        records = tuple(
            _record(
                candidate_id=f"cand-{index}",
                category=Category.AI,
                title=f"Headline {index}",
                summary="s" * 190,
                url=None,
                ordinal=index,
            )
            for index in range(16)
        )
        result = _render(*records)
        self.assertGreater(len(result.chunks), 1)
        for chunk in result.chunks:
            paragraphs = chunk.split("\n\n")
            self.assertEqual(paragraphs[0], "Morning briefing — 2026-07-15")
            self.assertEqual(paragraphs[1], "AI")
            for paragraph in paragraphs[2:]:
                lines = paragraph.split("\n")
                self.assertTrue(lines[0].startswith(("[NEW]", "[UPDATE]")))
                self.assertEqual(len(lines), 5)
                self.assertTrue(lines[3].startswith("Reasons: "))
                self.assertTrue(lines[4].startswith("Facts: "))

    # 65
    def test_oversize_record_raises_with_candidate_id(self) -> None:
        huge = _record(
            candidate_id="cand-oversize",
            category=Category.AI,
            title="H" * 100,
            summary="S" * 4100,
            url=None,
        )
        with self.assertRaises(briefing_renderer.OversizeRecordError) as ctx:
            _render(huge)
        self.assertEqual(ctx.exception.candidate_id, "cand-oversize")

    # 66
    def test_empty_records_return_empty_status_and_chunks(self) -> None:
        result = _render()
        self.assertEqual(result.status, briefing_renderer.RenderStatus.EMPTY)
        self.assertEqual(result.chunks, ())

    # 67
    def test_same_input_is_byte_identical(self) -> None:
        records = (
            _record(
                candidate_id="cand-a",
                category=Category.AI,
                title="Alpha",
                summary="Summary A",
                url="https://example.test/a",
                ordinal=1,
            ),
            _record(
                candidate_id="cand-b",
                category=Category.WORLD,
                title="Beta",
                summary="Summary B",
                url=None,
                ordinal=0,
            ),
        )
        first = _render(*records, parse_mode=None)
        second = _render(*records, parse_mode=None)
        self.assertEqual(first, second)
        self.assertEqual(first.chunks, second.chunks)
        self.assertEqual(first.chunks[0].encode("utf-8"), second.chunks[0].encode("utf-8"))
