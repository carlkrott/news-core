"""Run 7 per-subject editorial QC boundary — unittest coverage.

This test module exercises ``news_pipeline.editorial_qc`` end to end.
Every fixture URL is an RFC 2606 ``example.com`` / ``example.org`` URL so
the tests never touch a real network destination.
"""
from __future__ import annotations

import unittest
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

from news_pipeline.editorial_qc import (
    EditorialQCError,
    EditorialQCReason,
    NARRATIVE_MAX_LEN,
    SUMMARY_MAX_LEN,
    SubjectEditorialInput,
    SubjectEditorialOutput,
    SubjectSummary,
    render_summary,
    subject_policy,
    validate_empty_subject_report,
    validate_subject_inputs,
    validate_subject_outputs,
)
from news_pipeline.event_contracts import FactDelta, FactKind
from news_pipeline.models import Category, Subject


# ---------------------------------------------------------------------------
# Lightweight fixtures
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _FakePolicy:
    """Stand-in for source_registry.SubjectPolicy.

    Only ``subject`` is read by editorial_qc; everything else is for
    realism so the test fixtures look like the real SubjectPolicy
    surface.
    """

    subject: Subject
    label: str = "fixture"
    included_in_report: bool = True
    recency_days: int = 7
    max_story_count: int = 8


def _fact_delta(
    kind: FactKind = FactKind.STATE,
    unit: str = "release",
    old_value: str = "announced",
    new_value: str = "shipped",
) -> FactDelta:
    return FactDelta(
        kind=kind,
        unit=unit,
        old_value=old_value,
        new_value=new_value,
        topic_gate=Decimal("0.95"),
    )


def _input(
    subject: Subject,
    *,
    event_id: str = "evt-001",
    event_version: int = 1,
    title: str = "Studio acquires new console",
    source_urls: Iterable[str] = ("https://example.com/av/story-1",),
    fact_deltas: Iterable[FactDelta] = (_fact_delta(),),
    policy: _FakePolicy | None = None,
) -> SubjectEditorialInput:
    if policy is None:
        policy = _FakePolicy(subject=subject)
    return SubjectEditorialInput(
        subject=subject,
        event_id=event_id,
        event_version=event_version,
        title=title,
        fact_deltas=tuple(fact_deltas),
        source_urls=tuple(source_urls),
        policy=policy,
    )


def _output(
    subject: Subject,
    *,
    event_id: str,
    event_version: int,
    what_changed: str = "Studio took delivery of a 32-channel console.",
    why_it_matters: str = "It expands the tracking footprint for touring clients.",
    source_url: str = "https://example.com/av/story-1",
    fact_deltas: Iterable[FactDelta] = (_fact_delta(),),
) -> SubjectEditorialOutput:
    return SubjectEditorialOutput(
        subject=subject,
        event_id=event_id,
        event_version=event_version,
        what_changed=what_changed,
        why_it_matters=why_it_matters,
        source_url=source_url,
        fact_deltas=tuple(fact_deltas),
    )


# ---------------------------------------------------------------------------
# subject_policy
# ---------------------------------------------------------------------------


class SubjectPolicyTests(unittest.TestCase):
    def test_audio_engineering_is_separate_from_professional_av(self) -> None:
        av = set(c.value for c in subject_policy(Subject.PROFESSIONAL_AV))
        audio = set(c.value for c in subject_policy(Subject.AUDIO_ENGINEERING))
        self.assertEqual(av, {"audiovisual", "av_corporate"})
        self.assertEqual(audio, {"audio_engineering"})
        self.assertFalse(audio & av)

    def test_professional_av_accepts_string(self) -> None:
        av = [c.value for c in subject_policy("professional_av")]
        self.assertEqual(av, ["audiovisual", "av_corporate"])

    def test_unknown_subject_raises(self) -> None:
        with self.assertRaises(ValueError):
            subject_policy("not-a-subject")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


class SubjectInputValidationTests(unittest.TestCase):
    def test_professional_av_accepts_audiovisual_input(self) -> None:
        item = _input(
            Subject.PROFESSIONAL_AV,
            source_urls=("https://example.com/audiovisual/story-1",),
        )
        out = validate_subject_inputs(Subject.PROFESSIONAL_AV, [item])
        self.assertEqual(len(out), 1)
        self.assertIs(out[0].subject, Subject.PROFESSIONAL_AV)

    def test_professional_av_accepts_av_corporate_input(self) -> None:
        item = _input(
            Subject.PROFESSIONAL_AV,
            source_urls=("https://example.com/av-corporate/story-1",),
        )
        out = validate_subject_inputs(Subject.PROFESSIONAL_AV, [item])
        self.assertEqual(len(out), 1)

    def test_audio_engineering_subject_rejects_professional_av_input(self) -> None:
        # audio_engineering and professional_av are strictly separate
        # per architecture §10.  An input tagged professional_av that
        # the caller tries to file under audio_engineering must be
        # rejected with the typed SUBJECT_MISMATCH reason.
        av_input = _input(Subject.PROFESSIONAL_AV)
        with self.assertRaises(EditorialQCError) as ctx:
            validate_subject_inputs(Subject.AUDIO_ENGINEERING, [av_input])
        self.assertEqual(ctx.exception.reason, EditorialQCReason.SUBJECT_MISMATCH)
        self.assertEqual(ctx.exception.subject, Subject.AUDIO_ENGINEERING)

    def test_subject_mismatch_is_typed(self) -> None:
        good = _input(Subject.AI)
        bad = SubjectEditorialInput(
            subject=Subject.HARDWARE,
            event_id="evt-hw-001",
            event_version=1,
            title="GPU refresh",
            fact_deltas=(_fact_delta(),),
            source_urls=("https://example.com/hw/gpu",),
            policy=_FakePolicy(subject=Subject.HARDWARE),
        )
        with self.assertRaises(EditorialQCError) as ctx:
            validate_subject_inputs(Subject.AI, [good, bad])
        self.assertEqual(ctx.exception.reason, EditorialQCReason.SUBJECT_MISMATCH)
        self.assertEqual(ctx.exception.subject, Subject.AI)

    def test_error_identity_contains_event_subject_and_version(self) -> None:
        bad = _input(Subject.PROFESSIONAL_AV, event_id="evt-av-identity")
        with self.assertRaises(EditorialQCError) as ctx:
            validate_subject_inputs(Subject.AI, [bad])
        self.assertEqual(
            ctx.exception.identity,
            ("evt-av-identity", Subject.PROFESSIONAL_AV.value, 1),
        )

    def test_duplicate_event_identity_rejected(self) -> None:
        a = _input(Subject.AI, event_id="evt-same", event_version=2)
        b = _input(Subject.AI, event_id="evt-same", event_version=2, title="Other")
        with self.assertRaises(EditorialQCError) as ctx:
            validate_subject_inputs(Subject.AI, [a, b])
        self.assertEqual(ctx.exception.reason, EditorialQCReason.DUPLICATE_EVENT_IDENTITY)

    def test_hallucinated_url_rejected_at_input(self) -> None:
        # urlparse-friendly but the URL scheme is not http(s) and must be rejected.
        with self.assertRaises(ValueError):
            SubjectEditorialInput(
                subject=Subject.AI,
                event_id="evt-bad-url",
                event_version=1,
                title="Bad URL",
                fact_deltas=(_fact_delta(),),
                source_urls=("ftp://example.com/x",),
                policy=_FakePolicy(subject=Subject.AI),
            )

    def test_empty_inputs_is_explicit_no_delivery(self) -> None:
        with self.assertRaises(EditorialQCError) as ctx:
            validate_subject_inputs(Subject.AI, [])
        self.assertEqual(ctx.exception.reason, EditorialQCReason.EMPTY_SUBJECT_REPORT)


# ---------------------------------------------------------------------------
# Output validation
# ---------------------------------------------------------------------------


class SubjectOutputValidationTests(unittest.TestCase):
    def test_valid_roundtrip(self) -> None:
        i1 = _input(
            Subject.PROFESSIONAL_AV,
            event_id="evt-av-1",
            source_urls=("https://example.com/av/story-1",),
        )
        i2 = _input(
            Subject.PROFESSIONAL_AV,
            event_id="evt-av-2",
            source_urls=("https://example.com/av/story-2",),
        )
        o1 = _output(
            Subject.PROFESSIONAL_AV,
            event_id="evt-av-1",
            event_version=1,
            source_url="https://example.com/av/story-1",
        )
        o2 = _output(
            Subject.PROFESSIONAL_AV,
            event_id="evt-av-2",
            event_version=1,
            source_url="https://example.com/av/story-2",
        )
        out = validate_subject_outputs(Subject.PROFESSIONAL_AV, [i1, i2], [o1, o2])
        self.assertEqual(len(out), 2)

    def test_hallucinated_url_on_output(self) -> None:
        i1 = _input(Subject.AI, event_id="evt-1", source_urls=("https://example.com/ai/model",))
        bad = SubjectEditorialOutput(
            subject=Subject.AI,
            event_id="evt-1",
            event_version=1,
            what_changed="Model release notes updated.",
            why_it_matters="It changes prompt ergonomics for engineers.",
            source_url="https://example.org/never-cited",
            fact_deltas=(_fact_delta(),),
        )
        with self.assertRaises(EditorialQCError) as ctx:
            validate_subject_outputs(Subject.AI, [i1], [bad])
        self.assertEqual(ctx.exception.reason, EditorialQCReason.HALLUCINATED_URL)

    def test_missing_event_id_in_output_against_input(self) -> None:
        i1 = _input(Subject.AI, event_id="evt-1")
        bad = SubjectEditorialOutput(
            subject=Subject.AI,
            event_id="evt-OTHER",
            event_version=1,
            what_changed="Model release notes updated.",
            why_it_matters="It changes prompt ergonomics for engineers.",
            source_url="https://example.com/av/story-1",
            fact_deltas=(_fact_delta(),),
        )
        with self.assertRaises(EditorialQCError) as ctx:
            validate_subject_outputs(Subject.AI, [i1], [bad])
        self.assertEqual(ctx.exception.reason, EditorialQCReason.MISSING_EVENT_IDENTITY)

    def test_duplicate_event_id_across_outputs(self) -> None:
        # Inputs have distinct identities so input validation passes.
        i1 = _input(Subject.AI, event_id="evt-1")
        i2 = _input(Subject.AI, event_id="evt-2")
        # Both outputs then collide on the same identity — outputs side
        # must reject this even though the inputs are well-formed.
        o1 = _output(Subject.AI, event_id="evt-1", event_version=1)
        o2 = _output(Subject.AI, event_id="evt-1", event_version=1)
        with self.assertRaises(EditorialQCError) as ctx:
            validate_subject_outputs(Subject.AI, [i1, i2], [o1, o2])
        self.assertEqual(ctx.exception.reason, EditorialQCReason.DUPLICATE_EVENT_IDENTITY)

    def test_filler_preamble_rejected(self) -> None:
        i1 = _input(Subject.AI, event_id="evt-1", source_urls=("https://example.com/ai/story",))
        bad = SubjectEditorialOutput(
            subject=Subject.AI,
            event_id="evt-1",
            event_version=1,
            what_changed="In conclusion, the model release notes were updated.",
            why_it_matters="It changes prompt ergonomics for engineers.",
            source_url="https://example.com/ai/story",
            fact_deltas=(_fact_delta(),),
        )
        with self.assertRaises(EditorialQCError) as ctx:
            validate_subject_outputs(Subject.AI, [i1], [bad])
        self.assertEqual(ctx.exception.reason, EditorialQCReason.FILLER_PREAMBLE)

    def test_marketing_preamble_rejected(self) -> None:
        i1 = _input(Subject.AI, event_id="evt-1", source_urls=("https://example.com/ai/story",))
        bad = SubjectEditorialOutput(
            subject=Subject.AI,
            event_id="evt-1",
            event_version=1,
            what_changed="Announcing the launch of a brand-new model.",
            why_it_matters="It changes prompt ergonomics for engineers.",
            source_url="https://example.com/ai/story",
            fact_deltas=(_fact_delta(),),
        )
        with self.assertRaises(EditorialQCError) as ctx:
            validate_subject_outputs(Subject.AI, [i1], [bad])
        self.assertEqual(ctx.exception.reason, EditorialQCReason.FILLER_PREAMBLE)

    def test_fact_delta_mismatch_rejected(self) -> None:
        i1 = _input(
            Subject.AI,
            event_id="evt-1",
            source_urls=("https://example.com/ai/story",),
            fact_deltas=(_fact_delta(),),
        )
        bad = SubjectEditorialOutput(
            subject=Subject.AI,
            event_id="evt-1",
            event_version=1,
            what_changed="Model release notes updated.",
            why_it_matters="It changes prompt ergonomics for engineers.",
            source_url="https://example.com/ai/story",
            fact_deltas=(_fact_delta(unit="training_run"),),
        )
        with self.assertRaises(EditorialQCError) as ctx:
            validate_subject_outputs(Subject.AI, [i1], [bad])
        self.assertEqual(ctx.exception.reason, EditorialQCReason.FACT_DELTA_MISMATCH)

    def test_narrative_url_outside_allowed_sources(self) -> None:
        i1 = _input(
            Subject.AI,
            event_id="evt-1",
            source_urls=("https://example.com/ai/model",),
        )
        bad = SubjectEditorialOutput(
            subject=Subject.AI,
            event_id="evt-1",
            event_version=1,
            what_changed="Model release notes updated; see https://example.org/leak.",
            why_it_matters="It changes prompt ergonomics for engineers.",
            source_url="https://example.com/ai/model",
            fact_deltas=(_fact_delta(),),
        )
        with self.assertRaises(EditorialQCError) as ctx:
            validate_subject_outputs(Subject.AI, [i1], [bad])
        self.assertEqual(ctx.exception.reason, EditorialQCReason.HALLUCINATED_URL)

    def test_narrative_controls_are_rejected_not_scrubbed(self) -> None:
        i1 = _input(Subject.AI, event_id="evt-1", source_urls=("https://example.com/ai/story",))
        bad = _output(
            Subject.AI,
            event_id="evt-1",
            event_version=1,
            source_url="https://example.com/ai/story",
            what_changed="Model release\u202e notes updated.",
        )
        with self.assertRaises(EditorialQCError) as ctx:
            validate_subject_outputs(Subject.AI, [i1], [bad])
        self.assertEqual(ctx.exception.reason, EditorialQCReason.FACT_DELTA_MISMATCH)

    def test_narrative_too_long_rejected(self) -> None:
        i1 = _input(Subject.AI, event_id="evt-1", source_urls=("https://example.com/ai/story",))
        too_long = "x" * (NARRATIVE_MAX_LEN + 1)
        bad = SubjectEditorialOutput(
            subject=Subject.AI,
            event_id="evt-1",
            event_version=1,
            what_changed=too_long,
            why_it_matters="It changes prompt ergonomics for engineers.",
            source_url="https://example.com/ai/story",
            fact_deltas=(_fact_delta(),),
        )
        with self.assertRaises(EditorialQCError) as ctx:
            validate_subject_outputs(Subject.AI, [i1], [bad])
        # Misalignment surfaces as FACT_DELTA_MISMATCH per design (count
        # drift and length drift share the typed reason; alignment is
        # the contract).
        self.assertEqual(ctx.exception.reason, EditorialQCReason.FACT_DELTA_MISMATCH)

    def test_one_to_one_alignment_required(self) -> None:
        i1 = _input(Subject.AI, event_id="evt-1")
        i2 = _input(Subject.AI, event_id="evt-2")
        o1 = _output(Subject.AI, event_id="evt-1", event_version=1)
        with self.assertRaises(EditorialQCError) as ctx:
            validate_subject_outputs(Subject.AI, [i1, i2], [o1])
        self.assertEqual(ctx.exception.reason, EditorialQCReason.FACT_DELTA_MISMATCH)


# ---------------------------------------------------------------------------
# All-subject input rejection
# ---------------------------------------------------------------------------


class AllSubjectInputTests(unittest.TestCase):
    def test_all_subjects_marker_rejected(self) -> None:
        # The all-subjects marker is encoded as the "<ALL SUBJECTS>"
        # sentinel title per the architecture contract.
        bad = SubjectEditorialInput(
            subject=Subject.AI,
            event_id="evt-all-1",
            event_version=1,
            title="<ALL SUBJECTS>",
            fact_deltas=(_fact_delta(),),
            source_urls=("https://example.com/ai/all",),
            policy=_FakePolicy(subject=Subject.AI),
        )
        with self.assertRaises(EditorialQCError) as ctx:
            validate_subject_inputs(Subject.AI, [bad])
        self.assertEqual(ctx.exception.reason, EditorialQCReason.ALL_SUBJECT_INPUT)


# ---------------------------------------------------------------------------
# Empty subject report / zero-story no-delivery
# ---------------------------------------------------------------------------


class EmptySubjectReportTests(unittest.TestCase):
    def test_zero_story_with_no_inputs_is_valid(self) -> None:
        validate_empty_subject_report(Subject.AI, [], outputs=())

    def test_zero_story_no_delivery_valid(self) -> None:
        i1 = _input(Subject.AI, event_id="evt-empty", fact_deltas=())
        # Empty fact_deltas on every input + empty outputs = explicit no-delivery.
        validate_empty_subject_report(Subject.AI, [i1], outputs=())

    def test_zero_story_with_outputs_rejected(self) -> None:
        i1 = _input(Subject.AI, event_id="evt-empty", fact_deltas=())
        o1 = _output(Subject.AI, event_id="evt-empty", event_version=1)
        with self.assertRaises(EditorialQCError) as ctx:
            validate_empty_subject_report(Subject.AI, [i1], outputs=[o1])
        self.assertEqual(ctx.exception.reason, EditorialQCReason.FACT_DELTA_MISMATCH)

    def test_zero_story_with_nonempty_facts_rejected(self) -> None:
        # An input with facts and zero outputs is NOT a no-delivery —
        # it's a report that silently dropped its stories.
        i1 = _input(Subject.AI, event_id="evt-1", fact_deltas=(_fact_delta(),))
        with self.assertRaises(EditorialQCError) as ctx:
            validate_empty_subject_report(Subject.AI, [i1], outputs=())
        self.assertEqual(ctx.exception.reason, EditorialQCReason.EMPTY_SUBJECT_REPORT)


# ---------------------------------------------------------------------------
# render_summary
# ---------------------------------------------------------------------------


class RenderSummaryTests(unittest.TestCase):
    def test_render_combines_and_caps(self) -> None:
        i1 = _input(Subject.AI, event_id="evt-1", source_urls=("https://example.com/ai/story-1",))
        i2 = _input(Subject.AI, event_id="evt-2", source_urls=("https://example.com/ai/story-2",))
        o1 = _output(
            Subject.AI,
            event_id="evt-1",
            event_version=1,
            source_url="https://example.com/ai/story-1",
        )
        o2 = _output(
            Subject.AI,
            event_id="evt-2",
            event_version=1,
            source_url="https://example.com/ai/story-2",
        )
        validate_subject_outputs(Subject.AI, [i1, i2], [o1, o2])
        summary = render_summary([o1, o2])
        self.assertIsInstance(summary, SubjectSummary)
        self.assertIs(summary.subject, Subject.AI)
        self.assertLessEqual(len(summary.text), SUMMARY_MAX_LEN)
        self.assertIn("evt-1", summary.text)
        self.assertIn("evt-2", summary.text)
        self.assertEqual(
            summary.sources,
            ("https://example.com/ai/story-1", "https://example.com/ai/story-2"),
        )

    def test_render_empty_outputs_emits_no_delivery_marker(self) -> None:
        summary = render_summary([])
        self.assertIn("[no-delivery]", summary.text)
        self.assertEqual(summary.sources, ())


if __name__ == "__main__":
    unittest.main()
