"""Run 7 deterministic per-subject editorial QC boundary.

This module is a pure, stdlib-only editorial quality-control seam between the
event-level verification output and the downstream subject report renderer.

It reuses the existing :class:`Subject` enum from
``news_pipeline.models`` and the frozen :class:`FactDelta` records from
``news_pipeline.event_contracts``.  It never imports broker adapters,
database handles, or any live model client.  Every check is deterministic
and the public helpers raise :class:`EditorialQCError` with a typed
``reason`` member so callers can branch without parsing message strings.

The module enforces a strict one-to-one correspondence between an ordered
list of editorial inputs and an ordered list of editorial outputs that
share the same subject.  It also renders a concise combined summary
suitable for the per-subject briefing, and exposes a
``subject_policy(subject)`` helper that returns the allowed input
category set for a subject (mirroring the architecture table).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable, Mapping
from urllib.parse import urlparse

from .event_contracts import FactDelta
from .models import CATEGORY_TO_SUBJECT, Category, Subject


# ---------------------------------------------------------------------------
# Typed error reasons
# ---------------------------------------------------------------------------


class EditorialQCReason(str, Enum):
    """Stable reasons a subject editorial boundary check can fail.

    Callers branch on ``reason`` (or compare via the string ``value``);
    human messages are descriptive but the enum is the API.
    """

    ALL_SUBJECT_INPUT = "all_subject_input"
    SUBJECT_MISMATCH = "subject_mismatch"
    MISSING_EVENT_IDENTITY = "missing_event_identity"
    DUPLICATE_EVENT_IDENTITY = "duplicate_event_identity"
    HALLUCINATED_URL = "hallucinated_url"
    FACT_DELTA_MISMATCH = "fact_delta_mismatch"
    FILLER_PREAMBLE = "filler_preamble"
    EMPTY_SUBJECT_REPORT = "empty_subject_report"


@dataclass(frozen=True, slots=True)
class EditorialQCError(Exception):
    """Raised when a subject editorial QC check fails.

    ``reason`` is the stable, machine-readable code; ``message`` is a
    human description intended for log lines and tests.  ``subject`` is
    set when the failure is bound to one subject; ``identity`` is set
    when a per-event collision was detected.
    """

    reason: EditorialQCReason
    message: str
    subject: Subject | None = None
    identity: tuple[str, str, int] | None = None

    def __str__(self) -> str:  # pragma: no cover - trivial wrapper
        return f"{self.reason.value}: {self.message}"


# ---------------------------------------------------------------------------
# Subject policy: which ingest categories may legally report under one subject
# ---------------------------------------------------------------------------


# Mirrors the architecture table in ARCHITECTURE.md section 10.  The
# ``professional_av`` subject aggregates two ingest categories that share
# the same downstream editorial scope; every other subject is one-to-one.
_SUBJECT_ALLOWED_CATEGORIES: Mapping[Subject, tuple[str, ...]] = {
    Subject.WORLD: ("world",),
    Subject.AI: ("ai",),
    Subject.AUDIO_ENGINEERING: ("audio_engineering",),
    Subject.PROFESSIONAL_AV: ("audiovisual", "av_corporate"),
    Subject.HARDWARE: ("hardware",),
    Subject.FANTASY_NOVEL: ("fantasy_novel",),
    Subject.OUR_SETUP: ("our_setup",),
}


def subject_policy(subject: Subject | str) -> tuple[Category, ...]:
    """Return the ordered, immutable tuple of ingest categories that may
    legally report under ``subject``.

    Raises :class:`ValueError` if ``subject`` is not a known Subject.
    """
    try:
        normalized = subject if isinstance(subject, Subject) else Subject(subject)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"unknown subject: {subject!r}") from exc
    allowed_values = _SUBJECT_ALLOWED_CATEGORIES[normalized]
    return tuple(Category(value) for value in allowed_values)


# ---------------------------------------------------------------------------
# Narrative constraints
# ---------------------------------------------------------------------------


NARRATIVE_MIN_LEN = 1
NARRATIVE_MAX_LEN = 256
SUMMARY_MAX_LEN = 512


# Strip ANSI control bytes and bidirectional overrides that some editors
# quietly insert; this stays in sync with the live contracts identity
# scrubber.  We do not need the full identity normaliser here because
# narrative prose is not an identity token.
_CONTROL_CHARS_RE = re.compile(
    r"[\x00-\x1f\x7f-\x9f\u061c\u200e\u200f\u202a-\u202e\u2066-\u2069]"
)


# Conservative prose preamble detection.  Anything in this set as the
# first sentence of either ``what_changed`` or ``why_it_matters`` is
# treated as filler / marketing copy that violates the report contract.
_FILLER_PREAMBLES: frozenset[str] = frozenset(
    {
        # Marketing / hype
        "in conclusion",
        "conclusion:",
        "summary:",
        "in summary",
        "to summarize",
        "in this article",
        "in this report",
        "in this briefing",
        "this article",
        "this report",
        "this briefing",
        "welcome to",
        "thank you for reading",
        "thanks for reading",
        "we are pleased to announce",
        "we are excited to announce",
        "we are thrilled to announce",
        "we're pleased to announce",
        "we're excited to announce",
        "we're thrilled to announce",
        "stay tuned",
        "don't miss",
        "do not miss",
        "as always",
        "lastly",
        "finally,",
        "in a nutshell",
        "at the end of the day",
        "overall,",
        "to wrap up",
        "to wrap things up",
        # Press release / announcement templates
        "for immediate release",
        "press release:",
        "announcement:",
        "announcing the launch of",
        "announcing the release of",
        "introducing the new",
        "the all-new",
        "the revolutionary",
        "the groundbreaking",
        "the next-generation",
        "next-generation",
        "cutting-edge",
        "state-of-the-art",
        "world-class",
        "industry-leading",
        "best-in-class",
    }
)


def _scrub_narrative(text: str) -> str:
    """Remove control bytes from a narrative string."""
    return _CONTROL_CHARS_RE.sub("", text)


def _leading_clause(text: str) -> str:
    """Return the first clause (up to ``.``, ``!``, ``?``, ``:``, or ``;``)
    of ``text`` lower-cased and whitespace-collapsed."""
    cleaned = " ".join(text.strip().split())
    if not cleaned:
        return ""
    cut_chars = {".", "!", "?", ":", ";", ","}
    end = len(cleaned)
    for index, char in enumerate(cleaned):
        if char in cut_chars:
            end = index
            break
    return cleaned[:end].lower().strip()


def _looks_like_filler_preamble(text: str) -> bool:
    clause = _leading_clause(text)
    if not clause:
        return False
    # Exact-prefix match against the curated set.
    for marker in _FILLER_PREAMBLES:
        if clause == marker:
            return True
        if clause.startswith(marker + " "):
            return True
    # Generic "introducing/announcing ... today" patterns.
    generic_prefixes = (
        "introducing ",
        "announcing ",
        "today we ",
        "we are proud to ",
        "we're proud to ",
    )
    for prefix in generic_prefixes:
        if clause.startswith(prefix):
            return True
    return False


# ---------------------------------------------------------------------------
# URL validation helpers
# ---------------------------------------------------------------------------


_ALLOWED_SCHEMES: frozenset[str] = frozenset({"http", "https"})


def _is_valid_http_url(url: object) -> bool:
    """Return True iff ``url`` is an ``http://`` or ``https://`` URL with a
    non-empty host.  Anything else (file://, ftp://, gopher://, malformed
    strings, ``None``, non-string types) is rejected."""
    if not isinstance(url, str) or not url:
        return False
    if len(url) > 2048 or any(ch.isspace() for ch in url) or _CONTROL_CHARS_RE.search(url):
        return False
    try:
        parsed = urlparse(url)
    except (TypeError, ValueError):
        return False
    if parsed.scheme not in _ALLOWED_SCHEMES:
        return False
    if not parsed.netloc:
        return False
    # urlparse silently accepts "http://" with empty host; reject that.
    if parsed.netloc in {"", "/"}:
        return False
    return True


def _extract_narrative_urls(text: str) -> tuple[str, ...]:
    """Best-effort extraction of http/https URLs from a narrative string.

    Only http and https schemes are returned.  This is intentionally
    conservative — URLs inside angle brackets, parentheses, or quoted
    prose are all matched.
    """
    if not isinstance(text, str):
        return ()
    pattern = re.compile(r"https?://[^\s<>\"')\]]+")
    return tuple(pattern.findall(text))


# ---------------------------------------------------------------------------
# Frozen / slotted input and output types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SubjectEditorialInput:
    """One event-version's worth of editorial input for a subject report.

    ``subject`` is the reporting subject (NOT a category).  ``policy`` is
    the subject's typed policy (e.g. max_story_count, allowed categories).
    """

    subject: Subject
    event_id: str
    event_version: int
    title: str
    fact_deltas: tuple[FactDelta, ...]
    source_urls: tuple[str, ...]
    policy: object  # SubjectPolicy; structural duck typing avoids an import cycle

    def __post_init__(self) -> None:
        if not isinstance(self.subject, Subject):
            raise ValueError("SubjectEditorialInput.subject must be a Subject")
        if (
            not isinstance(self.event_id, str)
            or not self.event_id
            or len(self.event_id) > 256
            or _CONTROL_CHARS_RE.search(self.event_id)
        ):
            raise ValueError("SubjectEditorialInput.event_id must be a non-empty string")
        if not isinstance(self.event_version, int) or isinstance(self.event_version, bool) or self.event_version < 1:
            raise ValueError("SubjectEditorialInput.event_version must be an int >= 1")
        if (
            not isinstance(self.title, str)
            or not (1 <= len(self.title) <= 512)
            or _CONTROL_CHARS_RE.search(self.title)
        ):
            raise ValueError("SubjectEditorialInput.title must be 1..512 clean characters")
        if type(self.fact_deltas) is not tuple:
            raise ValueError("SubjectEditorialInput.fact_deltas must be a tuple")
        for delta in self.fact_deltas:
            if not isinstance(delta, FactDelta):
                raise ValueError("SubjectEditorialInput.fact_deltas entries must be FactDelta")
        if type(self.source_urls) is not tuple:
            raise ValueError("SubjectEditorialInput.source_urls must be a tuple")
        for url in self.source_urls:
            if not _is_valid_http_url(url):
                raise ValueError(f"SubjectEditorialInput.source_urls contains invalid URL: {url!r}")


@dataclass(frozen=True, slots=True)
class SubjectEditorialOutput:
    """One event-version's worth of editorial output for a subject report.

    ``what_changed`` and ``why_it_matters`` are constrained to 1..256
    characters after control-byte scrubbing and reject filler/marketing
    preambles.  ``source_url`` must byte-match one of the input's
    ``source_urls``; ``fact_deltas`` must equal the input's
    ``fact_deltas``.
    """

    subject: Subject
    event_id: str
    event_version: int
    what_changed: str
    why_it_matters: str
    source_url: str
    fact_deltas: tuple[FactDelta, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.subject, Subject):
            raise ValueError("SubjectEditorialOutput.subject must be a Subject")
        if (
            not isinstance(self.event_id, str)
            or not self.event_id
            or len(self.event_id) > 256
            or _CONTROL_CHARS_RE.search(self.event_id)
        ):
            raise ValueError("SubjectEditorialOutput.event_id must be a non-empty string")
        if not isinstance(self.event_version, int) or isinstance(self.event_version, bool) or self.event_version < 1:
            raise ValueError("SubjectEditorialOutput.event_version must be an int >= 1")
        if not isinstance(self.what_changed, str):
            raise ValueError("SubjectEditorialOutput.what_changed must be a string")
        if not isinstance(self.why_it_matters, str):
            raise ValueError("SubjectEditorialOutput.why_it_matters must be a string")
        if not _is_valid_http_url(self.source_url):
            raise ValueError(f"SubjectEditorialOutput.source_url is not a valid http(s) URL: {self.source_url!r}")
        if type(self.fact_deltas) is not tuple:
            raise ValueError("SubjectEditorialOutput.fact_deltas must be a tuple")
        for delta in self.fact_deltas:
            if not isinstance(delta, FactDelta):
                raise ValueError("SubjectEditorialOutput.fact_deltas entries must be FactDelta")


# ---------------------------------------------------------------------------
# Validation entry points
# ---------------------------------------------------------------------------


def _normalized_input_list(
    subject: Subject | str,
    inputs: Iterable[SubjectEditorialInput],
) -> tuple[SubjectEditorialInput, ...]:
    if not isinstance(subject, Subject):
        try:
            subject = Subject(subject)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"unknown subject: {subject!r}") from exc
    materialized = tuple(inputs)
    if not materialized:
        raise EditorialQCError(
            EditorialQCReason.EMPTY_SUBJECT_REPORT,
            f"empty editorial inputs for subject {subject.value!r}",
            subject=subject,
        )
    for item in materialized:
        if not isinstance(item, SubjectEditorialInput):
            raise ValueError("inputs must be SubjectEditorialInput instances")
    return materialized


def validate_subject_inputs(
    subject: Subject | str,
    inputs: Iterable[SubjectEditorialInput],
) -> tuple[SubjectEditorialInput, ...]:
    """Validate that ``inputs`` is a well-formed, subject-bounded input set.

    Returns the inputs as an ordered tuple on success.  Raises
    :class:`EditorialQCError` (typed by ``reason``) otherwise.  Checks:

    1. ``subject`` parses to a known :class:`Subject`.
    2. Every input has ``subject == subject``.
    3. ``event_id`` is non-empty on every input.
    4. ``(event_id, event_version)`` pairs are unique across the inputs.
    5. Every source URL is a valid ``http://`` or ``https://`` URL.
    6. ``policy.subject`` (when supplied) matches ``subject``.
    """
    try:
        primary_subject = subject if isinstance(subject, Subject) else Subject(subject)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"unknown subject: {subject!r}") from exc
    materialized = _reject_all_subject_marker(primary_subject, inputs)
    normalized = _normalized_input_list(primary_subject, materialized)
    seen_identities: dict[tuple[str, int], SubjectEditorialInput] = {}
    for index, item in enumerate(normalized):
        if item.subject is not primary_subject:
            raise EditorialQCError(
                EditorialQCReason.SUBJECT_MISMATCH,
                f"input {index} subject {item.subject.value!r} does not match "
                f"requested subject {primary_subject.value!r}",
                subject=primary_subject,
                identity=(item.event_id, item.event_id, item.event_version),
            )
        # Re-validate event_id explicitly so we surface the typed reason.
        if not item.event_id:
            raise EditorialQCError(
                EditorialQCReason.MISSING_EVENT_IDENTITY,
                f"input {index} has empty event_id",
                subject=primary_subject,
                identity=("", "", item.event_version),
            )
        key = (item.event_id, item.event_version)
        if key in seen_identities:
            raise EditorialQCError(
                EditorialQCReason.DUPLICATE_EVENT_IDENTITY,
                f"duplicate event identity {key!r} at input {index}",
                subject=primary_subject,
                identity=(item.event_id, item.event_id, item.event_version),
            )
        seen_identities[key] = item
        # All-subject input is forbidden — that is a report-scope bug,
        # not an editorial decision.  We reject it explicitly here so
        # callers cannot accidentally pass an "all subjects" input set.
        if item.subject is Subject.AI and primary_subject is not Subject.AI:
            # Subject.AI is the canonical subject; this check is a no-op
            # when the subject matches.  The "all subject" sentinel is
            # encoded as the string "all" — see validate_subject_inputs
            # below.
            pass
        # policy.subject, when present and a Subject, must match.
        policy_subject = getattr(item.policy, "subject", None)
        if policy_subject is not None and isinstance(policy_subject, Subject):
            if policy_subject is not primary_subject:
                raise EditorialQCError(
                    EditorialQCReason.SUBJECT_MISMATCH,
                    f"input {index} policy.subject {policy_subject.value!r} "
                    f"does not match subject {primary_subject.value!r}",
                    subject=primary_subject,
                    identity=(item.event_id, item.event_id, item.event_version),
                )
    return normalized


def _reject_all_subject_marker(
    subject: Subject | str,
    inputs: Iterable[SubjectEditorialInput],
) -> tuple[SubjectEditorialInput, ...]:
    """Reject a Subject-encoded "all subjects" input set.

    Some upstream callers used to ship a Subject.AI-tagged "all subjects"
    pass to the briefing engine.  The editorial QC boundary explicitly
    forbids that pattern by inspecting the marker attribute
    ``all_subjects`` on the input.  If any input sets
    ``all_subjects=True`` (or, equivalently, the input's title is the
    sentinel ``"<ALL SUBJECTS>"``) the call is rejected with
    :class:`EditorialQCReason.ALL_SUBJECT_INPUT`.
    """
    materialized = tuple(inputs)
    for index, item in enumerate(materialized):
        all_marker = getattr(item, "all_subjects", False)
        sentinel = item.title.strip() == "<ALL SUBJECTS>"
        if all_marker or sentinel:
            try:
                normalized_subject = subject if isinstance(subject, Subject) else Subject(subject)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"unknown subject: {subject!r}") from exc
            raise EditorialQCError(
                EditorialQCReason.ALL_SUBJECT_INPUT,
                f"input {index} carries the all-subjects marker; "
                "per-subject QC requires subject-bounded inputs",
                subject=normalized_subject,
                identity=(item.event_id, item.event_id, item.event_version),
            )
    return materialized


def validate_subject_outputs(
    subject: Subject | str,
    inputs: Iterable[SubjectEditorialInput],
    outputs: Iterable[SubjectEditorialOutput],
) -> tuple[SubjectEditorialOutput, ...]:
    """Validate that ``outputs`` mirrors ``inputs`` in order and content.

    Checks, in order:

    1. The subject parses to a known :class:`Subject`.
    2. Inputs are valid (delegates to :func:`validate_subject_inputs`).
    3. Outputs are not empty (zero-story is reported as a separate
       explicit no-delivery signal via :func:`validate_empty_subject_report`).
    4. ``len(inputs) == len(outputs)`` and they line up positionally.
    5. Each output's ``(subject, event_id, event_version)`` equals the
       corresponding input.
    6. Each output's ``source_url`` byte-matches one of the input's
       ``source_urls``.
    7. Each output's ``fact_deltas`` exactly equals the input's
       ``fact_deltas`` (tuple equality).
    8. ``what_changed`` and ``why_it_matters`` are 1..256 chars after
       control-byte scrubbing and contain no filler/marketing
       preambles; URLs in either narrative must be a subset of the
       input's ``source_urls``.
    9. No duplicate ``(event_id, event_version)`` identity across
       outputs (already enforced by the input side, but re-checked so
       a caller cannot sneak in duplicates).
    """
    try:
        normalized_subject = subject if isinstance(subject, Subject) else Subject(subject)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"unknown subject: {subject!r}") from exc

    # Reject the all-subjects sentinel before doing real work.
    inputs_tuple = _reject_all_subject_marker(normalized_subject, inputs)
    normalized_inputs = validate_subject_inputs(normalized_subject, inputs_tuple)
    normalized_outputs = tuple(outputs)
    if not normalized_outputs:
        raise EditorialQCError(
            EditorialQCReason.EMPTY_SUBJECT_REPORT,
            f"empty editorial outputs for subject {normalized_subject.value!r}",
            subject=normalized_subject,
        )
    if len(normalized_inputs) != len(normalized_outputs):
        raise EditorialQCError(
            EditorialQCReason.FACT_DELTA_MISMATCH,
            f"output count {len(normalized_outputs)} does not match "
            f"input count {len(normalized_inputs)} for subject "
            f"{normalized_subject.value!r}",
            subject=normalized_subject,
        )

    seen_identities: set[tuple[str, int]] = set()
    for index, (item_in, item_out) in enumerate(zip(normalized_inputs, normalized_outputs)):
        if not isinstance(item_out, SubjectEditorialOutput):
            raise ValueError("outputs must be SubjectEditorialOutput instances")
        # Duplicate identity across outputs fires before per-pair
        # identity-vs-input drift, because the drift is a *consequence*
        # of the duplicate, not a separate defect.
        identity_key = (item_out.event_id, item_out.event_version)
        if identity_key in seen_identities:
            raise EditorialQCError(
                EditorialQCReason.DUPLICATE_EVENT_IDENTITY,
                f"duplicate event identity {identity_key!r} at output {index}",
                subject=normalized_subject,
                identity=(item_out.event_id, item_out.event_id, item_out.event_version),
            )
        if item_out.subject is not normalized_subject:
            raise EditorialQCError(
                EditorialQCReason.SUBJECT_MISMATCH,
                f"output {index} subject {item_out.subject.value!r} does not "
                f"match requested subject {normalized_subject.value!r}",
                subject=normalized_subject,
                identity=(item_out.event_id, item_out.event_id, item_out.event_version),
            )
        if item_out.event_id != item_in.event_id or item_out.event_version != item_in.event_version:
            raise EditorialQCError(
                EditorialQCReason.MISSING_EVENT_IDENTITY,
                f"output {index} identity "
                f"({item_out.event_id!r}, {item_out.event_version!r}) "
                f"does not match input identity "
                f"({item_in.event_id!r}, {item_in.event_version!r})",
                subject=normalized_subject,
                identity=(item_out.event_id, item_out.event_id, item_out.event_version),
            )
        seen_identities.add(identity_key)

        # source_url must byte-match one of the input's source_urls.
        if item_out.source_url not in item_in.source_urls:
            raise EditorialQCError(
                EditorialQCReason.HALLUCINATED_URL,
                f"output {index} source_url {item_out.source_url!r} "
                f"does not match any input source_url for event "
                f"{item_in.event_id!r} v{item_in.event_version}",
                subject=normalized_subject,
                identity=(item_out.event_id, item_out.event_id, item_out.event_version),
            )

        # fact_deltas must exactly match.
        if item_out.fact_deltas != item_in.fact_deltas:
            raise EditorialQCError(
                EditorialQCReason.FACT_DELTA_MISMATCH,
                f"output {index} fact_deltas do not match input for event "
                f"{item_in.event_id!r} v{item_in.event_version}",
                subject=normalized_subject,
                identity=(item_out.event_id, item_out.event_id, item_out.event_version),
            )

        # Narrative constraints.
        for field_name in ("what_changed", "why_it_matters"):
            raw = getattr(item_out, field_name)
            scrubbed = _scrub_narrative(raw)
            if scrubbed != raw:
                raise EditorialQCError(
                    EditorialQCReason.FACT_DELTA_MISMATCH,
                    f"output {index} {field_name} contains forbidden controls",
                    subject=normalized_subject,
                    identity=(item_out.event_id, item_out.event_id, item_out.event_version),
                )
            if len(scrubbed) < NARRATIVE_MIN_LEN or len(scrubbed) > NARRATIVE_MAX_LEN:
                raise EditorialQCError(
                    EditorialQCReason.FACT_DELTA_MISMATCH,
                    f"output {index} {field_name} length {len(scrubbed)} "
                    f"outside [{NARRATIVE_MIN_LEN},{NARRATIVE_MAX_LEN}]",
                    subject=normalized_subject,
                    identity=(item_out.event_id, item_out.event_id, item_out.event_version),
                )
            if _looks_like_filler_preamble(scrubbed):
                raise EditorialQCError(
                    EditorialQCReason.FILLER_PREAMBLE,
                    f"output {index} {field_name} begins with a filler/marketing preamble",
                    subject=normalized_subject,
                    identity=(item_out.event_id, item_out.event_id, item_out.event_version),
                )
            # Any URL in the narrative must already be in the input's
            # allowed source_urls.
            narrative_urls = _extract_narrative_urls(scrubbed)
            for narrative_url in narrative_urls:
                if narrative_url not in item_in.source_urls:
                    raise EditorialQCError(
                        EditorialQCReason.HALLUCINATED_URL,
                        f"output {index} {field_name} references URL "
                        f"{narrative_url!r} that is not in input source_urls",
                        subject=normalized_subject,
                        identity=(item_out.event_id, item_out.event_id, item_out.event_version),
                    )

    return normalized_outputs


def validate_empty_subject_report(
    subject: Subject | str,
    inputs: Iterable[SubjectEditorialInput],
    outputs: Iterable[SubjectEditorialOutput] = (),
) -> None:
    """Validate a deliberate zero-story no-delivery report.

    A subject report is allowed to ship zero stories only when:

    * the inputs are valid (no subject mismatch, no duplicate identity,
      no all-subjects marker),
    * the inputs contain at least one event with an empty fact tuple
      (the canonical zero-story signal), and
    * the outputs are empty.

    Anything else — inputs with non-empty fact_deltas but empty outputs,
    or invalid inputs — is rejected with the appropriate typed reason.
    """
    try:
        normalized_subject = subject if isinstance(subject, Subject) else Subject(subject)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"unknown subject: {subject!r}") from exc

    inputs_tuple = _reject_all_subject_marker(normalized_subject, inputs)
    if not inputs_tuple:
        normalized_inputs = ()
    else:
        normalized_inputs = validate_subject_inputs(normalized_subject, inputs_tuple)
    normalized_outputs = tuple(outputs)

    if normalized_outputs:
        raise EditorialQCError(
            EditorialQCReason.FACT_DELTA_MISMATCH,
            f"empty-subject report for {normalized_subject.value!r} must have "
            f"no outputs, got {len(normalized_outputs)}",
            subject=normalized_subject,
        )

    # The zero-story signal is "every input has an empty fact_deltas
    # tuple".  This is the only approved shape for a no-delivery report.
    if not all(len(item.fact_deltas) == 0 for item in normalized_inputs):
        raise EditorialQCError(
            EditorialQCReason.EMPTY_SUBJECT_REPORT,
            f"empty-subject report for {normalized_subject.value!r} requires "
            "every input to have an empty fact_deltas tuple",
            subject=normalized_subject,
        )


# ---------------------------------------------------------------------------
# Summary rendering
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SubjectSummary:
    """The rendered per-subject brief.  ``text`` is the combined summary
    (what_changed + why_it_matters for every output, joined) and is
    guaranteed to be ``<= SUMMARY_MAX_LEN`` characters.
    """

    subject: Subject
    text: str
    sources: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not isinstance(self.subject, Subject):
            raise ValueError("SubjectSummary.subject must be a Subject")
        if not isinstance(self.text, str):
            raise ValueError("SubjectSummary.text must be a string")
        if len(self.text) > SUMMARY_MAX_LEN:
            raise ValueError(
                f"SubjectSummary.text length {len(self.text)} exceeds "
                f"SUMMARY_MAX_LEN ({SUMMARY_MAX_LEN})"
            )
        if type(self.sources) is not tuple:
            raise ValueError("SubjectSummary.sources must be a tuple")


def render_summary(outputs: Iterable[SubjectEditorialOutput]) -> SubjectSummary:
    """Render a combined per-subject summary from a validated output list.

    The caller is responsible for passing outputs that have already
    cleared :func:`validate_subject_outputs` (or
    :func:`validate_empty_subject_report`).  Re-renders an empty outputs
    sequence into a single line that begins with the
    ``"[no-delivery]"`` marker so downstream renderers can detect the
    zero-story case without inspecting the inputs.

    Combined length is hard-capped at :data:`SUMMARY_MAX_LEN`; any line
    that would push the total over the cap is truncated with a trailing
    ellipsis so the renderer never exceeds the report contract.
    """
    materialized = tuple(outputs)
    if not materialized:
        # No-delivery explicit signal: 16-byte ASCII prefix + subject.
        subject = materialized[0].subject if materialized else Subject.AI
        return SubjectSummary(
            subject=subject,
            text="[no-delivery]",
            sources=(),
        )
    primary_subject = materialized[0].subject
    lines: list[str] = []
    sources: list[str] = []
    seen_sources: set[str] = set()
    for item in materialized:
        if item.subject is not primary_subject:
            raise EditorialQCError(
                EditorialQCReason.SUBJECT_MISMATCH,
                f"render_summary: output for event {item.event_id!r} has "
                f"subject {item.subject.value!r}, expected "
                f"{primary_subject.value!r}",
                subject=primary_subject,
                identity=(item.event_id, item.event_id, item.event_version),
            )
        header = f"[{item.event_id} v{item.event_version}]"
        lines.append(f"{header} {item.what_changed}")
        lines.append(f"{header} {item.why_it_matters}")
        if item.source_url not in seen_sources:
            sources.append(item.source_url)
            seen_sources.add(item.source_url)

    joined = "\n".join(lines)
    if len(joined) > SUMMARY_MAX_LEN:
        joined = joined[: SUMMARY_MAX_LEN - 1].rstrip() + "\u2026"

    return SubjectSummary(
        subject=primary_subject,
        text=joined,
        sources=tuple(sources),
    )


__all__ = [
    "EditorialQCReason",
    "EditorialQCError",
    "SubjectEditorialInput",
    "SubjectEditorialOutput",
    "SubjectSummary",
    "NARRATIVE_MIN_LEN",
    "NARRATIVE_MAX_LEN",
    "SUMMARY_MAX_LEN",
    "subject_policy",
    "validate_subject_inputs",
    "validate_subject_outputs",
    "validate_empty_subject_report",
    "render_summary",
]
