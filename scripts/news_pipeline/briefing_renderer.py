"""Phase 4 — Slice 4 deterministic renderer.

Owns the strict briefing render value objects and the chunked plaintext
renderer required by the Phase 4 supervisor addendum §4. The module is
stdlib-only and reuses ``Category``, ``SemanticDecision``,
``SemanticReasonCode``, and ``FactDelta`` from existing ``news_pipeline``
modules.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import re
from zoneinfo import ZoneInfo

from .event_contracts import FactDelta, SemanticDecision, SemanticReasonCode
from .models import Category, Subject, subject_for_category


_LOCAL_TZ = ZoneInfo("Europe/London")
_ASCII_WHITESPACE_RE = re.compile(r"[ \t\r\n\v\f]+")

# C0, C1, DEL, and bidi controls are removed from normalized title/summary
# text and rejected in URL values.
_FORBIDDEN_CODEPOINTS = frozenset(
    {
        *range(0x00, 0x20),
        0x7F,
        *range(0x80, 0xA0),
        0x061C,
        0x200E,
        0x200F,
        *range(0x202A, 0x202F),
        *range(0x2066, 0x2070),
    }
)

_PARAGRAPH_SEPARATOR = "\n\n"
_MAX_CHUNK_UNITS = 4000

_CATEGORY_LABELS = {
    Category.AI: "AI",
    Category.WORLD: "World",
    Category.AUDIO_ENGINEERING: "Audio Engineering",
    Category.HARDWARE: "Hardware",
    Category.FANTASY_NOVEL: "Fantasy Novel",
    Category.AUDIOVISUAL: "Audiovisual",
    Category.AV_CORPORATE: "AV Corporate",
    Category.OUR_SETUP: "Our Setup",
}
_CATEGORY_ORDER = {category: index for index, category in enumerate(Category)}
_SUBJECT_LABELS = {
    Subject.AI: "AI",
    Subject.WORLD: "World",
    Subject.AUDIO_ENGINEERING: "Audio Engineering",
    Subject.PROFESSIONAL_AV: "Professional AV",
    Subject.HARDWARE: "Hardware",
    Subject.FANTASY_NOVEL: "Fantasy Novel",
    Subject.OUR_SETUP: "Our Setup",
}


class RenderStatus(str, Enum):
    EMPTY = "EMPTY"
    NO_DELIVERY = "NO_DELIVERY"
    RENDERED = "RENDERED"


class OversizeRecordError(ValueError):
    def __init__(self, candidate_id: str) -> None:
        self.candidate_id = candidate_id
        super().__init__(
            f"record for candidate_id {candidate_id!r} exceeds {_MAX_CHUNK_UNITS} UTF-16 units"
        )


@dataclass(frozen=True, slots=True)
class RenderRecord:
    candidate_id: str
    category: Category
    decision: SemanticDecision
    title: str
    summary: str
    url: str | None
    semantic_reasons: tuple[SemanticReasonCode, ...]
    fact_deltas: tuple[FactDelta, ...]
    ordinal: int
    subject_id: str | None = None
    event_id: str | None = None
    event_version: int | None = None

    def __post_init__(self) -> None:
        if type(self.candidate_id) is not str:
            raise TypeError(
                f"RenderRecord.candidate_id must be str, got {type(self.candidate_id).__name__}"
            )
        if not self.candidate_id:
            raise ValueError("RenderRecord.candidate_id must be non-empty")
        if len(self.candidate_id) > 256:
            raise ValueError("RenderRecord.candidate_id must be at most 256 code points")
        if not isinstance(self.category, Category):
            raise TypeError(
                f"RenderRecord.category must be Category, got {type(self.category).__name__}"
            )
        if self.subject_id is not None:
            if type(self.subject_id) is not str:
                raise TypeError("RenderRecord.subject_id must be str or None")
            try:
                subject = Subject(self.subject_id)
            except ValueError as exc:
                raise ValueError("RenderRecord.subject_id must be a known Subject") from exc
            if subject_for_category(self.category) is not subject:
                raise ValueError("RenderRecord.subject_id does not match category")
        if self.event_id is not None and type(self.event_id) is not str:
            raise TypeError("RenderRecord.event_id must be str or None")
        if self.event_id is not None and not self.event_id:
            raise ValueError("RenderRecord.event_id must be non-empty when supplied")
        if self.event_version is not None and (
            type(self.event_version) is not int or self.event_version <= 0
        ):
            raise ValueError("RenderRecord.event_version must be a positive int or None")
        if (self.event_id is None) != (self.event_version is None):
            raise ValueError("RenderRecord.event_id and event_version must be supplied together")
        if not isinstance(self.decision, SemanticDecision):
            raise TypeError(
                f"RenderRecord.decision must be SemanticDecision, got {type(self.decision).__name__}"
            )
        if self.decision not in (
            SemanticDecision.distinct_event,
            SemanticDecision.material_update,
        ):
            raise ValueError(
                "RenderRecord.decision must be distinct_event or material_update"
            )
        if type(self.title) is not str:
            raise TypeError(
                f"RenderRecord.title must be str, got {type(self.title).__name__}"
            )
        if type(self.summary) is not str:
            raise TypeError(
                f"RenderRecord.summary must be str, got {type(self.summary).__name__}"
            )
        if self.url is not None and type(self.url) is not str:
            raise TypeError(
                f"RenderRecord.url must be str or None, got {type(self.url).__name__}"
            )
        if type(self.semantic_reasons) is not tuple:
            raise TypeError("RenderRecord.semantic_reasons must be tuple")
        for reason in self.semantic_reasons:
            if not isinstance(reason, SemanticReasonCode):
                raise TypeError(
                    f"RenderRecord.semantic_reasons entries must be SemanticReasonCode, got {reason!r}"
                )
        if type(self.fact_deltas) is not tuple:
            raise TypeError("RenderRecord.fact_deltas must be tuple")
        for delta in self.fact_deltas:
            if not isinstance(delta, FactDelta):
                raise TypeError(
                    f"RenderRecord.fact_deltas entries must be FactDelta, got {delta!r}"
                )
        if type(self.ordinal) is not int or isinstance(self.ordinal, bool):
            raise TypeError(
                f"RenderRecord.ordinal must be int, got {type(self.ordinal).__name__}"
            )
        if self.ordinal < 0:
            raise ValueError("RenderRecord.ordinal must be non-negative")

        normalized_title = _normalize_display_text(self.title)
        normalized_summary = _normalize_display_text(self.summary)
        if not normalized_title:
            raise ValueError("RenderRecord.title normalizes to an empty string")
        if not normalized_summary:
            raise ValueError("RenderRecord.summary normalizes to an empty string")
        object.__setattr__(self, "title", normalized_title)
        object.__setattr__(self, "summary", normalized_summary)

        if self.url is not None:
            object.__setattr__(self, "url", _validate_url(self.url))


@dataclass(frozen=True, slots=True)
class RenderResult:
    status: RenderStatus
    chunks: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.status, RenderStatus):
            raise TypeError(
                f"RenderResult.status must be RenderStatus, got {type(self.status).__name__}"
            )
        if type(self.chunks) is not tuple:
            raise TypeError(f"RenderResult.chunks must be tuple, got {type(self.chunks).__name__}")
        for chunk in self.chunks:
            if type(chunk) is not str:
                raise TypeError(
                    f"RenderResult.chunks entries must be str, got {type(chunk).__name__}"
                )
        if self.status in (RenderStatus.EMPTY, RenderStatus.NO_DELIVERY):
            if self.chunks:
                raise ValueError("empty render statuses require an empty chunks tuple")
        elif not self.chunks:
            raise ValueError("RenderResult.RENDERED requires at least one chunk")


def utf16_units(text: str) -> int:
    if type(text) is not str:
        raise TypeError(f"utf16_units expects str, got {type(text).__name__}")
    return len(text.encode("utf-16-le")) // 2


def _is_forbidden_codepoint(ch: str) -> bool:
    return ord(ch) in _FORBIDDEN_CODEPOINTS


def _normalize_display_text(value: str) -> str:
    replaced = value.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    filtered_chars = []
    for ch in replaced:
        if _is_forbidden_codepoint(ch):
            continue
        filtered_chars.append(ch)
    collapsed = _ASCII_WHITESPACE_RE.sub(" ", "".join(filtered_chars))
    return collapsed.strip()


def _validate_upper_bound_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(
            f"upper_bound_utc must be datetime, got {type(value).__name__}"
        )
    if value.tzinfo is None:
        raise ValueError("upper_bound_utc must be timezone-aware")
    if value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError("upper_bound_utc must be UTC")
    return value.astimezone(timezone.utc)


def _validate_url(value: str) -> str:
    if type(value) is not str:
        raise TypeError(f"url must be str or None, got {type(value).__name__}")
    if not value:
        raise ValueError("url must be non-empty")
    if len(value) > 2048:
        raise ValueError("url must be at most 2048 characters")
    if any(ch.isspace() for ch in value):
        raise ValueError("url contains whitespace")
    if any(_is_forbidden_codepoint(ch) for ch in value):
        raise ValueError("url contains forbidden control characters")
    separator = "://"
    if separator not in value:
        raise ValueError("url must contain an http or https scheme")
    scheme, rest = value.split(separator, 1)
    if scheme not in ("http", "https"):
        raise ValueError("url scheme must be exactly lowercase http or https")
    delimiter_positions = [
        pos for term in ("/", "?", "#") if (pos := rest.find(term)) >= 0
    ]
    authority = rest if not delimiter_positions else rest[: min(delimiter_positions)]
    if not authority:
        raise ValueError("url authority must be non-empty")
    return value


def _render_fact_delta(delta: FactDelta) -> str:
    return ", ".join(
        (
            f"kind={delta.kind.value}",
            f"unit={_normalize_display_text(delta.unit)}",
            f"old_value={_normalize_display_text(delta.old_value)}",
            f"new_value={_normalize_display_text(delta.new_value)}",
            f"topic_gate={delta.topic_gate}",
        )
    )


def _render_record_paragraph(record: RenderRecord) -> str:
    prefix = "[NEW]" if record.decision is SemanticDecision.distinct_event else "[UPDATE]"
    url_text = record.url if record.url is not None else "(no URL)"
    reasons_text = ", ".join(reason.value for reason in record.semantic_reasons)
    if not reasons_text:
        reasons_text = "none"
    facts_text = ", ".join(_render_fact_delta(delta) for delta in record.fact_deltas)
    if not facts_text:
        facts_text = "none"
    return "\n".join(
        (
            f"{prefix} {record.title}",
            record.summary,
            url_text,
            f"Reasons: {reasons_text}",
            f"Facts: {facts_text}",
        )
    )


def _sort_key(record: RenderRecord) -> tuple[int, int, str]:
    return _CATEGORY_ORDER[record.category], record.ordinal, record.candidate_id


def render_briefing(
    records: Sequence[RenderRecord],
    upper_bound_utc: datetime,
    parse_mode=None,
    subject_id: str | None = None,
) -> RenderResult:
    del parse_mode
    if not isinstance(records, (tuple, list)):
        raise TypeError(f"records must be tuple or list, got {type(records).__name__}")
    if not records:
        if subject_id is not None:
            try:
                Subject(subject_id)
            except ValueError as exc:
                raise ValueError("subject_id must be a known Subject") from exc
            return RenderResult(status=RenderStatus.NO_DELIVERY, chunks=())
        return RenderResult(status=RenderStatus.EMPTY, chunks=())

    normalized_records = []
    for index, record in enumerate(records):
        if not isinstance(record, RenderRecord):
            raise TypeError(
                f"records[{index}] must be RenderRecord, got {type(record).__name__}"
            )
        normalized_records.append(record)

    identities = set()
    for record in normalized_records:
        if subject_id is not None and record.subject_id != subject_id:
            raise ValueError("render batch contains a cross-subject record")
        identity = (
            record.subject_id,
            record.event_id,
            record.event_version,
            record.candidate_id,
        )
        if identity in identities:
            raise ValueError("render batch contains a duplicate event identity")
        identities.add(identity)

    _validate_upper_bound_utc(upper_bound_utc)
    local_date = _validate_upper_bound_utc(upper_bound_utc).astimezone(_LOCAL_TZ).date()
    header = f"Morning briefing — {local_date:%Y-%m-%d}"
    subject_label = None
    if subject_id is not None:
        subject_label = _SUBJECT_LABELS[Subject(subject_id)]

    ordered_records = sorted(normalized_records, key=_sort_key)
    chunks: list[str] = []
    current_text: str | None = None
    current_category: Category | None = None

    for record in ordered_records:
        if record.category not in _CATEGORY_LABELS:
            raise ValueError(f"unhandled category: {record.category!r}")
        label = _CATEGORY_LABELS[record.category]
        record_paragraph = _render_record_paragraph(record)
        if current_text is None:
            first_parts = (header,) if subject_label is None else (header, subject_label)
            candidate_text = _PARAGRAPH_SEPARATOR.join((*first_parts, label, record_paragraph))
        elif current_category is record.category:
            candidate_text = current_text + _PARAGRAPH_SEPARATOR + record_paragraph
        else:
            candidate_text = current_text + _PARAGRAPH_SEPARATOR + label + _PARAGRAPH_SEPARATOR + record_paragraph

        if utf16_units(candidate_text) <= _MAX_CHUNK_UNITS:
            current_text = candidate_text
            current_category = record.category
            continue

        if current_text is not None:
            chunks.append(current_text)
        first_parts = (header,) if subject_label is None else (header, subject_label)
        candidate_text = _PARAGRAPH_SEPARATOR.join((*first_parts, label, record_paragraph))
        if utf16_units(candidate_text) > _MAX_CHUNK_UNITS:
            raise OversizeRecordError(record.candidate_id)
        current_text = candidate_text
        current_category = record.category

    if current_text is not None:
        chunks.append(current_text)

    return RenderResult(status=RenderStatus.RENDERED, chunks=tuple(chunks))


render_records = render_briefing


__all__ = (
    "OversizeRecordError",
    "RenderRecord",
    "RenderResult",
    "RenderStatus",
    "render_briefing",
    "render_records",
    "utf16_units",
)
