"""Phase 4 — Slice 3 deterministic summarizer engine.

The summarizer is the third Phase 4 module. Per the supervisor addendum
§2.3 and §2.5 item 6, it owns:

  * strict ``SummarizerInput`` (raw exact fields, type-only) and
    ``SummarizerRequestItem`` (bounds-checked) value types;
  * ``canonical_request_bytes`` / ``request_cache_key`` producing the
    exact, normalized request envelope and its full-byte SHA-256 cache key;
  * ``parse_summary_response`` enforcing the §2.3 response schema,
    converting its own internals to a typed malformed error;
  * ``SummarizerSession(transport, cache).summarize_category`` enforcing
    one-uncached-call-per-category, an eight-call total budget, INPUT_BOUNDS
    / TRANSPORT_ERROR / MALFORMED_OUTPUT / BUDGET_EXHAUSTED fallbacks,
    while preserving every raw input ID;

Stdlib + existing ``news_pipeline`` modules only. No time / network /
random / uuid / open / Path / subprocess / socket APIs.

Cache, error, and budget semantics follow §2.3 and §2.5 item 6 exactly;
slicing or dropping an ID is never permitted and any programmer exception
around the model transport or outside the parser's declared failure modes
propagates untouched.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from collections.abc import MutableMapping
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .event_contracts import FactDelta, FactKind, event_version_identity
from .editorial_qc import (
    EditorialQCError,
    SubjectEditorialInput,
    SubjectEditorialOutput,
    render_summary,
    subject_policy,
    validate_subject_inputs,
    validate_subject_outputs,
)
from .models import Category, Subject


# ---------------------------------------------------------------------------
# Public value-object exceptions and enums
# ---------------------------------------------------------------------------


class SummarizerError(Exception):
    """Base class for every Phase 4 summarizer-side exception."""


class SummarizerTransportError(SummarizerError):
    """Injected transport raised a transport-level failure.

    The session catches this single error class around the transport
    invocation and turns it into a per-item ``FALLBACK`` carrying
    ``SummarizerErrorCategory.TRANSPORT_ERROR``. Any other exception type
    around the transport propagates untouched.
    """


class SummarizerMalformedError(SummarizerError):
    """The parser converted its own internals to a typed malformed error.

    Triggered by ``UnicodeDecodeError``, ``json.JSONDecodeError``,
    duplicate-key detection, non-finite sentinel detection, response-size
    overflow, fence / trailing content detection, or schema violation. The
    session maps it to ``SummaryItem(error_category=MALFORMED_OUTPUT)``.
    Any other exception type out of the parser propagates untouched.
    """


class SummarizerErrorCategory(str, Enum):
    """Typed reason for a per-item ``SummarySource.FALLBACK`` row.

    Only the four enum values listed in §2.3 / §2.5 item 6 are accepted on
    a fallback summary; every other potential reason must surface as its
    own typed exception, not as a fallback.
    """

    INPUT_BOUNDS = "input_bounds"
    BUDGET_EXHAUSTED = "budget_exhausted"
    TRANSPORT_ERROR = "transport_error"
    MALFORMED_OUTPUT = "malformed_output"


class SummarySource(str, Enum):
    """Where the per-item summary came from.

    ``MODEL`` — a real invocation produced the summary.
    ``CACHE`` — a prior successful invocation was reused.
    ``FALLBACK`` — never invokes the model on this item directly; the
    reason lives in ``SummaryItem.error_category``.
    """

    MODEL = "model"
    CACHE = "cache"
    FALLBACK = "fallback"


# ---------------------------------------------------------------------------
# Forbidden control characters (C0 / C1 / bidi)
# ---------------------------------------------------------------------------


_C0_MAX = 0x1F
_C1_MIN = 0x80
_C1_MAX = 0x9F
_BIDI_CODE_POINTS = frozenset(
    (0x061C, 0x200E, 0x200F, 0x202A, 0x202B, 0x202C, 0x202D, 0x202E,
     0x2066, 0x2067, 0x2068, 0x2069, 0x206A, 0x206B, 0x206C, 0x206D,
     0x206E, 0x206F)
)


def _has_forbidden_controls(value: str) -> bool:
    """Return ``True`` if ``value`` contains any C0 / C1 / bidi code point."""
    if not value:
        return False
    for ch in value:
        cp = ord(ch)
        if cp <= _C0_MAX or cp == 0x7F:
            return True
        if _C1_MIN <= cp <= _C1_MAX:
            return True
        if cp in _BIDI_CODE_POINTS:
            return True
    return False


# ---------------------------------------------------------------------------
# Bounds constants — §2.3 / §2.5 item 6
# ---------------------------------------------------------------------------

MAX_REQUEST_BYTES = 65536
MAX_RESPONSE_BYTES = 32768
MAX_ITEMS_PER_CATEGORY = 32
TOTAL_CALL_BUDGET = 8
ONE_CALL_PER_CATEGORY = 1

CANDIDATE_ID_MIN = 1
CANDIDATE_ID_MAX = 256
TITLE_MIN = 1
TITLE_MAX = 512
SNIPPET_MIN = 0
SNIPPET_MAX = 2048
URL_MIN = 1
URL_MAX = 2048
SUMMARY_MIN = 1
SUMMARY_MAX = 512


# ---------------------------------------------------------------------------
# Strict value objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SummarizerInput:
    """Raw candidate fields supplied by the engine before bounds checks.

    Per §2.5 item 6, raw inputs are type-checked here and all candidate,
    title, snippet, and URL bounds are classified by the session before
    constructing ``SummarizerRequestItem``. Invalid raw values become
    INPUT_BOUNDS fallbacks without slicing or dropping the original ID.
    """

    candidate_id: str
    title: str
    snippet: str
    url: Optional[str]

    def __post_init__(self) -> None:
        if type(self.candidate_id) is not str:
            raise TypeError(
                f"SummarizerInput.candidate_id must be str, got {type(self.candidate_id).__name__}"
            )
        if type(self.title) is not str:
            raise TypeError(
                f"SummarizerInput.title must be str, got {type(self.title).__name__}"
            )
        if type(self.snippet) is not str:
            raise TypeError(
                f"SummarizerInput.snippet must be str, got {type(self.snippet).__name__}"
            )
        if self.url is not None and type(self.url) is not str:
            raise TypeError(
                f"SummarizerInput.url must be str or None, got {type(self.url).__name__}"
            )


_SCHEME_SEPARATOR = "://"
_URL_AUTHORITY_TERMINATORS = ("/", "?", "#")


def _validate_http_url(url: str) -> None:
    """Validate the §2.3 URL schema.

    Lowercase ``http`` or ``https`` scheme, 1..2048 code points, nonempty
    authority, no whitespace / C0 / C1 / bidi controls. Raised as
    ``ValueError`` so the constructor surfaces a single programmer
    contract violation to the caller.
    """
    if not (URL_MIN <= len(url) <= URL_MAX):
        raise ValueError(
            f"URL length must be {URL_MIN}..{URL_MAX} code points, got len={len(url)}"
        )
    if _SCHEME_SEPARATOR not in url:
        raise ValueError(
            f"URL must contain '{_SCHEME_SEPARATOR}' separator"
        )
    scheme, rest = url.split(_SCHEME_SEPARATOR, 1)
    if scheme not in ("http", "https"):
        raise ValueError("URL scheme must be exactly lowercase http or https")
    authority = rest
    delimiter_positions = [
        position for term in _URL_AUTHORITY_TERMINATORS
        if (position := rest.find(term)) >= 0
    ]
    if delimiter_positions:
        authority = rest[:min(delimiter_positions)]
    if not authority:
        raise ValueError("URL authority (netloc) must be nonempty")
    if _has_forbidden_controls(url):
        raise ValueError("URL contains forbidden control characters")
    # Explicit whitespace rejection — ASCII space / tab / CR / LF / VT / FF
    # are forbidden anywhere in the URL per §2.3.
    if any(ch.isspace() for ch in url):
        raise ValueError("URL contains whitespace characters")


@dataclass(frozen=True, slots=True)
class SummarizerRequestItem:
    """Strict bounds-checked item of a canonical request.

    Constructor validates: candidate ID 1..256 code points; title 1..512;
    snippet 0..2048; URL ``None`` or 1..2048 with exact lowercase
    ``http``/``https`` scheme, nonempty authority, and no C0/C1/bidi
    controls. Bounds failures raise ``ValueError``. The instance stores
    the exact value (no copy, no slice).
    """

    candidate_id: str
    title: str
    snippet: str
    url: Optional[str]

    def __post_init__(self) -> None:
        if type(self.candidate_id) is not str:
            raise TypeError(
                f"SummarizerRequestItem.candidate_id must be str, got {type(self.candidate_id).__name__}"
            )
        if not (CANDIDATE_ID_MIN <= len(self.candidate_id) <= CANDIDATE_ID_MAX):
            raise ValueError(
                f"SummarizerRequestItem.candidate_id length must be "
                f"{CANDIDATE_ID_MIN}..{CANDIDATE_ID_MAX} code points, got "
                f"len={len(self.candidate_id)}"
            )
        if type(self.title) is not str:
            raise TypeError(
                f"SummarizerRequestItem.title must be str, got {type(self.title).__name__}"
            )
        if not (TITLE_MIN <= len(self.title) <= TITLE_MAX):
            raise ValueError(
                f"SummarizerRequestItem.title length must be "
                f"{TITLE_MIN}..{TITLE_MAX} code points, got len={len(self.title)}"
            )
        if type(self.snippet) is not str:
            raise TypeError(
                f"SummarizerRequestItem.snippet must be str, got {type(self.snippet).__name__}"
            )
        if not (SNIPPET_MIN <= len(self.snippet) <= SNIPPET_MAX):
            raise ValueError(
                f"SummarizerRequestItem.snippet length must be "
                f"{SNIPPET_MIN}..{SNIPPET_MAX} code points, got len={len(self.snippet)}"
            )
        if self.url is not None:
            if type(self.url) is not str:
                raise TypeError(
                    f"SummarizerRequestItem.url must be str or None, got {type(self.url).__name__}"
                )
            _validate_http_url(self.url)


@dataclass(frozen=True, slots=True)
class SummaryItem:
    """One immutable per-item summary row.

    Common to MODEL / CACHE / FALLBACK sources. ``MODEL`` / ``CACHE``
    require ``error_category is None``; ``FALLBACK`` requires exactly one
    of the four ``SummarizerErrorCategory`` values listed in §2.3 / §2.5
    item 6. Summary text on a fallback is the exact original title when
    valid; the literal ``"Untitled item"`` is used when the title itself
    was invalid.
    """

    candidate_id: str
    summary: str
    source: SummarySource
    error_category: Optional[SummarizerErrorCategory]
    subject_id: Optional[str] = None
    event_id: Optional[str] = None
    event_version: Optional[int] = None
    source_url: Optional[str] = None
    what_changed: Optional[str] = None
    why_it_matters: Optional[str] = None

    def __post_init__(self) -> None:
        if type(self.candidate_id) is not str:
            raise TypeError(
                f"SummaryItem.candidate_id must be str, got {type(self.candidate_id).__name__}"
            )
        if type(self.summary) is not str:
            raise TypeError(
                f"SummaryItem.summary must be str, got {type(self.summary).__name__}"
            )
        if not (SUMMARY_MIN <= len(self.summary) <= SUMMARY_MAX):
            raise ValueError("SummaryItem.summary must contain 1..512 code points")
        if _has_forbidden_controls(self.summary):
            raise ValueError("SummaryItem.summary contains forbidden controls")
        if not isinstance(self.source, SummarySource):
            raise TypeError(f"SummaryItem.source must be SummarySource, got {self.source!r}")
        if self.error_category is not None and not isinstance(
            self.error_category, SummarizerErrorCategory
        ):
            raise TypeError("SummaryItem.error_category must be SummarizerErrorCategory or None")
        if self.subject_id is not None and type(self.subject_id) is not str:
            raise TypeError("SummaryItem.subject_id must be str or None")
        if self.event_id is not None and type(self.event_id) is not str:
            raise TypeError("SummaryItem.event_id must be str or None")
        if self.event_version is not None and (
            type(self.event_version) is not int or self.event_version <= 0
        ):
            raise ValueError("SummaryItem.event_version must be a positive int or None")
        if self.source_url is not None:
            if type(self.source_url) is not str:
                raise TypeError("SummaryItem.source_url must be str or None")
            _validate_http_url(self.source_url)
        for field_name in ("what_changed", "why_it_matters"):
            value = getattr(self, field_name)
            if value is not None and type(value) is not str:
                raise TypeError(f"SummaryItem.{field_name} must be str or None")
        if self.source is SummarySource.FALLBACK:
            if self.error_category is None:
                raise ValueError("FALLBACK requires an error_category")
        elif self.error_category is not None:
            raise ValueError("MODEL/CACHE forbid error_category")
        # Only INPUT_BOUNDS may preserve an invalid raw candidate ID verbatim.
        if not (
            self.source is SummarySource.FALLBACK
            and self.error_category is SummarizerErrorCategory.INPUT_BOUNDS
        ) and not (CANDIDATE_ID_MIN <= len(self.candidate_id) <= CANDIDATE_ID_MAX):
            raise ValueError("non-INPUT_BOUNDS SummaryItem candidate_id must be 1..256 code points")


@dataclass(frozen=True, slots=True)
class SummarizerCacheValue:
    """One persisted cache row.

    Holds the parsed summaries from a successful prior invocation plus
    the original model outcome flag, so a cache hit can return
    ``SummarySource.CACHE`` items without invoking the transport while
    still preserving the model-used truth for the engine.
    """

    items: Tuple[SummaryItem, ...]
    model_used: bool

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple):
            raise TypeError(
                f"SummarizerCacheValue.items must be tuple, got {type(self.items).__name__}"
            )
        for it in self.items:
            if not isinstance(it, SummaryItem):
                raise TypeError(
                    f"SummarizerCacheValue.items entry must be SummaryItem, got {it!r}"
                )
        if type(self.model_used) is not bool:
            raise TypeError(
                f"SummarizerCacheValue.model_used must be bool, got {type(self.model_used).__name__}"
            )
        if self.model_used is not True:
            raise ValueError("successful cache values require model_used=True")
        if not self.items:
            raise ValueError("successful cache values cannot be empty")
        if any(it.source is not SummarySource.MODEL or it.error_category is not None for it in self.items):
            raise ValueError("successful cache values require only MODEL/no-error items")
        ids = tuple(it.candidate_id for it in self.items)
        if len(set(ids)) != len(ids):
            raise ValueError("cache value candidate IDs must be unique")


@dataclass(frozen=True, slots=True)
class CategorySummaryResult:
    """One per-category result envelope.

    ``items`` preserves the original raw input order one-for-one. No raw
    input is sliced, dropped, or duplicated. ``model_used`` /
    ``cache_hit`` follow §2.3 bookkeeping rules:

      * a real invocation (model or transport failure) → ``model_used=True``
      * a cache hit → ``cache_hit=True, model_used=False``
      * INPUT_BOUNDS / BUDGET_EXHAUSTED fallbacks → both flags False
    """

    items: Tuple[SummaryItem, ...]
    model_used: bool
    cache_hit: bool

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple):
            raise TypeError(
                f"CategorySummaryResult.items must be tuple, got {type(self.items).__name__}"
            )
        for it in self.items:
            if not isinstance(it, SummaryItem):
                raise TypeError(
                    f"CategorySummaryResult.items entry must be SummaryItem, got {it!r}"
                )
        if type(self.model_used) is not bool or type(self.cache_hit) is not bool:
            raise TypeError(
                f"CategorySummaryResult.model_used/cache_hit must be bool, got "
                f"model_used={self.model_used!r} cache_hit={self.cache_hit!r}"
            )
        if self.model_used and self.cache_hit:
            raise ValueError("model_used and cache_hit are mutually exclusive")
        sources = tuple(it.source for it in self.items)
        if self.cache_hit:
            if SummarySource.CACHE not in sources or SummarySource.MODEL in sources:
                raise ValueError("cache_hit result requires CACHE and forbids MODEL items")
            if any(
                it.source is SummarySource.FALLBACK
                and it.error_category is not SummarizerErrorCategory.INPUT_BOUNDS
                for it in self.items
            ):
                raise ValueError("cache-hit fallbacks may only be INPUT_BOUNDS")
        elif SummarySource.CACHE in sources:
            raise ValueError("CACHE items require cache_hit=True")
        if not self.model_used and SummarySource.MODEL in sources:
            raise ValueError("MODEL items require model_used=True")
        if self.model_used:
            if not self.items:
                raise ValueError("model_used result cannot be empty")
            if any(
                it.source is SummarySource.FALLBACK
                and it.error_category is SummarizerErrorCategory.BUDGET_EXHAUSTED
                for it in self.items
            ):
                raise ValueError("model-used result cannot contain budget fallbacks")
            if not any(
                it.source is SummarySource.MODEL
                or (
                    it.source is SummarySource.FALLBACK
                    and it.error_category in (
                        SummarizerErrorCategory.TRANSPORT_ERROR,
                        SummarizerErrorCategory.MALFORMED_OUTPUT,
                    )
                )
                for it in self.items
            ):
                raise ValueError("model-used result requires a model or invocation-error item")
        elif not self.cache_hit and any(
            it.source is SummarySource.FALLBACK
            and it.error_category not in (
                SummarizerErrorCategory.INPUT_BOUNDS,
                SummarizerErrorCategory.BUDGET_EXHAUSTED,
            )
            for it in self.items
        ):
            raise ValueError("non-invocation result has an invocation-only error")


# ---------------------------------------------------------------------------
# Canonical request bytes + cache key
# ---------------------------------------------------------------------------


def _request_dict(
    category: Category, items: Sequence[SummarizerRequestItem]
) -> Dict[str, Any]:
    if not isinstance(category, Category):
        raise TypeError(
            f"category must be a Category enum, got {type(category).__name__}"
        )
    if not isinstance(items, (tuple, list)):
        raise TypeError(
            f"items must be a tuple / list, got {type(items).__name__}"
        )
    if len(items) > MAX_ITEMS_PER_CATEGORY:
        raise ValueError(f"canonical request allows at most {MAX_ITEMS_PER_CATEGORY} items")
    out_items = []
    seen_ids: Dict[str, None] = {}
    for item in items:
        if not isinstance(item, SummarizerRequestItem):
            raise TypeError(
                f"items entry must be SummarizerRequestItem, got "
                f"{type(item).__name__}"
            )
        if item.candidate_id in seen_ids:
            raise ValueError(
                f"duplicate candidate_id in canonical request items: "
                f"{item.candidate_id!r}"
            )
        seen_ids[item.candidate_id] = None
        out_items.append(
            {
                "candidate_id": item.candidate_id,
                "title": item.title,
                "snippet": item.snippet,
                "url": item.url,
            }
        )
    # Insertion order: ``category`` then ``items``; per item the exact order
    # is ``candidate_id``, ``title``, ``snippet``, ``url``.
    return {"category": category.value, "items": out_items}


def canonical_request_bytes(
    category: Category, items: Sequence[SummarizerRequestItem]
) -> bytes:
    """Encode the canonical §2.3 request envelope to bytes.

    Uses ``json.dumps(..., ensure_ascii=False, separators=(",", ":"),
    allow_nan=False).encode("utf-8")``. Max 65536 UTF-8 bytes; longer
    envelopes raise ``ValueError``. Insertion order is preserved so two
    semantically-equivalent requests produce byte-identical output and
    SHA-256 collisions only happen when items match exactly.
    """
    request_obj = _request_dict(category, items)
    text = json.dumps(
        request_obj,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )
    encoded = text.encode("utf-8")
    if len(encoded) > MAX_REQUEST_BYTES:
        raise ValueError(
            f"canonical request exceeds {MAX_REQUEST_BYTES} bytes, got "
            f"len={len(encoded)}"
        )
    return encoded


def request_cache_key(
    category: Category, items: Sequence[SummarizerRequestItem]
) -> bytes:
    """Return the SHA-256 cache key of the full canonical request bytes.

    The full envelope (including the category envelope) is hashed so the
    key changes when the category changes or item order / values change.
    """
    encoded = canonical_request_bytes(category, items)
    return hashlib.sha256(encoded).digest()


# ---------------------------------------------------------------------------
# Subject-scoped editorial request envelope
# ---------------------------------------------------------------------------


MAX_ITEMS_PER_SUBJECT = MAX_ITEMS_PER_CATEGORY
ONE_CALL_PER_SUBJECT = 1


def _fact_delta_dict(delta: FactDelta) -> Dict[str, str]:
    return {
        "kind": delta.kind.value,
        "unit": delta.unit,
        "old_value": delta.old_value,
        "new_value": delta.new_value,
        "topic_gate": str(delta.topic_gate),
    }


def _fact_delta_from_dict(raw: Any) -> FactDelta:
    if not isinstance(raw, dict) or set(raw) != {
        "kind", "unit", "old_value", "new_value", "topic_gate"
    }:
        raise SummarizerMalformedError("editorial fact_deltas item has an invalid shape")
    if any(type(raw[key]) is not str for key in raw):
        raise SummarizerMalformedError("editorial fact_deltas values must be strings")
    try:
        from decimal import Decimal

        return FactDelta(
            kind=FactKind(raw["kind"]),
            unit=raw["unit"],
            old_value=raw["old_value"],
            new_value=raw["new_value"],
            topic_gate=Decimal(raw["topic_gate"]),
        )
    except (TypeError, ValueError, ArithmeticError) as exc:
        raise SummarizerMalformedError("editorial fact_delta is invalid") from exc


def _subject_request_dict(
    subject: Subject, inputs: Sequence[SubjectEditorialInput]
) -> Dict[str, Any]:
    if not isinstance(subject, Subject):
        raise TypeError("subject must be a Subject enum")
    validated = validate_subject_inputs(subject, tuple(inputs))
    if len(validated) > MAX_ITEMS_PER_SUBJECT:
        raise ValueError("subject request allows at most 32 items")
    return {
        "subject": subject.value,
        "policy": {
            "allowed_categories": [
                category.value for category in subject_policy(subject)
            ],
            "verified_facts_only": True,
            "format": ("what_changed", "why_it_matters"),
            "source_url_rule": "byte_identical_to_input_source_urls",
        },
        "items": [
            {
                "event_id": item.event_id,
                "event_version": item.event_version,
                "title": item.title,
                "fact_deltas": [_fact_delta_dict(delta) for delta in item.fact_deltas],
                "source_urls": list(item.source_urls),
            }
            for item in validated
        ],
    }


def canonical_subject_request_bytes(
    subject: Subject, inputs: Sequence[SubjectEditorialInput]
) -> bytes:
    encoded = json.dumps(
        _subject_request_dict(subject, inputs),
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    if len(encoded) > MAX_REQUEST_BYTES:
        raise ValueError(
            f"canonical subject request exceeds {MAX_REQUEST_BYTES} bytes, got len={len(encoded)}"
        )
    return encoded


def subject_request_cache_key(
    subject: Subject, inputs: Sequence[SubjectEditorialInput]
) -> bytes:
    return hashlib.sha256(canonical_subject_request_bytes(subject, inputs)).digest()


def parse_subject_response(
    response_bytes: bytes | bytearray,
    *,
    subject: Subject,
    expected_inputs: Sequence[SubjectEditorialInput],
) -> Tuple[SubjectEditorialOutput, ...]:
    """Parse and QC one strict subject-scoped editorial response."""
    if not isinstance(response_bytes, (bytes, bytearray)):
        raise TypeError("response_bytes must be bytes")
    raw = bytes(response_bytes)
    if len(raw) > MAX_RESPONSE_BYTES:
        raise SummarizerMalformedError("subject response exceeds response byte limit")
    try:
        text = raw.decode("utf-8", errors="strict")
        parsed = json.loads(
            _strip_ascii_whitespace(text),
            parse_constant=lambda token: (_ for _ in ()).throw(
                SummarizerMalformedError(f"non-finite editorial constant: {token!r}")
            ),
            object_pairs_hook=_reject_duplicate_pairs,
        )
    except UnicodeDecodeError as exc:
        raise SummarizerMalformedError("subject response is not strict UTF-8") from exc
    except json.JSONDecodeError as exc:
        raise SummarizerMalformedError("subject response is not valid JSON") from exc
    if not isinstance(parsed, dict) or set(parsed) != {"items"}:
        raise SummarizerMalformedError("subject response root must contain only items")
    raw_items = parsed["items"]
    if not isinstance(raw_items, list) or len(raw_items) > MAX_ITEMS_PER_SUBJECT:
        raise SummarizerMalformedError("subject response items must be a bounded list")
    allowed_keys = frozenset(
        {
            "subject", "event_id", "event_version", "what_changed",
            "why_it_matters", "source_url", "fact_deltas",
        }
    )
    outputs: list[SubjectEditorialOutput] = []
    for raw_item in raw_items:
        if not isinstance(raw_item, dict) or frozenset(raw_item) != allowed_keys:
            raise SummarizerMalformedError("subject response item keys are invalid")
        try:
            output = SubjectEditorialOutput(
                subject=Subject(raw_item["subject"]),
                event_id=raw_item["event_id"],
                event_version=raw_item["event_version"],
                what_changed=raw_item["what_changed"],
                why_it_matters=raw_item["why_it_matters"],
                source_url=raw_item["source_url"],
                fact_deltas=tuple(_fact_delta_from_dict(item) for item in raw_item["fact_deltas"]),
            )
        except EditorialQCError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise SummarizerMalformedError("subject response item is invalid") from exc
        outputs.append(output)
    try:
        return validate_subject_outputs(subject, tuple(expected_inputs), tuple(outputs))
    except EditorialQCError:
        raise


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


_TRIM_CHARS = frozenset(" \t\r\n")


def _strip_ascii_whitespace(value: str) -> str:
    """Trim only ASCII space / tab / CR / LF from both ends."""
    start = 0
    end = len(value)
    while start < end and value[start] in _TRIM_CHARS:
        start += 1
    while end > start and value[end - 1] in _TRIM_CHARS:
        end -= 1
    return value[start:end]


def _reject_duplicate_pairs(pairs):
    """``object_pairs_hook`` that rejects duplicate keys in any dict level."""
    out: Dict[str, Any] = {}
    for key, val in pairs:
        if key in out:
            raise SummarizerMalformedError(
                f"duplicate JSON key in object: {key!r}"
            )
        out[key] = val
    return out


def _valid_item_keys() -> frozenset:
    return frozenset(("candidate_id", "summary"))


def _validate_response_item(
    raw_item: Any, expected_id: Optional[str] = None,
    *, position: int,
) -> Tuple[str, str]:
    if not isinstance(raw_item, dict):
        raise SummarizerMalformedError(
            f"response item at position {position} must be object"
        )
    keys = frozenset(raw_item.keys())
    allowed = _valid_item_keys()
    if keys != allowed:
        raise SummarizerMalformedError(
            f"response item at position {position} keys must be exactly candidate_id,summary"
        )
    candidate_id = raw_item["candidate_id"]
    if type(candidate_id) is not str:
        raise SummarizerMalformedError(
            f"response item at position {position}.candidate_id must be str"
        )
    if not (CANDIDATE_ID_MIN <= len(candidate_id) <= CANDIDATE_ID_MAX):
        raise SummarizerMalformedError(
            f"response item at position {position} candidate_id length invalid"
        )
    if expected_id is not None and candidate_id != expected_id:
        raise SummarizerMalformedError(
            f"response item at position {position} candidate_id mismatch"
        )
    summary = raw_item["summary"]
    if type(summary) is not str:
        raise SummarizerMalformedError(
            f"response item at position {position}.summary must be str"
        )
    if not (SUMMARY_MIN <= len(summary) <= SUMMARY_MAX):
        raise SummarizerMalformedError(
            f"response item at position {position} summary length invalid"
        )
    if _has_forbidden_controls(summary):
        raise SummarizerMalformedError(
            f"response item at position {position} summary contains forbidden controls"
        )
    return candidate_id, summary


def parse_summary_response(
    response_bytes: bytes,
    expected_items: Optional[Sequence[SummarizerRequestItem]] = None,
) -> List[Tuple[str, str]]:
    """Parse the §2.3 response into a list of ``(candidate_id, summary)``.

    Validation steps (any failure raises ``SummarizerMalformedError``):

      1. response length <= 32,768 bytes;
      2. strict UTF-8 decode, ASCII space/tab/CR/LF trim only;
      3. reject empty / triple-backtick / non-JSON trailing content;
      4. ``json.loads(allow_nan=False)`` with a duplicate-key hook covers
         duplicate keys and NaN / Infinity;
      5. top schema exactly ``{"items": [...]}`` (no other top keys, no
         duplicate keys, ``items`` is a list);
      6. per-item exact keys / types, ID 1..256 code points, summary 1..512
         code points, no forbidden controls;
      7. when ``expected_items`` is supplied, response IDs must appear in
         the same order, exactly once, never extra or missing.
    """
    if not isinstance(response_bytes, (bytes, bytearray)):
        raise TypeError(
            f"response_bytes must be bytes, got {type(response_bytes).__name__}"
        )
    raw = bytes(response_bytes)
    if len(raw) > MAX_RESPONSE_BYTES:
        raise SummarizerMalformedError(
            f"response length {len(raw)} exceeds {MAX_RESPONSE_BYTES} bytes"
        )
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise SummarizerMalformedError(
            f"response is not strict UTF-8: {exc}"
        ) from exc
    trimmed = _strip_ascii_whitespace(text)
    if not trimmed:
        raise SummarizerMalformedError(
            "response body is empty after whitespace trim"
        )
    if trimmed.startswith("```"):
        raise SummarizerMalformedError(
            "response is wrapped in a Markdown code fence"
        )

    def _reject_constant(token):
        # ``parse_constant`` is invoked for ``NaN`` / ``Infinity`` /
        # ``-Infinity`` literals; reject them per §2.3.
        raise SummarizerMalformedError(
            f"response contains non-finite constant: {token!r}"
        )

    try:
        parsed = json.loads(
            trimmed,
            parse_constant=_reject_constant,
            object_pairs_hook=_reject_duplicate_pairs,
        )
    except json.JSONDecodeError as exc:
        raise SummarizerMalformedError(
            f"response is not valid JSON: {exc}"
        ) from exc
    except RecursionError as exc:
        raise SummarizerMalformedError("response JSON nesting is too deep") from exc
    if not isinstance(parsed, dict):
        raise SummarizerMalformedError(
            f"expected object at response root, got {type(parsed).__name__}"
        )
    if set(parsed.keys()) != {"items"}:
        raise SummarizerMalformedError(
            f"response root keys must be exactly {{'items'}}, got "
            f"{sorted(parsed.keys())!r}"
        )
    items_raw = parsed["items"]
    if not isinstance(items_raw, list):
        raise SummarizerMalformedError(
            f"expected list at response.items, got {type(items_raw).__name__}"
        )
    if len(items_raw) > MAX_ITEMS_PER_CATEGORY:
        raise SummarizerMalformedError("response contains more than 32 items")
    expected = (
        list(expected_items) if expected_items is not None else None
    )
    if expected is not None and len(items_raw) != len(expected):
        raise SummarizerMalformedError(
            f"response items length mismatch: expected "
            f"{len(expected)} got {len(items_raw)}"
        )
    out: List[Tuple[str, str]] = []
    seen: Dict[str, int] = {}
    for position, raw_item in enumerate(items_raw):
        expected_id = (
            expected[position].candidate_id if expected is not None else None
        )
        cid, summary = _validate_response_item(
            raw_item, expected_id=expected_id, position=position
        )
        if cid in seen:
            raise SummarizerMalformedError(
                f"response candidate_id {cid!r} appears at multiple "
                f"positions ({seen[cid]} and {position})"
            )
        seen[cid] = position
        out.append((cid, summary))
    return out


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------


_UNTITLED_LITERAL = "Untitled item"


def _summarizer_input_bound_violated(raw: SummarizerInput) -> Tuple[bool, bool, bool, bool]:
    """Return candidate/title/snippet/url bound-failure flags."""
    candidate_bad = not (CANDIDATE_ID_MIN <= len(raw.candidate_id) <= CANDIDATE_ID_MAX)
    title_bad = (
        not (TITLE_MIN <= len(raw.title) <= TITLE_MAX)
        or _has_forbidden_controls(raw.title)
    )
    snippet_bad = not (SNIPPET_MIN <= len(raw.snippet) <= SNIPPET_MAX)
    url_bad = False
    if raw.url is not None:
        try:
            _validate_http_url(raw.url)
        except ValueError:
            url_bad = True
    return candidate_bad, title_bad, snippet_bad, url_bad


def _fallback_summary_text(raw: SummarizerInput) -> str:
    """Return the §2.3 fallback summary literal.

    Uses the exact original title when valid; falls back to the literal
    ``"Untitled item"`` when the title itself was invalid.
    """
    if (
        TITLE_MIN <= len(raw.title) <= TITLE_MAX
        and not _has_forbidden_controls(raw.title)
    ):
        return raw.title
    return _UNTITLED_LITERAL


def _fallback_item(
    raw: SummarizerInput,
    error_category: SummarizerErrorCategory,
) -> SummaryItem:
    return SummaryItem(
        candidate_id=raw.candidate_id,
        summary=_fallback_summary_text(raw),
        source=SummarySource.FALLBACK,
        error_category=error_category,
    )


def _cache_items_to_source(
    cached: SummarizerCacheValue,
) -> Tuple[SummaryItem, ...]:
    """Rebuild per-item rows from a cache hit, switching source to CACHE."""
    return tuple(
        SummaryItem(
            candidate_id=it.candidate_id,
            summary=it.summary,
            source=SummarySource.CACHE,
            error_category=None,
            subject_id=it.subject_id,
            event_id=it.event_id,
            event_version=it.event_version,
            source_url=it.source_url,
            what_changed=it.what_changed,
            why_it_matters=it.why_it_matters,
        )
        for it in cached.items
    )


class SummarizerSession:
    """Stateful one-run summarizer engine.

    Tracks model call count, cache-hit count, and per-category uncached-
    call counts. The constructor takes:

      * ``transport`` — a ``Callable[[bytes], bytes]`` injected by the
        engine; raises ``SummarizerTransportError`` on transport-level
        failures (the only exception caught around the transport);
      * ``cache`` — optional ``Mapping[bytes, SummarizerCacheValue]``.
        Keys are produced by ``request_cache_key``; values persist parsed
        summaries and the original model outcome.

    Methods:

      * ``summarize_category(category, raw_inputs)`` — see §2.3 / §2.5
        item 6 contract (INPUT_BOUNDS, BUDGET_EXHAUSTED, TRANSPORT_ERROR,
        MALFORMED_OUTPUT; preserves every raw input ID).
      * ``model_call_count`` / ``cache_hit_count`` — exact attributes for
        the future engine's bookkeeping surface.
    """

    def __init__(
        self,
        transport: Any,
        cache: Optional[MutableMapping[bytes, SummarizerCacheValue]] = None,
    ) -> None:
        if not callable(transport):
            raise TypeError(
                f"transport must be callable, got {type(transport).__name__}"
            )
        if cache is not None and not isinstance(cache, MutableMapping):
            raise TypeError("cache must be a mutable mapping or None")
        self._transport = transport
        self._cache: MutableMapping[bytes, SummarizerCacheValue] = (
            {} if cache is None else cache
        )
        self._model_calls = 0
        self._cache_hits = 0
        self._uncached_calls: Dict[Category, int] = {}
        self._uncached_subject_calls: Dict[Subject, int] = {}

    @property
    def model_call_count(self) -> int:
        return self._model_calls

    @property
    def cache_hit_count(self) -> int:
        return self._cache_hits

    def summarize_category(
        self,
        category: Category,
        raw_inputs: Sequence[SummarizerInput],
    ) -> CategorySummaryResult:
        """Produce a ``CategorySummaryResult`` for ``raw_inputs``.

        Caller contract:

          * ``category`` must be a ``Category`` enum member;
          * ``raw_inputs`` must be a tuple / list of ``SummarizerInput``.

        Per §2.3 / §2.5 item 6:

          * input bound failures produce one ``INPUT_BOUNDS`` fallback per
            affected raw input, preserving the original candidate ID;
          * valid request items are capped at 32 per category (overshoot
            produces ``INPUT_BOUNDS`` fallback for the extras, never
            silently dropped);
          * cache hit ⇒ ``CACHE`` source for every item,
            ``cache_hit=True``, ``model_used=False``, no budget consumption;
          * cache miss + budget left ⇒ transport invoked exactly once per
            category; parser error ⇒ ``MALFORMED_OUTPUT``; transport
            error ⇒ ``TRANSPORT_ERROR``;
          * a second uncached call per category, or a 9th total call,
            returns ``BUDGET_EXHAUSTED`` fallbacks for every valid item
            without invoking the transport.

        No raw input is sliced, dropped, or duplicated.
        """
        if not isinstance(category, Category):
            raise TypeError(
                f"category must be a Category enum, got {type(category).__name__}"
            )
        if not isinstance(raw_inputs, (tuple, list)):
            raise TypeError(
                f"raw_inputs must be tuple/list, got {type(raw_inputs).__name__}"
            )
        seen_raw_ids: Dict[str, None] = {}
        for idx, raw in enumerate(raw_inputs):
            if not isinstance(raw, SummarizerInput):
                raise TypeError(
                    f"raw_inputs[{idx}] must be SummarizerInput, got "
                    f"{type(raw).__name__}"
                )
            if raw.candidate_id in seen_raw_ids:
                raise ValueError("duplicate raw candidate_id")
            seen_raw_ids[raw.candidate_id] = None

        # Stage 1: per raw input classify as bound-rejected INPUT_BOUNDS or
        # eligible for a model request. Preserve original raw input order.
        classifications: List[Dict[str, Any]] = []
        any_valid = False
        for index, raw in enumerate(raw_inputs):
            candidate_bad, title_bad, snippet_bad, url_bad = _summarizer_input_bound_violated(raw)
            if candidate_bad or title_bad or snippet_bad or url_bad:
                classifications.append(
                    {"index": index, "raw": raw, "outcome": "input_bounds"}
                )
                continue
            # Strict request-item construction re-validates types / bounds
            # with the same length / content rules. It must not raise on any
            # raw input that just passed the bound checks here.
            request_item = SummarizerRequestItem(
                candidate_id=raw.candidate_id,
                title=raw.title,
                snippet=raw.snippet,
                url=raw.url,
            )
            classifications.append(
                {
                    "index": index,
                    "raw": raw,
                    "outcome": "valid",
                    "request_item": request_item,
                }
            )
            any_valid = True

        valid_indices: List[int] = [
            entry["index"] for entry in classifications if entry["outcome"] == "valid"
        ]

        # Empty-input short-circuit: no model call, both flags False.
        if not any_valid:
            items = tuple(
                _fallback_item(
                    entry["raw"], SummarizerErrorCategory.INPUT_BOUNDS
                )
                for entry in classifications
            )
            return CategorySummaryResult(
                items=items, model_used=False, cache_hit=False
            )

        # Build ordered valid item list (raw input order preserved). Cap at
        # MAX_ITEMS_PER_CATEGORY. Extras become INPUT_BOUNDS fallback.
        ordered_kept_items: List[SummarizerRequestItem] = []
        kept_indices: List[int] = []
        overflow_indices: List[int] = []
        for entry in classifications:
            if entry["outcome"] != "valid":
                continue
            if len(ordered_kept_items) < MAX_ITEMS_PER_CATEGORY:
                ordered_kept_items.append(entry["request_item"])
                kept_indices.append(entry["index"])
            else:
                entry["outcome"] = "overflow"
                overflow_indices.append(entry["index"])

        # Build the exact request bytes once. Aggregate-byte overflow is an
        # INPUT_BOUNDS outcome owned by the session and never invokes transport.
        try:
            request_bytes = canonical_request_bytes(category, ordered_kept_items)
        except ValueError:
            return CategorySummaryResult(
                items=tuple(
                    _fallback_item(entry["raw"], SummarizerErrorCategory.INPUT_BOUNDS)
                    for entry in classifications
                ),
                model_used=False,
                cache_hit=False,
            )
        cache_key = hashlib.sha256(request_bytes).digest()
        cached = self._cache.get(cache_key)
        if cached is not None:
            expected_cache_ids = tuple(it.candidate_id for it in ordered_kept_items)
            cached_ids = tuple(it.candidate_id for it in cached.items)
            if cached_ids != expected_cache_ids:
                raise RuntimeError("cache value candidate IDs/order do not match request")
            self._cache_hits += 1
            rebuilt: List[SummaryItem] = []
            cached_rows = {it.candidate_id: it for it in _cache_items_to_source(cached)}
            for entry in classifications:
                raw = entry["raw"]
                if entry["outcome"] in ("input_bounds", "overflow"):
                    rebuilt.append(
                        _fallback_item(
                            entry["raw"], SummarizerErrorCategory.INPUT_BOUNDS
                        )
                    )
                else:
                    row = cached_rows.get(raw.candidate_id)
                    if row is None:
                        raise RuntimeError("cache value candidate IDs do not match request")
                    rebuilt.append(row)
            return CategorySummaryResult(
                items=tuple(rebuilt), model_used=False, cache_hit=True
            )

        # Cache miss. Check budget before invoking the transport.
        uncached_count = self._uncached_calls.get(category, 0)
        budget_left_total = self._model_calls < TOTAL_CALL_BUDGET
        budget_left_category = uncached_count < ONE_CALL_PER_CATEGORY
        if not budget_left_total or not budget_left_category:
            rebuilt = []
            for entry in classifications:
                if entry["outcome"] in ("input_bounds", "overflow"):
                    rebuilt.append(
                        _fallback_item(
                            entry["raw"], SummarizerErrorCategory.INPUT_BOUNDS
                        )
                    )
                else:
                    rebuilt.append(
                        _fallback_item(
                            entry["raw"], SummarizerErrorCategory.BUDGET_EXHAUSTED
                        )
                    )
            return CategorySummaryResult(
                items=tuple(rebuilt), model_used=False, cache_hit=False
            )

        # Uncached call within budget — dispatch and process.
        try:
            response_bytes = self._transport(request_bytes)
        except SummarizerTransportError:
            self._model_calls += 1
            self._uncached_calls[category] = uncached_count + 1
            return CategorySummaryResult(
                items=self._items_with_fallback_for_valid(
                    classifications, SummarizerErrorCategory.TRANSPORT_ERROR
                ),
                model_used=True,
                cache_hit=False,
            )
        self._model_calls += 1
        self._uncached_calls[category] = uncached_count + 1
        try:
            parsed = parse_summary_response(
                response_bytes, expected_items=ordered_kept_items
            )
        except SummarizerMalformedError:
            return CategorySummaryResult(
                items=self._items_with_fallback_for_valid(
                    classifications, SummarizerErrorCategory.MALFORMED_OUTPUT
                ),
                model_used=True,
                cache_hit=False,
            )

        # Success — build per-item rows in raw input order; preserve
        # INPUT_BOUNDS rows for bound-rejected raw inputs.
        parsed_map: Dict[str, str] = {cid: summary for cid, summary in parsed}
        rebuilt = []
        for entry in classifications:
            raw = entry["raw"]
            if entry["outcome"] in ("input_bounds", "overflow"):
                rebuilt.append(
                    _fallback_item(
                        raw, SummarizerErrorCategory.INPUT_BOUNDS
                    )
                )
            else:
                try:
                    summary = parsed_map[raw.candidate_id]
                except KeyError as exc:
                    raise RuntimeError("parser success omitted a requested candidate ID") from exc
                rebuilt.append(
                    SummaryItem(
                        candidate_id=raw.candidate_id,
                        summary=summary,
                        source=SummarySource.MODEL,
                        error_category=None,
                    )
                )
        # Persist success into cache (parsed summaries + model outcome).
        success_items = tuple(
            SummaryItem(
                candidate_id=cid,
                summary=summary,
                source=SummarySource.MODEL,
                error_category=None,
            )
            for cid, summary in parsed
        )
        self._cache[cache_key] = SummarizerCacheValue(
            items=success_items, model_used=True
        )
        return CategorySummaryResult(
            items=tuple(rebuilt), model_used=True, cache_hit=False
        )

    def summarize_subject(
        self,
        subject: Subject,
        raw_inputs: Sequence[SubjectEditorialInput],
    ) -> CategorySummaryResult:
        """Generate one isolated, strictly-QC'd report for one subject.

        This is deliberately separate from ``summarize_category``. Legacy
        category callers retain their frozen contract, while Run 7 callers
        receive one request envelope containing only one subject's verified
        event versions, fact deltas, source URLs, and policy.
        """
        if not isinstance(subject, Subject):
            raise TypeError("subject must be a Subject enum")
        if not isinstance(raw_inputs, (tuple, list)):
            raise TypeError("raw_inputs must be tuple/list")
        validated = validate_subject_inputs(subject, tuple(raw_inputs))
        if not validated:
            return CategorySummaryResult(items=(), model_used=False, cache_hit=False)

        kept = tuple(validated[:MAX_ITEMS_PER_SUBJECT])
        overflow = tuple(validated[MAX_ITEMS_PER_SUBJECT:])
        try:
            request_bytes = canonical_subject_request_bytes(subject, kept)
        except ValueError:
            return CategorySummaryResult(
                items=self._subject_fallback_items(
                    validated, SummarizerErrorCategory.INPUT_BOUNDS
                ),
                model_used=False,
                cache_hit=False,
            )
        cache_key = hashlib.sha256(request_bytes).digest()
        cached = self._cache.get(cache_key)
        expected_ids = tuple(
            event_version_identity(item.subject.value, item.event_id, item.event_version)
            for item in kept
        )
        if cached is not None:
            cached_ids = tuple(item.candidate_id for item in cached.items)
            if cached_ids != expected_ids:
                raise RuntimeError("subject cache value IDs do not match request")
            self._cache_hits += 1
            cached_rows = {
                item.candidate_id: item for item in _cache_items_to_source(cached)
            }
            rows = [cached_rows[item_id] for item_id in expected_ids]
            rows.extend(
                self._subject_fallback_items(
                    overflow, SummarizerErrorCategory.INPUT_BOUNDS
                )
            )
            return CategorySummaryResult(
                items=tuple(rows), model_used=False, cache_hit=True
            )

        subject_calls = self._uncached_subject_calls.get(subject, 0)
        if self._model_calls >= TOTAL_CALL_BUDGET or subject_calls >= ONE_CALL_PER_SUBJECT:
            return CategorySummaryResult(
                items=self._subject_fallback_items(
                    kept, SummarizerErrorCategory.BUDGET_EXHAUSTED
                )
                + self._subject_fallback_items(
                    overflow, SummarizerErrorCategory.INPUT_BOUNDS
                ),
                model_used=False,
                cache_hit=False,
            )

        try:
            response_bytes = self._transport(request_bytes)
        except SummarizerTransportError:
            self._model_calls += 1
            self._uncached_subject_calls[subject] = subject_calls + 1
            return CategorySummaryResult(
                items=self._subject_fallback_items(
                    kept, SummarizerErrorCategory.TRANSPORT_ERROR
                )
                + self._subject_fallback_items(
                    overflow, SummarizerErrorCategory.INPUT_BOUNDS
                ),
                model_used=True,
                cache_hit=False,
            )
        self._model_calls += 1
        self._uncached_subject_calls[subject] = subject_calls + 1
        try:
            outputs = parse_subject_response(
                response_bytes, subject=subject, expected_inputs=kept
            )
        except (SummarizerMalformedError, EditorialQCError):
            return CategorySummaryResult(
                items=self._subject_fallback_items(
                    kept, SummarizerErrorCategory.MALFORMED_OUTPUT
                )
                + self._subject_fallback_items(
                    overflow, SummarizerErrorCategory.INPUT_BOUNDS
                ),
                model_used=True,
                cache_hit=False,
            )

        success_items = tuple(
            SummaryItem(
                candidate_id=event_version_identity(
                    output.subject.value, output.event_id, output.event_version
                ),
                summary=render_summary((output,)).text.replace("\n", " "),
                source=SummarySource.MODEL,
                error_category=None,
                subject_id=output.subject.value,
                event_id=output.event_id,
                event_version=output.event_version,
                source_url=output.source_url,
                what_changed=output.what_changed,
                why_it_matters=output.why_it_matters,
            )
            for output in outputs
        )
        self._cache[cache_key] = SummarizerCacheValue(
            items=success_items, model_used=True
        )
        return CategorySummaryResult(
            items=success_items
            + self._subject_fallback_items(
                overflow, SummarizerErrorCategory.INPUT_BOUNDS
            ),
            model_used=True,
            cache_hit=False,
        )

    @staticmethod
    def _subject_fallback_items(
        inputs: Sequence[SubjectEditorialInput],
        error_category: SummarizerErrorCategory,
    ) -> Tuple[SummaryItem, ...]:
        return tuple(
            SummaryItem(
                candidate_id=event_version_identity(
                    item.subject.value, item.event_id, item.event_version
                ),
                summary=item.title,
                source=SummarySource.FALLBACK,
                error_category=error_category,
                subject_id=item.subject.value,
                event_id=item.event_id,
                event_version=item.event_version,
                source_url=item.source_urls[0],
            )
            for item in inputs
        )

    def _items_with_fallback_for_valid(
        self,
        classifications: List[Dict[str, Any]],
        error_category: SummarizerErrorCategory,
    ) -> Tuple[SummaryItem, ...]:
        """Build the per-item rows when a transport / parser failure hits.

        Valid-bound rows take ``error_category``; previously-bound-failed
        rows keep ``INPUT_BOUNDS`` so no ID is silently rolled into the
        transport / parser category.
        """
        rebuilt: List[SummaryItem] = []
        for entry in classifications:
            if entry["outcome"] in ("input_bounds", "overflow"):
                rebuilt.append(
                    _fallback_item(
                        entry["raw"], SummarizerErrorCategory.INPUT_BOUNDS
                    )
                )
            else:
                rebuilt.append(_fallback_item(entry["raw"], error_category))
        return tuple(rebuilt)


__all__ = (
    "ONE_CALL_PER_CATEGORY",
    "ONE_CALL_PER_SUBJECT",
    "TOTAL_CALL_BUDGET",
    "MAX_ITEMS_PER_CATEGORY",
    "MAX_ITEMS_PER_SUBJECT",
    "MAX_REQUEST_BYTES",
    "MAX_RESPONSE_BYTES",
    "SummarizerCacheValue",
    "SummarizerError",
    "SummarizerErrorCategory",
    "SummarizerInput",
    "SummarizerMalformedError",
    "SummarizerRequestItem",
    "SummarizerSession",
    "SummarizerTransportError",
    "SummaryItem",
    "SummarySource",
    "CategorySummaryResult",
    "canonical_request_bytes",
    "canonical_subject_request_bytes",
    "parse_summary_response",
    "parse_subject_response",
    "request_cache_key",
    "subject_request_cache_key",
)
