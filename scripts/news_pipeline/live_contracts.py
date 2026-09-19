"""Immutable Phase 1 runtime contracts and deterministic row codecs.

This module is deliberately pure: it performs no file, database, network, clock,
random, UUID, or subprocess access. Callers must supply every timestamp and ID.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping

from .models import Subject, SubjectDecision

CATEGORY_VALUES = (
    "ai",
    "world",
    "audio_engineering",
    "hardware",
    "fantasy_novel",
    "audiovisual",
    "av_corporate",
    "our_setup",
)
SUBJECT_VALUES = tuple(subject.value for subject in Subject)
SUBJECT_DECISION_VALUES = tuple(decision.value for decision in SubjectDecision)
REPORT_SCOPE_VALUES = ("per_subject",)
OBSERVATION_KIND_VALUES = ("parsed_article", "query_failure", "query_comment", "fetch_marker")
CLAIM_STATUS_VALUES = ("pending", "verified", "rejected", "superseded")
EVIDENCE_ROLE_VALUES = ("supports", "contradicts")
VERIFICATION_STATE_VALUES = ("unverified", "watchlist", "verified", "rejected")
QUERY_STATUS_VALUES = ("pending", "running", "success", "partial", "failed", "rate_limited")
REPORT_STATUS_VALUES = ("pending", "generating", "complete", "failed")
DELIVERY_STATE_VALUES = ("not_attempted", "dry_run", "sent", "failed", "skipped")
DATE_TYPE_VALUES = (
    "published_at",
    "observed_at",
    "updated_at",
    "announced_at",
    "occurred_at",
    "scheduled_for",
    "first_seen_at",
    "last_seen_at",
    "verified_at",
    "valid_from",
    "superseded_at",
)
DATE_PRECISION_VALUES = ("instant", "day", "month", "year", "range", "unknown")
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


class SourceAdapter(str, Enum):
    SEARXNG = "searxng"
    RSS = "rss"
    HACKER_NEWS = "hacker_news"
    GITHUB = "github"


class SourceRole(str, Enum):
    DISCOVERY = "discovery"
    PRIMARY = "primary"
    NEUTRAL = "neutral"
    SPECIALIST = "specialist"


class ObservationKind(str, Enum):
    PARSED_ARTICLE = "parsed_article"
    QUERY_FAILURE = "query_failure"
    QUERY_COMMENT = "query_comment"
    FETCH_MARKER = "fetch_marker"


class ClaimStatus(str, Enum):
    PENDING = "pending"
    VERIFIED = "verified"
    REJECTED = "rejected"
    SUPERSEDED = "superseded"


class EvidenceRole(str, Enum):
    SUPPORTS = "supports"
    CONTRADICTS = "contradicts"


class VerificationState(str, Enum):
    UNVERIFIED = "unverified"
    WATCHLIST = "watchlist"
    VERIFIED = "verified"
    REJECTED = "rejected"


class QueryStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"


class ReportStatus(str, Enum):
    PENDING = "pending"
    GENERATING = "generating"
    COMPLETE = "complete"
    FAILED = "failed"


class DeliveryState(str, Enum):
    NOT_ATTEMPTED = "not_attempted"
    DRY_RUN = "dry_run"
    SENT = "sent"
    FAILED = "failed"
    SKIPPED = "skipped"


class DateType(str, Enum):
    PUBLISHED_AT = "published_at"
    OBSERVED_AT = "observed_at"
    UPDATED_AT = "updated_at"
    ANNOUNCED_AT = "announced_at"
    OCCURRED_AT = "occurred_at"
    SCHEDULED_FOR = "scheduled_for"
    FIRST_SEEN_AT = "first_seen_at"
    LAST_SEEN_AT = "last_seen_at"
    VERIFIED_AT = "verified_at"
    VALID_FROM = "valid_from"
    SUPERSEDED_AT = "superseded_at"


class DatePrecision(str, Enum):
    INSTANT = "instant"
    DAY = "day"
    MONTH = "month"
    YEAR = "year"
    RANGE = "range"
    UNKNOWN = "unknown"


def _text(name: str, value: object, *, optional: bool = False) -> str | None:
    if optional and value is None:
        return None
    if type(value) is not str or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _boolean(name: str, value: object) -> bool:
    if type(value) is not bool:
        raise ValueError(f"{name} must be a boolean")
    return value


def _integer(name: str, value: object, *, minimum: int = 0, optional: bool = False) -> int | None:
    if optional and value is None:
        return None
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _decimal(name: str, value: object, *, minimum: Decimal, maximum: Decimal) -> Decimal:
    if type(value) is not Decimal or not value.is_finite() or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be a finite Decimal between {minimum} and {maximum}")
    return value


def _enum(name: str, value: object, enum_type: type[Enum]) -> None:
    if not isinstance(value, enum_type):
        raise ValueError(f"{name} must be a {enum_type.__name__}")


def _strings(name: str, value: object, *, nonempty: bool = False) -> tuple[str, ...]:
    if type(value) is not tuple or any(type(item) is not str or not item.strip() for item in value):
        raise ValueError(f"{name} must be a tuple of non-empty strings")
    if nonempty and not value:
        raise ValueError(f"{name} must not be empty")
    if len(set(value)) != len(value):
        raise ValueError(f"{name} must not contain duplicates")
    return value


def _category(name: str, value: object) -> str:
    if value not in CATEGORY_VALUES:
        raise ValueError(f"{name} must be one of {CATEGORY_VALUES}")
    return str(value)


def _categories(name: str, value: object) -> tuple[str, ...]:
    values = _strings(name, value, nonempty=True)
    for item in values:
        _category(name, item)
    return values


def _timestamp(name: str, value: object, *, optional: bool = False) -> str | None:
    from datetime import datetime

    if optional and value is None:
        return None
    text = _text(name, value)
    assert text is not None
    if not text.endswith("Z"):
        raise ValueError(f"{name} must be a UTC ISO-8601 timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{name} must be a valid UTC ISO-8601 timestamp") from exc
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{name} must be UTC")
    return text


def _hash(name: str, value: object, *, optional: bool = False) -> str | None:
    if optional and value is None:
        return None
    text = _text(name, value)
    assert text is not None
    if _HASH_RE.fullmatch(text) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")
    return text


def _json_tuple(value: tuple[str, ...]) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _tuple_from_json(name: str, value: object) -> tuple[str, ...]:
    if type(value) is not str:
        raise ValueError(f"{name} row value must be JSON text")
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} row value is not valid JSON") from exc
    if type(decoded) is not list:
        raise ValueError(f"{name} row value must decode to a list")
    result = tuple(decoded)
    return _strings(name, result)


def _required(row: Mapping[str, Any], keys: tuple[str, ...]) -> None:
    missing = [key for key in keys if key not in row]
    if missing:
        raise ValueError(f"row is missing required keys: {missing}")


def stable_id(*parts: str | None, length: int | None = 32) -> str:
    if not parts or any(part is not None and type(part) is not str for part in parts):
        raise ValueError("stable_id parts must be strings or None")
    if length is not None and (type(length) is not int or not 1 <= length <= 64):
        raise ValueError("length must be None or an integer from 1 to 64")
    payload = json.dumps(parts, ensure_ascii=False, separators=(",", ":"))
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return digest if length is None else digest[:length]


@dataclass(frozen=True, slots=True)
class QuerySeed:
    text: str
    categories: tuple[str, ...]

    def __post_init__(self) -> None:
        _text("text", self.text)
        _strings("categories", self.categories, nonempty=True)


@dataclass(frozen=True, slots=True)
class SourceContract:
    source_id: str
    adapter_type: SourceAdapter
    source_role: SourceRole
    host: str
    category_scope: tuple[str, ...]
    enabled: bool
    queries: tuple[QuerySeed, ...]
    title_blocklist: tuple[str, ...] = ()
    content_blocklist: tuple[str, ...] = ()
    url_blocklist: tuple[str, ...] = ()
    allowlist_domains: tuple[str, ...] = ()
    cadence_minutes: int | None = None
    terms_notes: str | None = None
    rate_limit_notes: str | None = None
    next_due_at: str | None = None

    def __post_init__(self) -> None:
        _text("source_id", self.source_id)
        _enum("adapter_type", self.adapter_type, SourceAdapter)
        _enum("source_role", self.source_role, SourceRole)
        _text("host", self.host)
        _categories("category_scope", self.category_scope)
        _boolean("enabled", self.enabled)
        if type(self.queries) is not tuple or not self.queries or any(type(query) is not QuerySeed for query in self.queries):
            raise ValueError("queries must be a non-empty tuple of QuerySeed values")
        for query in self.queries:
            if not set(query.categories).issubset({"general", "it", "news"}):
                raise ValueError("query categories contain an unsupported SearXNG category")
        for name in ("title_blocklist", "content_blocklist", "url_blocklist", "allowlist_domains"):
            _strings(name, getattr(self, name))
        _integer("cadence_minutes", self.cadence_minutes, minimum=1, optional=True)
        _text("terms_notes", self.terms_notes, optional=True)
        _text("rate_limit_notes", self.rate_limit_notes, optional=True)
        _timestamp("next_due_at", self.next_due_at, optional=True)


@dataclass(frozen=True, slots=True)
class ObservationContract:
    observation_id: str
    source_id: str
    category: str
    kind: ObservationKind
    original_url: str
    canonical_url: str
    publisher: str
    retrieval_method: str
    raw_content_hash: str
    observed_at: str
    external_id: str | None = None
    author_handle: str | None = None
    title: str | None = None
    body: str | None = None
    raw: str | None = None
    published_at: str | None = None
    updated_at: str | None = None
    announced_at: str | None = None
    occurred_at: str | None = None
    scheduled_for: str | None = None
    publication_evidence: str | None = None
    unknown_date_reason: str | None = None

    def __post_init__(self) -> None:
        for name in ("observation_id", "source_id", "original_url", "canonical_url", "publisher", "retrieval_method"):
            _text(name, getattr(self, name))
        _category("category", self.category)
        _enum("kind", self.kind, ObservationKind)
        _hash("raw_content_hash", self.raw_content_hash)
        _timestamp("observed_at", self.observed_at)
        for name in ("external_id", "author_handle", "title", "body", "raw", "publication_evidence", "unknown_date_reason"):
            _text(name, getattr(self, name), optional=True)
        for name in ("published_at", "updated_at", "announced_at", "occurred_at", "scheduled_for"):
            _timestamp(name, getattr(self, name), optional=True)
        if self.published_at is None and self.publication_evidence is not None:
            raise ValueError("publication_evidence requires published_at")


@dataclass(frozen=True, slots=True)
class ClaimContract:
    claim_id: str
    observation_id: str
    subject: str
    predicate: str
    object_value: str
    statement_type: str
    extraction_confidence: Decimal
    status: ClaimStatus
    extracted_at: str

    def __post_init__(self) -> None:
        for name in ("claim_id", "observation_id", "subject", "predicate", "object_value", "statement_type"):
            _text(name, getattr(self, name))
        _decimal("extraction_confidence", self.extraction_confidence, minimum=Decimal("0"), maximum=Decimal("1"))
        _enum("status", self.status, ClaimStatus)
        _timestamp("extracted_at", self.extracted_at)


@dataclass(frozen=True, slots=True)
class EvidenceContract:
    evidence_id: str
    claim_id: str
    observation_id: str
    role: EvidenceRole
    exact_excerpt: str
    excerpt_hash: str
    independence_group: str
    observed_at: str

    def __post_init__(self) -> None:
        for name in ("evidence_id", "claim_id", "observation_id", "exact_excerpt", "independence_group"):
            _text(name, getattr(self, name))
        _enum("role", self.role, EvidenceRole)
        _hash("excerpt_hash", self.excerpt_hash)
        expected = hashlib.sha256(self.exact_excerpt.encode("utf-8")).hexdigest()
        if self.excerpt_hash != expected:
            raise ValueError("excerpt_hash does not match exact_excerpt")
        _timestamp("observed_at", self.observed_at)


@dataclass(frozen=True, slots=True)
class EventVersionContract:
    event_id: str
    version: int
    material_change_reason: str
    summary: str
    verification_state: VerificationState
    valid_from: str
    superseded_at: str | None = None
    verified_at: str | None = None

    def __post_init__(self) -> None:
        _text("event_id", self.event_id)
        _integer("version", self.version, minimum=1)
        _text("material_change_reason", self.material_change_reason)
        _text("summary", self.summary)
        _enum("verification_state", self.verification_state, VerificationState)
        _timestamp("valid_from", self.valid_from)
        _timestamp("superseded_at", self.superseded_at, optional=True)
        _timestamp("verified_at", self.verified_at, optional=True)
        if self.verification_state is VerificationState.VERIFIED and self.verified_at is None:
            raise ValueError("verified event versions require verified_at")


@dataclass(frozen=True, slots=True)
class EventDateContract:
    event_date_id: str
    event_id: str
    event_version: int
    date_type: DateType
    date_value: str | None
    precision: DatePrecision
    evidence_id: str | None = None
    unknown_reason: str | None = None

    def __post_init__(self) -> None:
        _text("event_date_id", self.event_date_id)
        _text("event_id", self.event_id)
        _integer("event_version", self.event_version, minimum=1)
        _enum("date_type", self.date_type, DateType)
        _enum("precision", self.precision, DatePrecision)
        _timestamp("date_value", self.date_value, optional=True)
        _text("evidence_id", self.evidence_id, optional=True)
        _text("unknown_reason", self.unknown_reason, optional=True)
        if (self.date_value is None) == (self.unknown_reason is None):
            raise ValueError("exactly one of date_value or unknown_reason is required")
        if self.date_value is None and self.precision is not DatePrecision.UNKNOWN:
            raise ValueError("unknown date values require unknown precision")


@dataclass(frozen=True, slots=True)
class QueryPlanContract:
    query_plan_id: str
    source_id: str
    query_text: str
    category: str
    reason_selected: str
    cooldown_seconds: int
    max_rounds: int
    created_at: str
    topic: str | None = None
    entity: str | None = None

    def __post_init__(self) -> None:
        for name in ("query_plan_id", "source_id", "query_text", "reason_selected"):
            _text(name, getattr(self, name))
        _category("category", self.category)
        _integer("cooldown_seconds", self.cooldown_seconds, minimum=0)
        _integer("max_rounds", self.max_rounds, minimum=1)
        _timestamp("created_at", self.created_at)
        _text("topic", self.topic, optional=True)
        _text("entity", self.entity, optional=True)


@dataclass(frozen=True, slots=True)
class QueryAttemptContract:
    attempt_id: str
    query_plan_id: str
    status: QueryStatus
    started_at: str
    returned_count: int = 0
    novel_count: int = 0
    verified_count: int = 0
    duplicate_count: int = 0
    stale_count: int = 0
    error_count: int = 0
    finished_at: str | None = None
    error: str | None = None
    rate_limit_reset_at: str | None = None

    def __post_init__(self) -> None:
        _text("attempt_id", self.attempt_id)
        _text("query_plan_id", self.query_plan_id)
        _enum("status", self.status, QueryStatus)
        _timestamp("started_at", self.started_at)
        for name in ("returned_count", "novel_count", "verified_count", "duplicate_count", "stale_count", "error_count"):
            _integer(name, getattr(self, name), minimum=0)
        _timestamp("finished_at", self.finished_at, optional=True)
        _text("error", self.error, optional=True)
        _timestamp("rate_limit_reset_at", self.rate_limit_reset_at, optional=True)
        if self.status in (QueryStatus.SUCCESS, QueryStatus.PARTIAL, QueryStatus.FAILED, QueryStatus.RATE_LIMITED) and self.finished_at is None:
            raise ValueError("terminal query status requires finished_at")
        if self.status in (QueryStatus.FAILED, QueryStatus.RATE_LIMITED) and self.error is None:
            raise ValueError("failed or rate-limited query requires error")


@dataclass(frozen=True, slots=True)
class ArtifactDigest:
    name: str
    sha256: str
    byte_count: int

    def __post_init__(self) -> None:
        _text("name", self.name)
        _hash("sha256", self.sha256)
        _integer("byte_count", self.byte_count, minimum=0)


@dataclass(frozen=True, slots=True)
class ReportContract:
    report_id: str
    window_start: str
    window_end: str
    status: ReportStatus
    delivery_state: DeliveryState
    created_at: str
    artifacts: tuple[ArtifactDigest, ...] = ()
    delivery_id: str | None = None

    def __post_init__(self) -> None:
        _text("report_id", self.report_id)
        _timestamp("window_start", self.window_start)
        _timestamp("window_end", self.window_end)
        _enum("status", self.status, ReportStatus)
        _enum("delivery_state", self.delivery_state, DeliveryState)
        _timestamp("created_at", self.created_at)
        if type(self.artifacts) is not tuple or any(type(item) is not ArtifactDigest for item in self.artifacts):
            raise ValueError("artifacts must be a tuple of ArtifactDigest values")
        if tuple(sorted(item.name for item in self.artifacts)) != tuple(item.name for item in self.artifacts):
            raise ValueError("artifacts must be ordered by name")
        if len({item.name for item in self.artifacts}) != len(self.artifacts):
            raise ValueError("artifact names must be unique")
        _text("delivery_id", self.delivery_id, optional=True)


def source_to_row(value: SourceContract) -> dict[str, Any]:
    query_data = [{"text": query.text, "categories": list(query.categories)} for query in value.queries]
    return {
        "source_id": value.source_id,
        "adapter_type": value.adapter_type.value,
        "source_role": value.source_role.value,
        "host": value.host,
        "category_scope_json": _json_tuple(value.category_scope),
        "enabled": int(value.enabled),
        "queries_json": json.dumps(query_data, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        "title_blocklist_json": _json_tuple(value.title_blocklist),
        "content_blocklist_json": _json_tuple(value.content_blocklist),
        "url_blocklist_json": _json_tuple(value.url_blocklist),
        "allowlist_domains_json": _json_tuple(value.allowlist_domains),
        "cadence_minutes": value.cadence_minutes,
        "terms_notes": value.terms_notes,
        "rate_limit_notes": value.rate_limit_notes,
        "next_due_at": value.next_due_at,
    }


def source_from_row(row: Mapping[str, Any]) -> SourceContract:
    keys = ("source_id", "adapter_type", "source_role", "host", "category_scope_json", "enabled", "queries_json", "title_blocklist_json", "content_blocklist_json", "url_blocklist_json", "allowlist_domains_json", "cadence_minutes", "terms_notes", "rate_limit_notes", "next_due_at")
    _required(row, keys)
    try:
        raw_queries = json.loads(row["queries_json"])
        queries = tuple(QuerySeed(text=item["text"], categories=tuple(item["categories"])) for item in raw_queries)
    except (TypeError, KeyError, json.JSONDecodeError) as exc:
        raise ValueError("queries_json is invalid") from exc
    if row["enabled"] not in (0, 1):
        raise ValueError("enabled row value must be 0 or 1")
    return SourceContract(
        source_id=row["source_id"], adapter_type=SourceAdapter(row["adapter_type"]), source_role=SourceRole(row["source_role"]),
        host=row["host"], category_scope=_tuple_from_json("category_scope_json", row["category_scope_json"]),
        enabled=bool(row["enabled"]), queries=queries,
        title_blocklist=_tuple_from_json("title_blocklist_json", row["title_blocklist_json"]),
        content_blocklist=_tuple_from_json("content_blocklist_json", row["content_blocklist_json"]),
        url_blocklist=_tuple_from_json("url_blocklist_json", row["url_blocklist_json"]),
        allowlist_domains=_tuple_from_json("allowlist_domains_json", row["allowlist_domains_json"]),
        cadence_minutes=row["cadence_minutes"], terms_notes=row["terms_notes"], rate_limit_notes=row["rate_limit_notes"], next_due_at=row["next_due_at"],
    )


def observation_to_row(value: ObservationContract) -> dict[str, Any]:
    return {field: getattr(value, field) for field in value.__dataclass_fields__} | {"kind": value.kind.value}


def observation_from_row(row: Mapping[str, Any]) -> ObservationContract:
    keys = tuple(ObservationContract.__dataclass_fields__)
    _required(row, keys)
    values = {key: row[key] for key in keys}
    values["kind"] = ObservationKind(values["kind"])
    return ObservationContract(**values)


def claim_to_row(value: ClaimContract) -> dict[str, Any]:
    return {"claim_id": value.claim_id, "observation_id": value.observation_id, "subject": value.subject, "predicate": value.predicate, "object_value": value.object_value, "statement_type": value.statement_type, "extraction_confidence": str(value.extraction_confidence), "status": value.status.value, "extracted_at": value.extracted_at}


def claim_from_row(row: Mapping[str, Any]) -> ClaimContract:
    keys = tuple(ClaimContract.__dataclass_fields__)
    _required(row, keys)
    return ClaimContract(claim_id=row["claim_id"], observation_id=row["observation_id"], subject=row["subject"], predicate=row["predicate"], object_value=row["object_value"], statement_type=row["statement_type"], extraction_confidence=Decimal(row["extraction_confidence"]), status=ClaimStatus(row["status"]), extracted_at=row["extracted_at"])


def evidence_to_row(value: EvidenceContract) -> dict[str, Any]:
    return {field: (getattr(value, field).value if field == "role" else getattr(value, field)) for field in value.__dataclass_fields__}


def evidence_from_row(row: Mapping[str, Any]) -> EvidenceContract:
    keys = tuple(EvidenceContract.__dataclass_fields__)
    _required(row, keys)
    values = {key: row[key] for key in keys}
    values["role"] = EvidenceRole(values["role"])
    return EvidenceContract(**values)


def event_version_to_row(value: EventVersionContract) -> dict[str, Any]:
    return {field: (getattr(value, field).value if field == "verification_state" else getattr(value, field)) for field in value.__dataclass_fields__}


def event_version_from_row(row: Mapping[str, Any]) -> EventVersionContract:
    keys = tuple(EventVersionContract.__dataclass_fields__)
    _required(row, keys)
    values = {key: row[key] for key in keys}
    values["verification_state"] = VerificationState(values["verification_state"])
    return EventVersionContract(**values)


def event_date_to_row(value: EventDateContract) -> dict[str, Any]:
    return {field: (getattr(value, field).value if field in ("date_type", "precision") else getattr(value, field)) for field in value.__dataclass_fields__}


def event_date_from_row(row: Mapping[str, Any]) -> EventDateContract:
    keys = tuple(EventDateContract.__dataclass_fields__)
    _required(row, keys)
    values = {key: row[key] for key in keys}
    values["date_type"] = DateType(values["date_type"])
    values["precision"] = DatePrecision(values["precision"])
    return EventDateContract(**values)


def query_plan_to_row(value: QueryPlanContract) -> dict[str, Any]:
    return {field: getattr(value, field) for field in value.__dataclass_fields__}


def query_plan_from_row(row: Mapping[str, Any]) -> QueryPlanContract:
    keys = tuple(QueryPlanContract.__dataclass_fields__)
    _required(row, keys)
    return QueryPlanContract(**{key: row[key] for key in keys})


def query_attempt_to_row(value: QueryAttemptContract) -> dict[str, Any]:
    return {field: (getattr(value, field).value if field == "status" else getattr(value, field)) for field in value.__dataclass_fields__}


def query_attempt_from_row(row: Mapping[str, Any]) -> QueryAttemptContract:
    keys = tuple(QueryAttemptContract.__dataclass_fields__)
    _required(row, keys)
    values = {key: row[key] for key in keys}
    values["status"] = QueryStatus(values["status"])
    return QueryAttemptContract(**values)


def report_to_row(value: ReportContract) -> dict[str, Any]:
    artifacts = [{"byte_count": item.byte_count, "name": item.name, "sha256": item.sha256} for item in value.artifacts]
    return {"report_id": value.report_id, "window_start": value.window_start, "window_end": value.window_end, "status": value.status.value, "delivery_state": value.delivery_state.value, "created_at": value.created_at, "artifacts_json": json.dumps(artifacts, separators=(",", ":"), sort_keys=True), "delivery_id": value.delivery_id}


def report_from_row(row: Mapping[str, Any]) -> ReportContract:
    keys = ("report_id", "window_start", "window_end", "status", "delivery_state", "created_at", "artifacts_json", "delivery_id")
    _required(row, keys)
    try:
        decoded = json.loads(row["artifacts_json"])
        artifacts = tuple(ArtifactDigest(name=item["name"], sha256=item["sha256"], byte_count=item["byte_count"]) for item in decoded)
    except (TypeError, KeyError, json.JSONDecodeError) as exc:
        raise ValueError("artifacts_json is invalid") from exc
    return ReportContract(report_id=row["report_id"], window_start=row["window_start"], window_end=row["window_end"], status=ReportStatus(row["status"]), delivery_state=DeliveryState(row["delivery_state"]), created_at=row["created_at"], artifacts=artifacts, delivery_id=row["delivery_id"])
