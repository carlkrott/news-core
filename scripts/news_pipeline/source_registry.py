"""Strict TOML loader for the Phase 1 news source registry.

All paths are explicit. Importing this module performs no file or environment I/O.
"""
from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from .live_contracts import (
    CATEGORY_VALUES,
    REPORT_SCOPE_VALUES,
    SUBJECT_VALUES,
    QuerySeed,
    SourceAdapter,
    SourceContract,
    SourceRole,
    stable_feed_lane_id,
)
from .models import Subject, subject_for_category

_SOURCE_TOP_KEYS = frozenset({"version", "sources"})
_SOURCE_REQUIRED = frozenset({"source_id", "adapter_type", "source_role", "host", "category_scope", "enabled", "queries"})
_SOURCE_OPTIONAL = frozenset({"title_blocklist", "content_blocklist", "url_blocklist", "allowlist_domains", "cadence_minutes", "terms_notes", "rate_limit_notes", "next_due_at"})
_QUERY_REQUIRED = frozenset({"text", "categories"})
_QUERY_OPTIONAL = frozenset({"pipeline_category", "feed_lane_id"})
_TOPIC_TOP_KEYS = frozenset({"version", "topics"})
_TOPIC_KEYS = frozenset({"category", "subject", "label", "included_in_subject_report", "consequential_only"})
_POLICY_TOP_KEYS = frozenset({"version", "pipeline", "report", "subjects", "deferred"})
_PIPELINE_KEYS = frozenset({"database", "journal_mode", "serialized_writer", "dry_run", "breaking_alerts", "reddit_direct_ingestion", "evidence_rule", "raw_body_retention_days", "normalized_retention"})
_REPORT_KEYS = frozenset({"scope", "categories", "subjects", "time", "timezone", "channel", "verified_events_only", "watchlist_max_items", "formats", "depth"})
_SUBJECT_POLICY_KEYS = frozenset({"label", "included_in_report", "inclusion_rules", "exclusion_rules", "materiality_rule", "recency_days", "max_story_count"})
_DEFERRED_KEYS = frozenset({"decisions"})


@dataclass(frozen=True, slots=True)
class TopicPolicy:
    category: str
    subject: Subject
    label: str
    included_in_subject_report: bool
    consequential_only: bool

    def __post_init__(self) -> None:
        if self.category not in CATEGORY_VALUES:
            raise ValueError(f"unknown topic category: {self.category!r}")
        if not isinstance(self.subject, Subject):
            raise ValueError("topic subject must be a known Subject")
        if subject_for_category(self.category) is not self.subject:
            raise ValueError(f"topic subject does not match category: {self.category!r}")
        if type(self.label) is not str or not self.label.strip():
            raise ValueError("topic label must be a non-empty string")
        if type(self.included_in_subject_report) is not bool or type(self.consequential_only) is not bool:
            raise ValueError("topic flags must be booleans")


@dataclass(frozen=True, slots=True)
class SubjectPolicy:
    subject: Subject
    label: str
    included_in_report: bool
    inclusion_rules: tuple[str, ...]
    exclusion_rules: tuple[str, ...]
    materiality_rule: str
    recency_days: int
    max_story_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.subject, Subject):
            raise ValueError("subject policy must name a known Subject")
        if type(self.label) is not str or not self.label.strip():
            raise ValueError("subject policy label must be a non-empty string")
        if type(self.included_in_report) is not bool:
            raise ValueError("subject policy included_in_report must be a boolean")
        for name in ("inclusion_rules", "exclusion_rules"):
            value = getattr(self, name)
            if type(value) is not tuple or not value or any(type(item) is not str or not item.strip() for item in value):
                raise ValueError(f"{name} must be a non-empty tuple of strings")
            if len(value) != len(set(value)):
                raise ValueError(f"{name} must not contain duplicates")
        if type(self.materiality_rule) is not str or not self.materiality_rule.strip():
            raise ValueError("materiality_rule must be a non-empty string")
        if type(self.recency_days) is not int or type(self.recency_days) is bool or self.recency_days < 1:
            raise ValueError("recency_days must be an integer >= 1")
        if type(self.max_story_count) is not int or type(self.max_story_count) is bool or self.max_story_count < 1:
            raise ValueError("max_story_count must be an integer >= 1")


@dataclass(frozen=True, slots=True)
class PipelinePolicy:
    database: str
    journal_mode: str
    serialized_writer: bool
    dry_run: bool
    breaking_alerts: str
    reddit_direct_ingestion: bool
    evidence_rule: str
    raw_body_retention_days: int
    normalized_retention: str

    def __post_init__(self) -> None:
        if self.database != "sqlite" or self.journal_mode != "wal" or self.serialized_writer is not True:
            raise ValueError("Phase 1 requires SQLite WAL with one serialized writer")
        if self.dry_run is not True:
            raise ValueError("Phase 1 policy must remain dry-run")
        if self.breaking_alerts != "pipeline_failure_only":
            raise ValueError("breaking alerts must be limited to pipeline failures")
        if self.reddit_direct_ingestion is not False:
            raise ValueError("direct Reddit ingestion must remain disabled")
        if type(self.evidence_rule) is not str or not self.evidence_rule.strip():
            raise ValueError("evidence_rule must be a non-empty string")
        if type(self.raw_body_retention_days) is not int or type(self.raw_body_retention_days) is bool or self.raw_body_retention_days != 30:
            raise ValueError("raw response bodies must be retained for 30 days")
        if self.normalized_retention != "indefinite":
            raise ValueError("normalized records must be retained indefinitely")


@dataclass(frozen=True, slots=True)
class ReportPolicy:
    scope: str
    categories: tuple[str, ...]
    subjects: tuple[Subject, ...]
    time: str
    timezone: str
    channel: str
    verified_events_only: bool
    watchlist_max_items: int
    formats: tuple[str, ...]
    depth: str

    def __post_init__(self) -> None:
        if self.scope not in REPORT_SCOPE_VALUES or self.time != "08:00" or self.timezone != "Europe/London" or self.channel != "telegram":
            raise ValueError("report delivery must be per-subject at 08:00 Europe/London on Telegram")
        if type(self.categories) is not tuple or len(self.categories) != len(set(self.categories)):
            raise ValueError("report categories must be a unique tuple")
        if set(self.categories) != set(CATEGORY_VALUES):
            raise ValueError("per-subject reports must retain all eight ingest categories")
        if type(self.subjects) is not tuple or any(not isinstance(subject, Subject) for subject in self.subjects):
            raise ValueError("report subjects must be a tuple of Subject values")
        if len(self.subjects) != len(set(self.subjects)) or {subject.value for subject in self.subjects} != set(SUBJECT_VALUES):
            raise ValueError("per-subject reports must define every subject exactly once")
        if self.verified_events_only is not True:
            raise ValueError("main report must contain verified events only")
        if type(self.watchlist_max_items) is not int or type(self.watchlist_max_items) is bool or self.watchlist_max_items != 3:
            raise ValueError("watchlist must be capped at three items")
        if self.formats != ("json", "jsonl", "markdown", "manifest"):
            raise ValueError("report formats must be json, jsonl, markdown, and manifest")
        if self.depth != "short_human_plus_comprehensive_archive":
            raise ValueError("report depth does not match the approved default")


@dataclass(frozen=True, slots=True)
class NewsConfig:
    version: int
    sources: tuple[SourceContract, ...]
    topics: tuple[TopicPolicy, ...]
    pipeline: PipelinePolicy
    report: ReportPolicy
    subject_policies: tuple[SubjectPolicy, ...]
    deferred_decisions: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.version != 2:
            raise ValueError("configuration contract version 2 required; migrate version 1 configuration")
        source_ids = tuple(source.source_id for source in self.sources)
        if source_ids != tuple(sorted(source_ids)) or len(source_ids) != len(set(source_ids)):
            raise ValueError("sources must be uniquely ordered by source_id")
        topic_categories = tuple(topic.category for topic in self.topics)
        if topic_categories != tuple(sorted(topic_categories)) or len(topic_categories) != len(set(topic_categories)):
            raise ValueError("topics must be uniquely ordered by category")
        if set(topic_categories) != set(CATEGORY_VALUES):
            raise ValueError("topics must define all eight categories")
        topic_subjects = tuple(topic.subject.value for topic in self.topics)
        if set(topic_subjects) != set(SUBJECT_VALUES):
            raise ValueError("topics must map all subjects")
        subject_ids = tuple(policy.subject.value for policy in self.subject_policies)
        if subject_ids != tuple(sorted(subject_ids)) or len(subject_ids) != len(set(subject_ids)) or set(subject_ids) != set(SUBJECT_VALUES):
            raise ValueError("subject policies must be uniquely ordered and complete")
        source_categories = {category for source in self.sources for category in source.category_scope}
        if source_categories != set(topic_categories):
            raise ValueError("source and topic category references are inconsistent")
        if set(self.report.categories) != set(topic_categories):
            raise ValueError("report and topic category references are inconsistent")
        if {subject.value for subject in self.report.subjects} != set(subject_ids):
            raise ValueError("report and subject policy references are inconsistent")
        if type(self.deferred_decisions) is not tuple or not self.deferred_decisions:
            raise ValueError("deferred_decisions must be a non-empty tuple")

    @property
    def sources_by_id(self) -> Mapping[str, SourceContract]:
        return MappingProxyType({source.source_id: source for source in self.sources})

    @property
    def topics_by_category(self) -> Mapping[str, TopicPolicy]:
        return MappingProxyType({topic.category: topic for topic in self.topics})

    @property
    def subjects_by_id(self) -> Mapping[Subject, SubjectPolicy]:
        return MappingProxyType({policy.subject: policy for policy in self.subject_policies})


def _load_toml(path: str | Path) -> dict[str, Any]:
    candidate = Path(path)
    with candidate.open("rb") as handle:
        value = tomllib.load(handle)
    if type(value) is not dict:
        raise ValueError(f"{candidate} must contain a TOML table")
    return value


def _exact_keys(name: str, value: Mapping[str, Any], *, required: frozenset[str], optional: frozenset[str] = frozenset()) -> None:
    actual = set(value)
    missing = required - actual
    unknown = actual - required - optional
    if missing or unknown:
        raise ValueError(f"{name} keys invalid; missing={sorted(missing)}, unknown={sorted(unknown)}")


def _version(name: str, raw: Mapping[str, Any]) -> int:
    value = raw.get("version")
    if type(value) is not int or type(value) is bool or value != 2:
        raise ValueError(f"{name} version 2 required; migrate version 1 configuration")
    return value


def _string_tuple(name: str, value: object, *, nonempty: bool = False) -> tuple[str, ...]:
    if type(value) is not list or any(type(item) is not str or not item.strip() for item in value):
        raise ValueError(f"{name} must be an array of non-empty strings")
    result = tuple(value)
    if nonempty and not result:
        raise ValueError(f"{name} must not be empty")
    if len(result) != len(set(result)):
        raise ValueError(f"{name} must not contain duplicates")
    return result


def _subject_tuple(name: str, value: object) -> tuple[Subject, ...]:
    values = _string_tuple(name, value, nonempty=True)
    try:
        subjects = tuple(Subject(item) for item in values)
    except ValueError as exc:
        raise ValueError(f"{name} contains an unknown subject") from exc
    return subjects


def _parse_sources(raw: Mapping[str, Any]) -> tuple[SourceContract, ...]:
    _exact_keys("news-sources", raw, required=_SOURCE_TOP_KEYS)
    _version("news-sources", raw)
    entries = raw["sources"]
    if type(entries) is not list or not entries:
        raise ValueError("sources must be a non-empty array of tables")
    result: list[SourceContract] = []
    for index, entry in enumerate(entries):
        if type(entry) is not dict:
            raise ValueError(f"sources[{index}] must be a table")
        _exact_keys(f"sources[{index}]", entry, required=_SOURCE_REQUIRED, optional=_SOURCE_OPTIONAL)
        category_scope = _string_tuple("category_scope", entry["category_scope"], nonempty=True)
        raw_queries = entry["queries"]
        if type(raw_queries) is not list or not raw_queries:
            raise ValueError(f"sources[{index}].queries must be a non-empty array of tables")
        queries: list[QuerySeed] = []
        for query_index, query in enumerate(raw_queries):
            if type(query) is not dict:
                raise ValueError(f"sources[{index}].queries[{query_index}] must be a table")
            _exact_keys(
                f"sources[{index}].queries[{query_index}]",
                query,
                required=_QUERY_REQUIRED,
                optional=_QUERY_OPTIONAL,
            )
            pipeline_category = query.get("pipeline_category")
            if pipeline_category is None and len(category_scope) == 1:
                pipeline_category = category_scope[0]
            lane_id = query.get("feed_lane_id")
            seed = QuerySeed(
                text=query["text"],
                categories=_string_tuple("query categories", query["categories"], nonempty=True),
                pipeline_category=pipeline_category,
                feed_lane_id=lane_id,
            )
            if lane_id is not None:
                assert seed.pipeline_category is not None
                expected_lane_id = stable_feed_lane_id(
                    entry["source_id"],
                    seed.pipeline_category,
                    seed.text,
                    seed.categories,
                )
                if lane_id != expected_lane_id:
                    raise ValueError(
                        f"sources[{index}].queries[{query_index}].feed_lane_id does not match its stable identity"
                    )
            queries.append(seed)
        try:
            adapter = SourceAdapter(entry["adapter_type"])
            role = SourceRole(entry["source_role"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"sources[{index}] has invalid adapter_type or source_role") from exc
        host = entry["host"]
        if type(host) is not str or "reddit.com" in host.casefold():
            raise ValueError("direct Reddit sources are disabled")
        result.append(SourceContract(
            source_id=entry["source_id"], adapter_type=adapter, source_role=role, host=host,
            category_scope=category_scope,
            enabled=entry["enabled"], queries=tuple(queries),
            title_blocklist=_string_tuple("title_blocklist", entry.get("title_blocklist", [])),
            content_blocklist=_string_tuple("content_blocklist", entry.get("content_blocklist", [])),
            url_blocklist=_string_tuple("url_blocklist", entry.get("url_blocklist", [])),
            allowlist_domains=_string_tuple("allowlist_domains", entry.get("allowlist_domains", [])),
            cadence_minutes=entry.get("cadence_minutes"), terms_notes=entry.get("terms_notes"),
            rate_limit_notes=entry.get("rate_limit_notes"), next_due_at=entry.get("next_due_at"),
        ))
    if len({source.source_id for source in result}) != len(result):
        raise ValueError("duplicate source_id")
    return tuple(sorted(result, key=lambda source: source.source_id))


def _parse_topics(raw: Mapping[str, Any]) -> tuple[TopicPolicy, ...]:
    _exact_keys("news-topics", raw, required=_TOPIC_TOP_KEYS)
    _version("news-topics", raw)
    entries = raw["topics"]
    if type(entries) is not list or not entries:
        raise ValueError("topics must be a non-empty array of tables")
    topics: list[TopicPolicy] = []
    for index, entry in enumerate(entries):
        if type(entry) is not dict:
            raise ValueError(f"topics[{index}] must be a table")
        _exact_keys(f"topics[{index}]", entry, required=_TOPIC_KEYS)
        try:
            subject = Subject(entry["subject"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"topics[{index}] has an invalid subject") from exc
        topics.append(TopicPolicy(
            category=entry["category"],
            subject=subject,
            label=entry["label"],
            included_in_subject_report=entry["included_in_subject_report"],
            consequential_only=entry["consequential_only"],
        ))
    if len({topic.category for topic in topics}) != len(topics):
        raise ValueError("duplicate topic category")
    return tuple(sorted(topics, key=lambda topic: topic.category))


def _parse_subjects(raw: Mapping[str, Any]) -> tuple[SubjectPolicy, ...]:
    if type(raw) is not dict or set(raw) != set(SUBJECT_VALUES):
        raise ValueError("subjects must define exactly every report subject")
    policies: list[SubjectPolicy] = []
    for subject_value, entry in raw.items():
        if type(entry) is not dict:
            raise ValueError(f"subjects.{subject_value} must be a table")
        _exact_keys(f"subjects.{subject_value}", entry, required=_SUBJECT_POLICY_KEYS)
        try:
            subject = Subject(subject_value)
        except ValueError as exc:
            raise ValueError(f"subjects.{subject_value} is not a known subject") from exc
        policies.append(SubjectPolicy(
            subject=subject,
            label=entry["label"],
            included_in_report=entry["included_in_report"],
            inclusion_rules=_string_tuple(f"subjects.{subject_value}.inclusion_rules", entry["inclusion_rules"], nonempty=True),
            exclusion_rules=_string_tuple(f"subjects.{subject_value}.exclusion_rules", entry["exclusion_rules"], nonempty=True),
            materiality_rule=entry["materiality_rule"],
            recency_days=entry["recency_days"],
            max_story_count=entry["max_story_count"],
        ))
    return tuple(sorted(policies, key=lambda policy: policy.subject.value))


def _parse_policy(raw: Mapping[str, Any]) -> tuple[PipelinePolicy, ReportPolicy, tuple[SubjectPolicy, ...], tuple[str, ...]]:
    _exact_keys("news-policy", raw, required=_POLICY_TOP_KEYS)
    _version("news-policy", raw)
    pipeline_raw = raw["pipeline"]
    report_raw = raw["report"]
    subjects_raw = raw["subjects"]
    deferred_raw = raw["deferred"]
    for name, value in (("pipeline", pipeline_raw), ("report", report_raw), ("deferred", deferred_raw)):
        if type(value) is not dict:
            raise ValueError(f"{name} must be a table")
    _exact_keys("pipeline", pipeline_raw, required=_PIPELINE_KEYS)
    _exact_keys("report", report_raw, required=_REPORT_KEYS)
    _exact_keys("deferred", deferred_raw, required=_DEFERRED_KEYS)
    pipeline = PipelinePolicy(**pipeline_raw)
    subject_policies = _parse_subjects(subjects_raw)
    report = ReportPolicy(
        scope=report_raw["scope"],
        categories=_string_tuple("report categories", report_raw["categories"], nonempty=True),
        subjects=_subject_tuple("report subjects", report_raw["subjects"]),
        time=report_raw["time"],
        timezone=report_raw["timezone"],
        channel=report_raw["channel"],
        verified_events_only=report_raw["verified_events_only"],
        watchlist_max_items=report_raw["watchlist_max_items"],
        formats=_string_tuple("report formats", report_raw["formats"], nonempty=True),
        depth=report_raw["depth"],
    )
    deferred = _string_tuple("deferred decisions", deferred_raw["decisions"], nonempty=True)
    return pipeline, report, subject_policies, deferred


def load_registry(sources_path: str | Path, topics_path: str | Path, policy_path: str | Path) -> NewsConfig:
    """Load and cross-validate the three explicit Phase 1 TOML files."""
    sources_raw = _load_toml(sources_path)
    topics_raw = _load_toml(topics_path)
    policy_raw = _load_toml(policy_path)
    versions = {_version("news-sources", sources_raw), _version("news-topics", topics_raw), _version("news-policy", policy_raw)}
    if versions != {2}:
        raise ValueError("all configuration files must use the same contract version")
    sources = _parse_sources(sources_raw)
    topics = _parse_topics(topics_raw)
    pipeline, report, subject_policies, deferred = _parse_policy(policy_raw)
    return NewsConfig(version=2, sources=sources, topics=topics, pipeline=pipeline, report=report, subject_policies=subject_policies, deferred_decisions=deferred)
