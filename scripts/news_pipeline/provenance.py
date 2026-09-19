"""Reviewed publisher provenance rules for the delivery-disabled news pipeline."""
from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

from .live_contracts import CATEGORY_VALUES, SourceRole

PROVENANCE_CONFIG_VERSION = 1
_SOURCE_ROLES = frozenset(SourceRole)


def normalize_publisher_host(value: str) -> str:
    """Return a lowercase host without a trailing dot or URL syntax."""
    if type(value) is not str or not value.strip():
        raise ValueError("publisher host must be a non-empty string")
    raw = value.strip()
    parsed = urlsplit(raw if "://" in raw else f"https://{raw}")
    host = parsed.hostname
    if host is None or not host.strip():
        raise ValueError("publisher host must contain a hostname")
    try:
        normalized = host.rstrip(".").encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise ValueError("publisher host is not valid IDNA") from exc
    if not normalized or any(char.isspace() for char in normalized):
        raise ValueError("publisher host must not contain whitespace")
    return normalized


def _text(name: str, value: object, *, optional: bool = False) -> str | None:
    if optional and value is None:
        return None
    if type(value) is not str or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _strings(name: str, value: object) -> tuple[str, ...]:
    if type(value) is not tuple or any(type(item) is not str or not item.strip() for item in value):
        raise ValueError(f"{name} must be a tuple of non-empty strings")
    result = tuple(item.strip() for item in value)
    if len(result) != len(set(result)):
        raise ValueError(f"{name} must not contain duplicates")
    return result


@dataclass(frozen=True, slots=True)
class PublisherRule:
    rule_id: str
    host: str
    source_role: SourceRole
    independence_group: str
    categories: tuple[str, ...]
    authority_entities: tuple[str, ...] = ()
    enabled: bool = True
    audit_note: str = ""

    def __post_init__(self) -> None:
        _text("rule_id", self.rule_id)
        normalized_host = normalize_publisher_host(self.host)
        object.__setattr__(self, "host", normalized_host)
        if not isinstance(self.source_role, SourceRole):
            raise ValueError("source_role must be a SourceRole")
        _text("independence_group", self.independence_group)
        categories = _strings("categories", self.categories)
        if not set(categories).issubset(set(CATEGORY_VALUES)):
            raise ValueError("categories contain an unknown news category")
        object.__setattr__(self, "categories", categories)
        authority_entities = _strings("authority_entities", self.authority_entities)
        object.__setattr__(self, "authority_entities", authority_entities)
        if type(self.enabled) is not bool:
            raise ValueError("enabled must be a boolean")
        _text("audit_note", self.audit_note, optional=True)


@dataclass(frozen=True, slots=True)
class PublisherProvenance:
    normalized_publisher_host: str
    effective_source_role: SourceRole
    independence_group: str
    matched_rule_id: str | None
    authority_match: bool
    classification_timestamp: str
    classification_reason: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "normalized_publisher_host", normalize_publisher_host(self.normalized_publisher_host))
        if not isinstance(self.effective_source_role, SourceRole):
            raise ValueError("effective_source_role must be a SourceRole")
        _text("independence_group", self.independence_group)
        _text("classification_timestamp", self.classification_timestamp)
        _text("classification_reason", self.classification_reason)
        if type(self.matched_rule_id) is not str and self.matched_rule_id is not None:
            raise ValueError("matched_rule_id must be a string or None")
        if type(self.authority_match) is not bool:
            raise ValueError("authority_match must be a boolean")


@dataclass(frozen=True, slots=True)
class ProvenanceConfig:
    version: int
    rules: tuple[PublisherRule, ...]

    def __post_init__(self) -> None:
        if self.version != PROVENANCE_CONFIG_VERSION:
            raise ValueError("provenance configuration version 1 required")
        ids = tuple(rule.rule_id for rule in self.rules)
        if len(ids) != len(set(ids)):
            raise ValueError("publisher rule IDs must be unique")


class PublisherRegistry:
    """Immutable reviewed publisher registry with fail-closed matching."""

    def __init__(self, rules: tuple[PublisherRule, ...] = ()) -> None:
        if type(rules) is not tuple or any(type(rule) is not PublisherRule for rule in rules):
            raise ValueError("rules must be a tuple of PublisherRule values")
        ids = tuple(rule.rule_id for rule in rules)
        if len(ids) != len(set(ids)):
            raise ValueError("publisher rule IDs must be unique")
        self._rules = rules

    @property
    def rules(self) -> tuple[PublisherRule, ...]:
        return self._rules

    @staticmethod
    def _host_matches(host: str, rule_host: str) -> bool:
        return host == rule_host or host.endswith(f".{rule_host}")

    def classify(
        self,
        canonical_url: str,
        *,
        category: str,
        classified_at: str,
        claim_subject: str | None = None,
    ) -> PublisherProvenance:
        if type(category) is not str or category not in CATEGORY_VALUES:
            raise ValueError("category must be a known news category")
        host = normalize_publisher_host(canonical_url)
        candidates = [
            rule for rule in self._rules
            if rule.enabled and category in rule.categories and self._host_matches(host, rule.host)
        ]
        if not candidates:
            return PublisherProvenance(
                host, SourceRole.DISCOVERY, "unknown", None, False, classified_at, "unknown_publisher"
            )
        ranked = sorted(candidates, key=lambda rule: (host == rule.host, len(rule.host)), reverse=True)
        best_rank = (host == ranked[0].host, len(ranked[0].host))
        best = [rule for rule in ranked if (host == rule.host, len(rule.host)) == best_rank]
        if len(best) != 1:
            raise ValueError(f"ambiguous publisher rules for host {host!r} and category {category!r}")
        rule = best[0]
        authority_match = False
        if claim_subject is not None:
            subject = _text("claim_subject", claim_subject)
            assert subject is not None
            authority_match = any(subject.casefold() == entity.casefold() for entity in rule.authority_entities)
        return PublisherProvenance(
            host,
            rule.source_role,
            rule.independence_group,
            rule.rule_id,
            authority_match,
            classified_at,
            "matched_rule",
        )


def _exact_keys(name: str, value: Mapping[str, Any], required: set[str], optional: set[str]) -> None:
    actual = set(value)
    missing = required - actual
    unknown = actual - required - optional
    if missing or unknown:
        raise ValueError(f"{name} keys invalid; missing={sorted(missing)}, unknown={sorted(unknown)}")


def _toml_strings(name: str, value: object) -> tuple[str, ...]:
    if type(value) is not list or any(type(item) is not str or not item.strip() for item in value):
        raise ValueError(f"{name} must be an array of non-empty strings")
    result = tuple(item.strip() for item in value)
    if len(result) != len(set(result)):
        raise ValueError(f"{name} must not contain duplicates")
    return result


def load_provenance(path: str | Path) -> ProvenanceConfig:
    candidate = Path(path)
    with candidate.open("rb") as handle:
        raw = tomllib.load(handle)
    if type(raw) is not dict or set(raw) != {"version", "publishers"}:
        raise ValueError("news-provenance keys must be exactly version and publishers")
    if type(raw["version"]) is not int or raw["version"] != PROVENANCE_CONFIG_VERSION:
        raise ValueError("provenance configuration version 1 required")
    entries = raw["publishers"]
    if type(entries) is not list or not entries:
        raise ValueError("publishers must be a non-empty array of tables")
    rules: list[PublisherRule] = []
    for index, entry in enumerate(entries):
        if type(entry) is not dict:
            raise ValueError(f"publishers[{index}] must be a table")
        _exact_keys(
            f"publishers[{index}]",
            entry,
            {"rule_id", "host", "source_role", "independence_group", "categories"},
            {"authority_entities", "enabled", "audit_note"},
        )
        rules.append(
            PublisherRule(
                rule_id=entry["rule_id"],
                host=entry["host"],
                source_role=SourceRole(entry["source_role"]),
                independence_group=entry["independence_group"],
                categories=_toml_strings("categories", entry["categories"]),
                authority_entities=_toml_strings("authority_entities", entry.get("authority_entities", [])),
                enabled=entry.get("enabled", True),
                audit_note=entry.get("audit_note", ""),
            )
        )
    return ProvenanceConfig(1, tuple(rules))


__all__ = [
    "PROVENANCE_CONFIG_VERSION",
    "ProvenanceConfig",
    "PublisherProvenance",
    "PublisherRegistry",
    "PublisherRule",
    "load_provenance",
    "normalize_publisher_host",
]
