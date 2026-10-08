"""Pure Phase 4 claim/evidence adapters and deterministic extraction."""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Mapping
from urllib.parse import unquote, urlsplit

from .event_contracts import FactKind, TypedFact
from .fact_extraction import extract_facts
from .live_contracts import (
    ClaimContract, ClaimStatus, EvidenceContract, EvidenceRole,
    ObservationContract, ObservationKind, SourceRole, stable_id,
)
from .verification import authority_entity_matches, lead_retrieval_reason


@dataclass(frozen=True, slots=True)
class ClaimRows:
    claims: tuple[ClaimContract, ...]
    evidence: tuple[EvidenceContract, ...]
    observation: ObservationContract | None = None


_MKINITCPIO_HOOK_UNIT_FACT = re.compile(
    r"(?P<excerpt>Starting\s+with\s+package\s+version\s+"
    r"(?P<version>\d{1,5}(?:\.\d{1,5})?-\d{1,5})\s*,\s*"
    r"the\s+(?P<subject>mkinitcpio)\s+systemd\s+hook\s+now\s+includes\s+"
    r"(?P<unit>[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)+)"
    r"(?:\s+\([^()\r\n]{1,128}\))?\s*\.)",
    re.IGNORECASE,
)


def observation_id_to_source_item_id(connection: sqlite3.Connection, observation_id: str) -> str:
    if type(observation_id) is not str or not observation_id.strip():
        raise ValueError("observation_id must be non-empty")
    direct = connection.execute(
        "SELECT source_item_id FROM source_items WHERE source_item_id=?", (observation_id,)
    ).fetchall()
    if direct:
        return direct[0][0]
    matches = connection.execute(
        "SELECT source_item_id FROM source_items WHERE external_id=? ORDER BY source_item_id",
        (observation_id,),
    ).fetchall()
    if len(matches) != 1:
        raise ValueError("observation_id external_id mapping must resolve exactly one source item")
    return matches[0][0]


def claim_row(contract: ClaimContract, connection=None) -> tuple:
    source_item_id = observation_id_to_source_item_id(connection, contract.observation_id) if connection is not None else contract.observation_id
    return (contract.claim_id, source_item_id, contract.subject, contract.predicate,
            contract.object_value, contract.statement_type, str(contract.extraction_confidence),
            contract.status.value, contract.extracted_at)


def evidence_row(contract: EvidenceContract, connection=None) -> tuple:
    source_item_id = observation_id_to_source_item_id(connection, contract.observation_id) if connection is not None else contract.observation_id
    return (contract.evidence_id, contract.claim_id, source_item_id, contract.role.value,
            contract.exact_excerpt, contract.excerpt_hash, contract.independence_group,
            contract.observed_at)


adapt_claim_contract = claim_row
adapt_evidence_contract = evidence_row


def _json_string_tuple(name: str, value: object) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if type(value) is str:
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"source item {name} must be valid JSON") from exc
    if type(value) not in (list, tuple) or any(type(item) is not str or not item.strip() for item in value):
        raise ValueError(f"source item {name} must be an array of non-empty strings")
    result = tuple(item.strip() for item in value)
    if len(result) != len(set(result)):
        raise ValueError(f"source item {name} must not contain duplicates")
    return result


def observation_from_source_item(row: Mapping[str, object]) -> ObservationContract:
    def text(name: str, optional: bool = False):
        value = row.get(name)
        if optional and value in (None, ""):
            return None
        if type(value) is not str or not value.strip():
            raise ValueError(f"source item {name} must be non-empty text")
        return value

    role_value = row.get("effective_source_role") or SourceRole.DISCOVERY.value
    try:
        effective_source_role = SourceRole(role_value)
    except (TypeError, ValueError) as exc:
        raise ValueError("source item effective_source_role is invalid") from exc
    retrieval_method = text("retrieval_method")
    transport_reason = lead_retrieval_reason(retrieval_method)
    transport_only = transport_reason is not None
    if transport_only:
        effective_source_role = SourceRole.DISCOVERY
    authority_value = row.get("authority_match")
    if authority_value is None:
        authority_value = False
    if authority_value not in (0, 1, False, True):
        raise ValueError("source item authority_match must be boolean")
    publisher = text("publisher")
    assert publisher is not None
    has_provenance = any(
        name in row
        for name in (
            "normalized_publisher_host", "effective_source_role", "independence_group",
            "matched_rule_id", "authority_match", "classification_reason",
        )
    )
    independence_group = (
        text("independence_group", True) or "unknown"
        if has_provenance
        else publisher.casefold()
    )
    published_at = text("published_at", True)
    raw_publication_evidence = text("publication_evidence", True)
    publication_evidence = (
        raw_publication_evidence if published_at is not None else None
    )
    unknown_date_reason = (
        None
        if published_at is not None
        else (raw_publication_evidence or "missing")
    )
    return ObservationContract(
        observation_id=text("source_item_id"), source_id=text("source_id"),
        category=text("category"), kind=ObservationKind.PARSED_ARTICLE,
        original_url=text("original_url"), canonical_url=text("canonical_url"),
        publisher=publisher, retrieval_method=text("retrieval_method"),
        raw_content_hash=text("raw_content_hash"), observed_at=text("retrieved_at"),
        external_id=text("external_id", True), author_handle=text("author_handle", True),
        title=text("title", True), body=text("body", True), raw=text("raw", True),
        published_at=published_at, updated_at=text("updated_at", True),
        publication_evidence=publication_evidence,
        unknown_date_reason=unknown_date_reason,
        publisher_host=text("normalized_publisher_host", True),
        effective_source_role=effective_source_role,
        independence_group=independence_group,
        matched_rule_id=text("matched_rule_id", True),
        authority_match=bool(authority_value) and not transport_only,
        classification_reason=(
            transport_reason
            if transport_reason is not None
            else text("classification_reason", True) or "unknown_publisher"
        ),
        authority_scope=_json_string_tuple("authority_scope_json", row.get("authority_scope_json")),
        authority_entities=_json_string_tuple("authority_entities_json", row.get("authority_entities_json")),
        classification_timestamp=text("classification_timestamp", True),
    )


# Context tokens are sorted, so digits sort first and must not become a subject.
_NUMERIC_CONTEXT_TOKEN = re.compile(r"\d+")


def _fact_fields(fact: TypedFact) -> tuple[str, str, str]:
    predicate = {FactKind.DATE: "has_date", FactKind.VERSION: "has_version",
                 FactKind.PRICE: "has_price", FactKind.PERCENT: "has_percentage",
                 FactKind.COUNT: "has_count"}[fact.kind]
    subject = next(
        (token for token in fact.context if not _NUMERIC_CONTEXT_TOKEN.fullmatch(token)),
        "source item",
    )
    return (subject, predicate, fact.value_normalized)


def _first_party_mkinitcpio_fact(
    observation: ObservationContract, *, extracted_at: str
) -> tuple[ClaimContract, EvidenceContract, int] | None:
    """Extract one tightly-scoped first-party mkinitcpio hook state change.

    The full bounded body sentence must state the package-version transition,
    name the exact approved claim entity, and identify the included systemd
    unit. Feed metadata, headlines, specialist copies, and unscoped publishers
    are deliberately ineligible.
    """
    if (
        observation.retrieval_method != "publisher-article-fetch"
        or observation.category != "our_setup"
        or observation.effective_source_role is not SourceRole.PRIMARY
        or observation.category not in observation.authority_scope
        or not observation.publisher_host
        or not observation.matched_rule_id
        or not observation.independence_group
        or observation.independence_group == "unknown"
    ):
        return None
    body = observation.body or ""
    match = _MKINITCPIO_HOOK_UNIT_FACT.search(body)
    if match is None:
        return None
    subject = next(
        (
            entity
            for entity in observation.authority_entities
            if entity.strip().casefold() == match.group("subject").casefold()
        ),
        None,
    )
    if subject is None:
        return None
    excerpt = match.group("excerpt")
    version = match.group("version")
    unit = match.group("unit")
    claim_id = stable_id(
        "claim", observation.observation_id, "mkinitcpio_hook_unit",
        version, unit, excerpt,
    )
    evidence_id = stable_id(
        "evidence", claim_id, observation.observation_id, excerpt
    )
    claim = ClaimContract(
        claim_id,
        observation.observation_id,
        subject,
        "systemd_hook_includes_unit",
        f"{unit} starting with package version {version}",
        "state",
        Decimal("0.9"),
        ClaimStatus.PENDING,
        extracted_at,
    )
    evidence = EvidenceContract(
        evidence_id,
        claim_id,
        observation.observation_id,
        EvidenceRole.SUPPORTS,
        excerpt,
        hashlib.sha256(excerpt.encode("utf-8")).hexdigest(),
        observation.independence_group,
        observation.observed_at,
    )
    return claim, evidence, match.start()


def _is_leading_publication_date(
    fact: TypedFact, observation: ObservationContract, material_fact_start: int
) -> bool:
    """Exclude a matching publication-date label, not a dated material fact."""
    if fact.kind is not FactKind.DATE or not observation.published_at:
        return False
    publication_date = observation.published_at[:10]
    return (
        fact.value_normalized == publication_date
        and 0 <= (position := (observation.body or "").find(fact.evidence)) < material_fact_start
        and position < 512
    )


def claims_from_observation(observation: ObservationContract, *, extracted_at: str | None = None) -> ClaimRows:
    title, body = observation.title or "", observation.body or observation.raw or ""
    extracted = extract_facts(title, body)
    when = extracted_at or observation.observed_at
    first_party_fact = _first_party_mkinitcpio_fact(observation, extracted_at=when)
    facts = tuple(
        fact
        for group in (
            extracted.dates,
            extracted.versions,
            extracted.prices,
            extracted.percentages,
            extracted.counts,
        )
        for fact in group
        if first_party_fact is None
        or not _is_leading_publication_date(fact, observation, first_party_fact[2])
    )
    release_tag_fact = _trusted_llamacpp_release_tag_fact(observation, facts)
    if release_tag_fact is not None:
        facts = (release_tag_fact[1],)
    claims: list[ClaimContract] = []
    evidence: list[EvidenceContract] = []
    if first_party_fact is not None:
        claims.append(first_party_fact[0])
        evidence.append(first_party_fact[1])
    if not facts:
        if claims:
            return ClaimRows(tuple(claims), tuple(evidence), observation)
        excerpt = next((value[:8192] for value in (title, body, observation.raw or "") if value), None)
        if excerpt is None:
            raise ValueError("source item has no bounded evidence")
        claim_id = stable_id("claim", observation.observation_id, "source_statement", excerpt)
        evidence_id = stable_id("evidence", claim_id, excerpt)
        claims.append(ClaimContract(claim_id, observation.observation_id, "source item",
                                    "source_statement", excerpt, "source_statement",
                                    Decimal("0.5"), ClaimStatus.PENDING, when))
        evidence.append(EvidenceContract(evidence_id, claim_id, observation.observation_id,
                                         EvidenceRole.SUPPORTS, excerpt,
                                         hashlib.sha256(excerpt.encode()).hexdigest(),
                                         observation.independence_group, observation.observed_at))
        return ClaimRows(tuple(claims), tuple(evidence), observation)
    for fact in facts:
        subject, predicate, value = _fact_fields(fact)
        if release_tag_fact is not None:
            subject = release_tag_fact[0]
        claim_id = stable_id("claim", observation.observation_id, fact.kind.value,
                             fact.unit, value, fact.evidence)
        evidence_id = stable_id("evidence", claim_id, observation.observation_id, fact.evidence)
        claims.append(ClaimContract(claim_id, observation.observation_id, subject, predicate,
                                    value, fact.kind.value, Decimal("0.8"), ClaimStatus.PENDING, when))
        evidence.append(EvidenceContract(evidence_id, claim_id, observation.observation_id,
                                         EvidenceRole.SUPPORTS, fact.evidence,
                                         hashlib.sha256(fact.evidence.encode()).hexdigest(),
                                         observation.independence_group, observation.observed_at))
    return ClaimRows(tuple(claims), tuple(evidence), observation)


def _trusted_llamacpp_release_tag_fact(
    observation: ObservationContract, facts: tuple[TypedFact, ...]
) -> tuple[str, TypedFact] | None:
    """Keep only the exact tag claim for the policy-bound first-party release API."""
    if (
        observation.source_id != "github-llamacpp-release"
        or observation.category != "our_setup"
        or observation.publisher != "llama.cpp"
        or observation.publisher_host != "github.com"
        or observation.effective_source_role is not SourceRole.PRIMARY
        or not observation.authority_match
        or observation.matched_rule_id != "llama-cpp-own-release"
        or observation.classification_reason != "matched_rule"
        or observation.authority_scope != ("our_setup",)
        or observation.authority_entities != ("llama.cpp",)
        or observation.independence_group != "ggml-org-llama.cpp-origin"
    ):
        return None
    parsed = urlsplit(observation.canonical_url)
    prefix = "/ggml-org/llama.cpp/releases/tag/"
    if (
        parsed.scheme != "https"
        or parsed.hostname != "github.com"
        or parsed.port is not None
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith(prefix)
    ):
        return None
    tag = unquote(parsed.path[len(prefix):])
    version = tag[1:] if tag.startswith("v") else tag
    components = version.split(".")
    if len(components) not in (2, 3) or any(not part.isdigit() for part in components):
        return None
    title = observation.title or ""
    matches = [
        fact for fact in facts
        if fact.kind is FactKind.VERSION
        and fact.value_normalized == version
        and fact.evidence in title
    ]
    distinct = {(fact.value_normalized, fact.evidence) for fact in matches}
    if len(distinct) != 1:
        return None
    return "llama.cpp", matches[0]


def persist_claim_rows(connection, rows: ClaimRows) -> int:
    return persist_claim_rows_counts(connection, rows)[0]


def _persist_claim_evidence_provenance(connection, rows: ClaimRows) -> None:
    if connection.execute(
        "SELECT 1 FROM schema_migrations WHERE version=11"
    ).fetchone() is None:
        return
    from .provenance import normalize_publisher_host

    context = rows.observation
    claims = {claim.claim_id: claim for claim in rows.claims}
    for evidence in rows.evidence:
        claim = claims.get(evidence.claim_id)
        if claim is None:
            raise ValueError("claim evidence references a claim absent from its ClaimRows")
        if context is not None and evidence.observation_id != context.observation_id:
            raise ValueError("claim evidence provenance must match its source observation")
        source_item_id = observation_id_to_source_item_id(connection, evidence.observation_id)
        role = context.effective_source_role.value if context is not None else SourceRole.DISCOVERY.value
        group = context.independence_group if context is not None else "unknown"
        rule_id = context.matched_rule_id if context is not None else None
        scopes = context.authority_scope if context is not None else ()
        entities = context.authority_entities if context is not None else ()
        category = context.category if context is not None else ""
        host_value = (
            context.publisher_host or context.canonical_url
            if context is not None
            else "unknown"
        )
        try:
            host = normalize_publisher_host(host_value)
        except ValueError:
            host = "unknown"
            role = SourceRole.DISCOVERY.value
            group = "unknown"
            rule_id = None
            scopes = ()
            entities = ()
        authority_match = (
            role == SourceRole.PRIMARY.value
            and category in scopes
            and authority_entity_matches(claim.subject, entities)
        )
        applied_at = (
            context.classification_timestamp or context.observed_at
            if context is not None
            else evidence.observed_at
        )
        reason = (
            context.classification_reason
            if context is not None
            else "missing_source_provenance"
        )
        values = (
            evidence.evidence_id,
            source_item_id,
            host,
            role,
            group or "unknown",
            rule_id,
            json.dumps(scopes, ensure_ascii=False, separators=(",", ":")),
            json.dumps(entities, ensure_ascii=False, separators=(",", ":")),
            int(authority_match),
            applied_at,
            reason,
        )
        connection.execute(
            """INSERT OR IGNORE INTO claim_evidence_provenance(
                   evidence_id,source_item_id,normalized_publisher_host,
                   effective_source_role,independence_group,matched_rule_id,
                   authority_scope_json,authority_entities_json,authority_match,
                   classification_timestamp,classification_reason)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            values,
        )
        persisted = connection.execute(
            """SELECT evidence_id,source_item_id,normalized_publisher_host,
                      effective_source_role,independence_group,matched_rule_id,
                      authority_scope_json,authority_entities_json,authority_match,
                      classification_timestamp,classification_reason
                 FROM claim_evidence_provenance WHERE evidence_id=?""",
            (evidence.evidence_id,),
        ).fetchone()
        if persisted != values:
            raise ValueError("claim evidence provenance conflicts with its immutable persisted snapshot")


def persist_claim_rows_counts(connection, rows: ClaimRows) -> tuple[int, int]:
    """Persist claims, evidence, and their immutable claim-specific provenance snapshot."""
    claims_inserted = 0
    evidence_inserted = 0
    for claim in rows.claims:
        connection.execute("""INSERT OR IGNORE INTO claims
            (claim_id,source_item_id,subject,predicate,object_value,statement_type,
             extraction_confidence,status,extracted_at) VALUES (?,?,?,?,?,?,?,?,?)""", claim_row(claim, connection))
        claims_inserted += connection.execute("SELECT changes()").fetchone()[0]
    for evidence in rows.evidence:
        connection.execute("""INSERT OR IGNORE INTO claim_evidence
            (evidence_id,claim_id,source_item_id,evidence_role,exact_excerpt,excerpt_hash,
             independence_group,observed_at) VALUES (?,?,?,?,?,?,?,?)""", evidence_row(evidence, connection))
        evidence_inserted += connection.execute("SELECT changes()").fetchone()[0]
    _persist_claim_evidence_provenance(connection, rows)
    return claims_inserted, evidence_inserted


persist_claims = persist_claim_rows
