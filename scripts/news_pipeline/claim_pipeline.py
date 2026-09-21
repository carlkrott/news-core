"""Pure Phase 4 claim/evidence adapters and deterministic extraction."""
from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Mapping

from .event_contracts import FactKind, TypedFact
from .fact_extraction import extract_facts
from .live_contracts import (
    ClaimContract, ClaimStatus, EvidenceContract, EvidenceRole,
    ObservationContract, ObservationKind, SourceRole, stable_id,
)


@dataclass(frozen=True, slots=True)
class ClaimRows:
    claims: tuple[ClaimContract, ...]
    evidence: tuple[EvidenceContract, ...]


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
        authority_match=bool(authority_value),
        classification_reason=text("classification_reason", True) or "unknown_publisher",
    )


def _fact_fields(fact: TypedFact) -> tuple[str, str, str]:
    predicate = {FactKind.DATE: "has_date", FactKind.VERSION: "has_version",
                 FactKind.PRICE: "has_price", FactKind.PERCENT: "has_percentage",
                 FactKind.COUNT: "has_count"}[fact.kind]
    return (fact.context[0] if fact.context else "source item", predicate, fact.value_normalized)


def claims_from_observation(observation: ObservationContract, *, extracted_at: str | None = None) -> ClaimRows:
    title, body = observation.title or "", observation.body or observation.raw or ""
    extracted = extract_facts(title, body)
    facts = tuple(f for group in (extracted.dates, extracted.versions, extracted.prices,
                                  extracted.percentages, extracted.counts) for f in group)
    when = extracted_at or observation.observed_at
    claims: list[ClaimContract] = []
    evidence: list[EvidenceContract] = []
    if not facts:
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
        return ClaimRows(tuple(claims), tuple(evidence))
    for fact in facts:
        subject, predicate, value = _fact_fields(fact)
        claim_id = stable_id("claim", observation.observation_id, fact.kind.value,
                             fact.unit, value, fact.evidence)
        evidence_id = stable_id("evidence", claim_id, observation.observation_id, fact.evidence)
        claims.append(ClaimContract(claim_id, observation.observation_id, subject, predicate,
                                    value, fact.kind.value, Decimal("0.8"), ClaimStatus.PENDING, when))
        evidence.append(EvidenceContract(evidence_id, claim_id, observation.observation_id,
                                         EvidenceRole.SUPPORTS, fact.evidence,
                                         hashlib.sha256(fact.evidence.encode()).hexdigest(),
                                         observation.independence_group, observation.observed_at))
    return ClaimRows(tuple(claims), tuple(evidence))


def persist_claim_rows(connection, rows: ClaimRows) -> int:
    inserted = 0
    for claim in rows.claims:
        connection.execute("""INSERT OR IGNORE INTO claims
            (claim_id,source_item_id,subject,predicate,object_value,statement_type,
             extraction_confidence,status,extracted_at) VALUES (?,?,?,?,?,?,?,?,?)""", claim_row(claim, connection))
        inserted += connection.execute("SELECT changes()").fetchone()[0]
    for evidence in rows.evidence:
        connection.execute("""INSERT OR IGNORE INTO claim_evidence
            (evidence_id,claim_id,source_item_id,evidence_role,exact_excerpt,excerpt_hash,
             independence_group,observed_at) VALUES (?,?,?,?,?,?,?,?)""", evidence_row(evidence, connection))
    return inserted


def persist_claim_rows_counts(connection, rows: ClaimRows) -> tuple[int, int]:
    """Persist rows and return actual claim/evidence INSERT OR IGNORE counts."""
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
    return claims_inserted, evidence_inserted


persist_claims = persist_claim_rows
