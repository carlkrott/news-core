"""Transactional, replay-safe Phase 4 processor and event-version store."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Mapping, cast

from .claim_pipeline import claims_from_observation, observation_from_source_item, persist_claim_rows_counts
from .adjudication import _family_hits, _has_positive
from .event_contracts import FactDelta, FactKind, event_version_identity
from .live_contracts import VerificationState, stable_id
from .schema_v5 import _validate_v5
from .schema_v7 import backfill_source_item_provenance, registry_from_connection
from .verification import plan_corroboration_queries, verify_evidence


@dataclass(frozen=True, slots=True)
class EventWrite:
    event_id: str
    summary: str
    material_change_reason: str
    verification_state: str = "unverified"
    valid_from: str = ""
    claim_ids: tuple[str, ...] = ()
    retraction: bool = False
    semantic_decision: str | None = None
    fact_deltas: tuple[object, ...] = ()
    event_version: int | None = None
    subject_id: str | None = None
    subject_suppressed: bool = False


@dataclass(frozen=True, slots=True)
class Phase4Report:
    selected: int = 0
    processed: int = 0
    claims_inserted: int = 0
    evidence_inserted: int = 0
    claim_status_updates: int = 0
    events_created: int = 0
    versions_appended: int = 0
    event_links_inserted: int = 0
    query_plans_inserted: int = 0
    event_dates_inserted: int = 0

    @property
    def selected_count(self): return self.selected
    @property
    def processed_count(self): return self.processed
    @property
    def event_versions_inserted(self): return self.versions_appended


def event_version_key(subject_id: str, event_id: str, event_version: int) -> str:
    """Return the ledger's canonical subject/event/version identity."""
    return event_version_identity(subject_id, event_id, event_version)


def _timestamp(value: object, name: str) -> str:
    if type(value) is not str or not value.endswith("Z"):
        raise ValueError(f"{name} must be a UTC ISO-8601 timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{name} must be a valid UTC ISO-8601 timestamp") from exc
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise ValueError(f"{name} must be UTC")
    return value


def _decision_value(decision):
    if isinstance(decision, Mapping):
        value = dict(decision)
    else:
        names = (
            "event_id", "summary", "material_change_reason", "verification_state",
            "valid_from", "claim_ids", "retraction", "semantic_decision",
            "fact_deltas", "event_version", "subject_id", "subject_suppressed", "decision",
        )
        value = {name: getattr(decision, name) for name in names if hasattr(decision, name)}
    if type(value) is not dict:
        raise ValueError("decision must be a mapping or EventWrite")
    return value


def _validated_fact_deltas(raw_deltas: object) -> tuple[object, ...]:
    if type(raw_deltas) not in (tuple, list):
        raise ValueError("fact_deltas must be a tuple or list")
    deltas = tuple(cast(tuple[object, ...] | list[object], raw_deltas))
    for delta in deltas:
        def valid_topic_gate(value: object) -> bool:
            try:
                gate = Decimal(str(value))
            except (InvalidOperation, ValueError):
                return False
            return gate.is_finite() and Decimal("0") <= gate <= Decimal("1")

        if isinstance(delta, FactDelta):
            valid = (
                type(delta.unit) is str
                and bool(delta.unit.strip())
                and type(delta.old_value) is str
                and type(delta.new_value) is str
                and bool(delta.old_value.strip())
                and bool(delta.new_value.strip())
                and delta.old_value != delta.new_value
                and valid_topic_gate(delta.topic_gate)
            )
        elif isinstance(delta, Mapping):
            kind = delta.get("kind")
            kind_value = getattr(kind, "value", kind)
            valid = (
                kind_value in {member.value for member in FactKind}
                and type(delta.get("unit")) is str
                and bool(delta["unit"].strip())
                and type(delta.get("old_value")) is str
                and type(delta.get("new_value")) is str
                and bool(delta["old_value"].strip())
                and bool(delta["new_value"].strip())
                and delta["old_value"] != delta["new_value"]
                and valid_topic_gate(delta.get("topic_gate"))
            )
        else:
            valid = False
        if not valid:
            raise ValueError("fact_deltas must contain grounded FactDelta records")
    return deltas


def _event_write_fields(item: Mapping) -> tuple[str, str, str, str, str, tuple[str, ...], bool, str | None, tuple[object, ...], int | None, str | None, bool]:
    allowed = {
        "event_id", "summary", "material_change_reason", "reason",
        "verification_state", "valid_from", "evaluated_at", "claim_ids",
        "retraction", "semantic_decision", "decision", "fact_deltas",
        "event_version", "subject_id", "subject_suppressed",
    }
    if any(key not in allowed for key in item):
        raise ValueError("event version mapping contains unknown keys")
    event_id, summary = item.get("event_id"), item.get("summary")
    reason, state, valid_from = item.get("material_change_reason", item.get("reason", "phase4")), item.get("verification_state", "unverified"), item.get("valid_from", item.get("evaluated_at"))
    if any(type(x) is not str or not x.strip() for x in (event_id, summary, reason)):
        raise ValueError("event version text fields must be non-empty strings")
    if isinstance(state, VerificationState): state = state.value
    if type(state) is not str or state not in {x.value for x in VerificationState}:
        raise ValueError("invalid verification_state")
    _timestamp(valid_from, "valid_from")
    semantic = item.get("semantic_decision", item.get("decision"))
    if semantic is not None:
        semantic = getattr(semantic, "value", semantic)
        if semantic not in {"distinct_event", "material_update", "rewrite"}:
            raise ValueError("invalid semantic_decision")
    raw_ids = item.get("claim_ids", ())
    if type(raw_ids) not in (tuple, list) or (semantic != "rewrite" and not raw_ids) or any(type(x) is not str or not x.strip() for x in raw_ids) or len(set(raw_ids)) != len(raw_ids):
        raise ValueError("claim_ids must be a duplicate-free sequence of strings")
    retraction = item.get("retraction", False)
    if type(retraction) is not bool:
        raise ValueError("retraction must be a boolean")
    if retraction:
        reason, state = "retraction", VerificationState.REJECTED.value
    fact_deltas = _validated_fact_deltas(item.get("fact_deltas", ()))
    if semantic == "material_update" and not fact_deltas:
        raise ValueError("material_update requires at least one grounded fact/state delta")
    expected_version = item.get("event_version")
    if expected_version is not None and (type(expected_version) is not int or expected_version <= 0):
        raise ValueError("event_version must be a positive int or None")
    subject_id = item.get("subject_id")
    if subject_id is not None and (type(subject_id) is not str or not subject_id.strip()):
        raise ValueError("subject_id must be a non-empty string or None")
    subject_suppressed = item.get("subject_suppressed", False)
    if type(subject_suppressed) is not bool:
        raise ValueError("subject_suppressed must be a boolean")
    return cast(tuple[str, str, str, str, str, tuple[str, ...], bool, str | None, tuple[object, ...], int | None, str | None, bool], (
        event_id, summary, reason, state, valid_from, tuple(raw_ids), retraction,
        semantic, fact_deltas, expected_version, subject_id, subject_suppressed,
    ))


def _require_v5(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA foreign_keys=ON")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement could not be enabled")
    _validate_v5(connection)


def _has_schema_v7(connection: sqlite3.Connection) -> bool:
    return connection.execute(
        "SELECT 1 FROM schema_migrations WHERE version=7"
    ).fetchone() is not None


def _v7_evidence_row(row: tuple, claim_subject: str) -> dict[str, object]:
    evidence_role, effective_role, group, authority_match, authority_entities = row
    if effective_role is None:
        return {
            "role": evidence_role,
            "effective_source_role": "",
            "independence_group": "",
            "authority_match": None,
        }
    authority = authority_match in (1, True)
    if not authority and effective_role == "primary" and isinstance(authority_entities, str):
        try:
            entities = json.loads(authority_entities)
        except json.JSONDecodeError:
            entities = ()
        if isinstance(entities, list):
            authority = any(
                isinstance(entity, str) and entity.casefold() == claim_subject.casefold()
                for entity in entities
            )
    return {
        "role": evidence_role,
        "effective_source_role": effective_role,
        "independence_group": group,
        "authority_match": authority,
    }


def _v7_claims_are_verified(
    connection: sqlite3.Connection, claim_ids: tuple[str, ...]
) -> bool:
    """Require verified claims and complete v7 evidence before direct promotion."""
    if not claim_ids or len(set(claim_ids)) != len(claim_ids):
        return False
    placeholders = ",".join("?" for _ in claim_ids)
    claim_rows = connection.execute(
        f"SELECT claim_id,subject,status FROM claims WHERE claim_id IN ({placeholders})",
        claim_ids,
    ).fetchall()
    if len(claim_rows) != len(claim_ids):
        return False
    all_evidence: list[dict[str, object]] = []
    for claim_id, subject, status in claim_rows:
        if status != VerificationState.VERIFIED.value:
            return False
        evidence_rows = connection.execute(
            """SELECT ce.evidence_role,sp.effective_source_role,
                      sp.independence_group,sp.authority_match,pr.authority_entities_json
                 FROM claim_evidence ce
                 LEFT JOIN source_item_provenance sp ON sp.source_item_id=ce.source_item_id
                 LEFT JOIN publisher_registry pr ON pr.rule_id=sp.matched_rule_id
                WHERE ce.claim_id=? ORDER BY ce.evidence_id""",
            (claim_id,),
        ).fetchall()
        if not evidence_rows:
            return False
        all_evidence.extend(
            _v7_evidence_row(row, str(subject)) for row in evidence_rows
        )
    return verify_evidence(tuple(all_evidence)) is VerificationState.VERIFIED


def _is_retraction_text(source: Mapping[str, object]) -> bool:
    text = " ".join(str(source.get(name) or "") for name in ("title", "body"))
    hits = _family_hits(text)
    return _has_positive(hits, "retraction") and not _has_positive(hits, "correction")


def _correction_retraction_kind(source: Mapping[str, object]) -> str | None:
    text = " ".join(str(source.get(name) or "") for name in ("title", "body"))
    hits = _family_hits(text)
    has_correction = _has_positive(hits, "correction")
    has_retraction = _has_positive(hits, "retraction")
    if has_retraction and not has_correction:
        return "retraction"
    if has_correction and not has_retraction:
        return "correction"
    return None


def _append_locked(connection: sqlite3.Connection, decisions: Iterable[EventWrite | Mapping], max_items: int) -> int:
    if type(max_items) is not int or max_items < 0:
        raise ValueError("max_items must be a non-negative int")
    written = 0
    for raw in tuple(decisions)[:max_items]:
        item = _decision_value(raw)
        (
            event_id, summary, reason, state, valid_from, claim_ids, _retraction,
            semantic, _fact_deltas, expected_version, _subject_id, subject_suppressed,
        ) = _event_write_fields(item)
        if subject_suppressed:
            continue
        if state == VerificationState.VERIFIED.value and _has_schema_v7(connection):
            if not _v7_claims_are_verified(connection, claim_ids):
                raise ValueError(
                    "verified event writes require v7-verified claims and evidence"
                )
        prior = connection.execute("SELECT version FROM event_versions WHERE event_id=? ORDER BY version DESC LIMIT 1", (event_id,)).fetchone()
        version = prior[0] + 1 if prior else 1
        if semantic == "rewrite":
            continue
        if semantic == "distinct_event" and prior is not None:
            continue
        if expected_version is not None and expected_version != version:
            raise ValueError(
                f"event_version {expected_version} is stale; next durable version is {version}"
            )
        latest = connection.execute("SELECT summary,material_change_reason,verification_state,valid_from,version FROM event_versions WHERE event_id=? ORDER BY version DESC LIMIT 1", (event_id,)).fetchone()
        linked = (tuple(r[0] for r in connection.execute("SELECT claim_id FROM event_claims WHERE event_id=? AND event_version=? ORDER BY claim_id", (event_id, latest[4]))) if latest else ())
        if latest and latest[:4] == (summary, reason, state, valid_from) and linked == tuple(sorted(claim_ids)):
            continue
        connection.execute("INSERT INTO event_versions(event_id,version,material_change_reason,summary,verification_state,valid_from,superseded_at,verified_at) VALUES (?,?,?,?,?,?,?,?)", (event_id, version, reason, summary, state, valid_from, None, valid_from if state == "verified" else None))
        for claim_id in claim_ids:
            connection.execute("INSERT INTO event_claims(event_id,event_version,claim_id) VALUES (?,?,?)", (event_id, version, claim_id))
        if version > 1:
            connection.execute("UPDATE event_versions SET superseded_at=? WHERE event_id=? AND version=? AND superseded_at IS NULL", (valid_from, event_id, version - 1))
        written += 1
    return written


def append_event_versions(connection: sqlite3.Connection, decisions: Iterable[EventWrite | Mapping], *, max_items: int = 100) -> int:
    """Append validated versions inside one locked transaction; max_items=0 is a write-free no-op."""
    _require_v5(connection)
    if type(max_items) is not int or max_items < 0:
        raise ValueError("max_items must be a non-negative int")
    connection.execute("BEGIN IMMEDIATE")
    try:
        written = _append_locked(connection, decisions, max_items)
        violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        if violations: raise sqlite3.IntegrityError(f"foreign-key violations: {violations[:3]}")
    except Exception:
        connection.rollback(); raise
    connection.commit()
    return written


persist_event_versions = append_event_versions


def append_event_version(connection: sqlite3.Connection, decision: EventWrite | Mapping) -> int:
    return append_event_versions(connection, (decision,), max_items=1)


def _source_decisions(connection, max_items: int):
    rows = connection.execute("""SELECT d.id,d.decision_kind,d.reason,d.decided_at,si.source_item_id,
        r.kind,r.provenance
        FROM decisions d JOIN source_items si ON si.source_item_id=json_extract(d.reason,'$.source_item_id')
        JOIN runs r ON r.id=d.run_id
        WHERE d.decided_by='phase3' AND d.decision_kind IN ('keep','promote')
          AND NOT EXISTS (SELECT 1 FROM claims c WHERE c.source_item_id=si.source_item_id)
        ORDER BY d.decided_at,d.id LIMIT ?""", (max_items,)).fetchall()
    result = []
    for decision_id, kind, reason, decided_at, source_item_id, kind_of_run, provenance in rows:
        if type(reason) is not str: raise ValueError("Phase 3 decision reason must be JSON text")
        try: parsed = json.loads(reason)
        except json.JSONDecodeError as exc: raise ValueError("Phase 3 decision reason is invalid JSON") from exc
        if type(parsed) is not dict or parsed.get("source_item_id") != source_item_id:
            raise ValueError("Phase 3 decision reason lacks a matching source_item_id")
        matched = parsed.get("matched_observation_ids", ())
        if type(matched) not in (list, tuple) or any(type(value) is not str or not value.strip() for value in matched) or len(set(matched)) != len(matched):
            raise ValueError("matched_observation_ids must be a unique sequence of strings")
        result.append((decision_id, kind, parsed, decided_at, source_item_id, kind_of_run, provenance))
    return result


def process_phase4(db_path: str | Path, evaluated_at: str, *, max_items: int = 100, max_corroboration_plans: int = 2) -> Phase4Report:
    _timestamp(evaluated_at, "evaluated_at")
    if not isinstance(db_path, (str, Path)) or not str(db_path).strip() or not Path(db_path).is_file():
        raise ValueError("db_path must name an existing database file")
    if type(max_items) is not int or not 0 <= max_items <= 1000: raise ValueError("max_items must be an integer from 0 to 1000")
    if type(max_corroboration_plans) is not int or not 0 <= max_corroboration_plans <= 2: raise ValueError("max_corroboration_plans must be an integer from 0 to 2")
    connection = sqlite3.connect(str(db_path), isolation_level=None, timeout=10.0)
    connection.execute("PRAGMA busy_timeout=10000")
    try:
        _require_v5(connection)
        has_v7 = _has_schema_v7(connection)
        connection.execute("BEGIN IMMEDIATE")
        selected = _source_decisions(connection, max_items)
        if max_items == 0: selected = []
        run_id = stable_id("phase4-process-run", evaluated_at, length=64)
        if selected:
            live = any(item[5] == "live_ingest" or item[6] == "observed_live" for item in selected)
            run_kind = "live_ingest" if live else "historical_replay"
            run_provenance = "observed_live" if live else "observed_historical"
            connection.execute("INSERT OR IGNORE INTO runs(id,started_at,finished_at,kind,provenance,source_dir,notes) VALUES (?,?,?,?,?,?,?)", (run_id,evaluated_at,None,run_kind,run_provenance,str(db_path),""))
        counts = [0,0,0,0,0,0,0,0,0]
        plans_remaining = max_corroboration_plans
        if has_v7 and selected:
            selected_item_ids = tuple(item[4] for item in selected)
            placeholders = ",".join("?" for _ in selected_item_ids)
            missing_provenance = tuple(
                row[0]
                for row in connection.execute(
                    f"""SELECT si.source_item_id
                           FROM source_items si
                          WHERE si.source_item_id IN ({placeholders})
                            AND NOT EXISTS (
                                SELECT 1 FROM source_item_provenance sp
                                 WHERE sp.source_item_id=si.source_item_id
                            )
                          ORDER BY si.source_item_id""",
                    selected_item_ids,
                )
            )
            if missing_provenance:
                backfill_source_item_provenance(
                    connection,
                    registry_from_connection(connection),
                    evaluated_at,
                    source_item_ids=missing_provenance,
                )
        for decision_id, kind, reason, decided_at, source_item_id, _kind_of_run, _provenance in selected:
            explicit_semantic = reason.get("semantic_decision", reason.get("decision"))
            if explicit_semantic is not None:
                explicit_semantic = getattr(explicit_semantic, "value", explicit_semantic)
                if explicit_semantic not in {"distinct_event", "material_update", "rewrite"}:
                    raise ValueError("invalid semantic_decision in Phase 3 decision")
                if explicit_semantic == "rewrite":
                    counts[7] += 1
                    continue
                if explicit_semantic == "material_update":
                    _validated_fact_deltas(reason.get("fact_deltas", ()))
            subject_suppressed = reason.get("subject_suppressed", False)
            if type(subject_suppressed) is not bool:
                raise ValueError("subject_suppressed in Phase 3 decision must be a boolean")
            columns = tuple(r[1] for r in connection.execute("PRAGMA table_info(source_items)"))
            if has_v7:
                columns += (
                    "normalized_publisher_host", "effective_source_role", "independence_group",
                    "matched_rule_id", "authority_match", "classification_reason",
                )
                source_row = connection.execute("""SELECT si.*,
                    sp.normalized_publisher_host,sp.effective_source_role,sp.independence_group,
                    sp.matched_rule_id,sp.authority_match,sp.classification_reason
                    FROM source_items si
                    LEFT JOIN source_item_provenance sp ON sp.source_item_id=si.source_item_id
                    WHERE si.source_item_id=?""", (source_item_id,)).fetchone()
            else:
                source_row = connection.execute("SELECT * FROM source_items WHERE source_item_id=?", (source_item_id,)).fetchone()
            source = dict(zip(columns, source_row))
            observation = observation_from_source_item(source)
            rows = claims_from_observation(observation, extracted_at=evaluated_at)
            inserted_claims, inserted_evidence = persist_claim_rows_counts(connection, rows)
            counts[0] += inserted_claims
            counts[1] += inserted_evidence
            claim_ids = tuple(c.claim_id for c in rows.claims)
            claim_keys = connection.execute("SELECT DISTINCT subject,predicate,object_value FROM claims WHERE claim_id IN (%s)" % ",".join("?" for _ in claim_ids), claim_ids).fetchall()
            equivalent_ids = set()
            for claim_key in claim_keys:
                equivalent = connection.execute("SELECT claim_id FROM claims WHERE subject=? AND predicate=? AND object_value=? ORDER BY claim_id", claim_key).fetchall()
                ids = tuple(row[0] for row in equivalent)
                equivalent_ids.update(ids)
                if has_v7:
                    evidence_rows = connection.execute("""SELECT ce.evidence_role,sp.effective_source_role,
                        sp.independence_group,sp.authority_match,pr.authority_entities_json
                        FROM claim_evidence ce
                        LEFT JOIN source_item_provenance sp ON sp.source_item_id=ce.source_item_id
                        LEFT JOIN publisher_registry pr ON pr.rule_id=sp.matched_rule_id
                        WHERE ce.claim_id IN (%s) ORDER BY ce.evidence_id""" % ",".join("?" for _ in ids), ids).fetchall()
                    state = verify_evidence(tuple(_v7_evidence_row(row, str(claim_key[0])) for row in evidence_rows))
                else:
                    evidence_rows = connection.execute("""SELECT ce.evidence_role,ce.independence_group,si.source_role
                        FROM claim_evidence ce JOIN source_items si ON si.source_item_id=ce.source_item_id
                        WHERE ce.claim_id IN (%s) ORDER BY ce.evidence_id""" % ",".join("?" for _ in ids), ids).fetchall()
                    state = verify_evidence(tuple({"role": r[0], "independence_group": r[1], "source_role": r[2]} for r in evidence_rows))
                if state is VerificationState.VERIFIED:
                    counts[2] += connection.execute("UPDATE claims SET status='verified' WHERE subject=? AND predicate=? AND object_value=? AND status='pending'", claim_key).rowcount
            if subject_suppressed:
                counts[7] += 1
                continue
            event_claim = connection.execute("SELECT subject,predicate,object_value FROM claims WHERE claim_id=?", (claim_ids[0],)).fetchone()
            equivalent = connection.execute("""SELECT DISTINCT ec.event_id FROM event_claims ec JOIN claims c ON c.claim_id=ec.claim_id
                WHERE c.subject=? AND c.predicate=? AND c.object_value=? ORDER BY ec.event_id""", event_claim).fetchall()
            requested_event_id = reason.get("event_id")
            if requested_event_id is not None and (type(requested_event_id) is not str or not requested_event_id.strip()):
                raise ValueError("event_id in Phase 3 decision must be a non-empty string")
            event_id = requested_event_id
            promote_resolution = None
            if event_id is None and kind == "promote":
                matches = []
                for observation_id in tuple(reason.get("matched_observation_ids", ())):
                    found = connection.execute("SELECT event_id FROM observations WHERE id=? AND event_id IS NOT NULL", (observation_id,)).fetchall()
                    matches.extend(r[0] for r in found)
                unique = sorted(set(matches))
                promote_resolution = len(unique)
                if promote_resolution == 1: event_id = unique[0]
            if kind != "promote" and event_id is None and equivalent: event_id = equivalent[0][0]
            if event_id is None: event_id = stable_id("phase4-event", event_claim[0], event_claim[1], event_claim[2], length=32)
            existed = connection.execute("SELECT 1 FROM events WHERE id=?", (event_id,)).fetchone() is not None
            if explicit_semantic == "distinct_event" and existed:
                counts[7] += 1
                continue
            if not existed:
                connection.execute("INSERT INTO events(id,run_id,category,started_at,ended_at,article_count,observation_count,status) VALUES (?,?,?,?,?,?,?,?)", (event_id,run_id,source["category"],evaluated_at,None,0,0,"complete"))
                counts[3] += 1
            summary = connection.execute("SELECT exact_excerpt FROM claim_evidence WHERE claim_id=? ORDER BY evidence_id LIMIT 1", (claim_ids[0],)).fetchone()[0]
            linked_ids = tuple(r[0] for r in connection.execute("SELECT claim_id FROM event_claims WHERE event_id=? ORDER BY claim_id", (event_id,)))
            current_claim_ids = tuple(claim_ids)
            prior_claim_ids = tuple(sorted(set(linked_ids) | equivalent_ids))
            claim_ids = tuple(sorted(set(current_claim_ids) | set(prior_claim_ids)))
            correction_retraction_kind = (
                kind == "promote"
                and promote_resolution == 1
                and "CORRECTION_OR_RETRACTION" in reason.get("semantic_reasons", ())
                and _correction_retraction_kind(source)
            )
            retraction_promotion = correction_retraction_kind == "retraction"
            correction_promotion = correction_retraction_kind == "correction"
            latest = connection.execute("SELECT version FROM event_versions WHERE event_id=? ORDER BY version DESC LIMIT 1", (event_id,)).fetchone()
            version = latest[0] + 1 if latest else 1
            expected_version = reason.get("event_version")
            if expected_version is not None and (type(expected_version) is not int or expected_version <= 0):
                raise ValueError("event_version in Phase 3 decision must be a positive int")
            if expected_version is not None and expected_version != version:
                raise ValueError(f"event_version {expected_version} is stale; next durable version is {version}")
            placeholders = ",".join("?" for _ in claim_ids)
            has_contradiction = connection.execute(f"SELECT 1 FROM claim_evidence WHERE claim_id IN ({placeholders}) AND evidence_role='contradicts' LIMIT 1", claim_ids).fetchone()
            promote_unresolved = kind == "promote" and promote_resolution != 1
            state = (VerificationState.REJECTED.value if retraction_promotion else
                     VerificationState.WATCHLIST.value if (has_contradiction or promote_unresolved) else
                     "verified" if all(connection.execute("SELECT status FROM claims WHERE claim_id=?", (cid,)).fetchone()[0] == "verified" for cid in claim_ids) else "unverified")
            reason_text = ("retraction" if retraction_promotion else
                           "correction" if correction_promotion else
                           "contradictory_evidence" if has_contradiction else
                           "promote_unresolved_observation_event" if promote_resolution == 0 else
                           "promote_ambiguous_observation_events" if promote_unresolved else
                           "promote" if kind == "promote" else "distinct")
            latest_version = connection.execute("SELECT material_change_reason,summary,verification_state,valid_from FROM event_versions WHERE event_id=? ORDER BY version DESC LIMIT 1", (event_id,)).fetchone()
            if latest_version != (reason_text, summary, state, evaluated_at):
                connection.execute("INSERT INTO event_versions(event_id,version,material_change_reason,summary,verification_state,valid_from,superseded_at,verified_at) VALUES (?,?,?,?,?,?,?,?)", (event_id,version,reason_text,summary,state,evaluated_at,None,evaluated_at if state == "verified" else None))
                counts[4] += 1
                if version > 1:
                    connection.execute("UPDATE event_versions SET superseded_at=? WHERE event_id=? AND version=? AND superseded_at IS NULL", (evaluated_at,event_id,version-1))
            elif latest:
                version = latest[0]
            for claim_id in claim_ids:
                connection.execute("INSERT OR IGNORE INTO event_claims(event_id,event_version,claim_id) VALUES (?,?,?)", (event_id,version,claim_id)); counts[5] += connection.execute("SELECT changes()").fetchone()[0]

            evidence_by_claim = {row[0]: row[1] for row in connection.execute(
                "SELECT claim_id,evidence_id FROM claim_evidence WHERE claim_id IN (%s) AND evidence_role='supports' ORDER BY evidence_id" % ",".join("?" for _ in current_claim_ids),
                current_claim_ids,
            ).fetchall()}
            for claim in rows.claims:
                if claim.predicate != "has_date":
                    continue
                evidence_id = evidence_by_claim.get(claim.claim_id)
                if evidence_id is None:
                    raise sqlite3.IntegrityError("date claim is missing supporting evidence")
                event_date_id = stable_id("event-date", event_id, str(version), claim.claim_id, claim.object_value, evidence_id, length=64)
                connection.execute("INSERT OR IGNORE INTO event_dates(event_date_id,event_id,event_version,date_type,date_value,date_precision,evidence_id,unknown_reason) VALUES (?,?,?,?,?,?,?,?)", (event_date_id, event_id, version, "occurred_at", claim.object_value, "day", evidence_id, None))
                counts[8] += connection.execute("SELECT changes()").fetchone()[0]

            if retraction_promotion:
                for claim_id in current_claim_ids:
                    counts[2] += connection.execute("UPDATE claims SET status='rejected' WHERE claim_id=? AND status!='rejected'", (claim_id,)).rowcount
                for claim_id in prior_claim_ids:
                    if claim_id not in current_claim_ids:
                        counts[2] += connection.execute("UPDATE claims SET status='superseded' WHERE claim_id=? AND status!='superseded'", (claim_id,)).rowcount

            pending = [c for c in rows.claims if connection.execute("SELECT status FROM claims WHERE claim_id=?", (c.claim_id,)).fetchone()[0] == "pending"]
            if pending and not retraction_promotion:
                source_row = connection.execute("SELECT source_id,category FROM source_items WHERE source_item_id=?", (source_item_id,)).fetchone()
                eligible = None
                for candidate_source, scope_json in connection.execute("SELECT source_id,category_scope_json FROM source_registry WHERE enabled=1 AND adapter_type='searxng' ORDER BY source_id"):
                    try: scope = json.loads(scope_json)
                    except (TypeError, json.JSONDecodeError): continue
                    if isinstance(scope, list) and source_row[1] in scope:
                        eligible = candidate_source; break
                if eligible is not None and plans_remaining:
                    plans = plan_corroboration_queries(pending, source_id=eligible, category=source_row[1], created_at=evaluated_at, max_items=plans_remaining)
                    plans_remaining -= len(plans)
                    for plan in plans:
                        connection.execute("INSERT OR IGNORE INTO query_plans(query_plan_id,source_id,query_text,category,topic,entity,reason_selected,cooldown_seconds,max_rounds,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)", (plan.query_plan_id,plan.source_id,plan.query_text,plan.category,plan.topic,plan.entity,plan.reason_selected,plan.cooldown_seconds,plan.max_rounds,plan.created_at)); counts[6] += connection.execute("SELECT changes()").fetchone()[0]
            counts[7] += 1
        if selected:
            notes = json.dumps({"claims_inserted": counts[0], "evidence_inserted": counts[1], "events_created": counts[3], "event_links_inserted": counts[5], "event_dates_inserted": counts[8], "processed": counts[7], "query_plans_inserted": counts[6], "status_updates": counts[2], "versions_appended": counts[4]}, sort_keys=True, separators=(",", ":"))
            connection.execute("UPDATE runs SET finished_at=?,notes=? WHERE id=?", (evaluated_at, notes, run_id))
        violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        if violations: raise sqlite3.IntegrityError(f"foreign-key violations: {violations[:3]}")
    except Exception:
        connection.rollback(); raise
    else: connection.commit()
    finally: connection.close()
    return Phase4Report(len(selected),counts[7],counts[0],counts[1],counts[2],counts[3],counts[4],counts[5],counts[6],counts[8])


run_phase4 = process_phase4
