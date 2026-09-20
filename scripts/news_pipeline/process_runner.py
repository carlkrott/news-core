"""Delivery-free Phase 3 processing over persisted Phase 2 source items."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Mapping

from .contracts import CandidateArticle, FilterResult, HistoryMatch
from .event_contracts import AdjudicationResult, EventCandidate, FactDelta, ModelRequest, SemanticDecision
from .filtering import evaluate_candidates
from .history import HistoryUnavailable, fetch_history_match, open_history
from .live_contracts import QueryPlanContract, stable_id
from .models import Category
from .novelty import AttemptYield, CategoryYield, NoveltyRecord, summarize_yield
from .phase3_api import evaluate_semantic_updates
from .policies import DEFAULT_POLICIES, QueryPolicy, SourcePolicy
from .query_planner import build_expansion_queries

MAX_ITEMS_DEFAULT = 500
_REQUIRED_TABLES = frozenset(
    {
        "runs",
        "decisions",
        "query_telemetry",
        "source_registry",
        "source_items",
        "query_plans",
        "query_attempts",
        "fetch_state",
    }
)
_TERMINAL_ATTEMPT_STATES = ("success", "partial", "failed", "rate_limited")


@dataclass(frozen=True, slots=True)
class ProcessReport:
    run_id: str
    evaluated_at: str
    source_items_seen: int
    source_items_pending: int
    source_items_processed: int
    decisions_persisted: int
    query_telemetry_persisted: int
    expansion_plans_persisted: int
    category_yields: tuple[CategoryYield, ...] = field(default_factory=tuple)
    expansion_plans: tuple[QueryPlanContract, ...] = field(default_factory=tuple)

    @property
    def eligible_count(self) -> int:
        return sum(summary.novel_count for summary in self.category_yields)


@dataclass(frozen=True, slots=True)
class _AttemptRow:
    attempt_id: str
    source_id: str
    category: str
    query_text: str
    returned_count: int
    duplicate_count: int
    error_count: int
    status: str
    started_at: str
    finished_at: str | None

    @property
    def telemetry_id(self) -> str:
        return stable_id("phase3-query-telemetry", self.attempt_id, length=64)

    @property
    def yield_value(self) -> AttemptYield:
        return AttemptYield(
            category=self.category,
            status=self.status,
            returned_count=self.returned_count,
            duplicate_count=self.duplicate_count,
        )


def _parse_time(value: str, *, name: str = "timestamp") -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise ValueError(f"{name} must be a UTC ISO-8601 timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{name} must be a valid UTC ISO-8601 timestamp") from exc
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{name} must be UTC")
    return parsed.astimezone(UTC)


def _optional_valid_time(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        _parse_time(value)
    except ValueError:
        return None
    return value


def _text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def candidate_from_source_item(row: Mapping[str, object], evaluated_at: str) -> CandidateArticle:
    """Adapt one persisted source item and fail closed on invalid date evidence."""
    _parse_time(evaluated_at, name="evaluated_at")
    source_item_id = _text(row.get("source_item_id"))
    category_text = _text(row.get("category"))
    if not source_item_id or not category_text:
        raise ValueError("source item lacks source_item_id or category")

    raw_published = row.get("published_at")
    published_at = _optional_valid_time(raw_published)
    evidence_raw = row.get("publication_evidence")
    if published_at is not None and evidence_raw == "source":
        evidence = "source"
    elif published_at is not None and isinstance(evidence_raw, str) and (
        evidence_raw == "metadata" or evidence_raw.startswith("metadata:")
    ):
        evidence = "metadata"
    elif raw_published in (None, "") and evidence_raw in (None, "", "missing"):
        evidence = "missing"
        published_at = None
    elif evidence_raw == "unparseable":
        evidence = "unparseable"
        published_at = None
    else:
        evidence = "unparseable"
        published_at = None

    original_url = _text(row.get("original_url")) or None
    canonical_url = _text(row.get("canonical_url")) or None
    observed_at = _optional_valid_time(row.get("retrieved_at"))
    return CandidateArticle(
        candidate_id=source_item_id,
        category=Category(category_text),
        query_group=category_text,
        title=_text(row.get("title")),
        snippet=_text(row.get("body")),
        original_url=original_url,
        canonical_url=canonical_url,
        published_at=published_at,
        published_evidence=evidence,
        observed_at=observed_at,
        evaluated_at=evaluated_at,
    )


def _decision_id(source_item_id: str) -> str:
    return stable_id("phase3-decision", source_item_id, length=64)


def _run_id(evaluated_at: str) -> str:
    return stable_id("phase3-process-run", evaluated_at, length=64)


def _forbidden_model(request: ModelRequest) -> str:
    del request
    raise AssertionError("Phase 3 model invocation is forbidden")


def _decision_kind(semantic_decision: SemanticDecision) -> str:
    if semantic_decision is SemanticDecision.distinct_event:
        return "keep"
    if semantic_decision is SemanticDecision.material_update:
        return "promote"
    if semantic_decision in {SemanticDecision.rewrite, SemanticDecision.bypass_phase2_terminal}:
        return "suppress"
    return "manual_review"


def _fact_delta_json(delta: FactDelta) -> dict[str, str]:
    return {
        "kind": delta.kind.value,
        "unit": delta.unit,
        "old_value": delta.old_value,
        "new_value": delta.new_value,
        "topic_gate": str(delta.topic_gate),
    }


def _reason_json(
    source_item_id: str,
    filter_result: FilterResult,
    semantic_result: AdjudicationResult,
) -> str:
    return json.dumps(
        {
            "matched_article_ids": list(semantic_result.matched_history_ids),
            "matched_observation_ids": list(semantic_result.matched_observation_ids),
            "phase2_decision": filter_result.decision.value,
            "phase2_reasons": [reason.value for reason in filter_result.reasons],
            "date_evidence": filter_result.date_evidence,
            "recency_status": filter_result.recency_status,
            "audit_only": filter_result.audit_only,
            "semantic_decision": semantic_result.semantic_decision.value,
            "semantic_reasons": [reason.value for reason in semantic_result.semantic_reasons],
            "event_id": semantic_result.event_id,
            "event_version": semantic_result.event_version,
            "subject_id": semantic_result.subject_id,
            "fact_deltas": [_fact_delta_json(delta) for delta in semantic_result.fact_deltas],
            "subject_suppressed": semantic_result.subject_suppressed,
            "source_item_id": source_item_id,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, isolation_level=None, timeout=10.0)
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=10000")
    return connection


def _require_v4(connection: sqlite3.Connection) -> None:
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    missing = _REQUIRED_TABLES - tables
    if missing:
        raise ValueError(f"Phase 3 requires schema v4; missing tables: {sorted(missing)}")
    marker = connection.execute("SELECT 1 FROM schema_migrations WHERE version=4").fetchone()
    if marker is None:
        raise ValueError("Phase 3 requires the schema v4 migration marker")


def _pending_rows(
    connection: sqlite3.Connection, max_items: int
) -> tuple[int, int, list[dict[str, object]]]:
    columns = tuple(row[1] for row in connection.execute("PRAGMA table_info(source_items)"))
    item_ids = [
        row[0]
        for row in connection.execute(
            "SELECT source_item_id FROM source_items ORDER BY retrieved_at,source_item_id"
        )
    ]
    decision_ids = {row[0] for row in connection.execute("SELECT id FROM decisions")}
    pending_ids = [item_id for item_id in item_ids if _decision_id(item_id) not in decision_ids]
    rows: list[dict[str, object]] = []
    for item_id in pending_ids[:max_items]:
        row = connection.execute(
            "SELECT * FROM source_items WHERE source_item_id=?", (item_id,)
        ).fetchone()
        if row is not None:
            rows.append(dict(zip(columns, row)))
    return len(item_ids), len(pending_ids), rows


def _history_rows(
    history_path: Path, filter_results: tuple[FilterResult, ...]
) -> tuple[HistoryMatch, ...]:
    pairs = {
        pair
        for result in filter_results
        for pair in zip(result.matched_article_ids, result.matched_observation_ids)
    }
    if not pairs:
        return ()
    try:
        history = open_history(str(history_path))
    except HistoryUnavailable:
        return ()
    try:
        output: list[HistoryMatch] = []
        for article_id, observation_id in sorted(pairs):
            match = fetch_history_match(history, article_id, observation_id)
            if match is not None:
                output.append(match)
        return tuple(output)
    finally:
        history.close()


def _unmirrored_attempts(connection: sqlite3.Connection) -> tuple[_AttemptRow, ...]:
    existing = {row[0] for row in connection.execute("SELECT id FROM query_telemetry")}
    rows = connection.execute(
        """SELECT qa.attempt_id,qp.source_id,qp.category,qp.query_text,
                  qa.returned_count,qa.duplicate_count,qa.error_count,qa.status,
                  qa.started_at,qa.finished_at
           FROM query_attempts qa
           JOIN query_plans qp ON qp.query_plan_id=qa.query_plan_id
           WHERE qa.status IN ('success','partial','failed','rate_limited')
           ORDER BY qa.started_at,qa.attempt_id"""
    ).fetchall()
    output = tuple(_AttemptRow(*row) for row in rows)
    return tuple(row for row in output if row.telemetry_id not in existing)


def _searxng_source(
    connection: sqlite3.Connection, category: str
) -> tuple[str, int] | None:
    rows = connection.execute(
        """SELECT source_id,category_scope_json,cadence_minutes
           FROM source_registry
           WHERE enabled=1 AND adapter_type='searxng'
           ORDER BY source_id"""
    ).fetchall()
    for source_id, category_scope_json, cadence_minutes in rows:
        try:
            categories = json.loads(category_scope_json)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(categories, list) and category in categories:
            cooldown = int(cadence_minutes) * 60 if cadence_minutes is not None else 3600
            return source_id, cooldown
    return None


def _cooldown_allows(
    connection: sqlite3.Connection, plan: QueryPlanContract, evaluated_at: str
) -> bool:
    existing = connection.execute(
        "SELECT created_at FROM query_plans WHERE query_plan_id=?", (plan.query_plan_id,)
    ).fetchone()
    if existing is None:
        return True
    latest_attempt = connection.execute(
        "SELECT MAX(started_at) FROM query_attempts WHERE query_plan_id=?", (plan.query_plan_id,)
    ).fetchone()[0]
    if latest_attempt is None:
        return False
    cutoff = _parse_time(evaluated_at) - timedelta(seconds=plan.cooldown_seconds)
    try:
        latest = _parse_time(latest_attempt, name="query attempt started_at")
    except ValueError:
        return False
    return latest <= cutoff


def _plan_expansions(
    connection: sqlite3.Connection,
    summaries: tuple[CategoryYield, ...],
    records: tuple[NoveltyRecord, ...],
    evaluated_at: str,
    minimum_novelty_target: int,
) -> tuple[QueryPlanContract, ...]:
    titles_by_category: dict[str, list[str]] = {}
    for record in records:
        if record.title:
            titles_by_category.setdefault(record.category, []).append(record.title)
    output: list[QueryPlanContract] = []
    for summary in summaries:
        if not summary.needs_expansion(minimum_novelty_target):
            continue
        source = _searxng_source(connection, summary.category)
        if source is None:
            continue
        source_id, cooldown_seconds = source
        plans = build_expansion_queries(
            source_id=source_id,
            category=summary.category,
            evaluated_at=evaluated_at,
            titles=tuple(titles_by_category.get(summary.category, ())),
            category_label=summary.category,
            cooldown_seconds=cooldown_seconds,
        )
        output.extend(
            plan for plan in plans if _cooldown_allows(connection, plan, evaluated_at)
        )
    return tuple(output)


def process_news(
    db_path: str | Path,
    evaluated_at: str,
    *,
    history_db_path: str | Path | None = None,
    source_policy: SourcePolicy | None = None,
    query_policies: Mapping[Category, QueryPolicy] | None = None,
    max_items: int = MAX_ITEMS_DEFAULT,
    minimum_novelty_target: int = 1,
) -> ProcessReport:
    """Classify pending source items and atomically persist Phase 3 state."""
    _parse_time(evaluated_at, name="evaluated_at")
    if type(max_items) is not int or not 1 <= max_items <= MAX_ITEMS_DEFAULT:
        raise ValueError("max_items must be an integer from 1 to 500")
    if type(minimum_novelty_target) is not int or minimum_novelty_target < 0:
        raise ValueError("minimum_novelty_target must be a non-negative int")
    path = Path(db_path)
    if not path.is_file():
        raise FileNotFoundError(path)
    history_path = Path(history_db_path) if history_db_path is not None else path
    policies: dict[Category, QueryPolicy] = dict(
        DEFAULT_POLICIES if query_policies is None else query_policies
    )
    connection = _connect(path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        _require_v4(connection)
        seen_count, pending_count, pending_rows = _pending_rows(connection, max_items)
        candidates = [candidate_from_source_item(row, evaluated_at) for row in pending_rows]
        filter_results = tuple(
            evaluate_candidates(
                candidates,
                str(history_path),
                source_policy or SourcePolicy(),
                policies,
            )
        ) if candidates else ()
        events = tuple(
            EventCandidate(
                candidate=candidate,
                filter_result=result,
                query_policy=policies[candidate.category],
            )
            for candidate, result in zip(candidates, filter_results)
        )
        semantic_results = evaluate_semantic_updates(
            events,
            _history_rows(history_path, filter_results),
            _forbidden_model,
            0,
        )
        records = tuple(
            NoveltyRecord(
                category=candidate.category.value,
                source_item_id=candidate.candidate_id,
                decision=filter_result.decision.value,
                semantic_decision=semantic_result.semantic_decision.value,
                source_id=_text(row.get("source_id")) or None,
                title=candidate.title,
            )
            for row, candidate, filter_result, semantic_result in zip(
                pending_rows, candidates, filter_results, semantic_results
            )
        )
        attempts = _unmirrored_attempts(connection)
        summaries = summarize_yield(
            records, tuple(attempt.yield_value for attempt in attempts)
        )
        expansion_plans = _plan_expansions(
            connection,
            summaries,
            records,
            evaluated_at,
            minimum_novelty_target,
        )

        process_run_id = _run_id(evaluated_at)
        connection.execute(
            """INSERT OR IGNORE INTO runs(
                   id,started_at,kind,provenance,source_dir,notes)
               VALUES(?,?,?,?,?,?)""",
            (
                process_run_id,
                evaluated_at,
                "live_ingest",
                "observed_live",
                None,
                "phase3 deterministic processing",
            ),
        )
        decisions_persisted = 0
        for row, filter_result, semantic_result in zip(
            pending_rows, filter_results, semantic_results
        ):
            cursor = connection.execute(
                """INSERT OR IGNORE INTO decisions(
                       id,run_id,article_id,decision_kind,reason,decided_at,decided_by)
                   VALUES(?,?,?,?,?,?,?)""",
                (
                    _decision_id(_text(row["source_item_id"])),
                    process_run_id,
                    None,
                    _decision_kind(semantic_result.semantic_decision),
                    _reason_json(_text(row["source_item_id"]), filter_result, semantic_result),
                    evaluated_at,
                    "phase3",
                ),
            )
            decisions_persisted += cursor.rowcount

        telemetry_persisted = 0
        for attempt in attempts:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO query_telemetry(
                       id,run_id,query_text,source,returned_count,error_count,
                       started_at,finished_at)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (
                    attempt.telemetry_id,
                    process_run_id,
                    attempt.query_text,
                    attempt.source_id,
                    attempt.returned_count,
                    attempt.error_count,
                    attempt.started_at,
                    attempt.finished_at,
                ),
            )
            telemetry_persisted += cursor.rowcount

        plans_persisted = 0
        for plan in expansion_plans:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO query_plans(
                       query_plan_id,source_id,query_text,category,topic,entity,
                       reason_selected,cooldown_seconds,max_rounds,created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (
                    plan.query_plan_id,
                    plan.source_id,
                    plan.query_text,
                    plan.category,
                    plan.topic,
                    plan.entity,
                    plan.reason_selected,
                    plan.cooldown_seconds,
                    plan.max_rounds,
                    plan.created_at,
                ),
            )
            plans_persisted += cursor.rowcount

        notes = json.dumps(
            {
                "decisions_persisted": decisions_persisted,
                "expansion_plans_persisted": plans_persisted,
                "phase": 3,
                "query_telemetry_persisted": telemetry_persisted,
                "source_items_processed": len(pending_rows),
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        connection.execute(
            "UPDATE runs SET finished_at=?,notes=? WHERE id=?",
            (evaluated_at, notes, process_run_id),
        )
        violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise ValueError(f"Phase 3 introduced foreign-key violations: {violations[:3]}")
        connection.commit()
        return ProcessReport(
            run_id=process_run_id,
            evaluated_at=evaluated_at,
            source_items_seen=seen_count,
            source_items_pending=pending_count,
            source_items_processed=len(pending_rows),
            decisions_persisted=decisions_persisted,
            query_telemetry_persisted=telemetry_persisted,
            expansion_plans_persisted=plans_persisted,
            category_yields=summaries,
            expansion_plans=expansion_plans,
        )
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def run_process(
    db_path: str | Path,
    evaluated_at: str,
    *,
    history_db_path: str | Path | None = None,
    source_policy: SourcePolicy | None = None,
    query_policies: Mapping[Category, QueryPolicy] | None = None,
    max_items: int = MAX_ITEMS_DEFAULT,
    minimum_novelty_target: int = 1,
) -> ProcessReport:
    return process_news(
        db_path,
        evaluated_at,
        history_db_path=history_db_path,
        source_policy=source_policy,
        query_policies=query_policies,
        max_items=max_items,
        minimum_novelty_target=minimum_novelty_target,
    )
