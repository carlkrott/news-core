"""Bounded one-candidate investigation contracts and persistence."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit

from .adapters.base import Transport
from .adapters.searxng import SearxngAdapter
from .live_contracts import (
    CATEGORY_VALUES,
    QueryPlanContract,
    QuerySeed,
    SourceAdapter,
    SourceContract,
    source_from_row,
    stable_id,
)
from .query_planner import build_investigation_queries

MAX_INVESTIGATION_ROUNDS = 2
INVESTIGATION_STATES = ("pending", "running", "complete", "failed", "blocked")
TERMINAL_STATES = frozenset({"complete", "failed", "blocked"})


@dataclass(frozen=True, slots=True)
class InvestigationJob:
    investigation_id: str
    candidate_id: str
    feed_lane_id: str
    query_plan_id: str
    category: str
    round_number: int
    state: str
    terminal_state: str | None
    targeted_query_ids: tuple[str, ...]
    created_at: str
    updated_at: str

    def __post_init__(self) -> None:
        for name in (
            "investigation_id", "candidate_id", "feed_lane_id", "query_plan_id", "category",
            "created_at", "updated_at",
        ):
            value = getattr(self, name)
            if type(value) is not str or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if self.category not in CATEGORY_VALUES:
            raise ValueError(f"category must be one of {CATEGORY_VALUES}")
        if type(self.round_number) is not int or type(self.round_number) is bool or not 0 <= self.round_number < MAX_INVESTIGATION_ROUNDS:
            raise ValueError("round_number must be 0 or 1")
        if self.state not in INVESTIGATION_STATES:
            raise ValueError(f"state must be one of {INVESTIGATION_STATES}")
        if self.state in TERMINAL_STATES and self.terminal_state != self.state:
            raise ValueError("terminal state must match a terminal investigation state")
        if self.state not in TERMINAL_STATES and self.terminal_state is not None:
            raise ValueError("non-terminal investigations cannot have terminal_state")
        if type(self.targeted_query_ids) is not tuple or any(type(value) is not str or not value.strip() for value in self.targeted_query_ids):
            raise ValueError("targeted_query_ids must be a tuple of non-empty strings")
        if len(set(self.targeted_query_ids)) != len(self.targeted_query_ids):
            raise ValueError("targeted_query_ids must not contain duplicates")


@dataclass(frozen=True, slots=True)
class InvestigationResult:
    job: InvestigationJob
    targeted_queries: tuple[QueryPlanContract, ...]
    query_results: tuple["InvestigationQueryResult", ...] = ()
    network_used: bool = False


@dataclass(frozen=True, slots=True)
class InvestigationQueryResult:
    """Bounded metadata returned by one broker-backed investigation query."""

    query_plan_id: str
    source_id: str
    status: str
    http_status: int | None
    returned_count: int
    item_ids: tuple[str, ...] = ()
    error_class: str | None = None

    def __post_init__(self) -> None:
        if self.status not in {"success", "failed"}:
            raise ValueError("investigation query status must be success or failed")
        if type(self.returned_count) is not int or self.returned_count < 0:
            raise ValueError("investigation returned_count must be a non-negative integer")
        if type(self.item_ids) is not tuple or any(type(item) is not str or not item for item in self.item_ids):
            raise ValueError("investigation item_ids must be a tuple of non-empty strings")
        if len(set(self.item_ids)) != len(self.item_ids):
            raise ValueError("investigation item_ids must not contain duplicates")
        if self.status == "success" and self.error_class is not None:
            raise ValueError("successful investigation query cannot have an error class")
        if self.status == "failed" and not self.error_class:
            raise ValueError("failed investigation query requires an error class")


def investigation_id(candidate_id: str, feed_lane_id: str, query_plan_id: str) -> str:
    """Return the retry-stable identity for one candidate investigation."""
    return stable_id("investigation", candidate_id, feed_lane_id, query_plan_id, length=64)


def make_investigation_job(
    *,
    candidate_id: str,
    feed_lane_id: str,
    query_plan_id: str,
    category: str,
    evaluated_at: str,
    round_number: int = 0,
    state: str = "pending",
    terminal_state: str | None = None,
    targeted_query_ids: tuple[str, ...] = (),
) -> InvestigationJob:
    return InvestigationJob(
        investigation_id=investigation_id(candidate_id, feed_lane_id, query_plan_id),
        candidate_id=candidate_id,
        feed_lane_id=feed_lane_id,
        query_plan_id=query_plan_id,
        category=category,
        round_number=round_number,
        state=state,
        terminal_state=terminal_state,
        targeted_query_ids=targeted_query_ids,
        created_at=evaluated_at,
        updated_at=evaluated_at,
    )


def investigation_payload(job: InvestigationJob) -> dict[str, object]:
    return {
        "investigation_id": job.investigation_id,
        "candidate_id": job.candidate_id,
        "feed_lane_id": job.feed_lane_id,
        "query_plan_id": job.query_plan_id,
        "category": job.category,
        "round_number": job.round_number,
    }


def _job_from_row(row: sqlite3.Row | tuple[object, ...]) -> InvestigationJob:
    try:
        targeted = tuple(json.loads(str(row[8])))
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("stored investigation query identity is invalid") from exc
    return InvestigationJob(
        investigation_id=str(row[0]),
        candidate_id=str(row[1]),
        feed_lane_id=str(row[2]),
        query_plan_id=str(row[3]),
        category=str(row[4]),
        round_number=int(str(row[5])),
        state=str(row[6]),
        terminal_state=None if row[7] is None else str(row[7]),
        targeted_query_ids=targeted,
        created_at=str(row[9]),
        updated_at=str(row[10]),
    )


def enqueue_candidate_investigations(
    state_db_path: str | Path,
    control_connection: sqlite3.Connection,
    *,
    due_slot_utc: str,
) -> tuple[tuple[str, bool], ...]:
    """Enqueue one deterministic investigation task for each new candidate.

    The state DB is read-only here.  If one source item appeared in multiple
    same-category lanes, the lexicographically first lane owns the single
    investigation identity; cross-category reuse has already been rejected by
    ingest.  Existing investigation rows are never scheduled again implicitly.
    """
    path = Path(state_db_path).resolve()
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=5.0)
    try:
        rows = connection.execute(
            """SELECT c.source_item_id,c.feed_lane_id,c.query_plan_id,c.category
               FROM candidate_feed_lanes c
               WHERE NOT EXISTS (
                   SELECT 1 FROM investigations i
                   WHERE i.candidate_id=c.source_item_id
               )
                 AND NOT EXISTS (
                   SELECT 1 FROM candidate_feed_lanes prior
                   WHERE prior.source_item_id=c.source_item_id
                     AND (prior.feed_lane_id < c.feed_lane_id
                          OR (prior.feed_lane_id=c.feed_lane_id
                              AND prior.query_plan_id<c.query_plan_id))
               )
               ORDER BY c.source_item_id,c.feed_lane_id,c.query_plan_id"""
        ).fetchall()
    finally:
        connection.close()

    from news_container.control_store import enqueue

    results: list[tuple[str, bool]] = []
    for candidate_id, feed_lane_id, query_plan_id, category in rows:
        job = make_investigation_job(
            candidate_id=str(candidate_id),
            feed_lane_id=str(feed_lane_id),
            query_plan_id=str(query_plan_id),
            category=str(category),
            evaluated_at=due_slot_utc,
        )
        results.append(
            enqueue(
                control_connection,
                kind="investigate",
                due_slot_utc=due_slot_utc,
                payload=investigation_payload(job),
            )
        )
    return tuple(results)


def validate_investigation_payload(payload: dict[str, object]) -> dict[str, object]:
    """Validate a queue payload and reject any multi-candidate shape."""
    if not isinstance(payload, dict):
        raise ValueError("investigate payload must be an object")
    required = (
        "investigation_id", "candidate_id", "feed_lane_id", "query_plan_id", "category", "round_number"
    )
    unknown = sorted(set(payload) - set(required))
    if unknown:
        raise ValueError(f"investigate payload contains unknown fields: {unknown}")
    missing = [key for key in required if key not in payload]
    if missing:
        raise ValueError(f"investigate payload is missing required fields: {missing}")
    for key in required[:-1]:
        if type(payload[key]) is not str or not str(payload[key]).strip():
            raise ValueError(f"investigate payload field {key!r} must be a non-empty string")
    if payload["category"] not in CATEGORY_VALUES:
        raise ValueError(f"investigate payload category must be one of {CATEGORY_VALUES}")
    round_number = payload["round_number"]
    if type(round_number) is not int or type(round_number) is bool or not 0 <= round_number < MAX_INVESTIGATION_ROUNDS:
        raise ValueError("investigate payload round_number must be 0 or 1")
    expected = investigation_id(
        str(payload["candidate_id"]),
        str(payload["feed_lane_id"]),
        str(payload["query_plan_id"]),
    )
    if payload["investigation_id"] != expected:
        raise ValueError("investigate payload investigation_id is not deterministic for its candidate")
    return dict(payload)


def persist_investigation(
    connection: sqlite3.Connection,
    job: InvestigationJob,
    *,
    fence_check: Callable[[], None] | None = None,
) -> bool:
    """Persist one investigation under an explicit monotonic state policy.

    Terminal rows are immutable on replay.  A failed/blocked row can only
    advance to the next bounded round through an explicit pending/running
    write.  ``fence_check`` is called before the transaction and immediately
    before commit so a worker that lost its control-store lease cannot publish
    a stale terminal receipt.
    """
    job.__post_init__()
    if fence_check is not None:
        fence_check()
    lane = connection.execute(
        """SELECT category FROM candidate_feed_lanes
           WHERE source_item_id=? AND feed_lane_id=? AND query_plan_id=?""",
        (job.candidate_id, job.feed_lane_id, job.query_plan_id),
    ).fetchone()
    if lane is None:
        raise ValueError("candidate is not linked to the requested feed lane and query plan")
    if lane[0] != job.category:
        raise ValueError("candidate feed lane category does not match investigation category")
    targeted_json = json.dumps(job.targeted_query_ids, ensure_ascii=False, separators=(",", ":"))
    connection.execute("BEGIN IMMEDIATE")
    try:
        existing = connection.execute(
            """SELECT investigation_id,feed_lane_id,query_plan_id,category,round_number,
                      state,terminal_state,targeted_query_ids_json
               FROM investigations WHERE candidate_id=?""",
            (job.candidate_id,),
        ).fetchone()
        if existing is not None:
            if tuple(existing[:4]) != (job.investigation_id, job.feed_lane_id, job.query_plan_id, job.category):
                raise ValueError("candidate already belongs to a different investigation identity")
            existing_round = int(existing[4])
            existing_state = str(existing[5])
            if job.round_number < existing_round:
                raise ValueError("investigation round cannot move backwards")
            try:
                existing_targeted = tuple(json.loads(existing[7]))
            except (TypeError, json.JSONDecodeError) as exc:
                raise ValueError("stored investigation query identity is invalid") from exc
            if job.round_number == existing_round:
                if existing_targeted != job.targeted_query_ids:
                    raise ValueError("investigation query identity changed within a round")
                if existing_state in TERMINAL_STATES:
                    if job.state != existing_state or job.terminal_state != existing[6]:
                        raise ValueError("terminal investigation state cannot regress or change")
                    connection.commit()
                    return False
                allowed = {
                    "pending": frozenset({"pending", "running", *TERMINAL_STATES}),
                    "running": frozenset({"running", *TERMINAL_STATES}),
                }
                if job.state not in allowed.get(existing_state, frozenset()):
                    raise ValueError("investigation state cannot move backwards")
            elif (
                job.round_number != existing_round + 1
                or existing_state not in {"failed", "blocked"}
                or job.state not in {"pending", "running"}
            ):
                raise ValueError("only failed or blocked investigations may advance one round")
        if fence_check is not None:
            fence_check()
        connection.execute(
            """INSERT INTO investigations(
                   investigation_id,candidate_id,feed_lane_id,query_plan_id,category,
                   round_number,state,terminal_state,targeted_query_ids_json,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(candidate_id) DO UPDATE SET
                   round_number=excluded.round_number,state=excluded.state,
                   terminal_state=excluded.terminal_state,
                   targeted_query_ids_json=excluded.targeted_query_ids_json,
                   updated_at=excluded.updated_at""",
            (
                job.investigation_id, job.candidate_id, job.feed_lane_id, job.query_plan_id,
                job.category, job.round_number, job.state, job.terminal_state,
                targeted_json, job.created_at, job.updated_at,
            ),
        )
        if fence_check is not None:
            fence_check()
        connection.commit()
        return True
    except Exception:
        connection.rollback()
        raise


def _search_source(
    connection: sqlite3.Connection,
    *,
    preferred_source_id: str,
    category: str,
) -> SourceContract:
    """Resolve one enabled SearXNG source for the candidate's category."""
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        "SELECT * FROM source_registry WHERE enabled=1 ORDER BY source_id"
    ).fetchall()
    ordered = sorted(rows, key=lambda row: (row["source_id"] != preferred_source_id, row["source_id"]))
    for row in ordered:
        source = source_from_row(dict(row))
        if source.adapter_type is SourceAdapter.SEARXNG and category in source.category_scope:
            return source
    raise ValueError(f"no enabled SearXNG investigation source for category {category!r}")


def _search_categories(source: SourceContract, category: str) -> tuple[str, ...]:
    for query in source.queries:
        if query.pipeline_category == category:
            return query.categories
    return ("news",)


async def _fetch_targeted_queries(
    source: SourceContract,
    *,
    category: str,
    targeted: tuple[QueryPlanContract, ...],
    evaluated_at: str,
    transport_factory: Callable[[SourceContract], Transport | None],
) -> tuple[InvestigationQueryResult, ...]:
    transport = transport_factory(source)
    if transport is None:
        raise ValueError("investigation transport factory returned no broker transport")
    adapter = SearxngAdapter(source, category=category, transport=transport)
    categories = _search_categories(source, category)
    results: list[InvestigationQueryResult] = []
    for plan in targeted:
        seed = QuerySeed(
            text=plan.query_text,
            categories=categories,
            pipeline_category=category,
            feed_lane_id=plan.feed_lane_id,
        )
        try:
            fetched = await adapter.fetch_query(seed, retrieved_at=evaluated_at)
        except Exception as exc:
            results.append(
                InvestigationQueryResult(
                    query_plan_id=plan.query_plan_id,
                    source_id=source.source_id,
                    status="failed",
                    http_status=None,
                    returned_count=0,
                    error_class=type(exc).__name__,
                )
            )
            continue
        if fetched.error is not None:
            results.append(
                InvestigationQueryResult(
                    query_plan_id=plan.query_plan_id,
                    source_id=source.source_id,
                    status="failed",
                    http_status=fetched.http_status,
                    returned_count=0,
                    error_class=type(fetched.error).__name__,
                )
            )
            continue
        results.append(
            InvestigationQueryResult(
                query_plan_id=plan.query_plan_id,
                source_id=source.source_id,
                status="success",
                http_status=fetched.http_status,
                returned_count=len(fetched.items),
                item_ids=tuple(item.source_item_id for item in fetched.items),
            )
        )
    return tuple(results)


def run_investigation(
    db_path: str,
    *,
    candidate_id: str,
    feed_lane_id: str,
    query_plan_id: str,
    category: str,
    evaluated_at: str,
    round_number: int = 0,
    transport_factory: Callable[[SourceContract], Transport | None] | None = None,
    lease_check: Callable[[], None] | None = None,
) -> InvestigationResult:
    """Run one bounded, broker-backed investigation for one candidate."""
    payload = validate_investigation_payload({
        "investigation_id": investigation_id(candidate_id, feed_lane_id, query_plan_id),
        "candidate_id": candidate_id,
        "feed_lane_id": feed_lane_id,
        "query_plan_id": query_plan_id,
        "category": category,
        "round_number": round_number,
    })
    connection = sqlite3.connect(db_path, isolation_level=None)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        row = connection.execute(
            """SELECT si.title,si.canonical_url,qp.source_id
               FROM source_items si
               JOIN candidate_feed_lanes cfl ON cfl.source_item_id=si.source_item_id
               JOIN query_plans qp ON qp.query_plan_id=cfl.query_plan_id
               WHERE si.source_item_id=? AND cfl.feed_lane_id=? AND cfl.query_plan_id=? AND cfl.category=?""",
            (candidate_id, feed_lane_id, query_plan_id, category),
        ).fetchone()
        if row is None:
            raise ValueError("candidate is not available in the requested feed lane")
        title = row[0] or category.replace("_", " ")
        host = urlsplit(row[1] or "").hostname
        targeted = build_investigation_queries(
            candidate_id=candidate_id,
            feed_lane_id=feed_lane_id,
            category=category,
            title=title,
            publisher_host=host,
            evaluated_at=evaluated_at,
            round_number=round_number,
        )
        targeted_ids = tuple(plan.query_plan_id for plan in targeted)
        existing = connection.execute(
            """SELECT investigation_id,candidate_id,feed_lane_id,query_plan_id,category,
                      round_number,state,terminal_state,targeted_query_ids_json,created_at,updated_at
               FROM investigations WHERE candidate_id=?""",
            (candidate_id,),
        ).fetchone()
        if existing is not None:
            existing_job = _job_from_row(existing)
            if (
                existing_job.investigation_id != investigation_id(candidate_id, feed_lane_id, query_plan_id)
                or existing_job.feed_lane_id != feed_lane_id
                or existing_job.query_plan_id != query_plan_id
                or existing_job.category != category
            ):
                raise ValueError("candidate already belongs to a different investigation identity")
            if round_number < existing_job.round_number:
                raise ValueError("investigation round cannot move backwards")
            if existing_job.state in TERMINAL_STATES and round_number == existing_job.round_number:
                if existing_job.targeted_query_ids != targeted_ids:
                    raise ValueError("stored investigation query identity changed within a round")
                return InvestigationResult(
                    job=existing_job,
                    targeted_queries=targeted,
                    query_results=(),
                    network_used=False,
                )
            if round_number != existing_job.round_number and (
                round_number != existing_job.round_number + 1
                or existing_job.state not in {"failed", "blocked"}
            ):
                raise ValueError("only failed or blocked investigations may advance one round")
        running = make_investigation_job(
            candidate_id=candidate_id,
            feed_lane_id=feed_lane_id,
            query_plan_id=query_plan_id,
            category=category,
            round_number=round_number,
            state="running",
            targeted_query_ids=targeted_ids,
            evaluated_at=evaluated_at,
        )
        persist_investigation(connection, running, fence_check=lease_check)
        query_results: tuple[InvestigationQueryResult, ...] = ()
        network_used = False
        try:
            if transport_factory is None:
                raise ValueError("investigation requires an explicit broker transport factory")
            source = _search_source(
                connection,
                preferred_source_id=row[2],
                category=category,
            )
            network_used = True
            query_results = asyncio.run(
                _fetch_targeted_queries(
                    source,
                    category=category,
                    targeted=targeted,
                    evaluated_at=evaluated_at,
                    transport_factory=transport_factory,
                )
            )
            state = "complete" if query_results and all(item.status == "success" for item in query_results) else "failed"
            terminal_state = state
        except Exception as exc:
            state = "failed"
            terminal_state = "failed"
            failure = type(exc).__name__
            query_results = tuple(
                InvestigationQueryResult(
                    query_plan_id=plan.query_plan_id,
                    source_id=str(row[2]),
                    status="failed",
                    http_status=None,
                    returned_count=0,
                    error_class=failure,
                )
                for plan in targeted
            )
        completed = make_investigation_job(
            candidate_id=candidate_id,
            feed_lane_id=feed_lane_id,
            query_plan_id=query_plan_id,
            category=category,
            round_number=round_number,
            state=state,
            terminal_state=terminal_state,
            targeted_query_ids=targeted_ids,
            evaluated_at=evaluated_at,
        )
        persist_investigation(connection, completed, fence_check=lease_check)
        return InvestigationResult(
            job=completed,
            targeted_queries=targeted,
            query_results=query_results,
            network_used=network_used,
        )
    finally:
        connection.close()


__all__ = [
    "INVESTIGATION_STATES",
    "MAX_INVESTIGATION_ROUNDS",
    "InvestigationJob",
    "InvestigationQueryResult",
    "InvestigationResult",
    "investigation_id",
    "investigation_payload",
    "enqueue_candidate_investigations",
    "make_investigation_job",
    "persist_investigation",
    "run_investigation",
    "validate_investigation_payload",
]
