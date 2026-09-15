"""Bounded, delivery-free async ingest orchestration for schema-v4 databases.

The runner owns retries, host pacing, leases, and persistence. Adapters own one
HTTP attempt and normalization. The caller owns schema migration and supplies
an existing v4 database plus an explicit UTC run timestamp.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Awaitable, Callable, Sequence
from urllib.parse import urlsplit

from .adapters.base import (
    AdapterError,
    FetchResult,
    ItemRejection,
    NormalizedItem,
    RetryableHttpError,
    Transport,
)
from .adapters.rss import RssAdapter
from .adapters.searxng import SearxngAdapter
from .live_contracts import QuerySeed, QueryStatus, SourceAdapter, SourceContract, source_to_row, stable_id
from .schema_v3 import V3_COLUMNS, V3_TABLES
from .schema_v4 import V4_COLUMNS, V4_TABLES
from .source_registry import load_registry

_GLOBAL_CONCURRENCY = 4
_HOST_CONCURRENCY = 1
_HOST_DELAY_SECONDS = 1.0
_LEASE_SECONDS = 300
_MAX_RETRIES = 2
_DEFAULT_RETRY_SECONDS = 300

AsyncSleep = Callable[[float], Awaitable[None]]
Monotonic = Callable[[], float]
UtcNow = Callable[[], str]
TransportFactory = Callable[[SourceContract], Transport | None]


@dataclass(frozen=True, slots=True)
class FilteredItem:
    source_item_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class QueryResult:
    source_id: str
    query_plan_id: str
    attempt_id: str
    query_text: str
    status: str
    http_status: int | None
    returned_count: int
    inserted_count: int
    duplicate_count: int
    rejected_count: int
    filtered_count: int
    retries: int
    retry_after_seconds: int | None = None
    rate_limit_reset_at: str | None = None
    error: str | None = None
    rejections: tuple[ItemRejection, ...] = field(default_factory=tuple)
    filtered_items: tuple[FilteredItem, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class SourceResult:
    source_id: str
    host: str
    queries_run: int
    queries_succeeded: int
    queries_partial: int
    queries_failed: int
    queries_rate_limited: int
    items_returned: int
    items_inserted: int
    items_duplicate: int
    items_rejected: int
    items_filtered: int
    retries: int
    next_due_at: str | None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class IngestReport:
    run_started_at: str
    sources_claimed: int
    sources_with_errors: int
    total_queries_run: int
    total_items_returned: int
    total_items_inserted: int
    total_items_duplicate: int
    total_items_rejected: int
    total_items_filtered: int
    total_retries: int
    query_results: tuple[QueryResult, ...] = field(default_factory=tuple)
    source_results: tuple[SourceResult, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class _Claim:
    source: SourceContract
    queries: tuple[QuerySeed, ...]
    lease_expires_at: str


@dataclass(frozen=True, slots=True)
class _Job:
    source: SourceContract
    query: QuerySeed
    query_plan_id: str
    attempt_id: str
    lease_expires_at: str
    etag: str | None
    last_modified: str | None


@dataclass(frozen=True, slots=True)
class _NetworkOutcome:
    job: _Job
    result: FetchResult
    finished_at: str
    retries: int


class _HostGate:
    """Serialize one host and enforce a minimum interval between starts."""

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.next_start = 0.0

    async def call(
        self,
        operation: Callable[[], Awaitable[FetchResult]],
        *,
        global_semaphore: asyncio.Semaphore,
        sleep: AsyncSleep,
        monotonic: Monotonic,
    ) -> FetchResult:
        async with self.lock:
            delay = self.next_start - monotonic()
            if delay > 0:
                await sleep(delay)
            async with global_semaphore:
                started = monotonic()
                self.next_start = started + _HOST_DELAY_SECONDS
                return await operation()


async def run_ingest(
    db_path: str | Path,
    sources_path: str | Path,
    topics_path: str | Path,
    policy_path: str | Path,
    run_started_at: str,
    *,
    source_ids: Sequence[str] | None = None,
    max_queries: int | None = None,
    transport_factory: TransportFactory | None = None,
    async_sleep: AsyncSleep = asyncio.sleep,
    monotonic: Monotonic = time.monotonic,
    utc_now: UtcNow | None = None,
) -> IngestReport:
    """Run one bounded ingest tick and return an immutable telemetry report."""
    path = Path(db_path)
    if not path.is_file():
        raise FileNotFoundError(f"database path does not exist: {path}")
    _parse_utc(run_started_at, "run_started_at")
    if max_queries is not None and (type(max_queries) is not int or max_queries < 1):
        raise ValueError("max_queries must be a positive integer")
    requested = None if source_ids is None else tuple(source_ids)
    if requested is not None:
        if not requested or any(type(value) is not str or not value for value in requested):
            raise ValueError("source_ids must be a non-empty sequence of source IDs")
        if len(set(requested)) != len(requested):
            raise ValueError("source_ids must not contain duplicates")

    _verify_schema(path)
    config = load_registry(sources_path, topics_path, policy_path)
    by_id = {source.source_id: source for source in config.sources}
    if requested is not None:
        unknown = sorted(set(requested) - set(by_id))
        if unknown:
            raise ValueError(f"unknown source IDs: {unknown}")
    for source in config.sources:
        if len(source.category_scope) != 1:
            raise ValueError(f"source {source.source_id!r} must have exactly one business category")

    now = utc_now if utc_now is not None else _utc_now
    connection = _open_writer(path)
    try:
        _sync_sources(connection, config.sources, run_started_at)
        claims = _claim_queries(
            connection,
            config.sources,
            run_started_at,
            source_ids=requested,
            max_queries=max_queries,
        )
        if not claims:
            return IngestReport(run_started_at, 0, 0, 0, 0, 0, 0, 0, 0, 0)

        jobs = _prepare_jobs(connection, claims, run_started_at)
        global_semaphore = asyncio.Semaphore(_GLOBAL_CONCURRENCY)
        gates = {claim.source.host: _HostGate() for claim in claims}
        tasks = [
            asyncio.create_task(
                _fetch_job(
                    job,
                    gate=gates[job.source.host],
                    global_semaphore=global_semaphore,
                    transport_factory=transport_factory,
                    sleep=async_sleep,
                    monotonic=monotonic,
                    now=now,
                )
            )
            for job in jobs
        ]
        gathered = await asyncio.gather(*tasks, return_exceptions=True)

        query_results: list[QueryResult] = []
        outcomes: list[_NetworkOutcome] = []
        for job, value in zip(jobs, gathered):
            if isinstance(value, BaseException):
                safe = _safe_error(value)
                value = _NetworkOutcome(
                    job=job,
                    result=FetchResult(error=AdapterError(safe), retryable=False),
                    finished_at=_validated_now(now),
                    retries=0,
                )
            outcomes.append(value)
            query_results.append(_persist_outcome(connection, value))

        source_results = _finalize_sources(connection, claims, query_results, outcomes)
        return IngestReport(
            run_started_at=run_started_at,
            sources_claimed=len(claims),
            sources_with_errors=sum(1 for result in source_results if result.error is not None),
            total_queries_run=len(query_results),
            total_items_returned=sum(result.returned_count for result in query_results),
            total_items_inserted=sum(result.inserted_count for result in query_results),
            total_items_duplicate=sum(result.duplicate_count for result in query_results),
            total_items_rejected=sum(result.rejected_count for result in query_results),
            total_items_filtered=sum(result.filtered_count for result in query_results),
            total_retries=sum(result.retries for result in query_results),
            query_results=tuple(query_results),
            source_results=tuple(source_results),
        )
    finally:
        connection.close()


def run_ingest_sync(*args, **kwargs) -> IngestReport:
    """Synchronous convenience wrapper; do not call from an active event loop."""
    return asyncio.run(run_ingest(*args, **kwargs))


def _verify_schema(db_path: Path) -> None:
    uri = f"{db_path.resolve().as_uri()}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        try:
            markers = {int(row[0]) for row in connection.execute("SELECT version FROM schema_migrations")}
            accepted_markers = ({1, 2, 3, 4}, {1, 2, 3, 4, 5}, {1, 2, 3, 4, 5, 6})
            if markers not in accepted_markers:
                raise ValueError(
                    "database schema markers must be a known additive v4-v6 prefix, "
                    f"got {sorted(markers)}"
                )
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            required = set(V3_TABLES) | set(V4_TABLES)
            missing = sorted(required - tables)
            if missing:
                raise ValueError(f"database is missing required tables: {missing}")
            for table, expected in {**V3_COLUMNS, **V4_COLUMNS}.items():
                actual = tuple(row[1] for row in connection.execute(f'PRAGMA table_info("{table}")'))
                if actual != expected:
                    raise ValueError(f"table {table} has incompatible columns: {actual}")
        finally:
            connection.close()
    except sqlite3.DatabaseError as exc:
        raise ValueError(f"database is not a compatible schema-v4 SQLite database: {exc}") from exc


def _open_writer(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA foreign_keys=ON")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        connection.close()
        raise ValueError("foreign-key enforcement could not be enabled")
    return connection


def _source_config_hash(source: SourceContract) -> str:
    row = source_to_row(source)
    encoded = json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sync_sources(
    connection: sqlite3.Connection,
    sources: tuple[SourceContract, ...],
    created_at: str,
) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        for source in sources:
            row = source_to_row(source)
            values = (
                row["source_id"], row["adapter_type"], row["source_role"], row["host"],
                row["category_scope_json"], row["enabled"], row["queries_json"],
                row["title_blocklist_json"], row["content_blocklist_json"],
                row["url_blocklist_json"], row["allowlist_domains_json"],
                row["cadence_minutes"], row["terms_notes"], row["rate_limit_notes"],
                row["next_due_at"], _source_config_hash(source), created_at,
            )
            connection.execute(
                """INSERT INTO source_registry(
                       source_id,adapter_type,source_role,host,category_scope_json,enabled,
                       queries_json,title_blocklist_json,content_blocklist_json,
                       url_blocklist_json,allowlist_domains_json,cadence_minutes,
                       terms_notes,rate_limit_notes,next_due_at,config_hash,created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(source_id) DO UPDATE SET
                       adapter_type=excluded.adapter_type,
                       source_role=excluded.source_role,
                       host=excluded.host,
                       category_scope_json=excluded.category_scope_json,
                       enabled=excluded.enabled,
                       queries_json=excluded.queries_json,
                       title_blocklist_json=excluded.title_blocklist_json,
                       content_blocklist_json=excluded.content_blocklist_json,
                       url_blocklist_json=excluded.url_blocklist_json,
                       allowlist_domains_json=excluded.allowlist_domains_json,
                       cadence_minutes=excluded.cadence_minutes,
                       terms_notes=excluded.terms_notes,
                       rate_limit_notes=excluded.rate_limit_notes,
                       config_hash=excluded.config_hash""",
                values,
            )
        connection.commit()
    except sqlite3.Error:
        connection.rollback()
        raise


def _claim_queries(
    connection: sqlite3.Connection,
    sources: tuple[SourceContract, ...],
    claimed_at: str,
    *,
    source_ids: tuple[str, ...] | None,
    max_queries: int | None,
) -> tuple[_Claim, ...]:
    lease_expires_at = _format_utc(_parse_utc(claimed_at, "claimed_at") + timedelta(seconds=_LEASE_SECONDS))
    configured = {source.source_id: source for source in sources}
    selected_ids = set(source_ids) if source_ids is not None else set(configured)
    connection.execute("BEGIN IMMEDIATE")
    try:
        rows = connection.execute(
            """SELECT source_id FROM source_registry
               WHERE enabled=1 AND (next_due_at IS NULL OR next_due_at<=?)
               ORDER BY CASE WHEN next_due_at IS NULL THEN 0 ELSE 1 END, next_due_at, source_id""",
            (claimed_at,),
        ).fetchall()
        remaining = max_queries
        claims: list[_Claim] = []
        for (source_id,) in rows:
            if source_id not in selected_ids or source_id not in configured:
                continue
            source = configured[source_id]
            count = len(source.queries) if remaining is None else min(len(source.queries), remaining)
            if count < 1:
                break
            connection.execute(
                """UPDATE source_registry SET next_due_at=?
                   WHERE source_id=? AND enabled=1 AND (next_due_at IS NULL OR next_due_at<=?)""",
                (lease_expires_at, source_id, claimed_at),
            )
            if connection.execute("SELECT changes()").fetchone()[0] != 1:
                continue
            claims.append(_Claim(source, source.queries[:count], lease_expires_at))
            if remaining is not None:
                remaining -= count
                if remaining == 0:
                    break
        connection.commit()
        return tuple(claims)
    except sqlite3.Error:
        connection.rollback()
        raise


def _prepare_jobs(
    connection: sqlite3.Connection,
    claims: tuple[_Claim, ...],
    started_at: str,
) -> tuple[_Job, ...]:
    connection.execute("BEGIN IMMEDIATE")
    jobs: list[_Job] = []
    try:
        state = {
            row[0]: (row[1], row[2])
            for row in connection.execute("SELECT source_id,etag,last_modified FROM fetch_state")
        }
        for claim in claims:
            category = claim.source.category_scope[0]
            for query in claim.queries:
                plan_id = stable_id(
                    "query-plan", claim.source.source_id, category, query.text, "\x1f".join(query.categories)
                )
                connection.execute(
                    """INSERT OR IGNORE INTO query_plans(
                           query_plan_id,source_id,query_text,category,topic,entity,
                           reason_selected,cooldown_seconds,max_rounds,created_at)
                       VALUES(?,?,?,?,NULL,NULL,?,?,?,?)""",
                    (
                        plan_id, claim.source.source_id, query.text, category,
                        "source-config", (claim.source.cadence_minutes or 60) * 60, 1, started_at,
                    ),
                )
                attempt_id = stable_id("query-attempt", plan_id, started_at)
                connection.execute(
                    """INSERT INTO query_attempts(
                           attempt_id,query_plan_id,status,started_at,finished_at,
                           returned_count,novel_count,verified_count,duplicate_count,
                           stale_count,error_count,error,rate_limit_reset_at)
                       VALUES(?,?,?, ?,NULL,0,0,0,0,0,0,NULL,NULL)
                       ON CONFLICT(attempt_id) DO UPDATE SET
                           status=excluded.status,started_at=excluded.started_at,finished_at=NULL,
                           returned_count=0,novel_count=0,verified_count=0,duplicate_count=0,
                           stale_count=0,error_count=0,error=NULL,rate_limit_reset_at=NULL""",
                    (attempt_id, plan_id, QueryStatus.RUNNING.value, started_at),
                )
                etag, last_modified = state.get(claim.source.source_id, (None, None))
                jobs.append(
                    _Job(
                        source=claim.source,
                        query=query,
                        query_plan_id=plan_id,
                        attempt_id=attempt_id,
                        lease_expires_at=claim.lease_expires_at,
                        etag=etag,
                        last_modified=last_modified,
                    )
                )
        connection.commit()
        return tuple(jobs)
    except sqlite3.Error:
        connection.rollback()
        raise


async def _fetch_job(
    job: _Job,
    *,
    gate: _HostGate,
    global_semaphore: asyncio.Semaphore,
    transport_factory: TransportFactory | None,
    sleep: AsyncSleep,
    monotonic: Monotonic,
    now: UtcNow,
) -> _NetworkOutcome:
    transport = transport_factory(job.source) if transport_factory is not None else None
    category = job.source.category_scope[0]
    if job.source.adapter_type is SourceAdapter.SEARXNG:
        adapter = SearxngAdapter(job.source, category=category, transport=transport)
        fetch = adapter.fetch_query
    elif job.source.adapter_type is SourceAdapter.RSS:
        adapter = RssAdapter(job.source, category=category, transport=transport)
        fetch = adapter.fetch_feed
    else:
        return _NetworkOutcome(
            job,
            FetchResult(error=AdapterError(f"unsupported adapter {job.source.adapter_type.value}")),
            _validated_now(now),
            0,
        )

    result: FetchResult | None = None
    retries = 0
    for attempt_number in range(_MAX_RETRIES + 1):
        if result is not None:
            base_delay = float(attempt_number)
            retry_delay = float(result.retry_after or 0)
            await sleep(max(base_delay, retry_delay))
            retries += 1
        retrieved_at = _validated_now(now)

        async def operation() -> FetchResult:
            return await fetch(
                job.query,
                retrieved_at=retrieved_at,
                etag=job.etag,
                last_modified=job.last_modified,
            )

        result = await gate.call(
            operation,
            global_semaphore=global_semaphore,
            sleep=sleep,
            monotonic=monotonic,
        )
        if not result.retryable:
            break
    assert result is not None
    return _NetworkOutcome(job, result, _validated_now(now), retries)


def _persist_outcome(connection: sqlite3.Connection, outcome: _NetworkOutcome) -> QueryResult:
    job = outcome.job
    result = outcome.result
    filtered, kept = _filter_items(
        result.items,
        title_blocklist=job.source.title_blocklist,
        content_blocklist=job.source.content_blocklist,
        url_blocklist=job.source.url_blocklist,
        allowlist_domains=job.source.allowlist_domains,
    )
    rejections = list(result.rejections)
    valid_items: list[NormalizedItem] = []
    for item in kept:
        if item.source_id != job.source.source_id:
            rejections.append(ItemRejection(-1, "SOURCE_MISMATCH", "adapter item source_id does not match claimed source"))
        elif item.category not in job.source.category_scope:
            rejections.append(ItemRejection(-1, "CATEGORY_MISMATCH", "adapter item category is outside source scope"))
        else:
            valid_items.append(item)

    connection.execute("BEGIN IMMEDIATE")
    try:
        lease = connection.execute(
            "SELECT next_due_at FROM source_registry WHERE source_id=?", (job.source.source_id,)
        ).fetchone()
        if lease is None or lease[0] != job.lease_expires_at:
            connection.rollback()
            return QueryResult(
                job.source.source_id, job.query_plan_id, job.attempt_id, job.query.text,
                "lease_lost", result.http_status, len(result.items) + len(result.rejections),
                0, 0, len(rejections), len(filtered), outcome.retries,
                _retry_after_int(result.retry_after), result.rate_limit_reset,
                "source lease was replaced before persistence", tuple(rejections), tuple(filtered),
            )

        inserted = 0
        duplicate = 0
        for item in valid_items:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO source_items(
                       source_item_id,source_id,external_id,category,original_url,
                       canonical_url,publisher,source_role,author_handle,retrieval_method,
                       raw_content_hash,title,body,raw,retrieved_at,published_at,updated_at,
                       publication_evidence)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    item.source_item_id, item.source_id, item.external_id, item.category,
                    item.original_url, item.canonical_url, item.publisher, item.source_role,
                    item.author_handle, item.retrieval_method, item.raw_content_hash,
                    item.title, item.body, item.raw, item.retrieved_at, item.published_at,
                    item.updated_at, item.publication_evidence,
                ),
            )
            if cursor.rowcount == 1:
                inserted += 1
            else:
                duplicate += 1

        status, error = _query_status(result, bool(rejections or filtered))
        error_count = len(rejections) + (1 if result.error is not None else 0)
        connection.execute(
            """UPDATE query_attempts SET
                   status=?,finished_at=?,returned_count=?,novel_count=?,verified_count=0,
                   duplicate_count=?,stale_count=0,error_count=?,error=?,rate_limit_reset_at=?
               WHERE attempt_id=?""",
            (
                status.value, outcome.finished_at, len(result.items) + len(result.rejections),
                inserted, duplicate, error_count, error, result.rate_limit_reset, job.attempt_id,
            ),
        )
        old = connection.execute(
            "SELECT etag,last_modified FROM fetch_state WHERE source_id=?", (job.source.source_id,)
        ).fetchone()
        etag = result.validators.etag if result.validators and result.validators.etag is not None else (old[0] if old else None)
        last_modified = (
            result.validators.last_modified
            if result.validators and result.validators.last_modified is not None
            else (old[1] if old else None)
        )
        connection.execute(
            """INSERT INTO fetch_state(
                   source_id,etag,last_modified,cursor,rate_limit_remaining,
                   rate_limit_reset_at,retry_after_seconds,last_http_status,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?)
               ON CONFLICT(source_id) DO UPDATE SET
                   etag=excluded.etag,last_modified=excluded.last_modified,cursor=excluded.cursor,
                   rate_limit_remaining=excluded.rate_limit_remaining,
                   rate_limit_reset_at=excluded.rate_limit_reset_at,
                   retry_after_seconds=excluded.retry_after_seconds,
                   last_http_status=excluded.last_http_status,updated_at=excluded.updated_at""",
            (
                job.source.source_id, etag, last_modified, result.cursor,
                result.rate_limit_remaining, result.rate_limit_reset,
                _retry_after_int(result.retry_after), result.http_status, outcome.finished_at,
            ),
        )
        connection.commit()
    except sqlite3.Error:
        connection.rollback()
        raise

    return QueryResult(
        source_id=job.source.source_id,
        query_plan_id=job.query_plan_id,
        attempt_id=job.attempt_id,
        query_text=job.query.text,
        status=status.value,
        http_status=result.http_status,
        returned_count=len(result.items) + len(result.rejections),
        inserted_count=inserted,
        duplicate_count=duplicate,
        rejected_count=len(rejections),
        filtered_count=len(filtered),
        retries=outcome.retries,
        retry_after_seconds=_retry_after_int(result.retry_after),
        rate_limit_reset_at=result.rate_limit_reset,
        error=error,
        rejections=tuple(rejections),
        filtered_items=tuple(filtered),
    )


def _filter_items(
    items: tuple[NormalizedItem, ...],
    *,
    title_blocklist: tuple[str, ...],
    content_blocklist: tuple[str, ...],
    url_blocklist: tuple[str, ...],
    allowlist_domains: tuple[str, ...],
) -> tuple[list[FilteredItem], list[NormalizedItem]]:
    filtered: list[FilteredItem] = []
    kept: list[NormalizedItem] = []
    for item in items:
        reason = _filter_reason(
            item,
            title_blocklist=title_blocklist,
            content_blocklist=content_blocklist,
            url_blocklist=url_blocklist,
            allowlist_domains=allowlist_domains,
        )
        if reason is None:
            kept.append(item)
        else:
            filtered.append(FilteredItem(item.source_item_id, reason))
    return filtered, kept


def _filter_reason(
    item: NormalizedItem,
    *,
    title_blocklist: tuple[str, ...],
    content_blocklist: tuple[str, ...],
    url_blocklist: tuple[str, ...],
    allowlist_domains: tuple[str, ...],
) -> str | None:
    title = (item.title or "").casefold()
    content = (item.body or "").casefold()
    urls = f"{item.original_url}\n{item.canonical_url}".casefold()
    for value in title_blocklist:
        if value.casefold() in title:
            return f"title_blocklist:{value}"
    for value in content_blocklist:
        if value.casefold() in content:
            return f"content_blocklist:{value}"
    for value in url_blocklist:
        if value.casefold() in urls:
            return f"url_blocklist:{value}"
    if allowlist_domains:
        hostname = (urlsplit(item.canonical_url).hostname or "").casefold().rstrip(".")
        allowed = any(
            hostname == domain.casefold().lstrip(".").rstrip(".")
            or hostname.endswith("." + domain.casefold().lstrip(".").rstrip("."))
            for domain in allowlist_domains
        )
        if not allowed:
            return f"allowlist_domain:{hostname}"
    return None


def _query_status(result: FetchResult, partial: bool) -> tuple[QueryStatus, str | None]:
    if result.error is None:
        return (QueryStatus.PARTIAL if partial else QueryStatus.SUCCESS), None
    status = getattr(result.error, "status", None)
    query_status = QueryStatus.RATE_LIMITED if status == 429 else QueryStatus.FAILED
    return query_status, _safe_error(result.error)


def _finalize_sources(
    connection: sqlite3.Connection,
    claims: tuple[_Claim, ...],
    query_results: list[QueryResult],
    outcomes: list[_NetworkOutcome],
) -> list[SourceResult]:
    outcome_by_attempt = {outcome.job.attempt_id: outcome for outcome in outcomes}
    results: list[SourceResult] = []
    for claim in claims:
        selected = [result for result in query_results if result.source_id == claim.source.source_id]
        selected_outcomes = [outcome_by_attempt[result.attempt_id] for result in selected]
        completion = max(outcome.finished_at for outcome in selected_outcomes)
        failures = [result for result in selected if result.status in ("failed", "rate_limited", "lease_lost")]
        if failures:
            retry_seconds = max(
                [_DEFAULT_RETRY_SECONDS]
                + [result.retry_after_seconds or 0 for result in failures]
            )
            next_due = _format_utc(_parse_utc(completion, "finished_at") + timedelta(seconds=retry_seconds))
            for result in failures:
                if result.rate_limit_reset_at is not None:
                    reset = _parse_utc(result.rate_limit_reset_at, "rate_limit_reset_at")
                    if reset > _parse_utc(next_due, "next_due_at"):
                        next_due = _format_utc(reset)
        else:
            next_due = _format_utc(
                _parse_utc(completion, "finished_at")
                + timedelta(minutes=claim.source.cadence_minutes or 60)
            )
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                "UPDATE source_registry SET next_due_at=? WHERE source_id=? AND next_due_at=?",
                (next_due, claim.source.source_id, claim.lease_expires_at),
            )
            changed = connection.execute("SELECT changes()").fetchone()[0]
            connection.commit()
        except sqlite3.Error:
            connection.rollback()
            raise
        effective_next_due = next_due if changed == 1 else None
        errors = tuple(dict.fromkeys(result.error for result in failures if result.error))
        results.append(
            SourceResult(
                source_id=claim.source.source_id,
                host=claim.source.host,
                queries_run=len(selected),
                queries_succeeded=sum(result.status == "success" for result in selected),
                queries_partial=sum(result.status == "partial" for result in selected),
                queries_failed=sum(result.status in ("failed", "lease_lost") for result in selected),
                queries_rate_limited=sum(result.status == "rate_limited" for result in selected),
                items_returned=sum(result.returned_count for result in selected),
                items_inserted=sum(result.inserted_count for result in selected),
                items_duplicate=sum(result.duplicate_count for result in selected),
                items_rejected=sum(result.rejected_count for result in selected),
                items_filtered=sum(result.filtered_count for result in selected),
                retries=sum(result.retries for result in selected),
                next_due_at=effective_next_due,
                error="; ".join(errors) if errors else ("source lease was replaced" if effective_next_due is None else None),
            )
        )
    return results


def _safe_error(error: BaseException) -> str:
    text = f"{type(error).__name__}: {error}"

    def strip_query(match: re.Match[str]) -> str:
        parsed = urlsplit(match.group(0))
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

    text = re.sub(r"https?://[^\s]+", strip_query, text)
    return text[:500]


def _retry_after_int(value: int | float | None) -> int | None:
    if value is None:
        return None
    return max(0, math.ceil(float(value)))


def _parse_utc(value: str, field_name: str) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise ValueError(f"{field_name} must be a UTC ISO-8601 timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid UTC ISO-8601 timestamp") from exc
    if parsed.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be UTC")
    return parsed.astimezone(UTC)


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_now() -> str:
    return _format_utc(datetime.now(UTC))


def _validated_now(now: UtcNow) -> str:
    value = now()
    _parse_utc(value, "utc_now result")
    return value
