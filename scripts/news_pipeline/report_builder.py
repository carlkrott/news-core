"""Phase 5 deterministic report orchestration."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence, cast

from .briefing_contracts import BriefingInput, compute_morning_window
from .briefing_ledger import BriefingLedger, normalize_canonical_payload
from .briefing_shadow_hook import run_shadow_briefing
from .briefing_renderer import RenderResult
from .event_contracts import AdjudicationResult, EventCandidate, SemanticDecision, SemanticReasonCode, event_version_identity
from .models import Category, Subject, subject_for_category
from .contracts import CandidateArticle, FilterResult, DecisionCode, ReasonCode, TrustTier
from .policies import QueryPolicy
from .report_artifacts import ArtifactMismatch, ArtifactRoot, ReportArtifacts, ArtifactResult, compute_artifacts, verify_artifacts
from .report_artifacts import _paths as _artifact_paths
from datetime import timedelta


@dataclass(frozen=True, slots=True)
class ReportRunResult:
    report_id: str
    window_start: str
    window_end: str
    generation_status: str
    artifact_result: ArtifactResult | None
    was_replayed: bool
    included_count: int
    excluded_count: int


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")
    return value.astimezone(timezone.utc)


def _z(value: datetime) -> str:
    utc = value.astimezone(timezone.utc)
    return utc.isoformat(
        timespec="microseconds" if utc.microsecond else "seconds"
    ).replace("+00:00", "Z")


def _parse_z(value: str, field: str = "timestamp") -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"{field} must be a UTC timestamp ending in Z")
    try:
        result = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{field} is not a valid timestamp") from exc
    if result.utcoffset() != timedelta(0):
        raise ValueError(f"{field} must be UTC")
    if _z(result) != value:
        raise ValueError(f"{field} must use canonical UTC Z form")
    return result


def _report_id(lower: str, upper: str) -> str:
    return "report-" + hashlib.sha256((lower + "\0" + upper).encode()).hexdigest()


def _policy(category: Category) -> QueryPolicy:
    return QueryPolicy(category=category, allowed_query_groups=(category.value,), recency=timedelta(days=7), missing_date_fallback=True, exact_title_lookback=timedelta(days=7), exact_url_lookback=timedelta(days=7), exact_identity_lookback=timedelta(days=7), cross_category_exact_url=True)


class _PassThroughSummarizer:
    model_call_count = 0
    cache_hit_count = 0
    def summarize_category(self, category: Category, raw_inputs: Sequence[Any]) -> Any:
        from .briefing_summarizer import CategorySummaryResult, SummaryItem, SummarySource, SummarizerErrorCategory
        return CategorySummaryResult(items=tuple(SummaryItem(candidate_id=x.candidate_id, summary=x.snippet or x.title, source=SummarySource.FALLBACK, error_category=SummarizerErrorCategory.INPUT_BOUNDS) for x in raw_inputs), model_used=False, cache_hit=False)


class _Renderer:
    def __init__(self) -> None:
        self.calls = 0
    def render_briefing(self, records: Sequence[Any], upper_bound_utc: datetime) -> RenderResult:
        from .briefing_renderer import render_briefing
        self.calls += 1
        return render_briefing(records, upper_bound_utc)


def _briefing_input(event_id: str, version: int, summary: str, category: Category, valid_from: str) -> BriefingInput:
    _parse_z(valid_from, "valid_from")
    candidate_id = f"ev:{event_id}:v{version}"
    article = CandidateArticle(candidate_id=candidate_id, category=category, query_group=category.value, title=summary[:256] or "Event summary unavailable", snippet=summary[:2048] or "Event summary unavailable", original_url=None, canonical_url=None, published_at=valid_from, published_evidence="source", observed_at=valid_from, evaluated_at=valid_from)
    filtered = FilterResult(candidate=article, decision=DecisionCode.KEEP, reasons=(ReasonCode.OK_KEEP,), matched_article_ids=(), matched_observation_ids=(), trust_tier=TrustTier.UNKNOWN, evaluated_publication_time=None, ordinal=0)
    ec = EventCandidate(candidate=article, filter_result=filtered, query_policy=_policy(category))
    decision = SemanticDecision.distinct_event if version == 1 else SemanticDecision.material_update
    reason = (SemanticReasonCode.DISTINCT_EVENT,) if version == 1 else (SemanticReasonCode.NUMERIC_REVISION,)
    adjudication = AdjudicationResult(candidate_id=candidate_id, semantic_decision=decision, phase2_decision=DecisionCode.KEEP, phase2_reasons=(ReasonCode.OK_KEEP,), semantic_reasons=reason, cluster_id=event_id, matched_candidate_ids=(candidate_id,), matched_history_ids=(), matched_observation_ids=(), fact_deltas=(), model_used=False, model_confidence=None, model_error_category=None, ordinal=0, event_version=version, subject_id=subject_for_category(category).value)
    return BriefingInput(event_candidate=ec, adjudication=adjudication)


def _validated_event_row(row: tuple[Any, ...]) -> tuple[str, int, str, str, str, str]:
    if len(row) != 6:
        raise ValueError("malformed event version row shape")
    event_id, version, summary, category, valid_from, change_reason = row
    if (
        type(event_id) is not str
        or not event_id
        or type(version) is not int
        or version < 1
        or type(summary) is not str
        or not summary.strip()
        or type(category) is not str
        or type(valid_from) is not str
        or type(change_reason) is not str
        or not change_reason.strip()
    ):
        raise ValueError("malformed event version row")
    _parse_z(valid_from, "valid_from")
    try:
        Category(category)
    except ValueError as exc:
        raise ValueError(f"invalid category in database: {category!r}") from exc
    return event_id, version, summary, category, valid_from, change_reason


def _candidate_id(event_id: str, version: int) -> str:
    return f"ev:{event_id}:v{version}"


_KNOWN_EVENT_STATES = frozenset({"unverified", "watchlist", "verified", "rejected"})


def _validated_event_status(state: object, superseded_at: object) -> tuple[str, str | None]:
    if type(state) is not str or state not in _KNOWN_EVENT_STATES:
        raise ValueError("malformed event verification state")
    if type(superseded_at) not in (str, type(None)):
        raise ValueError("malformed event superseded_at")
    state_value = cast(str, state)
    superseded_value = cast(str | None, superseded_at)
    if superseded_value is not None:
        _parse_z(superseded_value, "superseded_at")
    return state_value, superseded_value


def _has_canonical_source_url(con: sqlite3.Connection, event_id: str, version: int) -> bool:
    row = con.execute(
        """SELECT 1
           FROM event_claims ec
           JOIN claims c ON c.claim_id=ec.claim_id
           JOIN source_items si ON si.source_item_id=c.source_item_id
          WHERE ec.event_id=? AND ec.event_version=?
            AND si.canonical_url IS NOT NULL
            AND trim(si.canonical_url) <> ''
          LIMIT 1""",
        (event_id, version),
    ).fetchone()
    return row is not None


def _latest_events(con: sqlite3.Connection, lower: str, upper: str) -> list[tuple[str, int, str, str, str, str]]:
    rows = con.execute("""
        SELECT ev.event_id, ev.version, ev.summary, e.category, ev.valid_from,
               ev.material_change_reason, ev.verification_state, ev.superseded_at
        FROM event_versions ev JOIN events e ON e.id=ev.event_id
        JOIN (SELECT event_id, MAX(version) version FROM event_versions GROUP BY event_id) latest
          ON latest.event_id=ev.event_id AND latest.version=ev.version
    """).fetchall()
    lower_dt = _parse_z(lower, "window_start")
    upper_dt = _parse_z(upper, "window_end")
    checked: list[tuple[str, int, str, str, str, str]] = []
    for raw in rows:
        row = _validated_event_row(tuple(raw[:6]))
        state, superseded_at = _validated_event_status(raw[6], raw[7])
        valid_dt = _parse_z(row[4], "valid_from")
        if not lower_dt < valid_dt <= upper_dt:
            continue
        if state != "verified" or superseded_at is not None:
            continue
        if row[5].strip().casefold() == "retraction":
            continue
        if not _has_canonical_source_url(con, row[0], row[1]):
            continue
        checked.append(row)
    return sorted(checked, key=lambda row: (_parse_z(row[4], "valid_from"), row[0], row[1]))


def _items_from_links(con: sqlite3.Connection, report_id: str) -> list[tuple[str, int, str, str, str, str]]:
    rows = con.execute("""SELECT re.event_id,re.event_version,ev.summary,e.category,ev.valid_from,
               ev.material_change_reason,ev.verification_state,ev.superseded_at
        FROM report_events re JOIN event_versions ev ON ev.event_id=re.event_id AND ev.version=re.event_version
        JOIN events e ON e.id=ev.event_id WHERE re.report_id=?
        ORDER BY re.sort_order,re.event_id,re.event_version""", (report_id,)).fetchall()
    checked: list[tuple[str, int, str, str, str, str]] = []
    for raw in rows:
        state, superseded_at = _validated_event_status(raw[6], raw[7])
        row = _validated_event_row(tuple(raw[:6]))
        if state != "verified" or superseded_at is not None:
            continue
        if row[5].strip().casefold() == "retraction":
            continue
        if not _has_canonical_source_url(con, row[0], row[1]):
            continue
        checked.append(row)
    return checked


def _item_dict(con: sqlite3.Connection, row: tuple[str, int, str, str, str, str]) -> dict[str, Any]:
    event_id, version, summary, category, valid_from, change_reason = _validated_event_row(row)
    source_urls = tuple(row[0] for row in con.execute("""
        SELECT DISTINCT si.canonical_url
        FROM event_claims ec
        JOIN claims c ON c.claim_id=ec.claim_id
        JOIN source_items si ON si.source_item_id=c.source_item_id
        WHERE ec.event_id=? AND ec.event_version=?
        ORDER BY si.canonical_url
    """, (event_id, version)).fetchall())
    if any(type(url) is not str or not url for url in source_urls):
        raise ValueError("malformed source URL row")
    # Event dates are optional evidence; do not invent unavailable dates or links.
    date_row = con.execute("""SELECT date_value,date_type FROM event_dates
        WHERE event_id=? AND event_version=?
          AND date_type IN ('occurred_at','announced_at','scheduled_for')
          AND date_value IS NOT NULL
        ORDER BY CASE date_type WHEN 'occurred_at' THEN 0 WHEN 'announced_at' THEN 1 ELSE 2 END,
                 date_value LIMIT 1""", (event_id, version)).fetchone()
    semantic_reason = (
        SemanticReasonCode.DISTINCT_EVENT.value
        if version == 1
        else (
            SemanticReasonCode.CORRECTION_OR_RETRACTION.value
            if "correct" in change_reason.casefold()
            else SemanticReasonCode.NUMERIC_REVISION.value
        )
    )
    return {
        "event_id": event_id,
        "event_version": version,
        "title": summary[:256],
        "summary": summary,
        "category": category,
        "url": source_urls[0] if source_urls else None,
        "source_urls": list(source_urls),
        "event_date": date_row[0] if date_row else None,
        "event_date_type": date_row[1] if date_row else None,
        "decision": "distinct_event" if version == 1 else "material_update",
        "material_change_reason": change_reason,
        "verification": "verified",
        "semantic_reasons": [semantic_reason],
    }


def _existing(con: sqlite3.Connection, lower: str, upper: str) -> dict[str, Any] | None:
    columns = [x[1] for x in con.execute("PRAGMA table_info(reports)").fetchall()]
    row = con.execute("SELECT * FROM reports WHERE window_start=? AND window_end=?", (lower, upper)).fetchone()
    return dict(zip(columns, row)) if row is not None else None


def _sections(items: tuple[dict[str, Any], ...]) -> dict[str, Any]:
    corrections = [
        dict(item)
        for item in items
        if "correct" in str(item["material_change_reason"]).casefold()
    ]
    return {"corrections": corrections, "retractions": [], "leads": []}


def _health(items: tuple[dict[str, Any], ...]) -> dict[str, Any]:
    if items:
        return {
            "status": "degraded",
            "degraded": True,
            "reasons": ["deterministic_pass_through"],
        }
    return {"status": "healthy", "degraded": False, "reasons": []}


def _report_payload(
    lower: str,
    upper: str,
    generated: str,
    report_id: str,
    items: tuple[dict[str, Any], ...],
) -> ReportArtifacts:
    return ReportArtifacts(
        window_start=lower,
        window_end=upper,
        generated_at_utc=generated,
        report_id=report_id,
        items=items,
        health=_health(items),
        sections=_sections(items),
        counts={"items": len(items)},
    )


def _validate_generating_row(
    row: dict[str, Any], report_id: str, lower: str, upper: str
) -> str:
    if row["report_id"] != report_id:
        raise ArtifactMismatch("conflicting report identity")
    if row["window_start"] != lower or row["window_end"] != upper:
        raise ArtifactMismatch("conflicting report window")
    if row["generation_status"] != "generating":
        raise ArtifactMismatch("existing report is neither generating nor complete")
    if row["delivery_state"] != "dry_run" or row["delivery_id"] is not None:
        raise ArtifactMismatch("generating report is not delivery-free")
    hash_fields = ("json_sha256", "jsonl_sha256", "markdown_sha256", "manifest_sha256")
    if any(row[name] is not None for name in hash_fields):
        raise ArtifactMismatch("generating report contains premature artifact hashes")
    generated = row["created_at"]
    parsed = _parse_z(generated, "created_at")
    if _z(parsed) != generated or parsed < _parse_z(upper, "window_end"):
        raise ArtifactMismatch("generating report has invalid created_at")
    return generated


def _verify_complete(
    con: sqlite3.Connection,
    row: dict[str, Any],
    root: ArtifactRoot,
    lower: str,
    upper: str,
) -> tuple[ArtifactResult, int]:
    expected_id = _report_id(lower, upper)
    if row["report_id"] != expected_id:
        raise ArtifactMismatch("complete report identity mismatch")
    if row["generation_status"] != "complete" or row["delivery_state"] != "dry_run" or row["delivery_id"] is not None:
        raise ArtifactMismatch("report is not a complete dry-run")
    generated = row["created_at"]
    parsed_generated = _parse_z(generated, "created_at")
    if _z(parsed_generated) != generated:
        raise ArtifactMismatch("complete report created_at is not canonical")
    stored = tuple(
        row[name]
        for name in ("json_sha256", "jsonl_sha256", "markdown_sha256", "manifest_sha256")
    )
    if any(type(value) is not str or len(value) != 64 for value in stored):
        raise ArtifactMismatch("complete report has missing or malformed table hashes")
    json_path = _artifact_paths(root, lower, upper)[0]
    if not json_path.is_file():
        raise ArtifactMismatch(f"artifact not found: {json_path}")
    try:
        persisted = json.loads(json_path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArtifactMismatch("persisted report JSON is invalid") from exc
    if type(persisted) is not dict:
        raise ArtifactMismatch("persisted report JSON must be an object")
    linked_rows = _items_from_links(con, expected_id)
    links = tuple(_item_dict(con, item) for item in linked_rows)
    expected_links = [
        (
            expected_id,
            item["event_id"],
            item["event_version"],
            item["category"],
            ordinal,
            "verified_event",
        )
        for ordinal, item in enumerate(links)
    ]
    actual_links = [
        tuple(row)
        for row in con.execute(
            "SELECT report_id,event_id,event_version,section,sort_order,inclusion_reason "
            "FROM report_events WHERE report_id=? ORDER BY sort_order,event_id,event_version",
            (expected_id,),
        ).fetchall()
    ]
    if actual_links != expected_links:
        raise ArtifactMismatch("complete report_events metadata does not match report items")
    if tuple(persisted.get("items", ())) != links:
        raise ArtifactMismatch("persisted report items do not match report_events")
    report = _report_payload(lower, upper, generated, expected_id, links)
    result = verify_artifacts(root, report)
    hashes = (
        result.json_sha256,
        result.jsonl_sha256,
        result.markdown_sha256,
        result.manifest_sha256,
    )
    if hashes != stored:
        raise ArtifactMismatch("report table hash mismatch")
    return result, len(links)


def _recover_completed_events(
    con: sqlite3.Connection, run_id: str, lower: str, upper: str
) -> list[tuple[str, int, str, str, str, str]]:
    ledger_rows = con.execute(
        "SELECT candidate_id,event_status,payload_json,event_ordinal "
        "FROM shadow_briefing_run_events WHERE run_id=? ORDER BY event_ordinal",
        (run_id,),
    ).fetchall()
    candidates: dict[str, tuple[Any, ...]] = {}
    for raw in con.execute("""
        SELECT ev.event_id,ev.version,ev.summary,e.category,ev.valid_from,
               ev.material_change_reason,ev.verification_state,ev.superseded_at
        FROM event_versions ev JOIN events e ON e.id=ev.event_id
        WHERE EXISTS (
            SELECT 1
            FROM event_claims ec
            JOIN claims c ON c.claim_id=ec.claim_id
            JOIN source_items si ON si.source_item_id=c.source_item_id
            WHERE ec.event_id=ev.event_id
              AND ec.event_version=ev.version
              AND si.canonical_url IS NOT NULL
              AND trim(si.canonical_url) <> ''
        )""").fetchall():
        legacy_id = _candidate_id(raw[0], raw[1])
        ids = (legacy_id,) + tuple(
            event_version_identity(subject.value, raw[0], raw[1])
            for subject in Subject
        )
        for cid in ids:
            if cid in candidates:
                raise ArtifactMismatch(f"ambiguous event-version candidate ID: {cid!r}")
            candidates[cid] = tuple(raw)
    recovered: list[tuple[str, int, str, str, str, str]] = []
    for expected_ordinal, ledger_row in enumerate(ledger_rows):
        candidate_id, event_status, payload_json, ordinal = ledger_row
        if ordinal != expected_ordinal or event_status not in ("RECORDED", "ALREADY_SEEN"):
            raise ArtifactMismatch("malformed shadow run event ordering or status")
        raw = candidates.get(candidate_id)
        if raw is None:
            raise ArtifactMismatch(f"shadow candidate has no event version: {candidate_id!r}")
        event = _validated_event_row(raw[:6])
        event_id, version, summary, category, valid_from, change_reason = event
        _validated_event_status(raw[6], raw[7])
        if type(payload_json) is not str:
            raise ArtifactMismatch(f"shadow payload is not JSON text: {candidate_id!r}")
        try:
            first_seen_payload = json.loads(payload_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ArtifactMismatch(f"shadow payload is invalid JSON: {candidate_id!r}") from exc
        if not isinstance(first_seen_payload, dict) or type(first_seen_payload.get("category")) is not str:
            raise ArtifactMismatch(f"shadow payload lacks a valid primary category: {candidate_id!r}")
        try:
            Category(first_seen_payload["category"])
        except ValueError as exc:
            raise ArtifactMismatch(f"shadow payload has an invalid primary category: {candidate_id!r}") from exc
        category = first_seen_payload["category"]
        event = (event_id, version, summary, category, valid_from, change_reason)
        if (
            raw[6] != "verified"
            or raw[7] is not None
            or change_reason.strip().casefold() == "retraction"
            or not (_parse_z(lower, "window_start") < _parse_z(valid_from, "valid_from") <= _parse_z(upper, "window_end"))
        ):
            raise ArtifactMismatch(f"shadow candidate is no longer report-eligible: {candidate_id!r}")
        decision = "distinct_event" if version == 1 else "material_update"
        payload = {
            "candidate_id": candidate_id,
            "category": category,
            "decision": decision,
            "title": summary[:256],
            "summary": summary[:2048],
        }
        if candidate_id != _candidate_id(event_id, version):
            subject_id = next(
                subject.value
                for subject in Subject
                if candidate_id == event_version_identity(subject.value, event_id, version)
            )
            payload.update({
                "subject_id": subject_id,
                "event_id": event_id,
                "event_version": version,
            })
        expected_payload = normalize_canonical_payload(payload)
        if payload_json.encode("utf-8") != expected_payload:
            raise ArtifactMismatch(f"shadow payload conflicts with event row: {candidate_id!r}")
        recovered.append(event)
    return recovered


def _reconcile_report_events(
    con: sqlite3.Connection, report_id: str, items: tuple[dict[str, Any], ...]
) -> None:
    expected = [
        (
            report_id,
            item["event_id"],
            item["event_version"],
            item["category"],
            ordinal,
            "verified_event",
        )
        for ordinal, item in enumerate(items)
    ]
    expected_by_key = {(row[1], row[2]): row for row in expected}
    existing = con.execute(
        "SELECT report_id,event_id,event_version,section,sort_order,inclusion_reason "
        "FROM report_events WHERE report_id=? ORDER BY sort_order,event_id,event_version",
        (report_id,),
    ).fetchall()
    existing_keys: set[tuple[str, int]] = set()
    for raw in existing:
        row = tuple(raw)
        key = (row[1], row[2])
        if key in existing_keys or expected_by_key.get(key) != row:
            raise ArtifactMismatch("existing report_events conflict with expected links")
        existing_keys.add(key)
    for row in expected:
        if (row[1], row[2]) not in existing_keys:
            con.execute(
                "INSERT INTO report_events(report_id,event_id,event_version,section,sort_order,inclusion_reason) "
                "VALUES (?,?,?,?,?,?)",
                row,
            )
    final = con.execute(
        "SELECT report_id,event_id,event_version,section,sort_order,inclusion_reason "
        "FROM report_events WHERE report_id=? ORDER BY sort_order,event_id,event_version",
        (report_id,),
    ).fetchall()
    if [tuple(row) for row in final] != expected:
        raise ArtifactMismatch("final report_events do not exactly match expected links")


def run_report(
    db_path: str | Path,
    artifacts_root: Path,
    as_of_utc: datetime,
    *,
    last_completed_upper_utc: datetime | None = None,
) -> ReportRunResult:
    as_of = _utc(as_of_utc, "as_of_utc")
    prior = (
        _utc(last_completed_upper_utc, "last_completed_upper_utc")
        if last_completed_upper_utc is not None
        else None
    )
    if prior is not None and prior >= as_of:
        raise ValueError("last_completed_upper_utc must be strictly before as_of_utc")
    lower_dt, upper_dt = compute_morning_window(as_of, prior)
    if lower_dt > upper_dt:
        raise ValueError("reporting window must have lower <= upper")
    lower, upper, requested_generated = _z(lower_dt), _z(upper_dt), _z(as_of)
    path = Path(db_path)
    if not path.is_file():
        raise ValueError(f"db_path {path!s} must be an existing file")
    root = ArtifactRoot(Path(artifacts_root))
    con = sqlite3.connect(str(path), isolation_level=None, timeout=10)
    con.execute("PRAGMA foreign_keys=ON")
    try:
        report_id = _report_id(lower, upper)
        run_id = "report-run-" + hashlib.sha256(
            (lower + "\0" + upper).encode("utf-8")
        ).hexdigest()
        existing = _existing(con, lower, upper)
        if existing is None:
            con.execute("BEGIN IMMEDIATE")
            try:
                con.execute(
                    "INSERT INTO reports(report_id,window_start,window_end,generation_status,"
                    "json_sha256,jsonl_sha256,markdown_sha256,manifest_sha256,delivery_state,"
                    "delivery_id,created_at) VALUES (?,?,?,'generating',NULL,NULL,NULL,NULL,"
                    "'dry_run',NULL,?)",
                    (report_id, lower, upper, requested_generated),
                )
                con.execute("COMMIT")
            except sqlite3.IntegrityError:
                con.execute("ROLLBACK")
            except Exception:
                con.execute("ROLLBACK")
                raise
            existing = _existing(con, lower, upper)
            if existing is None:
                raise RuntimeError("generating report row was not persisted")
        if existing["generation_status"] == "complete":
            result, count = _verify_complete(con, existing, root, lower, upper)
            return ReportRunResult(
                report_id, lower, upper, "complete", result, True, count, 0
            )
        generated = _validate_generating_row(existing, report_id, lower, upper)
        run_as_of = _parse_z(generated, "created_at")
        excluded_count = 0
        if lower == upper:
            # The ledger contract intentionally rejects zero-width runs. A
            # watermark that already equals the current cutoff is nevertheless
            # a valid truthful empty report, so it has no shadow run at all.
            events = []
        else:
            ledger = BriefingLedger(con)
            ledger.initialize_schema()
            shadow = con.execute(
                "SELECT status,window_lower_utc,window_upper_utc,started_at_utc "
                "FROM shadow_briefing_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            if shadow is not None and (
                shadow[0] in ("FAILED", "STALE")
                or shadow[1] != lower
                or shadow[2] != upper
                or shadow[3] != generated
            ):
                raise ArtifactMismatch("cannot recover failed, stale, or conflicting shadow run")
            if shadow is not None and shadow[0] == "COMPLETED":
                events = _recover_completed_events(con, run_id, lower, upper)
            else:
                eligible_events = _latest_events(con, lower, upper)
                event_by_id = {
                    _candidate_id(row[0], row[1]): row for row in eligible_events
                }
                if len(event_by_id) != len(eligible_events):
                    raise ArtifactMismatch("duplicate event-version candidate identity")
                inputs = []
                for event_id, version, summary, category, valid_from, _reason in eligible_events:
                    stamp = _parse_z(valid_from, "valid_from")
                    if valid_from == upper:
                        stamp -= timedelta(microseconds=1)
                    inputs.append(
                        _briefing_input(
                            event_id, version, summary, Category(category), _z(stamp)
                        )
                    )
                engine = run_shadow_briefing(
                    run_id=run_id,
                    briefing_inputs=tuple(inputs),
                    ledger=ledger,
                    summarizer=_PassThroughSummarizer(),
                    renderer=_Renderer(),
                    as_of_utc=run_as_of,
                    last_completed_upper_utc=_parse_z(lower, "window_start"),
                )
                events = []
                for included in engine.included_items:
                    event = event_by_id.get(included.briefing_input.candidate_id)
                    if event is None:
                        raise ArtifactMismatch("engine returned an unknown report candidate")
                    events.append(event)
                excluded_count = len(engine.excluded_items)
                completed = con.execute(
                    "SELECT status FROM shadow_briefing_runs WHERE run_id=?", (run_id,)
                ).fetchone()
                if completed != ("COMPLETED",):
                    raise RuntimeError("shadow ledger did not reach COMPLETED")

        items = tuple(_item_dict(con, row) for row in events)
        report = _report_payload(lower, upper, generated, report_id, items)
        artifacts = compute_artifacts(root, report)
        con.execute("BEGIN IMMEDIATE")
        try:
            _reconcile_report_events(con, report_id, items)
            cursor = con.execute(
                "UPDATE reports SET json_sha256=?,jsonl_sha256=?,markdown_sha256=?,"
                "manifest_sha256=?,generation_status='complete',delivery_state='dry_run',"
                "delivery_id=NULL WHERE report_id=? AND generation_status='generating' "
                "AND json_sha256 IS NULL AND jsonl_sha256 IS NULL AND markdown_sha256 IS NULL "
                "AND manifest_sha256 IS NULL AND delivery_state='dry_run' AND delivery_id IS NULL",
                (
                    artifacts.json_sha256,
                    artifacts.jsonl_sha256,
                    artifacts.markdown_sha256,
                    artifacts.manifest_sha256,
                    report_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ArtifactMismatch("report finalization lost generating-row authority")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        completed_row = _existing(con, lower, upper)
        if completed_row is None:
            raise RuntimeError("completed report row disappeared")
        verified, count = _verify_complete(con, completed_row, root, lower, upper)
        return ReportRunResult(
            report_id,
            lower,
            upper,
            "complete",
            verified,
            False,
            count,
            excluded_count,
        )
    finally:
        con.close()
