"""Read-only Run 9 quality metrics aggregation and public-safe JSON CLI."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Mapping, Sequence

from .schema_v9 import validate_v9

RECEIPT_FIELDS = (
    "candidates_returned",
    "canonical_url_total",
    "canonical_url_covered",
    "canonical_url_missing",
    "canonical_url_non_article",
    "date_evidence_total",
    "date_evidence_covered",
    "date_evidence_source",
    "date_evidence_metadata",
    "date_evidence_missing",
    "date_evidence_unparseable",
    "stale_rejection_count",
    "exact_duplicate_count",
    "rewrite_count",
    "distinct_event_count",
    "material_update_count",
    "subject_relevance_reject_count",
    "cross_subject_collision_count",
    "delivered_event_version_repeat_count",
    "model_fallback_count",
    "model_malformed_count",
    "query_error_count",
    "transport_error_count",
)


@dataclass(frozen=True, slots=True)
class ReceiptMetrics:
    candidates_returned: int
    canonical_url_total: int
    canonical_url_covered: int
    canonical_url_missing: int
    canonical_url_non_article: int
    date_evidence_total: int
    date_evidence_covered: int
    date_evidence_source: int
    date_evidence_metadata: int
    date_evidence_missing: int
    date_evidence_unparseable: int
    stale_rejection_count: int
    exact_duplicate_count: int
    rewrite_count: int
    distinct_event_count: int
    material_update_count: int
    subject_relevance_reject_count: int
    cross_subject_collision_count: int
    delivered_event_version_repeat_count: int
    model_fallback_count: int
    model_malformed_count: int
    query_error_count: int
    transport_error_count: int

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            if type(value) is not int or value < 0:
                raise ValueError(f"receipt metric {field.name} must be a non-negative integer")
        if (
            self.canonical_url_covered
            + self.canonical_url_missing
            + self.canonical_url_non_article
            != self.canonical_url_total
        ):
            raise ValueError("canonical URL coverage counters must sum to their total")
        if self.date_evidence_source + self.date_evidence_metadata != self.date_evidence_covered:
            raise ValueError("date evidence source and metadata counts must sum to covered")
        if (
            self.date_evidence_covered
            + self.date_evidence_missing
            + self.date_evidence_unparseable
            != self.date_evidence_total
        ):
            raise ValueError("date evidence coverage counters must sum to their total")

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "ReceiptMetrics":
        if not isinstance(value, Mapping) or set(value) != set(RECEIPT_FIELDS):
            raise ValueError("receipt metrics must contain the exact keys")
        return cls(**{name: value[name] for name in RECEIPT_FIELDS})  # type: ignore[arg-type]

    def to_mapping(self) -> dict[str, int]:
        return {name: int(getattr(self, name)) for name in RECEIPT_FIELDS}


@dataclass(frozen=True, slots=True)
class UrlCoverage:
    total: int
    covered: int
    missing: int
    non_article: int | None

    def to_mapping(self) -> dict[str, int | None]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DateEvidenceCoverage:
    total: int
    covered: int
    source: int
    metadata: int
    missing: int
    unparseable: int | None

    def to_mapping(self) -> dict[str, int | None]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class QualityAuditReport:
    candidates_returned: int
    canonical_url_coverage: UrlCoverage
    date_evidence_coverage: DateEvidenceCoverage
    stale_rejection_count: int
    exact_duplicate_count: int
    rewrite_count: int
    distinct_event_count: int
    material_update_count: int
    pending_investigation_count: int
    provenance_unknown_count: int
    event_versions_by_state: dict[str, int]
    subject_relevance_reject_count: int | None
    cross_subject_collision_count: int
    report_events_by_subject: dict[str, int]
    delivered_event_version_repeat_count: int
    model_fallback_count: int | None
    model_malformed_count: int | None
    delivery_state_counts: dict[str, dict[str, int]]
    query_error_count: int
    transport_error_count: int | None
    unavailable_metrics: tuple[str, ...]
    absent_database_persistence: tuple[str, ...]
    sources: dict[str, str]
    integrity_check: str
    foreign_key_violation_count: int


def _scalar(connection: sqlite3.Connection, sql: str, params: tuple[object, ...] = ()) -> int:
    row = connection.execute(sql, params).fetchone()
    return 0 if row is None or row[0] is None else int(row[0])


def _group_counts(connection: sqlite3.Connection, table: str, column: str) -> dict[str, int]:
    rows = connection.execute(
        f"SELECT {column},COUNT(*) FROM {table} GROUP BY {column} ORDER BY {column}"
    ).fetchall()
    return {str(key): int(count) for key, count in rows}


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _semantic_counts(connection: sqlite3.Connection) -> tuple[dict[str, int], int]:
    counts = {"rewrite": 0, "distinct_event": 0, "material_update": 0}
    collisions = 0
    for (raw,) in connection.execute("SELECT reason FROM decisions ORDER BY id"):
        try:
            payload = json.loads(str(raw))
        except json.JSONDecodeError:
            continue
        if type(payload) is not dict:
            continue
        semantic = payload.get("semantic_decision")
        if semantic in counts:
            counts[str(semantic)] += 1
        reasons = payload.get("semantic_reasons", ())
        if payload.get("subject_suppressed") is True or (
            isinstance(reasons, list) and "subject_collision_suppressed" in reasons
        ):
            collisions += 1
    return counts, collisions


def _accepted_item_coverage(connection: sqlite3.Connection) -> tuple[UrlCoverage, DateEvidenceCoverage]:
    total = _scalar(connection, "SELECT COUNT(*) FROM source_items")
    covered = _scalar(
        connection,
        "SELECT COUNT(*) FROM source_items WHERE canonical_url IS NOT NULL AND trim(canonical_url) <> ''",
    )
    missing = total - covered
    source = _scalar(
        connection,
        "SELECT COUNT(*) FROM source_items WHERE publication_evidence='source'",
    )
    metadata = _scalar(
        connection,
        "SELECT COUNT(*) FROM source_items WHERE publication_evidence='metadata'",
    )
    date_covered = source + metadata
    return (
        UrlCoverage(total, covered, missing, None),
        DateEvidenceCoverage(total, date_covered, source, metadata, total - date_covered, None),
    )


def audit_database(
    path: str | Path,
    receipts: ReceiptMetrics | None = None,
) -> QualityAuditReport:
    """Audit one schema-v9 database without writing or exposing payload content."""
    resolved = Path(path).resolve()
    connection = sqlite3.connect(f"{resolved.as_uri()}?mode=ro", uri=True, timeout=5.0)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA query_only=ON")
        validate_v9(connection)
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        fk_count = len(connection.execute("PRAGMA foreign_key_check").fetchall())
        if integrity != "ok":
            raise ValueError("database integrity check failed")
        if fk_count:
            raise ValueError("database has foreign-key violations")

        db_candidates = _scalar(connection, "SELECT COALESCE(SUM(returned_count),0) FROM query_attempts")
        db_stale = _scalar(connection, "SELECT COALESCE(SUM(stale_count),0) FROM query_attempts")
        db_duplicates = _scalar(connection, "SELECT COALESCE(SUM(duplicate_count),0) FROM query_attempts")
        db_query_errors = _scalar(connection, "SELECT COALESCE(SUM(error_count),0) FROM query_attempts")
        semantic, db_collisions = _semantic_counts(connection)
        db_url, db_date = _accepted_item_coverage(connection)
        pending = _scalar(
            connection,
            "SELECT COUNT(*) FROM investigations WHERE state IN ('pending','running')",
        )
        provenance_unknown = _scalar(
            connection,
            "SELECT COUNT(*) FROM source_item_provenance WHERE classification_reason='unknown_publisher'",
        )
        event_states = {state: 0 for state in ("rejected", "unverified", "verified", "watchlist")}
        event_states.update(_group_counts(connection, "event_versions", "verification_state"))
        by_subject = _group_counts(connection, "report_events", "section")
        shadow_repeats = (
            _scalar(
                connection,
                "SELECT COUNT(*) FROM shadow_briefing_run_events WHERE event_status='ALREADY_SEEN'",
            )
            if _table_exists(connection, "shadow_briefing_run_events")
            else 0
        )
        delivery_counts = {
            "legacy_reports": _group_counts(connection, "reports", "delivery_state"),
            "legacy_deliveries": _group_counts(connection, "report_deliveries", "state"),
            "subject_outbox": _group_counts(connection, "subject_delivery_outbox", "state"),
            "subject_attempts": _group_counts(connection, "subject_delivery_attempts", "state"),
        }
        has_generation_receipts = _table_exists(
            connection, "subject_generation_receipts"
        )
        db_model_fallback = (
            _scalar(
                connection,
                "SELECT COALESCE(SUM(fallback_count),0) FROM subject_generation_receipts",
            )
            if has_generation_receipts
            else None
        )
        db_model_malformed = (
            _scalar(
                connection,
                "SELECT COALESCE(SUM(malformed_count),0) FROM subject_generation_receipts",
            )
            if has_generation_receipts
            else None
        )
        db_transport_errors = (
            _scalar(
                connection,
                "SELECT COALESCE(SUM(transport_error_count),0) FROM subject_generation_receipts",
            )
            if has_generation_receipts
            else None
        )

        unavailable: list[str] = []
        sources: dict[str, str] = {
            "pending_investigation_count": "db",
            "provenance_unknown_count": "db",
            "event_versions_by_state": "db",
            "report_events_by_subject": "db",
            "delivery_state_counts": "db",
        }
        if receipts is None:
            candidates = db_candidates
            url = db_url
            date = db_date
            stale = db_stale
            duplicates = db_duplicates
            rewrite = semantic["rewrite"]
            distinct = semantic["distinct_event"]
            material = semantic["material_update"]
            subject_rejects = None
            collisions = db_collisions
            repeats = shadow_repeats
            model_fallback = db_model_fallback
            model_malformed = db_model_malformed
            query_errors = db_query_errors
            transport_errors = db_transport_errors
            unavailable.extend(
                (
                    "canonical_url_non_article",
                    "date_evidence_unparseable",
                    "subject_relevance_reject_count",
                )
            )
            if not has_generation_receipts:
                unavailable.extend(
                    (
                        "model_fallback_count",
                        "model_malformed_count",
                        "transport_error_count",
                    )
                )
            for name in (
                "candidates_returned", "stale_rejection_count", "exact_duplicate_count",
                "rewrite_count", "distinct_event_count", "material_update_count",
                "cross_subject_collision_count", "delivered_event_version_repeat_count",
                "query_error_count",
            ):
                sources[name] = "db"
            sources["canonical_url_coverage"] = "db_partial"
            sources["date_evidence_coverage"] = "db_partial"
            if has_generation_receipts:
                sources["model_fallback_count"] = "db"
                sources["model_malformed_count"] = "db"
                sources["transport_error_count"] = "db"
            for name in unavailable:
                sources[name] = "unavailable"
        else:
            candidates = receipts.candidates_returned
            url = UrlCoverage(
                receipts.canonical_url_total,
                receipts.canonical_url_covered,
                receipts.canonical_url_missing,
                receipts.canonical_url_non_article,
            )
            date = DateEvidenceCoverage(
                receipts.date_evidence_total,
                receipts.date_evidence_covered,
                receipts.date_evidence_source,
                receipts.date_evidence_metadata,
                receipts.date_evidence_missing,
                receipts.date_evidence_unparseable,
            )
            stale = receipts.stale_rejection_count
            duplicates = receipts.exact_duplicate_count
            rewrite = receipts.rewrite_count
            distinct = receipts.distinct_event_count
            material = receipts.material_update_count
            subject_rejects = receipts.subject_relevance_reject_count
            collisions = receipts.cross_subject_collision_count
            repeats = receipts.delivered_event_version_repeat_count
            model_fallback = receipts.model_fallback_count
            model_malformed = receipts.model_malformed_count
            query_errors = db_query_errors + receipts.query_error_count
            transport_errors = receipts.transport_error_count
            for name in (
                "candidates_returned", "canonical_url_coverage", "date_evidence_coverage",
                "stale_rejection_count", "exact_duplicate_count", "rewrite_count",
                "distinct_event_count", "material_update_count",
                "subject_relevance_reject_count", "cross_subject_collision_count",
                "delivered_event_version_repeat_count", "model_fallback_count",
                "model_malformed_count", "transport_error_count",
            ):
                sources[name] = "receipt"
            sources["query_error_count"] = "db+receipt"

        return QualityAuditReport(
            candidates_returned=candidates,
            canonical_url_coverage=url,
            date_evidence_coverage=date,
            stale_rejection_count=stale,
            exact_duplicate_count=duplicates,
            rewrite_count=rewrite,
            distinct_event_count=distinct,
            material_update_count=material,
            pending_investigation_count=pending,
            provenance_unknown_count=provenance_unknown,
            event_versions_by_state=event_states,
            subject_relevance_reject_count=subject_rejects,
            cross_subject_collision_count=collisions,
            report_events_by_subject=by_subject,
            delivered_event_version_repeat_count=repeats,
            model_fallback_count=model_fallback,
            model_malformed_count=model_malformed,
            delivery_state_counts=delivery_counts,
            query_error_count=query_errors,
            transport_error_count=transport_errors,
            unavailable_metrics=tuple(sorted(unavailable)),
            absent_database_persistence=(
                "canonical URL and date coverage aggregates",
                "normalized semantic decision metrics",
                "subject relevance and collision receipts",
                "investigation per-query error details",
            ) + (() if has_generation_receipts else (
                "summarizer fallback and malformed aggregates",
            )),
            sources=sources,
            integrity_check=integrity,
            foreign_key_violation_count=fk_count,
        )
    finally:
        connection.close()


def _payload(report: QualityAuditReport) -> dict[str, object]:
    return {
        "schema": "news-quality-audit-v1",
        "metrics": {
            "candidates_returned": report.candidates_returned,
            "url_coverage": report.canonical_url_coverage.to_mapping(),
            "date_coverage": report.date_evidence_coverage.to_mapping(),
            "stale_rejection_count": report.stale_rejection_count,
            "exact_duplicate_count": report.exact_duplicate_count,
            "rewrite_count": report.rewrite_count,
            "distinct_event_count": report.distinct_event_count,
            "material_update_count": report.material_update_count,
            "pending_investigation_count": report.pending_investigation_count,
            "provenance_unknown_count": report.provenance_unknown_count,
            "event_versions_by_state": report.event_versions_by_state,
            "subject_relevance_reject_count": report.subject_relevance_reject_count,
            "cross_subject_collision_count": report.cross_subject_collision_count,
            "report_events_by_subject": report.report_events_by_subject,
            "delivered_event_version_repeat_count": report.delivered_event_version_repeat_count,
            "model_fallback_count": report.model_fallback_count,
            "model_malformed_count": report.model_malformed_count,
            "delivery_state_counts": report.delivery_state_counts,
            "query_error_count": report.query_error_count,
            "transport_error_count": report.transport_error_count,
        },
        "sources": dict(sorted(report.sources.items())),
        "unavailable_metrics": list(report.unavailable_metrics),
        "absent_database_persistence": list(report.absent_database_persistence),
        "integrity": {
            "integrity_check": report.integrity_check,
            "foreign_key_violation_count": report.foreign_key_violation_count,
        },
    }


def render_public_report(report: QualityAuditReport) -> str:
    """Return deterministic count-only JSON suitable for publication."""
    return json.dumps(_payload(report), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m news_pipeline.quality_audit")
    parser.add_argument("--db", required=True)
    parser.add_argument("--receipts")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        receipts = None
        if args.receipts:
            raw = json.loads(Path(args.receipts).read_text(encoding="utf-8"))
            receipts = ReceiptMetrics.from_mapping(raw)
        report = audit_database(args.db, receipts)
    except (OSError, ValueError, TypeError, sqlite3.Error, json.JSONDecodeError):
        print("quality audit failed", file=sys.stderr)
        return 2
    print(render_public_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "RECEIPT_FIELDS",
    "DateEvidenceCoverage",
    "QualityAuditReport",
    "ReceiptMetrics",
    "UrlCoverage",
    "audit_database",
    "main",
    "render_public_report",
]
