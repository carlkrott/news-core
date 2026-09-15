"""Explicit-gate job entry points used by Phase 6 disabled wrappers."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m news_pipeline.jobs")
    sub = parser.add_subparsers(dest="job", required=True)

    tick = sub.add_parser("tick")
    _add_common_db(tick)
    tick.add_argument("--sources")
    tick.add_argument("--topics")
    tick.add_argument("--policy")
    tick.add_argument("--run-started-at")
    tick.add_argument("--source-id", action="append", dest="source_ids")
    tick.add_argument("--max-queries", type=int)
    tick.add_argument("--enable-network", action="store_true")

    process = sub.add_parser("process")
    _add_common_db(process)
    process.add_argument("--evaluated-at")
    process.add_argument("--history-db")
    process.add_argument("--max-items", type=int, default=500)
    process.add_argument("--enable-network", action="store_true")

    for name in ("daily-close", "daily-report"):
        report = sub.add_parser(name)
        _add_common_db(report)
        report.add_argument("--artifact-root", required=True)
        report.add_argument("--as-of-utc", required=True)
        report.add_argument("--prior-upper-utc")
        report.add_argument("--config")
        report.add_argument("--telegram-api-base", default="https://api.telegram.org")
        report.add_argument("--enable-live-delivery", action="store_true")
        report.add_argument("--retry-failed", action="store_true")

    health = sub.add_parser("health")
    health.add_argument("--db", required=True)
    return parser


def _add_common_db(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--db", required=True)


def _parse_utc(value: str, *, name: str) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise ValueError(f"{name} must end in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{name} is not valid ISO-8601") from exc
    if parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{name} must be UTC")
    return parsed.astimezone(UTC)


def _json(value: dict[str, Any]) -> None:
    print(json.dumps(value, ensure_ascii=False, separators=(",", ":")))


def _disabled(job: str, reason: str) -> int:
    _json({"job": job, "state": "disabled", "network_used": False, "reason": reason})
    return 0


def _run_tick(args: argparse.Namespace) -> int:
    if not args.enable_network:
        return _disabled("tick", "network_gate_not_enabled")
    required = {"sources": args.sources, "topics": args.topics, "policy": args.policy, "run_started_at": args.run_started_at}
    missing = sorted(name for name, value in required.items() if not value)
    if missing:
        _json({"error": {"type": "arguments", "message": f"tick requires: {', '.join(missing)}"}})
        return 2
    from .ingest_runner import run_ingest_sync
    try:
        transport_factory = None
        if os.environ.get("NEWS_CONTAINER_MODE") == "1":
            from news_container.broker_client import broker_transport_factory

            transport_factory = broker_transport_factory
        report = run_ingest_sync(
            args.db, args.sources, args.topics, args.policy, args.run_started_at,
            source_ids=tuple(args.source_ids) if args.source_ids else None,
            max_queries=args.max_queries,
            transport_factory=transport_factory,
        )
    except Exception as exc:
        _json({"error": {"type": "runtime", "message": str(exc)}})
        return 1
    _json({"job": "tick", "state": "completed", "network_used": True, "report": report.__dict__ if hasattr(report, "__dict__") else str(report)})
    return 0


def _run_process(args: argparse.Namespace) -> int:
    if not args.enable_network:
        return _disabled("process", "activation_gate_not_enabled")
    if not args.evaluated_at:
        _json({"error": {"type": "arguments", "message": "process requires --evaluated-at"}})
        return 2
    from .process_runner import process_news
    try:
        report = process_news(args.db, args.evaluated_at, history_db_path=args.history_db, max_items=args.max_items)
        from .event_store import process_phase4
        event_report = process_phase4(
            args.db,
            args.evaluated_at,
            max_items=args.max_items,
        )
    except Exception as exc:
        _json({"error": {"type": "runtime", "message": str(exc)}})
        return 1
    _json({
        "job": "process",
        "state": "completed",
        "network_used": False,
        "report": report.__dict__ if hasattr(report, "__dict__") else str(report),
        "event_report": event_report.__dict__ if hasattr(event_report, "__dict__") else str(event_report),
    })
    return 0


def _report_args(args: argparse.Namespace) -> tuple[datetime, datetime | None]:
    as_of = _parse_utc(args.as_of_utc, name="--as-of-utc")
    prior = _parse_utc(args.prior_upper_utc, name="--prior-upper-utc") if args.prior_upper_utc else None
    if prior is not None and prior >= as_of:
        raise ValueError("--prior-upper-utc must be strictly before --as-of-utc")
    return as_of, prior


def _resolve_report_prior(
    db_path: str | Path,
    as_of: datetime,
    explicit_prior: datetime | None,
) -> datetime | None:
    """Resolve the exact current-window replay or latest prior boundary."""
    from .briefing_contracts import compute_morning_window

    _lower, current_upper = compute_morning_window(as_of, None)
    connection = sqlite3.connect(
        f"file:{Path(db_path).resolve()}?mode=ro", uri=True, timeout=5.0
    )
    try:
        rows = connection.execute(
            """SELECT report_id,window_start,window_end
                 FROM reports WHERE generation_status='complete'"""
        ).fetchall()
    finally:
        connection.close()

    parsed = [
        (
            str(report_id),
            _parse_utc(str(window_start), name="reports.window_start"),
            _parse_utc(str(window_end), name="reports.window_end"),
        )
        for report_id, window_start, window_end in rows
    ]
    current = [row for row in parsed if row[2] == current_upper]
    if len(current) > 1:
        raise ValueError("multiple complete reports exist for the current morning boundary")
    if current:
        _report_id, current_lower, _current_end = current[0]
        if explicit_prior not in {None, current_lower, current_upper}:
            raise ValueError("--prior-upper-utc does not match the current complete report")
        return current_lower
    if explicit_prior is not None:
        if explicit_prior >= current_upper:
            raise ValueError("--prior-upper-utc reaches a missing current report boundary")
        return explicit_prior
    return max((row[2] for row in parsed if row[2] < current_upper), default=None)


def _run_daily_report(args: argparse.Namespace) -> int:
    try:
        as_of, prior = _report_args(args)
        if args.job == "daily-close" and args.enable_live_delivery:
            raise ValueError("daily-close cannot enable live delivery; use daily-report")
        if not Path(args.db).is_file() or not Path(args.artifact_root).is_dir():
            raise ValueError("--db must be a file and --artifact-root must be a directory")
        prior = _resolve_report_prior(args.db, as_of, prior)
        from .report_builder import run_report
        result = run_report(Path(args.db), Path(args.artifact_root), as_of, last_completed_upper_utc=prior)
        from .delivery import deliver_report
        delivery = deliver_report(
            args.db, args.artifact_root, result.report_id,
            enable_live=args.enable_live_delivery,
            config_path=args.config,
            api_base=args.telegram_api_base,
            retry_failed=args.retry_failed,
        )
    except Exception as exc:
        _json({"error": {"type": "runtime", "message": str(exc)}})
        return 1
    _json({
        "job": args.job,
        "state": "completed",
        "report_id": result.report_id,
        "generation_status": result.generation_status,
        "was_replayed": result.was_replayed,
        "delivery_state": delivery.state,
        "network_used": delivery.network_used,
        "replayed": delivery.replayed,
        "message_count": len(delivery.message_ids),
    })
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.job == "tick":
        return _run_tick(args)
    if args.job == "process":
        return _run_process(args)
    if args.job in {"daily-close", "daily-report"}:
        return _run_daily_report(args)
    if args.job == "health":
        from .news_health import health_snapshot
        _json(health_snapshot(args.db))
        return 0
    raise AssertionError(args.job)


if __name__ == "__main__":
    raise SystemExit(main())
