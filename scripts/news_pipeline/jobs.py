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
from typing import Any, Callable


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m news_pipeline.jobs")
    sub = parser.add_subparsers(dest="job", required=True)

    tick = sub.add_parser("tick")
    _add_common_db(tick)
    tick.add_argument("--sources")
    tick.add_argument("--topics")
    tick.add_argument("--policy")
    tick.add_argument("--provenance")
    tick.add_argument("--run-started-at")
    tick.add_argument("--source-id", action="append", dest="source_ids")
    tick.add_argument("--max-queries", type=int)
    tick.add_argument("--enable-network", action="store_true")
    tick.add_argument("--control-db")

    process = sub.add_parser("process")
    _add_common_db(process)
    process.add_argument("--evaluated-at")
    process.add_argument("--history-db")
    process.add_argument("--sources")
    process.add_argument("--topics")
    process.add_argument("--policy")
    process.add_argument("--max-items", type=int, default=500)
    process.add_argument("--enable-network", action="store_true")

    investigate = sub.add_parser("investigate")
    _add_common_db(investigate)
    investigate.add_argument("--candidate-id", required=True)
    investigate.add_argument("--feed-lane-id", required=True)
    investigate.add_argument("--query-plan-id", required=True)
    investigate.add_argument("--investigation-id", required=True)
    investigate.add_argument("--category", required=True)
    investigate.add_argument("--round-number", type=int, default=0)
    investigate.add_argument("--evaluated-at", required=True)
    investigate.add_argument("--enable-network", action="store_true")
    investigate.add_argument("--control-db")
    investigate.add_argument("--control-task-id")
    investigate.add_argument("--control-owner")
    investigate.add_argument("--control-generation", type=int)

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
            provenance_path=args.provenance,
            max_queries=args.max_queries,
            transport_factory=transport_factory,
        )
        investigations_enqueued: tuple[tuple[str, bool], ...] = ()
        if args.control_db:
            from .investigation import enqueue_candidate_investigations
            from news_container.control_store import open as open_control

            control = open_control(args.control_db)
            try:
                investigations_enqueued = enqueue_candidate_investigations(
                    args.db,
                    control,
                    due_slot_utc=args.run_started_at,
                )
            finally:
                control.close()
    except Exception as exc:
        _json({"error": {"type": "runtime", "message": str(exc)}})
        return 1
    _json({
        "job": "tick",
        "state": "completed",
        "network_used": True,
        "report": report.__dict__ if hasattr(report, "__dict__") else str(report),
        "investigations_enqueued": sum(
            1 for _task_id, created in investigations_enqueued if created
        ),
    })
    return 0


def _run_process(args: argparse.Namespace) -> int:
    if not args.enable_network:
        return _disabled("process", "activation_gate_not_enabled")
    if not args.evaluated_at:
        _json({"error": {"type": "arguments", "message": "process requires --evaluated-at"}})
        return 2
    from .process_runner import process_news
    try:
        config_paths = (args.sources, args.topics, args.policy)
        if any(config_paths) and not all(config_paths):
            raise ValueError("process requires --sources, --topics, and --policy together")
        query_policies = None
        if all(config_paths):
            from .policies import query_policies_from_subject_policies
            from .source_registry import load_registry

            config = load_registry(args.sources, args.topics, args.policy)
            query_policies = query_policies_from_subject_policies(config.subjects_by_id)
        report = process_news(
            args.db,
            args.evaluated_at,
            history_db_path=args.history_db,
            query_policies=query_policies,
            max_items=args.max_items,
        )
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


def _run_investigate(args: argparse.Namespace) -> int:
    if not args.enable_network:
        return _disabled("investigate", "activation_gate_not_enabled")
    try:
        from .investigation import investigation_id, run_investigation

        expected_id = investigation_id(args.candidate_id, args.feed_lane_id, args.query_plan_id)
        if args.investigation_id != expected_id:
            raise ValueError("--investigation-id is not deterministic for the candidate")
        transport_factory = None
        if os.environ.get("NEWS_CONTAINER_MODE") == "1":
            from news_container.broker_client import broker_transport_factory

            transport_factory = broker_transport_factory
        fence_values = (
            args.control_db,
            args.control_task_id,
            args.control_owner,
            args.control_generation,
        )
        if not all(value is not None for value in fence_values):
            raise ValueError(
                "investigate requires --control-db, --control-task-id, "
                "--control-owner, and --control-generation"
            )
        lease_check: Callable[[], None] | None = None
        if all(value is not None for value in fence_values):
            from news_container.control_store import assert_claim

            def _check_lease() -> None:
                uri = f"{Path(args.control_db).resolve().as_uri()}?mode=ro"
                control = sqlite3.connect(uri, uri=True, timeout=5.0)
                control.row_factory = sqlite3.Row
                try:
                    assert_claim(
                        control,
                        task_id=args.control_task_id,
                        owner=args.control_owner,
                        generation=args.control_generation,
                    )
                finally:
                    control.close()

            lease_check = _check_lease

        result = run_investigation(
            args.db,
            candidate_id=args.candidate_id,
            feed_lane_id=args.feed_lane_id,
            query_plan_id=args.query_plan_id,
            category=args.category,
            evaluated_at=args.evaluated_at,
            round_number=args.round_number,
            transport_factory=transport_factory,
            lease_check=lease_check,
        )
    except Exception as exc:
        _json({"error": {"type": "runtime", "message": str(exc)}})
        return 1
    _json({
        "job": "investigate",
        "state": result.job.state,
        "network_used": result.network_used,
        "investigation_id": result.job.investigation_id,
        "candidate_id": result.job.candidate_id,
        "feed_lane_id": result.job.feed_lane_id,
        "round_number": result.job.round_number,
        "terminal_state": result.job.terminal_state,
        "targeted_query_count": len(result.targeted_queries),
        "evidence_count": sum(item.returned_count for item in result.query_results),
    })
    return 0 if result.job.state == "complete" else 1


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
    if args.job == "investigate":
        return _run_investigate(args)
    if args.job in {"daily-close", "daily-report"}:
        return _run_daily_report(args)
    if args.job == "health":
        from .news_health import health_snapshot
        _json(health_snapshot(args.db))
        return 0
    raise AssertionError(args.job)


if __name__ == "__main__":
    raise SystemExit(main())
