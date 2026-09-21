"""Explicit-gate job entry points used by Phase 6 disabled wrappers."""
from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import os
import re
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable


_FACTORY_RE = re.compile(
    r"news_private(?:\.[A-Za-z_][A-Za-z0-9_]*)+:[A-Za-z_][A-Za-z0-9_]*\Z"
)


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

    delivery = sub.add_parser("daily-deliver")
    _add_common_db(delivery)
    delivery.add_argument("--artifact-root", required=True)
    delivery.add_argument("--report-id", required=True)
    delivery.add_argument("--config")
    delivery.add_argument("--telegram-api-base", default="https://api.telegram.org")
    delivery.add_argument("--enable-live-delivery", action="store_true")
    delivery.add_argument("--retry-failed", action="store_true")

    health = sub.add_parser("health")
    health.add_argument("--db", required=True)

    migrate_v10 = sub.add_parser("migrate-v10")
    _add_common_db(migrate_v10)
    migrate_v10.add_argument("--applied-at", required=True)
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


def _load_subject_summarizer() -> Any | None:
    binding = os.environ.get("NEWS_SUBJECT_SUMMARIZER_FACTORY", "").strip()
    if not binding:
        return None
    if _FACTORY_RE.fullmatch(binding) is None:
        raise ValueError(
            "NEWS_SUBJECT_SUMMARIZER_FACTORY must be news_private.module.path:function"
        )
    module_name, factory_name = binding.split(":", 1)
    module = importlib.import_module(module_name)
    factory = getattr(module, factory_name, None)
    if not callable(factory):
        raise TypeError("subject summarizer factory is not callable")
    summarizer = factory()
    if not callable(getattr(summarizer, "summarize_subject", None)):
        raise TypeError("subject summarizer does not implement summarize_subject")
    for name in ("model_call_count", "cache_hit_count"):
        value = getattr(summarizer, name, None)
        if type(value) is not int or isinstance(value, bool) or value < 0:
            raise TypeError(f"subject summarizer {name} must be a non-negative int")
    return summarizer


def _validate_daily_report_schema(db_path: str | Path) -> None:
    from .schema_v10 import validate_v10

    connection = sqlite3.connect(db_path, isolation_level=None, timeout=5.0)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        validate_v10(connection)
    except (ValueError, sqlite3.Error) as exc:
        raise ValueError(f"schema v10 migration is required: {exc}") from exc
    finally:
        connection.close()


def _run_daily_report(args: argparse.Namespace) -> int:
    try:
        as_of, prior = _report_args(args)
        if not Path(args.db).is_file() or not Path(args.artifact_root).is_dir():
            raise ValueError("--db must be a file and --artifact-root must be a directory")
        _validate_daily_report_schema(args.db)
        prior = _resolve_report_prior(args.db, as_of, prior)
        from .report_builder import run_report
        summarizer = _load_subject_summarizer()
        model_calls_before = (
            summarizer.model_call_count if summarizer is not None else 0
        )
        kwargs: dict[str, Any] = {"last_completed_upper_utc": prior}
        if summarizer is not None:
            kwargs["summarizer"] = summarizer
        result = run_report(
            Path(args.db), Path(args.artifact_root), as_of, **kwargs
        )
        network_used = bool(
            summarizer is not None
            and summarizer.model_call_count > model_calls_before
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
        "delivery_state": "not_attempted",
        "network_used": network_used,
        "replayed": False,
        "message_count": 0,
    })
    return 0


def _run_daily_deliver(args: argparse.Namespace) -> int:
    """Consume one existing report; never run report/model generation."""
    try:
        if not Path(args.db).is_file() or not Path(args.artifact_root).is_dir():
            raise ValueError("--db must be a file and --artifact-root must be a directory")
        if type(args.report_id) is not str or not args.report_id.strip():
            raise ValueError("--report-id must be non-empty")
        from .delivery import deliver_report
        delivery = deliver_report(
            args.db,
            args.artifact_root,
            args.report_id,
            enable_live=args.enable_live_delivery,
            config_path=args.config,
            api_base=args.telegram_api_base,
            retry_failed=args.retry_failed,
        )
    except Exception as exc:
        _json({"error": {"type": "runtime", "message": str(exc)}})
        return 1
    _json({
        "job": "daily-deliver",
        "state": "completed",
        "report_id": delivery.report_id,
        "delivery_state": delivery.state,
        "network_used": delivery.network_used,
        "replayed": delivery.replayed,
        "message_count": len(delivery.message_ids),
    })
    return 0


def _run_migrate_v10(args: argparse.Namespace) -> int:
    connection: sqlite3.Connection | None = None
    try:
        if not Path(args.db).is_file():
            raise ValueError("--db must be a file")
        applied_at = _parse_utc(args.applied_at, name="--applied-at")
        from .schema_v10 import migrate_v10

        connection = sqlite3.connect(args.db, isolation_level=None, timeout=5.0)
        applied = migrate_v10(
            connection,
            applied_at.isoformat(timespec="seconds").replace("+00:00", "Z"),
        )
    except Exception as exc:
        _json({"error": {"type": "runtime", "message": str(exc)}})
        return 1
    finally:
        if connection is not None:
            connection.close()
    _json({
        "job": "migrate-v10",
        "state": "completed",
        "schema_version": 10,
        "applied": applied,
        "network_used": False,
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
    if args.job == "daily-deliver":
        return _run_daily_deliver(args)
    if args.job == "migrate-v10":
        return _run_migrate_v10(args)
    if args.job == "health":
        from .news_health import health_snapshot
        _json(health_snapshot(args.db))
        return 0
    raise AssertionError(args.job)


if __name__ == "__main__":
    raise SystemExit(main())
