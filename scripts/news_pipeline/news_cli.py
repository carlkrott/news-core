"""One-shot, shadow-only report CLI."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


_JOB = "shadow-report"


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        self.exit(2, json.dumps({"error": {"type": "arguments", "message": message}}, separators=(",", ":")) + "\n")


def _parse_utc(value: str) -> datetime:
    if not value.endswith("Z"):
        raise ValueError("timestamp must end in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError("invalid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("timestamp must be UTC")
    return parsed.astimezone(timezone.utc)


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(prog="python -m news_pipeline.news_cli")
    parser.add_argument("--db", required=True)
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument("--as-of-utc", required=True)
    parser.add_argument("--prior-upper-utc")
    return parser


def _fail(kind: str, message: str, code: int = 1) -> int:
    print(json.dumps({"error": {"type": kind, "message": message}}, separators=(",", ":")), file=sys.stderr)
    return code


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        as_of = _parse_utc(args.as_of_utc)
    except ValueError as exc:
        return _fail("input", f"--as-of-utc: {exc}")
    try:
        prior = _parse_utc(args.prior_upper_utc) if args.prior_upper_utc else None
        if prior is not None and prior >= as_of:
            return _fail("input", "--prior-upper-utc must be strictly before --as-of-utc")
        db = Path(args.db)
        root = Path(args.artifact_root)
        if not db.is_file():
            return _fail("input", f"--db is not an existing file: {args.db!r}")
        if not root.is_dir():
            return _fail("input", f"--artifact-root is not an existing directory: {args.artifact_root!r}")
    except ValueError as exc:
        return _fail("input", f"--prior-upper-utc: {exc}")
    from .observability import iso_ms, model_call_counts, utc_now
    from .subject_model_transport import recording_calls

    started = utc_now()
    with recording_calls() as model_calls:
        try:
            from .report_builder import run_report
            result = run_report(db, root, as_of, last_completed_upper_utc=prior)
        except Exception as exc:
            from .report_artifacts import ArtifactMismatch
            kind = "artifact_mismatch" if isinstance(exc, ArtifactMismatch) else "runtime"
            _observe(db, as_of=args.as_of_utc, started=started, exit_code=1, error=exc, payload={"error": {"type": kind}}, model_calls=model_calls)
            return _fail(kind, str(exc), 1)
    artifact = result.artifact_result
    output = {"report_id": result.report_id, "window_start": result.window_start, "window_end": result.window_end, "generation_status": result.generation_status, "was_replayed": result.was_replayed, "included_count": result.included_count, "excluded_count": result.excluded_count}
    if artifact is not None:
        output["artifacts"] = {"json_sha256": artifact.json_sha256, "jsonl_sha256": artifact.jsonl_sha256, "markdown_sha256": artifact.markdown_sha256, "manifest_sha256": artifact.manifest_sha256}
    finished = utc_now()
    output["model_calls"] = model_call_counts(model_calls)
    output["started_at"] = iso_ms(started)
    output["finished_at"] = iso_ms(finished)
    output["duration_ms"] = max(0, round((finished - started).total_seconds() * 1000))
    _observe(db, as_of=args.as_of_utc, started=started, exit_code=0, error=None, payload={**output, "state": "completed"}, model_calls=model_calls, finished=finished)
    print(json.dumps(output, separators=(",", ":")))
    return 0


def _observe(db: Path, *, as_of: str, started: datetime, exit_code: int, error: BaseException | None,
             payload: dict, model_calls: list, finished: datetime | None = None) -> None:
    """Best-effort job_runs + model_calls rows for the shadow report (never raises)."""
    from .observability import record_job_run, record_model_calls, utc_now

    record_model_calls(db, job=_JOB, records=model_calls)
    record_job_run(
        db,
        job=_JOB,
        slot_utc=as_of,
        started_at=started,
        finished_at=finished or utc_now(),
        exit_code=exit_code,
        state="completed" if error is None else "failed",
        error_class=None if error is None else type(error).__name__,
        payload=payload,
    )


if __name__ == "__main__":
    raise SystemExit(main())
