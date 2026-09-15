"""One-shot, shadow-only report CLI."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


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
    try:
        from .report_builder import run_report
        result = run_report(db, root, as_of, last_completed_upper_utc=prior)
    except Exception as exc:
        from .report_artifacts import ArtifactMismatch
        kind = "artifact_mismatch" if isinstance(exc, ArtifactMismatch) else "runtime"
        return _fail(kind, str(exc), 1)
    artifact = result.artifact_result
    output = {"report_id": result.report_id, "window_start": result.window_start, "window_end": result.window_end, "generation_status": result.generation_status, "was_replayed": result.was_replayed, "included_count": result.included_count, "excluded_count": result.excluded_count}
    if artifact is not None:
        output["artifacts"] = {"json_sha256": artifact.json_sha256, "jsonl_sha256": artifact.jsonl_sha256, "markdown_sha256": artifact.markdown_sha256, "manifest_sha256": artifact.manifest_sha256}
    print(json.dumps(output, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
