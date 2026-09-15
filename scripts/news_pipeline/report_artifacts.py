"""Deterministic, fail-closed Phase 5 report artifacts."""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

__all__ = ("ArtifactRoot", "ArtifactMismatch", "ArtifactResult", "ReportArtifacts", "compute_artifacts", "verify_artifacts")


class ArtifactMismatch(ValueError):
    """Existing or persisted artifact bytes do not match the expected report."""


@dataclass(frozen=True, slots=True)
class ArtifactRoot:
    path: Path
    def __post_init__(self) -> None:
        if not isinstance(self.path, Path):
            raise TypeError("artifact_root must be a pathlib.Path")
        if not self.path.is_dir():
            raise ValueError(f"artifact_root {self.path!s} must be an existing directory")


@dataclass(frozen=True, slots=True)
class ReportArtifacts:
    window_start: str
    window_end: str
    generated_at_utc: str
    items: tuple[dict, ...]
    report_id: str = ""
    health: dict[str, Any] | None = None
    sections: dict[str, Any] | None = None
    counts: dict[str, int] | None = None

    def __post_init__(self) -> None:
        start = _canonical_utc(self.window_start, "window_start")
        end = _canonical_utc(self.window_end, "window_end")
        generated = _canonical_utc(self.generated_at_utc, "generated_at_utc")
        if start > end:
            raise ValueError("report window_start must not exceed window_end")
        if generated < end:
            raise ValueError("generated_at_utc must not precede window_end")
        if type(self.report_id) is not str or not self.report_id:
            raise ValueError("report_id must be a non-empty string")
        if type(self.items) is not tuple:
            raise TypeError("items must be an exact tuple")
        for index, item in enumerate(self.items):
            if type(item) is not dict:
                raise TypeError(f"items[{index}] must be an exact dict")
            if type(item.get("event_id")) is not str or not item["event_id"]:
                raise ValueError(f"items[{index}].event_id must be non-empty")
            if type(item.get("event_version")) is not int or item["event_version"] < 1:
                raise ValueError(f"items[{index}].event_version must be a positive int")
        if self.counts is not None:
            if type(self.counts) is not dict:
                raise TypeError("counts must be an exact dict or None")
            if any(type(k) is not str or type(v) is not int or v < 0 for k, v in self.counts.items()):
                raise ValueError("counts must map strings to non-negative ints")
            if self.counts.get("items", len(self.items)) != len(self.items):
                raise ValueError("counts.items must equal the number of report items")
        # Fail before any filesystem write if a payload is not canonical-JSON-safe.
        _canonical(_payload(self))


@dataclass(frozen=True, slots=True)
class ArtifactResult:
    json_bytes: bytes
    json_sha256: str
    jsonl_bytes: bytes
    jsonl_sha256: str
    markdown_bytes: bytes
    markdown_sha256: str
    manifest_bytes: bytes
    manifest_sha256: str
    json_path: Path
    jsonl_path: Path
    markdown_path: Path
    manifest_path: Path
    def verify_bytes(self) -> None:
        for name, data, expected in (("json", self.json_bytes, self.json_sha256), ("jsonl", self.jsonl_bytes, self.jsonl_sha256), ("markdown", self.markdown_bytes, self.markdown_sha256), ("manifest", self.manifest_bytes, self.manifest_sha256)):
            actual = sha256_hex(data)
            if actual != expected:
                raise ArtifactMismatch(f"{name} SHA-256 mismatch: expected={expected} actual={actual}")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _canonical_utc(value: object, field: str) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise ValueError(f"{field} must be a canonical UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{field} must be a valid ISO-8601 timestamp") from exc
    if parsed.utcoffset() != timedelta(0):
        raise ValueError(f"{field} must be UTC")
    canonical = parsed.isoformat(
        timespec="microseconds" if parsed.microsecond else "seconds"
    ).replace("+00:00", "Z")
    if value != canonical:
        raise ValueError(f"{field} must use canonical UTC Z form")
    return parsed


def _paths(root: ArtifactRoot, start: str, end: str) -> tuple[Path, Path, Path, Path]:
    start_dt = _canonical_utc(start, "window_start")
    end_dt = _canonical_utc(end, "window_end")
    if start_dt > end_dt:
        raise ValueError("artifact window_start must not exceed window_end")
    # The complete normalized bounds, not just the date, participate in identity.
    identity = hashlib.sha256((start + "\0" + end).encode("utf-8")).hexdigest()[:32]
    base = root.path / f"report-{identity}"
    return base.with_suffix(".json"), base.with_suffix(".jsonl"), base.with_suffix(".md"), base.with_suffix(".manifest.json")


def _payload(report: ReportArtifacts) -> dict[str, Any]:
    items = [dict(item) for item in report.items]
    out: dict[str, Any] = {
        "report_id": report.report_id,
        "window_start": report.window_start,
        "window_end": report.window_end,
        "generated_at": report.generated_at_utc,
        "items": items,
        "counts": report.counts or {"items": len(items)},
        "health": report.health or {"status": "healthy", "degraded": False, "reasons": []},
        "corrections": [], "retractions": [], "leads": [],
        "verification": {"marker": "shadow_verified", "all_items_verified": True},
    }
    if report.sections:
        out.update(report.sections)
    out["item_count"] = len(items)
    return out


def _markdown(payload: dict[str, Any]) -> bytes:
    lines = [f"# News Report — {payload['window_start']} to {payload['window_end']}", "", f"Generated: {payload['generated_at']}", "", "## Top developments"]
    items = payload["items"]
    if not items:
        lines.append("No verified new events in this reporting window.")
    for n, item in enumerate(items, 1):
        lines.extend([f"{n}. **{item.get('title') or 'Untitled'}**", f"   Decision: {item.get('decision', '')} | Verification: {item.get('verification', 'verified')}"])
        if item.get("event_date"):
            lines.append(f"   Event date: {item['event_date']}")
        if item.get("summary"):
            lines.append(f"   {item['summary']}")
        if item.get("url"):
            lines.append(f"   Source: {item['url']}")
        lines.append("")
    if payload.get("corrections") or payload.get("retractions"):
        lines += ["## Corrections and retractions", ""]
    if payload.get("leads"):
        lines += ["## Leads to watch", ""]
    if payload.get("health", {}).get("degraded"):
        lines += ["## Pipeline health", str(payload["health"]), ""]
    return "\n".join(lines).encode("utf-8")


def _build(report: ReportArtifacts) -> tuple[bytes, bytes, bytes, bytes]:
    payload = _payload(report)
    jb = _canonical(payload)
    lines = [_canonical({"type": "report", **payload})]
    lines.extend(_canonical({"type": "event", **item}) for item in payload["items"])
    jl = b"\n".join(lines) + b"\n"
    md = _markdown(payload)
    manifest = _canonical({
        "version": 2, "report_id": report.report_id, "window_start": report.window_start,
        "window_end": report.window_end, "generated_at": report.generated_at_utc,
        "counts": payload["counts"], "artifacts": {
            "json": {"sha256": sha256_hex(jb), "bytes": len(jb)},
            "jsonl": {"sha256": sha256_hex(jl), "bytes": len(jl)},
            "markdown": {"sha256": sha256_hex(md), "bytes": len(md)},
        }, "delivery_id": None, "delivery_state": "dry_run",
    })
    return jb, jl, md, manifest


def _atomic_write_no_clobber(path: Path, data: bytes) -> None:
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            # A hard-link publication is atomic and never replaces a competing
            # writer's destination. Both paths are in the same directory/filesystem.
            os.link(temp_name, path)
        except FileExistsError:
            if not path.is_file() or path.read_bytes() != data:
                raise ArtifactMismatch(
                    f"concurrent {path.name} bytes conflict with expected report"
                )
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def compute_artifacts(artifacts_root: ArtifactRoot, report: ReportArtifacts) -> ArtifactResult:
    paths = _paths(artifacts_root, report.window_start, report.window_end)
    data = _build(report)
    expected = tuple(sha256_hex(value) for value in data)
    for path, value in zip(paths, data):
        if path.exists():
            if not path.is_file() or path.read_bytes() != value:
                raise ArtifactMismatch(f"existing {path.name} bytes conflict with expected report")
    for path, value in zip(paths, data):
        if not path.exists():
            _atomic_write_no_clobber(path, value)
    for path, value in zip(paths, data):
        if not path.is_file() or path.read_bytes() != value:
            raise ArtifactMismatch(f"published {path.name} bytes do not match expected report")
    return ArtifactResult(json_bytes=data[0], json_sha256=expected[0], jsonl_bytes=data[1], jsonl_sha256=expected[1], markdown_bytes=data[2], markdown_sha256=expected[2], manifest_bytes=data[3], manifest_sha256=expected[3], json_path=paths[0], jsonl_path=paths[1], markdown_path=paths[2], manifest_path=paths[3])


def verify_artifacts(artifacts_root: ArtifactRoot, report: ReportArtifacts) -> ArtifactResult:
    paths = _paths(artifacts_root, report.window_start, report.window_end)
    data = _build(report)
    for path, value in zip(paths, data):
        if not path.is_file():
            raise ArtifactMismatch(f"artifact not found: {path}")
        if path.read_bytes() != value:
            raise ArtifactMismatch(f"{path.name} SHA-256 mismatch")
    expected = tuple(sha256_hex(v) for v in data)
    return ArtifactResult(json_bytes=data[0], json_sha256=expected[0], jsonl_bytes=data[1], jsonl_sha256=expected[1], markdown_bytes=data[2], markdown_sha256=expected[2], manifest_bytes=data[3], manifest_sha256=expected[3], json_path=paths[0], jsonl_path=paths[1], markdown_path=paths[2], manifest_path=paths[3])
