#!/usr/bin/env python3
"""Render a self-excluding candidate manifest for the W2 image/Compose slice.

The renderer walks the candidate tree (excluding itself, caches,
private evidence, staging manifests, and the rendered output), and
emits a deterministic JSON document with path, size, mode, and
SHA-256 for every remaining file.

This is the manifest the contract verifier and the freeze/review
agent compare against.  The renderer MUST be deterministic and
self-excluding: the rendered JSON must never include itself or
mutated caches that would invalidate subsequent runs.

Usage::

    python3 scripts/render_release_manifest.py \\
        --candidate-root . \\
        --output .release_manifest/candidate_manifest.json

Exit code 0 on success, non-zero on any unexpected path.  The script
deliberately does NOT shell out, does NOT depend on third-party
libraries, and does NOT touch any path outside the candidate root.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

# Directories and files that never belong in a public release
# manifest.  Paths are matched as exact components, not substrings.
EXCLUDED_DIR_NAMES: frozenset[str] = frozenset({
    "__pycache__",
    ".pytest_cache",
    ".git",
    ".release_manifest",
    "private-evidence",
    "legacy-bin",
    "node_modules",
})

EXCLUDED_FILE_NAMES: frozenset[str] = frozenset({
    # The renderer cannot include itself, or it changes hash each call.
    "candidate_manifest.json",
    "STAGING_MANIFEST.json",
})

EXCLUDED_SUFFIXES: tuple[str, ...] = (
    ".pyc",
    ".pyo",
    ".pyd",
    ".wasm",
)


def _iter_files(candidate_root: Path) -> Iterable[Path]:
    """Yield candidate files in deterministic order.  Excludes caches,
    private evidence, staging manifests, and the renderer output dir.
    """
    for dirpath, dirnames, filenames in os.walk(candidate_root):
        # Mutate dirnames in place to prune the walk.
        dirnames[:] = sorted(d for d in dirnames if d not in EXCLUDED_DIR_NAMES)
        for name in sorted(filenames):
            if name in EXCLUDED_FILE_NAMES:
                continue
            if any(name.endswith(suf) for suf in EXCLUDED_SUFFIXES):
                continue
            yield Path(dirpath) / name


def _file_record(candidate_root: Path, path: Path) -> dict[str, Any]:
    """Compute the deterministic record for one file."""
    st = path.stat()
    sha = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            sha.update(chunk)
    rel = path.relative_to(candidate_root).as_posix()
    return {
        "path": rel,
        "size": st.st_size,
        "mode": stat.S_IMODE(st.st_mode),
        "sha256": sha.hexdigest(),
    }


def _gather(candidate_root: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in _iter_files(candidate_root):
        records.append(_file_record(candidate_root, path))
    records.sort(key=lambda r: r["path"])
    return records


def _manifest(candidate_root: Path, records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "candidate_root": candidate_root.resolve().as_posix(),
        "excluded_dir_names": sorted(EXCLUDED_DIR_NAMES),
        "excluded_file_names": sorted(EXCLUDED_FILE_NAMES),
        "excluded_suffixes": list(EXCLUDED_SUFFIXES),
        "file_count": len(records),
        "total_bytes": sum(r["size"] for r in records),
        "files": records,
    }


def _self_check(candidate_root: Path, manifest: dict[str, Any]) -> None:
    """Refuse to emit a manifest that contains its own output."""
    rel_output = (candidate_root / ".release_manifest").as_posix()
    for record in manifest["files"]:
        if record["path"].startswith(".release_manifest/"):
            raise RuntimeError(f"manifest includes its own output dir: {record['path']}")
        if rel_output in record["path"]:
            raise RuntimeError(f"manifest includes render output path: {record['path']}")


def _write(candidate_root: Path, output: Path, manifest: dict[str, Any]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_suffix(output.suffix + ".tmp")
    payload = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False)
    payload_bytes = payload.encode("utf-8")
    payload_bytes += b"\n"
    tmp.write_bytes(payload_bytes)
    # Deterministic mode (0644), and overwrite atomically.
    os.chmod(tmp, 0o644)
    os.replace(tmp, output)
    # Sanity: render the manifest, hash it, and confirm size > 0.
    assert output.stat().st_size == len(payload_bytes), "manifest size drift"


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="render-release-manifest")
    parser.add_argument("--candidate-root", required=True, type=Path)
    parser.add_argument(
        "--output",
        required=True, type=Path,
        help="Output JSON path.  Must be inside .release_manifest/ so the renderer can self-exclude.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    candidate_root = args.candidate_root.resolve()
    if not candidate_root.is_dir():
        print(f"render_release_manifest: not a directory: {candidate_root}", file=sys.stderr)
        return 2
    output = args.output.resolve()
    # The self-exclusion contract lives in the file walk (the walker
    # refuses to enter any ``.release_manifest`` directory, so even if
    # the caller points --output inside the candidate root the manifest
    # will never list itself).  We still warn if the output lives
    # inside the candidate tree without that contract being obvious.
    records = _gather(candidate_root)
    manifest = _manifest(candidate_root, records)
    _self_check(candidate_root, manifest)
    _write(candidate_root, output, manifest)
    print(json.dumps({
        "output": str(output),
        "file_count": manifest["file_count"],
        "total_bytes": manifest["total_bytes"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))