#!/usr/bin/env python3
"""Create a closed, publication-safe mirror of the candidate tree."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import sys
from pathlib import Path

from check_publication_safety import (
    _classify_path,
    _iter_files,
    is_export_excluded,
    scan,
)


class ExportError(RuntimeError):
    pass


def _within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _absolute_lexical(path: Path) -> Path:
    """Make an absolute path without resolving symlink aliases."""

    return Path(os.path.abspath(os.fspath(path)))


def _symlink_in_chain(path: Path) -> Path | None:
    """Return the first symlink in an existing lexical path chain."""

    current = path
    while True:
        if current.is_symlink():
            return current
        parent = current.parent
        if parent == current:
            return None
        current = parent


def _remove_staging(staging: Path) -> None:
    """Remove only a staging directory created by this invocation."""

    if staging.is_symlink() or staging.is_file():
        staging.unlink(missing_ok=True)
    elif staging.is_dir():
        shutil.rmtree(staging, ignore_errors=True)


def export_tree(source: Path, destination: Path) -> dict[str, object]:
    source = _absolute_lexical(source)
    destination = _absolute_lexical(destination)
    source_alias = _symlink_in_chain(source)
    if source_alias is not None:
        raise ExportError("source or source ancestor is a symlink")
    destination_alias = _symlink_in_chain(destination)
    if destination_alias is not None:
        raise ExportError("destination or destination ancestor is a symlink")
    if not source.is_dir():
        raise ExportError("source must be a directory")
    if destination.exists() or destination.is_symlink():
        raise ExportError("destination already exists")
    source_real = source.resolve()
    destination_real = destination.resolve()
    if _within(destination, source) or _within(destination_real, source_real):
        raise ExportError("destination must be outside the source tree")

    staging = destination.with_name(f".{destination.name}.partial-{os.getpid()}")
    if staging.exists() or staging.is_symlink():
        raise ExportError("staging destination already exists")

    selected: list[tuple[Path, Path]] = []
    rejected: list[str] = []
    seen_inodes: dict[tuple[int, int], Path] = {}
    for entry in _iter_files(source):
        rel = entry.relative_to(source)
        if entry.is_dir() and not entry.is_symlink():
            continue
        try:
            source_stat = entry.stat(follow_symlinks=False)
        except OSError as exc:
            rejected.append(f"{rel.as_posix()}: source entry cannot be inspected: {exc}")
            continue
        if entry.is_symlink() or not stat.S_ISREG(source_stat.st_mode):
            rejected.append(f"{rel.as_posix()}: symlink or non-regular file")
            continue
        inode = (source_stat.st_dev, source_stat.st_ino)
        if source_stat.st_nlink != 1 or inode in seen_inodes:
            rejected.append(f"{rel.as_posix()}: source file alias")
            continue
        seen_inodes[inode] = entry
        if is_export_excluded(rel):
            continue
        allowed, reason = _classify_path(rel, source_entry=entry)
        if not allowed:
            rejected.append(f"{rel.as_posix()}: {reason}")
            continue
        selected.append((entry, rel))
    if rejected:
        raise ExportError("unclassified candidate paths: " + "; ".join(rejected))

    staging_created = True
    try:
        staging.mkdir(mode=0o700, parents=False)
        for source_file, rel in selected:
            target = staging / rel
            target.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
            shutil.copyfile(source_file, target, follow_symlinks=False)
            source_mode = stat.S_IMODE(source_file.stat(follow_symlinks=False).st_mode)
            os.chmod(target, 0o755 if source_mode & 0o111 else 0o644)

        exit_code, findings = scan(staging)
        if exit_code != 0:
            details = "; ".join(f.render() for f in findings)
            raise ExportError(f"publication scan rejected staged export: {details}")
        total_bytes = sum((staging / rel).stat().st_size for _, rel in selected)
        os.replace(staging, destination)
    except BaseException:
        if staging_created:
            _remove_staging(staging)
        raise

    return {
        "status": "ok",
        "file_count": len(selected),
        "total_bytes": total_bytes,
        "destination": str(destination),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="export-public-tree")
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        result = export_tree(args.source, args.destination)
    except ExportError as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
