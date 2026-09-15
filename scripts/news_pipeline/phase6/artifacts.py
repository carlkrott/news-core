"""Slice 6.5 — deterministic PCJ-1 artifacts and offline replay.

The slice contract is intentionally narrow:

* the artifact set is fixed to ``input.json``, ``audit.jsonl``,
  ``preview.json``, and ``replay.json``;
* each filename maps to one exact manifest class;
* bytes are canonical UTF-8 and are verified by sha256 + byte count;
* delivery evidence is always ``0`` and the manifest classes are
  permanently non-evidentiary;
* writes are anchored to a retained directory file descriptor and use
  exclusive, no-follow file creation;
* replay is offline only and revalidates persisted bytes without touching
  any live network / clock / process / credential / UUID surface.

The module never renames, copies, or promotes artifacts. It performs only
append-only inserts into ``artifact_manifest`` and rolls back on any
failure.

Crash recovery
--------------
If an artifact file already exists under the retained root FD but the
matching ``artifact_manifest`` row is missing — the exact state left
behind by a process that crashed after writing the file but before the
INSERT committed — the file is treated as a recoverable orphan:

* the on-disk bytes must exactly match the canonical payload,
* the entry must be a regular file reachable without following any
  symlink,
* its mode must be exactly ``0o600``,
* its uid/gid must equal the current effective uid/gid,
* its sha256 and byte count must match the canonical record.

If every invariant holds, the missing manifest row is appended inside
the current transaction. The pre-existing file is never re-created,
renamed, copied, overwritten, or unlinked, and is intentionally NOT
added to the rollback set — a later rollback in the same invocation
must leave the orphan in place.

If any invariant is violated the caller is failed closed with
:class:`Pcj1ArtifactsConflictError`; the transaction is rolled back,
no manifest row is inserted, and the foreign file is left untouched
so an operator can inspect it.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import sqlite3
import stat
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path

from .schema import SCHEMA_V1_EXPECTED_TABLES
from .types import Phase6SandboxError, Phase6SchemaError


PCJ1_ARTIFACT_NAMES: tuple[str, ...] = (
    "input.json",
    "audit.jsonl",
    "preview.json",
    "replay.json",
)

PCJ1_ARTIFACT_CLASS_BY_NAME: dict[str, str] = {
    "input.json": "INPUT_SNAPSHOT",
    "audit.jsonl": "NON_DELIVERY_AUDIT",
    "preview.json": "NON_EVIDENT_PREVIEW",
    "replay.json": "NON_EVIDENT_REPLAY",
}

PCJ1_ARTIFACT_CLASSES: tuple[str, ...] = (
    "INPUT_SNAPSHOT",
    "NON_DELIVERY_AUDIT",
    "NON_EVIDENT_PREVIEW",
    "NON_EVIDENT_REPLAY",
)

NON_EVIDENT_DELIVERY_EVIDENCE: int = 0


class Pcj1ArtifactsError(Phase6SandboxError):
    """Base class for every Slice 6.5 failure mode."""


class Pcj1ArtifactsIdentityError(Pcj1ArtifactsError):
    """A caller-supplied payload or artifact name failed validation."""


class Pcj1ArtifactsConflictError(Pcj1ArtifactsError):
    """A file or manifest row already exists with incompatible bytes."""


class Pcj1ArtifactsReplayError(Pcj1ArtifactsError):
    """Offline replay found an on-disk artifact that does not match manifest."""


class Pcj1ArtifactsSchemaError(Pcj1ArtifactsError, Phase6SchemaError):
    """The connection does not expose the V1 artifact_manifest contract."""


@dataclasses.dataclass(frozen=True)
class Pcj1ArtifactRecord:
    """A single deterministic artifact payload plus its persisted metadata."""

    artifact_name: str
    artifact_class: str
    payload_bytes: bytes
    sha256: str
    byte_count: int
    persisted: bool = False
    duplicate: bool = False


@dataclasses.dataclass(frozen=True)
class Pcj1ArtifactWriteOutcome:
    """The full result of a materialize / replay operation."""

    records: tuple[Pcj1ArtifactRecord, ...]


def canonical_json_bytes(value: object) -> bytes:
    """Return canonical UTF-8 JSON bytes for ``value``.

    ``str`` and ``bytes`` are passed through directly; all other values are
    serialized with compact, key-sorted JSON and ``ensure_ascii=False``.
    """

    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def canonical_jsonl_bytes(rows: Sequence[object]) -> bytes:
    """Return canonical JSONL UTF-8 bytes for ``rows`` in input order."""

    if not rows:
        return b""
    payloads = [canonical_json_bytes(row) for row in rows]
    return b"\n".join(payloads) + b"\n"


def _sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _artifact_class_for_name(name: str) -> str:
    try:
        return PCJ1_ARTIFACT_CLASS_BY_NAME[name]
    except KeyError as exc:
        raise Pcj1ArtifactsIdentityError(
            f"unsupported artifact name: {name!r}"
        ) from exc


def _make_record(name: str, payload: bytes) -> Pcj1ArtifactRecord:
    artifact_class = _artifact_class_for_name(name)
    return Pcj1ArtifactRecord(
        artifact_name=name,
        artifact_class=artifact_class,
        payload_bytes=payload,
        sha256=_sha256_hex(payload),
        byte_count=len(payload),
    )


def build_pcj1_artifact_records(
    input_json: object,
    audit_rows: Sequence[object],
    preview_json: object,
    replay_json: object,
) -> tuple[Pcj1ArtifactRecord, ...]:
    """Build the fixed PCJ-1 artifact set in canonical byte form."""

    records = (
        _make_record("input.json", canonical_json_bytes(input_json)),
        _make_record("audit.jsonl", canonical_jsonl_bytes(audit_rows)),
        _make_record("preview.json", canonical_json_bytes(preview_json)),
        _make_record("replay.json", canonical_json_bytes(replay_json)),
    )
    return records


def _require_artifact_manifest_schema(conn: sqlite3.Connection) -> None:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    missing = [name for name in ("artifact_manifest",) if name not in tables]
    if missing:
        raise Pcj1ArtifactsSchemaError(
            f"missing required table(s): {', '.join(missing)}"
        )
    if not SCHEMA_V1_EXPECTED_TABLES.issubset(tables):
        raise Pcj1ArtifactsSchemaError(
            "connection does not look like the V1 phase6 schema"
        )

    columns = [
        row[1]
        for row in conn.execute("PRAGMA table_info(artifact_manifest)").fetchall()
    ]
    expected = [
        "artifact_name",
        "artifact_class",
        "sha256",
        "byte_count",
        "delivery_evidence",
    ]
    if columns != expected:
        raise Pcj1ArtifactsSchemaError(
            f"artifact_manifest columns mismatch: {columns!r}"
        )


def _read_existing_manifest(
    conn: sqlite3.Connection,
) -> dict[str, tuple[str, str, int, int]]:
    rows = conn.execute(
        "SELECT artifact_name, artifact_class, sha256, byte_count, delivery_evidence "
        "FROM artifact_manifest"
    ).fetchall()
    return {
        row[0]: (row[1], row[2], int(row[3]), int(row[4]))
        for row in rows
    }


def _read_bytes_at(root_fd: int, name: str) -> bytes:
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=root_fd)
    try:
        with os.fdopen(fd, "rb", closefd=True) as fh:
            return fh.read()
    finally:
        # fd is owned by the file object when closefd=True; the finally is
        # defensive if an exception is raised before the context manager enters.
        try:
            os.close(fd)
        except OSError:
            pass


def _remove_at(root_fd: int, name: str) -> None:
    try:
        os.unlink(name, dir_fd=root_fd)
    except FileNotFoundError:
        pass


def _write_bytes_at(root_fd: int, name: str, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    fd = os.open(name, flags, 0o600, dir_fd=root_fd)
    try:
        with os.fdopen(fd, "wb", closefd=True) as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def _verify_record_payload(record: Pcj1ArtifactRecord, actual: bytes) -> None:
    if actual != record.payload_bytes:
        raise Pcj1ArtifactsConflictError(
            f"payload mismatch for {record.artifact_name}"
        )
    if _sha256_hex(actual) != record.sha256:
        raise Pcj1ArtifactsConflictError(
            f"sha256 mismatch for {record.artifact_name}"
        )
    if len(actual) != record.byte_count:
        raise Pcj1ArtifactsConflictError(
            f"byte_count mismatch for {record.artifact_name}"
        )


# Mode we always create new artifact files with. A pre-existing file at
# crash-recovery time must carry exactly this mode to be considered a
# recoverable orphan from a prior invocation of this same module — any
# other mode means the file is foreign and we must fail closed.
_RECOVERY_REQUIRED_MODE: int = 0o600


def _validate_recoverable_orphan(
    root_fd: int,
    record: Pcj1ArtifactRecord,
) -> tuple[bytes, os.stat_result]:
    """Read+stat a pre-existing file via an anchored O_NOFOLLOW FD and
    confirm it carries every invariant Slice 6.5 enforces at creation
    time.

    Returns the on-disk bytes plus the stat result on success. Raises
    :class:`Pcj1ArtifactsConflictError` (fail-closed) on any of:

    * the entry is not a regular file (e.g. directory, fifo, socket);
    * the entry is reachable only via a symlink (the anchored
      ``O_NOFOLLOW`` open itself rejects this — surfaced here as a
      conflict rather than a bare ``OSError``);
    * the on-disk mode differs from :data:`_RECOVERY_REQUIRED_MODE`;
    * ownership differs from the current effective uid/gid (a file owned
      by another principal cannot have been produced by this module in
      this process tree);
    * the on-disk bytes / sha256 / size do not match the canonical
      record.

    The function never modifies, copies, renames, or unlinks the entry.
    Fail-closed semantics: a single invariant violation aborts recovery
    so the caller rolls back the transaction and leaves the foreign
    file in place untouched.

    A single anchored FD is held open across the stat and the read so
    the stat and the bytes describe the same file object — no TOCTOU
    window where another process could swap the contents under us.
    """

    try:
        fd = os.open(
            record.artifact_name,
            os.O_RDONLY | os.O_NOFOLLOW,
            dir_fd=root_fd,
        )
    except OSError as exc:
        raise Pcj1ArtifactsConflictError(
            f"cannot open pre-existing {record.artifact_name!r}: {exc!s}"
        ) from exc
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise Pcj1ArtifactsConflictError(
                f"pre-existing {record.artifact_name!r} is not a regular file"
            )
        if st.st_nlink != 1:
            raise Pcj1ArtifactsConflictError(
                f"pre-existing {record.artifact_name!r} has "
                f"{st.st_nlink} hard links; exactly one required"
            )
        actual_mode = stat.S_IMODE(st.st_mode)
        if actual_mode != _RECOVERY_REQUIRED_MODE:
            raise Pcj1ArtifactsConflictError(
                f"pre-existing {record.artifact_name!r} mode "
                f"{actual_mode:o} != required {_RECOVERY_REQUIRED_MODE:o}"
            )
        if st.st_uid != os.geteuid() or st.st_gid != os.getegid():
            raise Pcj1ArtifactsConflictError(
                f"pre-existing {record.artifact_name!r} ownership "
                f"{st.st_uid}:{st.st_gid} != "
                f"current {os.geteuid()}:{os.getegid()}"
            )
        # Size guard first — cheap way to reject obvious drift before
        # pulling the full payload off disk. The size invariant is
        # enforced canonically by _verify_record_payload below.
        if st.st_size != record.byte_count:
            raise Pcj1ArtifactsConflictError(
                f"pre-existing {record.artifact_name!r} size "
                f"{st.st_size} != canonical {record.byte_count}"
            )
        on_disk = os.read(fd, st.st_size)
        if len(on_disk) != st.st_size:
            raise Pcj1ArtifactsConflictError(
                f"pre-existing {record.artifact_name!r} short read: "
                f"got {len(on_disk)} of {st.st_size}"
            )
    finally:
        try:
            os.close(fd)
        except OSError:
            pass
    _verify_record_payload(record, on_disk)
    return on_disk, st


def _row_from_record(record: Pcj1ArtifactRecord) -> tuple[object, ...]:
    return (
        record.artifact_name,
        record.artifact_class,
        record.sha256,
        record.byte_count,
        NON_EVIDENT_DELIVERY_EVIDENCE,
    )


def write_pcj1_artifacts(
    root_fd: int,
    conn: sqlite3.Connection,
    input_json: object,
    audit_rows: Sequence[object],
    preview_json: object,
    replay_json: object,
) -> Pcj1ArtifactWriteOutcome:
    """Materialize the fixed artifact set and append manifest rows.

    Duplicate exact writes are treated as no-ops. A name collision with
    different bytes is a hard conflict and rolls back the whole bundle.
    """

    _require_artifact_manifest_schema(conn)
    records = build_pcj1_artifact_records(
        input_json, audit_rows, preview_json, replay_json
    )
    existing = _read_existing_manifest(conn)
    created_names: list[str] = []
    outcome: list[Pcj1ArtifactRecord] = []

    conn.execute("BEGIN IMMEDIATE")
    try:
        for record in records:
            current = existing.get(record.artifact_name)
            if current is not None:
                expected_class, expected_sha, expected_size, expected_delivery = current
                if (
                    expected_class != record.artifact_class
                    or expected_sha != record.sha256
                    or expected_size != record.byte_count
                    or expected_delivery != NON_EVIDENT_DELIVERY_EVIDENCE
                ):
                    raise Pcj1ArtifactsConflictError(
                        f"artifact_manifest row mismatch for {record.artifact_name}"
                    )
                on_disk = _read_bytes_at(root_fd, record.artifact_name)
                _verify_record_payload(record, on_disk)
                outcome.append(
                    dataclasses.replace(record, persisted=False, duplicate=True)
                )
                continue

            try:
                _write_bytes_at(root_fd, record.artifact_name, record.payload_bytes)
            except FileExistsError:
                # Crash-recovery path: a prior invocation of this same
                # module may have written the artifact file and then
                # crashed before the manifest INSERT landed. Fail-closed
                # validation decides whether we can treat the on-disk
                # file as the canonical payload and recover by
                # inserting the missing manifest row inside the current
                # transaction. We never rename, copy, overwrite, or
                # unlink the pre-existing file — the file is left
                # exactly as it was found, and crucially it is NOT
                # added to ``created_names`` so a later rollback in this
                # invocation cannot delete it.
                _validate_recoverable_orphan(root_fd, record)
                conn.execute(
                    "INSERT INTO artifact_manifest "
                    "(artifact_name, artifact_class, sha256, byte_count, delivery_evidence) "
                    "VALUES (?, ?, ?, ?, ?)",
                    _row_from_record(record),
                )
                outcome.append(
                    dataclasses.replace(record, persisted=True, duplicate=False)
                )
                continue
            created_names.append(record.artifact_name)
            on_disk = _read_bytes_at(root_fd, record.artifact_name)
            _verify_record_payload(record, on_disk)
            conn.execute(
                "INSERT INTO artifact_manifest "
                "(artifact_name, artifact_class, sha256, byte_count, delivery_evidence) "
                "VALUES (?, ?, ?, ?, ?)",
                _row_from_record(record),
            )
            outcome.append(
                dataclasses.replace(record, persisted=True, duplicate=False)
            )
        conn.commit()
    except BaseException:
        conn.rollback()
        for name in created_names:
            _remove_at(root_fd, name)
        raise

    return Pcj1ArtifactWriteOutcome(records=tuple(outcome))


def _manifest_rows_in_name_order(
    conn: sqlite3.Connection,
) -> list[tuple[str, str, str, int, int]]:
    order_expr = "CASE artifact_name " + " ".join(
        f"WHEN '{name}' THEN {index}"
        for index, name in enumerate(PCJ1_ARTIFACT_NAMES)
    ) + " END"
    rows = conn.execute(
        "SELECT artifact_name, artifact_class, sha256, byte_count, delivery_evidence "
        "FROM artifact_manifest "
        f"WHERE artifact_name IN ({', '.join('?' for _ in PCJ1_ARTIFACT_NAMES)}) "
        f"ORDER BY {order_expr}",
        PCJ1_ARTIFACT_NAMES,
    ).fetchall()
    return [
        (row[0], row[1], row[2], int(row[3]), int(row[4]))
        for row in rows
    ]


def replay_pcj1_artifacts(root_fd: int, conn: sqlite3.Connection) -> Pcj1ArtifactWriteOutcome:
    """Offline replay: read the persisted bytes and validate the manifest."""

    _require_artifact_manifest_schema(conn)
    rows = _manifest_rows_in_name_order(conn)
    if len(rows) != len(PCJ1_ARTIFACT_NAMES):
        raise Pcj1ArtifactsReplayError(
            "artifact_manifest does not contain the complete fixed set"
        )

    records: list[Pcj1ArtifactRecord] = []
    for artifact_name, artifact_class, sha256, byte_count, delivery_evidence in rows:
        expected_class = _artifact_class_for_name(artifact_name)
        if artifact_class != expected_class:
            raise Pcj1ArtifactsReplayError(
                f"artifact class mismatch for {artifact_name}"
            )
        if delivery_evidence != NON_EVIDENT_DELIVERY_EVIDENCE:
            raise Pcj1ArtifactsReplayError(
                f"delivery_evidence must stay 0 for {artifact_name}"
            )
        payload = _read_bytes_at(root_fd, artifact_name)
        if _sha256_hex(payload) != sha256 or len(payload) != byte_count:
            raise Pcj1ArtifactsReplayError(
                f"persisted bytes do not match manifest for {artifact_name}"
            )
        records.append(
            Pcj1ArtifactRecord(
                artifact_name=artifact_name,
                artifact_class=artifact_class,
                payload_bytes=payload,
                sha256=sha256,
                byte_count=byte_count,
                persisted=True,
                duplicate=False,
            )
        )
    return Pcj1ArtifactWriteOutcome(records=tuple(records))


__all__ = [
    "NON_EVIDENT_DELIVERY_EVIDENCE",
    "PCJ1_ARTIFACT_CLASSES",
    "PCJ1_ARTIFACT_CLASS_BY_NAME",
    "PCJ1_ARTIFACT_NAMES",
    "Pcj1ArtifactRecord",
    "Pcj1ArtifactWriteOutcome",
    "Pcj1ArtifactsConflictError",
    "Pcj1ArtifactsError",
    "Pcj1ArtifactsIdentityError",
    "Pcj1ArtifactsReplayError",
    "Pcj1ArtifactsSchemaError",
    "build_pcj1_artifact_records",
    "canonical_json_bytes",
    "canonical_jsonl_bytes",
    "replay_pcj1_artifacts",
    "write_pcj1_artifacts",
]
