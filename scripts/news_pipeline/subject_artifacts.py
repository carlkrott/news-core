"""Immutable, hash-verified subject report artifacts."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

from .models import Subject
from .report_artifacts import ArtifactMismatch, ArtifactRoot, _atomic_write_no_clobber
from .subject_delivery import subject_report_id


_ID_RE = re.compile(r"subject-report-[0-9a-f]{64}\Z")
_KEYS = frozenset(
    {
        "schema",
        "subject_report_id",
        "parent_report_id",
        "subject_id",
        "content_sha256",
        "story_count",
        "created_at",
        "chunks",
        "generation",
    }
)
_GENERATION_KEYS = frozenset(
    {
        "mode",
        "model_call_count",
        "cache_hit_count",
        "fallback_count",
        "malformed_count",
        "transport_error_count",
    }
)


@dataclass(frozen=True, slots=True)
class SubjectGeneration:
    mode: str
    model_call_count: int
    cache_hit_count: int
    fallback_count: int
    malformed_count: int
    transport_error_count: int

    def __post_init__(self) -> None:
        if self.mode not in {"model", "cache", "fallback", "empty"}:
            raise ValueError("subject generation mode is invalid")
        names = (
            "model_call_count",
            "cache_hit_count",
            "fallback_count",
            "malformed_count",
            "transport_error_count",
        )
        for name in names:
            value = getattr(self, name)
            if type(value) is not int or isinstance(value, bool) or value < 0:
                raise ValueError(f"subject generation {name} must be non-negative")
        if self.model_call_count > 1 or self.cache_hit_count > 1:
            raise ValueError("subject generation permits at most one model call/cache hit")
        if self.mode == "empty" and any(getattr(self, name) for name in names):
            raise ValueError("empty subject generation must have zero counters")

    def to_mapping(self) -> dict[str, int | str]:
        return {
            "mode": self.mode,
            "model_call_count": self.model_call_count,
            "cache_hit_count": self.cache_hit_count,
            "fallback_count": self.fallback_count,
            "malformed_count": self.malformed_count,
            "transport_error_count": self.transport_error_count,
        }

    @classmethod
    def from_mapping(cls, value: object) -> "SubjectGeneration":
        if type(value) is not dict or frozenset(value) != _GENERATION_KEYS:
            raise ArtifactMismatch("subject artifact generation metrics are malformed")
        try:
            return cls(**value)
        except (TypeError, ValueError) as exc:
            raise ArtifactMismatch("subject artifact generation metrics are invalid") from exc


@dataclass(frozen=True, slots=True)
class SubjectArtifact:
    subject_report_id: str
    parent_report_id: str
    subject: Subject
    content_sha256: str
    story_count: int
    created_at: str
    chunks: tuple[str, ...]
    generation: SubjectGeneration
    path: Path

    @property
    def rendered_text(self) -> str:
        return "\n\n".join(self.chunks)


@dataclass(frozen=True, slots=True)
class SubjectArtifactSpec:
    subject_report_id: str
    parent_report_id: str
    subject: Subject
    content_sha256: str
    story_count: int
    created_at: str
    chunks: tuple[str, ...]
    generation: SubjectGeneration


def _timestamp(value: object) -> str:
    if type(value) is not str or not value.endswith("Z"):
        raise ArtifactMismatch("subject artifact created_at must be canonical UTC")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ArtifactMismatch("subject artifact created_at is invalid") from exc
    if parsed.utcoffset() != timedelta(0):
        raise ArtifactMismatch("subject artifact created_at must be UTC")
    canonical = parsed.isoformat(
        timespec="microseconds" if parsed.microsecond else "seconds"
    ).replace("+00:00", "Z")
    if canonical != value:
        raise ArtifactMismatch("subject artifact created_at is not canonical")
    return value


def _artifact_path(root: ArtifactRoot, report_id: str) -> Path:
    if type(report_id) is not str or _ID_RE.fullmatch(report_id) is None:
        raise ArtifactMismatch("subject artifact ID is malformed")
    return root.path / "subject-reports" / f"{report_id}.json"


def _canonical(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def build_subject_artifact_payload(
    *,
    parent_report_id: str,
    subject: Subject,
    chunks: tuple[str, ...],
    story_count: int,
    created_at: str,
    generation: SubjectGeneration,
) -> dict[str, Any]:
    if type(parent_report_id) is not str or not parent_report_id:
        raise ValueError("parent_report_id must be non-empty")
    if not isinstance(subject, Subject):
        raise TypeError("subject must be a Subject")
    if type(chunks) is not tuple or any(
        type(chunk) is not str or not chunk for chunk in chunks
    ):
        raise ValueError("chunks must be a tuple of non-empty strings")
    if type(story_count) is not int or isinstance(story_count, bool) or story_count < 0:
        raise ValueError("story_count must be an integer >= 0")
    if bool(chunks) != (story_count > 0):
        raise ValueError("subject artifact chunks must match story_count emptiness")
    if not isinstance(generation, SubjectGeneration):
        raise TypeError("generation must be a SubjectGeneration")
    if story_count == 0 and generation.mode != "empty":
        raise ValueError("zero-story subject artifact requires empty generation mode")
    if story_count > 0 and generation.mode == "empty":
        raise ValueError("populated subject artifact forbids empty generation mode")
    created = _timestamp(created_at)
    rendered = "\n\n".join(chunks)
    content_hash = hashlib.sha256(rendered.encode("utf-8")).hexdigest()
    report_id = subject_report_id(parent_report_id, subject, content_hash)
    return {
        "schema": "news-subject-report-v1",
        "subject_report_id": report_id,
        "parent_report_id": parent_report_id,
        "subject_id": subject.value,
        "content_sha256": content_hash,
        "story_count": story_count,
        "created_at": created,
        "chunks": list(chunks),
        "generation": generation.to_mapping(),
    }


def validate_subject_artifact_payload(payload: object) -> SubjectArtifactSpec:
    if type(payload) is not dict or frozenset(payload) != _KEYS:
        raise ArtifactMismatch("subject artifact has incompatible keys")
    if payload["schema"] != "news-subject-report-v1":
        raise ArtifactMismatch("subject artifact schema is incompatible")
    try:
        subject = Subject(payload["subject_id"])
    except (TypeError, ValueError) as exc:
        raise ArtifactMismatch("subject artifact has an unknown subject") from exc
    chunks_raw = payload["chunks"]
    if type(chunks_raw) is not list:
        raise ArtifactMismatch("subject artifact chunks are malformed")
    chunks = tuple(chunks_raw)
    generation = SubjectGeneration.from_mapping(payload["generation"])
    try:
        rebuilt = build_subject_artifact_payload(
            parent_report_id=payload["parent_report_id"],
            subject=subject,
            chunks=chunks,
            story_count=payload["story_count"],
            created_at=payload["created_at"],
            generation=generation,
        )
    except (TypeError, ValueError, ArtifactMismatch) as exc:
        raise ArtifactMismatch("subject artifact payload is invalid") from exc
    if payload != rebuilt:
        raise ArtifactMismatch("subject artifact payload is not canonical")
    return SubjectArtifactSpec(
        subject_report_id=rebuilt["subject_report_id"],
        parent_report_id=rebuilt["parent_report_id"],
        subject=subject,
        content_sha256=rebuilt["content_sha256"],
        story_count=rebuilt["story_count"],
        created_at=rebuilt["created_at"],
        chunks=chunks,
        generation=generation,
    )


def write_subject_artifact(
    root: ArtifactRoot,
    *,
    parent_report_id: str,
    subject: Subject,
    chunks: Sequence[str],
    story_count: int,
    created_at: str,
    generation: SubjectGeneration,
) -> SubjectArtifact:
    if not isinstance(root, ArtifactRoot):
        raise TypeError("root must be an ArtifactRoot")
    if not isinstance(chunks, (tuple, list)):
        raise TypeError("chunks must be a tuple or list")
    payload = build_subject_artifact_payload(
        parent_report_id=parent_report_id,
        subject=subject,
        chunks=tuple(chunks),
        story_count=story_count,
        created_at=created_at,
        generation=generation,
    )
    path = _artifact_path(root, payload["subject_report_id"])
    path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
    _atomic_write_no_clobber(path, _canonical(payload))
    return load_subject_artifact(root, payload["subject_report_id"])


def load_subject_artifact(root: ArtifactRoot, report_id: str) -> SubjectArtifact:
    if not isinstance(root, ArtifactRoot):
        raise TypeError("root must be an ArtifactRoot")
    path = _artifact_path(root, report_id)
    if not path.is_file():
        raise ArtifactMismatch(f"subject artifact not found: {path}")
    try:
        payload = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArtifactMismatch("subject artifact is not valid UTF-8 JSON") from exc
    if type(payload) is not dict or frozenset(payload) != _KEYS:
        raise ArtifactMismatch("subject artifact has incompatible keys")
    if payload["schema"] != "news-subject-report-v1":
        raise ArtifactMismatch("subject artifact schema is incompatible")
    try:
        subject = Subject(payload["subject_id"])
    except (TypeError, ValueError) as exc:
        raise ArtifactMismatch("subject artifact has an unknown subject") from exc
    chunks_raw = payload["chunks"]
    if type(chunks_raw) is not list or any(
        type(chunk) is not str or not chunk for chunk in chunks_raw
    ):
        raise ArtifactMismatch("subject artifact chunks are malformed")
    chunks = tuple(chunks_raw)
    story_count = payload["story_count"]
    if (
        type(story_count) is not int
        or isinstance(story_count, bool)
        or story_count < 0
        or bool(chunks) != (story_count > 0)
    ):
        raise ArtifactMismatch("subject artifact story_count is malformed")
    parent = payload["parent_report_id"]
    if type(parent) is not str or not parent:
        raise ArtifactMismatch("subject artifact parent_report_id is malformed")
    rendered = "\n\n".join(chunks)
    content_hash = hashlib.sha256(rendered.encode("utf-8")).hexdigest()
    if payload["content_sha256"] != content_hash:
        raise ArtifactMismatch("subject artifact content hash does not match chunks")
    expected_id = subject_report_id(parent, subject, content_hash)
    if payload["subject_report_id"] != report_id or expected_id != report_id:
        raise ArtifactMismatch("subject artifact identity is inconsistent")
    created_at = _timestamp(payload["created_at"])
    generation = SubjectGeneration.from_mapping(payload["generation"])
    if story_count == 0 and generation.mode != "empty":
        raise ArtifactMismatch("zero-story subject artifact has non-empty generation mode")
    if story_count > 0 and generation.mode == "empty":
        raise ArtifactMismatch("populated subject artifact has empty generation mode")
    canonical = _canonical(payload)
    if path.read_bytes() != canonical:
        raise ArtifactMismatch("subject artifact bytes are not canonical")
    return SubjectArtifact(
        subject_report_id=report_id,
        parent_report_id=parent,
        subject=subject,
        content_sha256=content_hash,
        story_count=story_count,
        created_at=created_at,
        chunks=chunks,
        generation=generation,
        path=path,
    )


__all__ = [
    "SubjectArtifact",
    "SubjectArtifactSpec",
    "SubjectGeneration",
    "build_subject_artifact_payload",
    "load_subject_artifact",
    "validate_subject_artifact_payload",
    "write_subject_artifact",
]
