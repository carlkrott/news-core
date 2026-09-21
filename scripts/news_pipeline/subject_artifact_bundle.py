"""Atomic per-parent recovery bundle for subject artifacts and shadow events.

Bundles are private runtime recovery state inside the configured artifact root.
They are not delivery payloads and must not be copied into public publication
exports. Subject delivery consumes only separately persisted report/outbox rows.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence, cast

from .briefing_ledger import ShadowEvent, normalize_canonical_payload
from .event_contracts import SemanticDecision
from .models import Category, Subject
from .report_artifacts import ArtifactMismatch, ArtifactRoot, _atomic_write_no_clobber
from .subject_artifacts import (
    SubjectArtifact,
    SubjectArtifactSpec,
    SubjectGeneration,
    build_subject_artifact_payload,
    validate_subject_artifact_payload,
)


_REPORT_RE = re.compile(r"report-[0-9a-f]{64}\Z")
_BUNDLE_KEYS = frozenset(
    {
        "schema",
        "parent_report_id",
        "run_id",
        "updated_at",
        "created_at",
        "artifacts",
        "shadow_events",
    }
)
_EVENT_KEYS = frozenset(
    {
        "candidate_id",
        "category",
        "decision",
        "payload",
        "recorded_at_utc",
        "subject_id",
        "event_id",
        "event_version",
    }
)


@dataclass(frozen=True, slots=True)
class SubjectArtifactBundle:
    parent_report_id: str
    run_id: str
    updated_at: str
    created_at: str
    artifacts: tuple[SubjectArtifactSpec, ...]
    shadow_events: tuple[ShadowEvent, ...]
    path: Path


def _timestamp(value: object, field: str) -> str:
    if type(value) is not str or not value.endswith("Z"):
        raise ArtifactMismatch(f"subject bundle {field} must be canonical UTC")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ArtifactMismatch(f"subject bundle {field} is invalid") from exc
    if parsed.utcoffset() != timedelta(0):
        raise ArtifactMismatch(f"subject bundle {field} must be UTC")
    canonical = parsed.isoformat(
        timespec="microseconds" if parsed.microsecond else "seconds"
    ).replace("+00:00", "Z")
    if canonical != value:
        raise ArtifactMismatch(f"subject bundle {field} is not canonical")
    return value


def _canonical(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _path(root: ArtifactRoot, parent_report_id: str) -> Path:
    if type(parent_report_id) is not str or _REPORT_RE.fullmatch(parent_report_id) is None:
        raise ArtifactMismatch("subject bundle parent report ID is malformed")
    return root.path / "subject-bundles" / f"{parent_report_id}.json"


def _event_payload(event: ShadowEvent) -> dict[str, object]:
    return {
        "candidate_id": event.candidate_id,
        "category": cast(Category, event.category).value,
        "decision": cast(SemanticDecision, event.decision).value,
        "payload": json.loads(event.payload_json),
        "recorded_at_utc": event.recorded_at_utc,
        "subject_id": event.subject_id,
        "event_id": event.event_id,
        "event_version": event.event_version,
    }


def _parse_event(value: object) -> ShadowEvent:
    if type(value) is not dict or frozenset(value) != _EVENT_KEYS:
        raise ArtifactMismatch("subject bundle shadow event has incompatible keys")
    try:
        payload_json = normalize_canonical_payload(value["payload"])
        return ShadowEvent(
            candidate_id=value["candidate_id"],
            category=Category(value["category"]),
            decision=SemanticDecision(value["decision"]),
            payload_json=payload_json,
            recorded_at_utc=value["recorded_at_utc"],
            subject_id=value["subject_id"],
            event_id=value["event_id"],
            event_version=value["event_version"],
        )
    except (TypeError, ValueError) as exc:
        raise ArtifactMismatch("subject bundle shadow event is invalid") from exc


def _artifact_payloads(
    *,
    parent_report_id: str,
    created_at: str,
    subject_chunks: Mapping[Subject, tuple[str, ...]],
    story_counts: Mapping[Subject, int],
    subject_generations: Mapping[Subject, SubjectGeneration],
) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for subject in Subject:
        count = story_counts.get(subject, 0)
        chunks = subject_chunks.get(subject)
        generation = subject_generations.get(subject)
        if count == 0:
            chunks = ()
            generation = SubjectGeneration("empty", 0, 0, 0, 0, 0)
        elif chunks is None or generation is None:
            raise ArtifactMismatch(
                f"subject {subject.value} lacks durable rendered content or generation metrics"
            )
        payloads.append(
            build_subject_artifact_payload(
                parent_report_id=parent_report_id,
                subject=subject,
                chunks=chunks,
                story_count=count,
                created_at=created_at,
                generation=generation,
            )
        )
    return payloads


def write_subject_artifact_bundle(
    root: ArtifactRoot,
    *,
    parent_report_id: str,
    run_id: str,
    updated_at: str,
    created_at: str,
    subject_chunks: Mapping[Subject, tuple[str, ...]],
    story_counts: Mapping[Subject, int],
    subject_generations: Mapping[Subject, SubjectGeneration],
    shadow_events: Sequence[ShadowEvent],
) -> SubjectArtifactBundle:
    if not isinstance(root, ArtifactRoot):
        raise TypeError("root must be an ArtifactRoot")
    if type(run_id) is not str or not run_id:
        raise ValueError("run_id must be non-empty")
    updated_at = _timestamp(updated_at, "updated_at")
    created_at = _timestamp(created_at, "created_at")
    events = tuple(shadow_events)
    if any(not isinstance(event, ShadowEvent) for event in events):
        raise TypeError("shadow_events must contain only ShadowEvent values")
    payload = {
        "schema": "news-subject-artifact-bundle-v1",
        "parent_report_id": parent_report_id,
        "run_id": run_id,
        "updated_at": updated_at,
        "created_at": created_at,
        "artifacts": _artifact_payloads(
            parent_report_id=parent_report_id,
            created_at=created_at,
            subject_chunks=subject_chunks,
            story_counts=story_counts,
            subject_generations=subject_generations,
        ),
        "shadow_events": [_event_payload(event) for event in events],
    }
    path = _path(root, parent_report_id)
    path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
    _atomic_write_no_clobber(path, _canonical(payload))
    return load_subject_artifact_bundle(root, parent_report_id)


def load_subject_artifact_bundle(
    root: ArtifactRoot,
    parent_report_id: str,
) -> SubjectArtifactBundle:
    if not isinstance(root, ArtifactRoot):
        raise TypeError("root must be an ArtifactRoot")
    path = _path(root, parent_report_id)
    if not path.is_file():
        raise ArtifactMismatch("subject artifact bundle is missing")
    try:
        raw = path.read_bytes()
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArtifactMismatch("subject artifact bundle is not valid UTF-8 JSON") from exc
    if type(payload) is not dict or frozenset(payload) != _BUNDLE_KEYS:
        raise ArtifactMismatch("subject artifact bundle has incompatible keys")
    if payload["schema"] != "news-subject-artifact-bundle-v1":
        raise ArtifactMismatch("subject artifact bundle schema is incompatible")
    if payload["parent_report_id"] != parent_report_id:
        raise ArtifactMismatch("subject artifact bundle parent identity conflicts")
    if type(payload["run_id"]) is not str or not payload["run_id"]:
        raise ArtifactMismatch("subject artifact bundle run ID is malformed")
    updated_at = _timestamp(payload["updated_at"], "updated_at")
    created_at = _timestamp(payload["created_at"], "created_at")
    artifact_values = payload["artifacts"]
    if type(artifact_values) is not list or len(artifact_values) != len(Subject):
        raise ArtifactMismatch("subject artifact bundle must contain every subject exactly once")
    artifacts = tuple(validate_subject_artifact_payload(item) for item in artifact_values)
    if tuple(item.subject for item in artifacts) != tuple(Subject):
        raise ArtifactMismatch("subject artifact bundle subject order is incompatible")
    if any(
        item.parent_report_id != parent_report_id or item.created_at != created_at
        for item in artifacts
    ):
        raise ArtifactMismatch("subject artifact bundle metadata conflicts")
    event_values = payload["shadow_events"]
    if type(event_values) is not list:
        raise ArtifactMismatch("subject artifact bundle shadow events are malformed")
    shadow_events = tuple(_parse_event(item) for item in event_values)
    event_counts = {subject: 0 for subject in Subject}
    for event in shadow_events:
        event_counts[Subject(cast(str, event.subject_id))] += 1
    for artifact in artifacts:
        if artifact.story_count != event_counts[artifact.subject]:
            raise ArtifactMismatch(
                "subject artifact bundle story counts conflict with shadow events"
            )
    if raw != _canonical(payload):
        raise ArtifactMismatch("subject artifact bundle bytes are not canonical")
    return SubjectArtifactBundle(
        parent_report_id=parent_report_id,
        run_id=payload["run_id"],
        updated_at=updated_at,
        created_at=created_at,
        artifacts=artifacts,
        shadow_events=shadow_events,
        path=path,
    )


def find_subject_artifact_bundle(
    root: ArtifactRoot,
    parent_report_id: str,
) -> SubjectArtifactBundle | None:
    path = _path(root, parent_report_id)
    if not path.exists():
        return None
    return load_subject_artifact_bundle(root, parent_report_id)


def publish_subject_artifact_bundle(
    root: ArtifactRoot,
    bundle: SubjectArtifactBundle,
) -> tuple[SubjectArtifact, ...]:
    if not isinstance(root, ArtifactRoot):
        raise TypeError("root must be an ArtifactRoot")
    if not isinstance(bundle, SubjectArtifactBundle):
        raise TypeError("bundle must be a SubjectArtifactBundle")
    from . import subject_artifacts

    published = []
    for item in bundle.artifacts:
        artifact = subject_artifacts.write_subject_artifact(
            root,
            parent_report_id=item.parent_report_id,
            subject=item.subject,
            chunks=item.chunks,
            story_count=item.story_count,
            created_at=item.created_at,
            generation=item.generation,
        )
        if artifact.subject_report_id != item.subject_report_id:
            raise ArtifactMismatch("published subject artifact identity changed")
        published.append(artifact)
    return tuple(published)


__all__ = [
    "SubjectArtifactBundle",
    "find_subject_artifact_bundle",
    "load_subject_artifact_bundle",
    "publish_subject_artifact_bundle",
    "write_subject_artifact_bundle",
]
