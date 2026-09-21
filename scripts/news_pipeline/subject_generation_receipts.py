"""Durable schema-v10 subject generation receipt writer."""
from __future__ import annotations

import sqlite3

from .schema_v10 import validate_v10
from .subject_artifacts import SubjectArtifact


_FIELDS = (
    "mode",
    "model_call_count",
    "cache_hit_count",
    "fallback_count",
    "malformed_count",
    "transport_error_count",
    "created_at",
)


def record_subject_generation(
    connection: sqlite3.Connection,
    artifact: SubjectArtifact,
) -> bool:
    """Insert one immutable receipt; return False for an exact replay."""
    if not isinstance(connection, sqlite3.Connection):
        raise TypeError("connection must be sqlite3.Connection")
    if not isinstance(artifact, SubjectArtifact):
        raise TypeError("artifact must be a SubjectArtifact")
    if connection.in_transaction:
        raise ValueError("generation receipt writes require no active transaction")
    validate_v10(connection)
    expected = (
        artifact.generation.mode,
        artifact.generation.model_call_count,
        artifact.generation.cache_hit_count,
        artifact.generation.fallback_count,
        artifact.generation.malformed_count,
        artifact.generation.transport_error_count,
        artifact.created_at,
    )
    existing = connection.execute(
        """SELECT mode,model_call_count,cache_hit_count,fallback_count,
                  malformed_count,transport_error_count,created_at
             FROM subject_generation_receipts WHERE subject_report_id=?""",
        (artifact.subject_report_id,),
    ).fetchone()
    if existing is not None:
        if tuple(existing) != expected:
            raise ValueError("existing subject generation receipt conflicts with artifact")
        return False
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            """INSERT INTO subject_generation_receipts(
                   subject_report_id,mode,model_call_count,cache_hit_count,
                   fallback_count,malformed_count,transport_error_count,created_at)
               VALUES(?,?,?,?,?,?,?,?)""",
            (artifact.subject_report_id, *expected),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    persisted = connection.execute(
        """SELECT mode,model_call_count,cache_hit_count,fallback_count,
                  malformed_count,transport_error_count,created_at
             FROM subject_generation_receipts WHERE subject_report_id=?""",
        (artifact.subject_report_id,),
    ).fetchone()
    if persisted is None or tuple(persisted) != expected:
        raise ValueError("subject generation receipt read-back mismatch")
    return True


__all__ = ["record_subject_generation"]
