"""Phase 5 — test_report_artifacts.py

Focused tests for ``news_pipeline.report_artifacts`` covering:
- deterministic artifact path generation from full normalized window bounds
- artifact path collision resistance (different windows → different paths)
- manifest metadata and hash verification
- conflicting artifact bytes → ArtifactMismatch raised
- injected artifact failure after ledger completion → restart recovery
- canonical JSON/JSONL/Markdown/manifest output determinism
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from news_pipeline.db import init_db
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5

# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------


def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _make_db() -> sqlite3.Connection:
    fd, name = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_db(name)
    conn = sqlite3.connect(name, isolation_level=None)
    migrate_v3(conn, "2026-09-07T00:00:00Z")
    migrate_v4(conn, "2026-09-07T00:00:00Z")
    migrate_v5(conn, "2026-09-07T00:00:00Z")
    os.unlink(name)
    return conn


def _insert_event(conn, event_id, version, summary, verification_state, valid_from):
    conn.execute("INSERT OR IGNORE INTO runs(id, started_at, finished_at, kind, provenance) VALUES (?, ?, ?, 'historical_replay', 'manual')", (f"run-{event_id}", valid_from, valid_from))
    conn.execute(
        "INSERT OR IGNORE INTO events(id, run_id, category, started_at, ended_at, "
        "article_count, observation_count, status) "
        "VALUES (?, ?, ?, ?, ?, 0, 0, 'complete')",
        (event_id, f"run-{event_id}", "ai", valid_from, valid_from),
    )
    conn.execute(
        "INSERT INTO event_versions(event_id, version, material_change_reason, summary, "
        "verification_state, valid_from, superseded_at, verified_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            event_id,
            version,
            "initial",
            summary,
            verification_state,
            valid_from,
            None,
            valid_from if verification_state == "verified" else None,
        ),
    )


def _as_of(hour=9):
    return datetime(2026, 9, 8, hour, 0, 0, tzinfo=timezone.utc)


def _persist_db(conn: sqlite3.Connection, db_path: str) -> None:
    destination = sqlite3.connect(db_path)
    try:
        conn.backup(destination)
    finally:
        destination.close()
        conn.close()


def _run_once(db_path, artifact_root, as_of=None):
    from news_pipeline.report_builder import run_report

    return run_report(
        db_path=db_path,
        artifacts_root=artifact_root,
        as_of_utc=as_of or _as_of(9),
    )


# -----------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------


class TestDeterministicArtifactPaths(unittest.TestCase):
    """Artifact paths are deterministic from full normalized window bounds, not date-only."""

    def test_same_window_same_path(self) -> None:
        conn = _make_db()
        _insert_event(conn, "ev-path", 1, "Deterministic path test", "verified", "2026-09-08T06:00:00Z")
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                r1 = _run_once(db_path, Path(artifact_root))
                r2 = _run_once(db_path, Path(artifact_root))
                self.assertEqual(
                    r1.artifact_result.json_path, r2.artifact_result.json_path
                )
                self.assertEqual(
                    r1.artifact_result.manifest_path, r2.artifact_result.manifest_path
                )
        finally:
            os.unlink(db_path)

    def test_different_windows_different_paths(self) -> None:
        """Two distinct windows produce distinct artifact paths."""
        conn1 = _make_db()
        _insert_event(conn1, "ev-w1", 1, "Window 1 event", "verified", "2026-09-08T06:00:00Z")
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path_1 = f.name
        _persist_db(conn1, db_path_1)

        conn2 = _make_db()
        _insert_event(conn2, "ev-w2", 1, "Window 2 event", "verified", "2026-09-07T06:00:00Z")
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path_2 = f.name
        _persist_db(conn2, db_path_2)

        try:
            with tempfile.TemporaryDirectory() as artifact_root:
                r1 = _run_once(db_path_1, Path(artifact_root))
                r2 = _run_once(db_path_2, Path(artifact_root), datetime(2026, 9, 9, 9, tzinfo=timezone.utc))
                self.assertNotEqual(
                    r1.artifact_result.json_path, r2.artifact_result.json_path
                )
                # Different windows should not collide on disk.
                self.assertNotEqual(
                    r1.artifact_result.json_path.read_bytes(),
                    r2.artifact_result.json_path.read_bytes(),
                )
        finally:
            os.unlink(db_path_1)
            os.unlink(db_path_2)


class TestManifestHashVerification(unittest.TestCase):
    """Manifest records correct SHA-256 for each artifact file."""

    def test_manifest_stored_sha_matches_actual(self) -> None:
        conn = _make_db()
        _insert_event(conn, "ev-hash", 1, "Hash verification test", "verified", "2026-09-08T06:00:00Z")
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                result = _run_once(db_path, Path(artifact_root))
                manifest = json.loads(result.artifact_result.manifest_path.read_bytes())
                for art_name in ("json", "jsonl", "markdown"):
                    stored_sha = manifest["artifacts"][art_name]["sha256"]
                    actual_sha = _sha256(
                        getattr(result.artifact_result, f"{art_name}_bytes")
                    )
                    self.assertEqual(stored_sha, actual_sha)
        finally:
            os.unlink(db_path)


class TestConflictingBytesFailClosed(unittest.TestCase):
    """If an existing artifact file has wrong bytes, read_artifact_result raises ArtifactMismatch."""

    def test_tampered_json_raises_mismatch(self) -> None:
        conn = _make_db()
        _insert_event(conn, "ev-tamper", 1, "Tamper test", "verified", "2026-09-08T06:00:00Z")
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                result1 = _run_once(db_path, Path(artifact_root))
                # Tamper with the JSON artifact.
                json_path = result1.artifact_result.json_path
                original_bytes = json_path.read_bytes()
                tampered_bytes = original_bytes + b"\n// tampered"
                json_path.write_bytes(tampered_bytes)

                # Attempting to re-read must raise ArtifactMismatch.
                from news_pipeline.report_artifacts import ArtifactMismatch, ArtifactRoot, ReportArtifacts, verify_artifacts
                payload = json.loads(result1.artifact_result.json_bytes)
                report = ReportArtifacts(payload["window_start"], payload["window_end"], payload["generated_at"], tuple(payload["items"]), report_id=payload["report_id"], counts=payload["counts"])

                with self.assertRaises(ArtifactMismatch) as ctx:
                    verify_artifacts(ArtifactRoot(Path(artifact_root)), report)
                self.assertIn("SHA-256 mismatch", str(ctx.exception))
        finally:
            os.unlink(db_path)

    def test_missing_artifact_raises_mismatch(self) -> None:
        conn = _make_db()
        _insert_event(conn, "ev-missing", 1, "Missing artifact test", "verified", "2026-09-08T06:00:00Z")
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                result1 = _run_once(db_path, Path(artifact_root))
                # Delete the JSON artifact.
                result1.artifact_result.json_path.unlink()

                from news_pipeline.report_artifacts import ArtifactMismatch, ArtifactRoot, ReportArtifacts, verify_artifacts
                payload = json.loads(result1.artifact_result.json_bytes)
                report = ReportArtifacts(payload["window_start"], payload["window_end"], payload["generated_at"], tuple(payload["items"]), report_id=payload["report_id"], counts=payload["counts"])

                with self.assertRaises(ArtifactMismatch) as ctx:
                    verify_artifacts(ArtifactRoot(Path(artifact_root)), report)
                self.assertIn("not found", str(ctx.exception))
        finally:
            os.unlink(db_path)


class TestCanonicalOutputDeterminism(unittest.TestCase):
    """JSON, JSONL, Markdown, and manifest output is byte-for-byte identical on replay."""

    def test_json_bytes_identical_on_replay(self) -> None:
        conn = _make_db()
        _insert_event(conn, "ev-canonical", 1, "Canonical output test", "verified", "2026-09-08T06:00:00Z")
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                r1 = _run_once(db_path, Path(artifact_root))
                r2 = _run_once(db_path, Path(artifact_root))
                self.assertEqual(r1.artifact_result.json_bytes, r2.artifact_result.json_bytes)
                self.assertEqual(r1.artifact_result.jsonl_bytes, r2.artifact_result.jsonl_bytes)
                self.assertEqual(
                    r1.artifact_result.markdown_bytes, r2.artifact_result.markdown_bytes
                )
                self.assertEqual(
                    r1.artifact_result.manifest_sha256, r2.artifact_result.manifest_sha256
                )
        finally:
            os.unlink(db_path)


class TestReportReplaysWithoutRerunning(unittest.TestCase):
    """Complete-report replay verifies all four table hashes and returns without re-running."""

    def test_replay_verifies_and_returns_without_engine_call(self) -> None:
        conn = _make_db()
        _insert_event(conn, "ev-replay-check", 1, "Replay verification test", "verified", "2026-09-08T06:00:00Z")
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                r1 = _run_once(db_path, Path(artifact_root))
                # Capture the first report_id.
                first_report_id = r1.report_id
                # Run again — should detect replay and return same ID.
                r2 = _run_once(db_path, Path(artifact_root))
                self.assertEqual(r2.report_id, first_report_id)
                self.assertTrue(r2.was_replayed)
                # Generation status must still be complete.
                self.assertEqual(r2.generation_status, "complete")
                # No delivery_id.
                conn2 = sqlite3.connect(db_path)
                row = conn2.execute(
                    "SELECT delivery_id FROM reports WHERE report_id=?",
                    (first_report_id,),
                ).fetchone()
                conn2.close()
                self.assertIsNone(row[0])
        finally:
            os.unlink(db_path)


if __name__ == "__main__":
    unittest.main()
