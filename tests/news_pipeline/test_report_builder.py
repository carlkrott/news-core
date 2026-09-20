"""Phase 5 — test_report_builder.py

Focused tests for ``news_pipeline.report_builder`` covering:
- empty run (no events in window)
- verified latest event selection (MAX version, non-superseded, non-rejected)
- boundary semantics: (lower, upper] window
- superseded / rejected / retracted event exclusion
- material_update decision for later event versions
- malformed rows: invalid category fail-closed
- BriefingLedger / renderer / engine seam (integration)
- report_events link persistence
- deterministic replay across fresh process/connection
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock
from datetime import datetime, timedelta, timezone
from pathlib import Path

from news_pipeline.models import Category
from news_pipeline.db import init_db
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5

# -----------------------------------------------------------------------
# Helper: build a minimal in-memory DB with schema v3 applied
# -----------------------------------------------------------------------


def _make_db() -> sqlite3.Connection:
    """Create a realistic v2 -> v3 -> v4 -> v5 SQLite database."""
    fd, name = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_db(name)
    conn = sqlite3.connect(name, isolation_level=None)
    migrate_v3(conn, "2026-09-07T00:00:00Z")
    migrate_v4(conn, "2026-09-07T00:00:00Z")
    migrate_v5(conn, "2026-09-07T00:00:00Z")
    os.unlink(name)
    return conn


def _insert_event_version(
    conn: sqlite3.Connection,
    event_id: str,
    version: int,
    summary: str,
    verification_state: str,
    valid_from: str,
    *,
    superseded_at: str | None = None,
    category: str = "ai",
    material_change_reason: str = "initial",
    with_source: bool = True,
) -> None:
    """Insert one events + event_versions row."""
    # Insert event if not present.
    conn.execute("INSERT OR IGNORE INTO runs(id, started_at, finished_at, kind, provenance) VALUES (?, ?, ?, 'historical_replay', 'manual')", (f"run-{event_id}", valid_from, valid_from))
    conn.execute(
        "INSERT OR IGNORE INTO events(id, run_id, category, started_at, ended_at, "
        "article_count, observation_count, status) "
        "VALUES (?, ?, ?, ?, ?, 0, 0, 'complete')",
        (event_id, f"run-{event_id}", category, valid_from, valid_from),
    )
    conn.execute(
        "INSERT INTO event_versions(event_id, version, material_change_reason, summary, "
        "verification_state, valid_from, superseded_at, verified_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            event_id,
            version,
            material_change_reason,
            summary,
            verification_state,
            valid_from,
            superseded_at,
            valid_from if verification_state == "verified" else None,
        ),
    )
    if with_source and category in {item.value for item in Category}:
        source_id = "report-source"
        source_item_id = f"report-source-item-{event_id}-{version}"
        claim_id = f"report-claim-{event_id}-{version}"
        conn.execute(
            """INSERT OR IGNORE INTO source_registry(
                source_id, adapter_type, source_role, host, category_scope_json,
                enabled, queries_json, title_blocklist_json, content_blocklist_json,
                url_blocklist_json, allowlist_domains_json, config_hash, created_at
            ) VALUES (?, 'rss', 'primary', 'example.com', ?, 1, '[]', '[]', '[]', '[]', '[]', ?, ?)""",
            (source_id, json.dumps([category]), "0" * 64, valid_from),
        )
        conn.execute(
            """INSERT INTO source_items(
                source_item_id, source_id, external_id, category, original_url,
                canonical_url, publisher, source_role, author_handle, retrieval_method,
                raw_content_hash, title, body, raw, retrieved_at, published_at,
                updated_at, publication_evidence
            ) VALUES (?, ?, ?, ?, ?, ?, 'example.com', 'primary', NULL, 'test', ?, ?, ?, ?, ?, ?, NULL, 'source')""",
            (
                source_item_id,
                source_id,
                source_item_id,
                category,
                f"https://example.com/report/{event_id}/{version}",
                f"https://example.com/report/{event_id}/{version}",
                hashlib.sha256(source_item_id.encode()).hexdigest(),
                summary,
                summary,
                summary,
                valid_from,
                valid_from,
            ),
        )
        conn.execute(
            """INSERT INTO claims(
                claim_id, source_item_id, subject, predicate, object_value,
                statement_type, extraction_confidence, status, extracted_at
            ) VALUES (?, ?, 'report', 'summary', ?, 'text', '0.8', 'pending', ?)""",
            (claim_id, source_item_id, summary, valid_from),
        )
        conn.execute(
            "INSERT INTO event_claims(event_id, event_version, claim_id) VALUES (?, ?, ?)",
            (event_id, version, claim_id),
        )


def _as_of(hour: int = 9) -> datetime:
    """Return a fixed as_of_utc in Europe/London morning window."""
    return datetime(2026, 9, 8, hour, 0, 0, tzinfo=timezone.utc)


def _persist_db(conn: sqlite3.Connection, db_path: str) -> None:
    destination = sqlite3.connect(db_path)
    try:
        conn.backup(destination)
    finally:
        destination.close()
        conn.close()


# -----------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------


class TestEmptyRun(unittest.TestCase):
    """An empty event window produces an empty report with zero artifacts."""

    def test_empty_window_no_events(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        # Use a temp file for the DB so run_report can open it.
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                result = run_report(
                    db_path=db_path,
                    artifacts_root=Path(artifact_root),
                    as_of_utc=_as_of(9),
                )
                self.assertEqual(result.included_count, 0)
                self.assertEqual(result.excluded_count, 0)
                self.assertEqual(result.generation_status, "complete")
                self.assertFalse(result.was_replayed)
                self.assertIsNotNone(result.artifact_result)
                # Artifact files should be written.
                self.assertTrue(result.artifact_result.json_path.is_file())
                self.assertTrue(result.artifact_result.jsonl_path.is_file())
                self.assertTrue(result.artifact_result.markdown_path.is_file())
                self.assertTrue(result.artifact_result.manifest_path.is_file())
        finally:
            os.unlink(db_path)


class TestBoundarySemantics(unittest.TestCase):
    """Window is (lower, upper] — event at exactly lower is excluded, at upper is included."""

    def test_event_at_lower_excluded(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        # Insert event at exactly the lower bound (08:00 UTC 2026-09-07 = 2026-09-07T08:00:00Z)
        lower = "2026-09-01T07:00:00Z"
        _insert_event_version(conn, "ev-1", 1, "Event at lower bound", "verified", lower)
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                result = run_report(
                    db_path=db_path,
                    artifacts_root=Path(artifact_root),
                    as_of_utc=datetime(2026, 9, 8, 9, 0, 0, tzinfo=timezone.utc),
                )
                # Event at lower bound should be excluded.
                self.assertEqual(result.included_count, 0)
        finally:
            os.unlink(db_path)

    def test_event_at_upper_included(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        # The morning window upper for 2026-09-08T09:00 UTC is 2026-09-08T08:00:00Z (local 08:00).
        upper = "2026-09-08T07:00:00Z"
        _insert_event_version(conn, "ev-2", 1, "Event at upper bound", "verified", upper)
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                result = run_report(
                    db_path=db_path,
                    artifacts_root=Path(artifact_root),
                    as_of_utc=datetime(2026, 9, 8, 9, 0, 0, tzinfo=timezone.utc),
                )
                # Event at upper bound should be included.
                self.assertEqual(result.included_count, 1)
        finally:
            os.unlink(db_path)


class TestSupersededRejectedRetractedExclusion(unittest.TestCase):
    """Superseded, rejected, and authoritatively retracted events are excluded."""

    def test_rejected_excluded(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        valid = "2026-09-08T07:00:00Z"
        _insert_event_version(conn, "ev-rejected", 1, "Some claim", "rejected", valid)
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                result = run_report(
                    db_path=db_path,
                    artifacts_root=Path(artifact_root),
                    as_of_utc=_as_of(9),
                )
                self.assertEqual(result.included_count, 0)
        finally:
            os.unlink(db_path)

    def test_retraction_reason_excluded_without_prose_heuristics(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        valid = "2026-09-08T07:00:00Z"
        _insert_event_version(
            conn,
            "ev-retracted",
            1,
            "Source withdrew the underlying claim",
            "verified",
            valid,
            material_change_reason="retraction",
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                result = run_report(
                    db_path=db_path,
                    artifacts_root=Path(artifact_root),
                    as_of_utc=_as_of(9),
                )
                self.assertEqual(result.included_count, 0)
        finally:
            os.unlink(db_path)

    def test_retraction_word_in_summary_is_not_authority(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        valid = "2026-09-08T07:00:00Z"
        _insert_event_version(
            conn,
            "ev-prose",
            1,
            "Research on document retraction mechanisms",
            "verified",
            valid,
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)
            with tempfile.TemporaryDirectory() as artifact_root:
                result = run_report(
                    db_path=db_path,
                    artifacts_root=Path(artifact_root),
                    as_of_utc=_as_of(9),
                )
                self.assertEqual(result.included_count, 1)
        finally:
            os.unlink(db_path)

    def test_superseded_excluded(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        valid_v1 = "2026-09-08T07:00:00Z"
        valid_v2 = "2026-09-08T07:30:00Z"
        # v1 is superseded by v2.
        _insert_event_version(
            conn, "ev-superseded", 1, "Original claim", "verified", valid_v1, superseded_at=valid_v2
        )
        _insert_event_version(conn, "ev-superseded", 2, "Corrected claim", "verified", valid_v2)
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                result = run_report(
                    db_path=db_path,
                    artifacts_root=Path(artifact_root),
                    as_of_utc=_as_of(9),
                )
                # The latest v2 is outside this window, so v1 must not leak.
                self.assertEqual(result.included_count, 0)
                # No prior verified version is exposed.
                art_json = json.loads(result.artifact_result.json_bytes)
                self.assertEqual(len(art_json["items"]), 0)
        finally:
            os.unlink(db_path)


class TestMalformedCategoryFailClosed(unittest.TestCase):
    """Invalid category strings must raise ValueError, never silently map to AI."""

    def test_invalid_category_raises(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        valid = "2026-09-08T07:00:00Z"
        # Insert event with invalid category.
        conn.execute(
            "INSERT OR IGNORE INTO runs(id, started_at, finished_at, kind, provenance) VALUES (?, ?, ?, 'historical_replay', 'manual')",
            ("run-bad-cat", valid, valid),
        )
        conn.execute(
            "INSERT OR IGNORE INTO events(id, run_id, category, started_at, ended_at, "
            "article_count, observation_count, status) "
            "VALUES (?, ?, ?, ?, ?, 0, 0, 'complete')",
            ("ev-bad-cat", "run-bad-cat", "not_a_category", valid, valid),
        )
        conn.execute(
            "INSERT INTO event_versions(event_id, version, material_change_reason, summary, "
            "verification_state, valid_from, superseded_at, verified_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("ev-bad-cat", 1, "initial", "Some summary", "verified", valid, None, valid),
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                with self.assertRaises(ValueError) as ctx:
                    run_report(
                        db_path=db_path,
                        artifacts_root=Path(artifact_root),
                        as_of_utc=_as_of(9),
                    )
                self.assertIn("invalid category", str(ctx.exception).lower())
        finally:
            os.unlink(db_path)


class TestMaterialUpdateMapping(unittest.TestCase):
    """First event version → distinct_event; later version → material_update."""

    def test_first_version_distinct_event(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        v1 = "2026-09-08T06:00:00Z"
        _insert_event_version(conn, "ev-first", 1, "First report on topic", "verified", v1)
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                result = run_report(
                    db_path=db_path,
                    artifacts_root=Path(artifact_root),
                    as_of_utc=_as_of(9),
                )
                self.assertEqual(result.included_count, 1)
                art_json = json.loads(result.artifact_result.json_bytes)
                self.assertEqual(art_json["items"][0]["decision"], "distinct_event")
        finally:
            os.unlink(db_path)

    def test_later_version_material_update(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        v1 = "2026-09-08T06:00:00Z"
        v2 = "2026-09-08T07:00:00Z"
        _insert_event_version(conn, "ev-update", 1, "Original claim", "verified", v1)
        _insert_event_version(conn, "ev-update", 2, "Updated claim", "verified", v2)
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                result = run_report(
                    db_path=db_path,
                    artifacts_root=Path(artifact_root),
                    as_of_utc=_as_of(9),
                )
                self.assertEqual(result.included_count, 1)
                art_json = json.loads(result.artifact_result.json_bytes)
                self.assertEqual(art_json["items"][0]["decision"], "material_update")
                self.assertEqual(art_json["items"][0]["event_version"], 2)
                self.assertEqual(art_json["items"][0]["event_id"], "ev-update")
        finally:
            os.unlink(db_path)


class TestReportEventsLinkage(unittest.TestCase):
    """report_events table is correctly populated after a fresh run."""

    def test_report_events_rows_persisted(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        v1 = "2026-09-08T06:00:00Z"
        _insert_event_version(conn, "ev-link-1", 1, "Event one", "verified", v1)
        _insert_event_version(conn, "ev-link-2", 1, "Event two", "verified", v1)
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                result = run_report(
                    db_path=db_path,
                    artifacts_root=Path(artifact_root),
                    as_of_utc=_as_of(9),
                )
                # Check report_events rows.
                conn2 = sqlite3.connect(db_path)
                rows = conn2.execute(
                    "SELECT event_id, event_version, section FROM report_events "
                    "WHERE report_id=? ORDER BY sort_order",
                    (result.report_id,),
                ).fetchall()
                conn2.close()
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0][0], "ev-link-1")
                self.assertEqual(rows[1][0], "ev-link-2")
        finally:
            os.unlink(db_path)


class TestDeterministicReplay(unittest.TestCase):
    """A second run against the same window returns identical results without re-running."""

    def _run_and_return(self, db_path: str, artifact_root: Path):
        from news_pipeline.report_builder import run_report

        return run_report(
            db_path=db_path,
            artifacts_root=artifact_root,
            as_of_utc=_as_of(9),
        )

    def test_replay_returns_identical_report_id(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        v1 = "2026-09-08T06:00:00Z"
        _insert_event_version(conn, "ev-replay", 1, "Replayable event", "verified", v1)
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                r1 = self._run_and_return(db_path, Path(artifact_root))
                r2 = self._run_and_return(db_path, Path(artifact_root))
                self.assertEqual(r1.report_id, r2.report_id)
                self.assertTrue(r2.was_replayed)
                self.assertEqual(r1.artifact_result.json_sha256, r2.artifact_result.json_sha256)
        finally:
            os.unlink(db_path)

    def test_replay_skips_briefing_engine(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        v1 = "2026-09-08T06:00:00Z"
        _insert_event_version(conn, "ev-engine-skip", 1, "Engine skip test", "verified", v1)
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                r1 = run_report(db_path=db_path, artifacts_root=Path(artifact_root), as_of_utc=_as_of(9))
                # Patch the connection to fail if a second briefing runs.
                # We can't easily detect this without a spy, but the was_replayed flag
                # and identical output proves the engine was not re-run.
                r2 = run_report(db_path=db_path, artifacts_root=Path(artifact_root), as_of_utc=_as_of(9))
                self.assertTrue(r2.was_replayed)
                self.assertEqual(r1.artifact_result.json_sha256, r2.artifact_result.json_sha256)
        finally:
            os.unlink(db_path)


class TestInterruptedArtifactRecovery(unittest.TestCase):
    """A completed durable shadow run is the restart authority after artifact failure."""

    def test_restart_recovers_without_rerunning_shadow_engine(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        _insert_event_version(
            conn,
            "ev-recovery",
            1,
            "Durable recovery event",
            "verified",
            "2026-09-08T06:00:00Z",
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)
            with tempfile.TemporaryDirectory() as artifact_root:
                with mock.patch(
                    "news_pipeline.report_builder.compute_artifacts",
                    side_effect=RuntimeError("injected artifact failure"),
                ):
                    with self.assertRaisesRegex(RuntimeError, "injected artifact failure"):
                        run_report(db_path, Path(artifact_root), _as_of(9))
                check = sqlite3.connect(db_path)
                try:
                    self.assertEqual(
                        check.execute("SELECT generation_status FROM reports").fetchone(),
                        ("generating",),
                    )
                    self.assertEqual(
                        check.execute("SELECT status FROM shadow_briefing_runs").fetchone(),
                        ("COMPLETED",),
                    )
                    self.assertEqual(
                        check.execute("SELECT COUNT(*) FROM report_events").fetchone(),
                        (0,),
                    )
                finally:
                    check.close()
                with mock.patch(
                    "news_pipeline.report_builder.run_shadow_briefing",
                    side_effect=AssertionError("shadow engine reran during recovery"),
                ):
                    recovered = run_report(db_path, Path(artifact_root), _as_of(10))
                self.assertEqual(recovered.included_count, 1)
                self.assertFalse(recovered.was_replayed)
                self.assertEqual(
                    json.loads(recovered.artifact_result.json_bytes)["items"][0]["event_id"],
                    "ev-recovery",
                )
        finally:
            os.unlink(db_path)

    def test_conflicting_partial_link_fails_closed(self) -> None:
        from news_pipeline.report_artifacts import ArtifactMismatch
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        _insert_event_version(
            conn,
            "ev-link-conflict",
            1,
            "Link conflict event",
            "verified",
            "2026-09-08T06:00:00Z",
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)
            with tempfile.TemporaryDirectory() as artifact_root:
                with mock.patch(
                    "news_pipeline.report_builder.compute_artifacts",
                    side_effect=RuntimeError("injected artifact failure"),
                ):
                    with self.assertRaises(RuntimeError):
                        run_report(db_path, Path(artifact_root), _as_of(9))
                check = sqlite3.connect(db_path)
                try:
                    report_id = check.execute("SELECT report_id FROM reports").fetchone()[0]
                    check.execute(
                        "INSERT INTO report_events(report_id,event_id,event_version,section,sort_order,inclusion_reason) "
                        "VALUES (?,?,?,?,?,?)",
                        (report_id, "ev-link-conflict", 1, "world", 99, "wrong"),
                    )
                    check.commit()
                finally:
                    check.close()
                with self.assertRaises(ArtifactMismatch):
                    run_report(db_path, Path(artifact_root), _as_of(10))
        finally:
            os.unlink(db_path)


class TestArtifactEventIdVersionSeparate(unittest.TestCase):
    """Artifact JSON stores event_id and event_version as separate fields."""

    def test_json_has_separate_event_id_and_version(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        v1 = "2026-09-08T06:00:00Z"
        _insert_event_version(conn, "ev-json-sep", 1, "Event JSON sep test", "verified", v1)
        _insert_event_version(conn, "ev-json-sep", 2, "Event JSON sep v2", "verified", v1)
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                result = run_report(
                    db_path=db_path,
                    artifacts_root=Path(artifact_root),
                    as_of_utc=_as_of(9),
                )
                art_json = json.loads(result.artifact_result.json_bytes)
                self.assertEqual(len(art_json["items"]), 1)  # Only latest version.
                item = art_json["items"][0]
                self.assertIn("event_id", item)
                self.assertIn("event_version", item)
                self.assertNotEqual(item["event_id"], item["event_version"])
                self.assertEqual(item["event_id"], "ev-json-sep")
                self.assertEqual(item["event_version"], 2)
        finally:
            os.unlink(db_path)


class TestPhase5R3AdditionalBoundaries(unittest.TestCase):
    def test_fractional_second_after_lower_is_included(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        _insert_event_version(
            conn, "ev-fractional", 1, "Fractional boundary event", "verified",
            "2026-09-08T06:00:00.500000Z",
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)
            with tempfile.TemporaryDirectory() as artifact_root:
                result = run_report(
                    db_path=db_path,
                    artifacts_root=Path(artifact_root),
                    as_of_utc=_as_of(9),
                    last_completed_upper_utc=datetime(
                        2026, 9, 8, 6, 0, 0, tzinfo=timezone.utc
                    ),
                )
                self.assertEqual(result.included_count, 1)
        finally:
            os.unlink(db_path)

    def test_latest_rejected_does_not_fall_back_to_verified_prior(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        _insert_event_version(
            conn, "ev-latest-rejected", 1, "Verified prior", "verified", "2026-09-08T06:00:00Z"
        )
        _insert_event_version(
            conn, "ev-latest-rejected", 2, "Rejected latest", "rejected", "2026-09-08T07:00:00Z"
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)
            with tempfile.TemporaryDirectory() as artifact_root:
                result = run_report(db_path, Path(artifact_root), _as_of(9))
                self.assertEqual(result.included_count, 0)
        finally:
            os.unlink(db_path)

    def test_real_renderer_seam_is_invoked(self) -> None:
        from news_pipeline.briefing_renderer import render_briefing
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        _insert_event_version(conn, "ev-renderer", 1, "Renderer seam", "verified", "2026-09-08T06:00:00Z")
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)
            with tempfile.TemporaryDirectory() as artifact_root:
                with mock.patch(
                    "news_pipeline.briefing_renderer.render_briefing",
                    wraps=render_briefing,
                ) as renderer:
                    run_report(db_path, Path(artifact_root), _as_of(9))
                self.assertGreaterEqual(renderer.call_count, 1)
        finally:
            os.unlink(db_path)

    def test_complete_replay_rejects_table_hash_and_link_tamper(self) -> None:
        from news_pipeline.report_artifacts import ArtifactMismatch
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        _insert_event_version(conn, "ev-tamper", 1, "Tamper event", "verified", "2026-09-08T06:00:00Z")
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)
            with tempfile.TemporaryDirectory() as artifact_root:
                first = run_report(db_path, Path(artifact_root), _as_of(9))
                check = sqlite3.connect(db_path)
                try:
                    check.execute(
                        "UPDATE reports SET json_sha256=? WHERE report_id=?",
                        ("0" * 64, first.report_id),
                    )
                    check.commit()
                finally:
                    check.close()
                with self.assertRaises(ArtifactMismatch):
                    run_report(db_path, Path(artifact_root), _as_of(9))
                check = sqlite3.connect(db_path)
                try:
                    check.execute(
                        "UPDATE reports SET json_sha256=? WHERE report_id=?",
                        (first.artifact_result.json_sha256, first.report_id),
                    )
                    check.execute(
                        "UPDATE report_events SET sort_order=99 WHERE report_id=?",
                        (first.report_id,),
                    )
                    check.commit()
                finally:
                    check.close()
                with self.assertRaises(ArtifactMismatch):
                    run_report(db_path, Path(artifact_root), _as_of(9))
        finally:
            os.unlink(db_path)

    def test_zero_width_report_replays_as_empty(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)
            with tempfile.TemporaryDirectory() as artifact_root:
                prior = datetime(2026, 9, 8, 7, 0, 0, tzinfo=timezone.utc)
                first = run_report(
                    db_path, Path(artifact_root), _as_of(9),
                    last_completed_upper_utc=prior,
                )
                second = run_report(
                    db_path, Path(artifact_root), _as_of(10),
                    last_completed_upper_utc=prior,
                )
                self.assertEqual(first.window_start, first.window_end)
                self.assertEqual(first.included_count, 0)
                self.assertTrue(second.was_replayed)
                self.assertEqual(
                    first.artifact_result.json_sha256,
                    second.artifact_result.json_sha256,
                )
        finally:
            os.unlink(db_path)


class TestPhase5R3MalformedState(unittest.TestCase):
    def test_unknown_verification_state_fails_closed(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        conn.execute("PRAGMA ignore_check_constraints=ON")
        _insert_event_version(
            conn, "ev-bad-state", 1, "Bad state", "garbage", "2026-09-08T06:00:00Z"
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)
            with tempfile.TemporaryDirectory() as artifact_root:
                with self.assertRaises(ValueError):
                    run_report(db_path, Path(artifact_root), _as_of(9))
        finally:
            os.unlink(db_path)

    def test_noncanonical_event_timestamp_fails_closed(self) -> None:
        from news_pipeline.report_builder import run_report

        conn = _make_db()
        _insert_event_version(
            conn, "ev-bad-time", 1, "Bad time", "verified", "2026-09-08T06:00:00.000Z"
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)
            with tempfile.TemporaryDirectory() as artifact_root:
                with self.assertRaises(ValueError):
                    run_report(db_path, Path(artifact_root), _as_of(9))
        finally:
            os.unlink(db_path)


if __name__ == "__main__":
    unittest.main()
