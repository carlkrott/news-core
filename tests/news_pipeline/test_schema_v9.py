"""Tests for transactional additive schema-v9 migration."""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from news_pipeline.db import init_db
from news_pipeline.delivery_schema_v6 import migrate_v6
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5
from news_pipeline.schema_v7 import migrate_v7
from news_pipeline.schema_v8 import migrate_v8
from news_pipeline.schema_v9 import (
    SCHEMA_VERSION,
    V9_COLUMNS,
    V9_TABLES,
    apply_v9,
    migrate_v9,
    validate_v9,
)

APPLIED_AT = "2026-09-20T12:00:00Z"
REPLAY_AT = "2026-09-20T12:05:00Z"
VALID_HASH_A = "a" * 64
VALID_HASH_B = "b" * 64
VALID_HASH_C = "c" * 64


class SchemaV9Tests(unittest.TestCase):
    def _build_v8_db(self) -> tuple[tempfile.TemporaryDirectory[str], Path, sqlite3.Connection]:
        directory = tempfile.TemporaryDirectory()
        path = Path(directory.name) / "state.db"
        init_db(str(path))
        connection = sqlite3.connect(path, isolation_level=None)
        connection.execute("PRAGMA foreign_keys=ON")
        migrate_v3(connection, APPLIED_AT)
        migrate_v4(connection, APPLIED_AT)
        migrate_v5(connection, APPLIED_AT)
        migrate_v6(connection, APPLIED_AT)
        migrate_v7(connection, APPLIED_AT)
        migrate_v8(connection, APPLIED_AT)
        return directory, path, connection

    def _insert_report(self, connection: sqlite3.Connection, report_id: str = "rep-001") -> str:
        connection.execute(
            """INSERT INTO reports(
                report_id, window_start, window_end, generation_status,
                json_sha256, jsonl_sha256, markdown_sha256, manifest_sha256,
                delivery_state, delivery_id, created_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                report_id,
                "2026-09-20T00:00:00Z",
                "2026-09-20T12:00:00Z",
                "complete",
                VALID_HASH_A,
                VALID_HASH_A,
                VALID_HASH_A,
                VALID_HASH_A,
                "not_attempted",
                None,
                APPLIED_AT,
            ),
        )
        return report_id

    def test_first_apply_returns_true_replay_returns_false_idempotently(self) -> None:
        directory, _path, connection = self._build_v8_db()
        try:
            self.assertTrue(migrate_v9(connection, APPLIED_AT))
            self.assertEqual(
                connection.execute("SELECT applied_at FROM schema_migrations WHERE version=9").fetchone(),
                (APPLIED_AT,),
            )
            # Replay with a different timestamp returns False without modifying marker
            self.assertFalse(migrate_v9(connection, REPLAY_AT))
            self.assertEqual(
                connection.execute("SELECT applied_at FROM schema_migrations WHERE version=9").fetchone(),
                (APPLIED_AT,),
            )
            # apply_v9 alias
            self.assertFalse(apply_v9(connection, REPLAY_AT))
            validate_v9(connection)
        finally:
            connection.close()
            directory.cleanup()

    def test_ingest_schema_gate_accepts_v9(self) -> None:
        from news_pipeline.ingest_runner import _verify_schema

        directory, path, connection = self._build_v8_db()
        try:
            migrate_v9(connection, APPLIED_AT)
            _verify_schema(path)
        finally:
            connection.close()
            directory.cleanup()

    def test_exact_columns_and_tables_and_version(self) -> None:
        self.assertEqual(SCHEMA_VERSION, 9)
        self.assertEqual(V9_TABLES, ("subject_reports", "subject_delivery_outbox", "subject_delivery_attempts"))
        directory, _path, connection = self._build_v8_db()
        try:
            migrate_v9(connection, APPLIED_AT)
            for table, expected_columns in V9_COLUMNS.items():
                actual_columns = tuple(
                    row[1] for row in connection.execute(f"PRAGMA table_info({table})")
                )
                self.assertEqual(actual_columns, expected_columns, f"Column mismatch in {table}")
                # Verify column ordering matches V9_COLUMNS
                row_names = tuple(
                    row[0] for row in connection.execute("SELECT name FROM pragma_table_info(?)", (table,))
                )
                self.assertEqual(row_names, expected_columns, f"pragma_table_info mismatch in {table}")
        finally:
            connection.close()
            directory.cleanup()

    def test_exact_indexes_exist(self) -> None:
        directory, _path, connection = self._build_v8_db()
        try:
            migrate_v9(connection, APPLIED_AT)
            indexes = {
                row[0]: row[1]
                for row in connection.execute(
                    "SELECT name, tbl_name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
                )
            }
            self.assertEqual(indexes.get("idx_subject_reports_lookup"), "subject_reports")
            self.assertEqual(indexes.get("idx_subject_delivery_outbox_state"), "subject_delivery_outbox")
            self.assertEqual(indexes.get("idx_subject_delivery_attempts_ordinal"), "subject_delivery_attempts")

            # Check index column composition and order
            info_reports = [row[2] for row in connection.execute("PRAGMA index_info(idx_subject_reports_lookup)")]
            self.assertEqual(info_reports, ["parent_report_id", "subject_id", "revision"])

            info_outbox = [row[2] for row in connection.execute("PRAGMA index_info(idx_subject_delivery_outbox_state)")]
            self.assertEqual(info_outbox, ["state", "updated_at"])

            info_attempts = [row[2] for row in connection.execute("PRAGMA index_info(idx_subject_delivery_attempts_ordinal)")]
            self.assertEqual(info_attempts, ["subject_report_id", "ordinal"])
        finally:
            connection.close()
            directory.cleanup()

    def test_foreign_key_constraints_enforced(self) -> None:
        directory, _path, connection = self._build_v8_db()
        try:
            migrate_v9(connection, APPLIED_AT)
            # 1. subject_reports references reports(report_id)
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_reports(
                        subject_report_id, parent_report_id, subject_id, revision,
                        content_sha256, story_count, created_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)""",
                    ("srep-001", "nonexistent-report", "ai", 1, VALID_HASH_A, 5, APPLIED_AT),
                )

            # Insert valid parent report
            self._insert_report(connection, "rep-001")
            connection.execute(
                """INSERT INTO subject_reports(
                    subject_report_id, parent_report_id, subject_id, revision,
                    content_sha256, story_count, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)""",
                ("srep-001", "rep-001", "ai", 1, VALID_HASH_A, 5, APPLIED_AT),
            )

            # 2. subject_delivery_outbox references subject_reports(subject_report_id)
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_delivery_outbox(
                        subject_report_id, idempotency_key, channel, recipient_hash,
                        content_sha256, state, current_attempt_id, created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    ("srep-nonexistent", "idem-001", "telegram", VALID_HASH_B, VALID_HASH_A, "prepared", None, APPLIED_AT, APPLIED_AT),
                )

            # Insert valid outbox row
            connection.execute(
                """INSERT INTO subject_delivery_outbox(
                    subject_report_id, idempotency_key, channel, recipient_hash,
                    content_sha256, state, current_attempt_id, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ("srep-001", "idem-001", "telegram", VALID_HASH_B, VALID_HASH_A, "prepared", None, APPLIED_AT, APPLIED_AT),
            )

            # 3. subject_delivery_attempts references subject_delivery_outbox(subject_report_id)
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_delivery_attempts(
                        attempt_id, subject_report_id, ordinal, state, recipient_hash,
                        content_sha256, prepared_at, completed_at, message_ids_json, error_code, error_detail)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    ("att-001", "srep-nonexistent", 1, "prepared", VALID_HASH_B, VALID_HASH_A, APPLIED_AT, None, None, None, None),
                )

            # Valid attempt insert
            connection.execute(
                """INSERT INTO subject_delivery_attempts(
                    attempt_id, subject_report_id, ordinal, state, recipient_hash,
                    content_sha256, prepared_at, completed_at, message_ids_json, error_code, error_detail)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ("att-001", "srep-001", 1, "prepared", VALID_HASH_B, VALID_HASH_A, APPLIED_AT, None, None, None, None),
            )

            # FK check and integrity check pass
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        finally:
            connection.close()
            directory.cleanup()

    def test_check_constraints_and_uniques_subject_reports(self) -> None:
        directory, _path, connection = self._build_v8_db()
        try:
            migrate_v9(connection, APPLIED_AT)
            self._insert_report(connection, "rep-001")

            valid_subjects = (
                "ai", "world", "audio_engineering", "professional_av",
                "hardware", "fantasy_novel", "our_setup"
            )
            for idx, subj in enumerate(valid_subjects):
                connection.execute(
                    """INSERT INTO subject_reports(
                        subject_report_id, parent_report_id, subject_id, revision,
                        content_sha256, story_count, created_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)""",
                    (f"srep-{idx}", "rep-001", subj, 1, f"{idx:02d}" + "a" * 62, 0, APPLIED_AT),
                )

            # Invalid subject rejected
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_reports(
                        subject_report_id, parent_report_id, subject_id, revision,
                        content_sha256, story_count, created_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)""",
                    ("srep-bad-subj", "rep-001", "bad_subject", 1, VALID_HASH_A, 0, APPLIED_AT),
                )

            # revision < 1 rejected
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_reports(
                        subject_report_id, parent_report_id, subject_id, revision,
                        content_sha256, story_count, created_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)""",
                    ("srep-bad-rev", "rep-001", "ai", 0, VALID_HASH_A, 0, APPLIED_AT),
                )

            # content_sha256 != 64 chars rejected
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_reports(
                        subject_report_id, parent_report_id, subject_id, revision,
                        content_sha256, story_count, created_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)""",
                    ("srep-bad-hash", "rep-001", "ai", 2, "short_hash", 0, APPLIED_AT),
                )

            # story_count < 0 rejected
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_reports(
                        subject_report_id, parent_report_id, subject_id, revision,
                        content_sha256, story_count, created_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)""",
                    ("srep-bad-count", "rep-001", "ai", 2, VALID_HASH_A, -1, APPLIED_AT),
                )

            # UNIQUE(parent_report_id, subject_id, revision) violation
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_reports(
                        subject_report_id, parent_report_id, subject_id, revision,
                        content_sha256, story_count, created_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)""",
                    ("srep-dup-rev", "rep-001", "ai", 1, "99" + "b" * 62, 1, APPLIED_AT),
                )

            # UNIQUE(parent_report_id, subject_id, content_sha256) violation
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_reports(
                        subject_report_id, parent_report_id, subject_id, revision,
                        content_sha256, story_count, created_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)""",
                    ("srep-dup-hash", "rep-001", "ai", 2, "00" + "a" * 62, 1, APPLIED_AT),
                )
        finally:
            connection.close()
            directory.cleanup()

    def test_check_constraints_and_uniques_subject_delivery_outbox(self) -> None:
        directory, _path, connection = self._build_v8_db()
        try:
            migrate_v9(connection, APPLIED_AT)
            self._insert_report(connection, "rep-001")
            connection.execute(
                """INSERT INTO subject_reports(
                    subject_report_id, parent_report_id, subject_id, revision,
                    content_sha256, story_count, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)""",
                ("srep-001", "rep-001", "ai", 1, VALID_HASH_A, 5, APPLIED_AT),
            )
            connection.execute(
                """INSERT INTO subject_reports(
                    subject_report_id, parent_report_id, subject_id, revision,
                    content_sha256, story_count, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)""",
                ("srep-002", "rep-001", "world", 1, VALID_HASH_B, 2, APPLIED_AT),
            )

            # Valid outbox row with null channel and recipient_hash
            connection.execute(
                """INSERT INTO subject_delivery_outbox(
                    subject_report_id, idempotency_key, channel, recipient_hash,
                    content_sha256, state, current_attempt_id, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ("srep-001", "idem-001", None, None, VALID_HASH_A, "prepared", None, APPLIED_AT, APPLIED_AT),
            )

            # channel empty string rejected
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_delivery_outbox(
                        subject_report_id, idempotency_key, channel, recipient_hash,
                        content_sha256, state, current_attempt_id, created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    ("srep-002", "idem-empty-chan", "", VALID_HASH_B, VALID_HASH_A, "prepared", None, APPLIED_AT, APPLIED_AT),
                )

            # channel whitespace-only rejected
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_delivery_outbox(
                        subject_report_id, idempotency_key, channel, recipient_hash,
                        content_sha256, state, current_attempt_id, created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    ("srep-002", "idem-bad-chan", "   ", VALID_HASH_B, VALID_HASH_A, "prepared", None, APPLIED_AT, APPLIED_AT),
                )

            # channel untrimmed leading or trailing whitespace rejected
            for untrimmed in (" telegram", "telegram ", "  telegram  ", " \t "):
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        """INSERT INTO subject_delivery_outbox(
                            subject_report_id, idempotency_key, channel, recipient_hash,
                            content_sha256, state, current_attempt_id, created_at, updated_at)
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        ("srep-002", f"idem-untrimmed-{untrimmed}", untrimmed, VALID_HASH_B, VALID_HASH_A, "prepared", None, APPLIED_AT, APPLIED_AT),
                    )

            # recipient_hash invalid length rejected
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_delivery_outbox(
                        subject_report_id, idempotency_key, channel, recipient_hash,
                        content_sha256, state, current_attempt_id, created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    ("srep-002", "idem-bad-recip", "telegram", "short", VALID_HASH_A, "prepared", None, APPLIED_AT, APPLIED_AT),
                )

            # content_sha256 invalid length rejected
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_delivery_outbox(
                        subject_report_id, idempotency_key, channel, recipient_hash,
                        content_sha256, state, current_attempt_id, created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    ("srep-002", "idem-bad-hash", "telegram", VALID_HASH_B, "short", "prepared", None, APPLIED_AT, APPLIED_AT),
                )

            # state invalid rejected
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_delivery_outbox(
                        subject_report_id, idempotency_key, channel, recipient_hash,
                        content_sha256, state, current_attempt_id, created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    ("srep-002", "idem-bad-state", "telegram", VALID_HASH_B, VALID_HASH_A, "invalid_state", None, APPLIED_AT, APPLIED_AT),
                )

            # duplicate idempotency_key rejected
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_delivery_outbox(
                        subject_report_id, idempotency_key, channel, recipient_hash,
                        content_sha256, state, current_attempt_id, created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    ("srep-002", "idem-001", "telegram", VALID_HASH_B, VALID_HASH_A, "prepared", None, APPLIED_AT, APPLIED_AT),
                )

            # Valid states: prepared, sent, failed, ambiguous, skipped
            valid_outbox_states = ("prepared", "sent", "failed", "ambiguous", "skipped")
            for idx, st in enumerate(valid_outbox_states):
                connection.execute(
                    "UPDATE subject_delivery_outbox SET state=? WHERE subject_report_id='srep-001'",
                    (st,),
                )
        finally:
            connection.close()
            directory.cleanup()

    def test_check_constraints_and_uniques_subject_delivery_attempts(self) -> None:
        directory, _path, connection = self._build_v8_db()
        try:
            migrate_v9(connection, APPLIED_AT)
            self._insert_report(connection, "rep-001")
            connection.execute(
                """INSERT INTO subject_reports(
                    subject_report_id, parent_report_id, subject_id, revision,
                    content_sha256, story_count, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)""",
                ("srep-001", "rep-001", "ai", 1, VALID_HASH_A, 5, APPLIED_AT),
            )
            connection.execute(
                """INSERT INTO subject_delivery_outbox(
                    subject_report_id, idempotency_key, channel, recipient_hash,
                    content_sha256, state, current_attempt_id, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ("srep-001", "idem-001", "telegram", VALID_HASH_B, VALID_HASH_A, "prepared", None, APPLIED_AT, APPLIED_AT),
            )

            # Valid attempt insert
            connection.execute(
                """INSERT INTO subject_delivery_attempts(
                    attempt_id, subject_report_id, ordinal, state, recipient_hash,
                    content_sha256, prepared_at, completed_at, message_ids_json, error_code, error_detail)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ("att-001", "srep-001", 1, "prepared", VALID_HASH_B, VALID_HASH_A, APPLIED_AT, None, None, None, None),
            )

            # ordinal < 1 rejected
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_delivery_attempts(
                        attempt_id, subject_report_id, ordinal, state, recipient_hash,
                        content_sha256, prepared_at, completed_at, message_ids_json, error_code, error_detail)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    ("att-bad-ord", "srep-001", 0, "prepared", VALID_HASH_B, VALID_HASH_A, APPLIED_AT, None, None, None, None),
                )

            # state not in prepared/sent/failed/ambiguous rejected (e.g. skipped is NOT valid for attempt)
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_delivery_attempts(
                        attempt_id, subject_report_id, ordinal, state, recipient_hash,
                        content_sha256, prepared_at, completed_at, message_ids_json, error_code, error_detail)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    ("att-bad-st", "srep-001", 2, "skipped", VALID_HASH_B, VALID_HASH_A, APPLIED_AT, None, None, None, None),
                )

            # recipient_hash not 64 chars rejected
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_delivery_attempts(
                        attempt_id, subject_report_id, ordinal, state, recipient_hash,
                        content_sha256, prepared_at, completed_at, message_ids_json, error_code, error_detail)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    ("att-bad-recip", "srep-001", 2, "prepared", "short", VALID_HASH_A, APPLIED_AT, None, None, None, None),
                )

            # content_sha256 not 64 chars rejected
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_delivery_attempts(
                        attempt_id, subject_report_id, ordinal, state, recipient_hash,
                        content_sha256, prepared_at, completed_at, message_ids_json, error_code, error_detail)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    ("att-bad-hash", "srep-001", 2, "prepared", VALID_HASH_B, "short", APPLIED_AT, None, None, None, None),
                )

            # UNIQUE(subject_report_id, ordinal) violation
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """INSERT INTO subject_delivery_attempts(
                        attempt_id, subject_report_id, ordinal, state, recipient_hash,
                        content_sha256, prepared_at, completed_at, message_ids_json, error_code, error_detail)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    ("att-002", "srep-001", 1, "sent", VALID_HASH_B, VALID_HASH_A, APPLIED_AT, APPLIED_AT, '["m1"]', None, None),
                )

            # Different ordinal succeeds
            connection.execute(
                """INSERT INTO subject_delivery_attempts(
                    attempt_id, subject_report_id, ordinal, state, recipient_hash,
                    content_sha256, prepared_at, completed_at, message_ids_json, error_code, error_detail)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ("att-002", "srep-001", 2, "sent", VALID_HASH_B, VALID_HASH_A, APPLIED_AT, APPLIED_AT, '["m1"]', None, None),
            )
        finally:
            connection.close()
            directory.cleanup()

    def test_reject_invalid_predecessor_db(self) -> None:
        # 1. Bare database with no schema
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "bare.db"
            con = sqlite3.connect(p)
            with self.assertRaisesRegex(ValueError, "schema v9 requires a valid schema v8 database"):
                migrate_v9(con, APPLIED_AT)
            with self.assertRaisesRegex(ValueError, "schema v9 requires a valid schema v8 database"):
                validate_v9(con)
            con.close()

        # 2. Database only up to v7
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "v7.db"
            init_db(str(p))
            con = sqlite3.connect(p, isolation_level=None)
            con.execute("PRAGMA foreign_keys=ON")
            migrate_v3(con, APPLIED_AT)
            migrate_v4(con, APPLIED_AT)
            migrate_v5(con, APPLIED_AT)
            migrate_v6(con, APPLIED_AT)
            migrate_v7(con, APPLIED_AT)
            with self.assertRaisesRegex(ValueError, "schema v9 requires a valid schema v8 database"):
                migrate_v9(con, APPLIED_AT)
            with self.assertRaisesRegex(ValueError, "schema v9 requires a valid schema v8 database"):
                validate_v9(con)
            con.close()

    def test_reject_partial_state_and_marker_drift(self) -> None:
        # Partial state without marker (for each v9 table)
        for tbl in V9_TABLES:
            directory, _path, connection = self._build_v8_db()
            try:
                connection.execute(f"CREATE TABLE {tbl}(id TEXT PRIMARY KEY)")
                with self.assertRaisesRegex(ValueError, "partial schema v9 state without migration marker"):
                    migrate_v9(connection, APPLIED_AT)
                self.assertIsNone(
                    connection.execute("SELECT 1 FROM schema_migrations WHERE version=9").fetchone()
                )
            finally:
                connection.close()
                directory.cleanup()

        # Marker present but tables missing
        directory, _path, connection = self._build_v8_db()
        try:
            connection.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES(9, ?)",
                (APPLIED_AT,),
            )
            with self.assertRaisesRegex(ValueError, "schema v9 is incomplete; missing tables"):
                validate_v9(connection)
            with self.assertRaisesRegex(ValueError, "schema v9 is incomplete; missing tables"):
                migrate_v9(connection, APPLIED_AT)
        finally:
            connection.close()
            directory.cleanup()

        # Marker present but column mismatch
        directory, _path, connection = self._build_v8_db()
        try:
            migrate_v9(connection, APPLIED_AT)
            connection.execute("ALTER TABLE subject_reports ADD COLUMN extra_col TEXT")
            with self.assertRaisesRegex(ValueError, "incompatible columns"):
                validate_v9(connection)
            with self.assertRaisesRegex(ValueError, "incompatible columns"):
                migrate_v9(connection, APPLIED_AT)
        finally:
            connection.close()
            directory.cleanup()

        # Marker present but index dropped
        directory, _path, connection = self._build_v8_db()
        try:
            migrate_v9(connection, APPLIED_AT)
            connection.execute("DROP INDEX idx_subject_reports_lookup")
            with self.assertRaisesRegex(ValueError, "schema v9 index idx_subject_reports_lookup is missing or incompatible"):
                validate_v9(connection)
        finally:
            connection.close()
            directory.cleanup()

        # Marker present but index columns mismatched
        directory, _path, connection = self._build_v8_db()
        try:
            migrate_v9(connection, APPLIED_AT)
            connection.execute("DROP INDEX idx_subject_reports_lookup")
            connection.execute("CREATE INDEX idx_subject_reports_lookup ON subject_reports(revision, subject_id)")
            with self.assertRaisesRegex(ValueError, "has incompatible columns"):
                validate_v9(connection)
        finally:
            connection.close()
            directory.cleanup()

        # Marker present but invalid applied_at in schema_migrations
        directory, _path, connection = self._build_v8_db()
        try:
            migrate_v9(connection, APPLIED_AT)
            connection.execute("UPDATE schema_migrations SET applied_at='bad_timestamp' WHERE version=9")
            with self.assertRaisesRegex(ValueError, "applied_at must be a UTC ISO-8601 timestamp ending in Z"):
                validate_v9(connection)
        finally:
            connection.close()
            directory.cleanup()

        # Missing marker when tables exist
        directory, _path, connection = self._build_v8_db()
        try:
            migrate_v9(connection, APPLIED_AT)
            connection.execute("DELETE FROM schema_migrations WHERE version=9")
            with self.assertRaisesRegex(ValueError, "schema v9 migration marker is missing"):
                validate_v9(connection)
            with self.assertRaisesRegex(ValueError, "partial schema v9 state without migration marker"):
                migrate_v9(connection, APPLIED_AT)
        finally:
            connection.close()
            directory.cleanup()

        # foreign_keys PRAGMA disabled
        directory, _path, connection = self._build_v8_db()
        try:
            migrate_v9(connection, APPLIED_AT)
            connection.execute("PRAGMA foreign_keys=OFF")
            with self.assertRaisesRegex(ValueError, "foreign-key enforcement must be enabled"):
                validate_v9(connection)
        finally:
            connection.close()
            directory.cleanup()

    def test_marker_present_wrong_primary_key_is_rejected(self) -> None:
        directory, _path, connection = self._build_v8_db()
        try:
            migrate_v9(connection, APPLIED_AT)
            connection.execute("DROP TABLE subject_delivery_attempts")
            connection.execute(
                """CREATE TABLE subject_delivery_attempts(
                    attempt_id TEXT,
                    subject_report_id TEXT NOT NULL REFERENCES subject_delivery_outbox(subject_report_id),
                    ordinal INTEGER NOT NULL CHECK(ordinal >= 1),
                    state TEXT NOT NULL CHECK(state IN ('prepared','sent','failed','ambiguous')),
                    recipient_hash TEXT NOT NULL CHECK(length(recipient_hash) = 64),
                    content_sha256 TEXT NOT NULL CHECK(length(content_sha256) = 64),
                    prepared_at TEXT NOT NULL,
                    completed_at TEXT,
                    message_ids_json TEXT,
                    error_code TEXT,
                    error_detail TEXT,
                    PRIMARY KEY(subject_report_id, ordinal)
                )"""
            )
            connection.execute(
                "CREATE INDEX idx_subject_delivery_attempts_ordinal ON subject_delivery_attempts(subject_report_id, ordinal)"
            )
            with self.assertRaisesRegex(ValueError, "incompatible primary key"):
                validate_v9(connection)
        finally:
            connection.close()
            directory.cleanup()

    def test_marker_present_wrong_foreign_key_targets_are_rejected(self) -> None:
        directory, _path, connection = self._build_v8_db()
        try:
            migrate_v9(connection, APPLIED_AT)
            connection.execute("DROP TABLE subject_delivery_attempts")
            connection.execute(
                """CREATE TABLE subject_delivery_attempts(
                    attempt_id TEXT PRIMARY KEY,
                    subject_report_id TEXT NOT NULL REFERENCES subject_reports(subject_report_id),
                    ordinal INTEGER NOT NULL CHECK(ordinal >= 1),
                    state TEXT NOT NULL CHECK(state IN ('prepared','sent','failed','ambiguous')),
                    recipient_hash TEXT NOT NULL CHECK(length(recipient_hash) = 64),
                    content_sha256 TEXT NOT NULL CHECK(length(content_sha256) = 64),
                    prepared_at TEXT NOT NULL,
                    completed_at TEXT,
                    message_ids_json TEXT,
                    error_code TEXT,
                    error_detail TEXT,
                    UNIQUE(subject_report_id, ordinal)
                )"""
            )
            connection.execute(
                "CREATE INDEX idx_subject_delivery_attempts_ordinal ON subject_delivery_attempts(subject_report_id, ordinal)"
            )
            with self.assertRaisesRegex(ValueError, "foreign keys are incompatible"):
                validate_v9(connection)
        finally:
            connection.close()
            directory.cleanup()

    def test_marker_present_foreign_key_violations_rejected(self) -> None:
        directory, _path, connection = self._build_v8_db()
        try:
            migrate_v9(connection, APPLIED_AT)
            connection.execute("PRAGMA foreign_keys=OFF")
            connection.execute(
                """INSERT INTO subject_reports(
                    subject_report_id, parent_report_id, subject_id, revision,
                    content_sha256, story_count, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)""",
                ("srep-orphan", "nonexistent-rep", "ai", 1, VALID_HASH_A, 0, APPLIED_AT),
            )
            connection.execute("PRAGMA foreign_keys=ON")
            with self.assertRaisesRegex(ValueError, "foreign-key violations"):
                validate_v9(connection)
        finally:
            connection.close()
            directory.cleanup()

    def test_rollback_on_failure(self) -> None:
        directory, _path, connection = self._build_v8_db()
        try:
            # Create a conflicting index on an unrelated table before migration
            connection.execute("CREATE TABLE unrelated(id TEXT)")
            connection.execute("CREATE INDEX idx_subject_reports_lookup ON unrelated(id)")
            with self.assertRaises(sqlite3.OperationalError):
                migrate_v9(connection, APPLIED_AT)
            # Ensure none of the v9 tables or marker persisted
            self.assertIsNone(
                connection.execute("SELECT 1 FROM schema_migrations WHERE version=9").fetchone()
            )
            tables = {
                row[0] for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            self.assertTrue(set(V9_TABLES).isdisjoint(tables))
        finally:
            connection.close()
            directory.cleanup()

    def test_timestamp_validation(self) -> None:
        directory, _path, connection = self._build_v8_db()
        try:
            for invalid_ts in (
                None,
                12345,
                "2026-09-20 12:00:00",
                "2026-09-20T12:00:00",
                "2026-09-20T12:00:00+01:00",
                "not-a-date",
            ):
                with self.assertRaises(ValueError):
                    migrate_v9(connection, invalid_ts)  # type: ignore[arg-type]
        finally:
            connection.close()
            directory.cleanup()

    def test_connection_type_and_transaction_guards(self) -> None:
        with self.assertRaisesRegex(TypeError, "connection must be sqlite3.Connection"):
            migrate_v9("not-a-connection", APPLIED_AT)  # type: ignore[arg-type]

        directory, _path, connection = self._build_v8_db()
        try:
            connection.execute("BEGIN")
            with self.assertRaisesRegex(ValueError, "no active transaction"):
                migrate_v9(connection, APPLIED_AT)
            connection.rollback()
        finally:
            connection.close()
            directory.cleanup()


if __name__ == "__main__":
    unittest.main()
