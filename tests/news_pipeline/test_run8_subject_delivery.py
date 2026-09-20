"""Focused Run 8 subject-scoped delivery outbox tests."""
from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

from news_pipeline.db import init_db
from news_pipeline.delivery import DeliveryAmbiguous, DeliveryConflict, DeliveryRejected
from news_pipeline.delivery_schema_v6 import migrate_v6
from news_pipeline.models import Subject
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5
from news_pipeline.schema_v7 import migrate_v7
from news_pipeline.schema_v8 import migrate_v8
from news_pipeline.schema_v9 import migrate_v9
from news_pipeline.subject_delivery import (
    complete_subject_delivery,
    load_subject_delivery,
    prepare_subject_report,
    start_subject_delivery,
    subject_idempotency_key,
    subject_report_id,
)

T0 = "2026-09-20T12:00:00Z"
T1 = "2026-09-20T12:01:00Z"
T2 = "2026-09-20T12:02:00Z"
T3 = "2026-09-20T12:03:00Z"
T4 = "2026-09-20T12:04:00Z"
PARENT = "report-parent-001"
RECIPIENT = "a" * 64
OTHER_RECIPIENT = "b" * 64


class SubjectDeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.db_path = Path(self._directory.name) / "state.db"
        init_db(str(self.db_path))
        self.connection = sqlite3.connect(self.db_path, isolation_level=None)
        self.connection.execute("PRAGMA foreign_keys=ON")
        migrate_v3(self.connection, T0)
        migrate_v4(self.connection, T0)
        migrate_v5(self.connection, T0)
        migrate_v6(self.connection, T0)
        migrate_v7(self.connection, T0)
        migrate_v8(self.connection, T0)
        migrate_v9(self.connection, T0)
        self.connection.execute(
            """INSERT INTO reports(
                   report_id,window_start,window_end,generation_status,
                   json_sha256,jsonl_sha256,markdown_sha256,manifest_sha256,
                   delivery_state,delivery_id,created_at)
               VALUES(?,?,?,'complete',?,?,?,?, 'not_attempted',NULL,?)""",
            (
                PARENT,
                "2026-09-19T07:00:00Z",
                "2026-09-20T07:00:00Z",
                "1" * 64,
                "2" * 64,
                "3" * 64,
                "4" * 64,
                T0,
            ),
        )

    def tearDown(self) -> None:
        self.connection.close()
        self._directory.cleanup()

    def _prepare(
        self,
        *,
        subject: Subject = Subject.AI,
        text: str = "AI report\n",
        story_count: int = 1,
        created_at: str = T1,
    ):
        return prepare_subject_report(
            self.connection,
            parent_report_id=PARENT,
            subject=subject,
            rendered_text=text,
            story_count=story_count,
            created_at=created_at,
        )

    def _start(
        self,
        subject_report_id_value: str,
        *,
        recipient_hash: str = RECIPIENT,
        channel: str = "telegram",
        prepared_at: str = T2,
        retry_failed: bool = False,
    ):
        return start_subject_delivery(
            self.connection,
            subject_report_id=subject_report_id_value,
            recipient_hash=recipient_hash,
            channel=channel,
            prepared_at=prepared_at,
            retry_failed=retry_failed,
        )

    def test_identities_are_deterministic_and_subject_scoped(self) -> None:
        content_hash = hashlib.sha256(b"same report").hexdigest()
        ai_id = subject_report_id(PARENT, Subject.AI, content_hash)
        self.assertEqual(ai_id, subject_report_id(PARENT, "ai", content_hash))
        world_id = subject_report_id(PARENT, Subject.WORLD, content_hash)
        self.assertNotEqual(ai_id, world_id)
        ai_key = subject_idempotency_key(Subject.AI, ai_id, content_hash)
        self.assertEqual(ai_key, subject_idempotency_key("ai", ai_id, content_hash))
        self.assertNotEqual(
            ai_key,
            subject_idempotency_key(Subject.WORLD, world_id, content_hash),
        )

    def test_prepare_persists_exact_rows_and_same_content_replays(self) -> None:
        first = self._prepare()
        second = self._prepare(created_at=T2)
        self.assertEqual(first, second)
        self.assertEqual(first.report.parent_report_id, PARENT)
        self.assertEqual(first.report.subject_id, "ai")
        self.assertEqual(first.report.revision, 1)
        self.assertEqual(first.report.story_count, 1)
        self.assertEqual(first.outbox.state, "prepared")
        self.assertIsNone(first.outbox.channel)
        self.assertIsNone(first.outbox.recipient_hash)
        self.assertIsNone(first.outbox.current_attempt_id)
        self.assertIsNone(first.attempt)
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM subject_reports").fetchone()[0],
            1,
        )
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM subject_delivery_outbox").fetchone()[0],
            1,
        )

    def test_changed_content_creates_revision_without_mutating_sent(self) -> None:
        first = self._prepare(text="First report\n")
        started = self._start(first.report.subject_report_id)
        sent = complete_subject_delivery(
            self.connection,
            subject_report_id=first.report.subject_report_id,
            attempt_id=started.attempt.attempt_id,
            state="sent",
            completed_at=T3,
            message_ids=("message-1",),
        )
        second = self._prepare(text="Corrected report\n", created_at=T4)
        self.assertEqual(sent.outbox.state, "sent")
        self.assertEqual(second.report.revision, 2)
        self.assertNotEqual(second.report.subject_report_id, first.report.subject_report_id)
        self.assertEqual(second.outbox.state, "prepared")
        persisted_first = load_subject_delivery(
            self.connection, first.report.subject_report_id
        )
        self.assertEqual(persisted_first.outbox.state, "sent")
        self.assertEqual(persisted_first.attempt.message_ids, ("message-1",))

    def test_zero_story_is_skipped_without_attempt(self) -> None:
        skipped = self._prepare(
            subject=Subject.WORLD, text="", story_count=0
        )
        self.assertEqual(skipped.outbox.state, "skipped")
        self.assertIsNone(skipped.outbox.current_attempt_id)
        self.assertIsNone(skipped.attempt)
        replay = self._start(skipped.report.subject_report_id)
        self.assertEqual(replay, skipped)
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM subject_delivery_attempts"
            ).fetchone()[0],
            0,
        )

    def test_start_persists_prepared_attempt_before_external_send(self) -> None:
        prepared = self._prepare()
        started = self._start(prepared.report.subject_report_id)
        self.assertEqual(started.outbox.state, "prepared")
        self.assertEqual(started.outbox.channel, "telegram")
        self.assertEqual(started.outbox.recipient_hash, RECIPIENT)
        self.assertEqual(started.attempt.state, "prepared")
        self.assertEqual(started.attempt.ordinal, 1)
        self.assertEqual(
            started.outbox.current_attempt_id, started.attempt.attempt_id
        )
        readback = load_subject_delivery(
            self.connection, prepared.report.subject_report_id
        )
        self.assertEqual(readback, started)
        with self.assertRaises(DeliveryAmbiguous):
            self._start(prepared.report.subject_report_id, prepared_at=T3)

    def test_sent_replay_creates_no_new_attempt(self) -> None:
        prepared = self._prepare()
        started = self._start(prepared.report.subject_report_id)
        sent = complete_subject_delivery(
            self.connection,
            subject_report_id=prepared.report.subject_report_id,
            attempt_id=started.attempt.attempt_id,
            state="sent",
            completed_at=T3,
            message_ids=("message-1",),
        )
        replay = self._start(prepared.report.subject_report_id, prepared_at=T4)
        self.assertEqual(replay, sent)
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM subject_delivery_attempts"
            ).fetchone()[0],
            1,
        )

    def test_failed_retry_requires_explicit_flag_and_creates_ordinal_two(self) -> None:
        prepared = self._prepare()
        first = self._start(prepared.report.subject_report_id)
        failed = complete_subject_delivery(
            self.connection,
            subject_report_id=prepared.report.subject_report_id,
            attempt_id=first.attempt.attempt_id,
            state="failed",
            completed_at=T3,
            error_code="rejected",
            error_detail="provider rejected message",
        )
        self.assertEqual(failed.outbox.state, "failed")
        with self.assertRaises(DeliveryRejected):
            self._start(prepared.report.subject_report_id, prepared_at=T4)
        retry = self._start(
            prepared.report.subject_report_id,
            prepared_at=T4,
            retry_failed=True,
        )
        self.assertEqual(retry.attempt.ordinal, 2)
        self.assertNotEqual(retry.attempt.attempt_id, first.attempt.attempt_id)
        self.assertEqual(
            self.connection.execute(
                "SELECT state FROM subject_delivery_attempts "
                "WHERE attempt_id=?",
                (first.attempt.attempt_id,),
            ).fetchone(),
            ("failed",),
        )

    def test_ambiguous_outcome_blocks_automatic_retry(self) -> None:
        prepared = self._prepare()
        started = self._start(prepared.report.subject_report_id)
        ambiguous = complete_subject_delivery(
            self.connection,
            subject_report_id=prepared.report.subject_report_id,
            attempt_id=started.attempt.attempt_id,
            state="ambiguous",
            completed_at=T3,
            error_code="timeout",
            error_detail="provider outcome unknown",
        )
        self.assertEqual(ambiguous.outbox.state, "ambiguous")
        with self.assertRaises(DeliveryAmbiguous):
            self._start(
                prepared.report.subject_report_id,
                prepared_at=T4,
                retry_failed=True,
            )

    def test_recipient_and_channel_binding_cannot_change_on_retry(self) -> None:
        prepared = self._prepare()
        first = self._start(prepared.report.subject_report_id)
        complete_subject_delivery(
            self.connection,
            subject_report_id=prepared.report.subject_report_id,
            attempt_id=first.attempt.attempt_id,
            state="failed",
            completed_at=T3,
            error_code="rejected",
            error_detail="provider rejected message",
        )
        with self.assertRaises(DeliveryConflict):
            self._start(
                prepared.report.subject_report_id,
                recipient_hash=OTHER_RECIPIENT,
                prepared_at=T4,
                retry_failed=True,
            )
        with self.assertRaises(DeliveryConflict):
            self._start(
                prepared.report.subject_report_id,
                channel="email",
                prepared_at=T4,
                retry_failed=True,
            )

    def test_tampered_idempotency_key_fails_closed(self) -> None:
        prepared = self._prepare()
        self.connection.execute(
            "UPDATE subject_delivery_outbox SET idempotency_key=? "
            "WHERE subject_report_id=?",
            ("f" * 64, prepared.report.subject_report_id),
        )
        with self.assertRaises(DeliveryConflict):
            load_subject_delivery(
                self.connection, prepared.report.subject_report_id
            )

    def test_invalid_inputs_fail_before_mutation(self) -> None:
        invalid = (
            {"story_count": True},
            {"story_count": 0, "text": "not empty"},
            {"story_count": 1, "text": ""},
            {"created_at": "2026-09-20T12:00:00+00:00"},
            {"subject": "not-a-subject"},
        )
        for overrides in invalid:
            kwargs = {
                "subject": Subject.AI,
                "text": "AI report\n",
                "story_count": 1,
                "created_at": T1,
            }
            kwargs.update(overrides)
            with self.subTest(overrides=overrides), self.assertRaises(
                (TypeError, ValueError)
            ):
                self._prepare(**kwargs)
        with self.assertRaises(DeliveryRejected):
            prepare_subject_report(
                self.connection,
                parent_report_id="missing-parent",
                subject=Subject.AI,
                rendered_text="AI report\n",
                story_count=1,
                created_at=T1,
            )
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM subject_reports").fetchone()[0],
            0,
        )


if __name__ == "__main__":
    unittest.main()
