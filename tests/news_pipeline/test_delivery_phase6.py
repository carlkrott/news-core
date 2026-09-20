from __future__ import annotations

import json
import hashlib
import os
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from news_pipeline.db import init_db
from news_pipeline.delivery import (
    DeliveryAmbiguous,
    DeliveryConflict,
    DeliveryRejected,
    deliver_report,
    split_telegram_text,
)
from news_pipeline.delivery_schema_v6 import migrate_v6, validate_v6
from news_pipeline.report_builder import run_report
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5

MIGRATED_AT = "2026-09-07T00:00:00Z"
EVENT_AT = "2026-09-07T07:00:00Z"
AS_OF = datetime(2026, 9, 8, 8, 0, tzinfo=UTC)


class FakeTransport:
    def __init__(
        self,
        *,
        failure: Exception | None = None,
        recipient_seed: bytes = b"phase6-test-recipient",
    ) -> None:
        self.messages: list[str] = []
        self.failure = failure
        self._recipient_hash = hashlib.sha256(recipient_seed).hexdigest()

    @property
    def recipient_hash(self) -> str:
        return self._recipient_hash

    def send(self, text: str) -> str:
        if self.failure is not None:
            raise self.failure
        self.messages.append(text)
        return f"telegram-{len(self.messages)}"


def _make_v5(path: Path) -> None:
    init_db(str(path))
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        migrate_v3(connection, MIGRATED_AT)
        migrate_v4(connection, MIGRATED_AT)
        migrate_v5(connection, MIGRATED_AT)
    finally:
        connection.close()


def _insert_verified_event(path: Path) -> None:
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute(
            "INSERT INTO runs(id,started_at,finished_at,kind,provenance) VALUES(?,?,?,?,?)",
            ("run-event-1", EVENT_AT, EVENT_AT, "historical_replay", "manual"),
        )
        connection.execute(
            """INSERT INTO events(
                   id,run_id,category,started_at,ended_at,article_count,observation_count,status)
               VALUES(?,?,?,?,?,?,?,?)""",
            ("event-1", "run-event-1", "ai", EVENT_AT, EVENT_AT, 0, 0, "complete"),
        )
        connection.execute(
            """INSERT INTO event_versions(
                   event_id,version,material_change_reason,summary,verification_state,
                   valid_from,superseded_at,verified_at)
               VALUES(?,?,?,?,?,?,?,?)""",
            ("event-1", 1, "initial", "A verified event", "verified", EVENT_AT, None, EVENT_AT),
        )
    finally:
        connection.close()


def _prepare(tmp_path: Path) -> tuple[Path, Path, str]:
    db = tmp_path / "state.db"
    root = tmp_path / "artifacts"
    root.mkdir()
    _make_v5(db)
    _insert_verified_event(db)
    result = run_report(db, root, AS_OF)
    assert result.generation_status == "complete"
    connection = sqlite3.connect(db, isolation_level=None)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        assert migrate_v6(connection, "2026-09-08T08:01:00Z") is True
        assert migrate_v6(connection, "2026-09-08T08:01:00Z") is False
        validate_v6(connection)
    finally:
        connection.close()
    return db, root, result.report_id


def test_splitter_reconstructs_exact_text() -> None:
    text = ("header\n" + "x" * 4090 + "\n") * 3
    parts = split_telegram_text(text)
    assert parts
    assert all(0 < len(part) <= 4096 for part in parts)
    assert "".join(parts) == text


def test_v6_dry_run_is_durable_and_replay_safe(tmp_path: Path) -> None:
    db, root, report_id = _prepare(tmp_path)
    first = deliver_report(db, root, report_id, now="2026-09-08T08:02:00Z")
    second = deliver_report(db, root, report_id, now="2026-09-08T08:03:00Z")
    assert first.state == second.state == "dry_run"
    assert first.network_used is False
    assert second.replayed is True
    assert first.attempt_id == second.attempt_id
    connection = sqlite3.connect(db)
    try:
        assert connection.execute("SELECT delivery_state FROM reports").fetchone()[0] == "dry_run"
        assert connection.execute("SELECT COUNT(*) FROM report_delivery_attempts").fetchone()[0] == 1
    finally:
        connection.close()


def test_v6_requires_report_tables_before_mutation(tmp_path: Path) -> None:
    db = tmp_path / "state.db"
    _make_v5(db)
    connection = sqlite3.connect(db, isolation_level=None)
    try:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("DROP TABLE report_events")
        connection.execute("DROP TABLE reports")
        connection.execute("PRAGMA foreign_keys=ON")
        with pytest.raises(ValueError, match="report-capable v5 tables"):
            migrate_v6(connection, "2026-09-08T08:01:00Z")
        assert connection.execute("SELECT COUNT(*) FROM schema_migrations WHERE version=6").fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('report_deliveries','report_delivery_attempts')"
        ).fetchone()[0] == 0
    finally:
        connection.close()


def test_v6_fk_precondition_is_write_free(tmp_path: Path) -> None:
    db = tmp_path / "state.db"
    _make_v5(db)
    connection = sqlite3.connect(db, isolation_level=None)
    try:
        with pytest.raises(ValueError, match="foreign-key enforcement"):
            migrate_v6(connection, "2026-09-08T08:01:00Z")
        assert connection.execute("SELECT COUNT(*) FROM schema_migrations WHERE version=6").fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('report_deliveries','report_delivery_attempts')"
        ).fetchone()[0] == 0
    finally:
        connection.close()


def test_ingest_schema_gate_accepts_v5_v6_and_rejects_unknown(tmp_path: Path) -> None:
    from news_pipeline.ingest_runner import _verify_schema

    db = tmp_path / "state.db"
    _make_v5(db)
    _verify_schema(db)
    connection = sqlite3.connect(db, isolation_level=None)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        migrate_v6(connection, "2026-09-08T08:01:00Z")
    finally:
        connection.close()
    _verify_schema(db)
    connection = sqlite3.connect(db, isolation_level=None)
    try:
        connection.execute(
            "INSERT INTO schema_migrations(version,applied_at) VALUES(?,?)",
            (10, "2026-09-08T08:02:00Z"),
        )
    finally:
        connection.close()
    with pytest.raises(ValueError, match="known additive v4-v9 prefix"):
        _verify_schema(db)


def test_live_delivery_persists_receipt_and_replay_does_not_send(tmp_path: Path) -> None:
    db, root, report_id = _prepare(tmp_path)
    transport = FakeTransport()
    first = deliver_report(
        db, root, report_id, enable_live=True, transport=transport,
        now="2026-09-08T08:02:00Z",
    )
    replay_transport = FakeTransport()
    second = deliver_report(
        db, root, report_id, enable_live=True, transport=replay_transport,
        now="2026-09-08T08:03:00Z",
    )
    assert first.state == "sent"
    assert first.message_ids == ("telegram-1",)
    assert transport.messages
    assert second.replayed is True
    assert second.network_used is False
    assert replay_transport.messages == []
    connection = sqlite3.connect(db)
    try:
        assert connection.execute("SELECT delivery_state FROM reports").fetchone()[0] == "sent"
        assert connection.execute("SELECT state FROM report_deliveries").fetchone()[0] == "sent"
    finally:
        connection.close()


def test_dry_run_can_transition_to_live_recipient(tmp_path: Path) -> None:
    db, root, report_id = _prepare(tmp_path)
    dry_run = deliver_report(db, root, report_id, now="2026-09-08T08:02:00Z")
    transport = FakeTransport()
    live = deliver_report(
        db, root, report_id, enable_live=True, transport=transport,
        now="2026-09-08T08:03:00Z",
    )
    assert dry_run.state == "dry_run"
    assert live.state == "sent"
    assert transport.messages


def test_live_replay_rejects_different_recipient(tmp_path: Path) -> None:
    db, root, report_id = _prepare(tmp_path)
    first = FakeTransport()
    deliver_report(
        db, root, report_id, enable_live=True, transport=first,
        now="2026-09-08T08:02:00Z",
    )
    second = FakeTransport(recipient_seed=b"different-recipient")
    with pytest.raises(DeliveryConflict, match="recipient hash"):
        deliver_report(
            db, root, report_id, enable_live=True, transport=second,
            now="2026-09-08T08:03:00Z",
        )
    assert second.messages == []


def test_ambiguous_send_blocks_automatic_retry(tmp_path: Path) -> None:
    db, root, report_id = _prepare(tmp_path)
    with pytest.raises(DeliveryAmbiguous):
        deliver_report(
            db, root, report_id, enable_live=True,
            transport=FakeTransport(failure=DeliveryAmbiguous("unknown")),
            now="2026-09-08T08:02:00Z",
        )
    retry = FakeTransport()
    with pytest.raises(DeliveryAmbiguous):
        deliver_report(db, root, report_id, enable_live=True, transport=retry)
    assert retry.messages == []
    connection = sqlite3.connect(db)
    try:
        assert connection.execute("SELECT state FROM report_deliveries").fetchone()[0] == "ambiguous"
        assert connection.execute("SELECT state FROM report_delivery_attempts").fetchone()[0] == "ambiguous"
    finally:
        connection.close()


def test_known_rejection_can_only_retry_explicitly(tmp_path: Path) -> None:
    db, root, report_id = _prepare(tmp_path)
    with pytest.raises(DeliveryRejected):
        deliver_report(
            db, root, report_id, enable_live=True,
            transport=FakeTransport(failure=DeliveryRejected("rejected")),
            now="2026-09-08T08:02:00Z",
        )
    with pytest.raises(DeliveryRejected):
        deliver_report(db, root, report_id, enable_live=True, transport=FakeTransport())
    result = deliver_report(
        db, root, report_id, enable_live=True, transport=FakeTransport(),
        retry_failed=True, now="2026-09-08T08:04:00Z",
    )
    assert result.state == "sent"


def test_disabled_wrappers_never_enable_network(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    env.update(
        NEWS_PIPELINE_CODE_ROOT=str(root),
        NEWS_PIPELINE_DB=str(tmp_path / "missing.db"),
    )
    for wrapper in ("news-tick", "news-process"):
        result = subprocess.run(
            [str(root / "bin" / wrapper)], capture_output=True, text=True, env=env, check=False
        )
        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["state"] == "disabled"
        assert payload["network_used"] is False


def test_wrappers_resolve_workspace_code_when_installed_in_local_bin(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    home = tmp_path / "home"
    local_bin = home / ".local" / "bin"
    workspace = home / ".zeroclaw" / "workspace"
    local_bin.mkdir(parents=True)
    workspace.mkdir(parents=True)
    (workspace / "scripts").symlink_to(root / "scripts", target_is_directory=True)

    wrappers = (
        "news-tick",
        "news-process",
        "news-daily-close",
        "news-daily-report",
        "news-health",
    )
    for wrapper in wrappers:
        source = root / "bin" / wrapper
        body = source.read_text(encoding="utf-8")
        assert "BASH_SOURCE" not in body
        assert 'NEWS_PIPELINE_CODE_ROOT:-/app' in body
        installed = local_bin / wrapper
        installed.write_bytes(source.read_bytes())
        installed.chmod(0o700)

    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"PYTHONPATH", "NEWS_PIPELINE_CODE_ROOT"}
    }
    env.update(
        HOME=str(home),
        NEWS_PIPELINE_CODE_ROOT=str(workspace),
        NEWS_PIPELINE_DB=str(tmp_path / "missing.db"),
    )
    for wrapper in ("news-tick", "news-process"):
        result = subprocess.run(
            [str(local_bin / wrapper)], capture_output=True, text=True, env=env, check=False
        )
        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["state"] == "disabled"
        assert payload["network_used"] is False


def test_daily_report_without_prior_replays_current_window(tmp_path: Path) -> None:
    db, artifacts, report_id = _prepare(tmp_path)
    root = Path(__file__).resolve().parents[2]
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {
            "PYTHONPATH",
            "NEWS_PHASE6_ENABLE_LIVE_DELIVERY",
            "NEWS_PHASE6_RETRY_FAILED",
            "NEWS_PIPELINE_PRIOR_UPPER_UTC",
        }
    }
    env.update(
        NEWS_PIPELINE_CODE_ROOT=str(root),
        NEWS_PIPELINE_DB=str(db),
        NEWS_PIPELINE_ARTIFACT_ROOT=str(artifacts),
        NEWS_PIPELINE_AS_OF_UTC="2026-09-08T08:00:00Z",
    )
    result = subprocess.run(
        [str(root / "bin" / "news-daily-report")],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["report_id"] == report_id
    assert payload["was_replayed"] is True
    assert payload["delivery_state"] == "not_attempted"
    assert payload["network_used"] is False
    connection = sqlite3.connect(db)
    try:
        assert connection.execute("SELECT COUNT(*) FROM reports").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM report_deliveries").fetchone()[0] == 0
    finally:
        connection.close()


def test_daily_report_without_prior_advances_from_latest_completed_window(
    tmp_path: Path,
) -> None:
    db, artifacts, first_report_id = _prepare(tmp_path)
    root = Path(__file__).resolve().parents[2]
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {
            "PYTHONPATH",
            "NEWS_PHASE6_ENABLE_LIVE_DELIVERY",
            "NEWS_PHASE6_RETRY_FAILED",
            "NEWS_PIPELINE_PRIOR_UPPER_UTC",
        }
    }
    env.update(
        NEWS_PIPELINE_CODE_ROOT=str(root),
        NEWS_PIPELINE_DB=str(db),
        NEWS_PIPELINE_ARTIFACT_ROOT=str(artifacts),
        NEWS_PIPELINE_AS_OF_UTC="2026-09-09T08:00:00Z",
    )
    result = subprocess.run(
        [str(root / "bin" / "news-daily-report")],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["report_id"] != first_report_id
    assert payload["was_replayed"] is False
    assert payload["delivery_state"] == "not_attempted"
    assert payload["network_used"] is False
    connection = sqlite3.connect(db)
    try:
        windows = connection.execute(
            "SELECT window_start,window_end FROM reports ORDER BY window_end"
        ).fetchall()
        assert windows == [
            ("2026-09-01T07:00:00Z", "2026-09-08T07:00:00Z"),
            ("2026-09-08T07:00:00Z", "2026-09-09T07:00:00Z"),
        ]
        assert connection.execute("SELECT COUNT(*) FROM report_deliveries").fetchone()[0] == 0
    finally:
        connection.close()


def test_failed_receipt_cannot_be_relabelled_as_dry_run(tmp_path: Path) -> None:
    db, root, report_id = _prepare(tmp_path)
    with pytest.raises(DeliveryRejected):
        deliver_report(
            db, root, report_id, enable_live=True,
            transport=FakeTransport(failure=DeliveryRejected("rejected")),
            now="2026-09-08T08:02:00Z",
        )
    with pytest.raises(DeliveryRejected, match="explicit live retry_failed"):
        deliver_report(db, root, report_id, retry_failed=True, now="2026-09-08T08:03:00Z")
