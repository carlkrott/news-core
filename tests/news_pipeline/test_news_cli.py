"""Phase 5 — test_news_cli.py

Focused tests for ``news_pipeline.news_cli`` covering:
- required --db, --artifact-root, --as-of-utc arguments
- UTC timestamp validation (Z suffix, timezone-aware)
- invalid timestamp errors go to stderr
- no implicit clock (as-of-utc must be explicit)
- no network access proof (static analysis)
- no delivery option / no sender
- stdout JSON contract
"""
from __future__ import annotations

import json
import ast
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from news_pipeline.db import init_db
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5


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


def _persist_db(conn: sqlite3.Connection, db_path: str) -> None:
    destination = sqlite3.connect(db_path)
    try:
        conn.backup(destination)
    finally:
        destination.close()
        conn.close()


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


def _cli(args, stdin_data=None):
    """Run news_cli.py as a subprocess, return (returncode, stdout, stderr)."""
    project_root = Path(__file__).resolve().parents[2]
    env = {**os.environ, "PYTHONPATH": str(project_root / "scripts")}
    result = subprocess.run(
        [sys.executable, "-m", "news_pipeline.news_cli"] + args,
        input=stdin_data,
        capture_output=True,
        text=True,
        env=env,
    )
    return result.returncode, result.stdout, result.stderr


# -----------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------


class TestCLIMissingArgs(unittest.TestCase):
    """Missing required arguments produce a non-zero exit and a stderr message."""

    def test_missing_all_args(self) -> None:
        code, stdout, stderr = _cli([])
        self.assertNotEqual(code, 0)
        self.assertIn("--db", stderr)

    def test_missing_artifact_root(self) -> None:
        with tempfile.TemporaryFile(suffix=".db") as f:
            pass
        code, stdout, stderr = _cli(["--db", "/nonexistent/path.db"])
        self.assertNotEqual(code, 0)
        self.assertIn("--artifact-root", stderr)

    def test_missing_as_of_utc(self) -> None:
        code, stdout, stderr = _cli(
            ["--db", "/nonexistent/path.db", "--artifact-root", "/tmp"]
        )
        self.assertNotEqual(code, 0)
        self.assertIn("--as-of-utc", stderr)


class TestCLIInvalidTimestamp(unittest.TestCase):
    """Invalid UTC timestamps are rejected with a stderr error."""

    def test_missing_z_suffix(self) -> None:
        code, stdout, stderr = _cli(
            [
                "--db", "/nonexistent/path.db",
                "--artifact-root", "/tmp",
                "--as-of-utc", "2026-09-08T09:00:00",
            ]
        )
        self.assertNotEqual(code, 0)
        self.assertIn("Z", stderr)

    def test_local_time_rejected(self) -> None:
        code, stdout, stderr = _cli(
            [
                "--db", "/nonexistent/path.db",
                "--artifact-root", "/tmp",
                "--as-of-utc", "2026-09-08T09:00:00+05:00",
            ]
        )
        self.assertNotEqual(code, 0)
        self.assertIn("Z", stderr)

    def test_invalid_iso_format(self) -> None:
        code, stdout, stderr = _cli(
            [
                "--db", "/nonexistent/path.db",
                "--artifact-root", "/tmp",
                "--as-of-utc", "not-a-timestamp",
            ]
        )
        self.assertNotEqual(code, 0)


class TestCLIValidRun(unittest.TestCase):
    """A valid run with an empty DB returns exit 0 and a stdout line."""

    def test_valid_empty_run_exit_zero(self) -> None:
        conn = _make_db()
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                code, stdout, stderr = _cli(
                    [
                        "--db", db_path,
                        "--artifact-root", artifact_root,
                        "--as-of-utc", "2026-09-08T09:00:00Z",
                    ]
                )
                self.assertEqual(code, 0, stderr)
                self.assertEqual(json.loads(stdout)["generation_status"], "complete")
        finally:
            os.unlink(db_path)

    def test_json_output_is_valid(self) -> None:
        conn = _make_db()
        _insert_event(
            conn, "ev-cli-json", 1, "CLI JSON test", "verified", "2026-09-08T06:00:00Z"
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _persist_db(conn, db_path)

            with tempfile.TemporaryDirectory() as artifact_root:
                code, stdout, stderr = _cli(
                    [
                        "--db", db_path,
                        "--artifact-root", artifact_root,
                        "--as-of-utc", "2026-09-08T09:00:00Z",
                    ]
                )
                self.assertEqual(code, 0, stderr)
                parsed = json.loads(stdout)
                self.assertIn("report_id", parsed)
                self.assertIn("generation_status", parsed)
                self.assertEqual(parsed["generation_status"], "complete")
        finally:
            os.unlink(db_path)


class TestCLIPriorUpperBound(unittest.TestCase):
    """Optional --prior-upper-utc is accepted and validated against --as-of-utc."""

    def test_prior_upper_must_be_before_as_of(self) -> None:
        code, stdout, stderr = _cli(
            [
                "--db", "/nonexistent/path.db",
                "--artifact-root", "/tmp",
                "--as-of-utc", "2026-09-08T09:00:00Z",
                "--prior-upper-utc", "2026-09-08T10:00:00Z",
            ]
        )
        self.assertNotEqual(code, 0)
        self.assertIn("strictly before", stderr)

    def test_prior_upper_invalid_timestamp(self) -> None:
        code, stdout, stderr = _cli(
            [
                "--db", "/nonexistent/path.db",
                "--artifact-root", "/tmp",
                "--as-of-utc", "2026-09-08T09:00:00Z",
                "--prior-upper-utc", "not-valid",
            ]
        )
        self.assertNotEqual(code, 0)
        self.assertIn("--prior-upper-utc", stderr)


class TestCLINoNetworkNoDelivery(unittest.TestCase):
    """Static proof: news_cli.py makes no network calls and has no delivery option."""

    def test_no_socket_imports(self) -> None:
        project_root = Path(__file__).resolve().parents[2]
        script = project_root / "scripts" / "news_pipeline" / "news_cli.py"
        tree = ast.parse(script.read_text(), filename=str(script))
        forbidden_import_roots = {
            "aiohttp",
            "http",
            "requests",
            "smtplib",
            "socket",
            "subprocess",
            "urllib",
        }
        imported_roots = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".", 1)[0])
        self.assertTrue(
            forbidden_import_roots.isdisjoint(imported_roots),
            f"news_cli.py imports forbidden modules: {forbidden_import_roots & imported_roots}",
        )

    def test_no_network_calls_in_source(self) -> None:
        project_root = Path(__file__).resolve().parents[2]
        script = project_root / "scripts" / "news_pipeline" / "news_cli.py"
        tree = ast.parse(script.read_text(), filename=str(script))
        forbidden_call_names = {
            "create_connection",
            "getaddrinfo",
            "sendmail",
            "urlopen",
        }
        forbidden_option_names = {
            "--delivery",
            "--send",
            "--webhook",
        }
        called_names = set()
        option_names = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    called_names.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    called_names.add(node.func.attr)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                if node.value.startswith("--"):
                    option_names.add(node.value)
        self.assertTrue(
            forbidden_call_names.isdisjoint(called_names),
            f"news_cli.py calls forbidden functions: {forbidden_call_names & called_names}",
        )
        self.assertTrue(
            forbidden_option_names.isdisjoint(option_names),
            f"news_cli.py exposes forbidden options: {forbidden_option_names & option_names}",
        )


class TestCLINoImplicitClock(unittest.TestCase):
    """The CLI rejects bare --as-of-utc without an explicit timestamp."""

    def test_as_of_utc_must_be_explicit(self) -> None:
        # --as-of-utc with no value should fail.
        code, stdout, stderr = _cli(
            ["--db", "/nonexistent/path.db", "--artifact-root", "/tmp", "--as-of-utc"]
        )
        self.assertNotEqual(code, 0)


if __name__ == "__main__":
    unittest.main()
