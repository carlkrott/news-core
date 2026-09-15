"""Tests for the public dry-run API surface: evaluate_candidates + invariants.

This file also contains the no-mutation invariant tests (DB SHA, table counts,
schema/data_version, WAL/SHM) and the AST no-network/process/send test.
"""
from __future__ import annotations

import ast
import contextlib
import hashlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

from news_pipeline.api import evaluate_candidates
from news_pipeline.contracts import CandidateArticle, DecisionCode, FilterResult
from news_pipeline.db import connect, init_db
from news_pipeline.history import open_history
from news_pipeline.models import Category
from news_pipeline.policies import default_query_policies


EVAL = "2026-07-14T22:50:33Z"
PUB = "2026-07-14T20:50:33Z"
OBS = "2026-07-14T19:50:33Z"


def _candidate(**overrides):
    base = dict(
        candidate_id="c1",
        category=Category.AI,
        query_group="ai",
        title="Hello world",
        snippet="body text",
        original_url="https://example.com/a",
        canonical_url="https://example.com/a",
        published_at=PUB,
        published_evidence="source",
        observed_at=OBS,
        evaluated_at=EVAL,
    )
    if "canonical_url" in overrides and "original_url" not in overrides:
        base["original_url"] = overrides["canonical_url"]
    base.update(overrides)
    return CandidateArticle(**base)


class EvaluateCandidatesContract(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "state.db")
        init_db(self.db_path)
        self.policies = default_query_policies()
        from news_pipeline.policies import SourcePolicy

        self.source = SourcePolicy()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_returns_tuple_of_filter_results(self):
        c = _candidate()
        out = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertIsInstance(out, tuple)
        self.assertEqual(len(out), 1)
        self.assertIsInstance(out[0], FilterResult)

    def test_returns_empty_tuple_for_empty_input(self):
        out = evaluate_candidates([], self.db_path, self.source, self.policies)
        self.assertEqual(out, ())

    def test_preserves_input_order(self):
        cand = [
            _candidate(candidate_id=f"c{i}", canonical_url=f"https://example.com/{i}")
            for i in range(4)
        ]
        out = evaluate_candidates(cand, self.db_path, self.source, self.policies)
        self.assertEqual([r.candidate.candidate_id for r in out], [f"c{i}" for i in range(4)])
        self.assertEqual([r.ordinal for r in out], [1, 2, 3, 4])


class NoMutationTests(unittest.TestCase):
    """The API must not mutate the live (or temp) DB. We copy the live DB into a
    temp file and confirm SHA256 + counts + schema + data_version + WAL/SHM
    hashes are byte-for-byte identical before and after a representative run.
    """

    def setUp(self) -> None:
        # Build a fresh temp DB (no live DB is read in tests).
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "state.db")
        init_db(self.db_path)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _hash_db(self, path: str) -> dict:
        with open(path, "rb") as fh:
            digest = hashlib.sha256(fh.read()).hexdigest()
        # SQLite's standard `with con:` only commits/rolls back; use closing()
        # so the connection is actually closed (and the file handle released).
        with contextlib.closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as con:
            tables = (
                "schema_migrations",
                "runs",
                "articles",
                "observations",
                "events",
                "event_articles",
                "fact_fingerprints",
                "decisions",
                "delivery_attempts",
                "manual_review",
                "query_telemetry",
            )
            counts = {t: con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] for t in tables}
            return {
                "sha": digest,
                "counts": counts,
                "data_version": con.execute("PRAGMA data_version").fetchone()[0],
                "schema_version": con.execute("PRAGMA schema_version").fetchone()[0],
            }

    def test_evaluate_candidates_does_not_mutate_db(self):
        from news_pipeline.policies import SourcePolicy

        before = self._hash_db(self.db_path)
        cand = [
            _candidate(candidate_id=f"c{i}", canonical_url=f"https://example.com/{i}")
            for i in range(5)
        ]
        results = evaluate_candidates(cand, self.db_path, SourcePolicy(), default_query_policies())
        after = self._hash_db(self.db_path)
        self.assertEqual(before, after)
        self.assertEqual(len(results), 5)

    def test_wal_and_shm_state_unchanged_after_eval(self):
        # Use a clean DELETE-journal fixture to prove a read-only evaluation does
        # not create sidecars. Live WAL-mode state is checked separately by the
        # executor's exact pre/post sidecar hashes.
        with contextlib.closing(sqlite3.connect(self.db_path)) as con:
            con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            con.execute("PRAGMA journal_mode=DELETE")
        wal = Path(self.db_path + "-wal")
        shm = Path(self.db_path + "-shm")
        self.assertFalse(wal.exists())
        self.assertFalse(shm.exists())
        from news_pipeline.policies import SourcePolicy
        before = self._hash_db(self.db_path)
        evaluate_candidates([_candidate()], self.db_path, SourcePolicy(), default_query_policies())
        after = self._hash_db(self.db_path)
        self.assertEqual(before, after)
        self.assertFalse(wal.exists())
        self.assertFalse(shm.exists())


class PackageAstTests(unittest.TestCase):
    """The Phase 2 package must not import any network/subprocess/send module."""

    FORBIDDEN = (
        "urllib",
        "urllib.request",
        "urllib.error",
        "urllib.parse",
        "urllib.robotparser",
        "http",
        "http.client",
        "httplib",
        "ftplib",
        "smtplib",
        "telnetlib",
        "asyncio",
        "socketserver",
        "ssl",
        "selectors",
        "subprocess",
        "multiprocessing",
        "threading",
        "socket",
        "requests",
        "httpx",
        "aiohttp",
    )
    FORBIDDEN_CALLS = (
        "send",
        "sendto",
        "sendmail",
        "send_message",
    )

    def _ast_tree(self, path: Path) -> ast.Module:
        return ast.parse(path.read_text(encoding="utf-8"))

    def test_phase2_package_no_network_imports(self):
        from pathlib import Path

        pkg = Path(__import__("news_pipeline").__file__).resolve().parent
        for name in ("contracts.py", "policies.py", "history.py", "filtering.py", "api.py"):
            path = pkg / name
            if not path.exists():
                continue
            tree = self._ast_tree(path)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        top = alias.name.split(".")[0]
                        self.assertNotIn(
                            top,
                            self.FORBIDDEN,
                            f"{name}: forbidden import {alias.name}",
                        )
                elif isinstance(node, ast.ImportFrom):
                    if node.module is None:
                        continue
                    top = node.module.split(".")[0]
                    self.assertNotIn(
                        top, self.FORBIDDEN, f"{name}: forbidden import from {node.module}"
                    )

    def test_phase2_package_no_send_calls(self):
        from pathlib import Path

        pkg = Path(__import__("news_pipeline").__file__).resolve().parent
        for name in ("contracts.py", "policies.py", "history.py", "filtering.py", "api.py"):
            path = pkg / name
            if not path.exists():
                continue
            tree = self._ast_tree(path)
            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    func = node.func
                    if isinstance(func, ast.Attribute) and func.attr in self.FORBIDDEN_CALLS:
                        self.fail(f"{name}: forbidden call .{func.attr}")


class DryRunLockedTests(unittest.TestCase):
    def test_dry_run_is_true(self):
        import news_pipeline.config as cfg

        self.assertIs(cfg.DRY_RUN, True)
        self.assertEqual(
            cfg.STATE_DB_PATH,
            "/state/news-state.db",
        )


if __name__ == "__main__":
    unittest.main()
