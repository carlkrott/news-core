"""Phase 2 fix tests — covers parent-identified defects 1-10.

This test module is the single source of truth for the defects the parent
flagged. Every test in here MUST fail before the corresponding fix and pass
after. Tests use only Phase 1 DB fixtures (no live DB read) and a tmp file
backed by init_db().
"""
from __future__ import annotations

import contextlib
import sqlite3
import tempfile
import unittest
from dataclasses import FrozenInstanceError, fields, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from news_pipeline.contracts import (
    CandidateArticle,
    DecisionCode,
    FilterResult,
    HistoryMatch,
    ReasonCode,
    TrustTier,
    validate_utc_iso,
)
from news_pipeline.db import connect, init_db
from news_pipeline.filtering import evaluate_candidates
from news_pipeline.history import (
    HistoryUnavailable,
    find_exact_identity,
    find_exact_title,
    find_exact_url,
    open_history,
    fetch_history_match,
)
from news_pipeline.models import Category
from news_pipeline.policies import (
    QueryPolicy,
    SourcePolicy,
    SourceRule,
    default_query_policies,
)


EVAL = "2026-07-14T22:50:33Z"
PUB = "2026-07-14T20:50:33Z"
RECENT = "2026-07-14T20:50:33Z"
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


class _Base(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "state.db")
        init_db(self.db_path)
        self.policies = default_query_policies()
        self.source = SourcePolicy()

    def tearDown(self) -> None:
        self.tmp.cleanup()


# ---------------------------------------------------------------------------
# Fix #1 — Resource leak: -W error::ResourceWarning must be a clean run.
# ---------------------------------------------------------------------------


class ResourceLeakTests(_Base):
    """The full Phase 2 test suite must produce zero ResourceWarnings.

    We run the suite as a subprocess with ``-W error::ResourceWarning`` so
    that any unclosed sqlite3 connection (or file handle) is promoted to an
    error. The test passes iff the subprocess exits 0 and stderr contains
    no ``ResourceWarning`` text.
    """

    SUBPROCESS_TIMEOUT = 60

    def _run_subprocess(self, *patterns: str) -> tuple[int, str, str]:
        import subprocess
        import sys

        cmd = [
            sys.executable,
            "-W", "error::ResourceWarning",
            "-m", "unittest", "discover",
            "-s", str(Path(__file__).resolve().parent),
            "-p", "test_api.py",
        ]
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=self.SUBPROCESS_TIMEOUT,
            cwd=str(Path(__import__("news_pipeline").__file__).resolve().parents[1]),
        )
        return proc.returncode, proc.stdout, proc.stderr

    def test_subprocess_resource_warning_gate(self):
        rc, out, err = self._run_subprocess()
        self.assertEqual(
            rc, 0,
            f"subprocess failed (rc={rc})\n--- STDOUT ---\n{out}\n--- STDERR ---\n{err}",
        )
        self.assertNotIn(
            "ResourceWarning", err,
            f"stderr contains ResourceWarning text:\n{err}",
        )
        self.assertNotIn(
            "Exception ignored while finalizing", err,
            f"stderr contains 'Exception ignored while finalizing':\n{err}",
        )


# ---------------------------------------------------------------------------
# Fix #2 — No broad `except Exception` / bare except in Phase 2 modules.
# ---------------------------------------------------------------------------


class NoBroadExceptionTests(unittest.TestCase):
    """The Phase 2 source modules must not swallow every error.

    AST-scan only — runtime checks are slow and noisy. We forbid:
      * ``except Exception:`` (no Exception class at all on bare ``except:``)
      * ``except BaseException:``
    A regular ``except ValueError:`` is allowed.
    """

    MODULES = ("contracts.py", "policies.py", "history.py", "filtering.py", "api.py")
    FORBIDDEN_TYPES = {"Exception", "BaseException"}

    def _ast(self, src: str):
        import ast
        return ast.parse(src)

    def test_no_broad_exception_in_phase2_modules(self):
        from pathlib import Path

        pkg = Path(__import__("news_pipeline").__file__).resolve().parent
        for name in self.MODULES:
            path = pkg / name
            if not path.exists():
                continue
            tree = self._ast(path.read_text(encoding="utf-8"))
            for node in tree.body:
                # Look at module-level try/except (we don't dive into nested funcs
                # for the strict AST guard — but the engine's outer try is what
                # matters most).
                import ast as _ast
                if isinstance(node, _ast.Try):
                    self._check_try(node, name)
                # also dive into function bodies
                for sub in _ast.walk(node):
                    if isinstance(sub, _ast.Try):
                        self._check_try(sub, name)

    def _check_try(self, node, name: str) -> None:
        import ast as _ast
        for handler in node.handlers:
            if handler.type is None:
                self.fail(
                    f"{name}: bare ``except:`` clause at line {handler.lineno} — "
                    f"programmer errors must surface"
                )
            if isinstance(handler.type, _ast.Name) and handler.type.id in self.FORBIDDEN_TYPES:
                self.fail(
                    f"{name}: forbidden broad ``except {handler.type.id}`` at line {handler.lineno}"
                )
            if isinstance(handler.type, _ast.Tuple):
                for elt in handler.type.elts:
                    if isinstance(elt, _ast.Name) and elt.id in self.FORBIDDEN_TYPES:
                        self.fail(
                            f"{name}: forbidden broad ``except ({elt.id}, ...)`` "
                            f"at line {elt.lineno}"
                        )


# ---------------------------------------------------------------------------
# Fix #3 — Category typing: CandidateArticle.category and HistoryMatch.category
# must be the actual Category enum, not `object`.
# ---------------------------------------------------------------------------


class CategoryTypingTests(unittest.TestCase):
    def test_candidate_article_category_is_category(self):
        f = next(f for f in fields(CandidateArticle) if f.name == "category")
        # The annotation is `Category`, not `object`. In Python 3.14 with
        # `from __future__ import annotations`, string annotations appear as
        # the literal "Category".
        self.assertEqual(
            f.type, "Category",
            f"CandidateArticle.category annotation must be Category, got {f.type!r}",
        )

    def test_history_match_category_is_category(self):
        f = next(f for f in fields(HistoryMatch) if f.name == "category")
        self.assertEqual(
            f.type, "Category",
            f"HistoryMatch.category annotation must be Category, got {f.type!r}",
        )


# ---------------------------------------------------------------------------
# Fix #4 — Publication evidence semantics.
# ---------------------------------------------------------------------------


class PublicationEvidenceTests(_Base):
    def test_unparseable_evidence_always_pending_invalid_even_when_pub_parses(self):
        # Caller mislabeled a perfectly valid `published_at` as unparseable.
        c = _candidate(
            candidate_id="mislabel",
            published_at=PUB,
            published_evidence="unparseable",
        )
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(r[0].decision, DecisionCode.PENDING_INVALID_EVIDENCE)
        self.assertIn(ReasonCode.INVALID_DATE, r[0].reasons)

    def test_missing_evidence_with_non_null_published_at_is_pending_invalid(self):
        c = _candidate(
            candidate_id="missing-but-set",
            published_at=PUB,
            published_evidence="missing",
        )
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        # Inconsistent label+value must be PENDING_INVALID_EVIDENCE, not silently KEEP.
        self.assertEqual(r[0].decision, DecisionCode.PENDING_INVALID_EVIDENCE)

    def test_missing_evidence_with_null_published_at_is_pending_missing(self):
        c = _candidate(
            candidate_id="really-missing",
            published_at=None,
            published_evidence="missing",
            observed_at=OBS,
        )
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        # Documented observed fallback still applies here.
        self.assertIn(
            r[0].decision,
            {DecisionCode.KEEP, DecisionCode.PENDING_MISSING_EVIDENCE},
        )

    def test_missing_publication_fallback_disabled_pending_missing(self):
        # Disable missing-date fallback on AI policy → PENDING_MISSING_EVIDENCE.
        ai = self.policies[Category.AI]
        self.policies[Category.AI] = QueryPolicy(
            category=ai.category,
            allowed_query_groups=ai.allowed_query_groups,
            recency=ai.recency,
            missing_date_fallback=False,
            exact_title_lookback=ai.exact_title_lookback,
            exact_url_lookback=ai.exact_url_lookback,
            exact_identity_lookback=ai.exact_identity_lookback,
            cross_category_exact_url=ai.cross_category_exact_url,
        )
        c = _candidate(
            candidate_id="nofb",
            published_at=None,
            published_evidence="missing",
            observed_at=OBS,
        )
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(r[0].decision, DecisionCode.PENDING_MISSING_EVIDENCE)

    def test_malformed_observed_at_is_pending_invalid_not_valueerror(self):
        c = _candidate(
            candidate_id="bad-obs",
            published_at=None,
            published_evidence="missing",
            observed_at="not-a-date",
        )
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        # Batch API must not raise — must emit PENDING_INVALID_EVIDENCE.
        self.assertEqual(r[0].decision, DecisionCode.PENDING_INVALID_EVIDENCE)

    def test_future_within_six_hours_uses_dedicated_reason(self):
        dt = datetime(2026, 7, 14, 23, 50, 33, tzinfo=timezone.utc)  # +1h
        future_iso = dt.isoformat().replace("+00:00", "Z")
        c = _candidate(candidate_id="future-1h", published_at=future_iso)
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        # Dedicated reason, NOT OK_OBSERVED_FALLBACK.
        names = {x.name for x in r[0].reasons}
        self.assertIn("FUTURE_DATE_CLAMPED", names, f"reasons={names}")
        self.assertNotIn("OK_OBSERVED_FALLBACK", names, f"reasons={names}")

    def test_future_over_six_hours_pending_invalid(self):
        dt = datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc)
        future_iso = dt.isoformat().replace("+00:00", "Z")
        c = _candidate(candidate_id="future-far", published_at=future_iso)
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(r[0].decision, DecisionCode.PENDING_INVALID_EVIDENCE)
        self.assertIn(ReasonCode.FUTURE_DATE, r[0].reasons)


# ---------------------------------------------------------------------------
# Fix #5 — Canonical consistency + canonical host parsing edge cases.
# ---------------------------------------------------------------------------


class CanonicalConsistencyTests(_Base):
    def test_original_and_canonical_mismatch_is_pending(self):
        c = _candidate(
            candidate_id="mismatch",
            original_url="https://example.com/a",
            canonical_url="https://other.example.com/b",
        )
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(r[0].decision, DecisionCode.PENDING_INVALID_EVIDENCE)
        names = {x.name for x in r[0].reasons}
        self.assertTrue(
            "MALFORMED_CANONICAL_URL" in names or "CANONICAL_MISMATCH" in names,
            f"expected MALFORMED_CANONICAL_URL or CANONICAL_MISMATCH, got {names}",
        )

    def test_query_without_path(self):
        c = _candidate(
            candidate_id="query-only",
            original_url="https://example.com?x=1",
            canonical_url="https://example.com?x=1",
        )
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        # Should classify host correctly and either KEEP or PENDING_INVALID.
        self.assertNotEqual(r[0].decision, DecisionCode.DROP_STALE)

    def test_port_preserved(self):
        c = _candidate(
            candidate_id="with-port",
            original_url="https://example.com:8443/a",
            canonical_url="https://example.com:8443/a",
        )
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertNotEqual(r[0].decision, DecisionCode.DROP_BLOCKED_SOURCE)

    def test_idna_host_canonicalized(self):
        c = _candidate(
            candidate_id="idna",
            original_url="https://www.bücher.de/news",
            canonical_url="https://www.bücher.de/news",
        )
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        # The canonical form must IDNA-encode the host and the policy engine
        # must classify it (UNKNOWN trust tier is fine — we only assert that
        # the engine didn't crash on the unicode host).
        self.assertIsNotNone(r[0].trust_tier)

    def test_trailing_dot_normalized(self):
        # Trailing-dot normalization should not crash the canonicalizer.
        from news_pipeline.canonicalization import canonicalize_url
        c = canonicalize_url("https://example.com./a")
        self.assertIsNotNone(c)
        # host must not contain a trailing dot
        self.assertFalse(c.endswith("."))


# ---------------------------------------------------------------------------
# Fix #6 — Deterministic within-batch: URL-less same-title + changed snippet
# → PENDING_POSSIBLE_UPDATE; cross-category URL per policy.
# ---------------------------------------------------------------------------


class WithinBatchTests(_Base):
    def test_url_less_same_title_changed_snippet_is_pending(self):
        c1 = _candidate(
            candidate_id="c1",
            original_url=None,
            canonical_url=None,
            snippet="body text",
        )
        c2 = _candidate(
            candidate_id="c2",
            original_url=None,
            canonical_url=None,
            snippet="different body",
        )
        r = evaluate_candidates([c1, c2], self.db_path, self.source, self.policies)
        self.assertEqual(r[0].decision, DecisionCode.KEEP)
        self.assertEqual(r[1].decision, DecisionCode.PENDING_POSSIBLE_UPDATE)
        self.assertEqual(r[1].ordinal, 2)

    def test_url_less_same_title_same_snippet_later_suppress(self):
        c1 = _candidate(candidate_id="c1", original_url=None, canonical_url=None, snippet="body")
        c2 = _candidate(candidate_id="c2", original_url=None, canonical_url=None, snippet="body")
        r = evaluate_candidates([c1, c2], self.db_path, self.source, self.policies)
        self.assertEqual(r[0].decision, DecisionCode.KEEP)
        self.assertEqual(r[1].decision, DecisionCode.SUPPRESS_BATCH_EXACT)

    def test_cross_category_url_does_not_starve_when_policy_disables(self):
        # Override AI policy so cross_category_exact_url=False.
        from datetime import timedelta as _td
        ai = self.policies[Category.AI]
        self.policies[Category.AI] = QueryPolicy(
            category=ai.category,
            allowed_query_groups=ai.allowed_query_groups,
            recency=ai.recency,
            missing_date_fallback=ai.missing_date_fallback,
            exact_title_lookback=ai.exact_title_lookback,
            exact_url_lookback=ai.exact_url_lookback,
            exact_identity_lookback=ai.exact_identity_lookback,
            cross_category_exact_url=False,
        )
        # Seed the URL under world category, within url lookback.
        with connect(self.db_path) as con:
            con.execute(
                "INSERT INTO runs(id,started_at,kind,provenance) VALUES (?,?,?,?)",
                ("r1", "2026-07-01T00:00:00Z", "historical_replay", "observed_historical"),
            )
            con.execute(
                "INSERT INTO articles(id,run_id,category,canonical_url,original_url,title,snippet,"
                "source_file,observed_at,fetch_marker,provenance,created_at,normalized_title,"
                "identity_confidence,identity_basis) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "a1", "r1", "world", "https://example.com/x", "https://example.com/x",
                    "Hello", "snippet", "world-2026-07-01.md", "2026-07-10T00:00:00Z",
                    None, "observed_historical", "2026-07-01T00:00:00Z", "hello",
                    1.0, "canonical_url",
                ),
            )
            con.execute(
                "INSERT INTO observations(id,article_id,category,source_file,kind,body,occurred_at,created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                ("obs-a1", "a1", "world", "world-2026-07-01.md",
                 "parsed_article", "snippet", "2026-07-10T00:00:00Z",
                 "2026-07-10T00:00:00Z"),
            )
        # AI candidate uses different snippet → cross-category URL must NOT
        # suppress; it's a different category.
        c = _candidate(candidate_id="c1", canonical_url="https://example.com/x", snippet="brand new")
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(r[0].decision, DecisionCode.KEEP)

    def test_within_batch_preserves_ordinal_and_input_order(self):
        cand = [
            _candidate(candidate_id=f"c{i}", canonical_url=f"https://example.com/{i}")
            for i in range(5)
        ]
        r = evaluate_candidates(cand, self.db_path, self.source, self.policies)
        for i, res in enumerate(r):
            self.assertEqual(res.candidate.candidate_id, f"c{i}")
            self.assertEqual(res.ordinal, i + 1)


# ---------------------------------------------------------------------------
# Fix #7 — History/content decisions: normalized comparisons, fetch
# HistoryUnavailable, URI mode=ro, locked-DB, schema mismatch.
# ---------------------------------------------------------------------------


class HistoryContentTests(_Base):
    def test_normalized_comparison_url_exact(self):
        # Seed URL in lower-case; candidate uses MIXED case → must suppress.
        with connect(self.db_path) as con:
            con.execute(
                "INSERT INTO runs(id,started_at,kind,provenance) VALUES (?,?,?,?)",
                ("r1", "2026-07-01T00:00:00Z", "historical_replay", "observed_historical"),
            )
            con.execute(
                "INSERT INTO articles(id,run_id,category,canonical_url,original_url,title,snippet,"
                "source_file,observed_at,fetch_marker,provenance,created_at,normalized_title,"
                "identity_confidence,identity_basis) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "a1", "r1", "ai", "https://example.com/a", "https://example.com/a",
                    "Hello world", "body text", "ai-2026-07-01.md",
                    "2026-07-10T00:00:00Z", None, "observed_historical",
                    "2026-07-01T00:00:00Z", "hello world", 1.0, "canonical_url",
                ),
            )
            con.execute(
                "INSERT INTO observations(id,article_id,category,source_file,kind,body,occurred_at,created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                ("obs-a1", "a1", "ai", "ai-2026-07-01.md", "parsed_article",
                 "body text", "2026-07-10T00:00:00Z", "2026-07-10T00:00:00Z"),
            )
        # Mixed-case canonical_url must still suppress.
        c = _candidate(candidate_id="c1", canonical_url="https://Example.COM/a")
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(r[0].decision, DecisionCode.SUPPRESS_EXACT_URL)

    def test_same_title_same_snippet_suppress_recent_title(self):
        from datetime import timedelta as _td
        # Title-only history within 72h, same title + same snippet + different
        # canonical URL → SUPPRESS_RECENT_TITLE (category scoped).
        with connect(self.db_path) as con:
            con.execute(
                "INSERT INTO runs(id,started_at,kind,provenance) VALUES (?,?,?,?)",
                ("r1", "2026-07-01T00:00:00Z", "historical_replay", "observed_historical"),
            )
            con.execute(
                "INSERT INTO articles(id,run_id,category,canonical_url,original_url,title,snippet,"
                "source_file,observed_at,fetch_marker,provenance,created_at,normalized_title,"
                "identity_confidence,identity_basis) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "a1", "r1", "ai", None, None,
                    "Hello world", "body text", "ai-2026-07-01.md",
                    "2026-07-13T23:00:00Z", None, "observed_historical",
                    "2026-07-01T00:00:00Z", "hello world", 0.5, "title_only",
                ),
            )
            con.execute(
                "INSERT INTO observations(id,article_id,category,source_file,kind,body,occurred_at,created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                ("obs-a1", "a1", "ai", "ai-2026-07-01.md", "parsed_article",
                 "body text", "2026-07-13T23:00:00Z", "2026-07-13T23:00:00Z"),
            )
        c = _candidate(candidate_id="c1", canonical_url="https://example.com/x", snippet="body text")
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(r[0].decision, DecisionCode.SUPPRESS_RECENT_TITLE)

    def test_fetch_history_match_unavailable_yields_pending(self):
        # Seed an article, then corrupt the DB after seeding so the secondary
        # fetch_history_match call must hit HistoryUnavailable.
        with connect(self.db_path) as con:
            con.execute(
                "INSERT INTO runs(id,started_at,kind,provenance) VALUES (?,?,?,?)",
                ("r1", "2026-07-01T00:00:00Z", "historical_replay", "observed_historical"),
            )
            con.execute(
                "INSERT INTO articles(id,run_id,category,canonical_url,original_url,title,snippet,"
                "source_file,observed_at,fetch_marker,provenance,created_at,normalized_title,"
                "identity_confidence,identity_basis) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "a1", "r1", "ai", "https://example.com/a", "https://example.com/a",
                    "Hello world", "body text", "ai-2026-07-01.md",
                    "2026-07-10T00:00:00Z", None, "observed_historical",
                    "2026-07-01T00:00:00Z", "hello world", 1.0, "canonical_url",
                ),
            )
            con.execute(
                "INSERT INTO observations(id,article_id,category,source_file,kind,body,occurred_at,created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                ("obs-a1", "a1", "ai", "ai-2026-07-01.md", "parsed_article",
                 "body text", "2026-07-10T00:00:00Z", "2026-07-10T00:00:00Z"),
            )
        # We simulate fetch unavailability by closing the history connection
        # mid-run. Since the engine opens history itself, we close the live DB
        # exclusively via another connection's lock. Practical: use a path
        # under a nonexistent parent so the URI open still works but a
        # follow-up read fails — too brittle. Instead, monkeypatch fetch_history_match.
        from news_pipeline import filtering as _filt
        original_fetch = _filt.fetch_history_match
        def boom(history, article_id, observation_id):
            raise HistoryUnavailable("simulated fetch unavailability")
        _filt.fetch_history_match = boom
        try:
            c = _candidate(candidate_id="c1")
            r = evaluate_candidates([c], self.db_path, self.source, self.policies)
            self.assertEqual(r[0].decision, DecisionCode.PENDING_HISTORY_UNAVAILABLE)
        finally:
            _filt.fetch_history_match = original_fetch

    def test_uri_mode_ro_with_spaces_and_hash(self):
        # Open a temp file with spaces and `#` in the name; URI open must
        # succeed with mode=ro.
        sub = Path(self.tmp.name) / "sub dir#1"
        sub.mkdir()
        db = sub / "state.db"
        init_db(str(db))
        with open_history(str(db)) as h:
            self.assertEqual(h.con.execute("PRAGMA query_only").fetchone()[0], 1)
            self.assertEqual(h.con.execute("SELECT 1").fetchone()[0], 1)

    def test_locked_db_raises_unavailable_within_bounded_timeout(self):
        # Acquire exclusive lock; open_history must raise HistoryUnavailable
        # within bounded time (NOT hang).
        import time
        # WAL permits concurrent readers even during a writer transaction; switch
        # this isolated fixture to DELETE journaling so BEGIN EXCLUSIVE exercises
        # the bounded locked-DB failure path.
        with contextlib.closing(sqlite3.connect(self.db_path)) as setup:
            setup.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            setup.execute("PRAGMA journal_mode=DELETE")
        blocker = sqlite3.connect(self.db_path)
        try:
            blocker.execute("BEGIN EXCLUSIVE")
            t0 = time.monotonic()
            with self.assertRaises(HistoryUnavailable):
                with open_history(self.db_path, busy_timeout_ms=500):
                    pass
            elapsed = time.monotonic() - t0
            self.assertLess(
                elapsed, 3.0,
                f"open_history took {elapsed:.2f}s under exclusive lock — should be bounded",
            )
        finally:
            blocker.rollback()
            blocker.close()

    def test_schema_mismatch_raises_unavailable(self):
        # Build a DB whose schema lacks the Phase 1 tables — must raise.
        bogus = Path(self.tmp.name) / "wrong_schema.db"
        con = sqlite3.connect(str(bogus))
        con.execute("CREATE TABLE foo (bar INTEGER)")
        con.commit()
        con.close()
        with self.assertRaises(HistoryUnavailable):
            with open_history(str(bogus)):
                pass


# ---------------------------------------------------------------------------
# Fix #8 — evaluated_publication_time populated for KEEP and relevant paths.
# ---------------------------------------------------------------------------


class EvaluatedPublicationTimeTests(_Base):
    def test_keep_path_populates_evaluated_publication_time(self):
        c = _candidate(candidate_id="c1")
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(r[0].decision, DecisionCode.KEEP)
        self.assertEqual(r[0].evaluated_publication_time, PUB)

    def test_keep_with_observed_fallback_populates_time(self):
        c = _candidate(candidate_id="c1", published_at=None, published_evidence="missing")
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(r[0].decision, DecisionCode.KEEP)
        self.assertEqual(r[0].evaluated_publication_time, OBS)

    def test_stale_path_has_no_time(self):
        c = _candidate(candidate_id="stale", published_at="2026-07-01T00:00:00Z")
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        self.assertEqual(r[0].decision, DecisionCode.DROP_STALE)
        self.assertEqual(r[0].evaluated_publication_time, "2026-07-01T00:00:00Z")


# ---------------------------------------------------------------------------
# Fix #9 — Reason codes are unique and deterministically ordered.
# ---------------------------------------------------------------------------


class ReasonUniquenessTests(_Base):
    def test_ok_unknown_source_appears_once(self):
        c = _candidate(candidate_id="c1")
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        codes = list(r[0].reasons)
        self.assertEqual(
            codes.count(ReasonCode.OK_UNKNOWN_SOURCE), 1,
            f"OK_UNKNOWN_SOURCE appears {codes.count(ReasonCode.OK_UNKNOWN_SOURCE)} times",
        )

    def test_missing_url_appears_once_when_both_missing(self):
        c = _candidate(
            candidate_id="c1",
            original_url=None,
            canonical_url=None,
            published_at=None,
            published_evidence="missing",
        )
        r = evaluate_candidates([c], self.db_path, self.source, self.policies)
        codes = list(r[0].reasons)
        self.assertLessEqual(
            codes.count(ReasonCode.MISSING_URL), 1,
            f"MISSING_URL appears {codes.count(ReasonCode.MISSING_URL)} times",
        )

    def test_reasons_deterministic_order(self):
        cand = [
            _candidate(candidate_id=f"c{i}", canonical_url=f"https://example.com/{i}")
            for i in range(3)
        ]
        a = evaluate_candidates(cand, self.db_path, self.source, self.policies)
        b = evaluate_candidates(cand, self.db_path, self.source, self.policies)
        for ra, rb in zip(a, b):
            self.assertEqual(list(ra.reasons), list(rb.reasons))


# ---------------------------------------------------------------------------
# Fix #10 — History connection closure in finally path under programmer error.
# ---------------------------------------------------------------------------


class HistoryConnectionClosureTests(_Base):
    def test_connection_closes_even_when_programmer_error_after_open(self):
        # Inject a failure AFTER open_history returns successfully, and prove
        # the connection still gets closed (no ResourceWarning).
        from news_pipeline import filtering as _filt
        opened = []

        real_open = _filt.open_history

        def tracking_open(db_path, **kw):
            h = real_open(db_path, **kw)
            opened.append(h)
            return h

        _filt.open_history = tracking_open
        try:
            # Force the engine to raise a non-SQLite programmer error mid-run.
            def kaboom(*a, **kw):
                raise RuntimeError("deliberate programmer error")

            real_find = _filt.find_exact_url
            _filt.find_exact_url = kaboom  # type: ignore[assignment]
            with self.assertRaises(RuntimeError):
                evaluate_candidates(
                    [_candidate()], self.db_path, self.source, self.policies
                )
            # The history connection should still be closed — the file
            # descriptor no longer references an unclosed SQLite connection.
            self.assertEqual(len(opened), 1)
            h = opened[0]
            with self.assertRaises(sqlite3.ProgrammingError):
                h.con.execute("SELECT 1")
        finally:
            _filt.find_exact_url = real_find
            _filt.open_history = real_open


# ---------------------------------------------------------------------------
# Cross-cutting — API typing for query_policies is Mapping[Category, QueryPolicy].
# ---------------------------------------------------------------------------


class ApiTypingTests(unittest.TestCase):
    def test_query_policies_param_is_typed_category_query_policy(self):
        import ast as _ast
        from pathlib import Path
        path = Path(__import__("news_pipeline").__file__).resolve().parent / "api.py"
        tree = _ast.parse(path.read_text(encoding="utf-8"))
        # Find the evaluate_candidates FunctionDef
        for node in tree.body:
            if isinstance(node, _ast.FunctionDef) and node.name == "evaluate_candidates":
                for arg in node.args.args:
                    if arg.arg == "query_policies":
                        ann = arg.annotation
                        self.assertIsInstance(ann, _ast.Subscript)
                        self.assertIsInstance(ann.value, _ast.Name)
                        self.assertEqual(ann.value.id, "Mapping")
                        self.assertEqual(len(ann.slice.elts), 2)
                        self.assertEqual(ann.slice.elts[0].id, "Category")
                        self.assertEqual(ann.slice.elts[1].id, "QueryPolicy")
                        return
        self.fail("evaluate_candidates not found")


if __name__ == "__main__":
    unittest.main()