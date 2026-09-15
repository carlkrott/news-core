"""Phase 4 — Slice 2 shadow ledger tests.

Fifteen unittest methods for ``news_pipeline.briefing_ledger`` per the
Phase 4 supervisor addendum §3 / §6 (tests 41-55). Slice 2 owns only
this file plus ``news_pipeline.briefing_ledger``; every other Phase 4
file is forbidden here, and no Phase 1-3 / wrapper / cron / service /
DB file may be touched.

All tests use :memory: sqlite3 connections opened with
``isolation_level=None`` so the ledger operates in autocommit mode. Test
54 uses a TemporaryDirectory-backed file so two processes / connections
can contend for ``BEGIN IMMEDIATE``.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from news_pipeline.contracts import validate_utc_iso
from news_pipeline.event_contracts import SemanticDecision
from news_pipeline.models import Category

from news_pipeline import briefing_ledger


# ---------------------------------------------------------------------------
# Test fixtures (ledger-only; no Phase3 imports)
# ---------------------------------------------------------------------------


def _payload_dict(
    cid: str,
    category: Category = Category.AI,
    decision: SemanticDecision = SemanticDecision.distinct_event,
    title: str = "Headline",
    summary: str = "Summary text",
    url: str = "https://example.test/article",
) -> dict:
    """Build a canonical payload dict used by every ledger test.

    The keys are inserted in a canonical order; ``normalize_canonical_payload``
    sorts them so dicts with reordered keys still produce byte-identical bytes.
    Tests do not depend on any embedded ``ordinal`` field — the ledger assigns
    ``event_ordinal`` purely from the event-list input position.
    """
    return {
        "candidate_id": cid,
        "category": category.value,
        "decision": decision.value,
        "title": title,
        "summary": summary,
        "url": url,
    }


def _payload_bytes(payload: dict) -> bytes:
    """Canonical serialization, matching ``briefing_ledger`` exactly.

    Always runs through ``briefing_ledger.normalize_canonical_payload``
    so dict-insertion order does not produce a different byte sequence
    than the ledger's stored canonical bytes (which are sort_keys=True).
    """
    return briefing_ledger.normalize_canonical_payload(payload)


def _new_in_memory_ledger(foreign_keys_off: bool = False):
    """Open a fresh autocommit :memory: connection; return (conn, ledger)."""
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.execute("PRAGMA foreign_keys=OFF")
    ledger = briefing_ledger.BriefingLedger(conn)
    return conn, ledger


def _make_event(cid: str, payload: dict) -> briefing_ledger.ShadowEvent:
    """Helper to build one ``ShadowEvent`` with the canonical recorded_at_utc."""
    return briefing_ledger.ShadowEvent(
        candidate_id=cid,
        category=Category(payload["category"]),
        decision=SemanticDecision(payload["decision"]),
        payload_json=_payload_bytes(payload),
        recorded_at_utc="2026-07-02T07:00:00Z",
    )


# ---------------------------------------------------------------------------
# Tests 41-55 (method names frozen; bodies encode all 11 parent blockers)
# ---------------------------------------------------------------------------


class TestBriefingLedger(unittest.TestCase):
    """Phase 4 Slice 2 tests for the shadow briefing ledger."""

    maxDiff = None

    # ---- 41 ------------------------------------------------------------

    def test_requires_open_autocommit_connection(self) -> None:
        # 41: the ledger rejects a closed connection, refuses non-autocommit
        # connections (any isolation_level other than None), and never opens
        # or closes the underlying connection itself.

        # (a) Live autocommit connection: accepted.
        ok = sqlite3.connect(":memory:", isolation_level=None)
        ok.execute("PRAGMA foreign_keys=OFF")
        try:
            ledger = briefing_ledger.BriefingLedger(ok)
            ledger.initialize_schema()
            row = ok.execute("SELECT 1").fetchone()
            self.assertEqual(row, (1,))
            fk = ok.execute("PRAGMA foreign_keys").fetchone()[0]
            self.assertEqual(fk, 1)
            bt = ok.execute("PRAGMA busy_timeout").fetchone()[0]
            self.assertEqual(bt, 5000)
        finally:
            ok.close()

        # (b) A connection with isolation_level set (deferred txns) is
        # rejected because ``begin_run`` would conflict with sqlite3's
        # implicit transaction.
        deferred = sqlite3.connect(":memory:")
        try:
            with self.assertRaises(
                (
                    briefing_ledger.LedgerConnectionError,
                    briefing_ledger.LedgerContractError,
                    ValueError,
                    TypeError,
                )
            ):
                briefing_ledger.BriefingLedger(deferred)
        finally:
            deferred.close()

        # (c) A closed connection passed in is rejected at construction.
        closed = sqlite3.connect(":memory:", isolation_level=None)
        closed.execute("PRAGMA foreign_keys=OFF")
        closed.close()
        with self.assertRaises(
            (
                briefing_ledger.LedgerConnectionError,
                sqlite3.ProgrammingError,
                ValueError,
                TypeError,
            )
        ):
            briefing_ledger.BriefingLedger(closed)

        # (d) The ledger never opened or closed any path of its own.
        before = set(os.listdir(os.getcwd()))
        for _ in range(3):
            conn = sqlite3.connect(":memory:", isolation_level=None)
            conn.execute("PRAGMA foreign_keys=OFF")
            ledger = briefing_ledger.BriefingLedger(conn)
            ledger.initialize_schema()
            conn.close()
        after = set(os.listdir(os.getcwd()))
        new_artifacts = {p for p in (after - before) if p.endswith((".db", ".sqlite", ".sqlite3", ".wal", ".shm"))}
        self.assertEqual(new_artifacts, set(), f"unexpected sqlite artifacts: {sorted(new_artifacts)}")

    # ---- 42 ------------------------------------------------------------

    def test_initializes_exact_schema_pragmas_and_indexes(self) -> None:
        # 42: initialize_schema creates the exact three tables, the
        # index, sets foreign_keys=ON, busy_timeout=5000, and the
        # run_events table does not carry a "delivered" column.
        conn, ledger = _new_in_memory_ledger()
        try:
            # All public operations except initialize_schema are guarded.
            uninitialized_calls = (
                lambda: ledger.begin_run(
                    "run-A",
                    "2026-07-01T07:00:00Z",
                    "2026-07-02T07:00:00Z",
                    "2026-07-02T06:59:58Z",
                ),
                lambda: ledger.seen_candidate_ids(()),
                lambda: ledger.complete_run(
                    "run-A", "2026-07-02T07:00:00Z", ()
                ),
                lambda: ledger.fail_run(
                    "run-A", "2026-07-02T07:00:00Z", "E_TEST"
                ),
                lambda: ledger.recover_stale_runs(
                    datetime(2026, 7, 2, tzinfo=timezone.utc),
                    "2026-07-02T07:00:00Z",
                ),
                ledger.last_completed_upper_utc,
            )
            for call in uninitialized_calls:
                with self.assertRaises(briefing_ledger.LedgerStateError):
                    call()

            ledger.initialize_schema()

            tables = sorted(
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'shadow_briefing_%'"
                ).fetchall()
            )
            self.assertEqual(
                tables,
                [
                    "shadow_briefing_run_events",
                    "shadow_briefing_runs",
                    "shadow_briefing_seen",
                ],
            )

            indexes = sorted(
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_shadow_briefing_%'"
                ).fetchall()
            )
            self.assertEqual(
                indexes,
                ["idx_shadow_briefing_runs_status_updated"],
            )

            fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
            self.assertEqual(fk, 1)
            bt = conn.execute("PRAGMA busy_timeout").fetchone()[0]
            self.assertEqual(bt, 5000)

            runs_cols = [row[1] for row in conn.execute("PRAGMA table_info(shadow_briefing_runs)").fetchall()]
            self.assertEqual(
                runs_cols,
                [
                    "run_id",
                    "window_lower_utc",
                    "window_upper_utc",
                    "started_at_utc",
                    "updated_at_utc",
                    "status",
                    "error_code",
                ],
            )
            status_row = conn.execute(
                "SELECT sql FROM sqlite_master WHERE name='shadow_briefing_runs'"
            ).fetchone()[0]
            self.assertIn(
                "CHECK(status IN ('RUNNING','COMPLETED','FAILED','STALE'))",
                status_row,
            )

            seen_cols = [row[1] for row in conn.execute("PRAGMA table_info(shadow_briefing_seen)").fetchall()]
            self.assertEqual(
                seen_cols,
                [
                    "candidate_id",
                    "first_run_id",
                    "first_seen_at_utc",
                    "category",
                    "decision",
                    "payload_json",
                ],
            )
            decision_row = conn.execute(
                "SELECT sql FROM sqlite_master WHERE name='shadow_briefing_seen'"
            ).fetchone()[0]
            self.assertIn(
                "CHECK(decision IN ('distinct_event','material_update'))",
                decision_row,
            )

            ev_cols = [row[1] for row in conn.execute("PRAGMA table_info(shadow_briefing_run_events)").fetchall()]
            self.assertEqual(
                ev_cols,
                [
                    "run_id",
                    "candidate_id",
                    "event_status",
                    "payload_json",
                    "event_ordinal",
                ],
            )
            ev_sql = conn.execute(
                "SELECT sql FROM sqlite_master WHERE name='shadow_briefing_run_events'"
            ).fetchone()[0]
            self.assertIn(
                "CHECK(event_status IN ('RECORDED','ALREADY_SEEN'))",
                ev_sql,
            )
            self.assertIn(
                "CHECK(event_ordinal >= 0)",
                ev_sql,
            )
            # SQLite canonicalizes ``PRIMARY KEY(a, b)`` with a space; the
            # addendum writes ``PRIMARY KEY(run_id,candidate_id)`` without
            # one. Match both forms so the assertion survives re-formatting.
            self.assertIn(
                "PRIMARY KEY(run_id,candidate_id)".replace(", ", ","),
                ev_sql.replace(", ", ","),
            )
            self.assertIn(
                "UNIQUE(run_id,event_ordinal)".replace(", ", ","),
                ev_sql.replace(", ", ","),
            )
            for col in ("delivered", "delivered_at_utc", "delivered_chunk"):
                self.assertNotIn(col, ev_cols)
        finally:
            conn.close()

    # ---- 43 ------------------------------------------------------------

    def test_begin_run_is_idempotent_when_identical(self) -> None:
        # 43: a second begin_run with the same run_id and identical
        # (lower, upper, started) timestamps is a no-op. Each persisted
        # timestamp must be the normalized Z form via validate_utc_iso
        # (blocker #4) and lower < upper is required (addendum §3).
        conn, ledger = _new_in_memory_ledger()
        try:
            ledger.initialize_schema()
            # Use ISO with +00:00 offset to prove normalization to Z works
            # when begin_run is called with a non-Z but aware UTC form.
            lower = "2026-07-01T07:00:00+00:00"
            upper = "2026-07-02T07:00:00+00:00"
            started = "2026-07-02T06:59:58+00:00"
            ledger.begin_run("run-A", lower, upper, started)
            before_rows = conn.execute(
                "SELECT rowid, updated_at_utc, window_lower_utc, "
                "window_upper_utc, started_at_utc "
                "FROM shadow_briefing_runs WHERE run_id=?",
                ("run-A",),
            ).fetchall()
            self.assertEqual(len(before_rows), 1)
            row = before_rows[0]
            original_updated = row[1]
            # Normalized Z form must be stored.
            self.assertEqual(row[2], "2026-07-01T07:00:00Z")
            self.assertEqual(row[3], "2026-07-02T07:00:00Z")
            self.assertEqual(row[4], "2026-07-02T06:59:58Z")

            # Identical retry is idempotent — supply the same +00:00
            # string and verify row state is unchanged.
            ledger.begin_run("run-A", lower, upper, started)

            after_rows = conn.execute(
                "SELECT rowid, updated_at_utc FROM shadow_briefing_runs WHERE run_id=?",
                ("run-A",),
            ).fetchall()
            self.assertEqual(len(after_rows), 1)
            self.assertEqual(after_rows[0][1], original_updated)
        finally:
            conn.close()

    # ---- 44 ------------------------------------------------------------

    def test_begin_run_conflict_when_fields_differ(self) -> None:
        # 44: a conflicting begin_run on the same run_id raises
        # RunConflictError for every mismatched field. Also exercise
        # blocker #4 timestamps / lower<upper guards.

        conn, ledger = _new_in_memory_ledger()
        try:
            ledger.initialize_schema()
            ledger.begin_run(
                "run-A",
                "2026-07-01T07:00:00Z",
                "2026-07-02T07:00:00Z",
                "2026-07-02T06:59:58Z",
            )

            # Subcase: lower >= upper must raise LedgerContractError.
            with self.assertRaises(briefing_ledger.LedgerContractError):
                ledger.begin_run(
                    "run-A",
                    "2026-07-02T07:00:00Z",
                    "2026-07-02T07:00:00Z",
                    "2026-07-02T06:59:58Z",
                )
            with self.assertRaises(briefing_ledger.LedgerContractError):
                ledger.begin_run(
                    "run-A",
                    "2026-07-02T08:00:00Z",
                    "2026-07-02T07:00:00Z",
                    "2026-07-02T06:59:58Z",
                )

            # Subcase: non-UTC / naive timestamps rejected.
            for lower_bad in (
                "2026-07-01T07:00:00",          # naive
                "2026-07-01T07:00:00+01:00",    # nonzero offset
                "2026-07-01T07:00:00-05:00",    # nonzero offset
                "yesterday",                     # garbage
            ):
                with self.assertRaises(
                    briefing_ledger.LedgerContractError,
                    msg=f"begin_run lower={lower_bad!r} should be rejected",
                ):
                    ledger.begin_run(
                        "run-A",
                        lower_bad,
                        "2026-07-02T07:00:00Z",
                        "2026-07-02T06:59:58Z",
                    )

            # Subcase: bound/validate run_id 1..256 (blocker #8).
            for bad_id in ("", "x" * 257):
                with self.assertRaises(briefing_ledger.LedgerContractError):
                    ledger.begin_run(
                        bad_id,
                        "2026-07-01T07:00:00Z",
                        "2026-07-02T07:00:00Z",
                        "2026-07-02T06:59:58Z",
                    )

            for label, kwargs in (
                (
                    "lower",
                    {
                        "lower": "2026-07-01T07:30:00Z",
                        "upper": "2026-07-02T07:00:00Z",
                        "started": "2026-07-02T06:59:58Z",
                    },
                ),
                (
                    "upper",
                    {
                        "lower": "2026-07-01T07:00:00Z",
                        "upper": "2026-07-02T07:30:00Z",
                        "started": "2026-07-02T06:59:58Z",
                    },
                ),
                (
                    "started",
                    {
                        "lower": "2026-07-01T07:00:00Z",
                        "upper": "2026-07-02T07:00:00Z",
                        "started": "2026-07-02T06:55:00Z",
                    },
                ),
            ):
                with self.assertRaises(
                    briefing_ledger.RunConflictError,
                    msg=f"begin_run conflict on {label} mismatch",
                ):
                    ledger.begin_run("run-A", **kwargs)

            # Idempotency applies only to the pristine RUNNING row. Once
            # complete_run changes status/updated_at, the original begin
            # arguments conflict with the persisted six-field state.
            payload = _payload_dict("cand-begin-conflict")
            ledger.complete_run(
                "run-A",
                "2026-07-02T07:00:05Z",
                [_make_event("cand-begin-conflict", payload)],
            )
            with self.assertRaises(briefing_ledger.RunConflictError):
                ledger.begin_run(
                    "run-A",
                    "2026-07-01T07:00:00Z",
                    "2026-07-02T07:00:00Z",
                    "2026-07-02T06:59:58Z",
                )
        finally:
            conn.close()

    # ---- 45 ------------------------------------------------------------

    def test_seen_candidate_ids_preserves_request_order(self) -> None:
        # 45: seen_candidate_ids returns the exact input order regardless
        # of underlying B-tree order; rejects duplicate input with
        # LedgerContractError (blocker #7); only accepts a tuple.
        conn, ledger = _new_in_memory_ledger()
        try:
            ledger.initialize_schema()
            ledger.begin_run(
                "run-A",
                "2026-07-01T07:00:00Z",
                "2026-07-02T07:00:00Z",
                "2026-07-02T06:59:58Z",
            )

            ids_in = ("zeta", "alpha", "mid", "zed")
            payloads = [_payload_dict(cid) for cid in ids_in]
            events = [_make_event(cid, p) for cid, p in zip(ids_in, payloads)]
            result = ledger.complete_run(
                "run-A",
                "2026-07-02T07:00:05Z",
                events,
            )
            # All four are new in input order.
            self.assertEqual(tuple(result.new_ids), ids_in)
            self.assertEqual(tuple(result.already_seen_ids), ())

            # Asking in the same order returns them in input order.
            self.assertEqual(ledger.seen_candidate_ids(ids_in), ids_in)

            # Asking in a different order returns them in the new order.
            alt = ("zed", "mid", "alpha", "zeta")
            self.assertEqual(ledger.seen_candidate_ids(alt), alt)

            # Subset, in order: alpha, zeta (the supplied order).
            self.assertEqual(
                ledger.seen_candidate_ids(("alpha", "zeta")),
                ("alpha", "zeta"),
            )

            # Subset with an unseen id in the middle.
            partial = ledger.seen_candidate_ids(("alpha", "ghost", "zeta"))
            self.assertEqual(tuple(partial), ("alpha", "zeta"))

            # Empty input -> empty tuple.
            self.assertEqual(ledger.seen_candidate_ids(()), ())

            # Blockers: duplicate input rejected with LedgerContractError
            # (NOT silent dedup). Only tuple accepted; list rejected.
            with self.assertRaises(briefing_ledger.LedgerContractError):
                ledger.seen_candidate_ids(("alpha", "alpha"))
            with self.assertRaises(briefing_ledger.LedgerContractError):
                ledger.seen_candidate_ids(["alpha"])  # type: ignore[list-item]
        finally:
            conn.close()

    # ---- 46 ------------------------------------------------------------

    def test_complete_run_atomically_records_new_ids(self) -> None:
        # 46: complete_run inserts into global seen and run_events
        # atomically; status flips to COMPLETED; both inserts share one
        # transaction boundary. Event ordinals are input-position based
        # (blocker #2) — no payload-ordinal parsing.
        conn, ledger = _new_in_memory_ledger()
        try:
            ledger.initialize_schema()
            ledger.begin_run(
                "run-A",
                "2026-07-01T07:00:00Z",
                "2026-07-02T07:00:00Z",
                "2026-07-02T06:59:58Z",
            )
            ids = ["cand-1", "cand-2", "cand-3"]
            payloads = [_payload_dict(cid) for cid in ids]
            events = [_make_event(cid, p) for cid, p in zip(ids, payloads)]

            result = ledger.complete_run(
                "run-A",
                "2026-07-02T07:00:05Z",
                events,
            )

            self.assertEqual(tuple(result.new_ids), tuple(ids))
            self.assertEqual(tuple(result.already_seen_ids), ())

            seen_rows = conn.execute(
                "SELECT candidate_id, category, decision, first_run_id FROM shadow_briefing_seen ORDER BY candidate_id"
            ).fetchall()
            self.assertEqual(
                [r[0] for r in seen_rows],
                ["cand-1", "cand-2", "cand-3"],
            )
            self.assertTrue(all(r[3] == "run-A" for r in seen_rows))

            ev_rows = conn.execute(
                "SELECT candidate_id, event_status, event_ordinal FROM shadow_briefing_run_events "
                "WHERE run_id=? ORDER BY event_ordinal",
                ("run-A",),
            ).fetchall()
            self.assertEqual(
                [r[0] for r in ev_rows],
                ["cand-1", "cand-2", "cand-3"],
            )
            self.assertTrue(all(r[1] == "RECORDED" for r in ev_rows))
            self.assertEqual([r[2] for r in ev_rows], [0, 1, 2])

            run_row = conn.execute(
                "SELECT status, error_code, updated_at_utc FROM shadow_briefing_runs WHERE run_id=?",
                ("run-A",),
            ).fetchone()
            self.assertEqual(run_row[0], "COMPLETED")
            self.assertIsNone(run_row[1])
            # updated_at_utc is normalized to Z form via validate_utc_iso.
            self.assertEqual(run_row[2], "2026-07-02T07:00:05Z")
        finally:
            conn.close()

    # ---- 47 ------------------------------------------------------------

    def test_complete_run_partitions_already_seen_across_runs(self) -> None:
        # 47: complete_run on a second run partitions candidates into
        # RECORDED (new) and ALREADY_SEEN. Cross-run ALREADY_SEEN may
        # carry a different payload than the first-seen entry: run-B
        # persists its own canonical payload in run_events.payload_json
        # (blocker #3, #10); shadow_briefing_seen.payload_json stays as
        # the first-seen one — proven by reading both columns.
        conn, ledger = _new_in_memory_ledger()
        try:
            ledger.initialize_schema()

            # First run records payload A.
            ledger.begin_run(
                "run-A",
                "2026-07-01T07:00:00Z",
                "2026-07-02T07:00:00Z",
                "2026-07-02T06:59:58Z",
            )
            payload_shared_A = _payload_dict("shared", summary="Summary A")
            payload_only_a = _payload_dict("only-a", summary="Only-A")
            res_a = ledger.complete_run(
                "run-A",
                "2026-07-02T07:00:05Z",
                [
                    _make_event("shared", payload_shared_A),
                    _make_event("only-a", payload_only_a),
                ],
            )
            self.assertEqual(tuple(res_a.new_ids), ("shared", "only-a"))
            self.assertEqual(tuple(res_a.already_seen_ids), ())

            # Second run overlapping with the first re-uses ``shared``
            # with payload B (different bytes from payload_A). It must
            # succeed as ALREADY_SEEN and persist payload B in run-B's
            # own run_events row.
            ledger.begin_run(
                "run-B",
                "2026-07-02T07:00:00Z",
                "2026-07-03T07:00:00Z",
                "2026-07-03T06:59:58Z",
            )
            payload_shared_B = _payload_dict("shared", summary="Summary B")
            payload_only_b = _payload_dict("only-b", summary="Only-B")
            res_b = ledger.complete_run(
                "run-B",
                "2026-07-03T07:00:05Z",
                [
                    _make_event("shared", payload_shared_B),
                    _make_event("only-b", payload_only_b),
                ],
            )
            self.assertEqual(tuple(res_b.new_ids), ("only-b",))
            self.assertEqual(tuple(res_b.already_seen_ids), ("shared",))

            # run_events.payload_json carries run-B's canonical bytes
            # for ``shared``, distinct from run-A's.
            ev_b_shared = conn.execute(
                "SELECT payload_json FROM shadow_briefing_run_events "
                "WHERE run_id='run-B' AND candidate_id='shared'"
            ).fetchone()[0]
            self.assertEqual(
                ev_b_shared,
                _payload_bytes(payload_shared_B).decode("utf-8"),
            )

            # shadow_briefing_seen.payload_json still holds the
            # first-seen payload (run-A's). The global first-seen
            # payload is unchanged by run-B's later write.
            seen_shared = conn.execute(
                "SELECT payload_json, first_run_id FROM "
                "shadow_briefing_seen WHERE candidate_id='shared'"
            ).fetchone()
            self.assertEqual(seen_shared[1], "run-A")
            self.assertEqual(
                seen_shared[0],
                _payload_bytes(payload_shared_A).decode("utf-8"),
            )

            ev_b = conn.execute(
                "SELECT candidate_id, event_status FROM shadow_briefing_run_events "
                "WHERE run_id=? ORDER BY candidate_id",
                ("run-B",),
            ).fetchall()
            self.assertEqual(
                [r[0] for r in ev_b],
                ["only-b", "shared"],
            )
            d = {r[0]: r[1] for r in ev_b}
            self.assertEqual(d["only-b"], "RECORDED")
            self.assertEqual(d["shared"], "ALREADY_SEEN")

            # Global seen set still has only one row per candidate.
            seen_rows = conn.execute(
                "SELECT candidate_id, first_run_id FROM shadow_briefing_seen ORDER BY candidate_id"
            ).fetchall()
            self.assertEqual([r[0] for r in seen_rows], ["only-a", "only-b", "shared"])
            self.assertEqual(
                {r[0]: r[1] for r in seen_rows}["shared"],
                "run-A",
            )
        finally:
            conn.close()

    # ---- 48 ------------------------------------------------------------

    def test_complete_run_identical_retry_is_idempotent(self) -> None:
        # 48: an identical retry returns the persisted new/already
        # partition and does not mutate the run row. ``updated`` may be
        # supplied in +00:00 form and is normalized to Z (blocker #4).
        conn, ledger = _new_in_memory_ledger()
        try:
            ledger.initialize_schema()
            ledger.begin_run(
                "run-A",
                "2026-07-01T07:00:00Z",
                "2026-07-02T07:00:00Z",
                "2026-07-02T06:59:58Z",
            )
            payloads = [
                _payload_dict("cand-1"),
                _payload_dict("cand-2"),
            ]
            events = [_make_event(p["candidate_id"], p) for p in payloads]
            # First call uses the canonical Z form for updated.
            first = ledger.complete_run("run-A", "2026-07-02T07:00:05Z", events)
            self.assertEqual(tuple(first.new_ids), ("cand-1", "cand-2"))
            snapshot_runs = conn.execute(
                "SELECT updated_at_utc, status FROM shadow_briefing_runs WHERE run_id=?",
                ("run-A",),
            ).fetchone()
            snapshot_events = conn.execute(
                "SELECT payload_json FROM shadow_briefing_run_events WHERE run_id=? ORDER BY event_ordinal",
                ("run-A",),
            ).fetchall()

            # Identical retry with +00:00 form: normalize to Z; persisted
            # row must NOT mutate (already-COMPLETED).
            second = ledger.complete_run("run-A", "2026-07-02T07:00:05+00:00", events)
            self.assertEqual(tuple(second.new_ids), ("cand-1", "cand-2"))
            self.assertEqual(tuple(second.already_seen_ids), ())

            after_runs = conn.execute(
                "SELECT updated_at_utc, status FROM shadow_briefing_runs WHERE run_id=?",
                ("run-A",),
            ).fetchone()
            after_events = conn.execute(
                "SELECT payload_json FROM shadow_briefing_run_events WHERE run_id=? ORDER BY event_ordinal",
                ("run-A",),
            ).fetchall()
            self.assertEqual(after_runs, snapshot_runs)
            self.assertEqual(after_events, snapshot_events)
        finally:
            conn.close()

    # ---- 49 ------------------------------------------------------------

    def test_complete_run_conflicting_retry_rejected(self) -> None:
        # 49: any payload / id / order difference raised against this
        # run's persisted run-events raises RunConflictError, including
        # an ALREADY_SEEN event whose payload differs from the same
        # run's run_events row (blocker #3).
        def _event(cid: str, payload: dict) -> briefing_ledger.ShadowEvent:
            return briefing_ledger.ShadowEvent(
                candidate_id=cid,
                category=Category(payload["category"]),
                decision=SemanticDecision(payload["decision"]),
                payload_json=_payload_bytes(payload),
                recorded_at_utc="2026-07-02T07:00:00Z",
            )

        def _seed_run_A():
            conn, ledger = _new_in_memory_ledger()
            ledger.initialize_schema()
            ledger.begin_run(
                "run-A",
                "2026-07-01T07:00:00Z",
                "2026-07-02T07:00:00Z",
                "2026-07-02T06:59:58Z",
            )
            payload_a1 = _payload_dict("cand-1")
            payload_a2 = _payload_dict("cand-2")
            ledger.complete_run(
                "run-A",
                "2026-07-02T07:00:05Z",
                [_event("cand-1", payload_a1), _event("cand-2", payload_a2)],
            )
            return conn, ledger, payload_a1, payload_a2

        # (a) Reorder the same candidate IDs at different input
        # positions: retry's input position 0 carries cand-2's payload,
        # position 1 carries cand-1's payload. The supplied-vs-
        # persisted-by-ordinal pairing disagrees at every position.
        conn, ledger, _payload_a1, _payload_a2 = _seed_run_A()
        try:
            swap_left = _payload_dict("cand-2")
            swap_right = _payload_dict("cand-1")
            with self.assertRaises(briefing_ledger.RunConflictError):
                ledger.complete_run(
                    "run-A",
                    "2026-07-02T07:00:05Z",
                    [_event("cand-2", swap_left), _event("cand-1", swap_right)],
                )
        finally:
            conn.close()

        # (b) Same IDs/order, but cand-1's payload bytes differ from
        # the persisted run_events.payload_json for ord=0.
        conn, ledger, payload_a1, payload_a2 = _seed_run_A()
        try:
            tampered = _payload_dict("cand-1")
            tampered["summary"] = "Tampered summary"
            with self.assertRaises(briefing_ledger.RunConflictError):
                ledger.complete_run(
                    "run-A",
                    "2026-07-02T07:00:05Z",
                    [_event("cand-1", tampered), _event("cand-2", payload_a2)],
                )
        finally:
            conn.close()

        # (c) Cross-run ALREADY_SEEN semantics (blocker #3):
        #   - run-A records payload A for ``shared``.
        #   - run-B completes ``shared`` with payload B (different from A)
        #     — must SUCCEED as ALREADY_SEEN, persisting B in run-B's
        #     run_events row.
        #   - A retry of run-B with payload C (different from B) must
        #     conflict against run-B's persisted run_events.
        conn, ledger = _new_in_memory_ledger()
        try:
            ledger.initialize_schema()
            ledger.begin_run(
                "run-A",
                "2026-07-01T07:00:00Z",
                "2026-07-02T07:00:00Z",
                "2026-07-02T06:59:58Z",
            )
            ledger.begin_run(
                "run-B",
                "2026-07-02T07:00:00Z",
                "2026-07-03T07:00:00Z",
                "2026-07-03T06:59:58Z",
            )
            payload_A = _payload_dict("shared", summary="First-seen payload")
            res_a = ledger.complete_run(
                "run-A",
                "2026-07-02T07:00:05Z",
                [_event("shared", payload_A)],
            )
            self.assertEqual(tuple(res_a.new_ids), ("shared",))

            # run-B with payload B (different from A) succeeds as ALREADY_SEEN.
            payload_B = _payload_dict("shared", summary="Cross-run payload B")
            res_b = ledger.complete_run(
                "run-B",
                "2026-07-03T07:00:05Z",
                [_event("shared", payload_B)],
            )
            self.assertEqual(tuple(res_b.new_ids), ())
            self.assertEqual(tuple(res_b.already_seen_ids), ("shared",))

            # run-B's run_events.payload_json is B; first-seen is still A.
            run_b_pl = conn.execute(
                "SELECT payload_json FROM shadow_briefing_run_events "
                "WHERE run_id='run-B' AND candidate_id='shared'"
            ).fetchone()[0]
            self.assertEqual(
                run_b_pl,
                _payload_bytes(payload_B).decode("utf-8"),
            )
            seen_pl = conn.execute(
                "SELECT payload_json FROM shadow_briefing_seen "
                "WHERE candidate_id='shared'"
            ).fetchone()[0]
            self.assertEqual(
                seen_pl,
                _payload_bytes(payload_A).decode("utf-8"),
            )

            # Retry of completed run-B with payload C conflicts because
            # the supplied event payload differs from run-B's
            # persisted run_events row.
            payload_C = _payload_dict("shared", summary="Retry payload C")
            with self.assertRaises(briefing_ledger.RunConflictError):
                ledger.complete_run(
                    "run-B",
                    "2026-07-03T07:00:05Z",
                    [_event("shared", payload_C)],
                )
        finally:
            conn.close()

    # ---- 50 ------------------------------------------------------------

    def test_fail_run_inserts_no_seen_events(self) -> None:
        # 50: fail_run marks RUNNING as FAILED and inserts no rows in
        # seen / run_events. Blockers: error_code validated against
        # conservative [A-Z0-9_.-]{1,128}; bound run_id 1..256; updated
        # timestamp normalized to Z.
        conn, ledger = _new_in_memory_ledger()
        try:
            ledger.initialize_schema()
            ledger.begin_run(
                "run-A",
                "2026-07-01T07:00:00Z",
                "2026-07-02T07:00:00Z",
                "2026-07-02T06:59:58Z",
            )
            before_seen = conn.execute(
                "SELECT COUNT(*) FROM shadow_briefing_seen"
            ).fetchone()[0]
            before_ev = conn.execute(
                "SELECT COUNT(*) FROM shadow_briefing_run_events"
            ).fetchone()[0]
            ledger.fail_run("run-A", "2026-07-02T07:00:00Z", "E_TRANSPORT")
            run_row = conn.execute(
                "SELECT status, error_code FROM shadow_briefing_runs WHERE run_id=?",
                ("run-A",),
            ).fetchone()
            self.assertEqual(run_row[0], "FAILED")
            self.assertEqual(run_row[1], "E_TRANSPORT")
            after_seen = conn.execute(
                "SELECT COUNT(*) FROM shadow_briefing_seen"
            ).fetchone()[0]
            after_ev = conn.execute(
                "SELECT COUNT(*) FROM shadow_briefing_run_events"
            ).fetchone()[0]
            self.assertEqual(before_seen, after_seen)
            self.assertEqual(before_ev, after_ev)

            # Already-completed runs cannot be marked FAILED.
            ledger.begin_run(
                "run-B",
                "2026-07-02T07:00:00Z",
                "2026-07-03T07:00:00Z",
                "2026-07-03T06:59:58Z",
            )
            payload = _payload_dict("cand-1")
            events = [_make_event(p["candidate_id"], p) for p in [payload]]
            ledger.complete_run("run-B", "2026-07-03T07:00:05Z", events)
            with self.assertRaises(
                (
                    briefing_ledger.RunStateError,
                    briefing_ledger.RunConflictError,
                    briefing_ledger.LedgerContractError,
                )
            ):
                ledger.fail_run("run-B", "2026-07-03T07:01:00Z", "E_LATE")

            # Subcase: error_code rejects non-conforming characters.
            for bad_code in (
                "",                    # empty
                "x" * 129,             # over 128
                "E TRANSPORT",         # space
                "E/TRANSPORT",         # slash forbidden
                "E:TRANSPORT",         # colon forbidden
                "elowercase",          # lowercase forbidden
            ):
                with self.assertRaises(briefing_ledger.LedgerContractError):
                    ledger.fail_run("run-A", "2026-07-02T07:00:00Z", bad_code)
            for bad_code in ("", "x" * 257):
                with self.assertRaises(briefing_ledger.LedgerContractError):
                    ledger.fail_run(
                        bad_code, "2026-07-02T07:00:00Z", "E_TRANSPORT"
                    )
        finally:
            conn.close()

    # ---- 51 ------------------------------------------------------------

    def test_recover_stale_updates_only_old_running_rows(self) -> None:
        # 51: recover_stale_runs flips only RUNNING rows with
        # updated_at_utc < cutoff to STALE; ordered by run_id. The cutoff
        # is an aware zero-offset datetime that is normalized before
        # SQL comparison (blocker #4) — never lexical string compare
        # against a non-Z offset. updated_at_utc may be supplied as
        # +00:00 and is normalized to Z.
        conn, ledger = _new_in_memory_ledger()
        try:
            ledger.initialize_schema()
            ledger.begin_run("run-A", "2026-07-01T07:00:00Z", "2026-07-02T07:00:00Z", "2026-06-30T07:00:00Z")
            ledger.begin_run("run-B", "2026-07-02T07:00:00Z", "2026-07-03T07:00:00Z", "2026-07-02T07:30:00Z")
            ledger.begin_run("run-C", "2026-07-03T07:00:00Z", "2026-07-04T07:00:00Z", "2026-07-04T07:30:00Z")
            # Complete one; it must stay COMPLETED (not RUNNING) so it is
            # never recovered.
            payload = _payload_dict("cand-1")
            events = [_make_event(p["candidate_id"], p) for p in [payload]]
            ledger.begin_run("run-D", "2026-07-04T07:00:00Z", "2026-07-05T07:00:00Z", "2026-07-04T07:30:00Z")
            ledger.complete_run("run-D", "2026-07-05T07:00:05Z", events)

            cutoff = datetime(2026, 7, 3, 0, 0, 0, tzinfo=timezone.utc)
            recovered = ledger.recover_stale_runs(
                cutoff,
                "2026-07-05T08:00:00Z",
            )
            self.assertEqual(recovered, ("run-A", "run-B"))

            rows = conn.execute(
                "SELECT run_id, status FROM shadow_briefing_runs ORDER BY run_id"
            ).fetchall()
            self.assertEqual(
                rows,
                [
                    ("run-A", "STALE"),
                    ("run-B", "STALE"),
                    ("run-C", "RUNNING"),
                    ("run-D", "COMPLETED"),
                ],
            )

            # Caller timestamp was applied to the recovered rows (normalized Z).
            self.assertEqual(
                conn.execute(
                    "SELECT updated_at_utc FROM shadow_briefing_runs WHERE run_id='run-A'"
                ).fetchone()[0],
                "2026-07-05T08:00:00Z",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT updated_at_utc FROM shadow_briefing_runs WHERE run_id='run-B'"
                ).fetchone()[0],
                "2026-07-05T08:00:00Z",
            )

            # Subcases (blocker #4):
            #   - naive cutoff rejected.
            #   - aware non-zero-offset cutoff rejected.
            #   - updated in +00:00 form normalized to Z.
            with self.assertRaises(briefing_ledger.LedgerContractError):
                ledger.recover_stale_runs(
                    datetime(2026, 7, 3, 0, 0, 0),
                    "2026-07-05T08:00:00Z",
                )
            with self.assertRaises(briefing_ledger.LedgerContractError):
                ledger.recover_stale_runs(
                    datetime(2026, 7, 3, 0, 0, 0, tzinfo=timezone(timedelta(hours=1))),
                    "2026-07-05T08:00:00Z",
                )

            # updated_at may be supplied in +00:00 form; normalized Z.
            cutoff2 = datetime(2026, 7, 6, 0, 0, 0, tzinfo=timezone.utc)
            ledger.recover_stale_runs(
                cutoff2,
                "2026-07-06T08:00:00+00:00",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT updated_at_utc FROM shadow_briefing_runs WHERE run_id='run-C'"
                ).fetchone()[0],
                "2026-07-06T08:00:00Z",
            )

            # Persisted timestamps are whole-second Z values. Strict
            # updated_at < cutoff semantics exclude equality at both whole-
            # second and fractional cutoff values.
            ledger.begin_run(
                "run-E",
                "2026-07-06T07:00:00Z",
                "2026-07-07T07:00:00Z",
                "2026-07-06T12:00:00Z",
            )
            exact = ledger.recover_stale_runs(
                datetime(2026, 7, 6, 12, 0, 0, tzinfo=timezone.utc),
                "2026-07-06T12:00:01Z",
            )
            self.assertNotIn("run-E", exact)
            fractional = ledger.recover_stale_runs(
                datetime(
                    2026, 7, 6, 12, 0, 0, 500000,
                    tzinfo=timezone.utc,
                ),
                "2026-07-06T12:00:02Z",
            )
            self.assertNotIn("run-E", fractional)
        finally:
            conn.close()

    # ---- 52 ------------------------------------------------------------

    def test_last_completed_upper_utc_returns_latest(self) -> None:
        # 52: last_completed_upper_utc returns the latest of the
        # COMPLETED run rows' window_upper_utc parsed as aware UTC;
        # None when no COMPLETED rows exist.

        conn, ledger = _new_in_memory_ledger()
        try:
            ledger.initialize_schema()
            self.assertIsNone(ledger.last_completed_upper_utc())

            payload = _payload_dict("cand-1")
            events = [_make_event(p["candidate_id"], p) for p in [payload]]

            # Complete runs in non-monotonic order.
            ledger.begin_run(
                "run-A",
                "2026-07-01T07:00:00Z",
                "2026-07-02T07:00:00Z",
                "2026-07-02T06:59:58Z",
            )
            ledger.complete_run("run-A", "2026-07-02T07:00:05Z", events)

            ledger.begin_run(
                "run-B",
                "2026-07-02T07:00:00Z",
                "2026-07-03T07:00:00Z",
                "2026-07-03T06:59:58Z",
            )
            events_b = [_make_event("cand-2", _payload_dict("cand-2"))]
            ledger.complete_run("run-B", "2026-07-03T07:00:05Z", events_b)

            latest = ledger.last_completed_upper_utc()
            self.assertIsNotNone(latest)
            self.assertEqual(latest.tzinfo, timezone.utc)
            self.assertEqual(latest.utcoffset(), timedelta(0))
            self.assertEqual(
                latest,
                datetime(2026, 7, 3, 7, 0, 0, tzinfo=timezone.utc),
            )

            # Insert a STALE row older than the latest; it must NOT be
            # returned because it is not COMPLETED.
            conn.execute(
                "INSERT INTO shadow_briefing_runs(run_id, window_lower_utc, window_upper_utc, "
                "started_at_utc, updated_at_utc, status) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "run-X",
                    "2026-06-30T07:00:00Z",
                    "2026-07-30T07:00:00Z",
                    "2026-06-30T07:00:00Z",
                    "2026-06-30T07:00:00Z",
                    "STALE",
                ),
            )
            self.assertEqual(
                ledger.last_completed_upper_utc(),
                datetime(2026, 7, 3, 7, 0, 0, tzinfo=timezone.utc),
            )
        finally:
            conn.close()

    # ---- 53 ------------------------------------------------------------

    def test_payload_rejects_noncanonical_oversize_or_sensitive_keys(self) -> None:
        # 53: payload canonicalization rejects non-JSON-object / NaN /
        # Infinity / duplicate keys / oversize / sensitive nested keys
        # without leaking the rejected value, length, or hash.
        # Different second-run payload succeeds while first-seen payload
        # remains unchanged and run-B payload is persisted separately
        # (blocker #10); covered structurally as a subcase at the bottom.

        # (a) Oversize: 16385 UTF-8 bytes.
        big_title = "T" * 16385
        payload = _payload_dict("cand-1", title=big_title)
        with self.assertRaises(briefing_ledger.PayloadValidationError):
            briefing_ledger.normalize_canonical_payload(payload)

        # (b) Nonfinite: NaN/Infinity literals are rejected by the
        # addendum.
        for bad in (float("nan"), float("inf"), -float("inf")):
            payload = _payload_dict("cand-1")
            payload["score"] = bad
            with self.assertRaises(briefing_ledger.PayloadValidationError):
                briefing_ledger.normalize_canonical_payload(payload)

        # (c) Duplicate keys: only possible if we hand-build JSON,
        # because ``dict`` cannot hold duplicates. The addendum says
        # "no duplicate/nonfinite keys"; the normalizer must reject
        # JSON text containing duplicate keys at any depth.
        dup_text = '{"candidate_id":"cand-1","candidate_id":"cand-1","extra":1}'
        with self.assertRaises(briefing_ledger.PayloadValidationError):
            briefing_ledger.normalize_canonical_payload(dup_text)

        # (d) Sensitive keys at any depth are rejected and never
        # echoed. Test top-level, nested dict, dict-in-list, and
        # case-fold variants.
        sensitive_payloads = [
            {"api_token": "secret-value"},
            {"authorization": "Bearer secret-value"},
            {"PASSWORD": "secret-value"},
            {"top": {"inner": {"Token": "secret-value"}}},
            {"items": [{"authorization": "secret-value"}]},
            {"values": [{"nested": {"secrets": "secret-value"}}]},
        ]
        for payload in sensitive_payloads:
            with self.assertRaises(briefing_ledger.PayloadValidationError) as ctx:
                briefing_ledger.normalize_canonical_payload(payload)
            msg = str(ctx.exception)
            # Reject value, length, hash must not leak.
            for leaked in (
                "secret-value",
                "28",
            ):
                self.assertNotIn(leaked, msg)
            # Length-prefixed hash prefix is also forbidden.
            self.assertNotRegex(msg, r"len=\d+")
            self.assertNotRegex(msg, r"sha256[:=]")

        # (e) Canonical byte-equivalent re-serialization: passing a
        # canonical dict, a dict reordered, and pre-serialized JSON all
        # yield the exact same payload bytes.
        canonical = {
            "candidate_id": "cand-1",
            "category": Category.AI.value,
            "decision": SemanticDecision.distinct_event.value,
            "title": "Headline",
            "summary": "Summary",
            "url": "https://example.test/article",
        }
        reordered = {
            "url": "https://example.test/article",
            "summary": "Summary",
            "decision": SemanticDecision.distinct_event.value,
            "title": "Headline",
            "category": Category.AI.value,
            "candidate_id": "cand-1",
        }
        bytes_a = briefing_ledger.normalize_canonical_payload(canonical)
        bytes_b = briefing_ledger.normalize_canonical_payload(reordered)
        self.assertEqual(bytes_a, bytes_b)

        # Canonical raw bytes/text round-trip byte-identically. Raw JSON
        # with key-order or whitespace differences is rejected rather
        # than silently rewritten.
        self.assertEqual(
            briefing_ledger.normalize_canonical_payload(bytes_a), bytes_a
        )
        self.assertEqual(
            briefing_ledger.normalize_canonical_payload(bytes_a.decode("utf-8")),
            bytes_a,
        )
        for noncanonical in (
            '{"b":1, "a":2}',
            '{ "a":2,"b":1}',
            json.dumps({"title": "café"}, ensure_ascii=True),
        ):
            with self.assertRaises(briefing_ledger.PayloadValidationError):
                briefing_ledger.normalize_canonical_payload(noncanonical)

        # Encoding must be UTF-8 with ``ensure_ascii=False`` semantics:
        # non-ASCII strings survive verbatim (no \uXXXX escapes).
        payload_unicode = _payload_dict("cand-1")
        payload_unicode["title"] = "café — naïve"
        bytes_unicode = briefing_ledger.normalize_canonical_payload(payload_unicode)
        self.assertIn("café — naïve".encode("utf-8"), bytes_unicode)
        self.assertNotIn(b"\\u00e9", bytes_unicode)

        # (f) Cross-run payload divergence stays separate (blocker #3,
        # #10): a different second-run canonical payload for the same
        # candidate_id succeeds; both payloads are stored in their own
        # run's run_events row.
        conn, ledger = _new_in_memory_ledger()
        try:
            ledger.initialize_schema()
            ledger.begin_run(
                "run-A", "2026-07-01T07:00:00Z", "2026-07-02T07:00:00Z",
                "2026-07-02T06:59:58Z",
            )
            p_first = _payload_dict("shared", summary="First summary")
            ledger.complete_run(
                "run-A", "2026-07-02T07:00:05Z", [_make_event("shared", p_first)]
            )

            ledger.begin_run(
                "run-B", "2026-07-02T07:00:00Z", "2026-07-03T07:00:00Z",
                "2026-07-03T06:59:58Z",
            )
            p_second = _payload_dict("shared", summary="Different second summary")
            ledger.complete_run(
                "run-B", "2026-07-03T07:00:05Z", [_make_event("shared", p_second)]
            )

            # First-seen payload unchanged; run-B carries payload B.
            seen = conn.execute(
                "SELECT payload_json FROM shadow_briefing_seen "
                "WHERE candidate_id='shared'"
            ).fetchone()[0]
            self.assertEqual(seen, _payload_bytes(p_first).decode("utf-8"))
            rb = conn.execute(
                "SELECT payload_json FROM shadow_briefing_run_events "
                "WHERE run_id='run-B' AND candidate_id='shared'"
            ).fetchone()[0]
            self.assertEqual(rb, _payload_bytes(p_second).decode("utf-8"))
        finally:
            conn.close()

        # (g) ShadowEvent validates exact non-empty string IDs without
        # C0/C1/bidi controls, decision in {distinct_event, material_update},
        # recorded_at normalized UTC via validate_utc_iso (blocker #1).
        e = briefing_ledger.ShadowEvent(
            candidate_id="cand-1",
            category=Category.AI,
            decision=SemanticDecision.distinct_event,
            payload_json=_payload_bytes(_payload_dict("cand-1")),
            recorded_at_utc="2026-07-02T07:00:00Z",
        )
        self.assertEqual(e.candidate_id, "cand-1")
        for bad in ("", "x" * 257, "has\ttab", "bell\x07", "bidi\u202E"):
            with self.assertRaises(briefing_ledger.LedgerContractError):
                briefing_ledger.ShadowEvent(
                    candidate_id=bad,
                    category=Category.AI,
                    decision=SemanticDecision.distinct_event,
                    payload_json=_payload_bytes(_payload_dict("cand-1")),
                    recorded_at_utc="2026-07-02T07:00:00Z",
                )
        for bad_decision in (
            SemanticDecision.rewrite,
            SemanticDecision.bypass_phase2_terminal,
            SemanticDecision.pending_review,
            SemanticDecision.pending_model_error,
        ):
            with self.assertRaises(briefing_ledger.LedgerContractError):
                briefing_ledger.ShadowEvent(
                    candidate_id="cand-1",
                    category=Category.AI,
                    decision=bad_decision,
                    payload_json=_payload_bytes(_payload_dict("cand-1")),
                    recorded_at_utc="2026-07-02T07:00:00Z",
                )

        # Bytearray payload rejected (immutable bytes required).
        with self.assertRaises(briefing_ledger.LedgerContractError):
            briefing_ledger.ShadowEvent(
                candidate_id="cand-1",
                category=Category.AI,
                decision=SemanticDecision.distinct_event,
                payload_json=bytearray(b'{"ok":1}'),
                recorded_at_utc="2026-07-02T07:00:00Z",
            )

    # ---- 54 ------------------------------------------------------------

    def test_lock_error_maps_and_transaction_rolls_back(self) -> None:
        # 54: a SQLite OperationalError "database is locked" is mapped to
        # LockTimeoutError, no partial rows are left in seen/run_events,
        # and the BEGIN IMMEDIATE contract is preserved. After a
        # LockTimeoutError raised inside an OperationalError-only path,
        # the swallowed rollback never hides the typed error (blocker
        # #6).

        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "locked.db")
            # Hold a writer transaction on the first connection.
            outer = sqlite3.connect(db_path, isolation_level=None, timeout=1.0)
            outer.execute("PRAGMA foreign_keys=OFF")
            outer.execute("BEGIN IMMEDIATE")
            outer.execute("CREATE TABLE x (y INTEGER)")
            try:
                # Schema initialization is itself a mutation using
                # BEGIN IMMEDIATE, so it must map this writer lock.
                inner = sqlite3.connect(db_path, isolation_level=None, timeout=1.0)
                inner.execute("PRAGMA foreign_keys=OFF")
                try:
                    ledger = briefing_ledger.BriefingLedger(inner)
                    with self.assertRaises(briefing_ledger.LockTimeoutError):
                        ledger.initialize_schema()
                finally:
                    inner.close()
            finally:
                outer.execute("ROLLBACK")
                outer.close()

        # Independently: confirm the lock-failure path leaves the ledger
        # state empty (no half-written seen rows) and that the typed
        # error is LockTimeoutError, not RunConflictError.
        conn, ledger = _new_in_memory_ledger()
        try:
            ledger.initialize_schema()
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM shadow_briefing_runs").fetchone()[0],
                0,
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM shadow_briefing_seen").fetchone()[0],
                0,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM shadow_briefing_run_events"
                ).fetchone()[0],
                0,
            )
        finally:
            conn.close()

    # ---- 55 ------------------------------------------------------------

    def test_integrity_error_rolls_back_without_partial_seen_rows(self) -> None:
        # 55: a complete_run that triggers an ABORT during per-row
        # processing must roll the BEGIN IMMEDIATE transaction back so
        # no partial ``seen`` or run_events rows persist. We attach an
        # ``AFTER INSERT`` trigger on shadow_briefing_run_events that
        # raises ABORT for the SECOND candidate insertion; the first
        # row's INSERT must still have committed to disk in autocommit
        # but the surrounding transaction is rolled back by the
        # OperationalError-typed path inside the ledger. Finally, we
        # drop the trigger and prove a clean retry succeeds.

        conn, ledger = _new_in_memory_ledger()
        try:
            ledger.initialize_schema()
            ledger.begin_run(
                "run-A",
                "2026-07-01T07:00:00Z",
                "2026-07-02T07:00:00Z",
                "2026-07-02T06:59:58Z",
            )

            # Subcase (a): pre-flight duplicate candidate_id raises
            # LedgerContractError without opening a writer transaction.
            payload_dup1 = _payload_dict("dup-1")
            payload_dup2 = _payload_dict("dup-1")
            payload_dup2["title"] = "Different title"
            events_dup = [
                _make_event("dup-1", payload_dup1),
                _make_event("dup-1", payload_dup2),
            ]
            with self.assertRaises(briefing_ledger.LedgerContractError):
                ledger.complete_run("run-A", "2026-07-02T07:00:10Z", events_dup)
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM shadow_briefing_seen"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM shadow_briefing_run_events"
                ).fetchone()[0],
                0,
            )

            # Subcase (b): install trigger that raises ABORT on the
            # SECOND candidate inserted into run_events. complete_run
            # walks its events list and writes one shadow_briefing_seen
            # + one shadow_briefing_run_events row per event; the
            # second iteration's run_events INSERT trips the trigger,
            # SQLite aborts the still-open transaction, the ledger's
            # OperationalError-to-LockTimeoutError mapping is tested
            # for the busy/lock case (here ABORT is sqlite3's
            # OperationalError subclass), and the entire transaction
            # rolls back: zero seen/run_events rows survive.
            conn.execute(
                "CREATE TRIGGER trg_abort_second_event "
                "AFTER INSERT ON shadow_briefing_run_events "
                "WHEN (SELECT COUNT(*) FROM shadow_briefing_run_events) >= 1 "
                "AND NEW.event_ordinal = 1 "
                "BEGIN SELECT RAISE(ABORT, 'PARENT_FAULT_INJECTION'); END"
            )
            payload_ok_1 = _payload_dict("cand-1")
            payload_ok_2 = _payload_dict("cand-2")
            with self.assertRaises(
                (briefing_ledger.LedgerContractError, briefing_ledger.RunConflictError)
            ):
                ledger.complete_run(
                    "run-A", "2026-07-02T07:00:05Z",
                    [_make_event("cand-1", payload_ok_1),
                     _make_event("cand-2", payload_ok_2)],
                )

            # Run-A is still RUNNING (transaction rolled back); ZERO
            # seen/run_events rows leaked.
            self.assertEqual(
                conn.execute(
                    "SELECT status FROM shadow_briefing_runs WHERE run_id='run-A'"
                ).fetchone()[0],
                "RUNNING",
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM shadow_briefing_seen").fetchone()[0],
                0,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM shadow_briefing_run_events"
                ).fetchone()[0],
                0,
            )

            # Subcase (c): drop trigger; clean retry commits.
            conn.execute("DROP TRIGGER trg_abort_second_event")
            result = ledger.complete_run(
                "run-A", "2026-07-02T07:00:05Z",
                [_make_event("cand-1", payload_ok_1),
                 _make_event("cand-2", payload_ok_2)],
            )
            self.assertEqual(tuple(result.new_ids), ("cand-1", "cand-2"))
            self.assertEqual(
                conn.execute(
                    "SELECT status FROM shadow_briefing_runs WHERE run_id='run-A'"
                ).fetchone()[0],
                "COMPLETED",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM shadow_briefing_seen WHERE candidate_id='cand-1'"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM shadow_briefing_seen WHERE candidate_id='cand-2'"
                ).fetchone()[0],
                1,
            )
        finally:
            conn.close()


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
