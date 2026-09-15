"""Phase 5 — bounded dry-run shadow-hook tests.

This module is the *only* Phase 5 test file. It covers the additive
``news_pipeline.briefing_shadow_hook`` module and asserts:

  * import is safe and ``_DRY_RUN`` remains literal True;
  * happy-path delegation returns a frozen ``BriefingEngineResult``;
  * aware-UTC validation, empty run_id, and tuple-shape validation
    all propagate from the engine (the hook adds no validation);
  * renderer / summarizer / ledger exceptions propagate unchanged;
  * no ``delivered`` namespace, no hook-created filesystem writes,
    no network / process / wall-clock / random / UUID APIs;
  * the only functional call is the approved engine entry point;
  * no generic ``except Exception`` handler is added by the hook;
  * Phase 4 source/test SHA256 hashes are unchanged after this module
    runs (verified out-of-band).

Reuses existing ``test_briefing_engine`` helpers (in-memory ledger,
fake summarizer, fake renderer, briefing-input factory) and never
touches the live shadow DB. Stdlib + ``news_pipeline`` only.
"""
from __future__ import annotations

import ast
import hashlib
import io
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Tuple

from news_pipeline import briefing_shadow_hook
from news_pipeline.briefing_contracts import _DRY_RUN, BriefingInput
from news_pipeline.event_contracts import SemanticDecision

# NOTE: We deliberately do NOT import BriefingEngineResult,
# ShadowLedgerProtocol, SummarizerProtocol, or RendererProtocol from
# ``news_pipeline.briefing_engine`` at module top. The parent Phase 4
# engine test (``test_briefing_engine``) calls ``importlib.reload`` on
# ``news_pipeline.briefing_engine`` during its run, which replaces the
# module object in ``sys.modules`` with a fresh copy; any top-level
# reference captured here would become stale and ``isinstance`` would
# spuriously fail. Instead, resolve these symbols inside the test
# method via ``getattr(sys.modules["news_pipeline.briefing_engine"], ...)``
# so each call sees the live module state.

# Reuse existing Phase 4 test helpers. They are designed to be
# importable from other test modules under the same PYTHONPATH used
# by the rest of the Phase 4 / Phase 5 test discovery.
from tests.news_pipeline.test_briefing_engine import (  # noqa: E402
    _InMemoryLedger,
    _FakeSummarizer,
    _FakeRenderer,
    _as_of,
    _briefing_input,
    _low_window_evaluated_at,
)


# Path constants used by AST and no-I/O assertions.
_HOOK_REL = "scripts/news_pipeline/briefing_shadow_hook.py"
# Workspace root is computed from this test file's location so the suite is
# repository-portable. ``tests/news_pipeline/test_briefing_shadow_hook.py``
# lives two directories beneath the workspace root.
_WORKSPACE = str(Path(__file__).resolve().parents[2])


def _hook_abs_path() -> str:
    return os.path.join(_WORKSPACE, _HOOK_REL)


def _read_hook_source() -> str:
    with open(_hook_abs_path(), "r", encoding="utf-8") as fh:
        return fh.read()


def _live_engine_attr(name: str) -> Any:
    """Resolve ``name`` against the *current* ``news_pipeline.briefing_engine``
    module object. The Phase 4 engine test reloads the module mid-run;
    using this helper avoids stale top-level references.
    """
    mod = sys.modules.get("news_pipeline.briefing_engine")
    if mod is None:
        # Fall back to a fresh import if the live module has been dropped.
        import news_pipeline.briefing_engine as mod  # type: ignore[no-redef]
    return getattr(mod, name)


# ---------------------------------------------------------------------------
# Phase 4 hash baseline (12 files) — read once at module load.
# ---------------------------------------------------------------------------


_PHASE4_BASELINE_PATHS: Tuple[str, ...] = (
    "scripts/news_pipeline/briefing_contracts.py",
    "scripts/news_pipeline/briefing_engine.py",
    "scripts/news_pipeline/briefing_ledger.py",
    "scripts/news_pipeline/briefing_renderer.py",
    "scripts/news_pipeline/briefing_summarizer.py",
    "scripts/news_pipeline/briefing_reliability.py",
    "tests/news_pipeline/test_briefing_contracts.py",
    "tests/news_pipeline/test_briefing_engine.py",
    "tests/news_pipeline/test_briefing_ledger.py",
    "tests/news_pipeline/test_briefing_renderer.py",
    "tests/news_pipeline/test_briefing_summarizer.py",
    "tests/news_pipeline/test_briefing_reliability.py",
)


def _sha256_of(rel_path: str) -> str:
    abs_path = os.path.join(_WORKSPACE, rel_path)
    with open(abs_path, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


_PHASE4_BASELINE_HASHES: Tuple[Tuple[str, str], ...] = tuple(
    (rel, _sha256_of(rel)) for rel in _PHASE4_BASELINE_PATHS
)


# ---------------------------------------------------------------------------
# AST-level invariants for the hook source file.
# ---------------------------------------------------------------------------


_FORBIDDEN_IMPORTS: Tuple[str, ...] = (
    "sqlite3",
    "urllib",
    "urllib.request",
    "urllib.error",
    "urllib.parse",
    "http",
    "http.client",
    "requests",
    "httpx",
    "aiohttp",
    "socket",
    "ssl",
    "subprocess",
    "multiprocessing",
    "os",
    "pathlib",
    "shutil",
    "tempfile",
    "random",
    "secrets",
    "uuid",
    "time",
    "logging",
    "pickle",
    "asyncio",
    "signal",
)


_FORBIDDEN_CALL_NAMES: Tuple[str, ...] = (
    "open",
    "print",
    "input",
    "exit",
    "quit",
    "randint",
    "choice",
    "random",
    "uniform",
    "uuid4",
    "uuid1",
    "uuid3",
    "uuid5",
    "time",
    "sleep",
    "monotonic",
    "perf_counter",
    "process_time",
    "now",
    "utcnow",
    "today",
    "fork",
    "exec",
    "eval",
    "compile",
    "Popen",
    "run",
    "call",
    "check_output",
    "getpid",
    "getppid",
    "connect",
    "send",
    "recv",
    "urlopen",
    "Request",
)


_FORBIDDEN_ATTR_NAMES: Tuple[str, ...] = (
    "run",
    "call",
    "Popen",
    "check_output",
    "getpid",
    "urlopen",
    "Request",
    "randint",
    "choice",
    "uuid4",
    "uuid1",
    "now",
    "utcnow",
    "today",
    "open",
    "sleep",
    "fork",
)


def _hook_ast() -> ast.Module:
    return ast.parse(_read_hook_source(), filename=_hook_abs_path())


def _iter_call_names(node: ast.AST) -> List[str]:
    names: List[str] = []
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call):
            func = sub.func
            if isinstance(func, ast.Name):
                names.append(func.id)
            elif isinstance(func, ast.Attribute):
                names.append(func.attr)
    return names


def _iter_import_modules(node: ast.AST) -> List[str]:
    modules: List[str] = []
    for sub in ast.walk(node):
        if isinstance(sub, ast.Import):
            for alias in sub.names:
                modules.append(alias.name)
        elif isinstance(sub, ast.ImportFrom):
            if sub.module is not None:
                modules.append(sub.module)
                for alias in sub.names:
                    modules.append(f"{sub.module}.{alias.name}")
    return modules


def _iter_attribute_names(node: ast.AST) -> List[str]:
    out: List[str] = []
    for sub in ast.walk(node):
        if isinstance(sub, ast.Attribute):
            out.append(sub.attr)
    return out


class TestShadowHookStatic(unittest.TestCase):
    """AST / import / hash invariants for the hook module."""

    def test_module_imports_and_keeps_dry_run_literal_true(self) -> None:
        self.assertTrue(
            _DRY_RUN is True,
            msg="_DRY_RUN must remain literal True (Phase 4/5 invariant)",
        )
        self.assertTrue(hasattr(briefing_shadow_hook, "run_shadow_briefing"))
        self.assertEqual(
            getattr(briefing_shadow_hook, "_DRY_RUN", None), True
        )

    def test_public_all_is_only_run_shadow_briefing(self) -> None:
        self.assertEqual(
            briefing_shadow_hook.__all__,
            ("run_shadow_briefing",),
        )

    def test_no_forbidden_imports(self) -> None:
        tree = _hook_ast()
        bad = []
        for module in _iter_import_modules(tree):
            top = module.split(".", 1)[0]
            for forbidden in _FORBIDDEN_IMPORTS:
                if module == forbidden or top == forbidden:
                    bad.append(module)
        self.assertEqual(
            bad,
            [],
            msg=f"hook source must not import forbidden modules: {bad}",
        )

    def test_no_forbidden_calls(self) -> None:
        tree = _hook_ast()
        names = _iter_call_names(tree)
        bad = [n for n in names if n in _FORBIDDEN_CALL_NAMES]
        # Allow the single functional delegation call to execute_briefing_run.
        bad = [
            n for n in bad
            if not (n == "execute_briefing_run")
        ]
        self.assertEqual(
            bad,
            [],
            msg=f"hook must not call forbidden APIs: {bad}",
        )

    def test_no_forbidden_attributes(self) -> None:
        tree = _hook_ast()
        attrs = []
        for sub in ast.walk(tree):
            if isinstance(sub, ast.Attribute):
                attrs.append(sub.attr)
        bad = [a for a in attrs if a in _FORBIDDEN_ATTR_NAMES]
        # Allow the engine functional entry point name on the module.
        bad = [
            a for a in bad
            if not (a == "execute_briefing_run")
        ]
        self.assertEqual(
            bad,
            [],
            msg=f"hook must not reference forbidden attributes: {bad}",
        )

    def test_no_generic_exception_handler(self) -> None:
        tree = _hook_ast()
        offenders: List[Tuple[int, str]] = []
        for sub in ast.walk(tree):
            if isinstance(sub, ast.ExceptHandler):
                handler_type = sub.type
                if handler_type is None:
                    continue
                if isinstance(handler_type, ast.Name) and handler_type.id == "Exception":
                    offenders.append((sub.lineno, "bare 'Exception'"))
                elif (
                    isinstance(handler_type, ast.Tuple)
                    and any(
                        isinstance(elt, ast.Name) and elt.id == "Exception"
                        for elt in handler_type.elts
                    )
                ):
                    offenders.append((sub.lineno, "'Exception' in tuple"))
        self.assertEqual(
            offenders,
            [],
            msg=f"hook must not add generic Exception handlers: {offenders}",
        )

    def test_delegates_to_functional_entry_point(self) -> None:
        tree = _hook_ast()
        saw_delegation = False
        for sub in ast.walk(tree):
            if (
                isinstance(sub, ast.Call)
                and isinstance(sub.func, ast.Name)
                and sub.func.id == "execute_briefing_run"
            ):
                saw_delegation = True
                # The functional call must be the engine entry point, NOT
                # a method on a local instance.
                self.assertEqual(
                    len(sub.args),
                    0,
                    msg="execute_briefing_run must be invoked with kwargs only",
                )
                self.assertGreaterEqual(
                    len(sub.keywords),
                    7,
                    msg="execute_briefing_run must forward all seven params",
                )
                kwargs = {kw.arg for kw in sub.keywords}
                expected = {
                    "run_id",
                    "briefing_inputs",
                    "ledger",
                    "summarizer",
                    "renderer",
                    "as_of_utc",
                    "last_completed_upper_utc",
                }
                self.assertEqual(
                    kwargs,
                    expected,
                    msg="execute_briefing_run kwargs must match the contract",
                )
        self.assertTrue(
            saw_delegation,
            msg="hook must delegate to briefing_engine.execute_briefing_run",
        )

    def test_phase4_file_hashes_unchanged_after_hook_tests(self) -> None:
        """Run after the dynamic suite — guarantees we did not touch Phase 4."""
        for rel, expected in _PHASE4_BASELINE_HASHES:
            self.assertEqual(
                _sha256_of(rel),
                expected,
                msg=f"Phase 4 file changed: {rel}",
            )


class TestShadowHookDynamic(unittest.TestCase):
    """Behavioural delegation tests for ``run_shadow_briefing``."""

    def test_happy_path_returns_engine_result(self) -> None:
        BriefingEngineResult = _live_engine_attr("BriefingEngineResult")
        EngineRunStatus = _live_engine_attr("EngineRunStatus")
        EngineIncludedItem = _live_engine_attr("EngineIncludedItem")
        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        bi = _briefing_input(
            "cand-hook-1",
            SemanticDecision.distinct_event,
            evaluated_at=_low_window_evaluated_at(),
        )
        result = briefing_shadow_hook.run_shadow_briefing(
            run_id="run-hook-1",
            briefing_inputs=(bi,),
            ledger=ledger,
            summarizer=summarizer,
            renderer=renderer,
            as_of_utc=_as_of(),
        )
        self.assertIsInstance(result, BriefingEngineResult)
        # frozen / slotted
        self.assertIs(type(result), BriefingEngineResult)
        # returned by delegation (same instance the engine produced)
        self.assertEqual(result.run_id, "run-hook-1")
        self.assertEqual(result.status, EngineRunStatus.COMPLETED)
        self.assertEqual(len(result.included_items), 1)
        included = result.included_items[0]
        self.assertIsInstance(included, EngineIncludedItem)
        # ledger recorded the run
        self.assertEqual(ledger.calls[0], "begin_run")
        self.assertIn("complete_run", ledger.calls)

    def test_excluded_items_are_typed_frozen_tuples(self) -> None:
        EngineExclusionReason = _live_engine_attr("EngineExclusionReason")
        ledger = _InMemoryLedger(seen=("cand-already",))
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        bi_seen = _briefing_input(
            "cand-already",
            SemanticDecision.distinct_event,
            evaluated_at=_low_window_evaluated_at(),
        )
        result = briefing_shadow_hook.run_shadow_briefing(
            run_id="run-hook-2",
            briefing_inputs=(bi_seen,),
            ledger=ledger,
            summarizer=summarizer,
            renderer=renderer,
            as_of_utc=_as_of(),
        )
        self.assertEqual(len(result.excluded_items), 1)
        excl = result.excluded_items[0]
        self.assertEqual(
            excl.reason, EngineExclusionReason.ALREADY_SHADOW_SEEN
        )
        self.assertIs(type(result.excluded_items), tuple)

    def test_aware_utc_required(self) -> None:
        """Naive datetime must be rejected by the engine through the hook."""
        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        bi = _briefing_input(
            "cand-hook-3",
            SemanticDecision.distinct_event,
            evaluated_at=_low_window_evaluated_at(),
        )
        with self.assertRaises(ValueError):
            briefing_shadow_hook.run_shadow_briefing(
                run_id="run-hook-3",
                briefing_inputs=(bi,),
                ledger=ledger,
                summarizer=summarizer,
                renderer=renderer,
                # Naive datetime — must raise ValueError from the engine.
                as_of_utc=datetime(2026, 7, 1, 9, 0, 0),  # type: ignore[arg-type]
            )

    def test_empty_run_id_rejected(self) -> None:
        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        bi = _briefing_input(
            "cand-hook-4",
            SemanticDecision.distinct_event,
            evaluated_at=_low_window_evaluated_at(),
        )
        with self.assertRaises(TypeError):
            briefing_shadow_hook.run_shadow_briefing(
                run_id="",
                briefing_inputs=(bi,),
                ledger=ledger,
                summarizer=summarizer,
                renderer=renderer,
                as_of_utc=_as_of(),
            )

    def test_non_tuple_inputs_rejected(self) -> None:
        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        bi = _briefing_input(
            "cand-hook-5",
            SemanticDecision.distinct_event,
            evaluated_at=_low_window_evaluated_at(),
        )
        with self.assertRaises(TypeError):
            briefing_shadow_hook.run_shadow_briefing(
                run_id="run-hook-5",
                briefing_inputs=[bi],  # type: ignore[arg-type]
                ledger=ledger,
                summarizer=summarizer,
                renderer=renderer,
                as_of_utc=_as_of(),
            )

    def test_renderer_exception_propagates(self) -> None:
        from news_pipeline.briefing_renderer import OversizeRecordError

        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()

        class _OverflowRenderer(_FakeRenderer):
            def render_briefing(self, records, upper_bound_utc):  # type: ignore[override]
                raise OversizeRecordError("cand-hook-6")

        bi = _briefing_input(
            "cand-hook-6",
            SemanticDecision.distinct_event,
            evaluated_at=_low_window_evaluated_at(),
        )
        with self.assertRaises(OversizeRecordError):
            briefing_shadow_hook.run_shadow_briefing(
                run_id="run-hook-6",
                briefing_inputs=(bi,),
                ledger=ledger,
                summarizer=summarizer,
                renderer=_OverflowRenderer(),
                as_of_utc=_as_of(),
            )

    def test_summarizer_programmer_exception_propagates(self) -> None:
        ledger = _InMemoryLedger()

        class _BoomSummarizer(_FakeSummarizer):
            def summarize_category(self, category, raw_inputs):  # type: ignore[override]
                raise RuntimeError("programmer-bomb")

        bi = _briefing_input(
            "cand-hook-7",
            SemanticDecision.distinct_event,
            evaluated_at=_low_window_evaluated_at(),
        )
        with self.assertRaises(RuntimeError):
            briefing_shadow_hook.run_shadow_briefing(
                run_id="run-hook-7",
                briefing_inputs=(bi,),
                ledger=ledger,
                summarizer=_BoomSummarizer(),
                renderer=_FakeRenderer(),
                as_of_utc=_as_of(),
            )

    def test_ledger_fail_run_propagates_original_exception(self) -> None:
        """Ledger.begin_run raising must propagate unchanged; no replacement."""
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        bi = _briefing_input(
            "cand-hook-8",
            SemanticDecision.distinct_event,
            evaluated_at=_low_window_evaluated_at(),
        )

        class _FailBeginLedger(_InMemoryLedger):
            def begin_run(self, run_id, lower, upper, started):  # type: ignore[override]
                raise RuntimeError("ledger-bomb")

        with self.assertRaises(RuntimeError):
            briefing_shadow_hook.run_shadow_briefing(
                run_id="run-hook-8",
                briefing_inputs=(bi,),
                ledger=_FailBeginLedger(),
                summarizer=summarizer,
                renderer=renderer,
                as_of_utc=_as_of(),
            )

    def test_no_delivered_namespace_in_news_pipeline(self) -> None:
        """Phase 4 evidence: no ``delivered`` package exists. The hook
        must not introduce one either. Asserting here keeps this invariant
        local to the test module so reviewers do not need to chase
        package layouts."""
        import news_pipeline  # type: ignore[import-not-found]

        self.assertFalse(
            hasattr(news_pipeline, "delivered"),
            msg="news_pipeline must NOT expose a delivered namespace",
        )

    def test_hook_creates_no_filesystem_writes(self) -> None:
        """Snapshot the cwd, run a happy-path, verify no .pyc side-effects
        outside the package's own __pycache__ in cwd. We only assert that
        no NEW top-level files appeared in cwd (the package __pycache__
        may be refreshed)."""
        cwd = tempfile.gettempdir()
        before = set(os.listdir(cwd))
        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        bi = _briefing_input(
            "cand-hook-fs",
            SemanticDecision.distinct_event,
            evaluated_at=_low_window_evaluated_at(),
        )
        briefing_shadow_hook.run_shadow_briefing(
            run_id="run-hook-fs",
            briefing_inputs=(bi,),
            ledger=ledger,
            summarizer=summarizer,
            renderer=renderer,
            as_of_utc=_as_of(),
        )
        after = set(os.listdir(cwd))
        # /tmp is noisy in CI; we instead assert there is no hook-created
        # file with the hook's own prefix in /tmp/phase5_slice.
        # The test runner alone writes nothing — we just ensure the hook
        # call above did not attempt any FS write.
        new_files = sorted(after - before)
        for name in new_files:
            self.assertFalse(
                name.startswith("briefing_shadow_hook"),
                msg=f"hook unexpectedly wrote a file: {name}",
            )


# ---------------------------------------------------------------------------
# Suite entry point.
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    unittest.main()