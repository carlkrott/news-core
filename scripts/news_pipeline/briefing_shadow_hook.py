"""Phase 5 — bounded dry-run shadow-hook for the briefing engine.

This module is a *pure delegation* entry point. It exists only to give
Phase 5 callers a single named import that routes through the approved
Phase 4 functional engine entry point. It performs no SQL, no path
I/O, no network or model calls, no logging, no output persistence,
no clock / random / UUID / process APIs, and no validation of its own.

The caller owns:

  * the lifetime and connectivity of ``ledger`` (a
    ``ShadowLedgerProtocol``);
  * the deterministic ``run_id`` and aware-UTC ``as_of_utc`` /
    ``last_completed_upper_utc`` lifecycle timestamps;
  * the summarizer and renderer sessions (and any side effects they
    may carry — this hook does not touch them beyond forwarding).

The hook is intentionally minimal so future Phase 5 wiring (cron,
wrapper, persistent DB path, delivery, scheduling) can sit on top of
this entry point without re-implementing engine semantics.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Tuple

from .briefing_contracts import _DRY_RUN, BriefingInput
from .briefing_engine import (
    BriefingEngineResult,
    RendererProtocol,
    ShadowLedgerProtocol,
    SummarizerProtocol,
    execute_briefing_run,
)


__all__ = ("run_shadow_briefing",)


# Phase 4 / Phase 5 invariant: this flag must remain literal True.
# Re-asserting here keeps the constraint visible at the hook boundary.
if _DRY_RUN is not True:
    raise RuntimeError(
        "briefing_shadow_hook requires _DRY_RUN literal True (Phase 5 invariant)"
    )


def run_shadow_briefing(
    *,
    run_id: str,
    briefing_inputs: Tuple[BriefingInput, ...],
    ledger: ShadowLedgerProtocol,
    summarizer: SummarizerProtocol,
    renderer: RendererProtocol,
    as_of_utc: datetime,
    last_completed_upper_utc: Optional[datetime] = None,
) -> BriefingEngineResult:
    """Delegate one dry-run briefing to the approved engine entry point.

    Pure forwarder. No validation, no I/O, no exception handling. All
    behaviour is inherited from ``briefing_engine.execute_briefing_run``.
    """
    return execute_briefing_run(
        run_id=run_id,
        briefing_inputs=briefing_inputs,
        ledger=ledger,
        summarizer=summarizer,
        renderer=renderer,
        as_of_utc=as_of_utc,
        last_completed_upper_utc=last_completed_upper_utc,
    )