"""Phase 2 — public dry-run API.

The only public entry point is :func:`evaluate_candidates`. It returns a
tuple of :class:`FilterResult` values — no DB writes, no Markdown writes, no
telemetry, no network calls, no subprocesses, no current-time lookups.
"""
from __future__ import annotations

from typing import Mapping

from .contracts import CandidateArticle, FilterResult
from .filtering import evaluate_candidates as _evaluate_candidates
from .models import Category
from .policies import QueryPolicy, SourcePolicy


def evaluate_candidates(
    candidates: list[CandidateArticle],
    db_path: str,
    source_policy: SourcePolicy,
    query_policies: Mapping[Category, QueryPolicy],
) -> tuple[FilterResult, ...]:
    """Thin public wrapper that preserves the engine's pure contract."""
    return _evaluate_candidates(candidates, db_path, source_policy, query_policies)


__all__ = ["evaluate_candidates"]
