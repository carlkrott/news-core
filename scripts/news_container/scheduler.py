"""Deterministic scheduler that turns TOML schedules into enqueued tasks.

The scheduler is split into a *pure* layer (no I/O, no wall clock,
deterministic from inputs) and a thin *driver* that talks to the
control store.

Pure functions
--------------

``compute_due_slots(now_utc, schedule_blocks, *, horizon_minutes)``
returns a list of ``(kind, due_slot_utc_z)`` pairs whose Europe/London
local due time is on or before ``now_utc`` plus the requested horizon.
The function is **pure** — it never reads the wall clock, never
touches the network, and never imports anything that does.

``parse_schedule_toml(text)`` parses the TOML representation of the
runtime schedule (see ``config/runtime-schedule.example.toml``) into
a list of ``ScheduleBlock`` namedtuples.

Driver
------

``enqueue_due_slots(connection, schedule_blocks, *, now_utc,
horizon_minutes)`` is the only function that combines the two — it
calls the pure slot computation and enqueues each result through
``control_store.enqueue``.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import tomllib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .control_store import (
    ControlStoreError,
    DeliveryForbiddenError,
    enqueue,
    validate_kind,
    validate_payload,
)
from . import ALLOWED_KINDS

# The scheduler's only supported local timezone. Anything else is
# rejected with ScheduleError so a misconfigured cannot silently
# schedule against the wrong clock.
SCHEDULE_TZ_NAME = "Europe/London"
SCHEDULE_TZ = ZoneInfo(SCHEDULE_TZ_NAME)


class ScheduleError(Exception):
    """Misconfiguration in the runtime schedule."""


@dataclass(frozen=True)
class ScheduleBlock:
    """A single ``[schedule.<id>]`` block parsed from the schedule TOML."""

    block_id: str
    kind: str
    local_hours: frozenset[int]
    local_minutes: frozenset[int]
    weekdays: frozenset[int]  # 0=Mon..6=Sun, ISO weekday
    enabled: bool
    notes: str
    payload: dict[str, Any]


def parse_schedule_toml(text: str) -> list[ScheduleBlock]:
    """Parse a runtime schedule TOML document.

    Expected structure::

        [schedule.<id>]
        kind = "ingest"
        due_local = "06:30"     # Europe/London local time
        weekdays = ["mon","tue","wed","thu","fri"]
        enabled = true
        notes = "morning ingest window"
    """
    raw = tomllib.loads(text)
    schedule_table = raw.get("schedule")
    if not isinstance(schedule_table, dict) or not schedule_table:
        raise ScheduleError("runtime-schedule.toml must define at least one [schedule.*] block")

    blocks: list[ScheduleBlock] = []
    for block_id, body in schedule_table.items():
        if not isinstance(body, dict):
            raise ScheduleError(f"schedule.{block_id} must be a TOML table")
        kind = body.get("kind")
        if not isinstance(kind, str):
            raise ScheduleError(f"schedule.{block_id}.kind must be a string")
        try:
            validate_kind(kind)
        except ControlStoreError as exc:
            raise ScheduleError(str(exc)) from exc

        due_local = body.get("due_local")
        has_sets = "hours" in body or "minutes" in body
        if due_local is not None and has_sets:
            raise ScheduleError(
                f"schedule.{block_id} must use due_local or hours/minutes, not both"
            )
        if due_local is not None:
            if not isinstance(due_local, str):
                raise ScheduleError(f"schedule.{block_id}.due_local must be a string like '06:30'")
            try:
                local_hour, local_minute = (int(part) for part in due_local.split(":", 1))
            except (ValueError, TypeError) as exc:
                raise ScheduleError(
                    f"schedule.{block_id}.due_local must be HH:MM, got {due_local!r}"
                ) from exc
            if not (0 <= local_hour <= 23 and 0 <= local_minute <= 59):
                raise ScheduleError(
                    f"schedule.{block_id}.due_local out of range: {due_local!r}"
                )
            local_hours = frozenset({local_hour})
            local_minutes = frozenset({local_minute})
        elif has_sets:
            local_hours = _parse_clock_set(block_id, "hours", body.get("hours"), 23)
            local_minutes = _parse_clock_set(block_id, "minutes", body.get("minutes"), 59)
        else:
            raise ScheduleError(
                f"schedule.{block_id} must define due_local or both hours and minutes"
            )

        weekdays_raw = body.get("weekdays", ["mon", "tue", "wed", "thu", "fri", "sat", "sun"])
        weekdays = _parse_weekdays(block_id, weekdays_raw)

        enabled = bool(body.get("enabled", True))
        notes = str(body.get("notes", ""))
        payload_raw = body.get("payload", {})
        if not isinstance(payload_raw, dict):
            raise ScheduleError(f"schedule.{block_id}.payload must be a TOML table")
        try:
            payload = validate_payload(dict(payload_raw))
        except ControlStoreError as exc:
            raise ScheduleError(str(exc)) from exc

        blocks.append(
            ScheduleBlock(
                block_id=block_id,
                kind=kind,
                local_hours=local_hours,
                local_minutes=local_minutes,
                weekdays=weekdays,
                enabled=enabled,
                notes=notes,
                payload=payload,
            )
        )

    return blocks


_WEEKDAY_NAMES = {
    "mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6,
}


def _parse_clock_set(block_id: str, field: str, raw: Any, upper: int) -> frozenset[int]:
    if raw == "all":
        return frozenset(range(upper + 1))
    if not isinstance(raw, list) or not raw:
        raise ScheduleError(
            f"schedule.{block_id}.{field} must be 'all' or a non-empty integer list"
        )
    if any(type(value) is not int or not 0 <= value <= upper for value in raw):
        raise ScheduleError(
            f"schedule.{block_id}.{field} values must be integers from 0 through {upper}"
        )
    return frozenset(raw)


def _parse_weekdays(block_id: str, raw: Any) -> frozenset[int]:
    if not isinstance(raw, list) or not raw:
        raise ScheduleError(f"schedule.{block_id}.weekdays must be a non-empty list")
    out: set[int] = set()
    for entry in raw:
        if not isinstance(entry, str):
            raise ScheduleError(
                f"schedule.{block_id}.weekdays entries must be strings, got {entry!r}"
            )
        key = entry.lower()
        if key not in _WEEKDAY_NAMES:
            raise ScheduleError(
                f"schedule.{block_id}.weekdays contains unknown day {entry!r}"
            )
        out.add(_WEEKDAY_NAMES[key])
    return frozenset(out)


# ---------------------------------------------------------------------------
# Pure slot computation
# ---------------------------------------------------------------------------


def compute_due_slots(
    now_utc: datetime,
    schedule_blocks: Iterable[ScheduleBlock],
    *,
    horizon_minutes: int = 0,
    zone: ZoneInfo = SCHEDULE_TZ,
) -> list[tuple[str, str]]:
    """Return ``[(kind, due_slot_utc_z)]`` for slots in ``[now, now + horizon]``.

    ``horizon_minutes`` is the window size in minutes.  Zero means
    "exactly the slots at ``now_utc``" — used by the ``--once --at``
    mode to enqueue a single deterministic slot.

    The function is pure: it reads only its arguments and returns a
    freshly-sorted list.  Slots are returned sorted by ``due_slot_utc``
    ascending then ``kind`` for determinism.
    """
    if not isinstance(now_utc, datetime):
        raise TypeError("now_utc must be a datetime")
    if now_utc.tzinfo is None:
        raise ValueError("now_utc must be timezone-aware")
    if (now_utc.utcoffset() != UTC.utcoffset(now_utc)):
        raise ValueError("now_utc must be in UTC")
    if horizon_minutes < 0:
        raise ValueError("horizon_minutes must be non-negative")

    blocks = list(schedule_blocks)
    upper_local = (now_utc.astimezone(UTC) + timedelta(minutes=horizon_minutes)).astimezone(zone)
    lower_local = now_utc.astimezone(zone)
    # We sweep at minute granularity: every minute whose local
    # representation falls in [lower_local, upper_local] *and* whose
    # local clock time matches a schedule block contributes a slot.
    slots: list[tuple[str, str]] = []
    cursor_local = lower_local.replace(second=0, microsecond=0)
    # Advance by 1 minute until we cover the whole interval.
    last_emitted: set[tuple[str, datetime]] = set()
    while cursor_local <= upper_local:
        # The "next minute" iteration uses the *true* clock, which is
        # exactly the right semantics for spring-forward/fall-back:
        # the spring-forward gap simply skips non-existent local
        # times, and fall-back emits twice.  We dedupe by (kind,
        # local_due) to keep deterministic ids.
        weekday = cursor_local.weekday()
        for block in blocks:
            if not block.enabled:
                continue
            if weekday not in block.weekdays:
                continue
            if cursor_local.hour not in block.local_hours or cursor_local.minute not in block.local_minutes:
                continue
            key = (block.kind, cursor_local)
            if key in last_emitted:
                continue
            last_emitted.add(key)
            due_utc = cursor_local.astimezone(UTC)
            if due_utc < lower_local.astimezone(UTC):
                continue
            if due_utc > upper_local.astimezone(UTC):
                continue
            due_slot_utc_z = _format_utc_z(due_utc)
            slots.append((block.kind, due_slot_utc_z))
        cursor_local = cursor_local + timedelta(minutes=1)
        # After adding, re-anchor in zone so DST shifts are honoured.
        cursor_local = cursor_local.astimezone(zone)
    slots.sort(key=lambda pair: (pair[1], pair[0]))
    return slots


def next_due_slot(
    now_utc: datetime,
    schedule_blocks: Iterable[ScheduleBlock],
    *,
    zone: ZoneInfo = SCHEDULE_TZ,
) -> tuple[str, str] | None:
    """Return the soonest (kind, due_slot_utc_z) strictly after ``now_utc``.

    Used by the recurring loop to know how long to sleep before the
    next sweep.
    """
    blocks = list(schedule_blocks)
    horizon = 60 * 24 * 14  # two weeks — plenty for any sane schedule.
    slots = compute_due_slots(now_utc, blocks, horizon_minutes=horizon, zone=zone)
    future = [
        (kind, due)
        for kind, due in slots
        if _parse_utc_z(due) > now_utc.astimezone(UTC)
    ]
    if not future:
        return None
    return future[0]


def _format_utc_z(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc_z(value: str) -> datetime:
    if not value.endswith("Z"):
        raise ValueError(f"UTC timestamp must end with Z, got {value!r}")
    return datetime.fromisoformat(value[:-1] + "+00:00").astimezone(UTC)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def enqueue_due_slots(
    connection: sqlite3.Connection,
    schedule_blocks: list[ScheduleBlock],
    *,
    now_utc: datetime,
    horizon_minutes: int = 0,
) -> list[tuple[str, str, bool]]:
    """Compute slots in the window and enqueue them.  Returns one row per slot.

    The returned row is ``(kind, due_slot_utc_z, created)`` where
    ``created`` is ``False`` when the (kind, due_slot) pair was already
    in the store.
    """
    if (now_utc.utcoffset() != UTC.utcoffset(now_utc)):
        raise ValueError("now_utc must be in UTC")
    slots = compute_due_slots(now_utc, schedule_blocks, horizon_minutes=horizon_minutes)
    results: list[tuple[str, str, bool]] = []
    for kind, due_slot_utc in slots:
        payload = _payload_for_slot(kind, due_slot_utc, schedule_blocks)
        try:
            _, created = enqueue(
                connection,
                kind=kind,
                due_slot_utc=due_slot_utc,
                payload=payload,
            )
        except DeliveryForbiddenError:
            # Defensive — the schedule parser already blocks delivery kinds.
            raise
        results.append((kind, due_slot_utc, created))
    return results


def _payload_for_slot(
    kind: str,
    due_slot_utc: str,
    blocks: list[ScheduleBlock],
) -> dict[str, Any]:
    local = _parse_utc_z(due_slot_utc).astimezone(SCHEDULE_TZ)
    matches = [
        block.payload
        for block in blocks
        if block.enabled
        and block.kind == kind
        and local.weekday() in block.weekdays
        and local.hour in block.local_hours
        and local.minute in block.local_minutes
    ]
    if not matches:
        raise ScheduleError(f"no schedule block owns due slot {kind}@{due_slot_utc}")
    canonical = {
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        for payload in matches
    }
    if len(canonical) != 1:
        raise ScheduleError(
            f"overlapping schedule blocks disagree on payload for {kind}@{due_slot_utc}"
        )
    return dict(matches[0])


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="news-container-scheduler")
    sub = parser.add_subparsers(dest="command", required=True)

    once = sub.add_parser("once")
    once.add_argument("--schedule", required=True, type=Path)
    once.add_argument("--db", required=True, type=Path)
    once.add_argument(
        "--at", required=True, type=str,
        help="UTC-Z timestamp; enqueue every due slot at or before this instant.",
    )

    loop = sub.add_parser("loop")
    loop.add_argument("--schedule", required=True, type=Path)
    loop.add_argument("--db", required=True, type=Path)
    loop.add_argument(
        "--max-iterations", type=int, default=0,
        help="Bounded loop count for tests; 0 means run until interrupted.",
    )
    loop.add_argument(
        "--sleep-cap-seconds", type=float, default=60.0,
        help="Upper bound on the sleep between sweeps (default 60s).",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    blocks = parse_schedule_toml(args.schedule.read_text(encoding="utf-8"))
    if args.command == "once":
        at = _parse_utc_z(args.at)
        from .control_store import open as open_db
        connection = open_db(args.db)
        try:
            rows = enqueue_due_slots(connection, blocks, now_utc=at, horizon_minutes=0)
        finally:
            connection.close()
        print(json.dumps({"enqueued": [list(row) for row in rows]}, ensure_ascii=False))
        return 0
    if args.command == "loop":
        from .control_store import open as open_db
        connection = open_db(args.db)
        try:
            return _run_loop(
                connection, blocks,
                max_iterations=args.max_iterations,
                sleep_cap_seconds=args.sleep_cap_seconds,
            )
        finally:
            connection.close()
    return 2


def _run_loop(
    connection: sqlite3.Connection,
    blocks: list[ScheduleBlock],
    *,
    max_iterations: int,
    sleep_cap_seconds: float,
) -> int:
    """Recurring sweep with bounded sleep.

    The sleep is bounded by ``sleep_cap_seconds`` so a misconfigured
    schedule (e.g. ``--at`` 5 minutes in the future) doesn't cause the
    process to block indefinitely.  ``max_iterations`` lets tests
    drive a deterministic number of sweeps.
    """
    import time

    if sleep_cap_seconds <= 0:
        raise ValueError("sleep_cap_seconds must be positive")
    iterations = 0
    while True:
        now = datetime.now(UTC)
        enqueue_due_slots(connection, blocks, now_utc=now, horizon_minutes=0)
        iterations += 1
        if max_iterations and iterations >= max_iterations:
            return 0
        nxt = next_due_slot(now, blocks)
        sleep_for = sleep_cap_seconds
        if nxt is not None:
            _, due_slot = nxt
            delta = (_parse_utc_z(due_slot) - now).total_seconds()
            sleep_for = max(0.5, min(sleep_cap_seconds, delta))
        time.sleep(sleep_for)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))