"""Slice 6.1 — caller-supplied ExternalContext / time identity.

The phase6 sandbox must NEVER consult the system clock. The only source of
time identity is a frozen :class:`ExternalContext` provided by the caller.
"""
from __future__ import annotations

import dataclasses
import re

from .types import Phase6IdentityError


# Z-suffixed UTC subset. Up to microseconds; no offsets; no naive forms.
_ISO8601_ZULU_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$"
)

# Bounds on the optional caller-declared clock drift, in whole seconds.
_MAX_CLOCK_DRIFT_SECONDS = 86_400  # 24h


def _is_leap_year(year: int) -> bool:
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


def _days_in_month(year: int, month: int) -> int:
    if month in (1, 3, 5, 7, 8, 10, 12):
        return 31
    if month in (4, 6, 9, 11):
        return 30
    # February
    return 29 if _is_leap_year(year) else 28


# Local alias to match the patched-in function name in the validator.
_DAYS_IN_MONTH = _days_in_month


@dataclasses.dataclass(frozen=True)
class ExternalContext:
    """Frozen, caller-supplied identity record for a sandbox invocation.

    ``now_utc_iso`` is mandatory; ``timezone`` and ``clock_drift_seconds``
    are optional diagnostics that the sandbox records but never acts on.
    """

    now_utc_iso: str
    timezone: str | None = None
    clock_drift_seconds: int | None = None

    def __post_init__(self) -> None:
        # ``frozen=True`` means we cannot ``self.now_utc_iso = ...`` in
        # ``__setattr__``; instead we raise at construction time so the
        # caller never gets a half-constructed instance.
        if not isinstance(self.now_utc_iso, str):
            raise TypeError(
                "ExternalContext.now_utc_iso must be str, "
                f"got {type(self.now_utc_iso).__name__}"
            )


def validate_iso8601_utc(value: object) -> str:
    """Return ``value`` if it is a Z-suffixed UTC ISO-8601 string, else raise.

    The validator accepts:
      * second precision  ``YYYY-MM-DDTHH:MM:SSZ``
      * sub-second precision up to microseconds.

    It rejects offsets, naive forms, lowercase ``z``, surrounding whitespace,
    and obviously non-calendar garbage (including out-of-range months / days
    and invalid day-of-month for the given month).
    """
    if not isinstance(value, str):
        raise Phase6IdentityError(
            f"iso8601 utc value must be str, got {type(value).__name__}"
        )
    if not _ISO8601_ZULU_RE.match(value):
        raise Phase6IdentityError(
            f"iso8601 utc value must match Z-suffixed UTC subset: {value!r}"
        )
    # Range-check the calendar components.
    year = int(value[0:4])
    month = int(value[5:7])
    day = int(value[8:10])
    hour = int(value[11:13])
    minute = int(value[14:16])
    second = int(value[17:19])
    if not (1 <= month <= 12):
        raise Phase6IdentityError(f"month out of range: {month}")
    if not (1 <= day <= _DAYS_IN_MONTH(year, month)):
        raise Phase6IdentityError(f"day out of range: {day}")
    if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 60):
        # Allow leap seconds (60) per ISO-8601.
        raise Phase6IdentityError(
            f"time component out of range: {hour:02d}:{minute:02d}:{second:02d}"
        )
    return value


def validate_external_context(ctx: ExternalContext) -> str:
    """Validate ``ctx`` and return the canonical ``now_utc_iso`` string."""
    # now_utc_iso is type-annotated as str on the dataclass; a non-str would
    # be caught here regardless of how the caller built the object.
    canonical = validate_iso8601_utc(ctx.now_utc_iso)

    if ctx.timezone is not None and not isinstance(ctx.timezone, str):
        raise Phase6IdentityError(
            f"timezone must be str or None, got {type(ctx.timezone).__name__}"
        )

    if ctx.clock_drift_seconds is not None:
        drift = ctx.clock_drift_seconds
        if isinstance(drift, bool) or not isinstance(drift, int):
            raise Phase6IdentityError(
                "clock_drift_seconds must be int or None"
            )
        if drift < 0:
            raise Phase6IdentityError("clock_drift_seconds must be >= 0")
        if drift > _MAX_CLOCK_DRIFT_SECONDS:
            raise Phase6IdentityError(
                f"clock_drift_seconds must be <= {_MAX_CLOCK_DRIFT_SECONDS}"
            )

    return canonical


__all__ = [
    "ExternalContext",
    "validate_iso8601_utc",
    "validate_external_context",
]