"""Phase 3 fact extraction — deterministic regex-based.

Public surface:
  - ``extract_facts(title, snippet) -> ExtractedFacts``
  - ``canonical_decimal(d) -> str``

Implements the supervisor addendum §3 grammar and §3 normalization. Order of
extraction is date -> semver -> price -> percent -> count. Spans already
consumed by an earlier kind cannot produce a later fact.

The module is stdlib-only. No DB, no network, no time.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Tuple, List

from .event_contracts import ExtractedFacts, FactKind, TypedFact
from .clustering import tokenize as _tokenize  # local alias for context lines


# --- Regex grammar (addendum §3 verbatim) --------------------------------------

PRICE_RE = re.compile(
    r"(?<!\w)(?P<currency>USD|GBP|EUR|US\$|\$|£|€)\s*"
    r"(?P<value>\d{1,12}(?:\.\d{1,4})?)\s*"
    r"(?P<suffix>k|m|million|b|billion)?(?![\w.])",
    re.IGNORECASE | re.UNICODE,
)
SEMVER_RE = re.compile(
    r"(?<![\w.])v(?P<major>\d{1,5})\.(?P<minor>\d{1,5})"
    r"(?:\.(?P<patch>\d{1,5}))?(?![\w.])",
    re.IGNORECASE | re.UNICODE,
)
ISO_DATE_RE = re.compile(r"(?<!\d)(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})(?!\d)")
MONTH_DATE_RE = re.compile(
    r"(?<!\w)(?P<month>January|February|March|April|May|June|July|August|September|October|November|December|"
    r"Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+"
    r"(?P<day>\d{1,2})(?:,)?\s+(?P<year>\d{4})(?!\d)",
    re.IGNORECASE | re.UNICODE,
)
PERCENT_RE = re.compile(r"(?<![\w.])(?P<value>\d{1,3}(?:\.\d{1,4})?)\s*%(?!\w)")
COUNT_RE = re.compile(
    r"(?<![\w.])(?P<value>\d{1,12})\s+"
    r"(?P<unit>users?|employees?|subscribers?|players?|copies|units?|jobs?|layoffs?)(?!\w)",
    re.IGNORECASE | re.UNICODE,
)

_COUNT_UNIT_MAP = {
    "user": "users", "users": "users",
    "employee": "employees", "employees": "employees",
    "subscriber": "subscribers", "subscribers": "subscribers",
    "player": "players", "players": "players",
    "copies": "copies",
    "unit": "units", "units": "units",
    "job": "jobs", "jobs": "jobs",
    "layoff": "layoffs", "layoffs": "layoffs",
}


# --- Currency / suffix normalization ------------------------------------------

_CURRENCY_MAP = {
    "$": "usd", "US$": "usd", "USD": "usd",
    "£": "gbp", "GBP": "gbp",
    "€": "eur", "EUR": "eur",
}
_SUFFIX_MULT = {
    None: 1, "": 1, "k": 1000,
    "m": 1_000_000, "million": 1_000_000,
    "b": 1_000_000_000, "billion": 1_000_000_000,
}


def _normalize_currency(token: str) -> str:
    return _CURRENCY_MAP[token.upper().replace("US$", "US$") if token == "US$" else token]


def _currency_for(token: str) -> str | None:
    if token in _CURRENCY_MAP:
        return _CURRENCY_MAP[token]
    # Handle "US$" case-sensitively because regex matched it as "$"
    upper = token.upper()
    if upper in _CURRENCY_MAP:
        return _CURRENCY_MAP[upper]
    return None


# --- Canonical decimal text ---------------------------------------------------


def canonical_decimal(d: Decimal) -> str:
    """Fixed-point text form per addendum §3; negative zero rejected."""
    if d.is_zero() and d.is_signed():
        raise ValueError("negative zero is not allowed")
    text = format(d.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


# --- Span tracking for precedence --------------------------------------------


@dataclass(frozen=True)
class _Span:
    start: int
    end: int
    text: str


def _make_spans(text: str, matches) -> List[_Span]:
    return [_Span(m.start(), m.end(), m.group(0)) for m in matches]


def _merge_spans(*groups: List[_Span]) -> List[_Span]:
    spans: List[_Span] = []
    for g in groups:
        spans.extend(g)
    spans.sort(key=lambda s: (s.start, s.end))
    return spans


def _text_with_consumed(text: str, consumed: List[_Span]) -> str:
    """Build a copy of ``text`` where each consumed span is replaced by spaces.

    Used so a later regex cannot match a substring inside an earlier match.
    """
    chars = list(text)
    for span in consumed:
        for i in range(span.start, span.end):
            if chars[i] != "\n":
                chars[i] = " "
    return "".join(chars)


# --- Per-kind extractors (operate on text with earlier spans hidden) ---------


def _extract_dates(text: str, context: Tuple[str, ...]) -> Tuple[List[TypedFact], List[_Span]]:
    facts: List[TypedFact] = []
    spans: List[_Span] = []
    # ISO first
    for m in ISO_DATE_RE.finditer(text):
        try:
            d = date(int(m.group("year")), int(m.group("month")), int(m.group("day")))
            if d.year < 1900 or d.year > 2200:
                continue
        except ValueError:
            continue
        facts.append(TypedFact(
            kind=FactKind.DATE, unit="iso", value_normalized=d.isoformat(),
            context=context, evidence=text[m.start():m.end()][:128],
        ))
        spans.append(_Span(m.start(), m.end(), m.group(0)))
    # Month-name dates use the masked text so they don't collide with ISO dates
    masked = _text_with_consumed(text, spans)
    for m in MONTH_DATE_RE.finditer(masked):
        raw = text[m.start():m.end()]
        try:
            d = date(int(m.group("year")), _month_number(m.group("month")), int(m.group("day")))
            if d.year < 1900 or d.year > 2200:
                continue
            normalized = d.isoformat()
        except ValueError:
            continue
        if raw.strip() in (f.value_normalized for f in facts):
            continue
        facts.append(TypedFact(
            kind=FactKind.DATE, unit="month", value_normalized=normalized,
            context=context, evidence=raw[:128],
        ))
        spans.append(_Span(m.start(), m.end(), raw))
    return facts, spans


def _month_number(token: str) -> int:
    """Convert a Month token (full or 3-4 letter) to 1..12."""
    full = {
        "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
        "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
        "november": 11, "december": 12,
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7,
        "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    return full[token.lower()]


def _extract_semvers(text: str, context: Tuple[str, ...]) -> Tuple[List[TypedFact], List[_Span]]:
    facts: List[TypedFact] = []
    spans: List[_Span] = []
    seen = set()
    for m in SEMVER_RE.finditer(text):
        major = int(m.group("major"))
        minor = int(m.group("minor"))
        patch = m.group("patch")
        if not (0 <= major <= 99999 and 0 <= minor <= 99999):
            continue
        if patch is not None and not (0 <= int(patch) <= 99999):
            continue
        canonical = f"{major}.{minor}" + (f".{int(patch)}" if patch is not None else "")
        if canonical in seen:
            spans.append(_Span(m.start(), m.end(), m.group(0)))
            continue
        seen.add(canonical)
        facts.append(TypedFact(
            kind=FactKind.VERSION, unit="semver", value_normalized=canonical,
            context=context, evidence=text[m.start():m.end()][:128],
        ))
        spans.append(_Span(m.start(), m.end(), m.group(0)))
    return facts, spans


def _extract_prices(text: str, context: Tuple[str, ...]) -> Tuple[List[TypedFact], List[_Span]]:
    facts: List[TypedFact] = []
    spans: List[_Span] = []
    seen = set()
    for m in PRICE_RE.finditer(text):
        token = m.group("currency")
        currency = _currency_for(token)
        if currency is None:
            continue
        suffix = (m.group("suffix") or "").lower()
        mult = _SUFFIX_MULT.get(suffix, 1)
        try:
            val = Decimal(m.group("value"))
        except InvalidOperation:
            continue
        total = val * mult
        # Price must be >0 and <= 1e15
        if total <= 0 or total > Decimal("1000000000000000"):
            continue
        normalized = canonical_decimal(total)
        evidence = text[m.start():m.end()][:128]
        key = (currency, normalized)
        if key in seen:
            spans.append(_Span(m.start(), m.end(), m.group(0)))
            continue
        seen.add(key)
        facts.append(TypedFact(
            kind=FactKind.PRICE, unit=currency, value_normalized=normalized,
            context=context, evidence=evidence,
        ))
        spans.append(_Span(m.start(), m.end(), m.group(0)))
    return facts, spans


def _extract_percents(text: str, context: Tuple[str, ...]) -> Tuple[List[TypedFact], List[_Span]]:
    facts: List[TypedFact] = []
    spans: List[_Span] = []
    seen = set()
    for m in PERCENT_RE.finditer(text):
        try:
            val = Decimal(m.group("value"))
        except InvalidOperation:
            continue
        if val < 0 or val > 100:
            continue
        normalized = canonical_decimal(val)
        if normalized in seen:
            spans.append(_Span(m.start(), m.end(), m.group(0)))
            continue
        seen.add(normalized)
        facts.append(TypedFact(
            kind=FactKind.PERCENT, unit="percent", value_normalized=normalized,
            context=context, evidence=text[m.start():m.end()][:128],
        ))
        spans.append(_Span(m.start(), m.end(), m.group(0)))
    return facts, spans


def _extract_counts(text: str, context: Tuple[str, ...]) -> Tuple[List[TypedFact], List[_Span]]:
    facts: List[TypedFact] = []
    spans: List[_Span] = []
    seen = set()
    for m in COUNT_RE.finditer(text):
        try:
            val = int(m.group("value"))
        except ValueError:
            continue
        if val < 1 or val > 1_000_000_000_000:
            continue
        unit = _COUNT_UNIT_MAP[m.group("unit").lower()]
        key = (unit, val)
        if key in seen:
            spans.append(_Span(m.start(), m.end(), m.group(0)))
            continue
        seen.add(key)
        facts.append(TypedFact(
            kind=FactKind.COUNT, unit=unit, value_normalized=str(val),
            context=context, evidence=text[m.start():m.end()][:128],
        ))
        spans.append(_Span(m.start(), m.end(), m.group(0)))
    return facts, spans


# --- Public extract_facts -----------------------------------------------------


def extract_facts(title: str, snippet: str) -> ExtractedFacts:
    """Deterministic 5-bucket extraction. Order: date, semver, price, pct, count.

    ``title`` is used for the fact context. ``snippet`` and ``title`` are
    independently scanned but conflicts in the snippet never overwrite the
    title. (Implementation detail: we extract from title first; evidence
    within the snippet is used verbatim.)
    """
    # Walk through both surfaces separately — title's facts always sort before
    # the snippet's — and de-dup by (kind, unit, value_normalized, context,
    # evidence) tuple within each surface.
    date_facts: List[TypedFact] = []
    semver_facts: List[TypedFact] = []
    price_facts: List[TypedFact] = []
    percent_facts: List[TypedFact] = []
    count_facts: List[TypedFact] = []
    title_context = _tokenize(title)[:8]
    for src in (title, snippet):
        # Sequentially mask earlier kinds so they never match a later regex.
        d_facts, d_spans = _extract_dates(src, title_context)
        masked = _text_with_consumed(src, d_spans)
        s_facts, s_spans = _extract_semvers(masked, title_context)
        masked2 = _text_with_consumed(masked, s_spans)
        p_facts, p_spans = _extract_prices(masked2, title_context)
        masked3 = _text_with_consumed(masked2, p_spans)
        pct_facts, pct_spans = _extract_percents(masked3, title_context)
        masked4 = _text_with_consumed(masked3, pct_spans)
        c_facts, _ = _extract_counts(masked4, title_context)
        date_facts.extend(d_facts)
        semver_facts.extend(s_facts)
        price_facts.extend(p_facts)
        percent_facts.extend(pct_facts)
        count_facts.extend(c_facts)

    return ExtractedFacts(
        prices=tuple(_dedupe_sorted(price_facts)),
        versions=tuple(_dedupe_sorted(semver_facts)),
        dates=tuple(_dedupe_sorted(date_facts)),
        percentages=tuple(_dedupe_sorted(percent_facts)),
        counts=tuple(_dedupe_sorted(count_facts)),
    )


def _dedupe_sorted(facts: List[TypedFact]) -> List[TypedFact]:
    """Addendum §3: dedupe by (kind.value, unit, value_normalized, context, evidence)."""
    seen_set = set()
    out: List[TypedFact] = []
    for tf in _sort_key(facts):
        key = (tf.kind.value, tf.unit, tf.value_normalized, tf.context, tf.evidence)
        if key in seen_set:
            continue
        seen_set.add(key)
        out.append(tf)
    return out


def _sort_key(facts: List[TypedFact]) -> List[TypedFact]:
    """Sort by (kind.value, unit, value_normalized, context, evidence)."""
    return sorted(
        facts,
        key=lambda tf: (tf.kind.value, tf.unit, tf.value_normalized, tf.context, tf.evidence),
    )
