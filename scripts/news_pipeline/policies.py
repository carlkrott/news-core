"""Phase 2 — declarative source and query policies.

These types are *data*, not behaviour: they declare what the engine should do
without performing any I/O. The engine in ``filtering.py`` consumes them.

The defaults are documented as *configurable*; the supervisor-selected values
from the repair prompt are applied by ``default_query_policies()``. Production
integrations are free to override them — the engine never invents new rules.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from .canonicalization import canonicalize_url
from .contracts import ReasonCode, TrustTier
from .models import CATEGORY_TO_SUBJECT, Category, Subject


# ---------------------------------------------------------------------------
# Source policy
# ---------------------------------------------------------------------------

_VALID_ACTIONS = {"block", "allow", "trusted"}
_VALID_SCOPES = {"exact", "subdomain"}


@dataclass(frozen=True, slots=True)
class SourceRule:
    """One declarative rule in the source policy.

    ``scope='exact'`` matches the canonical host exactly.
    ``scope='subdomain'`` matches the canonical host exactly OR any subdomain
    via a strict ``.<domain>`` suffix boundary (never substring).
    """

    label: str
    host: str
    scope: str
    action: str

    def __post_init__(self) -> None:
        if not self.label:
            raise ValueError("SourceRule.label must be a non-empty string")
        if self.scope not in _VALID_SCOPES:
            raise ValueError(
                f"SourceRule.scope must be one of {sorted(_VALID_SCOPES)!r}, got {self.scope!r}"
            )
        if self.action not in _VALID_ACTIONS:
            raise ValueError(
                f"SourceRule.action must be one of {sorted(_VALID_ACTIONS)!r}, got {self.action!r}"
            )
        # Lowercase + IDNA-normalize the host so policy authors can use whatever form they like.
        normalized = self._normalize_host(self.host)
        object.__setattr__(self, "host", normalized)

    @staticmethod
    def _normalize_host(host: str) -> str:
        host = host.strip().lower().rstrip(".")
        if not host:
            raise ValueError("SourceRule.host must be non-empty")
        try:
            return host.encode("idna").decode("ascii")
        except UnicodeError as exc:
            raise ValueError(f"SourceRule.host is not IDNA-compatible: {host!r}") from exc


@dataclass(frozen=True, slots=True)
class SourceMatch:
    """Result of classifying one URL against a ``SourcePolicy``."""

    tier: TrustTier
    reason: ReasonCode
    matched_rule: str | None = None


@dataclass(frozen=True, slots=True)
class SourcePolicy:
    """Ordered (by declaration) set of source rules with strict precedence."""

    rules: tuple[SourceRule, ...] = ()

    def __post_init__(self) -> None:
        # Ensure tuple (frozen, hashable).
        object.__setattr__(self, "rules", tuple(self.rules))


def _canonical_host(url_or_none: str | None) -> str | None:
    if url_or_none is None or url_or_none == "":
        return None
    try:
        canonical = canonicalize_url(url_or_none)
    except ValueError:
        return None
    if canonical is None:
        return None
    # Strip default ports from the host portion if any slipped through.
    # canonicalize_url already handles this, but we re-split for safety.
    # Avoid ``urllib.parse`` here so the package's AST stays network-free.
    scheme_sep = canonical.find("://")
    if scheme_sep < 0:
        return None
    rest = canonical[scheme_sep + 3 :]
    delimiters = [idx for idx in (rest.find("/"), rest.find("?"), rest.find("#")) if idx >= 0]
    authority = rest if not delimiters else rest[:min(delimiters)]
    # Lower-case everything before any port component and strip credentials.
    # canonicalize_url already rejects userinfo, so this is defensive.
    at_idx = authority.rfind("@")
    if at_idx >= 0:
        authority = authority[at_idx + 1 :]
    # Strip port (square-bracketed IPv6 + numeric port).
    if authority.startswith("["):
        # IPv6
        end = authority.find("]")
        host = authority[1 : end] if end > 0 else authority
    else:
        port_idx = authority.find(":")
        host = authority if port_idx < 0 else authority[:port_idx]
    host = host.strip().lower()
    if not host:
        return None
    try:
        return host.encode("idna").decode("ascii")
    except UnicodeError:
        return None


def _host_matches(host: str, rule_host: str, scope: str) -> bool:
    if scope == "exact":
        return host == rule_host
    # subdomain: exact or `.<domain>` suffix boundary. Never substring.
    if host == rule_host:
        return True
    return host.endswith("." + rule_host)


def classify_source(policy: SourcePolicy, url: str | None) -> SourceMatch:
    """Classify a URL against ``policy``.

    Precedence: BLOCK exact > BLOCK subdomain > ALLOW exact > ALLOW subdomain
    > TRUSTED exact > TRUSTED subdomain > UNKNOWN. Block cannot be overridden
    by allow. A missing or malformed URL is UNKNOWN with a low-confidence
    reason — it is **not** a drop on its own.
    """
    if url is None or url == "":
        return SourceMatch(TrustTier.UNKNOWN, ReasonCode.MISSING_URL, None)
    host = _canonical_host(url)
    if host is None:
        return SourceMatch(TrustTier.UNKNOWN, ReasonCode.MALFORMED_CANONICAL_URL, None)

    precedence = [
        ("block", "exact", TrustTier.BLOCKED, ReasonCode.BLOCKED_SOURCE_EXACT),
        ("block", "subdomain", TrustTier.BLOCKED, ReasonCode.BLOCKED_SOURCE_SUBDOMAIN),
        ("allow", "exact", TrustTier.ALLOWED, ReasonCode.OK_KEEP),
        ("allow", "subdomain", TrustTier.ALLOWED, ReasonCode.OK_KEEP),
        ("trusted", "exact", TrustTier.TRUSTED, ReasonCode.OK_TRUSTED_SOURCE),
        ("trusted", "subdomain", TrustTier.TRUSTED, ReasonCode.OK_TRUSTED_SOURCE),
    ]
    for action, scope, tier, reason in precedence:
        for rule in policy.rules:
            if rule.action != action or rule.scope != scope:
                continue
            if _host_matches(host, rule.host, rule.scope):
                return SourceMatch(tier, reason, rule.label)
    return SourceMatch(TrustTier.UNKNOWN, ReasonCode.OK_UNKNOWN_SOURCE, None)


# ---------------------------------------------------------------------------
# Query policy
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QueryPolicy:
    """One category's per-query filtering policy.

    ``allowed_query_groups`` defaults to a single value: the ``Category.value``
    of the owning category. Production code is free to add explicit aliases
    (e.g. ``("ai", "ai_explained")``); the engine never invents new ones.
    """

    category: Category
    allowed_query_groups: tuple[str, ...]
    recency: timedelta
    missing_date_fallback: bool
    exact_title_lookback: timedelta
    exact_url_lookback: timedelta
    exact_identity_lookback: timedelta
    cross_category_exact_url: bool = True
    subject: Subject | None = None

    def __post_init__(self) -> None:
        if not self.allowed_query_groups:
            raise ValueError("QueryPolicy.allowed_query_groups must be non-empty")
        for qg in self.allowed_query_groups:
            if not isinstance(qg, str) or not qg:
                raise ValueError(f"QueryPolicy.allowed_query_groups contains bad entry: {qg!r}")


def query_policies_from_subject_policies(subject_policies: object) -> dict[Category, QueryPolicy]:
    """Build category policies from loaded subject ``recency_days`` values.

    ``source_registry.SubjectPolicy`` is intentionally not imported here to
    keep the declarative policy module independent.  The adapter accepts the
    loaded mapping (or its values) and only reads the stable ``subject`` and
    ``recency_days`` attributes.
    """
    if hasattr(subject_policies, "items"):
        entries = tuple(subject_policies.items())  # type: ignore[union-attr]
    else:
        entries = tuple((getattr(policy, "subject", None), policy) for policy in subject_policies)  # type: ignore[union-attr]

    recency_by_subject: dict[Subject, int] = {}
    for key, value in entries:
        subject_value = getattr(value, "subject", key)
        try:
            subject = subject_value if isinstance(subject_value, Subject) else Subject(str(subject_value))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"unknown subject policy key: {subject_value!r}") from exc
        days = getattr(value, "recency_days", value)
        if isinstance(days, bool) or not isinstance(days, int) or days < 1:
            raise ValueError(f"subject {subject.value!r} recency_days must be an integer >= 1")
        recency_by_subject[subject] = days

    missing = sorted(subject.value for subject in set(CATEGORY_TO_SUBJECT.values()) if subject not in recency_by_subject)
    if missing:
        raise ValueError(f"missing subject recency policies: {missing!r}")

    policies: dict[Category, QueryPolicy] = {}
    for category, subject in CATEGORY_TO_SUBJECT.items():
        window = timedelta(days=recency_by_subject[subject])
        policies[category] = QueryPolicy(
            category=category,
            allowed_query_groups=(category.value,),
            recency=window,
            missing_date_fallback=True,
            exact_title_lookback=min(window, timedelta(hours=72)),
            exact_url_lookback=window,
            exact_identity_lookback=window,
            cross_category_exact_url=True,
            subject=subject,
        )
    return policies


def default_query_policies() -> dict[Category, QueryPolicy]:
    """Supervisor-decided category defaults (configurable, not optimal)."""
    return {
        Category.AI: QueryPolicy(
            category=Category.AI,
            allowed_query_groups=(Category.AI.value,),
            recency=timedelta(hours=72),
            missing_date_fallback=True,
            exact_title_lookback=timedelta(hours=72),
            exact_url_lookback=timedelta(days=7),
            exact_identity_lookback=timedelta(days=7),
            cross_category_exact_url=True,
        ),
        Category.WORLD: QueryPolicy(
            category=Category.WORLD,
            allowed_query_groups=(Category.WORLD.value,),
            recency=timedelta(hours=48),
            missing_date_fallback=True,
            exact_title_lookback=timedelta(hours=72),
            exact_url_lookback=timedelta(days=7),
            exact_identity_lookback=timedelta(days=7),
            cross_category_exact_url=True,
        ),
        Category.AUDIO_ENGINEERING: QueryPolicy(
            category=Category.AUDIO_ENGINEERING,
            allowed_query_groups=(Category.AUDIO_ENGINEERING.value,),
            recency=timedelta(days=14),
            missing_date_fallback=True,
            exact_title_lookback=timedelta(hours=72),
            exact_url_lookback=timedelta(days=7),
            exact_identity_lookback=timedelta(days=7),
            cross_category_exact_url=True,
        ),
        Category.HARDWARE: QueryPolicy(
            category=Category.HARDWARE,
            allowed_query_groups=(Category.HARDWARE.value,),
            recency=timedelta(days=7),
            missing_date_fallback=True,
            exact_title_lookback=timedelta(hours=72),
            exact_url_lookback=timedelta(days=7),
            exact_identity_lookback=timedelta(days=7),
            cross_category_exact_url=True,
        ),
        Category.FANTASY_NOVEL: QueryPolicy(
            category=Category.FANTASY_NOVEL,
            allowed_query_groups=(Category.FANTASY_NOVEL.value,),
            recency=timedelta(days=14),
            missing_date_fallback=True,
            exact_title_lookback=timedelta(hours=72),
            exact_url_lookback=timedelta(days=7),
            exact_identity_lookback=timedelta(days=7),
            cross_category_exact_url=True,
        ),
        Category.AUDIOVISUAL: QueryPolicy(
            category=Category.AUDIOVISUAL,
            allowed_query_groups=(Category.AUDIOVISUAL.value,),
            recency=timedelta(days=7),
            missing_date_fallback=True,
            exact_title_lookback=timedelta(hours=72),
            exact_url_lookback=timedelta(days=7),
            exact_identity_lookback=timedelta(days=7),
            cross_category_exact_url=True,
        ),
        Category.AV_CORPORATE: QueryPolicy(
            category=Category.AV_CORPORATE,
            allowed_query_groups=(Category.AV_CORPORATE.value,),
            recency=timedelta(days=7),
            missing_date_fallback=True,
            exact_title_lookback=timedelta(hours=72),
            exact_url_lookback=timedelta(days=7),
            exact_identity_lookback=timedelta(days=7),
            cross_category_exact_url=True,
        ),
        Category.OUR_SETUP: QueryPolicy(
            category=Category.OUR_SETUP,
            allowed_query_groups=(Category.OUR_SETUP.value,),
            recency=timedelta(days=30),
            missing_date_fallback=True,
            exact_title_lookback=timedelta(hours=72),
            exact_url_lookback=timedelta(days=7),
            exact_identity_lookback=timedelta(days=7),
            cross_category_exact_url=True,
        ),
    }


# Exposed as a frozen module-level binding for convenience; deep copies happen
# at engine entry so callers can mutate their own dictionaries safely.
DEFAULT_POLICIES: dict[Category, QueryPolicy] = default_query_policies()
