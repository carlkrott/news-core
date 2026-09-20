"""Isolated Compose runtime scheduler/control plane for the news container.

The ``news_container`` package owns the collaborators that the Compose
services use to schedule, dispatch, and run the news pipeline jobs:

* :mod:`control_store` — a dedicated SQLite DB (separate from
  ``news-state.db``) with idempotent enqueue, claim/complete fencing,
  and a run history.  This DB is the *only* scheduler state that
  Compose services share.
* :mod:`scheduler` — pure functions that turn a TOML schedule into
  deterministic UTC due-slots and enqueue them through the store.
* :mod:`worker` — role workers that claim a fixed kind, take a shared
  ``fcntl`` flock, invoke ``news_pipeline.jobs.main`` with bounded
  stdout capture, and record the result.
* :mod:`broker_protocol` — the canonical JSON envelope contract for
  the search/feed Unix-domain-socket broker that mediates egress when
  ``NEWS_CONTAINER_MODE=1`` is set.
* :mod:`broker_client` — the stdlib AF_UNIX HTTP transport that
  carries broker envelopes; its ``broker_transport_factory`` is the
  integration point with ``news_pipeline.ingest_runner.run_ingest``.

The package is intentionally stdlib-only so it can be vendored into
the Compose image without taking on third-party wheels.  No code in
this package — nor any worker it produces — is permitted to enable
delivery, contact the network, or touch paths outside the canary
root when ``NEWS_CANARY_ROOT`` is set.
"""
from __future__ import annotations

__all__ = ["ALLOWED_KINDS", "DELIVERY_KIND_FORBIDDEN"]

# The set of job kinds the control plane is allowed to schedule.
# Anything outside this set is refused at enqueue time.  The
# ``delivery`` kind is explicitly forbidden: live delivery is not a
# concern of the runtime-control slice.
ALLOWED_KINDS = frozenset({"ingest", "investigate", "process", "validate", "report"})

# Names the workers must never let through their vocabulary.
DELIVERY_KIND_FORBIDDEN = "delivery"

# Re-export the broker public surface so callers can write
# ``from news_container import broker_transport_factory`` without
# reaching into the submodules.  Imports are deferred to keep module
# import order deterministic and to avoid pulling ``news_pipeline``
# into the worker control plane.
def __getattr__(name: str):  # pragma: no cover - trivial re-export
    if name in (
        "broker_transport_factory",
        "container_mode_enabled",
        "route_for_source",
        "BrokerTransport",
        "BrokerUnavailableError",
        "BrokerProtocolMismatch",
    ):
        from . import broker_client

        value = getattr(broker_client, name)
        globals()[name] = value
        return value
    if name in (
        "BrokerRequest",
        "BrokerResponse",
        "BrokerProtocolError",
        "UnsupportedRouteError",
        "DisallowedHeaderError",
        "InvalidEnvelopeError",
        "validate_request_payload",
        "derive_request_id",
        "socket_path_for_route",
        "PROTOCOL_VERSION",
        "ROUTE_SEARCH",
        "ROUTE_FEED",
        "ALLOWED_ROUTES",
        "ALLOWED_HEADERS",
        "ENV_CONTAINER_MODE",
        "ENV_SEARCH_SOCKET",
        "ENV_FEED_SOCKET",
    ):
        from . import broker_protocol

        value = getattr(broker_protocol, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module 'news_container' has no attribute {name!r}")