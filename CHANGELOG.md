# Changelog

All notable changes to the public news-core source are recorded here.

## [Unreleased]

- Publication preparation for the delivery-disabled, containerized news core.
- Added the closed public-export workflow and safety-review metadata.
- Added version-2 subject contracts with deterministic category-to-subject
  routing, per-subject report scope, recency windows, and fail-closed ambiguous
  routing.
- Added sanitized inclusion, exclusion, materiality, and story-cap examples for
  Audio Engineering and Professional AV subject boundaries.
- Added additive publisher provenance and verification identity contracts:
  reviewed publisher families, conservative unknown backfill, explicit authority
  matching, and fail-closed independence groups.  Live publisher rules remain
  private overlays; this structural change performs no live migration or
  delivery cutover.
- Added isolated feed-lane identities and per-candidate `investigate` jobs:
  query seeds are category-scoped, receipts retain lane provenance, ingest
  enqueues deterministic one-candidate tasks, targeted investigation plans are
  bounded, terminal receipts are immutable, and failed work has an explicit
  bounded-round retry path with lease-fenced state writes.  Public workers
  remain delivery-disabled.  This structural change performs no live migration
  or schedule activation.
- Added Run 4 admission quality controls: deterministic article-route
  classification, adapter and ingest-boundary URL validation, explicit date
  evidence and recency statuses, subject-configured freshness/history windows,
  per-lane URL/date coverage counters, audit-only URL-less decisions, and
  canonical-source-URL requirements for verified report events.  Public
  workers remain delivery-disabled; this structural change performs no live
  migration or schedule activation.
- Added Run 5 event-level novelty controls: durable event/version identity,
  exact URL/title fast prefilters, grounded fact/state delta requirements for
  material updates, rewrite suppression, subject-scoped briefing seen keys,
  deterministic primary-subject routing with secondary suppression, and
  audit preservation of later payloads.  Public workers remain
  delivery-disabled; this structural change performs no live migration or
  schedule activation.
- Added Run 6 verification-to-report promotion repair: persisted publisher
  provenance reaches every evidence evaluation, verified event versions
  reconcile into subject-scoped `report_events`, and silent promotion gaps
  become typed audit discrepancies.  Unverified, watchlist, rejected, and
  superseded versions remain excluded.  Public workers remain
  delivery-disabled; this structural change performs no live migration or
  schedule activation.

## [0.1.0] - 2026-09-15

- Initial containerized news-core candidate based on the r1-20260914 freeze.
- Five isolated roles: scheduler, ingest, process, validate, and report.
- Host-side search, feed, and LLM broker boundaries; delivery remains disabled.
- Runtime image uses a digest-pinned Python 3.11 slim base and a non-root user.
- Local validation covers the publication scanner, container contract, Python
  compilation, and Compose rendering.

This source release is delivered under the MIT License.  Container image
publication and provenance attestation remain separate, tag-gated actions.
