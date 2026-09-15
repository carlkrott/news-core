# Changelog

All notable changes to the public news-core source are recorded here.

## [Unreleased]

- Publication preparation for the delivery-disabled, containerized news core.
- Added the closed public-export workflow and safety-review metadata.

## [0.1.0] - 2026-09-15

- Initial containerized news-core candidate based on the r1-20260914 freeze.
- Five isolated roles: scheduler, ingest, process, validate, and report.
- Host-side search, feed, and LLM broker boundaries; delivery remains disabled.
- Runtime image uses a digest-pinned Python 3.11 slim base and a non-root user.
- Local validation covers the publication scanner, container contract, Python
  compilation, and Compose rendering.

This source release is delivered under the MIT License.  Container image
publication and provenance attestation remain separate, tag-gated actions.
