# news-core

> Portable **news-pipeline core** with delivery disabled by default.  MIT
> licensed, with no third-party runtime dependencies and no credential,
> host identity, or operator endpoint shipped.

This repository is the **public-export surface** of a larger news
pipeline.  It is intentionally narrow: only the code, sanitized
example configuration, build inputs, and supporting documentation
needed to build, run, and review the core.  Live delivery, operator
identities, host endpoints, credentials, and private evidence remain
outside this tree.

## What is in this repository

| Path                                         | Purpose                                                    |
|----------------------------------------------|------------------------------------------------------------|
| `scripts/news_pipeline/`                     | Application core (ingest, investigate, process, validate, report). |
| `scripts/news_container/`                    | Scheduler, role worker, control store, broker client/protocol. |
| `bin/news-*`                                 | Bash entrypoints that call into the application core.      |
| `scripts/check_publication_safety.py`        | Export-safety scanner (see below).                         |
| `scripts/export_public_tree.py`              | Closed-allowlist public source exporter.                   |
| `scripts/verify_container_contract.py`       | Docker/Compose contract verifier.                          |
| `CHANGELOG.md`                               | Public source history and release limitations.             |
| `tests/news_pipeline/test_publication_safety.py` | Focused publication-safety tests.                     |
| `config/*.example.toml`                      | Sanitized placeholder configuration.                       |
| `Dockerfile` / `compose*.yaml`               | Hardened image and six-role Compose topology.                |
| `host/`                                      | Public host-side egress broker and example policy.          |
| `deploy/systemd/`                            | Example systemd broker unit and environment template.      |
| `README.md` / `ARCHITECTURE.md` / ...        | Operator and reviewer documentation.                       |
| `.gitignore` / `.dockerignore` / `.gitattributes` | Source-control and build-context hygiene.             |
| `pyproject.toml`                             | Build metadata (zero runtime dependencies).                |

## What is deliberately *not* in this repository

- Live operator configuration (`config/news-*.toml`, `config/runtime-schedule.toml`).
- Live host brokers, installed systemd units, operator runbooks, or
  non-example deployment policy.
- Private evidence, staging manifests, runtime databases (`*.db`,
  `*.sqlite*`), WAL/SHM sidecars, log files, caches, or bytecode.
- Host identities, operator Tailnet / LAN endpoints, credentials,
  keys, certificates, or environment files.
- The bundled **Zeroclaw** scheduler/instance.  Zeroclaw is an
  *optional downstream adapter* and is not part of this core.  This
  repository does not include Zeroclaw's instance database, provider
  configuration, prompts, credentials, state, or generated content.

The public source export may contain broker implementation and systemd
templates, but those files are not live deployment authority.  The runtime
image contains only the application modules, sanitized examples, and
entrypoints listed in `Dockerfile`; tests, documentation, host brokers, and
publication tooling stay outside the image layer.

## Architecture in one paragraph

The news core is a Python 3.11+ stdlib application split into two
parts: `news_pipeline` (ingest → investigate → process → validate → report) and
`news_container` (a scheduler + role workers with a control store).
Every application container runs with `network_mode: none`; the only
network egress path is via host-side Unix-domain-socket brokers
(`search.sock`, `feed.sock`, `llm.sock`).  The brokers enforce a closed
allowlist, HTTPS-only feeds, DNS/IP/redirect/private-address checks,
bounded response sizes, and a delivery-disabled queue vocabulary.
Delivery (`telegram`, chat IDs, tokens) is hard-disabled in policy,
environment, mounted sockets, and the canary command.  See
[`ARCHITECTURE.md`](ARCHITECTURE.md) for the full topology, role
separation, and trust boundaries.

## Subject contracts

The public configuration keeps discovery categories separate and maps them to
one report subject only after deterministic routing.  `audiovisual` and
`av_corporate` both feed the `professional_av` subject; they are not mixed
with `audio_engineering` during discovery or investigation.  Audio Engineering
covers recording, production, broadcast, touring, and live sound.  Professional
AV covers corporate and installed AV, conferencing, control, displays, video,
stage technology, audio systems for those spaces, and lighting.  A candidate
with genuinely ambiguous subject evidence is held as
`pending_subject_review` rather than copied into multiple briefings.

The example contracts are version 2 and define per-subject report scope,
subject inclusion/exclusion rules, recency windows, materiality rules, and
story caps.  They are placeholders only; live source registries, operator
bindings, state, and credentials are not part of this repository.

## Subject delivery contract

Schema v9 adds public, additive contracts for subject report revisions, an
outbox, and exact delivery attempts.  Identity includes the parent report,
subject, and rendered-content hash, so replaying identical content is a no-op
while changed content becomes a new immutable revision.  Generation persists
the report/outbox boundary only; the separate `daily-deliver` action consumes
an already-generated report and never invokes model or report generation.

The public scheduler, control store, and workers still reject the operator-only
`delivery` kind.  Live subject schedules, recipient/channel bindings,
credentials, and the private delivery adapter remain outside this repository.
No schema migration or delivery activation occurs merely by building or
running the public source.

## Quality audit and disposable rehearsal

`python -m news_pipeline.quality_audit --db PATH [--receipts PATH]` opens an
existing schema-v9 SQLite database in read-only/query-only mode and emits
deterministic count-only JSON.  Receipt-only metrics are reported as
unavailable rather than inferred when `--receipts` is omitted.  The report does
not emit article bodies, URLs, prompts, generated copy, paths, recipients, or
delivery identifiers.  `absent_database_persistence` separately names metrics
that require typed receipts even when a receipt was supplied.

`python -m news_pipeline.synthetic_rehearsal --work-root NEW_DIR
--as-of-utc 2026-09-20T12:00:00Z` creates a disposable synthetic database and
artifact tree.  It exercises discovery receipts, freshness QC, investigation,
provenance, verification, eventing, per-subject editorial QC, replay, one
material update, and prepared subject outboxes.  Its transports are injected
in-process fixtures: it performs no external network, model, delivery, schema
migration against live state, schedule change, or service operation.  The
target directory must be empty, and every delivery-attempt count must remain
zero.

## Quick start (portable, no live delivery)

```bash
# Verify the publication surface is clean before doing anything else.
python3 scripts/check_publication_safety.py .

# Run the focused tests with stdlib unittest (no pytest required).
python3 -m unittest discover -s tests/news_pipeline -p test_publication_safety.py

# Compile-check the source tree.
python3 -m compileall -q scripts tests
```

The full Docker build path is described in
[`BUILD.md`](BUILD.md).  The runtime image uses one digest-pinned
Python slim base, a non-root default user, `cap_drop: [ALL]`,
`no-new-privileges`, and `network_mode: none`.

## Reporting issues

Use normal GitHub Issues for non-security bugs and feature requests.
Report security vulnerabilities through the private channel documented in
`SECURITY.md`; do **not** disclose vulnerabilities in a public issue.

## License

This repository is licensed under the MIT License.  See `LICENSE` for the
full text.  See `SECURITY.md` for the private disclosure policy and
`CONTRIBUTING.md` for the contribution process.
