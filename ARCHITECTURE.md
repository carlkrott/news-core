# Architecture

> Trust boundaries, role separation, and the Unix-domain-socket broker
> topology that lets a `network_mode: none` application container do
> useful work without ever opening a network namespace.

## 1. Application surface

The news core is a Python 3.11+ stdlib application with **zero
third-party runtime dependencies**.  It is split into two
top-level packages:

| Package                  | Responsibility                                                |
|--------------------------|---------------------------------------------------------------|
| `scripts/news_pipeline/` | Domain logic: ingest, canonicalize, cluster, fact-extract, claim, validate, report. |
| `scripts/news_container/`| Scheduler, role workers, control store, broker client & protocol. |

The public source export carries the application, broker implementation,
sanitized examples, Compose definitions, systemd templates, tests, and
publication tooling.  The runtime image carries only `news_pipeline`,
`news_container`, the sanitized runtime configuration, and the bash
entrypoints under `bin/`.  Tests, docs, host brokers, publication tooling,
private evidence, legacy scripts, and staging manifests are excluded from
the image layer by the explicit Dockerfile `COPY` list and `.dockerignore`.

Bash entrypoints use `NEWS_PIPELINE_CODE_ROOT` (default `/app`),
`NEWS_PIPELINE_DB` (default `/state/news-state.db`), and
`NEWS_PIPELINE_ARTIFACT_ROOT` (default `/artifacts`) so the same
entrypoints run in development, in the container image, and in CI.

## 2. Compose roles

One digest-pinned image runs six distinct Compose services.  Each
service executes a single command against a narrow subset of mounts
and is allowed to talk only to the brokers its role requires.

| Compose service      | Command                                          | Reads / writes                                 | Brokers |
|----------------------|--------------------------------------------------|------------------------------------------------|---------|
| `scheduler`          | `python -m news_container.scheduler`             | runtime control DB (RW), schedule (RO)         | none    |
| `ingest`             | `python -m news_container.worker --kind ingest`  | canary DB (RW), configs (RO), control (RW)     | search + feed |
| `investigate`        | `python -m news_container.worker --kind investigate` | canary DB (RW), control (RW)                | search + feed |
| `process`            | `python -m news_container.worker --kind process` | canary DB (RW), control (RW)                   | none    |
| `validate`           | `python -m news_container.worker --kind validate`| canary DB (RO/RW only as required for control receipt) | none |
| `report`             | `python -m news_container.worker --kind report`  | canary DB (RW), artifact root (RW), control (RW)| none    |

All services run with:

* `network_mode: none`
* a read-only root filesystem
* `cap_drop: [ALL]`
* `no-new-privileges`
* bounded memory, PIDs, and CPU
* a private tmpfs
* either an explicit one-shot command or a bounded healthcheck

No service publishes a port.  No service mounts the Docker socket.

## 3. Host broker topology

Every external call is mediated by a host-side Unix-domain-socket
broker.  Brokers live outside the container; containers connect to
them over the filesystem.

| Socket          | Purpose                                                                | Caller-supplied? |
|-----------------|------------------------------------------------------------------------|------------------|
| `search.sock`   | Local SearXNG `/search`; bounded query parameters; JSON response.      | No               |
| `feed.sock`     | HTTPS feed fetch with a closed TOML allowlist; DNS/IP/redirect/private-address checks on every hop. | Host/URL from policy |
| `llm.sock`      | GPU Manager combined-Gemma `/v1/chat/completions`; forced model ID; bounded request/response. | No |
| `delivery.sock` | **Not created, mounted, or enabled** in the public surface.             | n/a              |

Broker authorization is enforced by Unix-socket filesystem ownership
and per-service mount selection: the `ingest` and `investigate` services
mount `search.sock` and `feed.sock`; only the roles that own the broker
routes can dial them.  No bearer token, JWT, or HTTP `Authorization`
header is required inside the container.

Broker logs record route, destination class, status, byte count,
elapsed time, and error class.  They **never** record URL query
values, request or response bodies, headers, prompts, tokens, chat
IDs, or report text.

## 4. Container networking policy

* **No direct container networking.**  Every container uses
  `network_mode: none`.  The container cannot reach public DNS,
  public HTTPS, metadata IPs, the Tailnet, the LAN, or the host
  gateway.
* **No published ports.**
* **No `host.docker.internal`, no Tailnet or LAN IPs in any env,
  config, or mount.**
* **No Docker socket mount.**  Containers cannot start sibling
  containers or touch the daemon.

## 5. State, backup, and restore

Production state (`news-state.db`) and report artifacts remain
untouched.  Canary state is created with `sqlite3.Connection.backup()`
into an owner-only staging root, with the artifact tree copied
preserving bytes and modes.  Manifests of logical counts, schema
markers, integrity/FK status, stable-row hashes, artifact hashes, and
zero-delivery counts accompany every snapshot.

Restore rehearsals verify integrity, FKs, schema, counts, stable
digests, and report artifact hashes before and after.  No canary path
is allowed to resolve beneath a production state path, a credentials
directory, or a host identity path.

## 6. Scheduler and worker contract

* The scheduler and workers share a dedicated `runtime-control.db`,
  separate from `news-state.db`.
* Scheduler inserts use deterministic `(kind, due_slot)` IDs under
  `BEGIN IMMEDIATE`; duplicate scheduled slots are no-ops.  Event-specific
  `investigate` jobs add a deterministic `job_key`, so multiple candidates
  due in the same slot coexist without weakening scheduled-task idempotency.
* Each worker claims only its fixed kind with owner, generation,
  claim expiry, and attempt count.
* Main DB work is protected by one shared `flock`; lock contention
  is recorded and retried without concurrent writes.
* Scheduler / worker outputs are bounded, sanitized JSON; output
  bodies are hashed, not persisted verbatim.
* `delivery` and `--enable-live-delivery` are absent from the queue
  vocabulary at every layer (policy, scheduler, worker, broker).
* The canary schedule and config are isolated and cannot point to
  production state paths.

### Publisher provenance and verification identity

Discovery adapter identity and publisher evidence identity are separate.  The
`source_items` adapter/source fields describe how a result was discovered;
post-v6 provenance tables record the normalized publisher host, reviewed
publisher family, effective evidence role, authority match, and classification
reason used for verification.  Unknown publishers are retained as
`unknown_reviewed`/discovery and cannot verify a claim by themselves.

A one-source primary claim requires an explicit authority match.  Multiple
articles from one reviewed publisher family count as one independence group;
contradictory evidence still produces `watchlist` rather than being promoted.
`config/news-provenance.example.toml` is sanitized documentation only.  The
public image copies that example under its runtime filename; an operator may
supply a private reviewed file through the optional provenance path, and that
file takes precedence without entering the export.  Live publisher rules and
operator bindings remain private deployment overlays and must never be
committed to the public repository.  Schema-v7 application is an additive,
replay-safe structural path; this change does not migrate a live DB, change
schedules, or activate delivery.

## 7. ZeroClaw integration (optional, downstream)

Zeroclaw is **not** a dependency of the news core.  If the operator
wants to bridge Zerocaw's scheduler or chat interface to the news
core, that bridge is a **downstream adapter**:

* the bridge must run outside this image,
* the bridge must not mount the Zeroclaw instance database, provider
  config, prompts, credentials, state, or generated content into any
  news-container service,
* the bridge is the bridge's maintainer's responsibility; this
  repository carries no bridge code, no Zerocaw instance, and no
  compatibility shim.

## 8. Scheduler cutover is a separate gate

The current user crontab and any existing scheduler rows for the
legacy news pipeline remain unchanged.  Cutting over to the
containerized scheduler is a separate, explicit user authorization
and is **not** part of this export surface.

## 9. Operator pre-publication checklist

1. `python3 scripts/check_publication_safety.py .` exits `0`.
2. `python3 -m unittest discover -s tests/news_pipeline -p test_publication_safety.py` passes.
3. `python3 -m compileall -q scripts tests` returns silently.
4. `pyproject.toml` declares `requires-python = ">=3.11"` and
   `dependencies = []`.
5. No `LICENSE` / SPDX / claim of license is present until the operator
   explicitly selects publication rights.

## 10. Subject isolation

Ingest categories remain separate so source results and investigation context
cannot contaminate one another.  The typed subject layer maps them as follows:

| Subject | Input categories | Editorial boundary |
|---|---|---|
| `world` | `world` | Consequential public events |
| `ai` | `ai` | Models, services, policy, safety, research, and infrastructure |
| `audio_engineering` | `audio_engineering` | Recording, production, broadcast, touring, and live sound |
| `professional_av` | `audiovisual`, `av_corporate` | Corporate/installed AV, control, display, video, stage, audio, and lighting |
| `hardware` | `hardware` | Computer hardware and systems |
| `fantasy_novel` | `fantasy_novel` | Fantasy publishing and industry |
| `our_setup` | `our_setup` | Generic self-hosted and media-stack relevance |

Reports use `per_subject` scope.  The two Professional AV input categories
may combine only after event-level QC; Audio Engineering remains separate.
If category evidence maps to more than one subject, the assignment is
`pending_subject_review` and is not reportable until resolved.

## 11. Feed lanes and one-story investigations

Each source query is normalized into one stable `feed_lane_id` derived from
the source, pipeline category, query text, and adapter categories.  A source
contract may cover one pipeline category only; a query seed outside that
scope is rejected.  Ingest persists the lane on the query plan, receipt, and
candidate link, so result lists from separate lanes cannot silently become a
mixed discovery context.

Eligible candidates enqueue an `investigate` task with one candidate ID, one
feed lane, one query plan, and a bounded round number.  The deterministic
investigation identity is reused on enqueue retries and lease reclaim.
Terminal receipts are immutable; failed or blocked work advances only through
an explicit next-round retry.  Targeted event-specific queries are persisted
with their terminal state, and lease-fenced writes cannot publish stale
results.  Investigation workers can use only the existing search and feed
brokers and, like every public role, have no delivery socket or live delivery
credential.

## 12. Admission quality and report URL completeness

Canonicalization is syntactic and route-aware.  Tracking parameters,
fragments, default ports, host case, IDNA, query ordering, and dot segments
are normalized deterministically.  Root, home, index, tag, and search routes
are rejected as non-article results; near-miss article paths such as
`/homepage`, `/tagged`, `/searchlight`, and `/indexing` are not rejected by
substring matching.

Adapters and the ingest persistence boundary both enforce the article-URL
contract.  Missing, malformed, mismatched, non-canonical, and non-article
URLs remain rejection/audit evidence and never become persisted article
records.  Ingest receipts expose URL and publication-date coverage per feed
lane, category, and mapped subject.

Phase 2 records explicit date evidence (`source`, `metadata`,
`observed_fallback`, `missing`, or `unparseable`) and recency status
(`fresh`, `future_clamped`, `stale`, `missing`, or `invalid`).  Candidates
without a URL are retained only as `audit_only` pending decisions; they do
not enter history matching, event creation, or reports.  Subject policy
`recency_days` is the authority for freshness and exact URL/identity history
lookbacks when the process job receives the three versioned configuration
paths.

Report selection requires a verified event to have at least one non-empty
canonical source URL through its event-claim provenance chain.  URL-incomplete
events are excluded before briefing and artifact linkage; delivery remains
disabled.

## 13. Event novelty and subject collision handling

Exact canonical-URL and title/identity history checks remain fast admission
prefilters.  Once an event relationship is known, the persisted event ID and
positive event version are the durable novelty authority.  A rewrite with no
grounded fact or state delta is suppressed and cannot append a new event
version.  A `material_update` must carry typed or structurally equivalent
grounded `FactDelta` records before the event store accepts the next version;
the expected version is fenced against the current durable version.

Briefing seen identity is a bounded SHA-256 key over the canonical
`(subject,event,version)` tuple and uses the existing
`shadow_briefing_seen` ledger; the canonical subject, event, and version remain
in the payload for audit and recovery.  The first payload remains
authoritative; a later payload for the same identity is retained in the
run-event audit trail but is not delivered again.  Cross-lane events receive
one deterministic primary subject and explicit secondary-subject suppression
reasons, preventing the same event-version from entering multiple subject
briefings.
