# Security

> Threat model, trust boundaries, and disclosure policy for the
> news-core public export.

## 1. Threat model in one paragraph

The news core runs untrusted-by-default: every application container
has `network_mode: none`, no published ports, a read-only root
filesystem, all capabilities dropped, `no-new-privileges`, and a
non-root default user.  The only way out of the container is via
host-side Unix-domain-socket brokers whose caller authorization is
the Unix-socket filesystem ownership.  The brokers enforce a closed
TOML allowlist, HTTPS-only, DNS / IP / redirect / private-address
checks on every hop, bounded request and response sizes, and never
log URL query values, request/response bodies, headers, prompts,
tokens, chat IDs, or report text.  Delivery (`telegram`, chat IDs,
tokens) is hard-disabled in policy, in the environment, in the
mounted socket set, and in the canary command.

The model assumes:

* The host filesystem and Docker daemon are trusted.
* The operator is trusted to keep Tailnet / LAN / credential /
  state paths off the publication surface.
* The public core has no bundled credentials, no operator
  identifiers, and no private endpoints.

## 2. What this export never contains

The publication-safety scanner (`scripts/check_publication_safety.py`)
enforces an exact allowlist.  Every one of the following is a
hard reject:

* `private-evidence/` (operator evidence bundle, source-seed JSON).
* Runtime databases (`*.db`, `*.sqlite`, `*.sqlite3`) and their
  `*-wal` / `*-shm` sidecars.
* Log files, bytecode (`*.pyc`, `*.pyo`, `*.pyd`), Python caches
  (`__pycache__`, `.pytest_cache`, `.mypy_cache`, `.ruff_cache`).
* `STAGING_MANIFEST*`, `staging_manifest*`, `legacy-bin/`.
* Environment files (`.env`, `.env.*`, `.envrc`), credentials
  (`credentials*`, `secrets*`, `*.json` / `*.toml` named `secret*`),
  keys (`*.pem`, `*.key`, `*.crt`, `*.cer`, `*.pfx`, `*.p12`),
  SSH identities (`id_rsa`, `id_ed25519`, `id_ecdsa`, `id_dsa`,
  `authorized_keys`, `known_hosts`).
* Operator home paths (`/home/<user>`, `/home/korphaus`,
  `/Users/korphaus`, `/Users/<user>`).
* Host identities (`korphaus`, `carl`, `zeroclaw`, `.zeroclaw`,
  `Zeroclaw`).
* Private / Tailnet / LAN endpoints (`127.x.x.x`, `10.x.x.x`,
  `192.168.x.x`, `172.16-31.x.x`, `100.64-127.x.x` (CGNAT),
  `169.254.x.x` (link-local), `0.0.0.0`, `224-239.x.x.x` and
  `240-255.x.x.x` (multicast/reserved), `100.100.100.100`,
  `fd7a:115c:a1e0:`, `ts.net`, `tailnet`, `tailscale`,
  `headscale`).
* Bearer tokens, API keys, generic tokens, Telegram bot tokens,
  chat IDs, GitHub PATs, Slack tokens.
* Endpoints that are not in the documented `example.com` /
  `localhost` placeholder set.
* Symlinks and any non-regular files.
* Missing required docs (`README.md`, `ARCHITECTURE.md`,
  `SECURITY.md`, `CONTRIBUTING.md`, `BUILD.md`, `pyproject.toml`).

Synthetic secret-detector fixtures
(`phase6_searxng_token_fixture.json`,
`phase6_feed_token_fixture.xml`,
`phase6_telegram_token_fixture.toml`) are explicitly allowlisted as
fixtures inside the scanner; their basenames are recognised by the
scanner so the detector is exercised without exposing real secrets.

## 3. Trust boundaries

| Boundary                     | Inside                                              | Outside                                  |
|------------------------------|-----------------------------------------------------|------------------------------------------|
| Application container        | Python interpreter, `news_pipeline`, `news_container`. | Host filesystem, broker sockets, image registry. |
| Broker socket                | Application container (one role).                   | Host broker, destination service.        |
| `news-state.db`              | Role workers.                                       | Host backup tooling.                     |
| `runtime-control.db`         | Scheduler, role workers.                            | Host backup tooling.                     |
| Public export                | This repository.                                    | Operator private evidence, live config, installed host brokers, scheduler crontab. |

## 4. Container hardening

* `network_mode: none` -- no network namespace for any container.
* `cap_drop: [ALL]` -- no Linux capabilities.
* `no-new-privileges` -- setuid binaries are inert.
* `read_only: true` -- the root filesystem is immutable.
* `tmpfs: [/tmp]` -- ephemeral scratch space.
* `pids_limit`, `mem_limit`, `cpus` are bounded.
* `user:` defaults to a non-root UID; portable across hosts that
  honour UID mapping.  A host that requires container-root for
  bind-mount UID mapping must use a host-specific override with all
  capabilities dropped; the portable default remains non-root.
* `healthcheck` is an explicit one-shot command, not a network probe.
* No published ports; no `host.docker.internal`.

## 5. Broker hardening

* The Unix-socket path must be absolute, the parent directory a
  regular directory, the socket a non-symlink, and the owner /
  mode match what the role expects.
* Socket selection is role-driven, not caller-supplied.
* Search broker ignores the caller destination and always uses the
  configured loopback SearXNG.
* LLM broker ignores the caller destination and always uses the
  configured combined-Gemma endpoint with the forced model ID.
* Feed broker applies DNS / IP / redirect / private-address checks
  on every hop, rejects userinfo, normalises ports, and bounds the
  redirect count.
* Connect / read / total deadlines and a 64 KiB metadata cap are
  enforced at the broker and the client.

## 6. Reporting a vulnerability

Report vulnerabilities through GitHub's private vulnerability reporting:

`https://github.com/carlkrott/news-core/security/advisories/new`

Do **not** file a public GitHub issue, tweet, or otherwise disclose the
vulnerability publicly.  The repository owner will acknowledge reports
within a reasonable window and coordinate disclosure.

## 7. Out-of-scope hardening

* Defence against a compromised host kernel or a malicious Docker
  daemon is out of scope.
* Defence against an operator who deliberately publishes private
  paths is out of scope; the scanner exists precisely to make that
  visible before publication.
* Defence against physical access to the host is out of scope.

## 8. License

This repository is licensed under the MIT License.  See `LICENSE` for the
exact rights and conditions.
