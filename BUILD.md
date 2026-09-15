# Build

> Reproducible build for the news-core image, with all network egress
> confined to host-side Unix-domain-socket brokers.  This document is
> about the *Docker* build only; the *application* build is `pyproject.toml`.

## 1. Requirements

* Docker Engine with Compose v2.
* A rootless Docker context (the recommended default). Never target
  `/var/run/docker.sock` implicitly; select the context explicitly
  with `docker context use <rootless-context>`.
* Python 3.11+ in the build host (only required to run the
  publication-safety scanner and tests before building).

## 2. Build inputs

* `Dockerfile` -- digest-pinned Python slim base, no runtime
  package downloads, non-root default user, read-only filesystem.
* `compose.yaml` / `compose.canary.yaml` -- role-separated Compose
  topology; see `ARCHITECTURE.md`.
* `bin/container-entrypoint` -- the in-container entrypoint used by
  every role.
* `.dockerignore` -- excludes tests, docs, host brokers, private
  evidence, legacy scripts, staging manifests, caches, bytecode,
  environment files, keys, certificates, and live (non-example)
  configuration.

## 3. Image closure

The runtime image ships only:

* `scripts/news_pipeline/`
* `scripts/news_container/`
* `bin/news-*` entrypoints
* `bin/container-entrypoint`
* `config/*.example.toml` (sanitized placeholders only)
* `pyproject.toml` (read by Python at startup; no third-party
  packages are installed at build time)

The runtime image **does not** ship:

* `tests/`, `docs/`, `README.md`, `ARCHITECTURE.md`, `SECURITY.md`,
  `CONTRIBUTING.md`, `BUILD.md`.
* `scripts/check_publication_safety.py` and its tests.
* `private-evidence/`, `STAGING_MANIFEST*`, `legacy-bin/`.
* `.git`, `.gitignore`, `.gitattributes`.
* `__pycache__`, `*.pyc`, `*.pyo`, `*.pyd`, `.pytest_cache`,
  `.mypy_cache`, `.ruff_cache`.
* `*.db`, `*.db-wal`, `*.db-shm`, `*.sqlite*`, `*.log`.
* `.env`, `.envrc`, `*.pem`, `*.key`, `*.crt`, `*.cer`, `*.pfx`,
  `*.p12`, `credentials*`, `secrets*`.
* `config/news-policy.toml`, `config/news-sources.toml`,
  `config/news-topics.toml`, `config/runtime-schedule.toml`,
  `config/broker-policy.toml` (live, non-example configuration).

## 4. Image build

```bash
# 1. Verify the publication surface is clean.
python3 scripts/check_publication_safety.py .

# 2. Run the focused tests.
python3 -m unittest discover -s tests/news_pipeline -p test_publication_safety.py

# 3. Build the digest-pinned image.  Local builds keep truthful placeholder
#    provenance; CI/release builds must replace these with exact values.
docker build --pull \
  --build-arg OCI_SOURCE="unpublished" \
  --build-arg OCI_REVISION="unreleased" \
  --build-arg OCI_VERSION="0.1.0" \
  --build-arg OCI_LICENSE="MIT" \
  --build-arg OCI_VENDOR="unpublished" \
  -t news-core:dev .

# 4. Render Compose config without resolving secrets; secrets must
#    not appear in the rendered output.
docker compose -f compose.yaml config
```

The image label must include the source commit SHA on CI/release builds.
Local builds intentionally report `unreleased`.  Inspect with:

```bash
docker inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' news-core:dev
```

## 5. Image inspect checklist

* Default `User` is non-root (UID >= 1000).
* No non-empty credential environment variables beginning with
  `*_TOKEN`, `*_KEY`, `*_SECRET`, `TELEGRAM_*`; the inherited base-image
  `GPG_KEY` is explicitly neutralized in the final image.
* `ExposedPorts` is empty.
* `Healthcheck` is an explicit one-shot command (or absent) -- not a
  network probe.
* The filesystem does not contain any of the excluded paths listed
  in section 3.

## 6. Compose topology

| Service              | Image        | Command                                          | Network |
|----------------------|--------------|--------------------------------------------------|---------|
| `scheduler`          | `news-core`  | `python -m news_container.scheduler`             | none    |
| `ingest`             | `news-core`  | `python -m news_container.worker --kind ingest`  | none    |
| `process`            | `news-core`  | `python -m news_container.worker --kind process` | none    |
| `validate`           | `news-core`  | `python -m news_container.worker --kind validate`| none    |
| `report`             | `news-core`  | `python -m news_container.worker --kind report`  | none    |

Mounts are role-specific:

* Every service mounts a per-role control store path.
* The `scheduler` reads the runtime schedule file read-only.
* The `ingest` service reads the sanitized example configs read-only.
* The `report` service writes into a host-owned artifact directory.

The `delivery.sock` is never mounted, never created, never referenced.

## 7. Build without Compose

For ad-hoc verification:

```bash
# Run the scheduler one-shot.
docker run --rm \
  --network none \
  --read-only \
  --tmpfs /tmp:size=64m \
  -v "$PWD/runtime-control.db:/runtime-control.db:rw" \
  -v "$PWD/config/runtime-schedule.example.toml:/app/config/runtime-schedule.toml:ro" \
  news-core:dev \
  python -m news_container.scheduler --run-once
```

The verifier scripts (`scripts/render_release_manifest.py` and
`scripts/verify_container_contract.py`) and the Compose topology
(`compose.yaml`, `compose.canary.yaml`) are part of the public source
export.  They are source/build contracts only: the runtime image does not
carry the verifier scripts or documentation, and the live host policy and
state remain outside the repository.

## 8. Source manifest

A self-excluding candidate manifest with path, size, mode, and
SHA-256 is produced outside the public surface and never enters this
repository.  Public-side validation reuses only the publication-safety
scanner (`scripts/check_publication_safety.py`).

## 9. Out of scope

* Git / GitHub operations (commit, push, release, repo creation).
* License selection and SPDX claims.
* Live state migration, scheduler cutover, or production service
  activation.
* Delivery broker creation, mounting, or authorisation.
* Mounting credential files or the Docker socket.
