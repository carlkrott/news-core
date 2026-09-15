# syntax=docker/dockerfile:1.7
#
# Hardened single-image runtime for the delivery-disabled news pipeline.
#
# Closure contract:
#   * Base: official python:3.11-slim pinned by multi-arch index digest
#     (verified against registry-1.docker.io on 2026-09-14).  The
#     digest is hard-coded in the FROM directive so the build is
#     reproducible regardless of any caller ARG; the verifier asserts
#     this literal digest is present in the file.
#   * No package manager updates or installs. No pip / third-party
#     wheels. No additional runtime downloads. Only the pinned base
#     is pulled.
#   * Runtime tree is the union of /app/scripts/news_pipeline,
#     /app/scripts/news_container, bin/* entrypoints, and the sanitized
#     example runtime schedule. Tests, docs, host brokers, private
#     evidence, legacy wrappers, cache files, and staging manifests
#     are excluded by the explicit COPY list below.
#   * Default user is non-root. No published ports, no network
#     capability; the runtime containers override network_mode=none,
#     read_only, cap_drop ALL, no-new-privileges, tmpfs /tmp, and
#     bounded pids/memory/cpu through compose.yaml.

FROM python:3.11-slim@sha256:9534e5a8e315485d4061ed659af0fd78a284c015f9b73661b41d6bab25604534 AS runtime

# CI supplies the repository, commit, and release version.  The explicit
# placeholders keep local builds truthful before the repository is published.
ARG OCI_SOURCE="unpublished"
ARG OCI_REVISION="unreleased"
ARG OCI_VERSION="0.1.0"
ARG OCI_LICENSE="MIT"
ARG OCI_VENDOR="unpublished"

# Labels are pinned to this candidate.  They describe provenance and
# the closure contract; the verifier asserts them.
LABEL org.opencontainers.image.title="news-pipeline-container-runtime" \
      org.opencontainers.image.description="Delivery-disabled news pipeline runtime; AF_UNIX broker only; no network." \
      org.opencontainers.image.source="${OCI_SOURCE}" \
      org.opencontainers.image.revision="${OCI_REVISION}" \
      org.opencontainers.image.version="${OCI_VERSION}" \
      org.opencontainers.image.licenses="${OCI_LICENSE}" \
      org.opencontainers.image.vendor="${OCI_VENDOR}" \
      news.container.base_digest="sha256:9534e5a8e315485d4061ed659af0fd78a284c015f9b73661b41d6bab25604534" \
      news.container.base_image="docker.io/library/python:3.11-slim" \
      news.container.delivery_disabled="true"

# Default working tree under /app.  Only the runtime modules land here
# (see COPY allowlist below).  /state and /artifacts are runtime
# bind-mount targets; tmpfs is requested at the Compose layer.
WORKDIR /app

# Copy only the runtime tree.  Each path is listed explicitly so the
# final layer cannot accidentally absorb tests/, docs/, host/,
# private-evidence/, legacy-bin/, or staging manifests.
#
# /app/scripts/news_pipeline   - Phase 1..6 runtime pipeline code
# /app/scripts/news_container - scheduler, control store, worker,
#                               broker protocol, broker client
# /app/config                 - sanitized example configs (live policy
#                               lives on the host bind mount)
# /app/bin                    - role entrypoints and dispatch wrapper
COPY scripts/news_pipeline /app/scripts/news_pipeline
COPY scripts/news_container /app/scripts/news_container
COPY config/news-policy.example.toml /app/config/news-policy.toml
COPY config/news-sources.example.toml /app/config/news-sources.toml
COPY config/news-topics.example.toml /app/config/news-topics.toml
COPY config/runtime-schedule.example.toml /app/config/runtime-schedule.toml
COPY bin /app/bin

# Non-root runtime user.  Compose may override with a UID that
# matches the host canary directory; the portable default keeps
# numeric UID 65532 (distroless-style "nonroot" id).
#
# The /usr/bin/python3 symlink keeps the pre-existing news-* bin
# wrappers operational inside the image without modifying those
# files (the wrappers hard-code /usr/bin/python3; the slim base
# installs python3 at /usr/local/bin/python3).
RUN groupadd --system --gid 65532 ncnrun \
    && useradd  --system --uid 65532 --gid ncnrun \
                --home-dir /app --shell /usr/sbin/nologin \
                ncnrun \
    && ln -sf /usr/local/bin/python3.11 /usr/bin/python3 \
    && chmod -R a+rX /app \
    && chmod a+rx /app/bin/*

USER ncnrun

# Compose services run as one-shot commands; the entrypoint is the
# dispatcher that maps $NEWS_CONTAINER_ROLE -> scheduler/worker module
# exec and fails closed on any delivery/production sentinel.
ENV NEWS_CONTAINER_MODE=1 \
    NEWS_PIPELINE_CODE_ROOT=/app \
    PYTHONPATH=/app/scripts \
    PATH=/app/bin:/usr/local/bin:/usr/bin:/bin \
    GPG_KEY=""

ENTRYPOINT ["/app/bin/container-entrypoint"]
CMD ["scheduler"]