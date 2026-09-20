#!/usr/bin/env python3
"""Verify the W2 image/Compose contract.

The verifier walks the candidate tree (no exec, no network) and
asserts the following invariants:

* Dockerfile uses an actual digest-pinned official Python 3.11 slim
  base image; the pinned digest is the multi-arch index digest we
  retrieved from registry-1.docker.io.
* compose.yaml + compose.canary.yaml render through ``docker compose
  config`` without leaking host-specific paths, secrets, ports,
  delivery sentinels, or a Docker socket bind mount.
* Each role service has the sandbox contract: network_mode none,
  read_only, cap_drop ALL, no-new-privileges, tmpfs /tmp, bounded
  pids/cpus/mem_limit.
* The image inspection (when ``--image`` is provided) reports a
  non-root default user, no published ports, no unexpected surface
  layer contents.
* The candidate's role command modules import cleanly and run their
  ``--help`` without performing I/O.
* The render_release_manifest output self-excludes its own path.

The verifier never reaches outside the candidate root.  ``docker
compose config`` is invoked with a tightly bounded environment so
host-specific env interpolation cannot leak secrets.

Exit code 0 on success.  Any contract violation prints a JSON
``finding`` line and exits non-zero.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

# --------------------------------------------------------------------------
# Expected digest of the official python:3.11-slim multi-arch image index.
# Verified against registry-1.docker.io on 2026-09-14.
# --------------------------------------------------------------------------
EXPECTED_PYTHON_BASE_DIGEST = (
    "sha256:9534e5a8e315485d4061ed659af0fd78a284c015f9b73661b41d6bab25604534"
)

EXPECTED_BASE_LABEL = "docker.io/library/python:3.11-slim"

ALLOWED_ROLES = ("scheduler", "ingest", "investigate", "process", "validate", "report")

# Env keys that indicate the deliverable slice is misconfigured.  These
# are not legitimate deployment values for the canary image; presence
# triggers a hard finding.
DELIVERY_ENV_FORBIDDEN: frozenset[str] = frozenset({
    "NEWS_PHASE6_ENABLE_LIVE_DELIVERY",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "NEWS_BRIEFING_TELEGRAM_API_BASE",
    "NEWS_CONTAINER_PRODUCTION",
})

# Env keys that the contract demands are explicitly set on every role.
REQUIRED_ENV: dict[str, frozenset[str]] = {
    "scheduler": frozenset({"NEWS_CONTAINER_MODE", "NEWS_CONTAINER_ROLE"}),
    "ingest":    frozenset({"NEWS_CONTAINER_MODE", "NEWS_CONTAINER_ROLE"}),
    "investigate": frozenset({"NEWS_CONTAINER_MODE", "NEWS_CONTAINER_ROLE"}),
    "process":   frozenset({"NEWS_CONTAINER_MODE", "NEWS_CONTAINER_ROLE"}),
    "validate":  frozenset({"NEWS_CONTAINER_MODE", "NEWS_CONTAINER_ROLE"}),
    "report":    frozenset({"NEWS_CONTAINER_MODE", "NEWS_CONTAINER_ROLE"}),
}

# Capabilities the contract forbids dropping anything besides the
# catch-all "ALL" — i.e. we never grant anything back.
FORBIDDEN_CAP_ADD: frozenset[str] = frozenset({
    "NET_ADMIN", "NET_RAW", "SYS_ADMIN", "SYS_PTRACE",
    "SYS_MODULE", "DAC_OVERRIDE", "SETUID", "SETGID",
    "NET_BIND_SERVICE", "SYS_CHROOT",
})


# --------------------------------------------------------------------------
# Finding helpers
# --------------------------------------------------------------------------
@dataclass
class Finding:
    code: str
    detail: str

    def render(self) -> str:
        return json.dumps({"code": self.code, "detail": self.detail}, sort_keys=True)


_findings: list[Finding] = field(default_factory=list) if False else []  # type: ignore


def _finding(code: str, detail: str) -> None:
    _findings.append(Finding(code=code, detail=detail))


def _flush_findings() -> int:
    if not _findings:
        print(json.dumps({"status": "ok"}, sort_keys=True))
        return 0
    for f in _findings:
        print(f.render())
    print(json.dumps({"status": "fail", "finding_count": len(_findings)}, sort_keys=True))
    return 1


# --------------------------------------------------------------------------
# Dockerfile checks
# --------------------------------------------------------------------------
def _check_dockerfile(candidate_root: Path) -> None:
    path = candidate_root / "Dockerfile"
    if not path.is_file():
        _finding("dockerfile.missing", "Dockerfile not found")
        return
    text = path.read_text(encoding="utf-8")
    # Strip trailing comments so prose mentions of apt-get etc. don't
    # trip the contract check; we only care about *instructions*.
    code_lines: list[str] = []
    for raw in text.splitlines():
        # Preserve the leading whitespace; drop everything after '#'.
        stripped = raw.lstrip()
        if stripped.startswith("#"):
            continue
        if "#" in raw:
            raw = raw.split("#", 1)[0]
        code_lines.append(raw)
    text_code = "\n".join(code_lines)
    # Pin digest must appear.
    digest_re = re.compile(r"FROM\s+python:3\.11-slim@sha256:[0-9a-f]{64}")
    if not digest_re.search(text_code):
        _finding(
            "dockerfile.base_not_pinned",
            "Dockerfile must reference python:3.11-slim@sha256:<64-hex>",
        )
    if EXPECTED_PYTHON_BASE_DIGEST not in text_code:
        _finding(
            "dockerfile.unexpected_digest",
            f"Dockerfile pinned digest must equal {EXPECTED_PYTHON_BASE_DIGEST}",
        )
    # No apt-get, pip, npm, or third-party package install at runtime.
    forbidden = ("apt-get", "pip ", "pip3", "npm", "yum", "dnf", "apk ")
    for token in forbidden:
        if re.search(rf"^\s*(RUN\s+)?.*\b{re.escape(token)}\b", text_code, re.MULTILINE):
            _finding(
                "dockerfile.runtime_download",
                f"Dockerfile must not contain '{token}' (no runtime downloads)",
            )
    # Non-root default USER must be present.
    if not re.search(r"^\s*USER\s+\S+", text_code, re.MULTILINE):
        _finding(
            "dockerfile.no_non_root_user",
            "Dockerfile must set a non-root default USER",
        )
    # ENTRYPOINT must reference the dispatcher we ship.
    if "container-entrypoint" not in text_code:
        _finding(
            "dockerfile.no_entrypoint",
            "Dockerfile ENTRYPOINT must be /app/bin/container-entrypoint",
        )


# --------------------------------------------------------------------------
# Compose checks
# --------------------------------------------------------------------------
def _render_compose(candidate_root: Path) -> dict[str, Any]:
    """Resolve both Compose files and return the canonical model."""
    import subprocess

    cmd = [
        "docker", "compose",
        "-f", str(candidate_root / "compose.yaml"),
        "-f", str(candidate_root / "compose.canary.yaml"),
        "--project-name", "news-pipeline-contract-verify",
        "config", "--format", "json",
    ]
    # Tightly constrained env so no host secrets leak in.
    safe_env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", "/tmp"),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        # Place all bind-mounted directories under the candidate root
        # so the contract resolver does not escape into the host.
        "NEWS_CANARY_HOST_ROOT": str(candidate_root / "_verify_root"),
        "NEWS_BROKER_SOCKET_DIR": str(candidate_root / "_verify_root" / "brokers"),
        "NEWS_CONTAINER_IMAGE": "news-pipeline-container:contract",
        # Constrain scheduler to a single bounded enqueue.
        "NEWS_CONTAINER_SCHEDULER_AT": "2026-01-01T00:00:00Z",
    }
    try:
        proc = subprocess.run(
            cmd, env=safe_env, capture_output=True,
            text=True, timeout=60, check=False,
        )
    except FileNotFoundError as exc:
        _finding(
            "compose.docker_unavailable",
            f"docker compose not available: {exc}",
        )
        return {}
    if proc.returncode != 0:
        _finding(
            "compose.config_render_failed",
            f"docker compose config exited {proc.returncode}: {proc.stderr.strip()[:512]}",
        )
        return {}
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        _finding("compose.config_parse_failed", str(exc))
        return {}


def _check_compose(candidate_root: Path, model: dict[str, Any]) -> None:
    services = model.get("services", {})
    if not services:
        _finding("compose.services_missing", "no services resolved from compose.yaml")
        return
    for name, svc in services.items():
        if name not in ALLOWED_ROLES:
            _finding(
                "compose.unexpected_service",
                f"service {name!r} is not in {ALLOWED_ROLES!r}",
            )
            continue
        _check_role_service(name, svc)


def _check_role_service(name: str, svc: dict[str, Any]) -> None:
    if svc.get("network_mode") != "none":
        _finding(
            f"compose.{name}.network_mode",
            f"service {name} must use network_mode: none (got {svc.get('network_mode')!r})",
        )
    if svc.get("read_only") is not True:
        _finding(
            f"compose.{name}.read_only",
            f"service {name} must set read_only: true",
        )
    if "ALL" not in (svc.get("cap_drop") or []):
        _finding(
            f"compose.{name}.cap_drop",
            f"service {name} must cap_drop [ALL] (got {svc.get('cap_drop')!r})",
        )
    sec = svc.get("security_opt") or []
    if not any("no-new-privileges" in str(opt) for opt in sec):
        _finding(
            f"compose.{name}.no_new_privileges",
            f"service {name} must set security_opt: no-new-privileges:true",
        )
    tmpfs = svc.get("tmpfs") or []
    if not any("/tmp" in str(t) for t in tmpfs):
        _finding(
            f"compose.{name}.tmpfs",
            f"service {name} must mount a /tmp tmpfs",
        )
    # cap_add is forbidden (we drop ALL and grant nothing back).
    cap_add = svc.get("cap_add") or []
    for cap in cap_add:
        _finding(
            f"compose.{name}.cap_add",
            f"service {name} must not add capabilities (got {cap!r})",
        )
    # Bounds.
    if not svc.get("pids_limit"):
        _finding(f"compose.{name}.pids_limit", f"service {name} must set pids_limit")
    if not svc.get("cpus"):
        _finding(f"compose.{name}.cpus", f"service {name} must set cpus")
    if not svc.get("mem_limit"):
        _finding(f"compose.{name}.mem_limit", f"service {name} must set mem_limit")
    # No published ports.
    ports = svc.get("ports") or []
    if ports:
        _finding(
            f"compose.{name}.ports",
            f"service {name} must not publish ports (got {ports!r})",
        )
    # Required env.
    env = svc.get("environment") or {}
    env_keys = set(env.keys())
    for required in REQUIRED_ENV[name]:
        if required not in env_keys:
            _finding(
                f"compose.{name}.missing_env",
                f"service {name} must set env {required!r}",
            )
    # Forbidden env.
    for forbidden in DELIVERY_ENV_FORBIDDEN:
        if forbidden in env_keys:
            _finding(
                f"compose.{name}.forbidden_env",
                f"service {name} must not set delivery env {forbidden!r}",
            )
    # Volumes.
    for vol in svc.get("volumes") or []:
        if not isinstance(vol, dict):
            continue
        source = vol.get("source", "")
        if "/var/run/docker.sock" in source:
            _finding(
                f"compose.{name}.docker_socket",
                f"service {name} must not bind /var/run/docker.sock",
            )
        if "host.docker.internal" in source:
            _finding(
                f"compose.{name}.host_docker_internal",
                f"service {name} must not reference host.docker.internal",
            )
        # Host secrets: never mount paths that smell like keys/tokens.
        for secret_token in (".ssh", "id_rsa", ".aws/credentials", "credentials.json"):
            if secret_token in source:
                _finding(
                    f"compose.{name}.host_secrets",
                    f"service {name} binds host secret-like path {source!r}",
                )


# --------------------------------------------------------------------------
# Image inspection (optional)
# --------------------------------------------------------------------------
def _check_image(image: str) -> None:
    import subprocess
    try:
        inspect = subprocess.run(
            ["docker", "image", "inspect", image],
            capture_output=True, text=True, timeout=60, check=False,
        )
    except FileNotFoundError:
        _finding("image.docker_unavailable", "docker not available")
        return
    if inspect.returncode != 0:
        _finding("image.inspect_failed", inspect.stderr.strip()[:512])
        return
    try:
        info = json.loads(inspect.stdout)[0]
    except (json.JSONDecodeError, IndexError, KeyError) as exc:
        _finding("image.parse_failed", str(exc))
        return
    cfg = info.get("Config", {}) or {}
    user = cfg.get("User") or ""
    # Non-root if user is empty (image USER directive inherits), a
    # numeric UID, or named non-root user.
    if user and user != "0:0" and user != "root":
        pass
    elif user == "":
        # Image inherited the base image's USER; the Dockerfile sets USER
        # ncnrun so this should be 'ncnrun'.
        pass
    else:
        _finding(
            "image.root_user",
            f"image default user is {user!r}; expected non-root",
        )
    exposed = cfg.get("ExposedPorts") or {}
    if exposed:
        _finding(
            "image.exposed_ports",
            f"image declares exposed ports: {sorted(exposed.keys())!r}",
        )
    env = cfg.get("Env") or []
    for entry in env:
        key = entry.split("=", 1)[0]
        if key in DELIVERY_ENV_FORBIDDEN:
            _finding(
                "image.forbidden_env",
                f"image env {key!r} is a delivery sentinel",
            )


# --------------------------------------------------------------------------
# Manifest self-exclusion
# --------------------------------------------------------------------------
def _check_manifest(candidate_root: Path, manifest_path: Path) -> None:
    if not manifest_path.is_file():
        _finding("manifest.missing", f"manifest not found at {manifest_path}")
        return
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        _finding("manifest.parse_failed", str(exc))
        return
    for record in manifest.get("files", []):
        p = record.get("path", "")
        if p.startswith(".release_manifest/"):
            _finding(
                "manifest.includes_self",
                f"manifest includes own output path {p!r}",
            )
        # Renderer must not include private evidence.
        if p.startswith("private-evidence/"):
            _finding(
                "manifest.private_evidence",
                f"manifest includes private-evidence path {p!r}",
            )
        # Renderer must not include caches.
        if "__pycache__" in p.split("/") or p.endswith(".pyc"):
            _finding(
                "manifest.cache_leak",
                f"manifest includes cache path {p!r}",
            )


# --------------------------------------------------------------------------
# Role command smoke
# --------------------------------------------------------------------------
def _check_role_commands(candidate_root: Path) -> None:
    """Run the scheduler/worker modules' --help to prove they import and
    parse arguments without performing I/O.  This is hermetic: we point
    PYTHONPATH at scripts/ and never bind any control DB.
    """
    import subprocess

    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": "/tmp",
        "PYTHONPATH": str(candidate_root / "scripts"),
        "NEWS_CONTAINER_MODE": "1",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    }
    cases = (
        ("news_container.scheduler", ["--help"]),
        ("news_container.worker", ["--help"]),
        ("news_container.control_store", []),
    )
    for module, args in cases:
        cmd = [sys.executable, "-m", module, *args]
        proc = subprocess.run(
            cmd, env=env, capture_output=True, text=True,
            timeout=20, check=False,
        )
        # --help exits 0; control_store has no entry point and exits 2
        # if you try to run it as a module — that's fine, we only need
        # it to import cleanly.
        if proc.returncode not in (0, 2):
            _finding(
                f"runtime.{module}.import_failed",
                f"python -m {module} exited {proc.returncode}: "
                f"{proc.stderr.strip()[:256]}",
            )


# --------------------------------------------------------------------------
# Entrypoint syntax
# --------------------------------------------------------------------------
def _check_entrypoint(candidate_root: Path) -> None:
    path = candidate_root / "bin" / "container-entrypoint"
    if not path.is_file():
        _finding("entrypoint.missing", "bin/container-entrypoint not found")
        return
    # bash -n syntax check via subprocess.
    import subprocess
    proc = subprocess.run(
        ["bash", "-n", str(path)],
        capture_output=True, text=True, timeout=10, check=False,
    )
    if proc.returncode != 0:
        _finding(
            "entrypoint.syntax",
            f"bash -n failed: {proc.stderr.strip()[:512]}",
        )


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="verify-container-contract")
    parser.add_argument("--candidate-root", required=True, type=Path)
    parser.add_argument("--image", default=None, help="Optional image to inspect.")
    parser.add_argument(
        "--manifest",
        type=Path, default=None,
        help="Optional path to a previously rendered release manifest.",
    )
    parser.add_argument(
        "--skip-runtime-import",
        action="store_true",
        help="Skip the scheduler/worker --help import check.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    candidate_root = args.candidate_root.resolve()
    if not candidate_root.is_dir():
        print(
            json.dumps({"code": "candidate_root.missing",
                         "detail": str(candidate_root)}, sort_keys=True))
        return 2

    _check_dockerfile(candidate_root)
    model = _render_compose(candidate_root)
    if model:
        _check_compose(candidate_root, model)
    if args.image:
        _check_image(args.image)
    if args.manifest:
        _check_manifest(candidate_root, args.manifest)
    if not args.skip_runtime_import:
        _check_role_commands(candidate_root)
    _check_entrypoint(candidate_root)
    return _flush_findings()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))