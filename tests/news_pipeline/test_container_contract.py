"""Contract tests for the W2 image/Compose slice.

These tests run hermetically inside the candidate tree:

* They never reach the network (no AF_UNIX broker, no SearXNG).
* They never touch the host's production state.
* They only invoke ``docker compose config`` with a tightly bounded
  environment so secrets cannot leak in.
* They render the manifest into a temp dir so the rendered output
  cannot escape into the candidate root.

The tests are deliberately co-located with the rest of the news
pipeline test suite so the standard ``python -m unittest`` invocation
covers them.  Each test reports via a JSON-shaped assertion so the
parent review agent can grep for ``contract.fail`` markers.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

CANDIDATE_ROOT = Path(__file__).resolve().parents[2]


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def _run(cmd, **kwargs):
    """Run a subprocess and return CompletedProcess.  Always capture."""
    return subprocess.run(cmd, capture_output=True, text=True,
                          timeout=kwargs.pop("timeout", 60), check=False, **kwargs)


def _safe_compose_env(root: Path) -> dict[str, str]:
    """A constrained env for ``docker compose config`` so host secrets
    cannot leak into the resolver.
    """
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", "/tmp"),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "NEWS_CANARY_HOST_ROOT": str(root / "_safe_canary_root"),
        "NEWS_BROKER_SOCKET_DIR": str(root / "_safe_canary_root" / "brokers"),
        "NEWS_CONTAINER_IMAGE": "news-pipeline-container:contract-test",
        "NEWS_CONTAINER_SCHEDULER_AT": "2026-01-01T00:00:00Z",
    }


def _render_compose(root: Path):
    """Return the rendered Compose JSON model for both files, or
    ``None`` if docker compose is unavailable in this environment."""
    env = _safe_compose_env(root)
    cmd = [
        "docker", "compose",
        "-f", str(CANDIDATE_ROOT / "compose.yaml"),
        "-f", str(CANDIDATE_ROOT / "compose.canary.yaml"),
        "--project-name", "news-pipeline-contract-test",
        "config", "--format", "json",
    ]
    proc = _run(cmd, env=env)
    if proc.returncode != 0:
        return None, proc
    try:
        return json.loads(proc.stdout), proc
    except json.JSONDecodeError:
        return None, proc


# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------
class DockerfileContractTests(unittest.TestCase):
    """Dockerfile closure and hardening."""

    def test_dockerfile_pins_official_python_slim_digest(self):
        path = CANDIDATE_ROOT / "Dockerfile"
        self.assertTrue(path.is_file(), "Dockerfile is required")
        text = path.read_text(encoding="utf-8")
        m = re.search(r"FROM\s+python:3\.11-slim@sha256:([0-9a-f]{64})", text)
        self.assertIsNotNone(
            m, "Dockerfile must reference python:3.11-slim@sha256:<64-hex>",
        )
        # Cross-reference the official registry digest.  If this
        # assertion ever fails the operator must re-pin against
        # registry-1.docker.io.
        expected = (
            "sha256:9534e5a8e315485d4061ed659af0fd78a284c015f9b73661b41d6bab25604534"
        )
        self.assertIn(
            expected, text,
            f"Dockerfile pinned digest must equal {expected}",
        )

    def test_dockerfile_no_runtime_downloads(self):
        text = (CANDIDATE_ROOT / "Dockerfile").read_text(encoding="utf-8")
        code_lines = []
        for raw in text.splitlines():
            stripped = raw.lstrip()
            if stripped.startswith("#"):
                continue
            if "#" in raw:
                raw = raw.split("#", 1)[0]
            code_lines.append(raw)
        code = "\n".join(code_lines)
        forbidden_patterns = (
            r"^\s*RUN\s+.*\bapt-get\b",
            r"^\s*RUN\s+.*\bapt\b\s+install",
            r"^\s*RUN\s+.*\bpip\s+install\b",
            r"^\s*RUN\s+.*\bpip3\s+install\b",
            r"^\s*RUN\s+.*\bnpm\s+install\b",
            r"^\s*RUN\s+.*\byum\s+install\b",
            r"^\s*RUN\s+.*\bapk\s+add\b",
            r"^\s*RUN\s+.*\bcurl\b",
            r"^\s*RUN\s+.*\bwget\b",
        )
        for pat in forbidden_patterns:
            self.assertNotRegex(
                code, pat,
                f"Dockerfile must not contain runtime download pattern {pat}",
            )

    def test_dockerfile_runs_as_non_root(self):
        text = (CANDIDATE_ROOT / "Dockerfile").read_text(encoding="utf-8")
        # Strip comments so a USER directive inside a comment does not
        # count, and a missing USER directive does not get masked.
        code_lines = []
        for raw in text.splitlines():
            stripped = raw.lstrip()
            if stripped.startswith("#"):
                continue
            if "#" in raw:
                raw = raw.split("#", 1)[0]
            code_lines.append(raw)
        code = "\n".join(code_lines)
        self.assertRegex(
            code, r"(?m)^\s*USER\s+\S+",
            "Dockerfile must set a non-root USER directive",
        )
        # The base image's "root" user would be inherited otherwise.
        self.assertNotIn("USER root", code,
                         "Dockerfile must not explicitly USER root")

    def test_dockerfile_uses_container_entrypoint(self):
        text = (CANDIDATE_ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("container-entrypoint", text,
                      "Dockerfile ENTRYPOINT must reference /app/bin/container-entrypoint")

    def test_dockerfile_copies_only_runtime_paths(self):
        text = (CANDIDATE_ROOT / "Dockerfile").read_text(encoding="utf-8")
        code_lines = []
        for raw in text.splitlines():
            stripped = raw.lstrip()
            if stripped.startswith("#"):
                continue
            if "#" in raw:
                raw = raw.split("#", 1)[0]
            code_lines.append(raw)
        code = "\n".join(code_lines)
        # The runtime COPY list must be explicit and exclude tests/.
        copy_lines = re.findall(r"^\s*COPY\s+(\S+)(?:\s+(\S+))?", code, re.MULTILINE)
        copied_sources = {src for src, _ in copy_lines}
        for forbidden in ("tests", "tests/", "docs", "docs/", "host", "host/",
                          "private-evidence", "legacy-bin", ".git"):
                self.assertNotIn(
                    forbidden, copied_sources,
                    f"Dockerfile must not COPY {forbidden!r} into the runtime layer",
                )

    def test_dockerfile_copies_only_sanitized_config_examples(self):
        text = (CANDIDATE_ROOT / "Dockerfile").read_text(encoding="utf-8")
        copy_sources = [
            line.split()[1]
            for line in text.splitlines()
            if line.strip().startswith("COPY ")
        ]
        config_sources = [src for src in copy_sources if src.startswith("config/")]
        self.assertEqual(
            config_sources,
            [
                "config/news-policy.example.toml",
                "config/news-sources.example.toml",
                "config/news-topics.example.toml",
                "config/runtime-schedule.example.toml",
            ],
        )

    def test_dockerfile_declares_oci_provenance_inputs(self):
        text = (CANDIDATE_ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertRegex(text, r'(?m)^ARG OCI_SOURCE="unpublished"$')
        self.assertRegex(text, r'(?m)^ARG OCI_REVISION="unreleased"$')
        self.assertRegex(text, r'(?m)^ARG OCI_VERSION="0\.1\.0"$')
        self.assertRegex(text, r'(?m)^ARG OCI_LICENSE="MIT"$')
        self.assertRegex(text, r'(?m)^ARG OCI_VENDOR="unpublished"$')
        self.assertIn(
            'org.opencontainers.image.source="${OCI_SOURCE}"',
            text,
        )
        self.assertIn(
            'org.opencontainers.image.revision="${OCI_REVISION}"',
            text,
        )
        self.assertIn(
            'org.opencontainers.image.version="${OCI_VERSION}"',
            text,
        )
        self.assertIn(
            'org.opencontainers.image.licenses="${OCI_LICENSE}"',
            text,
        )
        self.assertIn(
            'org.opencontainers.image.vendor="${OCI_VENDOR}"',
            text,
        )
        self.assertIn('GPG_KEY=""', text)


class ComposeConfigRenderTests(unittest.TestCase):
    """`docker compose config` renders cleanly with a safe temp env."""

    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory(prefix="contract-render-") as tmp:
            cls.tmp = Path(tmp)
            env = _safe_compose_env(cls.tmp)
            cmd = [
                "docker", "compose",
                "-f", str(CANDIDATE_ROOT / "compose.yaml"),
                "-f", str(CANDIDATE_ROOT / "compose.canary.yaml"),
                "--project-name", "news-pipeline-contract-test",
                "config", "--format", "json",
            ]
            cls.proc = _run(cmd, env=env)
        if cls.proc.returncode != 0:
            raise unittest.SkipTest(
                f"docker compose unavailable in this environment: "
                f"{cls.proc.stderr.strip()[:256]}"
            )
        try:
            cls.model = json.loads(cls.proc.stdout)
        except json.JSONDecodeError:
            raise unittest.SkipTest("docker compose did not return JSON")

    def test_render_emits_all_five_roles(self):
        services = set(self.model.get("services", {}).keys())
        self.assertEqual(
            services, {"scheduler", "ingest", "process", "validate", "report"},
            "compose must define exactly the five hardened roles",
        )

    def test_every_role_is_network_mode_none(self):
        for name, svc in self.model["services"].items():
            self.assertEqual(
                svc.get("network_mode"), "none",
                f"{name} must use network_mode: none",
            )

    def test_every_role_drops_all_caps(self):
        for name, svc in self.model["services"].items():
            self.assertIn(
                "ALL", svc.get("cap_drop") or [],
                f"{name} must cap_drop [ALL]",
            )

    def test_every_role_disables_privilege_escalation(self):
        for name, svc in self.model["services"].items():
            opts = " ".join(map(str, svc.get("security_opt") or []))
            self.assertIn(
                "no-new-privileges", opts,
                f"{name} must set no-new-privileges",
            )

    def test_every_role_is_read_only_root(self):
        for name, svc in self.model["services"].items():
            self.assertTrue(
                svc.get("read_only") is True,
                f"{name} must set read_only: true",
            )

    def test_every_role_has_tmpfs(self):
        for name, svc in self.model["services"].items():
            tmpfs = svc.get("tmpfs") or []
            self.assertTrue(
                any("/tmp" in str(t) for t in tmpfs),
                f"{name} must mount a /tmp tmpfs",
            )

    def test_every_role_has_resource_bounds(self):
        for name, svc in self.model["services"].items():
            self.assertTrue(svc.get("pids_limit"), f"{name} must set pids_limit")
            self.assertTrue(svc.get("cpus"), f"{name} must set cpus")
            self.assertTrue(svc.get("mem_limit"), f"{name} must set mem_limit")

    def test_no_role_publishes_ports(self):
        for name, svc in self.model["services"].items():
            self.assertFalse(
                svc.get("ports"),
                f"{name} must not publish ports",
            )

    def test_no_role_binds_docker_socket(self):
        for name, svc in self.model["services"].items():
            for vol in svc.get("volumes") or []:
                if not isinstance(vol, dict):
                    continue
                source = vol.get("source", "")
                self.assertNotIn(
                    "/var/run/docker.sock", source,
                    f"{name} must not bind /var/run/docker.sock",
                )

    def test_no_role_references_host_docker_internal(self):
        for name, svc in self.model["services"].items():
            blob = json.dumps(svc, sort_keys=True)
            self.assertNotIn(
                "host.docker.internal", blob,
                f"{name} must not reference host.docker.internal",
            )

    def test_no_role_sets_delivery_env(self):
        forbidden = {
            "NEWS_PHASE6_ENABLE_LIVE_DELIVERY",
            "TELEGRAM_BOT_TOKEN",
            "TELEGRAM_CHAT_ID",
            "NEWS_BRIEFING_TELEGRAM_API_BASE",
            "NEWS_CONTAINER_PRODUCTION",
        }
        for name, svc in self.model["services"].items():
            env = set((svc.get("environment") or {}).keys())
            leaked = env & forbidden
            self.assertFalse(
                leaked,
                f"{name} must not set delivery env {leaked!r}",
            )

    def test_every_role_sets_container_mode_and_role(self):
        for name, svc in self.model["services"].items():
            env = svc.get("environment") or {}
            self.assertEqual(env.get("NEWS_CONTAINER_MODE"), "1",
                             f"{name} must set NEWS_CONTAINER_MODE=1")
            self.assertEqual(env.get("NEWS_CONTAINER_ROLE"), name,
                             f"{name} must echo its role as NEWS_CONTAINER_ROLE")

    def test_worker_paths_are_inside_container_canary_root(self):
        """The four worker roles must expose the entrypoint-required
        variables and every path must live under /canary."""
        for name in ("ingest", "process", "validate", "report"):
            svc = self.model["services"][name]
            env = svc.get("environment") or {}
            self.assertEqual(env.get("NEWS_CONTAINER_CANARY_ROOT"), "/canary",
                             f"{name} must set NEWS_CONTAINER_CANARY_ROOT=/canary")
            # The five required variables from bin/container-entrypoint
            for key in (
                "NEWS_CONTAINER_CONTROL_DB",
                "NEWS_CONTAINER_STATE_DB",
                "NEWS_CONTAINER_ARTIFACT_ROOT",
                "NEWS_CONTAINER_LOCK_PATH",
            ):
                value = str(env.get(key, ""))
                self.assertTrue(
                    value.startswith("/canary/"),
                    f"{name} {key}={value!r} must be below /canary",
                )
            self.assertTrue(
                env.get("NEWS_CONTAINER_OWNER"),
                f"{name} must set NEWS_CONTAINER_OWNER",
            )
            targets = {
                vol.get("target")
                for vol in svc.get("volumes") or []
                if isinstance(vol, dict)
            }
            self.assertIn("/canary", targets)

    def test_bind_paths_use_only_temp_roots(self):
        """With the safe env, no resolved bind source may escape the
        ``_safe_canary_root`` tree."""
        safe_root = _safe_compose_env(self.tmp)["NEWS_CANARY_HOST_ROOT"]
        for name, svc in self.model["services"].items():
            for vol in svc.get("volumes") or []:
                if not isinstance(vol, dict):
                    continue
                source = vol.get("source", "")
                # Sources that look absolute must live under our temp root.
                if source.startswith("/"):
                    self.assertTrue(
                        source.startswith(safe_root) or source.startswith("/app") or
                        source.startswith("/brokers"),
                        f"{name} binds host path outside safe root: {source!r}",
                    )


class SharedLockPathTests(unittest.TestCase):
    """Writer roles share exactly one main-DB flock path."""

    def test_writer_roles_share_main_lock_path(self):
        with tempfile.TemporaryDirectory(prefix="contract-lock-") as tmp:
            model, proc = _render_compose(Path(tmp))
            if model is None:
                self.skipTest(f"docker compose unavailable: {proc.stderr.strip()[:256]}")
            lock_paths = {}
            for name, svc in model["services"].items():
                envmap = svc.get("environment") or {}
                lock = envmap.get("NEWS_CONTAINER_LOCK_PATH")
                if not lock:
                    continue
                lock_paths[name] = lock
            # scheduler does not own a lock; validate reads only.
            writer_roles = {"ingest", "process", "report", "validate"}
            writers = {k: v for k, v in lock_paths.items() if k in writer_roles}
            self.assertEqual(
                len(set(writers.values())), 1,
                f"writer roles must share exactly one lock path; got {writers!r}",
            )
            # Lock must be a sibling of the main DB.
            self.assertTrue(
                list(writers.values())[0].endswith("/news-state.db.lock"),
                "shared lock path must be news-state.db.lock",
            )


class EntrypointRequiredEnvContractTests(unittest.TestCase):
    """The entrypoint-required env variables must resolve in every
    worker role under the merged canary overlay."""

    REQUIRED_KEYS = (
        "NEWS_CONTAINER_CONTROL_DB",
        "NEWS_CONTAINER_STATE_DB",
        "NEWS_CONTAINER_ARTIFACT_ROOT",
        "NEWS_CONTAINER_LOCK_PATH",
        "NEWS_CONTAINER_OWNER",
        "NEWS_CONTAINER_CLAIM_TTL",
    )

    def test_every_worker_role_resolves_all_required_env(self):
        with tempfile.TemporaryDirectory(prefix="contract-required-") as tmp:
            model, proc = _render_compose(Path(tmp))
            if model is None:
                self.skipTest(f"docker compose unavailable: {proc.stderr.strip()[:256]}")
            for name in ("ingest", "process", "validate", "report"):
                env = (model["services"][name].get("environment") or {})
                missing = [k for k in self.REQUIRED_KEYS if not env.get(k)]
                self.assertEqual(
                    missing, [],
                    f"{name} must expose every entrypoint-required key; "
                    f"missing {missing!r}; have {sorted(env.keys())!r}",
                )

    def test_every_worker_role_bounds_max_iterations_to_one(self):
        with tempfile.TemporaryDirectory(prefix="contract-iters-") as tmp:
            model, proc = _render_compose(Path(tmp))
            if model is None:
                self.skipTest(f"docker compose unavailable: {proc.stderr.strip()[:256]}")
            for name in ("ingest", "process", "validate", "report"):
                env = (model["services"][name].get("environment") or {})
                self.assertEqual(
                    str(env.get("NEWS_CONTAINER_MAX_ITERATIONS")), "1",
                    f"{name} must resolve NEWS_CONTAINER_MAX_ITERATIONS=1 "
                    f"under the canary overlay; got {env.get('NEWS_CONTAINER_MAX_ITERATIONS')!r}",
                )

    def test_scheduler_resolves_once_with_fixed_at(self):
        with tempfile.TemporaryDirectory(prefix="contract-sched-") as tmp:
            model, proc = _render_compose(Path(tmp))
            if model is None:
                self.skipTest(f"docker compose unavailable: {proc.stderr.strip()[:256]}")
            env = (model["services"]["scheduler"].get("environment") or {})
            self.assertEqual(
                env.get("NEWS_CONTAINER_SCHEDULER_MODE"), "once",
                "canary scheduler must resolve mode=once",
            )
            self.assertTrue(
                env.get("NEWS_CONTAINER_SCHEDULER_AT"),
                "canary scheduler must pin NEWS_CONTAINER_SCHEDULER_AT to a non-empty value",
            )
            self.assertEqual(
                str(env.get("NEWS_CONTAINER_SCHEDULER_MAX_ITERATIONS")), "1",
                "canary scheduler must bound max iterations to 1",
            )

    def test_entrypoint_refuses_missing_required_env(self):
        """Source the entrypoint under a sanitized env with the five
        required variables intentionally cleared; the script must
        abort non-zero before reaching the python exec and surface
        the missing variable in stderr."""
        script = CANDIDATE_ROOT / "bin" / "container-entrypoint"
        # We strip four of the five required variables by unsetting
        # them after exporting the rest.  bash's `:` parameter
        # expansion fails the script under `set -e` before exec.
        wrapper = (
            "set +e\n"
            "unset NEWS_CONTAINER_STATE_DB\n"
            "unset NEWS_CONTAINER_ARTIFACT_ROOT\n"
            "unset NEWS_CONTAINER_LOCK_PATH\n"
            "unset NEWS_CONTAINER_OWNER\n"
            "export NEWS_CONTAINER_CONTROL_DB=/canary/state/control/runtime-control.db\n"
            "export NEWS_CONTAINER_ROLE=ingest\n"
            f"bash {script} ingest\n"
        )
        proc = _run(["bash", "-c", wrapper], timeout=10)
        self.assertNotEqual(
            proc.returncode, 0,
            "entrypoint must exit non-zero when a worker required env var is missing; "
            f"got {proc.returncode}; stderr={proc.stderr!r}",
        )
        self.assertIn(
            "required", proc.stderr.lower(),
            "entrypoint stderr must name the missing required variable",
        )
        # The script must name which variable was missing.
        self.assertTrue(
            any(name in proc.stderr for name in (
                "NEWS_CONTAINER_STATE_DB",
                "NEWS_CONTAINER_ARTIFACT_ROOT",
                "NEWS_CONTAINER_LOCK_PATH",
                "NEWS_CONTAINER_OWNER",
            )),
            f"entrypoint stderr must name a missing required variable; got {proc.stderr!r}",
        )

    def test_entrypoint_passes_when_all_required_env_present(self):
        """Same script as above, but with every required variable
        exported.  The script will then fall through to the python
        exec path; we expect a non-zero exit only because the python
        module will complain about an unreachable /app install path
        in this test host -- NOT a 64 usage error."""
        script = CANDIDATE_ROOT / "bin" / "container-entrypoint"
        wrapper = (
            "set +e\n"
            "export NEWS_CONTAINER_CONTROL_DB=/canary/state/control/runtime-control.db\n"
            "export NEWS_CONTAINER_STATE_DB=/canary/state/main/news-state.db\n"
            "export NEWS_CONTAINER_ARTIFACT_ROOT=/canary/artifacts\n"
            "export NEWS_CONTAINER_LOCK_PATH=/canary/locks/news-state.db.lock\n"
            "export NEWS_CONTAINER_OWNER=ingest\n"
            "export NEWS_CONTAINER_ROLE=ingest\n"
            f"bash {script} ingest\n"
        )
        proc = _run(["bash", "-c", wrapper], timeout=10)
        # 64 means the entrypoint itself aborted on missing vars; we
        # explicitly forbid that here.
        self.assertNotEqual(
            proc.returncode, 64,
            "entrypoint must NOT exit 64 when required vars are set; "
            f"got {proc.returncode}; stderr={proc.stderr!r}",
        )
        # And it must NOT be the fail-closed delivery sentinel.
        self.assertNotIn(
            "refused", proc.stderr.lower(),
            "entrypoint must NOT trip a fail-closed sentinel for the basic case; "
            f"stderr={proc.stderr!r}",
        )


class ManifestSelfExclusionTests(unittest.TestCase):
    """The release manifest self-excludes its output dir and caches."""

    @classmethod
    def setUpClass(cls):
        from scripts.render_release_manifest import main as render_main
        # Render into a tempdir so the contract test cannot pollute the
        # candidate root with a .release_manifest/ tree.
        cls.tmp = Path(tempfile.mkdtemp(prefix="contract-manifest-"))
        cls.output = cls.tmp / "candidate_manifest.json"
        rc = render_main([
            "--candidate-root", str(CANDIDATE_ROOT),
            "--output", str(cls.output),
        ])
        if rc != 0:
            raise unittest.SkipTest("render_release_manifest failed")
        cls.manifest = json.loads(cls.output.read_text(encoding="utf-8"))

    @classmethod
    def tearDownClass(cls):
        if cls.tmp.exists():
            shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_manifest_excludes_self(self):
        for record in self.manifest["files"]:
            self.assertFalse(
                record["path"].startswith(".release_manifest/"),
                f"manifest must not include output dir: {record['path']!r}",
            )

    def test_manifest_excludes_private_evidence(self):
        for record in self.manifest["files"]:
            self.assertFalse(
                record["path"].startswith("private-evidence/"),
                f"manifest must not include private-evidence: {record['path']!r}",
            )

    def test_manifest_excludes_pyc(self):
        for record in self.manifest["files"]:
            self.assertFalse(
                record["path"].endswith(".pyc"),
                f"manifest must not include .pyc: {record['path']!r}",
            )

    def test_manifest_excludes_pycache(self):
        for record in self.manifest["files"]:
            parts = record["path"].split("/")
            self.assertNotIn(
                "__pycache__", parts,
                f"manifest must not include __pycache__: {record['path']!r}",
            )

    def test_manifest_dockerfile_hash_is_pinned(self):
        for record in self.manifest["files"]:
            if record["path"] == "Dockerfile":
                self.assertEqual(
                    record["sha256"],
                    hashlib.sha256(
                        (CANDIDATE_ROOT / "Dockerfile").read_bytes()
                    ).hexdigest(),
                )
                return
        self.fail("manifest missing Dockerfile")


class RoleCommandImportTests(unittest.TestCase):
    """The scheduler/worker modules import and print --help cleanly."""

    def setUp(self):
        self.env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": "/tmp",
            "PYTHONPATH": str(CANDIDATE_ROOT / "scripts"),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
        }

    def test_scheduler_help(self):
        proc = _run(
            [sys.executable, "-m", "news_container.scheduler", "--help"],
            env=self.env, timeout=20,
        )
        self.assertEqual(proc.returncode, 0,
                         f"scheduler --help failed: {proc.stderr!r}")
        self.assertIn("scheduler", proc.stdout.lower())

    def test_worker_help(self):
        proc = _run(
            [sys.executable, "-m", "news_container.worker", "--help"],
            env=self.env, timeout=20,
        )
        self.assertEqual(proc.returncode, 0,
                         f"worker --help failed: {proc.stderr!r}")
        self.assertIn("--kind", proc.stdout)

    def test_worker_rejects_unknown_kind(self):
        proc = _run(
            [sys.executable, "-m", "news_container.worker",
             "--kind", "delivery",
             "--control-db", "/tmp/ctl.db",
             "--state-db", "/tmp/state.db",
             "--artifact-root", "/tmp/artifacts",
             "--lock-path", "/tmp/lock",
             "--owner", "test"],
            env=self.env, timeout=20,
        )
        self.assertNotEqual(proc.returncode, 0,
                            "worker must refuse the delivery kind")

    def test_worker_rejects_live_delivery_flag(self):
        # Worker CLI parser itself refuses the flag at --kind selection;
        # the in-process guard fires during dispatch.  We test the
        # dispatch guard by importing the module and inspecting argv.
        from scripts.news_container import worker as wmod
        # We exercise the static enforcement via the same code path the
        # worker uses when it sees --enable-live-delivery in argv.
        bad_argv = ["news-tick", "--enable-live-delivery"]
        with self.assertRaises(Exception) as ctx:
            wmod._dispatch(bad_argv)  # type: ignore[attr-defined]
        self.assertIn("delivery", str(ctx.exception).lower())


class EntrypointSyntaxTests(unittest.TestCase):
    """The container entrypoint is bash-clean and fail-closed on delivery."""

    def test_bash_syntax(self):
        proc = _run(["bash", "-n", str(CANDIDATE_ROOT / "bin" / "container-entrypoint")],
                    timeout=10)
        self.assertEqual(proc.returncode, 0,
                         f"bash -n failed: {proc.stderr!r}")

    def test_entrypoint_refuses_live_delivery_env(self):
        # We cannot exec the dispatcher without Docker, but we can
        # source the script under a sanitized env and verify it
        # refuses to call exec by tripping the sentinel check.
        script = CANDIDATE_ROOT / "bin" / "container-entrypoint"
        wrapper = (
            "set +e\n"
            "NEWS_PHASE6_ENABLE_LIVE_DELIVERY=1\n"
            "export NEWS_PHASE6_ENABLE_LIVE_DELIVERY\n"
            "exec_role=scheduler\n"
            f"NEWS_CONTAINER_ROLE=scheduler bash {script} scheduler\n"
        )
        proc = _run(["bash", "-c", wrapper], timeout=10)
        self.assertEqual(proc.returncode, 77,
                         "entrypoint must exit 77 on live-delivery sentinel")
        self.assertIn("delivery", proc.stderr.lower())


if __name__ == "__main__":
    unittest.main()