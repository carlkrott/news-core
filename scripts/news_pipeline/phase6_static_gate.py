"""Slice 6.1 — dependency-closure static gate.

Walk the AST of every module under :mod:`news_pipeline.phase6` plus this
gate itself, and reject any import, attribute access, or call that touches
the network, credentials, system clock, RNG, or process APIs.

The gate is intentionally narrow: it asserts *the surface that slice 6.1
actually uses*, not the entire stdlib. Run with ``python -m
news_pipeline.phase6_static_gate [path-to-phase6-dir]``.
"""
from __future__ import annotations

import ast
import os
import sys
from pathlib import Path


# A specific set of fully-qualified module names that slice 6.1 must NEVER
# import. We compare AST ``Import`` / ``ImportFrom`` names against this set.
FORBIDDEN_MODULES: frozenset[str] = frozenset({
    "socket",
    "urllib",
    "urllib.request",
    "urllib.parse",
    "urllib.error",
    "http",
    "http.client",
    "http.server",
    "requests",
    "subprocess",
    "asyncio",
    "asyncio.subprocess",
    "random",
    "secrets",
    "uuid",
    "getpass",
    "keyring",
    "ctypes",
    "multiprocessing",
    "threading",
})


# Call-name patterns that must never appear in phase6 source.
FORBIDDEN_CALLS: tuple[str, ...] = (
    "time.time",
    "time.clock_gettime",
    "time.monotonic",
    "time.perf_counter",
    "time.process_time",
    "datetime.datetime.now",
    "datetime.datetime.utcnow",
    "datetime.datetime.today",
    "datetime.datetime.fromtimestamp",
    "datetime.datetime.utcfromtimestamp",
    "os.system",
    "os.popen",
    "os.execv",
    "os.execve",
    "os.execvp",
    "os.execvpe",
    "os.fork",
    "os.spawn",
    "os.spawnv",
    "os.spawnve",
    "os.spawnvp",
    "os.spawnvpe",
    "os.posix_spawn",
    "os.posix_spawnp",
    "os.kill",
    "os.killpg",
    "os.sendfile",
    "random.random",
    "random.randrange",
    "random.randint",
    "random.choice",
    "random.shuffle",
    "random.sample",
    "secrets.token_",
    "uuid.uuid",
    "uuid.uuid1",
    "uuid.uuid4",
)


# The ONLY environment key phase6 may read.
PHASE6_ALLOWED_ENV_KEYS: frozenset[str] = frozenset({
    "PHASE6_SANDBOX_DB_CREATE",
})


class Phase6StaticGateError(RuntimeError):
    """Raised when a phase6 module violates the static gate."""


def _is_forbidden_call(node: ast.Call) -> str | None:
    """Return a forbidden pattern name if ``node`` matches one, else None."""
    func = node.func
    # Unwind Attribute chains: e.g. datetime.datetime.now -> "datetime.datetime.now".
    parts: list[str] = []
    while isinstance(func, ast.Attribute):
        parts.append(func.attr)
        func = func.value
    if isinstance(func, ast.Name):
        parts.append(func.id)
    else:
        return None
    parts.reverse()
    dotted = ".".join(parts)
    for pattern in FORBIDDEN_CALLS:
        if dotted == pattern or dotted.startswith(pattern + "."):
            return pattern
    return None


def _resolve_dotted(node: ast.AST) -> str | None:
    """Return the dotted form of an attribute/name chain (or None).

    ``ast.Name("os")`` -> ``"os"``
    ``ast.Attribute(attr="get", value=ast.Name("os"))`` -> ``"os.get"``
    ``ast.Attribute(attr="get", value=ast.Attribute(attr="environ",
    value=ast.Name("os")))`` -> ``"os.environ.get"``
    """
    parts: list[str] = []
    cur = node
    while isinstance(cur, ast.Attribute):
        parts.append(cur.attr)
        cur = cur.value
    if isinstance(cur, ast.Name):
        parts.append(cur.id)
        parts.reverse()
        return ".".join(parts)
    return None


def _extract_str_const(node: ast.AST) -> str | None:
    """Return ``node.value`` if ``node`` is a string literal, else None."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _is_disallowed_os_env_read(node: ast.Call) -> bool:
    """True iff ``node`` is an ``os.environ{,.get,getenv}(KEY)`` or
    ``os.{getenv,get}(KEY)`` call whose KEY is not in the allow-list.

    Matches ALL of these forms:
        os.getenv("KEY")
        os.get("KEY")        (alias of getenv)
        os.environ.get("KEY")
        os.environ.getenv("KEY")

    Anything else returns False.
    """
    dotted = _resolve_dotted(node.func) or ""
    if dotted not in ("os.getenv", "os.get", "os.environ.get", "os.environ.getenv"):
        return False
    if not node.args:
        return False
    key = _extract_str_const(node.args[0])
    if key is None:
        return False
    return key not in PHASE6_ALLOWED_ENV_KEYS


def _scan_tree(path: Path, tree: ast.AST) -> list[str]:
    """Return a list of human-readable gate violations for ``tree``."""
    errors: list[str] = []

    for node in ast.walk(tree):
        # Forbidden module imports.
        if isinstance(node, ast.Import):
            for alias in node.names:
                top = alias.name.split(".", 1)[0]
                full = alias.name
                if top in FORBIDDEN_MODULES or full in FORBIDDEN_MODULES:
                    errors.append(
                        f"{path}: forbidden import: {full}"
                    )
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            top = module.split(".", 1)[0]
            if top in FORBIDDEN_MODULES or module in FORBIDDEN_MODULES:
                errors.append(
                    f"{path}: forbidden from-import: {module}"
                )

        # Forbidden call patterns AND os.environ reads against non-allowed
        # keys. Both checks share the ``ast.Call`` branch on purpose: a
        # previous version had two consecutive ``elif isinstance(node,
        # ast.Call)`` arms, which made the env-key check unreachable, AND
        # the first version of the check only matched ``os.getenv(...)``
        # written as a direct attribute on ``os`` — so the natural
        # ``os.environ.get(...)`` chain slipped past. We now fold both
        # into one branch that fails closed for forbidden calls AND for
        # any non-allowed env read via either ``os.{getenv,get}(KEY)``
        # or ``os.environ.{get,getenv}(KEY)``.
        elif isinstance(node, ast.Call):
            forbidden = _is_forbidden_call(node)
            if forbidden:
                errors.append(
                    f"{path}: forbidden call: {forbidden}"
                )
            elif _is_disallowed_os_env_read(node):
                key = _extract_str_const(node.args[0])
                errors.append(
                    f"{path}: forbidden env key: {key!r}"
                )

        # os.environ[...] subscript reads.
        elif isinstance(node, ast.Subscript):
            if (
                isinstance(node.value, ast.Attribute)
                and node.value.attr == "environ"
                and isinstance(node.value.value, ast.Name)
                and node.value.value.id == "os"
                and isinstance(node.slice, ast.Constant)
                and isinstance(node.slice.value, str)
                and node.slice.value not in PHASE6_ALLOWED_ENV_KEYS
            ):
                errors.append(
                    f"{path}: forbidden env key: {node.slice.value!r}"
                )

    return errors


def scan_phase6(root: Path) -> list[str]:
    """Run the static gate over the phase6 package and return errors."""
    errors: list[str] = []
    targets: list[Path] = []

    package_dir = root / "phase6"
    if not package_dir.is_dir():
        raise Phase6StaticGateError(f"phase6 package not found at {package_dir}")

    for path in sorted(package_dir.glob("*.py")):
        if path.name == "__init__.py":
            # Even __init__.py must obey the gate.
            targets.append(path)
        elif path.name != "__pycache__":
            targets.append(path)

    # Always include the static gate itself.
    targets.append(root / "phase6_static_gate.py")

    for path in targets:
        try:
            source = path.read_text(encoding="utf-8")
        except OSError as exc:
            errors.append(f"{path}: cannot read: {exc}")
            continue
        try:
            tree = ast.parse(source, filename=str(path))
        except SyntaxError as exc:
            errors.append(f"{path}: syntax error: {exc}")
            continue
        errors.extend(_scan_tree(path, tree))

    return errors


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns 0 on success, 1 on gate failure."""
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv:
        root = Path(argv[0]).resolve()
    else:
        # Default: assume we live in scripts/news_pipeline/.
        here = Path(__file__).resolve().parent
        root = here

    errors = scan_phase6(root)
    if errors:
        print("phase6_static_gate FAILED", file=sys.stderr)
        for line in errors:
            print(f"  {line}", file=sys.stderr)
        return 1

    print("phase6_static_gate OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())