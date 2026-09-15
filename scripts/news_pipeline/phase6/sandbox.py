"""Slice 6.1 — fixed-root, FD-anchored phase6 sandbox.

Public surface:

* :class:`Sandbox` — owns the exclusive session/bootstrap anchors and the
  validated ExternalContext.
* :func:`open_phase6_database` — convenience helper that creates a
  bootstrapped sandbox and returns a configured ``:memory:`` connection.

The sandbox must never touch the network, credentials, system clock, RNG,
or process APIs. The only environment variable it consults is the literal
``PHASE6_SANDBOX_DB_CREATE``, which must equal the string ``"1"``.
"""
from __future__ import annotations

import fcntl
import os
import sqlite3
from pathlib import Path

from .time_inputs import ExternalContext, validate_external_context
from .types import (
    Phase6BootstrapError,
    Phase6ConfigurationError,
)


#: Literal env-var gate. The *only* key the sandbox is allowed to read.
PHASE6_SANDBOX_DB_CREATE: str = "PHASE6_SANDBOX_DB_CREATE"

#: The single accepted value of PHASE6_SANDBOX_DB_CREATE.
_PHASE6_SANDBOX_DB_CREATE_EXPECTED: str = "1"

#: Documentation-only flag for slice 6.1. Slice 6.1 is dry-run by design.
#: Behaviour is gated exclusively by PHASE6_SANDBOX_DB_CREATE.
PHASE6_SANDBOX_DRY_RUN: bool = True


def expected_root_modes() -> dict[str, int]:
    """Return the documented mode bits the sandbox requires."""
    return {"root": 0o700, "lock": 0o600}


class Sandbox:
    """A bootstrap-anchored, exclusive-FD sandbox rooted at ``root``."""

    _SESSION_LOCK = "session.lock"
    _BOOTSTRAP_LOCK = "bootstrap.lock"

    def __init__(self, root: Path, context: ExternalContext) -> None:
        # Validate the time identity up-front; never consult the clock.
        self._context_canonical = validate_external_context(context)
        self._context = context

        if not isinstance(root, Path):
            raise Phase6ConfigurationError("root must be a pathlib.Path")
        if not root.is_absolute():
            raise Phase6ConfigurationError(
                f"root must be absolute, got {root!r}"
            )

        self._root = root
        self._session_fd: int | None = None
        self._bootstrap_fd: int | None = None

    # ---- bootstrap / close -------------------------------------------------

    def bootstrap(self) -> None:
        """Verify the root and take exclusive FDs for session/bootstrap."""
        self._verify_root()
        self._session_fd = self._open_exclusive_lock(self._SESSION_LOCK)
        self._bootstrap_fd = self._open_exclusive_lock(self._BOOTSTRAP_LOCK)

    def close(self) -> None:
        """Release both exclusive FDs and unlink the anchor files."""
        self._close_fd(self._bootstrap_fd)
        self._bootstrap_fd = None
        self._close_fd(self._session_fd)
        self._session_fd = None
        # Unlink the anchor files so a subsequent bootstrap can re-anchor.
        for name in (self._BOOTSTRAP_LOCK, self._SESSION_LOCK):
            path = self._root / name
            try:
                path.unlink()
            except FileNotFoundError:
                pass

    # ---- database ----------------------------------------------------------

    def open_database(self) -> sqlite3.Connection:
        """Return a configured ``:memory:`` SQLite connection.

        The connection has:

        * ``PRAGMA journal_mode = memory``
        * ``PRAGMA temp_store  = memory``
        * ``enable_load_extension(False)`` (default — we re-assert)

        and is explicitly URI-disallowed: only ``:memory:`` is accepted.
        """
        if self._session_fd is None or self._bootstrap_fd is None:
            raise Phase6BootstrapError(
                "sandbox.open_database requires bootstrap() to have succeeded"
            )

        self._assert_env_gate()

        # URI must be the literal :memory: string; no path, no file:.
        uri = ":memory:"
        conn = sqlite3.connect(uri, uri=False)
        # Hardening: belt-and-braces.
        conn.execute("PRAGMA journal_mode = memory")
        conn.execute("PRAGMA temp_store = memory")
        # Belt-and-braces: ensure load_extension stays off even if a
        # caller later toggles it (we cannot prevent that without a
        # sqlite3_set_authorizer hook, which is module-level and out of
        # scope for slice 6.1).
        try:
            conn.enable_load_extension(False)  # type: ignore[attr-defined]
        except (AttributeError, sqlite3.OperationalError):
            # enable_load_extension is compiled-out on some builds; that
            # is the safer default and is acceptable.
            pass
        return conn

    # ---- context accessors -------------------------------------------------

    @property
    def canonical_now_utc_iso(self) -> str:
        return self._context_canonical

    @property
    def root(self) -> Path:
        return self._root

    # ---- internals ---------------------------------------------------------

    def _verify_root(self) -> None:
        if self._root.is_symlink():
            raise Phase6BootstrapError(
                f"root must not be a symlink: {self._root}"
            )
        try:
            st = os.stat(self._root, follow_symlinks=False)
        except FileNotFoundError as exc:
            raise Phase6BootstrapError(
                f"root does not exist: {self._root}"
            ) from exc
        if not os.path.isdir(self._root):
            raise Phase6BootstrapError(
                f"root must be a directory: {self._root}"
            )

        modes = expected_root_modes()
        actual_mode = st.st_mode & 0o777
        if actual_mode != modes["root"]:
            raise Phase6BootstrapError(
                f"root mode must be {oct(modes['root'])}, got {oct(actual_mode)}"
            )

        my_uid = os.getuid()
        if st.st_uid != my_uid:
            raise Phase6BootstrapError(
                f"root must be owned by current uid ({my_uid}), got uid={st.st_uid}"
            )

    def _open_exclusive_lock(self, name: str) -> int:
        path = self._root / name
        modes = expected_root_modes()
        flags = (
            os.O_RDWR
            | os.O_CREAT
            | os.O_EXCL  # fail if it already exists
        )
        try:
            fd = os.open(path, flags, modes["lock"])
        except FileExistsError as exc:
            raise Phase6BootstrapError(
                f"anchor already exists: {path}"
            ) from exc
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            raise Phase6BootstrapError(
                f"could not exclusively lock {path}: {exc}"
            ) from exc
        return fd

    @staticmethod
    def _close_fd(fd: int | None) -> None:
        if fd is None:
            return
        try:
            os.close(fd)
        except OSError:
            # Closing an FD twice is harmless; swallow.
            pass

    @staticmethod
    def _assert_env_gate() -> None:
        # ONLY the literal PHASE6_SANDBOX_DB_CREATE key is read.
        actual = os.environ.get(PHASE6_SANDBOX_DB_CREATE)
        if actual != _PHASE6_SANDBOX_DB_CREATE_EXPECTED:
            raise Phase6ConfigurationError(
                f"{PHASE6_SANDBOX_DB_CREATE} must be the literal "
                f"{_PHASE6_SANDBOX_DB_CREATE_EXPECTED!r}, got {actual!r}"
            )


def open_phase6_database(
    root: Path, context: ExternalContext
) -> sqlite3.Connection:
    """Bootstrap ``root`` and return a configured ``:memory:`` connection."""
    sb = Sandbox(root, context)
    sb.bootstrap()
    try:
        return sb.open_database()
    except BaseException:
        sb.close()
        raise


__all__ = [
    "PHASE6_SANDBOX_DB_CREATE",
    "PHASE6_SANDBOX_DRY_RUN",
    "Sandbox",
    "expected_root_modes",
    "open_phase6_database",
]