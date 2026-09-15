"""Slice 6.1 — typed exception hierarchy for the phase6 sandbox.

Every user-visible failure mode has a dedicated subclass so that callers
can catch narrowly, and so that static analysis can prove the surface.
"""
from __future__ import annotations


class Phase6SandboxError(Exception):
    """Base class for every phase6 sandbox failure."""


class Phase6ConfigurationError(Phase6SandboxError):
    """The sandbox root, env-var gate, or constructor arguments are invalid."""


class Phase6IdentityError(Phase6SandboxError):
    """A caller-supplied ExternalContext / time identity is malformed."""


class Phase6BootstrapError(Phase6SandboxError):
    """The fixed root is not exclusively anchorable for this process."""


class Phase6SchemaError(Phase6SandboxError):
    """The V1 schema checksum, table set, or user_version is wrong."""


__all__ = [
    "Phase6SandboxError",
    "Phase6ConfigurationError",
    "Phase6IdentityError",
    "Phase6BootstrapError",
    "Phase6SchemaError",
]