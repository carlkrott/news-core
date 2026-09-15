"""Slice 6.1 — phase6 sandbox package.

The public surface is intentionally narrow:

* :class:`Sandbox`
* :func:`open_phase6_database`
* :data:`PHASE6_SANDBOX_DB_CREATE`
* :data:`PHASE6_SANDBOX_DRY_RUN`

Recovery, outbox, preview, and other later-slice behaviour is **not**
re-exported here. Slice 6.1 is dry-run by design.
"""
from __future__ import annotations

from .outbox import (
    EXPECTED_RESULT_FOR_EVENT_KIND,
    MAX_OUTBOX_PAYLOAD_JSON_LEN,
    BatchPreviewOutcome,
    OUTBOX_STATE_PREVIEW_ONLY,
    OUTBOX_TEMPLATE_ID,
    OutboxBatchError,
    OutboxConflictError,
    OutboxConsistencyError,
    OutboxError,
    OutboxIdentityError,
    OutboxMissingInputError,
    OutboxSchemaError,
    PreviewRow,
    canonical_payload_json,
    canonical_payload_payload,
    payload_sha256,
    queue_previews,
)
from .recovery import (
    BatchEvaluationOutcome,
    EvaluationResult,
    JobInput,
    MAX_BATCH_SIZE,
    RecoveryBatchError,
    RecoveryConsistencyError,
    RecoveryDuplicateError,
    RecoveryEvaluationError,
    RecoveryIdentityError,
    RecoverySchemaError,
    RESULT_EXPIRED,
    RESULT_NO_OBSERVATION,
    RESULT_STALE,
    canonical_input_json,
    canonical_input_payload,
    evaluate_batch,
    input_sha256,
    validate_job_input,
)
from .sandbox import (
    PHASE6_SANDBOX_DB_CREATE,
    PHASE6_SANDBOX_DRY_RUN,
    Sandbox,
    expected_root_modes,
    open_phase6_database,
)
from .time_inputs import ExternalContext, validate_external_context
from .types import (
    Phase6BootstrapError,
    Phase6ConfigurationError,
    Phase6IdentityError,
    Phase6SandboxError,
    Phase6SchemaError,
)


# Slice 6.1 ships DRY_RUN=True. The flag is documentation-only — no code
# path in slice 6.1 branches on it. Later slices may promote it to a real
# gate once their normative contract lands.
__all__ = [
    "PHASE6_SANDBOX_DB_CREATE",
    "PHASE6_SANDBOX_DRY_RUN",
    "Sandbox",
    "ExternalContext",
    "expected_root_modes",
    "open_phase6_database",
    "validate_external_context",
    "Phase6SandboxError",
    "Phase6ConfigurationError",
    "Phase6IdentityError",
    "Phase6BootstrapError",
    "Phase6SchemaError",
    # Slice 6.2 — immutable recovery evaluation.
    "BatchEvaluationOutcome",
    "EvaluationResult",
    "JobInput",
    "MAX_BATCH_SIZE",
    "RecoveryBatchError",
    "RecoveryConsistencyError",
    "RecoveryDuplicateError",
    "RecoveryEvaluationError",
    "RecoveryIdentityError",
    "RecoverySchemaError",
    "RESULT_EXPIRED",
    "RESULT_NO_OBSERVATION",
    "RESULT_STALE",
    "canonical_input_json",
    "canonical_input_payload",
    "evaluate_batch",
    "input_sha256",
    "validate_job_input",
    # Slice 6.4 — inert dry-run outbox.
    "EXPECTED_RESULT_FOR_EVENT_KIND",
    "MAX_OUTBOX_PAYLOAD_JSON_LEN",
    "OUTBOX_STATE_PREVIEW_ONLY",
    "OUTBOX_TEMPLATE_ID",
    "BatchPreviewOutcome",
    "OutboxBatchError",
    "OutboxConflictError",
    "OutboxConsistencyError",
    "OutboxError",
    "OutboxIdentityError",
    "OutboxMissingInputError",
    "OutboxSchemaError",
    "PreviewRow",
    "canonical_payload_json",
    "canonical_payload_payload",
    "payload_sha256",
    "queue_previews",
]