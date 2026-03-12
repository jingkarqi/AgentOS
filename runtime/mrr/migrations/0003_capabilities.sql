CREATE TABLE IF NOT EXISTS capabilities (
    capability_id TEXT NOT NULL,
    capability_name TEXT NOT NULL,
    capability_version TEXT NOT NULL,
    contract_version TEXT NOT NULL,
    description TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled', 'deprecated', 'revoked')),
    input_schema TEXT NOT NULL CHECK (json_valid(input_schema)),
    output_schema TEXT NOT NULL CHECK (json_valid(output_schema)),
    effect_class TEXT NOT NULL CHECK (
        effect_class IN (
            'read_only',
            'state_mutating_internal',
            'external_side_effect',
            'authority_mediated',
            'mixed'
        )
    ),
    idempotency_class TEXT NOT NULL CHECK (
        idempotency_class IN (
            'idempotent',
            'conditionally_idempotent',
            'non_idempotent',
            'unknown'
        )
    ),
    timeout_class TEXT NOT NULL CHECK (
        timeout_class IN ('short', 'medium', 'long', 'operator_defined')
    ),
    cost_class TEXT NOT NULL CHECK (
        cost_class IN ('low', 'moderate', 'high', 'variable')
    ),
    trust_level TEXT NOT NULL CHECK (
        trust_level IN (
            'kernel_trusted',
            'runtime_trusted',
            'sandboxed',
            'external_untrusted',
            'human_authority'
        )
    ),
    required_authority_scope TEXT NOT NULL,
    observability_class TEXT NOT NULL,
    streaming_support INTEGER NULL CHECK (streaming_support IN (0, 1)),
    supports_cancellation INTEGER NULL CHECK (supports_cancellation IN (0, 1)),
    supports_partial_result INTEGER NULL CHECK (supports_partial_result IN (0, 1)),
    supports_checkpoint_safe_resume INTEGER NULL CHECK (supports_checkpoint_safe_resume IN (0, 1)),
    side_effect_surface TEXT NULL CHECK (side_effect_surface IS NULL OR json_valid(side_effect_surface)),
    trust_zone_surface TEXT NULL CHECK (trust_zone_surface IS NULL OR json_valid(trust_zone_surface)),
    external_dependencies TEXT NULL CHECK (external_dependencies IS NULL OR json_valid(external_dependencies)),
    sandbox_profile TEXT NULL CHECK (sandbox_profile IS NULL OR json_valid(sandbox_profile)),
    evidence_profile TEXT NULL CHECK (evidence_profile IS NULL OR json_valid(evidence_profile)),
    rate_limit_class TEXT NULL,
    tenant_restrictions TEXT NULL CHECK (tenant_restrictions IS NULL OR json_valid(tenant_restrictions)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (capability_id, capability_version)
);

CREATE INDEX IF NOT EXISTS idx_capabilities_status_identity
    ON capabilities (status, capability_id, capability_version);

CREATE TRIGGER IF NOT EXISTS trg_capabilities_no_delete
BEFORE DELETE ON capabilities
BEGIN
    SELECT RAISE(FAIL, 'capabilities are durable contracts');
END;

CREATE TABLE IF NOT EXISTS capability_calls (
    call_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    caller_principal_id TEXT NOT NULL,
    caller_principal_role TEXT NOT NULL,
    capability_id TEXT NOT NULL,
    capability_version TEXT NULL,
    input_payload TEXT NOT NULL CHECK (json_valid(input_payload)),
    requested_at TEXT NOT NULL,
    dispatched_at TEXT NULL,
    completed_at TEXT NULL,
    policy_context_ref TEXT NOT NULL,
    budget_context_ref TEXT NOT NULL,
    effect_class TEXT NOT NULL,
    cost_class TEXT NULL CHECK (
        cost_class IS NULL OR cost_class IN ('low', 'moderate', 'high', 'variable')
    ),
    idempotency_expectation TEXT NOT NULL CHECK (
        idempotency_expectation IN (
            'idempotent',
            'conditionally_idempotent',
            'non_idempotent',
            'unknown'
        )
    ),
    correlation_id TEXT NOT NULL,
    checkpoint_ref TEXT NULL,
    state_version_ref TEXT NULL,
    timeout_override_ms INTEGER NULL CHECK (
        timeout_override_ms IS NULL OR timeout_override_ms >= 1
    ),
    request_reason_code TEXT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('requested', 'policy_blocked', 'dispatched', 'completed')
    ),
    outcome TEXT NULL CHECK (
        outcome IS NULL OR outcome IN ('succeeded', 'failed', 'denied', 'timed_out', 'cancelled')
    ),
    result_payload TEXT NULL CHECK (
        result_payload IS NULL OR json_valid(result_payload)
    ),
    side_effect_summary TEXT NULL CHECK (
        side_effect_summary IS NULL
        OR side_effect_summary IN (
            'not_attempted',
            'attempted_but_not_committed',
            'committed',
            'committed_with_unknown_finality',
            'unknown'
        )
    ),
    retry_safety TEXT NULL CHECK (
        retry_safety IS NULL
        OR retry_safety IN ('safe', 'unsafe', 'requires_operator_decision', 'unknown')
    ),
    provider_metadata TEXT NULL CHECK (
        provider_metadata IS NULL OR json_valid(provider_metadata)
    ),
    evidence_ref TEXT NULL,
    latency_ms INTEGER NULL CHECK (latency_ms IS NULL OR latency_ms >= 0),
    reason_code TEXT NULL,
    requested_event_id TEXT NOT NULL UNIQUE,
    requested_sequence_number INTEGER NOT NULL CHECK (requested_sequence_number >= 1),
    dispatched_event_id TEXT NULL UNIQUE,
    dispatched_sequence_number INTEGER NULL,
    outcome_event_id TEXT NULL UNIQUE,
    outcome_sequence_number INTEGER NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id),
    FOREIGN KEY (requested_event_id) REFERENCES events(event_id),
    FOREIGN KEY (dispatched_event_id) REFERENCES events(event_id),
    FOREIGN KEY (outcome_event_id) REFERENCES events(event_id)
);

CREATE INDEX IF NOT EXISTS idx_capability_calls_task_requested
    ON capability_calls (task_id, requested_sequence_number, call_id);

CREATE INDEX IF NOT EXISTS idx_capability_calls_status_requested
    ON capability_calls (status, requested_sequence_number, call_id);

CREATE TRIGGER IF NOT EXISTS trg_capability_calls_no_delete
BEFORE DELETE ON capability_calls
BEGIN
    SELECT RAISE(FAIL, 'capability_calls are durable audit summaries');
END;
