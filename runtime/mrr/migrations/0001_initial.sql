CREATE TABLE IF NOT EXISTS principals (
    principal_id TEXT PRIMARY KEY,
    principal_type TEXT NOT NULL,
    display_name TEXT NOT NULL,
    authority_scope_ref TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'suspended', 'revoked')),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS task_state_versions (
    state_version_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    version_number INTEGER NOT NULL CHECK (version_number >= 1),
    status TEXT NOT NULL CHECK (
        status IN (
            'created',
            'ready',
            'running',
            'waiting_on_capability',
            'waiting_on_policy',
            'waiting_on_control',
            'paused',
            'completed',
            'failed',
            'cancelled'
        )
    ),
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    UNIQUE(task_id, version_number)
);

CREATE INDEX IF NOT EXISTS idx_task_state_versions_task_version
    ON task_state_versions (task_id, version_number);

CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    goal TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN (
            'created',
            'ready',
            'running',
            'waiting_on_capability',
            'waiting_on_policy',
            'waiting_on_control',
            'paused',
            'completed',
            'failed',
            'cancelled'
        )
    ),
    owner_principal_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    policy_context_ref TEXT NOT NULL,
    working_state_ref TEXT NOT NULL,
    history_ref TEXT NOT NULL,
    checkpoint_ref TEXT NULL,
    budget_context_ref TEXT NOT NULL,
    priority INTEGER NOT NULL,
    result_ref TEXT NULL,
    FOREIGN KEY (owner_principal_id) REFERENCES principals(principal_id),
    FOREIGN KEY (working_state_ref) REFERENCES task_state_versions(state_version_id)
);

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_family TEXT NOT NULL CHECK (
        event_family IN ('task', 'capability', 'policy', 'checkpoint', 'control', 'failure', 'kernel')
    ),
    task_id TEXT NOT NULL,
    sequence_number INTEGER NOT NULL CHECK (sequence_number >= 1),
    occurred_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    emitted_by TEXT NOT NULL,
    correlation_id TEXT NULL,
    causation_event_id TEXT NULL,
    principal_id TEXT NULL,
    principal_role TEXT NULL,
    policy_context_ref TEXT NULL,
    budget_context_ref TEXT NULL,
    checkpoint_ref TEXT NULL,
    state_version_ref TEXT NULL,
    evidence_ref TEXT NULL,
    trace_ref TEXT NULL,
    tenant_id TEXT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    UNIQUE(task_id, sequence_number),
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);

CREATE INDEX IF NOT EXISTS idx_events_task_sequence
    ON events (task_id, sequence_number);

CREATE TRIGGER IF NOT EXISTS trg_events_no_update
BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(FAIL, 'events are append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_events_no_delete
BEFORE DELETE ON events
BEGIN
    SELECT RAISE(FAIL, 'events are append-only');
END;

CREATE TABLE IF NOT EXISTS checkpoints (
    checkpoint_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    state_version_ref TEXT NOT NULL,
    event_sequence_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    FOREIGN KEY (task_id) REFERENCES tasks(task_id),
    FOREIGN KEY (state_version_ref) REFERENCES task_state_versions(state_version_id)
);

CREATE INDEX IF NOT EXISTS idx_checkpoints_task_sequence
    ON checkpoints (task_id, event_sequence_number DESC);

CREATE TABLE IF NOT EXISTS event_outbox (
    outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    task_id TEXT NOT NULL,
    sequence_number INTEGER NOT NULL,
    publication_topic TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'published', 'failed')),
    created_at TEXT NOT NULL,
    published_at TEXT NULL,
    FOREIGN KEY (event_id) REFERENCES events(event_id),
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);

CREATE INDEX IF NOT EXISTS idx_event_outbox_status
    ON event_outbox (status, outbox_id);
