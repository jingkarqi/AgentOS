CREATE TABLE IF NOT EXISTS control_signals (
    signal_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    signal_type TEXT NOT NULL CHECK (
        signal_type IN (
            'approve',
            'deny',
            'steer',
            'pause',
            'resume',
            'modify_budget',
            'modify_scope',
            'take_over',
            'reassign',
            'terminate'
        )
    ),
    status TEXT NOT NULL CHECK (
        status IN ('received', 'deferred', 'applied', 'rejected')
    ),
    issuer_principal_id TEXT NOT NULL,
    issuer_principal_role TEXT NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    correlation_id TEXT NOT NULL,
    received_event_id TEXT NOT NULL UNIQUE,
    received_sequence_number INTEGER NOT NULL CHECK (received_sequence_number >= 1),
    created_at TEXT NOT NULL,
    applied_at TEXT NULL,
    outcome_event_id TEXT NULL UNIQUE,
    outcome_sequence_number INTEGER NULL,
    CHECK (
        (status = 'received'
            AND applied_at IS NULL
            AND outcome_event_id IS NULL
            AND outcome_sequence_number IS NULL)
        OR (status = 'deferred'
            AND applied_at IS NULL
            AND outcome_event_id IS NOT NULL
            AND outcome_sequence_number IS NOT NULL)
        OR (status = 'rejected'
            AND applied_at IS NULL
            AND outcome_event_id IS NOT NULL
            AND outcome_sequence_number IS NOT NULL)
        OR (status = 'applied'
            AND applied_at IS NOT NULL
            AND outcome_event_id IS NOT NULL
            AND outcome_sequence_number IS NOT NULL)
    ),
    FOREIGN KEY (task_id) REFERENCES tasks(task_id),
    FOREIGN KEY (issuer_principal_id) REFERENCES principals(principal_id),
    FOREIGN KEY (received_event_id) REFERENCES events(event_id),
    FOREIGN KEY (outcome_event_id) REFERENCES events(event_id)
);

CREATE INDEX IF NOT EXISTS idx_control_signals_task_created
    ON control_signals (task_id, received_sequence_number, signal_id);

CREATE INDEX IF NOT EXISTS idx_control_signals_status_created
    ON control_signals (status, received_sequence_number, signal_id);

CREATE TRIGGER IF NOT EXISTS trg_control_signals_no_delete
BEFORE DELETE ON control_signals
BEGIN
    SELECT RAISE(FAIL, 'control_signals are durable audit summaries');
END;

CREATE TRIGGER IF NOT EXISTS trg_control_signals_guarded_update
BEFORE UPDATE ON control_signals
BEGIN
    SELECT CASE
        WHEN OLD.signal_id != NEW.signal_id
            OR OLD.task_id != NEW.task_id
            OR OLD.signal_type != NEW.signal_type
            OR OLD.issuer_principal_id != NEW.issuer_principal_id
            OR OLD.issuer_principal_role != NEW.issuer_principal_role
            OR OLD.payload != NEW.payload
            OR OLD.correlation_id != NEW.correlation_id
            OR OLD.received_event_id != NEW.received_event_id
            OR OLD.received_sequence_number != NEW.received_sequence_number
            OR OLD.created_at != NEW.created_at
        THEN RAISE(FAIL, 'control_signals immutable fields cannot change')
        WHEN NOT (
            (OLD.status = NEW.status)
            OR (OLD.status = 'received' AND NEW.status IN ('deferred', 'applied', 'rejected'))
            OR (OLD.status = 'deferred' AND NEW.status IN ('applied', 'rejected'))
        )
        THEN RAISE(FAIL, 'invalid control signal status transition')
    END;
END;
