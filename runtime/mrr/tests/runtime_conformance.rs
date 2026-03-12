use std::convert::TryFrom;
use std::sync::Arc;

use agentos_mrr::{
    ControlSignalStatus, ControlSignalType, CreateTaskCommand, DeterministicIdGenerator,
    EventFamily, FixedClock, Principal, PrincipalAttribution, PrincipalStatus,
    RequireApprovalCommand, RuntimeDb, SubmitControlSignalCommand, TaskManager, TaskManagerConfig,
    TaskStatus,
};
use serde_json::json;
use sqlx::SqlitePool;
use time::OffsetDateTime;
use uuid::Uuid;

struct TestHarness {
    manager: TaskManager,
    owner_principal_id: Uuid,
    pool: SqlitePool,
}

async fn setup_harness() -> TestHarness {
    let db = RuntimeDb::connect_and_migrate("sqlite::memory:")
        .await
        .expect("in-memory db should initialize");

    let fixed_time = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let manager = TaskManager::new(
        db.clone_pool(),
        Arc::new(FixedClock::new(fixed_time)),
        Arc::new(DeterministicIdGenerator::new(100)),
        TaskManagerConfig::default(),
    );

    let owner_principal_id = Uuid::from_u128(1);
    manager
        .principal_store()
        .upsert(&Principal {
            principal_id: owner_principal_id,
            principal_type: "human".to_owned(),
            display_name: "Kernel Owner".to_owned(),
            authority_scope_ref: "authority/root".to_owned(),
            status: PrincipalStatus::Active,
            created_at: fixed_time,
        })
        .await
        .expect("principal should persist");

    TestHarness {
        manager,
        owner_principal_id,
        pool: db.clone_pool(),
    }
}

async fn create_active_principal(harness: &TestHarness, principal_id: Uuid, display_name: &str) {
    let fixed_time = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    harness
        .manager
        .principal_store()
        .upsert(&Principal {
            principal_id,
            principal_type: "human".to_owned(),
            display_name: display_name.to_owned(),
            authority_scope_ref: "authority/root".to_owned(),
            status: PrincipalStatus::Active,
            created_at: fixed_time,
        })
        .await
        .expect("principal should persist");
}

fn actor(principal_id: Uuid) -> PrincipalAttribution {
    PrincipalAttribution::acting(principal_id)
}

async fn create_task(harness: &TestHarness, goal: &str) -> Uuid {
    harness
        .manager
        .create_task(CreateTaskCommand::new(goal, harness.owner_principal_id))
        .await
        .expect("task should be created")
        .task_id
}

#[tokio::test]
async fn cts_03_illegal_transition_is_rejected_and_logged() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "illegal-transition").await;

    let error = harness
        .manager
        .complete_task(
            task_id,
            actor(harness.owner_principal_id),
            json!({"unexpected": true}),
        )
        .await
        .expect_err("created -> completed must be rejected");

    match error {
        agentos_mrr::RuntimeError::InvalidTransition {
            from_status,
            to_status,
            ..
        } => {
            assert_eq!(from_status, "created");
            assert_eq!(to_status, "completed");
        }
        other => panic!("unexpected error: {other}"),
    }

    let task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should remain queryable");
    assert_eq!(task.status, TaskStatus::Created);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event_type, "task.created");
    assert_eq!(events[1].event_type, "task.transition.rejected");
    assert_eq!(events[1].event_family, EventFamily::Failure);
}

#[tokio::test]
async fn cts_04_terminal_state_is_irreversible() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "terminality").await;

    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");
    harness
        .manager
        .complete_task(
            task_id,
            actor(harness.owner_principal_id),
            json!({"summary": "done"}),
        )
        .await
        .expect("complete should succeed");

    let error = harness
        .manager
        .resume_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect_err("completed task must not resume");

    assert!(matches!(
        error,
        agentos_mrr::RuntimeError::InvalidTransition { .. }
    ));

    let task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(task.status, TaskStatus::Completed);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let last = events.last().expect("rejection event should exist");
    assert_eq!(last.event_type, "task.transition.rejected");
    assert_eq!(last.payload["reason_code"], "terminal_state_irreversible");
}

#[tokio::test]
async fn cts_05_event_envelope_fields_are_present() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "event-envelope").await;

    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    assert!(events.len() >= 3);

    for event in events {
        assert!(!event.schema_version.is_empty());
        assert!(!event.event_type.is_empty());
        assert!(!event.emitted_by.is_empty());
        assert_eq!(event.task_id, task_id);
        assert!(event.sequence_number >= 1);
        assert!(event.payload.is_object());
    }
}

#[tokio::test]
async fn cts_06_event_history_is_append_only() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "append-only").await;

    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");

    let before = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let snapshot: Vec<(Uuid, serde_json::Value)> = before
        .iter()
        .map(|event| (event.event_id, event.payload.clone()))
        .collect();

    harness
        .manager
        .pause_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("pause should succeed");

    let after = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should reload");

    assert!(after.len() > before.len());
    let after_prefix: Vec<(Uuid, serde_json::Value)> = after
        .iter()
        .take(snapshot.len())
        .map(|event| (event.event_id, event.payload.clone()))
        .collect();
    assert_eq!(snapshot, after_prefix);
}

#[tokio::test]
async fn cts_07_sequence_numbers_are_monotonic_and_gapless() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "sequence").await;

    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");
    harness
        .manager
        .pause_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("pause should succeed");
    harness
        .manager
        .resume_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("resume should succeed");

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");

    for (index, event) in events.iter().enumerate() {
        assert_eq!(
            event.sequence_number,
            i64::try_from(index + 1).expect("small test size")
        );
    }
}

#[tokio::test]
async fn cts_14_checkpoint_creation_records_lineage() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "checkpoint-create").await;

    let running_task = harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");

    let checkpoint = harness
        .manager
        .create_checkpoint(
            task_id,
            actor(harness.owner_principal_id),
            json!({"reason_code": "operator_requested"}),
        )
        .await
        .expect("checkpoint should succeed");

    let task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(task.checkpoint_ref, Some(checkpoint.checkpoint_id));
    assert_eq!(
        checkpoint.state_version_ref,
        running_task.working_state_ref.state_version_id
    );
    assert_eq!(checkpoint.status, "running");

    let latest_checkpoint = harness
        .manager
        .get_latest_checkpoint_for_task(task_id)
        .await
        .expect("latest checkpoint should load")
        .expect("checkpoint should exist");
    assert_eq!(latest_checkpoint.checkpoint_id, checkpoint.checkpoint_id);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let checkpoint_event = events
        .iter()
        .find(|event| event.event_type == "checkpoint.created")
        .expect("checkpoint.created event should exist");

    assert_eq!(checkpoint_event.event_family, EventFamily::Checkpoint);
    assert_eq!(
        checkpoint_event.checkpoint_ref,
        Some(checkpoint.checkpoint_id)
    );
    assert_eq!(
        checkpoint_event.state_version_ref,
        Some(checkpoint.state_version_ref)
    );
    assert_eq!(
        checkpoint_event.payload["event_boundary_sequence_number"],
        checkpoint.event_sequence_number
    );
}

#[tokio::test]
async fn restore_checkpoint_rewinds_task_state_and_emits_restore_events() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "checkpoint-restore").await;

    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");
    let paused_task = harness
        .manager
        .pause_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("pause should succeed");

    let checkpoint = harness
        .manager
        .create_checkpoint(
            task_id,
            actor(harness.owner_principal_id),
            json!({"reason_code": "before_resume"}),
        )
        .await
        .expect("checkpoint should succeed");

    sqlx::query(
        "UPDATE tasks SET policy_context_ref = ?, budget_context_ref = ? WHERE task_id = ?",
    )
    .bind("policy/override")
    .bind("budget/override")
    .bind(task_id.to_string())
    .execute(&harness.pool)
    .await
    .expect("task refs should be mutable for restore validation");

    harness
        .manager
        .resume_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("resume should succeed");

    let restored_task = harness
        .manager
        .restore_checkpoint(
            task_id,
            actor(harness.owner_principal_id),
            Some(checkpoint.checkpoint_id),
        )
        .await
        .expect("restore should succeed");

    assert_eq!(restored_task.status, TaskStatus::Paused);
    assert_eq!(restored_task.checkpoint_ref, Some(checkpoint.checkpoint_id));
    assert_ne!(
        restored_task.working_state_ref,
        paused_task.working_state_ref
    );
    assert_eq!(
        restored_task.working_state_ref.task_id,
        paused_task.working_state_ref.task_id
    );
    assert!(
        restored_task.working_state_ref.version_number
            > paused_task.working_state_ref.version_number
    );
    assert_eq!(restored_task.policy_context_ref, "policy/default");
    assert_eq!(restored_task.budget_context_ref, "budget/default");

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let restore_events: Vec<_> = events
        .iter()
        .filter(|event| event.event_type.starts_with("checkpoint.restore."))
        .collect();
    assert!(restore_events.len() >= 3);
    assert_eq!(restore_events[0].event_type, "checkpoint.restore.requested");
    assert_eq!(restore_events[1].event_type, "checkpoint.restore.started");
    assert_eq!(
        restore_events
            .last()
            .expect("restore outcome should exist")
            .event_type,
        "checkpoint.restore.succeeded"
    );
    assert!(restore_events
        .iter()
        .all(|event| event.event_family == EventFamily::Checkpoint));
    assert_eq!(
        restore_events[0].correlation_id,
        restore_events[1].correlation_id
    );
    assert_eq!(
        restore_events[1].correlation_id,
        restore_events
            .last()
            .expect("restore outcome should exist")
            .correlation_id
    );
    assert_eq!(
        restore_events
            .last()
            .expect("restore outcome should exist")
            .checkpoint_ref,
        Some(checkpoint.checkpoint_id)
    );
    assert_eq!(
        restore_events
            .last()
            .expect("restore outcome should exist")
            .state_version_ref,
        Some(restored_task.working_state_ref.state_version_id)
    );
}

#[tokio::test]
async fn restore_rejects_checkpoint_from_other_task_and_logs_failure() {
    let harness = setup_harness().await;
    let task_a = create_task(&harness, "checkpoint-a").await;
    let task_b = create_task(&harness, "checkpoint-b").await;

    harness
        .manager
        .start_task(task_a, actor(harness.owner_principal_id))
        .await
        .expect("task a should start");
    harness
        .manager
        .start_task(task_b, actor(harness.owner_principal_id))
        .await
        .expect("task b should start");

    let checkpoint = harness
        .manager
        .create_checkpoint(
            task_a,
            actor(harness.owner_principal_id),
            json!({"reason_code": "task_a_snapshot"}),
        )
        .await
        .expect("checkpoint should succeed");

    let before_task = harness
        .manager
        .get_task(task_b)
        .await
        .expect("task b should load before failed restore");

    let error = harness
        .manager
        .restore_checkpoint(
            task_b,
            actor(harness.owner_principal_id),
            Some(checkpoint.checkpoint_id),
        )
        .await
        .expect_err("checkpoint from another task must be rejected");

    assert!(matches!(
        error,
        agentos_mrr::RuntimeError::CheckpointNotFound { .. }
    ));
    let task = harness
        .manager
        .get_task(task_b)
        .await
        .expect("task b should still load");
    assert_eq!(task.task_id, task_b);
    assert_eq!(task.status, before_task.status);
    assert_eq!(task.working_state_ref, before_task.working_state_ref);
    assert_eq!(task.checkpoint_ref, before_task.checkpoint_ref);

    let events = harness
        .manager
        .list_events_by_task(task_b)
        .await
        .expect("events should load");
    let restore_events: Vec<_> = events
        .iter()
        .filter(|event| event.event_type.starts_with("checkpoint.restore."))
        .collect();
    assert!(restore_events.len() >= 3);
    assert_eq!(
        restore_events
            .last()
            .expect("restore outcome should exist")
            .event_type,
        "checkpoint.restore.failed"
    );
    assert_eq!(
        restore_events
            .last()
            .expect("restore outcome should exist")
            .event_family,
        EventFamily::Checkpoint
    );
    assert_eq!(
        restore_events
            .last()
            .expect("restore outcome should exist")
            .payload["reason_code"],
        "checkpoint_not_found"
    );
}

#[tokio::test]
async fn restore_rejects_non_terminal_reentry_after_terminal_state() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "checkpoint-terminal-guard").await;

    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");

    let checkpoint = harness
        .manager
        .create_checkpoint(
            task_id,
            actor(harness.owner_principal_id),
            json!({"reason_code": "before_completion"}),
        )
        .await
        .expect("checkpoint should succeed");

    harness
        .manager
        .complete_task(
            task_id,
            actor(harness.owner_principal_id),
            json!({"summary": "done"}),
        )
        .await
        .expect("complete should succeed");

    let error = harness
        .manager
        .restore_checkpoint(
            task_id,
            actor(harness.owner_principal_id),
            Some(checkpoint.checkpoint_id),
        )
        .await
        .expect_err("terminal task must not restore into non-terminal state");

    assert!(matches!(
        error,
        agentos_mrr::RuntimeError::InvalidTransition {
            from_status,
            to_status,
            ..
        } if from_status == "completed" && to_status == "running"
    ));

    let task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(task.status, TaskStatus::Completed);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let restore_events: Vec<_> = events
        .iter()
        .filter(|event| event.event_type.starts_with("checkpoint.restore."))
        .collect();
    assert!(restore_events.len() >= 3);
    assert_eq!(
        restore_events
            .last()
            .expect("restore outcome should exist")
            .event_type,
        "checkpoint.restore.failed"
    );
    assert_eq!(
        restore_events
            .last()
            .expect("restore outcome should exist")
            .payload["reason_code"],
        "terminal_state_irreversible"
    );
}

#[tokio::test]
async fn restore_rejects_checkpoint_with_invalid_status_and_logs_failure() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "checkpoint-invalid-status").await;

    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");

    let checkpoint = harness
        .manager
        .create_checkpoint(
            task_id,
            actor(harness.owner_principal_id),
            json!({"reason_code": "before_corruption"}),
        )
        .await
        .expect("checkpoint should succeed");

    sqlx::query("UPDATE checkpoints SET status = ? WHERE checkpoint_id = ?")
        .bind("not_a_real_status")
        .bind(checkpoint.checkpoint_id.to_string())
        .execute(&harness.pool)
        .await
        .expect("checkpoint status should be mutable for test corruption");

    let error = harness
        .manager
        .restore_checkpoint(
            task_id,
            actor(harness.owner_principal_id),
            Some(checkpoint.checkpoint_id),
        )
        .await
        .expect_err("invalid checkpoint status must be rejected");

    assert!(matches!(
        error,
        agentos_mrr::RuntimeError::InvalidEnumValue {
            kind: "task_status",
            ..
        }
    ));

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let restore_events: Vec<_> = events
        .iter()
        .filter(|event| event.event_type.starts_with("checkpoint.restore."))
        .collect();
    assert!(restore_events.len() >= 3);
    assert_eq!(
        restore_events
            .last()
            .expect("restore outcome should exist")
            .event_type,
        "checkpoint.restore.failed"
    );
    assert_eq!(
        restore_events
            .last()
            .expect("restore outcome should exist")
            .payload["reason_code"],
        "checkpoint_status_invalid"
    );
}

#[tokio::test]
async fn restore_rejects_checkpoint_with_invalid_payload_and_logs_failure() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "checkpoint-invalid-payload").await;

    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");

    let checkpoint = harness
        .manager
        .create_checkpoint(
            task_id,
            actor(harness.owner_principal_id),
            json!({"reason_code": "before_payload_corruption"}),
        )
        .await
        .expect("checkpoint should succeed");

    sqlx::query("UPDATE checkpoints SET payload = ? WHERE checkpoint_id = ?")
        .bind(r#"{"checkpoint_version":"agentos.checkpoint.v0.1"}"#)
        .bind(checkpoint.checkpoint_id.to_string())
        .execute(&harness.pool)
        .await
        .expect("checkpoint payload should be mutable for test corruption");

    let error = harness
        .manager
        .restore_checkpoint(
            task_id,
            actor(harness.owner_principal_id),
            Some(checkpoint.checkpoint_id),
        )
        .await
        .expect_err("invalid checkpoint payload must be rejected");

    assert!(matches!(error, agentos_mrr::RuntimeError::Json(_)));

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let restore_events: Vec<_> = events
        .iter()
        .filter(|event| event.event_type.starts_with("checkpoint.restore."))
        .collect();
    assert!(restore_events.len() >= 3);
    assert_eq!(
        restore_events
            .last()
            .expect("restore outcome should exist")
            .event_type,
        "checkpoint.restore.failed"
    );
    assert_eq!(
        restore_events
            .last()
            .expect("restore outcome should exist")
            .payload["reason_code"],
        "checkpoint_payload_invalid"
    );
}

#[tokio::test]
async fn cts_20_task_failure_is_recorded_in_task_family() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "failure-family").await;

    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");

    harness
        .manager
        .fail_task(
            task_id,
            actor(harness.owner_principal_id),
            json!({"reason_code": "task_logic_failure", "summary": "simulated failure"}),
        )
        .await
        .expect("fail should succeed");

    let task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(task.status, TaskStatus::Failed);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let failure_event = events
        .iter()
        .find(|event| event.event_type == "task.failed")
        .expect("task.failed event should exist");

    assert_eq!(failure_event.event_family, EventFamily::Task);
    assert_eq!(failure_event.payload["to_status"], "failed");
    assert_eq!(
        failure_event.payload["details"]["failure"]["reason_code"],
        "task_logic_failure"
    );
}

#[tokio::test]
async fn non_owner_mutations_are_rejected_without_side_effects() {
    let harness = setup_harness().await;
    let other_principal_id = Uuid::from_u128(2);
    create_active_principal(&harness, other_principal_id, "Secondary Operator").await;

    let task_id = create_task(&harness, "owner-only-mutations").await;

    let start_error = harness
        .manager
        .start_task(task_id, actor(other_principal_id))
        .await
        .expect_err("non-owner start must be rejected");
    assert!(matches!(
        start_error,
        agentos_mrr::RuntimeError::UnauthorizedTaskAction {
            task_id: rejected_task_id,
            principal_id,
        } if rejected_task_id == task_id
            && principal_id == other_principal_id
    ));

    let task_after_rejected_start = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should remain queryable");
    assert_eq!(task_after_rejected_start.status, TaskStatus::Created);

    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("owner start should succeed");

    let pause_error = harness
        .manager
        .pause_task(task_id, actor(other_principal_id))
        .await
        .expect_err("non-owner pause must be rejected");
    assert!(matches!(
        pause_error,
        agentos_mrr::RuntimeError::UnauthorizedTaskAction { .. }
    ));

    let task_after_rejected_pause = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should remain queryable");
    assert_eq!(task_after_rejected_pause.status, TaskStatus::Running);

    let checkpoint = harness
        .manager
        .create_checkpoint(
            task_id,
            actor(harness.owner_principal_id),
            json!({"reason_code": "authorized_snapshot"}),
        )
        .await
        .expect("owner checkpoint should succeed");

    let checkpoint_error = harness
        .manager
        .create_checkpoint(
            task_id,
            actor(other_principal_id),
            json!({"reason_code": "unauthorized_snapshot"}),
        )
        .await
        .expect_err("non-owner checkpoint create must be rejected");
    assert!(matches!(
        checkpoint_error,
        agentos_mrr::RuntimeError::UnauthorizedTaskAction { .. }
    ));

    let restore_error = harness
        .manager
        .restore_checkpoint(
            task_id,
            actor(other_principal_id),
            Some(checkpoint.checkpoint_id),
        )
        .await
        .expect_err("non-owner restore must be rejected");
    assert!(matches!(
        restore_error,
        agentos_mrr::RuntimeError::UnauthorizedTaskAction { .. }
    ));

    let task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should remain queryable");
    assert_eq!(task.status, TaskStatus::Running);
    assert_eq!(task.checkpoint_ref, Some(checkpoint.checkpoint_id));

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let event_types: Vec<_> = events
        .iter()
        .map(|event| event.event_type.as_str())
        .collect();
    assert_eq!(
        event_types,
        vec![
            "task.created",
            "task.ready",
            "task.started",
            "checkpoint.created"
        ]
    );
}

#[tokio::test]
async fn restore_rejects_checkpoint_with_event_boundary_mismatch_and_logs_failure() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "checkpoint-boundary-mismatch").await;

    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");

    let checkpoint = harness
        .manager
        .create_checkpoint(
            task_id,
            actor(harness.owner_principal_id),
            json!({"reason_code": "before_boundary_corruption"}),
        )
        .await
        .expect("checkpoint should succeed");

    sqlx::query(
        "UPDATE checkpoints SET payload = json_set(payload, '$.event_boundary_sequence_number', ?) WHERE checkpoint_id = ?",
    )
    .bind(checkpoint.event_sequence_number + 1)
    .bind(checkpoint.checkpoint_id.to_string())
    .execute(&harness.pool)
    .await
    .expect("checkpoint payload should be mutable for test corruption");

    let error = harness
        .manager
        .restore_checkpoint(
            task_id,
            actor(harness.owner_principal_id),
            Some(checkpoint.checkpoint_id),
        )
        .await
        .expect_err("checkpoint event boundary mismatch must be rejected");

    assert!(matches!(
        error,
        agentos_mrr::RuntimeError::InvariantViolation(message)
        if message.contains("event boundary mismatch")
    ));

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let restore_events: Vec<_> = events
        .iter()
        .filter(|event| event.event_type.starts_with("checkpoint.restore."))
        .collect();
    assert!(restore_events.len() >= 3);
    assert_eq!(
        restore_events
            .last()
            .expect("restore outcome should exist")
            .event_type,
        "checkpoint.restore.failed"
    );
    assert_eq!(
        restore_events
            .last()
            .expect("restore outcome should exist")
            .payload["reason_code"],
        "checkpoint_event_boundary_mismatch"
    );
}

#[tokio::test]
async fn latest_state_ref_matches_last_successful_event() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "durability-order").await;

    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");
    let task = harness
        .manager
        .pause_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("pause should succeed");

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let last = events.last().expect("at least one event expected");

    assert_eq!(task.status, TaskStatus::Paused);
    assert_eq!(last.event_type, "task.paused");
    assert_eq!(
        last.state_version_ref,
        Some(task.working_state_ref.state_version_id)
    );
}

#[tokio::test]
async fn control_signal_pause_and_resume_are_recorded_and_applied() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "control-pause-resume").await;

    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");

    let pause_signal = harness
        .manager
        .submit_control_signal(
            task_id,
            actor(harness.owner_principal_id),
            SubmitControlSignalCommand {
                signal_type: ControlSignalType::Pause,
                payload: json!({"reason_code": "manual_pause"}),
            },
        )
        .await
        .expect("pause signal should be recorded");
    assert_eq!(pause_signal.signal_type, ControlSignalType::Pause);
    assert_eq!(pause_signal.status, ControlSignalStatus::Applied);
    assert_eq!(pause_signal.issuer_principal_id, harness.owner_principal_id);
    assert!(pause_signal.applied_at.is_some());

    let paused_task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("paused task should load");
    assert_eq!(paused_task.status, TaskStatus::Paused);

    let resume_signal = harness
        .manager
        .submit_control_signal(
            task_id,
            actor(harness.owner_principal_id),
            SubmitControlSignalCommand {
                signal_type: ControlSignalType::Resume,
                payload: json!({"reason_code": "manual_resume"}),
            },
        )
        .await
        .expect("resume signal should be recorded");
    assert_eq!(resume_signal.signal_type, ControlSignalType::Resume);
    assert_eq!(resume_signal.status, ControlSignalStatus::Applied);
    assert!(resume_signal.applied_at.is_some());

    let running_task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("running task should load");
    assert_eq!(running_task.status, TaskStatus::Running);

    let signals = harness
        .manager
        .list_control_signals_by_task(task_id)
        .await
        .expect("control signals should load");
    assert_eq!(signals.len(), 2);
    assert!(signals
        .iter()
        .all(|signal| signal.status == ControlSignalStatus::Applied));

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let event_types: Vec<_> = events
        .iter()
        .map(|event| event.event_type.as_str())
        .collect();
    assert_eq!(
        event_types,
        vec![
            "task.created",
            "task.ready",
            "task.started",
            "control.signal.received",
            "task.paused",
            "control.signal.applied",
            "control.signal.received",
            "task.resumed",
            "control.signal.applied",
        ]
    );

    let pause_received = &events[3];
    let pause_applied = &events[5];
    assert_eq!(pause_received.event_family, EventFamily::Control);
    assert_eq!(pause_applied.event_family, EventFamily::Control);
    assert_eq!(pause_received.payload["action"], "pause");
    assert_eq!(pause_applied.payload["action"], "pause");
    assert_eq!(
        pause_received.principal_id,
        Some(harness.owner_principal_id)
    );
    assert_eq!(pause_applied.principal_id, Some(harness.owner_principal_id));
    assert_eq!(pause_received.correlation_id, pause_applied.correlation_id);
}

#[tokio::test]
async fn control_signal_terminate_cancels_task_and_is_auditable() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "control-terminate").await;

    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");

    let terminate_signal = harness
        .manager
        .submit_control_signal(
            task_id,
            actor(harness.owner_principal_id),
            SubmitControlSignalCommand::new(ControlSignalType::Terminate),
        )
        .await
        .expect("terminate signal should be recorded");
    assert_eq!(terminate_signal.status, ControlSignalStatus::Applied);

    let task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(task.status, TaskStatus::Cancelled);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let last = events.last().expect("at least one event should exist");
    assert_eq!(last.event_type, "control.signal.applied");
    assert_eq!(last.payload["action"], "terminate");
    assert_eq!(last.payload["status_after"], "cancelled");
    assert_eq!(events[4].event_type, "task.cancelled");
    assert_eq!(
        events[4].payload["details"]["reason_code"],
        "terminated_by_control_signal"
    );
}

#[tokio::test]
async fn control_signal_rejection_is_recorded_without_task_side_effects() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "control-rejection").await;

    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");
    harness
        .manager
        .complete_task(
            task_id,
            actor(harness.owner_principal_id),
            json!({"summary": "done"}),
        )
        .await
        .expect("complete should succeed");

    let rejected_signal = harness
        .manager
        .submit_control_signal(
            task_id,
            actor(harness.owner_principal_id),
            SubmitControlSignalCommand::new(ControlSignalType::Pause),
        )
        .await
        .expect("rejected signal should still be recorded");
    assert_eq!(rejected_signal.status, ControlSignalStatus::Rejected);
    assert!(rejected_signal.applied_at.is_none());

    let task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(task.status, TaskStatus::Completed);

    let signals = harness
        .manager
        .list_control_signals_by_task(task_id)
        .await
        .expect("control signals should load");
    assert_eq!(signals.len(), 1);
    assert_eq!(signals[0].status, ControlSignalStatus::Rejected);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let last_two: Vec<_> = events
        .iter()
        .rev()
        .take(2)
        .map(|event| event.event_type.as_str())
        .collect();
    assert_eq!(
        last_two,
        vec!["control.signal.rejected", "control.signal.received"]
    );

    let rejected = events.last().expect("rejected event should exist");
    assert_eq!(rejected.payload["action"], "pause");
    assert_eq!(
        rejected.payload["reason_code"],
        "terminal_state_irreversible"
    );
}

#[tokio::test]
async fn cancel_from_created_is_rejected_and_logged() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "cancel-created").await;

    let error = harness
        .manager
        .cancel_task(
            task_id,
            actor(harness.owner_principal_id),
            json!({"reason_code": "operator_cancel"}),
        )
        .await
        .expect_err("created task must not cancel directly");

    assert!(matches!(
        error,
        agentos_mrr::RuntimeError::InvalidTransition { .. }
    ));

    let task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should remain queryable");
    assert_eq!(task.status, TaskStatus::Created);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    assert_eq!(events[1].event_type, "task.transition.rejected");
    assert_eq!(events[1].payload["to_status"], "cancelled");
}

#[tokio::test]
async fn non_owner_control_signal_is_allowed_and_auditable() {
    let harness = setup_harness().await;
    let supervisor_principal_id = Uuid::from_u128(2);
    create_active_principal(&harness, supervisor_principal_id, "Supervisor").await;

    let task_id = create_task(&harness, "supervisor-control").await;
    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");

    let supervisor = PrincipalAttribution {
        principal_id: supervisor_principal_id,
        principal_role: "supervising".to_owned(),
    };
    let signal = harness
        .manager
        .submit_control_signal(
            task_id,
            supervisor.clone(),
            SubmitControlSignalCommand::new(ControlSignalType::Pause),
        )
        .await
        .expect("supervisor control signal should be accepted");
    assert_eq!(signal.status, ControlSignalStatus::Applied);
    assert_eq!(signal.issuer_principal_id, supervisor_principal_id);

    let task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(task.status, TaskStatus::Paused);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    assert_eq!(events[3].event_type, "control.signal.received");
    assert_eq!(events[3].principal_id, Some(supervisor_principal_id));
    assert_eq!(events[3].principal_role.as_deref(), Some("supervising"));
    assert_eq!(events[5].event_type, "control.signal.applied");
    assert_eq!(events[5].principal_id, Some(supervisor_principal_id));
}

#[tokio::test]
async fn control_signal_terminate_rejects_pre_start_tasks() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "terminate-created").await;

    let signal = harness
        .manager
        .submit_control_signal(
            task_id,
            actor(harness.owner_principal_id),
            SubmitControlSignalCommand::new(ControlSignalType::Terminate),
        )
        .await
        .expect("signal should be audited even when rejected");
    assert_eq!(signal.status, ControlSignalStatus::Rejected);

    let task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(task.status, TaskStatus::Created);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    assert_eq!(events[1].event_type, "control.signal.received");
    assert_eq!(events[2].event_type, "control.signal.rejected");
    assert_eq!(events[2].payload["reason_code"], "illegal_transition");
}

#[tokio::test]
async fn require_approval_is_resolved_by_approve_control_signal() {
    let harness = setup_harness().await;
    let approver_principal_id = Uuid::from_u128(2);
    create_active_principal(&harness, approver_principal_id, "Approver").await;

    let task_id = create_task(&harness, "approval-approve").await;
    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");
    harness
        .manager
        .require_approval(
            task_id,
            actor(harness.owner_principal_id),
            RequireApprovalCommand {
                approval_gate_id: "gate-1".to_owned(),
                blocked_call_id: Some("call-1".to_owned()),
                details: json!({"capability_id": "C2"}),
            },
        )
        .await
        .expect("approval gate should be created");

    let waiting_task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(waiting_task.status, TaskStatus::WaitingOnControl);

    let signal = harness
        .manager
        .submit_control_signal(
            task_id,
            PrincipalAttribution {
                principal_id: approver_principal_id,
                principal_role: "approving".to_owned(),
            },
            SubmitControlSignalCommand {
                signal_type: ControlSignalType::Approve,
                payload: json!({
                    "approval_gate_id": "gate-1",
                    "blocked_call_id": "call-1",
                }),
            },
        )
        .await
        .expect("approve signal should resolve gate");
    assert_eq!(signal.status, ControlSignalStatus::Applied);

    let running_task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(running_task.status, TaskStatus::Running);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let event_types: Vec<_> = events
        .iter()
        .map(|event| event.event_type.as_str())
        .collect();
    assert_eq!(
        event_types,
        vec![
            "task.created",
            "task.ready",
            "task.started",
            "policy.result.require_approval",
            "task.awaiting_control",
            "control.signal.received",
            "task.control.approved",
            "control.signal.applied",
        ]
    );
    assert_eq!(events[3].payload["approval_gate_id"], "gate-1");
    assert_eq!(events[5].payload["approval_gate_id"], "gate-1");
    assert_eq!(events[7].payload["status_after"], "running");
}

#[tokio::test]
async fn require_approval_is_resolved_by_deny_control_signal() {
    let harness = setup_harness().await;
    let approver_principal_id = Uuid::from_u128(2);
    create_active_principal(&harness, approver_principal_id, "Approver").await;

    let task_id = create_task(&harness, "approval-deny").await;
    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");
    harness
        .manager
        .require_approval(
            task_id,
            actor(harness.owner_principal_id),
            RequireApprovalCommand {
                approval_gate_id: "gate-deny".to_owned(),
                blocked_call_id: Some("call-deny".to_owned()),
                details: json!({"capability_id": "C2"}),
            },
        )
        .await
        .expect("approval gate should be created");

    let signal = harness
        .manager
        .submit_control_signal(
            task_id,
            PrincipalAttribution {
                principal_id: approver_principal_id,
                principal_role: "approving".to_owned(),
            },
            SubmitControlSignalCommand {
                signal_type: ControlSignalType::Deny,
                payload: json!({
                    "approval_gate_id": "gate-deny",
                    "blocked_call_id": "call-deny",
                }),
            },
        )
        .await
        .expect("deny signal should resolve gate");
    assert_eq!(signal.status, ControlSignalStatus::Applied);

    let task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(task.status, TaskStatus::Failed);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    assert_eq!(events[6].event_type, "task.failed");
    assert_eq!(
        events[6].payload["details"]["reason_code"],
        "approval_denied"
    );
    assert_eq!(events[7].event_type, "control.signal.applied");
    assert_eq!(events[7].payload["action"], "deny");
    assert_eq!(events[7].payload["status_after"], "failed");
}

#[tokio::test]
async fn deferred_control_signal_is_applied_at_next_safe_boundary() {
    let harness = setup_harness().await;
    let task_id = create_task(&harness, "deferred-control").await;

    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");

    let deferred = harness
        .manager
        .submit_control_signal(
            task_id,
            actor(harness.owner_principal_id),
            SubmitControlSignalCommand {
                signal_type: ControlSignalType::Pause,
                payload: json!({"unsafe_boundary": true}),
            },
        )
        .await
        .expect("deferred signal should be recorded");
    assert_eq!(deferred.status, ControlSignalStatus::Deferred);

    let running_task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(running_task.status, TaskStatus::Running);

    let applied = harness
        .manager
        .apply_pending_control_signals(task_id)
        .await
        .expect("pending signals should apply at safe boundary");
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].status, ControlSignalStatus::Applied);

    let paused_task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(paused_task.status, TaskStatus::Paused);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let event_types: Vec<_> = events
        .iter()
        .map(|event| event.event_type.as_str())
        .collect();
    assert_eq!(
        event_types,
        vec![
            "task.created",
            "task.ready",
            "task.started",
            "control.signal.received",
            "control.signal.deferred",
            "task.paused",
            "control.signal.applied",
        ]
    );
}

#[tokio::test]
async fn minimum_control_actions_are_supported() {
    let harness = setup_harness().await;
    let takeover_principal_id = Uuid::from_u128(2);
    let reassigned_principal_id = Uuid::from_u128(3);
    create_active_principal(&harness, takeover_principal_id, "Supervisor").await;
    create_active_principal(&harness, reassigned_principal_id, "Reassigned Owner").await;

    let task_id = create_task(&harness, "initial-goal").await;
    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("start should succeed");

    harness
        .manager
        .submit_control_signal(
            task_id,
            actor(harness.owner_principal_id),
            SubmitControlSignalCommand {
                signal_type: ControlSignalType::Steer,
                payload: json!({"goal": "steered-goal", "hint": "narrow scope"}),
            },
        )
        .await
        .expect("steer should apply");
    let steered_task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(steered_task.goal, "steered-goal");

    harness
        .manager
        .submit_control_signal(
            task_id,
            actor(harness.owner_principal_id),
            SubmitControlSignalCommand {
                signal_type: ControlSignalType::ModifyBudget,
                payload: json!({"budget_context_ref": "budget/restricted"}),
            },
        )
        .await
        .expect("modify_budget should apply");
    let budgeted_task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(budgeted_task.budget_context_ref, "budget/restricted");

    harness
        .manager
        .submit_control_signal(
            task_id,
            PrincipalAttribution {
                principal_id: takeover_principal_id,
                principal_role: "supervising".to_owned(),
            },
            SubmitControlSignalCommand::new(ControlSignalType::TakeOver),
        )
        .await
        .expect("take_over should apply");
    let taken_over_task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(taken_over_task.owner_principal_id, takeover_principal_id);

    harness
        .manager
        .submit_control_signal(
            task_id,
            PrincipalAttribution {
                principal_id: takeover_principal_id,
                principal_role: "supervising".to_owned(),
            },
            SubmitControlSignalCommand {
                signal_type: ControlSignalType::Reassign,
                payload: json!({"owner_principal_id": reassigned_principal_id.to_string()}),
            },
        )
        .await
        .expect("reassign should apply");
    let reassigned_task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(reassigned_task.owner_principal_id, reassigned_principal_id);

    harness
        .manager
        .submit_control_signal(
            task_id,
            actor(reassigned_principal_id),
            SubmitControlSignalCommand {
                signal_type: ControlSignalType::ModifyScope,
                payload: json!({"goal": "scoped-goal", "scope_patch": {"mode": "narrow"}}),
            },
        )
        .await
        .expect("modify_scope should apply");
    let scoped_task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load");
    assert_eq!(scoped_task.goal, "scoped-goal");

    let signals = harness
        .manager
        .list_control_signals_by_task(task_id)
        .await
        .expect("control signals should load");
    assert_eq!(signals.len(), 5);
    assert!(signals
        .iter()
        .all(|signal| signal.status == ControlSignalStatus::Applied));

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let event_types: Vec<_> = events
        .iter()
        .map(|event| event.event_type.as_str())
        .collect();
    assert!(event_types.contains(&"task.steered"));
    assert!(event_types.contains(&"task.budget_modified"));
    assert!(event_types.contains(&"task.taken_over"));
    assert!(event_types.contains(&"task.reassigned"));
    assert!(event_types.contains(&"task.scope_modified"));
}
