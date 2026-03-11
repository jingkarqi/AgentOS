use std::convert::TryFrom;
use std::sync::Arc;

use agentos_mrr::{
    CreateTaskCommand, DeterministicIdGenerator, EventFamily, FixedClock, Principal,
    PrincipalAttribution, PrincipalStatus, RuntimeDb, TaskManager, TaskManagerConfig, TaskStatus,
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
