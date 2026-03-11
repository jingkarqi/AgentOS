use std::convert::TryFrom;
use std::sync::Arc;

use agentos_mrr::{
    CreateTaskCommand, DeterministicIdGenerator, EventFamily, FixedClock, Principal,
    PrincipalAttribution, PrincipalStatus, RuntimeDb, TaskManager, TaskManagerConfig, TaskStatus,
};
use serde_json::json;
use time::OffsetDateTime;
use uuid::Uuid;

struct TestHarness {
    manager: TaskManager,
    owner_principal_id: Uuid,
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
    }
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
