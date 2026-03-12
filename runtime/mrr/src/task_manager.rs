use std::convert::TryFrom;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{Executor, FromRow, Sqlite, SqlitePool, Transaction};
use uuid::Uuid;

use crate::checkpoint_store::{CheckpointRecord, CheckpointStore, NewCheckpointRecord};
use crate::clock::{Clock, IdGenerator};
use crate::control_signal::{
    ControlSignalRecord, ControlSignalStatus, ControlSignalStore, ControlSignalType,
    NewControlSignalRecord, SubmitControlSignalCommand,
};
use crate::db::{decode_timestamp, encode_timestamp, parse_optional_uuid, parse_uuid};
use crate::error::{Result, RuntimeError};
use crate::event_log::{EventDraft, EventEnvelope, EventFamily, EventLog};
use crate::principal::{PrincipalAttribution, PrincipalStatus, PrincipalStore};
use crate::state_store::{NewStateVersion, StateStore, StateVersionRef, TaskStateVersion};
use crate::task::{CreateTaskCommand, Task, TaskStatus};

#[derive(Debug, Clone)]
pub struct TaskManagerConfig {
    pub emitted_by: String,
    pub outbox_publication_topic: String,
}

impl Default for TaskManagerConfig {
    fn default() -> Self {
        Self {
            emitted_by: "task_manager".to_owned(),
            outbox_publication_topic: "task.events".to_owned(),
        }
    }
}

const CHECKPOINT_VERSION: &str = "agentos.checkpoint.v0.1";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckpointPayload {
    checkpoint_version: String,
    task_status: String,
    policy_context_ref: String,
    budget_context_ref: String,
    history_ref: String,
    result_ref: Option<String>,
    event_boundary_sequence_number: i64,
    in_flight_capability: Option<Value>,
    details: Value,
}

#[derive(Debug, Clone)]
pub struct RequireApprovalCommand {
    pub approval_gate_id: String,
    pub blocked_call_id: Option<String>,
    pub details: Value,
}

impl RequireApprovalCommand {
    pub fn new(approval_gate_id: impl Into<String>) -> Self {
        Self {
            approval_gate_id: approval_gate_id.into(),
            blocked_call_id: None,
            details: json!({}),
        }
    }
}

struct StatusChange<'a> {
    to_status: TaskStatus,
    event_type: &'a str,
    details: Value,
    result_ref: Option<String>,
    correlation_id: Option<String>,
    causation_event_id: Option<Uuid>,
}

struct TransitionRequest<'a> {
    allowed_from: &'a [TaskStatus],
    change: StatusChange<'a>,
}

struct RestoreFailure<'a> {
    requested_checkpoint_id: Option<Uuid>,
    correlation_id: &'a str,
    causation_event_id: Uuid,
    restore_source: &'a str,
    reason_code: &'a str,
}

struct AppliedStatusChange {
    event: EventEnvelope,
    state_version_ref: Uuid,
}

struct TaskMutation<'a> {
    event_type: &'a str,
    details: Value,
    goal: Option<String>,
    owner_principal_id: Option<Uuid>,
    budget_context_ref: Option<String>,
    correlation_id: Option<String>,
    causation_event_id: Option<Uuid>,
}

#[derive(Clone)]
pub struct TaskManager {
    pool: SqlitePool,
    principal_store: PrincipalStore,
    event_log: EventLog,
    state_store: StateStore,
    checkpoint_store: CheckpointStore,
    control_signal_store: ControlSignalStore,
    clock: Arc<dyn Clock>,
    id_generator: Arc<dyn IdGenerator>,
    config: TaskManagerConfig,
}

impl TaskManager {
    pub fn new(
        pool: SqlitePool,
        clock: Arc<dyn Clock>,
        id_generator: Arc<dyn IdGenerator>,
        config: TaskManagerConfig,
    ) -> Self {
        Self {
            principal_store: PrincipalStore::new(pool.clone()),
            event_log: EventLog::new(config.outbox_publication_topic.clone()),
            state_store: StateStore,
            checkpoint_store: CheckpointStore,
            control_signal_store: ControlSignalStore,
            pool,
            clock,
            id_generator,
            config,
        }
    }

    pub fn principal_store(&self) -> PrincipalStore {
        self.principal_store.clone()
    }

    pub fn checkpoint_store(&self) -> CheckpointStore {
        self.checkpoint_store.clone()
    }

    pub fn control_signal_store(&self) -> ControlSignalStore {
        self.control_signal_store.clone()
    }

    pub async fn create_task(&self, command: CreateTaskCommand) -> Result<Task> {
        let now = self.clock.now();
        let task_id = self.id_generator.next_uuid();
        let state_version_id = self.id_generator.next_uuid();
        let history_ref = format!("task://{task_id}/events");
        let goal = command.goal;
        let owner_principal_id = command.owner_principal_id;
        let policy_context_ref = command.policy_context_ref;
        let budget_context_ref = command.budget_context_ref;
        let priority = command.priority;
        let initial_state = command.initial_state;

        let mut tx = self.pool.begin().await?;
        self.ensure_active_principal_in_tx(&mut tx, owner_principal_id)
            .await?;

        let state_version = self
            .state_store
            .append_version(
                &mut tx,
                NewStateVersion {
                    state_version_id,
                    task_id,
                    status: TaskStatus::Created,
                    payload: json!({
                        "status": TaskStatus::Created.as_str(),
                        "goal": goal.clone(),
                        "data": initial_state,
                    }),
                    created_at: now,
                    created_by: self.config.emitted_by.clone(),
                },
            )
            .await?;

        sqlx::query(
            r#"
            INSERT INTO tasks (
                task_id,
                goal,
                status,
                owner_principal_id,
                created_at,
                updated_at,
                policy_context_ref,
                working_state_ref,
                history_ref,
                checkpoint_ref,
                budget_context_ref,
                priority,
                result_ref
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(task_id.to_string())
        .bind(&goal)
        .bind(TaskStatus::Created.as_str())
        .bind(owner_principal_id.to_string())
        .bind(encode_timestamp(now)?)
        .bind(encode_timestamp(now)?)
        .bind(&policy_context_ref)
        .bind(state_version.state_version_ref.state_version_id.to_string())
        .bind(&history_ref)
        .bind(Option::<String>::None)
        .bind(&budget_context_ref)
        .bind(priority)
        .bind(Option::<String>::None)
        .execute(&mut *tx)
        .await?;

        self.event_log
            .append(
                &mut tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "task.created".to_owned(),
                    event_family: EventFamily::Task,
                    task_id,
                    occurred_at: now,
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "to_status": TaskStatus::Created.as_str(),
                        "goal": goal,
                        "owner_principal_id": owner_principal_id,
                    }),
                    correlation_id: None,
                    causation_event_id: None,
                    principal: Some(PrincipalAttribution::owning(owner_principal_id)),
                    policy_context_ref: Some(policy_context_ref),
                    budget_context_ref: Some(budget_context_ref),
                    checkpoint_ref: None,
                    state_version_ref: Some(state_version.state_version_ref.state_version_id),
                    evidence_ref: None,
                    trace_ref: None,
                    tenant_id: None,
                },
            )
            .await?;

        tx.commit().await?;
        self.get_task(task_id).await
    }

    pub async fn start_task(&self, task_id: Uuid, actor: PrincipalAttribution) -> Result<Task> {
        let mut tx = self.pool.begin().await?;
        let row = fetch_task_row(&mut *tx, task_id).await?;
        self.ensure_task_actor_is_owner(&mut tx, &row, &actor)
            .await?;

        match row.status()? {
            TaskStatus::Created => {
                self.apply_status_change(
                    &mut tx,
                    &row,
                    actor.clone(),
                    StatusChange {
                        to_status: TaskStatus::Ready,
                        event_type: "task.ready",
                        details: json!({"reason_code": "ready_for_execution"}),
                        result_ref: None,
                        correlation_id: None,
                        causation_event_id: None,
                    },
                )
                .await?;

                let ready_row = fetch_task_row(&mut *tx, task_id).await?;
                self.apply_status_change(
                    &mut tx,
                    &ready_row,
                    actor,
                    StatusChange {
                        to_status: TaskStatus::Running,
                        event_type: "task.started",
                        details: json!({"reason_code": "started"}),
                        result_ref: None,
                        correlation_id: None,
                        causation_event_id: None,
                    },
                )
                .await?;
            }
            TaskStatus::Ready => {
                self.apply_status_change(
                    &mut tx,
                    &row,
                    actor,
                    StatusChange {
                        to_status: TaskStatus::Running,
                        event_type: "task.started",
                        details: json!({"reason_code": "started"}),
                        result_ref: None,
                        correlation_id: None,
                        causation_event_id: None,
                    },
                )
                .await?;
            }
            current => {
                self.record_transition_rejection(&mut tx, &row, TaskStatus::Running, actor)
                    .await?;
                tx.commit().await?;
                return Err(RuntimeError::InvalidTransition {
                    task_id,
                    from_status: current.as_str().to_owned(),
                    to_status: TaskStatus::Running.as_str().to_owned(),
                });
            }
        }

        tx.commit().await?;
        self.get_task(task_id).await
    }

    pub async fn pause_task(&self, task_id: Uuid, actor: PrincipalAttribution) -> Result<Task> {
        self.transition_with_allowed_from(
            task_id,
            actor,
            TransitionRequest {
                allowed_from: &[TaskStatus::Running],
                change: StatusChange {
                    to_status: TaskStatus::Paused,
                    event_type: "task.paused",
                    details: json!({"reason_code": "paused_by_operator"}),
                    result_ref: None,
                    correlation_id: None,
                    causation_event_id: None,
                },
            },
        )
        .await
    }

    pub async fn resume_task(&self, task_id: Uuid, actor: PrincipalAttribution) -> Result<Task> {
        self.transition_with_allowed_from(
            task_id,
            actor,
            TransitionRequest {
                allowed_from: &[TaskStatus::Paused],
                change: StatusChange {
                    to_status: TaskStatus::Running,
                    event_type: "task.resumed",
                    details: json!({"reason_code": "resumed_by_operator"}),
                    result_ref: None,
                    correlation_id: None,
                    causation_event_id: None,
                },
            },
        )
        .await
    }

    pub async fn complete_task(
        &self,
        task_id: Uuid,
        actor: PrincipalAttribution,
        result_payload: Value,
    ) -> Result<Task> {
        let result_ref = Some(format!("result://task/{task_id}/completed"));
        self.transition_with_allowed_from(
            task_id,
            actor,
            TransitionRequest {
                allowed_from: &[TaskStatus::Running],
                change: StatusChange {
                    to_status: TaskStatus::Completed,
                    event_type: "task.completed",
                    details: json!({ "result": result_payload }),
                    result_ref,
                    correlation_id: None,
                    causation_event_id: None,
                },
            },
        )
        .await
    }

    pub async fn fail_task(
        &self,
        task_id: Uuid,
        actor: PrincipalAttribution,
        failure_payload: Value,
    ) -> Result<Task> {
        let result_ref = Some(format!("result://task/{task_id}/failed"));
        self.transition_with_allowed_from(
            task_id,
            actor,
            TransitionRequest {
                allowed_from: &[TaskStatus::Running],
                change: StatusChange {
                    to_status: TaskStatus::Failed,
                    event_type: "task.failed",
                    details: json!({ "failure": failure_payload }),
                    result_ref,
                    correlation_id: None,
                    causation_event_id: None,
                },
            },
        )
        .await
    }

    pub async fn cancel_task(
        &self,
        task_id: Uuid,
        actor: PrincipalAttribution,
        cancellation_payload: Value,
    ) -> Result<Task> {
        self.transition_with_allowed_from(
            task_id,
            actor,
            TransitionRequest {
                allowed_from: &[TaskStatus::Running, TaskStatus::Paused],
                change: StatusChange {
                    to_status: TaskStatus::Cancelled,
                    event_type: "task.cancelled",
                    details: json!({ "cancellation": cancellation_payload }),
                    result_ref: None,
                    correlation_id: None,
                    causation_event_id: None,
                },
            },
        )
        .await
    }

    pub async fn require_approval(
        &self,
        task_id: Uuid,
        actor: PrincipalAttribution,
        command: RequireApprovalCommand,
    ) -> Result<Task> {
        let mut tx = self.pool.begin().await?;
        let row = fetch_task_row(&mut *tx, task_id).await?;
        self.ensure_task_actor_is_owner(&mut tx, &row, &actor)
            .await?;

        let current_status = row.status()?;
        if current_status != TaskStatus::Running {
            self.record_transition_rejection(&mut tx, &row, TaskStatus::WaitingOnControl, actor)
                .await?;
            tx.commit().await?;
            return Err(RuntimeError::InvalidTransition {
                task_id,
                from_status: current_status.as_str().to_owned(),
                to_status: TaskStatus::WaitingOnControl.as_str().to_owned(),
            });
        }

        let current_state_version_ref = parse_uuid("working_state_ref", &row.working_state_ref)?;
        let checkpoint_ref = parse_optional_uuid("checkpoint_ref", row.checkpoint_ref.clone())?;
        let correlation_id = format!("approval:gate:{}", command.approval_gate_id);
        let now = self.clock.now();

        let policy_event = self
            .event_log
            .append(
                &mut tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "policy.result.require_approval".to_owned(),
                    event_family: EventFamily::Policy,
                    task_id,
                    occurred_at: now,
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "outcome": "require_approval",
                        "reason_code": "requires_higher_authority",
                        "approval_gate_id": command.approval_gate_id,
                        "blocked_call_id": command.blocked_call_id,
                        "details": command.details.clone(),
                    }),
                    correlation_id: Some(correlation_id.clone()),
                    causation_event_id: None,
                    principal: Some(actor.clone()),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref,
                    state_version_ref: Some(current_state_version_ref),
                    evidence_ref: None,
                    trace_ref: None,
                    tenant_id: None,
                },
            )
            .await?;

        self.apply_status_change(
            &mut tx,
            &row,
            actor,
            StatusChange {
                to_status: TaskStatus::WaitingOnControl,
                event_type: "task.awaiting_control",
                details: json!({
                    "reason_code": "require_approval",
                    "approval_gate_id": command.approval_gate_id,
                    "blocked_call_id": command.blocked_call_id,
                    "details": command.details,
                }),
                result_ref: None,
                correlation_id: Some(correlation_id),
                causation_event_id: Some(policy_event.event_id),
            },
        )
        .await?;

        tx.commit().await?;
        self.get_task(task_id).await
    }

    pub async fn get_task(&self, task_id: Uuid) -> Result<Task> {
        let row = fetch_task_row(&self.pool, task_id).await?;
        row.into_task()
    }

    pub async fn get_latest_checkpoint_for_task(
        &self,
        task_id: Uuid,
    ) -> Result<Option<CheckpointRecord>> {
        self.checkpoint_store
            .get_latest_for_task(&self.pool, task_id)
            .await
    }

    pub async fn get_checkpoint_for_task(
        &self,
        task_id: Uuid,
        checkpoint_id: Uuid,
    ) -> Result<Option<CheckpointRecord>> {
        self.checkpoint_store
            .get_by_id_for_task(&self.pool, task_id, checkpoint_id)
            .await
    }

    pub async fn list_events_by_task(&self, task_id: Uuid) -> Result<Vec<EventEnvelope>> {
        self.event_log.list_by_task(&self.pool, task_id).await
    }

    pub async fn list_control_signals_by_task(
        &self,
        task_id: Uuid,
    ) -> Result<Vec<ControlSignalRecord>> {
        self.control_signal_store
            .list_by_task(&self.pool, task_id)
            .await
    }

    pub async fn apply_pending_control_signals(
        &self,
        task_id: Uuid,
    ) -> Result<Vec<ControlSignalRecord>> {
        let mut tx = self.pool.begin().await?;
        fetch_task_row(&mut *tx, task_id).await?;

        let deferred = self
            .control_signal_store
            .list_deferred_by_task_in(&mut *tx, task_id)
            .await?;
        let mut applied = Vec::with_capacity(deferred.len());

        for signal in deferred {
            let row = fetch_task_row(&mut *tx, task_id).await?;
            let current_state_version_ref =
                parse_uuid("working_state_ref", &row.working_state_ref)?;
            let checkpoint_ref = parse_optional_uuid("checkpoint_ref", row.checkpoint_ref.clone())?;
            let updated = self
                .apply_control_signal_in_tx(
                    &mut tx,
                    &row,
                    PrincipalAttribution {
                        principal_id: signal.issuer_principal_id,
                        principal_role: signal.issuer_principal_role.clone(),
                    },
                    signal.signal_id,
                    signal.signal_type.clone(),
                    signal.payload.clone(),
                    &signal.correlation_id,
                    signal.received_event_id,
                    current_state_version_ref,
                    checkpoint_ref,
                    false,
                )
                .await?;
            applied.push(updated);
        }

        tx.commit().await?;
        Ok(applied)
    }

    pub async fn submit_control_signal(
        &self,
        task_id: Uuid,
        actor: PrincipalAttribution,
        command: SubmitControlSignalCommand,
    ) -> Result<ControlSignalRecord> {
        let mut tx = self.pool.begin().await?;
        let row = fetch_task_row(&mut *tx, task_id).await?;
        self.ensure_active_principal_in_tx(&mut tx, actor.principal_id)
            .await?;

        let current_status = row.status()?;
        let current_state_version_ref = parse_uuid("working_state_ref", &row.working_state_ref)?;
        let checkpoint_ref = parse_optional_uuid("checkpoint_ref", row.checkpoint_ref.clone())?;
        let now = self.clock.now();
        let signal_id = self.id_generator.next_uuid();
        let correlation_id = format!("control:signal:{signal_id}");
        let (approval_gate_id, blocked_call_id) = control_signal_linkage(&command.payload);

        let received = self
            .event_log
            .append(
                &mut tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "control.signal.received".to_owned(),
                    event_family: EventFamily::Control,
                    task_id,
                    occurred_at: now,
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "signal_id": signal_id,
                        "action": command.signal_type.as_str(),
                        "issuer_principal_id": actor.principal_id,
                        "requested_effect": command.signal_type.as_str(),
                        "status_before": current_status.as_str(),
                        "approval_gate_id": approval_gate_id,
                        "blocked_call_id": blocked_call_id,
                        "details": command.payload.clone(),
                    }),
                    correlation_id: Some(correlation_id.clone()),
                    causation_event_id: None,
                    principal: Some(actor.clone()),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref,
                    state_version_ref: Some(current_state_version_ref),
                    evidence_ref: None,
                    trace_ref: None,
                    tenant_id: None,
                },
            )
            .await?;

        let signal = self
            .control_signal_store
            .record(
                &mut tx,
                NewControlSignalRecord {
                    signal_id,
                    task_id,
                    signal_type: command.signal_type.clone(),
                    status: ControlSignalStatus::Received,
                    issuer_principal_id: actor.principal_id,
                    issuer_principal_role: actor.principal_role.clone(),
                    payload: command.payload.clone(),
                    correlation_id: correlation_id.clone(),
                    received_event_id: received.event_id,
                    received_sequence_number: received.sequence_number,
                    created_at: now,
                    applied_at: None,
                    outcome_event_id: None,
                    outcome_sequence_number: None,
                },
            )
            .await?;

        let applied = self
            .apply_control_signal_in_tx(
                &mut tx,
                &row,
                actor,
                signal.signal_id,
                signal.signal_type,
                command.payload,
                &correlation_id,
                received.event_id,
                current_state_version_ref,
                checkpoint_ref,
                true,
            )
            .await?;

        tx.commit().await?;
        Ok(applied)
    }

    pub async fn create_checkpoint(
        &self,
        task_id: Uuid,
        actor: PrincipalAttribution,
        details: Value,
    ) -> Result<CheckpointRecord> {
        let mut tx = self.pool.begin().await?;
        let row = fetch_task_row(&mut *tx, task_id).await?;
        self.ensure_task_actor_is_owner(&mut tx, &row, &actor)
            .await?;

        let status = row.status()?;
        let now = self.clock.now();
        let checkpoint_id = self.id_generator.next_uuid();
        let working_state_ref = parse_uuid("working_state_ref", &row.working_state_ref)?;
        let event_boundary_sequence_number = fetch_latest_event_sequence(&mut *tx, task_id).await?;
        let checkpoint_payload = CheckpointPayload {
            checkpoint_version: CHECKPOINT_VERSION.to_owned(),
            task_status: status.as_str().to_owned(),
            policy_context_ref: row.policy_context_ref.clone(),
            budget_context_ref: row.budget_context_ref.clone(),
            history_ref: row.history_ref.clone(),
            result_ref: row.result_ref.clone(),
            event_boundary_sequence_number,
            in_flight_capability: None,
            details: details.clone(),
        };

        let checkpoint = self
            .checkpoint_store
            .record(
                &mut tx,
                NewCheckpointRecord {
                    checkpoint_id,
                    task_id,
                    state_version_ref: working_state_ref,
                    event_sequence_number: event_boundary_sequence_number,
                    status: status.as_str().to_owned(),
                    created_at: now,
                    created_by: self.config.emitted_by.clone(),
                    payload: serde_json::to_value(&checkpoint_payload)?,
                },
            )
            .await?;

        sqlx::query(
            r#"
            UPDATE tasks
            SET
                checkpoint_ref = ?,
                updated_at = ?
            WHERE task_id = ?
            "#,
        )
        .bind(checkpoint.checkpoint_id.to_string())
        .bind(encode_timestamp(now)?)
        .bind(task_id.to_string())
        .execute(&mut *tx)
        .await?;

        self.event_log
            .append(
                &mut tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "checkpoint.created".to_owned(),
                    event_family: EventFamily::Checkpoint,
                    task_id,
                    occurred_at: now,
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "checkpoint_version": CHECKPOINT_VERSION,
                        "status": status.as_str(),
                        "event_boundary_sequence_number": event_boundary_sequence_number,
                        "details": details,
                    }),
                    correlation_id: Some(format!("checkpoint:create:{checkpoint_id}")),
                    causation_event_id: None,
                    principal: Some(actor),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref: Some(checkpoint_id),
                    state_version_ref: Some(working_state_ref),
                    evidence_ref: None,
                    trace_ref: None,
                    tenant_id: None,
                },
            )
            .await?;

        tx.commit().await?;
        Ok(checkpoint)
    }

    pub async fn restore_checkpoint(
        &self,
        task_id: Uuid,
        actor: PrincipalAttribution,
        checkpoint_id: Option<Uuid>,
    ) -> Result<Task> {
        let mut tx = self.pool.begin().await?;
        let row = fetch_task_row(&mut *tx, task_id).await?;
        self.ensure_task_actor_is_owner(&mut tx, &row, &actor)
            .await?;

        let current_status = row.status()?;
        let current_state_version_ref = parse_uuid("working_state_ref", &row.working_state_ref)?;
        let now = self.clock.now();
        let restore_source = if checkpoint_id.is_some() {
            "explicit_checkpoint"
        } else {
            "latest_checkpoint"
        };
        let correlation_id = match checkpoint_id {
            Some(checkpoint_id) => format!("checkpoint:restore:{checkpoint_id}"),
            None => format!("checkpoint:restore:latest:{task_id}"),
        };

        let restore_requested = self
            .event_log
            .append(
                &mut tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "checkpoint.restore.requested".to_owned(),
                    event_family: EventFamily::Checkpoint,
                    task_id,
                    occurred_at: now,
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "from_status": current_status.as_str(),
                        "restore_source": restore_source,
                        "requested_checkpoint_id": checkpoint_id.map(|value| value.to_string()),
                    }),
                    correlation_id: Some(correlation_id.clone()),
                    causation_event_id: None,
                    principal: Some(actor.clone()),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref: checkpoint_id,
                    state_version_ref: Some(current_state_version_ref),
                    evidence_ref: None,
                    trace_ref: None,
                    tenant_id: None,
                },
            )
            .await?;

        let restore_started = self
            .event_log
            .append(
                &mut tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "checkpoint.restore.started".to_owned(),
                    event_family: EventFamily::Checkpoint,
                    task_id,
                    occurred_at: now,
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "from_status": current_status.as_str(),
                        "restore_source": restore_source,
                        "requested_checkpoint_id": checkpoint_id.map(|value| value.to_string()),
                    }),
                    correlation_id: Some(correlation_id.clone()),
                    causation_event_id: Some(restore_requested.event_id),
                    principal: Some(actor.clone()),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref: checkpoint_id,
                    state_version_ref: Some(current_state_version_ref),
                    evidence_ref: None,
                    trace_ref: None,
                    tenant_id: None,
                },
            )
            .await?;

        let checkpoint_result = match checkpoint_id {
            Some(checkpoint_id) => self
                .checkpoint_store
                .get_by_id_for_task(&mut *tx, task_id, checkpoint_id)
                .await?
                .ok_or(RuntimeError::CheckpointNotFound {
                    task_id,
                    checkpoint_id,
                }),
            None => self
                .checkpoint_store
                .get_latest_for_task_in(&mut *tx, task_id)
                .await?
                .ok_or(RuntimeError::NoCheckpointForTask { task_id }),
        };

        let checkpoint = match checkpoint_result {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                self.record_restore_failure(
                    &mut tx,
                    &row,
                    actor,
                    RestoreFailure {
                        requested_checkpoint_id: checkpoint_id,
                        correlation_id: &correlation_id,
                        causation_event_id: restore_started.event_id,
                        restore_source,
                        reason_code: "checkpoint_not_found",
                    },
                    &error,
                )
                .await?;
                tx.commit().await?;
                return Err(error);
            }
        };

        let checkpoint_status: TaskStatus = match checkpoint.status.parse() {
            Ok(status) => status,
            Err(error) => {
                self.record_restore_failure(
                    &mut tx,
                    &row,
                    actor.clone(),
                    RestoreFailure {
                        requested_checkpoint_id: Some(checkpoint.checkpoint_id),
                        correlation_id: &correlation_id,
                        causation_event_id: restore_started.event_id,
                        restore_source,
                        reason_code: "checkpoint_status_invalid",
                    },
                    &error,
                )
                .await?;
                tx.commit().await?;
                return Err(error);
            }
        };

        let checkpoint_payload: CheckpointPayload =
            match serde_json::from_value(checkpoint.payload.clone()) {
                Ok(payload) => payload,
                Err(error) => {
                    let error = RuntimeError::from(error);
                    self.record_restore_failure(
                        &mut tx,
                        &row,
                        actor.clone(),
                        RestoreFailure {
                            requested_checkpoint_id: Some(checkpoint.checkpoint_id),
                            correlation_id: &correlation_id,
                            causation_event_id: restore_started.event_id,
                            restore_source,
                            reason_code: "checkpoint_payload_invalid",
                        },
                        &error,
                    )
                    .await?;
                    tx.commit().await?;
                    return Err(error);
                }
            };

        if checkpoint_payload.checkpoint_version != CHECKPOINT_VERSION {
            let error = RuntimeError::InvariantViolation(format!(
                "checkpoint {} has unsupported version {}",
                checkpoint.checkpoint_id, checkpoint_payload.checkpoint_version
            ));
            self.record_restore_failure(
                &mut tx,
                &row,
                actor.clone(),
                RestoreFailure {
                    requested_checkpoint_id: Some(checkpoint.checkpoint_id),
                    correlation_id: &correlation_id,
                    causation_event_id: restore_started.event_id,
                    restore_source,
                    reason_code: "checkpoint_version_unsupported",
                },
                &error,
            )
            .await?;
            tx.commit().await?;
            return Err(error);
        }

        if checkpoint_payload.task_status != checkpoint.status {
            let error = RuntimeError::InvariantViolation(format!(
                "checkpoint {} status mismatch between row and payload",
                checkpoint.checkpoint_id
            ));
            self.record_restore_failure(
                &mut tx,
                &row,
                actor.clone(),
                RestoreFailure {
                    requested_checkpoint_id: Some(checkpoint.checkpoint_id),
                    correlation_id: &correlation_id,
                    causation_event_id: restore_started.event_id,
                    restore_source,
                    reason_code: "checkpoint_status_mismatch",
                },
                &error,
            )
            .await?;
            tx.commit().await?;
            return Err(error);
        }

        if checkpoint_payload.event_boundary_sequence_number != checkpoint.event_sequence_number {
            let error = RuntimeError::InvariantViolation(format!(
                "checkpoint {} event boundary mismatch between row and payload",
                checkpoint.checkpoint_id
            ));
            self.record_restore_failure(
                &mut tx,
                &row,
                actor.clone(),
                RestoreFailure {
                    requested_checkpoint_id: Some(checkpoint.checkpoint_id),
                    correlation_id: &correlation_id,
                    causation_event_id: restore_started.event_id,
                    restore_source,
                    reason_code: "checkpoint_event_boundary_mismatch",
                },
                &error,
            )
            .await?;
            tx.commit().await?;
            return Err(error);
        }

        if current_status.is_terminal() && !checkpoint_status.is_terminal() {
            let error = RuntimeError::InvalidTransition {
                task_id,
                from_status: current_status.as_str().to_owned(),
                to_status: checkpoint_status.as_str().to_owned(),
            };
            self.record_restore_failure(
                &mut tx,
                &row,
                actor.clone(),
                RestoreFailure {
                    requested_checkpoint_id: Some(checkpoint.checkpoint_id),
                    correlation_id: &correlation_id,
                    causation_event_id: restore_started.event_id,
                    restore_source,
                    reason_code: "terminal_state_irreversible",
                },
                &error,
            )
            .await?;
            tx.commit().await?;
            return Err(error);
        }

        let restored_state = self
            .state_store
            .get_by_ref_in(&mut *tx, checkpoint.state_version_ref)
            .await?;
        let restored_state_version = self
            .state_store
            .append_version(
                &mut tx,
                NewStateVersion {
                    state_version_id: self.id_generator.next_uuid(),
                    task_id,
                    status: checkpoint_status.clone(),
                    payload: restored_state.payload,
                    created_at: now,
                    created_by: self.config.emitted_by.clone(),
                },
            )
            .await?;

        sqlx::query(
            r#"
            UPDATE tasks
            SET
                status = ?,
                updated_at = ?,
                policy_context_ref = ?,
                working_state_ref = ?,
                checkpoint_ref = ?,
                budget_context_ref = ?,
                result_ref = ?
            WHERE task_id = ?
            "#,
        )
        .bind(checkpoint_status.as_str())
        .bind(encode_timestamp(now)?)
        .bind(&checkpoint_payload.policy_context_ref)
        .bind(
            restored_state_version
                .state_version_ref
                .state_version_id
                .to_string(),
        )
        .bind(checkpoint.checkpoint_id.to_string())
        .bind(&checkpoint_payload.budget_context_ref)
        .bind(checkpoint_payload.result_ref.clone())
        .bind(task_id.to_string())
        .execute(&mut *tx)
        .await?;

        self.event_log
            .append(
                &mut tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "checkpoint.restore.succeeded".to_owned(),
                    event_family: EventFamily::Checkpoint,
                    task_id,
                    occurred_at: now,
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "from_status": current_status.as_str(),
                        "to_status": checkpoint_status.as_str(),
                        "restore_source": restore_source,
                        "checkpoint_version": checkpoint_payload.checkpoint_version,
                        "restored_event_sequence_number": checkpoint.event_sequence_number,
                    }),
                    correlation_id: Some(correlation_id),
                    causation_event_id: Some(restore_started.event_id),
                    principal: Some(actor),
                    policy_context_ref: Some(checkpoint_payload.policy_context_ref),
                    budget_context_ref: Some(checkpoint_payload.budget_context_ref),
                    checkpoint_ref: Some(checkpoint.checkpoint_id),
                    state_version_ref: Some(
                        restored_state_version.state_version_ref.state_version_id,
                    ),
                    evidence_ref: None,
                    trace_ref: None,
                    tenant_id: None,
                },
            )
            .await?;

        tx.commit().await?;
        self.get_task(task_id).await
    }

    async fn transition_with_allowed_from(
        &self,
        task_id: Uuid,
        actor: PrincipalAttribution,
        request: TransitionRequest<'_>,
    ) -> Result<Task> {
        let TransitionRequest {
            allowed_from,
            change,
        } = request;
        let mut tx = self.pool.begin().await?;
        let row = fetch_task_row(&mut *tx, task_id).await?;
        self.ensure_task_actor_is_owner(&mut tx, &row, &actor)
            .await?;

        let current = row.status()?;

        if !allowed_from.iter().any(|status| status == &current) {
            self.record_transition_rejection(&mut tx, &row, change.to_status.clone(), actor)
                .await?;
            tx.commit().await?;
            return Err(RuntimeError::InvalidTransition {
                task_id,
                from_status: current.as_str().to_owned(),
                to_status: change.to_status.as_str().to_owned(),
            });
        }

        self.apply_status_change(&mut tx, &row, actor, change)
            .await?;
        tx.commit().await?;
        self.get_task(task_id).await
    }

    async fn apply_status_change(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &TaskRow,
        actor: PrincipalAttribution,
        change: StatusChange<'_>,
    ) -> Result<AppliedStatusChange> {
        let StatusChange {
            to_status,
            event_type,
            details,
            result_ref,
            correlation_id,
            causation_event_id,
        } = change;
        let task_id = row.task_id()?;
        let from_status = row.status()?;
        let now = self.clock.now();
        let preserved_result_ref = result_ref.or_else(|| row.result_ref.clone());
        let state_details = details.clone();

        let state_version = self
            .state_store
            .append_version(
                tx,
                NewStateVersion {
                    state_version_id: self.id_generator.next_uuid(),
                    task_id,
                    status: to_status.clone(),
                    payload: json!({
                        "status": to_status.as_str(),
                        "applied_event_type": event_type,
                        "from_status": from_status.as_str(),
                        "details": state_details,
                    }),
                    created_at: now,
                    created_by: self.config.emitted_by.clone(),
                },
            )
            .await?;

        sqlx::query(
            r#"
            UPDATE tasks
            SET
                status = ?,
                updated_at = ?,
                working_state_ref = ?,
                checkpoint_ref = ?,
                result_ref = ?
            WHERE task_id = ?
            "#,
        )
        .bind(to_status.as_str())
        .bind(encode_timestamp(now)?)
        .bind(state_version.state_version_ref.state_version_id.to_string())
        .bind(row.checkpoint_ref.clone())
        .bind(preserved_result_ref)
        .bind(task_id.to_string())
        .execute(&mut **tx)
        .await?;

        let event = self
            .event_log
            .append(
                tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: event_type.to_owned(),
                    event_family: EventFamily::Task,
                    task_id,
                    occurred_at: now,
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "from_status": from_status.as_str(),
                        "to_status": to_status.as_str(),
                        "details": details,
                    }),
                    correlation_id,
                    causation_event_id,
                    principal: Some(actor),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref: parse_optional_uuid(
                        "checkpoint_ref",
                        row.checkpoint_ref.clone(),
                    )?,
                    state_version_ref: Some(state_version.state_version_ref.state_version_id),
                    evidence_ref: None,
                    trace_ref: None,
                    tenant_id: None,
                },
            )
            .await?;

        Ok(AppliedStatusChange {
            event,
            state_version_ref: state_version.state_version_ref.state_version_id,
        })
    }

    async fn apply_task_mutation(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &TaskRow,
        actor: PrincipalAttribution,
        mutation: TaskMutation<'_>,
    ) -> Result<AppliedStatusChange> {
        let TaskMutation {
            event_type,
            details,
            goal,
            owner_principal_id,
            budget_context_ref,
            correlation_id,
            causation_event_id,
        } = mutation;
        let task_id = row.task_id()?;
        let status = row.status()?;
        let now = self.clock.now();
        let next_goal = goal.unwrap_or_else(|| row.goal.clone());
        let next_owner_principal_id = owner_principal_id.unwrap_or(row.owner_principal_id()?);
        let next_budget_context_ref =
            budget_context_ref.unwrap_or_else(|| row.budget_context_ref.clone());
        let state_details = details.clone();

        let state_version = self
            .state_store
            .append_version(
                tx,
                NewStateVersion {
                    state_version_id: self.id_generator.next_uuid(),
                    task_id,
                    status: status.clone(),
                    payload: json!({
                        "status": status.as_str(),
                        "applied_event_type": event_type,
                        "from_status": status.as_str(),
                        "details": state_details,
                    }),
                    created_at: now,
                    created_by: self.config.emitted_by.clone(),
                },
            )
            .await?;

        sqlx::query(
            r#"
            UPDATE tasks
            SET
                goal = ?,
                status = ?,
                owner_principal_id = ?,
                updated_at = ?,
                working_state_ref = ?,
                checkpoint_ref = ?,
                budget_context_ref = ?,
                result_ref = ?
            WHERE task_id = ?
            "#,
        )
        .bind(&next_goal)
        .bind(status.as_str())
        .bind(next_owner_principal_id.to_string())
        .bind(encode_timestamp(now)?)
        .bind(state_version.state_version_ref.state_version_id.to_string())
        .bind(row.checkpoint_ref.clone())
        .bind(&next_budget_context_ref)
        .bind(row.result_ref.clone())
        .bind(task_id.to_string())
        .execute(&mut **tx)
        .await?;

        let event = self
            .event_log
            .append(
                tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: event_type.to_owned(),
                    event_family: EventFamily::Task,
                    task_id,
                    occurred_at: now,
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "from_status": status.as_str(),
                        "to_status": status.as_str(),
                        "details": details,
                    }),
                    correlation_id,
                    causation_event_id,
                    principal: Some(actor),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(next_budget_context_ref),
                    checkpoint_ref: parse_optional_uuid(
                        "checkpoint_ref",
                        row.checkpoint_ref.clone(),
                    )?,
                    state_version_ref: Some(state_version.state_version_ref.state_version_id),
                    evidence_ref: None,
                    trace_ref: None,
                    tenant_id: None,
                },
            )
            .await?;

        Ok(AppliedStatusChange {
            event,
            state_version_ref: state_version.state_version_ref.state_version_id,
        })
    }

    async fn apply_control_signal_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &TaskRow,
        actor: PrincipalAttribution,
        signal_id: Uuid,
        signal_type: ControlSignalType,
        payload: Value,
        correlation_id: &str,
        received_event_id: Uuid,
        current_state_version_ref: Uuid,
        checkpoint_ref: Option<Uuid>,
        allow_defer: bool,
    ) -> Result<ControlSignalRecord> {
        let current_status = row.status()?;

        if allow_defer && control_signal_defer_requested(&payload) {
            return self
                .record_control_signal_deferred(
                    tx,
                    row,
                    actor,
                    signal_id,
                    signal_type,
                    payload,
                    correlation_id,
                    received_event_id,
                    current_status,
                    current_state_version_ref,
                    checkpoint_ref,
                    "unsafe_interruption_boundary",
                )
                .await;
        }

        match signal_type {
            ControlSignalType::Approve => {
                if current_status != TaskStatus::WaitingOnControl {
                    let reason_code = if current_status.is_terminal() {
                        "terminal_state_irreversible"
                    } else {
                        "illegal_transition"
                    };
                    return self
                        .record_control_signal_rejected(
                            tx,
                            row,
                            actor,
                            signal_id,
                            signal_type,
                            payload,
                            correlation_id,
                            received_event_id,
                            current_status,
                            current_state_version_ref,
                            checkpoint_ref,
                            reason_code,
                        )
                        .await;
                }

                let waiting_state = self
                    .state_store
                    .get_by_ref_in(&mut **tx, current_state_version_ref)
                    .await?;
                let (approval_gate_id, blocked_call_id) =
                    match validate_waiting_on_control_resolution(&waiting_state, &payload) {
                        Ok(linkage) => linkage,
                        Err(reason_code) => {
                            return self
                                .record_control_signal_rejected(
                                    tx,
                                    row,
                                    actor,
                                    signal_id,
                                    signal_type,
                                    payload,
                                    correlation_id,
                                    received_event_id,
                                    current_status,
                                    current_state_version_ref,
                                    checkpoint_ref,
                                    reason_code,
                                )
                                .await;
                        }
                    };

                let applied_change = self
                    .apply_status_change(
                        tx,
                        row,
                        actor.clone(),
                        StatusChange {
                            to_status: TaskStatus::Running,
                            event_type: "task.control.approved",
                            details: json!({
                                "reason_code": "approval_granted",
                                "control_signal_id": signal_id,
                                "requested_effect": "approve",
                                "approval_gate_id": approval_gate_id,
                                "blocked_call_id": blocked_call_id,
                                "details": payload.clone(),
                            }),
                            result_ref: None,
                            correlation_id: Some(correlation_id.to_owned()),
                            causation_event_id: Some(received_event_id),
                        },
                    )
                    .await?;

                self.record_control_signal_applied(
                    tx,
                    row.task_id()?,
                    actor,
                    signal_id,
                    signal_type,
                    payload,
                    correlation_id,
                    received_event_id,
                    current_status,
                    TaskStatus::Running,
                    applied_change,
                )
                .await
            }
            ControlSignalType::Deny => {
                if current_status != TaskStatus::WaitingOnControl {
                    let reason_code = if current_status.is_terminal() {
                        "terminal_state_irreversible"
                    } else {
                        "illegal_transition"
                    };
                    return self
                        .record_control_signal_rejected(
                            tx,
                            row,
                            actor,
                            signal_id,
                            signal_type,
                            payload,
                            correlation_id,
                            received_event_id,
                            current_status,
                            current_state_version_ref,
                            checkpoint_ref,
                            reason_code,
                        )
                        .await;
                }

                let waiting_state = self
                    .state_store
                    .get_by_ref_in(&mut **tx, current_state_version_ref)
                    .await?;
                let (approval_gate_id, blocked_call_id) =
                    match validate_waiting_on_control_resolution(&waiting_state, &payload) {
                        Ok(linkage) => linkage,
                        Err(reason_code) => {
                            return self
                                .record_control_signal_rejected(
                                    tx,
                                    row,
                                    actor,
                                    signal_id,
                                    signal_type,
                                    payload,
                                    correlation_id,
                                    received_event_id,
                                    current_status,
                                    current_state_version_ref,
                                    checkpoint_ref,
                                    reason_code,
                                )
                                .await;
                        }
                    };

                let applied_change = self
                    .apply_status_change(
                        tx,
                        row,
                        actor.clone(),
                        StatusChange {
                            to_status: TaskStatus::Failed,
                            event_type: "task.failed",
                            details: json!({
                                "reason_code": "approval_denied",
                                "control_signal_id": signal_id,
                                "requested_effect": "deny",
                                "approval_gate_id": approval_gate_id,
                                "blocked_call_id": blocked_call_id,
                                "details": payload.clone(),
                            }),
                            result_ref: Some(format!(
                                "result://task/{}/approval-denied",
                                row.task_id()?
                            )),
                            correlation_id: Some(correlation_id.to_owned()),
                            causation_event_id: Some(received_event_id),
                        },
                    )
                    .await?;

                self.record_control_signal_applied(
                    tx,
                    row.task_id()?,
                    actor,
                    signal_id,
                    signal_type,
                    payload,
                    correlation_id,
                    received_event_id,
                    current_status,
                    TaskStatus::Failed,
                    applied_change,
                )
                .await
            }
            ControlSignalType::Pause if current_status == TaskStatus::Running => {
                let applied_change = self
                    .apply_status_change(
                        tx,
                        row,
                        actor.clone(),
                        StatusChange {
                            to_status: TaskStatus::Paused,
                            event_type: "task.paused",
                            details: json!({
                                "reason_code": "paused_by_control_signal",
                                "control_signal_id": signal_id,
                                "requested_effect": "pause",
                                "details": payload.clone(),
                            }),
                            result_ref: None,
                            correlation_id: Some(correlation_id.to_owned()),
                            causation_event_id: Some(received_event_id),
                        },
                    )
                    .await?;

                self.record_control_signal_applied(
                    tx,
                    row.task_id()?,
                    actor,
                    signal_id,
                    signal_type,
                    payload,
                    correlation_id,
                    received_event_id,
                    current_status,
                    TaskStatus::Paused,
                    applied_change,
                )
                .await
            }
            ControlSignalType::Resume if current_status == TaskStatus::Paused => {
                let applied_change = self
                    .apply_status_change(
                        tx,
                        row,
                        actor.clone(),
                        StatusChange {
                            to_status: TaskStatus::Running,
                            event_type: "task.resumed",
                            details: json!({
                                "reason_code": "resumed_by_control_signal",
                                "control_signal_id": signal_id,
                                "requested_effect": "resume",
                                "details": payload.clone(),
                            }),
                            result_ref: None,
                            correlation_id: Some(correlation_id.to_owned()),
                            causation_event_id: Some(received_event_id),
                        },
                    )
                    .await?;

                self.record_control_signal_applied(
                    tx,
                    row.task_id()?,
                    actor,
                    signal_id,
                    signal_type,
                    payload,
                    correlation_id,
                    received_event_id,
                    current_status,
                    TaskStatus::Running,
                    applied_change,
                )
                .await
            }
            ControlSignalType::ModifyBudget => {
                let Some(next_budget_context_ref) = payload
                    .get("budget_context_ref")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                else {
                    return self
                        .record_control_signal_rejected(
                            tx,
                            row,
                            actor,
                            signal_id,
                            signal_type,
                            payload,
                            correlation_id,
                            received_event_id,
                            current_status,
                            current_state_version_ref,
                            checkpoint_ref,
                            "invalid_control_payload",
                        )
                        .await;
                };

                if current_status.is_terminal() {
                    return self
                        .record_control_signal_rejected(
                            tx,
                            row,
                            actor,
                            signal_id,
                            signal_type,
                            payload,
                            correlation_id,
                            received_event_id,
                            current_status,
                            current_state_version_ref,
                            checkpoint_ref,
                            "terminal_state_irreversible",
                        )
                        .await;
                }

                let applied_change = self
                    .apply_task_mutation(
                        tx,
                        row,
                        actor.clone(),
                        TaskMutation {
                            event_type: "task.budget_modified",
                            details: json!({
                                "reason_code": "budget_modified_by_control_signal",
                                "control_signal_id": signal_id,
                                "requested_effect": "modify_budget",
                                "budget_context_ref": next_budget_context_ref,
                                "details": payload.clone(),
                            }),
                            goal: None,
                            owner_principal_id: None,
                            budget_context_ref: Some(next_budget_context_ref),
                            correlation_id: Some(correlation_id.to_owned()),
                            causation_event_id: Some(received_event_id),
                        },
                    )
                    .await?;

                self.record_control_signal_applied(
                    tx,
                    row.task_id()?,
                    actor,
                    signal_id,
                    signal_type,
                    payload,
                    correlation_id,
                    received_event_id,
                    current_status.clone(),
                    current_status,
                    applied_change,
                )
                .await
            }
            ControlSignalType::ModifyScope => {
                if current_status.is_terminal() {
                    return self
                        .record_control_signal_rejected(
                            tx,
                            row,
                            actor,
                            signal_id,
                            signal_type,
                            payload,
                            correlation_id,
                            received_event_id,
                            current_status,
                            current_state_version_ref,
                            checkpoint_ref,
                            "terminal_state_irreversible",
                        )
                        .await;
                }

                let next_goal = payload
                    .get("goal")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let applied_change = self
                    .apply_task_mutation(
                        tx,
                        row,
                        actor.clone(),
                        TaskMutation {
                            event_type: "task.scope_modified",
                            details: json!({
                                "reason_code": "scope_modified_by_control_signal",
                                "control_signal_id": signal_id,
                                "requested_effect": "modify_scope",
                                "details": payload.clone(),
                            }),
                            goal: next_goal,
                            owner_principal_id: None,
                            budget_context_ref: None,
                            correlation_id: Some(correlation_id.to_owned()),
                            causation_event_id: Some(received_event_id),
                        },
                    )
                    .await?;

                self.record_control_signal_applied(
                    tx,
                    row.task_id()?,
                    actor,
                    signal_id,
                    signal_type,
                    payload,
                    correlation_id,
                    received_event_id,
                    current_status.clone(),
                    current_status,
                    applied_change,
                )
                .await
            }
            ControlSignalType::Steer => {
                if current_status.is_terminal() {
                    return self
                        .record_control_signal_rejected(
                            tx,
                            row,
                            actor,
                            signal_id,
                            signal_type,
                            payload,
                            correlation_id,
                            received_event_id,
                            current_status,
                            current_state_version_ref,
                            checkpoint_ref,
                            "terminal_state_irreversible",
                        )
                        .await;
                }

                let applied_change = self
                    .apply_task_mutation(
                        tx,
                        row,
                        actor.clone(),
                        TaskMutation {
                            event_type: "task.steered",
                            details: json!({
                                "reason_code": "steered_by_control_signal",
                                "control_signal_id": signal_id,
                                "requested_effect": "steer",
                                "details": payload.clone(),
                            }),
                            goal: payload
                                .get("goal")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                            owner_principal_id: None,
                            budget_context_ref: None,
                            correlation_id: Some(correlation_id.to_owned()),
                            causation_event_id: Some(received_event_id),
                        },
                    )
                    .await?;

                self.record_control_signal_applied(
                    tx,
                    row.task_id()?,
                    actor,
                    signal_id,
                    signal_type,
                    payload,
                    correlation_id,
                    received_event_id,
                    current_status.clone(),
                    current_status,
                    applied_change,
                )
                .await
            }
            ControlSignalType::TakeOver => {
                if current_status.is_terminal() {
                    return self
                        .record_control_signal_rejected(
                            tx,
                            row,
                            actor,
                            signal_id,
                            signal_type,
                            payload,
                            correlation_id,
                            received_event_id,
                            current_status,
                            current_state_version_ref,
                            checkpoint_ref,
                            "terminal_state_irreversible",
                        )
                        .await;
                }

                let applied_change = self
                    .apply_task_mutation(
                        tx,
                        row,
                        actor.clone(),
                        TaskMutation {
                            event_type: "task.taken_over",
                            details: json!({
                                "reason_code": "taken_over_by_control_signal",
                                "control_signal_id": signal_id,
                                "requested_effect": "take_over",
                                "next_owner_principal_id": actor.principal_id,
                                "details": payload.clone(),
                            }),
                            goal: None,
                            owner_principal_id: Some(actor.principal_id),
                            budget_context_ref: None,
                            correlation_id: Some(correlation_id.to_owned()),
                            causation_event_id: Some(received_event_id),
                        },
                    )
                    .await?;

                self.record_control_signal_applied(
                    tx,
                    row.task_id()?,
                    actor,
                    signal_id,
                    signal_type,
                    payload,
                    correlation_id,
                    received_event_id,
                    current_status.clone(),
                    current_status,
                    applied_change,
                )
                .await
            }
            ControlSignalType::Reassign => {
                if current_status.is_terminal() {
                    return self
                        .record_control_signal_rejected(
                            tx,
                            row,
                            actor,
                            signal_id,
                            signal_type,
                            payload,
                            correlation_id,
                            received_event_id,
                            current_status,
                            current_state_version_ref,
                            checkpoint_ref,
                            "terminal_state_irreversible",
                        )
                        .await;
                }

                let Some(next_owner_principal_id) = parse_control_target_principal_id(&payload)
                else {
                    return self
                        .record_control_signal_rejected(
                            tx,
                            row,
                            actor,
                            signal_id,
                            signal_type,
                            payload,
                            correlation_id,
                            received_event_id,
                            current_status,
                            current_state_version_ref,
                            checkpoint_ref,
                            "invalid_control_payload",
                        )
                        .await;
                };

                let target_status = self
                    .principal_status_in_tx(tx, next_owner_principal_id)
                    .await?;
                match target_status {
                    Some(PrincipalStatus::Active) => {}
                    Some(_) => {
                        return self
                            .record_control_signal_rejected(
                                tx,
                                row,
                                actor,
                                signal_id,
                                signal_type,
                                payload,
                                correlation_id,
                                received_event_id,
                                current_status,
                                current_state_version_ref,
                                checkpoint_ref,
                                "target_principal_inactive",
                            )
                            .await;
                    }
                    None => {
                        return self
                            .record_control_signal_rejected(
                                tx,
                                row,
                                actor,
                                signal_id,
                                signal_type,
                                payload,
                                correlation_id,
                                received_event_id,
                                current_status,
                                current_state_version_ref,
                                checkpoint_ref,
                                "target_principal_not_found",
                            )
                            .await;
                    }
                }

                let applied_change = self
                    .apply_task_mutation(
                        tx,
                        row,
                        actor.clone(),
                        TaskMutation {
                            event_type: "task.reassigned",
                            details: json!({
                                "reason_code": "reassigned_by_control_signal",
                                "control_signal_id": signal_id,
                                "requested_effect": "reassign",
                                "next_owner_principal_id": next_owner_principal_id,
                                "details": payload.clone(),
                            }),
                            goal: None,
                            owner_principal_id: Some(next_owner_principal_id),
                            budget_context_ref: None,
                            correlation_id: Some(correlation_id.to_owned()),
                            causation_event_id: Some(received_event_id),
                        },
                    )
                    .await?;

                self.record_control_signal_applied(
                    tx,
                    row.task_id()?,
                    actor,
                    signal_id,
                    signal_type,
                    payload,
                    correlation_id,
                    received_event_id,
                    current_status.clone(),
                    current_status,
                    applied_change,
                )
                .await
            }
            ControlSignalType::Terminate
                if matches!(current_status, TaskStatus::Running | TaskStatus::Paused) =>
            {
                let applied_change = self
                    .apply_status_change(
                        tx,
                        row,
                        actor.clone(),
                        StatusChange {
                            to_status: TaskStatus::Cancelled,
                            event_type: "task.cancelled",
                            details: json!({
                                "reason_code": "terminated_by_control_signal",
                                "control_signal_id": signal_id,
                                "requested_effect": "terminate",
                                "details": payload.clone(),
                            }),
                            result_ref: row.result_ref.clone(),
                            correlation_id: Some(correlation_id.to_owned()),
                            causation_event_id: Some(received_event_id),
                        },
                    )
                    .await?;

                self.record_control_signal_applied(
                    tx,
                    row.task_id()?,
                    actor,
                    signal_id,
                    signal_type,
                    payload,
                    correlation_id,
                    received_event_id,
                    current_status,
                    TaskStatus::Cancelled,
                    applied_change,
                )
                .await
            }
            signal_type => {
                self.record_control_signal_rejected(
                    tx,
                    row,
                    actor,
                    signal_id,
                    signal_type,
                    payload,
                    correlation_id,
                    received_event_id,
                    current_status.clone(),
                    current_state_version_ref,
                    checkpoint_ref,
                    if current_status.is_terminal() {
                        "terminal_state_irreversible"
                    } else {
                        "illegal_transition"
                    },
                )
                .await
            }
        }
    }

    async fn record_control_signal_applied(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: Uuid,
        actor: PrincipalAttribution,
        signal_id: Uuid,
        signal_type: ControlSignalType,
        payload: Value,
        correlation_id: &str,
        received_event_id: Uuid,
        current_status: TaskStatus,
        status_after: TaskStatus,
        applied_change: AppliedStatusChange,
    ) -> Result<ControlSignalRecord> {
        let updated_row = fetch_task_row(&mut **tx, task_id).await?;
        let checkpoint_ref =
            parse_optional_uuid("checkpoint_ref", updated_row.checkpoint_ref.clone())?;
        let (approval_gate_id, blocked_call_id) = control_signal_linkage(&payload);
        let outcome = self
            .event_log
            .append(
                tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "control.signal.applied".to_owned(),
                    event_family: EventFamily::Control,
                    task_id,
                    occurred_at: self.clock.now(),
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "signal_id": signal_id,
                        "action": signal_type.as_str(),
                        "issuer_principal_id": actor.principal_id,
                        "requested_effect": signal_type.as_str(),
                        "status_before": current_status.as_str(),
                        "status_after": status_after.as_str(),
                        "approval_gate_id": approval_gate_id,
                        "blocked_call_id": blocked_call_id,
                        "applied_event_type": applied_change.event.event_type,
                    }),
                    correlation_id: Some(correlation_id.to_owned()),
                    causation_event_id: Some(received_event_id),
                    principal: Some(actor),
                    policy_context_ref: Some(updated_row.policy_context_ref.clone()),
                    budget_context_ref: Some(updated_row.budget_context_ref.clone()),
                    checkpoint_ref,
                    state_version_ref: Some(applied_change.state_version_ref),
                    evidence_ref: None,
                    trace_ref: None,
                    tenant_id: None,
                },
            )
            .await?;

        self.control_signal_store
            .update_status(
                tx,
                signal_id,
                ControlSignalStatus::Applied,
                Some(outcome.recorded_at),
                outcome.event_id,
                outcome.sequence_number,
            )
            .await
    }

    async fn record_control_signal_deferred(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &TaskRow,
        actor: PrincipalAttribution,
        signal_id: Uuid,
        signal_type: ControlSignalType,
        payload: Value,
        correlation_id: &str,
        received_event_id: Uuid,
        current_status: TaskStatus,
        current_state_version_ref: Uuid,
        checkpoint_ref: Option<Uuid>,
        reason_code: &'static str,
    ) -> Result<ControlSignalRecord> {
        let (approval_gate_id, blocked_call_id) = control_signal_linkage(&payload);
        let outcome = self
            .event_log
            .append(
                tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "control.signal.deferred".to_owned(),
                    event_family: EventFamily::Control,
                    task_id: row.task_id()?,
                    occurred_at: self.clock.now(),
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "signal_id": signal_id,
                        "action": signal_type.as_str(),
                        "issuer_principal_id": actor.principal_id,
                        "requested_effect": signal_type.as_str(),
                        "status_before": current_status.as_str(),
                        "approval_gate_id": approval_gate_id,
                        "blocked_call_id": blocked_call_id,
                        "reason_code": reason_code,
                        "details": payload,
                    }),
                    correlation_id: Some(correlation_id.to_owned()),
                    causation_event_id: Some(received_event_id),
                    principal: Some(actor),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref,
                    state_version_ref: Some(current_state_version_ref),
                    evidence_ref: None,
                    trace_ref: None,
                    tenant_id: None,
                },
            )
            .await?;

        self.control_signal_store
            .update_status(
                tx,
                signal_id,
                ControlSignalStatus::Deferred,
                None,
                outcome.event_id,
                outcome.sequence_number,
            )
            .await
    }

    async fn record_control_signal_rejected(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &TaskRow,
        actor: PrincipalAttribution,
        signal_id: Uuid,
        signal_type: ControlSignalType,
        payload: Value,
        correlation_id: &str,
        received_event_id: Uuid,
        current_status: TaskStatus,
        current_state_version_ref: Uuid,
        checkpoint_ref: Option<Uuid>,
        reason_code: &'static str,
    ) -> Result<ControlSignalRecord> {
        let (approval_gate_id, blocked_call_id) = control_signal_linkage(&payload);
        let outcome = self
            .event_log
            .append(
                tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "control.signal.rejected".to_owned(),
                    event_family: EventFamily::Control,
                    task_id: row.task_id()?,
                    occurred_at: self.clock.now(),
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "signal_id": signal_id,
                        "action": signal_type.as_str(),
                        "issuer_principal_id": actor.principal_id,
                        "requested_effect": signal_type.as_str(),
                        "status_before": current_status.as_str(),
                        "approval_gate_id": approval_gate_id,
                        "blocked_call_id": blocked_call_id,
                        "reason_code": reason_code,
                        "details": payload,
                    }),
                    correlation_id: Some(correlation_id.to_owned()),
                    causation_event_id: Some(received_event_id),
                    principal: Some(actor),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref,
                    state_version_ref: Some(current_state_version_ref),
                    evidence_ref: None,
                    trace_ref: None,
                    tenant_id: None,
                },
            )
            .await?;

        self.control_signal_store
            .update_status(
                tx,
                signal_id,
                ControlSignalStatus::Rejected,
                None,
                outcome.event_id,
                outcome.sequence_number,
            )
            .await
    }

    async fn record_transition_rejection(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &TaskRow,
        attempted_status: TaskStatus,
        actor: PrincipalAttribution,
    ) -> Result<()> {
        let task_id = row.task_id()?;
        let from_status = row.status()?;

        self.event_log
            .append(
                tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "task.transition.rejected".to_owned(),
                    event_family: EventFamily::Failure,
                    task_id,
                    occurred_at: self.clock.now(),
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "from_status": from_status.as_str(),
                        "to_status": attempted_status.as_str(),
                        "reason_code": if from_status.is_terminal() {
                            "terminal_state_irreversible"
                        } else {
                            "illegal_transition"
                        },
                    }),
                    correlation_id: None,
                    causation_event_id: None,
                    principal: Some(actor),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref: parse_optional_uuid(
                        "checkpoint_ref",
                        row.checkpoint_ref.clone(),
                    )?,
                    state_version_ref: Some(parse_uuid(
                        "working_state_ref",
                        &row.working_state_ref,
                    )?),
                    evidence_ref: None,
                    trace_ref: None,
                    tenant_id: None,
                },
            )
            .await?;

        Ok(())
    }

    async fn record_restore_failure(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &TaskRow,
        actor: PrincipalAttribution,
        failure: RestoreFailure<'_>,
        error: &RuntimeError,
    ) -> Result<()> {
        let RestoreFailure {
            requested_checkpoint_id,
            correlation_id,
            causation_event_id,
            restore_source,
            reason_code,
        } = failure;
        let task_id = row.task_id()?;
        let current_status = row.status()?;
        let current_state_version_ref = parse_uuid("working_state_ref", &row.working_state_ref)?;

        self.event_log
            .append(
                tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "checkpoint.restore.failed".to_owned(),
                    event_family: EventFamily::Checkpoint,
                    task_id,
                    occurred_at: self.clock.now(),
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "from_status": current_status.as_str(),
                        "restore_source": restore_source,
                        "requested_checkpoint_id": requested_checkpoint_id.map(|value| value.to_string()),
                        "reason_code": reason_code,
                        "error": error.to_string(),
                    }),
                    correlation_id: Some(correlation_id.to_owned()),
                    causation_event_id: Some(causation_event_id),
                    principal: Some(actor),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref: requested_checkpoint_id,
                    state_version_ref: Some(current_state_version_ref),
                    evidence_ref: None,
                    trace_ref: None,
                    tenant_id: None,
                },
            )
            .await?;

        Ok(())
    }

    async fn ensure_task_actor_is_owner(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &TaskRow,
        actor: &PrincipalAttribution,
    ) -> Result<()> {
        self.ensure_active_principal_in_tx(tx, actor.principal_id)
            .await?;

        let task_id = row.task_id()?;
        let owner_principal_id = row.owner_principal_id()?;
        if actor.principal_id != owner_principal_id {
            return Err(RuntimeError::UnauthorizedTaskAction {
                task_id,
                principal_id: actor.principal_id,
            });
        }

        Ok(())
    }

    async fn ensure_active_principal_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        principal_id: Uuid,
    ) -> Result<()> {
        let Some(status) = self.principal_status_in_tx(tx, principal_id).await? else {
            return Err(RuntimeError::PrincipalNotFound { principal_id });
        };

        if status != PrincipalStatus::Active {
            return Err(RuntimeError::InvariantViolation(format!(
                "principal {} is not active",
                principal_id
            )));
        }

        Ok(())
    }

    async fn principal_status_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        principal_id: Uuid,
    ) -> Result<Option<PrincipalStatus>> {
        let status = sqlx::query_scalar::<_, String>(
            r#"
            SELECT status
            FROM principals
            WHERE principal_id = ?
            "#,
        )
        .bind(principal_id.to_string())
        .fetch_optional(&mut **tx)
        .await?;

        status.map(|value| value.parse()).transpose()
    }
}

#[derive(Debug, FromRow)]
struct TaskRow {
    task_id: String,
    goal: String,
    status: String,
    owner_principal_id: String,
    created_at: String,
    updated_at: String,
    policy_context_ref: String,
    working_state_ref: String,
    state_task_id: String,
    state_version_number: i64,
    history_ref: String,
    checkpoint_ref: Option<String>,
    budget_context_ref: String,
    priority: i64,
    result_ref: Option<String>,
}

impl TaskRow {
    fn task_id(&self) -> Result<Uuid> {
        parse_uuid("task_id", &self.task_id)
    }

    fn owner_principal_id(&self) -> Result<Uuid> {
        parse_uuid("owner_principal_id", &self.owner_principal_id)
    }

    fn status(&self) -> Result<TaskStatus> {
        self.status.parse()
    }

    fn into_task(self) -> Result<Task> {
        let Self {
            task_id,
            goal,
            status,
            owner_principal_id,
            created_at,
            updated_at,
            policy_context_ref,
            working_state_ref,
            state_task_id,
            state_version_number,
            history_ref,
            checkpoint_ref,
            budget_context_ref,
            priority,
            result_ref,
        } = self;

        let task_id = parse_uuid("task_id", &task_id)?;
        let owner_principal_id = parse_uuid("owner_principal_id", &owner_principal_id)?;
        let state_task_id = parse_uuid("state_task_id", &state_task_id)?;
        let priority = i32::try_from(priority).map_err(|_| {
            RuntimeError::InvariantViolation(format!("priority out of range for task {task_id}"))
        })?;

        Ok(Task {
            task_id,
            goal,
            status: status.parse()?,
            owner_principal_id,
            created_at: decode_timestamp(&created_at)?,
            updated_at: decode_timestamp(&updated_at)?,
            policy_context_ref,
            working_state_ref: StateVersionRef {
                state_version_id: parse_uuid("working_state_ref", &working_state_ref)?,
                task_id: state_task_id,
                version_number: state_version_number,
            },
            history_ref,
            checkpoint_ref: parse_optional_uuid("checkpoint_ref", checkpoint_ref)?,
            budget_context_ref,
            priority,
            result_ref,
        })
    }
}

async fn fetch_task_row<'e, E>(executor: E, task_id: Uuid) -> Result<TaskRow>
where
    E: Executor<'e, Database = Sqlite>,
{
    let row = sqlx::query_as::<_, TaskRow>(
        r#"
        SELECT
            t.task_id,
            t.goal,
            t.status,
            t.owner_principal_id,
            t.created_at,
            t.updated_at,
            t.policy_context_ref,
            t.working_state_ref,
            s.task_id AS state_task_id,
            s.version_number AS state_version_number,
            t.history_ref,
            t.checkpoint_ref,
            t.budget_context_ref,
            t.priority,
            t.result_ref
        FROM tasks t
        INNER JOIN task_state_versions s
            ON s.state_version_id = t.working_state_ref
           AND s.task_id = t.task_id
        WHERE t.task_id = ?
        "#,
    )
    .bind(task_id.to_string())
    .fetch_optional(executor)
    .await?;

    match row {
        Some(row) => Ok(row),
        None => Err(RuntimeError::TaskNotFound { task_id }),
    }
}

async fn fetch_latest_event_sequence<'e, E>(executor: E, task_id: Uuid) -> Result<i64>
where
    E: Executor<'e, Database = Sqlite>,
{
    let latest = sqlx::query_scalar(
        r#"
        SELECT COALESCE(MAX(sequence_number), 0)
        FROM events
        WHERE task_id = ?
        "#,
    )
    .bind(task_id.to_string())
    .fetch_one(executor)
    .await?;

    Ok(latest)
}

fn control_signal_linkage(payload: &Value) -> (Option<String>, Option<String>) {
    let approval_gate_id = payload
        .get("approval_gate_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let blocked_call_id = payload
        .get("blocked_call_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    (approval_gate_id, blocked_call_id)
}

fn control_signal_defer_requested(payload: &Value) -> bool {
    payload
        .get("unsafe_boundary")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn validate_waiting_on_control_resolution(
    waiting_state: &TaskStateVersion,
    payload: &Value,
) -> std::result::Result<(Option<String>, Option<String>), &'static str> {
    let Some(details) = waiting_state.payload.get("details") else {
        return Err("waiting_on_control_context_missing");
    };

    let required_gate_id = details.get("approval_gate_id").and_then(Value::as_str);
    let required_blocked_call_id = details.get("blocked_call_id").and_then(Value::as_str);
    let (requested_gate_id, requested_blocked_call_id) = control_signal_linkage(payload);

    if let Some(required_gate_id) = required_gate_id {
        match requested_gate_id.as_deref() {
            Some(candidate) if candidate == required_gate_id => {}
            Some(_) => return Err("approval_gate_mismatch"),
            None => return Err("approval_gate_id_required"),
        }
    }

    if let Some(required_blocked_call_id) = required_blocked_call_id {
        if let Some(candidate) = requested_blocked_call_id.as_deref() {
            if candidate != required_blocked_call_id {
                return Err("blocked_call_mismatch");
            }
        }
    }

    Ok((
        required_gate_id.map(str::to_owned).or(requested_gate_id),
        required_blocked_call_id
            .map(str::to_owned)
            .or(requested_blocked_call_id),
    ))
}

fn parse_control_target_principal_id(payload: &Value) -> Option<Uuid> {
    let candidate = payload
        .get("owner_principal_id")
        .or_else(|| payload.get("assignee_principal_id"))
        .or_else(|| payload.get("principal_id"))
        .and_then(Value::as_str)?;
    Uuid::parse_str(candidate).ok()
}
