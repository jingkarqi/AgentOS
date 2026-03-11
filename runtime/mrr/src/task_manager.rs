use std::convert::TryFrom;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{Executor, FromRow, Sqlite, SqlitePool, Transaction};
use uuid::Uuid;

use crate::checkpoint_store::{CheckpointRecord, CheckpointStore, NewCheckpointRecord};
use crate::clock::{Clock, IdGenerator};
use crate::db::{decode_timestamp, encode_timestamp, parse_optional_uuid, parse_uuid};
use crate::error::{Result, RuntimeError};
use crate::event_log::{EventDraft, EventEnvelope, EventFamily, EventLog};
use crate::principal::{PrincipalAttribution, PrincipalStatus, PrincipalStore};
use crate::state_store::{NewStateVersion, StateStore, StateVersionRef};
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

struct StatusChange<'a> {
    to_status: TaskStatus,
    event_type: &'a str,
    details: Value,
    result_ref: Option<String>,
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

#[derive(Clone)]
pub struct TaskManager {
    pool: SqlitePool,
    principal_store: PrincipalStore,
    event_log: EventLog,
    state_store: StateStore,
    checkpoint_store: CheckpointStore,
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
                allowed_from: &[TaskStatus::Created, TaskStatus::Ready, TaskStatus::Running],
                change: StatusChange {
                    to_status: TaskStatus::Cancelled,
                    event_type: "task.cancelled",
                    details: json!({ "cancellation": cancellation_payload }),
                    result_ref: None,
                },
            },
        )
        .await
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
    ) -> Result<()> {
        let StatusChange {
            to_status,
            event_type,
            details,
            result_ref,
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

        self.event_log
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
                    correlation_id: None,
                    causation_event_id: None,
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

        Ok(())
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

        let Some(status) = status else {
            return Err(RuntimeError::PrincipalNotFound { principal_id });
        };

        if status.parse::<PrincipalStatus>()? != PrincipalStatus::Active {
            return Err(RuntimeError::InvariantViolation(format!(
                "principal {} is not active",
                principal_id
            )));
        }

        Ok(())
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
