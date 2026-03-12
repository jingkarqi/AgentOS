use std::convert::TryFrom;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{Executor, FromRow, Sqlite, SqlitePool, Transaction};
use uuid::Uuid;

use crate::capability::{
    validate_payload_against_schema, CapabilityCallRecord, CapabilityCallStore, CapabilityContract,
    CapabilityDispatcher, CapabilityExecutionResult, CapabilityOutcome, CapabilityRegistry,
    CostClass, IdempotencyClass, InvokeCapabilityCommand, RegisterCapabilityCommand, RetrySafety,
    SideEffectSummary,
};
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

enum CapabilityPolicyDecision {
    Allow,
    AllowWithConstraints { applied_constraints: Vec<Value> },
    RequireApproval { approval_gate_id: String },
    Deny { reason_code: String },
}

enum ApprovalSignalAuthorization {
    Authorized,
    Rejected { reason_code: &'static str },
}

impl CapabilityPolicyDecision {
    fn outcome(&self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::AllowWithConstraints { .. } => "allow_with_constraints",
            Self::RequireApproval { .. } => "require_approval",
            Self::Deny { .. } => "deny",
        }
    }

    fn result_event_type(&self) -> &'static str {
        match self {
            Self::Allow => "policy.result.allow",
            Self::AllowWithConstraints { .. } => "policy.result.allow_with_constraints",
            Self::RequireApproval { .. } => "policy.result.require_approval",
            Self::Deny { .. } => "policy.result.deny",
        }
    }

    fn approval_gate_id(&self) -> Option<&str> {
        match self {
            Self::RequireApproval { approval_gate_id } => Some(approval_gate_id),
            _ => None,
        }
    }

    fn deny_reason_code(&self) -> Option<&str> {
        match self {
            Self::Deny { reason_code } => Some(reason_code),
            _ => None,
        }
    }

    fn applied_constraints(&self) -> &[Value] {
        match self {
            Self::AllowWithConstraints {
                applied_constraints,
            } => applied_constraints,
            _ => &[],
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

struct AppliedControlSignal {
    signal: ControlSignalRecord,
    resume_blocked_call_id: Option<String>,
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
    capability_registry: CapabilityRegistry,
    capability_call_store: CapabilityCallStore,
    capability_dispatcher: CapabilityDispatcher,
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
        Self::new_with_dispatcher(
            pool,
            clock,
            id_generator,
            config,
            CapabilityDispatcher::default(),
        )
    }

    pub fn new_with_dispatcher(
        pool: SqlitePool,
        clock: Arc<dyn Clock>,
        id_generator: Arc<dyn IdGenerator>,
        config: TaskManagerConfig,
        capability_dispatcher: CapabilityDispatcher,
    ) -> Self {
        Self {
            principal_store: PrincipalStore::new(pool.clone()),
            event_log: EventLog::new(config.outbox_publication_topic.clone()),
            state_store: StateStore,
            checkpoint_store: CheckpointStore,
            control_signal_store: ControlSignalStore,
            capability_registry: CapabilityRegistry::new(pool.clone()),
            capability_call_store: CapabilityCallStore::new(pool.clone()),
            capability_dispatcher,
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

    pub fn capability_registry(&self) -> CapabilityRegistry {
        self.capability_registry.clone()
    }

    pub fn capability_dispatcher(&self) -> CapabilityDispatcher {
        self.capability_dispatcher.clone()
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

    pub async fn register_capability(
        &self,
        command: RegisterCapabilityCommand,
    ) -> Result<CapabilityContract> {
        self.capability_registry
            .register(command.into_contract(self.clock.now()))
            .await
    }

    pub async fn list_capabilities(&self) -> Result<Vec<CapabilityContract>> {
        self.capability_registry.list().await
    }

    pub async fn list_capability_calls_by_task(
        &self,
        task_id: Uuid,
    ) -> Result<Vec<CapabilityCallRecord>> {
        fetch_task_row(&self.pool, task_id).await?;
        self.capability_call_store.list_by_task(task_id).await
    }

    pub async fn invoke_capability(
        &self,
        task_id: Uuid,
        actor: PrincipalAttribution,
        command: InvokeCapabilityCommand,
    ) -> Result<CapabilityCallRecord> {
        let InvokeCapabilityCommand {
            call_id,
            capability_id,
            capability_version,
            input_payload,
            idempotency_expectation,
            correlation_id,
            checkpoint_ref,
            state_version_ref,
            timeout_override_ms,
            reason_code,
            trace_ref,
        } = command;
        let mut tx = self.pool.begin().await?;
        let row = fetch_task_row(&mut *tx, task_id).await?;

        if row.status()? != TaskStatus::Running {
            self.record_transition_rejection(&mut tx, &row, TaskStatus::WaitingOnCapability, actor)
                .await?;
            tx.commit().await?;
            return Err(RuntimeError::InvalidTransition {
                task_id,
                from_status: row.status()?.as_str().to_owned(),
                to_status: TaskStatus::WaitingOnCapability.as_str().to_owned(),
            });
        }

        let resolved_contract = self
            .capability_registry
            .resolve_for_invocation_in_tx(&mut tx, &capability_id, capability_version.as_deref())
            .await?;
        let resolved_capability_version = resolved_contract
            .as_ref()
            .map(|contract| contract.capability_version.clone())
            .or(capability_version.clone());
        let resolved_effect_class = resolved_contract
            .as_ref()
            .map(|contract| contract.effect_class.as_str().to_owned())
            .unwrap_or_else(|| "unknown".to_owned());
        let resolved_idempotency_expectation = idempotency_expectation
            .clone()
            .or_else(|| {
                resolved_contract
                    .as_ref()
                    .map(|contract| contract.idempotency_class.clone())
            })
            .unwrap_or(IdempotencyClass::Unknown);
        let resolved_cost_class = resolved_contract
            .as_ref()
            .map(|contract| contract.cost_class.clone());
        let current_state_version_ref = parse_uuid("working_state_ref", &row.working_state_ref)?;
        let current_checkpoint_ref = checkpoint_ref.or(parse_optional_uuid(
            "checkpoint_ref",
            row.checkpoint_ref.clone(),
        )?);
        let state_version_ref = state_version_ref.or(Some(current_state_version_ref));
        let call_id = call_id.unwrap_or_else(|| format!("call:{}", self.id_generator.next_uuid()));
        let correlation_id = correlation_id.unwrap_or_else(|| call_id.clone());
        let now = self.clock.now();

        let requested_event = self
            .event_log
            .append(
                &mut tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "capability.requested".to_owned(),
                    event_family: EventFamily::Capability,
                    task_id,
                    occurred_at: now,
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "call_id": call_id.clone(),
                        "capability_id": capability_id.clone(),
                        "capability_version": resolved_capability_version.clone(),
                        "input_payload": input_payload.clone(),
                        "reason_code": reason_code.clone(),
                    }),
                    correlation_id: Some(correlation_id.clone()),
                    causation_event_id: None,
                    principal: Some(actor.clone()),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref: current_checkpoint_ref,
                    state_version_ref,
                    evidence_ref: None,
                    trace_ref: trace_ref.clone(),
                    tenant_id: None,
                },
            )
            .await?;

        let call = self
            .capability_call_store
            .record_requested(
                &mut tx,
                crate::capability::NewCapabilityCallRecord {
                    call_id: call_id.clone(),
                    task_id,
                    caller: actor.clone(),
                    capability_id: capability_id.clone(),
                    capability_version: resolved_capability_version.clone(),
                    input_payload: input_payload.clone(),
                    requested_at: now,
                    policy_context_ref: row.policy_context_ref.clone(),
                    budget_context_ref: row.budget_context_ref.clone(),
                    effect_class: resolved_effect_class.clone(),
                    cost_class: resolved_cost_class,
                    idempotency_expectation: resolved_idempotency_expectation.clone(),
                    correlation_id: correlation_id.clone(),
                    checkpoint_ref: current_checkpoint_ref,
                    state_version_ref,
                    timeout_override_ms,
                    request_reason_code: reason_code.clone(),
                    requested_event_id: requested_event.event_id,
                    requested_sequence_number: requested_event.sequence_number,
                    created_at: now,
                },
            )
            .await?;

        let Some(contract) = resolved_contract else {
            let denied = self
                .deny_capability_call_in_tx(
                    &mut tx,
                    &row,
                    actor,
                    &call,
                    None,
                    "unknown",
                    "unregistered_capability",
                    "kernel",
                    requested_event.event_id,
                    trace_ref,
                    &[],
                )
                .await?;
            tx.commit().await?;
            return Ok(denied);
        };

        let policy_requested = self
            .event_log
            .append(
                &mut tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "policy.evaluation.requested".to_owned(),
                    event_family: EventFamily::Policy,
                    task_id,
                    occurred_at: self.clock.now(),
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "call_id": call.call_id.clone(),
                        "capability_id": contract.capability_id.clone(),
                        "capability_version": contract.capability_version.clone(),
                        "effect_class": contract.effect_class.as_str(),
                    }),
                    correlation_id: Some(call.correlation_id.clone()),
                    causation_event_id: Some(requested_event.event_id),
                    principal: Some(actor.clone()),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref: call.checkpoint_ref,
                    state_version_ref: call.state_version_ref,
                    evidence_ref: None,
                    trace_ref: trace_ref.clone(),
                    tenant_id: None,
                },
            )
            .await?;

        let policy_decision = self
            .evaluate_capability_policy_in_tx(&mut tx, &row, &actor, &call, &contract, false)
            .await?;
        let policy_completed = self
            .event_log
            .append(
                &mut tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "policy.evaluation.completed".to_owned(),
                    event_family: EventFamily::Policy,
                    task_id,
                    occurred_at: self.clock.now(),
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "call_id": call.call_id.clone(),
                        "capability_id": contract.capability_id.clone(),
                        "capability_version": contract.capability_version.clone(),
                        "outcome": policy_decision.outcome(),
                        "applied_constraints": policy_decision.applied_constraints(),
                    }),
                    correlation_id: Some(call.correlation_id.clone()),
                    causation_event_id: Some(policy_requested.event_id),
                    principal: Some(actor.clone()),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref: call.checkpoint_ref,
                    state_version_ref: call.state_version_ref,
                    evidence_ref: None,
                    trace_ref: trace_ref.clone(),
                    tenant_id: None,
                },
            )
            .await?;

        let policy_result_event = self
            .event_log
            .append(
                &mut tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: policy_decision.result_event_type().to_owned(),
                    event_family: EventFamily::Policy,
                    task_id,
                    occurred_at: self.clock.now(),
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "call_id": call.call_id.clone(),
                        "blocked_call_id": match &policy_decision {
                            CapabilityPolicyDecision::RequireApproval { .. }
                            | CapabilityPolicyDecision::Deny { .. } => {
                                Value::String(call.call_id.clone())
                            }
                            CapabilityPolicyDecision::Allow
                            | CapabilityPolicyDecision::AllowWithConstraints { .. } => Value::Null,
                        },
                        "approval_gate_id": policy_decision.approval_gate_id(),
                        "capability_id": contract.capability_id.clone(),
                        "capability_version": contract.capability_version.clone(),
                        "outcome": policy_decision.outcome(),
                        "reason_code": match &policy_decision {
                            CapabilityPolicyDecision::Allow
                            | CapabilityPolicyDecision::AllowWithConstraints { .. } => None,
                            CapabilityPolicyDecision::RequireApproval { .. } => {
                                Some("requires_higher_authority")
                            }
                            CapabilityPolicyDecision::Deny { reason_code } => {
                                Some(reason_code.as_str())
                            }
                        },
                        "applied_constraints": policy_decision.applied_constraints(),
                    }),
                    correlation_id: Some(call.correlation_id.clone()),
                    causation_event_id: Some(policy_completed.event_id),
                    principal: Some(actor.clone()),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref: call.checkpoint_ref,
                    state_version_ref: call.state_version_ref,
                    evidence_ref: None,
                    trace_ref: trace_ref.clone(),
                    tenant_id: None,
                },
            )
            .await?;
        let applied_constraints = policy_decision.applied_constraints().to_vec();

        if let CapabilityPolicyDecision::Deny { reason_code } = &policy_decision {
            let denied = self
                .deny_capability_call_in_tx(
                    &mut tx,
                    &row,
                    actor,
                    &call,
                    Some(&contract.capability_version),
                    contract.effect_class.as_str(),
                    reason_code,
                    "policy",
                    policy_result_event.event_id,
                    trace_ref,
                    &[],
                )
                .await?;
            tx.commit().await?;
            return Ok(denied);
        }

        if let Err(message) =
            validate_payload_against_schema(&contract.input_schema, &input_payload)
        {
            let failed = self
                .complete_capability_call_in_tx(
                    &mut tx,
                    &row,
                    actor,
                    &call,
                    Some(&contract.capability_version),
                    contract.effect_class.as_str(),
                    CapabilityExecutionResult::failed(
                        "validation_failed",
                        json!({
                            "error": message,
                            "validation_phase": "input",
                        }),
                    ),
                    policy_result_event.event_id,
                    trace_ref,
                    &applied_constraints,
                )
                .await?;
            tx.commit().await?;
            return Ok(failed);
        }

        if let Some(reason_code) =
            validate_applied_constraints(&contract, &input_payload, &applied_constraints)?
        {
            let denied = self
                .deny_capability_call_in_tx(
                    &mut tx,
                    &row,
                    actor,
                    &call,
                    Some(&contract.capability_version),
                    contract.effect_class.as_str(),
                    reason_code,
                    "policy",
                    policy_result_event.event_id,
                    trace_ref,
                    &applied_constraints,
                )
                .await?;
            tx.commit().await?;
            return Ok(denied);
        }

        if self
            .capability_dispatcher
            .handler_for(&contract.capability_id, &contract.capability_version)
            .is_none()
        {
            let failed = self
                .complete_capability_call_in_tx(
                    &mut tx,
                    &row,
                    actor.clone(),
                    &call,
                    Some(&contract.capability_version),
                    contract.effect_class.as_str(),
                    CapabilityExecutionResult::failed(
                        "handler_not_registered",
                        json!({
                            "error": "capability handler is not registered",
                        }),
                    ),
                    policy_result_event.event_id,
                    trace_ref,
                    &applied_constraints,
                )
                .await?;
            tx.commit().await?;
            return Ok(failed);
        }

        if let CapabilityPolicyDecision::RequireApproval { approval_gate_id } = &policy_decision {
            let blocked = self
                .block_capability_call_for_approval_in_tx(
                    &mut tx,
                    &row,
                    actor,
                    &call,
                    &contract,
                    approval_gate_id,
                    policy_result_event.event_id,
                    reason_code.as_deref(),
                    trace_ref.as_deref(),
                )
                .await?;
            tx.commit().await?;
            return Ok(blocked);
        }

        tx.commit().await?;
        self.dispatch_capability_call(
            task_id,
            actor,
            &call.call_id,
            contract,
            policy_result_event.event_id,
            reason_code,
            trace_ref,
            applied_constraints,
        )
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
        let mut resumes = Vec::new();

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
            if let Some(blocked_call_id) = updated.resume_blocked_call_id.clone() {
                let outcome_event_id = updated.signal.outcome_event_id.ok_or_else(|| {
                    RuntimeError::InvariantViolation(format!(
                        "applied control signal {} is missing outcome_event_id",
                        updated.signal.signal_id
                    ))
                })?;
                resumes.push((blocked_call_id, outcome_event_id));
            }
            applied.push(updated.signal);
        }

        tx.commit().await?;

        for (blocked_call_id, outcome_event_id) in resumes {
            self.resume_blocked_capability_call_after_approval(
                task_id,
                &blocked_call_id,
                outcome_event_id,
            )
            .await?;
        }

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

        if let Some(blocked_call_id) = applied.resume_blocked_call_id.as_deref() {
            let outcome_event_id = applied.signal.outcome_event_id.ok_or_else(|| {
                RuntimeError::InvariantViolation(format!(
                    "applied control signal {} is missing outcome_event_id",
                    applied.signal.signal_id
                ))
            })?;
            self.resume_blocked_capability_call_after_approval(
                task_id,
                blocked_call_id,
                outcome_event_id,
            )
            .await?;
        }

        Ok(applied.signal)
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
        let pending_call = self
            .capability_call_store
            .get_latest_pending_by_task_in_tx(&mut tx, task_id)
            .await?;
        let in_flight_capability = pending_call.as_ref().map(checkpoint_in_flight_capability);
        let checkpoint_payload = CheckpointPayload {
            checkpoint_version: CHECKPOINT_VERSION.to_owned(),
            task_status: status.as_str().to_owned(),
            policy_context_ref: row.policy_context_ref.clone(),
            budget_context_ref: row.budget_context_ref.clone(),
            history_ref: row.history_ref.clone(),
            result_ref: row.result_ref.clone(),
            event_boundary_sequence_number,
            in_flight_capability,
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

        if let Some(call) = pending_call {
            self.capability_call_store
                .update_checkpoint_ref(&mut tx, &call.call_id, checkpoint.checkpoint_id, now)
                .await?;
        }

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

        if let Err(error) = self
            .validate_restorable_in_flight_capability_in_tx(
                &mut tx,
                task_id,
                checkpoint_status.clone(),
                checkpoint_payload.in_flight_capability.as_ref(),
            )
            .await
        {
            self.record_restore_failure(
                &mut tx,
                &row,
                actor.clone(),
                RestoreFailure {
                    requested_checkpoint_id: Some(checkpoint.checkpoint_id),
                    correlation_id: &correlation_id,
                    causation_event_id: restore_started.event_id,
                    restore_source,
                    reason_code: "checkpoint_in_flight_capability_not_recoverable",
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
    ) -> Result<AppliedControlSignal> {
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
                .await
                .map(wrap_applied_control_signal);
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
                        .await
                        .map(wrap_applied_control_signal);
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
                                .await
                                .map(wrap_applied_control_signal);
                        }
                    };
                match self
                    .authorize_approval_resolution_in_tx(
                        tx,
                        row,
                        &actor,
                        blocked_call_id.as_deref(),
                    )
                    .await?
                {
                    ApprovalSignalAuthorization::Authorized => {}
                    ApprovalSignalAuthorization::Rejected { reason_code } => {
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
                            .await
                            .map(wrap_applied_control_signal);
                    }
                }

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
                                "approval_gate_id": approval_gate_id.clone(),
                                "blocked_call_id": blocked_call_id.clone(),
                                "details": payload.clone(),
                            }),
                            result_ref: None,
                            correlation_id: Some(correlation_id.to_owned()),
                            causation_event_id: Some(received_event_id),
                        },
                    )
                    .await?;
                let resolved_payload = resolved_control_signal_payload(
                    payload,
                    approval_gate_id.clone(),
                    blocked_call_id.clone(),
                );

                let applied_signal = self
                    .record_control_signal_applied(
                        tx,
                        row.task_id()?,
                        actor,
                        signal_id,
                        signal_type,
                        resolved_payload,
                        correlation_id,
                        received_event_id,
                        current_status,
                        TaskStatus::Running,
                        applied_change,
                    )
                    .await?;

                Ok(AppliedControlSignal {
                    signal: applied_signal,
                    resume_blocked_call_id: blocked_call_id,
                })
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
                        .await
                        .map(wrap_applied_control_signal);
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
                                .await
                                .map(wrap_applied_control_signal);
                        }
                    };
                match self
                    .authorize_approval_resolution_in_tx(
                        tx,
                        row,
                        &actor,
                        blocked_call_id.as_deref(),
                    )
                    .await?
                {
                    ApprovalSignalAuthorization::Authorized => {}
                    ApprovalSignalAuthorization::Rejected { reason_code } => {
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
                            .await
                            .map(wrap_applied_control_signal);
                    }
                }
                let blocked_call_id_for_deny = blocked_call_id.clone();
                let resolved_payload = resolved_control_signal_payload(
                    payload.clone(),
                    approval_gate_id.clone(),
                    blocked_call_id.clone(),
                );

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
                                "approval_gate_id": approval_gate_id.clone(),
                                "blocked_call_id": blocked_call_id.clone(),
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

                let applied_signal = self
                    .record_control_signal_applied(
                        tx,
                        row.task_id()?,
                        actor,
                        signal_id,
                        signal_type,
                        resolved_payload,
                        correlation_id,
                        received_event_id,
                        current_status,
                        TaskStatus::Failed,
                        applied_change,
                    )
                    .await?;

                if let Some(blocked_call_id) = blocked_call_id_for_deny.as_deref() {
                    let outcome_event_id = applied_signal.outcome_event_id.ok_or_else(|| {
                        RuntimeError::InvariantViolation(format!(
                            "applied control signal {} is missing outcome_event_id",
                            applied_signal.signal_id
                        ))
                    })?;
                    let updated_row = fetch_task_row(&mut **tx, row.task_id()?).await?;
                    self.deny_blocked_capability_call_after_control_in_tx(
                        tx,
                        &updated_row,
                        blocked_call_id,
                        outcome_event_id,
                    )
                    .await?;
                }

                Ok(AppliedControlSignal {
                    signal: applied_signal,
                    resume_blocked_call_id: None,
                })
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
                .map(wrap_applied_control_signal)
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
                .map(wrap_applied_control_signal)
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
                        .await
                        .map(wrap_applied_control_signal);
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
                        .await
                        .map(wrap_applied_control_signal);
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
                .map(wrap_applied_control_signal)
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
                        .await
                        .map(wrap_applied_control_signal);
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
                .map(wrap_applied_control_signal)
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
                        .await
                        .map(wrap_applied_control_signal);
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
                .map(wrap_applied_control_signal)
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
                        .await
                        .map(wrap_applied_control_signal);
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
                .map(wrap_applied_control_signal)
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
                        .await
                        .map(wrap_applied_control_signal);
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
                        .await
                        .map(wrap_applied_control_signal);
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
                            .await
                            .map(wrap_applied_control_signal);
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
                            .await
                            .map(wrap_applied_control_signal);
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
                .map(wrap_applied_control_signal)
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
                .map(wrap_applied_control_signal)
            }
            signal_type => self
                .record_control_signal_rejected(
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
                .map(wrap_applied_control_signal),
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

    async fn evaluate_capability_policy_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &TaskRow,
        actor: &PrincipalAttribution,
        call: &CapabilityCallRecord,
        contract: &CapabilityContract,
        approval_already_granted: bool,
    ) -> Result<CapabilityPolicyDecision> {
        if let Some(reason_code) = self
            .validate_capability_dispatch_preconditions_in_tx(tx, row, actor, contract)
            .await?
        {
            return Ok(CapabilityPolicyDecision::Deny {
                reason_code: reason_code.to_owned(),
            });
        }

        if !approval_already_granted
            && contract.effect_class == crate::capability::EffectClass::ExternalSideEffect
        {
            return Ok(CapabilityPolicyDecision::RequireApproval {
                approval_gate_id: format!("approval:gate:{}", call.call_id),
            });
        }

        let applied_constraints = resolve_budget_policy_constraints(&row.budget_context_ref)?;
        if !applied_constraints.is_empty() {
            return Ok(CapabilityPolicyDecision::AllowWithConstraints {
                applied_constraints,
            });
        }

        Ok(CapabilityPolicyDecision::Allow)
    }

    async fn validate_capability_dispatch_preconditions_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &TaskRow,
        actor: &PrincipalAttribution,
        contract: &CapabilityContract,
    ) -> Result<Option<&'static str>> {
        if row.status()? != TaskStatus::Running {
            return Ok(Some("task_not_running"));
        }

        let Some(status) = self.principal_status_in_tx(tx, actor.principal_id).await? else {
            return Ok(Some("principal_not_found"));
        };

        if status != PrincipalStatus::Active {
            return Ok(Some("principal_not_active"));
        }

        if actor.principal_id != row.owner_principal_id()? {
            return Ok(Some("unauthorized_task_actor"));
        }

        if !contract.status.accepts_invocation() {
            return Ok(Some("capability_not_active"));
        }

        let Some(authority_scope_ref) = self
            .principal_authority_scope_in_tx(tx, actor.principal_id)
            .await?
        else {
            return Ok(Some("principal_not_found"));
        };

        if authority_scope_ref != contract.required_authority_scope
            && authority_scope_ref != "authority/root"
        {
            return Ok(Some("insufficient_authority_scope"));
        }

        let budget_policy = match resolve_budget_policy(&row.budget_context_ref) {
            Ok(policy) => policy,
            Err(_) => return Ok(Some("budget_context_invalid")),
        };
        if budget_policy
            .as_ref()
            .map(|policy| policy.disqualified)
            .unwrap_or(false)
        {
            return Ok(Some("budget_disqualified"));
        }

        Ok(None)
    }

    async fn authorize_approval_resolution_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &TaskRow,
        actor: &PrincipalAttribution,
        blocked_call_id: Option<&str>,
    ) -> Result<ApprovalSignalAuthorization> {
        let owner_principal_id = row.owner_principal_id()?;
        if actor.principal_id == owner_principal_id {
            return Ok(ApprovalSignalAuthorization::Rejected {
                reason_code: "approval_requires_distinct_principal",
            });
        }

        let Some(authority_scope_ref) = self
            .principal_authority_scope_in_tx(tx, actor.principal_id)
            .await?
        else {
            return Ok(ApprovalSignalAuthorization::Rejected {
                reason_code: "principal_not_found",
            });
        };

        if authority_scope_ref != "authority/root" {
            return Ok(ApprovalSignalAuthorization::Rejected {
                reason_code: "approval_requires_higher_authority",
            });
        }

        if let Some(blocked_call_id) = blocked_call_id {
            if let Some(call) = self
                .load_pending_capability_call_for_control_in_tx(tx, row.task_id()?, blocked_call_id)
                .await?
            {
                if actor.principal_id == call.caller_principal_id {
                    return Ok(ApprovalSignalAuthorization::Rejected {
                        reason_code: "approval_requires_distinct_principal",
                    });
                }
            }
        }

        Ok(ApprovalSignalAuthorization::Authorized)
    }

    #[allow(clippy::too_many_arguments)]
    async fn block_capability_call_for_approval_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &TaskRow,
        actor: PrincipalAttribution,
        call: &CapabilityCallRecord,
        contract: &CapabilityContract,
        approval_gate_id: &str,
        causation_event_id: Uuid,
        request_reason_code: Option<&str>,
        trace_ref: Option<&str>,
    ) -> Result<CapabilityCallRecord> {
        let task_id = row.task_id()?;
        let trace_ref = trace_ref.map(str::to_owned);
        let blocked_event = self
            .event_log
            .append(
                tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "capability.policy_blocked".to_owned(),
                    event_family: EventFamily::Capability,
                    task_id,
                    occurred_at: self.clock.now(),
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "call_id": call.call_id.clone(),
                        "blocked_call_id": call.call_id.clone(),
                        "approval_gate_id": approval_gate_id,
                        "capability_id": contract.capability_id.clone(),
                        "capability_version": contract.capability_version.clone(),
                        "blocked_by": "policy",
                        "reason_code": "requires_higher_authority",
                    }),
                    correlation_id: Some(call.correlation_id.clone()),
                    causation_event_id: Some(causation_event_id),
                    principal: Some(actor.clone()),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref: call.checkpoint_ref,
                    state_version_ref: call.state_version_ref,
                    evidence_ref: None,
                    trace_ref: trace_ref.clone(),
                    tenant_id: None,
                },
            )
            .await?;

        let applied_change = self
            .apply_status_change(
                tx,
                row,
                actor,
                StatusChange {
                    to_status: TaskStatus::WaitingOnControl,
                    event_type: "task.awaiting_control",
                    details: json!({
                        "reason_code": "require_approval",
                        "approval_gate_id": approval_gate_id,
                        "blocked_call_id": call.call_id.clone(),
                        "details": {
                            "call_id": call.call_id.clone(),
                            "capability_id": contract.capability_id.clone(),
                            "capability_version": contract.capability_version.clone(),
                            "effect_class": contract.effect_class.as_str(),
                        },
                    }),
                    result_ref: None,
                    correlation_id: Some(call.correlation_id.clone()),
                    causation_event_id: Some(blocked_event.event_id),
                },
            )
            .await?;
        let provider_metadata = capability_approval_metadata(
            approval_gate_id,
            &call.call_id,
            request_reason_code,
            trace_ref.as_deref(),
        );

        self.capability_call_store
            .mark_awaiting_approval(
                tx,
                &call.call_id,
                &contract.capability_version,
                contract.effect_class.as_str(),
                applied_change.state_version_ref,
                "requires_higher_authority",
                Some(&provider_metadata),
                self.clock.now(),
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_capability_call(
        &self,
        task_id: Uuid,
        actor: PrincipalAttribution,
        call_id: &str,
        contract: CapabilityContract,
        causation_event_id: Uuid,
        reason_code: Option<String>,
        trace_ref: Option<String>,
        applied_constraints: Vec<Value>,
    ) -> Result<CapabilityCallRecord> {
        let mut tx = self.pool.begin().await?;
        let row = fetch_task_row(&mut *tx, task_id).await?;
        let call = self
            .capability_call_store
            .get_in_tx(&mut tx, call_id)
            .await?;

        if call.task_id != task_id {
            return Err(RuntimeError::InvariantViolation(format!(
                "capability call {} does not belong to task {}",
                call_id, task_id
            )));
        }

        let Some(handler) = self
            .capability_dispatcher
            .handler_for(&contract.capability_id, &contract.capability_version)
        else {
            let mut failed = CapabilityExecutionResult::failed(
                "handler_not_registered",
                json!({
                    "error": "capability handler is not registered",
                }),
            );
            failed.provider_metadata = call.provider_metadata.clone();
            let completed = self
                .complete_capability_call_in_tx(
                    &mut tx,
                    &row,
                    actor,
                    &call,
                    Some(&contract.capability_version),
                    contract.effect_class.as_str(),
                    failed,
                    causation_event_id,
                    trace_ref,
                    &applied_constraints,
                )
                .await?;
            tx.commit().await?;
            return Ok(completed);
        };

        let current_state_version_ref = parse_uuid("working_state_ref", &row.working_state_ref)?;
        let dispatched_at = self.clock.now();
        let dispatched_event = self
            .event_log
            .append(
                &mut tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "capability.dispatched".to_owned(),
                    event_family: EventFamily::Capability,
                    task_id,
                    occurred_at: dispatched_at,
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "call_id": call.call_id.clone(),
                        "capability_id": contract.capability_id.clone(),
                        "capability_version": contract.capability_version.clone(),
                        "provider_id": contract.provider_id.clone(),
                        "effect_class": contract.effect_class.as_str(),
                        "applied_constraints": applied_constraints.clone(),
                    }),
                    correlation_id: Some(call.correlation_id.clone()),
                    causation_event_id: Some(causation_event_id),
                    principal: Some(actor.clone()),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref: call.checkpoint_ref,
                    state_version_ref: Some(current_state_version_ref),
                    evidence_ref: None,
                    trace_ref: trace_ref.clone(),
                    tenant_id: None,
                },
            )
            .await?;
        let dispatched_call = self
            .capability_call_store
            .mark_dispatched(
                &mut tx,
                &call.call_id,
                &contract.capability_version,
                contract.effect_class.as_str(),
                Some(current_state_version_ref),
                dispatched_at,
                dispatched_event.event_id,
                dispatched_event.sequence_number,
            )
            .await?;

        tx.commit().await?;

        let invocation = crate::capability::CapabilityInvocationContext {
            call_id: dispatched_call.call_id.clone(),
            task_id,
            caller: actor.clone(),
            capability_id: contract.capability_id.clone(),
            capability_version: contract.capability_version.clone(),
            input_payload: dispatched_call.input_payload.clone(),
            policy_context_ref: dispatched_call.policy_context_ref.clone(),
            budget_context_ref: dispatched_call.budget_context_ref.clone(),
            effect_class: contract.effect_class.clone(),
            cost_class: dispatched_call
                .cost_class
                .clone()
                .unwrap_or_else(|| contract.cost_class.clone()),
            idempotency_expectation: dispatched_call.idempotency_expectation.clone(),
            checkpoint_ref: dispatched_call.checkpoint_ref,
            state_version_ref: dispatched_call.state_version_ref,
            correlation_id: dispatched_call.correlation_id.clone(),
            timeout_override_ms: dispatched_call.timeout_override_ms,
            applied_constraints: applied_constraints.clone(),
            reason_code,
            trace_ref: trace_ref.clone(),
        };
        let mut execution_result = match handler.invoke(invocation) {
            Ok(result) => result,
            Err(error) => dispatched_failure_result(
                &contract,
                "provider_error",
                json!({
                    "error": error.to_string(),
                }),
            ),
        };

        if execution_result.outcome == CapabilityOutcome::Succeeded {
            if let Err(message) = validate_payload_against_schema(
                &contract.output_schema,
                &execution_result.result_payload,
            ) {
                execution_result = CapabilityExecutionResult {
                    outcome: CapabilityOutcome::Failed,
                    result_payload: json!({
                        "error": message,
                        "validation_phase": "output",
                    }),
                    side_effect_summary: execution_result.side_effect_summary.clone(),
                    retry_safety: execution_result.retry_safety.clone(),
                    completed_at: execution_result.completed_at,
                    evidence_ref: execution_result.evidence_ref.clone(),
                    latency_ms: execution_result.latency_ms,
                    provider_metadata: execution_result.provider_metadata.clone(),
                    reason_code: Some("output_validation_failed".to_owned()),
                };
            }
        }

        let constraint_metadata = policy_constraint_metadata(&applied_constraints);
        let base_metadata = merge_provider_metadata(
            call.provider_metadata.as_ref(),
            constraint_metadata.as_ref(),
        );
        execution_result.provider_metadata = merge_provider_metadata(
            base_metadata.as_ref(),
            execution_result.provider_metadata.as_ref(),
        );

        let mut tx = self.pool.begin().await?;
        let row = fetch_task_row(&mut *tx, task_id).await?;
        let completed = self
            .complete_capability_call_in_tx(
                &mut tx,
                &row,
                actor,
                &dispatched_call,
                Some(&contract.capability_version),
                contract.effect_class.as_str(),
                execution_result,
                dispatched_event.event_id,
                trace_ref,
                &applied_constraints,
            )
            .await?;
        tx.commit().await?;
        Ok(completed)
    }

    async fn load_pending_capability_call_for_control_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: Uuid,
        blocked_call_id: &str,
    ) -> Result<Option<CapabilityCallRecord>> {
        let call = match self
            .capability_call_store
            .get_in_tx(tx, blocked_call_id)
            .await
        {
            Ok(call) => call,
            Err(RuntimeError::Database(sqlx::Error::RowNotFound)) => return Ok(None),
            Err(error) => return Err(error),
        };
        if call.task_id != task_id {
            return Err(RuntimeError::InvariantViolation(format!(
                "blocked capability call {} does not belong to task {}",
                blocked_call_id, task_id
            )));
        }
        if call.status != crate::capability::CapabilityCallStatus::PolicyBlocked
            || call.outcome.is_some()
        {
            return Err(RuntimeError::InvariantViolation(format!(
                "blocked capability call {} is not pending approval",
                blocked_call_id
            )));
        }

        Ok(Some(call))
    }

    async fn resume_blocked_capability_call_after_approval(
        &self,
        task_id: Uuid,
        blocked_call_id: &str,
        causation_event_id: Uuid,
    ) -> Result<Option<CapabilityCallRecord>> {
        let mut tx = self.pool.begin().await?;
        let row = fetch_task_row(&mut *tx, task_id).await?;
        let Some(call) = self
            .load_pending_capability_call_for_control_in_tx(&mut tx, task_id, blocked_call_id)
            .await?
        else {
            return Ok(None);
        };
        let contract = self
            .capability_registry
            .resolve_for_invocation_in_tx(
                &mut tx,
                &call.capability_id,
                call.capability_version.as_deref(),
            )
            .await?
            .ok_or_else(|| {
                RuntimeError::InvariantViolation(format!(
                    "capability {} version {:?} is unavailable for blocked call {}",
                    call.capability_id, call.capability_version, call.call_id
                ))
            })?;
        let actor = PrincipalAttribution {
            principal_id: call.caller_principal_id,
            principal_role: call.caller_principal_role.clone(),
        };
        let policy_decision = self
            .evaluate_capability_policy_in_tx(&mut tx, &row, &actor, &call, &contract, true)
            .await?;
        let applied_constraints = policy_decision.applied_constraints().to_vec();
        if let CapabilityPolicyDecision::Deny { reason_code } = policy_decision {
            let denied = self
                .deny_capability_call_in_tx(
                    &mut tx,
                    &row,
                    actor,
                    &call,
                    Some(&contract.capability_version),
                    contract.effect_class.as_str(),
                    &reason_code,
                    "policy",
                    causation_event_id,
                    capability_trace_ref(&call),
                    &applied_constraints,
                )
                .await?;
            tx.commit().await?;
            return Ok(Some(denied));
        }

        if let Err(message) =
            validate_payload_against_schema(&contract.input_schema, &call.input_payload)
        {
            let failed = self
                .complete_capability_call_in_tx(
                    &mut tx,
                    &row,
                    actor,
                    &call,
                    Some(&contract.capability_version),
                    contract.effect_class.as_str(),
                    CapabilityExecutionResult::failed(
                        "validation_failed",
                        json!({
                            "error": message,
                            "validation_phase": "input",
                        }),
                    ),
                    causation_event_id,
                    capability_trace_ref(&call),
                    &applied_constraints,
                )
                .await?;
            tx.commit().await?;
            return Ok(Some(failed));
        }

        if let Some(reason_code) =
            validate_applied_constraints(&contract, &call.input_payload, &applied_constraints)?
        {
            let denied = self
                .deny_capability_call_in_tx(
                    &mut tx,
                    &row,
                    actor,
                    &call,
                    Some(&contract.capability_version),
                    contract.effect_class.as_str(),
                    reason_code,
                    "policy",
                    causation_event_id,
                    capability_trace_ref(&call),
                    &applied_constraints,
                )
                .await?;
            tx.commit().await?;
            return Ok(Some(denied));
        }

        tx.commit().await?;
        self.dispatch_capability_call(
            task_id,
            actor,
            &call.call_id,
            contract,
            causation_event_id,
            capability_request_reason_code(&call),
            capability_trace_ref(&call),
            applied_constraints,
        )
        .await
        .map(Some)
    }

    async fn deny_blocked_capability_call_after_control_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &TaskRow,
        blocked_call_id: &str,
        causation_event_id: Uuid,
    ) -> Result<Option<CapabilityCallRecord>> {
        let Some(call) = self
            .load_pending_capability_call_for_control_in_tx(tx, row.task_id()?, blocked_call_id)
            .await?
        else {
            return Ok(None);
        };
        let actor = PrincipalAttribution {
            principal_id: call.caller_principal_id,
            principal_role: call.caller_principal_role.clone(),
        };
        let mut denied = CapabilityExecutionResult::denied(
            "approval_denied",
            json!({
                "reason_code": "approval_denied",
                "blocked_by": "control_signal",
            }),
        );
        denied.provider_metadata = call.provider_metadata.clone();

        self.complete_capability_call_in_tx(
            tx,
            row,
            actor,
            &call,
            call.capability_version.as_deref(),
            &call.effect_class,
            denied,
            causation_event_id,
            capability_trace_ref(&call),
            &[],
        )
        .await
        .map(Some)
    }

    async fn deny_capability_call_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &TaskRow,
        actor: PrincipalAttribution,
        call: &CapabilityCallRecord,
        capability_version: Option<&str>,
        effect_class: &str,
        reason_code: &str,
        blocked_by: &str,
        causation_event_id: Uuid,
        trace_ref: Option<String>,
        applied_constraints: &[Value],
    ) -> Result<CapabilityCallRecord> {
        let task_id = row.task_id()?;
        let current_state_version_ref = parse_uuid("working_state_ref", &row.working_state_ref)?;
        let constraint_metadata = policy_constraint_metadata(applied_constraints);
        let provider_metadata = merge_provider_metadata(
            call.provider_metadata.as_ref(),
            constraint_metadata.as_ref(),
        );
        let blocked_event = self
            .event_log
            .append(
                tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: "capability.policy_blocked".to_owned(),
                    event_family: EventFamily::Capability,
                    task_id,
                    occurred_at: self.clock.now(),
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "call_id": call.call_id.clone(),
                        "blocked_call_id": call.call_id.clone(),
                        "capability_id": call.capability_id.clone(),
                        "capability_version": capability_version,
                        "blocked_by": blocked_by,
                        "reason_code": reason_code,
                        "applied_constraints": applied_constraints,
                    }),
                    correlation_id: Some(call.correlation_id.clone()),
                    causation_event_id: Some(causation_event_id),
                    principal: Some(actor.clone()),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref: call.checkpoint_ref,
                    state_version_ref: Some(current_state_version_ref),
                    evidence_ref: None,
                    trace_ref: trace_ref.clone(),
                    tenant_id: None,
                },
            )
            .await?;

        let completed_at = self.clock.now();
        let result_payload = json!({
            "reason_code": reason_code,
            "blocked_by": blocked_by,
        });
        let result_event = self
            .event_log
            .append(
                tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: CapabilityOutcome::Denied.result_event_type().to_owned(),
                    event_family: EventFamily::Capability,
                    task_id,
                    occurred_at: completed_at,
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "call_id": call.call_id.clone(),
                        "capability_id": call.capability_id.clone(),
                        "capability_version": capability_version,
                        "outcome": CapabilityOutcome::Denied.as_str(),
                        "completed_at": encode_timestamp(completed_at)?,
                        "result_payload": result_payload.clone(),
                        "side_effect_summary": SideEffectSummary::NotAttempted.as_str(),
                        "retry_safety": RetrySafety::RequiresOperatorDecision.as_str(),
                        "reason_code": reason_code,
                        "applied_constraints": applied_constraints,
                        "provider_metadata": provider_metadata.clone(),
                    }),
                    correlation_id: Some(call.correlation_id.clone()),
                    causation_event_id: Some(blocked_event.event_id),
                    principal: Some(actor),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref: call.checkpoint_ref,
                    state_version_ref: Some(current_state_version_ref),
                    evidence_ref: None,
                    trace_ref,
                    tenant_id: None,
                },
            )
            .await?;

        self.capability_call_store
            .mark_completed(
                tx,
                &call.call_id,
                capability_version,
                effect_class,
                Some(current_state_version_ref),
                completed_at,
                CapabilityOutcome::Denied,
                &result_payload,
                SideEffectSummary::NotAttempted,
                RetrySafety::RequiresOperatorDecision,
                provider_metadata.as_ref(),
                None,
                None,
                Some(reason_code),
                result_event.event_id,
                result_event.sequence_number,
            )
            .await
    }

    async fn complete_capability_call_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        row: &TaskRow,
        actor: PrincipalAttribution,
        call: &CapabilityCallRecord,
        capability_version: Option<&str>,
        effect_class: &str,
        execution_result: CapabilityExecutionResult,
        causation_event_id: Uuid,
        trace_ref: Option<String>,
        applied_constraints: &[Value],
    ) -> Result<CapabilityCallRecord> {
        let task_id = row.task_id()?;
        let current_state_version_ref = parse_uuid("working_state_ref", &row.working_state_ref)?;
        let completed_at = execution_result
            .completed_at
            .unwrap_or_else(|| self.clock.now());
        let constraint_metadata = policy_constraint_metadata(applied_constraints);
        let provider_metadata = merge_provider_metadata(
            call.provider_metadata.as_ref(),
            constraint_metadata.as_ref(),
        );
        let provider_metadata = merge_provider_metadata(
            provider_metadata.as_ref(),
            execution_result.provider_metadata.as_ref(),
        );
        let result_event = self
            .event_log
            .append(
                tx,
                self.id_generator.as_ref(),
                self.clock.as_ref(),
                EventDraft {
                    event_type: execution_result.outcome.result_event_type().to_owned(),
                    event_family: EventFamily::Capability,
                    task_id,
                    occurred_at: completed_at,
                    emitted_by: self.config.emitted_by.clone(),
                    payload: json!({
                        "call_id": call.call_id.clone(),
                        "capability_id": call.capability_id.clone(),
                        "capability_version": capability_version,
                        "outcome": execution_result.outcome.as_str(),
                        "completed_at": encode_timestamp(completed_at)?,
                        "result_payload": execution_result.result_payload.clone(),
                        "side_effect_summary": execution_result.side_effect_summary.as_str(),
                        "retry_safety": execution_result.retry_safety.as_str(),
                        "reason_code": execution_result.reason_code.clone(),
                        "applied_constraints": applied_constraints,
                        "provider_metadata": provider_metadata.clone(),
                    }),
                    correlation_id: Some(call.correlation_id.clone()),
                    causation_event_id: Some(causation_event_id),
                    principal: Some(actor),
                    policy_context_ref: Some(row.policy_context_ref.clone()),
                    budget_context_ref: Some(row.budget_context_ref.clone()),
                    checkpoint_ref: call.checkpoint_ref,
                    state_version_ref: Some(current_state_version_ref),
                    evidence_ref: execution_result.evidence_ref.clone(),
                    trace_ref,
                    tenant_id: None,
                },
            )
            .await?;

        self.capability_call_store
            .mark_completed(
                tx,
                &call.call_id,
                capability_version,
                effect_class,
                Some(current_state_version_ref),
                completed_at,
                execution_result.outcome,
                &execution_result.result_payload,
                execution_result.side_effect_summary,
                execution_result.retry_safety,
                provider_metadata.as_ref(),
                execution_result.evidence_ref.as_deref(),
                execution_result.latency_ms,
                execution_result.reason_code.as_deref(),
                result_event.event_id,
                result_event.sequence_number,
            )
            .await
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

    async fn validate_restorable_in_flight_capability_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: Uuid,
        checkpoint_status: TaskStatus,
        in_flight_capability: Option<&Value>,
    ) -> Result<()> {
        let Some(in_flight_capability) = in_flight_capability else {
            return Ok(());
        };

        let Some(call_id) = in_flight_capability.get("call_id").and_then(Value::as_str) else {
            return Err(RuntimeError::InvariantViolation(
                "checkpoint in_flight_capability is missing call_id".to_owned(),
            ));
        };

        if checkpoint_status != TaskStatus::WaitingOnControl {
            return Err(RuntimeError::InvariantViolation(format!(
                "checkpoint references in-flight capability {} outside waiting_on_control",
                call_id
            )));
        }

        let call = match self
            .load_pending_capability_call_for_control_in_tx(tx, task_id, call_id)
            .await
        {
            Ok(Some(call)) => call,
            Ok(None) | Err(RuntimeError::InvariantViolation(_)) => {
                return Err(RuntimeError::InvariantViolation(format!(
                    "checkpoint references blocked capability call {} that is no longer recoverable",
                    call_id
                )));
            }
            Err(error) => return Err(error),
        };

        if call.status != crate::capability::CapabilityCallStatus::PolicyBlocked
            || call.outcome.is_some()
        {
            return Err(RuntimeError::InvariantViolation(format!(
                "checkpoint references capability call {} that is not pending approval",
                call_id
            )));
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

    async fn principal_authority_scope_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        principal_id: Uuid,
    ) -> Result<Option<String>> {
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT authority_scope_ref
            FROM principals
            WHERE principal_id = ?
            "#,
        )
        .bind(principal_id.to_string())
        .fetch_optional(&mut **tx)
        .await
        .map_err(Into::into)
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

#[derive(Debug, Deserialize)]
struct InlineBudgetContext {
    limit: InlineBudgetLimit,
    spent: InlineBudgetSpent,
    #[serde(default)]
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct InlineBudgetLimit {
    amount: f64,
    #[serde(default)]
    soft_amount: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct InlineBudgetSpent {
    amount: f64,
}

#[derive(Debug)]
struct BudgetPolicyResolution {
    disqualified: bool,
    applied_constraints: Vec<Value>,
}

fn wrap_applied_control_signal(signal: ControlSignalRecord) -> AppliedControlSignal {
    AppliedControlSignal {
        signal,
        resume_blocked_call_id: None,
    }
}

fn resolved_control_signal_payload(
    payload: Value,
    approval_gate_id: Option<String>,
    blocked_call_id: Option<String>,
) -> Value {
    match payload {
        Value::Object(mut object) => {
            if let Some(approval_gate_id) = approval_gate_id {
                object
                    .entry("approval_gate_id".to_owned())
                    .or_insert(Value::String(approval_gate_id));
            }
            if let Some(blocked_call_id) = blocked_call_id {
                object
                    .entry("blocked_call_id".to_owned())
                    .or_insert(Value::String(blocked_call_id));
            }
            Value::Object(object)
        }
        payload => json!({
            "approval_gate_id": approval_gate_id,
            "blocked_call_id": blocked_call_id,
            "details": payload,
        }),
    }
}

fn resolve_budget_policy(budget_context_ref: &str) -> Result<Option<BudgetPolicyResolution>> {
    let Some(raw_budget) = budget_context_ref.strip_prefix("budget:inline:") else {
        return Ok(None);
    };
    let budget: InlineBudgetContext = serde_json::from_str(raw_budget).map_err(|error| {
        RuntimeError::InvariantViolation(format!("invalid inline budget context: {error}"))
    })?;

    let policy = budget
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("policy"));
    let mut disqualified = budget.spent.amount >= budget.limit.amount;
    if policy
        .and_then(|value| value.get("disqualified"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        disqualified = true;
    }

    let mut applied_constraints = Vec::new();
    if let Some(soft_amount) = budget.limit.soft_amount {
        if budget.spent.amount >= soft_amount {
            if let Some(max_cost_class) = policy
                .and_then(|value| value.get("max_cost_class"))
                .and_then(Value::as_str)
            {
                let max_cost_class: CostClass = max_cost_class.parse()?;
                applied_constraints.push(json!({
                    "kind": "max_cost_class",
                    "max_cost_class": max_cost_class.as_str(),
                    "reason_code": "budget_soft_limit_active",
                    "source": "budget_context",
                }));
            }

            if let Some(prefixes) = policy.and_then(|value| value.get("allowed_target_prefixes")) {
                let prefixes = prefixes.as_array().ok_or_else(|| {
                    RuntimeError::InvariantViolation(
                        "budget policy allowed_target_prefixes must be an array".to_owned(),
                    )
                })?;
                let allowed_prefixes = prefixes
                    .iter()
                    .map(|value| {
                        value.as_str().map(str::to_owned).ok_or_else(|| {
                            RuntimeError::InvariantViolation(
                                "budget policy allowed_target_prefixes must contain only strings"
                                    .to_owned(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                if !allowed_prefixes.is_empty() {
                    applied_constraints.push(json!({
                        "kind": "allowed_target_prefixes",
                        "field": "target",
                        "allowed_prefixes": allowed_prefixes,
                        "reason_code": "budget_soft_limit_target_scope",
                        "source": "budget_context",
                    }));
                }
            }
        }
    }

    Ok(Some(BudgetPolicyResolution {
        disqualified,
        applied_constraints,
    }))
}

fn budget_execution_is_disqualified(budget_context_ref: &str) -> Result<bool> {
    Ok(resolve_budget_policy(budget_context_ref)?
        .as_ref()
        .map(|policy| policy.disqualified)
        .unwrap_or(false))
}

fn resolve_budget_policy_constraints(budget_context_ref: &str) -> Result<Vec<Value>> {
    Ok(resolve_budget_policy(budget_context_ref)?
        .map(|policy| policy.applied_constraints)
        .unwrap_or_default())
}

fn validate_applied_constraints(
    contract: &CapabilityContract,
    input_payload: &Value,
    applied_constraints: &[Value],
) -> Result<Option<&'static str>> {
    for constraint in applied_constraints {
        let Some(kind) = constraint.get("kind").and_then(Value::as_str) else {
            return Err(RuntimeError::InvariantViolation(
                "applied constraint is missing kind".to_owned(),
            ));
        };

        match kind {
            "max_cost_class" => {
                let max_cost_class = constraint
                    .get("max_cost_class")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        RuntimeError::InvariantViolation(
                            "max_cost_class constraint is missing max_cost_class".to_owned(),
                        )
                    })?;
                let max_cost_class: CostClass = max_cost_class.parse()?;
                if cost_class_rank(&contract.cost_class) > cost_class_rank(&max_cost_class) {
                    return Ok(Some("budget_cost_class_disallowed"));
                }
            }
            "allowed_target_prefixes" => {
                let field = constraint
                    .get("field")
                    .and_then(Value::as_str)
                    .unwrap_or("target");
                let allowed_prefixes = constraint
                    .get("allowed_prefixes")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        RuntimeError::InvariantViolation(
                            "allowed_target_prefixes constraint is missing allowed_prefixes"
                                .to_owned(),
                        )
                    })?;
                let Some(target) = input_payload.get(field).and_then(Value::as_str) else {
                    return Ok(Some("budget_target_scope_disallowed"));
                };
                let allowed = allowed_prefixes.iter().any(|value| {
                    value
                        .as_str()
                        .map(|prefix| target.starts_with(prefix))
                        .unwrap_or(false)
                });
                if !allowed {
                    return Ok(Some("budget_target_scope_disallowed"));
                }
            }
            other => {
                return Err(RuntimeError::InvariantViolation(format!(
                    "unsupported applied constraint kind {other}",
                )));
            }
        }
    }

    Ok(None)
}

fn cost_class_rank(cost_class: &CostClass) -> u8 {
    match cost_class {
        CostClass::Low => 0,
        CostClass::Moderate => 1,
        CostClass::High => 2,
        CostClass::Variable => 3,
    }
}

fn control_signal_linkage(payload: &Value) -> (Option<String>, Option<String>) {
    let approval_gate_id = payload
        .get("approval_gate_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let blocked_call_id = payload
        .get("blocked_call_id")
        .or_else(|| payload.get("call_id"))
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
            None => {
                if required_blocked_call_id.is_none()
                    || requested_blocked_call_id.as_deref() != required_blocked_call_id
                {
                    return Err("approval_gate_id_required");
                }
            }
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

fn capability_approval_metadata(
    approval_gate_id: &str,
    blocked_call_id: &str,
    request_reason_code: Option<&str>,
    trace_ref: Option<&str>,
) -> Value {
    json!({
        "approval_gate_id": approval_gate_id,
        "blocked_call_id": blocked_call_id,
        "blocked_by": "policy",
        "request_reason_code": request_reason_code,
        "trace_ref": trace_ref,
    })
}

fn capability_approval_gate_id(call: &CapabilityCallRecord) -> Option<String> {
    call.provider_metadata
        .as_ref()?
        .get("approval_gate_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn capability_request_reason_code(call: &CapabilityCallRecord) -> Option<String> {
    if let Some(request_reason_code) = &call.request_reason_code {
        return Some(request_reason_code.clone());
    }

    call.provider_metadata
        .as_ref()?
        .get("request_reason_code")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn capability_trace_ref(call: &CapabilityCallRecord) -> Option<String> {
    call.provider_metadata
        .as_ref()?
        .get("trace_ref")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn checkpoint_in_flight_capability(call: &CapabilityCallRecord) -> Value {
    json!({
        "call_id": call.call_id,
        "blocked_call_id": call.call_id,
        "capability_id": call.capability_id,
        "capability_version": call.capability_version,
        "status": call.status.as_str(),
        "effect_class": call.effect_class,
        "correlation_id": call.correlation_id,
        "approval_gate_id": capability_approval_gate_id(call),
        "reason_code": call.reason_code,
        "state_version_ref": call.state_version_ref.map(|value| value.to_string()),
        "checkpoint_ref": call.checkpoint_ref.map(|value| value.to_string()),
    })
}

fn policy_constraint_metadata(applied_constraints: &[Value]) -> Option<Value> {
    if applied_constraints.is_empty() {
        None
    } else {
        Some(json!({
            "applied_constraints": applied_constraints,
        }))
    }
}

fn merge_provider_metadata(base: Option<&Value>, overlay: Option<&Value>) -> Option<Value> {
    match (base, overlay) {
        (None, None) => None,
        (Some(base), None) => Some(base.clone()),
        (None, Some(overlay)) => Some(overlay.clone()),
        (Some(Value::Object(base)), Some(Value::Object(overlay))) => {
            let mut merged = base.clone();
            for (key, value) in overlay {
                merged.insert(key.clone(), value.clone());
            }
            Some(Value::Object(merged))
        }
        (Some(Value::Object(base)), Some(overlay)) => {
            let mut merged = base.clone();
            merged.insert("provider_overlay".to_owned(), overlay.clone());
            Some(Value::Object(merged))
        }
        (_, Some(overlay)) => Some(overlay.clone()),
    }
}

fn dispatched_failure_result(
    contract: &CapabilityContract,
    reason_code: impl Into<String>,
    result_payload: Value,
) -> CapabilityExecutionResult {
    let mut failed = CapabilityExecutionResult::failed(reason_code, result_payload);
    match contract.effect_class {
        crate::capability::EffectClass::ReadOnly => {}
        crate::capability::EffectClass::StateMutatingInternal
        | crate::capability::EffectClass::ExternalSideEffect
        | crate::capability::EffectClass::AuthorityMediated
        | crate::capability::EffectClass::Mixed => {
            failed.side_effect_summary = SideEffectSummary::Unknown;
            failed.retry_safety = RetrySafety::RequiresOperatorDecision;
        }
    }
    failed
}
