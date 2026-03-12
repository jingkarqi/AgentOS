use std::sync::Arc;

use agentos_mrr::{
    CapabilityExecutionResult, CapabilityHandler, CapabilityOutcome, ControlSignalStatus,
    ControlSignalType, CostClass, CreateTaskCommand, DeterministicIdGenerator, EffectClass,
    FixedClock, IdempotencyClass, InvokeCapabilityCommand, Principal, PrincipalAttribution,
    PrincipalStatus, RegisterCapabilityCommand, RetrySafety, RuntimeDb, SideEffectSummary,
    SubmitControlSignalCommand, TaskManager, TaskManagerConfig, TaskStatus, TimeoutClass,
    TrustLevel,
};
use serde_json::json;
use tempfile::TempDir;
use time::OffsetDateTime;
use uuid::Uuid;

struct TestHarness {
    manager: TaskManager,
    owner_principal_id: Uuid,
}

struct EchoHandler;

impl CapabilityHandler for EchoHandler {
    fn invoke(
        &self,
        request: agentos_mrr::CapabilityInvocationContext,
    ) -> agentos_mrr::Result<CapabilityExecutionResult> {
        Ok(CapabilityExecutionResult::succeeded(json!({
            "echo": request.input_payload["message"].clone(),
            "call_id": request.call_id,
        })))
    }
}

struct SideEffectHandler;

impl CapabilityHandler for SideEffectHandler {
    fn invoke(
        &self,
        request: agentos_mrr::CapabilityInvocationContext,
    ) -> agentos_mrr::Result<CapabilityExecutionResult> {
        let mut result = CapabilityExecutionResult::succeeded(json!({
            "status": "written",
            "target": request.input_payload["target"].clone(),
        }));
        result.side_effect_summary = SideEffectSummary::Committed;
        result.retry_safety = RetrySafety::Unsafe;
        result.provider_metadata = Some(json!({"provider": "test-side-effect"}));
        Ok(result)
    }
}

struct FailingSideEffectHandler;

impl CapabilityHandler for FailingSideEffectHandler {
    fn invoke(
        &self,
        _request: agentos_mrr::CapabilityInvocationContext,
    ) -> agentos_mrr::Result<CapabilityExecutionResult> {
        Err(agentos_mrr::RuntimeError::InvariantViolation(
            "simulated provider failure".to_owned(),
        ))
    }
}

struct InvalidOutputSideEffectHandler;

impl CapabilityHandler for InvalidOutputSideEffectHandler {
    fn invoke(
        &self,
        request: agentos_mrr::CapabilityInvocationContext,
    ) -> agentos_mrr::Result<CapabilityExecutionResult> {
        let mut result = CapabilityExecutionResult::succeeded(json!({
            "unexpected": request.input_payload["target"].clone(),
        }));
        result.side_effect_summary = SideEffectSummary::Committed;
        result.retry_safety = RetrySafety::Unsafe;
        result.provider_metadata = Some(json!("opaque-provider-metadata"));
        Ok(result)
    }
}

struct ConstraintAwareEchoHandler;

impl CapabilityHandler for ConstraintAwareEchoHandler {
    fn invoke(
        &self,
        request: agentos_mrr::CapabilityInvocationContext,
    ) -> agentos_mrr::Result<CapabilityExecutionResult> {
        Ok(CapabilityExecutionResult::succeeded(json!({
            "echo": request.input_payload["message"].clone(),
            "call_id": request.call_id,
            "constraint_count": request.applied_constraints.len(),
        })))
    }
}

struct DurableDispatchObserverPanicHandler {
    database_url: String,
}

impl CapabilityHandler for DurableDispatchObserverPanicHandler {
    fn invoke(
        &self,
        request: agentos_mrr::CapabilityInvocationContext,
    ) -> agentos_mrr::Result<CapabilityExecutionResult> {
        let database_url = self.database_url.clone();
        let call_id = request.call_id.clone();
        let correlation_id = request.correlation_id.clone();
        let task_id = request.task_id;
        let observed = tokio::runtime::Runtime::new()
            .expect("runtime should initialize for durable-boundary observer")
            .block_on(async move {
                let db = RuntimeDb::connect_read_only(&database_url)
                    .await
                    .expect("read-only db should open");
                let status = sqlx::query_scalar::<_, String>(
                    "SELECT status FROM capability_calls WHERE call_id = ?",
                )
                .bind(&call_id)
                .fetch_one(db.pool())
                .await
                .expect("call record should be visible after durable dispatch");
                let dispatched_count = sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM events WHERE task_id = ? AND event_type = 'capability.dispatched' AND correlation_id = ?",
                )
                .bind(task_id.to_string())
                .bind(&correlation_id)
                .fetch_one(db.pool())
                .await
                .expect("dispatched event should be visible after durable dispatch");
                (status, dispatched_count)
            });
        assert_eq!(observed.0, "dispatched");
        assert_eq!(observed.1, 1);
        panic!("simulated provider crash after durable dispatch");
    }
}

struct PanicHandler;

impl CapabilityHandler for PanicHandler {
    fn invoke(
        &self,
        _request: agentos_mrr::CapabilityInvocationContext,
    ) -> agentos_mrr::Result<CapabilityExecutionResult> {
        panic!("handler should not be invoked")
    }
}

async fn setup_harness() -> TestHarness {
    let db = RuntimeDb::connect_and_migrate("sqlite::memory:")
        .await
        .expect("in-memory db should initialize");
    let fixed_time = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let manager = TaskManager::new(
        db.clone_pool(),
        Arc::new(FixedClock::new(fixed_time)),
        Arc::new(DeterministicIdGenerator::new(10_000)),
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

async fn setup_file_harness() -> (TestHarness, TempDir, String) {
    let tempdir = TempDir::new().expect("tempdir should initialize");
    let database_url = format!(
        "sqlite://{}",
        tempdir.path().join("runtime.sqlite").display()
    );
    let db = RuntimeDb::connect_and_migrate(&database_url)
        .await
        .expect("file-backed db should initialize");
    let fixed_time = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let manager = TaskManager::new(
        db.clone_pool(),
        Arc::new(FixedClock::new(fixed_time)),
        Arc::new(DeterministicIdGenerator::new(20_000)),
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

    (
        TestHarness {
            manager,
            owner_principal_id,
        },
        tempdir,
        database_url,
    )
}

fn actor(principal_id: Uuid) -> PrincipalAttribution {
    PrincipalAttribution::acting(principal_id)
}

fn approving_actor(principal_id: Uuid) -> PrincipalAttribution {
    PrincipalAttribution {
        principal_id,
        principal_role: "approving".to_owned(),
    }
}

async fn create_active_root_principal(
    harness: &TestHarness,
    principal_id: Uuid,
    display_name: &str,
) {
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

async fn create_running_task(harness: &TestHarness, goal: &str) -> Uuid {
    let task_id = harness
        .manager
        .create_task(CreateTaskCommand::new(goal, harness.owner_principal_id))
        .await
        .expect("task should be created")
        .task_id;
    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("task should start");
    task_id
}

async fn create_running_task_with_budget(
    harness: &TestHarness,
    goal: &str,
    budget_context_ref: &str,
) -> Uuid {
    let mut command = CreateTaskCommand::new(goal, harness.owner_principal_id);
    command.budget_context_ref = budget_context_ref.to_owned();
    let task_id = harness
        .manager
        .create_task(command)
        .await
        .expect("task should be created")
        .task_id;
    harness
        .manager
        .start_task(task_id, actor(harness.owner_principal_id))
        .await
        .expect("task should start");
    task_id
}

fn echo_capability_registration() -> RegisterCapabilityCommand {
    RegisterCapabilityCommand {
        capability_id: "cap.echo".to_owned(),
        capability_name: "Echo".to_owned(),
        capability_version: "1.0.0".to_owned(),
        contract_version: "contract.v0.1".to_owned(),
        description: "Echoes a message payload".to_owned(),
        provider_id: "test.echo".to_owned(),
        status: agentos_mrr::CapabilityStatus::Active,
        input_schema: json!({
            "type": "object",
            "required": ["message"],
            "properties": {
                "message": {"type": "string"}
            },
            "additionalProperties": false
        }),
        output_schema: json!({
            "type": "object",
            "required": ["echo", "call_id"],
            "properties": {
                "echo": {"type": "string"},
                "call_id": {"type": "string"}
            },
            "additionalProperties": false
        }),
        effect_class: EffectClass::ReadOnly,
        idempotency_class: IdempotencyClass::Idempotent,
        timeout_class: TimeoutClass::Short,
        cost_class: CostClass::Low,
        trust_level: TrustLevel::RuntimeTrusted,
        required_authority_scope: "authority/root".to_owned(),
        observability_class: "full".to_owned(),
        streaming_support: Some(false),
        supports_cancellation: Some(false),
        supports_partial_result: Some(false),
        supports_checkpoint_safe_resume: Some(false),
        side_effect_surface: Some(json!({"kind": "none"})),
        trust_zone_surface: None,
        external_dependencies: None,
        sandbox_profile: None,
        evidence_profile: None,
        rate_limit_class: None,
        tenant_restrictions: None,
    }
}

fn constraint_echo_capability_registration(cost_class: CostClass) -> RegisterCapabilityCommand {
    let mut command = echo_capability_registration();
    command.cost_class = cost_class;
    command.output_schema = json!({
        "type": "object",
        "required": ["echo", "call_id", "constraint_count"],
        "properties": {
            "echo": {"type": "string"},
            "call_id": {"type": "string"},
            "constraint_count": {"type": "integer", "minimum": 0}
        },
        "additionalProperties": false
    });
    command
}

fn side_effect_capability_registration() -> RegisterCapabilityCommand {
    RegisterCapabilityCommand {
        capability_id: "cap.side_effect".to_owned(),
        capability_name: "Side Effect".to_owned(),
        capability_version: "1.0.0".to_owned(),
        contract_version: "contract.v0.1".to_owned(),
        description: "Writes to an external sink".to_owned(),
        provider_id: "test.side-effect".to_owned(),
        status: agentos_mrr::CapabilityStatus::Active,
        input_schema: json!({
            "type": "object",
            "required": ["target"],
            "properties": {
                "target": {"type": "string"}
            },
            "additionalProperties": false
        }),
        output_schema: json!({
            "type": "object",
            "required": ["status", "target"],
            "properties": {
                "status": {"type": "string"},
                "target": {"type": "string"}
            },
            "additionalProperties": false
        }),
        effect_class: EffectClass::ExternalSideEffect,
        idempotency_class: IdempotencyClass::NonIdempotent,
        timeout_class: TimeoutClass::Medium,
        cost_class: CostClass::Moderate,
        trust_level: TrustLevel::Sandboxed,
        required_authority_scope: "authority/root".to_owned(),
        observability_class: "full".to_owned(),
        streaming_support: Some(false),
        supports_cancellation: Some(false),
        supports_partial_result: Some(false),
        supports_checkpoint_safe_resume: Some(false),
        side_effect_surface: Some(json!({"kind": "external"})),
        trust_zone_surface: None,
        external_dependencies: Some(json!(["test-sink"])),
        sandbox_profile: Some(json!({"network": false})),
        evidence_profile: None,
        rate_limit_class: None,
        tenant_restrictions: None,
    }
}

fn mixed_effect_capability_registration() -> RegisterCapabilityCommand {
    let mut command = side_effect_capability_registration();
    command.capability_id = "cap.mixed_effect".to_owned();
    command.capability_name = "Mixed Effect".to_owned();
    command.provider_id = "test.mixed-effect".to_owned();
    command.effect_class = EffectClass::Mixed;
    command
}

fn inline_budget_context(
    limit_amount: f64,
    soft_amount: Option<f64>,
    spent_amount: f64,
    policy: serde_json::Value,
) -> String {
    format!(
        "budget:inline:{}",
        json!({
            "budget_context_id": "budget-inline-test",
            "unit": "credits",
            "limit": {
                "amount": limit_amount,
                "soft_amount": soft_amount,
            },
            "spent": {
                "amount": spent_amount,
                "as_of": "2026-03-14T00:00:00Z",
            },
            "scope": {
                "task_id": "task-inline-test",
            },
            "updated_at": "2026-03-14T00:00:00Z",
            "metadata": {
                "policy": policy,
            },
        })
    )
}

#[tokio::test]
async fn cts_01_and_08_capability_calls_are_attributed_and_linked() {
    let harness = setup_harness().await;
    let task_id = create_running_task(&harness, "capability-echo").await;

    harness
        .manager
        .register_capability(echo_capability_registration())
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.echo",
        "1.0.0",
        Arc::new(EchoHandler),
    );

    let capabilities = harness
        .manager
        .list_capabilities()
        .await
        .expect("capabilities should list");
    assert_eq!(capabilities.len(), 1);
    assert_eq!(capabilities[0].capability_id, "cap.echo");
    assert_eq!(capabilities[0].effect_class, EffectClass::ReadOnly);

    let call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new("cap.echo", json!({"message": "hello"})),
        )
        .await
        .expect("capability invoke should succeed");

    assert_eq!(call.caller_principal_id, harness.owner_principal_id);
    assert_eq!(call.outcome, Some(CapabilityOutcome::Succeeded));
    assert!(call.result_payload.as_ref().expect("payload").is_object());

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
            "capability.requested",
            "policy.evaluation.requested",
            "policy.evaluation.completed",
            "policy.result.allow",
            "capability.dispatched",
            "capability.result.succeeded",
        ]
    );

    let requested = &events[3];
    let dispatched = &events[7];
    let result = &events[8];
    assert_eq!(requested.principal_id, Some(harness.owner_principal_id));
    assert_eq!(requested.payload["call_id"], call.call_id);
    assert_eq!(call.correlation_id, call.call_id);
    assert_eq!(
        requested.correlation_id.as_deref(),
        Some(call.call_id.as_str())
    );
    assert_eq!(requested.correlation_id, dispatched.correlation_id);
    assert_eq!(dispatched.correlation_id, result.correlation_id);
    assert_eq!(result.causation_event_id, Some(dispatched.event_id));

    let calls = harness
        .manager
        .list_capability_calls_by_task(task_id)
        .await
        .expect("call records should load");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].call_id, call.call_id);
    assert_eq!(calls[0].cost_class, Some(CostClass::Low));
    assert_eq!(calls[0].request_reason_code, None);
}

#[tokio::test]
async fn cts_09_unregistered_capability_is_denied_before_dispatch() {
    let harness = setup_harness().await;
    let task_id = create_running_task(&harness, "capability-missing").await;

    let call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new("cap.missing", json!({"message": "hello"})),
        )
        .await
        .expect("missing capability should still produce an auditable record");

    assert_eq!(call.outcome, Some(CapabilityOutcome::Denied));
    assert_eq!(call.reason_code.as_deref(), Some("unregistered_capability"));
    assert_eq!(call.status, agentos_mrr::CapabilityCallStatus::Completed);

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
            "capability.requested",
            "capability.policy_blocked",
            "capability.result.denied",
        ]
    );
    assert!(!event_types.contains(&"capability.dispatched"));
}

#[tokio::test]
async fn cts_08_invocation_audit_preserves_request_reason_and_cost_class() {
    let harness = setup_harness().await;
    let task_id = create_running_task(&harness, "capability-audit-fields").await;

    harness
        .manager
        .register_capability(echo_capability_registration())
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.echo",
        "1.0.0",
        Arc::new(EchoHandler),
    );

    let mut command = InvokeCapabilityCommand::new("cap.echo", json!({"message": "hello"}));
    command.reason_code = Some("need_context".to_owned());
    let call = harness
        .manager
        .invoke_capability(task_id, actor(harness.owner_principal_id), command)
        .await
        .expect("capability invoke should succeed");

    assert_eq!(call.request_reason_code.as_deref(), Some("need_context"));
    assert_eq!(call.reason_code, None);
    assert_eq!(call.cost_class, Some(CostClass::Low));
}

#[tokio::test]
async fn cts_10_input_schema_validation_fails_before_dispatch() {
    let harness = setup_harness().await;
    let task_id = create_running_task(&harness, "capability-validation").await;

    harness
        .manager
        .register_capability(echo_capability_registration())
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.echo",
        "1.0.0",
        Arc::new(PanicHandler),
    );

    let call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new("cap.echo", json!({})),
        )
        .await
        .expect("validation failure should still return a call record");

    assert_eq!(call.outcome, Some(CapabilityOutcome::Failed));
    assert_eq!(call.reason_code.as_deref(), Some("validation_failed"));

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
            "capability.requested",
            "policy.evaluation.requested",
            "policy.evaluation.completed",
            "policy.result.allow",
            "capability.result.failed",
        ]
    );
    assert!(!event_types.contains(&"capability.dispatched"));
}

#[tokio::test]
async fn cts_11_12_18_and_22_side_effect_calls_wait_for_approval_and_resume_after_approve() {
    let harness = setup_harness().await;
    let approver_principal_id = Uuid::from_u128(2);
    create_active_root_principal(&harness, approver_principal_id, "Approver").await;
    let task_id = create_running_task(&harness, "capability-side-effect").await;

    harness
        .manager
        .register_capability(side_effect_capability_registration())
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.side_effect",
        "1.0.0",
        Arc::new(SideEffectHandler),
    );

    let call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new(
                "cap.side_effect",
                json!({"target": "tenant://workspace/output.txt"}),
            ),
        )
        .await
        .expect("capability invoke should record a blocked call");

    assert_eq!(
        call.status,
        agentos_mrr::CapabilityCallStatus::PolicyBlocked
    );
    assert_eq!(call.outcome, None);
    assert_eq!(
        call.reason_code.as_deref(),
        Some("requires_higher_authority")
    );
    assert_eq!(
        harness
            .manager
            .get_task(task_id)
            .await
            .expect("task should load")
            .status,
        TaskStatus::WaitingOnControl
    );

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
            "capability.requested",
            "policy.evaluation.requested",
            "policy.evaluation.completed",
            "policy.result.require_approval",
            "capability.policy_blocked",
            "task.awaiting_control",
        ]
    );
    assert!(!event_types.contains(&"capability.dispatched"));

    let checkpoint = harness
        .manager
        .create_checkpoint(
            task_id,
            actor(harness.owner_principal_id),
            json!({"reason": "before-approval"}),
        )
        .await
        .expect("checkpoint should be created while approval is pending");
    assert_eq!(checkpoint.status, TaskStatus::WaitingOnControl.as_str());
    assert_eq!(
        checkpoint.payload["in_flight_capability"]["call_id"],
        json!(call.call_id.clone())
    );
    assert_eq!(
        checkpoint.payload["in_flight_capability"]["approval_gate_id"],
        call.provider_metadata
            .as_ref()
            .expect("provider metadata should carry approval linkage")["approval_gate_id"]
    );
    let pending_call = harness
        .manager
        .list_capability_calls_by_task(task_id)
        .await
        .expect("call records should load")
        .into_iter()
        .find(|candidate| candidate.call_id == call.call_id)
        .expect("pending blocked call should still exist");
    assert_eq!(pending_call.checkpoint_ref, Some(checkpoint.checkpoint_id));

    let approval_gate_id = call.provider_metadata.as_ref().and_then(|metadata| {
        metadata
            .get("approval_gate_id")
            .and_then(|value| value.as_str())
            .map(str::to_owned)
    });
    let approved = harness
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
                    "approval_gate_id": approval_gate_id,
                    "blocked_call_id": call.call_id.clone(),
                }),
            },
        )
        .await
        .expect("approval should resume the blocked capability");
    assert_eq!(approved.status, ControlSignalStatus::Applied);
    assert_eq!(
        harness
            .manager
            .get_task(task_id)
            .await
            .expect("task should load after approval")
            .status,
        TaskStatus::Running
    );

    let calls = harness
        .manager
        .list_capability_calls_by_task(task_id)
        .await
        .expect("call records should load");
    let completed_call = calls
        .into_iter()
        .find(|candidate| candidate.call_id == call.call_id)
        .expect("blocked call should still exist");
    assert_eq!(
        completed_call.status,
        agentos_mrr::CapabilityCallStatus::Completed
    );
    assert_eq!(completed_call.outcome, Some(CapabilityOutcome::Succeeded));
    assert_eq!(
        completed_call.side_effect_summary,
        Some(SideEffectSummary::Committed)
    );
    assert_eq!(completed_call.retry_safety, Some(RetrySafety::Unsafe));
    assert!(completed_call
        .result_payload
        .as_ref()
        .expect("payload")
        .is_object());

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let significant_event_types: Vec<_> = events
        .iter()
        .filter(|event| event.event_type != "checkpoint.created")
        .map(|event| event.event_type.as_str())
        .collect();
    assert_eq!(
        significant_event_types,
        vec![
            "task.created",
            "task.ready",
            "task.started",
            "capability.requested",
            "policy.evaluation.requested",
            "policy.evaluation.completed",
            "policy.result.require_approval",
            "capability.policy_blocked",
            "task.awaiting_control",
            "control.signal.received",
            "task.control.approved",
            "control.signal.applied",
            "capability.dispatched",
            "capability.result.succeeded",
        ]
    );
    let applied_index = significant_event_types
        .iter()
        .position(|event_type| *event_type == "control.signal.applied")
        .expect("control.signal.applied should exist");
    let dispatched_index = significant_event_types
        .iter()
        .position(|event_type| *event_type == "capability.dispatched")
        .expect("capability.dispatched should exist");
    assert!(applied_index < dispatched_index);
}

#[tokio::test]
async fn cts_22_denied_approval_does_not_dispatch_blocked_capability() {
    let harness = setup_harness().await;
    let approver_principal_id = Uuid::from_u128(2);
    create_active_root_principal(&harness, approver_principal_id, "Approver").await;
    let task_id = create_running_task(&harness, "capability-side-effect-denied").await;

    harness
        .manager
        .register_capability(side_effect_capability_registration())
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.side_effect",
        "1.0.0",
        Arc::new(SideEffectHandler),
    );

    let call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new(
                "cap.side_effect",
                json!({"target": "tenant://workspace/output.txt"}),
            ),
        )
        .await
        .expect("capability invoke should record a blocked call");

    let approval_gate_id = call.provider_metadata.as_ref().and_then(|metadata| {
        metadata
            .get("approval_gate_id")
            .and_then(|value| value.as_str())
            .map(str::to_owned)
    });
    let denied = harness
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
                    "approval_gate_id": approval_gate_id,
                    "blocked_call_id": call.call_id.clone(),
                }),
            },
        )
        .await
        .expect("deny should finalize the blocked capability");
    assert_eq!(denied.status, ControlSignalStatus::Applied);
    assert_eq!(
        harness
            .manager
            .get_task(task_id)
            .await
            .expect("task should load after deny")
            .status,
        TaskStatus::Failed
    );

    let calls = harness
        .manager
        .list_capability_calls_by_task(task_id)
        .await
        .expect("call records should load");
    let denied_call = calls
        .into_iter()
        .find(|candidate| candidate.call_id == call.call_id)
        .expect("blocked call should still exist");
    assert_eq!(
        denied_call.status,
        agentos_mrr::CapabilityCallStatus::Completed
    );
    assert_eq!(denied_call.outcome, Some(CapabilityOutcome::Denied));
    assert_eq!(denied_call.reason_code.as_deref(), Some("approval_denied"));
    assert_eq!(denied_call.dispatched_at, None);

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
            "capability.requested",
            "policy.evaluation.requested",
            "policy.evaluation.completed",
            "policy.result.require_approval",
            "capability.policy_blocked",
            "task.awaiting_control",
            "control.signal.received",
            "task.failed",
            "control.signal.applied",
            "capability.result.denied",
        ]
    );
    assert!(!event_types.contains(&"capability.dispatched"));
}

#[tokio::test]
async fn cts_22_approval_can_be_resolved_by_blocked_call_id_only() {
    let harness = setup_harness().await;
    let approver_principal_id = Uuid::from_u128(2);
    create_active_root_principal(&harness, approver_principal_id, "Approver").await;
    let task_id = create_running_task(&harness, "capability-approve-by-call-id").await;

    harness
        .manager
        .register_capability(side_effect_capability_registration())
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.side_effect",
        "1.0.0",
        Arc::new(SideEffectHandler),
    );

    let call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new(
                "cap.side_effect",
                json!({"target": "tenant://workspace/output.txt"}),
            ),
        )
        .await
        .expect("capability invoke should record a blocked call");

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
                    "blocked_call_id": call.call_id.clone(),
                }),
            },
        )
        .await
        .expect("approval by blocked call id should be accepted");
    assert_eq!(signal.status, ControlSignalStatus::Applied);

    let calls = harness
        .manager
        .list_capability_calls_by_task(task_id)
        .await
        .expect("call records should load");
    let completed_call = calls
        .into_iter()
        .find(|candidate| candidate.call_id == call.call_id)
        .expect("blocked call should still exist");
    assert_eq!(
        completed_call.status,
        agentos_mrr::CapabilityCallStatus::Completed
    );
    assert_eq!(completed_call.outcome, Some(CapabilityOutcome::Succeeded));
}

#[tokio::test]
async fn cts_22_self_approval_is_rejected_for_blocked_capability_calls() {
    let harness = setup_harness().await;
    let task_id = create_running_task(&harness, "capability-self-approval").await;

    harness
        .manager
        .register_capability(side_effect_capability_registration())
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.side_effect",
        "1.0.0",
        Arc::new(SideEffectHandler),
    );

    let call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new(
                "cap.side_effect",
                json!({"target": "tenant://workspace/output.txt"}),
            ),
        )
        .await
        .expect("capability invoke should record a blocked call");

    let approval_gate_id = call.provider_metadata.as_ref().and_then(|metadata| {
        metadata
            .get("approval_gate_id")
            .and_then(|value| value.as_str())
            .map(str::to_owned)
    });
    let signal = harness
        .manager
        .submit_control_signal(
            task_id,
            actor(harness.owner_principal_id),
            SubmitControlSignalCommand {
                signal_type: ControlSignalType::Approve,
                payload: json!({
                    "approval_gate_id": approval_gate_id,
                    "blocked_call_id": call.call_id.clone(),
                }),
            },
        )
        .await
        .expect("self-approval attempt should be audited and rejected");
    assert_eq!(signal.status, ControlSignalStatus::Rejected);

    let task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should remain queryable");
    assert_eq!(task.status, TaskStatus::WaitingOnControl);

    let calls = harness
        .manager
        .list_capability_calls_by_task(task_id)
        .await
        .expect("call records should load");
    let blocked_call = calls
        .into_iter()
        .find(|candidate| candidate.call_id == call.call_id)
        .expect("blocked call should still exist");
    assert_eq!(
        blocked_call.status,
        agentos_mrr::CapabilityCallStatus::PolicyBlocked
    );
    assert_eq!(blocked_call.outcome, None);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    assert_eq!(
        events
            .last()
            .expect("rejection event should exist")
            .event_type,
        "control.signal.rejected"
    );
    assert_eq!(
        events.last().expect("rejection event should exist").payload["reason_code"],
        "approval_requires_distinct_principal"
    );
    assert!(!events
        .iter()
        .any(|event| event.event_type == "capability.dispatched"));
}

#[tokio::test]
async fn cts_22_resume_revalidates_blocked_call_preconditions_before_dispatch() {
    let harness = setup_harness().await;
    let approver_principal_id = Uuid::from_u128(2);
    create_active_root_principal(&harness, approver_principal_id, "Approver").await;
    let task_id = create_running_task(&harness, "capability-revalidate-after-approval").await;

    harness
        .manager
        .register_capability(side_effect_capability_registration())
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.side_effect",
        "1.0.0",
        Arc::new(SideEffectHandler),
    );

    let call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new(
                "cap.side_effect",
                json!({"target": "tenant://workspace/output.txt"}),
            ),
        )
        .await
        .expect("capability invoke should record a blocked call");

    harness
        .manager
        .principal_store()
        .upsert(&Principal {
            principal_id: harness.owner_principal_id,
            principal_type: "human".to_owned(),
            display_name: "Kernel Owner".to_owned(),
            authority_scope_ref: "authority/root".to_owned(),
            status: PrincipalStatus::Suspended,
            created_at: OffsetDateTime::from_unix_timestamp(1_700_000_000)
                .expect("valid timestamp"),
        })
        .await
        .expect("principal status should update");

    let approval_gate_id = call.provider_metadata.as_ref().and_then(|metadata| {
        metadata
            .get("approval_gate_id")
            .and_then(|value| value.as_str())
            .map(str::to_owned)
    });
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
                    "approval_gate_id": approval_gate_id,
                    "blocked_call_id": call.call_id.clone(),
                }),
            },
        )
        .await
        .expect("approval should still be auditable");
    assert_eq!(signal.status, ControlSignalStatus::Applied);

    let task = harness
        .manager
        .get_task(task_id)
        .await
        .expect("task should load after approval resolution");
    assert_eq!(task.status, TaskStatus::Running);

    let calls = harness
        .manager
        .list_capability_calls_by_task(task_id)
        .await
        .expect("call records should load");
    let completed_call = calls
        .into_iter()
        .find(|candidate| candidate.call_id == call.call_id)
        .expect("blocked call should still exist");
    assert_eq!(
        completed_call.status,
        agentos_mrr::CapabilityCallStatus::Completed
    );
    assert_eq!(completed_call.outcome, Some(CapabilityOutcome::Denied));
    assert_eq!(
        completed_call.reason_code.as_deref(),
        Some("principal_not_active")
    );
    assert_eq!(completed_call.dispatched_at, None);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    assert!(events
        .iter()
        .any(|event| event.event_type == "task.control.approved"));
    assert!(events
        .iter()
        .any(|event| event.event_type == "capability.result.denied"));
    assert!(!events
        .iter()
        .any(|event| event.event_type == "capability.dispatched"));
}

#[tokio::test]
async fn cts_18_approval_linkage_survives_non_object_provider_metadata_overlay() {
    let harness = setup_harness().await;
    let approver_principal_id = Uuid::from_u128(2);
    create_active_root_principal(&harness, approver_principal_id, "Approver").await;
    let task_id = create_running_task(&harness, "capability-approval-metadata-overlay").await;

    harness
        .manager
        .register_capability(side_effect_capability_registration())
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.side_effect",
        "1.0.0",
        Arc::new(InvalidOutputSideEffectHandler),
    );

    let call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new(
                "cap.side_effect",
                json!({"target": "tenant://workspace/output.txt"}),
            ),
        )
        .await
        .expect("capability invoke should record a blocked call");

    let approval_gate_id = call.provider_metadata.as_ref().and_then(|metadata| {
        metadata
            .get("approval_gate_id")
            .and_then(|value| value.as_str())
            .map(str::to_owned)
    });
    harness
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
                    "approval_gate_id": approval_gate_id,
                    "blocked_call_id": call.call_id.clone(),
                }),
            },
        )
        .await
        .expect("approval should resume the blocked capability");

    let calls = harness
        .manager
        .list_capability_calls_by_task(task_id)
        .await
        .expect("call records should load");
    let completed_call = calls
        .into_iter()
        .find(|candidate| candidate.call_id == call.call_id)
        .expect("blocked call should still exist");

    assert_eq!(completed_call.outcome, Some(CapabilityOutcome::Failed));
    assert_eq!(
        completed_call.reason_code.as_deref(),
        Some("output_validation_failed")
    );
    assert_eq!(
        completed_call
            .provider_metadata
            .as_ref()
            .and_then(|metadata| metadata
                .get("approval_gate_id")
                .and_then(|value| value.as_str())),
        approval_gate_id.as_deref()
    );
    assert_eq!(
        completed_call
            .provider_metadata
            .as_ref()
            .and_then(|metadata| metadata
                .get("blocked_call_id")
                .and_then(|value| value.as_str())),
        Some(call.call_id.as_str())
    );
    assert_eq!(
        completed_call
            .provider_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("provider_overlay")),
        Some(&json!("opaque-provider-metadata"))
    );
}

#[tokio::test]
async fn cts_18_side_effect_provider_errors_are_not_marked_safe_to_retry() {
    let harness = setup_harness().await;
    let task_id = create_running_task(&harness, "capability-side-effect-provider-error").await;

    harness
        .manager
        .register_capability(mixed_effect_capability_registration())
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.mixed_effect",
        "1.0.0",
        Arc::new(FailingSideEffectHandler),
    );

    let call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new(
                "cap.mixed_effect",
                json!({"target": "tenant://workspace/output.txt"}),
            ),
        )
        .await
        .expect("capability invoke should produce an auditable failure");

    assert_eq!(call.outcome, Some(CapabilityOutcome::Failed));
    assert_eq!(call.reason_code.as_deref(), Some("provider_error"));
    assert_eq!(call.side_effect_summary, Some(SideEffectSummary::Unknown));
    assert_eq!(
        call.retry_safety,
        Some(RetrySafety::RequiresOperatorDecision)
    );
}

#[tokio::test]
async fn cts_18_side_effect_output_validation_failures_remain_operator_gated() {
    let harness = setup_harness().await;
    let task_id = create_running_task(&harness, "capability-side-effect-invalid-output").await;

    harness
        .manager
        .register_capability(mixed_effect_capability_registration())
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.mixed_effect",
        "1.0.0",
        Arc::new(InvalidOutputSideEffectHandler),
    );

    let call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new(
                "cap.mixed_effect",
                json!({"target": "tenant://workspace/output.txt"}),
            ),
        )
        .await
        .expect("capability invoke should produce an auditable failure");

    assert_eq!(call.outcome, Some(CapabilityOutcome::Failed));
    assert_eq!(
        call.reason_code.as_deref(),
        Some("output_validation_failed")
    );
    assert_eq!(call.side_effect_summary, Some(SideEffectSummary::Committed));
    assert_eq!(call.retry_safety, Some(RetrySafety::Unsafe));
}

#[tokio::test]
async fn cts_22_call_id_only_approval_keeps_approval_linkage_with_non_object_provider_metadata() {
    let harness = setup_harness().await;
    let approver_principal_id = Uuid::from_u128(2);
    create_active_root_principal(&harness, approver_principal_id, "Approver").await;
    let task_id = create_running_task(&harness, "capability-call-id-only-approval").await;

    harness
        .manager
        .register_capability(side_effect_capability_registration())
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.side_effect",
        "1.0.0",
        Arc::new(InvalidOutputSideEffectHandler),
    );

    let call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new(
                "cap.side_effect",
                json!({"target": "tenant://workspace/output.txt"}),
            ),
        )
        .await
        .expect("capability invoke should record a blocked call");

    let signal = harness
        .manager
        .submit_control_signal(
            task_id,
            approving_actor(approver_principal_id),
            SubmitControlSignalCommand {
                signal_type: ControlSignalType::Approve,
                payload: json!({
                    "call_id": call.call_id.clone(),
                }),
            },
        )
        .await
        .expect("call_id should be sufficient to approve blocked call");
    assert_eq!(signal.status, ControlSignalStatus::Applied);

    let completed_call = harness
        .manager
        .list_capability_calls_by_task(task_id)
        .await
        .expect("call records should load")
        .into_iter()
        .find(|candidate| candidate.call_id == call.call_id)
        .expect("completed call should exist");
    assert_eq!(completed_call.outcome, Some(CapabilityOutcome::Failed));
    assert_eq!(
        completed_call.reason_code.as_deref(),
        Some("output_validation_failed")
    );
    assert_eq!(
        completed_call.side_effect_summary,
        Some(SideEffectSummary::Committed)
    );
    assert_eq!(completed_call.retry_safety, Some(RetrySafety::Unsafe));
    let provider_metadata = completed_call
        .provider_metadata
        .expect("provider metadata should be preserved");
    assert_eq!(
        provider_metadata["approval_gate_id"],
        call.provider_metadata
            .as_ref()
            .expect("blocked call metadata should exist")["approval_gate_id"]
    );
    assert_eq!(
        provider_metadata["blocked_call_id"],
        json!(call.call_id.clone())
    );
    assert_eq!(
        provider_metadata["provider_overlay"],
        json!("opaque-provider-metadata")
    );
}

#[tokio::test]
async fn checkpoint_restore_rejects_stale_in_flight_capability_reference() {
    let harness = setup_harness().await;
    let approver_principal_id = Uuid::from_u128(2);
    create_active_root_principal(&harness, approver_principal_id, "Approver").await;
    let task_id = create_running_task(&harness, "capability-restore-stale-blocked-call").await;

    harness
        .manager
        .register_capability(side_effect_capability_registration())
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.side_effect",
        "1.0.0",
        Arc::new(SideEffectHandler),
    );

    let call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new(
                "cap.side_effect",
                json!({"target": "tenant://workspace/output.txt"}),
            ),
        )
        .await
        .expect("capability invoke should record a blocked call");

    let checkpoint = harness
        .manager
        .create_checkpoint(
            task_id,
            actor(harness.owner_principal_id),
            json!({"reason": "before-approval"}),
        )
        .await
        .expect("checkpoint should be created while approval is pending");

    let approved = harness
        .manager
        .submit_control_signal(
            task_id,
            approving_actor(approver_principal_id),
            SubmitControlSignalCommand {
                signal_type: ControlSignalType::Approve,
                payload: json!({
                    "blocked_call_id": call.call_id.clone(),
                }),
            },
        )
        .await
        .expect("approval should resume the blocked call");
    assert_eq!(approved.status, ControlSignalStatus::Applied);

    let error = harness
        .manager
        .restore_checkpoint(
            task_id,
            actor(harness.owner_principal_id),
            Some(checkpoint.checkpoint_id),
        )
        .await
        .expect_err("restore should reject stale blocked-call references");
    assert!(error.to_string().contains("no longer recoverable"));

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    assert!(events.iter().any(|event| {
        event.event_type == "checkpoint.restore.failed"
            && event.payload["reason_code"] == "checkpoint_in_flight_capability_not_recoverable"
    }));
}

#[tokio::test]
async fn capability_versions_are_durable_contracts() {
    let harness = setup_harness().await;

    let registration = echo_capability_registration();
    harness
        .manager
        .register_capability(registration.clone())
        .await
        .expect("initial registration should succeed");

    let mut mutated = registration.clone();
    mutated.output_schema = json!({
        "type": "object",
        "required": ["different"],
        "properties": {
            "different": {"type": "string"}
        },
        "additionalProperties": false
    });

    let error = harness
        .manager
        .register_capability(mutated)
        .await
        .expect_err("re-registering the same version with a different contract should fail");
    assert!(error.to_string().contains("durable contract"));

    let mut status_only = registration;
    status_only.status = agentos_mrr::CapabilityStatus::Revoked;
    let updated = harness
        .manager
        .register_capability(status_only)
        .await
        .expect("status-only updates should still be allowed");
    assert_eq!(updated.status, agentos_mrr::CapabilityStatus::Revoked);
}

#[tokio::test]
async fn cts_13_allow_with_constraints_are_bound_to_dispatch_and_result() {
    let harness = setup_harness().await;
    let budget_context_ref = inline_budget_context(
        100.0,
        Some(40.0),
        55.0,
        json!({
            "max_cost_class": "low",
        }),
    );
    let task_id = create_running_task_with_budget(
        &harness,
        "capability-allow-with-constraints",
        &budget_context_ref,
    )
    .await;

    harness
        .manager
        .register_capability(constraint_echo_capability_registration(CostClass::Low))
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.echo",
        "1.0.0",
        Arc::new(ConstraintAwareEchoHandler),
    );

    let call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new("cap.echo", json!({"message": "hello"})),
        )
        .await
        .expect("constrained capability invoke should succeed");

    assert_eq!(call.outcome, Some(CapabilityOutcome::Succeeded));
    assert_eq!(
        call.result_payload.as_ref().expect("payload")["constraint_count"],
        1
    );
    assert_eq!(
        call.provider_metadata.as_ref().expect("metadata")["applied_constraints"][0]["kind"],
        "max_cost_class"
    );

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    let policy_result = events
        .iter()
        .find(|event| event.event_type == "policy.result.allow_with_constraints")
        .expect("allow_with_constraints event should exist");
    assert_eq!(
        policy_result.payload["applied_constraints"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let dispatched = events
        .iter()
        .find(|event| event.event_type == "capability.dispatched")
        .expect("dispatch event should exist");
    assert_eq!(
        dispatched.payload["applied_constraints"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let result = events
        .iter()
        .find(|event| event.event_type == "capability.result.succeeded")
        .expect("result event should exist");
    assert_eq!(
        result.payload["provider_metadata"]["applied_constraints"][0]["kind"],
        "max_cost_class"
    );
}

#[tokio::test]
async fn cts_13_constraint_violation_is_denied_before_dispatch() {
    let harness = setup_harness().await;
    let budget_context_ref = inline_budget_context(
        100.0,
        Some(40.0),
        55.0,
        json!({
            "max_cost_class": "low",
        }),
    );
    let task_id = create_running_task_with_budget(
        &harness,
        "capability-constraint-violation",
        &budget_context_ref,
    )
    .await;

    harness
        .manager
        .register_capability(constraint_echo_capability_registration(CostClass::Moderate))
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.echo",
        "1.0.0",
        Arc::new(PanicHandler),
    );

    let call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new("cap.echo", json!({"message": "hello"})),
        )
        .await
        .expect("constraint violation should still return a denied call");

    assert_eq!(call.outcome, Some(CapabilityOutcome::Denied));
    assert_eq!(
        call.reason_code.as_deref(),
        Some("budget_cost_class_disallowed")
    );
    assert_eq!(call.dispatched_at, None);
    assert_eq!(
        call.provider_metadata.as_ref().expect("metadata")["applied_constraints"][0]["kind"],
        "max_cost_class"
    );

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    assert!(events
        .iter()
        .any(|event| event.event_type == "policy.result.allow_with_constraints"));
    assert!(!events
        .iter()
        .any(|event| event.event_type == "capability.dispatched"));
}

#[tokio::test]
async fn budget_disqualification_denies_dispatch_precondition() {
    let harness = setup_harness().await;
    let budget_context_ref = inline_budget_context(100.0, None, 100.0, json!({}));
    let task_id = create_running_task_with_budget(
        &harness,
        "capability-budget-disqualification",
        &budget_context_ref,
    )
    .await;

    harness
        .manager
        .register_capability(echo_capability_registration())
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.echo",
        "1.0.0",
        Arc::new(PanicHandler),
    );

    let call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new("cap.echo", json!({"message": "hello"})),
        )
        .await
        .expect("budget disqualification should produce a denied call");

    assert_eq!(call.outcome, Some(CapabilityOutcome::Denied));
    assert_eq!(call.reason_code.as_deref(), Some("budget_disqualified"));
    assert_eq!(call.dispatched_at, None);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    assert!(events
        .iter()
        .any(|event| event.event_type == "policy.result.deny"
            && event.payload["reason_code"] == "budget_disqualified"));
    assert!(!events
        .iter()
        .any(|event| event.event_type == "capability.dispatched"));
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_safe_dispatch_persists_dispatch_before_provider_panics() {
    let (harness, _tempdir, database_url) = setup_file_harness().await;
    let approver_principal_id = Uuid::from_u128(2);
    create_active_root_principal(&harness, approver_principal_id, "Approver").await;
    let task_id = create_running_task(&harness, "capability-durable-dispatch-boundary").await;

    harness
        .manager
        .register_capability(side_effect_capability_registration())
        .await
        .expect("capability should register");
    harness.manager.capability_dispatcher().register_handler(
        "cap.side_effect",
        "1.0.0",
        Arc::new(DurableDispatchObserverPanicHandler { database_url }),
    );

    let blocked_call = harness
        .manager
        .invoke_capability(
            task_id,
            actor(harness.owner_principal_id),
            InvokeCapabilityCommand::new(
                "cap.side_effect",
                json!({"target": "tenant://workspace/output.txt"}),
            ),
        )
        .await
        .expect("side effect capability should block for approval");
    assert_eq!(
        blocked_call.status,
        agentos_mrr::CapabilityCallStatus::PolicyBlocked
    );

    let manager = harness.manager.clone();
    let blocked_call_id = blocked_call.call_id.clone();
    let approval = tokio::spawn(async move {
        manager
            .submit_control_signal(
                task_id,
                approving_actor(approver_principal_id),
                SubmitControlSignalCommand {
                    signal_type: ControlSignalType::Approve,
                    payload: json!({
                        "blocked_call_id": blocked_call_id,
                    }),
                },
            )
            .await
    });
    let join_error = approval
        .await
        .expect_err("provider panic should surface through the resumed dispatch");
    assert!(join_error.is_panic());

    let call = harness
        .manager
        .list_capability_calls_by_task(task_id)
        .await
        .expect("call records should load")
        .into_iter()
        .find(|candidate| candidate.call_id == blocked_call.call_id)
        .expect("approved call should still exist");
    assert_eq!(call.status, agentos_mrr::CapabilityCallStatus::Dispatched);
    assert_eq!(call.outcome, None);
    assert!(call.dispatched_at.is_some());
    assert_eq!(call.completed_at, None);

    let events = harness
        .manager
        .list_events_by_task(task_id)
        .await
        .expect("events should load");
    assert!(events.iter().any(|event| {
        event.event_type == "capability.dispatched"
            && event.payload["call_id"] == blocked_call.call_id
    }));
    assert!(!events.iter().any(|event| {
        event.event_type.starts_with("capability.result.")
            && event.payload["call_id"] == blocked_call.call_id
    }));
}
