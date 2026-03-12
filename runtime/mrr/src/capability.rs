use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{FromRow, Sqlite, SqlitePool, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::db::{decode_timestamp, encode_timestamp, parse_optional_uuid, parse_uuid};
use crate::error::{Result, RuntimeError};
use crate::principal::PrincipalAttribution;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityStatus {
    Active,
    Disabled,
    Deprecated,
    Revoked,
}

impl CapabilityStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Disabled => "disabled",
            Self::Deprecated => "deprecated",
            Self::Revoked => "revoked",
        }
    }

    pub fn accepts_invocation(&self) -> bool {
        !matches!(self, Self::Disabled | Self::Revoked)
    }
}

impl std::str::FromStr for CapabilityStatus {
    type Err = RuntimeError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "active" => Ok(Self::Active),
            "disabled" => Ok(Self::Disabled),
            "deprecated" => Ok(Self::Deprecated),
            "revoked" => Ok(Self::Revoked),
            _ => Err(RuntimeError::InvalidEnumValue {
                kind: "capability_status",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EffectClass {
    ReadOnly,
    StateMutatingInternal,
    ExternalSideEffect,
    AuthorityMediated,
    Mixed,
}

impl EffectClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::StateMutatingInternal => "state_mutating_internal",
            Self::ExternalSideEffect => "external_side_effect",
            Self::AuthorityMediated => "authority_mediated",
            Self::Mixed => "mixed",
        }
    }
}

impl std::str::FromStr for EffectClass {
    type Err = RuntimeError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "read_only" => Ok(Self::ReadOnly),
            "state_mutating_internal" => Ok(Self::StateMutatingInternal),
            "external_side_effect" => Ok(Self::ExternalSideEffect),
            "authority_mediated" => Ok(Self::AuthorityMediated),
            "mixed" => Ok(Self::Mixed),
            _ => Err(RuntimeError::InvalidEnumValue {
                kind: "effect_class",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyClass {
    Idempotent,
    ConditionallyIdempotent,
    NonIdempotent,
    Unknown,
}

impl IdempotencyClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Idempotent => "idempotent",
            Self::ConditionallyIdempotent => "conditionally_idempotent",
            Self::NonIdempotent => "non_idempotent",
            Self::Unknown => "unknown",
        }
    }
}

impl std::str::FromStr for IdempotencyClass {
    type Err = RuntimeError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "idempotent" => Ok(Self::Idempotent),
            "conditionally_idempotent" => Ok(Self::ConditionallyIdempotent),
            "non_idempotent" => Ok(Self::NonIdempotent),
            "unknown" => Ok(Self::Unknown),
            _ => Err(RuntimeError::InvalidEnumValue {
                kind: "idempotency_class",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimeoutClass {
    Short,
    Medium,
    Long,
    OperatorDefined,
}

impl TimeoutClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Short => "short",
            Self::Medium => "medium",
            Self::Long => "long",
            Self::OperatorDefined => "operator_defined",
        }
    }
}

impl std::str::FromStr for TimeoutClass {
    type Err = RuntimeError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "short" => Ok(Self::Short),
            "medium" => Ok(Self::Medium),
            "long" => Ok(Self::Long),
            "operator_defined" => Ok(Self::OperatorDefined),
            _ => Err(RuntimeError::InvalidEnumValue {
                kind: "timeout_class",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CostClass {
    Low,
    Moderate,
    High,
    Variable,
}

impl CostClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Moderate => "moderate",
            Self::High => "high",
            Self::Variable => "variable",
        }
    }
}

impl std::str::FromStr for CostClass {
    type Err = RuntimeError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "low" => Ok(Self::Low),
            "moderate" => Ok(Self::Moderate),
            "high" => Ok(Self::High),
            "variable" => Ok(Self::Variable),
            _ => Err(RuntimeError::InvalidEnumValue {
                kind: "cost_class",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TrustLevel {
    KernelTrusted,
    RuntimeTrusted,
    Sandboxed,
    ExternalUntrusted,
    HumanAuthority,
}

impl TrustLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::KernelTrusted => "kernel_trusted",
            Self::RuntimeTrusted => "runtime_trusted",
            Self::Sandboxed => "sandboxed",
            Self::ExternalUntrusted => "external_untrusted",
            Self::HumanAuthority => "human_authority",
        }
    }
}

impl std::str::FromStr for TrustLevel {
    type Err = RuntimeError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "kernel_trusted" => Ok(Self::KernelTrusted),
            "runtime_trusted" => Ok(Self::RuntimeTrusted),
            "sandboxed" => Ok(Self::Sandboxed),
            "external_untrusted" => Ok(Self::ExternalUntrusted),
            "human_authority" => Ok(Self::HumanAuthority),
            _ => Err(RuntimeError::InvalidEnumValue {
                kind: "trust_level",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityOutcome {
    Succeeded,
    Failed,
    Denied,
    TimedOut,
    Cancelled,
}

impl CapabilityOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Denied => "denied",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn result_event_type(&self) -> &'static str {
        match self {
            Self::Succeeded => "capability.result.succeeded",
            Self::Failed => "capability.result.failed",
            Self::Denied => "capability.result.denied",
            Self::TimedOut => "capability.result.timed_out",
            Self::Cancelled => "capability.result.cancelled",
        }
    }
}

impl std::str::FromStr for CapabilityOutcome {
    type Err = RuntimeError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "denied" => Ok(Self::Denied),
            "timed_out" => Ok(Self::TimedOut),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(RuntimeError::InvalidEnumValue {
                kind: "capability_outcome",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SideEffectSummary {
    NotAttempted,
    AttemptedButNotCommitted,
    Committed,
    CommittedWithUnknownFinality,
    Unknown,
}

impl SideEffectSummary {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NotAttempted => "not_attempted",
            Self::AttemptedButNotCommitted => "attempted_but_not_committed",
            Self::Committed => "committed",
            Self::CommittedWithUnknownFinality => "committed_with_unknown_finality",
            Self::Unknown => "unknown",
        }
    }
}

impl std::str::FromStr for SideEffectSummary {
    type Err = RuntimeError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "not_attempted" => Ok(Self::NotAttempted),
            "attempted_but_not_committed" => Ok(Self::AttemptedButNotCommitted),
            "committed" => Ok(Self::Committed),
            "committed_with_unknown_finality" => Ok(Self::CommittedWithUnknownFinality),
            "unknown" => Ok(Self::Unknown),
            _ => Err(RuntimeError::InvalidEnumValue {
                kind: "side_effect_summary",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RetrySafety {
    Safe,
    Unsafe,
    RequiresOperatorDecision,
    Unknown,
}

impl RetrySafety {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Safe => "safe",
            Self::Unsafe => "unsafe",
            Self::RequiresOperatorDecision => "requires_operator_decision",
            Self::Unknown => "unknown",
        }
    }
}

impl std::str::FromStr for RetrySafety {
    type Err = RuntimeError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "safe" => Ok(Self::Safe),
            "unsafe" => Ok(Self::Unsafe),
            "requires_operator_decision" => Ok(Self::RequiresOperatorDecision),
            "unknown" => Ok(Self::Unknown),
            _ => Err(RuntimeError::InvalidEnumValue {
                kind: "retry_safety",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityCallStatus {
    Requested,
    PolicyBlocked,
    Dispatched,
    Completed,
}

impl CapabilityCallStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::PolicyBlocked => "policy_blocked",
            Self::Dispatched => "dispatched",
            Self::Completed => "completed",
        }
    }
}

impl std::str::FromStr for CapabilityCallStatus {
    type Err = RuntimeError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "requested" => Ok(Self::Requested),
            "policy_blocked" => Ok(Self::PolicyBlocked),
            "dispatched" => Ok(Self::Dispatched),
            "completed" => Ok(Self::Completed),
            _ => Err(RuntimeError::InvalidEnumValue {
                kind: "capability_call_status",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapabilityContract {
    pub capability_id: String,
    pub capability_name: String,
    pub capability_version: String,
    pub contract_version: String,
    pub description: String,
    pub provider_id: String,
    pub status: CapabilityStatus,
    pub input_schema: Value,
    pub output_schema: Value,
    pub effect_class: EffectClass,
    pub idempotency_class: IdempotencyClass,
    pub timeout_class: TimeoutClass,
    pub cost_class: CostClass,
    pub trust_level: TrustLevel,
    pub required_authority_scope: String,
    pub observability_class: String,
    pub streaming_support: Option<bool>,
    pub supports_cancellation: Option<bool>,
    pub supports_partial_result: Option<bool>,
    pub supports_checkpoint_safe_resume: Option<bool>,
    pub side_effect_surface: Option<Value>,
    pub trust_zone_surface: Option<Value>,
    pub external_dependencies: Option<Value>,
    pub sandbox_profile: Option<Value>,
    pub evidence_profile: Option<Value>,
    pub rate_limit_class: Option<String>,
    pub tenant_restrictions: Option<Value>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

impl CapabilityContract {
    pub fn validate_registration(&self) -> Result<()> {
        if self.capability_id.trim().is_empty() {
            return Err(RuntimeError::CapabilitySchemaValidation {
                context: "registration",
                message: "capability_id must not be empty".to_owned(),
            });
        }
        if self.capability_name.trim().is_empty() {
            return Err(RuntimeError::CapabilitySchemaValidation {
                context: "registration",
                message: "capability_name must not be empty".to_owned(),
            });
        }
        if self.capability_version.trim().is_empty() {
            return Err(RuntimeError::CapabilitySchemaValidation {
                context: "registration",
                message: "capability_version must not be empty".to_owned(),
            });
        }
        if self.contract_version.trim().is_empty() {
            return Err(RuntimeError::CapabilitySchemaValidation {
                context: "registration",
                message: "contract_version must not be empty".to_owned(),
            });
        }
        if self.provider_id.trim().is_empty() {
            return Err(RuntimeError::CapabilitySchemaValidation {
                context: "registration",
                message: "provider_id must not be empty".to_owned(),
            });
        }
        if self.required_authority_scope.trim().is_empty() {
            return Err(RuntimeError::CapabilitySchemaValidation {
                context: "registration",
                message: "required_authority_scope must not be empty".to_owned(),
            });
        }
        if self.observability_class.trim().is_empty() {
            return Err(RuntimeError::CapabilitySchemaValidation {
                context: "registration",
                message: "observability_class must not be empty".to_owned(),
            });
        }

        validate_schema_contract(&self.input_schema, "input_schema")?;
        validate_schema_contract(&self.output_schema, "output_schema")?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RegisterCapabilityCommand {
    pub capability_id: String,
    pub capability_name: String,
    pub capability_version: String,
    pub contract_version: String,
    pub description: String,
    pub provider_id: String,
    pub status: CapabilityStatus,
    pub input_schema: Value,
    pub output_schema: Value,
    pub effect_class: EffectClass,
    pub idempotency_class: IdempotencyClass,
    pub timeout_class: TimeoutClass,
    pub cost_class: CostClass,
    pub trust_level: TrustLevel,
    pub required_authority_scope: String,
    pub observability_class: String,
    pub streaming_support: Option<bool>,
    pub supports_cancellation: Option<bool>,
    pub supports_partial_result: Option<bool>,
    pub supports_checkpoint_safe_resume: Option<bool>,
    pub side_effect_surface: Option<Value>,
    pub trust_zone_surface: Option<Value>,
    pub external_dependencies: Option<Value>,
    pub sandbox_profile: Option<Value>,
    pub evidence_profile: Option<Value>,
    pub rate_limit_class: Option<String>,
    pub tenant_restrictions: Option<Value>,
}

impl RegisterCapabilityCommand {
    pub fn into_contract(self, now: OffsetDateTime) -> CapabilityContract {
        CapabilityContract {
            capability_id: self.capability_id,
            capability_name: self.capability_name,
            capability_version: self.capability_version,
            contract_version: self.contract_version,
            description: self.description,
            provider_id: self.provider_id,
            status: self.status,
            input_schema: self.input_schema,
            output_schema: self.output_schema,
            effect_class: self.effect_class,
            idempotency_class: self.idempotency_class,
            timeout_class: self.timeout_class,
            cost_class: self.cost_class,
            trust_level: self.trust_level,
            required_authority_scope: self.required_authority_scope,
            observability_class: self.observability_class,
            streaming_support: self.streaming_support,
            supports_cancellation: self.supports_cancellation,
            supports_partial_result: self.supports_partial_result,
            supports_checkpoint_safe_resume: self.supports_checkpoint_safe_resume,
            side_effect_surface: self.side_effect_surface,
            trust_zone_surface: self.trust_zone_surface,
            external_dependencies: self.external_dependencies,
            sandbox_profile: self.sandbox_profile,
            evidence_profile: self.evidence_profile,
            rate_limit_class: self.rate_limit_class,
            tenant_restrictions: self.tenant_restrictions,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapabilityCallRecord {
    pub call_id: String,
    pub task_id: Uuid,
    pub caller_principal_id: Uuid,
    pub caller_principal_role: String,
    pub capability_id: String,
    pub capability_version: Option<String>,
    pub input_payload: Value,
    #[serde(with = "time::serde::rfc3339")]
    pub requested_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub dispatched_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub completed_at: Option<OffsetDateTime>,
    pub policy_context_ref: String,
    pub budget_context_ref: String,
    pub effect_class: String,
    pub cost_class: Option<CostClass>,
    pub idempotency_expectation: IdempotencyClass,
    pub correlation_id: String,
    pub checkpoint_ref: Option<Uuid>,
    pub state_version_ref: Option<Uuid>,
    pub timeout_override_ms: Option<i64>,
    pub request_reason_code: Option<String>,
    pub status: CapabilityCallStatus,
    pub outcome: Option<CapabilityOutcome>,
    pub result_payload: Option<Value>,
    pub side_effect_summary: Option<SideEffectSummary>,
    pub retry_safety: Option<RetrySafety>,
    pub provider_metadata: Option<Value>,
    pub evidence_ref: Option<String>,
    pub latency_ms: Option<i64>,
    pub reason_code: Option<String>,
    pub requested_event_id: Uuid,
    pub requested_sequence_number: i64,
    pub dispatched_event_id: Option<Uuid>,
    pub dispatched_sequence_number: Option<i64>,
    pub outcome_event_id: Option<Uuid>,
    pub outcome_sequence_number: Option<i64>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct NewCapabilityCallRecord {
    pub call_id: String,
    pub task_id: Uuid,
    pub caller: PrincipalAttribution,
    pub capability_id: String,
    pub capability_version: Option<String>,
    pub input_payload: Value,
    pub requested_at: OffsetDateTime,
    pub policy_context_ref: String,
    pub budget_context_ref: String,
    pub effect_class: String,
    pub cost_class: Option<CostClass>,
    pub idempotency_expectation: IdempotencyClass,
    pub correlation_id: String,
    pub checkpoint_ref: Option<Uuid>,
    pub state_version_ref: Option<Uuid>,
    pub timeout_override_ms: Option<i64>,
    pub request_reason_code: Option<String>,
    pub requested_event_id: Uuid,
    pub requested_sequence_number: i64,
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct InvokeCapabilityCommand {
    pub call_id: Option<String>,
    pub capability_id: String,
    pub capability_version: Option<String>,
    pub input_payload: Value,
    pub idempotency_expectation: Option<IdempotencyClass>,
    pub correlation_id: Option<String>,
    pub checkpoint_ref: Option<Uuid>,
    pub state_version_ref: Option<Uuid>,
    pub timeout_override_ms: Option<i64>,
    pub reason_code: Option<String>,
    pub trace_ref: Option<String>,
}

impl InvokeCapabilityCommand {
    pub fn new(capability_id: impl Into<String>, input_payload: Value) -> Self {
        Self {
            call_id: None,
            capability_id: capability_id.into(),
            capability_version: None,
            input_payload,
            idempotency_expectation: None,
            correlation_id: None,
            checkpoint_ref: None,
            state_version_ref: None,
            timeout_override_ms: None,
            reason_code: None,
            trace_ref: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CapabilityInvocationContext {
    pub call_id: String,
    pub task_id: Uuid,
    pub caller: PrincipalAttribution,
    pub capability_id: String,
    pub capability_version: String,
    pub input_payload: Value,
    pub policy_context_ref: String,
    pub budget_context_ref: String,
    pub effect_class: EffectClass,
    pub cost_class: CostClass,
    pub idempotency_expectation: IdempotencyClass,
    pub checkpoint_ref: Option<Uuid>,
    pub state_version_ref: Option<Uuid>,
    pub correlation_id: String,
    pub timeout_override_ms: Option<i64>,
    pub applied_constraints: Vec<Value>,
    pub reason_code: Option<String>,
    pub trace_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapabilityExecutionResult {
    pub outcome: CapabilityOutcome,
    pub result_payload: Value,
    pub side_effect_summary: SideEffectSummary,
    pub retry_safety: RetrySafety,
    #[serde(with = "time::serde::rfc3339::option")]
    pub completed_at: Option<OffsetDateTime>,
    pub evidence_ref: Option<String>,
    pub latency_ms: Option<i64>,
    pub provider_metadata: Option<Value>,
    pub reason_code: Option<String>,
}

impl CapabilityExecutionResult {
    pub fn succeeded(result_payload: Value) -> Self {
        Self {
            outcome: CapabilityOutcome::Succeeded,
            result_payload,
            side_effect_summary: SideEffectSummary::NotAttempted,
            retry_safety: RetrySafety::Safe,
            completed_at: None,
            evidence_ref: None,
            latency_ms: None,
            provider_metadata: None,
            reason_code: None,
        }
    }

    pub fn failed(reason_code: impl Into<String>, result_payload: Value) -> Self {
        Self::failed_with_semantics(
            reason_code,
            result_payload,
            SideEffectSummary::NotAttempted,
            RetrySafety::Safe,
        )
    }

    pub fn failed_with_semantics(
        reason_code: impl Into<String>,
        result_payload: Value,
        side_effect_summary: SideEffectSummary,
        retry_safety: RetrySafety,
    ) -> Self {
        Self {
            outcome: CapabilityOutcome::Failed,
            result_payload,
            side_effect_summary,
            retry_safety,
            completed_at: None,
            evidence_ref: None,
            latency_ms: None,
            provider_metadata: None,
            reason_code: Some(reason_code.into()),
        }
    }

    pub fn denied(reason_code: impl Into<String>, result_payload: Value) -> Self {
        Self {
            outcome: CapabilityOutcome::Denied,
            result_payload,
            side_effect_summary: SideEffectSummary::NotAttempted,
            retry_safety: RetrySafety::RequiresOperatorDecision,
            completed_at: None,
            evidence_ref: None,
            latency_ms: None,
            provider_metadata: None,
            reason_code: Some(reason_code.into()),
        }
    }
}

pub trait CapabilityHandler: Send + Sync {
    fn invoke(&self, request: CapabilityInvocationContext) -> Result<CapabilityExecutionResult>;
}

#[derive(Clone, Default)]
pub struct CapabilityDispatcher {
    handlers: Arc<RwLock<HashMap<String, Arc<dyn CapabilityHandler>>>>,
}

impl CapabilityDispatcher {
    pub fn register_handler(
        &self,
        capability_id: impl Into<String>,
        capability_version: impl Into<String>,
        handler: Arc<dyn CapabilityHandler>,
    ) {
        let key = dispatch_key(&capability_id.into(), &capability_version.into());
        let mut handlers = self
            .handlers
            .write()
            .expect("capability dispatcher lock poisoned");
        handlers.insert(key, handler);
    }

    pub fn handler_for(
        &self,
        capability_id: &str,
        capability_version: &str,
    ) -> Option<Arc<dyn CapabilityHandler>> {
        let handlers = self
            .handlers
            .read()
            .expect("capability dispatcher lock poisoned");
        handlers
            .get(&dispatch_key(capability_id, capability_version))
            .cloned()
    }
}

#[derive(Clone)]
pub struct CapabilityRegistry {
    pool: SqlitePool,
}

impl CapabilityRegistry {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn register(&self, contract: CapabilityContract) -> Result<CapabilityContract> {
        contract.validate_registration()?;
        let mut tx = self.pool.begin().await?;
        self.upsert_in_tx(&mut tx, &contract).await?;
        tx.commit().await?;
        self.get(&contract.capability_id, &contract.capability_version)
            .await?
            .ok_or_else(|| {
                RuntimeError::InvariantViolation(format!(
                    "capability {}@{} disappeared after registration",
                    contract.capability_id, contract.capability_version
                ))
            })
    }

    pub async fn list(&self) -> Result<Vec<CapabilityContract>> {
        let rows = sqlx::query_as::<_, CapabilityContractRow>(
            r#"
            SELECT
                capability_id,
                capability_name,
                capability_version,
                contract_version,
                description,
                provider_id,
                status,
                input_schema,
                output_schema,
                effect_class,
                idempotency_class,
                timeout_class,
                cost_class,
                trust_level,
                required_authority_scope,
                observability_class,
                streaming_support,
                supports_cancellation,
                supports_partial_result,
                supports_checkpoint_safe_resume,
                side_effect_surface,
                trust_zone_surface,
                external_dependencies,
                sandbox_profile,
                evidence_profile,
                rate_limit_class,
                tenant_restrictions,
                created_at,
                updated_at
            FROM capabilities
            ORDER BY capability_id, capability_version
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(CapabilityContractRow::into_contract)
            .collect()
    }

    pub async fn get(
        &self,
        capability_id: &str,
        capability_version: &str,
    ) -> Result<Option<CapabilityContract>> {
        self.get_in(&self.pool, capability_id, capability_version)
            .await
    }

    pub async fn resolve_for_invocation(
        &self,
        capability_id: &str,
        capability_version: Option<&str>,
    ) -> Result<Option<CapabilityContract>> {
        match capability_version {
            Some(version) => self.get(capability_id, version).await,
            None => {
                let row = sqlx::query_as::<_, CapabilityContractRow>(
                    r#"
                    SELECT
                        capability_id,
                        capability_name,
                        capability_version,
                        contract_version,
                        description,
                        provider_id,
                        status,
                        input_schema,
                        output_schema,
                        effect_class,
                        idempotency_class,
                        timeout_class,
                        cost_class,
                        trust_level,
                        required_authority_scope,
                        observability_class,
                        streaming_support,
                        supports_cancellation,
                        supports_partial_result,
                        supports_checkpoint_safe_resume,
                        side_effect_surface,
                        trust_zone_surface,
                        external_dependencies,
                        sandbox_profile,
                        evidence_profile,
                        rate_limit_class,
                        tenant_restrictions,
                        created_at,
                        updated_at
                    FROM capabilities
                    WHERE capability_id = ?
                    ORDER BY
                        CASE status
                            WHEN 'active' THEN 0
                            WHEN 'deprecated' THEN 1
                            WHEN 'disabled' THEN 2
                            ELSE 3
                        END,
                        updated_at DESC,
                        capability_version DESC
                    LIMIT 1
                    "#,
                )
                .bind(capability_id)
                .fetch_optional(&self.pool)
                .await?;

                row.map(CapabilityContractRow::into_contract).transpose()
            }
        }
    }

    pub async fn resolve_for_invocation_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        capability_id: &str,
        capability_version: Option<&str>,
    ) -> Result<Option<CapabilityContract>> {
        match capability_version {
            Some(version) => self.get_in(&mut **tx, capability_id, version).await,
            None => {
                let row = sqlx::query_as::<_, CapabilityContractRow>(
                    r#"
                    SELECT
                        capability_id,
                        capability_name,
                        capability_version,
                        contract_version,
                        description,
                        provider_id,
                        status,
                        input_schema,
                        output_schema,
                        effect_class,
                        idempotency_class,
                        timeout_class,
                        cost_class,
                        trust_level,
                        required_authority_scope,
                        observability_class,
                        streaming_support,
                        supports_cancellation,
                        supports_partial_result,
                        supports_checkpoint_safe_resume,
                        side_effect_surface,
                        trust_zone_surface,
                        external_dependencies,
                        sandbox_profile,
                        evidence_profile,
                        rate_limit_class,
                        tenant_restrictions,
                        created_at,
                        updated_at
                    FROM capabilities
                    WHERE capability_id = ?
                    ORDER BY
                        CASE status
                            WHEN 'active' THEN 0
                            WHEN 'deprecated' THEN 1
                            WHEN 'disabled' THEN 2
                            ELSE 3
                        END,
                        updated_at DESC,
                        capability_version DESC
                    LIMIT 1
                    "#,
                )
                .bind(capability_id)
                .fetch_optional(&mut **tx)
                .await?;

                row.map(CapabilityContractRow::into_contract).transpose()
            }
        }
    }

    async fn get_in<'e, E>(
        &self,
        executor: E,
        capability_id: &str,
        capability_version: &str,
    ) -> Result<Option<CapabilityContract>>
    where
        E: sqlx::Executor<'e, Database = Sqlite>,
    {
        let row = sqlx::query_as::<_, CapabilityContractRow>(
            r#"
            SELECT
                capability_id,
                capability_name,
                capability_version,
                contract_version,
                description,
                provider_id,
                status,
                input_schema,
                output_schema,
                effect_class,
                idempotency_class,
                timeout_class,
                cost_class,
                trust_level,
                required_authority_scope,
                observability_class,
                streaming_support,
                supports_cancellation,
                supports_partial_result,
                supports_checkpoint_safe_resume,
                side_effect_surface,
                trust_zone_surface,
                external_dependencies,
                sandbox_profile,
                evidence_profile,
                rate_limit_class,
                tenant_restrictions,
                created_at,
                updated_at
            FROM capabilities
            WHERE capability_id = ? AND capability_version = ?
            "#,
        )
        .bind(capability_id)
        .bind(capability_version)
        .fetch_optional(executor)
        .await?;

        row.map(CapabilityContractRow::into_contract).transpose()
    }

    pub async fn upsert_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        contract: &CapabilityContract,
    ) -> Result<()> {
        contract.validate_registration()?;

        if let Some(existing) = self
            .get_in(
                &mut **tx,
                &contract.capability_id,
                &contract.capability_version,
            )
            .await?
        {
            if !durable_contract_matches(&existing, contract) {
                return Err(RuntimeError::InvariantViolation(format!(
                    "capability {}@{} is a durable contract and cannot be mutated in place",
                    contract.capability_id, contract.capability_version
                )));
            }

            sqlx::query(
                r#"
                UPDATE capabilities
                SET
                    status = ?,
                    updated_at = ?
                WHERE capability_id = ? AND capability_version = ?
                "#,
            )
            .bind(contract.status.as_str())
            .bind(encode_timestamp(contract.updated_at)?)
            .bind(&contract.capability_id)
            .bind(&contract.capability_version)
            .execute(&mut **tx)
            .await?;

            return Ok(());
        }

        sqlx::query(
            r#"
            INSERT INTO capabilities (
                capability_id,
                capability_name,
                capability_version,
                contract_version,
                description,
                provider_id,
                status,
                input_schema,
                output_schema,
                effect_class,
                idempotency_class,
                timeout_class,
                cost_class,
                trust_level,
                required_authority_scope,
                observability_class,
                streaming_support,
                supports_cancellation,
                supports_partial_result,
                supports_checkpoint_safe_resume,
                side_effect_surface,
                trust_zone_surface,
                external_dependencies,
                sandbox_profile,
                evidence_profile,
                rate_limit_class,
                tenant_restrictions,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&contract.capability_id)
        .bind(&contract.capability_name)
        .bind(&contract.capability_version)
        .bind(&contract.contract_version)
        .bind(&contract.description)
        .bind(&contract.provider_id)
        .bind(contract.status.as_str())
        .bind(json_to_text(&contract.input_schema)?)
        .bind(json_to_text(&contract.output_schema)?)
        .bind(contract.effect_class.as_str())
        .bind(contract.idempotency_class.as_str())
        .bind(contract.timeout_class.as_str())
        .bind(contract.cost_class.as_str())
        .bind(contract.trust_level.as_str())
        .bind(&contract.required_authority_scope)
        .bind(&contract.observability_class)
        .bind(contract.streaming_support.map(bool_to_sqlite))
        .bind(contract.supports_cancellation.map(bool_to_sqlite))
        .bind(contract.supports_partial_result.map(bool_to_sqlite))
        .bind(contract.supports_checkpoint_safe_resume.map(bool_to_sqlite))
        .bind(optional_json_to_text(contract.side_effect_surface.as_ref())?)
        .bind(optional_json_to_text(contract.trust_zone_surface.as_ref())?)
        .bind(optional_json_to_text(contract.external_dependencies.as_ref())?)
        .bind(optional_json_to_text(contract.sandbox_profile.as_ref())?)
        .bind(optional_json_to_text(contract.evidence_profile.as_ref())?)
        .bind(&contract.rate_limit_class)
        .bind(optional_json_to_text(contract.tenant_restrictions.as_ref())?)
        .bind(encode_timestamp(contract.created_at)?)
        .bind(encode_timestamp(contract.updated_at)?)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct CapabilityCallStore {
    pool: SqlitePool,
}

impl CapabilityCallStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn record_requested(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        input: NewCapabilityCallRecord,
    ) -> Result<CapabilityCallRecord> {
        sqlx::query(
            r#"
            INSERT INTO capability_calls (
                call_id,
                task_id,
                caller_principal_id,
                caller_principal_role,
                capability_id,
                capability_version,
                input_payload,
                requested_at,
                dispatched_at,
                completed_at,
                policy_context_ref,
                budget_context_ref,
                effect_class,
                cost_class,
                idempotency_expectation,
                correlation_id,
                checkpoint_ref,
                state_version_ref,
                timeout_override_ms,
                request_reason_code,
                status,
                outcome,
                result_payload,
                side_effect_summary,
                retry_safety,
                provider_metadata,
                evidence_ref,
                latency_ms,
                reason_code,
                requested_event_id,
                requested_sequence_number,
                dispatched_event_id,
                dispatched_sequence_number,
                outcome_event_id,
                outcome_sequence_number,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?, NULL, NULL, NULL, NULL, ?, ?)
            "#,
        )
        .bind(&input.call_id)
        .bind(input.task_id.to_string())
        .bind(input.caller.principal_id.to_string())
        .bind(&input.caller.principal_role)
        .bind(&input.capability_id)
        .bind(&input.capability_version)
        .bind(json_to_text(&input.input_payload)?)
        .bind(encode_timestamp(input.requested_at)?)
        .bind(&input.policy_context_ref)
        .bind(&input.budget_context_ref)
        .bind(&input.effect_class)
        .bind(input.cost_class.as_ref().map(CostClass::as_str))
        .bind(input.idempotency_expectation.as_str())
        .bind(&input.correlation_id)
        .bind(input.checkpoint_ref.map(|value| value.to_string()))
        .bind(input.state_version_ref.map(|value| value.to_string()))
        .bind(input.timeout_override_ms)
        .bind(&input.request_reason_code)
        .bind(CapabilityCallStatus::Requested.as_str())
        .bind(input.requested_event_id.to_string())
        .bind(input.requested_sequence_number)
        .bind(encode_timestamp(input.created_at)?)
        .bind(encode_timestamp(input.created_at)?)
        .execute(&mut **tx)
        .await?;

        self.get_in_tx(tx, &input.call_id).await
    }

    pub async fn mark_policy_blocked(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        call_id: &str,
        capability_version: Option<&str>,
        effect_class: &str,
        completed_at: OffsetDateTime,
        outcome: CapabilityOutcome,
        result_payload: &Value,
        side_effect_summary: SideEffectSummary,
        retry_safety: RetrySafety,
        reason_code: Option<&str>,
        provider_metadata: Option<&Value>,
        outcome_event_id: Uuid,
        outcome_sequence_number: i64,
    ) -> Result<CapabilityCallRecord> {
        sqlx::query(
            r#"
            UPDATE capability_calls
            SET
                capability_version = COALESCE(?, capability_version),
                effect_class = ?,
                status = ?,
                completed_at = ?,
                outcome = ?,
                result_payload = ?,
                side_effect_summary = ?,
                retry_safety = ?,
                provider_metadata = ?,
                reason_code = ?,
                outcome_event_id = ?,
                outcome_sequence_number = ?,
                updated_at = ?
            WHERE call_id = ?
            "#,
        )
        .bind(capability_version)
        .bind(effect_class)
        .bind(CapabilityCallStatus::PolicyBlocked.as_str())
        .bind(encode_timestamp(completed_at)?)
        .bind(outcome.as_str())
        .bind(json_to_text(result_payload)?)
        .bind(side_effect_summary.as_str())
        .bind(retry_safety.as_str())
        .bind(optional_json_to_text(provider_metadata)?)
        .bind(reason_code)
        .bind(outcome_event_id.to_string())
        .bind(outcome_sequence_number)
        .bind(encode_timestamp(completed_at)?)
        .bind(call_id)
        .execute(&mut **tx)
        .await?;

        self.get_in_tx(tx, call_id).await
    }

    pub async fn mark_awaiting_approval(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        call_id: &str,
        capability_version: &str,
        effect_class: &str,
        state_version_ref: Uuid,
        reason_code: &str,
        provider_metadata: Option<&Value>,
        updated_at: OffsetDateTime,
    ) -> Result<CapabilityCallRecord> {
        sqlx::query(
            r#"
            UPDATE capability_calls
            SET
                capability_version = ?,
                effect_class = ?,
                state_version_ref = ?,
                status = ?,
                reason_code = ?,
                provider_metadata = ?,
                completed_at = NULL,
                outcome = NULL,
                result_payload = NULL,
                side_effect_summary = NULL,
                retry_safety = NULL,
                evidence_ref = NULL,
                latency_ms = NULL,
                outcome_event_id = NULL,
                outcome_sequence_number = NULL,
                updated_at = ?
            WHERE call_id = ?
            "#,
        )
        .bind(capability_version)
        .bind(effect_class)
        .bind(state_version_ref.to_string())
        .bind(CapabilityCallStatus::PolicyBlocked.as_str())
        .bind(reason_code)
        .bind(optional_json_to_text(provider_metadata)?)
        .bind(encode_timestamp(updated_at)?)
        .bind(call_id)
        .execute(&mut **tx)
        .await?;

        self.get_in_tx(tx, call_id).await
    }

    pub async fn mark_dispatched(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        call_id: &str,
        capability_version: &str,
        effect_class: &str,
        state_version_ref: Option<Uuid>,
        dispatched_at: OffsetDateTime,
        dispatched_event_id: Uuid,
        dispatched_sequence_number: i64,
    ) -> Result<CapabilityCallRecord> {
        sqlx::query(
            r#"
            UPDATE capability_calls
            SET
                capability_version = ?,
                effect_class = ?,
                state_version_ref = COALESCE(?, state_version_ref),
                status = ?,
                dispatched_at = ?,
                dispatched_event_id = ?,
                dispatched_sequence_number = ?,
                updated_at = ?
            WHERE call_id = ?
            "#,
        )
        .bind(capability_version)
        .bind(effect_class)
        .bind(state_version_ref.map(|value| value.to_string()))
        .bind(CapabilityCallStatus::Dispatched.as_str())
        .bind(encode_timestamp(dispatched_at)?)
        .bind(dispatched_event_id.to_string())
        .bind(dispatched_sequence_number)
        .bind(encode_timestamp(dispatched_at)?)
        .bind(call_id)
        .execute(&mut **tx)
        .await?;

        self.get_in_tx(tx, call_id).await
    }

    pub async fn update_checkpoint_ref(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        call_id: &str,
        checkpoint_ref: Uuid,
        updated_at: OffsetDateTime,
    ) -> Result<CapabilityCallRecord> {
        sqlx::query(
            r#"
            UPDATE capability_calls
            SET
                checkpoint_ref = ?,
                updated_at = ?
            WHERE call_id = ?
            "#,
        )
        .bind(checkpoint_ref.to_string())
        .bind(encode_timestamp(updated_at)?)
        .bind(call_id)
        .execute(&mut **tx)
        .await?;

        self.get_in_tx(tx, call_id).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn mark_completed(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        call_id: &str,
        capability_version: Option<&str>,
        effect_class: &str,
        state_version_ref: Option<Uuid>,
        completed_at: OffsetDateTime,
        outcome: CapabilityOutcome,
        result_payload: &Value,
        side_effect_summary: SideEffectSummary,
        retry_safety: RetrySafety,
        provider_metadata: Option<&Value>,
        evidence_ref: Option<&str>,
        latency_ms: Option<i64>,
        reason_code: Option<&str>,
        outcome_event_id: Uuid,
        outcome_sequence_number: i64,
    ) -> Result<CapabilityCallRecord> {
        sqlx::query(
            r#"
            UPDATE capability_calls
            SET
                capability_version = COALESCE(?, capability_version),
                effect_class = ?,
                state_version_ref = COALESCE(?, state_version_ref),
                status = ?,
                completed_at = ?,
                outcome = ?,
                result_payload = ?,
                side_effect_summary = ?,
                retry_safety = ?,
                provider_metadata = ?,
                evidence_ref = ?,
                latency_ms = ?,
                reason_code = ?,
                outcome_event_id = ?,
                outcome_sequence_number = ?,
                updated_at = ?
            WHERE call_id = ?
            "#,
        )
        .bind(capability_version)
        .bind(effect_class)
        .bind(state_version_ref.map(|value| value.to_string()))
        .bind(CapabilityCallStatus::Completed.as_str())
        .bind(encode_timestamp(completed_at)?)
        .bind(outcome.as_str())
        .bind(json_to_text(result_payload)?)
        .bind(side_effect_summary.as_str())
        .bind(retry_safety.as_str())
        .bind(optional_json_to_text(provider_metadata)?)
        .bind(evidence_ref)
        .bind(latency_ms)
        .bind(reason_code)
        .bind(outcome_event_id.to_string())
        .bind(outcome_sequence_number)
        .bind(encode_timestamp(completed_at)?)
        .bind(call_id)
        .execute(&mut **tx)
        .await?;

        self.get_in_tx(tx, call_id).await
    }

    pub async fn list_by_task(&self, task_id: Uuid) -> Result<Vec<CapabilityCallRecord>> {
        let rows = sqlx::query_as::<_, CapabilityCallRow>(
            r#"
            SELECT
                call_id,
                task_id,
                caller_principal_id,
                caller_principal_role,
                capability_id,
                capability_version,
                input_payload,
                requested_at,
                dispatched_at,
                completed_at,
                policy_context_ref,
                budget_context_ref,
                effect_class,
                cost_class,
                idempotency_expectation,
                correlation_id,
                checkpoint_ref,
                state_version_ref,
                timeout_override_ms,
                request_reason_code,
                status,
                outcome,
                result_payload,
                side_effect_summary,
                retry_safety,
                provider_metadata,
                evidence_ref,
                latency_ms,
                reason_code,
                requested_event_id,
                requested_sequence_number,
                dispatched_event_id,
                dispatched_sequence_number,
                outcome_event_id,
                outcome_sequence_number,
                created_at,
                updated_at
            FROM capability_calls
            WHERE task_id = ?
            ORDER BY requested_sequence_number, call_id
            "#,
        )
        .bind(task_id.to_string())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(CapabilityCallRow::into_record)
            .collect()
    }

    pub async fn get(&self, call_id: &str) -> Result<Option<CapabilityCallRecord>> {
        let row = sqlx::query_as::<_, CapabilityCallRow>(
            r#"
            SELECT
                call_id,
                task_id,
                caller_principal_id,
                caller_principal_role,
                capability_id,
                capability_version,
                input_payload,
                requested_at,
                dispatched_at,
                completed_at,
                policy_context_ref,
                budget_context_ref,
                effect_class,
                cost_class,
                idempotency_expectation,
                correlation_id,
                checkpoint_ref,
                state_version_ref,
                timeout_override_ms,
                request_reason_code,
                status,
                outcome,
                result_payload,
                side_effect_summary,
                retry_safety,
                provider_metadata,
                evidence_ref,
                latency_ms,
                reason_code,
                requested_event_id,
                requested_sequence_number,
                dispatched_event_id,
                dispatched_sequence_number,
                outcome_event_id,
                outcome_sequence_number,
                created_at,
                updated_at
            FROM capability_calls
            WHERE call_id = ?
            "#,
        )
        .bind(call_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(CapabilityCallRow::into_record).transpose()
    }

    pub async fn get_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        call_id: &str,
    ) -> Result<CapabilityCallRecord> {
        let row = sqlx::query_as::<_, CapabilityCallRow>(
            r#"
            SELECT
                call_id,
                task_id,
                caller_principal_id,
                caller_principal_role,
                capability_id,
                capability_version,
                input_payload,
                requested_at,
                dispatched_at,
                completed_at,
                policy_context_ref,
                budget_context_ref,
                effect_class,
                cost_class,
                idempotency_expectation,
                correlation_id,
                checkpoint_ref,
                state_version_ref,
                timeout_override_ms,
                request_reason_code,
                status,
                outcome,
                result_payload,
                side_effect_summary,
                retry_safety,
                provider_metadata,
                evidence_ref,
                latency_ms,
                reason_code,
                requested_event_id,
                requested_sequence_number,
                dispatched_event_id,
                dispatched_sequence_number,
                outcome_event_id,
                outcome_sequence_number,
                created_at,
                updated_at
            FROM capability_calls
            WHERE call_id = ?
            "#,
        )
        .bind(call_id)
        .fetch_one(&mut **tx)
        .await?;

        row.into_record()
    }

    pub async fn get_latest_pending_by_task_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: Uuid,
    ) -> Result<Option<CapabilityCallRecord>> {
        let row = sqlx::query_as::<_, CapabilityCallRow>(
            r#"
            SELECT
                call_id,
                task_id,
                caller_principal_id,
                caller_principal_role,
                capability_id,
                capability_version,
                input_payload,
                requested_at,
                dispatched_at,
                completed_at,
                policy_context_ref,
                budget_context_ref,
                effect_class,
                cost_class,
                idempotency_expectation,
                correlation_id,
                checkpoint_ref,
                state_version_ref,
                timeout_override_ms,
                request_reason_code,
                status,
                outcome,
                result_payload,
                side_effect_summary,
                retry_safety,
                provider_metadata,
                evidence_ref,
                latency_ms,
                reason_code,
                requested_event_id,
                requested_sequence_number,
                dispatched_event_id,
                dispatched_sequence_number,
                outcome_event_id,
                outcome_sequence_number,
                created_at,
                updated_at
            FROM capability_calls
            WHERE task_id = ?
              AND status = ?
              AND outcome IS NULL
            ORDER BY requested_sequence_number DESC, call_id DESC
            LIMIT 1
            "#,
        )
        .bind(task_id.to_string())
        .bind(CapabilityCallStatus::PolicyBlocked.as_str())
        .fetch_optional(&mut **tx)
        .await?;

        row.map(CapabilityCallRow::into_record).transpose()
    }
}

#[derive(Debug, FromRow)]
struct CapabilityContractRow {
    capability_id: String,
    capability_name: String,
    capability_version: String,
    contract_version: String,
    description: String,
    provider_id: String,
    status: String,
    input_schema: String,
    output_schema: String,
    effect_class: String,
    idempotency_class: String,
    timeout_class: String,
    cost_class: String,
    trust_level: String,
    required_authority_scope: String,
    observability_class: String,
    streaming_support: Option<i64>,
    supports_cancellation: Option<i64>,
    supports_partial_result: Option<i64>,
    supports_checkpoint_safe_resume: Option<i64>,
    side_effect_surface: Option<String>,
    trust_zone_surface: Option<String>,
    external_dependencies: Option<String>,
    sandbox_profile: Option<String>,
    evidence_profile: Option<String>,
    rate_limit_class: Option<String>,
    tenant_restrictions: Option<String>,
    created_at: String,
    updated_at: String,
}

impl CapabilityContractRow {
    fn into_contract(self) -> Result<CapabilityContract> {
        Ok(CapabilityContract {
            capability_id: self.capability_id,
            capability_name: self.capability_name,
            capability_version: self.capability_version,
            contract_version: self.contract_version,
            description: self.description,
            provider_id: self.provider_id,
            status: self.status.parse()?,
            input_schema: serde_json::from_str(&self.input_schema)?,
            output_schema: serde_json::from_str(&self.output_schema)?,
            effect_class: self.effect_class.parse()?,
            idempotency_class: self.idempotency_class.parse()?,
            timeout_class: self.timeout_class.parse()?,
            cost_class: self.cost_class.parse()?,
            trust_level: self.trust_level.parse()?,
            required_authority_scope: self.required_authority_scope,
            observability_class: self.observability_class,
            streaming_support: self.streaming_support.map(sqlite_to_bool).transpose()?,
            supports_cancellation: self.supports_cancellation.map(sqlite_to_bool).transpose()?,
            supports_partial_result: self
                .supports_partial_result
                .map(sqlite_to_bool)
                .transpose()?,
            supports_checkpoint_safe_resume: self
                .supports_checkpoint_safe_resume
                .map(sqlite_to_bool)
                .transpose()?,
            side_effect_surface: parse_optional_json(self.side_effect_surface)?,
            trust_zone_surface: parse_optional_json(self.trust_zone_surface)?,
            external_dependencies: parse_optional_json(self.external_dependencies)?,
            sandbox_profile: parse_optional_json(self.sandbox_profile)?,
            evidence_profile: parse_optional_json(self.evidence_profile)?,
            rate_limit_class: self.rate_limit_class,
            tenant_restrictions: parse_optional_json(self.tenant_restrictions)?,
            created_at: decode_timestamp(&self.created_at)?,
            updated_at: decode_timestamp(&self.updated_at)?,
        })
    }
}

#[derive(Debug, FromRow)]
struct CapabilityCallRow {
    call_id: String,
    task_id: String,
    caller_principal_id: String,
    caller_principal_role: String,
    capability_id: String,
    capability_version: Option<String>,
    input_payload: String,
    requested_at: String,
    dispatched_at: Option<String>,
    completed_at: Option<String>,
    policy_context_ref: String,
    budget_context_ref: String,
    effect_class: String,
    cost_class: Option<String>,
    idempotency_expectation: String,
    correlation_id: String,
    checkpoint_ref: Option<String>,
    state_version_ref: Option<String>,
    timeout_override_ms: Option<i64>,
    request_reason_code: Option<String>,
    status: String,
    outcome: Option<String>,
    result_payload: Option<String>,
    side_effect_summary: Option<String>,
    retry_safety: Option<String>,
    provider_metadata: Option<String>,
    evidence_ref: Option<String>,
    latency_ms: Option<i64>,
    reason_code: Option<String>,
    requested_event_id: String,
    requested_sequence_number: i64,
    dispatched_event_id: Option<String>,
    dispatched_sequence_number: Option<i64>,
    outcome_event_id: Option<String>,
    outcome_sequence_number: Option<i64>,
    created_at: String,
    updated_at: String,
}

impl CapabilityCallRow {
    fn into_record(self) -> Result<CapabilityCallRecord> {
        Ok(CapabilityCallRecord {
            call_id: self.call_id,
            task_id: parse_uuid("task_id", &self.task_id)?,
            caller_principal_id: parse_uuid("caller_principal_id", &self.caller_principal_id)?,
            caller_principal_role: self.caller_principal_role,
            capability_id: self.capability_id,
            capability_version: self.capability_version,
            input_payload: serde_json::from_str(&self.input_payload)?,
            requested_at: decode_timestamp(&self.requested_at)?,
            dispatched_at: self
                .dispatched_at
                .as_deref()
                .map(decode_timestamp)
                .transpose()?,
            completed_at: self
                .completed_at
                .as_deref()
                .map(decode_timestamp)
                .transpose()?,
            policy_context_ref: self.policy_context_ref,
            budget_context_ref: self.budget_context_ref,
            effect_class: self.effect_class,
            cost_class: self.cost_class.map(|value| value.parse()).transpose()?,
            idempotency_expectation: self.idempotency_expectation.parse()?,
            correlation_id: self.correlation_id,
            checkpoint_ref: parse_optional_uuid("checkpoint_ref", self.checkpoint_ref)?,
            state_version_ref: parse_optional_uuid("state_version_ref", self.state_version_ref)?,
            timeout_override_ms: self.timeout_override_ms,
            request_reason_code: self.request_reason_code,
            status: self.status.parse()?,
            outcome: self.outcome.map(|value| value.parse()).transpose()?,
            result_payload: self
                .result_payload
                .as_deref()
                .map(serde_json::from_str)
                .transpose()?,
            side_effect_summary: self
                .side_effect_summary
                .map(|value| value.parse())
                .transpose()?,
            retry_safety: self.retry_safety.map(|value| value.parse()).transpose()?,
            provider_metadata: self
                .provider_metadata
                .as_deref()
                .map(serde_json::from_str)
                .transpose()?,
            evidence_ref: self.evidence_ref,
            latency_ms: self.latency_ms,
            reason_code: self.reason_code,
            requested_event_id: parse_uuid("requested_event_id", &self.requested_event_id)?,
            requested_sequence_number: self.requested_sequence_number,
            dispatched_event_id: parse_optional_uuid(
                "dispatched_event_id",
                self.dispatched_event_id,
            )?,
            dispatched_sequence_number: self.dispatched_sequence_number,
            outcome_event_id: parse_optional_uuid("outcome_event_id", self.outcome_event_id)?,
            outcome_sequence_number: self.outcome_sequence_number,
            created_at: decode_timestamp(&self.created_at)?,
            updated_at: decode_timestamp(&self.updated_at)?,
        })
    }
}

fn durable_contract_matches(existing: &CapabilityContract, candidate: &CapabilityContract) -> bool {
    existing.capability_id == candidate.capability_id
        && existing.capability_name == candidate.capability_name
        && existing.capability_version == candidate.capability_version
        && existing.contract_version == candidate.contract_version
        && existing.description == candidate.description
        && existing.provider_id == candidate.provider_id
        && existing.input_schema == candidate.input_schema
        && existing.output_schema == candidate.output_schema
        && existing.effect_class == candidate.effect_class
        && existing.idempotency_class == candidate.idempotency_class
        && existing.timeout_class == candidate.timeout_class
        && existing.cost_class == candidate.cost_class
        && existing.trust_level == candidate.trust_level
        && existing.required_authority_scope == candidate.required_authority_scope
        && existing.observability_class == candidate.observability_class
        && existing.streaming_support == candidate.streaming_support
        && existing.supports_cancellation == candidate.supports_cancellation
        && existing.supports_partial_result == candidate.supports_partial_result
        && existing.supports_checkpoint_safe_resume == candidate.supports_checkpoint_safe_resume
        && existing.side_effect_surface == candidate.side_effect_surface
        && existing.trust_zone_surface == candidate.trust_zone_surface
        && existing.external_dependencies == candidate.external_dependencies
        && existing.sandbox_profile == candidate.sandbox_profile
        && existing.evidence_profile == candidate.evidence_profile
        && existing.rate_limit_class == candidate.rate_limit_class
        && existing.tenant_restrictions == candidate.tenant_restrictions
}

pub fn validate_payload_against_schema(
    schema: &Value,
    payload: &Value,
) -> std::result::Result<(), String> {
    validate_against_schema(schema, payload, "$")
}

fn validate_schema_contract(schema: &Value, field: &'static str) -> Result<()> {
    if !schema.is_object() {
        return Err(RuntimeError::CapabilitySchemaValidation {
            context: field,
            message: "schema must be a JSON object".to_owned(),
        });
    }

    if schema.get("type").and_then(Value::as_str).is_none() {
        return Err(RuntimeError::CapabilitySchemaValidation {
            context: field,
            message: "schema must declare a top-level type".to_owned(),
        });
    }

    Ok(())
}

fn validate_against_schema(
    schema: &Value,
    payload: &Value,
    path: &str,
) -> std::result::Result<(), String> {
    let Some(schema_object) = schema.as_object() else {
        return Err(format!("{path}: schema must be an object"));
    };

    if let Some(enum_values) = schema_object.get("enum").and_then(Value::as_array) {
        if !enum_values.iter().any(|item| item == payload) {
            return Err(format!("{path}: value is not in enum"));
        }
    }

    let Some(schema_type) = schema_object.get("type").and_then(Value::as_str) else {
        return Ok(());
    };

    match schema_type {
        "object" => {
            let Some(object) = payload.as_object() else {
                return Err(format!("{path}: expected object"));
            };

            if let Some(required) = schema_object.get("required").and_then(Value::as_array) {
                for key in required {
                    let Some(key) = key.as_str() else {
                        return Err(format!("{path}: required entries must be strings"));
                    };
                    if !object.contains_key(key) {
                        return Err(format!("{path}.{key}: missing required property"));
                    }
                }
            }

            let properties = schema_object
                .get("properties")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();

            if matches!(
                schema_object.get("additionalProperties"),
                Some(Value::Bool(false))
            ) {
                for key in object.keys() {
                    if !properties.contains_key(key) {
                        return Err(format!(
                            "{path}.{key}: additional properties are not allowed"
                        ));
                    }
                }
            }

            for (key, value) in object {
                if let Some(property_schema) = properties.get(key) {
                    validate_against_schema(property_schema, value, &format!("{path}.{key}"))?;
                }
            }
        }
        "array" => {
            let Some(items) = payload.as_array() else {
                return Err(format!("{path}: expected array"));
            };
            if let Some(item_schema) = schema_object.get("items") {
                for (index, item) in items.iter().enumerate() {
                    validate_against_schema(item_schema, item, &format!("{path}[{index}]"))?;
                }
            }
        }
        "string" => {
            if !payload.is_string() {
                return Err(format!("{path}: expected string"));
            }
        }
        "integer" => {
            let Some(number) = payload.as_number() else {
                return Err(format!("{path}: expected integer"));
            };
            if !(number.is_i64() || number.is_u64()) {
                return Err(format!("{path}: expected integer"));
            }
        }
        "number" => {
            if !payload.is_number() {
                return Err(format!("{path}: expected number"));
            }
        }
        "boolean" => {
            if !payload.is_boolean() {
                return Err(format!("{path}: expected boolean"));
            }
        }
        "null" => {
            if !payload.is_null() {
                return Err(format!("{path}: expected null"));
            }
        }
        other => {
            return Err(format!("{path}: unsupported schema type {other}"));
        }
    }

    Ok(())
}

fn dispatch_key(capability_id: &str, capability_version: &str) -> String {
    format!("{capability_id}@{capability_version}")
}

fn bool_to_sqlite(value: bool) -> i64 {
    if value {
        1
    } else {
        0
    }
}

fn sqlite_to_bool(value: i64) -> Result<bool> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(RuntimeError::InvariantViolation(format!(
            "invalid sqlite boolean value {other}"
        ))),
    }
}

fn json_to_text(value: &Value) -> Result<String> {
    Ok(serde_json::to_string(value)?)
}

fn optional_json_to_text(value: Option<&Value>) -> Result<Option<String>> {
    value.map(json_to_text).transpose()
}

fn parse_optional_json(value: Option<String>) -> Result<Option<Value>> {
    value
        .map(|item| serde_json::from_str(&item))
        .transpose()
        .map_err(Into::into)
}
