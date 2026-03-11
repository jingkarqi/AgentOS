use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{FromRow, Sqlite, SqlitePool, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::clock::{Clock, IdGenerator};
use crate::db::{decode_timestamp, encode_timestamp, parse_optional_uuid, parse_uuid};
use crate::error::{Result, RuntimeError};
use crate::principal::PrincipalAttribution;

pub const EVENT_SCHEMA_VERSION: &str = "agentos.event-envelope.v0.1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventFamily {
    Task,
    Capability,
    Policy,
    Checkpoint,
    Control,
    Failure,
    Kernel,
}

impl EventFamily {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Task => "task",
            Self::Capability => "capability",
            Self::Policy => "policy",
            Self::Checkpoint => "checkpoint",
            Self::Control => "control",
            Self::Failure => "failure",
            Self::Kernel => "kernel",
        }
    }
}

impl std::str::FromStr for EventFamily {
    type Err = RuntimeError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "task" => Ok(Self::Task),
            "capability" => Ok(Self::Capability),
            "policy" => Ok(Self::Policy),
            "checkpoint" => Ok(Self::Checkpoint),
            "control" => Ok(Self::Control),
            "failure" => Ok(Self::Failure),
            "kernel" => Ok(Self::Kernel),
            _ => Err(RuntimeError::InvalidEnumValue {
                kind: "event_family",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventEnvelope {
    pub schema_version: String,
    pub event_id: Uuid,
    pub event_type: String,
    pub event_family: EventFamily,
    pub task_id: Uuid,
    pub sequence_number: i64,
    #[serde(with = "time::serde::rfc3339")]
    pub occurred_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub recorded_at: OffsetDateTime,
    pub emitted_by: String,
    pub payload: Value,
    pub correlation_id: Option<String>,
    pub causation_event_id: Option<Uuid>,
    pub principal_id: Option<Uuid>,
    pub principal_role: Option<String>,
    pub policy_context_ref: Option<String>,
    pub budget_context_ref: Option<String>,
    pub checkpoint_ref: Option<Uuid>,
    pub state_version_ref: Option<Uuid>,
    pub evidence_ref: Option<String>,
    pub trace_ref: Option<String>,
    pub tenant_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EventDraft {
    pub event_type: String,
    pub event_family: EventFamily,
    pub task_id: Uuid,
    pub occurred_at: OffsetDateTime,
    pub emitted_by: String,
    pub payload: Value,
    pub correlation_id: Option<String>,
    pub causation_event_id: Option<Uuid>,
    pub principal: Option<PrincipalAttribution>,
    pub policy_context_ref: Option<String>,
    pub budget_context_ref: Option<String>,
    pub checkpoint_ref: Option<Uuid>,
    pub state_version_ref: Option<Uuid>,
    pub evidence_ref: Option<String>,
    pub trace_ref: Option<String>,
    pub tenant_id: Option<String>,
}

impl EventDraft {
    pub fn new(
        task_id: Uuid,
        event_type: impl Into<String>,
        event_family: EventFamily,
        payload: Value,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            event_family,
            task_id,
            occurred_at: OffsetDateTime::now_utc(),
            emitted_by: "kernel".to_owned(),
            payload,
            correlation_id: None,
            causation_event_id: None,
            principal: None,
            policy_context_ref: None,
            budget_context_ref: None,
            checkpoint_ref: None,
            state_version_ref: None,
            evidence_ref: None,
            trace_ref: None,
            tenant_id: None,
        }
    }
}

#[derive(Clone)]
pub struct EventLog {
    publication_topic: String,
}

impl EventLog {
    pub fn new(publication_topic: impl Into<String>) -> Self {
        Self {
            publication_topic: publication_topic.into(),
        }
    }

    pub async fn append(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        id_generator: &dyn IdGenerator,
        clock: &dyn Clock,
        draft: EventDraft,
    ) -> Result<EventEnvelope> {
        let EventDraft {
            event_type,
            event_family,
            task_id,
            occurred_at,
            emitted_by,
            payload,
            correlation_id,
            causation_event_id,
            principal,
            policy_context_ref,
            budget_context_ref,
            checkpoint_ref,
            state_version_ref,
            evidence_ref,
            trace_ref,
            tenant_id,
        } = draft;

        let next_sequence: i64 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(MAX(sequence_number), 0) + 1
            FROM events
            WHERE task_id = ?
            "#,
        )
        .bind(task_id.to_string())
        .fetch_one(&mut **tx)
        .await?;

        let event_id = id_generator.next_uuid();
        let recorded_at = clock.now();
        let payload_json = serde_json::to_string(&payload)?;
        let principal_id = principal
            .as_ref()
            .map(|value| value.principal_id.to_string());
        let principal_role = principal.as_ref().map(|value| value.principal_role.clone());
        let causation_event_id_text = causation_event_id.map(|value| value.to_string());
        let checkpoint_ref_text = checkpoint_ref.map(|value| value.to_string());
        let state_version_ref_text = state_version_ref.map(|value| value.to_string());
        let recorded_at_text = encode_timestamp(recorded_at)?;
        let occurred_at_text = encode_timestamp(occurred_at)?;

        sqlx::query(
            r#"
            INSERT INTO events (
                schema_version,
                event_id,
                event_type,
                event_family,
                task_id,
                sequence_number,
                occurred_at,
                recorded_at,
                emitted_by,
                correlation_id,
                causation_event_id,
                principal_id,
                principal_role,
                policy_context_ref,
                budget_context_ref,
                checkpoint_ref,
                state_version_ref,
                evidence_ref,
                trace_ref,
                tenant_id,
                payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(EVENT_SCHEMA_VERSION)
        .bind(event_id.to_string())
        .bind(&event_type)
        .bind(event_family.as_str())
        .bind(task_id.to_string())
        .bind(next_sequence)
        .bind(&occurred_at_text)
        .bind(&recorded_at_text)
        .bind(&emitted_by)
        .bind(&correlation_id)
        .bind(causation_event_id_text)
        .bind(principal_id)
        .bind(principal_role)
        .bind(&policy_context_ref)
        .bind(&budget_context_ref)
        .bind(checkpoint_ref_text)
        .bind(state_version_ref_text)
        .bind(&evidence_ref)
        .bind(&trace_ref)
        .bind(&tenant_id)
        .bind(payload_json)
        .execute(&mut **tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO event_outbox (
                event_id,
                task_id,
                sequence_number,
                publication_topic,
                status,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(event_id.to_string())
        .bind(task_id.to_string())
        .bind(next_sequence)
        .bind(&self.publication_topic)
        .bind("pending")
        .bind(&recorded_at_text)
        .execute(&mut **tx)
        .await?;

        Ok(EventEnvelope {
            schema_version: EVENT_SCHEMA_VERSION.to_owned(),
            event_id,
            event_type,
            event_family,
            task_id,
            sequence_number: next_sequence,
            occurred_at,
            recorded_at,
            emitted_by,
            payload,
            correlation_id,
            causation_event_id,
            principal_id: principal.as_ref().map(|value| value.principal_id),
            principal_role: principal.map(|value| value.principal_role),
            policy_context_ref,
            budget_context_ref,
            checkpoint_ref,
            state_version_ref,
            evidence_ref,
            trace_ref,
            tenant_id,
        })
    }

    pub async fn list_by_task(
        &self,
        pool: &SqlitePool,
        task_id: Uuid,
    ) -> Result<Vec<EventEnvelope>> {
        let rows = sqlx::query_as::<_, EventRow>(
            r#"
            SELECT
                schema_version,
                event_id,
                event_type,
                event_family,
                task_id,
                sequence_number,
                occurred_at,
                recorded_at,
                emitted_by,
                correlation_id,
                causation_event_id,
                principal_id,
                principal_role,
                policy_context_ref,
                budget_context_ref,
                checkpoint_ref,
                state_version_ref,
                evidence_ref,
                trace_ref,
                tenant_id,
                payload
            FROM events
            WHERE task_id = ?
            ORDER BY sequence_number ASC
            "#,
        )
        .bind(task_id.to_string())
        .fetch_all(pool)
        .await?;

        rows.into_iter().map(EventRow::into_event).collect()
    }
}

#[derive(Debug, FromRow)]
struct EventRow {
    schema_version: String,
    event_id: String,
    event_type: String,
    event_family: String,
    task_id: String,
    sequence_number: i64,
    occurred_at: String,
    recorded_at: String,
    emitted_by: String,
    correlation_id: Option<String>,
    causation_event_id: Option<String>,
    principal_id: Option<String>,
    principal_role: Option<String>,
    policy_context_ref: Option<String>,
    budget_context_ref: Option<String>,
    checkpoint_ref: Option<String>,
    state_version_ref: Option<String>,
    evidence_ref: Option<String>,
    trace_ref: Option<String>,
    tenant_id: Option<String>,
    payload: String,
}

impl EventRow {
    fn into_event(self) -> Result<EventEnvelope> {
        let Self {
            schema_version,
            event_id,
            event_type,
            event_family,
            task_id,
            sequence_number,
            occurred_at,
            recorded_at,
            emitted_by,
            correlation_id,
            causation_event_id,
            principal_id,
            principal_role,
            policy_context_ref,
            budget_context_ref,
            checkpoint_ref,
            state_version_ref,
            evidence_ref,
            trace_ref,
            tenant_id,
            payload,
        } = self;

        Ok(EventEnvelope {
            schema_version,
            event_id: parse_uuid("event_id", &event_id)?,
            event_type,
            event_family: event_family.parse()?,
            task_id: parse_uuid("task_id", &task_id)?,
            sequence_number,
            occurred_at: decode_timestamp(&occurred_at)?,
            recorded_at: decode_timestamp(&recorded_at)?,
            emitted_by,
            payload: serde_json::from_str(&payload)?,
            correlation_id,
            causation_event_id: parse_optional_uuid("causation_event_id", causation_event_id)?,
            principal_id: parse_optional_uuid("principal_id", principal_id)?,
            principal_role,
            policy_context_ref,
            budget_context_ref,
            checkpoint_ref: parse_optional_uuid("checkpoint_ref", checkpoint_ref)?,
            state_version_ref: parse_optional_uuid("state_version_ref", state_version_ref)?,
            evidence_ref,
            trace_ref,
            tenant_id,
        })
    }
}
