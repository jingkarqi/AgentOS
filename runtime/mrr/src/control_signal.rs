use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{Executor, FromRow, Sqlite, SqlitePool, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::db::{decode_timestamp, encode_timestamp, parse_uuid};
use crate::error::{Result, RuntimeError};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ControlSignalType {
    Approve,
    Deny,
    Steer,
    Pause,
    Resume,
    ModifyBudget,
    ModifyScope,
    TakeOver,
    Reassign,
    Terminate,
}

impl ControlSignalType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Deny => "deny",
            Self::Steer => "steer",
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::ModifyBudget => "modify_budget",
            Self::ModifyScope => "modify_scope",
            Self::TakeOver => "take_over",
            Self::Reassign => "reassign",
            Self::Terminate => "terminate",
        }
    }
}

impl std::str::FromStr for ControlSignalType {
    type Err = RuntimeError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "approve" => Ok(Self::Approve),
            "deny" => Ok(Self::Deny),
            "steer" => Ok(Self::Steer),
            "pause" => Ok(Self::Pause),
            "resume" => Ok(Self::Resume),
            "modify_budget" => Ok(Self::ModifyBudget),
            "modify_scope" => Ok(Self::ModifyScope),
            "take_over" => Ok(Self::TakeOver),
            "reassign" => Ok(Self::Reassign),
            "terminate" => Ok(Self::Terminate),
            _ => Err(RuntimeError::InvalidEnumValue {
                kind: "control_signal_type",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ControlSignalStatus {
    Received,
    Deferred,
    Applied,
    Rejected,
}

impl ControlSignalStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Deferred => "deferred",
            Self::Applied => "applied",
            Self::Rejected => "rejected",
        }
    }
}

impl std::str::FromStr for ControlSignalStatus {
    type Err = RuntimeError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "received" => Ok(Self::Received),
            "deferred" => Ok(Self::Deferred),
            "applied" => Ok(Self::Applied),
            "rejected" => Ok(Self::Rejected),
            _ => Err(RuntimeError::InvalidEnumValue {
                kind: "control_signal_status",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ControlSignalRecord {
    pub signal_id: Uuid,
    pub task_id: Uuid,
    pub signal_type: ControlSignalType,
    pub status: ControlSignalStatus,
    pub issuer_principal_id: Uuid,
    pub issuer_principal_role: String,
    pub payload: Value,
    pub correlation_id: String,
    pub received_event_id: Uuid,
    pub received_sequence_number: i64,
    pub created_at: OffsetDateTime,
    pub applied_at: Option<OffsetDateTime>,
    pub outcome_event_id: Option<Uuid>,
    pub outcome_sequence_number: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct NewControlSignalRecord {
    pub signal_id: Uuid,
    pub task_id: Uuid,
    pub signal_type: ControlSignalType,
    pub status: ControlSignalStatus,
    pub issuer_principal_id: Uuid,
    pub issuer_principal_role: String,
    pub payload: Value,
    pub correlation_id: String,
    pub received_event_id: Uuid,
    pub received_sequence_number: i64,
    pub created_at: OffsetDateTime,
    pub applied_at: Option<OffsetDateTime>,
    pub outcome_event_id: Option<Uuid>,
    pub outcome_sequence_number: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct SubmitControlSignalCommand {
    pub signal_type: ControlSignalType,
    pub payload: Value,
}

impl SubmitControlSignalCommand {
    pub fn new(signal_type: ControlSignalType) -> Self {
        Self {
            signal_type,
            payload: json!({}),
        }
    }
}

#[derive(Clone, Default)]
pub struct ControlSignalStore;

impl ControlSignalStore {
    pub async fn record(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        record: NewControlSignalRecord,
    ) -> Result<ControlSignalRecord> {
        let payload = serde_json::to_string(&record.payload)?;

        sqlx::query(
            r#"
            INSERT INTO control_signals (
                signal_id,
                task_id,
                signal_type,
                status,
                issuer_principal_id,
                issuer_principal_role,
                payload,
                correlation_id,
                received_event_id,
                received_sequence_number,
                created_at,
                applied_at,
                outcome_event_id,
                outcome_sequence_number
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(record.signal_id.to_string())
        .bind(record.task_id.to_string())
        .bind(record.signal_type.as_str())
        .bind(record.status.as_str())
        .bind(record.issuer_principal_id.to_string())
        .bind(&record.issuer_principal_role)
        .bind(payload)
        .bind(&record.correlation_id)
        .bind(record.received_event_id.to_string())
        .bind(record.received_sequence_number)
        .bind(encode_timestamp(record.created_at)?)
        .bind(record.applied_at.map(encode_timestamp).transpose()?)
        .bind(record.outcome_event_id.map(|value| value.to_string()))
        .bind(record.outcome_sequence_number)
        .execute(&mut **tx)
        .await?;

        Ok(ControlSignalRecord {
            signal_id: record.signal_id,
            task_id: record.task_id,
            signal_type: record.signal_type,
            status: record.status,
            issuer_principal_id: record.issuer_principal_id,
            issuer_principal_role: record.issuer_principal_role,
            payload: record.payload,
            correlation_id: record.correlation_id,
            received_event_id: record.received_event_id,
            received_sequence_number: record.received_sequence_number,
            created_at: record.created_at,
            applied_at: record.applied_at,
            outcome_event_id: record.outcome_event_id,
            outcome_sequence_number: record.outcome_sequence_number,
        })
    }

    pub async fn update_status(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        signal_id: Uuid,
        status: ControlSignalStatus,
        applied_at: Option<OffsetDateTime>,
        outcome_event_id: Uuid,
        outcome_sequence_number: i64,
    ) -> Result<ControlSignalRecord> {
        sqlx::query(
            r#"
            UPDATE control_signals
            SET
                status = ?,
                applied_at = ?,
                outcome_event_id = ?,
                outcome_sequence_number = ?
            WHERE signal_id = ?
            "#,
        )
        .bind(status.as_str())
        .bind(applied_at.map(encode_timestamp).transpose()?)
        .bind(outcome_event_id.to_string())
        .bind(outcome_sequence_number)
        .bind(signal_id.to_string())
        .execute(&mut **tx)
        .await?;

        match self.get_in(&mut **tx, signal_id).await? {
            Some(record) => Ok(record),
            None => Err(RuntimeError::InvariantViolation(format!(
                "control signal {} disappeared during status update",
                signal_id
            ))),
        }
    }

    pub async fn list_by_task(
        &self,
        pool: &SqlitePool,
        task_id: Uuid,
    ) -> Result<Vec<ControlSignalRecord>> {
        let rows = sqlx::query_as::<_, ControlSignalRow>(
            r#"
            SELECT
                signal_id,
                task_id,
                signal_type,
                status,
                issuer_principal_id,
                issuer_principal_role,
                payload,
                correlation_id,
                received_event_id,
                received_sequence_number,
                created_at,
                applied_at,
                outcome_event_id,
                outcome_sequence_number
            FROM control_signals
            WHERE task_id = ?
            ORDER BY received_sequence_number ASC, signal_id ASC
            "#,
        )
        .bind(task_id.to_string())
        .fetch_all(pool)
        .await?;

        rows.into_iter()
            .map(ControlSignalRow::into_record)
            .collect()
    }

    pub async fn list_deferred_by_task_in<'e, E>(
        &self,
        executor: E,
        task_id: Uuid,
    ) -> Result<Vec<ControlSignalRecord>>
    where
        E: Executor<'e, Database = Sqlite>,
    {
        let rows = sqlx::query_as::<_, ControlSignalRow>(
            r#"
            SELECT
                signal_id,
                task_id,
                signal_type,
                status,
                issuer_principal_id,
                issuer_principal_role,
                payload,
                correlation_id,
                received_event_id,
                received_sequence_number,
                created_at,
                applied_at,
                outcome_event_id,
                outcome_sequence_number
            FROM control_signals
            WHERE task_id = ?
              AND status = 'deferred'
            ORDER BY received_sequence_number ASC, signal_id ASC
            "#,
        )
        .bind(task_id.to_string())
        .fetch_all(executor)
        .await?;

        rows.into_iter()
            .map(ControlSignalRow::into_record)
            .collect()
    }

    async fn get_in<'e, E>(
        &self,
        executor: E,
        signal_id: Uuid,
    ) -> Result<Option<ControlSignalRecord>>
    where
        E: Executor<'e, Database = Sqlite>,
    {
        let row = sqlx::query_as::<_, ControlSignalRow>(
            r#"
            SELECT
                signal_id,
                task_id,
                signal_type,
                status,
                issuer_principal_id,
                issuer_principal_role,
                payload,
                correlation_id,
                received_event_id,
                received_sequence_number,
                created_at,
                applied_at,
                outcome_event_id,
                outcome_sequence_number
            FROM control_signals
            WHERE signal_id = ?
            LIMIT 1
            "#,
        )
        .bind(signal_id.to_string())
        .fetch_optional(executor)
        .await?;

        row.map(ControlSignalRow::into_record).transpose()
    }
}

#[derive(Debug, FromRow)]
struct ControlSignalRow {
    signal_id: String,
    task_id: String,
    signal_type: String,
    status: String,
    issuer_principal_id: String,
    issuer_principal_role: String,
    payload: String,
    correlation_id: String,
    received_event_id: String,
    received_sequence_number: i64,
    created_at: String,
    applied_at: Option<String>,
    outcome_event_id: Option<String>,
    outcome_sequence_number: Option<i64>,
}

impl ControlSignalRow {
    fn into_record(self) -> Result<ControlSignalRecord> {
        let Self {
            signal_id,
            task_id,
            signal_type,
            status,
            issuer_principal_id,
            issuer_principal_role,
            payload,
            correlation_id,
            received_event_id,
            received_sequence_number,
            created_at,
            applied_at,
            outcome_event_id,
            outcome_sequence_number,
        } = self;

        Ok(ControlSignalRecord {
            signal_id: parse_uuid("signal_id", &signal_id)?,
            task_id: parse_uuid("task_id", &task_id)?,
            signal_type: signal_type.parse()?,
            status: status.parse()?,
            issuer_principal_id: parse_uuid("issuer_principal_id", &issuer_principal_id)?,
            issuer_principal_role,
            payload: serde_json::from_str(&payload)?,
            correlation_id,
            received_event_id: parse_uuid("received_event_id", &received_event_id)?,
            received_sequence_number,
            created_at: decode_timestamp(&created_at)?,
            applied_at: applied_at.as_deref().map(decode_timestamp).transpose()?,
            outcome_event_id: outcome_event_id
                .as_deref()
                .map(|value| parse_uuid("outcome_event_id", value))
                .transpose()?,
            outcome_sequence_number,
        })
    }
}
