use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{FromRow, Sqlite, SqlitePool, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::db::{decode_timestamp, encode_timestamp, parse_uuid};
use crate::error::Result;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckpointRecord {
    pub checkpoint_id: Uuid,
    pub task_id: Uuid,
    pub state_version_ref: Uuid,
    pub event_sequence_number: i64,
    pub status: String,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub created_by: String,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct NewCheckpointRecord {
    pub checkpoint_id: Uuid,
    pub task_id: Uuid,
    pub state_version_ref: Uuid,
    pub event_sequence_number: i64,
    pub status: String,
    pub created_at: OffsetDateTime,
    pub created_by: String,
    pub payload: Value,
}

#[derive(Clone, Default)]
pub struct CheckpointStore;

impl CheckpointStore {
    pub async fn record_placeholder(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        record: NewCheckpointRecord,
    ) -> Result<CheckpointRecord> {
        let payload = serde_json::to_string(&record.payload)?;
        sqlx::query(
            r#"
            INSERT INTO checkpoints (
                checkpoint_id,
                task_id,
                state_version_ref,
                event_sequence_number,
                status,
                created_at,
                created_by,
                payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(record.checkpoint_id.to_string())
        .bind(record.task_id.to_string())
        .bind(record.state_version_ref.to_string())
        .bind(record.event_sequence_number)
        .bind(&record.status)
        .bind(encode_timestamp(record.created_at)?)
        .bind(&record.created_by)
        .bind(payload)
        .execute(&mut **tx)
        .await?;

        Ok(CheckpointRecord {
            checkpoint_id: record.checkpoint_id,
            task_id: record.task_id,
            state_version_ref: record.state_version_ref,
            event_sequence_number: record.event_sequence_number,
            status: record.status,
            created_at: record.created_at,
            created_by: record.created_by,
            payload: record.payload,
        })
    }

    pub async fn get_latest_for_task(
        &self,
        pool: &SqlitePool,
        task_id: Uuid,
    ) -> Result<Option<CheckpointRecord>> {
        let row = sqlx::query_as::<_, CheckpointRow>(
            r#"
            SELECT checkpoint_id, task_id, state_version_ref, event_sequence_number, status, created_at, created_by, payload
            FROM checkpoints
            WHERE task_id = ?
            ORDER BY event_sequence_number DESC
            LIMIT 1
            "#,
        )
        .bind(task_id.to_string())
        .fetch_optional(pool)
        .await?;

        row.map(CheckpointRow::into_checkpoint).transpose()
    }
}

#[derive(Debug, FromRow)]
struct CheckpointRow {
    checkpoint_id: String,
    task_id: String,
    state_version_ref: String,
    event_sequence_number: i64,
    status: String,
    created_at: String,
    created_by: String,
    payload: String,
}

impl CheckpointRow {
    fn into_checkpoint(self) -> Result<CheckpointRecord> {
        let Self {
            checkpoint_id,
            task_id,
            state_version_ref,
            event_sequence_number,
            status,
            created_at,
            created_by,
            payload,
        } = self;

        Ok(CheckpointRecord {
            checkpoint_id: parse_uuid("checkpoint_id", &checkpoint_id)?,
            task_id: parse_uuid("task_id", &task_id)?,
            state_version_ref: parse_uuid("state_version_ref", &state_version_ref)?,
            event_sequence_number,
            status,
            created_at: decode_timestamp(&created_at)?,
            created_by,
            payload: serde_json::from_str(&payload)?,
        })
    }
}
