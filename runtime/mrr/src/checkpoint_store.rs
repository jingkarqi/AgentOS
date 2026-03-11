use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Executor, FromRow, Sqlite, SqlitePool, Transaction};
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
    pub async fn record(
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
        fetch_latest_checkpoint_row(pool, task_id)
            .await?
            .map(CheckpointRow::into_checkpoint)
            .transpose()
    }

    pub async fn get_latest_for_task_in<'e, E>(
        &self,
        executor: E,
        task_id: Uuid,
    ) -> Result<Option<CheckpointRecord>>
    where
        E: Executor<'e, Database = Sqlite>,
    {
        fetch_latest_checkpoint_row(executor, task_id)
            .await?
            .map(CheckpointRow::into_checkpoint)
            .transpose()
    }

    pub async fn get_by_id_for_task<'e, E>(
        &self,
        executor: E,
        task_id: Uuid,
        checkpoint_id: Uuid,
    ) -> Result<Option<CheckpointRecord>>
    where
        E: Executor<'e, Database = Sqlite>,
    {
        fetch_checkpoint_row_by_id(executor, task_id, checkpoint_id)
            .await?
            .map(CheckpointRow::into_checkpoint)
            .transpose()
    }
}

async fn fetch_latest_checkpoint_row<'e, E>(
    executor: E,
    task_id: Uuid,
) -> Result<Option<CheckpointRow>>
where
    E: Executor<'e, Database = Sqlite>,
{
    let row = sqlx::query_as::<_, CheckpointRow>(
        r#"
        SELECT
            c.checkpoint_id,
            c.task_id,
            c.state_version_ref,
            c.event_sequence_number,
            c.status,
            c.created_at,
            c.created_by,
            c.payload
        FROM checkpoints c
        INNER JOIN task_state_versions s
            ON s.state_version_id = c.state_version_ref
           AND s.task_id = c.task_id
        WHERE c.task_id = ?
        ORDER BY c.event_sequence_number DESC
        LIMIT 1
        "#,
    )
    .bind(task_id.to_string())
    .fetch_optional(executor)
    .await?;

    Ok(row)
}

async fn fetch_checkpoint_row_by_id<'e, E>(
    executor: E,
    task_id: Uuid,
    checkpoint_id: Uuid,
) -> Result<Option<CheckpointRow>>
where
    E: Executor<'e, Database = Sqlite>,
{
    let row = sqlx::query_as::<_, CheckpointRow>(
        r#"
        SELECT
            c.checkpoint_id,
            c.task_id,
            c.state_version_ref,
            c.event_sequence_number,
            c.status,
            c.created_at,
            c.created_by,
            c.payload
        FROM checkpoints c
        INNER JOIN task_state_versions s
            ON s.state_version_id = c.state_version_ref
           AND s.task_id = c.task_id
        WHERE c.task_id = ?
          AND c.checkpoint_id = ?
        LIMIT 1
        "#,
    )
    .bind(task_id.to_string())
    .bind(checkpoint_id.to_string())
    .fetch_optional(executor)
    .await?;

    Ok(row)
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
