use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{FromRow, Sqlite, SqlitePool, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::db::{decode_timestamp, encode_timestamp, parse_uuid};
use crate::error::Result;
use crate::task::TaskStatus;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateVersionRef {
    pub state_version_id: Uuid,
    pub task_id: Uuid,
    pub version_number: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskStateVersion {
    pub state_version_ref: StateVersionRef,
    pub status: TaskStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub created_by: String,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct NewStateVersion {
    pub state_version_id: Uuid,
    pub task_id: Uuid,
    pub status: TaskStatus,
    pub payload: Value,
    pub created_at: OffsetDateTime,
    pub created_by: String,
}

#[derive(Clone, Default)]
pub struct StateStore;

impl StateStore {
    pub async fn append_version(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        input: NewStateVersion,
    ) -> Result<TaskStateVersion> {
        let next_version: i64 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(MAX(version_number), 0) + 1
            FROM task_state_versions
            WHERE task_id = ?
            "#,
        )
        .bind(input.task_id.to_string())
        .fetch_one(&mut **tx)
        .await?;

        let payload = serde_json::to_string(&input.payload)?;
        let created_at = encode_timestamp(input.created_at)?;

        sqlx::query(
            r#"
            INSERT INTO task_state_versions (
                state_version_id,
                task_id,
                version_number,
                status,
                payload,
                created_at,
                created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(input.state_version_id.to_string())
        .bind(input.task_id.to_string())
        .bind(next_version)
        .bind(input.status.as_str())
        .bind(payload)
        .bind(&created_at)
        .bind(&input.created_by)
        .execute(&mut **tx)
        .await?;

        Ok(TaskStateVersion {
            state_version_ref: StateVersionRef {
                state_version_id: input.state_version_id,
                task_id: input.task_id,
                version_number: next_version,
            },
            status: input.status,
            created_at: input.created_at,
            created_by: input.created_by,
            payload: input.payload,
        })
    }

    pub async fn get_by_ref(
        &self,
        pool: &SqlitePool,
        state_version_id: Uuid,
    ) -> Result<TaskStateVersion> {
        self.get_by_ref_in(pool, state_version_id).await
    }

    pub async fn get_by_ref_in<'e, E>(
        &self,
        executor: E,
        state_version_id: Uuid,
    ) -> Result<TaskStateVersion>
    where
        E: sqlx::Executor<'e, Database = Sqlite>,
    {
        let row = sqlx::query_as::<_, StateVersionRow>(
            r#"
            SELECT state_version_id, task_id, version_number, status, payload, created_at, created_by
            FROM task_state_versions
            WHERE state_version_id = ?
            "#,
        )
        .bind(state_version_id.to_string())
        .fetch_one(executor)
        .await?;

        row.into_state_version()
    }
}

#[derive(Debug, FromRow)]
struct StateVersionRow {
    state_version_id: String,
    task_id: String,
    version_number: i64,
    status: String,
    payload: String,
    created_at: String,
    created_by: String,
}

impl StateVersionRow {
    fn into_state_version(self) -> Result<TaskStateVersion> {
        let Self {
            state_version_id,
            task_id,
            version_number,
            status,
            payload,
            created_at,
            created_by,
        } = self;

        let task_id = parse_uuid("task_id", &task_id)?;
        Ok(TaskStateVersion {
            state_version_ref: StateVersionRef {
                state_version_id: parse_uuid("state_version_id", &state_version_id)?,
                task_id,
                version_number,
            },
            status: status.parse()?,
            created_at: decode_timestamp(&created_at)?,
            created_by,
            payload: serde_json::from_str(&payload)?,
        })
    }
}
