use std::str::FromStr;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::SqlitePool;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::error::{Result, RuntimeError};

#[derive(Clone)]
pub struct RuntimeDb {
    pool: SqlitePool,
}

impl RuntimeDb {
    pub async fn connect(database_url: &str) -> Result<Self> {
        Self::connect_internal(database_url, true, false).await
    }

    pub async fn connect_existing(database_url: &str) -> Result<Self> {
        Self::connect_internal(database_url, false, false).await
    }

    pub async fn connect_read_only(database_url: &str) -> Result<Self> {
        Self::connect_internal(database_url, false, true).await
    }

    async fn connect_internal(
        database_url: &str,
        create_if_missing: bool,
        read_only: bool,
    ) -> Result<Self> {
        let is_memory = is_memory_url(database_url);
        let mut options = SqliteConnectOptions::from_str(database_url)
            .map_err(|error| {
                RuntimeError::InvariantViolation(format!("invalid sqlite url: {error}"))
            })?
            .foreign_keys(true)
            .create_if_missing(create_if_missing && !is_memory)
            .read_only(read_only && !is_memory);

        if !read_only {
            options = options
                .journal_mode(if is_memory {
                    SqliteJournalMode::Memory
                } else {
                    SqliteJournalMode::Wal
                })
                .synchronous(SqliteSynchronous::Full);
        }

        // v0.1 intentionally serializes access through one SQLite connection so task-local
        // state_version/event sequence allocation remains deterministic without cross-process locks.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;

        Ok(Self { pool })
    }

    pub async fn connect_and_migrate(database_url: &str) -> Result<Self> {
        let db = Self::connect(database_url).await?;
        db.migrate().await?;
        Ok(db)
    }

    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub fn clone_pool(&self) -> SqlitePool {
        self.pool.clone()
    }
}

pub fn encode_timestamp(value: OffsetDateTime) -> Result<String> {
    value
        .format(&Rfc3339)
        .map_err(|error| RuntimeError::Time(error.to_string()))
}

pub fn decode_timestamp(value: &str) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339).map_err(|error| RuntimeError::Time(error.to_string()))
}

pub fn parse_uuid(field: &'static str, value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|_| RuntimeError::InvalidUuid {
        field,
        value: value.to_owned(),
    })
}

pub fn parse_optional_uuid(field: &'static str, value: Option<String>) -> Result<Option<Uuid>> {
    value.map(|item| parse_uuid(field, &item)).transpose()
}

fn is_memory_url(database_url: &str) -> bool {
    database_url.contains(":memory:") || database_url.contains("mode=memory")
}
