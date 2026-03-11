use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("time error: {0}")]
    Time(String),
    #[error("task {task_id} not found")]
    TaskNotFound { task_id: Uuid },
    #[error("principal {principal_id} not found")]
    PrincipalNotFound { principal_id: Uuid },
    #[error("task {task_id} has no checkpoint to restore")]
    NoCheckpointForTask { task_id: Uuid },
    #[error("checkpoint {checkpoint_id} not found for task {task_id}")]
    CheckpointNotFound { task_id: Uuid, checkpoint_id: Uuid },
    #[error("principal {principal_id} is not authorized to mutate task {task_id}")]
    UnauthorizedTaskAction { task_id: Uuid, principal_id: Uuid },
    #[error("invalid task transition for {task_id}: {from_status} -> {to_status}")]
    InvalidTransition {
        task_id: Uuid,
        from_status: String,
        to_status: String,
    },
    #[error("invalid enum value for {kind}: {value}")]
    InvalidEnumValue { kind: &'static str, value: String },
    #[error("invalid uuid for {field}: {value}")]
    InvalidUuid { field: &'static str, value: String },
    #[error("invariant violation: {0}")]
    InvariantViolation(String),
}

pub type Result<T> = std::result::Result<T, RuntimeError>;
