use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::error::{Result, RuntimeError};
use crate::state_store::StateVersionRef;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Created,
    Ready,
    Running,
    WaitingOnCapability,
    WaitingOnPolicy,
    WaitingOnControl,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::WaitingOnCapability => "waiting_on_capability",
            Self::WaitingOnPolicy => "waiting_on_policy",
            Self::WaitingOnControl => "waiting_on_control",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl std::str::FromStr for TaskStatus {
    type Err = RuntimeError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "created" => Ok(Self::Created),
            "ready" => Ok(Self::Ready),
            "running" => Ok(Self::Running),
            "waiting_on_capability" => Ok(Self::WaitingOnCapability),
            "waiting_on_policy" => Ok(Self::WaitingOnPolicy),
            "waiting_on_control" => Ok(Self::WaitingOnControl),
            "paused" => Ok(Self::Paused),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(RuntimeError::InvalidEnumValue {
                kind: "task_status",
                value: value.to_owned(),
            }),
        }
    }
}

impl TaskStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Task {
    pub task_id: Uuid,
    pub goal: String,
    pub status: TaskStatus,
    pub owner_principal_id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    pub policy_context_ref: String,
    pub working_state_ref: StateVersionRef,
    pub history_ref: String,
    pub checkpoint_ref: Option<Uuid>,
    pub budget_context_ref: String,
    pub priority: i32,
    pub result_ref: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CreateTaskCommand {
    pub goal: String,
    pub owner_principal_id: Uuid,
    pub policy_context_ref: String,
    pub budget_context_ref: String,
    pub priority: i32,
    pub initial_state: Value,
}

impl CreateTaskCommand {
    pub fn new(goal: impl Into<String>, owner_principal_id: Uuid) -> Self {
        Self {
            goal: goal.into(),
            owner_principal_id,
            policy_context_ref: "policy/default".to_owned(),
            budget_context_ref: "budget/default".to_owned(),
            priority: 0,
            initial_state: json!({}),
        }
    }
}
