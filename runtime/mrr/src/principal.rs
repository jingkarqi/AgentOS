use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use sqlx::SqlitePool;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::db::{decode_timestamp, encode_timestamp, parse_uuid};
use crate::error::{Result, RuntimeError};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalStatus {
    Active,
    Suspended,
    Revoked,
}

impl PrincipalStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Suspended => "suspended",
            Self::Revoked => "revoked",
        }
    }
}

impl std::str::FromStr for PrincipalStatus {
    type Err = RuntimeError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "active" => Ok(Self::Active),
            "suspended" => Ok(Self::Suspended),
            "revoked" => Ok(Self::Revoked),
            _ => Err(RuntimeError::InvalidEnumValue {
                kind: "principal_status",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Principal {
    pub principal_id: Uuid,
    pub principal_type: String,
    pub display_name: String,
    pub authority_scope_ref: String,
    pub status: PrincipalStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrincipalAttribution {
    pub principal_id: Uuid,
    pub principal_role: String,
}

impl PrincipalAttribution {
    pub fn owning(principal_id: Uuid) -> Self {
        Self {
            principal_id,
            principal_role: "owning".to_owned(),
        }
    }

    pub fn acting(principal_id: Uuid) -> Self {
        Self {
            principal_id,
            principal_role: "acting".to_owned(),
        }
    }
}

#[derive(Clone)]
pub struct PrincipalStore {
    pool: SqlitePool,
}

impl PrincipalStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn upsert(&self, principal: &Principal) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO principals (
                principal_id,
                principal_type,
                display_name,
                authority_scope_ref,
                status,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(principal_id) DO UPDATE SET
                principal_type = excluded.principal_type,
                display_name = excluded.display_name,
                authority_scope_ref = excluded.authority_scope_ref,
                status = excluded.status,
                created_at = excluded.created_at
            "#,
        )
        .bind(principal.principal_id.to_string())
        .bind(&principal.principal_type)
        .bind(&principal.display_name)
        .bind(&principal.authority_scope_ref)
        .bind(principal.status.as_str())
        .bind(encode_timestamp(principal.created_at)?)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get(&self, principal_id: Uuid) -> Result<Principal> {
        let row = sqlx::query_as::<_, PrincipalRow>(
            r#"
            SELECT principal_id, principal_type, display_name, authority_scope_ref, status, created_at
            FROM principals
            WHERE principal_id = ?
            "#,
        )
        .bind(principal_id.to_string())
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => row.into_principal(),
            None => Err(RuntimeError::PrincipalNotFound { principal_id }),
        }
    }
}

#[derive(Debug, FromRow)]
struct PrincipalRow {
    principal_id: String,
    principal_type: String,
    display_name: String,
    authority_scope_ref: String,
    status: String,
    created_at: String,
}

impl PrincipalRow {
    fn into_principal(self) -> Result<Principal> {
        let Self {
            principal_id,
            principal_type,
            display_name,
            authority_scope_ref,
            status,
            created_at,
        } = self;

        Ok(Principal {
            principal_id: parse_uuid("principal_id", &principal_id)?,
            principal_type,
            display_name,
            authority_scope_ref,
            status: status.parse()?,
            created_at: decode_timestamp(&created_at)?,
        })
    }
}
