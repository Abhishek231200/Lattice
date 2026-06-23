/// Postgres metadata DB — stores tenants, timelines, endpoints, and scaling decisions.
/// Using sqlx with compile-time checked queries (macros disabled so we don't need a live DB
/// at build time; switched to `query_as!` when DATABASE_URL is set in CI).

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

use lattice_common::{Lsn, TenantId, TimelineId};
use lattice_common::proto::EndpointState;
use crate::autoscaler::ScalingDecision;

pub struct ControlPlaneDb {
    pool: PgPool,
}

impl ControlPlaneDb {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS tenants (
                tenant_id   TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS timelines (
                timeline_id TEXT PRIMARY KEY,
                tenant_id   TEXT NOT NULL REFERENCES tenants(tenant_id),
                parent_id   TEXT,
                branch_lsn  BIGINT NOT NULL DEFAULT 0,
                last_lsn    BIGINT NOT NULL DEFAULT 0,
                name        TEXT NOT NULL,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS endpoints (
                endpoint_id TEXT PRIMARY KEY,
                tenant_id   TEXT NOT NULL REFERENCES tenants(tenant_id),
                timeline_id TEXT NOT NULL REFERENCES timelines(timeline_id),
                name        TEXT NOT NULL,
                state       TEXT NOT NULL DEFAULT 'starting',
                cpu_millis  INT NOT NULL DEFAULT 1000,
                memory_mb   INT NOT NULL DEFAULT 512,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                suspended_at TIMESTAMPTZ
            );

            CREATE TABLE IF NOT EXISTS scaling_decisions (
                id          BIGSERIAL PRIMARY KEY,
                endpoint_id TEXT NOT NULL,
                action      TEXT NOT NULL,
                reason      TEXT NOT NULL,
                from_units  INT NOT NULL,
                to_units    INT NOT NULL,
                timestamp   TIMESTAMPTZ NOT NULL
            );
            "#
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Tenant operations
    // ---------------------------------------------------------------------------

    pub async fn create_tenant(&self, tenant_id: TenantId, name: &str) -> Result<()> {
        sqlx::query("INSERT INTO tenants (tenant_id, name) VALUES ($1, $2)")
            .bind(tenant_id.to_string())
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list_tenants(&self) -> Result<Vec<TenantRow>> {
        let rows = sqlx::query("SELECT tenant_id, name, created_at FROM tenants ORDER BY created_at")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(|r| TenantRow {
            tenant_id: r.get("tenant_id"),
            name: r.get("name"),
            created_at: r.get("created_at"),
        }).collect())
    }

    // ---------------------------------------------------------------------------
    // Timeline operations
    // ---------------------------------------------------------------------------

    pub async fn create_timeline(
        &self,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        parent_id: Option<TimelineId>,
        branch_lsn: Lsn,
        name: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO timelines (timeline_id, tenant_id, parent_id, branch_lsn, name) VALUES ($1, $2, $3, $4, $5)"
        )
        .bind(timeline_id.to_string())
        .bind(tenant_id.to_string())
        .bind(parent_id.map(|i| i.to_string()))
        .bind(branch_lsn.as_u64() as i64)
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_last_lsn(&self, timeline_id: TimelineId, lsn: Lsn) -> Result<()> {
        sqlx::query("UPDATE timelines SET last_lsn = $1 WHERE timeline_id = $2")
            .bind(lsn.as_u64() as i64)
            .bind(timeline_id.to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Endpoint operations
    // ---------------------------------------------------------------------------

    pub async fn create_endpoint(
        &self,
        endpoint_id: &str,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        name: &str,
        cpu_millis: i32,
        memory_mb: i32,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO endpoints (endpoint_id, tenant_id, timeline_id, name, cpu_millis, memory_mb)
             VALUES ($1, $2, $3, $4, $5, $6)"
        )
        .bind(endpoint_id)
        .bind(tenant_id.to_string())
        .bind(timeline_id.to_string())
        .bind(name)
        .bind(cpu_millis)
        .bind(memory_mb)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_endpoint_state(&self, endpoint_id: &str, state: &str) -> Result<()> {
        sqlx::query("UPDATE endpoints SET state = $1 WHERE endpoint_id = $2")
            .bind(state)
            .bind(endpoint_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn record_scaling_decision(&self, d: &ScalingDecision) -> Result<()> {
        sqlx::query(
            "INSERT INTO scaling_decisions (endpoint_id, action, reason, from_units, to_units, timestamp)
             VALUES ($1, $2, $3, $4, $5, $6)"
        )
        .bind(&d.endpoint_id)
        .bind(format!("{:?}", d.action))
        .bind(&d.reason)
        .bind(d.from_units as i32)
        .bind(d.to_units as i32)
        .bind(d.timestamp)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_scaling_decisions(&self, endpoint_id: &str, limit: i64) -> Result<Vec<ScalingDecisionRow>> {
        let rows = sqlx::query(
            "SELECT action, reason, from_units, to_units, timestamp
             FROM scaling_decisions WHERE endpoint_id = $1
             ORDER BY timestamp DESC LIMIT $2"
        )
        .bind(endpoint_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| ScalingDecisionRow {
            action: r.get("action"),
            reason: r.get("reason"),
            from_units: r.get::<i32, _>("from_units") as u32,
            to_units: r.get::<i32, _>("to_units") as u32,
            timestamp: r.get("timestamp"),
        }).collect())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TenantRow {
    pub tenant_id: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScalingDecisionRow {
    pub action: String,
    pub reason: String,
    pub from_units: u32,
    pub to_units: u32,
    pub timestamp: DateTime<Utc>,
}
