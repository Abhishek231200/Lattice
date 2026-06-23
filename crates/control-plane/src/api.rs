/// Control plane REST API — Phase 6.
///
/// Endpoints:
///   POST /tenants                   — create tenant
///   GET  /tenants                   — list tenants
///   POST /branches                  — create branch (COW, O(1))
///   POST /endpoints                 — start compute endpoint
///   DELETE /endpoints/:id           — stop endpoint
///   POST /endpoints/:id/suspend     — suspend endpoint
///   POST /endpoints/:id/resume      — resume endpoint
///   GET  /endpoints/:id/metrics     — scaling decisions + current metrics
///   GET  /metrics                   — Prometheus exposition

use std::sync::Arc;
use std::time::Instant;

use axum::{
    Router,
    routing::{get, post, delete},
    extract::{State, Path, Json},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use tracing::{info, error};
use uuid::Uuid;

use lattice_common::{Lsn, TenantId, TimelineId};
use lattice_common::proto::{
    CreateTenantRequest, CreateTenantResponse,
    CreateBranchRequest, CreateBranchResponse,
    StartEndpointRequest,
};

use crate::autoscaler::{Autoscaler, ScalingDecision};
use crate::compute::{ComputeOrchestrator, ComputeSpec};
use crate::db::ControlPlaneDb;
use crate::metrics::PrometheusClient;

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct ControlPlaneState {
    pub db: Arc<ControlPlaneDb>,
    pub autoscaler: Arc<Autoscaler>,
    pub orchestrator: Arc<dyn ComputeOrchestrator>,
    pub prometheus: Arc<PrometheusClient>,
    pub pageserver_url: String,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn router(state: ControlPlaneState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(get_metrics))
        .route("/tenants", post(create_tenant))
        .route("/tenants", get(list_tenants))
        .route("/branches", post(create_branch))
        .route("/endpoints", post(start_endpoint))
        .route("/endpoints/:id", delete(stop_endpoint))
        .route("/endpoints/:id/suspend", post(suspend_endpoint))
        .route("/endpoints/:id/resume", post(resume_endpoint))
        .route("/endpoints/:id/metrics", get(endpoint_metrics))
        .route("/autoscaler/decisions", get(scaling_decisions))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok", "service": "control-plane" }))
}

async fn get_metrics() -> impl IntoResponse {
    use prometheus::{Encoder, TextEncoder};
    let encoder = TextEncoder::new();
    let families = prometheus::gather();
    let mut buf = Vec::new();
    encoder.encode(&families, &mut buf).unwrap_or_default();
    String::from_utf8(buf).unwrap_or_default()
}

async fn create_tenant(
    State(state): State<ControlPlaneState>,
    Json(req): Json<CreateTenantRequest>,
) -> impl IntoResponse {
    let tenant_id = TenantId::new();
    match state.db.create_tenant(tenant_id, &req.name).await {
        Ok(_) => {
            info!(tenant = %tenant_id, name = %req.name, "tenant created");
            Json(CreateTenantResponse { tenant_id }).into_response()
        }
        Err(e) => {
            error!("create tenant: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

async fn list_tenants(State(state): State<ControlPlaneState>) -> impl IntoResponse {
    match state.db.list_tenants().await {
        Ok(tenants) => Json(tenants).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn create_branch(
    State(state): State<ControlPlaneState>,
    Json(req): Json<CreateBranchRequest>,
) -> impl IntoResponse {
    let start = Instant::now();

    // The branch is O(1): we create a metadata record in the DB and tell the
    // pageserver to register the new timeline (it creates zero data).
    let timeline_id = TimelineId::new();
    let branch_lsn = req.branch_lsn.unwrap_or(Lsn::INVALID);

    // Persist to DB.
    if let Err(e) = state.db.create_timeline(
        timeline_id,
        req.tenant_id,
        req.parent_timeline_id,
        branch_lsn,
        &req.name,
    ).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }

    // Notify pageserver to register the new timeline.
    let client = reqwest::Client::new();
    let _ = client.post(format!("{}/timelines/branch", state.pageserver_url))
        .json(&serde_json::json!({
            "tenant_id": req.tenant_id,
            "parent_timeline_id": req.parent_timeline_id,
            "at_lsn": branch_lsn,
            "name": req.name,
        }))
        .send()
        .await;

    let elapsed_us = start.elapsed().as_micros() as u64;
    info!(
        timeline = %timeline_id,
        parent = ?req.parent_timeline_id,
        branch_lsn = %branch_lsn,
        elapsed_us,
        "branch created"
    );

    Json(CreateBranchResponse { timeline_id, elapsed_us }).into_response()
}

async fn start_endpoint(
    State(state): State<ControlPlaneState>,
    Json(req): Json<StartEndpointRequest>,
) -> impl IntoResponse {
    let endpoint_id = Uuid::new_v4().to_string();
    let spec = ComputeSpec {
        endpoint_id: endpoint_id.clone(),
        tenant_id: req.tenant_id,
        timeline_id: req.timeline_id,
        cpu_millis: req.cpu_millis,
        memory_mb: req.memory_mb,
        pageserver_url: state.pageserver_url.clone(),
    };

    match state.orchestrator.start(spec).await {
        Ok(info) => {
            let _ = state.db.create_endpoint(
                &endpoint_id,
                req.tenant_id,
                req.timeline_id,
                &req.name,
                req.cpu_millis as i32,
                req.memory_mb as i32,
            ).await;
            state.autoscaler.register_endpoint(&endpoint_id, req.cpu_millis / 1000);
            info!(endpoint = %endpoint_id, "endpoint started");
            Json(serde_json::json!({ "endpoint_id": endpoint_id, "state": "starting", "host": info.host, "port": info.port })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn stop_endpoint(
    State(state): State<ControlPlaneState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.orchestrator.stop(&id).await {
        Ok(_) => {
            let _ = state.db.update_endpoint_state(&id, "stopped").await;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn suspend_endpoint(
    State(state): State<ControlPlaneState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.orchestrator.suspend(&id).await {
        Ok(_) => {
            let _ = state.db.update_endpoint_state(&id, "suspended").await;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn resume_endpoint(
    State(state): State<ControlPlaneState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.orchestrator.resume(&id).await {
        Ok(info) => {
            let _ = state.db.update_endpoint_state(&id, "active").await;
            Json(serde_json::json!({ "endpoint_id": id, "state": "active", "host": info.host })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn endpoint_metrics(
    State(state): State<ControlPlaneState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let decisions = state.db.list_scaling_decisions(&id, 100).await.unwrap_or_default();
    let current = state.prometheus.query_endpoint_metrics(&id).await;
    Json(serde_json::json!({
        "endpoint_id": id,
        "current": current.ok().map(|m| serde_json::json!({
            "active_connections": m.active_connections,
            "cpu_util": m.cpu_util,
            "p99_latency_ms": m.p99_latency_ms,
            "query_rate_per_sec": m.query_rate_per_sec,
        })),
        "decisions": decisions,
    })).into_response()
}

async fn scaling_decisions(State(state): State<ControlPlaneState>) -> impl IntoResponse {
    let decisions = state.autoscaler.decisions();
    Json(decisions).into_response()
}
