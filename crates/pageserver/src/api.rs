/// Pageserver HTTP API.
///
/// Endpoints:
///   GET  /health
///   GET  /metrics
///   POST /page              — get_page_at_lsn
///   POST /page/put          — put_page (for compute shim / tests)
///   POST /timelines         — create timeline
///   POST /timelines/branch  — create branch
///   GET  /timelines/{id}    — timeline info

use std::sync::Arc;
use axum::{
    Router,
    routing::{get, post},
    extract::{State, Path, Json},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use tracing::error;

use lattice_common::{Lsn, TenantId, TimelineId, RelTag, PageImage, PageVersion};
use lattice_common::proto::{GetPageRequest, GetPageResponse, PutPageRequest};

use crate::timeline::TimelineManager;
use crate::redo::RedoEngine;
use crate::metrics;

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct PageserverState {
    pub timelines: Arc<TimelineManager>,
    pub redo: Arc<RedoEngine>,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn router(state: PageserverState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(get_metrics))
        .route("/page", post(get_page))
        .route("/page/put", post(put_page))
        .route("/timelines", post(create_timeline))
        .route("/timelines/branch", post(create_branch))
        .route("/timelines/:id", get(timeline_info))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn get_metrics() -> impl IntoResponse {
    metrics::metrics_text()
}

async fn get_page(
    State(state): State<PageserverState>,
    Json(req): Json<GetPageRequest>,
) -> impl IntoResponse {
    let start = std::time::Instant::now();
    let tl_str = req.timeline_id.to_string();

    match state.timelines.get_timeline(req.tenant_id, req.timeline_id) {
        Err(e) => {
            error!("timeline not found: {e}");
            (StatusCode::NOT_FOUND, e.to_string()).into_response()
        }
        Ok(tl) => {
            match tl.get_page_at_lsn(&state.redo, req.rel, req.blk, req.lsn) {
                Ok(page) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    metrics::record_get_page(&tl_str, tl.meta.parent_id.is_some(), elapsed, true);
                    Json(GetPageResponse {
                        page: page.0.to_vec(),
                        effective_lsn: tl.last_lsn(),
                    }).into_response()
                }
                Err(e) => {
                    metrics::record_get_page(&tl_str, false, 0.0, false);
                    (StatusCode::NOT_FOUND, e.to_string()).into_response()
                }
            }
        }
    }
}

async fn put_page(
    State(state): State<PageserverState>,
    Json(req): Json<PutPageRequest>,
) -> impl IntoResponse {
    match state.timelines.get_timeline(req.tenant_id, req.timeline_id) {
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
        Ok(tl) => {
            if let Ok(img) = bytes::Bytes::from(req.page).try_into_image() {
                tl.put_image(req.rel, req.blk, req.lsn, img);
            }
            StatusCode::NO_CONTENT.into_response()
        }
    }
}

#[derive(Deserialize)]
struct CreateTimelineReq {
    tenant_id: TenantId,
    name: String,
}

#[derive(Serialize)]
struct CreateTimelineResp {
    timeline_id: TimelineId,
}

async fn create_timeline(
    State(state): State<PageserverState>,
    Json(req): Json<CreateTimelineReq>,
) -> impl IntoResponse {
    let tl = state.timelines.create_timeline(req.tenant_id, req.name);
    Json(CreateTimelineResp { timeline_id: tl.id() })
}

#[derive(Deserialize)]
struct CreateBranchReq {
    tenant_id: TenantId,
    parent_timeline_id: TimelineId,
    at_lsn: Lsn,
    name: String,
}

#[derive(Serialize)]
struct CreateBranchResp {
    timeline_id: TimelineId,
    elapsed_us: u64,
}

async fn create_branch(
    State(state): State<PageserverState>,
    Json(req): Json<CreateBranchReq>,
) -> impl IntoResponse {
    match state.timelines.create_branch(
        req.parent_timeline_id,
        req.tenant_id,
        req.at_lsn,
        req.name,
    ) {
        Ok((tl, elapsed)) => Json(CreateBranchResp {
            timeline_id: tl.id(),
            elapsed_us: elapsed.as_micros() as u64,
        }).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

#[derive(Serialize)]
struct TimelineInfoResp {
    id: TimelineId,
    parent_id: Option<TimelineId>,
    branch_lsn: Lsn,
    last_lsn: Lsn,
    name: String,
}

async fn timeline_info(
    State(state): State<PageserverState>,
    Path(id_str): Path<String>,
) -> impl IntoResponse {
    // For demo purposes, scan all tenants.  Production would pass tenant_id as a header.
    let id: TimelineId = match id_str.parse() {
        Ok(id) => id,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    // Placeholder — a real impl would index by TimelineId directly.
    (StatusCode::NOT_IMPLEMENTED, "use tenant-scoped endpoint").into_response()
}

// ---------------------------------------------------------------------------
// Helper extension trait
// ---------------------------------------------------------------------------

trait TryIntoImage {
    fn try_into_image(self) -> Result<PageImage, String>;
}

impl TryIntoImage for bytes::Bytes {
    fn try_into_image(self) -> Result<PageImage, String> {
        if self.len() != lattice_common::PAGE_SIZE {
            Err(format!("expected {} bytes, got {}", lattice_common::PAGE_SIZE, self.len()))
        } else {
            Ok(PageImage::new(self))
        }
    }
}
