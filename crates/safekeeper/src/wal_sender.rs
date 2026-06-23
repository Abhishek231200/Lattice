/// WAL sender — serves stored WAL records to the pageserver on demand.
/// Exposes an HTTP endpoint: GET /wal/{tenant}/{timeline}?from_lsn={lsn}

use axum::{
    Router,
    routing::get,
    extract::{Path, Query, State},
    Json,
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::HashMap;

use lattice_common::{Lsn, TenantId, TimelineId};
use lattice_common::proto::WalRecord;

use crate::wal_store::WalStore;

#[derive(Clone)]
pub struct SafekeeperState {
    /// tenant_id -> timeline_id -> WalStore
    stores: Arc<RwLock<HashMap<(TenantId, TimelineId), WalStore>>>,
}

impl SafekeeperState {
    pub fn new() -> Self {
        Self {
            stores: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register(&self, tenant: TenantId, timeline: TimelineId, store: WalStore) {
        self.stores.write().insert((tenant, timeline), store);
    }
}

#[derive(Deserialize)]
struct WalQuery {
    from_lsn: u64,
}

pub fn router(state: SafekeeperState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/wal/:tenant/:timeline", get(get_wal))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn get_wal(
    State(state): State<SafekeeperState>,
    Path((tenant_str, timeline_str)): Path<(String, String)>,
    Query(q): Query<WalQuery>,
) -> impl IntoResponse {
    let tenant: TenantId = match tenant_str.parse() {
        Ok(t) => t,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid tenant_id").into_response(),
    };
    let timeline: TimelineId = match timeline_str.parse() {
        Ok(t) => t,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid timeline_id").into_response(),
    };

    let store = match state.stores.read().get(&(tenant, timeline)).cloned() {
        Some(s) => s,
        None => return (StatusCode::NOT_FOUND, "timeline not found").into_response(),
    };

    match store.read_from(Lsn(q.from_lsn)) {
        Ok(records) => Json(records).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
