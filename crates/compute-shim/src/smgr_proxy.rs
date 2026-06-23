/// Storage manager proxy — implements the shim-path (Path A) for Phase 5.
///
/// Listens on a local port for page requests from the Postgres storage manager
/// hook (lattice_smgr C extension, Path B) and serves them from the pageserver
/// via `PageserverClient`, with a local `PageCache` in front.
///
/// For end-to-end demo without the C extension, `SmgrProxy` also exposes a
/// minimal "relation scan" API so a thin Rust query layer can drive it.

use std::sync::Arc;
use axum::{
    Router,
    routing::{get, post},
    extract::{State, Json},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use tracing::debug;

use lattice_common::{Lsn, RelTag, BlockNumber, PageImage, PAGE_SIZE};
use lattice_common::proto::GetPageRequest;

use crate::page_cache::{PageCache, CacheKey};
use crate::pageserver_client::PageserverClient;

#[derive(Clone)]
pub struct SmgrProxyState {
    pub client: Arc<PageserverClient>,
    pub cache: Arc<PageCache>,
}

pub fn router(state: SmgrProxyState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/smgr/page", post(smgr_get_page))
        .route("/smgr/stats", get(smgr_stats))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    axum::Json(serde_json::json!({ "status": "ok", "service": "compute-shim" }))
}

#[derive(Deserialize)]
struct SmgrPageReq {
    rel_spcnode: u32,
    rel_dbnode: u32,
    rel_relnode: u32,
    forknum: u8,
    blkno: u32,
    lsn: u64,
}

#[derive(Serialize)]
struct SmgrPageResp {
    /// Raw page bytes, base64-encoded for JSON transport.
    page_b64: String,
    cache_hit: bool,
}

async fn smgr_get_page(
    State(state): State<SmgrProxyState>,
    Json(req): Json<SmgrPageReq>,
) -> impl IntoResponse {
    let rel = RelTag::new(req.rel_spcnode, req.rel_dbnode, req.rel_relnode, req.forknum);
    let lsn = Lsn(req.lsn);
    let key = CacheKey { rel, blk: req.blkno, lsn };

    // Cache hit?
    if let Some(page) = state.cache.get(&key) {
        debug!(rel = %rel, blk = req.blkno, "page cache hit");
        return Json(SmgrPageResp {
            page_b64: base64_encode(page.as_bytes()),
            cache_hit: true,
        }).into_response();
    }

    // Fetch from pageserver.
    match state.client.get_page(rel, req.blkno, lsn).await {
        Ok(page) => {
            state.cache.put(key, page.clone());
            Json(SmgrPageResp {
                page_b64: base64_encode(page.as_bytes()),
                cache_hit: false,
            }).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn smgr_stats(State(state): State<SmgrProxyState>) -> impl IntoResponse {
    axum::Json(serde_json::json!({
        "cache_hit_rate": state.cache.hit_rate(),
    }))
}

fn base64_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    // Simple base64 without external dep.
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = if chunk.len() > 1 { chunk[1] as usize } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as usize } else { 0 };
        out.push(TABLE[(b0 >> 2)] as char);
        out.push(TABLE[((b0 & 3) << 4) | (b1 >> 4)] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((b1 & 0xF) << 2) | (b2 >> 6)] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[b2 & 0x3F] as char);
        } else {
            out.push('=');
        }
    }
    out
}
