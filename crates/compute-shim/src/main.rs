use std::sync::Arc;
use std::str::FromStr;
use tracing::info;
use tracing_subscriber::EnvFilter;

use lattice_common::{TenantId, TimelineId};
use lattice_compute_shim::{
    page_cache::PageCache,
    pageserver_client::PageserverClient,
    smgr_proxy::{router, SmgrProxyState},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    let pageserver_url = std::env::var("PAGESERVER_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:6400".to_string());
    let tenant_id: TenantId = std::env::var("TENANT_ID")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(TenantId::new);
    let timeline_id: TimelineId = std::env::var("TIMELINE_ID")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(TimelineId::new);

    info!(
        %pageserver_url, %tenant_id, %timeline_id,
        "starting compute shim"
    );

    let client = Arc::new(PageserverClient::new(pageserver_url, tenant_id, timeline_id));
    let cache = Arc::new(PageCache::new(4096)); // 32 MiB cache

    let state = SmgrProxyState { client, cache };

    let addr = "127.0.0.1:6403".parse::<std::net::SocketAddr>()?;
    info!("compute-shim listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(state)).await?;

    Ok(())
}
