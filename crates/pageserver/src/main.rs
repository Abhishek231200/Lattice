use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

use lattice_pageserver::{
    api::{router, PageserverState},
    config::{PageserverConfig, StorageConfig},
    metrics,
    redo::RedoEngine,
    store::{BlobLayerStorage, LayerStorage},
    timeline::TimelineManager,
};
use lattice_common::blob_store::LocalFsStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .with_target(true)
        .init();

    metrics::init();

    let config = PageserverConfig::default();

    // Build storage backend.
    let blob_store: Arc<dyn lattice_common::BlobStore> = match &config.storage {
        StorageConfig::LocalFs { path } => {
            info!("using local FS storage at {}", path.display());
            std::fs::create_dir_all(path)?;
            Arc::new(LocalFsStore::new(path)?)
        }
        StorageConfig::S3 { endpoint, bucket, .. } => {
            info!("using S3-compatible storage: endpoint={endpoint} bucket={bucket}");
            // S3 impl is wired in Phase 2 — fall back to local FS for now.
            Arc::new(LocalFsStore::new("/tmp/lattice/s3-local")?)
        }
    };

    let layer_storage: Arc<dyn LayerStorage> =
        Arc::new(BlobLayerStorage::new(blob_store));

    let timeline_manager = Arc::new(TimelineManager::new(layer_storage));
    let redo = Arc::new(RedoEngine::new());

    let state = PageserverState {
        timelines: timeline_manager,
        redo,
    };

    let addr = config.listen_addr.parse::<std::net::SocketAddr>()?;
    info!("pageserver listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(state)).await?;

    Ok(())
}
