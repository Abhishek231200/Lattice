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
use lattice_common::blob_store::{LocalFsStore, S3BlobStore, GcsBlobStore, AzureBlobStore};

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
        StorageConfig::S3 { endpoint, bucket, region, access_key, secret_key } => {
            if endpoint.is_empty() {
                info!("using AWS S3: region={region} bucket={bucket}");
                Arc::new(S3BlobStore::new_aws(region, bucket, access_key, secret_key)?)
            } else {
                info!("using S3-compatible (MinIO): endpoint={endpoint} bucket={bucket}");
                Arc::new(S3BlobStore::new_minio(endpoint, bucket, access_key, secret_key)?)
            }
        }
        StorageConfig::Gcs { bucket, service_account_key } => {
            if service_account_key.is_empty() {
                info!("using GCS (Application Default Credentials): bucket={bucket}");
                Arc::new(GcsBlobStore::new_from_adc(bucket)?)
            } else {
                info!("using GCS (service account): bucket={bucket}");
                Arc::new(GcsBlobStore::new(bucket, service_account_key)?)
            }
        }
        StorageConfig::Azure { account_name, access_key, container } => {
            info!("using Azure Blob Storage: account={account_name} container={container}");
            Arc::new(AzureBlobStore::new(account_name, access_key, container)?)
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
