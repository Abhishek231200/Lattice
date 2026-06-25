use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageserverConfig {
    /// Address to listen on for the HTTP API.
    pub listen_addr: String,
    /// Storage backend.
    pub storage: StorageConfig,
    /// Compaction settings.
    pub compaction: CompactionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageConfig {
    LocalFs { path: PathBuf },
    /// AWS S3 or any S3-compatible service (MinIO, Ceph).
    /// Set `endpoint` to a custom URL for MinIO; leave empty for AWS.
    S3 {
        endpoint: String,
        bucket: String,
        region: String,
        access_key: String,
        secret_key: String,
    },
    /// Google Cloud Storage via service-account JSON.
    /// If `service_account_key` is empty, falls back to Application Default Credentials.
    Gcs {
        bucket: String,
        service_account_key: String,
    },
    /// Azure Blob Storage.
    Azure {
        account_name: String,
        access_key: String,
        container: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Interval between compaction passes (seconds).
    pub interval_secs: u64,
    /// Number of delta layers before triggering compaction.
    pub delta_threshold: usize,
}

impl Default for PageserverConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:6400".to_string(),
            storage: StorageConfig::LocalFs {
                path: PathBuf::from("/tmp/lattice/data"),
            },
            compaction: CompactionConfig {
                interval_secs: 30,
                delta_threshold: 16,
            },
        }
    }
}
