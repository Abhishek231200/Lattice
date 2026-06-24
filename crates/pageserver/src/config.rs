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
    S3 {
        endpoint: String,
        bucket: String,
        region: String,
        access_key: String,
        secret_key: String,
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
