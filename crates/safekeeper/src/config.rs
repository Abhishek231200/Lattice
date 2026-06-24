use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafekeeperConfig {
    pub listen_addr: String,
    pub data_dir: PathBuf,
    pub pageserver_url: Option<String>,
}

impl Default for SafekeeperConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:6401".to_string(),
            data_dir: PathBuf::from("/tmp/lattice/wal"),
            pageserver_url: Some("http://127.0.0.1:6400".to_string()),
        }
    }
}
