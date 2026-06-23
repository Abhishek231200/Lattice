use thiserror::Error;

#[derive(Error, Debug)]
pub enum LatticeError {
    #[error("page not found: rel={rel} blk={blk} lsn={lsn}")]
    PageNotFound {
        rel: String,
        blk: u32,
        lsn: u64,
    },

    #[error("timeline not found: {0}")]
    TimelineNotFound(String),

    #[error("tenant not found: {0}")]
    TenantNotFound(String),

    #[error("invalid LSN: {0}")]
    InvalidLsn(u64),

    #[error("WAL decode error: {0}")]
    WalDecodeError(String),

    #[error("redo error: {0}")]
    RedoError(String),

    #[error("storage error: {0}")]
    StorageError(#[from] StorageError),

    #[error("compaction error: {0}")]
    CompactionError(String),

    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("key not found: {0}")]
    NotFound(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("backend error: {0}")]
    Backend(String),
}

pub type Result<T> = std::result::Result<T, LatticeError>;
