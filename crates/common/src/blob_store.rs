use async_trait::async_trait;
use bytes::Bytes;
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;
use crate::error::StorageError;

pub type StoreResult<T> = std::result::Result<T, StorageError>;

/// Cloud-agnostic blob storage abstraction.
/// Impls: local FS, MinIO/S3, GCS (stub), Azure Blob (stub).
/// All persistence in Lattice flows through this trait so the system is cloud-portable.
#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    /// Retrieve the value at `key`. Returns `StorageError::NotFound` if absent.
    async fn get(&self, key: &str) -> StoreResult<Bytes>;

    /// Store `data` at `key`, creating or overwriting.
    async fn put(&self, key: &str, data: Bytes) -> StoreResult<()>;

    /// List all keys with the given prefix.
    async fn list(&self, prefix: &str) -> StoreResult<Vec<String>>;

    /// Delete the key. Silently succeeds if not present.
    async fn delete(&self, key: &str) -> StoreResult<()>;

    /// Check if a key exists.
    async fn exists(&self, key: &str) -> StoreResult<bool> {
        match self.get(key).await {
            Ok(_) => Ok(true),
            Err(StorageError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

// ---------------------------------------------------------------------------
// Local filesystem implementation
// ---------------------------------------------------------------------------

/// `BlobStore` backed by the local filesystem.
/// Key path separators ('/') map to directory separators on disk.
/// Used locally and in tests; MinIO/S3 impl is in `pageserver::store::s3`.
#[derive(Debug, Clone)]
pub struct LocalFsStore {
    root: PathBuf,
}

impl LocalFsStore {
    pub fn new(root: impl AsRef<Path>) -> std::io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn key_to_path(&self, key: &str) -> PathBuf {
        // Prevent path traversal
        let safe = key.replace("..", "__");
        self.root.join(safe.replace('/', std::path::MAIN_SEPARATOR_STR))
    }
}

#[async_trait]
impl BlobStore for LocalFsStore {
    async fn get(&self, key: &str) -> StoreResult<Bytes> {
        let path = self.key_to_path(key);
        let mut file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    StorageError::NotFound(key.to_string())
                } else {
                    StorageError::Io(e)
                }
            })?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await.map_err(StorageError::Io)?;
        Ok(Bytes::from(buf))
    }

    async fn put(&self, key: &str, data: Bytes) -> StoreResult<()> {
        let path = self.key_to_path(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(StorageError::Io)?;
        }
        tokio::fs::write(&path, data).await.map_err(StorageError::Io)?;
        Ok(())
    }

    async fn list(&self, prefix: &str) -> StoreResult<Vec<String>> {
        let dir = self.key_to_path(prefix);
        // If prefix points to a directory, list its contents recursively.
        // If not a directory, list the parent and filter by the full path prefix.
        let search_dir = if dir.is_dir() {
            dir.clone()
        } else {
            dir.parent().unwrap_or(&self.root).to_path_buf()
        };

        let mut keys = Vec::new();
        self.collect_keys(&search_dir, prefix, &mut keys).await?;
        Ok(keys)
    }

    async fn delete(&self, key: &str) -> StoreResult<()> {
        let path = self.key_to_path(key);
        match tokio::fs::remove_file(&path).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StorageError::Io(e)),
        }
    }
}

impl LocalFsStore {
    fn collect_keys<'a>(
        &'a self,
        dir: &'a Path,
        prefix: &'a str,
        keys: &'a mut Vec<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StoreResult<()>> + Send + 'a>> {
        Box::pin(async move {
            let mut rd = match tokio::fs::read_dir(dir).await {
                Ok(rd) => rd,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(e) => return Err(StorageError::Io(e)),
            };
            while let Some(entry) = rd.next_entry().await.map_err(StorageError::Io)? {
                let path = entry.path();
                if path.is_dir() {
                    self.collect_keys(&path, prefix, keys).await?;
                } else {
                    let rel = path.strip_prefix(&self.root)
                        .map(|p| p.to_string_lossy().replace(std::path::MAIN_SEPARATOR, "/"))
                        .unwrap_or_default();
                    if rel.starts_with(prefix) {
                        keys.push(rel);
                    }
                }
            }
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// S3 / MinIO stub — wire protocol identical, just different endpoint
// ---------------------------------------------------------------------------

/// Marker struct; full impl in `pageserver::store::s3`.
pub struct S3Store;

// ---------------------------------------------------------------------------
// GCS stub
// ---------------------------------------------------------------------------
pub struct GcsStore;

// ---------------------------------------------------------------------------
// Azure Blob stub
// ---------------------------------------------------------------------------
pub struct AzureStore;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn round_trip() {
        let dir = tempdir().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();

        store.put("foo/bar", Bytes::from("hello")).await.unwrap();
        let got = store.get("foo/bar").await.unwrap();
        assert_eq!(got, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn not_found() {
        let dir = tempdir().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        let err = store.get("missing").await.unwrap_err();
        assert!(matches!(err, StorageError::NotFound(_)));
    }

    #[tokio::test]
    async fn list_and_delete() {
        let dir = tempdir().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();

        store.put("prefix/a", Bytes::from("a")).await.unwrap();
        store.put("prefix/b", Bytes::from("b")).await.unwrap();
        store.put("other/c", Bytes::from("c")).await.unwrap();

        let keys = store.list("prefix/").await.unwrap();
        assert_eq!(keys.len(), 2);

        store.delete("prefix/a").await.unwrap();
        store.delete("prefix/a").await.unwrap(); // idempotent

        let keys = store.list("prefix/").await.unwrap();
        assert_eq!(keys.len(), 1);
        assert!(keys[0].contains("prefix/b"));
    }

    #[tokio::test]
    async fn exists() {
        let dir = tempdir().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();

        assert!(!store.exists("x").await.unwrap());
        store.put("x", Bytes::from("v")).await.unwrap();
        assert!(store.exists("x").await.unwrap());
    }
}
