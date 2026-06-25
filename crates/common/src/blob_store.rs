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
// Cloud object storage (S3/MinIO, GCS, Azure Blob) via `object_store`
// ---------------------------------------------------------------------------

use object_store::{ObjectStore, path::Path as ObjPath};
use futures::StreamExt;

/// Shared helper: GET a key from any ObjectStore.
async fn cloud_get(inner: &dyn ObjectStore, key: &str) -> StoreResult<Bytes> {
    let path = ObjPath::from(key);
    match inner.get(&path).await {
        Ok(result) => result.bytes().await.map_err(|e| StorageError::Backend(e.to_string())),
        Err(object_store::Error::NotFound { .. }) => Err(StorageError::NotFound(key.to_string())),
        Err(e) => Err(StorageError::Backend(e.to_string())),
    }
}

/// Shared helper: PUT a key into any ObjectStore.
async fn cloud_put(inner: &dyn ObjectStore, key: &str, data: Bytes) -> StoreResult<()> {
    let path = ObjPath::from(key);
    inner.put(&path, data.into())
        .await
        .map(|_| ())
        .map_err(|e| StorageError::Backend(e.to_string()))
}

/// Shared helper: LIST keys with a given prefix.
async fn cloud_list(inner: &dyn ObjectStore, prefix: &str) -> StoreResult<Vec<String>> {
    let prefix_path = ObjPath::from(prefix);
    let mut stream = inner.list(Some(&prefix_path));
    let mut keys = Vec::new();
    while let Some(result) = stream.next().await {
        let meta = result.map_err(|e| StorageError::Backend(e.to_string()))?;
        keys.push(meta.location.to_string());
    }
    Ok(keys)
}

/// Shared helper: DELETE a key (idempotent — silently ignores NotFound).
async fn cloud_delete(inner: &dyn ObjectStore, key: &str) -> StoreResult<()> {
    let path = ObjPath::from(key);
    match inner.delete(&path).await {
        Ok(_) => Ok(()),
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(StorageError::Backend(e.to_string())),
    }
}

// ---------------------------------------------------------------------------
// S3BlobStore — AWS S3 and MinIO-compatible
// ---------------------------------------------------------------------------

/// BlobStore backed by Amazon S3 or any S3-compatible service (MinIO, Ceph, etc.).
pub struct S3BlobStore(std::sync::Arc<dyn ObjectStore>);

impl S3BlobStore {
    /// Connect to AWS S3 with standard credentials.
    pub fn new_aws(
        region: &str,
        bucket: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> anyhow::Result<Self> {
        let store = object_store::aws::AmazonS3Builder::new()
            .with_region(region)
            .with_bucket_name(bucket)
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key)
            .build()?;
        Ok(Self(std::sync::Arc::new(store)))
    }

    /// Connect to a MinIO (or other S3-compatible) endpoint.
    pub fn new_minio(
        endpoint: &str,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
    ) -> anyhow::Result<Self> {
        let store = object_store::aws::AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_region("us-east-1")   // MinIO ignores region but the builder requires it
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_allow_http(true)       // MinIO may serve over plain HTTP
            .build()?;
        Ok(Self(std::sync::Arc::new(store)))
    }
}

#[async_trait]
impl BlobStore for S3BlobStore {
    async fn get(&self, key: &str) -> StoreResult<Bytes> { cloud_get(&*self.0, key).await }
    async fn put(&self, key: &str, data: Bytes) -> StoreResult<()> { cloud_put(&*self.0, key, data).await }
    async fn list(&self, prefix: &str) -> StoreResult<Vec<String>> { cloud_list(&*self.0, prefix).await }
    async fn delete(&self, key: &str) -> StoreResult<()> { cloud_delete(&*self.0, key).await }
}

// ---------------------------------------------------------------------------
// GcsBlobStore — Google Cloud Storage
// ---------------------------------------------------------------------------

/// BlobStore backed by Google Cloud Storage.
pub struct GcsBlobStore(std::sync::Arc<dyn ObjectStore>);

impl GcsBlobStore {
    /// Connect to GCS using a service account key JSON string.
    pub fn new(bucket: &str, service_account_key_json: &str) -> anyhow::Result<Self> {
        let store = object_store::gcp::GoogleCloudStorageBuilder::new()
            .with_bucket_name(bucket)
            .with_service_account_key(service_account_key_json)
            .build()?;
        Ok(Self(std::sync::Arc::new(store)))
    }

    /// Connect to GCS using Application Default Credentials.
    pub fn new_from_adc(bucket: &str) -> anyhow::Result<Self> {
        let store = object_store::gcp::GoogleCloudStorageBuilder::new()
            .with_bucket_name(bucket)
            .build()?;
        Ok(Self(std::sync::Arc::new(store)))
    }
}

#[async_trait]
impl BlobStore for GcsBlobStore {
    async fn get(&self, key: &str) -> StoreResult<Bytes> { cloud_get(&*self.0, key).await }
    async fn put(&self, key: &str, data: Bytes) -> StoreResult<()> { cloud_put(&*self.0, key, data).await }
    async fn list(&self, prefix: &str) -> StoreResult<Vec<String>> { cloud_list(&*self.0, prefix).await }
    async fn delete(&self, key: &str) -> StoreResult<()> { cloud_delete(&*self.0, key).await }
}

// ---------------------------------------------------------------------------
// AzureBlobStore — Azure Blob Storage
// ---------------------------------------------------------------------------

/// BlobStore backed by Azure Blob Storage.
pub struct AzureBlobStore(std::sync::Arc<dyn ObjectStore>);

impl AzureBlobStore {
    /// Connect to Azure Blob Storage with an account name and access key.
    pub fn new(account_name: &str, access_key: &str, container: &str) -> anyhow::Result<Self> {
        let store = object_store::azure::MicrosoftAzureBuilder::new()
            .with_account(account_name)
            .with_access_key(access_key)
            .with_container_name(container)
            .build()?;
        Ok(Self(std::sync::Arc::new(store)))
    }
}

#[async_trait]
impl BlobStore for AzureBlobStore {
    async fn get(&self, key: &str) -> StoreResult<Bytes> { cloud_get(&*self.0, key).await }
    async fn put(&self, key: &str, data: Bytes) -> StoreResult<()> { cloud_put(&*self.0, key, data).await }
    async fn list(&self, prefix: &str) -> StoreResult<Vec<String>> { cloud_list(&*self.0, prefix).await }
    async fn delete(&self, key: &str) -> StoreResult<()> { cloud_delete(&*self.0, key).await }
}

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
