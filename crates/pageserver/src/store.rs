/// Persistent layer storage — reads and writes ImageLayer / DeltaLayer files to/from
/// the BlobStore (local FS in dev, MinIO/S3 in production).
///
/// Key layout in the blob store:
///   `{tenant_id}/{timeline_id}/layers/image_{lsn}.layer`
///   `{tenant_id}/{timeline_id}/layers/delta_{start_lsn}_{end_lsn}.layer`
///   `{tenant_id}/{timeline_id}/meta.json`

use std::sync::Arc;
use std::collections::HashMap;

use async_trait::async_trait;
use parking_lot::RwLock;
use tracing::{debug, info};

use lattice_common::{Lsn, TenantId, TimelineId, BlobStore};
use lattice_common::error::{LatticeError, Result};

use crate::layer::{ImageLayer, DeltaLayer};
use crate::timeline::TimelineMeta;

// ---------------------------------------------------------------------------
// LayerStorage trait
// ---------------------------------------------------------------------------

#[async_trait]
pub trait LayerStorage: Send + Sync + 'static {
    async fn load_image_layer(&self, tenant: TenantId, timeline: TimelineId, lsn: Lsn) -> Result<ImageLayer>;
    async fn save_image_layer(&self, tenant: TenantId, timeline: TimelineId, layer: &ImageLayer) -> Result<()>;

    async fn load_delta_layer(&self, tenant: TenantId, timeline: TimelineId, start_lsn: Lsn, end_lsn: Lsn) -> Result<DeltaLayer>;
    async fn save_delta_layer(&self, tenant: TenantId, timeline: TimelineId, layer: &DeltaLayer) -> Result<()>;

    async fn list_image_layers(&self, tenant: TenantId, timeline: TimelineId) -> Result<Vec<Lsn>>;
    async fn list_delta_layers(&self, tenant: TenantId, timeline: TimelineId) -> Result<Vec<(Lsn, Lsn)>>;

    async fn save_timeline_meta(&self, meta: &TimelineMeta) -> Result<()>;
    async fn load_timeline_meta(&self, tenant: TenantId, timeline: TimelineId) -> Result<TimelineMeta>;
}

// ---------------------------------------------------------------------------
// BlobStore-backed implementation
// ---------------------------------------------------------------------------

pub struct BlobLayerStorage {
    store: Arc<dyn BlobStore>,
}

impl BlobLayerStorage {
    pub fn new(store: Arc<dyn BlobStore>) -> Self {
        Self { store }
    }

    fn image_key(tenant: TenantId, timeline: TimelineId, lsn: Lsn) -> String {
        format!("{tenant}/{timeline}/layers/image_{:016X}.layer", lsn.as_u64())
    }

    fn delta_key(tenant: TenantId, timeline: TimelineId, start: Lsn, end: Lsn) -> String {
        format!(
            "{tenant}/{timeline}/layers/delta_{:016X}_{:016X}.layer",
            start.as_u64(), end.as_u64()
        )
    }

    fn meta_key(tenant: TenantId, timeline: TimelineId) -> String {
        format!("{tenant}/{timeline}/meta.json")
    }
}

#[async_trait]
impl LayerStorage for BlobLayerStorage {
    async fn load_image_layer(&self, tenant: TenantId, timeline: TimelineId, lsn: Lsn) -> Result<ImageLayer> {
        let key = Self::image_key(tenant, timeline, lsn);
        debug!(key, "loading image layer");
        let data = self.store.get(&key).await?;
        ImageLayer::deserialize(&data)
    }

    async fn save_image_layer(&self, tenant: TenantId, timeline: TimelineId, layer: &ImageLayer) -> Result<()> {
        let key = Self::image_key(tenant, timeline, layer.lsn);
        debug!(key, "saving image layer ({} pages)", layer.page_count());
        let data = layer.serialize()?;
        self.store.put(&key, data).await?;
        Ok(())
    }

    async fn load_delta_layer(&self, tenant: TenantId, timeline: TimelineId, start: Lsn, end: Lsn) -> Result<DeltaLayer> {
        let key = Self::delta_key(tenant, timeline, start, end);
        debug!(key, "loading delta layer");
        let data = self.store.get(&key).await?;
        DeltaLayer::deserialize(&data)
    }

    async fn save_delta_layer(&self, tenant: TenantId, timeline: TimelineId, layer: &DeltaLayer) -> Result<()> {
        let key = Self::delta_key(tenant, timeline, layer.start_lsn, layer.end_lsn);
        debug!(key, "saving delta layer");
        let data = layer.serialize()?;
        self.store.put(&key, data).await?;
        Ok(())
    }

    async fn list_image_layers(&self, tenant: TenantId, timeline: TimelineId) -> Result<Vec<Lsn>> {
        let prefix = format!("{tenant}/{timeline}/layers/image_");
        let keys = self.store.list(&prefix).await?;
        let mut lsns = Vec::new();
        for key in keys {
            if let Some(lsn_hex) = key.strip_prefix(&prefix).and_then(|s| s.strip_suffix(".layer")) {
                if let Ok(v) = u64::from_str_radix(lsn_hex, 16) {
                    lsns.push(Lsn(v));
                }
            }
        }
        lsns.sort();
        Ok(lsns)
    }

    async fn list_delta_layers(&self, tenant: TenantId, timeline: TimelineId) -> Result<Vec<(Lsn, Lsn)>> {
        let prefix = format!("{tenant}/{timeline}/layers/delta_");
        let keys = self.store.list(&prefix).await?;
        let mut result = Vec::new();
        for key in keys {
            if let Some(rest) = key.strip_prefix(&prefix).and_then(|s| s.strip_suffix(".layer")) {
                let parts: Vec<&str> = rest.splitn(2, '_').collect();
                if parts.len() == 2 {
                    if let (Ok(s), Ok(e)) = (
                        u64::from_str_radix(parts[0], 16),
                        u64::from_str_radix(parts[1], 16),
                    ) {
                        result.push((Lsn(s), Lsn(e)));
                    }
                }
            }
        }
        result.sort();
        Ok(result)
    }

    async fn save_timeline_meta(&self, meta: &TimelineMeta) -> Result<()> {
        let key = Self::meta_key(meta.tenant_id, meta.id);
        let data = serde_json::to_vec(meta)
            .map_err(|e| LatticeError::Internal(anyhow::anyhow!("meta serialize: {e}")))?;
        self.store.put(&key, bytes::Bytes::from(data)).await?;
        Ok(())
    }

    async fn load_timeline_meta(&self, tenant: TenantId, timeline: TimelineId) -> Result<TimelineMeta> {
        let key = Self::meta_key(tenant, timeline);
        let data = self.store.get(&key).await?;
        serde_json::from_slice(&data)
            .map_err(|e| LatticeError::Internal(anyhow::anyhow!("meta deserialize: {e}")))
    }
}

// ---------------------------------------------------------------------------
// In-memory implementation (for tests and Phase 1 MVP)
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct MemoryLayerStorage {
    data: RwLock<HashMap<String, bytes::Bytes>>,
}

impl MemoryLayerStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl LayerStorage for MemoryLayerStorage {
    async fn load_image_layer(&self, tenant: TenantId, timeline: TimelineId, lsn: Lsn) -> Result<ImageLayer> {
        let key = BlobLayerStorage::image_key(tenant, timeline, lsn);
        let data = self.data.read().get(&key).cloned()
            .ok_or_else(|| LatticeError::TimelineNotFound(key))?;
        ImageLayer::deserialize(&data)
    }

    async fn save_image_layer(&self, tenant: TenantId, timeline: TimelineId, layer: &ImageLayer) -> Result<()> {
        let key = BlobLayerStorage::image_key(tenant, timeline, layer.lsn);
        self.data.write().insert(key, layer.serialize()?);
        Ok(())
    }

    async fn load_delta_layer(&self, tenant: TenantId, timeline: TimelineId, start: Lsn, end: Lsn) -> Result<DeltaLayer> {
        let key = BlobLayerStorage::delta_key(tenant, timeline, start, end);
        let data = self.data.read().get(&key).cloned()
            .ok_or_else(|| LatticeError::TimelineNotFound(key))?;
        DeltaLayer::deserialize(&data)
    }

    async fn save_delta_layer(&self, tenant: TenantId, timeline: TimelineId, layer: &DeltaLayer) -> Result<()> {
        let key = BlobLayerStorage::delta_key(tenant, timeline, layer.start_lsn, layer.end_lsn);
        self.data.write().insert(key, layer.serialize()?);
        Ok(())
    }

    async fn list_image_layers(&self, tenant: TenantId, timeline: TimelineId) -> Result<Vec<Lsn>> {
        let prefix = format!("{tenant}/{timeline}/layers/image_");
        Ok(self.data.read().keys()
            .filter(|k| k.starts_with(&prefix))
            .filter_map(|k| {
                k.strip_prefix(&prefix)?.strip_suffix(".layer").and_then(|hex| {
                    u64::from_str_radix(hex, 16).ok().map(Lsn)
                })
            })
            .collect())
    }

    async fn list_delta_layers(&self, tenant: TenantId, timeline: TimelineId) -> Result<Vec<(Lsn, Lsn)>> {
        let prefix = format!("{tenant}/{timeline}/layers/delta_");
        Ok(self.data.read().keys()
            .filter(|k| k.starts_with(&prefix))
            .filter_map(|k| {
                let rest = k.strip_prefix(&prefix)?.strip_suffix(".layer")?;
                let (s, e) = rest.split_once('_')?;
                let s = u64::from_str_radix(s, 16).ok()?;
                let e = u64::from_str_radix(e, 16).ok()?;
                Some((Lsn(s), Lsn(e)))
            })
            .collect())
    }

    async fn save_timeline_meta(&self, meta: &TimelineMeta) -> Result<()> {
        let key = BlobLayerStorage::meta_key(meta.tenant_id, meta.id);
        let data = serde_json::to_vec(meta)
            .map_err(|e| LatticeError::Internal(anyhow::anyhow!("{e}")))?;
        self.data.write().insert(key, bytes::Bytes::from(data));
        Ok(())
    }

    async fn load_timeline_meta(&self, tenant: TenantId, timeline: TimelineId) -> Result<TimelineMeta> {
        let key = BlobLayerStorage::meta_key(tenant, timeline);
        let data = self.data.read().get(&key).cloned()
            .ok_or_else(|| LatticeError::TimelineNotFound(key))?;
        serde_json::from_slice(&data)
            .map_err(|e| LatticeError::Internal(anyhow::anyhow!("{e}")))
    }
}
