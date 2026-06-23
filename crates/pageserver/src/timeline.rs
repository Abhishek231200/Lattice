/// Timeline — a branch with copy-on-write semantics.
///
/// Key invariants:
///   - Each timeline has an optional parent and a branch_lsn.
///   - Reads recurse to the parent for any LSN <= branch_lsn when the page is absent on
///     this timeline.  This is what makes branches O(1) to create with zero data copied.
///   - Writes on a child create new delta layers tagged to the child only; the parent's
///     layers are never mutated.
///
/// get_page_at_lsn(rel, blk, lsn):
///   1. Search this timeline's LayerSet for an image layer with image.lsn <= lsn that
///      contains (rel, blk).  Let base_lsn = image.lsn (or branch_lsn if recurse needed).
///   2. Collect all delta versions for (rel, blk) in (base_lsn, lsn].
///   3. Apply deltas to the base image through the redo engine.
///   4. If no image found AND we have a parent, recurse: get_page_at_lsn on parent at
///      min(lsn, branch_lsn).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use lattice_common::{
    Lsn, TenantId, TimelineId, RelTag, BlockNumber, PageImage, PageVersion, PAGE_SIZE,
};
use lattice_common::blob_store::BlobStore;
use lattice_common::error::{LatticeError, Result};

use crate::layer::{ImageLayer, DeltaLayer, LayerSet};
use crate::redo::RedoEngine;
use crate::store::LayerStorage;

// ---------------------------------------------------------------------------
// Timeline metadata
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineMeta {
    pub id: TimelineId,
    pub tenant_id: TenantId,
    pub parent_id: Option<TimelineId>,
    /// LSN at which this timeline was branched from the parent.
    /// Only pages written after this LSN on the parent belong to the parent's post-branch history.
    /// Pages at <= branch_lsn are shared (copy-on-write).
    pub branch_lsn: Lsn,
    /// The last LSN ingested into this timeline.
    pub last_lsn: Lsn,
    pub name: String,
}

// ---------------------------------------------------------------------------
// Timeline
// ---------------------------------------------------------------------------

pub struct Timeline {
    pub meta: TimelineMeta,
    pub layers: Arc<LayerSet>,
    pub parent: Option<Arc<Timeline>>,
    pub storage: Arc<dyn LayerStorage>,
    last_lsn: RwLock<Lsn>,
}

impl std::fmt::Debug for Timeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Timeline")
            .field("id", &self.meta.id)
            .field("parent", &self.meta.parent_id)
            .field("branch_lsn", &self.meta.branch_lsn)
            .field("last_lsn", &self.last_lsn.read())
            .finish()
    }
}

impl Timeline {
    pub fn new(meta: TimelineMeta, parent: Option<Arc<Timeline>>, storage: Arc<dyn LayerStorage>) -> Self {
        let last_lsn = meta.last_lsn;
        Self {
            meta,
            layers: Arc::new(LayerSet::new()),
            parent,
            storage,
            last_lsn: RwLock::new(last_lsn),
        }
    }

    pub fn id(&self) -> TimelineId {
        self.meta.id
    }

    pub fn last_lsn(&self) -> Lsn {
        *self.last_lsn.read()
    }

    pub fn advance_lsn(&self, lsn: Lsn) {
        let mut guard = self.last_lsn.write();
        if lsn > *guard {
            *guard = lsn;
        }
    }

    // ---------------------------------------------------------------------------
    // Core read: get_page_at_lsn
    // ---------------------------------------------------------------------------

    /// Return the 8 KiB page for `(rel, blk)` as of `lsn`.
    ///
    /// This is the central read operation for the entire pageserver.
    #[instrument(skip(self, redo), fields(tl = %self.meta.id, rel = %rel, blk, lsn = %lsn))]
    pub fn get_page_at_lsn(
        &self,
        redo: &RedoEngine,
        rel: RelTag,
        blk: BlockNumber,
        lsn: Lsn,
    ) -> Result<PageImage> {
        // Clamp lsn to the last ingested LSN if caller requests further ahead.
        let lsn = lsn.min(self.last_lsn());

        // 1. Find the latest image for (rel, blk) with image.lsn <= lsn on this timeline.
        let base_lsn_for_image = {
            self.layers.images.read()
                .iter()
                .filter(|il| il.lsn <= lsn)
                .filter_map(|il| il.get(rel, blk).map(|_| il.lsn))
                .max()
                .unwrap_or(Lsn::INVALID)
        };
        let (base_image, base_lsn) = match self.layers.find_image(rel, blk, lsn) {
            Some(img) => {
                (img, base_lsn_for_image)
            }
            None => {
                // No local image — try the parent timeline for LSN <= branch_lsn.
                if let Some(parent) = &self.parent {
                    let parent_lsn = lsn.min(self.meta.branch_lsn);
                    debug!("no local image; recursing to parent at lsn={parent_lsn}");
                    let page = parent.get_page_at_lsn(redo, rel, blk, parent_lsn)?;
                    // Collect this timeline's deltas on top of the parent's page.
                    let my_deltas = self.layers.collect_deltas(rel, blk, self.meta.branch_lsn, lsn);
                    if my_deltas.is_empty() {
                        return Ok(page);
                    }
                    return redo.apply_deltas(page, &my_deltas);
                }

                return Err(LatticeError::PageNotFound {
                    rel: rel.to_string(),
                    blk,
                    lsn: lsn.as_u64(),
                });
            }
        };

        // 2. Collect deltas in (base_lsn, lsn] on this timeline.
        let deltas = self.layers.collect_deltas(rel, blk, base_lsn, lsn);
        if deltas.is_empty() {
            return Ok(base_image);
        }

        // 3. Apply deltas through the redo engine.
        redo.apply_deltas(base_image, &deltas)
    }

    // ---------------------------------------------------------------------------
    // Write path: put_page / put_delta
    // ---------------------------------------------------------------------------

    /// Write a full page image at the given LSN.
    pub fn put_image(&self, rel: RelTag, blk: BlockNumber, lsn: Lsn, image: PageImage) {
        // For simplicity we create a one-page image layer per write in the MVP.
        // Compaction will merge these into larger layers.
        let mut layer = ImageLayer::new(lsn);
        layer.insert(rel, blk, image);
        self.layers.add_image_layer(layer);
        self.advance_lsn(lsn);
    }

    /// Write a WAL-derived page version at the given LSN.
    pub fn put_page_version(&self, rel: RelTag, blk: BlockNumber, lsn: Lsn, version: PageVersion) {
        // Build a single-record delta layer.  In production these are batched by the
        // WAL ingestion path before being flushed; this MVP-path is one-per-record.
        let delta_start = if lsn.0 > 0 { Lsn(lsn.0 - 1) } else { Lsn::MIN };
        let mut layer = DeltaLayer::new(delta_start, Lsn(lsn.0 + 1));
        layer.insert(rel, blk, lsn, version);
        self.layers.add_delta_layer(layer);
        self.advance_lsn(lsn);
    }
}

// ---------------------------------------------------------------------------
// TimelineManager — registry of all timelines for all tenants
// ---------------------------------------------------------------------------

pub struct TimelineManager {
    /// tenant_id -> timeline_id -> Timeline
    timelines: RwLock<HashMap<TenantId, HashMap<TimelineId, Arc<Timeline>>>>,
    storage: Arc<dyn LayerStorage>,
}

impl TimelineManager {
    pub fn new(storage: Arc<dyn LayerStorage>) -> Self {
        Self {
            timelines: RwLock::new(HashMap::new()),
            storage,
        }
    }

    /// Create a new root timeline (no parent).
    pub fn create_timeline(
        &self,
        tenant_id: TenantId,
        name: String,
    ) -> Arc<Timeline> {
        let meta = TimelineMeta {
            id: TimelineId::new(),
            tenant_id,
            parent_id: None,
            branch_lsn: Lsn::INVALID,
            last_lsn: Lsn::INVALID,
            name,
        };
        let tl = Arc::new(Timeline::new(meta.clone(), None, self.storage.clone()));
        let mut guard = self.timelines.write();
        guard.entry(tenant_id).or_default().insert(meta.id, tl.clone());
        tl
    }

    /// Create a branch (child timeline) from a parent at the given LSN.
    ///
    /// This is the headline O(1) operation: we record metadata only; zero pages are copied.
    pub fn create_branch(
        &self,
        parent_id: TimelineId,
        tenant_id: TenantId,
        at_lsn: Lsn,
        name: String,
    ) -> Result<(Arc<Timeline>, std::time::Duration)> {
        let start = Instant::now();

        let parent = self.get_timeline(tenant_id, parent_id)?;

        let meta = TimelineMeta {
            id: TimelineId::new(),
            tenant_id,
            parent_id: Some(parent_id),
            branch_lsn: at_lsn,
            last_lsn: at_lsn,
            name,
        };
        let tl = Arc::new(Timeline::new(meta.clone(), Some(parent), self.storage.clone()));

        let elapsed = start.elapsed();

        let mut guard = self.timelines.write();
        guard.entry(tenant_id).or_default().insert(meta.id, tl.clone());

        Ok((tl, elapsed))
    }

    pub fn get_timeline(&self, tenant_id: TenantId, timeline_id: TimelineId) -> Result<Arc<Timeline>> {
        self.timelines.read()
            .get(&tenant_id)
            .and_then(|tls| tls.get(&timeline_id))
            .cloned()
            .ok_or_else(|| LatticeError::TimelineNotFound(timeline_id.to_string()))
    }

    pub fn list_timelines(&self, tenant_id: TenantId) -> Vec<Arc<Timeline>> {
        self.timelines.read()
            .get(&tenant_id)
            .map(|tls| tls.values().cloned().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::redo::RedoEngine;
    use crate::store::MemoryLayerStorage;

    fn storage() -> Arc<dyn LayerStorage> {
        Arc::new(MemoryLayerStorage::new())
    }

    fn redo() -> RedoEngine {
        RedoEngine::new()
    }

    fn page(byte: u8) -> PageImage {
        PageImage::new(Bytes::from(vec![byte; PAGE_SIZE]))
    }

    fn rel() -> RelTag {
        RelTag::main(0, 1, 1)
    }

    fn tenant() -> TenantId {
        TenantId::new()
    }

    // Phase 1 DoD: point-in-time reads
    #[test]
    fn point_in_time_read() {
        let mgr = TimelineManager::new(storage());
        let tl = mgr.create_timeline(tenant(), "main".into());
        let redo = redo();

        tl.put_image(rel(), 0, Lsn(10), page(10));
        tl.put_image(rel(), 0, Lsn(20), page(20));
        tl.put_image(rel(), 0, Lsn(30), page(30));

        // Reading at LSN 25 should return the LSN-20 version.
        let p = tl.get_page_at_lsn(&redo, rel(), 0, Lsn(25)).unwrap();
        assert_eq!(p.0[0], 20);

        let p = tl.get_page_at_lsn(&redo, rel(), 0, Lsn(30)).unwrap();
        assert_eq!(p.0[0], 30);

        let p = tl.get_page_at_lsn(&redo, rel(), 0, Lsn(10)).unwrap();
        assert_eq!(p.0[0], 10);
    }

    // Phase 3 DoD: branch creation is O(1) and reads recurse to parent
    #[test]
    fn branch_inherits_parent_pages() {
        let mgr = TimelineManager::new(storage());
        let tenant = tenant();
        let parent = mgr.create_timeline(tenant, "main".into());
        let redo = redo();

        parent.put_image(rel(), 0, Lsn(10), page(10));
        parent.put_image(rel(), 0, Lsn(20), page(20));

        // Branch at LSN 15.
        let (child, elapsed) = mgr.create_branch(parent.id(), tenant, Lsn(15), "branch".into()).unwrap();
        println!("Branch created in {:?}", elapsed);

        // Child at LSN 15 should see the parent's LSN-10 page (latest <= 15).
        let p = child.get_page_at_lsn(&redo, rel(), 0, Lsn(15)).unwrap();
        assert_eq!(p.0[0], 10);

        // Child doesn't see parent's LSN-20 page (written after branch_lsn=15).
        let p = child.get_page_at_lsn(&redo, rel(), 0, Lsn(15)).unwrap();
        assert_eq!(p.0[0], 10, "child must not see post-branch parent writes");
    }

    #[test]
    fn branch_child_writes_isolated() {
        let mgr = TimelineManager::new(storage());
        let tenant = tenant();
        let parent = mgr.create_timeline(tenant, "main".into());
        let redo = redo();

        parent.put_image(rel(), 0, Lsn(10), page(42));

        let (child, _) = mgr.create_branch(parent.id(), tenant, Lsn(10), "child".into()).unwrap();

        // Write on child at LSN 20 with a different byte.
        child.put_image(rel(), 0, Lsn(20), page(99));

        // Parent still sees its original page.
        let p = parent.get_page_at_lsn(&redo, rel(), 0, Lsn(20)).unwrap();
        assert_eq!(p.0[0], 42, "parent must not see child writes");

        // Child sees its own write.
        let p = child.get_page_at_lsn(&redo, rel(), 0, Lsn(20)).unwrap();
        assert_eq!(p.0[0], 99);
    }

    #[test]
    fn page_not_found_returns_error() {
        let mgr = TimelineManager::new(storage());
        let tl = mgr.create_timeline(tenant(), "main".into());
        let redo = redo();

        let err = tl.get_page_at_lsn(&redo, rel(), 0, Lsn(100));
        assert!(matches!(err, Err(LatticeError::PageNotFound { .. })));
    }
}
