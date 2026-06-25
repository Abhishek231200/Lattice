/// Image and delta layer types.
///
/// Storage model:
///   - ImageLayer: a full materialized snapshot of a set of pages at a given LSN.
///   - DeltaLayer: WAL-derived page deltas over an LSN range [start_lsn, end_lsn).
///
/// On-disk key structure (inside BlobStore):
///   `{tenant}/{timeline}/layers/{type}_{rel}_{start_lsn}_{end_lsn}.layer`
///
/// get_page_at_lsn algorithm:
///   1. Find the latest ImageLayer with lsn <= requested.
///   2. Collect all DeltaLayers in (image_lsn, requested_lsn].
///   3. Apply deltas in order to produce the final page.
///   4. If no layer found on this timeline, recurse to parent up to branch_lsn.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use lattice_common::{
    Lsn, RelTag, BlockNumber, PageImage, PageVersion, PageDelta, PAGE_SIZE,
};
use lattice_common::error::{LatticeError, Result};

// ---------------------------------------------------------------------------
// Layer key
// ---------------------------------------------------------------------------

/// Uniquely identifies a (relation, block) pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct PageKey {
    pub rel: RelTag,
    pub blk: BlockNumber,
}

impl PageKey {
    pub fn new(rel: RelTag, blk: BlockNumber) -> Self {
        Self { rel, blk }
    }
}

// ---------------------------------------------------------------------------
// ImageLayer
// ---------------------------------------------------------------------------

/// A full-page snapshot at a specific LSN for a set of (rel, block) keys.
/// Immutable once written.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageLayer {
    /// The LSN at which all pages in this layer are materialized.
    pub lsn: Lsn,
    /// Page data keyed by (rel, blk).
    pub pages: BTreeMap<PageKey, PageImage>,
}

impl ImageLayer {
    pub fn new(lsn: Lsn) -> Self {
        Self { lsn, pages: BTreeMap::new() }
    }

    pub fn insert(&mut self, rel: RelTag, blk: BlockNumber, image: PageImage) {
        self.pages.insert(PageKey::new(rel, blk), image);
    }

    pub fn get(&self, rel: RelTag, blk: BlockNumber) -> Option<&PageImage> {
        self.pages.get(&PageKey::new(rel, blk))
    }

    pub fn page_count(&self) -> usize {
        self.pages.len()
    }

    /// Serialize to bytes for storage.
    pub fn serialize(&self) -> Result<Bytes> {
        bincode::serialize(self)
            .map(Bytes::from)
            .map_err(|e| LatticeError::Internal(anyhow::anyhow!("ImageLayer serialize: {e}")))
    }

    /// Deserialize from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data)
            .map_err(|e| LatticeError::Internal(anyhow::anyhow!("ImageLayer deserialize: {e}")))
    }
}

// ---------------------------------------------------------------------------
// DeltaLayer
// ---------------------------------------------------------------------------

/// WAL-derived page deltas over LSN range [start_lsn, end_lsn).
/// Multiple deltas for the same (rel, blk) are stored in LSN order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaLayer {
    /// Inclusive start LSN.
    pub start_lsn: Lsn,
    /// Exclusive end LSN (i.e., the LSN of the first record NOT in this layer).
    pub end_lsn: Lsn,
    /// Deltas keyed by PageKey, stored as sorted (lsn, PageVersion) pairs.
    pub deltas: BTreeMap<PageKey, Vec<(Lsn, PageVersion)>>,
}

impl DeltaLayer {
    pub fn new(start_lsn: Lsn, end_lsn: Lsn) -> Self {
        assert!(start_lsn < end_lsn, "DeltaLayer: start must be < end");
        Self { start_lsn, end_lsn, deltas: BTreeMap::new() }
    }

    /// Insert a page version at the given LSN. LSNs must be inserted in order.
    pub fn insert(&mut self, rel: RelTag, blk: BlockNumber, lsn: Lsn, version: PageVersion) {
        assert!(
            lsn >= self.start_lsn && lsn < self.end_lsn,
            "DeltaLayer insert: lsn {lsn} out of range [{}, {})",
            self.start_lsn, self.end_lsn,
        );
        self.deltas
            .entry(PageKey::new(rel, blk))
            .or_default()
            .push((lsn, version));
    }

    /// Get all versions for a (rel, blk) with lsn in [start_lsn, up_to_lsn].
    pub fn get_versions(
        &self,
        rel: RelTag,
        blk: BlockNumber,
        up_to_lsn: Lsn,
    ) -> &[(Lsn, PageVersion)] {
        let key = PageKey::new(rel, blk);
        match self.deltas.get(&key) {
            None => &[],
            Some(versions) => {
                // Find the slice up to up_to_lsn (versions are stored in LSN order).
                let end = versions.partition_point(|(lsn, _)| *lsn <= up_to_lsn);
                &versions[..end]
            }
        }
    }

    pub fn covers(&self, lsn: Lsn) -> bool {
        lsn >= self.start_lsn && lsn < self.end_lsn
    }

    pub fn serialize(&self) -> Result<Bytes> {
        bincode::serialize(self)
            .map(Bytes::from)
            .map_err(|e| LatticeError::Internal(anyhow::anyhow!("DeltaLayer serialize: {e}")))
    }

    pub fn deserialize(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data)
            .map_err(|e| LatticeError::Internal(anyhow::anyhow!("DeltaLayer deserialize: {e}")))
    }
}

// ---------------------------------------------------------------------------
// LayerSet — in-memory collection for a timeline
// ---------------------------------------------------------------------------

/// All layers for one timeline, held in memory with RW-lock access.
/// Persisted versions live in the BlobStore; this is the hot cache.
#[derive(Debug, Default)]
pub struct LayerSet {
    /// Sorted by lsn descending for fast "find latest image <= lsn" lookup.
    pub images: RwLock<Vec<Arc<ImageLayer>>>,
    /// Sorted by start_lsn for efficient range queries.
    pub deltas: RwLock<Vec<Arc<DeltaLayer>>>,
}

impl LayerSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_image_layer(&self, layer: ImageLayer) {
        let mut images = self.images.write();
        images.push(Arc::new(layer));
        // Keep sorted descending by lsn so the latest image is first.
        images.sort_by(|a, b| b.lsn.cmp(&a.lsn));
    }

    pub fn add_delta_layer(&self, layer: DeltaLayer) {
        let mut deltas = self.deltas.write();
        deltas.push(Arc::new(layer));
        deltas.sort_by_key(|d| d.start_lsn);
    }

    /// Find the most recent image for (rel, blk) with image.lsn <= requested_lsn.
    pub fn find_image(&self, rel: RelTag, blk: BlockNumber, lsn: Lsn) -> Option<PageImage> {
        let images = self.images.read();
        for image_layer in images.iter() {
            if image_layer.lsn <= lsn {
                if let Some(page) = image_layer.get(rel, blk) {
                    return Some(page.clone());
                }
            }
        }
        None
    }

    /// Collect all delta versions for (rel, blk) in (after_lsn, up_to_lsn].
    pub fn collect_deltas(
        &self,
        rel: RelTag,
        blk: BlockNumber,
        after_lsn: Lsn,
        up_to_lsn: Lsn,
    ) -> Vec<(Lsn, PageVersion)> {
        let deltas = self.deltas.read();
        let mut result = Vec::new();
        for delta_layer in deltas.iter() {
            // Only consider layers that overlap (after_lsn, up_to_lsn].
            if delta_layer.end_lsn <= after_lsn || delta_layer.start_lsn > up_to_lsn {
                continue;
            }
            let versions = delta_layer.get_versions(rel, blk, up_to_lsn);
            for (lsn, v) in versions {
                if *lsn > after_lsn {
                    result.push((*lsn, v.clone()));
                }
            }
        }
        // Sort by LSN so we apply deltas in order.
        result.sort_by_key(|(lsn, _)| *lsn);
        result
    }

    /// Collect the latest version of every page that was written strictly after `since`.
    ///
    /// Used by `merge_branch` to enumerate the diverging changes on a branch.
    /// Images are stored sorted descending by LSN, so the first hit per key is the latest.
    pub fn pages_since_lsn(&self, since: Lsn) -> HashMap<PageKey, (Lsn, PageImage)> {
        let images = self.images.read();
        let mut result: HashMap<PageKey, (Lsn, PageImage)> = HashMap::new();
        for layer in images.iter() {  // descending LSN order → first hit = latest
            if layer.lsn > since {
                for (key, image) in &layer.pages {
                    result.entry(*key).or_insert_with(|| (layer.lsn, image.clone()));
                }
            }
        }
        result
    }

    /// Returns (image_layers, total_image_pages, delta_layers, total_delta_entries).
    /// Used by storage amplification benchmarks and tests.
    pub fn stats(&self) -> LayerStats {
        let images = self.images.read();
        let deltas = self.deltas.read();
        let total_image_pages: usize = images.iter().map(|l| l.page_count()).sum();
        let total_delta_entries: usize = deltas.iter().map(|l| l.deltas.len()).sum();
        LayerStats {
            image_layers: images.len(),
            total_image_pages,
            delta_layers: deltas.len(),
            total_delta_entries,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LayerStats {
    pub image_layers: usize,
    pub total_image_pages: usize,
    pub delta_layers: usize,
    pub total_delta_entries: usize,
}

impl LayerStats {
    /// Logical storage bytes: image pages at PAGE_SIZE + delta entries at an estimated
    /// average patch size (64 bytes overhead + payload).
    pub fn logical_bytes(&self) -> usize {
        self.total_image_pages * PAGE_SIZE + self.total_delta_entries * 64
    }

    pub fn logical_kb(&self) -> f64 {
        self.logical_bytes() as f64 / 1024.0
    }

    pub fn logical_mb(&self) -> f64 {
        self.logical_bytes() as f64 / (1024.0 * 1024.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_page(byte: u8) -> PageImage {
        PageImage::new(Bytes::from(vec![byte; PAGE_SIZE]))
    }

    fn rel() -> RelTag {
        RelTag::main(0, 1, 1)
    }

    #[test]
    fn image_layer_round_trip() {
        let mut layer = ImageLayer::new(Lsn(100));
        layer.insert(rel(), 0, make_page(0xAA));
        assert!(layer.get(rel(), 0).is_some());
        assert!(layer.get(rel(), 1).is_none());

        let bytes = layer.serialize().unwrap();
        let decoded = ImageLayer::deserialize(&bytes).unwrap();
        assert_eq!(decoded.lsn, Lsn(100));
        assert_eq!(decoded.get(rel(), 0).unwrap().0, make_page(0xAA).0);
    }

    #[test]
    fn delta_layer_versions() {
        let mut layer = DeltaLayer::new(Lsn(10), Lsn(50));
        layer.insert(rel(), 0, Lsn(10), PageVersion::Image(make_page(1)));
        layer.insert(rel(), 0, Lsn(20), PageVersion::Image(make_page(2)));
        layer.insert(rel(), 0, Lsn(30), PageVersion::Image(make_page(3)));

        let v = layer.get_versions(rel(), 0, Lsn(25));
        assert_eq!(v.len(), 2); // lsn 10 and 20

        let v = layer.get_versions(rel(), 0, Lsn(30));
        assert_eq!(v.len(), 3);
    }

    #[test]
    fn layer_set_find_image() {
        let ls = LayerSet::new();

        let mut img10 = ImageLayer::new(Lsn(10));
        img10.insert(rel(), 0, make_page(10));
        ls.add_image_layer(img10);

        let mut img30 = ImageLayer::new(Lsn(30));
        img30.insert(rel(), 0, make_page(30));
        ls.add_image_layer(img30);

        // At lsn=25 we should get the lsn=10 image (latest <= 25 that has the page)
        let page = ls.find_image(rel(), 0, Lsn(25)).unwrap();
        assert_eq!(page.0[0], 10);

        // At lsn=40 we should get the lsn=30 image
        let page = ls.find_image(rel(), 0, Lsn(40)).unwrap();
        assert_eq!(page.0[0], 30);
    }
}
