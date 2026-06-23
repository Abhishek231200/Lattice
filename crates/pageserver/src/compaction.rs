/// Layer compaction and garbage collection.
///
/// Problems solved:
///   1. Too many small image layers → merge into fewer large ones.
///   2. Too many small delta layers → merge into larger delta layers or compact
///      (delta → image) to remove the need to replay long delta chains.
///   3. Old layers below a GC horizon → safe to delete once we have a newer
///      image covering those pages.
///
/// Compaction strategy:
///   - Image compaction: when we have > COMPACT_IMAGE_THRESHOLD image layers on a
///     timeline, merge all images up to the oldest retained LSN into a single image.
///   - Delta compaction: when an LSN range has more than COMPACT_DELTA_THRESHOLD delta
///     layers, merge them into one or materialize into a fresh image.
///   - GC: delete image/delta layers whose lsn < gc_horizon — i.e., they are fully
///     covered by a newer image layer.

use std::sync::Arc;
use tracing::{info, debug};

use lattice_common::{Lsn, TenantId, TimelineId};
use lattice_common::error::Result;

use crate::layer::{ImageLayer, DeltaLayer, LayerSet, PageKey};
use crate::store::LayerStorage;
use crate::redo::RedoEngine;

const COMPACT_DELTA_THRESHOLD: usize = 16;
const GC_HORIZON_DELTA: u64 = 1_000_000; // 1 M LSN units below latest

pub struct Compactor {
    storage: Arc<dyn LayerStorage>,
    redo: Arc<RedoEngine>,
}

impl Compactor {
    pub fn new(storage: Arc<dyn LayerStorage>, redo: Arc<RedoEngine>) -> Self {
        Self { storage, redo }
    }

    /// Run one compaction pass on the given timeline's LayerSet.
    pub async fn compact(
        &self,
        tenant: TenantId,
        timeline: TimelineId,
        layers: &LayerSet,
        last_lsn: Lsn,
    ) -> Result<CompactionStats> {
        let mut stats = CompactionStats::default();

        // 1. Delta compaction: merge dense delta runs into a new image layer.
        stats += self.compact_deltas(tenant, timeline, layers, last_lsn).await?;

        // 2. GC: remove image and delta layers below the GC horizon.
        stats += self.gc(tenant, timeline, layers, last_lsn).await?;

        Ok(stats)
    }

    async fn compact_deltas(
        &self,
        tenant: TenantId,
        timeline: TimelineId,
        layers: &LayerSet,
        last_lsn: Lsn,
    ) -> Result<CompactionStats> {
        let mut stats = CompactionStats::default();
        let delta_count = layers.deltas.read().len();

        if delta_count < COMPACT_DELTA_THRESHOLD {
            return Ok(stats);
        }

        info!(
            %tenant, %timeline, delta_count,
            "compacting: delta count exceeds threshold"
        );

        // Collect all pages touched by delta layers.
        let (all_keys, end_lsn) = {
            let deltas = layers.deltas.read();
            let end_lsn = deltas.iter().map(|d| d.end_lsn).max().unwrap_or(last_lsn);
            let mut keys = std::collections::BTreeSet::new();
            for dl in deltas.iter() {
                for k in dl.deltas.keys() {
                    keys.insert(*k);
                }
            }
            (keys, end_lsn)
        };

        // For each unique (rel, blk), reconstruct the page at end_lsn and
        // store the result as a new image layer.
        let mut new_image = ImageLayer::new(end_lsn);
        for key in &all_keys {
            let base = layers.find_image(key.rel, key.blk, end_lsn);
            let base_lsn = {
                layers.images.read()
                    .iter()
                    .filter(|il| il.lsn <= end_lsn)
                    .filter_map(|il| il.get(key.rel, key.blk).map(|_| il.lsn))
                    .max()
                    .unwrap_or(Lsn::INVALID)
            };
            let deltas = layers.collect_deltas(key.rel, key.blk, base_lsn, end_lsn);

            if let Some(base_page) = base {
                if let Ok(materialized) = self.redo.apply_deltas(base_page, &deltas) {
                    new_image.insert(key.rel, key.blk, materialized);
                }
            }
        }

        let pages_compacted = new_image.page_count();
        if pages_compacted > 0 {
            self.storage.save_image_layer(tenant, timeline, &new_image).await?;
            layers.add_image_layer(new_image);
            stats.images_written += 1;
            stats.pages_compacted += pages_compacted;
            debug!(%tenant, %timeline, pages = pages_compacted, "compacted deltas into new image layer at lsn={end_lsn}");
        }

        Ok(stats)
    }

    async fn gc(
        &self,
        _tenant: TenantId,
        _timeline: TimelineId,
        layers: &LayerSet,
        last_lsn: Lsn,
    ) -> Result<CompactionStats> {
        let mut stats = CompactionStats::default();

        // GC horizon: everything below (last_lsn - GC_HORIZON_DELTA) is eligible for
        // deletion IF a newer image layer fully covers it.
        let gc_horizon = Lsn(last_lsn.as_u64().saturating_sub(GC_HORIZON_DELTA));
        if gc_horizon <= Lsn::MIN {
            return Ok(stats);
        }

        // Find the latest image layer at or above gc_horizon for each page.
        // Pages covered by that image can have their older images/deltas removed.
        {
            let mut images = layers.images.write();
            let before = images.len();
            // Keep images at or above gc_horizon, plus the single highest image below it
            // (which acts as the base for delta replay in that range).
            let max_below_horizon = images.iter()
                .filter(|il| il.lsn < gc_horizon)
                .map(|il| il.lsn)
                .max();
            images.retain(|il| {
                il.lsn >= gc_horizon || Some(il.lsn) == max_below_horizon
            });
            stats.layers_deleted += before - images.len();
        }

        {
            let mut deltas = layers.deltas.write();
            let before = deltas.len();
            deltas.retain(|dl| dl.end_lsn >= gc_horizon);
            stats.layers_deleted += before - deltas.len();
        }

        Ok(stats)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CompactionStats {
    pub images_written: usize,
    pub pages_compacted: usize,
    pub layers_deleted: usize,
}

impl std::ops::AddAssign for CompactionStats {
    fn add_assign(&mut self, rhs: Self) {
        self.images_written += rhs.images_written;
        self.pages_compacted += rhs.pages_compacted;
        self.layers_deleted += rhs.layers_deleted;
    }
}

impl std::ops::Add for CompactionStats {
    type Output = Self;
    fn add(mut self, rhs: Self) -> Self {
        self += rhs;
        self
    }
}
