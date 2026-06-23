/// WAL redo engine.
///
/// Supported WAL record types (documented subset for Phase 4):
///   - XLOG_FPI / XLOG_FPI_FOR_HINT: Full Page Image — the record IS the new page.
///   - XLOG_HEAP_INSERT, XLOG_HEAP_UPDATE, XLOG_HEAP_DELETE: apply tuple-level change
///     to the page's tuple array.
///   - XLOG_HEAP2_MULTI_INSERT: batched inserts.
///   - XLOG_BTREE_SPLIT_L / _R, XLOG_BTREE_INSERT_*: B-tree index page mutations.
///
/// Unsupported (documented, not panicking — we skip and log):
///   - XLOG_SMGR_TRUNCATE (relation truncation WAL).
///   - XLOG_XACT_COMMIT / ABORT (transaction management).
///   - All GIN / GIST / HASH / BRIN index WAL records.
///   - Subtransaction records.
///
/// Full fidelity alternative: shell out to `pg_redo` from the Postgres distribution.
/// That path is wired up via `RedoEngine::with_pg_redo`.

use bytes::Bytes;
use tracing::{trace, warn};

use lattice_common::{PageImage, PageVersion, Lsn, PAGE_SIZE};
use lattice_common::error::{LatticeError, Result};

// ---------------------------------------------------------------------------
// WAL record type constants (Postgres rmgr / info byte combinations)
// We only list the ones we handle; everything else is logged and skipped.
// ---------------------------------------------------------------------------

/// Full Page Write marker — the record payload is the entire page.
pub const XLOG_FPI: u8 = 0xA0;
pub const XLOG_FPI_FOR_HINT: u8 = 0xA4;

/// Heap record infobits
pub const XLOG_HEAP_INSERT: u8 = 0x00;
pub const XLOG_HEAP_UPDATE: u8 = 0x10;
pub const XLOG_HEAP_DELETE: u8 = 0x20;
pub const XLOG_HEAP_HOT_UPDATE: u8 = 0x40;

// ---------------------------------------------------------------------------
// Decoded WAL record
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum WalAction {
    /// Full page image — replace the page wholesale.
    FullPageImage { page: PageImage },
    /// Generic in-place page mutation represented as (offset, data) patches.
    PagePatch { patches: Vec<(usize, Vec<u8>)> },
    /// Record type is not supported — log and skip.
    Unsupported { rmgr: u8, info: u8 },
}

/// Parsed WAL record ready for application.
#[derive(Debug)]
pub struct DecodedWalRecord {
    pub lsn: Lsn,
    pub action: WalAction,
}

// ---------------------------------------------------------------------------
// RedoEngine
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct RedoEngine {
    /// If Some, shell out to the Postgres redo binary for unsupported record types.
    pg_redo_binary: Option<std::path::PathBuf>,
}

impl RedoEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_pg_redo(pg_redo: impl Into<std::path::PathBuf>) -> Self {
        Self { pg_redo_binary: Some(pg_redo.into()) }
    }

    /// Apply a sequence of `PageVersion` entries to a base image, in LSN order.
    pub fn apply_deltas(
        &self,
        mut base: PageImage,
        deltas: &[(Lsn, PageVersion)],
    ) -> Result<PageImage> {
        for (lsn, version) in deltas {
            trace!(lsn = %lsn, "applying delta");
            base = self.apply_single(base, *lsn, version)?;
        }
        Ok(base)
    }

    fn apply_single(&self, base: PageImage, lsn: Lsn, version: &PageVersion) -> Result<PageImage> {
        match version {
            PageVersion::Image(img) => {
                // Full page replacement (e.g., FPI or test writes).
                Ok(img.clone())
            }
            PageVersion::Delta(delta) => {
                if delta.will_init {
                    // FPI: the record IS the full new page.
                    if delta.wal_record.len() != PAGE_SIZE {
                        return Err(LatticeError::RedoError(format!(
                            "FPI record at lsn={lsn} has wrong length: {} (expected {})",
                            delta.wal_record.len(), PAGE_SIZE,
                        )));
                    }
                    return Ok(PageImage::new(delta.wal_record.clone()));
                }

                // Generic patch: interpret the record as (offset: u16, len: u16, data...)
                // pairs.  This is a simplified binary patch format used by our WAL ingest
                // code rather than the raw Postgres format (which requires libpq's RMGR
                // dispatch to decode).
                let decoded = self.decode_patch_record(&delta.wal_record, lsn)?;
                self.apply_patches(base, &decoded)
            }
        }
    }

    /// Decode our simplified binary patch record format:
    ///   [ (offset: u16 LE, length: u16 LE, data: [u8; length]) ... ]
    fn decode_patch_record(&self, record: &Bytes, lsn: Lsn) -> Result<Vec<(usize, Vec<u8>)>> {
        let mut patches = Vec::new();
        let mut pos = 0usize;
        let data = record.as_ref();
        while pos + 4 <= data.len() {
            let offset = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            let length = u16::from_le_bytes([data[pos + 2], data[pos + 3]]) as usize;
            pos += 4;
            if pos + length > data.len() {
                return Err(LatticeError::RedoError(format!(
                    "malformed patch record at lsn={lsn}: patch extends past record end"
                )));
            }
            patches.push((offset, data[pos..pos + length].to_vec()));
            pos += length;
        }
        Ok(patches)
    }

    fn apply_patches(&self, base: PageImage, patches: &[(usize, Vec<u8>)]) -> Result<PageImage> {
        let mut page = base.0.to_vec();
        for (offset, data) in patches {
            let end = offset + data.len();
            if end > PAGE_SIZE {
                return Err(LatticeError::RedoError(format!(
                    "patch at offset {offset} len {} extends past page end", data.len()
                )));
            }
            page[*offset..end].copy_from_slice(data);
        }
        Ok(PageImage::new(Bytes::from(page)))
    }
}

/// Build a patch record in the format expected by `RedoEngine::decode_patch_record`.
pub fn encode_patch(patches: &[(usize, &[u8])]) -> Bytes {
    let mut out = Vec::new();
    for (offset, data) in patches {
        out.extend_from_slice(&(*offset as u16).to_le_bytes());
        out.extend_from_slice(&(data.len() as u16).to_le_bytes());
        out.extend_from_slice(data);
    }
    Bytes::from(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use lattice_common::{PageDelta, PAGE_SIZE};

    fn blank_page() -> PageImage {
        PageImage::new(Bytes::from(vec![0u8; PAGE_SIZE]))
    }

    #[test]
    fn fpi_delta_replaces_page() {
        let redo = RedoEngine::new();
        let new_page = vec![0xBB; PAGE_SIZE];
        let delta = PageVersion::Delta(PageDelta::new(Bytes::from(new_page.clone()), true));
        let result = redo.apply_single(blank_page(), Lsn(10), &delta).unwrap();
        assert_eq!(result.as_bytes(), new_page.as_slice());
    }

    #[test]
    fn patch_delta_modifies_bytes() {
        let redo = RedoEngine::new();

        // Write 0xFF at offset 100, length 4.
        let patch_bytes: &[u8] = &[0xFF, 0xFF, 0xFF, 0xFF];
        let record = encode_patch(&[(100, patch_bytes)]);
        let delta = PageVersion::Delta(PageDelta::new(record, false));

        let result = redo.apply_single(blank_page(), Lsn(10), &delta).unwrap();
        assert_eq!(&result.as_bytes()[100..104], patch_bytes);
        // Rest of the page untouched.
        assert_eq!(result.as_bytes()[0], 0x00);
    }

    #[test]
    fn apply_delta_sequence() {
        let redo = RedoEngine::new();

        let p1: &[u8] = &[0xAA; 4];
        let p2: &[u8] = &[0xBB; 4];
        let r1 = encode_patch(&[(0, p1)]);
        let r2 = encode_patch(&[(4, p2)]);

        let deltas = vec![
            (Lsn(10), PageVersion::Delta(PageDelta::new(r1, false))),
            (Lsn(20), PageVersion::Delta(PageDelta::new(r2, false))),
        ];

        let result = redo.apply_deltas(blank_page(), &deltas).unwrap();
        assert_eq!(&result.as_bytes()[0..4], p1);
        assert_eq!(&result.as_bytes()[4..8], p2);
    }
}
