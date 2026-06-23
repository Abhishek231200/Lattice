use serde::{Deserialize, Serialize};
use bytes::Bytes;

/// Postgres page size: 8 KiB.
pub const PAGE_SIZE: usize = 8192;

/// Identifies a Postgres relation (table, index, etc.) within a database.
/// Maps directly to Postgres's `RelFileNode` structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RelTag {
    /// Tablespace OID (0 = default)
    pub spcnode: u32,
    /// Database OID
    pub dbnode: u32,
    /// Relation OID
    pub relnode: u32,
    /// Fork number: 0=main, 1=FSM, 2=VM, 3=init
    pub forknum: u8,
}

impl RelTag {
    pub fn new(spcnode: u32, dbnode: u32, relnode: u32, forknum: u8) -> Self {
        Self { spcnode, dbnode, relnode, forknum }
    }

    /// Convenience ctor for the main fork of a relation.
    pub fn main(spcnode: u32, dbnode: u32, relnode: u32) -> Self {
        Self::new(spcnode, dbnode, relnode, 0)
    }
}

impl std::fmt::Display for RelTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}/{}", self.spcnode, self.dbnode, self.relnode, self.forknum)
    }
}

/// Block number within a relation (0-indexed).
pub type BlockNumber = u32;

/// A full materialized 8 KiB Postgres page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageImage(pub Bytes);

impl PageImage {
    pub fn new(data: impl Into<Bytes>) -> Self {
        let b: Bytes = data.into();
        assert_eq!(b.len(), PAGE_SIZE, "PageImage must be exactly {PAGE_SIZE} bytes");
        Self(b)
    }

    /// Create a zeroed page (Postgres uses all-zero as uninitialized).
    pub fn zeroed() -> Self {
        Self(Bytes::from(vec![0u8; PAGE_SIZE]))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// A WAL-derived delta that, when applied to a page, brings it forward by one record.
/// We store the raw WAL record bytes and the target page — the redo engine interprets these.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageDelta {
    /// The WAL record bytes as received from Postgres.
    pub wal_record: Bytes,
    /// Whether to apply this delta to the existing page (false = full page write, i.e., FPW).
    pub will_init: bool,
}

impl PageDelta {
    pub fn new(wal_record: impl Into<Bytes>, will_init: bool) -> Self {
        Self {
            wal_record: wal_record.into(),
            will_init,
        }
    }
}

/// The versioned content stored for a (rel, block) at a given LSN — either a full image
/// or a WAL delta to be replayed on top of an earlier image.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PageVersion {
    Image(PageImage),
    Delta(PageDelta),
}
