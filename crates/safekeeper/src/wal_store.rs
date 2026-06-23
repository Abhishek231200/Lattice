/// Durable WAL buffer — receives raw WAL bytes from Postgres, fsyncs them to an
/// append-only file, and exposes them to the pageserver via the WalSender.
///
/// File layout per timeline:
///   `{data_dir}/{tenant_id}/{timeline_id}/wal/wal.{segment_no:010}.bin`
///
/// Each segment is capped at `SEGMENT_SIZE` bytes.  Within a segment, entries are:
///   [ lsn: u64 LE ][ length: u32 LE ][ data: [u8; length] ]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::io::{Write, Seek, SeekFrom};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info};
use anyhow::Result;

use lattice_common::{Lsn, TenantId, TimelineId};
use lattice_common::proto::WalRecord;

const SEGMENT_SIZE: u64 = 256 * 1024 * 1024; // 256 MiB

#[derive(Clone)]
pub struct WalStore {
    inner: Arc<Mutex<WalStoreInner>>,
}

impl WalStore {
    pub fn open(data_dir: impl AsRef<Path>, tenant: TenantId, timeline: TimelineId) -> Result<Self> {
        let dir = data_dir.as_ref().join(tenant.to_string()).join(timeline.to_string()).join("wal");
        std::fs::create_dir_all(&dir)?;
        let inner = WalStoreInner::open(dir, tenant, timeline)?;
        Ok(Self { inner: Arc::new(Mutex::new(inner)) })
    }

    /// Append a WAL record at the given LSN (called from the receiver task).
    pub async fn append(&self, lsn: Lsn, data: Bytes) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.append(lsn, data)
    }

    /// Flush all buffered writes to disk (fsync).
    pub async fn flush(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.flush()
    }

    /// Read WAL records starting at `from_lsn` up to the end of what we have.
    pub fn read_from(&self, from_lsn: Lsn) -> Result<Vec<WalRecord>> {
        let inner = self.inner.lock();
        inner.read_from(from_lsn)
    }

    pub fn last_lsn(&self) -> Lsn {
        self.inner.lock().last_lsn
    }
}

struct WalStoreInner {
    dir: PathBuf,
    tenant: TenantId,
    timeline: TimelineId,
    current_file: Option<std::fs::File>,
    current_segment: u64,
    current_offset: u64,
    last_lsn: Lsn,
    write_buf: Vec<u8>,
}

impl WalStoreInner {
    fn open(dir: PathBuf, tenant: TenantId, timeline: TimelineId) -> Result<Self> {
        // Find the latest segment file.
        let mut last_segment = 0u64;
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("wal.") && name.ends_with(".bin") {
                        if let Ok(n) = name[4..name.len()-4].parse::<u64>() {
                            last_segment = last_segment.max(n);
                        }
                    }
                }
            }
        }

        let mut store = Self {
            dir,
            tenant,
            timeline,
            current_file: None,
            current_segment: last_segment,
            current_offset: 0,
            last_lsn: Lsn::INVALID,
            write_buf: Vec::with_capacity(64 * 1024),
        };
        store.open_segment(last_segment)?;
        Ok(store)
    }

    fn segment_path(&self, seg: u64) -> PathBuf {
        self.dir.join(format!("wal.{seg:010}.bin"))
    }

    fn open_segment(&mut self, seg: u64) -> Result<()> {
        let path = self.segment_path(seg);
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        // Find current offset (end of file).
        let offset = file.seek(SeekFrom::End(0))?;
        self.current_file = Some(file);
        self.current_segment = seg;
        self.current_offset = offset;
        debug!("opened WAL segment {} at offset {}", path.display(), offset);
        Ok(())
    }

    fn append(&mut self, lsn: Lsn, data: Bytes) -> Result<()> {
        // If current segment is full, rotate.
        if self.current_offset >= SEGMENT_SIZE {
            self.flush()?;
            let next = self.current_segment + 1;
            info!("rotating WAL segment to {}", next);
            self.open_segment(next)?;
        }

        // Write entry header + data to the write buffer.
        self.write_buf.extend_from_slice(&lsn.as_u64().to_le_bytes());
        self.write_buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        self.write_buf.extend_from_slice(&data);
        self.current_offset += 12 + data.len() as u64;

        if lsn > self.last_lsn {
            self.last_lsn = lsn;
        }

        // Flush every 64 KiB.
        if self.write_buf.len() >= 64 * 1024 {
            self.flush()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.write_buf.is_empty() {
            return Ok(());
        }
        if let Some(file) = &mut self.current_file {
            file.write_all(&self.write_buf)?;
            file.sync_data()?;
        }
        self.write_buf.clear();
        Ok(())
    }

    fn read_from(&self, from_lsn: Lsn) -> Result<Vec<WalRecord>> {
        let mut records = Vec::new();
        // Scan all segments.
        let mut seg = 0u64;
        loop {
            let path = self.segment_path(seg);
            if !path.exists() {
                break;
            }
            let data = std::fs::read(&path)?;
            let mut pos = 0usize;
            while pos + 12 <= data.len() {
                let lsn = Lsn(u64::from_le_bytes(data[pos..pos+8].try_into().unwrap()));
                let len = u32::from_le_bytes(data[pos+8..pos+12].try_into().unwrap()) as usize;
                pos += 12;
                if pos + len > data.len() {
                    break;
                }
                if lsn >= from_lsn {
                    records.push(WalRecord {
                        lsn,
                        data: data[pos..pos+len].to_vec(),
                    });
                }
                pos += len;
            }
            seg += 1;
        }
        Ok(records)
    }
}
