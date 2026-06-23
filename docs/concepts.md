# Lattice — Core Concepts

## LSN (Log Sequence Number)
A `u64` that is a monotonically increasing position in Postgres's write-ahead log (WAL).
Every page version in Lattice corresponds to "the state of this page **as of LSN X**."
LSNs are the universal key for versioning — not timestamps, not snapshot IDs.

In Postgres format: `X/XXXXXXXX` (segment number / offset within segment, hex).

## WAL (Write-Ahead Log)
Postgres's append-only redo log.  Every change to a page is recorded as a WAL record
*before* the page is modified on disk.  If you have the base page plus all WAL records
from LSN A to LSN B, you can reconstruct the exact page state at any LSN in [A, B].

WAL records come in types — heap inserts/updates/deletes, B-tree splits, full-page
images (FPIs) written after a checkpoint.  Lattice supports a documented subset
(see `crates/pageserver/src/redo.rs`).

## Page (Block)
Postgres stores all relation data in fixed-size 8 KiB pages.  The atomic unit of
storage and versioning in Lattice is:

```
(tenant_id, timeline_id, rel_tag, block_number) @ LSN
```

`rel_tag` = `(spcnode, dbnode, relnode, forknum)` — maps to a Postgres relation file.

## Image Layer
A materialized snapshot of a set of pages at a single LSN.  Stored as a blob in
object storage.  Key: `image_{lsn:016X}.layer`.

An image layer is the "base" from which deltas are replayed.

## Delta Layer
WAL-derived page changes over an LSN range `[start_lsn, end_lsn)`.  Each entry is
`(rel, blk, lsn) → PageVersion` where `PageVersion` is either a full page image (FPI)
or a WAL patch.

Delta layers accumulate until compaction merges them into a new image layer.

## `get_page_at_lsn(rel, blk, lsn)` — the central read API

```
1. Find the latest ImageLayer with image.lsn <= lsn that contains (rel, blk).
2. Collect all DeltaLayer entries for (rel, blk) in (image.lsn, lsn].
3. Apply deltas in LSN order to the base image (redo engine).
4. If no image found on this timeline AND there is a parent timeline:
   recurse to parent.get_page_at_lsn(rel, blk, min(lsn, branch_lsn))
   then apply this timeline's own deltas on top.
```

If you understand this function plus timelines, you understand 80% of the storage half.

## Timeline (Branch)
A branch of the page history.  Defined by:

```rust
struct TimelineMeta {
    id:         TimelineId,
    tenant_id:  TenantId,
    parent_id:  Option<TimelineId>,
    branch_lsn: Lsn,   // point in parent's history where this branch diverged
    last_lsn:   Lsn,
    name:       String,
}
```

**Copy-on-write semantics**: at branch creation time, *zero pages are copied*.
Instead, reads on the child timeline fall through to the parent for any LSN ≤ branch_lsn.
Writes on the child create new delta layers tagged to the child timeline only — the
parent's layers are immutable.

This is why `create_branch` is O(1) and independent of database size.

## BlobStore
A cloud-agnostic storage abstraction:

```rust
trait BlobStore {
    async fn get(key: &str) -> Result<Bytes>;
    async fn put(key: &str, data: Bytes) -> Result<()>;
    async fn list(prefix: &str) -> Result<Vec<String>>;
    async fn delete(key: &str) -> Result<()>;
}
```

Implementations: `LocalFsStore` (dev/tests), `BlobLayerStorage` over MinIO/S3 (prod).
GCS and Azure Blob are stub-wired — implement the trait to add them.

## Safekeeper
Receives WAL from Postgres via the streaming replication protocol.
Appends records durably (fsync'd segments on disk) before forwarding to the pageserver.
Acts as a durable WAL buffer so the pageserver can safely lag behind Postgres.

## Autoscaler
A closed control loop that:
- Suspends idle endpoints (scale-to-zero).
- Scales up when CPU utilization crosses the high-water mark.
- Scales down when utilization is low AND p99 latency is below the SLO.
- Uses hysteresis (separate up/down cooldowns) to prevent flapping.

See `crates/control-plane/src/autoscaler.rs`.
