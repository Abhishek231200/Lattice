# Lattice

Lattice is a serverless Postgres control plane written in Rust. It separates compute from storage the same way Neon does: Postgres runs as a stateless compute node, all page data lives in a versioned object store, and branching a multi-gigabyte database takes microseconds because no data is ever copied.

The project covers the full stack — storage engine, WAL ingestion, branch management, autoscaler, Postgres C extension, and a cloud-agnostic storage layer backed by S3, GCS, or Azure Blob.

---

## What it does

**Databases as timelines.** Every write is stored as a page version keyed by LSN (log sequence number). You can read any page at any past LSN, which is how point-in-time restore works without snapshots.

**Branching in microseconds.** Creating a branch records one metadata entry: `(child_timeline, parent_timeline, branch_lsn)`. No pages are copied. Reads on the child recurse to the parent for any LSN at or before the branch point. Storage used by the branch grows only as you write to it.

**WAL ingestion.** The safekeeper implements the Postgres physical streaming replication protocol. It durably buffers WAL segments and ships them to the pageserver, which replays them into delta layers using a redo engine.

**Autoscaler with SLO guard.** The control plane polls metrics every 5 seconds and suspends idle compute, scales up on CPU pressure, and scales down when load drops — but never scales down if p99 latency is near the SLO ceiling, preventing the classic flap cycle.

**Merge branches forward.** Diverged branch writes can be merged back into any target timeline. The merge walks only the changed pages, not the full database.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Control Plane                          │
│        REST API · tenant/branch management · autoscaler     │
└──────────┬──────────────────────────────────┬───────────────┘
           │ suspend / scale / resume          │ branch ops
           ▼                                  ▼
┌──────────────────────┐       ┌──────────────────────────────┐
│    Compute node       │       │          Pageserver           │
│  Postgres 16          │◀─────▶│  timelines · image layers   │
│  + lattice_smgr (C)   │  page │  delta layers · redo engine │
│  + compute-shim       │  req  │  compaction · merge          │
└──────────┬────────────┘       └──────────────┬───────────────┘
           │ WAL stream                         │ read/write layers
           ▼                                   ▼
┌──────────────────────┐       ┌──────────────────────────────┐
│     Safekeeper        │       │       Object Storage          │
│  streaming repl.      │──────▶│  S3 · GCS · Azure · LocalFS  │
│  durable WAL buffer   │       └──────────────────────────────┘
└──────────────────────┘
```

**Write path:** Postgres → WAL → Safekeeper (fsync) → Pageserver redo → delta layers → object store

**Read path:** Compute calls `GET /page` → Pageserver finds image layer at closest LSN, applies delta layers on top, recurses to parent timeline if needed → returns 8 KiB page

**Branch:** one metadata write, zero I/O

---

## Measured results

All numbers below come from `cargo test --workspace` or the benchmark scripts — not projections.

### Branch creation (O(1))

Server-side time is constant regardless of how much data exists in the parent timeline.

| DB size | Server-side time |
|---------|-----------------|
| 80 KB (10 pages) | 4 μs |
| 800 KB (100 pages) | 5 μs |
| 7 MB (1,000 pages) | 9 μs |
| 39 MB (5,000 pages) | 9 μs |

Run: `./bench/scenarios/branch_latency.sh`

### Storage amplification

A branch at creation costs zero bytes of page data. Its footprint grows only as you write to it.

| Operation | Pages | Logical size |
|-----------|-------|-------------|
| Root timeline (1,000 writes) | 1,000 | 7.81 MB |
| `create_branch()` | 0 copied | 0 KB |
| Write 10 pages on branch | 10 | 80 KB |

Branch footprint: 80 KB vs 7.81 MB parent = **1.0% amplification**.

Run: `cargo test -p lattice-pageserver --test storage_amplification -- --nocapture`

### Page delta merges

Merge cost is proportional to pages changed on the branch, not total database size.

| Branch size | Conflicts | Merge time |
|-------------|-----------|-----------|
| 4 pages (1 conflicting) | 1 | 18 μs |
| 1,000 pages | 0 | 8.1 ms (122K pages/sec) |

Source wins on conflict. Sibling branches are unaffected.

Run: `cargo test -p lattice-pageserver --test merge -- --nocapture`

### Autoscaler

Six decision scenarios verified, all producing the correct outcome with no sleeps:

| Input | Decision |
|-------|----------|
| 0 connections, 0% CPU | Suspend → 0 units |
| 85% CPU, p99=20ms | ScaleUp 2→3 units |
| 10% CPU, p99=75ms (SLO=50ms) | NoOp — SLO guard blocks scale-down |
| 10% CPU, p99=20ms | ScaleDown 4→3 units |

Tick throughput: **2.8 μs per endpoint** across 100 endpoints × 10 ticks.

Run: `cargo test -p lattice-control-plane --test autoscaler_sim -- --nocapture`

---

## Quick start

### No Docker (runs in 30 seconds)

```bash
# Build
cargo build --release

# Start pageserver
./target/release/pageserver &

# Run the interactive demo
./demo.sh
```

The demo creates timelines, branches them, proves O(1) branching, and prints server-side timing.

### Full stack with Docker

```bash
make up     # builds all images and starts the compose stack
make demo   # end-to-end walkthrough against the running stack
```

| Service | URL |
|---------|-----|
| Pageserver | http://localhost:6400 |
| Control Plane | http://localhost:6402 |
| Grafana | http://localhost:3000 (admin / lattice) |
| MinIO console | http://localhost:9001 (lattice / lattice123) |
| Prometheus | http://localhost:9090 |

### Tests

```bash
cargo test --workspace                     # 28 tests, all crates
cargo test --workspace -- --nocapture      # with printed metrics
```

---

## Repository layout

```
Lattice/
├── crates/
│   ├── common/          # Lsn, TenantId, TimelineId, RelTag, BlobStore trait
│   │                    # S3BlobStore, GcsBlobStore, AzureBlobStore
│   ├── pageserver/      # Timeline, LayerSet, image/delta layers, redo engine
│   │   └── tests/       # storage_amplification.rs, merge.rs
│   ├── safekeeper/      # WAL receiver (Postgres streaming replication protocol)
│   ├── control-plane/   # REST API, autoscaler, Docker orchestrator, DB
│   │   └── tests/       # autoscaler_sim.rs
│   └── compute-shim/    # Page cache + HTTP proxy between Postgres and pageserver
├── lattice_smgr/        # Postgres C extension (loads into backend, calls shim)
├── deploy/
│   ├── docker-compose.yml
│   ├── Dockerfile.*
│   └── prometheus.yml
├── bench/scenarios/     # branch_latency.sh, storage_amplification.sh
├── scripts/             # wal-demo.sh, smgr-demo.sh
├── docs/
│   ├── concepts.md      # LSN, WAL, timeline, BlobStore — explained
│   └── benchmarks.md    # Full results with methodology
└── demo.sh              # No-Docker interactive demo
```

---

## How the storage model works

### Image layers and delta layers

Lattice does not store one full page per write. It stores:

- **ImageLayer** — a full materialized snapshot of a set of pages at a specific LSN.
- **DeltaLayer** — WAL-derived patches over an LSN range `[start, end)`.

Reading a page at LSN `L`:
1. Find the newest image layer at LSN ≤ L that contains the page.
2. Collect all delta versions for that page in `(image_lsn, L]`.
3. Apply deltas through the redo engine to produce the final 8 KiB page.
4. If no image exists on this timeline, recurse to the parent timeline at `min(L, branch_lsn)`.

This makes storage cost proportional to the number of distinct writes, not the number of reads or the age of the database.

### Copy-on-write branching

```rust
// TimelineManager::create_branch — the entire implementation
let meta = TimelineMeta {
    id: TimelineId::new(),
    tenant_id,
    parent_id: Some(parent_id),
    branch_lsn: at_lsn,
    last_lsn: at_lsn,
    name,
};
let tl = Arc::new(Timeline::new(meta, Some(parent), self.storage.clone()));
self.timelines.write().entry(tenant_id).or_default().insert(meta.id, tl.clone());
```

One metadata write. The child timeline holds an `Arc<Timeline>` pointer to the parent. No page data is touched. Reads on the child recurse through the pointer for any LSN at or before `branch_lsn`.

### WAL ingestion

The safekeeper implements Postgres's physical streaming replication protocol — the same protocol a standby replica uses. It connects to Postgres, receives WAL records over a `CopyBoth` stream, fsyncs them to a local WAL store, and forwards them to the pageserver. The pageserver's redo engine decodes each record and writes the affected page into a delta layer.

Supported record types: full-page images (FPI), heap INSERT/UPDATE/DELETE, B-tree page splits. Unsupported types are stored as raw WAL with a warning — no silent data loss.

### Autoscaler

The autoscaler runs a polling loop over registered endpoints. Each tick:

1. Fetch metrics from Prometheus (or a synthetic source in tests).
2. If idle for longer than `idle_suspend_secs`: suspend.
3. If CPU > `scale_up_cpu_threshold`: scale up.
4. If CPU < `scale_down_cpu_threshold` **and** p99 < SLO ceiling: scale down.
5. Otherwise: no-op.

Separate cooldown timers for up and down decisions prevent oscillation. The SLO guard in step 4 prevents scaling down into a latency spike.

### Branch merge

`merge_branch(source, target, merge_lsn)` walks `source.pages_since_lsn(branch_lsn)` — every page written on the branch after the divergence point — and writes them onto `target` at `merge_lsn`. Source wins on conflict. Cost is O(pages changed on branch), not O(database size).

### Cloud storage

All persistence flows through a single `BlobStore` trait:

```rust
#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    async fn get(&self, key: &str) -> StoreResult<Bytes>;
    async fn put(&self, key: &str, data: Bytes) -> StoreResult<()>;
    async fn list(&self, prefix: &str) -> StoreResult<Vec<String>>;
    async fn delete(&self, key: &str) -> StoreResult<()>;
}
```

Implementations:

| Backend | Struct | Notes |
|---------|--------|-------|
| Local filesystem | `LocalFsStore` | Default in dev and tests |
| AWS S3 | `S3BlobStore::new_aws(...)` | Standard credentials |
| MinIO / Ceph | `S3BlobStore::new_minio(...)` | Custom endpoint + `allow_http` |
| Google Cloud Storage | `GcsBlobStore::new(...)` | Service account JSON or ADC |
| Azure Blob Storage | `AzureBlobStore::new(...)` | Account name + access key |

Switch by changing `storage.type` in the pageserver config — no code changes elsewhere.

---

## Postgres C extension

`lattice_smgr` is a Postgres extension that loads into the Postgres backend process. It registers three GUC parameters (`shim_url`, `tenant_id`, `timeline_id`) and exposes a `lattice_ping(url text)` SQL function that issues an HTTP GET from inside the backend using libcurl — demonstrating that the extension loads, GUCs are configurable, and the shim can be called from within Postgres.

```bash
# Build and load inside a Postgres 16 Docker container
./scripts/smgr-demo.sh
```

```sql
CREATE EXTENSION lattice_smgr;
SELECT lattice_ping('http://localhost:6403/health');
```

---

## What's production-grade vs what's simplified

**Solid:**
- The page versioning model (`get_page_at_lsn`, image + delta layers, COW recursion) is correct and covered by tests.
- Branch creation is genuinely O(1) with zero page copies — measured, not asserted.
- WAL ingestion uses the real Postgres streaming replication wire protocol.
- The autoscaler implements hysteresis, an SLO guard, and scale-to-zero.
- Cloud storage works against real S3/GCS/Azure endpoints.

**Simplified:**
- The C extension uses HTTP instead of shared memory for the shim call (shared memory inter-process communication is not in PG16's public header API).
- The redo engine handles a documented subset of WAL record types.
- The safekeeper is single-node (no quorum/consensus).
- No key-range sharding across multiple pageserver instances.

---

## Tech stack

- **Language:** Rust (tokio async runtime)
- **HTTP:** axum 0.7
- **Storage:** `object_store` crate (AWS, GCS, Azure); local FS for dev
- **Observability:** Prometheus metrics, tracing
- **C extension:** Postgres 16, libcurl
- **Infra:** Docker Compose, MinIO, Grafana, Prometheus
