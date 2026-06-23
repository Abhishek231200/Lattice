# Lattice

**A miniature serverless-Postgres platform** that separates compute from storage.

Lattice stores Postgres pages as versioned **image + delta layers** keyed by LSN, creates
database branches in milliseconds via **copy-on-write timeline metadata** (footprint
proportional to the delta, not the database size), ingests real Postgres WAL, and runs a
closed-loop **autoscaler** that suspends idle compute and scales under load while protecting
a p99 SLO.

Built in Rust, cloud-agnostic behind a storage abstraction, with benchmarks for branch
latency, storage amplification, and compute saved vs static provisioning.

---

## Architecture

```
                     ┌────────────────────────────────┐
                     │        Control Plane            │
                     │  (REST API + Postgres meta DB)  │
                     │  - create tenant / branch       │
                     │  - start/stop compute           │
                     │  - autoscaler control loop      │
                     └───────────┬──────────┬──────────┘
                                 │          │
              scale up/down /    │          │  branch/timeline ops
              suspend            │          │
                                 ▼          ▼
         ┌──────────────────┐  ┌─────────────────────────────────┐
         │  Compute node     │  │          Pageserver             │
         │  (Postgres 16 +   │  │  - timelines (COW branching)   │
         │   lattice_smgr    │──▶  - image/delta layers          │
         │   C extension)    │  │  - get_page_at_lsn             │
         └────────┬──────────┘  │  - compaction / GC             │
                  │ WAL stream  └───────────┬─────────────────────┘
                  ▼                         │ layers
         ┌───────────────────┐     ┌────────▼──────────────┐
         │    Safekeeper      │────▶│   Object Storage      │
         │  - durable WAL     │     │   (MinIO / S3 / GCS)  │
         │  - fsync segments  │     └───────────────────────┘
         └───────────────────┘
```

**Write path**: Postgres → WAL → Safekeeper (durable) → Pageserver redo → delta layers → object store

**Read path**: Compute asks pageserver `get_page_at_lsn` → pageserver finds layers (recursing into parent timeline if needed) → returns 8 KiB page

**Branch**: pure metadata — record `(child_timeline, parent_timeline, branch_lsn)`, copy zero pages

---

## Quick Start

```bash
# Build all Rust crates
make build

# Run tests (includes all Phase 1–3 correctness proofs)
make test

# Start the full stack (requires Docker)
make demo

# Benchmark: branch creation latency vs DB size
make bench-branch

# Autoscaler demo (requires pgbench)
make bench-autoscaler
```

After `make demo`:

| Service | URL |
|---|---|
| Pageserver | http://localhost:5000 |
| Control Plane | http://localhost:5002 |
| Grafana Dashboard | http://localhost:3000 (admin/lattice) |
| MinIO Console | http://localhost:9001 (lattice/lattice123) |
| Prometheus | http://localhost:9090 |

---

## Repository Structure

```
lattice/
├── crates/
│   ├── common/          # Lsn, TenantId, TimelineId, RelTag, BlobStore trait
│   ├── pageserver/      # layer.rs, timeline.rs, redo.rs, compaction.rs
│   ├── safekeeper/      # WAL receiver (streaming replication protocol)
│   ├── control-plane/   # REST API, autoscaler, Docker orchestrator
│   └── compute-shim/    # Page cache + pageserver HTTP proxy
├── lattice_smgr/        # C extension: Postgres smgr hook → compute-shim
├── deploy/
│   ├── docker-compose.yml
│   ├── grafana/         # Pre-provisioned autoscaler dashboard
│   └── Dockerfile.*
├── bench/scenarios/     # branch_latency.sh, autoscaler_demo.sh
└── docs/
    ├── concepts.md      # LSN, WAL, page, timeline, BlobStore explained
    └── benchmarks.md    # Results + methodology
```

---

## Key Design Decisions

### Copy-on-Write Branching (O(1))
`create_branch(parent, at_lsn)` writes one metadata record. Zero pages are copied.
Reads on the child recurse to the parent for any LSN ≤ branch_lsn. This makes branch
creation time independent of database size — measured at ~200 μs regardless of whether
the parent has 1 MB or 10 GB of data.

### Layered Storage (Image + Delta)
Rather than storing every full page on every write, Lattice stores:
- **ImageLayer**: materialized snapshot at a given LSN.
- **DeltaLayer**: WAL-derived patches over an LSN range.

`get_page_at_lsn(rel, blk, lsn)` = find latest image ≤ lsn, replay deltas on top.
Storage used by N updates to one page is ~1 image + N small deltas, not N full pages.

### WAL Redo (Documented Subset)
Supported: FPI (full-page images), heap INSERT/UPDATE/DELETE, B-tree splits.
Unsupported (documented, not silently ignored): SMGR_TRUNCATE, GIN/GIST index records,
subtransaction records. The pageserver falls back to storing the raw WAL record for
unsupported types and logs a warning — no silent data loss.

### Autoscaler with SLO Guard
The autoscaler never scales down if p99 latency is near the SLO ceiling, preventing
the classic "scale down → latency spike → scale up → flap" cycle. Hysteresis (separate
up/down cooldowns) prevents oscillation.

### Cloud-Agnostic Storage
All persistence goes through the `BlobStore` trait. Ships with `LocalFsStore` (dev),
`BlobLayerStorage` over MinIO/S3 (staging/prod), and stubs for GCS and Azure Blob.
No cloud-specific calls leak into pageserver or control-plane logic.

---

## Honest Scope Note
This is a portfolio-grade, conceptually faithful reimplementation of the core ideas behind
a serverless-Postgres control plane. It is not a production system.

What's real:
- The page versioning model and `get_page_at_lsn` are correct and tested.
- Copy-on-write branching is genuinely O(1) and measured.
- WAL ingestion uses the real Postgres streaming replication protocol.
- The autoscaler implements hysteresis, SLO guards, and scale-to-zero.

What's simplified:
- The C extension (`lattice_smgr`) uses HTTP instead of shared memory for the shim call.
- The redo engine handles a documented subset of WAL record types.
- No distributed consensus for the safekeeper (single-node WAL store).
- No key-range sharding across multiple pageserver instances (designed for, not built).

---

## Benchmarks

See [`docs/benchmarks.md`](docs/benchmarks.md) for full results. Highlights:

- **Branch latency**: < 5 ms at any DB size (target: < 6,000 ms)
- **Storage amplification**: ~0 bytes added for unmodified branch pages
- **Autoscaler savings**: ~60–80% compute-seconds vs static peak provisioning
- **p99 get_page_at_lsn**: < 1 ms (local layers), < 50 ms (cold object storage)