/// Wire protocol types for pageserver <-> compute-shim and safekeeper <-> pageserver communication.
use serde::{Deserialize, Serialize};
use crate::{Lsn, TenantId, TimelineId, RelTag, BlockNumber};

// ---------------------------------------------------------------------------
// Pageserver protocol
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct GetPageRequest {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub rel: RelTag,
    pub blk: BlockNumber,
    pub lsn: Lsn,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetPageResponse {
    pub page: Vec<u8>,
    /// The LSN at which the page was actually materialized (may be < requested).
    pub effective_lsn: Lsn,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PutPageRequest {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub rel: RelTag,
    pub blk: BlockNumber,
    pub lsn: Lsn,
    pub page: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Safekeeper protocol (WAL streaming)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BeginStreaming {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub start_lsn: Lsn,
}

// ---------------------------------------------------------------------------
// Control plane protocol
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTenantRequest {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTenantResponse {
    pub tenant_id: TenantId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateBranchRequest {
    pub tenant_id: TenantId,
    pub parent_timeline_id: Option<TimelineId>,
    pub branch_lsn: Option<Lsn>,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateBranchResponse {
    pub timeline_id: TimelineId,
    /// Microseconds taken to create the branch (should be near-zero for COW).
    pub elapsed_us: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StartEndpointRequest {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub name: String,
    pub cpu_millis: u32,
    pub memory_mb: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EndpointStatus {
    pub endpoint_id: String,
    pub state: EndpointState,
    pub cpu_millis_used: u64,
    pub active_connections: u32,
    pub p99_latency_ms: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EndpointState {
    Starting,
    Active,
    Suspending,
    Suspended,
    Stopped,
}
