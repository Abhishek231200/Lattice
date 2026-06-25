/// Compute orchestrator — manages Docker containers (dev) or k8s pods (staging/prod).
/// Hidden behind a trait so the autoscaler doesn't know the orchestration backend.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use tracing::{info, warn};
use anyhow::Result;

use lattice_common::{TenantId, TimelineId};
use lattice_common::proto::EndpointState;

// ---------------------------------------------------------------------------
// ComputeOrchestrator trait
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeSpec {
    pub endpoint_id: String,
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    /// CPU in millicores.
    pub cpu_millis: u32,
    /// Memory in MiB.
    pub memory_mb: u32,
    /// Pageserver endpoint to serve pages.
    pub pageserver_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeInfo {
    pub endpoint_id: String,
    pub state: EndpointState,
    pub host: String,
    pub port: u16,
    pub cpu_millis: u32,
    pub memory_mb: u32,
}

#[async_trait]
pub trait ComputeOrchestrator: Send + Sync + 'static {
    async fn start(&self, spec: ComputeSpec) -> Result<ComputeInfo>;
    async fn stop(&self, endpoint_id: &str) -> Result<()>;
    async fn suspend(&self, endpoint_id: &str) -> Result<()>;
    async fn resume(&self, endpoint_id: &str) -> Result<ComputeInfo>;
    async fn resize(&self, endpoint_id: &str, cpu_millis: u32, memory_mb: u32) -> Result<()>;
    async fn info(&self, endpoint_id: &str) -> Result<Option<ComputeInfo>>;
}

// ---------------------------------------------------------------------------
// Docker orchestrator (local dev)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Mock orchestrator — records calls without running Docker. Used in tests.
// ---------------------------------------------------------------------------

/// Records every orchestration call for assertion in tests and simulations.
pub struct MockOrchestrator {
    pub calls: parking_lot::Mutex<Vec<String>>,
}

impl MockOrchestrator {
    pub fn new() -> Self {
        Self { calls: parking_lot::Mutex::new(Vec::new()) }
    }

    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().clone()
    }
}

#[async_trait]
impl ComputeOrchestrator for MockOrchestrator {
    async fn start(&self, spec: ComputeSpec) -> Result<ComputeInfo> {
        self.calls.lock().push(format!("start:{}", spec.endpoint_id));
        Ok(ComputeInfo {
            endpoint_id: spec.endpoint_id.clone(),
            state: EndpointState::Active,
            host: "mock".into(),
            port: 5432,
            cpu_millis: spec.cpu_millis,
            memory_mb: spec.memory_mb,
        })
    }

    async fn stop(&self, endpoint_id: &str) -> Result<()> {
        self.calls.lock().push(format!("stop:{endpoint_id}"));
        Ok(())
    }

    async fn suspend(&self, endpoint_id: &str) -> Result<()> {
        self.calls.lock().push(format!("suspend:{endpoint_id}"));
        Ok(())
    }

    async fn resume(&self, endpoint_id: &str) -> Result<ComputeInfo> {
        self.calls.lock().push(format!("resume:{endpoint_id}"));
        Ok(ComputeInfo {
            endpoint_id: endpoint_id.to_string(),
            state: EndpointState::Active,
            host: "mock".into(),
            port: 5432,
            cpu_millis: 1000,
            memory_mb: 512,
        })
    }

    async fn resize(&self, endpoint_id: &str, cpu_millis: u32, memory_mb: u32) -> Result<()> {
        self.calls.lock().push(format!("resize:{endpoint_id}:{cpu_millis}m/{memory_mb}MB"));
        Ok(())
    }

    async fn info(&self, endpoint_id: &str) -> Result<Option<ComputeInfo>> {
        Ok(Some(ComputeInfo {
            endpoint_id: endpoint_id.to_string(),
            state: EndpointState::Active,
            host: "mock".into(),
            port: 5432,
            cpu_millis: 1000,
            memory_mb: 512,
        }))
    }
}

// ---------------------------------------------------------------------------

/// Manages Postgres containers via the Docker CLI.  Each endpoint gets its own
/// container.  For demo purposes we use `docker run` via `tokio::process::Command`.
pub struct DockerOrchestrator {
    network: String,
    image: String,
}

impl DockerOrchestrator {
    pub fn new(network: impl Into<String>, image: impl Into<String>) -> Self {
        Self { network: network.into(), image: image.into() }
    }

    fn container_name(endpoint_id: &str) -> String {
        format!("lattice-compute-{endpoint_id}")
    }
}

#[async_trait]
impl ComputeOrchestrator for DockerOrchestrator {
    async fn start(&self, spec: ComputeSpec) -> Result<ComputeInfo> {
        let name = Self::container_name(&spec.endpoint_id);
        info!("starting compute container {name}");

        // CPU in docker = nanocpus (1 CPU = 1e9 nanocpus)
        let nanocpus = spec.cpu_millis as u64 * 1_000_000;
        let memory = format!("{}m", spec.memory_mb);

        let output = tokio::process::Command::new("docker")
            .args([
                "run", "-d",
                "--name", &name,
                "--network", &self.network,
                "--cpus", &format!("{:.3}", spec.cpu_millis as f64 / 1000.0),
                "--memory", &memory,
                "-e", &format!("PAGESERVER_URL={}", spec.pageserver_url),
                "-e", &format!("TENANT_ID={}", spec.tenant_id),
                "-e", &format!("TIMELINE_ID={}", spec.timeline_id),
                "-e", "POSTGRES_PASSWORD=lattice",
                "-e", "POSTGRES_USER=lattice",
                "-e", "POSTGRES_DB=lattice",
                &self.image,
            ])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("docker run failed: {stderr}"));
        }

        Ok(ComputeInfo {
            endpoint_id: spec.endpoint_id,
            state: EndpointState::Starting,
            host: name,
            port: 5432,
            cpu_millis: spec.cpu_millis,
            memory_mb: spec.memory_mb,
        })
    }

    async fn stop(&self, endpoint_id: &str) -> Result<()> {
        let name = Self::container_name(endpoint_id);
        info!("stopping compute container {name}");
        tokio::process::Command::new("docker")
            .args(["rm", "-f", &name])
            .output()
            .await?;
        Ok(())
    }

    async fn suspend(&self, endpoint_id: &str) -> Result<()> {
        let name = Self::container_name(endpoint_id);
        info!("suspending compute container {name}");
        tokio::process::Command::new("docker")
            .args(["pause", &name])
            .output()
            .await?;
        Ok(())
    }

    async fn resume(&self, endpoint_id: &str) -> Result<ComputeInfo> {
        let name = Self::container_name(endpoint_id);
        info!("resuming compute container {name}");
        let output = tokio::process::Command::new("docker")
            .args(["unpause", &name])
            .output()
            .await?;
        if !output.status.success() {
            return Err(anyhow::anyhow!("docker unpause failed"));
        }
        Ok(ComputeInfo {
            endpoint_id: endpoint_id.to_string(),
            state: EndpointState::Active,
            host: name,
            port: 5432,
            cpu_millis: 1000,
            memory_mb: 512,
        })
    }

    async fn resize(&self, endpoint_id: &str, cpu_millis: u32, memory_mb: u32) -> Result<()> {
        let name = Self::container_name(endpoint_id);
        info!("resizing {name}: cpu={cpu_millis}m mem={memory_mb}MB");
        tokio::process::Command::new("docker")
            .args([
                "update",
                "--cpus", &format!("{:.3}", cpu_millis as f64 / 1000.0),
                "--memory", &format!("{memory_mb}m"),
                &name,
            ])
            .output()
            .await?;
        Ok(())
    }

    async fn info(&self, endpoint_id: &str) -> Result<Option<ComputeInfo>> {
        let name = Self::container_name(endpoint_id);
        let output = tokio::process::Command::new("docker")
            .args(["inspect", "--format", "{{.State.Status}}", &name])
            .output()
            .await?;
        if !output.status.success() {
            return Ok(None);
        }
        let status = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let state = match status.as_str() {
            "running" => EndpointState::Active,
            "paused" => EndpointState::Suspended,
            "exited" => EndpointState::Stopped,
            _ => EndpointState::Stopped,
        };
        Ok(Some(ComputeInfo {
            endpoint_id: endpoint_id.to_string(),
            state,
            host: name,
            port: 5432,
            cpu_millis: 1000,
            memory_mb: 512,
        }))
    }
}
