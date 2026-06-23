use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlPlaneConfig {
    pub listen_addr: String,
    pub database_url: String,
    pub pageserver_url: String,
    pub safekeeper_url: String,
    pub autoscaler: AutoscalerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoscalerConfig {
    /// How often the autoscaler loop runs (seconds).
    pub poll_interval_secs: u64,
    /// Idle time before suspending an endpoint (seconds).
    pub idle_suspend_secs: u64,
    /// CPU utilization threshold (0.0–1.0) to trigger scale-up.
    pub scale_up_cpu_threshold: f64,
    /// CPU utilization threshold to trigger scale-down.
    pub scale_down_cpu_threshold: f64,
    /// p99 latency ceiling in milliseconds.  Autoscaler never scales down above this.
    pub slo_p99_latency_ms: f64,
    /// Minimum seconds to wait between scale-up and scale-down (hysteresis).
    pub cooldown_up_secs: u64,
    pub cooldown_down_secs: u64,
    /// Min / max compute units per endpoint.
    pub min_compute_units: u32,
    pub max_compute_units: u32,
}

impl Default for ControlPlaneConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:5002".to_string(),
            database_url: "postgres://lattice:lattice@localhost:5432/lattice".to_string(),
            pageserver_url: "http://localhost:5000".to_string(),
            safekeeper_url: "http://localhost:5001".to_string(),
            autoscaler: AutoscalerConfig {
                poll_interval_secs: 5,
                idle_suspend_secs: 30,
                scale_up_cpu_threshold: 0.75,
                scale_down_cpu_threshold: 0.25,
                slo_p99_latency_ms: 50.0,
                cooldown_up_secs: 30,
                cooldown_down_secs: 120,
                min_compute_units: 1,
                max_compute_units: 16,
            },
        }
    }
}
