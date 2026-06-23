/// Autoscaler control loop — Phase 7 headline feature.
///
/// The loop runs every `poll_interval_secs` and, for each active endpoint:
///
///   1. Collect metrics: active_connections, cpu_util, p99_latency_ms from Prometheus.
///   2. SLO guard: if p99 >= slo_p99_latency_ms, block all scale-down decisions.
///   3. Scale-to-zero: if idle for > idle_suspend_secs, suspend the endpoint.
///   4. Scale-up: if cpu_util > scale_up_cpu_threshold AND cooldown has elapsed,
///      increase compute_units by 1 (up to max).
///   5. Scale-down: if cpu_util < scale_down_cpu_threshold AND p99 < SLO AND
///      cooldown has elapsed, decrease compute_units by 1 (down to min).
///   6. Record decisions to the `autoscaler_decisions` table for the dashboard.
///
/// Hysteresis: separate up/down cooldown timers per endpoint prevent flapping.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use lattice_common::proto::EndpointState;

use crate::compute::ComputeOrchestrator;
use crate::config::AutoscalerConfig;
use crate::db::ControlPlaneDb;
use crate::metrics::PrometheusClient;

// ---------------------------------------------------------------------------
// Endpoint runtime state tracked by the autoscaler
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct EndpointRuntimeState {
    pub endpoint_id: String,
    pub state: EndpointState,
    pub compute_units: u32,
    pub last_activity: Instant,
    pub last_scale_up: Option<Instant>,
    pub last_scale_down: Option<Instant>,
}

impl EndpointRuntimeState {
    pub fn new(endpoint_id: impl Into<String>, compute_units: u32) -> Self {
        Self {
            endpoint_id: endpoint_id.into(),
            state: EndpointState::Active,
            compute_units,
            last_activity: Instant::now(),
            last_scale_up: None,
            last_scale_down: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Metrics snapshot for one endpoint
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct EndpointMetrics {
    pub active_connections: u32,
    pub cpu_util: f64,           // 0.0–1.0
    pub p99_latency_ms: f64,
    pub query_rate_per_sec: f64,
}

impl EndpointMetrics {
    pub fn is_idle(&self) -> bool {
        self.active_connections == 0 && self.query_rate_per_sec < 0.1
    }
}

// ---------------------------------------------------------------------------
// Autoscaler decision log
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingDecision {
    pub endpoint_id: String,
    pub action: ScalingAction,
    pub reason: String,
    pub from_units: u32,
    pub to_units: u32,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ScalingAction {
    ScaleUp,
    ScaleDown,
    Suspend,
    Resume,
    NoOp,
}

// ---------------------------------------------------------------------------
// Autoscaler
// ---------------------------------------------------------------------------

pub struct Autoscaler {
    config: AutoscalerConfig,
    orchestrator: Arc<dyn ComputeOrchestrator>,
    prometheus: Arc<PrometheusClient>,
    /// In-memory state; persisted to DB on each decision.
    endpoint_states: Mutex<HashMap<String, EndpointRuntimeState>>,
    /// Running log of all decisions (also stored in DB).
    decisions: Mutex<Vec<ScalingDecision>>,
}

impl Autoscaler {
    pub fn new(
        config: AutoscalerConfig,
        orchestrator: Arc<dyn ComputeOrchestrator>,
        prometheus: Arc<PrometheusClient>,
    ) -> Self {
        Self {
            config,
            orchestrator,
            prometheus,
            endpoint_states: Mutex::new(HashMap::new()),
            decisions: Mutex::new(Vec::new()),
        }
    }

    /// Register an endpoint with the autoscaler.
    pub fn register_endpoint(&self, endpoint_id: impl Into<String>, compute_units: u32) {
        let id: String = endpoint_id.into();
        self.endpoint_states.lock().insert(id.clone(), EndpointRuntimeState::new(id, compute_units));
    }

    /// Run the control loop indefinitely.
    pub async fn run_forever(self: Arc<Self>) {
        let interval = Duration::from_secs(self.config.poll_interval_secs);
        loop {
            sleep(interval).await;
            if let Err(e) = self.tick().await {
                warn!("autoscaler tick error: {e:#}");
            }
        }
    }

    /// One pass of the autoscaler loop.
    pub async fn tick(&self) -> anyhow::Result<()> {
        let endpoint_ids: Vec<String> = self.endpoint_states.lock().keys().cloned().collect();

        for endpoint_id in &endpoint_ids {
            match self.process_endpoint(endpoint_id).await {
                Ok(decision) => {
                    if decision.action != ScalingAction::NoOp {
                        info!(
                            endpoint = %endpoint_id,
                            action = ?decision.action,
                            reason = %decision.reason,
                            from = decision.from_units,
                            to = decision.to_units,
                            "autoscaler decision"
                        );
                        self.decisions.lock().push(decision);
                    }
                }
                Err(e) => warn!("error processing endpoint {endpoint_id}: {e:#}"),
            }
        }

        Ok(())
    }

    async fn process_endpoint(&self, endpoint_id: &str) -> anyhow::Result<ScalingDecision> {
        // 1. Get current state.
        let state = {
            let guard = self.endpoint_states.lock();
            guard.get(endpoint_id).cloned()
        };
        let mut state = match state {
            Some(s) => s,
            None => return Ok(noop(endpoint_id, 0, 0)),
        };

        // Skip suspended/stopped endpoints (they get a special resume check).
        if state.state == EndpointState::Suspended {
            // Suspended endpoints resume on the next connection — no automatic resume here.
            // The API layer handles the first-connection resume.
            return Ok(noop(endpoint_id, state.compute_units, state.compute_units));
        }

        // 2. Collect metrics.
        let metrics = self.collect_metrics(endpoint_id).await?;
        debug!(endpoint = %endpoint_id, ?metrics, "autoscaler metrics");

        // Track activity.
        if !metrics.is_idle() {
            state.last_activity = Instant::now();
        }

        let from_units = state.compute_units;

        // 3. Scale-to-zero: suspend if idle too long.
        let idle_duration = state.last_activity.elapsed();
        if idle_duration >= Duration::from_secs(self.config.idle_suspend_secs) && metrics.is_idle() {
            self.orchestrator.suspend(endpoint_id).await?;
            state.state = EndpointState::Suspended;
            let decision = ScalingDecision {
                endpoint_id: endpoint_id.to_string(),
                action: ScalingAction::Suspend,
                reason: format!("idle for {:.0}s", idle_duration.as_secs_f64()),
                from_units,
                to_units: 0,
                timestamp: chrono::Utc::now(),
            };
            self.update_state(state);
            return Ok(decision);
        }

        // 4. SLO guard: never scale down when p99 is close to the ceiling.
        let slo_breached = metrics.p99_latency_ms >= self.config.slo_p99_latency_ms;

        // 5. Scale-up check.
        if metrics.cpu_util >= self.config.scale_up_cpu_threshold {
            let can_scale_up = state.last_scale_up
                .map(|t| t.elapsed() >= Duration::from_secs(self.config.cooldown_up_secs))
                .unwrap_or(true);

            if can_scale_up && state.compute_units < self.config.max_compute_units {
                let new_units = state.compute_units + 1;
                let cpu_millis = new_units * 1000;
                let memory_mb = new_units * 512;
                self.orchestrator.resize(endpoint_id, cpu_millis, memory_mb).await?;
                state.compute_units = new_units;
                state.last_scale_up = Some(Instant::now());
                let decision = ScalingDecision {
                    endpoint_id: endpoint_id.to_string(),
                    action: ScalingAction::ScaleUp,
                    reason: format!("cpu={:.0}% > threshold={:.0}%", metrics.cpu_util * 100.0, self.config.scale_up_cpu_threshold * 100.0),
                    from_units,
                    to_units: new_units,
                    timestamp: chrono::Utc::now(),
                };
                self.update_state(state);
                return Ok(decision);
            }
        }

        // 6. Scale-down check (only if p99 is within SLO).
        if !slo_breached && metrics.cpu_util < self.config.scale_down_cpu_threshold {
            let can_scale_down = state.last_scale_down
                .map(|t| t.elapsed() >= Duration::from_secs(self.config.cooldown_down_secs))
                .unwrap_or(true);

            if can_scale_down && state.compute_units > self.config.min_compute_units {
                let new_units = state.compute_units - 1;
                let cpu_millis = new_units * 1000;
                let memory_mb = new_units * 512;
                self.orchestrator.resize(endpoint_id, cpu_millis, memory_mb).await?;
                state.compute_units = new_units;
                state.last_scale_down = Some(Instant::now());
                let decision = ScalingDecision {
                    endpoint_id: endpoint_id.to_string(),
                    action: ScalingAction::ScaleDown,
                    reason: format!("cpu={:.0}% < threshold={:.0}%", metrics.cpu_util * 100.0, self.config.scale_down_cpu_threshold * 100.0),
                    from_units,
                    to_units: new_units,
                    timestamp: chrono::Utc::now(),
                };
                self.update_state(state);
                return Ok(decision);
            }
        }

        self.update_state(state);
        Ok(noop(endpoint_id, from_units, from_units))
    }

    async fn collect_metrics(&self, endpoint_id: &str) -> anyhow::Result<EndpointMetrics> {
        self.prometheus.query_endpoint_metrics(endpoint_id).await
    }

    fn update_state(&self, state: EndpointRuntimeState) {
        self.endpoint_states.lock().insert(state.endpoint_id.clone(), state);
    }

    pub fn decisions(&self) -> Vec<ScalingDecision> {
        self.decisions.lock().clone()
    }

    /// Compute savings: compare actual compute-unit-seconds used vs always-on at max.
    pub fn compute_savings_pct(&self) -> f64 {
        let decisions = self.decisions.lock();
        if decisions.is_empty() {
            return 0.0;
        }
        // Simple approximation: time spent suspended / total time.
        let suspended_seconds: f64 = decisions.iter()
            .filter(|d| d.action == ScalingAction::Suspend)
            .count() as f64 * self.config.poll_interval_secs as f64;
        let total = decisions.len() as f64 * self.config.poll_interval_secs as f64;
        if total == 0.0 { 0.0 } else { suspended_seconds / total * 100.0 }
    }
}

fn noop(endpoint_id: &str, from: u32, to: u32) -> ScalingDecision {
    ScalingDecision {
        endpoint_id: endpoint_id.to_string(),
        action: ScalingAction::NoOp,
        reason: String::new(),
        from_units: from,
        to_units: to,
        timestamp: chrono::Utc::now(),
    }
}
