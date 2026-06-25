/// Prometheus client — queries Prometheus for per-endpoint metrics.
/// Used by the autoscaler to make scaling decisions.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use std::collections::{HashMap, VecDeque};

use crate::autoscaler::EndpointMetrics;

// ---------------------------------------------------------------------------
// MetricsSource trait — implemented by both the real Prometheus client and
// the synthetic source used in tests / simulation.
// ---------------------------------------------------------------------------

#[async_trait]
pub trait MetricsSource: Send + Sync + 'static {
    async fn query_endpoint_metrics(&self, endpoint_id: &str) -> Result<EndpointMetrics>;
}

// ---------------------------------------------------------------------------
// Prometheus client (production)
// ---------------------------------------------------------------------------

pub struct PrometheusClient {
    base_url: String,
    http: reqwest::Client,
}

impl PrometheusClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            http: reqwest::Client::new(),
        }
    }

    async fn query_scalar(&self, promql: &str) -> Result<f64> {
        let url = format!("{}/api/v1/query", self.base_url);
        let resp: PrometheusResponse = self.http
            .get(&url)
            .query(&[("query", promql)])
            .send()
            .await?
            .json()
            .await?;

        match resp.status.as_str() {
            "success" => {
                if let Some(result) = resp.data.result.first() {
                    result.value.1.parse::<f64>().map_err(|e| anyhow!("parse error: {e}"))
                } else {
                    Ok(0.0)
                }
            }
            _ => Err(anyhow!("prometheus query failed: {}", resp.status)),
        }
    }
}

#[async_trait]
impl MetricsSource for PrometheusClient {
    async fn query_endpoint_metrics(&self, endpoint_id: &str) -> Result<EndpointMetrics> {
        let cpu_util = self.query_scalar(&format!(
            r#"avg(rate(container_cpu_usage_seconds_total{{name=~"lattice-compute-{endpoint_id}"}}[1m]))"#
        )).await.unwrap_or(0.0);

        let active_connections = self.query_scalar(&format!(
            r#"pg_stat_activity_count{{endpoint="{endpoint_id}",state="active"}}"#
        )).await.unwrap_or(0.0) as u32;

        let p99_latency_ms = self.query_scalar(&format!(
            r#"histogram_quantile(0.99, rate(pg_query_duration_seconds_bucket{{endpoint="{endpoint_id}"}}[1m])) * 1000"#
        )).await.unwrap_or(0.0);

        let query_rate = self.query_scalar(&format!(
            r#"rate(pg_stat_statements_total_calls{{endpoint="{endpoint_id}"}}[1m])"#
        )).await.unwrap_or(0.0);

        Ok(EndpointMetrics {
            active_connections,
            cpu_util: cpu_util.min(1.0),
            p99_latency_ms,
            query_rate_per_sec: query_rate,
        })
    }
}

// ---------------------------------------------------------------------------
// Prometheus API response types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct PrometheusResponse {
    status: String,
    data: PrometheusData,
}

#[derive(Deserialize)]
struct PrometheusData {
    result: Vec<PrometheusResult>,
}

#[derive(Deserialize)]
struct PrometheusResult {
    /// [timestamp, value_string]
    value: (f64, String),
}

// ---------------------------------------------------------------------------
// Synthetic metrics source — injects pre-programmed snapshots per endpoint.
// Used by the autoscaler simulation tests without a live Prometheus.
// ---------------------------------------------------------------------------

pub struct SyntheticMetricsSource {
    /// Per-endpoint queue of scripted metric snapshots.
    per_endpoint: parking_lot::Mutex<HashMap<String, VecDeque<EndpointMetrics>>>,
    /// Returned when the queue for an endpoint is empty.
    default: EndpointMetrics,
}

impl SyntheticMetricsSource {
    pub fn new(default: EndpointMetrics) -> Self {
        Self {
            per_endpoint: parking_lot::Mutex::new(HashMap::new()),
            default,
        }
    }

    /// Push the next metric snapshot for a specific endpoint.
    pub fn push_for(&self, endpoint_id: &str, m: EndpointMetrics) {
        self.per_endpoint
            .lock()
            .entry(endpoint_id.to_string())
            .or_default()
            .push_back(m);
    }

    /// Push the same snapshot N times (useful for sustained conditions).
    pub fn push_n(&self, endpoint_id: &str, m: EndpointMetrics, n: usize) {
        let mut guard = self.per_endpoint.lock();
        let q = guard.entry(endpoint_id.to_string()).or_default();
        for _ in 0..n {
            q.push_back(m.clone());
        }
    }

    fn next_for(&self, endpoint_id: &str) -> EndpointMetrics {
        self.per_endpoint
            .lock()
            .get_mut(endpoint_id)
            .and_then(|q| q.pop_front())
            .unwrap_or_else(|| self.default.clone())
    }
}

#[async_trait]
impl MetricsSource for SyntheticMetricsSource {
    async fn query_endpoint_metrics(&self, endpoint_id: &str) -> Result<EndpointMetrics> {
        Ok(self.next_for(endpoint_id))
    }
}
