/// Prometheus client — queries Prometheus for per-endpoint metrics.
/// Used by the autoscaler to make scaling decisions.

use anyhow::{anyhow, Result};
use serde::Deserialize;

use crate::autoscaler::EndpointMetrics;

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

    /// Query Prometheus for all autoscaler-relevant metrics for one endpoint.
    pub async fn query_endpoint_metrics(&self, endpoint_id: &str) -> Result<EndpointMetrics> {
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
// Synthetic metrics for testing without Prometheus
// ---------------------------------------------------------------------------

pub struct SyntheticMetricsSource {
    /// Returns pre-programmed metric snapshots in sequence.
    snapshots: parking_lot::Mutex<std::collections::VecDeque<EndpointMetrics>>,
    default: EndpointMetrics,
}

impl SyntheticMetricsSource {
    pub fn new(default: EndpointMetrics) -> Self {
        Self {
            snapshots: parking_lot::Mutex::new(std::collections::VecDeque::new()),
            default,
        }
    }

    pub fn push(&self, m: EndpointMetrics) {
        self.snapshots.lock().push_back(m);
    }

    pub fn next(&self) -> EndpointMetrics {
        self.snapshots.lock().pop_front().unwrap_or_else(|| self.default.clone())
    }
}
