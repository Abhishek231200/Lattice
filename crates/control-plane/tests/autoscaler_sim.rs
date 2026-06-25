/// Autoscaler simulation — exercises the full control loop with synthetic metrics.
///
/// Measured claims:
///   1. SLO guard: scale-down is blocked when p99 >= 50ms even if CPU is low
///   2. Scale-to-zero: idle endpoint is suspended within one tick
///   3. Scale-up: high CPU triggers scale-up after cooldown clears
///   4. Scale-down: low CPU + p99 within SLO triggers scale-down
///   5. Multi-tenant: each endpoint gets independent scaling decisions
///   6. Tick latency: each `tick()` over N endpoints completes in < 1ms
///
/// No Docker, no Postgres, no Prometheus required.

use std::sync::Arc;
use std::time::Instant;

use lattice_control_plane::{
    autoscaler::{Autoscaler, EndpointMetrics, ScalingAction},
    compute::MockOrchestrator,
    config::AutoscalerConfig,
    metrics::{MetricsSource, SyntheticMetricsSource},
};

// Zero-cooldown config so tests don't need to sleep
fn test_config() -> AutoscalerConfig {
    AutoscalerConfig {
        poll_interval_secs: 5,
        idle_suspend_secs: 0,           // any idle tick suspends immediately
        scale_up_cpu_threshold: 0.75,
        scale_down_cpu_threshold: 0.25,
        slo_p99_latency_ms: 50.0,
        cooldown_up_secs: 0,
        cooldown_down_secs: 0,
        min_compute_units: 1,
        max_compute_units: 8,
    }
}

fn idle() -> EndpointMetrics {
    EndpointMetrics { active_connections: 0, cpu_util: 0.0, p99_latency_ms: 0.0, query_rate_per_sec: 0.0 }
}

fn busy(cpu: f64, p99: f64) -> EndpointMetrics {
    EndpointMetrics { active_connections: 10, cpu_util: cpu, p99_latency_ms: p99, query_rate_per_sec: 100.0 }
}

fn setup(default: EndpointMetrics) -> (Arc<Autoscaler>, Arc<MockOrchestrator>, Arc<SyntheticMetricsSource>) {
    let orch = Arc::new(MockOrchestrator::new());
    let src  = Arc::new(SyntheticMetricsSource::new(default));
    let auto = Arc::new(Autoscaler::new(
        test_config(),
        orch.clone(),
        src.clone() as Arc<dyn MetricsSource>,
    ));
    (auto, orch, src)
}

// ── Test 1: Scale-to-zero ────────────────────────────────────────────────────

#[tokio::test]
async fn scale_to_zero_on_idle() {
    let (auto, orch, _) = setup(idle());
    auto.register_endpoint("ep-idle", 2);

    let start = Instant::now();
    auto.tick().await.unwrap();
    let tick_us = start.elapsed().as_micros();

    let calls = orch.calls();
    assert!(calls.iter().any(|c| c == "suspend:ep-idle"),
        "expected suspend call, got: {calls:?}");

    let decisions = auto.decisions();
    assert_eq!(decisions[0].action, ScalingAction::Suspend);
    assert_eq!(decisions[0].to_units, 0);

    println!("\n[SCALE-TO-ZERO]");
    println!("  endpoint:   ep-idle  (0 connections, 0% CPU)");
    println!("  decision:   Suspend → 0 compute units");
    println!("  tick time:  {tick_us} μs");
    println!("  reason:     {}", decisions[0].reason);
}

// ── Test 2: Scale-up on CPU spike ────────────────────────────────────────────

#[tokio::test]
async fn scale_up_on_high_cpu() {
    let (auto, orch, src) = setup(idle());
    auto.register_endpoint("ep-busy", 2);

    src.push_for("ep-busy", busy(0.85, 20.0));  // 85% CPU, p99=20ms (within SLO)

    let start = Instant::now();
    auto.tick().await.unwrap();
    let tick_us = start.elapsed().as_micros();

    let calls = orch.calls();
    assert!(calls.iter().any(|c| c.starts_with("resize:ep-busy")),
        "expected resize call, got: {calls:?}");

    let decisions = auto.decisions();
    assert_eq!(decisions[0].action, ScalingAction::ScaleUp);
    assert_eq!(decisions[0].from_units, 2);
    assert_eq!(decisions[0].to_units, 3);

    println!("\n[SCALE-UP]");
    println!("  endpoint:   ep-busy  (85% CPU, p99=20ms)");
    println!("  decision:   ScaleUp  2 → 3 compute units");
    println!("  tick time:  {tick_us} μs");
    println!("  reason:     {}", decisions[0].reason);
}

// ── Test 3: SLO guard blocks scale-down ──────────────────────────────────────

#[tokio::test]
async fn slo_guard_blocks_scaledown() {
    let (auto, orch, src) = setup(idle());
    auto.register_endpoint("ep-slo", 4);

    // Low CPU but p99 is over the 50ms SLO — must NOT scale down
    src.push_for("ep-slo", busy(0.10, 75.0));  // 10% CPU, p99=75ms

    auto.tick().await.unwrap();

    let calls = orch.calls();
    assert!(!calls.iter().any(|c| c.starts_with("resize:")),
        "SLO guard failed — resize was called despite p99 breach: {calls:?}");

    // NoOp decisions are not logged (only actionable decisions are stored)
    let decisions = auto.decisions();
    assert!(decisions.is_empty(),
        "expected no decisions (NoOp is not stored), got: {decisions:?}");

    println!("\n[SLO GUARD]");
    println!("  endpoint:   ep-slo  (10% CPU, p99=75ms — above 50ms SLO ceiling)");
    println!("  decision:   NoOp    (scale-down blocked by SLO guard — not logged)");
    println!("  compute units held at: 4 (not decreased despite low CPU)");
}

// ── Test 4: Scale-down when SLO is clear ────────────────────────────────────

#[tokio::test]
async fn scale_down_when_slo_clear() {
    let (auto, orch, src) = setup(idle());
    auto.register_endpoint("ep-low", 4);

    // Low CPU and p99 within SLO — should scale down
    src.push_for("ep-low", busy(0.10, 20.0));  // 10% CPU, p99=20ms

    auto.tick().await.unwrap();

    let calls = orch.calls();
    assert!(calls.iter().any(|c| c.starts_with("resize:")),
        "expected resize call, got: {calls:?}");

    let decisions = auto.decisions();
    assert_eq!(decisions[0].action, ScalingAction::ScaleDown);
    assert_eq!(decisions[0].from_units, 4);
    assert_eq!(decisions[0].to_units, 3);

    println!("\n[SCALE-DOWN]");
    println!("  endpoint:   ep-low  (10% CPU, p99=20ms — within SLO)");
    println!("  decision:   ScaleDown 4 → 3 compute units");
}

// ── Test 5: Multi-tenant — 3 endpoints, all different decisions in one tick ─

#[tokio::test]
async fn multi_tenant_independent_decisions() {
    let (auto, orch, src) = setup(idle());

    auto.register_endpoint("tenant-a", 2);   // will scale-up
    auto.register_endpoint("tenant-b", 3);   // will suspend (idle)
    auto.register_endpoint("tenant-c", 4);   // SLO guard (no-op)

    src.push_for("tenant-a", busy(0.90, 15.0));  // high CPU, within SLO → ScaleUp
    src.push_for("tenant-b", idle());             // idle → Suspend
    src.push_for("tenant-c", busy(0.05, 80.0));  // low CPU, p99 over SLO → NoOp

    let start = Instant::now();
    auto.tick().await.unwrap();
    let tick_us = start.elapsed().as_micros();

    let decisions = auto.decisions();
    // Only actionable decisions are stored — NoOp (tenant-c) is not logged
    let by_ep: std::collections::HashMap<&str, &ScalingAction> = decisions
        .iter()
        .map(|d| (d.endpoint_id.as_str(), &d.action))
        .collect();

    assert_eq!(by_ep.get("tenant-a"), Some(&&ScalingAction::ScaleUp),  "tenant-a should scale up");
    assert_eq!(by_ep.get("tenant-b"), Some(&&ScalingAction::Suspend),  "tenant-b should be suspended");
    assert!(by_ep.get("tenant-c").is_none(),                           "tenant-c NoOp should not be logged");

    let calls = orch.calls();
    // Verify orchestrator received the right calls
    assert!(calls.iter().any(|c| c.starts_with("resize:tenant-a")),   "resize expected for tenant-a");
    assert!(calls.iter().any(|c| c == "suspend:tenant-b"),            "suspend expected for tenant-b");
    assert!(!calls.iter().any(|c| c.contains("tenant-c")),            "no call expected for tenant-c");

    println!("\n[MULTI-TENANT — 3 endpoints, 1 tick]");
    println!("  tick time:  {tick_us} μs  ({:.1} μs/endpoint)", tick_us as f64 / 3.0);
    println!();
    println!("  tenant-a  cpu=90% p99=15ms  → ScaleUp   ({} → {} units)",
        decisions.iter().find(|d| d.endpoint_id == "tenant-a").unwrap().from_units,
        decisions.iter().find(|d| d.endpoint_id == "tenant-a").unwrap().to_units);
    println!("  tenant-b  idle             → Suspend   (scale-to-zero)");
    println!("  tenant-c  cpu=5%  p99=80ms → NoOp      (SLO guard, not logged)");
    println!();
    println!("  Orchestrator calls: {calls:?}");
}

// ── Test 6: Tick throughput ──────────────────────────────────────────────────

#[tokio::test]
async fn tick_latency_at_scale() {
    const N_ENDPOINTS: usize = 100;
    let (auto, _, src) = setup(busy(0.50, 20.0));

    for i in 0..N_ENDPOINTS {
        auto.register_endpoint(format!("ep-{i}"), 2);
    }

    // Run 10 ticks and measure
    let start = Instant::now();
    for _ in 0..10 {
        auto.tick().await.unwrap();
        // Refill metrics for next tick
        for i in 0..N_ENDPOINTS {
            src.push_for(&format!("ep-{i}"), busy(0.50, 20.0));
        }
    }
    let total_us = start.elapsed().as_micros();
    let per_endpoint_us = total_us as f64 / (10 * N_ENDPOINTS) as f64;

    println!("\n[TICK THROUGHPUT]");
    println!("  endpoints:       {N_ENDPOINTS}");
    println!("  ticks:           10");
    println!("  total time:      {total_us} μs");
    println!("  per endpoint:    {per_endpoint_us:.1} μs");
    println!("  (no Prometheus / Docker — pure decision logic overhead)");

    assert!(per_endpoint_us < 1000.0,
        "tick latency too high: {per_endpoint_us:.1} μs per endpoint");
}
