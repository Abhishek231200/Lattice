use prometheus::{
    register_counter_vec, register_histogram_vec, register_gauge_vec,
    CounterVec, HistogramVec, GaugeVec, Encoder, TextEncoder,
};
use std::sync::OnceLock;

static GET_PAGE_LATENCY: OnceLock<HistogramVec> = OnceLock::new();
static GET_PAGE_REQUESTS: OnceLock<CounterVec> = OnceLock::new();
static LAYER_COUNT: OnceLock<GaugeVec> = OnceLock::new();
static BYTES_STORED: OnceLock<GaugeVec> = OnceLock::new();

pub fn init() {
    GET_PAGE_LATENCY.get_or_init(|| {
        register_histogram_vec!(
            "lattice_get_page_latency_seconds",
            "Latency of get_page_at_lsn calls",
            &["timeline", "parent_recurse"],
            vec![0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
        ).unwrap()
    });

    GET_PAGE_REQUESTS.get_or_init(|| {
        register_counter_vec!(
            "lattice_get_page_requests_total",
            "Total number of get_page_at_lsn calls",
            &["timeline", "status"],
        ).unwrap()
    });

    LAYER_COUNT.get_or_init(|| {
        register_gauge_vec!(
            "lattice_layer_count",
            "Number of image/delta layers per timeline",
            &["timeline", "layer_type"],
        ).unwrap()
    });

    BYTES_STORED.get_or_init(|| {
        register_gauge_vec!(
            "lattice_bytes_stored",
            "Approximate bytes stored per tenant",
            &["tenant"],
        ).unwrap()
    });
}

pub fn record_get_page(timeline: &str, parent_recurse: bool, latency_secs: f64, ok: bool) {
    if let Some(h) = GET_PAGE_LATENCY.get() {
        h.with_label_values(&[timeline, if parent_recurse { "true" } else { "false" }])
            .observe(latency_secs);
    }
    if let Some(c) = GET_PAGE_REQUESTS.get() {
        c.with_label_values(&[timeline, if ok { "ok" } else { "error" }]).inc();
    }
}

pub fn set_layer_counts(timeline: &str, image_count: usize, delta_count: usize) {
    if let Some(g) = LAYER_COUNT.get() {
        g.with_label_values(&[timeline, "image"]).set(image_count as f64);
        g.with_label_values(&[timeline, "delta"]).set(delta_count as f64);
    }
}

pub fn metrics_text() -> String {
    let encoder = TextEncoder::new();
    let families = prometheus::gather();
    let mut buf = Vec::new();
    encoder.encode(&families, &mut buf).unwrap_or_default();
    String::from_utf8(buf).unwrap_or_default()
}
