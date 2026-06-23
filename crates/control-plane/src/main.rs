use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

use lattice_control_plane::{
    api::{router, ControlPlaneState},
    autoscaler::Autoscaler,
    compute::DockerOrchestrator,
    config::ControlPlaneConfig,
    db::ControlPlaneDb,
    metrics::PrometheusClient,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    let config = ControlPlaneConfig::default();

    let db = Arc::new(ControlPlaneDb::connect(&config.database_url).await?);
    db.migrate().await?;
    info!("database migrations complete");

    let orchestrator = Arc::new(DockerOrchestrator::new(
        "lattice-network",
        "postgres:16-alpine",
    ));

    let prometheus = Arc::new(PrometheusClient::new("http://localhost:9090"));

    let autoscaler = Arc::new(Autoscaler::new(
        config.autoscaler.clone(),
        orchestrator.clone(),
        prometheus.clone(),
    ));

    // Spawn the autoscaler loop.
    {
        let autoscaler = autoscaler.clone();
        tokio::spawn(async move {
            autoscaler.run_forever().await;
        });
    }

    let state = ControlPlaneState {
        db,
        autoscaler,
        orchestrator,
        prometheus,
        pageserver_url: config.pageserver_url.clone(),
    };

    let addr = config.listen_addr.parse::<std::net::SocketAddr>()?;
    info!("control-plane listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(state)).await?;

    Ok(())
}
