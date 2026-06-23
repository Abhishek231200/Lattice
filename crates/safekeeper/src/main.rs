use tracing::info;
use tracing_subscriber::EnvFilter;

use lattice_safekeeper::{
    config::SafekeeperConfig,
    wal_sender::{router, SafekeeperState},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    let config = SafekeeperConfig::default();
    let state = SafekeeperState::new();

    let addr = config.listen_addr.parse::<std::net::SocketAddr>()?;
    info!("safekeeper listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(state)).await?;

    Ok(())
}
