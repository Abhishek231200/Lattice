use std::str::FromStr;
use tracing::info;
use tracing_subscriber::EnvFilter;

use lattice_common::{Lsn, TenantId, TimelineId};
use lattice_safekeeper::{
    config::SafekeeperConfig,
    wal_receiver::WalReceiverTask,
    wal_sender::{router, SafekeeperState},
    wal_store::WalStore,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    let config = SafekeeperConfig::default();
    let state = SafekeeperState::new();

    // Optional: connect to a Postgres instance and stream its WAL.
    // Activated by setting POSTGRES_HOST (and optionally POSTGRES_PORT,
    // POSTGRES_USER, WAL_TENANT_ID, WAL_TIMELINE_ID).
    if let Ok(pg_host) = std::env::var("POSTGRES_HOST") {
        let pg_port: u16 = std::env::var("POSTGRES_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5432);
        let pg_user = std::env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".into());

        let tenant_id = std::env::var("WAL_TENANT_ID")
            .ok()
            .and_then(|s| TenantId::from_str(&s).ok())
            .unwrap_or_else(TenantId::new);
        let timeline_id = std::env::var("WAL_TIMELINE_ID")
            .ok()
            .and_then(|s| TimelineId::from_str(&s).ok())
            .unwrap_or_else(TimelineId::new);

        let store = WalStore::open(&config.data_dir, tenant_id, timeline_id)?;

        state.register(tenant_id, timeline_id, store.clone());

        info!(
            %pg_host, pg_port, %pg_user,
            %tenant_id, %timeline_id,
            "WAL receiver enabled — will stream from Postgres"
        );

        let pageserver_url = config.pageserver_url.clone();
        let task = WalReceiverTask::new(
            tenant_id,
            timeline_id,
            pg_host,
            pg_port,
            pg_user,
            Lsn::INVALID,
            store,
            pageserver_url,
        );
        tokio::spawn(task.run_with_reconnect());
    }

    let addr = config.listen_addr.parse::<std::net::SocketAddr>()?;
    info!("safekeeper listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(state)).await?;

    Ok(())
}
