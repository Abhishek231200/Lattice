/// High-level WAL receiver task — wraps the replication protocol and drives the WalStore.
/// Spawned once per (tenant, timeline) by the safekeeper on startup or when a new
/// timeline is registered.

use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use anyhow::Result;

use lattice_common::{Lsn, TenantId, TimelineId};

use crate::replication::WalReceiver as RawWalReceiver;
use crate::wal_store::WalStore;

pub struct WalReceiverTask {
    tenant_id: TenantId,
    timeline_id: TimelineId,
    pg_host: String,
    pg_port: u16,
    pg_user: String,
    start_lsn: Lsn,
    store: WalStore,
    pageserver_url: Option<String>,
}

impl WalReceiverTask {
    pub fn new(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        pg_host: impl Into<String>,
        pg_port: u16,
        pg_user: impl Into<String>,
        start_lsn: Lsn,
        store: WalStore,
        pageserver_url: Option<String>,
    ) -> Self {
        Self {
            tenant_id,
            timeline_id,
            pg_host: pg_host.into(),
            pg_port,
            pg_user: pg_user.into(),
            start_lsn,
            store,
            pageserver_url,
        }
    }

    /// Run with automatic reconnect on error.
    pub async fn run_with_reconnect(mut self) {
        let mut backoff = Duration::from_secs(1);
        loop {
            info!(
                tenant = %self.tenant_id,
                timeline = %self.timeline_id,
                host = %self.pg_host,
                port = self.pg_port,
                "starting WAL receiver"
            );

            // Resume from where we left off.
            let from_lsn = self.store.last_lsn().max(self.start_lsn);

            let mut receiver = RawWalReceiver::new(
                self.tenant_id,
                self.timeline_id,
                self.store.clone(),
                self.pageserver_url.clone(),
            );

            match receiver.run(&self.pg_host, self.pg_port, &self.pg_user, from_lsn).await {
                Ok(_) => {
                    info!("WAL receiver exited cleanly");
                    return;
                }
                Err(e) => {
                    warn!("WAL receiver error: {e:#}; reconnecting in {}s", backoff.as_secs());
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                }
            }
        }
    }
}
