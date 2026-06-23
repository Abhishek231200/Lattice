/// Postgres streaming replication protocol — just enough to receive WAL.
///
/// The protocol flow after the initial handshake:
///   1. Client sends `START_REPLICATION SLOT slot LSN` command.
///   2. Server responds with CopyBothResponse.
///   3. Server streams XLogData messages (type 'w'):
///        byte  0: 'w'
///        bytes 1-8: WAL start LSN (big-endian)
///        bytes 9-16: WAL end LSN
///        bytes 17-24: system clock (microseconds since 2000-01-01)
///        bytes 25+: WAL record data
///   4. Client sends StandbyStatusUpdate ('r') keep-alives.
///
/// We decode the XLogData frames and hand the raw WAL bytes to the WalStore.
///
/// For the Postgres wire protocol prefix (startup / auth) we implement just
/// the minimal md5/trust auth path since we control the Postgres config.

use std::io;
use bytes::{Bytes, BytesMut, Buf, BufMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info, warn, error};
use anyhow::{anyhow, Result};

use lattice_common::{Lsn, TenantId, TimelineId};

use crate::wal_store::WalStore;

// ---------------------------------------------------------------------------
// Postgres message types
// ---------------------------------------------------------------------------

const MSG_COPY_DATA: u8 = b'd';
const MSG_COPY_DONE: u8 = b'c';
const MSG_ERROR_RESPONSE: u8 = b'E';

// Replication sub-message types (inside CopyData payload)
const XLOG_DATA: u8 = b'w';
const KEEPALIVE: u8 = b'k';
const STANDBY_STATUS: u8 = b'r';

/// Size of the XLogData header before the WAL bytes.
const XLOG_DATA_HEADER_SIZE: usize = 25; // 1 (type) + 8 (start) + 8 (end) + 8 (clock)

// ---------------------------------------------------------------------------
// WalReceiver
// ---------------------------------------------------------------------------

pub struct WalReceiver {
    tenant_id: TenantId,
    timeline_id: TimelineId,
    store: WalStore,
    /// LSN of the last record we flushed to the store.
    flushed_lsn: Lsn,
    /// Pageserver endpoint to notify after flushing.
    pageserver_url: Option<String>,
    http_client: reqwest::Client,
}

impl WalReceiver {
    pub fn new(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        store: WalStore,
        pageserver_url: Option<String>,
    ) -> Self {
        Self {
            tenant_id,
            timeline_id,
            store,
            flushed_lsn: Lsn::INVALID,
            pageserver_url,
            http_client: reqwest::Client::new(),
        }
    }

    /// Connect to Postgres and stream WAL starting from `start_lsn`.
    /// This implements the physical replication protocol (not logical).
    pub async fn run(
        &mut self,
        pg_host: &str,
        pg_port: u16,
        pg_user: &str,
        start_lsn: Lsn,
    ) -> Result<()> {
        let addr = format!("{pg_host}:{pg_port}");
        info!(%addr, start_lsn = %start_lsn, "connecting to Postgres for WAL streaming");

        let mut stream = TcpStream::connect(&addr).await?;
        stream.set_nodelay(true)?;

        // --- Startup ---
        self.send_startup(&mut stream, pg_user).await?;
        self.read_auth_ok(&mut stream).await?;

        // --- Identify system ---
        self.send_query(&mut stream, "IDENTIFY_SYSTEM").await?;
        let sys_info = self.read_single_row(&mut stream).await?;
        info!("system: {:?}", sys_info);

        // --- Start replication ---
        let cmd = format!(
            "START_REPLICATION SLOT lattice_repl PHYSICAL {}",
            self.lsn_to_pg_str(start_lsn)
        );
        self.send_query(&mut stream, &cmd).await?;
        self.expect_copy_both(&mut stream).await?;

        info!("WAL streaming started from {start_lsn}");

        // --- Main WAL receive loop ---
        loop {
            let msg = self.read_copy_data(&mut stream).await?;
            if msg.is_empty() {
                continue;
            }
            match msg[0] {
                XLOG_DATA => {
                    if msg.len() < XLOG_DATA_HEADER_SIZE {
                        warn!("short XLogData frame ({} bytes)", msg.len());
                        continue;
                    }
                    let start_lsn_raw = u64::from_be_bytes(msg[1..9].try_into().unwrap());
                    let end_lsn_raw = u64::from_be_bytes(msg[9..17].try_into().unwrap());
                    let wal_data = Bytes::copy_from_slice(&msg[XLOG_DATA_HEADER_SIZE..]);

                    let lsn = Lsn(start_lsn_raw);
                    debug!(lsn = %lsn, bytes = wal_data.len(), "received XLogData");

                    self.store.append(lsn, wal_data).await?;
                    self.flushed_lsn = Lsn(end_lsn_raw);

                    // Send status update to Postgres every 1000 records (simplified).
                    if self.flushed_lsn.as_u64() % 1000 == 0 {
                        self.send_status_update(&mut stream, self.flushed_lsn).await?;
                    }

                    // Notify pageserver.
                    if let Some(url) = &self.pageserver_url {
                        let _ = self.notify_pageserver(url, lsn).await;
                    }
                }
                KEEPALIVE => {
                    // Server keepalive — respond to prevent timeout.
                    self.send_status_update(&mut stream, self.flushed_lsn).await?;
                }
                other => {
                    debug!("unknown replication message type: 0x{:02x}", other);
                }
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Protocol helpers
    // ---------------------------------------------------------------------------

    async fn send_startup(&self, stream: &mut TcpStream, user: &str) -> Result<()> {
        // Startup message: length (4 bytes) + protocol version (4 bytes) + params
        let mut body = Vec::new();
        body.extend_from_slice(&196608u32.to_be_bytes()); // protocol 3.0
        body.extend_from_slice(b"user\0");
        body.extend_from_slice(user.as_bytes());
        body.push(0);
        body.extend_from_slice(b"replication\0");
        body.extend_from_slice(b"true\0");
        body.push(0); // terminator

        let len = (body.len() + 4) as u32;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(&body).await?;
        Ok(())
    }

    async fn read_auth_ok(&self, stream: &mut TcpStream) -> Result<()> {
        // Expect AuthenticationOk (R + 4-byte len + 4-byte 0) then ReadyForQuery.
        loop {
            let msg_type = self.read_byte(stream).await?;
            let len = self.read_u32(stream).await? as usize;
            let mut payload = vec![0u8; len - 4];
            stream.read_exact(&mut payload).await?;
            match msg_type {
                b'R' => {
                    // Auth response — 0 = OK, 5 = MD5 (we require trust auth)
                    let auth_type = u32::from_be_bytes(payload[..4].try_into().unwrap());
                    if auth_type != 0 {
                        return Err(anyhow!("unsupported auth type {auth_type}; configure pg_hba.conf with 'trust'"));
                    }
                }
                b'Z' => return Ok(()), // ReadyForQuery
                b'E' => return Err(anyhow!("postgres error during startup")),
                b'S' => {} // ParameterStatus — ignore
                b'K' => {} // BackendKeyData — ignore
                _ => {}
            }
        }
    }

    async fn send_query(&self, stream: &mut TcpStream, query: &str) -> Result<()> {
        // Simple query message: 'Q' + length + query + '\0'
        let body = format!("{query}\0");
        let len = (body.len() + 4) as u32;
        stream.write_all(&[b'Q']).await?;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(body.as_bytes()).await?;
        Ok(())
    }

    async fn read_single_row(&self, stream: &mut TcpStream) -> Result<Vec<String>> {
        let mut row = Vec::new();
        loop {
            let msg_type = self.read_byte(stream).await?;
            let len = self.read_u32(stream).await? as usize;
            let mut payload = vec![0u8; len - 4];
            stream.read_exact(&mut payload).await?;
            match msg_type {
                b'T' => {} // RowDescription — ignore
                b'D' => {
                    // DataRow
                    let col_count = u16::from_be_bytes(payload[..2].try_into().unwrap()) as usize;
                    let mut pos = 2;
                    for _ in 0..col_count {
                        let col_len = i32::from_be_bytes(payload[pos..pos+4].try_into().unwrap());
                        pos += 4;
                        if col_len < 0 {
                            row.push("NULL".to_string());
                        } else {
                            let s = String::from_utf8_lossy(&payload[pos..pos + col_len as usize]).into_owned();
                            row.push(s);
                            pos += col_len as usize;
                        }
                    }
                }
                b'C' | b'Z' => return Ok(row), // CommandComplete / ReadyForQuery
                _ => {}
            }
        }
    }

    async fn expect_copy_both(&self, stream: &mut TcpStream) -> Result<()> {
        let msg_type = self.read_byte(stream).await?;
        let len = self.read_u32(stream).await? as usize;
        let mut payload = vec![0u8; len - 4];
        stream.read_exact(&mut payload).await?;
        if msg_type != b'W' {
            return Err(anyhow!("expected CopyBothResponse ('W'), got 0x{:02x}", msg_type));
        }
        Ok(())
    }

    async fn read_copy_data(&self, stream: &mut TcpStream) -> Result<Vec<u8>> {
        let msg_type = self.read_byte(stream).await?;
        let len = self.read_u32(stream).await? as usize;
        if len < 4 {
            return Ok(vec![]);
        }
        let mut payload = vec![0u8; len - 4];
        stream.read_exact(&mut payload).await?;
        match msg_type {
            b'd' => Ok(payload),
            b'c' => Err(anyhow!("server sent CopyDone")),
            b'E' => Err(anyhow!("server sent error during copy")),
            _ => Ok(vec![]),
        }
    }

    async fn send_status_update(&self, stream: &mut TcpStream, lsn: Lsn) -> Result<()> {
        // StandbyStatusUpdate ('r') inside CopyData ('d')
        let mut payload = vec![STANDBY_STATUS];
        payload.extend_from_slice(&lsn.as_u64().to_be_bytes()); // received
        payload.extend_from_slice(&lsn.as_u64().to_be_bytes()); // flushed
        payload.extend_from_slice(&lsn.as_u64().to_be_bytes()); // applied
        payload.extend_from_slice(&0i64.to_be_bytes());          // clock
        payload.push(0);                                           // no reply requested

        let len = (payload.len() + 4) as u32;
        stream.write_all(&[MSG_COPY_DATA]).await?;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(&payload).await?;
        Ok(())
    }

    async fn read_byte(&self, stream: &mut TcpStream) -> Result<u8> {
        let mut buf = [0u8; 1];
        stream.read_exact(&mut buf).await?;
        Ok(buf[0])
    }

    async fn read_u32(&self, stream: &mut TcpStream) -> Result<u32> {
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await?;
        Ok(u32::from_be_bytes(buf))
    }

    fn lsn_to_pg_str(&self, lsn: Lsn) -> String {
        format!("{:X}/{:08X}", lsn.as_u64() >> 32, lsn.as_u64() & 0xFFFFFFFF)
    }

    async fn notify_pageserver(&self, base_url: &str, lsn: Lsn) -> Result<()> {
        let url = format!("{base_url}/wal/notify");
        self.http_client
            .post(&url)
            .json(&serde_json::json!({
                "tenant_id": self.tenant_id,
                "timeline_id": self.timeline_id,
                "lsn": lsn.as_u64(),
            }))
            .send()
            .await?;
        Ok(())
    }
}
