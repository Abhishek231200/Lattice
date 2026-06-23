/// Compute shim — bridges Postgres page reads to the Lattice pageserver.
///
/// There are two integration paths (Phase 5):
///
/// Path A (shim service — this file):
///   A Rust service that exposes a local socket following the Postgres FE/BE protocol.
///   When Postgres is configured to use this as its storage manager backend via the
///   `lattice_smgr` extension, page reads are proxied to the pageserver.
///   Used for local demo/testing without a C extension.
///
/// Path B (real Postgres extension — lattice_smgr/):
///   A C extension (pgsm hook) that intercepts `smgr_read` at the Postgres level and
///   calls into the pageserver over HTTP.  Enables real Postgres queries through Lattice.
///   Requires Postgres 16 headers.  Built separately with `pgxs`.

pub mod page_cache;
pub mod pageserver_client;
pub mod smgr_proxy;
