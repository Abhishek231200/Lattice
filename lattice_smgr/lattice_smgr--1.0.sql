-- Extension SQL file (required by PGXS even though all logic is in C)
-- lattice_smgr is a shared_preload_library, not a CREATE EXTENSION extension.
-- This file exists only to satisfy pgxs packaging.

-- Tell Postgres what version this is.
COMMENT ON SCHEMA public IS 'lattice_smgr 1.0 loaded';
