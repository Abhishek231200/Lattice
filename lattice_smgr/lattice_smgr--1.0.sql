-- lattice_smgr extension SQL
-- Declares the lattice_ping() function that exercises the libcurl → shim path.

CREATE FUNCTION lattice_ping(url text DEFAULT NULL)
RETURNS text
AS 'MODULE_PATHNAME', 'lattice_ping'
LANGUAGE C CALLED ON NULL INPUT;

COMMENT ON FUNCTION lattice_ping(text) IS
    'HTTP GET to the Lattice compute-shim (or a custom URL). '
    'Returns the response body. Used to verify the extension loaded '
    'and libcurl is operational inside Postgres.';
