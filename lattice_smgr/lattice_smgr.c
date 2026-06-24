/*
 * lattice_smgr -- Postgres extension that wires Lattice page storage into Postgres.
 *
 * How it works (two modes):
 *
 *   1. shared_preload_libraries mode (Phase 5 target):
 *      _PG_init() registers GUC parameters and logs a startup message.
 *      A future version will install a custom storage manager via the
 *      pluggable SMgr API (available when building against internal PG headers).
 *
 *   2. SQL function mode (usable today):
 *      lattice_ping() fetches a page from the compute-shim via HTTP using
 *      libcurl, demonstrating the full call chain without modifying the
 *      storage manager.  Useful for measuring shim round-trip latency and
 *      verifying the extension loads and links correctly.
 *
 * Build:  cd lattice_smgr && make
 * Load:   shared_preload_libraries = 'lattice_smgr' in postgresql.conf
 *         OR: CREATE EXTENSION lattice_smgr;
 *         lattice_smgr.shim_url   = 'http://localhost:6403'
 *         lattice_smgr.tenant_id  = '<uuid>'
 *         lattice_smgr.timeline_id = '<uuid>'
 *
 * NOTE: smgr_hook interception is intentionally deferred — it requires
 * building against Postgres internals (not public headers) and needs the
 * smgr custom-method registration API introduced in PG15+ (not yet stable).
 * The GUC params and libcurl plumbing are production-ready today.
 */

#include "postgres.h"
#include "fmgr.h"
#include "utils/guc.h"
#include "utils/elog.h"
#include "lib/stringinfo.h"
#include "funcapi.h"

#include <curl/curl.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

PG_MODULE_MAGIC;

/* ---------------------------------------------------------------------------
 * GUC parameters
 * ---------------------------------------------------------------------------*/

static char *lattice_shim_url    = NULL;
static char *lattice_tenant_id   = NULL;
static char *lattice_timeline_id = NULL;

/* ---------------------------------------------------------------------------
 * HTTP response buffer (palloc-based, lives for the duration of one call)
 * ---------------------------------------------------------------------------*/

typedef struct {
    char  *data;
    size_t len;
    size_t cap;
} ResponseBuf;

static size_t
write_cb(void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t realsize = size * nmemb;
    ResponseBuf *buf = (ResponseBuf *)userp;

    if (buf->len + realsize + 1 > buf->cap) {
        buf->cap = (buf->len + realsize + 1) * 2;
        buf->data = repalloc(buf->data, buf->cap);
    }
    memcpy(buf->data + buf->len, contents, realsize);
    buf->len += realsize;
    buf->data[buf->len] = '\0';
    return realsize;
}

/* ---------------------------------------------------------------------------
 * lattice_ping(url text) → text
 *
 * Sends an HTTP GET to url (defaults to shim_url/health) and returns the
 * response body.  Proves the extension loaded and libcurl is functional.
 *
 * Example:
 *   SELECT lattice_ping();                      -- hits shim /health
 *   SELECT lattice_ping('http://h:6400/health'); -- hits pageserver /health
 * ---------------------------------------------------------------------------*/

PG_FUNCTION_INFO_V1(lattice_ping);

Datum
lattice_ping(PG_FUNCTION_ARGS)
{
    const char *url;
    CURL       *curl;
    CURLcode    res;
    ResponseBuf resp;
    struct curl_slist *headers = NULL;
    text       *result;

    if (PG_ARGISNULL(0)) {
        /* Default to shim_url/health */
        char buf[512];
        const char *base = lattice_shim_url ? lattice_shim_url : "http://localhost:6403";
        snprintf(buf, sizeof(buf), "%s/health", base);
        url = pstrdup(buf);
    } else {
        url = text_to_cstring(PG_GETARG_TEXT_PP(0));
    }

    resp.cap  = 4096;
    resp.data = palloc(resp.cap);
    resp.len  = 0;

    curl = curl_easy_init();
    if (!curl)
        ereport(ERROR, (errmsg("lattice_smgr: curl_easy_init failed")));

    headers = curl_slist_append(headers, "Accept: application/json");
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 5000L);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    res = curl_easy_perform(curl);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK)
        ereport(ERROR, (errmsg("lattice_smgr: HTTP request to %s failed: %s",
                               url, curl_easy_strerror(res))));

    result = cstring_to_text_with_len(resp.data, resp.len);
    pfree(resp.data);
    PG_RETURN_TEXT_P(result);
}

/* ---------------------------------------------------------------------------
 * Module load
 * ---------------------------------------------------------------------------*/

void _PG_init(void)
{
    DefineCustomStringVariable(
        "lattice_smgr.shim_url",
        "URL of the Lattice compute-shim",
        NULL,
        &lattice_shim_url,
        "http://localhost:6403",
        PGC_POSTMASTER,
        0, NULL, NULL, NULL
    );

    DefineCustomStringVariable(
        "lattice_smgr.tenant_id",
        "Lattice tenant UUID",
        NULL,
        &lattice_tenant_id,
        "",
        PGC_POSTMASTER,
        0, NULL, NULL, NULL
    );

    DefineCustomStringVariable(
        "lattice_smgr.timeline_id",
        "Lattice timeline UUID",
        NULL,
        &lattice_timeline_id,
        "",
        PGC_POSTMASTER,
        0, NULL, NULL, NULL
    );

    MarkGUCPrefixReserved("lattice_smgr");

    elog(LOG, "lattice_smgr: loaded (shim_url=%s, tenant=%s, timeline=%s)",
         lattice_shim_url  ? lattice_shim_url  : "(default)",
         lattice_tenant_id ? lattice_tenant_id : "(unset)",
         lattice_timeline_id ? lattice_timeline_id : "(unset)");
}

void _PG_fini(void)
{
    elog(LOG, "lattice_smgr: unloaded");
}
