/*
 * lattice_smgr -- Postgres storage manager hook for Lattice.
 *
 * Intercepts smgr_read (block read) and routes the request to the Lattice
 * compute-shim over HTTP, which in turn calls the pageserver.
 *
 * Build: pgxs (see Makefile).
 * Load:  shared_preload_libraries = 'lattice_smgr' in postgresql.conf
 *        lattice_smgr.shim_url = 'http://localhost:5003'
 *        lattice_smgr.tenant_id = '<uuid>'
 *        lattice_smgr.timeline_id = '<uuid>'
 *
 * GUC parameters are read at startup (postmaster context) so they cannot be
 * changed per-session.  The actual HTTP call is synchronous in the backend
 * context (worker thread); we rely on the compute-shim's local page cache to
 * keep p99 latency low.
 *
 * NOTE: This implements Phase 5 Path B.  The HTTP approach trades a small
 * round-trip overhead for zero changes to Postgres internals beyond the hook.
 * A production implementation would use a shared-memory ring buffer or an
 * UNIX socket to eliminate the HTTP overhead.
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/smgr.h"
#include "storage/bufpage.h"
#include "utils/guc.h"
#include "utils/elog.h"
#include "lib/stringinfo.h"
#include "postmaster/bgworker.h"

#include <curl/curl.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

PG_MODULE_MAGIC;

/* ---------------------------------------------------------------------------
 * GUC parameters
 * ---------------------------------------------------------------------------*/

static char *lattice_shim_url   = NULL;
static char *lattice_tenant_id  = NULL;
static char *lattice_timeline_id = NULL;

/* ---------------------------------------------------------------------------
 * HTTP response buffer
 * ---------------------------------------------------------------------------*/

typedef struct {
    char  *data;
    size_t len;
    size_t cap;
} ResponseBuf;

static size_t write_cb(void *contents, size_t size, size_t nmemb, void *userp)
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
 * Fetch a page from the compute-shim.
 * Returns true and fills `buf` (BLCKSZ bytes) on success.
 * ---------------------------------------------------------------------------*/

static bool
lattice_fetch_page(RelFileNode rnode, ForkNumber forknum, BlockNumber blkno,
                   uint64 lsn, char *buf)
{
    CURL       *curl;
    CURLcode    res;
    ResponseBuf resp;
    char        url[512];
    char        json_body[512];
    struct curl_slist *headers = NULL;
    bool        ok = false;

    resp.cap  = BLCKSZ * 2;
    resp.data = palloc(resp.cap);
    resp.len  = 0;

    /* Build request URL and JSON body */
    snprintf(url, sizeof(url), "%s/smgr/page",
             lattice_shim_url ? lattice_shim_url : "http://localhost:5003");

    snprintf(json_body, sizeof(json_body),
             "{\"rel_spcnode\":%u,\"rel_dbnode\":%u,\"rel_relnode\":%u,"
             "\"forknum\":%d,\"blkno\":%u,\"lsn\":%lu}",
             rnode.spcNode, rnode.dbNode, rnode.relNode,
             (int)forknum, blkno, (unsigned long)lsn);

    curl = curl_easy_init();
    if (!curl)
    {
        elog(WARNING, "lattice_smgr: curl_easy_init failed");
        return false;
    }

    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_body);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 5000L);

    res = curl_easy_perform(curl);
    if (res != CURLE_OK)
    {
        elog(WARNING, "lattice_smgr: HTTP request failed: %s", curl_easy_strerror(res));
        goto cleanup;
    }

    /* Response JSON: {"page_b64":"...","cache_hit":...}
     * Decode the base64 page directly into `buf`.
     * We do a minimal inline base64 decode to avoid a dependency. */
    {
        const char *b64_start = strstr(resp.data, "\"page_b64\":\"");
        if (!b64_start)
        {
            elog(WARNING, "lattice_smgr: missing page_b64 in response");
            goto cleanup;
        }
        b64_start += strlen("\"page_b64\":\"");
        const char *b64_end = strchr(b64_start, '"');
        if (!b64_end)
        {
            elog(WARNING, "lattice_smgr: malformed page_b64 field");
            goto cleanup;
        }

        size_t b64_len = b64_end - b64_start;
        ok = lattice_base64_decode(b64_start, b64_len, buf);
    }

cleanup:
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    pfree(resp.data);
    return ok;
}

/* Minimal base64 decoder.  Returns true on success. */
static bool
lattice_base64_decode(const char *src, size_t src_len, char *dst)
{
    static const int8_t table[256] = {
        ['A']=0,['B']=1,['C']=2,['D']=3,['E']=4,['F']=5,['G']=6,['H']=7,
        ['I']=8,['J']=9,['K']=10,['L']=11,['M']=12,['N']=13,['O']=14,['P']=15,
        ['Q']=16,['R']=17,['S']=18,['T']=19,['U']=20,['V']=21,['W']=22,['X']=23,
        ['Y']=24,['Z']=25,['a']=26,['b']=27,['c']=28,['d']=29,['e']=30,['f']=31,
        ['g']=32,['h']=33,['i']=34,['j']=35,['k']=36,['l']=37,['m']=38,['n']=39,
        ['o']=40,['p']=41,['q']=42,['r']=43,['s']=44,['t']=45,['u']=46,['v']=47,
        ['w']=48,['x']=49,['y']=50,['z']=51,['0']=52,['1']=53,['2']=54,['3']=55,
        ['4']=56,['5']=57,['6']=58,['7']=59,['8']=60,['9']=61,['+']=62,['/']=63,
        ['=']=0, ['\0']=-1,
    };
    memset(dst, 0, BLCKSZ);

    size_t out_pos = 0;
    for (size_t i = 0; i < src_len; i += 4) {
        if (out_pos + 3 > BLCKSZ) break;
        int8_t a = table[(uint8_t)src[i]];
        int8_t b = (i+1 < src_len) ? table[(uint8_t)src[i+1]] : 0;
        int8_t c = (i+2 < src_len) ? table[(uint8_t)src[i+2]] : 0;
        int8_t d = (i+3 < src_len) ? table[(uint8_t)src[i+3]] : 0;
        dst[out_pos++] = (a << 2) | (b >> 4);
        if (i+2 < src_len && src[i+2] != '=') dst[out_pos++] = ((b & 0xF) << 4) | (c >> 2);
        if (i+3 < src_len && src[i+3] != '=') dst[out_pos++] = ((c & 3) << 6) | d;
    }
    return (out_pos == BLCKSZ);
}

/* ---------------------------------------------------------------------------
 * smgr hook — intercepts smgr_read
 * ---------------------------------------------------------------------------*/

static smgr_hook_type prev_smgr_hook = NULL;

static SMgrRelation
lattice_smgr_open(RelFileNodeBackend rnode)
{
    return smgropen(rnode.node, rnode.backend);
}

/*
 * Hook installed on smgr_read_hook (Postgres 16+).
 * When set, Postgres calls this INSTEAD of the default md (heap file) read.
 */
static void
lattice_smgr_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                  char *buffer)
{
    uint64 lsn = 0; /* TODO: read current WAL LSN from shared memory */

    if (!lattice_fetch_page(reln->smgr_rnode.node, forknum, blocknum, lsn, buffer))
    {
        /*
         * Fallback to the default storage manager.  This covers relations that
         * Lattice doesn't manage (e.g., pg_catalog tables during startup) and
         * cases where the shim is unreachable.
         */
        if (prev_smgr_hook)
            prev_smgr_hook(reln, forknum, blocknum, buffer);
        else
            smgrread(reln, forknum, blocknum, buffer);
    }
}

/* ---------------------------------------------------------------------------
 * Module load / unload
 * ---------------------------------------------------------------------------*/

void _PG_init(void)
{
    /* Register GUC parameters */
    DefineCustomStringVariable(
        "lattice_smgr.shim_url",
        "URL of the Lattice compute-shim (e.g. http://localhost:5003)",
        NULL,
        &lattice_shim_url,
        "http://localhost:5003",
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

    /* Install the smgr hook */
    prev_smgr_hook = smgr_hook;
    smgr_hook = lattice_smgr_read;

    elog(LOG, "lattice_smgr: loaded (shim_url=%s)",
         lattice_shim_url ? lattice_shim_url : "(default)");
}

void _PG_fini(void)
{
    smgr_hook = prev_smgr_hook;
}
