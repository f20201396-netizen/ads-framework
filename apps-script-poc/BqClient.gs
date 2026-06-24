/**
 * BqClient.gs — read-only BigQuery wrapper using the Advanced Service.
 *
 * Strict policy: this script never writes to BQ. Only query() is exposed.
 * Every call dry-runs first and aborts if scan estimate exceeds BQ.COST_CAP_BYTES.
 */

const BqClient = (function () {
  /** Returns estimated bytes scanned for `sql`. Throws on syntax errors. */
  function dryRunBytes(sql) {
    const cfg = Config.getAll();
    const req = {
      configuration: {
        query: {
          query:                 sql,
          useLegacySql:          false,
          dryRun:                true,
        },
      },
    };
    const job = Bigquery.Jobs.insert(req, cfg.bqProject);
    const bytes = parseInt(job.statistics.totalBytesProcessed, 10);
    return isNaN(bytes) ? 0 : bytes;
  }

  /**
   * Run a SELECT and return rows as plain objects keyed by column name.
   * Aborts if dry-run says > CAP bytes.
   */
  function query(sql, opts) {
    const cfg = Config.getAll();
    const CAP = Config.BQ.COST_CAP_BYTES;
    opts = opts || {};
    if (!opts.skipDryRun) {
      const bytes = dryRunBytes(sql);
      if (bytes > CAP) {
        throw new Error('BQ dry-run estimate ' + bytes + ' bytes exceeds cap ' + CAP);
      }
      Logger.log('BQ dry-run: ' + (bytes / 1e6).toFixed(1) + ' MB');
    }

    const req = {
      query:        sql,
      useLegacySql: false,
      timeoutMs:    180000,  // 3 min — BQ blocks until results or timeout
      maxResults:   10000,
    };
    let resp = Bigquery.Jobs.query(req, cfg.bqProject);

    const schema = resp.schema && resp.schema.fields ? resp.schema.fields : [];
    const out = [];

    const collect = function (rows) {
      if (!rows || !rows.length) return;
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const obj = {};
        for (let j = 0; j < schema.length; j++) {
          const f = schema[j];
          const v = r.f[j].v;
          obj[f.name] = coerce(v, f);
        }
        out.push(obj);
      }
    };

    collect(resp.rows);

    // Paginate any remaining results.
    while (resp.pageToken) {
      resp = Bigquery.Jobs.getQueryResults(cfg.bqProject, resp.jobReference.jobId, {
        pageToken:  resp.pageToken,
        maxResults: 10000,
      });
      collect(resp.rows);
    }

    Logger.log('BQ query returned ' + out.length + ' rows');
    return out;
  }

  /** Cast BQ scalar string values to JS types based on field schema. */
  function coerce(v, field) {
    if (v == null) return null;
    const t = field.type;
    if (t === 'INTEGER' || t === 'INT64')         return parseInt(v, 10);
    if (t === 'FLOAT'   || t === 'FLOAT64' || t === 'NUMERIC' || t === 'BIGNUMERIC') return parseFloat(v);
    if (t === 'BOOLEAN' || t === 'BOOL')          return v === 'true' || v === true;
    if (t === 'DATE')                              return v;  // 'YYYY-MM-DD' string
    if (t === 'TIMESTAMP' || t === 'DATETIME')     return v;  // ISO string
    return v;
  }

  return { query, dryRunBytes };
})();
