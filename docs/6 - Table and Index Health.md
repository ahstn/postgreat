## VI. Table and Index Health: Finding Bloat and Checking Index Usefulness

This guide moves from the query-level view to the table and index view. It covers PostgreSQL 14 through 18. Version differences are called out inline. The claim-by-claim audit is in [`docs/audit/claims.md`](./audit/claims.md).

All queries below ran on PostgreSQL 14.24, 15.19, 16.15, 17.11, and 18.6 unless a version note says otherwise.

### Before you start: how long have the counters been collecting?

Every signal in this guide comes from the cumulative statistics system. Those counters only mean something if they cover a long, representative period.[1]

- Counters **survive a clean shutdown and restart** in all supported versions.[1][1a]
- Counters are **reset** after a crash or immediate shutdown, after point-in-time recovery, and when someone calls `pg_stat_reset()` or a related reset function. The PostgreSQL 15 and later manuals also list starting from a base backup.[1][1a]
- `pg_stat_statements_reset()` resets only `pg_stat_statements`. It does not reset table or index counters.[2]
- Counters are kept per server. A standby has its own counts for queries run on it.[1]
- After `pg_stat_reset()`, autovacuum loses the counts it uses to decide when to run. Run a database-wide `ANALYZE` afterwards.[1]

```sql
SELECT datname, stats_reset, pg_postmaster_start_time() AS server_started
FROM pg_stat_database
WHERE datname = current_database();
```

`stats_reset` is NULL on PostgreSQL 15 and later if the database counters were never reset. PostgreSQL 14 shows the time the counters were created.

### A. Table Bloat Signals (`pg_stat_user_tables`)

`pg_stat_user_tables` tracks `n_live_tup` and `n_dead_tup`. Both are estimates.[1] The ratio of dead to live rows is a useful signal when you read it together with `last_autovacuum` and `last_autoanalyze`:

- A high dead-row ratio with an old `last_autovacuum` means autovacuum is not keeping up with this table, or something is holding back cleanup. Investigate. See [3 - Reactive Maintenance](./3%20-%20Reactive%20Maintenance%20-%20Vaccuming%20and%20Bloat.md).
- A high ratio with a very recent `last_autovacuum` can be normal on a high-churn table. It can also mean vacuum ran but could not remove rows because an old transaction or replication slot still needs them. Check `VACUUM (VERBOSE)` output or the autovacuum log.[3]

The dead-row ratio is not a direct measure of bloat on disk. Use `pgstattuple` when you need a real number.[4]

### B. Tables With Heavy Sequential Scans

`pg_stat_user_tables` also has `seq_scan`, `seq_tup_read`, and `idx_scan`. A common goal is to find large tables that are read mostly by sequential scans, which may mean an index is missing.

A naive `seq_scan > idx_scan` filter is a trap. For small tables, and for queries that read a large part of a table, a sequential scan is often the cheapest plan, and the planner is right to choose it.[5] The query below filters for tables large enough to matter and shows how many rows each sequential scan reads on average:

```sql
SELECT
  schemaname,
  relname AS table_name,
  seq_scan,
  seq_tup_read,
  seq_tup_read / NULLIF(seq_scan, 0) AS avg_rows_per_seq_scan,
  coalesce(idx_scan, 0) AS idx_scan,
  n_live_tup AS table_rows,
  pg_size_pretty(pg_relation_size(relid)) AS table_size
FROM pg_stat_user_tables
WHERE n_live_tup > 10000                       -- more than 10k rows
  AND pg_relation_size(relid) > 5 * 1024 * 1024 -- larger than 5 MB
  AND seq_scan * 50 > coalesce(idx_scan, 0)    -- seq scans are a big share
ORDER BY seq_tup_read DESC
LIMIT 15;
```

Notes:
- `idx_scan` is NULL for tables with no indexes, so the query uses `coalesce`.
- Using `relid` avoids failures on mixed-case or schema-qualified names.
- **Version note:** PostgreSQL 16 and later also have `last_seq_scan` and `last_idx_scan`, which show whether scans are still happening.[1]

A table on this list is a **candidate** for a new index, not proof. Find the queries behind the scans in `pg_stat_statements`, then check them with `EXPLAIN (ANALYZE, BUFFERS)`.[2][5]

### C. Index Diagnostics (`pg_stat_user_indexes`)

This view shows how often each index is used.[1] The columns are:

- `idx_scan`: number of scans that used the index.
- `idx_tup_read`: index entries returned by scans of this index.
- `idx_tup_fetch`: live table rows fetched by **simple** index scans using this index. Bitmap scans do not add to this column.[1]

#### Use case 1: Finding unused indexes

Indexes cost storage and slow down `INSERT`, `UPDATE`, and `DELETE`. An index that is truly unused is pure overhead.

A plain `WHERE idx_scan = 0` filter is unsafe. It also lists indexes that do work without being scanned: primary key, unique, and exclusion constraint indexes, and the replica identity index used by logical replication.

```sql
SELECT
  s.schemaname,
  s.relname AS table_name,
  s.indexrelname AS index_name,
  s.idx_scan,
  pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size,
  i.indisvalid AS is_valid,
  d.stats_reset,
  pg_get_indexdef(s.indexrelid) AS index_def
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
CROSS JOIN pg_stat_database d
WHERE d.datname = current_database()
  AND s.idx_scan = 0
  AND NOT i.indisunique
  AND NOT i.indisprimary
  AND NOT i.indisexclusion
  AND NOT i.indisreplident
  AND NOT EXISTS (
    SELECT 1 FROM pg_constraint c WHERE c.conindid = s.indexrelid
  )
ORDER BY pg_relation_size(s.indexrelid) DESC;
```

This is the same query as in [Helpful Commands](./Helpful%20Commands.md#find-unused-indexes-candidates-for-review).

Before you drop anything:
- Check that the counters cover a long, representative period (see the start of this guide).
- Run the query on every standby. An index unused on the primary may serve read queries on a replica.
- Think about rare jobs, such as month-end reports, that may not have run since the last reset.
- An index with `is_valid = false` is left over from a failed concurrent build. It is never scanned, but it still slows writes.
- Drop with `DROP INDEX CONCURRENTLY` and save the definition (`index_def`) so you can recreate it.[6]

Dropping a confirmed unused index lowers write overhead and frees storage. It is low risk once the checks above are done, but it is not free of risk.

#### Use case 2: Finding low-selectivity indexes

An index is selective when each scan returns few rows. The average entries returned per scan is:

`avg_tup_read_per_scan = idx_tup_read / idx_scan` (when `idx_scan > 0`)

```sql
SELECT
  s.relid::regclass AS table_name,
  s.indexrelid::regclass AS index_name,
  s.idx_scan,
  s.idx_tup_read / NULLIF(s.idx_scan, 0) AS avg_tup_read_per_scan,
  c.reltuples::bigint AS table_rows_estimate
FROM pg_stat_user_indexes s
JOIN pg_class c ON c.oid = s.relid
WHERE s.idx_scan > 0
ORDER BY avg_tup_read_per_scan DESC
LIMIT 20;
```

If the average is a large share of the table's rows, the index is not filtering much. The planner usually prefers a sequential scan in that case, so a high average often comes from bitmap scans or range queries that are reasonable.[5] Treat it as a reason to look at the queries. A more selective composite index or a partial index may help.

#### Use case 3: Checking whether index-only scans can work

An index-only scan answers a query from the index without visiting the table, but only for pages that the visibility map marks all-visible.[7]

The ratio `idx_tup_fetch / idx_tup_read` **cannot** reliably show failed index-only scans:
- Most index scans are ordinary index scans that need other columns from the table. For those, a ratio near 1.0 is normal.
- `idx_tup_fetch` is also lower when scans meet dead or uncommitted rows, and bitmap scans do not add to it at all.[1]
- The counters cover every query that used the index, so one ratio mixes many query shapes.

Use these checks instead:

1. **Look at the plan.** `EXPLAIN (ANALYZE)` shows `Index Only Scan` and `Heap Fetches`. Many heap fetches mean pages are not all-visible.[7][8]
2. **Check visibility coverage.** A low share of all-visible pages makes index-only scans visit the heap often. `VACUUM` sets the all-visible bits.[7]

```sql
SELECT
  c.oid::regclass AS table_name,
  c.relpages,
  c.relallvisible,
  round(100.0 * c.relallvisible / NULLIF(c.relpages, 0), 1) AS pct_all_visible
FROM pg_class c
WHERE c.relkind = 'r'
  AND c.relnamespace NOT IN ('pg_catalog'::regnamespace, 'information_schema'::regnamespace)
  AND c.relpages > 1000
ORDER BY pct_all_visible NULLS FIRST
LIMIT 20;
```

3. **If the plan is a plain index scan** because the query needs a column the index does not have, consider adding that column with `INCLUDE`. See [2 - Proactive Maintenance](./2%20-%20Proactive%20Maintenance%20-%20Indexes.md#d-index-only-scans-and-include).

`relpages` and `relallvisible` are updated by `VACUUM`, `ANALYZE`, and a few DDL commands, so they are estimates.[9]

**Table: Index efficiency signals**

| Signal | Columns used | What it means | Action |
|---|---|---|---|
| Unused index | `idx_scan` from Use case 1 | No scans since the counters were last reset. | Check the period, standbys, and rare jobs. Then `DROP INDEX CONCURRENTLY`. |
| Low selectivity | `idx_tup_read / idx_scan` | Each scan returns many entries. | Review the queries. Consider a composite or partial index. |
| Index-only scans visiting the heap | `Heap Fetches` in `EXPLAIN (ANALYZE)`, `relallvisible / relpages` | Many pages are not all-visible. | Tune autovacuum for the table, or run `VACUUM`. |
| Plain index scans that could be index-only | Plan shows `Index Scan` for a query needing one or two extra columns | The index lacks the needed columns. | Consider `INCLUDE`. Weigh the extra write and storage cost. |

## References

Links point to a specific major version. PostgreSQL 14 pages say the same thing unless a version note says otherwise.

- [1] [PostgreSQL 18: Cumulative Statistics System][1]
- [1a] [PostgreSQL 14: The Statistics Collector][1a]
- [2] [PostgreSQL 18: pg_stat_statements][2]
- [3] [PostgreSQL 18: Routine Vacuuming][3]
- [4] [PostgreSQL 18: pgstattuple][4]
- [5] [PostgreSQL 18: Examining Index Usage][5]
- [6] [PostgreSQL 18: DROP INDEX][6]
- [7] [PostgreSQL 18: Index-Only Scans and Covering Indexes][7]
- [8] [PostgreSQL 18: Using EXPLAIN][8]
- [9] [PostgreSQL 18: pg_class][9]

Further reading (community):

- [Cybertec: Get rid of your unused indexes][f1]

[1]: https://www.postgresql.org/docs/18/monitoring-stats.html
[1a]: https://www.postgresql.org/docs/14/monitoring-stats.html
[2]: https://www.postgresql.org/docs/18/pgstatstatements.html
[3]: https://www.postgresql.org/docs/18/routine-vacuuming.html
[4]: https://www.postgresql.org/docs/18/pgstattuple.html
[5]: https://www.postgresql.org/docs/18/indexes-examine.html
[6]: https://www.postgresql.org/docs/18/sql-dropindex.html
[7]: https://www.postgresql.org/docs/18/indexes-index-only-scans.html
[8]: https://www.postgresql.org/docs/18/using-explain.html
[9]: https://www.postgresql.org/docs/18/catalog-pg-class.html
[f1]: https://www.cybertec-postgresql.com/en/get-rid-of-your-unused-indexes/
