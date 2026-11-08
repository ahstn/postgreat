# Helpful Commands

Diagnostic queries for slow queries, table scans and index usage. They are starting points for investigation. None of them proves a problem on its own.

All queries were checked against PostgreSQL 14, 15, 16, 17 and 18.

## Before You Start

**Requirements for `pg_stat_statements` queries:**
- Add `pg_stat_statements` to `shared_preload_libraries` and restart the server.
- Run `CREATE EXTENSION pg_stat_statements;` in the database you connect to. The view shows statements from all databases on the server, but it only exists where the extension is created.
- To see the query text and `queryid` of statements run by other roles, you need superuser or the privileges of the `pg_read_all_stats` role. Without it you still see the numbers, but `query` shows `<insufficient privilege>` for other users' statements.
- Set `track_io_timing = on` if you want I/O timing columns to be non-zero.

**How to read the numbers:**
- All `pg_stat_*` and `pg_stat_statements` counters are cumulative since the last reset. A high number can be old history. Check when stats were last reset: `pg_stat_database.stats_reset` for table and index counters, and `pg_stat_statements_info.stats_reset` for statement counters.
- Counters are kept per server. A primary and each standby have their own counters. An index that looks unused on the primary may be used by read queries on a standby.
- Table and index counters are reset by `pg_stat_reset()` and related functions. They survive a clean restart. They are reset after an unclean shutdown (crash or immediate shutdown), after point-in-time recovery, and, on PostgreSQL 15 and later, when starting from a base backup. This applies to PostgreSQL 14 too: the old statistics collector also saved its file on clean shutdown.
- `pg_stat_statements` counters are reset by `pg_stat_statements_reset()`. They survive a clean restart when `pg_stat_statements.save = on` (the default).
- `pg_stat_user_tables` and `pg_stat_user_indexes` only show the current database. Always look at `schemaname` as well as the table name, because the same table name can exist in more than one schema.
- Times in `pg_stat_statements` are in milliseconds. Block counts are in blocks (8kB by default).

**Version notes:**
- `total_exec_time`, `mean_exec_time`, `max_exec_time` and `stddev_exec_time` exist in all supported versions (added in PostgreSQL 13).
- I/O timing columns were renamed in PostgreSQL 17: `blk_read_time` and `blk_write_time` became `shared_blk_read_time` and `shared_blk_write_time`. The queries below do not use these columns, so they run unchanged on 14 to 18.
- `last_seq_scan` and `last_idx_scan` exist in `pg_stat_user_tables` and `pg_stat_user_indexes` from PostgreSQL 16. They are useful for "when was this last used", but not available on 14 or 15.

## Find Slowest Queries (by total time)

```sql
SELECT
  round(total_exec_time::numeric, 2) AS total_time_ms,
  calls,
  round(mean_exec_time::numeric, 2) AS avg_time_ms,
  round((100 * total_exec_time / NULLIF(sum(total_exec_time) OVER (), 0))::numeric, 2) AS percent_total,
  query
FROM pg_stat_statements
--WHERE query ILIKE '%<query>%' -- uncomment to filter by query text
ORDER BY total_exec_time DESC
LIMIT 20;
```

## Find Queries with Highest Average Execution Time

```sql
SELECT
  round(mean_exec_time::numeric, 2) AS avg_time_ms,
  round(max_exec_time::numeric, 2) AS max_time_ms,
  calls,
  query
FROM pg_stat_statements
WHERE calls > 10  -- filter out rarely-run queries
ORDER BY mean_exec_time DESC
LIMIT 20;
```

## Find Tables with the Most Rows Read by Sequential Scans

Large values point at tables worth checking with `EXPLAIN`. A sequential scan is often the right plan, for example on small tables or queries that read most of a table.

```sql
SELECT
  schemaname,
  relname AS table_name,
  seq_scan,
  seq_tup_read,
  idx_scan,
  idx_tup_fetch,
  round(seq_tup_read::numeric / NULLIF(seq_scan, 0), 2) AS avg_rows_per_seq_scan
FROM pg_stat_user_tables
WHERE seq_scan > 0
ORDER BY seq_tup_read DESC
LIMIT 20;
```

## Find Tables with High Seq Scan to Index Scan Ratio

`idx_scan` is NULL when a table has no indexes. The ratio is NULL when there were no index scans.

```sql
SELECT
  schemaname,
  relname AS table_name,
  seq_scan,
  COALESCE(idx_scan, 0) AS idx_scan,
  round(seq_scan::numeric / NULLIF(COALESCE(idx_scan, 0), 0), 2) AS seq_to_idx_ratio,
  seq_tup_read,
  n_live_tup AS approx_row_count
FROM pg_stat_user_tables
WHERE n_live_tup > 1000  -- focus on tables with data
ORDER BY seq_tup_read DESC
LIMIT 20;
```

## Find Unused Indexes (candidates for review)

This lists indexes with no scans since the last stats reset. It leaves out indexes that back a constraint (primary key, unique, exclusion), unique indexes and the replica identity index, because those do work even when they are never scanned.

Before dropping anything:
- Check `stats_reset` in the output. A recent reset makes every index look unused. NULL means the counters have not been reset since they were created (seen on PostgreSQL 15 and later; 14 fills in the time the database stats were created). An unclean shutdown also discards counters, so also check how long the server has been up with `pg_postmaster_start_time()`.
- Run the same query on every standby. Counters are per server.
- Think about rare jobs (month-end reports, batch jobs) that may not have run since the reset.
- Drop with `DROP INDEX CONCURRENTLY` and keep the definition (`pg_get_indexdef`) so you can recreate it.

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

## Find Index Usage Statistics

```sql
SELECT
  schemaname,
  relname AS table_name,
  indexrelname AS index_name,
  idx_scan AS times_used,
  idx_tup_read,
  idx_tup_fetch,
  pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
-- WHERE relname LIKE '%<table>%' -- uncomment to filter by table name
ORDER BY idx_scan DESC;
```

## Find Queries Reading the Most Blocks per Call

`shared_blks_read` counts blocks that were not in `shared_buffers`. They may still have come from the OS page cache. A high value is a reason to look at the plan with `EXPLAIN (ANALYZE, BUFFERS)`. It is not proof of a missing index: large scans, cold caches and reporting queries also read many blocks.

```sql
SELECT
  round(total_exec_time::numeric, 2) AS total_time_ms,
  calls,
  round(shared_blks_read::numeric / NULLIF(calls, 0), 2) AS avg_blks_read,
  round(rows::numeric / NULLIF(calls, 0), 2) AS avg_rows,
  query
FROM pg_stat_statements
WHERE shared_blks_read > 1000
  -- AND query ILIKE '%<query>%' -- uncomment to filter by query text
ORDER BY shared_blks_read DESC
LIMIT 20;
```

## Find Queries Writing the Most Temporary Files (temp spill)

Temp writes usually mean a sort or hash did not fit in `work_mem` (times `hash_mem_multiplier` for hashes). Raise `work_mem` for the role or session that runs these queries before changing it globally.

```sql
SELECT
  round(total_exec_time::numeric, 2) AS total_time_ms,
  calls,
  temp_blks_written,
  temp_blks_read,
  round(temp_blks_written::numeric / NULLIF(calls, 0), 2) AS avg_temp_blks_written,
  query
FROM pg_stat_statements
WHERE temp_blks_written > 0
ORDER BY temp_blks_written DESC
LIMIT 20;
```

## Find Queries with Highest Shared Block Reads (I/O heavy)

```sql
SELECT
  round(total_exec_time::numeric, 2) AS total_time_ms,
  calls,
  shared_blks_read,
  shared_blks_hit,
  round(100.0 * shared_blks_hit / NULLIF(shared_blks_hit + shared_blks_read, 0), 2) AS hit_percent,
  query
FROM pg_stat_statements
ORDER BY shared_blks_read DESC
LIMIT 20;
```

## Find Queries with Highest Execution Time Variance

```sql
SELECT
  round(mean_exec_time::numeric, 2) AS avg_time_ms,
  round(stddev_exec_time::numeric, 2) AS stddev_time_ms,
  calls,
  query
FROM pg_stat_statements
WHERE calls > 10
ORDER BY stddev_exec_time DESC
LIMIT 20;
```

## Find Queries Returning Many Rows per Call

```sql
SELECT
  round(mean_exec_time::numeric, 2) AS avg_time_ms,
  calls,
  round((rows::numeric / NULLIF(calls, 0)), 2) AS avg_rows_per_call,
  query
FROM pg_stat_statements
WHERE calls > 10
ORDER BY avg_rows_per_call DESC
LIMIT 20;
```

## References

- [pg_stat_statements (PostgreSQL 14)](https://www.postgresql.org/docs/14/pgstatstatements.html)
- [pg_stat_statements (PostgreSQL 17)](https://www.postgresql.org/docs/17/pgstatstatements.html), column renames for I/O timing
- [pg_stat_statements (PostgreSQL 18)](https://www.postgresql.org/docs/18/pgstatstatements.html)
- [The Statistics Collector (PostgreSQL 14)](https://www.postgresql.org/docs/14/monitoring-stats.html)
- [The Cumulative Statistics System (PostgreSQL 18)](https://www.postgresql.org/docs/18/monitoring-stats.html)
- [pg_index catalog (PostgreSQL 18)](https://www.postgresql.org/docs/18/catalog-pg-index.html)
