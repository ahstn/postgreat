## Find Slowest Queries (by total time)

```sql
SELECT
  round(total_exec_time::numeric, 2) AS total_time_ms,
  calls,
  round(mean_exec_time::numeric, 2) AS avg_time_ms,
  round((100 * total_exec_time / sum(total_exec_time) OVER ())::numeric, 2) AS percent_total,
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

## Find Queries Doing Sequential Scans (likely missing indexes)

```sql
SELECT
  relname AS table_name,
  seq_scan,
  seq_tup_read,
  idx_scan,
  idx_tup_fetch,
  CASE WHEN seq_scan > 0
    THEN round((seq_tup_read::numeric / seq_scan), 2)
    ELSE 0
  END AS avg_rows_per_seq_scan
FROM pg_stat_user_tables
WHERE seq_scan > 0
ORDER BY seq_tup_read DESC
LIMIT 20;
```
## Find Tables with High Seq Scan to Index Scan Ratio

```sql
SELECT
  schemaname,
  relname AS table_name,
  seq_scan,
  idx_scan,
  CASE WHEN idx_scan > 0
    THEN round((seq_scan::numeric / idx_scan), 2)
    ELSE seq_scan
  END AS seq_to_idx_ratio,
  n_live_tup AS row_count
FROM pg_stat_user_tables
WHERE n_live_tup > 1000  -- focus on tables with data
ORDER BY seq_to_idx_ratio DESC
LIMIT 20;
```
## Find Unused Indexes (candidates for removal)

```sql
SELECT
  schemaname,
  relname AS table_name,
  indexrelname AS index_name,
  idx_scan,
  idx_tup_read,
  pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
WHERE idx_scan = 0
  AND indexrelname NOT LIKE '%_pkey'  -- keep primary keys
ORDER BY pg_relation_size(indexrelid) DESC;
```

## Find Index Usage Statistics

```sql
SELECT
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

## Identify Missing Indexes (queries with high rows read but no index)

```sql
SELECT
  round(total_exec_time::numeric, 2) AS total_time_ms,
  calls,
  round(shared_blks_read::numeric / NULLIF(calls, 0), 2) AS avg_blks_read,
  query
FROM pg_stat_statements
WHERE shared_blks_read > 1000
  -- AND query ILIKE '%<query>%' -- uncomment to filter by query text
ORDER BY shared_blks_read DESC
LIMIT 20;
```

## Find Queries Writing the Most Temporary Files (temp spill)

```sql
SELECT
  round(total_exec_time::numeric, 2) AS total_time_ms,
  calls,
  temp_blks_written,
  temp_blks_read,
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
  query
FROM pg_stat_statements
ORDER BY shared_blks_read DESC
LIMIT 20;
```

## Find Queries with Highest Execution Time Variance (PG13+)

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
