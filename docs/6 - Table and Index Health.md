## Table and Index Health: Pruning Bloat and Optimizing Access

This section expands on the analysis of table bloat and indexing strategies, moving from the query-level view to the table and index perspective.

### A. Table Bloat Analysis (Revisiting `pg_stat_user_tables`)

The user's provided document correctly identifies `pg_stat_user_tables` as the primary tool for monitoring bloat, using the "Bloat Hit List" query. This view tracks `n_live_tup` (live rows) and `n_dead_tup` (dead rows).

The key insight is that the _ratio_ of `n_dead_tup` to `n_live_tup` is the correct metric. However, this must be correlated with `last_autovacuum` and `last_autoanalyze`.

- A high `dead_tup_ratio` with an old `last_autovacuum` timestamp indicates autovacuum is not keeping up or is misconfigured for this table. This is a _problem_ requiring investigation.

- A high `dead_tup_ratio` with a very recent `last_autovacuum` timestamp is not necessarily a problem. It simply indicates a high-churn table that autovacuum is _successfully_ processing.


### B. Identifying Inefficient Access Patterns: `pg_stat_all_tables`

This view provides aggregate access statistics for all tables, including `seq_scan` (sequential scans) and `idx_scan` (index scans).[7] A common goal is to find tables where `seq_scan` is high, suggesting a missing index.

However, a naive query for `seq_scan > idx_scan` is a trap. For small tables, a sequential scan is _faster_ than an index scan, and the PostgreSQL query planner correctly chooses it.[10] Such a query will produce thousands of "false positives" on trivial tables.

An _actionable_ query must filter for tables that are large enough for this pattern to matter. The following query identifies tables that are non-trivial in size and row count _and_ have a high sequential scan rate:

```
SELECT
  schemaname,
  relname AS table_name,
  seq_scan,
  idx_scan,
  n_live_tup AS table_rows,
  pg_size_pretty(pg_relation_size(relname::regclass)) AS table_size
FROM pg_stat_all_tables
WHERE
  schemaname = 'public'
  AND n_live_tup > 10000 -- Only tables with > 10k rows
  AND pg_relation_size(relname::regclass) > 5000000 -- Only tables > 5MB
  AND (seq_scan * 50) > idx_scan -- Seq scans are significant
ORDER BY
  seq_scan DESC
LIMIT 15;
```

A table appearing on this list is a high-confidence candidate for new indexes. The next step is to cross-reference this `table_name` with `pg_stat_statements` to find the _exact queries_ causing these sequential scans.

### C. Advanced Index Diagnostics: `pg_stat_user_indexes`

This view provides granular statistics on the _usage_ and _efficiency_ of individual indexes. It is the primary tool for pruning unused indexes and identifying inefficient ones.[11]

#### Use Case 1: Finding Unused Indexes (The "Safe" Way)

Indexes are not free; they incur storage overhead and slow down `INSERT`, `UPDATE`, and `DELETE` operations.[11] An unused index is pure overhead.

A simple query `WHERE idx_scan = 0` is _dangerous_.[12] It will list indexes that are used to enforce `UNIQUE` and `PRIMARY KEY` constraints. These indexes may have `idx_scan = 0` if they are never used for _searching_, but they are _required_ for data integrity.

The following query _safely_ finds droppable indexes by explicitly excluding unique constraints and expression-based indexes [13]:

```sql
SELECT
  s.schemaname,
  s.relname AS tablename,
  s.indexrelname AS indexname,
  pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size
FROM pg_catalog.pg_stat_user_indexes s
JOIN pg_catalog.pg_index i ON s.indexrelid = i.indexrelid
WHERE
  s.idx_scan = 0 -- Has never been scanned
  AND NOT i.indisunique -- Is not a UNIQUE index
  AND NOT EXISTS ( -- Does not enforce a constraint
    SELECT 1 FROM pg_catalog.pg_constraint c
    WHERE c.conindid = s.indexrelid
  )
ORDER BY
  pg_relation_size(s.indexrelid) DESC;
```

Dropping indexes from this list is a "free lunch" optimization, reducing write overhead and freeing storage. Note: These statistics reset on `pg_stat_statements_reset()` or server restarts, so this query should be run on a production system after a long uptime to be confident the index is truly unused.[11]

#### Use Case 2: Finding Inefficient, Low-Selectivity Indexes

The `pg_stat_user_indexes` view contains `idx_scan` (number of times index was used), `idx_tup_read` (number of index entries returned), and `idx_tup_fetch` (number of table rows fetched).[11] These allow for sophisticated efficiency analysis.

An index is "selective" if it returns only a few rows per scan. An "unselective" index returns a large fraction of the table. We can calculate this:

avg_tup_read_per_scan = idx_tup_read / (idx_scan + 1e-9)

If `avg_tup_read_per_scan` is very high (e.g., 50% of the table's total rows), the index is not an efficient filter. The query planner is using an index to fetch half the table, which is often slower than a sequential scan.10 This index is a prime candidate for replacement with a more selective (e.g., composite) index or a partial index.

#### Use Case 3: Finding Missing `INCLUDE` Indexes (Failed Index-Only Scans)

An Index-Only Scan (as mentioned in the user's document) is a powerful optimization where PostgreSQL answers a query _entirely_ from the index, without ever touching the table file (the "heap").

We can detect when this _should_ be happening but _isn't_.

- `idx_tup_read` counts the index entries read.

- `idx_tup_fetch` counts the table (heap) rows fetched.[11]


In a perfect Index-Only Scan, idx_tup_fetch should be 0. We can create a "Heap Fetch Ratio":

heap_fetch_ratio = idx_tup_fetch / (idx_tup_read + 1e-9)

If this ratio is high (near 1.0), it means that for every index entry we read, we are also performing a heap fetch. This indicates the Index-Only Scan is failing. This is a smoking gun for two potential problems:

1. **Missing `INCLUDE` Column:** The query is `SELECT col_a, col_b FROM tbl WHERE col_a = 1;`. The index is `ON tbl(col_a)`. The planner uses the index to find the row but must fetch the heap to get `col_b`. The fix is to use the `INCLUDE` clause: `CREATE INDEX... ON tbl(col_a) INCLUDE (col_b)`.

2. **Stale Visibility Map (VM):** The index _is_ covering, but PostgreSQL must fetch the heap anyway to check if the row is _visible_ to the current transaction. This indicates the table's visibility map is out of date. The fix is to run `VACUUM` on the table.


This analysis is summarized in Table 2.

**Table 2: Index Efficiency Analysis**

|**Metric**|**Columns Used**|**Meaning**|**Optimization Action**|
|---|---|---|---|
|**Unused Index**|`idx_scan` (from query in II.C.1)|Index exists but is never used for scans. Pure overhead.|`DROP INDEX`. Safely reduces write overhead.|
|**Low Selectivity**|`idx_tup_read / idx_scan`|Index is used, but returns a huge number of rows per scan.|Replace with a more selective composite or partial index.|
|**Failed Index-Only**|`idx_tup_fetch / idx_tup_read`|Ratio is near 1.0. Index-Only Scan is failing.|1. Recreate index with `INCLUDE` clause.<br><br>  <br><br>2. Run `VACUUM` on the table to update the visibility map.|

[1]: https://www.postgresql.org/docs/current/pgstatstatements.html
[2]: https://www.cybertec-postgresql.com/en/postgresql-detecting-slow-queries-quickly/
[3]: https://medium.com/@amareswer/postgresql-performance-tuning-with-pg-stat-statements-5f849c3d49ab
[4]: https://aiven.io/docs/products/postgresql/howto/identify-pg-slow-queries
[6]: https://postgres.ai/docs/postgres-howtos/performance-optimization/monitoring/how-to-reduce-wal-generation-rates

[10]: https://stackoverflow.com/questions/66820661/index-scan-vs-sequential-scan-in-postgres
[11]: https://medium.com/@anasanjaria/how-to-determine-unused-index-in-postgresql-6af846686a3
[12]: https://www.datacamp.com/doc/postgresql/dropping-unused-indexes
[13]: https://www.cybertec-postgresql.com/en/get-rid-of-your-unused-indexes/
