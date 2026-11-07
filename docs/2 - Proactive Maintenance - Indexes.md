## II. Proactive Maintenance: Indexing Strategies

**Overview:**

Creating the right index matters as much as tuning `postgresql.conf`. The goal is not more indexes. The goal is the smallest set of indexes that matches real query patterns, while keeping write overhead and maintenance cost under control. Every index slows down writes and takes space.[1]

This guide covers PostgreSQL 14 through 18. Version differences are called out inline. The claim-by-claim audit is in [`docs/audit/claims.md`](./audit/claims.md).

**Table of Contents:**
- [A. Building Indexes Without Blocking Writes: CREATE INDEX CONCURRENTLY](#a-building-indexes-without-blocking-writes-create-index-concurrently)
- [B. Indexing Hot Subsets: Partial Indexes](#b-indexing-hot-subsets-partial-indexes)
- [C. Indexing Large, Correlated Data: BRIN Indexes](#c-indexing-large-correlated-data-brin-indexes)
- [D. Index-Only Scans and INCLUDE](#d-index-only-scans-and-include)
- [E. Multicolumn Indexes and Column Order](#e-multicolumn-indexes-and-column-order)
- [Version differences at a glance](#version-differences-at-a-glance)
- [References](#references)

See also [6 - Table and Index Health.md](./6%20-%20Table%20and%20Index%20Health.md) for checking whether indexes are used and whether they are bloated.

### A. Building Indexes Without Blocking Writes: CREATE INDEX CONCURRENTLY

**Why:** A plain `CREATE INDEX` takes a `SHARE` lock on the table. Reads continue, but `INSERT`, `UPDATE`, and `DELETE` wait until the build finishes. `CREATE INDEX CONCURRENTLY` takes a `SHARE UPDATE EXCLUSIVE` lock instead, so writes can continue.[2][6]

**Trade-offs and caveats (same in 14 to 18):**
- It is the usual choice for adding an index to a large, busy production table.[2]
- It takes longer. PostgreSQL scans the table twice and must wait for existing transactions that could use the index to finish.[2]
- It **cannot run inside a transaction block**.[2]
- Only one concurrent index build can run on a table at a time. It also conflicts with `VACUUM`, `ANALYZE`, and other `SHARE UPDATE EXCLUSIVE` operations on the same table.[2][6]
- If the build fails, it leaves an `INVALID` index behind. The planner ignores it, but it still adds update overhead. Drop it and retry, or use `REINDEX INDEX CONCURRENTLY`.[2]
- For unique indexes, uniqueness is enforced on new writes before the build finishes. Other transactions can see unique violations before the index is valid.[2]
- **Partitioned tables are not supported.** Build the index concurrently on each partition, then create the index on the parent with `ON ONLY` and attach the partition indexes. Creating the parent index is then a metadata-only step.[2]

**Sample SQL:**

```sql
CREATE INDEX CONCURRENTLY idx_users_email ON users (email);

-- Find indexes left invalid by a failed concurrent build
SELECT indexrelid::regclass AS index_name, indrelid::regclass AS table_name
FROM pg_index
WHERE NOT indisvalid;
```

Use `pg_stat_progress_create_index` to watch a long build.[7]

### B. Indexing Hot Subsets: Partial Indexes

**What:** A partial index has a `WHERE` clause. It stores entries only for rows that match the predicate.[3]

Soft deletes are a common use case:

```sql
CREATE INDEX idx_orders_active
ON orders (customer_id)
WHERE is_deleted = false;
```

**Why this helps:**
- The index is smaller because it skips rows outside the predicate.[3]
- Queries that target that subset search a smaller structure.[3]
- Rows that stay outside the predicate do not need entries in this index, so many writes avoid updating it.[3]

**Important caveat:** PostgreSQL can use a partial index only when it can prove the query's `WHERE` clause implies the index predicate. It understands simple cases such as `x < 1` implying `x < 2`. Otherwise the predicate must match part of the query's `WHERE` clause exactly.[3]

Matching happens at planning time. The manual's example: a prepared statement with `x < $1` cannot use an index with `WHERE x < 2`, because the plan must work for every parameter value.[3] A caveat applies: when PostgreSQL builds a custom plan for a prepared statement, it plans with the actual value, so a matching partial index can be used. Generic plans cannot use it. See `plan_cache_mode`.[8]

**Practical rule:** Use partial indexes for stable, high-value subsets, such as active rows, unprocessed jobs, or a narrow status range that most important queries hit. Write the query condition the same way as the index predicate.

### C. Indexing Large, Correlated Data: BRIN Indexes

**What:** A BRIN index stores a summary (such as minimum and maximum) for each range of table pages. It works best on very large tables where the column value follows the physical row order, such as an append-only timestamp.[4]

```sql
CREATE INDEX idx_logs_created_at ON logs USING BRIN (created_at);
```

**Why this helps:**
- BRIN indexes are very small compared with B-tree indexes, so scanning them adds little cost.[4]
- They let a bitmap scan skip page ranges that cannot contain matching rows.[4]

**Caveats:**
- BRIN is **lossy**. PostgreSQL rechecks every row in each matching page range.[4]
- It only helps when the physical order and the column order line up. Check `correlation` in `pg_stats` for the column. Values near `1` or `-1` are good. Values near `0` mean BRIN will skip little.[4][9]
- `pages_per_range` (default `128`) sets the trade-off. Smaller ranges give a larger but more precise index.[4]
- New page ranges are not summarized at insert time. They are summarized by `VACUUM` (manual or autovacuum), by the `autosummarize` index option (off by default), or by calling `brin_summarize_new_values()`. Until then, queries must scan those ranges.[4]

**Version notes:**
- PostgreSQL 16 and later allow HOT updates when only BRIN-indexed columns change.[10]
- PostgreSQL 17 and later can build BRIN indexes in parallel.[11]

**Practical rule:** Consider BRIN for huge append-heavy tables. Compare it with a B-tree on real queries before you choose.

### D. Index-Only Scans and INCLUDE

**What:** An index-only scan returns results from the index without reading the table (the heap) for each row, when the visibility map shows the page is all-visible.[5]

One way to make more queries eligible is to store extra columns with `INCLUDE`:

```sql
CREATE INDEX idx_users_username_cover_email
ON users (username)
INCLUDE (email);
```

**Why this helps:**
- The search key is still `username`, but PostgreSQL can return `email` from the index if the plan is an index-only scan.[2][5]
- This suits read-heavy lookups and API-style point queries.

**Caveats:**
- Index-only scans need the table's **visibility map**. If a page was changed since the last `VACUUM`, PostgreSQL still visits the heap for rows on that page. On tables with many updates, much of the benefit can disappear. `EXPLAIN (ANALYZE)` shows this as `Heap Fetches`.[5]
- Covering indexes are larger, so they cost more on writes and storage. Columns in `INCLUDE` cannot be used to search.[2][5]
- `INCLUDE` is supported by B-tree, GiST, and SP-GiST in PostgreSQL 14 to 18.[2]
- Index type support for index-only scans: B-tree always, GiST and SP-GiST for some operator classes, GIN never. BRIN is not used for index-only scans.[5]

**Practical rule:** Use `INCLUDE` when a few important, read-heavy queries keep fetching the same extra columns and the table is not churn-heavy. Keep autovacuum healthy so pages stay all-visible.

### E. Multicolumn Indexes and Column Order

- A multicolumn B-tree index is most efficient when queries constrain the leading (leftmost) columns.[12]
- **Version note:** PostgreSQL 18 adds B-tree **skip scan**. It can use an index on `(x, y)` for `WHERE y = 7700` when `x` has few distinct values. PostgreSQL 14 to 17 cannot do this efficiently, so a separate index on `y` may be needed there.[12][13]
- Put columns used with equality first. Put a range condition column after them.[12]

## Version differences at a glance

| Feature | 14 | 15 | 16 | 17 | 18 |
|---|---|---|---|---|---|
| `CREATE INDEX CONCURRENTLY` on partitioned tables | No | No | No | No | No |
| `INCLUDE` for B-tree, GiST, SP-GiST | Yes | Yes | Yes | Yes | Yes |
| HOT updates when only BRIN columns change | No | No | Yes | Yes | Yes |
| Parallel BRIN build | No | No | No | Yes | Yes |
| Parallel GIN build | No | No | No | No | Yes |
| B-tree skip scan | No | No | No | No | Yes |
| `pg_stat_user_indexes.last_idx_scan` | No | No | Yes | Yes | Yes |

## References

Links point to a specific major version. The PostgreSQL 14 pages say the same thing for each point unless a version note says otherwise.

- [1] [PostgreSQL 18: Indexes][1]
- [2] [PostgreSQL 18: CREATE INDEX][2] ([14][2a])
- [3] [PostgreSQL 18: Partial Indexes][3]
- [4] [PostgreSQL 18: BRIN Indexes][4]
- [5] [PostgreSQL 18: Index-Only Scans and Covering Indexes][5]
- [6] [PostgreSQL 18: Explicit Locking][6]
- [7] [PostgreSQL 18: Progress Reporting][7]
- [8] [PostgreSQL 18: PREPARE (custom and generic plans)][8]
- [9] [PostgreSQL 18: pg_stats view][9]
- [10] [PostgreSQL 16 release notes][10]
- [11] [PostgreSQL 17 release notes][11]
- [12] [PostgreSQL 18: Multicolumn Indexes][12]
- [13] [PostgreSQL 18 release notes][13]

[1]: https://www.postgresql.org/docs/18/indexes.html
[2]: https://www.postgresql.org/docs/18/sql-createindex.html
[2a]: https://www.postgresql.org/docs/14/sql-createindex.html
[3]: https://www.postgresql.org/docs/18/indexes-partial.html
[4]: https://www.postgresql.org/docs/18/brin.html
[5]: https://www.postgresql.org/docs/18/indexes-index-only-scans.html
[6]: https://www.postgresql.org/docs/18/explicit-locking.html
[7]: https://www.postgresql.org/docs/18/progress-reporting.html#CREATE-INDEX-PROGRESS-REPORTING
[8]: https://www.postgresql.org/docs/18/sql-prepare.html
[9]: https://www.postgresql.org/docs/18/view-pg-stats.html
[10]: https://www.postgresql.org/docs/16/release-16.html
[11]: https://www.postgresql.org/docs/17/release-17.html
[12]: https://www.postgresql.org/docs/18/indexes-multicolumn.html
[13]: https://www.postgresql.org/docs/18/release-18.html
