## II. Proactive Maintenance: Indexing Strategies

Creating the _right_ type of index is as important as configuration tuning. Using advanced indexing features avoids production-locking issues and optimizes for specific query patterns.

### A. High-Availability Indexing: CREATE INDEX CONCURRENTLY

**Purpose:** A standard `CREATE INDEX` command takes a `SHARE` lock on the table, which blocks all `INSERT`, `UPDATE`, and `DELETE` operations for the duration of the build. `CREATE INDEX CONCURRENTLY` avoids this blocking lock, allowing writes to continue.31

**Trade-offs:** This high-availability feature comes at a cost:
- **Pro:** Essential for adding indexes to live, high-traffic production tables.31
- **Con 1:** The build process takes significantly longer (e.g., 2-3x) than a standard build because it requires multiple scans of the table.31
- **Con 2:** It consumes more CPU and I/O resources over that longer period.[32]
- **Con 4:** If it fails (e.g., due to a unique constraint violation on the second pass), it may leave behind an "invalid" index. This invalid index must be manually dropped (`DROP INDEX...;`) before the `CREATE INDEX CONCURRENTLY` command can be retried.[34]

**Sample SQL:**

```sql
-- Allows writes to continue on the 'users' table during index creation
CREATE INDEX CONCURRENTLY idx_users_email ON users(email);
```


### B. Efficient Indexing for "Soft-Deletes": Partial Indexes

**Purpose:** A partial index is an index with a `WHERE` clause. It only indexes the subset of rows that match the predicate.35

The **canonical use case is for tables with a "soft-delete" boolean flag**, such as `is_deleted = false`.36 In most applications, 99% of queries are only interested in active records (`... WHERE is_deleted = false`).

**Rationale & Benefits:**

1. **Smaller Size:** The index is dramatically smaller because it omits all "deleted" rows.36
2. **Faster Reads:** Queries that use the _same_ `WHERE` clause are faster.
3. **Faster Writes:** This is the non-obvious benefit. An index must be updated on _every_ write to the table.37 With a full index, updating a "deleted" row (e.g., `UPDATE users SET last_login = NOW() WHERE id = 123`) would still require an index update. With a partial index `... WHERE is_deleted = false`, updates to rows that are _already_ deleted _do not touch the index at all_, as they do not match the predicate. This significantly reduces write amplification in high-churn tables.

**Sample SQL:**

```sql
-- Creates an index only on active, non-deleted orders
CREATE INDEX idx_orders_active ON orders (customer_id) WHERE is_deleted = false;
```

### C. Indexing for Large, Correlated Data: BRIN Indexes

**Purpose:** A BRIN (Block Range INdex) is a lightweight index designed for _very_ large tables where the indexed column's value has a _strong natural correlation_ with its physical storage location on disk.[38]

The perfect use case is a time-series or log table (e.g., IoT data) with a `created_at` column. New rows are inserted in chronological order and are _appended_ to the end of the table. This creates a natural physical correlation.[38]

**Trade-offs:**
- **Pro:** BRIN indexes are _dramatically_ smaller (often 1000x smaller) than a standard B-Tree index.[38]
- **Pro:** They are very fast to build and have a negligible impact on write speed.38
- **Con:** They are _completely useless_ if the data is not physically correlated. A BRIN index on a `user_id` column in a time-series table will perform no better than a sequential scan.38

- **Sample SQL:**

```sql
-- Creates a tiny, efficient BRIN index on a time-series log table
CREATE INDEX idx_logs_created_at ON logs USING BRIN (created_at);
```


### D. Optimizing for Read Performance: Index-Only Scans with INCLUDE

**Purpose:** An index-only scan occurs when PostgreSQL can answer a query _entirely_ from an index, without needing to fetch the corresponding row data from the main table (a "heap fetch"). This is a major I/O optimization.

**Use Case:** Consider the query: `SELECT email FROM users WHERE username = 'admin';`. A standard index `ON users(username)` would allow PostgreSQL to _find_ the row, but it would still have to visit the table file to retrieve the `email`.

The `INCLUDE` clause stores extra, non-indexed columns in the index's leaf nodes.

**Trade-off:** The index becomes larger, but read queries that are "covered" by the index become significantly faster.[41]

**Sample SQL:**

```sql
-- The index is built on 'username' for lookups
-- The 'email' column is just stored alongside it for index-only scans
CREATE INDEX idx_users_username_cover_email ON users(username) INCLUDE (email);
```
