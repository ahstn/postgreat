## II. Proactive Maintenance: Indexing Strategies

**Overview:**

Creating the right index is as important as tuning `postgresql.conf`. The goal is not to add more indexes, but to add the smallest set of indexes that matches real query patterns while keeping write overhead and maintenance cost under control.[1]

**Table of Contents:**
- [A. High-Availability Indexing: CREATE INDEX CONCURRENTLY](#a-high-availability-indexing-create-index-concurrently)
- [B. Efficient Indexing for Hot Subsets: Partial Indexes](#b-efficient-indexing-for-hot-subsets-partial-indexes)
- [C. Indexing for Large, Correlated Data: BRIN Indexes](#c-indexing-for-large-correlated-data-brin-indexes)
- [D. Optimizing for Read Performance: Index-Only Scans with INCLUDE](#d-optimizing-for-read-performance-index-only-scans-with-include)
- [References](#references)

See also [6 - Table and Index Health.md](./6 - Table and Index Health.md) for ongoing validation of index usefulness and bloat.

### A. High-Availability Indexing: CREATE INDEX CONCURRENTLY

**Purpose:** A regular `CREATE INDEX` blocks writes on the target table while the index is being built. `CREATE INDEX CONCURRENTLY` allows `INSERT`, `UPDATE`, and `DELETE` to continue while the build runs.[2]

**Trade-offs and caveats:**
- It is the default choice for adding indexes to large, live production tables where write availability matters.[2]
- It takes longer than a standard build because PostgreSQL performs multiple scans of the table and waits for conflicting transactions.[2]
- It **cannot run inside a transaction block**.[2]
- PostgreSQL allows **only one concurrent index build per table at a time**.[2]
- If the build fails, PostgreSQL can leave behind an `INVALID` index. That index is ignored by the planner but still incurs maintenance overhead until dropped or rebuilt.[2]
- For unique indexes, uniqueness checking can begin before the index is marked valid, so failures can surface in other transactions before the command itself finishes.[2]

**Sample SQL:**

```sql
CREATE INDEX CONCURRENTLY idx_users_email ON users(email);
```

### B. Efficient Indexing for Hot Subsets: Partial Indexes

**Purpose:** A partial index is an index with a `WHERE` clause. It stores entries only for rows that satisfy the predicate.[3]

Soft deletes are a common use case:

```sql
CREATE INDEX idx_orders_active
ON orders (customer_id)
WHERE is_deleted = false;
```

**Why this helps:**
- The index is smaller because it excludes rows outside the predicate.[3]
- Queries that target the same subset of rows can become faster because PostgreSQL searches a smaller structure.[3]
- Writes can be cheaper because rows that remain outside the predicate usually avoid maintenance for **this index**.

**Important caveat:** PostgreSQL can use a partial index only when the query's `WHERE` clause **implies the index predicate**. In practice that usually means the predicate must match very closely. Parameterized predicates often prevent the planner from proving that implication.[3]

**Practical rule:** Partial indexes are best when you have a stable, high-value subset such as active rows, unprocessed jobs, or a narrow status range that dominates important queries.

### C. Indexing for Large, Correlated Data: BRIN Indexes

**Purpose:** A BRIN index is designed for very large tables where the indexed value is naturally correlated with the physical order of rows on disk.[4]

The classic use case is append-heavy time-series or event data:

```sql
CREATE INDEX idx_logs_created_at ON logs USING BRIN (created_at);
```

**Why this helps:**
- BRIN indexes are extremely small compared with B-tree indexes.[4]
- They are fast to build and inexpensive to maintain.[4]
- They can skip large regions of a table during bitmap scans when correlation is strong.[4]

**Caveats:**
- BRIN indexes are **lossy**. PostgreSQL must recheck candidate tuples from the heap.[4]
- They work best when heap order and column order are aligned. If correlation is weak, performance can drift toward sequential-scan behavior.
- `pages_per_range` matters. Smaller ranges make the index larger but more precise; larger ranges make it smaller but less selective.[4]

**Practical rule:** Choose BRIN for huge append-heavy tables first, then compare it against B-tree on representative queries before standardizing on one index type.

### D. Optimizing for Read Performance: Index-Only Scans with INCLUDE

**Purpose:** An index-only scan can answer a query from the index alone, avoiding most heap reads.[5]

One way to make more queries eligible is to store extra columns with `INCLUDE`:

```sql
CREATE INDEX idx_users_username_cover_email
ON users(username)
INCLUDE (email);
```

**Why this helps:**
- The search key stays on `username`, but PostgreSQL can return `email` from the index if the plan qualifies for an index-only scan.[2][5]
- This is often useful for read-heavy lookup patterns and API-style point queries.

**Important caveats:**
- An index-only scan still depends on the table's **visibility map**. On frequently updated tables, PostgreSQL may still need heap visits, which reduces the benefit substantially.[5]
- Covering indexes are larger than non-covering indexes, so they increase write and storage overhead.[2][5]
- Included columns are supported only by B-tree, GiST, and SP-GiST indexes.[2]

**Practical rule:** Use `INCLUDE` when a small number of read-heavy queries repeatedly fetch the same extra columns and the underlying table is not churn-heavy.

## References

- [PostgreSQL 18 Documentation: CREATE INDEX][2]
- [PostgreSQL 18 Documentation: Partial Indexes][3]
- [PostgreSQL 18 Documentation: BRIN Indexes][4]
- [PostgreSQL 18 Documentation: Index-Only Scans and Covering Indexes][5]
- [PostgreSQL 18 Documentation: Indexes][1]

[1]: https://www.postgresql.org/docs/current/indexes.html
[2]: https://www.postgresql.org/docs/current/sql-createindex.html
[3]: https://www.postgresql.org/docs/current/indexes-partial.html
[4]: https://www.postgresql.org/docs/current/brin.html
[5]: https://www.postgresql.org/docs/current/indexes-index-only-scans.html
