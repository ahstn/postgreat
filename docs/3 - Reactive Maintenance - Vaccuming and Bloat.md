## III. Reactive Maintenance: Vacuuming and Bloat Management

Even with good autovacuum settings, some tables need manual attention. High-churn tables, bulk deletes, and long periods of heavy writes can leave enough dead rows or index bloat to justify a targeted maintenance pass.[1][2]

This guide covers PostgreSQL 14 through 18. Version differences are called out inline. The claim-by-claim audit is in [`docs/audit/claims.md`](./audit/claims.md). For autovacuum settings, see [4 - Automated Maintenance](./4%20-%20Automated%20Maintenance%20-%20Autovacuum%20%26%20Background%20Worker.md).

### A. Identify Tables That Need Attention

- **MVCC background:** `UPDATE` writes a new row version and `DELETE` marks the old one as deleted. Once no transaction can still see an old version, `VACUUM` can mark its space as reusable.[1][2]
- **What blocks cleanup:** `VACUUM` cannot remove row versions that an open transaction, a replication slot, a prepared transaction, or a standby with `hot_standby_feedback` might still need. A long-running or idle-in-transaction session is a common cause of growing dead rows.[1]
- **First-pass triage:** Start with `pg_stat_user_tables`. `n_live_tup` and `n_dead_tup` are estimates. A high dead-row ratio is a reason to look closer. It is not a measurement of bloat on disk.[3]
- **Practical rule:** Look at dead rows, table size, last vacuum and analyze times, and real symptoms such as slower scans or steady table growth together.

**Triage query (runs on 14 to 18):**

```sql
SELECT
    schemaname,
    relname AS table_name,
    n_live_tup,
    n_dead_tup,
    round(n_dead_tup::numeric / NULLIF(n_live_tup, 0), 4) AS dead_tup_ratio,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
    last_vacuum,
    last_autovacuum,
    last_autoanalyze
FROM pg_stat_user_tables
WHERE n_dead_tup > 1000
ORDER BY n_dead_tup DESC, pg_total_relation_size(relid) DESC
LIMIT 20;
```

`pg_total_relation_size()` counts the table, its indexes, and its TOAST data. Using `relid` avoids name-quoting problems.[3][9]

**Find what is holding back cleanup:**

```sql
-- Oldest open transactions
SELECT pid, usename, state, xact_start, now() - xact_start AS xact_age, left(query, 80) AS query
FROM pg_stat_activity
WHERE xact_start IS NOT NULL
ORDER BY xact_start
LIMIT 10;

-- Replication slots that keep old rows (xmin) or old WAL
SELECT slot_name, slot_type, active, xmin, catalog_xmin, restart_lsn
FROM pg_replication_slots;
```

### B. Choose the Right Manual Intervention

**1. Plain `VACUUM`**

- Use this first when autovacuum is behind. It runs alongside normal reads and writes because it does not take an exclusive lock.[2]
- In most cases it does **not** give space back to the operating system. It keeps the space for reuse in the same table.[2]
- It can cut off empty pages at the end of the table. That step needs a brief `ACCESS EXCLUSIVE` lock. You can turn it off per command (`VACUUM (TRUNCATE false)`) or per table (`vacuum_truncate` storage option). **Version note:** PostgreSQL 18 adds a server-wide `vacuum_truncate` setting.[2][10]
- `VACUUM (ANALYZE)` also refreshes planner statistics, which is useful after heavy churn.[2]
- `VACUUM (VERBOSE)` reports what it removed and what it could not remove yet.[2]

**2. `VACUUM FULL`**

- Use this only when you must physically shrink a heavily bloated table.[1][2]
- It rewrites the whole table and takes an `ACCESS EXCLUSIVE` lock. All reads and writes on the table wait until it finishes.[2][6]
- It needs extra disk space for the new copy of the table and its indexes until the old copy is released.[2]
- Progress is shown in `pg_stat_progress_cluster`, not `pg_stat_progress_vacuum`.[4]
- Practical rule: keep it for cases where getting disk space back is worth a maintenance window. It is not a routine substitute for healthy autovacuum. Third-party tools such as `pg_repack` can rebuild a table with shorter locks, but they are extensions you must install and test.

**3. `REINDEX` and `REINDEX CONCURRENTLY`**

- Use these when the problem is **index** bloat or a corrupt or invalid index, not table bloat.[7]
- **Plain `REINDEX` locking:** It locks out writes, but not reads, on the parent table. It takes an `ACCESS EXCLUSIVE` lock on the index being rebuilt, which blocks any query that tries to use that index.[7] The PostgreSQL 18 manual adds that the planner tries to lock every index of a table while planning, so `REINDEX` blocks almost every query on the table except some cached prepared plans.[7] The PostgreSQL 14 manual does not say this, but the behavior is the same: in a test on 14.24 and 18.6, a primary-key lookup waited while a different index on the table was being rebuilt. In practice, treat plain `REINDEX` as blocking the table.
- **`REINDEX CONCURRENTLY`:** Rebuilds without blocking writes. It takes longer and has limits:[7]
  - It cannot run inside a transaction block.
  - It cannot rebuild indexes on system catalogs, and `REINDEX SYSTEM` cannot use it.
  - It cannot rebuild exclusion-constraint indexes. They are skipped at table level and raise an error when named directly.
  - If it fails, it leaves an `INVALID` index. A name ending in `_ccnew` is the half-built new index: drop it and retry. A name ending in `_ccold` is the old index: drop it, because the rebuild worked.
- **Partitioned tables:** `REINDEX TABLE` and `REINDEX INDEX` on a partitioned table or index process each partition in its own transaction. This works in 14 to 18 and cannot run inside a transaction block.[7][7a]
- **Version note, privileges:** From PostgreSQL 17, `REINDEX`, `VACUUM`, and `ANALYZE` can be granted with the `MAINTAIN` privilege or the `pg_maintain` role. On 14 to 16, `REINDEX` needs table ownership (or superuser). `VACUUM` and `ANALYZE` also allow the database owner.[7][7a][11]
- Practical rule: do not use `REINDEX` as a generic substitute for `VACUUM`. Use it when index size, performance, or corruption points at the indexes.

### C. Measure More Precisely When Needed

- **`pgstattuple`:** When statistics suggest a problem and you need a real measurement, the `pgstattuple` extension reports dead tuple and free space percentages.[5]
  - `pgstattuple()` reads the whole relation. It takes only a read lock, but on a large table it is slow and causes a lot of I/O.[5]
  - `pgstattuple_approx()` is faster and uses the visibility map to skip pages.[5]
  - `pgstatindex()` reports B-tree index density (`avg_leaf_density`) and fragmentation.[5]
  - By default only superusers and members of `pg_stat_scan_tables` can run these functions.[5]
- **`pg_stat_progress_vacuum`:** Shows the phase and progress of a running `VACUUM` or autovacuum.[4] The columns differ by version:

| Version | Dead row columns | Other additions |
|---|---|---|
| 14 to 16 | `max_dead_tuples`, `num_dead_tuples` | |
| 17 | `max_dead_tuple_bytes`, `dead_tuple_bytes`, `num_dead_item_ids` | `indexes_total`, `indexes_processed` |
| 18 | same as 17 | `delay_time` (needs `track_cost_delay_timing`) |

```sql
-- Works on 14 to 18 (version-specific columns left out)
SELECT p.pid, p.relid::regclass AS table_name, p.phase,
       p.heap_blks_total, p.heap_blks_scanned, p.heap_blks_vacuumed,
       p.index_vacuum_count
FROM pg_stat_progress_vacuum p;

-- Example: measure one table precisely
CREATE EXTENSION IF NOT EXISTS pgstattuple;
SELECT * FROM pgstattuple_approx('public.my_table'::regclass);
```

**Practical rule:** Start with cheap statistics views. Use `pgstattuple` only on the tables where precision matters.

### D. Locks Taken by Maintenance Commands

The same in PostgreSQL 14 to 18.[6][7][2][12]

| Command | Table lock | What waits |
|---|---|---|
| `VACUUM`, `ANALYZE` | `SHARE UPDATE EXCLUSIVE` | Reads and writes continue. Other vacuums, schema changes, and concurrent index builds on the same table wait. The optional end-of-table truncation in `VACUUM` briefly takes `ACCESS EXCLUSIVE`. |
| `CREATE INDEX` | `SHARE` | Writes wait. Reads continue. |
| `CREATE INDEX CONCURRENTLY`, `REINDEX CONCURRENTLY` | `SHARE UPDATE EXCLUSIVE` | Reads and writes continue. Waits for older transactions at several steps. |
| `REINDEX` | `SHARE` on the table, `ACCESS EXCLUSIVE` on the index | Writes wait. Queries that plan against the table usually wait too, because planning locks every index. |
| `VACUUM FULL`, `CLUSTER` | `ACCESS EXCLUSIVE` | Everything waits, including `SELECT`. |

`CLUSTER` rewrites the table in the order of one index. Like `VACUUM FULL`, it needs extra disk space and blocks all access. The order is not kept for later writes.[12]

### E. A Decision Framework for Manual Maintenance

1. **Monitor:** Run the triage query from section A on a schedule and keep the results, so you can see trends.
2. **Pick targets:** Look for large tables whose dead-row ratio stays high (for example above 0.2) across several runs. The threshold is a starting point, not a documented rule.
3. **Investigate:** Is `last_autovacuum` recent? If a vacuum is running, check `pg_stat_progress_vacuum`. If vacuums run but dead rows stay, look for old transactions, prepared transactions, or replication slots holding cleanup back (section A).
4. **Act (light):** Run `VACUUM (VERBOSE, ANALYZE) my_table;`. It does not block reads or writes. Fix the autovacuum settings for that table so this does not repeat.
5. **Measure:** If the table or its indexes still look bloated, measure with `pgstattuple_approx()` or `pgstatindex()`.
6. **Act (heavy):** Use `REINDEX INDEX CONCURRENTLY` for index bloat. Use `VACUUM FULL` only in a maintenance window, as a last resort, when you need disk space back.
## References

Links point to a specific major version. PostgreSQL 14 pages say the same thing unless a version note says otherwise.

- [1] [PostgreSQL 18: Routine Vacuuming][1]
- [2] [PostgreSQL 18: VACUUM][2]
- [3] [PostgreSQL 18: Cumulative Statistics System][3]
- [4] [PostgreSQL 18: Progress Reporting][4]
- [5] [PostgreSQL 18: pgstattuple][5]
- [6] [PostgreSQL 18: Explicit Locking][6]
- [7] [PostgreSQL 18: REINDEX][7]
- [7a] [PostgreSQL 14: REINDEX][7a]
- [9] [PostgreSQL 18: Database Object Management Functions][9]
- [10] [PostgreSQL 18: Vacuuming configuration (`vacuum_truncate`)][10]
- [11] [PostgreSQL 17 release notes][11]
- [12] [PostgreSQL 18: CLUSTER][12]

Further reading (community, not version-pinned):

- [Fastware: Handling dead tuples in PostgreSQL][f1]

[1]: https://www.postgresql.org/docs/18/routine-vacuuming.html
[2]: https://www.postgresql.org/docs/18/sql-vacuum.html
[3]: https://www.postgresql.org/docs/18/monitoring-stats.html
[4]: https://www.postgresql.org/docs/18/progress-reporting.html
[5]: https://www.postgresql.org/docs/18/pgstattuple.html
[6]: https://www.postgresql.org/docs/18/explicit-locking.html
[7]: https://www.postgresql.org/docs/18/sql-reindex.html
[7a]: https://www.postgresql.org/docs/14/sql-reindex.html
[9]: https://www.postgresql.org/docs/18/functions-admin.html#FUNCTIONS-ADMIN-DBOBJECT
[10]: https://www.postgresql.org/docs/18/runtime-config-vacuum.html#GUC-VACUUM-TRUNCATE
[11]: https://www.postgresql.org/docs/17/release-17.html
[12]: https://www.postgresql.org/docs/18/sql-cluster.html
[f1]: https://www.postgresql.fastware.com/pzone/2025-03-improving-postgresql-efficiency-by-handling-dead-tuples
