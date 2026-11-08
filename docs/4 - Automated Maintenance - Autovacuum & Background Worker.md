## IV. Automated Maintenance: Tuning Background Workers and Autovacuum

Autovacuum exists so that manual vacuuming is the exception. On busy OLTP systems, getting it right matters more than occasional manual cleanup. The right settings depend on write rate, table sizes, storage throughput, and PostgreSQL version.[1][2]

This guide covers PostgreSQL 14 through 18. Version differences are called out inline and summarized in [Version differences at a glance](#version-differences-at-a-glance). The claim-by-claim audit is in [`docs/audit/claims.md`](./audit/claims.md).

### A. Worker Configuration

- **`autovacuum_max_workers`:**
  - **Purpose:** Maximum number of autovacuum worker processes running at one time, not counting the launcher. Default `3`.[1]
  - **Version note:** On PostgreSQL 14 to 17 it can only be changed with a server restart. On PostgreSQL 18 it can be changed with a configuration reload, but it is capped by `autovacuum_worker_slots`.[1][1a]
  - **Guidance:** More workers help when many tables need vacuuming at once. Each worker can use up to `autovacuum_work_mem` (or `maintenance_work_mem`), and the shared cost limit is split among them, so adding workers alone does not make vacuuming faster.[1][2]
  - **Practical rule:** Raise it as a measured step when tables wait for a free worker. It is not a universal "set it to 5" rule.

- **`autovacuum_worker_slots` (PostgreSQL 18 only):**
  - **Purpose:** Number of backend slots reserved for autovacuum workers. Default `16`. Server restart required.[1]
  - **Guidance:** Setting `autovacuum_max_workers` above this value has no effect. Keep enough slots so you can raise `autovacuum_max_workers` later with only a reload.[1]

- **`autovacuum_naptime`:**
  - **Purpose:** Minimum delay between autovacuum runs on any one database. Default `1min`. The launcher tries to start one worker in each database every `autovacuum_naptime`, so with N databases a worker starts about every `autovacuum_naptime / N`.[1][2]
  - **Guidance:** Lower values make autovacuum react faster but add background activity.
  - **Practical rule:** Shorten it only after you confirm that tables are waiting for autovacuum to notice them, not waiting for a free worker or I/O.

Autovacuum workers do not count toward `max_connections`.[2]

### B. Thresholds: When Autovacuum Starts

All supported versions have two vacuum triggers: one for dead rows (updates and deletes) and one for inserts. Autovacuum also runs `ANALYZE` on its own threshold.[2] Here `reltuples`, `relpages`, and `relallfrozen` come from `pg_class`.

**Dead-row trigger:**

| Version | Formula |
|---|---|
| 14 to 17 | `autovacuum_vacuum_threshold + autovacuum_vacuum_scale_factor × reltuples` |
| 18 | `min(autovacuum_vacuum_max_threshold, autovacuum_vacuum_threshold + autovacuum_vacuum_scale_factor × reltuples)` |

Defaults: threshold `50`, scale factor `0.2`. `autovacuum_vacuum_max_threshold` is new in 18 and defaults to `100000000` (100 million).[2][2a]

**Insert trigger:**

| Version | Formula |
|---|---|
| 14 to 17 | `autovacuum_vacuum_insert_threshold + autovacuum_vacuum_insert_scale_factor × reltuples` |
| 18 | `autovacuum_vacuum_insert_threshold + autovacuum_vacuum_insert_scale_factor × reltuples × (1 - relallfrozen / relpages)` |

Defaults: insert threshold `1000`, insert scale factor `0.2`. In 18, the frozen part of the table no longer counts, so large append-only tables are vacuumed more often.[2][2a]

**Analyze trigger (all versions):** `autovacuum_analyze_threshold + autovacuum_analyze_scale_factor × reltuples`, compared with rows inserted, updated, or deleted since the last `ANALYZE`. Defaults `50` and `0.1`.[2]

**What this means in practice:**
- On PostgreSQL 14 to 17, the old rule of thumb still holds: with defaults, a 1 billion row table waits for about 200 million dead rows before autovacuum starts. On PostgreSQL 18, the default cap starts it at 100 million.[2][2a]
- Large, busy tables still often need per-table settings. The fix is usually lower scale factors or explicit thresholds, not `autovacuum_vacuum_scale_factor = 0`.
- Autovacuum does not `ANALYZE` partitioned tables themselves, only their partitions. Run `ANALYZE` on the parent after loading data or when the data distribution changes.[2]
- Autovacuum cannot process temporary tables.[2]

**Example per-table settings:**

```sql
ALTER TABLE my_large_table SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 10000,
  autovacuum_analyze_scale_factor = 0.005,
  autovacuum_analyze_threshold = 5000
);

-- Large insert-mostly table
ALTER TABLE my_event_log SET (
  autovacuum_vacuum_insert_scale_factor = 0.01,
  autovacuum_vacuum_insert_threshold = 100000
);
```

### C. Cost-Based Throttling and Resource Budgets

- **`autovacuum_vacuum_cost_limit` and `autovacuum_vacuum_cost_delay`:**
  - **Purpose:** Throttle autovacuum so it does not take over the I/O system.[1]
  - **Defaults:** `autovacuum_vacuum_cost_limit = -1`, which means use `vacuum_cost_limit` (`200`). `autovacuum_vacuum_cost_delay = 2ms` in all supported versions. Setting it to `-1` would use `vacuum_cost_delay`.[1]
  - **How the limit is shared:** The cost limit is split proportionally among running workers, so the total never goes above the configured value. Setting it to `2000` raises the total budget tenfold. It does not give each worker ten times more. Workers on tables with their own per-table cost settings are left out of this balancing.[1][2]
  - **Practical rule:** If autovacuum falls behind, raise the cost limit in steps and watch I/O latency. Lowering the delay also speeds it up. Per-table cost settings are useful for a few very large tables.

- **`autovacuum_work_mem`:**
  - **Purpose:** Memory for each autovacuum worker. Default `-1`, which means use `maintenance_work_mem`.[3]
  - **Guidance:** The total can reach `autovacuum_max_workers` × this value. Set it explicitly when `maintenance_work_mem` is large.[3]
  - **Version note:** On PostgreSQL 14 to 16, values above 1 GB have no effect on how many dead rows a worker can track. PostgreSQL 17 removed that limit and uses much less memory for the same work.[3a][4]
  - **Practical rule:** Size it from table sizes and the worker count, not from a fixed rule such as `512MB`.

- **`vacuum_buffer_usage_limit` (PostgreSQL 16 and later):** Limits how much of `shared_buffers` one `VACUUM` or `ANALYZE` can use. Default `256kB` in 16, `2MB` in 17 and later. Higher values can make vacuum faster at the cost of pushing other data out of cache.[3][3b]

### D. Freezing and Transaction ID Wraparound

Autovacuum also freezes old rows so transaction IDs can be reused. This work runs even when autovacuum is turned off.[2]

- `autovacuum_freeze_max_age` (default `200000000`) forces a vacuum of a table whose oldest unfrozen transaction ID is older than this. `autovacuum_multixact_freeze_max_age` (default `400000000`) does the same for multixact IDs.[1]
- `vacuum_failsafe_age` (default `1600000000`, available in 14 and later) makes `VACUUM` skip index cleanup and cost delays to finish faster when wraparound is close.[1]
- Long-open transactions, old prepared transactions, and stale replication slots stop freezing from making progress.[2]

```sql
-- Databases closest to wraparound
SELECT datname, age(datfrozenxid) AS xid_age, mxid_age(datminmxid) AS mxid_age
FROM pg_database
ORDER BY xid_age DESC;

-- Tables closest to a forced (anti-wraparound) vacuum
SELECT c.oid::regclass AS table_name,
       greatest(age(c.relfrozenxid), age(t.relfrozenxid)) AS xid_age
FROM pg_class c
LEFT JOIN pg_class t ON c.reltoastrelid = t.oid
WHERE c.relkind IN ('r', 'm')
ORDER BY xid_age DESC
LIMIT 20;
```

### E. Monitoring Whether Autovacuum Is Working

- **`pg_stat_user_tables`:** Shows estimated live and dead rows, `n_ins_since_vacuum`, `n_mod_since_analyze`, and the last vacuum and analyze times.[5]
  - `n_dead_tup` is an estimate, not a measure of bloat on disk.[5]
  - **Version note:** PostgreSQL 16 adds `last_seq_scan` and `last_idx_scan`. PostgreSQL 18 adds `total_vacuum_time`, `total_autovacuum_time`, `total_analyze_time`, and `total_autoanalyze_time`.[5]
- **`pg_stat_progress_vacuum`:** Shows the phase and progress of each running vacuum. Column names for dead row tracking changed in PostgreSQL 17. See [3 - Reactive Maintenance](./3%20-%20Reactive%20Maintenance%20-%20Vaccuming%20and%20Bloat.md#c-measure-more-precisely-when-needed).[6]
- **`log_autovacuum_min_duration`:** Logs autovacuum runs that take at least this long. **Version note:** the default is `-1` (off) in PostgreSQL 14 and `10min` in 15 and later. Lower it (for example to `1s` or `0`) while you investigate.[7][7a]
- **PostgreSQL 18:** `track_cost_delay_timing` (default `off`) records time spent sleeping in cost delays. It shows in `pg_stat_progress_vacuum.delay_time` and in `VACUUM (VERBOSE)` and autovacuum log output.[6][8]

```sql
-- Tables with the most dead rows and when autovacuum last ran (14 to 18)
SELECT relid::regclass AS table_name, n_live_tup, n_dead_tup,
       n_ins_since_vacuum, last_autovacuum, last_autoanalyze, autovacuum_count
FROM pg_stat_user_tables
ORDER BY n_dead_tup DESC
LIMIT 20;
```

**Practical rule:** Watch trends, not single snapshots. The goal is not zero dead rows. The goal is to keep dead rows and table growth under control without hurting foreground queries.

## Version differences at a glance

| Item | 14 | 15 | 16 | 17 | 18 |
|---|---|---|---|---|---|
| `autovacuum_max_workers` change needs | restart | restart | restart | restart | reload (capped by `autovacuum_worker_slots`) |
| `autovacuum_vacuum_max_threshold` | n/a | n/a | n/a | n/a | 100,000,000 |
| Insert trigger ignores frozen pages | No | No | No | No | Yes |
| Dead row memory cap per vacuum | 1 GB | 1 GB | 1 GB | none | none |
| `vacuum_buffer_usage_limit` | n/a | n/a | 256kB | 2MB | 2MB |
| `log_autovacuum_min_duration` default | -1 | 10min | 10min | 10min | 10min |
| `pg_stat_progress_vacuum` dead row columns | tuples | tuples | tuples | bytes and item IDs | bytes and item IDs |
| `track_cost_delay_timing` | n/a | n/a | n/a | n/a | off |

Defaults were checked with `SHOW` on PostgreSQL 14.24, 15.19, 16.15, 17.11, and 18.6.

## References

Links point to a specific major version.

- [1] [PostgreSQL 18: Vacuuming configuration][1]
- [1a] [PostgreSQL 14: Automatic Vacuuming configuration][1a]
- [2] [PostgreSQL 18: Routine Vacuuming][2]
- [2a] [PostgreSQL 14: Routine Vacuuming][2a]
- [3] [PostgreSQL 18: Resource Consumption][3]
- [3a] [PostgreSQL 14: Resource Consumption][3a]
- [3b] [PostgreSQL 16: Resource Consumption][3b]
- [4] [PostgreSQL 17 release notes][4]
- [5] [PostgreSQL 18: Cumulative Statistics System][5]
- [6] [PostgreSQL 18: Progress Reporting][6]
- [7] [PostgreSQL 18: Error Reporting and Logging][7]
- [7a] [PostgreSQL 14: Error Reporting and Logging][7a]
- [8] [PostgreSQL 18: Run-time Statistics][8]

Further reading (community, not version-pinned):

- [Percona: Tuning autovacuum in PostgreSQL][f1]
- [pganalyze: How the VACUUM cost model works][f2]
- [EDB: Autovacuum tuning basics][f3]
- [AWS: Understanding autovacuum in Amazon RDS for PostgreSQL][f4]
- [Azure: Autovacuum tuning for PostgreSQL Flexible Server][f5]
- [Crunchy Data: Tuning Postgres for high write loads][f6]

[1]: https://www.postgresql.org/docs/18/runtime-config-vacuum.html
[1a]: https://www.postgresql.org/docs/14/runtime-config-autovacuum.html
[2]: https://www.postgresql.org/docs/18/routine-vacuuming.html
[2a]: https://www.postgresql.org/docs/14/routine-vacuuming.html
[3]: https://www.postgresql.org/docs/18/runtime-config-resource.html
[3a]: https://www.postgresql.org/docs/14/runtime-config-resource.html
[3b]: https://www.postgresql.org/docs/16/runtime-config-resource.html
[4]: https://www.postgresql.org/docs/17/release-17.html
[5]: https://www.postgresql.org/docs/18/monitoring-stats.html
[6]: https://www.postgresql.org/docs/18/progress-reporting.html
[7]: https://www.postgresql.org/docs/18/runtime-config-logging.html
[7a]: https://www.postgresql.org/docs/14/runtime-config-logging.html
[8]: https://www.postgresql.org/docs/18/runtime-config-statistics.html
[f1]: https://www.percona.com/blog/tuning-autovacuum-in-postgresql-and-autovacuum-internals/
[f2]: https://pganalyze.com/docs/vacuum-advisor/how-does-the-vacuum-cost-model-work
[f3]: https://www.enterprisedb.com/blog/autovacuum-tuning-basics
[f4]: https://aws.amazon.com/blogs/database/understanding-autovacuum-in-amazon-rds-for-postgresql-environments/
[f5]: https://learn.microsoft.com/en-us/azure/postgresql/troubleshoot/how-to-autovacuum-tuning
[f6]: https://www.crunchydata.com/blog/tuning-your-postgres-database-for-high-write-loads
