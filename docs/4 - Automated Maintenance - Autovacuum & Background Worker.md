## IV. Automated Maintenance: Tuning Background Workers and Autovacuum

Autovacuum exists to make manual vacuuming the exception rather than the norm. On busy OLTP systems, getting it right is often more important than occasional manual cleanup, but the right settings depend on write rate, table sizes, storage throughput, and PostgreSQL version.[1][2][3][7]

### A. Worker Configuration

- **`autovacuum_worker_slots`:**
  - **Purpose:** Reserves backend slots for autovacuum workers.[1]
  - **Current PostgreSQL 18 caveat:** This setting now limits how many autovacuum workers can actually run. If `autovacuum_max_workers` is set higher than the available worker slots, the slots still cap the effective concurrency.[1]

- **`autovacuum_max_workers`:**
  - **Purpose:** Maximum number of autovacuum worker processes that may run at one time.[1]
  - **Current guidance:** The default is `3`, but raising it is workload-dependent. Increase it only if you have enough I/O, CPU, memory, and worker slots to support more concurrent vacuum activity.[1][2][3][7]
  - **Practical rule:** Treat higher worker counts as a measured scaling step, not as a universal “set it to 5” rule.

- **`autovacuum_naptime`:**
  - **Purpose:** Minimum delay between autovacuum rounds on a given database.[1]
  - **Current guidance:** Lower values can make autovacuum more responsive, especially when many hot tables compete for attention, but they also increase background activity.[1][8][9]
  - **Practical rule:** If you have many databases or many hot tables, shorten this only after confirming that worker capacity and I/O are the real bottlenecks.

### B. Thresholds: When Autovacuum Starts

Autovacuum does not use a single trigger anymore. Current PostgreSQL distinguishes between update/delete-driven vacuuming and insert-driven vacuuming.[1][2]

- **Update/delete trigger:** vacuuming is driven by `autovacuum_vacuum_threshold + autovacuum_vacuum_scale_factor * table_row_count`, but current PostgreSQL can also cap that trigger with `autovacuum_vacuum_max_threshold`.[1]
- **Insert trigger:** insert-heavy tables can be vacuumed based on separate `autovacuum_vacuum_insert_threshold` and `autovacuum_vacuum_insert_scale_factor` settings.[1]

**What this means in practice:**
- Older rules of thumb like “a 1 billion row table waits for 200 million dead rows” are no longer correct without accounting for the current cap and the insert-specific triggers.
- Large hot tables still often need per-table tuning, but the right fix is not always `autovacuum_vacuum_scale_factor = 0`.
- Current best practice is to tune large churn-heavy tables individually, often by reducing scale factors, setting explicit thresholds, and adjusting analyze thresholds separately.[1][2][3][7]

**Example per-table tuning pattern:**

```sql
ALTER TABLE my_large_table SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 10000,
  autovacuum_analyze_scale_factor = 0.005,
  autovacuum_analyze_threshold = 5000
);
```

For especially large insert-heavy tables, also consider the insert-trigger settings instead of tuning only the dead-tuple path.[1]

### C. Cost-Based Throttling and Resource Budgets

- **`autovacuum_vacuum_cost_limit` and `autovacuum_vacuum_cost_delay`:**
  - **Purpose:** These parameters throttle autovacuum so it does not monopolize I/O.[1][4]
  - **Current guidance:** When `autovacuum_vacuum_cost_limit = -1`, autovacuum inherits `vacuum_cost_limit`. Likewise, `autovacuum_vacuum_cost_delay = -1` inherits `vacuum_cost_delay`.[1]
  - **Important caveat:** The effective cost budget is balanced across running autovacuum workers, so a simple “set it to `2000` and every worker gets 10x more work” interpretation is not correct once multiple workers are active.[1][4]
  - **Practical rule:** If autovacuum is falling behind, raise the effective vacuum budget carefully and measure impact. Tune both limit and delay based on storage capacity and workload instead of treating either knob as untouchable.[1][4][3][7]

- **`autovacuum_work_mem`:**
  - **Purpose:** Memory budget for each autovacuum worker.[1]
  - **Current guidance:** Set this explicitly when `maintenance_work_mem` is large enough that inheritance would over-allocate memory. Size it within the overall server memory budget and expected number of workers.[1][3][7]
  - **Practical rule:** Avoid fixed global recommendations like `512MB` without checking table sizes and total concurrent autovacuum memory demand.

### D. Monitoring Whether Autovacuum Is Working

- **`pg_stat_progress_vacuum`:**
  - **Use:** Shows live progress for currently running vacuum and autovacuum processes, including phase information.[5]
  - **Best for:** Determining whether a vacuum is progressing, stuck behind locks, or spending most of its time in heap scan, index cleanup, or truncation phases.[5]

- **`pg_stat_user_tables`:**
  - **Use:** Shows approximate live and dead tuple counts plus `last_autovacuum` and `last_autoanalyze` timestamps.[6]
  - **Best for:** First-pass triage of which tables autovacuum may be neglecting.
  - **Important caveat:** `n_dead_tup` is an estimate, not a precise physical bloat measurement.[6]

**Practical rule:** Watch trends, not single snapshots. The goal is not “zero dead tuples”; it is keeping dead tuples and table growth under control while maintaining foreground query performance.

## References

- [PostgreSQL 18 Documentation: Vacuuming / autovacuum settings][1]
- [PostgreSQL 18 Documentation: Routine Vacuuming][2]
- [Percona: Tuning autovacuum in PostgreSQL][3]
- [pganalyze: How the VACUUM cost model works][4]
- [PostgreSQL 18 Documentation: Progress Reporting][5]
- [PostgreSQL 18 Documentation: Monitoring Stats][6]
- [EDB: Autovacuum tuning basics][7]
- [AWS: Understanding autovacuum in Amazon RDS for PostgreSQL][8]
- [Azure: Autovacuum tuning for PostgreSQL Flexible Server][9]
- [Crunchy Data: Tuning Postgres for high write loads][10]

[1]: https://www.postgresql.org/docs/current/runtime-config-vacuum.html
[2]: https://www.postgresql.org/docs/current/routine-vacuuming.html
[3]: https://www.percona.com/blog/tuning-autovacuum-in-postgresql-and-autovacuum-internals/
[4]: https://pganalyze.com/docs/vacuum-advisor/how-does-the-vacuum-cost-model-work
[5]: https://www.postgresql.org/docs/current/progress-reporting.html
[6]: https://www.postgresql.org/docs/current/monitoring-stats.html
[7]: https://www.enterprisedb.com/blog/autovacuum-tuning-basics
[8]: https://aws.amazon.com/blogs/database/understanding-autovacuum-in-amazon-rds-for-postgresql-environments/
[9]: https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/how-to-autovacuum-tuning
[10]: https://www.crunchydata.com/blog/tuning-your-postgres-database-for-high-write-loads
