## IV. Automated Maintenance: Tuning Background Workers and Autovacuum

The goal of this section is to tune autovacuum so effectively that the manual maintenance described in [3 - Reactive Maintenance](./3 - Reactive Maintenance - Vaccuming and Bloat.md) is _rarely_ needed. This is the _most critical_ tuning for high-write OLTP workloads.[52]

### A. Autovacuum Worker Configuration

- **`autovacuum_max_workers`:**
    - **Purpose:** The maximum number of autovacuum processes that can run in parallel.[54]
    - **Rationale:** The default of 3 is often too low for a server with many active databases and tables.
    - **Recommendation:** Increase to `5`.[54] For large systems with many vCPUs and fast storage, 5-10 is reasonable.

- **`autovacuum_naptime`:**
    - **Purpose:** The minimum delay between autovacuum runs _on a given database_.[14] The launcher sleeps for this time, then checks for work.
    - **Rationale:** The default is `1min`.[14] If you have 60 databases on your instance, the launcher will only wake up and start a worker in a specific database every `1min / 60 = 1 second`.[55]
    - **Recommendation:** For high-churn systems with many tables, lowering this to `30s` makes autovacuum more responsive.[54]


### B. Autovacuum Thresholds: The Key to Taming Bloat

This is the central problem with default autovacuum. A vacuum is triggered when the number of dead tuples exceeds a threshold, calculated by this formula [53]:

`dead_tuples > autovacuum_vacuum_threshold + (autovacuum_vacuum_scale_factor * table_row_count)`

**The Default Problem:**
- By default, `autovacuum_vacuum_threshold = 50` [14] and `autovacuum_vacuum_scale_factor = 0.2` (i.e., 20% of the table).[57]
- This percentage-based formula works well for _small_ tables:
    - **10,000-row table:** Trigger = `50 + (0.2 * 10,000) = 2,050` dead tuples. (Reasonable).
- It is _catastrophic_ for _large_ tables:
    - **100-million-row table:** Trigger = `50 + (0.2 * 100,000,000) = 20,000,050` dead tuples.
    - **1-billion-row table:** Trigger = `50 + (0.2 * 1,000,000,000) = 200,000,050` dead tuples.[54]
- This means, by default, autovacuum _will not even start_ on a large table until 200 million rows are dead, by which point the table is disastrously bloated.


**The Solution: Per-Table Tuning:**
- The fix is to _disable_ the scale factor (which is only good for small tables) and use a _fixed, absolute threshold_ for large tables.[57]
- This is done via an `ALTER TABLE` command, _not_ by changing the global default.

**Sample SQL:**

```sql
-- For a very large, high-churn table:
-- Disable the 20% scale factor, set a fixed 10,000 row threshold
ALTER TABLE my_large_table SET (
  autovacuum_vacuum_scale_factor = 0,
  autovacuum_vacuum_threshold = 10000
);

-- Also adjust ANALYZE to run more frequently
ALTER TABLE my_large_table SET (
  autovacuum_analyze_scale_factor = 0,
  autovacuum_analyze_threshold = 5000
);
```

### C. Throttling and Resource Management

- **`autovacuum_vacuum_cost_limit` & `autovacuum_vacuum_cost_delay`:**
    - **Purpose:** These parameters _throttle_ autovacuum to prevent it from consuming too much I/O. Autovacuum accumulates a "cost" for every page it reads or dirties.[58] When it hits the `autovacuum_vacuum_cost_limit`, it _sleeps_ for `autovacuum_vacuum_cost_delay`.[59]
    - **The Default Problem:** The default `cost_limit` is 200 (when `autovacuum_vacuum_cost_limit = -1`, it inherits `vacuum_cost_limit = 200`).[53] The cost for dirtying a page is 20.57 This means the autovacuum worker _sleeps after processing only 10 dirty pages_ (a mere 80 KiB).[57]
    - **Rationale:** The default throttling is so aggressive that autovacuum _cannot keep up_ with any modern, write-intensive workload.
    - **Recommendation:** Do _not_ lower `cost_delay` (the sleep time), as that can saturate I/O. Instead, _increase_ the `cost_limit` (the amount of work done _before_ sleeping).
    - **Setting:** `autovacuum_vacuum_cost_limit = 2000` (10x the default).[15] This allows the worker to run 10x longer before its 2ms nap, making it far more effective.

- **`autovacuum_work_mem`:**
    - **Purpose:** Sets the memory for _each_ autovacuum worker process.[9]
    - **Rationale:** As noted in I.A.[4], this _must_ be set explicitly to avoid inheriting a multi-gigabyte `maintenance_work_mem`.
    - **Recommendation:** `autovacuum_work_mem = 512MB`.[9]


### D. Monitoring Autovacuum Activity

- **`pg_stat_progress_vacuum`:**
    - **Purpose:** This view shows _live, real-time_ information about vacuum and autovacuum processes _that are currently running_.[50]
    - **Use:** This is the primary tool to debug a _stuck_ vacuum. It shows the `pid`, the table being vacuumed, and the `phase` (e.g., "scanning heap", "vacuuming indexes", "truncating heap").[50]

- **`pg_stat_user_tables`:**
    - **Purpose:** This view shows the _results_ and _history_ of autovacuum.
    - **Use:** Check `n_dead_tup` (is it trending down?) and `last_autovacuum` / `last_autoanalyze` (when did it last run?) to confirm autovacuum is running successfully.[62]


## References

- [Tuning PostgreSQL for Write Heavy Workloads - CloudRaft][52]
- [Autovacuum Tuning Basics for Optimizing Performance - Best Practices - EDB Postgres AI][53]
- [08-PostgreSQL 17: Complete Tuning Guide for VACUUM & AUTOVACUUM - Medium][54]
- [Autovacuum Tuning - Azure Database for PostgreSQL | Microsoft Learn][55]
- [Tuning Autovacuum in PostgreSQL and Autovacuum Internals - Percona][56]
- [5mins of Postgres E12: The basics of tuning VACUUM and ][57]
- [How does the VACUUM cost model work? - pganalyze][58]
- [What is autovacuum_vacuum_cost_delay in autovacuum in PostgreSQL? - Stack Overflow][59]
- [Find bloated tables and indexes in PostgreSQL without extensions ][62]


[4]: https://www.mydbops.com/blog/postgresql-parameter-tuning-best-practices
[9]: https://www.crunchydata.com/blog/tuning-your-postgres-database-for-high-write-loads
[14]: https://aws.amazon.com/blogs/database/understanding-autovacuum-in-amazon-rds-for-postgresql-environments/
[52]: https://www.cloudraft.io/blog/tuning-postgresql-for-write-heavy-workloads
[53]: https://www.enterprisedb.com/blog/autovacuum-tuning-basics
[54]: https://medium.com/@jramcloud1/08-postgresql-17-complete-tuning-guide-for-vacuum-autovacuum-aa36b945a7cf
[55]: https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/how-to-autovacuum-tuning
[56]: https://www.percona.com/blog/tuning-autovacuum-in-postgresql-and-autovacuum-internals/
[57]: https://pganalyze.com/blog/5mins-postgres-tuning-vacuum-autovacuum
[58]: https://pganalyze.com/docs/vacuum-advisor/how-does-the-vacuum-cost-model-work
[59]: https://stackoverflow.com/questions/63671302/what-is-autovacuum-vacuum-cost-delay-in-autovacuum-in-postgresql
[62]: https://dba.stackexchange.com/questions/302507/find-bloated-tables-and-indexes-in-postgresql-without-extensions
