## III. Reactive Maintenance: Vacuuming and Bloat Management

Even with good autovacuum settings, some tables still need manual investigation or intervention. High-churn tables, bulk-delete workflows, and periods of sustained write pressure can all leave behind enough dead tuples or index bloat to justify a targeted maintenance pass.[1][2][7]

### A. Identify Tables That Need Attention

- **MVCC background:** `UPDATE` creates a new row version and `DELETE` leaves an old version behind. Once those old versions are no longer visible to any transaction, `VACUUM` can reclaim the space for reuse.[1][2]
- **First-pass triage:** `pg_stat_user_tables` is the right starting view, but `n_live_tup` and `n_dead_tup` are approximate statistics. A high dead-tuple ratio is a signal to investigate, not a direct measurement of on-disk bloat.[3]
- **Practical rule:** Look for a combination of dead tuples, table size, stale vacuum/analyze timestamps, and user-visible symptoms such as slower scans or excessive table growth.[7][8]

**Triage query:**

```sql
SELECT
    schemaname,
    relname AS table_name,
    n_live_tup,
    n_dead_tup,
    pg_size_pretty(
        pg_total_relation_size(format('%I.%I', schemaname, relname)::regclass)
    ) AS total_size,
    round(n_dead_tup::numeric / NULLIF(n_live_tup, 0), 4) AS dead_tup_ratio,
    last_autovacuum,
    last_autoanalyze
FROM pg_stat_user_tables
WHERE n_live_tup > 0
  AND n_dead_tup > 1000
ORDER BY dead_tup_ratio DESC,
         pg_total_relation_size(format('%I.%I', schemaname, relname)::regclass) DESC
LIMIT 20;
```

This query uses `pg_total_relation_size(...)` so the size column reflects the full table footprint, including indexes and TOAST data, instead of only the main heap relation.[3]

### B. Choose the Right Manual Intervention

**1. Plain `VACUUM`**

- Use this first when autovacuum is behind and you want to reclaim reusable space without blocking normal reads and writes.[2]
- It does **not** usually shrink the relation file on disk, but it makes dead-tuple space available for reuse by future writes.[2]
- `VACUUM (ANALYZE)` is a good manual follow-up when table churn has also made planner statistics stale.[2]

**2. `VACUUM FULL`**

- Use this only when you need to physically rewrite and shrink a heavily bloated table.[1][2]
- It requires an `ACCESS EXCLUSIVE` lock and can block both reads and writes for the duration of the rewrite.[2]
- It also needs extra disk space while the rewrite runs.[2]
- Practical rule: reserve this for cases where reclaiming space on disk is worth a disruptive maintenance window, not as a routine substitute for healthy autovacuum.[2][8]

**3. `REINDEX` / `REINDEX CONCURRENTLY`**

- Use this when the dominant problem is **index** bloat rather than heap bloat.[6]
- `REINDEX` is disruptive because it locks the target object. `REINDEX CONCURRENTLY` reduces locking impact and is usually the safer production choice when supported by the workload and maintenance window.[6]
- Practical rule: do not use `REINDEX` as a generic substitute for `VACUUM`; use it when index size, performance, or corruption concerns point specifically at the indexes.

### C. Measure More Precisely When Needed

- **`pgstattuple`:** When `pg_stat_user_tables` suggests a problem but you need a more direct measurement of table or index bloat, the `pgstattuple` extension provides tuple-level statistics including dead tuple percentage and free space.[5]
- **`pg_stat_progress_vacuum`:** While a manual `VACUUM` is running, use progress reporting to see its phase and whether it is actively making progress.[4]

**Practical rule:** Start with low-cost statistics views, then use `pgstattuple` only for the tables where precision matters enough to justify the extra work.[5][7]

## References

- [PostgreSQL 18 Documentation: Routine Vacuuming][1]
- [PostgreSQL 18 Documentation: VACUUM][2]
- [PostgreSQL 18 Documentation: Monitoring Stats][3]
- [PostgreSQL 18 Documentation: Progress Reporting][4]
- [PostgreSQL 18 Documentation: pgstattuple][5]
- [PostgreSQL 18 Documentation: REINDEX][6]
- [Fastware: Handling dead tuples in PostgreSQL][7]
- [Atlassian KB: VACUUM, ANALYZE, and REINDEX overview][8]

[1]: https://www.postgresql.org/docs/current/routine-vacuuming.html
[2]: https://www.postgresql.org/docs/current/sql-vacuum.html
[3]: https://www.postgresql.org/docs/current/monitoring-stats.html
[4]: https://www.postgresql.org/docs/current/progress-reporting.html
[5]: https://www.postgresql.org/docs/current/pgstattuple.html
[6]: https://www.postgresql.org/docs/current/sql-reindex.html
[7]: https://www.postgresql.fastware.com/pzone/2025-03-improving-postgresql-efficiency-by-handling-dead-tuples
[8]: https://support.atlassian.com/atlassian-knowledge-base/kb/optimize-and-improve-postgresql-performance-with-vacuum-analyze-and-reindex/
