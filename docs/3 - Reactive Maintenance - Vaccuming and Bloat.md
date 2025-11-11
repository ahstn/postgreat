## III. Reactive Maintenance: Vacuuming and Bloat Management

Even with tuning, autovacuum may not keep up with high-churn tables.[42] This leads to "bloat," where dead tuples (old, deleted row versions) consume disk space and slow down queries. This section covers how to identify and manually fix bloat.

### A. Identifying Bloat: Monitoring Dead Tuples

- **Purpose:** `UPDATE` and `DELETE` operations do not physically remove old row versions; they mark them as "dead tuples".[43] Bloat is the accumulation of these dead tuples.

- **Monitoring Query:** The primary monitoring tool is the `pg_stat_user_tables` view, which tracks live and dead tuples.[45]

- **Rationale:** Simply looking at `n_dead_tup` (the count of dead tuples) is misleading. A million dead tuples is a critical problem for a 10-million-row table, but it is negligible for a 10-billion-row table. The key metric is the _ratio_ of dead tuples to live tuples.

- **"Bloat Hit List" SQL:** This query provides a prioritized "hit list" of the most bloated tables, ordered by their dead tuple ratio.

```sql
SELECT
	schemaname,
	relname AS table_name,
	n_live_tup,
	n_dead_tup,
	pg_size_pretty(pg_relation_size(schemaname | '.' | relname)) as table_size,
	CASE WHEN n_live_tup > 0
		THEN round((n_dead_tup::float / n_live_tup::float)::numeric, 4)
		ELSE 0
	END AS dead_tup_ratio,
	last_autovacuum,
	last_autoanalyze
FROM pg_stat_user_tables
WHERE n_live_tup > 0 AND n_dead_tup > 1000 -- Ignore trivial tables
ORDER BY dead_tup_ratio DESC LIMIT 20;
```

## References

- [Documentation: 18: 24.1. Routine Vacuuming - PostgreSQL][42]
- [Optimize and Improve PostgreSQL Performance with VACUUM ][43]
- [PostgreSQL Performance Optimization — Cleaning Dead Tuples & Reindexing - Medium][44]
- [Dead Tuples in PostgreSQL. - DEV Community][45]
- [Improving PostgreSQL efficiency by handling dead tuples - Fujitsu Enterprise Postgres][46]
- [Documentation: 18: VACUUM - PostgreSQL][47]
- [VACUUM FULL ANALYZE much better than VACUUM ANALYZE + REINDEX : r/PostgreSQL - Reddit][48]
- [The Silent Killer of DB Performance: Demystifying Table Bloat in PostgreSQL - Medium][49]

[42]: https://www.postgresql.org/docs/current/routine-vacuuming.html
[43]: https://support.atlassian.com/atlassian-knowledge-base/kb/optimize-and-improve-postgresql-performance-with-vacuum-analyze-and-reindex/
[44]: https://medium.com/@nakulmitra2114/postgresql-performance-optimization-cleaning-dead-tuples-reindexing-9b1346408b97
[45]: https://dev.to/sandeepkumardev/how-to-handle-dead-tuples-in-postgresql-54m1
[46]: https://www.postgresql.fastware.com/pzone/2025-03-improving-postgresql-efficiency-by-handling-dead-tuples
[47]: https://www.postgresql.org/docs/current/sql-vacuum.html
[48]: https://www.reddit.com/r/PostgreSQL/comments/1dmfohx/vacuum_full_analyze_much_better_than_vacuum/
[49]: https://medium.com/@aminechichi99/the-silent-killer-of-db-performance-demystifying-table-bloat-in-postgresql-84773ddaf078
