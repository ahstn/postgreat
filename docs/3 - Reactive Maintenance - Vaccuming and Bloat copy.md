## III. Reactive Maintenance: Vacuuming and Bloat Management

Even with tuning, autovacuum may not keep up with high-churn tables.42 This leads to "bloat," where dead tuples (old, deleted row versions) consume disk space and slow down queries. This section covers how to identify and manually fix bloat.

### A. Identifying Bloat: Monitoring Dead Tuples

- **Purpose:** `UPDATE` and `DELETE` operations do not physically remove old row versions; they mark them as "dead tuples".43 Bloat is the accumulation of these dead tuples.
    
- **Monitoring Query:** The primary monitoring tool is the `pg_stat_user_tables` view, which tracks live and dead tuples.45
    
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
    

62

### B. Standard Maintenance: VACUUM and ANALYZE

- **`VACUUM`:**
    
    - **Action:** This is the standard, non-blocking cleanup command. It scans the table, reclaims space occupied by dead tuples, and makes that space available for _re-use_ by future `INSERT`s or `UPDATE`s _within the same table_.47
        
    - **Impact:** It does _not_ return space to the operating system. It runs in parallel with normal database operations (`SELECT`, `INSERT`, `UPDATE`) as it does not take an exclusive lock.42
        
- **`ANALYZE`:**
    
    - **Action:** This command updates the data statistics that the query planner uses to make intelligent decisions.42 It is essential for good query performance.
        
- **Sample SQL:**
    
    SQL
    
    ```
    -- Standard, non-blocking manual cleanup and statistics update
    VACUUM (VERBOSE, ANALYZE) my_problem_table;
    ```
    
    43
    

### C. Aggressive Maintenance: VACUUM (FULL), REINDEX, and CLUSTER

These commands are powerful, disruptive, and require an `ACCESS EXCLUSIVE` lock, which blocks _all_ activity (including `SELECT`s) on the table. They should _only_ be run during scheduled maintenance windows.48

- **`VACUUM (FULL)`:**
    
    - **Action:** Rewrites the _entire_ table into a new disk file, removing all bloat and returning the unused space to the operating system.42
        
    - **Impact:** This is a "stop the world" operation. It is the last resort for severe table fragmentation.48
        
- **`REINDEX`:**
    
    - **Action:** Rebuilds an index (or all indexes on a table) from scratch to remove index bloat.43
        
    - **Impact:** Also takes an `ACCESS EXCLUSIVE` lock on the table.
        
    - **Production Alternative:** PostgreSQL 12+ supports `REINDEX CONCURRENTLY`, which, like its `CREATE INDEX` counterpart, rebuilds the index without blocking writes.
        
- **`CLUSTER`:**
    
    - **Action:** Physically rewrites the table, sorting the rows on disk according to a specified index.42
        
    - **Impact:** `ACCESS EXCLUSIVE` lock. This is a one-time operation used, for example, to physically correlate data before creating a BRIN index.40
        

**Table 4: Maintenance Command Locking Behavior and Production Impact**

|**Command**|**Lock Type**|**Production Impact (Blocks...)**|
|---|---|---|
|`CREATE INDEX`|`SHARE`|`INSERT`, `UPDATE`, `DELETE` (Blocks Writes)|
|`CREATE INDEX CONCURRENTLY`|`SHARE UPDATE EXCLUSIVE`|**None.** (Allows Reads & Writes) 31|
|`VACUUM`|`SHARE UPDATE EXCLUSIVE`|**None.** (Allows Reads & Writes) 47|
|`ANALYZE`|`SHARE UPDATE EXCLUSIVE`|**None.** (Allows Reads & Writes)|
|`VACUUM (FULL)`|`ACCESS EXCLUSIVE`|**Blocks ALL Activity** (Reads & Writes) 42|
|`REINDEX`|`ACCESS EXCLUSIVE`|**Blocks ALL Activity** (Reads & Writes) 49|
|`REINDEX CONCURRENTLY` (12+)|`SHARE UPDATE EXCLUSIVE`|**None.** (Allows Reads & Writes)|
|`CLUSTER`|`ACCESS EXCLUSIVE`|**Blocks ALL Activity** (Reads & Writes) 42|

### D. A Decision Framework for Manual Maintenance

1. **Monitor:** Schedule the "Bloat Hit List" query (from III.A) to run daily.
    
2. **Analyze:** Identify tables where `dead_tup_ratio` is consistently high (e.g., > 20%).
    
3. **Investigate:** For a top offender, check `pg_stat_user_tables`: Is `last_autovacuum` recent? If not, check `pg_stat_progress_vacuum` 50: Is an autovacuum worker _stuck_ on this table? (It may be blocked by a long-running query 14).
    
4. **Act (Light):** If autovacuum seems to be blocked or falling behind, trigger a _manual, non-blocking_ vacuum during off-peak hours: `VACUUM (VERBOSE, ANALYZE) my_table;`.47
    
5. **Act (Heavy):** If manual `VACUUM` does not reduce bloat (the space is fragmented) or if indexes are also bloated (check with the `pgstattuple` extension 51), schedule a formal maintenance window.
    
6. **During Window:** Use `REINDEX CONCURRENTLY my_index;` to fix index bloat. As a last resort for extreme table fragmentation, use `VACUUM (FULL) my_table;` to reclaim disk space.