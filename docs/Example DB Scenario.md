# Example DB Scenario

A worked example of applying the tuning playbook to one hypothetical server. The values are **starting points to validate**, not prescriptions. Each row says what to measure before and after the change.

Checked against PostgreSQL 14 to 18. PostgreSQL 14 reaches end of life on 2026-11-12, so plan an upgrade if you are on it.

## Assumptions

One set of assumptions is used throughout this page.

**Primary server:**
- **Instance:** AWS `m8g.4xlarge` (Graviton4). 16 vCPU, 64 GiB RAM, EBS-only storage.
- **Instance limits (AWS):** network 7.5 Gbps baseline, up to 15 Gbps burst. EBS 5,000 Mbps baseline, up to 10,000 Mbps burst (625 / 1,250 MB/s, 20,000 / 40,000 IOPS). Burst is time-limited, so size for the baseline.
- **Storage:** one 500 GiB EBS `gp3` volume, provisioned at 12,000 IOPS and 500 MiB/s. gp3 includes 3,000 IOPS and 125 MiB/s; the rest is provisioned. Current gp3 limits are 80,000 IOPS (500 IOPS per GiB), 2,000 MiB/s (0.25 MiB/s per provisioned IOPS) and 64 TiB. This volume sits below the instance EBS baseline, so the volume is the I/O limit, not the instance.
- **Workload:** OLTP. Short transactions, mostly index lookups and small updates.
- **Connections:** the application opens at most 40 connections. There is no connection pooler.
- **OS:** Linux, dedicated to PostgreSQL.

**Replica:**
- One physical streaming replica on the same instance type, used for failover and some read queries.
- A smaller replica (for example `m7g.2xlarge`, 8 vCPU and 32 GiB) is possible, but a hot standby must have `max_connections`, `max_worker_processes`, `max_wal_senders`, `max_prepared_transactions` and `max_locks_per_transaction` at least as high as the primary. It may also fall behind on replay and cannot take the full load after failover.

## Memory Budget

Check the total before applying the table. Rough worst case on 64 GiB:

- `shared_buffers`: 16 GiB.
- Query memory: 40 connections × 2 sort or hash nodes each × `work_mem` 16MB × `hash_mem_multiplier` 2.0 = about 2.5 GiB. Parallel workers also get their own `work_mem`, so add a margin.
- Autovacuum: 5 workers × 1 GiB (`maintenance_work_mem`, inherited) = up to 5 GiB.
- Per-connection overhead, OS and page cache use the rest.

**Note on `work_mem` and poolers:** not having a pooler does not allow a higher `work_mem`. Every client connection is a server process, so the memory budget is set by how many connections run queries at the same time. Here that number is small and fixed (40), which is why 16MB is safe. If the connection count grows, lower `work_mem` or add a pooler.

**Version note:** `hash_mem_multiplier` defaults to 1.0 on PostgreSQL 14 and 2.0 on 15 and later. On 14 hash nodes use less memory at the same `work_mem`.

## Starting Configuration

Defaults below were read from `pg_settings.boot_val` on PostgreSQL 14.24, 15.19, 16.15, 17.11 and 18.6, and checked against the version-pinned manuals.

| **Parameter** | **PG14–17 default** | **PG18 default** | **Starting point** | **Why / how to validate** |
| --- | --- | --- | --- | --- |
| **`shared_buffers`** | 128MB | 128MB | 16GB | About 25% of RAM, the manual's suggested start. Restart required. Configure Linux huge pages for a buffer this size. Validate with the buffer hit ratio in `pg_stat_database` and checkpoint write volume. |
| **`effective_cache_size`** | 4GB | 4GB | 48GB | Planner hint only, nothing is allocated. 50–75% of RAM is the usual range. Compare plans with `EXPLAIN` before and after. |
| **`work_mem`** | 4MB | 4MB | 16MB | Conservative global value from the budget above. Find spills with `temp_blks_written` in `pg_stat_statements` or `log_temp_files`, then raise it per role (`ALTER ROLE reporting SET work_mem = '128MB'`) rather than globally. |
| **`hash_mem_multiplier`** | 1.0 on 14, 2.0 on 15–17 | 2.0 | Default | Multiplies `work_mem` for hash nodes. Include it in the memory budget. |
| **`maintenance_work_mem`** | 64MB | 64MB | 1GB | Faster `CREATE INDEX` and vacuum. Autovacuum workers inherit it unless `autovacuum_work_mem` is set. On 14–16, vacuum can only use up to 1GB for dead tuple IDs. On 17 and later that limit is gone and vacuum memory use is more compact. |
| **`autovacuum_work_mem`** | -1 (uses `maintenance_work_mem`) | -1 | -1 | Leave it inherited while `maintenance_work_mem` is 1GB. Set it explicitly only if you raise `maintenance_work_mem` much higher for index builds, so that autovacuum workers do not each take the large value. |
| **`max_connections`** | 100 | 100 | 60 | 40 for the application plus headroom for admin, monitoring and migrations. Restart required. The replica must use the same or a higher value. Watch `pg_stat_activity` for the real peak. |
| **`max_worker_processes`** | 8 | 8 | 16 | Pool for parallel workers, logical replication workers and extensions. Restart required. The replica must use the same or a higher value. |
| **`max_parallel_workers`** | 8 | 8 | 8 | Default. Matching vCPU count (16) leaves no CPU for OLTP sessions during large parallel queries. Raise only if reporting queries need it and CPU allows. |
| **`max_parallel_workers_per_gather`** | 2 | 2 | 2 | Default for OLTP. Raise per role or per session for reporting queries. Half the vCPUs per query (8) can starve other sessions. |
| **`max_parallel_maintenance_workers`** | 2 | 2 | 4 | Speeds up `CREATE INDEX` and parallel `VACUUM` index phases. Each worker uses part of `maintenance_work_mem`. |
| **`random_page_cost`** | 4.0 | 4.0 | 4.0 until tested, then try 1.1–2.0 | Lower values make index scans look cheaper. On SSD and a high cache hit rate, 1.1–2.0 is common. EBS is network storage with higher latency than local NVMe. Change it in a session, compare `EXPLAIN (ANALYZE, BUFFERS)` on representative queries, then apply globally. |
| **`effective_io_concurrency`** | 1 | 16 | Benchmark 16–64 | On 14–17 it mainly controls prefetch for bitmap heap scans. On 18 it also drives asynchronous I/O for more scan types. Benchmark bitmap-heavy queries at a few values. Too high can raise latency for everyone. Can be set per tablespace. |
| **`maintenance_io_concurrency`** | 10 | 16 | Default | Same idea for maintenance work. Tune only if vacuum or index builds are I/O bound. |
| **`io_method`** | n/a | worker | Default | PostgreSQL 18 only. `io_uring` is an option on Linux, but test it first. |
| **`checkpoint_timeout`** | 5min | 5min | 15min | Fewer checkpoints means fewer full-page writes and less WAL. The trade-off is longer crash recovery. Validate that most checkpoints are timed, not requested. |
| **`max_wal_size`** | 1GB | 1GB | 16GB | Lets checkpoints be triggered by time rather than WAL volume. Check requested vs timed checkpoints: `pg_stat_bgwriter` on 14–16, `pg_stat_checkpointer` on 17 and later. Fits easily on a 500 GiB volume. |
| **`min_wal_size`** | 80MB | 80MB | 2GB | Keeps recycled WAL segments for write bursts. |
| **`checkpoint_completion_target`** | 0.9 | 0.9 | 0.9 (default) | The default changed from 0.5 to 0.9 in PostgreSQL 14. No change needed on any supported version. |
| **`autovacuum_max_workers`** | 3 | 3 | 5 | More tables vacuumed in parallel. On 14–17 it needs a restart. On 18 it can be changed with a reload, up to `autovacuum_worker_slots` (default 16). More workers do not add vacuum throughput by themselves, see the cost limit row. |
| **`autovacuum_naptime`** | 1min | 1min | 1min (default) | Lower it only if there are many databases or autovacuum starts too late on busy tables. Check `last_autovacuum` and `n_dead_tup` in `pg_stat_user_tables`. |
| **`autovacuum_vacuum_cost_limit`** | -1 (uses `vacuum_cost_limit` = 200) | -1 (200) | 1000, raise to 2000 if vacuum lags | The limit is shared across all running autovacuum workers, not per worker. With 5 workers at 1000, each gets about 200. Raise it when dead tuples grow faster than vacuum removes them and the volume has I/O headroom. `autovacuum_vacuum_cost_delay` defaults to 2ms. |
| **`autovacuum_vacuum_scale_factor`** | 0.2 | 0.2 | 0.05 | Vacuum after about 5% of rows change instead of 20%. Tune large or hot tables individually with `ALTER TABLE ... SET (autovacuum_vacuum_scale_factor = ...)`. PostgreSQL 18 also adds `autovacuum_vacuum_max_threshold` (default 100,000,000 tuples), which caps the trigger point on very large tables. |
| **`track_io_timing`** | off | off | on | Needed for I/O timing in `pg_stat_statements` and `EXPLAIN (BUFFERS)`. Check overhead first with `pg_test_timing`. |
| **`wal_level`** | replica | replica | replica (default) | Physical streaming replication needs `replica` or `logical`. |
| **`max_wal_senders`** | 10 | 10 | 10 (default) | One sender per replica or backup stream. The default is enough here. The replica must use the same or a higher value. |
| **`max_replication_slots`** | 10 | 10 | 10 (default) | Use a physical replication slot for the replica (`primary_slot_name` on the standby). |
| **`max_slot_wal_keep_size`** | -1 (unlimited) | -1 | 100GB | A slot keeps WAL until the replica has it. If the replica stops, WAL grows without limit and can fill the volume. This caps it; the slot is invalidated past the cap and the replica must be rebuilt or restored from an archive. |
| **`wal_keep_size`** | 0 | 0 | 0 (default) | Not required when a replication slot or WAL archive is used. Set it (for example 4–16GB) only for a replica without a slot, as a fixed retention buffer. |
| **`hot_standby`** | on | on | on (default, on the replica) | Allows read queries on the replica. Consider `hot_standby_feedback` if replica queries are cancelled, but it can delay vacuum on the primary. |

## How to Roll This Out

1. Record a baseline: `pg_stat_statements` top queries, temp file usage, checkpoint counts, autovacuum activity and EBS metrics (IOPS, throughput, queue length).
2. Apply restart-only settings (`shared_buffers`, `max_connections`, `max_worker_processes`) in one maintenance window. On the replica, raise these first, then the primary.
3. Apply reloadable settings one group at a time and compare against the baseline.
4. Test planner settings (`random_page_cost`, `effective_io_concurrency`) in a session before applying them globally.

## References

- [Resource Consumption (PostgreSQL 18)](https://www.postgresql.org/docs/18/runtime-config-resource.html), [(PostgreSQL 16)](https://www.postgresql.org/docs/16/runtime-config-resource.html)
- [Write Ahead Log (PostgreSQL 18)](https://www.postgresql.org/docs/18/runtime-config-wal.html)
- [Replication (PostgreSQL 18)](https://www.postgresql.org/docs/18/runtime-config-replication.html)
- [Vacuuming and autovacuum settings (PostgreSQL 18)](https://www.postgresql.org/docs/18/runtime-config-vacuum.html), [(PostgreSQL 14)](https://www.postgresql.org/docs/14/runtime-config-autovacuum.html)
- [Hot standby (PostgreSQL 18)](https://www.postgresql.org/docs/18/hot-standby.html)
- [PostgreSQL 14 release notes](https://www.postgresql.org/docs/14/release-14.html), [17 release notes](https://www.postgresql.org/docs/17/release-17.html), [18 release notes](https://www.postgresql.org/docs/18/release-18.html)
- [Amazon EC2 general purpose instance specifications](https://docs.aws.amazon.com/ec2/latest/instancetypes/gp.html)
- [Amazon EBS General Purpose SSD volumes](https://docs.aws.amazon.com/ebs/latest/userguide/general-purpose.html)
