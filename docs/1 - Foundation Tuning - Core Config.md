# Foundational Tuning: Core Configuration / Parameter Groups

**Overview:**

A small group of `postgresql.conf` settings has a large effect on PostgreSQL performance and stability. The right values depend on hardware, workload, and PostgreSQL version. This guide covers PostgreSQL 14 through 18, the majors that are supported as of November 2026. Treat every number here as a starting point for a dedicated server, not a formula. Change one thing at a time and measure.[1][2]

Version notes are called out inline. A summary of default values by version is in [Version differences at a glance](#version-differences-at-a-glance). The full claim-by-claim audit for this guide is in [`docs/audit/claims.md`](./audit/claims.md).

**Table of Contents:**
- [A. Memory Allocation Strategy](#a-memory-allocation-strategy-ram-dependent)
  - [shared_buffers](#1-shared_buffers)
  - [effective_cache_size](#2-effective_cache_size)
  - [work_mem and hash_mem_multiplier](#3-work_mem-and-hash_mem_multiplier)
  - [maintenance_work_mem and autovacuum_work_mem](#4-maintenance_work_mem-and-autovacuum_work_mem)
- [B. Concurrency and Parallelism](#b-concurrency-and-parallelism)
  - [max_connections](#1-max_connections)
  - [max_worker_processes](#2-max_worker_processes)
  - [max_parallel_workers](#3-max_parallel_workers)
  - [max_parallel_workers_per_gather](#4-max_parallel_workers_per_gather)
- [C. Query Planner Cost Model](#c-query-planner-cost-model)
  - [random_page_cost](#1-random_page_cost)
  - [effective_io_concurrency](#2-effective_io_concurrency)
- [D. Checkpoint and WAL Management](#d-checkpoint-and-wal-management)
  - [max_wal_size and min_wal_size](#1-max_wal_size-and-min_wal_size)
  - [checkpoint_timeout and checkpoint_completion_target](#2-checkpoint_timeout-and-checkpoint_completion_target)
  - [Monitoring checkpoints](#3-monitoring-checkpoints)
- [Version differences at a glance](#version-differences-at-a-glance)
- [References](#references)

## A. Memory Allocation Strategy (RAM-Dependent)

PostgreSQL uses its own shared buffer cache and also relies on the operating system page cache. Good memory tuning balances the two. It does not assume one should take almost all of the RAM.[1]

### 1. shared_buffers

- **Purpose:** Sets the amount of memory PostgreSQL uses for its shared buffer cache.[1]
- **Default:** `128MB` in all supported versions. `initdb` may pick less if the kernel does not allow it.[1]
- **Guidance from the manual (same wording in 14 and 18):**
  - On a dedicated server with 1 GB or more of RAM, 25% of system memory is a reasonable starting value.[1][1a]
  - Some workloads benefit from more, but because PostgreSQL also relies on the OS cache, more than 40% of RAM is unlikely to work better than a smaller value.[1]
  - Larger values usually need a larger `max_wal_size`, so that writing a large amount of changed data is spread over a longer period.[1]
- **What the manual does not say:** There is no documented 8 GB cap. Some older community advice suggested one. Large-memory hosts can use more if testing supports it.
- **Restart required:** `shared_buffers` can only be set at server start.[1]
- **Illustrative starting points (25% of RAM):**
  - **16 GiB RAM:** `shared_buffers = 4GB`
  - **64 GiB RAM:** `shared_buffers = 16GB`
  - **256 GiB RAM:** `shared_buffers = 64GB`, then test both lower and higher values.
- **Related:** On Linux, large `shared_buffers` values work better with huge pages (`huge_pages`, default `try`).[1][10]

### 2. effective_cache_size

- **Purpose:** The planner's estimate of the disk cache available to a single query. It does not allocate or reserve memory.[2]
- **Default:** `4GB` in all supported versions.[2]
- **Guidance:**
  - Count both `shared_buffers` and the part of the OS page cache that will hold PostgreSQL data. Also allow for concurrent queries that share that cache.[2]
  - A higher value makes index scans more likely. A lower value makes sequential scans more likely.[2]
  - A starting estimate of 50% to 75% of RAM on a dedicated server is common community practice. It is not a number from the manual. Lower it if the host runs other services.
- **Illustrative starting points:**
  - **16 GiB RAM:** `effective_cache_size = 8GB` to `12GB`
  - **64 GiB RAM:** `effective_cache_size = 32GB` to `48GB`
  - **256 GiB RAM:** `effective_cache_size = 128GB` to `192GB`

### 3. work_mem and hash_mem_multiplier

- **Purpose:** The memory one sort or hash operation may use before it writes to temporary files.[1]
- **Default:** `work_mem = 4MB` in all supported versions.[1]
- **Guidance:**
  - `work_mem` applies **per operation**, not per query or per connection. One complex query can run several sort and hash operations at once. Many sessions can do that at the same time.[1]
  - Each parallel worker counts as a separate process, so a parallel plan can use `work_mem` once per worker per operation.[1]
  - Hash-based operations may use up to `work_mem` × `hash_mem_multiplier`.[1]
  - **Version note:** `hash_mem_multiplier` defaults to `1.0` in PostgreSQL 14 and `2.0` in 15 and later.[1a][1]
  - There is no safe universal value. Keep the global value conservative. Raise it per role or per session for known reporting or ETL work.
  - Validate with `EXPLAIN (ANALYZE, BUFFERS)` and with `log_temp_files`. Sorts that report `external merge`, or hashes with more than one batch, spilled to disk.[5][12]
- **Practical rule:** Small global value, larger targeted overrides, then measure.

### 4. maintenance_work_mem and autovacuum_work_mem

- **Purpose:** Memory for maintenance work such as `VACUUM`, `CREATE INDEX`, and `ALTER TABLE ADD FOREIGN KEY`.[1]
- **Default:** `64MB` in all supported versions. `autovacuum_work_mem` defaults to `-1`, which means "use `maintenance_work_mem`".[1]
- **Guidance:**
  - It is safe to set this higher than `work_mem`, because only one such operation normally runs per session.[1]
  - For parallel utility commands such as parallel `CREATE INDEX` or parallel `VACUUM`, the limit applies to the whole command, not to each worker.[1]
  - Autovacuum can use up to `autovacuum_max_workers` × this value at once. If you raise `maintenance_work_mem` a lot, set `autovacuum_work_mem` separately.[1]
  - **Version note:** In PostgreSQL 14 to 16, `VACUUM` can use at most 1 GB for its dead row list, no matter how high you set these values. PostgreSQL 17 replaced that storage with a more efficient structure and removed the 1 GB cap.[1a][13]
- **Illustrative starting points:**
  - **16 GiB RAM:** `maintenance_work_mem = 512MB`
  - **64 GiB RAM:** `maintenance_work_mem = 1GB`
  - **256 GiB RAM:** `maintenance_work_mem = 2GB`

## B. Concurrency and Parallelism

These settings set how many client backends and background workers PostgreSQL may run. Tune them together.

### 1. max_connections

- **Purpose:** Maximum number of concurrent connections.[7]
- **Default:** `100` (possibly less if `initdb` finds kernel limits). Server restart required.[7]
- **Guidance:**
  - PostgreSQL sizes some resources, including shared memory, directly from `max_connections`. A higher value allocates more of them.[7][14]
  - Each connection is a separate server process. Many busy connections compete for CPU, locks, and memory. Very high values usually hurt throughput, even though the manual itself does not state a limit.
  - There is no reliable formula based on vCPU count.
  - When applications open many more connections than the database needs to run at once, put a connection pooler in front of PostgreSQL. See [PostgreSQL Connection Poolers](./PostgreSQL%20Connection%20Poolers.md).[8]
  - On a standby, `max_connections` must be at least as large as on the primary.[15]
- **Practical rule:** Size for real simultaneous database work, not for every application socket.

### 2. max_worker_processes

- **Purpose:** Maximum number of background processes the cluster supports. Server restart required. Default `8`.[1]
- **Guidance:**
  - Parallel query workers, logical replication workers, and extension background workers all come from this pool.[1]
  - Do not set it equal to the parallel query budget. Leave room for other workers.
  - On a standby, it must be at least as large as on the primary.[1]
- **Not in this pool:** Autovacuum workers have their own limit (`autovacuum_max_workers`, and in PostgreSQL 18 also `autovacuum_worker_slots`). PostgreSQL 18 I/O workers are set by `io_workers`.[6][1]
- **Practical rule:** Start near the CPU count on a dedicated host. Then check it still leaves room for replication and extensions.

### 3. max_parallel_workers

- **Purpose:** Maximum number of workers the system supports for parallel operations, across the whole instance. Default `8`.[1]
- **Guidance:**
  - A value higher than `max_worker_processes` has no effect, because parallel workers come from that pool.[1]
  - Keep it below `max_worker_processes` so parallel queries cannot use up every worker slot.

### 4. max_parallel_workers_per_gather

- **Purpose:** Maximum number of workers one `Gather` or `Gather Merge` node can start. Default `2`. Setting it to `0` disables parallel query.[1]
- **Guidance:**
  - Each worker uses resources much like an extra session, including its own `work_mem`. The manual advises keeping this in mind when choosing a value.[1]
  - If workers are not available at run time, the plan runs with fewer workers than planned.[1]
  - Raising it can help large scans and aggregates. It is not automatically good for OLTP-heavy systems.
- **Practical rule:** Increase gradually after testing real plans. Do not assume "half the CPUs" is right everywhere.

## C. Query Planner Cost Model

Planner cost constants change which plan PostgreSQL picks. Change them carefully and check real queries before and after.

### 1. random_page_cost

- **Purpose:** The planner's cost for a non-sequential page fetch, relative to `seq_page_cost` (default `1.0`). Default `4.0` in all supported versions.[2]
- **What the manual says:**
  - Random access to storage is usually much more than four times slower than sequential access. The default of `4.0` is lower because it assumes most random reads, such as index reads, are served from cache.[2]
  - If the database is likely to be fully cached in memory, setting `random_page_cost` equal to `seq_page_cost` can be reasonable.[2]
  - Lower values make index scans more attractive. Higher values make them look more expensive.[2]
- **What the manual does not say:** It does not mention SSDs and does not recommend `1.1`. Values between `1.1` and `2.0` for SSD or NVMe storage are common community practice, not documented guidance.
- **Practical rule:** On fast storage or when the working set fits in memory, test lower values with representative queries. Change it per tablespace if storage differs (`ALTER TABLESPACE ... SET (random_page_cost = ...)`).[2]

### 2. effective_io_concurrency

- **Purpose:** How many concurrent storage I/O requests a single session should try to issue.[1]
- **Version note, behavior:**
  - **PostgreSQL 14 to 17:** This only affects prefetching in bitmap heap scans. It relies on the OS `posix_fadvise` call. Where that call is missing, any value other than `0` is an error. On some systems, such as Solaris, the call exists but does nothing.[1a]
  - **PostgreSQL 18:** It sets how many I/O requests a session tries to issue in parallel through the new asynchronous I/O system. That system is used by sequential scans, bitmap heap scans, `VACUUM`, and other operations.[1][16]
- **Version note, default:** `1` in PostgreSQL 14 to 17. `16` in PostgreSQL 18. `maintenance_io_concurrency` changed from `10` to `16` in PostgreSQL 18 as well.[1a][1][16]
- **Guidance:**
  - Older advice to set this to `200` for every SSD is too broad. On PostgreSQL 14 to 17 it can still help bitmap-scan-heavy workloads on SSD or networked storage. On PostgreSQL 18, start with the default.
  - PostgreSQL 18 also adds `io_method` (default `worker`) and `io_workers` (default `3`). Leave them at their defaults unless testing shows a gain.[1][16]
- **Practical rule:** Change this only with benchmarks on your storage.

## D. Checkpoint and WAL Management

Checkpoint tuning controls I/O bursts and crash-recovery time. In steady state, most checkpoints should be started by `checkpoint_timeout`, not by WAL volume.[4]

### 1. max_wal_size and min_wal_size

- **Purpose:** `max_wal_size` is a soft limit on WAL growth between automatic checkpoints. `min_wal_size` is how much old WAL is kept for recycling, even when the system is idle.[4][9]
- **Default:** `max_wal_size = 1GB` and `min_wal_size = 80MB` in all supported versions.[9]
- **Guidance:**
  - Checkpoints are expensive. Set the checkpoint parameters high enough that checkpoints do not happen too often.[4]
  - `max_wal_size` is not a hard limit. It can be exceeded under heavy load, a failing `archive_command`, or a lagging replication slot. Leave plenty of disk headroom.[4][9]
  - Larger values mean fewer checkpoints and less full-page-write WAL, but crash recovery may take longer.[4]
  - `checkpoint_warning` (default `30s`) logs a message when checkpoints caused by WAL volume happen closer together than that. If this appears often, raise `max_wal_size`.[4]
- **Illustrative starting points for write-heavy dedicated systems:**
  - **Small:** `max_wal_size = 4GB`
  - **Medium:** `max_wal_size = 8GB` to `16GB`
  - **Large:** `max_wal_size = 16GB` to `32GB`
- **Practical rule:** Increase until requested checkpoints are rare under normal peak load. Then confirm the recovery time is acceptable.

### 2. checkpoint_timeout and checkpoint_completion_target

- `checkpoint_timeout` defaults to `5min`. Raising it lowers checkpoint frequency and full-page-write volume, at the cost of longer recovery.[4][9]
- `checkpoint_completion_target` defaults to `0.9` in all supported versions. The manual recommends not going higher than `0.9`.[4]

### 3. Monitoring checkpoints

The view and column names depend on the version:

| Version | View | Scheduled checkpoints | Requested checkpoints |
|---|---|---|---|
| 14 to 16 | `pg_stat_bgwriter` | `checkpoints_timed` | `checkpoints_req` |
| 17 and 18 | `pg_stat_checkpointer` | `num_timed` | `num_requested` |

```sql
-- PostgreSQL 17 and later
SELECT num_timed, num_requested, write_time, sync_time, buffers_written, stats_reset
FROM pg_stat_checkpointer;

-- PostgreSQL 14 to 16
SELECT checkpoints_timed, checkpoints_req, checkpoint_write_time,
       checkpoint_sync_time, buffers_checkpoint, stats_reset
FROM pg_stat_bgwriter;
```

A high share of requested checkpoints during normal load means `max_wal_size` is too small for the write rate.[4][17]

**Version note:** `log_checkpoints` is `off` by default in PostgreSQL 14 and `on` in 15 and later. On 14, turn it on to see checkpoint timing in the server log.[5a][5]

## Version differences at a glance

Defaults below come from the PostgreSQL manuals and were checked with `SHOW` on PostgreSQL 14.24, 15.19, 16.15, 17.11, and 18.6. More detail is in [`docs/audit/versions-and-sources.md`](./audit/versions-and-sources.md).

| Setting | 14 | 15 | 16 | 17 | 18 |
|---|---|---|---|---|---|
| `hash_mem_multiplier` | 1.0 | 2.0 | 2.0 | 2.0 | 2.0 |
| `effective_io_concurrency` | 1 | 1 | 1 | 1 | 16 |
| `maintenance_io_concurrency` | 10 | 10 | 10 | 10 | 16 |
| `io_method` / `io_workers` | n/a | n/a | n/a | n/a | `worker` / 3 |
| `log_checkpoints` | off | on | on | on | on |
| Checkpoint stats view | `pg_stat_bgwriter` | `pg_stat_bgwriter` | `pg_stat_bgwriter` | `pg_stat_checkpointer` | `pg_stat_checkpointer` |
| `VACUUM` dead row memory cap | 1 GB | 1 GB | 1 GB | none | none |

Unchanged in 14 to 18: `shared_buffers` 128MB, `effective_cache_size` 4GB, `work_mem` 4MB, `maintenance_work_mem` 64MB, `max_connections` 100, `max_worker_processes` 8, `max_parallel_workers` 8, `max_parallel_workers_per_gather` 2, `random_page_cost` 4.0, `max_wal_size` 1GB, `min_wal_size` 80MB, `checkpoint_timeout` 5min, `checkpoint_completion_target` 0.9.

## References

Links point to a specific major version so they do not change meaning when a new release ships.

- [1] [PostgreSQL 18: Resource Consumption][1]
- [1a] [PostgreSQL 14: Resource Consumption][1a]
- [2] [PostgreSQL 18: Planner Cost Constants and Other Planner Options][2]
- [4] [PostgreSQL 18: WAL Configuration][4]
- [5] [PostgreSQL 18: Error Reporting and Logging][5]
- [5a] [PostgreSQL 14: Error Reporting and Logging][5a]
- [6] [PostgreSQL 18: Vacuuming (configuration)][6]
- [7] [PostgreSQL 18: Connections and Authentication][7]
- [8] [PgBouncer documentation][8]
- [9] [PostgreSQL 18: Write Ahead Log (configuration)][9]
- [10] [PostgreSQL 18: Managing Kernel Resources (huge pages)][10]
- [12] [PostgreSQL 18: Using EXPLAIN][12]
- [13] [PostgreSQL 17 release notes][13]
- [14] [PostgreSQL 18: Managing Kernel Resources (shared memory)][14]
- [15] [PostgreSQL 18: Hot Standby (parameter requirements)][15]
- [16] [PostgreSQL 18 release notes][16]
- [17] [PostgreSQL 18: Monitoring statistics (pg_stat_checkpointer)][17]

[1]: https://www.postgresql.org/docs/18/runtime-config-resource.html
[1a]: https://www.postgresql.org/docs/14/runtime-config-resource.html
[2]: https://www.postgresql.org/docs/18/runtime-config-query.html#RUNTIME-CONFIG-QUERY-CONSTANTS
[4]: https://www.postgresql.org/docs/18/wal-configuration.html
[5]: https://www.postgresql.org/docs/18/runtime-config-logging.html
[5a]: https://www.postgresql.org/docs/14/runtime-config-logging.html
[6]: https://www.postgresql.org/docs/18/runtime-config-vacuum.html
[7]: https://www.postgresql.org/docs/18/runtime-config-connection.html
[8]: https://www.pgbouncer.org/config.html
[9]: https://www.postgresql.org/docs/18/runtime-config-wal.html
[10]: https://www.postgresql.org/docs/18/kernel-resources.html#LINUX-HUGE-PAGES
[12]: https://www.postgresql.org/docs/18/using-explain.html
[13]: https://www.postgresql.org/docs/17/release-17.html
[14]: https://www.postgresql.org/docs/18/kernel-resources.html#SYSVIPC
[15]: https://www.postgresql.org/docs/18/hot-standby.html#HOT-STANDBY-ADMIN
[16]: https://www.postgresql.org/docs/18/release-18.html
[17]: https://www.postgresql.org/docs/18/monitoring-stats.html#MONITORING-PG-STAT-CHECKPOINTER-VIEW
