# Foundational Tuning: Core Configuration / Parameter Groups

**Overview:**

The performance and stability of a PostgreSQL instance are heavily influenced by a small group of core settings in `postgresql.conf`. The right values depend on hardware, workload shape, and PostgreSQL version. The guidance below is written against the current PostgreSQL documentation and is intended as a starting point for dedicated servers, not as a universal formula.[1][2]

**Table of Contents:**
- [A. Memory Allocation Strategy](#a-memory-allocation-strategy-ram-dependent)
  - [shared_buffers](#1-shared_buffers)
  - [effective_cache_size](#2-effective_cache_size)
  - [work_mem](#3-work_mem)
  - [maintenance_work_mem](#4-maintenance_work_mem)
- [B. Concurrency and Parallelism](#b-concurrency-and-parallelism)
  - [max_connections](#1-max_connections)
  - [max_worker_processes](#2-max_worker_processes)
  - [max_parallel_workers](#3-max_parallel_workers)
  - [max_parallel_workers_per_gather](#4-max_parallel_workers_per_gather)
- [C. Query Planner Cost Model](#c-query-planner-cost-model)
  - [random_page_cost](#1-random_page_cost)
  - [effective_io_concurrency](#2-effective_io_concurrency)
- [D. Checkpoint and WAL Management](#d-checkpoint-and-wal-management)
  - [max_wal_size & min_wal_size](#1-max_wal_size--min_wal_size)
- [References](#references)

## A. Memory Allocation Strategy (RAM-Dependent)

PostgreSQL relies on both its own shared buffer cache and the operating system page cache. Good memory tuning balances those two layers instead of assuming one should dominate the other.[1]

### 1. shared_buffers

- **Purpose:** Sets the amount of memory PostgreSQL uses for its shared buffer cache.[1]
- **Current guidance:**
  - On a dedicated server with 1 GiB or more of RAM, PostgreSQL documentation recommends **25% of system memory as a reasonable starting point**.[1]
  - PostgreSQL also notes that while some workloads benefit from higher values, it is **unlikely that more than 40% of RAM** will work better than a smaller allocation on most systems.[1]
  - There is **no current authoritative basis for a universal 8 GiB cap**. Large-memory systems may legitimately use much more than that if testing supports it.
  - Larger `shared_buffers` typically require raising `max_wal_size` so checkpoint writes are spread over a longer period.[1][4]
- **Illustrative starting points:**
  - **Small (16 GiB RAM):** `shared_buffers = 4GB`
  - **Medium (64 GiB RAM):** `shared_buffers = 16GB`
  - **Large (256 GiB RAM):** `shared_buffers = 64GB`

### 2. effective_cache_size

- **Purpose:** A planner estimate of the **disk cache available to a single query**. It does not reserve memory.[2]
- **Current guidance:**
  - This should reflect the memory likely available for PostgreSQL data in both `shared_buffers` and the OS page cache.[2]
  - On a dedicated database server, a **50-75% of RAM** starting estimate is common, but the right value depends on whether the host is shared with other services.
  - If set too low, the planner may underestimate the likelihood that index-driven access hits cache and can favor broader scans more often than necessary.[2]
- **Illustrative starting points:**
  - **Small (16 GiB RAM):** `effective_cache_size = 8GB-12GB`
  - **Medium (64 GiB RAM):** `effective_cache_size = 32GB-48GB`
  - **Large (256 GiB RAM):** `effective_cache_size = 128GB-192GB`

### 3. work_mem

- **Purpose:** Memory available to each internal sort or hash operation before writing temporary files.[1]
- **Current guidance:**
  - `work_mem` is **per operation**, not per query and not per connection. A single statement can use it multiple times, and parallel query workers multiply the total memory demand.[1]
  - Hash-based operations can use more than `work_mem` because PostgreSQL also applies `hash_mem_multiplier`.[1]
  - There is no safe universal global value. For mixed and OLTP-heavy systems, a conservative global setting is usually better, with larger session- or role-level overrides for analytical workloads.
  - Validate with `EXPLAIN (ANALYZE, BUFFERS)` and temporary-file logging. If sorts or hashes spill to disk, that is evidence the query may benefit from more memory.[2][5]
- **Practical rule:** Keep the global default conservative and raise it selectively for known expensive reporting or ETL sessions.

### 4. maintenance_work_mem

- **Purpose:** Memory budget for maintenance operations such as `VACUUM`, `CREATE INDEX`, and `ALTER TABLE ADD FOREIGN KEY`.[1]
- **Current guidance:**
  - This can be set higher than `work_mem` because it is used by maintenance tasks, not by every query.[1]
  - Parallel utility commands treat this as a command-wide budget, not a budget per worker.[1]
  - If `maintenance_work_mem` is raised aggressively, it is usually wise to set `autovacuum_work_mem` explicitly so autovacuum workers do not inherit an unexpectedly large value.[6]
- **Illustrative starting points:**
  - **Small (16 GiB RAM):** `maintenance_work_mem = 512MB`
  - **Medium (64 GiB RAM):** `maintenance_work_mem = 1GB`
  - **Large (256 GiB RAM):** `maintenance_work_mem = 2GB`

## B. Concurrency and Parallelism

These settings define how many backends and background workers PostgreSQL may use. They should be tuned together, not independently.

### 1. max_connections

- **Purpose:** Maximum number of concurrent client connections.[7]
- **Current guidance:**
  - PostgreSQL warns that increasing this value increases shared memory usage and can reduce performance if set much higher than necessary.[7]
  - There is **no reliable vCPU-based formula** that works across workloads.
  - For most production systems, keep this near the actual database-side concurrency you want and use a connection pooler such as PgBouncer in front of the database when the application fan-out is high.[7][8]
- **Practical rule:** Size for real simultaneous database work, not for every application socket.

### 2. max_worker_processes

- **Purpose:** Overall limit for background worker processes.[1]
- **Current guidance:**
  - This is the total budget that parallel query, logical replication workers, and extension background workers draw from.[1]
  - It must leave headroom for the features you actually use. Setting it equal to your parallel-query budget is a common mistake.
- **Practical rule:** Start with a value around the number of available CPUs on a dedicated host, then confirm it still leaves room for replication workers and extension background workers.

### 3. max_parallel_workers

- **Purpose:** Maximum number of parallel workers that can be active across the whole instance.[1]
- **Current guidance:**
  - This is capped by `max_worker_processes`.[1]
  - Do not set it so high that parallel query can consume the entire background-worker budget and starve maintenance or replication work.
- **Practical rule:** Start below the total worker budget and reserve headroom for non-query workers.

### 4. max_parallel_workers_per_gather

- **Purpose:** Maximum number of parallel workers a single `Gather` or `Gather Merge` node may request.[1]
- **Current guidance:**
  - PostgreSQL notes that each worker is effectively an additional process with its own CPU and memory demands.[1]
  - The default of `2` is intentionally conservative. Raising it can help large scans and aggregates, but it is not automatically beneficial on OLTP-heavy systems.
- **Practical rule:** Increase gradually after testing representative plans; do not assume "half the CPUs" is the right answer for every server.

## C. Query Planner Cost Model

Planner cost constants influence plan selection. They should be changed carefully and validated against real queries.

### 1. random_page_cost

- **Purpose:** Planner estimate for the cost of a non-sequential page fetch.[2]
- **Current guidance:**
  - The default of `4.0` is **not simply an HDD default**. PostgreSQL documentation explicitly says it models random access as slower than sequential access while also assuming many random reads are cached.[2]
  - On SSD/NVMe-backed systems, or when the working set largely fits in memory, lower values often make sense. In fully cached environments, PostgreSQL notes it can even be reasonable to set `random_page_cost` equal to `seq_page_cost`.[2]
  - Do not lower this blindly. Compare plans before and after changing it.
- **Practical rule:** Benchmark lower values on SSD-backed systems, but treat them as workload-specific tuning rather than a mandatory override.

### 2. effective_io_concurrency

- **Purpose:** Estimate of how many concurrent I/O operations the storage subsystem can support. It mainly affects bitmap heap scans.[2]
- **Current guidance:**
  - In current PostgreSQL, the default is `16`.[2]
  - Higher values can help on storage that handles concurrent reads well, but the setting has no effect on platforms where the required OS support is unavailable.[2]
  - Older advice to set this to `200` for every SSD-backed server is too broad for current PostgreSQL. Treat increases as a benchmark-driven change.
- **Practical rule:** Keep the default unless testing shows a real gain from higher concurrency on your storage stack.

## D. Checkpoint and WAL Management

Checkpoint tuning is primarily about controlling I/O bursts and recovery trade-offs. The goal is usually to avoid frequent requested checkpoints during steady-state load.[4]

### 1. max_wal_size & min_wal_size

- **Purpose:** `max_wal_size` is a soft limit on how much WAL accumulates before PostgreSQL forces a checkpoint. `min_wal_size` influences how much WAL is retained for recycling.[4]
- **Current guidance:**
  - PostgreSQL documentation recommends increasing `max_wal_size` if checkpoints happen too often.[4]
  - The goal is usually to let checkpoints be driven mostly by `checkpoint_timeout`, not by constant pressure from WAL volume.[4]
  - Larger values reduce checkpoint frequency, but they also increase crash-recovery time because more WAL may need to be replayed.[4]
  - Tune this together with `checkpoint_timeout` and `checkpoint_completion_target`, and watch `checkpoints_timed` versus `checkpoints_req` in `pg_stat_checkpointer`.[4][9]
- **Illustrative starting points for write-heavy dedicated systems:**
  - **Small:** `max_wal_size = 4GB`
  - **Medium:** `max_wal_size = 8GB-16GB`
  - **Large:** `max_wal_size = 16GB-32GB`
- **Practical rule:** Increase until requested checkpoints stop being a routine event under normal peak load, then confirm recovery-time trade-offs are acceptable.

## References

- [PostgreSQL 18 Documentation: Resource Consumption][1]
- [PostgreSQL 18 Documentation: Planner Cost Constants][2]
- [PostgreSQL 18 Documentation: WAL Configuration][4]
- [PostgreSQL 18 Documentation: Logging and Temp Files][5]
- [pganalyze: Tuning VACUUM and autovacuum][6]
- [PostgreSQL 18 Documentation: Connections and Authentication][7]
- [PgBouncer Documentation][8]
- [PostgreSQL 18 Documentation: Monitoring Database Activity][9]

[1]: https://www.postgresql.org/docs/current/runtime-config-resource.html
[2]: https://www.postgresql.org/docs/current/runtime-config-query.html
[4]: https://www.postgresql.org/docs/current/wal-configuration.html
[5]: https://www.postgresql.org/docs/current/runtime-config-logging.html
[6]: https://pganalyze.com/blog/5mins-postgres-tuning-vacuum-autovacuum
[7]: https://www.postgresql.org/docs/current/runtime-config-connection.html
[8]: https://www.pgbouncer.org/
[9]: https://www.postgresql.org/docs/current/monitoring-stats.html
