# Foundational Tuning: Core Configuration / Parameter Groups

The performance and stability of a PostgreSQL instance are predominantly dictated by the settings within its `postgresql.conf` configuration file. These parameters govern memory allocation, concurrency, I/O behavior, and query planning. Proper tuning requires aligning these settings with the available hardware (RAM, vCPU), storage type (SSD, NVMe), and the specific database workload (e.g., OLTP vs. OLAP).1

## A. Memory Allocation Strategy (RAM-Dependent)

Memory parameters are the most critical for performance, as they control how much data is cached in RAM, reducing costly disk I/O.[4] PostgreSQL relies on a "double buffering" system: its own internal `shared_buffers` cache and the operating system's (OS) file system cache.[6] Tuning is a balance between these two.

### 1. shared_buffers

- **Purpose:** Defines the amount of RAM that PostgreSQL allocates for its own dedicated data cache, used for "hot" pages and frequently accessed data.[8] This is an extremely effective performance parameter.[7]
- **Heuristics:**
    - A universally accepted starting point for a dedicated database server is 25% of the total system RAM.[6]
    - An alternative strategy, particularly for large-memory systems (64 GiB+), is to set it between 25-40% but with a _cap_ of 8 GiB.[8]
    - The reasoning for this 8 GiB cap relates to PostgreSQL's reliance on the OS cache. On a VM with 256 GiB of RAM, the OS file system cache is extremely efficient at managing memory. Allocating a massive `shared_buffers` (e.g., 64 GiB, or 25% of 256) can be detrimental. It consumes memory the OS could use more flexibly and, for some write-heavy workloads, the entire large buffer must be processed during checkpoints, which can induce I/O stalls.[6]
- **Tier-Based Examples (25% Rule):**
    - **Small (16 GiB RAM):** `shared_buffers = 4GB` [6]
    - **Medium (64 GiB RAM):** `shared_buffers = 16GB` [6]
    - **Large (256 GiB RAM):** `shared_buffers = 64GB` [6]


### 2. effective_cache_size

- **Purpose:** This parameter does _not_ allocate any memory. It is a _cost-model hint_ for the query planner.[6] It provides an estimate of the _total_ memory available for caching data, which includes both PostgreSQL's `shared_buffers` and, crucially, the entire OS file system cache.[8]
- **Heuristics & Rationale:**
    - A common, effective setting for a dedicated server is 75% of total system RAM.[6]
    - This parameter directly controls the planner's "confidence" in using index scans. If `effective_cache_size` is set too low (like the default), the planner assumes that fetching data via an index will require expensive physical disk I/O (a "cache miss"). It will therefore often favor a full-table _sequential scan_, even when a perfectly good index exists.[6]
    - By setting this to 75% of RAM, the administrator signals to the planner that a large portion of the database is likely already in the OS cache. The planner becomes "confident" that an index scan will be fast (a "cache hit") and will be far more likely to choose it.
- **Tier-Based Examples (75% Rule):**
    - **Small (16 GiB RAM):** `effective_cache_size = 12GB` [6]
    - **Medium (64 GiB RAM):** `effective_cache_size = 48GB` [6]
    - **Large (256 GiB RAM):** `effective_cache_size = 192GB` [6]

### 3. work_mem
- **Purpose:** This parameter allocates memory _privately_ for _each_ complex operation (like a sort, hash join, or aggregation) _within_ a query before it "spills" to temporary disk files.8

- **Heuristics & Rationale:**
    - This setting is highly dangerous to set high globally, as it is multiplied by the number of concurrent operations and connections.[9] A simple query might use `work_mem` multiple times (e.g., for a hash join and then a sort).
    - The total potential memory consumption is `work_mem * <operations_per_query> * <active_connections>`. A high `work_mem` with many connections is the most common cause of database server crashes due to Out-Of-Memory (OOM) errors.[12]
    - **Workload-Specific Tuning:**
        - **OLTP (Online Transaction Processing):** Characterized by many (e.g., 500+) simple, concurrent connections. `work_mem` _must_ be set low.  **Recommendation:** 16MB - 64MB.[4]
        - **OLAP (Online Analytical Processing):** Characterized by few (e.g., 5-10) complex, long-running queries. Connections are low, so `work_mem` can be high. **Recommendation:** 128MB - 256MB, or even higher.[4]
    - **Monitoring:** Use `EXPLAIN (ANALYZE)` on slow queries. If the output shows "Sort Method: external merge Disk:..." or "Hash Join... spill", it means `work_mem` is too low _for that query_. [8]


### 4. maintenance_work_mem

- **Purpose:** Allocates a large, dedicated chunk of memory for maintenance tasks: `VACUUM`, `CREATE INDEX`, `ALTER TABLE ADD FOREIGN KEY`, and restoring database dumps.[5]
- **Heuristics & Rationale:**
    - Because these operations are infrequent, it is safe to set this value significantly higher than `work_mem`.[9] A larger value can dramatically speed up index creation and vacuuming.[12]
    - **Recommendation:** 512MB - 2GB is a common, effective range.[4]

    - **The `autovacuum_work_mem` Trap:** The setting `autovacuum_work_mem` (discussed later) defaults to -1, which means autovacuum workers will _inherit_ the value of `maintenance_work_mem`.9 If `maintenance_work_mem` is set to 2 GB for manual tasks and `autovacuum_max_workers` is 3, the system could suddenly allocate 6 GB of RAM for a routine autovacuum.

    - **Best Practice:** Always set `autovacuum_work_mem` to its own, explicit value (e.g., 512MB) to decouple it from the high values used for manual maintenance.

- **Tier-Based Examples:**
    - **Small (16 GiB RAM):** `maintenance_work_mem = 512MB`
    - **Medium (64 GiB RAM):** `maintenance_work_mem = 1GB`
    - **Large (256 GiB RAM):** `maintenance_work_mem = 2GB`

## B. Concurrency and Parallelism (vCPU-Dependent)

These settings control how many processes PostgreSQL can use, both for client connections and internal background tasks. They must be scaled to the vCPU count of the cloud instance.

### 1. max_connections

- **Purpose:** Sets the maximum number of concurrent client connections allowed.15
- **Heuristics & Rationale:**
    - This is a common "trap" parameter. The default (e.g., 100) is often increased, but this is detrimental to performance. PostgreSQL's process-per-connection architecture does not scale well to thousands of connections.16 Each connection consumes memory (for `work_mem`, etc.) and CPU (for context switching).
    - **Best Practice:** Use an external connection pooler (e.g., PgBouncer).16 The application connects to the pooler (which can handle thousands of connections), and the pooler maintains a small, efficient pool of connections to the database.
    - **Recommendation:** Set `max_connections` on the database to a low value to serve the pooler, not the application. A common heuristic is `GREATEST(4 * vCPU, 100)`.15

### 2. max_worker_processes

- **Purpose:** This is the _master limit_ for _all_ background worker processes that PostgreSQL can fork.9 This single pool is used for parallel query workers, autovacuum workers, and logical replication workers.9
- **Heuristics & Rationale:** This value _must_ be at least the sum of `max_parallel_workers`, `autovacuum_max_workers`, and `max_logical_replication_workers`.
- **Recommendation:** A safe and effective default is to set this equal to the number of vCPUs.17

- **Tier-Based Examples:**
    - **Small (2 vCPU):** `max_worker_processes = 2` 17
    - **Medium (8 vCPU):** `max_worker_processes = 8` 17
    - **Large (32 vCPU):** `max_worker_processes = 32` 17

### 3. max_parallel_workers
- **Purpose:** Sets the maximum number of workers that can be active _in total, across all queries_ to support parallel query execution.4
- **Heuristics & Rationale:** This value is capped by `max_worker_processes`.9 For a mixed (OLTP/OLAP) workload, setting this equal to the total number of vCPUs is a good default.17

- **Tier-Based Examples:**
    - **Small (2 vCPU):** `max_parallel_workers = 2` 17
    - **Medium (8 vCPU):** `max_parallel_workers = 8` 17
    - **Large (32 vCPU):** `max_parallel_workers = 32` 17


### 4. max_parallel_workers_per_gather
- **Purpose:** This limits the number of parallel workers that can be used by a _single_ query node (a "gather" node).13
- **Heuristics & Rationale:**
    - Setting this equal to `max_parallel_workers` is dangerous. It would allow a single, complex analytical query to consume _all_ available parallel workers, starving all other concurrent queries.
    - This parameter limits the "blast radius" of a single runaway query.
    - **Recommendation:** Set this to half the number of vCPUs.17 This allows at least two complex queries to run fully in parallel, or one to run while leaving other workers free for smaller parallel tasks.
- **Tier-Based Examples:**
    - **Small (2 vCPU):** `max_parallel_workers_per_gather = 1` 17
    - **Medium (8 vCPU):** `max_parallel_workers_per_gather = 4` 17
    - **Large (32 vCPU):** `max_parallel_workers_per_gather = 16` 17

## C. Checkpoint and Write-Ahead Log (WAL) Management

Tuning the WAL and checkpoint process is essential for managing I/O, particularly for high-write OLTP workloads.8 A checkpoint is an I/O-intensive operation where all "dirty" data pages from `shared_buffers` are flushed to disk.

### 1. max_wal_size & min_wal_size

- **Purpose:** `max_wal_size` defines a _soft limit_ on the total size of WAL files that can accumulate before a checkpoint is _forced_.18
- **Heuristics & Rationale:**
    - The default (1 GB) is _far too low_ for any production write-heavy workload.18
    - On a busy system, this 1 GB limit can be filled in seconds, forcing a checkpoint. The system then writes more, fills the 1 GB again, and forces _another_ checkpoint. This results in the log message `LOG: checkpoints are occurring too frequently`.19
    - This behavior creates "bursty I/O" 20, where the disk is constantly being hammered by frantic checkpointing.
    - **The Goal:** The goal is to make checkpoints infrequent and _time-based_ (triggered by `checkpoint_timeout`), not _size-based_ (triggered by `max_wal_size`).21
    - **Recommendation:** Set `max_wal_size` high enough that it is _never_ hit during normal peak load. Values from 2-4 GB 4 to "dozens of GB" 22 are recommended. `min_wal_size` should also be raised to ensure WAL files are not recycled too quickly (e.g., to 1-2 GB 4).
- **Tier-Based Examples (Write-Heavy):**
    - **Small (16 GiB RAM):** `max_wal_size = 4GB`
    - **Medium (64 GiB RAM):** `max_wal_size = 16GB`
    - **Large (256 GiB RAM):** `max_wal_size = 32GB`
