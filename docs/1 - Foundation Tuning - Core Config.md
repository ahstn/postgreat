# Foundational Tuning: Core Configuration / Parameter Groups

**Overview:**

The performance and stability of a PostgreSQL instance are predominantly dictated by the settings within its `postgresql.conf` configuration file. These parameters govern memory allocation, concurrency, I/O behavior, and query planning. Proper tuning requires aligning these settings with the available hardware (RAM, vCPU), storage type (SSD, NVMe), and the specific database workload (e.g., OLTP vs. OLAP).[1]

**Table of Contents:**
- [A. Memory Allocation Strategy](#a-memory-allocation-strategy-ram-dependent)
  - [shared_buffers](#1-shared_buffers)
  - [effective_cache_size](#2-effective_cache_size)
  - [work_mem](#3-work_mem)
  - [maintenance_work_mem](#4-maintenance_work_mem)
- [B. Concurrency and Parallelism (vCPU-Dependent)](#b-concurrency-and-parallelism-vcpu-dependent)
  - [max_connections](#1-max_connections)
  - [max_worker_processes](#2-max_worker_processes)
  - [max_parallel_workers](#3-max_parallel_workers)
  - [max_parallel_workers_per_gather](#4-maintenance_work_mem)
- [C. Query Planner Cost Model (Storage-Dependent)](#c-query-planner-cost-model-storage-dependent)
  - [random_page_cost](#1-random_page_cost)
  - [effective_io_concurrency](#2-effective_io_concurrency)
- [D. Checkpoint and Write-Ahead Log (WAL) Management](#d-checkpoint-and-write-ahead-log-wal-management)
  - [max_wal_size & min_wal_size](#1-max_wal_size--min_wal_size)
- [References](#references)

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
    - **The `autovacuum_work_mem` Trap:** The setting `autovacuum_work_mem` (discussed later) defaults to -1, which means autovacuum workers will _inherit_ the value of `maintenance_work_mem`.[9] If `maintenance_work_mem` is set to 2 GB for manual tasks and `autovacuum_max_workers` is 3, the system could suddenly allocate 6 GB of RAM for a routine autovacuum.
    - **Best Practice:** Always set `autovacuum_work_mem` to its own, explicit value (e.g., 512MB) to decouple it from the high values used for manual maintenance.
- **Tier-Based Examples:**
    - **Small (16 GiB RAM):** `maintenance_work_mem = 512MB`
    - **Medium (64 GiB RAM):** `maintenance_work_mem = 1GB`
    - **Large (256 GiB RAM):** `maintenance_work_mem = 2GB`

## B. Concurrency and Parallelism (vCPU-Dependent)

These settings control how many processes PostgreSQL can use, both for client connections and internal background tasks. They must be scaled to the vCPU count of the cloud instance.

### 1. max_connections

- **Purpose:** Sets the maximum number of concurrent client connections allowed.[15]
- **Heuristics & Rationale:**
    - This is a common "trap" parameter. The default (e.g., 100) is often increased, but this is detrimental to performance. PostgreSQL's process-per-connection architecture does not scale well to thousands of connections.16 Each connection consumes memory (for `work_mem`, etc.) and CPU (for context switching).
    - **Best Practice:** Use an external connection pooler (e.g., PgBouncer).16 The application connects to the pooler (which can handle thousands of connections), and the pooler maintains a small, efficient pool of connections to the database.
    - **Recommendation:** Set `max_connections` on the database to a low value to serve the pooler, not the application. A common heuristic is `GREATEST(4 * vCPU, 100)`.[15]

### 2. max_worker_processes

- **Purpose:** This is the _master limit_ for _all_ background worker processes that PostgreSQL can fork.9 This single pool is used for parallel query workers, autovacuum workers, and logical replication workers.[9]
- **Heuristics & Rationale:** This value _must_ be at least the sum of `max_parallel_workers`, `autovacuum_max_workers`, and `max_logical_replication_workers`.
- **Recommendation:** A safe and effective default is to set this equal to the number of vCPUs.[17]

- **Tier-Based Examples:**
    - **Small (2 vCPU):** `max_worker_processes = 2`
    - **Medium (8 vCPU):** `max_worker_processes = 8`
    - **Large (32 vCPU):** `max_worker_processes = 32`

### 3. max_parallel_workers
- **Purpose:** Sets the maximum number of workers that can be active _in total, across all queries_ to support parallel query execution.[4]
- **Heuristics & Rationale:** This value is capped by `max_worker_processes`.9 For a mixed (OLTP/OLAP) workload, setting this equal to the total number of vCPUs is a good default.[17]

- **Tier-Based Examples:**
    - **Small (2 vCPU):** `max_parallel_workers = 2`
    - **Medium (8 vCPU):** `max_parallel_workers = 8`
    - **Large (32 vCPU):** `max_parallel_workers = 32`


### 4. max_parallel_workers_per_gather
- **Purpose:** This limits the number of parallel workers that can be used by a _single_ query node (a "gather" node).[13]
- **Heuristics & Rationale:**
    - Setting this equal to `max_parallel_workers` is dangerous. It would allow a single, complex analytical query to consume _all_ available parallel workers, starving all other concurrent queries.
    - This parameter limits the "blast radius" of a single runaway query.
    - **Recommendation:** Set this to half the number of vCPUs.[17] This allows at least two complex queries to run fully in parallel, or one to run while leaving other workers free for smaller parallel tasks.
- **Tier-Based Examples:**
    - **Small (2 vCPU):** `max_parallel_workers_per_gather = 1`
    - **Medium (8 vCPU):** `max_parallel_workers_per_gather = 4`
    - **Large (32 vCPU):** `max_parallel_workers_per_gather = 16`

## C. Query Planner Cost Model (Storage-Dependent)

These settings are _mandatory_ for tuning on modern cloud VMs, which exclusively use SSD or NVMe storage. The defaults are optimized for spinning HDDs and are dangerously suboptimal on SSDs.

### 1. random_page_cost

- **Purpose:** The planner's _cost estimate_ for fetching a single, non-sequential (random) disk page.[5]
- **Heuristics & Rationale:**
    - **HDD (Default):** `4.0`. This tells the planner that a random read (like an index scan) is 4x more expensive than a sequential read (`seq_page_cost`, default 1.0).[27]
    - **SSD / NVMe:** On modern storage, the time penalty for a random read versus a sequential read is negligible.[27]
    - **The Impact:** Leaving the default at 4.0 on an SSD _cripples_ the database. The planner will _wrongly_ conclude that full-table sequential scans are cheaper than index scans. It will _ignore_ your carefully created indexes.
    - **Recommendation:** Set to `1.0` or `1.1`.[11] This, combined with a high `effective_cache_size`, is the primary command to "tell" the planner to trust and use indexes.

### 2. effective_io_concurrency

- **Purpose:** Estimates the number of _concurrent I/O operations_ the underlying storage system can handle simultaneously.[27] This is primarily used to optimize bitmap heap scans.

- **Heuristics & Rationale:**
    - **HDD:** `1` or `2` (for a single disk or simple RAID).[27]
    - **SSD / NVMe / Cloud Storage:** These systems can handle massive concurrency.
    - **Recommendation:** A value of `200` is a common starting point for modern SSDs.[27] Benchmarks show significant performance gains by increasing this, though setting it too high can saturate I/O, so it should be monitored.[29]
    - **Tier-Based Recommendation:** Start with `200` for all tiers using SSD/NVMe.

## D. Checkpoint and Write-Ahead Log (WAL) Management

Tuning the WAL and checkpoint process is essential for managing I/O, particularly for high-write OLTP workloads.8 A checkpoint is an I/O-intensive operation where all "dirty" data pages from `shared_buffers` are flushed to disk.

### 1. max_wal_size & min_wal_size

- **Purpose:** `max_wal_size` defines a _soft limit_ on the total size of WAL files that can accumulate before a checkpoint is _forced_.[18]
- **Heuristics & Rationale:**
    - The default (1 GB) is _far too low_ for any production write-heavy workload.[18]
    - On a busy system, this 1 GB limit can be filled in seconds, forcing a checkpoint. The system then writes more, fills the 1 GB again, and forces _another_ checkpoint. This results in the log message `LOG: checkpoints are occurring too frequently`.[19]
    - This behavior creates "bursty I/O" [20], where the disk is constantly being hammered by frantic checkpointing.
    - **The Goal:** The goal is to make checkpoints infrequent and _time-based_ (triggered by `checkpoint_timeout`), not _size-based_ (triggered by `max_wal_size`).[21]
    - **Recommendation:** Set `max_wal_size` high enough that it is _never_ hit during normal peak load. Values from 2-4 GB 4 to "dozens of GB" 22 are recommended. `min_wal_size` should also be raised to ensure WAL files are not recycled too quickly (e.g., to 1-2 GB [4]).
- **Tier-Based Examples (Write-Heavy):**
    - **Small (16 GiB RAM):** `max_wal_size = 4GB`
    - **Medium (64 GiB RAM):** `max_wal_size = 16GB`
    - **Large (256 GiB RAM):** `max_wal_size = 32GB`

## References

- [Tuning max_wal_size in PostgreSQL - EDB Postgres][21]
- [Postgres 12 Tuning Configuration for heavily transaction server, have I set values too low?][22]
- [04 - PostgreSQL 17 Performance Tuning: Checkpoints Explained | by Jeyaram Ayyalusamy][23]
- [PostgreSQL High Checkpoint Time - Doctor Droid][24]
- [Documentation: 18: 28.5. WAL Configuration - PostgreSQL][25]
- [PostgreSQL: Experiences and tuning recommendations on Linux on IBM Z][26]
- [PostgreSQL Performance Tuning Settings - Vlad Mihalcea][27]
- [Tuning PostgreSQL performance for SSD | Frederik Himpe][28]
- [Tuning the PostgreSQL “effective_io_concurrency” Parameter ...][29]
- [PostgreSQL Tips, Tricks, and Tuning - Andrew Atkinson][30]

[1]: https://www.percona.com/blog/tuning-postgresql-database-parameters-to-optimize-performance/
[2]: https://aws.amazon.com/compare/the-difference-between-olap-and-oltp/
[3]: https://www.postgresql.org/docs/current/runtime-config-resource.html
[4]: https://www.mydbops.com/blog/postgresql-parameter-tuning-best-practices
[5]: https://www.tigerdata.com/learn/postgresql-performance-tuning-key-parameters
[6]: https://vladmihalcea.com/postgresql-performance-tuning-settings/
[8]: https://www.postgresql.org/docs/current/runtime-config-wal.html
[9]: https://www.crunchydata.com/blog/tuning-your-postgres-database-for-high-write-loads
[10]: https://www.geeksforgeeks.org/postgresql/postgresql-memory-management/
[11]: https://dev.to/sudo_anuj/mastering-postgresql-performance-linux-tuning-and-database-optimization-2dk8
[12]: https://medium.com/@jramcloud1/08-postgresql-17-complete-tuning-guide-for-vacuum-autovacuum-aa36b945a7cf
[13]: https://reintech.io/blog/postgresql-database-tuning-olap-vs-oltp
[14]: https://aws.amazon.com/blogs/database/understanding-autovacuum-in-amazon-rds-for-postgresql-environments/
[15]: https://pganalyze.com/blog/5mins-postgres-tuning-vacuum-autovacuum
[16]: https://www.instaclustr.com/education/postgresql/postgresql-tuning-6-things-you-can-do-to-improve-db-performance/
[17]: https://www.tigerdata.com/learn/postgresql-performance-tuning-key-parameters
[18]: https://www.postgresql.org/docs/current/runtime-config-wal.html
[19]: https://www.crunchydata.com/blog/tuning-your-postgres-database-for-high-write-loads
[20]: https://dev.to/shiviyer/how-does-the-wrong-checkpointing-configuration-in-postgresql-affect-the-performance-31d8
[21]: https://www.enterprisedb.com/blog/tuning-maxwalsize-postgresql
[27]: https://vladmihalcea.com/postgresql-performance-tuning-settings/
[29]: https://shaneborden.com/2022/12/27/tuning-the-postgresql-effective_io_concurrency-parameter/
[31]: https://www.bytebase.com/blog/postgres-create-index-concurrently/
[32]: https://dev.to/digitalpollution/overview-of-postgresql-indexing-lpi
[33]: https://www.postgresql.org/docs/current/sql-createindex.html
[34]: https://www.alexstoica.com/blog/create-index-concurrently-locks
[35]: https://www.postgresql.org/docs/current/indexes-partial.html
[36]: https://stackoverflow.com/questions/18915945/indexing-items-marked-as-deleted
[37]: https://medium.com/@sjksingh/indexing-isnt-free-a-playbook-for-surviving-over-indexing-in-postgresql-07c7a07299c8
[38]: https://www.crunchydata.com/blog/postgres-indexing-when-does-brin-win
[39]: https://www.cybertec-postgresql.com/en/btree-vs-brin-2-options-for-indexing-in-postgresql-data-warehouses/
[40]: https://www.reddit.com/r/PostgreSQL/comments/ujq2wg/speeding_up_an_expensive_postgresql_query_btree/
[41]: https://stackoverflow.com/questions/35466173/performance-effect-of-include-column-in-index
[42]: https://www.postgresql.org/docs/current/routine-vacuuming.html
[43]: https://support.atlassian.com/atlassian-knowledge-base/kb/optimize-and-improve-postgresql-performance-with-vacuum-analyze-and-reindex/
[44]: https://medium.com/@nakulmitra2114/postgresql-performance-optimization-cleaning-dead-tuples-reindexing-9b1346408b97
[45]: https://dev.to/sandeepkumardev/how-to-handle-dead-tuples-in-postgresql-54m1
[46]: https://www.postgresql.fastware.com/pzone/2025-03-improving-postgresql-efficiency-by-handling-dead-tuples
[47]: https://www.postgresql.org/docs/current/sql-vacuum.html
[48]: https://www.reddit.com/r/PostgreSQL/comments/1dmfohx/vacuum_full_analyze_much_better_than_vacuum/
[49]: https://medium.com/@aminechichi99/the-silent-killer-of-db-performance-demystifying-table-bloat-in-postgresql-84773ddaf078
[50]: https://www.postgresql.org/docs/current/progress-reporting.html
[51]: https://opensource-db.com/index-bloat-management-in-postgresql/
[52]: https://www.cloudraft.io/blog/tuning-postgresql-for-write-heavy-workloads
[53]: https://www.enterprisedb.com/blog/autovacuum-tuning-basics
[54]: https://medium.com/@jramcloud1/08-postgresql-17-complete-tuning-guide-for-vacuum-autovacuum-aa36b945a7cf
[55]: https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/how-to-autovacuum-tuning
[56]: https://www.percona.com/blog/tuning-autovacuum-in-postgresql-and-autovacuum-internals/
[57]: https://pganalyze.com/blog/5mins-postgres-tuning-vacuum-autovacuum
[58]: https://pganalyze.com/docs/vacuum-advisor/how-does-the-vacuum-cost-model-work
[59]: https://stackoverflow.com/questions/63671302/what-is-autovacuum-vacuum-cost-delay-in-autovacuum-in-postgresql
[60]: https://aws.amazon.com/blogs/database/parallel-vacuuming-in-amazon-rds-for-postgresql-and-amazon-aurora-postgresql/
[61]: https://www.citusdata.com/blog/2022/07/28/debugging-postgres-autovacuum-problems-13-tips/
[62]: https://dba.stackexchange.com/questions/302507/find-bloated-tables-and-indexes-in-postgresql-without-extensions
[63]: https://medium.com/@anasanjaria/how-to-determine-unused-index-in-postgresql-6af846686a3
[64]: https://dba.stackexchange.com/questions/137255/find-unused-indexes
[65]: https://www.tigerdata.com/learn/best-practices-for-postgres-database-replication
[66]: https://medium.com/@wasiualhasib/postgresql-hybrid-transactional-analytical-processing-using-25292f106239
[67]: https://medium.com/simform-engineering/unlocking-performance-a-deep-dive-into-table-partitioning-in-postgresql-3f5b8faa025f
[68]: https://www.postgresql.org/docs/current/ddl-partitioning.html
[69]: https://www.tigerdata.com/learn/when-to-consider-postgres-partitioning
[70]: https://www.prefect.io/blog/database-partitioning-prod-postgres-without-downtime
[71]: https://www.percona.com/blog/tune-linux-kernel-parameters-for-postgresql-optimization/
[72]: https://medium.com/@jramcloud1/postgresql-17-kernel-tuning-guide-managing-system-parameters-for-optimal-performance-fe097de1dcdb
[73]: https://www.postgresql.org/docs/current/kernel-resources.html
[74]: https://www.redhat.com/en/blog/postgresql-load-tuning-red-hat-enterprise-linux
[75]: https://www.datacamp.com/doc/postgresql/dropping-unused-indexes
