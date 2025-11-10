
### Database

One VM:
- m8g.4xl = 16 CPU and 64GB RAM
- 450GB SSD storage @ 500 MiB/s (12K IOPS)
- OLTP workloads
- No pooling, 10 connections from application/service
- No replica

---

### VM Example: Adjusted Configuration

- **Instance:** m8g.4xl (16 vCPU, 64GB RAM)
- **Storage:** 2TiB SSD
- **Workload:** OLTP, **40 connections (no pooler)**, has replica
- `work_mem` , or connection working memory must be tuned based on `max_connections`. As we don't use a pooler, we can get this higher than typical.


| **Parameter**                          | **Default Value**                    | **Recommended Value** | **Rationale**                                                                         |
| -------------------------------------- | ------------------------------------ | --------------------- | ------------------------------------------------------------------------------------- |
| **`shared_buffers`**                   | 128MB                                | 16GB                  | 25% of 64GB RAM                                                                       |
| **`effective_cache_size`**             | 4GB                                  | 48GB                  | 75% of 64GB RAM [1]                                                                   |
| **`work_mem`**                         | 4MB                                  | **64MB**              | **(Adjusted)** Standard OLTP, very safe for 40 connections [4]                        |
| **`maintenance_work_mem`**             | 64MB                                 | 1GB                   | For efficient manual maintenance [4]                                                  |
| **`max_connections`**                  | 100                                  | **60**                | **(Adjusted)** 40 for app + 20 buffer for superuser/maintenance.                      |
| **`max_worker_processes`**             | 8                                    | 16                    | Match vCPU count [3]                                                                  |
| **`max_parallel_workers`**             | 8                                    | 16                    | Match vCPU count [3]                                                                  |
| **`max_parallel_workers_per_gather`**  | 2                                    | 8                     | Half of vCPUs [5]                                                                     |
| **`max_parallel_maintenance_workers`** | 2                                    | 8                     | Half of vCPUs [3]                                                                     |
| **`random_page_cost`**                 | 4.0                                  | 1.1                   | **Crucial for SSD storage** [6]                                                       |
| **`effective_io_concurrency`**         | 1                                    | 200                   | **Crucial for SSD storage** [6]                                                       |
| **`checkpoint_timeout`**               | 5min                                 | 5min                  | Standard for OLTP [4]                                                                 |
| **`max_wal_size`**                     | 1GB                                  | 16GB                  | Avoid I/O spikes from frequent checkpoints [8]                                        |
| **`min_wal_size`**                     | 80MB                                 | 2GB                   | Paired with `max_wal_size` [9]                                                        |
| **`checkpoint_completion_target`**     | 0.5                                  | 0.9                   | Smooth out checkpoint I/O [9]                                                         |
| **`autovacuum_max_workers`**           | 3                                    | 5                     | Increase from default 3 [12]                                                          |
| **`autovacuum_naptime`**               | 1min                                 | 30s                   | More responsive autovacuum launcher [13]                                              |
| **`autovacuum_vacuum_cost_limit`**     | -1 (inherits 200)                    | 2000                  | 10x default, allows workers to be more effective [15]                                 |
| **`autovacuum_work_mem`**              | -1 (inherits `maintenance_work_mem`) | 512MB                 | **Crucial**; prevents inheriting large `maintenance_work_mem` [3]                     |
| **`autovacuum_vacuum_scale_factor`**   | 0.2                                  | 0.05                  | More aggressive than default (0.2). _Large tables should be tuned individually._ [15] |
| **`wal_level`**                        | `replica`                            | `replica`             | **Required for replication**                                                          |
| **`max_wal_senders`**                  | 10                                   | 10                    | **Required for replication**; allows replica connections [4]                          |
| **`wal_keep_size`**                    | 0MB                                  | 16GB                  | **Required for replication**; WAL retention for replica recovery.                     |
| **`hot_standby`**                      | `on`                                 | `on`                  | This setting is applied _on the replica_ (m7g.2xl) to allow read queries.             |
