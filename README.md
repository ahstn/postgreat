# PostGreat - PostgreSQL Configuration Analyzer

A modern Rust-based tool for analyzing PostgreSQL configuration and providing evidence-based tuning recommendations based on official PostgreSQL documentation and best practices.

## Features

- **Evidence-Based Recommendations**: All suggestions are based on your documentation of PostgreSQL best practices
- **Compute-Specific Tuning**: Accepts compute specifications (vCPU/RAM) for tailored recommendations
- **Multiple Output Formats**: Markdown, JSON, and plain text reports
- **Batch Analysis**: Analyze multiple databases from a YAML configuration file
- **Comprehensive Coverage**: Memory, concurrency, WAL, planner, autovacuum, and logging

## Installation

### Prerequisites

- Rust 1.70 or later
- PostgreSQL 12 or later (for the database being analyzed)

### Build from Source

```bash
git clone <repository-url>
cd postgreat
cargo build --release
```

The binary will be available at `target/release/postgreat`.

## Usage

### Analyze a Single Database

> [!NOTE]
>
> See [PostgreSQL Permissions](#postgresql-permission) for a secure, specific user

```bash
postgreat analyze \
  -h localhost \
  -p 5432 \
  -d mydatabase \
  -u postgres \
  -P password \
  --compute "8vCPU-64GB"
```

Or using environment variables:

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DATABASE=mydatabase
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=password

postgreat analyze --compute "8vCPU-64GB"
```

### Analyze Workload (Slow Queries & Index Candidates)

Requires `pg_stat_statements` to be installed and usable on the target database. If the extension
exists but PostgreSQL was not restarted with `shared_preload_libraries = 'pg_stat_statements'`,
PostGreat will return a warning-only workload result instead of failing the command.
The current workload report uses cumulative `pg_stat_statements` counters, so it does not support
an exact historical lookback window like "last 1 hour". Results reflect activity since the
statement's stats were first collected or `pg_stat_statements` was last reset; true time-windowed
reporting requires external snapshots or a bucketed extension such as `pg_stat_monitor`.
The report now includes workload metadata and coverage notes so you can see the effective scope
(`pg_stat_statements` since reset), entry evictions, query-text visibility, parse coverage, and why
an index candidate was emitted or suppressed.

```bash
postgreat workload \
  -h localhost \
  -p 5432 \
  -d mydatabase \
  -u postgres \
  -P password \
  --limit 20 \
  --min-calls 10
```

### Compute Specifications

PostGreat cannot always read host hardware (e.g., AWS RDS instances), so hardware-aware recommendations require the `--compute` flag. Provide the instance shape using one of the formats below:

1. **Tier names**: `small`, `medium`, `large`
   - `small`: 2 vCPU, 16GB RAM
   - `medium`: 8 vCPU, 64GB RAM
   - `large`: 32 vCPU, 256GB RAM

2. **Exact specs**: `8vCPU-64GB`, `4vcpu-16gb` (case-insensitive). The format is `<vCPU>vCPU-<memory>GB`.

### Analyze Multiple Databases

Create a YAML configuration file:

```yaml
# db-config.yaml
- host: db1.example.com
  port: 5432
  database: production_db
  username: postgres
  password: secret
  compute:
    vcpu: 8
    memory_gb: 64

- host: db2.example.com
  port: 5432
  database: analytics_db
  username: postgres
  password: secret
  compute:
    vcpu: 32
    memory_gb: 256
```

Then run:

```bash
postgreat config -c db-config.yaml
```

### Output Formats

Choose from three output formats:

```bash
# Markdown (default) - recommended for documentation
postgreat analyze ... -f markdown

# JSON - for programmatic consumption
postgreat analyze ... -f json

# Plain text - for quick review
postgreat analyze ... -f text
```

### Verbosity

Control logging output:

```bash
postgreat analyze ...          # Warning level only
postgreat analyze ... -v       # Info level
postgreat analyze ... -vv      # Debug level
postgreat analyze ... -vvv     # Trace level
```

## Analysis Categories

PostGreat analyzes six key areas:

### 1. Memory Configuration
- `shared_buffers` (25% of RAM, capped at 8GB for large systems)
- `effective_cache_size` (75% of RAM)
- `work_mem` (OLTP vs OLAP tuning)
- `maintenance_work_mem` (512MB-2GB based on system size)
- `wal_buffers` (16MB for high-write workloads)

### 2. Concurrency and Parallelism
- `max_connections` (use connection poolers)
- `max_worker_processes` (match vCPU count)
- `max_parallel_workers` (match vCPU count)
- `max_parallel_workers_per_gather` (half of vCPUs)
- `max_parallel_maintenance_workers` (half of vCPUs)

### 3. WAL and Checkpoint Management
- `max_wal_size` (2-32GB depending on system size)
- `checkpoint_timeout` (5min for OLTP, 15-30min for OLAP)
- `checkpoint_completion_target` (0.9 for I/O smoothing)

### 4. Query Planner Cost Model
- `random_page_cost` (1.1 for SSD/NVMe, critically important)
- `effective_io_concurrency` (200 for modern storage)

### 5. Autovacuum Configuration
- `autovacuum_max_workers` (5+ for high-churn systems)
- `autovacuum_vacuum_cost_limit` (2000, 10x default)
- `autovacuum_work_mem` (512MB, explicit setting required)
- Per-table tuning for large tables

### 6. Logging and Diagnostics
- `log_min_duration_statement` (1000ms to find slow queries)
- `log_lock_waits` (essential for diagnosing contention)

### 7. Table and Index Health
- Monitors table bloat via `pg_stat_user_tables`, correlating dead tuple ratios with the last autovacuum run
- Highlights sequential scan hotspots where large tables rely on sequential reads instead of indexes
- Surfaces unused or inefficient indexes (low selectivity, failed index-only scans) using `pg_stat_user_indexes`, `pg_index`, and `pg_constraint`, following the guidance in `docs/6 - Table and Index Health.md`

## Example Output

See [examples/report-example.md](examples/report-example.md) for a sample report generated by PostGreat.

## PostgreSQL Permissions

Rather than re-using a pre-existing user, consider creating a user with minimal read, and no write access:

```sql
-- Run as a superuser/admin role.
-- Replace: mydatabase, postgreat_ro, and password.

CREATE ROLE postgreat_ro
  LOGIN
  PASSWORD 'REPLACE_WITH_STRONG_PASSWORD'
  NOSUPERUSER
  NOCREATEDB
  NOCREATEROLE
  NOREPLICATION
  NOBYPASSRLS;

GRANT CONNECT ON DATABASE mydatabase TO postgreat_ro;

-- Needed for this repo's reads of pg_settings, pg_stat_* views, and pg_stat_statements visibility.
GRANT pg_read_all_settings TO postgreat_ro;
GRANT pg_read_all_stats TO postgreat_ro;

-- Optional safety default (not a hard security boundary, but useful guardrail).
ALTER ROLE postgreat_ro IN DATABASE mydatabase
  SET default_transaction_read_only = on;

-- Execute remaining statements while connected to the target DB.
-- psql: \connect mydatabase

DO $$
DECLARE s text;
BEGIN
  FOR s IN
    SELECT nspname
    FROM pg_namespace
    WHERE nspname NOT IN ('pg_catalog', 'information_schema')
      AND nspname NOT LIKE 'pg_toast%'
      AND nspname NOT LIKE 'pg_temp_%'
  LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO postgreat_ro', s);
    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO postgreat_ro', s);
  END LOOP;
END $$;

-- Optional: keep future objects readable.
-- Run as each schema owner that creates tables/views.
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO postgreat_ro;
```

## Development

### Project Structure

```
src/
├── main.rs              # CLI entry point
├── lib.rs               # Library exports
├── config.rs            # Configuration parsing
├── checker.rs           # Database connection and analysis orchestration
├── models.rs            # Data structures
├── reporter.rs          # Output formatting
└── analysis/            # Analysis modules by category
    ├── memory.rs
    ├── concurrency.rs
    ├── wal.rs
    ├── planner.rs
    ├── autovacuum.rs
    └── logging.rs
```

### Running Tests

Fast test suite:

```bash
cargo test
```

Live PostgreSQL integration tests:
- Require Docker and are ignored by default.
- Start a real PostgreSQL instance with `testcontainers`, seed it from `tests/_data/`, and invoke the `postgreat` binary end-to-end.
- Cover five scenarios:
  - `it_analyze`: seeded `analyze --format json` run with table/index-health findings
  - `it_workload`: happy-path `workload --format json` run with `pg_stat_statements`
  - `it_workload_unavailable`: extension missing and installed-but-not-preloaded behavior
  - `it_workload_visibility`: reduced query-text visibility without `pg_read_all_stats`
  - `it_workload_dealloc`: `pg_stat_statements` entry eviction/deallocation warnings

Run a single live test against PostgreSQL 18:

```bash
POSTGREAT_TEST_PG_VERSION=18 cargo test --test it_workload -- --ignored --test-threads=1
```

Run the full live suite against PostgreSQL 18:

```bash
POSTGREAT_TEST_PG_VERSION=18 cargo test --test it_analyze -- --ignored --test-threads=1
POSTGREAT_TEST_PG_VERSION=18 cargo test --test it_workload -- --ignored --test-threads=1
POSTGREAT_TEST_PG_VERSION=18 cargo test --test it_workload_unavailable -- --ignored --test-threads=1
POSTGREAT_TEST_PG_VERSION=18 cargo test --test it_workload_visibility -- --ignored --test-threads=1
POSTGREAT_TEST_PG_VERSION=18 cargo test --test it_workload_dealloc -- --ignored --test-threads=1
```

Swap `POSTGREAT_TEST_PG_VERSION=14` to run the same suite against PostgreSQL 14.

### Code Formatting and Linting

```bash
cargo fmt
cargo clippy
```

## Contributing

Contributions are welcome! Please ensure:

1. All recommendations are based on official PostgreSQL documentation or well-established best practices
2. Add tests for new analysis logic
3. Update documentation as needed
4. Follow Rust naming and style conventions
5. For workload-analysis changes touching `pg_stat_statements`, SQL parsing, or index coverage:
   - confirm cumulative vs real-time behavior explicitly
   - document version-specific columns and fallback behavior
   - check privilege-dependent visibility (`pg_read_all_stats`)
   - verify index semantics for partial, expression, invalid, `INCLUDE`, and non-B-tree indexes
   - cover ambiguous unqualified table names in tests
6. For PostgreSQL-semantic changes, include at least one official PostgreSQL doc link in the PR description and one regression test for the behavior being changed

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Inspired by [postgresqltuner.pl](https://github.com/jfcoz/postgresqltuner)
- Based on PostgreSQL official documentation and best practices from your research
- Rust ecosystem libraries: sqlx, clap, tokio, serde, snafu, tracing

## References

- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [PostgreSQL Wiki: Tuning Your PostgreSQL Server](https://wiki.postgresql.org/wiki/Tuning_Your_PostgreSQL_Server)
- RDS-specific considerations for compute specification since system stats aren't available
