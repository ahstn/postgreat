## PostgreSQL Connection Poolers

This overview is for the common question behind most pooler decisions: do you only need connection multiplexing, or do you want the proxy layer to also understand replicas, routing, and multi-tenant scale? For most teams, that question matters more than micro-benchmarks.[1][2][3][4][5]

Facts below were checked against upstream docs, release notes, and the GitHub API as of 2026-11-07. Pooler projects move quickly, so re-check versions and feature flags before relying on them.

**Why a pooler at all (PostgreSQL side).** PostgreSQL uses a "process per user" model: every client connection gets its own backend process.[32] `max_connections` defaults to typically 100, can only be changed at server start, and PostgreSQL sizes some resources, including shared memory, directly from it.[33] A pooler lets many clients share a much smaller number of backends.

### A. Quick Positioning

- **PgBouncer:** Best default when you want a small, well-understood pooler and nothing more.[1][6]
- **PgCat:** A "PgBouncer plus routing" option with read balancing, health-check failover, metrics, and optional sharding. Upstream has had no commits since February 27, 2025 and no application release since v1.2.0 (August 30, 2024), so treat it as unmaintained unless that changes.[2][11][34]
- **PgDog:** The most ambitious proxy in this group: pooling, read/write splitting, replica-lag and promotion-aware routing, prepared statements in transaction mode, and sharding. The open source edition is AGPL-3.0; a closed-source commercial Enterprise edition also exists.[3][13][29]
- **Odyssey:** A multi-threaded C pooler/router from Yandex with PAM/LDAP auth and active development (v1.5.2 released September 13, 2026). It runs only on Linux. It is broader than a minimal pooler but is not a sharding layer.[4][17]
- **Supavisor:** Best suited to multi-tenant and serverless-style fan-in, where the core problem is too many transient clients rather than replica routing. It is a pooler first, not a general-purpose query router.[5][10]

Licences (from each repository, as of 2026-11-07): PgBouncer ISC, PgCat MIT, PgDog AGPL-3.0 (plus a commercial Enterprise edition), Odyssey BSD-3-Clause, Supavisor Apache-2.0.[29][34][35][36][37][38][39]

### B. Feature Comparison

#### PgBouncer

- Supports session, transaction, and statement pooling, plus online reconfiguration (`RELOAD`) for most settings.[1][31]
- Online restart (`-R`, "takeover") was **removed in 1.26.0** (September 23, 2026). The project now recommends rolling restarts of several processes sharing a port with `so_reuseport`, which needs clients to reconnect to the remaining processes.[16][31] The features page still lists online restart, so trust the changelog here.[1]
- Keeps the clearest "minimal pooler" contract: about 2 kB of memory per connection by default, a single-threaded process (use `so_reuseport` to run several instances on one port), and an admin console with `SHOW` commands.[1][6][31]
- Protocol-level named prepared statements work in transaction and statement pooling. Since 1.24.0 `max_prepared_statements` defaults to 200, so this is on by default; set it to 0 to disable it. SQL-level `PREPARE` / `EXECUTE` / `DEALLOCATE`, `SET`, `LISTEN`, `WITH HOLD` cursors, and session advisory locks still do not work in transaction pooling.[1][6][16]
- Recent releases: 1.25.0 (November 2025) added LDAP authentication, client-side direct TLS (PostgreSQL 17 style), `transaction_timeout`, and faster ad hoc SCRAM. 1.26.0 tracks all parameters PostgreSQL reports by default (including `search_path` on PostgreSQL 18), and 1.25.2 / 1.26.0 / 1.26.1 fixed several CVEs, so stay on the latest patch release.[16]
- It does **not** try to be a read/write router or failover proxy. If you need replica-aware routing, PgBouncer usually stays the pooling layer in front of something else, or is replaced by a more featureful proxy.[1]

#### PgCat

- Supports session and transaction pooling on a multi-threaded Tokio runtime, read-query load balancing (random or least-outstanding-connections), health-check-based failover, admin databases, Prometheus metrics, TLS, and live config reload. The README marks these as stable.[2][12]
- Uses a built-in query parser (`sqlparser` crate) to route `SELECT` traffic to replicas and other statements, plus explicit transactions, to the primary. Options include `primary_reads_enabled` and manual `SET SERVER ROLE` overrides.[2][12]
- Operational extras: auth passthrough via `auth_query` (MD5), mirroring, and plugins such as prewarmer, query logger, table access, and intercept.[2][12]
- Sharding (extended SQL syntax, comment/regex hints, automatic) and mirroring are all still labelled **Experimental** in the README.[2]
- In transaction mode, `SET` and session advisory locks are not supported (use `SET LOCAL` and `pg_advisory_xact_lock`). The README also says prepared statements are unsupported there, but v1.1.0 (July 2023) added prepared statement support, controlled by `prepared_statements_cache_size` (default 0, meaning disabled).[2][12][26]
- Client authentication is MD5 only; PgCat can use MD5 or SCRAM-SHA-256 towards PostgreSQL.[2]

#### PgDog

- PgDog is a pooler, load balancer, query router, and sharding proxy in one Rust project, with transaction mode enabled by default and optional session and statement modes.[3][13][21]
- Its standout feature is parser-driven routing. It uses `pg_query` (the PostgreSQL parser) and says it understands all valid PostgreSQL queries. It splits reads from writes behind one endpoint, sends explicit transactions (where `SELECT FOR UPDATE` normally lives) to the primary, and sends write CTEs to the primary.[20]
- It tracks session state in transaction mode: named prepared statements (global cache with renamed statements), `SET` commands and startup parameters, and advisory locks (it pins the server connection until the lock is released). Advisory lock detection is off by default in unsharded setups without the load balancer and needs `query_parser = "session_control_and_locks"`.[21][27]
- Other features: manual routing via query comments or `pgdog.role` / `pgdog.shard` parameters, `LISTEN`/`NOTIFY` in transaction mode, query mirroring, Rust plugins loaded at runtime, and metrics via the admin database, an OpenMetrics endpoint (off by default), and an OTEL exporter.[3][22][23][25][28]
- Failover is routing only. PgDog can detect a promoted replica and redirect writes, but it does not detect primary failure or call `pg_promote()`; use Patroni or a managed service for that, or you risk split brain. The replica-lag ban is documented as "new and experimental".[24]
- Trade-offs: AGPL-3.0 may be disqualifying for some teams (the commercial Enterprise edition is the alternative), and the project releases very often.[13][29]

#### Odyssey

- Odyssey describes itself as an advanced multi-threaded PostgreSQL connection pooler and request router, used in large production setups.[4]
- Worker threads handle auth and proxying while sharing global server pools; the README says this matters for SSL/TLS performance.[4]
- Transaction pooling tracks transaction state, can issue `Cancel` plus `Rollback` for abandoned transactions, and remembers the last owning client to reduce repeated setup work.[4]
- Auth and operations: per database/user pool settings, SSL/TLS, md5 and clear text, PAM and LDAP, and UUID-based logging.[4] Prometheus export is via a separate Go exporter in the repo that reads the Odyssey console; the docs call it "in developing stage".[30]
- Protocol-level prepared statements in transaction pooling (`pool_reserve_prepared_statement`) and standby-lag polling (watchdog query) arrived in 1.3 (June 2022).[9] Later releases add more replica-aware routing options (for example prefer-standby target session attributes in v1.5.2).[17] It is more replica-aware than PgBouncer, but it is not a sharding layer.
- Odyssey builds and runs only on Linux.[4]

#### Supavisor

- Supavisor is a cloud-native, multi-tenant pooler cluster written in Elixir. Its core job is to absorb very large numbers of client connections and map them onto a much smaller pool of database connections.[10]
- On Supabase's hosted platform, transaction mode is on port 6543 and session mode on port 5432. Supabase's FAQ says session mode, unlike transaction mode, supports prepared statements.[5]
- Upstream is moving: v2.7.0 (September 8, 2025) added named prepared statement support behind a feature flag, and v2.10.0 (November 6, 2026) added optional LRU eviction of backend prepared statements and a `txn_mode_leak_action` setting to log or reject statements that leave session state behind in transaction mode.[18][19]
- The README lists a Prometheus `/metrics` endpoint, an OpenAPI management API, brief connection buffering for restarts or failovers, clustering, and rolling or blue/green deployments.[10]
- Routing is intentionally narrow. The README lists read-replica load balancing under "Future Work", although v1.0.0 release notes mention read replica support, so verify before relying on it. It is not a sharding proxy. Choose it when connection governance and bursty/serverless fan-in are the real problem.[10][40]

### C. Performance Notes

All numbers below are self-reported by a project or an adopter. They are useful for orders of magnitude, not for ranking.

- **PgBouncer** has the lowest-overhead design for pure pooling, but it is single-threaded, so one process eventually becomes CPU-bound; `so_reuseport` lets you run several.[6] In PgDog's benchmark it was slightly faster than PgDog and PgCat at 1 and 10 connections, and slower above 50.[8]
- **PgCat** has production evidence from Instacart (2023). Over about 1.5 million production queries across 12 hours, PgBouncer vs PgCat latency was p50 `4.68ms` vs `4.69ms`, p90 `10.8ms` vs `10.9ms`, and p99 `26.8ms` vs `27.8ms`.[7] That measured a PgCat build from 2023.
- **PgDog** published a head-to-head on May 5, 2025: `pgbench -S` (select-only, simple protocol), pool size 10, transaction mode, PgDog and PgCat with 2 worker threads, everything on one Ryzen 7 5800X desktop over localhost. At 1,000 client connections it reported `102,189 TPS` for PgDog, `81,021` for PgBouncer, and `93,369` for PgCat.[8] The author notes CPU hovered around 75% and that all three were bottlenecked by Postgres and I/O. It is a vendor benchmark of a 2025 build, read-only, on one machine.
- **Supavisor** targets safe fan-in rather than single-query latency. Its README shows a small local `pgbench` TPC-B run at about 189 TPS vs 196 TPS for PgBouncer (pool size 60, 100 clients) and a load test of about 1,003,200 client connections over 400 PostgreSQL connections at 20,000+ QPS on a two-node 64 vCPU cluster. Supabase notes this only works in transaction mode and used a 64 vCPU / 256 GB database.[5][10]
- **Odyssey** is designed to scale with worker threads.[4] I found no current, reproducible public benchmark against the other poolers, so characterize it as "built for higher concurrency than single-threaded PgBouncer on large hosts" rather than anchoring on a TPS figure.

### D. Community and Maintenance Snapshot

These are not quality scores, and stars say little about correctness. They do matter operationally because a pooler sits on the critical path for every query. Values come from the GitHub REST API on 2026-11-07.

| Project | GitHub stars | Latest stable release | Last repo push | Read on the signal |
| --- | ---: | --- | --- | --- |
| **PgBouncer** | 4,439[35] | `1.26.1` on October 14, 2026[15] | November 6, 2026[35] | Mature and active, with regular security releases. Safest default from a maintenance-risk perspective. |
| **PgCat** | 4,031[34] | `v1.2.0` on August 30, 2024 (the later `pgcat-0.2.x` tags are Helm chart releases)[11] | February 27, 2025[34] | No commits for over 20 months. Plan for self-support or a fork if you adopt it. |
| **PgDog** | 5,601[36] | `v0.1.64` on November 5, 2026[13] | November 7, 2026[36] | Very active and still pre-1.0, so pin versions and test upgrades. |
| **Odyssey** | 3,640[37] | `v1.5.2` on September 13, 2026 (`v1.6.0-rc1` pre-release October 5, 2026)[17] | November 7, 2026[37] | Healthy recent activity and a long production history at Yandex. |
| **Supavisor** | 2,282[38] | `v2.10.0` on November 6, 2026[19] | November 7, 2026[38] | Active and product-backed, especially attractive if you already operate in a Supabase-like or multi-tenant environment. |

### E. Selection Guidance

- Choose **PgBouncer** when the real problem is simply "Postgres cannot afford one backend per client" and you do not want your pooler to become a routing tier.[1][6][32]
- Choose **PgCat** only if you accept running unmaintained upstream code (or a fork) in exchange for replica routing and failover in one proxy.[2][7][11]
- Choose **PgDog** when you explicitly want a data-plane proxy, not just a pooler, and AGPL-3.0 (or buying the Enterprise edition) fits your legal and commercial model.[3][8][13][29]
- Choose **Odyssey** when you run on Linux and want a multi-threaded C pooler/router with PAM/LDAP auth, but do not need sharding.[4][17]
- Choose **Supavisor** when the dominant concern is multi-tenant connection fan-in, connection governance, and serverless-style burstiness rather than read/write query routing.[5][10]

## References

- [PgBouncer features and SQL feature map][1]
- [PgCat README and feature table][2]
- [PgDog feature summary][3]
- [Odyssey README][4]
- [Supabase Supavisor FAQ][5]
- [PgBouncer config: `max_prepared_statements`][6]
- [Instacart: Adopting PgCat (2023, Wayback Machine copy)][7]
- [PgDog blog: PgDog vs. PgBouncer (May 5, 2025)][8]
- [Odyssey 1.3 release notes: prepared statements and standby lag polling][9]
- [Supavisor README][10]
- [PgCat v1.2.0 release][11]
- [PgCat sample configuration (`pgcat.toml`)][12]
- [PgDog repository README][13]
- [PgBouncer repository][14]
- [PgBouncer releases][15]
- [PgBouncer changelog][16]
- [Odyssey v1.5.2 release notes][17]
- [Supavisor v2.7.0 release notes][18]
- [Supavisor v2.10.0 release notes][19]
- [PgDog load balancer docs][20]
- [PgDog transaction mode docs][21]
- [PgDog pub/sub docs][22]
- [PgDog mirroring docs][23]
- [PgDog replication and failover docs][24]
- [PgDog manual routing docs][25]
- [PgCat v1.1.0 release notes][26]
- [PgDog prepared statements docs][27]
- [PgDog metrics docs][28]
- [PgDog Enterprise edition][29]
- [Odyssey Prometheus metrics docs][30]
- [PgBouncer usage: admin console and rolling restarts][31]
- [PostgreSQL 18: How connections are established][32]
- [PostgreSQL 18: `max_connections`][33]
- [GitHub API: postgresml/pgcat][34]
- [GitHub API: pgbouncer/pgbouncer][35]
- [GitHub API: pgdogdev/pgdog][36]
- [GitHub API: yandex/odyssey][37]
- [GitHub API: supabase/supavisor][38]
- [PgBouncer licence (COPYRIGHT)][39]
- [Supavisor v1.0.0 release notes][40]

[1]: https://www.pgbouncer.org/features.html
[2]: https://github.com/postgresml/pgcat
[3]: https://docs.pgdog.dev/features/
[4]: https://github.com/yandex/odyssey
[5]: https://supabase.com/kb/troubleshooting/supavisor-faq-YyP5tI
[6]: https://www.pgbouncer.org/config.html#max_prepared_statements
[7]: https://web.archive.org/web/20251222112436/https://www.instacart.com/company/tech-innovation/adopting-pgcat-a-nextgen-postgres-proxy
[8]: https://pgdog.dev/blog/pgbouncer-vs-pgdog
[9]: https://github.com/yandex/odyssey/releases/tag/1.3
[10]: https://github.com/supabase/supavisor
[11]: https://github.com/postgresml/pgcat/releases/tag/v1.2.0
[12]: https://github.com/postgresml/pgcat/blob/main/pgcat.toml
[13]: https://github.com/pgdogdev/pgdog
[14]: https://github.com/pgbouncer/pgbouncer
[15]: https://github.com/pgbouncer/pgbouncer/releases
[16]: https://www.pgbouncer.org/changelog.html
[17]: https://github.com/yandex/odyssey/releases/tag/v1.5.2
[18]: https://github.com/supabase/supavisor/releases/tag/v2.7.0
[19]: https://github.com/supabase/supavisor/releases/tag/v2.10.0
[20]: https://docs.pgdog.dev/features/load-balancer/
[21]: https://docs.pgdog.dev/features/connection-pooler/transaction-mode/
[22]: https://docs.pgdog.dev/features/connection-pooler/pub_sub/
[23]: https://docs.pgdog.dev/features/mirroring/
[24]: https://docs.pgdog.dev/features/load-balancer/replication-failover/
[25]: https://docs.pgdog.dev/features/load-balancer/manual-routing/
[26]: https://github.com/postgresml/pgcat/releases/tag/v1.1.0
[27]: https://docs.pgdog.dev/features/connection-pooler/prepared-statements/
[28]: https://docs.pgdog.dev/features/metrics/
[29]: https://docs.pgdog.dev/enterprise_edition/
[30]: https://github.com/yandex/odyssey/blob/master/docs/features/prometheus-metrics.md
[31]: https://www.pgbouncer.org/usage.html
[32]: https://www.postgresql.org/docs/18/connect-estab.html
[33]: https://www.postgresql.org/docs/18/runtime-config-connection.html#GUC-MAX-CONNECTIONS
[34]: https://api.github.com/repos/postgresml/pgcat
[35]: https://api.github.com/repos/pgbouncer/pgbouncer
[36]: https://api.github.com/repos/pgdogdev/pgdog
[37]: https://api.github.com/repos/yandex/odyssey
[38]: https://api.github.com/repos/supabase/supavisor
[39]: https://github.com/pgbouncer/pgbouncer/blob/master/COPYRIGHT
[40]: https://github.com/supabase/supavisor/releases/tag/v1.0.0
