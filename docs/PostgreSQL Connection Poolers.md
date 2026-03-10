## PostgreSQL Connection Poolers

This overview is for the common question behind most pooler decisions: do you only need connection multiplexing, or do you want the proxy layer to also understand replicas, routing, and multi-tenant scale? For most teams, that question matters more than micro-benchmarks.[1][2][3][4][5]

### A. Quick Positioning

- **PgBouncer:** Best default when you want a small, well-understood pooler and nothing more.[1][6]
- **PgCat:** A stronger "PgBouncer plus routing" option with read balancing, failover-style banning, metrics, and optional sharding, but upstream maintenance appears materially quieter than the alternatives.[2][7]
- **PgDog:** The most ambitious proxy in this group: pooling, read/write splitting, lag-aware failover routing, prepared statements in transaction mode, and sharding are all first-class features. The main trade-off is the AGPL-3.0 license.[3][8]
- **Odyssey:** A mature multi-threaded C pooler/router with strong auth support and recent release activity. It sits between PgBouncer and PgDog in scope: more capable than a minimal pooler, less broad than a sharding platform.[4][9]
- **Supavisor:** Best suited to multi-tenant and serverless-style fan-in, where the core problem is too many transient clients rather than replica routing. It is a pooler first, not a general-purpose query router.[5][10]

### B. Feature Comparison

#### PgBouncer

- Supports session, transaction, and statement pooling, plus online reconfiguration and online restart/upgrade without dropping client connections.[1][6]
- Still has the clearest "minimal pooler" contract: very low memory overhead, a tight operational surface area, and an admin database with `SHOW` commands for runtime inspection.[1]
- Protocol-level prepared statements now work in transaction and statement pooling when `max_prepared_statements` is enabled, but SQL `PREPARE` / `DEALLOCATE` and other session-bound features remain poor fits for transaction pooling.[1][6]
- Security and auth have continued to improve: current releases add LDAP authentication, client-side direct TLS support, and SCRAM-related performance/security work.[6][16]
- It deliberately does **not** try to be a read/write router or failover proxy. If you need replica-aware routing, PgBouncer usually stays the pooling layer in front of something else, or is replaced by a more featureful proxy.[1]

#### PgCat

- Supports session and transaction pooling with a multi-threaded Tokio runtime, stable read-query load balancing, health-check-based failover, admin databases, Prometheus metrics, TLS, and live config reload.[2][12]
- Uses a built-in query parser to route `SELECT` traffic to replicas and other statements, plus explicit transactions, to the primary. It also supports different balancing strategies and options like primary reads and activity-based routing.[2][12]
- Operational extras are stronger than a basic pooler: auth passthrough via `auth_query`, mirroring for testing, and plugins such as prewarming and query logging are all part of the current project surface.[2][12]
- Sharding exists in several forms, including explicit shard selection, comment-based hints, and automatic sharding, but the project still labels those sharding paths and mirroring as experimental.[2]
- The big limitation has not changed: in transaction mode, prepared statements, `SET`, and advisory locks are not supported. If your client depends on session semantics, PgCat pushes you back toward session pooling.[2]

#### PgDog

- PgDog is a pooler, load balancer, query router, and sharding proxy in one executable, with transaction mode enabled by default and optional session and statement modes.[3][20][21]
- Its standout feature is parser-driven routing: PgDog says it understands all valid PostgreSQL queries, can split reads from writes behind a single endpoint, and handles edge cases such as `SELECT FOR UPDATE` and write CTEs by routing them to the primary.[20]
- It goes further than most poolers in transaction mode by tracking session state: prepared statements, `SET` options, startup parameters, and even advisory locks are handled explicitly so client behavior can stay closer to a direct connection.[3][21]
- Additional capabilities not covered in the original draft are substantial: manual routing hints, LISTEN/NOTIFY support in transaction mode, query mirroring, plugins, OpenMetrics/admin-database observability, and replica-lag / promotion-aware failover routing.[3][22][23][24][25]
- The trade-offs are equally real: AGPL-3.0 licensing may be disqualifying for some teams, and parts of the replication/failover stack are still documented as experimental rather than "set and forget".[13][24]

#### Odyssey

- Odyssey positions itself as an advanced multi-threaded PostgreSQL connection pooler and request router, aimed at large production deployments rather than minimalist pooling.[4][9]
- Its multi-threaded worker design shares global server pools and is explicitly optimized for better scaling and stronger SSL/TLS throughput on larger machines.[4]
- Odyssey's transaction pooling is more sophisticated than "borrow a connection and forget it": it tracks transaction state, can issue automatic `Cancel` plus `Rollback` on abandoned transactions, and remembers the last owning client to reduce repeated setup work.[4]
- It also has a strong auth and operations story: per-database/per-user pool controls, SSL/TLS, PAM and LDAP authentication, detailed UUID-based logging, and Prometheus support in current releases.[4][9]
- Like modern PgBouncer, Odyssey supports protocol-level prepared statements in transaction pooling, and release notes also added standby-lag polling to help avoid stale replica reads. It is more replica-aware than PgBouncer, but it is not a general sharding layer.[9][17]

#### Supavisor

- Supavisor is best understood as a cloud-native, multi-tenant pooler cluster rather than a general-purpose SQL router. Its core job is to absorb very large numbers of client connections and map them onto a much smaller pool of database connections.[5][10]
- Supabase's public docs expose separate transaction and session endpoints: transaction mode is the high-fan-in default, while session mode is the straightforward choice when you need session semantics such as prepared statements.[5]
- Upstream capability is moving, though: recent Supavisor releases added named prepared statement support behind a feature flag, so the product is no longer purely "prepared statements require session mode" even if the default guidance still says that.[18]
- Operationally, Supavisor is richer than the original table suggested. The official README and docs emphasize Prometheus metrics, management/API endpoints, connection buffering for brief restarts or failovers, clustered deployment, and blue/green or rolling replacement patterns.[10][18]
- Where it remains intentionally narrow is routing: current OSS docs do not present Supavisor as a general read/write splitter or sharding proxy. Choose it when connection governance and bursty/serverless fan-in are the real problem.[5][10][19]

### C. Performance Notes

- **PgBouncer** still has the best "minimal overhead" story. Its single-process design is hard to beat when the job is only pooling and the box is not yet proxy-CPU-bound.[1][6]
- **PgCat** has credible production evidence from Instacart rather than just synthetic throughput. Over roughly 1.5 million production queries across 12 hours, Instacart reported latency almost identical to PgBouncer: p50 `4.68ms` vs `4.69ms`, p90 `10.8ms` vs `10.9ms`, and p99 `26.8ms` vs `27.8ms`.[7]
- **PgDog** publishes the most detailed recent head-to-head benchmark in this set. In its May 5, 2025 `pgbench -S` comparison on localhost, PgDog reported `102,189 TPS` at 1,000 clients versus `81,021` for PgBouncer and `93,369` for PgCat.[8] Treat that as indicative, not universal: it is a project-authored benchmark, read-only, simple-protocol, and explicitly proxy-bound.
- **Supavisor** is more compelling on safe fan-in than on absolute single-query latency. Its README shows local `pgbench` results close to PgBouncer, and its public scale material focuses on serving roughly 1 million client connections by multiplexing onto about 400 PostgreSQL connections in transaction mode on very large hardware.[10]
- **Odyssey** benefits from a multi-threaded design, and the project's positioning has long emphasized linear scaling with CPU cores.[4][17] The strongest current public comparison I found is still secondary rather than an official benchmark, so it is safer to characterize Odyssey as "built for higher concurrency than PgBouncer on large hosts" than to anchor on a single TPS figure.[17]

### D. Community and Maintenance Snapshot

These are not quality scores, but they do matter operationally because a pooler sits on the critical path for every query.

| Project | GitHub stars | Latest release | Last observed repo push | Read on the signal |
| --- | ---: | --- | --- | --- |
| **PgBouncer** | 3,966[14] | `1.25.1` on December 3, 2025[15] | January 25, 2026[14] | Mature and active. Safest default from a maintenance-risk perspective. |
| **PgCat** | 3,876[2] | `0.2.5` on November 11, 2024[11] | February 27, 2025[2] | Technically strong, but upstream activity looks meaningfully quieter than the rest of this field. Plan for self-support if you adopt it. |
| **PgDog** | 4,109[13] | `v0.1.31` on March 5, 2026[13] | March 7, 2026[13] | Fast-moving and clearly active, with the usual trade-off that fast-moving infrastructure needs tighter change control. |
| **Odyssey** | 3,456[4] | current release train active through early 2026[9] | March 2026 activity visible in the repo[4] | Healthy recent activity and a long-running production pedigree. |
| **Supavisor** | 2,149[10] | `v2.8.0` on December 1, 2025[10] | March 10, 2026[10] | Active and product-backed, especially attractive if you already operate in a Supabase-like or multi-tenant environment. |

### E. Selection Guidance

- Choose **PgBouncer** when the real problem is simply "Postgres cannot afford one backend per client" and you do not want your pooler to become a routing tier.[1][6]
- Choose **PgCat** when you want replica routing and failover-style behavior in one proxy, but go in with open eyes about upstream dormancy.[2][7][11]
- Choose **PgDog** when you explicitly want a data-plane proxy, not just a pooler, and your legal/commercial model is compatible with AGPL-3.0.[3][8][13]
- Choose **Odyssey** when you want a multi-threaded C pooler/router with strong auth features and recent release activity, but you do not need PgDog-style sharding ambitions.[4][9]
- Choose **Supavisor** when the dominant concern is multi-tenant connection fan-in, connection governance, and serverless-style burstiness rather than read/write query routing.[5][10]

## References

- [PgBouncer features and compatibility][1]
- [PgCat README and feature table][2]
- [PgDog feature summary][3]
- [Odyssey README][4]
- [Supabase Supavisor FAQ][5]
- [PgBouncer config: `max_prepared_statements`][6]
- [Instacart: Adopting PgCat][7]
- [PgDog benchmark: PgBouncer vs PgDog][8]
- [Odyssey release notes: prepared statements and standby lag polling][9]
- [Supavisor README / repository][10]
- [PgCat releases][11]
- [PgCat sample configuration and parser features][12]
- [PgDog repository / releases][13]
- [PgBouncer repository][14]
- [PgBouncer releases][15]
- [PgBouncer recent release notes][16]
- [Onidel proxy comparison 2025][17]
- [Supavisor 2.7.0 release notes][18]
- [Supabase connection management docs][19]
- [PgDog load balancer docs][20]
- [PgDog transaction mode docs][21]
- [PgDog pub/sub docs][22]
- [PgDog mirroring docs][23]
- [PgDog replication and failover docs][24]
- [PgDog manual routing docs][25]

[1]: https://www.pgbouncer.org/features.html
[2]: https://github.com/postgresml/pgcat
[3]: https://docs.pgdog.dev/features/
[4]: https://github.com/yandex/odyssey
[5]: https://supabase.com/docs/guides/troubleshooting/supavisor-faq-YyP5tI
[6]: https://www.pgbouncer.org/config.html
[7]: https://www.instacart.com/company/tech-innovation/adopting-pgcat-a-nextgen-postgres-proxy
[8]: https://pgdog.dev/blog/pgbouncer-vs-pgdog
[9]: https://github.com/yandex/odyssey/releases/tag/1.3
[10]: https://github.com/supabase/supavisor
[11]: https://github.com/postgresml/pgcat/releases
[12]: https://github.com/postgresml/pgcat/blob/main/pgcat.toml
[13]: https://github.com/pgdogdev/pgdog
[14]: https://github.com/pgbouncer/pgbouncer
[15]: https://github.com/pgbouncer/pgbouncer/releases
[16]: https://www.pgbouncer.org/changelog.html
[17]: https://onidel.com/blog/postgresql-proxy-comparison-2025
[18]: https://github.com/supabase/supavisor/releases/tag/v2.7.0
[19]: https://supabase.com/docs/guides/database/connection-management
[20]: https://docs.pgdog.dev/features/load-balancer
[21]: https://docs.pgdog.dev/features/transaction-mode/
[22]: https://docs.pgdog.dev/features/pub_sub/
[23]: https://docs.pgdog.dev/features/mirroring/
[24]: https://docs.pgdog.dev/features/load-balancer/replication-failover/
[25]: https://docs.pgdog.dev/features/load-balancer/manual-routing/
