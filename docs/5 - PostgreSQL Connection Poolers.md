## V. PostgreSQL Connection Poolers

This overview is for the common question behind most pooler decisions: do you only need connection multiplexing, or do you want the proxy layer to also understand replicas, routing, and multi-tenant scale? For most teams, that question matters more than micro-benchmarks.[1][2][3][4][5]

### A. Quick Positioning

- **PgBouncer:** Best default when you want a small, boring, well-understood pooler and nothing more.[1][6]
- **PgCat:** A stronger "PgBouncer plus routing" option with read balancing, failover-style banning, metrics, and optional sharding, but upstream maintenance appears materially quieter than the alternatives.[2][7]
- **PgDog:** The most ambitious proxy in this group: pooling, read/write splitting, lag-aware failover routing, prepared statements in transaction mode, and sharding are all first-class features. The main trade-off is the AGPL-3.0 license.[3][8]
- **Odyssey:** A mature multi-threaded C pooler/router with strong auth support and recent release activity. It sits between PgBouncer and PgDog in scope: more capable than a minimal pooler, less broad than a sharding platform.[4][9]
- **Supavisor:** Best suited to multi-tenant and serverless-style fan-in, where the core problem is too many transient clients rather than replica routing. It is a pooler first, not a general-purpose query router.[5][10]

### B. Feature Comparison

| Pooler | Pooling modes | Prepared statements | Routing / replicas | Sharding | Observability | License |
| --- | --- | --- | --- | --- | --- | --- |
| **PgBouncer** | Session, transaction, statement[1] | Protocol-level prepared statements can work in transaction/statement mode when `max_prepared_statements` is enabled; SQL `PREPARE` remains a poor fit for transaction pooling.[1][6] | No built-in read/write routing or failover logic.[1] | No[1] | Admin database, `SHOW` commands; Prometheus usually via exporters.[1] | ISC[11] |
| **PgCat** | Session, transaction[2] | Supported in session mode; not supported in transaction mode.[2] | Yes: parser-based read routing, health checks, server banning, load balancing.[2][7] | Yes, but project materials still describe sharding/mirroring as experimental.[2] | Admin DB plus `/metrics` endpoint.[2] | MIT[12] |
| **PgDog** | Session, transaction, statement[3] | Supported in transaction mode via a global cache and query/protocol rewriting.[3][8] | Yes: single-endpoint read/write split, replica health checks, lag detection, failover-aware routing.[3] | Yes; this is a core product feature.[3] | Admin DB plus OpenMetrics / Prometheus-style metrics.[3] | AGPL-3.0[13] |
| **Odyssey** | Session, transaction[4] | Protocol-level prepared statements in transaction pooling are supported in current releases.[9] | Yes: request routing is part of the project scope, and release notes added standby-lag polling to help avoid stale replica reads.[4][9] | No general sharding story.[4] | Prometheus support plus detailed logging.[9] | BSD-3-Clause[14] |
| **Supavisor** | Transaction and session, exposed as separate endpoints in Supabase docs.[5] | Supabase docs still steer prepared-statement-heavy clients to session mode; upstream releases added named prepared statement support behind a feature flag, so this area is improving but is not as simple as PgBouncer session mode.[5][10] | Not a general read/write router in current OSS docs.[5][10] | Not a focus.[10] | Prometheus `/metrics`, management API, platform monitoring guidance.[5][10] | Apache-2.0[15] |

### C. Performance Notes

- **PgBouncer** still has the best "minimal overhead" story. Its single-process design is hard to beat when the job is only pooling and the box is not yet proxy-CPU-bound.[1][6]
- **PgCat** has credible production evidence from Instacart rather than just synthetic throughput. Over roughly 1.5 million production queries across 12 hours, Instacart reported latency almost identical to PgBouncer: p50 `4.68ms` vs `4.69ms`, p90 `10.8ms` vs `10.9ms`, and p99 `26.8ms` vs `27.8ms`.[7]
- **PgDog** publishes the most detailed recent head-to-head benchmark in this set. In its May 5, 2025 `pgbench -S` comparison on localhost, PgDog reported `102,189 TPS` at 1,000 clients versus `81,021` for PgBouncer and `93,369` for PgCat.[8] Treat that as indicative, not universal: it is a project-authored benchmark, read-only, simple-protocol, and explicitly proxy-bound.
- **Supavisor** is more compelling on safe fan-in than on absolute single-query latency. Its README shows local `pgbench` results close to PgBouncer, and its public scale material focuses on serving roughly 1 million client connections by multiplexing onto about 400 PostgreSQL connections in transaction mode on very large hardware.[10]
- **Odyssey** benefits from a multi-threaded design, and the project's positioning has long emphasized linear scaling with CPU cores.[4][16] The strongest current public comparison I found is still secondary rather than an official benchmark, so it is safer to characterize Odyssey as "built for higher concurrency than PgBouncer on large hosts" than to anchor on a single TPS figure.[17]

### D. Community and Maintenance Snapshot

These are not quality scores, but they do matter operationally because a pooler sits on the critical path for every query.

| Project | GitHub stars | Latest release | Last observed repo push | Read on the signal |
| --- | ---: | --- | --- | --- |
| **PgBouncer** | 3,966[18] | `1.25.1` on December 3, 2025[19] | January 25, 2026[18] | Mature and active. Safest default from a maintenance-risk perspective. |
| **PgCat** | 3,876[20] | `0.2.5` on November 11, 2024[21] | February 27, 2025[20] | Technically strong, but upstream activity looks meaningfully quieter than the rest of this field. Plan for self-support if you adopt it. |
| **PgDog** | 4,109[22] | `v0.1.31` on March 5, 2026[23] | March 7, 2026[22] | Fast-moving and clearly active, with the usual trade-off that fast-moving infrastructure needs tighter change control. |
| **Odyssey** | 3,456[24] | `v1.5.0` on January 28, 2026[25] | March 9, 2026[24] | Healthy recent activity and a long-running production pedigree. |
| **Supavisor** | 2,149[26] | `v2.8.0` on December 1, 2025[27] | March 10, 2026[26] | Active and product-backed, especially attractive if you already operate in a Supabase-like or multi-tenant environment. |

### E. Selection Guidance

- Choose **PgBouncer** when the real problem is simply "Postgres cannot afford one backend per client" and you do not want your pooler to become a routing tier.[1][6]
- Choose **PgCat** when you want replica routing and failover-style behavior in one proxy, but go in with open eyes about upstream dormancy.[2][7][20][21]
- Choose **PgDog** when you explicitly want a data-plane proxy, not just a pooler, and your legal/commercial model is compatible with AGPL-3.0.[3][8][13]
- Choose **Odyssey** when you want a multi-threaded C pooler/router with strong auth features and recent release activity, but you do not need PgDog-style sharding ambitions.[4][9]
- Choose **Supavisor** when the dominant concern is multi-tenant connection fan-in, connection governance, and serverless-style burstiness rather than read/write query routing.[5][10]

## References

- [PgBouncer features and compatibility][1]
- [PgCat README][2]
- [PgDog documentation: features][3]
- [Odyssey README][4]
- [Supabase Supavisor FAQ][5]
- [PgBouncer config: `max_prepared_statements`][6]
- [Instacart: Adopting PgCat][7]
- [PgDog benchmark: PgBouncer vs PgDog][8]
- [Odyssey release notes: prepared statements and standby lag polling][9]
- [Supavisor README][10]
- [PgBouncer repository license metadata][11]
- [PgCat repository license metadata][12]
- [PgDog repository license metadata][13]
- [Odyssey repository license metadata][14]
- [Supavisor repository license metadata][15]
- [Odyssey 1.0 release notes][16]
- [Onidel proxy comparison 2025][17]
- [PgBouncer repository][18]
- [PgBouncer releases][19]
- [PgCat repository][20]
- [PgCat releases][21]
- [PgDog repository][22]
- [PgDog releases][23]
- [Odyssey repository][24]
- [Odyssey releases][25]
- [Supavisor repository][26]
- [Supavisor releases][27]

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
[11]: https://github.com/pgbouncer/pgbouncer
[12]: https://github.com/postgresml/pgcat
[13]: https://github.com/pgdogdev/pgdog
[14]: https://github.com/yandex/odyssey
[15]: https://github.com/supabase/supavisor
[16]: https://github.com/yandex/odyssey/releases/tag/1.0
[17]: https://onidel.com/blog/postgresql-proxy-comparison-2025
[18]: https://github.com/pgbouncer/pgbouncer
[19]: https://github.com/pgbouncer/pgbouncer/releases
[20]: https://github.com/postgresml/pgcat
[21]: https://github.com/postgresml/pgcat/releases
[22]: https://github.com/pgdogdev/pgdog
[23]: https://github.com/pgdogdev/pgdog/releases
[24]: https://github.com/yandex/odyssey
[25]: https://github.com/yandex/odyssey/releases
[26]: https://github.com/supabase/supavisor
[27]: https://github.com/supabase/supavisor/releases
