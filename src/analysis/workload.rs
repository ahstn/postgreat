use crate::analysis::query_parser::{
    parse_query_columns, QueryColumnUsage, TableColumnUsage, TableRef,
};
use crate::checker::CheckerError;
use crate::models::{
    QueryIndexCandidate, SlowQueryGroup, SlowQueryInfo, SlowQueryKind, WorkloadResults,
};
use sqlx::{query_scalar, Pool, Postgres, Row};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
pub struct WorkloadOptions {
    pub limit: usize,
    pub min_calls: i64,
    pub max_query_len: usize,
    pub include_full_query: bool,
}

impl Default for WorkloadOptions {
    fn default() -> Self {
        Self {
            limit: 20,
            min_calls: 10,
            max_query_len: 200,
            include_full_query: false,
        }
    }
}

#[derive(Debug, Clone)]
struct StatementStat {
    queryid: i64,
    query: String,
    calls: i64,
    total_time_ms: f64,
    mean_time_ms: f64,
    max_time_ms: f64,
    rows: i64,
    shared_blks_read: i64,
    shared_blks_hit: i64,
    temp_blks_read: i64,
    temp_blks_written: i64,
}

#[derive(Debug, Clone, Copy)]
struct TimeColumns {
    total: &'static str,
    mean: &'static str,
    max: &'static str,
}

#[derive(Debug, Default)]
struct IndexCatalog {
    indexes_by_table: HashMap<String, Vec<Vec<String>>>,
    schemas_by_table: HashMap<String, Vec<String>>,
}

pub async fn analyze(
    pool: &Pool<Postgres>,
    opts: &WorkloadOptions,
) -> Result<WorkloadResults, CheckerError> {
    let mut results = WorkloadResults::default();

    if !pg_stat_statements_installed(pool).await? {
        results.warnings.push(
            "pg_stat_statements extension is not installed; enable it to analyze slow queries."
                .to_string(),
        );
        return Ok(results);
    }

    let version_num = match fetch_server_version(pool).await {
        Ok(version) => version,
        Err(err) => {
            results
                .warnings
                .push(format!("Failed to detect server version: {err}"));
            detect_pg_stat_statements_version(pool)
                .await
                .unwrap_or(130000)
        }
    };

    let time_columns = if version_num >= 130000 {
        TimeColumns {
            total: "total_exec_time",
            mean: "mean_exec_time",
            max: "max_exec_time",
        }
    } else {
        TimeColumns {
            total: "total_time",
            mean: "mean_time",
            max: "max_time",
        }
    };

    let stats = fetch_statements(pool, opts, time_columns).await?;
    if stats.is_empty() {
        results
            .warnings
            .push("No pg_stat_statements entries matched the filters.".to_string());
        return Ok(results);
    }

    results.slow_query_groups = build_slow_query_groups(&stats, opts);

    let index_catalog = fetch_index_catalog(pool).await?;
    let mut candidates = build_index_candidates(&stats, &index_catalog, opts, &mut results);
    candidates.sort_by(|a, b| {
        b.total_time_ms
            .partial_cmp(&a.total_time_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    candidates.truncate(opts.limit);
    results.query_index_candidates = candidates;

    Ok(results)
}

async fn pg_stat_statements_installed(pool: &Pool<Postgres>) -> Result<bool, CheckerError> {
    let query = "SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements' LIMIT 1";
    let exists = query_scalar::<_, i64>(query)
        .fetch_optional(pool)
        .await
        .map_err(|source| CheckerError::QueryError {
            query: query.into(),
            source,
        })?;
    Ok(exists.is_some())
}

async fn fetch_server_version(pool: &Pool<Postgres>) -> Result<i64, CheckerError> {
    let query = "SELECT current_setting('server_version_num')::int";
    query_scalar::<_, i64>(query)
        .fetch_one(pool)
        .await
        .map_err(|source| CheckerError::QueryError {
            query: query.into(),
            source,
        })
}

async fn detect_pg_stat_statements_version(pool: &Pool<Postgres>) -> Option<i64> {
    let query = r#"
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'pg_stat_statements'
          AND column_name = 'total_exec_time'
        LIMIT 1
    "#;
    query_scalar::<_, i64>(query)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .map(|_| 130000)
}

async fn fetch_statements(
    pool: &Pool<Postgres>,
    opts: &WorkloadOptions,
    columns: TimeColumns,
) -> Result<Vec<StatementStat>, CheckerError> {
    let fetch_limit = (opts.limit.max(1) * 5).max(50) as i64;
    let metrics = [
        ("total_time_ms", columns.total),
        ("mean_time_ms", columns.mean),
        ("shared_blks_read", "shared_blks_read"),
        ("temp_blks_written", "temp_blks_written"),
    ];

    let mut map: HashMap<i64, StatementStat> = HashMap::new();

    for (_, metric_column) in metrics {
        let query = format!(
            r#"
            SELECT
                s.queryid,
                s.query,
                s.calls,
                s.rows,
                s.shared_blks_read,
                s.shared_blks_hit,
                s.temp_blks_read,
                s.temp_blks_written,
                s.{total} AS total_time_ms,
                s.{mean} AS mean_time_ms,
                s.{max} AS max_time_ms
            FROM pg_stat_statements s
            WHERE s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
              AND s.calls >= $1
            ORDER BY s.{metric} DESC
            LIMIT $2
            "#,
            total = columns.total,
            mean = columns.mean,
            max = columns.max,
            metric = metric_column
        );

        let rows = sqlx::query(&query)
            .bind(opts.min_calls)
            .bind(fetch_limit)
            .fetch_all(pool)
            .await
            .map_err(|source| CheckerError::QueryError {
                query: query.clone(),
                source,
            })?;

        for row in rows {
            let queryid: i64 = row.get("queryid");
            if map.contains_key(&queryid) {
                continue;
            }

            map.insert(
                queryid,
                StatementStat {
                    queryid,
                    query: row.get("query"),
                    calls: row.get("calls"),
                    total_time_ms: row.get("total_time_ms"),
                    mean_time_ms: row.get("mean_time_ms"),
                    max_time_ms: row.get("max_time_ms"),
                    rows: row.get("rows"),
                    shared_blks_read: row.get("shared_blks_read"),
                    shared_blks_hit: row.get("shared_blks_hit"),
                    temp_blks_read: row.get("temp_blks_read"),
                    temp_blks_written: row.get("temp_blks_written"),
                },
            );
        }
    }

    Ok(map.into_values().collect())
}

fn build_slow_query_groups(stats: &[StatementStat], opts: &WorkloadOptions) -> Vec<SlowQueryGroup> {
    let groups = [
        (SlowQueryKind::TotalTime, "total"),
        (SlowQueryKind::MeanTime, "mean"),
        (SlowQueryKind::SharedBlksRead, "shared_blks_read"),
        (SlowQueryKind::TempBlksWritten, "temp_blks_written"),
    ];

    let mut results = Vec::new();
    for (kind, metric) in groups {
        let mut entries = stats.to_vec();
        match metric {
            "total" => entries.sort_by(|a, b| {
                b.total_time_ms
                    .partial_cmp(&a.total_time_ms)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            "mean" => entries.sort_by(|a, b| {
                b.mean_time_ms
                    .partial_cmp(&a.mean_time_ms)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            "shared_blks_read" => {
                entries.sort_by(|a, b| b.shared_blks_read.cmp(&a.shared_blks_read))
            }
            "temp_blks_written" => {
                entries.sort_by(|a, b| b.temp_blks_written.cmp(&a.temp_blks_written))
            }
            _ => {}
        }

        let queries = entries
            .into_iter()
            .take(opts.limit)
            .map(|stat| SlowQueryInfo {
                queryid: stat.queryid,
                calls: stat.calls,
                total_time_ms: stat.total_time_ms,
                mean_time_ms: stat.mean_time_ms,
                max_time_ms: stat.max_time_ms,
                rows: stat.rows,
                shared_blks_read: stat.shared_blks_read,
                shared_blks_hit: stat.shared_blks_hit,
                temp_blks_read: stat.temp_blks_read,
                temp_blks_written: stat.temp_blks_written,
                query_text: format_query_text(&stat.query, opts),
            })
            .collect();

        results.push(SlowQueryGroup { kind, queries });
    }

    results
}

fn format_query_text(query: &str, opts: &WorkloadOptions) -> String {
    if opts.include_full_query {
        normalize_query(query)
    } else {
        truncate_query(&normalize_query(query), opts.max_query_len)
    }
}

fn normalize_query(query: &str) -> String {
    query.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_query(query: &str, max_len: usize) -> String {
    if query.chars().count() <= max_len {
        return query.to_string();
    }

    let mut truncated = query
        .chars()
        .take(max_len.saturating_sub(1))
        .collect::<String>();
    truncated.push('…');
    truncated
}

fn build_index_candidates(
    stats: &[StatementStat],
    catalog: &IndexCatalog,
    opts: &WorkloadOptions,
    results: &mut WorkloadResults,
) -> Vec<QueryIndexCandidate> {
    let mut deduped: HashMap<String, QueryIndexCandidate> = HashMap::new();

    for stat in stats {
        match parse_query_columns(&stat.query) {
            Ok(usage) => {
                let per_query = build_candidates_for_usage(stat, &usage, catalog);
                for candidate in per_query {
                    let key = format!(
                        "{}.{}:{}",
                        candidate.schema,
                        candidate.table,
                        candidate.columns.join(",").to_lowercase()
                    );
                    let replace = match deduped.get(&key) {
                        Some(existing) => candidate.total_time_ms > existing.total_time_ms,
                        None => true,
                    };
                    if replace {
                        deduped.insert(key, candidate);
                    }
                }
            }
            Err(_) => results.parse_failures += 1,
        }
    }

    let mut candidates: Vec<QueryIndexCandidate> = deduped.into_values().collect();
    candidates.sort_by(|a, b| {
        b.total_time_ms
            .partial_cmp(&a.total_time_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    candidates.truncate(opts.limit * 2);
    candidates
}

fn build_candidates_for_usage(
    stat: &StatementStat,
    usage: &QueryColumnUsage,
    catalog: &IndexCatalog,
) -> Vec<QueryIndexCandidate> {
    let mut table_map = HashMap::new();
    for table in &usage.tables {
        table_map.insert(table.full_name(), table.clone());
    }

    let mut candidates = Vec::new();
    for (table_name, usage) in &usage.usage_by_table {
        let table_ref = table_map.get(table_name);
        let Some(table_ref) = table_ref else { continue };

        let mut columns = Vec::new();
        append_unique(&mut columns, &usage.filters);
        append_unique(&mut columns, &usage.joins);
        append_unique(&mut columns, &usage.orders);

        if columns.is_empty() {
            continue;
        }

        if columns.len() > 3 {
            columns.truncate(3);
        }

        let resolved = resolve_table_schema(table_ref, catalog);
        if resolved.schema != "unknown" && is_index_covered(&resolved.full_name, &columns, catalog)
        {
            continue;
        }

        let reason = format_reason(usage, resolved.ambiguous_schema);
        candidates.push(QueryIndexCandidate {
            schema: resolved.schema,
            table: resolved.table,
            columns,
            reason,
            queryid: stat.queryid,
            total_time_ms: stat.total_time_ms,
            mean_time_ms: stat.mean_time_ms,
            calls: stat.calls,
        });
    }

    candidates
}

fn append_unique(target: &mut Vec<String>, source: &[String]) {
    for value in source {
        if !target
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(value))
        {
            target.push(value.clone());
        }
    }
}

fn format_reason(usage: &TableColumnUsage, ambiguous_schema: bool) -> String {
    let mut parts = Vec::new();
    if !usage.filters.is_empty() {
        parts.push(format!("WHERE {}", usage.filters.join(", ")));
    }
    if !usage.joins.is_empty() {
        parts.push(format!("JOIN {}", usage.joins.join(", ")));
    }
    if !usage.orders.is_empty() {
        parts.push(format!("ORDER BY {}", usage.orders.join(", ")));
    }
    if ambiguous_schema {
        parts.push("schema ambiguous".to_string());
    }
    format!("heuristic from slow query: {}", parts.join("; "))
}

fn is_index_covered(table: &str, columns: &[String], catalog: &IndexCatalog) -> bool {
    let Some(indexes) = catalog.indexes_by_table.get(table) else {
        return false;
    };

    let target: Vec<String> = columns.iter().map(|c| c.to_lowercase()).collect();
    for index_columns in indexes {
        let index_lower: Vec<String> = index_columns.iter().map(|c| c.to_lowercase()).collect();
        if index_lower.len() >= target.len() && index_lower[..target.len()] == target[..] {
            return true;
        }
    }

    false
}

struct ResolvedTable {
    schema: String,
    table: String,
    full_name: String,
    ambiguous_schema: bool,
}

fn resolve_table_schema(table: &TableRef, catalog: &IndexCatalog) -> ResolvedTable {
    if let Some(schema) = &table.schema {
        let full_name = format!("{}.{}", schema, table.name);
        return ResolvedTable {
            schema: schema.clone(),
            table: table.name.clone(),
            full_name,
            ambiguous_schema: false,
        };
    }

    let schemas = catalog
        .schemas_by_table
        .get(&table.name)
        .cloned()
        .unwrap_or_default();

    if schemas.len() == 1 {
        let schema = schemas[0].clone();
        return ResolvedTable {
            schema: schema.clone(),
            table: table.name.clone(),
            full_name: format!("{}.{}", schema, table.name),
            ambiguous_schema: false,
        };
    }

    if schemas.contains(&"public".to_string()) {
        return ResolvedTable {
            schema: "public".to_string(),
            table: table.name.clone(),
            full_name: format!("public.{}", table.name),
            ambiguous_schema: true,
        };
    }

    ResolvedTable {
        schema: "unknown".to_string(),
        table: table.name.clone(),
        full_name: table.name.clone(),
        ambiguous_schema: true,
    }
}

async fn fetch_index_catalog(pool: &Pool<Postgres>) -> Result<IndexCatalog, CheckerError> {
    const QUERY: &str = r#"
        SELECT
            n.nspname AS schema_name,
            c.relname AS table_name,
            array_agg(a.attname ORDER BY arr.ord) AS columns
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_class idx ON idx.oid = i.indexrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS arr(attnum, ord)
            ON arr.attnum > 0
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = arr.attnum
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
        GROUP BY n.nspname, c.relname, idx.relname
    "#;

    let rows =
        sqlx::query(QUERY)
            .fetch_all(pool)
            .await
            .map_err(|source| CheckerError::QueryError {
                query: QUERY.into(),
                source,
            })?;

    let mut catalog = IndexCatalog::default();
    for row in rows {
        let schema: String = row.get("schema_name");
        let table: String = row.get("table_name");
        let columns: Vec<String> = row.get("columns");

        let full_name = format!("{}.{}", schema, table);
        catalog
            .indexes_by_table
            .entry(full_name)
            .or_default()
            .push(columns);

        let entry = catalog.schemas_by_table.entry(table).or_default();
        if !entry.contains(&schema) {
            entry.push(schema);
        }
    }

    Ok(catalog)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::query_parser::{QueryColumnUsage, TableColumnUsage, TableRef};

    fn make_usage() -> QueryColumnUsage {
        let mut usage = QueryColumnUsage::default();
        usage.tables.push(TableRef {
            schema: Some("public".into()),
            name: "orders".into(),
        });
        let mut table_usage = TableColumnUsage::default();
        table_usage.filters = vec!["customer_id".into(), "status".into()];
        table_usage.joins = vec!["org_id".into()];
        table_usage.orders = vec!["created_at".into()];
        usage
            .usage_by_table
            .insert("public.orders".into(), table_usage);
        usage
    }

    #[test]
    fn candidate_orders_columns_by_filter_join_order() {
        let usage = make_usage();
        let catalog = IndexCatalog::default();
        let stat = StatementStat {
            queryid: 1,
            query: "SELECT * FROM orders".into(),
            calls: 10,
            total_time_ms: 1000.0,
            mean_time_ms: 100.0,
            max_time_ms: 200.0,
            rows: 0,
            shared_blks_read: 0,
            shared_blks_hit: 0,
            temp_blks_read: 0,
            temp_blks_written: 0,
        };

        let candidates = build_candidates_for_usage(&stat, &usage, &catalog);
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].columns,
            vec!["customer_id", "status", "org_id"]
        );
    }

    #[test]
    fn candidate_skips_when_index_covers_prefix() {
        let mut usage = QueryColumnUsage::default();
        usage.tables.push(TableRef {
            schema: Some("public".into()),
            name: "orders".into(),
        });
        let mut table_usage = TableColumnUsage::default();
        table_usage.filters = vec!["customer_id".into(), "status".into()];
        usage
            .usage_by_table
            .insert("public.orders".into(), table_usage);
        let mut catalog = IndexCatalog::default();
        catalog.indexes_by_table.insert(
            "public.orders".into(),
            vec![vec!["customer_id".into(), "status".into()]],
        );

        let stat = StatementStat {
            queryid: 1,
            query: "SELECT * FROM orders".into(),
            calls: 10,
            total_time_ms: 1000.0,
            mean_time_ms: 100.0,
            max_time_ms: 200.0,
            rows: 0,
            shared_blks_read: 0,
            shared_blks_hit: 0,
            temp_blks_read: 0,
            temp_blks_written: 0,
        };

        let candidates = build_candidates_for_usage(&stat, &usage, &catalog);
        assert!(candidates.is_empty());
    }

    #[test]
    fn candidate_dedupes_by_columns() {
        let catalog = IndexCatalog::default();
        let stat_one = StatementStat {
            queryid: 1,
            query: "SELECT * FROM orders WHERE customer_id = $1 AND status = 'open'".into(),
            calls: 10,
            total_time_ms: 1000.0,
            mean_time_ms: 100.0,
            max_time_ms: 200.0,
            rows: 0,
            shared_blks_read: 0,
            shared_blks_hit: 0,
            temp_blks_read: 0,
            temp_blks_written: 0,
        };
        let stat_two = StatementStat {
            queryid: 2,
            query: "SELECT * FROM orders WHERE customer_id = $1 AND status = 'open'".into(),
            calls: 8,
            total_time_ms: 500.0,
            mean_time_ms: 120.0,
            max_time_ms: 200.0,
            rows: 0,
            shared_blks_read: 0,
            shared_blks_hit: 0,
            temp_blks_read: 0,
            temp_blks_written: 0,
        };

        let mut results = WorkloadResults::default();
        let candidates = build_index_candidates(
            &[stat_one, stat_two],
            &catalog,
            &WorkloadOptions::default(),
            &mut results,
        );
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].queryid, 1);
    }
}
