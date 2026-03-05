use crate::analysis::query_parser::{
    parse_query_columns, QueryColumnUsage, TableColumnUsage, TableRef,
};
use crate::checker::CheckerError;
use crate::models::{
    IndexIssueKind, QueryIndexCandidate, QueryIndexEvidence, SlowQueryGroup, SlowQueryInfo,
    SlowQueryKind, WorkloadCoverageStats, WorkloadFindingConfidence, WorkloadMetadata,
    WorkloadResults,
};
use sqlx::{query_scalar, Error, Pool, Postgres, Row};
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
    wal_bytes: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
struct TimeColumns {
    total: &'static str,
    max: &'static str,
}

#[derive(Debug, Clone, Default)]
struct WorkloadMetadataSnapshot {
    server_version: Option<i64>,
    stats_reset_at: Option<String>,
    seconds_since_reset: Option<f64>,
    entry_deallocations: Option<i64>,
    query_text_visible: bool,
    has_wal_bytes: bool,
}

#[derive(Debug)]
pub(crate) struct WorkloadAnalysis {
    pub(crate) results: WorkloadResults,
    pub(crate) available: bool,
}

impl WorkloadAnalysis {
    fn available(results: WorkloadResults) -> Self {
        Self {
            results,
            available: true,
        }
    }

    fn unavailable(results: WorkloadResults) -> Self {
        Self {
            results,
            available: false,
        }
    }
}

#[derive(Debug)]
enum PgStatStatementsAvailability {
    Available,
    Unavailable { warning: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StatementKey {
    queryid: i64,
    query: String,
}

const RECENT_STATS_RESET_WARNING_WINDOW_SECS: f64 = 24.0 * 60.0 * 60.0;
const PARSE_FAILURE_WARNING_RATIO: f64 = 0.10;
const PARSE_FAILURE_WARNING_MIN: usize = 3;

#[derive(Debug, Clone)]
struct IndexDefinition {
    schema: String,
    table: String,
    access_method: String,
    key_columns: Vec<String>,
    is_partial: bool,
    is_expression: bool,
    is_valid: bool,
}

#[derive(Debug, Default)]
struct IndexCatalog {
    indexes_by_table: HashMap<String, Vec<IndexDefinition>>,
    schemas_by_table: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Default)]
struct SearchKey {
    equality_columns: Vec<String>,
    ordered_columns: Vec<String>,
    display_columns: Vec<String>,
}

impl SearchKey {
    fn from_usage(usage: &TableColumnUsage) -> Self {
        let mut equality_columns = Vec::new();
        append_unique(&mut equality_columns, &usage.equality_filters);
        append_unique(&mut equality_columns, &usage.equality_joins);

        let mut ordered_columns = Vec::new();
        append_unique(&mut ordered_columns, &usage.non_equality_filters);
        append_unique(&mut ordered_columns, &usage.orders);

        let mut display_columns = Vec::new();
        append_unique(&mut display_columns, &usage.equality_filters);
        append_unique(&mut display_columns, &usage.equality_joins);
        append_unique(&mut display_columns, &usage.non_equality_filters);
        append_unique(&mut display_columns, &usage.orders);

        Self {
            equality_columns,
            ordered_columns,
            display_columns,
        }
    }

    fn is_empty(&self) -> bool {
        self.display_columns.is_empty()
    }
}

pub(crate) async fn analyze(
    pool: &Pool<Postgres>,
    opts: &WorkloadOptions,
) -> Result<WorkloadAnalysis, CheckerError> {
    let mut results = WorkloadResults::default();

    match preflight_pg_stat_statements(pool).await? {
        PgStatStatementsAvailability::Available => {}
        PgStatStatementsAvailability::Unavailable { warning } => {
            results.warnings.push(warning);
            return Ok(WorkloadAnalysis::unavailable(results));
        }
    }

    let metadata = collect_workload_metadata(pool, &mut results).await;
    results.workload_metadata = build_workload_metadata(&metadata);
    add_metadata_warnings(&metadata, &mut results);

    let time_columns = resolve_time_columns(pool, &mut results, metadata.server_version).await;

    let stats = fetch_statements(pool, opts, time_columns, metadata.has_wal_bytes).await?;
    if stats.is_empty() {
        results
            .warnings
            .push("No pg_stat_statements entries matched the filters.".to_string());
        return Ok(WorkloadAnalysis::available(results));
    }

    results.slow_query_groups = build_slow_query_groups(&stats, opts);

    let index_catalog = fetch_index_catalog(pool).await?;
    let candidate_build = build_index_candidates(&stats, &index_catalog, opts);
    let mut candidates = candidate_build.candidates;
    results.parse_failures = candidate_build.coverage_stats.parser_errors;
    results.coverage_stats = candidate_build.coverage_stats.clone();
    results.workload_metadata.parsed_queries = candidate_build.parsed_queries;
    results.workload_metadata.parse_failures = candidate_build.coverage_stats.parser_errors;
    results.workload_metadata.suppressed_candidates =
        candidate_build.coverage_stats.suppressed_by_existing_index;
    let workload_metadata = results.workload_metadata.clone();
    add_parse_failure_warning(stats.len(), &workload_metadata, &mut results);
    candidates.sort_by(|a, b| {
        b.total_time_ms
            .partial_cmp(&a.total_time_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    candidates.truncate(opts.limit);
    results.query_index_candidates = candidates;

    Ok(WorkloadAnalysis::available(results))
}

async fn collect_workload_metadata(
    pool: &Pool<Postgres>,
    results: &mut WorkloadResults,
) -> WorkloadMetadataSnapshot {
    let server_version = match fetch_server_version(pool).await {
        Ok(version) => Some(version),
        Err(err) => {
            results
                .warnings
                .push(format!("Failed to detect server version: {err}"));
            None
        }
    };

    let query_text_visible = match fetch_query_text_visibility(pool).await {
        Ok(visible) => visible,
        Err(err) => {
            results
                .warnings
                .push(format!("Failed to detect query text visibility: {err}"));
            true
        }
    };

    let has_wal_bytes = match pg_stat_statements_has_column(pool, "wal_bytes").await {
        Ok(has_column) => has_column,
        Err(err) => {
            results.warnings.push(format!(
                "Failed to detect pg_stat_statements write metrics: {err}"
            ));
            false
        }
    };

    let (stats_reset_at, seconds_since_reset, entry_deallocations) =
        match fetch_pg_stat_statements_info(pool).await {
            Ok(info) => info,
            Err(err) => {
                results
                    .warnings
                    .push(format!("Failed to read pg_stat_statements_info: {err}"));
                (None, None, None)
            }
        };

    WorkloadMetadataSnapshot {
        server_version,
        stats_reset_at,
        seconds_since_reset,
        entry_deallocations,
        query_text_visible,
        has_wal_bytes,
    }
}

fn build_workload_metadata(snapshot: &WorkloadMetadataSnapshot) -> WorkloadMetadata {
    WorkloadMetadata {
        server_version: snapshot.server_version,
        stats_reset_at: snapshot.stats_reset_at.clone(),
        entry_deallocations: snapshot.entry_deallocations,
        query_text_visible: snapshot.query_text_visible,
        ..WorkloadMetadata::default()
    }
}

fn add_metadata_warnings(snapshot: &WorkloadMetadataSnapshot, results: &mut WorkloadResults) {
    if let (Some(stats_reset_at), Some(seconds_since_reset)) =
        (&snapshot.stats_reset_at, snapshot.seconds_since_reset)
    {
        if seconds_since_reset <= RECENT_STATS_RESET_WARNING_WINDOW_SECS {
            results.warnings.push(format!(
                "Workload results are cumulative only since pg_stat_statements was last reset at {stats_reset_at}."
            ));
        }
    }

    if snapshot.entry_deallocations.unwrap_or(0) > 0 {
        results.warnings.push(format!(
            "pg_stat_statements has evicted {} entries due to capacity pressure; low-frequency statements and derived findings may be incomplete.",
            snapshot.entry_deallocations.unwrap_or(0)
        ));
    }

    if !snapshot.query_text_visible {
        results.warnings.push(
            "Query text visibility appears limited for the current role; grant pg_read_all_stats to avoid incomplete or anonymized workload findings."
                .to_string(),
        );
    }
}

fn add_parse_failure_warning(
    total_queries: usize,
    metadata: &WorkloadMetadata,
    results: &mut WorkloadResults,
) {
    if metadata.parse_failures == 0 || total_queries == 0 {
        return;
    }

    let failure_ratio = metadata.parse_failures as f64 / total_queries as f64;
    if metadata.parse_failures >= PARSE_FAILURE_WARNING_MIN
        || failure_ratio >= PARSE_FAILURE_WARNING_RATIO
    {
        results.warnings.push(format!(
            "Only {} of {} workload statements were parsed into index evidence; index candidate coverage is partial.",
            metadata.parsed_queries, total_queries
        ));
    }
}

async fn preflight_pg_stat_statements(
    pool: &Pool<Postgres>,
) -> Result<PgStatStatementsAvailability, CheckerError> {
    if !pg_stat_statements_installed(pool).await? {
        return Ok(PgStatStatementsAvailability::Unavailable {
            warning:
                "pg_stat_statements extension is not installed; enable it to analyze slow queries."
                    .to_string(),
        });
    }

    probe_pg_stat_statements(pool).await
}

async fn pg_stat_statements_installed(pool: &Pool<Postgres>) -> Result<bool, CheckerError> {
    let query = "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')";
    query_scalar::<_, bool>(query)
        .fetch_one(pool)
        .await
        .map_err(|source| CheckerError::QueryError {
            query: query.into(),
            source,
        })
}

async fn probe_pg_stat_statements(
    pool: &Pool<Postgres>,
) -> Result<PgStatStatementsAvailability, CheckerError> {
    let query = "SELECT 1 FROM pg_stat_statements LIMIT 1";
    match query_scalar::<_, i32>(query).fetch_optional(pool).await {
        Ok(_) => Ok(PgStatStatementsAvailability::Available),
        Err(source) => match pg_stat_statements_unavailable_warning(&source) {
            Some(warning) => Ok(PgStatStatementsAvailability::Unavailable { warning }),
            None => Err(CheckerError::QueryError {
                query: query.into(),
                source,
            }),
        },
    }
}

async fn fetch_server_version(pool: &Pool<Postgres>) -> Result<i64, CheckerError> {
    let query = "SELECT current_setting('server_version_num')::bigint";
    query_scalar::<_, i64>(query)
        .fetch_one(pool)
        .await
        .map_err(|source| CheckerError::QueryError {
            query: query.into(),
            source,
        })
}

async fn fetch_query_text_visibility(pool: &Pool<Postgres>) -> Result<bool, CheckerError> {
    let query = r#"
        SELECT current_setting('is_superuser')::boolean
            OR pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER')
    "#;
    query_scalar::<_, bool>(query)
        .fetch_one(pool)
        .await
        .map_err(|source| CheckerError::QueryError {
            query: query.into(),
            source,
        })
}

async fn detect_pg_stat_statements_version(pool: &Pool<Postgres>) -> Option<i64> {
    pg_stat_statements_has_column(pool, "total_exec_time")
        .await
        .ok()
        .filter(|exists| *exists)
        .map(|_| 130000)
}

async fn pg_stat_statements_has_column(
    pool: &Pool<Postgres>,
    column_name: &str,
) -> Result<bool, CheckerError> {
    let query = r#"
        SELECT EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'pg_stat_statements'
              AND column_name = $1
        )
    "#;
    query_scalar::<_, bool>(query)
        .bind(column_name)
        .fetch_one(pool)
        .await
        .map_err(|source| CheckerError::QueryError {
            query: query.into(),
            source,
        })
}

async fn fetch_pg_stat_statements_info(
    pool: &Pool<Postgres>,
) -> Result<(Option<String>, Option<f64>, Option<i64>), CheckerError> {
    let query = r#"
        SELECT
            stats_reset::text AS stats_reset_at,
            EXTRACT(EPOCH FROM now() - stats_reset) AS seconds_since_reset,
            dealloc::bigint AS entry_deallocations
        FROM pg_stat_statements_info
    "#;

    sqlx::query(query)
        .fetch_optional(pool)
        .await
        .map(|row| {
            row.map_or((None, None, None), |row| {
                (
                    row.get("stats_reset_at"),
                    row.get("seconds_since_reset"),
                    row.get("entry_deallocations"),
                )
            })
        })
        .map_err(|source| CheckerError::QueryError {
            query: query.into(),
            source,
        })
}

async fn resolve_time_columns(
    pool: &Pool<Postgres>,
    results: &mut WorkloadResults,
    server_version: Option<i64>,
) -> TimeColumns {
    let version_num = server_version
        .or(detect_pg_stat_statements_version(pool).await)
        .unwrap_or_else(|| {
            results.warnings.push(
                "Falling back to PostgreSQL 13+ timing columns for pg_stat_statements.".to_string(),
            );
            130000
        });

    if version_num >= 130000 {
        TimeColumns {
            total: "total_exec_time",
            max: "max_exec_time",
        }
    } else {
        TimeColumns {
            total: "total_time",
            max: "max_time",
        }
    }
}

async fn fetch_statements(
    pool: &Pool<Postgres>,
    opts: &WorkloadOptions,
    columns: TimeColumns,
    has_wal_bytes: bool,
) -> Result<Vec<StatementStat>, CheckerError> {
    let fetch_limit = (opts.limit.max(1) * 5).max(50) as i64;
    let metrics = [
        "total_time_ms",
        "mean_time_ms",
        "shared_blks_read",
        "temp_blks_written",
    ];

    let mut map: HashMap<StatementKey, StatementStat> = HashMap::new();

    for metric_column in metrics {
        let query = build_statement_query(columns, metric_column, has_wal_bytes);

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
            let stat = StatementStat {
                queryid: row.get("queryid"),
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
                wal_bytes: row.get("wal_bytes"),
            };
            let key = StatementKey {
                queryid: stat.queryid,
                query: stat.query.clone(),
            };
            if map.contains_key(&key) {
                continue;
            }

            map.insert(key, stat);
        }
    }

    Ok(map.into_values().collect())
}

fn build_statement_query(columns: TimeColumns, metric_column: &str, has_wal_bytes: bool) -> String {
    let wal_bytes_select = if has_wal_bytes {
        "SUM(COALESCE(s.wal_bytes, 0))::bigint AS wal_bytes,"
    } else {
        "NULL::bigint AS wal_bytes,"
    };

    format!(
        r#"
        WITH aggregated AS (
            SELECT
                COALESCE(s.queryid, 0)::bigint AS queryid,
                COALESCE(s.query, '<query text unavailable>') AS query,
                SUM(s.calls)::bigint AS calls,
                SUM(s.rows)::bigint AS rows,
                SUM(s.shared_blks_read)::bigint AS shared_blks_read,
                SUM(s.shared_blks_hit)::bigint AS shared_blks_hit,
                SUM(s.temp_blks_read)::bigint AS temp_blks_read,
                SUM(s.temp_blks_written)::bigint AS temp_blks_written,
                {wal_bytes}
                SUM(s.{total}) AS total_time_ms,
                CASE
                    WHEN SUM(s.calls) > 0
                        THEN SUM(s.{total}) / SUM(s.calls)::double precision
                    ELSE 0
                END AS mean_time_ms,
                MAX(s.{max}) AS max_time_ms
            FROM pg_stat_statements s
            WHERE s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            GROUP BY COALESCE(s.queryid, 0)::bigint, COALESCE(s.query, '<query text unavailable>')
            HAVING SUM(s.calls) >= $1
        )
        SELECT
            queryid,
            query,
            calls,
            rows,
            shared_blks_read,
            shared_blks_hit,
            temp_blks_read,
            temp_blks_written,
            wal_bytes,
            total_time_ms,
            mean_time_ms,
            max_time_ms
        FROM aggregated
        ORDER BY {metric} DESC
        LIMIT $2
        "#,
        wal_bytes = wal_bytes_select,
        total = columns.total,
        max = columns.max,
        metric = metric_column
    )
}

fn build_slow_query_groups(stats: &[StatementStat], opts: &WorkloadOptions) -> Vec<SlowQueryGroup> {
    let total_measured_time_ms: f64 = stats.iter().map(|stat| stat.total_time_ms).sum();
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
                total_time_pct: total_time_pct(stat.total_time_ms, total_measured_time_ms),
                cache_hit_ratio: cache_hit_ratio(stat.shared_blks_hit, stat.shared_blks_read),
                temp_blks_written_per_call: per_call_i64(stat.temp_blks_written, stat.calls),
                wal_bytes: stat.wal_bytes,
                wal_bytes_per_call: stat
                    .wal_bytes
                    .and_then(|wal_bytes| per_call_i64(wal_bytes, stat.calls)),
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

fn total_time_pct(total_time_ms: f64, total_measured_time_ms: f64) -> f64 {
    if total_measured_time_ms <= 0.0 {
        0.0
    } else {
        (total_time_ms / total_measured_time_ms) * 100.0
    }
}

fn cache_hit_ratio(shared_blks_hit: i64, shared_blks_read: i64) -> Option<f64> {
    let total = shared_blks_hit + shared_blks_read;
    if total <= 0 {
        None
    } else {
        Some(shared_blks_hit as f64 / total as f64)
    }
}

fn per_call_i64(value: i64, calls: i64) -> Option<f64> {
    if calls <= 0 {
        None
    } else {
        Some(value as f64 / calls as f64)
    }
}

fn pg_stat_statements_unavailable_warning(source: &Error) -> Option<String> {
    let message = source.as_database_error()?.message();
    if is_pg_stat_statements_preload_error_message(message) {
        Some(format!(
            "pg_stat_statements is installed but not usable: {message}. Add it to shared_preload_libraries and restart PostgreSQL before running workload analysis."
        ))
    } else {
        None
    }
}

fn is_pg_stat_statements_preload_error_message(message: &str) -> bool {
    message.contains("pg_stat_statements must be loaded via shared_preload_libraries")
}

#[derive(Debug, Default)]
struct CandidateBuildResult {
    candidates: Vec<QueryIndexCandidate>,
    coverage_stats: WorkloadCoverageStats,
    parsed_queries: usize,
}

fn build_index_candidates(
    stats: &[StatementStat],
    catalog: &IndexCatalog,
    opts: &WorkloadOptions,
) -> CandidateBuildResult {
    let mut deduped: HashMap<String, QueryIndexCandidate> = HashMap::new();
    let mut coverage_stats = WorkloadCoverageStats::default();
    let mut parsed_queries = 0;

    for stat in stats {
        match parse_query_columns(&stat.query) {
            Ok(usage) => {
                parsed_queries += 1;
                let per_query = build_candidates_for_usage(stat, &usage, catalog);
                merge_coverage_stats(&mut coverage_stats, &per_query.coverage_stats);
                for candidate in per_query.candidates {
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
            Err(_) => coverage_stats.parser_errors += 1,
        }
    }

    let mut candidates: Vec<QueryIndexCandidate> = deduped.into_values().collect();
    candidates.sort_by(|a, b| {
        b.total_time_ms
            .partial_cmp(&a.total_time_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    candidates.truncate(opts.limit * 2);
    CandidateBuildResult {
        candidates,
        coverage_stats,
        parsed_queries,
    }
}

fn merge_coverage_stats(target: &mut WorkloadCoverageStats, source: &WorkloadCoverageStats) {
    target.suppressed_by_existing_index += source.suppressed_by_existing_index;
    target.skipped_internal_tables += source.skipped_internal_tables;
    target.skipped_unresolved_schema += source.skipped_unresolved_schema;
    target.skipped_unsupported_parse_shape += source.skipped_unsupported_parse_shape;
    target.parser_errors += source.parser_errors;
}

fn build_candidates_for_usage(
    stat: &StatementStat,
    usage: &QueryColumnUsage,
    catalog: &IndexCatalog,
) -> CandidateBuildResult {
    let mut table_map = HashMap::new();
    for table in &usage.tables {
        table_map.insert(table.full_name(), table.clone());
    }

    let mut coverage_stats = WorkloadCoverageStats::default();
    let mut candidates = Vec::new();
    for (table_name, usage) in &usage.usage_by_table {
        let table_ref = table_map.get(table_name);
        let Some(table_ref) = table_ref else { continue };
        if is_internal_postgres_table(table_ref) {
            coverage_stats.skipped_internal_tables += 1;
            continue;
        }

        let search_key = SearchKey::from_usage(usage);
        if search_key.is_empty() {
            coverage_stats.skipped_unsupported_parse_shape += 1;
            continue;
        }

        let resolved = resolve_table_schema(table_ref, catalog);
        if resolved.schema == "unknown" {
            coverage_stats.skipped_unresolved_schema += 1;
            continue;
        }

        let mut notes = collect_non_covering_index_notes(&resolved.full_name, &search_key, catalog);
        if resolved.ambiguous_schema {
            notes.push(
                "table name resolved to public, but another schema may contain the same table"
                    .to_string(),
            );
        }
        if is_index_covered(&resolved.full_name, &search_key, catalog) {
            coverage_stats.suppressed_by_existing_index += 1;
            continue;
        }

        let columns = output_columns(&search_key);
        let reason = format_reason(usage, resolved.ambiguous_schema);
        candidates.push(QueryIndexCandidate {
            schema: resolved.schema,
            table: resolved.table,
            columns,
            reason,
            confidence: candidate_confidence(usage, resolved.ambiguous_schema, &notes),
            evidence: evidence_from_usage(usage),
            notes,
            queryid: stat.queryid,
            total_time_ms: stat.total_time_ms,
            mean_time_ms: stat.mean_time_ms,
            calls: stat.calls,
        });
    }

    CandidateBuildResult {
        candidates,
        coverage_stats,
        parsed_queries: 0,
    }
}

fn is_internal_postgres_table(table: &TableRef) -> bool {
    if let Some(schema) = &table.schema {
        return is_internal_postgres_schema(schema);
    }

    table.name.to_ascii_lowercase().starts_with("pg_")
}

fn is_internal_postgres_schema(schema: &str) -> bool {
    let schema = schema.to_ascii_lowercase();
    schema == "pg_catalog"
        || schema == "information_schema"
        || schema.starts_with("pg_toast")
        || schema.starts_with("pg_temp_")
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
    let mut where_columns = Vec::new();
    append_unique(&mut where_columns, &usage.equality_filters);
    append_unique(&mut where_columns, &usage.non_equality_filters);
    if !where_columns.is_empty() {
        parts.push(format!("WHERE {}", where_columns.join(", ")));
    }
    if !usage.equality_joins.is_empty() {
        parts.push(format!("JOIN {}", usage.equality_joins.join(", ")));
    }
    if !usage.orders.is_empty() {
        parts.push(format!("ORDER BY {}", usage.orders.join(", ")));
    }
    if ambiguous_schema {
        parts.push("schema ambiguous".to_string());
    }
    format!("heuristic from slow query: {}", parts.join("; "))
}

fn evidence_from_usage(usage: &TableColumnUsage) -> QueryIndexEvidence {
    QueryIndexEvidence {
        equality_filters: usage.equality_filters.clone(),
        non_equality_filters: usage.non_equality_filters.clone(),
        equality_joins: usage.equality_joins.clone(),
        order_by: usage.orders.clone(),
    }
}

fn candidate_confidence(
    usage: &TableColumnUsage,
    ambiguous_schema: bool,
    notes: &[String],
) -> WorkloadFindingConfidence {
    if ambiguous_schema || (usage.equality_filters.is_empty() && usage.equality_joins.is_empty()) {
        return WorkloadFindingConfidence::Low;
    }

    if !usage.non_equality_filters.is_empty() || !usage.orders.is_empty() || !notes.is_empty() {
        return WorkloadFindingConfidence::Medium;
    }

    WorkloadFindingConfidence::High
}

fn output_columns(search_key: &SearchKey) -> Vec<String> {
    let mut columns = search_key.display_columns.clone();
    if columns.len() > 3 {
        columns.truncate(3);
    }
    columns
}

fn is_index_covered(table: &str, search_key: &SearchKey, catalog: &IndexCatalog) -> bool {
    let Some(indexes) = catalog.indexes_by_table.get(table) else {
        return false;
    };

    indexes
        .iter()
        .any(|index| index_covers_search_key(index, search_key))
}

fn collect_non_covering_index_notes(
    table: &str,
    search_key: &SearchKey,
    catalog: &IndexCatalog,
) -> Vec<String> {
    let Some(indexes) = catalog.indexes_by_table.get(table) else {
        return Vec::new();
    };

    let mut notes = Vec::new();

    if indexes
        .iter()
        .any(|index| index.is_partial && index_overlaps_search_key(index, search_key))
    {
        notes.push(
            "existing partial index ignored because this command cannot prove the query matches its predicate"
                .to_string(),
        );
    }

    if indexes
        .iter()
        .any(|index| index.is_expression && index_overlaps_search_key(index, search_key))
    {
        notes.push(
            "existing expression index ignored because expression equivalence is not proven"
                .to_string(),
        );
    }

    if indexes
        .iter()
        .any(|index| !index.is_valid && index_overlaps_search_key(index, search_key))
    {
        notes.push("existing invalid index ignored".to_string());
    }

    if indexes.iter().any(|index| {
        !index.access_method.eq_ignore_ascii_case("btree")
            && index_overlaps_search_key(index, search_key)
    }) {
        notes.push("existing non-B-tree index not treated as generic coverage".to_string());
    }

    notes
}

fn index_overlaps_search_key(index: &IndexDefinition, search_key: &SearchKey) -> bool {
    index.key_columns.iter().any(|index_column| {
        search_key
            .display_columns
            .iter()
            .any(|candidate_column| index_column.eq_ignore_ascii_case(candidate_column))
    })
}

fn index_covers_search_key(index: &IndexDefinition, search_key: &SearchKey) -> bool {
    if !index.is_valid || index.is_partial || index.is_expression {
        return false;
    }

    if index.access_method.eq_ignore_ascii_case("btree") {
        btree_index_covers_search_key(index, search_key)
    } else {
        has_exact_prefix(&index.key_columns, &search_key.display_columns)
    }
}

fn btree_index_covers_search_key(index: &IndexDefinition, search_key: &SearchKey) -> bool {
    let required_len = search_key.equality_columns.len() + search_key.ordered_columns.len();
    if required_len == 0 || index.key_columns.len() < required_len {
        return false;
    }

    let leading = &index.key_columns[..search_key.equality_columns.len()];
    if !same_column_set(leading, &search_key.equality_columns) {
        return false;
    }

    has_exact_prefix(
        &index.key_columns[search_key.equality_columns.len()..],
        &search_key.ordered_columns,
    )
}

fn same_column_set(left: &[String], right: &[String]) -> bool {
    left.len() == right.len()
        && left.iter().all(|left_value| {
            right
                .iter()
                .any(|right_value| left_value.eq_ignore_ascii_case(right_value))
        })
}

fn has_exact_prefix(index_columns: &[String], target_columns: &[String]) -> bool {
    index_columns.len() >= target_columns.len()
        && index_columns
            .iter()
            .zip(target_columns.iter())
            .all(|(index_column, target_column)| index_column.eq_ignore_ascii_case(target_column))
}

struct ResolvedTable {
    schema: String,
    table: String,
    full_name: String,
    ambiguous_schema: bool,
}

const FETCH_INDEX_CATALOG_QUERY: &str = r#"
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        am.amname AS access_method,
        (i.indpred IS NOT NULL) AS is_partial,
        (i.indexprs IS NOT NULL) AS is_expression,
        i.indisvalid AS is_valid,
        COALESCE(
            array_agg(a.attname ORDER BY arr.ord) FILTER (WHERE a.attname IS NOT NULL),
            ARRAY[]::text[]
        ) AS key_columns
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indrelid
    JOIN pg_class idx ON idx.oid = i.indexrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_am am ON am.oid = idx.relam
    LEFT JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS arr(attnum, ord)
        ON arr.ord <= i.indnkeyatts
    LEFT JOIN pg_attribute a
        ON a.attrelid = c.oid
       AND a.attnum = arr.attnum
       AND arr.attnum > 0
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
    GROUP BY
        n.nspname,
        c.relname,
        idx.relname,
        am.amname,
        i.indpred,
        i.indexprs,
        i.indisvalid
"#;

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
    let rows = sqlx::query(FETCH_INDEX_CATALOG_QUERY)
        .fetch_all(pool)
        .await
        .map_err(|source| CheckerError::QueryError {
            query: FETCH_INDEX_CATALOG_QUERY.into(),
            source,
        })?;

    let mut catalog = IndexCatalog::default();
    for row in rows {
        let definition = IndexDefinition {
            schema: row.get("schema_name"),
            table: row.get("table_name"),
            access_method: row.get("access_method"),
            key_columns: row.get("key_columns"),
            is_partial: row.get("is_partial"),
            is_expression: row.get("is_expression"),
            is_valid: row.get("is_valid"),
        };

        let full_name = format!("{}.{}", definition.schema, definition.table);
        catalog
            .indexes_by_table
            .entry(full_name)
            .or_default()
            .push(definition.clone());

        let entry = catalog
            .schemas_by_table
            .entry(definition.table.clone())
            .or_default();
        if !entry.contains(&definition.schema) {
            entry.push(definition.schema.clone());
        }
    }

    Ok(catalog)
}

pub(crate) fn correlate_table_health(results: &mut WorkloadResults) {
    for candidate in &mut results.query_index_candidates {
        if results.seq_scan_info.iter().any(|table| {
            table.schema.eq_ignore_ascii_case(&candidate.schema)
                && table.table_name.eq_ignore_ascii_case(&candidate.table)
        }) {
            push_unique_note(
                &mut candidate.notes,
                "table is also a sequential scan hotspot".to_string(),
            );
        }

        if results.bloat_info.iter().any(|table| {
            table.schema.eq_ignore_ascii_case(&candidate.schema)
                && table.table_name.eq_ignore_ascii_case(&candidate.table)
        }) {
            push_unique_note(
                &mut candidate.notes,
                "table is also on the bloat watchlist".to_string(),
            );
        }

        let overlapping_unused_indexes: Vec<_> = results
            .index_usage_info
            .iter()
            .filter(|index| {
                index.issue == IndexIssueKind::Unused
                    && index.schema.eq_ignore_ascii_case(&candidate.schema)
                    && index.table_name.eq_ignore_ascii_case(&candidate.table)
                    && index_usage_overlaps_candidate(index, candidate)
            })
            .map(|index| {
                let columns = if index.key_columns.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", index.key_columns.join(", "))
                };
                format!(
                    "unused overlapping index {}{} already exists on this table",
                    index.index_name, columns
                )
            })
            .collect();
        for note in overlapping_unused_indexes {
            push_unique_note(&mut candidate.notes, note);
        }
    }
}

fn push_unique_note(notes: &mut Vec<String>, note: String) {
    if !notes.iter().any(|existing| existing == &note) {
        notes.push(note);
    }
}

fn index_usage_overlaps_candidate(
    index: &crate::models::IndexUsageInfo,
    candidate: &QueryIndexCandidate,
) -> bool {
    !index.key_columns.is_empty()
        && index.key_columns.iter().any(|index_column| {
            candidate
                .columns
                .iter()
                .any(|candidate_column| index_column.eq_ignore_ascii_case(candidate_column))
        })
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
        let table_usage = TableColumnUsage {
            equality_filters: vec!["customer_id".into(), "status".into()],
            equality_joins: vec!["org_id".into()],
            orders: vec!["created_at".into()],
            ..TableColumnUsage::default()
        };
        usage
            .usage_by_table
            .insert("public.orders".into(), table_usage);
        usage
    }

    fn make_stat(queryid: i64, query: &str, total_time_ms: f64) -> StatementStat {
        StatementStat {
            queryid,
            query: query.into(),
            calls: 10,
            total_time_ms,
            mean_time_ms: total_time_ms / 10.0,
            max_time_ms: total_time_ms / 5.0,
            rows: 0,
            shared_blks_read: 0,
            shared_blks_hit: 0,
            temp_blks_read: 0,
            temp_blks_written: 0,
            wal_bytes: None,
        }
    }

    fn make_index_definition(columns: &[&str]) -> IndexDefinition {
        IndexDefinition {
            schema: "public".into(),
            table: "orders".into(),
            access_method: "btree".into(),
            key_columns: columns.iter().map(|column| column.to_string()).collect(),
            is_partial: false,
            is_expression: false,
            is_valid: true,
        }
    }

    #[test]
    fn candidate_orders_columns_by_filter_join_order() {
        let usage = make_usage();
        let catalog = IndexCatalog::default();
        let stat = make_stat(1, "SELECT * FROM orders", 1000.0);

        let candidates = build_candidates_for_usage(&stat, &usage, &catalog);
        assert_eq!(candidates.candidates.len(), 1);
        assert_eq!(
            candidates.candidates[0].columns,
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
        let table_usage = TableColumnUsage {
            equality_filters: vec!["customer_id".into(), "status".into()],
            ..TableColumnUsage::default()
        };
        usage
            .usage_by_table
            .insert("public.orders".into(), table_usage);
        let mut catalog = IndexCatalog::default();
        catalog.indexes_by_table.insert(
            "public.orders".into(),
            vec![make_index_definition(&["status", "customer_id"])],
        );

        let stat = make_stat(1, "SELECT * FROM orders", 1000.0);

        let candidates = build_candidates_for_usage(&stat, &usage, &catalog);
        assert!(candidates.candidates.is_empty());
    }

    #[test]
    fn candidate_dedupes_by_columns() {
        let mut catalog = IndexCatalog::default();
        catalog
            .schemas_by_table
            .insert("orders".into(), vec!["public".into()]);
        let stat_one = make_stat(
            1,
            "SELECT * FROM orders WHERE customer_id = $1 AND status = 'open'",
            1000.0,
        );
        let stat_two = make_stat(
            2,
            "SELECT * FROM orders WHERE customer_id = $1 AND status = 'open'",
            500.0,
        );

        let build =
            build_index_candidates(&[stat_one, stat_two], &catalog, &WorkloadOptions::default());
        assert_eq!(build.candidates.len(), 1);
        assert_eq!(build.candidates[0].queryid, 1);
    }

    #[test]
    fn update_statement_produces_candidate_without_parse_failure() {
        let mut catalog = IndexCatalog::default();
        catalog
            .schemas_by_table
            .insert("orders".into(), vec!["public".into()]);
        let stat = make_stat(
            1,
            "UPDATE orders SET status = 'closed' WHERE customer_id = $1",
            1000.0,
        );

        let build = build_index_candidates(&[stat], &catalog, &WorkloadOptions::default());
        assert_eq!(build.coverage_stats.parser_errors, 0);
        assert!(build.candidates.iter().any(|candidate| {
            candidate.table == "orders"
                && candidate
                    .columns
                    .iter()
                    .any(|column| column == "customer_id")
        }));
    }

    #[test]
    fn delete_statement_produces_candidate_without_parse_failure() {
        let mut catalog = IndexCatalog::default();
        catalog
            .schemas_by_table
            .insert("orders".into(), vec!["public".into()]);
        let stat = make_stat(1, "DELETE FROM orders WHERE customer_id = $1", 1000.0);

        let build = build_index_candidates(&[stat], &catalog, &WorkloadOptions::default());
        assert_eq!(build.coverage_stats.parser_errors, 0);
        assert!(build.candidates.iter().any(|candidate| {
            candidate.table == "orders"
                && candidate
                    .columns
                    .iter()
                    .any(|column| column == "customer_id")
        }));
    }

    #[test]
    fn candidate_skips_internal_postgres_tables() {
        let mut usage = QueryColumnUsage::default();
        usage.tables.push(TableRef {
            schema: None,
            name: "pg_stat_database".into(),
        });
        let table_usage = TableColumnUsage {
            equality_filters: vec!["datid".into()],
            ..TableColumnUsage::default()
        };
        usage
            .usage_by_table
            .insert("pg_stat_database".into(), table_usage);

        let catalog = IndexCatalog::default();
        let stat = make_stat(1, "SELECT * FROM pg_stat_database WHERE datid = 1", 1000.0);

        let candidates = build_candidates_for_usage(&stat, &usage, &catalog);
        assert!(candidates.candidates.is_empty());
    }

    #[test]
    fn candidate_keeps_explicit_non_internal_schema_tables() {
        let mut usage = QueryColumnUsage::default();
        usage.tables.push(TableRef {
            schema: Some("public".into()),
            name: "pg_custom_events".into(),
        });
        let table_usage = TableColumnUsage {
            equality_filters: vec!["account_id".into()],
            ..TableColumnUsage::default()
        };
        usage
            .usage_by_table
            .insert("public.pg_custom_events".into(), table_usage);

        let catalog = IndexCatalog::default();
        let stat = make_stat(
            1,
            "SELECT * FROM public.pg_custom_events WHERE account_id = 42",
            1000.0,
        );

        let candidates = build_candidates_for_usage(&stat, &usage, &catalog);
        assert_eq!(candidates.candidates.len(), 1);
        assert_eq!(candidates.candidates[0].table, "pg_custom_events");
    }

    #[test]
    fn candidate_keeps_partial_index_candidates() {
        let mut usage = QueryColumnUsage::default();
        usage.tables.push(TableRef {
            schema: Some("public".into()),
            name: "orders".into(),
        });
        let table_usage = TableColumnUsage {
            equality_filters: vec!["customer_id".into()],
            ..TableColumnUsage::default()
        };
        usage
            .usage_by_table
            .insert("public.orders".into(), table_usage);

        let mut partial_index = make_index_definition(&["customer_id"]);
        partial_index.is_partial = true;

        let mut catalog = IndexCatalog::default();
        catalog
            .indexes_by_table
            .insert("public.orders".into(), vec![partial_index]);

        let stat = make_stat(1, "SELECT * FROM orders WHERE customer_id = $1", 1000.0);
        let candidates = build_candidates_for_usage(&stat, &usage, &catalog);
        assert_eq!(candidates.candidates.len(), 1);
    }

    #[test]
    fn candidate_keeps_when_include_columns_do_not_form_search_key() {
        let mut usage = QueryColumnUsage::default();
        usage.tables.push(TableRef {
            schema: Some("public".into()),
            name: "orders".into(),
        });
        let table_usage = TableColumnUsage {
            equality_filters: vec!["customer_id".into(), "status".into()],
            ..TableColumnUsage::default()
        };
        usage
            .usage_by_table
            .insert("public.orders".into(), table_usage);

        let mut catalog = IndexCatalog::default();
        catalog.indexes_by_table.insert(
            "public.orders".into(),
            vec![make_index_definition(&["customer_id"])],
        );

        let stat = make_stat(
            1,
            "SELECT * FROM orders WHERE customer_id = $1 AND status = $2",
            1000.0,
        );
        let candidates = build_candidates_for_usage(&stat, &usage, &catalog);
        assert_eq!(candidates.candidates.len(), 1);
    }

    #[test]
    fn candidate_keeps_when_order_sensitive_suffix_is_not_covered() {
        let mut usage = QueryColumnUsage::default();
        usage.tables.push(TableRef {
            schema: Some("public".into()),
            name: "orders".into(),
        });
        let table_usage = TableColumnUsage {
            equality_filters: vec!["customer_id".into(), "status".into()],
            orders: vec!["created_at".into()],
            ..TableColumnUsage::default()
        };
        usage
            .usage_by_table
            .insert("public.orders".into(), table_usage);

        let mut catalog = IndexCatalog::default();
        catalog.indexes_by_table.insert(
            "public.orders".into(),
            vec![make_index_definition(&[
                "status",
                "created_at",
                "customer_id",
            ])],
        );

        let stat = make_stat(
            1,
            "SELECT * FROM orders WHERE customer_id = $1 AND status = $2 ORDER BY created_at",
            1000.0,
        );
        let candidates = build_candidates_for_usage(&stat, &usage, &catalog);
        assert_eq!(candidates.candidates.len(), 1);
    }

    #[test]
    fn candidate_keeps_invalid_index_candidates() {
        let mut usage = QueryColumnUsage::default();
        usage.tables.push(TableRef {
            schema: Some("public".into()),
            name: "orders".into(),
        });
        let table_usage = TableColumnUsage {
            equality_filters: vec!["customer_id".into()],
            ..TableColumnUsage::default()
        };
        usage
            .usage_by_table
            .insert("public.orders".into(), table_usage);

        let mut invalid_index = make_index_definition(&["customer_id"]);
        invalid_index.is_valid = false;

        let mut catalog = IndexCatalog::default();
        catalog
            .indexes_by_table
            .insert("public.orders".into(), vec![invalid_index]);

        let stat = make_stat(1, "SELECT * FROM orders WHERE customer_id = $1", 1000.0);
        let candidates = build_candidates_for_usage(&stat, &usage, &catalog);
        assert_eq!(candidates.candidates.len(), 1);
    }

    #[test]
    fn candidate_keeps_expression_index_candidates() {
        let mut usage = QueryColumnUsage::default();
        usage.tables.push(TableRef {
            schema: Some("public".into()),
            name: "orders".into(),
        });
        let table_usage = TableColumnUsage {
            equality_filters: vec!["customer_id".into()],
            ..TableColumnUsage::default()
        };
        usage
            .usage_by_table
            .insert("public.orders".into(), table_usage);

        let mut expression_index = make_index_definition(&["customer_id"]);
        expression_index.is_expression = true;

        let mut catalog = IndexCatalog::default();
        catalog
            .indexes_by_table
            .insert("public.orders".into(), vec![expression_index]);

        let stat = make_stat(1, "SELECT * FROM orders WHERE customer_id = $1", 1000.0);
        let candidates = build_candidates_for_usage(&stat, &usage, &catalog);
        assert_eq!(candidates.candidates.len(), 1);
    }

    #[test]
    fn statement_query_aggregates_calls_and_times() {
        let query = build_statement_query(
            TimeColumns {
                total: "total_exec_time",
                max: "max_exec_time",
            },
            "total_time_ms",
            true,
        );
        assert!(query.contains("SUM(s.calls)::bigint AS calls"));
        assert!(query.contains("SUM(s.total_exec_time) AS total_time_ms"));
        assert!(query.contains("MAX(s.max_exec_time) AS max_time_ms"));
        assert!(query.contains("SUM(COALESCE(s.wal_bytes, 0))::bigint AS wal_bytes"));
    }

    #[test]
    fn statement_query_groups_by_query_identity_and_aggregated_calls() {
        let query = build_statement_query(
            TimeColumns {
                total: "total_exec_time",
                max: "max_exec_time",
            },
            "shared_blks_read",
            false,
        );
        assert!(query.contains("GROUP BY COALESCE(s.queryid, 0)::bigint, COALESCE(s.query, '<query text unavailable>')"));
        assert!(query.contains("HAVING SUM(s.calls) >= $1"));
        assert!(query.contains("ORDER BY shared_blks_read DESC"));
    }

    #[test]
    fn preload_error_message_is_classified_as_unavailable() {
        assert!(is_pg_stat_statements_preload_error_message(
            "pg_stat_statements must be loaded via shared_preload_libraries"
        ));
    }

    #[test]
    fn fetch_index_catalog_query_limits_to_key_columns() {
        assert!(FETCH_INDEX_CATALOG_QUERY.contains("arr.ord <= i.indnkeyatts"));
        assert!(FETCH_INDEX_CATALOG_QUERY.contains("(i.indpred IS NOT NULL) AS is_partial"));
    }

    #[test]
    fn metadata_warnings_cover_recent_reset_deallocations_and_query_visibility() {
        let snapshot = WorkloadMetadataSnapshot {
            stats_reset_at: Some("2026-03-05 10:00:00+00".into()),
            seconds_since_reset: Some(60.0),
            entry_deallocations: Some(7),
            query_text_visible: false,
            ..WorkloadMetadataSnapshot::default()
        };
        let mut results = WorkloadResults::default();
        add_metadata_warnings(&snapshot, &mut results);

        assert_eq!(results.warnings.len(), 3);
        assert!(results
            .warnings
            .iter()
            .any(|warning| warning.contains("last reset at")));
        assert!(results
            .warnings
            .iter()
            .any(|warning| warning.contains("evicted 7 entries")));
        assert!(results
            .warnings
            .iter()
            .any(|warning| warning.contains("pg_read_all_stats")));
    }

    #[test]
    fn parse_failure_warning_triggers_for_non_trivial_failure_rate() {
        let metadata = WorkloadMetadata {
            parsed_queries: 7,
            parse_failures: 3,
            ..WorkloadMetadata::default()
        };
        let mut results = WorkloadResults::default();
        add_parse_failure_warning(10, &metadata, &mut results);

        assert_eq!(results.warnings.len(), 1);
        assert!(results.warnings[0].contains("index candidate coverage is partial"));
    }

    #[test]
    fn candidate_notes_include_ignored_index_types() {
        let mut usage = QueryColumnUsage::default();
        usage.tables.push(TableRef {
            schema: Some("public".into()),
            name: "orders".into(),
        });
        let table_usage = TableColumnUsage {
            equality_filters: vec!["customer_id".into()],
            ..TableColumnUsage::default()
        };
        usage
            .usage_by_table
            .insert("public.orders".into(), table_usage);

        let mut partial_index = make_index_definition(&["customer_id"]);
        partial_index.is_partial = true;
        let mut expression_index = make_index_definition(&["customer_id"]);
        expression_index.is_expression = true;
        let mut invalid_index = make_index_definition(&["customer_id"]);
        invalid_index.is_valid = false;

        let mut catalog = IndexCatalog::default();
        catalog.indexes_by_table.insert(
            "public.orders".into(),
            vec![partial_index, expression_index, invalid_index],
        );

        let stat = make_stat(1, "SELECT * FROM orders WHERE customer_id = $1", 1000.0);
        let build = build_candidates_for_usage(&stat, &usage, &catalog);

        assert_eq!(build.candidates.len(), 1);
        assert!(build.candidates[0]
            .notes
            .iter()
            .any(|note| note.contains("partial index ignored")));
        assert!(build.candidates[0]
            .notes
            .iter()
            .any(|note| note.contains("expression index ignored")));
        assert!(build.candidates[0]
            .notes
            .iter()
            .any(|note| note.contains("invalid index ignored")));
        assert_eq!(
            build.candidates[0].confidence,
            WorkloadFindingConfidence::Medium
        );
    }

    #[test]
    fn correlate_table_health_adds_table_and_unused_index_notes() {
        let mut results = WorkloadResults {
            query_index_candidates: vec![QueryIndexCandidate {
                schema: "public".into(),
                table: "orders".into(),
                columns: vec!["customer_id".into()],
                reason: "heuristic".into(),
                confidence: WorkloadFindingConfidence::High,
                evidence: QueryIndexEvidence::default(),
                notes: Vec::new(),
                queryid: 1,
                total_time_ms: 10.0,
                mean_time_ms: 1.0,
                calls: 10,
            }],
            seq_scan_info: vec![crate::models::TableSeqScanInfo {
                schema: "public".into(),
                table_name: "orders".into(),
                seq_scan: 10,
                idx_scan: 0,
                live_tuples: 100,
                table_size_bytes: 1024,
                table_size_pretty: "1 kB".into(),
            }],
            bloat_info: vec![crate::models::TableBloatInfo {
                schema: "public".into(),
                table_name: "orders".into(),
                live_tuples: 100,
                dead_tuples: 10,
                dead_tup_ratio: 0.1,
                seq_scan: 10,
                idx_scan: 0,
                table_size_bytes: 1024,
                table_size_pretty: "1 kB".into(),
                last_autovacuum: None,
                last_autoanalyze: None,
                seconds_since_last_autovacuum: None,
                seconds_since_last_autoanalyze: None,
            }],
            index_usage_info: vec![crate::models::IndexUsageInfo {
                issue: IndexIssueKind::Unused,
                schema: "public".into(),
                table_name: "orders".into(),
                index_name: "orders_customer_id_idx".into(),
                key_columns: vec!["customer_id".into()],
                index_size_bytes: 1024,
                index_size_pretty: "1 kB".into(),
                scans: 0,
                tuples_read: 0,
                tuples_fetched: 0,
                avg_tuples_per_scan: 0.0,
                heap_fetch_ratio: 0.0,
                table_live_tup: Some(100),
                is_unique: false,
                enforces_constraint: false,
                is_expression: false,
                is_partial: false,
            }],
            ..WorkloadResults::default()
        };

        correlate_table_health(&mut results);

        assert!(results.query_index_candidates[0]
            .notes
            .iter()
            .any(|note| note.contains("sequential scan hotspot")));
        assert!(results.query_index_candidates[0]
            .notes
            .iter()
            .any(|note| note.contains("bloat watchlist")));
        assert!(results.query_index_candidates[0]
            .notes
            .iter()
            .any(|note| note.contains("unused overlapping index")));
    }
}
