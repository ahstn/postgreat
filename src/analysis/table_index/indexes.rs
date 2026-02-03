use super::push_table_index_suggestion;
use crate::checker::CheckerError;
use crate::models::{AnalysisResults, IndexIssueKind, IndexUsageInfo, SuggestionLevel};
use sqlx::{Pool, Postgres, Row};
use std::cmp::Ordering;

const MAX_INDEX_RESULTS_PER_KIND: usize = 8;
const MIN_INDEX_SIZE_BYTES: i64 = 5 * 1024 * 1024;
const LOW_SELECTIVITY_SCAN_THRESHOLD: i64 = 50;
const FAILED_INDEX_ONLY_MIN_TUP_READ: i64 = 10_000;
const LARGE_TABLE_MIN_ROWS: i64 = 10_000;

#[derive(Debug, Clone)]
struct IndexStatRow {
    schema: String,
    table_name: String,
    index_name: String,
    index_size_bytes: i64,
    index_size_pretty: String,
    idx_scan: i64,
    idx_tup_read: i64,
    idx_tup_fetch: i64,
    table_live_tup: Option<i64>,
    is_unique: bool,
    enforces_constraint: bool,
    is_expression: bool,
    is_partial: bool,
}

impl IndexStatRow {
    fn avg_tuples_per_scan(&self) -> f64 {
        if self.idx_scan <= 0 {
            0.0
        } else {
            self.idx_tup_read as f64 / self.idx_scan as f64
        }
    }

    fn heap_fetch_ratio(&self) -> f64 {
        if self.idx_tup_read <= 0 {
            0.0
        } else {
            self.idx_tup_fetch as f64 / self.idx_tup_read as f64
        }
    }
}

pub(super) async fn analyze(
    pool: &Pool<Postgres>,
    results: &mut AnalysisResults,
) -> Result<(), CheckerError> {
    let index_rows = fetch_index_stats(pool).await?;

    let unused_indexes = identify_unused_indexes(&index_rows);
    let low_selectivity_indexes = identify_low_selectivity_indexes(&index_rows);
    let failed_index_only_indexes = identify_failed_index_only_indexes(&index_rows);

    let mut index_findings = Vec::new();
    index_findings.extend(unused_indexes.clone());
    index_findings.extend(low_selectivity_indexes.clone());
    index_findings.extend(failed_index_only_indexes.clone());
    results.index_usage_info = index_findings;

    add_index_suggestions(&unused_indexes, results);
    add_index_suggestions(&low_selectivity_indexes, results);
    add_index_suggestions(&failed_index_only_indexes, results);

    // New checks from docs/2
    let soft_delete_candidates = fetch_soft_delete_candidates(pool).await?;
    let missing_partial_indexes = identify_missing_partial_indexes(&soft_delete_candidates);
    add_index_suggestions(&missing_partial_indexes, results);
    results.index_usage_info.extend(missing_partial_indexes);

    let brin_candidates = fetch_brin_candidates(pool).await?;
    let brin_findings = identify_brin_candidates(&brin_candidates);
    add_index_suggestions(&brin_findings, results);
    results.index_usage_info.extend(brin_findings);

    Ok(())
}

#[derive(Debug)]
struct SoftDeleteCandidate {
    schema: String,
    table_name: String,
    column_name: String,
    table_size_pretty: String,
}

async fn fetch_soft_delete_candidates(pool: &Pool<Postgres>) -> Result<Vec<SoftDeleteCandidate>, CheckerError> {
    // Find tables with soft-delete columns that DO NOT have a partial index filtering on them
    const QUERY: &str = r#"
        WITH soft_delete_cols AS (
            SELECT n.nspname, c.relname, a.attname, c.oid AS relid
            FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE a.attname IN ('is_deleted', 'deleted_at', 'archived', 'is_archived')
              AND c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ),
        tables_with_partial_idx AS (
            SELECT DISTINCT indrelid
            FROM pg_index
            WHERE indispartial = true
        )
        SELECT
            s.nspname,
            s.relname,
            s.attname,
            pg_size_pretty(pg_relation_size(s.relid)) as size_pretty
        FROM soft_delete_cols s
        LEFT JOIN tables_with_partial_idx p ON s.relid = p.indrelid
        WHERE p.indrelid IS NULL -- Table has no partial indexes at all (simplification, but effective)
    "#;

    let rows = sqlx::query(QUERY)
        .fetch_all(pool)
        .await
        .map_err(|source| CheckerError::QueryError {
            query: QUERY.into(),
            source,
        })?;

    let mut candidates = Vec::new();
    for row in rows {
        candidates.push(SoftDeleteCandidate {
            schema: row.get("nspname"),
            table_name: row.get("relname"),
            column_name: row.get("attname"),
            table_size_pretty: row.get("size_pretty"),
        });
    }
    Ok(candidates)
}

fn identify_missing_partial_indexes(candidates: &[SoftDeleteCandidate]) -> Vec<IndexUsageInfo> {
    candidates.iter().map(|c| IndexUsageInfo {
        issue: IndexIssueKind::MissingPartialIndex,
        schema: c.schema.clone(),
        table_name: c.table_name.clone(),
        index_name: format!("(missing on {})", c.column_name),
        index_size_bytes: 0,
        index_size_pretty: "0 B".to_string(),
        scans: 0,
        tuples_read: 0,
        tuples_fetched: 0,
        avg_tuples_per_scan: 0.0,
        heap_fetch_ratio: 0.0,
        table_live_tup: None,
        is_unique: false,
        enforces_constraint: false,
        is_expression: false,
        is_partial: false,
    }).collect()
}

#[derive(Debug)]
struct BrinCandidate {
    schema: String,
    table_name: String,
    column_name: String,
    correlation: f64,
    table_size_pretty: String,
}

async fn fetch_brin_candidates(pool: &Pool<Postgres>) -> Result<Vec<BrinCandidate>, CheckerError> {
    // Find large tables with highly correlated columns (good for BRIN) that are NOT the PK (usually)
    const QUERY: &str = r#"
        SELECT
            s.schemaname,
            s.tablename,
            s.attname,
            s.correlation,
            pg_size_pretty(pg_relation_size(c.oid)) as size_pretty
        FROM pg_stats s
        JOIN pg_class c ON c.relname = s.tablename
        JOIN pg_namespace n ON c.relnamespace = n.oid AND n.nspname = s.schemaname
        LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indnatts = 1 -- Check if single col index exists
        WHERE s.schemaname NOT IN ('pg_catalog', 'information_schema')
          AND abs(s.correlation) > 0.95
          AND pg_relation_size(c.oid) > 10000000 -- > 10MB
          AND c.relkind = 'r'
    "#;

    let rows = sqlx::query(QUERY)
        .fetch_all(pool)
        .await
        .map_err(|source| CheckerError::QueryError {
            query: QUERY.into(),
            source,
        })?;

    let mut candidates = Vec::new();
    for row in rows {
        candidates.push(BrinCandidate {
            schema: row.get("schemaname"),
            table_name: row.get("tablename"),
            column_name: row.get("attname"),
            correlation: row.get("correlation"),
            table_size_pretty: row.get("size_pretty"),
        });
    }
    Ok(candidates)
}

fn identify_brin_candidates(candidates: &[BrinCandidate]) -> Vec<IndexUsageInfo> {
    // Filter to top candidates
    candidates.iter().take(5).map(|c| IndexUsageInfo {
        issue: IndexIssueKind::BrinCandidate,
        schema: c.schema.clone(),
        table_name: c.table_name.clone(),
        index_name: c.column_name.clone(), // Use column name as proxy
        index_size_bytes: 0,
        index_size_pretty: "0 B".to_string(),
        scans: 0,
        tuples_read: 0,
        tuples_fetched: 0,
        avg_tuples_per_scan: 0.0,
        heap_fetch_ratio: 0.0,
        table_live_tup: None,
        is_unique: false,
        enforces_constraint: false,
        is_expression: false,
        is_partial: false,
    }).collect()
}

async fn fetch_index_stats(pool: &Pool<Postgres>) -> Result<Vec<IndexStatRow>, CheckerError> {
    const QUERY: &str = r#"
        SELECT
            s.schemaname,
            s.relname,
            s.indexrelname,
            s.idx_scan,
            s.idx_tup_read,
            s.idx_tup_fetch,
            pg_relation_size(s.indexrelid) AS index_size_bytes,
            pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size_pretty,
            t.n_live_tup,
            i.indisunique,
            i.indispartial,
            (i.indexprs IS NOT NULL) AS is_expression,
            EXISTS (
                SELECT 1 FROM pg_constraint c WHERE c.conindid = s.indexrelid
            ) AS enforces_constraint
        FROM pg_stat_user_indexes s
        JOIN pg_index i ON s.indexrelid = i.indexrelid
        LEFT JOIN pg_stat_user_tables t ON t.relid = s.relid
    "#;

    let rows =
        sqlx::query(QUERY)
            .fetch_all(pool)
            .await
            .map_err(|source| CheckerError::QueryError {
                query: QUERY.into(),
                source,
            })?;

    let mut stats = Vec::with_capacity(rows.len());
    for row in rows {
        stats.push(IndexStatRow {
            schema: row.get("schemaname"),
            table_name: row.get("relname"),
            index_name: row.get("indexrelname"),
            index_size_bytes: row.get("index_size_bytes"),
            index_size_pretty: row.get("index_size_pretty"),
            idx_scan: row.get("idx_scan"),
            idx_tup_read: row.get("idx_tup_read"),
            idx_tup_fetch: row.get("idx_tup_fetch"),
            table_live_tup: row.get("n_live_tup"),
            is_unique: row.get("indisunique"),
            enforces_constraint: row.get("enforces_constraint"),
            is_expression: row.get("is_expression"),
            is_partial: row.get("indispartial"),
        });
    }

    Ok(stats)
}

fn identify_unused_indexes(rows: &[IndexStatRow]) -> Vec<IndexUsageInfo> {
    let mut unused: Vec<IndexUsageInfo> = rows
        .iter()
        .filter(|row| {
            row.idx_scan == 0
                && row.index_size_bytes >= MIN_INDEX_SIZE_BYTES
                && !row.is_unique
                && !row.enforces_constraint
                && !row.is_expression
                && !row.is_partial
        })
        .map(|row| IndexUsageInfo {
            issue: IndexIssueKind::Unused,
            schema: row.schema.clone(),
            table_name: row.table_name.clone(),
            index_name: row.index_name.clone(),
            index_size_bytes: row.index_size_bytes,
            index_size_pretty: row.index_size_pretty.clone(),
            scans: row.idx_scan,
            tuples_read: row.idx_tup_read,
            tuples_fetched: row.idx_tup_fetch,
            avg_tuples_per_scan: 0.0,
            heap_fetch_ratio: 0.0,
            table_live_tup: row.table_live_tup,
            is_unique: row.is_unique,
            enforces_constraint: row.enforces_constraint,
            is_expression: row.is_expression,
            is_partial: row.is_partial,
        })
        .collect();

    unused.sort_by(|a, b| b.index_size_bytes.cmp(&a.index_size_bytes));
    unused.truncate(MAX_INDEX_RESULTS_PER_KIND);
    unused
}

fn identify_low_selectivity_indexes(rows: &[IndexStatRow]) -> Vec<IndexUsageInfo> {
    let mut findings: Vec<IndexUsageInfo> = rows
        .iter()
        .filter(|row| {
            let avg = row.avg_tuples_per_scan();
            let table_rows = row.table_live_tup.unwrap_or(0) as f64;
            row.idx_scan >= LOW_SELECTIVITY_SCAN_THRESHOLD
                && row.table_live_tup.unwrap_or(0) >= LARGE_TABLE_MIN_ROWS
                && table_rows > 0.0
                && avg / table_rows >= 0.5
        })
        .map(|row| IndexUsageInfo {
            issue: IndexIssueKind::LowSelectivity,
            schema: row.schema.clone(),
            table_name: row.table_name.clone(),
            index_name: row.index_name.clone(),
            index_size_bytes: row.index_size_bytes,
            index_size_pretty: row.index_size_pretty.clone(),
            scans: row.idx_scan,
            tuples_read: row.idx_tup_read,
            tuples_fetched: row.idx_tup_fetch,
            avg_tuples_per_scan: row.avg_tuples_per_scan(),
            heap_fetch_ratio: row.heap_fetch_ratio(),
            table_live_tup: row.table_live_tup,
            is_unique: row.is_unique,
            enforces_constraint: row.enforces_constraint,
            is_expression: row.is_expression,
            is_partial: row.is_partial,
        })
        .collect();

    findings.sort_by(|a, b| {
        let a_ratio = selectivity_ratio(a);
        let b_ratio = selectivity_ratio(b);
        b_ratio.partial_cmp(&a_ratio).unwrap_or(Ordering::Equal)
    });
    findings.truncate(MAX_INDEX_RESULTS_PER_KIND);
    findings
}

fn identify_failed_index_only_indexes(rows: &[IndexStatRow]) -> Vec<IndexUsageInfo> {
    let mut findings: Vec<IndexUsageInfo> = rows
        .iter()
        .filter(|row| {
            row.idx_tup_read >= FAILED_INDEX_ONLY_MIN_TUP_READ
                && row.heap_fetch_ratio() >= 0.9
                && row.idx_scan >= LOW_SELECTIVITY_SCAN_THRESHOLD
        })
        .map(|row| IndexUsageInfo {
            issue: IndexIssueKind::FailedIndexOnly,
            schema: row.schema.clone(),
            table_name: row.table_name.clone(),
            index_name: row.index_name.clone(),
            index_size_bytes: row.index_size_bytes,
            index_size_pretty: row.index_size_pretty.clone(),
            scans: row.idx_scan,
            tuples_read: row.idx_tup_read,
            tuples_fetched: row.idx_tup_fetch,
            avg_tuples_per_scan: row.avg_tuples_per_scan(),
            heap_fetch_ratio: row.heap_fetch_ratio(),
            table_live_tup: row.table_live_tup,
            is_unique: row.is_unique,
            enforces_constraint: row.enforces_constraint,
            is_expression: row.is_expression,
            is_partial: row.is_partial,
        })
        .collect();

    findings.sort_by(|a, b| {
        b.heap_fetch_ratio
            .partial_cmp(&a.heap_fetch_ratio)
            .unwrap_or(Ordering::Equal)
    });
    findings.truncate(MAX_INDEX_RESULTS_PER_KIND);
    findings
}

fn add_index_suggestions(indexes: &[IndexUsageInfo], results: &mut AnalysisResults) {
    for index in indexes {
        let parameter = format!("index {}.{}", index.schema, index.index_name);
        let (suggested_value, level, rationale) = match index.issue {
            IndexIssueKind::Unused => (
                "Drop unused index",
                SuggestionLevel::Important,
                format!(
                    "{} has never been scanned and is not enforcing a constraint. Dropping it reclaims {} and removes write overhead, per docs/6 guidance.",
                    parameter,
                    index.index_size_pretty
                ),
            ),
            IndexIssueKind::LowSelectivity => {
                let table_rows = index.table_live_tup.unwrap_or(0) as f64;
                let selectivity = if table_rows > 0.0 {
                    (index.avg_tuples_per_scan / table_rows * 100.0).min(100.0)
                } else {
                    0.0
                };
                (
                    "Replace with more selective (composite/partial) index",
                    SuggestionLevel::Recommended,
                    format!(
                        "{} returns ~{:.1}% of {} on each scan ({} tuples/read). This low selectivity means the planner touches a large fraction of the table; redesign the index per docs/6 section C.2.",
                        parameter,
                        selectivity,
                        format!("{}.{}", index.schema, index.table_name),
                        index.avg_tuples_per_scan as i64
                    ),
                )
            }
            IndexIssueKind::FailedIndexOnly => (
                "Add INCLUDE columns or VACUUM to refresh visibility",
                SuggestionLevel::Recommended,
                format!(
                    "{} performs index scans but still fetches heap pages {:.0}% of the time. Either add the missing SELECT columns via INCLUDE or VACUUM to refresh the visibility map so index-only scans can succeed, per docs/6 section C.3.",
                    parameter,
                    index.heap_fetch_ratio * 100.0
                ),
            ),
            IndexIssueKind::MissingPartialIndex => (
                "Create partial index on soft-delete column",
                SuggestionLevel::Important,
                format!(
                    "Table {}.{} has a soft-delete column but lacks a partial index. Add 'WHERE is_deleted = false' (or deleted_at IS NULL) to excludes dead rows from the index, reducing size and maintenance overhead.",
                    index.schema, index.table_name
                ),
            ),
            IndexIssueKind::BrinCandidate => (
                "Replace B-Tree with BRIN index",
                SuggestionLevel::Recommended,
                format!(
                    "Table {}.{} is large and physically ordered by {}. A BRIN index would be 100x smaller than a B-Tree while maintaining scan performance for range queries.",
                    index.schema, index.table_name, index.index_name
                ),
            ),
        };

        push_table_index_suggestion(
            results,
            &parameter,
            &format!("{} scans", index.scans),
            suggested_value,
            level,
            &rationale,
        );
    }
}

fn selectivity_ratio(index: &IndexUsageInfo) -> f64 {
    let table_rows = index.table_live_tup.unwrap_or(0) as f64;
    if table_rows <= 0.0 {
        0.0
    } else {
        index.avg_tuples_per_scan / table_rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_low_selectivity_index_when_half_table_scanned() {
        let rows = vec![IndexStatRow {
            schema: "public".into(),
            table_name: "sessions".into(),
            index_name: "sessions_user_id_idx".into(),
            index_size_bytes: 50 * 1024 * 1024,
            index_size_pretty: "50 MB".into(),
            idx_scan: 100,
            idx_tup_read: 45_000_000,
            idx_tup_fetch: 44_000_000,
            table_live_tup: Some(900_000),
            is_unique: false,
            enforces_constraint: false,
            is_expression: false,
            is_partial: false,
        }];

        let findings = identify_low_selectivity_indexes(&rows);
        assert_eq!(findings.len(), 1);
        assert!(matches!(findings[0].issue, IndexIssueKind::LowSelectivity));
    }
}
