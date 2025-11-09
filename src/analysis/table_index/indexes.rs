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

    Ok(())
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
