use super::push_table_index_suggestion;
use crate::checker::CheckerError;
use crate::models::{AnalysisResults, SuggestionLevel, TableBloatInfo, TableSeqScanInfo};
use sqlx::{Pool, Postgres, Row};
use std::cmp::Ordering;

const TABLE_BLOAT_MIN_ROWS: i64 = 10_000;
const TABLE_MIN_SIZE_BYTES: i64 = 5 * 1024 * 1024; // 5MB
const TABLE_DEAD_RATIO_ALERT: f64 = 0.20;
const TABLE_DEAD_RATIO_CRITICAL: f64 = 0.50;
const AUTOVAC_STALE_SECONDS: f64 = 86_400.0; // 24h
const MAX_BLOAT_RESULTS: usize = 10;
const MAX_SEQ_SCAN_RESULTS: usize = 10;
const SEQ_SCAN_MULTIPLIER: i64 = 50;

#[derive(Debug, Clone)]
struct TableStatRow {
    schema: String,
    table_name: String,
    live_tuples: i64,
    dead_tuples: i64,
    seq_scan: i64,
    idx_scan: i64,
    table_size_bytes: i64,
    table_size_pretty: String,
    last_autovacuum: Option<String>,
    last_autoanalyze: Option<String>,
    seconds_since_last_autovacuum: Option<f64>,
    seconds_since_last_autoanalyze: Option<f64>,
}

impl TableStatRow {
    fn dead_ratio(&self) -> f64 {
        if self.live_tuples <= 0 {
            0.0
        } else {
            self.dead_tuples as f64 / self.live_tuples as f64
        }
    }
}

pub(super) async fn analyze(
    pool: &Pool<Postgres>,
    results: &mut AnalysisResults,
) -> Result<(), CheckerError> {
    let table_rows = fetch_table_stats(pool).await?;

    let bloat_candidates = identify_bloat_tables(&table_rows);
    results.bloat_info = bloat_candidates.clone();
    add_bloat_suggestions(&bloat_candidates, results);

    let seq_scan_candidates = identify_seq_scan_hotspots(&table_rows);
    results.seq_scan_info = seq_scan_candidates.clone();
    add_seq_scan_suggestions(&seq_scan_candidates, results);

    Ok(())
}

async fn fetch_table_stats(pool: &Pool<Postgres>) -> Result<Vec<TableStatRow>, CheckerError> {
    const QUERY: &str = r#"
        SELECT
            s.schemaname,
            s.relname,
            COALESCE(s.n_live_tup, 0) AS n_live_tup,
            COALESCE(s.n_dead_tup, 0) AS n_dead_tup,
            COALESCE(s.seq_scan, 0) AS seq_scan,
            COALESCE(s.idx_scan, 0) AS idx_scan,
            pg_relation_size(s.relid) AS table_size_bytes,
            pg_size_pretty(pg_relation_size(s.relid)) AS table_size_pretty,
            to_char(s.last_autovacuum, 'YYYY-MM-DD HH24:MI:SS') AS last_autovacuum_text,
            to_char(s.last_autoanalyze, 'YYYY-MM-DD HH24:MI:SS') AS last_autoanalyze_text,
            EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - s.last_autovacuum)) AS seconds_since_last_autovacuum,
            EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - s.last_autoanalyze)) AS seconds_since_last_autoanalyze
        FROM pg_stat_user_tables s
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
        stats.push(TableStatRow {
            schema: row.get("schemaname"),
            table_name: row.get("relname"),
            live_tuples: row.get("n_live_tup"),
            dead_tuples: row.get("n_dead_tup"),
            seq_scan: row.get("seq_scan"),
            idx_scan: row.get("idx_scan"),
            table_size_bytes: row.get("table_size_bytes"),
            table_size_pretty: row.get("table_size_pretty"),
            last_autovacuum: row.get("last_autovacuum_text"),
            last_autoanalyze: row.get("last_autoanalyze_text"),
            seconds_since_last_autovacuum: row.get("seconds_since_last_autovacuum"),
            seconds_since_last_autoanalyze: row.get("seconds_since_last_autoanalyze"),
        });
    }

    Ok(stats)
}

fn identify_bloat_tables(rows: &[TableStatRow]) -> Vec<TableBloatInfo> {
    let mut candidates: Vec<TableBloatInfo> = rows
        .iter()
        .filter(|row| {
            row.live_tuples >= TABLE_BLOAT_MIN_ROWS
                && row.table_size_bytes >= TABLE_MIN_SIZE_BYTES
                && row.dead_ratio() >= TABLE_DEAD_RATIO_ALERT
        })
        .map(|row| TableBloatInfo {
            schema: row.schema.clone(),
            table_name: row.table_name.clone(),
            live_tuples: row.live_tuples,
            dead_tuples: row.dead_tuples,
            dead_tup_ratio: row.dead_ratio(),
            seq_scan: row.seq_scan,
            idx_scan: row.idx_scan,
            table_size_bytes: row.table_size_bytes,
            table_size_pretty: row.table_size_pretty.clone(),
            last_autovacuum: row.last_autovacuum.clone(),
            last_autoanalyze: row.last_autoanalyze.clone(),
            seconds_since_last_autovacuum: row.seconds_since_last_autovacuum,
            seconds_since_last_autoanalyze: row.seconds_since_last_autoanalyze,
        })
        .collect();

    candidates.sort_by(|a, b| {
        b.dead_tup_ratio
            .partial_cmp(&a.dead_tup_ratio)
            .unwrap_or(Ordering::Equal)
    });
    candidates.truncate(MAX_BLOAT_RESULTS);
    candidates
}

fn identify_seq_scan_hotspots(rows: &[TableStatRow]) -> Vec<TableSeqScanInfo> {
    let mut hotspots: Vec<TableSeqScanInfo> = rows
        .iter()
        .filter(|row| {
            row.live_tuples >= TABLE_BLOAT_MIN_ROWS
                && row.table_size_bytes >= TABLE_MIN_SIZE_BYTES
                && row.seq_scan * SEQ_SCAN_MULTIPLIER > row.idx_scan.max(1)
        })
        .map(|row| TableSeqScanInfo {
            schema: row.schema.clone(),
            table_name: row.table_name.clone(),
            seq_scan: row.seq_scan,
            idx_scan: row.idx_scan,
            live_tuples: row.live_tuples,
            table_size_bytes: row.table_size_bytes,
            table_size_pretty: row.table_size_pretty.clone(),
        })
        .collect();

    hotspots.sort_by(|a, b| b.seq_scan.cmp(&a.seq_scan));
    hotspots.truncate(MAX_SEQ_SCAN_RESULTS);
    hotspots
}

fn add_bloat_suggestions(tables: &[TableBloatInfo], results: &mut AnalysisResults) {
    for table in tables {
        let stale_autovacuum = table
            .seconds_since_last_autovacuum
            .map(|secs| secs > AUTOVAC_STALE_SECONDS)
            .unwrap_or(true);
        let level = if table.dead_tup_ratio >= TABLE_DEAD_RATIO_CRITICAL && stale_autovacuum {
            SuggestionLevel::Critical
        } else if stale_autovacuum {
            SuggestionLevel::Important
        } else {
            SuggestionLevel::Recommended
        };
        let rationale = if stale_autovacuum {
            format!(
                "{} has {:.1}% dead tuples but its last autovacuum ran {}. This indicates autovacuum tuning is not keeping up; increase per-table autovacuum aggressiveness (lower scale factor/threshold) or schedule a manual VACUUM to prune bloat.",
                format_table_name(table),
                table.dead_tup_ratio * 100.0,
                table
                    .last_autovacuum
                    .as_deref()
                    .unwrap_or("no recorded autovacuum")
            )
        } else {
            format!(
                "{} shows {:.1}% dead tuples even after a recent autovacuum. High-churn workloads may need more aggressive autovacuum settings or targeted VACUUM (FULL) during low-traffic windows.",
                format_table_name(table),
                table.dead_tup_ratio * 100.0
            )
        };

        push_table_index_suggestion(
            results,
            &format!("table {} bloat", format_table_name(table)),
            &format!("{:.1}% dead tuples", table.dead_tup_ratio * 100.0),
            "Reduce dead tuples with VACUUM or tighter autovacuum thresholds",
            level,
            &rationale,
        );
    }
}

fn add_seq_scan_suggestions(hotspots: &[TableSeqScanInfo], results: &mut AnalysisResults) {
    for table in hotspots {
        let full_table_name = format!("{}.{}", table.schema, table.table_name);
        let rationale = format!(
            "{} has {} sequential scans vs {} index scans on ~{} rows ({}). This matches the guidance from docs/6: filter-heavy queries are falling back to seq scans on sizable tables. Investigate pg_stat_statements for the offending queries and create composite/partial indexes to cover their predicates.",
            full_table_name,
            table.seq_scan,
            table.idx_scan,
            table.live_tuples,
            table.table_size_pretty
        );

        push_table_index_suggestion(
            results,
            &format!("table {} sequential scans", full_table_name),
            &format!("{} seq / {} idx scans", table.seq_scan, table.idx_scan),
            "Add or extend indexes to lower sequential scans",
            SuggestionLevel::Important,
            &rationale,
        );
    }
}

fn format_table_name(table: &TableBloatInfo) -> String {
    format!("{}.{}", table.schema, table.table_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_bloat_tables_by_ratio_and_size() {
        let rows = vec![TableStatRow {
            schema: "public".into(),
            table_name: "orders".into(),
            live_tuples: 200_000,
            dead_tuples: 60_000,
            seq_scan: 10,
            idx_scan: 500,
            table_size_bytes: 200 * 1024 * 1024,
            table_size_pretty: "200 MB".into(),
            last_autovacuum: Some("2025-11-01 01:00:00".into()),
            last_autoanalyze: Some("2025-11-01 01:00:00".into()),
            seconds_since_last_autovacuum: Some(2000.0),
            seconds_since_last_autoanalyze: Some(2000.0),
        }];

        let candidates = identify_bloat_tables(&rows);
        assert_eq!(candidates.len(), 1);
        assert!(candidates[0].dead_tup_ratio > 0.2);
    }

    #[test]
    fn detects_seq_scan_hotspots_only_when_seq_dominates() {
        let rows = vec![TableStatRow {
            schema: "public".into(),
            table_name: "events".into(),
            live_tuples: 150_000,
            dead_tuples: 1_000,
            seq_scan: 1000,
            idx_scan: 5,
            table_size_bytes: 100 * 1024 * 1024,
            table_size_pretty: "100 MB".into(),
            last_autovacuum: None,
            last_autoanalyze: None,
            seconds_since_last_autovacuum: None,
            seconds_since_last_autoanalyze: None,
        }];

        let hotspots = identify_seq_scan_hotspots(&rows);
        assert_eq!(hotspots.len(), 1);
    }
}
