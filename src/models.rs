use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a PostgreSQL configuration parameter with its current value and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgConfigParam {
    pub name: String,
    pub current_value: String,
    pub default_value: Option<String>,
    pub unit: Option<String>,
    pub context: String,
}

/// Represents a suggestion level for configuration improvements
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SuggestionLevel {
    /// Critical issues that need immediate attention
    Critical,
    /// Important improvements that should be addressed
    Important,
    /// Recommended optimizations for better performance
    Recommended,
    /// Informational suggestions or best practices
    Info,
}

impl SuggestionLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            SuggestionLevel::Critical => "CRITICAL",
            SuggestionLevel::Important => "IMPORTANT",
            SuggestionLevel::Recommended => "RECOMMENDED",
            SuggestionLevel::Info => "INFO",
        }
    }
}

/// Represents a single configuration suggestion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigSuggestion {
    /// The configuration parameter name
    pub parameter: String,
    /// Current value
    pub current_value: String,
    /// Suggested value
    pub suggested_value: String,
    /// The suggestion level (Critical, Important, Recommended, Info)
    pub level: SuggestionLevel,
    /// Rationale for the suggestion
    pub rationale: String,
}

/// Represents a category of configuration settings
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigCategory {
    /// Memory allocation parameters
    Memory,
    /// Concurrency and parallelism
    Concurrency,
    /// Write-Ahead Log (WAL) and checkpoint settings
    Wal,
    /// Query planner cost model
    Planner,
    /// Autovacuum settings
    Autovacuum,
    /// Logging and diagnostics
    Logging,
    /// Table and index health checks
    TableIndex,
}

impl ConfigCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            ConfigCategory::Memory => "Memory Configuration",
            ConfigCategory::Concurrency => "Concurrency and Parallelism",
            ConfigCategory::Wal => "WAL and Checkpoint Management",
            ConfigCategory::Planner => "Query Planner Cost Model",
            ConfigCategory::Autovacuum => "Autovacuum Configuration",
            ConfigCategory::Logging => "Logging and Diagnostics",
            ConfigCategory::TableIndex => "Table and Index Health",
        }
    }
}

/// Represents a table bloat analysis result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableBloatInfo {
    pub schema: String,
    pub table_name: String,
    pub live_tuples: i64,
    pub dead_tuples: i64,
    pub dead_tup_ratio: f64,
    pub seq_scan: i64,
    pub idx_scan: i64,
    pub table_size_bytes: i64,
    pub table_size_pretty: String,
    pub last_autovacuum: Option<String>,
    pub last_autoanalyze: Option<String>,
    pub seconds_since_last_autovacuum: Option<f64>,
    pub seconds_since_last_autoanalyze: Option<f64>,
}

/// Represents an index usage analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexUsageInfo {
    pub issue: IndexIssueKind,
    pub schema: String,
    pub table_name: String,
    pub index_name: String,
    pub index_size_bytes: i64,
    pub index_size_pretty: String,
    pub scans: i64,
    pub tuples_read: i64,
    pub tuples_fetched: i64,
    pub avg_tuples_per_scan: f64,
    pub heap_fetch_ratio: f64,
    pub table_live_tup: Option<i64>,
    pub is_unique: bool,
    pub enforces_constraint: bool,
    pub is_expression: bool,
    pub is_partial: bool,
}

/// Represents sequential scan hotspots that likely require new indexes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSeqScanInfo {
    pub schema: String,
    pub table_name: String,
    pub seq_scan: i64,
    pub idx_scan: i64,
    pub live_tuples: i64,
    pub table_size_bytes: i64,
    pub table_size_pretty: String,
}

/// Types of index issues detected during analysis
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IndexIssueKind {
    Unused,
    LowSelectivity,
    FailedIndexOnly,
    MissingPartialIndex,
    BrinCandidate,
}

/// Represents system statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SystemStats {
    pub shared_buffers: u64,
    pub work_mem: u64,
    pub maintenance_work_mem: u64,
    pub total_memory_gb: Option<f64>,
    pub cpu_count: Option<usize>,
    pub connection_count: Option<usize>,
    pub storage_type: crate::config::StorageType,
    pub workload_type: crate::config::WorkloadType,
    pub checkpoints_timed: Option<i64>,
    pub checkpoints_req: Option<i64>,
}

/// Overall analysis results
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct AnalysisResults {
    /// All configuration parameters
    pub params: HashMap<String, PgConfigParam>,
    /// Suggestions grouped by category
    pub suggestions_by_category: HashMap<ConfigCategory, Vec<ConfigSuggestion>>,
    /// Table bloat information
    pub bloat_info: Vec<TableBloatInfo>,
    /// Sequential scan hotspots
    pub seq_scan_info: Vec<TableSeqScanInfo>,
    /// Index usage information (unused/inefficient/etc.)
    pub index_usage_info: Vec<IndexUsageInfo>,
    /// System statistics
    pub system_stats: SystemStats,
}

impl AnalysisResults {
    pub fn merge(&mut self, other: AnalysisResults) {
        self.params.extend(other.params);
        for (category, suggestions) in other.suggestions_by_category {
            self.suggestions_by_category
                .entry(category)
                .or_default()
                .extend(suggestions);
        }
        self.bloat_info.extend(other.bloat_info);
        self.seq_scan_info.extend(other.seq_scan_info);
        self.index_usage_info.extend(other.index_usage_info);
        self.system_stats = other.system_stats;
    }
}

/// Represents groups of slow queries by category.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowQueryGroup {
    pub kind: SlowQueryKind,
    pub queries: Vec<SlowQueryInfo>,
}

/// Categories of slow queries in workload analysis.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SlowQueryKind {
    TotalTime,
    MeanTime,
    SharedBlksRead,
    TempBlksWritten,
}

/// Represents a single slow query entry from pg_stat_statements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowQueryInfo {
    pub queryid: i64,
    pub calls: i64,
    pub total_time_ms: f64,
    pub mean_time_ms: f64,
    pub max_time_ms: f64,
    pub rows: i64,
    pub shared_blks_read: i64,
    pub shared_blks_hit: i64,
    pub temp_blks_read: i64,
    pub temp_blks_written: i64,
    pub query_text: String,
}

/// Represents a heuristic index candidate derived from slow queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryIndexCandidate {
    pub schema: String,
    pub table: String,
    pub columns: Vec<String>,
    pub reason: String,
    pub queryid: i64,
    pub total_time_ms: f64,
    pub mean_time_ms: f64,
    pub calls: i64,
}

/// Workload analysis results for slow query and index candidate reporting.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkloadResults {
    pub slow_query_groups: Vec<SlowQueryGroup>,
    pub query_index_candidates: Vec<QueryIndexCandidate>,
    pub index_usage_info: Vec<IndexUsageInfo>,
    pub seq_scan_info: Vec<TableSeqScanInfo>,
    pub bloat_info: Vec<TableBloatInfo>,
    pub warnings: Vec<String>,
    pub parse_failures: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analysis_results_are_serializable() {
        let mut results = AnalysisResults::default();
        results.suggestions_by_category.insert(
            ConfigCategory::Memory,
            vec![ConfigSuggestion {
                parameter: "shared_buffers".into(),
                current_value: "16384".into(),
                suggested_value: "8GB".into(),
                level: SuggestionLevel::Critical,
                rationale: "test".into(),
            }],
        );

        serde_json::to_string(&results).expect("AnalysisResults should serialize");
    }
}
