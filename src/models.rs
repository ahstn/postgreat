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
}

/// Represents an index usage analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexUsageInfo {
    pub schema: String,
    pub table_name: String,
    pub index_name: String,
    pub index_size: String,
    pub scans: i64,
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
    /// Index usage information
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
        self.index_usage_info.extend(other.index_usage_info);
        self.system_stats = other.system_stats;
    }
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
