use crate::models::{
    AnalysisResults, ConfigCategory, ConfigSuggestion, IndexIssueKind, SuggestionLevel,
};
use clap::ValueEnum;
use snafu::{ResultExt, Snafu};
use std::collections::HashMap;

#[derive(Debug, Snafu)]
pub enum ReporterError {
    #[snafu(display("Failed to write output: {}", source))]
    OutputError { source: std::io::Error },
}

type Result<T, E = ReporterError> = std::result::Result<T, E>;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ReportFormat {
    /// Markdown formatted report
    Markdown,
    /// JSON formatted report
    Json,
    /// Plain text summary
    Text,
}

pub struct Reporter {
    format: ReportFormat,
}

impl Reporter {
    pub fn new(format: ReportFormat) -> Self {
        Self { format }
    }

    pub fn report(&self, results: &AnalysisResults) -> Result<()> {
        match self.format {
            ReportFormat::Markdown => self.report_markdown(results)?,
            ReportFormat::Json => self.report_json(results)?,
            ReportFormat::Text => self.report_text(results)?,
        }
        Ok(())
    }

    fn report_markdown(&self, results: &AnalysisResults) -> Result<()> {
        use std::io::Write;

        let stdout = std::io::stdout();
        let mut handle = stdout.lock();

        // Header
        writeln!(handle, "# PostgreSQL Configuration Analysis Report\n").context(OutputSnafu)?;

        // Summary statistics
        writeln!(handle, "## System Information\n").context(OutputSnafu)?;
        writeln!(
            handle,
            "- **Total Memory**: {}",
            results
                .system_stats
                .total_memory_gb
                .map(|gb| format!("{:.1} GB", gb))
                .unwrap_or_else(|| "Unknown".to_string())
        )
        .context(OutputSnafu)?;
        writeln!(
            handle,
            "- **vCPU Count**: {}",
            results
                .system_stats
                .cpu_count
                .map(|cpu| cpu.to_string())
                .unwrap_or_else(|| "Unknown".to_string())
        )
        .context(OutputSnafu)?;
        writeln!(
            handle,
            "- **Active Connections**: {}",
            results
                .system_stats
                .connection_count
                .map(|conn| conn.to_string())
                .unwrap_or_else(|| "Unknown".to_string())
        )
        .context(OutputSnafu)?;
        writeln!(
            handle,
            "- **Configuration Parameters**: {}",
            results.params.len()
        )
        .context(OutputSnafu)?;
        writeln!(handle).context(OutputSnafu)?;

        // Summary of suggestions by level
        let mut level_counts: HashMap<SuggestionLevel, usize> = HashMap::new();
        let total_suggestions: usize = results
            .suggestions_by_category
            .values()
            .map(|s| s.len())
            .sum();

        for suggestions in results.suggestions_by_category.values() {
            for suggestion in suggestions {
                *level_counts.entry(suggestion.level).or_insert(0) += 1;
            }
        }

        if total_suggestions > 0 {
            writeln!(handle, "## Summary of Suggestions\n").context(OutputSnafu)?;
            writeln!(
                handle,
                "Found **{}** configuration suggestions:",
                total_suggestions
            )
            .context(OutputSnafu)?;
            writeln!(handle).context(OutputSnafu)?;

            for (level, count) in &[
                (
                    SuggestionLevel::Critical,
                    level_counts
                        .get(&SuggestionLevel::Critical)
                        .copied()
                        .unwrap_or(0),
                ),
                (
                    SuggestionLevel::Important,
                    level_counts
                        .get(&SuggestionLevel::Important)
                        .copied()
                        .unwrap_or(0),
                ),
                (
                    SuggestionLevel::Recommended,
                    level_counts
                        .get(&SuggestionLevel::Recommended)
                        .copied()
                        .unwrap_or(0),
                ),
                (
                    SuggestionLevel::Info,
                    level_counts
                        .get(&SuggestionLevel::Info)
                        .copied()
                        .unwrap_or(0),
                ),
            ] {
                if *count > 0 {
                    writeln!(
                        handle,
                        "- **{} ({})**: {} suggestions",
                        level.as_str(),
                        self.format_level_badge(level),
                        count
                    )
                    .context(OutputSnafu)?;
                }
            }
            writeln!(handle).context(OutputSnafu)?;
        }

        // Detailed suggestions by category
        let mut categories: Vec<ConfigCategory> =
            results.suggestions_by_category.keys().copied().collect();
        categories.sort_by_key(|c| c.as_str());

        for category in categories {
            let suggestions = &results.suggestions_by_category[&category];

            // Sort by level (Critical first)
            let mut sorted_suggestions = suggestions.clone();
            sorted_suggestions.sort_by(|a, b| {
                let level_order = |level: &SuggestionLevel| match level {
                    SuggestionLevel::Critical => 0,
                    SuggestionLevel::Important => 1,
                    SuggestionLevel::Recommended => 2,
                    SuggestionLevel::Info => 3,
                };
                level_order(&a.level).cmp(&level_order(&b.level))
            });

            writeln!(handle, "## {}\n", category.as_str()).context(OutputSnafu)?;

            for suggestion in &sorted_suggestions {
                self.write_suggestion_markdown(&mut handle, suggestion)?;
            }

            writeln!(handle).context(OutputSnafu)?;
        }

        // Table & Index health summary
        if !results.bloat_info.is_empty()
            || !results.seq_scan_info.is_empty()
            || !results.index_usage_info.is_empty()
        {
            self.write_table_index_markdown(&mut handle, results)?;
        }

        // System configuration table
        writeln!(handle, "---\n").context(OutputSnafu)?;
        writeln!(handle, "## Current Configuration\n").context(OutputSnafu)?;
        writeln!(
            handle,
            "<details>\n<summary>Click to view all configuration parameters</summary>\n"
        )
        .context(OutputSnafu)?;
        writeln!(handle).context(OutputSnafu)?;

        writeln!(handle, "| Parameter | Current Value | Unit | Context |").context(OutputSnafu)?;
        writeln!(handle, "|-----------|--------------|------|---------|").context(OutputSnafu)?;

        let mut params: Vec<_> = results.params.values().collect();
        params.sort_by_key(|p| &p.name);

        for param in params {
            let unit = param.unit.as_deref().unwrap_or("");
            writeln!(
                handle,
                "| {} | {} | {} | {} |",
                param.name, param.current_value, unit, param.context
            )
            .context(OutputSnafu)?;
        }

        writeln!(handle).context(OutputSnafu)?;
        writeln!(handle, "</details>\n").context(OutputSnafu)?;

        Ok(())
    }

    fn write_suggestion_markdown(
        &self,
        handle: &mut std::io::StdoutLock,
        suggestion: &ConfigSuggestion,
    ) -> Result<()> {
        use std::io::Write;

        let level_badge = self.format_level_badge(&suggestion.level);

        writeln!(handle, "### {} {}\n", suggestion.parameter, level_badge).context(OutputSnafu)?;

        writeln!(handle, "**Current Value**: `{}`", suggestion.current_value)
            .context(OutputSnafu)?;
        writeln!(
            handle,
            "**Suggested Value**: `{}`",
            suggestion.suggested_value
        )
        .context(OutputSnafu)?;
        writeln!(handle).context(OutputSnafu)?;

        writeln!(handle, "**Rationale**:\n").context(OutputSnafu)?;
        writeln!(handle, "{}", suggestion.rationale).context(OutputSnafu)?;
        writeln!(handle).context(OutputSnafu)?;

        Ok(())
    }

    fn format_level_badge(&self, level: &SuggestionLevel) -> String {
        let badge = match level {
            SuggestionLevel::Critical => "![CRITICAL](https://img.shields.io/badge/CRITICAL-red)",
            SuggestionLevel::Important => {
                "![IMPORTANT](https://img.shields.io/badge/IMPORTANT-orange)"
            }
            SuggestionLevel::Recommended => {
                "![RECOMMENDED](https://img.shields.io/badge/RECOMMENDED-yellow)"
            }
            SuggestionLevel::Info => "![INFO](https://img.shields.io/badge/INFO-blue)",
        };
        badge.to_string()
    }

    fn report_json(&self, results: &AnalysisResults) -> Result<()> {
        use serde_json;

        let json = serde_json::to_string_pretty(results)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .context(OutputSnafu)?;

        println!("{}", json);
        Ok(())
    }

    fn report_text(&self, results: &AnalysisResults) -> Result<()> {
        use std::io::Write;

        let stdout = std::io::stdout();
        let mut handle = stdout.lock();

        writeln!(handle, "PostgreSQL Configuration Analysis Report").context(OutputSnafu)?;
        writeln!(handle, "==========================================\n").context(OutputSnafu)?;

        // Summary
        let total_suggestions: usize = results
            .suggestions_by_category
            .values()
            .map(|s| s.len())
            .sum();

        writeln!(handle, "Summary:").context(OutputSnafu)?;
        writeln!(handle, "  Total Suggestions: {}", total_suggestions).context(OutputSnafu)?;

        for (level, count) in &[
            (
                SuggestionLevel::Critical,
                results
                    .suggestions_by_category
                    .values()
                    .flat_map(|s| s.iter())
                    .filter(|s| s.level == SuggestionLevel::Critical)
                    .count(),
            ),
            (
                SuggestionLevel::Important,
                results
                    .suggestions_by_category
                    .values()
                    .flat_map(|s| s.iter())
                    .filter(|s| s.level == SuggestionLevel::Important)
                    .count(),
            ),
            (
                SuggestionLevel::Recommended,
                results
                    .suggestions_by_category
                    .values()
                    .flat_map(|s| s.iter())
                    .filter(|s| s.level == SuggestionLevel::Recommended)
                    .count(),
            ),
            (
                SuggestionLevel::Info,
                results
                    .suggestions_by_category
                    .values()
                    .flat_map(|s| s.iter())
                    .filter(|s| s.level == SuggestionLevel::Info)
                    .count(),
            ),
        ] {
            if *count > 0 {
                writeln!(
                    handle,
                    "  {} ({}): {}",
                    level.as_str(),
                    self.format_level_text(level),
                    count
                )
                .context(OutputSnafu)?;
            }
        }

        writeln!(handle).context(OutputSnafu)?;

        // Suggestions by category
        for (category, suggestions) in &results.suggestions_by_category {
            if !suggestions.is_empty() {
                writeln!(handle, "{}", category.as_str()).context(OutputSnafu)?;
                writeln!(handle, "{}", "=".repeat(category.as_str().len())).context(OutputSnafu)?;
                writeln!(handle).context(OutputSnafu)?;

                for suggestion in suggestions {
                    writeln!(
                        handle,
                        "  [{}] {}",
                        self.format_level_text(&suggestion.level),
                        suggestion.parameter
                    )
                    .context(OutputSnafu)?;
                    writeln!(handle, "    Current:  {}", suggestion.current_value)
                        .context(OutputSnafu)?;
                    writeln!(handle, "    Suggest:  {}", suggestion.suggested_value)
                        .context(OutputSnafu)?;
                    writeln!(handle, "    Why:      {}", suggestion.rationale)
                        .context(OutputSnafu)?;
                    writeln!(handle).context(OutputSnafu)?;
                }
            }
        }

        if !results.bloat_info.is_empty() {
            writeln!(handle, "Table Bloat Watchlist:").context(OutputSnafu)?;
            for table in &results.bloat_info {
                writeln!(
                    handle,
                    "  - {}.{}: {:.1}% dead tuples (last autovacuum: {})",
                    table.schema,
                    table.table_name,
                    table.dead_tup_ratio * 100.0,
                    table.last_autovacuum.as_deref().unwrap_or("never")
                )
                .context(OutputSnafu)?;
            }
            writeln!(handle).context(OutputSnafu)?;
        }

        if !results.seq_scan_info.is_empty() {
            writeln!(handle, "Sequential Scan Hotspots:").context(OutputSnafu)?;
            for table in &results.seq_scan_info {
                writeln!(
                    handle,
                    "  - {}.{}: {} seq vs {} idx scans ({} rows, {})",
                    table.schema,
                    table.table_name,
                    table.seq_scan,
                    table.idx_scan,
                    table.live_tuples,
                    table.table_size_pretty
                )
                .context(OutputSnafu)?;
            }
            writeln!(handle).context(OutputSnafu)?;
        }

        if !results.index_usage_info.is_empty() {
            writeln!(handle, "Index Findings:").context(OutputSnafu)?;
            for index in &results.index_usage_info {
                writeln!(
                    handle,
                    "  - [{}] {}.{} on {}.{} ({})",
                    self.format_issue_name(&index.issue),
                    index.schema,
                    index.index_name,
                    index.schema,
                    index.table_name,
                    index.index_size_pretty
                )
                .context(OutputSnafu)?;
            }
            writeln!(handle).context(OutputSnafu)?;
        }

        Ok(())
    }

    fn format_level_text(&self, level: &SuggestionLevel) -> &str {
        match level {
            SuggestionLevel::Critical => "CRIT",
            SuggestionLevel::Important => "IMP",
            SuggestionLevel::Recommended => "REC",
            SuggestionLevel::Info => "INFO",
        }
    }

    fn write_table_index_markdown(
        &self,
        handle: &mut std::io::StdoutLock,
        results: &AnalysisResults,
    ) -> Result<()> {
        use std::io::Write;

        writeln!(handle, "## Table & Index Health\n").context(OutputSnafu)?;

        if !results.bloat_info.is_empty() {
            writeln!(handle, "### Table Bloat Watchlist\n").context(OutputSnafu)?;
            writeln!(
                handle,
                "| Table | Dead % | Dead Tuples | Live Tuples | Last Autovacuum | Size |"
            )
            .context(OutputSnafu)?;
            writeln!(
                handle,
                "|-------|--------|-------------|-------------|-----------------|------|"
            )
            .context(OutputSnafu)?;

            for table in &results.bloat_info {
                writeln!(
                    handle,
                    "| {}.{} | {:.1}% | {} | {} | {} | {} |",
                    table.schema,
                    table.table_name,
                    table.dead_tup_ratio * 100.0,
                    table.dead_tuples,
                    table.live_tuples,
                    table.last_autovacuum.as_deref().unwrap_or("never"),
                    table.table_size_pretty
                )
                .context(OutputSnafu)?;
            }
            writeln!(handle).context(OutputSnafu)?;
        }

        if !results.seq_scan_info.is_empty() {
            writeln!(handle, "### Sequential Scan Hotspots\n").context(OutputSnafu)?;
            writeln!(
                handle,
                "| Table | Seq Scans | Idx Scans | Live Tuples | Size |"
            )
            .context(OutputSnafu)?;
            writeln!(
                handle,
                "|-------|-----------|-----------|-------------|------|"
            )
            .context(OutputSnafu)?;

            for table in &results.seq_scan_info {
                writeln!(
                    handle,
                    "| {}.{} | {} | {} | {} | {} |",
                    table.schema,
                    table.table_name,
                    table.seq_scan,
                    table.idx_scan,
                    table.live_tuples,
                    table.table_size_pretty
                )
                .context(OutputSnafu)?;
            }
            writeln!(handle).context(OutputSnafu)?;
        }

        if !results.index_usage_info.is_empty() {
            writeln!(handle, "### Index Findings\n").context(OutputSnafu)?;
            for issue in [
                IndexIssueKind::Unused,
                IndexIssueKind::LowSelectivity,
                IndexIssueKind::FailedIndexOnly,
                IndexIssueKind::MissingPartialIndex,
                IndexIssueKind::BrinCandidate,
            ] {
                let group: Vec<_> = results
                    .index_usage_info
                    .iter()
                    .filter(|idx| idx.issue == issue)
                    .collect();
                if group.is_empty() {
                    continue;
                }

                writeln!(handle, "#### {}\n", self.format_issue_name(&issue))
                    .context(OutputSnafu)?;
                writeln!(handle, "| Index | Table | Scans | Size | Notes |")
                    .context(OutputSnafu)?;
                writeln!(handle, "|-------|-------|-------|------|-------|")
                    .context(OutputSnafu)?;

                for idx in group {
                    let notes = match idx.issue {
                        IndexIssueKind::Unused => "never scanned".to_string(),
                        IndexIssueKind::LowSelectivity => {
                            let percentage = selectivity_ratio(idx) * 100.0;
                            format!("~{:.1}% of table per scan", percentage.min(100.0))
                        }
                        IndexIssueKind::FailedIndexOnly => {
                            format!("{:.0}% heap fetch ratio", idx.heap_fetch_ratio * 100.0)
                        }
                        IndexIssueKind::MissingPartialIndex => "missing soft-delete partial index".to_string(),
                        IndexIssueKind::BrinCandidate => "BRIN candidate for time-series/append-only".to_string(),
                    };

                    writeln!(
                        handle,
                        "| {}.{} | {}.{} | {} | {} | {} |",
                        idx.schema,
                        idx.index_name,
                        idx.schema,
                        idx.table_name,
                        idx.scans,
                        idx.index_size_pretty,
                        notes
                    )
                    .context(OutputSnafu)?;
                }
                writeln!(handle).context(OutputSnafu)?;
            }
        }

        Ok(())
    }

    fn format_issue_name(&self, issue: &IndexIssueKind) -> &str {
        match issue {
            IndexIssueKind::Unused => "Unused",
            IndexIssueKind::LowSelectivity => "Low Selectivity",
            IndexIssueKind::FailedIndexOnly => "Failed Index-Only",
            IndexIssueKind::MissingPartialIndex => "Missing Partial Index",
            IndexIssueKind::BrinCandidate => "BRIN Candidate",
        }
    }
}

fn selectivity_ratio(index: &crate::models::IndexUsageInfo) -> f64 {
    let table_rows = index.table_live_tup.unwrap_or(0) as f64;
    if table_rows <= 0.0 {
        0.0
    } else {
        index.avg_tuples_per_scan / table_rows
    }
}
