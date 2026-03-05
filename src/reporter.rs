use crate::models::{
    AnalysisResults, ConfigCategory, ConfigSuggestion, IndexIssueKind, SlowQueryKind,
    SuggestionLevel, WorkloadResults,
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
            .map_err(std::io::Error::other)
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
                        IndexIssueKind::MissingPartialIndex => {
                            "missing soft-delete partial index".to_string()
                        }
                        IndexIssueKind::BrinCandidate => {
                            "BRIN candidate for time-series/append-only".to_string()
                        }
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

pub struct WorkloadReporter {
    format: ReportFormat,
}

impl WorkloadReporter {
    pub fn new(format: ReportFormat) -> Self {
        Self { format }
    }

    pub fn report(&self, results: &WorkloadResults) -> Result<()> {
        match self.format {
            ReportFormat::Markdown => self.report_markdown(results)?,
            ReportFormat::Json => self.report_json(results)?,
            ReportFormat::Text => self.report_text(results)?,
        }
        Ok(())
    }

    fn report_markdown(&self, results: &WorkloadResults) -> Result<()> {
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        self.write_workload_markdown(&mut handle, results)
    }

    fn write_workload_markdown<W: std::io::Write>(
        &self,
        handle: &mut W,
        results: &WorkloadResults,
    ) -> Result<()> {
        writeln!(handle, "# PostgreSQL Workload Analysis Report\n").context(OutputSnafu)?;
        self.write_workload_summary_markdown(handle, results)?;

        let show_wal = results
            .slow_query_groups
            .iter()
            .flat_map(|group| group.queries.iter())
            .any(|query| query.wal_bytes_per_call.is_some());

        for group in &results.slow_query_groups {
            writeln!(handle, "## {}\n", format_slow_query_kind(group.kind)).context(OutputSnafu)?;
            writeln!(handle, "{}\n", describe_slow_query_kind(group.kind)).context(OutputSnafu)?;
            if group.queries.is_empty() {
                writeln!(handle, "No queries matched the filters.\n").context(OutputSnafu)?;
                continue;
            }

            if show_wal {
                writeln!(
                    handle,
                    "| Query ID | Calls | Total ms | % Total | Mean ms | Max ms | Rows | Shared Read | Temp Written | Cache Hit % | Temp/call | WAL/call | Query |"
                )
                .context(OutputSnafu)?;
                writeln!(
                    handle,
                    "|---------|-------|----------|---------|---------|--------|------|-------------|--------------|-------------|-----------|----------|-------|"
                )
                .context(OutputSnafu)?;
            } else {
                writeln!(
                    handle,
                    "| Query ID | Calls | Total ms | % Total | Mean ms | Max ms | Rows | Shared Read | Temp Written | Cache Hit % | Temp/call | Query |"
                )
                .context(OutputSnafu)?;
                writeln!(
                    handle,
                    "|---------|-------|----------|---------|---------|--------|------|-------------|--------------|-------------|-----------|-------|"
                )
                .context(OutputSnafu)?;
            }

            for query in &group.queries {
                if show_wal {
                    writeln!(
                        handle,
                        "| {} | {} | {:.2} | {:.1}% | {:.2} | {:.2} | {} | {} | {} | {} | {} | {} | {} |",
                        query.queryid,
                        query.calls,
                        query.total_time_ms,
                        query.total_time_pct,
                        query.mean_time_ms,
                        query.max_time_ms,
                        query.rows,
                        query.shared_blks_read,
                        query.temp_blks_written,
                        format_optional_pct(query.cache_hit_ratio),
                        format_optional_f64(query.temp_blks_written_per_call, " blocks"),
                        format_optional_i64_per_call(query.wal_bytes_per_call, " bytes"),
                        query.query_text.replace('|', "\\|")
                    )
                    .context(OutputSnafu)?;
                } else {
                    writeln!(
                        handle,
                        "| {} | {} | {:.2} | {:.1}% | {:.2} | {:.2} | {} | {} | {} | {} | {} | {} |",
                        query.queryid,
                        query.calls,
                        query.total_time_ms,
                        query.total_time_pct,
                        query.mean_time_ms,
                        query.max_time_ms,
                        query.rows,
                        query.shared_blks_read,
                        query.temp_blks_written,
                        format_optional_pct(query.cache_hit_ratio),
                        format_optional_f64(query.temp_blks_written_per_call, " blocks"),
                        query.query_text.replace('|', "\\|")
                    )
                    .context(OutputSnafu)?;
                }
            }
            writeln!(handle).context(OutputSnafu)?;
        }

        if !results.query_index_candidates.is_empty() {
            writeln!(handle, "## Index Candidates (Heuristic)\n").context(OutputSnafu)?;
            writeln!(
                handle,
                "| Table | Columns | Confidence | Calls | Total ms | Mean ms | Query ID | Evidence | Notes | Reason |"
            )
            .context(OutputSnafu)?;
            writeln!(
                handle,
                "|-------|---------|------------|-------|----------|---------|----------|----------|-------|--------|"
            )
            .context(OutputSnafu)?;
            for candidate in &results.query_index_candidates {
                writeln!(
                    handle,
                    "| {}.{} | {} | {} | {} | {:.2} | {:.2} | {} | {} | {} | {} |",
                    candidate.schema,
                    candidate.table,
                    candidate.columns.join(", "),
                    candidate.confidence.as_str(),
                    candidate.calls,
                    candidate.total_time_ms,
                    candidate.mean_time_ms,
                    candidate.queryid,
                    format_candidate_evidence(&candidate.evidence).replace('|', "\\|"),
                    format_notes(&candidate.notes).replace('|', "\\|"),
                    candidate.reason.replace('|', "\\|")
                )
                .context(OutputSnafu)?;
            }
            writeln!(handle).context(OutputSnafu)?;
        }

        if !results.bloat_info.is_empty()
            || !results.seq_scan_info.is_empty()
            || !results.index_usage_info.is_empty()
        {
            self.write_table_index_markdown(handle, results)?;
        }

        Ok(())
    }

    fn write_workload_summary_markdown<W: std::io::Write>(
        &self,
        handle: &mut W,
        results: &WorkloadResults,
    ) -> Result<()> {
        writeln!(handle, "## Summary\n").context(OutputSnafu)?;
        writeln!(
            handle,
            "- **Data source**: `{}`",
            results.workload_metadata.data_source
        )
        .context(OutputSnafu)?;
        writeln!(handle, "- **Scope**: `{}`", results.workload_metadata.scope)
            .context(OutputSnafu)?;
        writeln!(
            handle,
            "- **Stats reset at**: {}",
            results
                .workload_metadata
                .stats_reset_at
                .as_deref()
                .unwrap_or("unknown")
        )
        .context(OutputSnafu)?;
        writeln!(
            handle,
            "- **Entry deallocations**: {}",
            results
                .workload_metadata
                .entry_deallocations
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        )
        .context(OutputSnafu)?;
        writeln!(
            handle,
            "- **Server version**: {}",
            results
                .workload_metadata
                .server_version
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        )
        .context(OutputSnafu)?;
        writeln!(
            handle,
            "- **Query text visible**: {}",
            if results.workload_metadata.query_text_visible {
                "yes"
            } else {
                "no"
            }
        )
        .context(OutputSnafu)?;
        writeln!(
            handle,
            "- **Parsed queries**: {}",
            results.workload_metadata.parsed_queries
        )
        .context(OutputSnafu)?;
        writeln!(
            handle,
            "- **Parse failures**: {}",
            results.workload_metadata.parse_failures
        )
        .context(OutputSnafu)?;
        writeln!(
            handle,
            "- **Suppressed candidates**: {}",
            results.workload_metadata.suppressed_candidates
        )
        .context(OutputSnafu)?;
        writeln!(
            handle,
            "- **Coverage summary**: {} suppressed by existing indexes, {} internal tables skipped, {} unresolved-schema tables skipped, {} unsupported parse shapes, {} parser errors",
            results.coverage_stats.suppressed_by_existing_index,
            results.coverage_stats.skipped_internal_tables,
            results.coverage_stats.skipped_unresolved_schema,
            results.coverage_stats.skipped_unsupported_parse_shape,
            results.coverage_stats.parser_errors
        )
        .context(OutputSnafu)?;
        if results.warnings.is_empty() {
            writeln!(handle, "- **Warnings**: None").context(OutputSnafu)?;
        } else {
            for warning in &results.warnings {
                writeln!(handle, "- **Warning**: {}", warning).context(OutputSnafu)?;
            }
        }
        writeln!(handle).context(OutputSnafu)?;
        Ok(())
    }

    fn write_table_index_markdown<W: std::io::Write>(
        &self,
        handle: &mut W,
        results: &WorkloadResults,
    ) -> Result<()> {
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

                writeln!(handle, "#### {}\n", format_issue_name(&issue)).context(OutputSnafu)?;
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
                        IndexIssueKind::MissingPartialIndex => {
                            "missing soft-delete partial index".to_string()
                        }
                        IndexIssueKind::BrinCandidate => {
                            "BRIN candidate for time-series/append-only".to_string()
                        }
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

    fn report_json(&self, results: &WorkloadResults) -> Result<()> {
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        self.write_workload_json(&mut handle, results)
    }

    fn report_text(&self, results: &WorkloadResults) -> Result<()> {
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        self.write_workload_text(&mut handle, results)
    }

    fn write_workload_json<W: std::io::Write>(
        &self,
        handle: &mut W,
        results: &WorkloadResults,
    ) -> Result<()> {
        let json =
            serde_json::to_string_pretty(results).map_err(|err| ReporterError::OutputError {
                source: std::io::Error::other(err),
            })?;
        writeln!(handle, "{json}").context(OutputSnafu)?;
        Ok(())
    }

    fn write_workload_text<W: std::io::Write>(
        &self,
        handle: &mut W,
        results: &WorkloadResults,
    ) -> Result<()> {
        writeln!(handle, "PostgreSQL Workload Analysis Report").context(OutputSnafu)?;
        writeln!(
            handle,
            "Data source: {} ({})",
            results.workload_metadata.data_source, results.workload_metadata.scope
        )
        .context(OutputSnafu)?;
        writeln!(
            handle,
            "Stats reset at: {}",
            results
                .workload_metadata
                .stats_reset_at
                .as_deref()
                .unwrap_or("unknown")
        )
        .context(OutputSnafu)?;
        writeln!(
            handle,
            "Entry deallocations: {}",
            results
                .workload_metadata
                .entry_deallocations
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        )
        .context(OutputSnafu)?;
        writeln!(
            handle,
            "Query text visible: {}",
            if results.workload_metadata.query_text_visible {
                "yes"
            } else {
                "no"
            }
        )
        .context(OutputSnafu)?;
        writeln!(
            handle,
            "Parsed queries: {}, parse failures: {}, suppressed candidates: {}",
            results.workload_metadata.parsed_queries,
            results.workload_metadata.parse_failures,
            results.workload_metadata.suppressed_candidates
        )
        .context(OutputSnafu)?;
        writeln!(
            handle,
            "Coverage summary: {} suppressed, {} internal, {} unresolved-schema, {} unsupported shapes, {} parser errors",
            results.coverage_stats.suppressed_by_existing_index,
            results.coverage_stats.skipped_internal_tables,
            results.coverage_stats.skipped_unresolved_schema,
            results.coverage_stats.skipped_unsupported_parse_shape,
            results.coverage_stats.parser_errors
        )
        .context(OutputSnafu)?;
        if !results.warnings.is_empty() {
            for warning in &results.warnings {
                writeln!(handle, "Warning: {warning}").context(OutputSnafu)?;
            }
        }
        writeln!(handle).context(OutputSnafu)?;

        for group in &results.slow_query_groups {
            writeln!(handle, "{}:", format_slow_query_kind(group.kind)).context(OutputSnafu)?;
            writeln!(handle, "  {}", describe_slow_query_kind(group.kind)).context(OutputSnafu)?;
            for query in &group.queries {
                writeln!(
                    handle,
                    "  - {} calls, total {:.2}ms ({:.1}% of measured total), mean {:.2}ms, cache hit {}, temp/call {}, queryid {}",
                    query.calls,
                    query.total_time_ms,
                    query.total_time_pct,
                    query.mean_time_ms,
                    format_optional_pct(query.cache_hit_ratio),
                    format_optional_f64(query.temp_blks_written_per_call, " blocks"),
                    query.queryid
                )
                .context(OutputSnafu)?;
                if let Some(wal_bytes_per_call) = query.wal_bytes_per_call {
                    writeln!(handle, "    WAL/call: {:.1} bytes", wal_bytes_per_call)
                        .context(OutputSnafu)?;
                }
            }
            writeln!(handle).context(OutputSnafu)?;
        }

        if !results.query_index_candidates.is_empty() {
            writeln!(handle, "Index Candidates (Heuristic):").context(OutputSnafu)?;
            for candidate in &results.query_index_candidates {
                writeln!(
                    handle,
                    "  - {}.{} ({}) [{}]",
                    candidate.schema,
                    candidate.table,
                    candidate.columns.join(", "),
                    candidate.confidence.as_str()
                )
                .context(OutputSnafu)?;
                writeln!(
                    handle,
                    "    evidence: {}",
                    format_candidate_evidence(&candidate.evidence)
                )
                .context(OutputSnafu)?;
                if !candidate.notes.is_empty() {
                    writeln!(handle, "    notes: {}", format_notes(&candidate.notes))
                        .context(OutputSnafu)?;
                }
            }
            writeln!(handle).context(OutputSnafu)?;
        }

        if !results.bloat_info.is_empty()
            || !results.seq_scan_info.is_empty()
            || !results.index_usage_info.is_empty()
        {
            writeln!(handle, "Table & Index Health:").context(OutputSnafu)?;
            if !results.bloat_info.is_empty() {
                writeln!(handle, "  - Bloat watchlist: {}", results.bloat_info.len())
                    .context(OutputSnafu)?;
            }
            if !results.seq_scan_info.is_empty() {
                writeln!(
                    handle,
                    "  - Seq scan hotspots: {}",
                    results.seq_scan_info.len()
                )
                .context(OutputSnafu)?;
            }
            if !results.index_usage_info.is_empty() {
                writeln!(
                    handle,
                    "  - Index findings: {}",
                    results.index_usage_info.len()
                )
                .context(OutputSnafu)?;
            }
        }

        Ok(())
    }
}

fn format_candidate_evidence(evidence: &crate::models::QueryIndexEvidence) -> String {
    let mut parts = Vec::new();
    if !evidence.equality_filters.is_empty() {
        parts.push(format!("WHERE = {}", evidence.equality_filters.join(", ")));
    }
    if !evidence.non_equality_filters.is_empty() {
        parts.push(format!(
            "WHERE range {}",
            evidence.non_equality_filters.join(", ")
        ));
    }
    if !evidence.equality_joins.is_empty() {
        parts.push(format!("JOIN = {}", evidence.equality_joins.join(", ")));
    }
    if !evidence.order_by.is_empty() {
        parts.push(format!("ORDER BY {}", evidence.order_by.join(", ")));
    }
    if parts.is_empty() {
        "none".to_string()
    } else {
        parts.join("; ")
    }
}

fn format_notes(notes: &[String]) -> String {
    if notes.is_empty() {
        "none".to_string()
    } else {
        notes.join("; ")
    }
}

fn format_optional_pct(value: Option<f64>) -> String {
    value
        .map(|ratio| format!("{:.1}%", ratio * 100.0))
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_optional_f64(value: Option<f64>, unit: &str) -> String {
    value
        .map(|value| format!("{value:.1}{unit}"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_optional_i64_per_call(value: Option<f64>, unit: &str) -> String {
    value
        .map(|value| format!("{value:.1}{unit}"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_slow_query_kind(kind: SlowQueryKind) -> &'static str {
    match kind {
        SlowQueryKind::TotalTime => "Slow Queries by Total Time",
        SlowQueryKind::MeanTime => "Slow Queries by Mean Time",
        SlowQueryKind::SharedBlksRead => "Slow Queries by Shared Blocks Read",
        SlowQueryKind::TempBlksWritten => "Slow Queries by Temp Blocks Written",
    }
}

fn describe_slow_query_kind(kind: SlowQueryKind) -> &'static str {
    match kind {
        SlowQueryKind::TotalTime => {
            "Shows which statements consume the most cumulative execution time since pg_stat_statements was last reset; this is not a time-windowed ranking."
        }
        SlowQueryKind::MeanTime => {
            "Shows which statements are slow per execution within the cumulative pg_stat_statements dataset, useful for reducing end-user latency and fixing expensive query plans."
        }
        SlowQueryKind::SharedBlksRead => {
            "Highlights statements that perform the most disk-backed reads in the cumulative pg_stat_statements dataset, useful for spotting I/O-heavy access patterns and missing indexes."
        }
        SlowQueryKind::TempBlksWritten => {
            "Highlights statements that spill the most temporary blocks in the cumulative pg_stat_statements dataset, useful for identifying costly sort/hash operations and memory pressure."
        }
    }
}

fn format_issue_name(issue: &IndexIssueKind) -> &'static str {
    match issue {
        IndexIssueKind::Unused => "Unused",
        IndexIssueKind::LowSelectivity => "Low Selectivity",
        IndexIssueKind::FailedIndexOnly => "Failed Index-Only",
        IndexIssueKind::MissingPartialIndex => "Missing Partial Index",
        IndexIssueKind::BrinCandidate => "BRIN Candidate",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        QueryIndexCandidate, QueryIndexEvidence, SlowQueryGroup, SlowQueryInfo,
        WorkloadCoverageStats, WorkloadFindingConfidence, WorkloadMetadata,
    };

    fn sample_workload_results() -> WorkloadResults {
        WorkloadResults {
            workload_metadata: WorkloadMetadata {
                stats_reset_at: Some("2026-03-05 10:00:00+00".into()),
                entry_deallocations: Some(7),
                server_version: Some(160004),
                query_text_visible: false,
                parsed_queries: 7,
                parse_failures: 3,
                suppressed_candidates: 2,
                ..WorkloadMetadata::default()
            },
            coverage_stats: WorkloadCoverageStats {
                suppressed_by_existing_index: 2,
                skipped_internal_tables: 1,
                skipped_unresolved_schema: 1,
                skipped_unsupported_parse_shape: 2,
                parser_errors: 3,
            },
            slow_query_groups: vec![SlowQueryGroup {
                kind: SlowQueryKind::TotalTime,
                queries: vec![SlowQueryInfo {
                    queryid: 42,
                    calls: 10,
                    total_time_ms: 500.0,
                    mean_time_ms: 50.0,
                    max_time_ms: 100.0,
                    rows: 25,
                    shared_blks_read: 10,
                    shared_blks_hit: 90,
                    temp_blks_read: 0,
                    temp_blks_written: 20,
                    total_time_pct: 62.5,
                    cache_hit_ratio: Some(0.9),
                    temp_blks_written_per_call: Some(2.0),
                    wal_bytes: Some(2_048),
                    wal_bytes_per_call: Some(204.8),
                    query_text: "select * from orders where customer_id = $1".into(),
                }],
            }],
            query_index_candidates: vec![QueryIndexCandidate {
                schema: "public".into(),
                table: "orders".into(),
                columns: vec!["customer_id".into(), "created_at".into()],
                reason: "heuristic from slow query: WHERE customer_id; ORDER BY created_at".into(),
                confidence: WorkloadFindingConfidence::Low,
                evidence: QueryIndexEvidence {
                    equality_filters: vec!["customer_id".into()],
                    non_equality_filters: Vec::new(),
                    equality_joins: Vec::new(),
                    order_by: vec!["created_at".into()],
                },
                notes: vec![
                    "table name resolved to public, but another schema may contain the same table"
                        .into(),
                    "table is also a sequential scan hotspot".into(),
                ],
                queryid: 42,
                total_time_ms: 500.0,
                mean_time_ms: 50.0,
                calls: 10,
            }],
            warnings: vec![
                "Workload results are cumulative only since pg_stat_statements was last reset at 2026-03-05 10:00:00+00.".into(),
                "pg_stat_statements has evicted 7 entries due to capacity pressure; low-frequency statements and derived findings may be incomplete.".into(),
                "Query text visibility appears limited for the current role; grant pg_read_all_stats to avoid incomplete or anonymized workload findings.".into(),
                "Only 7 of 10 workload statements were parsed into index evidence; index candidate coverage is partial.".into(),
            ],
            ..WorkloadResults::default()
        }
    }

    #[test]
    fn workload_markdown_snapshot_includes_metadata_and_candidate_notes() {
        let reporter = WorkloadReporter::new(ReportFormat::Markdown);
        let results = sample_workload_results();
        let mut output = Vec::new();

        reporter
            .write_workload_markdown(&mut output, &results)
            .expect("markdown workload report should render");

        let rendered = String::from_utf8(output).expect("markdown should be utf8");
        assert!(rendered.contains("# PostgreSQL Workload Analysis Report"));
        assert!(rendered.contains("- **Data source**: `pg_stat_statements`"));
        assert!(rendered.contains("- **Entry deallocations**: 7"));
        assert!(rendered.contains("- **Query text visible**: no"));
        assert!(
            rendered.contains("Only 7 of 10 workload statements were parsed into index evidence")
        );
        assert!(rendered.contains("| public.orders | customer_id, created_at | low |"));
        assert!(rendered.contains("table is also a sequential scan hotspot"));
    }

    #[test]
    fn workload_markdown_reports_none_when_warnings_absent() {
        let reporter = WorkloadReporter::new(ReportFormat::Markdown);
        let mut results = WorkloadResults::default();
        results.workload_metadata.parsed_queries = 1;
        let mut output = Vec::new();

        reporter
            .write_workload_markdown(&mut output, &results)
            .expect("markdown workload report should render");

        let rendered = String::from_utf8(output).expect("markdown should be utf8");
        assert!(rendered.contains("- **Warnings**: None"));
    }

    #[test]
    fn workload_text_snapshot_includes_wal_and_coverage_summary() {
        let reporter = WorkloadReporter::new(ReportFormat::Text);
        let results = sample_workload_results();
        let mut output = Vec::new();

        reporter
            .write_workload_text(&mut output, &results)
            .expect("text workload report should render");

        let rendered = String::from_utf8(output).expect("text should be utf8");
        assert!(rendered.contains("Coverage summary: 2 suppressed, 1 internal, 1 unresolved-schema, 2 unsupported shapes, 3 parser errors"));
        assert!(rendered.contains("WAL/call: 204.8 bytes"));
        assert!(rendered.contains("evidence: WHERE = customer_id; ORDER BY created_at"));
    }

    #[test]
    fn workload_json_snapshot_includes_metadata_and_evidence_fields() {
        let reporter = WorkloadReporter::new(ReportFormat::Json);
        let results = sample_workload_results();
        let mut output = Vec::new();

        reporter
            .write_workload_json(&mut output, &results)
            .expect("json workload report should render");

        let rendered = String::from_utf8(output).expect("json should be utf8");
        assert!(rendered.contains("\"workload_metadata\""));
        assert!(rendered.contains("\"scope\": \"cumulative_since_reset\""));
        assert!(rendered.contains("\"query_text_visible\": false"));
        assert!(rendered.contains("\"confidence\": \"low\""));
        assert!(rendered.contains("\"equality_filters\": ["));
        assert!(rendered.contains("\"notes\": ["));
    }
}
