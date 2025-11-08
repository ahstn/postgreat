use crate::analysis::{get_param, param_value_as_gigabytes, param_value_as_seconds};
use crate::checker::CheckerError;
use crate::models::{
    AnalysisResults, ConfigCategory, ConfigSuggestion, SuggestionLevel, SystemStats,
};
use std::collections::HashMap;

type Result<T> = std::result::Result<T, CheckerError>;

/// Analyzes Write-Ahead Log and checkpoint configuration
pub fn analyze_wal(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    analyze_max_wal_size(params, stats, results)?;
    analyze_checkpoint_timeout(params, stats, results)?;
    analyze_checkpoint_completion_target(params, stats, results)?;

    Ok(())
}

fn analyze_max_wal_size(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    if let Some(param) = get_param(params, "max_wal_size") {
        let current_value = param.current_value.clone();
        let current_gb = param_value_as_gigabytes(param).unwrap_or(0);

        let recommended_gb = match stats.total_memory_gb {
            Some(mem) if mem <= 16.0 => 4,
            Some(mem) if mem <= 64.0 => 16,
            Some(mem) if mem > 64.0 => 32,
            _ => 16,
        };

        if current_gb < recommended_gb {
            add_suggestion(
                results,
                ConfigCategory::Wal,
                "max_wal_size",
                &current_value,
                &format!("{}GB", recommended_gb),
                if current_gb == 1 {
                    SuggestionLevel::Critical
                } else {
                    SuggestionLevel::Important
                },
                &format!(
                    "max_wal_size is too low at {}GB. The default (1GB) is far too low for \
                     production write-heavy workloads and causes frequent, size-based checkpoints. \
                     This creates bursty I/O. Set it high enough that checkpoints are time-based, \
                     not size-based ({}GB for your system).",
                    current_gb, recommended_gb
                ),
            );
        }
    }

    Ok(())
}

fn analyze_checkpoint_timeout(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    if let Some(param) = get_param(params, "checkpoint_timeout") {
        let current_value = param.current_value.clone();
        let current_seconds = param_value_as_seconds(param).unwrap_or(0);

        // Default is 5 minutes (300 seconds) which is good for OLTP
        // For OLAP/batch loads, 15-30 minutes is better
        let recommendation = if is_oltp_workload(stats, params) {
            "5min (good for OLTP)"
        } else {
            "15min (better for OLAP/batch loads)"
        };

        if current_seconds < 300 {
            add_suggestion(
                results,
                ConfigCategory::Wal,
                "checkpoint_timeout",
                &current_value,
                recommendation,
                SuggestionLevel::Important,
                "checkpoint_timeout is too short. Shorter timeouts cause more frequent \
                 checkpoints, increasing I/O load. For OLTP, 5min is a good balance. \
                 For OLAP/batch loads, 15-30min reduces checkpoints during long-running jobs.",
            );
        } else if !is_oltp_workload(stats, params) && current_seconds < 900 {
            // OLAP with less than 15 minutes
            add_suggestion(
                results,
                ConfigCategory::Wal,
                "checkpoint_timeout",
                &current_value,
                "15min",
                SuggestionLevel::Recommended,
                "For OLAP/batch workloads, consider increasing checkpoint_timeout to 15-30min \
                 to reduce the number of checkpoints during long-running operations.",
            );
        }
    }

    Ok(())
}

fn analyze_checkpoint_completion_target(
    params: &HashMap<String, crate::models::PgConfigParam>,
    _stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    if let Some(param) = get_param(params, "checkpoint_completion_target") {
        let current_value = param.current_value.clone();
        let current = current_value.parse::<f64>().unwrap_or(0.5);

        if (current - 0.9).abs() > 0.1 {
            let level = if current == 0.5 {
                SuggestionLevel::Important
            } else {
                SuggestionLevel::Recommended
            };

            add_suggestion(
                results,
                ConfigCategory::Wal,
                "checkpoint_completion_target",
                &current_value,
                "0.9",
                level,
                "checkpoint_completion_target is the I/O smoothing knob. The default 0.5 \
                 creates a high-intensity I/O spike during checkpoints. Setting it to 0.9 \
                 spreads the same amount of I/O over 90% of the checkpoint interval, \
                 creating low, slow, continuous background writes.",
            );
        }
    }

    Ok(())
}

// Helper functions

fn is_oltp_workload(
    stats: &SystemStats,
    params: &HashMap<String, crate::models::PgConfigParam>,
) -> bool {
    if let Some(active) = stats.connection_count {
        if active >= 200 {
            return true;
        }
    }

    if let Some(param) = get_param(params, "max_connections") {
        if let Ok(max_conn) = param.current_value.parse::<usize>() {
            if max_conn >= 200 {
                return true;
            }
        }
    }

    true
}

fn add_suggestion(
    results: &mut AnalysisResults,
    category: ConfigCategory,
    parameter: &str,
    current_value: &str,
    suggested_value: &str,
    level: SuggestionLevel,
    rationale: &str,
) {
    let suggestion = ConfigSuggestion {
        parameter: parameter.to_string(),
        current_value: current_value.to_string(),
        suggested_value: suggested_value.to_string(),
        level,
        rationale: rationale.to_string(),
    };

    results
        .suggestions_by_category
        .entry(category)
        .or_default()
        .push(suggestion);
}
