use crate::checker::CheckerError;
use crate::models::{AnalysisResults, ConfigCategory, ConfigSuggestion, SuggestionLevel};
use std::collections::HashMap;

type Result<T> = std::result::Result<T, CheckerError>;

/// Analyzes logging and diagnostics configuration
pub fn analyze_logging(
    params: &HashMap<String, crate::models::PgConfigParam>,
    _stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    analyze_log_min_duration_statement(params, results)?;
    analyze_log_lock_waits(params, results)?;
    analyze_deadlock_timeout(params, results)?;

    Ok(())
}

fn analyze_log_min_duration_statement(
    params: &HashMap<String, crate::models::PgConfigParam>,
    results: &mut AnalysisResults,
) -> Result<()> {
    let current_value = get_param_value(params, "log_min_duration_statement");

    if current_value == "-1" {
        // Disabled
        add_suggestion(
            results,
            ConfigCategory::Logging,
            "log_min_duration_statement",
            &current_value,
            "1000",
            SuggestionLevel::Important,
            "log_min_duration_statement is disabled. This is the primary tool for finding \
             slow queries. Set to 1000 (1 second) to log all queries taking 1 second or longer.",
        );
    } else if let Ok(current_ms) = current_value.parse::<i64>() {
        if current_ms > 5000 {
            add_suggestion(
                results,
                ConfigCategory::Logging,
                "log_min_duration_statement",
                &current_value,
                "1000",
                SuggestionLevel::Recommended,
                &format!(
                    "log_min_duration_statement is set quite high ({}ms). For most workloads, \
                     1000ms (1 second) is a good starting point to identify slow queries without \
                     excessive log noise.",
                    current_ms
                ),
            );
        } else if current_ms == 0 {
            add_suggestion(
                results,
                ConfigCategory::Logging,
                "log_min_duration_statement",
                &current_value,
                "1000",
                SuggestionLevel::Info,
                "log_min_duration_statement is logging ALL queries. This may generate \
                 excessive logs. For most workloads, 1000ms (1 second) is sufficient to \
                 identify slow queries.",
            );
        }
    }

    Ok(())
}

fn analyze_log_lock_waits(
    params: &HashMap<String, crate::models::PgConfigParam>,
    results: &mut AnalysisResults,
) -> Result<()> {
    let current_value = get_param_value(params, "log_lock_waits");

    if current_value == "off" || current_value == "false" {
        add_suggestion(
            results,
            ConfigCategory::Logging,
            "log_lock_waits",
            &current_value,
            "on",
            SuggestionLevel::Important,
            "log_lock_waits is disabled. This is invaluable for diagnosing \
             application-level concurrency and contention issues. Enable it to log \
             any session that waits for a lock longer than deadlock_timeout.",
        );
    }

    Ok(())
}

fn analyze_deadlock_timeout(
    params: &HashMap<String, crate::models::PgConfigParam>,
    results: &mut AnalysisResults,
) -> Result<()> {
    let current_value = get_param_value(params, "deadlock_timeout");
    let current_ms = parse_time_to_ms(&current_value).unwrap_or(1000);

    if current_ms > 1000 {
        // Default is 1 second (1000ms)
        add_suggestion(
            results,
            ConfigCategory::Logging,
            "deadlock_timeout",
            &current_value,
            "1s",
            SuggestionLevel::Info,
            "deadlock_timeout is set higher than the default 1s. While this may reduce \
             false positives in lock wait logging, it also means deadlock detection \
             takes longer. The default 1s is typically sufficient for most workloads.",
        );
    }

    Ok(())
}

// Helper functions

fn get_param_value(params: &HashMap<String, crate::models::PgConfigParam>, name: &str) -> String {
    params
        .get(name)
        .map(|p| p.current_value.clone())
        .unwrap_or_else(|| "unknown".to_string())
}

fn parse_time_to_ms(value: &str) -> Option<u64> {
    let lower = value.to_lowercase();

    if lower.ends_with("s") {
        if let Ok(sec) = lower.trim_end_matches("s").trim().parse::<u64>() {
            return Some(sec * 1000);
        }
    } else if lower.ends_with("min") {
        if let Ok(min) = lower.trim_end_matches("min").trim().parse::<u64>() {
            return Some(min * 60 * 1000);
        }
    } else if lower.ends_with("ms") {
        if let Ok(ms) = lower.trim_end_matches("ms").trim().parse::<u64>() {
            return Some(ms);
        }
    } else if let Ok(ms) = lower.parse::<u64>() {
        // Assume ms if no unit
        return Some(ms);
    }

    None
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
