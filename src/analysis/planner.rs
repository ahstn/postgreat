use crate::checker::CheckerError;
use crate::models::{AnalysisResults, ConfigCategory, ConfigSuggestion, SuggestionLevel};
use std::collections::HashMap;

type Result<T> = std::result::Result<T, CheckerError>;

/// Analyzes query planner cost model configuration
pub fn analyze_planner(
    params: &HashMap<String, crate::models::PgConfigParam>,
    _stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    analyze_random_page_cost(params, results)?;
    analyze_effective_io_concurrency(params, results)?;
    analyze_seq_page_cost(params, results)?;

    Ok(())
}

fn analyze_random_page_cost(
    params: &HashMap<String, crate::models::PgConfigParam>,
    results: &mut AnalysisResults,
) -> Result<()> {
    let current_value = get_param_value(params, "random_page_cost");
    let current = current_value.parse::<f64>().unwrap_or(4.0);

    // On SSD/NVMe, this should be 1.0 or 1.1
    // Default of 4.0 is for HDDs and is dangerously suboptimal on modern storage
    if current > 2.0 {
        add_suggestion(
            results,
            ConfigCategory::Planner,
            "random_page_cost",
            &current_value,
            "1.1",
            if current == 4.0 {
                SuggestionLevel::Critical
            } else {
                SuggestionLevel::Important
            },
            "random_page_cost is set for HDDs (default 4.0), but modern cloud VMs use SSD/NVMe. \
             On SSDs, random reads are nearly as fast as sequential reads. Setting this to 1.1 \
             (combined with high effective_cache_size) tells the planner to trust and use indexes \
             instead of always choosing sequential scans. This is MANDATORY for modern storage.",
        );
    } else if current > 1.5 {
        add_suggestion(
            results,
            ConfigCategory::Planner,
            "random_page_cost",
            &current_value,
            "1.1",
            SuggestionLevel::Recommended,
            "random_page_cost could be lowered to 1.1 for better index utilization on SSD storage.",
        );
    }

    Ok(())
}

fn analyze_effective_io_concurrency(
    params: &HashMap<String, crate::models::PgConfigParam>,
    results: &mut AnalysisResults,
) -> Result<()> {
    let current_value = get_param_value(params, "effective_io_concurrency");
    let current = current_value.parse::<u64>().unwrap_or(1);

    // Should be 200 for SSD/NVMe, default is 1 for HDDs
    if current < 100 {
        add_suggestion(
            results,
            ConfigCategory::Planner,
            "effective_io_concurrency",
            &current_value,
            "200",
            if current == 1 {
                SuggestionLevel::Important
            } else {
                SuggestionLevel::Recommended
            },
            "effective_io_concurrency should be set to 200 for modern SSD/NVMe storage. \
             Default of 1 is for single disk HDDs. Modern storage can handle massive concurrency \
             and benefits from higher values for bitmap heap scans.",
        );
    }

    Ok(())
}

fn analyze_seq_page_cost(
    params: &HashMap<String, crate::models::PgConfigParam>,
    results: &mut AnalysisResults,
) -> Result<()> {
    let current_value = get_param_value(params, "seq_page_cost");
    let current = current_value.parse::<f64>().unwrap_or(1.0);

    // Should be 1.0, but check if it's been modified unusually
    if current != 1.0 {
        add_suggestion(
            results,
            ConfigCategory::Planner,
            "seq_page_cost",
            &current_value,
            "1.0",
            SuggestionLevel::Info,
            "seq_page_cost has been modified from the default of 1.0. Unless you have a specific reason, \
             it's recommended to keep it at 1.0 and adjust random_page_cost instead for SSD/HDD tuning.",
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
