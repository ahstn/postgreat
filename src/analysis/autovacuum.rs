use crate::analysis::{get_param, param_value_as_megabytes, param_value_as_seconds};
use crate::checker::CheckerError;
use crate::models::{AnalysisResults, ConfigCategory, ConfigSuggestion, SuggestionLevel};
use std::collections::HashMap;

type Result<T> = std::result::Result<T, CheckerError>;

/// Analyzes autovacuum configuration
pub fn analyze_autovacuum(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    analyze_autovacuum_max_workers(params, stats, results)?;
    analyze_autovacuum_naptime(params, results)?;
    analyze_autovacuum_vacuum_cost_limit(params, results)?;
    analyze_autovacuum_work_mem(params, results)?;
    analyze_autovacuum_scale_factor(params, results)?;

    Ok(())
}

fn analyze_autovacuum_max_workers(
    params: &HashMap<String, crate::models::PgConfigParam>,
    _stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    let current_value = get_param_value(params, "autovacuum_max_workers");
    let current = current_value.parse::<usize>().unwrap_or(3);

    // Default is 3, recommended to increase to 5 for better responsiveness
    if current < 5 {
        add_suggestion(
            results,
            ConfigCategory::Autovacuum,
            "autovacuum_max_workers",
            &current_value,
            "5",
            SuggestionLevel::Important,
            "autovacuum_max_workers is too low. Default of 3 is often insufficient for \
             servers with many active databases and tables. Increasing to 5 allows more \
             parallel autovacuum processes to keep up with write-heavy workloads.",
        );
    }

    Ok(())
}

fn analyze_autovacuum_naptime(
    params: &HashMap<String, crate::models::PgConfigParam>,
    results: &mut AnalysisResults,
) -> Result<()> {
    if let Some(param) = get_param(params, "autovacuum_naptime") {
        let current_value = param.current_value.clone();
        let current_seconds = param_value_as_seconds(param).unwrap_or(60);

        // Default is 60s (1min), recommended to decrease to 30s for high-churn systems
        if current_seconds > 30 {
            add_suggestion(
                results,
                ConfigCategory::Autovacuum,
                "autovacuum_naptime",
                &current_value,
                "30s",
                SuggestionLevel::Recommended,
                "autovacuum_naptime controls how often the launcher checks for work. \
                 Lowering it to 30s makes autovacuum more responsive on high-churn systems, \
                 especially those with many databases and tables.",
            );
        }
    }

    Ok(())
}

fn analyze_autovacuum_vacuum_cost_limit(
    params: &HashMap<String, crate::models::PgConfigParam>,
    results: &mut AnalysisResults,
) -> Result<()> {
    let current_value = get_param_value(params, "autovacuum_vacuum_cost_limit");
    let current = current_value.parse::<u64>().unwrap_or(200);

    // Default is 200 (inherited from vacuum_cost_limit),
    // recommended to increase to 2000 for better throughput
    if current < 1000 {
        let level = if current == 200 {
            SuggestionLevel::Critical
        } else {
            SuggestionLevel::Important
        };

        add_suggestion(
            results,
            ConfigCategory::Autovacuum,
            "autovacuum_vacuum_cost_limit",
            &current_value,
            "2000",
            level,
            "autovacuum_vacuum_cost_limit is too low. Default 200 is so aggressive that \
             autovacuum sleeps after processing only 10 dirty pages. This prevents autovacuum \
             from keeping up with write-intensive workloads. Increase to 2000 (10x default) \
             to allow workers to do more work before sleeping.",
        );
    }

    Ok(())
}

fn analyze_autovacuum_work_mem(
    params: &HashMap<String, crate::models::PgConfigParam>,
    results: &mut AnalysisResults,
) -> Result<()> {
    let current_value = get_param_value(params, "autovacuum_work_mem");
    let recommended_mb = 512;

    if current_value == "-1" {
        if let Some(maint_param) = get_param(params, "maintenance_work_mem") {
            if let Some(maint_mb) = param_value_as_megabytes(maint_param) {
                if maint_mb > 1024 {
                    add_suggestion(
                        results,
                        ConfigCategory::Autovacuum,
                        "autovacuum_work_mem",
                        &current_value,
                        &format!("{}MB", recommended_mb),
                        SuggestionLevel::Critical,
                        "autovacuum_work_mem is -1 (inheriting maintenance_work_mem). \
                         If maintenance_work_mem is set to a large value (e.g., 2GB), \
                         and autovacuum_max_workers is 3 or more, the system could suddenly \
                         allocate several GB of RAM for routine autovacuum. Always set autovacuum_work_mem \
                         explicitly to decouple it from manual maintenance values.",
                    );
                }
            }
        }
    } else if let Some(param) = get_param(params, "autovacuum_work_mem") {
        if let Some(current_mb) = param_value_as_megabytes(param) {
            if current_mb < recommended_mb {
                add_suggestion(
                    results,
                    ConfigCategory::Autovacuum,
                    "autovacuum_work_mem",
                    &current_value,
                    &format!("{}MB", recommended_mb),
                    SuggestionLevel::Recommended,
                    "autovacuum_work_mem can be increased to 512MB per worker for better \
                     vacuum performance. This is safe because autovacuum operations are infrequent \
                     compared to normal queries.",
                );
            }
        }
    }

    Ok(())
}

fn analyze_autovacuum_scale_factor(
    params: &HashMap<String, crate::models::PgConfigParam>,
    results: &mut AnalysisResults,
) -> Result<()> {
    let current_value = get_param_value(params, "autovacuum_vacuum_scale_factor");
    let current = current_value.parse::<f64>().unwrap_or(0.2);

    // Default is 0.2 (20%), which is catastrophic for large tables
    // Should be overridden per-table for large tables
    if current > 0.1 {
        let level = if current == 0.2 {
            SuggestionLevel::Critical
        } else {
            SuggestionLevel::Important
        };

        add_suggestion(
            results,
            ConfigCategory::Autovacuum,
            "autovacuum_vacuum_scale_factor",
            &current_value,
            "0.1 or per-table override",
            level,
            &format!(
                "autovacuum_vacuum_scale_factor is too high at {} (20% default). \
                 This causes autovacuum to wait for 200 million dead tuples on a 1-billion row table \
                 before starting. For large tables, autovacuum_vacuum_scale_factor should be 0 \
                 and autovacuum_vacuum_threshold should be set to a fixed value (e.g., 10,000). \
                 This can be done per-table: \
                 ALTER TABLE my_large_table SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 10000);",
                current
            ),
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
