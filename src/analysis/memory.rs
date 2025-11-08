use crate::analysis::{get_param, param_value_as_gigabytes, param_value_as_megabytes};
use crate::checker::CheckerError;
use crate::models::{AnalysisResults, ConfigCategory, ConfigSuggestion, SuggestionLevel};
use std::collections::HashMap;

type Result<T> = std::result::Result<T, CheckerError>;

/// Analyzes memory configuration parameters based on documentation
pub fn analyze_memory(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    analyze_shared_buffers(params, stats, results)?;
    analyze_effective_cache_size(params, stats, results)?;
    analyze_work_mem(params, stats, results)?;
    analyze_maintenance_work_mem(params, stats, results)?;
    analyze_wal_buffers(params, results)?;

    Ok(())
}

fn analyze_shared_buffers(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    if let Some(spec) = get_compute_spec(stats) {
        if let Some(param) = get_param(params, "shared_buffers") {
            let current_value = param.current_value.clone();
            let recommended_mb = (spec.memory_mb() as f64 * 0.25 / (1024.0 * 1024.0)) as u64;
            let recommended_mb = recommended_mb.min(8192);

            if let Some(current_mb) = param_value_as_megabytes(param) {
                let variance =
                    (current_mb as f64 - recommended_mb as f64).abs() / recommended_mb as f64;

                if variance > 0.2 {
                    let level = if current_mb > recommended_mb {
                        SuggestionLevel::Important
                    } else {
                        SuggestionLevel::Critical
                    };

                    add_suggestion(
                        results,
                        ConfigCategory::Memory,
                        "shared_buffers",
                        &current_value,
                        &format!("{}MB", recommended_mb),
                        level,
                        &format!(
                            "shared_buffers should be ~25% of total RAM ({}GB). \
                             This is the single most effective performance parameter.",
                            spec.memory_gb
                        ),
                    );
                }
            }
        }
    }

    Ok(())
}

fn analyze_effective_cache_size(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    if let Some(spec) = get_compute_spec(stats) {
        if let Some(param) = get_param(params, "effective_cache_size") {
            let current_value = param.current_value.clone();
            let recommended_gb = (spec.memory_gb as f64 * 0.75) as u64;

            if let Some(current_gb) = param_value_as_gigabytes(param) {
                let variance =
                    (current_gb as f64 - recommended_gb as f64).abs() / recommended_gb as f64;

                if variance > 0.2 {
                    add_suggestion(
                        results,
                        ConfigCategory::Memory,
                        "effective_cache_size",
                        &current_value,
                        &format!("{}GB", recommended_gb),
                        SuggestionLevel::Critical,
                        "effective_cache_size is a cost model hint for the query planner. \
                         Setting it to ~75% of RAM helps the planner choose index scans over \
                         sequential scans. The default is dangerously low.",
                    );
                }
            }
        }
    }

    Ok(())
}

fn analyze_work_mem(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    let current_value = param_value_string(params, "work_mem");

    let recommended_mb = match stats.total_memory_gb {
        Some(mem) if mem <= 16.0 => 32,
        Some(mem) if mem <= 64.0 => 64,
        Some(mem) if mem > 64.0 => 64,
        _ => 64,
    };

    if let Some(param) = get_param(params, "work_mem") {
        if let Some(current_mb) = param_value_as_megabytes(param) {
            if current_mb > 512 {
                add_suggestion(
                    results,
                    ConfigCategory::Memory,
                    "work_mem",
                    &current_value,
                    &format!("{}MB", recommended_mb),
                    SuggestionLevel::Critical,
                    &format!(
                        "work_mem is dangerously high at {}MB. This setting is multiplied by \
                         the number of concurrent operations and connections. A high work_mem \
                         with many connections is the most common cause of OOM errors. \
                         For OLTP workloads, 16-64MB is recommended.",
                        current_mb
                    ),
                );
            } else if current_mb < (recommended_mb as f64 * 0.5) as u64 {
                add_suggestion(
                    results,
                    ConfigCategory::Memory,
                    "work_mem",
                    &current_value,
                    &format!("{}MB", recommended_mb),
                    SuggestionLevel::Important,
                    "work_mem is too low, which may cause sorts and hash joins to spill to disk. \
                     Use EXPLAIN (ANALYZE) to check for 'external merge Disk' or 'spill' messages.",
                );
            }
        }
    }

    Ok(())
}

fn analyze_maintenance_work_mem(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    if let Some(spec) = get_compute_spec(stats) {
        if let Some(param) = get_param(params, "maintenance_work_mem") {
            let current_value = param.current_value.clone();
            let recommended_mb = match spec.memory_gb {
                mem if mem <= 16 => 512,
                mem if mem <= 64 => 1024,
                _ => 2048,
            };

            if let Some(current_mb) = param_value_as_megabytes(param) {
                if current_mb < (recommended_mb as f64 * 0.8) as u64 {
                    add_suggestion(
                        results,
                        ConfigCategory::Memory,
                        "maintenance_work_mem",
                        &current_value,
                        &format!("{}MB", recommended_mb),
                        SuggestionLevel::Recommended,
                        &format!(
                            "maintenance_work_mem should be set to ~{}MB for your system ({}, {}GB RAM). \
                             A larger value can dramatically speed up index creation and vacuuming.",
                            recommended_mb, spec.vcpu, spec.memory_gb
                        ),
                    );
                }
            }
        }
    }

    Ok(())
}

fn analyze_wal_buffers(
    params: &HashMap<String, crate::models::PgConfigParam>,
    results: &mut AnalysisResults,
) -> Result<()> {
    let current_value = param_value_string(params, "wal_buffers");

    if current_value != "-1" {
        if let Some(param) = get_param(params, "wal_buffers") {
            if let Some(current_mb) = param_value_as_megabytes(param) {
                if current_mb < 16 {
                    add_suggestion(
                        results,
                        ConfigCategory::Memory,
                        "wal_buffers",
                        &current_value,
                        "16MB",
                        SuggestionLevel::Recommended,
                        "wal_buffers should be set to 16MB for high-write workloads. \
                         The default -1 (auto-sized) is usually adequate, but a fixed 16MB \
                         value can improve write performance.",
                    );
                }
            }
        }
    }

    Ok(())
}

fn get_compute_spec(stats: &crate::models::SystemStats) -> Option<crate::config::ComputeSpec> {
    match (stats.cpu_count, stats.total_memory_gb) {
        (Some(cpu), Some(mem)) => Some(crate::config::ComputeSpec {
            vcpu: cpu,
            memory_gb: mem as usize,
        }),
        _ => None,
    }
}

fn param_value_string(
    params: &HashMap<String, crate::models::PgConfigParam>,
    name: &str,
) -> String {
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
