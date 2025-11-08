use crate::checker::CheckerError;
use crate::models::{AnalysisResults, ConfigCategory, ConfigSuggestion, SuggestionLevel};
use std::collections::HashMap;

type Result<T> = std::result::Result<T, CheckerError>;

/// Analyzes concurrency and parallelism configuration
pub fn analyze_concurrency(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    analyze_max_connections(params, stats, results)?;
    analyze_max_worker_processes(params, stats, results)?;
    analyze_max_parallel_workers(params, stats, results)?;
    analyze_max_parallel_workers_per_gather(params, stats, results)?;
    analyze_max_parallel_maintenance_workers(params, stats, results)?;

    Ok(())
}

fn analyze_max_connections(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    let current_value = get_param_value(params, "max_connections");

    if let Some(cpu) = stats.cpu_count {
        let recommended = (4 * cpu).max(100); // GREATEST(4 * vCPU, 100)
        let current_conns = current_value.parse::<usize>().unwrap_or(0);

        if current_conns > recommended * 2 {
            add_suggestion(
                results,
                ConfigCategory::Concurrency,
                "max_connections",
                &current_value,
                &recommended.to_string(),
                SuggestionLevel::Critical,
                &format!(
                    "max_connections is dangerously high at {}. Each connection consumes \
                     memory and CPU. Best practice is to use a connection pooler (PgBouncer) \
                     and set max_connections to ~4 * vCPU ({} for your system). \
                     This is a common trap parameter.",
                    current_conns, recommended
                ),
            );
        } else if current_conns > recommended {
            add_suggestion(
                results,
                ConfigCategory::Concurrency,
                "max_connections",
                &current_value,
                &recommended.to_string(),
                SuggestionLevel::Important,
                "max_connections should be kept relatively low. Consider using a connection pooler \
                 for better connection management.",
            );
        }
    }

    Ok(())
}

fn analyze_max_worker_processes(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    if let Some(cpu) = stats.cpu_count {
        let current_value = get_param_value(params, "max_worker_processes");
        let recommended = cpu;

        if let Some(current_workers) = current_value.parse::<usize>().ok() {
            if current_workers != recommended {
                add_suggestion(
                    results,
                    ConfigCategory::Concurrency,
                    "max_worker_processes",
                    &current_value,
                    &recommended.to_string(),
                    SuggestionLevel::Recommended,
                    &format!(
                        "max_worker_processes should match your vCPU count ({}). \
                         This is the master limit for all background worker processes.",
                        recommended
                    ),
                );
            }
        }
    }

    Ok(())
}

fn analyze_max_parallel_workers(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    if let Some(cpu) = stats.cpu_count {
        let current_value = get_param_value(params, "max_parallel_workers");
        let recommended = cpu;

        if let Some(current_workers) = current_value.parse::<usize>().ok() {
            if current_workers > recommended {
                add_suggestion(
                    results,
                    ConfigCategory::Concurrency,
                    "max_parallel_workers",
                    &current_value,
                    &recommended.to_string(),
                    SuggestionLevel::Important,
                    &format!(
                        "max_parallel_workers (total across all queries) should not exceed vCPU count ({}). \
                         Setting it higher can cause CPU contention.",
                        recommended
                    ),
                );
            } else if current_workers < (recommended as f64 * 0.5) as usize {
                add_suggestion(
                    results,
                    ConfigCategory::Concurrency,
                    "max_parallel_workers",
                    &current_value,
                    &recommended.to_string(),
                    SuggestionLevel::Recommended,
                    "max_parallel_workers is underutilized. Consider increasing it to match \
                     your vCPU count for better parallel query performance.",
                );
            }
        }
    }

    Ok(())
}

fn analyze_max_parallel_workers_per_gather(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    if let Some(cpu) = stats.cpu_count {
        let current_value = get_param_value(params, "max_parallel_workers_per_gather");
        let recommended = (cpu / 2).max(1); // Half vCPU, but at least 1

        if let Some(current_workers) = current_value.parse::<usize>().ok() {
            if current_workers > cpu {
                add_suggestion(
                    results,
                    ConfigCategory::Concurrency,
                    "max_parallel_workers_per_gather",
                    &current_value,
                    &recommended.to_string(),
                    SuggestionLevel::Critical,
                    &format!(
                        "max_parallel_workers_per_gather (per query) should not exceed vCPU count ({}). \
                         Setting it to {} would allow a single query to consume all CPU resources, \
                         starving other concurrent queries.",
                        cpu, current_workers
                    ),
                );
            } else if current_workers == cpu {
                add_suggestion(
                    results,
                    ConfigCategory::Concurrency,
                    "max_parallel_workers_per_gather",
                    &current_value,
                    &recommended.to_string(),
                    SuggestionLevel::Important,
                    "Setting max_parallel_workers_per_gather equal to vCPU count is dangerous. \
                     It allows a single complex query to consume all parallel workers, starving \
                     other queries. Set it to half of vCPUs to limit the blast radius of a runaway query.",
                );
            } else if current_workers < (recommended as f64 * 0.5) as usize {
                add_suggestion(
                    results,
                    ConfigCategory::Concurrency,
                    "max_parallel_workers_per_gather",
                    &current_value,
                    &recommended.to_string(),
                    SuggestionLevel::Recommended,
                    &format!(
                        "max_parallel_workers_per_gather is underutilized. For mixed workloads, \
                         setting it to half of vCPUs (e.g., {}) allows at least two complex queries \
                         to run in parallel fully.",
                        recommended
                    ),
                );
            }
        }
    }

    Ok(())
}

fn analyze_max_parallel_maintenance_workers(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    if let Some(cpu) = stats.cpu_count {
        let current_value = get_param_value(params, "max_parallel_maintenance_workers");
        let recommended = (cpu / 2).max(1); // Half vCPU, but at least 1

        if let Some(current_workers) = current_value.parse::<usize>().ok() {
            if current_workers < recommended {
                add_suggestion(
                    results,
                    ConfigCategory::Concurrency,
                    "max_parallel_maintenance_workers",
                    &current_value,
                    &recommended.to_string(),
                    SuggestionLevel::Recommended,
                    &format!(
                        "max_parallel_maintenance_workers controls parallelism for manual \
                         VACUUM and CREATE INDEX commands. Setting it to {} (half of vCPUs) \
                         can significantly speed up maintenance operations.",
                        recommended
                    ),
                );
            }
        }
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
