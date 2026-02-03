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
    analyze_min_wal_size(params, stats, results)?;
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

        // Bursty I/O detection: if requested checkpoints (size-based) > timed checkpoints,
        // we are running out of WAL space before the timeout hits.
        if let (Some(req), Some(timed)) = (stats.checkpoints_req, stats.checkpoints_timed) {
            if req > timed && req > 10 {
                 add_suggestion(
                    results,
                    ConfigCategory::Wal,
                    "max_wal_size",
                    &current_value,
                    "Increase value",
                    SuggestionLevel::Critical,
                    &format!(
                        "Your system is triggering more size-based checkpoints ({}) than time-based ones ({}). \
                         This causes 'bursty I/O' performance degradation. Increase max_wal_size significantly \
                         until checkpoints_timed is the dominant reason for checkpoints.",
                        req, timed
                    ),
                );
                return Ok(()); // Return early to avoid double suggestion
            }
        }

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

fn analyze_min_wal_size(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    if let Some(param) = get_param(params, "min_wal_size") {
        let current_value = param.current_value.clone();
        let current_gb = param_value_as_gigabytes(param).unwrap_or(0);

        let recommended_gb = match stats.total_memory_gb {
            Some(mem) if mem <= 16.0 => 1,
            _ => 2,
        };

        if current_gb < recommended_gb {
            add_suggestion(
                results,
                ConfigCategory::Wal,
                "min_wal_size",
                &current_value,
                &format!("{}GB", recommended_gb),
                SuggestionLevel::Recommended,
                &format!(
                    "min_wal_size manages WAL file recycling. Setting this to {}GB ensures \
                     enough WAL segments are kept around to handle spikes in write traffic \
                     without needing to create new files from scratch.",
                    recommended_gb
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

        // Use explicit workload type or fallback to heuristic
        let is_oltp = match stats.workload_type {
            crate::config::WorkloadType::Oltp => true,
            crate::config::WorkloadType::Olap => false,
        };

        let recommendation = if is_oltp {
            "5min - 10min"
        } else {
            "15min - 30min"
        };

        if is_oltp {
             if current_seconds < 300 {
                add_suggestion(
                    results,
                    ConfigCategory::Wal,
                    "checkpoint_timeout",
                    &current_value,
                    recommendation,
                    SuggestionLevel::Important,
                    "checkpoint_timeout is too short. Shorter timeouts cause more frequent \
                     checkpoints, increasing I/O load. For OLTP, 5-10min is a good balance.",
                );
            }
        } else {
            // OLAP
            if current_seconds < 900 {
                add_suggestion(
                    results,
                    ConfigCategory::Wal,
                    "checkpoint_timeout",
                    &current_value,
                    recommendation,
                    SuggestionLevel::Recommended,
                    "For OLAP/batch workloads, consider increasing checkpoint_timeout to 15-30min \
                     to reduce the number of checkpoints during long-running operations.",
                );
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{PgConfigParam, SystemStats};
    use crate::config::WorkloadType;
    use std::collections::HashMap;

    fn create_param(value: &str, unit: Option<&str>) -> PgConfigParam {
        PgConfigParam {
            name: "test".to_string(),
            current_value: value.to_string(),
            default_value: None,
            unit: unit.map(|u| u.to_string()),
            context: "user".to_string(),
        }
    }

    #[test]
    fn test_checkpoint_timeout_oltp() {
        let mut params = HashMap::new();
        // 4 min, should trigger warning
        params.insert("checkpoint_timeout".to_string(), create_param("4", Some("min")));

        let stats = SystemStats {
            workload_type: WorkloadType::Oltp,
            ..Default::default()
        };

        let mut results = AnalysisResults::default();
        analyze_checkpoint_timeout(&params, &stats, &mut results).unwrap();

        let suggestion = results.suggestions_by_category[&ConfigCategory::Wal]
            .iter()
            .find(|s| s.parameter == "checkpoint_timeout")
            .expect("Should warn for short timeout on OLTP");

        assert!(suggestion.suggested_value.contains("5min"));
    }

    #[test]
    fn test_checkpoint_timeout_olap() {
        let mut params = HashMap::new();
        // 10 min, fine for OLTP but short for OLAP
        params.insert("checkpoint_timeout".to_string(), create_param("10", Some("min")));

        let stats = SystemStats {
            workload_type: WorkloadType::Olap,
            ..Default::default()
        };

        let mut results = AnalysisResults::default();
        analyze_checkpoint_timeout(&params, &stats, &mut results).unwrap();

        let suggestion = results.suggestions_by_category[&ConfigCategory::Wal]
            .iter()
            .find(|s| s.parameter == "checkpoint_timeout")
            .expect("Should recommend higher timeout for OLAP");

        assert!(suggestion.suggested_value.contains("15min"));
    }

    #[test]
    fn test_bursty_io_detection() {
        let mut params = HashMap::new();
        params.insert("max_wal_size".to_string(), create_param("1024", Some("MB")));

        let stats = SystemStats {
            checkpoints_timed: Some(100),
            checkpoints_req: Some(500), // 5x requested vs timed
            ..Default::default()
        };

        let mut results = AnalysisResults::default();
        analyze_max_wal_size(&params, &stats, &mut results).unwrap();

        let suggestion = results.suggestions_by_category[&ConfigCategory::Wal]
            .iter()
            .find(|s| s.parameter == "max_wal_size")
            .expect("Should warn about bursty I/O");

        assert_eq!(suggestion.level, SuggestionLevel::Critical);
        assert!(suggestion.rationale.contains("size-based checkpoints"));
    }
}
