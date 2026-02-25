use crate::checker::CheckerError;
use crate::models::{AnalysisResults, ConfigCategory, ConfigSuggestion, SuggestionLevel};
use std::collections::HashMap;

type Result<T> = std::result::Result<T, CheckerError>;

/// Analyzes query planner cost model configuration
pub fn analyze_planner(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    analyze_random_page_cost(params, stats, results)?;
    analyze_effective_io_concurrency(params, stats, results)?;
    analyze_seq_page_cost(params, results)?;

    Ok(())
}

fn analyze_random_page_cost(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    let current_value = get_param_value(params, "random_page_cost");
    let current = current_value.parse::<f64>().unwrap_or(4.0);

    // Recommendation depends on storage type
    let target_str = match stats.storage_type {
        crate::config::StorageType::Ssd => "1.1",
        crate::config::StorageType::Hdd => "4.0",
    };

    if stats.storage_type == crate::config::StorageType::Ssd {
        // For SSD, we want it low (1.1)
        if current > 2.0 {
            add_suggestion(
                results,
                ConfigCategory::Planner,
                "random_page_cost",
                &current_value,
                target_str,
                if current == 4.0 {
                    SuggestionLevel::Critical
                } else {
                    SuggestionLevel::Important
                },
                "random_page_cost is set for HDDs (default 4.0), but you are using SSD storage. \
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
                target_str,
                SuggestionLevel::Recommended,
                "random_page_cost could be lowered to 1.1 for better index utilization on SSD storage.",
            );
        }
    } else {
        // For HDD, we want it high (4.0)
        if current < 3.0 {
            add_suggestion(
                results,
                ConfigCategory::Planner,
                "random_page_cost",
                &current_value,
                target_str,
                SuggestionLevel::Recommended,
                "random_page_cost is too low for HDD storage. Random I/O is much more expensive \
                 than sequential I/O on spinning disks. Increasing this to 4.0 (the default) \
                 prevents the planner from optimistically choosing index scans that will cause \
                 disk thrashing.",
            );
        }
    }

    Ok(())
}

fn analyze_effective_io_concurrency(
    params: &HashMap<String, crate::models::PgConfigParam>,
    stats: &crate::models::SystemStats,
    results: &mut AnalysisResults,
) -> Result<()> {
    let current_value = get_param_value(params, "effective_io_concurrency");
    let current = current_value.parse::<u64>().unwrap_or(1);

    // Recommendation depends on storage type
    let target_str = match stats.storage_type {
        crate::config::StorageType::Ssd => "200",
        crate::config::StorageType::Hdd => "2",
    };

    if stats.storage_type == crate::config::StorageType::Ssd {
        if current < 100 {
            add_suggestion(
                results,
                ConfigCategory::Planner,
                "effective_io_concurrency",
                &current_value,
                target_str,
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
    } else {
        // HDD
        if current > 10 {
            add_suggestion(
                results,
                ConfigCategory::Planner,
                "effective_io_concurrency",
                &current_value,
                target_str,
                SuggestionLevel::Recommended,
                "effective_io_concurrency is too high for HDD storage. Spinning disks have limited \
                 IOPS and queue depth. Setting this too high (default for HDD is 1-2) can cause \
                 excessive seek activity.",
            );
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageType;
    use crate::models::{PgConfigParam, SystemStats};
    use std::collections::HashMap;

    fn create_param(value: &str) -> PgConfigParam {
        PgConfigParam {
            name: "test".to_string(),
            current_value: value.to_string(),
            default_value: None,
            unit: None,
            context: "user".to_string(),
        }
    }

    #[test]
    fn test_random_page_cost_ssd() {
        let mut params = HashMap::new();
        params.insert("random_page_cost".to_string(), create_param("4.0"));

        let stats = SystemStats {
            storage_type: StorageType::Ssd,
            ..Default::default()
        };

        let mut results = AnalysisResults::default();
        analyze_planner(&params, &stats, &mut results).unwrap();

        let suggestion = results.suggestions_by_category[&ConfigCategory::Planner]
            .iter()
            .find(|s| s.parameter == "random_page_cost")
            .expect("Should recommend 1.1 for SSD");

        assert_eq!(suggestion.suggested_value, "1.1");
        assert_eq!(suggestion.level, SuggestionLevel::Critical);
    }

    #[test]
    fn test_random_page_cost_hdd() {
        let mut params = HashMap::new();
        params.insert("random_page_cost".to_string(), create_param("1.1"));

        let stats = SystemStats {
            storage_type: StorageType::Hdd,
            ..Default::default()
        };

        let mut results = AnalysisResults::default();
        analyze_planner(&params, &stats, &mut results).unwrap();

        let suggestion = results.suggestions_by_category[&ConfigCategory::Planner]
            .iter()
            .find(|s| s.parameter == "random_page_cost")
            .expect("Should recommend 4.0 for HDD");

        assert_eq!(suggestion.suggested_value, "4.0");
    }

    #[test]
    fn test_effective_io_concurrency_ssd() {
        let mut params = HashMap::new();
        params.insert("effective_io_concurrency".to_string(), create_param("1"));

        let stats = SystemStats {
            storage_type: StorageType::Ssd,
            ..Default::default()
        };

        let mut results = AnalysisResults::default();
        analyze_planner(&params, &stats, &mut results).unwrap();

        let suggestion = results.suggestions_by_category[&ConfigCategory::Planner]
            .iter()
            .find(|s| s.parameter == "effective_io_concurrency")
            .expect("Should recommend 200 for SSD");

        assert_eq!(suggestion.suggested_value, "200");
    }
}
