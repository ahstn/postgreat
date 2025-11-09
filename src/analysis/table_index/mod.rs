use crate::checker::CheckerError;
use crate::models::{AnalysisResults, ConfigCategory, ConfigSuggestion, SuggestionLevel};
use sqlx::{Pool, Postgres};

mod bloat;
mod indexes;

/// Entry point that coordinates table bloat and index health analysis.
pub async fn analyze_table_index_health(
    pool: &Pool<Postgres>,
    results: &mut AnalysisResults,
) -> Result<(), CheckerError> {
    bloat::analyze(pool, results).await?;
    indexes::analyze(pool, results).await?;
    Ok(())
}

fn push_table_index_suggestion(
    results: &mut AnalysisResults,
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
        .entry(ConfigCategory::TableIndex)
        .or_default()
        .push(suggestion);
}
