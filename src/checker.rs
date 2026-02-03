use crate::analysis::workload::WorkloadOptions;
use crate::analysis::{
    autovacuum, concurrency, logging, memory, planner, table_index, wal, workload,
};
use crate::config::DbConfig;
use crate::models::{AnalysisResults, PgConfigParam, SystemStats, WorkloadResults};
use snafu::{ResultExt, Snafu};
use sqlx::{postgres::PgPoolOptions, query_scalar, Pool, Postgres, Row};
use std::collections::HashMap;
use tracing::{debug, info, warn};

#[derive(Debug, Snafu)]
pub enum CheckerError {
    #[snafu(display("Failed to connect to database: {}", source))]
    ConnectionError { source: sqlx::Error },

    #[snafu(display("Failed to execute query: {}", query))]
    QueryError { query: String, source: sqlx::Error },
}

type Result<T, E = CheckerError> = std::result::Result<T, E>;

pub struct ConfigChecker {
    config: DbConfig,
    pool: Pool<Postgres>,
}

impl ConfigChecker {
    pub async fn new(config: DbConfig) -> Result<Self> {
        info!(
            "Connecting to PostgreSQL at {}:{}",
            config.host, config.port
        );

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&config.connection_string())
            .await
            .context(ConnectionSnafu)?;

        info!("Successfully connected to database: {}", config.database);

        Ok(Self { config, pool })
    }

    pub async fn analyze(&mut self) -> Result<AnalysisResults> {
        let mut results = AnalysisResults::default();

        // Fetch all configuration parameters
        info!("Fetching configuration parameters...");
        let params = self.fetch_config_params().await?;
        results.params = params;

        let stats = self.fetch_system_stats().await?;
        results.system_stats = stats;

        // Run analysis checks
        if self.config.compute.is_none() {
            warn!("No compute specification provided; CPU and memory-based recommendations will be limited. Use --compute <tier|<vCPU>vCPU-<GB>GB> to enable full guidance.");
        }

        let params_snapshot = results.params.clone();
        let stats_snapshot = results.system_stats.clone();

        info!("Running memory configuration analysis...");
        memory::analyze_memory(&params_snapshot, &stats_snapshot, &mut results)?;

        info!("Running concurrency analysis...");
        concurrency::analyze_concurrency(&params_snapshot, &stats_snapshot, &mut results)?;

        info!("Running WAL configuration analysis...");
        wal::analyze_wal(&params_snapshot, &stats_snapshot, &mut results)?;

        info!("Running planner analysis...");
        planner::analyze_planner(&params_snapshot, &stats_snapshot, &mut results)?;

        info!("Running autovacuum analysis...");
        autovacuum::analyze_autovacuum(&params_snapshot, &stats_snapshot, &mut results)?;

        info!("Running logging analysis...");
        logging::analyze_logging(&params_snapshot, &stats_snapshot, &mut results)?;

        info!("Running table and index health analysis...");
        if let Err(err) = table_index::analyze_table_index_health(&self.pool, &mut results).await {
            warn!("Table/index health analysis skipped: {err}");
        }

        Ok(results)
    }

    pub async fn analyze_workload(&mut self, opts: WorkloadOptions) -> Result<WorkloadResults> {
        let mut results = workload::analyze(&self.pool, &opts).await?;

        info!("Running table and index health analysis...");
        let mut table_results = AnalysisResults::default();
        if let Err(err) =
            table_index::analyze_table_index_health(&self.pool, &mut table_results).await
        {
            warn!("Table/index health analysis skipped: {err}");
        } else {
            results.bloat_info = table_results.bloat_info;
            results.seq_scan_info = table_results.seq_scan_info;
            results.index_usage_info = table_results.index_usage_info;
        }

        Ok(results)
    }

    async fn fetch_config_params(&self) -> Result<HashMap<String, PgConfigParam>> {
        let query = r#"
            SELECT
                name,
                setting,
                unit,
                context,
                boot_val
            FROM pg_settings
            ORDER BY name
        "#;

        let rows = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .context(QuerySnafu { query })?;

        let mut params = HashMap::new();
        for row in rows {
            let name: String = row.get("name");
            let current_value: String = row.get("setting");
            let unit: Option<String> = row.get("unit");
            let context: String = row.get("context");
            let default_value: Option<String> = row.get("boot_val");

            // Skip parameters with empty values
            if current_value.is_empty() {
                continue;
            }

            let param = PgConfigParam {
                name: name.clone(),
                current_value,
                default_value,
                unit,
                context,
            };

            params.insert(name, param);
        }

        debug!("Fetched {} configuration parameters", params.len());
        Ok(params)
    }

    async fn fetch_system_stats(&self) -> Result<SystemStats> {
        let mut stats = SystemStats::default();

        // Record active connections for workload heuristics
        match query_scalar::<_, i64>("SELECT count(*) FROM pg_stat_activity")
            .fetch_one(&self.pool)
            .await
        {
            Ok(active) => stats.connection_count = Some(active as usize),
            Err(err) => warn!("Failed to read pg_stat_activity for connection count: {err}"),
        }

        // Fetch checkpoint stats for WAL analysis
        match sqlx::query("SELECT checkpoints_timed, checkpoints_req FROM pg_stat_bgwriter")
            .fetch_one(&self.pool)
            .await
        {
            Ok(row) => {
                stats.checkpoints_timed = row.try_get("checkpoints_timed").ok();
                stats.checkpoints_req = row.try_get("checkpoints_req").ok();
            }
            Err(err) => warn!("Failed to read pg_stat_bgwriter: {err}"),
        }

        // Use provided compute spec if available
        if let Some(compute) = &self.config.compute {
            stats.total_memory_gb = Some(compute.memory_gb as f64);
            stats.cpu_count = Some(compute.vcpu);
        }

        stats.storage_type = self.config.storage_type;
        stats.workload_type = self.config.workload_type;

        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::ComputeSpec;
    use rstest::rstest;

    #[rstest]
    #[case("small", 2, 16)]
    #[case("medium", 8, 64)]
    #[case("large", 32, 256)]
    #[case("8vCPU-64GB", 8, 64)]
    #[case("4vcpu-16gb", 4, 16)]
    fn test_compute_spec_parsing(
        #[case] input: &str,
        #[case] expected_vcpu: usize,
        #[case] expected_memory: usize,
    ) {
        let spec = ComputeSpec::from_string(input).unwrap();
        assert_eq!(spec.vcpu, expected_vcpu);
        assert_eq!(spec.memory_gb, expected_memory);
    }
}
