use clap::{Parser, Subcommand};
use postgreat::analysis::workload::WorkloadOptions;
use postgreat::checker::ConfigChecker;
use postgreat::config::{DbConfig, StorageType, WorkloadType};
use postgreat::reporter::{ReportFormat, Reporter, WorkloadReporter};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// PostgreSQL Configuration Analyzer - Analyzes and suggests improvements based on best practices
#[derive(Parser, Debug)]
#[command(name = "postgreat")]
#[command(version = "0.1.0")]
#[command(about = "PostgreSQL configuration analyzer")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Output format
    #[arg(short = 'f', long = "format", value_enum, default_value = "markdown")]
    format: ReportFormat,

    /// Enable verbose logging
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Analyze a single PostgreSQL database
    Analyze {
        /// Database host
        #[arg(
            short = 'H',
            long = "host",
            env = "POSTGRES_HOST",
            default_value = "localhost"
        )]
        host: String,

        /// Database port
        #[arg(long = "port", env = "POSTGRES_PORT", default_value = "5432")]
        port: u16,

        /// Database name
        #[arg(short = 'd', long = "database", env = "POSTGRES_DATABASE")]
        database: String,

        /// Username
        #[arg(short = 'u', long = "username", env = "POSTGRES_USER")]
        username: String,

        /// Password
        #[arg(short = 'p', long = "password", env = "POSTGRES_PASSWORD")]
        password: String,

        /// Compute spec (required for hardware-aware recommendations)
        #[arg(
            long = "compute",
            help = "Compute specification. Accepts tiers ('small'|'medium'|'large') or explicit '<vCPU>vCPU-<GB>GB' (case-insensitive)."
        )]
        compute: Option<String>,

        /// Storage type
        #[arg(long = "storage-type", value_enum, default_value = "ssd")]
        storage_type: StorageType,

        /// Workload type
        #[arg(long = "workload-type", value_enum, default_value = "oltp")]
        workload_type: WorkloadType,
    },
    /// Analyze multiple databases from a YAML config file
    Config {
        /// Path to YAML config file
        #[arg(short = 'c', long = "config")]
        config_path: String,
    },
    /// Analyze workload performance using pg_stat_statements
    Workload {
        /// Database host
        #[arg(
            short = 'H',
            long = "host",
            env = "POSTGRES_HOST",
            default_value = "localhost"
        )]
        host: String,

        /// Database port
        #[arg(long = "port", env = "POSTGRES_PORT", default_value = "5432")]
        port: u16,

        /// Database name
        #[arg(short = 'd', long = "database", env = "POSTGRES_DATABASE")]
        database: String,

        /// Username
        #[arg(short = 'u', long = "username", env = "POSTGRES_USER")]
        username: String,

        /// Password
        #[arg(short = 'p', long = "password", env = "POSTGRES_PASSWORD")]
        password: String,

        /// Top N queries per category
        #[arg(long = "limit", default_value = "20")]
        limit: usize,

        /// Minimum number of calls to consider
        #[arg(long = "min-calls", default_value = "10")]
        min_calls: i64,

        /// Maximum query length to display
        #[arg(long = "max-query-len", default_value = "200")]
        max_query_len: usize,

        /// Include full query text (no truncation)
        #[arg(long = "include-full-query", default_value_t = false)]
        include_full_query: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    let log_level = match cli.verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| log_level.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    match cli.command {
        Commands::Analyze {
            host,
            port,
            database,
            username,
            password,
            compute,
            storage_type,
            workload_type,
        } => {
            info!("Analyzing database: {}", database);
            let config = DbConfig::from_connection_params(
                host,
                port,
                database,
                username,
                password,
                compute,
                storage_type,
                workload_type,
            );

            let mut checker = ConfigChecker::new(config).await?;
            let results = checker.analyze().await?;

            let reporter = Reporter::new(cli.format);
            reporter.report(&results)?;
        }
        Commands::Config { config_path } => {
            info!("Loading config from: {}", config_path);
            let configs = DbConfig::from_config_file(&config_path)?;

            for config in configs {
                info!("Analyzing database: {}", config.database);
                let mut checker = ConfigChecker::new(config).await?;
                let results = checker.analyze().await?;

                let reporter = Reporter::new(cli.format);
                reporter.report(&results)?;
            }
        }
        Commands::Workload {
            host,
            port,
            database,
            username,
            password,
            limit,
            min_calls,
            max_query_len,
            include_full_query,
        } => {
            info!("Analyzing workload for database: {}", database);
            let config = DbConfig::from_connection_params(
                host,
                port,
                database,
                username,
                password,
                None,
                StorageType::Ssd,
                WorkloadType::Oltp,
            );

            let mut checker = ConfigChecker::new(config).await?;
            let opts = WorkloadOptions {
                limit,
                min_calls,
                max_query_len,
                include_full_query,
            };
            let results = checker.analyze_workload(opts).await?;

            let reporter = WorkloadReporter::new(cli.format);
            reporter.report(&results)?;
        }
    }

    Ok(())
}
