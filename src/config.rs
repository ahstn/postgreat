use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use snafu::{ResultExt, Snafu};
use sqlx::postgres::PgConnectOptions;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Snafu)]
pub enum ConfigError {
    #[snafu(display("Failed to read config file: {}", source))]
    FileRead { source: std::io::Error },

    #[snafu(display("Failed to read current working directory: {}", source))]
    CurrentDirRead { source: std::io::Error },

    #[snafu(display("Failed to parse YAML config: {}", source))]
    YamlParse { source: serde_yaml::Error },

    #[snafu(display("Invalid compute spec format: {}", spec))]
    InvalidComputeSpec { spec: String },

    #[snafu(display(
        "Config field '{}' references environment variable '{}', but it is not set",
        field,
        var
    ))]
    MissingEnvVar { field: &'static str, var: String },

    #[snafu(display("Config field '{}' uses unsupported env placeholder '{}'; use the exact form '{{env:VAR_NAME}}'", field, value))]
    InvalidEnvPlaceholder { field: &'static str, value: String },

    #[snafu(display(
        "Environment variable '{}' for config field '{}' has invalid value '{}' (expected {})",
        var,
        field,
        value,
        expected
    ))]
    InvalidEnvValue {
        field: &'static str,
        var: String,
        value: String,
        expected: &'static str,
    },

    #[snafu(display(
        "Config field '{}' has invalid value '{}' (expected {})",
        field,
        value,
        expected
    ))]
    InvalidFieldValue {
        field: &'static str,
        value: String,
        expected: &'static str,
    },

    #[snafu(display("Failed to load dotenv file '{}': {}", path.display(), source))]
    DotenvLoad {
        path: PathBuf,
        source: dotenvy::Error,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    pub compute: Option<ComputeSpec>,
    #[serde(default)]
    pub storage_type: StorageType,
    #[serde(default)]
    pub workload_type: WorkloadType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum StorageType {
    #[default]
    Ssd,
    Hdd,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum WorkloadType {
    #[default]
    Oltp,
    Olap,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComputeSpec {
    pub vcpu: usize,
    pub memory_gb: usize,
}

#[derive(Debug, Deserialize)]
struct RawDbConfig {
    host: Value,
    port: Value,
    database: Value,
    username: Value,
    password: Value,
    compute: Option<RawComputeSpec>,
    #[serde(default)]
    storage_type: Option<Value>,
    #[serde(default)]
    workload_type: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct RawComputeSpec {
    vcpu: Value,
    memory_gb: Value,
}

enum ResolvedToken {
    Literal(String),
    Env { var: String, value: String },
}

enum ValueSource {
    Literal,
    Env(String),
}

type Result<T, E = ConfigError> = std::result::Result<T, E>;

impl DbConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn from_connection_params(
        host: String,
        port: u16,
        database: String,
        username: String,
        password: String,
        compute: Option<String>,
        storage_type: StorageType,
        workload_type: WorkloadType,
    ) -> Self {
        let compute_spec = compute
            .map(|c| ComputeSpec::from_string(&c))
            .transpose()
            .unwrap_or_else(|e| {
                tracing::warn!("Failed to parse compute spec: {}", e);
                None
            });

        Self {
            host,
            port,
            database,
            username,
            password,
            compute: compute_spec,
            storage_type,
            workload_type,
        }
    }

    pub fn from_config_file(path: &str) -> Result<Vec<Self>> {
        let content = fs::read_to_string(path).context(FileReadSnafu)?;
        parse_configs_with_env(&content, &lookup_env_var)
    }

    pub fn connection_options(&self) -> PgConnectOptions {
        PgConnectOptions::new()
            .host(&self.host)
            .port(self.port)
            .username(&self.username)
            .password(&self.password)
            .database(&self.database)
    }
}

impl ComputeSpec {
    pub fn from_string(spec: &str) -> Result<Self> {
        // Handle predefined sizes
        match spec.to_lowercase().as_str() {
            "small" => Ok(Self {
                vcpu: 2,
                memory_gb: 16,
            }),
            "medium" => Ok(Self {
                vcpu: 8,
                memory_gb: 64,
            }),
            "large" => Ok(Self {
                vcpu: 32,
                memory_gb: 256,
            }),
            _ => {
                // Parse format: "8vCPU-64GB" or "4vCPU-16GB"
                let parts: Vec<&str> = spec.split('-').collect();
                if parts.len() != 2 {
                    return Err(ConfigError::InvalidComputeSpec {
                        spec: spec.to_string(),
                    });
                }

                let vcpu_part = parts[0].to_lowercase();
                let vcpu_str = vcpu_part.trim_end_matches("vcpu");
                let memory_part = parts[1].to_lowercase();
                let memory_str = memory_part.trim_end_matches("gb");

                let vcpu =
                    vcpu_str
                        .parse::<usize>()
                        .map_err(|_| ConfigError::InvalidComputeSpec {
                            spec: spec.to_string(),
                        })?;

                let memory_gb =
                    memory_str
                        .parse::<usize>()
                        .map_err(|_| ConfigError::InvalidComputeSpec {
                            spec: spec.to_string(),
                        })?;

                Ok(Self { vcpu, memory_gb })
            }
        }
    }

    pub fn memory_mb(&self) -> usize {
        self.memory_gb * 1024
    }
}

impl RawDbConfig {
    fn resolve<F>(self, env_lookup: &F) -> Result<DbConfig>
    where
        F: Fn(&str) -> Option<String>,
    {
        Ok(DbConfig {
            host: resolve_string(self.host, "host", env_lookup)?,
            port: resolve_u16(self.port, "port", env_lookup)?,
            database: resolve_string(self.database, "database", env_lookup)?,
            username: resolve_string(self.username, "username", env_lookup)?,
            password: resolve_string(self.password, "password", env_lookup)?,
            compute: self
                .compute
                .map(|compute| compute.resolve(env_lookup))
                .transpose()?,
            storage_type: match self.storage_type {
                Some(value) => resolve_storage_type(value, "storage_type", env_lookup)?,
                None => StorageType::default(),
            },
            workload_type: match self.workload_type {
                Some(value) => resolve_workload_type(value, "workload_type", env_lookup)?,
                None => WorkloadType::default(),
            },
        })
    }
}

impl RawComputeSpec {
    fn resolve<F>(self, env_lookup: &F) -> Result<ComputeSpec>
    where
        F: Fn(&str) -> Option<String>,
    {
        Ok(ComputeSpec {
            vcpu: resolve_usize(self.vcpu, "compute.vcpu", env_lookup)?,
            memory_gb: resolve_usize(self.memory_gb, "compute.memory_gb", env_lookup)?,
        })
    }
}

impl ResolvedToken {
    fn into_parts(self) -> (String, ValueSource) {
        match self {
            Self::Literal(value) => (value, ValueSource::Literal),
            Self::Env { var, value } => (value, ValueSource::Env(var)),
        }
    }

    fn into_value(self) -> String {
        match self {
            Self::Literal(value) => value,
            Self::Env { value, .. } => value,
        }
    }
}

pub fn load_dotenv_files_from_cli_args<I, S>(args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let cwd = env::current_dir().context(CurrentDirReadSnafu)?;
    let config_path = find_config_path(args);
    let dotenv_values = collect_dotenv_values(config_path.as_deref(), &cwd)?;
    let pending_updates = pending_dotenv_updates(|key| env::var_os(key).is_some(), dotenv_values);

    for (key, value) in pending_updates {
        // This runs before CLI parsing and before the Tokio runtime starts any worker threads.
        unsafe {
            env::set_var(key, value);
        }
    }

    Ok(())
}

fn parse_configs_with_env<F>(content: &str, env_lookup: &F) -> Result<Vec<DbConfig>>
where
    F: Fn(&str) -> Option<String>,
{
    let configs: Vec<RawDbConfig> = serde_yaml::from_str(content).context(YamlParseSnafu)?;
    configs
        .into_iter()
        .map(|config| config.resolve(env_lookup))
        .collect()
}

fn lookup_env_var(name: &str) -> Option<String> {
    env::var(name).ok()
}

fn resolve_string<F>(value: Value, field: &'static str, env_lookup: &F) -> Result<String>
where
    F: Fn(&str) -> Option<String>,
{
    match value {
        Value::String(raw) => Ok(resolve_token(raw, field, env_lookup)?.into_value()),
        other => Err(ConfigError::InvalidFieldValue {
            field,
            value: value_to_string(&other),
            expected: "a string or quoted {env:VAR_NAME}",
        }),
    }
}

fn resolve_u16<F>(value: Value, field: &'static str, env_lookup: &F) -> Result<u16>
where
    F: Fn(&str) -> Option<String>,
{
    match value {
        Value::Number(number) => parse_with_source(
            number.to_string(),
            ValueSource::Literal,
            field,
            "an integer between 0 and 65535",
            |raw| raw.parse::<u16>().ok(),
        ),
        Value::String(raw) => {
            let (value, source) = resolve_token(raw, field, env_lookup)?.into_parts();
            parse_with_source(
                value,
                source,
                field,
                "an integer between 0 and 65535",
                |raw| raw.parse::<u16>().ok(),
            )
        }
        other => Err(ConfigError::InvalidFieldValue {
            field,
            value: value_to_string(&other),
            expected: "an integer between 0 and 65535",
        }),
    }
}

fn resolve_usize<F>(value: Value, field: &'static str, env_lookup: &F) -> Result<usize>
where
    F: Fn(&str) -> Option<String>,
{
    match value {
        Value::Number(number) => parse_with_source(
            number.to_string(),
            ValueSource::Literal,
            field,
            "a non-negative integer",
            |raw| raw.parse::<usize>().ok(),
        ),
        Value::String(raw) => {
            let (value, source) = resolve_token(raw, field, env_lookup)?.into_parts();
            parse_with_source(value, source, field, "a non-negative integer", |raw| {
                raw.parse::<usize>().ok()
            })
        }
        other => Err(ConfigError::InvalidFieldValue {
            field,
            value: value_to_string(&other),
            expected: "a non-negative integer",
        }),
    }
}

fn resolve_storage_type<F>(value: Value, field: &'static str, env_lookup: &F) -> Result<StorageType>
where
    F: Fn(&str) -> Option<String>,
{
    match value {
        Value::String(raw) => {
            let (value, source) = resolve_token(raw, field, env_lookup)?.into_parts();
            parse_with_source(value, source, field, "'ssd' or 'hdd'", parse_storage_type)
        }
        other => Err(ConfigError::InvalidFieldValue {
            field,
            value: value_to_string(&other),
            expected: "'ssd' or 'hdd'",
        }),
    }
}

fn resolve_workload_type<F>(
    value: Value,
    field: &'static str,
    env_lookup: &F,
) -> Result<WorkloadType>
where
    F: Fn(&str) -> Option<String>,
{
    match value {
        Value::String(raw) => {
            let (value, source) = resolve_token(raw, field, env_lookup)?.into_parts();
            parse_with_source(
                value,
                source,
                field,
                "'oltp' or 'olap'",
                parse_workload_type,
            )
        }
        other => Err(ConfigError::InvalidFieldValue {
            field,
            value: value_to_string(&other),
            expected: "'oltp' or 'olap'",
        }),
    }
}

fn resolve_token<F>(raw: String, field: &'static str, env_lookup: &F) -> Result<ResolvedToken>
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(var) = parse_env_placeholder(&raw) {
        let value = env_lookup(var).ok_or_else(|| ConfigError::MissingEnvVar {
            field,
            var: var.to_string(),
        })?;
        return Ok(ResolvedToken::Env {
            var: var.to_string(),
            value,
        });
    }

    if raw.contains("{env:") {
        return Err(ConfigError::InvalidEnvPlaceholder { field, value: raw });
    }

    Ok(ResolvedToken::Literal(raw))
}

fn parse_env_placeholder(raw: &str) -> Option<&str> {
    raw.strip_prefix("{env:")
        .and_then(|value| value.strip_suffix('}'))
        .filter(|value| !value.is_empty())
}

fn parse_with_source<T, F>(
    value: String,
    source: ValueSource,
    field: &'static str,
    expected: &'static str,
    parser: F,
) -> Result<T>
where
    F: FnOnce(&str) -> Option<T>,
{
    parser(&value).ok_or(match source {
        ValueSource::Literal => ConfigError::InvalidFieldValue {
            field,
            value,
            expected,
        },
        ValueSource::Env(var) => ConfigError::InvalidEnvValue {
            field,
            var,
            value,
            expected,
        },
    })
}

fn parse_storage_type(value: &str) -> Option<StorageType> {
    match value.to_ascii_lowercase().as_str() {
        "ssd" => Some(StorageType::Ssd),
        "hdd" => Some(StorageType::Hdd),
        _ => None,
    }
}

fn parse_workload_type(value: &str) -> Option<WorkloadType> {
    match value.to_ascii_lowercase().as_str() {
        "oltp" => Some(WorkloadType::Oltp),
        "olap" => Some(WorkloadType::Olap),
        _ => None,
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Sequence(_) | Value::Mapping(_) | Value::Tagged(_) => serde_yaml::to_string(value)
            .unwrap_or_else(|_| "<unprintable yaml value>".to_string())
            .trim()
            .to_string(),
    }
}

fn find_config_path<I, S>(args: I) -> Option<PathBuf>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut iter = args.into_iter().skip(1);
    let mut in_config_subcommand = false;

    while let Some(arg) = iter.next() {
        let arg = arg.as_ref();

        if !in_config_subcommand {
            in_config_subcommand = arg == "config";
            continue;
        }

        if arg == "-c" || arg == "--config" {
            return iter.next().map(|value| PathBuf::from(value.as_ref()));
        }

        if let Some(value) = arg.strip_prefix("--config=") {
            return Some(PathBuf::from(value));
        }
    }

    None
}

fn collect_dotenv_values(
    config_path: Option<&Path>,
    cwd: &Path,
) -> Result<HashMap<String, String>> {
    let mut values = HashMap::new();

    if let Some(path) = find_dotenv_path(cwd) {
        values.extend(read_dotenv_file(&path)?);
    }

    if let Some(config_path) = config_path {
        let config_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
        let config_dotenv_path = config_dir.join(".env");
        if config_dotenv_path.is_file() {
            for (key, value) in read_dotenv_file(&config_dotenv_path)? {
                values.insert(key, value);
            }
        }
    }

    Ok(values)
}

fn pending_dotenv_updates<F>(
    existing_lookup: F,
    dotenv_values: HashMap<String, String>,
) -> HashMap<String, String>
where
    F: Fn(&str) -> bool,
{
    dotenv_values
        .into_iter()
        .filter(|(key, _)| !existing_lookup(key))
        .collect()
}

fn find_dotenv_path(start: &Path) -> Option<PathBuf> {
    start
        .ancestors()
        .map(|dir| dir.join(".env"))
        .find(|path| path.is_file())
}

fn read_dotenv_file(path: &Path) -> Result<HashMap<String, String>> {
    let mut values = HashMap::new();
    let iter = dotenvy::from_path_iter(path).context(DotenvLoadSnafu {
        path: path.to_path_buf(),
    })?;

    for item in iter {
        let (key, value) = item.context(DotenvLoadSnafu {
            path: path.to_path_buf(),
        })?;
        values.entry(key).or_insert(value);
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn parse_configs(
        content: &str,
        env_values: &[(&str, &str)],
    ) -> Result<Vec<DbConfig>, ConfigError> {
        let env_lookup = |key: &str| {
            env_values
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| (*value).to_string())
        };

        parse_configs_with_env(content, &env_lookup)
    }

    #[test]
    fn test_compute_spec_parsing() {
        assert_eq!(
            ComputeSpec::from_string("small").unwrap(),
            ComputeSpec {
                vcpu: 2,
                memory_gb: 16
            }
        );

        assert_eq!(
            ComputeSpec::from_string("8vCPU-64GB").unwrap(),
            ComputeSpec {
                vcpu: 8,
                memory_gb: 64
            }
        );

        assert_eq!(
            ComputeSpec::from_string("4vcpu-16gb").unwrap(),
            ComputeSpec {
                vcpu: 4,
                memory_gb: 16
            }
        );
    }

    #[test]
    fn test_config_file_literal_values_parse_unchanged() {
        let configs = parse_configs(
            r#"
- host: db1.example.com
  port: 5432
  database: production_db
  username: postgres
  password: secret
  compute:
    vcpu: 8
    memory_gb: 64
  storage_type: hdd
  workload_type: olap
"#,
            &[],
        )
        .unwrap();

        assert_eq!(configs.len(), 1);
        let config = &configs[0];
        assert_eq!(config.host, "db1.example.com");
        assert_eq!(config.port, 5432);
        assert_eq!(config.database, "production_db");
        assert_eq!(config.username, "postgres");
        assert_eq!(config.password, "secret");
        assert_eq!(
            config.compute,
            Some(ComputeSpec {
                vcpu: 8,
                memory_gb: 64
            })
        );
        assert_eq!(config.storage_type, StorageType::Hdd);
        assert_eq!(config.workload_type, WorkloadType::Olap);
    }

    #[test]
    fn test_config_file_resolves_string_env_placeholders() {
        let configs = parse_configs(
            r#"
- host: "{env:PG_HOST}"
  port: 5432
  database: "{env:PG_DATABASE}"
  username: "{env:PG_USER}"
  password: "{env:PG_PASSWORD}"
"#,
            &[
                ("PG_HOST", "db.internal"),
                ("PG_DATABASE", "app"),
                ("PG_USER", "reader"),
                ("PG_PASSWORD", "super-secret"),
            ],
        )
        .unwrap();

        let config = &configs[0];
        assert_eq!(config.host, "db.internal");
        assert_eq!(config.database, "app");
        assert_eq!(config.username, "reader");
        assert_eq!(config.password, "super-secret");
    }

    #[test]
    fn test_config_file_resolves_numeric_env_placeholders() {
        let configs = parse_configs(
            r#"
- host: localhost
  port: "{env:PG_PORT}"
  database: app
  username: postgres
  password: secret
  compute:
    vcpu: "{env:PG_VCPU}"
    memory_gb: "{env:PG_MEMORY_GB}"
"#,
            &[
                ("PG_PORT", "6432"),
                ("PG_VCPU", "16"),
                ("PG_MEMORY_GB", "128"),
            ],
        )
        .unwrap();

        let config = &configs[0];
        assert_eq!(config.port, 6432);
        assert_eq!(
            config.compute,
            Some(ComputeSpec {
                vcpu: 16,
                memory_gb: 128
            })
        );
    }

    #[test]
    fn test_config_file_resolves_enum_env_placeholders() {
        let configs = parse_configs(
            r#"
- host: localhost
  port: 5432
  database: app
  username: postgres
  password: secret
  storage_type: "{env:PG_STORAGE_TYPE}"
  workload_type: "{env:PG_WORKLOAD_TYPE}"
"#,
            &[("PG_STORAGE_TYPE", "ssd"), ("PG_WORKLOAD_TYPE", "olap")],
        )
        .unwrap();

        let config = &configs[0];
        assert_eq!(config.storage_type, StorageType::Ssd);
        assert_eq!(config.workload_type, WorkloadType::Olap);
    }

    #[test]
    fn test_config_file_errors_for_missing_env_var() {
        let err = parse_configs(
            r#"
- host: localhost
  port: 5432
  database: app
  username: postgres
  password: "{env:MISSING_PASSWORD}"
"#,
            &[],
        )
        .unwrap_err();

        assert!(matches!(
            err,
            ConfigError::MissingEnvVar {
                field: "password",
                ref var
            } if var == "MISSING_PASSWORD"
        ));
    }

    #[test]
    fn test_config_file_errors_for_invalid_numeric_env_var() {
        let err = parse_configs(
            r#"
- host: localhost
  port: "{env:PG_PORT}"
  database: app
  username: postgres
  password: secret
"#,
            &[("PG_PORT", "not-a-port")],
        )
        .unwrap_err();

        assert!(matches!(
            err,
            ConfigError::InvalidEnvValue {
                field: "port",
                ref var,
                ref value,
                ..
            } if var == "PG_PORT" && value == "not-a-port"
        ));
    }

    #[test]
    fn test_config_file_errors_for_invalid_enum_env_var() {
        let err = parse_configs(
            r#"
- host: localhost
  port: 5432
  database: app
  username: postgres
  password: secret
  storage_type: "{env:PG_STORAGE_TYPE}"
"#,
            &[("PG_STORAGE_TYPE", "nvme")],
        )
        .unwrap_err();

        assert!(matches!(
            err,
            ConfigError::InvalidEnvValue {
                field: "storage_type",
                ref var,
                ref value,
                ..
            } if var == "PG_STORAGE_TYPE" && value == "nvme"
        ));
    }

    #[test]
    fn test_collect_dotenv_values_prefers_config_dir_then_existing_env() {
        let temp = tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let config_dir = temp.path().join("configs");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&config_dir).unwrap();
        fs::write(
            temp.path().join(".env"),
            "SHARED=from-cwd\nONLY_CWD=available\n",
        )
        .unwrap();
        fs::write(
            config_dir.join(".env"),
            "SHARED=from-config\nONLY_CONFIG=available\n",
        )
        .unwrap();

        let config_path = config_dir.join("db-config.yaml");
        let collected = collect_dotenv_values(Some(&config_path), &workspace).unwrap();

        assert_eq!(collected.get("SHARED"), Some(&"from-config".to_string()));
        assert_eq!(collected.get("ONLY_CWD"), Some(&"available".to_string()));
        assert_eq!(collected.get("ONLY_CONFIG"), Some(&"available".to_string()));

        let pending = pending_dotenv_updates(|key| key == "SHARED", collected);

        assert!(!pending.contains_key("SHARED"));
        assert_eq!(pending.get("ONLY_CWD"), Some(&"available".to_string()));
        assert_eq!(pending.get("ONLY_CONFIG"), Some(&"available".to_string()));
    }
}
