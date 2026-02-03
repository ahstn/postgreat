use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::fs;

#[derive(Debug, Snafu)]
pub enum ConfigError {
    #[snafu(display("Failed to read config file: {}", source))]
    FileRead { source: std::io::Error },

    #[snafu(display("Failed to parse YAML config: {}", source))]
    YamlParse { source: serde_yaml::Error },

    #[snafu(display("Invalid compute spec format: {}", spec))]
    InvalidComputeSpec { spec: String },
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
        let configs: Vec<DbConfig> = serde_yaml::from_str(&content).context(YamlParseSnafu)?;
        Ok(configs)
    }

    pub fn connection_string(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.username, self.password, self.host, self.port, self.database
        )
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
