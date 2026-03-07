#![allow(dead_code)]

use assert_cmd::{cargo::cargo_bin_cmd, Command};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, raw_sql, Pool, Postgres};
use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use testcontainers_modules::{
    postgres,
    testcontainers::{runners::SyncRunner, Container, ImageExt},
};

const TEMPLATE_DB: &str = "postgreat_template";
const ADMIN_USER: &str = "postgres";
const ADMIN_PASSWORD: &str = "postgres";
const APP_USER: &str = "app_user";
const APP_PASSWORD: &str = "app_password";
const READER_USER: &str = "reader_user";
const READER_PASSWORD: &str = "reader_password";

const FIXTURE_SCHEMA_SQL: &str = include_str!("../_data/0-schema.sql");
const FIXTURE_DATA_SQL: &str = include_str!("../_data/1-data.sql");
const FIXTURE_BLOAT_SQL: &str = include_str!("../_data/2-bloat-and-indexes.sql");
const FIXTURE_ROLES_SQL: &str = include_str!("../_data/00-extensions-and-roles.sql");
const FIXTURE_WORKLOAD_SQL: &str = include_str!("../_data/3-workload.sql");

#[derive(Debug, Clone, Copy)]
pub enum ContainerProfile {
    WorkloadEnabled,
    WorkloadEnabledLowMax,
    NoPreload,
}

#[derive(Debug, Clone, Copy)]
pub enum TestRole {
    Admin,
    App,
    Reader,
}

pub struct TestPostgres {
    _container: Container<postgres::Postgres>,
    runtime: tokio::runtime::Runtime,
    host: String,
    port: u16,
    version_tag: String,
}

#[derive(Debug, Clone)]
pub struct TestDatabase {
    host: String,
    port: u16,
    name: String,
}

impl TestPostgres {
    pub fn start(profile: ContainerProfile) -> Self {
        let version_tag = env::var("POSTGREAT_TEST_PG_VERSION").unwrap_or_else(|_| "18".into());
        let image = postgres::Postgres::default()
            .with_init_sql(template_init_sql())
            .with_tag(version_tag.as_str())
            .with_cmd(container_cmd(profile))
            .with_startup_timeout(Duration::from_secs(120));
        let container = image.start().expect("postgres test container should start");
        let host = container
            .get_host()
            .expect("postgres host should resolve")
            .to_string();
        let port = container
            .get_host_port_ipv4(5432)
            .expect("postgres port should resolve");
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime should start");

        let server = Self {
            _container: container,
            runtime,
            host,
            port,
            version_tag,
        };
        server.seed_template_database();
        server
    }

    pub fn version_tag(&self) -> &str {
        &self.version_tag
    }

    pub fn create_test_database(&self, suffix: &str) -> TestDatabase {
        let db_name = format!(
            "pgtest_{}_{}_{}",
            sanitize_ident(suffix),
            sanitize_ident(self.version_tag()),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_millis()
        );
        let create_sql = format!(
            "CREATE DATABASE {} TEMPLATE {}",
            quote_ident(&db_name),
            quote_ident(TEMPLATE_DB)
        );
        let grant_sql = format!(
            "GRANT CONNECT ON DATABASE {} TO {}, {}",
            quote_ident(&db_name),
            quote_ident(APP_USER),
            quote_ident(READER_USER)
        );

        self.runtime.block_on(async {
            let admin_pool = self.admin_pool("postgres").await;
            sqlx::query(&create_sql)
                .execute(&admin_pool)
                .await
                .expect("test database should be created from template");
            sqlx::query(&grant_sql)
                .execute(&admin_pool)
                .await
                .expect("test roles should receive CONNECT on cloned database");
        });

        TestDatabase {
            host: self.host.clone(),
            port: self.port,
            name: db_name,
        }
    }

    pub fn apply_table_index_fixture(&self, db: &TestDatabase) {
        self.execute_sql(db, TestRole::Admin, FIXTURE_BLOAT_SQL);
        self.execute_sql(db, TestRole::Admin, "ANALYZE;");
    }

    pub fn run_workload_fixture_as_app(&self, db: &TestDatabase) {
        self.execute_sql(db, TestRole::App, FIXTURE_WORKLOAD_SQL);
    }

    pub fn reset_pg_stat_statements(&self, db: &TestDatabase) {
        self.execute_sql(db, TestRole::Admin, "SELECT pg_stat_statements_reset();");
    }

    pub fn drop_pg_stat_statements_extension(&self, db: &TestDatabase) {
        self.execute_sql(
            db,
            TestRole::Admin,
            "DROP EXTENSION IF EXISTS pg_stat_statements;",
        );
    }

    pub fn generate_distinct_queries_as_app(&self, db: &TestDatabase, count: usize) {
        self.runtime.block_on(async {
            let pool = self.pool_for_role(db, TestRole::App).await;
            for query_shape in 1..=count {
                let mut predicates = Vec::new();
                for value in 1..=query_shape {
                    predicates.push(format!("rental_id = {value}"));
                }
                let sql = format!(
                    "SELECT COUNT(*) FROM rental WHERE {};",
                    predicates.join(" OR ")
                );
                sqlx::query(&sql)
                    .execute(&pool)
                    .await
                    .expect("distinct workload query should execute");
            }
        });
    }

    pub fn pg_stat_statements_diagnostics(&self, db: &TestDatabase) -> Value {
        self.runtime.block_on(async {
            let pool = self.pool_for_role(db, TestRole::Admin).await;
            let max_entries: i64 = sqlx::query_scalar(
                "SELECT current_setting('pg_stat_statements.max')::bigint",
            )
            .fetch_one(&pool)
            .await
            .expect("pg_stat_statements.max should be readable");
            let statement_count: i64 = sqlx::query_scalar("SELECT count(*)::bigint FROM pg_stat_statements")
                .fetch_one(&pool)
                .await
                .expect("pg_stat_statements count should be readable");
            let deallocations: i64 =
                sqlx::query_scalar("SELECT COALESCE(dealloc, 0)::bigint FROM pg_stat_statements_info")
                    .fetch_one(&pool)
                    .await
                    .expect("pg_stat_statements_info should be readable");
            json!({
                "pg_stat_statements_max": max_entries,
                "statement_count": statement_count,
                "deallocations": deallocations,
            })
        })
    }

    pub fn analyze_command(&self, db: &TestDatabase, role: TestRole) -> Command {
        let mut command = cargo_bin_cmd!("postgreat");
        let credentials = db.credentials(role);
        command.args([
            "--format",
            "json",
            "analyze",
            "--host",
            db.host(),
            "--port",
            &db.port().to_string(),
            "--database",
            db.name(),
            "--username",
            credentials.0,
            "--password",
            credentials.1,
            "--compute",
            "8vCPU-64GB",
        ]);
        command
    }

    pub fn workload_command(&self, db: &TestDatabase, role: TestRole) -> Command {
        let mut command = cargo_bin_cmd!("postgreat");
        let credentials = db.credentials(role);
        command.args([
            "--format",
            "json",
            "workload",
            "--host",
            db.host(),
            "--port",
            &db.port().to_string(),
            "--database",
            db.name(),
            "--username",
            credentials.0,
            "--password",
            credentials.1,
            "--limit",
            "10",
            "--min-calls",
            "1",
            "--include-full-query",
        ]);
        command
    }

    fn seed_template_database(&self) {
        self.runtime.block_on(async {
            let template_pool = self.admin_pool(TEMPLATE_DB).await;
            sqlx::query("SELECT 1")
                .execute(&template_pool)
                .await
                .expect("template database should be ready after init SQL");
        });
    }

    fn execute_sql(&self, db: &TestDatabase, role: TestRole, sql: &str) {
        self.runtime.block_on(async {
            let pool = self.pool_for_role(db, role).await;
            raw_sql(sql)
                .execute(&pool)
                .await
                .expect("fixture SQL should execute");
        });
    }

    async fn admin_pool(&self, database: &str) -> Pool<Postgres> {
        let url = format!(
            "postgres://{}:{}@{}:{}/{}",
            ADMIN_USER, ADMIN_PASSWORD, self.host, self.port, database
        );
        PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("admin pool should connect")
    }

    async fn pool_for_role(&self, db: &TestDatabase, role: TestRole) -> Pool<Postgres> {
        let credentials = db.credentials(role);
        let url = format!(
            "postgres://{}:{}@{}:{}/{}",
            credentials.0, credentials.1, db.host, db.port, db.name
        );
        PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("role pool should connect")
    }
}

impl TestDatabase {
    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn credentials(&self, role: TestRole) -> (&'static str, &'static str) {
        match role {
            TestRole::Admin => (ADMIN_USER, ADMIN_PASSWORD),
            TestRole::App => (APP_USER, APP_PASSWORD),
            TestRole::Reader => (READER_USER, READER_PASSWORD),
        }
    }
}

pub fn parse_json_output(output: &[u8]) -> Value {
    serde_json::from_slice(output).unwrap_or_else(|err| {
        panic!(
            "CLI output should be valid JSON: {err}; stdout was: {}",
            String::from_utf8_lossy(output)
        )
    })
}

pub fn analyze_snapshot_view(value: &Value) -> Value {
    json!({
        "has_rental_bloat": has_table_named(&value["bloat_info"], "rental"),
        "has_rental_seq_scan": has_table_named(&value["seq_scan_info"], "rental"),
        "has_failed_index_only_finding": has_issue_kind(&value["index_usage_info"], "failed_index_only"),
    })
}

pub fn workload_happy_path_snapshot_view(value: &Value) -> Value {
    json!({
        "metadata": workload_metadata_summary(value),
        "warning_categories": warning_categories(&value["warnings"]),
        "slow_query_kinds": slow_query_kinds(value),
        "has_rental_candidate": has_candidate_for_table(value, "rental"),
        "has_non_equality_candidate": has_non_equality_candidate(value),
        "has_table_health_note": has_table_health_note(value),
    })
}

pub fn workload_visibility_snapshot_view(value: &Value) -> Value {
    json!({
        "metadata": workload_metadata_summary(value),
        "warning_categories": warning_categories(&value["warnings"]),
        "slow_query_kinds": slow_query_kinds(value),
    })
}

pub fn workload_dealloc_snapshot_view(value: &Value) -> Value {
    json!({
        "metadata": workload_metadata_summary(value),
        "warning_categories": warning_categories(&value["warnings"]),
        "slow_query_kinds": slow_query_kinds(value),
    })
}

pub fn workload_unavailable_snapshot_view(value: &Value) -> Value {
    json!({
        "warning_categories": warning_categories(&value["warnings"]),
        "stats_reset_state": stats_reset_state(value),
        "slow_query_group_count": value["slow_query_groups"]
            .as_array()
            .map_or(0, Vec::len),
        "candidate_count": value["query_index_candidates"]
            .as_array()
            .map_or(0, Vec::len),
    })
}

fn container_cmd(profile: ContainerProfile) -> Vec<String> {
    let mut args = vec!["-c".into()];
    match profile {
        ContainerProfile::WorkloadEnabled => {
            args.push("shared_preload_libraries=pg_stat_statements".into());
        }
        ContainerProfile::WorkloadEnabledLowMax => {
            args.push("shared_preload_libraries=pg_stat_statements".into());
            args.extend(["-c".into(), "pg_stat_statements.max=100".into()]);
        }
        ContainerProfile::NoPreload => {
            args.push("log_min_messages=warning".into());
        }
    }

    if !matches!(profile, ContainerProfile::NoPreload) {
        args.extend([
            "-c".into(),
            "compute_query_id=on".into(),
            "-c".into(),
            "pg_stat_statements.track=all".into(),
        ]);
    }

    args
}

fn template_init_sql() -> Vec<u8> {
    format!(
        "CREATE DATABASE {template_db};\n\\connect {template_db}\n{schema}\n{data}\nSET search_path = public;\n{roles}\nANALYZE;\n",
        template_db = TEMPLATE_DB,
        schema = FIXTURE_SCHEMA_SQL,
        data = FIXTURE_DATA_SQL,
        roles = FIXTURE_ROLES_SQL,
    )
    .into_bytes()
}

fn has_table_named(value: &Value, table_name: &str) -> bool {
    value
        .as_array()
        .into_iter()
        .flatten()
        .any(|entry| entry["table_name"].as_str() == Some(table_name))
}

fn has_issue_kind(value: &Value, issue: &str) -> bool {
    value
        .as_array()
        .into_iter()
        .flatten()
        .any(|entry| entry["issue"].as_str() == Some(issue))
}

fn workload_metadata_summary(value: &Value) -> Value {
    json!({
        "data_source": value["workload_metadata"]["data_source"],
        "scope": value["workload_metadata"]["scope"],
        "query_text_visible": value["workload_metadata"]["query_text_visible"],
        "stats_reset_state": stats_reset_state(value),
        "entry_deallocations_state": entry_deallocations_state(value),
    })
}

fn stats_reset_state(value: &Value) -> &'static str {
    if value["workload_metadata"]["stats_reset_at"].is_null() {
        "unavailable"
    } else {
        "available"
    }
}

fn entry_deallocations_state(value: &Value) -> &'static str {
    match value["workload_metadata"]["entry_deallocations"].as_i64() {
        Some(0) => "zero",
        Some(_) => "nonzero",
        None => "unavailable",
    }
}

fn warning_categories(value: &Value) -> Vec<Value> {
    let mut categories = value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|warning| {
            let warning = warning.as_str()?;
            let category = if warning.contains("last reset at") {
                Some("recent_reset")
            } else if warning.contains("pg_read_all_stats") {
                Some("limited_visibility")
            } else if warning.contains("evicted") {
                Some("deallocations")
            } else if warning.contains("not installed") {
                Some("extension_missing")
            } else if warning.contains("not usable") {
                Some("preload_unavailable")
            } else {
                None
            }?;
            Some(Value::String(category.to_string()))
        })
        .collect::<Vec<_>>();
    categories.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    categories.dedup();
    categories
}

fn slow_query_kinds(value: &Value) -> Vec<Value> {
    let mut kinds = value["slow_query_groups"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|group| {
            group["kind"]
                .as_str()
                .map(|kind| Value::String(kind.to_string()))
        })
        .collect::<Vec<_>>();
    kinds.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    kinds.dedup();
    kinds
}

fn has_candidate_for_table(value: &Value, table_name: &str) -> bool {
    value["query_index_candidates"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|candidate| candidate["table"].as_str() == Some(table_name))
}

fn has_non_equality_candidate(value: &Value) -> bool {
    value["query_index_candidates"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|candidate| {
            candidate["evidence"]["non_equality_filters"]
                .as_array()
                .is_some_and(|filters| !filters.is_empty())
        })
}

fn has_table_health_note(value: &Value) -> bool {
    value["query_index_candidates"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|candidate| {
            candidate["notes"].as_array().is_some_and(|notes| {
                notes.iter().any(|note| {
                    note.as_str().is_some_and(|note| {
                        note.contains("sequential scan hotspot") || note.contains("bloat watchlist")
                    })
                })
            })
        })
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn sanitize_ident(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}
