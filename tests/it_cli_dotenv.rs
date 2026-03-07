use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;
use std::fs;
use tempfile::tempdir;

#[test]
fn analyze_loads_dotenv_before_clap_parsing() {
    let temp = tempdir().expect("temp dir should be created");
    fs::write(
        temp.path().join(".env"),
        "POSTGRES_HOST=127.0.0.1\nPOSTGRES_PORT=1\nPOSTGRES_DATABASE=dotenv_db\nPOSTGRES_USER=dotenv_user\nPOSTGRES_PASSWORD=dotenv_password\n",
    )
    .expect(".env should be written");

    let mut command = cargo_bin_cmd!("postgreat");
    command
        .current_dir(temp.path())
        .env_remove("POSTGRES_HOST")
        .env_remove("POSTGRES_PORT")
        .env_remove("POSTGRES_DATABASE")
        .env_remove("POSTGRES_USER")
        .env_remove("POSTGRES_PASSWORD")
        .args(["analyze", "--compute", "small"]);

    command.assert().failure().stderr(
        predicate::str::contains("Failed to connect to database")
            .and(predicate::str::contains("required arguments were not provided").not()),
    );
}
