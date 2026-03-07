Based on the documentation for tuning postgres instances in @docs/ create a new Rust application with the best practices and idomatic code.

  The application should be similar to https://github.com/jfcoz/postgresqltuner/blob/master/postgresqltuner.pl which covers some similar config. For now our application should only
  look at configuration values, or @"docs/1 - Foundation Tuning - Core Config.md" and @"docs/4 - Automated Maintenance - Autovacuum & Background Worker.md", we will add index
  support later.

  Given our primary target for this is RDS databases, fetching the compute via bash won't be possible. Research for alternatives, or allow the user to specify via CLI args.

  For input a single DB should be accepted via CLI flags and arguments. For multiple DBs, a config file shared the same variable names (host, database, username, etc) as the flags
  will be used.

  For output, use reports with different levels for suggestion like `postgresqltuner.pl` with suggested value, current value, and rationale/reasoning. This should output in stdout
  in a format akin to Markdown.


  I've added a cargo project with clippy and rustfmt for linting and formatting, clap should be used for the CLI, sqlx for DB interactions and optionally tokio (with futures) if
  async is benefitical. Here are other blessed libraries that may help:
  - https://docs.rs/bon/latest/bon/ - Builder pattern and solution to missing named parameters
  - https://github.com/shepmaster/snafu - error handling, anyhow and thiserror in one
      - User guide: https://docs.rs/snafu/latest/snafu/guide/index.html
  - https://lib.rs/crates/tracing - logging standard
      - https://crates.io/crates/tap - debug logging
  - https://lib.rs/crates/serde - (de)serialize standard
  - https://github.com/rust-itertools/itertools - extra interator adaptors

  Create a README.md, and a separate markdown document to store thoughts, progress and remaining items as you work
