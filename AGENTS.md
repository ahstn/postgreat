# Repository Guidelines

## Project Structure & Module Organization
- `src/` holds the CLI crate: `main.rs` for the clap entrypoint, `checker.rs` for DB orchestration, `config.rs` for CLI/YAML parsing, `analysis/` for domain modules (memory, WAL, planner, etc.), and `reporter.rs` for Markdown/JSON/text output.
- `docs/` contains the tuning playbooks referenced by the inception prompt (foundation tuning in `docs/1` and autovacuum guidance in `docs/4`).
- `README.md` explains user flows, while `PROGRESS.md` captures architectural decisions; keep both synchronized with code changes.
- Tests live alongside code (e.g., `config.rs`); integration tests may reside under `tests/` if they grow beyond unit scope.

## Build, Test, and Development Commands
- `cargo fmt && cargo clippy` – enforce Rust style/lints before sending patches.
- `cargo test` – run the current unit test suite (pure Rust, no DB needed yet).
- `cargo run -- analyze ... --compute "8vCPU-64GB"` – exercise the CLI against a target database; POSTGRES_* env vars mirror flag names.

## Coding Style & Naming Conventions
- Use idiomatic Rust with `rustfmt` defaults (4-space indentation, snake_case for functions/modules, CamelCase for types).
- Favor `snafu` for error contexts and `anyhow` only at the binary boundary.
- Keep analyzer modules focused on one domain and share helpers via `analysis::mod`.
- When extending the CLI, document new flags in README examples and clap `help` strings.

## Testing Guidelines
- Prefer deterministic unit tests using fixtures or `rstest`; mock database IO where possible.
- Name tests with the behavior under test (e.g., `test_compute_spec_parsing_tiers`).
- When adding SQLx queries, consider the offline `sqlx` feature or provide schema docs so they can be checked in CI later.

## Commit & Pull Request Guidelines
- Follow the existing Conventional Commit style (`feat: ...`, `fix: ...`).
- Commits should be scoped to a logical change set and include updates to docs/tests when behavior changes.
- PRs should describe the problem, the tuning guidance they implement (cite `docs/1` or `docs/4`), steps to reproduce/verify, and any flags or YAML schema changes.
