mod support;

use support::{
    parse_json_output, workload_unavailable_snapshot_view, ContainerProfile, TestPostgres, TestRole,
};

#[test]
#[ignore = "requires Docker"]
fn workload_unavailable_scenarios() {
    let enabled_server = TestPostgres::start(ContainerProfile::WorkloadEnabled);
    let no_extension_db = enabled_server.create_test_database("workload_no_extension");
    enabled_server.drop_pg_stat_statements_extension(&no_extension_db);

    let no_extension_assert = enabled_server
        .workload_command(&no_extension_db, TestRole::Admin)
        .assert()
        .success();
    let no_extension_json = parse_json_output(&no_extension_assert.get_output().stdout);

    assert!(
        no_extension_json["warnings"]
            .as_array()
            .is_some_and(|warnings| warnings.iter().any(|warning| {
                warning
                    .as_str()
                    .is_some_and(|warning| warning.contains("not installed"))
            })),
        "expected missing-extension warning"
    );
    assert!(
        no_extension_json["slow_query_groups"]
            .as_array()
            .is_some_and(|groups| groups.is_empty()),
        "expected warning-only workload result when extension is missing"
    );

    let missing_snapshot = format!(
        "workload_unavailable_missing_pg{}",
        enabled_server.version_tag()
    );
    insta::assert_json_snapshot!(
        missing_snapshot,
        workload_unavailable_snapshot_view(&no_extension_json)
    );

    let no_preload_server = TestPostgres::start(ContainerProfile::NoPreload);
    let no_preload_db = no_preload_server.create_test_database("workload_no_preload");

    let no_preload_assert = no_preload_server
        .workload_command(&no_preload_db, TestRole::Admin)
        .assert()
        .success();
    let no_preload_json = parse_json_output(&no_preload_assert.get_output().stdout);

    assert!(
        no_preload_json["warnings"]
            .as_array()
            .is_some_and(|warnings| warnings.iter().any(|warning| {
                warning
                    .as_str()
                    .is_some_and(|warning| warning.contains("not usable"))
            })),
        "expected preload warning"
    );
    assert!(
        no_preload_json["slow_query_groups"]
            .as_array()
            .is_some_and(|groups| groups.is_empty()),
        "expected warning-only workload result when preload is missing"
    );

    let preload_snapshot = format!(
        "workload_unavailable_preload_pg{}",
        no_preload_server.version_tag()
    );
    insta::assert_json_snapshot!(
        preload_snapshot,
        workload_unavailable_snapshot_view(&no_preload_json)
    );
}
