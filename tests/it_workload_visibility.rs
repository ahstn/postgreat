mod support;

use support::{
    parse_json_output, workload_visibility_snapshot_view, ContainerProfile, TestPostgres, TestRole,
};

#[test]
#[ignore = "requires Docker"]
fn workload_visibility_warning_json_snapshot() {
    let server = TestPostgres::start(ContainerProfile::WorkloadEnabled);
    let db = server.create_test_database("workload_visibility");
    server.apply_table_index_fixture(&db);
    server.reset_pg_stat_statements(&db);
    server.run_workload_fixture_as_app(&db);

    let assert = server
        .workload_command(&db, TestRole::Reader)
        .assert()
        .success();
    let json = parse_json_output(&assert.get_output().stdout);

    assert_eq!(
        json["workload_metadata"]["query_text_visible"],
        serde_json::Value::Bool(false)
    );
    assert!(
        json["warnings"]
            .as_array()
            .is_some_and(|warnings| warnings.iter().any(|warning| {
                warning
                    .as_str()
                    .is_some_and(|warning| warning.contains("pg_read_all_stats"))
            })),
        "expected reduced visibility warning"
    );

    let snapshot_name = format!("workload_visibility_pg{}", server.version_tag());
    insta::assert_json_snapshot!(snapshot_name, workload_visibility_snapshot_view(&json));
}
