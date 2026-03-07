mod support;

use support::{
    parse_json_output, workload_dealloc_snapshot_view, ContainerProfile, TestPostgres, TestRole,
};

#[test]
#[ignore = "requires Docker"]
fn workload_deallocation_warning_json_snapshot() {
    let server = TestPostgres::start(ContainerProfile::WorkloadEnabledLowMax);
    let db = server.create_test_database("workload_dealloc");
    server.reset_pg_stat_statements(&db);
    server.generate_distinct_queries_as_app(&db, 256);

    let assert = server
        .workload_command(&db, TestRole::Admin)
        .assert()
        .success();
    let json = parse_json_output(&assert.get_output().stdout);
    let diagnostics = server.pg_stat_statements_diagnostics(&db);

    let entry_deallocations = json["workload_metadata"]["entry_deallocations"]
        .as_i64()
        .expect("entry_deallocations should be reported");
    assert!(
        entry_deallocations > 0,
        "expected statement deallocations; diagnostics: {diagnostics}"
    );
    assert!(
        json["warnings"]
            .as_array()
            .is_some_and(|warnings| warnings.iter().any(|warning| {
                warning
                    .as_str()
                    .is_some_and(|warning| warning.contains("evicted"))
            })),
        "expected deallocation warning"
    );

    let snapshot_name = format!("workload_dealloc_pg{}", server.version_tag());
    insta::assert_json_snapshot!(snapshot_name, workload_dealloc_snapshot_view(&json));
}
