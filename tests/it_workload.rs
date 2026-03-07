mod support;

use support::{
    parse_json_output, workload_happy_path_snapshot_view, ContainerProfile, TestPostgres, TestRole,
};

#[test]
#[ignore = "requires Docker"]
fn workload_happy_path_json_snapshot() {
    let server = TestPostgres::start(ContainerProfile::WorkloadEnabled);
    let db = server.create_test_database("workload");
    server.apply_table_index_fixture(&db);
    server.reset_pg_stat_statements(&db);
    server.run_workload_fixture_as_app(&db);

    let assert = server
        .workload_command(&db, TestRole::Admin)
        .assert()
        .success();
    let json = parse_json_output(&assert.get_output().stdout);

    assert_eq!(
        json["workload_metadata"]["data_source"],
        "pg_stat_statements"
    );
    assert_eq!(json["workload_metadata"]["scope"], "cumulative_since_reset");
    assert!(
        json["coverage_stats"].is_object(),
        "expected coverage stats"
    );
    assert_eq!(
        json["workload_metadata"]["query_text_visible"],
        serde_json::Value::Bool(true)
    );

    let candidates = json["query_index_candidates"]
        .as_array()
        .expect("query_index_candidates should be an array");
    assert!(!candidates.is_empty(), "expected workload index candidates");
    assert!(
        candidates.iter().any(|candidate| {
            candidate["confidence"].is_string()
                && candidate["evidence"].is_object()
                && candidate["notes"].is_array()
        }),
        "expected candidate confidence, evidence, and notes"
    );
    assert!(
        candidates.iter().any(|candidate| {
            candidate["notes"].as_array().is_some_and(|notes| {
                notes.iter().any(|note| {
                    note.as_str().is_some_and(|note| {
                        note.contains("sequential scan hotspot") || note.contains("bloat watchlist")
                    })
                })
            })
        }),
        "expected candidate correlation with seeded table/index health findings"
    );
    assert!(
        json["warnings"]
            .as_array()
            .is_some_and(|warnings| warnings.iter().any(|warning| {
                warning
                    .as_str()
                    .is_some_and(|warning| warning.contains("last reset at"))
            })),
        "expected reset-scope warning"
    );

    let snapshot_name = format!("workload_pg{}", server.version_tag());
    insta::assert_json_snapshot!(snapshot_name, workload_happy_path_snapshot_view(&json));
}
