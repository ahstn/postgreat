mod support;

use support::{analyze_snapshot_view, parse_json_output, ContainerProfile, TestPostgres, TestRole};

#[test]
#[ignore = "requires Docker"]
fn analyze_happy_path_json_snapshot() {
    let server = TestPostgres::start(ContainerProfile::WorkloadEnabled);
    let db = server.create_test_database("analyze");
    server.apply_table_index_fixture(&db);

    let assert = server
        .analyze_command(&db, TestRole::Admin)
        .assert()
        .success();
    let json = parse_json_output(&assert.get_output().stdout);

    assert!(
        json["bloat_info"]
            .as_array()
            .is_some_and(|entries| !entries.is_empty()),
        "expected seeded bloat findings"
    );
    assert!(
        json["seq_scan_info"]
            .as_array()
            .is_some_and(|entries| !entries.is_empty()),
        "expected seeded sequential scan findings"
    );
    assert!(
        json["index_usage_info"]
            .as_array()
            .is_some_and(|entries| !entries.is_empty()),
        "expected seeded index findings"
    );

    let snapshot_name = format!("analyze_pg{}", server.version_tag());
    insta::assert_json_snapshot!(snapshot_name, analyze_snapshot_view(&json));
}

#[test]
#[ignore = "requires Docker"]
fn analyze_accepts_passwords_with_url_reserved_characters() {
    let server = TestPostgres::start(ContainerProfile::WorkloadEnabled);
    let db = server.create_test_database("special_password");
    server.apply_table_index_fixture(&db);

    let role_name = format!("reader_{}", db.name());
    let password = "reader:p@ss/word";
    server.create_readonly_role(&db, &role_name, password);

    server
        .analyze_command_with_credentials(&db, &role_name, password)
        .assert()
        .success();
}
