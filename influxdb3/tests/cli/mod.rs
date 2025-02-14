use crate::server::{ConfigProvider, TestServer};
use assert_cmd::cargo::CommandCargoExt;
use assert_cmd::Command as AssertCmd;
use observability_deps::tracing::debug;
use pretty_assertions::assert_eq;
use serde_json::{json, Value};
use std::{
    io::Write,
    process::{Command, Stdio},
    thread,
};
use test_helpers::tempfile::NamedTempFile;
use test_helpers::tempfile::TempDir;
use test_helpers::{assert_contains, assert_not_contains};

#[cfg(feature = "system-py")]
pub const WRITE_REPORTS_PLUGIN_CODE: &str = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    for table_batch in table_batches:
        # Skip if table_name is write_reports
        if table_batch["table_name"] == "write_reports":
            continue

        row_count = len(table_batch["rows"])

        # Double row count if table name matches args table_name
        if args and "double_count_table" in args and table_batch["table_name"] == args["double_count_table"]:
            row_count *= 2

        line = LineBuilder("write_reports")\
            .tag("table_name", table_batch["table_name"])\
            .int64_field("row_count", row_count)
        influxdb3_local.write(line)
"#;

pub fn run(args: &[&str]) -> String {
    let process = Command::cargo_bin("influxdb3")
        .unwrap()
        .args(args)
        .stdout(Stdio::piped())
        .output()
        .unwrap();

    String::from_utf8_lossy(&process.stdout).trim().into()
}

pub fn run_and_err(args: &[&str]) -> String {
    let process = Command::cargo_bin("influxdb3")
        .unwrap()
        .args(args)
        .stderr(Stdio::piped())
        .output()
        .unwrap();

    String::from_utf8_lossy(&process.stderr).trim().into()
}

pub fn run_with_confirmation(args: &[&str]) -> String {
    let mut child_process = Command::cargo_bin("influxdb3")
        .unwrap()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child_process.stdin.take().expect("failed to open stdin");
    thread::spawn(move || {
        stdin
            .write_all(b"yes\n")
            .expect("cannot write confirmation msg to stdin");
    });
    let output = child_process.wait_with_output().unwrap();
    if !output.status.success() {
        panic!(
            "failed to run 'influxdb3 {}' with status {}",
            args.join(" "),
            output.status
        );
    }

    String::from_utf8(output.stdout).unwrap().trim().into()
}

pub fn run_with_confirmation_and_err(args: &[&str]) -> String {
    let mut child_process = Command::cargo_bin("influxdb3")
        .unwrap()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child_process.stdin.take().expect("failed to open stdin");
    thread::spawn(move || {
        stdin
            .write_all(b"yes\n")
            .expect("cannot write confirmation msg to stdin");
    });

    String::from_utf8(child_process.wait_with_output().unwrap().stderr)
        .unwrap()
        .trim()
        .into()
}

pub fn run_with_stdin_input(input: impl Into<String>, args: &[&str]) -> String {
    let input = input.into();
    let mut child_process = Command::cargo_bin("influxdb3")
        .unwrap()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child_process.stdin.take().expect("failed to open stdin");
    thread::spawn(move || {
        stdin
            .write_all(input.as_bytes())
            .expect("cannot write to stdin");
    });

    String::from_utf8(child_process.wait_with_output().unwrap().stdout)
        .unwrap()
        .trim()
        .into()
}

// Helper function to create a temporary Python plugin file
fn create_plugin_file(code: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(file, "{}", code).unwrap();
    file
}

#[test_log::test(tokio::test)]
async fn test_telemetry_disabled_with_debug_msg() {
    let serve_args = &[
        "serve",
        "--node-id",
        "the-best-node",
        "--object-store",
        "memory",
    ];

    let expected_disabled: &str = "Initializing TelemetryStore with upload disabled.";

    // validate we get a debug message indicating upload disabled
    let output = AssertCmd::cargo_bin("influxdb3")
        .unwrap()
        .args(serve_args)
        .arg("-vv")
        .arg("--disable-telemetry-upload")
        .timeout(std::time::Duration::from_millis(500))
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();
    let output = String::from_utf8(output).expect("must be able to convert output to String");
    assert_contains!(output, expected_disabled);
}

#[test_log::test(tokio::test)]
async fn test_telemetry_disabled() {
    let serve_args = &[
        "serve",
        "--node-id",
        "the-best-node",
        "--object-store",
        "memory",
    ];

    let expected_disabled: &str = "Initializing TelemetryStore with upload disabled.";
    // validate no message when debug output disabled
    let output = AssertCmd::cargo_bin("influxdb3")
        .unwrap()
        .args(serve_args)
        .arg("-v")
        .arg("--disable-telemetry-upload")
        .timeout(std::time::Duration::from_millis(500))
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();
    let output = String::from_utf8(output).expect("must be able to convert output to String");
    assert_not_contains!(output, expected_disabled);
}

#[test_log::test(tokio::test)]
async fn test_telemetry_enabled_with_debug_msg() {
    let serve_args = &[
        "serve",
        "--node-id",
        "the-best-node",
        "--object-store",
        "memory",
    ];

    let expected_enabled: &str =
        "Initializing TelemetryStore with upload enabled for http://localhost:9999.";

    // validate debug output shows which endpoint we are hitting when telemetry enabled
    let output = AssertCmd::cargo_bin("influxdb3")
        .unwrap()
        .args(serve_args)
        .arg("-vv")
        .arg("--telemetry-endpoint")
        .arg("http://localhost:9999")
        .timeout(std::time::Duration::from_millis(500))
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();
    let output = String::from_utf8(output).expect("must be able to convert output to String");
    assert_contains!(output, expected_enabled);
}

#[test_log::test(tokio::test)]
async fn test_telementry_enabled() {
    let serve_args = &[
        "serve",
        "--node-id",
        "the-best-node",
        "--object-store",
        "memory",
    ];

    let expected_enabled: &str =
        "Initializing TelemetryStore with upload enabled for http://localhost:9999.";

    // validate no telemetry endpoint reported when debug output not enabled
    let output = AssertCmd::cargo_bin("influxdb3")
        .unwrap()
        .args(serve_args)
        .arg("-v")
        .arg("--telemetry-endpoint")
        .arg("http://localhost:9999")
        .timeout(std::time::Duration::from_millis(500))
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();
    let output = String::from_utf8(output).expect("must be able to convert output to String");
    assert_not_contains!(output, expected_enabled);
}

#[test_log::test(tokio::test)]
async fn test_show_databases() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let output = run_with_confirmation(&["create", "database", "foo", "--host", &server_addr]);
    debug!(output, "create database");
    let output = run_with_confirmation(&["create", "database", "bar", "--host", &server_addr]);
    debug!(output, "create database");
    let output = run(&["show", "databases", "--host", &server_addr]);
    assert_eq!(
        "\
        +---------------+\n\
        | iox::database |\n\
        +---------------+\n\
        | bar           |\n\
        | foo           |\n\
        +---------------+\
        ",
        output
    );
    let output = run(&[
        "show",
        "databases",
        "--host",
        &server_addr,
        "--format",
        "json",
    ]);
    assert_eq!(
        r#"[{"iox::database":"bar"},{"iox::database":"foo"}]"#,
        output
    );
    let output = run(&[
        "show",
        "databases",
        "--host",
        &server_addr,
        "--format",
        "csv",
    ]);
    assert_eq!(
        "\
        iox::database\n\
        bar\n\
        foo\
        ",
        output
    );
    let output = run(&[
        "show",
        "databases",
        "--host",
        &server_addr,
        "--format",
        "jsonl",
    ]);
    assert_eq!(
        "\
        {\"iox::database\":\"bar\"}\n\
        {\"iox::database\":\"foo\"}\
        ",
        output
    );
    run_with_confirmation(&["delete", "database", "foo", "--host", &server_addr]);
    let output = run(&["show", "databases", "--host", &server_addr]);
    assert_eq!(
        "\
        +---------------+\n\
        | iox::database |\n\
        +---------------+\n\
        | bar           |\n\
        +---------------+",
        output
    );
    let output = run(&[
        "show",
        "databases",
        "--host",
        &server_addr,
        "--show-deleted",
    ]);
    // don't assert on actual output since it contains a time stamp which would be flaky
    assert_contains!(output, "foo-");
}

#[test_log::test(tokio::test)]
async fn test_create_database() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let result = run_with_confirmation(&["create", "database", db_name, "--host", &server_addr]);
    debug!(result = ?result, "create database");
    assert_contains!(&result, "Database \"foo\" created successfully");
}

#[test_log::test(tokio::test)]
async fn test_create_database_limit() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    for i in 0..5 {
        let name = format!("{db_name}{i}");
        let result = run_with_confirmation(&["create", "database", &name, "--host", &server_addr]);
        debug!(result = ?result, "create database");
        assert_contains!(&result, format!("Database \"{name}\" created successfully"));
    }

    let result =
        run_with_confirmation_and_err(&["create", "database", "foo5", "--host", &server_addr]);
    debug!(result = ?result, "create database");
    assert_contains!(
        &result,
        "Adding a new database would exceed limit of 5 databases"
    );
}

#[test_log::test(tokio::test)]
async fn test_delete_database() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    server
        .write_lp_to_db(
            db_name,
            "cpu,t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000",
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");
    let result = run_with_confirmation(&["delete", "database", db_name, "--host", &server_addr]);
    debug!(result = ?result, "delete database");
    assert_contains!(&result, "Database \"foo\" deleted successfully");
}

#[test_log::test(tokio::test)]
async fn test_delete_missing_database() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let result =
        run_with_confirmation_and_err(&["delete", "database", db_name, "--host", &server_addr]);
    debug!(result = ?result, "delete missing database");
    assert_contains!(&result, "404");
}

#[test_log::test(tokio::test)]
async fn test_create_table() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let table_name = "bar";
    let result = run_with_confirmation(&["create", "database", db_name, "--host", &server_addr]);
    debug!(result = ?result, "create database");
    assert_contains!(&result, "Database \"foo\" created successfully");
    let result = run_with_confirmation(&[
        "create",
        "table",
        table_name,
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--tags",
        "one",
        "two",
        "three",
        "--fields",
        "four:utf8",
        "five:uint64",
        "six:float64",
        "seven:int64",
        "eight:bool",
    ]);
    debug!(result = ?result, "create table");
    assert_contains!(&result, "Table \"foo\".\"bar\" created successfully");
    // Check that we can query the table and that it has no values
    let result = server
        .api_v3_query_sql(&[
            ("db", "foo"),
            ("q", "SELECT * FROM bar"),
            ("format", "json"),
        ])
        .await
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(result, json!([]));
    server
        .write_lp_to_db(
            db_name,
            format!("{table_name},one=1,two=2,three=3 four=\"4\",five=5u,six=6,seven=7i,eight=true 2998574937"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");
    // Check that we can get data from the table
    let result = server
        .api_v3_query_sql(&[
            ("db", "foo"),
            ("q", "SELECT * FROM bar"),
            ("format", "json"),
        ])
        .await
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(
        result,
        json!([{
            "one": "1",
            "two": "2",
            "three": "3",
            "four": "4",
            "five": 5,
            "six": 6.0,
            "seven": 7,
            "eight": true,
            "time": "2065-01-07T17:28:57"
        }])
    );
}

#[test_log::test(tokio::test)]
async fn test_create_table_fail_existing() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let table_name = "bar";
    let result = run_with_confirmation(&["create", "database", db_name, "--host", &server_addr]);
    debug!(result = ?result, "create database");
    assert_contains!(&result, "Database \"foo\" created successfully");
    let result = run_with_confirmation(&[
        "create",
        "table",
        table_name,
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--tags",
        "one",
        "--fields",
        "four:utf8",
    ]);
    debug!(result = ?result, "create table");
    assert_contains!(&result, "Table \"foo\".\"bar\" created successfully");

    let result = run_with_confirmation_and_err(&[
        "create",
        "table",
        table_name,
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--tags",
        "one",
        "--fields",
        "four:utf8",
    ]);

    insta::assert_snapshot!("test_create_table_fail_existing", result);
}

#[test_log::test(tokio::test)]
async fn test_delete_table() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let table_name = "cpu";
    server
        .write_lp_to_db(
            db_name,
            format!("{table_name},t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");
    let result = run_with_confirmation(&[
        "delete",
        "table",
        table_name,
        "--database",
        db_name,
        "--host",
        &server_addr,
    ]);
    debug!(result = ?result, "delete table");
    assert_contains!(&result, "Table \"foo\".\"cpu\" deleted successfully");
}

#[test_log::test(tokio::test)]
async fn test_delete_missing_table() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let table_name = "mem";
    server
        .write_lp_to_db(
            db_name,
            format!("{table_name},t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    let result = run_with_confirmation_and_err(&[
        "delete",
        "table",
        "cpu",
        "--database",
        db_name,
        "--host",
        &server_addr,
    ]);
    debug!(result = ?result, "delete missing table");
    assert_contains!(&result, "404");
}

#[tokio::test]
async fn test_create_delete_distinct_cache() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let table_name = "bar";
    server
        .write_lp_to_db(
            db_name,
            format!("{table_name},t1=a,t2=aa,t3=aaa f1=true,f2=\"hello\",f3=42"),
            influxdb3_client::Precision::Second,
        )
        .await
        .unwrap();
    let cache_name = "baz";
    // first create the cache:
    let result = run(&[
        "create",
        "distinct_cache",
        "--host",
        &server_addr,
        "--database",
        db_name,
        "--table",
        table_name,
        "--columns",
        "t1,t2",
        cache_name,
    ]);
    assert_contains!(&result, "new cache created");
    // doing the same thing over again will be a no-op
    let result = run(&[
        "create",
        "distinct_cache",
        "--host",
        &server_addr,
        "--database",
        db_name,
        "--table",
        table_name,
        "--columns",
        "t1,t2",
        cache_name,
    ]);
    assert_contains!(
        &result,
        "a cache already exists for the provided parameters"
    );
    // now delete it:
    let result = run(&[
        "delete",
        "distinct_cache",
        "--host",
        &server_addr,
        "--database",
        db_name,
        "--table",
        table_name,
        cache_name,
    ]);
    assert_contains!(&result, "distinct cache deleted successfully");
    // trying to delete again should result in an error as the cache no longer exists:
    let result = run_and_err(&[
        "delete",
        "distinct_cache",
        "--host",
        &server_addr,
        "--database",
        db_name,
        "--table",
        table_name,
        cache_name,
    ]);
    assert_contains!(&result, "[404 Not Found]: cache not found");
}

#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_create_trigger_and_run() {
    // create a plugin and trigger and write data in, verifying that the trigger is enabled
    // and sent data
    let plugin_file = create_plugin_file(WRITE_REPORTS_PLUGIN_CODE);
    let plugin_dir = plugin_file.path().parent().unwrap().to_str().unwrap();
    let plugin_filename = plugin_file.path().file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let trigger_name = "test_trigger";

    // Setup: create database and plugin
    run_with_confirmation(&["create", "database", "--host", &server_addr, db_name]);

    // creating the trigger should enable it
    let result = run_with_confirmation(&[
        "create",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--plugin-filename",
        plugin_filename,
        "--trigger-spec",
        "all_tables",
        "--trigger-arguments",
        "double_count_table=cpu",
        trigger_name,
    ]);
    debug!(result = ?result, "create trigger");
    assert_contains!(&result, "Trigger test_trigger created successfully");

    // now let's write data and see if it gets processed
    server
        .write_lp_to_db(
            db_name,
            "cpu,host=a f1=1.0\ncpu,host=b f1=2.0\nmem,host=a usage=234",
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    let expected = json!(
        [
            {"table_name": "cpu", "row_count": 4},
            {"table_name": "mem", "row_count": 1}
        ]
    );

    // query to see if the processed data is there. we loop because it could take a bit to write
    // back the data. There's also a condition where the table may have been created, but the
    // write hasn't happened yet, which returns empty results. This ensures we don't hit that race.
    let mut check_count = 0;
    loop {
        match server
            .api_v3_query_sql(&[
                ("db", db_name),
                ("q", "SELECT table_name, row_count FROM write_reports"),
                ("format", "json"),
            ])
            .await
            .json::<Value>()
            .await
        {
            Ok(value) => {
                if value == expected {
                    return;
                }
                check_count += 1;
                if check_count > 10 {
                    panic!(
                        "Unexpected query result, got: {:#?}, expected {:#?}",
                        value, expected
                    );
                }
            }
            Err(e) => {
                check_count += 1;
                if check_count > 10 {
                    panic!("Failed to query processed data: {}", e);
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        };
    }
}

#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_triggers_are_started() {
    // create a plugin and trigger and write data in, verifying that the trigger is enabled
    // and sent data
    let plugin_file = create_plugin_file(WRITE_REPORTS_PLUGIN_CODE);
    let plugin_dir = plugin_file.path().parent().unwrap().to_str().unwrap();
    let plugin_filename = plugin_file.path().file_name().unwrap().to_str().unwrap();

    // create tmp dir for object store
    let tmp_file = TempDir::new().unwrap();
    let tmp_dir = tmp_file.path().to_str().unwrap();

    let mut server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .with_object_store_dir(tmp_dir)
        .spawn()
        .await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let trigger_name = "test_trigger";

    // Setup: create database and plugin
    run_with_confirmation(&["create", "database", "--host", &server_addr, db_name]);

    // creating the trigger should enable it
    let result = run_with_confirmation(&[
        "create",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--plugin-filename",
        plugin_filename,
        "--trigger-spec",
        "all_tables",
        "--trigger-arguments",
        "double_count_table=cpu",
        trigger_name,
    ]);
    debug!(result = ?result, "create trigger");
    assert_contains!(&result, "Trigger test_trigger created successfully");

    // restart the server
    server.kill();

    server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .with_object_store_dir(tmp_dir)
        .spawn()
        .await;

    // now let's write data and see if it gets processed
    server
        .write_lp_to_db(
            db_name,
            "cpu,host=a f1=1.0\ncpu,host=b f1=2.0\nmem,host=a usage=234",
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    let expected = json!(
        [
            {"table_name": "cpu", "row_count": 4},
            {"table_name": "mem", "row_count": 1}
        ]
    );

    // query to see if the processed data is there. we loop because it could take a bit to write
    // back the data. There's also a condition where the table may have been created, but the
    // write hasn't happened yet, which returns empty results. This ensures we don't hit that race.
    let mut check_count = 0;
    loop {
        match server
            .api_v3_query_sql(&[
                ("db", db_name),
                ("q", "SELECT table_name, row_count FROM write_reports"),
                ("format", "json"),
            ])
            .await
            .json::<Value>()
            .await
        {
            Ok(value) => {
                if value == expected {
                    return;
                }
                check_count += 1;
                if check_count > 10 {
                    panic!(
                        "Unexpected query result, got: {:#?}, expected {:#?}",
                        value, expected
                    );
                }
            }
            Err(e) => {
                check_count += 1;
                if check_count > 10 {
                    panic!("Failed to query processed data: {}", e);
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        };
    }
}

#[test_log::test(tokio::test)]
async fn test_database_create_persists() {
    // create tmp dir for object store
    let tmp_file = TempDir::new().unwrap();
    let tmp_dir = tmp_file.path().to_str().unwrap();

    let mut server = TestServer::configure()
        .with_object_store_dir(tmp_dir)
        .spawn()
        .await;

    let server_addr = server.client_addr();
    let db_name = "foo";
    let result = run_with_confirmation(&["create", "database", db_name, "--host", &server_addr]);
    debug!(result = ?result, "create database");
    assert_contains!(&result, "Database \"foo\" created successfully");

    // restart the server
    server.kill();

    server = TestServer::configure()
        .with_object_store_dir(tmp_dir)
        .spawn()
        .await;

    let server_addr = server.client_addr();

    let result = run_with_confirmation(&["show", "databases", "--host", &server_addr]);
    assert_eq!(
        r#"+---------------+
| iox::database |
+---------------+
| foo           |
+---------------+"#,
        result
    );
}

#[test_log::test(tokio::test)]
async fn test_trigger_enable() {
    let plugin_file = create_plugin_file(
        r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("done")
"#,
    );
    let plugin_dir = plugin_file.path().parent().unwrap().to_str().unwrap();
    let plugin_filename = plugin_file.path().file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let trigger_name = "test_trigger";

    // Setup: create database, plugin, and trigger
    run_with_confirmation(&["create", "database", "--host", &server_addr, db_name]);

    run_with_confirmation(&[
        "create",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--plugin-filename",
        plugin_filename,
        "--trigger-spec",
        "all_tables",
        trigger_name,
    ]);

    // Test enabling
    let result = run_with_confirmation(&[
        "enable",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        trigger_name,
    ]);
    debug!(result = ?result, "enable trigger");
    assert_contains!(&result, "Trigger test_trigger enabled successfully");

    // Test disable
    let result = run_with_confirmation(&[
        "disable",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        trigger_name,
    ]);
    debug!(result = ?result, "disable trigger");
    assert_contains!(&result, "Trigger test_trigger disabled successfully");
}

#[test_log::test(tokio::test)]
async fn test_delete_enabled_trigger() {
    let plugin_file = create_plugin_file(
        r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("done")
"#,
    );
    let plugin_dir = plugin_file.path().parent().unwrap().to_str().unwrap();
    let plugin_filename = plugin_file.path().file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let trigger_name = "test_trigger";

    // Setup: create database, plugin, and enable trigger
    run_with_confirmation(&["create", "database", "--host", &server_addr, db_name]);

    run_with_confirmation(&[
        "create",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--plugin-filename",
        plugin_filename,
        "--trigger-spec",
        "all_tables",
        trigger_name,
    ]);

    // Try to delete the enabled trigger without force flag
    let result = run_with_confirmation_and_err(&[
        "delete",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        trigger_name,
    ]);
    debug!(result = ?result, "delete enabled trigger without force");
    assert_contains!(&result, "command failed");

    // Delete active trigger with force flag
    let result = run_with_confirmation(&[
        "delete",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        trigger_name,
        "--force",
    ]);
    debug!(result = ?result, "delete enabled trigger with force");
    assert_contains!(&result, "Trigger test_trigger deleted successfully");
}

#[test_log::test(tokio::test)]
async fn test_table_specific_trigger() {
    let plugin_file = create_plugin_file(
        r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("done")
"#,
    );
    let plugin_dir = plugin_file.path().parent().unwrap().to_str().unwrap();
    let plugin_filename = plugin_file.path().file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let table_name = "bar";
    let trigger_name = "test_trigger";

    // Setup: create database, table, and plugin
    run_with_confirmation(&["create", "database", "--host", &server_addr, db_name]);

    run_with_confirmation(&[
        "create",
        "table",
        table_name,
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--tags",
        "tag1",
        "--fields",
        "field1:float64",
    ]);

    // Create table-specific trigger
    let result = run_with_confirmation(&[
        "create",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--plugin-filename",
        plugin_filename,
        "--trigger-spec",
        &format!("table:{}", table_name),
        trigger_name,
    ]);
    debug!(result = ?result, "create table-specific trigger");
    assert_contains!(&result, "Trigger test_trigger created successfully");
}

#[test]
fn test_create_token() {
    let result = run_with_confirmation(&["create", "token"]);
    assert_contains!(
        &result,
        "This will grant you access to every HTTP endpoint or deny it otherwise"
    );
}

#[test_log::test(tokio::test)]
async fn test_show_system() {
    let server = TestServer::configure().spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";

    server
        .write_lp_to_db(
            db_name,
            "cpu,t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000",
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    struct SuccessTestCase<'a> {
        name: &'static str,
        args: Vec<&'a str>,
    }

    let cases = vec![
        SuccessTestCase {
            name: "summary should show up to ten entries from each table",
            args: vec![
                "show",
                "system",
                "--host",
                server_addr.as_str(),
                "--database",
                db_name,
                "summary",
            ],
        },
        SuccessTestCase {
            name:
                "table NAME should show queries table without information or system schema queries",
            args: vec![
                "show",
                "system",
                "--host",
                server_addr.as_str(),
                "--database",
                db_name,
                "table",
                "queries",
            ],
        },
        SuccessTestCase {
            name: "table-list should list system schema tables only",
            args: vec![
                "show",
                "system",
                "--host",
                server_addr.as_str(),
                "--database",
                db_name,
                "table-list",
            ],
        },
    ];

    for case in cases {
        let output = run(&case.args);
        let snap_name = case.name.replace(' ', "_");
        insta::assert_snapshot!(snap_name, output);
    }

    struct FailTestCase<'a> {
        name: &'static str,
        args: Vec<&'a str>,
    }

    let cases = vec![
        FailTestCase {
            name: "fail without database name",
            args: vec!["show", "system", "table-list"],
        },
        FailTestCase {
            name: "random table name doesn't exist, should error",
            args: vec!["show", "system", "--host", server_addr.as_str(), "--database", db_name, "table", "meow"],
        },
        FailTestCase {
            name: "iox schema table name exists, but should error because we're concerned here with system tables",
            args: vec!["show", "system", "--host", server_addr.as_str(), "--database", db_name, "table", "cpu"],
        },
        FailTestCase {
            name: "fail without output-file when format is parquet for table",
            args: vec!["show", "system", "--host", server_addr.as_str(), "--database", db_name, "table", "--format", "parquet","distinct_caches"]
        },
        FailTestCase {
            name: "fail without output-file when format is parquet for table-list",
            args: vec!["show", "system", "--host", server_addr.as_str(), "--database", db_name, "table-list", "--format", "parquet"]
        },
        FailTestCase {
            name: "fail without output-file when format is parquet for summary",
            args: vec!["show", "system", "--host", server_addr.as_str(), "--database", db_name, "summary", "--format", "parquet"]
        },
    ];

    for case in cases {
        let output = run_and_err(&case.args);
        let snap_name = case.name.replace(' ', "_");
        insta::assert_snapshot!(snap_name, output);
    }
}

#[tokio::test]
async fn distinct_cache_create_and_delete() {
    let server = TestServer::spawn().await;
    let db_name = "foo";
    let server_addr = server.client_addr();
    server
        .write_lp_to_db(
            db_name,
            "cpu,t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000",
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    let result = run_with_confirmation(&[
        "create",
        "distinct_cache",
        "-H",
        &server_addr,
        "-d",
        db_name,
        "-t",
        "cpu",
        "--columns",
        "t1,t2",
        "--max-cardinality",
        "20000",
        "--max-age",
        "200s",
        "cache_money",
    ]);

    insta::assert_yaml_snapshot!(result);

    let result = run_with_confirmation(&[
        "delete",
        "distinct_cache",
        "-H",
        &server_addr,
        "-d",
        db_name,
        "-t",
        "cpu",
        "cache_money",
    ]);

    insta::assert_yaml_snapshot!(result);
}

#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_wal_plugin_test() {
    use crate::server::ConfigProvider;
    use influxdb3_client::Precision;

    // Create plugin file
    let plugin_file = create_plugin_file(
        r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("arg1: " + args["arg1"])

    query_params = {"host": args["host"]}
    query_result = influxdb3_local.query("SELECT host, region, usage FROM cpu where host = $host", query_params)
    influxdb3_local.info("query result: " + str(query_result))
    influxdb3_local.info("i", query_result, args["host"])
    influxdb3_local.warn("w:", query_result)
    influxdb3_local.error("err", query_result)

    for table_batch in table_batches:
        influxdb3_local.info("table: " + table_batch["table_name"])

        for row in table_batch["rows"]:
            influxdb3_local.info("row: " + str(row))

    line = LineBuilder("some_table")\
        .tag("tag1", "tag1_value")\
        .tag("tag2", "tag2_value")\
        .int64_field("field1", 1)\
        .float64_field("field2", 2.0)\
        .string_field("field3", "number three")
    influxdb3_local.write(line)

    other_line = LineBuilder("other_table")
    other_line.int64_field("other_field", 1)
    other_line.float64_field("other_field2", 3.14)
    other_line.time_ns(1302)

    influxdb3_local.write_to_db("mytestdb", other_line)

    influxdb3_local.info("done")"#,
    );

    let plugin_dir = plugin_file.path().parent().unwrap().to_str().unwrap();
    let plugin_name = plugin_file.path().file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let server_addr = server.client_addr();

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=s1,region=us-east usage=0.9\n\
            cpu,host=s2,region=us-east usage=0.89\n\
            cpu,host=s1,region=us-east usage=0.85",
            Precision::Nanosecond,
        )
        .await
        .unwrap();

    let db_name = "foo";

    // Run the test
    let result = run_with_confirmation(&[
        "test",
        "wal_plugin",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--lp",
        "test_input,tag1=tag1_value,tag2=tag2_value field1=1i 500",
        "--input-arguments",
        "arg1=arg1_value,host=s2",
        plugin_name,
    ]);
    debug!(result = ?result, "test wal plugin");

    let res = serde_json::from_str::<serde_json::Value>(&result).unwrap();

    let expected_result = r#"{
  "log_lines": [
    "INFO: arg1: arg1_value",
    "INFO: query result: [{'host': 's2', 'region': 'us-east', 'usage': 0.89}]",
    "INFO: i [{'host': 's2', 'region': 'us-east', 'usage': 0.89}] s2",
    "WARN: w: [{'host': 's2', 'region': 'us-east', 'usage': 0.89}]",
    "ERROR: err [{'host': 's2', 'region': 'us-east', 'usage': 0.89}]",
    "INFO: table: test_input",
    "INFO: row: {'tag1': 'tag1_value', 'tag2': 'tag2_value', 'field1': 1, 'time': 500}",
    "INFO: done"
  ],
  "database_writes": {
    "mytestdb": [
      "other_table other_field=1i,other_field2=3.14 1302"
    ],
    "foo": [
      "some_table,tag1=tag1_value,tag2=tag2_value field1=1i,field2=2.0,field3=\"number three\""
    ]
  },
  "errors": []
}"#;
    let expected_result = serde_json::from_str::<serde_json::Value>(expected_result).unwrap();
    assert_eq!(res, expected_result);
}
#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_schedule_plugin_test() {
    use crate::server::ConfigProvider;
    use influxdb3_client::Precision;

    // Create plugin file with a scheduled task
    let plugin_file = create_plugin_file(
        r#"
def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    influxdb3_local.info(f"args are {args}")
    influxdb3_local.info("Successfully called")"#,
    );

    let plugin_dir = plugin_file.path().parent().unwrap().to_str().unwrap();
    let plugin_name = plugin_file.path().file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let server_addr = server.client_addr();

    // Write some test data
    server
        .write_lp_to_db(
            "foo",
            "cpu,host=host1,region=us-east usage=0.75\n\
             cpu,host=host2,region=us-west usage=0.82\n\
             cpu,host=host3,region=us-east usage=0.91",
            Precision::Nanosecond,
        )
        .await
        .unwrap();

    let db_name = "foo";

    // Run the schedule plugin test
    let result = run_with_confirmation(&[
        "test",
        "schedule_plugin",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--schedule",
        "*/5 * * * * *", // Run every 5 seconds
        "--input-arguments",
        "region=us-east",
        plugin_name,
    ]);
    debug!(result = ?result, "test schedule plugin");

    let res = serde_json::from_str::<Value>(&result).unwrap();

    // The trigger_time will be dynamic, so we'll just verify it exists and is in the right format
    let trigger_time = res["trigger_time"].as_str().unwrap();
    assert!(trigger_time.contains('T')); // Basic RFC3339 format check

    // Check the rest of the response structure
    let expected_result = serde_json::json!({
        "log_lines": [
            "INFO: args are {'region': 'us-east'}",
            "INFO: Successfully called"
        ],
        "database_writes": {
        },
        "errors": []
    });
    assert_eq!(res["log_lines"], expected_result["log_lines"]);
    assert_eq!(res["database_writes"], expected_result["database_writes"]);
    assert_eq!(res["errors"], expected_result["errors"]);
}

#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_wal_plugin_errors() {
    use crate::server::ConfigProvider;
    use influxdb3_client::Precision;

    struct Test {
        name: &'static str,
        plugin_code: &'static str,
        expected_error: &'static str,
    }

    let  tests = vec![
        Test {
            name: "invalid_python",
            plugin_code: r#"
        lkjasdf9823
        jjjjj / sss"#,
            expected_error: "error executing plugin: IndentationError: unexpected indent (<string>, line 2)",
        },
        Test {
            name: "no_process_writes",
            plugin_code: r#"
def not_process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("done")"#,
            expected_error: "error executing plugin: the process_writes function is not present in the plugin. Should be defined as: process_writes(influxdb3_local, table_batches, args=None)",
        },
        Test {
            name: "no_args",
            plugin_code: r#"
def process_writes(influxdb3_local, table_batches):
    influxdb3_local.info("done")
"#,
            expected_error: "error executing plugin: TypeError: process_writes() takes 2 positional arguments but 3 were given",
        },
        Test {
            name: "no_table_batches",
            plugin_code: r#"
def process_writes(influxdb3_local, args=None):
    influxdb3_local.info("done")
"#,
            expected_error: "error executing plugin: TypeError: process_writes() takes from 1 to 2 positional arguments but 3 were given",
        },
        Test {
            name: "no_influxdb3_local",
            plugin_code: r#"
def process_writes(table_batches, args=None):
    influxdb3_local.info("done")
"#,
            expected_error: "error executing plugin: TypeError: process_writes() takes from 1 to 2 positional arguments but 3 were given",
        },
        Test {
            name: "line_builder_no_field",
            plugin_code: r#"
def process_writes(influxdb3_local, table_batches, args=None):
    line = LineBuilder("some_table")
    influxdb3_local.write(line)
"#,
            expected_error: "error executing plugin: InvalidLineError: At least one field is required: some_table",
        },
        Test {
            name: "query_no_table",
            plugin_code: r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.query("SELECT foo FROM not_here")
"#,
            expected_error: "error executing plugin: QueryError: error: error while planning query: Error during planning: table 'public.iox.not_here' not found executing query: SELECT foo FROM not_here",
        }
    ];

    let plugin_dir = TempDir::new().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir.path().to_str().unwrap())
        .spawn()
        .await;
    let server_addr = server.client_addr();

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=s1,region=us-east usage=0.9\n\
            cpu,host=s2,region=us-east usage=0.89\n\
            cpu,host=s1,region=us-east usage=0.85",
            Precision::Nanosecond,
        )
        .await
        .unwrap();

    let db_name = "foo";

    for test in tests {
        let mut plugin_file = NamedTempFile::new_in(plugin_dir.path()).unwrap();
        writeln!(plugin_file, "{}", test.plugin_code).unwrap();
        let plugin_name = plugin_file.path().file_name().unwrap().to_str().unwrap();

        let result = run_with_confirmation(&[
            "test",
            "wal_plugin",
            "--database",
            db_name,
            "--host",
            &server_addr,
            "--lp",
            "test_input,tag1=tag1_value,tag2=tag2_value field1=1i 500",
            "--input-arguments",
            "arg1=arg1_value,host=s2",
            plugin_name,
        ]);
        debug!(result = ?result, "test wal plugin");

        println!("{}", result);
        let res = serde_json::from_str::<serde_json::Value>(&result).unwrap();
        let errors = res.get("errors").unwrap().as_array().unwrap();
        let error = errors[0].as_str().unwrap();
        assert_eq!(
            error, test.expected_error,
            "test: {}, response was: {}",
            test.name, result
        );
    }
}

#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_load_wal_plugin_from_gh() {
    use crate::server::ConfigProvider;
    use influxdb3_client::Precision;

    let plugin_dir = TempDir::new().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir.path().to_str().unwrap())
        .spawn()
        .await;
    let server_addr = server.client_addr();

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=s1,region=us-east usage=0.9\n\
            cpu,host=s2,region=us-east usage=0.89\n\
            cpu,host=s1,region=us-east usage=0.85",
            Precision::Nanosecond,
        )
        .await
        .unwrap();

    let db_name = "foo";

    // this will pull from https://github.com/influxdata/influxdb3_plugins/blob/main/examples/wal_plugin/wal_plugin.py
    let plugin_name = "gh:examples/wal_plugin/wal_plugin.py";

    // Run the test to make sure it'll load from GH
    let result = run_with_confirmation(&[
        "test",
        "wal_plugin",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--lp",
        "test_input,tag1=tag1_value,tag2=tag2_value field1=1i 500",
        plugin_name,
    ]);
    debug!(result = ?result, "test wal plugin");

    let res = serde_json::from_str::<serde_json::Value>(&result).unwrap();

    let expected_result = r#"{
  "log_lines": [
    "INFO: wal_plugin.py done"
  ],
  "database_writes": {
    "foo": [
      "write_reports,table_name=test_input row_count=1i"
    ]
  },
  "errors": []
}"#;
    let expected_result = serde_json::from_str::<serde_json::Value>(expected_result).unwrap();
    assert_eq!(res, expected_result);
}

#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_request_plugin_and_trigger() {
    let plugin_code = r#"
import json

def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    for k, v in query_parameters.items():
        influxdb3_local.info(f"query_parameters: {k}={v}")
    for k, v in request_headers.items():
        influxdb3_local.info(f"request_headers: {k}={v}")

    request_data = json.loads(request_body)

    influxdb3_local.info("parsed JSON request body:", request_data)

    # write the data to the database
    line = LineBuilder("request_data").tag("tag1", "tag1_value").int64_field("field1", 1)
    # get a string of the line to return as the body
    line_str = line.build()

    influxdb3_local.write(line)

    return 200, {"Content-Type": "application/json"}, json.dumps({"status": "ok", "line": line_str})
"#;
    let plugin_file = create_plugin_file(plugin_code);
    let plugin_dir = plugin_file.path().parent().unwrap().to_str().unwrap();
    let plugin_filename = plugin_file.path().file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let server_addr = server.client_addr();
    let db_name = "foo";

    // Setup: create database and plugin
    run_with_confirmation(&["create", "database", "--host", &server_addr, db_name]);

    let trigger_path = "foo";
    // creating the trigger should enable it
    let result = run_with_confirmation(&[
        "create",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--plugin-filename",
        plugin_filename,
        "--trigger-spec",
        "request:bar",
        "--trigger-arguments",
        "test_arg=hello",
        trigger_path,
    ]);
    debug!(result = ?result, "create trigger");
    assert_contains!(&result, "Trigger foo created successfully");

    // send an HTTP request to the server
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/v3/engine/bar", server_addr))
        .header("Content-Type", "application/json")
        .query(&[("q1", "whatevs")])
        .body(r#"{"hello": "world"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    let body = serde_json::from_str::<serde_json::Value>(&body).unwrap();

    // the plugin was just supposed to return the line that it wrote into the DB
    assert_eq!(
        body,
        json!({"status": "ok", "line": "request_data,tag1=tag1_value field1=1i"})
    );

    // query to see if the plugin did the write into the DB
    let val = server
        .api_v3_query_sql(&[
            ("db", db_name),
            ("q", "SELECT tag1, field1 FROM request_data"),
            ("format", "json"),
        ])
        .await
        .json::<Value>()
        .await
        .unwrap();

    assert_eq!(val, json!([{"tag1": "tag1_value", "field1": 1}]));

    // now update it to make sure that it reloads
    let plugin_code = r#"
import json

def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    return 200, {"Content-Type": "application/json"}, json.dumps({"status": "updated"})
"#;
    // clear all bytes from the plugin file
    plugin_file.reopen().unwrap().set_len(0).unwrap();
    plugin_file
        .reopen()
        .unwrap()
        .write_all(plugin_code.as_bytes())
        .unwrap();

    // send an HTTP request to the server
    let response = client
        .post(format!("{}/api/v3/engine/bar", server_addr))
        .header("Content-Type", "application/json")
        .query(&[("q1", "whatevs")])
        .body(r#"{"hello": "world"}"#)
        .send()
        .await
        .unwrap();

    let body = response.text().await.unwrap();
    let body = serde_json::from_str::<serde_json::Value>(&body).unwrap();
    assert_eq!(body, json!({"status": "updated"}));
}

#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_trigger_create_validates_file_present() {
    let plugin_dir = TempDir::new().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir.path().to_str().unwrap())
        .spawn()
        .await;
    let server_addr = server.client_addr();
    let db_name = "foo";

    // Setup: create database and plugin
    run_with_confirmation(&["create", "database", "--host", &server_addr, db_name]);

    let trigger_path = "foo";
    // creating the trigger should return a 404 error from github
    let result = run_with_confirmation_and_err(&[
        "create",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--plugin-filename",
        "gh:not_a_file.py",
        "--trigger-spec",
        "request:foo",
        "--trigger-arguments",
        "test_arg=hello",
        trigger_path,
    ]);
    debug!(result = ?result, "create trigger");
    assert_contains!(&result, "error reading file from Github: 404 Not Found");
}

#[test_log::test(tokio::test)]
async fn write_and_query_via_stdin() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let result = run_with_stdin_input(
        "bar,tag1=1,tag2=2 field1=1,field2=2 0",
        &["write", "--database", db_name, "--host", &server_addr],
    );
    assert_eq!("success", result);
    debug!(result = ?result, "wrote data to database");
    let result = run_with_stdin_input(
        "SELECT * FROM bar",
        &["query", "--database", db_name, "--host", &server_addr],
    );
    debug!(result = ?result, "queried data to database");
    assert_eq!(
        [
            "+--------+--------+------+------+---------------------+",
            "| field1 | field2 | tag1 | tag2 | time                |",
            "+--------+--------+------+------+---------------------+",
            "| 1.0    | 2.0    | 1    | 2    | 1970-01-01T00:00:00 |",
            "+--------+--------+------+------+---------------------+",
        ]
        .join("\n"),
        result
    );
}

#[test_log::test(tokio::test)]
async fn write_and_query_via_file() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let result = run(&[
        "write",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--file",
        "tests/server/fixtures/file.lp",
    ]);
    assert_eq!("success", result);
    debug!(result = ?result, "wrote data to database");
    let result = run(&[
        "query",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--file",
        "tests/server/fixtures/file.sql",
    ]);
    debug!(result = ?result, "queried data to database");
    assert_eq!(
        [
            "+--------+--------+------+------+---------------------+",
            "| field1 | field2 | tag1 | tag2 | time                |",
            "+--------+--------+------+------+---------------------+",
            "| 1.0    | 2.0    | 1    | 2    | 1970-01-01T00:00:00 |",
            "+--------+--------+------+------+---------------------+",
        ]
        .join("\n"),
        result
    );
}
#[test_log::test(tokio::test)]
async fn write_and_query_via_string() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let result = run(&[
        "write",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "bar,tag1=1,tag2=2 field1=1,field2=2 0",
    ]);
    assert_eq!("success", result);
    debug!(result = ?result, "wrote data to database");
    let result = run(&[
        "query",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "SELECT * FROM bar",
    ]);
    debug!(result = ?result, "queried data to database");
    assert_eq!(
        [
            "+--------+--------+------+------+---------------------+",
            "| field1 | field2 | tag1 | tag2 | time                |",
            "+--------+--------+------+------+---------------------+",
            "| 1.0    | 2.0    | 1    | 2    | 1970-01-01T00:00:00 |",
            "+--------+--------+------+------+---------------------+",
        ]
        .join("\n"),
        result
    );
}

#[test_log::test(tokio::test)]
async fn write_with_precision_arg() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let table_name = "bar";

    struct TestCase {
        name: &'static str,
        precision: Option<&'static str>,
        expected: &'static str,
    }

    let tests = vec![
        TestCase {
            name: "default precision is seconds",
            precision: None,
            expected: "1970-01-01T00:00:01",
        },
        TestCase {
            name: "set seconds precision",
            precision: Some("s"),
            expected: "1970-01-01T00:00:01",
        },
        TestCase {
            name: "set milliseconds precision",
            precision: Some("ms"),
            expected: "1970-01-01T00:00:00.001",
        },
        TestCase {
            name: "set microseconds precision",
            precision: Some("us"),
            expected: "1970-01-01T00:00:00.000001",
        },
        TestCase {
            name: "set nanoseconds precision",
            precision: Some("ns"),
            expected: "1970-01-01T00:00:00.000000001",
        },
    ];

    for test in tests {
        let TestCase {
            name,
            precision,
            expected,
        } = test;
        let name_tag = name.replace(' ', "_");
        let lp = format!("{table_name},name={name_tag} theanswer=42 1");

        let mut args = vec!["write", "--host", &server_addr, "--database", db_name];
        if let Some(precision) = precision {
            args.extend(vec!["--precision", precision]);
        }
        args.push(&lp);

        run(args.as_slice());
        let result = server
            .api_v3_query_sql(&[
                ("db", db_name),
                (
                    "q",
                    format!("SELECT * FROM {table_name} WHERE name='{name_tag}'").as_str(),
                ),
                ("format", "json"),
            ])
            .await
            .json::<Value>()
            .await
            .unwrap();
        assert_eq!(
            result,
            json!([{
                "name": name_tag,
                "theanswer": 42.0,
                "time": expected,
            }]),
            "test failed: {name}",
        );
    }

    let lp = format!("{table_name},name=invalid_precision theanswer=42 1");
    let output = run_and_err(&[
        "write",
        "--host",
        &server_addr,
        "--database",
        db_name,
        "--precision",
        "fake",
        &lp,
    ]);
    insta::assert_snapshot!("invalid_precision", output);
}
