use loom::policy::apply_runtime_policy;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread")]
async fn allow_all_policy_allows_imports_without_explicit_path_lists() {
    let script_dir = tempdir().expect("script dir");
    let external_dir = tempdir().expect("external dir");

    let module_path = external_dir.path().join("logic.loom");
    std::fs::write(&module_path, "value() => 1").expect("write module");

    let policy_path = script_dir.path().join(".loomrc.json");
    std::fs::write(
        &policy_path,
        r#"{"version":1,"trust_mode":"trusted","allow_all":true}"#,
    )
    .expect("write policy");

    let source = format!("@import \"{}\" as lib", module_path.to_string_lossy());
    let program = loom::parser::parse(&source).expect("parse program");

    let mut runtime =
        loom::runtime::Runtime::new().with_script_dir(script_dir.path().to_str().expect("path"));
    apply_runtime_policy(&mut runtime, Some(policy_path.as_path()), None)
        .expect("apply policy should succeed");

    runtime
        .execute(&program)
        .await
        .expect("import should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn wildcard_path_lists_allow_external_imports() {
    let script_dir = tempdir().expect("script dir");
    let external_dir = tempdir().expect("external dir");

    let module_path = external_dir.path().join("logic.loom");
    std::fs::write(&module_path, "value() => 1").expect("write module");

    let policy_path = script_dir.path().join(".loomrc.json");
    std::fs::write(
        &policy_path,
        r#"{
            "version":1,
            "trust_mode":"trusted",
            "allow_all":false,
            "read_paths":["*"],
            "import_paths":["*"]
        }"#,
    )
    .expect("write policy");

    let source = format!("@import \"{}\" as lib", module_path.to_string_lossy());
    let program = loom::parser::parse(&source).expect("parse program");

    let mut runtime =
        loom::runtime::Runtime::new().with_script_dir(script_dir.path().to_str().expect("path"));
    apply_runtime_policy(&mut runtime, Some(policy_path.as_path()), None)
        .expect("apply policy should succeed");

    runtime
        .execute(&program)
        .await
        .expect("import should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn allow_all_with_restrictive_import_paths_only_limits_imports() {
    let script_dir = tempdir().expect("script dir");
    let external_dir = tempdir().expect("external dir");

    let module_path = external_dir.path().join("logic.loom");
    std::fs::write(&module_path, "value() => 1").expect("write module");
    let data_path = external_dir.path().join("data.txt");
    std::fs::write(&data_path, "ok").expect("write data");

    let policy_path = script_dir.path().join(".loomrc.json");
    std::fs::write(
        &policy_path,
        format!(
            r#"{{
                "version":1,
                "trust_mode":"trusted",
                "allow_all":true,
                "import_paths":["{}"]
            }}"#,
            script_dir.path().to_string_lossy()
        ),
    )
    .expect("write policy");

    let read_program =
        loom::parser::parse(&format!("\"{}\" >> @read", data_path.to_string_lossy()))
            .expect("parse read program");
    let import_program = loom::parser::parse(&format!(
        "@import \"{}\" as lib",
        module_path.to_string_lossy()
    ))
    .expect("parse import program");

    let mut runtime =
        loom::runtime::Runtime::new().with_script_dir(script_dir.path().to_str().expect("path"));
    apply_runtime_policy(&mut runtime, Some(policy_path.as_path()), None)
        .expect("apply policy should succeed");

    runtime
        .execute(&read_program)
        .await
        .expect("reads should remain allow-all");
    let err = runtime
        .execute(&import_program)
        .await
        .expect_err("import should be restricted");
    assert!(err.contains("Unauthorized Import"));
}

#[test]
fn policy_requires_version_field() {
    let script_dir = tempdir().expect("script dir");
    let policy_path = script_dir.path().join(".loomrc.json");
    std::fs::write(&policy_path, r#"{"allow_all":true}"#).expect("write policy");

    let mut runtime =
        loom::runtime::Runtime::new().with_script_dir(script_dir.path().to_str().expect("path"));
    let err = apply_runtime_policy(&mut runtime, Some(policy_path.as_path()), None)
        .expect_err("missing version should fail");
    assert!(err.contains("Missing required policy field 'version'"));
}

#[test]
fn policy_rejects_unsupported_version() {
    let script_dir = tempdir().expect("script dir");
    let policy_path = script_dir.path().join(".loomrc.json");
    std::fs::write(&policy_path, r#"{"version":2,"allow_all":true}"#).expect("write policy");

    let mut runtime =
        loom::runtime::Runtime::new().with_script_dir(script_dir.path().to_str().expect("path"));
    let err = apply_runtime_policy(&mut runtime, Some(policy_path.as_path()), None)
        .expect_err("unsupported version should fail");
    assert!(err.contains("Unsupported policy version 2"));
}

#[test]
fn policy_rejects_invalid_allow_glob_patterns() {
    let script_dir = tempdir().expect("script dir");
    let policy_path = script_dir.path().join(".loomrc.json");
    std::fs::write(
        &policy_path,
        r#"{"version":1,"allow_all":false,"read_paths":["./broken["]}"#,
    )
    .expect("write policy");

    let mut runtime =
        loom::runtime::Runtime::new().with_script_dir(script_dir.path().to_str().expect("path"));
    let err = apply_runtime_policy(&mut runtime, Some(policy_path.as_path()), None)
        .expect_err("invalid glob should fail");
    assert!(err.contains("Invalid read_paths glob"));
}

#[tokio::test(flavor = "multi_thread")]
async fn import_globs_allow_matching_modules_and_block_nonmatching() {
    let script_dir = tempdir().expect("script dir");
    let allowed_dir = script_dir.path().join("lib_orders");
    let denied_dir = script_dir.path().join("lib.misc");
    std::fs::create_dir_all(&allowed_dir).expect("create allowed lib");
    std::fs::create_dir_all(&denied_dir).expect("create denied lib");

    let allowed_module = allowed_dir.join("ok.loom");
    let denied_module = denied_dir.join("no.loom");
    std::fs::write(&allowed_module, "value() => 1").expect("write allowed module");
    std::fs::write(&denied_module, "value() => 0").expect("write denied module");

    let policy_path = script_dir.path().join(".loomrc.json");
    std::fs::write(
        &policy_path,
        r#"{
            "version":1,
            "trust_mode":"trusted",
            "allow_all":false,
            "read_paths":["./lib_*"],
            "import_paths":["./lib_*"]
        }"#,
    )
    .expect("write policy");

    let allowed_program = loom::parser::parse(&format!(
        "@import \"{}\" as lib",
        allowed_module.to_string_lossy()
    ))
    .expect("parse allowed import");
    let denied_program = loom::parser::parse(&format!(
        "@import \"{}\" as lib",
        denied_module.to_string_lossy()
    ))
    .expect("parse denied import");

    let mut runtime =
        loom::runtime::Runtime::new().with_script_dir(script_dir.path().to_str().expect("path"));
    apply_runtime_policy(&mut runtime, Some(policy_path.as_path()), None)
        .expect("apply policy should succeed");

    runtime
        .execute(&allowed_program)
        .await
        .expect("matching import glob should succeed");
    let err = runtime
        .execute(&denied_program)
        .await
        .expect_err("non-matching import glob should fail");
    assert!(err.contains("Unauthorized Import"));
}

#[tokio::test(flavor = "multi_thread")]
async fn allow_all_with_restrictive_write_paths_only_limits_writes() {
    let script_dir = tempdir().expect("script dir");
    let external_dir = tempdir().expect("external dir");

    let input_path = external_dir.path().join("input.txt");
    std::fs::write(&input_path, "ok").expect("write input");
    let allowed_output = script_dir.path().join("out_data").join("ok.txt");
    let denied_output = script_dir.path().join("out.blocked").join("no.txt");
    std::fs::create_dir_all(allowed_output.parent().expect("allowed parent"))
        .expect("create allowed parent");
    std::fs::create_dir_all(denied_output.parent().expect("denied parent"))
        .expect("create denied parent");

    let policy_path = script_dir.path().join(".loomrc.json");
    std::fs::write(
        &policy_path,
        r#"{
            "version":1,
            "trust_mode":"trusted",
            "allow_all":true,
            "write_paths":["./out_*"]
        }"#,
    )
    .expect("write policy");

    let read_program =
        loom::parser::parse(&format!("\"{}\" >> @read", input_path.to_string_lossy()))
            .expect("parse read program");
    let write_allowed_program =
        loom::parser::parse(&format!("42 >> \"{}\"", allowed_output.to_string_lossy()))
            .expect("parse allowed write program");
    let write_denied_program =
        loom::parser::parse(&format!("42 >> \"{}\"", denied_output.to_string_lossy()))
            .expect("parse denied write program");

    let mut runtime =
        loom::runtime::Runtime::new().with_script_dir(script_dir.path().to_str().expect("path"));
    apply_runtime_policy(&mut runtime, Some(policy_path.as_path()), None)
        .expect("apply policy should succeed");

    runtime
        .execute(&read_program)
        .await
        .expect("reads should remain allow-all");
    runtime
        .execute(&write_allowed_program)
        .await
        .expect("matching write glob should succeed");
    let err = runtime
        .execute(&write_denied_program)
        .await
        .expect_err("non-matching write glob should fail");
    assert!(err.contains("Unauthorized Write"));
}

#[tokio::test(flavor = "multi_thread")]
async fn deny_globs_override_allow_all_reads() {
    let script_dir = tempdir().expect("script dir");
    let blocked = script_dir.path().join("secret.pem");
    std::fs::write(&blocked, "super-secret").expect("write blocked file");

    let policy_path = script_dir.path().join(".loomrc.json");
    std::fs::write(
        &policy_path,
        r#"{
            "version":1,
            "trust_mode":"trusted",
            "allow_all":true,
            "deny_globs":["**/*.pem"]
        }"#,
    )
    .expect("write policy");

    let program = loom::parser::parse(&format!("\"{}\" >> @read", blocked.to_string_lossy()))
        .expect("parse read program");

    let mut runtime =
        loom::runtime::Runtime::new().with_script_dir(script_dir.path().to_str().expect("path"));
    apply_runtime_policy(&mut runtime, Some(policy_path.as_path()), None)
        .expect("apply policy should succeed");

    let err = runtime
        .execute(&program)
        .await
        .expect_err("deny_globs should override allow_all");
    assert!(err.contains("denied by deny_globs"));
}

#[tokio::test(flavor = "multi_thread")]
async fn restricted_trust_mode_from_policy_blocks_writes() {
    let script_dir = tempdir().expect("script dir");
    let output = script_dir.path().join("out.txt");
    let policy_path = script_dir.path().join(".loomrc.json");
    std::fs::write(
        &policy_path,
        r#"{
            "version":1,
            "trust_mode":"restricted",
            "allow_all":true
        }"#,
    )
    .expect("write policy");

    let program = loom::parser::parse(&format!("42 >> \"{}\"", output.to_string_lossy()))
        .expect("parse write program");

    let mut runtime =
        loom::runtime::Runtime::new().with_script_dir(script_dir.path().to_str().expect("path"));
    apply_runtime_policy(&mut runtime, Some(policy_path.as_path()), None)
        .expect("apply policy should succeed");

    let err = runtime
        .execute(&program)
        .await
        .expect_err("restricted mode should block writes");
    assert!(err.contains("write operation is disabled in restricted mode"));
}

#[tokio::test(flavor = "multi_thread")]
async fn relative_glob_paths_are_resolved_from_policy_directory() {
    let script_dir = tempdir().expect("script dir");
    let policy_dir = script_dir.path().join("config");
    let data_dir = script_dir.path().join("data_orders");
    std::fs::create_dir_all(&policy_dir).expect("create policy dir");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let input = data_dir.join("ok.txt");
    std::fs::write(&input, "ok").expect("write input");

    let policy_path = policy_dir.join(".loomrc.json");
    std::fs::write(
        &policy_path,
        r#"{
            "version":1,
            "trust_mode":"trusted",
            "allow_all":false,
            "read_paths":["../data_*"]
        }"#,
    )
    .expect("write policy");

    let program = loom::parser::parse(&format!("\"{}\" >> @read", input.to_string_lossy()))
        .expect("parse read program");

    let mut runtime =
        loom::runtime::Runtime::new().with_script_dir(script_dir.path().to_str().expect("path"));
    apply_runtime_policy(&mut runtime, Some(policy_path.as_path()), None)
        .expect("apply policy should succeed");

    runtime
        .execute(&program)
        .await
        .expect("relative glob should resolve from policy directory");
}

#[tokio::test(flavor = "multi_thread")]
async fn network_hosts_allows_http_post_when_host_matches() {
    let script_dir = tempdir().expect("script dir");
    let url = "mock://allowed.local/post?echo_body=1".to_string();
    let host = url
        .strip_prefix("mock://")
        .expect("url should have host")
        .split('/')
        .next()
        .expect("host")
        .to_string();

    let policy_path = script_dir.path().join(".loomrc.json");
    std::fs::write(
        &policy_path,
        format!(
            r#"{{
                "version":1,
                "trust_mode":"trusted",
                "allow_all":false,
                "network_hosts":["{}"]
            }}"#,
            host
        ),
    )
    .expect("write policy");

    let source = format!(
        "@import \"std.http\" as http\n42 >> @http.post(\\\"{}\")",
        url
    );
    let program = loom::parser::parse(&source).expect("parse");

    let mut runtime =
        loom::runtime::Runtime::new().with_script_dir(script_dir.path().to_str().expect("path"));
    apply_runtime_policy(&mut runtime, Some(policy_path.as_path()), None)
        .expect("apply policy should succeed");

    runtime
        .execute(&program)
        .await
        .expect("http should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn restricted_mode_blocks_http_even_with_network_hosts_wildcard() {
    let script_dir = tempdir().expect("script dir");
    let url = "mock://allowed.local/post?echo_body=1".to_string();

    let policy_path = script_dir.path().join(".loomrc.json");
    std::fs::write(
        &policy_path,
        r#"{
            "version":1,
            "trust_mode":"restricted",
            "allow_all":true,
            "network_hosts":["*"]
        }"#,
    )
    .expect("write policy");

    let source = format!(
        "@import \"std.http\" as http\n42 >> @http.post(\\\"{}\")",
        url
    );
    let program = loom::parser::parse(&source).expect("parse");

    let mut runtime =
        loom::runtime::Runtime::new().with_script_dir(script_dir.path().to_str().expect("path"));
    apply_runtime_policy(&mut runtime, Some(policy_path.as_path()), None)
        .expect("apply policy should succeed");

    let err = runtime
        .execute(&program)
        .await
        .expect_err("restricted mode should block network");
    assert!(err.contains("network operation is disabled in restricted mode"));
}
