use super::*;

#[test]
fn host_sandbox_dry_run_next_preserves_live_snapshot() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let sandbox_root = temp.path().join("sandbox");
    let live_snapshot_root = temp.path().join("live-snapshot");
    fs::create_dir_all(&sandbox_root).expect("create sandbox root");
    fs::create_dir_all(&live_snapshot_root).expect("create live snapshot root");

    let paths = test_runtime_paths(&sandbox_root);
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("..")
        .canonicalize()
        .expect("workspace root");

    let previous_home = std::env::var_os("HOME");
    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    let previous_fast_browser_home = std::env::var_os("FAST_BROWSER_HOME");
    let previous_codex_app_support = std::env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT");
    let previous_repo_root = std::env::var_os("CODEX_ROTATE_REPO_ROOT");
    let previous_environment = std::env::var_os("CODEX_ROTATE_ENVIRONMENT");
    let previous_usage_url = std::env::var_os("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");
    let previous_disable_launch = std::env::var_os("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH");

    unsafe {
        std::env::set_var("HOME", temp.path());
        std::env::set_var("CODEX_ROTATE_ENVIRONMENT", "host");
        std::env::set_var("CODEX_ROTATE_REPO_ROOT", &workspace_root);
        std::env::set_var("CODEX_ROTATE_HOME", &paths.rotate_home);
        std::env::set_var("CODEX_HOME", &paths.codex_home);
        std::env::set_var("FAST_BROWSER_HOME", &paths.fast_browser_home);
        std::env::set_var(
            "CODEX_ROTATE_CODEX_APP_SUPPORT",
            &paths.codex_app_support_dir,
        );
        std::env::set_var("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH", "1");
    }

    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");
    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");
    let source_persona_paths =
        host_persona_paths(&paths, source.persona.as_ref().unwrap()).expect("source paths");
    let target_persona_paths =
        host_persona_paths(&paths, target.persona.as_ref().unwrap()).expect("target paths");
    assert!(
        !source_persona_paths.root.join("fast-browser-home").exists(),
        "source persona should not provision persona-local fast-browser-home during sandbox setup"
    );
    assert!(
        !target_persona_paths.root.join("fast-browser-home").exists(),
        "target persona should not provision persona-local fast-browser-home during sandbox setup"
    );
    let source_leveldb = source_persona_paths
        .codex_app_support_dir
        .join("Local Storage")
        .join("leveldb");
    let target_leveldb = target_persona_paths
        .codex_app_support_dir
        .join("Local Storage")
        .join("leveldb");
    fs::create_dir_all(&source_leveldb).expect("create source leveldb");
    fs::create_dir_all(&target_leveldb).expect("create target leveldb");
    fs::write(
        source_leveldb.join("source-project.marker"),
        "source-local-storage",
    )
    .expect("write source local storage marker");
    fs::write(
        target_leveldb.join("target-project.marker"),
        "target-local-storage",
    )
    .expect("write target local storage marker");
    ensure_live_root_bindings(&paths, &source).expect("bind source roots");

    fs::create_dir_all(paths.codex_auth_file.parent().unwrap()).expect("create auth parent");
    codex_rotate_core::auth::write_codex_auth(&paths.codex_auth_file, &source.auth)
        .expect("write source auth");

    let pool = codex_rotate_core::pool::Pool {
        active_index: 0,
        accounts: vec![source.clone(), target.clone()],
    };
    codex_rotate_core::pool::save_pool(&pool).expect("save sandbox pool");

    let live_accounts = live_snapshot_root.join("accounts.json");
    let live_auth = live_snapshot_root.join("auth.json");
    fs::write(
        &live_accounts,
        serde_json::to_string_pretty(&pool).expect("serialize live pool"),
    )
    .expect("write live accounts");
    fs::write(
        &live_auth,
        serde_json::to_string_pretty(&source.auth).expect("serialize live auth"),
    )
    .expect("write live auth");
    let live_accounts_before = fs::read_to_string(&live_accounts).expect("read live accounts");
    let live_auth_before = fs::read_to_string(&live_auth).expect("read live auth");

    let usage_server = start_guest_bridge(
        json!({
            "user_id": target.account_id.clone(),
            "account_id": target.account_id.clone(),
            "email": target.email.clone(),
            "plan_type": target.plan_type.clone(),
            "rate_limit": {
                "allowed": true,
                "limit_reached": false,
                "primary_window": {
                    "used_percent": 10.0,
                    "limit_window_seconds": 3600,
                    "reset_after_seconds": 3600,
                    "reset_at": 2_000_000_000,
                },
                "secondary_window": null
            },
            "code_review_rate_limit": null,
            "additional_rate_limits": null,
            "credits": null,
            "promo": null
        })
        .to_string(),
    )
    .expect("start usage server");
    unsafe {
        std::env::set_var(
            "CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE",
            format!("http://127.0.0.1:{}", usage_server.port),
        );
    }

    let first_result = rotate_next(None, None).expect("rotate next");
    match &first_result {
        NextResult::Rotated { message, summary } => {
            assert!(message.contains("ROTATE"));
            assert_eq!(summary.account_id, target.account_id);
        }
        NextResult::Stayed { .. } => panic!("unexpected next result: stayed"),
        NextResult::Created { .. } => panic!("unexpected next result: created"),
    }

    let first_pool_after = load_pool().expect("load sandbox pool after forward rotation");
    assert_eq!(first_pool_after.active_index, 1);
    let first_auth_after = codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file)
        .expect("load sandbox auth after forward rotation");
    assert_eq!(first_auth_after.tokens.account_id, target.account_id);

    let first_target_paths =
        host_persona_paths(&paths, target.persona.as_ref().unwrap()).expect("target persona paths");
    assert!(is_symlink_to(&paths.codex_home, &first_target_paths.codex_home).unwrap());
    assert!(is_symlink_to(
        &paths.codex_app_support_dir,
        &first_target_paths.codex_app_support_dir
    )
    .unwrap());
    assert_eq!(
        fs::read_to_string(source_leveldb.join("source-project.marker"))
            .expect("read source marker after forward rotation"),
        "source-local-storage"
    );
    assert_eq!(
        fs::read_to_string(target_leveldb.join("target-project.marker"))
            .expect("read target marker after forward rotation"),
        "target-local-storage"
    );
    assert!(
        !source_leveldb.join("target-project.marker").exists(),
        "source persona should remain isolated from target local storage during forward rotation"
    );
    assert!(
        !target_leveldb.join("source-project.marker").exists(),
        "target persona should remain isolated from source local storage during forward rotation"
    );
    let first_checkpoint_cleared = load_rotation_checkpoint()
        .expect("load checkpoint")
        .is_none();
    assert!(first_checkpoint_cleared);

    let second_result = rotate_next(None, None).expect("rotate back to source");
    match &second_result {
        NextResult::Rotated { message, summary } => {
            assert!(message.contains("ROTATE"));
            assert_eq!(summary.account_id, source.account_id);
        }
        NextResult::Stayed { .. } => panic!("unexpected return result: stayed"),
        NextResult::Created { .. } => panic!("unexpected return result: created"),
    }

    let second_pool_after = load_pool().expect("load sandbox pool after return rotation");
    assert_eq!(second_pool_after.active_index, 0);
    let second_auth_after = codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file)
        .expect("load sandbox auth after return rotation");
    assert_eq!(second_auth_after.tokens.account_id, source.account_id);

    let second_target_paths =
        host_persona_paths(&paths, source.persona.as_ref().unwrap()).expect("source persona paths");
    assert!(is_symlink_to(&paths.codex_home, &second_target_paths.codex_home).unwrap());
    assert!(is_symlink_to(
        &paths.codex_app_support_dir,
        &second_target_paths.codex_app_support_dir
    )
    .unwrap());
    assert_eq!(
        fs::read_to_string(source_leveldb.join("source-project.marker"))
            .expect("read source marker after return rotation"),
        "source-local-storage"
    );
    assert_eq!(
        fs::read_to_string(target_leveldb.join("target-project.marker"))
            .expect("read target marker after return rotation"),
        "target-local-storage"
    );
    assert!(
        !source_leveldb.join("target-project.marker").exists(),
        "source persona should remain isolated from target local storage after return rotation"
    );
    assert!(
        !target_leveldb.join("source-project.marker").exists(),
        "target persona should remain isolated from source local storage after return rotation"
    );
    let second_checkpoint_cleared = load_rotation_checkpoint()
        .expect("load checkpoint after return rotation")
        .is_none();
    assert!(second_checkpoint_cleared);

    assert_eq!(
        fs::read_to_string(&live_accounts).expect("read live accounts after lifecycle"),
        live_accounts_before
    );
    assert_eq!(
        fs::read_to_string(&live_auth).expect("read live auth after lifecycle"),
        live_auth_before
    );

    report_sandbox_rotation_lifecycle(
        &workspace_root,
        &sandbox_root,
        &live_snapshot_root,
        &format!("http://127.0.0.1:{}", usage_server.port),
        &pool,
        &source.auth,
        &first_result,
        &first_target_paths,
        &first_pool_after,
        &first_auth_after,
        first_checkpoint_cleared,
        &second_result,
        &second_target_paths,
        &second_pool_after,
        &second_auth_after,
        second_checkpoint_cleared,
        &live_accounts_before,
        &live_auth_before,
    );

    drop(usage_server);
    restore_env("CODEX_ROTATE_ENVIRONMENT", previous_environment);
    restore_env("CODEX_ROTATE_REPO_ROOT", previous_repo_root);
    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
    restore_env("FAST_BROWSER_HOME", previous_fast_browser_home);
    restore_env("CODEX_ROTATE_CODEX_APP_SUPPORT", previous_codex_app_support);
    restore_env("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", previous_usage_url);
    restore_env(
        "CODEX_ROTATE_DISABLE_MANAGED_LAUNCH",
        previous_disable_launch,
    );
    restore_env("HOME", previous_home);
}

#[test]
fn host_sandbox_dry_run_prev_restores_live_snapshot() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let sandbox_root = temp.path().join("sandbox");
    let live_snapshot_root = temp.path().join("live-snapshot");
    fs::create_dir_all(&sandbox_root).expect("create sandbox root");
    fs::create_dir_all(&live_snapshot_root).expect("create live snapshot root");

    let paths = test_runtime_paths(&sandbox_root);
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("..")
        .canonicalize()
        .expect("workspace root");

    let previous_home = std::env::var_os("HOME");
    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    let previous_fast_browser_home = std::env::var_os("FAST_BROWSER_HOME");
    let previous_codex_app_support = std::env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT");
    let previous_repo_root = std::env::var_os("CODEX_ROTATE_REPO_ROOT");
    let previous_environment = std::env::var_os("CODEX_ROTATE_ENVIRONMENT");
    let previous_usage_url = std::env::var_os("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");
    let previous_disable_launch = std::env::var_os("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH");

    unsafe {
        std::env::set_var("HOME", temp.path());
        std::env::set_var("CODEX_ROTATE_ENVIRONMENT", "host");
        std::env::set_var("CODEX_ROTATE_REPO_ROOT", &workspace_root);
        std::env::set_var("CODEX_ROTATE_HOME", &paths.rotate_home);
        std::env::set_var("CODEX_HOME", &paths.codex_home);
        std::env::set_var("FAST_BROWSER_HOME", &paths.fast_browser_home);
        std::env::set_var(
            "CODEX_ROTATE_CODEX_APP_SUPPORT",
            &paths.codex_app_support_dir,
        );
        std::env::set_var("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH", "1");
    }

    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");
    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");
    ensure_live_root_bindings(&paths, &source).expect("bind source roots");

    fs::create_dir_all(paths.codex_auth_file.parent().unwrap()).expect("create auth parent");
    codex_rotate_core::auth::write_codex_auth(&paths.codex_auth_file, &source.auth)
        .expect("write source auth");

    let pool = codex_rotate_core::pool::Pool {
        active_index: 0,
        accounts: vec![source.clone(), target.clone()],
    };
    codex_rotate_core::pool::save_pool(&pool).expect("save sandbox pool");

    let live_accounts = live_snapshot_root.join("accounts.json");
    let live_auth = live_snapshot_root.join("auth.json");
    fs::write(
        &live_accounts,
        serde_json::to_string_pretty(&pool).expect("serialize live pool"),
    )
    .expect("write live accounts");
    fs::write(
        &live_auth,
        serde_json::to_string_pretty(&source.auth).expect("serialize live auth"),
    )
    .expect("write live auth");
    let live_accounts_before = fs::read_to_string(&live_accounts).expect("read live accounts");
    let live_auth_before = fs::read_to_string(&live_auth).expect("read live auth");

    let usage_server = start_guest_bridge(
        json!({
            "user_id": target.account_id.clone(),
            "account_id": target.account_id.clone(),
            "email": target.email.clone(),
            "plan_type": target.plan_type.clone(),
            "rate_limit": {
                "allowed": true,
                "limit_reached": false,
                "primary_window": {
                    "used_percent": 10.0,
                    "limit_window_seconds": 3600,
                    "reset_after_seconds": 3600,
                    "reset_at": 2_000_000_000,
                },
                "secondary_window": null
            },
            "code_review_rate_limit": null,
            "additional_rate_limits": null,
            "credits": null,
            "promo": null
        })
        .to_string(),
    )
    .expect("start usage server");
    unsafe {
        std::env::set_var(
            "CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE",
            format!("http://127.0.0.1:{}", usage_server.port),
        );
    }

    let first_result = rotate_next(None, None).expect("rotate next");
    match &first_result {
        NextResult::Rotated { message, summary } => {
            assert!(message.contains("ROTATE"));
            assert_eq!(summary.account_id, target.account_id);
        }
        NextResult::Stayed { .. } => panic!("unexpected next result: stayed"),
        NextResult::Created { .. } => panic!("unexpected next result: created"),
    }

    let first_pool_after = load_pool().expect("load sandbox pool after forward rotation");
    assert_eq!(first_pool_after.active_index, 1);
    let first_auth_after = codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file)
        .expect("load sandbox auth after forward rotation");
    assert_eq!(first_auth_after.tokens.account_id, target.account_id);

    let first_target_paths =
        host_persona_paths(&paths, target.persona.as_ref().unwrap()).expect("target persona paths");
    assert!(is_symlink_to(&paths.codex_home, &first_target_paths.codex_home).unwrap());
    assert!(is_symlink_to(
        &paths.codex_app_support_dir,
        &first_target_paths.codex_app_support_dir
    )
    .unwrap());
    let first_checkpoint_cleared = load_rotation_checkpoint()
        .expect("load checkpoint")
        .is_none();
    assert!(first_checkpoint_cleared);

    let backward_message = rotate_prev(None, None).expect("rotate prev");
    assert!(backward_message.contains("ROTATE"));
    assert!(!backward_message.trim().is_empty());

    let second_pool_after = load_pool().expect("load sandbox pool after prev rotation");
    assert_eq!(second_pool_after.active_index, 0);
    let second_auth_after = codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file)
        .expect("load sandbox auth after prev rotation");
    assert_eq!(second_auth_after.tokens.account_id, source.account_id);

    let second_target_paths =
        host_persona_paths(&paths, source.persona.as_ref().unwrap()).expect("source persona paths");
    assert!(is_symlink_to(&paths.codex_home, &second_target_paths.codex_home).unwrap());
    assert!(is_symlink_to(
        &paths.codex_app_support_dir,
        &second_target_paths.codex_app_support_dir
    )
    .unwrap());
    let second_checkpoint_cleared = load_rotation_checkpoint()
        .expect("load checkpoint after prev rotation")
        .is_none();
    assert!(second_checkpoint_cleared);

    assert_eq!(
        fs::read_to_string(&live_accounts).expect("read live accounts after lifecycle"),
        live_accounts_before
    );
    assert_eq!(
        fs::read_to_string(&live_auth).expect("read live auth after lifecycle"),
        live_auth_before
    );

    let second_result = NextResult::Rotated {
        message: backward_message,
        summary: summarize_codex_auth(&second_auth_after),
    };
    report_sandbox_rotation_lifecycle(
        &workspace_root,
        &sandbox_root,
        &live_snapshot_root,
        &format!("http://127.0.0.1:{}", usage_server.port),
        &pool,
        &source.auth,
        &first_result,
        &first_target_paths,
        &first_pool_after,
        &first_auth_after,
        first_checkpoint_cleared,
        &second_result,
        &second_target_paths,
        &second_pool_after,
        &second_auth_after,
        second_checkpoint_cleared,
        &live_accounts_before,
        &live_auth_before,
    );

    drop(usage_server);
    restore_env("CODEX_ROTATE_ENVIRONMENT", previous_environment);
    restore_env("CODEX_ROTATE_REPO_ROOT", previous_repo_root);
    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
    restore_env("FAST_BROWSER_HOME", previous_fast_browser_home);
    restore_env("CODEX_ROTATE_CODEX_APP_SUPPORT", previous_codex_app_support);
    restore_env("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", previous_usage_url);
    restore_env(
        "CODEX_ROTATE_DISABLE_MANAGED_LAUNCH",
        previous_disable_launch,
    );
    restore_env("HOME", previous_home);
}
