use super::*;

#[test]
fn rollback_after_failed_host_activation_restores_state_and_symlinks() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    let previous_account_flow_file = std::env::var_os("CODEX_ROTATE_ACCOUNT_FLOW_FILE");
    let invalid_account_flow = temp.path().join("missing-workflow.yaml");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
        std::env::set_var("CODEX_ROTATE_ACCOUNT_FLOW_FILE", &invalid_account_flow);
    }

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
    codex_rotate_core::pool::save_pool(&pool).expect("save pool");

    let prepared = PreparedRotation {
        action: PreparedRotationAction::Switch,
        pool: pool.clone(),
        previous_index: 0,
        target_index: 1,
        previous: source.clone(),
        target: target.clone(),
        message: "rotating".to_string(),
        persist_pool: false,
    };

    // Simulate partial activation
    switch_host_persona(&paths, &source, &target, true).expect("switch persona");
    codex_rotate_core::pool::write_selected_account_auth(&target).expect("write target auth");

    let target_persona_paths =
        host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();
    assert!(is_symlink_to(&paths.codex_home, &target_persona_paths.codex_home).unwrap());
    assert!(is_symlink_to(
        &paths.debug_profile_dir,
        &target_persona_paths.debug_profile_dir
    )
    .unwrap());

    rollback_after_failed_host_activation(&paths, &prepared, false, 9333).expect("rollback");

    // Verify restoration
    let source_persona_paths =
        host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    assert!(is_symlink_to(&paths.codex_home, &source_persona_paths.codex_home).unwrap());
    assert!(is_symlink_to(
        &paths.debug_profile_dir,
        &source_persona_paths.debug_profile_dir
    )
    .unwrap());
    let restored_auth =
        codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file).expect("load auth");
    assert_eq!(restored_auth.tokens.account_id, "acct-source");
    let restored_pool = load_pool().expect("load pool");
    assert_eq!(restored_pool.active_index, 0);

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
    restore_env("CODEX_ROTATE_ACCOUNT_FLOW_FILE", previous_account_flow_file);
}

#[test]
fn host_activation_aborts_and_retains_source_when_export_fails() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
    }

    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");
    ensure_live_root_bindings(&paths, &source).expect("bind source roots");

    fs::create_dir_all(paths.codex_auth_file.parent().unwrap()).expect("create auth parent");
    codex_rotate_core::auth::write_codex_auth(&paths.codex_auth_file, &source.auth)
        .expect("write source auth");

    // Seed state DB with an active thread so it attempts to connect to the app server
    let runtime_paths = resolve_paths().expect("resolve runtime paths");
    fs::create_dir_all(runtime_paths.codex_state_db_file.parent().unwrap())
        .expect("create state parent");
    let connection =
        rusqlite::Connection::open(&runtime_paths.codex_state_db_file).expect("open state");
    connection
        .execute_batch(
            r#"
create table threads (
  id text primary key,
  rollout_path text not null default '',
  updated_at integer not null,
  archived integer not null default 0
);
insert into threads (id, rollout_path, updated_at, archived) values
  ('thread-active', '', 1, 0);
"#,
        )
        .expect("seed state");

    let pool = codex_rotate_core::pool::Pool {
        active_index: 0,
        accounts: vec![source.clone(), target.clone()],
    };
    codex_rotate_core::pool::save_pool(&pool).expect("save pool");

    let process_guard = ProcessTracker::new()
        .expect("create process tracker")
        .leak_guard("host activation managed codex cleanup");
    let managed_codex =
        ManagedCodexProcess::start(&paths.debug_profile_dir).expect("start managed codex");
    process_guard.record_test_owned_process(
        managed_codex.pid(),
        "managed-codex",
        managed_codex.command(),
    );

    let prepared = PreparedRotation {
        action: PreparedRotationAction::Switch,
        pool: pool.clone(),
        previous_index: 0,
        target_index: 1,
        previous: source.clone(),
        target: target.clone(),
        message: "rotating".to_string(),
        persist_pool: false,
    };

    // Export should fail here due to no listening app server (connection refused)
    let error = activate_host_rotation(
        &paths,
        &prepared,
        9333,
        None,
        Vec::new(),
        RotationCommandOptions::default(),
    )
    .expect_err("host activation should fail during export phase");

    let message = format!("{:#}", error);
    assert!(
        message.contains("initial thread/read request failed before relaunch")
            || message.contains("Managed Codex launch is disabled"),
        "Unexpected error message: {}",
        message
    );

    // Verify restoration: pool index remains 0, auth remains source, symlinks remain source
    let restored_pool = load_pool().expect("load pool");
    assert_eq!(restored_pool.active_index, 0);

    let restored_auth =
        codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file).expect("load auth");
    assert_eq!(restored_auth.tokens.account_id, "acct-source");

    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).unwrap());
    assert!(is_symlink_to(&paths.debug_profile_dir, &source_paths.debug_profile_dir).unwrap());

    drop(managed_codex);

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
}

#[test]
fn host_activation_retains_target_state_when_relaunch_fails() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    let previous_disable_launch = std::env::var_os("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
        std::env::set_var("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH", "1");
    }

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
    codex_rotate_core::pool::save_pool(&pool).expect("save pool");

    let process_guard = ProcessTracker::new()
        .expect("create process tracker")
        .leak_guard("host activation managed codex cleanup");
    let managed_codex =
        ManagedCodexProcess::start(&paths.debug_profile_dir).expect("start managed codex");
    process_guard.record_test_owned_process(
        managed_codex.pid(),
        "managed-codex",
        managed_codex.command(),
    );

    let prepared = PreparedRotation {
        action: PreparedRotationAction::Switch,
        pool: pool.clone(),
        previous_index: 0,
        target_index: 1,
        previous: source.clone(),
        target: target.clone(),
        message: "rotating".to_string(),
        persist_pool: false,
    };

    let probe = TcpListener::bind("127.0.0.1:0").expect("bind probe port");
    let port = probe.local_addr().expect("probe local addr").port();
    drop(probe);

    let error = activate_host_rotation(
        &paths,
        &prepared,
        port,
        None,
        Vec::new(),
        RotationCommandOptions::default(),
    )
    .expect_err("host activation should fail after commit");
    let message = format!("{:#}", error);
    assert!(!message.trim().is_empty());

    let committed_pool = load_pool().expect("load committed pool");
    eprintln!("host activation error: {message}");
    eprintln!(
        "host activation pool: {}",
        fs::read_to_string(paths.rotate_home.join("accounts.json")).expect("read accounts.json")
    );
    assert_eq!(committed_pool.active_index, 0);
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();
    assert!(is_symlink_to(&paths.codex_home, &target_paths.codex_home).unwrap());
    let restored_auth =
        codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file).expect("load auth");
    assert_eq!(restored_auth.tokens.account_id, "acct-target");

    drop(managed_codex);

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
    restore_env(
        "CODEX_ROTATE_DISABLE_MANAGED_LAUNCH",
        previous_disable_launch,
    );
}

#[test]
fn host_activation_rejects_unready_target_without_committing_pool() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let mut target = test_account("acct-target", "persona-target");
    target.persona = None;

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
    }

    provision_host_persona(&paths, &source, None).expect("provision source");
    ensure_live_root_bindings(&paths, &source).expect("bind source roots");

    fs::create_dir_all(paths.codex_auth_file.parent().unwrap()).expect("create auth parent");
    codex_rotate_core::auth::write_codex_auth(&paths.codex_auth_file, &source.auth)
        .expect("write source auth");

    let pool = codex_rotate_core::pool::Pool {
        active_index: 0,
        accounts: vec![source.clone(), target.clone()],
    };
    codex_rotate_core::pool::save_pool(&pool).expect("save pool");

    let prepared = PreparedRotation {
        action: PreparedRotationAction::Switch,
        pool: pool.clone(),
        previous_index: 0,
        target_index: 1,
        previous: source.clone(),
        target: target.clone(),
        message: "rotating".to_string(),
        persist_pool: false,
    };

    let error = activate_host_rotation(
        &paths,
        &prepared,
        9333,
        None,
        Vec::new(),
        RotationCommandOptions::default(),
    )
    .expect_err("host activation should fail before committing pool");
    let message = format!("{:#}", error);
    assert!(
        message.contains("persona metadata")
            || message.contains("Failed to list running processes")
            || message.contains("Operation not permitted"),
        "{message}"
    );

    let restored_pool = load_pool().expect("load restored pool");
    assert_eq!(restored_pool.active_index, 0);
    let restored_auth =
        codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file).expect("load auth");
    assert_eq!(restored_auth.tokens.account_id, source.account_id);
    let source_paths =
        host_persona_paths(&paths, source.persona.as_ref().unwrap()).expect("source persona paths");
    assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).unwrap());
    assert!(is_symlink_to(
        &paths.codex_app_support_dir,
        &source_paths.codex_app_support_dir
    )
    .unwrap());
    assert!(is_symlink_to(&paths.debug_profile_dir, &source_paths.debug_profile_dir).unwrap());

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
}

#[test]
fn rotate_next_rechecks_disabled_target_before_persona_ready_mutates_live_roots() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_environment = std::env::var_os("CODEX_ROTATE_ENVIRONMENT");
    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    let previous_fast_browser_home = std::env::var_os("FAST_BROWSER_HOME");
    let previous_codex_app_support = std::env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT");
    let previous_usage_url = std::env::var_os("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");

    unsafe {
        std::env::set_var("CODEX_ROTATE_ENVIRONMENT", "host");
        std::env::set_var("CODEX_ROTATE_HOME", &paths.rotate_home);
        std::env::set_var("CODEX_HOME", &paths.codex_home);
        std::env::set_var("FAST_BROWSER_HOME", &paths.fast_browser_home);
        std::env::set_var(
            "CODEX_ROTATE_CODEX_APP_SUPPORT",
            &paths.codex_app_support_dir,
        );
    }

    struct NoActivationBackend;
    impl RotationBackend for NoActivationBackend {
        fn activate(
            &self,
            _prepared: &PreparedRotation,
            _port: u16,
            _progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
            _source_thread_candidates: Vec<String>,
            _options: RotationCommandOptions,
        ) -> Result<Vec<ThreadHandoff>> {
            panic!("disabled target must be rejected before activation");
        }

        fn rollback_after_failed_activation(
            &self,
            _prepared: &PreparedRotation,
            _port: u16,
            _progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
        ) -> Result<()> {
            panic!("disabled target must be rejected before rollback is needed");
        }

        fn rotate_next(
            &self,
            _port: u16,
            _progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
            _options: RotationCommandOptions,
        ) -> Result<NextResult> {
            unreachable!()
        }

        fn rotate_prev(
            &self,
            _port: u16,
            _progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
            _options: RotationCommandOptions,
        ) -> Result<String> {
            unreachable!()
        }

        fn relogin(
            &self,
            _port: u16,
            _selector: &str,
            _options: ReloginOptions,
            _progress: Option<AutomationProgressCallback>,
        ) -> Result<String> {
            unreachable!()
        }
    }

    let result = (|| -> Result<()> {
        let mut source = test_account("acct-source", "persona-source");
        source.email = "acct-source@gmail.com".to_string();
        source.label = "acct-source@gmail.com_free".to_string();
        let mut target = test_account("acct-target", "persona-target");
        target.email = "acct-target@astronlab.com".to_string();
        target.label = "acct-target@astronlab.com_free".to_string();

        provision_host_persona(&paths, &source, None).expect("provision source");
        ensure_live_root_bindings(&paths, &source).expect("bind source roots");
        fs::create_dir_all(paths.codex_auth_file.parent().unwrap()).expect("create auth parent");
        codex_rotate_core::auth::write_codex_auth(&paths.codex_auth_file, &source.auth)
            .expect("write source auth");
        codex_rotate_core::pool::save_pool(&codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        })
        .expect("save pool");

        let target_paths =
            host_persona_paths(&paths, target.persona.as_ref().unwrap()).expect("target paths");
        assert!(
            !target_paths.root.exists(),
            "target persona should start unmaterialized"
        );

        let (usage_url, handle) = start_usage_server_that_disables_domain(
            paths.rotate_home.join("accounts.json"),
            "astronlab.com",
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
        )?;
        unsafe {
            std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", usage_url);
        }

        let error = rotate_next_impl_with_retry(
            &NoActivationBackend,
            9333,
            None,
            false,
            RotationCommandOptions::default(),
            0,
        )
        .expect_err("disabled target should abort before activation");
        handle.join().expect("usage server should finish");
        assert!(error
            .to_string()
            .contains("is in a disabled domain and cannot be activated"));

        let source_paths =
            host_persona_paths(&paths, source.persona.as_ref().unwrap()).expect("source paths");
        assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).unwrap());
        assert!(
            !target_paths.root.exists(),
            "target persona must not be provisioned after becoming disabled"
        );
        let auth_after = codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file)
            .expect("load auth after rejected rotation");
        assert_eq!(auth_after.tokens.account_id, source.account_id);
        Ok(())
    })();

    restore_env("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", previous_usage_url);
    restore_env("CODEX_ROTATE_ENVIRONMENT", previous_environment);
    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
    restore_env("FAST_BROWSER_HOME", previous_fast_browser_home);
    restore_env("CODEX_ROTATE_CODEX_APP_SUPPORT", previous_codex_app_support);
    result.expect("disabled target should be rejected before live root mutation");
}

#[test]
fn host_activation_stages_target_without_committing_pool() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");
    let path_guard = FilesystemTracker::new()
        .expect("create filesystem tracker")
        .leak_guard("host activation filesystem cleanup");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
    }

    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");
    ensure_live_root_bindings(&paths, &source).expect("bind source roots");

    let pool = codex_rotate_core::pool::Pool {
        active_index: 0,
        accounts: vec![source.clone(), target.clone()],
    };
    codex_rotate_core::pool::save_pool(&pool).expect("save pool");

    let prepared = PreparedRotation {
        action: PreparedRotationAction::Switch,
        pool: pool.clone(),
        previous_index: 0,
        target_index: 1,
        previous: source.clone(),
        target: target.clone(),
        message: "rotating".to_string(),
        persist_pool: false,
    };

    let activation = activate_host_rotation(
        &paths,
        &prepared,
        9333,
        None,
        Vec::new(),
        RotationCommandOptions::default(),
    )
    .expect("host activation");
    assert!(activation.items.is_empty());

    let committed_pool = load_pool().expect("load committed pool");
    assert_eq!(committed_pool.active_index, 0);
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();
    assert!(is_symlink_to(&paths.codex_home, &target_paths.codex_home).unwrap());
    path_guard.record_symlink_target(&target_paths.codex_home, "target codex-home", false);
    path_guard.record_symlink_target(
        &target_paths.codex_app_support_dir,
        "target app-support",
        false,
    );
    assert!(is_symlink_to(&paths.debug_profile_dir, &target_paths.debug_profile_dir).unwrap());
    path_guard.record_symlink_target(
        &target_paths.debug_profile_dir,
        "target managed-profile",
        false,
    );

    drop(temp);
    path_guard
        .assert_clean()
        .expect("host activation targets should be removed");

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
}

#[test]
fn host_activation_force_managed_window_failure_aborts_before_switch() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    let previous_disable_launch = std::env::var_os("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
        std::env::set_var("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH", "1");
    }

    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");
    ensure_live_root_bindings(&paths, &source).expect("bind source roots");

    let pool = codex_rotate_core::pool::Pool {
        active_index: 0,
        accounts: vec![source.clone(), target.clone()],
    };
    codex_rotate_core::pool::save_pool(&pool).expect("save pool");

    let prepared = PreparedRotation {
        action: PreparedRotationAction::Switch,
        pool: pool.clone(),
        previous_index: 0,
        target_index: 1,
        previous: source.clone(),
        target: target.clone(),
        message: "rotating".to_string(),
        persist_pool: false,
    };

    let progress_messages = Arc::new(Mutex::new(Vec::new()));
    let progress_sink = Arc::clone(&progress_messages);
    let progress: Arc<dyn Fn(String) + Send + Sync> =
        Arc::new(move |message| progress_sink.lock().expect("progress").push(message));

    let error = activate_host_rotation(
        &paths,
        &prepared,
        9333,
        Some(&progress),
        Vec::new(),
        RotationCommandOptions {
            force_managed_window: true,
        },
    )
    .expect_err("host activation should fail before switching when -mw launch fails");
    assert!(
        format!("{:#}", error).contains("requested by -mw"),
        "error should mention -mw launch failure: {error:#}"
    );

    let messages = progress_messages.lock().expect("progress messages");
    assert!(
        messages
            .iter()
            .any(|message| message.contains("opening a managed window")),
        "progress should mention auto-launch attempt: {:?}",
        *messages
    );

    let pool_after = load_pool().expect("load pool");
    assert_eq!(pool_after.active_index, 0);
    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).unwrap());
    assert!(is_symlink_to(&paths.debug_profile_dir, &source_paths.debug_profile_dir).unwrap());

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
    restore_env(
        "CODEX_ROTATE_DISABLE_MANAGED_LAUNCH",
        previous_disable_launch,
    );
}
