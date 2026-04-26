use super::*;

#[test]
fn ensure_host_personas_ready_repairs_misbound_live_roots() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");

    // Bind to target initially
    ensure_live_root_bindings(&paths, &target).expect("bind target roots");

    let mut pool = codex_rotate_core::pool::Pool {
        active_index: 0, // Should be source
        accounts: vec![source.clone(), target.clone()],
    };

    // This should repair the bindings back to source because active_index is 0
    ensure_host_personas_ready(&paths, &mut pool).expect("repair roots");

    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).unwrap());
    assert!(is_symlink_to(&paths.debug_profile_dir, &source_paths.debug_profile_dir).unwrap());
}

#[test]
fn repair_host_history_dry_run_does_not_provision_target_persona() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    let previous_environment = std::env::var_os("CODEX_ROTATE_ENVIRONMENT");
    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    let previous_fast_browser_home = std::env::var_os("FAST_BROWSER_HOME");
    let previous_codex_app_support = std::env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT");

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

    let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();
    assert!(!target_paths.codex_home.exists());

    let output = repair_host_history("acct-source", &["acct-target".to_string()], false, false)
        .expect("dry-run repair");
    assert!(output.contains("Repair mode: dry-run"));
    assert!(!target_paths.codex_home.exists());

    restore_env("CODEX_ROTATE_ENVIRONMENT", previous_environment);
    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
    restore_env("FAST_BROWSER_HOME", previous_fast_browser_home);
    restore_env("CODEX_ROTATE_CODEX_APP_SUPPORT", previous_codex_app_support);
}

#[test]
fn checkpoint_recovery_prefers_fallback_index_when_account_ids_repeat() {
    let source = test_account("acct-team", "persona-source");
    let target = test_account("acct-team", "persona-target");
    let pool = codex_rotate_core::pool::Pool {
        active_index: 0,
        accounts: vec![source, target],
    };

    let resolved =
        resolve_checkpoint_account_index(&pool, "acct-team", 1, "target").expect("resolve");
    assert_eq!(resolved, 1);
}

#[test]
fn host_persona_paths_rejects_traversal_root() {
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let mut account = test_account("acct-source", "persona-source");
    account.persona.as_mut().unwrap().host_root_rel_path = Some("../escape".to_string());

    let result = host_persona_paths(&paths, account.persona.as_ref().unwrap());
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("cannot contain parent-directory segments"));
}

struct RecordingRetryBackend {
    rollback_calls: Arc<Mutex<usize>>,
}

impl RotationBackend for RecordingRetryBackend {
    fn activate(
        &self,
        _prepared: &PreparedRotation,
        _port: u16,
        _progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
        _source_thread_candidates: Vec<String>,
        _options: RotationCommandOptions,
    ) -> Result<Vec<ThreadHandoff>> {
        panic!("activate should not run in rollback/retry helper tests");
    }

    fn rollback_after_failed_activation(
        &self,
        _prepared: &PreparedRotation,
        _port: u16,
        _progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<()> {
        let mut calls = self.rollback_calls.lock().expect("rollback calls");
        *calls += 1;
        Ok(())
    }

    fn rotate_next(
        &self,
        _port: u16,
        _progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
        _options: RotationCommandOptions,
    ) -> Result<NextResult> {
        panic!("rotate_next should not run in rollback/retry helper tests");
    }

    fn rotate_prev(
        &self,
        _port: u16,
        _progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
        _options: RotationCommandOptions,
    ) -> Result<String> {
        panic!("rotate_prev should not run in rollback/retry helper tests");
    }

    fn relogin(
        &self,
        _port: u16,
        _selector: &str,
        _options: ReloginOptions,
        _progress: Option<AutomationProgressCallback>,
    ) -> Result<String> {
        panic!("relogin should not run in rollback/retry helper tests");
    }
}

#[test]
fn rollback_and_maybe_retry_after_disabled_target_retries_once_after_rollback() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let previous_environment = std::env::var_os("CODEX_ROTATE_ENVIRONMENT");
    unsafe {
        std::env::set_var("CODEX_ROTATE_ENVIRONMENT", "vm");
    }
    let rollback_calls = Arc::new(Mutex::new(0usize));
    let backend = RecordingRetryBackend {
        rollback_calls: rollback_calls.clone(),
    };
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");
    let prepared = PreparedRotation {
        action: PreparedRotationAction::Switch,
        pool: codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        },
        previous_index: 0,
        target_index: 1,
        previous: source,
        target,
        message: "rotating".to_string(),
        persist_pool: false,
    };
    let progress_messages = Arc::new(Mutex::new(Vec::new()));
    let progress_sink = progress_messages.clone();
    let progress: Arc<dyn Fn(String) + Send + Sync> =
        Arc::new(move |message| progress_sink.lock().expect("progress").push(message));

    let result = rollback_and_maybe_retry_after_disabled_target(
            &backend,
            &prepared,
            9333,
            Some(progress),
            DisabledTargetRetryContext {
                budget: 1,
                error: anyhow!(
                    "Target account acct-target is in a disabled domain and cannot be activated."
                ),
                message: "Rotation target became disabled after activation; restored the previous account and re-evaluating eligible target.",
            },
            |_| {
                Ok(NextResult::Rotated {
                    message: "retried".to_string(),
                    summary: codex_rotate_core::auth::AuthSummary {
                        email: "acct-target@astronlab.com".to_string(),
                        account_id: "acct-target".to_string(),
                        plan_type: "free".to_string(),
                    },
                })
            },
        )
        .expect("retry result");

    assert!(matches!(result, NextResult::Rotated { .. }));
    assert_eq!(*rollback_calls.lock().expect("rollback calls"), 1);
    assert_eq!(
            progress_messages.lock().expect("progress").as_slice(),
            ["Rotation target became disabled after activation; restored the previous account and re-evaluating eligible target."]
        );
    restore_env("CODEX_ROTATE_ENVIRONMENT", previous_environment);
}

#[test]
fn rollback_and_maybe_retry_after_disabled_target_returns_error_when_budget_exhausted() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let previous_environment = std::env::var_os("CODEX_ROTATE_ENVIRONMENT");
    unsafe {
        std::env::set_var("CODEX_ROTATE_ENVIRONMENT", "vm");
    }
    let rollback_calls = Arc::new(Mutex::new(0usize));
    let backend = RecordingRetryBackend {
        rollback_calls: rollback_calls.clone(),
    };
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");
    let prepared = PreparedRotation {
        action: PreparedRotationAction::Switch,
        pool: codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        },
        previous_index: 0,
        target_index: 1,
        previous: source,
        target,
        message: "rotating".to_string(),
        persist_pool: false,
    };

    let error = rollback_and_maybe_retry_after_disabled_target(
        &backend,
        &prepared,
        9333,
        None,
        DisabledTargetRetryContext {
            budget: 0,
            error: anyhow!(
                "Target account acct-target is in a disabled domain and cannot be activated."
            ),
            message: "unused",
        },
        |_| panic!("retry closure should not run when the retry budget is exhausted"),
    )
    .expect_err("budget exhausted should preserve the disabled-target error");

    assert!(error
        .to_string()
        .contains("is in a disabled domain and cannot be activated"));
    assert_eq!(*rollback_calls.lock().expect("rollback calls"), 1);
    restore_env("CODEX_ROTATE_ENVIRONMENT", previous_environment);
}
