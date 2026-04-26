use super::*;

#[test]
fn rotation_lock_prevents_concurrent_rotation() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", temp.path());
    }

    let _lock = RotationLock::acquire().expect("acquire first lock");

    let result = rotate_next(None, None);
    let error = match result {
        Ok(_) => panic!("rotate_next should fail due to lock contention"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("Another rotation is already in progress"));
    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}

#[test]
fn relogin_respects_rotation_lock() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    let previous_account_flow_file = std::env::var_os("CODEX_ROTATE_ACCOUNT_FLOW_FILE");
    let invalid_account_flow = temp.path().join("missing-workflow.yaml");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
        std::env::set_var("CODEX_ROTATE_ACCOUNT_FLOW_FILE", &invalid_account_flow);
    }

    let source = test_account("acct-source", "persona-source");
    provision_host_persona(&paths, &source, None).expect("provision source");
    ensure_live_root_bindings(&paths, &source).expect("bind source roots");
    let pool = codex_rotate_core::pool::Pool {
        active_index: 0,
        accounts: vec![source],
    };
    codex_rotate_core::pool::save_pool(&pool).expect("save pool");

    let _lock = RotationLock::acquire().expect("acquire lock");
    let result = relogin(
        Some(9333),
        "non-pool-selector",
        ReloginOptions::default(),
        None,
    );
    let error = match result {
        Ok(_) => panic!("relogin should fail due to lock contention"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("Another rotation is already in progress"));

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
    restore_env("CODEX_ROTATE_ACCOUNT_FLOW_FILE", previous_account_flow_file);
}

#[test]
fn relogin_pool_selector_does_not_self_contend_on_rotation_lock() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    let previous_codex_bin = std::env::var_os("CODEX_ROTATE_CODEX_BIN");
    let previous_account_flow_file = std::env::var_os("CODEX_ROTATE_ACCOUNT_FLOW_FILE");
    let invalid_account_flow = temp.path().join("missing-workflow.yaml");
    let fake_codex_log = temp.path().join("fake-codex.log");
    let fake_codex_bin = temp.path().join("bin").join("codex");
    write_fake_codex_bin(&fake_codex_bin, &fake_codex_log);
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
        std::env::set_var("CODEX_ROTATE_CODEX_BIN", &fake_codex_bin);
        std::env::set_var("CODEX_ROTATE_ACCOUNT_FLOW_FILE", &invalid_account_flow);
    }

    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");
    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");
    ensure_live_root_bindings(&paths, &source).expect("bind source roots");
    let pool = codex_rotate_core::pool::Pool {
        active_index: 0,
        accounts: vec![source.clone(), target.clone()],
    };
    codex_rotate_core::pool::save_pool(&pool).expect("save pool");
    fs::write(
        paths.rotate_home.join("accounts.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "version": 9,
            "pending": {
                target.email.clone(): {
                    "stored": {
                        "email": target.email.clone(),
                        "profile_name": "persona-target",
                        "template": "acct-target@astronlab.com",
                        "suffix": 1,
                        "selector": target.label.clone(),
                        "alias": null,
                        "birth_month": 1,
                        "birth_day": 24,
                        "birth_year": 1990,
                        "created_at": "2026-04-13T02:52:15.012Z",
                        "updated_at": "2026-04-13T02:52:15.012Z"
                    },
                    "started_at": "2026-04-13T02:52:15.012Z"
                }
            }
        }))
        .expect("serialize credential store"),
    )
    .expect("write credential store");

    let result = relogin(Some(9333), "acct-target", ReloginOptions::default(), None);
    let error = match result {
        Ok(_) => panic!("relogin should fail because workflow file is missing"),
        Err(error) => error.to_string(),
    };
    assert!(
        !error.contains("Another rotation is already in progress"),
        "pool-backed relogin should not self-contend on rotation lock; got: {error}"
    );
    let codex_calls = fs::read_to_string(&fake_codex_log).unwrap_or_default();
    assert!(
            codex_calls.trim().is_empty(),
            "relogin test should not invoke real codex login in this failure path; calls:\n{codex_calls}"
        );

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
    restore_env("CODEX_ROTATE_CODEX_BIN", previous_codex_bin);
    restore_env("CODEX_ROTATE_ACCOUNT_FLOW_FILE", previous_account_flow_file);
}

#[test]
fn relogin_host_switches_persona_and_restores() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    let previous_codex_bin = std::env::var_os("CODEX_ROTATE_CODEX_BIN");
    let previous_account_flow_file = std::env::var_os("CODEX_ROTATE_ACCOUNT_FLOW_FILE");
    let invalid_account_flow = temp.path().join("missing-workflow.yaml");
    let fake_codex_log = temp.path().join("fake-codex.log");
    let fake_codex_bin = temp.path().join("bin").join("codex");
    write_fake_codex_bin(&fake_codex_bin, &fake_codex_log);
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
        std::env::set_var("CODEX_ROTATE_CODEX_BIN", &fake_codex_bin);
        std::env::set_var("CODEX_ROTATE_ACCOUNT_FLOW_FILE", &invalid_account_flow);
    }

    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");
    ensure_live_root_bindings(&paths, &source).expect("bind source roots");

    let pool = codex_rotate_core::pool::Pool {
        active_index: 0,
        accounts: vec![source.clone(), target.clone()],
    };
    codex_rotate_core::pool::save_pool(&pool).expect("save pool");
    fs::write(
        paths.rotate_home.join("accounts.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "version": 9,
            "pending": {
                target.email.clone(): {
                    "stored": {
                        "email": target.email.clone(),
                        "profile_name": "persona-target",
                        "template": "acct-target@astronlab.com",
                        "suffix": 1,
                        "selector": target.label.clone(),
                        "alias": null,
                        "birth_month": 1,
                        "birth_day": 24,
                        "birth_year": 1990,
                        "created_at": "2026-04-13T02:52:15.012Z",
                        "updated_at": "2026-04-13T02:52:15.012Z"
                    },
                    "started_at": "2026-04-13T02:52:15.012Z"
                }
            }
        }))
        .expect("serialize credential store"),
    )
    .expect("write credential store");

    // Verify initial state
    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).unwrap());

    let result = relogin_host(9333, "acct-target", ReloginOptions::default(), None);
    assert!(
        result.is_err(),
        "relogin should fail before browser automation starts"
    );
    let codex_calls = fs::read_to_string(&fake_codex_log).unwrap_or_default();
    assert!(
        codex_calls.trim().is_empty(),
        "host relogin test should not invoke real codex login in this failure path; calls:\n{codex_calls}"
    );

    // Verify restoration after failure/success
    assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).unwrap());

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
    restore_env("CODEX_ROTATE_CODEX_BIN", previous_codex_bin);
    restore_env("CODEX_ROTATE_ACCOUNT_FLOW_FILE", previous_account_flow_file);
}

#[test]
fn wait_for_all_threads_idle_reports_unavailable_app_server() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
    }

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

    let unavailable_listener = TcpListener::bind("127.0.0.1:0").expect("reserve unavailable port");
    let unavailable_port = unavailable_listener
        .local_addr()
        .expect("unavailable port")
        .port();
    drop(unavailable_listener);

    let result = wait_for_all_threads_idle(unavailable_port, None);
    assert!(result.is_err());
    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("CDP endpoint failed")
            || message.contains("No Codex page target")
            || message.contains("Failed to query")
            || message.contains("only supported on macOS")
            || message.contains("Managed Codex launch")
    );

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
}

#[test]
fn managed_codex_detection_clears_after_process_exit() {
    let temp = tempdir().expect("tempdir");
    let profile_dir = temp.path().join("managed-profile");
    fs::create_dir_all(&profile_dir).expect("create profile");
    fs::write(profile_dir.join("stale.log"), "stale").expect("write stale state");

    let process_guard = ProcessTracker::new()
        .expect("create process tracker")
        .leak_guard("managed codex detection cleanup");
    let process = ManagedCodexProcess::start(&profile_dir).expect("start managed codex");
    process_guard.record_test_owned_process(process.pid(), "managed-codex", process.command());
    assert!(managed_codex_is_running(&profile_dir).expect("detect running codex"));

    drop(process);
    let mut stopped = false;
    for _ in 0..20 {
        if !managed_codex_is_running(&profile_dir).expect("detect stopped codex") {
            stopped = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(stopped, "managed codex process should stop cleanly");
    stop_managed_codex_instance(9333, &profile_dir).expect("stop should be a no-op");
}

#[test]
fn managed_codex_stop_helper_terminates_running_instance() {
    let temp = tempdir().expect("tempdir");
    let profile_dir = temp.path().join("managed-profile");
    fs::create_dir_all(&profile_dir).expect("create profile");

    let process_guard = ProcessTracker::new()
        .expect("create process tracker")
        .leak_guard("managed codex stop cleanup");
    let process = ManagedCodexProcess::start(&profile_dir).expect("start managed codex");
    process_guard.record_test_owned_process(process.pid(), "managed-codex", process.command());
    assert!(managed_codex_is_running(&profile_dir).expect("detect running codex"));

    stop_managed_codex_instance(9333, &profile_dir).expect("stop running codex");
    let mut stopped = false;
    for _ in 0..20 {
        if !managed_codex_is_running(&profile_dir).expect("detect stopped codex") {
            stopped = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        stopped,
        "managed codex process should stop after stop helper"
    );

    drop(process);
    process_guard
        .assert_clean()
        .expect("managed codex should exit cleanly");
}
