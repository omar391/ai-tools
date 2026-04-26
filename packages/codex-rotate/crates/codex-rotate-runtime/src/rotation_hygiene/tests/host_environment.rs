use super::*;

#[test]
fn rotation_phase_labels_are_actionable() {
    assert_eq!(RotationPhase::Prepare.to_string(), "prepare");
    assert_eq!(RotationPhase::Activate.to_string(), "activate");
    assert_eq!(RotationPhase::Rollback.to_string(), "rollback");
}

#[test]
fn lineage_sync_contract_states_unique_ids_and_additive_sync() {
    assert!(LINEAGE_SYNC_CONTRACT.contains("API handoff sync creates different local thread IDs"));
    assert!(LINEAGE_SYNC_CONTRACT.contains("First materialization uses API handoff/import"));
    assert!(LINEAGE_SYNC_CONTRACT.contains(
            "one local thread per lineage per persona with no duplicate logical conversations on repeated sync"
        ));
    assert!(LINEAGE_SYNC_CONTRACT.contains("codex-rotate-vm"));
}

#[test]
fn current_environment_defaults_to_host_from_state() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let rotate_home = tempdir().expect("tempdir");
    let previous = std::env::var_os("CODEX_ROTATE_ENVIRONMENT");
    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::remove_var("CODEX_ROTATE_ENVIRONMENT");
        std::env::set_var("CODEX_ROTATE_HOME", rotate_home.path());
    }
    fs::write(
        rotate_home.path().join("accounts.json"),
        serde_json::to_string(&json!({
            "accounts": [],
            "active_index": 0,
        }))
        .expect("serialize state"),
    )
    .expect("write state");

    let environment = current_environment().expect("current environment");
    restore_env("CODEX_ROTATE_ENVIRONMENT", previous);
    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    assert_eq!(environment, RotationEnvironment::Host);
}

#[test]
fn current_environment_env_override_wins() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let rotate_home = tempdir().expect("tempdir");
    let previous = std::env::var_os("CODEX_ROTATE_ENVIRONMENT");
    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", rotate_home.path());
    }

    fs::write(
        rotate_home.path().join("accounts.json"),
        serde_json::to_string(&json!({
            "accounts": [],
            "active_index": 0,
            "environment": "vm",
            "vm": {
                "basePackagePath": "/vm/base.utm",
                "personaRoot": "/vm/personas",
                "utmAppPath": "/Applications/UTM.app"
            }
        }))
        .expect("serialize state"),
    )
    .expect("write state");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", rotate_home.path());
        std::env::set_var("CODEX_ROTATE_ENVIRONMENT", "host");
    }

    let environment = current_environment().expect("current environment");
    restore_env("CODEX_ROTATE_ENVIRONMENT", previous);
    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    assert_eq!(environment, RotationEnvironment::Host);
}

#[test]
fn live_root_migration_moves_directory_into_persona_and_links_it() {
    let temp = tempdir().expect("tempdir");
    let live = temp.path().join(".codex");
    let target = temp
        .path()
        .join("personas")
        .join("host")
        .join("acct")
        .join("codex-home");
    fs::create_dir_all(&live).expect("create live root");
    fs::write(live.join("config.toml"), "model = \"gpt-5\"\n").expect("write config");

    migrate_live_root_if_needed(&live, &target).expect("migrate root");
    ensure_symlink_dir(&live, &target).expect("link root");

    assert!(target.join("config.toml").exists());
    assert!(is_symlink_to(&live, &target).expect("check symlink"));
}

#[test]
fn live_root_migration_resumes_when_target_directory_is_empty() {
    let temp = tempdir().expect("tempdir");
    let live = temp.path().join(".codex");
    let target = temp
        .path()
        .join("personas")
        .join("host")
        .join("acct")
        .join("codex-home");
    fs::create_dir_all(&live).expect("create live root");
    fs::write(live.join("config.toml"), "model = \"gpt-5\"\n").expect("write config");
    fs::create_dir_all(&target).expect("create empty target");

    migrate_live_root_if_needed(&live, &target).expect("resume migration");
    ensure_symlink_dir(&live, &target).expect("link root");

    assert!(target.join("config.toml").exists());
    assert!(is_symlink_to(&live, &target).expect("check symlink"));
}

#[test]
fn ensure_symlink_dir_repairs_broken_symlink() {
    let temp = tempdir().expect("tempdir");
    let live = temp.path().join(".codex");
    let target = temp.path().join("personas").join("host").join("acct");
    fs::create_dir_all(&target).expect("create target");
    #[cfg(unix)]
    std::os::unix::fs::symlink(temp.path().join("missing"), &live).expect("create broken symlink");
    #[cfg(windows)]
    std::os::windows::fs::symlink_dir(temp.path().join("missing"), &live)
        .expect("create broken symlink");

    ensure_symlink_dir(&live, &target).expect("repair symlink");
    assert!(is_symlink_to(&live, &target).expect("check repaired symlink"));
}

#[test]
fn finalize_rotation_after_import_commits_pool_after_complete_import() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", temp.path());
    }

    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");
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

    let import_outcome = ThreadHandoffImportOutcome {
        completed_source_thread_ids: vec!["thread-source".to_string()],
        failures: Vec::new(),
        prevented_duplicates_count: 0,
    };

    finalize_rotation_after_import(&prepared, &import_outcome).expect("finalize import");

    let committed_pool = load_pool().expect("load committed pool");
    assert_eq!(committed_pool.active_index, 1);

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}

#[test]
fn finalize_rotation_after_import_rejects_partial_import_without_committing_pool() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", temp.path());
    }

    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");
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

    let import_outcome = ThreadHandoffImportOutcome {
        completed_source_thread_ids: vec!["thread-source".to_string()],
        failures: vec![ThreadHandoffImportFailure {
            source_thread_id: "thread-source".to_string(),
            created_thread_id: Some("thread-target".to_string()),
            stage: ThreadHandoffImportFailureStage::InjectItems,
            error: "permission denied".to_string(),
        }],
        prevented_duplicates_count: 0,
    };

    let error = finalize_rotation_after_import(&prepared, &import_outcome)
        .expect_err("partial import should fail");
    assert!(error.to_string().contains("Partial thread handoff import"));

    let committed_pool = load_pool().expect("load committed pool");
    assert_eq!(committed_pool.active_index, 0);

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}

#[test]
fn recover_incomplete_rotation_state_repairs_target_authoritative_checkpoint() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    let previous_fast_browser_home = std::env::var_os("FAST_BROWSER_HOME");
    let previous_codex_app_support = std::env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
        std::env::set_var("FAST_BROWSER_HOME", paths.fast_browser_home.clone());
        std::env::set_var(
            "CODEX_ROTATE_CODEX_APP_SUPPORT",
            paths.codex_app_support_dir.clone(),
        );
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
    codex_rotate_core::pool::save_pool(&pool).expect("save pool");

    codex_rotate_core::pool::save_rotation_checkpoint(Some(&RotationCheckpoint {
        phase: RotationCheckpointPhase::Import,
        previous_index: 0,
        target_index: 1,
        previous_account_id: source.account_id.clone(),
        target_account_id: target.account_id.clone(),
    }))
    .expect("save checkpoint");

    switch_host_persona(&paths, &source, &target, false).expect("switch persona");
    codex_rotate_core::pool::write_selected_account_auth(&target).expect("write target auth");

    recover_incomplete_rotation_state().expect("recover rotation");

    let recovered_pool = load_pool().expect("load recovered pool");
    assert_eq!(recovered_pool.active_index, 1);
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();
    assert!(is_symlink_to(&paths.codex_home, &target_paths.codex_home).unwrap());
    assert!(is_symlink_to(&paths.debug_profile_dir, &target_paths.debug_profile_dir).unwrap());
    let recovered_auth =
        codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file).expect("load auth");
    assert_eq!(recovered_auth.tokens.account_id, "acct-target");
    assert!(load_rotation_checkpoint()
        .expect("load checkpoint")
        .is_none());

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
    restore_env("FAST_BROWSER_HOME", previous_fast_browser_home);
    restore_env("CODEX_ROTATE_CODEX_APP_SUPPORT", previous_codex_app_support);
}

#[test]
fn recover_incomplete_rotation_state_clears_source_authoritative_checkpoint() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    let previous_fast_browser_home = std::env::var_os("FAST_BROWSER_HOME");
    let previous_codex_app_support = std::env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
        std::env::set_var("FAST_BROWSER_HOME", paths.fast_browser_home.clone());
        std::env::set_var(
            "CODEX_ROTATE_CODEX_APP_SUPPORT",
            paths.codex_app_support_dir.clone(),
        );
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
    codex_rotate_core::pool::save_pool(&pool).expect("save pool");

    codex_rotate_core::pool::save_rotation_checkpoint(Some(&RotationCheckpoint {
        phase: RotationCheckpointPhase::Prepare,
        previous_index: 0,
        target_index: 1,
        previous_account_id: source.account_id.clone(),
        target_account_id: target.account_id.clone(),
    }))
    .expect("save checkpoint");

    recover_incomplete_rotation_state().expect("recover rotation");

    let recovered_pool = load_pool().expect("load recovered pool");
    assert_eq!(recovered_pool.active_index, 0);
    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).unwrap());
    assert!(is_symlink_to(&paths.debug_profile_dir, &source_paths.debug_profile_dir).unwrap());
    let recovered_auth =
        codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file).expect("load auth");
    assert_eq!(recovered_auth.tokens.account_id, "acct-source");
    assert!(load_rotation_checkpoint()
        .expect("load checkpoint")
        .is_none());

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
    restore_env("FAST_BROWSER_HOME", previous_fast_browser_home);
    restore_env("CODEX_ROTATE_CODEX_APP_SUPPORT", previous_codex_app_support);
}

#[test]
fn recover_incomplete_rotation_state_activate_checkpoint_requires_managed_profile_match() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    let previous_fast_browser_home = std::env::var_os("FAST_BROWSER_HOME");
    let previous_codex_app_support = std::env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
        std::env::set_var("FAST_BROWSER_HOME", paths.fast_browser_home.clone());
        std::env::set_var(
            "CODEX_ROTATE_CODEX_APP_SUPPORT",
            paths.codex_app_support_dir.clone(),
        );
    }

    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");
    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");
    ensure_live_root_bindings(&paths, &source).expect("bind source roots");

    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();

    fs::create_dir_all(paths.codex_auth_file.parent().unwrap()).expect("create auth parent");
    codex_rotate_core::auth::write_codex_auth(&paths.codex_auth_file, &source.auth)
        .expect("write source auth");

    let pool = codex_rotate_core::pool::Pool {
        active_index: 0,
        accounts: vec![source.clone(), target.clone()],
    };
    codex_rotate_core::pool::save_pool(&pool).expect("save pool");

    codex_rotate_core::pool::save_rotation_checkpoint(Some(&RotationCheckpoint {
        phase: RotationCheckpointPhase::Activate,
        previous_index: 0,
        target_index: 1,
        previous_account_id: source.account_id.clone(),
        target_account_id: target.account_id.clone(),
    }))
    .expect("save checkpoint");

    switch_host_persona(&paths, &source, &target, false).expect("switch persona");
    codex_rotate_core::pool::write_selected_account_auth(&target).expect("write target auth");
    ensure_symlink_dir(&paths.debug_profile_dir, &source_paths.debug_profile_dir)
        .expect("misbind managed profile root");

    recover_incomplete_rotation_state().expect("recover rotation");

    let recovered_pool = load_pool().expect("load recovered pool");
    assert_eq!(recovered_pool.active_index, 0);
    assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).unwrap());
    assert!(is_symlink_to(
        &paths.codex_app_support_dir,
        &source_paths.codex_app_support_dir
    )
    .unwrap());
    assert!(is_symlink_to(&paths.debug_profile_dir, &source_paths.debug_profile_dir).unwrap());
    assert!(!is_symlink_to(&paths.codex_home, &target_paths.codex_home).unwrap());
    let recovered_auth =
        codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file).expect("load auth");
    assert_eq!(recovered_auth.tokens.account_id, "acct-source");
    assert!(load_rotation_checkpoint()
        .expect("load checkpoint")
        .is_none());

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
    restore_env("FAST_BROWSER_HOME", previous_fast_browser_home);
    restore_env("CODEX_ROTATE_CODEX_APP_SUPPORT", previous_codex_app_support);
}

#[test]
fn ensure_symlink_dir_restores_original_link_when_replacement_is_denied() {
    let temp = tempdir().expect("tempdir");
    let live = temp.path().join(".codex");
    let original_target = temp.path().join("original");
    let replacement_target = temp.path().join("replacement");
    fs::create_dir_all(&original_target).expect("create original target");
    fs::create_dir_all(&replacement_target).expect("create replacement target");

    symlink_dir(&original_target, &live).expect("create original symlink");

    let mut attempts = 0;
    let result = ensure_symlink_dir_with(&live, &replacement_target, |target, link| {
        attempts += 1;
        if attempts == 1 {
            Err(io::Error::new(
                ErrorKind::PermissionDenied,
                "permission denied",
            ))
        } else {
            symlink_dir(target, link)
        }
    });

    let error = result.expect_err("replacement should fail");
    assert!(error
        .to_string()
        .contains("Permission denied while replacing symlink"));
    assert_eq!(attempts, 2);
    assert!(is_symlink_to(&live, &original_target).expect("original symlink restored"));
}

#[test]
fn vm_environment_reports_runtime_boundary() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let previous_environment = std::env::var_os("CODEX_ROTATE_ENVIRONMENT");
    let temp = tempdir().expect("tempdir");
    let vm_base = temp.path().join("base.utm");
    let vm_personas = temp.path().join("personas");
    let vm_utm = temp.path().join("UTM.app");
    fs::create_dir_all(&vm_base).expect("create vm base");
    fs::write(vm_base.join("config.plist"), "base").expect("write base config");
    fs::create_dir_all(&vm_personas).expect("create vm personas");
    fs::create_dir_all(&vm_utm).expect("create vm app");

    unsafe {
        std::env::set_var("CODEX_ROTATE_ENVIRONMENT", "vm");
    }

    let env = current_environment().expect("resolve environment");
    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", temp.path());
    }
    fs::write(
        temp.path().join("accounts.json"),
        serde_json::to_string(&json!({
            "accounts": [],
            "active_index": 0,
            "environment": "vm",
            "vm": {
                "basePackagePath": vm_base,
                "personaRoot": vm_personas,
                "utmAppPath": vm_utm,
            }
        }))
        .expect("serialize vm state"),
    )
    .expect("write vm state");
    let backend_error = match select_rotation_backend() {
        Ok(_) => panic!("vm backend is outside runtime"),
        Err(error) => error,
    };

    restore_env("CODEX_ROTATE_ENVIRONMENT", previous_environment);
    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);

    assert_eq!(env, RotationEnvironment::Vm);
    assert!(backend_error.to_string().contains("codex-rotate-vm crate"));
}
