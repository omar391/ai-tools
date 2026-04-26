use super::*;

#[test]
fn switch_host_persona_does_not_resurrect_deleted_projects_from_thread_history() {
    let temp = tempfile::Builder::new()
        .prefix("codex-rotate-project-removal-")
        .tempdir_in(std::env::current_dir().expect("current dir"))
        .expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    provision_host_persona(&paths, &source, None).expect("provision source");

    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();
    fs::create_dir_all(&target_paths.codex_home).expect("create stale target codex home");

    let removed_project = temp.path().join("projects/shared-visible");
    fs::create_dir_all(&removed_project).expect("create removed project");
    let status = Command::new("git")
        .arg("init")
        .arg("-q")
        .arg(&removed_project)
        .status()
        .expect("git init project");
    assert!(status.success(), "git init failed: {status}");

    fs::write(
        source_paths.codex_home.join("config.toml"),
        "model = \"gpt-5.3-codex\"\n",
    )
    .expect("write source config");
    fs::write(
        target_paths.codex_home.join("config.toml"),
        format!(
            "[projects.\"{}\"]\ntrust_level = \"trusted\"\n",
            encode_toml_basic_string(&removed_project.display().to_string())
        ),
    )
    .expect("write target config");

    seed_threads_table(
        &source_paths.codex_home.join("state_5.sqlite"),
        &[("thread-source", "/tmp/source.jsonl", 100)],
    );
    seed_threads_table(
        &target_paths.codex_home.join("state_5.sqlite"),
        &[("thread-target", "/tmp/target.jsonl", 200)],
    );
    update_thread_metadata(
        &source_paths.codex_home.join("state_5.sqlite"),
        "thread-source",
        &removed_project.display().to_string(),
        false,
    );
    update_thread_metadata(
        &target_paths.codex_home.join("state_5.sqlite"),
        "thread-target",
        &removed_project.display().to_string(),
        false,
    );

    fs::write(
        source_paths.codex_home.join(CODEX_GLOBAL_STATE_FILE_NAME),
        serde_json::to_string(&json!({
            "selected-remote-host-id": "local",
            SAVED_WORKSPACE_ROOTS_KEY: [],
            PROJECT_ORDER_KEY: [],
            ACTIVE_WORKSPACE_ROOTS_KEY: [],
        }))
        .expect("serialize source global state"),
    )
    .expect("write source global state");
    fs::write(
        target_paths.codex_home.join(CODEX_GLOBAL_STATE_FILE_NAME),
        serde_json::to_string(&json!({
            SAVED_WORKSPACE_ROOTS_KEY: [removed_project.display().to_string()],
            PROJECT_ORDER_KEY: [removed_project.display().to_string()],
            ACTIVE_WORKSPACE_ROOTS_KEY: [removed_project.display().to_string()],
        }))
        .expect("serialize target global state"),
    )
    .expect("write target global state");

    ensure_live_root_bindings(&paths, &source).expect("bind source");
    switch_host_persona(&paths, &source, &target, false).expect("switch");
    switch_host_persona(&paths, &target, &source, false).expect("switch back");

    assert!(
        !fs::symlink_metadata(source_paths.codex_home.join("config.toml"))
            .expect("source config metadata")
            .file_type()
            .is_symlink()
    );
    assert!(
        !fs::symlink_metadata(target_paths.codex_home.join("config.toml"))
            .expect("target config metadata")
            .file_type()
            .is_symlink()
    );

    for config_path in [
        source_paths.codex_home.join("config.toml"),
        target_paths.codex_home.join("config.toml"),
    ] {
        let config = fs::read_to_string(&config_path).expect("read config");
        assert!(
            !config.contains(&project_table_heading(&removed_project)),
            "did not expect {} to contain removed project {}",
            config_path.display(),
            removed_project.display()
        );
    }

    for state_path in [
        source_paths.codex_home.join(CODEX_GLOBAL_STATE_FILE_NAME),
        target_paths.codex_home.join(CODEX_GLOBAL_STATE_FILE_NAME),
    ] {
        let state: Value = serde_json::from_str(
            &fs::read_to_string(&state_path).expect("read workspace visibility state"),
        )
        .expect("parse workspace visibility state");
        for key in [
            SAVED_WORKSPACE_ROOTS_KEY,
            PROJECT_ORDER_KEY,
            ACTIVE_WORKSPACE_ROOTS_KEY,
        ] {
            let values = state
                .get(key)
                .and_then(Value::as_array)
                .expect("workspace visibility array")
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>();
            assert!(
                !values.contains(&removed_project.display().to_string().as_str()),
                "did not expect {key} in {} to contain removed project {}",
                state_path.display(),
                removed_project.display()
            );
        }
    }

    let expected_project = removed_project.display().to_string();
    assert!(
        read_thread_cwds_from_state_db(&source_paths.codex_home.join("state_5.sqlite"))
            .expect("read source thread cwd values")
            .contains(&expected_project)
    );
    assert!(
        read_thread_cwds_from_state_db(&target_paths.codex_home.join("state_5.sqlite"))
            .expect("read target thread cwd values")
            .contains(&expected_project)
    );
}

#[test]
fn switch_host_persona_syncs_config_projects_from_source_only() {
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    provision_host_persona(&paths, &source, None).expect("provision source");

    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();
    fs::create_dir_all(&target_paths.codex_home).expect("create stale target codex home");

    let source_project = temp.path().join("projects/source-visible");
    let target_project = temp.path().join("projects/target-visible");
    let archived_backfill_project = temp.path().join("projects/archived-backfill");
    let missing_project = temp.path().join("projects/missing-project");
    fs::create_dir_all(&source_project).expect("create source project");
    fs::create_dir_all(&target_project).expect("create target project");
    fs::create_dir_all(&archived_backfill_project).expect("create archived project");
    for project in [&source_project, &target_project, &archived_backfill_project] {
        let status = Command::new("git")
            .arg("init")
            .arg("-q")
            .arg(project)
            .status()
            .expect("git init project");
        assert!(status.success(), "git init failed: {status}");
    }

    fs::write(
        source_paths.codex_home.join("config.toml"),
        format!(
            "model = \"gpt-5.3-codex\"\n\n[projects.\"{}\"]\ntrust_level = \"trusted\"\n",
            encode_toml_basic_string(&source_project.display().to_string())
        ),
    )
    .expect("write source config");
    fs::write(
            target_paths.codex_home.join("config.toml"),
            format!(
                "personality = \"pragmatic\"\n\n[plugins.\"computer-use@openai-bundled\"]\nenabled = true\n\n[projects.\"{}\"]\ntrust_level = \"trusted\"\n",
                encode_toml_basic_string(&target_project.display().to_string())
            ),
        )
        .expect("write target config");

    seed_threads_table(
        &source_paths.codex_home.join("state_5.sqlite"),
        &[
            ("thread-source", "/tmp/source.jsonl", 100),
            ("thread-archived-backfill", "/tmp/backfill.jsonl", 101),
            ("thread-missing", "/tmp/missing.jsonl", 102),
        ],
    );
    update_thread_metadata(
        &source_paths.codex_home.join("state_5.sqlite"),
        "thread-source",
        &source_project.display().to_string(),
        false,
    );
    update_thread_metadata(
        &source_paths.codex_home.join("state_5.sqlite"),
        "thread-archived-backfill",
        &archived_backfill_project.display().to_string(),
        true,
    );
    update_thread_metadata(
        &source_paths.codex_home.join("state_5.sqlite"),
        "thread-missing",
        &missing_project.display().to_string(),
        false,
    );

    seed_threads_table(
        &target_paths.codex_home.join("state_5.sqlite"),
        &[("thread-target", "/tmp/target.jsonl", 200)],
    );
    update_thread_metadata(
        &target_paths.codex_home.join("state_5.sqlite"),
        "thread-target",
        &target_project.display().to_string(),
        false,
    );

    ensure_live_root_bindings(&paths, &source).expect("bind source");
    switch_host_persona(&paths, &source, &target, false).expect("switch");

    let source_config =
        fs::read_to_string(source_paths.codex_home.join("config.toml")).expect("source config");
    let target_config =
        fs::read_to_string(target_paths.codex_home.join("config.toml")).expect("target config");
    assert_eq!(source_config, target_config);
    assert!(
        !fs::symlink_metadata(target_paths.codex_home.join("config.toml"))
            .expect("target config metadata")
            .file_type()
            .is_symlink()
    );

    assert!(source_config.contains("model = \"gpt-5.4\""));
    assert!(source_config.contains("model_reasoning_effort = \"xhigh\""));
    assert!(!source_config.contains("[plugins.\"computer-use@openai-bundled\"]"));
    assert!(target_config.contains("model = \"gpt-5.4\""));
    assert!(target_config.contains("model_reasoning_effort = \"xhigh\""));
    assert!(!target_config.contains("[plugins.\"computer-use@openai-bundled\"]"));

    assert!(source_config.contains(&project_table_heading(&source_project)));
    assert!(!source_config.contains(&project_table_heading(&target_project)));
    assert!(!source_config.contains(&project_table_heading(&archived_backfill_project)));
    assert!(!source_config.contains(&project_table_heading(&missing_project)));

    assert!(target_config.contains(&project_table_heading(&source_project)));
    assert!(!target_config.contains(&project_table_heading(&target_project)));
    assert!(!target_config.contains(&project_table_heading(&archived_backfill_project)));
    assert!(!target_config.contains(&project_table_heading(&missing_project)));
}

#[test]
fn switch_host_persona_keeps_workspace_visibility_in_shared_state_only() {
    let temp = tempfile::Builder::new()
        .prefix("codex-rotate-visibility-sync-")
        .tempdir_in(std::env::current_dir().expect("current dir"))
        .expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    provision_host_persona(&paths, &source, None).expect("provision source");

    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();
    fs::create_dir_all(&target_paths.codex_home).expect("create stale target codex home");

    let source_project = temp.path().join("projects/source-visible");
    let target_project = temp.path().join("projects/target-visible");
    let archived_backfill_project = temp.path().join("projects/archived-backfill");
    let missing_project = temp.path().join("projects/missing-project");
    fs::create_dir_all(&source_project).expect("create source project");
    fs::create_dir_all(&target_project).expect("create target project");
    fs::create_dir_all(&archived_backfill_project).expect("create archived project");
    for project in [&source_project, &target_project, &archived_backfill_project] {
        let status = Command::new("git")
            .arg("init")
            .arg("-q")
            .arg(project)
            .status()
            .expect("git init project");
        assert!(status.success(), "git init failed: {status}");
    }

    fs::write(
        source_paths.codex_home.join("config.toml"),
        format!(
            "[projects.\"{}\"]\ntrust_level = \"trusted\"\n",
            encode_toml_basic_string(&source_project.display().to_string())
        ),
    )
    .expect("write source config");
    fs::write(
        target_paths.codex_home.join("config.toml"),
        format!(
            "[projects.\"{}\"]\ntrust_level = \"trusted\"\n",
            encode_toml_basic_string(&target_project.display().to_string())
        ),
    )
    .expect("write target config");

    seed_threads_table(
        &source_paths.codex_home.join("state_5.sqlite"),
        &[
            ("thread-source", "/tmp/source.jsonl", 100),
            ("thread-archived-backfill", "/tmp/backfill.jsonl", 101),
            ("thread-missing", "/tmp/missing.jsonl", 102),
        ],
    );
    update_thread_metadata(
        &source_paths.codex_home.join("state_5.sqlite"),
        "thread-source",
        &source_project.display().to_string(),
        false,
    );
    update_thread_metadata(
        &source_paths.codex_home.join("state_5.sqlite"),
        "thread-archived-backfill",
        &archived_backfill_project.display().to_string(),
        true,
    );
    update_thread_metadata(
        &source_paths.codex_home.join("state_5.sqlite"),
        "thread-missing",
        &missing_project.display().to_string(),
        false,
    );

    seed_threads_table(
        &target_paths.codex_home.join("state_5.sqlite"),
        &[("thread-target", "/tmp/target.jsonl", 200)],
    );
    update_thread_metadata(
        &target_paths.codex_home.join("state_5.sqlite"),
        "thread-target",
        &target_project.display().to_string(),
        false,
    );

    fs::write(
        source_paths.codex_home.join(CODEX_GLOBAL_STATE_FILE_NAME),
        serde_json::to_string(&json!({
            "selected-remote-host-id": "local",
            SAVED_WORKSPACE_ROOTS_KEY: [source_project.display().to_string()],
            PROJECT_ORDER_KEY: [source_project.display().to_string()],
            ACTIVE_WORKSPACE_ROOTS_KEY: [source_project.display().to_string()],
        }))
        .expect("serialize source global state"),
    )
    .expect("write source global state");
    fs::write(
        target_paths.codex_home.join(CODEX_GLOBAL_STATE_FILE_NAME),
        serde_json::to_string(&json!({
            "electron-main-window-bounds": {"x": 1, "y": 2},
            SAVED_WORKSPACE_ROOTS_KEY: [target_project.display().to_string()],
            PROJECT_ORDER_KEY: [target_project.display().to_string()],
            ACTIVE_WORKSPACE_ROOTS_KEY: [],
        }))
        .expect("serialize target global state"),
    )
    .expect("write target global state");

    ensure_live_root_bindings(&paths, &source).expect("bind source");
    switch_host_persona(&paths, &source, &target, false).expect("switch");

    let expected_projects = vec![source_project.display().to_string()];
    let missing_project_string = missing_project.display().to_string();
    for state_path in [
        source_paths.codex_home.join(CODEX_GLOBAL_STATE_FILE_NAME),
        target_paths.codex_home.join(CODEX_GLOBAL_STATE_FILE_NAME),
    ] {
        let state: Value = serde_json::from_str(
            &fs::read_to_string(&state_path).expect("read workspace visibility state"),
        )
        .expect("parse workspace visibility state");
        let saved_roots = state
            .get(SAVED_WORKSPACE_ROOTS_KEY)
            .and_then(Value::as_array)
            .expect("saved roots array")
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect::<Vec<_>>();
        assert_eq!(
            saved_roots,
            expected_projects,
            "expected saved roots in {} to stay on shared active state",
            state_path.display()
        );
        assert!(
            !saved_roots.contains(&target_project.display().to_string()),
            "did not expect target-only project {} in saved roots {}",
            target_project.display(),
            state_path.display()
        );
        assert!(
            !saved_roots.contains(&archived_backfill_project.display().to_string()),
            "did not expect thread cwd history to recreate archived project {} in saved roots {}",
            archived_backfill_project.display(),
            state_path.display()
        );
        assert!(
            !saved_roots.contains(&missing_project_string),
            "did not expect missing project {} in saved roots {}",
            missing_project.display(),
            state_path.display()
        );
    }

    let source_state: Value = serde_json::from_str(
        &fs::read_to_string(source_paths.codex_home.join(CODEX_GLOBAL_STATE_FILE_NAME))
            .expect("read source global state"),
    )
    .expect("parse source global state");
    assert_eq!(
        source_state
            .get("selected-remote-host-id")
            .and_then(Value::as_str),
        Some("local")
    );
    assert_eq!(
        source_state
            .get(ACTIVE_WORKSPACE_ROOTS_KEY)
            .and_then(Value::as_array)
            .expect("source active roots")
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect::<Vec<_>>(),
        vec![source_project.display().to_string()]
    );

    let target_state: Value = serde_json::from_str(
        &fs::read_to_string(target_paths.codex_home.join(CODEX_GLOBAL_STATE_FILE_NAME))
            .expect("read target global state"),
    )
    .expect("parse target global state");
    assert_eq!(
        target_state
            .get("selected-remote-host-id")
            .and_then(Value::as_str),
        Some("local")
    );
    assert!(target_state.get("electron-main-window-bounds").is_none());
    assert_eq!(
        target_state
            .get(ACTIVE_WORKSPACE_ROOTS_KEY)
            .and_then(Value::as_array)
            .expect("target active roots")
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect::<Vec<_>>(),
        vec![source_project.display().to_string()]
    );
}

#[test]
fn normalize_workspace_visibility_path_filters_noise_and_canonicalizes_repo_roots() {
    let temp = tempfile::Builder::new()
        .prefix("codex-rotate-visibility-normalize-")
        .tempdir_in(std::env::current_dir().expect("current dir"))
        .expect("tempdir");
    let workspace_root = temp.path();

    let repo_root = workspace_root.join("ai-rules");
    let nested_project = repo_root.join("skills").join("domain-evaluator");
    let repo_local_worktree = repo_root.join("worktrees").join("task-a");
    fs::create_dir_all(&nested_project).expect("create nested project");
    fs::create_dir_all(&repo_local_worktree).expect("create repo-local worktree");
    let status = Command::new("git")
        .arg("init")
        .arg("-q")
        .arg(&repo_root)
        .status()
        .expect("git init repo root");
    assert!(status.success(), "git init failed: {status}");

    let normalized_nested =
        normalize_workspace_visibility_path(nested_project.to_str().expect("nested project path"))
            .expect("normalize nested project");
    assert_eq!(
        normalized_nested,
        Some(repo_root.to_string_lossy().into_owned())
    );

    let normalized_worktree = normalize_workspace_visibility_path(
        repo_local_worktree
            .to_str()
            .expect("repo-local worktree path"),
    )
    .expect("normalize repo-local worktree");
    assert_eq!(
        normalized_worktree,
        Some(repo_root.to_string_lossy().into_owned())
    );

    assert!(
        normalize_workspace_visibility_path("/private/tmp/codex-temp-project")
            .expect("normalize private tmp")
            .is_none()
    );
    assert!(
        normalize_workspace_visibility_path("/Users/omar/.codex-rotate")
            .expect("normalize codex rotate home")
            .is_none()
    );
    assert!(
        normalize_workspace_visibility_path("/Users/omar/Documents/project")
            .expect("normalize documents path")
            .is_none()
    );
    assert!(
        normalize_workspace_visibility_path("/Users/omar/Downloads/project")
            .expect("normalize downloads path")
            .is_none()
    );
}

#[cfg(unix)]
#[test]
fn normalize_workspace_visibility_path_excludes_documents_symlink_before_canonicalize() {
    use std::os::unix::fs::symlink;

    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let previous_home = std::env::var_os("HOME");
    let documents_root = temp.path().join("home").join("Documents");
    let external_project = temp.path().join("outside-project");
    fs::create_dir_all(&documents_root).expect("create documents root");
    fs::create_dir_all(&external_project).expect("create external project");
    let linked_project = documents_root.join("linked-project");
    symlink(&external_project, &linked_project).expect("symlink linked project");

    unsafe {
        std::env::set_var("HOME", temp.path().join("home"));
    }

    let normalized =
        normalize_workspace_visibility_path(linked_project.to_str().expect("linked project path"))
            .expect("normalize linked project");
    assert!(
        normalized.is_none(),
        "documents-root paths should be filtered before canonicalization follows symlinks"
    );

    restore_env("HOME", previous_home);
}

#[test]
fn should_sync_project_path_excludes_documents_root_even_if_known() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let home = temp.path().join("home");
    let previous_home = std::env::var_os("HOME");
    let documents_project = home.join("Documents").join("project");
    let downloads_project = home.join("Downloads").join("project");
    fs::create_dir_all(documents_project.parent().expect("documents parent"))
        .expect("create documents root");
    fs::create_dir_all(downloads_project.parent().expect("downloads parent"))
        .expect("create downloads root");

    let mut known_projects = BTreeSet::new();
    known_projects.insert(documents_project.display().to_string());
    known_projects.insert(downloads_project.display().to_string());

    unsafe {
        std::env::set_var("HOME", &home);
    }

    assert!(!should_sync_project_path(
        &documents_project.display().to_string(),
        &known_projects,
    ));
    assert!(!should_sync_project_path(
        &downloads_project.display().to_string(),
        &known_projects,
    ));

    restore_env("HOME", previous_home);
}

#[test]
fn should_sync_project_path_allows_tmp_projects() {
    let temp = tempdir().expect("tempdir");
    let project = temp.path().join("project");
    fs::create_dir_all(&project).expect("create tmp project");

    let mut known_projects = BTreeSet::new();
    known_projects.insert(project.display().to_string());

    assert!(should_sync_project_path(
        &project.display().to_string(),
        &known_projects,
    ));
}

#[test]
fn transfer_thread_recovery_state_between_accounts_moves_pending_events() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
    }

    let mut watch_state = crate::watch::WatchState::default();
    let source_event = crate::thread_recovery::ThreadRecoveryEvent {
        source_log_id: 42,
        source_ts: 1_717_171_717,
        thread_id: "thread-source".to_string(),
        kind: crate::thread_recovery::ThreadRecoveryKind::QuotaExhausted,
        exhausted_turn_id: Some("turn-1".to_string()),
        exhausted_email: Some("source@example.com".to_string()),
        exhausted_account_id: Some("acct-source".to_string()),
        message: "quota exhausted".to_string(),
        rehydration: None,
    };
    watch_state.set_account_state(
        "acct-source".to_string(),
        crate::watch::AccountWatchState {
            last_thread_recovery_log_id: Some(99),
            thread_recovery_pending: true,
            thread_recovery_pending_events: vec![source_event.clone()],
            thread_recovery_backfill_complete: true,
            ..crate::watch::AccountWatchState::default()
        },
    );
    write_watch_state(&watch_state).expect("write source watch state");

    transfer_thread_recovery_state_between_accounts("acct-source", "acct-target")
        .expect("transfer recovery state");

    let updated = read_watch_state().expect("read updated watch state");
    let source_state = updated.account_state("acct-source");
    let target_state = updated.account_state("acct-target");

    assert_eq!(source_state.last_thread_recovery_log_id, None);
    assert!(!source_state.thread_recovery_pending);
    assert!(source_state.thread_recovery_pending_events.is_empty());
    assert_eq!(target_state.last_thread_recovery_log_id, Some(99));
    assert!(target_state.thread_recovery_pending);
    assert_eq!(
        target_state.thread_recovery_pending_events,
        vec![source_event]
    );
    assert!(target_state.thread_recovery_backfill_complete);

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
}
