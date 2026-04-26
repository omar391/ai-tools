use super::*;

#[test]
fn switch_host_persona_repoints_live_roots_to_target_persona() {
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-team", "persona-source");
    let target = test_account("acct-team", "persona-target");

    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");

    let source_paths = host_persona_paths(&paths, source.persona.as_ref().expect("source persona"))
        .expect("source persona paths");
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().expect("target persona"))
        .expect("target persona paths");
    fs::write(source_paths.codex_home.join("history.jsonl"), "source\n")
        .expect("write source history");

    ensure_live_root_bindings(&paths, &source).expect("bind source roots");
    assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).expect("source symlink"));
    assert!(is_symlink_to(
        &paths.codex_app_support_dir,
        &source_paths.codex_app_support_dir
    )
    .expect("source app-support symlink"));
    assert!(
        is_symlink_to(&paths.debug_profile_dir, &source_paths.debug_profile_dir)
            .expect("source managed-profile symlink")
    );

    switch_host_persona(&paths, &source, &target, false).expect("switch persona");

    assert!(is_symlink_to(&paths.codex_home, &target_paths.codex_home).expect("target symlink"));
    assert!(is_symlink_to(
        &paths.codex_app_support_dir,
        &target_paths.codex_app_support_dir
    )
    .expect("target app-support symlink"));
    assert!(
        is_symlink_to(&paths.debug_profile_dir, &target_paths.debug_profile_dir)
            .expect("target managed-profile symlink")
    );
    assert!(target_paths.codex_app_support_dir.exists());
    assert!(source_paths.codex_home.join("history.jsonl").exists());
}

#[test]
fn switch_host_persona_materializes_missing_target_persona_without_cloning_managed_profile() {
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    provision_host_persona(&paths, &source, None).expect("provision source");

    let source_paths = host_persona_paths(&paths, source.persona.as_ref().expect("source persona"))
        .expect("source persona paths");
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().expect("target persona"))
        .expect("target persona paths");
    assert!(!target_paths.root.exists());

    fs::create_dir_all(&paths.debug_profile_dir).expect("create live managed profile root");
    fs::write(
        paths.debug_profile_dir.join("legacy-profile-state.json"),
        "legacy",
    )
    .expect("write legacy managed profile marker");

    ensure_live_root_bindings(&paths, &source).expect("bind source roots");
    assert!(source_paths
        .debug_profile_dir
        .join("legacy-profile-state.json")
        .exists());
    assert!(
        is_symlink_to(&paths.debug_profile_dir, &source_paths.debug_profile_dir)
            .expect("source managed-profile symlink")
    );
    assert!(!target_paths
        .debug_profile_dir
        .join("legacy-profile-state.json")
        .exists());

    switch_host_persona(&paths, &source, &target, true).expect("switch persona");

    assert!(target_paths.root.exists());
    assert!(target_paths.debug_profile_dir.exists());
    assert!(
        is_symlink_to(&paths.debug_profile_dir, &target_paths.debug_profile_dir)
            .expect("target managed-profile symlink")
    );
    assert!(!target_paths
        .debug_profile_dir
        .join("legacy-profile-state.json")
        .exists());
    assert!(source_paths
        .debug_profile_dir
        .join("legacy-profile-state.json")
        .exists());
}

#[test]
fn provision_host_persona_keeps_fast_browser_home_live_only() {
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");

    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();

    assert!(
        !source_paths.root.join("fast-browser-home").exists(),
        "source persona should not materialize a persona-local fast-browser-home"
    );
    assert!(
        !target_paths.root.join("fast-browser-home").exists(),
        "target persona should not materialize a persona-local fast-browser-home"
    );
    assert!(
        !source_paths.codex_app_support_dir.exists(),
        "source persona should not materialize codex-app-support before activation"
    );
    assert!(
        !target_paths.codex_app_support_dir.exists(),
        "target persona should not materialize codex-app-support before activation"
    );
    assert!(
        !source_paths.root.join("managed-profile").exists(),
        "source persona should not materialize managed-profile before use"
    );
    assert!(
        !target_paths.root.join("managed-profile").exists(),
        "target persona should not materialize managed-profile before use"
    );
}

#[test]
fn switch_host_persona_does_not_seed_missing_target_conversation_state() {
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    provision_host_persona(&paths, &source, None).expect("provision source");
    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();

    seed_threads_table(
        &source_paths.codex_home.join("state_5.sqlite"),
        &[
            (
                "thread-source",
                "/Users/test/.codex/sessions/2026/01/01/rollout-source.jsonl",
                100,
            ),
            (
                "thread-archived",
                "/Users/test/.codex/archived_sessions/rollout-archived.jsonl",
                90,
            ),
        ],
    );

    let source_rollout = source_paths
        .codex_home
        .join("sessions/2026/01/01/rollout-source.jsonl");
    let source_archive = source_paths
        .codex_home
        .join("archived_sessions/rollout-archived.jsonl");
    fs::create_dir_all(source_rollout.parent().unwrap()).expect("create source rollout parent");
    fs::create_dir_all(source_archive.parent().unwrap()).expect("create source archive parent");
    fs::write(&source_rollout, "{\"thread\":\"source\"}\n").expect("write source rollout");
    fs::write(&source_archive, "{\"thread\":\"archived\"}\n").expect("write source archive");
    fs::write(
            source_paths.codex_home.join(SESSION_INDEX_FILE_NAME),
            "{\"id\":\"thread-source\",\"thread_name\":\"source\",\"updated_at\":\"2026-01-01T00:00:00Z\"}\n",
        )
        .expect("write source index");

    ensure_live_root_bindings(&paths, &source).expect("bind source");
    assert!(!target_paths.root.exists());

    switch_host_persona(&paths, &source, &target, true).expect("switch persona");

    assert!(!target_paths.codex_home.join("state_5.sqlite").exists());
    assert!(!target_paths
        .codex_home
        .join("sessions/2026/01/01/rollout-source.jsonl")
        .exists());
    assert!(!target_paths
        .codex_home
        .join("archived_sessions/rollout-archived.jsonl")
        .exists());
    assert!(!target_paths
        .codex_home
        .join(SESSION_INDEX_FILE_NAME)
        .exists());

    let store =
        ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("open sync store");
    assert_eq!(
        store
            .get_lineage_id(&conversation_sync_identity(&source), "thread-source")
            .expect("source binding"),
        None
    );
    assert_eq!(
        store
            .get_local_thread_id(&conversation_sync_identity(&target), "thread-source")
            .expect("target binding"),
        None
    );
}

#[test]
fn switch_host_persona_preserves_existing_target_conversation_state_without_raw_merge() {
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");
    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();

    seed_threads_table(
        &source_paths.codex_home.join("state_5.sqlite"),
        &[
            (
                "thread-source",
                "/Users/test/.codex/sessions/2026/01/01/rollout-source.jsonl",
                100,
            ),
            (
                "thread-archived",
                "/Users/test/.codex/archived_sessions/rollout-archived.jsonl",
                90,
            ),
        ],
    );
    seed_threads_table(
        &target_paths.codex_home.join("state_5.sqlite"),
        &[(
            "thread-target-local",
            "/Users/test/.codex/sessions/2026/01/02/rollout-target.jsonl",
            110,
        )],
    );

    let source_rollout = source_paths
        .codex_home
        .join("sessions/2026/01/01/rollout-source.jsonl");
    let source_archive = source_paths
        .codex_home
        .join("archived_sessions/rollout-archived.jsonl");
    let target_rollout = target_paths
        .codex_home
        .join("sessions/2026/01/02/rollout-target.jsonl");
    fs::create_dir_all(source_rollout.parent().unwrap()).expect("create source rollout parent");
    fs::create_dir_all(source_archive.parent().unwrap()).expect("create source archive parent");
    fs::create_dir_all(target_rollout.parent().unwrap()).expect("create target rollout parent");
    fs::write(&source_rollout, "{\"thread\":\"source\"}\n").expect("write source rollout");
    fs::write(&source_archive, "{\"thread\":\"archived\"}\n").expect("write source archive");
    fs::write(&target_rollout, "{\"thread\":\"target\"}\n").expect("write target rollout");
    fs::write(
            source_paths.codex_home.join(SESSION_INDEX_FILE_NAME),
            concat!(
                "{\"id\":\"thread-source\",\"thread_name\":\"source\",\"updated_at\":\"2026-01-01T00:00:00Z\"}\n",
                "{\"id\":\"thread-archived\",\"thread_name\":\"archived\",\"updated_at\":\"2026-01-01T00:00:01Z\"}\n"
            ),
        )
        .expect("write source index");
    fs::write(
            target_paths.codex_home.join(SESSION_INDEX_FILE_NAME),
            "{\"id\":\"thread-target-local\",\"thread_name\":\"target\",\"updated_at\":\"2026-01-02T00:00:00Z\"}\n",
        )
        .expect("write target index");

    ensure_live_root_bindings(&paths, &source).expect("bind source");
    switch_host_persona(&paths, &source, &target, true).expect("switch persona");

    assert_eq!(
        read_thread_ids(&target_paths.codex_home.join("state_5.sqlite")),
        vec!["thread-target-local".to_string()]
    );
    assert!(!target_paths
        .codex_home
        .join("sessions/2026/01/01/rollout-source.jsonl")
        .exists());
    assert!(!target_paths
        .codex_home
        .join("archived_sessions/rollout-archived.jsonl")
        .exists());
    assert!(target_paths
        .codex_home
        .join("sessions/2026/01/02/rollout-target.jsonl")
        .exists());

    let target_index =
        fs::read_to_string(target_paths.codex_home.join(SESSION_INDEX_FILE_NAME)).unwrap();
    assert!(!target_index.contains("\"thread-source\""));
    assert!(!target_index.contains("\"thread-archived\""));
    assert!(target_index.contains("\"thread-target-local\""));

    let store =
        ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("open sync store");
    let target_sync_identity = conversation_sync_identity(&target);
    assert_eq!(
        store
            .get_local_thread_id(&target_sync_identity, "thread-source")
            .expect("target source binding"),
        None
    );
    assert_eq!(
        store
            .get_local_thread_id(&target_sync_identity, "thread-archived")
            .expect("target archived binding"),
        None
    );
    assert_eq!(
        store
            .get_lineage_id(&target_sync_identity, "thread-target-local")
            .expect("target local lineage"),
        None
    );
}

#[test]
fn switch_host_persona_keeps_thread_ids_and_session_indexes_persona_local() {
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");

    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();

    seed_threads_table(
        &source_paths.codex_home.join("state_5.sqlite"),
        &[(
            "thread-source",
            "/Users/test/.codex/sessions/2026/01/01/rollout-source.jsonl",
            100,
        )],
    );
    seed_threads_table(
        &target_paths.codex_home.join("state_5.sqlite"),
        &[(
            "thread-target",
            "/Users/test/.codex/sessions/2026/01/01/rollout-target.jsonl",
            200,
        )],
    );

    let source_rollout = source_paths
        .codex_home
        .join("sessions/2026/01/01/rollout-source.jsonl");
    let target_rollout = target_paths
        .codex_home
        .join("sessions/2026/01/01/rollout-target.jsonl");
    fs::create_dir_all(source_rollout.parent().unwrap()).expect("create source rollout parent");
    fs::create_dir_all(target_rollout.parent().unwrap()).expect("create target rollout parent");
    fs::write(&source_rollout, "{\"thread\":\"source\"}\n").expect("write source rollout");
    fs::write(&target_rollout, "{\"thread\":\"target\"}\n").expect("write target rollout");

    fs::write(
            source_paths.codex_home.join("session_index.jsonl"),
            "{\"id\":\"thread-source\",\"thread_name\":\"source\",\"updated_at\":\"2026-01-01T00:00:00Z\"}\n",
        )
        .expect("write source index");
    fs::write(
            target_paths.codex_home.join("session_index.jsonl"),
            "{\"id\":\"thread-target\",\"thread_name\":\"target\",\"updated_at\":\"2026-01-02T00:00:00Z\"}\n",
        )
        .expect("write target index");

    ensure_live_root_bindings(&paths, &source).expect("bind source");
    switch_host_persona(&paths, &source, &target, false).expect("switch");

    let source_thread_ids = read_thread_ids(&source_paths.codex_home.join("state_5.sqlite"));
    let target_thread_ids = read_thread_ids(&target_paths.codex_home.join("state_5.sqlite"));
    assert_eq!(source_thread_ids, vec!["thread-source".to_string()]);
    assert_eq!(target_thread_ids, vec!["thread-target".to_string()]);

    assert!(source_paths
        .codex_home
        .join("sessions/2026/01/01/rollout-source.jsonl")
        .exists());
    assert!(!source_paths
        .codex_home
        .join("sessions/2026/01/01/rollout-target.jsonl")
        .exists());
    assert!(!target_paths
        .codex_home
        .join("sessions/2026/01/01/rollout-source.jsonl")
        .exists());
    assert!(target_paths
        .codex_home
        .join("sessions/2026/01/01/rollout-target.jsonl")
        .exists());

    let source_index =
        fs::read_to_string(source_paths.codex_home.join("session_index.jsonl")).unwrap();
    let target_index =
        fs::read_to_string(target_paths.codex_home.join("session_index.jsonl")).unwrap();
    assert!(source_index.contains("\"thread-source\""));
    assert!(!source_index.contains("\"thread-target\""));
    assert!(!target_index.contains("\"thread-source\""));
    assert!(target_index.contains("\"thread-target\""));
}

#[test]
fn switch_host_persona_keeps_app_support_local_storage_persona_local() {
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");

    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();

    let source_leveldb = source_paths
        .codex_app_support_dir
        .join("Local Storage")
        .join("leveldb");
    let target_leveldb = target_paths
        .codex_app_support_dir
        .join("Local Storage")
        .join("leveldb");
    fs::create_dir_all(&source_leveldb).expect("create source local storage leveldb");
    fs::create_dir_all(&target_leveldb).expect("create target local storage leveldb");
    fs::write(
        source_leveldb.join("source-project.marker"),
        "source-local-storage",
    )
    .expect("write source marker");
    fs::write(
        target_leveldb.join("target-project.marker"),
        "target-local-storage",
    )
    .expect("write target marker");

    ensure_live_root_bindings(&paths, &source).expect("bind source");
    switch_host_persona(&paths, &source, &target, false).expect("switch");

    assert_eq!(
        fs::read_to_string(source_leveldb.join("source-project.marker")).unwrap(),
        "source-local-storage"
    );
    assert_eq!(
        fs::read_to_string(target_leveldb.join("target-project.marker")).unwrap(),
        "target-local-storage"
    );
    assert!(
        !source_leveldb.join("target-project.marker").exists(),
        "source persona should not inherit target local storage state"
    );
    assert!(
        !target_leveldb.join("source-project.marker").exists(),
        "target persona should not inherit source local storage state"
    );
}
