use super::*;

#[test]
fn switch_host_persona_links_shared_settings_and_local_skills() {
    let temp = tempfile::Builder::new()
        .prefix("codex-rotate-shared-state-")
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
    let target_project = temp.path().join("projects/target-only");
    fs::create_dir_all(&source_project).expect("create source project");
    fs::create_dir_all(&target_project).expect("create target project");
    for project in [&source_project, &target_project] {
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
                "model = \"gpt-5.3-codex\"\napproval_policy = \"never\"\n\n[projects.\"{}\"]\ntrust_level = \"trusted\"\n",
                encode_toml_basic_string(&source_project.display().to_string())
            ),
        )
        .expect("write source config");
    fs::write(
            target_paths.codex_home.join("config.toml"),
            format!(
                "model = \"old-model\"\npersonality = \"target-only\"\n\n[projects.\"{}\"]\ntrust_level = \"trusted\"\n",
                encode_toml_basic_string(&target_project.display().to_string())
            ),
        )
        .expect("write target config");

    fs::write(
        source_paths.codex_home.join(CODEX_GLOBAL_STATE_FILE_NAME),
        serde_json::to_string(&json!({
            "default-service-tier": "flex",
            "skip-full-access-confirm": true,
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
            "target-only-setting": true,
            SAVED_WORKSPACE_ROOTS_KEY: [target_project.display().to_string()],
            PROJECT_ORDER_KEY: [target_project.display().to_string()],
            ACTIVE_WORKSPACE_ROOTS_KEY: [target_project.display().to_string()],
        }))
        .expect("serialize target global state"),
    )
    .expect("write target global state");

    fs::write(source_paths.codex_home.join("AGENTS.md"), "source agents\n")
        .expect("write source agents");
    fs::write(target_paths.codex_home.join("AGENTS.md"), "target agents\n")
        .expect("write target agents");
    fs::create_dir_all(source_paths.codex_home.join("rules")).expect("create source rules");
    fs::write(
        source_paths.codex_home.join("rules").join("default.rules"),
        "source rules\n",
    )
    .expect("write source rules");
    fs::create_dir_all(target_paths.codex_home.join("rules")).expect("create target rules");
    fs::write(
        target_paths.codex_home.join("rules").join("obsolete.rules"),
        "obsolete\n",
    )
    .expect("write obsolete rule");
    fs::create_dir_all(source_paths.codex_home.join("skills").join("local-skill"))
        .expect("create source skill");
    fs::write(
        source_paths
            .codex_home
            .join("skills")
            .join("local-skill")
            .join("SKILL.md"),
        "# Source Skill\n",
    )
    .expect("write source skill");
    fs::create_dir_all(
        target_paths
            .codex_home
            .join("skills")
            .join("obsolete-skill"),
    )
    .expect("create target skill");
    fs::write(
        target_paths
            .codex_home
            .join("skills")
            .join("obsolete-skill")
            .join("SKILL.md"),
        "# Obsolete Skill\n",
    )
    .expect("write obsolete skill");
    fs::create_dir_all(source_paths.codex_home.join("vendor_imports"))
        .expect("create source imports");
    fs::write(
        source_paths
            .codex_home
            .join("vendor_imports")
            .join("skills-curated-cache.json"),
        "{}\n",
    )
    .expect("write source imports");
    fs::create_dir_all(target_paths.codex_home.join("vendor_imports"))
        .expect("create target imports");
    fs::write(
        target_paths
            .codex_home
            .join("vendor_imports")
            .join("stale.json"),
        "{}\n",
    )
    .expect("write stale imports");

    ensure_live_root_bindings(&paths, &source).expect("bind source");
    switch_host_persona(&paths, &source, &target, false).expect("switch");

    let shared_codex_home = host_shared_codex_home_root(&paths);
    for entry in SHARED_CODEX_HOME_ENTRIES {
        assert_eq!(
            fs::read_link(source_paths.codex_home.join(entry)).expect("source shared link"),
            shared_codex_home.join(entry)
        );
        assert_eq!(
            fs::read_link(target_paths.codex_home.join(entry)).expect("target shared link"),
            shared_codex_home.join(entry)
        );
    }

    let target_config =
        fs::read_to_string(target_paths.codex_home.join("config.toml")).expect("target config");
    assert!(target_config.contains("model = \"gpt-5.3-codex\""));
    assert!(target_config.contains("approval_policy = \"never\""));
    assert!(!target_config.contains("personality = \"target-only\""));
    assert!(target_config.contains(&project_table_heading(&source_project)));
    assert!(!target_config.contains(&project_table_heading(&target_project)));

    let target_state: Value = serde_json::from_str(
        &fs::read_to_string(target_paths.codex_home.join(CODEX_GLOBAL_STATE_FILE_NAME))
            .expect("target global state"),
    )
    .expect("parse target global state");
    assert_eq!(
        target_state
            .get("default-service-tier")
            .and_then(Value::as_str),
        Some("flex")
    );
    assert_eq!(
        target_state
            .get("skip-full-access-confirm")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert!(target_state.get("target-only-setting").is_none());
    let saved_roots = target_state
        .get(SAVED_WORKSPACE_ROOTS_KEY)
        .and_then(Value::as_array)
        .expect("saved roots")
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    assert!(saved_roots.contains(&source_project.display().to_string().as_str()));
    assert!(!saved_roots.contains(&target_project.display().to_string().as_str()));

    assert_eq!(
        fs::read_to_string(target_paths.codex_home.join("AGENTS.md")).unwrap(),
        "source agents\n"
    );
    assert_eq!(
        fs::read_to_string(target_paths.codex_home.join("rules").join("default.rules")).unwrap(),
        "source rules\n"
    );
    assert!(!target_paths
        .codex_home
        .join("rules")
        .join("obsolete.rules")
        .exists());
    assert!(target_paths
        .codex_home
        .join("skills")
        .join("local-skill")
        .join("SKILL.md")
        .exists());
    assert!(!target_paths
        .codex_home
        .join("skills")
        .join("obsolete-skill")
        .exists());
    assert!(target_paths
        .codex_home
        .join("vendor_imports")
        .join("skills-curated-cache.json")
        .exists());
    assert!(!target_paths
        .codex_home
        .join("vendor_imports")
        .join("stale.json")
        .exists());

    fs::create_dir_all(source_paths.codex_home.join("skills").join("runtime-skill"))
        .expect("create runtime skill");
    fs::write(
        source_paths
            .codex_home
            .join("skills")
            .join("runtime-skill")
            .join("SKILL.md"),
        "# Runtime Skill\n",
    )
    .expect("write runtime skill");
    assert!(target_paths
        .codex_home
        .join("skills")
        .join("runtime-skill")
        .join("SKILL.md")
        .exists());
    fs::remove_dir_all(source_paths.codex_home.join("skills").join("runtime-skill"))
        .expect("remove runtime skill");
    assert!(!target_paths
        .codex_home
        .join("skills")
        .join("runtime-skill")
        .exists());
}

#[test]
fn shared_codex_home_migrates_entries_linked_to_legacy_host_shared_data() {
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    let legacy_shared_codex_home = paths
        .rotate_home
        .join("personas")
        .join("host")
        .join("shared-data")
        .join("codex-home");

    fs::create_dir_all(&source_paths.codex_home).expect("create source codex home");
    fs::create_dir_all(legacy_shared_codex_home.join("skills").join("legacy-skill"))
        .expect("create legacy skill");
    fs::write(
        legacy_shared_codex_home.join("config.toml"),
        "model = \"gpt-5.5\"\n",
    )
    .expect("write legacy config");
    fs::write(
        legacy_shared_codex_home
            .join("skills")
            .join("legacy-skill")
            .join("SKILL.md"),
        "# Legacy Skill\n",
    )
    .expect("write legacy skill");
    ensure_symlink_path(
        &source_paths.codex_home.join("config.toml"),
        &legacy_shared_codex_home.join("config.toml"),
    )
    .expect("link legacy config");
    ensure_symlink_path(
        &source_paths.codex_home.join("skills"),
        &legacy_shared_codex_home.join("skills"),
    )
    .expect("link legacy skills");

    ensure_host_persona_shared_codex_home_links(&paths, &source_paths)
        .expect("migrate shared links");

    let shared_codex_home = host_shared_codex_home_root(&paths);
    assert_eq!(
        shared_codex_home,
        paths.rotate_home.join("personas/shared-data/codex-home")
    );
    assert_eq!(
        fs::read_to_string(shared_codex_home.join("config.toml")).unwrap(),
        "model = \"gpt-5.5\"\n"
    );
    assert_eq!(
        fs::read_to_string(
            shared_codex_home
                .join("skills")
                .join("legacy-skill")
                .join("SKILL.md")
        )
        .unwrap(),
        "# Legacy Skill\n"
    );
    assert_eq!(
        fs::read_link(source_paths.codex_home.join("config.toml")).unwrap(),
        shared_codex_home.join("config.toml")
    );
    assert_eq!(
        fs::read_link(source_paths.codex_home.join("skills")).unwrap(),
        shared_codex_home.join("skills")
    );
}

#[test]
fn switch_host_persona_syncs_current_archive_state_via_lineage_mapping() {
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");

    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();

    let lineage_id = "lineage-archive-1";
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
        "/workspace/shared-project",
        true,
    );
    update_thread_metadata(
        &target_paths.codex_home.join("state_5.sqlite"),
        "thread-target",
        "/workspace/shared-project",
        false,
    );
    let mut store =
        ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("open sync store");
    store
        .bind_local_thread_id(
            &conversation_sync_identity(&source),
            lineage_id,
            "thread-source",
        )
        .expect("bind source lineage");
    store
        .bind_local_thread_id(
            &conversation_sync_identity(&target),
            lineage_id,
            "thread-target",
        )
        .expect("bind target lineage");

    ensure_live_root_bindings(&paths, &source).expect("bind source");
    switch_host_persona(&paths, &source, &target, false).expect("switch");

    assert!(thread_is_archived(
        &source_paths.codex_home.join("state_5.sqlite"),
        "thread-source",
    ));
    assert!(thread_is_archived(
        &target_paths.codex_home.join("state_5.sqlite"),
        "thread-target",
    ));
}
