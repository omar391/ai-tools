use super::*;

#[test]
fn host_conversation_snapshot_syncs_active_and_archived_jsonl_one_way() {
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");

    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();
    let source_db = source_paths.codex_home.join("state_5.sqlite");
    let target_db = target_paths.codex_home.join("state_5.sqlite");

    let source_threads = [
        ("active-thread-1", false, 500_i64),
        ("active-thread-2", false, 490_i64),
        ("archived-thread-1", true, 480_i64),
        ("archived-thread-2", true, 470_i64),
        ("archived-thread-3", true, 460_i64),
    ];
    let mut source_rollout_paths = BTreeMap::new();
    for (thread_id, archived, updated_at) in source_threads {
        let root = if archived {
            source_paths.codex_home.join("archived_sessions")
        } else {
            source_paths.codex_home.join("sessions")
        };
        let rollout_path = root
            .join("2026")
            .join("04")
            .join("25")
            .join(format!("rollout-2026-04-25T00-00-00-{thread_id}.jsonl"));
        fs::create_dir_all(rollout_path.parent().unwrap()).expect("create rollout parent");
        fs::write(
                &rollout_path,
                format!(
                    "{{\"id\":\"{thread_id}\",\"thread_id\":\"{thread_id}\",\"payload\":{{\"type\":\"user_message\",\"message\":\"literal {thread_id} text must stay\"}}}}\n"
                ),
            )
            .expect("write source rollout");
        source_rollout_paths.insert(
            thread_id,
            (rollout_path.display().to_string(), archived, updated_at),
        );
    }

    let source_rows = source_rollout_paths
        .iter()
        .map(|(thread_id, (path, _, updated_at))| (*thread_id, path.as_str(), *updated_at))
        .collect::<Vec<_>>();
    seed_threads_table(&source_db, &source_rows);
    for (thread_id, (_, archived, _)) in &source_rollout_paths {
        update_thread_metadata(
            &source_db,
            thread_id,
            "/workspace/source-project",
            *archived,
        );
    }
    fs::write(
            source_paths.codex_home.join(SESSION_INDEX_FILE_NAME),
            source_rollout_paths
                .iter()
                .map(|(thread_id, (_, _, updated_at))| {
                    format!(
                        "{{\"id\":\"{thread_id}\",\"thread_name\":\"title {thread_id}\",\"updated_at\":\"2026-04-25T00:00:{:02}Z\"}}",
                        updated_at % 60
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
                + "\n",
        )
        .expect("write source session index");

    let target_only_rollout = target_paths
        .codex_home
        .join("sessions/2026/04/24/rollout-target-only.jsonl");
    fs::create_dir_all(target_only_rollout.parent().unwrap()).expect("create target-only parent");
    fs::write(&target_only_rollout, "{\"id\":\"target-only\"}\n")
        .expect("write target-only rollout");
    let target_only_rollout_string = target_only_rollout.display().to_string();
    seed_threads_table(
        &target_db,
        &[("target-only", target_only_rollout_string.as_str(), 100)],
    );
    fs::write(
            target_paths.codex_home.join(SESSION_INDEX_FILE_NAME),
            "{\"id\":\"target-only\",\"thread_name\":\"target only\",\"updated_at\":\"2026-04-24T00:00:00Z\"}\n",
        )
        .expect("write target session index");

    sync_host_persona_conversation_snapshot(
        &source_paths.codex_home,
        &conversation_sync_identity(&source),
        &target_paths.codex_home,
        &conversation_sync_identity(&target),
        &paths.conversation_sync_db_file,
    )
    .expect("sync snapshot");

    let store =
        ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("open sync store");
    let target_state_ids = read_thread_ids(&target_db);
    assert!(target_state_ids.contains(&"target-only".to_string()));
    assert!(target_only_rollout.exists());
    let target_index = fs::read_to_string(target_paths.codex_home.join(SESSION_INDEX_FILE_NAME))
        .expect("read target index");
    assert!(target_index.contains("\"target-only\""));

    for (source_thread_id, (_, archived, _)) in &source_rollout_paths {
        let lineage_id = store
            .get_lineage_id(&conversation_sync_identity(&source), source_thread_id)
            .expect("source lineage")
            .expect("source lineage exists");
        let target_thread_id = store
            .get_local_thread_id(&conversation_sync_identity(&target), &lineage_id)
            .expect("target binding")
            .expect("target binding exists");
        assert_ne!(&target_thread_id, source_thread_id);
        assert_eq!(target_thread_id.len(), source_thread_id.len());
        let target_root = if *archived {
            target_paths.codex_home.join("archived_sessions")
        } else {
            target_paths.codex_home.join("sessions")
        };
        let target_rollout = find_thread_rollout_path(&target_root, &target_thread_id)
            .unwrap_or_else(|| {
                panic!("missing target rollout for {source_thread_id} as {target_thread_id}")
            });
        assert!(
            target_rollout.starts_with(&target_root),
            "rollout should be in the expected archive root: {}",
            target_rollout.display()
        );
        let contents = fs::read_to_string(&target_rollout).expect("read target rollout");
        assert!(contents.contains(&format!("\"id\":\"{target_thread_id}\"")));
        assert!(contents.contains(&format!("\"thread_id\":\"{target_thread_id}\"")));
        assert!(contents.contains(&format!(
            "\"message\":\"literal {source_thread_id} text must stay\""
        )));
        assert!(!contents.contains(&format!("\"thread_id\":\"{source_thread_id}\"")));
        assert!(target_state_ids.contains(&target_thread_id));
        assert!(!target_state_ids.contains(&source_thread_id.to_string()));
        assert_eq!(
            thread_is_archived(&target_db, &target_thread_id),
            *archived,
            "archive state should sync for {source_thread_id}"
        );
        assert!(target_index.contains(&format!("\"id\":\"{target_thread_id}\"")));
        assert!(!target_index.contains(&format!("\"id\":\"{source_thread_id}\"")));
    }
}

#[test]
fn host_conversation_snapshot_moves_thread_between_active_and_archive_roots() {
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-source", "persona-source");
    let target = test_account("acct-target", "persona-target");

    provision_host_persona(&paths, &source, None).expect("provision source");
    provision_host_persona(&paths, &target, None).expect("provision target");

    let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
    let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();
    let source_db = source_paths.codex_home.join("state_5.sqlite");
    let target_db = target_paths.codex_home.join("state_5.sqlite");
    let source_thread_id = "thread-move";
    let target_thread_id = "target-move";

    let active_source_rollout = source_paths
        .codex_home
        .join("sessions/2026/04/25/rollout-thread-move.jsonl");
    fs::create_dir_all(active_source_rollout.parent().unwrap())
        .expect("create active source parent");
    fs::write(
        &active_source_rollout,
        "{\"id\":\"thread-move\",\"thread_id\":\"thread-move\"}\n",
    )
    .expect("write active source rollout");
    let active_source_rollout_string = active_source_rollout.display().to_string();
    seed_threads_table(
        &source_db,
        &[(source_thread_id, active_source_rollout_string.as_str(), 100)],
    );
    seed_threads_table(&target_db, &[]);
    fs::write(
            source_paths.codex_home.join(SESSION_INDEX_FILE_NAME),
            "{\"id\":\"thread-move\",\"thread_name\":\"move me\",\"updated_at\":\"2026-04-25T00:00:00Z\"}\n",
        )
        .expect("write source index");

    let mut store =
        ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("open sync store");
    store
        .bind_local_thread_id(
            &conversation_sync_identity(&source),
            source_thread_id,
            source_thread_id,
        )
        .expect("bind source");
    store
        .bind_local_thread_id(
            &conversation_sync_identity(&target),
            source_thread_id,
            target_thread_id,
        )
        .expect("bind target");

    sync_host_persona_conversation_snapshot(
        &source_paths.codex_home,
        &conversation_sync_identity(&source),
        &target_paths.codex_home,
        &conversation_sync_identity(&target),
        &paths.conversation_sync_db_file,
    )
    .expect("sync active");

    let active_target =
        find_thread_rollout_path(&target_paths.codex_home.join("sessions"), target_thread_id)
            .expect("active target rollout");
    assert!(active_target.exists());
    assert!(find_thread_rollout_path(
        &target_paths.codex_home.join("archived_sessions"),
        target_thread_id,
    )
    .is_none());

    let archived_source_rollout = source_paths
        .codex_home
        .join("archived_sessions/2026/04/25/rollout-thread-move.jsonl");
    fs::create_dir_all(archived_source_rollout.parent().unwrap())
        .expect("create archived source parent");
    fs::rename(&active_source_rollout, &archived_source_rollout).expect("archive source rollout");
    let archived_source_rollout_string = archived_source_rollout.display().to_string();
    let connection = rusqlite::Connection::open(&source_db).expect("open source db");
    connection
        .execute(
            "update threads set rollout_path = ?1, archived = 1 where id = ?2",
            rusqlite::params![archived_source_rollout_string, source_thread_id],
        )
        .expect("update source archive state");

    sync_host_persona_conversation_snapshot(
        &source_paths.codex_home,
        &conversation_sync_identity(&source),
        &target_paths.codex_home,
        &conversation_sync_identity(&target),
        &paths.conversation_sync_db_file,
    )
    .expect("sync archived");

    assert!(
        find_thread_rollout_path(&target_paths.codex_home.join("sessions"), target_thread_id)
            .is_none(),
        "active target rollout should be removed after archive sync"
    );
    let archived_target = find_thread_rollout_path(
        &target_paths.codex_home.join("archived_sessions"),
        target_thread_id,
    )
    .expect("archived target rollout");
    assert!(archived_target.exists());
    assert!(thread_is_archived(&target_db, target_thread_id));

    sync_host_persona_conversation_snapshot(
        &source_paths.codex_home,
        &conversation_sync_identity(&source),
        &target_paths.codex_home,
        &conversation_sync_identity(&target),
        &paths.conversation_sync_db_file,
    )
    .expect("sync archived idempotently");
    let target_index = fs::read_to_string(target_paths.codex_home.join(SESSION_INDEX_FILE_NAME))
        .expect("read target index");
    assert_eq!(target_index.matches(target_thread_id).count(), 1);
}
