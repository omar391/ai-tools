use super::*;

#[test]
fn import_appends_only_missing_suffix_for_existing_prefix() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
    }

    let items = numbered_user_response_items(5);
    let watermark = thread_handoff_content_watermark(&items).expect("watermark");
    let mut store =
        ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("open store");
    store
        .bind_and_update_watermark("target-sync", "lineage-1", "target-thread", Some("old"))
        .expect("seed old target binding");

    struct PrefixTransport {
        existing_items: Vec<Value>,
        injected: Arc<Mutex<Vec<(String, Vec<Value>)>>>,
    }
    impl ConversationTransport for PrefixTransport {
        fn list_threads(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }
        fn read_thread(&self, id: &str) -> Result<Value> {
            assert_eq!(id, "target-thread");
            Ok(thread_read_response_from_response_items(
                id,
                &self.existing_items,
            ))
        }
        fn start_thread(&self, _cwd: Option<&str>) -> Result<String> {
            panic!("prefix update must preserve the existing target-local thread")
        }
        fn inject_items(&self, id: &str, items: Vec<Value>) -> Result<()> {
            self.injected
                .lock()
                .expect("injected")
                .push((id.to_string(), items));
            Ok(())
        }
    }

    let injected = Arc::new(Mutex::new(Vec::new()));
    let transport = PrefixTransport {
        existing_items: items[..2].to_vec(),
        injected: Arc::clone(&injected),
    };
    let handoff = ThreadHandoff {
        source_thread_id: "source-thread".to_string(),
        lineage_id: "lineage-1".to_string(),
        watermark: Some(watermark.clone()),
        cwd: None,
        items: items.clone(),
        metadata: ThreadHandoffMetadata::default(),
    };

    let outcome = import_thread_handoffs(&transport, "target-sync", &[handoff], None)
        .expect("import returns outcome");
    assert!(outcome.is_complete());
    assert_eq!(
        store
            .get_local_thread_id("target-sync", "lineage-1")
            .expect("target binding"),
        Some("target-thread".to_string())
    );
    assert_eq!(
        store
            .get_watermark("target-sync", "lineage-1")
            .expect("target watermark"),
        Some(watermark)
    );
    let injected = injected.lock().expect("injected");
    assert_eq!(injected.len(), 1);
    assert_eq!(injected[0].0, "target-thread");
    assert_eq!(injected[0].1, items[2..].to_vec());

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}

#[test]
fn import_replaces_truncated_suffix_without_duplicating_history() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
    }

    let items = numbered_user_response_items(5);
    let watermark = thread_handoff_content_watermark(&items).expect("watermark");
    let mut store =
        ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("open store");
    store
        .bind_and_update_watermark("target-sync", "lineage-1", "partial-thread", Some("old"))
        .expect("seed old target binding");

    struct SuffixTransport {
        existing_items: Vec<Value>,
        injected: Arc<Mutex<Vec<(String, Vec<Value>)>>>,
        cleaned: Arc<Mutex<Vec<(String, String)>>>,
    }
    impl ConversationTransport for SuffixTransport {
        fn list_threads(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }
        fn read_thread(&self, id: &str) -> Result<Value> {
            assert_eq!(id, "partial-thread");
            Ok(thread_read_response_from_response_items(
                id,
                &self.existing_items,
            ))
        }
        fn start_thread(&self, _cwd: Option<&str>) -> Result<String> {
            Ok("replacement-thread".to_string())
        }
        fn inject_items(&self, id: &str, items: Vec<Value>) -> Result<()> {
            self.injected
                .lock()
                .expect("injected")
                .push((id.to_string(), items));
            Ok(())
        }
        fn cleanup_replaced_thread(
            &self,
            kept_thread_id: &str,
            stale_thread_id: &str,
        ) -> Result<()> {
            self.cleaned
                .lock()
                .expect("cleaned")
                .push((kept_thread_id.to_string(), stale_thread_id.to_string()));
            Ok(())
        }
    }

    let injected = Arc::new(Mutex::new(Vec::new()));
    let cleaned = Arc::new(Mutex::new(Vec::new()));
    let transport = SuffixTransport {
        existing_items: items[3..].to_vec(),
        injected: Arc::clone(&injected),
        cleaned: Arc::clone(&cleaned),
    };
    let handoff = ThreadHandoff {
        source_thread_id: "source-thread".to_string(),
        lineage_id: "lineage-1".to_string(),
        watermark: Some(watermark.clone()),
        cwd: None,
        items: items.clone(),
        metadata: ThreadHandoffMetadata::default(),
    };

    let outcome = import_thread_handoffs(&transport, "target-sync", &[handoff], None)
        .expect("import returns outcome");
    assert!(outcome.is_complete());
    assert_eq!(
        store
            .get_local_thread_id("target-sync", "lineage-1")
            .expect("target binding"),
        Some("replacement-thread".to_string())
    );
    assert_eq!(
        store
            .get_watermark("target-sync", "lineage-1")
            .expect("target watermark"),
        Some(watermark)
    );
    let injected = injected.lock().expect("injected");
    assert_eq!(injected.len(), 1);
    assert_eq!(injected[0].0, "replacement-thread");
    assert_eq!(injected[0].1, items);
    assert_eq!(
        cleaned.lock().expect("cleaned").as_slice(),
        &[(
            "replacement-thread".to_string(),
            "partial-thread".to_string()
        )]
    );

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}

#[test]
fn import_carries_archived_state_in_handoff_metadata() {
    let temp = tempdir().expect("tempdir");
    let codex_home = temp.path().join("codex-home");
    let sessions = codex_home.join("sessions");
    fs::create_dir_all(&sessions).expect("create sessions");
    let target_thread_id = "target-thread-local";
    let rollout_path = sessions.join(format!("rollout-{target_thread_id}.jsonl"));
    fs::write(&rollout_path, "{}\n").expect("write rollout");
    let state_db = codex_home.join("state_5.sqlite");
    let connection = rusqlite::Connection::open(&state_db).expect("open state db");
    connection
        .execute_batch(
            r#"
create table threads (
    id text primary key,
    rollout_path text not null,
    created_at integer not null,
    updated_at integer not null,
    title text not null,
    first_user_message text not null default '',
    has_user_event integer not null default 0,
    archived integer not null default 0
);
"#,
        )
        .expect("create threads table");
    drop(connection);

    publish_thread_sidebar_metadata(
        &codex_home,
        target_thread_id,
        &ThreadHandoffMetadata {
            title: Some("Archived import".to_string()),
            first_user_message: Some("archive me".to_string()),
            archived: Some(true),
            ..ThreadHandoffMetadata::default()
        },
    )
    .expect("publish metadata");

    let connection = rusqlite::Connection::open(&state_db).expect("reopen state db");
    let archived: i64 = connection
        .query_row(
            "select archived from threads where id = ?1",
            [target_thread_id],
            |row| row.get(0),
        )
        .expect("query archived state");
    assert_eq!(archived, 1);
}

#[test]
fn import_repairs_poisoned_same_id_target_binding() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
    }

    let mut store =
        ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("open sync store");
    store
        .bind_local_thread_id("target-sync", "lineage-1", "source-thread")
        .expect("seed poisoned binding");

    struct RepairTransport;
    impl ConversationTransport for RepairTransport {
        fn list_threads(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }
        fn read_thread(&self, _id: &str) -> Result<Value> {
            Ok(json!({}))
        }
        fn start_thread(&self, _cwd: Option<&str>) -> Result<String> {
            Ok("target-thread".to_string())
        }
        fn inject_items(&self, _id: &str, _items: Vec<Value>) -> Result<()> {
            Ok(())
        }
    }

    let handoff = ThreadHandoff {
        source_thread_id: "source-thread".to_string(),
        lineage_id: "lineage-1".to_string(),
        watermark: Some("turn-1".to_string()),
        cwd: None,
        items: vec![],
        metadata: ThreadHandoffMetadata::default(),
    };
    let outcome = import_thread_handoffs(&RepairTransport, "target-sync", &[handoff], None)
        .expect("import returns outcome");
    assert!(outcome.is_complete());
    assert_eq!(outcome.failures.len(), 0);
    assert_eq!(
        store
            .get_local_thread_id("target-sync", "lineage-1")
            .expect("target binding"),
        Some("target-thread".to_string())
    );
    assert_ne!(
        store
            .get_local_thread_id("target-sync", "lineage-1")
            .expect("target binding")
            .unwrap(),
        "source-thread"
    );
    assert_eq!(
        store
            .get_watermark("target-sync", "lineage-1")
            .expect("watermark"),
        Some("turn-1".to_string())
    );

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}

#[test]
fn import_reclaims_missing_existing_target_binding() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
    }

    let mut store =
        ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("open sync store");
    store
        .bind_local_thread_id("target-sync", "lineage-1", "missing-target-thread")
        .expect("seed missing binding");

    struct MissingBindingTransport {
        injected_thread_ids: Arc<Mutex<Vec<String>>>,
    }
    impl ConversationTransport for MissingBindingTransport {
        fn list_threads(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }
        fn read_thread(&self, _id: &str) -> Result<Value> {
            Ok(json!({}))
        }
        fn thread_exists(&self, id: &str) -> Result<bool> {
            Ok(id != "missing-target-thread")
        }
        fn start_thread(&self, _cwd: Option<&str>) -> Result<String> {
            Ok("new-target-thread".to_string())
        }
        fn inject_items(&self, id: &str, _items: Vec<Value>) -> Result<()> {
            self.injected_thread_ids
                .lock()
                .expect("injected thread ids")
                .push(id.to_string());
            Ok(())
        }
    }

    let injected_thread_ids = Arc::new(Mutex::new(Vec::new()));
    let transport = MissingBindingTransport {
        injected_thread_ids: injected_thread_ids.clone(),
    };
    let handoff = ThreadHandoff {
        source_thread_id: "source-thread".to_string(),
        lineage_id: "lineage-1".to_string(),
        watermark: Some("turn-1".to_string()),
        cwd: None,
        items: vec![json!({
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "hi"}],
        })],
        metadata: ThreadHandoffMetadata::default(),
    };
    let outcome = import_thread_handoffs(&transport, "target-sync", &[handoff], None)
        .expect("import returns outcome");
    assert!(outcome.is_complete());
    assert_eq!(
        injected_thread_ids.lock().expect("injected ids").as_slice(),
        &["new-target-thread".to_string()]
    );
    assert_eq!(
        store
            .get_local_thread_id("target-sync", "lineage-1")
            .expect("target binding"),
        Some("new-target-thread".to_string())
    );

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}

#[test]
fn import_reclaims_existing_binding_when_inject_reports_thread_not_found() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
    }

    let mut store =
        ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("open sync store");
    store
        .bind_local_thread_id("target-sync", "lineage-1", "stale-target-thread")
        .expect("seed stale binding");

    struct InjectRepairTransport {
        injected_thread_ids: Arc<Mutex<Vec<String>>>,
    }
    impl ConversationTransport for InjectRepairTransport {
        fn list_threads(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }
        fn read_thread(&self, id: &str) -> Result<Value> {
            Ok(json!({ "thread": { "id": id, "turns": [] } }))
        }
        fn thread_exists(&self, _id: &str) -> Result<bool> {
            Ok(true)
        }
        fn start_thread(&self, _cwd: Option<&str>) -> Result<String> {
            Ok("replacement-target-thread".to_string())
        }
        fn inject_items(&self, id: &str, _items: Vec<Value>) -> Result<()> {
            self.injected_thread_ids
                .lock()
                .expect("injected thread ids")
                .push(id.to_string());
            if id == "stale-target-thread" {
                Err(anyhow!("thread not found: stale-target-thread"))
            } else {
                Ok(())
            }
        }
    }

    let injected_thread_ids = Arc::new(Mutex::new(Vec::new()));
    let transport = InjectRepairTransport {
        injected_thread_ids: injected_thread_ids.clone(),
    };
    let handoff = ThreadHandoff {
        source_thread_id: "source-thread".to_string(),
        lineage_id: "lineage-1".to_string(),
        watermark: Some("turn-1".to_string()),
        cwd: None,
        items: vec![json!({
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "hi"}],
        })],
        metadata: ThreadHandoffMetadata::default(),
    };
    let outcome = import_thread_handoffs(&transport, "target-sync", &[handoff], None)
        .expect("import returns outcome");
    assert!(outcome.is_complete());
    assert_eq!(
        injected_thread_ids.lock().expect("injected ids").as_slice(),
        &[
            "stale-target-thread".to_string(),
            "replacement-target-thread".to_string(),
        ]
    );
    assert_eq!(
        store
            .get_local_thread_id("target-sync", "lineage-1")
            .expect("target binding"),
        Some("replacement-target-thread".to_string())
    );

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}

#[test]
fn import_publishes_sidebar_metadata_for_existing_current_binding() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
    }

    let mut store =
        ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("open sync store");
    store
        .bind_local_thread_id("target-sync", "lineage-1", "target-thread")
        .expect("seed binding");
    store
        .set_watermark("target-sync", "lineage-1", Some("turn-1"))
        .expect("seed watermark");

    struct MetadataTransport {
        calls: Arc<Mutex<Vec<(String, Option<String>, Option<bool>)>>>,
    }
    impl ConversationTransport for MetadataTransport {
        fn list_threads(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }
        fn read_thread(&self, _id: &str) -> Result<Value> {
            Ok(json!({}))
        }
        fn start_thread(&self, _cwd: Option<&str>) -> Result<String> {
            unreachable!("existing current binding should not create a duplicate thread")
        }
        fn inject_items(&self, _id: &str, _items: Vec<Value>) -> Result<()> {
            unreachable!("same watermark should not reinject items")
        }
        fn publish_thread_metadata(
            &self,
            thread_id: &str,
            metadata: &ThreadHandoffMetadata,
        ) -> Result<()> {
            self.calls.lock().expect("metadata calls lock").push((
                thread_id.to_string(),
                metadata.title.clone(),
                metadata.projectless,
            ));
            Ok(())
        }
    }

    let calls = Arc::new(Mutex::new(Vec::new()));
    let transport = MetadataTransport {
        calls: calls.clone(),
    };
    let handoff = ThreadHandoff {
        source_thread_id: "source-thread".to_string(),
        lineage_id: "lineage-1".to_string(),
        watermark: Some("turn-1".to_string()),
        cwd: None,
        items: vec![],
        metadata: ThreadHandoffMetadata {
            title: Some("Greet user".to_string()),
            first_user_message: Some("hi".to_string()),
            updated_at: Some(1_777_038_485),
            updated_at_ms: None,
            session_index_updated_at: Some("2026-04-24T13:48:05.018117Z".to_string()),
            projectless: Some(true),
            workspace_root_hint: Some("/tmp/projectless-root".to_string()),
            ..ThreadHandoffMetadata::default()
        },
    };

    let outcome = import_thread_handoffs(&transport, "target-sync", &[handoff], None)
        .expect("import returns outcome");
    assert!(outcome.is_complete());
    assert_eq!(
        calls.lock().expect("metadata calls").as_slice(),
        &[(
            "target-thread".to_string(),
            Some("Greet user".to_string()),
            Some(true)
        )]
    );

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}

#[test]
fn import_preserves_matching_established_binding_without_materialization_probe() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
    }

    let items = vec![json!({
        "type": "message",
        "role": "user",
        "content": [{"type": "input_text", "text": "Greet user"}],
    })];
    let watermark = thread_handoff_content_watermark(&items).expect("watermark");
    let mut store =
        ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("open store");
    store
        .bind_and_update_watermark(
            "source-sync",
            "lineage-1",
            "source-local-thread",
            Some(&watermark),
        )
        .expect("seed source binding");

    struct MatchingTransport {
        metadata_calls: Arc<Mutex<Vec<String>>>,
    }
    impl ConversationTransport for MatchingTransport {
        fn list_threads(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }
        fn read_thread(&self, _id: &str) -> Result<Value> {
            Ok(json!({}))
        }
        fn thread_exists(&self, _id: &str) -> Result<bool> {
            panic!("matching content watermark should not probe materialization")
        }
        fn start_thread(&self, _cwd: Option<&str>) -> Result<String> {
            panic!("matching content watermark should not create a replacement thread")
        }
        fn inject_items(&self, _id: &str, _items: Vec<Value>) -> Result<()> {
            panic!("matching content watermark should not reinject items")
        }
        fn publish_thread_metadata(
            &self,
            thread_id: &str,
            _metadata: &ThreadHandoffMetadata,
        ) -> Result<()> {
            self.metadata_calls
                .lock()
                .expect("metadata calls")
                .push(thread_id.to_string());
            Ok(())
        }
    }

    let metadata_calls = Arc::new(Mutex::new(Vec::new()));
    let transport = MatchingTransport {
        metadata_calls: metadata_calls.clone(),
    };
    let handoff = ThreadHandoff {
        source_thread_id: "target-local-thread".to_string(),
        lineage_id: "lineage-1".to_string(),
        watermark: Some(watermark),
        cwd: None,
        items,
        metadata: ThreadHandoffMetadata::default(),
    };

    let outcome = import_thread_handoffs(&transport, "source-sync", &[handoff], None)
        .expect("import returns outcome");
    assert!(outcome.is_complete());
    assert_eq!(
        store
            .get_local_thread_id("source-sync", "lineage-1")
            .expect("source binding"),
        Some("source-local-thread".to_string())
    );
    assert_eq!(
        metadata_calls.lock().expect("metadata calls").as_slice(),
        &["source-local-thread".to_string()]
    );

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}
