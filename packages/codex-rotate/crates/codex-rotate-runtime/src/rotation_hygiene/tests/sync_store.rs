use super::*;

#[test]
fn test_sync_store_migration_resilience() {
    let temp = tempdir().expect("tempdir");
    let db_path = temp.path().join("test.sqlite");

    // 1. Create a DB with old schema (e.g. missing watermarks table)
    {
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute(
            "CREATE TABLE conversation_bindings (
                account_id TEXT NOT NULL,
                lineage_id TEXT NOT NULL,
                local_thread_id TEXT NOT NULL,
                PRIMARY KEY (account_id, lineage_id)
            )",
            [],
        )
        .expect("create");
    }

    // 2. Open via ConversationSyncStore - should migrate and add watermarks table
    let mut store = ConversationSyncStore::new(&db_path).expect("migrate");
    store
        .set_watermark("acct", "lineage", Some("turn"))
        .expect("set watermark");
    assert_eq!(
        store
            .get_watermark("acct", "lineage")
            .expect("get")
            .expect("found"),
        "turn"
    );
}

#[test]
fn test_import_resilience_to_partial_failures() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let _paths = test_runtime_paths(temp.path());

    let account_id = "acct-1";
    let handoffs = vec![
        ThreadHandoff {
            source_thread_id: "s1".to_string(),
            lineage_id: "l1".to_string(),
            watermark: Some("w1".to_string()),
            cwd: None,
            items: vec![],
            metadata: ThreadHandoffMetadata::default(),
        },
        ThreadHandoff {
            source_thread_id: "s2".to_string(),
            lineage_id: "l2".to_string(),
            watermark: Some("w2".to_string()),
            cwd: None,
            items: vec![],
            metadata: ThreadHandoffMetadata::default(),
        },
    ];

    struct FailingTransport;
    impl ConversationTransport for FailingTransport {
        fn list_threads(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }
        fn read_thread(&self, _id: &str) -> Result<Value> {
            Ok(json!({}))
        }
        fn start_thread(&self, _cwd: Option<&str>) -> Result<String> {
            Err(anyhow!("Failed to start thread"))
        }
        fn inject_items(&self, _id: &str, _items: Vec<Value>) -> Result<()> {
            Ok(())
        }
    }

    let outcome = import_thread_handoffs(&FailingTransport, account_id, &handoffs, None)
        .expect("import call succeeds even if individual handoffs fail");

    assert!(!outcome.is_complete());
    assert_eq!(outcome.failures.len(), 2);
    assert!(outcome.completed_source_thread_ids.is_empty());
}

#[test]
fn import_metadata_failure_persists_created_thread_binding_for_retry() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
    }

    struct MetadataFailingTransport;
    impl ConversationTransport for MetadataFailingTransport {
        fn list_threads(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }
        fn read_thread(&self, _id: &str) -> Result<Value> {
            Ok(json!({}))
        }
        fn start_thread(&self, _cwd: Option<&str>) -> Result<String> {
            Ok("target-local-thread".to_string())
        }
        fn inject_items(&self, _id: &str, _items: Vec<Value>) -> Result<()> {
            Ok(())
        }
        fn publish_thread_metadata(
            &self,
            _thread_id: &str,
            _metadata: &ThreadHandoffMetadata,
        ) -> Result<()> {
            Err(anyhow!("metadata write failed"))
        }
    }

    let handoff = ThreadHandoff {
        source_thread_id: "source-thread".to_string(),
        lineage_id: "lineage-1".to_string(),
        watermark: Some("items-fnv1a64-1234-1".to_string()),
        cwd: None,
        items: vec![json!({
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "Greet user"}],
        })],
        metadata: ThreadHandoffMetadata {
            title: Some("Greet user".to_string()),
            first_user_message: Some("Greet user".to_string()),
            ..ThreadHandoffMetadata::default()
        },
    };

    let outcome =
        import_thread_handoffs(&MetadataFailingTransport, "target-sync", &[handoff], None)
            .expect("import call");
    assert!(!outcome.is_complete());
    assert_eq!(outcome.failures.len(), 1);

    let store = ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("open store");
    let local_thread_id = store
        .get_local_thread_id("target-sync", "lineage-1")
        .expect("read local binding")
        .expect("binding should survive metadata failure");
    assert_eq!(local_thread_id, "target-local-thread");
    assert!(!is_pending_lineage_claim(&local_thread_id));
    assert_eq!(
        store
            .get_watermark("target-sync", "lineage-1")
            .expect("read watermark"),
        Some("items-fnv1a64-1234-1".to_string())
    );

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}

#[test]
fn test_lineage_claim_prevents_duplicate_materialization_race() {
    let temp = tempdir().expect("tempdir");
    let db_path = temp.path().join("conversation-sync.sqlite");
    let mut store = ConversationSyncStore::new(&db_path).expect("store");

    let claim_token = match store
        .claim_lineage_binding("acct-target", "lineage-a")
        .expect("first claim")
    {
        LineageBindingClaim::Claimed { claim_token } => claim_token,
        _ => panic!("first claim should reserve lineage"),
    };

    match store
        .claim_lineage_binding("acct-target", "lineage-a")
        .expect("second claim")
    {
        LineageBindingClaim::Busy => {}
        _ => panic!("second claim should be busy while first is pending"),
    }

    store
        .finalize_lineage_claim(
            "acct-target",
            "lineage-a",
            &claim_token,
            "thread-target-1",
            Some("turn-123"),
        )
        .expect("finalize");

    match store
        .claim_lineage_binding("acct-target", "lineage-a")
        .expect("claim after finalize")
    {
        LineageBindingClaim::Existing(local_thread_id) => {
            assert_eq!(local_thread_id, "thread-target-1");
        }
        _ => panic!("finalized lineage should return existing local thread id"),
    }

    assert_eq!(
        store
            .get_watermark("acct-target", "lineage-a")
            .expect("get watermark"),
        Some("turn-123".to_string())
    );
}

#[test]
fn stale_lineage_claim_can_be_reclaimed() {
    let temp = tempdir().expect("tempdir");
    let db_path = temp.path().join("conversation-sync.sqlite");
    let mut store = ConversationSyncStore::new(&db_path).expect("store");
    store
        .bind_local_thread_id(
            "acct-target",
            "lineage-stale",
            "__pending_lineage_claim__:123-1",
        )
        .expect("seed stale claim");

    let claim_token = match store
        .claim_lineage_binding("acct-target", "lineage-stale")
        .expect("claim stale lineage")
    {
        LineageBindingClaim::Claimed { claim_token } => claim_token,
        LineageBindingClaim::Busy => panic!("stale claim should not stay busy"),
        LineageBindingClaim::Existing(local_thread_id) => {
            panic!("stale claim should not return existing {local_thread_id}")
        }
    };
    assert!(is_pending_lineage_claim(&claim_token));

    store
        .finalize_lineage_claim(
            "acct-target",
            "lineage-stale",
            &claim_token,
            "target-thread",
            Some("watermark"),
        )
        .expect("finalize reclaimed claim");
    assert_eq!(
        store
            .get_local_thread_id("acct-target", "lineage-stale")
            .expect("read binding"),
        Some("target-thread".to_string())
    );
}

#[test]
fn translate_recovery_events_preserves_unresolved_source_entries() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
    }
    fs::create_dir_all(&paths.rotate_home).expect("create rotate home");

    let bound_source_event = crate::thread_recovery::ThreadRecoveryEvent {
        source_log_id: 1,
        source_ts: 1,
        thread_id: "source-thread-bound".to_string(),
        kind: crate::thread_recovery::ThreadRecoveryKind::QuotaExhausted,
        exhausted_turn_id: Some("turn-1".to_string()),
        exhausted_email: Some("acct-source@astronlab.com".to_string()),
        exhausted_account_id: Some("acct-source".to_string()),
        message: "quota exhausted".to_string(),
        rehydration: None,
    };
    let unresolved_source_event = crate::thread_recovery::ThreadRecoveryEvent {
        source_log_id: 2,
        source_ts: 2,
        thread_id: "source-thread-unbound".to_string(),
        kind: crate::thread_recovery::ThreadRecoveryKind::QuotaExhausted,
        exhausted_turn_id: Some("turn-2".to_string()),
        exhausted_email: Some("acct-source@astronlab.com".to_string()),
        exhausted_account_id: Some("acct-source".to_string()),
        message: "quota exhausted".to_string(),
        rehydration: None,
    };

    let mut initial_watch_state = crate::watch::WatchState::default();
    let mut source_state = initial_watch_state.account_state("acct-source");
    source_state.thread_recovery_pending = true;
    source_state.thread_recovery_pending_events =
        vec![bound_source_event.clone(), unresolved_source_event.clone()];
    initial_watch_state.set_account_state("acct-source", source_state);
    crate::watch::write_watch_state(&initial_watch_state).expect("write initial watch state");

    let mut store = ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("store");
    store
        .bind_local_thread_id("acct-source", "lineage-bound", "source-thread-bound")
        .expect("bind source lineage");
    store
        .bind_local_thread_id("acct-target", "lineage-bound", "target-thread-bound")
        .expect("bind target lineage");

    translate_recovery_events_after_rotation("acct-source", "acct-target", 9333, &[])
        .expect("translate recovery events");

    let next_watch_state = crate::watch::read_watch_state().expect("read watch state");
    let next_source_state = next_watch_state.account_state("acct-source");
    let next_target_state = next_watch_state.account_state("acct-target");

    assert!(next_source_state.thread_recovery_pending);
    assert_eq!(next_source_state.thread_recovery_pending_events.len(), 1);
    assert_eq!(
        next_source_state.thread_recovery_pending_events[0].thread_id,
        unresolved_source_event.thread_id
    );

    assert!(next_target_state.thread_recovery_pending);
    assert_eq!(next_target_state.thread_recovery_pending_events.len(), 1);
    assert_eq!(
        next_target_state.thread_recovery_pending_events[0].thread_id,
        "target-thread-bound"
    );

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
}

#[test]
fn translate_recovery_events_keeps_translated_entries_for_same_account_personas() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
    }
    fs::create_dir_all(&paths.rotate_home).expect("create rotate home");

    let bound_source_event = crate::thread_recovery::ThreadRecoveryEvent {
        source_log_id: 100,
        source_ts: 1,
        thread_id: "source-thread-bound".to_string(),
        kind: crate::thread_recovery::ThreadRecoveryKind::QuotaExhausted,
        exhausted_turn_id: Some("turn-1".to_string()),
        exhausted_email: Some("shared-account@astronlab.com".to_string()),
        exhausted_account_id: Some("acct-shared".to_string()),
        message: "quota exhausted".to_string(),
        rehydration: None,
    };
    let unresolved_source_event = crate::thread_recovery::ThreadRecoveryEvent {
        source_log_id: 101,
        source_ts: 2,
        thread_id: "source-thread-unbound".to_string(),
        kind: crate::thread_recovery::ThreadRecoveryKind::QuotaExhausted,
        exhausted_turn_id: Some("turn-2".to_string()),
        exhausted_email: Some("shared-account@astronlab.com".to_string()),
        exhausted_account_id: Some("acct-shared".to_string()),
        message: "quota exhausted".to_string(),
        rehydration: None,
    };

    let mut initial_watch_state = crate::watch::WatchState::default();
    let mut shared_state = initial_watch_state.account_state("acct-shared");
    shared_state.thread_recovery_pending = true;
    shared_state.thread_recovery_pending_events =
        vec![bound_source_event.clone(), unresolved_source_event.clone()];
    initial_watch_state.set_account_state("acct-shared", shared_state);
    crate::watch::write_watch_state(&initial_watch_state).expect("write initial watch state");

    let mut store = ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("store");
    store
        .bind_local_thread_id(
            "host-persona:source",
            "lineage-bound",
            "source-thread-bound",
        )
        .expect("bind source lineage");
    store
        .bind_local_thread_id(
            "host-persona:target",
            "lineage-bound",
            "target-thread-bound",
        )
        .expect("bind target lineage");

    translate_recovery_events_after_rotation_with_identity(
        "acct-shared",
        "acct-shared",
        "host-persona:source",
        "host-persona:target",
        9333,
        &[],
    )
    .expect("translate recovery events");

    let next_watch_state = crate::watch::read_watch_state().expect("read watch state");
    let next_shared_state = next_watch_state.account_state("acct-shared");
    let thread_ids = next_shared_state
        .thread_recovery_pending_events
        .iter()
        .map(|event| event.thread_id.as_str())
        .collect::<Vec<_>>();

    assert!(next_shared_state.thread_recovery_pending);
    assert_eq!(
        thread_ids,
        vec!["source-thread-unbound", "target-thread-bound"]
    );

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
}

#[test]
fn capture_source_recovery_before_rotation_records_session_turn_quota_event() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
    }
    fs::create_dir_all(&paths.codex_home).expect("create codex home");
    fs::create_dir_all(&paths.rotate_home).expect("create rotate home");

    let logs =
        rusqlite::Connection::open(paths.codex_home.join("logs_1.sqlite")).expect("open logs");
    logs.execute_batch(
            r#"
create table logs (
  id integer primary key,
  ts integer,
  target text,
  feedback_log_body text,
  thread_id text
);
insert into logs (id, ts, target, feedback_log_body, thread_id) values
  (
    100,
    1775445612,
    'codex_core::session::turn',
    'session_loop{thread_id=source-thread}:submission_dispatch{otel.name="op.dispatch.user_input_with_turn_context" submission.id="turn-source" codex.op="user_input_with_turn_context"}:turn{otel.name="session_task.turn" thread.id=source-thread turn.id=turn-source model=gpt-5.5}:run_turn: Turn error: You''ve hit your usage limit. To get more access now, send a request to your admin or try again later.',
    'source-thread'
  );
            "#,
        )
        .expect("seed logs");

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
        persist_pool: true,
    };

    write_watch_state(&crate::watch::WatchState::default()).expect("write watch state");
    capture_source_thread_recovery_events_before_rotation(&prepared, 9333)
        .expect("capture source recovery");

    let state = crate::watch::read_watch_state().expect("read watch state");
    let source_state = state.account_state("acct-source");
    assert_eq!(source_state.last_thread_recovery_log_id, Some(100));
    assert!(source_state.thread_recovery_pending);
    assert_eq!(source_state.thread_recovery_pending_events.len(), 1);
    assert_eq!(
        source_state.thread_recovery_pending_events[0].thread_id,
        "source-thread"
    );

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
}
