use super::*;

#[test]
fn guardrail_snapshot_sync_uses_cow_without_hard_links() {
    let code = include_str!("../host_snapshot.rs");
    let production_code = code
        .split_once("\n#[cfg(test)]\nmod tests {")
        .map(|(before, _)| before)
        .unwrap_or(code);
    assert!(production_code.contains("copy_file_best_effort_cow"));
    assert!(!production_code.contains("hard_link"));
}

#[test]
fn test_idempotent_additive_sync_and_uniqueness() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let lineage_id = "lineage-1";
    let store = ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("store");

    // Mock handoff from A
    let handoff = ThreadHandoff {
        source_thread_id: lineage_id.to_string(),
        lineage_id: lineage_id.to_string(),
        watermark: Some("turn-1".to_string()),
        cwd: None,
        items: vec![json!({"type": "text", "text": "hello"})],
        metadata: ThreadHandoffMetadata::default(),
    };

    let target_thread_id = "target-thread-1";
    let account_id_b = "acct-b";

    struct MockConversationTransport {
        thread_id: String,
    }
    impl ConversationTransport for MockConversationTransport {
        fn list_threads(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }
        fn read_thread(&self, _id: &str) -> Result<Value> {
            Ok(json!({}))
        }
        fn start_thread(&self, _cwd: Option<&str>) -> Result<String> {
            Ok(self.thread_id.clone())
        }
        fn inject_items(&self, _id: &str, _items: Vec<Value>) -> Result<()> {
            Ok(())
        }
    }

    let transport = MockConversationTransport {
        thread_id: target_thread_id.to_string(),
    };

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
    }

    let outcome =
        import_thread_handoffs(&transport, account_id_b, &[handoff.clone()], None).expect("import");
    assert!(outcome.is_complete());
    assert_eq!(
        outcome.completed_source_thread_ids,
        vec![lineage_id.to_string()]
    );

    // Verify binding and watermark
    let bound_id = store
        .get_local_thread_id(account_id_b, lineage_id)
        .expect("get")
        .expect("found");
    assert_eq!(bound_id, target_thread_id);
    assert_ne!(bound_id, lineage_id); // Uniqueness check

    let watermark = store
        .get_watermark(account_id_b, lineage_id)
        .expect("get")
        .expect("found");
    assert_eq!(watermark, "turn-1");

    // 2. Repeated sync without new content: no duplicate
    let outcome2 =
        import_thread_handoffs(&transport, account_id_b, &[handoff], None).expect("import 2");
    assert!(outcome2.is_complete());
    assert_eq!(
        outcome2.completed_source_thread_ids,
        vec![lineage_id.to_string()]
    );

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}

#[test]
fn host_conversation_sync_identity_distinguishes_same_account_personas() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let source = test_account("acct-team", "persona-source");
    let target = test_account("acct-team", "persona-target");
    let source_sync_identity = conversation_sync_identity(&source);
    let target_sync_identity = conversation_sync_identity(&target);
    assert_ne!(source_sync_identity, target_sync_identity);

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
    }

    let mut store = ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("store");
    store
        .bind_local_thread_id(&source_sync_identity, "lineage-team", "source-thread")
        .expect("bind source");

    struct MockConversationTransport;
    impl ConversationTransport for MockConversationTransport {
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
        lineage_id: "lineage-team".to_string(),
        watermark: Some("turn-1".to_string()),
        cwd: None,
        items: vec![],
        metadata: ThreadHandoffMetadata::default(),
    };
    let outcome = import_thread_handoffs(
        &MockConversationTransport,
        &target_sync_identity,
        &[handoff],
        None,
    )
    .expect("import");
    assert!(outcome.is_complete());
    assert_eq!(
        store
            .get_local_thread_id(&source_sync_identity, "lineage-team")
            .expect("get source"),
        Some("source-thread".to_string())
    );
    assert_eq!(
        store
            .get_local_thread_id(&target_sync_identity, "lineage-team")
            .expect("get target"),
        Some("target-thread".to_string())
    );
    assert_ne!(
        store
            .get_local_thread_id(&source_sync_identity, "lineage-team")
            .expect("get source")
            .unwrap(),
        store
            .get_local_thread_id(&target_sync_identity, "lineage-team")
            .expect("get target")
            .unwrap()
    );

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}

#[test]
fn map_thread_item_to_response_item_converts_user_and_agent_messages() {
    let message_user_item = json!({
        "type": "message",
        "role": "user",
        "content": [
            {
                "type": "input_text",
                "text": "hello message"
            }
        ]
    });
    let mapped_message_user = map_thread_item_to_response_item(&message_user_item).unwrap();
    assert_eq!(mapped_message_user["type"], "message");
    assert_eq!(mapped_message_user["role"], "user");
    assert_eq!(mapped_message_user["content"][0]["text"], "hello message");
    assert_eq!(mapped_message_user["content"][0]["type"], "input_text");

    let message_assistant_item = json!({
        "type": "message",
        "role": "assistant",
        "content": [
            {
                "type": "text",
                "text": "hello assistant"
            }
        ]
    });
    let mapped_message_assistant =
        map_thread_item_to_response_item(&message_assistant_item).unwrap();
    assert_eq!(mapped_message_assistant["type"], "message");
    assert_eq!(mapped_message_assistant["role"], "assistant");
    assert_eq!(
        mapped_message_assistant["content"][0]["text"],
        "hello assistant"
    );
    assert_eq!(
        mapped_message_assistant["content"][0]["type"],
        "output_text"
    );

    let user_item = json!({
        "type": "userMessage",
        "content": [
            {
                "type": "text",
                "text": "hello"
            }
        ]
    });
    let mapped_user = map_thread_item_to_response_item(&user_item).unwrap();
    assert_eq!(mapped_user["type"], "message");
    assert_eq!(mapped_user["role"], "user");
    assert_eq!(mapped_user["content"][0]["text"], "hello");

    let agent_item = json!({
        "type": "agentMessage",
        "text": "hi there"
    });
    let mapped_agent = map_thread_item_to_response_item(&agent_item).unwrap();
    assert_eq!(mapped_agent["type"], "message");
    assert_eq!(mapped_agent["role"], "assistant");
    assert_eq!(mapped_agent["content"][0]["text"], "hi there");
}

#[test]
fn truncate_handoff_text_enforces_max_limit() {
    let long_text = "a".repeat(MAX_HANDOFF_TEXT_CHARS + 10);
    let truncated = truncate_handoff_text(&long_text);
    assert!(truncated.contains("[… truncated]"));
    assert_eq!(
        truncated.chars().count(),
        MAX_HANDOFF_TEXT_CHARS + "\n[… truncated]".chars().count()
    );

    let short_text = "short";
    assert_eq!(truncate_handoff_text(short_text), "short");
}

#[test]
fn state_backed_export_skips_archived_threads_without_pending_recovery() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    let state_db = paths.codex_home.join("state_5.sqlite");
    fs::create_dir_all(&paths.codex_home).expect("create codex home");
    seed_threads_table(
        &state_db,
        &[
            ("thread-old", "/tmp/old.jsonl", 10),
            ("thread-new", "/tmp/new.jsonl", 30),
            ("thread-archived", "/tmp/archived.jsonl", 20),
        ],
    );
    update_thread_metadata(&state_db, "thread-archived", "/tmp/archived", true);

    assert_eq!(
        read_thread_handoff_candidate_ids_from_state_db(&state_db).expect("read candidates"),
        vec![
            "thread-new".to_string(),
            "thread-old".to_string(),
            "thread-archived".to_string(),
        ]
    );

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
    }

    struct ExportMockTransport {
        responses: BTreeMap<String, Value>,
    }
    impl ConversationTransport for ExportMockTransport {
        fn list_threads(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }
        fn read_thread(&self, id: &str) -> Result<Value> {
            self.responses
                .get(id)
                .cloned()
                .ok_or_else(|| anyhow!("thread not found"))
        }
        fn start_thread(&self, _cwd: Option<&str>) -> Result<String> {
            unreachable!("export should not start threads")
        }
        fn inject_items(&self, _id: &str, _items: Vec<Value>) -> Result<()> {
            unreachable!("export should not inject items")
        }
    }
    fn thread_response(id: &str) -> Value {
        json!({
            "thread": {
                "id": id,
                "cwd": "/tmp/project",
                "preview": format!("preview {id}"),
                "turns": [
                    {
                        "id": format!("turn-{id}"),
                        "items": [
                            {
                                "type": "userMessage",
                                "content": [
                                    { "type": "text", "text": format!("hello {id}") }
                                ]
                            }
                        ]
                    }
                ]
            }
        })
    }
    let transport = ExportMockTransport {
        responses: BTreeMap::from([
            ("thread-new".to_string(), thread_response("thread-new")),
            ("thread-old".to_string(), thread_response("thread-old")),
            (
                "thread-archived".to_string(),
                thread_response("thread-archived"),
            ),
        ]),
    };
    let continue_thread_ids = BTreeSet::from(["thread-new".to_string()]);
    let pending_thread_ids = BTreeSet::new();
    let handoffs = export_thread_handoffs_from_candidates(
        &transport,
        "source-sync",
        vec![
            "thread-new".to_string(),
            "thread-old".to_string(),
            "thread-archived".to_string(),
            "thread-missing".to_string(),
        ],
        &continue_thread_ids,
        &pending_thread_ids,
    )
    .expect("export handoffs");

    assert_eq!(
        handoffs
            .iter()
            .map(|handoff| handoff.source_thread_id.as_str())
            .collect::<Vec<_>>(),
        vec!["thread-new", "thread-old"]
    );
    let serialized = serde_json::to_value(&handoffs[0]).expect("serialize handoff");
    assert!(
        serialized.get("continue_prompt").is_none(),
        "conversation sync must not carry an invented continuation prompt"
    );
    assert!(
        serialized.get("continuePrompt").is_none(),
        "conversation sync must not carry an invented continuation prompt"
    );
    let store =
        ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("open sync store");
    for thread_id in ["thread-new", "thread-old", "thread-archived"] {
        assert_eq!(
            store
                .get_lineage_id("source-sync", thread_id)
                .expect("source lineage"),
            Some(thread_id.to_string())
        );
    }

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
}

#[test]
fn export_skips_stale_or_unmaterialized_handoff_candidates() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
    }

    struct ExportUnavailableTransport;
    impl ConversationTransport for ExportUnavailableTransport {
        fn list_threads(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }

        fn read_thread(&self, id: &str) -> Result<Value> {
            match id {
                "thread-good" => Ok(json!({
                    "thread": {
                        "id": id,
                        "cwd": "/tmp/project",
                        "turns": [
                            {
                                "id": "turn-1",
                                "items": [
                                    {
                                        "type": "userMessage",
                                        "content": [{ "type": "text", "text": "hello" }]
                                    }
                                ]
                            }
                        ]
                    }
                })),
                "thread-not-loaded" => Err(anyhow!(
                    "Codex thread/read request failed: {{\"code\":-32600,\"message\":\"thread not loaded: thread-not-loaded\"}}"
                )),
                "thread-unmaterialized" => Err(anyhow!(
                    "Codex thread/read request failed: {{\"code\":-32600,\"message\":\"thread thread-unmaterialized is not materialized yet; includeTurns is unavailable before first user message\"}}"
                )),
                other => Err(anyhow!("unexpected thread {other}")),
            }
        }

        fn start_thread(&self, _cwd: Option<&str>) -> Result<String> {
            unreachable!("export should not start threads")
        }

        fn inject_items(&self, _id: &str, _items: Vec<Value>) -> Result<()> {
            unreachable!("export should not inject items")
        }
    }

    let handoffs = export_thread_handoffs_from_candidates(
        &ExportUnavailableTransport,
        "source-sync",
        vec![
            "thread-good".to_string(),
            "thread-not-loaded".to_string(),
            "thread-unmaterialized".to_string(),
        ],
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
    .expect("export should skip stale candidates");

    assert_eq!(
        handoffs
            .iter()
            .map(|handoff| handoff.source_thread_id.as_str())
            .collect::<Vec<_>>(),
        vec!["thread-good"]
    );

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}

#[test]
fn export_propagates_non_recoverable_handoff_read_errors() {
    struct ExportTimeoutTransport;
    impl ConversationTransport for ExportTimeoutTransport {
        fn list_threads(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }

        fn read_thread(&self, _id: &str) -> Result<Value> {
            Err(anyhow!(
                "Timed out waiting for thread/read response from Codex."
            ))
        }

        fn start_thread(&self, _cwd: Option<&str>) -> Result<String> {
            unreachable!("export should not start threads")
        }

        fn inject_items(&self, _id: &str, _items: Vec<Value>) -> Result<()> {
            unreachable!("export should not inject items")
        }
    }

    let error = export_thread_handoffs_from_candidates(
        &ExportTimeoutTransport,
        "source-sync",
        vec!["thread-timeout".to_string()],
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
    .expect_err("unexpected read errors should still fail export");

    assert!(format!("{error:#}").contains("Timed out waiting for thread/read response"));
}

#[test]
fn state_backed_export_keeps_archived_pending_recovery_threads() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());
    fs::create_dir_all(&paths.codex_home).expect("create codex home");
    let state_db = paths.codex_home.join("state_5.sqlite");
    seed_threads_table(&state_db, &[("thread-archived", "/tmp/archived.jsonl", 20)]);
    update_thread_metadata(&state_db, "thread-archived", "/tmp/archived", true);

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        std::env::set_var("CODEX_HOME", paths.codex_home.clone());
    }

    struct ExportMockTransport;
    impl ConversationTransport for ExportMockTransport {
        fn list_threads(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }
        fn read_thread(&self, id: &str) -> Result<Value> {
            Ok(json!({
                "thread": {
                    "id": id,
                    "cwd": "/tmp/project",
                    "turns": [
                        {
                            "id": format!("turn-{id}"),
                            "items": [
                                {
                                    "type": "userMessage",
                                    "content": [
                                        { "type": "text", "text": format!("hello {id}") }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            }))
        }
        fn start_thread(&self, _cwd: Option<&str>) -> Result<String> {
            unreachable!("export should not start threads")
        }
        fn inject_items(&self, _id: &str, _items: Vec<Value>) -> Result<()> {
            unreachable!("export should not inject items")
        }
    }

    let continue_thread_ids = BTreeSet::new();
    let pending_thread_ids = BTreeSet::from(["thread-archived".to_string()]);
    let handoffs = export_thread_handoffs_from_candidates(
        &ExportMockTransport,
        "source-sync",
        vec!["thread-archived".to_string()],
        &continue_thread_ids,
        &pending_thread_ids,
    )
    .expect("export handoffs");

    assert_eq!(
        handoffs
            .iter()
            .map(|handoff| handoff.source_thread_id.as_str())
            .collect::<Vec<_>>(),
        vec!["thread-archived"]
    );
    assert_eq!(handoffs[0].metadata.archived, Some(true));

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    restore_env("CODEX_HOME", previous_codex_home);
}

#[test]
fn exported_handoff_watermark_is_stable_across_persona_local_thread_ids() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
    }

    fn thread_response(thread_id: &str, turn_id: &str) -> Value {
        json!({
            "thread": {
                "id": thread_id,
                "cwd": "/tmp/project",
                "turns": [
                    {
                        "id": turn_id,
                        "items": [
                            {
                                "type": "userMessage",
                                "content": [
                                    { "type": "text", "text": "Greet user" }
                                ]
                            }
                        ]
                    }
                ]
            }
        })
    }

    let source_handoff = export_single_thread_handoff_from_response(
        thread_response("source-local-thread", "source-local-turn"),
        "source-local-thread",
        "source-sync",
    )
    .expect("export source")
    .expect("source handoff");
    let source_watermark = source_handoff
        .watermark
        .clone()
        .expect("source content watermark");
    assert!(source_watermark.starts_with("items-fnv1a64-"));

    let mut store =
        ConversationSyncStore::new(&paths.conversation_sync_db_file).expect("open store");
    assert_eq!(
        store
            .get_watermark("source-sync", "source-local-thread")
            .expect("source watermark"),
        Some(source_watermark.clone())
    );
    store
        .bind_and_update_watermark(
            "target-sync",
            "source-local-thread",
            "target-local-thread",
            Some(&source_watermark),
        )
        .expect("seed target binding");

    let target_handoff = export_single_thread_handoff_from_response(
        thread_response("target-local-thread", "target-local-turn"),
        "target-local-thread",
        "target-sync",
    )
    .expect("export target")
    .expect("target handoff");
    assert_eq!(target_handoff.lineage_id, source_handoff.lineage_id);
    assert_eq!(target_handoff.watermark, Some(source_watermark));

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}

#[test]
fn export_handoff_preserves_full_mapped_history() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
    }

    let long_text = "x".repeat(MAX_HANDOFF_TEXT_CHARS + 128);
    let turns = (0..64)
        .map(|index| {
            let text = if index == 0 {
                long_text.clone()
            } else {
                format!("message {index}")
            };
            json!({
                "id": format!("turn-{index}"),
                "items": [
                    {
                        "type": "userMessage",
                        "content": [{ "type": "text", "text": text }]
                    }
                ]
            })
        })
        .collect::<Vec<_>>();

    let handoff = export_single_thread_handoff_from_response(
        json!({ "thread": { "id": "source-thread", "turns": turns } }),
        "source-thread",
        "source-sync",
    )
    .expect("export")
    .expect("handoff");

    assert_eq!(handoff.items.len(), 64);
    assert_eq!(
        handoff.watermark.as_deref(),
        thread_handoff_content_watermark(&handoff.items).as_deref()
    );
    assert!(handoff.watermark.as_deref().unwrap().ends_with("-64"));
    let first_text = handoff.items[0]["content"][0]["text"]
        .as_str()
        .expect("first item text");
    assert_eq!(first_text.len(), MAX_HANDOFF_TEXT_CHARS + 128);

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}

#[test]
fn export_handoff_captures_projectless_ui_metadata() {
    let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
    let temp = tempdir().expect("tempdir");
    let paths = test_runtime_paths(temp.path());

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
    }

    struct ProjectlessExportTransport;
    impl ConversationTransport for ProjectlessExportTransport {
        fn list_threads(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }
        fn read_thread(&self, id: &str) -> Result<Value> {
            Ok(json!({
                "thread": {
                    "id": id,
                    "cwd": "/tmp/source-projectless-root",
                    "turns": [
                        {
                            "id": "turn-1",
                            "items": [
                                {
                                    "type": "userMessage",
                                    "content": [
                                        { "type": "text", "text": "hi" }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            }))
        }
        fn start_thread(&self, _cwd: Option<&str>) -> Result<String> {
            unreachable!("export should not start threads")
        }
        fn inject_items(&self, _id: &str, _items: Vec<Value>) -> Result<()> {
            unreachable!("export should not inject items")
        }
        fn read_thread_ui_metadata(&self, thread_id: &str) -> Result<ThreadHandoffMetadata> {
            assert_eq!(thread_id, "source-thread");
            Ok(ThreadHandoffMetadata {
                projectless: Some(true),
                workspace_root_hint: Some("/tmp/source-projectless-root".to_string()),
                ..ThreadHandoffMetadata::default()
            })
        }
    }

    let handoff = export_single_thread_handoff_with_identity(
        &ProjectlessExportTransport,
        "source-thread",
        "source-sync",
    )
    .expect("export")
    .expect("handoff");

    assert_eq!(handoff.metadata.projectless, Some(true));
    assert_eq!(
        handoff.metadata.workspace_root_hint.as_deref(),
        Some("/tmp/source-projectless-root")
    );

    restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
}

#[test]
fn thread_handoff_ignores_legacy_continue_prompt_field() {
    let handoff: ThreadHandoff = serde_json::from_value(json!({
            "source_thread_id": "source-thread",
            "lineage_id": "lineage",
            "watermark": "turn-1",
            "cwd": "/tmp/project",
            "items": [],
            "metadata": {},
            "continue_prompt": "Continue this transferred conversation from its latest unfinished state.",
            "continuePrompt": "Continue this transferred conversation from its latest unfinished state."
        }))
        .expect("legacy handoff should deserialize without continuation behavior");

    assert_eq!(handoff.source_thread_id, "source-thread");
    let serialized = serde_json::to_value(handoff).expect("serialize handoff");
    assert!(serialized.get("continue_prompt").is_none());
    assert!(serialized.get("continuePrompt").is_none());
}
