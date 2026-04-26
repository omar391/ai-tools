use super::*;

#[test]
fn publish_thread_sidebar_metadata_updates_state_db_and_session_index() {
    let temp = tempdir().expect("tempdir");
    let codex_home = temp.path().join("codex-home");
    fs::create_dir_all(&codex_home).expect("create codex home");
    let state_db = codex_home.join("state_5.sqlite");
    let connection = rusqlite::Connection::open(&state_db).expect("open state db");
    connection
        .execute_batch(
            r#"
create table threads (
    id text primary key,
    title text not null default '',
    first_user_message text not null default '',
    updated_at integer not null default 0,
    updated_at_ms integer,
    has_user_event integer not null default 0
);
insert into threads (id) values ('target-thread');
"#,
        )
        .expect("seed state db");
    drop(connection);

    publish_thread_sidebar_metadata(
        &codex_home,
        "target-thread",
        &ThreadHandoffMetadata {
            title: Some("Greet user".to_string()),
            first_user_message: Some("hi".to_string()),
            updated_at: Some(1_777_038_485),
            updated_at_ms: None,
            session_index_updated_at: Some("2026-04-24T13:48:05.018117Z".to_string()),
            ..ThreadHandoffMetadata::default()
        },
    )
    .expect("publish metadata");

    let connection = rusqlite::Connection::open(&state_db).expect("reopen state db");
    let row = connection
            .query_row(
                "select title, first_user_message, has_user_event, updated_at from threads where id = 'target-thread'",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .expect("query target row");
    assert_eq!(
        row,
        ("Greet user".to_string(), "hi".to_string(), 1, 1_777_038_485)
    );

    let index =
        fs::read_to_string(codex_home.join("session_index.jsonl")).expect("read session index");
    assert!(index.contains("\"id\":\"target-thread\""));
    assert!(index.contains("\"thread_name\":\"Greet user\""));
    assert!(index.contains("\"updated_at\":\"2026-04-24T13:48:05.018117Z\""));
}

#[test]
fn handoff_metadata_cleanup_removes_stale_source_id_from_target_persona() {
    let temp = tempdir().expect("tempdir");
    let codex_home = temp.path().join("codex-home");
    let sessions = codex_home.join("sessions/2026/04/24");
    let archived_sessions = codex_home.join("archived_sessions/2026/04/24");
    let shell_snapshots = codex_home.join("shell_snapshots");
    fs::create_dir_all(&sessions).expect("create sessions");
    fs::create_dir_all(&archived_sessions).expect("create archived sessions");
    fs::create_dir_all(&shell_snapshots).expect("create shell snapshots");
    let source_thread_id = "019dbfbf-55e7-7421-9d81-5911ba464259";
    let target_thread_id = "019dc06b-f17e-7e11-9ffa-504142c12c82";
    let stale_rollout = sessions.join(format!("rollout-{source_thread_id}.jsonl"));
    let target_rollout = sessions.join(format!("rollout-{target_thread_id}.jsonl"));
    let stale_archived = archived_sessions.join(format!("rollout-{source_thread_id}.jsonl"));
    let stale_snapshot = shell_snapshots.join(format!("{source_thread_id}.json"));
    fs::write(&stale_rollout, "{}\n").expect("write stale rollout");
    fs::write(&target_rollout, "{}\n").expect("write target rollout");
    fs::write(&stale_archived, "{}\n").expect("write stale archived rollout");
    fs::write(&stale_snapshot, "{}\n").expect("write stale snapshot");

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
    updated_at_ms integer,
    source text not null,
    model_provider text not null,
    cwd text not null,
    title text not null,
    first_user_message text not null default '',
    sandbox_policy text not null,
    approval_mode text not null,
    tokens_used integer not null default 0,
    has_user_event integer not null default 0,
    archived integer not null default 0
);
"#,
        )
        .expect("create threads table");
    connection
            .execute(
                "insert into threads (id, rollout_path, created_at, updated_at, source, model_provider, cwd, title, first_user_message, sandbox_policy, approval_mode, has_user_event, archived) values (?1, ?2, 1, 1, 'vscode', 'openai', '/', 'Greet user', 'hi', 'workspace-write', 'never', 1, 0)",
                rusqlite::params![source_thread_id, stale_rollout.display().to_string()],
            )
            .expect("insert stale source row");
    connection
            .execute(
                "insert into threads (id, rollout_path, created_at, updated_at, source, model_provider, cwd, title, first_user_message, sandbox_policy, approval_mode, has_user_event, archived) values (?1, ?2, 1, 1, 'vscode', 'openai', '/', '', '', 'workspace-write', 'never', 0, 0)",
                rusqlite::params![target_thread_id, target_rollout.display().to_string()],
            )
            .expect("insert target row");
    drop(connection);
    fs::write(
            codex_home.join("session_index.jsonl"),
            format!(
                "{{\"id\":\"{source_thread_id}\",\"thread_name\":\"Greet user\",\"updated_at\":\"2026-04-24T13:48:05Z\"}}\n{{\"id\":\"{target_thread_id}\",\"thread_name\":\"\",\"updated_at\":\"2026-04-24T13:48:06Z\"}}\n"
            ),
        )
        .expect("write session index");

    cleanup_stale_thread_handoff_source(&codex_home, target_thread_id, source_thread_id)
        .expect("cleanup stale source");
    publish_thread_sidebar_metadata(
        &codex_home,
        target_thread_id,
        &ThreadHandoffMetadata {
            title: Some("Greet user".to_string()),
            first_user_message: Some("hi".to_string()),
            updated_at: Some(1_777_038_485),
            session_index_updated_at: Some("2026-04-24T13:48:05.018117Z".to_string()),
            ..ThreadHandoffMetadata::default()
        },
    )
    .expect("publish target metadata");

    let connection = rusqlite::Connection::open(&state_db).expect("reopen state db");
    let stale_count: i64 = connection
        .query_row(
            "select count(*) from threads where id = ?1",
            [source_thread_id],
            |row| row.get(0),
        )
        .expect("query stale count");
    assert_eq!(stale_count, 0);
    let target_row = connection
        .query_row(
            "select title, first_user_message, has_user_event, archived from threads where id = ?1",
            [target_thread_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .expect("query target row");
    assert_eq!(
        target_row,
        ("Greet user".to_string(), "hi".to_string(), 1, 0)
    );

    let index = fs::read_to_string(codex_home.join("session_index.jsonl")).expect("read index");
    assert!(!index.contains(source_thread_id));
    assert!(index.contains(target_thread_id));
    assert!(!stale_rollout.exists());
    assert!(!stale_archived.exists());
    assert!(!stale_snapshot.exists());
    assert!(target_rollout.exists());
}

#[test]
fn sidebar_visibility_accepts_existing_nonempty_first_user_preview() {
    let temp = tempdir().expect("tempdir");
    let codex_home = temp.path().join("codex-home");
    fs::create_dir_all(&codex_home).expect("create codex home");
    let state_db = codex_home.join("state_5.sqlite");
    let connection = rusqlite::Connection::open(&state_db).expect("open state db");
    connection
        .execute_batch(
            r#"
create table threads (
    id text primary key,
    title text not null default '',
    first_user_message text not null default ''
);
insert into threads (id, title, first_user_message)
values ('target-thread', 'Greet user', 'hi');
"#,
        )
        .expect("seed state db");
    drop(connection);

    assert!(thread_sidebar_metadata_visible(
        &codex_home,
        "target-thread",
        &ThreadHandoffMetadata {
            title: Some("Greet user".to_string()),
            first_user_message: Some("different imported preview".to_string()),
            ..ThreadHandoffMetadata::default()
        },
    )
    .expect("check visibility"));
}

#[test]
fn publish_thread_sidebar_metadata_replaces_legacy_transfer_prompt_preview() {
    let temp = tempdir().expect("tempdir");
    let codex_home = temp.path().join("codex-home");
    fs::create_dir_all(&codex_home).expect("create codex home");
    let state_db = codex_home.join("state_5.sqlite");
    let legacy_prompt = "Continue this transferred conversation from its latest unfinished state. The prior history came from another isolated persona.";
    let connection = rusqlite::Connection::open(&state_db).expect("open state db");
    connection
        .execute_batch(
            r#"
create table threads (
    id text primary key,
    title text not null default '',
    first_user_message text not null default '',
    has_user_event integer not null default 0
);
"#,
        )
        .expect("create threads table");
    connection
        .execute(
            "insert into threads (id, title, first_user_message) values ('target-thread', ?1, ?1)",
            [legacy_prompt],
        )
        .expect("seed legacy prompt");
    drop(connection);

    publish_thread_sidebar_metadata(
        &codex_home,
        "target-thread",
        &ThreadHandoffMetadata {
            title: Some("Actual preview".to_string()),
            first_user_message: Some("actual user text".to_string()),
            ..ThreadHandoffMetadata::default()
        },
    )
    .expect("publish metadata");

    let connection = rusqlite::Connection::open(&state_db).expect("reopen state db");
    let row = connection
            .query_row(
                "select title, first_user_message, has_user_event from threads where id = 'target-thread'",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .expect("query target row");
    assert_eq!(
        row,
        (
            "Actual preview".to_string(),
            "actual user text".to_string(),
            1
        )
    );
}

#[test]
fn thread_not_found_message_accepts_host_rollout_errors() {
    assert!(thread_not_found_message(
        "thread not found: stale-target-thread"
    ));
    assert!(thread_not_found_message(
        "Codex thread/read request failed: no rollout found for thread id stale-target-thread"
    ));
    assert!(thread_not_found_message(
        "No thread found for stale-target-thread"
    ));
    assert!(thread_not_found_message(
        "unknown thread stale-target-thread"
    ));
    assert!(thread_not_found_message(
        "thread stale-target-thread does not exist"
    ));
    assert!(thread_not_found_message(
            "thread-store internal error: failed to load thread history : No such file or directory (os error 2)"
        ));
    assert!(is_terminal_thread_read_error(&anyhow!(
            "Codex thread/read request failed: thread-store internal error: failed to load thread history : No such file or directory (os error 2)"
        )));
}

#[test]
fn archived_state_db_row_keeps_thread_binding_existing_without_rollout() {
    let temp = tempdir().expect("tempdir");
    let codex_home = temp.path().join("codex-home");
    fs::create_dir_all(&codex_home).expect("create codex home");
    let state_db = codex_home.join("state_5.sqlite");
    let existing_rollout = codex_home.join("sessions/rollout-active-existing.jsonl");
    fs::create_dir_all(existing_rollout.parent().unwrap()).expect("create sessions");
    fs::write(&existing_rollout, "{}\n").expect("write rollout");
    let connection = rusqlite::Connection::open(&state_db).expect("open state db");
    connection
        .execute_batch(
            r#"
create table threads (
    id text primary key,
    rollout_path text not null default '',
    archived integer not null default 0
);
"#,
        )
        .expect("create threads table");
    connection
        .execute(
            "insert into threads (id, rollout_path, archived) values (?1, ?2, ?3)",
            rusqlite::params!["archived-thread", "/missing/rollout.jsonl", 1],
        )
        .expect("insert archived");
    connection
        .execute(
            "insert into threads (id, rollout_path, archived) values (?1, ?2, ?3)",
            rusqlite::params!["active-missing", "/missing/rollout.jsonl", 0],
        )
        .expect("insert active missing");
    connection
        .execute(
            "insert into threads (id, rollout_path, archived) values (?1, ?2, ?3)",
            rusqlite::params!["active-existing", existing_rollout.display().to_string(), 0],
        )
        .expect("insert active existing");
    drop(connection);

    assert!(
        thread_state_db_indicates_existing_thread(&codex_home, "archived-thread")
            .expect("archived exists")
    );
    assert!(
        !thread_state_db_indicates_existing_thread(&codex_home, "active-missing")
            .expect("active missing")
    );
    assert!(
        thread_state_db_indicates_existing_thread(&codex_home, "active-existing")
            .expect("active existing")
    );
}

#[test]
fn publish_thread_sidebar_metadata_inserts_missing_state_row_for_imported_thread() {
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
    updated_at_ms integer,
    source text not null,
    model_provider text not null,
    cwd text not null,
    title text not null,
    first_user_message text not null default '',
    sandbox_policy text not null,
    approval_mode text not null,
    tokens_used integer not null default 0,
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
            title: Some("Greet user".to_string()),
            first_user_message: Some("hi".to_string()),
            updated_at: Some(1_777_038_485),
            updated_at_ms: None,
            session_index_updated_at: Some("2026-04-24T13:48:05.018117Z".to_string()),
            source: Some("vscode".to_string()),
            model_provider: Some("openai".to_string()),
            cwd: Some("/tmp/project".to_string()),
            sandbox_policy: Some("workspace-write".to_string()),
            approval_mode: Some("on-request".to_string()),
            ..ThreadHandoffMetadata::default()
        },
    )
    .expect("publish metadata");

    let connection = rusqlite::Connection::open(&state_db).expect("reopen state db");
    let row = connection
            .query_row(
                "select title, first_user_message, has_user_event, archived, rollout_path, source, cwd, approval_mode from threads where id = ?1",
                [target_thread_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .expect("query inserted target row");
    assert_eq!(row.0, "Greet user");
    assert_eq!(row.1, "hi");
    assert_eq!(row.2, 1);
    assert_eq!(row.3, 0);
    assert_eq!(row.4, rollout_path.display().to_string());
    assert_eq!(row.5, "vscode");
    assert_eq!(row.6, "/tmp/project");
    assert_eq!(row.7, "on-request");
}

#[test]
fn publish_thread_sidebar_metadata_derives_user_preview_from_rollout() {
    let temp = tempdir().expect("tempdir");
    let codex_home = temp.path().join("codex-home");
    let sessions = codex_home.join("sessions");
    fs::create_dir_all(&sessions).expect("create sessions");
    let target_thread_id = "target-thread-local";
    let rollout_path = sessions.join(format!("rollout-{target_thread_id}.jsonl"));
    fs::write(
        &rollout_path,
        serde_json::to_string(&json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "hi from rollout"}],
            }
        }))
        .expect("serialize rollout")
            + "\n",
    )
    .expect("write rollout");
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
    source text not null,
    model_provider text not null,
    cwd text not null,
    title text not null,
    first_user_message text not null default '',
    sandbox_policy text not null,
    approval_mode text not null,
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
            updated_at: Some(1_777_038_485),
            ..ThreadHandoffMetadata::default()
        },
    )
    .expect("publish metadata");

    let connection = rusqlite::Connection::open(&state_db).expect("reopen state db");
    let row = connection
        .query_row(
            "select title, first_user_message, has_user_event from threads where id = ?1",
            [target_thread_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .expect("query inserted target row");
    assert_eq!(row.0, "hi from rollout");
    assert_eq!(row.1, "hi from rollout");
    assert_eq!(row.2, 1);
}
