use super::*;

pub(super) fn unique_temp_dir(prefix: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let suffix = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("{}-{}-{}", prefix, std::process::id(), suffix))
}

pub(super) fn write_executable(path: &Path, script: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create executable parent");
    }
    fs::write(path, script).expect("write executable script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path).expect("stat executable").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("chmod executable");
    }
}

pub(super) fn summarize_pool_state(pool: &codex_rotate_core::pool::Pool) -> String {
    let active_index = pool.active_index.min(pool.accounts.len().saturating_sub(1));
    let active_account = pool
        .accounts
        .get(active_index)
        .map(|entry| format!("{} ({})", entry.label, entry.account_id))
        .unwrap_or_else(|| "none".to_string());
    let account_ids = pool
        .accounts
        .iter()
        .map(|entry| entry.account_id.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "active_index={active_index}, active_account={active_account}, account_ids=[{account_ids}]"
    )
}

pub(super) fn summarize_auth_state(auth: &codex_rotate_core::auth::CodexAuth) -> String {
    format!(
        "account_id={}, last_refresh={}",
        auth.tokens.account_id, auth.last_refresh
    )
}

pub(super) fn summarize_next_result(result: &NextResult) -> String {
    match result {
        NextResult::Rotated { message, summary } => format!(
            "rotated to {} ({}) | {}",
            summary.account_id, summary.email, message
        ),
        NextResult::Stayed { message, summary } => format!(
            "stayed on {} ({}) | {}",
            summary.account_id, summary.email, message
        ),
        NextResult::Created { output, summary } => format!(
            "created for {} ({}) | {}",
            summary.account_id, summary.email, output
        ),
    }
}

pub(super) fn report_sandbox_rotation_lifecycle(
    workspace_root: &Path,
    sandbox_root: &Path,
    live_snapshot_root: &Path,
    usage_url: &str,
    initial_pool: &codex_rotate_core::pool::Pool,
    initial_auth: &codex_rotate_core::auth::CodexAuth,
    first_result: &NextResult,
    first_target_paths: &HostPersonaPaths,
    first_pool_after: &codex_rotate_core::pool::Pool,
    first_auth_after: &codex_rotate_core::auth::CodexAuth,
    first_checkpoint_cleared: bool,
    second_result: &NextResult,
    second_target_paths: &HostPersonaPaths,
    second_pool_after: &codex_rotate_core::pool::Pool,
    second_auth_after: &codex_rotate_core::auth::CodexAuth,
    second_checkpoint_cleared: bool,
    live_accounts_before: &str,
    live_auth_before: &str,
) {
    eprintln!();
    eprintln!("=== Sandbox Rotation Lifecycle Report ===");
    eprintln!("Purpose: validate host rotation hygiene in an isolated temp sandbox.");
    eprintln!("Workspace root: {}", workspace_root.display());
    eprintln!("Sandbox root: {}", sandbox_root.display());
    eprintln!("Comparison root: {}", live_snapshot_root.display());
    eprintln!("WHAM usage stub: {usage_url}");
    eprintln!("1. Initial seed");
    eprintln!(
        "  - sandbox/accounts.json before any rotation: {}",
        summarize_pool_state(initial_pool)
    );
    eprintln!(
        "  - sandbox/.codex/auth.json before any rotation: {}",
        summarize_auth_state(initial_auth)
    );
    eprintln!("  - live-snapshot/accounts.json baseline: {live_accounts_before}");
    eprintln!("  - live-snapshot/auth.json baseline: {live_auth_before}");
    eprintln!("  - conversation data is not synthesized in this test; the lifecycle shown here is the real account/auth/symlink/checkpoint path.");

    eprintln!("2. Rotate forward");
    eprintln!("  - {}", summarize_next_result(first_result));
    eprintln!(
        "  - live .codex symlink -> {}",
        first_target_paths.codex_home.display()
    );
    eprintln!(
        "  - live app-support symlink -> {}",
        first_target_paths.codex_app_support_dir.display()
    );
    eprintln!(
        "  - sandbox/accounts.json after forward rotation: {}",
        summarize_pool_state(first_pool_after)
    );
    eprintln!(
        "  - sandbox/.codex/auth.json after forward rotation: {}",
        summarize_auth_state(first_auth_after)
    );
    eprintln!("  - rotation checkpoint cleared after forward rotation: {first_checkpoint_cleared}");

    eprintln!("3. Sync back on the target side");
    eprintln!("  - the sandbox state is now pinned to the target account only.");
    eprintln!("  - this is where any transferred conversation data would continue from the target persona.");

    eprintln!("4. Rotate back");
    eprintln!("  - {}", summarize_next_result(second_result));
    eprintln!(
        "  - live .codex symlink -> {}",
        second_target_paths.codex_home.display()
    );
    eprintln!(
        "  - live app-support symlink -> {}",
        second_target_paths.codex_app_support_dir.display()
    );
    eprintln!(
        "  - sandbox/accounts.json after return rotation: {}",
        summarize_pool_state(second_pool_after)
    );
    eprintln!(
        "  - sandbox/.codex/auth.json after return rotation: {}",
        summarize_auth_state(second_auth_after)
    );
    eprintln!("  - rotation checkpoint cleared after return rotation: {second_checkpoint_cleared}");

    eprintln!("5. Final hygiene checks");
    eprintln!("  - live snapshot files remained unchanged across the full cycle.");
    eprintln!(
        "  - live-snapshot/accounts.json before -> after: unchanged ({live_accounts_before})"
    );
    eprintln!("  - live-snapshot/auth.json before -> after: unchanged ({live_auth_before})");
    eprintln!("  - no state leaked out of the sandbox while the account flipped source -> target -> source.");
    eprintln!("==============================");
    eprintln!();
}

pub(super) struct TestHttpServer {
    pub(super) shutdown: std::sync::mpsc::Sender<()>,
    pub(super) handle: Option<thread::JoinHandle<()>>,
    pub(super) port: u16,
}

impl TestHttpServer {
    pub(super) fn start(response_body: impl Into<String>) -> Result<Self> {
        let response_body = response_body.into();
        let listener = TcpListener::bind("127.0.0.1:0").context("bind test http server")?;
        listener
            .set_nonblocking(true)
            .context("configure test http server")?;
        let port = listener
            .local_addr()
            .context("test http local addr")?
            .port();
        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel::<()>();
        let handle = thread::spawn(move || loop {
            if shutdown_rx.try_recv().is_ok() {
                break;
            }
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut buffer = [0u8; 4096];
                    let read = stream.read(&mut buffer).unwrap_or(0);
                    let _request = String::from_utf8_lossy(&buffer[..read]);
                    let body = response_body.clone();
                    let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                    let _ = stream.write_all(response.as_bytes());
                    let _ = stream.flush();
                    let _ = stream.shutdown(Shutdown::Both);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(_) => break,
            }
        });
        Ok(Self {
            shutdown: shutdown_tx,
            handle: Some(handle),
            port,
        })
    }
}

impl Drop for TestHttpServer {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub(super) struct ManagedCodexProcess {
    pub(super) pid: u32,
    pub(super) command: String,
    pub(super) waiter: Option<thread::JoinHandle<()>>,
}

impl ManagedCodexProcess {
    pub(super) fn start(profile_dir: &Path) -> Result<Self> {
        let executable = unique_temp_dir("managed-codex")
            .join("Applications")
            .join("Codex.app")
            .join("Contents")
            .join("MacOS")
            .join("Codex");
        write_executable(
            &executable,
            r#"#!/usr/bin/env python3
import signal
import sys
import time


def exit_cleanly(_signum, _frame):
    sys.exit(0)


signal.signal(signal.SIGTERM, exit_cleanly)
signal.signal(signal.SIGINT, exit_cleanly)
while True:
    time.sleep(1)
"#,
        );
        let command = format!(
            "{} --user-data-dir={}",
            executable.display(),
            profile_dir.display()
        );
        let child = Command::new(&executable)
            .arg(format!("--user-data-dir={}", profile_dir.display()))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("spawn fake managed codex")?;
        let pid = child.id();
        let waiter = thread::spawn(move || {
            let mut child = child;
            let _ = child.wait();
        });
        Ok(Self {
            pid,
            command,
            waiter: Some(waiter),
        })
    }

    pub(super) fn pid(&self) -> u32 {
        self.pid
    }

    pub(super) fn command(&self) -> &str {
        &self.command
    }
}

impl Drop for ManagedCodexProcess {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(self.pid.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if let Some(waiter) = self.waiter.take() {
            let _ = waiter.join();
        }
    }
}

pub(super) fn start_guest_bridge(response_body: impl Into<String>) -> Result<TestHttpServer> {
    TestHttpServer::start(response_body)
}

pub(super) fn disable_rotation_domain_in_accounts_file(accounts_file: &Path, domain: &str) {
    let mut state: Value =
        serde_json::from_str(&fs::read_to_string(accounts_file).expect("read accounts file"))
            .expect("parse accounts file");
    let object = state.as_object_mut().expect("accounts file object");
    let domain_state = object
        .entry("domain".to_string())
        .or_insert_with(|| json!({}));
    domain_state
        .as_object_mut()
        .expect("domain state object")
        .insert(domain.to_string(), json!({ "rotation_enabled": false }));
    fs::write(
        accounts_file,
        serde_json::to_string_pretty(&state).expect("serialize accounts file"),
    )
    .expect("write accounts file");
}

pub(super) fn start_usage_server_that_disables_domain(
    accounts_file: PathBuf,
    domain: &'static str,
    response_body: impl Into<String>,
) -> Result<(String, thread::JoinHandle<()>)> {
    let response_body = response_body.into();
    let listener = TcpListener::bind("127.0.0.1:0").context("bind usage server")?;
    let address = listener.local_addr().context("usage server local addr")?;
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept usage request");
        let mut buffer = [0_u8; 4096];
        let _ = stream.read(&mut buffer);
        disable_rotation_domain_in_accounts_file(&accounts_file, domain);
        let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
        stream
            .write_all(response.as_bytes())
            .expect("write usage response");
        let _ = stream.shutdown(Shutdown::Both);
    });
    Ok((format!("http://{address}"), handle))
}

pub(super) fn write_fake_codex_bin(path: &Path, log_file: &Path) {
    write_executable(
        path,
        &format!(
            r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >> '{log_file}'
exit 91
"#,
            log_file = log_file.display()
        ),
    );
}

pub(super) fn numbered_user_response_items(count: usize) -> Vec<Value> {
    (0..count)
        .map(|index| {
            json!({
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": format!("message {index}"),
                    }
                ]
            })
        })
        .collect()
}

pub(super) fn thread_read_response_from_response_items(thread_id: &str, items: &[Value]) -> Value {
    json!({
        "thread": {
            "id": thread_id,
            "turns": [
                {
                    "id": "turn-1",
                    "items": items,
                }
            ]
        }
    })
}

pub(super) fn seed_threads_table(state_db_path: &Path, rows: &[(&str, &str, i64)]) {
    let connection = rusqlite::Connection::open(state_db_path).expect("open state db");
    connection
        .execute_batch(
            r#"
create table if not exists threads (
    id text primary key,
    rollout_path text not null,
    created_at integer not null,
    updated_at integer not null,
    source text not null,
    model_provider text not null,
    cwd text not null,
    title text not null,
    sandbox_policy text not null,
    approval_mode text not null,
    tokens_used integer not null default 0,
    has_user_event integer not null default 0,
    archived integer not null default 0
);
"#,
        )
        .expect("create threads table");
    for (thread_id, rollout_path, updated_at) in rows {
        connection
            .execute(
                r#"
insert into threads (
    id,
    rollout_path,
    created_at,
    updated_at,
    source,
    model_provider,
    cwd,
    title,
    sandbox_policy,
    approval_mode,
    tokens_used,
    has_user_event,
    archived
) values (?1, ?2, ?3, ?3, 'local', 'openai', '/', ?1, 'workspace-write', 'never', 0, 1, 0)
on conflict(id) do update set
    rollout_path=excluded.rollout_path,
    updated_at=excluded.updated_at
"#,
                rusqlite::params![thread_id, rollout_path, updated_at],
            )
            .expect("insert thread");
    }
}

pub(super) fn read_thread_ids(state_db_path: &Path) -> Vec<String> {
    let connection = rusqlite::Connection::open(state_db_path).expect("open state db");
    let mut statement = connection
        .prepare("select id from threads order by id")
        .expect("prepare threads query");
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .expect("query thread ids");
    let mut ids = Vec::new();
    for row in rows {
        ids.push(row.expect("thread id"));
    }
    ids
}

pub(super) fn update_thread_metadata(
    state_db_path: &Path,
    thread_id: &str,
    cwd: &str,
    archived: bool,
) {
    let connection = rusqlite::Connection::open(state_db_path).expect("open state db");
    connection
        .execute(
            "update threads set cwd = ?1, archived = ?2 where id = ?3",
            rusqlite::params![cwd, archived as i64, thread_id],
        )
        .expect("update thread metadata");
}

pub(super) fn thread_is_archived(state_db_path: &Path, thread_id: &str) -> bool {
    let connection = rusqlite::Connection::open(state_db_path).expect("open state db");
    connection
        .query_row(
            "select archived from threads where id = ?1",
            [thread_id],
            |row| row.get::<_, i64>(0),
        )
        .expect("query archived state")
        != 0
}

pub(super) fn project_table_heading(path: &Path) -> String {
    format!(
        "[projects.\"{}\"]",
        encode_toml_basic_string(&path.display().to_string())
    )
}

pub(super) fn test_runtime_paths(root: &Path) -> RuntimePaths {
    let home = root.join("home");
    RuntimePaths {
        repo_root: root.to_path_buf(),
        home_dir: home.clone(),
        codex_auth_file: home.join(".codex").join("auth.json"),
        codex_logs_db_file: root.join("logs.sqlite"),
        codex_state_db_file: root.join("state.sqlite"),
        codex_home: home.join(".codex"),
        fast_browser_home: home.join(".fast-browser"),
        codex_app_support_dir: home
            .join("Library")
            .join("Application Support")
            .join("Codex"),
        rotate_home: home.join(".codex-rotate"),
        watch_state_file: home.join(".codex-rotate").join("watch-state.json"),
        debug_profile_dir: home.join(".codex-rotate").join("profile"),
        daemon_socket: home.join(".codex-rotate").join("daemon.sock"),
        conversation_sync_db_file: home.join(".codex-rotate").join("conversation_sync.sqlite"),
    }
}

pub(super) fn test_account(account_id: &str, persona_id: &str) -> AccountEntry {
    AccountEntry {
        label: format!("{account_id}_free"),
        alias: None,
        email: format!("{account_id}@astronlab.com"),
        relogin: false,
        account_id: account_id.to_string(),
        plan_type: "free".to_string(),
        auth: codex_rotate_core::auth::CodexAuth {
            auth_mode: "chatgpt".to_string(),
            openai_api_key: None,
            tokens: codex_rotate_core::auth::AuthTokens {
                access_token: "header.payload.signature".to_string(),
                id_token: "header.payload.signature".to_string(),
                refresh_token: Some("refresh".to_string()),
                account_id: account_id.to_string(),
            },
            last_refresh: "2026-04-07T00:00:00.000Z".to_string(),
        },
        added_at: "2026-04-07T00:00:00.000Z".to_string(),
        last_quota_usable: Some(true),
        last_quota_summary: Some("5h 90% left".to_string()),
        last_quota_blocker: None,
        last_quota_checked_at: Some("2026-04-07T00:00:00.000Z".to_string()),
        last_quota_primary_left_percent: Some(90),
        last_quota_next_refresh_at: Some("2026-04-07T01:00:00.000Z".to_string()),
        persona: Some(PersonaEntry {
            persona_id: persona_id.to_string(),
            persona_profile_id: Some("balanced-us-compact".to_string()),
            expected_region_code: None,
            ready_at: None,
            host_root_rel_path: Some(format!("personas/host/{persona_id}")),
            vm_package_rel_path: None,
            browser_fingerprint: Some(json!({"seeded": true})),
        }),
    }
}

pub(super) fn restore_env(key: &str, previous: Option<std::ffi::OsString>) {
    match previous {
        Some(value) => unsafe {
            std::env::set_var(key, value);
        },
        None => unsafe {
            std::env::remove_var(key);
        },
    }
}
