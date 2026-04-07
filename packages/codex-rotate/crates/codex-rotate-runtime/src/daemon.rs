use std::fs;
use std::io::BufReader;
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use codex_rotate_core::auth::{load_codex_auth, summarize_codex_auth};
use codex_rotate_core::pool::{
    cmd_add, cmd_list, cmd_prev, cmd_remove, cmd_status, load_pool, rotate_next_internal,
    NextResult,
};
use codex_rotate_core::quota::CachedQuotaState;
use codex_rotate_core::workflow::{
    cmd_create, cmd_relogin, migrate_legacy_credential_store_if_needed, CreateCommandOptions,
    CreateCommandSource, ReloginOptions,
};

use crate::dev_refresh::{
    current_process_local_cli_build, daemon_socket_is_older_than_binary,
    local_cli_sources_newer_than_binary, rebuild_local_cli,
};
use crate::hook::{read_live_account, switch_live_account_to_current_auth};
use crate::ipc::{
    read_request, write_message, ClientRequest, CreateInvocation, InvokeAction,
    RuntimeCapabilities, ServerMessage, StatusSnapshot,
};
use crate::launcher::ensure_debug_codex_instance;
use crate::paths::{legacy_rotate_app_home, resolve_paths};
use crate::watch::{refresh_quota_cache, run_watch_iteration, WatchIterationOptions, WatchState};

const DEFAULT_PORT: u16 = 9333;
const DEFAULT_INTERVAL_SECONDS: u64 = 15;
const LOW_QUOTA_INTERVAL_SECONDS: u64 = 5;
const CRITICAL_QUOTA_INTERVAL_SECONDS: u64 = 2;
const DAEMON_TAKEOVER_ENV: &str = "CODEX_ROTATE_DAEMON_TAKEOVER";
const DAEMON_TAKEOVER_TIMEOUT: Duration = Duration::from_secs(10);
const DAEMON_TAKEOVER_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Default)]
struct DaemonState {
    snapshot: StatusSnapshot,
    quota_cache: Option<CachedQuotaState>,
}

impl DaemonState {
    fn new() -> Self {
        Self {
            snapshot: StatusSnapshot {
                capabilities: RuntimeCapabilities::current(),
                ..StatusSnapshot::default()
            },
            quota_cache: None,
        }
    }
}

#[derive(Clone, Default)]
struct SharedDaemon {
    state: Arc<Mutex<DaemonState>>,
    subscribers: Arc<Mutex<Vec<Sender<StatusSnapshot>>>>,
}

impl SharedDaemon {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(DaemonState::new())),
            subscribers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn snapshot(&self) -> StatusSnapshot {
        self.state
            .lock()
            .expect("daemon state mutex")
            .snapshot
            .clone()
    }

    fn broadcast(&self) {
        let snapshot = self.snapshot();
        let mut subscribers = self.subscribers.lock().expect("subscriber mutex");
        subscribers.retain(|sender| sender.send(snapshot.clone()).is_ok());
    }

    fn add_subscriber(&self, sender: Sender<StatusSnapshot>) {
        self.subscribers
            .lock()
            .expect("subscriber mutex")
            .push(sender);
    }
}

fn managed_codex_port() -> u16 {
    std::env::var("CODEX_ROTATE_DEBUG_PORT")
        .ok()
        .and_then(|value| value.trim().parse::<u16>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_PORT)
}

pub fn run_daemon_forever() -> Result<()> {
    if maybe_refresh_local_daemon_process()? {
        return Ok(());
    }
    migrate_runtime_state()?;

    #[cfg(unix)]
    {
        let paths = resolve_paths()?;
        fs::create_dir_all(&paths.rotate_home)
            .with_context(|| format!("Failed to create {}.", paths.rotate_home.display()))?;

        if takeover_requested() {
            wait_for_previous_daemon_to_release_socket();
        }
        if crate::ipc::daemon_is_reachable() {
            return Ok(());
        }
        if paths.daemon_socket.exists() {
            fs::remove_file(&paths.daemon_socket).with_context(|| {
                format!(
                    "Failed to remove stale daemon socket {}.",
                    paths.daemon_socket.display()
                )
            })?;
        }

        let listener = UnixListener::bind(&paths.daemon_socket)
            .with_context(|| format!("Failed to bind {}.", paths.daemon_socket.display()))?;
        let _socket_guard = SocketGuard(paths.daemon_socket.clone());
        let daemon = SharedDaemon::new();

        initialize_runtime(&daemon);
        daemon.broadcast();
        spawn_watch_loop(daemon.clone());

        for stream in listener.incoming() {
            let daemon = daemon.clone();
            match stream {
                Ok(stream) => {
                    thread::spawn(move || {
                        let _ = handle_client(daemon, stream);
                    });
                }
                Err(error) => eprintln!("codex-rotate: daemon accept failed: {error}"),
            }
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        Err(anyhow!(
            "Local daemon transport is currently only implemented for Unix-like platforms."
        ))
    }
}

#[cfg(unix)]
struct SocketGuard(std::path::PathBuf);

#[cfg(unix)]
impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn initialize_runtime(daemon: &SharedDaemon) {
    let port = managed_codex_port();
    let result = daemon.with_state_mut(|state| {
        let _ = ensure_debug_codex_instance(None, Some(port), None, None);
        refresh_live_account_state(state, true)
    });

    if let Err(error) = result {
        daemon.set_error_message(format!("startup failed: {error}"));
    }
}

fn spawn_watch_loop(daemon: SharedDaemon) {
    if !daemon.snapshot().capabilities.quota_watch {
        return;
    }

    thread::spawn(move || loop {
        match maybe_refresh_local_daemon_process() {
            Ok(true) => std::process::exit(0),
            Ok(false) => {}
            Err(error) => daemon.set_error_message(format!("daemon refresh failed: {error}")),
        }
        let result = daemon.with_state_mut(|state| run_watch_check(state, false));
        if let Err(error) = result {
            daemon.set_error_message(format!("watch failed: {error}"));
        }
        daemon.broadcast();
        thread::sleep(next_watch_interval(daemon.snapshot().current_quota_percent));
    });
}

#[cfg(unix)]
fn handle_client(daemon: SharedDaemon, stream: UnixStream) -> Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let request = read_request(&mut reader)?;

    match request {
        ClientRequest::Subscribe => {
            let (sender, receiver) = mpsc::channel();
            daemon.add_subscriber(sender.clone());
            sender.send(daemon.snapshot()).ok();
            let mut writer = stream;
            for snapshot in receiver {
                write_message(&mut writer, &ServerMessage::Snapshot { snapshot })?;
            }
        }
        ClientRequest::Invoke { action } => {
            let mut writer = stream;
            let response = match daemon.handle_invoke(action) {
                Ok(output) => ServerMessage::Result {
                    ok: true,
                    output: Some(output),
                    error: None,
                },
                Err(error) => ServerMessage::Result {
                    ok: false,
                    output: None,
                    error: Some(error.to_string()),
                },
            };
            write_message(&mut writer, &response)?;
        }
    }

    Ok(())
}

impl SharedDaemon {
    fn set_error_message(&self, message: String) {
        {
            let mut state = self.state.lock().expect("daemon state mutex");
            state.snapshot.last_message = Some(message);
        }
        self.broadcast();
    }

    fn with_state_mut<T, F>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(&mut DaemonState) -> Result<T>,
    {
        let mut state = self.state.lock().expect("daemon state mutex");
        operation(&mut state)
    }

    fn handle_invoke(&self, action: InvokeAction) -> Result<String> {
        let result = self.with_state_mut(|state| run_invoke_action(state, action));
        self.broadcast();
        result
    }
}

fn maybe_refresh_local_daemon_process() -> Result<bool> {
    let Some(build) = current_process_local_cli_build() else {
        return Ok(false);
    };
    let daemon_socket = crate::ipc::daemon_socket_path()?;
    let sources_newer_than_binary = local_cli_sources_newer_than_binary(&build)?;
    if sources_newer_than_binary {
        rebuild_local_cli(&build)?;
    }
    let binary_newer_than_running_socket =
        daemon_socket_is_older_than_binary(&daemon_socket, &build.cli_binary)?;
    if !sources_newer_than_binary && !binary_newer_than_running_socket {
        return Ok(false);
    }

    Command::new(&build.cli_binary)
        .arg("daemon")
        .arg("run")
        .env(DAEMON_TAKEOVER_ENV, "1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| {
            format!(
                "Failed to relaunch {} after rebuilding the local daemon.",
                build.cli_binary.display()
            )
        })?;
    Ok(true)
}

fn takeover_requested() -> bool {
    matches!(std::env::var(DAEMON_TAKEOVER_ENV).as_deref(), Ok("1"))
}

fn wait_for_previous_daemon_to_release_socket() {
    let deadline = Instant::now() + DAEMON_TAKEOVER_TIMEOUT;
    while Instant::now() < deadline {
        if !crate::ipc::daemon_is_reachable() {
            break;
        }
        thread::sleep(DAEMON_TAKEOVER_POLL_INTERVAL);
    }
}

fn run_invoke_action(state: &mut DaemonState, action: InvokeAction) -> Result<String> {
    match action {
        InvokeAction::Status => {
            refresh_static_snapshot(state);
            cmd_status()
        }
        InvokeAction::List => {
            refresh_static_snapshot(state);
            cmd_list()
        }
        InvokeAction::Add { alias } => {
            let output = cmd_add(alias.as_deref())?;
            refresh_static_snapshot(state);
            state.snapshot.last_message = Some(first_line(&output));
            Ok(output)
        }
        InvokeAction::Next => run_manual_next(state),
        InvokeAction::Prev => run_manual_prev(state),
        InvokeAction::Create { options } => run_manual_create(state, options),
        InvokeAction::Relogin { options } => run_manual_relogin(state, options),
        InvokeAction::Remove { selector } => {
            let output = cmd_remove(&selector)?;
            refresh_static_snapshot(state);
            state.snapshot.last_message = Some(first_line(&output));
            Ok(output)
        }
        InvokeAction::Refresh => {
            run_watch_check(state, true)?;
            Ok(state
                .snapshot
                .last_message
                .clone()
                .unwrap_or_else(|| "watch healthy".to_string()))
        }
        InvokeAction::OpenManaged => {
            ensure_debug_codex_instance(None, Some(managed_codex_port()), None, None)?;
            refresh_live_account_state(state, true)?;
            Ok(state
                .snapshot
                .last_message
                .clone()
                .unwrap_or_else(|| "launcher ready".to_string()))
        }
    }
}

fn run_watch_check(state: &mut DaemonState, force_quota_refresh: bool) -> Result<()> {
    let port = managed_codex_port();
    let previous_displayed_email = state.snapshot.current_email.clone();
    let auth_changed = refresh_auth_summary(&mut state.snapshot);

    let result = run_watch_iteration(WatchIterationOptions {
        port: Some(port),
        after_signal_id: None,
        cooldown_ms: None,
        force_quota_refresh: force_quota_refresh || auth_changed,
    })?;

    refresh_inventory_count(&mut state.snapshot);
    refresh_auth_summary(&mut state.snapshot);
    if let Some(live) = result.live.as_ref() {
        state.snapshot.current_email = Some(live.email.clone());
        state.snapshot.current_plan = Some(live.plan_type.clone());
    } else if let Some(email) = result.state.last_live_email.as_ref() {
        state.snapshot.current_email = Some(email.clone());
    }
    if let Some(quota) = result.state.quota.as_ref() {
        set_quota_summary(state, quota);
    }
    if result.rotated {
        if let Some(rotation) = result.rotation.as_ref() {
            state.snapshot.last_rotation_from_email = previous_displayed_email;
            state.snapshot.last_rotation_to_email = Some(rotation.email.clone());
        }
        state.snapshot.last_rotation_reason = result.decision.reason.clone();
        state.snapshot.last_message = Some(format!(
            "rotated: {}",
            result
                .decision
                .reason
                .clone()
                .unwrap_or_else(|| "quota exhausted".to_string())
        ));
    } else if let Some(error) = result.decision.assessment_error.as_deref() {
        state.snapshot.last_message = Some(format!("quota probe failed: {}", error));
    } else {
        state.snapshot.last_message = Some("watch healthy".to_string());
    }
    Ok(())
}

fn run_manual_next(state: &mut DaemonState) -> Result<String> {
    let port = managed_codex_port();
    let previous_displayed_email = state.snapshot.current_email.clone();
    let result = rotate_next_internal()?;
    refresh_static_snapshot(state);
    if let Some(summary) = next_result_summary(&result) {
        state.snapshot.last_rotation_from_email = previous_displayed_email;
        state.snapshot.last_rotation_to_email = Some(summary.email.clone());
    }
    if state.snapshot.capabilities.live_account_sync {
        if let Ok(live) = switch_live_account_to_current_auth(Some(port), false, 15_000) {
            state.snapshot.current_email = Some(live.email);
            state.snapshot.current_plan = Some(live.plan_type);
        }
    }
    refresh_quota_state(state, true);
    state.snapshot.last_rotation_reason = Some("manual rotation".to_string());
    let output = match result {
        NextResult::Rotated { message, .. }
        | NextResult::Stayed { message, .. }
        | NextResult::Created {
            output: message, ..
        } => message,
    };
    state.snapshot.last_message = Some(first_line(&output));
    Ok(output)
}

fn run_manual_prev(state: &mut DaemonState) -> Result<String> {
    let port = managed_codex_port();
    let previous_displayed_email = state.snapshot.current_email.clone();
    let output = cmd_prev()?;
    refresh_static_snapshot(state);
    state.snapshot.last_rotation_from_email = previous_displayed_email;
    state.snapshot.last_rotation_to_email = state.snapshot.current_email.clone();
    state.snapshot.last_rotation_reason = Some("manual rotation".to_string());
    if state.snapshot.capabilities.live_account_sync {
        if let Ok(live) = switch_live_account_to_current_auth(Some(port), false, 15_000) {
            state.snapshot.current_email = Some(live.email);
            state.snapshot.current_plan = Some(live.plan_type);
        }
    }
    refresh_quota_state(state, true);
    state.snapshot.last_message = Some(first_line(&output));
    Ok(output)
}

fn run_manual_create(state: &mut DaemonState, options: CreateInvocation) -> Result<String> {
    let output = cmd_create(CreateCommandOptions {
        alias: options.alias,
        profile_name: options.profile_name,
        base_email: options.base_email,
        force: options.force,
        ignore_current: options.ignore_current,
        restore_previous_auth_after_create: options.restore_previous_auth_after_create,
        require_usable_quota: options.require_usable_quota,
        source: CreateCommandSource::Manual,
    })?;
    refresh_static_snapshot(state);
    refresh_quota_state(state, true);
    state.snapshot.last_rotation_to_email = state.snapshot.current_email.clone();
    state.snapshot.last_message = Some(first_line(&output));
    Ok(output)
}

fn run_manual_relogin(
    state: &mut DaemonState,
    options: crate::ipc::ReloginInvocation,
) -> Result<String> {
    let output = cmd_relogin(
        &options.selector,
        ReloginOptions {
            allow_email_change: options.allow_email_change,
            logout_first: options.logout_first,
            manual_login: options.manual_login,
        },
    )?;
    refresh_static_snapshot(state);
    refresh_quota_state(state, true);
    state.snapshot.last_message = Some(first_line(&output));
    Ok(output)
}

fn next_result_summary(result: &NextResult) -> Option<codex_rotate_core::auth::AuthSummary> {
    match result {
        NextResult::Rotated { summary, .. }
        | NextResult::Stayed { summary, .. }
        | NextResult::Created { summary, .. } => Some(summary.clone()),
    }
}

fn refresh_static_snapshot(state: &mut DaemonState) {
    refresh_inventory_count(&mut state.snapshot);
    refresh_auth_summary(&mut state.snapshot);
}

fn refresh_inventory_count(snapshot: &mut StatusSnapshot) {
    snapshot.inventory_count = load_pool().ok().map(|pool| pool.accounts.len());
}

fn refresh_auth_summary(snapshot: &mut StatusSnapshot) -> bool {
    let previous_email = snapshot.current_email.clone();
    let previous_plan = snapshot.current_plan.clone();
    let paths = match resolve_paths() {
        Ok(paths) => paths,
        Err(_) => return false,
    };
    if let Ok(auth) = load_codex_auth(&paths.codex_auth_file) {
        let summary = summarize_codex_auth(&auth);
        snapshot.current_email = Some(summary.email);
        snapshot.current_plan = Some(summary.plan_type);
    } else {
        snapshot.current_email = None;
        snapshot.current_plan = None;
    }
    snapshot.current_email != previous_email || snapshot.current_plan != previous_plan
}

fn refresh_live_account_state(state: &mut DaemonState, force_quota_refresh: bool) -> Result<()> {
    let port = managed_codex_port();
    refresh_static_snapshot(state);
    let live = read_live_account(Some(port))?;
    if let Some(account) = live.account {
        state.snapshot.current_email = account.email;
        state.snapshot.current_plan = account.plan_type;
    }
    refresh_quota_state(state, force_quota_refresh);
    if state.snapshot.last_message.is_none() {
        state.snapshot.last_message = Some("launcher ready".to_string());
    }
    Ok(())
}

fn refresh_quota_state(state: &mut DaemonState, force_refresh: bool) {
    match refresh_quota_cache(force_refresh, state.quota_cache.as_ref()) {
        Ok(quota) => set_quota_summary(state, &quota),
        Err(error) => {
            state.snapshot.last_message = Some(format!("quota refresh failed: {}", error));
        }
    }
}

fn set_quota_summary(state: &mut DaemonState, quota: &CachedQuotaState) {
    state.snapshot.current_quota = Some(quota.summary.clone());
    state.snapshot.current_quota_percent = quota.primary_quota_left_percent;
    state.quota_cache = Some(quota.clone());
}

fn next_watch_interval(current_quota_percent: Option<u8>) -> Duration {
    let seconds = match current_quota_percent {
        Some(percent) if percent <= 2 => CRITICAL_QUOTA_INTERVAL_SECONDS,
        Some(percent) if percent <= 20 => LOW_QUOTA_INTERVAL_SECONDS,
        _ => DEFAULT_INTERVAL_SECONDS,
    };
    Duration::from_secs(seconds)
}

fn first_line(output: &str) -> String {
    output
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or(output)
        .to_string()
}

fn migrate_runtime_state() -> Result<()> {
    let _ = migrate_legacy_credential_store_if_needed()?;
    migrate_legacy_tray_home_if_needed()?;
    Ok(())
}

fn migrate_legacy_tray_home_if_needed() -> Result<()> {
    let legacy_home = legacy_rotate_app_home()?;
    if !legacy_home.exists() {
        return Ok(());
    }

    let paths = resolve_paths()?;
    fs::create_dir_all(&paths.rotate_home)
        .with_context(|| format!("Failed to create {}.", paths.rotate_home.display()))?;

    move_if_missing(
        &legacy_home.join("watch-state.json"),
        &paths.watch_state_file,
        true,
    )?;
    move_if_missing(
        &legacy_home.join("profile"),
        &paths.debug_profile_dir,
        false,
    )?;

    let legacy_session = legacy_home.join("session.json");
    if legacy_session.exists() {
        fs::remove_file(&legacy_session)
            .with_context(|| format!("Failed to remove {}.", legacy_session.display()))?;
    }

    if fs::read_dir(&legacy_home)?.next().is_none() {
        fs::remove_dir(&legacy_home)
            .with_context(|| format!("Failed to remove {}.", legacy_home.display()))?;
    }

    Ok(())
}

fn move_if_missing(from: &std::path::Path, to: &std::path::Path, is_file: bool) -> Result<()> {
    if !from.exists() || to.exists() {
        return Ok(());
    }
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    fs::rename(from, to).or_else(|_| {
        if is_file {
            fs::copy(from, to).with_context(|| format!("Failed to copy {}.", from.display()))?;
            fs::remove_file(from).with_context(|| format!("Failed to remove {}.", from.display()))
        } else {
            copy_dir_recursive(from, to)?;
            fs::remove_dir_all(from)
                .with_context(|| format!("Failed to remove {}.", from.display()))
        }
    })
}

fn copy_dir_recursive(from: &std::path::Path, to: &std::path::Path) -> Result<()> {
    fs::create_dir_all(to).with_context(|| format!("Failed to create {}.", to.display()))?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let source = entry.path();
        let destination = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&source, &destination)?;
        } else {
            fs::copy(&source, &destination)
                .with_context(|| format!("Failed to copy {}.", source.display()))?;
        }
    }
    Ok(())
}

pub fn capabilities() -> RuntimeCapabilities {
    RuntimeCapabilities::current()
}

pub fn watch_state_path() -> Result<std::path::PathBuf> {
    Ok(resolve_paths()?.watch_state_file)
}

pub fn daemon_socket_path() -> Result<std::path::PathBuf> {
    Ok(resolve_paths()?.daemon_socket)
}

pub fn managed_profile_dir() -> Result<std::path::PathBuf> {
    Ok(resolve_paths()?.debug_profile_dir)
}

pub fn read_watch_state_file() -> Result<WatchState> {
    crate::watch::read_watch_state()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{stamp}"))
    }

    #[test]
    fn migrates_legacy_tray_home_into_rotate_home() {
        let _guard = ENV_MUTEX.lock().expect("env mutex");
        let fake_home = unique_temp_dir("codex-rotate-home");
        let rotate_home = fake_home.join(".codex-rotate");
        let legacy_home = fake_home.join(".codex-rotate-app");
        let previous_home = std::env::var_os("HOME");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");

        fs::create_dir_all(legacy_home.join("profile")).expect("create legacy profile");
        fs::write(legacy_home.join("watch-state.json"), "{}").expect("write watch state");
        fs::write(legacy_home.join("profile").join("marker.txt"), "profile").expect("write marker");
        fs::write(legacy_home.join("session.json"), "{}").expect("write session");

        unsafe {
            std::env::set_var("HOME", &fake_home);
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
        }

        let result = migrate_legacy_tray_home_if_needed();

        match previous_home {
            Some(value) => unsafe {
                std::env::set_var("HOME", value);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }
        match previous_rotate_home {
            Some(value) => unsafe {
                std::env::set_var("CODEX_ROTATE_HOME", value);
            },
            None => unsafe {
                std::env::remove_var("CODEX_ROTATE_HOME");
            },
        }

        result.expect("legacy tray migration");
        assert!(rotate_home.join("watch-state.json").exists());
        assert!(rotate_home.join("profile").join("marker.txt").exists());
        assert!(!legacy_home.exists());
        fs::remove_dir_all(&fake_home).ok();
    }
}
