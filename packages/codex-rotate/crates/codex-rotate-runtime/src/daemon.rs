use std::fs;
use std::io::BufReader;
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::panic::{self, AssertUnwindSafe};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{Duration as ChronoDuration, Local, Utc};
use codex_rotate_core::auth::{load_codex_auth, summarize_codex_auth};
use codex_rotate_core::pool::{
    cmd_add, cmd_list, cmd_prev, cmd_remove, cmd_status, current_pool_overview,
    rotate_next_internal_with_progress, NextResult,
};
use codex_rotate_core::quota::CachedQuotaState;
use codex_rotate_core::workflow::{
    cmd_create_with_progress, cmd_relogin_with_progress,
    migrate_legacy_credential_store_if_needed, CreateCommandOptions, CreateCommandSource,
    ReloginOptions,
};

use crate::dev_refresh::{
    current_process_local_cli_build, daemon_socket_is_older_than_binary,
    ensure_tray_process_registered, local_cli_sources_newer_than_binary, local_refresh_disabled,
    maybe_start_background_release_cli_build, preferred_release_cli_binary, rebuild_local_cli,
    stop_other_local_daemons, INSTANCE_HOME_ENV,
};
use crate::hook::{read_live_account, read_live_account_if_running, switch_live_account_to_current_auth};
use crate::ipc::{
    read_request, write_message, ClientRequest, CreateInvocation, InvokeAction,
    RuntimeCapabilities, ServerMessage, StatusSnapshot,
};
use crate::launcher::ensure_debug_codex_instance;
use crate::paths::{legacy_rotate_app_home, resolve_paths};
use crate::runtime_log::{log_daemon_error, log_daemon_info};
use crate::watch::{refresh_quota_cache, run_watch_iteration, WatchIterationOptions, WatchState};

const DEFAULT_PORT: u16 = 9333;
const DEFAULT_INTERVAL_SECONDS: u64 = 15;
const DAEMON_TAKEOVER_ENV: &str = "CODEX_ROTATE_DAEMON_TAKEOVER";
const DAEMON_TAKEOVER_TIMEOUT: Duration = Duration::from_secs(10);
const DAEMON_TAKEOVER_POLL_INTERVAL: Duration = Duration::from_millis(100);
const TRAY_SUPERVISOR_INTERVAL: Duration = Duration::from_secs(2);

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
    snapshot_cache: Arc<Mutex<StatusSnapshot>>,
    subscribers: Arc<Mutex<Vec<Sender<StatusSnapshot>>>>,
    in_flight_invocations: Arc<AtomicUsize>,
}

impl SharedDaemon {
    fn new() -> Self {
        let state = DaemonState::new();
        let snapshot = state.snapshot.clone();
        Self {
            state: Arc::new(Mutex::new(state)),
            snapshot_cache: Arc::new(Mutex::new(snapshot)),
            subscribers: Arc::new(Mutex::new(Vec::new())),
            in_flight_invocations: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn snapshot(&self) -> StatusSnapshot {
        self.snapshot_cache
            .lock()
            .expect("snapshot cache mutex")
            .clone()
    }

    fn add_subscriber(&self, sender: Sender<StatusSnapshot>) {
        self.subscribers
            .lock()
            .expect("subscriber mutex")
            .push(sender);
    }

    fn publish_snapshot(&self, snapshot: StatusSnapshot) {
        {
            let mut cache = self.snapshot_cache.lock().expect("snapshot cache mutex");
            *cache = snapshot.clone();
        }
        let mut subscribers = self.subscribers.lock().expect("subscriber mutex");
        subscribers.retain(|sender| sender.send(snapshot.clone()).is_ok());
    }

    fn publish_state_snapshot(&self) {
        let snapshot = self
            .state
            .lock()
            .expect("daemon state mutex")
            .snapshot
            .clone();
        self.publish_snapshot(snapshot);
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
    if maybe_refresh_local_daemon_process(None)? {
        return Ok(());
    }
    migrate_runtime_state()?;

    #[cfg(unix)]
    {
        let paths = resolve_paths()?;
        unsafe {
            std::env::set_var(INSTANCE_HOME_ENV, &paths.rotate_home);
        }
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
        log_daemon_info(format!(
            "Daemon listening on {}.",
            paths.daemon_socket.display()
        ));
        if let Some(build) = current_process_local_cli_build() {
            let instance_home = paths.rotate_home.to_string_lossy().into_owned();
            if let Err(error) =
                stop_other_local_daemons(
                    &build,
                    &paths.daemon_socket,
                    std::process::id(),
                    Some(instance_home.as_str()),
                )
            {
                log_daemon_error(format!("Failed to stop stale daemons: {error:#}"));
            }
        }
        let _socket_guard = SocketGuard(paths.daemon_socket.clone());
        let daemon = SharedDaemon::new();

        initialize_runtime(&daemon);
        daemon.publish_state_snapshot();
        spawn_tray_supervisor_loop();
        spawn_watch_loop(daemon.clone());

        for stream in listener.incoming() {
            let daemon = daemon.clone();
            match stream {
                Ok(stream) => {
                    thread::spawn(move || {
                        if let Err(error) = handle_client(daemon, stream) {
                            log_daemon_error(format!("client handler failed: {error:#}"));
                        }
                    });
                }
                Err(error) => {
                    let message = format!("daemon accept failed: {error}");
                    log_daemon_error(&message);
                    eprintln!("codex-rotate: {message}");
                }
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
    let result = daemon.with_state_mut(|state| {
        refresh_live_account_state(state, true, false)
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
        daemon.clear_next_tick();
        match maybe_refresh_local_daemon_process(Some(&daemon)) {
            Ok(true) => std::process::exit(0),
            Ok(false) => {}
            Err(error) => daemon.set_error_message(format!("daemon refresh failed: {error}")),
        }
        let progress_daemon = daemon.clone();
        let progress: Arc<dyn Fn(String) + Send + Sync> =
            Arc::new(move |message| progress_daemon.set_progress_message(message));
        let result =
            daemon.with_state_mut(|state| run_watch_check(state, false, Some(progress.clone())));
        if let Err(error) = result {
            daemon.set_error_message(format!("watch failed: {error}"));
        }
        daemon.publish_state_snapshot();
        let interval = next_watch_interval(daemon.snapshot().current_quota_percent);
        daemon.set_next_tick(next_tick_label(interval));
        thread::sleep(interval);
    });
}

fn spawn_tray_supervisor_loop() {
    thread::spawn(move || loop {
        match ensure_tray_process_registered() {
            Ok(true) => log_daemon_info("Restored Codex Rotate tray launch agent."),
            Ok(false) => {}
            Err(error) => {
                let message = format!("tray supervision failed: {error}");
                log_daemon_error(&message);
            }
        }
        thread::sleep(TRAY_SUPERVISOR_INTERVAL);
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
            let action_name = format!("{action:?}");
            let response = match panic::catch_unwind(AssertUnwindSafe(|| daemon.handle_invoke(action)))
            {
                Ok(Ok(output)) => ServerMessage::Result {
                    ok: true,
                    output: Some(output),
                    error: None,
                },
                Ok(Err(error)) => {
                    log_daemon_error(format!("invoke {action_name} failed: {error:#}"));
                    ServerMessage::Result {
                        ok: false,
                        output: None,
                        error: Some(error.to_string()),
                    }
                }
                Err(payload) => {
                    let detail = if let Some(message) = payload.downcast_ref::<&str>() {
                        (*message).to_string()
                    } else if let Some(message) = payload.downcast_ref::<String>() {
                        message.clone()
                    } else {
                        "unknown panic payload".to_string()
                    };
                    log_daemon_error(format!("invoke {action_name} panicked: {detail}"));
                    ServerMessage::Result {
                        ok: false,
                        output: None,
                        error: Some(format!("Daemon invoke panicked: {detail}")),
                    }
                }
            };
            write_message(&mut writer, &response)?;
        }
    }

    Ok(())
}

impl SharedDaemon {
    fn set_error_message(&self, message: String) {
        log_daemon_error(&message);
        let snapshot = {
            let mut state = self.state.lock().expect("daemon state mutex");
            state.snapshot.last_message = Some(message);
            state.snapshot.next_tick_at = None;
            state.snapshot.clone()
        };
        self.publish_snapshot(snapshot);
    }

    fn set_progress_message(&self, message: String) {
        log_daemon_info(&message);
        let mut snapshot = self.snapshot();
        snapshot.last_message = Some(message);
        snapshot.next_tick_at = None;
        self.publish_snapshot(snapshot);
    }

    fn set_next_tick(&self, next_tick_at: String) {
        let snapshot = {
            let mut state = self.state.lock().expect("daemon state mutex");
            state.snapshot.next_tick_at = Some(next_tick_at);
            state.snapshot.clone()
        };
        self.publish_snapshot(snapshot);
    }

    fn clear_next_tick(&self) {
        let snapshot = {
            let mut state = self.state.lock().expect("daemon state mutex");
            state.snapshot.next_tick_at = None;
            state.snapshot.clone()
        };
        self.publish_snapshot(snapshot);
    }

    fn with_state_mut<T, F>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(&mut DaemonState) -> Result<T>,
    {
        let mut state = self.state.lock().expect("daemon state mutex");
        operation(&mut state)
    }

    fn handle_invoke(&self, action: InvokeAction) -> Result<String> {
        let _guard = InvocationGuard::new(self.in_flight_invocations.clone());
        let result = run_invoke_action(self, action);
        self.publish_state_snapshot();
        result
    }

    fn has_in_flight_invocations(&self) -> bool {
        self.in_flight_invocations.load(Ordering::SeqCst) > 0
    }
}

struct InvocationGuard {
    counter: Arc<AtomicUsize>,
}

impl InvocationGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self { counter }
    }
}

impl Drop for InvocationGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

fn maybe_refresh_local_daemon_process(daemon: Option<&SharedDaemon>) -> Result<bool> {
    if local_refresh_disabled() {
        return Ok(false);
    }
    if takeover_requested() {
        return Ok(false);
    }
    if daemon.is_some_and(SharedDaemon::has_in_flight_invocations) {
        return Ok(false);
    }
    let Some(build) = current_process_local_cli_build() else {
        return Ok(false);
    };
    let instance_home = resolve_paths()?.rotate_home;
    let daemon_socket = crate::ipc::daemon_socket_path()?;
    let sources_newer_than_binary = local_cli_sources_newer_than_binary(&build)?;
    if sources_newer_than_binary {
        log_daemon_info(format!(
            "Local CLI/runtime sources changed. Rebuilding {}.",
            build.cli_binary.display()
        ));
        rebuild_local_cli(&build)?;
    }
    if maybe_start_background_release_cli_build(&build)? {
        log_daemon_info("Queued background release build for codex-rotate.");
    }
    if let Some(release_binary) = preferred_release_cli_binary(&build)? {
        log_daemon_info(format!(
            "Promoting daemon to release binary {}.",
            release_binary.display()
        ));
        Command::new(&release_binary)
            .arg("daemon")
            .arg("run")
            .env(DAEMON_TAKEOVER_ENV, "1")
            .env(INSTANCE_HOME_ENV, &instance_home)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| {
                format!(
                    "Failed to relaunch {} after preparing the release daemon.",
                    release_binary.display()
                )
            })?;
        return Ok(true);
    }
    let binary_newer_than_running_socket =
        daemon_socket_is_older_than_binary(&daemon_socket, &build.cli_binary)?;
    if !sources_newer_than_binary && !binary_newer_than_running_socket {
        return Ok(false);
    }
    log_daemon_info(format!(
        "Refreshing daemon with rebuilt binary {}.",
        build.cli_binary.display()
    ));

    Command::new(&build.cli_binary)
        .arg("daemon")
        .arg("run")
        .env(DAEMON_TAKEOVER_ENV, "1")
        .env(INSTANCE_HOME_ENV, &instance_home)
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

fn run_invoke_action(daemon: &SharedDaemon, action: InvokeAction) -> Result<String> {
    match action {
        InvokeAction::Status => daemon.with_state_mut(|state| {
            refresh_static_snapshot(state);
            cmd_status()
        }),
        InvokeAction::List => daemon.with_state_mut(|state| {
            refresh_static_snapshot(state);
            cmd_list()
        }),
        InvokeAction::Add { alias } => daemon.with_state_mut(|state| {
            let output = cmd_add(alias.as_deref())?;
            refresh_static_snapshot(state);
            state.snapshot.last_message = Some(first_line(&output));
            Ok(output)
        }),
        InvokeAction::Next => {
            let progress_daemon = daemon.clone();
            let progress: Arc<dyn Fn(String) + Send + Sync> =
                Arc::new(move |message| progress_daemon.set_progress_message(message));
            daemon.with_state_mut(|state| run_manual_next(state, Some(progress)))
        }
        InvokeAction::Prev => daemon.with_state_mut(run_manual_prev),
        InvokeAction::Create { options } => {
            let progress_daemon = daemon.clone();
            let progress: Arc<dyn Fn(String) + Send + Sync> =
                Arc::new(move |message| progress_daemon.set_progress_message(message));
            daemon.with_state_mut(|state| run_manual_create(state, options, Some(progress)))
        }
        InvokeAction::Relogin { options } => {
            let progress_daemon = daemon.clone();
            let progress: Arc<dyn Fn(String) + Send + Sync> =
                Arc::new(move |message| progress_daemon.set_progress_message(message));
            daemon.with_state_mut(|state| run_manual_relogin(state, options, Some(progress)))
        }
        InvokeAction::Remove { selector } => daemon.with_state_mut(|state| {
            let output = cmd_remove(&selector)?;
            refresh_static_snapshot(state);
            state.snapshot.last_message = Some(first_line(&output));
            Ok(output)
        }),
        InvokeAction::Refresh => daemon.with_state_mut(|state| {
            let progress_daemon = daemon.clone();
            let progress: Arc<dyn Fn(String) + Send + Sync> =
                Arc::new(move |message| progress_daemon.set_progress_message(message));
            run_watch_check(state, true, Some(progress))?;
            Ok(state
                .snapshot
                .last_message
                .clone()
                .unwrap_or_else(|| "watch healthy".to_string()))
        }),
        InvokeAction::OpenManaged => daemon.with_state_mut(|state| {
            ensure_debug_codex_instance(None, Some(managed_codex_port()), None, None)?;
            refresh_live_account_state(state, true, true)?;
            Ok(state
                .snapshot
                .last_message
                .clone()
                .unwrap_or_else(|| "launcher ready".to_string()))
        }),
    }
}

fn run_watch_check(
    state: &mut DaemonState,
    force_quota_refresh: bool,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<()> {
    let port = managed_codex_port();
    let previous_displayed_email = state.snapshot.current_email.clone();
    let auth_changed = refresh_auth_summary(&mut state.snapshot);
    state.snapshot.next_tick_at = None;

    let result = run_watch_iteration(WatchIterationOptions {
        port: Some(port),
        after_signal_id: None,
        cooldown_ms: None,
        force_quota_refresh: force_quota_refresh || auth_changed,
        progress,
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

fn run_manual_next(
    state: &mut DaemonState,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<String> {
    let port = managed_codex_port();
    let previous_displayed_email = state.snapshot.current_email.clone();
    let result = rotate_next_internal_with_progress(progress)?;
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

fn run_manual_create(
    state: &mut DaemonState,
    options: CreateInvocation,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<String> {
    let output = cmd_create_with_progress(CreateCommandOptions {
        alias: options.alias,
        profile_name: options.profile_name,
        base_email: options.base_email,
        force: options.force,
        ignore_current: options.ignore_current,
        restore_previous_auth_after_create: options.restore_previous_auth_after_create,
        require_usable_quota: options.require_usable_quota,
        source: CreateCommandSource::Manual,
    }, progress)?;
    refresh_static_snapshot(state);
    refresh_quota_state(state, true);
    state.snapshot.last_rotation_to_email = state.snapshot.current_email.clone();
    state.snapshot.last_message = Some(first_line(&output));
    Ok(output)
}

fn run_manual_relogin(
    state: &mut DaemonState,
    options: crate::ipc::ReloginInvocation,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<String> {
    let output = cmd_relogin_with_progress(
        &options.selector,
        ReloginOptions {
            allow_email_change: options.allow_email_change,
            logout_first: options.logout_first,
            manual_login: options.manual_login,
        },
        progress,
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
    if let Ok(overview) = current_pool_overview() {
        snapshot.inventory_count = Some(overview.inventory_count);
        snapshot.inventory_active_slot = overview.inventory_active_slot;
    } else {
        snapshot.inventory_count = None;
        snapshot.inventory_active_slot = None;
    }
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

fn refresh_live_account_state(
    state: &mut DaemonState,
    force_quota_refresh: bool,
    launch_if_needed: bool,
) -> Result<()> {
    let port = managed_codex_port();
    refresh_static_snapshot(state);
    let live = if launch_if_needed {
        Some(read_live_account(Some(port))?)
    } else {
        read_live_account_if_running(Some(port))?
    };
    if let Some(live) = live {
        if let Some(account) = live.account {
            state.snapshot.current_email = account.email;
            state.snapshot.current_plan = account.plan_type;
        }
    }
    refresh_quota_state(state, force_quota_refresh);
    if state.snapshot.last_message.is_none() {
        state.snapshot.last_message = Some(
            if launch_if_needed {
                "launcher ready".to_string()
            } else {
                "watch healthy".to_string()
            },
        );
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
    let _ = current_quota_percent;
    Duration::from_secs(DEFAULT_INTERVAL_SECONDS)
}

fn next_tick_label(interval: Duration) -> String {
    let next_tick = Utc::now()
        + ChronoDuration::from_std(interval).unwrap_or_else(|_| ChronoDuration::seconds(0));
    next_tick
        .with_timezone(&Local)
        .format("%-I:%M:%S %p")
        .to_string()
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

    #[test]
    fn takeover_child_skips_local_daemon_refresh() {
        let _guard = ENV_MUTEX.lock().expect("env mutex");
        let previous_takeover = std::env::var_os(DAEMON_TAKEOVER_ENV);

        unsafe {
            std::env::set_var(DAEMON_TAKEOVER_ENV, "1");
        }

        let result = maybe_refresh_local_daemon_process(None).expect("refresh result");

        match previous_takeover {
            Some(value) => unsafe {
                std::env::set_var(DAEMON_TAKEOVER_ENV, value);
            },
            None => unsafe {
                std::env::remove_var(DAEMON_TAKEOVER_ENV);
            },
        }

        assert!(!result);
    }

    #[test]
    fn skips_local_daemon_refresh_while_invoke_is_active() {
        let daemon = SharedDaemon::new();
        let _guard = InvocationGuard::new(daemon.in_flight_invocations.clone());

        let result =
            maybe_refresh_local_daemon_process(Some(&daemon)).expect("refresh result");

        assert!(!result);
    }

    #[test]
    fn next_watch_interval_never_drops_below_fifteen_seconds() {
        assert_eq!(next_watch_interval(None), Duration::from_secs(15));
        assert_eq!(next_watch_interval(Some(100)), Duration::from_secs(15));
        assert_eq!(next_watch_interval(Some(20)), Duration::from_secs(15));
        assert_eq!(next_watch_interval(Some(2)), Duration::from_secs(15));
        assert_eq!(next_watch_interval(Some(0)), Duration::from_secs(15));
    }
}
