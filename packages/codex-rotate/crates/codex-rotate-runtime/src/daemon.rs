use std::fs;
use std::io::{BufReader, Read};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::panic::{self, AssertUnwindSafe};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::hook::{read_live_account, read_live_account_if_running};
use crate::ipc::{
    read_request, write_message, ClientRequest, CreateInvocation, InvokeAction,
    RuntimeCapabilities, ServerMessage, SnapshotMessageKind, StatusSnapshot,
};
use crate::launcher::ensure_debug_codex_instance;
use crate::paths::{legacy_rotate_app_home, resolve_paths};
use crate::rotation_hygiene::{
    recover_incomplete_rotation_state, relogin as run_shared_relogin,
    rotate_next as run_shared_next, rotate_prev as run_shared_prev,
};
use crate::runtime_log::{log_daemon_error, log_daemon_info};
use crate::watch::{
    auto_create_enabled, read_watch_state, refresh_quota_cache, run_watch_iteration,
    set_auto_create_enabled, tray_enabled, WatchIterationOptions, WatchState,
};
use anyhow::{anyhow, Context, Result};
use chrono::{Duration as ChronoDuration, Local, Utc};
use codex_rotate_core::auth::{load_codex_auth, summarize_codex_auth};
use codex_rotate_core::cancel;
use codex_rotate_core::pool::{
    cmd_add, cmd_list, cmd_remove, cmd_status, current_pool_overview_without_activation,
    restore_codex_auth_from_active_pool, sync_pool_current_auth_into_pool_without_activation,
    NextResult,
};
use codex_rotate_core::quota::CachedQuotaState;
use codex_rotate_core::workflow::{
    cmd_create_with_progress, migrate_legacy_credential_store_if_needed, CreateCommandOptions,
    CreateCommandSource, ReloginOptions,
};
use codex_rotate_refresh::{
    current_process_local_build, daemon_socket_is_older_than_binary,
    ensure_tray_process_registered, local_refresh_disabled, maybe_start_background_release_build,
    preferred_release_binary, rebuild_local_binary, schedule_tray_relaunch_process,
    sources_newer_than_binary, stop_other_local_daemons, supports_live_local_refresh,
    tray_service_pid, TargetKind, INSTANCE_HOME_ARG,
};

const DEFAULT_PORT: u16 = 9333;
const RISKY_INTERVAL_SECONDS: u64 = 15;
const HEALTHY_INTERVAL_SECONDS: u64 = 30;
const LOW_QUOTA_WATCH_THRESHOLD_PERCENT: u8 = 20;
const DAEMON_TAKEOVER_TIMEOUT: Duration = Duration::from_secs(10);
const DAEMON_TAKEOVER_POLL_INTERVAL: Duration = Duration::from_millis(100);
const TRAY_SUPERVISOR_INTERVAL: Duration = Duration::from_secs(2);
const LOCAL_SOURCE_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const CLIENT_DISCONNECT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DAEMON_ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(100);
pub const DAEMON_TAKEOVER_ARG: &str = "--takeover";
const DISABLED_TARGET_ERROR_SNIPPET: &str = "is in a disabled domain and cannot be activated";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DaemonRunOptions {
    pub instance_home: Option<String>,
    pub takeover: bool,
}

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
    active_operations: Arc<AtomicUsize>,
    shutdown_requested: Arc<AtomicBool>,
    shutdown_when_idle_requested: Arc<AtomicBool>,
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
            active_operations: Arc::new(AtomicUsize::new(0)),
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            shutdown_when_idle_requested: Arc::new(AtomicBool::new(false)),
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

fn apply_instance_home_override(instance_home: Option<&str>) {
    let Some(instance_home) = instance_home
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", instance_home);
    }
}

pub fn run_daemon_forever(options: DaemonRunOptions) -> Result<()> {
    apply_instance_home_override(options.instance_home.as_deref());
    if maybe_refresh_local_daemon_process(None, options.takeover)? {
        return Ok(());
    }
    migrate_runtime_state()?;
    recover_incomplete_rotation_state()?;

    #[cfg(unix)]
    {
        let paths = resolve_paths()?;
        fs::create_dir_all(&paths.rotate_home)
            .with_context(|| format!("Failed to create {}.", paths.rotate_home.display()))?;

        if options.takeover {
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
        listener
            .set_nonblocking(true)
            .context("Failed to configure daemon socket as nonblocking.")?;
        log_daemon_info(format!(
            "Daemon listening on {}.",
            paths.daemon_socket.display()
        ));
        if let Some(build) = current_process_local_build(TargetKind::Cli) {
            let instance_home = options
                .instance_home
                .clone()
                .unwrap_or_else(|| paths.rotate_home.to_string_lossy().into_owned());
            if let Err(error) = stop_other_local_daemons(
                &build,
                &paths.daemon_socket,
                std::process::id(),
                Some(instance_home.as_str()),
            ) {
                log_daemon_error(format!("Failed to stop stale daemons: {error:#}"));
            }
        }
        let _socket_guard = SocketGuard(paths.daemon_socket.clone());
        let daemon = SharedDaemon::new();

        initialize_runtime(&daemon);
        daemon.publish_state_snapshot();
        spawn_local_source_refresh_loop(daemon.clone());
        spawn_tray_supervisor_loop();
        spawn_watch_loop(daemon.clone());

        loop {
            if poll_shutdown_request(&daemon) {
                break;
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    let daemon = daemon.clone();
                    thread::spawn(move || {
                        if let Err(error) = handle_client(daemon, stream) {
                            log_daemon_error(format!("client handler failed: {error:#}"));
                        }
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(DAEMON_ACCEPT_POLL_INTERVAL);
                }
                Err(error) => {
                    let message = format!("daemon accept failed: {error}");
                    log_daemon_error(&message);
                    eprintln!("codex-rotate: {message}");
                    thread::sleep(DAEMON_ACCEPT_POLL_INTERVAL);
                }
            }
        }
        log_daemon_info("Daemon shutdown requested; exiting.");
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

#[cfg(unix)]
struct ClientDisconnectMonitor {
    canceled: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

#[cfg(unix)]
impl ClientDisconnectMonitor {
    fn attach(stream: &UnixStream) -> Result<Self> {
        let canceled = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let mut reader = stream.try_clone()?;
        reader
            .set_read_timeout(Some(CLIENT_DISCONNECT_POLL_INTERVAL))
            .context("Failed to configure daemon disconnect monitor timeout.")?;
        let canceled_signal = canceled.clone();
        let done_signal = done.clone();
        let handle = thread::spawn(move || {
            let mut buffer = [0_u8; 1];
            while !done_signal.load(Ordering::Relaxed) {
                match reader.read(&mut buffer) {
                    Ok(0) => {
                        canceled_signal.store(true, Ordering::SeqCst);
                        break;
                    }
                    Ok(_) => {}
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::WouldBlock
                                | std::io::ErrorKind::TimedOut
                                | std::io::ErrorKind::Interrupted
                        ) => {}
                    Err(_) => {
                        canceled_signal.store(true, Ordering::SeqCst);
                        break;
                    }
                }
            }
        });
        Ok(Self {
            canceled,
            done,
            handle: Some(handle),
        })
    }

    fn cancel_token(&self) -> Arc<AtomicBool> {
        self.canceled.clone()
    }

    fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::SeqCst)
    }

    fn finish(mut self) {
        self.done.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn initialize_runtime(daemon: &SharedDaemon) {
    let result = daemon.with_state_mut(|state| {
        hydrate_quota_cache_from_watch_state(state);
        refresh_live_account_state(state, false, false)
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
        match maybe_refresh_local_daemon_process(Some(&daemon), false) {
            Ok(true) => std::process::exit(0),
            Ok(false) => {}
            Err(error) => daemon.set_error_message(format!("daemon refresh failed: {error}")),
        }
        let progress_daemon = daemon.clone();
        let progress: Arc<dyn Fn(String) + Send + Sync> =
            Arc::new(move |message| progress_daemon.set_progress_message(message));
        let _activity = ActivityGuard::new(daemon.active_operations.clone());
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

fn spawn_local_source_refresh_loop(daemon: SharedDaemon) {
    thread::spawn(move || loop {
        if poll_shutdown_request(&daemon) {
            break;
        }
        match maybe_refresh_local_daemon_process(Some(&daemon), false) {
            Ok(true) => std::process::exit(0),
            Ok(false) => {}
            Err(error) => daemon.set_error_message(format!("daemon refresh failed: {error}")),
        }
        match maybe_refresh_local_tray_process() {
            Ok(true) => log_daemon_info("Refreshed Codex Rotate tray after local changes."),
            Ok(false) => {}
            Err(error) => {
                let message = format!("tray refresh failed: {error}");
                log_daemon_error(&message);
            }
        }
        thread::sleep(LOCAL_SOURCE_REFRESH_INTERVAL);
    });
}

fn spawn_tray_supervisor_loop() {
    thread::spawn(move || loop {
        match tray_enabled() {
            Ok(true) => {
                let refreshed = match maybe_refresh_local_tray_process() {
                    Ok(true) => {
                        log_daemon_info("Refreshed Codex Rotate tray after local changes.");
                        true
                    }
                    Ok(false) => false,
                    Err(error) => {
                        let message = format!("tray refresh failed: {error}");
                        log_daemon_error(&message);
                        false
                    }
                };
                if refreshed {
                    thread::sleep(TRAY_SUPERVISOR_INTERVAL);
                    continue;
                }
                match ensure_tray_process_registered() {
                    Ok(true) => log_daemon_info("Restored Codex Rotate tray launch agent."),
                    Ok(false) => {}
                    Err(error) => {
                        let message = format!("tray supervision failed: {error}");
                        log_daemon_error(&message);
                    }
                }
            }
            Ok(false) => {}
            Err(error) => {
                let message = format!("tray supervision state failed: {error}");
                log_daemon_error(&message);
            }
        }
        thread::sleep(TRAY_SUPERVISOR_INTERVAL);
    });
}

fn maybe_refresh_local_tray_process() -> Result<bool> {
    if local_refresh_disabled() {
        return Ok(false);
    }
    let Some(tray_binary) = resolve_tray_binary_for_supervisor() else {
        return Ok(false);
    };
    let Some(build) = codex_rotate_refresh::detect_local_build(&tray_binary, TargetKind::Tray)
    else {
        return Ok(false);
    };
    if !supports_live_local_refresh(&build) {
        return Ok(false);
    }

    #[cfg(target_os = "macos")]
    if tray_service_pid()?.is_some() {
        return Ok(false);
    }

    let sources_newer_than_binary = sources_newer_than_binary(&build)?;
    if sources_newer_than_binary {
        log_daemon_info(format!(
            "Local tray sources changed while tray was offline. Rebuilding {}.",
            build.binary_path.display()
        ));
        rebuild_local_binary(&build)?;
    }
    if maybe_start_background_release_build(&build)? {
        log_daemon_info("Queued background release build for codex-rotate-tray.");
    }
    if let Some(release_binary) = preferred_release_binary(&build)? {
        log_daemon_info(format!(
            "Promoting tray to release binary {} from daemon supervisor.",
            release_binary.display()
        ));
        schedule_tray_relaunch_process(&release_binary)?;
        return Ok(true);
    }
    if !sources_newer_than_binary {
        return Ok(false);
    }

    log_daemon_info(format!(
        "Relaunching tray with rebuilt binary {} from daemon supervisor.",
        build.binary_path.display()
    ));
    schedule_tray_relaunch_process(&build.binary_path)?;
    Ok(true)
}

fn resolve_tray_binary_for_supervisor() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("CODEX_ROTATE_TRAY_BIN").map(PathBuf::from) {
        if path.is_file() {
            return Some(path);
        }
    }

    if let Some(cli_build) = current_process_local_build(TargetKind::Cli) {
        let binary_name = tray_binary_name();
        for candidate in [
            cli_build
                .repo_root
                .join("target")
                .join("release")
                .join(binary_name),
            cli_build
                .repo_root
                .join("target")
                .join("debug")
                .join(binary_name),
        ] {
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }

    let current_exe = std::env::current_exe().ok()?;
    let current_dir = current_exe.parent()?;
    let sibling = current_dir.join(tray_binary_name());
    sibling.is_file().then_some(sibling)
}

fn tray_binary_name() -> &'static str {
    #[cfg(windows)]
    {
        "codex-rotate-tray.exe"
    }

    #[cfg(not(windows))]
    {
        "codex-rotate-tray"
    }
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
                write_message(
                    &mut writer,
                    &ServerMessage::Snapshot {
                        snapshot: Box::new(snapshot),
                    },
                )?;
            }
        }
        ClientRequest::Invoke { action, repo_root } => {
            let mut writer = stream;
            let action_name = format!("{action:?}");
            let disconnect_monitor = ClientDisconnectMonitor::attach(&writer)?;
            let cancel_token = disconnect_monitor.cancel_token();
            let response = match panic::catch_unwind(AssertUnwindSafe(|| {
                cancel::with_cancel_token(cancel_token, || {
                    if let Some(request_repo_root) = repo_root.as_deref() {
                        let daemon_repo_root = codex_rotate_core::paths::resolve_paths()?
                            .repo_root
                            .to_string_lossy()
                            .into_owned();
                        if request_repo_root != daemon_repo_root {
                            return Err(anyhow!(
                                "Daemon repo root mismatch: daemon={}, request={}",
                                daemon_repo_root,
                                request_repo_root
                            ));
                        }
                    }
                    daemon.handle_invoke(action)
                })
            })) {
                Ok(Ok(output)) => ServerMessage::Result {
                    ok: true,
                    output: Some(output),
                    error: None,
                },
                Ok(Err(error)) => {
                    if disconnect_monitor.is_canceled() {
                        log_daemon_info(format!(
                            "invoke {action_name} canceled after the client disconnected."
                        ));
                    } else {
                        log_daemon_error(format!("invoke {action_name} failed: {error:#}"));
                    }
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
            let client_disconnected = disconnect_monitor.is_canceled();
            disconnect_monitor.finish();
            if !client_disconnected {
                write_message(&mut writer, &response)?;
            }
        }
    }

    Ok(())
}

impl SharedDaemon {
    fn set_error_message(&self, message: String) {
        log_daemon_error(&message);
        let snapshot = {
            let mut state = self.state.lock().expect("daemon state mutex");
            set_snapshot_message(&mut state.snapshot, SnapshotMessageKind::Error, message);
            state.snapshot.next_tick_at = None;
            state.snapshot.clone()
        };
        self.publish_snapshot(snapshot);
    }

    fn set_progress_message(&self, message: String) {
        log_daemon_info(&message);
        let mut snapshot = self.snapshot();
        set_snapshot_message(&mut snapshot, SnapshotMessageKind::Progress, message);
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
        if matches!(action, InvokeAction::Shutdown) {
            return Ok(request_daemon_shutdown(self));
        }
        if matches!(action, InvokeAction::ShutdownWhenIdle) {
            return Ok(request_daemon_shutdown_when_idle(self));
        }
        let _guard = InvocationGuard::new(self.in_flight_invocations.clone());
        let _activity = ActivityGuard::new(self.active_operations.clone());
        let result = run_invoke_action(self, action);
        self.publish_state_snapshot();
        result
    }

    fn has_in_flight_invocations(&self) -> bool {
        self.in_flight_invocations.load(Ordering::SeqCst) > 0
    }

    fn has_active_operations(&self) -> bool {
        self.active_operations.load(Ordering::SeqCst) > 0
    }

    fn request_shutdown(&self) {
        self.shutdown_when_idle_requested
            .store(false, Ordering::SeqCst);
        self.shutdown_requested.store(true, Ordering::SeqCst);
    }

    fn shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
    }

    fn request_shutdown_when_idle(&self) {
        self.shutdown_when_idle_requested
            .store(true, Ordering::SeqCst);
    }

    fn shutdown_when_idle_requested(&self) -> bool {
        self.shutdown_when_idle_requested.load(Ordering::SeqCst)
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

struct ActivityGuard {
    counter: Arc<AtomicUsize>,
}

impl ActivityGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self { counter }
    }
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

fn maybe_refresh_local_daemon_process(
    daemon: Option<&SharedDaemon>,
    takeover_requested: bool,
) -> Result<bool> {
    if local_refresh_disabled() {
        return Ok(false);
    }
    if takeover_requested {
        return Ok(false);
    }
    if daemon.is_some_and(SharedDaemon::has_in_flight_invocations) {
        return Ok(false);
    }
    if daemon.is_some_and(SharedDaemon::has_active_operations) {
        return Ok(false);
    }
    let Some(build) = current_process_local_build(TargetKind::Cli) else {
        return Ok(false);
    };
    if !supports_live_local_refresh(&build) {
        return Ok(false);
    }
    let instance_home = resolve_paths()?.rotate_home;
    let daemon_socket = crate::ipc::daemon_socket_path()?;
    let sources_newer_than_binary = sources_newer_than_binary(&build)?;
    if sources_newer_than_binary {
        log_daemon_info(format!(
            "Local CLI/runtime sources changed. Rebuilding {}.",
            build.binary_path.display()
        ));
        rebuild_local_binary(&build)?;
    }
    if maybe_start_background_release_build(&build)? {
        log_daemon_info("Queued background release build for codex-rotate.");
    }
    if let Some(release_binary) = preferred_release_binary(&build)? {
        log_daemon_info(format!(
            "Promoting daemon to release binary {}.",
            release_binary.display()
        ));
        Command::new(&release_binary)
            .arg("daemon")
            .arg(DAEMON_TAKEOVER_ARG)
            .arg(INSTANCE_HOME_ARG)
            .arg(&instance_home)
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
        daemon_socket_is_older_than_binary(&daemon_socket, &build.binary_path)?;
    if !sources_newer_than_binary && !binary_newer_than_running_socket {
        return Ok(false);
    }
    log_daemon_info(format!(
        "Refreshing daemon with rebuilt binary {}.",
        build.binary_path.display()
    ));

    Command::new(&build.binary_path)
        .arg("daemon")
        .arg(DAEMON_TAKEOVER_ARG)
        .arg(INSTANCE_HOME_ARG)
        .arg(&instance_home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| {
            format!(
                "Failed to relaunch {} after rebuilding the local daemon.",
                build.binary_path.display()
            )
        })?;
    Ok(true)
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
            set_snapshot_message(
                &mut state.snapshot,
                SnapshotMessageKind::Status,
                first_line(&output),
            );
            Ok(output)
        }),
        InvokeAction::SetAutoCreateEnabled { enabled } => daemon.with_state_mut(|state| {
            set_auto_create_enabled(enabled)?;
            state.snapshot.auto_create_enabled = enabled;
            let message = if enabled {
                "Auto create enabled."
            } else {
                "Auto create disabled."
            };
            set_snapshot_message(
                &mut state.snapshot,
                SnapshotMessageKind::Status,
                message.to_string(),
            );
            Ok(message.to_string())
        }),
        InvokeAction::Shutdown => Ok(request_daemon_shutdown(daemon)),
        InvokeAction::ShutdownWhenIdle => Ok(request_daemon_shutdown_when_idle(daemon)),
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
            set_snapshot_message(
                &mut state.snapshot,
                SnapshotMessageKind::Status,
                first_line(&output),
            );
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
            refresh_live_account_state(state, false, true)?;
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
    let _ = sync_pool_current_auth_into_pool_without_activation();
    state.snapshot.next_tick_at = None;

    let result = run_watch_iteration(WatchIterationOptions {
        port: Some(port),
        after_signal_id: None,
        cooldown_ms: None,
        force_quota_refresh: force_quota_refresh || auth_changed,
        progress,
    })?;

    refresh_static_snapshot(state);
    if let Some(live) = result.live.as_ref() {
        state.snapshot.current_email = Some(live.email.clone());
        state.snapshot.current_plan = Some(live.plan_type.clone());
    } else if let Some(email) = result.current_account_state.last_live_email.as_ref() {
        state.snapshot.current_email = Some(email.clone());
    }
    if let Some(quota) = result.current_account_state.quota.as_ref() {
        set_quota_summary(state, quota);
    }
    if result.rotated {
        if let Some(rotation) = result.rotation.as_ref() {
            state.snapshot.last_rotation_from_email = previous_displayed_email;
            state.snapshot.last_rotation_to_email = Some(rotation.email.clone());
        }
        state.snapshot.last_rotation_reason = result.decision.reason.clone();
        set_snapshot_message(
            &mut state.snapshot,
            SnapshotMessageKind::Status,
            format!(
                "rotated: {}",
                result
                    .decision
                    .reason
                    .clone()
                    .unwrap_or_else(|| "quota exhausted".to_string())
            ),
        );
    } else if let Some(error) = result.decision.assessment_error.as_deref() {
        set_snapshot_message(
            &mut state.snapshot,
            SnapshotMessageKind::Error,
            format!("quota probe failed: {}", error),
        );
    } else {
        set_snapshot_message(
            &mut state.snapshot,
            SnapshotMessageKind::Status,
            "watch healthy",
        );
    }
    Ok(())
}

fn run_manual_next(
    state: &mut DaemonState,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<String> {
    let previous_displayed_email = state.snapshot.current_email.clone();
    let result = match run_shared_next(Some(managed_codex_port()), progress.clone()) {
        Ok(result) => result,
        Err(error) => {
            refresh_static_snapshot(state);
            refresh_quota_state(state, false);
            state.snapshot.last_rotation_reason = Some("manual rotation failed".to_string());
            set_snapshot_message(
                &mut state.snapshot,
                SnapshotMessageKind::Error,
                manual_rotation_error_message(&error),
            );
            return Err(error);
        }
    };
    refresh_static_snapshot(state);
    if let Some(summary) = next_result_summary(&result) {
        state.snapshot.last_rotation_from_email = previous_displayed_email;
        state.snapshot.last_rotation_to_email = Some(summary.email.clone());
    }
    if let Some(summary) = next_result_summary(&result) {
        state.snapshot.current_email = Some(summary.email.clone());
        state.snapshot.current_plan = Some(summary.plan_type.clone());
    }
    refresh_quota_state(state, false);
    state.snapshot.last_rotation_reason = Some("manual rotation".to_string());
    let output = match result {
        NextResult::Rotated { message, .. }
        | NextResult::Stayed { message, .. }
        | NextResult::Created {
            output: message, ..
        } => message,
    };
    set_snapshot_message(
        &mut state.snapshot,
        SnapshotMessageKind::Status,
        first_line(&output),
    );
    Ok(output)
}

fn manual_rotation_error_message(error: &anyhow::Error) -> String {
    let detail = format!("{error:#}");
    if detail.contains(DISABLED_TARGET_ERROR_SNIPPET) {
        return "rotation blocked: a target account domain is disabled; re-enable it in ~/.codex-rotate/accounts.json or use rotate prev".to_string();
    }
    format!("rotation failed: {}", first_line(&detail))
}

fn run_manual_prev(state: &mut DaemonState) -> Result<String> {
    let previous_displayed_email = state.snapshot.current_email.clone();
    let result = run_shared_prev(Some(managed_codex_port()), None)?;
    refresh_static_snapshot(state);
    state.snapshot.last_rotation_from_email = previous_displayed_email;
    let summary = summarize_codex_auth(&load_codex_auth(&resolve_paths()?.codex_auth_file)?);
    state.snapshot.last_rotation_to_email = Some(summary.email.clone());
    state.snapshot.last_rotation_reason = Some("manual rotation".to_string());
    state.snapshot.current_email = Some(summary.email.clone());
    state.snapshot.current_plan = Some(summary.plan_type.clone());
    refresh_quota_state(state, false);
    set_snapshot_message(
        &mut state.snapshot,
        SnapshotMessageKind::Status,
        first_line(&result),
    );
    Ok(result)
}

fn run_manual_create(
    state: &mut DaemonState,
    options: CreateInvocation,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<String> {
    let output = cmd_create_with_progress(
        CreateCommandOptions {
            alias: options.alias,
            profile_name: options.profile_name,
            template: options.template,
            force: options.force,
            ignore_current: options.ignore_current,
            restore_previous_auth_after_create: options.restore_previous_auth_after_create,
            require_usable_quota: options.require_usable_quota,
            source: CreateCommandSource::Manual,
        },
        progress,
    )?;
    refresh_static_snapshot(state);
    refresh_quota_state(state, false);
    state.snapshot.last_rotation_to_email = state.snapshot.current_email.clone();
    set_snapshot_message(
        &mut state.snapshot,
        SnapshotMessageKind::Status,
        first_line(&output),
    );
    Ok(output)
}

fn run_manual_relogin(
    state: &mut DaemonState,
    options: crate::ipc::ReloginInvocation,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<String> {
    let output = run_shared_relogin(
        Some(managed_codex_port()),
        &options.selector,
        ReloginOptions {
            allow_email_change: options.allow_email_change,
            logout_first: options.logout_first,
            manual_login: options.manual_login,
        },
        progress,
    )?;
    refresh_static_snapshot(state);
    refresh_quota_state(state, false);
    set_snapshot_message(
        &mut state.snapshot,
        SnapshotMessageKind::Status,
        first_line(&output),
    );
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
    let _ = sync_pool_current_auth_into_pool_without_activation();
    refresh_inventory_count(&mut state.snapshot);
    refresh_auth_summary(&mut state.snapshot);
    state.snapshot.auto_create_enabled = auto_create_enabled().unwrap_or(true);
}

fn hydrate_quota_cache_from_watch_state(state: &mut DaemonState) {
    if state.quota_cache.is_some() {
        return;
    }
    let Ok(watch_state) = read_watch_state() else {
        return;
    };
    let Ok(paths) = resolve_paths() else {
        return;
    };
    let Ok(auth) = load_codex_auth(&paths.codex_auth_file) else {
        return;
    };
    let account_state = watch_state.account_state(&summarize_codex_auth(&auth).account_id);
    let Some(quota) = account_state.quota.as_ref() else {
        return;
    };
    set_quota_summary(state, quota);
}

fn refresh_inventory_count(snapshot: &mut StatusSnapshot) {
    if let Ok(overview) = current_pool_overview_without_activation() {
        snapshot.inventory_count = Some(overview.inventory_count);
        snapshot.inventory_active_slot = overview.inventory_active_slot;
        snapshot.inventory_healthy_count = Some(overview.inventory_healthy_count);
    } else {
        snapshot.inventory_count = None;
        snapshot.inventory_active_slot = None;
        snapshot.inventory_healthy_count = None;
    }
}

fn refresh_auth_summary(snapshot: &mut StatusSnapshot) -> bool {
    let previous_email = snapshot.current_email.clone();
    let previous_plan = snapshot.current_plan.clone();
    let paths = match resolve_paths() {
        Ok(paths) => paths,
        Err(_) => return false,
    };
    if !paths.codex_auth_file.exists() {
        let _ = restore_codex_auth_from_active_pool();
    }
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
        set_snapshot_message(
            &mut state.snapshot,
            SnapshotMessageKind::Status,
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
            set_snapshot_message(
                &mut state.snapshot,
                SnapshotMessageKind::Error,
                format!("quota refresh failed: {}", error),
            );
        }
    }
}

fn set_quota_summary(state: &mut DaemonState, quota: &CachedQuotaState) {
    state.snapshot.current_quota = Some(quota.summary.clone());
    state.snapshot.current_quota_percent = quota.primary_quota_left_percent;
    state.quota_cache = Some(quota.clone());
}

fn set_snapshot_message(
    snapshot: &mut StatusSnapshot,
    kind: SnapshotMessageKind,
    message: impl Into<String>,
) {
    snapshot.last_message = Some(message.into());
    snapshot.last_message_kind = Some(kind);
}

fn poll_shutdown_request(daemon: &SharedDaemon) -> bool {
    if daemon.shutdown_requested() {
        return true;
    }
    if daemon.shutdown_when_idle_requested() && !daemon.has_active_operations() {
        let _ = request_daemon_shutdown(daemon);
        return true;
    }
    false
}

fn request_daemon_shutdown(daemon: &SharedDaemon) -> String {
    let message = "Codex Rotate is shutting down.".to_string();
    if let Ok(mut state) = daemon.state.try_lock() {
        state.snapshot.next_tick_at = None;
        set_snapshot_message(
            &mut state.snapshot,
            SnapshotMessageKind::Status,
            message.clone(),
        );
        daemon.publish_snapshot(state.snapshot.clone());
    } else {
        let mut snapshot = daemon.snapshot();
        snapshot.next_tick_at = None;
        set_snapshot_message(&mut snapshot, SnapshotMessageKind::Status, message);
        daemon.publish_snapshot(snapshot);
    }
    daemon.request_shutdown();
    "Stopping Codex Rotate daemon.".to_string()
}

fn request_daemon_shutdown_when_idle(daemon: &SharedDaemon) -> String {
    if !daemon.has_active_operations() {
        return request_daemon_shutdown(daemon);
    }

    let message = "Codex Rotate will shut down after the current task finishes.".to_string();
    if let Ok(mut state) = daemon.state.try_lock() {
        state.snapshot.next_tick_at = None;
        set_snapshot_message(
            &mut state.snapshot,
            SnapshotMessageKind::Status,
            message.clone(),
        );
        daemon.publish_snapshot(state.snapshot.clone());
    } else {
        let mut snapshot = daemon.snapshot();
        snapshot.next_tick_at = None;
        set_snapshot_message(&mut snapshot, SnapshotMessageKind::Status, message);
        daemon.publish_snapshot(snapshot);
    }
    daemon.request_shutdown_when_idle();
    "Will stop Codex Rotate after the current task finishes.".to_string()
}

fn next_watch_interval(current_quota_percent: Option<u8>) -> Duration {
    let seconds = match current_quota_percent {
        Some(percent) if percent > LOW_QUOTA_WATCH_THRESHOLD_PERCENT => HEALTHY_INTERVAL_SECONDS,
        _ => RISKY_INTERVAL_SECONDS,
    };
    Duration::from_secs(seconds)
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
    if let Err(rename_error) = fs::rename(from, to) {
        if to.exists() && !from.exists() {
            return Ok(());
        }
        if !from.exists() {
            return Err(anyhow::anyhow!(
                "Failed to move {} to {}: source disappeared after rename fallback ({rename_error}).",
                from.display(),
                to.display()
            ));
        }
        if is_file {
            fs::copy(from, to).with_context(|| {
                format!(
                    "Failed to copy {} after rename fallback ({rename_error}).",
                    from.display()
                )
            })?;
            fs::remove_file(from).with_context(|| format!("Failed to remove {}.", from.display()))
        } else {
            copy_dir_recursive(from, to)?;
            fs::remove_dir_all(from)
                .with_context(|| format!("Failed to remove {}.", from.display()))
        }
    } else {
        Ok(())
    }
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
    use crate::test_support::env_mutex;
    use codex_rotate_core::auth::{write_codex_auth, AuthTokens, CodexAuth};
    use codex_rotate_core::pool::load_pool;
    use std::fs;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    #[cfg(unix)]
    use std::os::unix::net::UnixStream;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{stamp}"))
    }

    fn make_test_auth(account_id: &str) -> CodexAuth {
        CodexAuth {
            auth_mode: "chatgpt".to_string(),
            openai_api_key: None,
            tokens: AuthTokens {
                id_token: "id-token".to_string(),
                access_token: "access-token".to_string(),
                refresh_token: Some("refresh-token".to_string()),
                account_id: account_id.to_string(),
            },
            last_refresh: "2026-04-08T00:00:00.000Z".to_string(),
        }
    }

    fn make_test_auth_with_email(email: &str, account_id: &str, plan_type: &str) -> CodexAuth {
        let (access_token, id_token) = match (email, account_id, plan_type) {
            ("dev.1@astronlab.com", "acct-1", "free") => (
                "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL3Byb2ZpbGUiOnsiZW1haWwiOiJkZXYuMUBhc3Ryb25sYWIuY29tIn0sImh0dHBzOi8vYXBpLm9wZW5haS5jb20vYXV0aCI6eyJjaGF0Z3B0X2FjY291bnRfaWQiOiJhY2N0LTEiLCJjaGF0Z3B0X3BsYW5fdHlwZSI6ImZyZWUifX0.signature",
                "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJlbWFpbCI6ImRldi4xQGFzdHJvbmxhYi5jb20ifQ.signature",
            ),
            ("dev.2@astronlab.com", "acct-2", "free") => (
                "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL3Byb2ZpbGUiOnsiZW1haWwiOiJkZXYuMkBhc3Ryb25sYWIuY29tIn0sImh0dHBzOi8vYXBpLm9wZW5haS5jb20vYXV0aCI6eyJjaGF0Z3B0X2FjY291bnRfaWQiOiJhY2N0LTIiLCJjaGF0Z3B0X3BsYW5fdHlwZSI6ImZyZWUifX0.signature",
                "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJlbWFpbCI6ImRldi4yQGFzdHJvbmxhYi5jb20ifQ.signature",
            ),
            _ => panic!("unexpected auth fixture {email}/{account_id}/{plan_type}"),
        };
        CodexAuth {
            auth_mode: "chatgpt".to_string(),
            openai_api_key: None,
            tokens: AuthTokens {
                access_token: access_token.to_string(),
                id_token: id_token.to_string(),
                refresh_token: Some("refresh-token".to_string()),
                account_id: account_id.to_string(),
            },
            last_refresh: "2026-04-08T00:00:00.000Z".to_string(),
        }
    }

    fn spawn_quota_server(response_body: String) -> (String, Arc<AtomicUsize>, Arc<AtomicBool>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("quota listener");
        listener
            .set_nonblocking(true)
            .expect("nonblocking quota listener");
        let address = listener.local_addr().expect("quota listener addr");
        let request_count = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let request_count_thread = request_count.clone();
        let stop_thread = stop.clone();
        std::thread::spawn(move || {
            while !stop_thread.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        request_count_thread.fetch_add(1, Ordering::SeqCst);
                        let mut buffer = [0_u8; 1024];
                        let _ = stream.read(&mut buffer);
                        let body = response_body.as_bytes();
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            response_body
                        );
                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.flush();
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        (
            format!("http://127.0.0.1:{}/wham/usage", address.port()),
            request_count,
            stop,
        )
    }

    #[cfg(unix)]
    #[test]
    fn client_disconnect_monitor_only_marks_its_own_stream_closed() {
        let (server_stream, client_stream) = UnixStream::pair().expect("unix stream pair");
        let monitor = ClientDisconnectMonitor::attach(&server_stream).expect("attach monitor");

        assert!(!monitor.is_canceled());
        drop(client_stream);

        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && !monitor.is_canceled() {
            thread::sleep(Duration::from_millis(20));
        }

        assert!(
            monitor.is_canceled(),
            "monitor should observe peer shutdown"
        );
        monitor.finish();
    }

    #[test]
    fn migrates_legacy_tray_home_into_rotate_home() {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
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
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let result = maybe_refresh_local_daemon_process(None, true).expect("refresh result");

        assert!(!result);
    }

    #[test]
    fn tray_supervisor_uses_explicit_tray_binary_override() {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let previous_tray_bin = std::env::var_os("CODEX_ROTATE_TRAY_BIN");
        let fake_tray = unique_temp_dir("codex-rotate-tray-binary");
        fs::write(&fake_tray, "").expect("write fake tray");

        unsafe {
            std::env::set_var("CODEX_ROTATE_TRAY_BIN", &fake_tray);
        }

        let resolved = resolve_tray_binary_for_supervisor();

        match previous_tray_bin {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_TRAY_BIN", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_TRAY_BIN") },
        }
        fs::remove_file(&fake_tray).ok();

        assert_eq!(resolved.as_deref(), Some(fake_tray.as_path()));
    }

    #[test]
    fn instance_home_override_updates_resolved_rotate_home() {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let override_home = tempdir.path().join("rotate-instance");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path().join("original-home"));
        }

        apply_instance_home_override(Some(&override_home.to_string_lossy()));
        let resolved_rotate_home = resolve_paths().expect("resolve paths").rotate_home;

        match previous_rotate_home {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_HOME", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_HOME") },
        }

        assert_eq!(resolved_rotate_home, override_home);
    }

    #[test]
    fn skips_local_daemon_refresh_while_invoke_is_active() {
        let daemon = SharedDaemon::new();
        let _guard = InvocationGuard::new(daemon.in_flight_invocations.clone());

        let result =
            maybe_refresh_local_daemon_process(Some(&daemon), false).expect("refresh result");

        assert!(!result);
    }

    #[test]
    fn shutdown_invoke_does_not_wait_for_the_daemon_state_lock() {
        let daemon = SharedDaemon::new();
        let state_guard = daemon.state.lock().expect("daemon state mutex");
        let (sender, receiver) = mpsc::channel();
        let daemon_for_thread = daemon.clone();

        let handle = thread::spawn(move || {
            let result = daemon_for_thread.handle_invoke(InvokeAction::Shutdown);
            sender.send(result).expect("send shutdown result");
        });

        let response = receiver.recv_timeout(Duration::from_millis(200));
        drop(state_guard);
        handle.join().expect("join shutdown thread");

        let output = response.expect("shutdown should complete without waiting for the state lock");
        assert_eq!(
            output.expect("shutdown invoke"),
            "Stopping Codex Rotate daemon."
        );
        assert!(daemon.shutdown_requested());
        assert_eq!(
            daemon.snapshot().last_message.as_deref(),
            Some("Codex Rotate is shutting down.")
        );
    }

    #[test]
    fn shutdown_when_idle_waits_for_active_operations_to_finish() {
        let daemon = SharedDaemon::new();
        let _activity = ActivityGuard::new(daemon.active_operations.clone());

        let output = daemon
            .handle_invoke(InvokeAction::ShutdownWhenIdle)
            .expect("shutdown when idle");

        assert_eq!(
            output,
            "Will stop Codex Rotate after the current task finishes."
        );
        assert!(!daemon.shutdown_requested());
        assert!(daemon.shutdown_when_idle_requested());
        assert!(!poll_shutdown_request(&daemon));
    }

    #[test]
    fn shutdown_when_idle_requests_shutdown_once_idle() {
        let daemon = SharedDaemon::new();

        let output = daemon
            .handle_invoke(InvokeAction::ShutdownWhenIdle)
            .expect("shutdown when idle");

        assert_eq!(output, "Stopping Codex Rotate daemon.");
        assert!(daemon.shutdown_requested());
        assert!(poll_shutdown_request(&daemon));
    }

    #[test]
    fn next_watch_interval_adapts_between_healthy_and_risky_states() {
        assert_eq!(next_watch_interval(None), Duration::from_secs(15));
        assert_eq!(next_watch_interval(Some(100)), Duration::from_secs(30));
        assert_eq!(next_watch_interval(Some(21)), Duration::from_secs(30));
        assert_eq!(next_watch_interval(Some(20)), Duration::from_secs(15));
        assert_eq!(next_watch_interval(Some(2)), Duration::from_secs(15));
        assert_eq!(next_watch_interval(Some(0)), Duration::from_secs(15));
    }

    #[test]
    fn initialize_runtime_reuses_fresh_watch_quota_cache_without_probe() {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let rotate_home = tempdir.path().join("rotate");
        let codex_home = tempdir.path().join("codex");
        fs::create_dir_all(&rotate_home).expect("create rotate home");
        fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_wham = std::env::var_os("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");

        let body = r#"{"user_id":"user-1","account_id":"acct-123","email":"dev.audit@astronlab.com","plan_type":"free","rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":40.0,"limit_window_seconds":18000,"reset_after_seconds":3600,"reset_at":1775185200},"secondary_window":{"used_percent":0.0,"limit_window_seconds":604800,"reset_after_seconds":86400,"reset_at":1775271600}},"code_review_rate_limit":null,"additional_rate_limits":null,"credits":null,"promo":null}"#.to_string();
        let (wham_url, request_count, stop_server) = spawn_quota_server(body);

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
            std::env::set_var("CODEX_HOME", &codex_home);
            std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", &wham_url);
        }

        write_codex_auth(&codex_home.join("auth.json"), &make_test_auth("acct-123"))
            .expect("write auth");
        crate::watch::write_watch_state(&crate::watch::WatchState {
            accounts: std::iter::once((
                "acct-123".to_string(),
                crate::watch::AccountWatchState {
                    quota: Some(CachedQuotaState {
                        account_id: "acct-123".to_string(),
                        fetched_at: "2026-04-08T08:00:00.000Z".to_string(),
                        next_refresh_at: "2099-04-08T08:30:00.000Z".to_string(),
                        summary: "5h 60% left".to_string(),
                        usable: true,
                        blocker: None,
                        primary_quota_left_percent: Some(60),
                        error: None,
                    }),
                    ..crate::watch::AccountWatchState::default()
                },
            ))
            .collect(),
            ..crate::watch::WatchState::default()
        })
        .expect("write watch state");

        let daemon = SharedDaemon::new();
        initialize_runtime(&daemon);

        stop_server.store(true, Ordering::Relaxed);

        match previous_rotate_home {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_HOME", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_HOME") },
        }
        match previous_codex_home {
            Some(value) => unsafe { std::env::set_var("CODEX_HOME", value) },
            None => unsafe { std::env::remove_var("CODEX_HOME") },
        }
        match previous_wham {
            Some(value) => unsafe {
                std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", value)
            },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE") },
        }

        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        let cached_summary = daemon
            .with_state_mut(|state| Ok(state.snapshot.current_quota.clone()))
            .expect("read daemon state");
        assert_eq!(cached_summary.as_deref(), Some("5h 60% left"));
    }

    #[test]
    fn refresh_static_snapshot_auto_adds_missing_auth_account_into_pool() {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let rotate_home = tempdir.path().join("rotate");
        let codex_home = tempdir.path().join("codex");
        fs::create_dir_all(&rotate_home).expect("create rotate home");
        fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let paths = resolve_paths()?;
            let pool_file = paths.rotate_home.join("accounts.json");
            fs::write(
                &pool_file,
                serde_json::to_string_pretty(&serde_json::json!({
                    "accounts": [{
                        "label": "dev.1@astronlab.com_free",
                        "email": "dev.1@astronlab.com",
                        "account_id": "acct-1",
                        "plan_type": "free",
                        "auth": make_test_auth_with_email("dev.1@astronlab.com", "acct-1", "free"),
                        "added_at": "2026-04-08T00:00:00.000Z"
                    }],
                    "active_index": 0
                }))?,
            )?;
            write_codex_auth(
                &codex_home.join("auth.json"),
                &make_test_auth_with_email("dev.2@astronlab.com", "acct-2", "free"),
            )?;

            let mut state = DaemonState::new();
            refresh_static_snapshot(&mut state);

            let pool = load_pool()?;
            assert_eq!(pool.accounts.len(), 2);
            assert_eq!(pool.active_index, 0);
            assert_eq!(pool.accounts[1].email, "dev.2@astronlab.com");
            assert_eq!(pool.accounts[1].account_id, "acct-2");

            let snapshot = state.snapshot;
            assert_eq!(snapshot.inventory_count, Some(2));
            assert_eq!(snapshot.inventory_active_slot, Some(1));
            Ok(())
        })();

        match previous_rotate_home {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_HOME", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_HOME") },
        }
        match previous_codex_home {
            Some(value) => unsafe { std::env::set_var("CODEX_HOME", value) },
            None => unsafe { std::env::remove_var("CODEX_HOME") },
        }

        result.expect("refresh_static_snapshot should auto-add the missing auth account");
    }

    #[test]
    fn refresh_static_snapshot_restores_missing_auth_from_active_pool() {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let rotate_home = tempdir.path().join("rotate");
        let codex_home = tempdir.path().join("codex");
        fs::create_dir_all(&rotate_home).expect("create rotate home");
        fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let paths = resolve_paths()?;
            fs::write(
                paths.rotate_home.join("accounts.json"),
                serde_json::to_string_pretty(&serde_json::json!({
                    "accounts": [{
                        "label": "dev.1@astronlab.com_free",
                        "email": "dev.1@astronlab.com",
                        "account_id": "acct-1",
                        "plan_type": "free",
                        "auth": make_test_auth_with_email("dev.1@astronlab.com", "acct-1", "free"),
                        "added_at": "2026-04-15T00:00:00.000Z"
                    }],
                    "active_index": 0
                }))?,
            )?;

            let mut state = DaemonState::new();
            refresh_static_snapshot(&mut state);

            assert_eq!(
                state.snapshot.current_email.as_deref(),
                Some("dev.1@astronlab.com")
            );
            assert_eq!(state.snapshot.current_plan.as_deref(), Some("free"));
            assert!(paths.codex_auth_file.exists());
            Ok(())
        })();

        match previous_rotate_home {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_HOME", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_HOME") },
        }
        match previous_codex_home {
            Some(value) => unsafe { std::env::set_var("CODEX_HOME", value) },
            None => unsafe { std::env::remove_var("CODEX_HOME") },
        }

        result.expect("refresh_static_snapshot should restore missing auth from the active pool");
    }

    #[test]
    fn refresh_quota_state_skips_probe_when_in_memory_cache_is_fresh() {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let rotate_home = tempdir.path().join("rotate");
        let codex_home = tempdir.path().join("codex");
        fs::create_dir_all(&rotate_home).expect("create rotate home");
        fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_wham = std::env::var_os("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");

        let body = r#"{"user_id":"user-1","account_id":"acct-123","email":"dev.audit@astronlab.com","plan_type":"free","rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":40.0,"limit_window_seconds":18000,"reset_after_seconds":3600,"reset_at":1775185200},"secondary_window":{"used_percent":0.0,"limit_window_seconds":604800,"reset_after_seconds":86400,"reset_at":1775271600}},"code_review_rate_limit":null,"additional_rate_limits":null,"credits":null,"promo":null}"#.to_string();
        let (wham_url, request_count, stop_server) = spawn_quota_server(body);

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
            std::env::set_var("CODEX_HOME", &codex_home);
            std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", &wham_url);
        }

        write_codex_auth(&codex_home.join("auth.json"), &make_test_auth("acct-123"))
            .expect("write auth");

        let mut state = DaemonState::new();
        state.quota_cache = Some(CachedQuotaState {
            account_id: "acct-123".to_string(),
            fetched_at: "2026-04-08T08:00:00.000Z".to_string(),
            next_refresh_at: "2099-04-08T08:30:00.000Z".to_string(),
            summary: "5h 60% left".to_string(),
            usable: true,
            blocker: None,
            primary_quota_left_percent: Some(60),
            error: None,
        });

        refresh_quota_state(&mut state, false);

        stop_server.store(true, Ordering::Relaxed);

        match previous_rotate_home {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_HOME", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_HOME") },
        }
        match previous_codex_home {
            Some(value) => unsafe { std::env::set_var("CODEX_HOME", value) },
            None => unsafe { std::env::remove_var("CODEX_HOME") },
        }
        match previous_wham {
            Some(value) => unsafe {
                std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", value)
            },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE") },
        }

        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        assert_eq!(state.snapshot.current_quota.as_deref(), Some("5h 60% left"));
    }
}
