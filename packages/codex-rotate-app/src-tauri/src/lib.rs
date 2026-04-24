use codex_rotate_refresh::{
    current_process_local_build, daemon_socket_is_older_than_binary, detect_local_build,
    local_refresh_disabled, maybe_start_background_release_build, preferred_release_binary,
    rebuild_local_binary, schedule_tray_relaunch_process, sources_newer_than_binary,
    spawn_detached_process, stop_running_daemons, supports_live_local_refresh, TargetKind,
};
use codex_rotate_runtime::ipc::{
    daemon_is_reachable, daemon_socket_path, subscribe, SnapshotMessageKind, StatusSnapshot,
};
use codex_rotate_runtime::log_isolation::active_managed_codex_thread_ids;
use codex_rotate_runtime::runtime_log::{log_tray_error, log_tray_info};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const DAEMON_START_ATTEMPTS: usize = 40;
const LOCAL_BUILD_DAEMON_START_ATTEMPTS: usize = 240;
const DAEMON_START_POLL_INTERVAL: Duration = Duration::from_millis(250);
const DAEMON_START_RETRY_DELAY: Duration = Duration::from_secs(2);
const DAEMON_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const TRAY_REFRESH_CHECK_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone, Default)]
pub struct SharedRenderState {
    inner: Arc<Mutex<RenderState>>,
}

#[derive(Clone, Default)]
struct RenderState {
    last_rendered: Option<RenderedSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderedSnapshot {
    pub account_text: String,
    pub inventory_text: String,
    pub plan_text: String,
    pub quota_text: String,
    pub status_text: String,
    pub rotation_text: String,
    pub tooltip_text: String,
    pub quota_percent: Option<u8>,
    pub show_activity_badge: bool,
    pub auto_create_enabled: bool,
    pub launch_enabled: bool,
    pub check_enabled: bool,
    pub rotate_enabled: bool,
}

impl SharedRenderState {
    pub fn begin_render(&self, rendered: &RenderedSnapshot) -> bool {
        let mut state = self.inner.lock().expect("render state mutex");
        if state.last_rendered.as_ref() == Some(rendered) {
            return false;
        }
        state.last_rendered = Some(rendered.clone());
        true
    }
}

pub fn rendered_snapshot(snapshot: &StatusSnapshot) -> RenderedSnapshot {
    let rotation_text = match (
        snapshot.last_rotation_from_email.as_deref(),
        snapshot.last_rotation_to_email.as_deref(),
    ) {
        (Some(from), Some(to)) if from != to => format!("Last rotation: {from} -> {to}"),
        (_, Some(to)) => format!("Last rotation: {to}"),
        _ => "Last rotation: none".to_string(),
    };

    let status_body = match (
        snapshot.last_message.as_deref(),
        snapshot.next_tick_at.as_deref(),
    ) {
        (Some(message), Some(next_tick_at)) => {
            format!("{message} (next tick {next_tick_at})")
        }
        (Some(message), None) => message.to_string(),
        (None, Some(next_tick_at)) => format!("next tick {next_tick_at}"),
        (None, None) => "starting".to_string(),
    };

    RenderedSnapshot {
        account_text: format!(
            "Account: {}",
            snapshot.current_email.as_deref().unwrap_or("unknown")
        ),
        inventory_text: match (
            snapshot.inventory_active_slot,
            snapshot.inventory_healthy_count,
            snapshot.inventory_count,
        ) {
            (Some(slot), Some(healthy), Some(count)) if count > 0 && slot <= count => {
                format!("Inventory: {slot} current | {healthy} healthy | {count} total")
            }
            (_, Some(healthy), Some(count)) => {
                format!("Inventory: {healthy} healthy | {count} total")
            }
            (_, _, Some(count)) => format!("Inventory: {count} total"),
            (_, _, None) => "Inventory: unknown".to_string(),
        },
        plan_text: format!(
            "Plan: {}",
            snapshot.current_plan.as_deref().unwrap_or("unknown")
        ),
        quota_text: format!(
            "Quota: {}",
            snapshot.current_quota.as_deref().unwrap_or("unknown")
        ),
        status_text: format!("Status: {}", status_body),
        rotation_text,
        tooltip_text: match snapshot.current_quota_percent {
            Some(percent) => format!("Codex Rotate\nQuota: {percent}%\nClick for status"),
            None => "Codex Rotate\nClick for status".to_string(),
        },
        quota_percent: snapshot.current_quota_percent,
        show_activity_badge: snapshot.last_message_kind == Some(SnapshotMessageKind::Progress),
        auto_create_enabled: snapshot.auto_create_enabled,
        launch_enabled: snapshot.capabilities.managed_launch,
        check_enabled: snapshot.capabilities.quota_watch,
        rotate_enabled: snapshot.capabilities.live_account_sync,
    }
}

pub fn error_snapshot(message: impl Into<String>) -> StatusSnapshot {
    StatusSnapshot {
        last_message: Some(message.into()),
        ..StatusSnapshot::default()
    }
}

pub fn resolve_cli_binary() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os("CODEX_ROTATE_CLI_BIN").map(PathBuf::from) {
        return Ok(path);
    }

    let current_exe = std::env::current_exe().map_err(|error| error.to_string())?;
    let current_dir = current_exe
        .parent()
        .ok_or_else(|| "Failed to resolve the tray binary directory.".to_string())?;
    let binary_name = if cfg!(windows) {
        "codex-rotate.exe"
    } else {
        "codex-rotate"
    };
    Ok(current_dir.join(binary_name))
}

pub fn ensure_daemon_running() -> Result<(), String> {
    let cli_binary = resolve_cli_binary()?;
    let max_attempts = if detect_local_build(&cli_binary, TargetKind::Cli).is_some() {
        LOCAL_BUILD_DAEMON_START_ATTEMPTS
    } else {
        DAEMON_START_ATTEMPTS
    };
    ensure_daemon_running_with(&cli_binary, DAEMON_START_POLL_INTERVAL, max_attempts)
}

fn ensure_daemon_running_with(
    cli_binary: &Path,
    poll_interval: Duration,
    max_attempts: usize,
) -> Result<(), String> {
    refresh_local_daemon_if_needed(&cli_binary, poll_interval, max_attempts)?;
    if daemon_is_reachable() {
        return Ok(());
    }
    log_tray_info(format!(
        "Launching Codex Rotate daemon via {}.",
        cli_binary.display()
    ));
    spawn_detached_process(cli_binary, &["daemon"])
        .map_err(|error| format!("Failed to launch {}: {}", cli_binary.display(), error))?;

    for _ in 0..max_attempts {
        if daemon_is_reachable() {
            return Ok(());
        }
        thread::sleep(poll_interval);
    }

    Err("Timed out waiting for the Codex Rotate daemon to start.".to_string())
}

fn refresh_local_daemon_if_needed(
    cli_binary: &Path,
    poll_interval: Duration,
    max_attempts: usize,
) -> Result<(), String> {
    if local_refresh_disabled() {
        return Ok(());
    }
    let Some(build) = detect_local_build(cli_binary, TargetKind::Cli) else {
        return Ok(());
    };
    let daemon_socket = daemon_socket_path()
        .map_err(|error| format!("Failed to resolve daemon socket: {error}"))?;
    let sources_newer_than_binary = sources_newer_than_binary(&build)
        .map_err(|error| format!("Failed to inspect local CLI freshness: {error}"))?;
    if sources_newer_than_binary {
        rebuild_local_binary(&build)
            .map_err(|error| format!("Failed to rebuild local codex-rotate CLI: {error}"))?;
    }
    let binary_newer_than_running_daemon =
        daemon_socket_is_older_than_binary(&daemon_socket, cli_binary)
            .map_err(|error| format!("Failed to compare daemon freshness: {error}"))?;
    if !sources_newer_than_binary && !binary_newer_than_running_daemon {
        return Ok(());
    }

    if daemon_is_reachable() {
        stop_running_daemons(cli_binary, &daemon_socket)
            .map_err(|error| format!("Failed to stop the stale local daemon: {error}"))?;
        if !wait_for_daemon_state(false, poll_interval, max_attempts) {
            return Err("Timed out waiting for the stale Codex Rotate daemon to stop.".to_string());
        }
    }
    Ok(())
}

fn wait_for_daemon_state(reachable: bool, poll_interval: Duration, max_attempts: usize) -> bool {
    for _ in 0..max_attempts {
        if daemon_is_reachable() == reachable {
            return true;
        }
        thread::sleep(poll_interval);
    }
    daemon_is_reachable() == reachable
}

pub fn maybe_refresh_current_tray() -> Result<bool, String> {
    if local_refresh_disabled() {
        return Ok(false);
    }
    let Some(build) = current_process_local_build(TargetKind::Tray) else {
        return Ok(false);
    };
    if !supports_live_local_refresh(&build) {
        return Ok(false);
    }
    let sources_newer_than_binary = sources_newer_than_binary(&build)
        .map_err(|error| format!("Failed to inspect local tray freshness: {error}"))?;
    let release_binary = preferred_release_binary(&build)
        .map_err(|error| format!("Failed to inspect release tray freshness: {error}"))?;
    if !sources_newer_than_binary && release_binary.is_none() {
        return Ok(false);
    }
    if auto_refresh_blocked_by_active_threads()? {
        return Ok(false);
    }

    if sources_newer_than_binary {
        log_tray_info(format!(
            "Local tray sources changed. Rebuilding {}.",
            build.binary_path.display()
        ));
        rebuild_local_binary(&build)
            .map_err(|error| format!("Failed to rebuild local codex-rotate tray: {error}"))?;
    }
    if maybe_start_background_release_build(&build)
        .map_err(|error| format!("Failed to queue background release tray build: {error}"))?
    {
        log_tray_info("Queued background release build for codex-rotate-tray.");
    }
    if let Some(release_binary) = release_binary {
        log_tray_info(format!(
            "Promoting tray to release binary {}.",
            release_binary.display()
        ));
        schedule_tray_relaunch(&release_binary)
            .map_err(|error| format!("Failed to relaunch local tray: {error}"))?;
        return Ok(true);
    }
    if !sources_newer_than_binary {
        return Ok(false);
    }

    log_tray_info("Scheduling tray relaunch after rebuild.");
    schedule_tray_relaunch(&build.binary_path)
        .map_err(|error| format!("Failed to relaunch local tray: {error}"))?;
    Ok(true)
}

fn schedule_tray_relaunch(tray_binary: &Path) -> Result<(), String> {
    schedule_tray_relaunch_process(tray_binary)
        .map_err(|error| format!("Failed to relaunch local tray: {error}"))
}

fn auto_refresh_blocked_by_active_threads() -> Result<bool, String> {
    active_managed_codex_thread_ids(None)
        .map(|thread_ids| !thread_ids.is_empty())
        .map_err(|error| format!("Failed to inspect active Codex threads: {error}"))
}

pub fn spawn_subscription_loop<F>(on_snapshot: F)
where
    F: FnMut(StatusSnapshot) + Send + 'static,
{
    let _ = spawn_subscription_loop_controlled(Arc::new(AtomicBool::new(false)), on_snapshot);
}

pub fn spawn_subscription_loop_controlled<F>(
    stop: Arc<AtomicBool>,
    mut on_snapshot: F,
) -> thread::JoinHandle<()>
where
    F: FnMut(StatusSnapshot) + Send + 'static,
{
    thread::spawn(move || loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }

        if let Err(error) = ensure_daemon_running() {
            let message = format!("daemon start failed: {}", error);
            log_tray_error(&message);
            on_snapshot(error_snapshot(message));
            if sleep_or_stop(&stop, DAEMON_START_RETRY_DELAY) {
                return;
            }
            continue;
        }

        let mut subscription = match subscribe() {
            Ok(subscription) => subscription,
            Err(error) => {
                let message = format!("daemon subscribe failed: {}", error);
                log_tray_error(&message);
                on_snapshot(error_snapshot(message));
                if sleep_or_stop(&stop, DAEMON_START_RETRY_DELAY) {
                    return;
                }
                continue;
            }
        };

        loop {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            match subscription.recv() {
                Ok(snapshot) => on_snapshot(snapshot),
                Err(error) => {
                    let message = format!("daemon disconnected: {}", error);
                    log_tray_error(&message);
                    on_snapshot(error_snapshot(message));
                    if sleep_or_stop(&stop, DAEMON_RECONNECT_DELAY) {
                        return;
                    }
                    break;
                }
            }
        }
    })
}

pub fn spawn_tray_refresh_loop_controlled<F, G>(
    stop: Arc<AtomicBool>,
    mut on_restarted: F,
    mut on_error: G,
) -> thread::JoinHandle<()>
where
    F: FnMut() + Send + 'static,
    G: FnMut(String) + Send + 'static,
{
    thread::spawn(move || loop {
        if sleep_or_stop(&stop, TRAY_REFRESH_CHECK_INTERVAL) {
            return;
        }
        match maybe_refresh_current_tray() {
            Ok(true) => {
                log_tray_info("Tray refresh completed; exiting stale tray instance.");
                on_restarted();
                return;
            }
            Ok(false) => {}
            Err(error) => {
                log_tray_error(format!("tray refresh failed: {error}"));
                on_error(error)
            }
        }
    })
}

fn sleep_or_stop(stop: &AtomicBool, duration: Duration) -> bool {
    thread::sleep(duration);
    stop.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::{Mutex, OnceLock};

    static ENV_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

    fn env_mutex() -> &'static Mutex<()> {
        ENV_MUTEX.get_or_init(|| Mutex::new(()))
    }

    fn restore_var(name: &str, value: Option<OsString>) {
        match value {
            Some(value) => unsafe {
                std::env::set_var(name, value);
            },
            None => unsafe {
                std::env::remove_var(name);
            },
        }
    }

    fn sample_rendered_snapshot() -> RenderedSnapshot {
        RenderedSnapshot {
            account_text: "Account: dev.1@astronlab.com".to_string(),
            inventory_text: "Inventory: 2 current | 1 healthy | 3 total".to_string(),
            plan_text: "Plan: free".to_string(),
            quota_text: "Quota: 5h 80% left".to_string(),
            status_text: "Status: watch healthy".to_string(),
            rotation_text: "Last rotation: dev.0@astronlab.com -> dev.1@astronlab.com".to_string(),
            tooltip_text: "Codex Rotate\nQuota: 80%\nClick for status".to_string(),
            quota_percent: Some(80),
            show_activity_badge: false,
            auto_create_enabled: true,
            launch_enabled: true,
            check_enabled: true,
            rotate_enabled: true,
        }
    }

    #[test]
    fn render_state_dedups_identical_rendered_snapshots() {
        let render_state = SharedRenderState::default();
        let rendered = sample_rendered_snapshot();
        assert!(render_state.begin_render(&rendered));
        assert!(!render_state.begin_render(&rendered));
    }

    #[test]
    fn render_state_allows_changed_rendered_snapshots() {
        let render_state = SharedRenderState::default();
        let rendered = sample_rendered_snapshot();
        let mut changed = rendered.clone();
        changed.status_text = "Status: rotated".to_string();
        assert!(render_state.begin_render(&rendered));
        assert!(render_state.begin_render(&changed));
    }

    #[test]
    fn rendered_snapshot_appends_next_tick_to_idle_status() {
        let snapshot = StatusSnapshot {
            last_message: Some("watch healthy".to_string()),
            next_tick_at: Some("8:15:30 PM".to_string()),
            ..StatusSnapshot::default()
        };

        let rendered = rendered_snapshot(&snapshot);

        assert_eq!(
            rendered.status_text,
            "Status: watch healthy (next tick 8:15:30 PM)"
        );
    }

    #[test]
    fn rendered_snapshot_shows_inventory_slot_and_total() {
        let snapshot = StatusSnapshot {
            inventory_active_slot: Some(2),
            inventory_healthy_count: Some(1),
            inventory_count: Some(3),
            ..StatusSnapshot::default()
        };

        let rendered = rendered_snapshot(&snapshot);

        assert_eq!(
            rendered.inventory_text,
            "Inventory: 2 current | 1 healthy | 3 total"
        );
    }

    #[test]
    fn rendered_snapshot_marks_progress_as_activity_badge() {
        let snapshot = StatusSnapshot {
            last_message: Some("Creating dev.48@astronlab.com.".to_string()),
            last_message_kind: Some(SnapshotMessageKind::Progress),
            ..StatusSnapshot::default()
        };

        let rendered = rendered_snapshot(&snapshot);

        assert!(rendered.show_activity_badge);
    }

    #[test]
    fn rendered_snapshot_preserves_explicit_disabled_domain_message() {
        let snapshot = StatusSnapshot {
            last_message: Some("switched away from disabled domain astronlab.com".to_string()),
            ..StatusSnapshot::default()
        };

        let rendered = rendered_snapshot(&snapshot);

        assert_eq!(
            rendered.status_text,
            "Status: switched away from disabled domain astronlab.com"
        );
    }

    #[test]
    fn resolve_cli_binary_prefers_override() {
        let _guard = env_mutex().lock().expect("env mutex");
        let previous = std::env::var_os("CODEX_ROTATE_CLI_BIN");
        let expected = std::path::PathBuf::from("/tmp/codex-rotate-custom");
        unsafe {
            std::env::set_var("CODEX_ROTATE_CLI_BIN", &expected);
        }

        let resolved = resolve_cli_binary().expect("resolve cli binary");

        restore_var("CODEX_ROTATE_CLI_BIN", previous);
        assert_eq!(resolved, expected);
    }

    #[test]
    fn resolve_cli_binary_defaults_to_codex_rotate_name() {
        let _guard = env_mutex().lock().expect("env mutex");
        let previous = std::env::var_os("CODEX_ROTATE_CLI_BIN");
        unsafe {
            std::env::remove_var("CODEX_ROTATE_CLI_BIN");
        }

        let resolved = resolve_cli_binary().expect("resolve cli binary");
        let expected = if cfg!(windows) {
            "codex-rotate.exe"
        } else {
            "codex-rotate"
        };

        restore_var("CODEX_ROTATE_CLI_BIN", previous);
        assert_eq!(
            resolved.file_name().and_then(|value| value.to_str()),
            Some(expected)
        );
    }
}
