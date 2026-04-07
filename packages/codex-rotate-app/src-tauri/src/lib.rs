use codex_rotate_runtime::ipc::{daemon_is_reachable, subscribe, StatusSnapshot};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const DAEMON_START_ATTEMPTS: usize = 40;
const DAEMON_START_POLL_INTERVAL: Duration = Duration::from_millis(250);
const DAEMON_START_RETRY_DELAY: Duration = Duration::from_secs(2);
const DAEMON_RECONNECT_DELAY: Duration = Duration::from_secs(1);

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

    RenderedSnapshot {
        account_text: format!(
            "Account: {}",
            snapshot.current_email.as_deref().unwrap_or("unknown")
        ),
        inventory_text: match snapshot.inventory_count {
            Some(count) => format!("Inventory: {count} account(s)"),
            None => "Inventory: unknown".to_string(),
        },
        plan_text: format!(
            "Plan: {}",
            snapshot.current_plan.as_deref().unwrap_or("unknown")
        ),
        quota_text: format!(
            "Quota: {}",
            snapshot.current_quota.as_deref().unwrap_or("unknown")
        ),
        status_text: format!(
            "Status: {}",
            snapshot.last_message.as_deref().unwrap_or("starting")
        ),
        rotation_text,
        tooltip_text: match snapshot.current_quota_percent {
            Some(percent) => format!("Codex Rotate v2\nQuota: {percent}%\nClick for status"),
            None => "Codex Rotate v2\nClick for status".to_string(),
        },
        quota_percent: snapshot.current_quota_percent,
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
        "codex-rotate-v2.exe"
    } else {
        "codex-rotate-v2"
    };
    Ok(current_dir.join(binary_name))
}

pub fn ensure_daemon_running() -> Result<(), String> {
    ensure_daemon_running_with(DAEMON_START_POLL_INTERVAL, DAEMON_START_ATTEMPTS)
}

fn ensure_daemon_running_with(
    poll_interval: Duration,
    max_attempts: usize,
) -> Result<(), String> {
    if daemon_is_reachable() {
        return Ok(());
    }

    let cli_binary = resolve_cli_binary()?;
    let _child = Command::new(&cli_binary)
        .arg("daemon")
        .arg("run")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| format!("Failed to launch {}: {}", cli_binary.display(), error))?;

    for _ in 0..max_attempts {
        if daemon_is_reachable() {
            return Ok(());
        }
        thread::sleep(poll_interval);
    }

    Err("Timed out waiting for the Codex Rotate v2 daemon to start.".to_string())
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
            on_snapshot(error_snapshot(format!("daemon start failed: {}", error)));
            if sleep_or_stop(&stop, DAEMON_START_RETRY_DELAY) {
                return;
            }
            continue;
        }

        let mut subscription = match subscribe() {
            Ok(subscription) => subscription,
            Err(error) => {
                on_snapshot(error_snapshot(format!("daemon subscribe failed: {}", error)));
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
                    on_snapshot(error_snapshot(format!("daemon disconnected: {}", error)));
                    if sleep_or_stop(&stop, DAEMON_RECONNECT_DELAY) {
                        return;
                    }
                    break;
                }
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
            inventory_text: "Inventory: 3 account(s)".to_string(),
            plan_text: "Plan: free".to_string(),
            quota_text: "Quota: 5h 80% left".to_string(),
            status_text: "Status: watch healthy".to_string(),
            rotation_text: "Last rotation: dev.0@astronlab.com -> dev.1@astronlab.com".to_string(),
            tooltip_text: "Codex Rotate v2\nQuota: 80%\nClick for status".to_string(),
            quota_percent: Some(80),
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
    fn resolve_cli_binary_prefers_override() {
        let _guard = env_mutex().lock().expect("env mutex");
        let previous = std::env::var_os("CODEX_ROTATE_CLI_BIN");
        let expected = std::path::PathBuf::from("/tmp/codex-rotate-v2-custom");
        unsafe {
            std::env::set_var("CODEX_ROTATE_CLI_BIN", &expected);
        }

        let resolved = resolve_cli_binary().expect("resolve cli binary");

        restore_var("CODEX_ROTATE_CLI_BIN", previous);
        assert_eq!(resolved, expected);
    }

    #[test]
    fn resolve_cli_binary_defaults_to_v2_name() {
        let _guard = env_mutex().lock().expect("env mutex");
        let previous = std::env::var_os("CODEX_ROTATE_CLI_BIN");
        unsafe {
            std::env::remove_var("CODEX_ROTATE_CLI_BIN");
        }

        let resolved = resolve_cli_binary().expect("resolve cli binary");
        let expected = if cfg!(windows) {
            "codex-rotate-v2.exe"
        } else {
            "codex-rotate-v2"
        };

        restore_var("CODEX_ROTATE_CLI_BIN", previous);
        assert_eq!(
            resolved.file_name().and_then(|value| value.to_str()),
            Some(expected)
        );
    }

}
