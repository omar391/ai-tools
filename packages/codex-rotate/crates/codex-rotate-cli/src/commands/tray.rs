use std::env;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
#[cfg(not(target_os = "macos"))]
use codex_rotate_refresh::stop_running_trays;
use codex_rotate_refresh::{
    clear_tray_service_registration, detect_local_build, launch_tray_process,
    preferred_release_binary, rebuild_local_binary, sources_newer_than_binary,
    stop_running_daemons, tray_service_pid, TargetKind,
};
use codex_rotate_runtime::ipc::{daemon_is_reachable, daemon_socket_path, invoke, InvokeAction};
use codex_rotate_runtime::watch::set_tray_enabled;

use crate::write_output;

const DAEMON_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) fn run_tray_command(writer: &mut dyn Write, args: &[String]) -> Result<()> {
    let command = args.first().map(String::as_str).unwrap_or("open");
    match command {
        "open" => write_output(writer, &tray_open_message()?),
        "status" => {
            let tray_running = tray_is_running()?;
            if tray_running && daemon_is_reachable() {
                write_output(writer, "Codex Rotate tray is running.")
            } else if tray_running {
                Err(anyhow!(
                    "Codex Rotate tray is running but the daemon is unavailable."
                ))
            } else {
                Err(anyhow!("Codex Rotate tray is not running."))
            }
        }
        "quit" => write_output(writer, &tray_quit_message()?),
        "restart" => {
            let _ = tray_quit_message()?;
            write_output(writer, &tray_open_message()?)
        }
        "help" | "--help" | "-h" => write_output(
            writer,
            "Usage: codex-rotate tray [open|status|quit|restart]",
        ),
        other => Err(anyhow!(
            "Unknown tray command: \"{other}\". Run \"codex-rotate tray help\" for usage."
        )),
    }
}

fn tray_open_message() -> Result<String> {
    let tray_binary = resolve_tray_binary()?;
    set_tray_enabled(true)?;
    refresh_local_tray_if_needed(&tray_binary)?;
    if tray_is_running_with_path(&tray_binary)? {
        if daemon_is_reachable() {
            return Ok("Codex Rotate tray is already running.".to_string());
        }
        clear_tray_service_registration();
        #[cfg(not(target_os = "macos"))]
        stop_running_trays(&tray_binary)?;
        if !wait_for_tray_state(&tray_binary, false) {
            return Err(anyhow!(
                "Timed out waiting for the unhealthy Codex Rotate tray to stop."
            ));
        }
    }

    launch_tray_binary(&tray_binary)?;

    if wait_for_tray_state(&tray_binary, true) {
        wait_for_stable_tray_after_open(&tray_binary)?;
        return Ok("Started Codex Rotate tray.".to_string());
    }

    Err(anyhow!(
        "Timed out waiting for the Codex Rotate tray to start."
    ))
}

fn tray_quit_message() -> Result<String> {
    let tray_binary = resolve_tray_binary()?;
    set_tray_enabled(false)?;
    let daemon_was_running = daemon_is_reachable();
    if daemon_was_running {
        request_daemon_shutdown(DAEMON_SHUTDOWN_TIMEOUT);
    }
    #[cfg(target_os = "macos")]
    {
        if !tray_is_running_with_path(&tray_binary)? {
            clear_tray_service_registration();
            stop_daemon_if_still_running()?;
            return Ok(if daemon_was_running {
                "Stopped Codex Rotate daemon.".to_string()
            } else {
                "Codex Rotate tray is not running.".to_string()
            });
        }
        clear_tray_service_registration();
        if wait_for_tray_state(&tray_binary, false) {
            stop_daemon_if_still_running()?;
            return Ok(if daemon_was_running {
                "Stopped Codex Rotate tray and daemon.".to_string()
            } else {
                "Stopped Codex Rotate tray.".to_string()
            });
        }
        Err(anyhow!(
            "Timed out waiting for the Codex Rotate tray to stop."
        ))
    }

    #[cfg(not(target_os = "macos"))]
    {
        let process_ids = list_running_tray_process_ids(&tray_binary)?;
        if process_ids.is_empty() {
            clear_tray_service_registration();
            stop_daemon_if_still_running()?;
            return Ok(if daemon_was_running {
                "Stopped Codex Rotate daemon.".to_string()
            } else {
                "Codex Rotate tray is not running.".to_string()
            });
        }

        for process_id in process_ids {
            stop_process(process_id)
                .with_context(|| format!("Failed to stop tray pid {}.", process_id))?;
        }
        clear_tray_service_registration();

        if wait_for_tray_state(&tray_binary, false) {
            stop_daemon_if_still_running()?;
            return Ok(if daemon_was_running {
                "Stopped Codex Rotate tray and daemon.".to_string()
            } else {
                "Stopped Codex Rotate tray.".to_string()
            });
        }

        Err(anyhow!(
            "Timed out waiting for the Codex Rotate tray to stop."
        ))
    }
}

pub(crate) fn run_with_timeout<F, T>(timeout: Duration, operation: F) -> Option<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let _ = sender.send(operation());
    });
    match receiver.recv_timeout(timeout) {
        Ok(result) => Some(result),
        Err(RecvTimeoutError::Timeout) | Err(RecvTimeoutError::Disconnected) => None,
    }
}

fn request_daemon_shutdown(timeout: Duration) -> bool {
    matches!(
        run_with_timeout(timeout, || invoke(InvokeAction::Shutdown)),
        Some(Ok(_))
    )
}

fn stop_daemon_if_still_running() -> Result<()> {
    if !daemon_is_reachable() {
        return Ok(());
    }

    let current_binary =
        env::current_exe().context("Failed to resolve the codex-rotate CLI binary.")?;
    let daemon_socket = daemon_socket_path().context("Failed to resolve daemon socket path.")?;
    stop_running_daemons(&current_binary, &daemon_socket)?;

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if !daemon_is_reachable() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }

    if daemon_is_reachable() {
        return Err(anyhow!(
            "Timed out waiting for the Codex Rotate daemon to stop."
        ));
    }
    Ok(())
}

fn tray_is_running() -> Result<bool> {
    let tray_binary = resolve_tray_binary()?;
    tray_is_running_with_path(&tray_binary)
}

fn tray_is_running_with_path(tray_binary: &Path) -> Result<bool> {
    #[cfg(target_os = "macos")]
    {
        let _ = tray_binary;
        Ok(tray_service_pid()?.is_some())
    }

    #[cfg(not(target_os = "macos"))]
    Ok(!list_running_tray_process_ids(tray_binary)?.is_empty())
}

fn refresh_local_tray_if_needed(tray_binary: &Path) -> Result<()> {
    let Some(build) = detect_local_build(tray_binary, TargetKind::Tray) else {
        return Ok(());
    };
    let sources_newer_than_binary = sources_newer_than_binary(&build)?;
    if !sources_newer_than_binary {
        return Ok(());
    }

    rebuild_local_binary(&build)?;
    if tray_is_running_with_path(tray_binary)? {
        #[cfg(target_os = "macos")]
        clear_tray_service_registration();
        #[cfg(not(target_os = "macos"))]
        stop_running_trays(tray_binary)?;
        if !wait_for_tray_state(tray_binary, false) {
            return Err(anyhow!(
                "Timed out waiting for the stale Codex Rotate tray to stop."
            ));
        }
    }
    Ok(())
}

fn launch_tray_binary(tray_binary: &Path) -> Result<()> {
    launch_tray_process(tray_binary)
}

#[cfg(target_os = "macos")]
fn wait_for_stable_tray_after_open(tray_binary: &Path) -> Result<()> {
    let expected_binaries = stable_tray_binary_candidates(tray_binary)?;
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if tray_service_matches_any_binary(&expected_binaries)? {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(anyhow!(
        "Timed out waiting for Codex Rotate tray to settle on one of: {}.",
        expected_binaries
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

#[cfg(not(target_os = "macos"))]
fn wait_for_stable_tray_after_open(_tray_binary: &Path) -> Result<()> {
    Ok(())
}

fn wait_for_tray_state(tray_binary: &Path, running: bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tray_is_running_with_path(tray_binary) {
            Ok(value) if value == running => return true,
            _ => thread::sleep(Duration::from_millis(100)),
        }
    }
    tray_is_running_with_path(tray_binary).ok() == Some(running)
}

#[cfg(target_os = "macos")]
fn tray_service_matches_any_binary(expected_binaries: &[PathBuf]) -> Result<bool> {
    let Some(process_id) = tray_service_pid()? else {
        return Ok(false);
    };
    let output = Command::new("ps")
        .args(["-p", &process_id.to_string(), "-o", "command="])
        .output()?;
    if !output.status.success() {
        return Ok(false);
    }
    let command = String::from_utf8_lossy(&output.stdout);
    Ok(expected_binaries
        .iter()
        .any(|expected_binary| service_command_matches_binary(&command, expected_binary)))
}

#[cfg(target_os = "macos")]
pub(crate) fn stable_tray_binary_candidates(tray_binary: &Path) -> Result<Vec<PathBuf>> {
    let mut candidates = vec![tray_binary.to_path_buf()];
    let Some(build) = detect_local_build(tray_binary, TargetKind::Tray) else {
        return Ok(candidates);
    };
    let Some(release_binary) = preferred_release_binary(&build)? else {
        return Ok(candidates);
    };
    if !candidates.contains(&release_binary) {
        candidates.push(release_binary);
    }
    Ok(candidates)
}

#[cfg(target_os = "macos")]
pub(crate) fn service_command_matches_binary(command: &str, binary: &Path) -> bool {
    if command_matches_binary(command, binary) {
        return true;
    }
    let mut parts = command.split_whitespace();
    let first = parts.next();
    let second = parts.next();
    let binary = binary.display().to_string();
    shell_like_command(first) && second == Some(binary.as_str())
}

pub(crate) fn command_matches_binary(command: &str, binary: &Path) -> bool {
    command.split_whitespace().next().map(Path::new) == Some(binary)
}

#[cfg(not(target_os = "macos"))]
fn list_running_tray_process_ids(tray_binary: &Path) -> Result<Vec<u32>> {
    let tray_binaries = tray_binary_candidates(tray_binary);

    #[cfg(windows)]
    {
        let output = Command::new("tasklist")
            .args([
                "/FO",
                "CSV",
                "/NH",
                "/FI",
                &format!("IMAGENAME eq {}", tray_binary_name()),
            ])
            .output()
            .context("Failed to query running tray processes.")?;
        if !output.status.success() {
            let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(anyhow!(
                "{}",
                if detail.is_empty() {
                    "Failed to query running tray processes.".to_string()
                } else {
                    detail
                }
            ));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Ok(stdout
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with("INFO:"))
            .filter_map(|line| {
                let columns = line
                    .split("\",\"")
                    .map(|value| value.trim_matches('"'))
                    .collect::<Vec<_>>();
                columns.get(1).and_then(|value| parse_process_id(value))
            })
            .collect::<Vec<_>>());
    }

    #[cfg(not(windows))]
    {
        let output = Command::new("ps")
            .args(["ax", "-o", "pid=,command="])
            .output()
            .context("Failed to query running tray processes.")?;
        if !output.status.success() {
            let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(anyhow!(
                "{}",
                if detail.is_empty() {
                    "Failed to query running tray processes.".to_string()
                } else {
                    detail
                }
            ));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Ok(stdout
            .lines()
            .map(str::trim)
            .filter(|line| {
                let mut parts = line.split_whitespace();
                let _pid = parts.next();
                let first = parts.next();
                let second = parts.next();
                tray_binaries
                    .iter()
                    .any(|tray_binary| command_tokens_match_binary(first, second, tray_binary))
            })
            .filter_map(|line| line.split_whitespace().next().and_then(parse_process_id))
            .collect::<Vec<_>>());
    }
}

#[cfg(not(target_os = "macos"))]
fn tray_binary_candidates(tray_binary: &Path) -> Vec<String> {
    let mut binaries = vec![tray_binary.display().to_string()];
    let Some(build) = detect_local_build(tray_binary, TargetKind::Tray) else {
        return binaries;
    };

    let Some(binary_name) = tray_binary.file_name() else {
        return binaries;
    };
    for candidate in [
        build
            .repo_root
            .join("target")
            .join("debug")
            .join(binary_name),
        build
            .repo_root
            .join("target")
            .join("release")
            .join(binary_name),
    ] {
        let candidate = candidate.display().to_string();
        if !binaries.contains(&candidate) {
            binaries.push(candidate);
        }
    }
    binaries
}

#[cfg(not(target_os = "macos"))]
fn command_tokens_match_binary(first: Option<&str>, second: Option<&str>, binary: &str) -> bool {
    first == Some(binary) || (shell_like_command(first) && second == Some(binary))
}

fn shell_like_command(command: Option<&str>) -> bool {
    let Some(command) = command else {
        return false;
    };
    let Some(name) = Path::new(command)
        .file_name()
        .and_then(|value| value.to_str())
    else {
        return false;
    };
    matches!(name, "sh" | "bash" | "zsh" | "dash")
}

#[cfg(not(target_os = "macos"))]
fn stop_process(process_id: u32) -> Result<()> {
    #[cfg(windows)]
    {
        let status = Command::new("taskkill")
            .args(["/PID", &process_id.to_string(), "/T", "/F"])
            .status()
            .context("Failed to invoke taskkill.")?;
        if status.success() {
            return Ok(());
        }
        return Err(anyhow!("taskkill exited with status {}.", status));
    }

    #[cfg(not(windows))]
    {
        let status = Command::new("kill")
            .args(["-TERM", &process_id.to_string()])
            .status()
            .context("Failed to invoke kill.")?;
        if status.success() {
            return Ok(());
        }
        Err(anyhow!("kill exited with status {}.", status))
    }
}

#[cfg(not(target_os = "macos"))]
fn parse_process_id(raw: &str) -> Option<u32> {
    raw.trim().parse::<u32>().ok().filter(|value| *value > 0)
}

fn resolve_tray_binary() -> Result<PathBuf> {
    let mut candidates = Vec::new();
    if let Some(value) = env::var_os("CODEX_ROTATE_TRAY_BIN") {
        candidates.push(PathBuf::from(value));
    }

    if let Ok(current_exe) = env::current_exe() {
        if let Some(parent) = current_exe.parent() {
            candidates.push(parent.join(tray_binary_name()));
        }
    }

    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("..")
        .join("..");
    candidates.push(
        repo_root
            .join("target")
            .join("debug")
            .join(tray_binary_name()),
    );
    candidates.push(
        repo_root
            .join("target")
            .join("release")
            .join(tray_binary_name()),
    );

    for candidate in candidates {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    Err(anyhow!(
        "Unable to find the codex-rotate tray binary. Set CODEX_ROTATE_TRAY_BIN to override."
    ))
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
