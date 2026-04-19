use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, Once, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};

use crate::cdp::is_cdp_page_ready;
use crate::paths::resolve_paths;

const DISABLE_MANAGED_LAUNCH_ENV: &str = "CODEX_ROTATE_DISABLE_MANAGED_LAUNCH";
const ENABLE_MANAGED_LAUNCH_IN_TESTS_ENV: &str = "CODEX_ROTATE_ENABLE_MANAGED_LAUNCH_IN_TESTS";
const PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(8);
const PROCESS_STOP_POLL_INTERVAL: Duration = Duration::from_millis(200);

pub fn ensure_debug_codex_instance(
    app_path: Option<&str>,
    port: Option<u16>,
    profile_dir: Option<&Path>,
    wait_ms: Option<u64>,
) -> Result<()> {
    let paths = resolve_paths()?;
    let app_path = app_path.unwrap_or("/Applications/Codex.app");
    let port = port.unwrap_or(9333);
    let profile_dir = profile_dir.unwrap_or(&paths.debug_profile_dir);

    if is_cdp_page_ready(port) {
        return Ok(());
    }

    if let Some(reason) = managed_launch_disabled_reason() {
        return Err(anyhow!(reason));
    }

    std::fs::create_dir_all(&paths.rotate_home)
        .with_context(|| format!("Failed to create {}.", paths.rotate_home.display()))?;
    std::fs::create_dir_all(profile_dir)
        .with_context(|| format!("Failed to create {}.", profile_dir.display()))?;

    #[cfg(target_os = "macos")]
    let output = Command::new("open")
        .arg("-na")
        .arg(app_path)
        .arg("--args")
        .arg(format!("--user-data-dir={}", profile_dir.display()))
        .arg(format!("--remote-debugging-port={}", port))
        .output()
        .with_context(|| format!("Failed to launch Codex from {}.", app_path))?;

    #[cfg(not(target_os = "macos"))]
    let output = {
        return Err(anyhow!(
            "Managed Codex launch is currently only supported on macOS."
        ));
    };

    if !output.status.success() {
        return Err(anyhow!(
            "{}",
            String::from_utf8_lossy(&output.stderr)
                .trim()
                .to_string()
                .if_empty_then(|| format!("Failed to launch Codex from {}.", app_path))
        ));
    }

    track_test_managed_launch(profile_dir);

    let deadline = Instant::now() + Duration::from_millis(wait_ms.unwrap_or(15_000));
    while Instant::now() < deadline {
        if is_cdp_page_ready(port) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(500));
    }

    Err(anyhow!(
        "Codex did not expose a remote debugging target on port {}.",
        port
    ))
}

fn managed_launch_disabled_reason() -> Option<&'static str> {
    if env_flag_enabled(DISABLE_MANAGED_LAUNCH_ENV) {
        return Some("Managed Codex launch is disabled by CODEX_ROTATE_DISABLE_MANAGED_LAUNCH.");
    }
    if running_under_test_harness() && !managed_launch_allowed_in_tests() {
        return Some(
            "Managed Codex launch is disabled while running tests. Set CODEX_ROTATE_ENABLE_MANAGED_LAUNCH_IN_TESTS=1 to opt in.",
        );
    }
    None
}

fn managed_launch_allowed_in_tests() -> bool {
    env_flag_enabled(ENABLE_MANAGED_LAUNCH_IN_TESTS_ENV)
}

fn env_flag_enabled(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn running_under_test_harness() -> bool {
    if std::env::var_os("RUST_TEST_THREADS").is_some()
        || std::env::var_os("NEXTEST").is_some()
        || std::env::var_os("CARGO_TARGET_TMPDIR").is_some()
    {
        return true;
    }

    if std::env::args().any(|arg| {
        arg == "--nocapture"
            || arg == "--show-output"
            || arg.starts_with("--test-threads=")
            || arg.starts_with("--exact")
    }) {
        return true;
    }

    std::env::current_exe()
        .ok()
        .map(|path| path_looks_like_rust_test_binary(&path))
        .unwrap_or(false)
}

fn path_looks_like_rust_test_binary(path: &Path) -> bool {
    let value = path.to_string_lossy();
    value.contains("/target/debug/deps/")
        || value.contains("/target/release/deps/")
        || value.contains("/.worktree-target/debug/deps/")
        || value.contains("/.worktree-target/release/deps/")
        || value.contains("\\target\\debug\\deps\\")
        || value.contains("\\target\\release\\deps\\")
}

fn should_track_test_managed_launches() -> bool {
    running_under_test_harness() && managed_launch_allowed_in_tests()
}

fn track_test_managed_launch(profile_dir: &Path) {
    #[cfg(not(target_os = "macos"))]
    {
        let _ = profile_dir;
    }

    #[cfg(target_os = "macos")]
    {
        if !should_track_test_managed_launches() {
            return;
        }
        let tracked = tracked_test_launch_profiles();
        let mut profiles = tracked.lock().unwrap_or_else(|error| error.into_inner());
        if !profiles.iter().any(|existing| existing == profile_dir) {
            profiles.push(profile_dir.to_path_buf());
        }
        register_test_managed_launch_cleanup_once();
    }
}

#[cfg(target_os = "macos")]
fn tracked_test_launch_profiles() -> &'static Mutex<Vec<PathBuf>> {
    static TRACKED: OnceLock<Mutex<Vec<PathBuf>>> = OnceLock::new();
    TRACKED.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(target_os = "macos")]
fn register_test_managed_launch_cleanup_once() {
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        let _ = unsafe { atexit(cleanup_tracked_test_managed_launches_at_exit) };
    });
}

#[cfg(target_os = "macos")]
extern "C" fn cleanup_tracked_test_managed_launches_at_exit() {
    let _ = cleanup_tracked_test_managed_launches();
}

#[cfg(target_os = "macos")]
fn cleanup_tracked_test_managed_launches() -> Result<()> {
    let profiles = tracked_test_launch_profiles()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();

    for profile_dir in profiles {
        let root_pids = managed_codex_root_pids_for_profile(&profile_dir)?;
        if root_pids.is_empty() {
            continue;
        }
        signal_processes("TERM", &root_pids)?;
        if !wait_for_processes_to_exit(&root_pids, PROCESS_STOP_TIMEOUT)? {
            signal_processes("KILL", &root_pids)?;
            let _ = wait_for_processes_to_exit(&root_pids, PROCESS_STOP_TIMEOUT)?;
        }
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn managed_codex_root_pids_for_profile(profile_dir: &Path) -> Result<Vec<u32>> {
    let profile_marker = format!("--user-data-dir={}", profile_dir.display());
    Ok(list_processes()?
        .into_iter()
        .filter(|process| {
            process
                .command
                .contains("/Applications/Codex.app/Contents/MacOS/Codex")
                && process.command.contains(&profile_marker)
        })
        .map(|process| process.pid)
        .collect())
}

#[cfg(target_os = "macos")]
fn signal_processes(signal: &str, pids: &[u32]) -> Result<()> {
    if pids.is_empty() {
        return Ok(());
    }
    let mut command = Command::new("kill");
    command.arg(format!("-{signal}"));
    for pid in pids {
        command.arg(pid.to_string());
    }
    let output = command.output().context("Failed to invoke kill.")?;
    if output.status.success() {
        return Ok(());
    }
    Err(anyhow!(
        "Failed to signal pids {} with {signal}: {}",
        pids.iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(", "),
        String::from_utf8_lossy(&output.stderr).trim()
    ))
}

#[cfg(target_os = "macos")]
fn wait_for_processes_to_exit(pids: &[u32], timeout: Duration) -> Result<bool> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let running = list_processes()?
            .into_iter()
            .any(|process| pids.contains(&process.pid));
        if !running {
            return Ok(true);
        }
        thread::sleep(PROCESS_STOP_POLL_INTERVAL);
    }
    Ok(false)
}

#[cfg(target_os = "macos")]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ProcessInfo {
    pid: u32,
    command: String,
}

#[cfg(target_os = "macos")]
fn list_processes() -> Result<Vec<ProcessInfo>> {
    let output = Command::new("ps")
        .args(["-axo", "pid=,command="])
        .output()
        .context("Failed to list running processes.")?;
    if !output.status.success() {
        return Err(anyhow!(
            "Failed to list running processes: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    let mut processes = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut parts = trimmed.splitn(2, char::is_whitespace);
        let pid = match parts
            .next()
            .and_then(|value| value.trim().parse::<u32>().ok())
        {
            Some(pid) => pid,
            None => continue,
        };
        let command = parts.next().map(str::trim).unwrap_or_default().to_string();
        if command.is_empty() {
            continue;
        }
        processes.push(ProcessInfo { pid, command });
    }
    Ok(processes)
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn atexit(callback: extern "C" fn()) -> i32;
}

trait IfEmptyThen {
    fn if_empty_then<F>(self, fallback: F) -> String
    where
        F: FnOnce() -> String;
}

impl IfEmptyThen for String {
    fn if_empty_then<F>(self, fallback: F) -> String
    where
        F: FnOnce() -> String,
    {
        if self.is_empty() {
            fallback()
        } else {
            self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::env_mutex;

    fn restore_env(name: &str, previous: Option<std::ffi::OsString>) {
        match previous {
            Some(value) => unsafe { std::env::set_var(name, value) },
            None => unsafe { std::env::remove_var(name) },
        }
    }

    #[test]
    fn managed_launch_is_blocked_by_default_when_running_tests() {
        let _env_guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let previous_test_threads = std::env::var_os("RUST_TEST_THREADS");
        let previous_disable = std::env::var_os(DISABLE_MANAGED_LAUNCH_ENV);
        let previous_enable_tests = std::env::var_os(ENABLE_MANAGED_LAUNCH_IN_TESTS_ENV);
        unsafe {
            std::env::set_var("RUST_TEST_THREADS", "1");
            std::env::remove_var(DISABLE_MANAGED_LAUNCH_ENV);
            std::env::remove_var(ENABLE_MANAGED_LAUNCH_IN_TESTS_ENV);
        }

        let reason = managed_launch_disabled_reason().expect("managed launch should be blocked");
        assert!(reason.contains("running tests"));

        restore_env("RUST_TEST_THREADS", previous_test_threads);
        restore_env(DISABLE_MANAGED_LAUNCH_ENV, previous_disable);
        restore_env(ENABLE_MANAGED_LAUNCH_IN_TESTS_ENV, previous_enable_tests);
    }

    #[test]
    fn managed_launch_can_be_opted_in_during_tests() {
        let _env_guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let previous_test_threads = std::env::var_os("RUST_TEST_THREADS");
        let previous_disable = std::env::var_os(DISABLE_MANAGED_LAUNCH_ENV);
        let previous_enable_tests = std::env::var_os(ENABLE_MANAGED_LAUNCH_IN_TESTS_ENV);
        unsafe {
            std::env::set_var("RUST_TEST_THREADS", "1");
            std::env::remove_var(DISABLE_MANAGED_LAUNCH_ENV);
            std::env::set_var(ENABLE_MANAGED_LAUNCH_IN_TESTS_ENV, "1");
        }

        assert!(managed_launch_disabled_reason().is_none());
        assert!(should_track_test_managed_launches());

        restore_env("RUST_TEST_THREADS", previous_test_threads);
        restore_env(DISABLE_MANAGED_LAUNCH_ENV, previous_disable);
        restore_env(ENABLE_MANAGED_LAUNCH_IN_TESTS_ENV, previous_enable_tests);
    }

    #[test]
    fn explicit_disable_env_overrides_test_opt_in() {
        let _env_guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let previous_test_threads = std::env::var_os("RUST_TEST_THREADS");
        let previous_disable = std::env::var_os(DISABLE_MANAGED_LAUNCH_ENV);
        let previous_enable_tests = std::env::var_os(ENABLE_MANAGED_LAUNCH_IN_TESTS_ENV);
        unsafe {
            std::env::set_var("RUST_TEST_THREADS", "1");
            std::env::set_var(DISABLE_MANAGED_LAUNCH_ENV, "1");
            std::env::set_var(ENABLE_MANAGED_LAUNCH_IN_TESTS_ENV, "1");
        }

        let reason = managed_launch_disabled_reason().expect("disable env should win");
        assert!(reason.contains(DISABLE_MANAGED_LAUNCH_ENV));

        restore_env("RUST_TEST_THREADS", previous_test_threads);
        restore_env(DISABLE_MANAGED_LAUNCH_ENV, previous_disable);
        restore_env(ENABLE_MANAGED_LAUNCH_IN_TESTS_ENV, previous_enable_tests);
    }

    #[test]
    fn managed_launch_tracking_is_off_without_test_opt_in() {
        let _env_guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let previous_test_threads = std::env::var_os("RUST_TEST_THREADS");
        let previous_enable_tests = std::env::var_os(ENABLE_MANAGED_LAUNCH_IN_TESTS_ENV);
        unsafe {
            std::env::set_var("RUST_TEST_THREADS", "1");
            std::env::remove_var(ENABLE_MANAGED_LAUNCH_IN_TESTS_ENV);
        }

        assert!(!should_track_test_managed_launches());

        restore_env("RUST_TEST_THREADS", previous_test_threads);
        restore_env(ENABLE_MANAGED_LAUNCH_IN_TESTS_ENV, previous_enable_tests);
    }

    #[test]
    fn detects_rust_test_binary_paths() {
        assert!(path_looks_like_rust_test_binary(Path::new(
            "/repo/target/debug/deps/codex_rotate_runtime-abc123"
        )));
        assert!(path_looks_like_rust_test_binary(Path::new(
            "/repo/.worktree-target/debug/deps/suite-abc123"
        )));
        assert!(!path_looks_like_rust_test_binary(Path::new(
            "/repo/target/debug/codex-rotate"
        )));
    }
}
