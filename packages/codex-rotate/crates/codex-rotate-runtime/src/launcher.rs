use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, Once, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};

use crate::cdp::is_cdp_page_ready;
use crate::log_isolation::managed_codex_root_pids;
use crate::paths::resolve_paths;
use crate::runtime_log::log_daemon_info;

const DISABLE_MANAGED_LAUNCH_ENV: &str = "CODEX_ROTATE_DISABLE_MANAGED_LAUNCH";
const DIRECT_MANAGED_LAUNCH_ENV: &str = "CODEX_ROTATE_MANAGED_LAUNCH_DIRECT";
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

    if managed_codex_instance_ready(port, profile_dir)? {
        log_daemon_info(format!(
            "Managed Codex instance already ready for profile {} on port {}.",
            profile_dir.display(),
            port
        ));
        return Ok(());
    }

    if let Some(reason) = managed_launch_disabled_reason() {
        return Err(anyhow!(reason));
    }

    std::fs::create_dir_all(&paths.rotate_home)
        .with_context(|| format!("Failed to create {}.", paths.rotate_home.display()))?;
    std::fs::create_dir_all(profile_dir)
        .with_context(|| format!("Failed to create {}.", profile_dir.display()))?;
    log_daemon_info(format!(
        "Relaunching managed Codex for profile {} on port {}.",
        profile_dir.display(),
        port
    ));

    #[cfg(target_os = "macos")]
    launch_managed_codex(app_path, port, profile_dir)?;

    #[cfg(not(target_os = "macos"))]
    {
        return Err(anyhow!(
            "Managed Codex launch is currently only supported on macOS."
        ));
    }

    track_test_managed_launch(profile_dir);

    let deadline = Instant::now() + Duration::from_millis(wait_ms.unwrap_or(15_000));
    while Instant::now() < deadline {
        if managed_codex_instance_ready(port, profile_dir)? {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(500));
    }

    Err(anyhow!(
        "Codex did not expose a managed remote debugging target on port {} for profile {}.",
        port,
        profile_dir.display()
    ))
}

fn managed_codex_instance_ready(port: u16, profile_dir: &Path) -> Result<bool> {
    let has_cdp_page = is_cdp_page_ready(port);
    let has_matching_profile_process = !managed_codex_root_pids(profile_dir)?.is_empty();
    Ok(managed_codex_instance_ready_state(
        has_cdp_page,
        has_matching_profile_process,
    ))
}

fn managed_codex_instance_ready_state(
    has_cdp_page: bool,
    has_matching_profile_process: bool,
) -> bool {
    has_cdp_page && has_matching_profile_process
}

#[cfg(target_os = "macos")]
fn launch_managed_codex(app_path: &str, port: u16, profile_dir: &Path) -> Result<()> {
    let args = managed_codex_launch_args(port, profile_dir);
    if env_flag_enabled(DIRECT_MANAGED_LAUNCH_ENV) {
        let executable = resolve_macos_app_executable(Path::new(app_path));
        let child = Command::new(&executable)
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("Failed to launch Codex from {}.", executable.display()))?;
        track_managed_launch_child(profile_dir, child);
        return Ok(());
    }

    let output = Command::new("open")
        .arg("-na")
        .arg(app_path)
        .arg("--args")
        .args(&args)
        .output()
        .with_context(|| format!("Failed to launch Codex from {}.", app_path))?;
    if output.status.success() {
        return Ok(());
    }

    Err(anyhow!(
        "{}",
        String::from_utf8_lossy(&output.stderr)
            .trim()
            .to_string()
            .if_empty_then(|| format!("Failed to launch Codex from {}.", app_path))
    ))
}

#[cfg(target_os = "macos")]
fn resolve_macos_app_executable(app_path: &Path) -> PathBuf {
    if app_path.extension().and_then(|value| value.to_str()) != Some("app") {
        return app_path.to_path_buf();
    }

    let bundle_name = app_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("Codex");
    app_path.join("Contents").join("MacOS").join(bundle_name)
}

#[cfg(target_os = "macos")]
fn managed_codex_launch_args(port: u16, profile_dir: &Path) -> Vec<String> {
    let mut args = vec![
        format!("--user-data-dir={}", profile_dir.display()),
        format!("--remote-debugging-port={}", port),
    ];
    if managed_launch_allowed_in_tests() {
        args.push("--use-mock-keychain".to_string());
    }
    args
}

fn managed_launch_disabled_reason() -> Option<&'static str> {
    if env_flag_enabled(DISABLE_MANAGED_LAUNCH_ENV) {
        return Some("Managed Codex launch is disabled by CODEX_ROTATE_DISABLE_MANAGED_LAUNCH.");
    }
    if running_under_test_harness() && std::env::var_os(DISABLE_MANAGED_LAUNCH_ENV).is_none() {
        return Some(
            "Managed Codex launch is disabled while running tests. Set CODEX_ROTATE_DISABLE_MANAGED_LAUNCH=0 to opt in.",
        );
    }
    None
}

fn managed_launch_allowed_in_tests() -> bool {
    std::env::var_os(DISABLE_MANAGED_LAUNCH_ENV).is_some()
        && !env_flag_enabled(DISABLE_MANAGED_LAUNCH_ENV)
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
    managed_launch_allowed_in_tests()
}

#[cfg(target_os = "macos")]
struct TrackedManagedChild {
    profile_dir: PathBuf,
    child: Child,
}

#[cfg(target_os = "macos")]
fn tracked_managed_launch_children() -> &'static Mutex<Vec<TrackedManagedChild>> {
    static TRACKED: OnceLock<Mutex<Vec<TrackedManagedChild>>> = OnceLock::new();
    TRACKED.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(target_os = "macos")]
fn track_managed_launch_child(profile_dir: &Path, child: Child) {
    let tracked = tracked_managed_launch_children();
    let mut children = tracked.lock().unwrap_or_else(|error| error.into_inner());
    children.retain(|entry| entry.profile_dir != profile_dir);
    children.push(TrackedManagedChild {
        profile_dir: profile_dir.to_path_buf(),
        child,
    });
}

#[cfg(not(target_os = "macos"))]
pub fn reap_tracked_managed_launch_children() -> Result<()> {
    Ok(())
}

#[cfg(target_os = "macos")]
pub fn reap_tracked_managed_launch_children() -> Result<()> {
    let tracked = tracked_managed_launch_children();
    let mut children = tracked.lock().unwrap_or_else(|error| error.into_inner());
    let mut index = 0;
    while index < children.len() {
        let profile_display = children[index].profile_dir.display().to_string();
        match children[index].child.try_wait().with_context(|| {
            format!(
                "Failed to check managed Codex child for profile {}.",
                profile_display
            )
        })? {
            Some(_) => {
                children.remove(index);
            }
            None => {
                index += 1;
            }
        }
    }
    Ok(())
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
        let root_pids = managed_codex_root_pids(&profile_dir)?;
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
        reap_tracked_managed_launch_children()?;
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
        unsafe {
            std::env::set_var("RUST_TEST_THREADS", "1");
            std::env::remove_var(DISABLE_MANAGED_LAUNCH_ENV);
        }

        let reason = managed_launch_disabled_reason().expect("managed launch should be blocked");
        assert!(reason.contains("running tests"));

        restore_env("RUST_TEST_THREADS", previous_test_threads);
        restore_env(DISABLE_MANAGED_LAUNCH_ENV, previous_disable);
    }

    #[test]
    fn managed_launch_can_be_opted_in_during_tests() {
        let _env_guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let previous_test_threads = std::env::var_os("RUST_TEST_THREADS");
        let previous_disable = std::env::var_os(DISABLE_MANAGED_LAUNCH_ENV);
        unsafe {
            std::env::set_var("RUST_TEST_THREADS", "1");
            std::env::set_var(DISABLE_MANAGED_LAUNCH_ENV, "0");
        }

        assert!(managed_launch_disabled_reason().is_none());
        assert!(should_track_test_managed_launches());

        restore_env("RUST_TEST_THREADS", previous_test_threads);
        restore_env(DISABLE_MANAGED_LAUNCH_ENV, previous_disable);
    }

    #[test]
    fn explicit_disable_env_overrides_test_opt_in() {
        let _env_guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let previous_test_threads = std::env::var_os("RUST_TEST_THREADS");
        let previous_disable = std::env::var_os(DISABLE_MANAGED_LAUNCH_ENV);
        unsafe {
            std::env::set_var("RUST_TEST_THREADS", "1");
            std::env::set_var(DISABLE_MANAGED_LAUNCH_ENV, "1");
        }

        let reason = managed_launch_disabled_reason().expect("disable env should win");
        assert!(reason.contains(DISABLE_MANAGED_LAUNCH_ENV));

        restore_env("RUST_TEST_THREADS", previous_test_threads);
        restore_env(DISABLE_MANAGED_LAUNCH_ENV, previous_disable);
    }

    #[test]
    fn managed_launch_tracking_is_off_without_test_opt_in() {
        let _env_guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let previous_test_threads = std::env::var_os("RUST_TEST_THREADS");
        let previous_disable = std::env::var_os(DISABLE_MANAGED_LAUNCH_ENV);
        unsafe {
            std::env::set_var("RUST_TEST_THREADS", "1");
            std::env::remove_var(DISABLE_MANAGED_LAUNCH_ENV);
        }

        assert!(!should_track_test_managed_launches());

        restore_env("RUST_TEST_THREADS", previous_test_threads);
        restore_env(DISABLE_MANAGED_LAUNCH_ENV, previous_disable);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn direct_managed_launch_resolves_bundle_executable() {
        assert_eq!(
            resolve_macos_app_executable(Path::new("/Applications/Codex.app")),
            PathBuf::from("/Applications/Codex.app/Contents/MacOS/Codex")
        );
        assert_eq!(
            resolve_macos_app_executable(Path::new("/tmp/custom-codex")),
            PathBuf::from("/tmp/custom-codex")
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn managed_launch_args_add_mock_keychain_during_test_opt_in() {
        let _env_guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let previous_test_threads = std::env::var_os("RUST_TEST_THREADS");
        let previous_disable = std::env::var_os(DISABLE_MANAGED_LAUNCH_ENV);
        unsafe {
            std::env::set_var("RUST_TEST_THREADS", "1");
            std::env::set_var(DISABLE_MANAGED_LAUNCH_ENV, "0");
        }

        let args = managed_codex_launch_args(9333, Path::new("/tmp/profile"));
        assert!(args.iter().any(|arg| arg == "--use-mock-keychain"));

        restore_env("RUST_TEST_THREADS", previous_test_threads);
        restore_env(DISABLE_MANAGED_LAUNCH_ENV, previous_disable);
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

    #[test]
    fn managed_instance_ready_state_requires_cdp_and_profile_process() {
        assert!(!managed_codex_instance_ready_state(false, false));
        assert!(!managed_codex_instance_ready_state(true, false));
        assert!(!managed_codex_instance_ready_state(false, true));
        assert!(managed_codex_instance_ready_state(true, true));
    }
}
