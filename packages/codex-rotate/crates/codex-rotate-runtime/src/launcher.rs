use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};

use crate::cdp::is_cdp_page_ready;
use crate::paths::resolve_paths;

const DISABLE_MANAGED_LAUNCH_ENV: &str = "CODEX_ROTATE_DISABLE_MANAGED_LAUNCH";
const ENABLE_MANAGED_LAUNCH_IN_TESTS_ENV: &str = "CODEX_ROTATE_ENABLE_MANAGED_LAUNCH_IN_TESTS";

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
    if running_under_test_harness() && !env_flag_enabled(ENABLE_MANAGED_LAUNCH_IN_TESTS_ENV) {
        return Some(
            "Managed Codex launch is disabled while running tests. Set CODEX_ROTATE_ENABLE_MANAGED_LAUNCH_IN_TESTS=1 to opt in.",
        );
    }
    None
}

fn env_flag_enabled(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn running_under_test_harness() -> bool {
    std::env::var_os("RUST_TEST_THREADS").is_some() || std::env::var_os("NEXTEST").is_some()
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
}
