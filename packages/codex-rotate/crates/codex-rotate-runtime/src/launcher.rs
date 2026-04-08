use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};

use crate::cdp::is_cdp_ready;
use crate::paths::resolve_paths;

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

    if is_cdp_ready(port) {
        return Ok(());
    }

    if managed_launch_disabled() {
        return Err(anyhow!(
            "Managed Codex launch is disabled by CODEX_ROTATE_DISABLE_MANAGED_LAUNCH."
        ));
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
        if is_cdp_ready(port) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(500));
    }

    Err(anyhow!(
        "Codex did not expose a remote debugging target on port {}.",
        port
    ))
}

fn managed_launch_disabled() -> bool {
    std::env::var("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
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
