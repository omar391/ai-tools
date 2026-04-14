use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use codex_rotate_core::paths::resolve_paths;

use crate::process::spawn_detached_process;

#[cfg(target_os = "macos")]
pub const MACOS_TRAY_LAUNCHD_LABEL: &str = "com.astronlab.codex-rotate.tray";

pub fn launch_tray_process(tray_binary: &Path) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        let label = tray_launchd_label();
        let plist_path = write_tray_launch_agent_plist(tray_binary)?;
        bootstrap_tray_launch_agent_after_reset(
            &plist_path,
            "Failed to bootstrap Codex Rotate tray launch agent after reset",
        )?;
        kickstart_tray_launch_agent(&label, "Failed to start Codex Rotate tray launch agent.")?;
        return Ok(());
    }

    #[cfg(not(target_os = "macos"))]
    {
        spawn_detached_process(tray_binary, &[])
    }
}

pub fn ensure_tray_process_registered() -> Result<bool> {
    #[cfg(target_os = "macos")]
    {
        let plist_path = tray_launch_agent_plist_path()?;
        if !plist_path.is_file() {
            return Ok(false);
        }

        let label = tray_launchd_label();
        let registered = launchctl_service_is_registered(&label)?;
        let running = tray_service_pid()?.is_some();
        if !registered_tray_service_requires_restart(registered, running) {
            return Ok(false);
        }

        if !registered {
            bootstrap_tray_launch_agent_after_reset(
                &plist_path,
                "Failed to restore Codex Rotate tray launch agent after reset",
            )?;
        }
        kickstart_tray_launch_agent(
            &label,
            "Failed to kickstart Codex Rotate tray launch agent.",
        )?;
        return Ok(true);
    }

    #[cfg(not(target_os = "macos"))]
    {
        Ok(false)
    }
}

fn registered_tray_service_requires_restart(registered: bool, running: bool) -> bool {
    !registered || !running
}

pub fn schedule_tray_relaunch_process(tray_binary: &Path) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        let label = tray_launchd_label();
        let plist_path = write_tray_launch_agent_plist(tray_binary)?;
        let script = build_tray_launch_agent_reset_script(&plist_path, &label);
        return spawn_detached_process(Path::new("/bin/sh"), &["-c", script.as_str()]);
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let script = format!("sleep 1; exec {}", shell_single_quote(tray_binary));
        return spawn_detached_process(Path::new("/bin/sh"), &["-c", script.as_str()]);
    }

    #[cfg(not(unix))]
    {
        spawn_detached_process(tray_binary, &[])
    }
}

pub fn clear_tray_service_registration() {
    #[cfg(target_os = "macos")]
    {
        let label = tray_launchd_label();
        if let Ok(plist_path) = tray_launch_agent_plist_path() {
            let _ = launchctl_bootout_plist_quiet(&plist_path);
            let _ = fs::remove_file(plist_path);
        }
        let _ = launchctl_remove_label_quiet(&label);
    }
}

#[cfg(target_os = "macos")]
pub fn tray_service_pid() -> Result<Option<u32>> {
    launchctl_service_pid(&tray_launchd_label())
}

#[cfg(not(target_os = "macos"))]
pub fn tray_service_pid() -> Result<Option<u32>> {
    Ok(None)
}

#[cfg(target_os = "macos")]
fn tray_launch_agent_plist_path() -> Result<PathBuf> {
    Ok(resolve_paths()?.rotate_home.join("tray.launchd.plist"))
}

#[cfg(target_os = "macos")]
fn write_tray_launch_agent_plist(tray_binary: &Path) -> Result<PathBuf> {
    let plist_path = tray_launch_agent_plist_path()?;
    if let Some(parent) = plist_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    fs::write(&plist_path, tray_launch_agent_plist_contents(tray_binary))
        .with_context(|| format!("Failed to write {}.", plist_path.display()))?;
    Ok(plist_path)
}

#[cfg(target_os = "macos")]
fn tray_launch_agent_plist_contents(tray_binary: &Path) -> String {
    let label = tray_launchd_label();
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>"#,
    );
    xml.push_str(&xml_escape(&label));
    xml.push_str(
        r#"</string>
  <key>ProgramArguments</key>
  <array>
    <string>"#,
    );
    xml.push_str(&xml_escape(&tray_binary.display().to_string()));
    xml.push_str(
        r#"</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ProcessType</key>
  <string>Interactive</string>
"#,
    );
    let env_vars = launch_agent_environment_variables();
    if !env_vars.is_empty() {
        xml.push_str("  <key>EnvironmentVariables</key>\n  <dict>\n");
        for (key, value) in env_vars {
            xml.push_str("    <key>");
            xml.push_str(&xml_escape(&key));
            xml.push_str("</key>\n    <string>");
            xml.push_str(&xml_escape(&value));
            xml.push_str("</string>\n");
        }
        xml.push_str("  </dict>\n");
    }
    xml.push_str("</dict>\n</plist>\n");
    xml
}

#[cfg(target_os = "macos")]
fn launch_agent_environment_variables() -> Vec<(String, String)> {
    [
        "CODEX_ROTATE_HOME",
        "CODEX_ROTATE_CLI_BIN",
        "CODEX_ROTATE_TRAY_BIN",
        "CODEX_ROTATE_DEBUG_PORT",
        "PATH",
    ]
    .iter()
    .filter_map(|key| {
        std::env::var_os(key).map(|value| (key.to_string(), value.to_string_lossy().to_string()))
    })
    .collect()
}

#[cfg(target_os = "macos")]
fn launchctl_bootstrap_plist(plist_path: &Path) -> Result<()> {
    let output = launchctl_output([
        "bootstrap",
        &launchctl_user_domain(),
        &plist_path.display().to_string(),
    ])?;
    if output.status.success() {
        return Ok(());
    }
    Err(anyhow!(
        "{}",
        format_launchctl_failure("bootstrap", &output, Some(plist_path))
    ))
}

#[cfg(target_os = "macos")]
fn launchctl_kickstart_label(label: &str) -> Result<()> {
    let service = launchctl_service_target(label);
    let output = launchctl_output(["kickstart", "-k", &service])?;
    if output.status.success() {
        return Ok(());
    }
    Err(anyhow!(
        "{}",
        format_launchctl_failure("kickstart", &output, None)
    ))
}

#[cfg(target_os = "macos")]
fn launchctl_service_is_registered(label: &str) -> Result<bool> {
    let service = launchctl_service_target(label);
    let output = launchctl_output(["print", &service])?;
    if output.status.success() {
        return Ok(true);
    }
    if launchctl_output_is_absent_service(&output) {
        return Ok(false);
    }
    Err(anyhow!(
        "{}",
        format_launchctl_failure("print", &output, None)
    ))
}

#[cfg(target_os = "macos")]
fn launchctl_service_pid(label: &str) -> Result<Option<u32>> {
    let service = launchctl_service_target(label);
    let output = launchctl_output(["print", &service])?;
    if !output.status.success() {
        if launchctl_output_is_absent_service(&output) {
            return Ok(None);
        }
        return Err(anyhow!(
            "{}",
            format_launchctl_failure("print", &output, None)
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.lines().find_map(parse_launchctl_service_pid))
}

#[cfg(target_os = "macos")]
fn bootstrap_tray_launch_agent_after_reset(plist_path: &Path, message: &str) -> Result<()> {
    let label = tray_launchd_label();
    launchctl_bootout_plist_quiet(plist_path).ok();
    launchctl_remove_label_quiet(&label).ok();
    let mut last_error = None;
    for _ in 0..5 {
        match launchctl_bootstrap_plist(plist_path) {
            Ok(()) => return Ok(()),
            Err(error) => {
                last_error = Some(error);
                std::thread::sleep(Duration::from_millis(250));
                launchctl_bootout_plist_quiet(plist_path).ok();
                launchctl_remove_label_quiet(&label).ok();
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("Failed to bootstrap tray launch agent.")))
        .with_context(|| message.to_string())
}

#[cfg(target_os = "macos")]
fn kickstart_tray_launch_agent(label: &str, message: &str) -> Result<()> {
    launchctl_kickstart_label(label).with_context(|| message.to_string())
}

#[cfg(target_os = "macos")]
fn launchctl_output<const N: usize>(args: [&str; N]) -> Result<Output> {
    Command::new("launchctl")
        .args(args)
        .output()
        .context("Failed to invoke launchctl.")
}

#[cfg(target_os = "macos")]
fn launchctl_bootout_plist_quiet(plist_path: &Path) -> Result<()> {
    let output = Command::new("launchctl")
        .arg("bootout")
        .arg(launchctl_user_domain())
        .arg(plist_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .context("Failed to invoke launchctl bootout.")?;
    if output.status.success() || launchctl_output_is_absent_service(&output) {
        return Ok(());
    }
    Err(anyhow!(
        "{}",
        format_launchctl_failure("bootout", &output, Some(plist_path))
    ))
}

#[cfg(target_os = "macos")]
fn launchctl_remove_label_quiet(label: &str) -> Result<()> {
    let output = Command::new("launchctl")
        .arg("remove")
        .arg(label)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .context("Failed to invoke launchctl remove.")?;
    if output.status.success() || launchctl_output_is_absent_service(&output) {
        return Ok(());
    }
    Err(anyhow!(
        "{}",
        format_launchctl_failure("remove", &output, None)
    ))
}

#[cfg(target_os = "macos")]
fn launchctl_output_is_absent_service(output: &Output) -> bool {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    [stderr.as_ref(), stdout.as_ref()].iter().any(|text| {
        text.contains("Could not find service")
            || text.contains("No such process")
            || text.contains("service cannot load in requested session")
    })
}

#[cfg(target_os = "macos")]
fn format_launchctl_failure(action: &str, output: &Output, plist_path: Option<&Path>) -> String {
    let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
    match plist_path {
        Some(path) if !detail.is_empty() => format!(
            "launchctl {action} exited with status {} for {}: {}",
            output.status,
            path.display(),
            detail
        ),
        Some(path) => format!(
            "launchctl {action} exited with status {} for {}.",
            output.status,
            path.display()
        ),
        None if !detail.is_empty() => {
            format!(
                "launchctl {action} exited with status {}: {}",
                output.status, detail
            )
        }
        None => format!("launchctl {action} exited with status {}.", output.status),
    }
}

#[cfg(target_os = "macos")]
fn launchctl_user_domain() -> String {
    format!("gui/{}", effective_user_id())
}

#[cfg(target_os = "macos")]
fn launchctl_service_target(label: &str) -> String {
    format!("{}/{}", launchctl_user_domain(), label)
}

#[cfg(target_os = "macos")]
fn tray_launchd_label() -> String {
    default_tray_launchd_label().unwrap_or_else(|_| MACOS_TRAY_LAUNCHD_LABEL.to_string())
}

#[cfg(target_os = "macos")]
fn default_tray_launchd_label() -> Result<String> {
    let rotate_home = resolve_paths()?.rotate_home;
    Ok(format!(
        "{MACOS_TRAY_LAUNCHD_LABEL}.{}",
        stable_label_suffix(&rotate_home)
    ))
}

#[cfg(target_os = "macos")]
fn effective_user_id() -> u32 {
    std::env::var("UID")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .or_else(|| {
            Command::new("id")
                .arg("-u")
                .output()
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| {
                    String::from_utf8_lossy(&output.stdout)
                        .trim()
                        .parse::<u32>()
                        .ok()
                })
        })
        .unwrap_or(0)
}

#[cfg(any(unix, target_os = "macos"))]
fn shell_single_quote(path: &Path) -> String {
    shell_single_quote_string(&path.display().to_string())
}

#[cfg(any(unix, target_os = "macos"))]
fn shell_single_quote_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[cfg(target_os = "macos")]
fn build_tray_launch_agent_reset_script(plist_path: &Path, label: &str) -> String {
    format!(
        "sleep 1; \
launchctl bootout {domain} {plist} >/dev/null 2>&1 || true; \
launchctl remove {label} >/dev/null 2>&1 || true; \
i=0; \
while [ $i -lt 5 ]; do \
  launchctl bootstrap {domain} {plist} >/dev/null 2>&1 && \
  launchctl kickstart -k {service} >/dev/null 2>&1 && exit 0; \
  i=$((i + 1)); \
  sleep 1; \
  launchctl bootout {domain} {plist} >/dev/null 2>&1 || true; \
  launchctl remove {label} >/dev/null 2>&1 || true; \
done; \
exit 1",
        domain = shell_single_quote_string(&launchctl_user_domain()),
        plist = shell_single_quote(plist_path),
        label = shell_single_quote_string(label),
        service = shell_single_quote_string(&launchctl_service_target(label)),
    )
}

#[cfg(target_os = "macos")]
fn stable_label_suffix(path: &Path) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in path.to_string_lossy().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[cfg(target_os = "macos")]
fn parse_launchctl_service_pid(line: &str) -> Option<u32> {
    let value = line.trim().strip_prefix("pid = ")?;
    value.trim().parse::<u32>().ok()
}

#[cfg(target_os = "macos")]
fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn env_mutex() -> &'static Mutex<()> {
        static ENV_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_MUTEX.get_or_init(|| Mutex::new(()))
    }

    fn restore_var(name: &str, value: Option<OsString>) {
        match value {
            Some(value) => unsafe { std::env::set_var(name, value) },
            None => unsafe { std::env::remove_var(name) },
        }
    }

    #[test]
    fn registered_tray_service_requires_running_pid() {
        assert!(!registered_tray_service_requires_restart(true, true));
        assert!(registered_tray_service_requires_restart(true, false));
        assert!(registered_tray_service_requires_restart(false, false));
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{stamp}"))
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn tray_launch_agent_plist_enables_keepalive() {
        let _guard = env_mutex().lock().expect("env mutex");
        let previous_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_debug_port = std::env::var_os("CODEX_ROTATE_DEBUG_PORT");
        let fake_home = unique_temp_dir("codex-rotate-tray-agent");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &fake_home);
            std::env::set_var("CODEX_ROTATE_DEBUG_PORT", "9333");
        }

        let plist = tray_launch_agent_plist_contents(Path::new("/tmp/codex-rotate-tray"));
        assert!(plist.contains("<key>KeepAlive</key>"));
        assert!(plist.contains("<true/>"));
        assert!(plist.contains(MACOS_TRAY_LAUNCHD_LABEL));
        assert!(plist.contains("CODEX_ROTATE_DEBUG_PORT"));

        restore_var("CODEX_ROTATE_HOME", previous_home);
        restore_var("CODEX_ROTATE_DEBUG_PORT", previous_debug_port);
        fs::remove_dir_all(&fake_home).ok();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn launchctl_absent_service_output_is_treated_as_benign() {
        let output = Output {
            status: exit_status_from_code(5),
            stdout: Vec::new(),
            stderr: b"Could not find service \"com.astronlab.codex-rotate.tray\" in domain for user gui: 501\n".to_vec(),
        };
        assert!(launchctl_output_is_absent_service(&output));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn ensure_tray_process_registered_skips_when_no_plist_marker_exists() {
        let _guard = env_mutex().lock().expect("env mutex");
        let previous_home = std::env::var_os("CODEX_ROTATE_HOME");
        let fake_home = unique_temp_dir("codex-rotate-tray-supervisor");
        fs::create_dir_all(&fake_home).expect("create fake home");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &fake_home);
        }

        let restored = ensure_tray_process_registered().expect("ensure tray registration");
        assert!(!restored);

        restore_var("CODEX_ROTATE_HOME", previous_home);
        fs::remove_dir_all(&fake_home).ok();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn tray_launchd_label_is_scoped_to_rotate_home() {
        let _guard = env_mutex().lock().expect("env mutex");
        let previous_home = std::env::var_os("CODEX_ROTATE_HOME");
        let first_home = unique_temp_dir("codex-rotate-tray-home-a");
        let second_home = unique_temp_dir("codex-rotate-tray-home-b");
        fs::create_dir_all(&first_home).expect("create first fake home");
        fs::create_dir_all(&second_home).expect("create second fake home");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &first_home);
        }
        let first_label = tray_launchd_label();

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &second_home);
        }
        let second_label = tray_launchd_label();

        assert!(first_label.starts_with(MACOS_TRAY_LAUNCHD_LABEL));
        assert!(second_label.starts_with(MACOS_TRAY_LAUNCHD_LABEL));
        assert_ne!(first_label, second_label);

        restore_var("CODEX_ROTATE_HOME", previous_home);
        fs::remove_dir_all(&first_home).ok();
        fs::remove_dir_all(&second_home).ok();
    }

    #[cfg(target_os = "macos")]
    fn exit_status_from_code(code: i32) -> std::process::ExitStatus {
        use std::os::unix::process::ExitStatusExt;
        std::process::ExitStatus::from_raw(code << 8)
    }
}
