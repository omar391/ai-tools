use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use anyhow::{Context, Result};
use serde_json::Value;

use crate::{IsolatedHomeFixture, IsolatedHomeGuard};

const DEFAULT_PROFILE_NAME: &str = "managed-dev-1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FakeManagedBrowserOpenerLaunchFailure {
    pub message: String,
}

#[derive(Debug)]
pub struct FakeManagedBrowserOpenerInstallGuard {
    previous_asset_root: Option<OsString>,
    previous_browser_shim_log: Option<OsString>,
    previous_fast_browser_profile: Option<OsString>,
    previous_browser: Option<OsString>,
    _home_guard: IsolatedHomeGuard,
}

#[derive(Debug)]
pub struct FakeManagedBrowserOpenerFixtureBuilder {
    home: IsolatedHomeFixture,
    profile_name: String,
    launch_failure: Option<FakeManagedBrowserOpenerLaunchFailure>,
    callback_log_path: Option<PathBuf>,
}

#[derive(Debug)]
pub struct FakeManagedBrowserOpenerFixture {
    _home: IsolatedHomeFixture,
    _install_guard: FakeManagedBrowserOpenerInstallGuard,
    asset_root: PathBuf,
    opener_path: PathBuf,
    log_path: PathBuf,
    callback_log_path: Option<PathBuf>,
    profile_name: String,
}

impl FakeManagedBrowserOpenerFixtureBuilder {
    pub fn new(prefix: impl AsRef<str>) -> Result<Self> {
        Ok(Self {
            home: IsolatedHomeFixture::new(prefix)?,
            profile_name: DEFAULT_PROFILE_NAME.to_string(),
            launch_failure: None,
            callback_log_path: None,
        })
    }

    pub fn profile_name(mut self, profile_name: impl AsRef<str>) -> Self {
        self.profile_name = profile_name.as_ref().trim().to_string();
        if self.profile_name.is_empty() {
            self.profile_name = DEFAULT_PROFILE_NAME.to_string();
        }
        self
    }

    pub fn launch_failure(mut self, message: impl AsRef<str>) -> Self {
        self.launch_failure = Some(FakeManagedBrowserOpenerLaunchFailure {
            message: message.as_ref().to_string(),
        });
        self
    }

    pub fn callback_log_path(mut self, path: impl AsRef<Path>) -> Self {
        self.callback_log_path = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn build(self) -> Result<FakeManagedBrowserOpenerFixture> {
        let asset_root = self.home.sandbox_root().join("assets");
        fs::create_dir_all(&asset_root)
            .with_context(|| format!("create {}", asset_root.display()))?;

        let log_path = self.home.sandbox_root().join("browser-shim.log.jsonl");
        fs::write(&log_path, "").with_context(|| format!("create {}", log_path.display()))?;

        if let Some(callback_log_path) = self.callback_log_path.as_ref() {
            if let Some(parent) = callback_log_path.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("create {}", parent.display()))?;
            }
            fs::write(callback_log_path, "")
                .with_context(|| format!("create {}", callback_log_path.display()))?;
        }

        let opener_path = asset_root.join("codex-login-managed-browser-opener.ts");
        fs::write(
            &opener_path,
            render_fake_managed_browser_opener_script(
                self.profile_name.as_str(),
                &opener_path,
                &log_path,
                self.callback_log_path.as_deref(),
                self.launch_failure
                    .as_ref()
                    .map(|failure| failure.message.as_str()),
            ),
        )
        .with_context(|| format!("write {}", opener_path.display()))?;
        make_executable(&opener_path)?;

        let install_guard = FakeManagedBrowserOpenerFixture::install_environment(
            &self.home,
            &asset_root,
            &opener_path,
            &log_path,
            self.profile_name.as_str(),
        );

        Ok(FakeManagedBrowserOpenerFixture {
            _home: self.home,
            _install_guard: install_guard,
            asset_root,
            opener_path,
            log_path,
            callback_log_path: self.callback_log_path,
            profile_name: self.profile_name,
        })
    }
}

impl FakeManagedBrowserOpenerFixture {
    pub fn new(prefix: impl AsRef<str>) -> Result<Self> {
        FakeManagedBrowserOpenerFixtureBuilder::new(prefix)?.build()
    }

    pub fn builder(prefix: impl AsRef<str>) -> Result<FakeManagedBrowserOpenerFixtureBuilder> {
        FakeManagedBrowserOpenerFixtureBuilder::new(prefix)
    }

    pub fn asset_root(&self) -> &Path {
        &self.asset_root
    }

    pub fn opener_path(&self) -> &Path {
        &self.opener_path
    }

    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    pub fn callback_log_path(&self) -> Option<&Path> {
        self.callback_log_path.as_deref()
    }

    pub fn profile_name(&self) -> &str {
        self.profile_name.as_str()
    }

    pub fn expected_user_data_dir(&self) -> PathBuf {
        self.asset_root
            .parent()
            .unwrap_or(self.asset_root.as_path())
            .join(".fast-browser")
            .join("profiles")
            .join(&self.profile_name)
    }

    pub fn launch(&self, url: impl AsRef<str>) -> Result<Output> {
        self.launch_with_env(url, None, None)
    }

    pub fn launch_with_profile(
        &self,
        url: impl AsRef<str>,
        profile_name: impl AsRef<str>,
    ) -> Result<Output> {
        self.launch_with_env(url, Some(profile_name.as_ref()), None)
    }

    pub fn launch_with_env(
        &self,
        url: impl AsRef<str>,
        profile_name: Option<&str>,
        browser_path: Option<&Path>,
    ) -> Result<Output> {
        let profile_name = profile_name.unwrap_or(self.profile_name.as_str());
        let browser_path = browser_path.unwrap_or(self.opener_path.as_path());

        let output = Command::new(&self.opener_path)
            .arg(url.as_ref())
            .env("CODEX_ROTATE_ASSET_ROOT", &self.asset_root)
            .env("CODEX_ROTATE_BROWSER_SHIM_LOG", &self.log_path)
            .env("FAST_BROWSER_PROFILE", profile_name)
            .env("BROWSER", browser_path)
            .output()
            .with_context(|| format!("run {}", self.opener_path.display()))?;
        Ok(output)
    }

    pub fn log_entries(&self) -> Result<Vec<Value>> {
        read_json_lines(&self.log_path)
    }

    pub fn callback_entries(&self) -> Result<Vec<Value>> {
        match self.callback_log_path.as_ref() {
            Some(path) => read_json_lines(path),
            None => Ok(Vec::new()),
        }
    }

    fn install_environment(
        home: &IsolatedHomeFixture,
        asset_root: &Path,
        opener_path: &Path,
        log_path: &Path,
        profile_name: &str,
    ) -> FakeManagedBrowserOpenerInstallGuard {
        let previous_asset_root = std::env::var_os("CODEX_ROTATE_ASSET_ROOT");
        let previous_browser_shim_log = std::env::var_os("CODEX_ROTATE_BROWSER_SHIM_LOG");
        let previous_fast_browser_profile = std::env::var_os("FAST_BROWSER_PROFILE");
        let previous_browser = std::env::var_os("BROWSER");
        let home_guard = home.install();

        unsafe {
            std::env::set_var("CODEX_ROTATE_ASSET_ROOT", asset_root);
            std::env::set_var("CODEX_ROTATE_BROWSER_SHIM_LOG", log_path);
            std::env::set_var("FAST_BROWSER_PROFILE", profile_name);
            std::env::set_var("BROWSER", opener_path);
        }

        FakeManagedBrowserOpenerInstallGuard {
            previous_asset_root,
            previous_browser_shim_log,
            previous_fast_browser_profile,
            previous_browser,
            _home_guard: home_guard,
        }
    }
}

impl Drop for FakeManagedBrowserOpenerInstallGuard {
    fn drop(&mut self) {
        restore_var("CODEX_ROTATE_ASSET_ROOT", self.previous_asset_root.take());
        restore_var(
            "CODEX_ROTATE_BROWSER_SHIM_LOG",
            self.previous_browser_shim_log.take(),
        );
        restore_var(
            "FAST_BROWSER_PROFILE",
            self.previous_fast_browser_profile.take(),
        );
        restore_var("BROWSER", self.previous_browser.take());
    }
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

fn read_json_lines(path: &Path) -> Result<Vec<Value>> {
    let raw = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    Ok(raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str::<Value>(line))
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn render_fake_managed_browser_opener_script(
    expected_profile_name: &str,
    expected_browser_path: &Path,
    log_path: &Path,
    callback_log_path: Option<&Path>,
    launch_failure: Option<&str>,
) -> String {
    let expected_profile_name = shell_single_quote(expected_profile_name);
    let expected_browser_path = shell_single_quote(&expected_browser_path.to_string_lossy());
    let log_path = shell_single_quote(&log_path.to_string_lossy());
    let callback_log_path = callback_log_path
        .map(|path| shell_single_quote(&path.to_string_lossy()))
        .unwrap_or_else(|| "''".to_string());
    let launch_failure = launch_failure
        .map(shell_single_quote)
        .unwrap_or_else(|| "''".to_string());

    let script = r#"#!/bin/sh
set -eu

EXPECTED_PROFILE_NAME=__EXPECTED_PROFILE_NAME__
EXPECTED_BROWSER_PATH=__EXPECTED_BROWSER_PATH__
LAUNCH_FAILURE=__LAUNCH_FAILURE__
CALLBACK_LOG_PATH=__CALLBACK_LOG_PATH__
LOG_PATH=__LOG_PATH__

json_escape() {
    printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

json_string() {
    printf '"%s"' "$(json_escape "$1")"
}

append_json_line() {
    printf '%s\n' "$1" >>"$LOG_PATH"
}

append_blocked_non_url() {
    profile="$1"
    browser="$2"
    append_json_line "$(printf '{"event":%s,"profile":%s,"browser":%s}' \
        "$(json_string browser_shim_blocked_non_url_open)" \
        "$(json_string "$profile")" \
        "$(json_string "$browser")")"
}

append_profile_escape() {
    profile="$1"
    expected_profile="$2"
    append_json_line "$(printf '{"event":%s,"profile":%s,"expectedProfileName":%s}' \
        "$(json_string browser_shim_profile_escape)" \
        "$(json_string "$profile")" \
        "$(json_string "$expected_profile")")"
}

append_browser_escape() {
    browser="$1"
    expected_browser="$2"
    append_json_line "$(printf '{"event":%s,"browser":%s,"expectedBrowserPath":%s}' \
        "$(json_string browser_shim_browser_escape)" \
        "$(json_string "$browser")" \
        "$(json_string "$expected_browser")")"
}

append_invoked() {
    profile="$1"
    url="$2"
    browser="$3"
    user_data_dir="$4"
    append_json_line "$(printf '{"event":%s,"profile":%s,"url":%s,"browser":%s,"userDataDir":%s}' \
        "$(json_string browser_shim_invoked)" \
        "$(json_string "$profile")" \
        "$(json_string "$url")" \
        "$(json_string "$browser")" \
        "$(json_string "$user_data_dir")")"
}

append_opened_url() {
    profile="$1"
    url="$2"
    browser="$3"
    user_data_dir="$4"
    opener_path="$5"
    append_json_line "$(printf '{"event":%s,"ok":true,"profile":%s,"url":%s,"browser":%s,"userDataDir":%s,"openerPath":%s}' \
        "$(json_string browser_shim_opened_url)" \
        "$(json_string "$profile")" \
        "$(json_string "$url")" \
        "$(json_string "$browser")" \
        "$(json_string "$user_data_dir")" \
        "$(json_string "$opener_path")")"
}

append_launch_failed() {
    profile="$1"
    url="$2"
    browser="$3"
    message="$4"
    append_json_line "$(printf '{"event":%s,"profile":%s,"url":%s,"browser":%s,"message":%s}' \
        "$(json_string browser_shim_launch_failed)" \
        "$(json_string "$profile")" \
        "$(json_string "$url")" \
        "$(json_string "$browser")" \
        "$(json_string "$message")")"
}

append_callback() {
    profile="$1"
    url="$2"
    browser="$3"
    user_data_dir="$4"
    opener_path="$5"
    [ -n "$CALLBACK_LOG_PATH" ] || return 0
    printf '{"event":%s,"ok":true,"profile":%s,"url":%s,"browser":%s,"userDataDir":%s,"openerPath":%s}\n' \
        "$(json_string browser_shim_post_launch_callback)" \
        "$(json_string "$profile")" \
        "$(json_string "$url")" \
        "$(json_string "$browser")" \
        "$(json_string "$user_data_dir")" \
        "$(json_string "$opener_path")" >>"$CALLBACK_LOG_PATH"
}

pick_url() {
    for value in "$@"; do
        case "$value" in
            http://*|https://*)
                printf '%s\n' "$value"
                return 0
                ;;
        esac
    done
    return 1
}

failure() {
    message="$1"
    event="$2"
    append_json_line "$(printf '{"event":%s,"message":%s}' \
        "$(json_string "$event")" \
        "$(json_string "$message")")"
    printf '%s\n' "$message" >&2
    exit 1
}

profile_name=$(printf '%s' "${FAST_BROWSER_PROFILE:-$EXPECTED_PROFILE_NAME}" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
browser_env=${BROWSER:-}
url=$(pick_url "$@" || true)
user_data_dir="$HOME/.fast-browser/profiles/$profile_name"

if [ -n "$browser_env" ] && [ "$EXPECTED_BROWSER_PATH" != '' ] && [ "$browser_env" != "$EXPECTED_BROWSER_PATH" ]; then
    append_browser_escape "$browser_env" "$EXPECTED_BROWSER_PATH"
    failure "Managed browser opener was invoked with an unexpected BROWSER path." browser_shim_browser_escape
fi

if [ "$profile_name" != "$EXPECTED_PROFILE_NAME" ]; then
    append_profile_escape "$profile_name" "$EXPECTED_PROFILE_NAME"
    failure "Managed browser opener was asked to use an unexpected managed profile." browser_shim_profile_escape
fi

if [ -z "$url" ]; then
    append_blocked_non_url "$profile_name" "$browser_env"
    printf '%s\n' "Managed Codex browser opener refused a non-URL browser launch request." >&2
    exit 1
fi

append_invoked "$profile_name" "$url" "$browser_env" "$user_data_dir"

if [ -n "$LAUNCH_FAILURE" ]; then
    append_launch_failed "$profile_name" "$url" "$browser_env" "$LAUNCH_FAILURE"
    printf '%s\n' "$LAUNCH_FAILURE" >&2
    exit 1
fi

append_opened_url "$profile_name" "$url" "$browser_env" "$user_data_dir" "$EXPECTED_BROWSER_PATH"
append_callback "$profile_name" "$url" "$browser_env" "$user_data_dir" "$EXPECTED_BROWSER_PATH"
printf '{"ok":true,"profile":%s,"url":%s,"browser":%s,"userDataDir":%s,"openerPath":%s}\n' \
    "$(json_string "$profile_name")" \
    "$(json_string "$url")" \
    "$(json_string "$browser_env")" \
    "$(json_string "$user_data_dir")" \
    "$(json_string "$EXPECTED_BROWSER_PATH")"
"#;

    script
        .replace("__EXPECTED_PROFILE_NAME__", &expected_profile_name)
        .replace("__EXPECTED_BROWSER_PATH__", &expected_browser_path)
        .replace("__LAUNCH_FAILURE__", &launch_failure)
        .replace("__CALLBACK_LOG_PATH__", &callback_log_path)
        .replace("__LOG_PATH__", &log_path)
}

fn shell_single_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }

    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn make_executable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(path)
            .with_context(|| format!("read {} metadata", path.display()))?
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions)
            .with_context(|| format!("chmod +x {}", path.display()))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_mutex() -> &'static std::sync::Mutex<()> {
        crate::test_environment_mutex()
    }

    #[test]
    fn opener_logs_profile_and_supports_success_and_callback() -> Result<()> {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let callback_root = tempfile::tempdir().expect("callback tempdir");
        let callback_log_path = callback_root.path().join("callback-log.jsonl");
        let fixture = FakeManagedBrowserOpenerFixture::builder("codex-rotate-browser-opener")?
            .callback_log_path(&callback_log_path)
            .build()?;

        let output = fixture.launch("https://auth.openai.com/oauth/authorize?state=test")?;
        assert!(output.status.success());

        let stdout = String::from_utf8(output.stdout).expect("stdout utf8");
        let result: Value = serde_json::from_str(stdout.trim()).expect("parse result");
        assert_eq!(
            result["profile"],
            Value::String(fixture.profile_name().to_string())
        );
        assert_eq!(
            result["userDataDir"],
            Value::String(
                fixture
                    .expected_user_data_dir()
                    .to_string_lossy()
                    .to_string()
            )
        );

        let events = fixture.log_entries()?;
        assert!(events
            .iter()
            .any(|event| event["event"] == "browser_shim_invoked"));
        assert!(events
            .iter()
            .any(|event| event["event"] == "browser_shim_opened_url"));
        assert!(!fixture.callback_entries()?.is_empty());
        assert!(callback_log_path.exists());
        Ok(())
    }

    #[test]
    fn opener_rejects_profile_and_browser_env_mismatches() -> Result<()> {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = FakeManagedBrowserOpenerFixture::builder("codex-rotate-browser-opener-fail")?
            .build()?;

        let wrong_profile = fixture.launch_with_profile(
            "https://auth.openai.com/oauth/authorize?state=test",
            "wrong-profile",
        )?;
        assert!(!wrong_profile.status.success());

        let wrong_browser = fixture.launch_with_env(
            "https://auth.openai.com/oauth/authorize?state=test",
            None,
            Some(Path::new("/usr/bin/open")),
        )?;
        assert!(!wrong_browser.status.success());

        let events = fixture.log_entries()?;
        assert!(events
            .iter()
            .any(|event| event["event"] == "browser_shim_profile_escape"));
        assert!(events
            .iter()
            .any(|event| event["event"] == "browser_shim_browser_escape"));
        Ok(())
    }

    #[test]
    fn opener_reports_launch_failure_without_callback() -> Result<()> {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture =
            FakeManagedBrowserOpenerFixture::builder("codex-rotate-browser-opener-launch")?
                .launch_failure("simulated launch failure")
                .build()?;

        let output = fixture.launch("https://auth.openai.com/oauth/authorize?state=test")?;
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("simulated launch failure"));
        assert!(fixture.callback_entries()?.is_empty());
        assert!(fixture
            .log_entries()?
            .iter()
            .any(|event| event["event"] == "browser_shim_launch_failed"));
        Ok(())
    }
}
