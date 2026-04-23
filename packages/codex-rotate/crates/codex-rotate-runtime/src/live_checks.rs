use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use codex_rotate_core::pool::load_pool;
use serde::Deserialize;

use crate::paths::resolve_paths;

const CODEX_APP_PATH_ENV: &str = "CODEX_ROTATE_LIVE_CODEX_APP_PATH";
const CHROME_APP_PATH_ENV: &str = "CODEX_ROTATE_LIVE_CHROME_APP_PATH";
const STAGING_ACCOUNTS_JSON_ENV: &str = "CODEX_ROTATE_STAGING_ACCOUNTS_JSON";
const VM_UTM_APP_PATH_ENV: &str = "CODEX_ROTATE_VM_UTM_APP_PATH";
const VM_UTMCTL_BIN_ENV: &str = "CODEX_ROTATE_UTMCTL_BIN";
const VM_BASE_PACKAGE_PATH_ENV: &str = "CODEX_ROTATE_VM_BASE_PACKAGE_PATH";
const VM_BRIDGE_ROOT_ENV: &str = "CODEX_ROTATE_VM_BRIDGE_ROOT";
const VM_PERSONA_ROOT_ENV: &str = "CODEX_ROTATE_VM_PERSONA_ROOT";
const LIVE_ALIAS_ROOT_ENV: &str = "CODEX_ROTATE_LIVE_ALIAS_ROOT";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LivePrereqCheck {
    pub label: String,
    pub satisfied: bool,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveCapabilityReport {
    pub suite: String,
    pub checks: Vec<LivePrereqCheck>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct LiveStagingAccount {
    pub email: String,
    #[serde(default)]
    pub profile_name: Option<String>,
}

pub fn load_live_staging_accounts(minimum_accounts: usize) -> Result<Vec<LiveStagingAccount>> {
    if let Some(raw) = env::var_os(STAGING_ACCOUNTS_JSON_ENV) {
        let raw = raw.to_string_lossy();
        return load_live_staging_accounts_from_json(&raw, minimum_accounts)
            .map_err(|error| anyhow!("Failed to parse {}: {}", STAGING_ACCOUNTS_JSON_ENV, error));
    }

    load_live_staging_accounts_from_pool(minimum_accounts)
}

impl LiveCapabilityReport {
    pub fn ready(&self) -> bool {
        self.checks.iter().all(|check| check.satisfied)
    }

    pub fn format(&self) -> String {
        let mut lines = vec![format!("{} live prerequisites:", self.suite)];
        for check in &self.checks {
            let status = if check.satisfied { "ok" } else { "missing" };
            lines.push(format!("  [{status}] {}: {}", check.label, check.detail));
        }
        lines.push(format!(
            "  ready: {}",
            if self.ready() { "yes" } else { "no" }
        ));
        lines.join("\n")
    }

    pub fn ensure_ready(&self) -> Result<()> {
        if self.ready() {
            return Ok(());
        }
        Err(anyhow!(self.format()))
    }
}

pub fn host_live_capability_report() -> Result<LiveCapabilityReport> {
    ensure_host_live_alias_environment()?;
    let home = dirs::home_dir().context("Failed to resolve home directory.")?;
    let checks = vec![
        required_live_path_env(
            "CODEX_ROTATE_HOME",
            &home.join(".codex-rotate"),
            "host rotation home",
        )?,
        required_live_path_env("CODEX_HOME", &home.join(".codex"), "Codex home")?,
        required_live_path_env(
            "FAST_BROWSER_HOME",
            &home.join(".fast-browser"),
            "fast-browser home",
        )?,
        required_live_path_env(
            "CODEX_ROTATE_CODEX_APP_SUPPORT",
            &home
                .join("Library")
                .join("Application Support")
                .join("Codex"),
            "Codex app-support directory",
        )?,
        installed_app_bundle_check(CODEX_APP_PATH_ENV, "/Applications/Codex.app", "Codex.app")?,
        installed_app_bundle_check(
            CHROME_APP_PATH_ENV,
            "/Applications/Google Chrome.app",
            "Chrome.app",
        )?,
        staging_accounts_check(2)?,
    ];

    Ok(LiveCapabilityReport {
        suite: "host".to_string(),
        checks,
    })
}

pub fn vm_live_capability_report() -> Result<LiveCapabilityReport> {
    ensure_host_live_alias_environment()?;
    let home = dirs::home_dir().context("Failed to resolve home directory.")?;
    let checks = vec![
        required_live_path_env(
            "CODEX_ROTATE_HOME",
            &home.join(".codex-rotate"),
            "host rotation home",
        )?,
        required_live_path_env("CODEX_HOME", &home.join(".codex"), "Codex home")?,
        required_live_path_env(
            "FAST_BROWSER_HOME",
            &home.join(".fast-browser"),
            "fast-browser home",
        )?,
        required_live_path_env(
            "CODEX_ROTATE_CODEX_APP_SUPPORT",
            &home
                .join("Library")
                .join("Application Support")
                .join("Codex"),
            "Codex app-support directory",
        )?,
        installed_app_bundle_check(VM_UTM_APP_PATH_ENV, "/Applications/UTM.app", "UTM.app")?,
        installed_binary_check(VM_UTMCTL_BIN_ENV, "utmctl")?,
        required_live_path_env(
            VM_BASE_PACKAGE_PATH_ENV,
            &home.join("vm-base.utm"),
            "VM base package",
        )?,
        required_live_path_env(
            VM_BRIDGE_ROOT_ENV,
            &home.join("vm-bridge"),
            "VM bridge root",
        )?,
        required_live_path_env(
            VM_PERSONA_ROOT_ENV,
            &home.join("vm-personas"),
            "VM persona root",
        )?,
        apfs_check_from_env(VM_BASE_PACKAGE_PATH_ENV, "VM base package")?,
        apfs_check_from_env(VM_PERSONA_ROOT_ENV, "VM persona root")?,
        staging_accounts_check(2)?,
    ];

    Ok(LiveCapabilityReport {
        suite: "vm".to_string(),
        checks,
    })
}

pub fn require_host_live_capabilities() -> Result<LiveCapabilityReport> {
    let report = host_live_capability_report()?;
    report.ensure_ready()?;
    Ok(report)
}

pub fn require_vm_live_capabilities() -> Result<LiveCapabilityReport> {
    let report = vm_live_capability_report()?;
    report.ensure_ready()?;
    Ok(report)
}

fn ensure_host_live_alias_environment() -> Result<()> {
    let home = dirs::home_dir().context("Failed to resolve home directory.")?;
    let alias_root = env::var_os(LIVE_ALIAS_ROOT_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/codex-live-alias"));

    ensure_default_live_path_alias(
        "CODEX_ROTATE_HOME",
        &home.join(".codex-rotate"),
        &alias_root.join("rotate-home"),
    )?;
    ensure_default_live_path_alias(
        "CODEX_HOME",
        &home.join(".codex"),
        &alias_root.join("codex-home"),
    )?;
    ensure_default_live_path_alias(
        "FAST_BROWSER_HOME",
        &home.join(".fast-browser"),
        &alias_root.join("fast-browser-home"),
    )?;
    ensure_default_live_path_alias(
        "CODEX_ROTATE_CODEX_APP_SUPPORT",
        &home
            .join("Library")
            .join("Application Support")
            .join("Codex"),
        &alias_root.join("app-support-codex"),
    )?;

    Ok(())
}

fn ensure_default_live_path_alias(
    env_name: &str,
    default_path: &Path,
    alias_path: &Path,
) -> Result<()> {
    let needs_alias = env::var_os(env_name)
        .map(PathBuf::from)
        .map(|path| path == default_path)
        .unwrap_or(true);
    if !needs_alias {
        return Ok(());
    }

    if !default_path.exists() {
        return Ok(());
    }

    let alias_parent = alias_path
        .parent()
        .ok_or_else(|| anyhow!("Alias path {} is missing a parent.", alias_path.display()))?;
    fs::create_dir_all(alias_parent)
        .with_context(|| format!("Failed to create {}.", alias_parent.display()))?;

    if let Ok(existing_target) = fs::read_link(alias_path) {
        if existing_target == default_path {
            unsafe {
                env::set_var(env_name, alias_path);
            }
            return Ok(());
        }
        fs::remove_file(alias_path)
            .with_context(|| format!("Failed to replace stale alias {}.", alias_path.display()))?;
    } else if alias_path.exists() {
        if alias_path.is_dir() {
            fs::remove_dir_all(alias_path).with_context(|| {
                format!(
                    "Failed to replace stale live alias directory {}.",
                    alias_path.display()
                )
            })?;
        } else {
            fs::remove_file(alias_path)
                .with_context(|| format!("Failed to replace {}.", alias_path.display()))?;
        }
    }

    #[cfg(unix)]
    std::os::unix::fs::symlink(default_path, alias_path).with_context(|| {
        format!(
            "Failed to create live alias {} -> {}.",
            alias_path.display(),
            default_path.display()
        )
    })?;
    #[cfg(windows)]
    std::os::windows::fs::symlink_dir(default_path, alias_path).with_context(|| {
        format!(
            "Failed to create live alias {} -> {}.",
            alias_path.display(),
            default_path.display()
        )
    })?;

    unsafe {
        env::set_var(env_name, alias_path);
    }
    Ok(())
}

fn required_live_path_env(name: &str, default_path: &Path, label: &str) -> Result<LivePrereqCheck> {
    let Some(raw_value) = env::var_os(name) else {
        return Ok(LivePrereqCheck {
            label: label.to_string(),
            satisfied: false,
            detail: format!(
                "set {name} to an absolute isolated path (default would be {})",
                default_path.display()
            ),
        });
    };

    let path = PathBuf::from(&raw_value);
    if !path.is_absolute() {
        return Ok(LivePrereqCheck {
            label: label.to_string(),
            satisfied: false,
            detail: format!("{name} must be an absolute path, got {}", path.display()),
        });
    }

    if path == default_path {
        return Ok(LivePrereqCheck {
            label: label.to_string(),
            satisfied: false,
            detail: format!(
                "{name} must not use the default Codex state root {}",
                path.display()
            ),
        });
    }

    if !path.exists() {
        return Ok(LivePrereqCheck {
            label: label.to_string(),
            satisfied: false,
            detail: format!("{} does not exist at {}", name, path.display()),
        });
    }

    Ok(LivePrereqCheck {
        label: label.to_string(),
        satisfied: true,
        detail: path.display().to_string(),
    })
}

fn installed_app_bundle_check(
    env_name: &str,
    default_path: &str,
    label: &str,
) -> Result<LivePrereqCheck> {
    let path = env::var_os(env_name)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(default_path));
    if !path.exists() {
        return Ok(LivePrereqCheck {
            label: label.to_string(),
            satisfied: false,
            detail: format!("{} is missing at {}", label, path.display()),
        });
    }

    Ok(LivePrereqCheck {
        label: label.to_string(),
        satisfied: true,
        detail: path.display().to_string(),
    })
}

fn installed_binary_check(env_name: &str, binary_name: &str) -> Result<LivePrereqCheck> {
    if let Some(path) = env::var_os(env_name).map(PathBuf::from) {
        if !path.exists() {
            return Ok(LivePrereqCheck {
                label: binary_name.to_string(),
                satisfied: false,
                detail: format!("{} is missing at {}", binary_name, path.display()),
            });
        }
        return Ok(LivePrereqCheck {
            label: binary_name.to_string(),
            satisfied: true,
            detail: path.display().to_string(),
        });
    }

    let output = Command::new("which")
        .arg(binary_name)
        .output()
        .context("Failed to probe the system PATH for utmctl.")?;
    if !output.status.success() {
        return Ok(LivePrereqCheck {
            label: binary_name.to_string(),
            satisfied: false,
            detail: format!("set {env_name} or install {binary_name} in PATH"),
        });
    }

    let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(LivePrereqCheck {
        label: binary_name.to_string(),
        satisfied: true,
        detail: if path.is_empty() {
            binary_name.to_string()
        } else {
            path
        },
    })
}

fn staging_accounts_check(minimum_accounts: usize) -> Result<LivePrereqCheck> {
    match load_live_staging_accounts(minimum_accounts) {
        Ok(accounts) => Ok(LivePrereqCheck {
            label: "staging accounts".to_string(),
            satisfied: true,
            detail: format!("{} account(s) configured", accounts.len()),
        }),
        Err(error) => Ok(LivePrereqCheck {
            label: "staging accounts".to_string(),
            satisfied: false,
            detail: error.to_string(),
        }),
    }
}

fn load_live_staging_accounts_from_json(
    raw: &str,
    minimum_accounts: usize,
) -> Result<Vec<LiveStagingAccount>> {
    let parsed = serde_json::from_str::<serde_json::Value>(raw)?;
    let entries = parsed.as_array().ok_or_else(|| {
        anyhow!(
            "{} must be a JSON array of account entries",
            STAGING_ACCOUNTS_JSON_ENV
        )
    })?;

    for (index, entry) in entries.iter().enumerate() {
        let object = entry.as_object().ok_or_else(|| {
            anyhow!(
                "{} entry at index {index} must be a JSON object",
                STAGING_ACCOUNTS_JSON_ENV
            )
        })?;

        if object.contains_key("password") {
            return Err(anyhow!(
                "{} entry at index {index} includes forbidden field `password`; remove passwords from rotation-only staging accounts",
                STAGING_ACCOUNTS_JSON_ENV
            ));
        }
    }

    let accounts = serde_json::from_value::<Vec<LiveStagingAccount>>(parsed)?;
    if accounts.len() < minimum_accounts {
        return Err(anyhow!(
            "{} needs at least {minimum_accounts} account entries, but only {} were provided",
            STAGING_ACCOUNTS_JSON_ENV,
            accounts.len()
        ));
    }

    for account in &accounts {
        if account.email.trim().is_empty() {
            return Err(anyhow!(
                "{} contains an account with an empty email",
                STAGING_ACCOUNTS_JSON_ENV
            ));
        }
    }

    Ok(accounts)
}

fn load_live_staging_accounts_from_pool(
    minimum_accounts: usize,
) -> Result<Vec<LiveStagingAccount>> {
    let paths = resolve_paths()?;
    let pool = load_pool().with_context(|| {
        format!(
            "set {} to a JSON array of at least {minimum_accounts} accounts or ensure {} exists",
            STAGING_ACCOUNTS_JSON_ENV,
            paths.rotate_home.join("accounts.json").display()
        )
    })?;

    let mut candidates = pool
        .accounts
        .into_iter()
        .filter(|account| {
            !account.email.trim().is_empty() && !matches!(account.last_quota_usable, Some(false))
        })
        .collect::<Vec<_>>();

    candidates.sort_by_key(|account| {
        (
            plan_priority(&account.plan_type),
            quota_priority(account.last_quota_usable),
            account.email.to_ascii_lowercase(),
        )
    });

    let selected = candidates
        .into_iter()
        .take(minimum_accounts)
        .map(|account| LiveStagingAccount {
            email: account.email,
            profile_name: account.alias,
        })
        .collect::<Vec<_>>();

    if selected.len() < minimum_accounts {
        return Err(anyhow!(
            "set {} to a JSON array of at least {minimum_accounts} accounts or ensure {} contains at least {minimum_accounts} healthy rotation candidates (team preferred)",
            STAGING_ACCOUNTS_JSON_ENV,
            paths.rotate_home.join("accounts.json").display()
        ));
    }

    Ok(selected)
}

fn plan_priority(plan_type: &str) -> u8 {
    match plan_type.trim().to_ascii_lowercase().as_str() {
        "team" => 0,
        "enterprise" | "pro" | "plus" => 1,
        "free" => 2,
        _ => 3,
    }
}

fn quota_priority(last_quota_usable: Option<bool>) -> u8 {
    match last_quota_usable {
        Some(true) => 0,
        None => 1,
        Some(false) => 2,
    }
}

fn apfs_check_from_env(env_name: &str, label: &str) -> Result<LivePrereqCheck> {
    let Some(raw_value) = env::var_os(env_name) else {
        return Ok(LivePrereqCheck {
            label: label.to_string(),
            satisfied: false,
            detail: format!("set {env_name} to an absolute APFS-backed path"),
        });
    };

    let path = PathBuf::from(&raw_value);
    if !path.exists() {
        return Ok(LivePrereqCheck {
            label: label.to_string(),
            satisfied: false,
            detail: format!("{} does not exist at {}", env_name, path.display()),
        });
    }

    let filesystem_type = filesystem_type(&path)?;
    if filesystem_type != "apfs" {
        return Ok(LivePrereqCheck {
            label: label.to_string(),
            satisfied: false,
            detail: format!(
                "{} must be on APFS, but {} is on {}",
                label,
                path.display(),
                filesystem_type
            ),
        });
    }

    Ok(LivePrereqCheck {
        label: label.to_string(),
        satisfied: true,
        detail: path.display().to_string(),
    })
}

fn filesystem_type(path: &Path) -> Result<String> {
    let output = Command::new("mount")
        .output()
        .context("Failed to inspect mounted filesystems.")?;
    if !output.status.success() {
        return Err(anyhow!(
            "Failed to inspect mounted filesystems: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    let path = path.canonicalize().with_context(|| {
        format!(
            "Failed to canonicalize {} for filesystem inspection.",
            path.display()
        )
    })?;
    let mut best_match: Option<(usize, String)> = None;
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let Some((_, mount_and_rest)) = line.split_once(" on ") else {
            continue;
        };
        let Some((mount_point, rest)) = mount_and_rest.split_once(" (") else {
            continue;
        };
        let mount_point = Path::new(mount_point);
        if !path.starts_with(mount_point) {
            continue;
        }
        let mount_len = mount_point.to_string_lossy().len();
        let replace = best_match
            .as_ref()
            .map(|(current_len, _)| mount_len > *current_len)
            .unwrap_or(true);
        if replace {
            let filesystem_type = rest
                .split(',')
                .next()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            best_match = Some((mount_len, filesystem_type));
        }
    }

    best_match
        .map(|(_, filesystem_type)| filesystem_type)
        .ok_or_else(|| {
            anyhow!(
                "Could not determine the filesystem type for {}.",
                path.display()
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::env_mutex;
    use std::ffi::OsString;

    fn restore_env(name: &str, value: Option<OsString>) {
        match value {
            Some(value) => unsafe {
                env::set_var(name, value);
            },
            None => unsafe {
                env::remove_var(name);
            },
        }
    }

    #[test]
    fn ensure_host_live_alias_environment_installs_aliases_for_default_paths() {
        let _guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempfile::tempdir().expect("tempdir");
        let home = temp.path().join("home");
        let alias_root = temp.path().join("alias-root");
        let defaults = [
            home.join(".codex-rotate"),
            home.join(".codex"),
            home.join(".fast-browser"),
            home.join("Library")
                .join("Application Support")
                .join("Codex"),
        ];
        for path in &defaults {
            fs::create_dir_all(path).expect("create default live path");
        }

        let previous_home = env::var_os("HOME");
        let previous_rotate_home = env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = env::var_os("CODEX_HOME");
        let previous_fast_browser_home = env::var_os("FAST_BROWSER_HOME");
        let previous_codex_app_support = env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT");
        let previous_alias_root = env::var_os(LIVE_ALIAS_ROOT_ENV);

        unsafe {
            env::set_var("HOME", &home);
            env::remove_var("CODEX_ROTATE_HOME");
            env::remove_var("CODEX_HOME");
            env::remove_var("FAST_BROWSER_HOME");
            env::remove_var("CODEX_ROTATE_CODEX_APP_SUPPORT");
            env::set_var(LIVE_ALIAS_ROOT_ENV, &alias_root);
        }

        ensure_host_live_alias_environment().expect("install host live aliases");

        let rotate_home_alias = alias_root.join("rotate-home");
        let codex_home_alias = alias_root.join("codex-home");
        let fast_browser_home_alias = alias_root.join("fast-browser-home");
        let app_support_alias = alias_root.join("app-support-codex");
        assert_eq!(
            env::var_os("CODEX_ROTATE_HOME"),
            Some(rotate_home_alias.into_os_string())
        );
        assert_eq!(
            env::var_os("CODEX_HOME"),
            Some(codex_home_alias.into_os_string())
        );
        assert_eq!(
            env::var_os("FAST_BROWSER_HOME"),
            Some(fast_browser_home_alias.into_os_string())
        );
        assert_eq!(
            env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT"),
            Some(app_support_alias.into_os_string())
        );

        assert_eq!(
            fs::read_link(alias_root.join("rotate-home")).expect("rotate-home alias"),
            home.join(".codex-rotate")
        );
        assert_eq!(
            fs::read_link(alias_root.join("codex-home")).expect("codex-home alias"),
            home.join(".codex")
        );
        assert_eq!(
            fs::read_link(alias_root.join("fast-browser-home")).expect("fast-browser alias"),
            home.join(".fast-browser")
        );
        assert_eq!(
            fs::read_link(alias_root.join("app-support-codex")).expect("app-support alias"),
            home.join("Library")
                .join("Application Support")
                .join("Codex")
        );

        restore_env("HOME", previous_home);
        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
        restore_env("FAST_BROWSER_HOME", previous_fast_browser_home);
        restore_env("CODEX_ROTATE_CODEX_APP_SUPPORT", previous_codex_app_support);
        restore_env(LIVE_ALIAS_ROOT_ENV, previous_alias_root);
    }
}
