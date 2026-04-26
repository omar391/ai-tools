use std::env;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use codex_rotate_core::pool::load_pool;
use serde::Deserialize;

use crate::paths::resolve_paths;

const CODEX_APP_PATH_ENV: &str = "CODEX_ROTATE_LIVE_CODEX_APP_PATH";
const CHROME_APP_PATH_ENV: &str = "CODEX_ROTATE_LIVE_CHROME_APP_PATH";
const STAGING_ACCOUNTS_JSON_ENV: &str = "CODEX_ROTATE_STAGING_ACCOUNTS_JSON";
const ISOLATED_LIVE_ENV: &str = "CODEX_ROTATE_LIVE_ISOLATED";
const ISOLATED_LIVE_ROOT_ENV: &str = "CODEX_ROTATE_LIVE_ISOLATED_ROOT";

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

    require_isolated_rotate_home_for_pool_load()?;
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
    let home = dirs::home_dir().context("Failed to resolve home directory.")?;
    let mut checks = vec![
        isolated_live_ack_check(),
        isolated_live_path_env_check(
            "CODEX_ROTATE_HOME",
            &home.join(".codex-rotate"),
            "host rotation home",
        )?,
        isolated_live_path_env_check("CODEX_HOME", &home.join(".codex"), "Codex home")?,
        isolated_live_path_env_check(
            "FAST_BROWSER_HOME",
            &home.join(".fast-browser"),
            "fast-browser home",
        )?,
        isolated_live_path_env_check(
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
    ];
    checks.push(staging_accounts_check(2)?);

    Ok(LiveCapabilityReport {
        suite: "host".to_string(),
        checks,
    })
}

pub fn require_host_live_capabilities() -> Result<LiveCapabilityReport> {
    let report = host_live_capability_report()?;
    report.ensure_ready()?;
    Ok(report)
}

pub fn isolated_live_ack_check() -> LivePrereqCheck {
    let enabled = env::var(ISOLATED_LIVE_ENV)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    LivePrereqCheck {
        label: "isolated live test mode".to_string(),
        satisfied: enabled,
        detail: if enabled {
            format!("{ISOLATED_LIVE_ENV}=1")
        } else {
            format!("set {ISOLATED_LIVE_ENV}=1 and {ISOLATED_LIVE_ROOT_ENV} to a disposable root")
        },
    }
}

pub fn isolated_live_path_env_check(
    name: &str,
    default_path: &Path,
    label: &str,
) -> Result<LivePrereqCheck> {
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

    if !path.exists() {
        return Ok(LivePrereqCheck {
            label: label.to_string(),
            satisfied: false,
            detail: format!("{} does not exist at {}", name, path.display()),
        });
    }

    let Some(isolated_root) = env::var_os(ISOLATED_LIVE_ROOT_ENV).map(PathBuf::from) else {
        return Ok(LivePrereqCheck {
            label: label.to_string(),
            satisfied: false,
            detail: format!("set {ISOLATED_LIVE_ROOT_ENV} to the disposable live-test root"),
        });
    };
    if !isolated_root.is_absolute() || !isolated_root.exists() {
        return Ok(LivePrereqCheck {
            label: label.to_string(),
            satisfied: false,
            detail: format!(
                "{ISOLATED_LIVE_ROOT_ENV} must be an existing absolute path, got {}",
                isolated_root.display()
            ),
        });
    }

    let canonical_path = path.canonicalize().unwrap_or_else(|_| path.clone());
    let canonical_root = isolated_root
        .canonicalize()
        .unwrap_or_else(|_| isolated_root.clone());
    if !canonical_path.starts_with(&canonical_root) {
        return Ok(LivePrereqCheck {
            label: label.to_string(),
            satisfied: false,
            detail: format!(
                "{} must be under isolated root {} (got {})",
                name,
                canonical_root.display(),
                canonical_path.display()
            ),
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

fn require_isolated_rotate_home_for_pool_load() -> Result<()> {
    let ack = isolated_live_ack_check();
    if !ack.satisfied {
        return Err(anyhow!(
            "{}; refusing to read a live pool outside an isolated root",
            ack.detail
        ));
    }

    let home = dirs::home_dir().context("Failed to resolve home directory.")?;
    let check = isolated_live_path_env_check(
        "CODEX_ROTATE_HOME",
        &home.join(".codex-rotate"),
        "host rotation home",
    )?;
    if !check.satisfied {
        return Err(anyhow!(
            "{}: {}; refusing to read a live pool outside an isolated root",
            check.label,
            check.detail
        ));
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::env_mutex;
    use std::ffi::OsString;
    use std::fs;

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
    fn isolated_live_ack_requires_explicit_opt_in() {
        let _guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let previous_isolated = env::var_os(ISOLATED_LIVE_ENV);

        unsafe {
            env::remove_var(ISOLATED_LIVE_ENV);
        }

        let check = isolated_live_ack_check();
        assert!(!check.satisfied);
        assert!(check.detail.contains(ISOLATED_LIVE_ENV));

        restore_env(ISOLATED_LIVE_ENV, previous_isolated);
    }

    #[test]
    fn isolated_live_path_must_be_under_disposable_root() {
        let _guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempfile::tempdir().expect("tempdir");
        let isolated_root = temp.path().join("isolated");
        let inside = isolated_root.join(".codex-rotate");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&inside).expect("create inside");
        fs::create_dir_all(&outside).expect("create outside");

        let previous_root = env::var_os(ISOLATED_LIVE_ROOT_ENV);
        let previous_rotate_home = env::var_os("CODEX_ROTATE_HOME");
        unsafe {
            env::set_var(ISOLATED_LIVE_ROOT_ENV, &isolated_root);
            env::set_var("CODEX_ROTATE_HOME", &inside);
        }

        let check = isolated_live_path_env_check(
            "CODEX_ROTATE_HOME",
            &isolated_root.join(".codex-rotate"),
            "host rotation home",
        )
        .expect("inside check");
        assert!(check.satisfied);

        unsafe {
            env::set_var("CODEX_ROTATE_HOME", &outside);
        }
        let check = isolated_live_path_env_check(
            "CODEX_ROTATE_HOME",
            &isolated_root.join(".codex-rotate"),
            "host rotation home",
        )
        .expect("outside check");
        assert!(!check.satisfied);
        assert!(check.detail.contains("must be under isolated root"));

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env(ISOLATED_LIVE_ROOT_ENV, previous_root);
    }

    #[test]
    fn staging_accounts_pool_fallback_requires_isolated_live_root() {
        let _guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempfile::tempdir().expect("tempdir");
        let rotate_home = temp.path().join(".codex-rotate");
        fs::create_dir_all(&rotate_home).expect("create rotate home");

        let previous_staging = env::var_os(STAGING_ACCOUNTS_JSON_ENV);
        let previous_isolated = env::var_os(ISOLATED_LIVE_ENV);
        let previous_root = env::var_os(ISOLATED_LIVE_ROOT_ENV);
        let previous_rotate_home = env::var_os("CODEX_ROTATE_HOME");
        unsafe {
            env::remove_var(STAGING_ACCOUNTS_JSON_ENV);
            env::remove_var(ISOLATED_LIVE_ENV);
            env::remove_var(ISOLATED_LIVE_ROOT_ENV);
            env::set_var("CODEX_ROTATE_HOME", &rotate_home);
        }

        let error =
            load_live_staging_accounts(1).expect_err("pool fallback must require isolation");
        assert!(error.to_string().contains(ISOLATED_LIVE_ENV));
        assert!(error.to_string().contains("refusing to read a live pool"));

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env(ISOLATED_LIVE_ROOT_ENV, previous_root);
        restore_env(ISOLATED_LIVE_ENV, previous_isolated);
        restore_env(STAGING_ACCOUNTS_JSON_ENV, previous_staging);
    }
}
