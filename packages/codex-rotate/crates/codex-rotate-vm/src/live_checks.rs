use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use codex_rotate_runtime::live_checks::{
    isolated_live_ack_check, isolated_live_path_env_check, load_live_staging_accounts,
    LiveCapabilityReport, LivePrereqCheck,
};

const VM_UTM_APP_PATH_ENV: &str = "CODEX_ROTATE_VM_UTM_APP_PATH";
const VM_UTMCTL_BIN_ENV: &str = "CODEX_ROTATE_UTMCTL_BIN";
const VM_BASE_PACKAGE_PATH_ENV: &str = "CODEX_ROTATE_VM_BASE_PACKAGE_PATH";
const VM_BRIDGE_ROOT_ENV: &str = "CODEX_ROTATE_VM_BRIDGE_ROOT";
const VM_PERSONA_ROOT_ENV: &str = "CODEX_ROTATE_VM_PERSONA_ROOT";

pub fn vm_live_capability_report() -> Result<LiveCapabilityReport> {
    let home = dirs::home_dir().context("Failed to resolve home directory.")?;
    let checks = vec![
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
        installed_app_bundle_check(VM_UTM_APP_PATH_ENV, "/Applications/UTM.app", "UTM.app")?,
        installed_binary_check(VM_UTMCTL_BIN_ENV, "utmctl")?,
        isolated_live_path_env_check(
            VM_BASE_PACKAGE_PATH_ENV,
            &home.join("vm-base.utm"),
            "VM base package",
        )?,
        isolated_live_path_env_check(
            VM_BRIDGE_ROOT_ENV,
            &home.join("vm-bridge"),
            "VM bridge root",
        )?,
        isolated_live_path_env_check(
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

pub fn require_vm_live_capabilities() -> Result<LiveCapabilityReport> {
    let report = vm_live_capability_report()?;
    report.ensure_ready()?;
    Ok(report)
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
