use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use codex_rotate_core::pool::{
    load_pool, persist_prepared_rotation_pool, resolve_persona_profile, resolve_pool_account,
    save_pool, write_selected_account_auth, PersonaEntry, PreparedRotation, PreparedRotationAction,
    VmEnvironmentConfig,
};
use codex_rotate_core::workflow::{cmd_generate_browser_fingerprint, ReloginOptions};
use codex_rotate_runtime::rotation_hygiene::ThreadHandoff;
use fs2::available_space;
use serde_json::{json, Value};

use crate::guest_bridge::send_guest_request;

#[derive(Clone, Debug)]
pub struct VmBackend {
    pub config: Option<VmEnvironmentConfig>,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
struct GuestThreadHandoffExportResult {
    #[serde(default)]
    handoffs: Vec<ThreadHandoff>,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
struct GuestThreadHandoffImportResult {
    #[serde(default)]
    failures: Vec<Value>,
}

impl VmBackend {
    pub fn new(config: Option<VmEnvironmentConfig>) -> Self {
        Self { config }
    }

    pub fn activate(
        &self,
        prepared: &PreparedRotation,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<Vec<ThreadHandoff>> {
        self.validate_config()?;
        let handoffs = match self.export_guest_handoffs(&prepared.previous.account_id) {
            Ok(handoffs) => handoffs,
            Err(error) => {
                if let Some(progress) = progress.as_ref() {
                    progress(format!(
                        "Skipping VM source handoff export because guest bridge export was unavailable: {error:#}"
                    ));
                }
                Vec::new()
            }
        };
        self.stop_all_persona_vms(progress.as_ref())?;

        let persona = prepared
            .target
            .persona
            .as_ref()
            .ok_or_else(|| anyhow!("Target account is missing persona metadata."))?;

        self.ensure_persona_package_ready(persona)?;
        self.launch_vm(persona, progress.as_ref())?;
        self.start_guest_codex()?;

        if !handoffs.is_empty() {
            self.import_guest_handoffs(&prepared.target.account_id, &handoffs)?;
        }

        Ok(Vec::new())
    }

    pub fn rollback_after_failed_activation(
        &self,
        progress: Option<&Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<()> {
        if let Some(progress) = progress {
            progress("Rolling back failed VM activation...".to_string());
        }
        self.stop_all_persona_vms(progress)
    }

    pub fn relogin(&self, selector: &str, options: ReloginOptions) -> Result<String> {
        self.validate_config()?;
        let target_account = resolve_pool_account(selector)?.ok_or_else(|| {
            anyhow!(
                "Cannot relogin to non-pool account {} in VM mode.",
                selector
            )
        })?;

        let persona = target_account
            .persona
            .as_ref()
            .ok_or_else(|| anyhow!("Target account is missing persona metadata."))?;

        let previous_pool = load_pool()?;
        let mut pool = previous_pool.clone();
        let active_index = pool.active_index.min(pool.accounts.len().saturating_sub(1));
        let target_index = pool
            .accounts
            .iter()
            .position(|entry| entry.account_id == target_account.account_id)
            .ok_or_else(|| anyhow!("Failed to resolve relogin target {} in the pool.", selector))?;

        persist_prepared_rotation_pool(&PreparedRotation {
            action: PreparedRotationAction::Stay,
            pool: pool.clone(),
            previous_index: active_index,
            target_index: active_index,
            previous: pool.accounts[active_index].clone(),
            target: pool.accounts[active_index].clone(),
            message: String::new(),
            persist_pool: true,
        })?;

        let is_active_account = target_index == active_index;
        let active_persona = pool.accounts[active_index].persona.clone();

        if !is_active_account {
            self.stop_all_persona_vms(None)?;
        }
        self.ensure_persona_package_ready(persona)?;
        self.launch_vm(persona, None)?;

        let restore_active_vm = || {
            self.stop_all_persona_vms(None).ok();
            if let Some(active_persona) = active_persona.as_ref() {
                self.ensure_persona_package_ready(active_persona).ok();
                self.launch_vm(active_persona, None).ok();
                self.start_guest_codex().ok();
            }
        };

        let result = (|| -> Result<String> {
            let relogin_response: Value = self.send_guest_request(
                "relogin",
                json!({
                    "selector": selector,
                    "options": options
                }),
            )?;

            let output = relogin_response
                .get("output")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();

            let guest_auth_val = relogin_response
                .get("auth")
                .ok_or_else(|| anyhow!("Guest relogin response did not include auth state."))?;

            let guest_auth: codex_rotate_core::auth::CodexAuth =
                serde_json::from_value(guest_auth_val.clone())
                    .with_context(|| "Failed to parse guest auth state.")?;

            if let Some(entry) = pool
                .accounts
                .iter_mut()
                .find(|account| account.account_id == target_account.account_id)
            {
                entry.auth = guest_auth.clone();
            }
            save_pool(&pool).with_context(|| {
                format!(
                    "Failed to persist guest auth for {} back to the host pool.",
                    target_account.label
                )
            })?;

            if is_active_account {
                if let Some(active_entry) = pool.accounts.get(pool.active_index) {
                    if let Err(error) = write_selected_account_auth(active_entry) {
                        let mut failures = vec![format!("host auth sync failed: {error:#}")];
                        if let Err(rollback_error) =
                            rollback_vm_relogin_auth_sync_failure(&previous_pool)
                        {
                            failures.push(format!("rollback failed: {rollback_error:#}"));
                        }
                        return Err(anyhow!(failures.join(" | ")));
                    }
                }
            } else {
                restore_active_vm();
            }

            Ok(output)
        })();

        if result.is_err() && !is_active_account {
            restore_active_vm();
        }

        result
    }

    pub fn export_guest_handoffs(&self, account_id: &str) -> Result<Vec<ThreadHandoff>> {
        let result: GuestThreadHandoffExportResult = send_guest_request(
            "export-thread-handoffs",
            json!({
                "account_id": account_id,
            }),
        )?;
        Ok(result.handoffs)
    }

    pub fn import_guest_handoffs(
        &self,
        target_account_id: &str,
        handoffs: &[ThreadHandoff],
    ) -> Result<()> {
        let result: GuestThreadHandoffImportResult = send_guest_request(
            "import-thread-handoffs",
            json!({
                "target_account_id": target_account_id,
                "handoffs": handoffs,
            }),
        )?;
        if result.failures.is_empty() {
            return Ok(());
        }
        Err(anyhow!(
            "Guest handoff import reported {} failure(s).",
            result.failures.len()
        ))
    }

    pub fn start_guest_codex(&self) -> Result<()> {
        send_guest_request::<Value, Value>("start-codex", json!({}))?;
        Ok(())
    }

    pub fn send_guest_request<REQ, RES>(&self, command: &str, payload: REQ) -> Result<RES>
    where
        REQ: serde::Serialize,
        RES: serde::de::DeserializeOwned,
    {
        send_guest_request(command, payload)
    }

    pub fn launch_vm(
        &self,
        persona: &PersonaEntry,
        progress: Option<&Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<()> {
        let package_path = self.resolve_persona_package_path(persona)?;
        if let Some(progress) = progress {
            progress(format!(
                "Launching VM package at {}...",
                package_path.display()
            ));
        }

        let status = Command::new(utmctl_binary())
            .arg("start")
            .arg(&package_path)
            .status()
            .with_context(|| {
                format!(
                    "Failed to execute `utmctl start {}`.",
                    package_path.display()
                )
            })?;

        if !status.success() {
            return Err(anyhow!(
                "utmctl start failed (exit code {}).",
                status.code().unwrap_or(-1)
            ));
        }

        self.wait_for_vm_started(persona, progress)
    }

    pub fn wait_for_vm_started(
        &self,
        persona: &PersonaEntry,
        progress: Option<&Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(60);

        if let Some(progress) = progress {
            progress(format!(
                "Waiting for VM \"{}\" to boot...",
                persona.persona_id
            ));
        }

        while start.elapsed() < timeout {
            let output = Command::new(utmctl_binary())
                .arg("status")
                .arg(&persona.persona_id)
                .output()
                .with_context(|| "Failed to execute `utmctl status`.")?;

            if output.status.success() {
                let status = String::from_utf8_lossy(&output.stdout)
                    .trim()
                    .to_lowercase();
                if status == "started" {
                    return Ok(());
                }
            }

            std::thread::sleep(std::time::Duration::from_secs(2));
        }

        Err(anyhow!(
            "Timed out waiting for VM \"{}\" to boot after {}s.",
            persona.persona_id,
            timeout.as_secs()
        ))
    }

    pub fn stop_all_persona_vms(
        &self,
        progress: Option<&Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<()> {
        let output = Command::new(utmctl_binary())
            .arg("list")
            .output()
            .with_context(|| "Failed to execute `utmctl list`.")?;

        if !output.status.success() {
            return Err(anyhow!(
                "utmctl list failed (exit code {}).",
                output.status.code().unwrap_or(-1)
            ));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 3 {
                continue;
            }
            let status = parts[1];
            let name = parts[2..].join(" ");

            if status == "started" && name.starts_with("persona-") {
                if let Some(progress) = progress {
                    progress(format!("Stopping VM \"{}\"...", name));
                }
                let stop_status = Command::new(utmctl_binary())
                    .arg("stop")
                    .arg(parts[0])
                    .status()
                    .with_context(|| format!("Failed to execute `utmctl stop {}`.", parts[0]))?;

                if !stop_status.success() {
                    return Err(anyhow!(
                        "utmctl stop {} failed (exit code {}).",
                        parts[0],
                        stop_status.code().unwrap_or(-1)
                    ));
                }

                let start = std::time::Instant::now();
                let timeout = std::time::Duration::from_secs(30);
                let mut stopped = false;
                while start.elapsed() < timeout {
                    let check_output = Command::new(utmctl_binary())
                        .arg("status")
                        .arg(parts[0])
                        .output();

                    if let Ok(check) = check_output {
                        let check_status =
                            String::from_utf8_lossy(&check.stdout).trim().to_lowercase();
                        if check_status == "stopped"
                            || check_status == "suspended"
                            || check_status.is_empty()
                        {
                            stopped = true;
                            break;
                        }
                    }
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }

                if !stopped {
                    return Err(anyhow!(
                        "Timed out waiting for VM \"{}\" to stop after {}s.",
                        name,
                        timeout.as_secs()
                    ));
                }
            }
        }

        Ok(())
    }

    pub fn ensure_persona_package_ready(&self, persona: &PersonaEntry) -> Result<()> {
        self.validate_config()?;
        let target_path = self.resolve_persona_package_path(persona)?;
        if target_path.exists() {
            return Ok(());
        }

        let config = self
            .config
            .as_ref()
            .ok_or_else(|| anyhow!("VM configuration is missing."))?;

        let base_path = config
            .base_package_path
            .as_ref()
            .ok_or_else(|| anyhow!("VM base_package_path is not configured."))?;

        let base_path = PathBuf::from(base_path);
        if !base_path.exists() {
            return Err(anyhow!(
                "VM base package not found at {}.",
                base_path.display()
            ));
        }

        ensure_clone_capacity(&base_path, &target_path)?;

        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let status = Command::new("cp")
            .arg("-R")
            .arg(&base_path)
            .arg(&target_path)
            .status()
            .with_context(|| {
                format!(
                    "Failed to clone VM base package from {} to {}.",
                    base_path.display(),
                    target_path.display()
                )
            })?;

        if !status.success() {
            return Err(anyhow!(
                "Failed to clone VM base package (exit code {}).",
                status.code().unwrap_or(-1)
            ));
        }

        if persona.browser_fingerprint.is_none() {
            if let Some(profile) = resolve_persona_profile(
                persona
                    .persona_profile_id
                    .as_deref()
                    .unwrap_or("balanced-us-compact"),
                None,
            ) {
                if let Ok(fingerprint) =
                    cmd_generate_browser_fingerprint(&persona.persona_id, &profile)
                {
                    let mut pool = load_pool()?;
                    if let Some(entry) = pool.accounts.iter_mut().find(|account| {
                        account.account_id == persona.persona_id
                            || account
                                .persona
                                .as_ref()
                                .map(|candidate| candidate.persona_id == persona.persona_id)
                                .unwrap_or(false)
                    }) {
                        if let Some(entry_persona) = entry.persona.as_mut() {
                            entry_persona.browser_fingerprint = Some(fingerprint);
                            save_pool(&pool)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn resolve_persona_package_path(&self, persona: &PersonaEntry) -> Result<PathBuf> {
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| anyhow!("VM configuration is missing."))?;

        let persona_root = config
            .persona_root
            .as_ref()
            .ok_or_else(|| anyhow!("VM persona_root is not configured."))?;

        let package_name = validate_vm_persona_id(&persona.persona_id)?;
        Ok(PathBuf::from(persona_root).join(format!("{package_name}.utm")))
    }

    pub fn validate_config(&self) -> Result<()> {
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| anyhow!("VM configuration is missing."))?;
        validate_vm_environment_config(config)
    }
}

fn rollback_vm_relogin_auth_sync_failure(
    previous_pool: &codex_rotate_core::pool::Pool,
) -> Result<()> {
    save_pool(previous_pool)?;
    if let Some(active_entry) = previous_pool.accounts.get(
        previous_pool
            .active_index
            .min(previous_pool.accounts.len().saturating_sub(1)),
    ) {
        write_selected_account_auth(active_entry)?;
    }
    Ok(())
}

fn utmctl_binary() -> String {
    std::env::var("CODEX_ROTATE_UTMCTL_BIN")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "utmctl".to_string())
}

fn validate_vm_environment_config(config: &VmEnvironmentConfig) -> Result<()> {
    let base_package_path = require_absolute_existing_directory(
        config
            .base_package_path
            .as_deref()
            .ok_or_else(|| anyhow!("VM base_package_path is not configured."))?,
        "VM base_package_path",
    )?;
    let persona_root = require_absolute_path(
        config
            .persona_root
            .as_deref()
            .ok_or_else(|| anyhow!("VM persona_root is not configured."))?,
        "VM persona_root",
    )?;
    let _utm_app_path = require_absolute_existing_directory(
        config
            .utm_app_path
            .as_deref()
            .ok_or_else(|| anyhow!("VM utm_app_path is not configured."))?,
        "VM utm_app_path",
    )?;

    if let Some(bridge_root) = config.bridge_root.as_deref() {
        require_absolute_directory(bridge_root, "VM bridge_root")?;
    }

    if !persona_root.exists() {
        fs::create_dir_all(&persona_root)
            .with_context(|| format!("Failed to create {}.", persona_root.display()))?;
    }
    ensure_apfs_filesystem(&base_package_path, "VM base package")?;
    ensure_apfs_filesystem(&persona_root, "VM persona root")?;
    Ok(())
}

fn require_absolute_existing_directory(path: &str, field: &str) -> Result<PathBuf> {
    let path = require_absolute_path(path, field)?;
    let metadata = fs::metadata(&path)
        .with_context(|| format!("{} does not exist at {}.", field, path.display()))?;
    if !metadata.is_dir() {
        return Err(anyhow!(
            "{} must be a directory at {}.",
            field,
            path.display()
        ));
    }
    Ok(path)
}

fn require_absolute_directory(path: &str, field: &str) -> Result<PathBuf> {
    let path = require_absolute_path(path, field)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {} parent {}.", field, parent.display()))?;
    }
    if path.exists() {
        let metadata = fs::metadata(&path)
            .with_context(|| format!("Failed to inspect {} at {}.", field, path.display()))?;
        if !metadata.is_dir() {
            return Err(anyhow!(
                "{} must be a directory at {}.",
                field,
                path.display()
            ));
        }
    } else {
        fs::create_dir_all(&path)
            .with_context(|| format!("Failed to create {} at {}.", field, path.display()))?;
    }
    Ok(path)
}

fn require_absolute_path(path: &str, field: &str) -> Result<PathBuf> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("{field} cannot be empty."));
    }
    let candidate = PathBuf::from(trimmed);
    if !candidate.is_absolute() {
        return Err(anyhow!("{field} must be an absolute path: {trimmed}."));
    }
    if candidate
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(anyhow!(
            "{field} cannot contain parent-directory segments: {trimmed}."
        ));
    }
    Ok(candidate)
}

fn validate_vm_persona_id(persona_id: &str) -> Result<String> {
    let normalized = persona_id.trim();
    if normalized.is_empty() {
        return Err(anyhow!("Persona id cannot be empty."));
    }
    if normalized
        .chars()
        .any(|character| matches!(character, '/' | '\\' | ':'))
    {
        return Err(anyhow!(
            "Persona id {normalized:?} cannot contain path separators or drive prefixes."
        ));
    }
    if normalized.contains("..") {
        return Err(anyhow!(
            "Persona id {normalized:?} cannot contain parent-directory segments."
        ));
    }
    Ok(normalized.to_string())
}

fn ensure_apfs_filesystem(path: &Path, label: &str) -> Result<()> {
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
        let mount_len = mount_point.as_os_str().len();
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

    let filesystem_type = best_match
        .map(|(_, filesystem_type)| filesystem_type)
        .ok_or_else(|| {
            anyhow!(
                "Could not determine the filesystem type for {}.",
                path.display()
            )
        })?;

    if filesystem_type != "apfs" {
        return Err(anyhow!(
            "{label} requires APFS-backed storage, but {} is on {}.",
            path.display(),
            filesystem_type
        ));
    }
    Ok(())
}

fn directory_size(path: &Path) -> Result<u64> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("Failed to inspect {}.", path.display()))?;
    if metadata.is_file() || metadata.file_type().is_symlink() {
        return Ok(metadata.len());
    }

    let mut size = 0u64;
    for entry in fs::read_dir(path)
        .with_context(|| format!("Failed to read directory {}.", path.display()))?
    {
        let entry = entry?;
        size = size.saturating_add(directory_size(&entry.path())?);
    }
    Ok(size)
}

fn ensure_clone_capacity(base_package_path: &Path, target_root: &Path) -> Result<()> {
    let required_bytes = directory_size(base_package_path)?;
    let target_parent = target_root.parent().unwrap_or(target_root);
    let available_bytes = available_space(target_parent).with_context(|| {
        format!(
            "Failed to determine free space for {}.",
            target_parent.display()
        )
    })?;
    if available_bytes < required_bytes {
        return Err(anyhow!(
            "Not enough free space to provision VM persona at {}: need at least {} bytes, found {} bytes.",
            target_root.display(),
            required_bytes,
            available_bytes
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_rotate_core::pool::VmExpectedEgressMode;
    use serde_json::json;
    use tempfile::tempdir;

    fn test_vm_backend(root: &Path) -> VmBackend {
        let base_path = root.join("base.utm");
        let persona_root = root.join("personas");
        let utm_app_path = root.join("UTM.app");
        fs::create_dir_all(&base_path).expect("create base");
        fs::write(base_path.join("config.plist"), "base").expect("write base config");
        fs::create_dir_all(&persona_root).expect("create persona root");
        fs::create_dir_all(&utm_app_path).expect("create utm app");

        VmBackend {
            config: Some(VmEnvironmentConfig {
                base_package_path: Some(base_path.to_str().unwrap().to_string()),
                persona_root: Some(persona_root.to_str().unwrap().to_string()),
                utm_app_path: Some(utm_app_path.to_str().unwrap().to_string()),
                bridge_root: None,
                expected_egress_mode: VmExpectedEgressMode::ProvisionOnly,
            }),
        }
    }

    #[test]
    fn vm_backend_validates_missing_config() {
        let backend = VmBackend { config: None };
        let result = backend.validate_config();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("VM configuration is missing"));
    }

    #[test]
    fn vm_persona_package_resolution_rejects_unsafe_persona_ids() {
        let temp = tempdir().expect("tempdir");
        let backend = test_vm_backend(temp.path());
        let persona = PersonaEntry {
            persona_id: "../escape".to_string(),
            browser_fingerprint: Some(json!({"seeded": true})),
            ..PersonaEntry::default()
        };

        let result = backend.resolve_persona_package_path(&persona);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("cannot contain path separators"));
    }
}
