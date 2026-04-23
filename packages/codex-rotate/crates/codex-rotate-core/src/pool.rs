use std::collections::{hash_map::DefaultHasher, HashSet};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::auth::{
    decode_jwt_payload, extract_account_id_from_auth, extract_account_id_from_token,
    is_token_expired, load_codex_auth, summarize_codex_auth, write_codex_auth, AuthSummary,
    CodexAuth,
};
use crate::paths::resolve_paths;
use crate::persona::get_persona_profile;
use crate::quota::{
    describe_quota_blocker, format_compact_quota, get_quota_left, has_usable_quota,
    quota_next_refresh_at, UsageCredits, UsageResponse, UsageWindow,
};
#[cfg(test)]
use crate::state::write_rotate_state_json;
use crate::state::{load_rotate_state_json, update_rotate_state_json, RotateStateOwner};
use crate::workflow::{
    auto_disable_domain_for_account, cmd_create, cmd_create_with_progress,
    create_next_fallback_options, extract_email_domain,
    family_suspends_domain_on_terminal_refresh_failure,
    is_auto_create_retry_stopped_for_reusable_account, load_disabled_rotation_domains,
    reconcile_added_account_credential_state, record_removed_account,
};

const DEFAULT_OAUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const DEBUG_POOL_DRIFT_ENV: &str = "CODEX_ROTATE_DEBUG_POOL_DRIFT";
const OAUTH_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const WHAM_USAGE_URL: &str = "https://chatgpt.com/backend-api/wham/usage";
const REQUEST_TIMEOUT_SECONDS: u64 = 8;

const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const CYAN: &str = "\x1b[36m";
const RESET: &str = "\x1b[0m";

fn debug_pool_drift_enabled() -> bool {
    std::env::var(DEBUG_POOL_DRIFT_ENV)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn debug_prepare_pool_state(label: &str, pool: &Pool) {
    if !debug_pool_drift_enabled() {
        return;
    }

    let active_email = pool
        .accounts
        .get(pool.active_index)
        .map(|entry| entry.email.clone());
    eprintln!(
        "codex-rotate core debug [{label}] active_index={} active_email={:?} account_count={}",
        pool.active_index,
        active_email,
        pool.accounts.len()
    );
}

fn account_rotation_enabled(disabled_domains: &HashSet<String>, email: &str) -> bool {
    extract_email_domain(email)
        .map(|domain| !disabled_domains.contains(&domain))
        .unwrap_or(true)
}

fn inventory_account_visible(disabled_domains: &HashSet<String>, entry: &AccountEntry) -> bool {
    !account_requires_terminal_cleanup(entry)
        && (account_rotation_enabled(disabled_domains, &entry.email)
            || entry.last_quota_usable != Some(true))
}

fn normalize_cached_quota_usability(entry: &mut AccountEntry) -> bool {
    let mut changed = false;
    if matches!(entry.last_quota_usable, Some(true))
        && cached_quota_indicates_unusable(entry).unwrap_or(false)
    {
        entry.last_quota_usable = Some(false);
        changed = true;
    }
    changed
}

fn cached_quota_indicates_unusable(entry: &AccountEntry) -> Option<bool> {
    if entry.last_quota_blocker.is_some() {
        return Some(true);
    }
    let summary = entry.last_quota_summary.as_deref()?;
    Some(cached_summary_has_exhausted_window(summary))
}

fn cached_summary_has_exhausted_window(summary: &str) -> bool {
    let normalized = summary.to_ascii_lowercase();
    let tokens = normalized.split_whitespace().collect::<Vec<_>>();
    tokens
        .windows(2)
        .filter_map(|window| {
            let [percent, label] = window else {
                return None;
            };
            let percent = percent.trim_end_matches([',', ';', ')']);
            let label = label.trim_matches(|ch: char| !ch.is_ascii_alphabetic());
            if label != "left" || !percent.ends_with('%') {
                return None;
            }
            percent
                .trim_end_matches('%')
                .parse::<f64>()
                .ok()
                .map(|value| value <= 0.0)
        })
        .any(|is_zero| is_zero)
}

fn is_terminal_refresh_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("refresh_token_invalidated")
        || message.contains("refresh token has been invalidated")
        || message.contains("refresh token already rotated")
        || message.contains("refresh_token_reused")
}

fn account_requires_terminal_cleanup(entry: &AccountEntry) -> bool {
    entry
        .last_quota_blocker
        .as_deref()
        .map(is_terminal_refresh_error)
        .unwrap_or(false)
}

fn cleanup_terminal_account(pool: &mut Pool, index: usize) -> Result<bool> {
    let Some(entry) = pool.accounts.get(index).cloned() else {
        return Ok(false);
    };
    let should_disable_domain = family_suspends_domain_on_terminal_refresh_failure(&entry.email)?;
    let deleted = record_removed_account(&entry.email)?;
    if deleted && should_disable_domain {
        auto_disable_domain_for_account(&entry.email)?;
    }
    Ok(deleted)
}

fn prune_terminal_accounts_from_pool(pool: &mut Pool) -> Result<bool> {
    let mut indices = pool
        .accounts
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| account_requires_terminal_cleanup(entry).then_some(index))
        .collect::<Vec<_>>();
    indices.sort_unstable();
    indices.dedup();
    let mut changed = false;
    for index in indices.into_iter().rev() {
        changed |= cleanup_terminal_account(pool, index)?;
    }
    Ok(changed)
}

fn disabled_rotation_target_error(domains: &[String]) -> anyhow::Error {
    let listed = domains.join(", ");
    let key_hint = if domains.len() == 1 {
        format!("domain[\"{}\"].rotation_enabled", domains[0])
    } else {
        "domain[\"<domain>\"].rotation_enabled".to_string()
    };
    anyhow!(
        "No rotation target is available because rotation is disabled for {} account(s). Set {} to true in ~/.codex-rotate/accounts.json to re-enable them.",
        listed,
        key_hint
    )
}

fn disabled_rotation_domains_for_pool(
    pool: &Pool,
    disabled_domains: &HashSet<String>,
    exclude_index: Option<usize>,
) -> Vec<String> {
    let mut domains = pool
        .accounts
        .iter()
        .enumerate()
        .filter(|(index, _)| Some(*index) != exclude_index)
        .filter_map(|(_, entry)| extract_email_domain(&entry.email))
        .filter(|domain| disabled_domains.contains(domain))
        .collect::<Vec<_>>();
    domains.sort();
    domains.dedup();
    domains
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Pool {
    pub active_index: usize,
    pub accounts: Vec<AccountEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AccountEntry {
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    pub email: String,
    pub account_id: String,
    pub plan_type: String,
    pub auth: CodexAuth,
    pub added_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_quota_usable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_quota_summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_quota_blocker: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_quota_checked_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_quota_primary_left_percent: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_quota_next_refresh_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persona: Option<PersonaEntry>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PersonaEntry {
    pub persona_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persona_profile_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_region_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_root_rel_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vm_package_rel_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub browser_fingerprint: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PersonaProfile {
    pub id: String,
    pub os_family: String,
    pub user_agent: String,
    pub accept_language: String,
    pub timezone: String,
    pub screen_width: u32,
    pub screen_height: u32,
    pub device_scale_factor: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub browser_fingerprint: Option<serde_json::Value>,
}

pub static PERSONA_PROFILES: OnceLock<Vec<PersonaProfile>> = OnceLock::new();

pub fn get_persona_profiles() -> &'static [PersonaProfile] {
    PERSONA_PROFILES.get_or_init(|| {
        [
            "balanced-us-compact",
            "balanced-eu-wide",
            "balanced-apac-standard",
        ]
        .iter()
        .map(|id| {
            let profile = get_persona_profile(id);
            PersonaProfile {
                id: profile.persona_profile_id,
                os_family: serde_json::to_string(&profile.os_family)
                    .unwrap_or_else(|_| "\"macos\"".to_string())
                    .trim_matches('"')
                    .to_string(),
                user_agent: profile.browser.user_agent,
                accept_language: profile.language,
                timezone: profile.timezone,
                screen_width: profile.vm_hardware.screen_width,
                screen_height: profile.vm_hardware.screen_height,
                device_scale_factor: profile.device_scale_factor,
                browser_fingerprint: None,
            }
        })
        .collect()
    })
}

pub fn resolve_persona_profile(
    profile_id: &str,
    browser_fingerprint: Option<serde_json::Value>,
) -> Option<PersonaProfile> {
    get_persona_profiles()
        .iter()
        .find(|profile| profile.id == profile_id)
        .map(|profile| PersonaProfile {
            browser_fingerprint,
            ..profile.clone()
        })
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RotationEnvironment {
    #[default]
    Host,
    Vm,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VmExpectedEgressMode {
    #[default]
    ProvisionOnly,
    Validate,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct VmEnvironmentConfig {
    pub base_package_path: Option<String>,
    pub persona_root: Option<String>,
    pub utm_app_path: Option<String>,
    pub bridge_root: Option<String>,
    pub expected_egress_mode: VmExpectedEgressMode,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RotationEnvironmentSettings {
    pub environment: RotationEnvironment,
    pub vm: Option<VmEnvironmentConfig>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RotationCheckpointPhase {
    #[default]
    Prepare,
    Export,
    Activate,
    Import,
    Commit,
    Rollback,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct RotationCheckpoint {
    pub phase: RotationCheckpointPhase,
    pub previous_index: usize,
    pub target_index: usize,
    pub previous_account_id: String,
    pub target_account_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedRotationAction {
    Switch,
    Stay,
    CreateRequired,
}

#[derive(Clone, Debug)]
pub struct PreparedRotation {
    pub action: PreparedRotationAction,
    pub pool: Pool,
    pub previous_index: usize,
    pub target_index: usize,
    pub previous: AccountEntry,
    pub target: AccountEntry,
    pub message: String,
    pub persist_pool: bool,
}

#[derive(Clone, Debug)]
pub struct AccountInspection {
    pub usage: Option<UsageResponse>,
    pub error: Option<String>,
    pub updated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolOverview {
    pub inventory_count: usize,
    pub inventory_active_slot: Option<usize>,
    pub inventory_healthy_count: usize,
}

#[derive(Clone, Debug)]
pub struct RotationCandidate {
    pub index: usize,
    pub entry: AccountEntry,
    pub inspection: AccountInspection,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum NextResult {
    Rotated {
        message: String,
        summary: AuthSummary,
    },
    Stayed {
        message: String,
        summary: AuthSummary,
    },
    Created {
        output: String,
        summary: AuthSummary,
    },
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct RotationEnvironmentState {
    environment: RotationEnvironment,
    vm: Option<VmEnvironmentConfig>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReusableAccountProbeMode {
    CurrentFirst,
    OthersFirst,
    OthersOnly,
}

#[derive(Clone, Debug, Deserialize)]
struct OAuthTokenResponse {
    access_token: Option<String>,
    id_token: Option<String>,
    refresh_token: Option<String>,
}

#[derive(Debug)]
struct HttpError {
    status: u16,
    message: String,
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for HttpError {}

struct LineEmitter<'a> {
    writer: Option<&'a mut dyn Write>,
    lines: Vec<String>,
}

impl<'a> LineEmitter<'a> {
    fn buffered() -> Self {
        Self {
            writer: None,
            lines: Vec::new(),
        }
    }

    fn streaming(writer: &'a mut dyn Write) -> Self {
        Self {
            writer: Some(writer),
            lines: Vec::new(),
        }
    }

    fn push_line(&mut self, line: impl Into<String>) -> Result<()> {
        let line = line.into();
        if let Some(writer) = self.writer.as_deref_mut() {
            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
            writer.flush()?;
        } else {
            self.lines.push(line);
        }
        Ok(())
    }

    fn finish(self) -> String {
        self.lines.join("\n")
    }
}

pub fn cmd_add(alias: Option<&str>) -> Result<String> {
    let paths = resolve_paths()?;
    let auth = load_codex_auth(&paths.codex_auth_file)?;
    let mut pool = load_pool()?;
    let account_id = extract_account_id_from_auth(&auth);
    let email = extract_email_from_auth(&auth);
    let plan_type = extract_plan_from_auth(&auth);
    let label = build_account_label(&email, &plan_type);
    let next_alias = normalize_alias(alias);

    let existing_index = pool
        .accounts
        .iter()
        .position(|account| account.label == label);
    let duplicate_index = pool
        .accounts
        .iter()
        .position(|account| account_entry_matches_auth_identity(account, &auth));

    if let Some(existing_index) = existing_index {
        let previous_account_id = pool.accounts[existing_index].account_id.clone();
        apply_auth_to_account(&mut pool.accounts[existing_index], auth.clone());
        if let Some(alias) = next_alias.clone() {
            if alias != pool.accounts[existing_index].label {
                pool.accounts[existing_index].alias = Some(alias);
            }
        } else if pool.accounts[existing_index].alias.as_deref()
            == Some(pool.accounts[existing_index].label.as_str())
        {
            pool.accounts[existing_index].alias = None;
        }

        if previous_account_id != account_id {
            // Keep warning behavior in stdout response text.
        }

        if let Some(duplicate_index) = duplicate_index {
            if duplicate_index != existing_index {
                pool.accounts.remove(duplicate_index);
                if duplicate_index < existing_index && pool.active_index > 0 {
                    pool.active_index -= 1;
                }
            }
        }

        pool.active_index = pool
            .accounts
            .iter()
            .position(|account| account.label == label)
            .unwrap_or(existing_index);
        save_pool(&pool)?;
        let _ = reconcile_added_account_credential_state(&pool.accounts[pool.active_index])?;
        return Ok(format!(
            "{GREEN}OK{RESET} Updated account \"{}\" ({}{})",
            label,
            pool.accounts[pool.active_index].email,
            pool.accounts[pool.active_index]
                .alias
                .as_ref()
                .map(|value| format!(", alias {value}"))
                .unwrap_or_default()
        ));
    }

    if let Some(duplicate_index) = duplicate_index {
        let previous_label = pool.accounts[duplicate_index].label.clone();
        apply_auth_to_account(&mut pool.accounts[duplicate_index], auth.clone());
        if let Some(alias) = next_alias.clone() {
            if alias != pool.accounts[duplicate_index].label {
                pool.accounts[duplicate_index].alias = Some(alias);
            }
        } else if pool.accounts[duplicate_index].alias.as_deref()
            == Some(pool.accounts[duplicate_index].label.as_str())
        {
            pool.accounts[duplicate_index].alias = None;
        }

        pool.active_index = duplicate_index;
        save_pool(&pool)?;
        let _ = reconcile_added_account_credential_state(&pool.accounts[duplicate_index])?;
        return Ok(format!(
            "{GREEN}OK{RESET} Updated account \"{}\" ({}){}",
            pool.accounts[duplicate_index].label,
            pool.accounts[duplicate_index].email,
            if previous_label != pool.accounts[duplicate_index].label {
                format!(
                    "\n{YELLOW}WARN{RESET} Account moved from \"{}\".",
                    previous_label
                )
            } else {
                String::new()
            }
        ));
    }

    pool.accounts.push(AccountEntry {
        label: label.clone(),
        alias: next_alias.filter(|value| value != &label),
        email: email.clone(),
        account_id,
        plan_type: plan_type.clone(),
        auth,
        added_at: now_iso(),
        last_quota_usable: None,
        last_quota_summary: None,
        last_quota_blocker: None,
        last_quota_checked_at: None,
        last_quota_primary_left_percent: None,
        last_quota_next_refresh_at: None,
        persona: None,
    });
    pool.active_index = pool.accounts.len() - 1;
    save_pool(&pool)?;
    let _ = reconcile_added_account_credential_state(&pool.accounts[pool.active_index])?;
    Ok(format!(
        "{GREEN}OK{RESET} Added account \"{}\" ({}, {}{}) - pool now has {} account(s)",
        label,
        email,
        plan_type,
        pool.accounts[pool.active_index]
            .alias
            .as_ref()
            .map(|value| format!(", alias {value}"))
            .unwrap_or_default(),
        pool.accounts.len()
    ))
}

pub fn cmd_add_expected_email(expected_email: &str, alias: Option<&str>) -> Result<String> {
    let _ = cmd_add(alias)?;

    let normalized_expected_email = expected_email.trim().to_lowercase();
    if normalized_expected_email.is_empty() {
        return Err(anyhow!("Expected email for pool add cannot be empty."));
    }

    let paths = resolve_paths()?;
    let auth = load_codex_auth(&paths.codex_auth_file)?;
    let account_id = extract_account_id_from_auth(&auth);
    let plan_type = extract_plan_from_auth(&auth);
    let expected_label = build_account_label(&normalized_expected_email, &plan_type);
    let next_alias = normalize_alias(alias).filter(|value| value != &expected_label);

    let mut pool = load_pool()?;
    let index = find_pool_account_index_by_identity(
        &pool,
        &account_id,
        &normalized_expected_email,
        &plan_type,
    )
    .ok_or_else(|| {
        anyhow!(
            "Added auth for {}, but could not find the corresponding pool entry.",
            normalized_expected_email
        )
    })?;

    let entry = &mut pool.accounts[index];
    let changed = entry.email != normalized_expected_email
        || entry.label != expected_label
        || entry.alias != next_alias;
    entry.email = normalized_expected_email.clone();
    entry.label = expected_label.clone();
    entry.alias = next_alias;
    pool.active_index = index;
    if changed {
        save_pool(&pool)?;
    }
    let _ = reconcile_added_account_credential_state(&pool.accounts[index])?;

    Ok(format!(
        "{GREEN}OK{RESET} Updated account \"{}\" ({}){}",
        pool.accounts[index].label,
        pool.accounts[index].email,
        pool.accounts[index]
            .alias
            .as_ref()
            .map(|value| format!(", alias {value}"))
            .unwrap_or_default()
    ))
}

pub fn cmd_next() -> Result<String> {
    cmd_next_with_progress(None)
}

pub fn prepare_next_rotation_with_progress(
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<PreparedRotation> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    debug_prepare_pool_state("prepare_next.loaded", &pool);
    let mut dirty = normalize_pool_entries(&mut pool);
    debug_prepare_pool_state("prepare_next.normalized", &pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    debug_prepare_pool_state("prepare_next.synced_auth", &pool);
    dirty |= prune_terminal_accounts_from_pool(&mut pool)?;
    debug_prepare_pool_state("prepare_next.pruned", &pool);
    if pool.accounts.is_empty() {
        if dirty {
            save_pool(&pool)?;
        }
        return Err(anyhow!("No accounts in pool. Run: codex-rotate add"));
    }
    let disabled_domains = load_disabled_rotation_domains()?;

    let previous_index = pool.active_index;
    let previous = pool.accounts[previous_index].clone();
    let mut cursor_index = previous_index;
    let mut inspected_later_indices = HashSet::new();
    let mut round_robin_steps = 0usize;

    while round_robin_steps < pool.accounts.len().saturating_sub(1) {
        let Some(candidate_index) =
            find_next_immediate_round_robin_index(cursor_index, &pool.accounts)
        else {
            break;
        };
        round_robin_steps += 1;
        if !account_rotation_enabled(&disabled_domains, &pool.accounts[candidate_index].email) {
            cursor_index = candidate_index;
            continue;
        }

        let inspection = inspect_account(
            &mut pool.accounts[candidate_index],
            &paths.codex_auth_file,
            false,
        )?;
        if debug_pool_drift_enabled() {
            eprintln!(
                "codex-rotate core debug [prepare_next.inspect] candidate_index={} candidate_email={} usable={:?} error={:?} summary={:?}",
                candidate_index,
                pool.accounts[candidate_index].email,
                inspection.usage.as_ref().map(has_usable_quota),
                inspection.error,
                inspection.usage.as_ref().map(format_compact_quota)
            );
        }
        dirty |= inspection.updated;
        if account_requires_terminal_cleanup(&pool.accounts[candidate_index]) {
            dirty |= cleanup_terminal_account(&mut pool, candidate_index)?;
            if pool.accounts.is_empty() {
                if dirty {
                    save_pool(&pool)?;
                }
                return Err(anyhow!("No accounts in pool. Run: codex-rotate add"));
            }
            cursor_index = previous_index.min(pool.accounts.len().saturating_sub(1));
            continue;
        }
        inspected_later_indices.insert(candidate_index);
        if inspection
            .usage
            .as_ref()
            .map(has_usable_quota)
            .unwrap_or(false)
        {
            let target = pool.accounts[candidate_index].clone();
            let previous_label = previous.label.clone();
            let previous_email = previous.email.clone();
            let target_label = target.label.clone();
            let target_email = target.email.clone();
            let target_plan_type = target.plan_type.clone();
            let total_accounts = pool.accounts.len();
            let quota_summary = inspection
                .usage
                .as_ref()
                .map(format_compact_quota)
                .unwrap_or_else(|| "quota unavailable".to_string());
            return Ok(PreparedRotation {
                action: PreparedRotationAction::Switch,
                pool,
                previous_index,
                target_index: candidate_index,
                previous,
                target,
                message: format!(
                    "{GREEN}ROTATE{RESET} {} ({}) -> {BOLD}{}{RESET} ({CYAN}{}{RESET}, {})\n{DIM}  [{}/{}] | {} | checked now{RESET}",
                    previous_label,
                    previous_email,
                    target_label,
                    target_email,
                    target_plan_type,
                    candidate_index + 1,
                    total_accounts,
                    quota_summary,
                ),
                persist_pool: dirty,
            });
        }

        cursor_index = candidate_index;
    }

    let mut reasons = Vec::new();
    let result = find_next_usable_account(
        &mut pool,
        &paths.codex_auth_file,
        ReusableAccountProbeMode::OthersFirst,
        &mut reasons,
        dirty,
        &inspected_later_indices,
        &disabled_domains,
    )?;
    dirty = result.1;

    if let Some(candidate) = result.0 {
        if candidate.index == previous_index {
            let current_label = previous.label.clone();
            let current_email = previous.email.clone();
            let current_plan_type = previous.plan_type.clone();
            let total_accounts = pool.accounts.len();
            let quota_summary = candidate
                .inspection
                .usage
                .as_ref()
                .map(format_compact_quota)
                .unwrap_or_else(|| "quota unavailable".to_string());
            return Ok(PreparedRotation {
                action: PreparedRotationAction::Stay,
                pool,
                previous_index,
                target_index: previous_index,
                previous: previous.clone(),
                target: previous,
                message: format!(
                    "{GREEN}ROTATE{RESET} Stayed on {BOLD}{}{RESET} ({CYAN}{}{RESET}, {})\n{DIM}  No other account has usable quota | [{}/{}] | {}{RESET}",
                    current_label,
                    current_email,
                    current_plan_type,
                    previous_index + 1,
                    total_accounts,
                    quota_summary,
                ),
                persist_pool: dirty,
            });
        }

        let target = candidate.entry.clone();
        let previous_label = previous.label.clone();
        let previous_email = previous.email.clone();
        let target_label = target.label.clone();
        let target_email = target.email.clone();
        let target_plan_type = target.plan_type.clone();
        let total_accounts = pool.accounts.len();
        let quota_summary = candidate
            .inspection
            .usage
            .as_ref()
            .map(format_compact_quota)
            .unwrap_or_else(|| "quota unavailable".to_string());
        return Ok(PreparedRotation {
            action: PreparedRotationAction::Switch,
            pool,
            previous_index,
            target_index: candidate.index,
            previous,
            target,
            message: format!(
                "{GREEN}ROTATE{RESET} {} ({}) -> {BOLD}{}{RESET} ({CYAN}{}{RESET}, {})\n{DIM}  [{}/{}] | {}{RESET}",
                previous_label,
                previous_email,
                target_label,
                target_email,
                target_plan_type,
                candidate.index + 1,
                total_accounts,
                quota_summary
            ),
            persist_pool: dirty,
        });
    }

    let previous_rotation_enabled =
        account_rotation_enabled(&disabled_domains, &pool.accounts[previous_index].email);
    let has_other_enabled_target = pool.accounts.iter().enumerate().any(|(index, entry)| {
        index != previous_index && account_rotation_enabled(&disabled_domains, &entry.email)
    });
    if !previous_rotation_enabled || !has_other_enabled_target {
        return Err(disabled_rotation_target_error(
            &disabled_rotation_domains_for_pool(&pool, &disabled_domains, Some(previous_index)),
        ));
    }

    Ok(PreparedRotation {
        action: PreparedRotationAction::CreateRequired,
        pool,
        previous_index,
        target_index: previous_index,
        previous: previous.clone(),
        target: previous,
        message: progress
            .as_ref()
            .map(|_| "Auto rotation is creating a replacement account.".to_string())
            .unwrap_or_else(|| {
                "Auto rotation requires creating a replacement account.".to_string()
            }),
        persist_pool: dirty,
    })
}

pub fn prepare_prev_rotation() -> Result<PreparedRotation> {
    let paths = resolve_paths()?;
    let disabled_domains = load_disabled_rotation_domains()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    if pool.accounts.is_empty() {
        return Err(anyhow!("No accounts in pool. Run: codex-rotate add"));
    }
    if pool.accounts.len() == 1 {
        if dirty {
            save_pool(&pool)?;
        }
        return Err(anyhow!(
            "Only 1 account in pool. Add more with: codex-rotate add"
        ));
    }

    let previous_index = pool.active_index;
    let Some(target_index) = (1..pool.accounts.len())
        .map(|offset| (pool.active_index + pool.accounts.len() - offset) % pool.accounts.len())
        .find(|index| account_rotation_enabled(&disabled_domains, &pool.accounts[*index].email))
    else {
        return Err(disabled_rotation_target_error(
            &disabled_rotation_domains_for_pool(&pool, &disabled_domains, Some(previous_index)),
        ));
    };
    let previous = pool.accounts[previous_index].clone();
    let target = pool.accounts[target_index].clone();
    let previous_label = previous.label.clone();
    let previous_email = previous.email.clone();
    let target_label = target.label.clone();
    let target_email = target.email.clone();
    let target_plan_type = target.plan_type.clone();
    let total_accounts = pool.accounts.len();
    Ok(PreparedRotation {
        action: PreparedRotationAction::Switch,
        pool,
        previous_index,
        target_index,
        previous: previous.clone(),
        target: target.clone(),
        message: format!(
            "{GREEN}ROTATE{RESET} {} ({}) -> {BOLD}{}{RESET} ({CYAN}{}{RESET}, {})\n{DIM}  [{}/{}]{RESET}",
            previous_label,
            previous_email,
            target_label,
            target_email,
            target_plan_type,
            target_index + 1,
            total_accounts,
        ),
        persist_pool: dirty,
    })
}

pub fn persist_prepared_rotation_pool(prepared: &PreparedRotation) -> Result<()> {
    let mut pool = prepared.pool.clone();
    pool.active_index = prepared
        .target_index
        .min(pool.accounts.len().saturating_sub(1));
    save_pool(&pool)
}

pub fn rollback_prepared_rotation(prepared: &PreparedRotation) -> Result<()> {
    let paths = resolve_paths()?;
    write_codex_auth(&paths.codex_auth_file, &prepared.previous.auth)?;
    restore_pool_active_index(prepared.previous_index)?;
    Ok(())
}

pub fn resolve_pool_account(selector: &str) -> Result<Option<AccountEntry>> {
    let pool = load_pool()?;
    match resolve_account_selector(&pool, selector) {
        Ok(selection) => Ok(Some(selection.entry)),
        Err(error) if error.to_string().contains("not found in pool") => Ok(None),
        Err(error) => Err(error),
    }
}

pub fn cmd_next_with_progress(
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<String> {
    match rotate_next_internal_with_progress(progress)? {
        NextResult::Rotated { message, .. }
        | NextResult::Stayed { message, .. }
        | NextResult::Created {
            output: message, ..
        } => Ok(message),
    }
}

pub fn rotate_next_internal() -> Result<NextResult> {
    rotate_next_internal_with_progress(None)
}

pub fn rotate_next_internal_with_progress(
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<NextResult> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    dirty |= prune_terminal_accounts_from_pool(&mut pool)?;
    if pool.accounts.is_empty() {
        if dirty {
            save_pool(&pool)?;
        }
        return Err(anyhow!("No accounts in pool. Run: codex-rotate add"));
    }
    let disabled_domains = load_disabled_rotation_domains()?;

    let previous_index = pool.active_index;
    let previous = pool.accounts[previous_index].clone();
    let mut cursor_index = previous_index;
    let mut inspected_later_indices = HashSet::new();
    let mut round_robin_steps = 0usize;

    while round_robin_steps < pool.accounts.len().saturating_sub(1) {
        let Some(candidate_index) =
            find_next_immediate_round_robin_index(cursor_index, &pool.accounts)
        else {
            break;
        };
        round_robin_steps += 1;
        if !account_rotation_enabled(&disabled_domains, &pool.accounts[candidate_index].email) {
            cursor_index = candidate_index;
            continue;
        }

        let inspection = inspect_account(
            &mut pool.accounts[candidate_index],
            &paths.codex_auth_file,
            false,
        )?;
        dirty |= inspection.updated;
        if account_requires_terminal_cleanup(&pool.accounts[candidate_index]) {
            dirty |= cleanup_terminal_account(&mut pool, candidate_index)?;
            if pool.accounts.is_empty() {
                if dirty {
                    save_pool(&pool)?;
                }
                return Err(anyhow!("No accounts in pool. Run: codex-rotate add"));
            }
            cursor_index = previous_index.min(pool.accounts.len().saturating_sub(1));
            continue;
        }
        inspected_later_indices.insert(candidate_index);
        if inspection
            .usage
            .as_ref()
            .map(has_usable_quota)
            .unwrap_or(false)
        {
            pool.active_index = candidate_index;
            write_codex_auth(&paths.codex_auth_file, &pool.accounts[candidate_index].auth)?;
            save_pool(&pool)?;
            let quota_summary = inspection
                .usage
                .as_ref()
                .map(format_compact_quota)
                .unwrap_or_else(|| "quota unavailable".to_string());
            let summary = summarize_codex_auth(&pool.accounts[candidate_index].auth);
            return Ok(NextResult::Rotated {
                summary,
                message: format!(
                    "{GREEN}ROTATE{RESET} {} ({}) -> {BOLD}{}{RESET} ({CYAN}{}{RESET}, {})\n{DIM}  [{}/{}] | {} | checked now{RESET}",
                    previous.label,
                    previous.email,
                    pool.accounts[candidate_index].label,
                    pool.accounts[candidate_index].email,
                    pool.accounts[candidate_index].plan_type,
                    pool.active_index + 1,
                    pool.accounts.len(),
                    quota_summary,
                ),
            });
        }

        cursor_index = candidate_index;
    }

    let mut reasons = Vec::new();
    let result = find_next_usable_account(
        &mut pool,
        &paths.codex_auth_file,
        ReusableAccountProbeMode::OthersFirst,
        &mut reasons,
        dirty,
        &inspected_later_indices,
        &disabled_domains,
    )?;
    dirty = result.1;

    if let Some(candidate) = result.0 {
        if candidate.index == previous_index {
            if dirty {
                save_pool(&pool)?;
            }
            let quota_summary = candidate
                .inspection
                .usage
                .as_ref()
                .map(format_compact_quota)
                .unwrap_or_else(|| "quota unavailable".to_string());
            return Ok(NextResult::Stayed {
                summary: summarize_codex_auth(&candidate.entry.auth),
                message: format!(
                    "{GREEN}ROTATE{RESET} Stayed on {BOLD}{}{RESET} ({CYAN}{}{RESET}, {})\n{DIM}  No other account has usable quota | [{}/{}] | {}{RESET}",
                    candidate.entry.label,
                    candidate.entry.email,
                    candidate.entry.plan_type,
                    pool.active_index + 1,
                    pool.accounts.len(),
                    quota_summary,
                ),
            });
        }

        pool.active_index = candidate.index;
        write_codex_auth(&paths.codex_auth_file, &candidate.entry.auth)?;
        save_pool(&pool)?;
        return Ok(NextResult::Rotated {
            summary: summarize_codex_auth(&candidate.entry.auth),
            message: format!(
                "{GREEN}ROTATE{RESET} {} ({}) -> {BOLD}{}{RESET} ({CYAN}{}{RESET}, {})\n{DIM}  [{}/{}] | {}{RESET}",
                previous.label,
                previous.email,
                candidate.entry.label,
                candidate.entry.email,
                candidate.entry.plan_type,
                pool.active_index + 1,
                pool.accounts.len(),
                candidate
                    .inspection
                    .usage
                    .as_ref()
                    .map(format_compact_quota)
                    .unwrap_or_else(|| "quota unavailable".to_string())
            ),
        });
    }

    let previous_rotation_enabled =
        account_rotation_enabled(&disabled_domains, &pool.accounts[previous_index].email);
    let has_other_enabled_target = pool.accounts.iter().enumerate().any(|(index, entry)| {
        index != previous_index && account_rotation_enabled(&disabled_domains, &entry.email)
    });
    if !previous_rotation_enabled || !has_other_enabled_target {
        return Err(disabled_rotation_target_error(
            &disabled_rotation_domains_for_pool(&pool, &disabled_domains, Some(previous_index)),
        ));
    }

    if dirty {
        save_pool(&pool)?;
    }
    let output = match progress.clone() {
        Some(progress) => cmd_create_with_progress(create_next_fallback_options(), Some(progress)),
        None => cmd_create(create_next_fallback_options()),
    };
    let output = match output {
        Ok(output) => output,
        Err(error) if is_auto_create_retry_stopped_for_reusable_account(&error) => {
            return rotate_next_internal_with_progress(progress);
        }
        Err(error) => return Err(error),
    };
    let auth = load_codex_auth(&paths.codex_auth_file)?;
    Ok(NextResult::Created {
        summary: summarize_codex_auth(&auth),
        output: output.trim_end().to_string(),
    })
}

pub fn cmd_prev() -> Result<String> {
    let paths = resolve_paths()?;
    let disabled_domains = load_disabled_rotation_domains()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    if pool.accounts.is_empty() {
        return Err(anyhow!("No accounts in pool. Run: codex-rotate add"));
    }
    if pool.accounts.len() == 1 {
        if dirty {
            save_pool(&pool)?;
        }
        return Err(anyhow!(
            "Only 1 account in pool. Add more with: codex-rotate add"
        ));
    }

    let previous_index = pool.active_index;
    let Some(next_index) = (1..pool.accounts.len())
        .map(|offset| (pool.active_index + pool.accounts.len() - offset) % pool.accounts.len())
        .find(|index| account_rotation_enabled(&disabled_domains, &pool.accounts[*index].email))
    else {
        return Err(disabled_rotation_target_error(
            &disabled_rotation_domains_for_pool(&pool, &disabled_domains, Some(previous_index)),
        ));
    };
    pool.active_index = next_index;
    let next = pool.accounts[pool.active_index].clone();
    write_codex_auth(&paths.codex_auth_file, &next.auth)?;
    save_pool(&pool)?;

    let previous = &pool.accounts[previous_index];
    Ok(format!(
        "{GREEN}ROTATE{RESET} {} ({}) -> {BOLD}{}{RESET} ({CYAN}{}{RESET}, {})\n{DIM}  [{}/{}]{RESET}",
        previous.label,
        previous.email,
        next.label,
        next.email,
        next.plan_type,
        pool.active_index + 1,
        pool.accounts.len(),
    ))
}

pub fn cmd_list() -> Result<String> {
    let mut emitter = LineEmitter::buffered();
    cmd_list_impl(&mut emitter)?;
    Ok(emitter.finish())
}

// TODO: expose a structured healthy-account list so callers can use this logic directly
// instead of scraping the rendered account-pool text.
pub fn cmd_list_stream(writer: &mut dyn Write) -> Result<()> {
    let mut emitter = LineEmitter::streaming(writer);
    cmd_list_impl(&mut emitter)
}

fn cmd_list_impl(output: &mut LineEmitter<'_>) -> Result<()> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let listed_at = Utc::now();
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    dirty |= prune_terminal_accounts_from_pool(&mut pool)?;
    if pool.accounts.is_empty() {
        output.push_line(format!(
            "{YELLOW}WARN{RESET} No accounts in pool. Add one with: codex-rotate add"
        ))?;
        if dirty {
            save_pool(&pool)?;
        }
        return Ok(());
    }
    let disabled_domains = load_disabled_rotation_domains()?;
    let refresh_order = build_list_quota_refresh_order(&pool, listed_at);
    let refresh_indices = refresh_order.into_iter().collect::<HashSet<_>>();
    let display_order = build_list_account_display_order(&pool);

    let mut usable_count = 0;
    let mut exhausted_count = 0;
    let mut unavailable_count = 0;
    let mut healthy_account_sections = Vec::new();
    output.push_line(String::new())?;
    let visible_count = pool
        .accounts
        .iter()
        .filter(|entry| inventory_account_visible(&disabled_domains, entry))
        .count();
    output.push_line(format!(
        "{BOLD}Codex OAuth Account Pool{RESET} ({} account(s))",
        visible_count
    ))?;
    output.push_line(String::new())?;
    output.push_line(format!("{BOLD}Total Accounts{RESET}"))?;
    output.push_line(String::new())?;

    for index in display_order {
        if index >= pool.accounts.len() {
            continue;
        }
        if !inventory_account_visible(&disabled_domains, &pool.accounts[index]) {
            continue;
        }
        let is_active = index == pool.active_index;
        let account_header_line = build_list_account_header_line(&pool.accounts[index], is_active);
        output.push_line(account_header_line.clone())?;

        if refresh_indices.contains(&index)
            && account_quota_refresh_due_for_list(&pool.accounts[index], listed_at)
        {
            let inspection =
                inspect_account(&mut pool.accounts[index], &paths.codex_auth_file, is_active)?;
            dirty |= inspection.updated;
            if account_requires_terminal_cleanup(&pool.accounts[index]) {
                dirty |= cleanup_terminal_account(&mut pool, index)?;
                continue;
            }
        }

        let quota_line = format_cached_quota_line(&pool.accounts[index]);
        let mut account_lines = vec![account_header_line];
        let account_detail_lines =
            build_list_account_detail_lines(&pool.accounts[index], &quota_line);
        for line in &account_detail_lines {
            output.push_line(line.clone())?;
        }
        account_lines.extend(account_detail_lines);

        let is_healthy = matches!(pool.accounts[index].last_quota_usable, Some(true));
        match pool.accounts[index].last_quota_usable {
            Some(true) => usable_count += 1,
            Some(false) => exhausted_count += 1,
            None => unavailable_count += 1,
        }
        if is_healthy {
            healthy_account_sections.push(account_lines);
        }
    }

    if dirty {
        save_pool(&pool)?;
    }
    if usable_count == 0 {
        let mut details = Vec::new();
        if exhausted_count > 0 {
            details.push(format!("{exhausted_count} exhausted"));
        }
        if unavailable_count > 0 {
            details.push(format!("{unavailable_count} unavailable"));
        }
        output.push_line(format!(
            "{YELLOW}WARN{RESET} All accounts are exhausted or unavailable{}.",
            if details.is_empty() {
                String::new()
            } else {
                format!(" ({})", details.join(", "))
            }
        ))?;
    }
    output.push_line(String::new())?;
    output.push_line(format!(
        "{BOLD}Healthy Accounts{RESET} ({} account(s))",
        usable_count
    ))?;
    output.push_line(String::new())?;
    if healthy_account_sections.is_empty() {
        output.push_line(format!("  {DIM}No healthy accounts.{RESET}"))?;
    } else {
        for account_lines in healthy_account_sections {
            for line in account_lines {
                output.push_line(line)?;
            }
        }
    }
    output.push_line(String::new())?;
    Ok(())
}

fn build_list_account_header_line(entry: &AccountEntry, is_active: bool) -> String {
    let label = if is_active {
        format!("{BOLD}{}{RESET}", entry.label)
    } else {
        entry.label.clone()
    };
    format!(
        "  {} {}  {CYAN}{}{RESET}  {DIM}{}{RESET}  {DIM}{}{RESET}",
        if is_active {
            format!("{GREEN}>{RESET}")
        } else {
            " ".to_string()
        },
        label,
        entry.email,
        entry.plan_type,
        format_short_account_id(&entry.account_id)
    )
}

fn build_list_account_detail_lines(entry: &AccountEntry, quota_line: &str) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(alias) = entry.alias.as_ref() {
        lines.push(format!("    {DIM}alias{RESET}  {}", alias));
    }
    let quota_detail_line = if let Some(next_refresh_at) = format_list_quota_refresh_eta(entry) {
        format!(
            "    {DIM}quota{RESET}  {} {DIM}| next refresh{RESET} {}",
            quota_line, next_refresh_at
        )
    } else {
        format!("    {DIM}quota{RESET}  {}", quota_line)
    };
    lines.push(quota_detail_line);
    lines
}

fn format_cached_quota_line(entry: &AccountEntry) -> String {
    let checked_suffix = entry
        .last_quota_checked_at
        .as_deref()
        .map(|value| format!(" {DIM}(cached {value}){RESET}"))
        .unwrap_or_default();

    if let Some(summary) = entry.last_quota_summary.as_deref() {
        return format!("{summary}{checked_suffix}");
    }

    if let Some(blocker) = entry.last_quota_blocker.as_deref() {
        return format!("unavailable ({blocker}){checked_suffix}");
    }

    if entry.last_quota_checked_at.is_some() {
        return format!("unavailable (quota probe failed){checked_suffix}");
    }

    "unknown (run codex-rotate status or rotate to refresh)".to_string()
}

fn format_list_quota_refresh_eta(entry: &AccountEntry) -> Option<String> {
    effective_cached_quota_next_refresh_at(entry)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn build_list_account_display_order(pool: &Pool) -> Vec<usize> {
    let mut indices = (0..pool.accounts.len()).collect::<Vec<_>>();
    indices.sort_by(|left, right| {
        let left_eta = effective_cached_quota_next_refresh_at(&pool.accounts[*left]);
        let right_eta = effective_cached_quota_next_refresh_at(&pool.accounts[*right]);
        left_eta
            .is_none()
            .cmp(&right_eta.is_none())
            .then_with(|| left_eta.cmp(&right_eta))
            .then_with(|| left.cmp(right))
    });
    indices
}

fn build_list_quota_refresh_order(pool: &Pool, now: DateTime<Utc>) -> Vec<usize> {
    let mut refreshes = pool
        .accounts
        .iter()
        .enumerate()
        .filter(|(_, entry)| entry.last_quota_checked_at.is_none())
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    refreshes.sort_by(|left, right| {
        let left_priority = if *left == pool.active_index { 0 } else { 1 };
        let right_priority = if *right == pool.active_index { 0 } else { 1 };
        left_priority
            .cmp(&right_priority)
            .then_with(|| left.cmp(right))
    });

    let mut candidates = pool
        .accounts
        .iter()
        .enumerate()
        .filter(|(_, entry)| entry.last_quota_checked_at.is_some())
        .filter(|(_, entry)| cached_quota_state_is_stale(entry, now))
        .map(|(index, entry)| {
            let priority = if index == pool.active_index {
                0
            } else if entry.last_quota_usable == Some(true) {
                1
            } else {
                2
            };
            (index, priority, cached_quota_checked_at(entry))
        })
        .collect::<Vec<_>>();

    candidates.sort_by(|left, right| {
        left.1
            .cmp(&right.1)
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| left.0.cmp(&right.0))
    });

    refreshes.extend(candidates.into_iter().map(|(index, _, _)| index));

    refreshes
}

fn cached_quota_state_is_stale(entry: &AccountEntry, now: DateTime<Utc>) -> bool {
    let Some(next_refresh_at) = effective_cached_quota_next_refresh_at(entry) else {
        return true;
    };
    now >= next_refresh_at
}

fn account_quota_refresh_due_for_list(entry: &AccountEntry, now: DateTime<Utc>) -> bool {
    entry.last_quota_checked_at.is_none() || cached_quota_state_is_stale(entry, now)
}

fn cached_quota_checked_at(entry: &AccountEntry) -> Option<DateTime<Utc>> {
    entry
        .last_quota_checked_at
        .as_deref()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn cached_quota_next_refresh_at(entry: &AccountEntry) -> Option<DateTime<Utc>> {
    entry
        .last_quota_next_refresh_at
        .as_deref()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn effective_cached_quota_next_refresh_at(entry: &AccountEntry) -> Option<DateTime<Utc>> {
    if let Some(next_refresh_at) = cached_quota_next_refresh_at(entry) {
        return Some(next_refresh_at);
    }
    let checked_at = cached_quota_checked_at(entry)?;
    legacy_cached_quota_next_refresh_at(entry, checked_at)
        .or_else(|| Some(checked_at + cached_quota_refresh_interval(entry)))
}

fn cached_quota_refresh_interval(entry: &AccountEntry) -> Duration {
    match entry.last_quota_usable {
        Some(true) => match entry.last_quota_primary_left_percent.unwrap_or(0) {
            value if value > 20 => Duration::seconds(60),
            value if value > 10 => Duration::seconds(30),
            _ => Duration::seconds(15),
        },
        Some(false) | None => Duration::seconds(15),
    }
}

fn legacy_cached_quota_next_refresh_at(
    entry: &AccountEntry,
    checked_at: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    if entry.last_quota_usable != Some(false) {
        return None;
    }
    let blocker = entry.last_quota_blocker.as_deref()?;
    let reset_text = blocker.split("resets in ").nth(1)?.trim();
    Some(checked_at + parse_compact_duration(reset_text)?)
}

fn parse_compact_duration(value: &str) -> Option<Duration> {
    let mut seconds = 0_i64;
    for part in value.split_whitespace() {
        if part.len() < 2 {
            return None;
        }
        let (amount, unit) = part.split_at(part.len() - 1);
        let amount = amount.parse::<i64>().ok()?;
        seconds += match unit {
            "d" => amount.saturating_mul(86_400),
            "h" => amount.saturating_mul(3_600),
            "m" => amount.saturating_mul(60),
            "s" => amount,
            _ => return None,
        };
    }
    Some(Duration::seconds(seconds))
}

pub fn cmd_status() -> Result<String> {
    let mut emitter = LineEmitter::buffered();
    cmd_status_impl(&mut emitter)?;
    Ok(emitter.finish())
}

pub fn current_pool_overview() -> Result<PoolOverview> {
    current_pool_overview_with_activation(true)
}

pub fn current_pool_overview_without_activation() -> Result<PoolOverview> {
    current_pool_overview_with_activation(false)
}

fn current_pool_overview_with_activation(activate_current: bool) -> Result<PoolOverview> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |=
        sync_pool_current_auth_from_codex(&mut pool, &paths.codex_auth_file, activate_current)?;
    dirty |= prune_terminal_accounts_from_pool(&mut pool)?;
    if dirty {
        save_pool(&pool)?;
    }
    let disabled_domains = load_disabled_rotation_domains()?;
    let visible_indices = pool
        .accounts
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| {
            inventory_account_visible(&disabled_domains, entry).then_some(index)
        })
        .collect::<Vec<_>>();
    Ok(PoolOverview {
        inventory_count: visible_indices.len(),
        inventory_active_slot: visible_indices
            .iter()
            .position(|index| *index == pool.active_index)
            .map(|slot| slot.saturating_add(1)),
        inventory_healthy_count: visible_indices
            .iter()
            .filter(|index| pool.accounts[**index].last_quota_usable == Some(true))
            .count(),
    })
}

pub fn cmd_status_stream(writer: &mut dyn Write) -> Result<()> {
    let mut emitter = LineEmitter::streaming(writer);
    cmd_status_impl(&mut emitter)
}

fn cmd_status_impl(output: &mut LineEmitter<'_>) -> Result<()> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    dirty |= prune_terminal_accounts_from_pool(&mut pool)?;
    let mut live_pool_index = None;

    output.push_line(String::new())?;
    output.push_line(format!("{BOLD}Codex Rotate Status{RESET}"))?;
    output.push_line(String::new())?;

    if paths.codex_auth_file.exists() {
        let auth = load_codex_auth(&paths.codex_auth_file)?;
        let email = extract_email_from_auth(&auth);
        let plan = extract_plan_from_auth(&auth);
        let account_id = extract_account_id_from_auth(&auth);
        output.push_line(format!(
            "  {BOLD}Auth file target:{RESET} {CYAN}{}{RESET}  ({})",
            email, plan
        ))?;
        output.push_line(format!("  {BOLD}Account ID:{RESET}       {}", account_id))?;
        output.push_line(format!(
            "  {BOLD}Last refresh:{RESET}     {}",
            auth.last_refresh
        ))?;

        live_pool_index = find_pool_account_index_by_identity(&pool, &account_id, &email, &plan);

        if let Some(index) = live_pool_index {
            let inspection =
                inspect_account(&mut pool.accounts[index], &paths.codex_auth_file, true)?;
            dirty |= inspection.updated;
            if account_requires_terminal_cleanup(&pool.accounts[index]) {
                dirty |= cleanup_terminal_account(&mut pool, index)?;
                live_pool_index = None;
                output.push_line(format!(
                    "  {BOLD}Quota:{RESET}            unavailable ({})",
                    inspection
                        .error
                        .unwrap_or_else(|| "unknown error".to_string())
                ))?;
            } else if let Some(usage) = inspection.usage.as_ref() {
                if let Some(window) = usage
                    .rate_limit
                    .as_ref()
                    .and_then(|limits| limits.primary_window.as_ref())
                {
                    output.push_line(format!(
                        "  {BOLD}Quota (5h):{RESET}       {}",
                        format_usage_window(window)
                    ))?;
                }
                if let Some(window) = usage
                    .rate_limit
                    .as_ref()
                    .and_then(|limits| limits.secondary_window.as_ref())
                {
                    output.push_line(format!(
                        "  {BOLD}Quota (week):{RESET}     {}",
                        format_usage_window(window)
                    ))?;
                }
                if let Some(window) = usage
                    .code_review_rate_limit
                    .as_ref()
                    .and_then(|limits| limits.primary_window.as_ref())
                {
                    output.push_line(format!(
                        "  {BOLD}Code review:{RESET}      {}",
                        format_usage_window(window)
                    ))?;
                }
                if let Some(credits) = format_credits_full(usage.credits.as_ref()) {
                    output.push_line(format!("  {BOLD}Credits:{RESET}          {}", credits))?;
                }
            } else {
                output.push_line(format!(
                    "  {BOLD}Quota:{RESET}            unavailable ({})",
                    inspection
                        .error
                        .unwrap_or_else(|| "unknown error".to_string())
                ))?;
            }
        } else {
            match fetch_usage_with_recovery(&auth) {
                Ok((refreshed_auth, usage, refreshed)) => {
                    if refreshed {
                        write_codex_auth(&paths.codex_auth_file, &refreshed_auth)?;
                    }
                    if let Some(window) = usage
                        .rate_limit
                        .as_ref()
                        .and_then(|limits| limits.primary_window.as_ref())
                    {
                        output.push_line(format!(
                            "  {BOLD}Quota (5h):{RESET}       {}",
                            format_usage_window(window)
                        ))?;
                    }
                    if let Some(window) = usage
                        .rate_limit
                        .as_ref()
                        .and_then(|limits| limits.secondary_window.as_ref())
                    {
                        output.push_line(format!(
                            "  {BOLD}Quota (week):{RESET}     {}",
                            format_usage_window(window)
                        ))?;
                    }
                    if let Some(window) = usage
                        .code_review_rate_limit
                        .as_ref()
                        .and_then(|limits| limits.primary_window.as_ref())
                    {
                        output.push_line(format!(
                            "  {BOLD}Code review:{RESET}      {}",
                            format_usage_window(window)
                        ))?;
                    }
                    if let Some(credits) = format_credits_full(usage.credits.as_ref()) {
                        output
                            .push_line(format!("  {BOLD}Credits:{RESET}          {}", credits))?;
                    }
                }
                Err(error) => {
                    output.push_line(format!(
                        "  {BOLD}Quota:{RESET}            unavailable ({})",
                        error
                    ))?;
                }
            }
        }
    } else {
        output.push_line(format!("{YELLOW}WARN{RESET} No Codex auth file found."))?;
    }

    output.push_line(format!(
        "\n  {BOLD}Pool file:{RESET}        {}",
        paths.pool_file.display()
    ))?;
    output.push_line(format!(
        "  {BOLD}Pool size:{RESET}        {} account(s)",
        pool.accounts.len()
    ))?;

    if let Some(index) = live_pool_index {
        if let Some(active) = pool.accounts.get(index) {
            output.push_line(format!(
                "  {BOLD}Active slot:{RESET}      {} [{}/{}]",
                active.label,
                index + 1,
                pool.accounts.len()
            ))?;
            if let Some(alias) = &active.alias {
                output.push_line(format!("  {BOLD}Active alias:{RESET}     {}", alias))?;
            }
        }
    } else if paths.codex_auth_file.exists() {
        output.push_line(format!(
            "  {BOLD}Active slot:{RESET}      {YELLOW}not in pool{RESET}"
        ))?;
        if let Some(active) = pool.accounts.get(pool.active_index) {
            output.push_line(format!(
                "  {BOLD}Pool pointer:{RESET}     {} [{}/{}]",
                active.label,
                pool.active_index + 1,
                pool.accounts.len()
            ))?;
            if let Some(alias) = &active.alias {
                output.push_line(format!("  {BOLD}Pointer alias:{RESET}    {}", alias))?;
            }
        }
    } else if let Some(active) = pool.accounts.get(pool.active_index) {
        output.push_line(format!(
            "  {BOLD}Active slot:{RESET}      {} [{}/{}]",
            active.label,
            pool.active_index + 1,
            pool.accounts.len()
        ))?;
        if let Some(alias) = &active.alias {
            output.push_line(format!("  {BOLD}Active alias:{RESET}     {}", alias))?;
        }
    }

    if dirty {
        save_pool(&pool)?;
    }
    output.push_line(String::new())?;
    Ok(())
}

pub fn cmd_remove(selector: &str) -> Result<String> {
    if selector.trim().is_empty() {
        return Err(anyhow!("Usage: codex-rotate remove <selector>"));
    }
    let mut pool = load_pool()?;
    let selection = resolve_account_selector(&pool, selector)?;
    let removed = pool.accounts.remove(selection.index);
    record_removed_account(&removed.email)?;
    if pool.accounts.is_empty() || pool.active_index >= pool.accounts.len() {
        pool.active_index = 0;
    }
    save_pool(&pool)?;
    Ok(format!(
        "{GREEN}OK{RESET} Removed \"{}\" ({}). Pool now has {} account(s).",
        get_account_summary(&removed),
        removed.email,
        pool.accounts.len()
    ))
}

pub fn current_auth_summary() -> Result<AuthSummary> {
    let paths = resolve_paths()?;
    let auth = load_codex_auth(&paths.codex_auth_file)?;
    Ok(summarize_codex_auth(&auth))
}

pub fn other_usable_account_exists() -> Result<bool> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    dirty |= prune_terminal_accounts_from_pool(&mut pool)?;

    if pool.accounts.len() <= 1 {
        if dirty {
            save_pool(&pool)?;
        }
        return Ok(false);
    }

    let disabled_domains = load_disabled_rotation_domains()?;
    let mut reasons = Vec::new();
    let skip_indices = HashSet::new();
    let (candidate, candidate_dirty) = find_next_usable_account(
        &mut pool,
        &paths.codex_auth_file,
        ReusableAccountProbeMode::OthersOnly,
        &mut reasons,
        dirty,
        &skip_indices,
        &disabled_domains,
    )?;
    if candidate_dirty {
        save_pool(&pool)?;
    }
    Ok(candidate.is_some())
}

pub fn load_pool() -> Result<Pool> {
    let state = load_rotate_state_json()?;
    let object = state.as_object().cloned().unwrap_or_default();
    let mut pool: Pool = serde_json::from_value(json!({
        "active_index": object.get("active_index").cloned().unwrap_or_else(|| Value::Number(0usize.into())),
        "accounts": object.get("accounts").cloned().unwrap_or_else(|| Value::Array(Vec::new())),
    }))
    .context("Invalid pool data in rotate state.")?;
    normalize_pool_entries(&mut pool);
    Ok(pool)
}

pub fn load_rotation_environment_settings() -> Result<RotationEnvironmentSettings> {
    let state = load_rotate_state_json()?;
    let parsed: RotationEnvironmentState =
        serde_json::from_value(state).context("Invalid environment config in rotate state.")?;
    Ok(RotationEnvironmentSettings {
        environment: parsed.environment,
        vm: parsed.vm,
    })
}

pub fn save_pool(pool: &Pool) -> Result<()> {
    let active_index = pool.active_index;
    let accounts = serde_json::to_value(&pool.accounts)?;
    update_rotate_state_json(RotateStateOwner::Pool, move |state| {
        if !state.is_object() {
            *state = Value::Object(Map::new());
        }
        let object = state
            .as_object_mut()
            .expect("rotate state must be a JSON object");
        object.insert(
            "active_index".to_string(),
            Value::Number(active_index.into()),
        );
        object.insert("accounts".to_string(), accounts.clone());
        Ok(())
    })
}

pub fn load_rotation_checkpoint() -> Result<Option<RotationCheckpoint>> {
    let state = load_rotate_state_json()?;
    let Some(rotation) = state.get("rotation") else {
        return Ok(None);
    };

    if rotation.is_null() {
        return Ok(None);
    }

    serde_json::from_value(rotation.clone())
        .map(Some)
        .context("Invalid rotation checkpoint in rotate state.")
}

pub fn save_rotation_checkpoint(checkpoint: Option<&RotationCheckpoint>) -> Result<()> {
    update_rotate_state_json(RotateStateOwner::FullState, move |state| {
        if !state.is_object() {
            *state = Value::Object(Map::new());
        }
        let object = state
            .as_object_mut()
            .expect("rotate state must be a JSON object");
        match checkpoint {
            Some(checkpoint) => {
                object.insert("rotation".to_string(), serde_json::to_value(checkpoint)?);
            }
            None => {
                object.remove("rotation");
            }
        }
        Ok(())
    })
}

pub fn write_selected_account_auth(entry: &AccountEntry) -> Result<()> {
    let paths = resolve_paths()?;
    let Some(parent) = paths.codex_auth_file.parent() else {
        return Err(anyhow!(
            "Failed to resolve the parent directory for {}.",
            paths.codex_auth_file.display()
        ));
    };
    fs::create_dir_all(parent)
        .with_context(|| format!("Failed to create {}.", parent.display()))?;
    write_codex_auth(&paths.codex_auth_file, &entry.auth)
}

fn extract_email_from_auth(auth: &CodexAuth) -> String {
    if let Ok(payload) = decode_jwt_payload(&auth.tokens.access_token) {
        if let Some(email) = payload
            .get("https://api.openai.com/profile")
            .and_then(Value::as_object)
            .and_then(|profile| profile.get("email"))
            .and_then(Value::as_str)
        {
            return email.to_string();
        }
    }
    if let Ok(payload) = decode_jwt_payload(&auth.tokens.id_token) {
        if let Some(email) = payload.get("email").and_then(Value::as_str) {
            return email.to_string();
        }
    }
    "unknown".to_string()
}

pub(crate) fn extract_plan_from_auth(auth: &CodexAuth) -> String {
    decode_jwt_payload(&auth.tokens.access_token)
        .ok()
        .and_then(|payload| {
            payload
                .get("https://api.openai.com/auth")
                .and_then(Value::as_object)
                .and_then(|auth| auth.get("chatgpt_plan_type"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn extract_client_id_from_auth(auth: &CodexAuth) -> String {
    if let Ok(payload) = decode_jwt_payload(&auth.tokens.access_token) {
        if let Some(client_id) = payload.get("client_id").and_then(Value::as_str) {
            return client_id.to_string();
        }
    }
    if let Ok(payload) = decode_jwt_payload(&auth.tokens.id_token) {
        if let Some(audience) = payload.get("aud") {
            if let Some(values) = audience.as_array() {
                if let Some(client_id) = values.first().and_then(Value::as_str) {
                    return client_id.to_string();
                }
            }
        }
    }
    DEFAULT_OAUTH_CLIENT_ID.to_string()
}

fn normalize_email_for_label(email: &str) -> String {
    let normalized = email.trim().to_lowercase();
    if normalized.is_empty() {
        "unknown".to_string()
    } else {
        normalized
    }
}

pub(crate) fn normalize_plan_type_for_label(plan_type: &str) -> String {
    let normalized = plan_type
        .trim()
        .to_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    let compact = normalized
        .split('-')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if compact.is_empty() {
        "unknown".to_string()
    } else {
        compact
    }
}

fn build_account_label(email: &str, plan_type: &str) -> String {
    format!(
        "{}_{}",
        normalize_email_for_label(email),
        normalize_plan_type_for_label(plan_type)
    )
}

fn normalize_identity_email(email: &str) -> Option<String> {
    let normalized = email.trim().to_lowercase();
    if normalized.is_empty() || normalized == "unknown" {
        None
    } else {
        Some(normalized)
    }
}

fn normalize_identity_plan_type(plan_type: &str) -> Option<String> {
    let normalized = normalize_plan_type_for_label(plan_type);
    if normalized == "unknown" {
        None
    } else {
        Some(normalized)
    }
}

fn should_preserve_expected_email(existing_email: &str, auth_email: &str) -> bool {
    let normalized_existing = existing_email.trim().to_lowercase();
    let normalized_auth = auth_email.trim().to_lowercase();
    let existing_is_gmail_plus = normalized_existing.ends_with("@gmail.com")
        && normalized_existing
            .split_once('@')
            .map(|(local, _)| local.contains('+'))
            .unwrap_or(false);
    !normalized_existing.is_empty()
        && normalized_existing != "unknown"
        && normalized_existing != normalized_auth
        && normalized_auth.ends_with("@gmail.com")
        && (!normalized_existing.ends_with("@gmail.com") || existing_is_gmail_plus)
}

pub(crate) fn account_entry_matches_identity(
    entry: &AccountEntry,
    account_id: &str,
    email: &str,
    plan_type: &str,
) -> bool {
    let target_email = normalize_identity_email(email);
    let entry_email = normalize_identity_email(&entry.email);
    let target_plan = normalize_identity_plan_type(plan_type);
    let entry_plan = normalize_identity_plan_type(&entry.plan_type);

    if target_email.is_some() && entry_email.as_deref() == target_email.as_deref() {
        return match (entry_plan.as_deref(), target_plan.as_deref()) {
            (Some(existing_plan), Some(target_plan)) => existing_plan == target_plan,
            _ => true,
        };
    }

    let normalized_account_id = account_id.trim();
    let has_matching_account_id = !normalized_account_id.is_empty()
        && (entry.account_id == normalized_account_id
            || entry.auth.tokens.account_id == normalized_account_id);
    if !has_matching_account_id {
        return false;
    }

    if let (Some(existing_plan), Some(target_plan)) =
        (entry_plan.as_deref(), target_plan.as_deref())
    {
        if existing_plan != target_plan {
            return false;
        }
    }

    match (entry_email.as_deref(), target_email.as_deref()) {
        (_, None) => true,
        (None, Some(_)) => true,
        (Some(existing_email), Some(target_email)) => {
            should_preserve_expected_email(existing_email, target_email)
                || should_preserve_expected_email(target_email, existing_email)
        }
    }
}

pub(crate) fn account_entry_matches_auth_identity(entry: &AccountEntry, auth: &CodexAuth) -> bool {
    account_entry_matches_identity(
        entry,
        &extract_account_id_from_auth(auth),
        &extract_email_from_auth(auth),
        &extract_plan_from_auth(auth),
    )
}

fn normalize_alias(alias: Option<&str>) -> Option<String> {
    alias.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn get_account_summary(entry: &AccountEntry) -> String {
    match &entry.alias {
        Some(alias) => format!("{} ({alias})", entry.label),
        None => entry.label.clone(),
    }
}

pub(crate) fn format_account_summary_for_display(entry: &AccountEntry) -> String {
    get_account_summary(entry)
}

pub(crate) fn sync_pool_active_account_from_codex(
    pool: &mut Pool,
    auth_path: &Path,
) -> Result<bool> {
    sync_pool_current_auth_from_codex(pool, auth_path, true)
}

pub fn sync_pool_active_account_from_current_auth() -> Result<bool> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let changed = sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    if changed {
        save_pool(&pool)?;
    }
    Ok(changed)
}

pub fn sync_pool_current_auth_into_pool_without_activation() -> Result<bool> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let changed = sync_pool_current_auth_from_codex(&mut pool, &paths.codex_auth_file, false)?;
    if changed {
        save_pool(&pool)?;
    }
    Ok(changed)
}

pub fn restore_codex_auth_from_active_pool() -> Result<bool> {
    let paths = resolve_paths()?;
    if paths.codex_auth_file.exists() {
        return Ok(false);
    }

    let mut pool = load_pool()?;
    if pool.accounts.is_empty() {
        return Ok(false);
    }

    let mut dirty = normalize_pool_entries(&mut pool);
    let active_index = pool.active_index.min(pool.accounts.len().saturating_sub(1));
    if pool.active_index != active_index {
        pool.active_index = active_index;
        dirty = true;
    }
    if dirty {
        save_pool(&pool)?;
    }

    let Some(parent) = paths.codex_auth_file.parent() else {
        return Ok(false);
    };
    fs::create_dir_all(parent)
        .with_context(|| format!("Failed to create {}.", parent.display()))?;

    let active = &pool.accounts[active_index];
    write_codex_auth(&paths.codex_auth_file, &active.auth)?;
    Ok(true)
}

pub fn validate_persona_egress(persona: &PersonaEntry, mode: VmExpectedEgressMode) -> Result<()> {
    if mode == VmExpectedEgressMode::ProvisionOnly {
        return Ok(());
    }

    let actual_region = fetch_actual_egress_region()?;
    validate_persona_egress_with_actual(persona, mode, &actual_region)
}

pub fn validate_persona_egress_with_actual(
    persona: &PersonaEntry,
    mode: VmExpectedEgressMode,
    actual_region: &str,
) -> Result<()> {
    if mode == VmExpectedEgressMode::ProvisionOnly {
        return Ok(());
    }

    if let Some(expected) = &persona.expected_region_code {
        if !expected.eq_ignore_ascii_case(actual_region) {
            return Err(anyhow!(
                "Persona egress validation failed: expected {}, found {}.",
                expected,
                actual_region
            ));
        }
    }

    Ok(())
}

fn fetch_actual_egress_region() -> Result<String> {
    // In a real environment, this would call an external API or check a local proxy.
    // For now, we will return a default or use an environment variable for testing.
    if let Ok(region) = std::env::var("CODEX_ROTATE_MOCK_REGION") {
        return Ok(region);
    }

    // Default to US for now if no mock is provided.
    Ok("US".to_string())
}

pub fn restore_pool_active_index(index: usize) -> Result<bool> {
    let mut pool = load_pool()?;
    if pool.accounts.is_empty() {
        return Ok(false);
    }

    let restored_index = index.min(pool.accounts.len().saturating_sub(1));
    if pool.active_index == restored_index {
        return Ok(false);
    }

    pool.active_index = restored_index;
    save_pool(&pool)?;
    Ok(true)
}

fn sync_pool_current_auth_from_codex(
    pool: &mut Pool,
    auth_path: &Path,
    activate_current: bool,
) -> Result<bool> {
    if !auth_path.exists() {
        return Ok(false);
    }
    let current_auth = load_codex_auth(auth_path)?;
    sync_pool_current_auth_from_auth(pool, current_auth, activate_current)
}

fn sync_pool_current_auth_from_auth(
    pool: &mut Pool,
    current_auth: CodexAuth,
    activate_current: bool,
) -> Result<bool> {
    let current_account_id = extract_account_id_from_auth(&current_auth);
    let current_email = extract_email_from_auth(&current_auth);
    let normalized_email = normalize_email_for_label(&current_email);

    if normalized_email == "unknown" {
        return Ok(false);
    }

    let current_plan_type = extract_plan_from_auth(&current_auth);
    let current_label = build_account_label(&current_email, &current_plan_type);
    let mut changed = false;

    let Some(current_index) = find_pool_account_index_by_identity(
        pool,
        &current_account_id,
        &current_email,
        &current_plan_type,
    ) else {
        pool.accounts.push(AccountEntry {
            label: current_label,
            alias: None,
            email: current_email,
            account_id: current_account_id,
            plan_type: current_plan_type,
            auth: current_auth,
            added_at: now_iso(),
            last_quota_usable: None,
            last_quota_summary: None,
            last_quota_blocker: None,
            last_quota_checked_at: None,
            last_quota_primary_left_percent: None,
            last_quota_next_refresh_at: None,
            persona: None,
        });
        let added_index = pool.accounts.len() - 1;
        if activate_current || pool.accounts.len() == 1 {
            pool.active_index = added_index;
        }
        let _ = reconcile_added_account_credential_state(&pool.accounts[added_index])?;
        return Ok(true);
    };

    if activate_current && pool.active_index != current_index {
        if debug_pool_drift_enabled() {
            let previous_email = pool
                .accounts
                .get(pool.active_index)
                .map(|entry| entry.email.clone());
            let matched_email = pool
                .accounts
                .get(current_index)
                .map(|entry| entry.email.clone());
            eprintln!(
                "codex-rotate core debug [sync_current_auth] previous_active_index={} previous_active_email={:?} matched_index={} matched_email={:?} auth_email={} auth_plan={}",
                pool.active_index,
                previous_email,
                current_index,
                matched_email,
                current_email,
                current_plan_type
            );
        }
        pool.active_index = current_index;
        changed = true;
    }
    let applied_auth = apply_auth_to_account(&mut pool.accounts[current_index], current_auth);
    let _ = reconcile_added_account_credential_state(&pool.accounts[current_index])?;
    Ok(applied_auth || changed)
}

fn find_pool_account_index_by_identity(
    pool: &Pool,
    account_id: &str,
    email: &str,
    plan_type: &str,
) -> Option<usize> {
    if pool
        .accounts
        .get(pool.active_index)
        .map(|entry| account_entry_matches_identity(entry, account_id, email, plan_type))
        .unwrap_or(false)
    {
        return Some(pool.active_index);
    }

    if let (Some(normalized_email), Some(normalized_plan)) = (
        normalize_identity_email(email),
        normalize_identity_plan_type(plan_type),
    ) {
        if let Some(index) = pool.accounts.iter().position(|entry| {
            normalize_identity_email(&entry.email).as_deref() == Some(normalized_email.as_str())
                && normalize_identity_plan_type(&entry.plan_type).as_deref()
                    == Some(normalized_plan.as_str())
        }) {
            return Some(index);
        }
    }

    pool.accounts
        .iter()
        .position(|entry| account_entry_matches_identity(entry, account_id, email, plan_type))
}

pub(crate) fn normalize_pool_entries(pool: &mut Pool) -> bool {
    let mut changed = false;
    for entry in &mut pool.accounts {
        changed |= normalize_cached_quota_usability(entry);
        let auth_email = extract_email_from_auth(&entry.auth);
        let next_email = if should_preserve_expected_email(&entry.email, &auth_email) {
            entry.email.clone()
        } else {
            auth_email
        };
        let next_label = build_account_label(&next_email, &entry.plan_type);
        let current_alias = normalize_alias(entry.alias.as_deref());
        if entry.label != next_label {
            if current_alias.is_none() && !entry.label.is_empty() {
                entry.alias = Some(entry.label.clone());
            }
            entry.label = next_label.clone();
            changed = true;
        }
        if entry.email != next_email {
            entry.email = next_email;
            changed = true;
        }

        let next_alias = normalize_alias(entry.alias.as_deref());
        match next_alias {
            Some(alias) if alias == entry.label => {
                if entry.alias.is_some() {
                    entry.alias = None;
                    changed = true;
                }
            }
            Some(alias) => {
                if entry.alias.as_deref() != Some(alias.as_str()) {
                    entry.alias = Some(alias);
                    changed = true;
                }
            }
            None => {
                if entry.alias.is_some() {
                    entry.alias = None;
                    changed = true;
                }
            }
        }

        let next_account_id = extract_account_id_from_auth(&entry.auth);
        if entry.account_id != next_account_id {
            entry.account_id = next_account_id;
            changed = true;
        }

        let next_persona = normalized_persona(entry);
        if entry.persona.as_ref() != Some(&next_persona) {
            entry.persona = Some(next_persona);
            changed = true;
        }
    }

    let max_active_index = pool.accounts.len().saturating_sub(1);
    let normalized_active_index = pool.active_index.min(max_active_index);
    if pool.active_index != normalized_active_index {
        pool.active_index = normalized_active_index;
        changed = true;
    }
    changed
}

fn normalized_persona(entry: &AccountEntry) -> PersonaEntry {
    let mut hasher = DefaultHasher::new();
    entry.account_id.hash(&mut hasher);
    entry.label.hash(&mut hasher);
    let persona_hash = hasher.finish();
    let persona_id = format!(
        "persona-{}-{:08x}",
        sanitize_persona_token(&entry.label),
        (persona_hash & 0xffff_ffff) as u32
    );
    let persona_profile_id = match (persona_hash % 3) as usize {
        0 => "balanced-us-compact",
        1 => "balanced-eu-wide",
        _ => "balanced-apac-standard",
    };
    let expected_region_code = entry
        .persona
        .as_ref()
        .and_then(|persona| persona.expected_region_code.clone());
    PersonaEntry {
        persona_id: entry
            .persona
            .as_ref()
            .map(|persona| persona.persona_id.clone())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(persona_id.clone()),
        persona_profile_id: Some(
            entry
                .persona
                .as_ref()
                .and_then(|persona| persona.persona_profile_id.clone())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| persona_profile_id.to_string()),
        ),
        expected_region_code,
        ready_at: entry
            .persona
            .as_ref()
            .and_then(|persona| persona.ready_at.clone()),
        host_root_rel_path: Some(
            entry
                .persona
                .as_ref()
                .and_then(|persona| persona.host_root_rel_path.clone())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| format!("personas/host/{persona_id}")),
        ),
        vm_package_rel_path: entry
            .persona
            .as_ref()
            .and_then(|persona| persona.vm_package_rel_path.clone()),
        browser_fingerprint: entry
            .persona
            .as_ref()
            .and_then(|persona| persona.browser_fingerprint.clone()),
    }
}

fn sanitize_persona_token(value: &str) -> String {
    let normalized = value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    let compact = normalized
        .split('-')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if compact.is_empty() {
        "account".to_string()
    } else {
        compact
    }
}

fn apply_auth_to_account(entry: &mut AccountEntry, auth: CodexAuth) -> bool {
    let auth_email = extract_email_from_auth(&auth);
    let next_email = if should_preserve_expected_email(&entry.email, &auth_email) {
        entry.email.clone()
    } else {
        auth_email
    };
    let next_plan = extract_plan_from_auth(&auth);
    let next_account_id = extract_account_id_from_auth(&auth);
    let next_label = build_account_label(&next_email, &next_plan);
    let next_alias = normalize_alias(entry.alias.as_deref());

    let changed = entry.label != next_label
        || entry.alias != next_alias
        || entry.email != next_email
        || entry.plan_type != next_plan
        || entry.account_id != next_account_id
        || entry.auth != auth;

    entry.label = next_label;
    if let Some(alias) = next_alias {
        if alias != entry.label {
            entry.alias = Some(alias);
        } else {
            entry.alias = None;
        }
    } else {
        entry.alias = None;
    }
    entry.email = next_email;
    entry.plan_type = next_plan;
    entry.account_id = next_account_id;
    entry.auth = auth;
    changed
}

fn apply_usage_to_account(entry: &mut AccountEntry, usage: &UsageResponse) -> bool {
    let next_email =
        if usage.email.is_empty() || should_preserve_expected_email(&entry.email, &usage.email) {
            entry.email.clone()
        } else {
            usage.email.clone()
        };
    let next_plan = if usage.plan_type.is_empty() {
        entry.plan_type.clone()
    } else {
        usage.plan_type.clone()
    };
    let next_label = build_account_label(&next_email, &next_plan);
    let next_alias = normalize_alias(entry.alias.as_deref());

    let changed = entry.label != next_label
        || entry.alias != next_alias
        || entry.email != next_email
        || entry.plan_type != next_plan;

    entry.label = next_label;
    if let Some(alias) = next_alias {
        if alias != entry.label {
            entry.alias = Some(alias);
        } else {
            entry.alias = None;
        }
    } else {
        entry.alias = None;
    }
    entry.email = next_email;
    entry.plan_type = next_plan;
    changed
}

fn write_codex_auth_if_current_account(auth_path: &Path, entry: &AccountEntry) -> Result<bool> {
    if !auth_path.exists() {
        return Ok(false);
    }
    let current_auth = load_codex_auth(auth_path)?;
    if !account_entry_matches_auth_identity(entry, &current_auth) {
        return Ok(false);
    }
    if current_auth != entry.auth {
        write_codex_auth(auth_path, &entry.auth)?;
        return Ok(true);
    }
    Ok(false)
}

fn apply_quota_inspection_to_account(
    entry: &mut AccountEntry,
    inspection: &AccountInspection,
    checked_at: &str,
) -> bool {
    let checked_at_value = DateTime::parse_from_rfc3339(checked_at)
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    let next_usable = inspection.usage.as_ref().map(has_usable_quota);
    let next_summary = inspection.usage.as_ref().map(format_compact_quota);
    let next_primary_left_percent = inspection.usage.as_ref().and_then(|usage| {
        get_quota_left(
            usage
                .rate_limit
                .as_ref()
                .and_then(|limits| limits.primary_window.as_ref()),
        )
        .map(|value| value.round() as u8)
    });
    let next_refresh_at = quota_next_refresh_at(
        inspection.usage.as_ref(),
        inspection.error.as_deref(),
        checked_at_value,
    )
    .to_rfc3339_opts(SecondsFormat::Millis, true);
    let next_blocker = inspection
        .usage
        .as_ref()
        .map(|usage| {
            if has_usable_quota(usage) {
                String::new()
            } else {
                describe_quota_blocker(usage)
            }
        })
        .and_then(|value| if value.is_empty() { None } else { Some(value) })
        .or_else(|| inspection.error.clone());

    let changed = entry.last_quota_usable != next_usable
        || entry.last_quota_summary != next_summary
        || entry.last_quota_blocker != next_blocker
        || entry.last_quota_checked_at.as_deref() != Some(checked_at)
        || entry.last_quota_primary_left_percent != next_primary_left_percent
        || entry.last_quota_next_refresh_at.as_deref() != Some(next_refresh_at.as_str());

    entry.last_quota_usable = next_usable;
    entry.last_quota_summary = next_summary;
    entry.last_quota_blocker = next_blocker;
    entry.last_quota_checked_at = Some(checked_at.to_string());
    entry.last_quota_primary_left_percent = next_primary_left_percent;
    entry.last_quota_next_refresh_at = Some(next_refresh_at);
    changed
}

pub(crate) fn inspect_account(
    entry: &mut AccountEntry,
    auth_path: &Path,
    persist_if_current: bool,
) -> Result<AccountInspection> {
    let inspected_at = now_iso();
    let inspection = match fetch_usage_with_recovery(&entry.auth) {
        Ok((auth, usage, _)) => {
            let mut updated = apply_auth_to_account(entry, auth.clone());
            updated |= apply_usage_to_account(entry, &usage);
            let inspection = AccountInspection {
                usage: Some(usage),
                error: None,
                updated: false,
            };
            updated |= apply_quota_inspection_to_account(entry, &inspection, &inspected_at);
            if persist_if_current {
                updated |= write_codex_auth_if_current_account(auth_path, entry)?;
            }
            AccountInspection {
                updated,
                ..inspection
            }
        }
        Err(error) => {
            let inspection = AccountInspection {
                usage: None,
                error: Some(error.to_string()),
                updated: false,
            };
            let updated = apply_quota_inspection_to_account(entry, &inspection, &inspected_at);
            AccountInspection {
                updated,
                ..inspection
            }
        }
    };
    Ok(inspection)
}

fn fetch_usage_with_recovery(auth: &CodexAuth) -> Result<(CodexAuth, UsageResponse, bool)> {
    let mut working_auth = auth.clone();
    let mut refreshed = false;

    if is_token_expired(&working_auth.tokens.access_token, 60) {
        working_auth = refresh_auth(&working_auth)?;
        refreshed = true;
    }

    match fetch_usage_once(&working_auth) {
        Ok(usage) => Ok((working_auth, usage, refreshed)),
        Err(error) => {
            if refreshed
                || !error
                    .downcast_ref::<HttpError>()
                    .map(|value| value.status == 401)
                    .unwrap_or(false)
            {
                return Err(error);
            }
            working_auth = refresh_auth(&working_auth)?;
            let usage = fetch_usage_once(&working_auth)?;
            Ok((working_auth, usage, true))
        }
    }
}

fn fetch_usage_once(auth: &CodexAuth) -> Result<UsageResponse> {
    let usage_url = std::env::var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE")
        .unwrap_or_else(|_| WHAM_USAGE_URL.to_string());
    let response = core_http_client()
        .get(&usage_url)
        .header("Accept", "application/json")
        .header(
            "Authorization",
            format!("Bearer {}", auth.tokens.access_token),
        )
        .header("ChatGPT-Account-Id", extract_account_id_from_auth(auth))
        .header("User-Agent", "codex-rotate-cli-rs")
        .send()
        .with_context(|| format!("Usage lookup failed: {usage_url}"))?;
    let status = response.status();
    let body = response.text().unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!(HttpError {
            status: status.as_u16(),
            message: build_http_error_message("Usage lookup", status.as_u16(), &body),
        }));
    }
    serde_json::from_str(&body).context("Usage lookup returned invalid JSON.")
}

fn refresh_auth(auth: &CodexAuth) -> Result<CodexAuth> {
    let refresh_token = auth
        .tokens
        .refresh_token
        .as_ref()
        .ok_or_else(|| anyhow!("No refresh token is available for this account."))?;
    let response = core_http_client()
        .post(
            std::env::var("CODEX_REFRESH_TOKEN_URL_OVERRIDE")
                .unwrap_or_else(|_| OAUTH_TOKEN_URL.to_string()),
        )
        .header("Accept", "application/json")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("User-Agent", "codex-rotate-rs")
        .form(&[
            ("client_id", extract_client_id_from_auth(auth)),
            ("grant_type", "refresh_token".to_string()),
            ("refresh_token", refresh_token.clone()),
        ])
        .send()
        .context("Token refresh failed.")?;
    let status = response.status();
    let raw = response.text().unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!(HttpError {
            status: status.as_u16(),
            message: build_http_error_message("Token refresh", status.as_u16(), &raw),
        }));
    }
    let token_response: OAuthTokenResponse =
        serde_json::from_str(&raw).context("Token refresh returned invalid JSON.")?;
    let access_token = token_response
        .access_token
        .ok_or_else(|| anyhow!("Token refresh response did not include an access token."))?;
    let refreshed_id_token = token_response.id_token.clone();
    Ok(CodexAuth {
        auth_mode: auth.auth_mode.clone(),
        openai_api_key: auth.openai_api_key.clone(),
        tokens: crate::auth::AuthTokens {
            access_token: access_token.clone(),
            id_token: refreshed_id_token
                .clone()
                .unwrap_or_else(|| auth.tokens.id_token.clone()),
            refresh_token: token_response
                .refresh_token
                .or_else(|| auth.tokens.refresh_token.clone()),
            account_id: extract_account_id_from_token(&access_token)
                .or_else(|| {
                    refreshed_id_token
                        .as_deref()
                        .and_then(extract_account_id_from_token)
                })
                .unwrap_or_else(|| auth.tokens.account_id.clone()),
        },
        last_refresh: now_iso(),
    })
}

fn build_http_error_message(action: &str, status: u16, body: &str) -> String {
    if let Ok(parsed) = serde_json::from_str::<Value>(body) {
        if let Some(value) = parsed.get("error_description").and_then(Value::as_str) {
            return format!("{action} failed ({status}): {value}");
        }
        if let Some(value) = parsed.get("error").and_then(Value::as_str) {
            return format!("{action} failed ({status}): {value}");
        }
        if let Some(value) = parsed.get("message").and_then(Value::as_str) {
            return format!("{action} failed ({status}): {value}");
        }
        if let Some(error) = parsed.get("error").and_then(Value::as_object) {
            let code = error.get("code").and_then(Value::as_str);
            let message = error.get("message").and_then(Value::as_str);
            if code == Some("refresh_token_reused") {
                return format!(
                    "{action} failed ({status}): refresh token already rotated; sign in again"
                );
            }
            if let (Some(code), Some(message)) = (code, message) {
                return format!("{action} failed ({status}): {code}: {message}");
            }
            if let Some(message) = message {
                return format!("{action} failed ({status}): {message}");
            }
            if let Some(code) = code {
                return format!("{action} failed ({status}): {code}");
            }
        }
    }
    format!("{action} failed ({status})")
}

fn core_http_client() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        Client::builder()
            .timeout(std::time::Duration::from_secs(REQUEST_TIMEOUT_SECONDS))
            .build()
            .expect("failed to build codex-rotate core HTTP client")
    })
}

pub fn find_next_cached_usable_account_index(
    active_index: usize,
    accounts: &[AccountEntry],
) -> Option<usize> {
    if accounts.len() <= 1 {
        return None;
    }
    for offset in 1..accounts.len() {
        let index = (active_index + offset) % accounts.len();
        if accounts[index].last_quota_usable == Some(true) {
            return Some(index);
        }
    }
    None
}

pub fn find_next_immediate_round_robin_index(
    active_index: usize,
    accounts: &[AccountEntry],
) -> Option<usize> {
    if accounts.len() <= 1 {
        return None;
    }
    for offset in 1..accounts.len() {
        let index = (active_index + offset) % accounts.len();
        let entry = &accounts[index];
        let has_cached_inspection = entry.last_quota_checked_at.is_some();
        if entry.last_quota_usable == Some(true) || !has_cached_inspection {
            return Some(index);
        }
    }
    None
}

pub fn build_reusable_account_probe_order(
    active_index: usize,
    account_count: usize,
    mode: ReusableAccountProbeMode,
) -> Vec<usize> {
    if account_count == 0 {
        return Vec::new();
    }
    let normalized_active_index = active_index.min(account_count - 1);
    let mut others = Vec::new();
    for offset in 1..account_count {
        others.push((normalized_active_index + offset) % account_count);
    }
    match mode {
        ReusableAccountProbeMode::CurrentFirst => {
            let mut order = vec![normalized_active_index];
            order.extend(others);
            order
        }
        ReusableAccountProbeMode::OthersFirst => {
            let mut order = others;
            order.push(normalized_active_index);
            order
        }
        ReusableAccountProbeMode::OthersOnly => others,
    }
}

pub(crate) fn find_next_usable_account(
    pool: &mut Pool,
    auth_path: &Path,
    mode: ReusableAccountProbeMode,
    reasons: &mut Vec<String>,
    dirty: bool,
    skip_indices: &HashSet<usize>,
    disabled_domains: &HashSet<String>,
) -> Result<(Option<RotationCandidate>, bool)> {
    let mut next_dirty = dirty;
    next_dirty |= prune_terminal_accounts_from_pool(pool)?;
    let mut effective_disabled_domains = if next_dirty != dirty {
        load_disabled_rotation_domains()?
    } else {
        disabled_domains.clone()
    };
    let probe_order =
        build_reusable_account_probe_order(pool.active_index, pool.accounts.len(), mode);

    for index in probe_order {
        if index >= pool.accounts.len() {
            continue;
        }
        if skip_indices.contains(&index) {
            continue;
        }
        if !account_rotation_enabled(&effective_disabled_domains, &pool.accounts[index].email) {
            if let Some(domain) = extract_email_domain(&pool.accounts[index].email) {
                reasons.push(format!(
                    "{}: rotation disabled for {}",
                    pool.accounts[index].label, domain
                ));
            }
            continue;
        }
        let inspection = inspect_account(
            &mut pool.accounts[index],
            auth_path,
            index == pool.active_index,
        )?;
        next_dirty |= inspection.updated;
        if account_requires_terminal_cleanup(&pool.accounts[index]) {
            next_dirty |= cleanup_terminal_account(pool, index)?;
            effective_disabled_domains = load_disabled_rotation_domains()?;
            continue;
        }
        if let Some(usage) = inspection.usage.as_ref() {
            if has_usable_quota(usage) {
                return Ok((
                    Some(RotationCandidate {
                        index,
                        entry: pool.accounts[index].clone(),
                        inspection,
                    }),
                    next_dirty,
                ));
            }
            reasons.push(format!(
                "{}: {}",
                pool.accounts[index].label,
                describe_quota_blocker(usage)
            ));
        } else {
            reasons.push(format!(
                "{}: {}",
                pool.accounts[index].label,
                inspection
                    .error
                    .clone()
                    .unwrap_or_else(|| "unknown error".to_string())
            ));
        }
    }
    Ok((None, next_dirty))
}

pub(crate) fn resolve_account_selector(pool: &Pool, selector: &str) -> Result<AccountSelection> {
    let normalized_selector = selector.trim();
    if normalized_selector.is_empty() {
        return Err(anyhow!("Account selector cannot be empty."));
    }

    let exact_matches = pool
        .accounts
        .iter()
        .enumerate()
        .filter(|(_, entry)| {
            entry.label == normalized_selector
                || entry.alias.as_deref() == Some(normalized_selector)
                || entry.account_id == normalized_selector
                || format_short_account_id(&entry.account_id) == normalized_selector
        })
        .map(|(index, entry)| AccountSelection {
            index,
            entry: entry.clone(),
        })
        .collect::<Vec<_>>();

    if exact_matches.len() == 1 {
        return Ok(exact_matches[0].clone());
    }
    if exact_matches.len() > 1 {
        return Err(anyhow!(
            "Selector \"{}\" matched multiple accounts. Use the full composite key.",
            normalized_selector
        ));
    }

    let normalized_email = normalized_selector.to_lowercase();
    let email_matches = pool
        .accounts
        .iter()
        .enumerate()
        .filter(|(_, entry)| entry.email.to_lowercase() == normalized_email)
        .map(|(index, entry)| AccountSelection {
            index,
            entry: entry.clone(),
        })
        .collect::<Vec<_>>();
    if email_matches.len() == 1 {
        return Ok(email_matches[0].clone());
    }
    if email_matches.len() > 1 {
        return Err(anyhow!(
            "Email \"{}\" matched multiple accounts: {}. Use the full composite key.",
            normalized_selector,
            email_matches
                .iter()
                .map(|selection| selection.entry.label.clone())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Err(anyhow!(
        "Account \"{}\" not found in pool.",
        normalized_selector
    ))
}

#[derive(Clone)]
pub(crate) struct AccountSelection {
    pub index: usize,
    pub entry: AccountEntry,
}

pub(crate) fn format_short_account_id(account_id: &str) -> String {
    if account_id.len() > 8 {
        format!("{}...", &account_id[..8])
    } else {
        account_id.to_string()
    }
}

fn format_usage_window(window: &UsageWindow) -> String {
    let left = get_quota_left(Some(window)).unwrap_or(0.0);
    let reset_text = format!(
        " (resets in {})",
        format_duration(window.reset_after_seconds.max(0))
    );
    format!("{} left{}", format_percent(left), reset_text)
}

fn format_percent(value: f64) -> String {
    if (value.fract()).abs() < f64::EPSILON {
        format!("{}%", value as i64)
    } else {
        format!("{value:.1}%")
    }
}

fn format_duration(total_seconds: i64) -> String {
    if total_seconds <= 0 {
        return "0s".to_string();
    }
    let mut remaining = total_seconds;
    let mut parts = Vec::new();
    for (label, seconds) in [("d", 86_400), ("h", 3_600), ("m", 60), ("s", 1)] {
        let amount = remaining / seconds;
        if amount > 0 {
            parts.push(format!("{amount}{label}"));
            remaining -= amount * seconds;
        }
        if parts.len() == 2 {
            break;
        }
    }
    parts.join(" ")
}

fn format_credits_full(credits: Option<&UsageCredits>) -> Option<String> {
    let credits = credits?;
    if credits.unlimited {
        return Some("unlimited".to_string());
    }
    if !credits.has_credits {
        return Some("none".to_string());
    }
    let mut details = Vec::new();
    if let Some(balance) = credits.balance {
        details.push(format!("balance {balance}"));
    }
    if let Some(local) = credits.approx_local_messages {
        details.push(format!("~{local} local msgs"));
    }
    if let Some(cloud) = credits.approx_cloud_messages {
        details.push(format!("~{cloud} cloud msgs"));
    }
    Some(if details.is_empty() {
        "available".to_string()
    } else {
        details.join(", ")
    })
}

fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{RotateHomeGuard, ENV_MUTEX};
    use base64::Engine;
    use serde_json::json;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::thread;
    use std::time::{Duration as StdDuration, Instant};

    fn stored_entry(usable: Option<bool>, checked_at: Option<&str>) -> AccountEntry {
        AccountEntry {
            label: "a_free".to_string(),
            alias: None,
            email: "a@example.com".to_string(),
            account_id: "acct-a".to_string(),
            plan_type: "free".to_string(),
            auth: CodexAuth {
                auth_mode: "chatgpt".to_string(),
                openai_api_key: None,
                tokens: crate::auth::AuthTokens {
                    access_token: "a.b.c".to_string(),
                    id_token: "a.b.c".to_string(),
                    refresh_token: None,
                    account_id: "acct-a".to_string(),
                },
                last_refresh: "2026-04-02T00:00:00.000Z".to_string(),
            },
            added_at: "2026-04-02T00:00:00.000Z".to_string(),
            last_quota_usable: usable,
            last_quota_summary: None,
            last_quota_blocker: None,
            last_quota_checked_at: checked_at.map(ToOwned::to_owned),
            last_quota_primary_left_percent: None,
            last_quota_next_refresh_at: None,
            persona: None,
        }
    }

    fn restore_env_var(key: &str, previous: Option<std::ffi::OsString>) {
        match previous {
            Some(value) => unsafe {
                std::env::set_var(key, value);
            },
            None => unsafe {
                std::env::remove_var(key);
            },
        }
    }

    fn strip_ansi(input: &str) -> String {
        input
            .replace(BOLD, "")
            .replace(DIM, "")
            .replace(GREEN, "")
            .replace(YELLOW, "")
            .replace(CYAN, "")
            .replace(RESET, "")
    }

    fn spawn_usage_server(body: String) -> (String, thread::JoinHandle<()>) {
        spawn_usage_server_with_delay(body, StdDuration::from_millis(0))
    }

    fn spawn_usage_server_with_delay(
        body: String,
        response_delay: StdDuration,
    ) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind usage server");
        listener
            .set_nonblocking(true)
            .expect("set usage server nonblocking");
        let address = listener.local_addr().expect("usage server address");
        let handle = thread::spawn(move || {
            let deadline = Instant::now() + StdDuration::from_secs(5);
            loop {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut buffer = [0_u8; 4096];
                        let _ = stream.read(&mut buffer);
                        if !response_delay.is_zero() {
                            thread::sleep(response_delay);
                        }
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        stream
                            .write_all(response.as_bytes())
                            .expect("write usage response");
                        stream.flush().expect("flush usage response");
                        return;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if Instant::now() >= deadline {
                            panic!("timed out waiting for quota request");
                        }
                        thread::sleep(StdDuration::from_millis(10));
                    }
                    Err(error) => panic!("usage server accept failed: {error}"),
                }
            }
        });
        (format!("http://{address}/usage"), handle)
    }

    #[derive(Clone, Default)]
    struct SharedWriter {
        buffer: Arc<StdMutex<Vec<u8>>>,
    }

    impl SharedWriter {
        fn snapshot(&self) -> String {
            String::from_utf8(self.buffer.lock().expect("writer mutex").clone())
                .expect("utf8 output")
        }
    }

    impl Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.buffer
                .lock()
                .expect("writer mutex")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn make_jwt(payload: serde_json::Value) -> String {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"alg":"none","typ":"JWT"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
        format!("{header}.{payload}.signature")
    }

    fn make_auth(email: &str, account_id: &str, plan_type: &str) -> CodexAuth {
        CodexAuth {
            auth_mode: "chatgpt".to_string(),
            openai_api_key: None,
            tokens: crate::auth::AuthTokens {
                access_token: make_jwt(json!({
                    "https://api.openai.com/profile": {
                        "email": email
                    },
                    "https://api.openai.com/auth": {
                        "chatgpt_account_id": account_id,
                        "chatgpt_plan_type": plan_type
                    }
                })),
                id_token: make_jwt(json!({
                    "email": email
                })),
                refresh_token: Some("refresh".to_string()),
                account_id: account_id.to_string(),
            },
            last_refresh: "2026-04-07T00:00:00.000Z".to_string(),
        }
    }

    fn configured_entry(
        email: &str,
        account_id: &str,
        plan_type: &str,
        usable: Option<bool>,
        checked_at: Option<&str>,
    ) -> AccountEntry {
        AccountEntry {
            label: format!("{email}_{plan_type}"),
            alias: None,
            email: email.to_string(),
            account_id: account_id.to_string(),
            plan_type: plan_type.to_string(),
            auth: make_auth(email, account_id, plan_type),
            added_at: "2026-04-07T00:00:00.000Z".to_string(),
            last_quota_usable: usable,
            last_quota_summary: usable.map(|value| {
                if value {
                    "5h 90% left".to_string()
                } else {
                    "5h 0% left".to_string()
                }
            }),
            last_quota_blocker: None,
            last_quota_checked_at: checked_at.map(ToOwned::to_owned),
            last_quota_primary_left_percent: usable.map(|value| if value { 90 } else { 0 }),
            last_quota_next_refresh_at: checked_at.map(|_| "2026-04-07T01:00:00.000Z".to_string()),
            persona: None,
        }
    }

    fn write_disabled_domain_state() -> Result<()> {
        let mut state = load_rotate_state_json()?;
        if !state.is_object() {
            state = json!({});
        }
        state["domain"] = json!({
            "astronlab.com": {
                "rotation_enabled": false
            }
        });
        write_rotate_state_json(&state)
    }

    fn terminal_cleanup_account(email: &str) -> AccountEntry {
        let mut entry = stored_entry(Some(false), Some("2026-04-07T01:00:00.000Z"));
        entry.email = email.to_string();
        entry.account_id = "acct-terminal".to_string();
        entry.last_quota_blocker = Some("refresh token has been invalidated".to_string());
        entry
    }

    fn write_terminal_cleanup_state(
        relogin: Vec<&str>,
        suspend_domain_on_terminal_refresh_failure: bool,
    ) -> Result<()> {
        let family = json!({
            "profile_name": "dev-1",
            "template": "dev.{n}@astronlab.com",
            "next_suffix": 2,
            "created_at": "2026-04-05T00:00:00.000Z",
            "updated_at": "2026-04-05T00:00:00.000Z",
            "last_created_email": "dev.1@astronlab.com",
            "relogin": relogin,
            "suspend_domain_on_terminal_refresh_failure": suspend_domain_on_terminal_refresh_failure,
        });
        write_rotate_state_json(&json!({
            "accounts": [terminal_cleanup_account("dev.1@astronlab.com")],
            "active_index": 0,
            "version": 7,
            "default_create_template": "dev.{n}@astronlab.com",
            "families": {
                "dev-1::dev.{n}@astronlab.com": family
            },
            "pending": {},
            "skipped": [],
            "domain": {
                "astronlab.com": {
                    "rotation_enabled": true
                }
            }
        }))
    }

    #[test]
    fn save_pool_preserves_credential_store_sections() {
        let _guard = RotateHomeGuard::enter("codex-rotate-save-pool-preserve");
        write_rotate_state_json(&json!({
            "accounts": [configured_entry("dev.1@astronlab.com", "acct-1", "free", Some(true), None)],
            "active_index": 0,
            "version": 7,
            "default_create_template": "dev.{n}@astronlab.com",
            "families": {
                "dev-1::dev.{n}@astronlab.com": {
                    "profile_name": "dev-1",
                    "template": "dev.{n}@astronlab.com",
                    "next_suffix": 3,
                    "created_at": "2026-04-05T00:00:00.000Z",
                    "updated_at": "2026-04-05T00:00:00.000Z",
                    "last_created_email": "dev.2@astronlab.com",
                    "relogin": []
                }
            },
            "pending": {
                "dev.3@astronlab.com": {
                    "email": "dev.3@astronlab.com",
                    "profile_name": "dev-1",
                    "template": "dev.{n}@astronlab.com",
                    "suffix": 3,
                    "selector": null,
                    "alias": null,
                    "created_at": "2026-04-05T00:00:00.000Z",
                    "updated_at": "2026-04-05T00:00:00.000Z",
                    "started_at": "2026-04-05T00:00:00.000Z"
                }
            },
            "skipped": ["dev.4@astronlab.com"],
            "domain": {
                "astronlab.com": {
                    "rotation_enabled": false
                }
            }
        }))
        .expect("write initial state");

        save_pool(&Pool {
            active_index: 1,
            accounts: vec![
                configured_entry("dev.1@astronlab.com", "acct-1", "free", Some(true), None),
                configured_entry("dev.2@astronlab.com", "acct-2", "free", None, None),
            ],
        })
        .expect("save pool");

        let state = load_rotate_state_json().expect("load rotate state");
        assert_eq!(state["version"], json!(9));
        assert_eq!(
            state["default_create_template"],
            json!("dev.{n}@astronlab.com")
        );
        assert!(state["families"].is_object());
        assert!(state["pending"].is_object());
        assert_eq!(state["skipped"], json!(["dev.4@astronlab.com"]));
        assert_eq!(
            state["domain"]["astronlab.com"]["rotation_enabled"],
            json!(false)
        );
        assert_eq!(state["active_index"], json!(1));
        assert_eq!(state["accounts"][1]["email"], json!("dev.2@astronlab.com"));
    }

    #[test]
    fn prune_terminal_accounts_does_not_disable_domain_for_relogin_only_families() {
        let _guard = RotateHomeGuard::enter("codex-rotate-terminal-cleanup-relogin-only");
        write_terminal_cleanup_state(Vec::new(), false).expect("write relogin-only state");

        let mut pool = Pool {
            active_index: 0,
            accounts: vec![terminal_cleanup_account("dev.1@astronlab.com")],
        };

        let changed =
            prune_terminal_accounts_from_pool(&mut pool).expect("prune terminal accounts");
        assert!(changed);

        let state = load_rotate_state_json().expect("load rotate state");
        assert_eq!(
            state["domain"]["astronlab.com"]["rotation_enabled"],
            json!(true)
        );
    }

    #[test]
    fn prune_terminal_accounts_disables_domain_for_suspend_flagged_families() {
        let _guard = RotateHomeGuard::enter("codex-rotate-terminal-cleanup-suspend-flag");
        write_terminal_cleanup_state(Vec::new(), true).expect("write suspend-flag state");

        let mut pool = Pool {
            active_index: 0,
            accounts: vec![terminal_cleanup_account("dev.1@astronlab.com")],
        };

        let changed =
            prune_terminal_accounts_from_pool(&mut pool).expect("prune terminal accounts");
        assert!(changed);

        let state = load_rotate_state_json().expect("load rotate state");
        assert_eq!(
            state["domain"]["astronlab.com"]["rotation_enabled"],
            json!(false)
        );
    }

    #[test]
    fn load_rotation_environment_settings_defaults_to_host() {
        let _guard = RotateHomeGuard::enter("codex-rotate-env-default-host");
        write_rotate_state_json(&json!({
            "accounts": [],
            "active_index": 0
        }))
        .expect("write default rotate state");

        let settings = load_rotation_environment_settings().expect("load settings");
        assert_eq!(settings.environment, RotationEnvironment::Host);
        assert!(settings.vm.is_none());
    }

    #[test]
    fn load_rotation_environment_settings_reads_vm_config() {
        let _guard = RotateHomeGuard::enter("codex-rotate-env-vm");
        write_rotate_state_json(&json!({
            "accounts": [],
            "active_index": 0,
            "environment": "vm",
            "vm": {
                "basePackagePath": "/vm/base.utm",
                "personaRoot": "/vm/personas",
                "utmAppPath": "/Applications/UTM.app",
                "bridgeRoot": "/vm/bridge",
                "expectedEgressMode": "validate"
            }
        }))
        .expect("write vm rotate state");

        let settings = load_rotation_environment_settings().expect("load settings");
        assert_eq!(settings.environment, RotationEnvironment::Vm);
        let vm = settings.vm.expect("vm config");
        assert_eq!(vm.base_package_path.as_deref(), Some("/vm/base.utm"));
        assert_eq!(vm.persona_root.as_deref(), Some("/vm/personas"));
        assert_eq!(vm.utm_app_path.as_deref(), Some("/Applications/UTM.app"));
        assert_eq!(vm.bridge_root.as_deref(), Some("/vm/bridge"));
        assert_eq!(vm.expected_egress_mode, VmExpectedEgressMode::Validate);
    }

    #[test]
    fn normalize_pool_entries_assigns_deterministic_persona_defaults() {
        let mut pool = Pool {
            active_index: 0,
            accounts: vec![configured_entry(
                "dev.1@astronlab.com",
                "acct-1",
                "free",
                Some(true),
                None,
            )],
        };

        assert!(normalize_pool_entries(&mut pool));
        let persona = pool.accounts[0]
            .persona
            .clone()
            .expect("persona metadata should be assigned");
        assert!(persona.persona_id.starts_with("persona-"));
        assert!(persona
            .host_root_rel_path
            .as_deref()
            .unwrap()
            .starts_with("personas/host/"));
        assert!(persona.persona_profile_id.is_some());

        let mut second_pool = Pool {
            active_index: 0,
            accounts: vec![configured_entry(
                "dev.1@astronlab.com",
                "acct-1",
                "free",
                Some(true),
                None,
            )],
        };
        normalize_pool_entries(&mut second_pool);
        assert_eq!(second_pool.accounts[0].persona, Some(persona));
    }

    #[test]
    fn prepare_prev_rotation_stages_previous_selection_until_commit() {
        let _guard = RotateHomeGuard::enter("codex-rotate-prepare-prev-stage");
        let mut previous =
            configured_entry("dev.1@astronlab.com", "acct-1", "free", Some(true), None);
        previous.last_quota_checked_at = Some("2099-01-01T00:00:00.000Z".to_string());
        previous.last_quota_next_refresh_at = Some("2099-01-01T01:00:00.000Z".to_string());
        let mut current =
            configured_entry("dev.2@astronlab.com", "acct-2", "free", Some(true), None);
        current.last_quota_checked_at = Some("2099-01-01T00:00:00.000Z".to_string());
        current.last_quota_next_refresh_at = Some("2099-01-01T01:00:00.000Z".to_string());
        write_rotate_state_json(&json!({
            "accounts": [previous, current],
            "active_index": 1
        }))
        .expect("write rotate state");

        let initial_pool = load_pool().expect("load pool");
        write_selected_account_auth(&initial_pool.accounts[1]).expect("write current auth");

        let prepared = prepare_prev_rotation().expect("prepare prev");
        assert_eq!(prepared.action, PreparedRotationAction::Switch);
        assert_eq!(prepared.previous_index, 1);
        assert_eq!(prepared.target_index, 0);

        let staged_state = load_rotate_state_json().expect("load staged state");
        assert_eq!(staged_state["active_index"], json!(1));
        let staged_auth =
            crate::auth::load_codex_auth(&resolve_paths().expect("resolve paths").codex_auth_file)
                .expect("load staged auth");
        assert_eq!(staged_auth.tokens.account_id, "acct-2");

        persist_prepared_rotation_pool(&prepared).expect("persist prepared pool");
        write_selected_account_auth(&prepared.target).expect("write target auth");

        let committed_state = load_rotate_state_json().expect("load committed state");
        assert_eq!(committed_state["active_index"], json!(0));
        let committed_auth =
            crate::auth::load_codex_auth(&resolve_paths().expect("resolve paths").codex_auth_file)
                .expect("load committed auth");
        assert_eq!(committed_auth.tokens.account_id, "acct-1");
    }

    #[test]
    fn cached_next_rotation_prefers_later_usable_slot() {
        let accounts = vec![
            stored_entry(Some(true), None),
            stored_entry(Some(false), None),
            stored_entry(Some(true), None),
        ];
        assert_eq!(find_next_cached_usable_account_index(0, &accounts), Some(2));
    }

    #[test]
    fn immediate_round_robin_skips_explicitly_unusable_slots() {
        let accounts = vec![
            stored_entry(None, None),
            stored_entry(Some(true), None),
            stored_entry(Some(false), Some("2026-04-02T00:00:00.000Z")),
        ];
        assert_eq!(find_next_immediate_round_robin_index(1, &accounts), Some(0));
    }

    #[test]
    fn probe_order_respects_mode() {
        assert_eq!(
            build_reusable_account_probe_order(1, 4, ReusableAccountProbeMode::CurrentFirst),
            vec![1, 2, 3, 0]
        );
        assert_eq!(
            build_reusable_account_probe_order(1, 4, ReusableAccountProbeMode::OthersFirst),
            vec![2, 3, 0, 1]
        );
        assert_eq!(
            build_reusable_account_probe_order(1, 4, ReusableAccountProbeMode::OthersOnly),
            vec![2, 3, 0]
        );
    }

    #[test]
    fn pool_identity_lookup_prefers_exact_email_match() {
        let mut first = stored_entry(Some(true), None);
        first.email = "dev.26@astronlab.com".to_string();
        first.account_id = "acct-26".to_string();
        first.auth.tokens.account_id = "acct-26".to_string();
        let mut second = stored_entry(Some(true), None);
        second.email = "dev.27@astronlab.com".to_string();
        second.account_id = "acct-27".to_string();
        second.auth.tokens.account_id = "acct-27".to_string();
        let pool = Pool {
            active_index: 0,
            accounts: vec![first, second],
        };

        assert_eq!(
            find_pool_account_index_by_identity(&pool, "acct-27", "dev.26@astronlab.com", "free"),
            Some(0)
        );
    }

    #[test]
    fn pool_identity_lookup_falls_back_to_email_match() {
        let mut first = stored_entry(Some(true), None);
        first.email = "dev.26@astronlab.com".to_string();
        first.account_id = "acct-26".to_string();
        first.auth.tokens.account_id = "acct-26".to_string();
        let pool = Pool {
            active_index: 0,
            accounts: vec![first],
        };

        assert_eq!(
            find_pool_account_index_by_identity(&pool, "missing", "dev.26@astronlab.com", "free"),
            Some(0)
        );
    }

    #[test]
    fn pool_identity_lookup_distinguishes_same_email_different_plan() {
        let mut team = stored_entry(Some(true), None);
        team.email = "dev.1@hotspotprime.com".to_string();
        team.label = "dev.1@hotspotprime.com_team".to_string();
        team.plan_type = "team".to_string();
        team.account_id = "acct-team".to_string();
        team.auth = make_auth("dev.1@hotspotprime.com", "acct-team", "team");

        let mut free = stored_entry(Some(true), None);
        free.email = "dev.1@hotspotprime.com".to_string();
        free.label = "dev.1@hotspotprime.com_free".to_string();
        free.plan_type = "free".to_string();
        free.account_id = "acct-free".to_string();
        free.auth = make_auth("dev.1@hotspotprime.com", "acct-free", "free");

        let pool = Pool {
            active_index: 0,
            accounts: vec![team, free],
        };

        assert_eq!(
            find_pool_account_index_by_identity(
                &pool,
                "acct-team",
                "dev.1@hotspotprime.com",
                "team",
            ),
            Some(0)
        );
        assert_eq!(
            find_pool_account_index_by_identity(
                &pool,
                "acct-free",
                "dev.1@hotspotprime.com",
                "free",
            ),
            Some(1)
        );
    }

    #[test]
    fn pool_identity_lookup_ignores_shared_account_id_for_different_team_email() {
        let mut first = stored_entry(Some(true), None);
        first.email = "dev.2@hotspotprime.com".to_string();
        first.label = "dev.2@hotspotprime.com_team".to_string();
        first.plan_type = "team".to_string();
        first.account_id = "acct-team".to_string();
        first.auth = make_auth("dev.2@hotspotprime.com", "acct-team", "team");

        let pool = Pool {
            active_index: 0,
            accounts: vec![first],
        };

        assert_eq!(
            find_pool_account_index_by_identity(
                &pool,
                "acct-team",
                "dev.3@hotspotprime.com",
                "team"
            ),
            None
        );
    }

    #[test]
    fn cached_list_quota_line_uses_saved_summary() {
        let mut entry = stored_entry(Some(true), Some("2026-04-07T00:00:00.000Z"));
        entry.last_quota_summary = Some("7d 90% left".to_string());

        let rendered = format_cached_quota_line(&entry);

        assert!(rendered.contains("7d 90% left"));
        assert!(rendered.contains("cached 2026-04-07T00:00:00.000Z"));
    }

    #[test]
    fn cached_list_quota_line_marks_unchecked_entries_without_network_lookup() {
        let entry = stored_entry(None, None);

        let rendered = format_cached_quota_line(&entry);

        assert_eq!(
            rendered,
            "unknown (run codex-rotate status or rotate to refresh)"
        );
    }

    #[test]
    fn cached_list_quota_state_respects_usable_ttl() {
        let now = DateTime::parse_from_rfc3339("2026-04-08T12:01:00.000Z")
            .expect("parse now")
            .with_timezone(&Utc);
        let mut fresh = stored_entry(Some(true), Some("2026-04-08T12:00:30.000Z"));
        fresh.last_quota_primary_left_percent = Some(40);
        assert!(!cached_quota_state_is_stale(&fresh, now));

        let mut stale = stored_entry(Some(true), Some("2026-04-08T11:59:50.000Z"));
        stale.last_quota_primary_left_percent = Some(40);
        assert!(cached_quota_state_is_stale(&stale, now));
    }

    #[test]
    fn cached_list_quota_state_waits_for_zero_percent_reset_time() {
        let checked_at = DateTime::parse_from_rfc3339("2026-04-08T12:00:00.000Z")
            .expect("parse checked_at")
            .with_timezone(&Utc);
        let mut exhausted = stored_entry(Some(false), Some("2026-04-08T12:00:00.000Z"));
        exhausted.last_quota_blocker = Some("5h quota exhausted, resets in 2h 15m".to_string());

        let before_reset = DateTime::parse_from_rfc3339("2026-04-08T14:14:59.000Z")
            .expect("parse before_reset")
            .with_timezone(&Utc);
        let after_reset = DateTime::parse_from_rfc3339("2026-04-08T14:15:01.000Z")
            .expect("parse after_reset")
            .with_timezone(&Utc);

        assert_eq!(
            legacy_cached_quota_next_refresh_at(&exhausted, checked_at),
            Some(
                DateTime::parse_from_rfc3339("2026-04-08T14:15:00.000Z")
                    .expect("parse expected reset")
                    .with_timezone(&Utc)
            )
        );
        assert!(!cached_quota_state_is_stale(&exhausted, before_reset));
        assert!(cached_quota_state_is_stale(&exhausted, after_reset));
    }

    #[test]
    fn list_account_refresh_due_only_when_refresh_time_elapsed_or_missing() {
        let now = DateTime::parse_from_rfc3339("2026-04-08T12:05:00.000Z")
            .expect("parse now")
            .with_timezone(&Utc);

        let mut fresh = stored_entry(Some(true), Some("2026-04-08T12:04:30.000Z"));
        fresh.last_quota_primary_left_percent = Some(40);
        fresh.last_quota_next_refresh_at = Some("2026-04-08T12:10:00.000Z".to_string());
        assert!(!account_quota_refresh_due_for_list(&fresh, now));

        let mut stale = stored_entry(Some(true), Some("2026-04-08T12:03:30.000Z"));
        stale.last_quota_primary_left_percent = Some(40);
        stale.last_quota_next_refresh_at = Some("2026-04-08T12:04:59.000Z".to_string());
        assert!(account_quota_refresh_due_for_list(&stale, now));

        let unknown = stored_entry(None, None);
        assert!(account_quota_refresh_due_for_list(&unknown, now));
    }

    #[test]
    fn list_quota_refresh_order_prioritizes_active_then_oldest_stale_usable() {
        let now = DateTime::parse_from_rfc3339("2026-04-08T12:05:00.000Z")
            .expect("parse now")
            .with_timezone(&Utc);

        let mut active = stored_entry(Some(false), Some("2026-04-08T12:04:00.000Z"));
        active.last_quota_blocker = Some("rate limited".to_string());

        let mut oldest_stale_usable = stored_entry(Some(true), Some("2026-04-08T12:03:30.000Z"));
        oldest_stale_usable.last_quota_primary_left_percent = Some(40);

        let mut fresher_stale_usable = stored_entry(Some(true), Some("2026-04-08T12:04:10.000Z"));
        fresher_stale_usable.last_quota_primary_left_percent = Some(40);

        let mut fresh_usable = stored_entry(Some(true), Some("2026-04-08T12:04:45.000Z"));
        fresh_usable.last_quota_primary_left_percent = Some(40);

        let pool = Pool {
            active_index: 0,
            accounts: vec![
                active,
                fresher_stale_usable,
                fresh_usable,
                oldest_stale_usable,
            ],
        };

        assert_eq!(build_list_quota_refresh_order(&pool, now), vec![0, 3]);
    }

    #[test]
    fn list_quota_refresh_order_includes_unknown_and_all_stale_entries() {
        let now = DateTime::parse_from_rfc3339("2026-04-08T12:05:00.000Z")
            .expect("parse now")
            .with_timezone(&Utc);

        let mut stale_active = stored_entry(Some(false), Some("2026-04-08T12:04:00.000Z"));
        stale_active.last_quota_blocker = Some("rate limited".to_string());

        let mut unknown = stored_entry(None, None);
        unknown.label = "unknown".to_string();
        unknown.email = "unknown@example.com".to_string();
        unknown.account_id = "acct-unknown".to_string();

        let mut stale_usable = stored_entry(Some(true), Some("2026-04-08T12:03:30.000Z"));
        stale_usable.last_quota_primary_left_percent = Some(40);

        let pool = Pool {
            active_index: 0,
            accounts: vec![stale_active, unknown, stale_usable],
        };

        assert_eq!(build_list_quota_refresh_order(&pool, now), vec![1, 0, 2]);
    }

    #[test]
    fn list_account_display_order_sorts_by_next_quota_refresh_eta() {
        let mut later = stored_entry(Some(true), Some("2026-04-08T12:00:00.000Z"));
        later.label = "later".to_string();
        later.last_quota_primary_left_percent = Some(80);
        later.last_quota_next_refresh_at = Some("2026-04-08T12:20:00.000Z".to_string());

        let mut unknown = stored_entry(None, None);
        unknown.label = "unknown".to_string();

        let mut sooner = stored_entry(Some(false), Some("2026-04-08T12:00:00.000Z"));
        sooner.label = "sooner".to_string();
        sooner.last_quota_blocker = Some("7d quota exhausted, resets in 10m".to_string());

        let pool = Pool {
            active_index: 1,
            accounts: vec![later, unknown, sooner],
        };

        assert_eq!(build_list_account_display_order(&pool), vec![2, 0, 1]);
    }

    #[test]
    fn cmd_list_refreshes_stale_cached_usable_quota() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_usage_url = std::env::var_os("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let mut stale = stored_entry(Some(true), Some("2026-04-07T12:00:00.000Z"));
            stale.email = "dev.60@astronlab.com".to_string();
            stale.account_id = "acct-60".to_string();
            stale.label = "dev.60@astronlab.com_free".to_string();
            stale.auth = make_auth("dev.60@astronlab.com", "acct-60", "free");
            stale.auth.tokens.account_id = "acct-60".to_string();
            stale.last_quota_summary = Some("5h 99% left".to_string());
            stale.last_quota_primary_left_percent = Some(99);

            save_pool(&Pool {
                active_index: 0,
                accounts: vec![stale],
            })?;

            let (usage_url, handle) = spawn_usage_server(
                json!({
                    "user_id": "user-60",
                    "account_id": "acct-60",
                    "email": "dev.60@astronlab.com",
                    "plan_type": "free",
                    "rate_limit": {
                        "allowed": true,
                        "limit_reached": false,
                        "primary_window": {
                            "used_percent": 60.0,
                            "limit_window_seconds": 18000,
                            "reset_after_seconds": 7200,
                            "reset_at": 0
                        },
                        "secondary_window": null
                    },
                    "code_review_rate_limit": null,
                    "additional_rate_limits": null,
                    "credits": null,
                    "promo": null
                })
                .to_string(),
            );
            unsafe {
                std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", &usage_url);
            }

            let output = cmd_list()?;
            handle.join().expect("usage server should finish");

            assert!(output.contains("5h 40% left"));

            let refreshed = load_pool()?;
            assert_eq!(
                refreshed.accounts[0].last_quota_primary_left_percent,
                Some(40)
            );
            assert!(refreshed.accounts[0]
                .last_quota_summary
                .as_deref()
                .unwrap_or_default()
                .contains("5h 40% left"));
            Ok(())
        })();

        restore_env_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", previous_usage_url);
        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("list should refresh stale cached quota");
    }

    #[test]
    fn cmd_list_prints_total_and_healthy_sections_separately() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let mut healthy = stored_entry(Some(true), Some("2026-04-09T02:00:00.000Z"));
            healthy.label = "dev.healthy@astronlab.com_free".to_string();
            healthy.email = "dev.healthy@astronlab.com".to_string();
            healthy.account_id = "acct-healthy".to_string();
            healthy.last_quota_summary = Some("7d 88% left".to_string());
            healthy.last_quota_primary_left_percent = Some(88);
            healthy.last_quota_next_refresh_at = Some("2099-01-01T00:00:00.000Z".to_string());

            let mut exhausted = stored_entry(Some(false), Some("2026-04-09T02:00:00.000Z"));
            exhausted.label = "dev.exhausted@astronlab.com_free".to_string();
            exhausted.email = "dev.exhausted@astronlab.com".to_string();
            exhausted.account_id = "acct-exhausted".to_string();
            exhausted.last_quota_summary = Some("7d 0% left".to_string());
            exhausted.last_quota_blocker = Some("7d quota exhausted, resets in 6d".to_string());
            exhausted.last_quota_primary_left_percent = Some(0);
            exhausted.last_quota_next_refresh_at = Some("2099-01-01T00:00:00.000Z".to_string());

            save_pool(&Pool {
                active_index: 0,
                accounts: vec![healthy, exhausted],
            })?;

            let output = strip_ansi(&cmd_list()?);

            assert!(output.contains("Total Accounts"));
            assert!(output.contains("Healthy Accounts (1 account(s))"));

            let total_index = output.find("Total Accounts").expect("total section");
            let healthy_index = output
                .find("Healthy Accounts (1 account(s))")
                .expect("healthy section");
            assert!(healthy_index > total_index);

            assert_eq!(
                output
                    .match_indices("dev.healthy@astronlab.com_free")
                    .count(),
                2
            );
            assert_eq!(
                output
                    .match_indices("dev.exhausted@astronlab.com_free")
                    .count(),
                1
            );
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("list should print total and healthy sections");
    }

    #[test]
    fn cmd_list_excludes_weekly_exhausted_accounts_from_healthy_section() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let mut weekly_exhausted = stored_entry(Some(true), Some("2099-01-01T00:00:00.000Z"));
            weekly_exhausted.label = "dev.4@hotspotprime.com_team".to_string();
            weekly_exhausted.email = "dev.4@hotspotprime.com".to_string();
            weekly_exhausted.account_id = "acct-4".to_string();
            weekly_exhausted.plan_type = "team".to_string();
            weekly_exhausted.auth = make_auth("dev.4@hotspotprime.com", "acct-4", "team");
            weekly_exhausted.last_quota_summary =
                Some("5h 100% left, 5h | week 0% left, 3d 11h".to_string());
            weekly_exhausted.last_quota_primary_left_percent = Some(100);
            weekly_exhausted.last_quota_next_refresh_at =
                Some("2099-01-01T00:01:00.000Z".to_string());

            let mut healthy = stored_entry(Some(true), Some("2099-01-01T00:00:00.000Z"));
            healthy.label = "dev.6@hotspotprime.com_team".to_string();
            healthy.email = "dev.6@hotspotprime.com".to_string();
            healthy.account_id = "acct-6".to_string();
            healthy.plan_type = "team".to_string();
            healthy.auth = make_auth("dev.6@hotspotprime.com", "acct-6", "team");
            healthy.last_quota_summary =
                Some("5h 74% left, 4h 45m | week 96% left, 6d 23h".to_string());
            healthy.last_quota_primary_left_percent = Some(74);
            healthy.last_quota_next_refresh_at = Some("2099-01-01T00:01:00.000Z".to_string());

            save_pool(&Pool {
                active_index: 0,
                accounts: vec![weekly_exhausted, healthy],
            })?;

            let output = strip_ansi(&cmd_list()?);

            assert!(output.contains("Healthy Accounts (1 account(s))"));
            let healthy_index = output
                .find("Healthy Accounts (1 account(s))")
                .expect("healthy section");
            let healthy_section = &output[healthy_index..];
            assert!(healthy_section.contains("dev.6@hotspotprime.com_team"));
            assert!(!healthy_section.contains("dev.4@hotspotprime.com_team"));

            let refreshed = load_pool()?;
            assert_eq!(refreshed.accounts[0].last_quota_usable, Some(false));
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("list should exclude weekly exhausted accounts from healthy section");
    }

    #[test]
    fn cmd_list_hides_healthy_accounts_from_disabled_domains() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let mut healthy_disabled = stored_entry(Some(true), Some("2026-04-09T02:00:00.000Z"));
            healthy_disabled.label = "dev.hidden@astronlab.com_free".to_string();
            healthy_disabled.email = "dev.hidden@astronlab.com".to_string();
            healthy_disabled.account_id = "acct-hidden".to_string();
            healthy_disabled.auth = make_auth("dev.hidden@astronlab.com", "acct-hidden", "free");
            healthy_disabled.auth.tokens.account_id = "acct-hidden".to_string();
            healthy_disabled.last_quota_checked_at = Some("2099-01-01T00:00:00.000Z".to_string());

            let mut exhausted_disabled =
                stored_entry(Some(false), Some("2026-04-09T02:00:00.000Z"));
            exhausted_disabled.label = "dev.visible@astronlab.com_free".to_string();
            exhausted_disabled.email = "dev.visible@astronlab.com".to_string();
            exhausted_disabled.account_id = "acct-visible".to_string();
            exhausted_disabled.auth =
                make_auth("dev.visible@astronlab.com", "acct-visible", "free");
            exhausted_disabled.auth.tokens.account_id = "acct-visible".to_string();
            exhausted_disabled.last_quota_checked_at = Some("2099-01-01T00:00:00.000Z".to_string());

            save_pool(&Pool {
                active_index: 1,
                accounts: vec![healthy_disabled, exhausted_disabled],
            })?;
            write_disabled_domain_state()?;
            assert!(load_disabled_rotation_domains()?.contains("astronlab.com"));

            let output = strip_ansi(&cmd_list()?);

            assert!(
                output.contains("Codex OAuth Account Pool (1 account(s))"),
                "{output}"
            );
            assert!(!output.contains("dev.hidden@astronlab.com_free"));
            assert!(output.contains("dev.visible@astronlab.com_free"));
            assert!(output.contains("Healthy Accounts (0 account(s))"));
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("list should hide healthy disabled-domain accounts");
    }

    #[test]
    fn cmd_list_prunes_invalidated_refresh_token_accounts_and_suspends_domain() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let mut invalidated = stored_entry(None, Some("2026-04-14T06:08:54.124Z"));
            invalidated.label = "devbench.9@astronlab.com_free".to_string();
            invalidated.email = "devbench.9@astronlab.com".to_string();
            invalidated.account_id = "acct-invalidated".to_string();
            invalidated.auth = make_auth("devbench.9@astronlab.com", "acct-invalidated", "free");
            invalidated.auth.tokens.account_id = "acct-invalidated".to_string();
            invalidated.last_quota_blocker = Some("Token refresh failed (401): refresh_token_invalidated: Your refresh token has been invalidated. Please try signing in again.".to_string());

            write_rotate_state_json(&json!({
                "families": {
                    "dev-1::devbench.{n}@astronlab.com": {
                        "profile_name": "dev-1",
                        "template": "devbench.{n}@astronlab.com",
                        "next_suffix": 10,
                        "max_skipped_slots": 0,
                        "created_at": "2026-04-13T05:00:00.000Z",
                        "updated_at": "2026-04-14T06:11:25.913Z",
                        "last_created_email": "devbench.9@astronlab.com",
                        "relogin": [],
                        "suspend_domain_on_terminal_refresh_failure": true
                    }
                }
            }))?;
            save_pool(&Pool {
                active_index: 0,
                accounts: vec![invalidated],
            })?;

            let output = strip_ansi(&cmd_list()?);
            assert!(!output.contains("devbench.9@astronlab.com_free"));

            let state = load_rotate_state_json()?;
            let accounts = state["accounts"].as_array().expect("accounts");
            assert_eq!(accounts.len(), 1);
            assert_eq!(accounts[0]["email"], "devbench.9@astronlab.com");
            assert!(accounts[0]["auth"]["tokens"]["access_token"]
                .as_str()
                .is_some());
            assert_eq!(accounts[0]["auth"]["tokens"]["refresh_token"], "refresh");
            assert_eq!(
                state["domain"]["astronlab.com"]["rotation_enabled"],
                Value::Bool(false)
            );
            let reactivate_at = state["domain"]["astronlab.com"]["reactivate_at"]
                .as_str()
                .expect("reactivate_at");
            let parsed = DateTime::parse_from_rfc3339(reactivate_at)
                .expect("parse reactivate_at")
                .with_timezone(&Utc);
            let delta_days = (parsed - Utc::now()).num_days();
            assert!((8..=9).contains(&delta_days), "{reactivate_at}");
            assert_eq!(
                state["families"]["dev-1::devbench.{n}@astronlab.com"]["relogin"]
                    .as_array()
                    .map(|entries| entries.iter().filter_map(Value::as_str).collect::<Vec<_>>()),
                Some(vec!["devbench.9@astronlab.com"])
            );
            assert_eq!(
                state["families"]["dev-1::devbench.{n}@astronlab.com"]
                    ["suspend_domain_on_terminal_refresh_failure"],
                Value::Bool(true)
            );
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("list should prune invalidated refresh-token accounts");
    }

    #[test]
    fn cmd_list_prunes_reused_refresh_token_accounts() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let mut reused = stored_entry(Some(false), Some("2099-01-01T00:00:00.000Z"));
            reused.label = "devbench.10@astronlab.com_free".to_string();
            reused.email = "devbench.10@astronlab.com".to_string();
            reused.account_id = "acct-reused".to_string();
            reused.auth = make_auth("devbench.10@astronlab.com", "acct-reused", "free");
            reused.auth.tokens.account_id = "acct-reused".to_string();
            reused.last_quota_blocker = Some(
                "Token refresh failed (401): refresh_token_reused: previous refresh token already rotated."
                    .to_string(),
            );

            write_rotate_state_json(&json!({
                "families": {
                    "dev-1::devbench.{n}@astronlab.com": {
                        "profile_name": "dev-1",
                        "template": "devbench.{n}@astronlab.com",
                        "next_suffix": 11,
                        "max_skipped_slots": 0,
                        "created_at": "2026-04-13T05:00:00.000Z",
                        "updated_at": "2026-04-21T00:00:00.000Z",
                        "last_created_email": "devbench.10@astronlab.com",
                        "relogin": [],
                        "suspend_domain_on_terminal_refresh_failure": true
                    }
                }
            }))?;
            save_pool(&Pool {
                active_index: 0,
                accounts: vec![reused],
            })?;

            let output = strip_ansi(&cmd_list()?);
            assert!(!output.contains("devbench.10@astronlab.com_free"));

            let state = load_rotate_state_json()?;
            let accounts = state["accounts"].as_array().expect("accounts");
            assert_eq!(accounts.len(), 1);
            assert_eq!(accounts[0]["email"], "devbench.10@astronlab.com");
            assert!(accounts[0]["auth"]["tokens"]["access_token"]
                .as_str()
                .is_some());
            assert_eq!(accounts[0]["auth"]["tokens"]["refresh_token"], "refresh");
            assert_eq!(
                state["domain"]["astronlab.com"]["rotation_enabled"],
                Value::Bool(false)
            );
            assert_eq!(
                state["families"]["dev-1::devbench.{n}@astronlab.com"]["relogin"]
                    .as_array()
                    .map(|entries| entries.iter().filter_map(Value::as_str).collect::<Vec<_>>()),
                Some(vec!["devbench.10@astronlab.com"])
            );
            assert_eq!(
                state["families"]["dev-1::devbench.{n}@astronlab.com"]
                    ["suspend_domain_on_terminal_refresh_failure"],
                Value::Bool(true)
            );
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("list should prune reused refresh-token accounts");
    }

    #[test]
    fn record_removed_account_uses_current_relogin_shape() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            write_rotate_state_json(&json!({
                "families": {
                    "dev-1::devbench.{n}@astronlab.com": {
                        "profile_name": "dev-1",
                        "template": "devbench.{n}@astronlab.com",
                        "next_suffix": 10,
                        "max_skipped_slots": 0,
                        "created_at": "2026-04-13T05:00:00.000Z",
                        "updated_at": "2026-04-14T06:11:25.913Z",
                        "last_created_email": "devbench.9@astronlab.com",
                        "relogin": []
                    }
                }
            }))?;

            assert!(!family_suspends_domain_on_terminal_refresh_failure(
                "devbench.9@astronlab.com"
            )?);
            assert!(record_removed_account("devbench.9@astronlab.com")?);

            let state = load_rotate_state_json()?;
            assert_eq!(
                state["families"]["dev-1::devbench.{n}@astronlab.com"]["relogin"],
                json!(["devbench.9@astronlab.com"])
            );
            assert_eq!(
                state["families"]["dev-1::devbench.{n}@astronlab.com"]
                    ["suspend_domain_on_terminal_refresh_failure"],
                Value::Null
            );
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("record_removed_account should keep current relogin shape");
    }

    #[test]
    fn cmd_list_sorts_total_accounts_by_quota_refresh_eta() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let mut later = stored_entry(Some(true), Some("2026-04-09T02:00:00.000Z"));
            later.label = "dev.later@astronlab.com_free".to_string();
            later.email = "dev.later@astronlab.com".to_string();
            later.account_id = "acct-later".to_string();
            later.last_quota_summary = Some("7d 88% left".to_string());
            later.last_quota_primary_left_percent = Some(88);
            later.last_quota_next_refresh_at = Some("2099-01-03T00:00:00.000Z".to_string());

            let mut unknown = stored_entry(None, None);
            unknown.label = "dev.unknown@astronlab.com_free".to_string();
            unknown.email = "dev.unknown@astronlab.com".to_string();
            unknown.account_id = "acct-unknown".to_string();

            let mut sooner = stored_entry(Some(false), Some("2026-04-09T02:00:00.000Z"));
            sooner.label = "dev.sooner@astronlab.com_free".to_string();
            sooner.email = "dev.sooner@astronlab.com".to_string();
            sooner.account_id = "acct-sooner".to_string();
            sooner.last_quota_summary = Some("7d 0% left".to_string());
            sooner.last_quota_blocker = Some("7d quota exhausted, resets in 1d".to_string());
            sooner.last_quota_primary_left_percent = Some(0);
            sooner.last_quota_next_refresh_at = Some("2099-01-01T00:00:00.000Z".to_string());

            save_pool(&Pool {
                active_index: 2,
                accounts: vec![later, unknown, sooner],
            })?;

            let output = strip_ansi(&cmd_list()?);
            let total_index = output.find("Total Accounts").expect("total section");
            let healthy_index = output
                .find("Healthy Accounts (1 account(s))")
                .expect("healthy section");
            let total_section = &output[total_index..healthy_index];

            let sooner_index = total_section
                .find("dev.sooner@astronlab.com_free")
                .expect("sooner account");
            let later_index = total_section
                .find("dev.later@astronlab.com_free")
                .expect("later account");
            let unknown_index = total_section
                .find("dev.unknown@astronlab.com_free")
                .expect("unknown account");

            assert!(sooner_index < later_index);
            assert!(later_index < unknown_index);
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("list should sort total accounts by quota refresh eta");
    }

    #[test]
    fn cmd_list_shows_next_quota_refresh_eta_when_available() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let mut entry = stored_entry(Some(true), Some("2026-04-09T02:00:00.000Z"));
            entry.label = "dev.eta@astronlab.com_free".to_string();
            entry.email = "dev.eta@astronlab.com".to_string();
            entry.account_id = "acct-eta".to_string();
            entry.last_quota_summary = Some("7d 88% left".to_string());
            entry.last_quota_primary_left_percent = Some(88);
            entry.last_quota_next_refresh_at = Some("2099-01-03T00:00:00.000Z".to_string());

            save_pool(&Pool {
                active_index: 0,
                accounts: vec![entry],
            })?;

            let output = strip_ansi(&cmd_list()?);

            assert!(output.contains("| next refresh 2099-01-03T00:00:00.000Z"));
            assert!(!output.contains("\n    next refresh"));
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("list should show next quota refresh eta");
    }

    #[test]
    fn cmd_list_stream_emits_account_lines_before_slow_quota_refresh_finishes() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_usage_url = std::env::var_os("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let mut stale = stored_entry(Some(true), Some("2026-04-07T12:00:00.000Z"));
            stale.email = "dev.61@astronlab.com".to_string();
            stale.account_id = "acct-61".to_string();
            stale.label = "dev.61@astronlab.com_free".to_string();
            stale.auth = make_auth("dev.61@astronlab.com", "acct-61", "free");
            stale.auth.tokens.account_id = "acct-61".to_string();
            stale.last_quota_summary = Some("5h 99% left".to_string());
            stale.last_quota_primary_left_percent = Some(99);

            save_pool(&Pool {
                active_index: 0,
                accounts: vec![stale],
            })?;

            let (usage_url, handle) = spawn_usage_server_with_delay(
                json!({
                    "user_id": "user-61",
                    "account_id": "acct-61",
                    "email": "dev.61@astronlab.com",
                    "plan_type": "free",
                    "rate_limit": {
                        "allowed": true,
                        "limit_reached": false,
                        "primary_window": {
                            "used_percent": 80.0,
                            "limit_window_seconds": 18000,
                            "reset_after_seconds": 3600,
                            "reset_at": 0
                        },
                        "secondary_window": null
                    },
                    "code_review_rate_limit": null,
                    "additional_rate_limits": null,
                    "credits": null,
                    "promo": null
                })
                .to_string(),
                StdDuration::from_millis(400),
            );
            unsafe {
                std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", &usage_url);
            }

            let writer = SharedWriter::default();
            let probe_writer = writer.clone();
            let join = thread::spawn(move || {
                let mut writer = writer;
                cmd_list_stream(&mut writer)
            });

            let mut partial = String::new();
            for _ in 0..10 {
                thread::sleep(StdDuration::from_millis(100));
                partial = probe_writer.snapshot();
                if partial.contains("Codex OAuth Account Pool") {
                    break;
                }
            }
            assert!(partial.contains("Codex OAuth Account Pool"));
            assert!(partial.contains("dev.61@astronlab.com"));
            assert!(!partial.contains("    \u{1b}[2mquota"));

            join.join()
                .expect("list stream thread")
                .expect("list stream");
            handle.join().expect("usage server should finish");

            let final_output = probe_writer.snapshot();
            assert!(final_output.contains("5h 20% left"));
            Ok(())
        })();

        restore_env_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", previous_usage_url);
        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("list stream should emit header before slow refresh completes");
    }

    #[test]
    fn sync_pool_active_account_adds_missing_current_auth_to_pool() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        }

        let result = (|| -> Result<()> {
            let mut pool = Pool {
                active_index: 0,
                accounts: vec![stored_entry(Some(true), None)],
            };

            let changed = sync_pool_current_auth_from_auth(
                &mut pool,
                make_auth("dev.35@astronlab.com", "acct-35", "free"),
                true,
            )?;

            assert!(changed);
            assert_eq!(pool.accounts.len(), 2);
            assert_eq!(pool.active_index, 1);
            assert_eq!(pool.accounts[1].email, "dev.35@astronlab.com");
            assert_eq!(pool.accounts[1].account_id, "acct-35");
            assert_eq!(pool.accounts[1].label, "dev.35@astronlab.com_free");
            Ok(())
        })();

        match previous_rotate_home {
            Some(value) => unsafe {
                std::env::set_var("CODEX_ROTATE_HOME", value);
            },
            None => unsafe {
                std::env::remove_var("CODEX_ROTATE_HOME");
            },
        }
        result.expect("sync should materialize current auth into pool");
    }

    #[test]
    fn sync_pool_active_account_skips_unknown_email_auth() {
        let mut pool = Pool {
            active_index: 0,
            accounts: vec![stored_entry(Some(true), None)],
        };

        let changed = sync_pool_current_auth_from_auth(
            &mut pool,
            make_auth("unknown", "acct-35", "free"),
            true,
        )
        .expect("sync should succeed");

        assert!(!changed);
        assert_eq!(pool.accounts.len(), 1);
        assert_eq!(pool.active_index, 0);
    }

    #[test]
    fn sync_pool_active_account_prefers_existing_active_match_over_duplicate() {
        let primary = configured_entry(
            "dev.5@hotspotprime.com",
            "acct-shared",
            "team",
            Some(true),
            Some("2026-04-07T00:00:00.000Z"),
        );
        let duplicate = primary.clone();
        let other = configured_entry(
            "dev.2.astronlab@gmail.com",
            "acct-2",
            "free",
            Some(true),
            Some("2026-04-07T00:00:00.000Z"),
        );

        let mut pool = Pool {
            active_index: 2,
            accounts: vec![duplicate, other, primary],
        };

        let changed = sync_pool_current_auth_from_auth(
            &mut pool,
            make_auth("dev.5@hotspotprime.com", "acct-shared", "team"),
            true,
        )
        .expect("sync should succeed");

        assert!(!changed);
        assert_eq!(pool.active_index, 2);
        assert_eq!(
            pool.accounts[pool.active_index].email,
            "dev.5@hotspotprime.com"
        );
    }

    #[test]
    fn sync_pool_active_account_from_current_auth_persists_missing_auth_into_pool() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let paths = resolve_paths()?;
            if let Some(parent) = paths.codex_auth_file.parent() {
                std::fs::create_dir_all(parent).expect("create auth parent");
            }
            write_codex_auth(
                &paths.codex_auth_file,
                &make_auth("dev.36@astronlab.com", "acct-36", "free"),
            )?;

            save_pool(&Pool {
                active_index: 0,
                accounts: vec![stored_entry(Some(true), None)],
            })?;

            let changed = sync_pool_active_account_from_current_auth()?;
            let pool = load_pool()?;

            assert!(changed);
            assert_eq!(pool.accounts.len(), 2);
            assert_eq!(pool.active_index, 1);
            assert_eq!(pool.accounts[1].email, "dev.36@astronlab.com");
            assert_eq!(pool.accounts[1].account_id, "acct-36");
            assert_eq!(pool.accounts[1].label, "dev.36@astronlab.com_free");
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("current auth sync should persist the missing pool entry");
    }

    #[test]
    fn sync_pool_current_auth_into_pool_without_activation_preserves_active_index() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let paths = resolve_paths()?;
            if let Some(parent) = paths.codex_auth_file.parent() {
                std::fs::create_dir_all(parent).expect("create auth parent");
            }
            write_codex_auth(
                &paths.codex_auth_file,
                &make_auth("dev.36@astronlab.com", "acct-36", "free"),
            )?;

            save_pool(&Pool {
                active_index: 0,
                accounts: vec![stored_entry(Some(true), None)],
            })?;

            let changed = sync_pool_current_auth_into_pool_without_activation()?;
            let pool = load_pool()?;

            assert!(changed);
            assert_eq!(pool.accounts.len(), 2);
            assert_eq!(pool.active_index, 0);
            assert_eq!(pool.accounts[1].email, "dev.36@astronlab.com");
            assert_eq!(pool.accounts[1].account_id, "acct-36");
            assert_eq!(pool.accounts[1].label, "dev.36@astronlab.com_free");
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("passive current auth sync should preserve the active pool entry");
    }

    #[test]
    fn sync_pool_current_auth_into_pool_without_activation_clears_family_relogin_email() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let paths = resolve_paths()?;
            if let Some(parent) = paths.codex_auth_file.parent() {
                std::fs::create_dir_all(parent).expect("create auth parent");
            }
            write_codex_auth(
                &paths.codex_auth_file,
                &make_auth("dev.36@astronlab.com", "acct-36", "free"),
            )?;

            let existing = configured_entry("dev.36@astronlab.com", "acct-36", "free", None, None);
            write_rotate_state_json(&json!({
                "accounts": [existing],
                "active_index": 0,
                "version": 9,
                "default_create_template": "dev.{n}@astronlab.com",
                "families": {
                    "dev-1::dev.{n}@astronlab.com": {
                        "profile_name": "dev-1",
                        "template": "dev.{n}@astronlab.com",
                        "next_suffix": 37,
                        "max_skipped_slots": 0,
                        "relogin": ["dev.36@astronlab.com"],
                        "last_created_email": "dev.36@astronlab.com",
                        "created_at": "2026-04-05T00:00:00.000Z",
                        "updated_at": "2026-04-05T00:00:00.000Z"
                    }
                }
            }))?;

            let _ = sync_pool_current_auth_into_pool_without_activation()?;
            let state = load_rotate_state_json()?;

            assert_eq!(
                state["families"]["dev-1::dev.{n}@astronlab.com"]["relogin"],
                json!([])
            );
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("passive current auth sync should clear matching family relogin entries");
    }

    #[test]
    fn restore_codex_auth_from_active_pool_restores_missing_auth_file() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            save_pool(&Pool {
                active_index: 0,
                accounts: vec![AccountEntry {
                    label: "dev.restore@astronlab.com_free".to_string(),
                    alias: None,
                    email: "dev.restore@astronlab.com".to_string(),
                    account_id: "acct-restore".to_string(),
                    plan_type: "free".to_string(),
                    auth: make_auth("dev.restore@astronlab.com", "acct-restore", "free"),
                    added_at: "2026-04-15T00:00:00.000Z".to_string(),
                    last_quota_usable: None,
                    last_quota_summary: None,
                    last_quota_blocker: None,
                    last_quota_checked_at: None,
                    last_quota_primary_left_percent: None,
                    last_quota_next_refresh_at: None,
                    persona: None,
                }],
            })?;

            let paths = resolve_paths()?;
            assert!(!paths.codex_auth_file.exists());

            let restored = restore_codex_auth_from_active_pool()?;
            assert!(restored);
            assert!(paths.codex_auth_file.exists());

            let auth = load_codex_auth(&paths.codex_auth_file)?;
            assert_eq!(extract_account_id_from_auth(&auth), "acct-restore");
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("active pool auth should restore missing codex auth");
    }

    #[test]
    fn normalize_pool_entries_preserves_non_gmail_target_when_auth_is_gmail() {
        let mut pool = Pool {
            active_index: 0,
            accounts: vec![AccountEntry {
                label: "devbench.12@astronlab.com_free".to_string(),
                alias: None,
                email: "devbench.12@astronlab.com".to_string(),
                account_id: "acct-12".to_string(),
                plan_type: "free".to_string(),
                auth: make_auth("1.dev.astronlab@gmail.com", "acct-12", "free"),
                added_at: "2026-04-12T00:00:00.000Z".to_string(),
                last_quota_usable: None,
                last_quota_summary: None,
                last_quota_blocker: None,
                last_quota_checked_at: None,
                last_quota_primary_left_percent: None,
                last_quota_next_refresh_at: None,
                persona: None,
            }],
        };

        let changed = normalize_pool_entries(&mut pool);

        assert!(changed);
        assert_eq!(pool.accounts[0].email, "devbench.12@astronlab.com");
        assert_eq!(pool.accounts[0].label, "devbench.12@astronlab.com_free");
        assert!(!normalize_pool_entries(&mut pool));
    }

    #[test]
    fn normalize_pool_entries_marks_weekly_exhausted_cached_accounts_unusable() {
        let mut pool = Pool {
            active_index: 0,
            accounts: vec![AccountEntry {
                label: "dev.4@hotspotprime.com_team".to_string(),
                alias: None,
                email: "dev.4@hotspotprime.com".to_string(),
                account_id: "acct-4".to_string(),
                plan_type: "team".to_string(),
                auth: make_auth("dev.4@hotspotprime.com", "acct-4", "team"),
                added_at: "2026-04-18T00:00:00.000Z".to_string(),
                last_quota_usable: Some(true),
                last_quota_summary: Some("5h 100% left, 5h | week 0% left, 3d 11h".to_string()),
                last_quota_blocker: None,
                last_quota_checked_at: Some("2026-04-18T02:01:57.804Z".to_string()),
                last_quota_primary_left_percent: Some(100),
                last_quota_next_refresh_at: Some("2026-04-18T02:02:57.804Z".to_string()),
                persona: None,
            }],
        };

        let changed = normalize_pool_entries(&mut pool);

        assert!(changed);
        assert_eq!(pool.accounts[0].last_quota_usable, Some(false));
    }

    #[test]
    fn cmd_add_expected_email_preserves_target_email_against_provider_gmail_auth() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let paths = resolve_paths()?;
            if let Some(parent) = paths.codex_auth_file.parent() {
                std::fs::create_dir_all(parent).expect("create auth parent");
            }
            write_codex_auth(
                &paths.codex_auth_file,
                &make_auth("1.dev.astronlab@gmail.com", "acct-devbench-12", "free"),
            )?;

            let output = cmd_add_expected_email("devbench.12@astronlab.com", None)?;
            let pool = load_pool()?;

            assert!(strip_ansi(&output).contains("devbench.12@astronlab.com_free"));
            assert_eq!(pool.accounts.len(), 1);
            assert_eq!(pool.active_index, 0);
            assert_eq!(pool.accounts[0].email, "devbench.12@astronlab.com");
            assert_eq!(pool.accounts[0].label, "devbench.12@astronlab.com_free");
            assert_eq!(pool.accounts[0].account_id, "acct-devbench-12");
            assert_eq!(
                extract_email_from_auth(&pool.accounts[0].auth),
                "1.dev.astronlab@gmail.com"
            );
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("cmd_add_expected_email should preserve the target email");
    }

    #[test]
    fn cmd_add_expected_email_preserves_target_gmail_plus_family_against_provider_gmail_auth() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let paths = resolve_paths()?;
            if let Some(parent) = paths.codex_auth_file.parent() {
                std::fs::create_dir_all(parent).expect("create auth parent");
            }
            write_codex_auth(
                &paths.codex_auth_file,
                &make_auth("1.dev.astronlab+6@gmail.com", "acct-dev-gmail-6", "free"),
            )?;

            let output = cmd_add_expected_email("dev3astronlab+6@gmail.com", None)?;
            let pool = load_pool()?;

            assert!(strip_ansi(&output).contains("dev3astronlab+6@gmail.com_free"));
            assert_eq!(pool.accounts.len(), 1);
            assert_eq!(pool.active_index, 0);
            assert_eq!(pool.accounts[0].email, "dev3astronlab+6@gmail.com");
            assert_eq!(pool.accounts[0].label, "dev3astronlab+6@gmail.com_free");
            assert_eq!(pool.accounts[0].account_id, "acct-dev-gmail-6");
            assert_eq!(
                extract_email_from_auth(&pool.accounts[0].auth),
                "1.dev.astronlab+6@gmail.com"
            );
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("cmd_add_expected_email should preserve the gmail-plus target email");
    }

    #[test]
    fn current_pool_overview_counts_cached_healthy_accounts() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            save_pool(&Pool {
                active_index: 1,
                accounts: vec![
                    stored_entry(Some(false), Some("2026-04-08T12:00:00.000Z")),
                    stored_entry(Some(true), Some("2026-04-08T12:00:00.000Z")),
                    stored_entry(Some(true), Some("2026-04-08T12:00:00.000Z")),
                    stored_entry(None, None),
                ],
            })?;

            let overview = current_pool_overview()?;
            assert_eq!(overview.inventory_count, 4);
            assert_eq!(overview.inventory_active_slot, Some(2));
            assert_eq!(overview.inventory_healthy_count, 2);
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("overview should count healthy accounts");
    }

    #[test]
    fn current_pool_overview_hides_healthy_accounts_from_disabled_domains() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let mut healthy_disabled = stored_entry(Some(true), Some("2026-04-08T12:00:00.000Z"));
            healthy_disabled.label = "dev.hidden@astronlab.com_free".to_string();
            healthy_disabled.email = "dev.hidden@astronlab.com".to_string();
            healthy_disabled.account_id = "acct-hidden".to_string();
            healthy_disabled.auth = make_auth("dev.hidden@astronlab.com", "acct-hidden", "free");
            healthy_disabled.auth.tokens.account_id = "acct-hidden".to_string();

            let mut healthy_enabled = stored_entry(Some(true), Some("2026-04-08T12:00:00.000Z"));
            healthy_enabled.label = "dev.visible@gmail.com_plus".to_string();
            healthy_enabled.email = "dev.visible@gmail.com".to_string();
            healthy_enabled.account_id = "acct-visible".to_string();
            healthy_enabled.auth = make_auth("dev.visible@gmail.com", "acct-visible", "plus");
            healthy_enabled.auth.tokens.account_id = "acct-visible".to_string();

            let mut exhausted_disabled =
                stored_entry(Some(false), Some("2026-04-08T12:00:00.000Z"));
            exhausted_disabled.label = "dev.exhausted@astronlab.com_free".to_string();
            exhausted_disabled.email = "dev.exhausted@astronlab.com".to_string();
            exhausted_disabled.account_id = "acct-exhausted".to_string();
            exhausted_disabled.auth =
                make_auth("dev.exhausted@astronlab.com", "acct-exhausted", "free");
            exhausted_disabled.auth.tokens.account_id = "acct-exhausted".to_string();

            save_pool(&Pool {
                active_index: 0,
                accounts: vec![healthy_disabled, healthy_enabled, exhausted_disabled],
            })?;
            write_disabled_domain_state()?;
            assert!(load_disabled_rotation_domains()?.contains("astronlab.com"));

            let overview = current_pool_overview()?;
            assert_eq!(overview.inventory_count, 2);
            assert_eq!(overview.inventory_active_slot, None);
            assert_eq!(overview.inventory_healthy_count, 1);
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("overview should hide healthy disabled-domain accounts");
    }

    #[test]
    fn rotate_next_skips_disabled_domain_accounts() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_usage_url = std::env::var_os("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            write_disabled_domain_state()?;
            save_pool(&Pool {
                active_index: 0,
                accounts: vec![
                    configured_entry(
                        "dev.1@astronlab.com",
                        "acct-1",
                        "free",
                        Some(false),
                        Some("2026-04-07T00:00:00.000Z"),
                    ),
                    configured_entry(
                        "dev.user@gmail.com",
                        "acct-gmail",
                        "free",
                        Some(true),
                        Some("2026-04-07T00:00:00.000Z"),
                    ),
                ],
            })?;

            let (usage_url, handle) = spawn_usage_server(
                json!({
                    "user_id": "user-gmail",
                    "account_id": "acct-gmail",
                    "email": "dev.user@gmail.com",
                    "plan_type": "free",
                    "rate_limit": {
                        "allowed": true,
                        "limit_reached": false,
                        "primary_window": {
                            "used_percent": 20.0,
                            "limit_window_seconds": 18000,
                            "reset_after_seconds": 7200,
                            "reset_at": 0
                        },
                        "secondary_window": null
                    },
                    "code_review_rate_limit": null,
                    "additional_rate_limits": null,
                    "credits": null,
                    "promo": null
                })
                .to_string(),
            );
            unsafe {
                std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", &usage_url);
            }

            let output = rotate_next_internal_with_progress(None)?;
            handle.join().expect("usage server should finish");

            match output {
                NextResult::Rotated { summary, .. } => {
                    assert_eq!(summary.email, "dev.user@gmail.com");
                }
                _ => panic!("expected rotation result"),
            }

            let refreshed = load_pool()?;
            assert_eq!(refreshed.active_index, 1);
            assert_eq!(refreshed.accounts[1].email, "dev.user@gmail.com");
            Ok(())
        })();

        restore_env_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", previous_usage_url);
        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("next should skip disabled domains");
    }

    #[test]
    fn rotate_next_fails_when_only_disabled_targets_remain() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            write_disabled_domain_state()?;
            save_pool(&Pool {
                active_index: 0,
                accounts: vec![
                    configured_entry(
                        "dev.1@astronlab.com",
                        "acct-1",
                        "free",
                        Some(true),
                        Some("2026-04-07T00:00:00.000Z"),
                    ),
                    configured_entry(
                        "dev.2@astronlab.com",
                        "acct-2",
                        "free",
                        Some(true),
                        Some("2026-04-07T00:00:00.000Z"),
                    ),
                ],
            })?;

            let error = match rotate_next_internal_with_progress(None) {
                Ok(_) => panic!("expected disabled-domain rotation error"),
                Err(error) => error,
            };
            assert!(error
                .to_string()
                .contains("No rotation target is available because rotation is disabled"));
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("next should fail when all targets are disabled");
    }

    #[test]
    fn cmd_prev_fails_when_only_disabled_targets_remain() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            write_disabled_domain_state()?;
            save_pool(&Pool {
                active_index: 1,
                accounts: vec![
                    configured_entry(
                        "dev.1@astronlab.com",
                        "acct-1",
                        "free",
                        Some(true),
                        Some("2026-04-07T00:00:00.000Z"),
                    ),
                    configured_entry(
                        "dev.user@gmail.com",
                        "acct-gmail",
                        "free",
                        Some(true),
                        Some("2026-04-07T00:00:00.000Z"),
                    ),
                ],
            })?;

            let error = cmd_prev().unwrap_err();
            assert!(error
                .to_string()
                .contains("No rotation target is available because rotation is disabled"));
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("prev should fail when all previous targets are disabled");
    }

    #[test]
    fn other_usable_account_exists_ignores_disabled_domains() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let codex_home = tempdir.path().join("codex-home");
        std::fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            write_disabled_domain_state()?;
            save_pool(&Pool {
                active_index: 0,
                accounts: vec![
                    configured_entry(
                        "dev.user@gmail.com",
                        "acct-gmail",
                        "free",
                        Some(false),
                        Some("2026-04-07T00:00:00.000Z"),
                    ),
                    configured_entry(
                        "dev.1@astronlab.com",
                        "acct-1",
                        "free",
                        Some(true),
                        Some("2026-04-07T00:00:00.000Z"),
                    ),
                ],
            })?;

            assert!(!other_usable_account_exists()?);
            Ok(())
        })();

        restore_env_var("CODEX_HOME", previous_codex_home);
        restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
        result.expect("disabled domains should not count as reusable accounts");
    }

    #[test]
    fn rollback_prepared_rotation_restores_previous_auth_and_active_index() {
        let _guard = RotateHomeGuard::enter("codex-rotate-rollback-prepared");
        let previous = configured_entry("dev.1@astronlab.com", "acct-1", "free", Some(true), None);
        let target = configured_entry("dev.2@astronlab.com", "acct-2", "free", Some(true), None);

        let pool = Pool {
            active_index: 0,
            accounts: vec![previous.clone(), target.clone()],
        };
        save_pool(&pool).expect("save initial pool");

        let paths = resolve_paths().expect("resolve paths");
        if let Some(parent) = paths.codex_auth_file.parent() {
            std::fs::create_dir_all(parent).expect("create auth parent");
        }
        write_codex_auth(&paths.codex_auth_file, &previous.auth).expect("write initial auth");

        let prepared = PreparedRotation {
            action: PreparedRotationAction::Switch,
            pool: pool.clone(),
            previous_index: 0,
            target_index: 1,
            previous: previous.clone(),
            target: target.clone(),
            message: "rotating".to_string(),
            persist_pool: false,
        };

        // Simulate a partial activation: auth is written but pool is not committed
        write_codex_auth(&paths.codex_auth_file, &target.auth).expect("write target auth");

        rollback_prepared_rotation(&prepared).expect("rollback");

        let restored_auth = load_codex_auth(&paths.codex_auth_file).expect("load restored auth");
        assert_eq!(extract_account_id_from_auth(&restored_auth), "acct-1");

        let restored_pool = load_pool().expect("load restored pool");
        assert_eq!(restored_pool.active_index, 0);
    }

    #[test]
    fn validate_persona_egress_fails_when_region_mismatches_in_validate_mode() {
        let mut persona = PersonaEntry::default();
        persona.expected_region_code = Some("US".to_string());

        // We will mock the egress check to return "GB"
        let result =
            validate_persona_egress_with_actual(&persona, VmExpectedEgressMode::Validate, "GB");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expected US, found GB"));
    }

    #[test]
    fn validate_persona_egress_succeeds_when_region_matches_in_validate_mode() {
        let mut persona = PersonaEntry::default();
        persona.expected_region_code = Some("US".to_string());

        let result =
            validate_persona_egress_with_actual(&persona, VmExpectedEgressMode::Validate, "US");
        assert!(result.is_ok());
    }

    #[test]
    fn validate_persona_egress_succeeds_in_provision_only_mode_even_if_region_mismatches() {
        let mut persona = PersonaEntry::default();
        persona.expected_region_code = Some("US".to_string());

        let result = validate_persona_egress_with_actual(
            &persona,
            VmExpectedEgressMode::ProvisionOnly,
            "GB",
        );
        assert!(result.is_ok());
    }
}
