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
    MIN_HEALTHY_QUOTA_LEFT_PERCENT,
};
#[cfg(test)]
use crate::state::write_rotate_state_json;
use crate::state::{load_rotate_state_json, update_rotate_state_json, RotateStateOwner};
use crate::workflow::{
    auto_disable_domain_for_account, cmd_create, cmd_create_with_progress,
    create_next_fallback_options, extract_email_domain,
    family_suspends_domain_on_terminal_refresh_failure,
    is_auto_create_retry_stopped_for_reusable_account, load_disabled_rotation_domains,
    load_relogin_account_emails, reconcile_added_account_credential_state, record_removed_account,
};

mod identity;
mod list_status;
mod models;
mod normalization;
mod persistence;
mod quota_refresh;
mod rotation;
mod selection;
mod sync;

use self::identity::*;
#[cfg(test)]
use self::list_status::*;
use self::models::*;
use self::normalization::*;
use self::quota_refresh::*;
use self::selection::*;
use self::sync::*;

pub(crate) use self::identity::{
    account_entry_matches_auth_identity, account_entry_matches_identity,
    format_account_summary_for_display,
};
pub use self::list_status::{
    cmd_list, cmd_list_stream, cmd_list_stream_with_options, cmd_list_with_options, cmd_status,
    cmd_status_stream, current_pool_overview, current_pool_overview_without_activation,
    ListOptions,
};
pub use self::models::*;
pub(crate) use self::normalization::normalize_pool_entries;
pub use self::persistence::{
    load_codex_mode_config, load_codex_mode_config_from_path, load_pool, load_rotation_checkpoint,
    load_rotation_environment_settings, save_pool, save_rotation_checkpoint,
    write_selected_account_auth,
};
pub(crate) use self::quota_refresh::inspect_account;
pub use self::rotation::{
    cmd_next, cmd_next_with_progress, cmd_prev, persist_prepared_rotation_pool,
    prepare_next_rotation_with_progress, prepare_prev_rotation, prepare_set_rotation,
    resolve_pool_account, rollback_prepared_rotation, rotate_next_internal,
    rotate_next_internal_with_progress,
};
pub use self::selection::{
    build_reusable_account_probe_order, find_next_cached_usable_account_index,
    find_next_immediate_round_robin_index,
};
pub(crate) use self::selection::{
    find_next_usable_account, format_short_account_id, resolve_account_selector, AccountSelection,
};
pub(crate) use self::sync::sync_pool_active_account_from_codex;
pub use self::sync::{
    restore_codex_auth_from_active_pool, restore_pool_active_index,
    sync_pool_active_account_from_current_auth,
    sync_pool_current_auth_into_pool_without_activation, validate_persona_egress,
    validate_persona_egress_with_actual,
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

fn account_marked_for_relogin(relogin_accounts: &HashSet<String>, email: &str) -> bool {
    relogin_accounts.contains(&normalize_email_for_label(email))
}

fn inventory_account_visible(disabled_domains: &HashSet<String>, entry: &AccountEntry) -> bool {
    !account_requires_terminal_cleanup(entry)
        && account_rotation_enabled(disabled_domains, &entry.email)
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
    if entry
        .last_quota_primary_left_percent
        .map(|value| (value as f64) < MIN_HEALTHY_QUOTA_LEFT_PERCENT)
        .unwrap_or(false)
    {
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
                .map(|value| value < MIN_HEALTHY_QUOTA_LEFT_PERCENT)
        })
        .any(|is_below_threshold| is_below_threshold)
}

fn is_terminal_refresh_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("refresh_token_invalidated")
        || message.contains("refresh token has been invalidated")
        || message.contains("refresh token already rotated")
        || message.contains("refresh_token_reused")
        || message.contains("token_expired")
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
    Ok(true)
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
    let relogin_accounts = load_relogin_account_emails()?;
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
        &relogin_accounts,
    )?;
    if candidate_dirty {
        save_pool(&pool)?;
    }
    Ok(candidate.is_some())
}

fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests;
