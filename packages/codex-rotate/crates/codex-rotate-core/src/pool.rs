use std::collections::HashSet;
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
use crate::quota::{
    describe_quota_blocker, format_compact_quota, get_quota_left, has_usable_quota,
    quota_next_refresh_at, UsageCredits, UsageResponse, UsageWindow,
};
use crate::state::{load_rotate_state_json, write_rotate_state_json};
use crate::workflow::{
    cmd_create, cmd_create_with_progress, create_next_fallback_options, extract_email_domain,
    is_auto_create_retry_stopped_for_reusable_account, load_disabled_rotation_domains,
    reconcile_added_account_credential_state, record_deleted_account,
};

const DEFAULT_OAUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const OAUTH_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const WHAM_USAGE_URL: &str = "https://chatgpt.com/backend-api/wham/usage";
const REQUEST_TIMEOUT_SECONDS: u64 = 8;
const LIST_STALE_QUOTA_REFRESH_LIMIT: usize = 2;

const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const CYAN: &str = "\x1b[36m";
const RESET: &str = "\x1b[0m";

fn account_rotation_enabled(disabled_domains: &HashSet<String>, email: &str) -> bool {
    extract_email_domain(email)
        .map(|domain| !disabled_domains.contains(&domain))
        .unwrap_or(true)
}

fn inventory_account_visible(disabled_domains: &HashSet<String>, entry: &AccountEntry) -> bool {
    account_rotation_enabled(disabled_domains, &entry.email)
        || entry.last_quota_usable != Some(true)
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
    let duplicate_index = pool.accounts.iter().position(|account| {
        account.account_id == account_id || account.auth.tokens.account_id == account_id
    });

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
    let index = find_pool_account_index_by_identity(&pool, &account_id, &normalized_expected_email)
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
    let disabled_domains = load_disabled_rotation_domains()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    if pool.accounts.is_empty() {
        return Err(anyhow!("No accounts in pool. Run: codex-rotate add"));
    }

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

pub fn cmd_list_stream(writer: &mut dyn Write) -> Result<()> {
    let mut emitter = LineEmitter::streaming(writer);
    cmd_list_impl(&mut emitter)
}

fn cmd_list_impl(output: &mut LineEmitter<'_>) -> Result<()> {
    let paths = resolve_paths()?;
    let disabled_domains = load_disabled_rotation_domains()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    if pool.accounts.is_empty() {
        output.push_line(format!(
            "{YELLOW}WARN{RESET} No accounts in pool. Add one with: codex-rotate add"
        ))?;
        if dirty {
            save_pool(&pool)?;
        }
        return Ok(());
    }
    let refresh_order =
        build_list_quota_refresh_order(&pool, Utc::now(), LIST_STALE_QUOTA_REFRESH_LIMIT);
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
        if !inventory_account_visible(&disabled_domains, &pool.accounts[index]) {
            continue;
        }
        let is_active = index == pool.active_index;
        let account_header_line = build_list_account_header_line(&pool.accounts[index], is_active);
        output.push_line(account_header_line.clone())?;

        if refresh_indices.contains(&index) {
            let inspection =
                inspect_account(&mut pool.accounts[index], &paths.codex_auth_file, is_active)?;
            dirty |= inspection.updated;
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

fn build_list_quota_refresh_order(
    pool: &Pool,
    now: DateTime<Utc>,
    max_refreshes: usize,
) -> Vec<usize> {
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

    if max_refreshes == 0 {
        return refreshes;
    }

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

    refreshes.extend(
        candidates
            .into_iter()
            .take(max_refreshes)
            .map(|(index, _, _)| index),
    );

    refreshes
}

fn cached_quota_state_is_stale(entry: &AccountEntry, now: DateTime<Utc>) -> bool {
    let Some(next_refresh_at) = effective_cached_quota_next_refresh_at(entry) else {
        return true;
    };
    now >= next_refresh_at
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
    let paths = resolve_paths()?;
    let disabled_domains = load_disabled_rotation_domains()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    if dirty {
        save_pool(&pool)?;
    }
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

        live_pool_index = find_pool_account_index_by_identity(&pool, &account_id, &email);

        if let Some(index) = live_pool_index {
            let inspection =
                inspect_account(&mut pool.accounts[index], &paths.codex_auth_file, true)?;
            dirty |= inspection.updated;
            if let Some(usage) = inspection.usage.as_ref() {
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
    record_deleted_account(&removed.email)?;
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
    let disabled_domains = load_disabled_rotation_domains()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;

    if pool.accounts.len() <= 1 {
        if dirty {
            save_pool(&pool)?;
        }
        return Ok(false);
    }

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

pub(crate) fn save_pool(pool: &Pool) -> Result<()> {
    let mut state = load_rotate_state_json()?;
    if !state.is_object() {
        state = Value::Object(Map::new());
    }
    let object = state
        .as_object_mut()
        .expect("rotate state must be a JSON object");
    object.insert(
        "active_index".to_string(),
        Value::Number(pool.active_index.into()),
    );
    object.insert(
        "accounts".to_string(),
        serde_json::to_value(&pool.accounts)?,
    );
    write_rotate_state_json(&state)
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

fn extract_plan_from_auth(auth: &CodexAuth) -> String {
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

fn normalize_plan_type_for_label(plan_type: &str) -> String {
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

fn should_preserve_expected_email(existing_email: &str, auth_email: &str) -> bool {
    let normalized_existing = existing_email.trim().to_lowercase();
    let normalized_auth = auth_email.trim().to_lowercase();
    !normalized_existing.is_empty()
        && normalized_existing != "unknown"
        && normalized_existing != normalized_auth
        && !normalized_existing.ends_with("@gmail.com")
        && normalized_auth.ends_with("@gmail.com")
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
    if !auth_path.exists() {
        return Ok(false);
    }
    let current_auth = load_codex_auth(auth_path)?;
    sync_pool_active_account_from_auth(pool, current_auth)
}

fn sync_pool_active_account_from_auth(pool: &mut Pool, current_auth: CodexAuth) -> Result<bool> {
    let current_account_id = extract_account_id_from_auth(&current_auth);
    let current_email = extract_email_from_auth(&current_auth);
    let normalized_email = normalize_email_for_label(&current_email);

    if normalized_email == "unknown" {
        return Ok(false);
    }

    let current_plan_type = extract_plan_from_auth(&current_auth);
    let current_label = build_account_label(&current_email, &current_plan_type);
    let mut changed = false;

    let Some(current_index) =
        find_pool_account_index_by_identity(pool, &current_account_id, &current_email)
    else {
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
        });
        pool.active_index = pool.accounts.len() - 1;
        let _ = reconcile_added_account_credential_state(&pool.accounts[pool.active_index])?;
        return Ok(true);
    };

    if pool.active_index != current_index {
        pool.active_index = current_index;
        changed = true;
    }
    Ok(apply_auth_to_account(&mut pool.accounts[current_index], current_auth) || changed)
}

fn find_pool_account_index_by_identity(
    pool: &Pool,
    account_id: &str,
    email: &str,
) -> Option<usize> {
    if let Some(index) = pool.accounts.iter().position(|entry| {
        entry.account_id == account_id || entry.auth.tokens.account_id == account_id
    }) {
        return Some(index);
    }

    let normalized_email = email.trim().to_lowercase();
    if normalized_email.is_empty() || normalized_email == "unknown" {
        return None;
    }

    pool.accounts
        .iter()
        .position(|entry| entry.email.trim().eq_ignore_ascii_case(&normalized_email))
}

pub(crate) fn normalize_pool_entries(pool: &mut Pool) -> bool {
    let mut changed = false;
    for entry in &mut pool.accounts {
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
    }

    let max_active_index = pool.accounts.len().saturating_sub(1);
    let normalized_active_index = pool.active_index.min(max_active_index);
    if pool.active_index != normalized_active_index {
        pool.active_index = normalized_active_index;
        changed = true;
    }
    changed
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
    let next_email = if usage.email.is_empty() {
        entry.email.clone()
    } else if should_preserve_expected_email(&entry.email, &usage.email) {
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

fn write_codex_auth_if_current_account(
    auth_path: &Path,
    account_id: &str,
    auth: &CodexAuth,
) -> Result<bool> {
    if !auth_path.exists() {
        return Ok(false);
    }
    let current_auth = load_codex_auth(auth_path)?;
    if extract_account_id_from_auth(&current_auth) != account_id {
        return Ok(false);
    }
    if current_auth != *auth {
        write_codex_auth(auth_path, auth)?;
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
                updated |=
                    write_codex_auth_if_current_account(auth_path, &entry.account_id, &entry.auth)?;
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
    let probe_order =
        build_reusable_account_probe_order(pool.active_index, pool.accounts.len(), mode);

    for index in probe_order {
        if skip_indices.contains(&index) {
            continue;
        }
        if !account_rotation_enabled(disabled_domains, &pool.accounts[index].email) {
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
    use crate::test_support::ENV_MUTEX;
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
    fn pool_identity_lookup_prefers_account_id_match() {
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
            find_pool_account_index_by_identity(&pool, "acct-27", "dev.26@astronlab.com"),
            Some(1)
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
            find_pool_account_index_by_identity(&pool, "missing", "dev.26@astronlab.com"),
            Some(0)
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

        assert_eq!(build_list_quota_refresh_order(&pool, now, 2), vec![0, 3]);
    }

    #[test]
    fn list_quota_refresh_order_includes_unknown_entries_outside_stale_refresh_cap() {
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

        assert_eq!(build_list_quota_refresh_order(&pool, now, 1), vec![1, 0]);
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

            thread::sleep(StdDuration::from_millis(100));
            let partial = probe_writer.snapshot();
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

            let changed = sync_pool_active_account_from_auth(
                &mut pool,
                make_auth("dev.35@astronlab.com", "acct-35", "free"),
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

        let changed =
            sync_pool_active_account_from_auth(&mut pool, make_auth("unknown", "acct-35", "free"))
                .expect("sync should succeed");

        assert!(!changed);
        assert_eq!(pool.accounts.len(), 1);
        assert_eq!(pool.active_index, 0);
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
            }],
        };

        let changed = normalize_pool_entries(&mut pool);

        assert!(!changed);
        assert_eq!(pool.accounts[0].email, "devbench.12@astronlab.com");
        assert_eq!(pool.accounts[0].label, "devbench.12@astronlab.com_free");
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
}
