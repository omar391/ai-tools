use std::collections::HashSet;
use std::io::Write;
use std::path::Path;
use std::sync::OnceLock;

use anyhow::{anyhow, Context, Result};
use chrono::{SecondsFormat, Utc};
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
    describe_quota_blocker, format_compact_quota, get_quota_left, has_usable_quota, UsageCredits,
    UsageResponse, UsageWindow,
};
use crate::state::{load_rotate_state_json, write_rotate_state_json};
use crate::workflow::{
    cmd_create, create_next_fallback_options, is_auto_create_retry_stopped_for_reusable_account,
    reconcile_added_account_credential_state,
};

const DEFAULT_OAUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const OAUTH_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const WHAM_USAGE_URL: &str = "https://chatgpt.com/backend-api/wham/usage";
const REQUEST_TIMEOUT_SECONDS: u64 = 8;

const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const CYAN: &str = "\x1b[36m";
const RESET: &str = "\x1b[0m";

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
}

#[derive(Clone, Debug)]
pub struct AccountInspection {
    pub usage: Option<UsageResponse>,
    pub error: Option<String>,
    pub updated: bool,
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

pub fn cmd_next() -> Result<String> {
    match rotate_next_internal()? {
        NextResult::Rotated { message, .. }
        | NextResult::Stayed { message, .. }
        | NextResult::Created {
            output: message, ..
        } => Ok(message),
    }
}

pub fn rotate_next_internal() -> Result<NextResult> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    if pool.accounts.is_empty() {
        return Err(anyhow!("No accounts in pool. Run: codex-rotate add"));
    }

    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;

    let previous_index = pool.active_index;
    let previous = pool.accounts[previous_index].clone();
    let mut cursor_index = previous_index;
    let mut inspected_later_indices = HashSet::new();

    loop {
        let Some(candidate_index) =
            find_next_immediate_round_robin_index(cursor_index, &pool.accounts)
        else {
            break;
        };

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

    if dirty {
        save_pool(&pool)?;
    }
    let output = match cmd_create(create_next_fallback_options()) {
        Ok(output) => output,
        Err(error) if is_auto_create_retry_stopped_for_reusable_account(&error) => {
            return rotate_next_internal();
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
    let mut pool = load_pool()?;
    if pool.accounts.is_empty() {
        return Err(anyhow!("No accounts in pool. Run: codex-rotate add"));
    }
    if pool.accounts.len() == 1 {
        return Err(anyhow!(
            "Only 1 account in pool. Add more with: codex-rotate add"
        ));
    }

    let _ = sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    let previous_index = pool.active_index;
    pool.active_index = (pool.active_index + pool.accounts.len() - 1) % pool.accounts.len();
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
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    if pool.accounts.is_empty() {
        output.push_line(format!(
            "{YELLOW}WARN{RESET} No accounts in pool. Add one with: codex-rotate add"
        ))?;
        return Ok(());
    }
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;

    let mut usable_count = 0;
    let mut exhausted_count = 0;
    let mut unavailable_count = 0;
    output.push_line(String::new())?;
    output.push_line(format!(
        "{BOLD}Codex OAuth Account Pool{RESET} ({} account(s))",
        pool.accounts.len()
    ))?;
    output.push_line(String::new())?;

    for index in 0..pool.accounts.len() {
        let is_active = index == pool.active_index;
        let label = if is_active {
            format!("{BOLD}{}{RESET}", pool.accounts[index].label)
        } else {
            pool.accounts[index].label.clone()
        };
        let email = pool.accounts[index].email.clone();
        let plan_type = pool.accounts[index].plan_type.clone();
        let account_id = pool.accounts[index].account_id.clone();
        let alias = pool.accounts[index].alias.clone();
        output.push_line(format!(
            "  {} {}  {CYAN}{}{RESET}  {DIM}{}{RESET}  {DIM}{}{RESET}",
            if is_active {
                format!("{GREEN}>{RESET}")
            } else {
                " ".to_string()
            },
            label,
            email,
            plan_type,
            format_short_account_id(&account_id)
        ))?;
        if let Some(alias) = alias {
            output.push_line(format!("    {DIM}alias{RESET}  {}", alias))?;
        }
        let inspection =
            inspect_account(&mut pool.accounts[index], &paths.codex_auth_file, is_active)?;
        dirty |= inspection.updated;
        if let Some(usage) = inspection.usage.as_ref() {
            if has_usable_quota(usage) {
                usable_count += 1;
            } else {
                exhausted_count += 1;
            }
            output.push_line(format!(
                "    {DIM}quota{RESET}  {}",
                format_compact_quota(usage)
            ))?;
        } else {
            unavailable_count += 1;
            output.push_line(format!(
                "    {DIM}quota{RESET}  unavailable ({})",
                inspection
                    .error
                    .unwrap_or_else(|| "unknown error".to_string())
            ))?;
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
    Ok(())
}

pub fn cmd_status() -> Result<String> {
    let mut emitter = LineEmitter::buffered();
    cmd_status_impl(&mut emitter)?;
    Ok(emitter.finish())
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
    write_rotate_state_json(&state, false)
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
    let current_account_id = extract_account_id_from_auth(&current_auth);
    let current_email = extract_email_from_auth(&current_auth);
    let Some(current_index) =
        find_pool_account_index_by_identity(pool, &current_account_id, &current_email)
    else {
        return Ok(false);
    };

    let mut changed = false;
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
        let next_label = build_account_label(&entry.email, &entry.plan_type);
        let current_alias = normalize_alias(entry.alias.as_deref());
        if entry.label != next_label {
            if current_alias.is_none() && !entry.label.is_empty() {
                entry.alias = Some(entry.label.clone());
            }
            entry.label = next_label.clone();
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
    let next_email = extract_email_from_auth(&auth);
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
    let next_usable = inspection.usage.as_ref().map(has_usable_quota);
    let next_summary = inspection.usage.as_ref().map(format_compact_quota);
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
        || entry.last_quota_checked_at.as_deref() != Some(checked_at);

    entry.last_quota_usable = next_usable;
    entry.last_quota_summary = next_summary;
    entry.last_quota_blocker = next_blocker;
    entry.last_quota_checked_at = Some(checked_at.to_string());
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
) -> Result<(Option<RotationCandidate>, bool)> {
    let mut next_dirty = dirty;
    let probe_order =
        build_reusable_account_probe_order(pool.active_index, pool.accounts.len(), mode);

    for index in probe_order {
        if skip_indices.contains(&index) {
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
        }
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
}
