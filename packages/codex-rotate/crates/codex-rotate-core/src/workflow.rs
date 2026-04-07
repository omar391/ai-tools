use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use rand::rngs::OsRng;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::auth::{
    extract_account_id_from_auth, load_codex_auth, summarize_codex_auth, write_codex_auth,
    CodexAuth,
};
use crate::bridge::run_automation_bridge;
use crate::paths::{legacy_credentials_file, resolve_paths};
use crate::pool::{
    cmd_add, find_next_usable_account, format_account_summary_for_display, inspect_account,
    load_pool, normalize_pool_entries, resolve_account_selector, save_pool,
    sync_pool_active_account_from_codex, AccountEntry, AccountInspection, Pool,
    ReusableAccountProbeMode,
};
use crate::quota::{describe_quota_blocker, format_compact_quota, has_usable_quota};
use crate::state::{load_rotate_state_json, write_rotate_state_json};

const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RESET: &str = "\x1b[0m";
const DEFAULT_CODEX_BIN: &str = "codex";
const DEFAULT_OPENAI_FULL_NAME: &str = "Dev Astronlab";
const DEFAULT_OPENAI_BIRTH_MONTH: u8 = 1;
const DEFAULT_OPENAI_BIRTH_DAY: u8 = 24;
const DEFAULT_OPENAI_BIRTH_YEAR: u16 = 1990;
const DEFAULT_CREATE_BASE_EMAIL: &str = "dev.{n}@astronlab.com";
const ROTATE_STATE_VERSION: u8 = 4;
const EMAIL_FAMILY_PLACEHOLDER: &str = "{n}";
const AUTO_CREATE_RETRY_DELAY: Duration = Duration::from_secs(2);
const AUTO_CREATE_RETRY_STOPPED_FOR_REUSABLE_ACCOUNT: &str =
    "Automatic account creation stopped retrying because a reusable account is now available.";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateCommandSource {
    Manual,
    Next,
}

#[derive(Clone, Debug)]
pub struct CreateCommandOptions {
    pub alias: Option<String>,
    pub profile_name: Option<String>,
    pub base_email: Option<String>,
    pub force: bool,
    pub ignore_current: bool,
    pub restore_previous_auth_after_create: bool,
    pub require_usable_quota: bool,
    pub source: CreateCommandSource,
}

impl Default for CreateCommandOptions {
    fn default() -> Self {
        Self {
            alias: None,
            profile_name: None,
            base_email: None,
            force: false,
            ignore_current: false,
            restore_previous_auth_after_create: false,
            require_usable_quota: false,
            source: CreateCommandSource::Manual,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReloginOptions {
    pub allow_email_change: bool,
    pub logout_first: bool,
    pub manual_login: bool,
}

impl Default for ReloginOptions {
    fn default() -> Self {
        Self {
            allow_email_change: false,
            logout_first: true,
            manual_login: false,
        }
    }
}

#[derive(Clone, Debug)]
struct CreateCommandResult {
    entry: AccountEntry,
    inspection: Option<AccountInspection>,
    profile_name: String,
    base_email: String,
}

#[derive(Debug)]
enum CreateFlowAttemptFailure {
    Fatal(anyhow::Error),
    Retryable(anyhow::Error),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CredentialFamily {
    pub profile_name: String,
    pub base_email: String,
    pub next_suffix: u32,
    pub created_at: String,
    pub updated_at: String,
    pub last_created_email: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodexRotateSecretRef {
    #[serde(rename = "type")]
    pub ref_type: String,
    pub store: String,
    pub object_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredCredential {
    pub email: String,
    pub profile_name: String,
    pub base_email: String,
    pub suffix: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub birth_month: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub birth_day: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub birth_year: Option<u16>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingCredential {
    #[serde(flatten)]
    pub stored: StoredCredential,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CodexRotateSecretLocator {
    LoginLookup {
        store: String,
        username: String,
        uris: Vec<String>,
        field_path: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialStore {
    pub version: u8,
    pub default_create_base_email: String,
    pub families: HashMap<String, CredentialFamily>,
    pub pending: HashMap<String, PendingCredential>,
}

impl Default for CredentialStore {
    fn default() -> Self {
        Self {
            version: ROTATE_STATE_VERSION,
            default_create_base_email: DEFAULT_CREATE_BASE_EMAIL.to_string(),
            families: HashMap::new(),
            pending: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct AdultBirthDate {
    birth_month: u8,
    birth_day: u8,
    birth_year: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkflowFileMetadata {
    workflow_ref: Option<String>,
    preferred_profile_name: Option<String>,
    preferred_email: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManagedProfileEntry {
    name: String,
}

#[derive(Clone, Debug, Deserialize)]
struct ManagedProfilesPayload {
    default: Option<String>,
    profiles: Vec<ManagedProfileEntry>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManagedProfilesInspection {
    managed_profiles: ManagedProfilesPayload,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
struct SystemChromeProfileCandidate {
    directory: String,
    name: String,
    emails: Vec<String>,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
struct SystemChromeProfileMatch {
    directory: String,
    name: String,
    emails: Vec<String>,
    matched_email: String,
    score: i32,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeEnsureSecretPayload<'a> {
    profile_name: &'a str,
    email: &'a str,
    password: &'a str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeFindSecretPayload<'a> {
    profile_name: &'a str,
    email: &'a str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeDeleteSecretPayload<'a> {
    profile_name: &'a str,
    email: &'a str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeLoginOptions<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    codex_bin: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow_file: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow_run_stamp: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prefer_signup_recovery: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    full_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    birth_month: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    birth_day: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    birth_year: Option<u16>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeCompleteLoginPayload<'a> {
    profile_name: &'a str,
    email: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    account_login_locator: Option<&'a CodexRotateSecretLocator>,
    #[serde(skip_serializing_if = "Option::is_none")]
    options: Option<BridgeLoginOptions<'a>>,
}

#[derive(Clone, Debug)]
struct EmailFamily {
    normalized: String,
    local_part: String,
    domain_part: String,
    mode: EmailFamilyMode,
}

#[derive(Clone, Debug)]
enum EmailFamilyMode {
    GmailPlus,
    Template { prefix: String, suffix: String },
}

pub fn cmd_create(options: CreateCommandOptions) -> Result<String> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    let previous_auth = if options.restore_previous_auth_after_create {
        load_codex_auth_if_exists()?
    } else {
        None
    };

    if !options.force && !pool.accounts.is_empty() {
        let previous_index = pool.active_index;
        let previous = pool.accounts[previous_index].clone();
        let mut reasons = Vec::new();
        let skip_indices = HashSet::new();
        let (candidate, candidate_dirty) = find_next_usable_account(
            &mut pool,
            &paths.codex_auth_file,
            if options.ignore_current {
                ReusableAccountProbeMode::OthersOnly
            } else {
                ReusableAccountProbeMode::CurrentFirst
            },
            &mut reasons,
            dirty,
            &skip_indices,
        )?;
        dirty = candidate_dirty;

        if let Some(candidate) = candidate {
            let switched = candidate.index != previous_index;
            if switched {
                pool.active_index = candidate.index;
                write_codex_auth(&paths.codex_auth_file, &candidate.entry.auth)?;
            }
            if dirty || switched {
                save_pool(&pool)?;
            }

            let quota_summary = candidate
                .inspection
                .usage
                .as_ref()
                .map(format_compact_quota)
                .unwrap_or_else(|| "quota unavailable".to_string());

            if switched {
                return Ok(format!(
                    "{GREEN}OK{RESET} Reused {} instead of creating a new account.\nQuota: {}",
                    candidate.entry.label, quota_summary
                ));
            }

            return Ok(format!(
                "{GREEN}OK{RESET} Current account {} still has healthy quota.\nQuota: {}",
                previous.label, quota_summary
            ));
        }
    }

    if dirty {
        save_pool(&pool)?;
    }

    let result = execute_create_flow(&options)?;
    let quota_summary = summarize_quota_for_create(&result);
    if options.restore_previous_auth_after_create {
        restore_active_auth(previous_auth.as_ref())?;
        return Ok(format!(
            "{GREEN}OK{RESET} Created {} via \"{}\" from {}.\nQuota: {}\nCurrent session unchanged.",
            result.entry.label, result.profile_name, result.base_email, quota_summary
        ));
    }
    Ok(format!(
        "{GREEN}OK{RESET} Created {} via \"{}\" from {}.\nQuota: {}",
        result.entry.label, result.profile_name, result.base_email, quota_summary
    ))
}

pub fn migrate_legacy_credential_store_if_needed() -> Result<bool> {
    let legacy_file = legacy_credentials_file()?;
    if !legacy_file.exists() {
        return Ok(false);
    }

    let raw = fs::read_to_string(&legacy_file)
        .with_context(|| format!("Failed to read {}.", legacy_file.display()))?;
    let parsed: Value = serde_json::from_str(&raw)
        .with_context(|| format!("Invalid credential store at {}.", legacy_file.display()))?;
    let store = normalize_credential_store(parsed);
    save_credential_store(&store)?;
    fs::remove_file(&legacy_file)
        .with_context(|| format!("Failed to remove {}.", legacy_file.display()))?;
    Ok(true)
}

pub fn cmd_relogin(selector: &str, options: ReloginOptions) -> Result<String> {
    let selection = {
        let pool = load_pool()?;
        resolve_account_selector(&pool, selector)?
    };
    let existing = selection.entry.clone();
    let expected_email = existing.email.clone();
    let mut store = load_credential_store()?;
    let stored_credential = resolve_relogin_credential(&store, &existing);

    if should_use_stored_credential_relogin(stored_credential.as_ref(), &options) {
        let stored_credential = stored_credential.ok_or_else(|| {
            anyhow!("Stored credential lookup unexpectedly failed for {expected_email}.")
        })?;
        let mut updated_stored = stored_credential.clone();
        updated_stored.updated_at = now_iso();
        let account_login_locator = build_openai_account_login_locator(&updated_stored.email);

        let previous_auth = load_codex_auth_if_exists()?;
        let login_result = (|| -> Result<()> {
            if should_logout_before_stored_relogin(&options) {
                let paths = resolve_paths()?;
                if paths.codex_auth_file.exists() {
                    run_codex_command(["logout"])?;
                }
            }

            run_complete_codex_login(
                &updated_stored.profile_name,
                &updated_stored.email,
                Some(&account_login_locator),
                None,
                None,
                None,
                None,
                None,
            )
        })();
        if let Err(error) = login_result {
            restore_active_auth(previous_auth.as_ref())?;
            return Err(error);
        }

        let auth = load_current_auth()?;
        let logged_in_email = summarize_codex_auth(&auth).email;
        if !options.allow_email_change
            && normalize_email_key(&logged_in_email) != normalize_email_key(&expected_email)
        {
            restore_active_auth(previous_auth.as_ref())?;
            return Err(anyhow!(
                "Expected {}, but Codex logged into {}.",
                expected_email,
                logged_in_email
            ));
        }

        let _ = cmd_add(existing.alias.as_deref())?;
        if let Some(inspected) =
            inspect_pool_entry_by_account_id(&extract_account_id_from_auth(&auth))?
        {
            let mut dirty = false;
            if store
                .pending
                .remove(&normalize_email_key(&updated_stored.email))
                .is_some()
            {
                dirty = true;
            }
            dirty |= upsert_family_for_account(
                &mut store,
                &StoredCredential {
                    selector: Some(inspected.entry.label.clone()),
                    alias: inspected
                        .entry
                        .alias
                        .clone()
                        .or_else(|| existing.alias.clone()),
                    updated_at: now_iso(),
                    ..updated_stored
                },
            );
            if dirty {
                save_credential_store(&store)?;
            }
        }

        return Ok(format!(
            "{GREEN}OK{RESET} Re-logged {} with stored managed-browser credentials.",
            format_account_summary_for_display(&existing)
        ));
    }

    if stored_credential.is_none() && !options.manual_login {
        eprintln!(
            "{YELLOW}WARN{RESET} No stored credentials were found for {}. Falling back to manual login.",
            expected_email
        );
    }

    if options.logout_first {
        let paths = resolve_paths()?;
        if paths.codex_auth_file.exists() {
            run_codex_command(["logout"])?;
        }
    }

    run_codex_command(["login"])?;

    let auth = load_current_auth()?;
    let logged_in_email = summarize_codex_auth(&auth).email;
    if normalize_email_key(&logged_in_email) != normalize_email_key(&expected_email)
        && !options.allow_email_change
    {
        return Err(anyhow!(
            "Logged into {}, but \"{}\" expects {}. The pool was not updated. Re-run with --allow-email-change if you want to replace it.",
            logged_in_email,
            format_account_summary_for_display(&existing),
            expected_email
        ));
    }

    cmd_add(existing.alias.as_deref())
}

pub fn should_use_stored_credential_relogin(
    stored_credential: Option<&StoredCredential>,
    options: &ReloginOptions,
) -> bool {
    stored_credential.is_some() && !options.manual_login
}

fn should_logout_before_stored_relogin(options: &ReloginOptions) -> bool {
    options.logout_first
}

pub fn create_next_fallback_options() -> CreateCommandOptions {
    CreateCommandOptions {
        require_usable_quota: true,
        source: CreateCommandSource::Next,
        ..CreateCommandOptions::default()
    }
}

pub fn reconcile_added_account_credential_state(entry: &AccountEntry) -> Result<bool> {
    let raw_state = load_rotate_state_json()?;
    let raw_pending = normalize_pending_credential_map(raw_state.get("pending"));
    let mut store = normalize_credential_store(raw_state);
    let mut dirty = false;
    let updated_at = now_iso();
    let normalized_email = normalize_email_key(&entry.email);

    if let Some(pending) = raw_pending.get(&normalized_email).cloned() {
        dirty = true;
        store.pending.remove(&normalized_email);
        dirty |= upsert_family_for_account(
            &mut store,
            &StoredCredential {
                email: entry.email.clone(),
                profile_name: pending.stored.profile_name,
                base_email: pending.stored.base_email,
                suffix: pending.stored.suffix,
                selector: Some(entry.label.clone()),
                alias: entry.alias.clone(),
                birth_month: pending.stored.birth_month,
                birth_day: pending.stored.birth_day,
                birth_year: pending.stored.birth_year,
                created_at: pending.stored.created_at,
                updated_at: updated_at.clone(),
            },
        );
    } else if let Some(family_match) = select_family_for_account_email(&store, &entry.email) {
        dirty |= upsert_family_for_account(
            &mut store,
            &StoredCredential {
                email: entry.email.clone(),
                profile_name: family_match.family.profile_name,
                base_email: family_match.family.base_email,
                suffix: family_match.suffix,
                selector: Some(entry.label.clone()),
                alias: entry.alias.clone(),
                birth_month: None,
                birth_day: None,
                birth_year: None,
                created_at: family_match.family.created_at,
                updated_at,
            },
        );
    }

    if dirty {
        save_credential_store(&store)?;
    }

    Ok(dirty)
}

fn execute_create_flow(options: &CreateCommandOptions) -> Result<CreateCommandResult> {
    let mut attempt = 1usize;
    loop {
        match execute_create_flow_attempt(options) {
            Ok(result) => return Ok(result),
            Err(CreateFlowAttemptFailure::Retryable(error))
                if should_retry_create_until_usable(options) =>
            {
                if reusable_account_exists_for_auto_create_retry(options)? {
                    return Err(anyhow!(AUTO_CREATE_RETRY_STOPPED_FOR_REUSABLE_ACCOUNT));
                }
                eprintln!(
                    "{YELLOW}WARN{RESET} Automatic account creation attempt {attempt} failed: {error}. Retrying with a fresh account in {}s.",
                    AUTO_CREATE_RETRY_DELAY.as_secs()
                );
                attempt = attempt.saturating_add(1);
                thread::sleep(AUTO_CREATE_RETRY_DELAY);
            }
            Err(CreateFlowAttemptFailure::Retryable(error))
            | Err(CreateFlowAttemptFailure::Fatal(error)) => return Err(error),
        }
    }
}

fn fatal<T>(result: Result<T>) -> std::result::Result<T, CreateFlowAttemptFailure> {
    result.map_err(CreateFlowAttemptFailure::Fatal)
}

fn should_retry_create_until_usable(options: &CreateCommandOptions) -> bool {
    options.require_usable_quota && matches!(options.source, CreateCommandSource::Next)
}

pub fn is_auto_create_retry_stopped_for_reusable_account(error: &anyhow::Error) -> bool {
    error
        .to_string()
        .contains(AUTO_CREATE_RETRY_STOPPED_FOR_REUSABLE_ACCOUNT)
}

fn reusable_account_exists_for_auto_create_retry(options: &CreateCommandOptions) -> Result<bool> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;

    if pool.accounts.is_empty() {
        if dirty {
            save_pool(&pool)?;
        }
        return Ok(false);
    }

    let mut reasons = Vec::new();
    let skip_indices = HashSet::new();
    let mode = if options.ignore_current {
        ReusableAccountProbeMode::OthersOnly
    } else {
        ReusableAccountProbeMode::CurrentFirst
    };
    let (candidate, candidate_dirty) = find_next_usable_account(
        &mut pool,
        &paths.codex_auth_file,
        mode,
        &mut reasons,
        dirty,
        &skip_indices,
    )?;
    if candidate_dirty {
        save_pool(&pool)?;
    }
    Ok(candidate.is_some())
}

fn prepare_next_auto_create_attempt(
    store: &mut CredentialStore,
    family_key: &str,
    profile_name: &str,
    base_email: &str,
    suffix: u32,
    created_email: &str,
    started_at: &str,
) -> Result<()> {
    let normalized_email = normalize_email_key(created_email);
    if !store.pending.contains_key(&normalized_email) {
        return Ok(());
    }

    store.pending.remove(&normalized_email);
    let updated_at = now_iso();
    let next_suffix = suffix.saturating_add(1);
    if let Some(family) = store.families.get_mut(family_key) {
        family.next_suffix = family.next_suffix.max(next_suffix);
        family.updated_at = updated_at;
    } else {
        store.families.insert(
            family_key.to_string(),
            CredentialFamily {
                profile_name: profile_name.to_string(),
                base_email: base_email.to_string(),
                next_suffix,
                created_at: started_at.to_string(),
                updated_at,
                last_created_email: None,
            },
        );
    }

    save_credential_store(store)
}

fn execute_create_flow_attempt(
    options: &CreateCommandOptions,
) -> std::result::Result<CreateCommandResult, CreateFlowAttemptFailure> {
    let paths = fatal(resolve_paths())?;
    let previous_auth = fatal(load_codex_auth_if_exists())?;
    let mut store = fatal(load_credential_store())?;
    let workflow_file = resolve_account_flow_file_for_create(&paths, options);
    let workflow_file_display = workflow_file.display().to_string();
    let workflow_metadata = fatal(read_workflow_file_metadata(&workflow_file))?;
    let profile_name = fatal(resolve_managed_profile_name(
        options.profile_name.as_deref(),
        workflow_metadata.preferred_profile_name.as_deref(),
        Some(workflow_file_display.as_str()),
    ))?;
    let base_email = fatal(resolve_create_base_email_for_profile(
        &store,
        &profile_name,
        options.base_email.as_deref(),
        options.alias.as_deref(),
    ))?;

    let pool = fatal(load_pool())?;
    let family_key = fatal(make_credential_family_key(&profile_name, &base_email))?;
    let family = store.families.get(&family_key).cloned();
    let started_at = now_iso();
    let known_emails = collect_known_account_emails(&pool, &store);
    let existing_pending = select_pending_credential_for_family(
        &store,
        &profile_name,
        &base_email,
        options.alias.as_deref(),
    );
    let reusing_pending = existing_pending.is_some();
    let suffix = match existing_pending.as_ref() {
        Some(entry) => entry.stored.suffix,
        None => fatal(compute_next_account_family_suffix(
            &base_email,
            family
                .as_ref()
                .map(|entry| entry.next_suffix)
                .unwrap_or_else(|| derive_family_frontier_suffix(&base_email, &known_emails)),
            known_emails,
        ))?,
    };
    let created_email = existing_pending
        .as_ref()
        .map(|entry| entry.stored.email.clone())
        .unwrap_or_else(|| build_account_family_email(&base_email, suffix).unwrap_or_default());
    let existing_pending = existing_pending.unwrap_or_else(|| PendingCredential {
        stored: StoredCredential {
            email: created_email.clone(),
            profile_name: profile_name.clone(),
            base_email: base_email.clone(),
            suffix,
            selector: None,
            alias: normalize_alias(options.alias.as_deref()),
            birth_month: None,
            birth_day: None,
            birth_year: None,
            created_at: started_at.clone(),
            updated_at: started_at.clone(),
        },
        started_at: Some(started_at.clone()),
    });
    let birth_date = resolve_credential_birth_date(Some(&existing_pending.stored), Utc::now());
    if previous_auth
        .as_ref()
        .map(|auth| auth_matches_target_email(auth, &created_email))
        .unwrap_or(false)
    {
        let auth = previous_auth
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("Current Codex auth disappeared before create could finish."))
            .map_err(CreateFlowAttemptFailure::Fatal)?;
        let result = finalize_created_account(
            &mut store,
            family.as_ref(),
            &family_key,
            &profile_name,
            &base_email,
            suffix,
            &PendingCredential {
                stored: StoredCredential {
                    email: created_email.clone(),
                    profile_name: profile_name.clone(),
                    base_email: base_email.clone(),
                    suffix,
                    selector: existing_pending.stored.selector.clone(),
                    alias: existing_pending
                        .stored
                        .alias
                        .clone()
                        .or_else(|| normalize_alias(options.alias.as_deref())),
                    birth_month: Some(birth_date.birth_month),
                    birth_day: Some(birth_date.birth_day),
                    birth_year: Some(birth_date.birth_year),
                    created_at: existing_pending.stored.created_at.clone(),
                    updated_at: started_at.clone(),
                },
                started_at: existing_pending
                    .started_at
                    .clone()
                    .or_else(|| Some(started_at.clone())),
            },
            options,
            &auth,
            started_at.as_str(),
            previous_auth.as_ref(),
        );
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                if should_retry_create_until_usable(options) {
                    fatal(restore_active_auth(previous_auth.as_ref()))?;
                    fatal(prepare_next_auto_create_attempt(
                        &mut store,
                        &family_key,
                        &profile_name,
                        &base_email,
                        suffix,
                        &created_email,
                        started_at.as_str(),
                    ))?;
                    return Err(CreateFlowAttemptFailure::Retryable(error));
                }
                return Err(CreateFlowAttemptFailure::Fatal(error));
            }
        };
        if options.restore_previous_auth_after_create {
            fatal(restore_active_auth(previous_auth.as_ref()))?;
        }
        return Ok(result);
    }
    let account_login_locator = build_openai_account_login_locator(&created_email);
    if !reusing_pending {
        let existing_secret_ref: Option<CodexRotateSecretRef> = run_automation_bridge(
            "find-account-secret-ref",
            BridgeFindSecretPayload {
                profile_name: &profile_name,
                email: &created_email,
            },
        )
        .map_err(CreateFlowAttemptFailure::Fatal)?;
        if existing_secret_ref.is_none() {
            let generated_password = generate_password(18);
            let _: CodexRotateSecretRef = run_automation_bridge(
                "ensure-account-secret-ref",
                BridgeEnsureSecretPayload {
                    profile_name: &profile_name,
                    email: &created_email,
                    password: &generated_password,
                },
            )
            .map_err(CreateFlowAttemptFailure::Fatal)?;
        }
    }
    let pending = PendingCredential {
        stored: StoredCredential {
            email: created_email.clone(),
            profile_name: profile_name.clone(),
            base_email: base_email.clone(),
            suffix,
            selector: existing_pending.stored.selector.clone(),
            alias: existing_pending
                .stored
                .alias
                .clone()
                .or_else(|| normalize_alias(options.alias.as_deref())),
            birth_month: Some(birth_date.birth_month),
            birth_day: Some(birth_date.birth_day),
            birth_year: Some(birth_date.birth_year),
            created_at: existing_pending.stored.created_at.clone(),
            updated_at: started_at.clone(),
        },
        started_at: existing_pending
            .started_at
            .clone()
            .or_else(|| Some(started_at.clone())),
    };
    store
        .pending
        .insert(normalize_email_key(&created_email), pending.clone());
    fatal(save_credential_store(&store))?;

    let login_result = run_complete_codex_login(
        &profile_name,
        &created_email,
        Some(&account_login_locator),
        Some(workflow_file.as_path()),
        Some(codex_bin().as_str()),
        Some(started_at.as_str()),
        Some(true),
        Some(&birth_date),
    );
    if let Err(error) = login_result {
        fatal(restore_active_auth(previous_auth.as_ref()))?;
        if should_retry_create_until_usable(options) {
            fatal(prepare_next_auto_create_attempt(
                &mut store,
                &family_key,
                &profile_name,
                &base_email,
                suffix,
                &created_email,
                started_at.as_str(),
            ))?;
            return Err(CreateFlowAttemptFailure::Retryable(error));
        }
        fatal(save_credential_store(&store))?;
        return Err(CreateFlowAttemptFailure::Fatal(error));
    }

    let auth = fatal(load_current_auth())?;
    let logged_in_email = summarize_codex_auth(&auth).email;
    if normalize_email_key(&logged_in_email) != normalize_email_key(&created_email) {
        let error = anyhow!(
            "Expected {}, but Codex logged into {}.",
            created_email,
            logged_in_email
        );
        fatal(restore_active_auth(previous_auth.as_ref()))?;
        if should_retry_create_until_usable(options) {
            fatal(prepare_next_auto_create_attempt(
                &mut store,
                &family_key,
                &profile_name,
                &base_email,
                suffix,
                &created_email,
                started_at.as_str(),
            ))?;
            return Err(CreateFlowAttemptFailure::Retryable(error));
        }
        fatal(save_credential_store(&store))?;
        return Err(CreateFlowAttemptFailure::Fatal(error));
    }

    let result = finalize_created_account(
        &mut store,
        family.as_ref(),
        &family_key,
        &profile_name,
        &base_email,
        suffix,
        &pending,
        options,
        &auth,
        started_at.as_str(),
        previous_auth.as_ref(),
    );
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            if should_retry_create_until_usable(options) {
                fatal(restore_active_auth(previous_auth.as_ref()))?;
                fatal(prepare_next_auto_create_attempt(
                    &mut store,
                    &family_key,
                    &profile_name,
                    &base_email,
                    suffix,
                    &created_email,
                    started_at.as_str(),
                ))?;
                return Err(CreateFlowAttemptFailure::Retryable(error));
            }
            return Err(CreateFlowAttemptFailure::Fatal(error));
        }
    };

    if options.restore_previous_auth_after_create {
        fatal(restore_active_auth(previous_auth.as_ref()))?;
    }

    Ok(result)
}

fn auth_matches_target_email(auth: &CodexAuth, target_email: &str) -> bool {
    normalize_email_key(&summarize_codex_auth(auth).email) == normalize_email_key(target_email)
}

fn finalize_created_account(
    store: &mut CredentialStore,
    family: Option<&CredentialFamily>,
    family_key: &str,
    profile_name: &str,
    base_email: &str,
    suffix: u32,
    pending: &PendingCredential,
    options: &CreateCommandOptions,
    auth: &CodexAuth,
    started_at: &str,
    previous_auth: Option<&CodexAuth>,
) -> Result<CreateCommandResult> {
    let created_email = pending.stored.email.clone();
    let _ = cmd_add(options.alias.as_deref())?;
    let inspected = inspect_pool_entry_by_account_id(&extract_account_id_from_auth(auth))?
        .ok_or_else(|| {
            anyhow!(
                "Created {}, but could not find the new account in the pool after login.",
                created_email
            )
        })?;

    let updated_at = now_iso();
    store.pending.remove(&normalize_email_key(&created_email));
    upsert_family_for_account(
        store,
        &StoredCredential {
            email: created_email.clone(),
            profile_name: profile_name.to_string(),
            base_email: base_email.to_string(),
            suffix,
            selector: Some(inspected.entry.label.clone()),
            alias: inspected
                .entry
                .alias
                .clone()
                .or_else(|| normalize_alias(options.alias.as_deref())),
            birth_month: pending.stored.birth_month,
            birth_day: pending.stored.birth_day,
            birth_year: pending.stored.birth_year,
            created_at: pending.stored.created_at.clone(),
            updated_at: updated_at.clone(),
        },
    );
    store.families.insert(
        family_key.to_string(),
        CredentialFamily {
            profile_name: profile_name.to_string(),
            base_email: base_email.to_string(),
            next_suffix: family
                .map(|entry| entry.next_suffix.max(suffix + 1))
                .unwrap_or(suffix + 1),
            created_at: family
                .map(|entry| entry.created_at.clone())
                .unwrap_or_else(|| started_at.to_string()),
            updated_at,
            last_created_email: Some(created_email.clone()),
        },
    );
    save_credential_store(store)?;

    if options.require_usable_quota {
        match inspected.inspection.usage.as_ref() {
            Some(usage) if has_usable_quota(usage) => {}
            Some(usage) => {
                restore_active_auth(previous_auth)?;
                return Err(anyhow!(
                    "Created {}, but it does not have usable quota ({}).",
                    inspected.entry.label,
                    describe_quota_blocker(usage)
                ));
            }
            None => {
                restore_active_auth(previous_auth)?;
                return Err(anyhow!(
                    "Created {}, but quota inspection was unavailable ({}).",
                    inspected.entry.label,
                    inspected
                        .inspection
                        .error
                        .clone()
                        .unwrap_or_else(|| "unknown error".to_string())
                ));
            }
        }
    }

    Ok(CreateCommandResult {
        entry: inspected.entry,
        inspection: Some(inspected.inspection),
        profile_name: profile_name.to_string(),
        base_email: base_email.to_string(),
    })
}

fn summarize_quota_for_create(result: &CreateCommandResult) -> String {
    match result.inspection.as_ref() {
        Some(inspection) => match inspection.usage.as_ref() {
            Some(usage) => format_compact_quota(usage),
            None => format!(
                "quota unavailable ({})",
                inspection
                    .error
                    .clone()
                    .unwrap_or_else(|| "unknown error".to_string())
            ),
        },
        None => "quota unavailable".to_string(),
    }
}

fn inspect_pool_entry_by_account_id(account_id: &str) -> Result<Option<InspectedPoolEntry>> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let index = pool.accounts.iter().position(|entry| {
        entry.account_id == account_id || entry.auth.tokens.account_id == account_id
    });
    let Some(index) = index else {
        return Ok(None);
    };
    let inspection = inspect_account(
        &mut pool.accounts[index],
        &paths.codex_auth_file,
        index == pool.active_index,
    )?;
    if inspection.updated {
        save_pool(&pool)?;
    }
    Ok(Some(InspectedPoolEntry {
        entry: pool.accounts[index].clone(),
        inspection,
    }))
}

struct InspectedPoolEntry {
    entry: AccountEntry,
    inspection: AccountInspection,
}

fn load_current_auth() -> Result<CodexAuth> {
    let paths = resolve_paths()?;
    load_codex_auth(&paths.codex_auth_file)
}

fn load_codex_auth_if_exists() -> Result<Option<CodexAuth>> {
    let paths = resolve_paths()?;
    if !paths.codex_auth_file.exists() {
        return Ok(None);
    }
    Ok(Some(load_codex_auth(&paths.codex_auth_file)?))
}

fn restore_active_auth(previous_auth: Option<&CodexAuth>) -> Result<()> {
    let paths = resolve_paths()?;
    match previous_auth {
        Some(previous_auth) => {
            write_codex_auth(&paths.codex_auth_file, previous_auth)?;
            let mut pool = load_pool()?;
            if let Some(index) = pool.accounts.iter().position(|entry| {
                entry.account_id == extract_account_id_from_auth(previous_auth)
                    || entry.auth.tokens.account_id == previous_auth.tokens.account_id
            }) {
                pool.active_index = index;
                save_pool(&pool)?;
            }
        }
        None => {
            if paths.codex_auth_file.exists() {
                fs::remove_file(&paths.codex_auth_file).with_context(|| {
                    format!("Failed to remove {}.", paths.codex_auth_file.display())
                })?;
            }
        }
    }
    Ok(())
}

fn run_complete_codex_login(
    profile_name: &str,
    email: &str,
    account_login_locator: Option<&CodexRotateSecretLocator>,
    workflow_file: Option<&Path>,
    codex_bin: Option<&str>,
    workflow_run_stamp: Option<&str>,
    prefer_signup_recovery: Option<bool>,
    birth_date: Option<&AdultBirthDate>,
) -> Result<()> {
    let fallback_birth_date;
    let birth_date = match birth_date {
        Some(value) => value,
        None => {
            fallback_birth_date = default_openai_birth_date();
            &fallback_birth_date
        }
    };
    let options = BridgeLoginOptions {
        codex_bin,
        workflow_file: workflow_file.and_then(|path| path.to_str()),
        workflow_run_stamp,
        prefer_signup_recovery,
        full_name: Some(DEFAULT_OPENAI_FULL_NAME),
        birth_month: Some(birth_date.birth_month),
        birth_day: Some(birth_date.birth_day),
        birth_year: Some(birth_date.birth_year),
    };
    let _: Value = run_automation_bridge(
        "complete-codex-login",
        BridgeCompleteLoginPayload {
            profile_name,
            email,
            account_login_locator,
            options: Some(options),
        },
    )?;
    Ok(())
}

fn build_openai_account_login_locator(email: &str) -> CodexRotateSecretLocator {
    CodexRotateSecretLocator::LoginLookup {
        store: "bitwarden-cli".to_string(),
        username: email.trim().to_lowercase(),
        uris: vec![
            "https://auth.openai.com".to_string(),
            "https://chatgpt.com".to_string(),
        ],
        field_path: "/password".to_string(),
    }
}

fn resolve_account_flow_file_for_create(
    paths: &crate::paths::CorePaths,
    _options: &CreateCommandOptions,
) -> std::path::PathBuf {
    paths.account_flow_file.clone()
}

fn run_codex_command<const N: usize>(args: [&str; N]) -> Result<()> {
    let args_vec = args
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    let result = Command::new(codex_bin())
        .args(args_vec.iter().map(String::as_str))
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .with_context(|| format!("Failed to run {} {}.", codex_bin(), args.join(" ")))?;
    if result.success() {
        return Ok(());
    }
    Err(anyhow!(
        "\"{} {}\" exited with status {}.",
        codex_bin(),
        args.join(" "),
        result.code().unwrap_or_default()
    ))
}

fn codex_bin() -> String {
    std::env::var("CODEX_ROTATE_CODEX_BIN").unwrap_or_else(|_| DEFAULT_CODEX_BIN.to_string())
}

fn load_credential_store() -> Result<CredentialStore> {
    let _ = migrate_legacy_credential_store_if_needed()?;
    let state = load_rotate_state_json()?;
    let raw_pending = normalize_pending_credential_map(state.get("pending"));
    let store = normalize_credential_store(state);
    let dropped_non_dev_pending = raw_pending
        .into_values()
        .filter(|record| should_drop_non_dev_pending_credential(&record.stored.base_email))
        .collect::<Vec<_>>();

    if !dropped_non_dev_pending.is_empty() {
        save_credential_store(&store)?;
        cleanup_dropped_non_dev_pending_secrets(&dropped_non_dev_pending);
    }

    Ok(store)
}

fn save_credential_store(store: &CredentialStore) -> Result<()> {
    let mut state = load_rotate_state_json()?;
    if !state.is_object() {
        state = Value::Object(Map::new());
    }
    let object = state
        .as_object_mut()
        .expect("rotate state must be a JSON object");
    let credential_state = serialize_credential_store(store);
    if let Some(version) = credential_state.get("version").cloned() {
        object.insert("version".to_string(), version);
    }
    if let Some(default_create_base_email) =
        credential_state.get("default_create_base_email").cloned()
    {
        object.insert(
            "default_create_base_email".to_string(),
            default_create_base_email,
        );
    }
    if store.families.is_empty() {
        object.remove("families");
    } else if let Some(families) = credential_state.get("families").cloned() {
        object.insert("families".to_string(), families);
    }
    if store.pending.is_empty() {
        object.remove("pending");
    } else if let Some(pending) = credential_state.get("pending").cloned() {
        object.insert("pending".to_string(), pending);
    }
    write_rotate_state_json(&state)
}

fn cleanup_dropped_non_dev_pending_secrets(records: &[PendingCredential]) {
    for record in records {
        let result = run_automation_bridge::<_, bool>(
            "delete-account-secret-ref",
            BridgeDeleteSecretPayload {
                profile_name: &record.stored.profile_name,
                email: &record.stored.email,
            },
        );
        if let Err(error) = result {
            eprintln!(
                "{YELLOW}WARN{RESET} Failed to remove stale Bitwarden secret for {}: {}",
                record.stored.email, error
            );
        }
    }
}

fn normalize_credential_store(raw: Value) -> CredentialStore {
    let inventory_emails = collect_inventory_emails_from_state(&raw);
    let raw_version = raw
        .get("version")
        .and_then(Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
        .unwrap_or_default();
    let default_create_base_email = raw
        .get("default_create_base_email")
        .and_then(Value::as_str)
        .and_then(|value| normalize_base_email_family(value).ok())
        .unwrap_or_else(|| DEFAULT_CREATE_BASE_EMAIL.to_string());
    let migrate_legacy_non_default_families =
        raw_version < ROTATE_STATE_VERSION || raw.get("default_create_base_email").is_none();
    let mut families = raw
        .get("families")
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(key, value)| {
                    serde_json::from_value::<CredentialFamily>(value.clone())
                        .ok()
                        .map(|record| (key.clone(), record))
                })
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();
    let legacy_accounts = normalize_stored_credential_map(raw.get("accounts"));
    for account in legacy_accounts.values() {
        merge_legacy_account_into_families(&mut families, account);
    }
    if migrate_legacy_non_default_families {
        families.retain(|_, family| {
            !should_drop_legacy_non_default_family(&family.base_email, &default_create_base_email)
        });
    }
    let mut pending = normalize_pending_credential_map(raw.get("pending"))
        .into_iter()
        .filter(|(email, record)| {
            !inventory_emails.contains(email)
                && !pending_is_superseded_by_inventory(record, &inventory_emails)
                && !should_drop_non_dev_pending_credential(&record.stored.base_email)
        })
        .collect::<HashMap<_, _>>();
    if migrate_legacy_non_default_families {
        pending.retain(|_, record| {
            !should_drop_legacy_non_default_family(
                &record.stored.base_email,
                &default_create_base_email,
            )
        });
    }

    CredentialStore {
        version: ROTATE_STATE_VERSION,
        default_create_base_email,
        families,
        pending,
    }
}

fn should_drop_legacy_non_default_family(base_email: &str, default_base_email: &str) -> bool {
    let Ok(normalized_base_email) = normalize_base_email_family(base_email) else {
        return false;
    };
    if normalized_base_email == default_base_email {
        return false;
    }
    let Ok(parsed) = parse_email_family(&normalized_base_email) else {
        return false;
    };
    if parsed.domain_part != "astronlab.com" {
        return false;
    }
    match parsed.mode {
        EmailFamilyMode::Template { prefix, .. } => {
            prefix.starts_with("bench") || prefix.contains("devicefix")
        }
        EmailFamilyMode::GmailPlus => false,
    }
}

fn should_drop_non_dev_pending_credential(base_email: &str) -> bool {
    normalize_base_email_family(base_email)
        .map(|value| value != DEFAULT_CREATE_BASE_EMAIL)
        .unwrap_or(true)
}

fn collect_inventory_emails_from_state(raw: &Value) -> HashSet<String> {
    raw.get("accounts")
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| {
                    entry
                        .get("email")
                        .and_then(Value::as_str)
                        .map(normalize_email_key)
                })
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default()
}

fn pending_is_superseded_by_inventory(
    pending: &PendingCredential,
    inventory_emails: &HashSet<String>,
) -> bool {
    inventory_emails
        .iter()
        .filter_map(|email| extract_account_family_suffix(email, &pending.stored.base_email).ok())
        .flatten()
        .max()
        .map(|suffix| suffix > pending.stored.suffix)
        .unwrap_or(false)
}

fn normalize_stored_credential_map(raw: Option<&Value>) -> HashMap<String, StoredCredential> {
    raw.and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(email, value)| {
                    normalize_stored_credential(value)
                        .map(|record| (normalize_email_key(email), record))
                })
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default()
}

fn normalize_pending_credential_map(raw: Option<&Value>) -> HashMap<String, PendingCredential> {
    raw.and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(email, value)| {
                    normalize_pending_credential(value)
                        .map(|record| (normalize_email_key(email), record))
                })
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default()
}

fn normalize_stored_credential(raw: &Value) -> Option<StoredCredential> {
    serde_json::from_value::<StoredCredential>(raw.clone()).ok()
}

fn normalize_pending_credential(raw: &Value) -> Option<PendingCredential> {
    let object = raw.as_object()?;
    Some(PendingCredential {
        stored: normalize_stored_credential(raw)?,
        started_at: object
            .get("started_at")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

fn serialize_credential_store(store: &CredentialStore) -> Value {
    let pending = store
        .pending
        .iter()
        .map(|(email, record)| (email.clone(), serialize_pending_credential(record)))
        .collect::<Map<String, Value>>();
    json!({
        "version": ROTATE_STATE_VERSION,
        "default_create_base_email": store.default_create_base_email,
        "families": store.families,
        "pending": pending,
    })
}

fn serialize_pending_credential(record: &PendingCredential) -> Value {
    let mut value = serialize_stored_credential(&record.stored)
        .as_object()
        .cloned()
        .unwrap_or_default();
    if let Some(started_at) = record.started_at.as_ref() {
        value.insert("started_at".to_string(), Value::String(started_at.clone()));
    }
    Value::Object(value)
}

fn serialize_stored_credential(record: &StoredCredential) -> Value {
    let mut object = Map::new();
    object.insert("email".to_string(), Value::String(record.email.clone()));
    object.insert(
        "profile_name".to_string(),
        Value::String(record.profile_name.clone()),
    );
    object.insert(
        "base_email".to_string(),
        Value::String(record.base_email.clone()),
    );
    object.insert("suffix".to_string(), Value::Number(record.suffix.into()));
    object.insert(
        "selector".to_string(),
        record
            .selector
            .as_ref()
            .map(|value| Value::String(value.clone()))
            .unwrap_or(Value::Null),
    );
    object.insert(
        "alias".to_string(),
        record
            .alias
            .as_ref()
            .map(|value| Value::String(value.clone()))
            .unwrap_or(Value::Null),
    );
    object.insert(
        "created_at".to_string(),
        Value::String(record.created_at.clone()),
    );
    object.insert(
        "updated_at".to_string(),
        Value::String(record.updated_at.clone()),
    );
    if let Some(value) = record.birth_month {
        object.insert(
            "birth_month".to_string(),
            Value::Number(u64::from(value).into()),
        );
    }
    if let Some(value) = record.birth_day {
        object.insert(
            "birth_day".to_string(),
            Value::Number(u64::from(value).into()),
        );
    }
    if let Some(value) = record.birth_year {
        object.insert(
            "birth_year".to_string(),
            Value::Number(u64::from(value).into()),
        );
    }
    Value::Object(object)
}

fn read_workflow_file_metadata(file_path: &std::path::Path) -> Result<WorkflowFileMetadata> {
    if !file_path.exists() {
        return Err(anyhow!(
            "Workflow file was not found at {}.",
            file_path.display()
        ));
    }

    let raw = fs::read_to_string(file_path)
        .with_context(|| format!("Failed to read workflow file {}.", file_path.display()))?;
    Ok(WorkflowFileMetadata {
        workflow_ref: derive_workflow_ref_from_file_path(file_path),
        ..parse_workflow_file_metadata(&raw)
    })
}

fn parse_workflow_file_metadata(raw: &str) -> WorkflowFileMetadata {
    let mut document_indent = None;
    let mut metadata_indent = None;
    let mut metadata_value_indent = None;
    let mut preferred_profile_name = None;
    let mut preferred_email = None;

    for line in raw.lines() {
        let trimmed_start = line.trim_start();
        if trimmed_start.is_empty() || trimmed_start.starts_with('#') {
            continue;
        }

        let indent = line.len().saturating_sub(trimmed_start.len());

        if let Some(current_metadata_indent) = metadata_indent {
            if indent <= current_metadata_indent {
                metadata_indent = None;
                metadata_value_indent = None;
            }
        }

        if let Some(current_metadata_indent) = metadata_indent {
            if indent > current_metadata_indent {
                let expected_indent = metadata_value_indent.get_or_insert(indent);
                if indent != *expected_indent || trimmed_start.starts_with('-') {
                    continue;
                }
                let Some((key, raw_value)) = trimmed_start.split_once(':') else {
                    continue;
                };
                let normalized = normalize_workflow_scalar(raw_value);
                match key.trim() {
                    "preferredProfile" => preferred_profile_name = normalized,
                    "preferredEmail" => preferred_email = normalized,
                    _ => {}
                }
                continue;
            }
        }

        if let Some(current_document_indent) = document_indent {
            if indent <= current_document_indent && trimmed_start != "document:" {
                document_indent = None;
            }
        }

        if let Some(current_document_indent) = document_indent {
            if indent > current_document_indent && trimmed_start == "metadata:" {
                metadata_indent = Some(indent);
                metadata_value_indent = None;
                continue;
            }
        }

        if trimmed_start == "document:" {
            document_indent = Some(indent);
            metadata_indent = None;
            metadata_value_indent = None;
        }
    }

    WorkflowFileMetadata {
        workflow_ref: None,
        preferred_profile_name,
        preferred_email,
    }
}

fn normalize_workflow_scalar(raw_value: &str) -> Option<String> {
    let trimmed = raw_value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut comment_index = None;
    let bytes = trimmed.as_bytes();
    for index in 0..bytes.len().saturating_sub(1) {
        if bytes[index].is_ascii_whitespace() && bytes[index + 1] == b'#' {
            comment_index = Some(index);
            break;
        }
    }

    let without_comment = comment_index
        .map(|index| &trimmed[..index])
        .unwrap_or(trimmed)
        .trim();
    if without_comment.is_empty() {
        return None;
    }

    let mut chars = without_comment.chars();
    let first = chars.next()?;
    let last = without_comment.chars().last()?;
    if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
        let inner = &without_comment[1..without_comment.len().saturating_sub(1)];
        let normalized = inner.trim();
        return (!normalized.is_empty()).then(|| normalized.to_string());
    }

    Some(without_comment.to_string())
}

fn derive_workflow_ref_from_file_path(file_path: &Path) -> Option<String> {
    let canonical_path = file_path.canonicalize().ok()?;
    let paths = resolve_paths().ok()?;
    let workspace_root = paths.repo_root.join(".fast-browser").join("workflows");
    derive_workflow_ref_from_root(&canonical_path, &workspace_root, "workspace").or_else(|| {
        paths.repo_root.parent().and_then(|parent| {
            let global_root = parent
                .join("ai-rules")
                .join("skills")
                .join("fast-browser")
                .join("workflows");
            derive_workflow_ref_from_root(&canonical_path, &global_root, "sys")
        })
    })
}

fn derive_workflow_ref_from_root(
    file_path: &Path,
    root_dir: &Path,
    scope_prefix: &str,
) -> Option<String> {
    let relative_path = file_path.strip_prefix(root_dir).ok()?;
    if relative_path.extension().and_then(|value| value.to_str()) != Some("yaml") {
        return None;
    }

    let segments = relative_path
        .iter()
        .map(|segment| segment.to_str())
        .collect::<Option<Vec<_>>>()?;
    if segments.len() != 3 {
        return None;
    }

    let workflow_name = Path::new(segments[2]).file_stem()?.to_str()?;
    let parts = [
        Some(scope_prefix.to_string()),
        slugify_workflow_path_segment(segments[0]),
        slugify_workflow_path_segment(segments[1]),
        slugify_workflow_path_segment(workflow_name),
    ]
    .into_iter()
    .collect::<Option<Vec<_>>>()?;
    (parts.len() == 4).then(|| parts.join("."))
}

fn slugify_workflow_path_segment(value: &str) -> Option<String> {
    let mut slug = String::new();
    let mut last_was_separator = false;

    for ch in value.chars().flat_map(|ch| ch.to_lowercase()) {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
            last_was_separator = false;
        } else if !last_was_separator {
            slug.push('-');
            last_was_separator = true;
        }
    }

    let normalized = slug.trim_matches('-').to_string();
    (!normalized.is_empty()).then_some(normalized)
}

fn inspect_managed_profiles() -> Result<ManagedProfilesInspection> {
    run_automation_bridge("inspect-managed-profiles", json!({}))
}

fn resolve_managed_profile_name(
    requested_profile_name: Option<&str>,
    preferred_profile_name: Option<&str>,
    preferred_profile_source: Option<&str>,
) -> Result<String> {
    let inspection = inspect_managed_profiles()?;
    let available_profile_names = inspection
        .managed_profiles
        .profiles
        .iter()
        .map(|profile| profile.name.as_str())
        .collect::<Vec<_>>();
    resolve_managed_profile_name_from_candidates(
        &available_profile_names,
        requested_profile_name,
        preferred_profile_name,
        preferred_profile_source,
        inspection.managed_profiles.default.as_deref(),
    )
}

fn resolve_managed_profile_name_from_candidates(
    available_names: &[&str],
    requested_profile_name: Option<&str>,
    preferred_profile_name: Option<&str>,
    preferred_profile_source: Option<&str>,
    default_profile_name: Option<&str>,
) -> Result<String> {
    if let Some(requested_profile_name) = requested_profile_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if available_names
            .iter()
            .any(|value| *value == requested_profile_name)
        {
            return Ok(requested_profile_name.to_string());
        }
        return Err(anyhow!(
            "Managed fast-browser profile \"{}\" was not found.",
            requested_profile_name
        ));
    }

    if let Some(preferred_profile_name) = preferred_profile_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if available_names
            .iter()
            .any(|value| *value == preferred_profile_name)
        {
            return Ok(preferred_profile_name.to_string());
        }
        let suffix = preferred_profile_source
            .map(|value| format!(" from {value}"))
            .unwrap_or_default();
        return Err(anyhow!(
            "Managed fast-browser profile \"{}\"{} was not found.",
            preferred_profile_name,
            suffix
        ));
    }

    if let Some(default_profile_name) = default_profile_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if available_names
            .iter()
            .any(|value| *value == default_profile_name)
        {
            return Ok(default_profile_name.to_string());
        }
    }

    available_names
        .first()
        .map(|value| (*value).to_string())
        .ok_or_else(|| anyhow!("No managed fast-browser profiles are configured."))
}

fn resolve_create_base_email(
    requested_base_email: Option<&str>,
    discovered_base_email: Option<&str>,
) -> Result<String> {
    if let Some(requested_base_email) = requested_base_email {
        return normalize_base_email_family(requested_base_email);
    }
    if let Some(discovered_base_email) = discovered_base_email {
        return normalize_base_email_family(discovered_base_email);
    }
    normalize_base_email_family(DEFAULT_CREATE_BASE_EMAIL)
}

fn resolve_create_base_email_for_profile(
    store: &CredentialStore,
    profile_name: &str,
    requested_base_email: Option<&str>,
    alias: Option<&str>,
) -> Result<String> {
    let discovered_base_email = if requested_base_email.is_none() {
        select_pending_base_email_hint_for_profile(store, profile_name, alias)
    } else {
        None
    };
    resolve_create_base_email(requested_base_email, discovered_base_email.as_deref())
}

fn make_credential_family_key(profile_name: &str, base_email: &str) -> Result<String> {
    Ok(format!(
        "{}::{}",
        profile_name,
        normalize_base_email_family(base_email)?
    ))
}

fn normalize_base_email_family(email: &str) -> Result<String> {
    Ok(parse_email_family(email)?.normalized)
}

fn parse_email_family(value: &str) -> Result<EmailFamily> {
    let normalized = value.trim().to_lowercase();
    let parts = normalized.split('@').collect::<Vec<_>>();
    if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
        return Err(anyhow!("\"{}\" is not a valid email family.", value));
    }
    let local_part = parts[0].to_string();
    let domain_part = parts[1].to_string();
    let placeholder_count = local_part.matches(EMAIL_FAMILY_PLACEHOLDER).count();

    if placeholder_count == 1 {
        let segments = local_part
            .split(EMAIL_FAMILY_PLACEHOLDER)
            .collect::<Vec<_>>();
        let prefix = segments[0].to_string();
        let suffix = segments[1].to_string();
        if format!("{}{}", prefix, suffix).trim().is_empty() {
            return Err(anyhow!(
                "\"{}\" must keep some stable local-part text around {}.",
                value,
                EMAIL_FAMILY_PLACEHOLDER
            ));
        }
        return Ok(EmailFamily {
            normalized: format!("{prefix}{EMAIL_FAMILY_PLACEHOLDER}{suffix}@{domain_part}"),
            local_part,
            domain_part,
            mode: EmailFamilyMode::Template { prefix, suffix },
        });
    }

    if placeholder_count > 1 {
        return Err(anyhow!(
            "\"{}\" may only contain one {} placeholder.",
            value,
            EMAIL_FAMILY_PLACEHOLDER
        ));
    }

    if domain_part != "gmail.com" {
        return Err(anyhow!(
            "\"{}\" is not a supported base email family. Use gmail.com or a template like dev.{{N}}@example.com.",
            value
        ));
    }

    let base_local = local_part
        .split('+')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("\"{}\" does not contain a valid Gmail local part.", value))?;

    Ok(EmailFamily {
        normalized: format!("{base_local}@gmail.com"),
        local_part: base_local.to_string(),
        domain_part: "gmail.com".to_string(),
        mode: EmailFamilyMode::GmailPlus,
    })
}

fn build_account_family_email(base_email: &str, suffix: u32) -> Result<String> {
    if suffix < 1 {
        return Err(anyhow!("Invalid email family suffix \"{}\".", suffix));
    }
    let parsed = parse_email_family(base_email)?;
    Ok(match parsed.mode {
        EmailFamilyMode::Template {
            prefix,
            suffix: tail,
        } => {
            format!("{prefix}{suffix}{tail}@{}", parsed.domain_part)
        }
        EmailFamilyMode::GmailPlus => {
            format!("{}+{}@{}", parsed.local_part, suffix, parsed.domain_part)
        }
    })
}

fn extract_account_family_suffix(candidate_email: &str, base_email: &str) -> Result<Option<u32>> {
    let parsed = parse_email_family(base_email)?;
    let normalized_candidate = candidate_email.trim().to_lowercase();
    Ok(match parsed.mode {
        EmailFamilyMode::Template { prefix, suffix } => {
            let domain_suffix = format!("@{}", parsed.domain_part);
            if !normalized_candidate.ends_with(&domain_suffix) {
                return Ok(None);
            }
            let without_domain = normalized_candidate
                .strip_suffix(&domain_suffix)
                .unwrap_or_default();
            let middle = without_domain
                .strip_prefix(&prefix)
                .and_then(|value| value.strip_suffix(&suffix));
            middle
                .filter(|value| {
                    !value.is_empty() && value.chars().all(|character| character.is_ascii_digit())
                })
                .and_then(|value| value.parse::<u32>().ok())
        }
        EmailFamilyMode::GmailPlus => {
            let expected_prefix = format!("{}+", parsed.local_part);
            let expected_suffix = format!("@{}", parsed.domain_part);
            let middle = normalized_candidate
                .strip_prefix(&expected_prefix)
                .and_then(|value| value.strip_suffix(&expected_suffix));
            middle
                .filter(|value| {
                    !value.is_empty() && value.chars().all(|character| character.is_ascii_digit())
                })
                .and_then(|value| value.parse::<u32>().ok())
        }
    })
}

fn compute_next_account_family_suffix(
    base_email: &str,
    family_next_suffix: u32,
    known_emails: Vec<String>,
) -> Result<u32> {
    let mut used = HashSet::new();
    for email in known_emails {
        if let Some(suffix) = extract_account_family_suffix(&email, base_email)? {
            used.insert(suffix);
        }
    }
    let mut candidate = family_next_suffix.max(1);
    while used.contains(&candidate) {
        candidate += 1;
    }
    Ok(candidate)
}

fn derive_family_frontier_suffix(base_email: &str, known_emails: &[String]) -> u32 {
    known_emails
        .iter()
        .filter_map(|email| extract_account_family_suffix(email, base_email).ok())
        .flatten()
        .max()
        .map(|suffix| suffix.saturating_add(1))
        .unwrap_or(1)
}

fn collect_known_account_emails(pool: &Pool, store: &CredentialStore) -> Vec<String> {
    let mut emails = pool
        .accounts
        .iter()
        .map(|entry| entry.email.clone())
        .collect::<Vec<_>>();
    emails.extend(store.pending.keys().cloned());
    emails
}

fn select_pending_credential_for_family(
    store: &CredentialStore,
    profile_name: &str,
    base_email: &str,
    alias: Option<&str>,
) -> Option<PendingCredential> {
    let normalized_base_email = normalize_base_email_family(base_email).ok()?;
    let normalized_alias = normalize_alias(alias);
    let mut matches = store
        .pending
        .values()
        .filter(|entry| {
            entry.stored.profile_name == profile_name
                && normalize_base_email_family(&entry.stored.base_email)
                    .map(|value| value == normalized_base_email)
                    .unwrap_or(false)
                && (normalized_alias.is_none()
                    || normalize_alias(entry.stored.alias.as_deref()) == normalized_alias)
        })
        .cloned()
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        left.stored
            .suffix
            .cmp(&right.stored.suffix)
            .then_with(|| {
                parse_sortable_timestamp(
                    left.started_at
                        .as_deref()
                        .or(Some(left.stored.created_at.as_str()))
                        .or(Some(left.stored.updated_at.as_str())),
                )
                .cmp(&parse_sortable_timestamp(
                    right
                        .started_at
                        .as_deref()
                        .or(Some(right.stored.created_at.as_str()))
                        .or(Some(right.stored.updated_at.as_str())),
                ))
            })
            .then_with(|| {
                parse_sortable_timestamp(Some(left.stored.updated_at.as_str())).cmp(
                    &parse_sortable_timestamp(Some(right.stored.updated_at.as_str())),
                )
            })
    });
    matches.into_iter().next()
}

fn select_pending_base_email_hint_for_profile(
    store: &CredentialStore,
    profile_name: &str,
    alias: Option<&str>,
) -> Option<String> {
    let normalized_alias = normalize_alias(alias);
    let mut matches = store
        .pending
        .values()
        .filter(|entry| {
            entry.stored.profile_name == profile_name
                && (normalized_alias.is_none()
                    || normalize_alias(entry.stored.alias.as_deref()) == normalized_alias)
        })
        .cloned()
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        let left_priority =
            get_create_family_hint_priority(&left.stored.base_email, left.stored.suffix + 1);
        let right_priority =
            get_create_family_hint_priority(&right.stored.base_email, right.stored.suffix + 1);
        left_priority
            .family_rank
            .cmp(&right_priority.family_rank)
            .reverse()
            .then_with(|| {
                left_priority
                    .frontier
                    .cmp(&right_priority.frontier)
                    .reverse()
            })
            .then_with(|| {
                parse_sortable_timestamp(
                    left.started_at
                        .as_deref()
                        .or(Some(left.stored.created_at.as_str()))
                        .or(Some(left.stored.updated_at.as_str())),
                )
                .cmp(&parse_sortable_timestamp(
                    right
                        .started_at
                        .as_deref()
                        .or(Some(right.stored.created_at.as_str()))
                        .or(Some(right.stored.updated_at.as_str())),
                ))
            })
            .then_with(|| left.stored.suffix.cmp(&right.stored.suffix))
            .then_with(|| {
                parse_sortable_timestamp(Some(left.stored.updated_at.as_str())).cmp(
                    &parse_sortable_timestamp(Some(right.stored.updated_at.as_str())),
                )
            })
    });

    matches
        .into_iter()
        .find_map(|entry| normalize_base_email_family(&entry.stored.base_email).ok())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CreateFamilyHintPriority {
    family_rank: u8,
    frontier: u32,
}

fn get_create_family_hint_priority(base_email: &str, frontier: u32) -> CreateFamilyHintPriority {
    let normalized_frontier = frontier.max(1);
    let family_rank = parse_email_family(base_email)
        .ok()
        .map(|parsed| match parsed.mode {
            EmailFamilyMode::Template { prefix, suffix } => {
                if parsed.domain_part == "astronlab.com" && prefix == "dev." && suffix.is_empty() {
                    2
                } else {
                    1
                }
            }
            EmailFamilyMode::GmailPlus => 0,
        })
        .unwrap_or(0);
    CreateFamilyHintPriority {
        family_rank,
        frontier: normalized_frontier,
    }
}

#[cfg(test)]
fn should_use_default_create_family_hint(base_email: Option<&str>) -> bool {
    base_email
        .and_then(|value| parse_email_family(value).ok())
        .map(|parsed| matches!(parsed.mode, EmailFamilyMode::Template { .. }))
        .unwrap_or(false)
}

#[cfg(test)]
fn normalize_gmail_base_email(email: &str) -> Result<String> {
    normalize_base_email_family(email)
}

#[cfg(test)]
fn compute_next_gmail_alias_suffix(
    base_email: &str,
    family_next_suffix: u32,
    known_emails: Vec<String>,
) -> Result<u32> {
    compute_next_account_family_suffix(base_email, family_next_suffix, known_emails)
}

#[cfg(test)]
fn normalize_email_candidate(value: &str) -> Option<String> {
    let trimmed = value.trim().to_lowercase();
    let (local, domain) = trimmed.split_once('@')?;
    if local.is_empty() || domain.is_empty() || domain.starts_with('.') || domain.ends_with('.') {
        return None;
    }
    domain.contains('.').then_some(trimmed)
}

#[cfg(test)]
fn extract_supported_gmail_emails(emails: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut supported = Vec::new();
    for email in emails {
        let Ok(normalized) = normalize_gmail_base_email(&email) else {
            continue;
        };
        if seen.insert(normalized.clone()) {
            supported.push(normalized);
        }
    }
    supported
}

#[cfg(test)]
fn tokenize_managed_profile_name(profile_name: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in profile_name.trim().to_lowercase().chars() {
        if ch.is_ascii_alphanumeric() {
            current.push(ch);
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

#[cfg(test)]
fn score_email_for_managed_profile_name(profile_name: &str, email: &str) -> i32 {
    let Some(normalized_email) = normalize_email_candidate(email) else {
        return i32::MIN;
    };

    let local_part = normalized_email
        .split('@')
        .next()
        .unwrap_or_default()
        .split('+')
        .next()
        .unwrap_or_default()
        .to_string();
    let compact_local = local_part
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>();
    let local_segments = local_part
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(ToOwned::to_owned)
        .collect::<HashSet<_>>();
    let significant_tokens = tokenize_managed_profile_name(profile_name)
        .into_iter()
        .filter(|token| {
            token.len() > 1 || token.chars().all(|character| character.is_ascii_digit())
        })
        .collect::<Vec<_>>();

    let mut score = 0;
    for token in significant_tokens {
        if local_segments.contains(&token) {
            score += if token.chars().all(|character| character.is_ascii_digit()) {
                140
            } else {
                120
            };
            continue;
        }
        if compact_local.starts_with(&token) || compact_local.ends_with(&token) {
            score += 40;
            continue;
        }
        if compact_local.contains(&token) {
            score += 25;
        }
    }

    let compact_profile = profile_name
        .to_lowercase()
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>();
    if compact_profile.len() >= 3 {
        if compact_local.contains(&compact_profile) {
            score += 80;
        } else {
            let reversed = compact_profile.chars().rev().collect::<String>();
            if compact_local.contains(&reversed) {
                score += 40;
            }
        }
    }

    score
}

#[cfg(test)]
fn select_best_email_for_managed_profile(
    profile_name: &str,
    emails: impl IntoIterator<Item = String>,
    preferred_base_email: Option<&str>,
) -> Option<String> {
    let normalized_preferred =
        preferred_base_email.and_then(|value| normalize_gmail_base_email(value).ok());
    let mut candidates = extract_supported_gmail_emails(emails)
        .into_iter()
        .enumerate()
        .map(|(index, email)| {
            let exact_preferred = normalized_preferred
                .as_ref()
                .map(|preferred| preferred == &email)
                .unwrap_or(false);
            let score = score_email_for_managed_profile_name(profile_name, &email);
            (index, email, exact_preferred, score)
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.2
            .cmp(&right.2)
            .reverse()
            .then_with(|| left.3.cmp(&right.3).reverse())
            .then_with(|| left.0.cmp(&right.0))
    });
    candidates.into_iter().next().map(|(_, email, _, _)| email)
}

#[cfg(test)]
fn select_stored_base_email_hint(store: &CredentialStore, profile_name: &str) -> Option<String> {
    let mut candidates = HashMap::<String, (u32, i64, u32)>::new();

    let mut remember = |raw_email: Option<&str>, updated_at: Option<&str>, frontier: u32| {
        let Some(raw_email) = raw_email else {
            return;
        };
        let Ok(base_email) = normalize_base_email_family(raw_email) else {
            return;
        };
        let entry = candidates.entry(base_email).or_insert((0, 0, 1));
        entry.0 += 1;
        entry.1 = entry.1.max(parse_sortable_timestamp(updated_at));
        entry.2 = entry.2.max(frontier.max(1));
    };

    for family in store.families.values() {
        if family.profile_name == profile_name {
            remember(
                Some(&family.base_email),
                Some(family.updated_at.as_str()),
                family.next_suffix,
            );
        }
    }
    for pending in store.pending.values() {
        if pending.stored.profile_name == profile_name {
            remember(
                Some(&pending.stored.base_email),
                pending
                    .started_at
                    .as_deref()
                    .or(Some(pending.stored.updated_at.as_str())),
                pending.stored.suffix.saturating_add(1),
            );
        }
    }

    candidates
        .into_iter()
        .max_by(|left, right| {
            let left_priority = get_create_family_hint_priority(&left.0, left.1 .2);
            let right_priority = get_create_family_hint_priority(&right.0, right.1 .2);
            left_priority
                .family_rank
                .cmp(&right_priority.family_rank)
                .then_with(|| left_priority.frontier.cmp(&right_priority.frontier))
                .then_with(|| left.1 .0.cmp(&right.1 .0))
                .then_with(|| left.1 .1.cmp(&right.1 .1))
                .then_with(|| right.0.cmp(&left.0))
        })
        .map(|(base_email, _)| base_email)
}

#[cfg(test)]
fn select_best_system_chrome_profile_match(
    profile_name: &str,
    profiles: &[SystemChromeProfileCandidate],
    preferred_base_email: Option<&str>,
) -> Option<SystemChromeProfileMatch> {
    let normalized_preferred =
        preferred_base_email.and_then(|value| normalize_gmail_base_email(value).ok());
    profiles
        .iter()
        .filter_map(|profile| {
            let matched_email = select_best_email_for_managed_profile(
                profile_name,
                profile.emails.clone(),
                preferred_base_email,
            )?;
            let emails = extract_supported_gmail_emails(profile.emails.clone());
            let score = if normalized_preferred
                .as_ref()
                .map(|preferred| preferred == &matched_email)
                .unwrap_or(false)
            {
                10_000
            } else {
                score_email_for_managed_profile_name(profile_name, &matched_email)
            };
            Some(SystemChromeProfileMatch {
                directory: profile.directory.clone(),
                name: profile.name.clone(),
                emails,
                matched_email,
                score,
            })
        })
        .max_by(|left, right| {
            left.score
                .cmp(&right.score)
                .then_with(|| right.directory.cmp(&left.directory))
        })
}

fn resolve_relogin_credential(
    store: &CredentialStore,
    entry: &AccountEntry,
) -> Option<StoredCredential> {
    if let Some(pending) = store
        .pending
        .get(&normalize_email_key(&entry.email))
        .map(|value| value.stored.clone())
    {
        return Some(pending);
    }
    let family_match = select_family_for_account_email(store, &entry.email)?;
    Some(StoredCredential {
        email: entry.email.clone(),
        profile_name: family_match.family.profile_name.clone(),
        base_email: family_match.family.base_email.clone(),
        suffix: family_match.suffix,
        selector: Some(entry.label.clone()),
        alias: entry.alias.clone(),
        birth_month: None,
        birth_day: None,
        birth_year: None,
        created_at: family_match.family.created_at.clone(),
        updated_at: family_match.family.updated_at.clone(),
    })
}

#[derive(Clone)]
struct FamilyAccountMatch {
    key: String,
    family: CredentialFamily,
    suffix: u32,
}

fn select_family_for_account_email(
    store: &CredentialStore,
    email: &str,
) -> Option<FamilyAccountMatch> {
    let normalized_email = normalize_email_key(email);
    let mut matches = store
        .families
        .iter()
        .filter_map(|(key, family)| {
            extract_account_family_suffix(&normalized_email, &family.base_email)
                .ok()
                .flatten()
                .map(|suffix| FamilyAccountMatch {
                    key: key.clone(),
                    family: family.clone(),
                    suffix,
                })
        })
        .collect::<Vec<_>>();

    if matches.is_empty() {
        return None;
    }

    matches.sort_by(|left, right| {
        let left_exact =
            left.family.last_created_email.as_deref() == Some(normalized_email.as_str());
        let right_exact =
            right.family.last_created_email.as_deref() == Some(normalized_email.as_str());
        left_exact
            .cmp(&right_exact)
            .then_with(|| {
                parse_sortable_timestamp(Some(left.family.updated_at.as_str())).cmp(
                    &parse_sortable_timestamp(Some(right.family.updated_at.as_str())),
                )
            })
            .then_with(|| right.key.cmp(&left.key))
    });

    let top = matches.pop()?;
    let top_exact = top.family.last_created_email.as_deref() == Some(normalized_email.as_str());
    if top_exact {
        let other_exact_exists = matches.iter().any(|entry| {
            entry.family.last_created_email.as_deref() == Some(normalized_email.as_str())
        });
        if other_exact_exists {
            return None;
        }
        return Some(top);
    }

    if matches.is_empty() {
        return Some(top);
    }

    None
}

fn upsert_family_for_account(store: &mut CredentialStore, account: &StoredCredential) -> bool {
    let Ok(family_key) = make_credential_family_key(&account.profile_name, &account.base_email)
    else {
        return false;
    };
    let next_updated_at = account.updated_at.clone();
    let next_created_at = account.created_at.clone();
    let next_last_created_email = Some(account.email.clone());
    let next_suffix = account.suffix.saturating_add(1);
    match store.families.get_mut(&family_key) {
        Some(existing) => {
            let previous = existing.clone();
            existing.next_suffix = existing.next_suffix.max(next_suffix);
            if parse_sortable_timestamp(Some(next_created_at.as_str()))
                < parse_sortable_timestamp(Some(existing.created_at.as_str()))
                || existing.created_at.trim().is_empty()
            {
                existing.created_at = next_created_at.clone();
            }
            if parse_sortable_timestamp(Some(next_updated_at.as_str()))
                >= parse_sortable_timestamp(Some(existing.updated_at.as_str()))
            {
                existing.updated_at = next_updated_at.clone();
                existing.last_created_email = next_last_created_email.clone();
            }
            previous != *existing
        }
        None => {
            store.families.insert(
                family_key,
                CredentialFamily {
                    profile_name: account.profile_name.clone(),
                    base_email: account.base_email.clone(),
                    next_suffix,
                    created_at: next_created_at,
                    updated_at: next_updated_at,
                    last_created_email: next_last_created_email,
                },
            );
            true
        }
    }
}

fn merge_legacy_account_into_families(
    families: &mut HashMap<String, CredentialFamily>,
    account: &StoredCredential,
) {
    let Ok(family_key) = make_credential_family_key(&account.profile_name, &account.base_email)
    else {
        return;
    };
    let updated_at = parse_sortable_timestamp(Some(account.updated_at.as_str()));
    let created_at = parse_sortable_timestamp(Some(account.created_at.as_str()));
    match families.get_mut(&family_key) {
        Some(existing) => {
            existing.next_suffix = existing.next_suffix.max(account.suffix.saturating_add(1));
            if created_at < parse_sortable_timestamp(Some(existing.created_at.as_str()))
                || existing.created_at.trim().is_empty()
            {
                existing.created_at = account.created_at.clone();
            }
            if updated_at >= parse_sortable_timestamp(Some(existing.updated_at.as_str())) {
                existing.updated_at = account.updated_at.clone();
                existing.last_created_email = Some(account.email.clone());
            }
        }
        None => {
            families.insert(
                family_key,
                CredentialFamily {
                    profile_name: account.profile_name.clone(),
                    base_email: account.base_email.clone(),
                    next_suffix: account.suffix.saturating_add(1),
                    created_at: account.created_at.clone(),
                    updated_at: account.updated_at.clone(),
                    last_created_email: Some(account.email.clone()),
                },
            );
        }
    }
}

fn parse_sortable_timestamp(value: Option<&str>) -> i64 {
    value
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.timestamp_millis())
        .unwrap_or(0)
}

fn normalize_email_key(email: &str) -> String {
    email.trim().to_lowercase()
}

fn normalize_alias(alias: Option<&str>) -> Option<String> {
    alias
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn generate_password(length: usize) -> String {
    const UPPERCASE: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ";
    const LOWERCASE: &[u8] = b"abcdefghijkmnopqrstuvwxyz";
    const DIGITS: &[u8] = b"23456789";
    const ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789";

    assert!(length >= 12);

    let mut chars = vec![
        pick_random_char(UPPERCASE),
        pick_random_char(LOWERCASE),
        pick_random_char(DIGITS),
    ];
    while chars.len() < length {
        chars.push(pick_random_char(ALPHABET));
    }
    fisher_yates_shuffle(&mut chars);
    chars.into_iter().collect()
}

fn pick_random_char(source: &[u8]) -> char {
    let mut bytes = [0u8; 8];
    OsRng.fill_bytes(&mut bytes);
    let index = u64::from_le_bytes(bytes) as usize % source.len();
    source[index] as char
}

fn fisher_yates_shuffle(chars: &mut [char]) {
    for index in (1..chars.len()).rev() {
        let mut bytes = [0u8; 8];
        OsRng.fill_bytes(&mut bytes);
        let swap_index = u64::from_le_bytes(bytes) as usize % (index + 1);
        chars.swap(index, swap_index);
    }
}

fn resolve_credential_birth_date(
    _credential: Option<&StoredCredential>,
    _now: DateTime<Utc>,
) -> AdultBirthDate {
    default_openai_birth_date()
}

fn default_openai_birth_date() -> AdultBirthDate {
    AdultBirthDate {
        birth_month: DEFAULT_OPENAI_BIRTH_MONTH,
        birth_day: DEFAULT_OPENAI_BIRTH_DAY,
        birth_year: DEFAULT_OPENAI_BIRTH_YEAR,
    }
}

fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{stamp}"))
    }

    fn make_pending(
        email: &str,
        profile_name: &str,
        base_email: &str,
        suffix: u32,
        created_at: &str,
    ) -> PendingCredential {
        PendingCredential {
            stored: StoredCredential {
                email: email.to_string(),
                profile_name: profile_name.to_string(),
                base_email: base_email.to_string(),
                suffix,
                selector: None,
                alias: None,
                birth_month: None,
                birth_day: None,
                birth_year: None,
                created_at: created_at.to_string(),
                updated_at: created_at.to_string(),
            },
            started_at: Some(created_at.to_string()),
        }
    }

    fn repo_root() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("..")
            .join("..")
            .canonicalize()
            .expect("repo root")
    }

    #[test]
    fn stored_credentials_used_only_for_non_manual_relogin() {
        let stored = StoredCredential {
            email: "dev.user+1@gmail.com".to_string(),
            profile_name: "dev-1".to_string(),
            base_email: "dev.user@gmail.com".to_string(),
            suffix: 1,
            selector: Some("dev.user+1@gmail.com_free".to_string()),
            alias: None,
            birth_month: None,
            birth_day: None,
            birth_year: None,
            created_at: "2026-03-20T00:00:00.000Z".to_string(),
            updated_at: "2026-03-20T00:00:00.000Z".to_string(),
        };

        assert!(should_use_stored_credential_relogin(
            Some(&stored),
            &ReloginOptions::default()
        ));
        assert!(!should_use_stored_credential_relogin(
            Some(&stored),
            &ReloginOptions {
                manual_login: true,
                ..ReloginOptions::default()
            }
        ));
    }

    #[test]
    fn read_workflow_file_metadata_reads_preferred_profile_from_main_workflow() {
        let workflow_file = repo_root()
            .join(".fast-browser")
            .join("workflows")
            .join("web")
            .join("auth.openai.com")
            .join("codex-rotate-account-flow-main.yaml");

        let metadata = read_workflow_file_metadata(&workflow_file).expect("workflow metadata");

        assert_eq!(
            metadata.workflow_ref.as_deref(),
            Some("workspace.web.auth-openai-com.codex-rotate-account-flow-main")
        );
        assert_eq!(metadata.preferred_profile_name.as_deref(), Some("dev-1"));
        assert_eq!(metadata.preferred_email, None);
    }

    #[test]
    fn derive_workflow_ref_from_file_path_handles_alternate_local_workflow() {
        let workflow_file = repo_root()
            .join(".fast-browser")
            .join("workflows")
            .join("web")
            .join("auth.openai.com")
            .join("codex-rotate-account-flow-minimal.yaml");

        assert_eq!(
            derive_workflow_ref_from_file_path(&workflow_file).as_deref(),
            Some("workspace.web.auth-openai-com.codex-rotate-account-flow-minimal")
        );
    }

    #[test]
    fn parse_workflow_file_metadata_handles_quotes_and_comments() {
        let metadata = parse_workflow_file_metadata(
            r#"
document:
  metadata:
    preferredProfile: "dev-1" # comment
    preferredEmail: 'dev.41@astronlab.com'
    targets:
      - id: primary
"#,
        );

        assert_eq!(metadata.preferred_profile_name.as_deref(), Some("dev-1"));
        assert_eq!(
            metadata.preferred_email.as_deref(),
            Some("dev.41@astronlab.com")
        );
    }

    #[test]
    fn normalize_gmail_base_address_before_suffixing() {
        assert_eq!(
            normalize_gmail_base_email("Dev.User+17@gmail.com").unwrap(),
            "dev.user@gmail.com"
        );
    }

    #[test]
    fn compute_next_gmail_alias_suffix_respects_known_emails_and_frontier() {
        assert_eq!(
            compute_next_gmail_alias_suffix(
                "dev.user@gmail.com",
                1,
                vec![
                    "dev.user+1@gmail.com".to_string(),
                    "dev.user+7@gmail.com".to_string(),
                    "other@gmail.com".to_string(),
                ],
            )
            .unwrap(),
            2
        );
        assert_eq!(
            compute_next_gmail_alias_suffix(
                "dev.user@gmail.com",
                5,
                vec![
                    "dev.user+1@gmail.com".to_string(),
                    "dev.user+2@gmail.com".to_string(),
                ],
            )
            .unwrap(),
            5
        );
    }

    #[test]
    fn builds_and_normalizes_templated_families() {
        assert_eq!(
            normalize_base_email_family("Dev.{N}@HotspotPrime.com").unwrap(),
            "dev.{n}@hotspotprime.com"
        );
        assert_eq!(
            build_account_family_email("dev.{N}@hotspotprime.com", 7).unwrap(),
            "dev.7@hotspotprime.com"
        );
        assert_eq!(
            compute_next_account_family_suffix(
                "dev.{N}@hotspotprime.com",
                1,
                vec![
                    "dev.1@hotspotprime.com".to_string(),
                    "dev.4@hotspotprime.com".to_string(),
                    "other@gmail.com".to_string(),
                ],
            )
            .unwrap(),
            2
        );
        assert_eq!(
            compute_next_account_family_suffix(
                "dev.{N}@astronlab.com",
                3,
                vec!["dev.21@astronlab.com".to_string()],
            )
            .unwrap(),
            3
        );
    }

    #[test]
    fn create_family_hint_accepts_templates_and_rejects_gmail() {
        assert!(should_use_default_create_family_hint(Some(
            "dev.{n}@astronlab.com"
        )));
        assert!(should_use_default_create_family_hint(Some(
            "qa.{n}@astronlab.com"
        )));
        assert!(!should_use_default_create_family_hint(Some(
            "dev.user@gmail.com"
        )));
        assert!(!should_use_default_create_family_hint(None));
    }

    #[test]
    fn resolve_managed_profile_name_from_candidates_matches_requested_preferred_and_default() {
        assert_eq!(
            resolve_managed_profile_name_from_candidates(
                &["dev-1", "other"],
                Some("dev-1"),
                None,
                None,
                Some("other"),
            )
            .unwrap(),
            "dev-1"
        );
        assert_eq!(
            resolve_managed_profile_name_from_candidates(
                &["dev-1", "other"],
                None,
                Some("other"),
                Some("workflow"),
                Some("dev-1"),
            )
            .unwrap(),
            "other"
        );
        assert_eq!(
            resolve_managed_profile_name_from_candidates(
                &["dev-1", "other"],
                None,
                None,
                None,
                Some("other"),
            )
            .unwrap(),
            "other"
        );
    }

    #[test]
    fn resolve_create_base_email_prefers_requested_then_discovered_then_default() {
        assert_eq!(
            resolve_create_base_email(Some("other@gmail.com"), Some("dev.user@gmail.com")).unwrap(),
            "other@gmail.com"
        );
        assert_eq!(
            resolve_create_base_email(None, Some("Dev.User+4@gmail.com")).unwrap(),
            "dev.user@gmail.com"
        );
        assert_eq!(
            resolve_create_base_email(None, None).unwrap(),
            "dev.{n}@astronlab.com"
        );
    }

    #[test]
    fn score_email_prefers_exact_profile_token_match() {
        assert!(
            score_email_for_managed_profile_name("dev-1", "1.dev.astronlab@gmail.com")
                > score_email_for_managed_profile_name("dev-1", "dev.2.astronlab@gmail.com")
        );
    }

    #[test]
    fn select_best_email_and_system_chrome_profile_match() {
        assert_eq!(
            select_best_email_for_managed_profile(
                "dev-1",
                vec![
                    "other@gmail.com".to_string(),
                    "1.dev.astronlab@gmail.com".to_string(),
                ],
                None,
            )
            .as_deref(),
            Some("1.dev.astronlab@gmail.com")
        );

        let match_result = select_best_system_chrome_profile_match(
            "dev-1",
            &[
                SystemChromeProfileCandidate {
                    directory: "Profile 1".to_string(),
                    name: "Personal".to_string(),
                    emails: vec!["other@gmail.com".to_string()],
                },
                SystemChromeProfileCandidate {
                    directory: "Profile 2".to_string(),
                    name: "Dev".to_string(),
                    emails: vec!["1.dev.astronlab@gmail.com".to_string()],
                },
            ],
            None,
        )
        .expect("profile match");
        assert_eq!(match_result.directory, "Profile 2");
        assert_eq!(match_result.matched_email, "1.dev.astronlab@gmail.com");
    }

    #[test]
    fn stored_relogin_honors_logout_setting() {
        assert!(should_logout_before_stored_relogin(
            &ReloginOptions::default()
        ));
        assert!(!should_logout_before_stored_relogin(&ReloginOptions {
            logout_first: false,
            ..ReloginOptions::default()
        }));
    }

    #[test]
    fn uses_fixed_default_birth_date() {
        let value = default_openai_birth_date();
        assert_eq!(value.birth_month, 1);
        assert_eq!(value.birth_day, 24);
        assert_eq!(value.birth_year, 1990);
    }

    #[test]
    fn normalize_credential_store_sets_v4_default_create_family() {
        let store = normalize_credential_store(json!({}));
        assert_eq!(store.version, 4);
        assert_eq!(store.default_create_base_email, "dev.{n}@astronlab.com");
    }

    #[test]
    fn select_pending_base_email_hint_prefers_dev_template_family() {
        let mut store = CredentialStore::default();
        store.pending.insert(
            "qa.300@astronlab.com".to_string(),
            make_pending(
                "qa.300@astronlab.com",
                "dev-1",
                "qa.{n}@astronlab.com",
                300,
                "2026-04-06T17:00:00.000Z",
            ),
        );
        store.pending.insert(
            "dev.30@astronlab.com".to_string(),
            make_pending(
                "dev.30@astronlab.com",
                "dev-1",
                "dev.{n}@astronlab.com",
                30,
                "2026-04-06T16:00:00.000Z",
            ),
        );

        assert_eq!(
            select_pending_base_email_hint_for_profile(&store, "dev-1", None).as_deref(),
            Some("dev.{n}@astronlab.com")
        );
    }

    #[test]
    fn select_pending_credential_for_family_drains_lowest_suffix_first() {
        let mut store = CredentialStore::default();
        store.pending.insert(
            "dev.user+1@gmail.com".to_string(),
            make_pending(
                "dev.user+1@gmail.com",
                "dev-1",
                "dev.user@gmail.com",
                1,
                "2026-03-20T00:00:00.000Z",
            ),
        );
        store.pending.insert(
            "dev.user+3@gmail.com".to_string(),
            make_pending(
                "dev.user+3@gmail.com",
                "dev-1",
                "dev.user@gmail.com",
                3,
                "2026-03-20T03:00:00.000Z",
            ),
        );

        assert_eq!(
            select_pending_credential_for_family(&store, "dev-1", "dev.user@gmail.com", None)
                .map(|entry| entry.stored.email),
            Some("dev.user+1@gmail.com".to_string())
        );
    }

    #[test]
    fn select_pending_credential_for_family_can_filter_by_alias() {
        let mut store = CredentialStore::default();
        let mut left = make_pending(
            "dev.user+2@gmail.com",
            "dev-1",
            "dev.user@gmail.com",
            2,
            "2026-03-20T02:00:00.000Z",
        );
        left.stored.alias = Some("team-a".to_string());
        let mut right = make_pending(
            "dev.user+3@gmail.com",
            "dev-1",
            "dev.user@gmail.com",
            3,
            "2026-03-20T03:00:00.000Z",
        );
        right.stored.alias = Some("team-b".to_string());
        store.pending.insert(left.stored.email.clone(), left);
        store.pending.insert(right.stored.email.clone(), right);

        assert_eq!(
            select_pending_credential_for_family(
                &store,
                "dev-1",
                "dev.user@gmail.com",
                Some("team-a"),
            )
            .map(|entry| entry.stored.email),
            Some("dev.user+2@gmail.com".to_string())
        );
    }

    #[test]
    fn select_pending_base_email_hint_prefers_oldest_family_when_rank_is_equal() {
        let mut store = CredentialStore::default();
        store.pending.insert(
            "1.dev.astronlab+1@gmail.com".to_string(),
            make_pending(
                "1.dev.astronlab+1@gmail.com",
                "dev-1",
                "1.dev.astronlab@gmail.com",
                1,
                "2026-03-20T00:00:00.000Z",
            ),
        );
        store.pending.insert(
            "arjuda.anjum+1@gmail.com".to_string(),
            make_pending(
                "arjuda.anjum+1@gmail.com",
                "dev-1",
                "arjuda.anjum@gmail.com",
                1,
                "2026-03-21T00:00:00.000Z",
            ),
        );

        assert_eq!(
            select_pending_base_email_hint_for_profile(&store, "dev-1", None).as_deref(),
            Some("1.dev.astronlab@gmail.com")
        );
    }

    #[test]
    fn select_stored_base_email_hint_prefers_common_and_high_frontier_template() {
        let mut store = CredentialStore::default();
        store.families.insert(
            "dev-1::qa.{n}@astronlab.com".to_string(),
            CredentialFamily {
                profile_name: "dev-1".to_string(),
                base_email: "qa.{n}@astronlab.com".to_string(),
                next_suffix: 300,
                created_at: "2026-04-06T16:00:00.000Z".to_string(),
                updated_at: "2026-04-06T16:00:00.000Z".to_string(),
                last_created_email: Some("qa.299@astronlab.com".to_string()),
            },
        );
        store.pending.insert(
            "dev.35@astronlab.com".to_string(),
            make_pending(
                "dev.35@astronlab.com",
                "dev-1",
                "dev.{n}@astronlab.com",
                35,
                "2026-04-06T17:00:00.000Z",
            ),
        );
        store.pending.insert(
            "dev.36@astronlab.com".to_string(),
            make_pending(
                "dev.36@astronlab.com",
                "dev-1",
                "dev.{n}@astronlab.com",
                36,
                "2026-04-06T18:00:00.000Z",
            ),
        );

        assert_eq!(
            select_stored_base_email_hint(&store, "dev-1").as_deref(),
            Some("dev.{n}@astronlab.com")
        );

        let mut store = CredentialStore::default();
        store.pending.insert(
            "qa.300@astronlab.com".to_string(),
            make_pending(
                "qa.300@astronlab.com",
                "dev-1",
                "qa.{n}@astronlab.com",
                300,
                "2026-04-06T18:00:00.000Z",
            ),
        );
        store.pending.insert(
            "dev.35@astronlab.com".to_string(),
            make_pending(
                "dev.35@astronlab.com",
                "dev-1",
                "dev.{n}@astronlab.com",
                35,
                "2026-04-06T17:00:00.000Z",
            ),
        );

        assert_eq!(
            select_stored_base_email_hint(&store, "dev-1").as_deref(),
            Some("dev.{n}@astronlab.com")
        );
    }

    #[test]
    fn resolve_create_base_email_for_profile_uses_pending_hint_before_new_default_family() {
        let mut store = CredentialStore::default();
        store.default_create_base_email = "qa.{n}@astronlab.com".to_string();
        store.pending.insert(
            "qa.300@astronlab.com".to_string(),
            make_pending(
                "qa.300@astronlab.com",
                "dev-1",
                "qa.{n}@astronlab.com",
                300,
                "2026-04-06T17:00:00.000Z",
            ),
        );

        assert_eq!(
            resolve_create_base_email_for_profile(&store, "dev-1", None, None).unwrap(),
            "qa.{n}@astronlab.com"
        );
    }

    #[test]
    fn resolve_create_base_email_for_profile_falls_back_to_dev_default_for_new_creates() {
        let mut store = CredentialStore::default();
        store.default_create_base_email = "qa.{n}@astronlab.com".to_string();

        assert_eq!(
            resolve_create_base_email_for_profile(&store, "dev-1", None, None).unwrap(),
            "dev.{n}@astronlab.com"
        );
    }

    #[test]
    fn resolve_create_base_email_for_profile_respects_explicit_override() {
        let mut store = CredentialStore::default();
        store.pending.insert(
            "dev.30@astronlab.com".to_string(),
            make_pending(
                "dev.30@astronlab.com",
                "dev-1",
                "dev.{n}@astronlab.com",
                30,
                "2026-04-06T16:00:00.000Z",
            ),
        );

        assert_eq!(
            resolve_create_base_email_for_profile(
                &store,
                "dev-1",
                Some("qa.{n}@astronlab.com"),
                None,
            )
            .unwrap(),
            "qa.{n}@astronlab.com"
        );
    }

    #[test]
    fn normalize_credential_store_drops_legacy_bench_families_on_v4_migration() {
        let store = normalize_credential_store(json!({
            "version": 3,
            "families": {
                "dev-1::bench.devicefix.{n}@astronlab.com": {
                    "profile_name": "dev-1",
                    "base_email": "bench.devicefix.{n}@astronlab.com",
                    "next_suffix": 8,
                    "created_at": "2026-04-06T00:00:00.000Z",
                    "updated_at": "2026-04-06T00:00:00.000Z",
                    "last_created_email": "bench.devicefix.7@astronlab.com"
                },
                "dev-1::dev.{n}@astronlab.com": {
                    "profile_name": "dev-1",
                    "base_email": "dev.{n}@astronlab.com",
                    "next_suffix": 35,
                    "created_at": "2026-04-06T00:00:00.000Z",
                    "updated_at": "2026-04-06T00:00:00.000Z",
                    "last_created_email": "dev.34@astronlab.com"
                }
            },
            "pending": {
                "bench.devicefix.8@astronlab.com": {
                    "email": "bench.devicefix.8@astronlab.com",
                    "profile_name": "dev-1",
                    "base_email": "bench.devicefix.{n}@astronlab.com",
                    "suffix": 8,
                    "selector": null,
                    "alias": null,
                    "created_at": "2026-04-06T00:00:00.000Z",
                    "updated_at": "2026-04-06T00:00:00.000Z"
                },
                "dev.35@astronlab.com": {
                    "email": "dev.35@astronlab.com",
                    "profile_name": "dev-1",
                    "base_email": "dev.{n}@astronlab.com",
                    "suffix": 35,
                    "selector": null,
                    "alias": null,
                    "created_at": "2026-04-06T00:00:00.000Z",
                    "updated_at": "2026-04-06T00:00:00.000Z"
                }
            }
        }));
        assert_eq!(store.version, 4);
        assert_eq!(store.default_create_base_email, "dev.{n}@astronlab.com");
        assert!(store.families.contains_key("dev-1::dev.{n}@astronlab.com"));
        assert!(!store
            .families
            .contains_key("dev-1::bench.devicefix.{n}@astronlab.com"));
        assert!(store.pending.contains_key("dev.35@astronlab.com"));
        assert!(!store
            .pending
            .contains_key("bench.devicefix.8@astronlab.com"));
    }

    #[test]
    fn normalize_credential_store_drops_non_dev_pending_even_in_v4_state() {
        let store = normalize_credential_store(json!({
            "version": 4,
            "pending": {
                "qa.300@astronlab.com": {
                    "email": "qa.300@astronlab.com",
                    "profile_name": "dev-1",
                    "base_email": "qa.{n}@astronlab.com",
                    "suffix": 300,
                    "selector": null,
                    "alias": null,
                    "created_at": "2026-04-06T17:00:00.000Z",
                    "updated_at": "2026-04-06T17:00:00.000Z",
                    "started_at": "2026-04-06T17:00:00.000Z"
                },
                "dev.user+1@gmail.com": {
                    "email": "dev.user+1@gmail.com",
                    "profile_name": "dev-1",
                    "base_email": "dev.user@gmail.com",
                    "suffix": 1,
                    "selector": null,
                    "alias": null,
                    "created_at": "2026-04-06T18:00:00.000Z",
                    "updated_at": "2026-04-06T18:00:00.000Z",
                    "started_at": "2026-04-06T18:00:00.000Z"
                },
                "dev.35@astronlab.com": {
                    "email": "dev.35@astronlab.com",
                    "profile_name": "dev-1",
                    "base_email": "dev.{n}@astronlab.com",
                    "suffix": 35,
                    "selector": null,
                    "alias": null,
                    "created_at": "2026-04-06T19:00:00.000Z",
                    "updated_at": "2026-04-06T19:00:00.000Z",
                    "started_at": "2026-04-06T19:00:00.000Z"
                }
            }
        }));
        assert_eq!(
            store.pending.keys().cloned().collect::<Vec<_>>(),
            vec!["dev.35@astronlab.com".to_string()]
        );
    }

    #[test]
    fn reuses_existing_birth_date() {
        let now = DateTime::parse_from_rfc3339("2026-04-02T00:00:00.000Z")
            .unwrap()
            .with_timezone(&Utc);
        let value = resolve_credential_birth_date(
            Some(&StoredCredential {
                email: "dev.user+1@gmail.com".to_string(),
                profile_name: "dev-1".to_string(),
                base_email: "dev.user@gmail.com".to_string(),
                suffix: 1,
                selector: None,
                alias: None,
                birth_month: Some(7),
                birth_day: Some(14),
                birth_year: Some(1994),
                created_at: "2026-03-20T00:00:00.000Z".to_string(),
                updated_at: "2026-03-20T00:00:00.000Z".to_string(),
            }),
            now,
        );
        assert_eq!(value.birth_month, 1);
        assert_eq!(value.birth_day, 24);
        assert_eq!(value.birth_year, 1990);
    }

    #[test]
    fn drops_legacy_secret_fields_from_loaded_records() {
        let store = normalize_credential_store(json!({
            "accounts": {
                "dev.user+1@gmail.com": {
                    "email": "dev.user+1@gmail.com",
                    "password": "pw-1",
                    "account_secret_ref": {
                        "type": "secret_ref",
                        "store": "bitwarden-cli",
                        "object_id": "bw-1"
                    },
                    "profile_name": "dev-1",
                    "base_email": "dev.user@gmail.com",
                    "suffix": 1,
                    "selector": "dev.user+1@gmail.com_free",
                    "alias": null,
                    "created_at": "2026-03-20T00:00:00.000Z",
                    "updated_at": "2026-03-20T00:00:00.000Z"
                }
            }
        }));
        let family = store.families.get("dev-1::dev.user@gmail.com").unwrap();
        assert_eq!(family.profile_name, "dev-1");
        assert_eq!(family.base_email, "dev.user@gmail.com");
        assert_eq!(family.next_suffix, 2);
        assert_eq!(
            family.last_created_email.as_deref(),
            Some("dev.user+1@gmail.com")
        );
    }

    #[test]
    fn drops_pending_entries_that_already_exist_in_inventory() {
        let store = normalize_credential_store(json!({
            "accounts": [
                {
                    "email": "dev.1@astronlab.com"
                }
            ],
            "pending": {
                "dev.1@astronlab.com": {
                    "email": "dev.1@astronlab.com",
                    "profile_name": "dev-1",
                    "base_email": "dev.{n}@astronlab.com",
                    "suffix": 1,
                    "selector": null,
                    "alias": null,
                    "created_at": "2026-04-05T04:50:10.406Z",
                    "updated_at": "2026-04-05T05:39:48.882Z"
                }
            }
        }));
        assert!(store.pending.is_empty());
    }

    #[test]
    fn drops_pending_entries_superseded_by_newer_inventory_suffixes() {
        let store = normalize_credential_store(json!({
            "accounts": [
                {
                    "email": "dev.23@astronlab.com"
                }
            ],
            "pending": {
                "dev.1@astronlab.com": {
                    "email": "dev.1@astronlab.com",
                    "profile_name": "dev-1",
                    "base_email": "dev.{n}@astronlab.com",
                    "suffix": 1,
                    "selector": null,
                    "alias": null,
                    "created_at": "2026-04-05T04:50:10.406Z",
                    "updated_at": "2026-04-05T05:39:48.882Z"
                }
            }
        }));
        assert!(store.pending.is_empty());
    }

    #[test]
    fn builds_openai_login_locator_from_email() {
        let locator = build_openai_account_login_locator("Dev.User+1@gmail.com");
        match locator {
            CodexRotateSecretLocator::LoginLookup {
                store,
                username,
                uris,
                field_path,
            } => {
                assert_eq!(store, "bitwarden-cli");
                assert_eq!(username, "dev.user+1@gmail.com");
                assert_eq!(field_path, "/password");
                assert_eq!(
                    uris,
                    vec![
                        "https://auth.openai.com".to_string(),
                        "https://chatgpt.com".to_string()
                    ]
                );
            }
        }
    }

    #[test]
    fn relogin_family_match_prefers_exact_last_created_email() {
        let mut store = CredentialStore::default();
        store.families.insert(
            "dev-1::dev.user@gmail.com".to_string(),
            CredentialFamily {
                profile_name: "dev-1".to_string(),
                base_email: "dev.user@gmail.com".to_string(),
                next_suffix: 4,
                created_at: "2026-03-20T00:00:00.000Z".to_string(),
                updated_at: "2026-03-20T01:00:00.000Z".to_string(),
                last_created_email: Some("dev.user+3@gmail.com".to_string()),
            },
        );
        store.families.insert(
            "dev-2::dev.user@gmail.com".to_string(),
            CredentialFamily {
                profile_name: "dev-2".to_string(),
                base_email: "dev.user@gmail.com".to_string(),
                next_suffix: 5,
                created_at: "2026-03-20T00:00:00.000Z".to_string(),
                updated_at: "2026-03-20T02:00:00.000Z".to_string(),
                last_created_email: Some("dev.user+2@gmail.com".to_string()),
            },
        );

        let match_result = select_family_for_account_email(&store, "dev.user+2@gmail.com")
            .expect("expected exact family match");
        assert_eq!(match_result.family.profile_name, "dev-2");
        assert_eq!(match_result.suffix, 2);
    }

    #[test]
    fn relogin_family_match_refuses_ambiguous_non_exact_matches() {
        let mut store = CredentialStore::default();
        store.families.insert(
            "dev-1::dev.user@gmail.com".to_string(),
            CredentialFamily {
                profile_name: "dev-1".to_string(),
                base_email: "dev.user@gmail.com".to_string(),
                next_suffix: 4,
                created_at: "2026-03-20T00:00:00.000Z".to_string(),
                updated_at: "2026-03-20T01:00:00.000Z".to_string(),
                last_created_email: Some("dev.user+3@gmail.com".to_string()),
            },
        );
        store.families.insert(
            "dev-2::dev.user@gmail.com".to_string(),
            CredentialFamily {
                profile_name: "dev-2".to_string(),
                base_email: "dev.user@gmail.com".to_string(),
                next_suffix: 5,
                created_at: "2026-03-20T00:00:00.000Z".to_string(),
                updated_at: "2026-03-20T02:00:00.000Z".to_string(),
                last_created_email: Some("dev.user+4@gmail.com".to_string()),
            },
        );

        assert!(select_family_for_account_email(&store, "dev.user+2@gmail.com").is_none());
    }

    #[test]
    fn derive_family_frontier_suffix_uses_highest_observed_suffix() {
        let known = vec![
            "dev.20@astronlab.com".to_string(),
            "dev.22@astronlab.com".to_string(),
            "dev.23@astronlab.com".to_string(),
        ];
        assert_eq!(
            derive_family_frontier_suffix("dev.{n}@astronlab.com", &known),
            24
        );
    }

    #[test]
    fn add_reconciliation_moves_matching_pending_into_family_state() {
        let mut store = CredentialStore::default();
        store.pending.insert(
            "dev.24@astronlab.com".to_string(),
            PendingCredential {
                stored: StoredCredential {
                    email: "dev.24@astronlab.com".to_string(),
                    profile_name: "dev-1".to_string(),
                    base_email: "dev.{n}@astronlab.com".to_string(),
                    suffix: 24,
                    selector: None,
                    alias: None,
                    birth_month: Some(1),
                    birth_day: Some(24),
                    birth_year: Some(1990),
                    created_at: "2026-04-05T05:51:09.049Z".to_string(),
                    updated_at: "2026-04-05T05:51:09.049Z".to_string(),
                },
                started_at: Some("2026-04-05T05:51:09.049Z".to_string()),
            },
        );
        let entry = AccountEntry {
            label: "dev.24@astronlab.com_free".to_string(),
            alias: None,
            email: "dev.24@astronlab.com".to_string(),
            account_id: "acct-24".to_string(),
            plan_type: "free".to_string(),
            auth: CodexAuth {
                auth_mode: "chatgpt".to_string(),
                openai_api_key: None,
                tokens: crate::auth::AuthTokens {
                    id_token: "id".to_string(),
                    access_token: "access".to_string(),
                    refresh_token: Some("refresh".to_string()),
                    account_id: "acct-24".to_string(),
                },
                last_refresh: "2026-04-05T05:51:09.049Z".to_string(),
            },
            added_at: "2026-04-05T05:51:09.049Z".to_string(),
            last_quota_usable: None,
            last_quota_summary: None,
            last_quota_blocker: None,
            last_quota_checked_at: None,
        };

        let pending = store.pending.remove("dev.24@astronlab.com").unwrap();
        assert!(upsert_family_for_account(
            &mut store,
            &StoredCredential {
                email: entry.email.clone(),
                profile_name: pending.stored.profile_name,
                base_email: pending.stored.base_email,
                suffix: pending.stored.suffix,
                selector: Some(entry.label.clone()),
                alias: entry.alias.clone(),
                birth_month: pending.stored.birth_month,
                birth_day: pending.stored.birth_day,
                birth_year: pending.stored.birth_year,
                created_at: pending.stored.created_at,
                updated_at: "2026-04-05T05:52:00.000Z".to_string(),
            },
        ));
        let family = store.families.get("dev-1::dev.{n}@astronlab.com").unwrap();
        assert_eq!(family.next_suffix, 25);
        assert_eq!(
            family.last_created_email.as_deref(),
            Some("dev.24@astronlab.com")
        );
        assert!(store.pending.is_empty());
    }

    #[test]
    fn migrates_legacy_credential_store_into_accounts_json() {
        let _guard = ENV_MUTEX.lock().expect("env mutex");
        let rotate_home = unique_temp_dir("codex-rotate-legacy-store");
        fs::create_dir_all(&rotate_home).expect("create rotate home");
        let accounts_path = rotate_home.join("accounts.json");
        let legacy_path = rotate_home.join("credentials.json");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
        }

        let result = (|| -> Result<()> {
            fs::write(
                &accounts_path,
                serde_json::json!({
                    "active_index": 2,
                    "accounts": [{ "email": "dev.22@astronlab.com" }],
                })
                .to_string(),
            )?;
            fs::write(
                &legacy_path,
                serde_json::json!({
                    "version": 3,
                    "families": {
                        "dev-1::dev.{n}@astronlab.com": {
                            "profile_name": "dev-1",
                            "base_email": "dev.{n}@astronlab.com",
                            "next_suffix": 23,
                            "created_at": "2026-04-05T00:00:00.000Z",
                            "updated_at": "2026-04-05T00:00:00.000Z",
                            "last_created_email": "dev.22@astronlab.com"
                        }
                    },
                    "pending": {
                        "dev.23@astronlab.com": {
                            "email": "dev.23@astronlab.com",
                            "profile_name": "dev-1",
                            "base_email": "dev.{n}@astronlab.com",
                            "suffix": 23,
                            "selector": null,
                            "alias": null,
                            "created_at": "2026-04-05T00:00:00.000Z",
                            "updated_at": "2026-04-05T00:00:00.000Z",
                            "started_at": "2026-04-05T00:00:00.000Z"
                        }
                    }
                })
                .to_string(),
            )?;

            assert!(migrate_legacy_credential_store_if_needed()?);

            let merged: serde_json::Value =
                serde_json::from_str(&fs::read_to_string(&accounts_path)?)?;
            assert_eq!(merged["active_index"], 2);
            assert_eq!(merged["accounts"][0]["email"], "dev.22@astronlab.com");
            assert_eq!(
                merged["families"]["dev-1::dev.{n}@astronlab.com"]["next_suffix"],
                23
            );
            assert_eq!(
                merged["pending"]["dev.23@astronlab.com"]["email"],
                "dev.23@astronlab.com"
            );
            assert!(!legacy_path.exists());
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
        fs::remove_dir_all(&rotate_home).ok();
        result.expect("legacy credential migration");
    }

    #[test]
    fn loading_credential_store_migrates_legacy_file_automatically() {
        let _guard = ENV_MUTEX.lock().expect("env mutex");
        let rotate_home = unique_temp_dir("codex-rotate-load-store");
        fs::create_dir_all(&rotate_home).expect("create rotate home");
        let legacy_path = rotate_home.join("credentials.json");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
        }

        let result = (|| -> Result<()> {
            fs::write(
                &legacy_path,
                serde_json::json!({
                    "version": 3,
                    "families": {
                        "dev-1::dev.{n}@astronlab.com": {
                            "profile_name": "dev-1",
                            "base_email": "dev.{n}@astronlab.com",
                            "next_suffix": 23,
                            "created_at": "2026-04-05T00:00:00.000Z",
                            "updated_at": "2026-04-05T00:00:00.000Z",
                            "last_created_email": "dev.22@astronlab.com"
                        }
                    },
                    "pending": {
                        "dev.23@astronlab.com": {
                            "email": "dev.23@astronlab.com",
                            "profile_name": "dev-1",
                            "base_email": "dev.{n}@astronlab.com",
                            "suffix": 23,
                            "selector": null,
                            "alias": null,
                            "created_at": "2026-04-05T00:00:00.000Z",
                            "updated_at": "2026-04-05T00:00:00.000Z",
                            "started_at": "2026-04-05T00:00:00.000Z"
                        }
                    }
                })
                .to_string(),
            )?;

            let store = load_credential_store()?;
            assert_eq!(
                store
                    .families
                    .get("dev-1::dev.{n}@astronlab.com")
                    .map(|family| family.next_suffix),
                Some(23)
            );
            assert!(store.pending.contains_key("dev.23@astronlab.com"));
            assert!(!legacy_path.exists());
            assert!(rotate_home.join("accounts.json").exists());
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
        fs::remove_dir_all(&rotate_home).ok();
        result.expect("load credential store migration");
    }
}
