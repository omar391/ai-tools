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
use crate::paths::resolve_paths;
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
    pub families: HashMap<String, CredentialFamily>,
    pub pending: HashMap<String, PendingCredential>,
}

impl Default for CredentialStore {
    fn default() -> Self {
        Self {
            version: 3,
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

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkflowFileMetadata {
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

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeWorkflowMetadataPayload<'a> {
    file_path: &'a str,
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
        let login_result = run_complete_codex_login(
            &updated_stored.profile_name,
            &updated_stored.email,
            Some(&account_login_locator),
            None,
            None,
            None,
            None,
            None,
        );
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
    let pending_base_hint_raw = if options.base_email.is_some() {
        None
    } else {
        select_pending_base_email_hint_for_profile(&store, &profile_name, options.alias.as_deref())
    };
    let stored_base_hint_raw = select_stored_base_email_hint(&store, &profile_name);
    let pending_base_hint = pending_base_hint_raw
        .as_deref()
        .filter(|value| should_use_default_create_family_hint(Some(value)))
        .map(ToOwned::to_owned);
    let stored_base_hint = stored_base_hint_raw
        .as_deref()
        .filter(|value| should_use_default_create_family_hint(Some(value)))
        .map(ToOwned::to_owned);
    let base_email = fatal(resolve_create_base_email(
        options.base_email.as_deref(),
        pending_base_hint
            .as_deref()
            .or(stored_base_hint.as_deref())
            .or(workflow_metadata.preferred_email.as_deref()),
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
    let paths = resolve_paths()?;
    let state = load_rotate_state_json()?;
    let mut store = normalize_credential_store(state);
    if paths.credentials_file.exists() {
        let raw = fs::read_to_string(&paths.credentials_file)
            .with_context(|| format!("Failed to read {}.", paths.credentials_file.display()))?;
        let parsed: Value = serde_json::from_str(&raw).with_context(|| {
            format!(
                "Invalid credential store at {}.",
                paths.credentials_file.display()
            )
        })?;
        store = normalize_credential_store(parsed);
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
    if store.families.is_empty() && store.pending.is_empty() {
        object.remove("version");
        object.remove("families");
        object.remove("pending");
    } else {
        let credential_state = serialize_credential_store(store);
        if let Some(version) = credential_state.get("version").cloned() {
            object.insert("version".to_string(), version);
        }
        if let Some(families) = credential_state.get("families").cloned() {
            object.insert("families".to_string(), families);
        }
        if let Some(pending) = credential_state.get("pending").cloned() {
            object.insert("pending".to_string(), pending);
        }
    }
    write_rotate_state_json(&state, true)
}

fn normalize_credential_store(raw: Value) -> CredentialStore {
    let inventory_emails = collect_inventory_emails_from_state(&raw);
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

    CredentialStore {
        version: 3,
        families,
        pending: normalize_pending_credential_map(raw.get("pending"))
            .into_iter()
            .filter(|(email, record)| {
                !inventory_emails.contains(email)
                    && !pending_is_superseded_by_inventory(record, &inventory_emails)
            })
            .collect(),
    }
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
        "version": 3,
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
    let payload = BridgeWorkflowMetadataPayload {
        file_path: file_path
            .to_str()
            .ok_or_else(|| anyhow!("Workflow file path is not valid UTF-8."))?,
    };
    run_automation_bridge("read-workflow-metadata", payload)
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

fn should_use_default_create_family_hint(base_email: Option<&str>) -> bool {
    base_email
        .and_then(|value| normalize_base_email_family(value).ok())
        .map(|value| value == DEFAULT_CREATE_BASE_EMAIL)
        .unwrap_or(false)
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
        .then_with(|| left.stored.suffix.cmp(&right.stored.suffix))
        .then_with(|| {
            parse_sortable_timestamp(Some(left.stored.updated_at.as_str())).cmp(
                &parse_sortable_timestamp(Some(right.stored.updated_at.as_str())),
            )
        })
    });
    let raw_email = matches
        .first()
        .map(|entry| entry.stored.base_email.as_str())
        .or_else(|| matches.first().map(|entry| entry.stored.email.as_str()))?;
    normalize_base_email_family(raw_email).ok()
}

fn select_stored_base_email_hint(store: &CredentialStore, profile_name: &str) -> Option<String> {
    let mut candidates = HashMap::<String, (u32, i64)>::new();
    let mut remember = |raw_email: Option<&str>, updated_at: Option<&str>| {
        let Some(raw_email) = raw_email else {
            return;
        };
        let Ok(base_email) = normalize_base_email_family(raw_email) else {
            return;
        };
        let updated_at_value = parse_sortable_timestamp(updated_at);
        let entry = candidates.entry(base_email).or_insert((0, 0));
        entry.0 += 1;
        entry.1 = entry.1.max(updated_at_value);
    };

    for family in store
        .families
        .values()
        .filter(|entry| entry.profile_name == profile_name)
    {
        remember(Some(&family.base_email), Some(&family.updated_at));
    }
    for pending in store
        .pending
        .values()
        .filter(|entry| entry.stored.profile_name == profile_name)
    {
        remember(
            Some(&pending.stored.base_email),
            pending
                .started_at
                .as_deref()
                .or(Some(pending.stored.updated_at.as_str())),
        );
    }

    candidates
        .into_iter()
        .max_by(|left, right| {
            left.1
                 .0
                .cmp(&right.1 .0)
                .then_with(|| left.1 .1.cmp(&right.1 .1))
                .then_with(|| right.0.cmp(&left.0))
        })
        .map(|entry| entry.0)
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
    fn uses_fixed_default_birth_date() {
        let value = default_openai_birth_date();
        assert_eq!(value.birth_month, 1);
        assert_eq!(value.birth_day, 24);
        assert_eq!(value.birth_year, 1990);
    }

    #[test]
    fn implicit_create_family_hint_accepts_only_default_dev_template() {
        assert!(should_use_default_create_family_hint(Some(
            "dev.{n}@astronlab.com"
        )));
        assert!(!should_use_default_create_family_hint(Some(
            "bench.device.{n}@astronlab.com"
        )));
        assert!(!should_use_default_create_family_hint(Some(
            "dev.user@gmail.com"
        )));
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
}
