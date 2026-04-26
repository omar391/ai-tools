use super::*;

pub(super) fn create_lock_path() -> Result<std::path::PathBuf> {
    Ok(resolve_paths()?
        .rotate_home
        .join("locks")
        .join("create.lock"))
}

pub(super) fn create_execution_mutex() -> &'static Mutex<()> {
    static CREATE_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
    CREATE_MUTEX.get_or_init(|| Mutex::new(()))
}

pub(super) fn create_lock_source_label(source: CreateCommandSource) -> &'static str {
    match source {
        CreateCommandSource::Manual => "manual",
        CreateCommandSource::Next => "next",
    }
}

pub(super) fn read_create_execution_lock_metadata(
    path: &Path,
) -> Option<CreateExecutionLockMetadata> {
    let raw = fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

pub(super) fn format_create_execution_lock_error(
    metadata: Option<CreateExecutionLockMetadata>,
) -> String {
    let Some(metadata) = metadata else {
        return format!("{CREATE_ALREADY_IN_PROGRESS_PREFIX}.");
    };
    let mut details = vec![
        format!("pid {}", metadata.pid),
        format!("started {}", metadata.started_at),
        format!("source {}", metadata.source),
    ];
    if let Some(profile_name) = metadata.profile_name.as_deref() {
        details.push(format!("profile {}", profile_name));
    }
    if let Some(template) = metadata.template.as_deref() {
        details.push(format!("base {}", template));
    }
    if let Some(alias) = metadata.alias.as_deref() {
        details.push(format!("alias {}", alias));
    }
    format!(
        "{CREATE_ALREADY_IN_PROGRESS_PREFIX} ({}).",
        details.join(", ")
    )
}

pub(super) fn acquire_create_execution_lock(
    options: &CreateCommandOptions,
    progress: Option<&AutomationProgressCallback>,
) -> Result<CreateExecutionLock> {
    let process_guard = create_execution_mutex()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let lock_path = create_lock_path()?;
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("Failed to open {}.", lock_path.display()))?;
    let mut reported_wait = false;
    loop {
        cancel::check_canceled()?;
        match file.try_lock_exclusive() {
            Ok(()) => break,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if !reported_wait {
                    report_progress(
                        progress,
                        format!(
                            "{} Waiting for it to finish.",
                            format_create_execution_lock_error(
                                read_create_execution_lock_metadata(&lock_path)
                            )
                        ),
                    );
                    reported_wait = true;
                }
                cancel::sleep_with_cancellation(CREATE_LOCK_WAIT_INTERVAL)?;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("Failed to acquire create lock at {}.", lock_path.display())
                });
            }
        }
    }

    let metadata = CreateExecutionLockMetadata {
        pid: std::process::id(),
        started_at: now_iso(),
        source: create_lock_source_label(options.source).to_string(),
        profile_name: options.profile_name.clone(),
        template: options.template.clone(),
        alias: options.alias.clone(),
        force: options.force,
        ignore_current: options.ignore_current,
        require_usable_quota: options.require_usable_quota,
    };
    let serialized = serde_json::to_vec_pretty(&metadata)
        .context("Failed to serialize create lock metadata.")?;
    file.set_len(0)
        .with_context(|| format!("Failed to truncate {}.", lock_path.display()))?;
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("Failed to seek {}.", lock_path.display()))?;
    file.write_all(&serialized)
        .with_context(|| format!("Failed to write {}.", lock_path.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("Failed to finalize {}.", lock_path.display()))?;
    file.flush()
        .with_context(|| format!("Failed to flush {}.", lock_path.display()))?;
    Ok(CreateExecutionLock {
        process_guard: Some(process_guard),
        file: Some(file),
        path: lock_path,
    })
}

pub fn is_create_already_in_progress_error(error: &anyhow::Error) -> bool {
    error
        .to_string()
        .starts_with(CREATE_ALREADY_IN_PROGRESS_PREFIX)
}

pub fn cmd_create(options: CreateCommandOptions) -> Result<String> {
    cmd_create_with_progress(options, None)
}

pub fn cmd_create_with_progress(
    options: CreateCommandOptions,
    progress: Option<AutomationProgressCallback>,
) -> Result<String> {
    let _lock = acquire_create_execution_lock(&options, progress.as_ref())?;
    let paths = resolve_paths()?;
    let disabled_domains = load_disabled_rotation_domains()?;
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
            &disabled_domains,
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

    let result = match execute_create_flow_with_progress(&options, progress) {
        Ok(result) => result,
        Err(error) if should_preserve_pending_on_create_error(&error) => {
            return Ok(format!(
                "{YELLOW}WARN{RESET} Account creation is environment-blocked at final add_phone. Pending account state was preserved."
            ));
        }
        Err(error) => return Err(error),
    };
    let quota_summary = summarize_quota_for_create(&result);
    if options.restore_previous_auth_after_create {
        restore_active_auth(previous_auth.as_ref())?;
        return Ok(format!(
            "{GREEN}OK{RESET} Created {} via \"{}\" from {}.\nQuota: {}\nCurrent session unchanged.",
            result.entry.label, result.profile_name, result.template, quota_summary
        ));
    }
    Ok(format!(
        "{GREEN}OK{RESET} Created {} via \"{}\" from {}.\nQuota: {}",
        result.entry.label, result.profile_name, result.template, quota_summary
    ))
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
                template: pending.stored.template,
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
                template: family_match.family.template,
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

pub(super) fn execute_create_flow_with_progress(
    options: &CreateCommandOptions,
    progress: Option<AutomationProgressCallback>,
) -> Result<CreateCommandResult> {
    let mut attempt = 1usize;
    let mut retry_reserved_emails = HashSet::new();
    loop {
        cancel::check_canceled()?;
        match execute_create_flow_attempt(options, progress.clone(), &retry_reserved_emails) {
            Ok(result) => return Ok(result),
            Err(CreateFlowAttemptFailure::Retryable {
                error,
                retry_reserved_email,
            }) if should_retry_create_after_error(options, &error) => {
                let workflow_skip = is_workflow_skip_account_error(&error);
                if let Some(email) = retry_reserved_email {
                    retry_reserved_emails.insert(normalize_email_key(&email));
                }
                if should_stop_create_retry_for_reusable_account(options)
                    && reusable_account_exists_for_auto_create_retry(options)?
                {
                    return Err(anyhow!(AUTO_CREATE_RETRY_STOPPED_FOR_REUSABLE_ACCOUNT));
                }
                report_progress(
                    progress.as_ref(),
                    format!(
                        "{} account creation attempt {attempt} failed: {error}. Retrying in {}s.",
                        if workflow_skip {
                            "Workflow-skipped"
                        } else {
                            "Automatic"
                        },
                        AUTO_CREATE_RETRY_DELAY.as_secs()
                    ),
                );
                eprintln!(
                    "{YELLOW}WARN{RESET} {} account creation attempt {attempt} failed: {error}. Retrying with a fresh account in {}s.",
                    if workflow_skip {
                        "Workflow-skipped"
                    } else {
                        "Automatic"
                    },
                    AUTO_CREATE_RETRY_DELAY.as_secs()
                );
                attempt = attempt.saturating_add(1);
                cancel::sleep_with_cancellation(AUTO_CREATE_RETRY_DELAY)?;
            }
            Err(CreateFlowAttemptFailure::Fatal(error))
                if should_preserve_pending_on_create_error(&error) =>
            {
                return Err(error);
            }
            Err(CreateFlowAttemptFailure::Fatal(error))
                if should_retry_create_until_usable(options) =>
            {
                if should_stop_create_retry_for_reusable_account(options)
                    && reusable_account_exists_for_auto_create_retry(options)?
                {
                    return Err(anyhow!(AUTO_CREATE_RETRY_STOPPED_FOR_REUSABLE_ACCOUNT));
                }
                report_progress(
                    progress.as_ref(),
                    format!(
                        "Automatic account creation attempt {attempt} failed: {error}. Retrying in {}s.",
                        AUTO_CREATE_RETRY_DELAY.as_secs()
                    ),
                );
                eprintln!(
                    "{YELLOW}WARN{RESET} Automatic account creation attempt {attempt} failed: {error}. Retrying with a fresh account in {}s.",
                    AUTO_CREATE_RETRY_DELAY.as_secs()
                );
                attempt = attempt.saturating_add(1);
                cancel::sleep_with_cancellation(AUTO_CREATE_RETRY_DELAY)?;
            }
            Err(CreateFlowAttemptFailure::Retryable { error, .. })
            | Err(CreateFlowAttemptFailure::Fatal(error)) => return Err(error),
        }
    }
}

pub(super) fn report_progress(
    progress: Option<&AutomationProgressCallback>,
    message: impl Into<String>,
) {
    if let Some(progress) = progress {
        progress(message.into());
    }
}

pub(super) fn fatal<T>(result: Result<T>) -> std::result::Result<T, CreateFlowAttemptFailure> {
    result.map_err(CreateFlowAttemptFailure::Fatal)
}

pub(super) fn should_preserve_pending_on_create_error(error: &anyhow::Error) -> bool {
    is_final_add_phone_environment_blocker_error(error)
}

pub(super) fn should_retry_create_until_usable(options: &CreateCommandOptions) -> bool {
    options.require_usable_quota && matches!(options.source, CreateCommandSource::Next)
}

pub(super) fn should_retry_create_after_error(
    options: &CreateCommandOptions,
    error: &anyhow::Error,
) -> bool {
    should_retry_create_until_usable(options)
        || is_workflow_skip_account_error(error)
        || is_missing_account_login_ref_error(error)
}

pub(super) fn should_stop_create_retry_for_reusable_account(
    options: &CreateCommandOptions,
) -> bool {
    matches!(options.source, CreateCommandSource::Next)
}

pub fn is_auto_create_retry_stopped_for_reusable_account(error: &anyhow::Error) -> bool {
    error
        .to_string()
        .contains(AUTO_CREATE_RETRY_STOPPED_FOR_REUSABLE_ACCOUNT)
}

pub(super) fn reusable_account_exists_for_auto_create_retry(
    options: &CreateCommandOptions,
) -> Result<bool> {
    let paths = resolve_paths()?;
    let disabled_domains = load_disabled_rotation_domains()?;
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
        &disabled_domains,
    )?;
    if candidate_dirty {
        save_pool(&pool)?;
    }
    Ok(candidate.is_some())
}

pub(super) fn skip_pending_account_and_advance_family(
    store: &mut CredentialStore,
    family_key: &str,
    profile_name: &str,
    template: &str,
    suffix: u32,
    created_email: &str,
    started_at: &str,
) -> Result<()> {
    let normalized_email = normalize_email_key(created_email);
    if !store.pending.contains_key(&normalized_email) {
        return Ok(());
    }

    store.pending.remove(&normalized_email);
    let max_skipped_slots = max_skipped_slots_for_family(store.families.get(family_key));
    let mut existing_family_skips =
        collect_skipped_account_emails_for_family(store, profile_name, template);
    existing_family_skips.retain(|email| normalize_email_key(email) != normalized_email);
    existing_family_skips.sort_by_key(|email| {
        extract_account_family_suffix(email, template)
            .ok()
            .flatten()
            .unwrap_or(0)
    });
    let existing_skip_count = existing_family_skips.len() as u32;
    if max_skipped_slots > 0 && existing_skip_count >= max_skipped_slots {
        let to_remove =
            existing_skip_count.saturating_sub(max_skipped_slots.saturating_sub(1)) as usize;
        for email in existing_family_skips.into_iter().take(to_remove) {
            store.skipped.remove(&normalize_email_key(&email));
        }
    }
    store.skipped.insert(normalized_email);
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
                template: template.to_string(),
                next_suffix,
                max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
                created_at: started_at.to_string(),
                updated_at,
                last_created_email: None,
                relogin: Vec::new(),
                suspend_domain_on_terminal_refresh_failure: false,
            },
        );
    }

    save_credential_store(store)
}

pub(super) fn prefer_signup_recovery_for_create(reusing_pending: bool) -> bool {
    !reusing_pending
}

pub(super) fn execute_create_flow_attempt(
    options: &CreateCommandOptions,
    progress: Option<AutomationProgressCallback>,
    retry_reserved_emails: &HashSet<String>,
) -> std::result::Result<CreateCommandResult, CreateFlowAttemptFailure> {
    let paths = fatal(resolve_paths())?;
    let previous_auth = fatal(load_codex_auth_if_exists())?;
    let mut store = fatal(load_credential_store())?;
    let workflow_file = resolve_account_flow_file_for_create(&paths, options);
    let workflow_file_display = workflow_file.display().to_string();
    let workflow_metadata = fatal(read_workflow_file_metadata(&workflow_file))?;
    let login_workflow_defaults = fatal(resolve_login_workflow_defaults(None))?;
    let profile_name = fatal(resolve_managed_profile_name(
        options.profile_name.as_deref(),
        workflow_metadata.preferred_profile_name.as_deref(),
        Some(workflow_file_display.as_str()),
    ))?;
    let template = fatal(resolve_create_template_for_profile(
        &store,
        &profile_name,
        options.template.as_deref(),
        options.alias.as_deref(),
    ))?;
    fatal(ensure_rotation_enabled_for_template_in_store(
        &store, &template,
    ))?;

    let pool = fatal(load_pool())?;
    let family_key = fatal(make_credential_family_key(&profile_name, &template))?;
    let family = store.families.get(&family_key).cloned();
    let started_at = now_iso();
    let known_emails = collect_known_account_emails(&pool, &store);
    let skipped_emails =
        collect_skipped_account_emails_for_family(&store, &profile_name, &template);
    let existing_pending = select_pending_credential_for_family(
        &store,
        &profile_name,
        &template,
        options.alias.as_deref(),
        retry_reserved_emails,
    );
    let reusing_pending = existing_pending.is_some();
    let suffix = match existing_pending.as_ref() {
        Some(entry) => entry.stored.suffix,
        None => fatal(compute_create_attempt_family_suffix(
            family.as_ref(),
            &template,
            known_emails,
            skipped_emails,
            retry_reserved_emails,
        ))?,
    };
    fatal(ensure_suffix_within_domain_limit_in_store(
        &store, &template, suffix,
    ))?;
    let created_email = existing_pending
        .as_ref()
        .map(|entry| entry.stored.email.clone())
        .unwrap_or_else(|| build_account_family_email(&template, suffix).unwrap_or_default());
    let existing_pending = existing_pending.unwrap_or_else(|| PendingCredential {
        stored: StoredCredential {
            email: created_email.clone(),
            profile_name: profile_name.clone(),
            template: template.clone(),
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
    let birth_date = resolve_credential_birth_date(
        Some(&existing_pending.stored),
        workflow_metadata.default_birth_date().as_ref(),
    )
    .unwrap_or_else(|| login_workflow_defaults.birth_date.clone());
    report_progress(
        progress.as_ref(),
        if reusing_pending {
            format!(
                "Reusing pending account {} via {}.",
                created_email, profile_name
            )
        } else {
            format!(
                "Creating {} via {} from {}.",
                created_email, profile_name, template
            )
        },
    );
    if previous_auth
        .as_ref()
        .map(|auth| auth_matches_target_email(auth, &created_email))
        .unwrap_or(false)
    {
        report_progress(
            progress.as_ref(),
            format!(
                "{} is already the active Codex auth. Finalizing.",
                created_email
            ),
        );
        let auth = previous_auth
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("Current Codex auth disappeared before create could finish."))
            .map_err(CreateFlowAttemptFailure::Fatal)?;
        let result = finalize_created_account(FinalizeCreatedAccountArgs {
            store: &mut store,
            family: family.as_ref(),
            family_key: &family_key,
            profile_name: &profile_name,
            template: &template,
            suffix,
            pending: &PendingCredential {
                stored: StoredCredential {
                    email: created_email.clone(),
                    profile_name: profile_name.clone(),
                    template: template.clone(),
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
            auth: &auth,
            started_at: started_at.as_str(),
            previous_auth: previous_auth.as_ref(),
            progress: progress.clone(),
        });
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                if should_retry_create_until_usable(options) {
                    fatal(restore_active_auth(previous_auth.as_ref()))?;
                    fatal(skip_pending_account_and_advance_family(
                        &mut store,
                        &family_key,
                        &profile_name,
                        &template,
                        suffix,
                        &created_email,
                        started_at.as_str(),
                    ))?;
                    return Err(CreateFlowAttemptFailure::Retryable {
                        error,
                        retry_reserved_email: Some(created_email.clone()),
                    });
                }
                return Err(CreateFlowAttemptFailure::Fatal(error));
            }
        };
        if options.restore_previous_auth_after_create {
            fatal(restore_active_auth(previous_auth.as_ref()))?;
        }
        return Ok(result);
    }
    let mut account_login_locator = Some(build_openai_account_login_locator(&created_email));
    let mut skip_locator_preflight = false;
    let mut generated_password: Option<String> = None;
    if !reusing_pending {
        report_progress(
            progress.as_ref(),
            format!("Preparing password for {}.", created_email),
        );
        let password = generate_password(18);
        let persona_paths =
            current_persona_managed_profile_dir(true).map_err(CreateFlowAttemptFailure::Fatal)?;
        let profile_dir = persona_paths.as_ref().map(|path| path.to_string_lossy());

        match run_automation_bridge::<_, CodexRotateSecretRef>(
            "prepare-account-secret-ref",
            BridgeEnsureSecretPayload {
                profile_name: &profile_name,
                profile_dir: profile_dir.as_ref().map(|s| s.as_ref()),
                email: &created_email,
                password: password.as_str(),
            },
        ) {
            Ok(_) => {
                skip_locator_preflight = true;
            }
            Err(error) if is_optional_account_secret_prepare_error(&error) => {
                report_progress(
                    progress.as_ref(),
                    format!(
                        "{YELLOW}WARN{RESET} Bitwarden is unavailable for {}. Continuing with the generated signup password without storing a vault secret.",
                        created_email
                    ),
                );
                account_login_locator = None;
            }
            Err(error) => return Err(CreateFlowAttemptFailure::Fatal(error)),
        }
        generated_password = Some(password);
    }
    let pending = PendingCredential {
        stored: StoredCredential {
            email: created_email.clone(),
            profile_name: profile_name.clone(),
            template: template.clone(),
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

    let persona_profile = load_pool()
        .map_err(CreateFlowAttemptFailure::Fatal)?
        .accounts
        .last()
        .and_then(|entry| {
            let persona = entry.persona.as_ref()?;
            resolve_persona_profile(
                persona.persona_profile_id.as_deref()?,
                persona.browser_fingerprint.clone(),
            )
        });

    report_progress(
        progress.as_ref(),
        format!("Starting managed login for {}.", created_email),
    );
    let login_result = run_complete_codex_login(CompleteCodexLoginArgs {
        profile_name: &profile_name,
        email: &created_email,
        account_login_locator: account_login_locator.as_ref(),
        workflow_ref: workflow_metadata.workflow_ref.as_deref(),
        codex_bin: Some(codex_bin().as_str()),
        workflow_run_stamp: Some(started_at.as_str()),
        skip_locator_preflight: Some(skip_locator_preflight),
        prefer_signup_recovery: Some(prefer_signup_recovery_for_create(reusing_pending)),
        prefer_password_login: skip_locator_preflight.then_some(true),
        password: generated_password.as_deref(),
        treat_final_add_phone_as_environment_blocker: Some(true),
        birth_date: Some(&birth_date),
        persona_profile,
        progress: progress.clone(),
    });
    let login_outcome = match login_result {
        Ok(value) => value,
        Err(error) => {
            fatal(restore_active_auth(previous_auth.as_ref()))?;
            if should_preserve_pending_on_create_error(&error) {
                fatal(save_credential_store(&store))?;
                return Err(CreateFlowAttemptFailure::Fatal(error));
            }
            if should_retry_create_after_error(options, &error) {
                fatal(skip_pending_account_and_advance_family(
                    &mut store,
                    &family_key,
                    &profile_name,
                    &template,
                    suffix,
                    &created_email,
                    started_at.as_str(),
                ))?;
                return Err(CreateFlowAttemptFailure::Retryable {
                    error,
                    retry_reserved_email: Some(created_email.clone()),
                });
            }
            fatal(save_credential_store(&store))?;
            return Err(CreateFlowAttemptFailure::Fatal(error));
        }
    };

    if let Some(fingerprint) = login_outcome.browser_fingerprint.clone() {
        let mut pool = fatal(load_pool())?;
        if let Some(entry) = pool.accounts.last_mut() {
            if let Some(persona) = entry.persona.as_mut() {
                persona.browser_fingerprint = Some(fingerprint);
            }
        }
        fatal(save_pool(&pool))?;
    }

    let auth = fatal(load_auth_for_completed_login(&login_outcome))?;
    let logged_in_email = summarize_codex_auth(&auth).email;
    if normalize_email_key(&logged_in_email) != normalize_email_key(&created_email)
        && !workflow_verified_expected_email(
            login_outcome.verified_account_email.as_deref(),
            &created_email,
        )
    {
        let error = anyhow!(
            "Expected {}, but Codex logged into {}.",
            created_email,
            logged_in_email
        );
        fatal(restore_active_auth(previous_auth.as_ref()))?;
        if should_retry_create_until_usable(options) {
            fatal(skip_pending_account_and_advance_family(
                &mut store,
                &family_key,
                &profile_name,
                &template,
                suffix,
                &created_email,
                started_at.as_str(),
            ))?;
            return Err(CreateFlowAttemptFailure::Retryable {
                error,
                retry_reserved_email: Some(created_email.clone()),
            });
        }
        fatal(save_credential_store(&store))?;
        return Err(CreateFlowAttemptFailure::Fatal(error));
    }

    report_progress(
        progress.as_ref(),
        format!("Managed login finished for {}. Finalizing.", created_email),
    );
    let result = finalize_created_account(FinalizeCreatedAccountArgs {
        store: &mut store,
        family: family.as_ref(),
        family_key: &family_key,
        profile_name: &profile_name,
        template: &template,
        suffix,
        pending: &pending,
        options,
        auth: &auth,
        started_at: started_at.as_str(),
        previous_auth: previous_auth.as_ref(),
        progress: progress.clone(),
    });
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            if should_retry_create_until_usable(options) {
                fatal(restore_active_auth(previous_auth.as_ref()))?;
                fatal(skip_pending_account_and_advance_family(
                    &mut store,
                    &family_key,
                    &profile_name,
                    &template,
                    suffix,
                    &created_email,
                    started_at.as_str(),
                ))?;
                return Err(CreateFlowAttemptFailure::Retryable {
                    error,
                    retry_reserved_email: Some(created_email.clone()),
                });
            }
            return Err(CreateFlowAttemptFailure::Fatal(error));
        }
    };

    if options.restore_previous_auth_after_create {
        fatal(restore_active_auth(previous_auth.as_ref()))?;
    }

    Ok(result)
}

pub(super) fn auth_matches_target_email(auth: &CodexAuth, target_email: &str) -> bool {
    normalize_email_key(&summarize_codex_auth(auth).email) == normalize_email_key(target_email)
}

pub(super) struct FinalizeCreatedAccountArgs<'a> {
    store: &'a mut CredentialStore,
    family: Option<&'a CredentialFamily>,
    family_key: &'a str,
    profile_name: &'a str,
    template: &'a str,
    suffix: u32,
    pending: &'a PendingCredential,
    options: &'a CreateCommandOptions,
    auth: &'a CodexAuth,
    started_at: &'a str,
    previous_auth: Option<&'a CodexAuth>,
    progress: Option<AutomationProgressCallback>,
}

pub(super) fn finalize_created_account(
    args: FinalizeCreatedAccountArgs<'_>,
) -> Result<CreateCommandResult> {
    let FinalizeCreatedAccountArgs {
        store,
        family,
        family_key,
        profile_name,
        template,
        suffix,
        pending,
        options,
        auth,
        started_at,
        previous_auth,
        progress,
    } = args;
    let created_email = pending.stored.email.clone();
    report_progress(
        progress.as_ref(),
        format!("Adding {} to the account pool.", created_email),
    );
    let _ = cmd_add_expected_email(&created_email, options.alias.as_deref())?;
    report_progress(
        progress.as_ref(),
        format!("Inspecting quota for {}.", created_email),
    );
    let inspected = inspect_pool_entry_for_created_email(
        &extract_account_id_from_auth(auth),
        &created_email,
        &summarize_codex_auth(auth).plan_type,
    )?
    .ok_or_else(|| {
        anyhow!(
            "Created {}, but could not find the new account in the pool after login.",
            created_email
        )
    })?;

    let updated_at = now_iso();
    store.pending.remove(&normalize_email_key(&created_email));
    store.skipped.remove(&normalize_email_key(&created_email));
    upsert_family_for_account(
        store,
        &StoredCredential {
            email: created_email.clone(),
            profile_name: profile_name.to_string(),
            template: template.to_string(),
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
            template: template.to_string(),
            next_suffix: family
                .map(|entry| entry.next_suffix.max(suffix + 1))
                .unwrap_or(suffix + 1),
            max_skipped_slots: family
                .map(|entry| entry.max_skipped_slots)
                .unwrap_or(DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY),
            created_at: family
                .map(|entry| entry.created_at.clone())
                .unwrap_or_else(|| started_at.to_string()),
            updated_at,
            last_created_email: Some(created_email.clone()),
            relogin: family
                .map(|entry| entry.relogin.clone())
                .unwrap_or_default(),
            suspend_domain_on_terminal_refresh_failure: family
                .map(|entry| entry.suspend_domain_on_terminal_refresh_failure)
                .unwrap_or(false),
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

    report_progress(
        progress.as_ref(),
        format!("Created {} with usable quota.", inspected.entry.label),
    );

    Ok(CreateCommandResult {
        entry: inspected.entry,
        inspection: Some(inspected.inspection),
        profile_name: profile_name.to_string(),
        template: template.to_string(),
    })
}

pub(super) fn summarize_quota_for_create(result: &CreateCommandResult) -> String {
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

pub(super) fn find_created_pool_entry_index(
    pool: &Pool,
    account_id: &str,
    expected_email: &str,
    expected_plan: &str,
) -> Option<usize> {
    let normalized_expected_email = normalize_email_key(expected_email);
    let normalized_expected_plan = normalize_created_pool_plan_type(expected_plan);
    if !normalized_expected_email.is_empty() && normalized_expected_email != "unknown" {
        if let Some(index) = pool.accounts.iter().position(|entry| {
            normalize_email_key(entry.email.as_str()) == normalized_expected_email
                && normalize_created_pool_plan_type(&entry.plan_type) == normalized_expected_plan
        }) {
            return Some(index);
        }
    }

    pool.accounts.iter().position(|entry| {
        (entry.account_id == account_id || entry.auth.tokens.account_id == account_id)
            && normalize_created_pool_plan_type(&entry.plan_type) == normalized_expected_plan
    })
}

pub(super) fn normalize_created_pool_plan_type(plan_type: &str) -> String {
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

pub(super) fn inspect_pool_entry_for_created_email(
    account_id: &str,
    expected_email: &str,
    expected_plan: &str,
) -> Result<Option<InspectedPoolEntry>> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let index = find_created_pool_entry_index(&pool, account_id, expected_email, expected_plan);
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

pub(super) fn inspect_pool_entry_by_account_id(
    account_id: &str,
) -> Result<Option<InspectedPoolEntry>> {
    inspect_pool_entry_for_created_email(account_id, "", "")
}

pub(super) struct InspectedPoolEntry {
    pub(super) entry: AccountEntry,
    pub(super) inspection: AccountInspection,
}
