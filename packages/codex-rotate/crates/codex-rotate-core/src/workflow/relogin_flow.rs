use super::*;

pub fn cmd_relogin(selector: &str, options: ReloginOptions) -> Result<String> {
    cmd_relogin_with_progress(selector, options, None)
}

pub fn cmd_relogin_with_progress(
    selector: &str,
    options: ReloginOptions,
    progress: Option<AutomationProgressCallback>,
) -> Result<String> {
    let mut store = load_credential_store()?;
    let selection = {
        let pool = load_pool()?;
        resolve_relogin_target(&pool, &store, selector)?
    };
    let pending = selection.pending.clone();
    let existing = selection.as_ref().map(|selection| selection.entry.clone());
    let expected_email = existing
        .as_ref()
        .map(|entry| entry.email.clone())
        .or_else(|| pending.as_ref().map(|pending| pending.stored.email.clone()))
        .ok_or_else(|| {
            anyhow!(
                "Account \"{}\" not found in pool or pending state.",
                selector
            )
        })?;
    let existing_alias = existing
        .as_ref()
        .and_then(|entry| entry.alias.clone())
        .or_else(|| {
            pending
                .as_ref()
                .and_then(|pending| pending.stored.alias.clone())
        });
    let display_summary = existing
        .as_ref()
        .map(format_account_summary_for_display)
        .unwrap_or_else(|| expected_email.clone());
    reconcile_pending_relogin_target(&mut store, pending.as_ref())?;
    ensure_rotation_enabled_for_email_in_store(&store, &expected_email)?;
    let resolved_stored_credential = existing
        .as_ref()
        .and_then(|entry| resolve_relogin_credential(&store, entry))
        .or_else(|| pending.as_ref().map(|pending| pending.stored.clone()));
    let relogin_birth_date = relogin_birth_date_for_pending(pending.as_ref());
    let prefer_signup_recovery = Some(prefer_signup_recovery_for_relogin(
        pending.as_ref(),
        existing
            .as_ref()
            .and_then(|entry| resolve_relogin_credential(&store, entry))
            .as_ref(),
    ));
    let mut prepared_account_login_locator: Option<CodexRotateSecretLocator> = None;
    let mut prepared_skip_locator_preflight = false;
    let mut prepared_password: Option<String> = None;

    if should_prepare_signup_recovery_password(
        pending.as_ref(),
        prefer_signup_recovery == Some(true),
        options.manual_login,
    ) {
        let profile_name = pending
            .as_ref()
            .map(|record| record.stored.profile_name.as_str())
            .ok_or_else(|| {
                anyhow!("Signup-recovery relogin is missing a profile for {expected_email}.")
            })?;
        report_progress(
            progress.as_ref(),
            format!("Preparing password for {}.", expected_email),
        );
        let generated_password = generate_password(18);
        let persona_paths = current_persona_managed_profile_dir(true)?;
        let profile_dir = persona_paths.as_ref().map(|path| path.to_string_lossy());

        match run_automation_bridge::<_, CodexRotateSecretRef>(
            "prepare-account-secret-ref",
            BridgeEnsureSecretPayload {
                profile_name,
                profile_dir: profile_dir.as_ref().map(|s| s.as_ref()),
                email: &expected_email,
                password: generated_password.as_str(),
            },
        ) {
            Ok(_) => {
                prepared_account_login_locator =
                    Some(build_openai_account_login_locator(&expected_email));
                prepared_skip_locator_preflight = true;
            }
            Err(error) if is_optional_account_secret_prepare_error(&error) => {
                report_progress(
                    progress.as_ref(),
                    format!(
                        "{YELLOW}WARN{RESET} Bitwarden is unavailable for {}. Continuing with the generated recovery password without storing a vault secret.",
                        expected_email
                    ),
                );
            }
            Err(error) => return Err(error),
        }
        prepared_password = Some(generated_password);
    }

    let stored_credential = resolved_stored_credential;

    if prepared_account_login_locator.is_some()
        || should_use_stored_credential_relogin(stored_credential.as_ref(), &options)
    {
        let stored_credential = stored_credential
            .or_else(|| pending.as_ref().map(|pending| pending.stored.clone()))
            .ok_or_else(|| {
                anyhow!("Stored credential lookup unexpectedly failed for {expected_email}.")
            })?;
        let mut updated_stored = stored_credential.clone();
        updated_stored.updated_at = now_iso();
        let account_login_locator = prepared_account_login_locator
            .clone()
            .unwrap_or_else(|| build_openai_account_login_locator(&updated_stored.email));

        let persona_profile = existing.as_ref().and_then(|entry| {
            let persona = entry.persona.as_ref()?;
            resolve_persona_profile(
                persona.persona_profile_id.as_deref()?,
                persona.browser_fingerprint.clone(),
            )
        });

        let previous_auth = load_codex_auth_if_exists()?;
        let login_result = (|| -> Result<CompleteCodexLoginOutcome> {
            if should_logout_before_stored_relogin(&options) {
                let paths = resolve_paths()?;
                if paths.codex_auth_file.exists() {
                    run_codex_command(["logout"])?;
                }
            }

            run_complete_codex_login(CompleteCodexLoginArgs {
                profile_name: &updated_stored.profile_name,
                email: &updated_stored.email,
                account_login_locator: Some(&account_login_locator),
                workflow_ref: None,
                codex_bin: None,
                workflow_run_stamp: None,
                skip_locator_preflight: prepared_skip_locator_preflight.then_some(true),
                prefer_signup_recovery,
                prefer_password_login: prepared_skip_locator_preflight.then_some(true),
                password: prepared_password.as_deref(),
                treat_final_add_phone_as_environment_blocker: Some(true),
                birth_date: relogin_birth_date.as_ref(),
                persona_profile,
                progress: progress.clone(),
            })
        })();
        let login_outcome = match login_result {
            Ok(value) => value,
            Err(error) => {
                if is_final_add_phone_environment_blocker_error(&error) {
                    reconcile_pending_relogin_target(&mut store, pending.as_ref())?;
                    return Ok(format!(
                        "{YELLOW}WARN{RESET} Re-login for {} is environment-blocked at final add_phone. Pending account state was preserved.",
                        display_summary
                    ));
                }
                restore_active_auth_after_relogin(previous_auth.as_ref())?;
                if is_workflow_skip_account_error(&error)
                    || is_missing_account_login_ref_error(&error)
                {
                    if let Some(pending) = pending.as_ref() {
                        let family_key = make_credential_family_key(
                            &pending.stored.profile_name,
                            &pending.stored.template,
                        )?;
                        skip_pending_account_and_advance_family(
                            &mut store,
                            &family_key,
                            &pending.stored.profile_name,
                            &pending.stored.template,
                            pending.stored.suffix,
                            &pending.stored.email,
                            pending
                                .started_at
                                .as_deref()
                                .unwrap_or(pending.stored.created_at.as_str()),
                        )?;
                    } else {
                        reconcile_pending_relogin_target(&mut store, pending.as_ref())?;
                    }
                } else {
                    reconcile_pending_relogin_target(&mut store, pending.as_ref())?;
                }
                return Err(error);
            }
        };

        let auth = load_auth_for_completed_login(&login_outcome)?;
        let logged_in_email = summarize_codex_auth(&auth).email;
        if !options.allow_email_change
            && normalize_email_key(&logged_in_email) != normalize_email_key(&expected_email)
            && !workflow_verified_expected_email(
                login_outcome.verified_account_email.as_deref(),
                &expected_email,
            )
        {
            restore_active_auth_after_relogin(previous_auth.as_ref())?;
            return Err(anyhow!(
                "Expected {}, but Codex logged into {}.",
                expected_email,
                logged_in_email
            ));
        }

        let _ = cmd_add_expected_email(&expected_email, existing_alias.as_deref())?;
        if let Some(fingerprint) = login_outcome.browser_fingerprint {
            let mut pool = load_pool()?;
            let account_id = extract_account_id_from_auth(&auth);
            if let Some(entry) = pool
                .accounts
                .iter_mut()
                .find(|a| a.account_id == account_id)
            {
                if let Some(persona) = entry.persona.as_mut() {
                    persona.browser_fingerprint = Some(fingerprint);
                    save_pool(&pool)?;
                }
            }
        }

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
                        .or_else(|| existing_alias.clone()),
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
            display_summary
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

    report_progress(
        progress.as_ref(),
        format!("Opening Codex login flow for {expected_email}."),
    );
    run_codex_command(["login"])?;

    let auth = load_current_auth()?;
    let logged_in_email = summarize_codex_auth(&auth).email;
    if normalize_email_key(&logged_in_email) != normalize_email_key(&expected_email)
        && !options.allow_email_change
    {
        return Err(anyhow!(
            "Logged into {}, but \"{}\" expects {}. The pool was not updated. Re-run with --allow-email-change if you want to replace it.",
            logged_in_email,
            display_summary,
            expected_email
        ));
    }

    cmd_add_expected_email(&expected_email, existing_alias.as_deref())
}

pub(super) fn resolve_relogin_target(
    pool: &Pool,
    store: &CredentialStore,
    selector: &str,
) -> Result<ReloginTargetSelection> {
    if let Ok(selection) = resolve_account_selector(pool, selector) {
        return Ok(ReloginTargetSelection {
            selection: Some(selection),
            pending: None,
        });
    }

    let pending = store
        .pending
        .get(&normalize_email_key(selector))
        .cloned()
        .or_else(|| synthesize_pending_relogin_target(store, selector))
        .ok_or_else(|| {
            anyhow!(
                "Account \"{}\" not found in pool or pending state.",
                selector
            )
        })?;

    Ok(ReloginTargetSelection {
        selection: None,
        pending: Some(pending),
    })
}

pub(super) fn synthesize_pending_relogin_target(
    store: &CredentialStore,
    selector: &str,
) -> Option<PendingCredential> {
    let family_match = select_family_for_account_email(store, selector)?;
    let birth_date = resolve_login_workflow_defaults(None)
        .ok()
        .map(|defaults| defaults.birth_date);
    let now = now_iso();

    Some(PendingCredential {
        stored: StoredCredential {
            email: normalize_email_key(selector),
            profile_name: family_match.family.profile_name,
            template: family_match.family.template,
            suffix: family_match.suffix,
            selector: None,
            alias: None,
            birth_month: birth_date.as_ref().map(|value| value.birth_month),
            birth_day: birth_date.as_ref().map(|value| value.birth_day),
            birth_year: birth_date.as_ref().map(|value| value.birth_year),
            created_at: family_match.family.created_at,
            updated_at: now.clone(),
        },
        started_at: Some(now),
    })
}

pub(super) struct ReloginTargetSelection {
    pub(super) selection: Option<crate::pool::AccountSelection>,
    pub(super) pending: Option<PendingCredential>,
}

impl ReloginTargetSelection {
    fn as_ref(&self) -> Option<&crate::pool::AccountSelection> {
        self.selection.as_ref()
    }
}

pub fn should_use_stored_credential_relogin(
    stored_credential: Option<&StoredCredential>,
    options: &ReloginOptions,
) -> bool {
    stored_credential.is_some() && !options.manual_login
}

pub(super) fn should_logout_before_stored_relogin(options: &ReloginOptions) -> bool {
    options.logout_first
}

pub(super) fn prefer_signup_recovery_for_relogin(
    pending: Option<&PendingCredential>,
    stored_credential: Option<&StoredCredential>,
) -> bool {
    pending.is_some() && stored_credential.is_none()
}

pub(super) fn should_prepare_signup_recovery_password(
    pending: Option<&PendingCredential>,
    prefer_signup_recovery: bool,
    manual_login: bool,
) -> bool {
    prefer_signup_recovery && pending.is_some() && !manual_login
}

pub(super) fn relogin_birth_date_for_pending(
    pending: Option<&PendingCredential>,
) -> Option<AdultBirthDate> {
    pending.and_then(|pending| resolve_credential_birth_date(Some(&pending.stored), None))
}

pub(super) fn ensure_pending_relogin_target(
    store: &mut CredentialStore,
    pending: Option<&PendingCredential>,
) -> bool {
    let Some(pending) = pending else {
        return false;
    };
    let normalized_email = normalize_email_key(&pending.stored.email);
    let skipped_before = store.skipped.len();
    store
        .skipped
        .retain(|email| normalize_email_key(email) != normalized_email);
    let removed_skipped = store.skipped.len() != skipped_before;
    if store.pending.contains_key(&normalized_email) {
        return removed_skipped;
    }
    store.pending.insert(normalized_email, pending.clone());
    true
}

pub(super) fn reconcile_pending_relogin_target(
    store: &mut CredentialStore,
    pending: Option<&PendingCredential>,
) -> Result<()> {
    if ensure_pending_relogin_target(store, pending) {
        save_credential_store(store)?;
    }
    Ok(())
}
