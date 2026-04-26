use super::*;

// Translated recovery events point at target-local threads, so the source
// account log cursor is not meaningful on the target side.
const TRANSLATED_RECOVERY_SOURCE_LOG_ID: i64 = 0;

pub(super) fn ensure_no_rotation_drift(prepared: &PreparedRotation) -> Result<()> {
    let pool = load_pool()?;
    if pool.active_index != prepared.previous_index {
        return Err(anyhow!(
            "Rotation aborted: pool drift detected (active_index changed from {} to {}).",
            prepared.previous_index,
            pool.active_index
        ));
    }
    Ok(())
}

pub(super) fn ensure_target_account_still_valid(prepared: &PreparedRotation) -> Result<()> {
    ensure_target_account_still_present(prepared)?;

    let disabled_domains = codex_rotate_core::workflow::load_disabled_rotation_domains()?;
    let domain = codex_rotate_core::workflow::extract_email_domain(&prepared.target.email)
        .unwrap_or_default();
    if disabled_domains.contains(&domain) {
        return Err(anyhow!(
            "Target account {} is in a disabled domain and cannot be activated.",
            prepared.target.label
        ));
    }
    Ok(())
}

pub(super) fn ensure_target_account_still_present(prepared: &PreparedRotation) -> Result<()> {
    let pool = load_pool()?;
    let target_sync_identity = conversation_sync_identity(&prepared.target);
    if !pool
        .accounts
        .iter()
        .any(|a| conversation_sync_identity(a) == target_sync_identity)
    {
        return Err(anyhow!(
            "Target account {} was removed mid-flow.",
            prepared.target.label
        ));
    }
    Ok(())
}

pub(super) fn host_rotation_checkpointing_enabled() -> Result<bool> {
    Ok(matches!(current_environment()?, RotationEnvironment::Host))
}

pub(super) fn rotation_checkpoint_for_prepared(
    prepared: &PreparedRotation,
    phase: RotationCheckpointPhase,
) -> RotationCheckpoint {
    RotationCheckpoint {
        phase,
        previous_index: prepared.previous_index,
        target_index: prepared.target_index,
        previous_account_id: prepared.previous.account_id.clone(),
        target_account_id: prepared.target.account_id.clone(),
    }
}

pub(super) fn save_rotation_checkpoint_for_prepared(
    prepared: &PreparedRotation,
    phase: RotationCheckpointPhase,
) -> Result<()> {
    if host_rotation_checkpointing_enabled()? {
        save_rotation_checkpoint(Some(&rotation_checkpoint_for_prepared(prepared, phase)))?;
    }
    Ok(())
}

pub(super) fn clear_rotation_checkpoint() -> Result<()> {
    save_rotation_checkpoint(None)
}

pub(super) fn resolve_checkpoint_account_index(
    pool: &codex_rotate_core::pool::Pool,
    account_id: &str,
    fallback_index: usize,
    role: &str,
) -> Result<usize> {
    if fallback_index < pool.accounts.len()
        && pool.accounts[fallback_index].account_id == account_id
    {
        return Ok(fallback_index);
    }

    if let Some(index) = pool
        .accounts
        .iter()
        .position(|entry| entry.account_id == account_id)
    {
        return Ok(index);
    }

    if fallback_index < pool.accounts.len() {
        return Ok(fallback_index);
    }

    if role == "target" && !pool.accounts.is_empty() {
        return Ok(pool.active_index.min(pool.accounts.len().saturating_sub(1)));
    }

    Err(anyhow!(
        "Unable to resolve the {role} account for an interrupted rotation."
    ))
}

pub(super) fn live_root_matches_persona(
    paths: &RuntimePaths,
    entry: &AccountEntry,
) -> Result<bool> {
    let persona = entry
        .persona
        .as_ref()
        .ok_or_else(|| anyhow!("Account {} is missing persona metadata.", entry.label))?;
    let persona_paths = host_persona_paths(paths, persona)?;
    Ok(is_symlink_to(&paths.codex_home, &persona_paths.codex_home)?
        && is_symlink_to(
            &paths.codex_app_support_dir,
            &persona_paths.codex_app_support_dir,
        )?
        && is_symlink_to(&paths.debug_profile_dir, &persona_paths.debug_profile_dir)?)
}

pub(super) fn recover_incomplete_rotation_state_without_lock() -> Result<()> {
    let Some(checkpoint) = load_rotation_checkpoint()? else {
        return Ok(());
    };

    let paths = resolve_paths()?;
    let _ = sync_pool_active_account_from_current_auth();
    let pool = load_pool()?;
    if pool.accounts.is_empty() {
        clear_rotation_checkpoint()?;
        return Ok(());
    }

    let previous_index = resolve_checkpoint_account_index(
        &pool,
        &checkpoint.previous_account_id,
        checkpoint.previous_index,
        "previous",
    )?;
    let target_index = resolve_checkpoint_account_index(
        &pool,
        &checkpoint.target_account_id,
        checkpoint.target_index,
        "target",
    )?;

    if previous_index == target_index {
        clear_rotation_checkpoint()?;
        return Ok(());
    }

    let previous = pool.accounts[previous_index].clone();
    let target = pool.accounts[target_index].clone();
    let target_is_authoritative = match checkpoint.phase {
        RotationCheckpointPhase::Prepare
        | RotationCheckpointPhase::Export
        | RotationCheckpointPhase::Rollback => false,
        RotationCheckpointPhase::Activate => live_root_matches_persona(&paths, &target)?,
        RotationCheckpointPhase::Import | RotationCheckpointPhase::Commit => true,
    };

    if target_is_authoritative {
        switch_host_persona(&paths, &previous, &target, false)?;
        write_selected_account_auth(&target)?;
        restore_pool_active_index(target_index)?;
    } else {
        switch_host_persona(&paths, &target, &previous, false)?;
        write_selected_account_auth(&previous)?;
        restore_pool_active_index(previous_index)?;
    }

    clear_rotation_checkpoint()?;
    Ok(())
}

pub(crate) fn recover_incomplete_rotation_state() -> Result<()> {
    let _lock = RotationLock::acquire()?;
    recover_incomplete_rotation_state_without_lock()
}

pub(super) fn capture_host_source_thread_candidates(port: u16) -> Result<Vec<String>> {
    let paths = resolve_paths()?;
    if !managed_codex_is_running(&paths.debug_profile_dir)? {
        return Ok(Vec::new());
    }
    let mut thread_ids = read_active_thread_ids(Some(port)).unwrap_or_default();
    thread_ids.extend(read_thread_handoff_candidate_ids_from_state_db(
        &paths.codex_state_db_file,
    )?);
    Ok(thread_ids)
}

pub(super) fn finalize_rotation_after_import(
    prepared: &PreparedRotation,
    import_outcome: &ThreadHandoffImportOutcome,
) -> Result<()> {
    ensure_no_rotation_drift(prepared)?;
    if !import_outcome.is_complete() {
        return Err(anyhow!(import_outcome.describe()));
    }
    persist_prepared_rotation_pool(prepared)?;
    Ok(())
}

pub(super) fn capture_source_thread_recovery_events_before_rotation(
    prepared: &PreparedRotation,
    port: u16,
) -> Result<()> {
    use crate::watch::{read_watch_state, write_watch_state};

    if prepared.action != PreparedRotationAction::Switch {
        return Ok(());
    }

    let mut state = read_watch_state()?;
    let source_account_id = &prepared.previous.account_id;
    let source_state = state.account_state(source_account_id);
    let latest_recoverable_log_id = read_latest_recoverable_turn_failure_log_id()?;
    let recovery_last_log_id = source_state.last_thread_recovery_log_id.or_else(|| {
        latest_recoverable_log_id
            .map(|id| id.saturating_sub(ROTATION_THREAD_RECOVERY_LOOKBACK_LOGS))
    });

    let recovery = run_thread_recovery_iteration(RecoveryIterationOptions {
        port: Some(port),
        current_live_email: source_state
            .last_live_email
            .clone()
            .or_else(|| Some(prepared.previous.email.clone())),
        current_quota_usable: source_state.quota.as_ref().map(|quota| quota.usable),
        current_primary_quota_left_percent: source_state
            .quota
            .as_ref()
            .and_then(|quota| quota.primary_quota_left_percent),
        rotated: false,
        last_log_id: recovery_last_log_id,
        pending: source_state.thread_recovery_pending,
        pending_events: source_state.thread_recovery_pending_events.clone(),
        detect_only: true,
    })?;

    let mut updated_source_state = source_state;
    updated_source_state.last_thread_recovery_log_id = recovery.last_log_id;
    updated_source_state.thread_recovery_pending = recovery.pending;
    updated_source_state.thread_recovery_pending_events = recovery.pending_events;
    updated_source_state.thread_recovery_backfill_complete = true;
    state.set_account_state(source_account_id.to_string(), updated_source_state);
    write_watch_state(&state)
}

#[cfg(test)]
pub(crate) fn translate_recovery_events_after_rotation(
    source_account_id: &str,
    target_account_id: &str,
    port: u16,
    source_handoffs: &[ThreadHandoff],
) -> Result<()> {
    translate_recovery_events_after_rotation_with_identity(
        source_account_id,
        target_account_id,
        source_account_id,
        target_account_id,
        port,
        source_handoffs,
    )
}

pub(super) fn translate_recovery_events_after_rotation_with_identity(
    source_account_id: &str,
    target_account_id: &str,
    source_sync_identity: &str,
    target_sync_identity: &str,
    port: u16,
    source_handoffs: &[ThreadHandoff],
) -> Result<()> {
    use crate::watch::{read_watch_state, write_watch_state};

    let mut state = read_watch_state()?;
    let pending_events = state
        .account_state(source_account_id)
        .thread_recovery_pending_events
        .clone();

    if pending_events.is_empty() {
        return Ok(());
    }

    let paths = crate::paths::resolve_paths()?;
    let store = ConversationSyncStore::new(&paths.conversation_sync_db_file)?;
    let transport = HostConversationTransport::new(port);
    let handoffs_by_source_thread_id = source_handoffs
        .iter()
        .map(|handoff| (handoff.source_thread_id.clone(), handoff.clone()))
        .collect::<BTreeMap<_, _>>();

    let mut translated_events = Vec::new();
    let mut unresolved_events = Vec::new();
    for mut event in pending_events {
        let exported_handoff = handoffs_by_source_thread_id
            .get(&event.thread_id)
            .cloned()
            .or_else(|| {
                export_single_thread_handoff_with_identity(
                    &transport,
                    &event.thread_id,
                    source_sync_identity,
                )
                .ok()
                .flatten()
            });
        let lineage_id = store
            .get_lineage_id(source_sync_identity, &event.thread_id)?
            .or_else(|| {
                exported_handoff
                    .as_ref()
                    .map(|handoff| handoff.lineage_id.clone())
            })
            .unwrap_or_else(|| event.thread_id.clone());
        let target_thread_id = match store.get_local_thread_id(target_sync_identity, &lineage_id)? {
            Some(existing) => Some(existing),
            None => match exported_handoff.clone() {
                Some(handoff) => {
                    let import_result =
                        import_thread_handoffs(&transport, target_sync_identity, &[handoff], None);
                    if import_result
                        .as_ref()
                        .map(|outcome| outcome.is_complete())
                        .unwrap_or(false)
                    {
                        store.get_local_thread_id(target_sync_identity, &lineage_id)?
                    } else {
                        None
                    }
                }
                _ => None,
            },
        };
        if let Some(target_thread_id) = target_thread_id {
            event.thread_id = target_thread_id;
            event.source_log_id = TRANSLATED_RECOVERY_SOURCE_LOG_ID;
            if let Some(handoff) = exported_handoff {
                event.rehydration = Some(ThreadRecoveryRehydration {
                    lineage_id: handoff.lineage_id,
                    cwd: handoff.cwd,
                    items: handoff.items,
                });
            }
            translated_events.push(event);
        } else {
            unresolved_events.push(event);
        }
    }

    if !translated_events.is_empty() {
        if source_account_id == target_account_id {
            let mut shared_state = state.account_state(target_account_id);
            shared_state.thread_recovery_pending_events = unresolved_events;
            for translated in translated_events {
                if !shared_state
                    .thread_recovery_pending_events
                    .iter()
                    .any(|e| e.thread_id == translated.thread_id)
                {
                    shared_state.thread_recovery_pending_events.push(translated);
                }
            }
            shared_state.thread_recovery_pending =
                !shared_state.thread_recovery_pending_events.is_empty();
            state.set_account_state(target_account_id.to_string(), shared_state);
        } else {
            let mut target_state = state.account_state(target_account_id);
            target_state.thread_recovery_pending = true;
            for translated in translated_events {
                if !target_state
                    .thread_recovery_pending_events
                    .iter()
                    .any(|e| e.thread_id == translated.thread_id)
                {
                    target_state.thread_recovery_pending_events.push(translated);
                }
            }
            state.set_account_state(target_account_id.to_string(), target_state);

            let mut source_state = state.account_state(source_account_id);
            source_state.thread_recovery_pending_events = unresolved_events;
            source_state.thread_recovery_pending =
                !source_state.thread_recovery_pending_events.is_empty();
            state.set_account_state(source_account_id.to_string(), source_state);
        }

        write_watch_state(&state)?;
    }

    Ok(())
}

pub(super) fn recover_translated_thread_events_after_rotation(
    target_account_id: &str,
    target_email: &str,
    port: u16,
    progress: Option<&Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<()> {
    use crate::watch::{read_watch_state, write_watch_state};

    let mut state = read_watch_state()?;
    let mut target_state = state.account_state(target_account_id);
    if !target_state.thread_recovery_pending
        && target_state.thread_recovery_pending_events.is_empty()
    {
        return Ok(());
    }

    let recovery = match run_thread_recovery_iteration(RecoveryIterationOptions {
        port: Some(port),
        current_live_email: Some(target_email.to_string()),
        current_quota_usable: target_state.quota.as_ref().map(|quota| quota.usable),
        current_primary_quota_left_percent: target_state
            .quota
            .as_ref()
            .and_then(|quota| quota.primary_quota_left_percent),
        rotated: true,
        last_log_id: target_state.last_thread_recovery_log_id,
        pending: target_state.thread_recovery_pending,
        pending_events: target_state.thread_recovery_pending_events.clone(),
        detect_only: false,
    }) {
        Ok(recovery) => recovery,
        Err(error) => {
            target_state.thread_recovery_pending = target_state.thread_recovery_pending
                || !target_state.thread_recovery_pending_events.is_empty();
            state.set_account_state(target_account_id.to_string(), target_state);
            let _ = write_watch_state(&state);
            return Err(error).with_context(|| {
                format!(
                    "Failed to recover pending interrupted threads for rotated account {}.",
                    target_account_id
                )
            });
        }
    };

    if let Some(progress) = progress {
        if !recovery.continued_thread_ids.is_empty() {
            progress(format!(
                "Recovered {} interrupted thread(s) after rotation.",
                recovery.continued_thread_ids.len()
            ));
        }
    }

    target_state.last_thread_recovery_log_id = recovery.last_log_id;
    target_state.thread_recovery_pending = recovery.pending;
    target_state.thread_recovery_pending_events = recovery.pending_events;
    target_state.thread_recovery_backfill_complete = true;
    state.set_account_state(target_account_id.to_string(), target_state);
    write_watch_state(&state)?;
    Ok(())
}

pub(super) fn translate_and_recover_thread_events_after_rotation(
    prepared: &PreparedRotation,
    port: u16,
    handoffs: &[ThreadHandoff],
    progress: Option<&Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<()> {
    let previous_sync_identity = conversation_sync_identity(&prepared.previous);
    let target_sync_identity = conversation_sync_identity(&prepared.target);
    translate_recovery_events_after_rotation_with_identity(
        &prepared.previous.account_id,
        &prepared.target.account_id,
        &previous_sync_identity,
        &target_sync_identity,
        port,
        handoffs,
    )?;
    recover_translated_thread_events_after_rotation(
        &prepared.target.account_id,
        &prepared.target.email,
        port,
        progress,
    )
}

pub(super) fn maybe_complete_non_switch_next_result(
    backend: &dyn RotationBackend,
    prepared: &PreparedRotation,
    port: u16,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    options: RotationCommandOptions,
    allow_create: bool,
    disabled_retry_budget: usize,
) -> Result<Option<NextResult>> {
    match prepared.action {
        PreparedRotationAction::Stay => {
            if prepared.persist_pool {
                ensure_no_rotation_drift(prepared)?;
                persist_prepared_rotation_pool(prepared)?;
            }
            let summary = summarize_codex_auth(&prepared.target.auth);
            Ok(Some(NextResult::Stayed {
                message: prepared.message.clone(),
                summary,
            }))
        }
        PreparedRotationAction::CreateRequired if allow_create => {
            if prepared.persist_pool {
                ensure_no_rotation_drift(prepared)?;
                persist_prepared_rotation_pool(prepared)?;
            }
            let create_output = cmd_create_with_progress(
                CreateCommandOptions {
                    force: true,
                    ignore_current: true,
                    require_usable_quota: true,
                    restore_previous_auth_after_create: true,
                    source: CreateCommandSource::Next,
                    ..CreateCommandOptions::default()
                },
                progress.clone(),
            )?;
            restore_pool_active_index(prepared.previous_index)?;
            let next = rotate_next_impl_with_retry(
                backend,
                port,
                progress,
                false,
                options,
                disabled_retry_budget,
            )?;
            let summary = match &next {
                NextResult::Rotated { summary, .. }
                | NextResult::Stayed { summary, .. }
                | NextResult::Created { summary, .. } => summary.clone(),
            };
            let combined = match next {
                NextResult::Rotated { message, .. }
                | NextResult::Stayed { message, .. }
                | NextResult::Created {
                    output: message, ..
                } => {
                    format!("{}\n{}", create_output.trim_end(), message)
                }
            };
            Ok(Some(NextResult::Created {
                output: combined,
                summary,
            }))
        }
        PreparedRotationAction::CreateRequired => Err(anyhow!(
            "Auto rotation requires creating a replacement account, but the retry budget is exhausted."
        )),
        PreparedRotationAction::Switch => Ok(None),
    }
}

pub(super) fn is_disabled_target_validation_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string().contains(DISABLED_TARGET_ERROR_SNIPPET))
}

pub(super) struct DisabledTargetRetryContext<'a> {
    pub(super) budget: usize,
    pub(super) error: anyhow::Error,
    pub(super) message: &'a str,
}

pub(super) fn rollback_and_maybe_retry_after_disabled_target<F>(
    backend: &dyn RotationBackend,
    prepared: &PreparedRotation,
    port: u16,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    retry_context: DisabledTargetRetryContext<'_>,
    retry: F,
) -> Result<NextResult>
where
    F: FnOnce(Option<Arc<dyn Fn(String) + Send + Sync>>) -> Result<NextResult>,
{
    save_rotation_checkpoint_for_prepared(prepared, RotationCheckpointPhase::Rollback).ok();
    backend
        .rollback_after_failed_activation(prepared, port, progress.clone())
        .with_context(|| {
            format!(
                "Failed to roll back disabled target {} after activation.",
                prepared.target.label
            )
        })?;
    clear_rotation_checkpoint().ok();

    if retry_context.budget == 0 {
        return Err(retry_context.error);
    }

    if let Some(progress) = progress.as_ref() {
        progress(retry_context.message.to_string());
    }

    retry(progress)
}

pub(super) fn rotate_next_impl(
    backend: &dyn RotationBackend,
    port: u16,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    allow_create: bool,
    options: RotationCommandOptions,
) -> Result<NextResult> {
    rotate_next_impl_with_retry(backend, port, progress, allow_create, options, 1)
}

pub(super) fn rotate_next_impl_with_retry(
    backend: &dyn RotationBackend,
    port: u16,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    allow_create: bool,
    options: RotationCommandOptions,
    mut disabled_retry_budget: usize,
) -> Result<NextResult> {
    recover_incomplete_rotation_state_without_lock()?;
    debug_pool_drift_state("after_recover");
    let source_thread_candidates = backend.capture_source_thread_candidates(port)?;
    let mut prepared = prepare_next_rotation_with_progress(progress.clone())?;
    if debug_pool_drift_enabled() {
        eprintln!(
            "codex-rotate debug [after_prepare] previous_index={} target_index={} previous_email={} target_email={}",
            prepared.previous_index,
            prepared.target_index,
            prepared.previous.email,
            prepared.target.email
        );
    }
    let paths = resolve_paths()?;
    if prepared.action == PreparedRotationAction::Switch {
        if let Err(error) = ensure_target_account_still_valid(&prepared) {
            if !is_disabled_target_validation_error(&error) {
                return Err(error);
            }
            if disabled_retry_budget == 0 {
                return Err(error);
            }
            disabled_retry_budget = disabled_retry_budget.saturating_sub(1);
            if let Some(progress) = progress.as_ref() {
                progress(
                    "Rotation target became disabled before activation; re-evaluating eligible target."
                        .to_string(),
                );
            }
            prepared = prepare_next_rotation_with_progress(progress.clone())?;
            if debug_pool_drift_enabled() {
                eprintln!(
                    "codex-rotate debug [after_reprepare] previous_index={} target_index={} previous_email={} target_email={}",
                    prepared.previous_index,
                    prepared.target_index,
                    prepared.previous.email,
                    prepared.target.email
                );
            }
            if prepared.action == PreparedRotationAction::Switch {
                ensure_target_account_still_valid(&prepared)?;
            }
        }
    }

    let _ = ensure_host_personas_ready(&paths, &mut prepared.pool)?;
    debug_pool_drift_state("after_persona_ready");

    if let Some(result) = maybe_complete_non_switch_next_result(
        backend,
        &prepared,
        port,
        progress.clone(),
        options,
        allow_create,
        disabled_retry_budget,
    )? {
        return Ok(result);
    }

    if let Err(error) = ensure_target_account_still_valid(&prepared) {
        if !is_disabled_target_validation_error(&error) {
            return Err(error);
        }
        if disabled_retry_budget == 0 {
            return Err(error);
        }
        disabled_retry_budget = disabled_retry_budget.saturating_sub(1);
        if let Some(progress) = progress.as_ref() {
            progress(
                "Rotation target became disabled mid-flow; re-evaluating eligible target."
                    .to_string(),
            );
        }
        prepared = prepare_next_rotation_with_progress(progress.clone())?;
        if debug_pool_drift_enabled() {
            eprintln!(
                "codex-rotate debug [after_reprepare] previous_index={} target_index={} previous_email={} target_email={}",
                prepared.previous_index,
                prepared.target_index,
                prepared.previous.email,
                prepared.target.email
            );
        }
        if prepared.action == PreparedRotationAction::Switch {
            ensure_target_account_still_valid(&prepared)?;
        }
        let _ = ensure_host_personas_ready(&paths, &mut prepared.pool)?;
        debug_pool_drift_state("after_reprepare_persona_ready");
        if let Some(result) = maybe_complete_non_switch_next_result(
            backend,
            &prepared,
            port,
            progress.clone(),
            options,
            allow_create,
            disabled_retry_budget,
        )? {
            return Ok(result);
        }
    }
    let _ = capture_source_thread_recovery_events_before_rotation(&prepared, port);
    save_rotation_checkpoint_for_prepared(&prepared, RotationCheckpointPhase::Activate)?;

    let handoffs = backend
        .activate(
            &prepared,
            port,
            progress.clone(),
            source_thread_candidates.clone(),
            options,
        )
        .with_context(|| {
            format!(
                "Failed to activate persona {}.",
                prepared
                    .target
                    .persona
                    .as_ref()
                    .map(|persona| persona.persona_id.as_str())
                    .unwrap_or(prepared.target.label.as_str())
            )
        })?;
    debug_pool_drift_state("after_activate");

    if let Err(error) = ensure_target_account_still_valid(&prepared) {
        if is_disabled_target_validation_error(&error) {
            return rollback_and_maybe_retry_after_disabled_target(
                backend,
                &prepared,
                port,
                progress.clone(),
                DisabledTargetRetryContext {
                    budget: disabled_retry_budget,
                    error,
                    message: "Rotation target became disabled after activation; restored the previous account and re-evaluating eligible target.",
                },
                |progress| {
                    rotate_next_impl_with_retry(
                        backend,
                        port,
                        progress,
                        allow_create,
                        options,
                        disabled_retry_budget.saturating_sub(1),
                    )
                },
            );
        }
        save_rotation_checkpoint_for_prepared(&prepared, RotationCheckpointPhase::Rollback).ok();
        let rollback_result =
            backend.rollback_after_failed_activation(&prepared, port, progress.clone());
        if rollback_result.is_ok() {
            clear_rotation_checkpoint().ok();
        }
        return Err(error);
    }

    save_rotation_checkpoint_for_prepared(&prepared, RotationCheckpointPhase::Import)?;

    let import_outcome = if handoffs.is_empty() {
        ThreadHandoffImportOutcome::default()
    } else {
        let transport = HostConversationTransport::new(port);
        let target_sync_identity = conversation_sync_identity(&prepared.target);
        import_thread_handoffs(
            &transport,
            &target_sync_identity,
            &handoffs,
            progress.as_ref(),
        )?
    };

    let result = (|| -> Result<()> {
        if let Some(progress) = progress.as_ref() {
            progress(format!("Activated persona for {}.", prepared.target.label));
        }
        debug_pool_drift_state("before_finalize");
        ensure_target_account_still_valid(&prepared)?;
        sync_host_conversation_snapshot_after_import(&prepared)?;
        finalize_rotation_after_import(&prepared, &import_outcome)?;
        let _ = translate_and_recover_thread_events_after_rotation(
            &prepared,
            port,
            &handoffs,
            progress.as_ref(),
        );
        Ok(())
    })();

    if let Err(error) = result {
        if is_disabled_target_validation_error(&error) {
            return rollback_and_maybe_retry_after_disabled_target(
                backend,
                &prepared,
                port,
                progress.clone(),
                DisabledTargetRetryContext {
                    budget: disabled_retry_budget,
                    error,
                    message: "Rotation target became disabled before commit; restored the previous account and re-evaluating eligible target.",
                },
                |progress| {
                    rotate_next_impl_with_retry(
                        backend,
                        port,
                        progress,
                        allow_create,
                        options,
                        disabled_retry_budget.saturating_sub(1),
                    )
                },
            );
        }
        save_rotation_checkpoint_for_prepared(&prepared, RotationCheckpointPhase::Rollback).ok();
        let rollback_result =
            backend.rollback_after_failed_activation(&prepared, port, progress.clone());
        if rollback_result.is_ok() {
            clear_rotation_checkpoint().ok();
        }
        return Err(error);
    }

    clear_rotation_checkpoint()?;

    Ok(NextResult::Rotated {
        message: prepared.message,
        summary: summarize_codex_auth(&prepared.target.auth),
    })
}

pub(super) fn rotate_prev_impl(
    backend: &dyn RotationBackend,
    port: u16,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    options: RotationCommandOptions,
) -> Result<String> {
    recover_incomplete_rotation_state_without_lock()?;
    let source_thread_candidates = backend.capture_source_thread_candidates(port)?;
    let mut prepared = prepare_prev_rotation()?;
    let paths = resolve_paths()?;
    if prepared.action == PreparedRotationAction::Switch {
        ensure_target_account_still_valid(&prepared)?;
    }
    let _ = ensure_host_personas_ready(&paths, &mut prepared.pool)?;
    if prepared.action != PreparedRotationAction::Switch {
        if prepared.persist_pool {
            ensure_no_rotation_drift(&prepared)?;
            persist_prepared_rotation_pool(&prepared)?;
        }
        return Ok(prepared.message);
    }

    ensure_target_account_still_valid(&prepared)?;
    let _ = capture_source_thread_recovery_events_before_rotation(&prepared, port);
    save_rotation_checkpoint_for_prepared(&prepared, RotationCheckpointPhase::Activate)?;

    let handoffs = backend.activate(
        &prepared,
        port,
        progress.clone(),
        source_thread_candidates,
        options,
    )?;

    save_rotation_checkpoint_for_prepared(&prepared, RotationCheckpointPhase::Import)?;

    let import_outcome = if handoffs.is_empty() {
        ThreadHandoffImportOutcome::default()
    } else {
        let transport = HostConversationTransport::new(port);
        let target_sync_identity = conversation_sync_identity(&prepared.target);
        import_thread_handoffs(
            &transport,
            &target_sync_identity,
            &handoffs,
            progress.as_ref(),
        )?
    };

    let result = (|| -> Result<()> {
        if let Some(progress) = progress.as_ref() {
            progress(format!("Activated persona for {}.", prepared.target.label));
        }
        sync_host_conversation_snapshot_after_import(&prepared)?;
        finalize_rotation_after_import(&prepared, &import_outcome)?;
        let _ = translate_and_recover_thread_events_after_rotation(
            &prepared,
            port,
            &handoffs,
            progress.as_ref(),
        );
        Ok(())
    })();

    if let Err(error) = result {
        save_rotation_checkpoint_for_prepared(&prepared, RotationCheckpointPhase::Rollback).ok();
        let rollback_result =
            backend.rollback_after_failed_activation(&prepared, port, progress.clone());
        if rollback_result.is_ok() {
            clear_rotation_checkpoint().ok();
        }
        return Err(error);
    }

    clear_rotation_checkpoint()?;

    Ok(prepared.message)
}

pub(super) fn rotate_set_impl(
    backend: &dyn RotationBackend,
    port: u16,
    selector: &str,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    options: RotationCommandOptions,
) -> Result<String> {
    recover_incomplete_rotation_state_without_lock()?;
    let source_thread_candidates = backend.capture_source_thread_candidates(port)?;
    let mut prepared = prepare_set_rotation(selector)?;
    let paths = resolve_paths()?;
    if prepared.action == PreparedRotationAction::Switch {
        ensure_target_account_still_present(&prepared)?;
    }
    let _ = ensure_host_personas_ready(&paths, &mut prepared.pool)?;
    if prepared.action != PreparedRotationAction::Switch {
        if prepared.persist_pool {
            ensure_no_rotation_drift(&prepared)?;
            persist_prepared_rotation_pool(&prepared)?;
        }
        return Ok(prepared.message);
    }

    ensure_target_account_still_present(&prepared)?;
    let _ = capture_source_thread_recovery_events_before_rotation(&prepared, port);
    save_rotation_checkpoint_for_prepared(&prepared, RotationCheckpointPhase::Activate)?;

    let handoffs = backend.activate(
        &prepared,
        port,
        progress.clone(),
        source_thread_candidates,
        options,
    )?;

    save_rotation_checkpoint_for_prepared(&prepared, RotationCheckpointPhase::Import)?;

    let import_outcome = if handoffs.is_empty() {
        ThreadHandoffImportOutcome::default()
    } else {
        let transport = HostConversationTransport::new(port);
        let target_sync_identity = conversation_sync_identity(&prepared.target);
        import_thread_handoffs(
            &transport,
            &target_sync_identity,
            &handoffs,
            progress.as_ref(),
        )?
    };

    let result = (|| -> Result<()> {
        if let Some(progress) = progress.as_ref() {
            progress(format!("Activated persona for {}.", prepared.target.label));
        }
        ensure_target_account_still_present(&prepared)?;
        sync_host_conversation_snapshot_after_import(&prepared)?;
        finalize_rotation_after_import(&prepared, &import_outcome)?;
        let _ = translate_and_recover_thread_events_after_rotation(
            &prepared,
            port,
            &handoffs,
            progress.as_ref(),
        );
        Ok(())
    })();

    if let Err(error) = result {
        save_rotation_checkpoint_for_prepared(&prepared, RotationCheckpointPhase::Rollback).ok();
        let rollback_result =
            backend.rollback_after_failed_activation(&prepared, port, progress.clone());
        if rollback_result.is_ok() {
            clear_rotation_checkpoint().ok();
        }
        return Err(error);
    }

    clear_rotation_checkpoint()?;

    Ok(prepared.message)
}

pub(super) fn relogin_host(
    port: u16,
    selector: &str,
    options: ReloginOptions,
    progress: Option<AutomationProgressCallback>,
) -> Result<String> {
    let Some(target_account) = resolve_pool_account(selector)? else {
        return cmd_relogin_with_progress(selector, options, progress);
    };

    // `relogin` already holds the shared rotation lock at the public entry point.
    // Use the no-lock variant here to avoid self-contention.
    recover_incomplete_rotation_state_without_lock()?;

    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    ensure_host_personas_ready(&paths, &mut pool)?;
    let active_index = pool.active_index.min(pool.accounts.len().saturating_sub(1));
    let target_index = pool
        .accounts
        .iter()
        .position(|entry| entry.account_id == target_account.account_id)
        .ok_or_else(|| anyhow!("Failed to resolve relogin target {} in the pool.", selector))?;
    codex_rotate_core::pool::persist_prepared_rotation_pool(&PreparedRotation {
        action: PreparedRotationAction::Stay,
        pool: pool.clone(),
        previous_index: active_index,
        target_index: active_index,
        previous: pool.accounts[active_index].clone(),
        target: pool.accounts[active_index].clone(),
        message: String::new(),
        persist_pool: true,
    })?;
    if target_index == active_index {
        return cmd_relogin_with_progress(selector, options, progress);
    }

    let managed_running_before = managed_codex_is_running(&paths.debug_profile_dir)?;
    if managed_running_before {
        wait_for_all_threads_idle(port, progress.as_ref())?;
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    switch_host_persona(
        &paths,
        &pool.accounts[active_index],
        &pool.accounts[target_index],
        true,
    )?;
    write_selected_account_auth(&pool.accounts[target_index])?;
    let output = cmd_relogin_with_progress(selector, options, progress.clone());
    switch_host_persona(
        &paths,
        &pool.accounts[target_index],
        &pool.accounts[active_index],
        false,
    )?;
    write_selected_account_auth(&pool.accounts[active_index])?;
    if let Ok(mut current_pool) = load_pool() {
        current_pool.active_index = active_index;
        let _ = save_pool(&current_pool);
    }
    if managed_running_before {
        ensure_debug_codex_instance(None, Some(port), None, None)?;
    }
    output
}

#[derive(Debug)]
pub(super) struct HostRotationActivation {
    pub(super) items: Vec<ThreadHandoff>,
}

pub(super) fn activate_host_rotation(
    paths: &RuntimePaths,
    prepared: &PreparedRotation,
    port: u16,
    progress: Option<&Arc<dyn Fn(String) + Send + Sync>>,
    source_thread_candidates: Vec<String>,
    options: RotationCommandOptions,
) -> Result<HostRotationActivation> {
    let managed_running_before = managed_codex_is_running(&paths.debug_profile_dir)?;
    let mut managed_running_for_handoff = managed_running_before;
    let mut pre_wait_thread_ids = source_thread_candidates;
    if !managed_running_for_handoff && options.force_managed_window {
        if let Some(progress) = progress {
            progress(
                "Managed Codex is not running; opening a managed window for thread handoff sync."
                    .to_string(),
            );
        }
        ensure_debug_codex_instance(None, Some(port), None, None)
            .context("Failed to open managed Codex window requested by -mw.")?;
        managed_running_for_handoff = true;
    } else if !managed_running_for_handoff {
        if let Some(progress) = progress {
            progress(
                "Managed Codex is not running; switching personas without thread handoff sync. Use -mw to open a managed window first."
                    .to_string(),
            );
        }
    }
    if managed_running_for_handoff {
        pre_wait_thread_ids.extend(read_active_thread_ids(Some(port))?);
        pre_wait_thread_ids.extend(read_thread_handoff_candidate_ids_from_state_db(
            &paths.codex_state_db_file,
        )?);
    }
    if managed_running_for_handoff {
        if let Some(progress) = progress {
            progress("Waiting for active Codex work to become handoff-safe.".to_string());
        }
        wait_for_all_threads_idle(port, progress)?;
    }
    let exported_handoffs = if managed_running_for_handoff {
        let previous_sync_identity = conversation_sync_identity(&prepared.previous);
        export_thread_handoffs_with_identity_and_candidates(
            port,
            &prepared.previous.account_id,
            &previous_sync_identity,
            pre_wait_thread_ids,
        )?
    } else {
        Vec::new()
    };

    if managed_running_for_handoff {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    let transition = (|| -> Result<()> {
        switch_host_persona(paths, &prepared.previous, &prepared.target, false)?;
        write_selected_account_auth(&prepared.target)?;

        Ok(())
    })();

    match transition {
        Ok(_) => {
            if managed_running_for_handoff {
                if let Some(progress) = progress {
                    progress(
                        "Restarting managed Codex after committing the target persona.".to_string(),
                    );
                }
                ensure_debug_codex_instance(None, Some(port), None, None).with_context(|| {
                    format!(
                        "Committed host activation for {} but failed to relaunch managed Codex.",
                        prepared.target.label
                    )
                })?;
            }
            Ok(HostRotationActivation {
                items: exported_handoffs,
            })
        }
        Err(error) => {
            let rollback_error = rollback_after_failed_host_activation(
                paths,
                prepared,
                managed_running_for_handoff,
                port,
            );
            if let Err(rollback_error) = rollback_error {
                return Err(anyhow!(
                    "{error} (rollback after failed host activation also failed: {rollback_error:#})"
                ));
            }
            Err(error)
        }
    }
}

pub(super) fn sync_host_conversation_snapshot_after_import(
    prepared: &PreparedRotation,
) -> Result<()> {
    if current_environment()? != RotationEnvironment::Host {
        return Ok(());
    }
    let paths = resolve_paths()?;
    let source = host_persona_paths(
        &paths,
        prepared
            .previous
            .persona
            .as_ref()
            .ok_or_else(|| anyhow!("Source account is missing persona metadata."))?,
    )?;
    let target = host_persona_paths(
        &paths,
        prepared
            .target
            .persona
            .as_ref()
            .ok_or_else(|| anyhow!("Target account is missing persona metadata."))?,
    )?;
    sync_host_persona_conversation_snapshot(
        &source.codex_home,
        &conversation_sync_identity(&prepared.previous),
        &target.codex_home,
        &conversation_sync_identity(&prepared.target),
        &paths.conversation_sync_db_file,
    )
}

pub(super) fn rollback_after_failed_host_activation(
    paths: &RuntimePaths,
    prepared: &PreparedRotation,
    managed_running_before: bool,
    port: u16,
) -> Result<()> {
    let mut failures = Vec::new();

    if let Err(error) = rollback_prepared_rotation(prepared) {
        failures.push(format!("core rollback failed: {error:#}"));
    }
    if let Err(error) = switch_host_persona(paths, &prepared.target, &prepared.previous, false) {
        failures.push(format!("symlink rollback failed: {error:#}"));
    }
    if managed_running_before {
        if let Err(error) = ensure_debug_codex_instance(None, Some(port), None, None) {
            failures.push(format!("managed Codex relaunch failed: {error:#}"));
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(failures.join(" | ")))
    }
}

#[cfg(test)]
pub(super) fn transfer_thread_recovery_state_between_accounts(
    source_account_id: &str,
    target_account_id: &str,
) -> Result<()> {
    if source_account_id.trim().is_empty()
        || target_account_id.trim().is_empty()
        || source_account_id == target_account_id
    {
        return Ok(());
    }
    let mut watch_state = read_watch_state()?;
    let mut source_state = watch_state.account_state(source_account_id);
    let mut target_state = watch_state.account_state(target_account_id);

    target_state.last_thread_recovery_log_id = source_state.last_thread_recovery_log_id;
    target_state.thread_recovery_pending = source_state.thread_recovery_pending;
    target_state.thread_recovery_pending_events =
        source_state.thread_recovery_pending_events.clone();
    target_state.thread_recovery_backfill_complete = source_state.thread_recovery_backfill_complete;

    source_state.last_thread_recovery_log_id = None;
    source_state.thread_recovery_pending = false;
    source_state.thread_recovery_pending_events.clear();

    watch_state.set_account_state(source_account_id.to_string(), source_state);
    watch_state.set_account_state(target_account_id.to_string(), target_state);
    write_watch_state(&watch_state)
}
