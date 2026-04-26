use super::*;

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
    let relogin_accounts = load_relogin_account_emails()?;

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
        if account_marked_for_relogin(&relogin_accounts, &pool.accounts[candidate_index].email) {
            cursor_index = candidate_index;
            continue;
        }
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
        &relogin_accounts,
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

pub fn prepare_set_rotation(selector: &str) -> Result<PreparedRotation> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    if pool.accounts.is_empty() {
        return Err(anyhow!("No accounts in pool. Run: codex-rotate add"));
    }

    let previous_index = pool.active_index.min(pool.accounts.len().saturating_sub(1));
    let previous = pool.accounts[previous_index].clone();
    let selection = resolve_account_selector(&pool, selector)?;
    let target_index = selection.index;
    let target = selection.entry;

    let previous_label = previous.label.clone();
    let previous_email = previous.email.clone();
    let previous_plan_type = previous.plan_type.clone();
    let target_label = target.label.clone();
    let target_email = target.email.clone();
    let target_plan_type = target.plan_type.clone();
    let total_accounts = pool.accounts.len();

    if target_index == previous_index {
        return Ok(PreparedRotation {
            action: PreparedRotationAction::Stay,
            pool,
            previous_index,
            target_index,
            previous: previous.clone(),
            target: previous,
            message: format!(
                "{GREEN}ROTATE{RESET} Stayed on {BOLD}{}{RESET} ({CYAN}{}{RESET}, {})\n{DIM}  [{}/{}] | selected explicitly{RESET}",
                previous_label,
                previous_email,
                previous_plan_type,
                previous_index + 1,
                total_accounts,
            ),
            persist_pool: dirty,
        });
    }

    Ok(PreparedRotation {
        action: PreparedRotationAction::Switch,
        pool,
        previous_index,
        target_index,
        previous,
        target,
        message: format!(
            "{GREEN}ROTATE{RESET} {} ({}) -> {BOLD}{}{RESET} ({CYAN}{}{RESET}, {})\n{DIM}  [{}/{}] | selected explicitly{RESET}",
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
    let relogin_accounts = load_relogin_account_emails()?;

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
        if account_marked_for_relogin(&relogin_accounts, &pool.accounts[candidate_index].email) {
            cursor_index = candidate_index;
            continue;
        }
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
        &relogin_accounts,
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
