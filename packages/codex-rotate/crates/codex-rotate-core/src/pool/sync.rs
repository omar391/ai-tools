use super::*;

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

pub(super) fn fetch_actual_egress_region() -> Result<String> {
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

pub(super) fn sync_pool_current_auth_from_codex(
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

pub(super) fn sync_pool_current_auth_from_auth(
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
            relogin: false,
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

pub(super) fn find_pool_account_index_by_identity(
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

    if let Some(index) = pool
        .accounts
        .iter()
        .position(|entry| account_entry_matches_identity(entry, account_id, email, plan_type))
    {
        return Some(index);
    }

    if pool
        .accounts
        .get(pool.active_index)
        .map(|entry| account_entry_matches_email_plan(entry, email, plan_type))
        .unwrap_or(false)
    {
        return Some(pool.active_index);
    }

    if let Some(index) = pool
        .accounts
        .iter()
        .position(|entry| account_entry_matches_email_plan(entry, email, plan_type))
    {
        return Some(index);
    }

    None
}
