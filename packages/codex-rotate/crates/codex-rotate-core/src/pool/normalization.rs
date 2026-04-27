use super::*;
use std::collections::HashMap;

pub(crate) fn normalize_pool_entries(pool: &mut Pool) -> bool {
    let mut changed = false;
    for entry in &mut pool.accounts {
        changed |= normalize_cached_quota_usability(entry);
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

        let next_persona = normalized_persona(entry);
        if entry.persona.as_ref() != Some(&next_persona) {
            entry.persona = Some(next_persona);
            changed = true;
        }
    }

    changed |= dedupe_pool_entries(pool);

    let max_active_index = pool.accounts.len().saturating_sub(1);
    let normalized_active_index = pool.active_index.min(max_active_index);
    if pool.active_index != normalized_active_index {
        pool.active_index = normalized_active_index;
        changed = true;
    }
    changed
}

fn dedupe_pool_entries(pool: &mut Pool) -> bool {
    let original_active_index = pool.active_index;
    let mut changed = false;
    let mut deduped = Vec::with_capacity(pool.accounts.len());
    let mut index_by_label = HashMap::new();
    let mut normalized_active_index = 0usize;

    for (original_index, entry) in pool.accounts.drain(..).enumerate() {
        let label = entry.label.clone();
        if let Some(existing_index) = index_by_label.get(&label).copied() {
            let merged = merge_duplicate_account_entries(&deduped[existing_index], entry);
            if deduped[existing_index] != merged {
                deduped[existing_index] = merged;
            }
            if original_index == original_active_index {
                normalized_active_index = existing_index;
            }
            changed = true;
            continue;
        }

        let deduped_index = deduped.len();
        if original_index == original_active_index {
            normalized_active_index = deduped_index;
        }
        index_by_label.insert(label, deduped_index);
        deduped.push(entry);
    }

    pool.accounts = deduped;
    pool.active_index = normalized_active_index;
    changed
}

fn merge_duplicate_account_entries(
    existing: &AccountEntry,
    incoming: AccountEntry,
) -> AccountEntry {
    let mut merged = incoming;

    if merged.alias.is_none() {
        merged.alias = existing.alias.clone();
    }

    if !existing.added_at.trim().is_empty()
        && (merged.added_at.trim().is_empty() || existing.added_at < merged.added_at)
    {
        merged.added_at = existing.added_at.clone();
    }

    let keep_existing_quota = match (
        existing.last_quota_checked_at.as_deref(),
        merged.last_quota_checked_at.as_deref(),
    ) {
        (Some(existing_checked_at), Some(merged_checked_at)) => {
            existing_checked_at > merged_checked_at
        }
        (Some(_), None) => true,
        _ => false,
    };
    if keep_existing_quota {
        merged.last_quota_usable = existing.last_quota_usable;
        merged.last_quota_summary = existing.last_quota_summary.clone();
        merged.last_quota_blocker = existing.last_quota_blocker.clone();
        merged.last_quota_checked_at = existing.last_quota_checked_at.clone();
        merged.last_quota_primary_left_percent = existing.last_quota_primary_left_percent;
        merged.last_quota_next_refresh_at = existing.last_quota_next_refresh_at.clone();
    }

    if merged.persona.is_none() {
        merged.persona = existing.persona.clone();
    }

    merged.persona = Some(normalized_persona(&merged));
    merged
}

pub(super) fn normalized_persona(entry: &AccountEntry) -> PersonaEntry {
    let mut hasher = DefaultHasher::new();
    entry.account_id.hash(&mut hasher);
    entry
        .alias
        .as_deref()
        .unwrap_or(entry.label.as_str())
        .hash(&mut hasher);
    let persona_hash = hasher.finish();
    let persona_id = format!(
        "persona-{}-{:08x}",
        sanitize_persona_token(&entry.label),
        (persona_hash & 0xffff_ffff) as u32
    );
    let persona_profile_id = match (persona_hash % 3) as usize {
        0 => "balanced-us-compact",
        1 => "balanced-eu-wide",
        _ => "balanced-apac-standard",
    };
    let expected_region_code = entry
        .persona
        .as_ref()
        .and_then(|persona| persona.expected_region_code.clone());
    PersonaEntry {
        persona_id: entry
            .persona
            .as_ref()
            .map(|persona| persona.persona_id.clone())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(persona_id.clone()),
        persona_profile_id: Some(
            entry
                .persona
                .as_ref()
                .and_then(|persona| persona.persona_profile_id.clone())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| persona_profile_id.to_string()),
        ),
        expected_region_code,
        ready_at: entry
            .persona
            .as_ref()
            .and_then(|persona| persona.ready_at.clone()),
        host_root_rel_path: Some(
            entry
                .persona
                .as_ref()
                .and_then(|persona| persona.host_root_rel_path.clone())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| format!("personas/host/{persona_id}")),
        ),
        vm_package_rel_path: entry
            .persona
            .as_ref()
            .and_then(|persona| persona.vm_package_rel_path.clone()),
        browser_fingerprint: entry
            .persona
            .as_ref()
            .and_then(|persona| persona.browser_fingerprint.clone()),
    }
}

pub(super) fn sanitize_persona_token(value: &str) -> String {
    let normalized = value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
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
        "account".to_string()
    } else {
        compact
    }
}

pub(super) fn apply_auth_to_account(entry: &mut AccountEntry, auth: CodexAuth) -> bool {
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
        || entry.relogin
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
    entry.relogin = false;
    entry.plan_type = next_plan;
    entry.account_id = next_account_id;
    entry.auth = auth;
    changed
}
