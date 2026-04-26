use super::*;

pub(super) fn load_credential_store() -> Result<CredentialStore> {
    let _ = migrate_legacy_credential_store_if_needed()?;
    let raw = load_rotate_state_json()?;
    let needs_migration = rotate_state_requires_template_migration(&raw);
    let mut store = normalize_credential_store(raw);
    let reactivated_domains = reactivate_elapsed_domains(&mut store.domain, Utc::now());
    if needs_migration || reactivated_domains {
        save_credential_store(&store)?;
    }
    Ok(store)
}

pub(super) fn reactivate_elapsed_domains(
    domains: &mut HashMap<String, DomainConfig>,
    now: DateTime<Utc>,
) -> bool {
    let mut changed = false;
    for config in domains.values_mut() {
        if config.rotation_enabled {
            if config.reactivate_at.is_some() {
                config.reactivate_at = None;
                changed = true;
            }
            continue;
        }
        let Some(reactivate_at) = config.reactivate_at.as_deref() else {
            continue;
        };
        let reactivate_at = parse_sortable_timestamp(Some(reactivate_at));
        if reactivate_at > 0 && reactivate_at <= now.timestamp_millis() {
            config.rotation_enabled = true;
            config.reactivate_at = None;
            changed = true;
        }
    }
    changed
}

pub fn auto_disable_domain_for_account(email: &str) -> Result<bool> {
    let Some(domain) = extract_email_domain(email) else {
        return Ok(false);
    };
    let mut store = load_credential_store()?;
    let reactivate_at = (Utc::now() + chrono::Duration::days(AUTO_DOMAIN_REACTIVATION_DAYS))
        .to_rfc3339_opts(SecondsFormat::Millis, true);
    let config = store.domain.entry(domain).or_insert(DomainConfig {
        rotation_enabled: true,
        max_suffix_per_family: None,
        reactivate_at: None,
    });
    let changed =
        config.rotation_enabled || config.reactivate_at.as_deref() != Some(reactivate_at.as_str());
    config.rotation_enabled = false;
    config.reactivate_at = Some(reactivate_at);
    if changed {
        save_credential_store(&store)?;
    }
    Ok(changed)
}

pub fn load_disabled_rotation_domains() -> Result<HashSet<String>> {
    Ok(load_credential_store()?
        .domain
        .into_iter()
        .filter_map(|(domain, config)| (!config.rotation_enabled).then_some(domain))
        .collect())
}

pub fn load_relogin_account_emails() -> Result<HashSet<String>> {
    Ok(load_credential_store()?
        .families
        .into_values()
        .flat_map(|family| family.relogin.into_iter())
        .map(|email| normalize_email_key(&email))
        .collect())
}

pub(super) fn save_credential_store(store: &CredentialStore) -> Result<()> {
    let credential_state = serialize_credential_store(store);
    let mut dropped_non_dev_pending = Vec::new();
    update_rotate_state_json(RotateStateOwner::CredentialStore, |state| {
        dropped_non_dev_pending = normalize_pending_credential_map(state.get("pending"))
            .into_values()
            .filter(|record| should_drop_non_dev_pending_credential(&record.stored.template))
            .filter(|record| {
                !store
                    .pending
                    .contains_key(&normalize_email_key(&record.stored.email))
            })
            .collect::<Vec<_>>();
        if !state.is_object() {
            *state = Value::Object(Map::new());
        }
        let object = state
            .as_object_mut()
            .expect("rotate state must be a JSON object");
        object.remove("default_create_base_email");
        if let Some(version) = credential_state.get("version").cloned() {
            object.insert("version".to_string(), version);
        }
        if let Some(default_create_template) =
            credential_state.get("default_create_template").cloned()
        {
            object.insert(
                "default_create_template".to_string(),
                default_create_template,
            );
        }
        if store.domain.is_empty() {
            object.remove("domain");
        } else if let Some(domain) = credential_state.get("domain").cloned() {
            object.insert("domain".to_string(), domain);
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
        if store.skipped.is_empty() {
            object.remove("skipped");
        } else if let Some(skipped) = credential_state.get("skipped").cloned() {
            object.insert("skipped".to_string(), skipped);
        }
        Ok(())
    })?;
    cleanup_dropped_non_dev_pending_secrets(&dropped_non_dev_pending);
    Ok(())
}

pub(crate) fn migrate_rotate_state_credential_sections(raw: &Value) -> Option<Value> {
    if !rotate_state_requires_template_migration(raw) {
        return None;
    }

    let store = normalize_credential_store(raw.clone());
    let credential_state = serialize_credential_store(&store);
    let mut migrated = raw.clone();
    if !migrated.is_object() {
        migrated = Value::Object(Map::new());
    }
    let object = migrated
        .as_object_mut()
        .expect("rotate state must be a JSON object");
    object.remove("default_create_base_email");
    if let Some(version) = credential_state.get("version").cloned() {
        object.insert("version".to_string(), version);
    }
    if let Some(default_create_template) = credential_state.get("default_create_template").cloned()
    {
        object.insert(
            "default_create_template".to_string(),
            default_create_template,
        );
    } else {
        object.remove("default_create_template");
    }
    if store.domain.is_empty() {
        object.remove("domain");
    } else if let Some(domain) = credential_state.get("domain").cloned() {
        object.insert("domain".to_string(), domain);
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
    if store.skipped.is_empty() {
        object.remove("skipped");
    } else if let Some(skipped) = credential_state.get("skipped").cloned() {
        object.insert("skipped".to_string(), skipped);
    }
    Some(migrated)
}

pub(super) fn cleanup_dropped_non_dev_pending_secrets(records: &[PendingCredential]) {
    let Ok(paths) = resolve_paths() else {
        return;
    };
    let Ok(pool) = load_pool() else {
        return;
    };
    let current_account = pool.accounts.get(pool.active_index);
    let persona_paths = current_account
        .and_then(|entry| entry.persona.as_ref())
        .map(|persona| host_persona_root(&paths, persona).join("managed-profile"));
    let profile_dir = persona_paths.as_ref().map(|path| path.to_string_lossy());

    for record in records {
        let result = run_automation_bridge::<_, bool>(
            "delete-account-secret-ref",
            BridgeDeleteSecretPayload {
                profile_name: &record.stored.profile_name,
                profile_dir: profile_dir.as_ref().map(|s| s.as_ref()),
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

pub(super) fn host_persona_root(
    paths: &crate::paths::CorePaths,
    persona: &PersonaEntry,
) -> PathBuf {
    persona
        .host_root_rel_path
        .as_deref()
        .map(|relative| paths.rotate_home.join(relative))
        .unwrap_or_else(|| {
            paths
                .rotate_home
                .join("personas")
                .join("host")
                .join(&persona.persona_id)
        })
}

pub(super) fn current_persona_managed_profile_dir(ensure_exists: bool) -> Result<Option<PathBuf>> {
    let paths = resolve_paths()?;
    let pool = load_pool()?;
    let profile_dir = pool
        .accounts
        .get(pool.active_index)
        .and_then(|entry| entry.persona.as_ref())
        .map(|persona| host_persona_root(&paths, persona).join("managed-profile"));

    if ensure_exists {
        if let Some(path) = profile_dir.as_ref() {
            fs::create_dir_all(path)
                .with_context(|| format!("Failed to create {}.", path.display()))?;
        }
    }

    Ok(profile_dir)
}

pub(super) fn rotate_state_requires_template_migration(raw: &Value) -> bool {
    let raw_version = raw
        .get("version")
        .and_then(Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
        .unwrap_or_default();
    if raw_version < ROTATE_STATE_VERSION || raw.get("default_create_base_email").is_some() {
        return true;
    }
    if raw
        .get("default_create_template")
        .and_then(Value::as_str)
        .map(template_value_requires_migration)
        .unwrap_or(false)
    {
        return true;
    }
    if raw
        .get("families")
        .and_then(Value::as_object)
        .map(|families| {
            families.iter().any(|(key, value)| {
                if value.get("base_email").is_some() {
                    return true;
                }
                let Some(profile_name) = value.get("profile_name").and_then(Value::as_str) else {
                    return false;
                };
                let Some(template) = value
                    .get("template")
                    .or_else(|| value.get("base_email"))
                    .and_then(Value::as_str)
                else {
                    return false;
                };
                if template_value_requires_migration(template) {
                    return true;
                }
                migrate_legacy_template_value(template)
                    .ok()
                    .and_then(|template| make_credential_family_key(profile_name, &template).ok())
                    .map(|normalized_key| normalized_key != *key)
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
    {
        return true;
    }
    raw.get("pending")
        .and_then(Value::as_object)
        .map(|pending| {
            pending.values().any(|value| {
                value.get("base_email").is_some()
                    || value
                        .get("template")
                        .or_else(|| value.get("base_email"))
                        .and_then(Value::as_str)
                        .map(template_value_requires_migration)
                        .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

pub(super) fn normalize_credential_store(raw: Value) -> CredentialStore {
    let inventory_emails = collect_inventory_emails_from_state(&raw);
    let raw_version = raw
        .get("version")
        .and_then(Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
        .unwrap_or_default();
    let explicit_default_create_template = raw
        .get("default_create_template")
        .or_else(|| raw.get("default_create_base_email"))
        .and_then(Value::as_str)
        .and_then(|value| migrate_legacy_template_value(value).ok());
    let domain = raw
        .get("domain")
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(key, value)| {
                    normalize_domain_key(key).and_then(|domain| {
                        serde_json::from_value::<DomainConfig>(value.clone())
                            .ok()
                            .map(|record| (domain, record))
                    })
                })
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();
    let migrate_legacy_non_default_families = raw_version < ROTATE_STATE_VERSION
        || raw.get("default_create_template").is_none()
        || raw.get("default_create_base_email").is_some();
    let mut families = raw
        .get("families")
        .and_then(Value::as_object)
        .map(|map| {
            let mut families = HashMap::new();
            for value in map.values() {
                let Some(record) = normalize_credential_family(value) else {
                    continue;
                };
                merge_normalized_family(&mut families, record);
            }
            families
        })
        .unwrap_or_default();
    let legacy_accounts = normalize_stored_credential_map(raw.get("accounts"));
    for account in legacy_accounts.values() {
        merge_legacy_account_into_families(&mut families, account);
    }
    let mut pending = normalize_pending_credential_map(raw.get("pending"))
        .into_iter()
        .filter(|(email, record)| {
            !inventory_emails.contains(email)
                && (!migrate_legacy_non_default_families
                    || !should_drop_non_dev_pending_credential(&record.stored.template))
        })
        .collect::<HashMap<_, _>>();
    let migration_default_create_template = explicit_default_create_template
        .clone()
        .or_else(|| infer_default_create_template_from_store_records(&families, &pending))
        .unwrap_or_default();
    if migrate_legacy_non_default_families {
        families.retain(|_, family| {
            !should_drop_legacy_non_default_family(
                &family.template,
                &migration_default_create_template,
            )
        });
    }
    if migrate_legacy_non_default_families {
        pending.retain(|_, record| {
            !should_drop_legacy_non_default_family(
                &record.stored.template,
                &migration_default_create_template,
            )
        });
    }
    let skipped = normalize_email_set(raw.get("skipped"))
        .into_iter()
        .filter(|email| !inventory_emails.contains(email) && !pending.contains_key(email))
        .collect::<HashSet<_>>();
    let default_create_template = migration_default_create_template;

    CredentialStore {
        version: ROTATE_STATE_VERSION,
        default_create_template,
        domain,
        families,
        pending,
        skipped,
    }
}

pub(super) fn infer_default_create_template_from_store_records(
    families: &HashMap<String, CredentialFamily>,
    pending: &HashMap<String, PendingCredential>,
) -> Option<String> {
    let mut candidates = HashMap::<String, (u32, i64, u32)>::new();

    let mut remember = |template: &str, updated_at: Option<&str>, frontier: u32| {
        let Ok(normalized) = normalize_template_family(template) else {
            return;
        };
        let entry = candidates.entry(normalized).or_insert((0, 0, 1));
        entry.0 += 1;
        entry.1 = entry.1.max(parse_sortable_timestamp(updated_at));
        entry.2 = entry.2.max(frontier.max(1));
    };

    for family in families.values() {
        remember(
            &family.template,
            Some(family.updated_at.as_str()),
            family.next_suffix,
        );
    }
    for record in pending.values() {
        remember(
            &record.stored.template,
            record
                .started_at
                .as_deref()
                .or(Some(record.stored.updated_at.as_str())),
            record.stored.suffix.saturating_add(1),
        );
    }

    candidates
        .into_iter()
        .max_by(|left, right| {
            let left_priority = get_create_family_hint_priority(&left.0, left.1 .2);
            let right_priority = get_create_family_hint_priority(&right.0, right.1 .2);
            left_priority
                .cmp(&right_priority)
                .then_with(|| left.1 .0.cmp(&right.1 .0))
                .then_with(|| left.1 .1.cmp(&right.1 .1))
                .then_with(|| left.0.cmp(&right.0))
        })
        .map(|(template, _)| template)
}

pub(super) fn should_drop_legacy_non_default_family(
    template: &str,
    default_template: &str,
) -> bool {
    let Ok(normalized_template) = migrate_legacy_template_value(template) else {
        return false;
    };
    if normalized_template == default_template {
        return false;
    }
    let Ok(parsed) = parse_email_family(&normalized_template) else {
        return false;
    };
    if parsed.domain_part != "astronlab.com" {
        return false;
    }
    parsed.prefix.starts_with("bench") || parsed.prefix.contains("devicefix")
}

pub(super) fn should_drop_non_dev_pending_credential(template: &str) -> bool {
    parse_email_family(template)
        .map(|parsed| parsed.domain_part == "astronlab.com" && parsed.prefix != "dev.")
        .unwrap_or(true)
}

pub(super) fn collect_inventory_emails_from_state(raw: &Value) -> HashSet<String> {
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

pub(super) fn normalize_stored_credential_map(
    raw: Option<&Value>,
) -> HashMap<String, StoredCredential> {
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

pub(super) fn normalize_credential_family(raw: &Value) -> Option<CredentialFamily> {
    let mut record = serde_json::from_value::<CredentialFamily>(raw.clone()).ok()?;
    record.template = migrate_legacy_template_value(&record.template).ok()?;
    record.relogin = record
        .relogin
        .iter()
        .map(|email| normalize_email_key(email))
        .collect::<Vec<_>>();
    record.relogin.sort();
    record.relogin.dedup();
    record.suspend_domain_on_terminal_refresh_failure = raw
        .as_object()
        .is_some_and(|family| family.contains_key("deleted"))
        || record.suspend_domain_on_terminal_refresh_failure;
    record.last_created_email = record
        .last_created_email
        .as_deref()
        .map(normalize_email_key);
    Some(record)
}

pub(super) fn normalize_pending_credential_map(
    raw: Option<&Value>,
) -> HashMap<String, PendingCredential> {
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

pub(super) fn merge_normalized_family(
    families: &mut HashMap<String, CredentialFamily>,
    family: CredentialFamily,
) {
    let Ok(family_key) = make_credential_family_key(&family.profile_name, &family.template) else {
        return;
    };
    match families.get_mut(&family_key) {
        Some(existing) => {
            existing.next_suffix = existing.next_suffix.max(family.next_suffix);
            existing.max_skipped_slots = existing.max_skipped_slots.max(family.max_skipped_slots);
            existing.relogin.extend(family.relogin.iter().cloned());
            existing.relogin.sort();
            existing.relogin.dedup();
            existing.suspend_domain_on_terminal_refresh_failure |=
                family.suspend_domain_on_terminal_refresh_failure;
            if parse_sortable_timestamp(Some(family.created_at.as_str()))
                < parse_sortable_timestamp(Some(existing.created_at.as_str()))
                || existing.created_at.trim().is_empty()
            {
                existing.created_at = family.created_at.clone();
            }
            if parse_sortable_timestamp(Some(family.updated_at.as_str()))
                >= parse_sortable_timestamp(Some(existing.updated_at.as_str()))
            {
                existing.updated_at = family.updated_at.clone();
                existing.last_created_email = family.last_created_email.clone();
            }
        }
        None => {
            families.insert(family_key, family);
        }
    }
}

pub(super) fn normalize_email_set(raw: Option<&Value>) -> HashSet<String> {
    raw.and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(Value::as_str)
                .map(normalize_email_key)
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default()
}

pub(super) fn template_value_requires_migration(value: &str) -> bool {
    normalize_template_family(value).is_err() && migrate_legacy_template_value(value).is_ok()
}

pub(super) fn normalize_stored_credential(raw: &Value) -> Option<StoredCredential> {
    let mut record = serde_json::from_value::<StoredCredential>(raw.clone()).ok()?;
    record.template = migrate_legacy_template_value(&record.template).ok()?;
    Some(record)
}

pub(super) fn normalize_pending_credential(raw: &Value) -> Option<PendingCredential> {
    let object = raw.as_object()?;
    Some(PendingCredential {
        stored: normalize_stored_credential(raw)?,
        started_at: object
            .get("started_at")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

pub(super) fn serialize_credential_store(store: &CredentialStore) -> Value {
    let pending = store
        .pending
        .iter()
        .map(|(email, record)| (email.clone(), serialize_pending_credential(record)))
        .collect::<Map<String, Value>>();
    let mut skipped = store.skipped.iter().cloned().collect::<Vec<_>>();
    skipped.sort();
    json!({
        "version": ROTATE_STATE_VERSION,
        "default_create_template": store.default_create_template,
        "domain": store.domain,
        "families": store.families,
        "pending": pending,
        "skipped": skipped,
    })
}

pub(super) fn serialize_pending_credential(record: &PendingCredential) -> Value {
    let mut value = serialize_stored_credential(&record.stored)
        .as_object()
        .cloned()
        .unwrap_or_default();
    if let Some(started_at) = record.started_at.as_ref() {
        value.insert("started_at".to_string(), Value::String(started_at.clone()));
    }
    Value::Object(value)
}

pub(super) fn serialize_stored_credential(record: &StoredCredential) -> Value {
    let mut object = Map::new();
    object.insert("email".to_string(), Value::String(record.email.clone()));
    object.insert(
        "profile_name".to_string(),
        Value::String(record.profile_name.clone()),
    );
    object.insert(
        "template".to_string(),
        Value::String(record.template.clone()),
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

pub fn record_removed_account(email: &str) -> Result<bool> {
    let normalized_email = normalize_email_key(email);
    let mut store = load_credential_store()?;
    let Some(family_match) = select_family_for_account_email(&store, &normalized_email) else {
        let removed_pending = store.pending.remove(&normalized_email).is_some();
        let removed_skipped = store.skipped.remove(&normalized_email);
        if removed_pending || removed_skipped {
            save_credential_store(&store)?;
            return Ok(true);
        }
        return Ok(false);
    };

    let removed_pending = store.pending.remove(&normalized_email).is_some();
    let removed_skipped = store.skipped.remove(&normalized_email);
    let mut dirty = removed_pending || removed_skipped;
    if let Some(family) = store.families.get_mut(&family_match.key) {
        if !family
            .relogin
            .iter()
            .any(|entry| normalize_email_key(entry) == normalized_email)
        {
            family.relogin.push(normalized_email.clone());
            family.relogin.sort();
            family.relogin.dedup();
            dirty = true;
        }
    }
    if dirty {
        save_credential_store(&store)?;
    }
    Ok(dirty)
}

pub(crate) fn family_suspends_domain_on_terminal_refresh_failure(email: &str) -> Result<bool> {
    let store = load_credential_store()?;
    Ok(select_family_for_account_email(&store, email)
        .map(|family_match| {
            family_match
                .family
                .suspend_domain_on_terminal_refresh_failure
        })
        .unwrap_or(false))
}

pub fn extract_email_domain(email: &str) -> Option<String> {
    let normalized = normalize_email_key(email);
    let (_, domain) = normalized.split_once('@')?;
    normalize_domain_key(domain)
}

pub(super) fn normalize_domain_key(domain: &str) -> Option<String> {
    let normalized = domain.trim().trim_matches('.').to_lowercase();
    if normalized.is_empty() || !normalized.contains('.') {
        return None;
    }
    Some(normalized)
}

pub(super) fn normalize_alias(alias: Option<&str>) -> Option<String> {
    alias
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub(super) fn generate_password(length: usize) -> String {
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

pub(super) fn pick_random_char(source: &[u8]) -> char {
    let mut bytes = [0u8; 8];
    OsRng.fill_bytes(&mut bytes);
    let index = u64::from_le_bytes(bytes) as usize % source.len();
    source[index] as char
}

pub(super) fn fisher_yates_shuffle(chars: &mut [char]) {
    for index in (1..chars.len()).rev() {
        let mut bytes = [0u8; 8];
        OsRng.fill_bytes(&mut bytes);
        let swap_index = u64::from_le_bytes(bytes) as usize % (index + 1);
        chars.swap(index, swap_index);
    }
}

pub(super) fn resolve_credential_birth_date(
    credential: Option<&StoredCredential>,
    fallback_birth_date: Option<&AdultBirthDate>,
) -> Option<AdultBirthDate> {
    if let Some(credential) = credential {
        if let (Some(birth_month), Some(birth_day), Some(birth_year)) = (
            credential.birth_month,
            credential.birth_day,
            credential.birth_year,
        ) {
            return Some(AdultBirthDate {
                birth_month,
                birth_day,
                birth_year,
            });
        }
    }

    fallback_birth_date.cloned()
}
