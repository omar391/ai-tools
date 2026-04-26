use super::*;

pub(super) fn resolve_create_template(
    requested_template: Option<&str>,
    discovered_template: Option<&str>,
    default_template: &str,
) -> Result<String> {
    if let Some(requested_template) = requested_template {
        return normalize_template_family(requested_template);
    }
    let trimmed_default = default_template.trim();
    if !trimmed_default.is_empty() {
        return normalize_template_family(trimmed_default);
    }
    if let Some(discovered_template) = discovered_template {
        return normalize_template_family(discovered_template);
    }
    Err(anyhow!(
        "No default_create_template is configured in ~/.codex-rotate/accounts.json."
    ))
}

pub(super) fn resolve_create_template_for_profile(
    store: &CredentialStore,
    profile_name: &str,
    requested_template: Option<&str>,
    alias: Option<&str>,
) -> Result<String> {
    let discovered_template = if requested_template.is_none() {
        select_pending_template_hint_for_profile(store, profile_name, alias)
            .or_else(|| select_stored_template_hint(store, profile_name))
    } else {
        None
    };
    resolve_create_template(
        requested_template,
        discovered_template.as_deref(),
        &store.default_create_template,
    )
}

pub(super) fn make_credential_family_key(profile_name: &str, template: &str) -> Result<String> {
    Ok(format!(
        "{}::{}",
        profile_name,
        normalize_template_family(template)?
    ))
}

pub(super) fn disabled_rotation_error(domain: &str) -> anyhow::Error {
    anyhow!(
        "Rotation is disabled for {} accounts. Set domain[\"{}\"].rotation_enabled to true in ~/.codex-rotate/accounts.json to re-enable them.",
        domain,
        domain
    )
}

pub(super) fn disabled_rotation_domain_in_store(
    store: &CredentialStore,
    domain: &str,
) -> Option<String> {
    let normalized = normalize_domain_key(domain)?;
    store
        .domain
        .get(&normalized)
        .filter(|config| !config.rotation_enabled)
        .map(|_| normalized)
}

pub(super) fn disabled_rotation_domain_for_email_in_store(
    store: &CredentialStore,
    email: &str,
) -> Option<String> {
    extract_email_domain(email).and_then(|domain| disabled_rotation_domain_in_store(store, &domain))
}

pub(super) fn disabled_rotation_domain_for_template_in_store(
    store: &CredentialStore,
    template: &str,
) -> Option<String> {
    parse_email_family(template)
        .ok()
        .and_then(|family| disabled_rotation_domain_in_store(store, &family.domain_part))
}

pub(super) fn ensure_rotation_enabled_for_email_in_store(
    store: &CredentialStore,
    email: &str,
) -> Result<()> {
    if let Some(domain) = disabled_rotation_domain_for_email_in_store(store, email) {
        return Err(disabled_rotation_error(&domain));
    }
    Ok(())
}

pub(super) fn ensure_rotation_enabled_for_template_in_store(
    store: &CredentialStore,
    template: &str,
) -> Result<()> {
    if let Some(domain) = disabled_rotation_domain_for_template_in_store(store, template) {
        return Err(disabled_rotation_error(&domain));
    }
    Ok(())
}

pub(super) fn max_suffix_per_family_for_template_in_store(
    store: &CredentialStore,
    template: &str,
) -> Option<u32> {
    let family = parse_email_family(template).ok()?;
    store
        .domain
        .get(&family.domain_part)
        .and_then(|config| config.max_suffix_per_family)
}

pub(super) fn ensure_suffix_within_domain_limit_in_store(
    store: &CredentialStore,
    template: &str,
    suffix: u32,
) -> Result<()> {
    let Some(max_suffix) = max_suffix_per_family_for_template_in_store(store, template) else {
        return Ok(());
    };
    if suffix <= max_suffix {
        return Ok(());
    }
    Err(anyhow!(
        "Account creation for {} stops at suffix {}. {} would exceed the domain limit.",
        template,
        max_suffix,
        suffix
    ))
}

#[cfg(test)]
pub(super) fn is_rotation_enabled_for_email_in_store(store: &CredentialStore, email: &str) -> bool {
    disabled_rotation_domain_for_email_in_store(store, email).is_none()
}

pub(super) fn normalize_template_family(email: &str) -> Result<String> {
    Ok(parse_email_family(email)?.normalized)
}

pub(super) fn migrate_legacy_template_value(value: &str) -> Result<String> {
    let normalized = value.trim().to_lowercase();
    if normalized.contains(EMAIL_FAMILY_PLACEHOLDER) {
        return normalize_template_family(&normalized);
    }
    let Some((local_part, domain_part)) = normalized.split_once('@') else {
        return Err(anyhow!("\"{}\" is not a valid email family.", value));
    };
    if domain_part != "gmail.com" {
        return Err(anyhow!(
            "\"{}\" is not a valid email template. Use a template like dev.{{n}}@example.com or name+{{n}}@gmail.com.",
            value
        ));
    }
    let base_local = local_part
        .split('+')
        .next()
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| anyhow!("\"{}\" does not contain a valid Gmail local part.", value))?;
    normalize_template_family(&format!(
        "{base_local}+{EMAIL_FAMILY_PLACEHOLDER}@gmail.com"
    ))
}

pub(super) fn parse_email_family(value: &str) -> Result<EmailFamily> {
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
            domain_part,
            prefix,
            suffix,
        });
    }

    if placeholder_count > 1 {
        return Err(anyhow!(
            "\"{}\" may only contain one {} placeholder.",
            value,
            EMAIL_FAMILY_PLACEHOLDER
        ));
    }

    Err(anyhow!(
        "\"{}\" is not a valid email template. Use a template like dev.{{n}}@example.com or name+{{n}}@gmail.com.",
        value
    ))
}

pub(super) fn build_account_family_email(template: &str, suffix: u32) -> Result<String> {
    if suffix < 1 {
        return Err(anyhow!("Invalid email family suffix \"{}\".", suffix));
    }
    let parsed = parse_email_family(template)?;
    Ok(format!(
        "{}{suffix}{}@{}",
        parsed.prefix, parsed.suffix, parsed.domain_part
    ))
}

pub(super) fn extract_account_family_suffix(
    candidate_email: &str,
    template: &str,
) -> Result<Option<u32>> {
    let parsed = parse_email_family(template)?;
    let normalized_candidate = candidate_email.trim().to_lowercase();
    let domain_suffix = format!("@{}", parsed.domain_part);
    if !normalized_candidate.ends_with(&domain_suffix) {
        return Ok(None);
    }
    if parsed.suffix.is_empty() && parsed.prefix.ends_with('+') {
        let bare_prefix = parsed.prefix.trim_end_matches('+');
        if normalized_candidate == format!("{bare_prefix}@{}", parsed.domain_part) {
            return Ok(Some(0));
        }
    }
    let without_domain = normalized_candidate
        .strip_suffix(&domain_suffix)
        .unwrap_or_default();
    let middle = without_domain
        .strip_prefix(&parsed.prefix)
        .and_then(|value| value.strip_suffix(&parsed.suffix));
    Ok(middle
        .filter(|value| {
            !value.is_empty() && value.chars().all(|character| character.is_ascii_digit())
        })
        .and_then(|value| value.parse::<u32>().ok()))
}

#[cfg(test)]
pub(super) fn compute_next_account_family_suffix(
    template: &str,
    known_emails: Vec<String>,
) -> Result<u32> {
    compute_next_account_family_suffix_with_skips(
        template,
        known_emails,
        Vec::new(),
        DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
    )
}

pub(super) fn compute_next_account_family_suffix_with_skips(
    template: &str,
    known_emails: Vec<String>,
    skipped_emails: Vec<String>,
    max_skipped_slots: u32,
) -> Result<u32> {
    let mut used = HashSet::new();
    for email in known_emails {
        if let Some(suffix) = extract_account_family_suffix(&email, template)? {
            used.insert(suffix);
        }
    }
    let mut skipped = HashSet::new();
    for email in skipped_emails {
        if let Some(suffix) = extract_account_family_suffix(&email, template)? {
            skipped.insert(suffix);
        }
    }
    let mut candidate = 1;
    let should_reserve_skipped = (skipped.len() as u32) <= max_skipped_slots;
    while used.contains(&candidate) || (should_reserve_skipped && skipped.contains(&candidate)) {
        candidate += 1;
    }
    Ok(candidate)
}

pub(super) fn compute_fresh_account_family_suffix(
    family: Option<&CredentialFamily>,
    template: &str,
    known_emails: Vec<String>,
    skipped_emails: Vec<String>,
) -> Result<u32> {
    let mut known_suffixes = HashSet::new();
    for email in &known_emails {
        if let Some(suffix) = extract_account_family_suffix(email, template)? {
            known_suffixes.insert(suffix);
        }
    }
    let mut covered_suffixes = known_suffixes.clone();
    for email in &skipped_emails {
        if let Some(suffix) = extract_account_family_suffix(email, template)? {
            covered_suffixes.insert(suffix);
        }
    }
    let should_reserve_skipped = !skipped_emails.is_empty()
        && (skipped_emails.len() as u32) <= max_skipped_slots_for_family(family);
    let computed = compute_next_account_family_suffix_with_skips(
        template,
        known_emails,
        skipped_emails.clone(),
        max_skipped_slots_for_family(family),
    )?;
    if !should_reserve_skipped {
        if let Some(entry) = family {
            let frontier = entry.next_suffix;
            if frontier > computed && (1..frontier).all(|suffix| known_suffixes.contains(&suffix)) {
                return Ok(frontier);
            }
        }
        Ok(computed)
    } else {
        Ok(family
            .map(|entry| entry.next_suffix.max(computed))
            .unwrap_or(computed))
    }
}

pub(super) fn compute_create_attempt_family_suffix(
    family: Option<&CredentialFamily>,
    template: &str,
    mut known_emails: Vec<String>,
    skipped_emails: Vec<String>,
    retry_reserved_emails: &HashSet<String>,
) -> Result<u32> {
    known_emails.extend(retry_reserved_emails.iter().cloned());
    compute_fresh_account_family_suffix(family, template, known_emails, skipped_emails)
}

pub(super) fn collect_known_account_emails(pool: &Pool, store: &CredentialStore) -> Vec<String> {
    let mut emails = pool
        .accounts
        .iter()
        .map(|entry| entry.email.clone())
        .collect::<Vec<_>>();
    emails.extend(store.pending.keys().cloned());
    emails
}

pub(super) fn collect_skipped_account_emails_for_family(
    store: &CredentialStore,
    profile_name: &str,
    template: &str,
) -> Vec<String> {
    let Ok(family_key) = make_credential_family_key(profile_name, template) else {
        return Vec::new();
    };
    store
        .skipped
        .iter()
        .filter(|email| {
            select_family_for_account_email(store, email)
                .map(|matched| matched.key == family_key)
                .unwrap_or_else(|| {
                    extract_account_family_suffix(email, template)
                        .map(|suffix| suffix.is_some())
                        .unwrap_or(false)
                })
        })
        .cloned()
        .collect()
}

pub(super) fn max_skipped_slots_for_family(family: Option<&CredentialFamily>) -> u32 {
    family
        .map(|entry| entry.max_skipped_slots)
        .unwrap_or(DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY)
}

pub(super) fn family_is_selectable_for_create_hint(
    store: &CredentialStore,
    template: &str,
    frontier: u32,
) -> bool {
    disabled_rotation_domain_for_template_in_store(store, template).is_none()
        && max_suffix_per_family_for_template_in_store(store, template)
            .map(|limit| frontier <= limit)
            .unwrap_or(true)
}

pub(super) fn select_pending_credential_for_family(
    store: &CredentialStore,
    profile_name: &str,
    template: &str,
    alias: Option<&str>,
    excluded_emails: &HashSet<String>,
) -> Option<PendingCredential> {
    let normalized_template = normalize_template_family(template).ok()?;
    let normalized_alias = normalize_alias(alias);
    let mut matches = store
        .pending
        .values()
        .filter(|entry| {
            entry.stored.profile_name == profile_name
                && !excluded_emails.contains(&normalize_email_key(&entry.stored.email))
                && normalize_template_family(&entry.stored.template)
                    .map(|value| value == normalized_template)
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

pub(super) fn select_pending_template_hint_for_profile(
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
                && family_is_selectable_for_create_hint(
                    store,
                    &entry.stored.template,
                    entry.stored.suffix.saturating_add(1),
                )
                && (normalized_alias.is_none()
                    || normalize_alias(entry.stored.alias.as_deref()) == normalized_alias)
        })
        .cloned()
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        let left_priority =
            get_create_family_hint_priority(&left.stored.template, left.stored.suffix + 1);
        let right_priority =
            get_create_family_hint_priority(&right.stored.template, right.stored.suffix + 1);
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
        .find_map(|entry| normalize_template_family(&entry.stored.template).ok())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct CreateFamilyHintPriority {
    family_rank: u8,
    frontier: u32,
}

pub(super) fn get_create_family_hint_priority(
    template: &str,
    frontier: u32,
) -> CreateFamilyHintPriority {
    let normalized_frontier = frontier.max(1);
    let family_rank = parse_email_family(template)
        .ok()
        .map(|parsed| {
            if parsed.domain_part == "astronlab.com"
                && parsed.prefix == "dev."
                && parsed.suffix.is_empty()
            {
                2
            } else {
                1
            }
        })
        .unwrap_or(0);
    CreateFamilyHintPriority {
        family_rank,
        frontier: normalized_frontier,
    }
}

#[cfg(test)]
pub(super) fn should_use_default_create_family_hint(template: Option<&str>) -> bool {
    template
        .and_then(|value| parse_email_family(value).ok())
        .map(|_| true)
        .unwrap_or(false)
}

#[cfg(test)]
pub(super) fn normalize_gmail_template(email: &str) -> Result<String> {
    let normalized = normalize_email_candidate(email)
        .ok_or_else(|| anyhow!("\"{}\" is not a valid Gmail address.", email))?;
    let (local_part, domain_part) = normalized
        .split_once('@')
        .ok_or_else(|| anyhow!("\"{}\" is not a valid Gmail address.", email))?;
    if domain_part != "gmail.com" {
        return Err(anyhow!("\"{}\" is not a Gmail address.", email));
    }
    let base_local = local_part
        .split('+')
        .next()
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| anyhow!("\"{}\" does not contain a valid Gmail local part.", email))?;
    Ok(format!("{base_local}@gmail.com"))
}

#[cfg(test)]
pub(super) fn compute_next_gmail_alias_suffix(
    template: &str,
    known_emails: Vec<String>,
) -> Result<u32> {
    compute_next_account_family_suffix(&migrate_legacy_template_value(template)?, known_emails)
}

#[cfg(test)]
pub(super) fn normalize_email_candidate(value: &str) -> Option<String> {
    let trimmed = value.trim().to_lowercase();
    let (local, domain) = trimmed.split_once('@')?;
    if local.is_empty() || domain.is_empty() || domain.starts_with('.') || domain.ends_with('.') {
        return None;
    }
    domain.contains('.').then_some(trimmed)
}

#[cfg(test)]
pub(super) fn extract_supported_gmail_emails(
    emails: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut supported = Vec::new();
    for email in emails {
        let Ok(normalized) = normalize_gmail_template(&email) else {
            continue;
        };
        if seen.insert(normalized.clone()) {
            supported.push(normalized);
        }
    }
    supported
}

#[cfg(test)]
pub(super) fn tokenize_managed_profile_name(profile_name: &str) -> Vec<String> {
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
pub(super) fn score_email_for_managed_profile_name(profile_name: &str, email: &str) -> i32 {
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
pub(super) fn select_best_email_for_managed_profile(
    profile_name: &str,
    emails: impl IntoIterator<Item = String>,
    preferred_template: Option<&str>,
) -> Option<String> {
    let normalized_preferred =
        preferred_template.and_then(|value| normalize_gmail_template(value).ok());
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

pub(super) fn select_stored_template_hint(
    store: &CredentialStore,
    profile_name: &str,
) -> Option<String> {
    let mut candidates = HashMap::<String, (u32, i64, u32)>::new();

    let mut remember = |raw_email: Option<&str>, updated_at: Option<&str>, frontier: u32| {
        let Some(raw_email) = raw_email else {
            return;
        };
        let Ok(template) = normalize_template_family(raw_email) else {
            return;
        };
        let entry = candidates.entry(template).or_insert((0, 0, 1));
        entry.0 += 1;
        entry.1 = entry.1.max(parse_sortable_timestamp(updated_at));
        entry.2 = entry.2.max(frontier.max(1));
    };

    for family in store.families.values() {
        if family.profile_name == profile_name {
            if !family_is_selectable_for_create_hint(store, &family.template, family.next_suffix) {
                continue;
            }
            remember(
                Some(&family.template),
                Some(family.updated_at.as_str()),
                family.next_suffix,
            );
        }
    }
    for pending in store.pending.values() {
        if pending.stored.profile_name == profile_name {
            if !family_is_selectable_for_create_hint(
                store,
                &pending.stored.template,
                pending.stored.suffix.saturating_add(1),
            ) {
                continue;
            }
            remember(
                Some(&pending.stored.template),
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
        .map(|(template, _)| template)
}

#[cfg(test)]
pub(super) fn select_best_system_chrome_profile_match(
    profile_name: &str,
    profiles: &[SystemChromeProfileCandidate],
    preferred_template: Option<&str>,
) -> Option<SystemChromeProfileMatch> {
    let normalized_preferred =
        preferred_template.and_then(|value| normalize_gmail_template(value).ok());
    profiles
        .iter()
        .filter_map(|profile| {
            let matched_email = select_best_email_for_managed_profile(
                profile_name,
                profile.emails.clone(),
                preferred_template,
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

pub(super) fn resolve_relogin_credential(
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
        template: family_match.family.template.clone(),
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
pub(super) struct FamilyAccountMatch {
    pub(super) key: String,
    pub(super) family: CredentialFamily,
    pub(super) suffix: u32,
}

pub(super) fn select_family_for_account_email(
    store: &CredentialStore,
    email: &str,
) -> Option<FamilyAccountMatch> {
    let normalized_email = normalize_email_key(email);
    let mut matches = store
        .families
        .iter()
        .filter_map(|(key, family)| {
            extract_account_family_suffix(&normalized_email, &family.template)
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

pub(super) fn upsert_family_for_account(
    store: &mut CredentialStore,
    account: &StoredCredential,
) -> bool {
    let Ok(family_key) = make_credential_family_key(&account.profile_name, &account.template)
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
            existing
                .relogin
                .retain(|email| normalize_email_key(email) != normalize_email_key(&account.email));
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
                    template: account.template.clone(),
                    next_suffix,
                    max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
                    created_at: next_created_at,
                    updated_at: next_updated_at,
                    last_created_email: next_last_created_email,
                    relogin: Vec::new(),
                    suspend_domain_on_terminal_refresh_failure: false,
                },
            );
            true
        }
    }
}

pub(super) fn merge_legacy_account_into_families(
    families: &mut HashMap<String, CredentialFamily>,
    account: &StoredCredential,
) {
    let Ok(family_key) = make_credential_family_key(&account.profile_name, &account.template)
    else {
        return;
    };
    let updated_at = parse_sortable_timestamp(Some(account.updated_at.as_str()));
    let created_at = parse_sortable_timestamp(Some(account.created_at.as_str()));
    match families.get_mut(&family_key) {
        Some(existing) => {
            existing.next_suffix = existing.next_suffix.max(account.suffix.saturating_add(1));
            existing
                .relogin
                .retain(|email| normalize_email_key(email) != normalize_email_key(&account.email));
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
                    template: account.template.clone(),
                    next_suffix: account.suffix.saturating_add(1),
                    max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
                    created_at: account.created_at.clone(),
                    updated_at: account.updated_at.clone(),
                    last_created_email: Some(account.email.clone()),
                    relogin: Vec::new(),
                    suspend_domain_on_terminal_refresh_failure: false,
                },
            );
        }
    }
}

pub(super) fn parse_sortable_timestamp(value: Option<&str>) -> i64 {
    value
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.timestamp_millis())
        .unwrap_or(0)
}

pub(super) fn normalize_email_key(email: &str) -> String {
    email.trim().to_lowercase()
}
