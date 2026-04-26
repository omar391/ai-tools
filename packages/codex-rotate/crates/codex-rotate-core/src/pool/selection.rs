use super::*;

pub fn find_next_cached_usable_account_index(
    active_index: usize,
    accounts: &[AccountEntry],
) -> Option<usize> {
    if accounts.len() <= 1 {
        return None;
    }
    for offset in 1..accounts.len() {
        let index = (active_index + offset) % accounts.len();
        if accounts[index].last_quota_usable == Some(true) {
            return Some(index);
        }
    }
    None
}

pub fn find_next_immediate_round_robin_index(
    active_index: usize,
    accounts: &[AccountEntry],
) -> Option<usize> {
    if accounts.len() <= 1 {
        return None;
    }
    for offset in 1..accounts.len() {
        let index = (active_index + offset) % accounts.len();
        let entry = &accounts[index];
        let has_cached_inspection = entry.last_quota_checked_at.is_some();
        if entry.last_quota_usable == Some(true) || !has_cached_inspection {
            return Some(index);
        }
    }
    None
}

pub fn build_reusable_account_probe_order(
    active_index: usize,
    account_count: usize,
    mode: ReusableAccountProbeMode,
) -> Vec<usize> {
    if account_count == 0 {
        return Vec::new();
    }
    let normalized_active_index = active_index.min(account_count - 1);
    let mut others = Vec::new();
    for offset in 1..account_count {
        others.push((normalized_active_index + offset) % account_count);
    }
    match mode {
        ReusableAccountProbeMode::CurrentFirst => {
            let mut order = vec![normalized_active_index];
            order.extend(others);
            order
        }
        ReusableAccountProbeMode::OthersFirst => {
            let mut order = others;
            order.push(normalized_active_index);
            order
        }
        ReusableAccountProbeMode::OthersOnly => others,
    }
}

pub(crate) fn find_next_usable_account(
    pool: &mut Pool,
    auth_path: &Path,
    mode: ReusableAccountProbeMode,
    reasons: &mut Vec<String>,
    dirty: bool,
    skip_indices: &HashSet<usize>,
    disabled_domains: &HashSet<String>,
) -> Result<(Option<RotationCandidate>, bool)> {
    let mut next_dirty = dirty;
    next_dirty |= prune_terminal_accounts_from_pool(pool)?;
    let mut effective_disabled_domains = if next_dirty != dirty {
        load_disabled_rotation_domains()?
    } else {
        disabled_domains.clone()
    };
    let probe_order =
        build_reusable_account_probe_order(pool.active_index, pool.accounts.len(), mode);

    for index in probe_order {
        if index >= pool.accounts.len() {
            continue;
        }
        if skip_indices.contains(&index) {
            continue;
        }
        if account_entry_marked_for_relogin(&pool.accounts[index]) {
            reasons.push(format!(
                "{}: scheduled for relogin",
                pool.accounts[index].label
            ));
            continue;
        }
        if !account_rotation_enabled(&effective_disabled_domains, &pool.accounts[index].email) {
            if let Some(domain) = extract_email_domain(&pool.accounts[index].email) {
                reasons.push(format!(
                    "{}: rotation disabled for {}",
                    pool.accounts[index].label, domain
                ));
            }
            continue;
        }
        let inspection = inspect_account(
            &mut pool.accounts[index],
            auth_path,
            index == pool.active_index,
        )?;
        next_dirty |= inspection.updated;
        if account_requires_terminal_cleanup(&pool.accounts[index]) {
            next_dirty |= cleanup_terminal_account(pool, index)?;
            effective_disabled_domains = load_disabled_rotation_domains()?;
            continue;
        }
        if let Some(usage) = inspection.usage.as_ref() {
            if has_usable_quota(usage) {
                return Ok((
                    Some(RotationCandidate {
                        index,
                        entry: pool.accounts[index].clone(),
                        inspection,
                    }),
                    next_dirty,
                ));
            }
            reasons.push(format!(
                "{}: {}",
                pool.accounts[index].label,
                describe_quota_blocker(usage)
            ));
        } else {
            reasons.push(format!(
                "{}: {}",
                pool.accounts[index].label,
                inspection
                    .error
                    .clone()
                    .unwrap_or_else(|| "unknown error".to_string())
            ));
        }
    }
    Ok((None, next_dirty))
}

pub(crate) fn resolve_account_selector(pool: &Pool, selector: &str) -> Result<AccountSelection> {
    let normalized_selector = selector.trim();
    if normalized_selector.is_empty() {
        return Err(anyhow!("Account selector cannot be empty."));
    }

    let exact_matches = pool
        .accounts
        .iter()
        .enumerate()
        .filter(|(_, entry)| {
            entry.label == normalized_selector
                || entry.alias.as_deref() == Some(normalized_selector)
                || entry.account_id == normalized_selector
                || format_short_account_id(&entry.account_id) == normalized_selector
        })
        .map(|(index, entry)| AccountSelection {
            index,
            entry: entry.clone(),
        })
        .collect::<Vec<_>>();

    if exact_matches.len() == 1 {
        return Ok(exact_matches[0].clone());
    }
    if exact_matches.len() > 1 {
        return Err(anyhow!(
            "Selector \"{}\" matched multiple accounts. Use the full composite key.",
            normalized_selector
        ));
    }

    let normalized_email = normalized_selector.to_lowercase();
    let email_matches = pool
        .accounts
        .iter()
        .enumerate()
        .filter(|(_, entry)| entry.email.to_lowercase() == normalized_email)
        .map(|(index, entry)| AccountSelection {
            index,
            entry: entry.clone(),
        })
        .collect::<Vec<_>>();
    if email_matches.len() == 1 {
        return Ok(email_matches[0].clone());
    }
    if email_matches.len() > 1 {
        return Err(anyhow!(
            "Email \"{}\" matched multiple accounts: {}. Use the full composite key.",
            normalized_selector,
            email_matches
                .iter()
                .map(|selection| selection.entry.label.clone())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Err(anyhow!(
        "Account \"{}\" not found in pool.",
        normalized_selector
    ))
}

#[derive(Clone)]
pub(crate) struct AccountSelection {
    pub index: usize,
    pub entry: AccountEntry,
}

pub(crate) fn format_short_account_id(account_id: &str) -> String {
    if account_id.len() > 8 {
        format!("{}...", &account_id[..8])
    } else {
        account_id.to_string()
    }
}

pub(super) fn format_usage_window(window: &UsageWindow) -> String {
    let left = get_quota_left(Some(window)).unwrap_or(0.0);
    let reset_text = format!(
        " (resets in {})",
        format_duration(window.reset_after_seconds.max(0))
    );
    format!("{} left{}", format_percent(left), reset_text)
}

pub(super) fn format_percent(value: f64) -> String {
    if (value.fract()).abs() < f64::EPSILON {
        format!("{}%", value as i64)
    } else {
        format!("{value:.1}%")
    }
}

pub(super) fn format_duration(total_seconds: i64) -> String {
    if total_seconds <= 0 {
        return "0s".to_string();
    }
    let mut remaining = total_seconds;
    let mut parts = Vec::new();
    for (label, seconds) in [("d", 86_400), ("h", 3_600), ("m", 60), ("s", 1)] {
        let amount = remaining / seconds;
        if amount > 0 {
            parts.push(format!("{amount}{label}"));
            remaining -= amount * seconds;
        }
        if parts.len() == 2 {
            break;
        }
    }
    parts.join(" ")
}

pub(super) fn format_credits_full(credits: Option<&UsageCredits>) -> Option<String> {
    let credits = credits?;
    if credits.unlimited {
        return Some("unlimited".to_string());
    }
    if !credits.has_credits {
        return Some("none".to_string());
    }
    let mut details = Vec::new();
    if let Some(balance) = credits.balance {
        details.push(format!("balance {balance}"));
    }
    if let Some(local) = credits.approx_local_messages {
        details.push(format!("~{local} local msgs"));
    }
    if let Some(cloud) = credits.approx_cloud_messages {
        details.push(format!("~{cloud} cloud msgs"));
    }
    Some(if details.is_empty() {
        "available".to_string()
    } else {
        details.join(", ")
    })
}
