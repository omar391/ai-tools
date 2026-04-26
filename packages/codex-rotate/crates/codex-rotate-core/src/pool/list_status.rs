use super::*;

pub(super) const LIST_QUOTA_REFRESH_LIMIT_ENV: &str = "CODEX_ROTATE_LIST_QUOTA_REFRESH_LIMIT";
pub(super) const DEFAULT_LIST_QUOTA_REFRESH_LIMIT: usize = 4;

struct LineEmitter<'a> {
    writer: Option<&'a mut dyn Write>,
    lines: Vec<String>,
}

impl<'a> LineEmitter<'a> {
    fn buffered() -> Self {
        Self {
            writer: None,
            lines: Vec::new(),
        }
    }

    fn streaming(writer: &'a mut dyn Write) -> Self {
        Self {
            writer: Some(writer),
            lines: Vec::new(),
        }
    }

    fn push_line(&mut self, line: impl Into<String>) -> Result<()> {
        let line = line.into();
        if let Some(writer) = self.writer.as_deref_mut() {
            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
            writer.flush()?;
        } else {
            self.lines.push(line);
        }
        Ok(())
    }

    fn finish(self) -> String {
        self.lines.join("\n")
    }
}

pub fn cmd_list() -> Result<String> {
    let mut emitter = LineEmitter::buffered();
    cmd_list_impl(&mut emitter)?;
    Ok(emitter.finish())
}

// TODO: expose a structured healthy-account list so callers can use this logic directly
// instead of scraping the rendered account-pool text.
pub fn cmd_list_stream(writer: &mut dyn Write) -> Result<()> {
    let mut emitter = LineEmitter::streaming(writer);
    cmd_list_impl(&mut emitter)
}

fn cmd_list_impl(output: &mut LineEmitter<'_>) -> Result<()> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let listed_at = Utc::now();
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    dirty |= prune_terminal_accounts_from_pool(&mut pool)?;
    if pool.accounts.is_empty() {
        output.push_line(format!(
            "{YELLOW}WARN{RESET} No accounts in pool. Add one with: codex-rotate add"
        ))?;
        if dirty {
            save_pool(&pool)?;
        }
        return Ok(());
    }
    let disabled_domains = load_disabled_rotation_domains()?;
    let refresh_order = build_list_quota_refresh_order(&pool, listed_at);
    let refresh_indices = refresh_order
        .into_iter()
        .take(list_quota_refresh_limit())
        .collect::<HashSet<_>>();
    let display_order = build_list_account_display_order(&pool);

    let mut usable_count = 0;
    let mut exhausted_count = 0;
    let mut unavailable_count = 0;
    let mut healthy_account_sections = Vec::new();
    output.push_line(String::new())?;
    let visible_count = pool
        .accounts
        .iter()
        .filter(|entry| inventory_account_visible(&disabled_domains, entry))
        .count();
    output.push_line(format!(
        "{BOLD}Codex OAuth Account Pool{RESET} ({} account(s))",
        visible_count
    ))?;
    output.push_line(String::new())?;
    output.push_line(format!("{BOLD}Total Accounts{RESET}"))?;
    output.push_line(String::new())?;

    for index in display_order {
        if index >= pool.accounts.len() {
            continue;
        }
        if !inventory_account_visible(&disabled_domains, &pool.accounts[index]) {
            continue;
        }
        let is_active = index == pool.active_index;
        let account_header_line = build_list_account_header_line(&pool.accounts[index], is_active);
        output.push_line(account_header_line.clone())?;

        if refresh_indices.contains(&index)
            && account_quota_refresh_due_for_list(&pool.accounts[index], listed_at)
        {
            let inspection =
                inspect_account(&mut pool.accounts[index], &paths.codex_auth_file, is_active)?;
            dirty |= inspection.updated;
            if account_requires_terminal_cleanup(&pool.accounts[index]) {
                dirty |= cleanup_terminal_account(&mut pool, index)?;
                continue;
            }
        }

        let quota_line = format_cached_quota_line(&pool.accounts[index]);
        let mut account_lines = vec![account_header_line];
        let account_detail_lines =
            build_list_account_detail_lines(&pool.accounts[index], &quota_line);
        for line in &account_detail_lines {
            output.push_line(line.clone())?;
        }
        account_lines.extend(account_detail_lines);

        let is_healthy = matches!(pool.accounts[index].last_quota_usable, Some(true));
        match pool.accounts[index].last_quota_usable {
            Some(true) => usable_count += 1,
            Some(false) => exhausted_count += 1,
            None => unavailable_count += 1,
        }
        if is_healthy {
            healthy_account_sections.push(account_lines);
        }
    }

    if dirty {
        save_pool(&pool)?;
    }
    if usable_count == 0 {
        let mut details = Vec::new();
        if exhausted_count > 0 {
            details.push(format!("{exhausted_count} exhausted"));
        }
        if unavailable_count > 0 {
            details.push(format!("{unavailable_count} unavailable"));
        }
        output.push_line(format!(
            "{YELLOW}WARN{RESET} All accounts are exhausted or unavailable{}.",
            if details.is_empty() {
                String::new()
            } else {
                format!(" ({})", details.join(", "))
            }
        ))?;
    }
    output.push_line(String::new())?;
    output.push_line(format!(
        "{BOLD}Healthy Accounts{RESET} ({} account(s))",
        usable_count
    ))?;
    output.push_line(String::new())?;
    if healthy_account_sections.is_empty() {
        output.push_line(format!("  {DIM}No healthy accounts.{RESET}"))?;
    } else {
        for account_lines in healthy_account_sections {
            for line in account_lines {
                output.push_line(line)?;
            }
        }
    }
    output.push_line(String::new())?;
    Ok(())
}

pub(super) fn build_list_account_header_line(entry: &AccountEntry, is_active: bool) -> String {
    let label = if is_active {
        format!("{BOLD}{}{RESET}", entry.label)
    } else {
        entry.label.clone()
    };
    format!(
        "  {} {}  {CYAN}{}{RESET}  {DIM}{}{RESET}  {DIM}{}{RESET}",
        if is_active {
            format!("{GREEN}>{RESET}")
        } else {
            " ".to_string()
        },
        label,
        entry.email,
        entry.plan_type,
        format_short_account_id(&entry.account_id)
    )
}

pub(super) fn build_list_account_detail_lines(
    entry: &AccountEntry,
    quota_line: &str,
) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(alias) = entry.alias.as_ref() {
        lines.push(format!("    {DIM}alias{RESET}  {}", alias));
    }
    let quota_detail_line = if let Some(next_refresh_at) = format_list_quota_refresh_eta(entry) {
        format!(
            "    {DIM}quota{RESET}  {} {DIM}| next refresh{RESET} {}",
            quota_line, next_refresh_at
        )
    } else {
        format!("    {DIM}quota{RESET}  {}", quota_line)
    };
    lines.push(quota_detail_line);
    lines
}

pub(super) fn format_cached_quota_line(entry: &AccountEntry) -> String {
    let checked_suffix = entry
        .last_quota_checked_at
        .as_deref()
        .map(|value| format!(" {DIM}(cached {value}){RESET}"))
        .unwrap_or_default();

    if let Some(summary) = entry.last_quota_summary.as_deref() {
        return format!("{summary}{checked_suffix}");
    }

    if let Some(blocker) = entry.last_quota_blocker.as_deref() {
        return format!("unavailable ({blocker}){checked_suffix}");
    }

    if entry.last_quota_checked_at.is_some() {
        return format!("unavailable (quota probe failed){checked_suffix}");
    }

    "unknown (run codex-rotate status or rotate to refresh)".to_string()
}

pub(super) fn format_list_quota_refresh_eta(entry: &AccountEntry) -> Option<String> {
    effective_cached_quota_next_refresh_at(entry)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
}

pub(super) fn build_list_account_display_order(pool: &Pool) -> Vec<usize> {
    let mut indices = (0..pool.accounts.len()).collect::<Vec<_>>();
    indices.sort_by(|left, right| {
        let left_eta = effective_cached_quota_next_refresh_at(&pool.accounts[*left]);
        let right_eta = effective_cached_quota_next_refresh_at(&pool.accounts[*right]);
        left_eta
            .is_none()
            .cmp(&right_eta.is_none())
            .then_with(|| left_eta.cmp(&right_eta))
            .then_with(|| left.cmp(right))
    });
    indices
}

pub(super) fn build_list_quota_refresh_order(pool: &Pool, now: DateTime<Utc>) -> Vec<usize> {
    let mut refreshes = pool
        .accounts
        .iter()
        .enumerate()
        .filter(|(_, entry)| entry.last_quota_checked_at.is_none())
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    refreshes.sort_by(|left, right| {
        let left_priority = if *left == pool.active_index { 0 } else { 1 };
        let right_priority = if *right == pool.active_index { 0 } else { 1 };
        left_priority
            .cmp(&right_priority)
            .then_with(|| left.cmp(right))
    });

    let mut candidates = pool
        .accounts
        .iter()
        .enumerate()
        .filter(|(_, entry)| entry.last_quota_checked_at.is_some())
        .filter(|(_, entry)| cached_quota_state_is_stale(entry, now))
        .map(|(index, entry)| {
            let priority = if index == pool.active_index {
                0
            } else if entry.last_quota_usable == Some(true) {
                1
            } else {
                2
            };
            (index, priority, cached_quota_checked_at(entry))
        })
        .collect::<Vec<_>>();

    candidates.sort_by(|left, right| {
        left.1
            .cmp(&right.1)
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| left.0.cmp(&right.0))
    });

    refreshes.extend(candidates.into_iter().map(|(index, _, _)| index));

    refreshes
}

pub(super) fn list_quota_refresh_limit() -> usize {
    std::env::var(LIST_QUOTA_REFRESH_LIMIT_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_LIST_QUOTA_REFRESH_LIMIT)
}

pub(super) fn cached_quota_state_is_stale(entry: &AccountEntry, now: DateTime<Utc>) -> bool {
    let Some(next_refresh_at) = effective_cached_quota_next_refresh_at(entry) else {
        return true;
    };
    now >= next_refresh_at
}

pub(super) fn account_quota_refresh_due_for_list(entry: &AccountEntry, now: DateTime<Utc>) -> bool {
    entry.last_quota_checked_at.is_none() || cached_quota_state_is_stale(entry, now)
}

pub(super) fn cached_quota_checked_at(entry: &AccountEntry) -> Option<DateTime<Utc>> {
    entry
        .last_quota_checked_at
        .as_deref()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

pub(super) fn cached_quota_next_refresh_at(entry: &AccountEntry) -> Option<DateTime<Utc>> {
    entry
        .last_quota_next_refresh_at
        .as_deref()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

pub(super) fn effective_cached_quota_next_refresh_at(
    entry: &AccountEntry,
) -> Option<DateTime<Utc>> {
    if let Some(next_refresh_at) = cached_quota_next_refresh_at(entry) {
        return Some(next_refresh_at);
    }
    let checked_at = cached_quota_checked_at(entry)?;
    legacy_cached_quota_next_refresh_at(entry, checked_at)
        .or_else(|| Some(checked_at + cached_quota_refresh_interval(entry)))
}

pub(super) fn cached_quota_refresh_interval(entry: &AccountEntry) -> Duration {
    match entry.last_quota_usable {
        Some(true) => match entry.last_quota_primary_left_percent.unwrap_or(0) {
            value if value > 20 => Duration::seconds(60),
            value if value > 10 => Duration::seconds(30),
            _ => Duration::seconds(15),
        },
        Some(false) | None => Duration::seconds(15),
    }
}

pub(super) fn legacy_cached_quota_next_refresh_at(
    entry: &AccountEntry,
    checked_at: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    if entry.last_quota_usable != Some(false) {
        return None;
    }
    let blocker = entry.last_quota_blocker.as_deref()?;
    let reset_text = blocker.split("resets in ").nth(1)?.trim();
    Some(checked_at + parse_compact_duration(reset_text)?)
}

pub(super) fn parse_compact_duration(value: &str) -> Option<Duration> {
    let mut seconds = 0_i64;
    for part in value.split_whitespace() {
        if part.len() < 2 {
            return None;
        }
        let (amount, unit) = part.split_at(part.len() - 1);
        let amount = amount.parse::<i64>().ok()?;
        seconds += match unit {
            "d" => amount.saturating_mul(86_400),
            "h" => amount.saturating_mul(3_600),
            "m" => amount.saturating_mul(60),
            "s" => amount,
            _ => return None,
        };
    }
    Some(Duration::seconds(seconds))
}

pub fn cmd_status() -> Result<String> {
    let mut emitter = LineEmitter::buffered();
    cmd_status_impl(&mut emitter)?;
    Ok(emitter.finish())
}

pub fn current_pool_overview() -> Result<PoolOverview> {
    current_pool_overview_with_activation(true)
}

pub fn current_pool_overview_without_activation() -> Result<PoolOverview> {
    current_pool_overview_with_activation(false)
}

pub(super) fn current_pool_overview_with_activation(
    activate_current: bool,
) -> Result<PoolOverview> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |=
        sync_pool_current_auth_from_codex(&mut pool, &paths.codex_auth_file, activate_current)?;
    dirty |= prune_terminal_accounts_from_pool(&mut pool)?;
    if dirty {
        save_pool(&pool)?;
    }
    let disabled_domains = load_disabled_rotation_domains()?;
    let visible_indices = pool
        .accounts
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| {
            inventory_account_visible(&disabled_domains, entry).then_some(index)
        })
        .collect::<Vec<_>>();
    Ok(PoolOverview {
        inventory_count: visible_indices.len(),
        inventory_active_slot: visible_indices
            .iter()
            .position(|index| *index == pool.active_index)
            .map(|slot| slot.saturating_add(1)),
        inventory_healthy_count: visible_indices
            .iter()
            .filter(|index| pool.accounts[**index].last_quota_usable == Some(true))
            .count(),
    })
}

pub fn cmd_status_stream(writer: &mut dyn Write) -> Result<()> {
    let mut emitter = LineEmitter::streaming(writer);
    cmd_status_impl(&mut emitter)
}

fn cmd_status_impl(output: &mut LineEmitter<'_>) -> Result<()> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    dirty |= prune_terminal_accounts_from_pool(&mut pool)?;
    let mut live_pool_index = None;

    output.push_line(String::new())?;
    output.push_line(format!("{BOLD}Codex Rotate Status{RESET}"))?;
    output.push_line(String::new())?;

    if paths.codex_auth_file.exists() {
        let auth = load_codex_auth(&paths.codex_auth_file)?;
        let email = extract_email_from_auth(&auth);
        let plan = extract_plan_from_auth(&auth);
        let account_id = extract_account_id_from_auth(&auth);
        output.push_line(format!(
            "  {BOLD}Auth file target:{RESET} {CYAN}{}{RESET}  ({})",
            email, plan
        ))?;
        output.push_line(format!("  {BOLD}Account ID:{RESET}       {}", account_id))?;
        output.push_line(format!(
            "  {BOLD}Last refresh:{RESET}     {}",
            auth.last_refresh
        ))?;

        live_pool_index = find_pool_account_index_by_identity(&pool, &account_id, &email, &plan);

        if let Some(index) = live_pool_index {
            let inspection =
                inspect_account(&mut pool.accounts[index], &paths.codex_auth_file, true)?;
            dirty |= inspection.updated;
            if account_requires_terminal_cleanup(&pool.accounts[index]) {
                dirty |= cleanup_terminal_account(&mut pool, index)?;
                live_pool_index = None;
                output.push_line(format!(
                    "  {BOLD}Quota:{RESET}            unavailable ({})",
                    inspection
                        .error
                        .unwrap_or_else(|| "unknown error".to_string())
                ))?;
            } else if let Some(usage) = inspection.usage.as_ref() {
                if let Some(window) = usage
                    .rate_limit
                    .as_ref()
                    .and_then(|limits| limits.primary_window.as_ref())
                {
                    output.push_line(format!(
                        "  {BOLD}Quota (5h):{RESET}       {}",
                        format_usage_window(window)
                    ))?;
                }
                if let Some(window) = usage
                    .rate_limit
                    .as_ref()
                    .and_then(|limits| limits.secondary_window.as_ref())
                {
                    output.push_line(format!(
                        "  {BOLD}Quota (week):{RESET}     {}",
                        format_usage_window(window)
                    ))?;
                }
                if let Some(window) = usage
                    .code_review_rate_limit
                    .as_ref()
                    .and_then(|limits| limits.primary_window.as_ref())
                {
                    output.push_line(format!(
                        "  {BOLD}Code review:{RESET}      {}",
                        format_usage_window(window)
                    ))?;
                }
                if let Some(credits) = format_credits_full(usage.credits.as_ref()) {
                    output.push_line(format!("  {BOLD}Credits:{RESET}          {}", credits))?;
                }
            } else {
                output.push_line(format!(
                    "  {BOLD}Quota:{RESET}            unavailable ({})",
                    inspection
                        .error
                        .unwrap_or_else(|| "unknown error".to_string())
                ))?;
            }
        } else {
            match fetch_usage_with_recovery(&auth) {
                Ok((refreshed_auth, usage, refreshed)) => {
                    if refreshed {
                        write_codex_auth(&paths.codex_auth_file, &refreshed_auth)?;
                    }
                    if let Some(window) = usage
                        .rate_limit
                        .as_ref()
                        .and_then(|limits| limits.primary_window.as_ref())
                    {
                        output.push_line(format!(
                            "  {BOLD}Quota (5h):{RESET}       {}",
                            format_usage_window(window)
                        ))?;
                    }
                    if let Some(window) = usage
                        .rate_limit
                        .as_ref()
                        .and_then(|limits| limits.secondary_window.as_ref())
                    {
                        output.push_line(format!(
                            "  {BOLD}Quota (week):{RESET}     {}",
                            format_usage_window(window)
                        ))?;
                    }
                    if let Some(window) = usage
                        .code_review_rate_limit
                        .as_ref()
                        .and_then(|limits| limits.primary_window.as_ref())
                    {
                        output.push_line(format!(
                            "  {BOLD}Code review:{RESET}      {}",
                            format_usage_window(window)
                        ))?;
                    }
                    if let Some(credits) = format_credits_full(usage.credits.as_ref()) {
                        output
                            .push_line(format!("  {BOLD}Credits:{RESET}          {}", credits))?;
                    }
                }
                Err(error) => {
                    output.push_line(format!(
                        "  {BOLD}Quota:{RESET}            unavailable ({})",
                        error
                    ))?;
                }
            }
        }
    } else {
        output.push_line(format!("{YELLOW}WARN{RESET} No Codex auth file found."))?;
    }

    output.push_line(format!(
        "\n  {BOLD}Pool file:{RESET}        {}",
        paths.pool_file.display()
    ))?;
    output.push_line(format!(
        "  {BOLD}Pool size:{RESET}        {} account(s)",
        pool.accounts.len()
    ))?;

    if let Some(index) = live_pool_index {
        if let Some(active) = pool.accounts.get(index) {
            output.push_line(format!(
                "  {BOLD}Active slot:{RESET}      {} [{}/{}]",
                active.label,
                index + 1,
                pool.accounts.len()
            ))?;
            if let Some(alias) = &active.alias {
                output.push_line(format!("  {BOLD}Active alias:{RESET}     {}", alias))?;
            }
        }
    } else if paths.codex_auth_file.exists() {
        output.push_line(format!(
            "  {BOLD}Active slot:{RESET}      {YELLOW}not in pool{RESET}"
        ))?;
        if let Some(active) = pool.accounts.get(pool.active_index) {
            output.push_line(format!(
                "  {BOLD}Pool pointer:{RESET}     {} [{}/{}]",
                active.label,
                pool.active_index + 1,
                pool.accounts.len()
            ))?;
            if let Some(alias) = &active.alias {
                output.push_line(format!("  {BOLD}Pointer alias:{RESET}    {}", alias))?;
            }
        }
    } else if let Some(active) = pool.accounts.get(pool.active_index) {
        output.push_line(format!(
            "  {BOLD}Active slot:{RESET}      {} [{}/{}]",
            active.label,
            pool.active_index + 1,
            pool.accounts.len()
        ))?;
        if let Some(alias) = &active.alias {
            output.push_line(format!("  {BOLD}Active alias:{RESET}     {}", alias))?;
        }
    }

    if dirty {
        save_pool(&pool)?;
    }
    output.push_line(String::new())?;
    Ok(())
}
