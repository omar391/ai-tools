use super::*;

pub(super) fn apply_usage_to_account(entry: &mut AccountEntry, usage: &UsageResponse) -> bool {
    let next_email =
        if usage.email.is_empty() || should_preserve_expected_email(&entry.email, &usage.email) {
            entry.email.clone()
        } else {
            usage.email.clone()
        };
    let next_plan = if usage.plan_type.is_empty() {
        entry.plan_type.clone()
    } else {
        usage.plan_type.clone()
    };
    let next_label = build_account_label(&next_email, &next_plan);
    let next_alias = normalize_alias(entry.alias.as_deref());

    let changed = entry.label != next_label
        || entry.alias != next_alias
        || entry.email != next_email
        || entry.plan_type != next_plan;

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
    entry.plan_type = next_plan;
    changed
}

pub(super) fn write_codex_auth_if_current_account(
    auth_path: &Path,
    entry: &AccountEntry,
) -> Result<bool> {
    if !auth_path.exists() {
        return Ok(false);
    }
    let current_auth = load_codex_auth(auth_path)?;
    if !account_entry_matches_auth_identity(entry, &current_auth) {
        return Ok(false);
    }
    if current_auth != entry.auth {
        write_codex_auth(auth_path, &entry.auth)?;
        return Ok(true);
    }
    Ok(false)
}

pub(super) fn apply_quota_inspection_to_account(
    entry: &mut AccountEntry,
    inspection: &AccountInspection,
    checked_at: &str,
) -> bool {
    let checked_at_value = DateTime::parse_from_rfc3339(checked_at)
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    let next_usable = inspection.usage.as_ref().map(has_usable_quota);
    let next_summary = inspection.usage.as_ref().map(format_compact_quota);
    let next_primary_left_percent = inspection.usage.as_ref().and_then(|usage| {
        get_quota_left(
            usage
                .rate_limit
                .as_ref()
                .and_then(|limits| limits.primary_window.as_ref()),
        )
        .map(|value| value.round() as u8)
    });
    let next_refresh_at = quota_next_refresh_at(
        inspection.usage.as_ref(),
        inspection.error.as_deref(),
        checked_at_value,
    )
    .to_rfc3339_opts(SecondsFormat::Millis, true);
    let next_blocker = inspection
        .usage
        .as_ref()
        .map(|usage| {
            if has_usable_quota(usage) {
                String::new()
            } else {
                describe_quota_blocker(usage)
            }
        })
        .and_then(|value| if value.is_empty() { None } else { Some(value) })
        .or_else(|| inspection.error.clone());

    let changed = entry.last_quota_usable != next_usable
        || entry.last_quota_summary != next_summary
        || entry.last_quota_blocker != next_blocker
        || entry.last_quota_checked_at.as_deref() != Some(checked_at)
        || entry.last_quota_primary_left_percent != next_primary_left_percent
        || entry.last_quota_next_refresh_at.as_deref() != Some(next_refresh_at.as_str());

    entry.last_quota_usable = next_usable;
    entry.last_quota_summary = next_summary;
    entry.last_quota_blocker = next_blocker;
    entry.last_quota_checked_at = Some(checked_at.to_string());
    entry.last_quota_primary_left_percent = next_primary_left_percent;
    entry.last_quota_next_refresh_at = Some(next_refresh_at);
    changed
}

pub(crate) fn inspect_account(
    entry: &mut AccountEntry,
    auth_path: &Path,
    persist_if_current: bool,
) -> Result<AccountInspection> {
    let inspected_at = now_iso();
    let inspection = match fetch_usage_with_recovery(&entry.auth) {
        Ok((auth, usage, _)) => {
            let mut updated = apply_auth_to_account(entry, auth.clone());
            updated |= apply_usage_to_account(entry, &usage);
            let inspection = AccountInspection {
                usage: Some(usage),
                error: None,
                updated: false,
            };
            updated |= apply_quota_inspection_to_account(entry, &inspection, &inspected_at);
            if persist_if_current {
                updated |= write_codex_auth_if_current_account(auth_path, entry)?;
            }
            AccountInspection {
                updated,
                ..inspection
            }
        }
        Err(error) => {
            let inspection = AccountInspection {
                usage: None,
                error: Some(error.to_string()),
                updated: false,
            };
            let updated = apply_quota_inspection_to_account(entry, &inspection, &inspected_at);
            AccountInspection {
                updated,
                ..inspection
            }
        }
    };
    Ok(inspection)
}

pub(super) fn fetch_usage_with_recovery(
    auth: &CodexAuth,
) -> Result<(CodexAuth, UsageResponse, bool)> {
    let mut working_auth = auth.clone();
    let mut refreshed = false;

    if is_token_expired(&working_auth.tokens.access_token, 60) {
        working_auth = refresh_auth(&working_auth)?;
        refreshed = true;
    }

    match fetch_usage_once(&working_auth) {
        Ok(usage) => Ok((working_auth, usage, refreshed)),
        Err(error) => {
            if refreshed
                || !error
                    .downcast_ref::<HttpError>()
                    .map(|value| value.status == 401)
                    .unwrap_or(false)
            {
                return Err(error);
            }
            working_auth = refresh_auth(&working_auth)?;
            let usage = fetch_usage_once(&working_auth)?;
            Ok((working_auth, usage, true))
        }
    }
}

pub(super) fn fetch_usage_once(auth: &CodexAuth) -> Result<UsageResponse> {
    let usage_url = std::env::var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE")
        .unwrap_or_else(|_| WHAM_USAGE_URL.to_string());
    let response = core_http_client()
        .get(&usage_url)
        .header("Accept", "application/json")
        .header(
            "Authorization",
            format!("Bearer {}", auth.tokens.access_token),
        )
        .header("ChatGPT-Account-Id", extract_account_id_from_auth(auth))
        .header("User-Agent", "codex-rotate-cli-rs")
        .send()
        .with_context(|| format!("Usage lookup failed: {usage_url}"))?;
    let status = response.status();
    let body = response.text().unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!(HttpError {
            status: status.as_u16(),
            message: build_http_error_message("Usage lookup", status.as_u16(), &body),
        }));
    }
    serde_json::from_str(&body).context("Usage lookup returned invalid JSON.")
}

pub(super) fn refresh_auth(auth: &CodexAuth) -> Result<CodexAuth> {
    let refresh_token = auth
        .tokens
        .refresh_token
        .as_ref()
        .ok_or_else(|| anyhow!("No refresh token is available for this account."))?;
    let response = core_http_client()
        .post(
            std::env::var("CODEX_REFRESH_TOKEN_URL_OVERRIDE")
                .unwrap_or_else(|_| OAUTH_TOKEN_URL.to_string()),
        )
        .header("Accept", "application/json")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("User-Agent", "codex-rotate-rs")
        .form(&[
            ("client_id", extract_client_id_from_auth(auth)),
            ("grant_type", "refresh_token".to_string()),
            ("refresh_token", refresh_token.clone()),
        ])
        .send()
        .context("Token refresh failed.")?;
    let status = response.status();
    let raw = response.text().unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!(HttpError {
            status: status.as_u16(),
            message: build_http_error_message("Token refresh", status.as_u16(), &raw),
        }));
    }
    let token_response: OAuthTokenResponse =
        serde_json::from_str(&raw).context("Token refresh returned invalid JSON.")?;
    let access_token = token_response
        .access_token
        .ok_or_else(|| anyhow!("Token refresh response did not include an access token."))?;
    let refreshed_id_token = token_response.id_token.clone();
    Ok(CodexAuth {
        auth_mode: auth.auth_mode.clone(),
        openai_api_key: auth.openai_api_key.clone(),
        tokens: crate::auth::AuthTokens {
            access_token: access_token.clone(),
            id_token: refreshed_id_token
                .clone()
                .unwrap_or_else(|| auth.tokens.id_token.clone()),
            refresh_token: token_response
                .refresh_token
                .or_else(|| auth.tokens.refresh_token.clone()),
            account_id: extract_account_id_from_token(&access_token)
                .or_else(|| {
                    refreshed_id_token
                        .as_deref()
                        .and_then(extract_account_id_from_token)
                })
                .unwrap_or_else(|| auth.tokens.account_id.clone()),
        },
        last_refresh: now_iso(),
    })
}

pub(super) fn build_http_error_message(action: &str, status: u16, body: &str) -> String {
    if let Ok(parsed) = serde_json::from_str::<Value>(body) {
        if let Some(value) = parsed.get("error_description").and_then(Value::as_str) {
            return format!("{action} failed ({status}): {value}");
        }
        if let Some(value) = parsed.get("error").and_then(Value::as_str) {
            return format!("{action} failed ({status}): {value}");
        }
        if let Some(value) = parsed.get("message").and_then(Value::as_str) {
            return format!("{action} failed ({status}): {value}");
        }
        if let Some(error) = parsed.get("error").and_then(Value::as_object) {
            let code = error.get("code").and_then(Value::as_str);
            let message = error.get("message").and_then(Value::as_str);
            if code == Some("refresh_token_reused") {
                return format!(
                    "{action} failed ({status}): refresh token already rotated; sign in again"
                );
            }
            if let (Some(code), Some(message)) = (code, message) {
                return format!("{action} failed ({status}): {code}: {message}");
            }
            if let Some(message) = message {
                return format!("{action} failed ({status}): {message}");
            }
            if let Some(code) = code {
                return format!("{action} failed ({status}): {code}");
            }
        }
    }
    format!("{action} failed ({status})")
}

pub(super) fn core_http_client() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        Client::builder()
            .timeout(std::time::Duration::from_secs(REQUEST_TIMEOUT_SECONDS))
            .build()
            .expect("failed to build codex-rotate core HTTP client")
    })
}
