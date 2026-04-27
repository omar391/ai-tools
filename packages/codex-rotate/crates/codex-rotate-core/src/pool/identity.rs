use super::*;

pub(super) fn extract_email_from_auth(auth: &CodexAuth) -> String {
    if let Ok(payload) = decode_jwt_payload(&auth.tokens.access_token) {
        if let Some(email) = payload
            .get("https://api.openai.com/profile")
            .and_then(Value::as_object)
            .and_then(|profile| profile.get("email"))
            .and_then(Value::as_str)
        {
            return email.to_string();
        }
    }
    if let Ok(payload) = decode_jwt_payload(&auth.tokens.id_token) {
        if let Some(email) = payload.get("email").and_then(Value::as_str) {
            return email.to_string();
        }
    }
    "unknown".to_string()
}

pub(crate) fn extract_plan_from_auth(auth: &CodexAuth) -> String {
    decode_jwt_payload(&auth.tokens.access_token)
        .ok()
        .and_then(|payload| {
            payload
                .get("https://api.openai.com/auth")
                .and_then(Value::as_object)
                .and_then(|auth| auth.get("chatgpt_plan_type"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| "unknown".to_string())
}

pub(super) fn extract_client_id_from_auth(auth: &CodexAuth) -> String {
    if let Ok(payload) = decode_jwt_payload(&auth.tokens.access_token) {
        if let Some(client_id) = payload.get("client_id").and_then(Value::as_str) {
            return client_id.to_string();
        }
    }
    if let Ok(payload) = decode_jwt_payload(&auth.tokens.id_token) {
        if let Some(audience) = payload.get("aud") {
            if let Some(values) = audience.as_array() {
                if let Some(client_id) = values.first().and_then(Value::as_str) {
                    return client_id.to_string();
                }
            }
        }
    }
    DEFAULT_OAUTH_CLIENT_ID.to_string()
}

pub(super) fn normalize_email_for_label(email: &str) -> String {
    let normalized = email.trim().to_lowercase();
    if normalized.is_empty() {
        "unknown".to_string()
    } else {
        normalized
    }
}

pub(crate) fn normalize_plan_type_for_label(plan_type: &str) -> String {
    let normalized = plan_type
        .trim()
        .to_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
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
        "unknown".to_string()
    } else {
        compact
    }
}

pub(super) fn build_account_label(email: &str, plan_type: &str) -> String {
    format!(
        "{}_{}",
        normalize_email_for_label(email),
        normalize_plan_type_for_label(plan_type)
    )
}

pub(super) fn normalize_identity_email(email: &str) -> Option<String> {
    let normalized = email.trim().to_lowercase();
    if normalized.is_empty() || normalized == "unknown" {
        None
    } else {
        Some(normalized)
    }
}

pub(super) fn normalize_identity_plan_type(plan_type: &str) -> Option<String> {
    let normalized = normalize_plan_type_for_label(plan_type);
    if normalized == "unknown" {
        None
    } else {
        Some(normalized)
    }
}

pub(super) fn should_preserve_expected_email(existing_email: &str, auth_email: &str) -> bool {
    let normalized_existing = existing_email.trim().to_lowercase();
    let normalized_auth = auth_email.trim().to_lowercase();
    let existing_is_gmail_plus = normalized_existing.ends_with("@gmail.com")
        && normalized_existing
            .split_once('@')
            .map(|(local, _)| local.contains('+'))
            .unwrap_or(false);
    !normalized_existing.is_empty()
        && normalized_existing != "unknown"
        && normalized_existing != normalized_auth
        && normalized_auth.ends_with("@gmail.com")
        && (!normalized_existing.ends_with("@gmail.com") || existing_is_gmail_plus)
}

pub(super) fn account_entry_matches_email_plan(
    entry: &AccountEntry,
    email: &str,
    plan_type: &str,
) -> bool {
    let target_email = normalize_identity_email(email);
    let entry_email = normalize_identity_email(&entry.email);
    let target_plan = normalize_identity_plan_type(plan_type);
    let entry_plan = normalize_identity_plan_type(&entry.plan_type);

    if target_email.is_none() || entry_email.as_deref() != target_email.as_deref() {
        return false;
    }

    match (entry_plan.as_deref(), target_plan.as_deref()) {
        (Some(existing_plan), Some(target_plan)) => existing_plan == target_plan,
        _ => true,
    }
}

pub(crate) fn account_entry_matches_identity(
    entry: &AccountEntry,
    account_id: &str,
    email: &str,
    plan_type: &str,
) -> bool {
    let normalized_account_id = account_id.trim();
    let has_matching_account_id = !normalized_account_id.is_empty()
        && (entry.account_id == normalized_account_id
            || entry.auth.tokens.account_id == normalized_account_id);
    if !has_matching_account_id {
        return false;
    }

    let target_email = normalize_identity_email(email);
    let entry_email = normalize_identity_email(&entry.email);
    let target_plan = normalize_identity_plan_type(plan_type);
    let entry_plan = normalize_identity_plan_type(&entry.plan_type);

    if let (Some(existing_plan), Some(target_plan)) =
        (entry_plan.as_deref(), target_plan.as_deref())
    {
        if existing_plan != target_plan {
            return false;
        }
    }

    match (entry_email.as_deref(), target_email.as_deref()) {
        (_, None) => true,
        (None, Some(_)) => true,
        (Some(existing_email), Some(target_email)) => {
            existing_email == target_email
                || should_preserve_expected_email(existing_email, target_email)
                || should_preserve_expected_email(target_email, existing_email)
        }
    }
}

pub(crate) fn account_entry_matches_auth_identity(entry: &AccountEntry, auth: &CodexAuth) -> bool {
    account_entry_matches_identity(
        entry,
        &extract_account_id_from_auth(auth),
        &extract_email_from_auth(auth),
        &extract_plan_from_auth(auth),
    )
}

pub(super) fn normalize_alias(alias: Option<&str>) -> Option<String> {
    alias.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

pub(super) fn get_account_summary(entry: &AccountEntry) -> String {
    match &entry.alias {
        Some(alias) => format!("{} ({alias})", entry.label),
        None => entry.label.clone(),
    }
}

pub(crate) fn format_account_summary_for_display(entry: &AccountEntry) -> String {
    get_account_summary(entry)
}
