use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};

use crate::auth::{summarize_codex_auth, CodexAuth};

const WHAM_USAGE_URL: &str = "https://chatgpt.com/backend-api/wham/usage";
const REQUEST_TIMEOUT_SECONDS: u64 = 8;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UsageWindow {
    pub used_percent: f64,
    pub limit_window_seconds: i64,
    pub reset_after_seconds: i64,
    pub reset_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UsageRateLimit {
    pub allowed: bool,
    pub limit_reached: bool,
    pub primary_window: Option<UsageWindow>,
    pub secondary_window: Option<UsageWindow>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UsageCredits {
    pub has_credits: bool,
    pub unlimited: bool,
    pub balance: Option<i64>,
    pub approx_local_messages: Option<i64>,
    pub approx_cloud_messages: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UsageResponse {
    pub user_id: String,
    pub account_id: String,
    pub email: String,
    pub plan_type: String,
    pub rate_limit: Option<UsageRateLimit>,
    pub code_review_rate_limit: Option<UsageRateLimit>,
    pub additional_rate_limits: Option<serde_json::Value>,
    pub credits: Option<UsageCredits>,
    pub promo: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct QuotaAssessment {
    pub usage: UsageResponse,
    pub usable: bool,
    pub summary: String,
    pub blocker: Option<String>,
    pub primary_quota_left_percent: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CachedQuotaState {
    pub account_id: String,
    pub fetched_at: String,
    pub next_refresh_at: String,
    pub summary: String,
    pub usable: bool,
    pub blocker: Option<String>,
    pub primary_quota_left_percent: Option<u8>,
    pub error: Option<String>,
}

pub fn inspect_quota(auth: &CodexAuth) -> Result<QuotaAssessment> {
    let summary = summarize_codex_auth(auth);
    let usage_url = std::env::var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE")
        .unwrap_or_else(|_| WHAM_USAGE_URL.to_string());
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(REQUEST_TIMEOUT_SECONDS))
        .build()
        .context("Failed to build quota probe client.")?;
    let response = client
        .get(&usage_url)
        .header("Accept", "application/json")
        .header("Authorization", format!("Bearer {}", auth.tokens.access_token))
        .header("ChatGPT-Account-Id", summary.account_id)
        .header("User-Agent", "codex-rotate-rs")
        .send()
        .with_context(|| format!("Usage lookup failed: {usage_url}"))?;
    let status = response.status();
    let body = response.text().unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!(
            "Usage lookup failed ({}): {}",
            status.as_u16(),
            if body.is_empty() {
                status.canonical_reason().unwrap_or("unknown error").to_string()
            } else {
                body
            }
        ));
    }

    let usage: UsageResponse =
        serde_json::from_str(&body).context("Usage lookup returned invalid JSON.")?;
    let usable = has_usable_quota(&usage);
    Ok(QuotaAssessment {
        summary: format_quota_summary(&usage),
        blocker: if usable {
            None
        } else {
            Some(describe_quota_blocker(&usage))
        },
        primary_quota_left_percent: get_quota_left(
            usage.rate_limit
                .as_ref()
                .and_then(|limits| limits.primary_window.as_ref()),
        ),
        usage,
        usable,
    })
}

pub fn get_quota_left(window: Option<&UsageWindow>) -> Option<f64> {
    let window = window?;
    Some((100.0 - window.used_percent).clamp(0.0, 100.0))
}

pub fn has_usable_quota(usage: &UsageResponse) -> bool {
    let primary_left = get_quota_left(
        usage.rate_limit
            .as_ref()
            .and_then(|limits| limits.primary_window.as_ref()),
    );
    if usage
        .rate_limit
        .as_ref()
        .map(|limits| limits.allowed)
        .unwrap_or(false)
        && primary_left.map(|value| value > 0.0).unwrap_or(false)
    {
        return true;
    }

    usage.credits
        .as_ref()
        .map(|credits| credits.unlimited || credits.has_credits)
        .unwrap_or(false)
}

pub fn describe_quota_blocker(usage: &UsageResponse) -> String {
    let primary = usage
        .rate_limit
        .as_ref()
        .and_then(|limits| limits.primary_window.as_ref());
    let primary_left = get_quota_left(primary);
    if primary_left.map(|value| value <= 0.0).unwrap_or(false) {
        let label = format_window_label(primary, "current");
        let reset = primary
            .and_then(|window| format_reset_suffix(window.reset_after_seconds));
        return format!("{} quota exhausted{}", label, reset.unwrap_or_default());
    }
    if usage
        .rate_limit
        .as_ref()
        .map(|limits| limits.limit_reached || !limits.allowed)
        .unwrap_or(false)
    {
        return "usage limit reached".to_string();
    }
    "no usable quota".to_string()
}

pub fn format_quota_summary(usage: &UsageResponse) -> String {
    let mut parts = Vec::new();
    if let Some(text) = format_usage_window(
        usage.rate_limit
            .as_ref()
            .and_then(|limits| limits.primary_window.as_ref()),
        "primary",
    ) {
        parts.push(text);
    }
    if let Some(text) = format_usage_window(
        usage.rate_limit
            .as_ref()
            .and_then(|limits| limits.secondary_window.as_ref()),
        "secondary",
    ) {
        parts.push(text);
    }
    if let Some(text) = format_credits(usage.credits.as_ref()) {
        parts.push(text);
    }
    if parts.is_empty() {
        "quota unavailable".to_string()
    } else {
        parts.join(" | ")
    }
}

pub fn format_compact_quota(usage: &UsageResponse) -> String {
    let mut parts = Vec::new();
    if let Some(window) = usage
        .rate_limit
        .as_ref()
        .and_then(|limits| limits.primary_window.as_ref())
    {
        parts.push(format!(
            "5h {}",
            format_usage_window_value(window, true)
        ));
    }
    if let Some(window) = usage
        .rate_limit
        .as_ref()
        .and_then(|limits| limits.secondary_window.as_ref())
    {
        parts.push(format!(
            "week {}",
            format_usage_window_value(window, true)
        ));
    }
    if let Some(text) = format_credits_compact(usage.credits.as_ref()) {
        parts.push(format!("credits {}", text));
    }
    if parts.is_empty() {
        "unavailable".to_string()
    } else {
        parts.join(" | ")
    }
}

pub fn quota_cache_ttl(assessment: Option<&QuotaAssessment>, error: Option<&str>) -> chrono::Duration {
    if error.is_some() {
        return chrono::Duration::seconds(30);
    }
    let Some(assessment) = assessment else {
        return chrono::Duration::seconds(30);
    };
    if !assessment.usable {
        return chrono::Duration::seconds(30);
    }
    match assessment.primary_quota_left_percent.unwrap_or(0.0) {
        value if value > 20.0 => chrono::Duration::minutes(5),
        value if value > 10.0 => chrono::Duration::seconds(90),
        _ => chrono::Duration::seconds(30),
    }
}

pub fn build_cached_quota_state(
    account_id: &str,
    assessment: Option<&QuotaAssessment>,
    error: Option<&str>,
    fetched_at: DateTime<Utc>,
) -> CachedQuotaState {
    let ttl = quota_cache_ttl(assessment, error);
    CachedQuotaState {
        account_id: account_id.to_string(),
        fetched_at: fetched_at.to_rfc3339_opts(SecondsFormat::Millis, true),
        next_refresh_at: (fetched_at + ttl).to_rfc3339_opts(SecondsFormat::Millis, true),
        summary: assessment
            .map(|value| value.summary.clone())
            .unwrap_or_else(|| "quota unavailable".to_string()),
        usable: assessment.map(|value| value.usable).unwrap_or(false),
        blocker: assessment
            .and_then(|value| value.blocker.clone())
            .or_else(|| error.map(ToOwned::to_owned)),
        primary_quota_left_percent: assessment
            .and_then(|value| value.primary_quota_left_percent.map(|percent| percent.round() as u8)),
        error: error.map(ToOwned::to_owned),
    }
}

pub fn quota_cache_is_stale(cache: Option<&CachedQuotaState>, account_id: &str, now: DateTime<Utc>) -> bool {
    let Some(cache) = cache else {
        return true;
    };
    if cache.account_id != account_id {
        return true;
    }
    let Ok(next_refresh_at) = DateTime::parse_from_rfc3339(&cache.next_refresh_at) else {
        return true;
    };
    now >= next_refresh_at.with_timezone(&Utc)
}

fn format_usage_window(window: Option<&UsageWindow>, fallback_label: &str) -> Option<String> {
    let window = window?;
    let left = get_quota_left(Some(window))?;
    let label = format_window_label(Some(window), fallback_label);
    let reset = format_reset_suffix(window.reset_after_seconds)
        .map(|value| format!(", {}", value.trim_start_matches(", ")))
        .unwrap_or_default();
    Some(format!("{} {} left{}", label, format_percent(left), reset))
}

fn format_usage_window_value(window: &UsageWindow, compact: bool) -> String {
    let left = get_quota_left(Some(window)).unwrap_or(0.0);
    let reset_text = if compact {
        format_reset_suffix(window.reset_after_seconds)
            .map(|value| format!(", {}", value.trim_start_matches(", resets in ")))
            .unwrap_or_default()
    } else {
        format_reset_suffix(window.reset_after_seconds).unwrap_or_default()
    };
    format!("{} left{}", format_percent(left), reset_text)
}

fn format_credits(credits: Option<&UsageCredits>) -> Option<String> {
    let credits = credits?;
    if credits.unlimited {
        return Some("credits unlimited".to_string());
    }
    if !credits.has_credits {
        return None;
    }
    let mut details = Vec::new();
    if let Some(balance) = credits.balance {
        details.push(format!("balance {}", balance));
    }
    if let Some(local) = credits.approx_local_messages {
        details.push(format!("~{} local", local));
    }
    if let Some(cloud) = credits.approx_cloud_messages {
        details.push(format!("~{} cloud", cloud));
    }
    Some(if details.is_empty() {
        "credits available".to_string()
    } else {
        format!("credits {}", details.join(", "))
    })
}

fn format_credits_compact(credits: Option<&UsageCredits>) -> Option<String> {
    let credits = credits?;
    if credits.unlimited {
        return Some("unlimited".to_string());
    }
    if !credits.has_credits {
        return None;
    }
    let mut details = Vec::new();
    if let Some(balance) = credits.balance {
        details.push(format!("balance {}", balance));
    }
    if let Some(local) = credits.approx_local_messages {
        details.push(format!("~{} local msgs", local));
    }
    if let Some(cloud) = credits.approx_cloud_messages {
        details.push(format!("~{} cloud msgs", cloud));
    }
    Some(if details.is_empty() {
        "available".to_string()
    } else {
        details.join(", ")
    })
}

fn format_window_label(window: Option<&UsageWindow>, fallback: &str) -> String {
    let Some(window) = window else {
        return fallback.to_string();
    };
    let total_seconds = window.limit_window_seconds;
    if total_seconds > 0 && total_seconds % 86_400 == 0 {
        return format!("{}d", total_seconds / 86_400);
    }
    if total_seconds > 0 && total_seconds % 3_600 == 0 {
        return format!("{}h", total_seconds / 3_600);
    }
    if total_seconds > 0 && total_seconds % 60 == 0 {
        return format!("{}m", total_seconds / 60);
    }
    fallback.to_string()
}

fn format_reset_suffix(total_seconds: i64) -> Option<String> {
    if total_seconds < 0 {
        return None;
    }
    Some(format!(", resets in {}", format_duration(total_seconds)))
}

fn format_duration(total_seconds: i64) -> String {
    if total_seconds <= 0 {
        return "0s".to_string();
    }
    let mut remaining = total_seconds;
    let units = [("d", 86_400), ("h", 3_600), ("m", 60), ("s", 1)];
    let mut parts = Vec::new();
    for (label, unit_seconds) in units {
        let amount = remaining / unit_seconds;
        if amount > 0 {
            parts.push(format!("{}{}", amount, label));
            remaining -= amount * unit_seconds;
        }
        if parts.len() == 2 {
            break;
        }
    }
    parts.join(" ")
}

fn format_percent(value: f64) -> String {
    let rounded = value.round();
    if (value - rounded).abs() < f64::EPSILON {
        format!("{}%", rounded as i64)
    } else {
        format!("{:.1}%", value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_usage(primary_used_percent: f64) -> UsageResponse {
        UsageResponse {
            user_id: "user-1".to_string(),
            account_id: "acct-123".to_string(),
            email: "dev.22@astronlab.com".to_string(),
            plan_type: "free".to_string(),
            rate_limit: Some(UsageRateLimit {
                allowed: true,
                limit_reached: false,
                primary_window: Some(UsageWindow {
                    used_percent: primary_used_percent,
                    limit_window_seconds: 18_000,
                    reset_after_seconds: 7_200,
                    reset_at: 1_775_138_000,
                }),
                secondary_window: Some(UsageWindow {
                    used_percent: 100.0,
                    limit_window_seconds: 604_800,
                    reset_after_seconds: 86_400,
                    reset_at: 1_775_210_000,
                }),
            }),
            code_review_rate_limit: None,
            additional_rate_limits: None,
            credits: Some(UsageCredits {
                has_credits: false,
                unlimited: false,
                balance: None,
                approx_local_messages: None,
                approx_cloud_messages: None,
            }),
            promo: None,
        }
    }

    #[test]
    fn usable_when_primary_window_has_remaining_quota() {
        assert!(has_usable_quota(&make_usage(10.0)));
    }

    #[test]
    fn blocker_mentions_window_label() {
        let mut usage = make_usage(100.0);
        if let Some(rate_limit) = usage.rate_limit.as_mut() {
            rate_limit.allowed = false;
            rate_limit.limit_reached = true;
            rate_limit.primary_window.as_mut().unwrap().limit_window_seconds = 604_800;
            rate_limit.primary_window.as_mut().unwrap().reset_after_seconds = 3_600;
        }
        assert!(describe_quota_blocker(&usage).contains("7d quota exhausted"));
    }

    #[test]
    fn summary_includes_duration_labels() {
        assert!(format_quota_summary(&make_usage(10.0)).contains("5h 90% left"));
    }

    #[test]
    fn ttl_policy_is_adaptive() {
        let assessment = QuotaAssessment {
            usage: make_usage(60.0),
            usable: true,
            summary: "5h 40% left".to_string(),
            blocker: None,
            primary_quota_left_percent: Some(40.0),
        };
        assert_eq!(quota_cache_ttl(Some(&assessment), None), chrono::Duration::minutes(5));

        let assessment = QuotaAssessment {
            primary_quota_left_percent: Some(15.0),
            ..assessment
        };
        assert_eq!(quota_cache_ttl(Some(&assessment), None), chrono::Duration::seconds(90));

        let assessment = QuotaAssessment {
            primary_quota_left_percent: Some(5.0),
            ..assessment
        };
        assert_eq!(quota_cache_ttl(Some(&assessment), None), chrono::Duration::seconds(30));
    }

    #[test]
    fn cache_staleness_depends_on_account_and_next_refresh() {
        let now = DateTime::parse_from_rfc3339("2026-04-03T12:00:00.000Z")
            .unwrap()
            .with_timezone(&Utc);
        let fresh = CachedQuotaState {
            account_id: "acct-123".to_string(),
            fetched_at: "2026-04-03T11:59:00.000Z".to_string(),
            next_refresh_at: "2026-04-03T12:05:00.000Z".to_string(),
            summary: "5h 40% left".to_string(),
            usable: true,
            blocker: None,
            primary_quota_left_percent: Some(40),
            error: None,
        };
        assert!(!quota_cache_is_stale(Some(&fresh), "acct-123", now));
        assert!(quota_cache_is_stale(Some(&fresh), "acct-other", now));

        let expired = CachedQuotaState {
            next_refresh_at: "2026-04-03T11:59:59.000Z".to_string(),
            ..fresh
        };
        assert!(quota_cache_is_stale(Some(&expired), "acct-123", now));
    }
}
