use super::*;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct CodexModeProfile {
    pub model: String,
    pub model_reasoning_effort: String,
}

impl CodexModeProfile {
    fn new(model: &str, model_reasoning_effort: &str) -> Self {
        Self {
            model: model.to_string(),
            model_reasoning_effort: model_reasoning_effort.to_string(),
        }
    }

    fn merge_defaults(&mut self, default: &Self) {
        if self.model.trim().is_empty() {
            self.model = default.model.clone();
        }
        if self.model_reasoning_effort.trim().is_empty() {
            self.model_reasoning_effort = default.model_reasoning_effort.clone();
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct CodexModeConfig {
    #[serde(flatten)]
    pub plans: BTreeMap<String, CodexModeProfile>,
}

impl CodexModeConfig {
    pub fn with_defaults(config: Option<Self>) -> Self {
        let mut config = config.unwrap_or_default();
        for plan_type in ["free", "team"] {
            let Some(default_profile) = Self::default_profile(plan_type) else {
                continue;
            };
            config
                .plans
                .entry(plan_type.to_string())
                .and_modify(|profile| profile.merge_defaults(&default_profile))
                .or_insert(default_profile);
        }
        config
    }

    pub fn profile_for_plan_type(&self, plan_type: &str) -> Option<&CodexModeProfile> {
        self.plans.get(plan_type)
    }

    fn default_profile(plan_type: &str) -> Option<CodexModeProfile> {
        match plan_type {
            "free" => Some(CodexModeProfile::new("gpt-5.4", "xhigh")),
            "team" => Some(CodexModeProfile::new("gpt-5.5", "xhigh")),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Pool {
    pub active_index: usize,
    pub accounts: Vec<AccountEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AccountEntry {
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    pub email: String,
    pub account_id: String,
    pub plan_type: String,
    pub auth: CodexAuth,
    pub added_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_quota_usable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_quota_summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_quota_blocker: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_quota_checked_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_quota_primary_left_percent: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_quota_next_refresh_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persona: Option<PersonaEntry>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PersonaEntry {
    pub persona_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persona_profile_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_region_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_root_rel_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vm_package_rel_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub browser_fingerprint: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PersonaProfile {
    pub id: String,
    pub os_family: String,
    pub user_agent: String,
    pub accept_language: String,
    pub timezone: String,
    pub screen_width: u32,
    pub screen_height: u32,
    pub device_scale_factor: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub browser_fingerprint: Option<serde_json::Value>,
}

pub static PERSONA_PROFILES: OnceLock<Vec<PersonaProfile>> = OnceLock::new();

pub fn get_persona_profiles() -> &'static [PersonaProfile] {
    PERSONA_PROFILES.get_or_init(|| {
        [
            "balanced-us-compact",
            "balanced-eu-wide",
            "balanced-apac-standard",
        ]
        .iter()
        .map(|id| {
            let profile = get_persona_profile(id);
            PersonaProfile {
                id: profile.persona_profile_id,
                os_family: serde_json::to_string(&profile.os_family)
                    .unwrap_or_else(|_| "\"macos\"".to_string())
                    .trim_matches('"')
                    .to_string(),
                user_agent: profile.browser.user_agent,
                accept_language: profile.language,
                timezone: profile.timezone,
                screen_width: profile.vm_hardware.screen_width,
                screen_height: profile.vm_hardware.screen_height,
                device_scale_factor: profile.device_scale_factor,
                browser_fingerprint: None,
            }
        })
        .collect()
    })
}

pub fn resolve_persona_profile(
    profile_id: &str,
    browser_fingerprint: Option<serde_json::Value>,
) -> Option<PersonaProfile> {
    get_persona_profiles()
        .iter()
        .find(|profile| profile.id == profile_id)
        .map(|profile| PersonaProfile {
            browser_fingerprint,
            ..profile.clone()
        })
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RotationEnvironment {
    #[default]
    Host,
    Vm,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VmExpectedEgressMode {
    #[default]
    ProvisionOnly,
    Validate,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct VmEnvironmentConfig {
    pub base_package_path: Option<String>,
    pub persona_root: Option<String>,
    pub utm_app_path: Option<String>,
    pub bridge_root: Option<String>,
    pub expected_egress_mode: VmExpectedEgressMode,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RotationEnvironmentSettings {
    pub environment: RotationEnvironment,
    pub vm: Option<VmEnvironmentConfig>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RotationCheckpointPhase {
    #[default]
    Prepare,
    Export,
    Activate,
    Import,
    Commit,
    Rollback,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct RotationCheckpoint {
    pub phase: RotationCheckpointPhase,
    pub previous_index: usize,
    pub target_index: usize,
    pub previous_account_id: String,
    pub target_account_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedRotationAction {
    Switch,
    Stay,
    CreateRequired,
}

#[derive(Clone, Debug)]
pub struct PreparedRotation {
    pub action: PreparedRotationAction,
    pub pool: Pool,
    pub previous_index: usize,
    pub target_index: usize,
    pub previous: AccountEntry,
    pub target: AccountEntry,
    pub message: String,
    pub persist_pool: bool,
}

#[derive(Clone, Debug)]
pub struct AccountInspection {
    pub usage: Option<UsageResponse>,
    pub error: Option<String>,
    pub updated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolOverview {
    pub inventory_count: usize,
    pub inventory_active_slot: Option<usize>,
    pub inventory_healthy_count: usize,
}

#[derive(Clone, Debug)]
pub struct RotationCandidate {
    pub index: usize,
    pub entry: AccountEntry,
    pub inspection: AccountInspection,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum NextResult {
    Rotated {
        message: String,
        summary: AuthSummary,
    },
    Stayed {
        message: String,
        summary: AuthSummary,
    },
    Created {
        output: String,
        summary: AuthSummary,
    },
}

#[derive(Default, Deserialize)]
#[serde(default)]
pub(super) struct RotationEnvironmentState {
    pub(super) environment: RotationEnvironment,
    pub(super) vm: Option<VmEnvironmentConfig>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReusableAccountProbeMode {
    CurrentFirst,
    OthersFirst,
    OthersOnly,
}
