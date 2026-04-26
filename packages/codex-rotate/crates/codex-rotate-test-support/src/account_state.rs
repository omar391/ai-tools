use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use base64::Engine;
use codex_rotate_core::auth::{AuthTokens, CodexAuth};
use codex_rotate_core::fs_security::write_private_string;
use codex_rotate_core::pool::{
    AccountEntry, PersonaEntry, RotationEnvironment, VmEnvironmentConfig, VmExpectedEgressMode,
};
use serde_json::{json, Map, Value};

use crate::{IsolatedHomeFixture, IsolatedHomeGuard};

const STABLE_TIMESTAMP: &str = "2026-04-07T00:00:00.000Z";

pub fn test_auth(email: &str, account_id: &str, plan_type: &str) -> CodexAuth {
    CodexAuth {
        auth_mode: "chatgpt".to_string(),
        openai_api_key: None,
        tokens: AuthTokens {
            access_token: make_jwt(json!({
                "https://api.openai.com/profile": {
                    "email": email,
                },
                "https://api.openai.com/auth": {
                    "chatgpt_account_id": account_id,
                    "chatgpt_plan_type": plan_type,
                }
            })),
            id_token: make_jwt(json!({
                "email": email,
            })),
            refresh_token: Some("refresh".to_string()),
            account_id: account_id.to_string(),
        },
        last_refresh: STABLE_TIMESTAMP.to_string(),
    }
}

pub fn test_persona(persona_id: &str) -> PersonaEntry {
    PersonaEntry {
        persona_id: persona_id.to_string(),
        persona_profile_id: Some("balanced-us-compact".to_string()),
        expected_region_code: None,
        ready_at: None,
        host_root_rel_path: Some(format!("personas/host/{persona_id}")),
        vm_package_rel_path: None,
        browser_fingerprint: Some(json!({
            "seeded": true,
        })),
    }
}

pub fn test_account(email: &str, account_id: &str, plan_type: &str) -> AccountEntry {
    let persona_id = format!("persona-{}", sanitize_token(account_id));
    AccountEntry {
        label: format!("{email}_{plan_type}"),
        alias: None,
        email: email.to_string(),
        relogin: false,
        account_id: account_id.to_string(),
        plan_type: plan_type.to_string(),
        auth: test_auth(email, account_id, plan_type),
        added_at: STABLE_TIMESTAMP.to_string(),
        last_quota_usable: None,
        last_quota_summary: None,
        last_quota_blocker: None,
        last_quota_checked_at: None,
        last_quota_primary_left_percent: None,
        last_quota_next_refresh_at: None,
        persona: Some(test_persona(&persona_id)),
    }
}

pub fn stable_test_accounts() -> Vec<AccountEntry> {
    vec![
        test_account("dev.1@astronlab.com", "acct-1", "free"),
        test_account("dev.2@astronlab.com", "acct-2", "free"),
    ]
}

pub fn test_vm_environment(root: &Path) -> Result<VmEnvironmentConfig> {
    let base_package_path = root.join("base.utm");
    let persona_root = root.join("personas");
    let utm_app_path = root.join("UTM.app");
    let bridge_root = root.join("bridge");

    fs::create_dir_all(&base_package_path)
        .with_context(|| format!("create {}", base_package_path.display()))?;
    fs::write(base_package_path.join("config.plist"), "base")
        .with_context(|| format!("write {}", base_package_path.join("config.plist").display()))?;
    fs::create_dir_all(&persona_root)
        .with_context(|| format!("create {}", persona_root.display()))?;
    fs::create_dir_all(&utm_app_path)
        .with_context(|| format!("create {}", utm_app_path.display()))?;
    fs::create_dir_all(&bridge_root)
        .with_context(|| format!("create {}", bridge_root.display()))?;

    Ok(VmEnvironmentConfig {
        base_package_path: Some(base_package_path.to_string_lossy().to_string()),
        persona_root: Some(persona_root.to_string_lossy().to_string()),
        utm_app_path: Some(utm_app_path.to_string_lossy().to_string()),
        bridge_root: Some(bridge_root.to_string_lossy().to_string()),
        expected_egress_mode: VmExpectedEgressMode::ProvisionOnly,
    })
}

#[derive(Debug)]
pub struct IsolatedAccountStateFixtureBuilder {
    home: IsolatedHomeFixture,
    accounts: Vec<AccountEntry>,
    active_index: usize,
    environment: RotationEnvironment,
    vm: Option<VmEnvironmentConfig>,
}

#[derive(Debug)]
pub struct IsolatedAccountStateFixture {
    _home_guard: IsolatedHomeGuard,
    home: IsolatedHomeFixture,
    state_path: PathBuf,
    accounts: Vec<AccountEntry>,
    active_index: usize,
    environment: RotationEnvironment,
    vm: Option<VmEnvironmentConfig>,
}

impl IsolatedAccountStateFixtureBuilder {
    pub fn new(prefix: impl AsRef<str>) -> Result<Self> {
        Ok(Self {
            home: IsolatedHomeFixture::new(prefix)?,
            accounts: stable_test_accounts(),
            active_index: 0,
            environment: RotationEnvironment::Host,
            vm: None,
        })
    }

    pub fn sandbox_root(&self) -> &Path {
        self.home.sandbox_root()
    }

    pub fn rotate_home(&self) -> &Path {
        self.home.rotate_home()
    }

    pub fn environment(mut self, environment: RotationEnvironment) -> Self {
        self.environment = environment;
        if matches!(self.environment, RotationEnvironment::Host) {
            self.vm = None;
        }
        self
    }

    pub fn active_index(mut self, active_index: usize) -> Self {
        self.active_index = active_index;
        self
    }

    pub fn active_account(mut self, account_id: &str) -> Result<Self> {
        self.active_index = self
            .accounts
            .iter()
            .position(|account| account.account_id == account_id)
            .ok_or_else(|| anyhow!("unknown account id {account_id}"))?;
        Ok(self)
    }

    pub fn accounts(mut self, accounts: Vec<AccountEntry>) -> Self {
        self.accounts = accounts;
        self
    }

    pub fn push_account(mut self, account: AccountEntry) -> Self {
        self.accounts.push(account);
        self
    }

    pub fn persona_for_account(mut self, account_id: &str, persona: PersonaEntry) -> Result<Self> {
        let account = self
            .accounts
            .iter_mut()
            .find(|account| account.account_id == account_id)
            .ok_or_else(|| anyhow!("unknown account id {account_id}"))?;
        account.persona = Some(persona);
        Ok(self)
    }

    pub fn vm_config(mut self, vm: VmEnvironmentConfig) -> Self {
        self.environment = RotationEnvironment::Vm;
        self.vm = Some(vm);
        self
    }

    pub fn build(mut self) -> Result<IsolatedAccountStateFixture> {
        if self.accounts.is_empty() {
            return Err(anyhow!(
                "isolated account state fixture requires at least one account"
            ));
        }
        if self.active_index >= self.accounts.len() {
            return Err(anyhow!(
                "active_index {} is out of bounds for {} account(s)",
                self.active_index,
                self.accounts.len()
            ));
        }

        for account in &mut self.accounts {
            normalize_account(account)?;
        }

        if matches!(self.environment, RotationEnvironment::Vm) && self.vm.is_none() {
            return Err(anyhow!("vm environment requires a vm config"));
        }

        if let Some(vm) = self.vm.as_ref() {
            materialize_vm_environment(vm)?;
        }
        materialize_accounts(&self.home, &self.accounts, self.vm.as_ref())?;

        let state_path = self.home.rotate_home().join("accounts.json");
        let state = build_state(
            &self.accounts,
            self.active_index,
            self.environment,
            self.vm.as_ref(),
        );
        write_private_string(&state_path, &serde_json::to_string_pretty(&state)?)
            .context("write isolated account state")?;

        let home_guard = self.home.install();

        Ok(IsolatedAccountStateFixture {
            _home_guard: home_guard,
            home: self.home,
            state_path,
            accounts: self.accounts,
            active_index: self.active_index,
            environment: self.environment,
            vm: self.vm,
        })
    }
}

impl IsolatedAccountStateFixture {
    pub fn new(prefix: impl AsRef<str>) -> Result<Self> {
        IsolatedAccountStateFixtureBuilder::new(prefix)?.build()
    }

    pub fn builder(prefix: impl AsRef<str>) -> Result<IsolatedAccountStateFixtureBuilder> {
        IsolatedAccountStateFixtureBuilder::new(prefix)
    }

    pub fn sandbox_root(&self) -> &Path {
        self.home.sandbox_root()
    }

    pub fn home_dir(&self) -> &Path {
        self.home.home_dir()
    }

    pub fn rotate_home(&self) -> &Path {
        self.home.rotate_home()
    }

    pub fn codex_home(&self) -> &Path {
        self.home.codex_home()
    }

    pub fn fast_browser_home(&self) -> &Path {
        self.home.fast_browser_home()
    }

    pub fn codex_app_support_dir(&self) -> &Path {
        self.home.codex_app_support_dir()
    }

    pub fn state_path(&self) -> &Path {
        &self.state_path
    }

    pub fn accounts(&self) -> &[AccountEntry] {
        &self.accounts
    }

    pub fn active_account(&self) -> &AccountEntry {
        &self.accounts[self.active_index]
    }

    pub fn environment(&self) -> RotationEnvironment {
        self.environment
    }

    pub fn vm_config(&self) -> Option<&VmEnvironmentConfig> {
        self.vm.as_ref()
    }

    pub fn read_state(&self) -> Result<Value> {
        let raw = fs::read_to_string(&self.state_path)
            .with_context(|| format!("read {}", self.state_path.display()))?;
        let value = serde_json::from_str(&raw)
            .with_context(|| format!("parse {}", self.state_path.display()))?;
        Ok(value)
    }
}

fn build_state(
    accounts: &[AccountEntry],
    active_index: usize,
    environment: RotationEnvironment,
    vm: Option<&VmEnvironmentConfig>,
) -> Value {
    let mut state = Map::new();
    state.insert(
        "accounts".to_string(),
        serde_json::to_value(accounts).expect("serialize accounts"),
    );
    state.insert(
        "active_index".to_string(),
        serde_json::to_value(active_index).expect("serialize active index"),
    );
    state.insert(
        "environment".to_string(),
        serde_json::to_value(environment).expect("serialize environment"),
    );
    if let Some(vm) = vm {
        state.insert(
            "vm".to_string(),
            serde_json::to_value(vm).expect("serialize vm config"),
        );
    }
    Value::Object(state)
}

fn materialize_vm_environment(vm: &VmEnvironmentConfig) -> Result<()> {
    let base_package_path = vm
        .base_package_path
        .as_deref()
        .ok_or_else(|| anyhow!("vm config is missing base_package_path"))?;
    let base_package_path = absolute_path(base_package_path, "base_package_path")?;
    fs::create_dir_all(&base_package_path)
        .with_context(|| format!("create {}", base_package_path.display()))?;
    fs::write(base_package_path.join("config.plist"), "base")
        .with_context(|| format!("write {}", base_package_path.join("config.plist").display()))?;

    let persona_root = vm
        .persona_root
        .as_deref()
        .ok_or_else(|| anyhow!("vm config is missing persona_root"))?;
    let persona_root = absolute_path(persona_root, "persona_root")?;
    fs::create_dir_all(&persona_root)
        .with_context(|| format!("create {}", persona_root.display()))?;

    let utm_app_path = vm
        .utm_app_path
        .as_deref()
        .ok_or_else(|| anyhow!("vm config is missing utm_app_path"))?;
    let utm_app_path = absolute_path(utm_app_path, "utm_app_path")?;
    fs::create_dir_all(&utm_app_path)
        .with_context(|| format!("create {}", utm_app_path.display()))?;

    if let Some(bridge_root) = vm.bridge_root.as_deref() {
        let bridge_root = absolute_path(bridge_root, "bridge_root")?;
        fs::create_dir_all(&bridge_root)
            .with_context(|| format!("create {}", bridge_root.display()))?;
    }

    Ok(())
}

fn materialize_accounts(
    home: &IsolatedHomeFixture,
    accounts: &[AccountEntry],
    vm: Option<&VmEnvironmentConfig>,
) -> Result<()> {
    let persona_root = vm
        .and_then(|vm| vm.persona_root.as_deref())
        .map(|value| absolute_path(value, "persona_root"))
        .transpose()?;

    for account in accounts {
        let persona = account
            .persona
            .as_ref()
            .ok_or_else(|| anyhow!("account {} is missing persona metadata", account.account_id))?;

        let host_root_rel_path = persona.host_root_rel_path.as_deref().ok_or_else(|| {
            anyhow!(
                "persona {} is missing host_root_rel_path",
                persona.persona_id
            )
        })?;
        let host_root_rel_path = relative_persona_path(host_root_rel_path, "host_root_rel_path")?;
        let host_root = home.rotate_home().join(host_root_rel_path);
        fs::create_dir_all(&host_root)
            .with_context(|| format!("create {}", host_root.display()))?;
        fs::create_dir_all(host_root.join("codex-home"))
            .with_context(|| format!("create {}", host_root.join("codex-home").display()))?;

        if let Some(persona_root) = persona_root.as_ref() {
            let package_root = persona
                .vm_package_rel_path
                .as_deref()
                .map(|relative| relative_persona_path(relative, "vm_package_rel_path"))
                .transpose()?
                .map(|relative| persona_root.join(relative))
                .unwrap_or_else(|| persona_root.join(&persona.persona_id));
            fs::create_dir_all(&package_root)
                .with_context(|| format!("create {}", package_root.display()))?;
        }
    }

    Ok(())
}

fn normalize_account(account: &mut AccountEntry) -> Result<()> {
    if account.account_id.trim().is_empty() {
        return Err(anyhow!("account_id cannot be empty"));
    }
    if account.email.trim().is_empty() {
        return Err(anyhow!("email cannot be empty"));
    }
    if account.label.trim().is_empty() {
        account.label = format!("{}_{}", account.email, account.plan_type);
    }

    let persona = account
        .persona
        .get_or_insert_with(|| test_persona(&default_persona_id(&account.account_id)));

    if persona.persona_id.trim().is_empty() {
        persona.persona_id = default_persona_id(&account.account_id);
    }

    if persona.persona_profile_id.is_none() {
        persona.persona_profile_id = Some("balanced-us-compact".to_string());
    }

    if persona.host_root_rel_path.is_none() {
        persona.host_root_rel_path = Some(format!("personas/host/{}", persona.persona_id));
    }

    if let Some(host_root_rel_path) = persona.host_root_rel_path.as_deref() {
        let _ = relative_persona_path(host_root_rel_path, "host_root_rel_path")?;
    }

    if let Some(vm_package_rel_path) = persona.vm_package_rel_path.as_deref() {
        let _ = relative_persona_path(vm_package_rel_path, "vm_package_rel_path")?;
    }

    if persona.browser_fingerprint.is_none() {
        persona.browser_fingerprint = Some(json!({
            "seeded": true,
        }));
    }

    if account.auth.tokens.account_id.trim().is_empty()
        || account.auth.tokens.account_id != account.account_id
    {
        account.auth.tokens.account_id = account.account_id.clone();
    }

    Ok(())
}

fn default_persona_id(account_id: &str) -> String {
    let sanitized = sanitize_token(account_id);
    if sanitized.is_empty() {
        "persona".to_string()
    } else {
        format!("persona-{sanitized}")
    }
}

fn sanitize_token(value: &str) -> String {
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
    normalized
        .split('-')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

fn relative_persona_path(path: &str, field: &str) -> Result<PathBuf> {
    let candidate = PathBuf::from(path.trim());
    if candidate.as_os_str().is_empty() {
        return Err(anyhow!("{field} cannot be empty"));
    }
    if candidate.is_absolute() {
        return Err(anyhow!("{field} must be relative to the rotate home"));
    }
    if candidate
        .components()
        .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
    {
        return Err(anyhow!(
            "{field} cannot contain parent-directory segments or absolute path markers"
        ));
    }
    Ok(candidate)
}

fn absolute_path(path: &str, field: &str) -> Result<PathBuf> {
    let candidate = PathBuf::from(path.trim());
    if candidate.as_os_str().is_empty() {
        return Err(anyhow!("{field} cannot be empty"));
    }
    if !candidate.is_absolute() {
        return Err(anyhow!("{field} must be absolute"));
    }
    Ok(candidate)
}

fn make_jwt(payload: Value) -> String {
    let header =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"none","typ":"JWT"}"#);
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
    format!("{header}.{payload}.signature")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_mutex() -> &'static std::sync::Mutex<()> {
        crate::test_environment_mutex()
    }

    #[test]
    fn fixture_seeds_stable_multi_account_state_and_materializes_host_roots() -> Result<()> {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = IsolatedAccountStateFixture::new("codex-rotate-account-state")?;

        assert_eq!(fixture.accounts().len(), 2);
        assert_eq!(fixture.environment(), RotationEnvironment::Host);
        assert!(fixture.state_path().exists());

        let state = fixture.read_state()?;
        assert_eq!(state["active_index"], Value::Number(0usize.into()));
        assert_eq!(
            state["accounts"].as_array().map(|value| value.len()),
            Some(2)
        );

        assert!(!fixture
            .rotate_home()
            .join("personas/host/persona-acct-1")
            .join("managed-profile")
            .exists());
        assert!(!fixture
            .rotate_home()
            .join("personas/host/persona-acct-1")
            .join("codex-app-support")
            .exists());
        assert!(!fixture
            .rotate_home()
            .join("personas/host/persona-acct-1")
            .join("fast-browser-home")
            .exists());
        assert!(fixture
            .rotate_home()
            .join("personas/host/persona-acct-2")
            .join("codex-home")
            .exists());
        Ok(())
    }

    #[test]
    fn builder_customizes_environment_active_account_and_persona_metadata() -> Result<()> {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let builder = IsolatedAccountStateFixture::builder("codex-rotate-account-state-vm")?;
        let vm_config = test_vm_environment(builder.sandbox_root())?;

        let mut custom_persona = test_persona("custom-persona");
        custom_persona.host_root_rel_path = Some("personas/host/custom-persona-root".to_string());
        custom_persona.vm_package_rel_path = Some("custom-packages/custom-persona.utm".to_string());
        custom_persona.browser_fingerprint = Some(json!({"seeded": "custom"}));

        let fixture = builder
            .accounts(vec![
                test_account("dev.1@astronlab.com", "acct-1", "free"),
                test_account("dev.2@astronlab.com", "acct-2", "plus"),
            ])
            .persona_for_account("acct-2", custom_persona)?
            .active_account("acct-2")?
            .vm_config(vm_config)
            .build()?;

        assert_eq!(fixture.environment(), RotationEnvironment::Vm);
        assert_eq!(fixture.active_account().account_id, "acct-2");

        let state = fixture.read_state()?;
        assert_eq!(state["active_index"], Value::Number(1usize.into()));
        assert_eq!(state["environment"], Value::String("vm".to_string()));
        assert_eq!(
            state["accounts"][1]["persona"]["hostRootRelPath"],
            Value::String("personas/host/custom-persona-root".to_string())
        );
        assert_eq!(
            state["accounts"][1]["persona"]["vmPackageRelPath"],
            Value::String("custom-packages/custom-persona.utm".to_string())
        );

        let vm = fixture.vm_config().expect("vm config");
        let persona_root = PathBuf::from(vm.persona_root.as_ref().expect("persona root"));
        assert!(persona_root.exists());
        assert!(persona_root
            .join("custom-packages/custom-persona.utm")
            .exists());
        assert!(
            PathBuf::from(vm.base_package_path.as_ref().expect("base package path"))
                .join("config.plist")
                .exists()
        );
        assert!(PathBuf::from(vm.utm_app_path.as_ref().expect("utm app path")).exists());
        Ok(())
    }
}
