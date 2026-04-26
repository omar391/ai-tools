use super::*;
use crate::test_support::{RotateHomeGuard, ENV_MUTEX};
use base64::Engine;
use serde_json::json;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex as StdMutex};
use std::thread;
use std::time::{Duration as StdDuration, Instant};

fn stored_entry(usable: Option<bool>, checked_at: Option<&str>) -> AccountEntry {
    AccountEntry {
        label: "a_free".to_string(),
        alias: None,
        email: "a@example.com".to_string(),
        account_id: "acct-a".to_string(),
        plan_type: "free".to_string(),
        auth: CodexAuth {
            auth_mode: "chatgpt".to_string(),
            openai_api_key: None,
            tokens: crate::auth::AuthTokens {
                access_token: "a.b.c".to_string(),
                id_token: "a.b.c".to_string(),
                refresh_token: None,
                account_id: "acct-a".to_string(),
            },
            last_refresh: "2026-04-02T00:00:00.000Z".to_string(),
        },
        added_at: "2026-04-02T00:00:00.000Z".to_string(),
        last_quota_usable: usable,
        last_quota_summary: None,
        last_quota_blocker: None,
        last_quota_checked_at: checked_at.map(ToOwned::to_owned),
        last_quota_primary_left_percent: None,
        last_quota_next_refresh_at: None,
        persona: None,
    }
}

fn restore_env_var(key: &str, previous: Option<std::ffi::OsString>) {
    match previous {
        Some(value) => unsafe {
            std::env::set_var(key, value);
        },
        None => unsafe {
            std::env::remove_var(key);
        },
    }
}

fn strip_ansi(input: &str) -> String {
    input
        .replace(BOLD, "")
        .replace(DIM, "")
        .replace(GREEN, "")
        .replace(YELLOW, "")
        .replace(CYAN, "")
        .replace(RESET, "")
}

fn spawn_usage_server(body: String) -> (String, thread::JoinHandle<()>) {
    spawn_usage_server_with_delay(body, StdDuration::from_millis(0))
}

fn spawn_usage_server_with_delay(
    body: String,
    response_delay: StdDuration,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind usage server");
    listener
        .set_nonblocking(true)
        .expect("set usage server nonblocking");
    let address = listener.local_addr().expect("usage server address");
    let handle = thread::spawn(move || {
        let deadline = Instant::now() + StdDuration::from_secs(5);
        loop {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut buffer = [0_u8; 4096];
                    let _ = stream.read(&mut buffer);
                    if !response_delay.is_zero() {
                        thread::sleep(response_delay);
                    }
                    let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                    stream
                        .write_all(response.as_bytes())
                        .expect("write usage response");
                    stream.flush().expect("flush usage response");
                    return;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        panic!("timed out waiting for quota request");
                    }
                    thread::sleep(StdDuration::from_millis(10));
                }
                Err(error) => panic!("usage server accept failed: {error}"),
            }
        }
    });
    (format!("http://{address}/usage"), handle)
}

#[derive(Clone, Default)]
struct SharedWriter {
    buffer: Arc<StdMutex<Vec<u8>>>,
}

impl SharedWriter {
    fn snapshot(&self) -> String {
        String::from_utf8(self.buffer.lock().expect("writer mutex").clone()).expect("utf8 output")
    }
}

impl Write for SharedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer
            .lock()
            .expect("writer mutex")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn make_jwt(payload: serde_json::Value) -> String {
    let header =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"none","typ":"JWT"}"#);
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
    format!("{header}.{payload}.signature")
}

fn make_auth(email: &str, account_id: &str, plan_type: &str) -> CodexAuth {
    CodexAuth {
        auth_mode: "chatgpt".to_string(),
        openai_api_key: None,
        tokens: crate::auth::AuthTokens {
            access_token: make_jwt(json!({
                "https://api.openai.com/profile": {
                    "email": email
                },
                "https://api.openai.com/auth": {
                    "chatgpt_account_id": account_id,
                    "chatgpt_plan_type": plan_type
                }
            })),
            id_token: make_jwt(json!({
                "email": email
            })),
            refresh_token: Some("refresh".to_string()),
            account_id: account_id.to_string(),
        },
        last_refresh: "2026-04-07T00:00:00.000Z".to_string(),
    }
}

fn configured_entry(
    email: &str,
    account_id: &str,
    plan_type: &str,
    usable: Option<bool>,
    checked_at: Option<&str>,
) -> AccountEntry {
    AccountEntry {
        label: format!("{email}_{plan_type}"),
        alias: None,
        email: email.to_string(),
        account_id: account_id.to_string(),
        plan_type: plan_type.to_string(),
        auth: make_auth(email, account_id, plan_type),
        added_at: "2026-04-07T00:00:00.000Z".to_string(),
        last_quota_usable: usable,
        last_quota_summary: usable.map(|value| {
            if value {
                "5h 90% left".to_string()
            } else {
                "5h 0% left".to_string()
            }
        }),
        last_quota_blocker: None,
        last_quota_checked_at: checked_at.map(ToOwned::to_owned),
        last_quota_primary_left_percent: usable.map(|value| if value { 90 } else { 0 }),
        last_quota_next_refresh_at: checked_at.map(|_| "2026-04-07T01:00:00.000Z".to_string()),
        persona: None,
    }
}

fn write_disabled_domain_state() -> Result<()> {
    let mut state = load_rotate_state_json()?;
    if !state.is_object() {
        state = json!({});
    }
    state["domain"] = json!({
        "astronlab.com": {
            "rotation_enabled": false
        }
    });
    write_rotate_state_json(&state)
}

fn terminal_cleanup_account(email: &str) -> AccountEntry {
    let mut entry = stored_entry(Some(false), Some("2026-04-07T01:00:00.000Z"));
    entry.email = email.to_string();
    entry.account_id = "acct-terminal".to_string();
    entry.last_quota_blocker = Some("refresh token has been invalidated".to_string());
    entry
}

fn write_terminal_cleanup_state(
    relogin: Vec<&str>,
    suspend_domain_on_terminal_refresh_failure: bool,
) -> Result<()> {
    let family = json!({
        "profile_name": "dev-1",
        "template": "dev.{n}@astronlab.com",
        "next_suffix": 2,
        "created_at": "2026-04-05T00:00:00.000Z",
        "updated_at": "2026-04-05T00:00:00.000Z",
        "last_created_email": "dev.1@astronlab.com",
        "relogin": relogin,
        "suspend_domain_on_terminal_refresh_failure": suspend_domain_on_terminal_refresh_failure,
    });
    write_rotate_state_json(&json!({
        "accounts": [terminal_cleanup_account("dev.1@astronlab.com")],
        "active_index": 0,
        "version": 7,
        "default_create_template": "dev.{n}@astronlab.com",
        "families": {
            "dev-1::dev.{n}@astronlab.com": family
        },
        "pending": {},
        "skipped": [],
        "domain": {
            "astronlab.com": {
                "rotation_enabled": true
            }
        }
    }))
}

#[test]
fn save_pool_preserves_credential_store_sections() {
    let _guard = RotateHomeGuard::enter("codex-rotate-save-pool-preserve");
    write_rotate_state_json(&json!({
        "accounts": [configured_entry("dev.1@astronlab.com", "acct-1", "free", Some(true), None)],
        "active_index": 0,
        "version": 7,
        "default_create_template": "dev.{n}@astronlab.com",
        "families": {
            "dev-1::dev.{n}@astronlab.com": {
                "profile_name": "dev-1",
                "template": "dev.{n}@astronlab.com",
                "next_suffix": 3,
                "created_at": "2026-04-05T00:00:00.000Z",
                "updated_at": "2026-04-05T00:00:00.000Z",
                "last_created_email": "dev.2@astronlab.com",
                "relogin": []
            }
        },
        "pending": {
            "dev.3@astronlab.com": {
                "email": "dev.3@astronlab.com",
                "profile_name": "dev-1",
                "template": "dev.{n}@astronlab.com",
                "suffix": 3,
                "selector": null,
                "alias": null,
                "created_at": "2026-04-05T00:00:00.000Z",
                "updated_at": "2026-04-05T00:00:00.000Z",
                "started_at": "2026-04-05T00:00:00.000Z"
            }
        },
        "skipped": ["dev.4@astronlab.com"],
        "domain": {
            "astronlab.com": {
                "rotation_enabled": false
            }
        }
    }))
    .expect("write initial state");

    save_pool(&Pool {
        active_index: 1,
        accounts: vec![
            configured_entry("dev.1@astronlab.com", "acct-1", "free", Some(true), None),
            configured_entry("dev.2@astronlab.com", "acct-2", "free", None, None),
        ],
    })
    .expect("save pool");

    let state = load_rotate_state_json().expect("load rotate state");
    assert_eq!(state["version"], json!(9));
    assert_eq!(
        state["default_create_template"],
        json!("dev.{n}@astronlab.com")
    );
    assert!(state["families"].is_object());
    assert!(state["pending"].is_object());
    assert_eq!(state["skipped"], json!(["dev.4@astronlab.com"]));
    assert_eq!(
        state["domain"]["astronlab.com"]["rotation_enabled"],
        json!(false)
    );
    assert_eq!(state["active_index"], json!(1));
    assert_eq!(state["accounts"][1]["email"], json!("dev.2@astronlab.com"));
}

#[test]
fn prune_terminal_accounts_does_not_disable_domain_for_relogin_only_families() {
    let _guard = RotateHomeGuard::enter("codex-rotate-terminal-cleanup-relogin-only");
    write_terminal_cleanup_state(Vec::new(), false).expect("write relogin-only state");

    let mut pool = Pool {
        active_index: 0,
        accounts: vec![terminal_cleanup_account("dev.1@astronlab.com")],
    };

    let changed = prune_terminal_accounts_from_pool(&mut pool).expect("prune terminal accounts");
    assert!(changed);

    let state = load_rotate_state_json().expect("load rotate state");
    assert_eq!(
        state["domain"]["astronlab.com"]["rotation_enabled"],
        json!(true)
    );
}

#[test]
fn prune_terminal_accounts_disables_domain_for_suspend_flagged_families() {
    let _guard = RotateHomeGuard::enter("codex-rotate-terminal-cleanup-suspend-flag");
    write_terminal_cleanup_state(Vec::new(), true).expect("write suspend-flag state");

    let mut pool = Pool {
        active_index: 0,
        accounts: vec![terminal_cleanup_account("dev.1@astronlab.com")],
    };

    let changed = prune_terminal_accounts_from_pool(&mut pool).expect("prune terminal accounts");
    assert!(changed);

    let state = load_rotate_state_json().expect("load rotate state");
    assert_eq!(
        state["domain"]["astronlab.com"]["rotation_enabled"],
        json!(false)
    );
}

#[test]
fn load_rotation_environment_settings_defaults_to_host() {
    let _guard = RotateHomeGuard::enter("codex-rotate-env-default-host");
    write_rotate_state_json(&json!({
        "accounts": [],
        "active_index": 0
    }))
    .expect("write default rotate state");

    let settings = load_rotation_environment_settings().expect("load settings");
    assert_eq!(settings.environment, RotationEnvironment::Host);
    assert!(settings.vm.is_none());
}

#[test]
fn load_rotation_environment_settings_reads_vm_config() {
    let _guard = RotateHomeGuard::enter("codex-rotate-env-vm");
    write_rotate_state_json(&json!({
        "accounts": [],
        "active_index": 0,
        "environment": "vm",
        "vm": {
            "basePackagePath": "/vm/base.utm",
            "personaRoot": "/vm/personas",
            "utmAppPath": "/Applications/UTM.app",
            "bridgeRoot": "/vm/bridge",
            "expectedEgressMode": "validate"
        }
    }))
    .expect("write vm rotate state");

    let settings = load_rotation_environment_settings().expect("load settings");
    assert_eq!(settings.environment, RotationEnvironment::Vm);
    let vm = settings.vm.expect("vm config");
    assert_eq!(vm.base_package_path.as_deref(), Some("/vm/base.utm"));
    assert_eq!(vm.persona_root.as_deref(), Some("/vm/personas"));
    assert_eq!(vm.utm_app_path.as_deref(), Some("/Applications/UTM.app"));
    assert_eq!(vm.bridge_root.as_deref(), Some("/vm/bridge"));
    assert_eq!(vm.expected_egress_mode, VmExpectedEgressMode::Validate);
}

#[test]
fn normalize_pool_entries_assigns_deterministic_persona_defaults() {
    let mut pool = Pool {
        active_index: 0,
        accounts: vec![configured_entry(
            "dev.1@astronlab.com",
            "acct-1",
            "free",
            Some(true),
            None,
        )],
    };

    assert!(normalize_pool_entries(&mut pool));
    let persona = pool.accounts[0]
        .persona
        .clone()
        .expect("persona metadata should be assigned");
    assert!(persona.persona_id.starts_with("persona-"));
    assert!(persona
        .host_root_rel_path
        .as_deref()
        .unwrap()
        .starts_with("personas/host/"));
    assert!(persona.persona_profile_id.is_some());

    let mut second_pool = Pool {
        active_index: 0,
        accounts: vec![configured_entry(
            "dev.1@astronlab.com",
            "acct-1",
            "free",
            Some(true),
            None,
        )],
    };
    normalize_pool_entries(&mut second_pool);
    assert_eq!(second_pool.accounts[0].persona, Some(persona));
}

#[test]
fn prepare_prev_rotation_stages_previous_selection_until_commit() {
    let _guard = RotateHomeGuard::enter("codex-rotate-prepare-prev-stage");
    let mut previous = configured_entry("dev.1@astronlab.com", "acct-1", "free", Some(true), None);
    previous.last_quota_checked_at = Some("2099-01-01T00:00:00.000Z".to_string());
    previous.last_quota_next_refresh_at = Some("2099-01-01T01:00:00.000Z".to_string());
    let mut current = configured_entry("dev.2@astronlab.com", "acct-2", "free", Some(true), None);
    current.last_quota_checked_at = Some("2099-01-01T00:00:00.000Z".to_string());
    current.last_quota_next_refresh_at = Some("2099-01-01T01:00:00.000Z".to_string());
    write_rotate_state_json(&json!({
        "accounts": [previous, current],
        "active_index": 1
    }))
    .expect("write rotate state");

    let initial_pool = load_pool().expect("load pool");
    write_selected_account_auth(&initial_pool.accounts[1]).expect("write current auth");

    let prepared = prepare_prev_rotation().expect("prepare prev");
    assert_eq!(prepared.action, PreparedRotationAction::Switch);
    assert_eq!(prepared.previous_index, 1);
    assert_eq!(prepared.target_index, 0);

    let staged_state = load_rotate_state_json().expect("load staged state");
    assert_eq!(staged_state["active_index"], json!(1));
    let staged_auth =
        crate::auth::load_codex_auth(&resolve_paths().expect("resolve paths").codex_auth_file)
            .expect("load staged auth");
    assert_eq!(staged_auth.tokens.account_id, "acct-2");

    persist_prepared_rotation_pool(&prepared).expect("persist prepared pool");
    write_selected_account_auth(&prepared.target).expect("write target auth");

    let committed_state = load_rotate_state_json().expect("load committed state");
    assert_eq!(committed_state["active_index"], json!(0));
    let committed_auth =
        crate::auth::load_codex_auth(&resolve_paths().expect("resolve paths").codex_auth_file)
            .expect("load committed auth");
    assert_eq!(committed_auth.tokens.account_id, "acct-1");
}

#[test]
fn prepare_set_rotation_stages_selected_target_until_commit() {
    let _guard = RotateHomeGuard::enter("codex-rotate-prepare-set-stage");
    let mut previous = configured_entry("dev.1@astronlab.com", "acct-1", "free", Some(true), None);
    previous.last_quota_checked_at = Some("2099-01-01T00:00:00.000Z".to_string());
    previous.last_quota_next_refresh_at = Some("2099-01-01T01:00:00.000Z".to_string());
    let mut target = configured_entry("dev.2@astronlab.com", "acct-2", "free", Some(false), None);
    target.last_quota_checked_at = Some("2099-01-01T00:00:00.000Z".to_string());
    target.last_quota_next_refresh_at = Some("2099-01-01T01:00:00.000Z".to_string());
    write_rotate_state_json(&json!({
        "accounts": [previous, target],
        "active_index": 0
    }))
    .expect("write rotate state");

    let initial_pool = load_pool().expect("load pool");
    write_selected_account_auth(&initial_pool.accounts[0]).expect("write current auth");

    let prepared = prepare_set_rotation("acct-2").expect("prepare set");
    assert_eq!(prepared.action, PreparedRotationAction::Switch);
    assert_eq!(prepared.previous_index, 0);
    assert_eq!(prepared.target_index, 1);

    let staged_state = load_rotate_state_json().expect("load staged state");
    assert_eq!(staged_state["active_index"], json!(0));
    let staged_auth =
        crate::auth::load_codex_auth(&resolve_paths().expect("resolve paths").codex_auth_file)
            .expect("load staged auth");
    assert_eq!(staged_auth.tokens.account_id, "acct-1");

    persist_prepared_rotation_pool(&prepared).expect("persist prepared pool");
    write_selected_account_auth(&prepared.target).expect("write target auth");

    let committed_state = load_rotate_state_json().expect("load committed state");
    assert_eq!(committed_state["active_index"], json!(1));
    let committed_auth =
        crate::auth::load_codex_auth(&resolve_paths().expect("resolve paths").codex_auth_file)
            .expect("load committed auth");
    assert_eq!(committed_auth.tokens.account_id, "acct-2");
}

#[test]
fn prepare_set_rotation_returns_stay_for_active_selector() {
    let _guard = RotateHomeGuard::enter("codex-rotate-prepare-set-stay");
    let first = configured_entry("dev.1@astronlab.com", "acct-1", "free", Some(true), None);
    let second = configured_entry("dev.2@astronlab.com", "acct-2", "free", Some(false), None);
    write_rotate_state_json(&json!({
        "accounts": [first, second],
        "active_index": 0
    }))
    .expect("write rotate state");

    let initial_pool = load_pool().expect("load pool");
    write_selected_account_auth(&initial_pool.accounts[0]).expect("write current auth");

    let prepared = prepare_set_rotation("acct-1").expect("prepare set");
    assert_eq!(prepared.action, PreparedRotationAction::Stay);
    assert_eq!(prepared.previous_index, 0);
    assert_eq!(prepared.target_index, 0);
    assert!(prepared.message.contains("Stayed on"));
}

#[test]
fn cached_next_rotation_prefers_later_usable_slot() {
    let accounts = vec![
        stored_entry(Some(true), None),
        stored_entry(Some(false), None),
        stored_entry(Some(true), None),
    ];
    assert_eq!(find_next_cached_usable_account_index(0, &accounts), Some(2));
}

#[test]
fn immediate_round_robin_skips_explicitly_unusable_slots() {
    let accounts = vec![
        stored_entry(None, None),
        stored_entry(Some(true), None),
        stored_entry(Some(false), Some("2026-04-02T00:00:00.000Z")),
    ];
    assert_eq!(find_next_immediate_round_robin_index(1, &accounts), Some(0));
}

#[test]
fn probe_order_respects_mode() {
    assert_eq!(
        build_reusable_account_probe_order(1, 4, ReusableAccountProbeMode::CurrentFirst),
        vec![1, 2, 3, 0]
    );
    assert_eq!(
        build_reusable_account_probe_order(1, 4, ReusableAccountProbeMode::OthersFirst),
        vec![2, 3, 0, 1]
    );
    assert_eq!(
        build_reusable_account_probe_order(1, 4, ReusableAccountProbeMode::OthersOnly),
        vec![2, 3, 0]
    );
}

#[test]
fn pool_identity_lookup_prefers_exact_email_match() {
    let mut first = stored_entry(Some(true), None);
    first.email = "dev.26@astronlab.com".to_string();
    first.account_id = "acct-26".to_string();
    first.auth.tokens.account_id = "acct-26".to_string();
    let mut second = stored_entry(Some(true), None);
    second.email = "dev.27@astronlab.com".to_string();
    second.account_id = "acct-27".to_string();
    second.auth.tokens.account_id = "acct-27".to_string();
    let pool = Pool {
        active_index: 0,
        accounts: vec![first, second],
    };

    assert_eq!(
        find_pool_account_index_by_identity(&pool, "acct-27", "dev.26@astronlab.com", "free"),
        Some(0)
    );
}

#[test]
fn pool_identity_lookup_falls_back_to_email_match() {
    let mut first = stored_entry(Some(true), None);
    first.email = "dev.26@astronlab.com".to_string();
    first.account_id = "acct-26".to_string();
    first.auth.tokens.account_id = "acct-26".to_string();
    let pool = Pool {
        active_index: 0,
        accounts: vec![first],
    };

    assert_eq!(
        find_pool_account_index_by_identity(&pool, "missing", "dev.26@astronlab.com", "free"),
        Some(0)
    );
}

#[test]
fn pool_identity_lookup_distinguishes_same_email_different_plan() {
    let mut team = stored_entry(Some(true), None);
    team.email = "dev.1@hotspotprime.com".to_string();
    team.label = "dev.1@hotspotprime.com_team".to_string();
    team.plan_type = "team".to_string();
    team.account_id = "acct-team".to_string();
    team.auth = make_auth("dev.1@hotspotprime.com", "acct-team", "team");

    let mut free = stored_entry(Some(true), None);
    free.email = "dev.1@hotspotprime.com".to_string();
    free.label = "dev.1@hotspotprime.com_free".to_string();
    free.plan_type = "free".to_string();
    free.account_id = "acct-free".to_string();
    free.auth = make_auth("dev.1@hotspotprime.com", "acct-free", "free");

    let pool = Pool {
        active_index: 0,
        accounts: vec![team, free],
    };

    assert_eq!(
        find_pool_account_index_by_identity(&pool, "acct-team", "dev.1@hotspotprime.com", "team",),
        Some(0)
    );
    assert_eq!(
        find_pool_account_index_by_identity(&pool, "acct-free", "dev.1@hotspotprime.com", "free",),
        Some(1)
    );
}

#[test]
fn pool_identity_lookup_ignores_shared_account_id_for_different_team_email() {
    let mut first = stored_entry(Some(true), None);
    first.email = "dev.2@hotspotprime.com".to_string();
    first.label = "dev.2@hotspotprime.com_team".to_string();
    first.plan_type = "team".to_string();
    first.account_id = "acct-team".to_string();
    first.auth = make_auth("dev.2@hotspotprime.com", "acct-team", "team");

    let pool = Pool {
        active_index: 0,
        accounts: vec![first],
    };

    assert_eq!(
        find_pool_account_index_by_identity(&pool, "acct-team", "dev.3@hotspotprime.com", "team"),
        None
    );
}

#[test]
fn cached_list_quota_line_uses_saved_summary() {
    let mut entry = stored_entry(Some(true), Some("2026-04-07T00:00:00.000Z"));
    entry.last_quota_summary = Some("7d 90% left".to_string());

    let rendered = format_cached_quota_line(&entry);

    assert!(rendered.contains("7d 90% left"));
    assert!(rendered.contains("cached 2026-04-07T00:00:00.000Z"));
}

#[test]
fn cached_list_quota_line_marks_unchecked_entries_without_network_lookup() {
    let entry = stored_entry(None, None);

    let rendered = format_cached_quota_line(&entry);

    assert_eq!(
        rendered,
        "unknown (run codex-rotate status or rotate to refresh)"
    );
}

#[test]
fn cached_list_quota_state_respects_usable_ttl() {
    let now = DateTime::parse_from_rfc3339("2026-04-08T12:01:00.000Z")
        .expect("parse now")
        .with_timezone(&Utc);
    let mut fresh = stored_entry(Some(true), Some("2026-04-08T12:00:30.000Z"));
    fresh.last_quota_primary_left_percent = Some(40);
    assert!(!cached_quota_state_is_stale(&fresh, now));

    let mut stale = stored_entry(Some(true), Some("2026-04-08T11:59:50.000Z"));
    stale.last_quota_primary_left_percent = Some(40);
    assert!(cached_quota_state_is_stale(&stale, now));
}

#[test]
fn cached_list_quota_state_waits_for_zero_percent_reset_time() {
    let checked_at = DateTime::parse_from_rfc3339("2026-04-08T12:00:00.000Z")
        .expect("parse checked_at")
        .with_timezone(&Utc);
    let mut exhausted = stored_entry(Some(false), Some("2026-04-08T12:00:00.000Z"));
    exhausted.last_quota_blocker = Some("5h quota exhausted, resets in 2h 15m".to_string());

    let before_reset = DateTime::parse_from_rfc3339("2026-04-08T14:14:59.000Z")
        .expect("parse before_reset")
        .with_timezone(&Utc);
    let after_reset = DateTime::parse_from_rfc3339("2026-04-08T14:15:01.000Z")
        .expect("parse after_reset")
        .with_timezone(&Utc);

    assert_eq!(
        legacy_cached_quota_next_refresh_at(&exhausted, checked_at),
        Some(
            DateTime::parse_from_rfc3339("2026-04-08T14:15:00.000Z")
                .expect("parse expected reset")
                .with_timezone(&Utc)
        )
    );
    assert!(!cached_quota_state_is_stale(&exhausted, before_reset));
    assert!(cached_quota_state_is_stale(&exhausted, after_reset));
}

#[test]
fn list_account_refresh_due_only_when_refresh_time_elapsed_or_missing() {
    let now = DateTime::parse_from_rfc3339("2026-04-08T12:05:00.000Z")
        .expect("parse now")
        .with_timezone(&Utc);

    let mut fresh = stored_entry(Some(true), Some("2026-04-08T12:04:30.000Z"));
    fresh.last_quota_primary_left_percent = Some(40);
    fresh.last_quota_next_refresh_at = Some("2026-04-08T12:10:00.000Z".to_string());
    assert!(!account_quota_refresh_due_for_list(&fresh, now));

    let mut stale = stored_entry(Some(true), Some("2026-04-08T12:03:30.000Z"));
    stale.last_quota_primary_left_percent = Some(40);
    stale.last_quota_next_refresh_at = Some("2026-04-08T12:04:59.000Z".to_string());
    assert!(account_quota_refresh_due_for_list(&stale, now));

    let unknown = stored_entry(None, None);
    assert!(account_quota_refresh_due_for_list(&unknown, now));
}

#[test]
fn list_quota_refresh_order_prioritizes_active_then_oldest_stale_usable() {
    let now = DateTime::parse_from_rfc3339("2026-04-08T12:05:00.000Z")
        .expect("parse now")
        .with_timezone(&Utc);

    let mut active = stored_entry(Some(false), Some("2026-04-08T12:04:00.000Z"));
    active.last_quota_blocker = Some("rate limited".to_string());

    let mut oldest_stale_usable = stored_entry(Some(true), Some("2026-04-08T12:03:30.000Z"));
    oldest_stale_usable.last_quota_primary_left_percent = Some(40);

    let mut fresher_stale_usable = stored_entry(Some(true), Some("2026-04-08T12:04:10.000Z"));
    fresher_stale_usable.last_quota_primary_left_percent = Some(40);

    let mut fresh_usable = stored_entry(Some(true), Some("2026-04-08T12:04:45.000Z"));
    fresh_usable.last_quota_primary_left_percent = Some(40);

    let pool = Pool {
        active_index: 0,
        accounts: vec![
            active,
            fresher_stale_usable,
            fresh_usable,
            oldest_stale_usable,
        ],
    };

    assert_eq!(build_list_quota_refresh_order(&pool, now), vec![0, 3]);
}

#[test]
fn list_quota_refresh_order_includes_unknown_and_all_stale_entries() {
    let now = DateTime::parse_from_rfc3339("2026-04-08T12:05:00.000Z")
        .expect("parse now")
        .with_timezone(&Utc);

    let mut stale_active = stored_entry(Some(false), Some("2026-04-08T12:04:00.000Z"));
    stale_active.last_quota_blocker = Some("rate limited".to_string());

    let mut unknown = stored_entry(None, None);
    unknown.label = "unknown".to_string();
    unknown.email = "unknown@example.com".to_string();
    unknown.account_id = "acct-unknown".to_string();

    let mut stale_usable = stored_entry(Some(true), Some("2026-04-08T12:03:30.000Z"));
    stale_usable.last_quota_primary_left_percent = Some(40);

    let pool = Pool {
        active_index: 0,
        accounts: vec![stale_active, unknown, stale_usable],
    };

    assert_eq!(build_list_quota_refresh_order(&pool, now), vec![1, 0, 2]);
}

#[test]
fn list_quota_refresh_limit_uses_env_override() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let previous_limit = std::env::var_os(LIST_QUOTA_REFRESH_LIMIT_ENV);

    unsafe {
        std::env::set_var(LIST_QUOTA_REFRESH_LIMIT_ENV, "2");
    }
    assert_eq!(list_quota_refresh_limit(), 2);

    unsafe {
        std::env::set_var(LIST_QUOTA_REFRESH_LIMIT_ENV, "invalid");
    }
    assert_eq!(list_quota_refresh_limit(), DEFAULT_LIST_QUOTA_REFRESH_LIMIT);

    restore_env_var(LIST_QUOTA_REFRESH_LIMIT_ENV, previous_limit);
}

#[test]
fn list_account_display_order_sorts_by_next_quota_refresh_eta() {
    let mut later = stored_entry(Some(true), Some("2026-04-08T12:00:00.000Z"));
    later.label = "later".to_string();
    later.last_quota_primary_left_percent = Some(80);
    later.last_quota_next_refresh_at = Some("2026-04-08T12:20:00.000Z".to_string());

    let mut unknown = stored_entry(None, None);
    unknown.label = "unknown".to_string();

    let mut sooner = stored_entry(Some(false), Some("2026-04-08T12:00:00.000Z"));
    sooner.label = "sooner".to_string();
    sooner.last_quota_blocker = Some("7d quota exhausted, resets in 10m".to_string());

    let pool = Pool {
        active_index: 1,
        accounts: vec![later, unknown, sooner],
    };

    assert_eq!(build_list_account_display_order(&pool), vec![2, 0, 1]);
}

#[test]
fn cmd_list_refreshes_stale_cached_usable_quota() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    let previous_usage_url = std::env::var_os("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        let mut stale = stored_entry(Some(true), Some("2026-04-07T12:00:00.000Z"));
        stale.email = "dev.60@astronlab.com".to_string();
        stale.account_id = "acct-60".to_string();
        stale.label = "dev.60@astronlab.com_free".to_string();
        stale.auth = make_auth("dev.60@astronlab.com", "acct-60", "free");
        stale.auth.tokens.account_id = "acct-60".to_string();
        stale.last_quota_summary = Some("5h 99% left".to_string());
        stale.last_quota_primary_left_percent = Some(99);

        save_pool(&Pool {
            active_index: 0,
            accounts: vec![stale],
        })?;

        let (usage_url, handle) = spawn_usage_server(
            json!({
                "user_id": "user-60",
                "account_id": "acct-60",
                "email": "dev.60@astronlab.com",
                "plan_type": "free",
                "rate_limit": {
                    "allowed": true,
                    "limit_reached": false,
                    "primary_window": {
                        "used_percent": 60.0,
                        "limit_window_seconds": 18000,
                        "reset_after_seconds": 7200,
                        "reset_at": 0
                    },
                    "secondary_window": null
                },
                "code_review_rate_limit": null,
                "additional_rate_limits": null,
                "credits": null,
                "promo": null
            })
            .to_string(),
        );
        unsafe {
            std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", &usage_url);
        }

        let output = cmd_list()?;
        handle.join().expect("usage server should finish");

        assert!(output.contains("5h 40% left"));

        let refreshed = load_pool()?;
        assert_eq!(
            refreshed.accounts[0].last_quota_primary_left_percent,
            Some(40)
        );
        assert!(refreshed.accounts[0]
            .last_quota_summary
            .as_deref()
            .unwrap_or_default()
            .contains("5h 40% left"));
        Ok(())
    })();

    restore_env_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", previous_usage_url);
    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("list should refresh stale cached quota");
}

#[test]
fn cmd_list_prints_total_and_healthy_sections_separately() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        let mut healthy = stored_entry(Some(true), Some("2026-04-09T02:00:00.000Z"));
        healthy.label = "dev.healthy@astronlab.com_free".to_string();
        healthy.email = "dev.healthy@astronlab.com".to_string();
        healthy.account_id = "acct-healthy".to_string();
        healthy.last_quota_summary = Some("7d 88% left".to_string());
        healthy.last_quota_primary_left_percent = Some(88);
        healthy.last_quota_next_refresh_at = Some("2099-01-01T00:00:00.000Z".to_string());

        let mut exhausted = stored_entry(Some(false), Some("2026-04-09T02:00:00.000Z"));
        exhausted.label = "dev.exhausted@astronlab.com_free".to_string();
        exhausted.email = "dev.exhausted@astronlab.com".to_string();
        exhausted.account_id = "acct-exhausted".to_string();
        exhausted.last_quota_summary = Some("7d 0% left".to_string());
        exhausted.last_quota_blocker = Some("7d quota exhausted, resets in 6d".to_string());
        exhausted.last_quota_primary_left_percent = Some(0);
        exhausted.last_quota_next_refresh_at = Some("2099-01-01T00:00:00.000Z".to_string());

        save_pool(&Pool {
            active_index: 0,
            accounts: vec![healthy, exhausted],
        })?;

        let output = strip_ansi(&cmd_list()?);

        assert!(output.contains("Total Accounts"));
        assert!(output.contains("Healthy Accounts (1 account(s))"));

        let total_index = output.find("Total Accounts").expect("total section");
        let healthy_index = output
            .find("Healthy Accounts (1 account(s))")
            .expect("healthy section");
        assert!(healthy_index > total_index);

        assert_eq!(
            output
                .match_indices("dev.healthy@astronlab.com_free")
                .count(),
            2
        );
        assert_eq!(
            output
                .match_indices("dev.exhausted@astronlab.com_free")
                .count(),
            1
        );
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("list should print total and healthy sections");
}

#[test]
fn cmd_list_excludes_weekly_exhausted_accounts_from_healthy_section() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        let mut weekly_exhausted = stored_entry(Some(true), Some("2099-01-01T00:00:00.000Z"));
        weekly_exhausted.label = "dev.4@hotspotprime.com_team".to_string();
        weekly_exhausted.email = "dev.4@hotspotprime.com".to_string();
        weekly_exhausted.account_id = "acct-4".to_string();
        weekly_exhausted.plan_type = "team".to_string();
        weekly_exhausted.auth = make_auth("dev.4@hotspotprime.com", "acct-4", "team");
        weekly_exhausted.last_quota_summary =
            Some("5h 100% left, 5h | week 0% left, 3d 11h".to_string());
        weekly_exhausted.last_quota_primary_left_percent = Some(100);
        weekly_exhausted.last_quota_next_refresh_at = Some("2099-01-01T00:01:00.000Z".to_string());

        let mut healthy = stored_entry(Some(true), Some("2099-01-01T00:00:00.000Z"));
        healthy.label = "dev.6@hotspotprime.com_team".to_string();
        healthy.email = "dev.6@hotspotprime.com".to_string();
        healthy.account_id = "acct-6".to_string();
        healthy.plan_type = "team".to_string();
        healthy.auth = make_auth("dev.6@hotspotprime.com", "acct-6", "team");
        healthy.last_quota_summary =
            Some("5h 74% left, 4h 45m | week 96% left, 6d 23h".to_string());
        healthy.last_quota_primary_left_percent = Some(74);
        healthy.last_quota_next_refresh_at = Some("2099-01-01T00:01:00.000Z".to_string());

        save_pool(&Pool {
            active_index: 0,
            accounts: vec![weekly_exhausted, healthy],
        })?;

        let output = strip_ansi(&cmd_list()?);

        assert!(output.contains("Healthy Accounts (1 account(s))"));
        let healthy_index = output
            .find("Healthy Accounts (1 account(s))")
            .expect("healthy section");
        let healthy_section = &output[healthy_index..];
        assert!(healthy_section.contains("dev.6@hotspotprime.com_team"));
        assert!(!healthy_section.contains("dev.4@hotspotprime.com_team"));

        let refreshed = load_pool()?;
        assert_eq!(refreshed.accounts[0].last_quota_usable, Some(false));
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("list should exclude weekly exhausted accounts from healthy section");
}

#[test]
fn cmd_list_hides_all_accounts_from_disabled_domains() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        let mut healthy_disabled = stored_entry(Some(true), Some("2026-04-09T02:00:00.000Z"));
        healthy_disabled.label = "dev.hidden@astronlab.com_free".to_string();
        healthy_disabled.email = "dev.hidden@astronlab.com".to_string();
        healthy_disabled.account_id = "acct-hidden".to_string();
        healthy_disabled.auth = make_auth("dev.hidden@astronlab.com", "acct-hidden", "free");
        healthy_disabled.auth.tokens.account_id = "acct-hidden".to_string();
        healthy_disabled.last_quota_checked_at = Some("2099-01-01T00:00:00.000Z".to_string());

        let mut exhausted_disabled = stored_entry(Some(false), Some("2026-04-09T02:00:00.000Z"));
        exhausted_disabled.label = "dev.visible@astronlab.com_free".to_string();
        exhausted_disabled.email = "dev.visible@astronlab.com".to_string();
        exhausted_disabled.account_id = "acct-visible".to_string();
        exhausted_disabled.auth = make_auth("dev.visible@astronlab.com", "acct-visible", "free");
        exhausted_disabled.auth.tokens.account_id = "acct-visible".to_string();
        exhausted_disabled.last_quota_checked_at = Some("2099-01-01T00:00:00.000Z".to_string());

        save_pool(&Pool {
            active_index: 1,
            accounts: vec![healthy_disabled, exhausted_disabled],
        })?;
        write_disabled_domain_state()?;
        assert!(load_disabled_rotation_domains()?.contains("astronlab.com"));

        let output = strip_ansi(&cmd_list()?);

        assert!(
            output.contains("Codex OAuth Account Pool (0 account(s))"),
            "{output}"
        );
        assert!(!output.contains("dev.hidden@astronlab.com_free"));
        assert!(!output.contains("dev.visible@astronlab.com_free"));
        assert!(output.contains("Healthy Accounts (0 account(s))"));
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("list should hide disabled-domain accounts");
}

#[test]
fn cmd_list_prunes_invalidated_refresh_token_accounts_and_suspends_domain() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        let mut invalidated = stored_entry(None, Some("2026-04-14T06:08:54.124Z"));
        invalidated.label = "devbench.9@astronlab.com_free".to_string();
        invalidated.email = "devbench.9@astronlab.com".to_string();
        invalidated.account_id = "acct-invalidated".to_string();
        invalidated.auth = make_auth("devbench.9@astronlab.com", "acct-invalidated", "free");
        invalidated.auth.tokens.account_id = "acct-invalidated".to_string();
        invalidated.last_quota_blocker = Some("Token refresh failed (401): refresh_token_invalidated: Your refresh token has been invalidated. Please try signing in again.".to_string());

        write_rotate_state_json(&json!({
            "families": {
                "dev-1::devbench.{n}@astronlab.com": {
                    "profile_name": "dev-1",
                    "template": "devbench.{n}@astronlab.com",
                    "next_suffix": 10,
                    "max_skipped_slots": 0,
                    "created_at": "2026-04-13T05:00:00.000Z",
                    "updated_at": "2026-04-14T06:11:25.913Z",
                    "last_created_email": "devbench.9@astronlab.com",
                    "relogin": [],
                    "suspend_domain_on_terminal_refresh_failure": true
                }
            }
        }))?;
        save_pool(&Pool {
            active_index: 0,
            accounts: vec![invalidated],
        })?;

        let output = strip_ansi(&cmd_list()?);
        assert!(!output.contains("devbench.9@astronlab.com_free"));

        let state = load_rotate_state_json()?;
        let accounts = state["accounts"].as_array().expect("accounts");
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0]["email"], "devbench.9@astronlab.com");
        assert!(accounts[0]["auth"]["tokens"]["access_token"]
            .as_str()
            .is_some());
        assert_eq!(accounts[0]["auth"]["tokens"]["refresh_token"], "refresh");
        assert_eq!(
            state["domain"]["astronlab.com"]["rotation_enabled"],
            Value::Bool(false)
        );
        let reactivate_at = state["domain"]["astronlab.com"]["reactivate_at"]
            .as_str()
            .expect("reactivate_at");
        let parsed = DateTime::parse_from_rfc3339(reactivate_at)
            .expect("parse reactivate_at")
            .with_timezone(&Utc);
        let delta_days = (parsed - Utc::now()).num_days();
        assert!((8..=9).contains(&delta_days), "{reactivate_at}");
        assert_eq!(
            state["families"]["dev-1::devbench.{n}@astronlab.com"]["relogin"]
                .as_array()
                .map(|entries| entries.iter().filter_map(Value::as_str).collect::<Vec<_>>()),
            Some(vec!["devbench.9@astronlab.com"])
        );
        assert_eq!(
            state["families"]["dev-1::devbench.{n}@astronlab.com"]
                ["suspend_domain_on_terminal_refresh_failure"],
            Value::Bool(true)
        );
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("list should prune invalidated refresh-token accounts");
}

#[test]
fn cmd_list_prunes_reused_refresh_token_accounts() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        let mut reused = stored_entry(Some(false), Some("2099-01-01T00:00:00.000Z"));
        reused.label = "devbench.10@astronlab.com_free".to_string();
        reused.email = "devbench.10@astronlab.com".to_string();
        reused.account_id = "acct-reused".to_string();
        reused.auth = make_auth("devbench.10@astronlab.com", "acct-reused", "free");
        reused.auth.tokens.account_id = "acct-reused".to_string();
        reused.last_quota_blocker = Some(
                "Token refresh failed (401): refresh_token_reused: previous refresh token already rotated."
                    .to_string(),
            );

        write_rotate_state_json(&json!({
            "families": {
                "dev-1::devbench.{n}@astronlab.com": {
                    "profile_name": "dev-1",
                    "template": "devbench.{n}@astronlab.com",
                    "next_suffix": 11,
                    "max_skipped_slots": 0,
                    "created_at": "2026-04-13T05:00:00.000Z",
                    "updated_at": "2026-04-21T00:00:00.000Z",
                    "last_created_email": "devbench.10@astronlab.com",
                    "relogin": [],
                    "suspend_domain_on_terminal_refresh_failure": true
                }
            }
        }))?;
        save_pool(&Pool {
            active_index: 0,
            accounts: vec![reused],
        })?;

        let output = strip_ansi(&cmd_list()?);
        assert!(!output.contains("devbench.10@astronlab.com_free"));

        let state = load_rotate_state_json()?;
        let accounts = state["accounts"].as_array().expect("accounts");
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0]["email"], "devbench.10@astronlab.com");
        assert!(accounts[0]["auth"]["tokens"]["access_token"]
            .as_str()
            .is_some());
        assert_eq!(accounts[0]["auth"]["tokens"]["refresh_token"], "refresh");
        assert_eq!(
            state["domain"]["astronlab.com"]["rotation_enabled"],
            Value::Bool(false)
        );
        assert_eq!(
            state["families"]["dev-1::devbench.{n}@astronlab.com"]["relogin"]
                .as_array()
                .map(|entries| entries.iter().filter_map(Value::as_str).collect::<Vec<_>>()),
            Some(vec!["devbench.10@astronlab.com"])
        );
        assert_eq!(
            state["families"]["dev-1::devbench.{n}@astronlab.com"]
                ["suspend_domain_on_terminal_refresh_failure"],
            Value::Bool(true)
        );
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("list should prune reused refresh-token accounts");
}

#[test]
fn record_removed_account_uses_current_relogin_shape() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        write_rotate_state_json(&json!({
            "families": {
                "dev-1::devbench.{n}@astronlab.com": {
                    "profile_name": "dev-1",
                    "template": "devbench.{n}@astronlab.com",
                    "next_suffix": 10,
                    "max_skipped_slots": 0,
                    "created_at": "2026-04-13T05:00:00.000Z",
                    "updated_at": "2026-04-14T06:11:25.913Z",
                    "last_created_email": "devbench.9@astronlab.com",
                    "relogin": []
                }
            }
        }))?;

        assert!(!family_suspends_domain_on_terminal_refresh_failure(
            "devbench.9@astronlab.com"
        )?);
        assert!(record_removed_account("devbench.9@astronlab.com")?);

        let state = load_rotate_state_json()?;
        assert_eq!(
            state["families"]["dev-1::devbench.{n}@astronlab.com"]["relogin"],
            json!(["devbench.9@astronlab.com"])
        );
        assert_eq!(
            state["families"]["dev-1::devbench.{n}@astronlab.com"]
                ["suspend_domain_on_terminal_refresh_failure"],
            Value::Null
        );
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("record_removed_account should keep current relogin shape");
}

#[test]
fn cmd_list_sorts_total_accounts_by_quota_refresh_eta() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        let mut later = stored_entry(Some(true), Some("2026-04-09T02:00:00.000Z"));
        later.label = "dev.later@astronlab.com_free".to_string();
        later.email = "dev.later@astronlab.com".to_string();
        later.account_id = "acct-later".to_string();
        later.last_quota_summary = Some("7d 88% left".to_string());
        later.last_quota_primary_left_percent = Some(88);
        later.last_quota_next_refresh_at = Some("2099-01-03T00:00:00.000Z".to_string());

        let mut unknown = stored_entry(None, None);
        unknown.label = "dev.unknown@astronlab.com_free".to_string();
        unknown.email = "dev.unknown@astronlab.com".to_string();
        unknown.account_id = "acct-unknown".to_string();

        let mut sooner = stored_entry(Some(false), Some("2026-04-09T02:00:00.000Z"));
        sooner.label = "dev.sooner@astronlab.com_free".to_string();
        sooner.email = "dev.sooner@astronlab.com".to_string();
        sooner.account_id = "acct-sooner".to_string();
        sooner.last_quota_summary = Some("7d 0% left".to_string());
        sooner.last_quota_blocker = Some("7d quota exhausted, resets in 1d".to_string());
        sooner.last_quota_primary_left_percent = Some(0);
        sooner.last_quota_next_refresh_at = Some("2099-01-01T00:00:00.000Z".to_string());

        save_pool(&Pool {
            active_index: 2,
            accounts: vec![later, unknown, sooner],
        })?;

        let output = strip_ansi(&cmd_list()?);
        let total_index = output.find("Total Accounts").expect("total section");
        let healthy_index = output
            .find("Healthy Accounts (1 account(s))")
            .expect("healthy section");
        let total_section = &output[total_index..healthy_index];

        let sooner_index = total_section
            .find("dev.sooner@astronlab.com_free")
            .expect("sooner account");
        let later_index = total_section
            .find("dev.later@astronlab.com_free")
            .expect("later account");
        let unknown_index = total_section
            .find("dev.unknown@astronlab.com_free")
            .expect("unknown account");

        assert!(sooner_index < later_index);
        assert!(later_index < unknown_index);
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("list should sort total accounts by quota refresh eta");
}

#[test]
fn cmd_list_shows_next_quota_refresh_eta_when_available() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        let mut entry = stored_entry(Some(true), Some("2026-04-09T02:00:00.000Z"));
        entry.label = "dev.eta@astronlab.com_free".to_string();
        entry.email = "dev.eta@astronlab.com".to_string();
        entry.account_id = "acct-eta".to_string();
        entry.last_quota_summary = Some("7d 88% left".to_string());
        entry.last_quota_primary_left_percent = Some(88);
        entry.last_quota_next_refresh_at = Some("2099-01-03T00:00:00.000Z".to_string());

        save_pool(&Pool {
            active_index: 0,
            accounts: vec![entry],
        })?;

        let output = strip_ansi(&cmd_list()?);

        assert!(output.contains("| next refresh 2099-01-03T00:00:00.000Z"));
        assert!(!output.contains("\n    next refresh"));
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("list should show next quota refresh eta");
}

#[test]
fn cmd_list_stream_emits_account_lines_before_slow_quota_refresh_finishes() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    let previous_usage_url = std::env::var_os("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        let mut stale = stored_entry(Some(true), Some("2026-04-07T12:00:00.000Z"));
        stale.email = "dev.61@astronlab.com".to_string();
        stale.account_id = "acct-61".to_string();
        stale.label = "dev.61@astronlab.com_free".to_string();
        stale.auth = make_auth("dev.61@astronlab.com", "acct-61", "free");
        stale.auth.tokens.account_id = "acct-61".to_string();
        stale.last_quota_summary = Some("5h 99% left".to_string());
        stale.last_quota_primary_left_percent = Some(99);

        save_pool(&Pool {
            active_index: 0,
            accounts: vec![stale],
        })?;

        let (usage_url, handle) = spawn_usage_server_with_delay(
            json!({
                "user_id": "user-61",
                "account_id": "acct-61",
                "email": "dev.61@astronlab.com",
                "plan_type": "free",
                "rate_limit": {
                    "allowed": true,
                    "limit_reached": false,
                    "primary_window": {
                        "used_percent": 80.0,
                        "limit_window_seconds": 18000,
                        "reset_after_seconds": 3600,
                        "reset_at": 0
                    },
                    "secondary_window": null
                },
                "code_review_rate_limit": null,
                "additional_rate_limits": null,
                "credits": null,
                "promo": null
            })
            .to_string(),
            StdDuration::from_millis(400),
        );
        unsafe {
            std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", &usage_url);
        }

        let writer = SharedWriter::default();
        let probe_writer = writer.clone();
        let join = thread::spawn(move || {
            let mut writer = writer;
            cmd_list_stream(&mut writer)
        });

        let mut partial = String::new();
        for _ in 0..10 {
            thread::sleep(StdDuration::from_millis(100));
            partial = probe_writer.snapshot();
            if partial.contains("Codex OAuth Account Pool") {
                break;
            }
        }
        assert!(partial.contains("Codex OAuth Account Pool"));
        assert!(partial.contains("dev.61@astronlab.com"));
        assert!(!partial.contains("    \u{1b}[2mquota"));

        join.join()
            .expect("list stream thread")
            .expect("list stream");
        handle.join().expect("usage server should finish");

        let final_output = probe_writer.snapshot();
        assert!(final_output.contains("5h 20% left"));
        Ok(())
    })();

    restore_env_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", previous_usage_url);
    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("list stream should emit header before slow refresh completes");
}

#[test]
fn sync_pool_active_account_adds_missing_current_auth_to_pool() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
    }

    let result = (|| -> Result<()> {
        let mut pool = Pool {
            active_index: 0,
            accounts: vec![stored_entry(Some(true), None)],
        };

        let changed = sync_pool_current_auth_from_auth(
            &mut pool,
            make_auth("dev.35@astronlab.com", "acct-35", "free"),
            true,
        )?;

        assert!(changed);
        assert_eq!(pool.accounts.len(), 2);
        assert_eq!(pool.active_index, 1);
        assert_eq!(pool.accounts[1].email, "dev.35@astronlab.com");
        assert_eq!(pool.accounts[1].account_id, "acct-35");
        assert_eq!(pool.accounts[1].label, "dev.35@astronlab.com_free");
        Ok(())
    })();

    match previous_rotate_home {
        Some(value) => unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", value);
        },
        None => unsafe {
            std::env::remove_var("CODEX_ROTATE_HOME");
        },
    }
    result.expect("sync should materialize current auth into pool");
}

#[test]
fn sync_pool_active_account_skips_unknown_email_auth() {
    let mut pool = Pool {
        active_index: 0,
        accounts: vec![stored_entry(Some(true), None)],
    };

    let changed =
        sync_pool_current_auth_from_auth(&mut pool, make_auth("unknown", "acct-35", "free"), true)
            .expect("sync should succeed");

    assert!(!changed);
    assert_eq!(pool.accounts.len(), 1);
    assert_eq!(pool.active_index, 0);
}

#[test]
fn sync_pool_active_account_prefers_existing_active_match_over_duplicate() {
    let primary = configured_entry(
        "dev.5@hotspotprime.com",
        "acct-shared",
        "team",
        Some(true),
        Some("2026-04-07T00:00:00.000Z"),
    );
    let duplicate = primary.clone();
    let other = configured_entry(
        "dev.2.astronlab@gmail.com",
        "acct-2",
        "free",
        Some(true),
        Some("2026-04-07T00:00:00.000Z"),
    );

    let mut pool = Pool {
        active_index: 2,
        accounts: vec![duplicate, other, primary],
    };

    let changed = sync_pool_current_auth_from_auth(
        &mut pool,
        make_auth("dev.5@hotspotprime.com", "acct-shared", "team"),
        true,
    )
    .expect("sync should succeed");

    assert!(!changed);
    assert_eq!(pool.active_index, 2);
    assert_eq!(
        pool.accounts[pool.active_index].email,
        "dev.5@hotspotprime.com"
    );
}

#[test]
fn sync_pool_active_account_from_current_auth_persists_missing_auth_into_pool() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        let paths = resolve_paths()?;
        if let Some(parent) = paths.codex_auth_file.parent() {
            std::fs::create_dir_all(parent).expect("create auth parent");
        }
        write_codex_auth(
            &paths.codex_auth_file,
            &make_auth("dev.36@astronlab.com", "acct-36", "free"),
        )?;

        save_pool(&Pool {
            active_index: 0,
            accounts: vec![stored_entry(Some(true), None)],
        })?;

        let changed = sync_pool_active_account_from_current_auth()?;
        let pool = load_pool()?;

        assert!(changed);
        assert_eq!(pool.accounts.len(), 2);
        assert_eq!(pool.active_index, 1);
        assert_eq!(pool.accounts[1].email, "dev.36@astronlab.com");
        assert_eq!(pool.accounts[1].account_id, "acct-36");
        assert_eq!(pool.accounts[1].label, "dev.36@astronlab.com_free");
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("current auth sync should persist the missing pool entry");
}

#[test]
fn sync_pool_current_auth_into_pool_without_activation_preserves_active_index() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        let paths = resolve_paths()?;
        if let Some(parent) = paths.codex_auth_file.parent() {
            std::fs::create_dir_all(parent).expect("create auth parent");
        }
        write_codex_auth(
            &paths.codex_auth_file,
            &make_auth("dev.36@astronlab.com", "acct-36", "free"),
        )?;

        save_pool(&Pool {
            active_index: 0,
            accounts: vec![stored_entry(Some(true), None)],
        })?;

        let changed = sync_pool_current_auth_into_pool_without_activation()?;
        let pool = load_pool()?;

        assert!(changed);
        assert_eq!(pool.accounts.len(), 2);
        assert_eq!(pool.active_index, 0);
        assert_eq!(pool.accounts[1].email, "dev.36@astronlab.com");
        assert_eq!(pool.accounts[1].account_id, "acct-36");
        assert_eq!(pool.accounts[1].label, "dev.36@astronlab.com_free");
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("passive current auth sync should preserve the active pool entry");
}

#[test]
fn sync_pool_current_auth_into_pool_without_activation_clears_family_relogin_email() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        let paths = resolve_paths()?;
        if let Some(parent) = paths.codex_auth_file.parent() {
            std::fs::create_dir_all(parent).expect("create auth parent");
        }
        write_codex_auth(
            &paths.codex_auth_file,
            &make_auth("dev.36@astronlab.com", "acct-36", "free"),
        )?;

        let existing = configured_entry("dev.36@astronlab.com", "acct-36", "free", None, None);
        write_rotate_state_json(&json!({
            "accounts": [existing],
            "active_index": 0,
            "version": 9,
            "default_create_template": "dev.{n}@astronlab.com",
            "families": {
                "dev-1::dev.{n}@astronlab.com": {
                    "profile_name": "dev-1",
                    "template": "dev.{n}@astronlab.com",
                    "next_suffix": 37,
                    "max_skipped_slots": 0,
                    "relogin": ["dev.36@astronlab.com"],
                    "last_created_email": "dev.36@astronlab.com",
                    "created_at": "2026-04-05T00:00:00.000Z",
                    "updated_at": "2026-04-05T00:00:00.000Z"
                }
            }
        }))?;

        let _ = sync_pool_current_auth_into_pool_without_activation()?;
        let state = load_rotate_state_json()?;

        assert_eq!(
            state["families"]["dev-1::dev.{n}@astronlab.com"]["relogin"],
            json!([])
        );
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("passive current auth sync should clear matching family relogin entries");
}

#[test]
fn restore_codex_auth_from_active_pool_restores_missing_auth_file() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        save_pool(&Pool {
            active_index: 0,
            accounts: vec![AccountEntry {
                label: "dev.restore@astronlab.com_free".to_string(),
                alias: None,
                email: "dev.restore@astronlab.com".to_string(),
                account_id: "acct-restore".to_string(),
                plan_type: "free".to_string(),
                auth: make_auth("dev.restore@astronlab.com", "acct-restore", "free"),
                added_at: "2026-04-15T00:00:00.000Z".to_string(),
                last_quota_usable: None,
                last_quota_summary: None,
                last_quota_blocker: None,
                last_quota_checked_at: None,
                last_quota_primary_left_percent: None,
                last_quota_next_refresh_at: None,
                persona: None,
            }],
        })?;

        let paths = resolve_paths()?;
        assert!(!paths.codex_auth_file.exists());

        let restored = restore_codex_auth_from_active_pool()?;
        assert!(restored);
        assert!(paths.codex_auth_file.exists());

        let auth = load_codex_auth(&paths.codex_auth_file)?;
        assert_eq!(extract_account_id_from_auth(&auth), "acct-restore");
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("active pool auth should restore missing codex auth");
}

#[test]
fn normalize_pool_entries_preserves_non_gmail_target_when_auth_is_gmail() {
    let mut pool = Pool {
        active_index: 0,
        accounts: vec![AccountEntry {
            label: "devbench.12@astronlab.com_free".to_string(),
            alias: None,
            email: "devbench.12@astronlab.com".to_string(),
            account_id: "acct-12".to_string(),
            plan_type: "free".to_string(),
            auth: make_auth("1.dev.astronlab@gmail.com", "acct-12", "free"),
            added_at: "2026-04-12T00:00:00.000Z".to_string(),
            last_quota_usable: None,
            last_quota_summary: None,
            last_quota_blocker: None,
            last_quota_checked_at: None,
            last_quota_primary_left_percent: None,
            last_quota_next_refresh_at: None,
            persona: None,
        }],
    };

    let changed = normalize_pool_entries(&mut pool);

    assert!(changed);
    assert_eq!(pool.accounts[0].email, "devbench.12@astronlab.com");
    assert_eq!(pool.accounts[0].label, "devbench.12@astronlab.com_free");
    assert!(!normalize_pool_entries(&mut pool));
}

#[test]
fn normalize_pool_entries_marks_weekly_exhausted_cached_accounts_unusable() {
    let mut pool = Pool {
        active_index: 0,
        accounts: vec![AccountEntry {
            label: "dev.4@hotspotprime.com_team".to_string(),
            alias: None,
            email: "dev.4@hotspotprime.com".to_string(),
            account_id: "acct-4".to_string(),
            plan_type: "team".to_string(),
            auth: make_auth("dev.4@hotspotprime.com", "acct-4", "team"),
            added_at: "2026-04-18T00:00:00.000Z".to_string(),
            last_quota_usable: Some(true),
            last_quota_summary: Some("5h 100% left, 5h | week 0% left, 3d 11h".to_string()),
            last_quota_blocker: None,
            last_quota_checked_at: Some("2026-04-18T02:01:57.804Z".to_string()),
            last_quota_primary_left_percent: Some(100),
            last_quota_next_refresh_at: Some("2026-04-18T02:02:57.804Z".to_string()),
            persona: None,
        }],
    };

    let changed = normalize_pool_entries(&mut pool);

    assert!(changed);
    assert_eq!(pool.accounts[0].last_quota_usable, Some(false));
}

#[test]
fn normalize_pool_entries_marks_sub_three_percent_cached_accounts_unusable() {
    let mut pool = Pool {
        active_index: 0,
        accounts: vec![AccountEntry {
            label: "dev.5@hotspotprime.com_team".to_string(),
            alias: None,
            email: "dev.5@hotspotprime.com".to_string(),
            account_id: "acct-5".to_string(),
            plan_type: "team".to_string(),
            auth: make_auth("dev.5@hotspotprime.com", "acct-5", "team"),
            added_at: "2026-04-18T00:00:00.000Z".to_string(),
            last_quota_usable: Some(true),
            last_quota_summary: Some("5h 2.9% left, 8m | week 94% left, 6d 11h".to_string()),
            last_quota_blocker: None,
            last_quota_checked_at: Some("2026-04-18T02:01:57.804Z".to_string()),
            last_quota_primary_left_percent: Some(3),
            last_quota_next_refresh_at: Some("2026-04-18T02:02:57.804Z".to_string()),
            persona: None,
        }],
    };

    let changed = normalize_pool_entries(&mut pool);

    assert!(changed);
    assert_eq!(pool.accounts[0].last_quota_usable, Some(false));
}

#[test]
fn cmd_add_expected_email_preserves_target_email_against_provider_gmail_auth() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        let paths = resolve_paths()?;
        if let Some(parent) = paths.codex_auth_file.parent() {
            std::fs::create_dir_all(parent).expect("create auth parent");
        }
        write_codex_auth(
            &paths.codex_auth_file,
            &make_auth("1.dev.astronlab@gmail.com", "acct-devbench-12", "free"),
        )?;

        let output = cmd_add_expected_email("devbench.12@astronlab.com", None)?;
        let pool = load_pool()?;

        assert!(strip_ansi(&output).contains("devbench.12@astronlab.com_free"));
        assert_eq!(pool.accounts.len(), 1);
        assert_eq!(pool.active_index, 0);
        assert_eq!(pool.accounts[0].email, "devbench.12@astronlab.com");
        assert_eq!(pool.accounts[0].label, "devbench.12@astronlab.com_free");
        assert_eq!(pool.accounts[0].account_id, "acct-devbench-12");
        assert_eq!(
            extract_email_from_auth(&pool.accounts[0].auth),
            "1.dev.astronlab@gmail.com"
        );
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("cmd_add_expected_email should preserve the target email");
}

#[test]
fn cmd_add_expected_email_preserves_target_gmail_plus_family_against_provider_gmail_auth() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        let paths = resolve_paths()?;
        if let Some(parent) = paths.codex_auth_file.parent() {
            std::fs::create_dir_all(parent).expect("create auth parent");
        }
        write_codex_auth(
            &paths.codex_auth_file,
            &make_auth("1.dev.astronlab+6@gmail.com", "acct-dev-gmail-6", "free"),
        )?;

        let output = cmd_add_expected_email("dev3astronlab+6@gmail.com", None)?;
        let pool = load_pool()?;

        assert!(strip_ansi(&output).contains("dev3astronlab+6@gmail.com_free"));
        assert_eq!(pool.accounts.len(), 1);
        assert_eq!(pool.active_index, 0);
        assert_eq!(pool.accounts[0].email, "dev3astronlab+6@gmail.com");
        assert_eq!(pool.accounts[0].label, "dev3astronlab+6@gmail.com_free");
        assert_eq!(pool.accounts[0].account_id, "acct-dev-gmail-6");
        assert_eq!(
            extract_email_from_auth(&pool.accounts[0].auth),
            "1.dev.astronlab+6@gmail.com"
        );
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("cmd_add_expected_email should preserve the gmail-plus target email");
}

#[test]
fn current_pool_overview_counts_cached_healthy_accounts() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        save_pool(&Pool {
            active_index: 1,
            accounts: vec![
                stored_entry(Some(false), Some("2026-04-08T12:00:00.000Z")),
                stored_entry(Some(true), Some("2026-04-08T12:00:00.000Z")),
                stored_entry(Some(true), Some("2026-04-08T12:00:00.000Z")),
                stored_entry(None, None),
            ],
        })?;

        let overview = current_pool_overview()?;
        assert_eq!(overview.inventory_count, 4);
        assert_eq!(overview.inventory_active_slot, Some(2));
        assert_eq!(overview.inventory_healthy_count, 2);
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("overview should count healthy accounts");
}

#[test]
fn current_pool_overview_hides_all_accounts_from_disabled_domains() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        let mut healthy_disabled = stored_entry(Some(true), Some("2026-04-08T12:00:00.000Z"));
        healthy_disabled.label = "dev.hidden@astronlab.com_free".to_string();
        healthy_disabled.email = "dev.hidden@astronlab.com".to_string();
        healthy_disabled.account_id = "acct-hidden".to_string();
        healthy_disabled.auth = make_auth("dev.hidden@astronlab.com", "acct-hidden", "free");
        healthy_disabled.auth.tokens.account_id = "acct-hidden".to_string();

        let mut healthy_enabled = stored_entry(Some(true), Some("2026-04-08T12:00:00.000Z"));
        healthy_enabled.label = "dev.visible@gmail.com_plus".to_string();
        healthy_enabled.email = "dev.visible@gmail.com".to_string();
        healthy_enabled.account_id = "acct-visible".to_string();
        healthy_enabled.auth = make_auth("dev.visible@gmail.com", "acct-visible", "plus");
        healthy_enabled.auth.tokens.account_id = "acct-visible".to_string();

        let mut exhausted_disabled = stored_entry(Some(false), Some("2026-04-08T12:00:00.000Z"));
        exhausted_disabled.label = "dev.exhausted@astronlab.com_free".to_string();
        exhausted_disabled.email = "dev.exhausted@astronlab.com".to_string();
        exhausted_disabled.account_id = "acct-exhausted".to_string();
        exhausted_disabled.auth =
            make_auth("dev.exhausted@astronlab.com", "acct-exhausted", "free");
        exhausted_disabled.auth.tokens.account_id = "acct-exhausted".to_string();

        save_pool(&Pool {
            active_index: 0,
            accounts: vec![healthy_disabled, healthy_enabled, exhausted_disabled],
        })?;
        write_disabled_domain_state()?;
        assert!(load_disabled_rotation_domains()?.contains("astronlab.com"));

        let overview = current_pool_overview()?;
        assert_eq!(overview.inventory_count, 1);
        assert_eq!(overview.inventory_active_slot, None);
        assert_eq!(overview.inventory_healthy_count, 1);
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("overview should hide disabled-domain accounts");
}

#[test]
fn rotate_next_skips_disabled_domain_accounts() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");
    let previous_usage_url = std::env::var_os("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        write_disabled_domain_state()?;
        save_pool(&Pool {
            active_index: 0,
            accounts: vec![
                configured_entry(
                    "dev.1@astronlab.com",
                    "acct-1",
                    "free",
                    Some(false),
                    Some("2026-04-07T00:00:00.000Z"),
                ),
                configured_entry(
                    "dev.user@gmail.com",
                    "acct-gmail",
                    "free",
                    Some(true),
                    Some("2026-04-07T00:00:00.000Z"),
                ),
            ],
        })?;

        let (usage_url, handle) = spawn_usage_server(
            json!({
                "user_id": "user-gmail",
                "account_id": "acct-gmail",
                "email": "dev.user@gmail.com",
                "plan_type": "free",
                "rate_limit": {
                    "allowed": true,
                    "limit_reached": false,
                    "primary_window": {
                        "used_percent": 20.0,
                        "limit_window_seconds": 18000,
                        "reset_after_seconds": 7200,
                        "reset_at": 0
                    },
                    "secondary_window": null
                },
                "code_review_rate_limit": null,
                "additional_rate_limits": null,
                "credits": null,
                "promo": null
            })
            .to_string(),
        );
        unsafe {
            std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", &usage_url);
        }

        let output = rotate_next_internal_with_progress(None)?;
        handle.join().expect("usage server should finish");

        match output {
            NextResult::Rotated { summary, .. } => {
                assert_eq!(summary.email, "dev.user@gmail.com");
            }
            _ => panic!("expected rotation result"),
        }

        let refreshed = load_pool()?;
        assert_eq!(refreshed.active_index, 1);
        assert_eq!(refreshed.accounts[1].email, "dev.user@gmail.com");
        Ok(())
    })();

    restore_env_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", previous_usage_url);
    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("next should skip disabled domains");
}

#[test]
fn rotate_next_fails_when_only_disabled_targets_remain() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        write_disabled_domain_state()?;
        save_pool(&Pool {
            active_index: 0,
            accounts: vec![
                configured_entry(
                    "dev.1@astronlab.com",
                    "acct-1",
                    "free",
                    Some(true),
                    Some("2026-04-07T00:00:00.000Z"),
                ),
                configured_entry(
                    "dev.2@astronlab.com",
                    "acct-2",
                    "free",
                    Some(true),
                    Some("2026-04-07T00:00:00.000Z"),
                ),
            ],
        })?;

        let error = match rotate_next_internal_with_progress(None) {
            Ok(_) => panic!("expected disabled-domain rotation error"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("No rotation target is available because rotation is disabled"));
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("next should fail when all targets are disabled");
}

#[test]
fn cmd_prev_fails_when_only_disabled_targets_remain() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        write_disabled_domain_state()?;
        save_pool(&Pool {
            active_index: 1,
            accounts: vec![
                configured_entry(
                    "dev.1@astronlab.com",
                    "acct-1",
                    "free",
                    Some(true),
                    Some("2026-04-07T00:00:00.000Z"),
                ),
                configured_entry(
                    "dev.user@gmail.com",
                    "acct-gmail",
                    "free",
                    Some(true),
                    Some("2026-04-07T00:00:00.000Z"),
                ),
            ],
        })?;

        let error = cmd_prev().unwrap_err();
        assert!(error
            .to_string()
            .contains("No rotation target is available because rotation is disabled"));
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("prev should fail when all previous targets are disabled");
}

#[test]
fn other_usable_account_exists_ignores_disabled_domains() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let codex_home = tempdir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).expect("create codex home");

    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    let previous_codex_home = std::env::var_os("CODEX_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", tempdir.path());
        std::env::set_var("CODEX_HOME", &codex_home);
    }

    let result = (|| -> Result<()> {
        write_disabled_domain_state()?;
        save_pool(&Pool {
            active_index: 0,
            accounts: vec![
                configured_entry(
                    "dev.user@gmail.com",
                    "acct-gmail",
                    "free",
                    Some(false),
                    Some("2026-04-07T00:00:00.000Z"),
                ),
                configured_entry(
                    "dev.1@astronlab.com",
                    "acct-1",
                    "free",
                    Some(true),
                    Some("2026-04-07T00:00:00.000Z"),
                ),
            ],
        })?;

        assert!(!other_usable_account_exists()?);
        Ok(())
    })();

    restore_env_var("CODEX_HOME", previous_codex_home);
    restore_env_var("CODEX_ROTATE_HOME", previous_rotate_home);
    result.expect("disabled domains should not count as reusable accounts");
}

#[test]
fn rollback_prepared_rotation_restores_previous_auth_and_active_index() {
    let _guard = RotateHomeGuard::enter("codex-rotate-rollback-prepared");
    let previous = configured_entry("dev.1@astronlab.com", "acct-1", "free", Some(true), None);
    let target = configured_entry("dev.2@astronlab.com", "acct-2", "free", Some(true), None);

    let pool = Pool {
        active_index: 0,
        accounts: vec![previous.clone(), target.clone()],
    };
    save_pool(&pool).expect("save initial pool");

    let paths = resolve_paths().expect("resolve paths");
    if let Some(parent) = paths.codex_auth_file.parent() {
        std::fs::create_dir_all(parent).expect("create auth parent");
    }
    write_codex_auth(&paths.codex_auth_file, &previous.auth).expect("write initial auth");

    let prepared = PreparedRotation {
        action: PreparedRotationAction::Switch,
        pool: pool.clone(),
        previous_index: 0,
        target_index: 1,
        previous: previous.clone(),
        target: target.clone(),
        message: "rotating".to_string(),
        persist_pool: false,
    };

    // Simulate a partial activation: auth is written but pool is not committed
    write_codex_auth(&paths.codex_auth_file, &target.auth).expect("write target auth");

    rollback_prepared_rotation(&prepared).expect("rollback");

    let restored_auth = load_codex_auth(&paths.codex_auth_file).expect("load restored auth");
    assert_eq!(extract_account_id_from_auth(&restored_auth), "acct-1");

    let restored_pool = load_pool().expect("load restored pool");
    assert_eq!(restored_pool.active_index, 0);
}

#[test]
fn validate_persona_egress_fails_when_region_mismatches_in_validate_mode() {
    let mut persona = PersonaEntry::default();
    persona.expected_region_code = Some("US".to_string());

    // We will mock the egress check to return "GB"
    let result =
        validate_persona_egress_with_actual(&persona, VmExpectedEgressMode::Validate, "GB");
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("expected US, found GB"));
}

#[test]
fn validate_persona_egress_succeeds_when_region_matches_in_validate_mode() {
    let mut persona = PersonaEntry::default();
    persona.expected_region_code = Some("US".to_string());

    let result =
        validate_persona_egress_with_actual(&persona, VmExpectedEgressMode::Validate, "US");
    assert!(result.is_ok());
}

#[test]
fn validate_persona_egress_succeeds_in_provision_only_mode_even_if_region_mismatches() {
    let mut persona = PersonaEntry::default();
    persona.expected_region_code = Some("US".to_string());

    let result =
        validate_persona_egress_with_actual(&persona, VmExpectedEgressMode::ProvisionOnly, "GB");
    assert!(result.is_ok());
}
