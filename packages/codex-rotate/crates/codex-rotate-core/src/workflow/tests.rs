use super::*;
use crate::test_support::{RotateHomeGuard, ENV_MUTEX};
use base64::Engine;
use std::fs;
use std::path::Path;
use std::process::Command as ProcessCommand;
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{stamp}"))
}

fn with_rotate_home<T>(prefix: &str, test: impl FnOnce(&Path) -> T) -> T {
    let guard = RotateHomeGuard::enter(prefix);
    if let Some(home) = dirs::home_dir() {
        assert_ne!(guard.path(), home.join(".codex-rotate"));
    }
    test(guard.path())
}

#[test]
fn with_rotate_home_never_uses_live_rotate_home() {
    with_rotate_home("codex-rotate-guarded-home", |rotate_home| {
        let configured = std::env::var_os("CODEX_ROTATE_HOME").expect("rotate home env");
        assert_eq!(Path::new(&configured), rotate_home);
        if let Some(home) = dirs::home_dir() {
            assert_ne!(rotate_home, home.join(".codex-rotate"));
        }
    });
}

fn make_pending(
    email: &str,
    profile_name: &str,
    template: &str,
    suffix: u32,
    created_at: &str,
) -> PendingCredential {
    PendingCredential {
        stored: StoredCredential {
            email: email.to_string(),
            profile_name: profile_name.to_string(),
            template: template.to_string(),
            suffix,
            selector: None,
            alias: None,
            birth_month: None,
            birth_day: None,
            birth_year: None,
            created_at: created_at.to_string(),
            updated_at: created_at.to_string(),
        },
        started_at: Some(created_at.to_string()),
    }
}

fn make_jwt(payload: &str) -> String {
    format!(
        "{}.{}.signature",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"none","typ":"JWT"}"#),
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload)
    )
}

fn make_auth(email: &str, account_id: &str) -> CodexAuth {
    CodexAuth {
        auth_mode: "chatgpt".to_string(),
        openai_api_key: None,
        tokens: crate::auth::AuthTokens {
            access_token: make_jwt(&format!(
                r#"{{"https://api.openai.com/profile":{{"email":"{email}"}},"https://api.openai.com/auth":{{"chatgpt_account_id":"{account_id}","chatgpt_plan_type":"free"}}}}"#
            )),
            id_token: make_jwt(&format!(r#"{{"email":"{email}"}}"#)),
            refresh_token: Some(format!("refresh-{account_id}")),
            account_id: account_id.to_string(),
        },
        last_refresh: "2026-04-13T02:52:15.012Z".to_string(),
    }
}

fn make_account_entry(email: &str, account_id: &str) -> AccountEntry {
    AccountEntry {
        label: format!("{email}_free"),
        alias: None,
        email: email.to_string(),
        account_id: account_id.to_string(),
        plan_type: "free".to_string(),
        auth: make_auth(email, account_id),
        added_at: "2026-04-13T02:52:15.012Z".to_string(),
        last_quota_usable: Some(true),
        last_quota_summary: Some("5h 90% left".to_string()),
        last_quota_blocker: None,
        last_quota_checked_at: Some("2026-04-13T02:52:15.012Z".to_string()),
        last_quota_primary_left_percent: Some(90),
        last_quota_next_refresh_at: Some("2026-04-13T03:52:15.012Z".to_string()),
        persona: None,
    }
}

fn repo_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("..")
        .join("..")
        .canonicalize()
        .expect("repo root")
}

#[test]
fn codex_bin_uses_explicit_override_before_app_bundle() {
    let app_root = unique_temp_dir("codex-rotate-codex-bin-override");
    fs::create_dir_all(&app_root).expect("create app root");
    let app_path = app_root.join("Codex");
    fs::write(&app_path, "stub").expect("write app bundle stub");

    let resolved = resolve_codex_bin_with_paths(Some("/tmp/custom-codex"), &app_path);
    assert_eq!(resolved, "/tmp/custom-codex");

    fs::remove_dir_all(&app_root).ok();
}

#[test]
fn codex_bin_prefers_app_bundle_when_present() {
    let app_root = unique_temp_dir("codex-rotate-codex-bin-app");
    fs::create_dir_all(&app_root).expect("create app root");
    let app_path = app_root.join("Codex");
    fs::write(&app_path, "stub").expect("write app bundle stub");

    let resolved = resolve_codex_bin_with_paths(None, &app_path);
    assert_eq!(resolved, app_path.to_string_lossy());

    fs::remove_dir_all(&app_root).ok();
}

#[test]
fn codex_bin_falls_back_to_bare_codex_when_app_bundle_is_absent() {
    let app_root = unique_temp_dir("codex-rotate-codex-bin-fallback");
    let app_path = app_root.join("Codex");

    let resolved = resolve_codex_bin_with_paths(None, &app_path);
    assert_eq!(resolved, DEFAULT_CODEX_BIN);
}

#[test]
fn stored_credentials_used_only_for_non_manual_relogin() {
    let stored = StoredCredential {
        email: "dev.user+1@gmail.com".to_string(),
        profile_name: "dev-1".to_string(),
        template: "dev.user+{n}@gmail.com".to_string(),
        suffix: 1,
        selector: Some("dev.user+1@gmail.com_free".to_string()),
        alias: None,
        birth_month: None,
        birth_day: None,
        birth_year: None,
        created_at: "2026-03-20T00:00:00.000Z".to_string(),
        updated_at: "2026-03-20T00:00:00.000Z".to_string(),
    };

    assert!(should_use_stored_credential_relogin(
        Some(&stored),
        &ReloginOptions::default()
    ));
    assert!(!should_use_stored_credential_relogin(
        Some(&stored),
        &ReloginOptions {
            manual_login: true,
            ..ReloginOptions::default()
        }
    ));
}

#[test]
fn pending_relogin_with_stored_credential_prefers_login_recovery_not_signup() {
    let pending = make_pending(
        "dev3astronlab+5@gmail.com",
        "dev-1",
        "dev3astronlab+{n}@gmail.com",
        5,
        "2026-04-14T15:12:25.003Z",
    );
    let stored = pending.stored.clone();

    assert!(!prefer_signup_recovery_for_relogin(
        Some(&pending),
        Some(&stored),
    ));
}

#[test]
fn synthesized_pending_relogin_without_stored_credential_can_prefer_signup_recovery() {
    let pending = make_pending(
        "dev3astronlab+6@gmail.com",
        "dev-1",
        "dev3astronlab+{n}@gmail.com",
        6,
        "2026-04-14T16:12:25.003Z",
    );

    assert!(prefer_signup_recovery_for_relogin(Some(&pending), None));
    assert!(!prefer_signup_recovery_for_relogin(None, None));
}

#[test]
fn pending_signup_recovery_relogin_prepares_password_only_when_needed() {
    let pending = make_pending(
        "dev3astronlab+6@gmail.com",
        "dev-1",
        "dev3astronlab+{n}@gmail.com",
        6,
        "2026-04-14T16:12:25.003Z",
    );

    assert!(should_prepare_signup_recovery_password(
        Some(&pending),
        true,
        false
    ));
    assert!(!should_prepare_signup_recovery_password(
        Some(&pending),
        false,
        false
    ));
    assert!(!should_prepare_signup_recovery_password(
        Some(&pending),
        true,
        true
    ));
    assert!(!should_prepare_signup_recovery_password(None, true, false));
}

#[test]
fn read_workflow_file_metadata_reads_preferred_profile_from_main_workflow() {
    let workflow_file = repo_root()
        .join(".fast-browser")
        .join("workflows")
        .join("web")
        .join("auth.openai.com")
        .join("codex-rotate-account-flow-main.yaml");

    let metadata = read_workflow_file_metadata(&workflow_file).expect("workflow metadata");

    assert_eq!(
        metadata.workflow_ref.as_deref(),
        Some("workspace.web.auth-openai-com.codex-rotate-account-flow-main")
    );
    assert_eq!(metadata.preferred_profile_name.as_deref(), Some("dev-1"));
    assert_eq!(metadata.preferred_email, None);
    assert_eq!(metadata.default_full_name.as_deref(), Some("Dev Astronlab"));
    assert_eq!(metadata.default_birth_month, Some(1));
    assert_eq!(metadata.default_birth_day, Some(24));
    assert_eq!(metadata.default_birth_year, Some(1990));
}

#[test]
fn derive_workflow_ref_from_file_path_handles_alternate_local_workflow() {
    let workflow_file = repo_root()
        .join(".fast-browser")
        .join("workflows")
        .join("web")
        .join("auth.openai.com")
        .join("codex-rotate-account-flow-minimal.yaml");

    assert_eq!(
        derive_workflow_ref_from_file_path(&workflow_file).as_deref(),
        Some("workspace.web.auth-openai-com.codex-rotate-account-flow-minimal")
    );
}

#[test]
fn derive_workflow_ref_from_file_path_honors_bridge_repo_root_override() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let tempdir = tempfile::tempdir().expect("tempdir");
    let main_root = tempdir.path().join("main-root");
    let bridge_root = tempdir.path().join("bridge-root");

    for root in [&main_root, &bridge_root] {
        fs::create_dir_all(root.join("packages").join("codex-rotate")).expect("create asset root");
        fs::create_dir_all(
            root.join(".fast-browser")
                .join("workflows")
                .join("web")
                .join("auth.openai.com"),
        )
        .expect("create workflow dir");
    }

    let workflow_file = bridge_root
        .join(".fast-browser")
        .join("workflows")
        .join("web")
        .join("auth.openai.com")
        .join("codex-rotate-account-flow-main.yaml");
    fs::write(
        &workflow_file,
        "document:\n  metadata:\n    preferredProfile: dev-1\n",
    )
    .expect("write workflow file");

    let previous_repo_root = std::env::var_os("CODEX_ROTATE_REPO_ROOT");
    let previous_bridge_root = std::env::var_os("CODEX_ROTATE_BRIDGE_REPO_ROOT");
    unsafe {
        std::env::set_var("CODEX_ROTATE_REPO_ROOT", &main_root);
        std::env::set_var("CODEX_ROTATE_BRIDGE_REPO_ROOT", &bridge_root);
    }

    let derived = derive_workflow_ref_from_file_path(&workflow_file);

    match previous_repo_root {
        Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_REPO_ROOT", value) },
        None => unsafe { std::env::remove_var("CODEX_ROTATE_REPO_ROOT") },
    }
    match previous_bridge_root {
        Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_BRIDGE_REPO_ROOT", value) },
        None => unsafe { std::env::remove_var("CODEX_ROTATE_BRIDGE_REPO_ROOT") },
    }

    assert_eq!(
        derived.as_deref(),
        Some("workspace.web.auth-openai-com.codex-rotate-account-flow-main")
    );
}

#[test]
fn parse_workflow_file_metadata_handles_quotes_and_comments() {
    let metadata = parse_workflow_file_metadata(
        r#"
document:
  metadata:
    preferredProfile: "dev-1" # comment
    preferredEmail: 'dev.41@astronlab.com'
    targets:
      - id: primary
input:
  schema:
    document:
      properties:
        full_name:
          default: "Dev Astronlab"
        birth_month:
          default: 1
        birth_day:
          default: 24
        birth_year:
          default: 1990
"#,
    );

    assert_eq!(metadata.preferred_profile_name.as_deref(), Some("dev-1"));
    assert_eq!(
        metadata.preferred_email.as_deref(),
        Some("dev.41@astronlab.com")
    );
    assert_eq!(metadata.default_full_name.as_deref(), Some("Dev Astronlab"));
    assert_eq!(metadata.default_birth_month, Some(1));
    assert_eq!(metadata.default_birth_day, Some(24));
    assert_eq!(metadata.default_birth_year, Some(1990));
}

#[test]
fn resolve_login_workflow_defaults_uses_explicit_value() {
    let defaults =
        resolve_login_workflow_defaults(Some("workspace.web.auth-openai-com.custom-flow"))
            .expect("login workflow defaults");

    assert_eq!(
        defaults.workflow_ref,
        "workspace.web.auth-openai-com.custom-flow"
    );
    assert_eq!(defaults.full_name, "Dev Astronlab");
    assert_eq!(
        defaults.birth_date,
        AdultBirthDate {
            birth_month: 1,
            birth_day: 24,
            birth_year: 1990,
        }
    );
}

#[test]
fn resolve_login_workflow_defaults_falls_back_to_default_for_missing_or_blank_values() {
    let defaults = resolve_login_workflow_defaults(None).expect("login workflow defaults");
    assert_eq!(
        defaults.workflow_ref,
        "workspace.web.auth-openai-com.codex-rotate-account-flow-main"
    );
    assert_eq!(defaults.full_name, "Dev Astronlab");
    assert_eq!(
        resolve_login_workflow_defaults(Some("   "))
            .expect("login workflow defaults")
            .workflow_ref,
        "workspace.web.auth-openai-com.codex-rotate-account-flow-main"
    );
}

#[test]
fn reads_auth_flow_summary_from_raw_fast_browser_output() {
    let result = FastBrowserRunResult {
        output: Some(json!({
            "stage": "email_verification",
            "success": false,
            "account_ready": true,
            "next_action": "retry_verification",
            "codex_session": {
                "auth_url": "https://auth.openai.com",
                "callback_port": "8765",
                "pid": "4321",
                "session_dir": "/tmp/codex-session"
            }
        })),
        ..FastBrowserRunResult::default()
    };

    let summary = read_codex_rotate_auth_flow_summary(&result);
    assert_eq!(summary.stage.as_deref(), Some("email_verification"));
    assert_eq!(summary.success, Some(false));
    assert_eq!(summary.account_ready, Some(true));
    assert_eq!(summary.next_action.as_deref(), Some("retry_verification"));
    assert_eq!(
        summary
            .codex_session
            .as_ref()
            .and_then(|session| session.callback_port),
        Some(8765)
    );
    assert_eq!(
        summary
            .codex_session
            .as_ref()
            .and_then(|session| session.pid),
        Some(4321)
    );
}

#[test]
fn deserializes_bridge_login_attempt_result_with_extra_fast_browser_fields() {
    let payload = json!({
        "result": {
            "ok": true,
            "workflow": {
                "ref": "workspace.web.auth-openai-com.codex-rotate-account-flow-device-auth"
            },
            "finalUrl": "https://chatgpt.com/#settings/Security",
            "output": {
                "success": true,
                "next_action": "complete",
                "codex_session": {
                    "auth_url": "https://auth.openai.com/authorize",
                    "callback_port": "8765"
                }
            },
            "mode": "managed",
            "steps": {},
            "artifactMode": "full",
            "state": {
                "steps": {
                    "start_codex_login_session": {
                        "action": {
                            "value": {
                                "auth_url": "https://auth.openai.com/authorize",
                                "callback_port": "8765",
                                "pid": "4321"
                            }
                        }
                    }
                }
            },
            "runtime_profiles": {
                "default": "dev-1"
            },
            "observability": {
                "runId": "demo"
            }
        },
        "error_message": null,
        "managed_runtime_reset_performed": false
    });

    let result: BridgeLoginAttemptResult =
        serde_json::from_value(payload).expect("bridge payload should deserialize");
    let normalized = result.result.expect("normalized fast-browser result");
    let summary = read_codex_rotate_auth_flow_summary(&normalized);
    let session = read_codex_rotate_auth_flow_session(&normalized).expect("session payload");

    assert_eq!(summary.success, Some(true));
    assert_eq!(summary.next_action.as_deref(), Some("complete"));
    assert_eq!(session.callback_port, Some(8765));
    assert_eq!(session.pid, Some(4321));
}

#[test]
fn deserializes_bridge_login_attempt_result_from_bare_fast_browser_result() {
    let payload = json!({
        "ok": true,
        "status": "completed",
        "output": {
            "success": true,
            "next_action": "complete"
        },
        "state": {
            "steps": {
                "start_codex_login_session": {
                    "action": {
                        "value": {
                            "auth_url": "https://auth.openai.com/authorize",
                            "callback_port": "8765"
                        }
                    }
                }
            }
        }
    });

    let result: BridgeLoginAttemptResult =
        serde_json::from_value(payload).expect("bare fast-browser result should deserialize");
    let normalized = result.result.expect("normalized fast-browser result");
    let summary = read_codex_rotate_auth_flow_summary(&normalized);
    let session = read_codex_rotate_auth_flow_session(&normalized).expect("session payload");

    assert_eq!(summary.success, Some(true));
    assert_eq!(summary.next_action.as_deref(), Some("complete"));
    assert_eq!(session.callback_port, Some(8765));
}

#[test]
fn deserializes_bridge_login_attempt_result_with_stringified_fast_browser_result() {
    let payload = json!({
        "result": serde_json::to_string(&json!({
            "ok": true,
            "output": {
                "success": true,
                "next_action": "complete"
            }
        }))
        .expect("stringify payload"),
        "error_message": null
    });

    let result: BridgeLoginAttemptResult = serde_json::from_value(payload)
        .expect("stringified fast-browser result should deserialize");
    let normalized = result.result.expect("normalized fast-browser result");
    let summary = read_codex_rotate_auth_flow_summary(&normalized);

    assert_eq!(summary.success, Some(true));
    assert_eq!(summary.next_action.as_deref(), Some("complete"));
}

#[test]
fn reads_auth_flow_session_from_start_step_when_summary_omits_it() {
    let result = FastBrowserRunResult {
        state: Some(FastBrowserState {
            steps: HashMap::from([(
                "start_codex_login_session".to_string(),
                FastBrowserStepState {
                    action: Some(json!({
                        "value": {
                            "auth_url": "https://auth.openai.com",
                            "callback_port": "7654",
                            "pid": "2468",
                            "stdout_path": "/tmp/codex.stdout"
                        }
                    })),
                },
            )]),
        }),
        ..FastBrowserRunResult::default()
    };

    let session = read_codex_rotate_auth_flow_session(&result).expect("session");
    assert_eq!(session.auth_url.as_deref(), Some("https://auth.openai.com"));
    assert_eq!(session.callback_port, Some(7654));
    assert_eq!(session.pid, Some(2468));
    assert_eq!(session.stdout_path.as_deref(), Some("/tmp/codex.stdout"));
}

#[test]
fn reads_auth_flow_summary_from_finalize_step_when_output_is_missing() {
    let result = FastBrowserRunResult {
        state: Some(FastBrowserState {
            steps: HashMap::from([(
                "finalize_selected_flow".to_string(),
                FastBrowserStepState {
                    action: Some(json!({
                        "ok": true,
                        "value": {
                            "stage": "success",
                            "callback_complete": true,
                            "success": true,
                            "next_action": "complete",
                            "verified_account_email": "dev.49@astronlab.com",
                            "codex_session": {
                                "auth_url": "https://auth.openai.com/authorize",
                                "callback_port": "8765",
                                "pid": "4321"
                            }
                        }
                    })),
                },
            )]),
        }),
        ..FastBrowserRunResult::default()
    };

    let summary = read_codex_rotate_auth_flow_summary(&result);
    let session = read_codex_rotate_auth_flow_session(&result).expect("session payload");

    assert_eq!(summary.stage.as_deref(), Some("success"));
    assert_eq!(summary.callback_complete, Some(true));
    assert_eq!(summary.success, Some(true));
    assert_eq!(summary.next_action.as_deref(), Some("complete"));
    assert_eq!(
        summary.verified_account_email.as_deref(),
        Some("dev.49@astronlab.com")
    );
    assert_eq!(session.callback_port, Some(8765));
    assert_eq!(session.pid, Some(4321));
}

#[test]
fn reads_auth_flow_summary_from_recent_events_when_output_and_state_summary_are_missing() {
    let result = FastBrowserRunResult {
        recent_events: Some(vec![json!({
            "step_id": "finalize_flow_summary",
            "phase": "action",
            "status": "ok",
            "details": {
                "result": {
                    "value": {
                        "stage": "add_phone",
                        "next_action": "skip_account",
                        "replay_reason": "add_phone",
                        "error_message": "OpenAI still requires phone setup before the Codex callback can complete.",
                        "callback_complete": false,
                        "success": false
                    }
                }
            }
        })]),
        ..FastBrowserRunResult::default()
    };

    let summary = read_codex_rotate_auth_flow_summary(&result);

    assert_eq!(summary.stage.as_deref(), Some("add_phone"));
    assert_eq!(summary.callback_complete, Some(false));
    assert_eq!(summary.success, Some(false));
    assert_eq!(summary.next_action.as_deref(), Some("skip_account"));
    assert_eq!(summary.replay_reason.as_deref(), Some("add_phone"));
}

#[test]
fn hydrates_auth_flow_recent_events_from_observability_run_path() {
    let fixture_root = unique_temp_dir("codex-rotate-observability-recent-events");
    fs::create_dir_all(&fixture_root).expect("create fixture root");
    let run_path = fixture_root.join("run.json");
    fs::write(
            &run_path,
            serde_json::to_string(&json!({
                "final_url": "https://auth.openai.com/add-phone",
                "recent_events": [
                    {
                        "step_id": "finalize_flow_summary",
                        "phase": "action",
                        "status": "ok",
                        "details": {
                            "result": {
                                "value": {
                                    "stage": "add_phone",
                                    "next_action": "skip_account",
                                    "replay_reason": "add_phone",
                                    "error_message": "OpenAI still requires phone setup before the Codex callback can complete.",
                                    "callback_complete": false,
                                    "success": false
                                }
                            }
                        }
                    }
                ]
            }))
            .expect("serialize run payload"),
        )
        .expect("write run payload");

    let bridge_payload = json!({
        "result": {
            "ok": true,
            "status": "completed",
            "observability": {
                "run_path": run_path,
                "status_path": run_path,
            }
        }
    });

    let result: BridgeLoginAttemptResult =
        serde_json::from_value(bridge_payload).expect("bridge payload should deserialize");
    let summary =
        read_codex_rotate_auth_flow_summary(result.result.as_ref().expect("fast-browser result"));

    assert_eq!(summary.stage.as_deref(), Some("add_phone"));
    assert_eq!(summary.next_action.as_deref(), Some("skip_account"));

    let _ = fs::remove_dir_all(&fixture_root);
}

#[test]
fn reads_auth_flow_summary_from_device_auth_callback_metadata_when_output_is_missing() {
    let result = FastBrowserRunResult {
        final_url: Some("https://auth.openai.com/deviceauth/callback".to_string()),
        page: Some(json!({
            "url": "https://auth.openai.com/deviceauth/callback",
            "title": "Signed in to Codex",
            "text": "Signed in to Codex\nYou may now close this page"
        })),
        ..FastBrowserRunResult::default()
    };

    let summary = read_codex_rotate_auth_flow_summary(&result);

    assert_eq!(summary.stage.as_deref(), Some("success"));
    assert_eq!(
        summary.current_url.as_deref(),
        Some("https://auth.openai.com/deviceauth/callback")
    );
    assert_eq!(summary.callback_complete, Some(true));
    assert_eq!(summary.success, Some(true));
    assert_eq!(summary.next_action.as_deref(), Some("complete"));
}

#[test]
fn reads_auth_flow_summary_from_camel_case_localhost_final_url_when_output_is_missing() {
    let bridge_payload = json!({
        "result": {
            "ok": true,
            "finalUrl": "http://localhost:1455/success",
            "page": {
                "url": "https://auth.openai.com/sign-in-with-chatgpt/codex/consent",
                "title": "Signed in to Codex",
                "text": "Signed in to Codex\nYou may now close this page"
            }
        }
    });

    let result: BridgeLoginAttemptResult =
        serde_json::from_value(bridge_payload).expect("bridge payload should deserialize");
    let summary =
        read_codex_rotate_auth_flow_summary(result.result.as_ref().expect("fast-browser result"));

    assert_eq!(summary.stage.as_deref(), Some("success"));
    assert_eq!(
        summary.current_url.as_deref(),
        Some("http://localhost:1455/success")
    );
    assert_eq!(summary.callback_complete, Some(true));
    assert_eq!(summary.success, Some(true));
    assert_eq!(summary.next_action.as_deref(), Some("complete"));
}

#[test]
fn hydrates_auth_flow_summary_from_snake_case_observability_run_path() {
    let fixture_root = unique_temp_dir("codex-rotate-observability-hydrate");
    fs::create_dir_all(&fixture_root).expect("create fixture root");
    let run_path = fixture_root.join("run.json");
    fs::write(
        &run_path,
        serde_json::to_string(&json!({
            "final_url": "http://localhost:1455/success",
            "page": {
                "url": "http://localhost:1455/success",
                "title": "Signed in to Codex",
                "text": "Signed in to Codex\nYou may now close this page"
            }
        }))
        .expect("serialize run payload"),
    )
    .expect("write run payload");

    let bridge_payload = json!({
        "result": {
            "ok": true,
            "status": "completed",
            "observability": {
                "run_path": run_path,
                "status_path": run_path,
            }
        }
    });

    let result: BridgeLoginAttemptResult =
        serde_json::from_value(bridge_payload).expect("bridge payload should deserialize");
    let summary =
        read_codex_rotate_auth_flow_summary(result.result.as_ref().expect("fast-browser result"));

    assert_eq!(summary.stage.as_deref(), Some("success"));
    assert_eq!(
        summary.current_url.as_deref(),
        Some("http://localhost:1455/success")
    );
    assert_eq!(summary.callback_complete, Some(true));
    assert_eq!(summary.success, Some(true));
    assert_eq!(summary.next_action.as_deref(), Some("complete"));

    let _ = fs::remove_dir_all(&fixture_root);
}

#[test]
fn reads_auth_flow_summary_from_success_copy_without_callback_url() {
    let result = FastBrowserRunResult {
        page: Some(json!({
            "url": "https://auth.openai.com/sign-in-with-chatgpt/codex/consent",
            "title": "Signed in to Codex",
            "text": "Signed in to Codex\nYou may now close this page"
        })),
        ..FastBrowserRunResult::default()
    };

    let summary = read_codex_rotate_auth_flow_summary(&result);

    assert_eq!(summary.stage.as_deref(), Some("success"));
    assert_eq!(
        summary.current_url.as_deref(),
        Some("https://auth.openai.com/sign-in-with-chatgpt/codex/consent")
    );
    assert_eq!(summary.callback_complete, Some(true));
    assert_eq!(summary.success, Some(true));
    assert_eq!(summary.next_action.as_deref(), Some("complete"));
}

#[test]
fn reads_auth_flow_summary_from_action_only_step_metadata_when_output_is_missing() {
    let result = FastBrowserRunResult {
        state: Some(FastBrowserState {
            steps: HashMap::from([(
                "inspect_device_authorization_after_callback_code".to_string(),
                FastBrowserStepState {
                    action: Some(json!({
                        "current_url": "https://auth.openai.com/deviceauth/callback?code=ac_example&state=state_example",
                        "headline": "Signed in to Codex You may now close this page",
                        "success": true
                    })),
                },
            )]),
        }),
        ..FastBrowserRunResult::default()
    };

    let summary = read_codex_rotate_auth_flow_summary(&result);

    assert_eq!(summary.stage.as_deref(), Some("success"));
    assert_eq!(
        summary.current_url.as_deref(),
        Some("https://auth.openai.com/deviceauth/callback?code=ac_example&state=state_example")
    );
    assert_eq!(summary.callback_complete, Some(true));
    assert_eq!(summary.success, Some(true));
    assert_eq!(summary.next_action.as_deref(), Some("complete"));
}

#[test]
fn callback_metadata_overrides_pessimistic_output_summary() {
    let result = FastBrowserRunResult {
        output: Some(json!({
            "success": false,
            "callback_complete": false,
            "next_action": "retry_attempt",
            "stage": "oauth_consent"
        })),
        state: Some(FastBrowserState {
            steps: HashMap::from([(
                "inspect_device_authorization_after_callback_code".to_string(),
                FastBrowserStepState {
                    action: Some(json!({
                        "current_url": "https://auth.openai.com/deviceauth/callback?code=ac_example&state=state_example",
                        "headline": "Signed in to Codex You may now close this page",
                        "success": true
                    })),
                },
            )]),
        }),
        ..FastBrowserRunResult::default()
    };

    let summary = read_codex_rotate_auth_flow_summary(&result);

    assert_eq!(summary.stage.as_deref(), Some("success"));
    assert_eq!(summary.callback_complete, Some(true));
    assert_eq!(summary.success, Some(true));
    assert_eq!(summary.next_action.as_deref(), Some("complete"));
}

#[test]
fn bridge_login_attempt_result_accepts_failed_runs_without_state_or_output() {
    let payload = json!({
        "result": {
            "ok": false,
            "status": "failed",
            "finalUrl": "https://auth.openai.com/about-you",
            "error": {
                "message": "about-you-fields-not-found"
            }
        },
        "error_message": "about-you-fields-not-found"
    });

    let result: BridgeLoginAttemptResult =
        serde_json::from_value(payload).expect("bridge payload should deserialize");

    assert!(result.result.is_some());
    assert_eq!(
        result.error_message.as_deref(),
        Some("about-you-fields-not-found")
    );
    let summary = read_codex_rotate_auth_flow_summary(
        result
            .result
            .as_ref()
            .expect("normalized fast-browser result"),
    );
    assert_eq!(summary.next_action, None);
    assert_eq!(summary.error_message, None);
}

#[test]
fn treats_cancelled_codex_exit_after_local_callback_as_recoverable() {
    let summary = CodexRotateAuthFlowSummary {
            current_url: Some("http://localhost:1455/auth/callback".to_string()),
            callback_complete: Some(true),
            codex_login_exit_ok: Some(false),
            codex_login_stderr_tail: Some(
                "Starting local login server on http://localhost:1455.\nLogin server error: Login cancelled"
                    .to_string(),
            ),
            ..CodexRotateAuthFlowSummary::default()
        };

    assert!(login_cancelled_after_callback(&summary));
}

#[test]
fn does_not_treat_non_callback_cancelled_exit_as_recoverable() {
    let summary = CodexRotateAuthFlowSummary {
        current_url: Some("https://auth.openai.com/log-in".to_string()),
        callback_complete: Some(true),
        codex_login_exit_ok: Some(false),
        codex_login_stderr_tail: Some("Login server error: Login cancelled".to_string()),
        ..CodexRotateAuthFlowSummary::default()
    };

    assert!(!login_cancelled_after_callback(&summary));
}

#[test]
fn normalize_gmail_base_address_before_suffixing() {
    assert_eq!(
        normalize_gmail_template("Dev.User+17@gmail.com").unwrap(),
        "dev.user@gmail.com"
    );
}

#[test]
fn compute_next_gmail_alias_suffix_fills_first_gap() {
    assert_eq!(
        compute_next_gmail_alias_suffix(
            "dev.user+{n}@gmail.com",
            vec![
                "dev.user+1@gmail.com".to_string(),
                "dev.user+7@gmail.com".to_string(),
                "other@gmail.com".to_string(),
            ],
        )
        .unwrap(),
        2
    );
    assert_eq!(
        compute_next_gmail_alias_suffix(
            "dev.user+{n}@gmail.com",
            vec![
                "dev.user+1@gmail.com".to_string(),
                "dev.user+2@gmail.com".to_string(),
            ],
        )
        .unwrap(),
        3
    );
}

#[test]
fn builds_and_normalizes_templated_families() {
    assert_eq!(
        normalize_template_family("Dev.{N}@HotspotPrime.com").unwrap(),
        "dev.{n}@hotspotprime.com"
    );
    assert_eq!(
        build_account_family_email("dev.{N}@hotspotprime.com", 7).unwrap(),
        "dev.7@hotspotprime.com"
    );
    assert_eq!(
        compute_next_account_family_suffix(
            "dev.{N}@hotspotprime.com",
            vec![
                "dev.1@hotspotprime.com".to_string(),
                "dev.4@hotspotprime.com".to_string(),
                "other@gmail.com".to_string(),
            ],
        )
        .unwrap(),
        2
    );
    assert_eq!(
        compute_next_account_family_suffix(
            "dev.{N}@astronlab.com",
            vec!["dev.21@astronlab.com".to_string()],
        )
        .unwrap(),
        1
    );
}

#[test]
fn compute_next_account_family_suffix_ignores_failed_skipped_slots() {
    assert_eq!(
        compute_next_account_family_suffix(
            "dev.{N}@astronlab.com",
            vec![
                "dev.1@astronlab.com".to_string(),
                "dev.3@astronlab.com".to_string(),
            ],
        )
        .unwrap(),
        2
    );
}

#[test]
fn compute_next_account_family_suffix_can_reserve_skipped_slots_under_cap() {
    assert_eq!(
        compute_next_account_family_suffix_with_skips(
            "dev.{N}@astronlab.com",
            vec![
                "dev.1@astronlab.com".to_string(),
                "dev.2@astronlab.com".to_string(),
                "dev.3@astronlab.com".to_string(),
            ],
            vec![
                "dev.4@astronlab.com".to_string(),
                "dev.5@astronlab.com".to_string(),
            ],
            10,
        )
        .unwrap(),
        6
    );
}

#[test]
fn compute_next_account_family_suffix_reserves_skipped_slots_at_skip_cap() {
    assert_eq!(
        compute_next_account_family_suffix_with_skips(
            "dev.{N}@astronlab.com",
            vec![
                "dev.1@astronlab.com".to_string(),
                "dev.2@astronlab.com".to_string(),
                "dev.3@astronlab.com".to_string(),
            ],
            (4..=13)
                .map(|suffix| format!("dev.{suffix}@astronlab.com"))
                .collect(),
            10,
        )
        .unwrap(),
        14
    );
}

#[test]
fn compute_fresh_account_family_suffix_respects_family_frontier_when_skips_exist() {
    let family = CredentialFamily {
        profile_name: "dev-1".to_string(),
        template: "devbench.{n}@astronlab.com".to_string(),
        next_suffix: 16,
        max_skipped_slots: 2,
        created_at: "2026-04-13T05:00:00.000Z".to_string(),
        updated_at: "2026-04-13T05:00:00.000Z".to_string(),
        last_created_email: None,
        relogin: Vec::new(),
        suspend_domain_on_terminal_refresh_failure: false,
    };

    let next_suffix = compute_fresh_account_family_suffix(
        Some(&family),
        "devbench.{n}@astronlab.com",
        (1..=13)
            .map(|suffix| format!("devbench.{suffix}@astronlab.com"))
            .collect(),
        vec![
            "devbench.14@astronlab.com".to_string(),
            "devbench.15@astronlab.com".to_string(),
        ],
    )
    .expect("next suffix");

    assert_eq!(next_suffix, 16);
}

#[test]
fn compute_fresh_account_family_suffix_preserves_gap_fill_without_skips() {
    let family = CredentialFamily {
        profile_name: "dev-1".to_string(),
        template: "dev.{n}@astronlab.com".to_string(),
        next_suffix: 99,
        max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
        created_at: "2026-04-13T05:00:00.000Z".to_string(),
        updated_at: "2026-04-13T05:00:00.000Z".to_string(),
        last_created_email: None,
        relogin: Vec::new(),
        suspend_domain_on_terminal_refresh_failure: false,
    };

    let next_suffix = compute_fresh_account_family_suffix(
        Some(&family),
        "dev.{n}@astronlab.com",
        vec![
            "dev.1@astronlab.com".to_string(),
            "dev.3@astronlab.com".to_string(),
        ],
        Vec::new(),
    )
    .expect("next suffix");

    assert_eq!(next_suffix, 2);
}

#[test]
fn compute_fresh_account_family_suffix_ignores_frontier_when_skips_are_not_reserved() {
    let family = CredentialFamily {
        profile_name: "dev-1".to_string(),
        template: "dev3astronlab+{n}@gmail.com".to_string(),
        next_suffix: 7,
        max_skipped_slots: 0,
        created_at: "2026-04-13T05:00:00.000Z".to_string(),
        updated_at: "2026-04-14T06:11:25.913Z".to_string(),
        last_created_email: Some("dev3astronlab+1@gmail.com".to_string()),
        relogin: Vec::new(),
        suspend_domain_on_terminal_refresh_failure: false,
    };

    let next_suffix = compute_fresh_account_family_suffix(
        Some(&family),
        "dev3astronlab+{n}@gmail.com",
        vec![
            "dev3astronlab+1@gmail.com".to_string(),
            "dev3astronlab+2@gmail.com".to_string(),
        ],
        vec!["dev3astronlab+6@gmail.com".to_string()],
    )
    .expect("next suffix");

    assert_eq!(next_suffix, 3);
}

#[test]
fn collect_known_account_emails_includes_family_relogin_entries() {
    let mut store = CredentialStore::default();
    store.families.insert(
        "dev-1::dev.{n}@astronlab.com".to_string(),
        CredentialFamily {
            profile_name: "dev-1".to_string(),
            template: "dev.{n}@astronlab.com".to_string(),
            next_suffix: 10,
            max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
            created_at: "2026-04-13T05:00:00.000Z".to_string(),
            updated_at: "2026-04-13T05:00:00.000Z".to_string(),
            last_created_email: Some("dev.9@astronlab.com".to_string()),
            relogin: vec!["dev.2@astronlab.com".to_string()],
            suspend_domain_on_terminal_refresh_failure: false,
        },
    );

    let next_suffix = compute_fresh_account_family_suffix(
        store.families.get("dev-1::dev.{n}@astronlab.com"),
        "dev.{n}@astronlab.com",
        collect_known_account_emails(
            &Pool {
                active_index: 0,
                accounts: vec![make_account_entry("dev.1@astronlab.com", "acct-1")],
            },
            &store,
        ),
        Vec::new(),
    )
    .expect("next suffix");

    assert_eq!(next_suffix, 3);
}

#[test]
fn prepare_next_auto_create_attempt_preserves_current_skip_when_budget_is_full() {
    with_rotate_home("codex-rotate-skip-budget", |_| {
        let mut store = CredentialStore::default();
        store.pending.insert(
            "devbench.14@astronlab.com".to_string(),
            make_pending(
                "devbench.14@astronlab.com",
                "dev-1",
                "devbench.{n}@astronlab.com",
                14,
                "2026-04-13T06:00:00.000Z",
            ),
        );
        store
            .skipped
            .extend(["devbench.12@astronlab.com", "devbench.13@astronlab.com"].map(str::to_string));
        store.families.insert(
            "dev-1::devbench.{n}@astronlab.com".to_string(),
            CredentialFamily {
                profile_name: "dev-1".to_string(),
                template: "devbench.{n}@astronlab.com".to_string(),
                next_suffix: 14,
                max_skipped_slots: 2,
                created_at: "2026-04-13T05:00:00.000Z".to_string(),
                updated_at: "2026-04-13T05:00:00.000Z".to_string(),
                last_created_email: None,
                relogin: Vec::new(),
                suspend_domain_on_terminal_refresh_failure: false,
            },
        );

        skip_pending_account_and_advance_family(
            &mut store,
            "dev-1::devbench.{n}@astronlab.com",
            "dev-1",
            "devbench.{n}@astronlab.com",
            14,
            "devbench.14@astronlab.com",
            "2026-04-13T06:00:00.000Z",
        )
        .expect("prepare next attempt");

        assert!(!store.pending.contains_key("devbench.14@astronlab.com"));
        assert!(store.skipped.contains("devbench.14@astronlab.com"));
        assert!(!store.skipped.contains("devbench.12@astronlab.com"));
        assert!(store.skipped.contains("devbench.13@astronlab.com"));

        let next_suffix = compute_next_account_family_suffix_with_skips(
            "devbench.{n}@astronlab.com",
            (1..=13)
                .map(|suffix| format!("devbench.{suffix}@astronlab.com"))
                .collect(),
            collect_skipped_account_emails_for_family(
                &store,
                "dev-1",
                "devbench.{n}@astronlab.com",
            ),
            max_skipped_slots_for_family(store.families.get("dev-1::devbench.{n}@astronlab.com")),
        )
        .expect("next suffix");

        assert_eq!(next_suffix, 15);
    });
}

#[test]
fn skip_pending_account_and_advance_family_unblocks_next_gmail_suffix() {
    with_rotate_home("codex-rotate-gmail-skip", |_| {
        let mut store = CredentialStore::default();
        store.pending.insert(
            "dev3astronlab+5@gmail.com".to_string(),
            make_pending(
                "dev3astronlab+5@gmail.com",
                "dev-1",
                "dev3astronlab+{n}@gmail.com",
                5,
                "2026-04-14T19:56:37.881Z",
            ),
        );
        store.families.insert(
            "dev-1::dev3astronlab+{n}@gmail.com".to_string(),
            CredentialFamily {
                profile_name: "dev-1".to_string(),
                template: "dev3astronlab+{n}@gmail.com".to_string(),
                next_suffix: 6,
                max_skipped_slots: 0,
                created_at: "2026-04-13T05:00:00.000Z".to_string(),
                updated_at: "2026-04-14T19:56:10.036Z".to_string(),
                last_created_email: Some("dev3astronlab+4@gmail.com".to_string()),
                relogin: Vec::new(),
                suspend_domain_on_terminal_refresh_failure: false,
            },
        );

        skip_pending_account_and_advance_family(
            &mut store,
            "dev-1::dev3astronlab+{n}@gmail.com",
            "dev-1",
            "dev3astronlab+{n}@gmail.com",
            5,
            "dev3astronlab+5@gmail.com",
            "2026-04-14T19:56:37.881Z",
        )
        .expect("skip pending gmail account");

        assert!(!store.pending.contains_key("dev3astronlab+5@gmail.com"));
        assert!(store.skipped.contains("dev3astronlab+5@gmail.com"));
        assert_eq!(
            store
                .families
                .get("dev-1::dev3astronlab+{n}@gmail.com")
                .expect("gmail family")
                .next_suffix,
            6
        );

        let next_suffix = compute_fresh_account_family_suffix(
            store.families.get("dev-1::dev3astronlab+{n}@gmail.com"),
            "dev3astronlab+{n}@gmail.com",
            vec![
                "dev3astronlab+1@gmail.com".to_string(),
                "dev3astronlab+2@gmail.com".to_string(),
                "dev3astronlab+3@gmail.com".to_string(),
                "dev3astronlab+4@gmail.com".to_string(),
            ],
            collect_skipped_account_emails_for_family(
                &store,
                "dev-1",
                "dev3astronlab+{n}@gmail.com",
            ),
        )
        .expect("compute next gmail suffix");
        assert_eq!(next_suffix, 5);
    });
}

#[test]
fn skip_pending_account_and_advance_family_keeps_existing_runtime_gmail_skips_reserved() {
    with_rotate_home("codex-rotate-gmail-skip-reserved", |_| {
        let mut store = CredentialStore::default();
        store
            .skipped
            .insert("dev3astronlab+5@gmail.com".to_string());
        store.pending.insert(
            "dev3astronlab+6@gmail.com".to_string(),
            make_pending(
                "dev3astronlab+6@gmail.com",
                "dev-1",
                "dev3astronlab+{n}@gmail.com",
                6,
                "2026-04-14T21:40:53.532Z",
            ),
        );
        store.families.insert(
            "dev-1::dev3astronlab+{n}@gmail.com".to_string(),
            CredentialFamily {
                profile_name: "dev-1".to_string(),
                template: "dev3astronlab+{n}@gmail.com".to_string(),
                next_suffix: 6,
                max_skipped_slots: 0,
                created_at: "2026-04-13T05:00:00.000Z".to_string(),
                updated_at: "2026-04-14T21:40:53.532Z".to_string(),
                last_created_email: Some("dev3astronlab+4@gmail.com".to_string()),
                relogin: Vec::new(),
                suspend_domain_on_terminal_refresh_failure: false,
            },
        );

        skip_pending_account_and_advance_family(
            &mut store,
            "dev-1::dev3astronlab+{n}@gmail.com",
            "dev-1",
            "dev3astronlab+{n}@gmail.com",
            6,
            "dev3astronlab+6@gmail.com",
            "2026-04-14T21:40:53.532Z",
        )
        .expect("skip pending gmail account");

        assert!(store.skipped.contains("dev3astronlab+5@gmail.com"));
        assert!(store.skipped.contains("dev3astronlab+6@gmail.com"));
        assert_eq!(
            store
                .families
                .get("dev-1::dev3astronlab+{n}@gmail.com")
                .expect("gmail family")
                .next_suffix,
            7
        );

        let next_suffix = compute_fresh_account_family_suffix(
            store.families.get("dev-1::dev3astronlab+{n}@gmail.com"),
            "dev3astronlab+{n}@gmail.com",
            vec![
                "dev3astronlab+1@gmail.com".to_string(),
                "dev3astronlab+2@gmail.com".to_string(),
                "dev3astronlab+3@gmail.com".to_string(),
                "dev3astronlab+4@gmail.com".to_string(),
            ],
            collect_skipped_account_emails_for_family(
                &store,
                "dev-1",
                "dev3astronlab+{n}@gmail.com",
            ),
        )
        .expect("compute next gmail suffix");
        assert_eq!(next_suffix, 5);
    });
}

#[test]
fn compute_create_attempt_family_suffix_advances_past_current_retry_reserved_gmail_slot() {
    let family = CredentialFamily {
        profile_name: "dev-1".to_string(),
        template: "dev3astronlab+{n}@gmail.com".to_string(),
        next_suffix: 7,
        max_skipped_slots: 0,
        created_at: "2026-04-13T05:00:00.000Z".to_string(),
        updated_at: "2026-04-15T04:22:55.044Z".to_string(),
        last_created_email: Some("dev3astronlab+1@gmail.com".to_string()),
        relogin: Vec::new(),
        suspend_domain_on_terminal_refresh_failure: false,
    };

    let next_suffix = compute_create_attempt_family_suffix(
        Some(&family),
        "dev3astronlab+{n}@gmail.com",
        vec![
            "dev3astronlab+1@gmail.com".to_string(),
            "dev3astronlab+2@gmail.com".to_string(),
            "dev3astronlab+3@gmail.com".to_string(),
            "dev3astronlab+4@gmail.com".to_string(),
        ],
        vec![
            "dev3astronlab+5@gmail.com".to_string(),
            "dev3astronlab+6@gmail.com".to_string(),
        ],
        &["dev3astronlab+5@gmail.com".to_string()]
            .into_iter()
            .collect(),
    )
    .expect("next suffix");

    assert_eq!(next_suffix, 6);
}

#[test]
fn compute_next_account_family_suffix_fills_missing_dev_slots_before_frontier() {
    assert_eq!(
        compute_next_account_family_suffix(
            "dev.{N}@astronlab.com",
            vec![
                "dev.45@astronlab.com".to_string(),
                "dev.47@astronlab.com".to_string(),
                "dev.48@astronlab.com".to_string(),
                "dev.58@astronlab.com".to_string(),
            ],
        )
        .unwrap(),
        1
    );
    assert_eq!(
        compute_next_account_family_suffix(
            "dev.{N}@astronlab.com",
            (1..=58)
                .filter(|suffix| *suffix != 46)
                .map(|suffix| format!("dev.{suffix}@astronlab.com"))
                .collect(),
        )
        .unwrap(),
        46
    );
}

#[test]
fn fresh_create_prefers_signup_recovery_but_reused_pending_create_does_not() {
    assert!(prefer_signup_recovery_for_create(false));
    assert!(!prefer_signup_recovery_for_create(true));
}

#[test]
fn create_execution_lock_blocks_other_process_and_records_metadata() {
    with_rotate_home("codex-rotate-create-lock", |_| {
        let options = CreateCommandOptions {
            alias: Some("dev-1".to_string()),
            profile_name: Some("dev-1".to_string()),
            template: Some("dev.{n}@astronlab.com".to_string()),
            force: true,
            ignore_current: true,
            require_usable_quota: true,
            source: CreateCommandSource::Manual,
            ..CreateCommandOptions::default()
        };
        let lock = acquire_create_execution_lock(&options, None).expect("acquire create lock");
        let lock_path = create_lock_path().expect("create lock path");
        let metadata = read_create_execution_lock_metadata(&lock_path).expect("lock metadata");
        assert_eq!(metadata.pid, std::process::id());
        assert_eq!(metadata.source, "manual");
        assert_eq!(metadata.profile_name.as_deref(), Some("dev-1"));
        assert_eq!(metadata.template.as_deref(), Some("dev.{n}@astronlab.com"));
        assert_eq!(metadata.alias.as_deref(), Some("dev-1"));

        let output = ProcessCommand::new("ruby")
            .arg("-e")
            .arg(
                r#"
path = ARGV[0]
File.open(path, File::RDWR | File::CREAT, 0o644) do |file|
  locked = file.flock(File::LOCK_EX | File::LOCK_NB)
  exit(locked ? 7 : 0)
end
"#,
            )
            .arg(&lock_path)
            .output()
            .expect("run ruby flock probe");
        assert_eq!(
            output.status.code(),
            Some(0),
            "second process unexpectedly acquired create lock: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        drop(lock);

        assert!(
            !lock_path.exists(),
            "create lock file should be removed after the holder drops"
        );

        let output = ProcessCommand::new("ruby")
            .arg("-e")
            .arg(
                r#"
path = ARGV[0]
File.open(path, File::RDWR | File::CREAT, 0o644) do |file|
  locked = file.flock(File::LOCK_EX | File::LOCK_NB)
  exit(locked ? 0 : 9)
end
"#,
            )
            .arg(&lock_path)
            .output()
            .expect("run ruby flock release probe");
        assert_eq!(
            output.status.code(),
            Some(0),
            "second process could not acquire released create lock: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    });
}

#[test]
fn create_execution_lock_waits_for_same_process_release() {
    with_rotate_home("codex-rotate-create-lock-wait", |_| {
        let options = CreateCommandOptions {
            profile_name: Some("dev-1".to_string()),
            template: Some("dev.{n}@astronlab.com".to_string()),
            force: true,
            source: CreateCommandSource::Manual,
            ..CreateCommandOptions::default()
        };
        let first_lock =
            acquire_create_execution_lock(&options, None).expect("acquire first create lock");
        let (tx, rx) = mpsc::channel();
        let options_clone = options.clone();
        let waiter = thread::spawn(move || {
            let acquired = acquire_create_execution_lock(&options_clone, None).is_ok();
            tx.send(acquired).expect("send wait result");
        });

        assert!(rx.recv_timeout(Duration::from_millis(250)).is_err());
        drop(first_lock);
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), true);
        waiter.join().expect("join waiter");
    });
}

#[test]
fn create_family_hint_accepts_templates_and_rejects_gmail() {
    assert!(should_use_default_create_family_hint(Some(
        "dev.{n}@astronlab.com"
    )));
    assert!(should_use_default_create_family_hint(Some(
        "qa.{n}@astronlab.com"
    )));
    assert!(!should_use_default_create_family_hint(Some(
        "dev.user@gmail.com"
    )));
    assert!(!should_use_default_create_family_hint(None));
}

#[test]
fn resolve_managed_profile_name_from_candidates_matches_requested_preferred_and_default() {
    assert_eq!(
        resolve_managed_profile_name_from_candidates(
            &["dev-1", "other"],
            Some("dev-1"),
            None,
            None,
            Some("other"),
        )
        .unwrap(),
        "dev-1"
    );
    assert_eq!(
        resolve_managed_profile_name_from_candidates(
            &["dev-1", "other"],
            None,
            Some("other"),
            Some("workflow"),
            Some("dev-1"),
        )
        .unwrap(),
        "other"
    );
    assert_eq!(
        resolve_managed_profile_name_from_candidates(
            &["dev-1", "other"],
            None,
            None,
            None,
            Some("other"),
        )
        .unwrap(),
        "other"
    );
}

#[test]
fn resolve_create_template_prefers_requested_then_default_then_discovered() {
    assert_eq!(
        resolve_create_template(
            Some("other+{n}@gmail.com"),
            Some("dev.user+{n}@gmail.com"),
            "dev.{n}@astronlab.com",
        )
        .unwrap(),
        "other+{n}@gmail.com"
    );
    assert_eq!(
        resolve_create_template(
            None,
            Some("Dev.User+{N}@gmail.com"),
            "dev.{n}@astronlab.com",
        )
        .unwrap(),
        "dev.{n}@astronlab.com"
    );
    assert_eq!(
        resolve_create_template(None, None, "dev.{n}@astronlab.com").unwrap(),
        "dev.{n}@astronlab.com"
    );
}

#[test]
fn score_email_prefers_exact_profile_token_match() {
    assert!(
        score_email_for_managed_profile_name("dev-1", "1.dev.astronlab@gmail.com")
            > score_email_for_managed_profile_name("dev-1", "dev.2.astronlab@gmail.com")
    );
}

#[test]
fn select_best_email_and_system_chrome_profile_match() {
    assert_eq!(
        select_best_email_for_managed_profile(
            "dev-1",
            vec![
                "other@gmail.com".to_string(),
                "1.dev.astronlab@gmail.com".to_string(),
            ],
            None,
        )
        .as_deref(),
        Some("1.dev.astronlab@gmail.com")
    );

    let match_result = select_best_system_chrome_profile_match(
        "dev-1",
        &[
            SystemChromeProfileCandidate {
                directory: "Profile 1".to_string(),
                name: "Personal".to_string(),
                emails: vec!["other@gmail.com".to_string()],
            },
            SystemChromeProfileCandidate {
                directory: "Profile 2".to_string(),
                name: "Dev".to_string(),
                emails: vec!["1.dev.astronlab@gmail.com".to_string()],
            },
        ],
        None,
    )
    .expect("profile match");
    assert_eq!(match_result.directory, "Profile 2");
    assert_eq!(match_result.matched_email, "1.dev.astronlab@gmail.com");
}

#[test]
fn stored_relogin_honors_logout_setting() {
    assert!(!should_logout_before_stored_relogin(
        &ReloginOptions::default()
    ));
    assert!(should_logout_before_stored_relogin(&ReloginOptions {
        logout_first: true,
        ..ReloginOptions::default()
    }));
    assert!(!should_logout_before_stored_relogin(&ReloginOptions {
        logout_first: false,
        ..ReloginOptions::default()
    }));
}

#[test]
fn auto_create_retry_gate_only_applies_to_next_with_quota_requirement() {
    assert!(should_retry_create_until_usable(&CreateCommandOptions {
        require_usable_quota: true,
        source: CreateCommandSource::Next,
        ..CreateCommandOptions::default()
    }));
    assert!(!should_retry_create_until_usable(&CreateCommandOptions {
        require_usable_quota: false,
        source: CreateCommandSource::Next,
        ..CreateCommandOptions::default()
    }));
    assert!(!should_retry_create_until_usable(&CreateCommandOptions {
        require_usable_quota: true,
        source: CreateCommandSource::Manual,
        ..CreateCommandOptions::default()
    }));
}

#[test]
fn workflow_skip_account_errors_are_retried_for_create() {
    let error = anyhow::Error::new(WorkflowSkipAccountError::new(
        "skip this account".to_string(),
    ));
    assert!(is_workflow_skip_account_error(&error));
    assert!(should_retry_create_after_error(
        &CreateCommandOptions::default(),
        &error
    ));
}

#[test]
fn missing_account_login_ref_errors_are_retried_for_create() {
    let error =
            anyhow!("Workflow input 'account_login_ref' must be a secret ref to satisfy step 'fill_signup_password'");
    assert!(is_missing_account_login_ref_error(&error));
    assert!(should_retry_create_after_error(
        &CreateCommandOptions::default(),
        &error
    ));
}

#[test]
fn unrelated_create_errors_do_not_trigger_policy_skip() {
    let error = anyhow!("Codex browser login did not reach the callback.");
    assert!(!is_workflow_skip_account_error(&error));
    assert!(!is_missing_account_login_ref_error(&error));
    assert!(!is_optional_account_secret_prepare_error(&error));
    assert!(!should_retry_create_after_error(
        &CreateCommandOptions::default(),
        &error
    ));
}

#[test]
fn optional_account_secret_prepare_errors_match_bitwarden_unavailability() {
    assert!(is_optional_account_secret_prepare_error(&anyhow!(
        "Bitwarden CLI is locked. Re-run this command interactively."
    )));
    assert!(is_optional_account_secret_prepare_error(&anyhow!(
        "Bitwarden CLI timed out while trying to read Bitwarden CLI status."
    )));
    assert!(is_optional_account_secret_prepare_error(&anyhow!(
        "Fast-browser secret broker failed to read secret-store status."
    )));
    assert!(!is_optional_account_secret_prepare_error(&anyhow!(
        "Bitwarden item already exists with a different password."
    )));
}

#[test]
fn reusable_account_retry_stop_only_applies_to_next_source() {
    assert!(should_stop_create_retry_for_reusable_account(
        &CreateCommandOptions {
            source: CreateCommandSource::Next,
            ..CreateCommandOptions::default()
        }
    ));
    assert!(!should_stop_create_retry_for_reusable_account(
        &CreateCommandOptions {
            source: CreateCommandSource::Manual,
            force: true,
            ..CreateCommandOptions::default()
        }
    ));
}

#[test]
fn reads_default_birth_date_from_workflow_metadata() {
    let metadata = read_workflow_file_metadata(
        &repo_root()
            .join(".fast-browser")
            .join("workflows")
            .join("web")
            .join("auth.openai.com")
            .join("codex-rotate-account-flow-main.yaml"),
    )
    .expect("workflow metadata");
    let value = metadata.default_birth_date().expect("default birth date");
    assert_eq!(value.birth_month, 1);
    assert_eq!(value.birth_day, 24);
    assert_eq!(value.birth_year, 1990);
}

#[test]
fn normalize_credential_store_leaves_v7_default_create_family_empty_until_configured() {
    let store = normalize_credential_store(json!({}));
    assert_eq!(store.version, 9);
    assert!(store.default_create_template.is_empty());
}

#[test]
fn normalize_credential_store_reads_domain_rotation_config() {
    let store = normalize_credential_store(json!({
        "domain": {
            "AstronLab.com": {
                "rotation_enabled": false,
                "max_suffix_per_family": 6
            }
        }
    }));

    assert_eq!(
        store.domain.get("astronlab.com"),
        Some(&DomainConfig {
            rotation_enabled: false,
            max_suffix_per_family: Some(6),
            reactivate_at: None,
        })
    );
    assert!(!is_rotation_enabled_for_email_in_store(
        &store,
        "dev.1@astronlab.com"
    ));
    assert!(is_rotation_enabled_for_email_in_store(
        &store,
        "dev.user@gmail.com"
    ));
}

#[test]
fn save_credential_store_persists_domain_rotation_config() {
    with_rotate_home("codex-rotate-domain-config-save", |_| {
        let mut store = CredentialStore::default();
        store.domain.insert(
            "astronlab.com".to_string(),
            DomainConfig {
                rotation_enabled: false,
                max_suffix_per_family: Some(6),
                reactivate_at: None,
            },
        );

        save_credential_store(&store).expect("save credential store");

        let saved = load_rotate_state_json().expect("load rotate state");
        assert_eq!(
            saved["domain"]["astronlab.com"]["rotation_enabled"],
            Value::Bool(false)
        );
        assert_eq!(
            saved["domain"]["astronlab.com"]["max_suffix_per_family"],
            Value::from(6)
        );
    });
}

#[test]
fn reactivate_elapsed_domains_reenables_expired_domain_config() {
    let mut domains = HashMap::from([(
        "astronlab.com".to_string(),
        DomainConfig {
            rotation_enabled: false,
            max_suffix_per_family: None,
            reactivate_at: Some("2026-04-01T00:00:00.000Z".to_string()),
        },
    )]);

    assert!(reactivate_elapsed_domains(
        &mut domains,
        DateTime::parse_from_rfc3339("2026-04-14T00:00:00.000Z")
            .expect("parse now")
            .with_timezone(&Utc)
    ));
    assert_eq!(
        domains.get("astronlab.com"),
        Some(&DomainConfig {
            rotation_enabled: true,
            max_suffix_per_family: None,
            reactivate_at: None,
        })
    );
}

#[test]
fn save_credential_store_preserves_pool_sections() {
    with_rotate_home("codex-rotate-credential-store-preserve", |_| {
        write_rotate_state_json(&json!({
            "accounts": [
                make_account_entry("dev.1@astronlab.com", "acct-1"),
                make_account_entry("dev.2@astronlab.com", "acct-2")
            ],
            "active_index": 1,
            "version": 7
        }))
        .expect("write initial state");

        let mut store = CredentialStore::default();
        store.default_create_template = "dev.{n}@astronlab.com".to_string();
        store.families.insert(
            "dev-1::dev.{n}@astronlab.com".to_string(),
            CredentialFamily {
                profile_name: "dev-1".to_string(),
                template: "dev.{n}@astronlab.com".to_string(),
                next_suffix: 3,
                max_skipped_slots: 0,
                created_at: "2026-04-05T00:00:00.000Z".to_string(),
                updated_at: "2026-04-05T00:00:00.000Z".to_string(),
                last_created_email: Some("dev.2@astronlab.com".to_string()),
                relogin: Vec::new(),
                suspend_domain_on_terminal_refresh_failure: false,
            },
        );

        save_credential_store(&store).expect("save credential store");

        let saved = load_rotate_state_json().expect("load rotate state");
        assert_eq!(
            saved["accounts"][0]["email"],
            Value::String("dev.1@astronlab.com".to_string())
        );
        assert_eq!(saved["active_index"], Value::from(1));
        assert_eq!(
            saved["default_create_template"],
            Value::String("dev.{n}@astronlab.com".to_string())
        );
        assert!(saved["families"].is_object());
    });
}

#[test]
fn concurrent_pool_and_credential_store_writes_preserve_valid_rotate_state() {
    with_rotate_home("codex-rotate-concurrent-state", |_| {
        write_rotate_state_json(&json!({
            "accounts": [
                make_account_entry("dev.1@astronlab.com", "acct-1")
            ],
            "active_index": 0,
            "version": 7,
            "families": {
                "dev-1::dev.{n}@astronlab.com": {
                    "profile_name": "dev-1",
                    "template": "dev.{n}@astronlab.com",
                    "next_suffix": 2,
                    "max_skipped_slots": 0,
                    "created_at": "2026-04-05T00:00:00.000Z",
                    "updated_at": "2026-04-05T00:00:00.000Z",
                    "last_created_email": "dev.1@astronlab.com",
                    "relogin": []
                }
            }
        }))
        .expect("write initial state");

        let barrier = Arc::new(Barrier::new(3));
        let pool_barrier = Arc::clone(&barrier);
        let pool_thread = thread::spawn(move || {
            pool_barrier.wait();
            crate::pool::save_pool(&Pool {
                active_index: 1,
                accounts: vec![
                    make_account_entry("dev.1@astronlab.com", "acct-1"),
                    make_account_entry("dev.2@astronlab.com", "acct-2"),
                ],
            })
        });

        let store_barrier = Arc::clone(&barrier);
        let store_thread = thread::spawn(move || {
            store_barrier.wait();
            let mut store = load_credential_store().expect("load credential store");
            store.default_create_template = "dev.{n}@astronlab.com".to_string();
            store.pending.insert(
                "dev.3@astronlab.com".to_string(),
                make_pending(
                    "dev.3@astronlab.com",
                    "dev-1",
                    "dev.{n}@astronlab.com",
                    3,
                    "2026-04-05T00:00:00.000Z",
                ),
            );
            save_credential_store(&store)
        });

        barrier.wait();
        pool_thread.join().expect("pool thread").expect("save pool");
        store_thread
            .join()
            .expect("store thread")
            .expect("save credential store");

        let saved = load_rotate_state_json().expect("load rotate state");
        assert_eq!(saved["active_index"], Value::from(1));
        assert_eq!(
            saved["accounts"][1]["email"],
            Value::String("dev.2@astronlab.com".to_string())
        );
        assert_eq!(
            saved["pending"]["dev.3@astronlab.com"]["email"],
            Value::String("dev.3@astronlab.com".to_string())
        );
        assert_eq!(
            saved["families"]["dev-1::dev.{n}@astronlab.com"]["last_created_email"],
            Value::String("dev.1@astronlab.com".to_string())
        );
    });
}

#[test]
fn normalize_credential_store_reads_skipped_emails() {
    let store = normalize_credential_store(json!({
        "skipped": [
            "dev.91@astronlab.com",
            "dev.92@astronlab.com"
        ]
    }));

    assert!(store.skipped.contains("dev.91@astronlab.com"));
    assert!(store.skipped.contains("dev.92@astronlab.com"));
}

#[test]
fn normalize_credential_store_defaults_family_skip_cap() {
    let store = normalize_credential_store(json!({
        "families": {
            "dev-1::dev.{n}@astronlab.com": {
                "profile_name": "dev-1",
                "template": "dev.{n}@astronlab.com",
                "next_suffix": 23,
                "created_at": "2026-04-05T00:00:00.000Z",
                "updated_at": "2026-04-05T00:00:00.000Z",
                "last_created_email": "dev.22@astronlab.com"
            }
        }
    }));

    assert_eq!(
        store
            .families
            .get("dev-1::dev.{n}@astronlab.com")
            .map(|family| family.max_skipped_slots),
        Some(DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY)
    );
}

#[test]
fn normalize_credential_store_reads_family_relogin_emails() {
    let store = normalize_credential_store(json!({
        "families": {
            "dev-1::dev.{n}@astronlab.com": {
                "profile_name": "dev-1",
                "template": "dev.{n}@astronlab.com",
                "next_suffix": 23,
                "created_at": "2026-04-05T00:00:00.000Z",
                "updated_at": "2026-04-05T00:00:00.000Z",
                "last_created_email": "dev.22@astronlab.com",
                "relogin": ["DEV.2@astronlab.com", "dev.3@astronlab.com"]
            }
        }
    }));

    assert_eq!(
        store
            .families
            .get("dev-1::dev.{n}@astronlab.com")
            .map(|family| family.relogin.clone()),
        Some(vec![
            "dev.2@astronlab.com".to_string(),
            "dev.3@astronlab.com".to_string()
        ])
    );
}

#[test]
fn normalize_credential_store_migrates_legacy_deleted_into_current_suspend_flag() {
    let store = normalize_credential_store(json!({
        "families": {
            "dev-1::dev.{n}@astronlab.com": {
                "profile_name": "dev-1",
                "template": "dev.{n}@astronlab.com",
                "next_suffix": 23,
                "created_at": "2026-04-05T00:00:00.000Z",
                "updated_at": "2026-04-05T00:00:00.000Z",
                "last_created_email": "dev.22@astronlab.com",
                "deleted": ["DEV.2@astronlab.com", "dev.3@astronlab.com"]
            }
        }
    }));

    let family = store
        .families
        .get("dev-1::dev.{n}@astronlab.com")
        .expect("family");
    assert_eq!(
        family.relogin,
        vec![
            "dev.2@astronlab.com".to_string(),
            "dev.3@astronlab.com".to_string()
        ]
    );
    assert!(family.suspend_domain_on_terminal_refresh_failure);
}

#[test]
fn select_pending_template_hint_prefers_dev_template_family() {
    let mut store = CredentialStore::default();
    store.pending.insert(
        "qa.300@astronlab.com".to_string(),
        make_pending(
            "qa.300@astronlab.com",
            "dev-1",
            "qa.{n}@astronlab.com",
            300,
            "2026-04-06T17:00:00.000Z",
        ),
    );
    store.pending.insert(
        "dev.30@astronlab.com".to_string(),
        make_pending(
            "dev.30@astronlab.com",
            "dev-1",
            "dev.{n}@astronlab.com",
            30,
            "2026-04-06T16:00:00.000Z",
        ),
    );

    assert_eq!(
        select_pending_template_hint_for_profile(&store, "dev-1", None).as_deref(),
        Some("dev.{n}@astronlab.com")
    );
}

#[test]
fn select_pending_credential_for_family_drains_lowest_suffix_first() {
    let mut store = CredentialStore::default();
    store.pending.insert(
        "dev.user+1@gmail.com".to_string(),
        make_pending(
            "dev.user+1@gmail.com",
            "dev-1",
            "dev.user+{n}@gmail.com",
            1,
            "2026-03-20T00:00:00.000Z",
        ),
    );
    store.pending.insert(
        "dev.user+3@gmail.com".to_string(),
        make_pending(
            "dev.user+3@gmail.com",
            "dev-1",
            "dev.user+{n}@gmail.com",
            3,
            "2026-03-20T03:00:00.000Z",
        ),
    );

    assert_eq!(
        select_pending_credential_for_family(
            &store,
            "dev-1",
            "dev.user+{n}@gmail.com",
            None,
            &HashSet::new(),
        )
        .map(|entry| entry.stored.email),
        Some("dev.user+1@gmail.com".to_string())
    );
}

#[test]
fn select_pending_credential_for_family_can_filter_by_alias() {
    let mut store = CredentialStore::default();
    let mut left = make_pending(
        "dev.user+2@gmail.com",
        "dev-1",
        "dev.user+{n}@gmail.com",
        2,
        "2026-03-20T02:00:00.000Z",
    );
    left.stored.alias = Some("team-a".to_string());
    let mut right = make_pending(
        "dev.user+3@gmail.com",
        "dev-1",
        "dev.user+{n}@gmail.com",
        3,
        "2026-03-20T03:00:00.000Z",
    );
    right.stored.alias = Some("team-b".to_string());
    store.pending.insert(left.stored.email.clone(), left);
    store.pending.insert(right.stored.email.clone(), right);

    assert_eq!(
        select_pending_credential_for_family(
            &store,
            "dev-1",
            "dev.user+{n}@gmail.com",
            Some("team-a"),
            &HashSet::new(),
        )
        .map(|entry| entry.stored.email),
        Some("dev.user+2@gmail.com".to_string())
    );
}

#[test]
fn select_pending_credential_for_family_ignores_retry_reserved_email() {
    let mut store = CredentialStore::default();
    store.pending.insert(
        "dev.user+5@gmail.com".to_string(),
        make_pending(
            "dev.user+5@gmail.com",
            "dev-1",
            "dev.user+{n}@gmail.com",
            5,
            "2026-03-20T05:00:00.000Z",
        ),
    );
    store.pending.insert(
        "dev.user+6@gmail.com".to_string(),
        make_pending(
            "dev.user+6@gmail.com",
            "dev-1",
            "dev.user+{n}@gmail.com",
            6,
            "2026-03-20T06:00:00.000Z",
        ),
    );
    let excluded = HashSet::from([normalize_email_key("dev.user+5@gmail.com")]);

    assert_eq!(
        select_pending_credential_for_family(
            &store,
            "dev-1",
            "dev.user+{n}@gmail.com",
            None,
            &excluded,
        )
        .map(|entry| entry.stored.email),
        Some("dev.user+6@gmail.com".to_string())
    );
}

#[test]
fn select_pending_template_hint_prefers_oldest_family_when_rank_is_equal() {
    let mut store = CredentialStore::default();
    store.pending.insert(
        "1.dev.astronlab+1@gmail.com".to_string(),
        make_pending(
            "1.dev.astronlab+1@gmail.com",
            "dev-1",
            "1.dev.astronlab+{n}@gmail.com",
            1,
            "2026-03-20T00:00:00.000Z",
        ),
    );
    store.pending.insert(
        "arjuda.anjum+1@gmail.com".to_string(),
        make_pending(
            "arjuda.anjum+1@gmail.com",
            "dev-1",
            "arjuda.anjum+{n}@gmail.com",
            1,
            "2026-03-21T00:00:00.000Z",
        ),
    );

    assert_eq!(
        select_pending_template_hint_for_profile(&store, "dev-1", None).as_deref(),
        Some("1.dev.astronlab+{n}@gmail.com")
    );
}

#[test]
fn select_stored_template_hint_prefers_common_and_high_frontier_template() {
    let mut store = CredentialStore::default();
    store.families.insert(
        "dev-1::qa.{n}@astronlab.com".to_string(),
        CredentialFamily {
            profile_name: "dev-1".to_string(),
            template: "qa.{n}@astronlab.com".to_string(),
            next_suffix: 300,
            max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
            created_at: "2026-04-06T16:00:00.000Z".to_string(),
            updated_at: "2026-04-06T16:00:00.000Z".to_string(),
            last_created_email: Some("qa.299@astronlab.com".to_string()),
            relogin: Vec::new(),
            suspend_domain_on_terminal_refresh_failure: false,
        },
    );
    store.pending.insert(
        "dev.35@astronlab.com".to_string(),
        make_pending(
            "dev.35@astronlab.com",
            "dev-1",
            "dev.{n}@astronlab.com",
            35,
            "2026-04-06T17:00:00.000Z",
        ),
    );
    store.pending.insert(
        "dev.36@astronlab.com".to_string(),
        make_pending(
            "dev.36@astronlab.com",
            "dev-1",
            "dev.{n}@astronlab.com",
            36,
            "2026-04-06T18:00:00.000Z",
        ),
    );

    assert_eq!(
        select_stored_template_hint(&store, "dev-1").as_deref(),
        Some("dev.{n}@astronlab.com")
    );

    let mut store = CredentialStore::default();
    store.pending.insert(
        "qa.300@astronlab.com".to_string(),
        make_pending(
            "qa.300@astronlab.com",
            "dev-1",
            "qa.{n}@astronlab.com",
            300,
            "2026-04-06T18:00:00.000Z",
        ),
    );
    store.pending.insert(
        "dev.35@astronlab.com".to_string(),
        make_pending(
            "dev.35@astronlab.com",
            "dev-1",
            "dev.{n}@astronlab.com",
            35,
            "2026-04-06T17:00:00.000Z",
        ),
    );

    assert_eq!(
        select_stored_template_hint(&store, "dev-1").as_deref(),
        Some("dev.{n}@astronlab.com")
    );
}

#[test]
fn resolve_create_template_for_profile_uses_pending_hint_before_new_default_family() {
    let mut store = CredentialStore::default();
    store.default_create_template = "qa.{n}@astronlab.com".to_string();
    store.pending.insert(
        "qa.300@astronlab.com".to_string(),
        make_pending(
            "qa.300@astronlab.com",
            "dev-1",
            "qa.{n}@astronlab.com",
            300,
            "2026-04-06T17:00:00.000Z",
        ),
    );

    assert_eq!(
        resolve_create_template_for_profile(&store, "dev-1", None, None).unwrap(),
        "qa.{n}@astronlab.com"
    );
}

#[test]
fn resolve_create_template_for_profile_uses_store_default_for_new_creates() {
    let mut store = CredentialStore::default();
    store.default_create_template = "qa.{n}@astronlab.com".to_string();

    assert_eq!(
        resolve_create_template_for_profile(&store, "dev-1", None, None).unwrap(),
        "qa.{n}@astronlab.com"
    );
}

#[test]
fn resolve_create_template_for_profile_prefers_store_default_before_existing_family_hint() {
    let mut store = CredentialStore::default();
    store.default_create_template = "dev.{n}@astronlab.com".to_string();
    store.domain.insert(
        "astronlab.com".to_string(),
        DomainConfig {
            rotation_enabled: false,
            max_suffix_per_family: None,
            reactivate_at: None,
        },
    );
    store.domain.insert(
        "gmail.com".to_string(),
        DomainConfig {
            rotation_enabled: true,
            max_suffix_per_family: Some(6),
            reactivate_at: None,
        },
    );
    store.families.insert(
        "dev-1::dev3astronlab+{n}@gmail.com".to_string(),
        CredentialFamily {
            profile_name: "dev-1".to_string(),
            template: "dev3astronlab+{n}@gmail.com".to_string(),
            next_suffix: 3,
            max_skipped_slots: 0,
            created_at: "2026-04-13T05:00:00.000Z".to_string(),
            updated_at: "2026-04-14T06:11:25.913Z".to_string(),
            last_created_email: Some("dev3astronlab+2@gmail.com".to_string()),
            relogin: Vec::new(),
            suspend_domain_on_terminal_refresh_failure: false,
        },
    );

    assert_eq!(
        resolve_create_template_for_profile(&store, "dev-1", None, None).unwrap(),
        "dev.{n}@astronlab.com"
    );
}

#[test]
fn resolve_create_template_for_profile_returns_store_default_even_when_hints_are_available() {
    let mut store = CredentialStore::default();
    store.default_create_template = "dev.{n}@astronlab.com".to_string();
    store.domain.insert(
        "astronlab.com".to_string(),
        DomainConfig {
            rotation_enabled: false,
            max_suffix_per_family: None,
            reactivate_at: None,
        },
    );
    store.domain.insert(
        "gmail.com".to_string(),
        DomainConfig {
            rotation_enabled: true,
            max_suffix_per_family: Some(6),
            reactivate_at: None,
        },
    );
    store.families.insert(
        "dev-1::dev.{n}@astronlab.com".to_string(),
        CredentialFamily {
            profile_name: "dev-1".to_string(),
            template: "dev.{n}@astronlab.com".to_string(),
            next_suffix: 107,
            max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
            created_at: "2026-04-13T05:00:00.000Z".to_string(),
            updated_at: "2026-04-14T06:11:25.913Z".to_string(),
            last_created_email: Some("dev.106@astronlab.com".to_string()),
            relogin: Vec::new(),
            suspend_domain_on_terminal_refresh_failure: false,
        },
    );
    store.families.insert(
        "dev-1::dev3astronlab+{n}@gmail.com".to_string(),
        CredentialFamily {
            profile_name: "dev-1".to_string(),
            template: "dev3astronlab+{n}@gmail.com".to_string(),
            next_suffix: 3,
            max_skipped_slots: 0,
            created_at: "2026-04-13T05:00:00.000Z".to_string(),
            updated_at: "2026-04-14T06:11:25.913Z".to_string(),
            last_created_email: Some("dev3astronlab+2@gmail.com".to_string()),
            relogin: Vec::new(),
            suspend_domain_on_terminal_refresh_failure: false,
        },
    );

    assert_eq!(
        resolve_create_template_for_profile(&store, "dev-1", None, None).unwrap(),
        "dev.{n}@astronlab.com"
    );
}

#[test]
fn resolve_create_template_for_profile_respects_explicit_override() {
    let mut store = CredentialStore::default();
    store.pending.insert(
        "dev.30@astronlab.com".to_string(),
        make_pending(
            "dev.30@astronlab.com",
            "dev-1",
            "dev.{n}@astronlab.com",
            30,
            "2026-04-06T16:00:00.000Z",
        ),
    );

    assert_eq!(
        resolve_create_template_for_profile(&store, "dev-1", Some("qa.{n}@astronlab.com"), None,)
            .unwrap(),
        "qa.{n}@astronlab.com"
    );
}

#[test]
fn create_template_guard_blocks_disabled_domain() {
    let mut store = CredentialStore::default();
    store.domain.insert(
        "astronlab.com".to_string(),
        DomainConfig {
            rotation_enabled: false,
            max_suffix_per_family: None,
            reactivate_at: None,
        },
    );

    let error =
        ensure_rotation_enabled_for_template_in_store(&store, "dev.{n}@astronlab.com").unwrap_err();
    assert!(error
        .to_string()
        .contains("Rotation is disabled for astronlab.com accounts"));
}

#[test]
fn create_template_guard_blocks_suffix_beyond_domain_limit() {
    let mut store = CredentialStore::default();
    store.domain.insert(
        "gmail.com".to_string(),
        DomainConfig {
            rotation_enabled: true,
            max_suffix_per_family: Some(6),
            reactivate_at: None,
        },
    );

    ensure_suffix_within_domain_limit_in_store(&store, "dev3astronlab+{n}@gmail.com", 6)
        .expect("suffix 6 should be allowed");
    let error =
        ensure_suffix_within_domain_limit_in_store(&store, "dev3astronlab+{n}@gmail.com", 7)
            .unwrap_err();
    assert!(error.to_string().contains("stops at suffix 6"));
}

#[test]
fn cmd_relogin_rejects_disabled_domain_selector() {
    with_rotate_home("codex-rotate-relogin-disabled-domain", |rotate_home| {
        let codex_home = rotate_home.join("codex-home");
        fs::create_dir_all(&codex_home).expect("create codex home");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        unsafe {
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            save_pool(&Pool {
                active_index: 0,
                accounts: vec![make_account_entry("dev.1@astronlab.com", "acct-1")],
            })?;

            let mut store = CredentialStore::default();
            store.domain.insert(
                "astronlab.com".to_string(),
                DomainConfig {
                    rotation_enabled: false,
                    max_suffix_per_family: None,
                    reactivate_at: None,
                },
            );
            save_credential_store(&store)?;

            let error = cmd_relogin("dev.1@astronlab.com", ReloginOptions::default()).unwrap_err();
            assert!(error
                .to_string()
                .contains("Rotation is disabled for astronlab.com accounts"));
            Ok(())
        })();

        match previous_codex_home {
            Some(value) => unsafe {
                std::env::set_var("CODEX_HOME", value);
            },
            None => unsafe {
                std::env::remove_var("CODEX_HOME");
            },
        }
        result.expect("relogin should block disabled domain");
    });
}

#[test]
fn relogin_selector_accepts_pending_email() {
    let pool = Pool {
        active_index: 0,
        accounts: vec![make_account_entry("dev.1@astronlab.com", "acct-1")],
    };
    let mut store = CredentialStore::default();
    store.pending.insert(
        "dev3astronlab+5@gmail.com".to_string(),
        make_pending(
            "dev3astronlab+5@gmail.com",
            "dev-1",
            "dev3astronlab+{n}@gmail.com",
            5,
            "2026-04-14T15:12:25.003Z",
        ),
    );

    let selection = resolve_relogin_target(&pool, &store, "dev3astronlab+5@gmail.com")
        .expect("pending selector should resolve");
    assert!(selection.selection.is_none());
    assert_eq!(
        selection
            .pending
            .as_ref()
            .map(|entry| entry.stored.email.as_str()),
        Some("dev3astronlab+5@gmail.com")
    );
}

#[test]
fn relogin_selector_accepts_exact_family_email_without_pool_or_pending() {
    let pool = Pool {
        active_index: 0,
        accounts: vec![make_account_entry("dev.1@astronlab.com", "acct-1")],
    };
    let mut store = CredentialStore::default();
    store.families.insert(
        "dev-1::dev3astronlab+{n}@gmail.com".to_string(),
        CredentialFamily {
            profile_name: "dev-1".to_string(),
            template: "dev3astronlab+{n}@gmail.com".to_string(),
            next_suffix: 7,
            max_skipped_slots: 0,
            created_at: "2026-04-13T05:00:00.000Z".to_string(),
            updated_at: "2026-04-14T16:01:04.952Z".to_string(),
            last_created_email: Some("dev3astronlab+4@gmail.com".to_string()),
            relogin: Vec::new(),
            suspend_domain_on_terminal_refresh_failure: false,
        },
    );

    let selection = resolve_relogin_target(&pool, &store, "dev3astronlab+6@gmail.com")
        .expect("family selector should resolve");
    let pending = selection.pending.expect("synthetic pending relogin target");
    assert!(selection.selection.is_none());
    assert_eq!(pending.stored.email, "dev3astronlab+6@gmail.com");
    assert_eq!(pending.stored.profile_name, "dev-1");
    assert_eq!(pending.stored.template, "dev3astronlab+{n}@gmail.com");
    assert_eq!(pending.stored.suffix, 6);
    assert_eq!(pending.stored.birth_month, Some(1));
    assert_eq!(pending.stored.birth_day, Some(24));
    assert_eq!(pending.stored.birth_year, Some(1990));
}

#[test]
fn relogin_birth_date_for_pending_reuses_pending_profile_birth_date() {
    let mut pending = make_pending(
        "dev3astronlab+5@gmail.com",
        "dev-1",
        "dev3astronlab+{n}@gmail.com",
        5,
        "2026-04-14T15:12:25.003Z",
    );
    pending.stored.birth_month = Some(1);
    pending.stored.birth_day = Some(24);
    pending.stored.birth_year = Some(1990);

    let birth_date = relogin_birth_date_for_pending(Some(&pending)).expect("pending birth date");
    assert_eq!(birth_date.birth_month, 1);
    assert_eq!(birth_date.birth_day, 24);
    assert_eq!(birth_date.birth_year, 1990);
}

#[test]
fn ensure_pending_relogin_target_inserts_synthesized_family_email_once() {
    let mut store = CredentialStore::default();
    let pending = make_pending(
        "dev3astronlab+6@gmail.com",
        "dev-1",
        "dev3astronlab+{n}@gmail.com",
        6,
        "2026-04-14T16:12:25.003Z",
    );

    assert!(ensure_pending_relogin_target(&mut store, Some(&pending)));
    assert!(store.pending.contains_key("dev3astronlab+6@gmail.com"));
    assert!(!ensure_pending_relogin_target(&mut store, Some(&pending)));
}

#[test]
fn ensure_pending_relogin_target_removes_matching_skipped_entry() {
    let mut store = CredentialStore::default();
    store.skipped = [
        "dev3astronlab+5@gmail.com".to_string(),
        "devbench.19@astronlab.com".to_string(),
    ]
    .into_iter()
    .collect();
    let pending = make_pending(
        "dev3astronlab+5@gmail.com",
        "dev-1",
        "dev3astronlab+{n}@gmail.com",
        5,
        "2026-04-14T16:12:25.003Z",
    );

    assert!(ensure_pending_relogin_target(&mut store, Some(&pending)));
    assert!(store.pending.contains_key("dev3astronlab+5@gmail.com"));
    assert_eq!(
        store.skipped,
        ["devbench.19@astronlab.com".to_string()]
            .into_iter()
            .collect()
    );
}

#[test]
fn ensure_pending_relogin_target_reports_dirty_when_only_skipped_cleanup_changed() {
    let mut store = CredentialStore::default();
    let pending = make_pending(
        "dev3astronlab+5@gmail.com",
        "dev-1",
        "dev3astronlab+{n}@gmail.com",
        5,
        "2026-04-14T16:12:25.003Z",
    );
    store
        .pending
        .insert("dev3astronlab+5@gmail.com".to_string(), pending.clone());
    store.skipped = ["dev3astronlab+5@gmail.com".to_string()]
        .into_iter()
        .collect();

    assert!(ensure_pending_relogin_target(&mut store, Some(&pending)));
    assert!(store.pending.contains_key("dev3astronlab+5@gmail.com"));
    assert!(store.skipped.is_empty());
}

#[test]
fn normalize_credential_store_drops_legacy_bench_families_on_v4_migration() {
    let store = normalize_credential_store(json!({
        "version": 3,
        "families": {
            "dev-1::bench.devicefix.{n}@astronlab.com": {
                "profile_name": "dev-1",
                "template": "bench.devicefix.{n}@astronlab.com",
                "next_suffix": 8,
                "created_at": "2026-04-06T00:00:00.000Z",
                "updated_at": "2026-04-06T00:00:00.000Z",
                "last_created_email": "bench.devicefix.7@astronlab.com"
            },
            "dev-1::dev.{n}@astronlab.com": {
                "profile_name": "dev-1",
                "template": "dev.{n}@astronlab.com",
                "next_suffix": 35,
                "created_at": "2026-04-06T00:00:00.000Z",
                "updated_at": "2026-04-06T00:00:00.000Z",
                "last_created_email": "dev.34@astronlab.com"
            }
        },
        "pending": {
            "bench.devicefix.8@astronlab.com": {
                "email": "bench.devicefix.8@astronlab.com",
                "profile_name": "dev-1",
                "template": "bench.devicefix.{n}@astronlab.com",
                "suffix": 8,
                "selector": null,
                "alias": null,
                "created_at": "2026-04-06T00:00:00.000Z",
                "updated_at": "2026-04-06T00:00:00.000Z"
            },
            "dev.35@astronlab.com": {
                "email": "dev.35@astronlab.com",
                "profile_name": "dev-1",
                "template": "dev.{n}@astronlab.com",
                "suffix": 35,
                "selector": null,
                "alias": null,
                "created_at": "2026-04-06T00:00:00.000Z",
                "updated_at": "2026-04-06T00:00:00.000Z"
            }
        }
    }));
    assert_eq!(store.version, 9);
    assert_eq!(store.default_create_template, "dev.{n}@astronlab.com");
    assert!(store.families.contains_key("dev-1::dev.{n}@astronlab.com"));
    assert!(!store
        .families
        .contains_key("dev-1::bench.devicefix.{n}@astronlab.com"));
    assert!(store.pending.contains_key("dev.35@astronlab.com"));
    assert!(!store
        .pending
        .contains_key("bench.devicefix.8@astronlab.com"));
}

#[test]
fn normalize_credential_store_keeps_gmail_pending_but_drops_non_dev_templates_in_v4_state() {
    let store = normalize_credential_store(json!({
        "version": 4,
        "pending": {
            "qa.300@astronlab.com": {
                "email": "qa.300@astronlab.com",
                "profile_name": "dev-1",
                "template": "qa.{n}@astronlab.com",
                "suffix": 300,
                "selector": null,
                "alias": null,
                "created_at": "2026-04-06T17:00:00.000Z",
                "updated_at": "2026-04-06T17:00:00.000Z",
                "started_at": "2026-04-06T17:00:00.000Z"
            },
            "dev.user+1@gmail.com": {
                "email": "dev.user+1@gmail.com",
                "profile_name": "dev-1",
                "template": "dev.user+{n}@gmail.com",
                "suffix": 1,
                "selector": null,
                "alias": null,
                "created_at": "2026-04-06T18:00:00.000Z",
                "updated_at": "2026-04-06T18:00:00.000Z",
                "started_at": "2026-04-06T18:00:00.000Z"
            },
            "dev.35@astronlab.com": {
                "email": "dev.35@astronlab.com",
                "profile_name": "dev-1",
                "template": "dev.{n}@astronlab.com",
                "suffix": 35,
                "selector": null,
                "alias": null,
                "created_at": "2026-04-06T19:00:00.000Z",
                "updated_at": "2026-04-06T19:00:00.000Z",
                "started_at": "2026-04-06T19:00:00.000Z"
            }
        }
    }));
    let mut pending_keys = store.pending.keys().cloned().collect::<Vec<_>>();
    pending_keys.sort();
    assert_eq!(
        pending_keys,
        vec![
            "dev.35@astronlab.com".to_string(),
            "dev.user+1@gmail.com".to_string()
        ]
    );
}

#[test]
fn normalize_credential_store_keeps_current_default_gmail_template_pending() {
    let store = normalize_credential_store(json!({
        "version": 7,
        "default_create_template": "dev3astronlab+{n}@gmail.com",
        "families": {
            "dev-1::dev3astronlab+{n}@gmail.com": {
                "profile_name": "dev-1",
                "template": "dev3astronlab+{n}@gmail.com",
                "next_suffix": 7,
                "max_skipped_slots": 0,
                "created_at": "2026-04-13T05:00:00.000Z",
                "updated_at": "2026-04-14T15:12:25.003Z",
                "last_created_email": "dev3astronlab+4@gmail.com",
                "relogin": []
            }
        },
        "pending": {
            "dev3astronlab+5@gmail.com": {
                "email": "dev3astronlab+5@gmail.com",
                "profile_name": "dev-1",
                "template": "dev3astronlab+{n}@gmail.com",
                "suffix": 5,
                "selector": null,
                "alias": null,
                "created_at": "2026-04-14T15:12:25.003Z",
                "updated_at": "2026-04-14T15:12:25.003Z",
                "started_at": "2026-04-14T15:12:25.003Z"
            }
        }
    }));
    assert!(store.pending.contains_key("dev3astronlab+5@gmail.com"));
}

#[test]
fn reuses_existing_birth_date() {
    let value = resolve_credential_birth_date(
        Some(&StoredCredential {
            email: "dev.user+1@gmail.com".to_string(),
            profile_name: "dev-1".to_string(),
            template: "dev.user+{n}@gmail.com".to_string(),
            suffix: 1,
            selector: None,
            alias: None,
            birth_month: Some(7),
            birth_day: Some(14),
            birth_year: Some(1994),
            created_at: "2026-03-20T00:00:00.000Z".to_string(),
            updated_at: "2026-03-20T00:00:00.000Z".to_string(),
        }),
        Some(&AdultBirthDate {
            birth_month: 1,
            birth_day: 24,
            birth_year: 1990,
        }),
    )
    .expect("birth date");
    assert_eq!(value.birth_month, 7);
    assert_eq!(value.birth_day, 14);
    assert_eq!(value.birth_year, 1994);
}

#[test]
fn drops_legacy_secret_fields_from_loaded_records() {
    let store = normalize_credential_store(json!({
        "accounts": {
            "dev.user+1@gmail.com": {
                "email": "dev.user+1@gmail.com",
                "password": "pw-1",
                "account_secret_ref": {
                    "type": "secret_ref",
                    "store": "bitwarden-cli",
                    "object_id": "bw-1"
                },
                "profile_name": "dev-1",
                "template": "dev.user+{n}@gmail.com",
                "suffix": 1,
                "selector": "dev.user+1@gmail.com_free",
                "alias": null,
                "created_at": "2026-03-20T00:00:00.000Z",
                "updated_at": "2026-03-20T00:00:00.000Z"
            }
        }
    }));
    let family = store.families.get("dev-1::dev.user+{n}@gmail.com").unwrap();
    assert_eq!(family.profile_name, "dev-1");
    assert_eq!(family.template, "dev.user+{n}@gmail.com");
    assert_eq!(family.next_suffix, 2);
    assert_eq!(
        family.last_created_email.as_deref(),
        Some("dev.user+1@gmail.com")
    );
}

#[test]
fn drops_pending_entries_that_already_exist_in_inventory() {
    let store = normalize_credential_store(json!({
        "accounts": [
            make_account_entry("dev.1@astronlab.com", "acct-1")
        ],
        "pending": {
            "dev.1@astronlab.com": {
                "email": "dev.1@astronlab.com",
                "profile_name": "dev-1",
                "template": "dev.{n}@astronlab.com",
                "suffix": 1,
                "selector": null,
                "alias": null,
                "created_at": "2026-04-05T04:50:10.406Z",
                "updated_at": "2026-04-05T05:39:48.882Z"
            }
        }
    }));
    assert!(store.pending.is_empty());
}

#[test]
fn keeps_pending_entries_for_missing_lower_suffixes() {
    let store = normalize_credential_store(json!({
        "accounts": [
            make_account_entry("dev.23@astronlab.com", "acct-23")
        ],
        "pending": {
            "dev.1@astronlab.com": {
                "email": "dev.1@astronlab.com",
                "profile_name": "dev-1",
                "template": "dev.{n}@astronlab.com",
                "suffix": 1,
                "selector": null,
                "alias": null,
                "created_at": "2026-04-05T04:50:10.406Z",
                "updated_at": "2026-04-05T05:39:48.882Z"
            }
        }
    }));
    assert!(store.pending.contains_key("dev.1@astronlab.com"));
}

#[test]
fn builds_openai_login_locator_from_email() {
    let locator = build_openai_account_login_locator("Dev.User+1@gmail.com");
    match locator {
        CodexRotateSecretLocator::LoginLookup {
            store,
            username,
            uris,
            field_path,
        } => {
            assert_eq!(store, "bitwarden-cli");
            assert_eq!(username, "dev.user+1@gmail.com");
            assert_eq!(field_path, "/password");
            assert_eq!(
                uris,
                vec![
                    "https://auth.openai.com".to_string(),
                    "https://chatgpt.com".to_string()
                ]
            );
        }
    }
}

#[test]
fn codex_login_retry_policy_recognizes_verification_waits() {
    assert!(is_retryable_codex_login_workflow_error_message(
        "signup-verification-code-missing"
    ));
    assert!(is_retryable_codex_login_workflow_error_message(
            "login-verification-submit-stuck:email_verification:https://auth.openai.com/email-verification"
        ));
    assert!(!is_retryable_codex_login_workflow_error_message(
        "OpenAI rejected the stored password"
    ));
    assert!(!is_retryable_codex_login_workflow_error_message(
        "device auth failed with status 429"
    ));
}

#[test]
fn codex_login_retry_policy_uses_expected_delay_tables() {
    assert_eq!(
        codex_login_retry_delay_ms(Some("verification_artifact_pending"), 1),
        5_000
    );
    assert_eq!(
        codex_login_retry_delay_ms(Some("verification_artifact_pending"), 2),
        10_000
    );
    assert_eq!(
        codex_login_retry_delay_ms(Some("device_auth_rate_limit"), 1),
        30_000
    );
    assert_eq!(
        codex_login_retry_delay_ms(Some("device_auth_rate_limit"), 2),
        60_000
    );
}

#[test]
fn codex_login_retry_policy_keeps_reusable_device_auth_session_after_post_issue_429() {
    assert!(!should_reset_device_auth_session_for_rate_limit(
        "Error logging in with device code: device auth failed with status 429 Too Many Requests",
        Some(&CodexRotateAuthFlowSession {
            auth_url: Some("https://auth.openai.com/codex/device".to_string()),
            device_code: Some("ABCD-12345".to_string()),
            ..CodexRotateAuthFlowSession::default()
        })
    ));
}

#[test]
fn codex_login_retry_policy_drops_unissued_device_auth_session_after_429() {
    assert!(should_reset_device_auth_session_for_rate_limit(
            "Error logging in with device code: device code request failed with status 429 Too Many Requests",
            Some(&CodexRotateAuthFlowSession::default())
        ));
}

#[test]
fn codex_login_retry_policy_resets_expected_sessions() {
    assert!(!should_reset_codex_login_session_for_retry(
        Some("retryable_timeout"),
        1
    ));
    assert!(should_reset_codex_login_session_for_retry(
        Some("retryable_timeout"),
        2
    ));
    assert!(should_reset_codex_login_session_for_retry(
        Some("state_mismatch"),
        1
    ));
    assert!(should_reset_codex_login_session_for_retry(
        Some("state_mismatch"),
        2
    ));
    assert!(should_reset_codex_login_session_for_retry(
        Some("username_not_found"),
        1
    ));
    assert!(should_reset_codex_login_session_for_retry(
        Some("final_add_phone"),
        1
    ));
}

#[test]
fn codex_login_retry_policy_extends_final_add_phone_budget() {
    assert_eq!(
        codex_login_max_attempts(None),
        DEFAULT_CODEX_LOGIN_MAX_ATTEMPTS
    );
    assert_eq!(
        codex_login_max_attempts(Some("retryable_timeout")),
        DEFAULT_CODEX_LOGIN_MAX_ATTEMPTS
    );
    assert_eq!(
        codex_login_max_attempts(Some("final_add_phone")),
        FINAL_ADD_PHONE_CODEX_LOGIN_MAX_ATTEMPTS
    );
}

#[test]
fn codex_login_retry_policy_skips_account_after_final_add_phone_budget() {
    assert!(!should_skip_account_after_retry_exhaustion(None));
    assert!(!should_skip_account_after_retry_exhaustion(Some(
        "retryable_timeout"
    )));
    assert!(should_skip_account_after_retry_exhaustion(Some(
        "final_add_phone"
    )));
}

#[test]
fn final_add_phone_short_circuit_flag_is_opt_in() {
    unsafe {
        std::env::remove_var(CODEX_ROTATE_STOP_ON_FINAL_ADD_PHONE_ENV);
    }
    assert!(!stop_on_final_add_phone_retry_exhaustion());
    unsafe {
        std::env::set_var(CODEX_ROTATE_STOP_ON_FINAL_ADD_PHONE_ENV, "1");
    }
    assert!(stop_on_final_add_phone_retry_exhaustion());
    unsafe {
        std::env::remove_var(CODEX_ROTATE_STOP_ON_FINAL_ADD_PHONE_ENV);
    }
}

#[test]
fn final_add_phone_short_circuit_returns_skip_account_error() {
    let error = final_add_phone_short_circuit_error(
        "dev.user+5@gmail.com",
        Some("https://auth.openai.com/log-in"),
        Some("blocked"),
    );
    assert!(is_workflow_skip_account_error(&error));
    assert!(should_retry_create_after_error(
        &CreateCommandOptions::default(),
        &error
    ));
}

#[test]
fn detects_final_add_phone_environment_blocker_errors() {
    assert!(is_final_add_phone_environment_blocker_error(&anyhow!(
            "OpenAI final_add_phone blocked dev3astronlab+5@gmail.com (https://auth.openai.com/log-in)."
        )));
    assert!(is_final_add_phone_environment_blocker_error(&anyhow!(
            "The workflow requested skipping dev3astronlab+5@gmail.com after exhausting final add-phone retries (https://auth.openai.com/log-in)."
        )));
    assert!(is_final_add_phone_environment_blocker_error(&anyhow!(
        "OpenAI still requires phone setup before the Codex callback can complete."
    )));
    assert!(!is_final_add_phone_environment_blocker_error(&anyhow!(
        "Codex browser login did not reach the callback for dev3astronlab+5@gmail.com."
    )));
}

#[test]
fn create_preserves_pending_for_final_add_phone_environment_blockers() {
    assert!(should_preserve_pending_on_create_error(&anyhow!(
            "OpenAI final_add_phone blocked dev3astronlab+6@gmail.com (https://auth.openai.com/add-phone)."
        )));
    assert!(should_preserve_pending_on_create_error(&anyhow!(
            "The workflow requested skipping dev3astronlab+6@gmail.com after exhausting final add-phone retries (https://auth.openai.com/log-in)."
        )));
    assert!(!should_preserve_pending_on_create_error(&anyhow!(
            "Workflow input 'account_login_ref' must be a secret ref to satisfy step 'fill_signup_password'"
        )));
}

#[test]
fn relogin_family_match_prefers_exact_last_created_email() {
    let mut store = CredentialStore::default();
    store.families.insert(
        "dev-1::dev.user+{n}@gmail.com".to_string(),
        CredentialFamily {
            profile_name: "dev-1".to_string(),
            template: "dev.user+{n}@gmail.com".to_string(),
            next_suffix: 4,
            max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
            created_at: "2026-03-20T00:00:00.000Z".to_string(),
            updated_at: "2026-03-20T01:00:00.000Z".to_string(),
            last_created_email: Some("dev.user+3@gmail.com".to_string()),
            relogin: Vec::new(),
            suspend_domain_on_terminal_refresh_failure: false,
        },
    );
    store.families.insert(
        "dev-2::dev.user+{n}@gmail.com".to_string(),
        CredentialFamily {
            profile_name: "dev-2".to_string(),
            template: "dev.user+{n}@gmail.com".to_string(),
            next_suffix: 5,
            max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
            created_at: "2026-03-20T00:00:00.000Z".to_string(),
            updated_at: "2026-03-20T02:00:00.000Z".to_string(),
            last_created_email: Some("dev.user+2@gmail.com".to_string()),
            relogin: Vec::new(),
            suspend_domain_on_terminal_refresh_failure: false,
        },
    );

    let match_result = select_family_for_account_email(&store, "dev.user+2@gmail.com")
        .expect("expected exact family match");
    assert_eq!(match_result.family.profile_name, "dev-2");
    assert_eq!(match_result.suffix, 2);
}

#[test]
fn relogin_family_match_refuses_ambiguous_non_exact_matches() {
    let mut store = CredentialStore::default();
    store.families.insert(
        "dev-1::dev.user+{n}@gmail.com".to_string(),
        CredentialFamily {
            profile_name: "dev-1".to_string(),
            template: "dev.user+{n}@gmail.com".to_string(),
            next_suffix: 4,
            max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
            created_at: "2026-03-20T00:00:00.000Z".to_string(),
            updated_at: "2026-03-20T01:00:00.000Z".to_string(),
            last_created_email: Some("dev.user+3@gmail.com".to_string()),
            relogin: Vec::new(),
            suspend_domain_on_terminal_refresh_failure: false,
        },
    );
    store.families.insert(
        "dev-2::dev.user+{n}@gmail.com".to_string(),
        CredentialFamily {
            profile_name: "dev-2".to_string(),
            template: "dev.user+{n}@gmail.com".to_string(),
            next_suffix: 5,
            max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
            created_at: "2026-03-20T00:00:00.000Z".to_string(),
            updated_at: "2026-03-20T02:00:00.000Z".to_string(),
            last_created_email: Some("dev.user+4@gmail.com".to_string()),
            relogin: Vec::new(),
            suspend_domain_on_terminal_refresh_failure: false,
        },
    );

    assert!(select_family_for_account_email(&store, "dev.user+2@gmail.com").is_none());
}

#[test]
fn relogin_family_match_supports_bare_gmail_template() {
    let mut store = CredentialStore::default();
    store.families.insert(
        "dev-1::dev.user+{n}@gmail.com".to_string(),
        CredentialFamily {
            profile_name: "dev-1".to_string(),
            template: "dev.user+{n}@gmail.com".to_string(),
            next_suffix: 4,
            max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
            created_at: "2026-03-20T00:00:00.000Z".to_string(),
            updated_at: "2026-03-20T01:00:00.000Z".to_string(),
            last_created_email: Some("dev.user+3@gmail.com".to_string()),
            relogin: Vec::new(),
            suspend_domain_on_terminal_refresh_failure: false,
        },
    );

    let match_result = select_family_for_account_email(&store, "dev.user@gmail.com")
        .expect("expected bare gmail family match");
    assert_eq!(match_result.family.profile_name, "dev-1");
    assert_eq!(match_result.suffix, 0);
}

#[test]
fn add_reconciliation_moves_matching_pending_into_family_state() {
    let mut store = CredentialStore::default();
    store.pending.insert(
        "dev.24@astronlab.com".to_string(),
        PendingCredential {
            stored: StoredCredential {
                email: "dev.24@astronlab.com".to_string(),
                profile_name: "dev-1".to_string(),
                template: "dev.{n}@astronlab.com".to_string(),
                suffix: 24,
                selector: None,
                alias: None,
                birth_month: Some(1),
                birth_day: Some(24),
                birth_year: Some(1990),
                created_at: "2026-04-05T05:51:09.049Z".to_string(),
                updated_at: "2026-04-05T05:51:09.049Z".to_string(),
            },
            started_at: Some("2026-04-05T05:51:09.049Z".to_string()),
        },
    );
    let entry = AccountEntry {
        label: "dev.24@astronlab.com_free".to_string(),
        alias: None,
        email: "dev.24@astronlab.com".to_string(),
        account_id: "acct-24".to_string(),
        plan_type: "free".to_string(),
        auth: CodexAuth {
            auth_mode: "chatgpt".to_string(),
            openai_api_key: None,
            tokens: crate::auth::AuthTokens {
                id_token: "id".to_string(),
                access_token: "access".to_string(),
                refresh_token: Some("refresh".to_string()),
                account_id: "acct-24".to_string(),
            },
            last_refresh: "2026-04-05T05:51:09.049Z".to_string(),
        },
        added_at: "2026-04-05T05:51:09.049Z".to_string(),
        last_quota_usable: None,
        last_quota_summary: None,
        last_quota_blocker: None,
        last_quota_checked_at: None,
        last_quota_primary_left_percent: None,
        last_quota_next_refresh_at: None,
        persona: None,
    };

    let pending = store.pending.remove("dev.24@astronlab.com").unwrap();
    assert!(upsert_family_for_account(
        &mut store,
        &StoredCredential {
            email: entry.email.clone(),
            profile_name: pending.stored.profile_name,
            template: pending.stored.template,
            suffix: pending.stored.suffix,
            selector: Some(entry.label.clone()),
            alias: entry.alias.clone(),
            birth_month: pending.stored.birth_month,
            birth_day: pending.stored.birth_day,
            birth_year: pending.stored.birth_year,
            created_at: pending.stored.created_at,
            updated_at: "2026-04-05T05:52:00.000Z".to_string(),
        },
    ));
    let family = store.families.get("dev-1::dev.{n}@astronlab.com").unwrap();
    assert_eq!(family.next_suffix, 25);
    assert_eq!(
        family.last_created_email.as_deref(),
        Some("dev.24@astronlab.com")
    );
    assert!(store.pending.is_empty());
}

#[test]
fn add_reconciliation_updates_matching_bare_gmail_family_state() {
    with_rotate_home("codex-rotate-add-reconcile-bare-gmail", |_| {
        let mut store = CredentialStore::default();
        store.families.insert(
            "dev-1::supplyprima1+{n}@gmail.com".to_string(),
            CredentialFamily {
                profile_name: "dev-1".to_string(),
                template: "supplyprima1+{n}@gmail.com".to_string(),
                next_suffix: 3,
                max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
                created_at: "2026-04-05T05:51:09.049Z".to_string(),
                updated_at: "2026-04-05T05:51:09.049Z".to_string(),
                last_created_email: Some("supplyprima1+2@gmail.com".to_string()),
                relogin: Vec::new(),
                suspend_domain_on_terminal_refresh_failure: false,
            },
        );
        save_credential_store(&store).expect("save credential store");

        let entry = AccountEntry {
            label: "supplyprima1@gmail.com_free".to_string(),
            alias: None,
            email: "supplyprima1@gmail.com".to_string(),
            account_id: "acct-supplyprima1".to_string(),
            plan_type: "free".to_string(),
            auth: make_auth("supplyprima1@gmail.com", "acct-supplyprima1"),
            added_at: "2026-04-05T05:51:09.049Z".to_string(),
            last_quota_usable: None,
            last_quota_summary: None,
            last_quota_blocker: None,
            last_quota_checked_at: None,
            last_quota_primary_left_percent: None,
            last_quota_next_refresh_at: None,
            persona: None,
        };

        let changed = reconcile_added_account_credential_state(&entry)
            .expect("reconcile added account should succeed");
        assert!(changed);

        let updated_store = load_credential_store().expect("load credential store");
        let family = updated_store
            .families
            .get("dev-1::supplyprima1+{n}@gmail.com")
            .expect("gmail family should remain");
        assert_eq!(family.next_suffix, 3);
        assert_eq!(
            family.last_created_email.as_deref(),
            Some("supplyprima1@gmail.com")
        );
        assert!(
            parse_sortable_timestamp(Some(family.updated_at.as_str()))
                > parse_sortable_timestamp(Some("2026-04-05T05:51:09.049Z"))
        );
    });
}

#[test]
fn created_pool_lookup_prefers_expected_email_over_stale_account_id_match() {
    let pool = Pool {
        active_index: 0,
        accounts: vec![
            AccountEntry {
                label: "dev.98@astronlab.com_free".to_string(),
                alias: None,
                email: "dev.98@astronlab.com".to_string(),
                account_id: "acct-shared".to_string(),
                plan_type: "free".to_string(),
                auth: CodexAuth {
                    auth_mode: "chatgpt".to_string(),
                    openai_api_key: None,
                    tokens: crate::auth::AuthTokens {
                        id_token: "id-old".to_string(),
                        access_token: "access-old".to_string(),
                        refresh_token: Some("refresh-old".to_string()),
                        account_id: "acct-shared".to_string(),
                    },
                    last_refresh: "2026-04-13T02:52:14.756Z".to_string(),
                },
                added_at: "2026-04-13T02:52:14.756Z".to_string(),
                last_quota_usable: None,
                last_quota_summary: None,
                last_quota_blocker: None,
                last_quota_checked_at: None,
                last_quota_primary_left_percent: None,
                last_quota_next_refresh_at: None,
                persona: None,
            },
            AccountEntry {
                label: "devbench.17@astronlab.com_free".to_string(),
                alias: None,
                email: "devbench.17@astronlab.com".to_string(),
                account_id: "acct-devbench-17".to_string(),
                plan_type: "free".to_string(),
                auth: CodexAuth {
                    auth_mode: "chatgpt".to_string(),
                    openai_api_key: None,
                    tokens: crate::auth::AuthTokens {
                        id_token: "id-new".to_string(),
                        access_token: "access-new".to_string(),
                        refresh_token: Some("refresh-new".to_string()),
                        account_id: "acct-devbench-17".to_string(),
                    },
                    last_refresh: "2026-04-13T02:52:15.012Z".to_string(),
                },
                added_at: "2026-04-13T02:52:15.012Z".to_string(),
                last_quota_usable: None,
                last_quota_summary: None,
                last_quota_blocker: None,
                last_quota_checked_at: None,
                last_quota_primary_left_percent: None,
                last_quota_next_refresh_at: None,
                persona: None,
            },
        ],
    };

    assert_eq!(
        find_created_pool_entry_index(&pool, "acct-shared", "devbench.17@astronlab.com", "free",),
        Some(1)
    );
}

#[test]
fn created_pool_lookup_distinguishes_same_email_different_plan() {
    let pool = Pool {
        active_index: 0,
        accounts: vec![
            AccountEntry {
                label: "dev.1@hotspotprime.com_team".to_string(),
                alias: None,
                email: "dev.1@hotspotprime.com".to_string(),
                account_id: "acct-team".to_string(),
                plan_type: "team".to_string(),
                auth: CodexAuth {
                    auth_mode: "chatgpt".to_string(),
                    openai_api_key: None,
                    tokens: crate::auth::AuthTokens {
                        id_token: "id-team".to_string(),
                        access_token: "access-team".to_string(),
                        refresh_token: Some("refresh-team".to_string()),
                        account_id: "acct-team".to_string(),
                    },
                    last_refresh: "2026-04-14T21:43:26.386Z".to_string(),
                },
                added_at: "2026-04-14T21:41:35.592Z".to_string(),
                last_quota_usable: None,
                last_quota_summary: None,
                last_quota_blocker: None,
                last_quota_checked_at: None,
                last_quota_primary_left_percent: None,
                last_quota_next_refresh_at: None,
                persona: None,
            },
            AccountEntry {
                label: "dev.1@hotspotprime.com_free".to_string(),
                alias: None,
                email: "dev.1@hotspotprime.com".to_string(),
                account_id: "acct-free".to_string(),
                plan_type: "free".to_string(),
                auth: CodexAuth {
                    auth_mode: "chatgpt".to_string(),
                    openai_api_key: None,
                    tokens: crate::auth::AuthTokens {
                        id_token: "id-free".to_string(),
                        access_token: "access-free".to_string(),
                        refresh_token: Some("refresh-free".to_string()),
                        account_id: "acct-free".to_string(),
                    },
                    last_refresh: "2026-04-14T21:43:29.948Z".to_string(),
                },
                added_at: "2026-04-14T21:41:35.592Z".to_string(),
                last_quota_usable: None,
                last_quota_summary: None,
                last_quota_blocker: None,
                last_quota_checked_at: None,
                last_quota_primary_left_percent: None,
                last_quota_next_refresh_at: None,
                persona: None,
            },
        ],
    };

    assert_eq!(
        find_created_pool_entry_index(&pool, "acct-team", "dev.1@hotspotprime.com", "team",),
        Some(0)
    );
    assert_eq!(
        find_created_pool_entry_index(&pool, "acct-free", "dev.1@hotspotprime.com", "free",),
        Some(1)
    );
}

#[test]
fn completed_login_prefers_session_auth_file_over_default_auth_home() {
    with_rotate_home("codex-rotate-session-auth-preferred", |rotate_home| {
        let codex_home = rotate_home.join("codex-home");
        let detached_home = rotate_home.join("detached-codex-home");
        fs::create_dir_all(&codex_home).expect("create codex home");
        fs::create_dir_all(&detached_home).expect("create detached codex home");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        unsafe {
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let shared_auth_path = codex_home.join("auth.json");
            let detached_auth_path = detached_home.join("auth.json");
            write_codex_auth(
                &shared_auth_path,
                &make_auth("dev.98@astronlab.com", "acct-98"),
            )?;
            write_codex_auth(
                &detached_auth_path,
                &make_auth("devbench.17@astronlab.com", "acct-devbench-17"),
            )?;

            let auth = load_auth_for_completed_login(&CompleteCodexLoginOutcome {
                codex_session: Some(CodexRotateAuthFlowSession {
                    auth_file_path: Some(detached_auth_path.display().to_string()),
                    ..CodexRotateAuthFlowSession::default()
                }),
                ..CompleteCodexLoginOutcome::default()
            })?;

            assert_eq!(
                summarize_codex_auth(&auth).email,
                "devbench.17@astronlab.com"
            );
            assert_eq!(extract_account_id_from_auth(&auth), "acct-devbench-17");
            Ok(())
        })();

        match previous_codex_home {
            Some(value) => unsafe {
                std::env::set_var("CODEX_HOME", value);
            },
            None => unsafe {
                std::env::remove_var("CODEX_HOME");
            },
        }

        result.expect("session auth should override default auth");
    });
}

#[test]
fn relogin_restore_without_snapshot_preserves_current_auth_file() {
    with_rotate_home(
        "codex-rotate-relogin-restore-preserve-auth",
        |rotate_home| {
            let codex_home = rotate_home.join("codex-home");
            fs::create_dir_all(&codex_home).expect("create codex home");
            let previous_codex_home = std::env::var_os("CODEX_HOME");
            unsafe {
                std::env::set_var("CODEX_HOME", &codex_home);
            }

            let result = (|| -> Result<()> {
                let auth_path = codex_home.join("auth.json");
                let current_auth = make_auth("dev3astronlab+1@gmail.com", "acct-gmail-1");
                write_codex_auth(&auth_path, &current_auth)?;

                restore_active_auth_after_relogin(None)?;

                let restored = load_codex_auth(&auth_path)?;
                assert_eq!(restored, current_auth);
                Ok(())
            })();

            match previous_codex_home {
                Some(value) => unsafe {
                    std::env::set_var("CODEX_HOME", value);
                },
                None => unsafe {
                    std::env::remove_var("CODEX_HOME");
                },
            }

            result.expect("relogin restore should preserve existing auth without a snapshot");
        },
    );
}

#[test]
fn migrates_legacy_credential_store_into_accounts_json() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let rotate_home = unique_temp_dir("codex-rotate-legacy-store");
    fs::create_dir_all(&rotate_home).expect("create rotate home");
    let accounts_path = rotate_home.join("accounts.json");
    let legacy_path = rotate_home.join("credentials.json");
    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
    }

    let result = (|| -> Result<()> {
        fs::write(
            &accounts_path,
            serde_json::json!({
                "active_index": 2,
                "accounts": [
                    make_account_entry("dev.20@astronlab.com", "acct-20"),
                    make_account_entry("dev.21@astronlab.com", "acct-21"),
                    make_account_entry("dev.22@astronlab.com", "acct-22")
                ],
            })
            .to_string(),
        )?;
        fs::write(
            &legacy_path,
            serde_json::json!({
                "version": 3,
                "families": {
                    "dev-1::dev.{n}@astronlab.com": {
                        "profile_name": "dev-1",
                        "template": "dev.{n}@astronlab.com",
                        "next_suffix": 23,
                        "created_at": "2026-04-05T00:00:00.000Z",
                        "updated_at": "2026-04-05T00:00:00.000Z",
                        "last_created_email": "dev.22@astronlab.com"
                    }
                },
                "pending": {
                    "dev.23@astronlab.com": {
                        "email": "dev.23@astronlab.com",
                        "profile_name": "dev-1",
                        "template": "dev.{n}@astronlab.com",
                        "suffix": 23,
                        "selector": null,
                        "alias": null,
                        "created_at": "2026-04-05T00:00:00.000Z",
                        "updated_at": "2026-04-05T00:00:00.000Z",
                        "started_at": "2026-04-05T00:00:00.000Z"
                    }
                }
            })
            .to_string(),
        )?;

        assert!(migrate_legacy_credential_store_if_needed()?);

        let merged: serde_json::Value = serde_json::from_str(&fs::read_to_string(&accounts_path)?)?;
        assert_eq!(merged["active_index"], 2);
        assert_eq!(merged["accounts"][2]["email"], "dev.22@astronlab.com");
        assert_eq!(
            merged["families"]["dev-1::dev.{n}@astronlab.com"]["next_suffix"],
            23
        );
        assert_eq!(
            merged["pending"]["dev.23@astronlab.com"]["email"],
            "dev.23@astronlab.com"
        );
        assert!(!legacy_path.exists());
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
    fs::remove_dir_all(&rotate_home).ok();
    result.expect("legacy credential migration");
}

#[test]
fn loading_credential_store_migrates_legacy_file_automatically() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let rotate_home = unique_temp_dir("codex-rotate-load-store");
    fs::create_dir_all(&rotate_home).expect("create rotate home");
    let legacy_path = rotate_home.join("credentials.json");
    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
    }

    let result = (|| -> Result<()> {
        fs::write(
            &legacy_path,
            serde_json::json!({
                "version": 3,
                "families": {
                    "dev-1::dev.{n}@astronlab.com": {
                        "profile_name": "dev-1",
                        "template": "dev.{n}@astronlab.com",
                        "next_suffix": 23,
                        "created_at": "2026-04-05T00:00:00.000Z",
                        "updated_at": "2026-04-05T00:00:00.000Z",
                        "last_created_email": "dev.22@astronlab.com"
                    }
                },
                "pending": {
                    "dev.23@astronlab.com": {
                        "email": "dev.23@astronlab.com",
                        "profile_name": "dev-1",
                        "template": "dev.{n}@astronlab.com",
                        "suffix": 23,
                        "selector": null,
                        "alias": null,
                        "created_at": "2026-04-05T00:00:00.000Z",
                        "updated_at": "2026-04-05T00:00:00.000Z",
                        "started_at": "2026-04-05T00:00:00.000Z"
                    }
                }
            })
            .to_string(),
        )?;

        let store = load_credential_store()?;
        assert_eq!(
            store
                .families
                .get("dev-1::dev.{n}@astronlab.com")
                .map(|family| family.next_suffix),
            Some(23)
        );
        assert!(store.pending.contains_key("dev.23@astronlab.com"));
        assert!(!legacy_path.exists());
        assert!(rotate_home.join("accounts.json").exists());
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
    fs::remove_dir_all(&rotate_home).ok();
    result.expect("load credential store migration");
}

#[test]
fn loading_credential_store_keeps_read_path_side_effect_free() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let rotate_home = unique_temp_dir("codex-rotate-pure-load-store");
    fs::create_dir_all(&rotate_home).expect("create rotate home");
    let accounts_path = rotate_home.join("accounts.json");
    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");

    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
    }

    let result = (|| -> Result<()> {
        fs::write(
            &accounts_path,
            serde_json::json!({
                "version": 9,
                "pending": {
                    "dev.5@astronlab.com": {
                        "email": "dev.5@astronlab.com",
                        "profile_name": "dev-1",
                        "template": "dev.{n}@astronlab.com",
                        "suffix": 5,
                        "selector": null,
                        "alias": null,
                        "created_at": "2026-04-05T00:00:00.000Z",
                        "updated_at": "2026-04-05T00:00:00.000Z",
                        "started_at": "2026-04-05T00:00:00.000Z"
                    }
                }
            })
            .to_string(),
        )?;

        let before = fs::read_to_string(&accounts_path)?;
        let store = load_credential_store()?;
        let after = fs::read_to_string(&accounts_path)?;

        assert!(store.pending.contains_key("dev.5@astronlab.com"));
        assert_eq!(after, before);
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
    fs::remove_dir_all(&rotate_home).ok();
    result.expect("pure credential store load");
}
