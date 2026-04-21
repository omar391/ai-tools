#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use codex_rotate_core::pool::load_pool;
use codex_rotate_test_support::{IsolatedAccountStateFixture, WatchTriggerHarness};

fn write_executable(path: &Path, contents: &str) -> Result<()> {
    fs::write(path, contents)?;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

fn env_mutex() -> &'static std::sync::Mutex<()> {
    static MUTEX: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    MUTEX.get_or_init(|| std::sync::Mutex::new(()))
}

fn spawn_usage_server(
    source_account_id: String,
    source_body: String,
    target_account_id: String,
    target_body: String,
) -> (String, std::thread::JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind usage server");
    listener.set_nonblocking(true).expect("set usage server nonblocking");
    let address = listener.local_addr().expect("usage server address");
    let handle = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if let Ok((mut stream, _)) = listener.accept() {
                use std::io::{Read, Write};
                let mut buf = [0; 1024];
                let bytes_read = stream.read(&mut buf).unwrap_or(0);
                let request_text = String::from_utf8_lossy(&buf[..bytes_read]);
                let account_id = request_text
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.trim()
                            .eq_ignore_ascii_case("ChatGPT-Account-Id")
                            .then(|| value.trim().to_string())
                    })
                    .unwrap_or_default();
                let body = if account_id == source_account_id {
                    source_body.as_str()
                } else if account_id == target_account_id {
                    target_body.as_str()
                } else {
                    source_body.as_str()
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body,
                );
                let _ = stream.write_all(response.as_bytes());
                continue;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    });
    (format!("http://127.0.0.1:{}", address.port()), handle)
}

#[test]
fn hermetic_watch_triggered_host_rotation_happy_path() -> Result<()> {
    let fixture = IsolatedAccountStateFixture::builder("watch-host")?
        .active_index(0)
        .build()?;

    let sandbox = fixture.sandbox_root().to_path_buf();
    let rotate_home = fixture.rotate_home().to_path_buf();
    let codex_home = fixture.codex_home().to_path_buf();
    let accounts = fixture.accounts();
    let source_account = &accounts[0];
    let target_account = &accounts[1];

    // Prepare assets
    let asset_root = sandbox.join("assets");
    fs::create_dir_all(&asset_root)?;
    let opener_path = asset_root.join("codex-login-managed-browser-opener.ts");
    fs::write(&opener_path, "#!/usr/bin/env node\nprocess.exit(0)\n")?;
    write_executable(&opener_path, &fs::read_to_string(&opener_path)?)?;

    // Fake codex binary
    let fake_codex = sandbox.join("fake-codex.mjs");
    fs::write(
        &fake_codex,
        r#"#!/usr/bin/env node
const fs = require('node:fs');
const process = require('node:process');

const log = (msg) => {
    if (process.env.RELOGIN_CALL_LOG) {
        fs.appendFileSync(process.env.RELOGIN_CALL_LOG, `${msg}\n`);
    }
};
let realCodexHome = process.env.CODEX_HOME;
try { realCodexHome = fs.realpathSync(realCodexHome); } catch(e) {}
log(`CALLED with args: ${process.argv.slice(2).join(' ')} (CODEX_HOME=${realCodexHome})`);

if (process.argv.includes('app-server')) {
    let buffer = '';
    function send(message) { process.stdout.write(JSON.stringify(message) + '\n'); }

    process.stdin.setEncoding('utf8');
    process.stdin.on('end', () => process.exit(0));
    process.stdin.on('data', (chunk) => {
      buffer += chunk;
      while (true) {
        const newlineIndex = buffer.indexOf('\n');
        if (newlineIndex === -1) break;
        const line = buffer.slice(0, newlineIndex).trim();
        buffer = buffer.slice(newlineIndex + 1);
        if (!line) continue;
        try {
            const message = JSON.parse(line);
            if (message.method === 'initialize') {
                send({ id: message.id, result: { userAgent: 'fake', codexHome: realCodexHome || '/tmp', platformFamily: 'unix', platformOs: 'macos' } });
            } else if (message.method === 'account/login/start') {
                log(`RPC account/login/start (CODEX_HOME=${realCodexHome})`);
                send({ id: message.id, result: { type: 'chatgpt', loginId: 'login-123', authUrl: 'http://localhost:1455/auth/callback?redirect_uri=http://localhost:1455/callback' } });
                setTimeout(() => send({ jsonrpc: '2.0', method: 'account/login/completed', params: { success: true, loginId: 'login-123', error: null } }), 25);
            } else if (message.method === 'account/login/cancel') {
                send({ id: message.id, result: { status: 'canceled' } });
            } else if (message.method === 'thread/read') {
                log(`RPC ${message.method}`);
                send({ id: message.id, result: [] });
            } else if (message.method === 'thread/start' || message.method === 'thread/injectItems' || message.method === 'turn/start') {
                log(`RPC ${message.method}`);
                send({ id: message.id, result: {} });
            }
        } catch (e) {}
      }
    });
} else if (process.argv.includes('login')) {
    log('CLI login called');
} else if (process.argv.includes('logout')) {
    log('CLI logout called');
}
"#,
    )?;
    write_executable(&fake_codex, &fs::read_to_string(&fake_codex)?)?;

    // Fake automation-bridge
    let fake_bridge = sandbox.join("fake-bridge.mjs");
    fs::write(
        &fake_bridge,
        r#"#!/usr/bin/env node
import fs from 'node:fs';
import process from 'node:process';
import { execSync } from 'node:child_process';

const args = process.argv.slice(2);
const requestFileIndex = args.indexOf('--request-file');
if (requestFileIndex !== -1) {
    const requestFile = args[requestFileIndex + 1];
    const request = JSON.parse(fs.readFileSync(requestFile, 'utf8'));
    if (request.command === 'prepare-account-secret-ref') {
        process.stdout.write(JSON.stringify({
            ok: true,
            result: { type: "bitwarden", store: "test", object_id: "test-id" }
        }) + '\n');
    } else if (request.command === 'complete-codex-login-attempt') {
        if (request.payload?.options?.codexBin) {
            execSync(`"${request.payload.options.codexBin}" login`, { stdio: 'inherit', env: process.env });
        }
        process.stdout.write(JSON.stringify({
            ok: true,
            result: {
                output: { success: true, callback_complete: true },
                finalUrl: "http://localhost:1455/callback",
                page: { url: "http://localhost:1455/callback" }
            }
        }) + '\n');
    } else {
        process.stdout.write(JSON.stringify({ ok: true, result: {} }) + '\n');
    }
}
"#,
    )?;
    write_executable(&fake_bridge, &fs::read_to_string(&fake_bridge)?)?;

    let fast_browser_runtime = sandbox.join("fast-browser-runtime.sh");
    write_executable(
        &fast_browser_runtime,
        "#!/bin/sh\nset -eu\nif [ \"${2-}\" = \"profiles\" ] && [ \"${3-}\" = \"inspect\" ]; then\n  printf '%s\\n' '{\"abiVersion\":\"1.0.0\",\"command\":\"profiles.inspect\",\"ok\":true,\"result\":{\"managedProfiles\":{\"default\":\"dev-1\",\"profiles\":[{\"name\":\"dev-1\"},{\"name\":\"managed-dev-1\"}]}}}'\n  exit 0\nfi\nprintf 'unexpected fast-browser runtime args: %s\\n' \"$*\" >&2\nexit 1\n",
    )?;

    // Create a dummy workflow file
    let workflow_dir = sandbox.join(".fast-browser").join("workflows").join("web").join("auth.openai.com");
    fs::create_dir_all(&workflow_dir)?;
    let workflow_file = workflow_dir.join("codex-rotate-account-flow-main.yaml");
    fs::copy(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../../.fast-browser/workflows/web/auth.openai.com/codex-rotate-account-flow-main.yaml"),
        &workflow_file
    )?;

    // We must provide the correct environment to the WatchTriggerHarness.
    // The harness uses the current process environment variables inside resolve_paths().
    // We set them for the current test.
    let _env_guard = env_mutex()
        .lock()
        .unwrap_or_else(|e| e.into_inner());

    let source_usage_body = serde_json::json!({
            "user_id": source_account.account_id.clone(),
            "account_id": source_account.account_id.clone(),
            "email": source_account.email.clone(),
            "plan_type": source_account.plan_type.clone(),
            "rate_limit": {
                "allowed": true,
                "limit_reached": true,
                "primary_window": {
                    "used_percent": 100.0,
                    "limit_window_seconds": 10800,
                    "reset_after_seconds": 3600,
                    "reset_at": 1729600000
                },
                "secondary_window": null
            },
            "code_review_rate_limit": null,
            "additional_rate_limits": null,
            "credits": null,
            "promo": null
        }).to_string();
    let target_usage_body = serde_json::json!({
            "user_id": target_account.account_id.clone(),
            "account_id": target_account.account_id.clone(),
            "email": target_account.email.clone(),
            "plan_type": target_account.plan_type.clone(),
            "rate_limit": {
                "allowed": true,
                "limit_reached": false,
                "primary_window": {
                    "used_percent": 25.0,
                    "limit_window_seconds": 10800,
                    "reset_after_seconds": 3600,
                    "reset_at": 1729600000
                },
                "secondary_window": null
            },
            "code_review_rate_limit": null,
            "additional_rate_limits": null,
            "credits": null,
            "promo": null
        }).to_string();

    let (usage_url, _usage_handle) = spawn_usage_server(
        source_account.account_id.clone(),
        source_usage_body,
        target_account.account_id.clone(),
        target_usage_body,
    );

    let sandbox_canonical = sandbox.canonicalize()?;
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
        std::env::set_var("CODEX_HOME", &codex_home);
        std::env::set_var("CODEX_ROTATE_CODEX_BIN", &fake_codex);
        std::env::set_var("CODEX_ROTATE_ENVIRONMENT", "host");
        std::env::set_var("CODEX_ROTATE_REPO_ROOT", &sandbox_canonical);
        std::env::set_var("CODEX_ROTATE_ASSET_ROOT", &asset_root);
        std::env::set_var("CODEX_ROTATE_AUTOMATION_BRIDGE", &fake_bridge);
        std::env::set_var("CODEX_ROTATE_ACCOUNT_FLOW_FILE", &workflow_file);
        std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", &usage_url);
        std::env::set_var("CODEX_ROTATE_FAST_BROWSER_RUNTIME", &fast_browser_runtime);
    }

    eprintln!("watch_trigger_e2e: creating harness");
    let harness = WatchTriggerHarness::new();

    // Ensure watch state has autoCreateEnabled so that rotation triggers a create/next
    eprintln!("watch_trigger_e2e: reading watch state");
    let mut watch_state = harness.read_watch_state()?;
    eprintln!("watch_trigger_e2e: updating watch state");
    watch_state.auto_create_enabled = true;
    let mut account_state = codex_rotate_runtime::watch::AccountWatchState::default();
    account_state.last_signal_id = Some(0);
    watch_state.set_account_state(source_account.account_id.clone(), account_state);
    eprintln!("watch_trigger_e2e: writing watch state");
    harness.write_watch_state(&watch_state)?;

    // Trigger usage limit signal for the active account
    eprintln!("watch_trigger_e2e: clearing signals");
    harness.clear_signals()?;
    eprintln!("watch_trigger_e2e: inserting usage signal");
    harness.insert_usage_limit_signal(1, 1000)?;

    println!("starting watch trigger iteration");
    let result = harness.trigger_now()?;
    println!("finished watch trigger iteration");
    println!("Watch iteration result: {:#?}", result);
    
    // Assert rotation happened successfully
    assert!(result.rotated, "Watch trigger should have performed a rotation.");
    assert_eq!(result.current_account_id, target_account.account_id);
    
    // Verify pool active index updated
    let pool = load_pool()?;
    assert_eq!(pool.active_index, 1);
    
    // Verify symlink updated to target persona
    let current_home_link = fs::read_link(&codex_home).context("read codex_home symlink")?;
    let target_persona = target_account.persona.as_ref().unwrap();
    let expected_home = rotate_home.join(target_persona.host_root_rel_path.as_ref().unwrap()).join("codex-home");
    assert_eq!(current_home_link, expected_home);

    // Verify auth was saved for the target persona
    let target_auth_file = expected_home.join("auth.json");
    assert!(target_auth_file.exists());

    // Clean up environment
    unsafe {
        std::env::remove_var("CODEX_ROTATE_HOME");
        std::env::remove_var("CODEX_HOME");
        std::env::remove_var("CODEX_ROTATE_CODEX_BIN");
        std::env::remove_var("CODEX_ROTATE_ENVIRONMENT");
        std::env::remove_var("CODEX_ROTATE_REPO_ROOT");
        std::env::remove_var("CODEX_ROTATE_ASSET_ROOT");
        std::env::remove_var("CODEX_ROTATE_AUTOMATION_BRIDGE");
        std::env::remove_var("CODEX_ROTATE_ACCOUNT_FLOW_FILE");
        std::env::remove_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");
        std::env::remove_var("CODEX_ROTATE_FAST_BROWSER_RUNTIME");
    }

    Ok(())
}
#[test]
fn hermetic_watch_triggered_host_rotation_with_activation_failure() -> Result<()> {
    let fixture = IsolatedAccountStateFixture::builder("watch-host")?
        .active_index(0)
        .build()?;

    let sandbox = fixture.sandbox_root().to_path_buf();
    let rotate_home = fixture.rotate_home().to_path_buf();
    let codex_home = fixture.codex_home().to_path_buf();
    let accounts = fixture.accounts();
    let source_account = &accounts[0];
    let target_account = &accounts[1];

    // Prepare assets
    let asset_root = sandbox.join("assets");
    fs::create_dir_all(&asset_root)?;
    let opener_path = asset_root.join("codex-login-managed-browser-opener.ts");
    fs::write(&opener_path, "#!/usr/bin/env node\nprocess.exit(0)\n")?;
    write_executable(&opener_path, &fs::read_to_string(&opener_path)?)?;

    // Fake codex binary
    let fake_codex = sandbox.join("fake-codex.mjs");
    fs::write(
        &fake_codex,
        r#"#!/usr/bin/env node
const fs = require('node:fs');
const process = require('node:process');

const log = (msg) => {
    if (process.env.RELOGIN_CALL_LOG) {
        fs.appendFileSync(process.env.RELOGIN_CALL_LOG, `${msg}\n`);
    }
};
let realCodexHome = process.env.CODEX_HOME;
try { realCodexHome = fs.realpathSync(realCodexHome); } catch(e) {}
log(`CALLED with args: ${process.argv.slice(2).join(' ')} (CODEX_HOME=${realCodexHome})`);

if (process.argv.includes('app-server')) {
    let buffer = '';
    function send(message) { process.stdout.write(JSON.stringify(message) + '\n'); }

    process.stdin.setEncoding('utf8');
    process.stdin.on('end', () => process.exit(0));
    process.stdin.on('data', (chunk) => {
      buffer += chunk;
      while (true) {
        const newlineIndex = buffer.indexOf('\n');
        if (newlineIndex === -1) break;
        const line = buffer.slice(0, newlineIndex).trim();
        buffer = buffer.slice(newlineIndex + 1);
        if (!line) continue;
        try {
            const message = JSON.parse(line);
            if (message.method === 'initialize') {
                send({ id: message.id, result: { userAgent: 'fake', codexHome: realCodexHome || '/tmp', platformFamily: 'unix', platformOs: 'macos' } });
            } else if (message.method === 'account/login/start') {
                log(`RPC account/login/start (CODEX_HOME=${realCodexHome})`);
                send({ id: message.id, result: { type: 'chatgpt', loginId: 'login-123', authUrl: 'http://localhost:1455/auth/callback?redirect_uri=http://localhost:1455/callback' } });
                setTimeout(() => send({ jsonrpc: '2.0', method: 'account/login/completed', params: { success: true, loginId: 'login-123', error: null } }), 25);
            } else if (message.method === 'account/login/cancel') {
                send({ id: message.id, result: { status: 'canceled' } });
            } else if (message.method === 'thread/read') {
                log(`RPC ${message.method}`);
                send({ id: message.id, result: [] });
            } else if (message.method === 'thread/start' || message.method === 'thread/injectItems' || message.method === 'turn/start') {
                log(`RPC ${message.method}`);
                send({ id: message.id, result: {} });
            }
        } catch (e) {}
      }
    });
} else if (process.argv.includes('login')) {
    log('CLI login called');
} else if (process.argv.includes('logout')) {
    log('CLI logout called');
}
"#,
    )?;
    write_executable(&fake_codex, &fs::read_to_string(&fake_codex)?)?;

    // Fake automation-bridge
    let fake_bridge = sandbox.join("fake-bridge.mjs");
    fs::write(
        &fake_bridge,
        r#"#!/usr/bin/env node
import fs from 'node:fs';
import process from 'node:process';
import { execSync } from 'node:child_process';

const args = process.argv.slice(2);
const requestFileIndex = args.indexOf('--request-file');
if (requestFileIndex !== -1) {
    const requestFile = args[requestFileIndex + 1];
    const request = JSON.parse(fs.readFileSync(requestFile, 'utf8'));
    if (request.command === 'prepare-account-secret-ref') {
        process.stdout.write(JSON.stringify({
            ok: true,
            result: { type: "bitwarden", store: "test", object_id: "test-id" }
        }) + '\n');
    } else if (request.command === 'complete-codex-login-attempt') {
        if (request.payload?.options?.codexBin) {
            execSync(`"${request.payload.options.codexBin}" login`, { stdio: 'inherit', env: process.env });
        }
        process.stdout.write(JSON.stringify({
            ok: true,
            result: {
                output: { success: true, callback_complete: true },
                finalUrl: "http://localhost:1455/callback",
                page: { url: "http://localhost:1455/callback" }
            }
        }) + '\n');
    } else {
        process.stdout.write(JSON.stringify({ ok: true, result: {} }) + '\n');
    }
}
"#,
    )?;
    write_executable(&fake_bridge, &fs::read_to_string(&fake_bridge)?)?;

    let fast_browser_runtime = sandbox.join("fast-browser-runtime.sh");
    write_executable(
        &fast_browser_runtime,
        "#!/bin/sh\nset -eu\nif [ \"${2-}\" = \"profiles\" ] && [ \"${3-}\" = \"inspect\" ]; then\n  printf '%s\\n' '{\"abiVersion\":\"1.0.0\",\"command\":\"profiles.inspect\",\"ok\":true,\"result\":{\"managedProfiles\":{\"default\":\"dev-1\",\"profiles\":[{\"name\":\"dev-1\"},{\"name\":\"managed-dev-1\"}]}}}'\n  exit 0\nfi\nprintf 'unexpected fast-browser runtime args: %s\\n' \"$*\" >&2\nexit 1\n",
    )?;

    // Create a dummy workflow file
    let workflow_dir = sandbox.join(".fast-browser").join("workflows").join("web").join("auth.openai.com");
    fs::create_dir_all(&workflow_dir)?;
    let workflow_file = workflow_dir.join("codex-rotate-account-flow-main.yaml");
    fs::copy(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../../.fast-browser/workflows/web/auth.openai.com/codex-rotate-account-flow-main.yaml"),
        &workflow_file
    )?;

    // We must provide the correct environment to the WatchTriggerHarness.
    // The harness uses the current process environment variables inside resolve_paths().
    // We set them for the current test.
    let _env_guard = env_mutex()
        .lock()
        .unwrap_or_else(|e| e.into_inner());

    let source_usage_body = serde_json::json!({
            "user_id": source_account.account_id.clone(),
            "account_id": source_account.account_id.clone(),
            "email": source_account.email.clone(),
            "plan_type": source_account.plan_type.clone(),
            "rate_limit": {
                "allowed": true,
                "limit_reached": true,
                "primary_window": {
                    "used_percent": 100.0,
                    "limit_window_seconds": 10800,
                    "reset_after_seconds": 3600,
                    "reset_at": 1729600000
                },
                "secondary_window": null
            },
            "code_review_rate_limit": null,
            "additional_rate_limits": null,
            "credits": null,
            "promo": null
        }).to_string();
    let target_usage_body = serde_json::json!({
            "user_id": target_account.account_id.clone(),
            "account_id": target_account.account_id.clone(),
            "email": target_account.email.clone(),
            "plan_type": target_account.plan_type.clone(),
            "rate_limit": {
                "allowed": true,
                "limit_reached": false,
                "primary_window": {
                    "used_percent": 25.0,
                    "limit_window_seconds": 10800,
                    "reset_after_seconds": 3600,
                    "reset_at": 1729600000
                },
                "secondary_window": null
            },
            "code_review_rate_limit": null,
            "additional_rate_limits": null,
            "credits": null,
            "promo": null
        }).to_string();

    let (usage_url, _usage_handle) = spawn_usage_server(
        source_account.account_id.clone(),
        source_usage_body,
        target_account.account_id.clone(),
        target_usage_body,
    );

    let sandbox_canonical = sandbox.canonicalize()?;
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
        std::env::set_var("CODEX_HOME", &codex_home);
        std::env::set_var("CODEX_ROTATE_CODEX_BIN", &fake_codex);
        std::env::set_var("CODEX_ROTATE_ENVIRONMENT", "host");
        std::env::set_var("CODEX_ROTATE_REPO_ROOT", &sandbox_canonical);
        std::env::set_var("CODEX_ROTATE_ASSET_ROOT", &asset_root);
        std::env::set_var("CODEX_ROTATE_AUTOMATION_BRIDGE", &fake_bridge);
        std::env::set_var("CODEX_ROTATE_ACCOUNT_FLOW_FILE", &workflow_file);
        std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", &usage_url);
        std::env::set_var("CODEX_ROTATE_FAST_BROWSER_RUNTIME", &fast_browser_runtime);
    }

    eprintln!("watch_trigger_e2e: creating harness");
    let harness = WatchTriggerHarness::new();

    // Ensure watch state has autoCreateEnabled so that rotation triggers a create/next
    eprintln!("watch_trigger_e2e: reading watch state");
    let mut watch_state = harness.read_watch_state()?;
    eprintln!("watch_trigger_e2e: updating watch state");
    watch_state.auto_create_enabled = true;
    let mut account_state = codex_rotate_runtime::watch::AccountWatchState::default();
    account_state.last_signal_id = Some(0);
    watch_state.set_account_state(source_account.account_id.clone(), account_state);
    eprintln!("watch_trigger_e2e: writing watch state");
    harness.write_watch_state(&watch_state)?;

    // Trigger usage limit signal for the active account
    eprintln!("watch_trigger_e2e: clearing signals");
    harness.clear_signals()?;
    eprintln!("watch_trigger_e2e: inserting usage signal");
    harness.insert_usage_limit_signal(1, 1000)?;


    // INJECT FAILURE: Make target codex-home read-only so activation fails
    let target_persona = target_account.persona.as_ref().unwrap();
    let expected_home = rotate_home.join(target_persona.host_root_rel_path.as_ref().unwrap()).join("codex-home");
    fs::create_dir_all(&expected_home)?;
    let mut perms = fs::metadata(&expected_home)?.permissions();
    perms.set_readonly(true);
    fs::set_permissions(&expected_home, perms)?;

    let result = harness.trigger_now();
    assert!(result.is_err(), "Watch trigger should have failed during activation.");
    let err_msg = format!("{:#}", result.unwrap_err());
    assert!(err_msg.contains("Permission denied") || err_msg.contains("Read-only file system"), "Unexpected error: {}", err_msg);

    // Verify rollback semantics
    let pool = load_pool()?;
    assert_eq!(pool.active_index, 0);
    let current_home_link = fs::read_link(&codex_home).context("read codex_home symlink")?;
    let source_persona = source_account.persona.as_ref().unwrap();
    let source_home = rotate_home.join(source_persona.host_root_rel_path.as_ref().unwrap()).join("codex-home");
    assert_eq!(current_home_link, source_home);

    // Restore permissions so cleanup can succeed
    let mut perms = fs::metadata(&expected_home)?.permissions();
    perms.set_readonly(false);
    fs::set_permissions(&expected_home, perms)?;


    // Clean up environment
    unsafe {
        std::env::remove_var("CODEX_ROTATE_HOME");
        std::env::remove_var("CODEX_HOME");
        std::env::remove_var("CODEX_ROTATE_CODEX_BIN");
        std::env::remove_var("CODEX_ROTATE_ENVIRONMENT");
        std::env::remove_var("CODEX_ROTATE_REPO_ROOT");
        std::env::remove_var("CODEX_ROTATE_ASSET_ROOT");
        std::env::remove_var("CODEX_ROTATE_AUTOMATION_BRIDGE");
        std::env::remove_var("CODEX_ROTATE_ACCOUNT_FLOW_FILE");
        std::env::remove_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");
        std::env::remove_var("CODEX_ROTATE_FAST_BROWSER_RUNTIME");
    }

    Ok(())
}
