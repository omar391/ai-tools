#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use codex_rotate_test_support::IsolatedAccountStateFixture;

fn cli_binary() -> &'static str {
    env!("CARGO_BIN_EXE_codex-rotate")
}

fn write_executable(path: &Path, contents: &str) -> Result<()> {
    fs::write(path, contents)?;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

#[test]
fn hermetic_host_relogin_switches_persona_and_applies_auth() -> Result<()> {
    let fixture = IsolatedAccountStateFixture::builder("relogin-host")?
        .active_index(0)
        .build()?;

    let sandbox = fixture.sandbox_root().to_path_buf();
    let rotate_home = fixture.rotate_home().to_path_buf();
    let codex_home = fixture.codex_home().to_path_buf();
    let accounts = fixture.accounts();
    let target_account = &accounts[1];

    // Prepare assets
    let asset_root = sandbox.join("assets");
    fs::create_dir_all(&asset_root)?;
    let opener_path = asset_root.join("codex-login-managed-browser-opener.ts");
    let browser_shim_log = sandbox.join("browser-shim.log.jsonl");
    fs::write(&browser_shim_log, "")?;
    fs::write(
        &opener_path,
        r#"#!/usr/bin/env node
import fs from 'node:fs';
import process from 'node:process';

const logPath = process.env.CODEX_ROTATE_BROWSER_SHIM_LOG;
const argv = process.argv.slice(2);
const url = argv.find((value) => /^https?:\/\//i.test(String(value).trim())) ?? null;
const event = {
    event: 'browser_shim_invoked',
    profile: process.env.FAST_BROWSER_PROFILE || null,
    browser: process.env.BROWSER || null,
    url,
    argv,
};
if (logPath) {
    fs.appendFileSync(logPath, `${JSON.stringify(event)}\n`);
}
if (!url) {
    process.stderr.write('Managed browser opener received a non-URL launch request.\n');
    process.exit(1);
}
if (logPath) {
    fs.appendFileSync(
        logPath,
        `${JSON.stringify({
            event: 'browser_shim_opened_url',
            profile: event.profile,
            browser: event.browser,
            url,
        })}\n`,
    );
}
process.stdout.write(JSON.stringify({ ok: true, profile: event.profile, browser: event.browser, url }) + '\n');
"#,
    )?;
    write_executable(&opener_path, &fs::read_to_string(&opener_path)?)?;

    let fake_open_bin = sandbox.join("fake-open-bin");
    fs::create_dir_all(&fake_open_bin)?;
    let system_open_log = sandbox.join("system-open.log");
    fs::write(&system_open_log, "")?;
    let trap_script = format!(
        "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$*\" >> '{}'\nexit 91\n",
        system_open_log.display()
    );
    write_executable(&fake_open_bin.join("open"), &trap_script)?;
    write_executable(&fake_open_bin.join("xdg-open"), &trap_script)?;

    let call_log = sandbox.join("relogin-calls.log");
    
    // Update accounts.json (CredentialStore) to include families and domain state
    let mut state_json = fixture.read_state()?;
    if let serde_json::Value::Object(ref mut map) = state_json {
        map.insert("version".to_string(), serde_json::json!(9));
        map.insert("families".to_string(), serde_json::json!({
            "default:dev.{n}@astronlab.com": {
                "profile_name": "default",
                "template": "dev.{n}@astronlab.com",
                "next_suffix": 3,
                "created_at": "2026-04-07T00:00:00.000Z",
                "updated_at": "2026-04-07T00:00:00.000Z"
            }
        }));
        map.insert("domain".to_string(), serde_json::json!({
            "astronlab.com": {
                "rotation_enabled": true
            }
        }));
    }
    fs::write(fixture.state_path(), serde_json::to_string(&state_json)?)?;

    // Seed the live codex auth file so relogin knows the previous active account
    fs::write(
        codex_home.join("auth.json"),
        serde_json::to_string(&accounts[0].auth)?,
    )?;

    // Create a fake codex binary that implements the app-server JSON-RPC
    let fake_codex = sandbox.join("fake-codex.mjs");
    fs::write(
        &fake_codex,
        r#"#!/usr/bin/env node
import { execFileSync } from 'node:child_process';
import process from 'node:process';
import fs from 'node:fs';

const log = (msg) => fs.appendFileSync(process.env.RELOGIN_CALL_LOG, `${msg}\n`);
let realCodexHome = process.env.CODEX_HOME;
try { realCodexHome = fs.realpathSync(realCodexHome); } catch(e) {}
log(`CALLED with args: ${process.argv.slice(2).join(' ')} (CODEX_HOME=${realCodexHome})`);

if (process.argv.includes('app-server')) {
    let buffer = '';
    function send(message) { process.stdout.write(JSON.stringify(message) + '\n'); }

    process.stdin.setEncoding('utf8');
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
                const authUrl = 'http://localhost:1455/auth/callback?redirect_uri=http://localhost:1455/callback';
                log(`RPC account/login/start (CODEX_HOME=${realCodexHome})`);
                send({ id: message.id, result: { type: 'chatgpt', loginId: 'login-123', authUrl } });
                execFileSync('open', [authUrl], { stdio: 'inherit', env: process.env });
                setTimeout(() => send({ jsonrpc: '2.0', method: 'account/login/completed', params: { success: true, loginId: 'login-123', error: null } }), 25);
            } else if (message.method === 'account/login/cancel') {
                send({ id: message.id, result: { status: 'canceled' } });
            }
        } catch (e) {}
      }
    });
} else if (process.argv.includes('login')) {
    log('CLI login called');
    execFileSync(
        'open',
        ['https://auth.openai.com/oauth/authorize?state=relogin-no-system-browser'],
        { stdio: 'inherit', env: process.env },
    );
}
"#,
    )?;
    write_executable(&fake_codex, &fs::read_to_string(&fake_codex)?)?;
    
    // We need to mock automation-bridge for relogin if it uses it.
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

    // Create a dummy workflow file
    let workflow_dir = sandbox.join(".fast-browser").join("workflows").join("web").join("auth.openai.com");
    fs::create_dir_all(&workflow_dir)?;
    let workflow_file = workflow_dir.join("codex-rotate-account-flow-main.yaml");
    fs::copy(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../../.fast-browser/workflows/web/auth.openai.com/codex-rotate-account-flow-main.yaml"),
        &workflow_file
    )?;

    let sandbox_canonical = sandbox.canonicalize()?;
    let result = Command::new(cli_binary())
        .arg("relogin")
        .arg(&target_account.account_id)
        .env("CODEX_ROTATE_HOME", &rotate_home)
        .env("CODEX_HOME", &codex_home)
        .env("RELOGIN_CALL_LOG", &call_log)
        .env("CODEX_ROTATE_CODEX_BIN", &fake_codex)
        .env("CODEX_ROTATE_ENVIRONMENT", "host")
        .env("CODEX_ROTATE_REPO_ROOT", &sandbox_canonical)
        .env("CODEX_ROTATE_ASSET_ROOT", &asset_root)
        .env("CODEX_ROTATE_CLI_BIN", cli_binary())
        .env("CODEX_ROTATE_AUTOMATION_BRIDGE", &fake_bridge)
        .env("CODEX_ROTATE_ACCOUNT_FLOW_FILE", &workflow_file)
        .env(
            "CODEX_ROTATE_BROWSER_SHIM_LOG",
            &browser_shim_log,
        )
        .env("PATH", format!(
            "{}:{}",
            fake_open_bin.display(),
            std::env::var_os("PATH").unwrap_or_default().to_string_lossy()
        ))
        .output()?;

    if !result.status.success() {
        eprintln!("relogin stdout: {}", String::from_utf8_lossy(&result.stdout));
        eprintln!("relogin stderr: {}", String::from_utf8_lossy(&result.stderr));
    }
    assert!(result.status.success(), "relogin failed: {}", String::from_utf8_lossy(&result.stderr));

    // Verify that the fake codex was called with the target persona's CODEX_HOME
    println!("Checking call log at {}", call_log.display());
    let calls = fs::read_to_string(&call_log).context("read call log")?;
    let target_persona = target_account.persona.as_ref().unwrap();
    let expected_home = rotate_home.join(&target_persona.host_root_rel_path.as_ref().unwrap()).join("codex-home");
    
    assert!(calls.contains(&expected_home.to_string_lossy().to_string()), 
        "Expected call with {}, but got:\n{}", expected_home.display(), calls);

    let browser_shim_log_contents =
        fs::read_to_string(&browser_shim_log).context("read browser shim log")?;
    assert!(
        browser_shim_log_contents.contains("\"event\":\"browser_shim_invoked\""),
        "expected managed browser opener to be invoked, log was:\n{}",
        browser_shim_log_contents
    );
    assert!(
        browser_shim_log_contents.contains(&opener_path.to_string_lossy().to_string()),
        "expected opener path to be used, log was:\n{}",
        browser_shim_log_contents
    );
    assert!(
        browser_shim_log_contents.contains("http://localhost:1455/auth/callback?redirect_uri=http://localhost:1455/callback"),
        "expected opener URL to be routed through the managed browser shim, log was:\n{}",
        browser_shim_log_contents
    );

    let system_open_calls = fs::read_to_string(&system_open_log).context("read system open log")?;
    assert!(
        system_open_calls.trim().is_empty(),
        "system browser fallback should not be used, log was:\n{}",
        system_open_calls
    );

    // Verify that after relogin, the active persona is restored to the original one (index 0)
    let state = fixture.read_state()?;
    assert_eq!(state["active_index"], 0);
    
    // Check live symlink
    println!("Checking symlink at {}", codex_home.display());
    let current_home_link = fs::read_link(&codex_home).context("read codex_home symlink")?;
    let original_persona = accounts[0].persona.as_ref().unwrap();
    let original_expected_home = rotate_home.join(original_persona.host_root_rel_path.as_ref().unwrap()).join("codex-home");
    assert_eq!(current_home_link, original_expected_home);

    Ok(())
}
