#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};
use codex_rotate_core::pool::load_pool;
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

fn spawn_usage_server(body: String) -> (String, std::thread::JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind usage server");
    listener
        .set_nonblocking(true)
        .expect("set usage server nonblocking");
    let address = listener.local_addr().expect("usage server address");
    let handle = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if let Ok((mut stream, _)) = listener.accept() {
                use std::io::{Read, Write};

                let mut buf = [0; 1024];
                let _ = stream.read(&mut buf);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
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
fn hermetic_same_account_reopen_retains_native_persona_history() -> Result<()> {
    let fixture = IsolatedAccountStateFixture::builder("reopen-host")?
        .active_index(0)
        .build()?;

    let sandbox = fixture.sandbox_root().to_path_buf();
    let rotate_home = fixture.rotate_home().to_path_buf();
    let codex_home = fixture.codex_home().to_path_buf();
    let accounts = fixture.accounts();

    let (usage_url, _usage_handle) = spawn_usage_server(
        serde_json::json!({
            "user_id": accounts[0].account_id.clone(),
            "account_id": accounts[0].account_id.clone(),
            "email": accounts[0].email.clone(),
            "plan_type": accounts[0].plan_type.clone(),
            "rate_limit": {
                "allowed": true,
                "limit_reached": false,
                "primary_window": {
                    "used_percent": 0.0,
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
        })
        .to_string(),
    );

    // Seed the live codex auth file so next knows the previous active account
    fs::write(
        codex_home.join("auth.json"),
        serde_json::to_string(&accounts[0].auth)?,
    )?;

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
            } else if (message.method === 'thread/start' || message.method === 'thread/injectItems') {
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

    // Fake automation bridge
    let fake_bridge = sandbox.join("fake-bridge.mjs");
    fs::write(
        &fake_bridge,
        r#"#!/usr/bin/env node
import process from 'node:process';
process.stdout.write(JSON.stringify({ ok: true, result: {} }) + '\n');
"#,
    )?;
    write_executable(&fake_bridge, &fs::read_to_string(&fake_bridge)?)?;

    let call_log = sandbox.join("relogin-calls.log");
    let sandbox_canonical = sandbox.canonicalize()?;

    let run_cli = |cmd: &str| -> Result<()> {
        let result = Command::new(cli_binary())
            .arg(cmd)
            .env("CODEX_ROTATE_HOME", &rotate_home)
            .env("CODEX_HOME", &codex_home)
            .env("RELOGIN_CALL_LOG", &call_log)
            .env("CODEX_ROTATE_CODEX_BIN", &fake_codex)
            .env("CODEX_ROTATE_ENVIRONMENT", "host")
            .env("CODEX_ROTATE_REPO_ROOT", &sandbox_canonical)
            .env("CODEX_ROTATE_AUTOMATION_BRIDGE", &fake_bridge)
            .env("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", &usage_url)
            .env("CODEX_ROTATE_DISABLE_LOCAL_REFRESH", "1")
            .env("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH", "1")
            .output()?;

        if !result.status.success() {
            eprintln!(
                "{} stdout: {}",
                cmd,
                String::from_utf8_lossy(&result.stdout)
            );
            eprintln!(
                "{} stderr: {}",
                cmd,
                String::from_utf8_lossy(&result.stderr)
            );
            anyhow::bail!("{} failed", cmd);
        }
        Ok(())
    };

    // Step 1: Rotate to the next account (index 1)
    run_cli("next")?;

    // Verify pool active index updated to 1
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
    }
    let pool = load_pool()?;
    assert_eq!(pool.active_index, 1);

    // Write a dummy file to account 0's persona to simulate native history
    let original_persona = accounts[0].persona.as_ref().unwrap();
    let account_0_home = rotate_home
        .join(original_persona.host_root_rel_path.as_ref().unwrap())
        .join("codex-home");
    fs::create_dir_all(&account_0_home)?;
    fs::write(account_0_home.join("dummy-history.txt"), "history content")?;

    // Step 2: Rotate to the next account (index 0, assuming 2 accounts)
    run_cli("next")?;

    let pool = load_pool()?;
    assert_eq!(pool.active_index, 0);

    // Verify the dummy history file still exists and is accessible
    let current_home_link = fs::read_link(&codex_home).context("read codex_home symlink")?;
    assert_eq!(current_home_link, account_0_home);
    assert!(current_home_link.join("dummy-history.txt").exists());

    // Clean up environment
    unsafe {
        std::env::remove_var("CODEX_ROTATE_HOME");
    }

    Ok(())
}
