#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{stamp}"))
}

#[test]
fn internal_managed_login_starts_codex_app_server_and_exits_on_completion() {
    let fixture_root = unique_temp_dir("codex-rotate-managed-login");
    fs::create_dir_all(&fixture_root).expect("create fixture root");
    let fake_codex_path = fixture_root.join("fake-codex.mjs");

    fs::write(
        &fake_codex_path,
        [
            "#!/usr/bin/env node",
            "import process from 'node:process';",
            "let buffer = '';",
            "function send(message) { process.stdout.write(JSON.stringify(message) + '\\n'); }",
            "process.stdin.setEncoding('utf8');",
            "process.stdin.on('data', (chunk) => {",
            "  buffer += chunk;",
            "  while (true) {",
            "    const newlineIndex = buffer.indexOf('\\n');",
            "    if (newlineIndex === -1) break;",
            "    const line = buffer.slice(0, newlineIndex).trim();",
            "    buffer = buffer.slice(newlineIndex + 1);",
            "    if (!line) continue;",
            "    const message = JSON.parse(line);",
            "    if (message.method === 'initialize') {",
            "      send({ id: message.id, result: { userAgent: 'fake', codexHome: '/tmp', platformFamily: 'unix', platformOs: 'macos' } });",
            "    } else if (message.method === 'account/login/start') {",
            "      send({ id: message.id, result: { type: 'chatgpt', loginId: 'login-123', authUrl: 'https://auth.openai.com/oauth/authorize?redirect_uri=' + encodeURIComponent('http://localhost:1455/auth/callback') } });",
            "      setTimeout(() => send({ jsonrpc: '2.0', method: 'account/login/completed', params: { success: true, loginId: 'login-123', error: null } }), 25);",
            "    } else if (message.method === 'account/login/cancel') {",
            "      send({ id: message.id, result: { status: 'canceled' } });",
            "    }",
            "  }",
            "});",
        ]
        .join("\n"),
    )
    .expect("write fake codex");
    let mut permissions = fs::metadata(&fake_codex_path)
        .expect("fake codex metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&fake_codex_path, permissions).expect("set fake codex permissions");

    let result = Command::new(env!("CARGO_BIN_EXE_codex-rotate"))
        .args(["internal", "managed-login"])
        .env("CODEX_ROTATE_REAL_CODEX", &fake_codex_path)
        .output()
        .expect("run managed login");

    fs::remove_dir_all(&fixture_root).ok();

    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("Starting local login server on http://localhost:1455."));
    assert!(stderr.contains("https://auth.openai.com/oauth/authorize?"));
    assert!(stderr.contains("Successfully logged in"));
}
