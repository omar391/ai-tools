# ai-tools

A Rust-first workspace of local tools for managing AI coding assistants.

## Packages

### [`codex-rotate`](packages/codex-rotate/)

Rotate Codex CLI OAuth tokens across multiple ChatGPT accounts.

```sh
codex-rotate add                # Snapshot current ~/.codex/auth.json into pool using email_plan as the key
codex-rotate add work           # Same, but also keep "work" as an optional alias
codex-rotate create             # Reuse a healthy account first; only create when needed, or force with --force
codex-rotate next               # Swap to next account with usable quota
codex-rotate prev               # Swap to previous account
codex-rotate list               # Show all accounts with live quota info
codex-rotate status             # Show current auth-file target details and quota
codex-rotate relogin <selector> # Repair a dead entry; stored credentials are used automatically when available
codex-rotate remove <selector>  # Remove account from pool
codex-rotate daemon run         # Start the background daemon used by the tray shell
```

`add` defaults the pool key to `{email}_{plan-type}`. Old manual labels are preserved as optional aliases, and `relogin` / `remove` accept either the composite key, the alias, the full account id, the short account id shown in `list`, or the email when it is unique in the pool.

`create` and automated `relogin` use the shared fast-browser managed Chrome profiles plus the auth URL emitted by `BROWSER=/usr/bin/false codex login`, so the regular system browser does not need to take over the OAuth handoff. Account inventory and credential metadata now live in `~/.codex-rotate/accounts.json`; the daemon-owned runtime state is `~/.codex-rotate/watch-state.json`, `~/.codex-rotate/profile/`, and `~/.codex-rotate/daemon.sock`. Managed login wrapper scripts are generated per checkout under `<repo-root>/.codex-rotate/bin/` instead of `~/.codex-rotate/bin/`.

The tray is only a UI shell over the daemon. The CLI owns the watch loop, managed Codex launch, live account sync, and create/relogin orchestration.

#### Setup

```sh
cargo build --package codex-rotate-cli

# Optional npm-style wrapper once the native binary is available
node /path/to/ai-tools/packages/codex-rotate/index.ts help
```

## Proxy Rnd

The proxy audit script and architecture notes now live under
`packages/codex-rotate/crates/codex-rotate-core/proxy-rnd/`.

Use [`packages/codex-rotate/crates/codex-rotate-core/proxy-rnd/codex_proxy_audit.py`](packages/codex-rotate/crates/codex-rotate-core/proxy-rnd/codex_proxy_audit.py) to audit three things in one run:

- whether public echo services see metadata added by your proxy
- whether a Codex CLI command keeps all network traffic pinned to the proxy
- whether Codex Desktop and its helper processes keep all network traffic pinned to the proxy

Recommended invocation:

```sh
python3 packages/codex-rotate/crates/codex-rotate-core/proxy-rnd/codex_proxy_audit.py \
  --proxy-url 'socks5h://127.0.0.1:1080' \
  --cli-command 'codex exec "Reply exactly: proxy-audit-ok"'
```

Useful flags:

- `--skip-app`: only run the metadata and CLI checks
- `--skip-cli`: only run the metadata and desktop-app checks
- `--no-direct-baseline`: skip non-proxied baseline calls to public echo services
- `--leave-app-open`: keep the launched Codex app instance running after the audit
- `--output-json /path/to/report.json`: choose where the JSON report is written

What the script flags:

- public services seeing headers such as `Via`, `X-Forwarded-For`, `Forwarded`, or `X-Real-IP`
- Codex CLI or Codex Desktop processes making direct remote TCP/UDP connections instead of connecting only to the proxy or loopback
- app helper processes that bypass the proxy even if the main Electron window is configured correctly

Notes:

- close any existing Codex Desktop windows before running the app phase if you want the cleanest result
- the default CLI command performs a real authenticated Codex request, so it will consume normal usage
- for env-based clients the script uses `socks5h://` so hostname resolution happens through the proxy

## Adding a new tool

Create a new package under `packages/`:

```text
packages/
  codex-rotate/
  my-new-tool/
    index.ts
    package.json
```
