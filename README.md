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

`create` and automated `relogin` use the shared fast-browser managed Chrome profiles plus the auth URL emitted by `BROWSER=/usr/bin/false codex login`, so the regular system browser does not need to take over the OAuth handoff. Account inventory and credential metadata now live in `~/.codex-rotate/accounts.json`; the daemon-owned runtime state is `~/.codex-rotate/watch-state.json`, `~/.codex-rotate/profile/`, and `~/.codex-rotate/daemon.sock`.

The tray is only a UI shell over the daemon. The CLI owns the watch loop, managed Codex launch, live account sync, and create/relogin orchestration.

#### Setup

```sh
cargo build --package codex-rotate-cli

# Optional npm-style wrapper once the native binary is available
node /path/to/ai-tools/packages/codex-rotate/index.ts help
```

## Adding a new tool

Create a new package under `packages/`:

```text
packages/
  codex-rotate/
  my-new-tool/
    index.ts
    package.json
```
