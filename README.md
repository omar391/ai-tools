# ai-tools

A Bun workspace monorepo of CLI utilities for managing AI coding assistants.

## Packages

### [`codex-rotate`](packages/codex-rotate/)

Rotate Codex CLI OAuth tokens across multiple ChatGPT accounts.

```sh
codex-rotate add              # Snapshot current ~/.codex/auth.json into pool using email_plan as the key
codex-rotate add work         # Same, but also keep "work" as an optional alias
codex-rotate create           # Resume the oldest unfinished Gmail+N account, or create a new one, then switch to it
codex-rotate next              # Swap to next account with usable quota
codex-rotate prev              # Swap to previous account
codex-rotate list              # Show all accounts with live quota info
codex-rotate status            # Show current auth details and quota
codex-rotate relogin <selector> # Repair a dead entry; stored credentials are used automatically when available
codex-rotate remove <selector>  # Remove account from pool
```

`add` now defaults the pool key to `{email}_{plan-type}`. Old manual labels are preserved as optional aliases, and `relogin` / `remove` accept either the composite key, the alias, the full account id, the short account id shown in `list`, or the email when it is unique in the pool.

`create` and automated `relogin` use the shared fast-browser managed Chrome profiles plus the browser URL emitted by `BROWSER=/usr/bin/false codex login`. New account passwords are stored in `~/.codex-rotate/credentials.json` with `0600` permissions. `create` now defaults to the local workflow `preferred_profile` (`dev-1`), discovers the Gmail base address from that profile unless you override it with `--profile` or `--base-email`, and drains the oldest unfinished pending alias before allocating a fresh `+N`. `next` now auto-creates one new account when the existing pool is fully exhausted or unavailable.

`relogin` now prefers stored credentials. Use `--manual-login` to force the legacy browser flow, `--device-auth` for device auth, `--keep-session` to skip `codex logout` for manual relogins, or `--allow-email-change` if you intentionally want to replace the selected entry with a different signed-in email.

**Setup:**

```sh
bun install

# Add alias to ~/.zshrc
alias codex-rotate='bun run /path/to/ai-tools/packages/codex-rotate/index.ts'
source ~/.zshrc
```

> **Note:** After running `codex-rotate next` or `prev`, **restart Codex** (re-open the IDE window) for it to pick up the new auth tokens. The rotation updates `~/.codex/auth.json` on disk, but the running Codex process caches the session in memory.

## Adding a new tool

Create a new package under `packages/`:

```text
packages/
  codex-rotate/
  my-new-tool/
    index.ts
    package.json
```

## Requirements

Any TypeScript-capable runtime: [Bun](https://bun.sh), [tsx](https://github.com/privatenumber/tsx), [Deno](https://deno.land), etc.
