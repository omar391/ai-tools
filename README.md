# ai-tools

A Bun workspace monorepo of CLI utilities for managing AI coding assistants.

## Packages

### [`codex-rotate`](packages/codex-rotate/)

Rotate Codex CLI OAuth tokens across multiple ChatGPT accounts.

```sh
codex-rotate add <label>      # Snapshot current ~/.codex/auth.json into pool
codex-rotate next              # Swap to next account (round-robin)
codex-rotate prev              # Swap to previous account
codex-rotate list              # Show all accounts with active marker
codex-rotate status            # Show current auth details
codex-rotate remove <label>    # Remove account from pool
```

**Setup:**

```sh
bun install

# Add alias to ~/.zshrc
alias codex-rotate='bun run /path/to/ai-tools/packages/codex-rotate/index.ts'
source ~/.zshrc
```

## Adding a new tool

Create a new package under `packages/`:

```
packages/
  codex-rotate/
  my-new-tool/
    index.ts
    package.json
```

## Requirements

- [Bun](https://bun.sh) v1.0+
