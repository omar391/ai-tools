#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <mounted-guest-root>" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

if [[ -n "${CODEX_ROTATE_CLI_BIN:-}" && -x "${CODEX_ROTATE_CLI_BIN}" ]]; then
  exec "${CODEX_ROTATE_CLI_BIN}" internal vm-bootstrap "$@"
fi

if [[ -x "$REPO_ROOT/target/debug/codex-rotate" ]]; then
  exec "$REPO_ROOT/target/debug/codex-rotate" internal vm-bootstrap "$@"
fi

if [[ -x "$REPO_ROOT/target/release/codex-rotate" ]]; then
  exec "$REPO_ROOT/target/release/codex-rotate" internal vm-bootstrap "$@"
fi

if command -v codex-rotate >/dev/null 2>&1; then
  exec codex-rotate internal vm-bootstrap "$@"
fi

if command -v node >/dev/null 2>&1; then
  exec node "$REPO_ROOT/packages/codex-rotate/index.js" internal vm-bootstrap "$@"
fi

echo "error: could not find codex-rotate runtime. Build the CLI or set CODEX_ROTATE_CLI_BIN." >&2
exit 1
