#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <mounted-guest-root>" >&2
  exit 1
fi

GUEST_ROOT="$1"
APP_ROOT="${GUEST_ROOT%/}"

# Auto-download UTM and utmctl if missing
if ! command -v utmctl >/dev/null 2>&1; then
  if [[ -x "/Applications/UTM.app/Contents/MacOS/utmctl" ]]; then
    echo "utmctl found in /Applications/UTM.app, but not in PATH. Please add it to your PATH." >&2
  else
    echo "utmctl not found. Installing UTM via Homebrew..."
    if command -v brew >/dev/null 2>&1; then
      brew install --cask utm
    else
      echo "error: Homebrew is required to auto-download UTM." >&2
      exit 1
    fi
  fi
fi

# Resolve repository root (parent of packages/codex-rotate/scripts)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

BRIDGE_ROOT="${CODEX_ROTATE_VM_BRIDGE_ROOT:-$APP_ROOT/Users/Shared/codex-rotate-bridge}"
AUTOSTART_DIR="$APP_ROOT/Library/LaunchAgents"
BOOTSTRAP_STAMP="$APP_ROOT/Users/Shared/.codex-rotate-vm-base-sealed"

# H02. Verify Codex Desktop
if [[ ! -d "$APP_ROOT/Applications/Codex.app" ]]; then
  echo "error: Codex Desktop not found at $APP_ROOT/Applications/Codex.app" >&2
  exit 1
fi

# H03. Verify Codex CLI
if [[ ! -x "$APP_ROOT/usr/local/bin/codex" ]]; then
  echo "error: Codex CLI not found at $APP_ROOT/usr/local/bin/codex" >&2
  exit 1
fi

# H04. Verify Chrome
if [[ ! -d "$APP_ROOT/Applications/Google Chrome.app" ]]; then
  echo "error: Google Chrome not found at $APP_ROOT/Applications/Google Chrome.app" >&2
  exit 1
fi

# H05. Verify Node
# Node could be in /usr/local/bin or elsewhere; we check common locations.
NODE_PATH="$APP_ROOT/usr/local/bin/node"
if [[ ! -x "$NODE_PATH" ]]; then
  echo "error: Node.js not found at $NODE_PATH" >&2
  exit 1
fi

mkdir -p "$BRIDGE_ROOT" "$AUTOSTART_DIR"

# H06. Install guest bridge assets
# We copy the relevant files from the host repo into the guest bridge root.
# In a real run, this would be the contents of the codex-rotate package.
echo "installing guest bridge assets into $BRIDGE_ROOT..."
cp -R "$REPO_ROOT/packages/codex-rotate"/* "$BRIDGE_ROOT/"

cat >"$AUTOSTART_DIR/com.codexrotate.guest-bridge.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>com.codexrotate.guest-bridge</string>
    <key>ProgramArguments</key>
    <array>
      <string>$NODE_PATH</string>
      <string>$BRIDGE_ROOT/index.js</string>
      <string>guest-bridge</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/Users/Shared/codex-rotate-bridge/guest-bridge.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/Shared/codex-rotate-bridge/guest-bridge.err.log</string>
  </dict>
</plist>
PLIST

cat >"$BOOTSTRAP_STAMP" <<EOF
sealed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
bridge_root=$BRIDGE_ROOT
notes=Install or verify Codex Desktop, Codex CLI, Chrome, Node, then replace the LaunchAgent placeholder with the real guest bridge runner.
EOF

echo "codex-rotate VM base bootstrap prepared:"
echo "  guest root: $APP_ROOT"
echo "  bridge root: $BRIDGE_ROOT"
echo "  launch agent: $AUTOSTART_DIR/com.codexrotate.guest-bridge.plist"
echo "  seal stamp: $BOOTSTRAP_STAMP"
