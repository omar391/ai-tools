.PHONY: all tray codex-rotate sandbox-dry-run dry-run

all: tray codex-rotate

tray:
	cargo build -p codex-rotate-tray

codex-rotate:
	cargo build -p codex-rotate-cli --bin codex-rotate

sandbox-dry-run:
	cargo test -p codex-rotate-runtime --lib rotation_hygiene::tests::host_sandbox_dry_run_next_preserves_live_snapshot -- --exact --nocapture

dry-run: sandbox-dry-run
