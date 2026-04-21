.PHONY: all tray codex-rotate sandbox-dry-run dry-run hermetic-host hermetic-vm host-live vm-live

all: tray codex-rotate

tray:
	cargo build -p codex-rotate-tray

codex-rotate:
	cargo build -p codex-rotate-cli --bin codex-rotate

sandbox-dry-run:
	cargo test -p codex-rotate-runtime --lib rotation_hygiene::tests::host_sandbox_dry_run_next_preserves_live_snapshot -- --exact --nocapture

dry-run: sandbox-dry-run

hermetic-host:
	cargo test -p codex-rotate-runtime --lib -- --nocapture
	cargo test -p codex-rotate-cli --tests -- --nocapture

hermetic-vm:
	cargo test -p codex-rotate-runtime --lib -- --nocapture
	cargo test -p codex-rotate-cli --tests -- --nocapture

host-live:
	cargo run -p codex-rotate-cli -- internal live-check host

vm-live:
	cargo run -p codex-rotate-cli -- internal live-check vm
