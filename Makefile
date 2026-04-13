.PHONY: all tray codex-rotate

all: tray codex-rotate

tray:
	cargo build -p codex-rotate-tray

codex-rotate:
	cargo build -p codex-rotate-cli --bin codex-rotate
