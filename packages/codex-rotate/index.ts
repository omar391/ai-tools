#!/usr/bin/env bun

import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const MODULE_DIR = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(MODULE_DIR, "..", "..");
const RUST_BIN =
  process.env.CODEX_ROTATE_RUST_BIN ??
  join(
    REPO_ROOT,
    "target",
    "debug",
    process.platform === "win32" ? "codex-rotate-cli.exe" : "codex-rotate-cli",
  );
const CARGO_BIN = process.env.CARGO_BIN ?? "cargo";

const args = process.argv.slice(2);
const directBinaryExists = existsSync(RUST_BIN);
const runner = directBinaryExists ? RUST_BIN : CARGO_BIN;
const runnerArgs = directBinaryExists
  ? args
  : ["run", "--quiet", "--package", "codex-rotate-cli", "--", ...args];

const result = spawnSync(runner, runnerArgs, {
  cwd: REPO_ROOT,
  stdio: "inherit",
  env: process.env,
});

if (result.error) {
  console.error(result.error.message || `Failed to run ${runner}`);
  process.exit(1);
}

if (result.signal) {
  console.error(`codex-rotate-cli was interrupted by signal ${result.signal}.`);
  process.exit(1);
}

process.exit(typeof result.status === "number" ? result.status : 1);
