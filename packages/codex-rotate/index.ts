#!/usr/bin/env bun

import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";

export {
  buildReusableAccountProbeOrder,
  findNextCachedUsableAccountIndex,
  findNextImmediateRoundRobinIndex,
  generateRandomAdultBirthDate,
  resolveCredentialBirthDate,
  shouldUseStoredCredentialRelogin,
} from "./service.ts";

const MODULE_DIR = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(MODULE_DIR, "..", "..");
const LEGACY_ENTRYPOINT = join(MODULE_DIR, "legacy.ts");
const RUST_BIN = process.env.CODEX_ROTATE_RUST_BIN
  ?? join(REPO_ROOT, "target", "debug", process.platform === "win32" ? "codex-rotate-cli.exe" : "codex-rotate-cli");
const CARGO_BIN = process.env.CARGO_BIN ?? "cargo";
const BUN_BIN = process.env.BUN_BIN ?? "bun";

function runCommand(command: string, commandArgs: string[]) {
  return spawnSync(command, commandArgs, {
    cwd: REPO_ROOT,
    stdio: "inherit",
  });
}

function runLegacyCli(args: string[]): never {
  const result = runCommand(BUN_BIN, [LEGACY_ENTRYPOINT, ...args]);
  if (result.error) {
    const detail = result.error.message || `Failed to run ${BUN_BIN}`;
    console.error(detail);
    process.exit(1);
  }

  if (result.signal) {
    console.error(`legacy codex-rotate was interrupted by signal ${result.signal}.`);
    process.exit(1);
  }

  process.exit(typeof result.status === "number" ? result.status : 1);
}

function runRustCli(args: string[]): never {
  if (process.env.CODEX_ROTATE_USE_LEGACY === "1") {
    runLegacyCli(args);
  }

  const directBinaryExists = existsSync(RUST_BIN);
  const command = directBinaryExists ? RUST_BIN : CARGO_BIN;
  const commandArgs = directBinaryExists
    ? args
    : ["run", "--quiet", "--package", "codex-rotate-cli", "--", ...args];
  const result = runCommand(command, commandArgs);

  if (result.error) {
    if (!directBinaryExists && result.error.code === "ENOENT") {
      runLegacyCli(args);
    }
    const detail = result.error.message || `Failed to run ${command}`;
    console.error(detail);
    process.exit(1);
  }

  if (result.signal) {
    console.error(`codex-rotate-cli was interrupted by signal ${result.signal}.`);
    process.exit(1);
  }

  process.exit(typeof result.status === "number" ? result.status : 1);
}

const isMainModule = process.argv[1]
  ? import.meta.url === pathToFileURL(process.argv[1]).href
  : false;

if (isMainModule) {
  runRustCli(process.argv.slice(2));
}
