#!/usr/bin/env bun

import { existsSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import {
  cmdCreate,
  cmdRelogin,
  type CreateCommandOptions,
  type ReloginOptions,
} from "./service.ts";

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
const SELF_ENTRYPOINT = fileURLToPath(import.meta.url);
const RUST_BIN = process.env.CODEX_ROTATE_RUST_BIN
  ?? join(REPO_ROOT, "target", "debug", process.platform === "win32" ? "codex-rotate-cli.exe" : "codex-rotate-cli");
const CARGO_BIN = process.env.CARGO_BIN ?? "cargo";
const BUN_BIN = process.env.BUN_BIN ?? "bun";
const INTERNAL_LEGACY_ENV = "CODEX_ROTATE_INTERNAL_LEGACY";
const FORCE_LEGACY_ENV = "CODEX_ROTATE_USE_LEGACY";
const BOLD = "\x1b[1m";
const DIM = "\x1b[2m";
const CYAN = "\x1b[36m";
const RED = "\x1b[31m";
const RESET = "\x1b[0m";

function die(message: string): never {
  console.error(`${RED}x${RESET} ${message}`);
  process.exit(1);
}

function normalizeAlias(alias: string | null | undefined): string | undefined {
  if (typeof alias !== "string") return undefined;
  const trimmed = alias.trim();
  return trimmed || undefined;
}

function parseCreateOptions(args: string[]): CreateCommandOptions {
  const positionals: string[] = [];
  let profileName: string | undefined;
  let baseEmail: string | undefined;
  let force = false;
  let ignoreCurrent = false;

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (arg === "--force") {
      force = true;
      continue;
    }
    if (arg === "--ignore-current") {
      ignoreCurrent = true;
      continue;
    }
    if (arg === "--profile") {
      profileName = args[index + 1];
      if (!profileName) {
        die("Usage: codex-rotate create [alias] [--force] [--ignore-current] [--profile <managed-name>] [--base-email <email-family>]");
      }
      index += 1;
      continue;
    }
    if (arg.startsWith("--profile=")) {
      profileName = arg.slice("--profile=".length) || undefined;
      continue;
    }
    if (arg === "--base-email") {
      baseEmail = args[index + 1];
      if (!baseEmail) {
        die("Usage: codex-rotate create [alias] [--force] [--ignore-current] [--profile <managed-name>] [--base-email <email-family>]");
      }
      index += 1;
      continue;
    }
    if (arg.startsWith("--base-email=")) {
      baseEmail = arg.slice("--base-email=".length) || undefined;
      continue;
    }
    if (arg.startsWith("-")) {
      die(`Unknown create option: "${arg}"`);
    }
    positionals.push(arg);
  }

  if (positionals.length > 1) {
    die("Usage: codex-rotate create [alias] [--force] [--ignore-current] [--profile <managed-name>] [--base-email <email-family>]");
  }

  return {
    alias: normalizeAlias(positionals[0]),
    profileName,
    baseEmail,
    force,
    ignoreCurrent,
    source: "manual",
  };
}

function parseReloginOptions(args: string[]): { selector: string; options: ReloginOptions } {
  const positionals: string[] = [];
  const options: ReloginOptions = {
    allowEmailChange: false,
    deviceAuth: false,
    logoutFirst: true,
    manualLogin: false,
  };

  for (const arg of args) {
    switch (arg) {
      case "--allow-email-change":
        options.allowEmailChange = true;
        break;
      case "--device-auth":
        options.deviceAuth = true;
        options.manualLogin = true;
        break;
      case "--manual-login":
        options.manualLogin = true;
        options.deviceAuth = false;
        break;
      case "--browser-login":
      case "--no-device-auth":
        options.manualLogin = true;
        options.deviceAuth = false;
        break;
      case "--logout-first":
        options.logoutFirst = true;
        break;
      case "--keep-session":
      case "--no-logout-first":
        options.logoutFirst = false;
        break;
      default:
        if (arg.startsWith("-")) {
          die(`Unknown relogin option: "${arg}"`);
        }
        positionals.push(arg);
        break;
    }
  }

  if (positionals.length !== 1) {
    die("Usage: codex-rotate relogin <selector> [--allow-email-change] [--manual-login] [--device-auth] [--keep-session]");
  }

  const selector = positionals[0];
  if (!selector) {
    die("Usage: codex-rotate relogin <selector> [--allow-email-change] [--manual-login] [--device-auth] [--keep-session]");
  }

  return { selector, options };
}

function cmdHelp(): void {
  console.log(`
${BOLD}codex-rotate${RESET} - Rotate Codex CLI OAuth tokens across multiple ChatGPT accounts.

${BOLD}USAGE${RESET}
  codex-rotate <command> [args]

${BOLD}COMMANDS${RESET}
  ${CYAN}add${RESET} [alias]      Snapshot current ~/.codex/auth.json into the pool
  ${CYAN}create${RESET} [alias]   Reuse a healthy account, or create a new one when needed
  ${CYAN}next${RESET}             Swap to the next account with usable quota
  ${CYAN}prev${RESET}             Swap to the previous account
  ${CYAN}list${RESET}             Show all accounts with live quota info
  ${CYAN}status${RESET}           Show the current active account info and quota
  ${CYAN}relogin${RESET} <selector> Repair that account in one step
  ${CYAN}remove${RESET} <selector>  Remove that account from the pool
  ${CYAN}help${RESET}             Show this help message

${BOLD}WORKFLOW${RESET}
  1. Log into ChatGPT account #1:  ${DIM}codex auth login${RESET}
  2. Save it:                      ${DIM}codex-rotate add${RESET}
  3. Log into ChatGPT account #2:  ${DIM}codex auth login${RESET}
  4. Save it:                      ${DIM}codex-rotate add work${RESET}
  5. Check quota:                  ${DIM}codex-rotate list${RESET}
  6. Rotate:                       ${DIM}codex-rotate next${RESET}
  7. Resume/backfill or create:    ${DIM}codex-rotate create${RESET}
  8. Repair a dead entry:          ${DIM}codex-rotate relogin person@example.com_free${RESET}
`);
}

function isLegacyOnlyCommand(command: string | undefined): boolean {
  return command === "create"
    || command === "new"
    || command === "relogin"
    || command === "reauth"
    || command === "__legacy_next_create";
}

function runCommand(command: string, commandArgs: string[]) {
  return spawnSync(command, commandArgs, {
    cwd: REPO_ROOT,
    stdio: "inherit",
    env: process.env,
  });
}

function runLegacyCli(args: string[]): never {
  const result = spawnSync(BUN_BIN, [SELF_ENTRYPOINT, ...args], {
    cwd: REPO_ROOT,
    stdio: "inherit",
    env: {
      ...process.env,
      [INTERNAL_LEGACY_ENV]: "1",
      [FORCE_LEGACY_ENV]: "0",
    },
  });
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
  const command = args[0];
  if (process.env[FORCE_LEGACY_ENV] === "1" && isLegacyOnlyCommand(command)) {
    runLegacyCli(args);
  }

  const directBinaryExists = existsSync(RUST_BIN);
  const runner = directBinaryExists ? RUST_BIN : CARGO_BIN;
  const commandArgs = directBinaryExists
    ? args
    : ["run", "--quiet", "--package", "codex-rotate-cli", "--", ...args];
  const result = runCommand(runner, commandArgs);

  if (result.error) {
    if (!directBinaryExists && result.error.code === "ENOENT" && isLegacyOnlyCommand(command)) {
      runLegacyCli(args);
    }
    const detail = result.error.message || `Failed to run ${runner}`;
    console.error(detail);
    process.exit(1);
  }

  if (result.signal) {
    console.error(`codex-rotate-cli was interrupted by signal ${result.signal}.`);
    process.exit(1);
  }

  process.exit(typeof result.status === "number" ? result.status : 1);
}

async function runInternalLegacyCli(args: string[]): Promise<void> {
  const [command, ...rest] = args;

  switch (command) {
    case "create":
    case "new":
      await cmdCreate(parseCreateOptions(rest));
      return;
    case "relogin":
    case "reauth": {
      const { selector, options } = parseReloginOptions(rest);
      await cmdRelogin(selector, options);
      return;
    }
    case "__legacy_next_create":
      await cmdCreate({
        source: "next",
        requireUsableQuota: true,
      });
      return;
    case "help":
    case "--help":
    case "-h":
    case undefined:
      cmdHelp();
      return;
    default:
      die(
        `Legacy TypeScript mode only supports create/relogin automation commands. `
        + `Run the Rust CLI for "${command}".`,
      );
  }
}

const isMainModule = process.argv[1]
  ? import.meta.url === pathToFileURL(process.argv[1]).href
  : false;

if (isMainModule) {
  if (process.env[INTERNAL_LEGACY_ENV] === "1") {
    await runInternalLegacyCli(process.argv.slice(2));
  } else {
    runRustCli(process.argv.slice(2));
  }
}
