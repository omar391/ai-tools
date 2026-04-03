#!/usr/bin/env bun
/**
 * codex-rotate CLI entrypoint.
 *
 * This file stays intentionally thin: parse argv, print help, and dispatch into
 * the service layer.
 */

import { pathToFileURL } from "node:url";
import {
  cmdAdd,
  cmdCreate,
  cmdList,
  cmdNext,
  cmdPrev,
  cmdRelogin,
  cmdRemove,
  cmdStatus,
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

const BOLD = "\x1b[1m";
const DIM = "\x1b[2m";
const CYAN = "\x1b[36m";
const RED = "\x1b[31m";
const RESET = "\x1b[0m";

function die(message: string): never {
  console.error(`${RED}✖${RESET} ${message}`);
  process.exit(1);
}

function normalizeAlias(alias: string | null | undefined): string | undefined {
  if (typeof alias !== "string") return undefined;
  const trimmed = alias.trim();
  return trimmed || undefined;
}

function parseAddAlias(args: string[]): string | undefined {
  if (args.length > 1) {
    die("Usage: codex-rotate add [alias]");
  }
  const alias = args[0];
  if (alias?.startsWith("-")) {
    die("Usage: codex-rotate add [alias]");
  }
  return alias;
}

function parseRemoveSelector(args: string[]): string {
  if (args.length !== 1 || args[0]?.startsWith("-")) {
    die("Usage: codex-rotate remove <selector>");
  }
  return args[0]!;
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
${BOLD}codex-rotate${RESET} — Rotate Codex CLI OAuth tokens across multiple ChatGPT accounts.

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

${BOLD}RELOGIN FLAGS${RESET}
  Default behavior uses stored Bitwarden credentials when available
  ${DIM}--manual-login${RESET}       Force the legacy manual browser login flow
  ${DIM}--device-auth${RESET}        Use the device auth flow instead of browser login
  ${DIM}--keep-session${RESET}       Skip the pre-login ${DIM}codex logout${RESET} for manual relogins
  ${DIM}--allow-email-change${RESET} Replace the selected account even if the signed-in email changed

${BOLD}CREATE FLAGS${RESET}
  ${DIM}--force${RESET}                  Create a new account even if a healthy pool account exists
  ${DIM}--ignore-current${RESET}         Ignore the current slot when probing reusable healthy accounts
  ${DIM}--profile <managed-name>${RESET} Choose the fast-browser managed profile
  ${DIM}--base-email <email-family>${RESET} Override the create email family for this run
  ${DIM}(omitted values default to workflow preferred_profile "dev-1" and discover email from that profile)${RESET}

${BOLD}FILES${RESET}
  Pool:  ${DIM}~/.codex-rotate/accounts.json${RESET}
  Creds: ${DIM}~/.codex-rotate/credentials.json${RESET} ${DIM}(metadata only after Bitwarden migration)${RESET}
  Auth:  ${DIM}~/.codex/auth.json${RESET}
`);
}

async function main(): Promise<void> {
  const [command, ...args] = process.argv.slice(2);

  switch (command) {
    case "add":
      cmdAdd(parseAddAlias(args));
      break;
    case "create":
    case "new":
      await cmdCreate(parseCreateOptions(args));
      break;
    case "next":
    case "n":
      await cmdNext();
      break;
    case "prev":
    case "p":
      cmdPrev();
      break;
    case "list":
    case "ls":
      await cmdList();
      break;
    case "status":
    case "s":
      await cmdStatus();
      break;
    case "relogin":
    case "reauth": {
      const { selector, options } = parseReloginOptions(args);
      await cmdRelogin(selector, options);
      break;
    }
    case "remove":
    case "rm":
      cmdRemove(parseRemoveSelector(args));
      break;
    case "help":
    case "--help":
    case "-h":
    case undefined:
      cmdHelp();
      break;
    default:
      die(`Unknown command: "${command}". Run "codex-rotate help" for usage.`);
  }
}

const isMainModule = process.argv[1]
  ? import.meta.url === pathToFileURL(process.argv[1]).href
  : false;

if (isMainModule) {
  await main();
}
