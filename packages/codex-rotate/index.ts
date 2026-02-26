#!/usr/bin/env bun
/**
 * codex-rotate — Rotate Codex CLI OAuth tokens across multiple ChatGPT accounts.
 *
 * Usage:
 *   codex-rotate add <label>      Snapshot current ~/.codex/auth.json into the pool
 *   codex-rotate next             Swap to the next account in round-robin order
 *   codex-rotate prev             Swap to the previous account
 *   codex-rotate list             Show all accounts in the pool
 *   codex-rotate status           Show current active account info
 *   codex-rotate remove <label>   Remove an account from the pool
 *   codex-rotate help             Show this help message
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";

// ── Paths ──────────────────────────────────────────────────────────────────────

const CODEX_HOME = process.env.CODEX_HOME ?? join(homedir(), ".codex");
const CODEX_AUTH = join(CODEX_HOME, "auth.json");

const ROTATE_HOME = join(homedir(), ".codex-rotate");
const POOL_FILE = join(ROTATE_HOME, "accounts.json");

// ── Types ──────────────────────────────────────────────────────────────────────

interface CodexAuth {
  auth_mode: string;
  OPENAI_API_KEY: string | null;
  tokens: {
    id_token: string;
    access_token: string;
    refresh_token: string;
    account_id: string;
  };
  last_refresh: string;
}

interface AccountEntry {
  label: string;
  email: string;
  account_id: string;
  plan_type: string;
  auth: CodexAuth;
  added_at: string;
}

interface Pool {
  active_index: number;
  accounts: AccountEntry[];
}

// ── Helpers ────────────────────────────────────────────────────────────────────

const BOLD = "\x1b[1m";
const DIM = "\x1b[2m";
const GREEN = "\x1b[32m";
const YELLOW = "\x1b[33m";
const CYAN = "\x1b[36m";
const RED = "\x1b[31m";
const RESET = "\x1b[0m";

function die(msg: string): never {
  console.error(`${RED}✖${RESET} ${msg}`);
  process.exit(1);
}

function info(msg: string): void {
  console.log(`${GREEN}✔${RESET} ${msg}`);
}

function warn(msg: string): void {
  console.log(`${YELLOW}⚠${RESET} ${msg}`);
}

/** Decode a JWT payload without any library — just base64url decode the middle segment. */
function decodeJwtPayload(jwt: string): Record<string, unknown> {
  const parts = jwt.split(".");
  if (parts.length !== 3) throw new Error("Invalid JWT");
  const payload = parts[1]!
    .replaceAll("-", "+")
    .replaceAll("_", "/");
  const json = Buffer.from(payload, "base64").toString("utf-8");
  return JSON.parse(json);
}

function extractEmailFromAuth(auth: CodexAuth): string {
  try {
    const payload = decodeJwtPayload(auth.tokens.id_token);
    return (payload.email as string) ?? "unknown";
  } catch {
    return "unknown";
  }
}

function extractPlanFromAuth(auth: CodexAuth): string {
  try {
    const payload = decodeJwtPayload(auth.tokens.id_token);
    const authInfo = payload["https://api.openai.com/auth"] as Record<string, unknown> | undefined;
    return (authInfo?.chatgpt_plan_type as string) ?? "unknown";
  } catch {
    return "unknown";
  }
}

// ── Pool I/O ───────────────────────────────────────────────────────────────────

function ensureRotateDir(): void {
  if (!existsSync(ROTATE_HOME)) {
    mkdirSync(ROTATE_HOME, { recursive: true });
  }
}

function loadPool(): Pool {
  if (!existsSync(POOL_FILE)) {
    return { active_index: 0, accounts: [] };
  }
  const raw = readFileSync(POOL_FILE, "utf-8");
  return JSON.parse(raw) as Pool;
}

function savePool(pool: Pool): void {
  ensureRotateDir();
  writeFileSync(POOL_FILE, JSON.stringify(pool, null, 2), { mode: 0o600 });
}

function loadCodexAuth(): CodexAuth {
  if (!existsSync(CODEX_AUTH)) {
    die(`Codex auth file not found at ${CODEX_AUTH}\nRun "codex auth login" first.`);
  }
  const raw = readFileSync(CODEX_AUTH, "utf-8");
  return JSON.parse(raw) as CodexAuth;
}

function writeCodexAuth(auth: CodexAuth): void {
  writeFileSync(CODEX_AUTH, JSON.stringify(auth, null, 2), { mode: 0o600 });
}

// ── Commands ───────────────────────────────────────────────────────────────────

function cmdAdd(label: string): void {
  if (!label) die('Usage: codex-rotate add <label>');

  const auth = loadCodexAuth();
  const pool = loadPool();

  // Check for duplicate label
  const existing = pool.accounts.findIndex((a) => a.label === label);
  if (existing !== -1) {
    // Update in place
    const entry = pool.accounts[existing]!;
    entry.auth = auth;
    entry.email = extractEmailFromAuth(auth);
    entry.plan_type = extractPlanFromAuth(auth);
    entry.account_id = auth.tokens.account_id;
    pool.active_index = existing;
    savePool(pool);
    info(`Updated account "${label}" (${entry.email})`);
    return;
  }

  // Check for duplicate account_id
  const dupAcct = pool.accounts.find((a) => a.auth.tokens.account_id === auth.tokens.account_id);
  if (dupAcct) {
    warn(`This account is already in the pool as "${dupAcct.label}". Updating its tokens.`);
    dupAcct.auth = auth;
    dupAcct.email = extractEmailFromAuth(auth);
    dupAcct.plan_type = extractPlanFromAuth(auth);
    savePool(pool);
    return;
  }

  const entry: AccountEntry = {
    label,
    email: extractEmailFromAuth(auth),
    account_id: auth.tokens.account_id,
    plan_type: extractPlanFromAuth(auth),
    auth,
    added_at: new Date().toISOString(),
  };

  pool.accounts.push(entry);
  pool.active_index = pool.accounts.length - 1;
  savePool(pool);
  info(`Added account "${label}" (${entry.email}, ${entry.plan_type}) — pool now has ${pool.accounts.length} account(s)`);
}

function cmdNext(): void {
  const pool = loadPool();
  if (pool.accounts.length === 0) die("No accounts in pool. Run: codex-rotate add <label>");
  if (pool.accounts.length === 1) die("Only 1 account in pool. Add more with: codex-rotate add <label>");

  // Save current auth back into the active slot (preserves any token refreshes Codex did)
  if (existsSync(CODEX_AUTH)) {
    const currentAuth = loadCodexAuth();
    const activeAccount = pool.accounts[pool.active_index];
    if (activeAccount && activeAccount.auth.tokens.account_id === currentAuth.tokens.account_id) {
      activeAccount.auth = currentAuth;
    }
  }

  // Advance to next
  const prevIndex = pool.active_index;
  pool.active_index = (pool.active_index + 1) % pool.accounts.length;
  const next = pool.accounts[pool.active_index]!;

  // Write new auth
  writeCodexAuth(next.auth);
  savePool(pool);

  const prev = pool.accounts[prevIndex]!;
  console.log(
    `${GREEN}⟳${RESET} Rotated: ${DIM}${prev.label}${RESET} (${prev.email}) → ${BOLD}${next.label}${RESET} (${CYAN}${next.email}${RESET}, ${next.plan_type})\n` +
    `${DIM}  [${pool.active_index + 1}/${pool.accounts.length}]${RESET}`
  );
}

function cmdPrev(): void {
  const pool = loadPool();
  if (pool.accounts.length === 0) die("No accounts in pool. Run: codex-rotate add <label>");
  if (pool.accounts.length === 1) die("Only 1 account in pool. Add more with: codex-rotate add <label>");

  // Save current auth back into the active slot
  if (existsSync(CODEX_AUTH)) {
    const currentAuth = loadCodexAuth();
    const activeAccount = pool.accounts[pool.active_index];
    if (activeAccount && activeAccount.auth.tokens.account_id === currentAuth.tokens.account_id) {
      activeAccount.auth = currentAuth;
    }
  }

  const prevIndex = pool.active_index;
  pool.active_index = (pool.active_index - 1 + pool.accounts.length) % pool.accounts.length;
  const next = pool.accounts[pool.active_index]!;

  writeCodexAuth(next.auth);
  savePool(pool);

  const prev = pool.accounts[prevIndex]!;
  console.log(
    `${GREEN}⟳${RESET} Rotated: ${DIM}${prev.label}${RESET} (${prev.email}) → ${BOLD}${next.label}${RESET} (${CYAN}${next.email}${RESET}, ${next.plan_type})\n` +
    `${DIM}  [${pool.active_index + 1}/${pool.accounts.length}]${RESET}`
  );
}

function cmdList(): void {
  const pool = loadPool();
  if (pool.accounts.length === 0) {
    warn("No accounts in pool. Add one with: codex-rotate add <label>");
    return;
  }

  console.log(`\n${BOLD}Codex OAuth Account Pool${RESET} (${pool.accounts.length} account(s))\n`);

  for (let i = 0; i < pool.accounts.length; i++) {
    const a = pool.accounts[i]!;
    const isActive = i === pool.active_index;
    const marker = isActive ? `${GREEN}▶${RESET}` : " ";
    const labelStr = isActive ? `${BOLD}${a.label}${RESET}` : a.label;
    const planStr = `${DIM}${a.plan_type}${RESET}`;
    console.log(`  ${marker} ${labelStr}  ${CYAN}${a.email}${RESET}  ${planStr}  ${DIM}${a.account_id.slice(0, 8)}…${RESET}`);
  }
  console.log();
}

function cmdStatus(): void {
  const pool = loadPool();
  const auth = existsSync(CODEX_AUTH) ? loadCodexAuth() : null;

  console.log(`\n${BOLD}Codex Rotate Status${RESET}\n`);

  if (auth) {
    const email = extractEmailFromAuth(auth);
    const plan = extractPlanFromAuth(auth);
    console.log(`  ${BOLD}Active in Codex:${RESET}  ${CYAN}${email}${RESET}  (${plan})`);
    console.log(`  ${BOLD}Account ID:${RESET}       ${auth.tokens.account_id}`);
    console.log(`  ${BOLD}Last refresh:${RESET}     ${auth.last_refresh}`);
  } else {
    warn("No Codex auth file found.");
  }

  console.log(`\n  ${BOLD}Pool file:${RESET}        ${POOL_FILE}`);
  console.log(`  ${BOLD}Pool size:${RESET}        ${pool.accounts.length} account(s)`);

  if (pool.accounts.length > 0) {
    const active = pool.accounts[pool.active_index]!;
    console.log(`  ${BOLD}Active slot:${RESET}      ${active.label} [${pool.active_index + 1}/${pool.accounts.length}]`);
  }
  console.log();
}

function cmdRemove(label: string): void {
  if (!label) die("Usage: codex-rotate remove <label>");

  const pool = loadPool();
  const idx = pool.accounts.findIndex((a) => a.label === label);
  if (idx === -1) die(`Account "${label}" not found in pool.`);

  const removed = pool.accounts.splice(idx, 1)[0]!;

  // Adjust active_index
  if (pool.accounts.length === 0 || pool.active_index >= pool.accounts.length) {
    pool.active_index = 0;
  }

  savePool(pool);
  info(`Removed "${removed.label}" (${removed.email}). Pool now has ${pool.accounts.length} account(s).`);
}

function cmdHelp(): void {
  console.log(`
${BOLD}codex-rotate${RESET} — Rotate Codex CLI OAuth tokens across multiple ChatGPT accounts.

${BOLD}USAGE${RESET}
  codex-rotate <command> [args]

${BOLD}COMMANDS${RESET}
  ${CYAN}add${RESET} <label>      Snapshot current ~/.codex/auth.json into the pool
  ${CYAN}next${RESET}             Swap to the next account (round-robin)
  ${CYAN}prev${RESET}             Swap to the previous account
  ${CYAN}list${RESET}             Show all accounts in the pool
  ${CYAN}status${RESET}           Show current active account info
  ${CYAN}remove${RESET} <label>   Remove an account from the pool
  ${CYAN}help${RESET}             Show this help message

${BOLD}WORKFLOW${RESET}
  1. Log into ChatGPT account #1:  ${DIM}codex auth login${RESET}
  2. Save it:                      ${DIM}codex-rotate add personal${RESET}
  3. Log into ChatGPT account #2:  ${DIM}codex auth login${RESET}
  4. Save it:                      ${DIM}codex-rotate add work${RESET}
  5. Rotate:                       ${DIM}codex-rotate next${RESET}

${BOLD}FILES${RESET}
  Pool:  ${DIM}~/.codex-rotate/accounts.json${RESET}
  Auth:  ${DIM}~/.codex/auth.json${RESET}
`);
}

// ── Main ───────────────────────────────────────────────────────────────────────

const [command, ...args] = process.argv.slice(2);

switch (command) {
  case "add":
    cmdAdd(args[0] ?? '');
    break;
  case "next":
  case "n":
    cmdNext();
    break;
  case "prev":
  case "p":
    cmdPrev();
    break;
  case "list":
  case "ls":
    cmdList();
    break;
  case "status":
  case "s":
    cmdStatus();
    break;
  case "remove":
  case "rm":
    cmdRemove(args[0] ?? '');
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
