#!/usr/bin/env bun
/**
 * codex-rotate — Rotate Codex CLI OAuth tokens across multiple ChatGPT accounts.
 *
 * Usage:
 *   codex-rotate add [alias]      Snapshot current ~/.codex/auth.json into the pool
 *   codex-rotate create [alias]   Create a new OpenAI account and switch to it
 *   codex-rotate next             Swap to the next account with usable quota
 *   codex-rotate prev             Swap to the previous account
 *   codex-rotate list             Show all accounts in the pool with live quota
 *   codex-rotate status           Show current active account info and quota
 *   codex-rotate relogin <selector> Repair a pool entry in one step
 *   codex-rotate remove <selector>  Remove an account from the pool
 *   codex-rotate help             Show this help message
 */

import { spawnSync } from "node:child_process";
import { randomInt } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import {
  buildGmailAliasEmail,
  cancelCodexBrowserLoginSession,
  computeNextGmailAliasSuffix,
  generatePassword,
  loadCredentialStore,
  makeCredentialFamilyKey,
  readLocalWorkflowMetadata,
  resolveManagedProfileName,
  resolveCreateBaseEmail,
  readCodexRotateAuthFlowSummary,
  readCodexRotateAuthFlowSession,
  runCodexBrowserLoginWorkflow,
  saveCredentialStore,
  shouldUseDefaultCreateFamilyHint,
  selectPendingBaseEmailHintForProfile,
  selectStoredBaseEmailHint,
  selectPendingCredentialForFamily,
  type CodexRotateAuthFlowSession,
  type CredentialStore,
  type PendingCredential,
  type StoredCredential,
} from "./automation.ts";

// ── Paths ──────────────────────────────────────────────────────────────────────

const CODEX_HOME = process.env.CODEX_HOME ?? join(homedir(), ".codex");
const CODEX_AUTH = join(CODEX_HOME, "auth.json");

const ROTATE_HOME = join(homedir(), ".codex-rotate");
const POOL_FILE = join(ROTATE_HOME, "accounts.json");

// ── Network ────────────────────────────────────────────────────────────────────

const DEFAULT_OAUTH_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann";
const OAUTH_TOKEN_URL = process.env.CODEX_REFRESH_TOKEN_URL_OVERRIDE ?? "https://auth.openai.com/oauth/token";
const WHAM_USAGE_URL = "https://chatgpt.com/backend-api/wham/usage";
const REQUEST_TIMEOUT_MS = 8000;
const CODEX_BIN = process.env.CODEX_ROTATE_CODEX_BIN ?? "codex";
const CREATE_DEFAULT_PROFILE_WORKFLOW = "local:web:auth.openai.com:codex-rotate-account-flow";
const CODEX_LOGIN_MAX_ATTEMPTS = 3;
const CODEX_LOGIN_MAX_REPLAY_PASSES = 5;
const CODEX_LOGIN_RETRY_DELAYS_MS = [30_000, 60_000] as const;

// ── Types ──────────────────────────────────────────────────────────────────────

interface CodexAuth {
  auth_mode: string;
  OPENAI_API_KEY: string | null;
  tokens: {
    id_token: string;
    access_token: string;
    refresh_token: string | null;
    account_id: string;
  };
  last_refresh: string;
}

interface AccountEntry {
  label: string;
  alias?: string;
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

interface OAuthTokenResponse {
  access_token?: string;
  id_token?: string;
  refresh_token?: string;
  token_type?: string;
  expires_in?: number;
  scope?: string;
  error?: string;
  error_description?: string;
}

interface UsageWindow {
  used_percent: number;
  limit_window_seconds: number;
  reset_after_seconds: number;
  reset_at: number;
}

interface UsageRateLimit {
  allowed: boolean;
  limit_reached: boolean;
  primary_window: UsageWindow | null;
  secondary_window: UsageWindow | null;
}

interface UsageCredits {
  has_credits: boolean;
  unlimited: boolean;
  balance: number | null;
  approx_local_messages: number | null;
  approx_cloud_messages: number | null;
}

interface UsageResponse {
  user_id: string;
  account_id: string;
  email: string;
  plan_type: string;
  rate_limit: UsageRateLimit | null;
  code_review_rate_limit: UsageRateLimit | null;
  additional_rate_limits: unknown;
  credits: UsageCredits | null;
  promo: unknown;
}

interface AccountInspection {
  usage: UsageResponse | null;
  error: string | null;
  updated: boolean;
}

interface RotationCandidate {
  index: number;
  entry: AccountEntry;
  inspection: AccountInspection;
}

interface ReloginOptions {
  allowEmailChange: boolean;
  deviceAuth: boolean;
  logoutFirst: boolean;
  manualLogin: boolean;
}

interface AccountSelection {
  entry: AccountEntry;
  index: number;
}

interface CreateCommandOptions {
  alias?: string;
  profileName?: string;
  baseEmail?: string;
  requireUsableQuota?: boolean;
  source?: "manual" | "next";
}

interface CreateCommandResult {
  entry: AccountEntry;
  inspection: AccountInspection | null;
  profileName: string;
  baseEmail: string;
  createdEmail: string;
}

interface SignupStateSummary {
  accountReady: boolean;
  antiBotGate: boolean;
  createAccountFailed: boolean;
  existingAccountPrompt: boolean;
  followUpStep: boolean;
  invalidCredentials: boolean;
  needsEmailVerification: boolean;
  rateLimitExceeded: boolean;
  sessionEnded: boolean;
}

class HttpError extends Error {
  status: number;
  body: string;

  constructor(message: string, status: number, body: string) {
    super(message);
    this.name = "HttpError";
    this.status = status;
    this.body = body;
  }
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

function note(msg: string): void {
  console.log(`${DIM}${msg}${RESET}`);
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

function parseJson<T>(raw: string, fallbackMessage: string): T {
  try {
    return JSON.parse(raw) as T;
  } catch {
    throw new Error(fallbackMessage);
  }
}

/** Decode a JWT payload without any library — just base64url decode the middle segment. */
function decodeJwtPayload(jwt: string): Record<string, unknown> {
  const parts = jwt.split(".");
  if (parts.length !== 3) throw new Error("Invalid JWT");

  const payload = parts[1]!
    .replaceAll("-", "+")
    .replaceAll("_", "/");
  const padded = payload + "=".repeat((4 - (payload.length % 4)) % 4);
  const json = Buffer.from(padded, "base64").toString("utf-8");
  return parseJson<Record<string, unknown>>(json, "Invalid JWT payload");
}

function extractEmailFromAuth(auth: CodexAuth): string {
  try {
    const accessPayload = decodeJwtPayload(auth.tokens.access_token);
    const profile = accessPayload["https://api.openai.com/profile"] as Record<string, unknown> | undefined;
    if (typeof profile?.email === "string") return profile.email;

    const idPayload = decodeJwtPayload(auth.tokens.id_token);
    return (idPayload.email as string) ?? "unknown";
  } catch {
    return "unknown";
  }
}

function extractPlanFromAuth(auth: CodexAuth): string {
  try {
    const payload = decodeJwtPayload(auth.tokens.access_token);
    const authInfo = payload["https://api.openai.com/auth"] as Record<string, unknown> | undefined;
    return (authInfo?.chatgpt_plan_type as string) ?? "unknown";
  } catch {
    return "unknown";
  }
}

function normalizeEmailForLabel(email: string): string {
  const normalized = email.trim().toLowerCase();
  return normalized || "unknown";
}

function normalizePlanTypeForLabel(planType: string): string {
  const normalized = planType
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");

  return normalized || "unknown";
}

function buildAccountLabel(email: string, planType: string): string {
  return `${normalizeEmailForLabel(email)}_${normalizePlanTypeForLabel(planType)}`;
}

function normalizeAlias(alias: string | null | undefined): string | undefined {
  if (typeof alias !== "string") return undefined;
  const trimmed = alias.trim();
  return trimmed || undefined;
}

function getAccountSelector(entry: AccountEntry): string {
  return entry.label;
}

function getAccountSummary(entry: AccountEntry): string {
  return entry.alias ? `${entry.label} (${entry.alias})` : entry.label;
}

function extractAccountIdFromToken(jwt: string): string | null {
  try {
    const payload = decodeJwtPayload(jwt);
    const authInfo = payload["https://api.openai.com/auth"] as Record<string, unknown> | undefined;
    const accountId = authInfo?.chatgpt_account_id;
    return typeof accountId === "string" ? accountId : null;
  } catch {
    return null;
  }
}

function extractAccountIdFromAuth(auth: CodexAuth): string {
  return extractAccountIdFromToken(auth.tokens.access_token)
    ?? extractAccountIdFromToken(auth.tokens.id_token)
    ?? auth.tokens.account_id;
}

function extractClientIdFromAuth(auth: CodexAuth): string {
  try {
    const accessPayload = decodeJwtPayload(auth.tokens.access_token);
    if (typeof accessPayload.client_id === "string") return accessPayload.client_id;
  } catch {
    // Fall through to ID token/aud.
  }

  try {
    const idPayload = decodeJwtPayload(auth.tokens.id_token);
    const audience = idPayload.aud;
    if (Array.isArray(audience) && typeof audience[0] === "string") {
      return audience[0];
    }
  } catch {
    // Fall through to default.
  }

  return DEFAULT_OAUTH_CLIENT_ID;
}

function getTokenExpiry(jwt: string): number | null {
  try {
    const payload = decodeJwtPayload(jwt);
    return typeof payload.exp === "number" ? payload.exp : null;
  } catch {
    return null;
  }
}

function isTokenExpired(jwt: string, skewSeconds = 60): boolean {
  const exp = getTokenExpiry(jwt);
  if (exp === null) return false;
  return exp <= Math.floor(Date.now() / 1000) + skewSeconds;
}

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
  const pool = parseJson<Pool>(raw, `Invalid pool file at ${POOL_FILE}`);
  normalizePoolEntries(pool);
  return pool;
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
  return parseJson<CodexAuth>(raw, `Invalid Codex auth file at ${CODEX_AUTH}`);
}

function writeCodexAuth(auth: CodexAuth): void {
  writeFileSync(CODEX_AUTH, JSON.stringify(auth, null, 2), { mode: 0o600 });
}

function syncPoolActiveAccountFromCodex(pool: Pool): boolean {
  if (!existsSync(CODEX_AUTH)) return false;

  const active = pool.accounts[pool.active_index];
  if (!active) return false;

  const currentAuth = loadCodexAuth();
  if (active.auth.tokens.account_id !== currentAuth.tokens.account_id) return false;

  return applyAuthToAccount(active, currentAuth);
}

function findPoolEntryByAccountId(pool: Pool, accountId: string): AccountEntry | undefined {
  return pool.accounts.find((entry) => entry.account_id === accountId || entry.auth.tokens.account_id === accountId);
}

function normalizePoolEntries(pool: Pool): boolean {
  let changed = false;

  for (const entry of pool.accounts) {
    const nextLabel = buildAccountLabel(entry.email, entry.plan_type);
    const currentAlias = normalizeAlias(entry.alias);

    if (entry.label !== nextLabel) {
      if (!currentAlias && entry.label) {
        entry.alias = entry.label;
      }
      entry.label = nextLabel;
      changed = true;
    }

    const nextAlias = normalizeAlias(entry.alias);
    if (nextAlias) {
      if (nextAlias === entry.label) {
        delete entry.alias;
        changed = true;
      } else if (entry.alias !== nextAlias) {
        entry.alias = nextAlias;
        changed = true;
      }
    } else if ("alias" in entry) {
      delete entry.alias;
      changed = true;
    }

    const nextAccountId = extractAccountIdFromAuth(entry.auth);
    if (entry.account_id !== nextAccountId) {
      entry.account_id = nextAccountId;
      changed = true;
    }
  }

  const maxActiveIndex = Math.max(0, pool.accounts.length - 1);
  const normalizedActiveIndex = Math.min(Math.max(pool.active_index, 0), maxActiveIndex);
  if (pool.active_index !== normalizedActiveIndex) {
    pool.active_index = normalizedActiveIndex;
    changed = true;
  }

  return changed;
}

function applyAuthToAccount(entry: AccountEntry, auth: CodexAuth): boolean {
  const nextEmail = extractEmailFromAuth(auth);
  const nextPlan = extractPlanFromAuth(auth);
  const nextAccountId = extractAccountIdFromAuth(auth);
  const nextLabel = buildAccountLabel(nextEmail, nextPlan);
  const nextAlias = normalizeAlias(entry.alias);

  const changed =
    entry.label !== nextLabel
    || entry.alias !== nextAlias
    || entry.email !== nextEmail
    || entry.plan_type !== nextPlan
    || entry.account_id !== nextAccountId
    || entry.auth.auth_mode !== auth.auth_mode
    || entry.auth.OPENAI_API_KEY !== auth.OPENAI_API_KEY
    || entry.auth.tokens.id_token !== auth.tokens.id_token
    || entry.auth.tokens.access_token !== auth.tokens.access_token
    || entry.auth.tokens.refresh_token !== auth.tokens.refresh_token
    || entry.auth.tokens.account_id !== auth.tokens.account_id
    || entry.auth.last_refresh !== auth.last_refresh;

  entry.label = nextLabel;
  if (nextAlias && nextAlias !== nextLabel) {
    entry.alias = nextAlias;
  } else if ("alias" in entry) {
    delete entry.alias;
  }
  entry.email = nextEmail;
  entry.plan_type = nextPlan;
  entry.account_id = nextAccountId;
  entry.auth = auth;

  return changed;
}

function applyUsageToAccount(entry: AccountEntry, usage: UsageResponse): boolean {
  const nextEmail = usage.email || entry.email;
  const nextPlan = usage.plan_type || entry.plan_type;
  const nextLabel = buildAccountLabel(nextEmail, nextPlan);
  const nextAlias = normalizeAlias(entry.alias);

  const changed =
    entry.label !== nextLabel
    || entry.alias !== nextAlias
    || entry.email !== nextEmail
    || entry.plan_type !== nextPlan;

  entry.label = nextLabel;
  if (nextAlias && nextAlias !== nextLabel) {
    entry.alias = nextAlias;
  } else if ("alias" in entry) {
    delete entry.alias;
  }
  entry.email = nextEmail;
  entry.plan_type = nextPlan;

  return changed;
}

function writeCodexAuthIfCurrentAccount(accountId: string, auth: CodexAuth): boolean {
  if (!existsSync(CODEX_AUTH)) return false;

  const currentAuth = loadCodexAuth();
  if (extractAccountIdFromAuth(currentAuth) !== accountId) return false;

  const currentChanged =
    currentAuth.tokens.id_token !== auth.tokens.id_token
    || currentAuth.tokens.access_token !== auth.tokens.access_token
    || currentAuth.tokens.refresh_token !== auth.tokens.refresh_token
    || currentAuth.tokens.account_id !== auth.tokens.account_id
    || currentAuth.last_refresh !== auth.last_refresh
    || currentAuth.auth_mode !== auth.auth_mode
    || currentAuth.OPENAI_API_KEY !== auth.OPENAI_API_KEY;

  if (!currentChanged) return false;

  writeCodexAuth(auth);
  return true;
}

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === "string") return error;
  return "Unknown error";
}

function normalizeEmailKey(email: string): string {
  return email.trim().toLowerCase();
}

function loadCodexAuthIfExists(): CodexAuth | null {
  if (!existsSync(CODEX_AUTH)) {
    return null;
  }
  return loadCodexAuth();
}

function restoreActiveAuth(previousAuth: CodexAuth | null): void {
  if (!previousAuth) {
    if (existsSync(CODEX_AUTH)) {
      rmSync(CODEX_AUTH, { force: true });
    }
    return;
  }

  writeCodexAuth(previousAuth);

  const pool = loadPool();
  const previousAccountId = extractAccountIdFromAuth(previousAuth);
  const previousIndex = pool.accounts.findIndex(
    (entry) => entry.account_id === previousAccountId || entry.auth.tokens.account_id === previousAuth.tokens.account_id,
  );

  if (previousIndex !== -1) {
    pool.active_index = previousIndex;
    savePool(pool);
  }
}

function collectKnownAccountEmails(pool: Pool, store: CredentialStore): string[] {
  return [
    ...pool.accounts.map((entry) => entry.email),
    ...Object.keys(store.accounts),
    ...Object.keys(store.pending),
  ];
}

function getStoredCredential(store: CredentialStore, email: string): StoredCredential | undefined {
  return store.accounts[normalizeEmailKey(email)];
}

export function shouldUseStoredCredentialRelogin(
  storedCredential: StoredCredential | undefined,
  options: ReloginOptions,
): boolean {
  return Boolean(storedCredential && !options.manualLogin && !options.deviceAuth);
}

export function shouldAttemptPasswordRecoveryAfterSignup(
  existingPending: PendingCredential | null | undefined,
  signupState: SignupStateSummary,
): boolean {
  void existingPending;
  return signupState.existingAccountPrompt;
}

export function shouldRecoverAfterPasswordVerificationError(error: unknown): boolean {
  const message = getErrorMessage(error);
  return /rejected the stored password|requires additional account setup|remained on an auth prompt/i.test(message);
}

interface AdultBirthDate {
  birthMonth: number;
  birthDay: number;
  birthYear: number;
}

export function generateRandomAdultBirthDate(
  now = new Date(),
  minAgeYears = 20,
  maxAgeYears = 45,
  pickOffsetMs: (maxExclusive: number) => number = randomInt,
): AdultBirthDate {
  if (!Number.isInteger(minAgeYears) || !Number.isInteger(maxAgeYears) || minAgeYears < 0 || maxAgeYears < minAgeYears) {
    throw new Error(`Invalid adult birth date range: min=${minAgeYears}, max=${maxAgeYears}`);
  }

  const latestBirthMs = Date.UTC(
    now.getUTCFullYear() - minAgeYears,
    now.getUTCMonth(),
    now.getUTCDate(),
    12,
    0,
    0,
    0,
  );
  const earliestBirthMs = Date.UTC(
    now.getUTCFullYear() - maxAgeYears,
    now.getUTCMonth(),
    now.getUTCDate(),
    12,
    0,
    0,
    0,
  );
  const spanMs = latestBirthMs - earliestBirthMs;
  if (!Number.isSafeInteger(spanMs) || spanMs < 0) {
    throw new Error(`Unsupported adult birth date range span: ${spanMs}`);
  }

  const chosenBirthMs = earliestBirthMs + (spanMs === 0 ? 0 : pickOffsetMs(spanMs + 1));
  const birthDate = new Date(chosenBirthMs);
  return {
    birthMonth: birthDate.getUTCMonth() + 1,
    birthDay: birthDate.getUTCDate(),
    birthYear: birthDate.getUTCFullYear(),
  };
}

export function resolveCredentialBirthDate(
  credential: Pick<StoredCredential, "birth_month" | "birth_day" | "birth_year"> | null | undefined,
  now = new Date(),
  minAgeYears = 20,
  maxAgeYears = 45,
  pickOffsetMs: (maxExclusive: number) => number = randomInt,
): AdultBirthDate {
  const birthMonth = credential?.birth_month;
  const birthDay = credential?.birth_day;
  const birthYear = credential?.birth_year;
  if (
    typeof birthMonth === "number"
    && Number.isInteger(birthMonth)
    && typeof birthDay === "number"
    && Number.isInteger(birthDay)
    && typeof birthYear === "number"
    && Number.isInteger(birthYear)
    && birthMonth >= 1
    && birthMonth <= 12
    && birthDay >= 1
    && birthDay <= 31
    && birthYear >= 1900
  ) {
    return {
      birthMonth,
      birthDay,
      birthYear,
    };
  }
  return generateRandomAdultBirthDate(now, minAgeYears, maxAgeYears, pickOffsetMs);
}

async function inspectPoolEntryByAccountId(accountId: string): Promise<{ pool: Pool; entry: AccountEntry; inspection: AccountInspection } | null> {
  const pool = loadPool();
  const entry = findPoolEntryByAccountId(pool, accountId);
  if (!entry) {
    return null;
  }

  const isCurrent = pool.accounts[pool.active_index]?.account_id === accountId;
  const inspection = await inspectAccount(entry, { persistIfCurrent: isCurrent });
  if (inspection.updated) {
    savePool(pool);
  }

  return { pool, entry, inspection };
}

function summarizeQuotaForCreate(result: CreateCommandResult): string {
  if (!result.inspection) {
    return "quota unavailable";
  }

  if (result.inspection.usage) {
    return formatCompactQuota(result.inspection.usage);
  }

  return `quota unavailable (${result.inspection.error ?? "unknown error"})`;
}

function runCodexCommand(args: string[]): void {
  const result = spawnSync(CODEX_BIN, args, { stdio: "inherit" });

  if (result.error) {
    die(`Failed to run "${CODEX_BIN} ${args.join(" ")}": ${getErrorMessage(result.error)}`);
  }

  if (result.signal) {
    die(`"${CODEX_BIN} ${args.join(" ")}" was interrupted by signal ${result.signal}.`);
  }

  if (typeof result.status === "number" && result.status !== 0) {
    die(`"${CODEX_BIN} ${args.join(" ")}" exited with status ${result.status}.`);
  }
}

function formatPercent(value: number): string {
  return Number.isInteger(value) ? `${value}` : value.toFixed(1).replace(/\.0$/, "");
}

function getQuotaLeft(window: UsageWindow | null | undefined): number | null {
  if (!window || typeof window.used_percent !== "number") return null;
  return Math.max(0, Math.min(100, 100 - window.used_percent));
}

function formatDuration(totalSeconds: number | null | undefined): string {
  if (typeof totalSeconds !== "number" || !Number.isFinite(totalSeconds)) return "unknown";

  let remaining = Math.max(0, Math.floor(totalSeconds));
  const units: Array<[label: string, seconds: number]> = [
    ["d", 86400],
    ["h", 3600],
    ["m", 60],
    ["s", 1],
  ];

  const parts: string[] = [];

  for (const [label, unitSeconds] of units) {
    if (remaining < unitSeconds && parts.length === 0 && label !== "s") continue;

    const amount = Math.floor(remaining / unitSeconds);
    if (amount > 0 || parts.length > 0 || label === "s") {
      parts.push(`${amount}${label}`);
      remaining -= amount * unitSeconds;
    }

    if (parts.length === 2) break;
  }

  return parts.join(" ");
}

function formatUsageWindow(window: UsageWindow | null | undefined, compact = false): string {
  const left = getQuotaLeft(window);
  if (left === null) return "unavailable";

  const resetText = typeof window?.reset_after_seconds === "number"
    ? compact
      ? `, ${formatDuration(window.reset_after_seconds)} reset`
      : ` (resets in ${formatDuration(window.reset_after_seconds)})`
    : "";

  return `${formatPercent(left)}% left${resetText}`;
}

function formatCredits(credits: UsageCredits | null | undefined, compact = false): string | null {
  if (!credits) return null;
  if (credits.unlimited) return "unlimited";
  if (!credits.has_credits) return compact ? null : "none";

  const details: string[] = [];
  if (typeof credits.balance === "number") details.push(`balance ${credits.balance}`);
  if (typeof credits.approx_local_messages === "number") details.push(`~${credits.approx_local_messages} local msgs`);
  if (typeof credits.approx_cloud_messages === "number") details.push(`~${credits.approx_cloud_messages} cloud msgs`);

  return details.length > 0 ? details.join(", ") : "available";
}

function formatCompactQuota(usage: UsageResponse): string {
  const parts: string[] = [];

  const fiveHour = usage.rate_limit?.primary_window;
  if (fiveHour) parts.push(`5h ${formatUsageWindow(fiveHour, true)}`);

  const weekly = usage.rate_limit?.secondary_window;
  if (weekly) parts.push(`week ${formatUsageWindow(weekly, true)}`);

  const credits = formatCredits(usage.credits, true);
  if (credits) parts.push(`credits ${credits}`);

  return parts.length > 0 ? parts.join(" | ") : "unavailable";
}

function hasUsableQuota(usage: UsageResponse): boolean {
  const primaryLeft = getQuotaLeft(usage.rate_limit?.primary_window);
  if (usage.rate_limit?.allowed && primaryLeft !== null && primaryLeft > 0) {
    return true;
  }

  return Boolean(usage.credits?.unlimited || usage.credits?.has_credits);
}

function describeQuotaBlocker(usage: UsageResponse): string {
  const primary = usage.rate_limit?.primary_window;
  const primaryLeft = getQuotaLeft(primary);

  if (primaryLeft !== null && primaryLeft <= 0) {
    const reset = typeof primary?.reset_after_seconds === "number"
      ? `, resets in ${formatDuration(primary.reset_after_seconds)}`
      : "";
    return `5h quota exhausted${reset}`;
  }

  if (usage.rate_limit?.limit_reached || usage.rate_limit?.allowed === false) {
    return "usage limit reached";
  }

  return "no usable quota";
}

function summarizeHttpError(body: string): string | null {
  try {
    const parsed = JSON.parse(body) as Record<string, unknown>;
    if (typeof parsed.error_description === "string") return parsed.error_description;
    if (typeof parsed.error === "string") return parsed.error;
    if (typeof parsed.message === "string") return parsed.message;

    const nestedError = parsed.error as Record<string, unknown> | undefined;
    if (nestedError && typeof nestedError === "object") {
      const code = typeof nestedError.code === "string" ? nestedError.code : null;
      const message = typeof nestedError.message === "string" ? nestedError.message : null;

      if (code === "refresh_token_reused") {
        return "refresh token already rotated; sign in again";
      }

      if (message && code) return `${code}: ${message}`;
      if (message) return message;
      if (code) return code;
    }
  } catch {
    // Ignore non-JSON bodies.
  }

  return null;
}

function buildHttpError(action: string, status: number, body: string): HttpError {
  const summary = summarizeHttpError(body);
  const message = summary ? `${action} failed (${status}): ${summary}` : `${action} failed (${status})`;
  return new HttpError(message, status, body);
}

async function fetchUsageOnce(auth: CodexAuth): Promise<UsageResponse> {
  const response = await fetch(WHAM_USAGE_URL, {
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${auth.tokens.access_token}`,
      "ChatGPT-Account-Id": extractAccountIdFromAuth(auth),
      "User-Agent": "codex-cli",
    },
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });

  const body = await response.text();
  if (!response.ok) {
    throw buildHttpError("Usage lookup", response.status, body);
  }

  return parseJson<UsageResponse>(body, "Usage lookup returned invalid JSON");
}

function extractAccountIdFromTokenSet(tokens: Pick<OAuthTokenResponse, "access_token" | "id_token">, fallback: string): string {
  if (tokens.access_token) {
    const accountId = extractAccountIdFromToken(tokens.access_token);
    if (accountId) return accountId;
  }

  if (tokens.id_token) {
    const accountId = extractAccountIdFromToken(tokens.id_token);
    if (accountId) return accountId;
  }

  return fallback;
}

async function refreshAuth(auth: CodexAuth): Promise<CodexAuth> {
  if (!auth.tokens.refresh_token) {
    throw new Error("No refresh token is available for this account.");
  }

  const body = new URLSearchParams({
    client_id: extractClientIdFromAuth(auth),
    grant_type: "refresh_token",
    refresh_token: auth.tokens.refresh_token,
  });

  const response = await fetch(OAUTH_TOKEN_URL, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/x-www-form-urlencoded",
      "User-Agent": "codex-rotate",
    },
    body,
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });

  const raw = await response.text();
  if (!response.ok) {
    throw buildHttpError("Token refresh", response.status, raw);
  }

  const tokenResponse = parseJson<OAuthTokenResponse>(raw, "Token refresh returned invalid JSON");
  if (!tokenResponse.access_token) {
    throw new Error("Token refresh response did not include an access token.");
  }

  const nextAuth: CodexAuth = {
    ...auth,
    tokens: {
      ...auth.tokens,
      access_token: tokenResponse.access_token,
      id_token: tokenResponse.id_token ?? auth.tokens.id_token,
      refresh_token: tokenResponse.refresh_token ?? auth.tokens.refresh_token,
      account_id: extractAccountIdFromTokenSet(tokenResponse, auth.tokens.account_id),
    },
    last_refresh: new Date().toISOString(),
  };

  return nextAuth;
}

function shouldRetryUsageWithRefresh(error: unknown): boolean {
  return error instanceof HttpError && error.status === 401;
}

async function fetchUsageWithRecovery(auth: CodexAuth): Promise<{ auth: CodexAuth; usage: UsageResponse; refreshed: boolean }> {
  let workingAuth = auth;
  let refreshed = false;

  if (isTokenExpired(workingAuth.tokens.access_token)) {
    workingAuth = await refreshAuth(workingAuth);
    refreshed = true;
  }

  try {
    const usage = await fetchUsageOnce(workingAuth);
    return { auth: workingAuth, usage, refreshed };
  } catch (error) {
    if (refreshed || !shouldRetryUsageWithRefresh(error)) {
      throw error;
    }

    workingAuth = await refreshAuth(workingAuth);
    const usage = await fetchUsageOnce(workingAuth);
    return { auth: workingAuth, usage, refreshed: true };
  }
}

async function inspectAccount(entry: AccountEntry, options?: { persistIfCurrent?: boolean }): Promise<AccountInspection> {
  try {
    const { auth, usage } = await fetchUsageWithRecovery(entry.auth);

    let updated = applyAuthToAccount(entry, auth);
    updated = applyUsageToAccount(entry, usage) || updated;
    if (options?.persistIfCurrent) {
      updated = writeCodexAuthIfCurrentAccount(entry.account_id, entry.auth) || updated;
    }

    return { usage, error: null, updated };
  } catch (error) {
    return { usage: null, error: getErrorMessage(error), updated: false };
  }
}

async function findNextUsableAccount(pool: Pool): Promise<{ candidate: RotationCandidate | null; reasons: string[]; dirty: boolean }> {
  const reasons: string[] = [];
  let dirty = false;

  for (let offset = 1; offset < pool.accounts.length; offset++) {
    const index = (pool.active_index + offset) % pool.accounts.length;
    const entry = pool.accounts[index]!;
    const inspection = await inspectAccount(entry);
    dirty = inspection.updated || dirty;

    if (!inspection.usage) {
      reasons.push(`${entry.label}: ${inspection.error ?? "unknown error"}`);
      continue;
    }

    if (!hasUsableQuota(inspection.usage)) {
      reasons.push(`${entry.label}: ${describeQuotaBlocker(inspection.usage)}`);
      continue;
    }

    return {
      candidate: { index, entry, inspection },
      reasons,
      dirty,
    };
  }

  const currentEntry = pool.accounts[pool.active_index];
  if (currentEntry) {
    const inspection = await inspectAccount(currentEntry, { persistIfCurrent: true });
    dirty = inspection.updated || dirty;

    if (!inspection.usage) {
      reasons.push(`${currentEntry.label}: ${inspection.error ?? "unknown error"}`);
    } else if (hasUsableQuota(inspection.usage)) {
      return {
        candidate: { index: pool.active_index, entry: currentEntry, inspection },
        reasons,
        dirty,
      };
    } else {
      reasons.push(`${currentEntry.label}: ${describeQuotaBlocker(inspection.usage)}`);
    }
  }

  return { candidate: null, reasons, dirty };
}

function formatAccountMarker(isActive: boolean): string {
  return isActive ? `${GREEN}▶${RESET}` : " ";
}

function formatShortAccountId(accountId: string): string {
  return accountId.length > 8 ? `${accountId.slice(0, 8)}…` : accountId;
}

function selectAccountsByEmail(pool: Pool, email: string): AccountSelection[] {
  const normalized = email.trim().toLowerCase();
  return pool.accounts
    .map((entry, index) => ({ entry, index }))
    .filter(({ entry }) => entry.email.trim().toLowerCase() === normalized);
}

function resolveAccountSelector(pool: Pool, selector: string): AccountSelection {
  const normalizedSelector = selector.trim();
  if (!normalizedSelector) {
    die("Account selector cannot be empty.");
  }

  const exactMatches = pool.accounts
    .map((entry, index) => ({ entry, index }))
    .filter(({ entry }) =>
      entry.label === normalizedSelector
      || entry.alias === normalizedSelector
      || entry.account_id === normalizedSelector
      || formatShortAccountId(entry.account_id) === normalizedSelector,
    );

  if (exactMatches.length === 1) {
    return exactMatches[0]!;
  }

  if (exactMatches.length > 1) {
    die(`Selector "${normalizedSelector}" matched multiple accounts. Use the full composite key.`);
  }

  const emailMatches = selectAccountsByEmail(pool, normalizedSelector);
  if (emailMatches.length === 1) {
    return emailMatches[0]!;
  }

  if (emailMatches.length > 1) {
    die(
      `Email "${normalizedSelector}" matched multiple accounts: `
      + `${emailMatches.map(({ entry }) => getAccountSelector(entry)).join(", ")}. `
      + `Use the full composite key.`,
    );
  }

  die(`Account "${normalizedSelector}" not found in pool.`);
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

  for (let index = 0; index < args.length; index++) {
    const arg = args[index]!;
    if (arg === "--profile") {
      profileName = args[index + 1];
      if (!profileName) {
        die("Usage: codex-rotate create [alias] [--profile <managed-name>] [--base-email <email-family>]");
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
        die("Usage: codex-rotate create [alias] [--profile <managed-name>] [--base-email <email-family>]");
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
    die("Usage: codex-rotate create [alias] [--profile <managed-name>] [--base-email <email-family>]");
  }

  return {
    alias: normalizeAlias(positionals[0]),
    profileName,
    baseEmail,
    source: "manual",
  };
}

async function runAutomatedCodexLogin(
  profileName: string,
  email: string,
  password: string,
  previousAuth: CodexAuth | null,
  expectedEmail: string | null,
  workflowRunStamp?: string,
  options?: {
    preferSignupRecovery?: boolean;
    fullName?: string;
    birthMonth?: number;
    birthDay?: number;
    birthYear?: number;
  },
): Promise<CodexAuth> {
  let allowSignupRecovery = options?.preferSignupRecovery === true;
  for (let attempt = 1; attempt <= CODEX_LOGIN_MAX_ATTEMPTS; attempt += 1) {
    let codexSession: CodexRotateAuthFlowSession | null = null;

    try {
      note(
        attempt === 1
          ? `Completing Codex login in managed profile "${profileName}".`
          : `Retrying Codex login in managed profile "${profileName}" (attempt ${attempt}/${CODEX_LOGIN_MAX_ATTEMPTS}).`,
      );
      for (let replayPass = 1; replayPass <= CODEX_LOGIN_MAX_REPLAY_PASSES; replayPass += 1) {
        const loginWorkflowRunStamp = workflowRunStamp
          ? `${workflowRunStamp}-codex-login-${attempt}-${replayPass}`
          : undefined;
        const loginResult = await runCodexBrowserLoginWorkflow(
          profileName,
          email,
          password,
          loginWorkflowRunStamp,
          {
            codexBin: CODEX_BIN,
            codexSession,
            preferSignupRecovery: allowSignupRecovery,
            fullName: options?.fullName,
            birthMonth: options?.birthMonth,
            birthDay: options?.birthDay,
            birthYear: options?.birthYear,
          },
        );
        const flow = readCodexRotateAuthFlowSummary(loginResult);
        codexSession = readCodexRotateAuthFlowSession(loginResult) ?? codexSession;
        const callbackComplete = flow.callback_complete === true;
        const success = flow.success === true;
        const currentUrl = typeof flow.current_url === "string" ? flow.current_url : null;
        const nextAction = typeof flow.next_action === "string" ? flow.next_action : null;
        const replayReason = typeof flow.replay_reason === "string" ? flow.replay_reason : null;
        const retryReason = typeof flow.retry_reason === "string" ? flow.retry_reason : null;
        const errorMessage = typeof flow.error_message === "string" && flow.error_message.trim()
          ? flow.error_message.trim()
          : null;

        if (replayReason && replayReason !== "auth_prompt") {
          allowSignupRecovery = false;
        }

        if (nextAction === "fail_invalid_credentials") {
          throw new Error(errorMessage ?? `OpenAI rejected the stored password for ${email}.`);
        }

        if (nextAction === "replay_auth_url" && replayPass < CODEX_LOGIN_MAX_REPLAY_PASSES) {
          const replayReasonLabel = replayReason
            ? replayReason.replace(/_/g, " ")
            : "the next auth step";
          note(
            `OpenAI still needs ${replayReasonLabel} for ${email}${currentUrl ? ` (${currentUrl})` : ""}. `
            + `Replaying the workflow-owned Codex auth session in managed profile "${profileName}" (${replayPass + 1}/${CODEX_LOGIN_MAX_REPLAY_PASSES}).`,
          );
          await sleep(1000);
          continue;
        }
        if (nextAction === "retry_attempt") {
          restoreActiveAuth(previousAuth);
          if (attempt < CODEX_LOGIN_MAX_ATTEMPTS) {
            const delayMs = CODEX_LOGIN_RETRY_DELAYS_MS[Math.min(attempt - 1, CODEX_LOGIN_RETRY_DELAYS_MS.length - 1)] ?? 30_000;
            const retryReasonLabel = retryReason
              ? retryReason.replace(/_/g, " ")
              : "needs another retry";
            note(
              `OpenAI ${retryReasonLabel} for ${email}${currentUrl ? ` (${currentUrl})` : ""}. `
              + `Waiting ${Math.round(delayMs / 1000)}s before retrying.`,
            );
            await sleep(delayMs);
            break;
          }
          throw new Error(errorMessage ?? `OpenAI could not complete the Codex login for ${email}.`);
        }
        if (!callbackComplete && !success) {
          throw new Error(
            errorMessage
            ?? `Codex browser login did not reach the callback for ${email}${currentUrl ? ` (${currentUrl})` : ""}.`,
          );
        }
        if (flow.codex_login_exit_ok === false) {
          throw new Error(
            `"codex login" did not exit cleanly for ${email}.`
            + `${flow.codex_login_stderr_tail ? `\n${flow.codex_login_stderr_tail}` : ""}`,
          );
        }

        const auth = loadCodexAuth();
        const loggedInEmail = extractEmailFromAuth(auth);

        if (expectedEmail && normalizeEmailKey(loggedInEmail) !== normalizeEmailKey(expectedEmail)) {
          restoreActiveAuth(previousAuth);
          throw new Error(`Expected ${expectedEmail}, but Codex logged into ${loggedInEmail}.`);
        }

        return auth;
      }
    } catch (error) {
      restoreActiveAuth(previousAuth);
      throw error;
    } finally {
      if (codexSession) {
        cancelCodexBrowserLoginSession(codexSession);
      }
    }
  }

  restoreActiveAuth(previousAuth);
  throw new Error(`Codex browser login exhausted all retry attempts for ${email}.`);
}

async function executeCreateFlow(options: CreateCommandOptions): Promise<CreateCommandResult> {
  const previousAuth = loadCodexAuthIfExists();
  const store = loadCredentialStore();
  const workflowMetadata = readLocalWorkflowMetadata(CREATE_DEFAULT_PROFILE_WORKFLOW);
  const profileName = resolveManagedProfileName({
    requestedProfileName: options.profileName,
    preferredProfileName: workflowMetadata.preferredProfileName,
    preferredProfileSource: workflowMetadata.filePath,
  });
  const pendingBaseEmailHintRaw = options.baseEmail
    ? null
    : selectPendingBaseEmailHintForProfile(store, profileName, options.alias ?? null);
  const storedBaseEmailHintRaw = selectStoredBaseEmailHint(store, profileName);
  const pendingBaseEmailHint = shouldUseDefaultCreateFamilyHint(pendingBaseEmailHintRaw)
    ? pendingBaseEmailHintRaw
    : null;
  const storedBaseEmailHint = shouldUseDefaultCreateFamilyHint(storedBaseEmailHintRaw)
    ? storedBaseEmailHintRaw
    : null;
  if (!options.baseEmail) {
    if (pendingBaseEmailHintRaw && !pendingBaseEmailHint) {
      note(`Ignoring legacy Gmail pending create family hint ${pendingBaseEmailHintRaw} and defaulting to Astronlab template.`);
    } else if (storedBaseEmailHintRaw && !storedBaseEmailHint) {
      note(`Ignoring legacy Gmail stored create family hint ${storedBaseEmailHintRaw} and defaulting to Astronlab template.`);
    }
  }
  const baseEmail = resolveCreateBaseEmail(
    options.baseEmail ?? null,
    pendingBaseEmailHint ?? storedBaseEmailHint,
  );
  const pool = loadPool();
  const familyKey = makeCredentialFamilyKey(profileName, baseEmail);
  const family = store.families[familyKey];
  const startedAt = new Date().toISOString();
  const existingPending = selectPendingCredentialForFamily(store, profileName, baseEmail, options.alias ?? null);
  const suffix = existingPending
    ? existingPending.suffix
    : computeNextGmailAliasSuffix(
      baseEmail,
      family?.next_suffix ?? 1,
      collectKnownAccountEmails(pool, store),
    );
  const createdEmail = existingPending?.email ?? buildGmailAliasEmail(baseEmail, suffix);
  const password = existingPending?.password ?? generatePassword();
  const birthDate = resolveCredentialBirthDate(existingPending);
  const pending: PendingCredential = existingPending
    ? {
      ...existingPending,
      alias: existingPending.alias ?? options.alias ?? null,
      birth_month: existingPending.birth_month ?? birthDate.birthMonth,
      birth_day: existingPending.birth_day ?? birthDate.birthDay,
      birth_year: existingPending.birth_year ?? birthDate.birthYear,
      updated_at: startedAt,
    }
    : {
      email: createdEmail,
      password,
      profile_name: profileName,
      base_email: baseEmail,
      suffix,
      selector: null,
      alias: options.alias ?? null,
      birth_month: birthDate.birthMonth,
      birth_day: birthDate.birthDay,
      birth_year: birthDate.birthYear,
      created_at: startedAt,
      updated_at: startedAt,
      started_at: startedAt,
    };

  store.pending[normalizeEmailKey(createdEmail)] = pending;
  saveCredentialStore(store);

  let shouldRestoreAuth = false;
  const openAiWorkflowRunStamp = startedAt;

  try {
    note(
      existingPending
        ? `Resuming ${createdEmail} in managed profile "${profileName}".`
        : `Creating ${createdEmail} in managed profile "${profileName}".`,
    );
    let auth: CodexAuth | null = null;
    auth = await runAutomatedCodexLogin(
      profileName,
      createdEmail,
      password,
      previousAuth,
      createdEmail,
      openAiWorkflowRunStamp,
      {
        preferSignupRecovery: true,
        fullName: "Dev Astronlab",
        birthMonth: pending.birth_month,
        birthDay: pending.birth_day,
        birthYear: pending.birth_year,
      },
    );

    shouldRestoreAuth = true;
    cmdAdd(options.alias);

    const inspected = await inspectPoolEntryByAccountId(extractAccountIdFromAuth(auth));
    if (!inspected) {
      throw new Error(`Created ${createdEmail}, but could not find the new account in the pool after login.`);
    }

    const { entry, inspection } = inspected;
    const updatedAt = new Date().toISOString();
    delete store.pending[normalizeEmailKey(createdEmail)];
    store.accounts[normalizeEmailKey(createdEmail)] = {
      email: createdEmail,
      password,
      profile_name: profileName,
      base_email: baseEmail,
      suffix,
      selector: entry.label,
      alias: entry.alias ?? options.alias ?? null,
      birth_month: pending.birth_month,
      birth_day: pending.birth_day,
      birth_year: pending.birth_year,
      created_at: pending.created_at,
      updated_at: updatedAt,
    };
    store.families[familyKey] = {
      profile_name: profileName,
      base_email: baseEmail,
      next_suffix: Math.max(family?.next_suffix ?? 1, suffix + 1),
      created_at: family?.created_at ?? startedAt,
      updated_at: updatedAt,
      last_created_email: createdEmail,
    };
    saveCredentialStore(store);

    if (options.requireUsableQuota && (!inspection.usage || !hasUsableQuota(inspection.usage))) {
      restoreActiveAuth(previousAuth);
      shouldRestoreAuth = false;
      const reason = inspection.usage ? describeQuotaBlocker(inspection.usage) : (inspection.error ?? "quota unavailable");
      throw new Error(`Created ${entry.label}, but it does not have usable quota (${reason}).`);
    }

    shouldRestoreAuth = false;
    return {
      entry,
      inspection,
      profileName,
      baseEmail,
      createdEmail,
    };
  } catch (error) {
    if (shouldRestoreAuth) {
      restoreActiveAuth(previousAuth);
    }
    saveCredentialStore(store);
    throw error;
  }
}

// ── Commands ───────────────────────────────────────────────────────────────────

function cmdAdd(alias?: string): void {
  const auth = loadCodexAuth();
  const pool = loadPool();
  const accountId = extractAccountIdFromAuth(auth);
  const email = extractEmailFromAuth(auth);
  const planType = extractPlanFromAuth(auth);
  const label = buildAccountLabel(email, planType);
  const nextAlias = normalizeAlias(alias);

  const existingIndex = pool.accounts.findIndex((account) => account.label === label);
  const duplicateIndex = pool.accounts.findIndex((account) => account.account_id === accountId || account.auth.tokens.account_id === accountId);

  if (existingIndex !== -1) {
    const entry = pool.accounts[existingIndex]!;
    const previousAccountId = entry.account_id;
    applyAuthToAccount(entry, auth);
    if (nextAlias && nextAlias !== entry.label) {
      entry.alias = nextAlias;
    } else if (!nextAlias && entry.alias === entry.label) {
      delete entry.alias;
    }

    if (previousAccountId !== accountId) {
      warn(
        `Composite key "${label}" was previously tied to ${formatShortAccountId(previousAccountId)}. `
        + `Overwriting it with ${formatShortAccountId(accountId)}.`,
      );
    }

    if (duplicateIndex !== -1 && duplicateIndex !== existingIndex) {
      const removed = pool.accounts.splice(duplicateIndex, 1)[0]!;
      if (duplicateIndex < existingIndex) {
        pool.active_index = Math.max(0, pool.active_index - 1);
      }
      warn(`Removed stale row "${getAccountSummary(removed)}" for the same account.`);
    }

    pool.active_index = pool.accounts.findIndex((account) => account.label === label);
    savePool(pool);
    info(`Updated account "${label}" (${entry.email}${entry.alias ? `, alias ${entry.alias}` : ""})`);
    return;
  }

  if (duplicateIndex !== -1) {
    const entry = pool.accounts[duplicateIndex]!;
    const previousLabel = entry.label;

    applyAuthToAccount(entry, auth);
    if (nextAlias && nextAlias !== entry.label) {
      entry.alias = nextAlias;
    } else if (!nextAlias && entry.alias === entry.label) {
      delete entry.alias;
    }

    if (previousLabel !== entry.label) {
      warn(`Account moved from "${previousLabel}" to "${entry.label}". Updating the existing row.`);
    }

    pool.active_index = duplicateIndex;
    savePool(pool);
    info(`Updated account "${entry.label}" (${entry.email}${entry.alias ? `, alias ${entry.alias}` : ""})`);
    return;
  }

  const entry: AccountEntry = {
    label,
    ...(nextAlias && nextAlias !== label ? { alias: nextAlias } : {}),
    email,
    account_id: accountId,
    plan_type: planType,
    auth,
    added_at: new Date().toISOString(),
  };

  pool.accounts.push(entry);
  pool.active_index = pool.accounts.length - 1;
  savePool(pool);
  info(
    `Added account "${label}" (${entry.email}, ${entry.plan_type}${entry.alias ? `, alias ${entry.alias}` : ""}) `
    + `— pool now has ${pool.accounts.length} account(s)`,
  );
}

async function cmdCreate(options: CreateCommandOptions): Promise<CreateCommandResult> {
  const result = await executeCreateFlow(options);
  const quotaSummary = summarizeQuotaForCreate(result);
  info(
    `Created ${result.entry.label} via "${result.profileName}" `
    + `from ${result.baseEmail}.`,
  );
  note(`Quota: ${quotaSummary}`);
  return result;
}

async function cmdNext(): Promise<void> {
  const pool = loadPool();
  if (pool.accounts.length === 0) die("No accounts in pool. Run: codex-rotate add");

  let dirty = normalizePoolEntries(pool);
  dirty = syncPoolActiveAccountFromCodex(pool) || dirty;

  const previousIndex = pool.active_index;
  const previous = pool.accounts[previousIndex]!;
  const { candidate, reasons, dirty: candidateDirty } = await findNextUsableAccount(pool);
  dirty = candidateDirty || dirty;

  if (!candidate) {
    if (dirty) savePool(pool);
    note("No account has usable quota. Creating a new account in the managed browser profile.");
    try {
      await cmdCreate({
        source: "next",
        requireUsableQuota: true,
      });
      return;
    } catch (error) {
      die(
        `All accounts are exhausted or unavailable.\n${reasons.map((reason) => `  - ${reason}`).join("\n")}\n`
        + `  - auto-create: ${getErrorMessage(error)}`,
      );
    }
  }

  if (candidate.index === previousIndex) {
    if (dirty) savePool(pool);
    const quotaSummary = candidate.inspection.usage ? formatCompactQuota(candidate.inspection.usage) : "quota unavailable";
    console.log(
      `${GREEN}⟳${RESET} Stayed on ${BOLD}${getAccountSelector(candidate.entry)}${RESET} (${CYAN}${candidate.entry.email}${RESET}, ${candidate.entry.plan_type})\n`
      + `${DIM}  No other account has usable quota · [${pool.active_index + 1}/${pool.accounts.length}] · ${quotaSummary}${RESET}`,
    );
    return;
  }

  pool.active_index = candidate.index;
  writeCodexAuth(candidate.entry.auth);
  savePool(pool);

  const quotaSummary = candidate.inspection.usage ? formatCompactQuota(candidate.inspection.usage) : "quota unavailable";
  console.log(
    `${GREEN}⟳${RESET} Rotated: ${DIM}${getAccountSelector(previous)}${RESET} (${previous.email}) → ${BOLD}${getAccountSelector(candidate.entry)}${RESET} (${CYAN}${candidate.entry.email}${RESET}, ${candidate.entry.plan_type})\n`
    + `${DIM}  [${pool.active_index + 1}/${pool.accounts.length}] · ${quotaSummary}${RESET}`,
  );
}

function cmdPrev(): void {
  const pool = loadPool();
  if (pool.accounts.length === 0) die("No accounts in pool. Run: codex-rotate add");
  if (pool.accounts.length === 1) die("Only 1 account in pool. Add more with: codex-rotate add");

  syncPoolActiveAccountFromCodex(pool);

  const previousIndex = pool.active_index;
  pool.active_index = (pool.active_index - 1 + pool.accounts.length) % pool.accounts.length;
  const next = pool.accounts[pool.active_index]!;

  writeCodexAuth(next.auth);
  savePool(pool);

  const previous = pool.accounts[previousIndex]!;
  console.log(
    `${GREEN}⟳${RESET} Rotated: ${DIM}${getAccountSelector(previous)}${RESET} (${previous.email}) → ${BOLD}${getAccountSelector(next)}${RESET} (${CYAN}${next.email}${RESET}, ${next.plan_type})\n`
    + `${DIM}  [${pool.active_index + 1}/${pool.accounts.length}]${RESET}`,
  );
}

async function cmdList(): Promise<void> {
  const pool = loadPool();
  let dirty = normalizePoolEntries(pool);
  let usableCount = 0;
  let exhaustedCount = 0;
  let unavailableCount = 0;
  if (pool.accounts.length === 0) {
    warn("No accounts in pool. Add one with: codex-rotate add");
    return;
  }

  dirty = syncPoolActiveAccountFromCodex(pool) || dirty;

  console.log(`\n${BOLD}Codex OAuth Account Pool${RESET} (${pool.accounts.length} account(s))\n`);

  for (let index = 0; index < pool.accounts.length; index++) {
    const entry = pool.accounts[index]!;
    const isActive = index === pool.active_index;
    const inspection = await inspectAccount(entry, { persistIfCurrent: isActive });
    dirty = inspection.updated || dirty;
    const label = isActive ? `${BOLD}${getAccountSelector(entry)}${RESET}` : getAccountSelector(entry);

    console.log(
      `  ${formatAccountMarker(isActive)} ${label}  ${CYAN}${entry.email}${RESET}  ${DIM}${entry.plan_type}${RESET}  ${DIM}${formatShortAccountId(entry.account_id)}${RESET}`,
    );
    if (entry.alias) {
      console.log(`    ${DIM}alias${RESET}  ${entry.alias}`);
    }

    if (inspection.usage) {
      if (hasUsableQuota(inspection.usage)) {
        usableCount += 1;
      } else {
        exhaustedCount += 1;
      }
      console.log(`    ${DIM}quota${RESET}  ${formatCompactQuota(inspection.usage)}`);
    } else {
      unavailableCount += 1;
      console.log(`    ${DIM}quota${RESET}  unavailable (${inspection.error ?? "unknown error"})`);
    }
  }

  if (dirty) savePool(pool);

  if (usableCount === 0) {
    const details: string[] = [];
    if (exhaustedCount > 0) details.push(`${exhaustedCount} exhausted`);
    if (unavailableCount > 0) details.push(`${unavailableCount} unavailable`);
    warn(`All accounts are exhausted or unavailable${details.length > 0 ? ` (${details.join(", ")})` : ""}.`);
  }

  console.log();
}

async function cmdStatus(): Promise<void> {
  const pool = loadPool();
  let dirty = normalizePoolEntries(pool);
  dirty = syncPoolActiveAccountFromCodex(pool) || dirty;
  const auth = existsSync(CODEX_AUTH) ? loadCodexAuth() : null;

  console.log(`\n${BOLD}Codex Rotate Status${RESET}\n`);

  if (auth) {
    const email = extractEmailFromAuth(auth);
    const plan = extractPlanFromAuth(auth);
    console.log(`  ${BOLD}Active in Codex:${RESET}  ${CYAN}${email}${RESET}  (${plan})`);
    console.log(`  ${BOLD}Account ID:${RESET}       ${extractAccountIdFromAuth(auth)}`);
    console.log(`  ${BOLD}Last refresh:${RESET}     ${auth.last_refresh}`);

    const matchingEntry = findPoolEntryByAccountId(pool, extractAccountIdFromAuth(auth));
    if (matchingEntry) {
      const inspection = await inspectAccount(matchingEntry, { persistIfCurrent: true });
      dirty = inspection.updated || dirty;

      if (inspection.usage) {
        console.log(`  ${BOLD}Quota (5h):${RESET}       ${formatUsageWindow(inspection.usage.rate_limit?.primary_window)}`);
        console.log(`  ${BOLD}Quota (week):${RESET}     ${formatUsageWindow(inspection.usage.rate_limit?.secondary_window)}`);
        if (inspection.usage.code_review_rate_limit?.primary_window) {
          console.log(`  ${BOLD}Code review:${RESET}      ${formatUsageWindow(inspection.usage.code_review_rate_limit.primary_window)}`);
        }

        const credits = formatCredits(inspection.usage.credits);
        if (credits) {
          console.log(`  ${BOLD}Credits:${RESET}          ${credits}`);
        }
      } else {
        console.log(`  ${BOLD}Quota:${RESET}            unavailable (${inspection.error ?? "unknown error"})`);
      }
    } else {
      try {
        const { auth: refreshedAuth, usage } = await fetchUsageWithRecovery(auth);
        if (refreshedAuth.tokens.access_token !== auth.tokens.access_token || refreshedAuth.last_refresh !== auth.last_refresh) {
          writeCodexAuth(refreshedAuth);
        }

        console.log(`  ${BOLD}Quota (5h):${RESET}       ${formatUsageWindow(usage.rate_limit?.primary_window)}`);
        console.log(`  ${BOLD}Quota (week):${RESET}     ${formatUsageWindow(usage.rate_limit?.secondary_window)}`);
        if (usage.code_review_rate_limit?.primary_window) {
          console.log(`  ${BOLD}Code review:${RESET}      ${formatUsageWindow(usage.code_review_rate_limit.primary_window)}`);
        }

        const credits = formatCredits(usage.credits);
        if (credits) {
          console.log(`  ${BOLD}Credits:${RESET}          ${credits}`);
        }
      } catch (error) {
        console.log(`  ${BOLD}Quota:${RESET}            unavailable (${getErrorMessage(error)})`);
      }
    }
  } else {
    warn("No Codex auth file found.");
  }

  console.log(`\n  ${BOLD}Pool file:${RESET}        ${POOL_FILE}`);
  console.log(`  ${BOLD}Pool size:${RESET}        ${pool.accounts.length} account(s)`);

  if (pool.accounts.length > 0) {
    const active = pool.accounts[pool.active_index]!;
    console.log(`  ${BOLD}Active slot:${RESET}      ${active.label} [${pool.active_index + 1}/${pool.accounts.length}]`);
    if (active.alias) {
      console.log(`  ${BOLD}Active alias:${RESET}     ${active.alias}`);
    }
  }

  if (dirty) savePool(pool);
  console.log();
}

async function cmdRelogin(selector: string, options: ReloginOptions): Promise<void> {
  const pool = loadPool();
  const selection = resolveAccountSelector(pool, selector);
  const existing = selection.entry;
  const expectedEmail = existing.email;
  const store = loadCredentialStore();
  const storedCredential = getStoredCredential(store, expectedEmail);
  const shouldUseStoredCredentials = shouldUseStoredCredentialRelogin(storedCredential, options);

  if (shouldUseStoredCredentials && storedCredential) {
    note(`Using stored credentials for ${expectedEmail} in managed profile "${storedCredential.profile_name}".`);
    const previousAuth = loadCodexAuthIfExists();
    await runAutomatedCodexLogin(
      storedCredential.profile_name,
      storedCredential.email,
      storedCredential.password,
      previousAuth,
      options.allowEmailChange ? null : expectedEmail,
    );
    cmdAdd(existing.alias);

    const auth = loadCodexAuth();
    const updated = getStoredCredential(store, storedCredential.email);
    const inspected = await inspectPoolEntryByAccountId(extractAccountIdFromAuth(auth));
    if (updated && inspected) {
      store.accounts[normalizeEmailKey(storedCredential.email)] = {
        ...updated,
        selector: inspected.entry.label,
        alias: inspected.entry.alias ?? existing.alias ?? null,
        updated_at: new Date().toISOString(),
      };
      saveCredentialStore(store);
    }
    return;
  }

  if (!storedCredential && !options.manualLogin && !options.deviceAuth) {
    note(`No stored credentials were found for ${expectedEmail}. Falling back to manual browser login.`);
  }

  if (options.logoutFirst && existsSync(CODEX_AUTH)) {
    note(`Running "${CODEX_BIN} logout" before re-login.`);
    runCodexCommand(["logout"]);
  }

  note(`Complete the login flow for ${expectedEmail}.`);

  const loginArgs = ["login"];
  if (options.deviceAuth) {
    loginArgs.push("--device-auth");
  }

  runCodexCommand(loginArgs);

  const auth = loadCodexAuth();
  const loggedInEmail = extractEmailFromAuth(auth);

  if (loggedInEmail !== expectedEmail && !options.allowEmailChange) {
    die(
      `Logged into ${loggedInEmail}, but "${getAccountSummary(existing)}" expects ${expectedEmail}. `
      + `The pool was not updated. Re-run with --allow-email-change if you want to replace it.`,
    );
  }

  cmdAdd(existing.alias);
}

function cmdRemove(selector: string): void {
  if (!selector) die("Usage: codex-rotate remove <selector>");

  const pool = loadPool();
  const { entry, index } = resolveAccountSelector(pool, selector);

  const removed = pool.accounts.splice(index, 1)[0]!;

  if (pool.accounts.length === 0 || pool.active_index >= pool.accounts.length) {
    pool.active_index = 0;
  }

  savePool(pool);
  info(`Removed "${getAccountSummary(entry)}" (${removed.email}). Pool now has ${pool.accounts.length} account(s).`);
}

function cmdHelp(): void {
  console.log(`
${BOLD}codex-rotate${RESET} — Rotate Codex CLI OAuth tokens across multiple ChatGPT accounts.

${BOLD}USAGE${RESET}
  codex-rotate <command> [args]

${BOLD}COMMANDS${RESET}
  ${CYAN}add${RESET} [alias]      Snapshot current ~/.codex/auth.json into the pool
  ${CYAN}create${RESET} [alias]   Resume the oldest unfinished account or create a new one
  ${CYAN}next${RESET}             Swap to the next account with usable quota
  ${CYAN}prev${RESET}             Swap to the previous account
  ${CYAN}list${RESET}             Show all accounts with live quota info
  ${CYAN}status${RESET}           Show the current active account info and quota
  ${CYAN}relogin${RESET} <selector> Repair that account in one step
  ${CYAN}remove${RESET} <selector>  Remove an account from the pool
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
  Default behavior uses stored credentials when available
  ${DIM}--manual-login${RESET}       Force the legacy manual browser login flow
  ${DIM}--device-auth${RESET}        Use the device auth flow instead of browser login
  ${DIM}--keep-session${RESET}       Skip the pre-login ${DIM}codex logout${RESET} for manual relogins
  ${DIM}--allow-email-change${RESET} Replace the selected account even if the signed-in email changed

${BOLD}CREATE FLAGS${RESET}
  ${DIM}--profile <managed-name>${RESET} Choose the fast-browser managed profile
  ${DIM}--base-email <email-family>${RESET} Override the create email family for this run
  ${DIM}(omitted values default to workflow preferred_profile "dev-1" and discover email from that profile)${RESET}

${BOLD}FILES${RESET}
  Pool:  ${DIM}~/.codex-rotate/accounts.json${RESET}
  Creds: ${DIM}~/.codex-rotate/credentials.json${RESET}
  Auth:  ${DIM}~/.codex/auth.json${RESET}
`);
}

// ── Main ───────────────────────────────────────────────────────────────────────

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
