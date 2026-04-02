import { createHash, randomBytes } from "node:crypto";
import {
  spawn,
  spawnSync,
  type ChildProcessWithoutNullStreams,
  type SpawnSyncReturns,
} from "node:child_process";
import {
  chmodSync,
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readdirSync,
  readFileSync,
  renameSync,
  rmSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath, pathToFileURL } from "node:url";

const MODULE_DIR = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(MODULE_DIR, "..", "..");
const ROTATE_HOME = join(homedir(), ".codex-rotate");
const FAST_BROWSER_HOME = join(homedir(), ".fast-browser");
const FAST_BROWSER_PROFILES_HOME = join(FAST_BROWSER_HOME, "profiles");
const FAST_BROWSER_DAEMON_DIR = join(FAST_BROWSER_HOME, "daemon");
const FAST_BROWSER_SCRIPT_DEFAULT = resolve(
  REPO_ROOT,
  "..",
  "ai-rules",
  "skills",
  "fast-browser",
  "scripts",
  "fast-browser.mjs",
);

const FAST_BROWSER_SCRIPT = process.env.CODEX_ROTATE_FAST_BROWSER_SCRIPT ?? FAST_BROWSER_SCRIPT_DEFAULT;
const FAST_BROWSER_RUNTIME = process.env.CODEX_ROTATE_FAST_BROWSER_RUNTIME
  ?? (process.versions.bun ? "node" : process.execPath);
const FAST_BROWSER_PLAYWRIGHT_MODULE = join(REPO_ROOT, "node_modules", "playwright");
const LOGIN_CAPTURE_TIMEOUT_MS = 15_000;

const CODEX_ROTATE_ACCOUNT_FLOW_WORKFLOW = "local:web:auth.openai.com:codex-rotate-account-flow";
const GMAIL_CAPTURE_WORKFLOW = "global:web:mail.google.com:capture-active-account-email";
export const CODEX_ROTATE_OPENAI_TEMP_RUNTIME_KEY = "openai-account-runtime";

export const CREDENTIALS_FILE = join(ROTATE_HOME, "credentials.json");
export const FAST_BROWSER_WORKFLOWS_ROOT = join(REPO_ROOT, ".fast-browser", "workflows");
const FAST_BROWSER_MANAGED_PROFILE_ARCHIVE_ROOT = join(FAST_BROWSER_PROFILES_HOME, "_archive");

export interface CredentialFamily {
  profile_name: string;
  base_email: string;
  next_suffix: number;
  created_at: string;
  updated_at: string;
  last_created_email: string | null;
}

export interface StoredCredential {
  email: string;
  password: string;
  profile_name: string;
  base_email: string;
  suffix: number;
  selector: string | null;
  alias: string | null;
  birth_month?: number;
  birth_day?: number;
  birth_year?: number;
  created_at: string;
  updated_at: string;
}

export interface PendingCredential extends StoredCredential {
  started_at: string;
}

export interface CredentialStore {
  version: 1;
  families: Record<string, CredentialFamily>;
  accounts: Record<string, StoredCredential>;
  pending: Record<string, PendingCredential>;
}

interface LegacyCredentialStore {
  version?: unknown;
  defaults?: unknown;
  families?: unknown;
  accounts?: unknown;
  pending?: unknown;
}

export interface LocalWorkflowMetadata {
  filePath: string;
  preferredProfileName: string | null;
  preferredEmail: string | null;
}

interface ManagedProfileEntry {
  name: string;
  profileDirectory: string;
  profileMode: string;
  type: string;
  userDataDir: string;
}

interface ManagedProfilesPayload {
  default: string | null;
  profiles: ManagedProfileEntry[];
}

interface ChromeProfileAccountInfoEntry {
  email?: unknown;
}

interface ChromeProfilePreferences {
  account_info?: ChromeProfileAccountInfoEntry[];
}

interface SystemChromeProfileEntry {
  directory: string;
  name: string;
}

interface SystemChromeProfileMatch {
  directory: string;
  name: string;
  emails: string[];
  matchedEmail: string;
  score: number;
}

export interface ManagedProfilesInspection {
  chromeUserDataDir: string;
  profiles: Array<Record<string, unknown>>;
  managedProfilesRoot: string;
  managedProfiles: ManagedProfilesPayload;
}

interface FastBrowserStepState {
  action?: Record<string, unknown>;
}

interface FastBrowserState {
  steps?: Record<string, FastBrowserStepState>;
}

interface FastBrowserCommandResult {
  status: number | null;
  signal: NodeJS.Signals | null;
  stdout: string;
  stderr: string;
}

export interface FastBrowserPause {
  relay_url?: string | null;
  reason?: string | null;
}

export interface FastBrowserRunResult {
  ok: boolean;
  status?: string;
  state?: FastBrowserState;
  pause?: FastBrowserPause | null;
  finalUrl?: string | null;
}

export function buildTemporaryWorkflowProfileName(workflowRunStamp: string, key: string): string {
  const normalizedStamp = String(workflowRunStamp || "run")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 24);
  const normalizedKey = String(key || "temp")
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 24);
  return `workflow-run-${normalizedStamp}-${createHash("sha256").update(normalizedKey).digest("hex").slice(0, 12)}`;
}

export function buildCodexRotateOpenAiTempProfileName(workflowRunStamp: string): string {
  return buildTemporaryWorkflowProfileName(workflowRunStamp, CODEX_ROTATE_OPENAI_TEMP_RUNTIME_KEY);
}

interface FastBrowserDaemonRunResponse {
  ok: boolean;
  result?: FastBrowserRunResult;
  error?: {
    message?: string;
  };
}

const FAST_BROWSER_DAEMON_TIMEOUT_PATTERN = /Timed out waiting for fast-browser daemon response from\s+(.+?\.sock)/i;
const FAST_BROWSER_EVENT_PREFIX = "__FAST_BROWSER_EVENT__";

interface FastBrowserProgressEvent {
  time?: unknown;
  workflow?: unknown;
  stepId?: unknown;
  phase?: unknown;
  status?: unknown;
  message?: unknown;
  details?: unknown;
}

export interface BrowserLoginSession {
  child: ChildProcessWithoutNullStreams;
  authUrl: string;
  callbackUrl: string;
  callbackPort: number;
  buffers: {
    stdout: string;
    stderr: string;
  };
  exitPromise: Promise<CommandExit>;
  browserCaptureScriptPath: string | null;
  browserCaptureOutputPath: string | null;
  browserCaptureShimDir: string | null;
}

interface CommandExit {
  code: number | null;
  signal: NodeJS.Signals | null;
}

export interface ParsedCodexBrowserLoginCapture {
  authUrl: string;
  callbackUrl: string;
  callbackPort: number;
}

type BrowserCaptureMode = "none" | "headless_tmp" | "headed_tmp";

function parseJson<T>(raw: string, fallbackMessage: string): T {
  try {
    return JSON.parse(raw) as T;
  } catch {
    throw new Error(fallbackMessage);
  }
}

function ensureRotateDir(): void {
  if (!existsSync(ROTATE_HOME)) {
    mkdirSync(ROTATE_HOME, { recursive: true });
  }
}

function writeExecutableScript(filePath: string, contents: string): void {
  writeFileSync(filePath, contents);
  chmodSync(filePath, 0o700);
}

function createBrowserCaptureShim(): { scriptPath: string; outputPath: string; shimDir: string } {
  ensureRotateDir();
  const uniqueId = `${process.pid}-${Date.now()}-${randomBytes(4).toString("hex")}`;
  const shimDir = join(ROTATE_HOME, `codex-login-browser-shim-${uniqueId}`);
  mkdirSync(shimDir, { recursive: true });
  const scriptPath = join(shimDir, "browser-capture.js");
  const outputPath = join(shimDir, "browser-capture.log");
  const script = `#!/usr/bin/env node
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const outputPath = process.env.CODEX_ROTATE_BROWSER_CAPTURE_OUT || "";
const playwrightRequirePath = process.env.CODEX_ROTATE_PLAYWRIGHT_REQUIRE || "playwright";
const viewerBin = process.env.FAST_BROWSER_CHROME_BIN || "";
const captureMode = String(process.env.CODEX_ROTATE_BROWSER_CAPTURE_MODE || "none").trim().toLowerCase();
const captureUrl = process.argv.slice(2).join(" ").trim();
let context = null;
let viewerRoot = null;
let closed = false;
let keepAliveTimer = null;

function isParentAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 1) {
    return false;
  }
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return !(error && typeof error === "object" && "code" in error && error.code === "ESRCH");
  }
}

async function closeAndExit(code) {
  if (closed) {
    return;
  }
  closed = true;
  if (keepAliveTimer) {
    clearInterval(keepAliveTimer);
    keepAliveTimer = null;
  }
  try {
    if (context) {
      await context.close();
    }
  } catch {}
  try {
    if (viewerRoot) {
      fs.rmSync(viewerRoot, { recursive: true, force: true });
    }
  } catch {}
  process.exit(code);
}

async function launchCaptureViewer() {
  if (!captureUrl || captureMode === "none") {
    return;
  }
  try {
    const playwright = require(playwrightRequirePath);
    viewerRoot = fs.mkdtempSync(path.join(os.tmpdir(), "codex-login-capture-"));
    const userDataDir = path.join(viewerRoot, "profile");
    fs.mkdirSync(userDataDir, { recursive: true });
    context = await playwright.chromium.launchPersistentContext(userDataDir, {
      headless: captureMode !== "headed_tmp",
      executablePath: viewerBin || undefined,
      viewport: null,
      ignoreHTTPSErrors: true,
      args: captureMode === "headed_tmp" ? ["--new-window", "--window-size=1120,840"] : [],
    });
    if (captureMode === "headless_tmp") {
      const page = context.pages()[0] || await context.newPage();
      const notice = [
        "<title>Codex Login Capture</title>",
        "<h1>Captured</h1>",
        "<p>codex-rotate consumed the browser launch request in an isolated temporary browser.</p>",
      ].join("");
      await page.goto("data:text/html;charset=utf-8," + encodeURIComponent(notice), { waitUntil: "domcontentloaded" }).catch(() => {});
    } else if (captureMode === "headed_tmp") {
      const page = context.pages()[0] || await context.newPage();
      const notice = [
        "<title>Codex Login Capture</title>",
        "<style>body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;padding:24px;line-height:1.4}code{word-break:break-all}</style>",
        "<h1>Codex login URL captured</h1>",
        "<p>This temporary browser window exists only to consume the browser-launch request safely.</p>",
        "<p>codex-rotate continues the real login in the managed profile.</p>",
        captureUrl ? "<details><summary>Captured URL</summary><code>" + captureUrl.replace(/[&<>]/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[char])) + "</code></details>" : "",
      ].join("");
      await page.goto("data:text/html;charset=utf-8," + encodeURIComponent(notice), { waitUntil: "domcontentloaded" }).catch(() => {});
    }
  } catch {}
}

if (outputPath && captureUrl) {
  fs.appendFileSync(outputPath, captureUrl + "\\n");
}

(async () => {
  await launchCaptureViewer();
  keepAliveTimer = setInterval(() => {
    if (!isParentAlive(process.ppid)) {
      void closeAndExit(0);
    }
  }, 500);
})();

process.on("SIGTERM", () => {
  void closeAndExit(0);
});
process.on("SIGINT", () => {
  void closeAndExit(0);
});
process.on("SIGHUP", () => {
  void closeAndExit(0);
});
process.on("uncaughtException", () => {
  void closeAndExit(1);
});
process.on("unhandledRejection", () => {
  void closeAndExit(1);
});

setTimeout(() => {
  void closeAndExit(0);
}, 15 * 60 * 1000);

if (!captureUrl) {
  setTimeout(() => {
    void closeAndExit(0);
  }, 1000);
}
`;
  writeExecutableScript(scriptPath, script);

  const openerShim = `#!/usr/bin/env bash
set -euo pipefail
capture_script="\${CODEX_ROTATE_BROWSER_CAPTURE_SCRIPT:-}"
if [[ -z "$capture_script" ]]; then
  exit 0
fi
url=""
for arg in "$@"; do
  case "$arg" in
    http://*|https://*)
      url="$arg"
      ;;
  esac
done
if [[ -n "$url" ]]; then
  exec "$capture_script" "$url"
fi
exec "$capture_script" "$@"
`;

  for (const name of ["open", "xdg-open", "x-www-browser", "sensible-browser"]) {
    writeExecutableScript(join(shimDir, name), openerShim);
  }

  return { scriptPath, outputPath, shimDir };
}

function cleanupBrowserCaptureShim(
  scriptPath: string | null | undefined,
  outputPath: string | null | undefined,
  shimDir: string | null | undefined,
): void {
  for (const targetPath of [scriptPath, outputPath]) {
    if (!targetPath) {
      continue;
    }
    try {
      unlinkSync(targetPath);
    } catch {}
  }
  if (shimDir) {
    try {
      rmSync(shimDir, { recursive: true, force: true });
    } catch {}
  }
}

function readBrowserCaptureOutput(outputPath: string | null | undefined): string {
  if (!outputPath || !existsSync(outputPath)) {
    return "";
  }
  try {
    return readFileSync(outputPath, "utf8");
  } catch {
    return "";
  }
}

function writePrivateJson(filePath: string, value: unknown): void {
  ensureRotateDir();
  writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
  chmodSync(filePath, 0o600);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

export function normalizeCredentialStore(raw: LegacyCredentialStore | null | undefined): CredentialStore {
  return {
    version: 1,
    families: isRecord(raw?.families) ? raw.families as Record<string, CredentialFamily> : {},
    accounts: isRecord(raw?.accounts) ? raw.accounts as Record<string, StoredCredential> : {},
    pending: isRecord(raw?.pending) ? raw.pending as Record<string, PendingCredential> : {},
  };
}

export function loadCredentialStore(): CredentialStore {
  if (!existsSync(CREDENTIALS_FILE)) {
    return normalizeCredentialStore(null);
  }

  const raw = readFileSync(CREDENTIALS_FILE, "utf8");
  const parsed = parseJson<LegacyCredentialStore>(raw, `Invalid credential store at ${CREDENTIALS_FILE}`);
  return normalizeCredentialStore(parsed);
}

export function saveCredentialStore(store: CredentialStore): void {
  writePrivateJson(CREDENTIALS_FILE, {
    version: 1,
    families: store.families,
    accounts: store.accounts,
    pending: store.pending,
  });
}

const EMAIL_FAMILY_PLACEHOLDER = "{n}";

interface ParsedEmailFamily {
  normalized: string;
  localPart: string;
  domainPart: string;
  mode: "gmail_plus" | "template";
  templatePrefix?: string;
  templateSuffix?: string;
}

export function makeCredentialFamilyKey(profileName: string, baseEmail: string): string {
  return `${profileName}::${normalizeBaseEmailFamily(baseEmail)}`;
}

function parseEmailFamily(value: string): ParsedEmailFamily {
  const normalized = value.trim().toLowerCase();
  const match = normalized.match(/^([^@]+)@([^@]+)$/);
  if (!match) {
    throw new Error(`"${value}" is not a valid email family.`);
  }

  const localPart = match[1];
  const domainPart = match[2];
  if (!localPart || !domainPart) {
    throw new Error(`"${value}" is not a valid email family.`);
  }

  const placeholderParts = localPart.split(EMAIL_FAMILY_PLACEHOLDER);
  if (placeholderParts.length === 2) {
    const templatePrefix = placeholderParts[0] ?? "";
    const templateSuffix = placeholderParts[1] ?? "";
    if ((templatePrefix + templateSuffix).trim().length === 0) {
      throw new Error(`"${value}" must keep some stable local-part text around ${EMAIL_FAMILY_PLACEHOLDER}.`);
    }
    return {
      normalized: `${templatePrefix}${EMAIL_FAMILY_PLACEHOLDER}${templateSuffix}@${domainPart}`,
      localPart,
      domainPart,
      mode: "template",
      templatePrefix,
      templateSuffix,
    };
  }

  if (placeholderParts.length > 2) {
    throw new Error(`"${value}" may only contain one ${EMAIL_FAMILY_PLACEHOLDER} placeholder.`);
  }

  if (domainPart !== "gmail.com") {
    throw new Error(
      `"${value}" is not a supported base email family. Use gmail.com or a template like dev.{N}@example.com.`,
    );
  }

  const baseLocal = localPart.split("+")[0]?.trim();
  if (!baseLocal) {
    throw new Error(`"${value}" does not contain a valid Gmail local part.`);
  }

  return {
    normalized: `${baseLocal}@gmail.com`,
    localPart: `${baseLocal}`,
    domainPart: "gmail.com",
    mode: "gmail_plus",
  };
}

export function normalizeBaseEmailFamily(email: string): string {
  return parseEmailFamily(email).normalized;
}

export function buildAccountFamilyEmail(baseEmail: string, suffix: number): string {
  if (!Number.isInteger(suffix) || suffix < 1) {
    throw new Error(`Invalid email family suffix "${suffix}".`);
  }

  const parsed = parseEmailFamily(baseEmail);
  if (parsed.mode === "template") {
    return `${parsed.templatePrefix ?? ""}${suffix}${parsed.templateSuffix ?? ""}@${parsed.domainPart}`;
  }
  return `${parsed.localPart}+${suffix}@${parsed.domainPart}`;
}

export function extractAccountFamilySuffix(candidateEmail: string, baseEmail: string): number | null {
  const parsed = parseEmailFamily(baseEmail);
  const normalizedCandidate = candidateEmail.trim().toLowerCase();
  const match = parsed.mode === "template"
    ? normalizedCandidate.match(
      new RegExp(
        `^${escapeRegExp(parsed.templatePrefix ?? "")}(\\d+)${escapeRegExp(parsed.templateSuffix ?? "")}@${escapeRegExp(parsed.domainPart)}$`,
        "i",
      ),
    )
    : normalizedCandidate.match(
      new RegExp(`^${escapeRegExp(parsed.localPart)}\\+(\\d+)@${escapeRegExp(parsed.domainPart)}$`, "i"),
    );
  if (!match) {
    return null;
  }

  const suffix = Number.parseInt(match[1] ?? "", 10);
  return Number.isInteger(suffix) ? suffix : null;
}

export function computeNextAccountFamilySuffix(
  baseEmail: string,
  familyNextSuffix: number,
  knownEmails: Iterable<string>,
): number {
  const usedSuffixes = new Set<number>();

  for (const email of knownEmails) {
    const suffix = extractAccountFamilySuffix(email, baseEmail);
    if (suffix) {
      usedSuffixes.add(suffix);
    }
  }

  let candidate = Math.max(1, familyNextSuffix);
  while (usedSuffixes.has(candidate)) {
    candidate += 1;
  }
  return candidate;
}

export function normalizeGmailBaseEmail(email: string): string {
  return normalizeBaseEmailFamily(email);
}

export function buildGmailAliasEmail(baseEmail: string, suffix: number): string {
  return buildAccountFamilyEmail(baseEmail, suffix);
}

export function extractGmailAliasSuffix(candidateEmail: string, baseEmail: string): number | null {
  return extractAccountFamilySuffix(candidateEmail, baseEmail);
}

export function computeNextGmailAliasSuffix(
  baseEmail: string,
  familyNextSuffix: number,
  knownEmails: Iterable<string>,
): number {
  return computeNextAccountFamilySuffix(baseEmail, familyNextSuffix, knownEmails);
}

function parseSortableTimestamp(value: string | null | undefined): number {
  const timestamp = Date.parse(value ?? "");
  return Number.isFinite(timestamp) ? timestamp : 0;
}

export function selectPendingCredentialForFamily(
  store: CredentialStore,
  profileName: string,
  baseEmail: string,
  alias?: string | null,
): PendingCredential | null {
  const normalizedBaseEmail = normalizeBaseEmailFamily(baseEmail);
  const normalizedAlias = alias?.trim().toLowerCase() || null;
  const matches = Object.values(store.pending).filter((entry) => (
    entry.profile_name === profileName
    && normalizeBaseEmailFamily(entry.base_email) === normalizedBaseEmail
    && (!normalizedAlias || (entry.alias?.trim().toLowerCase() || null) === normalizedAlias)
  ));

  if (matches.length === 0) {
    return null;
  }

  matches.sort((left, right) => {
    // Drain the oldest reserved alias first so unfinished +1/+2 entries are
    // promoted before newer aliases are allocated or resumed.
    if ((left.suffix || 0) !== (right.suffix || 0)) {
      return (left.suffix || 0) - (right.suffix || 0);
    }
    const leftStartedAt = parseSortableTimestamp(left.started_at || left.created_at || left.updated_at);
    const rightStartedAt = parseSortableTimestamp(right.started_at || right.created_at || right.updated_at);
    if (leftStartedAt !== rightStartedAt) {
      return leftStartedAt - rightStartedAt;
    }
    const leftUpdatedAt = parseSortableTimestamp(left.updated_at || left.started_at);
    const rightUpdatedAt = parseSortableTimestamp(right.updated_at || right.started_at);
    return leftUpdatedAt - rightUpdatedAt;
  });

  return matches[0] ?? null;
}

export function selectPendingBaseEmailHintForProfile(
  store: CredentialStore,
  profileName: string,
  alias?: string | null,
): string | null {
  const normalizedAlias = alias?.trim().toLowerCase() || null;
  const matches = Object.values(store.pending).filter((entry) => (
    entry.profile_name === profileName
    && (!normalizedAlias || (entry.alias?.trim().toLowerCase() || null) === normalizedAlias)
  ));

  if (matches.length === 0) {
    return null;
  }

  matches.sort((left, right) => {
    const leftStartedAt = parseSortableTimestamp(left.started_at || left.created_at || left.updated_at);
    const rightStartedAt = parseSortableTimestamp(right.started_at || right.created_at || right.updated_at);
    if (leftStartedAt !== rightStartedAt) {
      return leftStartedAt - rightStartedAt;
    }
    if ((left.suffix || 0) !== (right.suffix || 0)) {
      return (left.suffix || 0) - (right.suffix || 0);
    }
    const leftUpdatedAt = parseSortableTimestamp(left.updated_at || left.started_at);
    const rightUpdatedAt = parseSortableTimestamp(right.updated_at || right.started_at);
    return leftUpdatedAt - rightUpdatedAt;
  });

  const rawEmail = matches[0]?.base_email || matches[0]?.email;
  if (!rawEmail) {
    return null;
  }
  try {
    return normalizeBaseEmailFamily(rawEmail);
  } catch {
    return null;
  }
}

export function generatePassword(length = 18): string {
  const uppercase = "ABCDEFGHJKLMNPQRSTUVWXYZ";
  const lowercase = "abcdefghijkmnopqrstuvwxyz";
  const digits = "23456789";
  const alphabet = `${uppercase}${lowercase}${digits}`;

  if (length < 12) {
    throw new Error("Generated passwords must be at least 12 characters long.");
  }

  const pick = (source: string): string => source[randomBytes(1)[0]! % source.length]!;
  const chars = [
    pick(uppercase),
    pick(lowercase),
    pick(digits),
  ];

  while (chars.length < length) {
    chars.push(pick(alphabet));
  }

  for (let index = chars.length - 1; index > 0; index--) {
    const swapIndex = randomBytes(1)[0]! % (index + 1);
    const current = chars[index]!;
    chars[index] = chars[swapIndex]!;
    chars[swapIndex] = current;
  }

  return chars.join("");
}

function normalizeEmailCandidate(value: string): string | null {
  const trimmed = value.trim().toLowerCase();
  return /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(trimmed) ? trimmed : null;
}

function readChromeProfileAccountEmails(userDataDir: string, profileDirectory: string): string[] {
  const preferencesPath = join(userDataDir, profileDirectory, "Preferences");
  if (!existsSync(preferencesPath)) {
    return [];
  }

  let parsed: ChromeProfilePreferences;
  try {
    parsed = parseJson<ChromeProfilePreferences>(
      readFileSync(preferencesPath, "utf8"),
      `Invalid Chrome profile preferences at ${preferencesPath}`,
    );
  } catch {
    return [];
  }

  const rawEntries = Array.isArray(parsed.account_info) ? parsed.account_info : [];
  const emails = rawEntries
    .map((entry) => (typeof entry?.email === "string" ? normalizeEmailCandidate(entry.email) : null))
    .filter((value): value is string => Boolean(value));

  return [...new Set(emails)];
}

function extractSupportedGmailEmails(emails: Iterable<string>): string[] {
  const supported = new Set<string>();
  for (const email of emails) {
    try {
      supported.add(normalizeGmailBaseEmail(email));
    } catch {}
  }
  return [...supported];
}

function tokenizeManagedProfileName(profileName: string): string[] {
  return profileName
    .trim()
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .map((token) => token.trim())
    .filter((token) => token.length > 0);
}

export function scoreEmailForManagedProfileName(profileName: string, email: string): number {
  const normalizedEmail = normalizeEmailCandidate(email);
  if (!normalizedEmail) {
    return Number.NEGATIVE_INFINITY;
  }

  const localPart = normalizedEmail.slice(0, normalizedEmail.indexOf("@")).split("+")[0] ?? "";
  const compactLocal = localPart.replace(/[^a-z0-9]/g, "");
  const localSegments = new Set(localPart.split(/[^a-z0-9]+/).filter(Boolean));
  const tokens = tokenizeManagedProfileName(profileName);
  const significantTokens = tokens.filter((token) => token.length > 1 || /^\d+$/.test(token));

  let score = 0;
  for (const token of significantTokens) {
    if (localSegments.has(token)) {
      score += /^\d+$/.test(token) ? 140 : 120;
      continue;
    }
    if (compactLocal.startsWith(token) || compactLocal.endsWith(token)) {
      score += 40;
      continue;
    }
    if (compactLocal.includes(token)) {
      score += 25;
    }
  }

  const compactProfile = profileName.toLowerCase().replace(/[^a-z0-9]/g, "");
  if (compactProfile.length >= 3) {
    if (compactLocal.includes(compactProfile)) {
      score += 80;
    } else {
      const reversedCompactProfile = compactProfile.split("").reverse().join("");
      if (compactLocal.includes(reversedCompactProfile)) {
        score += 40;
      }
    }
  }

  return score;
}

export function selectBestEmailForManagedProfile(
  profileName: string,
  emails: Iterable<string>,
  preferredBaseEmail?: string | null,
): string | null {
  let normalizedPreferred: string | null = null;
  if (preferredBaseEmail) {
    try {
      normalizedPreferred = normalizeGmailBaseEmail(preferredBaseEmail);
    } catch {
      normalizedPreferred = null;
    }
  }
  const candidates = extractSupportedGmailEmails(emails)
    .map((email, index) => ({
      email,
      index,
      exactPreferred: normalizedPreferred ? email === normalizedPreferred : false,
      score: scoreEmailForManagedProfileName(profileName, email),
    }))
    .sort((left, right) => {
      if (left.exactPreferred !== right.exactPreferred) {
        return left.exactPreferred ? -1 : 1;
      }
      if (left.score !== right.score) {
        return right.score - left.score;
      }
      return left.index - right.index;
    });

  return candidates[0]?.email ?? null;
}

export function selectStoredBaseEmailHint(store: CredentialStore, profileName: string): string | null {
  const candidates = new Map<string, { count: number; updatedAt: number }>();
  const remember = (rawEmail: string | null | undefined, updatedAt: string | null | undefined): void => {
    if (!rawEmail) return;
    let baseEmail: string;
    try {
      baseEmail = normalizeBaseEmailFamily(rawEmail);
    } catch {
      return;
    }
    const existing = candidates.get(baseEmail);
    const updatedAtValue = parseSortableTimestamp(updatedAt);
    candidates.set(baseEmail, {
      count: (existing?.count ?? 0) + 1,
      updatedAt: Math.max(existing?.updatedAt ?? 0, updatedAtValue),
    });
  };

  for (const family of Object.values(store.families)) {
    if (family.profile_name === profileName) {
      remember(family.base_email, family.updated_at);
    }
  }
  for (const account of Object.values(store.accounts)) {
    if (account.profile_name === profileName) {
      remember(account.base_email || account.email, account.updated_at);
    }
  }
  for (const pending of Object.values(store.pending)) {
    if (pending.profile_name === profileName) {
      remember(pending.base_email || pending.email, pending.updated_at || pending.started_at);
    }
  }

  return [...candidates.entries()]
    .sort((left, right) => {
      if (left[1].count !== right[1].count) {
        return right[1].count - left[1].count;
      }
      if (left[1].updatedAt !== right[1].updatedAt) {
        return right[1].updatedAt - left[1].updatedAt;
      }
      return left[0].localeCompare(right[0]);
    })[0]?.[0] ?? null;
}

function parseSystemChromeProfiles(rawProfiles: Array<Record<string, unknown>>): SystemChromeProfileEntry[] {
  return rawProfiles
    .map((profile) => ({
      directory: typeof profile.directory === "string" ? profile.directory : "",
      name: typeof profile.name === "string" ? profile.name : "",
    }))
    .filter((profile) => profile.directory && profile.name);
}

export function selectBestSystemChromeProfileMatch(
  profileName: string,
  profiles: Array<{ directory: string; name: string; emails: string[] }>,
  preferredBaseEmail?: string | null,
): SystemChromeProfileMatch | null {
  let normalizedPreferred: string | null = null;
  if (preferredBaseEmail) {
    try {
      normalizedPreferred = normalizeGmailBaseEmail(preferredBaseEmail);
    } catch {
      normalizedPreferred = null;
    }
  }
  const candidates = profiles
    .map((profile) => {
      const matchedEmail = selectBestEmailForManagedProfile(profileName, profile.emails, preferredBaseEmail);
      if (!matchedEmail) {
        return null;
      }
      return {
        directory: profile.directory,
        name: profile.name,
        emails: extractSupportedGmailEmails(profile.emails),
        matchedEmail,
        score: normalizedPreferred && matchedEmail === normalizedPreferred
          ? 10_000
          : scoreEmailForManagedProfileName(profileName, matchedEmail),
      };
    })
    .filter((value): value is SystemChromeProfileMatch => Boolean(value))
    .sort((left, right) => {
      if (left.score !== right.score) {
        return right.score - left.score;
      }
      return left.name.localeCompare(right.name);
    });

  return candidates[0] ?? null;
}

function directoryHasEntries(directoryPath: string): boolean {
  try {
    return readdirSync(directoryPath).length > 0;
  } catch {
    return false;
  }
}

async function sleep(milliseconds: number): Promise<void> {
  await new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

function readPidIfExists(pidPath: string): number | null {
  try {
    if (!existsSync(pidPath)) {
      return null;
    }
    const pid = Number.parseInt(readFileSync(pidPath, "utf8").trim(), 10);
    return Number.isInteger(pid) && pid > 0 ? pid : null;
  } catch {
    return null;
  }
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return !(error && typeof error === "object" && "code" in error && error.code === "ESRCH");
  }
}

async function requestFastBrowserDaemonShutdown(socketPath: string): Promise<boolean> {
  const protocolModuleUrl = pathToFileURL(resolve(
    REPO_ROOT,
    "..",
    "ai-rules",
    "skills",
    "fast-browser",
    "lib",
    "daemon",
    "protocol.mjs",
  )).href;

  try {
    const { sendDaemonRequest } = await import(protocolModuleUrl);
    const response = await sendDaemonRequest(socketPath, { method: "shutdown" }, 10_000);
    return response?.ok === true;
  } catch {
    return false;
  }
}

function findManagedChromeProcess(profileName: string): { pid: number; port: number | null } | null {
  const userDataDir = join(FAST_BROWSER_PROFILES_HOME, profileName);
  const result = spawnSync("ps", ["-Ao", "pid=,command="], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });
  const output = result.stdout ?? "";
  for (const line of output.split("\n")) {
    if (!line.includes("Google Chrome")) {
      continue;
    }
    if (!line.includes(`--user-data-dir=${userDataDir}`)) {
      continue;
    }
    const pidMatch = line.trim().match(/^(\d+)\s+/);
    if (!pidMatch) {
      continue;
    }
    const portMatch = line.match(/--remote-debugging-port=(\d+)/);
    return {
      pid: Number.parseInt(pidMatch[1]!, 10),
      port: portMatch ? Number.parseInt(portMatch[1]!, 10) : null,
    };
  }
  return null;
}

async function requestManagedChromeShutdown(profileName: string): Promise<boolean> {
  const chrome = findManagedChromeProcess(profileName);
  if (!chrome?.port) {
    return false;
  }

  const chromeModuleUrl = pathToFileURL(resolve(
    REPO_ROOT,
    "..",
    "ai-rules",
    "skills",
    "fast-browser",
    "lib",
    "backends",
    "local-chrome-cdp.mjs",
  )).href;

  try {
    const { closeChromeBrowserViaCdp } = await import(chromeModuleUrl);
    return await closeChromeBrowserViaCdp(chrome.port);
  } catch {
    return false;
  }
}

function requestDaemonProcessTermination(pidPath: string): boolean {
  const pid = readPidIfExists(pidPath);
  if (!pid || !isProcessAlive(pid)) {
    return false;
  }
  try {
    process.kill(pid, "SIGTERM");
    return true;
  } catch {
    return false;
  }
}

async function waitForManagedProfileShutdown(pidPath: string, timeoutMs: number): Promise<boolean> {
  const pid = readPidIfExists(pidPath);
  if (!pid || !isProcessAlive(pid)) {
    return true;
  }

  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!isProcessAlive(pid)) {
      return true;
    }
    await sleep(100);
  }

  return !isProcessAlive(pid);
}

async function resetManagedProfileRuntime(profileName: string, socketPath?: string | null): Promise<void> {
  const resolvedSocketPath = socketPath?.trim() || join(FAST_BROWSER_DAEMON_DIR, `${profileName}.sock`);
  const pidPath = join(FAST_BROWSER_DAEMON_DIR, `${profileName}.pid`);
  const hadSocket = existsSync(resolvedSocketPath);
  const hadPid = Boolean(readPidIfExists(pidPath));

  let shutdownAccepted = !hadSocket;
  if (hadSocket) {
    shutdownAccepted = await requestFastBrowserDaemonShutdown(resolvedSocketPath);
  }

  if (!shutdownAccepted) {
    await requestManagedChromeShutdown(profileName);
    if (!requestDaemonProcessTermination(pidPath) && hadPid) {
      throw new Error(
        `Managed profile "${profileName}" did not accept a fast-browser shutdown request. `
        + "Quit the managed browser normally and retry.",
      );
    }
  }

  const exitedCleanly = await waitForManagedProfileShutdown(pidPath, 20_000);
  if (!exitedCleanly) {
    throw new Error(
      `Managed profile "${profileName}" is still running after a normal shutdown request. `
      + "Quit the managed browser normally and retry.",
    );
  }

  try {
    if (hadSocket && existsSync(resolvedSocketPath)) {
      unlinkSync(resolvedSocketPath);
    }
  } catch {}

  try {
    if (hadPid && existsSync(pidPath)) {
      unlinkSync(pidPath);
    }
  } catch {}
}

function archiveManagedProfileDirectory(profileName: string, userDataDir: string): void {
  if (!directoryHasEntries(userDataDir)) {
    rmSync(userDataDir, { recursive: true, force: true });
    return;
  }

  mkdirSync(FAST_BROWSER_MANAGED_PROFILE_ARCHIVE_ROOT, { recursive: true });
  const archiveName = `${profileName}-system-bootstrap-${new Date().toISOString().replace(/[:.]/g, "-")}`;
  const archivePath = join(FAST_BROWSER_MANAGED_PROFILE_ARCHIVE_ROOT, archiveName);
  renameSync(userDataDir, archivePath);
}

function runBootstrapRsync(sourcePath: string, destinationPath: string): void {
  const result = spawnSync("rsync", [
    "-a",
    "--delete",
    "--exclude=Cache",
    "--exclude=Code Cache",
    "--exclude=GPUCache",
    "--exclude=DawnCache",
    "--exclude=GrShaderCache",
    "--exclude=ShaderCache",
    "--exclude=Crashpad",
    "--exclude=Singleton*",
    sourcePath,
    destinationPath,
  ], {
    cwd: REPO_ROOT,
    encoding: "utf8",
  });
  if (result.error) {
    throw result.error;
  }
  if (typeof result.status === "number" && result.status !== 0) {
    const details = [result.stdout, result.stderr].filter(Boolean).join("\n").trim();
    throw new Error(details || `rsync bootstrap failed for ${sourcePath}`);
  }
}

async function bootstrapManagedProfileFromSystemProfile(
  managedProfileName: string,
  managedProfileDirectory: string,
  managedUserDataDir: string,
  chromeUserDataDir: string,
  sourceProfileDirectory: string,
): Promise<void> {
  await resetManagedProfileRuntime(managedProfileName);
  if (existsSync(managedUserDataDir)) {
    archiveManagedProfileDirectory(managedProfileName, managedUserDataDir);
  }

  mkdirSync(managedUserDataDir, { recursive: true });
  runBootstrapRsync(join(chromeUserDataDir, "Local State"), `${managedUserDataDir}/`);
  runBootstrapRsync(join(chromeUserDataDir, sourceProfileDirectory), `${managedUserDataDir}/`);
}

function ensureFastBrowserScript(): void {
  if (!existsSync(FAST_BROWSER_SCRIPT)) {
    throw new Error(
      `fast-browser script not found at ${FAST_BROWSER_SCRIPT}. `
      + "Set CODEX_ROTATE_FAST_BROWSER_SCRIPT or install the shared fast-browser skill repo next to ai-tools.",
    );
  }
}

function ensureFastBrowserPlaywright(): void {
  if (!existsSync(FAST_BROWSER_PLAYWRIGHT_MODULE)) {
    throw new Error(
      `Playwright is not installed in ${REPO_ROOT}. `
      + 'Run "bun install" after adding the playwright dependency before using create/relogin automation.',
    );
  }
}

function runFastBrowserCommandSync(args: string[], options?: { requirePlaywright?: boolean }): SpawnSyncReturns<string> {
  ensureFastBrowserScript();
  if (options?.requirePlaywright !== false) {
    ensureFastBrowserPlaywright();
  }

  const result = spawnSync(FAST_BROWSER_RUNTIME, [FAST_BROWSER_SCRIPT, ...args], {
    cwd: REPO_ROOT,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
    stdio: ["ignore", "pipe", "inherit"],
  });

  if (result.error) {
    throw result.error;
  }

  return result;
}

function createFastBrowserOutputPath(): string {
  mkdirSync(ROTATE_HOME, { recursive: true });
  return join(
    ROTATE_HOME,
    `fast-browser-${Date.now()}-${randomBytes(6).toString("hex")}.json`,
  );
}

async function runFastBrowserCommand(
  args: string[],
  options?: { requirePlaywright?: boolean },
): Promise<FastBrowserCommandResult> {
  ensureFastBrowserScript();
  if (options?.requirePlaywright !== false) {
    ensureFastBrowserPlaywright();
  }

  const outputPath = createFastBrowserOutputPath();
  let stdoutFd = openSync(outputPath, "w");

  try {
    const child = spawn(FAST_BROWSER_RUNTIME, [FAST_BROWSER_SCRIPT, ...args], {
      cwd: REPO_ROOT,
      stdio: ["ignore", stdoutFd, "inherit"],
    });
    closeSync(stdoutFd);
    stdoutFd = -1;

    const exit = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (code, signal) => {
        resolve({ code, signal });
      });
    });

    return {
      status: exit.code,
      signal: exit.signal,
      stdout: existsSync(outputPath) ? readFileSync(outputPath, "utf8") : "",
      stderr: "",
    };
  } finally {
    if (stdoutFd !== -1) {
      closeSync(stdoutFd);
    }
    try {
      unlinkSync(outputPath);
    } catch {}
  }
}

function parseFastBrowserJson<T>(
  result: Pick<SpawnSyncReturns<string>, "status" | "stdout"> | FastBrowserCommandResult,
  actionLabel: string,
): T {
  if (typeof result.status === "number" && result.status !== 0) {
    const summary = result.stdout?.trim() || `${actionLabel} exited with status ${result.status}.`;
    throw new Error(summary);
  }

  const stdout = result.stdout?.trim();
  if (!stdout) {
    throw new Error(`${actionLabel} did not return JSON output.`);
  }

  return parseJson<T>(stdout, `${actionLabel} returned invalid JSON.`);
}

function formatDaemonBridgeResult(result: SpawnSyncReturns<string>): { status: number | null; stdout: string } {
  if (typeof result.status === "number" && result.status !== 0) {
    const combined = [result.stdout, result.stderr]
      .filter((value) => typeof value === "string" && value.trim().length > 0)
      .join("\n")
      .trim();
    return {
      status: result.status,
      stdout: combined,
    };
  }

  return {
    status: result.status,
    stdout: result.stdout ?? "",
  };
}

function parseFastBrowserProgressEventLine(line: string): FastBrowserProgressEvent | null {
  if (!line.startsWith(FAST_BROWSER_EVENT_PREFIX)) {
    return null;
  }
  const raw = line.slice(FAST_BROWSER_EVENT_PREFIX.length).trim();
  if (!raw) {
    return null;
  }
  try {
    return JSON.parse(raw) as FastBrowserProgressEvent;
  } catch {
    return null;
  }
}

function formatFastBrowserProgressEvent(event: FastBrowserProgressEvent): string | null {
  if (!event || typeof event !== "object") {
    return null;
  }

  const workflow = typeof event.workflow === "string" ? event.workflow : null;
  const stepId = typeof event.stepId === "string" ? event.stepId : null;
  const phase = typeof event.phase === "string" ? event.phase : null;
  const status = typeof event.status === "string" ? event.status : null;
  const message = typeof event.message === "string" ? event.message : null;
  const time = typeof event.time === "string" ? event.time : null;
  const details = (event.details && typeof event.details === "object" && !Array.isArray(event.details))
    ? event.details as Record<string, unknown>
    : null;
  if (shouldSuppressFastBrowserProgressEvent(phase, status)) {
    return null;
  }

  const scope = [workflow, stepId].filter(Boolean).join("/");
  const state = formatFastBrowserEventState(phase, status);
  const detailParts: string[] = [];
  const relayUrl = typeof details?.relay_url === "string" ? details.relay_url : null;
  const reason = typeof details?.reason === "string" ? details.reason : null;
  const workflowStack = Array.isArray(details?.workflow_stack) ? details.workflow_stack : null;
  const runPath = typeof details?.run_path === "string"
    ? details.run_path
    : (typeof details?.run_status_path === "string" ? details.run_status_path : null);
  const currentUrl = typeof details?.current_url === "string" ? details.current_url : null;
  const stage = typeof details?.stage === "string" ? details.stage : null;
  const screenshotPath = typeof details?.screenshot_path === "string" ? details.screenshot_path : null;
  const stepGoal = typeof details?.step_goal === "string" ? details.step_goal : null;
  const actionKind = typeof details?.action_kind === "string" ? details.action_kind : null;
  const headline = typeof details?.headline === "string" ? details.headline : null;
  if (reason) detailParts.push(`reason=${reason}`);
  if (relayUrl) detailParts.push(`relay_url=${relayUrl}`);
  if (workflowStack && workflowStack.length > 0) detailParts.push(`workflow_stack=${workflowStack.length}`);
  if (headline) detailParts.push(`headline=${JSON.stringify(headline)}`);
  if (actionKind) detailParts.push(`action=${actionKind}`);
  if (stage) detailParts.push(`stage=${stage}`);
  if (currentUrl) detailParts.push(`url=${currentUrl}`);
  if (runPath) detailParts.push(`run=${runPath}`);
  if (screenshotPath) detailParts.push(`screenshot=${screenshotPath}`);

  const primaryText = stepGoal || message;
  const prefix = [scope, state].filter(Boolean).join(" ");
  const suffix = detailParts.length > 0 ? ` (${detailParts.join(", ")})` : "";
  const core = prefix && primaryText
    ? `${prefix}: ${primaryText}${suffix}`
    : primaryText
      ? `${primaryText}${suffix}`
      : prefix
        ? `${prefix}${suffix}`
        : suffix
          ? suffix.slice(1, -1)
          : null;
  if (!core) {
    return null;
  }
  if (time) {
    return `${time} ${core}`;
  }
  return core;
}

function shouldSuppressFastBrowserProgressEvent(phase: string | null, status: string | null): boolean {
  const key = [phase || "", status || ""].join(":");
  return key === "pre:start"
    || key === "pre:ok"
    || key === "post:start"
    || key === "post:ok"
    || key === "action:start";
}

function formatFastBrowserEventState(phase: string | null, status: string | null): string {
  const key = [phase || "", status || ""].join(":");
  switch (key) {
    case "step:start":
      return "step";
    case "step:ok":
      return "step ok";
    case "step:skipped":
      return "step skip";
    case "action:ok":
      return "done";
    case "action:resume":
      return "resume";
    case "workflow:finish":
      return "workflow finish";
    default:
      return [phase, status].filter(Boolean).join(" ");
  }
}

function emitFastBrowserProgressEvent(event: FastBrowserProgressEvent): void {
  const line = formatFastBrowserProgressEvent(event);
  if (!line) {
    return;
  }
  process.stderr.write(`[fast-browser] ${line}\n`);
}

async function runFastBrowserWorkflow(
  workflowRef: string,
  inputs: Record<string, string>,
  profileName: string,
  options?: {
    headed?: boolean;
    workflowRunStamp?: string;
    retainTemporaryProfilesOnSuccess?: boolean;
    artifactMode?: "minimal" | "full";
    debugMode?: "off" | "step";
  },
): Promise<FastBrowserRunResult> {
  return await runFastBrowserDaemonWorkflow(workflowRef, inputs, profileName, options);
}

async function runFastBrowserDaemonWorkflow(
  workflowRef: string,
  inputs: Record<string, string>,
  profileName: string,
  options?: {
    headed?: boolean;
    workflowRunStamp?: string;
    retainTemporaryProfilesOnSuccess?: boolean;
    artifactMode?: "minimal" | "full";
    debugMode?: "off" | "step";
  },
): Promise<FastBrowserRunResult> {
  ensureFastBrowserPlaywright();
  const clientModuleUrl = pathToFileURL(resolve(
    REPO_ROOT,
    "..",
    "ai-rules",
    "skills",
    "fast-browser",
    "lib",
    "daemon",
    "client.mjs",
  )).href;
  const bridgeScript = `
    import { runDaemonWorkflow } from ${JSON.stringify(clientModuleUrl)};
    const response = await runDaemonWorkflow({
      profileName: ${JSON.stringify(profileName)},
      workflowRef: ${JSON.stringify(workflowRef)},
      inputs: ${JSON.stringify(inputs)},
      headed: ${Boolean(options?.headed)},
      workflowRunStamp: ${JSON.stringify(options?.workflowRunStamp ?? null)},
      retainTemporaryProfilesOnSuccess: ${Boolean(options?.retainTemporaryProfilesOnSuccess)},
      artifactMode: ${JSON.stringify(options?.artifactMode ?? "minimal")},
      debugMode: ${JSON.stringify(options?.debugMode ?? "off")},
      responseMode: "action_only",
      onEvent: (event) => {
        process.stderr.write(${JSON.stringify(FAST_BROWSER_EVENT_PREFIX)} + JSON.stringify(event) + "\\n");
      },
    });
    console.log(JSON.stringify(response));
  `;
  const executeBridge = async (): Promise<FastBrowserCommandResult> => await new Promise((resolve, reject) => {
    const child = spawn("node", ["--input-type=module", "-e", bridgeScript], {
      cwd: REPO_ROOT,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    let stderrBuffer = "";

    const flushStderrLine = (line: string): void => {
      const progressEvent = parseFastBrowserProgressEventLine(line);
      if (progressEvent) {
        emitFastBrowserProgressEvent(progressEvent);
        return;
      }
      stderr += `${line}\n`;
      process.stderr.write(`${line}\n`);
    };

    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      stdout += chunk;
    });

    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk: string) => {
      stderrBuffer += chunk;
      while (true) {
        const newlineIndex = stderrBuffer.indexOf("\n");
        if (newlineIndex === -1) {
          break;
        }
        const line = stderrBuffer.slice(0, newlineIndex);
        stderrBuffer = stderrBuffer.slice(newlineIndex + 1);
        if (line.trim()) {
          flushStderrLine(line);
        }
      }
    });

    child.once("error", reject);
    child.once("close", (code, signal) => {
      if (stderrBuffer.trim()) {
        flushStderrLine(stderrBuffer.trimEnd());
      }
      resolve({
        status: code,
        signal,
        stdout,
        stderr,
      });
    });
  });

  let result = await executeBridge();
  if (typeof result.status === "number" && result.status !== 0) {
    const combinedOutput = [result.stdout, result.stderr].filter(Boolean).join("\n");
    if (await resetStuckFastBrowserDaemon(profileName, combinedOutput)) {
      result = await executeBridge();
    }
  }
  const response = parseFastBrowserJson<FastBrowserDaemonRunResponse>(
    { status: result.status, stdout: result.stdout },
    `fast-browser workflow ${workflowRef}`,
  );

  if (!response?.ok || !response.result) {
    throw new Error(response?.error?.message || `fast-browser workflow ${workflowRef} failed.`);
  }

  if (response.result.status === "paused") {
    const reason = response.result.pause?.reason ?? "pause";
    const relay = response.result.pause?.relay_url ? ` Open ${response.result.pause.relay_url} to continue the workflow.` : "";
    throw new Error(`fast-browser workflow ${workflowRef} paused for ${reason}.${relay}`);
  }

  return response.result;
}

export async function deleteTemporaryFastBrowserProfile(profileName: string): Promise<void> {
  const normalized = String(profileName || "").trim();
  if (!normalized) {
    return;
  }
  const browserProfilesModuleUrl = pathToFileURL(resolve(
    REPO_ROOT,
    "..",
    "ai-rules",
    "skills",
    "fast-browser",
    "lib",
    "browser-profiles.mjs",
  )).href;
  const bridgeScript = `
    import { deleteManagedProfile } from ${JSON.stringify(browserProfilesModuleUrl)};
    await deleteManagedProfile(${JSON.stringify(normalized)}, { allowDefault: false });
    console.log(JSON.stringify({ ok: true }));
  `;
  const result = spawnSync("node", ["--input-type=module", "-e", bridgeScript], {
    cwd: REPO_ROOT,
    stdio: ["ignore", "pipe", "pipe"],
    encoding: "utf8",
  });
  if (result.status !== 0) {
    throw new Error(result.stderr.trim() || `Failed to delete temporary fast-browser profile "${normalized}".`);
  }
}

async function resetStuckFastBrowserDaemon(profileName: string, output: string | null | undefined): Promise<boolean> {
  const match = output?.match(FAST_BROWSER_DAEMON_TIMEOUT_PATTERN);
  if (!match) {
    return false;
  }

  await resetManagedProfileRuntime(profileName, match[1]?.trim() || null);
  return true;
}

export function inspectManagedProfiles(): ManagedProfilesInspection {
  return parseFastBrowserJson<ManagedProfilesInspection>(
    runFastBrowserCommandSync(["inspect-profiles"], { requirePlaywright: false }),
    "fast-browser inspect-profiles",
  );
}

function assertLocalWorkflowReference(workflowRef: string): { surface: string; target: string; name: string } {
  const parts = workflowRef.trim().split(":");
  if (parts.length !== 4 || parts[0] !== "local") {
    throw new Error(`Expected a local workflow reference, got "${workflowRef}".`);
  }

  const [, surface, target, name] = parts;
  const segmentPattern = /^[A-Za-z0-9._-]+$/;
  if (!surface || !segmentPattern.test(surface)) {
    throw new Error(`Invalid workflow surface in "${workflowRef}".`);
  }
  if (!target || !segmentPattern.test(target)) {
    throw new Error(`Invalid workflow target in "${workflowRef}".`);
  }
  if (!name || !segmentPattern.test(name)) {
    throw new Error(`Invalid workflow name in "${workflowRef}".`);
  }

  return { surface, target, name };
}

function localWorkflowReferenceToPath(workflowRef: string): string {
  const parsed = assertLocalWorkflowReference(workflowRef);
  return join(FAST_BROWSER_WORKFLOWS_ROOT, parsed.surface, parsed.target, `${parsed.name}.yaml`);
}

function normalizeWorkflowScalar(rawValue: string | null | undefined): string | null {
  const trimmed = rawValue?.trim();
  if (!trimmed) return null;

  const withoutComment = trimmed.replace(/\s+#.*$/, "").trim();
  if (!withoutComment) return null;

  const first = withoutComment[0];
  const last = withoutComment[withoutComment.length - 1];
  if ((first === "\"" && last === "\"") || (first === "'" && last === "'")) {
    return withoutComment.slice(1, -1).trim() || null;
  }

  return withoutComment;
}

function parseWorkflowYamlDocument(raw: string): Record<string, unknown> | null {
  const rubyScript = `
require "json"
require "yaml"
content = STDIN.read
begin
  data = YAML.safe_load(content, permitted_classes: [], permitted_symbols: [], aliases: false)
rescue ArgumentError
  data = YAML.safe_load(content)
end
puts JSON.generate(data)
`;
  const result = spawnSync("ruby", ["-e", rubyScript], {
    cwd: REPO_ROOT,
    input: raw,
    stdio: ["pipe", "pipe", "pipe"],
    encoding: "utf8",
  });
  if (result.status !== 0) {
    return null;
  }
  try {
    const parsed = JSON.parse(result.stdout);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? parsed as Record<string, unknown>
      : null;
  } catch {
    return null;
  }
}

export function parseLocalWorkflowMetadata(raw: string): Omit<LocalWorkflowMetadata, "filePath"> {
  const parsed = parseWorkflowYamlDocument(raw);
  const document = parsed?.document && typeof parsed.document === "object" && !Array.isArray(parsed.document)
    ? parsed.document as Record<string, unknown>
    : null;
  const metadata = document?.metadata && typeof document.metadata === "object" && !Array.isArray(document.metadata)
    ? document.metadata as Record<string, unknown>
    : null;
  return {
    preferredProfileName: normalizeWorkflowScalar(typeof metadata?.preferredProfile === "string" ? metadata.preferredProfile : null),
    preferredEmail: normalizeWorkflowScalar(typeof metadata?.preferredEmail === "string" ? metadata.preferredEmail : null),
  };
}

export function readLocalWorkflowMetadata(workflowRef: string): LocalWorkflowMetadata {
  const filePath = localWorkflowReferenceToPath(workflowRef);
  if (!existsSync(filePath)) {
    throw new Error(`Workflow "${workflowRef}" not found at ${filePath}.`);
  }

  const raw = readFileSync(filePath, "utf8");
  return {
    filePath,
    ...parseLocalWorkflowMetadata(raw),
  };
}

export function resolveManagedProfileNameFromCandidates(
  availableNames: Iterable<string>,
  options?: {
    requestedProfileName?: string;
    preferredProfileName?: string | null;
    preferredProfileSource?: string | null;
    defaultProfileName?: string | null;
  },
): string {
  const availableProfileNames = new Set(availableNames);

  const requestedProfile = options?.requestedProfileName?.trim();
  if (requestedProfile) {
    if (!availableProfileNames.has(requestedProfile)) {
      throw new Error(`Managed fast-browser profile "${requestedProfile}" was not found.`);
    }
    return requestedProfile;
  }

  const preferredProfile = options?.preferredProfileName?.trim();
  if (preferredProfile) {
    if (!availableProfileNames.has(preferredProfile)) {
      const source = options?.preferredProfileSource ? ` from ${options.preferredProfileSource}` : "";
      throw new Error(`Managed fast-browser profile "${preferredProfile}"${source} was not found.`);
    }
    return preferredProfile;
  }

  const defaultProfile = options?.defaultProfileName?.trim();
  if (defaultProfile && availableProfileNames.has(defaultProfile)) {
    return defaultProfile;
  }

  const fallback = [...availableProfileNames][0];
  if (fallback) {
    return fallback;
  }

  throw new Error("No managed fast-browser profiles are configured.");
}

export function resolveManagedProfileName(
  options?: {
    requestedProfileName?: string;
    preferredProfileName?: string | null;
    preferredProfileSource?: string | null;
  },
): string {
  const inspection = inspectManagedProfiles();
  return resolveManagedProfileNameFromCandidates(
    inspection.managedProfiles.profiles.map((profile) => profile.name),
    {
      requestedProfileName: options?.requestedProfileName,
      preferredProfileName: options?.preferredProfileName,
      preferredProfileSource: options?.preferredProfileSource,
      defaultProfileName: inspection.managedProfiles.default ?? null,
    },
  );
}

export function resolveCreateBaseEmail(
  requestedBaseEmail: string | null | undefined,
  discoveredBaseEmail: string | null | undefined,
): string {
  if (requestedBaseEmail) {
    return normalizeBaseEmailFamily(requestedBaseEmail);
  }
  if (discoveredBaseEmail) {
    return normalizeBaseEmailFamily(discoveredBaseEmail);
  }
  throw new Error("Could not determine the base email family for create.");
}

function getManagedProfileEntry(inspection: ManagedProfilesInspection, profileName: string): ManagedProfileEntry | null {
  return inspection.managedProfiles.profiles.find((profile) => profile.name === profileName) ?? null;
}

async function maybeBootstrapManagedProfileFromSystem(
  inspection: ManagedProfilesInspection,
  profileName: string,
  preferredBaseEmail?: string | null,
): Promise<string[]> {
  const managedProfile = getManagedProfileEntry(inspection, profileName);
  if (!managedProfile) {
    return [];
  }

  const existingEmails = extractSupportedGmailEmails(
    readChromeProfileAccountEmails(managedProfile.userDataDir, managedProfile.profileDirectory),
  );
  if (existingEmails.length > 0) {
    return existingEmails;
  }

  const systemProfiles = parseSystemChromeProfiles(inspection.profiles)
    .map((profile) => ({
      ...profile,
      emails: extractSupportedGmailEmails(
        readChromeProfileAccountEmails(inspection.chromeUserDataDir, profile.directory),
      ),
    }));

  const match = selectBestSystemChromeProfileMatch(profileName, systemProfiles, preferredBaseEmail);
  if (!match) {
    return [];
  }

  await bootstrapManagedProfileFromSystemProfile(
    profileName,
    managedProfile.profileDirectory,
    managedProfile.userDataDir,
    inspection.chromeUserDataDir,
    match.directory,
  );

  return extractSupportedGmailEmails(
    readChromeProfileAccountEmails(managedProfile.userDataDir, managedProfile.profileDirectory),
  );
}

export async function discoverGmailBaseEmail(
  profileName: string,
  options?: { preferredBaseEmail?: string | null },
): Promise<string> {
  const inspection = inspectManagedProfiles();
  const managedProfile = getManagedProfileEntry(inspection, profileName);
  if (!managedProfile) {
    throw new Error(`Managed fast-browser profile "${profileName}" was not found.`);
  }

  let candidateEmails = extractSupportedGmailEmails(
    readChromeProfileAccountEmails(managedProfile.userDataDir, managedProfile.profileDirectory),
  );
  if (candidateEmails.length === 0) {
    candidateEmails = await maybeBootstrapManagedProfileFromSystem(
      inspection,
      profileName,
      options?.preferredBaseEmail ?? null,
    );
  }

  const selectedEmail = selectBestEmailForManagedProfile(
    profileName,
    candidateEmails,
    options?.preferredBaseEmail ?? null,
  );
  if (!selectedEmail) {
    const result = await runFastBrowserDaemonWorkflow(
      GMAIL_CAPTURE_WORKFLOW,
      options?.preferredBaseEmail ? { preferred_email: options.preferredBaseEmail } : {},
      profileName,
    );
    const capturedEmail = readWorkflowActionString(result, "capture_active_account", "email");
    if (!capturedEmail) {
      throw new Error(`Could not discover a Gmail address from managed profile "${profileName}".`);
    }
    return normalizeGmailBaseEmail(capturedEmail);
  }

  return normalizeGmailBaseEmail(selectedEmail);
}

export async function runOpenAiAccountCreation(
  profileName: string,
  email: string,
  password: string,
  workflowRunStamp?: string,
  options?: {
    artifactMode?: "minimal" | "full";
  },
): Promise<FastBrowserRunResult> {
  return await runFastBrowserWorkflow(
    CODEX_ROTATE_ACCOUNT_FLOW_WORKFLOW,
    { mode: "signup", email, password },
    profileName,
    {
      workflowRunStamp,
      retainTemporaryProfilesOnSuccess: Boolean(workflowRunStamp),
      artifactMode: options?.artifactMode ?? "minimal",
    },
  );
}

export async function runOpenAiEmailVerification(
  profileName: string,
  email: string,
  password?: string,
  workflowRunStamp?: string,
  options?: {
    artifactMode?: "minimal" | "full";
  },
): Promise<FastBrowserRunResult> {
  return await runFastBrowserWorkflow(
    CODEX_ROTATE_ACCOUNT_FLOW_WORKFLOW,
    password
      ? { mode: "resume_openai_setup", email, password }
      : { mode: "resume_openai_setup", email },
    profileName,
    {
      workflowRunStamp,
      retainTemporaryProfilesOnSuccess: Boolean(workflowRunStamp),
      artifactMode: options?.artifactMode ?? "minimal",
    },
  );
}

export async function runCodexBrowserLoginWorkflow(
  profileName: string,
  authUrl: string,
  email: string,
  password: string,
  workflowRunStamp?: string,
  options?: {
    artifactMode?: "minimal" | "full";
    preferSignupRecovery?: boolean;
    fullName?: string;
    birthMonth?: number;
    birthDay?: number;
    birthYear?: number;
  },
): Promise<FastBrowserRunResult> {
  return await runFastBrowserWorkflow(
    CODEX_ROTATE_ACCOUNT_FLOW_WORKFLOW,
    {
      mode: "codex_login",
      auth_url: authUrl,
      email,
      password,
      prefer_signup_recovery: options?.preferSignupRecovery === true ? "true" : "false",
      full_name: options?.fullName ?? "Dev Astronlab",
      birth_month: String(options?.birthMonth ?? 1),
      birth_day: String(options?.birthDay ?? 1),
      birth_year: String(options?.birthYear ?? 1990),
    },
    profileName,
    {
      workflowRunStamp,
      retainTemporaryProfilesOnSuccess: Boolean(workflowRunStamp),
      artifactMode: options?.artifactMode ?? "minimal",
    },
  );
}

export function readWorkflowActionString(
  result: FastBrowserRunResult,
  stepId: string,
  field: string,
): string | null {
  const step = result.state?.steps?.[stepId];
  const value = step?.action?.[field];
  return typeof value === "string" && value.trim() ? value : null;
}

export function readWorkflowActionBoolean(
  result: FastBrowserRunResult,
  stepId: string,
  field: string,
): boolean {
  return result.state?.steps?.[stepId]?.action?.[field] === true;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function extractAuthUrl(output: string): string | null {
  const match = output.match(/https:\/\/auth\.openai\.com\/oauth\/authorize\?[^\s]+/);
  return match?.[0] ?? null;
}

export function parseCodexBrowserLoginCapture(output: string): ParsedCodexBrowserLoginCapture | null {
  const authUrl = extractAuthUrl(output);
  if (!authUrl) {
    return null;
  }

  const parsedAuthUrl = new URL(authUrl);
  const callbackUrl = parsedAuthUrl.searchParams.get("redirect_uri") ?? "";
  if (!callbackUrl) {
    throw new Error(`Could not find redirect_uri in the captured Codex login URL.`);
  }

  const callbackPort = Number.parseInt(new URL(callbackUrl).port, 10);
  if (!Number.isInteger(callbackPort)) {
    throw new Error(`Could not determine the callback port from ${callbackUrl}.`);
  }

  return {
    authUrl,
    callbackUrl,
    callbackPort,
  };
}

export async function startCodexBrowserLoginSession(
  codexBin: string,
  timeoutMs = LOGIN_CAPTURE_TIMEOUT_MS,
): Promise<BrowserLoginSession> {
  return await new Promise<BrowserLoginSession>((resolvePromise, rejectPromise) => {
    const browserCapture = createBrowserCaptureShim();
    const child = spawn(codexBin, ["login"], {
      cwd: REPO_ROOT,
      env: {
        ...process.env,
        PATH: `${browserCapture.shimDir}:${process.env.PATH ?? ""}`,
        BROWSER: browserCapture.scriptPath,
        CODEX_ROTATE_BROWSER_CAPTURE_SCRIPT: browserCapture.scriptPath,
        CODEX_ROTATE_BROWSER_CAPTURE_OUT: browserCapture.outputPath,
        CODEX_ROTATE_PLAYWRIGHT_REQUIRE: FAST_BROWSER_PLAYWRIGHT_MODULE,
        CODEX_ROTATE_BROWSER_CAPTURE_MODE: "headless_tmp" as BrowserCaptureMode,
      },
      stdio: ["pipe", "pipe", "pipe"],
    });
    child.stdin.end();

    const buffers = {
      stdout: "",
      stderr: "",
    };

    const exitPromise = new Promise<CommandExit>((resolveExit) => {
      child.once("exit", (code, signal) => {
        cleanupBrowserCaptureShim(browserCapture.scriptPath, browserCapture.outputPath, browserCapture.shimDir);
        resolveExit({ code, signal });
      });
    });

    let settled = false;

    const finishReject = (error: Error): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      rejectPromise(error);
    };

    const maybeResolve = (): void => {
      if (settled) return;

      const combined = `${buffers.stdout}\n${buffers.stderr}\n${readBrowserCaptureOutput(browserCapture.outputPath)}`;
      let parsedCapture: ParsedCodexBrowserLoginCapture | null;
      try {
        parsedCapture = parseCodexBrowserLoginCapture(combined);
      } catch (error) {
        finishReject(error instanceof Error ? error : new Error(String(error)));
        return;
      }

      if (!parsedCapture) {
        return;
      }

      settled = true;
      clearTimeout(timer);
      resolvePromise({
        child,
        authUrl: parsedCapture.authUrl,
        callbackUrl: parsedCapture.callbackUrl,
        callbackPort: parsedCapture.callbackPort,
        buffers,
        exitPromise,
        browserCaptureScriptPath: browserCapture.scriptPath,
        browserCaptureOutputPath: browserCapture.outputPath,
        browserCaptureShimDir: browserCapture.shimDir,
      });
    };

    child.stdout.on("data", (chunk: Buffer | string) => {
      buffers.stdout += chunk.toString();
      maybeResolve();
    });

    child.stderr.on("data", (chunk: Buffer | string) => {
      buffers.stderr += chunk.toString();
      maybeResolve();
    });

    child.once("error", (error) => {
      cleanupBrowserCaptureShim(browserCapture.scriptPath, browserCapture.outputPath, browserCapture.shimDir);
      finishReject(error instanceof Error ? error : new Error(String(error)));
    });

    child.once("exit", (code, signal) => {
      if (settled) {
        return;
      }
      const output = `${buffers.stdout}\n${buffers.stderr}`.trim();
      finishReject(
        new Error(
          `Failed to capture the Codex browser login URL before "${codexBin} login" exited `
          + `(${code ?? signal ?? "unknown"}).${output ? `\n${output}` : ""}`,
        ),
      );
    });

    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      child.kill("SIGTERM");
      rejectPromise(new Error(`Timed out waiting for "${codexBin} login" to emit its browser URL.`));
    }, timeoutMs);
  });
}

export async function waitForCodexBrowserLoginExit(
  session: BrowserLoginSession,
  timeoutMs = 15_000,
): Promise<void> {
  const exit = await withTimeout(session.exitPromise, timeoutMs, () => cancelCodexBrowserLoginSession(session));

  if (exit.code !== 0) {
    const output = `${session.buffers.stdout}\n${session.buffers.stderr}`.trim();
    throw new Error(
      `"codex login" exited with ${exit.code ?? exit.signal ?? "an unknown status"}.`
      + `${output ? `\n${output}` : ""}`,
    );
  }
}

export function cancelCodexBrowserLoginSession(session: BrowserLoginSession): void {
  if (session.child.exitCode !== null || session.child.signalCode !== null) {
    cleanupBrowserCaptureShim(session.browserCaptureScriptPath, session.browserCaptureOutputPath, session.browserCaptureShimDir);
    return;
  }

  session.child.kill("SIGTERM");
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, onTimeout: () => void): Promise<T> {
  let timeoutId: NodeJS.Timeout | null = null;

  try {
    return await new Promise<T>((resolvePromise, rejectPromise) => {
      timeoutId = setTimeout(() => {
        onTimeout();
        rejectPromise(new Error(`Timed out after ${timeoutMs}ms.`));
      }, timeoutMs);

      promise.then(resolvePromise, rejectPromise);
    });
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }
}
