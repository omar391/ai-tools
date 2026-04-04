import { createHash, randomBytes } from "node:crypto";
import { spawn, spawnSync, type SpawnSyncReturns } from "node:child_process";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
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

const FAST_BROWSER_SCRIPT =
  process.env.CODEX_ROTATE_FAST_BROWSER_SCRIPT ?? FAST_BROWSER_SCRIPT_DEFAULT;
const FAST_BROWSER_RUNTIME =
  process.env.CODEX_ROTATE_FAST_BROWSER_RUNTIME ??
  (process.versions.bun ? "node" : process.execPath);
const FAST_BROWSER_PLAYWRIGHT_MODULE = join(
  REPO_ROOT,
  "node_modules",
  "playwright",
);
const FAST_BROWSER_DAEMON_CLIENT_MODULE = pathToFileURL(
  resolve(
    REPO_ROOT,
    "..",
    "ai-rules",
    "skills",
    "fast-browser",
    "lib",
    "daemon",
    "client.mjs",
  ),
).href;
const CODEX_LOGIN_MANAGED_BROWSER_OPENER_DEFAULT = resolve(
  MODULE_DIR,
  "codex-login-managed-browser-opener.mjs",
);
const CODEX_LOGIN_MANAGED_APP_SERVER_HELPER_DEFAULT = resolve(
  MODULE_DIR,
  "codex-login-app-server-helper.mjs",
);

const CODEX_ROTATE_ACCOUNT_FLOW_ID =
  "workspace.web.auth-openai-com.codex-rotate-account-flow";
export const CODEX_ROTATE_ACCOUNT_FLOW_FILE = join(
  REPO_ROOT,
  ".fast-browser",
  "workflows",
  "web",
  "auth.openai.com",
  "codex-rotate-account-flow.yaml",
);
export const CODEX_ROTATE_OPENAI_TEMP_RUNTIME_KEY = "openai-account-runtime";
const OPENAI_ACCOUNT_SECRET_URIS = [
  "https://auth.openai.com",
  "https://chatgpt.com",
];
const DEFAULT_OPENAI_FULL_NAME = "M Omar Faruque";
const DEFAULT_OPENAI_BIRTH_MONTH = 1;
const DEFAULT_OPENAI_BIRTH_DAY = 24;
const DEFAULT_OPENAI_BIRTH_YEAR = 1990;

export function shouldPromptForCodexRotateSecretUnlock(): boolean {
  return (
    process.env.CODEX_ROTATE_ALLOW_INTERACTIVE_SECRET_UNLOCK === "1" ||
    (process.stdin.isTTY && process.stderr.isTTY)
  );
}

export const CREDENTIALS_FILE = join(ROTATE_HOME, "credentials.json");

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

export interface CodexRotateSecretRef {
  type: "secret_ref";
  store: "bitwarden-cli";
  object_id: string;
  field_path?: string | null;
  version?: string | null;
}

export interface CodexRotateLoginLookupSecretLocator {
  kind: "login_lookup";
  store?: "bitwarden-cli";
  username: string;
  uris: string[];
  field_path?: string | null;
}

export interface CodexRotateNamedSecretLocator {
  kind: "named_secret";
  store?: "bitwarden-cli";
  name: string;
  field_path?: string | null;
}

export interface CodexRotateEnvVarSecretLocator {
  kind: "env_var";
  name: string;
}

export type CodexRotateSecretLocator =
  | CodexRotateLoginLookupSecretLocator
  | CodexRotateNamedSecretLocator
  | CodexRotateEnvVarSecretLocator;

export interface PendingCredential extends StoredCredential {
  started_at: string;
}

export interface CredentialStore {
  version: 3;
  families: Record<string, CredentialFamily>;
  pending: Record<string, PendingCredential>;
}

interface LegacyCredentialStore {
  version?: unknown;
  defaults?: unknown;
  families?: unknown;
  accounts?: unknown;
  pending?: unknown;
}

export interface WorkflowFileMetadata {
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
  output?: Record<string, unknown> | null;
  pause?: FastBrowserPause | null;
  finalUrl?: string | null;
  observability?: {
    runId?: string | null;
    runPath?: string | null;
    statusPath?: string | null;
    eventsPath?: string | null;
  } | null;
}

export function buildTemporaryWorkflowProfileName(
  workflowRunStamp: string,
  key: string,
): string {
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

export function buildCodexRotateOpenAiTempProfileName(
  workflowRunStamp: string,
): string {
  return buildTemporaryWorkflowProfileName(
    workflowRunStamp,
    CODEX_ROTATE_OPENAI_TEMP_RUNTIME_KEY,
  );
}

interface FastBrowserDaemonRunResponse {
  ok: boolean;
  result?: FastBrowserRunResult;
  error?: {
    message?: string;
  };
}

const FAST_BROWSER_DAEMON_TIMEOUT_PATTERN =
  /Timed out waiting for fast-browser daemon response from\s+(.+?\.sock)/i;
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

export interface CodexRotateAuthFlowSession {
  auth_url?: string | null;
  callback_url?: string | null;
  callback_port?: number | null;
  device_code?: string | null;
  session_dir?: string | null;
  pid?: number | null;
  stdout_path?: string | null;
  stderr_path?: string | null;
  exit_path?: string | null;
}

export interface CodexRotateAuthFlowSummary {
  stage?: string | null;
  current_url?: string | null;
  headline?: string | null;
  callback_complete?: boolean;
  success?: boolean;
  account_ready?: boolean;
  needs_email_verification?: boolean;
  follow_up_step?: boolean;
  add_phone_prompt?: boolean;
  retryable_timeout?: boolean;
  session_ended?: boolean;
  existing_account_prompt?: boolean;
  username_not_found?: boolean;
  invalid_credentials?: boolean;
  rate_limit_exceeded?: boolean;
  anti_bot_gate?: boolean;
  auth_prompt?: boolean;
  consent_blocked?: boolean;
  consent_error?: string | null;
  next_action?: string | null;
  replay_reason?: string | null;
  retry_reason?: string | null;
  error_message?: string | null;
  codex_session?: CodexRotateAuthFlowSession | null;
  codex_login_exit_ok?: boolean;
  codex_login_exit_code?: number | null;
  codex_login_stdout_tail?: string | null;
  codex_login_stderr_tail?: string | null;
}

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

function writePrivateJson(filePath: string, value: unknown): void {
  ensureRotateDir();
  writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
  chmodSync(filePath, 0o600);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

export function normalizeCredentialStore(
  raw: LegacyCredentialStore | null | undefined,
): CredentialStore {
  const families = isRecord(raw?.families)
    ? ({ ...(raw.families as Record<string, CredentialFamily>) } as Record<
        string,
        CredentialFamily
      >)
    : {};
  const legacyAccounts = normalizeCredentialRecordMap(raw?.accounts);
  for (const account of Object.values(legacyAccounts)) {
    const familyKey = `${account.profile_name}::${normalizeBaseEmailFamily(account.base_email)}`;
    const existing = families[familyKey];
    const updatedAtValues = [
      parseSortableTimestamp(existing?.updated_at),
      parseSortableTimestamp(account.updated_at),
    ];
    const createdAtValues = [
      parseSortableTimestamp(existing?.created_at),
      parseSortableTimestamp(account.created_at),
    ];
    families[familyKey] = {
      profile_name: account.profile_name,
      base_email: normalizeBaseEmailFamily(account.base_email),
      next_suffix: Math.max(
        existing?.next_suffix ?? 1,
        (account.suffix || 0) + 1,
      ),
      created_at:
        createdAtValues[0] !== 0 &&
        createdAtValues[0] <= (createdAtValues[1] || Number.MAX_SAFE_INTEGER)
          ? (existing?.created_at ?? account.created_at)
          : account.created_at,
      updated_at:
        updatedAtValues[0] >= updatedAtValues[1]
          ? (existing?.updated_at ?? account.updated_at)
          : account.updated_at,
      last_created_email:
        updatedAtValues[0] >= updatedAtValues[1]
          ? (existing?.last_created_email ?? account.email)
          : account.email,
    };
  }
  return {
    version: 3,
    families,
    pending: normalizeCredentialRecordMap(raw?.pending) as Record<
      string,
      PendingCredential
    >,
  };
}

export function buildOpenAiAccountLoginLocator(
  email: string,
): CodexRotateSecretLocator {
  return {
    kind: "login_lookup",
    store: "bitwarden-cli",
    username: String(email || "")
      .trim()
      .toLowerCase(),
    uris: [...OPENAI_ACCOUNT_SECRET_URIS],
    field_path: "/password",
  };
}

function isStoreBackedSecretLocator(
  locator: CodexRotateSecretLocator | null | undefined,
): locator is Exclude<
  CodexRotateSecretLocator,
  CodexRotateEnvVarSecretLocator
> {
  return Boolean(locator && locator.kind !== "env_var");
}

function isMissingOptionalSecretLocatorError(
  locator: CodexRotateSecretLocator,
  error: unknown,
): boolean {
  const message = error instanceof Error ? error.message : String(error || "");
  if (locator.kind === "env_var") {
    return false;
  }
  return (
    /No Bitwarden login item matched/i.test(message) ||
    /No Bitwarden item matched the exact name/i.test(message)
  );
}

async function resolveOptionalCodexRotateSecretLocator(
  profileName: string,
  locator: CodexRotateSecretLocator | null | undefined,
): Promise<CodexRotateSecretLocator | null> {
  if (!locator) {
    return null;
  }
  const {
    ensureDaemonSecretStoreReadyInteractive,
    resolveDaemonSecretLocator,
  } = await import(FAST_BROWSER_DAEMON_CLIENT_MODULE);
  if (isStoreBackedSecretLocator(locator)) {
    await ensureDaemonSecretStoreReadyInteractive({
      profileName,
      store: locator.store ?? "bitwarden-cli",
      promptIfLocked: shouldPromptForCodexRotateSecretUnlock(),
    });
  }
  try {
    const response = await resolveDaemonSecretLocator({
      profileName,
      locator,
    });
    if (!response?.ok) {
      throw new Error(
        response?.error?.message ||
          "fast-browser failed to resolve the requested secret locator.",
      );
    }
    return locator;
  } catch (error) {
    if (isMissingOptionalSecretLocatorError(locator, error)) {
      return null;
    }
    throw error;
  }
}

export async function ensureBitwardenCliAccountSecretRef(
  profileName: string,
  email: string,
  password: string,
): Promise<CodexRotateSecretRef> {
  const normalizedProfileName = String(profileName || "").trim();
  const normalizedEmail = String(email || "")
    .trim()
    .toLowerCase();
  if (!normalizedProfileName) {
    throw new Error(
      "Bitwarden account secrets require a managed profile name.",
    );
  }
  if (!normalizedEmail) {
    throw new Error("Bitwarden account secrets require a non-empty email.");
  }
  const normalizedPassword = String(password || "");
  if (!normalizedPassword) {
    throw new Error(
      `Bitwarden account secret for ${normalizedEmail} requires a non-empty password.`,
    );
  }

  const {
    ensureDaemonLoginSecretRef,
    ensureDaemonSecretStoreReadyInteractive,
  } = await import(FAST_BROWSER_DAEMON_CLIENT_MODULE);
  await ensureDaemonSecretStoreReadyInteractive({
    profileName: normalizedProfileName,
    store: "bitwarden-cli",
    promptIfLocked: shouldPromptForCodexRotateSecretUnlock(),
  });
  const response = await ensureDaemonLoginSecretRef({
    profileName: normalizedProfileName,
    store: "bitwarden-cli",
    name: buildCodexRotateAccountSecretName(normalizedEmail),
    username: normalizedEmail,
    password: normalizedPassword,
    notes: `Managed by codex-rotate for ${normalizedEmail}.`,
    uris: OPENAI_ACCOUNT_SECRET_URIS,
  });
  if (!response?.ok) {
    throw new Error(
      response?.error?.message ||
        `Fast-browser Bitwarden adapter failed while creating or reusing the vault item for ${normalizedEmail}.`,
    );
  }
  const ref = normalizeCodexRotateSecretRef(response?.ref);
  if (!ref) {
    throw new Error(
      `Fast-browser Bitwarden adapter did not return a secret ref for ${normalizedEmail}.`,
    );
  }
  return ref;
}

export async function findBitwardenCliAccountSecretRef(
  profileName: string,
  email: string,
): Promise<CodexRotateSecretRef | null> {
  const normalizedProfileName = String(profileName || "").trim();
  const normalizedEmail = String(email || "")
    .trim()
    .toLowerCase();
  if (!normalizedProfileName) {
    throw new Error(
      "Bitwarden account secret lookup requires a managed profile name.",
    );
  }
  if (!normalizedEmail) {
    throw new Error(
      "Bitwarden account secret lookup requires a non-empty email.",
    );
  }

  const { ensureDaemonSecretStoreReadyInteractive, findDaemonLoginSecretRef } =
    await import(FAST_BROWSER_DAEMON_CLIENT_MODULE);
  await ensureDaemonSecretStoreReadyInteractive({
    profileName: normalizedProfileName,
    store: "bitwarden-cli",
    promptIfLocked: shouldPromptForCodexRotateSecretUnlock(),
  });
  const response = await findDaemonLoginSecretRef({
    profileName: normalizedProfileName,
    store: "bitwarden-cli",
    username: normalizedEmail,
    uris: OPENAI_ACCOUNT_SECRET_URIS,
  });
  if (!response?.ok) {
    throw new Error(
      response?.error?.message ||
        `Fast-browser Bitwarden adapter failed while looking up the vault item for ${normalizedEmail}.`,
    );
  }
  return normalizeCodexRotateSecretRef(response?.ref);
}

export function loadCredentialStore(): CredentialStore {
  if (!existsSync(CREDENTIALS_FILE)) {
    return normalizeCredentialStore(null);
  }

  const raw = readFileSync(CREDENTIALS_FILE, "utf8");
  const parsed = parseJson<LegacyCredentialStore>(
    raw,
    `Invalid credential store at ${CREDENTIALS_FILE}`,
  );
  return normalizeCredentialStore(parsed);
}

export function saveCredentialStore(store: CredentialStore): void {
  writePrivateJson(CREDENTIALS_FILE, {
    version: 3,
    families: store.families,
    pending: serializeCredentialRecordMap(store.pending),
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

export function makeCredentialFamilyKey(
  profileName: string,
  baseEmail: string,
): string {
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
      throw new Error(
        `"${value}" must keep some stable local-part text around ${EMAIL_FAMILY_PLACEHOLDER}.`,
      );
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
    throw new Error(
      `"${value}" may only contain one ${EMAIL_FAMILY_PLACEHOLDER} placeholder.`,
    );
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

export function buildAccountFamilyEmail(
  baseEmail: string,
  suffix: number,
): string {
  if (!Number.isInteger(suffix) || suffix < 1) {
    throw new Error(`Invalid email family suffix "${suffix}".`);
  }

  const parsed = parseEmailFamily(baseEmail);
  if (parsed.mode === "template") {
    return `${parsed.templatePrefix ?? ""}${suffix}${parsed.templateSuffix ?? ""}@${parsed.domainPart}`;
  }
  return `${parsed.localPart}+${suffix}@${parsed.domainPart}`;
}

export function extractAccountFamilySuffix(
  candidateEmail: string,
  baseEmail: string,
): number | null {
  const parsed = parseEmailFamily(baseEmail);
  const normalizedCandidate = candidateEmail.trim().toLowerCase();
  const match =
    parsed.mode === "template"
      ? normalizedCandidate.match(
          new RegExp(
            `^${escapeRegExp(parsed.templatePrefix ?? "")}(\\d+)${escapeRegExp(parsed.templateSuffix ?? "")}@${escapeRegExp(parsed.domainPart)}$`,
            "i",
          ),
        )
      : normalizedCandidate.match(
          new RegExp(
            `^${escapeRegExp(parsed.localPart)}\\+(\\d+)@${escapeRegExp(parsed.domainPart)}$`,
            "i",
          ),
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

export function buildGmailAliasEmail(
  baseEmail: string,
  suffix: number,
): string {
  return buildAccountFamilyEmail(baseEmail, suffix);
}

export function extractGmailAliasSuffix(
  candidateEmail: string,
  baseEmail: string,
): number | null {
  return extractAccountFamilySuffix(candidateEmail, baseEmail);
}

export function computeNextGmailAliasSuffix(
  baseEmail: string,
  familyNextSuffix: number,
  knownEmails: Iterable<string>,
): number {
  return computeNextAccountFamilySuffix(
    baseEmail,
    familyNextSuffix,
    knownEmails,
  );
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
  const matches = Object.values(store.pending).filter(
    (entry) =>
      entry.profile_name === profileName &&
      normalizeBaseEmailFamily(entry.base_email) === normalizedBaseEmail &&
      (!normalizedAlias ||
        (entry.alias?.trim().toLowerCase() || null) === normalizedAlias),
  );

  if (matches.length === 0) {
    return null;
  }

  matches.sort((left, right) => {
    // Drain the oldest reserved alias first so unfinished +1/+2 entries are
    // promoted before newer aliases are allocated or resumed.
    if ((left.suffix || 0) !== (right.suffix || 0)) {
      return (left.suffix || 0) - (right.suffix || 0);
    }
    const leftStartedAt = parseSortableTimestamp(
      left.started_at || left.created_at || left.updated_at,
    );
    const rightStartedAt = parseSortableTimestamp(
      right.started_at || right.created_at || right.updated_at,
    );
    if (leftStartedAt !== rightStartedAt) {
      return leftStartedAt - rightStartedAt;
    }
    const leftUpdatedAt = parseSortableTimestamp(
      left.updated_at || left.started_at,
    );
    const rightUpdatedAt = parseSortableTimestamp(
      right.updated_at || right.started_at,
    );
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
  const matches = Object.values(store.pending).filter(
    (entry) =>
      entry.profile_name === profileName &&
      (!normalizedAlias ||
        (entry.alias?.trim().toLowerCase() || null) === normalizedAlias),
  );

  if (matches.length === 0) {
    return null;
  }

  matches.sort((left, right) => {
    const leftStartedAt = parseSortableTimestamp(
      left.started_at || left.created_at || left.updated_at,
    );
    const rightStartedAt = parseSortableTimestamp(
      right.started_at || right.created_at || right.updated_at,
    );
    if (leftStartedAt !== rightStartedAt) {
      return leftStartedAt - rightStartedAt;
    }
    if ((left.suffix || 0) !== (right.suffix || 0)) {
      return (left.suffix || 0) - (right.suffix || 0);
    }
    const leftUpdatedAt = parseSortableTimestamp(
      left.updated_at || left.started_at,
    );
    const rightUpdatedAt = parseSortableTimestamp(
      right.updated_at || right.started_at,
    );
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

  const pick = (source: string): string =>
    source[randomBytes(1)[0]! % source.length]!;
  const chars = [pick(uppercase), pick(lowercase), pick(digits)];

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

function readChromeProfileAccountEmails(
  userDataDir: string,
  profileDirectory: string,
): string[] {
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

  const rawEntries = Array.isArray(parsed.account_info)
    ? parsed.account_info
    : [];
  const emails = rawEntries
    .map((entry) =>
      typeof entry?.email === "string"
        ? normalizeEmailCandidate(entry.email)
        : null,
    )
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

export function scoreEmailForManagedProfileName(
  profileName: string,
  email: string,
): number {
  const normalizedEmail = normalizeEmailCandidate(email);
  if (!normalizedEmail) {
    return Number.NEGATIVE_INFINITY;
  }

  const localPart =
    normalizedEmail.slice(0, normalizedEmail.indexOf("@")).split("+")[0] ?? "";
  const compactLocal = localPart.replace(/[^a-z0-9]/g, "");
  const localSegments = new Set(localPart.split(/[^a-z0-9]+/).filter(Boolean));
  const tokens = tokenizeManagedProfileName(profileName);
  const significantTokens = tokens.filter(
    (token) => token.length > 1 || /^\d+$/.test(token),
  );

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
      const reversedCompactProfile = compactProfile
        .split("")
        .reverse()
        .join("");
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
      exactPreferred: normalizedPreferred
        ? email === normalizedPreferred
        : false,
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

export function selectStoredBaseEmailHint(
  store: CredentialStore,
  profileName: string,
): string | null {
  const candidates = new Map<string, { count: number; updatedAt: number }>();
  const remember = (
    rawEmail: string | null | undefined,
    updatedAt: string | null | undefined,
  ): void => {
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
  for (const pending of Object.values(store.pending)) {
    if (pending.profile_name === profileName) {
      remember(
        pending.base_email || pending.email,
        pending.updated_at || pending.started_at,
      );
    }
  }

  return (
    [...candidates.entries()].sort((left, right) => {
      if (left[1].count !== right[1].count) {
        return right[1].count - left[1].count;
      }
      if (left[1].updatedAt !== right[1].updatedAt) {
        return right[1].updatedAt - left[1].updatedAt;
      }
      return left[0].localeCompare(right[0]);
    })[0]?.[0] ?? null
  );
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
      const matchedEmail = selectBestEmailForManagedProfile(
        profileName,
        profile.emails,
        preferredBaseEmail,
      );
      if (!matchedEmail) {
        return null;
      }
      return {
        directory: profile.directory,
        name: profile.name,
        emails: extractSupportedGmailEmails(profile.emails),
        matchedEmail,
        score:
          normalizedPreferred && matchedEmail === normalizedPreferred
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
    return !(
      error &&
      typeof error === "object" &&
      "code" in error &&
      error.code === "ESRCH"
    );
  }
}

async function requestFastBrowserDaemonShutdown(
  socketPath: string,
): Promise<boolean> {
  const protocolModuleUrl = pathToFileURL(
    resolve(
      REPO_ROOT,
      "..",
      "ai-rules",
      "skills",
      "fast-browser",
      "lib",
      "daemon",
      "protocol.mjs",
    ),
  ).href;

  try {
    const { sendDaemonRequest } = await import(protocolModuleUrl);
    const response = await sendDaemonRequest(
      socketPath,
      { method: "shutdown" },
      10_000,
    );
    return response?.ok === true;
  } catch {
    return false;
  }
}

function findManagedChromeProcess(
  profileName: string,
): { pid: number; port: number | null } | null {
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

async function requestManagedChromeShutdown(
  profileName: string,
): Promise<boolean> {
  const chrome = findManagedChromeProcess(profileName);
  if (!chrome?.port) {
    return false;
  }

  const chromeModuleUrl = pathToFileURL(
    resolve(
      REPO_ROOT,
      "..",
      "ai-rules",
      "skills",
      "fast-browser",
      "lib",
      "backends",
      "local-chrome-cdp.mjs",
    ),
  ).href;

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

async function waitForManagedProfileShutdown(
  pidPath: string,
  timeoutMs: number,
): Promise<boolean> {
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

async function resetManagedProfileRuntime(
  profileName: string,
  socketPath?: string | null,
): Promise<void> {
  const resolvedSocketPath =
    socketPath?.trim() || join(FAST_BROWSER_DAEMON_DIR, `${profileName}.sock`);
  const pidPath = join(FAST_BROWSER_DAEMON_DIR, `${profileName}.pid`);
  const hadSocket = existsSync(resolvedSocketPath);
  const hadPid = Boolean(readPidIfExists(pidPath));

  let shutdownAccepted = !hadSocket;
  if (hadSocket) {
    shutdownAccepted =
      await requestFastBrowserDaemonShutdown(resolvedSocketPath);
  }

  if (!shutdownAccepted) {
    await requestManagedChromeShutdown(profileName);
    if (!requestDaemonProcessTermination(pidPath) && hadPid) {
      throw new Error(
        `Managed profile "${profileName}" did not accept a fast-browser shutdown request. ` +
          "Quit the managed browser normally and retry.",
      );
    }
  }

  const exitedCleanly = await waitForManagedProfileShutdown(pidPath, 20_000);
  if (!exitedCleanly) {
    throw new Error(
      `Managed profile "${profileName}" is still running after a normal shutdown request. ` +
        "Quit the managed browser normally and retry.",
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

function ensureFastBrowserScript(): void {
  if (!existsSync(FAST_BROWSER_SCRIPT)) {
    throw new Error(
      `fast-browser script not found at ${FAST_BROWSER_SCRIPT}. ` +
        "Set CODEX_ROTATE_FAST_BROWSER_SCRIPT or install the shared fast-browser skill repo next to ai-tools.",
    );
  }
}

function ensureFastBrowserPlaywright(): void {
  if (!existsSync(FAST_BROWSER_PLAYWRIGHT_MODULE)) {
    throw new Error(
      `Playwright is not installed in ${REPO_ROOT}. ` +
        'Run "bun install" after adding the playwright dependency before using create/relogin automation.',
    );
  }
}

function resolveCodexLoginManagedBrowserOpenerPath(): string {
  const override = process.env.CODEX_ROTATE_BROWSER_OPENER_BIN?.trim();
  if (override) {
    return override;
  }
  return CODEX_LOGIN_MANAGED_BROWSER_OPENER_DEFAULT;
}

function resolveCodexLoginManagedLoginHelperPath(): string {
  const override = process.env.CODEX_ROTATE_LOGIN_HELPER_BIN?.trim();
  if (override) {
    return override;
  }
  return CODEX_LOGIN_MANAGED_APP_SERVER_HELPER_DEFAULT;
}

function ensureCodexLoginManagedBrowserOpener(): void {
  const openerPath = resolveCodexLoginManagedBrowserOpenerPath();
  if (!existsSync(openerPath)) {
    throw new Error(
      `Managed Codex browser opener script not found at ${openerPath}.`,
    );
  }
  chmodSync(openerPath, 0o700);
}

function ensureCodexLoginManagedLoginHelper(): void {
  const helperPath = resolveCodexLoginManagedLoginHelperPath();
  if (!existsSync(helperPath)) {
    throw new Error(
      `Managed Codex login helper script not found at ${helperPath}.`,
    );
  }
  chmodSync(helperPath, 0o700);
}

function shellSingleQuote(value: string): string {
  return `'${String(value).replace(/'/g, `'\"'\"'`)}'`;
}

function renderCodexLoginManagedBrowserWrapper(
  realCodexBin: string,
  profileName: string,
  shimDir: string,
  openerPath: string,
  loginHelperPath: string,
): string {
  return [
    "#!/bin/sh",
    `export FAST_BROWSER_PROFILE=${shellSingleQuote(profileName)}`,
    `export BROWSER=${shellSingleQuote(openerPath)}`,
    `export PATH=${shellSingleQuote(shimDir)}:"$PATH"`,
    `export CODEX_ROTATE_REAL_CODEX=${shellSingleQuote(realCodexBin)}`,
    'if [ "$1" = "login" ]; then',
    "  shift",
    `  exec ${shellSingleQuote(loginHelperPath)} "$@"`,
    "fi",
    `exec ${shellSingleQuote(realCodexBin)} \"$@\"`,
    "",
  ].join("\n");
}

function ensureCodexLoginManagedBrowserShims(
  shimDir: string,
  openerPath: string,
): void {
  mkdirSync(shimDir, { recursive: true });
  const shimContent = [
    "#!/bin/sh",
    `exec ${shellSingleQuote(openerPath)} \"$@\"`,
    "",
  ].join("\n");
  for (const shimName of ["open", "xdg-open"]) {
    const shimPath = join(shimDir, shimName);
    const current = existsSync(shimPath)
      ? readFileSync(shimPath, "utf8")
      : null;
    if (current !== shimContent) {
      writeFileSync(shimPath, shimContent, { mode: 0o700 });
    }
    chmodSync(shimPath, 0o700);
  }
}

export function buildCodexLoginManagedBrowserWrapperPath(
  profileName: string,
  codexBin: string,
): string {
  const openerPath = resolveCodexLoginManagedBrowserOpenerPath();
  const loginHelperPath = resolveCodexLoginManagedLoginHelperPath();
  const profileToken =
    String(profileName || "default")
      .toLowerCase()
      .replace(/[^a-z0-9._-]+/g, "-")
      .replace(/^-|-$/g, "")
      .slice(0, 32) || "default";
  const hash = createHash("sha256")
    .update(`${profileName}\n${codexBin}\n${openerPath}\n${loginHelperPath}`)
    .digest("hex")
    .slice(0, 12);
  return join(ROTATE_HOME, "bin", `codex-login-${profileToken}-${hash}`);
}

export function ensureCodexLoginManagedBrowserWrapper(
  profileName: string,
  codexBin: string,
): string {
  ensureCodexLoginManagedBrowserOpener();
  ensureCodexLoginManagedLoginHelper();
  mkdirSync(join(ROTATE_HOME, "bin"), { recursive: true });
  const openerPath = resolveCodexLoginManagedBrowserOpenerPath();
  const loginHelperPath = resolveCodexLoginManagedLoginHelperPath();
  const shimDir = join(ROTATE_HOME, "bin", "codex-login-shims");
  ensureCodexLoginManagedBrowserShims(shimDir, openerPath);
  const wrapperPath = buildCodexLoginManagedBrowserWrapperPath(
    profileName,
    codexBin,
  );
  const content = renderCodexLoginManagedBrowserWrapper(
    codexBin,
    profileName,
    shimDir,
    openerPath,
    loginHelperPath,
  );
  const current = existsSync(wrapperPath)
    ? readFileSync(wrapperPath, "utf8")
    : null;
  if (current !== content) {
    writeFileSync(wrapperPath, content, { mode: 0o700 });
  }
  chmodSync(wrapperPath, 0o700);
  return wrapperPath;
}

function runFastBrowserCommandSync(
  args: string[],
  options?: { requirePlaywright?: boolean },
): SpawnSyncReturns<string> {
  ensureFastBrowserScript();
  if (options?.requirePlaywright !== false) {
    ensureFastBrowserPlaywright();
  }

  const result = spawnSync(
    FAST_BROWSER_RUNTIME,
    [FAST_BROWSER_SCRIPT, ...args],
    {
      cwd: REPO_ROOT,
      encoding: "utf8",
      maxBuffer: 64 * 1024 * 1024,
      stdio: ["ignore", "pipe", "inherit"],
    },
  );

  if (result.error) {
    throw result.error;
  }

  return result;
}

function parseFastBrowserJson<T>(
  result:
    | Pick<SpawnSyncReturns<string>, "status" | "stdout">
    | FastBrowserCommandResult,
  actionLabel: string,
): T {
  if (typeof result.status === "number" && result.status !== 0) {
    const summary =
      result.stdout?.trim() ||
      `${actionLabel} exited with status ${result.status}.`;
    throw new Error(summary);
  }

  const stdout = result.stdout?.trim();
  if (!stdout) {
    throw new Error(`${actionLabel} did not return JSON output.`);
  }

  return parseJson<T>(stdout, `${actionLabel} returned invalid JSON.`);
}

function buildFastBrowserWorkflowError(
  workflowRef: string,
  response: FastBrowserDaemonRunResponse | null | undefined,
): Error {
  const error = new Error(
    response?.error?.message || `fast-browser workflow ${workflowRef} failed.`,
  );
  if (response?.result && typeof response.result === "object") {
    (
      error as Error & { fastBrowserResult?: FastBrowserRunResult }
    ).fastBrowserResult = response.result;
  }
  return error;
}

function readFastBrowserResultFromError(
  error: unknown,
): FastBrowserRunResult | null {
  if (!error || typeof error !== "object") {
    return null;
  }
  const result = (error as { fastBrowserResult?: unknown }).fastBrowserResult;
  if (!result || typeof result !== "object" || Array.isArray(result)) {
    return null;
  }
  return result as FastBrowserRunResult;
}

export function isRetryableCodexLoginWorkflowErrorMessage(
  message: string,
): boolean {
  const normalized = String(message || "")
    .trim()
    .toLowerCase();
  if (!normalized) {
    return false;
  }

  return (
    /(?:signup|login)-verification-code-missing\b/.test(normalized) ||
    /(?:signup|login)-verification-submit-stuck:email_verification\b/.test(
      normalized,
    )
  );
}

function parseFastBrowserProgressEventLine(
  line: string,
): FastBrowserProgressEvent | null {
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

function formatFastBrowserProgressEvent(
  event: FastBrowserProgressEvent,
): string | null {
  if (!event || typeof event !== "object") {
    return null;
  }

  const workflow = typeof event.workflow === "string" ? event.workflow : null;
  const stepId = typeof event.stepId === "string" ? event.stepId : null;
  const phase = typeof event.phase === "string" ? event.phase : null;
  const status = typeof event.status === "string" ? event.status : null;
  const message = typeof event.message === "string" ? event.message : null;
  const time = typeof event.time === "string" ? event.time : null;
  const details =
    event.details &&
    typeof event.details === "object" &&
    !Array.isArray(event.details)
      ? (event.details as Record<string, unknown>)
      : null;
  if (shouldSuppressFastBrowserProgressEvent(phase, status)) {
    return null;
  }

  const scope = [workflow, stepId].filter(Boolean).join("/");
  const state = formatFastBrowserEventState(phase, status);
  const detailParts: string[] = [];
  const relayUrl =
    typeof details?.relay_url === "string" ? details.relay_url : null;
  const reason = typeof details?.reason === "string" ? details.reason : null;
  const workflowStack = Array.isArray(details?.workflow_stack)
    ? details.workflow_stack
    : null;
  const runPath =
    typeof details?.run_path === "string"
      ? details.run_path
      : typeof details?.run_status_path === "string"
        ? details.run_status_path
        : null;
  const currentUrl =
    typeof details?.current_url === "string" ? details.current_url : null;
  const stage = typeof details?.stage === "string" ? details.stage : null;
  const screenshotPath =
    typeof details?.screenshot_path === "string"
      ? details.screenshot_path
      : null;
  const stepGoal =
    typeof details?.step_goal === "string" ? details.step_goal : null;
  const actionKind =
    typeof details?.action_kind === "string" ? details.action_kind : null;
  const headline =
    typeof details?.headline === "string" ? details.headline : null;
  if (reason) detailParts.push(`reason=${reason}`);
  if (relayUrl) detailParts.push(`relay_url=${relayUrl}`);
  if (workflowStack && workflowStack.length > 0)
    detailParts.push(`workflow_stack=${workflowStack.length}`);
  if (headline) detailParts.push(`headline=${JSON.stringify(headline)}`);
  if (actionKind) detailParts.push(`action=${actionKind}`);
  if (stage) detailParts.push(`stage=${stage}`);
  if (currentUrl) detailParts.push(`url=${currentUrl}`);
  if (runPath) detailParts.push(`run=${runPath}`);
  if (screenshotPath) detailParts.push(`screenshot=${screenshotPath}`);

  const primaryText = stepGoal || message;
  const prefix = [scope, state].filter(Boolean).join(" ");
  const suffix = detailParts.length > 0 ? ` (${detailParts.join(", ")})` : "";
  const core =
    prefix && primaryText
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

function shouldSuppressFastBrowserProgressEvent(
  phase: string | null,
  status: string | null,
): boolean {
  const key = [phase || "", status || ""].join(":");
  return (
    key === "pre:start" ||
    key === "pre:ok" ||
    key === "post:start" ||
    key === "post:ok" ||
    key === "action:start"
  );
}

function formatFastBrowserEventState(
  phase: string | null,
  status: string | null,
): string {
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

async function runFastBrowserDaemonWorkflow(
  workflowRef: string,
  inputs: Record<string, unknown>,
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
  const clientModuleUrl = pathToFileURL(
    resolve(
      REPO_ROOT,
      "..",
      "ai-rules",
      "skills",
      "fast-browser",
      "lib",
      "daemon",
      "client.mjs",
    ),
  ).href;
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
  const executeBridge = async (): Promise<FastBrowserCommandResult> =>
    await new Promise((resolve, reject) => {
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
    const combinedOutput = [result.stdout, result.stderr]
      .filter(Boolean)
      .join("\n");
    if (await resetStuckFastBrowserDaemon(profileName, combinedOutput)) {
      result = await executeBridge();
    }
  }
  const response = parseFastBrowserJson<FastBrowserDaemonRunResponse>(
    { status: result.status, stdout: result.stdout },
    `fast-browser workflow ${workflowRef}`,
  );

  if (!response?.ok || !response.result) {
    throw buildFastBrowserWorkflowError(workflowRef, response);
  }

  if (response.result.status === "paused") {
    const reason = response.result.pause?.reason ?? "pause";
    const relay = response.result.pause?.relay_url
      ? ` Open ${response.result.pause.relay_url} to continue the workflow.`
      : "";
    throw new Error(
      `fast-browser workflow ${workflowRef} paused for ${reason}.${relay}`,
    );
  }

  return response.result;
}

async function resetStuckFastBrowserDaemon(
  profileName: string,
  output: string | null | undefined,
): Promise<boolean> {
  const match = output?.match(FAST_BROWSER_DAEMON_TIMEOUT_PATTERN);
  if (!match) {
    return false;
  }

  await resetManagedProfileRuntime(profileName, match[1]?.trim() || null);
  return true;
}

export function inspectManagedProfiles(): ManagedProfilesInspection {
  return parseFastBrowserJson<ManagedProfilesInspection>(
    runFastBrowserCommandSync(["inspect-profiles"], {
      requirePlaywright: false,
    }),
    "fast-browser inspect-profiles",
  );
}

function normalizeWorkflowScalar(
  rawValue: string | null | undefined,
): string | null {
  const trimmed = rawValue?.trim();
  if (!trimmed) return null;

  const withoutComment = trimmed.replace(/\s+#.*$/, "").trim();
  if (!withoutComment) return null;

  const first = withoutComment[0];
  const last = withoutComment[withoutComment.length - 1];
  if ((first === '"' && last === '"') || (first === "'" && last === "'")) {
    return withoutComment.slice(1, -1).trim() || null;
  }

  return withoutComment;
}

function parseWorkflowYamlDocument(
  raw: string,
): Record<string, unknown> | null {
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
      ? (parsed as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}

export function parseWorkflowFileMetadata(
  raw: string,
): Omit<WorkflowFileMetadata, "filePath"> {
  const parsed = parseWorkflowYamlDocument(raw);
  const document =
    parsed?.document &&
    typeof parsed.document === "object" &&
    !Array.isArray(parsed.document)
      ? (parsed.document as Record<string, unknown>)
      : null;
  const metadata =
    document?.metadata &&
    typeof document.metadata === "object" &&
    !Array.isArray(document.metadata)
      ? (document.metadata as Record<string, unknown>)
      : null;
  return {
    preferredProfileName: normalizeWorkflowScalar(
      typeof metadata?.preferredProfile === "string"
        ? metadata.preferredProfile
        : null,
    ),
    preferredEmail: normalizeWorkflowScalar(
      typeof metadata?.preferredEmail === "string"
        ? metadata.preferredEmail
        : null,
    ),
  };
}

export function readWorkflowFileMetadata(
  filePath: string,
): WorkflowFileMetadata {
  if (!existsSync(filePath)) {
    throw new Error(`Workflow file was not found at ${filePath}.`);
  }

  const raw = readFileSync(filePath, "utf8");
  return {
    filePath,
    ...parseWorkflowFileMetadata(raw),
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
      throw new Error(
        `Managed fast-browser profile "${requestedProfile}" was not found.`,
      );
    }
    return requestedProfile;
  }

  const preferredProfile = options?.preferredProfileName?.trim();
  if (preferredProfile) {
    if (!availableProfileNames.has(preferredProfile)) {
      const source = options?.preferredProfileSource
        ? ` from ${options.preferredProfileSource}`
        : "";
      throw new Error(
        `Managed fast-browser profile "${preferredProfile}"${source} was not found.`,
      );
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

export function resolveManagedProfileName(options?: {
  requestedProfileName?: string;
  preferredProfileName?: string | null;
  preferredProfileSource?: string | null;
}): string {
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
  return normalizeBaseEmailFamily("dev.{N}@astronlab.com");
}

export function shouldUseDefaultCreateFamilyHint(
  baseEmail: string | null | undefined,
): boolean {
  if (!baseEmail) {
    return false;
  }
  return parseEmailFamily(baseEmail).mode !== "gmail_plus";
}

async function runCodexBrowserLoginWorkflow(
  profileName: string,
  email: string,
  accountLoginLocator: CodexRotateSecretLocator | null,
  workflowRunStamp?: string,
  options?: {
    artifactMode?: "minimal" | "full";
    codexBin?: string;
    codexSession?: CodexRotateAuthFlowSession | null;
    preferSignupRecovery?: boolean;
    fullName?: string;
    birthMonth?: number;
    birthDay?: number;
    birthYear?: number;
  },
): Promise<FastBrowserRunResult> {
  const codexBin = ensureCodexLoginManagedBrowserWrapper(
    profileName,
    String(options?.codexBin || "codex").trim() || "codex",
  );
  return await runFastBrowserDaemonWorkflow(
    CODEX_ROTATE_ACCOUNT_FLOW_ID,
    {
      mode: "codex_login",
      codex_bin: codexBin,
      ...(options?.codexSession?.auth_url
        ? { auth_url: options.codexSession.auth_url }
        : {}),
      ...(options?.codexSession?.callback_url
        ? { callback_url: options.codexSession.callback_url }
        : {}),
      ...(options?.codexSession?.callback_port !== undefined &&
      options.codexSession.callback_port !== null
        ? { callback_port: String(options.codexSession.callback_port) }
        : {}),
      ...(options?.codexSession?.device_code
        ? { device_code: options.codexSession.device_code }
        : {}),
      ...(options?.codexSession?.session_dir
        ? { codex_session_dir: options.codexSession.session_dir }
        : {}),
      ...(options?.codexSession?.pid !== undefined &&
      options.codexSession.pid !== null
        ? { codex_login_pid: String(options.codexSession.pid) }
        : {}),
      ...(options?.codexSession?.stdout_path
        ? { codex_login_stdout_path: options.codexSession.stdout_path }
        : {}),
      ...(options?.codexSession?.stderr_path
        ? { codex_login_stderr_path: options.codexSession.stderr_path }
        : {}),
      ...(options?.codexSession?.exit_path
        ? { codex_login_exit_path: options.codexSession.exit_path }
        : {}),
      email,
      ...(accountLoginLocator
        ? { account_login_locator: accountLoginLocator }
        : {}),
      full_name: String(options?.fullName ?? DEFAULT_OPENAI_FULL_NAME).trim(),
      prefer_signup_recovery:
        options?.preferSignupRecovery === true ? "true" : "false",
      birth_month: String(options?.birthMonth ?? DEFAULT_OPENAI_BIRTH_MONTH),
      birth_day: String(options?.birthDay ?? DEFAULT_OPENAI_BIRTH_DAY),
      birth_year: String(options?.birthYear ?? DEFAULT_OPENAI_BIRTH_YEAR),
    },
    profileName,
    {
      workflowRunStamp,
      retainTemporaryProfilesOnSuccess: Boolean(workflowRunStamp),
      artifactMode: options?.artifactMode ?? "minimal",
    },
  );
}

export async function completeCodexLoginViaWorkflow(
  profileName: string,
  email: string,
  accountLoginLocator: CodexRotateSecretLocator | null,
  options?: {
    codexBin?: string;
    workflowRunStamp?: string;
    preferSignupRecovery?: boolean;
    fullName?: string;
    birthMonth?: number;
    birthDay?: number;
    birthYear?: number;
    maxAttempts?: number;
    maxReplayPasses?: number;
    retryDelaysMs?: readonly number[];
    onNote?: ((message: string) => void) | null;
    restoreState?: (() => void) | null;
  },
): Promise<CodexRotateAuthFlowSummary> {
  const workflowAccountLoginLocator =
    await resolveOptionalCodexRotateSecretLocator(
      profileName,
      accountLoginLocator,
    );

  const maxAttempts = Math.max(1, Number(options?.maxAttempts ?? 6));
  const maxReplayPasses = Math.max(1, Number(options?.maxReplayPasses ?? 5));
  const retryDelaysMs =
    Array.isArray(options?.retryDelaysMs) && options.retryDelaysMs.length > 0
      ? options.retryDelaysMs
      : [30_000, 60_000, 120_000, 240_000, 300_000];
  const note = typeof options?.onNote === "function" ? options.onNote : null;
  const restoreState =
    typeof options?.restoreState === "function" ? options.restoreState : null;
  let allowSignupRecovery = options?.preferSignupRecovery === true;
  let codexSession: CodexRotateAuthFlowSession | null = null;

  const sleep = async (milliseconds: number) =>
    await new Promise((resolve) => {
      setTimeout(resolve, milliseconds);
    });

  try {
    if (workflowAccountLoginLocator) {
      note?.(
        `Found a stored OpenAI login secret for ${email}; attempting password login first.`,
      );
    } else {
      note?.(
        `No stored OpenAI login secret was found for ${email}; using one-time-code recovery.`,
      );
    }

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      try {
        note?.(
          attempt === 1
            ? `Completing Codex login in managed profile "${profileName}".`
            : `Retrying Codex login in managed profile "${profileName}" (attempt ${attempt}/${maxAttempts}).`,
        );

        for (
          let replayPass = 1;
          replayPass <= maxReplayPasses;
          replayPass += 1
        ) {
          const loginWorkflowRunStamp = options?.workflowRunStamp
            ? `${options.workflowRunStamp}-codex-login-${attempt}-${replayPass}`
            : undefined;
          const loginResult = await runCodexBrowserLoginWorkflow(
            profileName,
            email,
            workflowAccountLoginLocator,
            loginWorkflowRunStamp,
            {
              codexBin: options?.codexBin,
              codexSession,
              preferSignupRecovery: allowSignupRecovery,
              fullName: options?.fullName,
              birthMonth: options?.birthMonth,
              birthDay: options?.birthDay,
              birthYear: options?.birthYear,
            },
          );
          const flow = readCodexRotateAuthFlowSummary(loginResult);
          codexSession =
            readCodexRotateAuthFlowSession(loginResult) ?? codexSession;
          const callbackComplete = flow.callback_complete === true;
          const success = flow.success === true;
          const currentUrl =
            typeof flow.current_url === "string" ? flow.current_url : null;
          const nextAction =
            typeof flow.next_action === "string" ? flow.next_action : null;
          const replayReason =
            typeof flow.replay_reason === "string" ? flow.replay_reason : null;
          const retryReason =
            typeof flow.retry_reason === "string" ? flow.retry_reason : null;
          const errorMessage =
            typeof flow.error_message === "string" && flow.error_message.trim()
              ? flow.error_message.trim()
              : null;
          const sawOauthConsent = flow.saw_oauth_consent === true;
          const existingAccountPrompt = flow.existing_account_prompt === true;

          if (
            sawOauthConsent ||
            existingAccountPrompt ||
            (replayReason && replayReason !== "auth_prompt")
          ) {
            allowSignupRecovery = false;
          }
          if (nextAction === "fail_invalid_credentials") {
            throw new Error(
              errorMessage ??
                `OpenAI rejected the stored password for ${email}.`,
            );
          }
          if (
            nextAction === "replay_auth_url" &&
            replayPass < maxReplayPasses
          ) {
            const replayReasonLabel = replayReason
              ? replayReason.replace(/_/g, " ")
              : "the next auth step";
            note?.(
              `OpenAI still needs ${replayReasonLabel} for ${email}${currentUrl ? ` (${currentUrl})` : ""}. ` +
                `Replaying the workflow-owned Codex auth session in managed profile "${profileName}" (${replayPass + 1}/${maxReplayPasses}).`,
            );
            await sleep(1000);
            continue;
          }
          if (nextAction === "retry_attempt") {
            restoreState?.();
            if (attempt < maxAttempts) {
              const delayMs =
                retryDelaysMs[
                  Math.min(attempt - 1, retryDelaysMs.length - 1)
                ] ?? 30_000;
              const retryReasonLabel = retryReason
                ? retryReason.replace(/_/g, " ")
                : "needs another retry";
              if (retryReason === "retryable_timeout") {
                codexSession = null;
              }
              note?.(
                `OpenAI ${retryReasonLabel} for ${email}${currentUrl ? ` (${currentUrl})` : ""}. ` +
                  `${retryReason === "retryable_timeout" ? "Starting a fresh Codex auth session. " : ""}` +
                  `Waiting ${Math.round(delayMs / 1000)}s before retrying.`,
              );
              await sleep(delayMs);
              break;
            }
            throw new Error(
              errorMessage ??
                `OpenAI could not complete the Codex login for ${email}.`,
            );
          }
          if (!callbackComplete && !success) {
            throw new Error(
              errorMessage ??
                `Codex browser login did not reach the callback for ${email}${currentUrl ? ` (${currentUrl})` : ""}.`,
            );
          }
          if (flow.codex_login_exit_ok === false) {
            throw new Error(
              `"codex login" did not exit cleanly for ${email}.` +
                `${flow.codex_login_stderr_tail ? `\n${flow.codex_login_stderr_tail}` : ""}`,
            );
          }
          return flow;
        }
      } catch (error) {
        restoreState?.();
        const failedResult = readFastBrowserResultFromError(error);
        if (failedResult) {
          codexSession =
            readCodexRotateAuthFlowSession(failedResult) ?? codexSession;
        }
        const message = error instanceof Error ? error.message : String(error);
        const verificationArtifactPending =
          isRetryableCodexLoginWorkflowErrorMessage(message);
        const deviceAuthRateLimited =
          /device code request failed with status 429|device auth failed with status 429|codex-login-exited-before-auth-url:.*429 Too Many Requests|429 Too Many Requests/i.test(
            message,
          );
        if (verificationArtifactPending && attempt < maxAttempts) {
          const delayMs =
            retryDelaysMs[Math.min(attempt - 1, retryDelaysMs.length - 1)] ??
            30_000;
          note?.(
            `OpenAI verification is not ready for ${email}. ` +
              `Waiting ${Math.round(delayMs / 1000)}s before retrying the same managed-profile flow.`,
          );
          await sleep(delayMs);
          continue;
        }
        if (deviceAuthRateLimited && attempt < maxAttempts) {
          const delayMs =
            retryDelaysMs[Math.min(attempt - 1, retryDelaysMs.length - 1)] ??
            30_000;
          note?.(
            `Codex device authorization is rate limited for ${email}. ` +
              `Waiting ${Math.round(delayMs / 1000)}s before retrying.`,
          );
          await sleep(delayMs);
          continue;
        }
        throw error;
      }
    }

    restoreState?.();
    throw new Error(
      `Codex browser login exhausted all retry attempts for ${email}.`,
    );
  } finally {
    if (codexSession) {
      cancelCodexBrowserLoginSession(codexSession);
    }
  }
}

function normalizeCredentialRecordMap(
  raw: unknown,
): Record<string, StoredCredential> {
  if (!isRecord(raw)) {
    return {};
  }
  const entries = Object.entries(raw)
    .map(([email, value]) => {
      const normalized = normalizeCredentialRecord(value);
      return normalized ? ([email, normalized] as const) : null;
    })
    .filter((entry): entry is readonly [string, StoredCredential] =>
      Boolean(entry),
    );
  return Object.fromEntries(entries);
}

function normalizeCredentialRecord(raw: unknown): StoredCredential | null {
  if (!isRecord(raw)) {
    return null;
  }
  const normalized = { ...raw } as Record<string, unknown>;
  delete normalized.password;
  delete normalized.account_secret_ref;
  return normalized as unknown as StoredCredential;
}

function serializeCredentialRecordMap(
  raw: Record<string, StoredCredential>,
): Record<string, StoredCredential> {
  return Object.fromEntries(
    Object.entries(raw).map(([email, value]) => [
      email,
      serializeCredentialRecord(value),
    ]),
  );
}

export function serializeCredentialStore(
  store: CredentialStore,
): CredentialStore {
  return {
    version: 3,
    families: store.families,
    pending: serializeCredentialRecordMap(store.pending) as Record<
      string,
      PendingCredential
    >,
  };
}

function serializeCredentialRecord(raw: StoredCredential): StoredCredential {
  const serialized: Record<string, unknown> = {
    email: raw.email,
    profile_name: raw.profile_name,
    base_email: raw.base_email,
    suffix: raw.suffix,
    selector: raw.selector,
    alias: raw.alias,
    created_at: raw.created_at,
    updated_at: raw.updated_at,
  };
  if (typeof raw.birth_month === "number") {
    serialized.birth_month = raw.birth_month;
  }
  if (typeof raw.birth_day === "number") {
    serialized.birth_day = raw.birth_day;
  }
  if (typeof raw.birth_year === "number") {
    serialized.birth_year = raw.birth_year;
  }
  if (
    "started_at" in raw &&
    typeof (raw as PendingCredential).started_at === "string"
  ) {
    serialized.started_at = (raw as PendingCredential).started_at;
  }
  return serialized as unknown as StoredCredential;
}

function normalizeCodexRotateSecretRef(
  raw: unknown,
): CodexRotateSecretRef | null {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    return null;
  }
  const record = raw as Record<string, unknown>;
  const objectId =
    typeof record.object_id === "string" ? record.object_id.trim() : "";
  if (!objectId) {
    return null;
  }
  const store =
    typeof record.store === "string" && record.store.trim()
      ? record.store.trim()
      : "bitwarden-cli";
  if (store !== "bitwarden-cli") {
    return null;
  }
  const type = record.type === undefined ? "secret_ref" : record.type;
  if (type !== "secret_ref") {
    return null;
  }
  return {
    type: "secret_ref",
    store: "bitwarden-cli",
    object_id: objectId,
    field_path:
      typeof record.field_path === "string" ? record.field_path : null,
    version: typeof record.version === "string" ? record.version : null,
  };
}

function buildCodexRotateAccountSecretName(email: string): string {
  return `codex-rotate/openai/${String(email || "")
    .trim()
    .toLowerCase()}`;
}

function readWorkflowActionString(
  result: FastBrowserRunResult,
  stepId: string,
  field: string,
): string | null {
  const step = result.state?.steps?.[stepId];
  const value = step?.action?.[field];
  return typeof value === "string" && value.trim() ? value : null;
}

function readWorkflowOutputRecord<T>(result: FastBrowserRunResult): T | null {
  const value = result.output;
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as T;
}

function readCodexRotateAuthFlowSummary(
  result: FastBrowserRunResult,
): CodexRotateAuthFlowSummary {
  return readWorkflowOutputRecord<CodexRotateAuthFlowSummary>(result) ?? {};
}

function normalizeCodexRotateAuthFlowSession(
  raw: unknown,
): CodexRotateAuthFlowSession | null {
  if (!isRecord(raw)) {
    return null;
  }
  const callbackPort = raw.callback_port;
  const pid = raw.pid;
  const session: CodexRotateAuthFlowSession = {
    auth_url:
      typeof raw.auth_url === "string" && raw.auth_url.trim()
        ? raw.auth_url.trim()
        : null,
    callback_url:
      typeof raw.callback_url === "string" && raw.callback_url.trim()
        ? raw.callback_url.trim()
        : null,
    callback_port:
      typeof callbackPort === "number"
        ? callbackPort
        : typeof callbackPort === "string" && callbackPort.trim()
          ? Number.parseInt(callbackPort, 10)
          : null,
    device_code:
      typeof raw.device_code === "string" && raw.device_code.trim()
        ? raw.device_code.trim()
        : null,
    session_dir:
      typeof raw.session_dir === "string" && raw.session_dir.trim()
        ? raw.session_dir.trim()
        : null,
    pid:
      typeof pid === "number"
        ? pid
        : typeof pid === "string" && pid.trim()
          ? Number.parseInt(pid, 10)
          : null,
    stdout_path:
      typeof raw.stdout_path === "string" && raw.stdout_path.trim()
        ? raw.stdout_path.trim()
        : null,
    stderr_path:
      typeof raw.stderr_path === "string" && raw.stderr_path.trim()
        ? raw.stderr_path.trim()
        : null,
    exit_path:
      typeof raw.exit_path === "string" && raw.exit_path.trim()
        ? raw.exit_path.trim()
        : null,
  };
  if (
    !session.auth_url &&
    !session.session_dir &&
    !session.stdout_path &&
    !session.stderr_path &&
    !session.exit_path
  ) {
    return null;
  }
  return session;
}

function readCodexRotateAuthFlowSession(
  result: FastBrowserRunResult,
): CodexRotateAuthFlowSession | null {
  const summary = readCodexRotateAuthFlowSummary(result);
  const summarySession = normalizeCodexRotateAuthFlowSession(
    summary.codex_session,
  );
  if (summarySession) {
    return summarySession;
  }
  const startStepAction =
    result.state?.steps?.start_codex_login_session?.action;
  if (isRecord(startStepAction)) {
    return normalizeCodexRotateAuthFlowSession(
      startStepAction.value ?? startStepAction,
    );
  }
  return null;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function cancelCodexBrowserLoginSession(
  session: CodexRotateAuthFlowSession | null | undefined,
): void {
  const pid = Number(session?.pid || 0);
  if (!Number.isInteger(pid) || pid <= 1) {
    return;
  }
  try {
    process.kill(pid, 0);
  } catch {
    return;
  }
  try {
    process.kill(pid, "SIGTERM");
  } catch {}
}
