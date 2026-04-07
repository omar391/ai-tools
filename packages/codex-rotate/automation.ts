import { createHash } from "node:crypto";
import { spawn, spawnSync, type SpawnSyncReturns } from "node:child_process";
import {
  chmodSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import {
  basename,
  dirname,
  extname,
  join,
  relative,
  resolve,
  sep,
} from "node:path";
import process from "node:process";
import { fileURLToPath, pathToFileURL } from "node:url";

const MODULE_DIR = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(MODULE_DIR, "..", "..");
const DEFAULT_ROTATE_HOME = join(homedir(), ".codex-rotate");
let ROTATE_HOME = resolve(process.env.CODEX_ROTATE_HOME || DEFAULT_ROTATE_HOME);
const CODEX_HOME = resolve(process.env.CODEX_HOME || join(homedir(), ".codex"));
const CODEX_AUTH_FILE = join(CODEX_HOME, "auth.json");
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
const NODE_BINARY =
  process.env.CODEX_ROTATE_NODE_BIN?.trim() ||
  process.env.NODE_BIN?.trim() ||
  process.execPath ||
  "node";
const FAST_BROWSER_RUNTIME =
  process.env.CODEX_ROTATE_FAST_BROWSER_RUNTIME ?? NODE_BINARY;
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
const FAST_BROWSER_BITWARDEN_SESSION_MODULE = pathToFileURL(
  resolve(
    REPO_ROOT,
    "..",
    "ai-rules",
    "skills",
    "fast-browser",
    "lib",
    "secret-adapters",
    "bitwarden-session.mjs",
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

const DEFAULT_CODEX_ROTATE_ACCOUNT_FLOW_ID =
  "workspace.web.auth-openai-com.codex-rotate-account-flow-main";
export const CODEX_ROTATE_ACCOUNT_FLOW_FILE = resolve(
  process.env.CODEX_ROTATE_ACCOUNT_FLOW_FILE ||
    join(
      REPO_ROOT,
      ".fast-browser",
      "workflows",
      "web",
      "auth.openai.com",
      "codex-rotate-account-flow-main.yaml",
    ),
);

const CODEX_ROTATE_AUTH_FLOW_ARTIFACT_MODE: "minimal" | "full" =
  process.env.CODEX_ROTATE_AUTH_FLOW_ARTIFACT_MODE === "full"
    ? "full"
    : "minimal";
export const CODEX_ROTATE_OPENAI_TEMP_RUNTIME_KEY = "openai-account-runtime";
const FAST_BROWSER_WORKFLOWS_ROOT = resolve(
  REPO_ROOT,
  ".fast-browser",
  "workflows",
);
const FAST_BROWSER_GLOBAL_WORKFLOWS_ROOT = resolve(
  REPO_ROOT,
  "..",
  "ai-rules",
  "skills",
  "fast-browser",
  "workflows",
);
const OPENAI_ACCOUNT_SECRET_URIS = [
  "https://auth.openai.com",
  "https://chatgpt.com",
];
const DEFAULT_OPENAI_FULL_NAME = "Dev Astronlab";
const DEFAULT_OPENAI_BIRTH_MONTH = 1;
const DEFAULT_OPENAI_BIRTH_DAY = 24;
const DEFAULT_OPENAI_BIRTH_YEAR = 1990;
const DEFAULT_CODEX_LOGIN_RETRY_DELAYS_MS = [
  15_000, 30_000, 60_000, 120_000, 240_000,
] as const;
const DEFAULT_CODEX_LOGIN_VERIFICATION_RETRY_DELAYS_MS = [
  5_000, 10_000, 20_000, 30_000, 60_000,
] as const;
const DEFAULT_CODEX_LOGIN_RETRYABLE_TIMEOUT_DELAYS_MS = [
  8_000, 15_000, 30_000, 60_000, 120_000,
] as const;
const DEFAULT_CODEX_LOGIN_RATE_LIMIT_RETRY_DELAYS_MS = [
  30_000, 60_000, 120_000, 240_000, 300_000,
] as const;
const BITWARDEN_BINARY = process.env.CODEX_ROTATE_BW_BIN?.trim() || "bw";
const FAST_BROWSER_PLAYWRIGHT_MODULE = resolvePlaywrightModulePath();
const FAST_BROWSER_NODE_PATH = dirname(FAST_BROWSER_PLAYWRIGHT_MODULE);
const LEGACY_ROTATE_HOME_FILE_PATTERNS = [
  /^codex-login-browser-capture-.*\.js$/,
  /^fast-browser-.*\.json$/,
];
const LEGACY_ROTATE_HOME_DIR_PATTERNS = [/^codex-login-browser-shim-.+$/];
const LEGACY_ROTATE_HOME_BIN_FILE_PATTERNS = [/^codex-login-managed-.+$/];
const CURRENT_CODEX_LOGIN_WRAPPER_PATTERN =
  /^codex-login-[a-z0-9._-]+-[0-9a-f]{12}$/;

export function shouldPromptForCodexRotateSecretUnlock(): boolean {
  return (
    process.env.CODEX_ROTATE_ALLOW_INTERACTIVE_SECRET_UNLOCK === "1" ||
    (process.stdin.isTTY && process.stderr.isTTY)
  );
}

function resolvePlaywrightModulePath(): string {
  const override = process.env.CODEX_ROTATE_PLAYWRIGHT_MODULE?.trim();
  const directCandidates = [
    override ? resolve(override) : null,
    join(REPO_ROOT, "node_modules", "playwright"),
    join(process.cwd(), "node_modules", "playwright"),
  ].filter((value): value is string => Boolean(value));
  for (const candidate of directCandidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }

  const siblingRepoMarker = join("packages", "codex-rotate", "package.json");
  try {
    for (const entry of readdirSync(dirname(REPO_ROOT), {
      withFileTypes: true,
    })) {
      if (!entry.isDirectory()) {
        continue;
      }
      const siblingRoot = join(dirname(REPO_ROOT), entry.name);
      if (siblingRoot === REPO_ROOT) {
        continue;
      }
      if (!existsSync(join(siblingRoot, siblingRepoMarker))) {
        continue;
      }
      const candidate = join(siblingRoot, "node_modules", "playwright");
      if (existsSync(candidate)) {
        return candidate;
      }
    }
  } catch {}

  return join(REPO_ROOT, "node_modules", "playwright");
}

function buildNodePathEnv(extraPath: string): string {
  const entries = [extraPath, process.env.NODE_PATH || ""].filter(Boolean);
  return entries.join(process.platform === "win32" ? ";" : ":");
}

export function getCodexRotateHome(): string {
  return ROTATE_HOME;
}

export function setCodexRotateHomeForTesting(rootDir: string | null): void {
  ROTATE_HOME = resolve(rootDir || DEFAULT_ROTATE_HOME);
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
const FAST_BROWSER_STARTUP_SILENCE_TIMEOUT_MS = 15_000;
const FAST_BROWSER_DAEMON_SOCKET_CLOSED_PATTERN =
  /Daemon closed the socket before sending a response/i;
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
  codex_home_path?: string | null;
  auth_file_path?: string | null;
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
  cleanupLegacyCodexRotateArtifacts(ROTATE_HOME);
}

function matchesAnyPattern(
  value: string,
  patterns: readonly RegExp[],
): boolean {
  return patterns.some((pattern) => pattern.test(value));
}

export function cleanupLegacyCodexRotateArtifacts(rootDir = ROTATE_HOME): void {
  if (!existsSync(rootDir)) {
    return;
  }

  for (const entry of readdirSync(rootDir, { withFileTypes: true })) {
    const entryPath = join(rootDir, entry.name);
    if (
      entry.isFile() &&
      matchesAnyPattern(entry.name, LEGACY_ROTATE_HOME_FILE_PATTERNS)
    ) {
      rmSync(entryPath, { force: true });
      continue;
    }
    if (
      entry.isDirectory() &&
      matchesAnyPattern(entry.name, LEGACY_ROTATE_HOME_DIR_PATTERNS)
    ) {
      rmSync(entryPath, { recursive: true, force: true });
      continue;
    }
    if (!entry.isDirectory() || entry.name !== "bin") {
      continue;
    }
    for (const binEntry of readdirSync(entryPath, { withFileTypes: true })) {
      const binEntryPath = join(entryPath, binEntry.name);
      const binEntryContents =
        binEntry.isFile() &&
        CURRENT_CODEX_LOGIN_WRAPPER_PATTERN.test(binEntry.name)
          ? readFileSync(binEntryPath, "utf8")
          : null;
      if (
        binEntry.isFile() &&
        (matchesAnyPattern(
          binEntry.name,
          LEGACY_ROTATE_HOME_BIN_FILE_PATTERNS,
        ) ||
          (typeof binEntryContents === "string" &&
            !binEntryContents.includes("codex-login-app-server-helper.mjs")))
      ) {
        rmSync(binEntryPath, { force: true });
      }
    }
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
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

async function findBitwardenCliAccountSecretRefWithOptions(
  profileName: string,
  email: string,
  promptIfLocked: boolean,
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
    promptIfLocked,
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

export async function findBitwardenCliAccountSecretRef(
  profileName: string,
  email: string,
): Promise<CodexRotateSecretRef | null> {
  return findBitwardenCliAccountSecretRefWithOptions(
    profileName,
    email,
    shouldPromptForCodexRotateSecretUnlock(),
  );
}

export async function deleteBitwardenCliAccountSecretRef(
  profileName: string,
  email: string,
): Promise<boolean> {
  const normalizedProfileName = String(profileName || "").trim();
  const normalizedEmail = String(email || "")
    .trim()
    .toLowerCase();
  if (!normalizedProfileName) {
    throw new Error(
      "Bitwarden account secret deletion requires a managed profile name.",
    );
  }
  if (!normalizedEmail) {
    throw new Error(
      "Bitwarden account secret deletion requires a non-empty email.",
    );
  }

  const { ensureDaemonSecretStoreReadyInteractive } = await import(
    FAST_BROWSER_DAEMON_CLIENT_MODULE
  );
  await ensureDaemonSecretStoreReadyInteractive({
    profileName: normalizedProfileName,
    store: "bitwarden-cli",
    promptIfLocked: false,
  });

  const ref = await findBitwardenCliAccountSecretRefWithOptions(
    normalizedProfileName,
    normalizedEmail,
    false,
  );
  if (!ref) {
    return false;
  }

  const { buildBitwardenCliEnv } = await import(
    FAST_BROWSER_BITWARDEN_SESSION_MODULE
  );
  const result = spawnSync(
    BITWARDEN_BINARY,
    ["delete", "item", ref.object_id],
    {
      env: buildBitwardenCliEnv(process.env),
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      timeout: 60_000,
    },
  );
  if (result.error) {
    throw result.error;
  }
  if ((result.status ?? 1) !== 0) {
    const detail = [result.stderr, result.stdout]
      .map((value) => String(value || "").trim())
      .find((value) => value.length > 0);
    throw new Error(
      detail ||
        `Bitwarden CLI failed while deleting the vault item for ${normalizedEmail}.`,
    );
  }
  return true;
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
        "Install the repo dependencies before using create/relogin automation.",
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
  nodeBin: string,
  openerPath: string,
  loginHelperPath: string,
): string {
  return [
    "#!/bin/sh",
    `export FAST_BROWSER_PROFILE=${shellSingleQuote(profileName)}`,
    `export BROWSER=${shellSingleQuote(openerPath)}`,
    `export PATH=${shellSingleQuote(shimDir)}:"$PATH"`,
    `export CODEX_ROTATE_NODE_BIN=${shellSingleQuote(nodeBin)}`,
    `export CODEX_ROTATE_REAL_CODEX=${shellSingleQuote(realCodexBin)}`,
    'if [ "$1" = "login" ]; then',
    "  shift",
    `  exec ${shellSingleQuote(nodeBin)} ${shellSingleQuote(loginHelperPath)} "$@"`,
    "fi",
    `exec ${shellSingleQuote(realCodexBin)} \"$@\"`,
    "",
  ].join("\n");
}

function ensureCodexLoginManagedBrowserShims(
  shimDir: string,
  nodeBin: string,
  openerPath: string,
): void {
  mkdirSync(shimDir, { recursive: true });
  const shimContent = [
    "#!/bin/sh",
    `exec ${shellSingleQuote(nodeBin)} ${shellSingleQuote(openerPath)} \"$@\"`,
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
  ensureCodexLoginManagedBrowserShims(shimDir, NODE_BINARY, openerPath);
  const wrapperPath = buildCodexLoginManagedBrowserWrapperPath(
    profileName,
    codexBin,
  );
  const content = renderCodexLoginManagedBrowserWrapper(
    codexBin,
    profileName,
    shimDir,
    NODE_BINARY,
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

export function getCodexLoginRetryDelaysMs(
  reason: string | null,
  overrideDelaysMs?: readonly number[] | null,
): readonly number[] {
  if (Array.isArray(overrideDelaysMs) && overrideDelaysMs.length > 0) {
    return overrideDelaysMs;
  }
  switch (reason) {
    case "verification_artifact_pending":
      return DEFAULT_CODEX_LOGIN_VERIFICATION_RETRY_DELAYS_MS;
    case "retryable_timeout":
      return DEFAULT_CODEX_LOGIN_RETRYABLE_TIMEOUT_DELAYS_MS;
    case "device_auth_rate_limit":
    case "rate_limit":
      return DEFAULT_CODEX_LOGIN_RATE_LIMIT_RETRY_DELAYS_MS;
    default:
      return DEFAULT_CODEX_LOGIN_RETRY_DELAYS_MS;
  }
}

export function getCodexLoginRetryDelayMs(
  reason: string | null,
  attempt: number,
  overrideDelaysMs?: readonly number[] | null,
): number {
  const delays = getCodexLoginRetryDelaysMs(reason, overrideDelaysMs);
  const index = Math.max(0, Math.min(attempt - 1, delays.length - 1));
  return delays[index] ?? DEFAULT_CODEX_LOGIN_RETRY_DELAYS_MS[0];
}

export function shouldResetCodexLoginSessionForRetry(
  retryReason: string | null,
  attempt: number,
): boolean {
  return (
    retryReason === "state_mismatch" ||
    (retryReason === "retryable_timeout" && attempt >= 2)
  );
}

export function shouldResetDeviceAuthSessionForRateLimit(
  message: string,
  session: CodexRotateAuthFlowSession | null | undefined,
): boolean {
  const normalized = String(message || "")
    .trim()
    .toLowerCase();
  if (!normalized) {
    return true;
  }
  const hasReusableDeviceChallenge =
    typeof session?.auth_url === "string" &&
    session.auth_url.trim().length > 0 &&
    typeof session?.device_code === "string" &&
    session.device_code.trim().length > 0;
  if (
    /device auth failed with status 429|device auth failed:.*429 too many requests/i.test(
      normalized,
    ) &&
    hasReusableDeviceChallenge
  ) {
    return false;
  }
  return true;
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
      const child = spawn(
        NODE_BINARY,
        ["--input-type=module", "-e", bridgeScript],
        {
          cwd: REPO_ROOT,
          env: {
            ...process.env,
            FAST_BROWSER_WORKSPACE_ROOT: REPO_ROOT,
            NODE_PATH: buildNodePathEnv(FAST_BROWSER_NODE_PATH),
          },
          stdio: ["ignore", "pipe", "pipe"],
        },
      );
      let settled = false;
      let sawFirstProgressEvent = false;
      let stdout = "";
      let stderr = "";
      let stderrBuffer = "";
      const socketPath = join(FAST_BROWSER_DAEMON_DIR, `${profileName}.sock`);
      const startupSilenceTimer = setTimeout(() => {
        if (settled || sawFirstProgressEvent) {
          return;
        }
        settled = true;
        child.kill("SIGKILL");
        reject(
          new Error(
            `Timed out waiting for fast-browser daemon response from ${socketPath}`,
          ),
        );
      }, FAST_BROWSER_STARTUP_SILENCE_TIMEOUT_MS);

      const flushStderrLine = (line: string): void => {
        const progressEvent = parseFastBrowserProgressEventLine(line);
        if (progressEvent) {
          sawFirstProgressEvent = true;
          clearTimeout(startupSilenceTimer);
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

      child.once("error", (error) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(startupSilenceTimer);
        reject(error);
      });
      child.once("close", (code, signal) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(startupSilenceTimer);
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

  let result: FastBrowserCommandResult;
  try {
    result = await executeBridge();
  } catch (error) {
    const message =
      error instanceof Error ? error.message : String(error || "");
    if (!(await resetStuckFastBrowserDaemon(profileName, message))) {
      throw error;
    }
    result = await executeBridge();
  }
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
  if (match) {
    await resetManagedProfileRuntime(profileName, match[1]?.trim() || null);
    return true;
  }

  if (shouldResetFastBrowserDaemonForSocketClose(output)) {
    await resetManagedProfileRuntime(profileName, null);
    return true;
  }

  return false;
}

export function shouldResetFastBrowserDaemonForSocketClose(
  output: string | null | undefined,
): boolean {
  return FAST_BROWSER_DAEMON_SOCKET_CLOSED_PATTERN.test(String(output || ""));
}

export function inspectManagedProfiles(): ManagedProfilesInspection {
  return parseFastBrowserJson<ManagedProfilesInspection>(
    runFastBrowserCommandSync(["inspect-profiles"], {
      requirePlaywright: false,
    }),
    "fast-browser inspect-profiles",
  );
}

function slugifyWorkflowPathSegment(value: string): string | null {
  const slug = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-");
  return slug.replace(/^-+|-+$/g, "") || null;
}

function deriveWorkflowRefFromRoot(
  filePath: string,
  rootDir: string,
  scopePrefix: "workspace" | "sys",
): string | null {
  const candidatePath = resolve(filePath);
  const relativePath = relative(rootDir, candidatePath);
  if (
    !relativePath ||
    relativePath === ".." ||
    relativePath.startsWith(`..${sep}`) ||
    extname(relativePath) !== ".yaml"
  ) {
    return null;
  }

  const segments = relativePath.split(sep).filter(Boolean);
  if (segments.length !== 3) {
    return null;
  }

  const [surface, target, workflowFile] = segments;
  const workflowName = basename(workflowFile, ".yaml");
  const parts = [
    scopePrefix,
    slugifyWorkflowPathSegment(surface),
    slugifyWorkflowPathSegment(target),
    slugifyWorkflowPathSegment(workflowName),
  ].filter((part): part is string => Boolean(part));

  return parts.length === 4 ? parts.join(".") : null;
}

export function deriveWorkflowRefFromFilePath(filePath: string): string | null {
  return (
    deriveWorkflowRefFromRoot(
      filePath,
      FAST_BROWSER_WORKFLOWS_ROOT,
      "workspace",
    ) ||
    deriveWorkflowRefFromRoot(
      filePath,
      FAST_BROWSER_GLOBAL_WORKFLOWS_ROOT,
      "sys",
    )
  );
}

async function runCodexBrowserLoginWorkflow(
  profileName: string,
  email: string,
  accountLoginLocator: CodexRotateSecretLocator | null,
  workflowRunStamp?: string,
  options?: {
    artifactMode?: "minimal" | "full";
    codexBin?: string;
    workflowFile?: string;
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
  const workflowFile =
    typeof options?.workflowFile === "string" &&
    options.workflowFile.trim().length > 0
      ? resolve(options.workflowFile)
      : CODEX_ROTATE_ACCOUNT_FLOW_FILE;
  const workflowRef =
    deriveWorkflowRefFromFilePath(workflowFile) ??
    DEFAULT_CODEX_ROTATE_ACCOUNT_FLOW_ID;
  return await runFastBrowserDaemonWorkflow(
    workflowRef,
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
      artifactMode:
        options?.artifactMode ?? CODEX_ROTATE_AUTH_FLOW_ARTIFACT_MODE,
    },
  );
}

export async function completeCodexLoginViaWorkflow(
  profileName: string,
  email: string,
  accountLoginLocator: CodexRotateSecretLocator | null,
  options?: {
    codexBin?: string;
    workflowFile?: string;
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
      : null;
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
              workflowFile: options?.workflowFile,
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
          const consentError =
            typeof flow.consent_error === "string" && flow.consent_error.trim()
              ? flow.consent_error.trim()
              : null;
          const stateMismatch =
            consentError === "state_mismatch" ||
            (callbackComplete &&
              flow.codex_login_exit_ok === false &&
              /state mismatch/i.test(
                [
                  flow.headline,
                  flow.codex_login_stderr_tail,
                  flow.codex_login_stdout_tail,
                  errorMessage,
                ]
                  .filter((value): value is string => typeof value === "string")
                  .join("\n"),
              ));
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
              const delayMs = getCodexLoginRetryDelayMs(
                retryReason,
                attempt,
                retryDelaysMs,
              );
              const retryReasonLabel = retryReason
                ? retryReason.replace(/_/g, " ")
                : "needs another retry";
              if (shouldResetCodexLoginSessionForRetry(retryReason, attempt)) {
                codexSession = null;
              }
              note?.(
                `OpenAI ${retryReasonLabel} for ${email}${currentUrl ? ` (${currentUrl})` : ""}. ` +
                  `${shouldResetCodexLoginSessionForRetry(retryReason, attempt) ? "Starting a fresh Codex auth session. " : ""}` +
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
          if (stateMismatch) {
            restoreState?.();
            if (attempt < maxAttempts) {
              const delayMs = getCodexLoginRetryDelayMs(
                "state_mismatch",
                attempt,
                retryDelaysMs,
              );
              codexSession = null;
              note?.(
                `OpenAI returned a state mismatch during the Codex callback for ${email}${currentUrl ? ` (${currentUrl})` : ""}. ` +
                  `Starting a fresh Codex auth session and retrying in ${Math.round(delayMs / 1000)}s.`,
              );
              await sleep(delayMs);
              break;
            }
            throw new Error(
              errorMessage ??
                `OpenAI returned a state mismatch during the Codex callback for ${email}${currentUrl ? ` (${currentUrl})` : ""}.`,
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
          promoteCodexAuthFromSession(
            readCodexRotateAuthFlowSession(loginResult),
          );
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
          const delayMs = getCodexLoginRetryDelayMs(
            "verification_artifact_pending",
            attempt,
            retryDelaysMs,
          );
          note?.(
            `OpenAI verification is not ready for ${email}. ` +
              `Waiting ${Math.round(delayMs / 1000)}s before retrying the same managed-profile flow.`,
          );
          await sleep(delayMs);
          continue;
        }
        if (deviceAuthRateLimited && attempt < maxAttempts) {
          const delayMs = getCodexLoginRetryDelayMs(
            "device_auth_rate_limit",
            attempt,
            retryDelaysMs,
          );
          const resetDeviceAuthSession =
            shouldResetDeviceAuthSessionForRateLimit(message, codexSession);
          if (resetDeviceAuthSession) {
            codexSession = null;
          }
          note?.(
            `Codex device authorization is rate limited for ${email}. ` +
              `${resetDeviceAuthSession ? "" : "Reusing the existing device code session when retrying. "}` +
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
    codex_home_path:
      typeof raw.codex_home_path === "string" && raw.codex_home_path.trim()
        ? raw.codex_home_path.trim()
        : null,
    auth_file_path:
      typeof raw.auth_file_path === "string" && raw.auth_file_path.trim()
        ? raw.auth_file_path.trim()
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
    !session.codex_home_path &&
    !session.auth_file_path &&
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

function promoteCodexAuthFromSession(
  session: CodexRotateAuthFlowSession | null | undefined,
): void {
  const authFilePath =
    typeof session?.auth_file_path === "string" &&
    session.auth_file_path.trim().length > 0
      ? resolve(session.auth_file_path)
      : null;
  if (!authFilePath) {
    return;
  }
  if (!existsSync(authFilePath)) {
    throw new Error(
      `Codex device authorization completed without producing ${authFilePath}.`,
    );
  }
  mkdirSync(dirname(CODEX_AUTH_FILE), { recursive: true });
  copyFileSync(authFilePath, CODEX_AUTH_FILE);
}
