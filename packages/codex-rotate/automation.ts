import { spawn, spawnSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  readdirSync,
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
const DEFAULT_ROTATE_HOME = join(homedir(), ".codex-rotate");
let ROTATE_HOME = resolve(process.env.CODEX_ROTATE_HOME || DEFAULT_ROTATE_HOME);
const FAST_BROWSER_HOME = join(homedir(), ".fast-browser");
const FAST_BROWSER_PROFILES_HOME = join(FAST_BROWSER_HOME, "profiles");
const FAST_BROWSER_DAEMON_DIR = join(FAST_BROWSER_HOME, "daemon");
const NODE_BINARY =
  process.env.CODEX_ROTATE_NODE_BIN?.trim() ||
  process.env.NODE_BIN?.trim() ||
  process.execPath ||
  "node";
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
const CODEX_ROTATE_AUTH_FLOW_ARTIFACT_MODE: "minimal" | "full" =
  process.env.CODEX_ROTATE_AUTH_FLOW_ARTIFACT_MODE === "full"
    ? "full"
    : "minimal";
const OPENAI_ACCOUNT_SECRET_URIS = [
  "https://auth.openai.com",
  "https://chatgpt.com",
];
const BITWARDEN_BINARY = process.env.CODEX_ROTATE_BW_BIN?.trim() || "bw";
const FAST_BROWSER_PLAYWRIGHT_MODULE = resolvePlaywrightModulePath();
const FAST_BROWSER_NODE_PATH = dirname(FAST_BROWSER_PLAYWRIGHT_MODULE);
const FAST_BROWSER_SECRET_BROKER_SOCKET = join(
  FAST_BROWSER_DAEMON_DIR,
  "secrets.sock",
);
const FAST_BROWSER_SECRET_BROKER_PID = join(
  FAST_BROWSER_DAEMON_DIR,
  "secrets.pid",
);
const FAST_BROWSER_SECRET_BROKEN_CWD_PATTERN =
  /(process\.cwd failed|uv_cwd|ENOENT:\s*process\.cwd|current working directory was likely removed)/i;

function getProcessCwdSafe(): string | null {
  try {
    return process.cwd();
  } catch {
    return null;
  }
}

function resolveStableWorkingDirectory(): string {
  const candidates = [
    process.env.CODEX_ROTATE_SAFE_CWD,
    REPO_ROOT,
    process.env.CODEX_ROTATE_HOME,
    DEFAULT_ROTATE_HOME,
    homedir(),
  ]
    .map((value) => (typeof value === "string" ? value.trim() : ""))
    .filter(Boolean)
    .map((value) => resolve(value));

  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }

  return homedir();
}

function ensureProcessWorkingDirectory(): void {
  const current = getProcessCwdSafe();
  if (current && existsSync(current)) {
    return;
  }
  const fallback = resolveStableWorkingDirectory();
  if (existsSync(fallback)) {
    process.chdir(fallback);
  }
}

ensureProcessWorkingDirectory();

export function shouldPromptForCodexRotateSecretUnlock(): boolean {
  return (
    process.env.CODEX_ROTATE_ALLOW_INTERACTIVE_SECRET_UNLOCK === "1" ||
    (process.stdin.isTTY && process.stderr.isTTY)
  );
}

function resolvePlaywrightModulePath(): string {
  const currentWorkingDirectory = getProcessCwdSafe();
  const override = process.env.CODEX_ROTATE_PLAYWRIGHT_MODULE?.trim();
  const directCandidates = [
    override ? resolve(override) : null,
    join(REPO_ROOT, "node_modules", "playwright"),
    currentWorkingDirectory
      ? join(currentWorkingDirectory, "node_modules", "playwright")
      : null,
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

export type CodexRotateSecretLocator =
  | CodexRotateLoginLookupSecretLocator
  | CodexRotateNamedSecretLocator;

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
  recent_events?: Array<Record<string, unknown>> | null;
  page?: Record<string, unknown> | null;
  current?: Record<string, unknown> | null;
  error?: {
    message?: string | null;
  } | null;
  pause?: FastBrowserPause | null;
  finalUrl?: string | null;
  observability?: {
    runId?: string | null;
    runPath?: string | null;
    statusPath?: string | null;
    eventsPath?: string | null;
    run_id?: string | null;
    run_path?: string | null;
    status_path?: string | null;
    events_path?: string | null;
  } | null;
}

interface FastBrowserDaemonRunResponse {
  ok: boolean;
  result?: FastBrowserRunResult;
  error?: {
    message?: string;
  };
}

const FAST_BROWSER_BRIDGE_INACTIVITY_TIMEOUT_MS = Number(
  process.env.CODEX_ROTATE_FAST_BROWSER_BRIDGE_INACTIVITY_TIMEOUT_MS || 60_000,
);
const FAST_BROWSER_EVENT_PREFIX = "__FAST_BROWSER_EVENT__";

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

interface CodexRotateLoginWorkflowAttemptResult {
  result?: FastBrowserRunResult | null;
  error_message?: string | null;
  managed_runtime_reset_performed?: boolean;
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

function isMissingOptionalSecretLocatorError(
  locator: CodexRotateSecretLocator,
  error: unknown,
): boolean {
  const message = error instanceof Error ? error.message : String(error || "");
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
  return await withBitwardenSecretBrokerRecovery(async () => {
    const {
      ensureDaemonSecretStoreReadyInteractive,
      resolveDaemonSecretLocator,
    } = await import(FAST_BROWSER_DAEMON_CLIENT_MODULE);
    await ensureDaemonSecretStoreReadyInteractive({
      profileName,
      store: locator.store ?? "bitwarden-cli",
      promptIfLocked: shouldPromptForCodexRotateSecretUnlock(),
    });
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
  });
}

function normalizeBitwardenCliAccountSecretIdentity(
  profileName: string,
  email: string,
): { profileName: string; email: string } {
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
  return {
    profileName: normalizedProfileName,
    email: normalizedEmail,
  };
}

async function withReadyBitwardenSecretBroker<T>(
  profileName: string,
  promptIfLocked: boolean,
  operation: (daemon: {
    ensureDaemonSecretStoreReadyInteractive: (options: {
      profileName: string;
      store: "bitwarden-cli";
      promptIfLocked: boolean;
    }) => Promise<unknown>;
    findDaemonLoginSecretRef: (options: {
      profileName: string;
      store: "bitwarden-cli";
      username: string;
      uris: string[];
    }) => Promise<{
      ok?: boolean;
      ref?: unknown;
      error?: { message?: string };
    }>;
    ensureDaemonLoginSecretRef: (options: {
      profileName: string;
      store: "bitwarden-cli";
      name: string;
      username: string;
      password: string;
      notes: string;
      uris: string[];
    }) => Promise<{
      ok?: boolean;
      ref?: unknown;
      error?: { message?: string };
    }>;
  }) => Promise<T>,
): Promise<T> {
  return await withBitwardenSecretBrokerRecovery(async () => {
    const daemon = await import(FAST_BROWSER_DAEMON_CLIENT_MODULE);
    await daemon.ensureDaemonSecretStoreReadyInteractive({
      profileName,
      store: "bitwarden-cli",
      promptIfLocked,
    });
    return await operation(daemon);
  });
}

export async function prepareBitwardenCliAccountSecretRef(
  profileName: string,
  email: string,
  password: string,
): Promise<CodexRotateSecretRef> {
  const normalized = normalizeBitwardenCliAccountSecretIdentity(
    profileName,
    email,
  );
  const normalizedPassword = String(password || "");
  if (!normalizedPassword) {
    throw new Error(
      `Bitwarden account secret for ${normalized.email} requires a non-empty password.`,
    );
  }

  return await withReadyBitwardenSecretBroker(
    normalized.profileName,
    shouldPromptForCodexRotateSecretUnlock(),
    async (daemon) => {
      const existing = await daemon.findDaemonLoginSecretRef({
        profileName: normalized.profileName,
        store: "bitwarden-cli",
        username: normalized.email,
        uris: OPENAI_ACCOUNT_SECRET_URIS,
      });
      if (!existing?.ok) {
        throw new Error(
          existing?.error?.message ||
            `Fast-browser Bitwarden adapter failed while looking up the vault item for ${normalized.email}.`,
        );
      }
      const existingRef = normalizeCodexRotateSecretRef(existing?.ref);
      if (existingRef) {
        return existingRef;
      }

      const created = await daemon.ensureDaemonLoginSecretRef({
        profileName: normalized.profileName,
        store: "bitwarden-cli",
        name: buildCodexRotateAccountSecretName(normalized.email),
        username: normalized.email,
        password: normalizedPassword,
        notes: `Managed by codex-rotate for ${normalized.email}.`,
        uris: OPENAI_ACCOUNT_SECRET_URIS,
      });
      if (!created?.ok) {
        throw new Error(
          created?.error?.message ||
            `Fast-browser Bitwarden adapter failed while creating or reusing the vault item for ${normalized.email}.`,
        );
      }
      const createdRef = normalizeCodexRotateSecretRef(created?.ref);
      if (!createdRef) {
        throw new Error(
          `Fast-browser Bitwarden adapter did not return a secret ref for ${normalized.email}.`,
        );
      }
      return createdRef;
    },
  );
}

async function findBitwardenCliAccountSecretRefWithOptions(
  profileName: string,
  email: string,
  promptIfLocked: boolean,
): Promise<CodexRotateSecretRef | null> {
  const normalized = normalizeBitwardenCliAccountSecretIdentity(
    profileName,
    email,
  );

  return await withReadyBitwardenSecretBroker(
    normalized.profileName,
    promptIfLocked,
    async (daemon) => {
      const response = await daemon.findDaemonLoginSecretRef({
        profileName: normalized.profileName,
        store: "bitwarden-cli",
        username: normalized.email,
        uris: OPENAI_ACCOUNT_SECRET_URIS,
      });
      if (!response?.ok) {
        throw new Error(
          response?.error?.message ||
            `Fast-browser Bitwarden adapter failed while looking up the vault item for ${normalized.email}.`,
        );
      }
      return normalizeCodexRotateSecretRef(response?.ref);
    },
  );
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
  const normalized = normalizeBitwardenCliAccountSecretIdentity(
    profileName,
    email,
  );

  return await withReadyBitwardenSecretBroker(
    normalized.profileName,
    false,
    async (daemon) => {
      const response = await daemon.findDaemonLoginSecretRef({
        profileName: normalized.profileName,
        store: "bitwarden-cli",
        username: normalized.email,
        uris: OPENAI_ACCOUNT_SECRET_URIS,
      });
      if (!response?.ok) {
        throw new Error(
          response?.error?.message ||
            `Fast-browser Bitwarden adapter failed while looking up the vault item for ${normalized.email}.`,
        );
      }
      const ref = normalizeCodexRotateSecretRef(response?.ref);
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
          cwd: resolveStableWorkingDirectory(),
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
            `Bitwarden CLI failed while deleting the vault item for ${normalized.email}.`,
        );
      }
      return true;
    },
  );
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

async function requestFastBrowserSecretBrokerShutdown(): Promise<boolean> {
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
      FAST_BROWSER_SECRET_BROKER_SOCKET,
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

async function resetFastBrowserSecretBroker(): Promise<void> {
  const hadSocket = existsSync(FAST_BROWSER_SECRET_BROKER_SOCKET);
  const hadPid = Boolean(readPidIfExists(FAST_BROWSER_SECRET_BROKER_PID));

  let shutdownAccepted = !hadSocket;
  if (hadSocket) {
    shutdownAccepted = await requestFastBrowserSecretBrokerShutdown();
  }

  if (
    !shutdownAccepted &&
    !requestDaemonProcessTermination(FAST_BROWSER_SECRET_BROKER_PID) &&
    hadPid
  ) {
    throw new Error(
      "fast-browser secret broker did not accept a normal shutdown request. Stop it cleanly and retry.",
    );
  }

  const exitedCleanly = await waitForManagedProfileShutdown(
    FAST_BROWSER_SECRET_BROKER_PID,
    20_000,
  );
  if (!exitedCleanly) {
    throw new Error(
      "fast-browser secret broker is still running after a normal shutdown request. Stop it cleanly and retry.",
    );
  }

  try {
    if (hadSocket && existsSync(FAST_BROWSER_SECRET_BROKER_SOCKET)) {
      unlinkSync(FAST_BROWSER_SECRET_BROKER_SOCKET);
    }
  } catch {}

  try {
    if (hadPid && existsSync(FAST_BROWSER_SECRET_BROKER_PID)) {
      unlinkSync(FAST_BROWSER_SECRET_BROKER_PID);
    }
  } catch {}
}

async function withBitwardenSecretBrokerRecovery<T>(
  operation: () => Promise<T>,
): Promise<T> {
  try {
    return await operation();
  } catch (error) {
    if (!shouldResetFastBrowserSecretBrokerForBrokenCwd(error)) {
      throw error;
    }
    await resetFastBrowserSecretBroker();
    return await operation();
  }
}

export async function resetManagedProfileRuntime(
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

function ensureFastBrowserPlaywright(): void {
  if (!existsSync(FAST_BROWSER_PLAYWRIGHT_MODULE)) {
    throw new Error(
      `Playwright is not installed in ${REPO_ROOT}. ` +
        "Install the repo dependencies before using create/relogin automation.",
    );
  }
}

function parseFastBrowserJson<T>(
  result: Pick<FastBrowserCommandResult, "status" | "stdout">,
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

  return parseJsonFromMixedStdout<T>(
    stdout,
    `${actionLabel} returned invalid JSON.`,
  );
}

function parseJsonFromMixedStdout<T>(
  stdout: string,
  fallbackMessage: string,
): T {
  try {
    return JSON.parse(stdout) as T;
  } catch {}

  const lines = stdout
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter(Boolean);
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    try {
      return JSON.parse(lines[index]) as T;
    } catch {}
  }

  for (const marker of ['{"ok":', '{"result":', '{"status":', '{"error":']) {
    const candidate = extractTrailingJsonObject(stdout, marker);
    if (!candidate) {
      continue;
    }
    try {
      return JSON.parse(candidate) as T;
    } catch {}
  }

  throw new Error(fallbackMessage);
}

function extractTrailingJsonObject(
  stdout: string,
  marker: string,
): string | null {
  const start = stdout.lastIndexOf(marker);
  if (start === -1) {
    return null;
  }
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let index = start; index < stdout.length; index += 1) {
    const char = stdout[index];
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (char === "\\") {
        escaped = true;
      } else if (char === '"') {
        inString = false;
      }
      continue;
    }
    if (char === '"') {
      inString = true;
      continue;
    }
    if (char === "{") {
      depth += 1;
      continue;
    }
    if (char !== "}") {
      continue;
    }
    depth -= 1;
    if (depth === 0) {
      return stdout.slice(start, index + 1);
    }
  }
  return null;
}

export function hydrateFastBrowserRunResultFromObservability(
  result: FastBrowserRunResult,
): FastBrowserRunResult {
  const observability =
    result.observability &&
    typeof result.observability === "object" &&
    !Array.isArray(result.observability)
      ? result.observability
      : null;
  const runPathCandidates = [
    observability?.runPath,
    observability?.statusPath,
    observability?.run_path,
    observability?.status_path,
  ]
    .map((value) => (typeof value === "string" ? value.trim() : ""))
    .filter(Boolean);

  for (const runPath of runPathCandidates) {
    try {
      if (!existsSync(runPath)) {
        continue;
      }
      const snapshot = parseJson<Record<string, unknown>>(
        readFileSync(runPath, "utf8"),
        `fast-browser run artifact ${runPath} returned invalid JSON.`,
      );
      if (
        !snapshot ||
        typeof snapshot !== "object" ||
        Array.isArray(snapshot)
      ) {
        continue;
      }
      const snapshotPage =
        snapshot.page &&
        typeof snapshot.page === "object" &&
        !Array.isArray(snapshot.page)
          ? (snapshot.page as Record<string, unknown>)
          : null;
      const snapshotCurrent =
        snapshot.current &&
        typeof snapshot.current === "object" &&
        !Array.isArray(snapshot.current)
          ? (snapshot.current as Record<string, unknown>)
          : null;
      const snapshotOutput =
        snapshot.output &&
        typeof snapshot.output === "object" &&
        !Array.isArray(snapshot.output)
          ? (snapshot.output as Record<string, unknown>)
          : null;
      const snapshotRecentEvents = Array.isArray(snapshot.recent_events)
        ? (snapshot.recent_events as Array<Record<string, unknown>>)
        : Array.isArray(snapshot.recentEvents)
          ? (snapshot.recentEvents as Array<Record<string, unknown>>)
          : Array.isArray(snapshot.events)
            ? (snapshot.events as Array<Record<string, unknown>>)
            : null;

      return {
        ...result,
        finalUrl:
          result.finalUrl ||
          (typeof snapshot.finalUrl === "string" ? snapshot.finalUrl : null) ||
          (typeof snapshot.final_url === "string"
            ? snapshot.final_url
            : null) ||
          (typeof snapshotPage?.url === "string" ? snapshotPage.url : null),
        output: result.output ?? snapshotOutput,
        recent_events: result.recent_events ?? snapshotRecentEvents,
        page: result.page ?? snapshotPage,
        current: result.current ?? snapshotCurrent,
      };
    } catch {}
  }

  return result;
}

function maybeLogFastBrowserRunResultDebug(
  workflowRef: string,
  result: FastBrowserRunResult,
): void {
  if (process.env.CODEX_ROTATE_DEBUG_AUTH_FLOW_RESULT !== "1") {
    return;
  }
  const pageUrl =
    result.page && typeof result.page.url === "string" ? result.page.url : null;
  const pageTitle =
    result.page && typeof result.page.title === "string"
      ? result.page.title
      : null;
  const currentUrl =
    result.current &&
    typeof result.current === "object" &&
    result.current &&
    "details" in result.current &&
    result.current.details &&
    typeof result.current.details === "object" &&
    "current_url" in result.current.details &&
    typeof result.current.details.current_url === "string"
      ? result.current.details.current_url
      : null;
  const callbackInspectAction =
    result.state?.steps?.["inspect_device_authorization_after_callback_code"]
      ?.action ?? null;
  console.error(
    `[codex-rotate] fast-browser result debug workflow=${workflowRef} finalUrl=${String(
      result.finalUrl || "",
    )} pageUrl=${String(pageUrl || "")} pageTitle=${JSON.stringify(
      pageTitle || "",
    )} currentUrl=${String(currentUrl || "")} runPath=${String(
      result.observability?.runPath || "",
    )} statusPath=${String(
      result.observability?.statusPath || "",
    )} hasState=${Boolean(
      result.state,
    )} hasOutput=${Boolean(result.output)} callbackInspectAction=${JSON.stringify(
      callbackInspectAction,
    )}`,
  );
}

export function buildFastBrowserWorkflowError(
  workflowRef: string,
  response: FastBrowserDaemonRunResponse | null | undefined,
): Error {
  const hydratedResult =
    response?.result && typeof response.result === "object"
      ? hydrateFastBrowserRunResultFromObservability(response.result)
      : null;
  const error = new Error(
    hydratedResult?.error?.message ||
      response?.error?.message ||
      `fast-browser workflow ${workflowRef} failed.`,
  );
  if (hydratedResult) {
    (
      error as Error & { fastBrowserResult?: FastBrowserRunResult }
    ).fastBrowserResult = hydratedResult;
  }
  return error;
}

export function isFastBrowserRunResultFailure(
  result: FastBrowserRunResult | null | undefined,
): boolean {
  if (!result || typeof result !== "object") {
    return false;
  }
  const normalizedStatus =
    typeof result.status === "string" ? result.status.trim().toLowerCase() : "";
  if (normalizedStatus === "failed" || normalizedStatus === "error") {
    return true;
  }
  return Boolean(
    result.error &&
    typeof result.error === "object" &&
    typeof result.error.message === "string" &&
    result.error.message.trim(),
  );
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
    responseMode?: "action_only" | "full";
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
      responseMode: ${JSON.stringify(options?.responseMode ?? "full")},
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
      let stdout = "";
      let stderr = "";
      let stderrBuffer = "";
      const socketPath = join(FAST_BROWSER_DAEMON_DIR, `${profileName}.sock`);
      let inactivityTimer: NodeJS.Timeout | null = null;
      const resetInactivityTimer = (): void => {
        if (
          settled ||
          !Number.isFinite(FAST_BROWSER_BRIDGE_INACTIVITY_TIMEOUT_MS) ||
          FAST_BROWSER_BRIDGE_INACTIVITY_TIMEOUT_MS <= 0
        ) {
          return;
        }
        if (inactivityTimer) {
          clearTimeout(inactivityTimer);
        }
        inactivityTimer = setTimeout(() => {
          if (settled) {
            return;
          }
          settled = true;
          child.kill("SIGKILL");
          reject(
            new Error(
              `Timed out waiting for fast-browser daemon response from ${socketPath}`,
            ),
          );
        }, FAST_BROWSER_BRIDGE_INACTIVITY_TIMEOUT_MS);
      };
      resetInactivityTimer();

      const flushStderrLine = (line: string): void => {
        if (line.startsWith(FAST_BROWSER_EVENT_PREFIX)) {
          process.stderr.write(`${line}\n`);
          return;
        }
        stderr += `${line}\n`;
        process.stderr.write(`${line}\n`);
      };

      child.stdout.setEncoding("utf8");
      child.stdout.on("data", (chunk: string) => {
        resetInactivityTimer();
        stdout += chunk;
      });

      child.stderr.setEncoding("utf8");
      child.stderr.on("data", (chunk: string) => {
        resetInactivityTimer();
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
        if (inactivityTimer) {
          clearTimeout(inactivityTimer);
        }
        reject(error);
      });
      child.once("close", (code, signal) => {
        if (settled) {
          return;
        }
        settled = true;
        if (inactivityTimer) {
          clearTimeout(inactivityTimer);
        }
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

  const result = await executeBridge();
  const response = parseFastBrowserJson<FastBrowserDaemonRunResponse>(
    { status: result.status, stdout: result.stdout },
    `fast-browser workflow ${workflowRef}`,
  );

  if (!response?.ok || !response.result) {
    throw buildFastBrowserWorkflowError(workflowRef, response);
  }

  if (isFastBrowserRunResultFailure(response.result)) {
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

  const hydratedResult = hydrateFastBrowserRunResultFromObservability(
    response.result,
  );
  maybeLogFastBrowserRunResultDebug(workflowRef, hydratedResult);
  return hydratedResult;
}

function shouldResetFastBrowserSecretBrokerForBrokenCwd(
  output: string | null | undefined,
): boolean {
  const text =
    output instanceof Error
      ? `${output.message}\n${output.stack || ""}`
      : String(output || "");
  return FAST_BROWSER_SECRET_BROKEN_CWD_PATTERN.test(text);
}

function requireWorkflowInputString(
  value: string | null | undefined,
  field: string,
): string {
  const normalized = typeof value === "string" ? value.trim() : "";
  if (!normalized) {
    throw new Error(`Automation bridge requires a non-empty ${field}.`);
  }
  return normalized;
}

function requireWorkflowInputInteger(
  value: number | null | undefined,
  field: string,
): number {
  if (!Number.isInteger(value)) {
    throw new Error(`Automation bridge requires an integer ${field}.`);
  }
  return Number(value);
}

async function runCodexBrowserLoginWorkflow(
  profileName: string,
  email: string,
  accountLoginLocator: CodexRotateSecretLocator | null,
  workflowRunStamp?: string,
  options?: {
    artifactMode?: "minimal" | "full";
    codexBin?: string;
    workflowRef?: string;
    codexSession?: CodexRotateAuthFlowSession | null;
    preferSignupRecovery?: boolean;
    fullName?: string;
    birthMonth?: number;
    birthDay?: number;
    birthYear?: number;
  },
): Promise<FastBrowserRunResult> {
  const codexBin = String(options?.codexBin || "codex").trim() || "codex";
  const workflowRef = requireWorkflowInputString(
    options?.workflowRef,
    "workflowRef",
  );
  const fullName = requireWorkflowInputString(options?.fullName, "fullName");
  const birthMonth = requireWorkflowInputInteger(
    options?.birthMonth,
    "birthMonth",
  );
  const birthDay = requireWorkflowInputInteger(options?.birthDay, "birthDay");
  const birthYear = requireWorkflowInputInteger(
    options?.birthYear,
    "birthYear",
  );
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
      full_name: fullName,
      prefer_signup_recovery:
        options?.preferSignupRecovery === true ? "true" : "false",
      birth_month: String(birthMonth),
      birth_day: String(birthDay),
      birth_year: String(birthYear),
    },
    profileName,
    {
      workflowRunStamp,
      retainTemporaryProfilesOnSuccess: Boolean(workflowRunStamp),
      artifactMode:
        options?.artifactMode ?? CODEX_ROTATE_AUTH_FLOW_ARTIFACT_MODE,
      responseMode: "action_only",
    },
  );
}

export async function completeCodexLoginViaWorkflowAttempt(
  profileName: string,
  email: string,
  accountLoginLocator: CodexRotateSecretLocator | null,
  options?: {
    codexBin?: string;
    workflowRef?: string;
    workflowRunStamp?: string;
    preferSignupRecovery?: boolean;
    fullName?: string;
    birthMonth?: number;
    birthDay?: number;
    birthYear?: number;
    skipLocatorPreflight?: boolean;
    codexSession?: CodexRotateAuthFlowSession | null;
  },
): Promise<CodexRotateLoginWorkflowAttemptResult> {
  const workflowAccountLoginLocator =
    options?.skipLocatorPreflight === true
      ? accountLoginLocator
      : await resolveOptionalCodexRotateSecretLocator(
          profileName,
          accountLoginLocator,
        );

  try {
    const loginResult = await runCodexBrowserLoginWorkflow(
      profileName,
      email,
      workflowAccountLoginLocator,
      options?.workflowRunStamp,
      {
        codexBin: options?.codexBin,
        workflowRef: options?.workflowRef,
        codexSession: options?.codexSession ?? null,
        preferSignupRecovery: options?.preferSignupRecovery === true,
        fullName: options?.fullName,
        birthMonth: options?.birthMonth,
        birthDay: options?.birthDay,
        birthYear: options?.birthYear,
      },
    );
    return {
      result: loginResult,
      error_message: null,
      managed_runtime_reset_performed: false,
    };
  } catch (error) {
    const failedResult = readFastBrowserResultFromError(error);
    const message = error instanceof Error ? error.message : String(error);
    return {
      result: failedResult,
      error_message: message,
      managed_runtime_reset_performed: false,
    };
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
