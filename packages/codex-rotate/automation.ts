import { spawn, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const MODULE_DIR = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(
  process.env.CODEX_ROTATE_REPO_ROOT || resolve(MODULE_DIR, "..", ".."),
);
const DEFAULT_ROTATE_HOME = join(homedir(), ".codex-rotate");
let ROTATE_HOME = resolve(process.env.CODEX_ROTATE_HOME || DEFAULT_ROTATE_HOME);
const NODE_BINARY =
  process.env.CODEX_ROTATE_NODE_BIN?.trim() ||
  process.env.NODE_BIN?.trim() ||
  process.execPath ||
  "node";
const MAIN_WORKTREE_ROOT = discoverMainWorktreeRoot(REPO_ROOT);
const FAST_BROWSER_SCRIPT = resolveFastBrowserSkillPath([
  "scripts",
  "fast-browser.mjs",
]);
const CODEX_ROTATE_AUTH_FLOW_ARTIFACT_MODE: "minimal" | "full" =
  process.env.CODEX_ROTATE_AUTH_FLOW_ARTIFACT_MODE === "full"
    ? "full"
    : "minimal";
const OPENAI_ACCOUNT_SECRET_URIS = [
  "https://auth.openai.com",
  "https://chatgpt.com",
];
const OPENAI_ACCOUNT_SECRET_FIELD_PATH = "/password";
const DEBUG_BRIDGE_ROOT = process.env.CODEX_ROTATE_DEBUG_BRIDGE_ROOT === "1";

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

function discoverMainWorktreeRoot(repoRoot: string): string | null {
  const result = spawnSync("git", ["worktree", "list", "--porcelain"], {
    cwd: repoRoot,
    encoding: "utf8",
  });
  if (result.status !== 0) {
    return null;
  }

  for (const line of result.stdout.split(/\r?\n/)) {
    if (!line.startsWith("worktree ")) {
      continue;
    }
    const candidate = line.slice("worktree ".length).trim();
    if (candidate && resolve(candidate) !== resolve(repoRoot)) {
      return resolve(candidate);
    }
  }

  return null;
}

function fastBrowserSkillPathCandidates(
  repoRoot: string,
  relativeParts: string[],
  mainWorktreeRoot: string | null = MAIN_WORKTREE_ROOT,
): string[] {
  const roots = [resolve(repoRoot)];
  if (mainWorktreeRoot) {
    const normalizedMainRoot = resolve(mainWorktreeRoot);
    if (!roots.includes(normalizedMainRoot)) {
      roots.push(normalizedMainRoot);
    }
  }

  return roots.map((root) =>
    join(dirname(root), "ai-rules", "skills", "fast-browser", ...relativeParts),
  );
}

export function resolveFastBrowserSkillPath(
  relativeParts: string[],
  repoRoot: string = REPO_ROOT,
  mainWorktreeRoot: string | null = MAIN_WORKTREE_ROOT,
): string {
  const candidates = fastBrowserSkillPathCandidates(
    repoRoot,
    relativeParts,
    mainWorktreeRoot,
  );
  return candidates.find((candidate) => existsSync(candidate)) || candidates[0];
}

export function resolveCodexRotateRepoRoot(): string {
  return REPO_ROOT;
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

if (DEBUG_BRIDGE_ROOT) {
  process.stderr.write(
    `[codex-rotate-ts] repoRoot=${REPO_ROOT} cwd=${process.cwd()} fastBrowserScript=${FAST_BROWSER_SCRIPT}\n`,
  );
}

export function shouldPromptForCodexRotateSecretUnlock(): boolean {
  return (
    process.env.CODEX_ROTATE_ALLOW_INTERACTIVE_SECRET_UNLOCK === "1" ||
    (process.stdin.isTTY && process.stderr.isTTY)
  );
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
  relayUrl?: string | null;
  reason?: string | null;
  sessionId?: string | null;
  nonce?: string | null;
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

export interface GmailVerificationArtifactForCleanup {
  gmailMessageId: string;
  gmailThreadId: string | null;
  gmailMessageUrl: string | null;
  messageSubject: string | null;
  messagePreview: string | null;
  selectedEmail: string | null;
}

interface FastBrowserDaemonRunResponse {
  ok: boolean;
  result?: FastBrowserRunResult;
  error?: {
    message?: string;
  };
}

interface FastBrowserCliResponse<TResult> {
  abiVersion?: string;
  command?: string;
  ok: boolean;
  result?: TResult;
  error?: {
    code?: string;
    message?: string;
    details?: unknown;
  };
}

function normalizeOptionalString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function readVerificationArtifactRecord(
  raw: unknown,
): GmailVerificationArtifactForCleanup | null {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    return null;
  }
  const record = raw as Record<string, unknown>;
  const gmailMessageId =
    normalizeOptionalString(record.gmail_message_id) ||
    normalizeOptionalString(record.gmailMessageId);
  if (!gmailMessageId) {
    return null;
  }
  return {
    gmailMessageId,
    gmailThreadId:
      normalizeOptionalString(record.gmail_thread_id) ||
      normalizeOptionalString(record.gmailThreadId),
    gmailMessageUrl:
      normalizeOptionalString(record.gmail_message_url) ||
      normalizeOptionalString(record.gmailMessageUrl),
    messageSubject:
      normalizeOptionalString(record.message_subject) ||
      normalizeOptionalString(record.messageSubject),
    messagePreview:
      normalizeOptionalString(record.message_preview) ||
      normalizeOptionalString(record.messagePreview),
    selectedEmail:
      normalizeOptionalString(record.selected_email) ||
      normalizeOptionalString(record.selectedEmail),
  };
}

function readVerificationArtifactFromAction(
  action: unknown,
): GmailVerificationArtifactForCleanup | null {
  if (!action || typeof action !== "object" || Array.isArray(action)) {
    return null;
  }
  const record = action as Record<string, unknown>;
  return (
    readVerificationArtifactRecord(record?.result) ||
    readVerificationArtifactRecord(
      (record?.result as Record<string, unknown> | undefined)?.result,
    ) ||
    readVerificationArtifactRecord(
      (
        (record?.result as Record<string, unknown> | undefined)?.result as
          | Record<string, unknown>
          | undefined
      )?.output,
    ) ||
    readVerificationArtifactRecord(
      (record?.result as Record<string, unknown> | undefined)?.output,
    ) ||
    readVerificationArtifactRecord(record?.output) ||
    readVerificationArtifactRecord(record)
  );
}

export function collectVerificationArtifactsForCleanup(
  result: FastBrowserRunResult | null | undefined,
): GmailVerificationArtifactForCleanup[] {
  const steps = result?.state?.steps;
  if (!steps || typeof steps !== "object") {
    return [];
  }
  const artifacts: GmailVerificationArtifactForCleanup[] = [];
  const seenMessageIds = new Set<string>();

  for (const step of Object.values(steps)) {
    const artifact = readVerificationArtifactFromAction(step?.action);
    if (!artifact || seenMessageIds.has(artifact.gmailMessageId)) {
      continue;
    }
    seenMessageIds.add(artifact.gmailMessageId);
    artifacts.push(artifact);
  }

  return artifacts;
}

export function extractFastBrowserCliResult<TResult>(
  response: FastBrowserCliResponse<TResult> | null | undefined,
): TResult | undefined {
  return response?.result;
}

export interface FastBrowserProfileDaemonStatus {
  ok?: boolean;
  lifecycle_state?: string | null;
  mode?: string | null;
  request_queue?: {
    active?: {
      id?: number | null;
      method?: string | null;
      workflow_ref?: string | null;
      started_at?: number | null;
      running_for_ms?: number | null;
      disconnected_at?: number | null;
      disconnect_reason?: string | null;
    } | null;
    queued?: Array<Record<string, unknown>> | null;
    queued_count?: number | null;
  } | null;
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
}

function parseJson<T>(raw: string, fallbackMessage: string): T {
  try {
    return JSON.parse(raw) as T;
  } catch {
    throw new Error(fallbackMessage);
  }
}

export function isSuppressedFastBrowserEventLine(line: string): boolean {
  const event = parseFastBrowserEventLine(line);
  return event?.phase === "daemon" && event.status === "heartbeat";
}

function parseFastBrowserEventLine(
  line: string,
): { phase?: string; status?: string } | null {
  if (!line.startsWith(FAST_BROWSER_EVENT_PREFIX)) {
    return null;
  }
  const raw = line.slice(FAST_BROWSER_EVENT_PREFIX.length).trim();
  if (!raw) {
    return null;
  }
  try {
    return JSON.parse(raw) as {
      phase?: unknown;
      status?: unknown;
    };
  } catch {
    return null;
  }
}

export function shouldResetFastBrowserBridgeInactivityTimer(
  line: string,
): boolean {
  const event = parseFastBrowserEventLine(line);
  if (!event) {
    return true;
  }
  return !(event.phase === "daemon" && event.status === "queued");
}

function ensureRotateDir(): void {
  if (!existsSync(ROTATE_HOME)) {
    mkdirSync(ROTATE_HOME, { recursive: true });
  }
}

async function runFastBrowserCliCommand(
  args: string[],
  options?: {
    stdinText?: string;
    actionLabel?: string;
  },
): Promise<FastBrowserCommandResult> {
  return await new Promise((resolve, reject) => {
    if (DEBUG_BRIDGE_ROOT) {
      process.stderr.write(
        `[codex-rotate-ts] fast-browser spawn cwd=${REPO_ROOT} workspaceEnv=${REPO_ROOT} args=${JSON.stringify(args)}\n`,
      );
    }
    const child = spawn(NODE_BINARY, [FAST_BROWSER_SCRIPT, ...args], {
      cwd: REPO_ROOT,
      env: {
        ...process.env,
        FAST_BROWSER_WORKSPACE_ROOT: REPO_ROOT,
      },
      stdio: ["pipe", "pipe", "pipe"],
    });
    let settled = false;
    let stdout = "";
    let stderr = "";
    let stderrBuffer = "";
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
            `Timed out waiting for fast-browser CLI response while ${options?.actionLabel || args.join(" ")}`,
          ),
        );
      }, FAST_BROWSER_BRIDGE_INACTIVITY_TIMEOUT_MS);
    };
    resetInactivityTimer();

    const flushStderrLine = (line: string): void => {
      if (line.startsWith(FAST_BROWSER_EVENT_PREFIX)) {
        if (!isSuppressedFastBrowserEventLine(line)) {
          process.stderr.write(`${line}\n`);
        }
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
      stderrBuffer += chunk;
      while (true) {
        const newlineIndex = stderrBuffer.indexOf("\n");
        if (newlineIndex === -1) {
          break;
        }
        const line = stderrBuffer.slice(0, newlineIndex);
        stderrBuffer = stderrBuffer.slice(newlineIndex + 1);
        if (line.trim()) {
          if (shouldResetFastBrowserBridgeInactivityTimer(line)) {
            resetInactivityTimer();
          }
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

    if (typeof options?.stdinText === "string") {
      child.stdin.end(options.stdinText);
    } else {
      child.stdin.end();
    }
  });
}

async function runFastBrowserCliJsonRequest<TResult>(
  args: string[],
  request: Record<string, unknown>,
  actionLabel: string,
): Promise<FastBrowserCliResponse<TResult>> {
  const result = await runFastBrowserCliCommand([...args, "--json"], {
    stdinText: JSON.stringify(request),
    actionLabel,
  });
  return parseFastBrowserJson<FastBrowserCliResponse<TResult>>(
    { status: result.status, stdout: result.stdout },
    actionLabel,
  );
}

async function ensureFastBrowserSecretSession(
  profileName: string,
  store: "bitwarden-cli",
  promptIfLocked: boolean,
): Promise<void> {
  void profileName;
  const response = await runFastBrowserCliJsonRequest<Record<string, unknown>>(
    ["secrets", "session"],
    {
      action: "ensure",
      store,
      promptIfLocked,
    },
    "fast-browser secrets session ensure",
  );
  if (!response?.ok) {
    throw new Error(
      response?.error?.message ||
        "fast-browser failed to ensure the requested secret session.",
    );
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

export function isUnavailableOptionalSecretLocatorError(
  error: unknown,
): boolean {
  const message = error instanceof Error ? error.message : String(error || "");
  return (
    /Bitwarden CLI is locked/i.test(message) ||
    /Bitwarden CLI is not logged in/i.test(message) ||
    /Bitwarden CLI is not ready/i.test(message) ||
    /timed out while trying to read Bitwarden CLI status/i.test(message) ||
    /failed to read secret-store status/i.test(message)
  );
}

async function resolveOptionalCodexRotateSecretLocator(
  profileName: string,
  locator: CodexRotateSecretLocator | null | undefined,
): Promise<CodexRotateSecretLocator | null> {
  if (!locator) {
    return null;
  }
  try {
    await ensureFastBrowserSecretSession(
      profileName,
      locator.store ?? "bitwarden-cli",
      shouldPromptForCodexRotateSecretUnlock(),
    );
    const response = await runFastBrowserCliJsonRequest<
      Record<string, unknown>
    >(
      ["secrets", "item"],
      {
        action: "resolve",
        selector: {
          kind: "locator",
          locator: toFastBrowserCliSecretLocator(locator),
        },
      },
      "fast-browser secrets item resolve",
    );
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
    if (isUnavailableOptionalSecretLocatorError(error)) {
      return null;
    }
    throw error;
  }
}

async function resolveOptionalCodexRotateSecretRef(
  profileName: string,
  locator: CodexRotateSecretLocator | null | undefined,
): Promise<CodexRotateSecretRef | null> {
  if (!locator) {
    return null;
  }
  try {
    await ensureFastBrowserSecretSession(
      profileName,
      locator.store ?? "bitwarden-cli",
      shouldPromptForCodexRotateSecretUnlock(),
    );
    const selector = buildFastBrowserSecretRefResolveSelector(locator);
    const response = await runFastBrowserCliJsonRequest<
      Record<string, unknown>
    >(
      ["secrets", "item"],
      {
        action: "resolve",
        selector,
      },
      "fast-browser secrets item resolve",
    );
    if (!response?.ok) {
      throw new Error(
        response?.error?.message ||
          "fast-browser failed to resolve the requested secret ref.",
      );
    }
    return applyLocatorFieldPathToSecretRef(
      normalizeCodexRotateSecretRef(extractFastBrowserCliResult(response)?.ref),
      locator,
    );
  } catch (error) {
    if (isMissingOptionalSecretLocatorError(locator, error)) {
      return null;
    }
    if (isUnavailableOptionalSecretLocatorError(error)) {
      return null;
    }
    throw error;
  }
}

export function buildFastBrowserSecretRefResolveSelector(
  locator: CodexRotateSecretLocator,
): Record<string, unknown> {
  if (locator.kind === "login_lookup") {
    return {
      kind: "login",
      ...(locator.store ? { store: locator.store } : {}),
      username: locator.username,
      uris: locator.uris,
    };
  }
  return {
    kind: "locator",
    locator: toFastBrowserCliSecretLocator(locator),
  };
}

export function applyLocatorFieldPathToSecretRef(
  ref: CodexRotateSecretRef | null,
  locator: CodexRotateSecretLocator,
): CodexRotateSecretRef | null {
  if (!ref) {
    return null;
  }
  if (ref.field_path) {
    return ref;
  }
  const fieldPath = locator.field_path ?? null;
  if (!fieldPath) {
    return ref;
  }
  return {
    ...ref,
    field_path: fieldPath,
  };
}

export function applyAccountPasswordFieldPath(
  ref: CodexRotateSecretRef | null,
): CodexRotateSecretRef | null {
  if (!ref || ref.field_path) {
    return ref;
  }
  return {
    ...ref,
    field_path: OPENAI_ACCOUNT_SECRET_FIELD_PATH,
  };
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
  operation: () => Promise<T>,
): Promise<T> {
  await ensureFastBrowserSecretSession(
    profileName,
    "bitwarden-cli",
    promptIfLocked,
  );
  return await operation();
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
    async () => {
      const existing = await runFastBrowserCliJsonRequest<{
        ref?: Record<string, unknown> | null;
      }>(
        ["secrets", "item"],
        {
          action: "resolve",
          selector: {
            kind: "login",
            store: "bitwarden-cli",
            username: normalized.email,
            uris: OPENAI_ACCOUNT_SECRET_URIS,
          },
        },
        `fast-browser secrets item resolve for ${normalized.email}`,
      );
      if (!existing?.ok) {
        throw new Error(
          existing?.error?.message ||
            `Fast-browser Bitwarden adapter failed while looking up the vault item for ${normalized.email}.`,
        );
      }
      const existingRef = normalizeCodexRotateSecretRef(
        extractFastBrowserCliResult(existing)?.ref,
      );
      if (existingRef) {
        return applyAccountPasswordFieldPath(existingRef);
      }

      const created = await runFastBrowserCliJsonRequest<{
        ref?: Record<string, unknown> | null;
      }>(
        ["secrets", "item"],
        {
          action: "ensure",
          kind: "login",
          store: "bitwarden-cli",
          name: buildCodexRotateAccountSecretName(normalized.email),
          username: normalized.email,
          password: normalizedPassword,
          notes: `Managed by codex-rotate for ${normalized.email}.`,
          uris: OPENAI_ACCOUNT_SECRET_URIS,
        },
        `fast-browser secrets item ensure for ${normalized.email}`,
      );
      if (!created?.ok) {
        throw new Error(
          created?.error?.message ||
            `Fast-browser Bitwarden adapter failed while creating or reusing the vault item for ${normalized.email}.`,
        );
      }
      const createdRef = normalizeCodexRotateSecretRef(
        extractFastBrowserCliResult(created)?.ref,
      );
      if (!createdRef) {
        throw new Error(
          `Fast-browser Bitwarden adapter did not return a secret ref for ${normalized.email}.`,
        );
      }
      return applyAccountPasswordFieldPath(createdRef);
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
    async () => {
      const response = await runFastBrowserCliJsonRequest<{
        ref?: Record<string, unknown> | null;
      }>(
        ["secrets", "item"],
        {
          action: "resolve",
          selector: {
            kind: "login",
            store: "bitwarden-cli",
            username: normalized.email,
            uris: OPENAI_ACCOUNT_SECRET_URIS,
          },
        },
        `fast-browser secrets item resolve for ${normalized.email}`,
      );
      if (!response?.ok) {
        throw new Error(
          response?.error?.message ||
            `Fast-browser Bitwarden adapter failed while looking up the vault item for ${normalized.email}.`,
        );
      }
      return applyAccountPasswordFieldPath(
        normalizeCodexRotateSecretRef(
          extractFastBrowserCliResult(response)?.ref,
        ),
      );
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
    async () => {
      const response = await runFastBrowserCliJsonRequest<{
        ref?: Record<string, unknown> | null;
      }>(
        ["secrets", "item"],
        {
          action: "resolve",
          selector: {
            kind: "login",
            store: "bitwarden-cli",
            username: normalized.email,
            uris: OPENAI_ACCOUNT_SECRET_URIS,
          },
        },
        `fast-browser secrets item resolve for ${normalized.email}`,
      );
      if (!response?.ok) {
        throw new Error(
          response?.error?.message ||
            `Fast-browser Bitwarden adapter failed while looking up the vault item for ${normalized.email}.`,
        );
      }
      const ref = normalizeCodexRotateSecretRef(
        extractFastBrowserCliResult(response)?.ref,
      );
      if (!ref) {
        return false;
      }

      const deleted = await runFastBrowserCliJsonRequest<
        Record<string, unknown>
      >(
        ["secrets", "item"],
        {
          action: "delete",
          store: "bitwarden-cli",
          objectId: ref.object_id,
        },
        `fast-browser secrets item delete for ${normalized.email}`,
      );
      if (!deleted?.ok) {
        throw new Error(
          deleted?.error?.message ||
            `Fast-browser Bitwarden adapter failed while deleting the vault item for ${normalized.email}.`,
        );
      }
      return true;
    },
  );
}

function parseFastBrowserJson<T>(
  result: Pick<FastBrowserCommandResult, "status" | "stdout">,
  actionLabel: string,
): T {
  const stdout = result.stdout?.trim();
  if (!stdout) {
    if (typeof result.status === "number" && result.status !== 0) {
      throw new Error(`${actionLabel} exited with status ${result.status}.`);
    }
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
  const response = await runFastBrowserCliJsonRequest<FastBrowserRunResult>(
    ["workflows", "run"],
    {
      workflowRef,
      inputs,
      profileName,
      headed: Boolean(options?.headed),
      workflowRunStamp: options?.workflowRunStamp ?? null,
      retainTemporaryProfilesOnSuccess: Boolean(
        options?.retainTemporaryProfilesOnSuccess,
      ),
      artifactMode: options?.artifactMode ?? "minimal",
      debugMode: options?.debugMode ?? "off",
      responseMode:
        options?.responseMode === "action_only" ? "action-only" : "full",
    },
    `fast-browser workflow ${workflowRef}`,
  );

  if (!response?.ok || !response.result) {
    throw buildFastBrowserWorkflowError(
      workflowRef,
      response as FastBrowserDaemonRunResponse,
    );
  }

  if (isFastBrowserRunResultFailure(response.result)) {
    throw buildFastBrowserWorkflowError(
      workflowRef,
      response as FastBrowserDaemonRunResponse,
    );
  }

  if (response.result.status === "paused") {
    const reason = response.result.pause?.reason ?? "pause";
    const relayUrl =
      response.result.pause?.relayUrl || response.result.pause?.relay_url;
    const relay = relayUrl ? ` Open ${relayUrl} to continue the workflow.` : "";
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

async function deleteVerificationArtifactsAfterSuccessfulLogin(
  profileName: string,
  email: string,
  result: FastBrowserRunResult,
): Promise<void> {
  const artifacts = collectVerificationArtifactsForCleanup(result);
  if (artifacts.length === 0) {
    return;
  }

  for (const artifact of artifacts) {
    try {
      await runFastBrowserDaemonWorkflow(
        "workflow.sys.web.mail-google-com.delete-verification-artifact",
        {
          enabled: true,
          preferred_email: artifact.selectedEmail || undefined,
          search_query: `from:openai to:${String(email || "").trim()} newer_than:7d`,
          message_match_text: String(email || "").trim() || "OpenAI",
          gmail_message_id: artifact.gmailMessageId,
          gmail_thread_id: artifact.gmailThreadId || undefined,
          gmail_message_url: artifact.gmailMessageUrl || undefined,
          message_subject: artifact.messageSubject || undefined,
          message_preview: artifact.messagePreview || undefined,
        },
        profileName,
        {
          artifactMode: "minimal",
          responseMode: "action_only",
        },
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      process.stderr.write(
        `[codex-rotate] post-success Gmail verification artifact cleanup failed for ${email}: ${message}\n`,
      );
    }
  }
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
  accountLoginRef: CodexRotateSecretRef | null,
  workflowRunStamp?: string,
  options?: {
    artifactMode?: "minimal" | "full";
    codexBin?: string;
    workflowRef?: string;
    codexSession?: CodexRotateAuthFlowSession | null;
    preferSignupRecovery?: boolean;
    preferPasswordLogin?: boolean;
    password?: string;
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
      ...(accountLoginRef ? { account_login_ref: accountLoginRef } : {}),
      ...(accountLoginLocator
        ? { account_login_locator: accountLoginLocator }
        : {}),
      full_name: fullName,
      prefer_signup_recovery:
        options?.preferSignupRecovery === true ? "true" : "false",
      ...(options?.preferPasswordLogin !== undefined
        ? {
            prefer_password_login:
              options.preferPasswordLogin === true ? "true" : "false",
          }
        : {}),
      ...(options?.password
        ? {
            password: String(options.password),
          }
        : {}),
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
    preferPasswordLogin?: boolean;
    password?: string;
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
  const workflowAccountLoginRef = await resolveOptionalCodexRotateSecretRef(
    profileName,
    workflowAccountLoginLocator,
  );

  try {
    const loginResult = await runCodexBrowserLoginWorkflow(
      profileName,
      email,
      workflowAccountLoginLocator,
      workflowAccountLoginRef,
      options?.workflowRunStamp,
      {
        codexBin: options?.codexBin,
        workflowRef: options?.workflowRef,
        codexSession: options?.codexSession ?? null,
        preferSignupRecovery: options?.preferSignupRecovery === true,
        preferPasswordLogin: options?.preferPasswordLogin,
        password: options?.password,
        fullName: options?.fullName,
        birthMonth: options?.birthMonth,
        birthDay: options?.birthDay,
        birthYear: options?.birthYear,
      },
    );
    await deleteVerificationArtifactsAfterSuccessfulLogin(
      profileName,
      email,
      loginResult,
    );
    return {
      result: loginResult,
      error_message: null,
    };
  } catch (error) {
    const failedResult = readFastBrowserResultFromError(error);
    const message = error instanceof Error ? error.message : String(error);
    return {
      result: failedResult,
      error_message: message,
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
    typeof record.object_id === "string"
      ? record.object_id.trim()
      : typeof record.objectId === "string"
        ? record.objectId.trim()
        : "";
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
  if (type !== "secret_ref" && type !== "secret-ref") {
    return null;
  }
  return {
    type: "secret_ref",
    store: "bitwarden-cli",
    object_id: objectId,
    field_path:
      typeof record.field_path === "string"
        ? record.field_path
        : typeof record.fieldPath === "string"
          ? record.fieldPath
          : null,
    version: typeof record.version === "string" ? record.version : null,
  };
}

function toFastBrowserCliSecretLocator(
  locator: CodexRotateSecretLocator,
): Record<string, unknown> {
  if (locator.kind === "login_lookup") {
    return {
      kind: "login-lookup",
      ...(locator.store ? { store: locator.store } : {}),
      username: locator.username,
      uris: locator.uris,
      ...(locator.field_path ? { fieldPath: locator.field_path } : {}),
    };
  }
  return {
    kind: "named-secret",
    ...(locator.store ? { store: locator.store } : {}),
    name: locator.name,
    ...(locator.field_path ? { fieldPath: locator.field_path } : {}),
  };
}

function buildCodexRotateAccountSecretName(email: string): string {
  return `codex-rotate/openai/${String(email || "")
    .trim()
    .toLowerCase()}`;
}
