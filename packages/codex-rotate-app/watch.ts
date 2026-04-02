import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { decideRotation, rotateNow } from "./controller.ts";
import { sanitizeDeviceLoginPayload } from "./auth.ts";
import { readLiveAccount, switchLiveAccountToCurrentAuth } from "./hook.ts";
import { ensureDebugCodexInstance } from "./launcher.ts";
import { readLatestCodexSignalId } from "./logs.ts";
import { resolveCodexRotateAppPaths } from "./paths.ts";
import type { RotationDecision, RotationResult } from "./types.ts";

export interface WatchState {
  lastSignalId: number | null;
  lastCheckedAt: string | null;
  lastLiveEmail: string | null;
  lastRotationAt: string | null;
  lastRotationReason: string | null;
  lastRotatedEmail: string | null;
}

export interface WatchIterationResult {
  state: WatchState;
  decision: RotationDecision;
  rotated: boolean;
  rotation: RotationResult | null;
  live: { email: string; planType: string; accountId: string } | null;
}

export interface WatchLiveOptions {
  port?: number;
  intervalMs?: number;
  afterSignalId?: number | null;
  cooldownMs?: number;
  once?: boolean;
  onEvent?: (message: string) => void;
}

function sanitizeWatchIterationResult(result: WatchIterationResult): WatchIterationResult {
  if (!result.rotation) {
    return result;
  }
  return {
    ...result,
    rotation: {
      ...result.rotation,
      loginPayload: sanitizeDeviceLoginPayload(result.rotation.loginPayload),
    },
  };
}

const DEFAULT_INTERVAL_MS = 5_000;
const DEFAULT_COOLDOWN_MS = 15_000;

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function watchStatePath(): string {
  const paths = resolveCodexRotateAppPaths();
  return `${paths.rotateAppHome}/watch-state.json`;
}

function ensureRotateAppHome(): void {
  const paths = resolveCodexRotateAppPaths();
  if (!existsSync(paths.rotateAppHome)) {
    mkdirSync(paths.rotateAppHome, { recursive: true });
  }
}

function defaultWatchState(): WatchState {
  return {
    lastSignalId: null,
    lastCheckedAt: null,
    lastLiveEmail: null,
    lastRotationAt: null,
    lastRotationReason: null,
    lastRotatedEmail: null,
  };
}

export function readWatchState(): WatchState {
  const path = watchStatePath();
  if (!existsSync(path)) {
    return defaultWatchState();
  }
  try {
    return {
      ...defaultWatchState(),
      ...(JSON.parse(readFileSync(path, "utf8")) as Partial<WatchState>),
    };
  } catch {
    return defaultWatchState();
  }
}

export function writeWatchState(state: WatchState): void {
  ensureRotateAppHome();
  writeFileSync(watchStatePath(), JSON.stringify(state, null, 2), "utf8");
}

function shouldRotate(decision: RotationDecision): { rotate: boolean; reason: string | null } {
  if (decision.shouldRotate) {
    return { rotate: true, reason: decision.reason };
  }
  return { rotate: false, reason: null };
}

function cooldownActive(state: WatchState, cooldownMs: number): boolean {
  if (!state.lastRotationAt) {
    return false;
  }
  const lastRotationTime = Date.parse(state.lastRotationAt);
  if (Number.isNaN(lastRotationTime)) {
    return false;
  }
  return Date.now() - lastRotationTime < cooldownMs;
}

export async function runWatchIteration(options?: {
  port?: number;
  afterSignalId?: number | null;
  cooldownMs?: number;
  onEvent?: (message: string) => void;
}): Promise<WatchIterationResult> {
  const port = options?.port ?? 9333;
  const cooldownMs = options?.cooldownMs ?? DEFAULT_COOLDOWN_MS;
  await ensureDebugCodexInstance({ port });

  const previousState = readWatchState();
  let afterSignalId = options?.afterSignalId ?? previousState.lastSignalId;
  if (afterSignalId === null) {
    afterSignalId = readLatestCodexSignalId(resolveCodexRotateAppPaths().codexLogsDbFile);
  }

  const liveAccount = await readLiveAccount({ port });
  const decision = await decideRotation({ afterSignalId });
  const rotationCheck = shouldRotate(decision);

  let rotated = false;
  let rotation: RotationResult | null = null;
  let live: { email: string; planType: string; accountId: string } | null = null;

  if (rotationCheck.rotate && !cooldownActive(previousState, cooldownMs)) {
    options?.onEvent?.(`rotation triggered: ${rotationCheck.reason ?? "unknown"}`);
    rotation = rotateNow();
    live = await switchLiveAccountToCurrentAuth({ port, ensureLaunched: false });
    rotated = true;
  } else if (rotationCheck.rotate) {
    options?.onEvent?.("rotation suppressed by cooldown");
  }

  const nextState: WatchState = {
    lastSignalId: decision.lastSignalId,
    lastCheckedAt: new Date().toISOString(),
    lastLiveEmail: live?.email ?? liveAccount.account?.email ?? previousState.lastLiveEmail,
    lastRotationAt: rotated ? new Date().toISOString() : previousState.lastRotationAt,
    lastRotationReason: rotated ? rotationCheck.reason : previousState.lastRotationReason,
    lastRotatedEmail: rotated ? rotation?.summary.email ?? null : previousState.lastRotatedEmail,
  };
  writeWatchState(nextState);

  return {
    state: nextState,
    decision,
    rotated,
    rotation,
    live,
  };
}

export async function watchLive(options?: WatchLiveOptions): Promise<void> {
  const intervalMs = Math.max(1_000, options?.intervalMs ?? DEFAULT_INTERVAL_MS);
  const log = options?.onEvent ?? ((message: string) => console.log(message));

  do {
    const result = await runWatchIteration({
      port: options?.port,
      afterSignalId: options?.afterSignalId,
      cooldownMs: options?.cooldownMs,
      onEvent: log,
    });
    log(JSON.stringify(sanitizeWatchIterationResult(result), null, 2));
    if (options?.once) {
      return;
    }
    await sleep(intervalMs);
  } while (true);
}
