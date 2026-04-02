import { buildLoginStartRequest, loadCodexAuth } from "./auth.ts";
import { readCodexSignals } from "./logs.ts";
import { resolveCodexRotateAppPaths } from "./paths.ts";
import { inspectQuota } from "./quota.ts";
import { runRotateNext } from "./rotate.ts";
import type {
  CodexDesktopMcpRequest,
  DeviceLoginPayload,
  RotationDecision,
  RotationResult,
} from "./types.ts";

export async function decideRotation(options?: { afterSignalId?: number | null }): Promise<RotationDecision> {
  const paths = resolveCodexRotateAppPaths();
  const signals = readCodexSignals(paths.codexLogsDbFile, { afterId: options?.afterSignalId ?? null });
  const lastSignalId = signals.at(-1)?.id ?? (options?.afterSignalId ?? null);
  if (signals.length === 0) {
    return {
      lastSignalId,
      signals,
      assessment: null,
      assessmentError: null,
      shouldRotate: false,
      reason: null,
    };
  }

  const auth = loadCodexAuth(paths.codexAuthFile);
  try {
    const assessment = await inspectQuota(auth);
    return {
      lastSignalId,
      signals,
      assessment,
      assessmentError: null,
      shouldRotate: !assessment.usable,
      reason: assessment.usable ? null : assessment.blocker,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      lastSignalId,
      signals,
      assessment: null,
      assessmentError: message,
      shouldRotate: false,
      reason: `quota probe failed: ${message}`,
    };
  }
}

export function rotateNow(): RotationResult {
  const paths = resolveCodexRotateAppPaths();
  return runRotateNext({
    authFilePath: paths.codexAuthFile,
    rotateEntrypoint: paths.codexRotateEntrypoint,
    runtime: paths.runtime,
    repoRoot: paths.repoRoot,
  });
}

export function buildCurrentLoginRequest(): CodexDesktopMcpRequest<DeviceLoginPayload> {
  const paths = resolveCodexRotateAppPaths();
  return buildLoginStartRequest(loadCodexAuth(paths.codexAuthFile));
}
