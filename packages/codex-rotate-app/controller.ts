import { buildLoginStartRequest, loadCodexAuth } from "./auth.ts";
import { readCodexSignals } from "./logs.ts";
import { resolveCodexRotateAppPaths } from "./paths.ts";
import { inspectQuota } from "./quota.ts";
import { runRotateCommand } from "./rotate.ts";
import type {
  CodexLogSignal,
  CodexDesktopMcpRequest,
  DeviceLoginPayload,
  QuotaAssessment,
  RotationDecision,
  RotationResult,
} from "./types.ts";

export const LOW_QUOTA_ROTATION_THRESHOLD_PERCENT = 10;

export function planRotation(
  assessment: QuotaAssessment | null,
  signals: ReadonlyArray<CodexLogSignal>,
  options?: { lowQuotaThresholdPercent?: number },
): Pick<RotationDecision, "shouldRotate" | "reason" | "rotationCommand" | "rotationArgs"> {
  const lowQuotaThresholdPercent = options?.lowQuotaThresholdPercent ?? LOW_QUOTA_ROTATION_THRESHOLD_PERCENT;

  if (!assessment) {
    return {
      shouldRotate: false,
      reason: signals.length > 0 ? "quota assessment unavailable" : null,
      rotationCommand: null,
      rotationArgs: [],
    };
  }

  if (!assessment.usable) {
    return {
      shouldRotate: true,
      reason: assessment.blocker,
      rotationCommand: "next",
      rotationArgs: [],
    };
  }

  if (
    typeof assessment.primaryQuotaLeftPercent === "number"
    && assessment.primaryQuotaLeftPercent <= lowQuotaThresholdPercent
  ) {
    return {
      shouldRotate: true,
      reason: `quota low: ${assessment.primaryQuotaLeftPercent}% left`,
      rotationCommand: "create",
      rotationArgs: ["--ignore-current"],
    };
  }

  return {
    shouldRotate: false,
    reason: null,
    rotationCommand: null,
    rotationArgs: [],
  };
}

export async function decideRotation(options?: { afterSignalId?: number | null }): Promise<RotationDecision> {
  const paths = resolveCodexRotateAppPaths();
  const signals = readCodexSignals(paths.codexLogsDbFile, { afterId: options?.afterSignalId ?? null });
  const lastSignalId = signals.at(-1)?.id ?? (options?.afterSignalId ?? null);
  const auth = loadCodexAuth(paths.codexAuthFile);
  try {
    const assessment = await inspectQuota(auth);
    const rotation = planRotation(assessment, signals);
    return {
      lastSignalId,
      signals,
      assessment,
      assessmentError: null,
      ...rotation,
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
      rotationCommand: null,
      rotationArgs: [],
    };
  }
}

export async function readQuotaAssessment() {
  const paths = resolveCodexRotateAppPaths();
  return await inspectQuota(loadCodexAuth(paths.codexAuthFile));
}

export function rotateNow(options?: {
  command?: "next" | "create";
  args?: string[];
}): RotationResult {
  const paths = resolveCodexRotateAppPaths();
  return runRotateCommand({
    authFilePath: paths.codexAuthFile,
    rotateEntrypoint: paths.codexRotateEntrypoint,
    runtime: paths.runtime,
    repoRoot: paths.repoRoot,
    command: options?.command,
    args: options?.args,
  });
}

export function buildCurrentLoginRequest(): CodexDesktopMcpRequest<DeviceLoginPayload> {
  const paths = resolveCodexRotateAppPaths();
  return buildLoginStartRequest(loadCodexAuth(paths.codexAuthFile));
}
