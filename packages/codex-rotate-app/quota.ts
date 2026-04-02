import type {
  CodexAuth,
  QuotaAssessment,
  UsageCredits,
  UsageResponse,
  UsageWindow,
} from "./types.ts";
import { summarizeCodexAuth } from "./auth.ts";

const WHAM_USAGE_URL = "https://chatgpt.com/backend-api/wham/usage";
const REQUEST_TIMEOUT_MS = 8000;

function formatPercent(value: number): string {
  return `${Math.max(0, Math.min(100, Math.round(value)))}%`;
}

function getQuotaLeft(window: UsageWindow | null | undefined): number | null {
  if (!window || typeof window.used_percent !== "number") {
    return null;
  }
  return Math.max(0, Math.min(100, 100 - window.used_percent));
}

function formatDuration(totalSeconds: number | null | undefined): string {
  if (typeof totalSeconds !== "number" || !Number.isFinite(totalSeconds)) {
    return "unknown";
  }
  let remaining = Math.max(0, Math.floor(totalSeconds));
  const units: Array<[string, number]> = [
    ["d", 86400],
    ["h", 3600],
    ["m", 60],
    ["s", 1],
  ];
  const parts: string[] = [];
  for (const [label, unitSeconds] of units) {
    if (remaining < unitSeconds && parts.length === 0 && label !== "s") {
      continue;
    }
    const amount = Math.floor(remaining / unitSeconds);
    if (amount > 0 || parts.length > 0 || label === "s") {
      parts.push(`${amount}${label}`);
      remaining -= amount * unitSeconds;
    }
    if (parts.length === 2) {
      break;
    }
  }
  return parts.join(" ");
}

function formatUsageWindow(window: UsageWindow | null | undefined, label: string): string | null {
  const left = getQuotaLeft(window);
  if (left === null) {
    return null;
  }
  const resetText = typeof window?.reset_after_seconds === "number"
    ? `, ${formatDuration(window.reset_after_seconds)} reset`
    : "";
  return `${label} ${formatPercent(left)} left${resetText}`;
}

function formatCredits(credits: UsageCredits | null | undefined): string | null {
  if (!credits) {
    return null;
  }
  if (credits.unlimited) {
    return "credits unlimited";
  }
  if (!credits.has_credits) {
    return null;
  }
  const details: string[] = [];
  if (typeof credits.balance === "number") {
    details.push(`balance ${credits.balance}`);
  }
  if (typeof credits.approx_local_messages === "number") {
    details.push(`~${credits.approx_local_messages} local`);
  }
  if (typeof credits.approx_cloud_messages === "number") {
    details.push(`~${credits.approx_cloud_messages} cloud`);
  }
  return `credits ${details.join(", ") || "available"}`;
}

export function hasUsableQuota(usage: UsageResponse): boolean {
  const primaryLeft = getQuotaLeft(usage.rate_limit?.primary_window);
  if (usage.rate_limit?.allowed && primaryLeft !== null && primaryLeft > 0) {
    return true;
  }
  return Boolean(usage.credits?.unlimited || usage.credits?.has_credits);
}

export function describeQuotaBlocker(usage: UsageResponse): string {
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

export function formatQuotaSummary(usage: UsageResponse): string {
  const parts = [
    formatUsageWindow(usage.rate_limit?.primary_window, "5h"),
    formatUsageWindow(usage.rate_limit?.secondary_window, "7d"),
    formatCredits(usage.credits),
  ].filter((value): value is string => Boolean(value));
  return parts.join(" | ") || "quota unavailable";
}

export async function inspectQuota(auth: CodexAuth): Promise<QuotaAssessment> {
  const summary = summarizeCodexAuth(auth);
  const response = await fetch(WHAM_USAGE_URL, {
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${auth.tokens.access_token}`,
      "ChatGPT-Account-Id": summary.accountId,
      "User-Agent": "codex-rotate-app",
    },
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  const body = await response.text();
  if (!response.ok) {
    throw new Error(`Usage lookup failed (${response.status}): ${body || response.statusText}`);
  }
  const usage = JSON.parse(body) as UsageResponse;
  const usable = hasUsableQuota(usage);
  return {
    usage,
    usable,
    summary: formatQuotaSummary(usage),
    blocker: usable ? null : describeQuotaBlocker(usage),
  };
}
