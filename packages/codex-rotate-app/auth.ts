import { existsSync, readFileSync } from "node:fs";
import type {
  AuthSummary,
  CodexAuth,
  CodexDesktopMcpRequest,
  DeviceLoginPayload,
} from "./types.ts";

const DEFAULT_JSON_RPC_ID = "codex-rotate-app-login";

function parseJson<T>(raw: string, fallbackMessage: string): T {
  try {
    return JSON.parse(raw) as T;
  } catch {
    throw new Error(fallbackMessage);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

export function decodeJwtPayload(jwt: string): Record<string, unknown> {
  const parts = jwt.split(".");
  if (parts.length !== 3) {
    throw new Error("Invalid JWT");
  }

  const payload = parts[1]!
    .replaceAll("-", "+")
    .replaceAll("_", "/");
  const padded = payload + "=".repeat((4 - (payload.length % 4)) % 4);
  return parseJson<Record<string, unknown>>(
    Buffer.from(padded, "base64").toString("utf8"),
    "Invalid JWT payload",
  );
}

function extractAccountIdFromToken(jwt: string): string | null {
  try {
    const payload = decodeJwtPayload(jwt);
    const authInfo = payload["https://api.openai.com/auth"];
    if (!isRecord(authInfo)) {
      return null;
    }
    return typeof authInfo.chatgpt_account_id === "string" ? authInfo.chatgpt_account_id : null;
  } catch {
    return null;
  }
}

export function loadCodexAuth(authFilePath: string): CodexAuth {
  if (!existsSync(authFilePath)) {
    throw new Error(`Codex auth file not found at ${authFilePath}.`);
  }
  return parseJson<CodexAuth>(readFileSync(authFilePath, "utf8"), `Invalid Codex auth file at ${authFilePath}`);
}

export function summarizeCodexAuth(auth: CodexAuth): AuthSummary {
  let email = "unknown";
  let planType = "unknown";

  try {
    const payload = decodeJwtPayload(auth.tokens.access_token);
    const profile = payload["https://api.openai.com/profile"];
    if (isRecord(profile) && typeof profile.email === "string") {
      email = profile.email;
    }
    const authInfo = payload["https://api.openai.com/auth"];
    if (isRecord(authInfo) && typeof authInfo.chatgpt_plan_type === "string") {
      planType = authInfo.chatgpt_plan_type;
    }
  } catch {
    // Fall back to id_token/defaults below.
  }

  if (email === "unknown") {
    try {
      const payload = decodeJwtPayload(auth.tokens.id_token);
      if (typeof payload.email === "string") {
        email = payload.email;
      }
    } catch {
      // Ignore fallback failure.
    }
  }

  return {
    email,
    planType,
    accountId: extractAccountIdFromToken(auth.tokens.access_token)
      ?? extractAccountIdFromToken(auth.tokens.id_token)
      ?? auth.tokens.account_id,
  };
}

export function buildDeviceLoginPayload(auth: CodexAuth): DeviceLoginPayload {
  const summary = summarizeCodexAuth(auth);
  return {
    type: "chatgptAuthTokens",
    accessToken: auth.tokens.access_token,
    chatgptAccountId: summary.accountId,
    chatgptPlanType: summary.planType === "unknown" ? null : summary.planType,
  };
}

export function redactAccessToken(accessToken: string): string {
  if (accessToken.length <= 24) {
    return accessToken;
  }
  return `${accessToken.slice(0, 24)}...`;
}

export function sanitizeDeviceLoginPayload(payload: DeviceLoginPayload): DeviceLoginPayload {
  return {
    ...payload,
    accessToken: redactAccessToken(payload.accessToken),
  };
}

export function buildLoginStartRequest(
  auth: CodexAuth,
  options?: {
    hostId?: string;
    requestId?: string;
  },
): CodexDesktopMcpRequest<DeviceLoginPayload> {
  return {
    type: "mcp-request",
    hostId: options?.hostId ?? "local",
    request: {
      jsonrpc: "2.0",
      id: options?.requestId ?? DEFAULT_JSON_RPC_ID,
      method: "account/login/start",
      params: buildDeviceLoginPayload(auth),
    },
  };
}

export function sanitizeLoginStartRequest(
  request: CodexDesktopMcpRequest<DeviceLoginPayload>,
): CodexDesktopMcpRequest<DeviceLoginPayload> {
  return {
    ...request,
    request: {
      ...request.request,
      params: sanitizeDeviceLoginPayload(request.request.params),
    },
  };
}
