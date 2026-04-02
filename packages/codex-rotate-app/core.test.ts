import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import {
  buildDeviceLoginPayload,
  buildLoginStartRequest,
  sanitizeLoginStartRequest,
  summarizeCodexAuth,
} from "./auth.ts";
import { readCodexSignals } from "./logs.ts";
import { describeQuotaBlocker, formatQuotaSummary, hasUsableQuota } from "./quota.ts";
import { formatRotationSummary, runRotateNext } from "./rotate.ts";
import type { CodexAuth, UsageResponse } from "./types.ts";

function base64UrlEncode(input: string): string {
  return Buffer.from(input, "utf8").toString("base64url");
}

function makeJwt(payload: Record<string, unknown>): string {
  return [
    base64UrlEncode(JSON.stringify({ alg: "none", typ: "JWT" })),
    base64UrlEncode(JSON.stringify(payload)),
    "signature",
  ].join(".");
}

function makeAuth(): CodexAuth {
  return {
    auth_mode: "chatgpt",
    OPENAI_API_KEY: null,
    tokens: {
      access_token: makeJwt({
        "https://api.openai.com/profile": { email: "dev.22@astronlab.com" },
        "https://api.openai.com/auth": {
          chatgpt_account_id: "acct-123",
          chatgpt_plan_type: "free",
        },
      }),
      id_token: makeJwt({ email: "dev.22@astronlab.com" }),
      refresh_token: "refresh-token",
      account_id: "acct-fallback",
    },
    last_refresh: "2026-04-02T00:00:00.000Z",
  };
}

function makeUsage(overrides?: Partial<UsageResponse>): UsageResponse {
  return {
    user_id: "user-1",
    account_id: "acct-123",
    email: "dev.22@astronlab.com",
    plan_type: "free",
    rate_limit: {
      allowed: true,
      limit_reached: false,
      primary_window: {
        used_percent: 10,
        limit_window_seconds: 18000,
        reset_after_seconds: 7200,
        reset_at: 1775138000,
      },
      secondary_window: {
        used_percent: 100,
        limit_window_seconds: 604800,
        reset_after_seconds: 86400,
        reset_at: 1775210000,
      },
    },
    code_review_rate_limit: null,
    additional_rate_limits: null,
    credits: {
      has_credits: false,
      unlimited: false,
      balance: null,
      approx_local_messages: null,
      approx_cloud_messages: null,
    },
    promo: null,
    ...overrides,
  };
}

describe("auth helpers", () => {
  test("summarizeCodexAuth extracts email, plan, and account id", () => {
    const summary = summarizeCodexAuth(makeAuth());
    expect(summary).toEqual({
      email: "dev.22@astronlab.com",
      planType: "free",
      accountId: "acct-123",
    });
  });

  test("buildDeviceLoginPayload matches account/login/start contract", () => {
    const payload = buildDeviceLoginPayload(makeAuth());
    expect(payload).toEqual({
      type: "chatgptAuthTokens",
      accessToken: makeAuth().tokens.access_token,
      chatgptAccountId: "acct-123",
      chatgptPlanType: "free",
    });
  });

  test("buildLoginStartRequest wraps payload in mcp-request envelope", () => {
    const request = buildLoginStartRequest(makeAuth(), { requestId: "rotate-1" });
    expect(request.request.method).toBe("account/login/start");
    expect(request.request.id).toBe("rotate-1");
    expect(request.request.params.chatgptAccountId).toBe("acct-123");
  });

  test("sanitizeLoginStartRequest redacts the access token", () => {
    const request = sanitizeLoginStartRequest(buildLoginStartRequest(makeAuth(), { requestId: "rotate-2" }));
    expect(request.request.params.accessToken).toEndWith("...");
    expect(request.request.params.accessToken).not.toBe(makeAuth().tokens.access_token);
    expect(request.request.params.chatgptAccountId).toBe("acct-123");
  });
});

describe("quota helpers", () => {
  test("hasUsableQuota returns true when primary window has remaining quota", () => {
    expect(hasUsableQuota(makeUsage())).toBe(true);
  });

  test("describeQuotaBlocker explains exhausted quota", () => {
    const usage = makeUsage({
      rate_limit: {
        allowed: false,
        limit_reached: true,
        primary_window: {
          used_percent: 100,
          limit_window_seconds: 18000,
          reset_after_seconds: 3600,
          reset_at: 1775138000,
        },
        secondary_window: null,
      },
    });
    expect(describeQuotaBlocker(usage)).toContain("5h quota exhausted");
  });

  test("formatQuotaSummary includes 5h and 7d windows", () => {
    expect(formatQuotaSummary(makeUsage())).toContain("5h 90% left");
  });
});

describe("rotation helpers", () => {
  test("runRotateNext reloads auth after successful rotate command", () => {
    const tempDir = mkdtempSync(join(tmpdir(), "codex-rotate-app-"));
    try {
      const authPath = join(tempDir, "auth.json");
      writeFileSync(authPath, JSON.stringify(makeAuth(), null, 2), "utf8");

      const result = runRotateNext({
        authFilePath: authPath,
        rotateEntrypoint: "/fake/codex-rotate/index.ts",
        runtime: "bun",
        repoRoot: tempDir,
        run: () => ({
          status: 0,
          stdout: "ok",
          stderr: "",
          signal: null,
          output: [],
          pid: 1,
        }),
      });

      expect(formatRotationSummary(result.summary)).toContain("dev.22@astronlab.com");
      expect(result.loginPayload.chatgptAccountId).toBe("acct-123");
    } finally {
      rmSync(tempDir, { force: true, recursive: true });
    }
  });
});

describe("log signal filtering", () => {
  test("readCodexSignals keeps real quota-hit events and ignores string noise", () => {
    const tempDir = mkdtempSync(join(tmpdir(), "codex-rotate-app-logs-"));
    try {
      const logsPath = join(tempDir, "logs.sqlite");
      const schemaResult = spawnSync(
        "sqlite3",
        [
          logsPath,
          `
create table logs (
  id integer primary key,
  ts integer,
  target text,
  feedback_log_body text
);
insert into logs (id, ts, target, feedback_log_body) values
  (1, 1000, 'log', 'Received message {"type":"error","error":{"type":"usage_limit_reached","message":"The usage limit has been reached"},"status_code":429}'),
  (2, 1001, 'codex_app_server::outgoing_message', 'app-server event: account/rateLimits/updated targeted_connections=1'),
  (3, 1002, 'codex_api::endpoint::responses_websocket', 'local tool output mentioning usage_limit_reached but not a real limit event');
`,
        ],
        {
          encoding: "utf8",
        },
      );
      expect(schemaResult.status).toBe(0);

      const signals = readCodexSignals(logsPath);
      expect(signals).toEqual([
        {
          id: 1,
          ts: 1000,
          kind: "usage_limit_reached",
          target: "log",
          body: 'Received message {"type":"error","error":{"type":"usage_limit_reached","message":"The usage limit has been reached"},"status_code":429}',
        },
        {
          id: 2,
          ts: 1001,
          kind: "rate_limits_updated",
          target: "codex_app_server::outgoing_message",
          body: "app-server event: account/rateLimits/updated targeted_connections=1",
        },
      ]);
    } finally {
      rmSync(tempDir, { force: true, recursive: true });
    }
  });
});
