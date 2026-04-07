import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import {
  buildCodexLoginManagedBrowserWrapperPath,
  buildAccountFamilyEmail,
  CODEX_ROTATE_ACCOUNT_FLOW_FILE,
  ROTATE_STATE_FILE,
  buildCodexRotateOpenAiTempProfileName,
  cleanupLegacyCodexRotateArtifacts,
  computeNextAccountFamilySuffix,
  computeNextGmailAliasSuffix,
  deriveFamilyFrontierSuffix,
  deriveWorkflowRefFromFilePath,
  ensureCodexLoginManagedBrowserWrapper,
  getCodexRotateHome,
  getCodexLoginRetryDelayMs,
  isRetryableCodexLoginWorkflowErrorMessage,
  loadCredentialStore,
  normalizeBaseEmailFamily,
  normalizeCredentialStore,
  normalizeGmailBaseEmail,
  readWorkflowFileMetadata,
  resolveCreateBaseEmail,
  resolveManagedProfileNameFromCandidates,
  scoreEmailForManagedProfileName,
  saveCredentialStore,
  serializeCredentialStore,
  setCodexRotateHomeForTesting,
  shouldResetDeviceAuthSessionForRateLimit,
  shouldResetCodexLoginSessionForRetry,
  shouldUseDefaultCreateFamilyHint,
  selectBestEmailForManagedProfile,
  selectBestSystemChromeProfileMatch,
  selectPendingBaseEmailHintForProfile,
  selectPendingCredentialForFamily,
  shouldPromptForCodexRotateSecretUnlock,
  shouldResetFastBrowserDaemonForSocketClose,
  selectStoredBaseEmailHint,
} from "./automation.ts";

function makeSecretRef(objectId: string) {
  return {
    type: "secret_ref" as const,
    store: "bitwarden-cli" as const,
    object_id: objectId,
    field_path: null,
    version: null,
  };
}

describe("gmail alias helpers", () => {
  test("normalizes the Gmail base address before suffixing", () => {
    expect(normalizeGmailBaseEmail("Dev.User+17@gmail.com")).toBe(
      "dev.user@gmail.com",
    );
  });

  test("picks the next alias suffix from known emails", () => {
    expect(
      computeNextGmailAliasSuffix("dev.user@gmail.com", 1, [
        "dev.user+1@gmail.com",
        "dev.user+7@gmail.com",
        "other@gmail.com",
      ]),
    ).toBe(2);
  });

  test("respects the persisted family counter when it is ahead of the pool", () => {
    expect(
      computeNextGmailAliasSuffix("dev.user@gmail.com", 5, [
        "dev.user+1@gmail.com",
        "dev.user+2@gmail.com",
      ]),
    ).toBe(5);
  });
});

describe("templated email family helpers", () => {
  test("normalizes a templated family address", () => {
    expect(normalizeBaseEmailFamily("Dev.{N}@HotspotPrime.com")).toBe(
      "dev.{n}@hotspotprime.com",
    );
  });

  test("builds a concrete email from a templated family", () => {
    expect(buildAccountFamilyEmail("dev.{N}@hotspotprime.com", 7)).toBe(
      "dev.7@hotspotprime.com",
    );
  });

  test("picks the next suffix from templated family emails", () => {
    expect(
      computeNextAccountFamilySuffix("dev.{N}@hotspotprime.com", 1, [
        "dev.1@hotspotprime.com",
        "dev.4@hotspotprime.com",
        "other@gmail.com",
      ]),
    ).toBe(2);
  });

  test("respects the persisted family pointer for sparse templated families", () => {
    expect(
      computeNextAccountFamilySuffix("dev.{N}@astronlab.com", 3, [
        "dev.21@astronlab.com",
      ]),
    ).toBe(3);
  });

  test("can derive the next frontier suffix when no family cursor was persisted", () => {
    expect(
      deriveFamilyFrontierSuffix("dev.{N}@astronlab.com", [
        "dev.20@astronlab.com",
        "dev.22@astronlab.com",
        "dev.23@astronlab.com",
      ]),
    ).toBe(24);
  });

  test("accepts templated families and rejects Gmail for implicit create hints", () => {
    expect(shouldUseDefaultCreateFamilyHint("dev.{n}@astronlab.com")).toBe(
      true,
    );
    expect(shouldUseDefaultCreateFamilyHint("qa.{n}@astronlab.com")).toBe(true);
    expect(shouldUseDefaultCreateFamilyHint("dev.user@gmail.com")).toBe(false);
  });
});

describe("workflow metadata", () => {
  test("reads preferred_profile from the main local codex-rotate workflow", () => {
    const metadata = readWorkflowFileMetadata(CODEX_ROTATE_ACCOUNT_FLOW_FILE);

    expect(metadata.workflowRef).toBe(
      "workspace.web.auth-openai-com.codex-rotate-account-flow-main",
    );
    expect(metadata.preferredProfileName).toBe("dev-1");
    expect(metadata.preferredEmail).toBeNull();
  });

  test("derives the minimal workflow ref from an alternate local workflow file", () => {
    const minimalWorkflowFile = join(
      dirname(CODEX_ROTATE_ACCOUNT_FLOW_FILE),
      "codex-rotate-account-flow-minimal.yaml",
    );

    expect(deriveWorkflowRefFromFilePath(minimalWorkflowFile)).toBe(
      "workspace.web.auth-openai-com.codex-rotate-account-flow-minimal",
    );
    expect(readWorkflowFileMetadata(minimalWorkflowFile).workflowRef).toBe(
      "workspace.web.auth-openai-com.codex-rotate-account-flow-minimal",
    );
  });
});

describe("credential store normalization", () => {
  test("drops pending entries that already exist in the account inventory", () => {
    const store = normalizeCredentialStore({
      accounts: [
        {
          email: "dev.1@astronlab.com",
        },
      ],
      pending: {
        "dev.1@astronlab.com": {
          email: "dev.1@astronlab.com",
          profile_name: "dev-1",
          base_email: "dev.{n}@astronlab.com",
          suffix: 1,
          selector: null,
          alias: null,
          created_at: "2026-04-05T04:50:10.406Z",
          updated_at: "2026-04-05T05:39:48.882Z",
        },
      },
    } as never);

    expect(store.pending).toEqual({});
  });

  test("drops stale pending entries that are behind the family frontier", () => {
    const store = normalizeCredentialStore({
      accounts: [
        {
          email: "dev.23@astronlab.com",
        },
      ],
      pending: {
        "dev.1@astronlab.com": {
          email: "dev.1@astronlab.com",
          profile_name: "dev-1",
          base_email: "dev.{n}@astronlab.com",
          suffix: 1,
          selector: null,
          alias: null,
          created_at: "2026-04-05T04:50:10.406Z",
          updated_at: "2026-04-05T05:39:48.882Z",
        },
      },
    } as never);

    expect(store.pending).toEqual({});
  });

  test("migrates to v4 and drops stale benchmark families", () => {
    const store = normalizeCredentialStore({
      version: 3,
      families: {
        "dev-1::bench.devicefix.{n}@astronlab.com": {
          profile_name: "dev-1",
          base_email: "bench.devicefix.{n}@astronlab.com",
          next_suffix: 8,
          created_at: "2026-04-06T00:00:00.000Z",
          updated_at: "2026-04-06T00:00:00.000Z",
          last_created_email: "bench.devicefix.7@astronlab.com",
        },
        "dev-1::dev.{n}@astronlab.com": {
          profile_name: "dev-1",
          base_email: "dev.{n}@astronlab.com",
          next_suffix: 35,
          created_at: "2026-04-06T00:00:00.000Z",
          updated_at: "2026-04-06T00:00:00.000Z",
          last_created_email: "dev.34@astronlab.com",
        },
      },
      pending: {
        "bench.devicefix.8@astronlab.com": {
          email: "bench.devicefix.8@astronlab.com",
          profile_name: "dev-1",
          base_email: "bench.devicefix.{n}@astronlab.com",
          suffix: 8,
          selector: null,
          alias: null,
          created_at: "2026-04-06T00:00:00.000Z",
          updated_at: "2026-04-06T00:00:00.000Z",
          started_at: "2026-04-06T00:00:00.000Z",
        },
        "dev.35@astronlab.com": {
          email: "dev.35@astronlab.com",
          profile_name: "dev-1",
          base_email: "dev.{n}@astronlab.com",
          suffix: 35,
          selector: null,
          alias: null,
          created_at: "2026-04-06T00:00:00.000Z",
          updated_at: "2026-04-06T00:00:00.000Z",
          started_at: "2026-04-06T00:00:00.000Z",
        },
      },
    } as never);

    expect(store.version).toBe(4);
    expect(store.default_create_base_email).toBe("dev.{n}@astronlab.com");
    expect(Object.keys(store.families)).toEqual([
      "dev-1::dev.{n}@astronlab.com",
    ]);
    expect(Object.keys(store.pending)).toEqual(["dev.35@astronlab.com"]);
  });

  test("drops non-dev pending entries even in version 4 state", () => {
    const store = normalizeCredentialStore({
      version: 4,
      pending: {
        "qa.300@astronlab.com": {
          email: "qa.300@astronlab.com",
          profile_name: "dev-1",
          base_email: "qa.{n}@astronlab.com",
          suffix: 300,
          selector: null,
          alias: null,
          created_at: "2026-04-06T17:00:00.000Z",
          updated_at: "2026-04-06T17:00:00.000Z",
          started_at: "2026-04-06T17:00:00.000Z",
        },
        "dev.user+1@gmail.com": {
          email: "dev.user+1@gmail.com",
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 1,
          selector: null,
          alias: null,
          created_at: "2026-04-06T18:00:00.000Z",
          updated_at: "2026-04-06T18:00:00.000Z",
          started_at: "2026-04-06T18:00:00.000Z",
        },
        "dev.35@astronlab.com": {
          email: "dev.35@astronlab.com",
          profile_name: "dev-1",
          base_email: "dev.{n}@astronlab.com",
          suffix: 35,
          selector: null,
          alias: null,
          created_at: "2026-04-06T19:00:00.000Z",
          updated_at: "2026-04-06T19:00:00.000Z",
          started_at: "2026-04-06T19:00:00.000Z",
        },
      },
    } as never);

    expect(Object.keys(store.pending)).toEqual(["dev.35@astronlab.com"]);
  });
});

describe("temporary profile naming", () => {
  test("derives the same retained OpenAI temp profile for the same workflow run stamp", () => {
    expect(
      buildCodexRotateOpenAiTempProfileName("2026-03-22T10:11:12.000Z"),
    ).toBe(buildCodexRotateOpenAiTempProfileName("2026-03-22T10:11:12.000Z"));
  });

  test("changes the retained OpenAI temp profile when the workflow run stamp changes", () => {
    expect(
      buildCodexRotateOpenAiTempProfileName("2026-03-22T10:11:12.000Z"),
    ).not.toBe(
      buildCodexRotateOpenAiTempProfileName("2026-03-22T10:11:13.000Z"),
    );
  });
});

describe("fast-browser daemon recovery", () => {
  test("detects detached daemon socket-close crashes as retryable runtime resets", () => {
    expect(
      shouldResetFastBrowserDaemonForSocketClose(
        "Error: Daemon closed the socket before sending a response",
      ),
    ).toBe(true);
    expect(
      shouldResetFastBrowserDaemonForSocketClose(
        "Timed out waiting for fast-browser daemon response from /tmp/demo.sock",
      ),
    ).toBe(false);
    expect(shouldResetFastBrowserDaemonForSocketClose("other failure")).toBe(
      false,
    );
  });
});

describe("codex login managed-browser wrapper", () => {
  test("derives a stable wrapper path for the same profile and codex binary", () => {
    expect(buildCodexLoginManagedBrowserWrapperPath("dev-1", "codex")).toBe(
      buildCodexLoginManagedBrowserWrapperPath("dev-1", "codex"),
    );
  });

  test("changes the wrapper path when the profile changes", () => {
    expect(buildCodexLoginManagedBrowserWrapperPath("dev-1", "codex")).not.toBe(
      buildCodexLoginManagedBrowserWrapperPath("dev-2", "codex"),
    );
  });

  test("intercepts login through the dedicated managed-login helper", () => {
    const fixtureRoot = mkdtempSync(join(tmpdir(), "codex-rotate-wrapper-"));
    const rotateHome = join(fixtureRoot, "rotate-home");
    const helperLogPath = join(fixtureRoot, "helper-log.json");
    const helperPath = join(fixtureRoot, "fake-helper.mjs");
    const codexMarkerPath = join(fixtureRoot, "codex-invoked.txt");
    const codexPath = join(fixtureRoot, "fake-codex.sh");
    const previousRotateHome = getCodexRotateHome();

    writeFileSync(
      helperPath,
      [
        "#!/usr/bin/env node",
        'import { writeFileSync } from "node:fs";',
        "const logPath = process.env.CODEX_ROTATE_TEST_HELPER_LOG;",
        "writeFileSync(logPath, JSON.stringify({",
        "  argv: process.argv.slice(2),",
        "  profile: process.env.FAST_BROWSER_PROFILE || null,",
        "  realCodex: process.env.CODEX_ROTATE_REAL_CODEX || null,",
        "}));",
      ].join("\n"),
      { encoding: "utf8", mode: 0o700 },
    );
    writeFileSync(
      codexPath,
      [
        "#!/bin/sh",
        `printf 'invoked' > ${JSON.stringify(codexMarkerPath)}`,
        "exit 0",
      ].join("\n"),
      { mode: 0o700 },
    );

    const previousHelper = process.env.CODEX_ROTATE_LOGIN_HELPER_BIN;
    const previousLog = process.env.CODEX_ROTATE_TEST_HELPER_LOG;

    try {
      mkdirSync(rotateHome, { recursive: true });
      setCodexRotateHomeForTesting(rotateHome);
      process.env.CODEX_ROTATE_LOGIN_HELPER_BIN = helperPath;
      process.env.CODEX_ROTATE_TEST_HELPER_LOG = helperLogPath;

      const wrapperPath = ensureCodexLoginManagedBrowserWrapper(
        "managed-dev-1",
        codexPath,
      );
      const result = spawnSync(wrapperPath, ["login"], {
        encoding: "utf8",
        env: process.env,
      });

      expect(result.status).toBe(0);
      const logged = JSON.parse(readFileSync(helperLogPath, "utf8")) as {
        argv: string[];
        profile: string | null;
        realCodex: string | null;
      };
      expect(logged.profile).toBe("managed-dev-1");
      expect(logged.argv).toEqual([]);
      expect(logged.realCodex).toBe(codexPath);
      expect(existsSync(codexMarkerPath)).toBe(false);
    } finally {
      setCodexRotateHomeForTesting(previousRotateHome);
      if (previousHelper === undefined) {
        delete process.env.CODEX_ROTATE_LOGIN_HELPER_BIN;
      } else {
        process.env.CODEX_ROTATE_LOGIN_HELPER_BIN = previousHelper;
      }
      if (previousLog === undefined) {
        delete process.env.CODEX_ROTATE_TEST_HELPER_LOG;
      } else {
        process.env.CODEX_ROTATE_TEST_HELPER_LOG = previousLog;
      }
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });

  test("routes macOS open-based launches through the managed-profile opener", () => {
    const fixtureRoot = mkdtempSync(join(tmpdir(), "codex-rotate-wrapper-"));
    const rotateHome = join(fixtureRoot, "rotate-home");
    const openerLogPath = join(fixtureRoot, "opener-log.json");
    const openerPath = join(fixtureRoot, "fake-opener.mjs");
    const helperPath = join(fixtureRoot, "fake-helper.mjs");
    const codexPath = join(fixtureRoot, "fake-codex.sh");
    const previousRotateHome = getCodexRotateHome();

    writeFileSync(
      openerPath,
      [
        "#!/usr/bin/env node",
        'import { writeFileSync } from "node:fs";',
        "const logPath = process.env.CODEX_ROTATE_TEST_OPENER_LOG;",
        "writeFileSync(logPath, JSON.stringify({",
        "  argv: process.argv.slice(2),",
        "  profile: process.env.FAST_BROWSER_PROFILE || null,",
        "  browser: process.env.BROWSER || null,",
        "}));",
      ].join("\n"),
      { encoding: "utf8", mode: 0o700 },
    );
    writeFileSync(helperPath, ["#!/bin/sh", "exit 0"].join("\n"), {
      mode: 0o700,
    });
    writeFileSync(
      codexPath,
      [
        "#!/bin/sh",
        'open "https://auth.openai.com/oauth/authorize?state=test-wrapper"',
        "exit 0",
      ].join("\n"),
      { mode: 0o700 },
    );

    const previousOpener = process.env.CODEX_ROTATE_BROWSER_OPENER_BIN;
    const previousHelper = process.env.CODEX_ROTATE_LOGIN_HELPER_BIN;
    const previousLog = process.env.CODEX_ROTATE_TEST_OPENER_LOG;

    try {
      mkdirSync(rotateHome, { recursive: true });
      setCodexRotateHomeForTesting(rotateHome);
      process.env.CODEX_ROTATE_BROWSER_OPENER_BIN = openerPath;
      process.env.CODEX_ROTATE_LOGIN_HELPER_BIN = helperPath;
      process.env.CODEX_ROTATE_TEST_OPENER_LOG = openerLogPath;

      const wrapperPath = ensureCodexLoginManagedBrowserWrapper(
        "managed-dev-1",
        codexPath,
      );
      const result = spawnSync(wrapperPath, ["status"], {
        encoding: "utf8",
        env: process.env,
      });

      expect(result.status).toBe(0);
      const logged = JSON.parse(readFileSync(openerLogPath, "utf8")) as {
        argv: string[];
        profile: string | null;
        browser: string | null;
      };
      expect(logged.profile).toBe("managed-dev-1");
      expect(logged.argv).toContain(
        "https://auth.openai.com/oauth/authorize?state=test-wrapper",
      );
      expect(logged.browser).toBe(openerPath);
    } finally {
      setCodexRotateHomeForTesting(previousRotateHome);
      if (previousOpener === undefined) {
        delete process.env.CODEX_ROTATE_BROWSER_OPENER_BIN;
      } else {
        process.env.CODEX_ROTATE_BROWSER_OPENER_BIN = previousOpener;
      }
      if (previousHelper === undefined) {
        delete process.env.CODEX_ROTATE_LOGIN_HELPER_BIN;
      } else {
        process.env.CODEX_ROTATE_LOGIN_HELPER_BIN = previousHelper;
      }
      if (previousLog === undefined) {
        delete process.env.CODEX_ROTATE_TEST_OPENER_LOG;
      } else {
        process.env.CODEX_ROTATE_TEST_OPENER_LOG = previousLog;
      }
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });

  test("blocks non-URL browser launches instead of falling back to the system browser", () => {
    const openerPath = join(
      import.meta.dir,
      "codex-login-managed-browser-opener.mjs",
    );
    const result = spawnSync(process.execPath, [openerPath, "/tmp/not-a-url"], {
      encoding: "utf8",
      env: {
        ...process.env,
        FAST_BROWSER_PROFILE: "managed-dev-1",
      },
    });

    expect(result.status).toBe(1);
    expect(result.stderr).toContain(
      "Managed Codex browser opener refused a non-URL browser launch request.",
    );
  });
});

describe("managed login helper", () => {
  test("starts login through codex app-server and exits on completion", () => {
    const fixtureRoot = mkdtempSync(join(tmpdir(), "codex-rotate-app-server-"));
    const helperPath = join(
      import.meta.dir,
      "codex-login-app-server-helper.mjs",
    );
    const fakeCodexPath = join(fixtureRoot, "fake-codex.mjs");

    writeFileSync(
      fakeCodexPath,
      [
        "#!/usr/bin/env node",
        "import process from 'node:process';",
        "let buffer = '';",
        "function send(message) { process.stdout.write(JSON.stringify(message) + '\\n'); }",
        "process.stdin.setEncoding('utf8');",
        "process.stdin.on('data', (chunk) => {",
        "  buffer += chunk;",
        "  while (true) {",
        "    const newlineIndex = buffer.indexOf('\\n');",
        "    if (newlineIndex === -1) break;",
        "    const line = buffer.slice(0, newlineIndex).trim();",
        "    buffer = buffer.slice(newlineIndex + 1);",
        "    if (!line) continue;",
        "    const message = JSON.parse(line);",
        "    if (message.method === 'initialize') {",
        "      send({ id: message.id, result: { userAgent: 'fake', codexHome: '/tmp', platformFamily: 'unix', platformOs: 'macos' } });",
        "    } else if (message.method === 'account/login/start') {",
        "      send({ id: message.id, result: { type: 'chatgpt', loginId: 'login-123', authUrl: 'https://auth.openai.com/oauth/authorize?redirect_uri=' + encodeURIComponent('http://localhost:1455/auth/callback') } });",
        "      setTimeout(() => send({ jsonrpc: '2.0', method: 'account/login/completed', params: { success: true, loginId: 'login-123', error: null } }), 25);",
        "    } else if (message.method === 'account/login/cancel') {",
        "      send({ id: message.id, result: { status: 'canceled' } });",
        "    }",
        "  }",
        "});",
      ].join("\n"),
      { encoding: "utf8", mode: 0o700 },
    );

    try {
      const result = spawnSync(process.execPath, [helperPath], {
        encoding: "utf8",
        env: {
          ...process.env,
          CODEX_ROTATE_REAL_CODEX: fakeCodexPath,
        },
      });

      expect(result.status).toBe(0);
      expect(result.stderr).toContain(
        "Starting local login server on http://localhost:1455.",
      );
      expect(result.stderr).toContain(
        "https://auth.openai.com/oauth/authorize?",
      );
      expect(result.stderr).toContain("Successfully logged in");
    } finally {
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });
});

describe("automation bridge transport", () => {
  test("accepts request payloads through --request-file", () => {
    const fixtureRoot = mkdtempSync(join(tmpdir(), "codex-rotate-bridge-"));
    const requestPath = join(fixtureRoot, "request.json");

    writeFileSync(
      requestPath,
      JSON.stringify({
        command: "read-workflow-metadata",
        payload: {
          filePath: CODEX_ROTATE_ACCOUNT_FLOW_FILE,
        },
      }),
      "utf8",
    );

    try {
      const result = spawnSync(
        "bun",
        [
          "packages/codex-rotate/automation-bridge.ts",
          "--request-file",
          requestPath,
        ],
        {
          cwd: join(import.meta.dir, "..", ".."),
          encoding: "utf8",
        },
      );

      expect(result.status).toBe(0);
      const response = JSON.parse(result.stdout) as {
        ok: boolean;
        result?: { preferredProfileName?: string | null };
      };
      expect(response.ok).toBe(true);
      expect(response.result?.preferredProfileName).toBe("dev-1");
    } finally {
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });
});

describe("secret unlock prompt policy", () => {
  test("allows the bridge to force interactive secret unlock prompts", () => {
    const previous = process.env.CODEX_ROTATE_ALLOW_INTERACTIVE_SECRET_UNLOCK;
    try {
      process.env.CODEX_ROTATE_ALLOW_INTERACTIVE_SECRET_UNLOCK = "1";
      expect(shouldPromptForCodexRotateSecretUnlock()).toBe(true);
    } finally {
      if (previous === undefined) {
        delete process.env.CODEX_ROTATE_ALLOW_INTERACTIVE_SECRET_UNLOCK;
      } else {
        process.env.CODEX_ROTATE_ALLOW_INTERACTIVE_SECRET_UNLOCK = previous;
      }
    }
  });
});

describe("codex login retry policy", () => {
  test("retries when verification code collection is not ready yet", () => {
    expect(
      isRetryableCodexLoginWorkflowErrorMessage(
        "signup-verification-code-missing",
      ),
    ).toBe(true);
    expect(
      isRetryableCodexLoginWorkflowErrorMessage(
        "login-verification-submit-stuck:email_verification:https://auth.openai.com/email-verification",
      ),
    ).toBe(true);
  });

  test("does not retry unrelated managed-browser failures", () => {
    expect(
      isRetryableCodexLoginWorkflowErrorMessage(
        "OpenAI rejected the stored password",
      ),
    ).toBe(false);
    expect(
      isRetryableCodexLoginWorkflowErrorMessage(
        "device auth failed with status 429",
      ),
    ).toBe(false);
  });

  test("uses shorter early retries for verification propagation waits", () => {
    expect(getCodexLoginRetryDelayMs("verification_artifact_pending", 1)).toBe(
      5_000,
    );
    expect(getCodexLoginRetryDelayMs("verification_artifact_pending", 2)).toBe(
      10_000,
    );
  });

  test("keeps device-auth rate-limit retries conservative", () => {
    expect(getCodexLoginRetryDelayMs("device_auth_rate_limit", 1)).toBe(30_000);
    expect(getCodexLoginRetryDelayMs("device_auth_rate_limit", 2)).toBe(60_000);
  });

  test("keeps a reusable device-auth session after a post-issuance 429", () => {
    expect(
      shouldResetDeviceAuthSessionForRateLimit(
        "Error logging in with device code: device auth failed with status 429 Too Many Requests",
        {
          auth_url: "https://auth.openai.com/codex/device",
          device_code: "ABCD-12345",
        } as never,
      ),
    ).toBe(false);
  });

  test("drops the device-auth session when rate limiting happens before a code is issued", () => {
    expect(
      shouldResetDeviceAuthSessionForRateLimit(
        "Error logging in with device code: device code request failed with status 429 Too Many Requests",
        {
          auth_url: null,
          device_code: null,
        } as never,
      ),
    ).toBe(true);
  });

  test("does not recycle the codex auth session on the first timeout retry", () => {
    expect(shouldResetCodexLoginSessionForRetry("retryable_timeout", 1)).toBe(
      false,
    );
    expect(shouldResetCodexLoginSessionForRetry("retryable_timeout", 2)).toBe(
      true,
    );
  });

  test("always recycles the codex auth session after a state mismatch", () => {
    expect(shouldResetCodexLoginSessionForRetry("state_mismatch", 1)).toBe(
      true,
    );
    expect(shouldResetCodexLoginSessionForRetry("state_mismatch", 2)).toBe(
      true,
    );
  });
});

describe("legacy rotate-home cleanup", () => {
  test("removes obsolete root and bin artifacts but keeps current files", () => {
    const root = mkdtempSync(join(tmpdir(), "codex-rotate-home-"));
    const binDir = join(root, "bin");
    mkdirSync(binDir, { recursive: true });
    writeFileSync(join(root, "accounts.json"), "{}");
    writeFileSync(
      join(root, "codex-login-browser-capture-1.js"),
      "console.log('legacy');",
    );
    writeFileSync(join(root, "fast-browser-1.json"), "");
    mkdirSync(join(root, "codex-login-browser-shim-123"), { recursive: true });
    writeFileSync(
      join(binDir, "codex-login-managed-dev-1-deadbeef"),
      "#!/bin/sh",
    );
    writeFileSync(
      join(binDir, "codex-login-dev-1-deadbeefcafe"),
      "#!/bin/sh\nexec 'codex' \"$@\"\n",
    );
    writeFileSync(
      join(binDir, "codex-login-dev-1-123456789abc"),
      "#!/bin/sh\nexec '/tmp/codex-login-app-server-helper.mjs' \"$@\"\n",
    );

    try {
      cleanupLegacyCodexRotateArtifacts(root);

      expect(existsSync(join(root, "accounts.json"))).toBe(true);
      expect(existsSync(join(binDir, "codex-login-dev-1-123456789abc"))).toBe(
        true,
      );
      expect(existsSync(join(root, "codex-login-browser-capture-1.js"))).toBe(
        false,
      );
      expect(existsSync(join(root, "fast-browser-1.json"))).toBe(false);
      expect(existsSync(join(root, "codex-login-browser-shim-123"))).toBe(
        false,
      );
      expect(
        existsSync(join(binDir, "codex-login-managed-dev-1-deadbeef")),
      ).toBe(false);
      expect(existsSync(join(binDir, "codex-login-dev-1-deadbeefcafe"))).toBe(
        false,
      );
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("create resolution helpers", () => {
  test("prefers an explicit profile over workflow preferred_profile", () => {
    expect(
      resolveManagedProfileNameFromCandidates(["dev-1", "other"], {
        requestedProfileName: "other",
        preferredProfileName: "dev-1",
        preferredProfileSource: "/tmp/workflow.yaml",
        defaultProfileName: "dev-1",
      }),
    ).toBe("other");
  });

  test("uses workflow preferred_profile when no explicit profile is provided", () => {
    expect(
      resolveManagedProfileNameFromCandidates(["dev-1", "other"], {
        preferredProfileName: "dev-1",
        preferredProfileSource: "/tmp/workflow.yaml",
        defaultProfileName: "other",
      }),
    ).toBe("dev-1");
  });

  test("prefers explicit base email over the discovered profile email", () => {
    expect(
      resolveCreateBaseEmail("other@gmail.com", "dev.user@gmail.com"),
    ).toBe("other@gmail.com");
  });

  test("uses the discovered profile email when no explicit base email is provided", () => {
    expect(resolveCreateBaseEmail(null, "Dev.User+4@gmail.com")).toBe(
      "dev.user@gmail.com",
    );
  });

  test("defaults to the Astronlab template when no hint is available", () => {
    expect(resolveCreateBaseEmail(null, null)).toBe("dev.{n}@astronlab.com");
  });

  test("ignores legacy Gmail hints on the default create path", () => {
    expect(shouldUseDefaultCreateFamilyHint("dev.user@gmail.com")).toBe(false);
    expect(shouldUseDefaultCreateFamilyHint("dev.user+4@gmail.com")).toBe(
      false,
    );
  });

  test("keeps templated hints on the default create path", () => {
    expect(shouldUseDefaultCreateFamilyHint("dev.{N}@astronlab.com")).toBe(
      true,
    );
    expect(shouldUseDefaultCreateFamilyHint("qa.{N}@astronlab.com")).toBe(true);
  });

  test("accepts an explicit templated base email family", () => {
    expect(resolveCreateBaseEmail("dev.{N}@hotspotprime.com", null)).toBe(
      "dev.{n}@hotspotprime.com",
    );
  });

  test("matches the most likely Gmail base email for a managed profile name", () => {
    expect(
      selectBestEmailForManagedProfile("dev-1", [
        "arjuda.anjum@gmail.com",
        "dev.2.astronlab@gmail.com",
        "1.dev.astronlab@gmail.com",
      ]),
    ).toBe("1.dev.astronlab@gmail.com");
  });

  test("scores exact profile-token matches above generic matches", () => {
    expect(
      scoreEmailForManagedProfileName("dev-1", "1.dev.astronlab@gmail.com"),
    ).toBeGreaterThan(
      scoreEmailForManagedProfileName("dev-1", "dev.2.astronlab@gmail.com"),
    );
  });

  test("picks the best matching system Chrome profile by its available Gmail accounts", () => {
    const match = selectBestSystemChromeProfileMatch("dev-1", [
      {
        directory: "Profile 1",
        name: "Alamin",
        emails: ["mohammadalamin4512@gmail.com"],
      },
      {
        directory: "Default",
        name: "Arjuda",
        emails: [
          "arjuda.anjum@gmail.com",
          "dev.2.astronlab@gmail.com",
          "1.dev.astronlab@gmail.com",
        ],
      },
    ]);

    expect(match?.directory).toBe("Default");
    expect(match?.matchedEmail).toBe("1.dev.astronlab@gmail.com");
  });
});

describe("credential store normalization", () => {
  test("ignores legacy defaults while keeping family data", () => {
    const store = normalizeCredentialStore({
      version: 1,
      defaults: {
        profile_name: "old-profile",
        base_email: "old@gmail.com",
        updated_at: "2026-03-20T00:00:00.000Z",
      },
      families: {
        "dev-1::dev.user@gmail.com": {
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          next_suffix: 3,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T00:00:00.000Z",
          last_created_email: "dev.user+2@gmail.com",
        },
      },
      pending: {},
    });

    expect("defaults" in store).toBe(false);
    expect(Object.keys(store.families)).toEqual(["dev-1::dev.user@gmail.com"]);
  });

  test("migrates legacy accounts into families during normalization", () => {
    const store = normalizeCredentialStore({
      accounts: {
        "dev.user+1@gmail.com": {
          email: "dev.user+1@gmail.com",
          password: "pw-1",
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 1,
          selector: "dev.user+1@gmail.com_free",
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T00:00:00.000Z",
        },
      },
    });

    expect("accounts" in (store as Record<string, unknown>)).toBe(false);
    expect(store.families["dev-1::dev.user@gmail.com"]).toEqual({
      profile_name: "dev-1",
      base_email: "dev.user@gmail.com",
      next_suffix: 2,
      created_at: "2026-03-20T00:00:00.000Z",
      updated_at: "2026-03-20T00:00:00.000Z",
      last_created_email: "dev.user+1@gmail.com",
    });
  });

  test("writes only families and pending after normalizing old account records", () => {
    const store = normalizeCredentialStore({
      accounts: {
        "dev.user+1@gmail.com": {
          email: "dev.user+1@gmail.com",
          password: "pw-1",
          account_secret_ref: makeSecretRef("bw-dev-user-1"),
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 1,
          selector: "dev.user+1@gmail.com_free",
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T00:00:00.000Z",
        },
      },
    });

    const serialized = serializeCredentialStore(store) as Record<
      string,
      unknown
    >;
    expect("accounts" in serialized).toBe(false);
    expect(serialized.version).toBe(4);
    expect(serialized.default_create_base_email).toBe("dev.{n}@astronlab.com");
    expect(serialized.families).toEqual({
      "dev-1::dev.user@gmail.com": {
        profile_name: "dev-1",
        base_email: "dev.user@gmail.com",
        next_suffix: 2,
        created_at: "2026-03-20T00:00:00.000Z",
        updated_at: "2026-03-20T00:00:00.000Z",
        last_created_email: "dev.user+1@gmail.com",
      },
    });
  });

  test("ignores persisted Bitwarden refs from old files and relies on runtime lookup instead", () => {
    const store = normalizeCredentialStore({
      accounts: {
        "dev.user+1@gmail.com": {
          email: "dev.user+1@gmail.com",
          account_secret_ref: makeSecretRef("bw-dev-user-1"),
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 1,
          selector: "dev.user+1@gmail.com_free",
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T00:00:00.000Z",
        },
      },
    });

    expect(store.families["dev-1::dev.user@gmail.com"]).toEqual({
      profile_name: "dev-1",
      base_email: "dev.user@gmail.com",
      next_suffix: 2,
      created_at: "2026-03-20T00:00:00.000Z",
      updated_at: "2026-03-20T00:00:00.000Z",
      last_created_email: "dev.user+1@gmail.com",
    });
  });

  test("ignores merged pool accounts when normalizing credential families", () => {
    const store = normalizeCredentialStore({
      active_index: 0,
      accounts: [
        {
          label: "dev.23@astronlab.com_free",
          email: "dev.23@astronlab.com",
          account_id: "acct-123",
        },
      ],
      version: 3,
      families: {
        "dev-1::dev.{n}@astronlab.com": {
          profile_name: "dev-1",
          base_email: "dev.{n}@astronlab.com",
          next_suffix: 24,
          created_at: "2026-04-05T00:00:00.000Z",
          updated_at: "2026-04-05T00:00:00.000Z",
          last_created_email: "dev.23@astronlab.com",
        },
      },
      pending: {},
    });

    expect(store.families["dev-1::dev.{n}@astronlab.com"]?.next_suffix).toBe(
      24,
    );
  });
});

describe("credential store state-file merge", () => {
  test("writes credential state into accounts.json without a separate credential file", () => {
    const fixtureRoot = mkdtempSync(join(tmpdir(), "codex-rotate-state-"));
    const previousRotateHome = getCodexRotateHome();

    try {
      setCodexRotateHomeForTesting(fixtureRoot);
      writeFileSync(
        ROTATE_STATE_FILE,
        JSON.stringify({
          active_index: 2,
          accounts: [{ email: "dev.22@astronlab.com" }],
          version: 3,
          families: {
            "dev-1::dev.{n}@astronlab.com": {
              profile_name: "dev-1",
              base_email: "dev.{n}@astronlab.com",
              next_suffix: 23,
              created_at: "2026-04-05T00:00:00.000Z",
              updated_at: "2026-04-05T00:00:00.000Z",
              last_created_email: "dev.22@astronlab.com",
            },
          },
          pending: {
            "dev.23@astronlab.com": {
              email: "dev.23@astronlab.com",
              profile_name: "dev-1",
              base_email: "dev.{n}@astronlab.com",
              suffix: 23,
              selector: null,
              alias: null,
              created_at: "2026-04-05T00:00:00.000Z",
              updated_at: "2026-04-05T00:00:00.000Z",
              started_at: "2026-04-05T00:00:00.000Z",
            },
          },
        }),
      );

      const store = loadCredentialStore();
      expect(store.families["dev-1::dev.{n}@astronlab.com"]?.next_suffix).toBe(
        23,
      );

      saveCredentialStore(store);

      const merged = JSON.parse(readFileSync(ROTATE_STATE_FILE, "utf8")) as {
        active_index?: number;
        accounts?: Array<{ email?: string }>;
        version?: number;
        default_create_base_email?: string;
        families?: Record<string, { next_suffix?: number }>;
      };
      expect(merged.active_index).toBe(2);
      expect(merged.accounts?.[0]?.email).toBe("dev.22@astronlab.com");
      expect(merged.version).toBe(4);
      expect(merged.default_create_base_email).toBe("dev.{n}@astronlab.com");
      expect(
        merged.families?.["dev-1::dev.{n}@astronlab.com"]?.next_suffix,
      ).toBe(23);
    } finally {
      setCodexRotateHomeForTesting(previousRotateHome);
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });

  test("drops empty credential metadata from accounts.json after merge", () => {
    const fixtureRoot = mkdtempSync(join(tmpdir(), "codex-rotate-state-"));
    const previousRotateHome = getCodexRotateHome();

    try {
      setCodexRotateHomeForTesting(fixtureRoot);
      writeFileSync(
        ROTATE_STATE_FILE,
        JSON.stringify({
          active_index: 0,
          accounts: [{ email: "dev.23@astronlab.com" }],
          version: 3,
          families: {},
          pending: {},
        }),
      );

      saveCredentialStore(
        normalizeCredentialStore({
          version: 3,
          families: {},
          pending: {},
        }),
      );

      const merged = JSON.parse(readFileSync(ROTATE_STATE_FILE, "utf8")) as {
        active_index?: number;
        accounts?: Array<{ email?: string }>;
        version?: number;
        default_create_base_email?: string;
        families?: Record<string, unknown>;
        pending?: Record<string, unknown>;
      };
      expect(merged.active_index).toBe(0);
      expect(merged.accounts?.[0]?.email).toBe("dev.23@astronlab.com");
      expect(merged.version).toBe(4);
      expect(merged.default_create_base_email).toBe("dev.{n}@astronlab.com");
      expect("families" in merged).toBe(false);
      expect("pending" in merged).toBe(false);
    } finally {
      setCodexRotateHomeForTesting(previousRotateHome);
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });
});

describe("pending credential reuse", () => {
  test("drains the oldest pending credential for the same family first", () => {
    const store = {
      version: 4,
      default_create_base_email: "dev.{n}@astronlab.com",
      families: {},
      pending: {
        "dev.user+1@gmail.com": {
          email: "dev.user+1@gmail.com",
          account_secret_ref: makeSecretRef("bw-dev-user-1"),
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 1,
          selector: null,
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T00:00:00.000Z",
          started_at: "2026-03-20T00:00:00.000Z",
        },
        "dev.user+3@gmail.com": {
          email: "dev.user+3@gmail.com",
          account_secret_ref: makeSecretRef("bw-dev-user-3"),
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 3,
          selector: null,
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T03:00:00.000Z",
          started_at: "2026-03-20T03:00:00.000Z",
        },
      },
    } as Parameters<typeof selectPendingCredentialForFamily>[0];

    expect(
      selectPendingCredentialForFamily(store, "dev-1", "dev.user@gmail.com")
        ?.email,
    ).toBe("dev.user+1@gmail.com");
  });

  test("can restrict reuse to a matching alias when provided", () => {
    const store = {
      version: 4,
      default_create_base_email: "dev.{n}@astronlab.com",
      families: {},
      pending: {
        "dev.user+2@gmail.com": {
          email: "dev.user+2@gmail.com",
          account_secret_ref: makeSecretRef("bw-dev-user-2"),
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 2,
          selector: null,
          alias: "team-a",
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T02:00:00.000Z",
          started_at: "2026-03-20T02:00:00.000Z",
        },
        "dev.user+3@gmail.com": {
          email: "dev.user+3@gmail.com",
          account_secret_ref: makeSecretRef("bw-dev-user-3"),
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 3,
          selector: null,
          alias: "team-b",
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T03:00:00.000Z",
          started_at: "2026-03-20T03:00:00.000Z",
        },
      },
    } as Parameters<typeof selectPendingCredentialForFamily>[0];

    expect(
      selectPendingCredentialForFamily(
        store,
        "dev-1",
        "dev.user@gmail.com",
        "team-a",
      )?.email,
    ).toBe("dev.user+2@gmail.com");
  });

  test("still prefers the lowest suffix even if a newer pending entry was touched later", () => {
    const store = {
      version: 4,
      default_create_base_email: "dev.{n}@astronlab.com",
      families: {},
      pending: {
        "dev.user+1@gmail.com": {
          email: "dev.user+1@gmail.com",
          account_secret_ref: makeSecretRef("bw-dev-user-1"),
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 1,
          selector: null,
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T05:00:00.000Z",
          started_at: "2026-03-20T00:00:00.000Z",
        },
        "dev.user+2@gmail.com": {
          email: "dev.user+2@gmail.com",
          account_secret_ref: makeSecretRef("bw-dev-user-2"),
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 2,
          selector: null,
          alias: null,
          created_at: "2026-03-20T00:10:00.000Z",
          updated_at: "2026-03-21T00:00:00.000Z",
          started_at: "2026-03-20T00:10:00.000Z",
        },
      },
    } as Parameters<typeof selectPendingCredentialForFamily>[0];

    expect(
      selectPendingCredentialForFamily(store, "dev-1", "dev.user@gmail.com")
        ?.email,
    ).toBe("dev.user+1@gmail.com");
  });

  test("prefers the oldest pending family for a profile before switching to a newly discovered family", () => {
    const store = {
      version: 4,
      default_create_base_email: "dev.{n}@astronlab.com",
      families: {},
      pending: {
        "1.dev.astronlab+1@gmail.com": {
          email: "1.dev.astronlab+1@gmail.com",
          account_secret_ref: makeSecretRef("bw-1-dev-astronlab-1"),
          profile_name: "dev-1",
          base_email: "1.dev.astronlab@gmail.com",
          suffix: 1,
          selector: null,
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-21T00:00:00.000Z",
          started_at: "2026-03-20T00:00:00.000Z",
        },
        "arjuda.anjum+1@gmail.com": {
          email: "arjuda.anjum+1@gmail.com",
          account_secret_ref: makeSecretRef("bw-arjuda-1"),
          profile_name: "dev-1",
          base_email: "arjuda.anjum@gmail.com",
          suffix: 1,
          selector: null,
          alias: null,
          created_at: "2026-03-21T00:00:00.000Z",
          updated_at: "2026-03-21T00:00:00.000Z",
          started_at: "2026-03-21T00:00:00.000Z",
        },
      },
    } as Parameters<typeof selectPendingBaseEmailHintForProfile>[0];

    expect(selectPendingBaseEmailHintForProfile(store, "dev-1")).toBe(
      "1.dev.astronlab@gmail.com",
    );
  });

  test("prefers the higher-frontier template pending family", () => {
    const store = {
      version: 4,
      default_create_base_email: "dev.{n}@astronlab.com",
      families: {},
      pending: {
        "qa.300@astronlab.com": {
          email: "qa.300@astronlab.com",
          account_secret_ref: makeSecretRef("bw-bench-device-3"),
          profile_name: "dev-1",
          base_email: "qa.{n}@astronlab.com",
          suffix: 300,
          selector: null,
          alias: null,
          created_at: "2026-04-06T17:00:00.000Z",
          updated_at: "2026-04-06T17:00:00.000Z",
          started_at: "2026-04-06T17:00:00.000Z",
        },
        "dev.30@astronlab.com": {
          email: "dev.30@astronlab.com",
          account_secret_ref: makeSecretRef("bw-dev-30"),
          profile_name: "dev-1",
          base_email: "dev.{n}@astronlab.com",
          suffix: 30,
          selector: null,
          alias: null,
          created_at: "2026-04-06T16:00:00.000Z",
          updated_at: "2026-04-06T16:00:00.000Z",
          started_at: "2026-04-06T16:00:00.000Z",
        },
      },
    } as Parameters<typeof selectPendingBaseEmailHintForProfile>[0];

    expect(selectPendingBaseEmailHintForProfile(store, "dev-1")).toBe(
      "dev.{n}@astronlab.com",
    );
  });
});

describe("stored base-email hints", () => {
  test("prefers the most common and recent base email for a managed profile", () => {
    const store = normalizeCredentialStore({
      families: {
        "dev-1::1.dev.astronlab@gmail.com": {
          profile_name: "dev-1",
          base_email: "1.dev.astronlab@gmail.com",
          next_suffix: 4,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-21T00:00:00.000Z",
          last_created_email: "1.dev.astronlab+3@gmail.com",
        },
      },
      accounts: {
        "1.dev.astronlab+3@gmail.com": {
          email: "1.dev.astronlab+3@gmail.com",
          account_secret_ref: makeSecretRef("bw-1-dev-astronlab-3"),
          profile_name: "dev-1",
          base_email: "1.dev.astronlab@gmail.com",
          suffix: 3,
          selector: "1.dev.astronlab+3@gmail.com_free",
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-21T01:00:00.000Z",
        },
      },
      pending: {
        "1.dev.astronlab+1@gmail.com": {
          email: "1.dev.astronlab+1@gmail.com",
          account_secret_ref: makeSecretRef("bw-1-dev-astronlab-1"),
          profile_name: "dev-1",
          base_email: "1.dev.astronlab@gmail.com",
          suffix: 1,
          selector: null,
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-21T02:00:00.000Z",
          started_at: "2026-03-20T00:00:00.000Z",
        },
      },
    });

    expect(selectStoredBaseEmailHint(store, "dev-1")).toBe(
      "1.dev.astronlab@gmail.com",
    );
  });

  test("prefers the higher-frontier template family over a newer lower-frontier one", () => {
    const store = normalizeCredentialStore({
      families: {
        "dev-1::qa.{n}@astronlab.com": {
          profile_name: "dev-1",
          base_email: "qa.{n}@astronlab.com",
          next_suffix: 300,
          created_at: "2026-04-06T00:00:00.000Z",
          updated_at: "2026-04-06T17:00:00.000Z",
          last_created_email: "qa.299@astronlab.com",
        },
        "dev-1::dev.{n}@astronlab.com": {
          profile_name: "dev-1",
          base_email: "dev.{n}@astronlab.com",
          next_suffix: 30,
          created_at: "2026-04-06T00:00:00.000Z",
          updated_at: "2026-04-06T16:00:00.000Z",
          last_created_email: "dev.29@astronlab.com",
        },
      },
      pending: {},
    });

    expect(selectStoredBaseEmailHint(store, "dev-1")).toBe(
      "dev.{n}@astronlab.com",
    );
  });
});
