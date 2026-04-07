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
import { join } from "node:path";

import {
  buildCodexLoginManagedBrowserWrapperPath,
  buildCodexRotateOpenAiTempProfileName,
  cleanupLegacyCodexRotateArtifacts,
  ensureCodexLoginManagedBrowserWrapper,
  getCodexRotateHome,
  getCodexLoginRetryDelayMs,
  isRetryableCodexLoginWorkflowErrorMessage,
  setCodexRotateHomeForTesting,
  shouldResetDeviceAuthSessionForRateLimit,
  shouldResetCodexLoginSessionForRetry,
  shouldPromptForCodexRotateSecretUnlock,
  shouldResetFastBrowserDaemonForSocketClose,
  shouldResetFastBrowserRuntimeForBrokenCwd,
  shouldResetFastBrowserSecretBrokerForBrokenCwd,
} from "./automation.ts";

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

  test("detects broken working-directory bitwarden failures as secret-broker resets", () => {
    expect(
      shouldResetFastBrowserSecretBrokerForBrokenCwd(
        "Command failed: bw status\nError: ENOENT: process.cwd failed with error no such file or directory, uv_cwd",
      ),
    ).toBe(true);
    expect(
      shouldResetFastBrowserSecretBrokerForBrokenCwd(
        "Bitwarden CLI is locked.",
      ),
    ).toBe(false);
  });

  test("detects broken working-directory failures as managed-runtime resets too", () => {
    expect(
      shouldResetFastBrowserRuntimeForBrokenCwd(
        "ENOENT: process.cwd failed with error no such file or directory, the current working directory was likely removed without changing the working directory, uv_cwd",
      ),
    ).toBe(true);
    expect(shouldResetFastBrowserRuntimeForBrokenCwd("other failure")).toBe(
      false,
    );
  });

  test("automation module still imports when the original cwd is deleted", () => {
    const automationModuleUrl = new URL("./automation.ts", import.meta.url)
      .href;
    const result = spawnSync(
      process.execPath,
      [
        "--experimental-strip-types",
        "--input-type=module",
        "-e",
        `
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const tempDir = mkdtempSync(join(tmpdir(), "codex-rotate-deleted-cwd-"));
process.chdir(tempDir);
rmSync(tempDir, { recursive: true, force: true });

const automation = await import(${JSON.stringify(automationModuleUrl)});
console.log(typeof automation.shouldResetFastBrowserSecretBrokerForBrokenCwd);
`,
      ],
      {
        encoding: "utf8",
      },
    );

    expect(result.status).toBe(0);
    expect(result.stdout.trim()).toBe("function");
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

  test("intercepts login through the rust managed-login entrypoint", () => {
    const fixtureRoot = mkdtempSync(join(tmpdir(), "codex-rotate-wrapper-"));
    const rotateHome = join(fixtureRoot, "rotate-home");
    const cliLogPath = join(fixtureRoot, "cli-log.json");
    const cliPath = join(fixtureRoot, "fake-codex-rotate.sh");
    const codexMarkerPath = join(fixtureRoot, "codex-invoked.txt");
    const codexPath = join(fixtureRoot, "fake-codex.sh");
    const previousRotateHome = getCodexRotateHome();

    writeFileSync(
      codexPath,
      [
        "#!/bin/sh",
        `printf 'invoked' > ${JSON.stringify(codexMarkerPath)}`,
        "exit 0",
      ].join("\n"),
      { mode: 0o700 },
    );

    writeFileSync(
      cliPath,
      [
        "#!/bin/sh",
        'cat <<EOF > "$CODEX_ROTATE_TEST_CLI_LOG"',
        '{"argv":["$1","$2"],"profile":"$FAST_BROWSER_PROFILE","realCodex":"$CODEX_ROTATE_REAL_CODEX"}',
        "EOF",
        "exit 0",
      ].join("\n"),
      { mode: 0o700 },
    );

    const previousCli = process.env.CODEX_ROTATE_CLI_BIN;
    const previousLog = process.env.CODEX_ROTATE_TEST_CLI_LOG;

    try {
      mkdirSync(rotateHome, { recursive: true });
      setCodexRotateHomeForTesting(rotateHome);
      process.env.CODEX_ROTATE_CLI_BIN = cliPath;
      process.env.CODEX_ROTATE_TEST_CLI_LOG = cliLogPath;

      const wrapperPath = ensureCodexLoginManagedBrowserWrapper(
        "managed-dev-1",
        codexPath,
      );
      const result = spawnSync(wrapperPath, ["login"], {
        encoding: "utf8",
        env: process.env,
      });

      expect(result.status).toBe(0);
      const logged = JSON.parse(readFileSync(cliLogPath, "utf8")) as {
        argv: string[];
        profile: string | null;
        realCodex: string | null;
      };
      expect(logged.profile).toBe("managed-dev-1");
      expect(logged.argv).toEqual(["internal", "managed-login"]);
      expect(logged.realCodex).toBe(codexPath);
      expect(existsSync(codexMarkerPath)).toBe(false);
    } finally {
      setCodexRotateHomeForTesting(previousRotateHome);
      if (previousCli === undefined) {
        delete process.env.CODEX_ROTATE_CLI_BIN;
      } else {
        process.env.CODEX_ROTATE_CLI_BIN = previousCli;
      }
      if (previousLog === undefined) {
        delete process.env.CODEX_ROTATE_TEST_CLI_LOG;
      } else {
        process.env.CODEX_ROTATE_TEST_CLI_LOG = previousLog;
      }
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });

  test("routes macOS open-based launches through the managed-profile opener", () => {
    const fixtureRoot = mkdtempSync(join(tmpdir(), "codex-rotate-wrapper-"));
    const rotateHome = join(fixtureRoot, "rotate-home");
    const openerLogPath = join(fixtureRoot, "opener-log.json");
    const openerPath = join(fixtureRoot, "fake-opener.mjs");
    const cliPath = join(fixtureRoot, "fake-codex-rotate.sh");
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
    writeFileSync(cliPath, ["#!/bin/sh", "exit 0"].join("\n"), {
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
    const previousCli = process.env.CODEX_ROTATE_CLI_BIN;
    const previousLog = process.env.CODEX_ROTATE_TEST_OPENER_LOG;

    try {
      mkdirSync(rotateHome, { recursive: true });
      setCodexRotateHomeForTesting(rotateHome);
      process.env.CODEX_ROTATE_BROWSER_OPENER_BIN = openerPath;
      process.env.CODEX_ROTATE_CLI_BIN = cliPath;
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
      if (previousCli === undefined) {
        delete process.env.CODEX_ROTATE_CLI_BIN;
      } else {
        process.env.CODEX_ROTATE_CLI_BIN = previousCli;
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

describe("automation bridge transport", () => {
  test("accepts request payloads through --request-file", () => {
    const fixtureRoot = mkdtempSync(join(tmpdir(), "codex-rotate-bridge-"));
    const requestPath = join(fixtureRoot, "request.json");

    writeFileSync(
      requestPath,
      JSON.stringify({
        command: "unsupported-command",
        payload: {},
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

      expect(result.status).toBe(1);
      const response = JSON.parse(result.stdout) as {
        ok: boolean;
        error?: { message?: string | null };
      };
      expect(response.ok).toBe(false);
      expect(response.error?.message).toContain(
        "Unsupported automation bridge command: unsupported-command",
      );
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
      "#!/bin/sh\nexec '/tmp/codex-rotate' internal managed-login \"$@\"\n",
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
