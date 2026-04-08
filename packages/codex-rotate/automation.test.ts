import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { shouldPromptForCodexRotateSecretUnlock } from "./automation.ts";

describe("fast-browser daemon recovery", () => {
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
console.log(typeof automation.completeCodexLoginViaWorkflowAttempt);
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
  test("blocks non-URL browser launches instead of falling back to the system browser", () => {
    const openerPath = join(
      import.meta.dir,
      "codex-login-managed-browser-opener.ts",
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
