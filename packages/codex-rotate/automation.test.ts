import { describe, expect, setDefaultTimeout, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

import {
  buildFastBrowserWorkflowError,
  shouldPromptForCodexRotateSecretUnlock,
} from "./automation.ts";

setDefaultTimeout(15_000);

describe("fast-browser daemon recovery", () => {
  test("automation module still imports when the original cwd is deleted", () => {
    const automationModuleUrl = new URL("./automation.ts", import.meta.url)
      .href;
    const result = spawnSync(
      "node",
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

describe("fast-browser workflow failures", () => {
  test("surfaces nested daemon workflow errors instead of a generic callback failure", () => {
    const error = buildFastBrowserWorkflowError(
      "workspace.web.auth-openai-com.codex-rotate-account-flow-main",
      {
        ok: false,
        result: {
          ok: false,
          status: "failed",
          error: {
            message:
              "Workflow validation failed for codex-rotate-account-flow-main.yaml",
          },
        },
      },
    );

    expect(error.message).toContain("Workflow validation failed");
    expect(error.message).not.toContain("fast-browser workflow");
  });
});

describe("active auth workflows", () => {
  test("validate against current fast-browser digest pins", async () => {
    const repoRoot = join(import.meta.dir, "..", "..");
    const workflowsModulePath = join(
      import.meta.dir,
      "..",
      "..",
      "..",
      "ai-rules",
      "skills",
      "fast-browser",
      "lib",
      "workflows.mjs",
    );
    expect(existsSync(workflowsModulePath)).toBe(true);
    const workflowPaths = [
      join(
        repoRoot,
        ".fast-browser",
        "workflows",
        "web",
        "auth.openai.com",
        "codex-rotate-account-flow-main.yaml",
      ),
      join(
        repoRoot,
        ".fast-browser",
        "workflows",
        "web",
        "auth.openai.com",
        "codex-rotate-account-flow-minimal.yaml",
      ),
      join(
        repoRoot,
        ".fast-browser",
        "workflows",
        "web",
        "auth.openai.com",
        "codex-rotate-account-flow-device-auth.yaml",
      ),
    ];

    const result = spawnSync(
      "node",
      [
        "--input-type=module",
        "-e",
        `
import { pathToFileURL } from "node:url";
const { loadWorkflowRecord, validateWorkflowRecord } = await import(${JSON.stringify(
          pathToFileURL(workflowsModulePath).href,
        )});
for (const workflowPath of ${JSON.stringify(workflowPaths)}) {
  const record = await loadWorkflowRecord(workflowPath, "project");
  await validateWorkflowRecord(record);
  console.log("ok\\t" + workflowPath);
}
`,
      ],
      {
        cwd: repoRoot,
        encoding: "utf8",
        env: {
          ...process.env,
          NODE_OPTIONS: undefined,
        },
      },
    );

    if (result.status !== 0) {
      throw new Error(
        `workflow validation failed\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`,
      );
    }
    expect(result.stdout).toContain("codex-rotate-account-flow-main.yaml");
    expect(result.stdout).toContain("codex-rotate-account-flow-minimal.yaml");
    expect(result.stdout).toContain(
      "codex-rotate-account-flow-device-auth.yaml",
    );
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
