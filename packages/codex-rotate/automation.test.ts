import { describe, expect, setDefaultTimeout, test } from "bun:test";
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
import { chromium } from "playwright";

import {
  applyAccountPasswordFieldPath,
  applyLocatorFieldPathToSecretRef,
  buildFastBrowserSecretRefResolveSelector,
  buildFastBrowserWorkflowError,
  collectVerificationArtifactsForCleanup,
  extractFastBrowserCliResult,
  hydrateFastBrowserRunResultFromObservability,
  isFastBrowserRunResultFailure,
  isSuppressedFastBrowserEventLine,
  isUnavailableOptionalSecretLocatorError,
  resolveFastBrowserSkillPath,
  shouldResetFastBrowserBridgeInactivityTimer,
  shouldPromptForCodexRotateSecretUnlock,
} from "./automation.ts";

setDefaultTimeout(30_000);

const repoRoot = join(import.meta.dir, "..", "..");
const minimalWorkflowPath = join(
  repoRoot,
  ".fast-browser",
  "workflows",
  "web",
  "auth.openai.com",
  "codex-rotate-account-flow-minimal.yaml",
);
const originalWorkflowPath = join(
  repoRoot,
  ".fast-browser",
  "workflows",
  "web",
  "auth.openai.com",
  "codex-rotate-account-flow.yaml",
);
const stepwiseWorkflowPath = join(
  repoRoot,
  ".fast-browser",
  "workflows",
  "web",
  "auth.openai.com",
  "codex-rotate-account-flow-stepwise.yaml",
);
const deviceAuthWorkflowPath = join(
  repoRoot,
  ".fast-browser",
  "workflows",
  "web",
  "auth.openai.com",
  "codex-rotate-account-flow-device-auth.yaml",
);
const gmailCaptureWorkflowPath = join(
  repoRoot,
  "..",
  "ai-rules",
  "skills",
  "fast-browser",
  "workflows",
  "web",
  "mail.google.com",
  "capture-active-account-email.yaml",
);
const gmailVerificationWorkflowPath = join(
  repoRoot,
  "..",
  "ai-rules",
  "skills",
  "fast-browser",
  "workflows",
  "web",
  "mail.google.com",
  "collect-verification-artifact.yaml",
);
const AsyncFunction = Object.getPrototypeOf(async function () {})
  .constructor as new (
  ...args: string[]
) => (...runtimeArgs: unknown[]) => Promise<unknown>;

async function loadWorkflow(workflowPath: string) {
  const rubyScript = `
require "json"
require "yaml"
path = ARGV[0]
content = File.read(path)
begin
  data = YAML.safe_load(content, permitted_classes: [], permitted_symbols: [], aliases: false)
rescue ArgumentError
  data = YAML.safe_load(content)
end
puts JSON.generate(data)
`;
  const parsed = spawnSync("ruby", ["-e", rubyScript, workflowPath], {
    encoding: "utf8",
  });
  if (parsed.status !== 0) {
    throw new Error(
      `Failed to parse workflow YAML at ${workflowPath}\nstdout:\n${parsed.stdout}\nstderr:\n${parsed.stderr}`,
    );
  }
  return JSON.parse(parsed.stdout) as {
    use?: {
      functions?: Record<
        string,
        {
          with?: {
            body?: {
              script?: string;
            };
          };
        }
      >;
    };
    do?: Array<Record<string, unknown>>;
  };
}

async function loadMinimalWorkflow() {
  return loadWorkflow(minimalWorkflowPath);
}

function findWorkflowStep<T = Record<string, unknown>>(
  workflow: Awaited<ReturnType<typeof loadWorkflow>>,
  stepId: string,
): T | undefined {
  return workflow.do?.find((entry) => stepId in entry)?.[stepId] as
    | T
    | undefined;
}

async function loadMinimalAboutYouHelperScript() {
  const workflow = await loadMinimalWorkflow();
  const script =
    workflow.use?.functions?.fill_openai_about_you_form?.with?.body?.script;
  if (!script) {
    throw new Error("fill_openai_about_you_form script was not found");
  }
  return script;
}

async function loadOriginalAboutYouHelperScript() {
  const workflow = await loadWorkflow(originalWorkflowPath);
  const script =
    workflow.use?.functions?.fill_openai_about_you_form?.with?.body?.script;
  if (!script) {
    throw new Error("original fill_openai_about_you_form script was not found");
  }
  return script;
}

async function loadMinimalStepScript(stepId: string) {
  const workflow = await loadWorkflow(minimalWorkflowPath);
  const step = workflow.do?.find((entry) => stepId in entry)?.[stepId] as
    | {
        metadata?: {
          templateRef?: string;
        };
        run?: {
          script?: {
            code?: string;
          };
        };
        with?: {
          body?: {
            script?: string;
          };
        };
      }
    | undefined;
  const script =
    step?.run?.script?.code ||
    step?.with?.body?.script ||
    (step?.metadata?.templateRef
      ? workflow.use?.functions?.[step.metadata.templateRef]?.with?.body?.script
      : undefined);
  if (!script) {
    throw new Error(`${stepId} script was not found`);
  }
  return script;
}

async function loadWorkflowStepScript(workflowPath: string, stepId: string) {
  const workflow = await loadWorkflow(workflowPath);
  const step = workflow.do?.find((entry) => stepId in entry)?.[stepId] as
    | {
        metadata?: {
          templateRef?: string;
        };
        run?: {
          script?: {
            code?: string;
          };
        };
        with?: {
          body?: {
            script?: string;
          };
        };
      }
    | undefined;
  const script =
    step?.run?.script?.code ||
    step?.with?.body?.script ||
    (step?.metadata?.templateRef
      ? workflow.use?.functions?.[step.metadata.templateRef]?.with?.body?.script
      : undefined);
  if (!script) {
    throw new Error(`${stepId} script was not found in ${workflowPath}`);
  }
  return script;
}

async function loadWorkflowFunctionScript(
  workflowPath: string,
  functionId: string,
) {
  const workflow = await loadWorkflow(workflowPath);
  const script = workflow.use?.functions?.[functionId]?.with?.body?.script;
  if (!script) {
    throw new Error(`${functionId} script was not found in ${workflowPath}`);
  }
  return script;
}

async function loadOriginalStepScript(stepId: string) {
  const workflow = await loadWorkflow(originalWorkflowPath);
  const step = workflow.do?.find((entry) => stepId in entry)?.[stepId] as
    | {
        metadata?: {
          templateRef?: string;
        };
        with?: {
          body?: {
            script?: string;
          };
        };
      }
    | undefined;
  const script =
    step?.with?.body?.script ||
    (step?.metadata?.templateRef
      ? workflow.use?.functions?.[step.metadata.templateRef]?.with?.body?.script
      : undefined);
  if (!script) {
    throw new Error(`${stepId} script was not found in original workflow`);
  }
  return script;
}

async function runMinimalAboutYouHelper(
  html: string,
  args: Record<string, string>,
) {
  const browser = await chromium.launch({ headless: true, channel: "chrome" });
  try {
    const page = await browser.newPage();
    await page.setContent(html);
    const script = await loadMinimalAboutYouHelperScript();
    const execute = new AsyncFunction("page", "args", script);
    return (await execute(page, args)) as {
      ok: boolean;
      name_value?: string | null;
      month_value?: string | null;
      day_value?: string | null;
      year_value?: string | null;
      birthday_value?: string | null;
      age_value?: string | null;
      submit_disabled?: boolean | null;
      submit_text?: string | null;
    };
  } finally {
    await browser.close();
  }
}

async function runOriginalAboutYouHelper(
  html: string,
  args: Record<string, string>,
) {
  const browser = await chromium.launch({ headless: true, channel: "chrome" });
  try {
    const page = await browser.newPage();
    await page.setContent(html);
    const script = await loadOriginalAboutYouHelperScript();
    const execute = new AsyncFunction("page", "args", script);
    return (await execute(page, args)) as {
      ok: boolean;
      name_value?: string | null;
      month_value?: string | null;
      day_value?: string | null;
      year_value?: string | null;
      birthday_value?: string | null;
      age_value?: string | null;
      submit_disabled?: boolean | null;
      submit_text?: string | null;
    };
  } finally {
    await browser.close();
  }
}

async function runDeviceAuthAboutYouHelper(
  html: string,
  args: Record<string, string>,
  url = "about:blank",
) {
  const browser = await chromium.launch({ headless: true, channel: "chrome" });
  try {
    const page = await browser.newPage();
    await page.route("**/*", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "text/html",
        body: "<html><body></body></html>",
      });
    });
    await page.goto(url);
    await page.setContent(html);
    const script = await loadWorkflowStepScript(
      deviceAuthWorkflowPath,
      "fill_prepare_signup_about_you",
    );
    const execute = new AsyncFunction("page", "state", "args", script);
    return (await execute(page, {}, args)) as Record<string, unknown>;
  } finally {
    await browser.close();
  }
}

async function runMinimalStepScript(
  stepId: string,
  html: string,
  state: Record<string, unknown>,
) {
  const browser = await chromium.launch({ headless: true, channel: "chrome" });
  try {
    const page = await browser.newPage();
    await page.setContent(html);
    const script = await loadMinimalStepScript(stepId);
    const execute = new AsyncFunction("page", "state", script);
    return (await execute(page, state)) as Record<string, unknown>;
  } finally {
    await browser.close();
  }
}

async function runOriginalStepScript(
  stepId: string,
  html: string,
  state: Record<string, unknown>,
) {
  const browser = await chromium.launch({ headless: true, channel: "chrome" });
  try {
    const page = await browser.newPage();
    await page.setContent(html);
    const script = await loadOriginalStepScript(stepId);
    const execute = new AsyncFunction("page", "state", script);
    return (await execute(page, state)) as Record<string, unknown>;
  } finally {
    await browser.close();
  }
}

async function runWorkflowStepScript(
  workflowPath: string,
  stepId: string,
  html: string,
  state: Record<string, unknown>,
  args: Record<string, unknown> = {},
  url = "about:blank",
) {
  const browser = await chromium.launch({ headless: true, channel: "chrome" });
  try {
    const page = await browser.newPage();
    await page.route("**/*", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "text/html",
        body: html,
      });
    });
    await page.goto(url);
    const script = await loadWorkflowStepScript(workflowPath, stepId);
    const execute = new AsyncFunction("page", "state", "args", script);
    return (await execute(page, state, args)) as Record<string, unknown>;
  } finally {
    await browser.close();
  }
}

async function runWorkflowStepScriptOnContent(
  workflowPath: string,
  stepId: string,
  html: string,
  state: Record<string, unknown>,
  args: Record<string, unknown> = {},
) {
  const browser = await chromium.launch({ headless: true, channel: "chrome" });
  try {
    const page = await browser.newPage();
    await page.setContent(html);
    const script = await loadWorkflowStepScript(workflowPath, stepId);
    const execute = new AsyncFunction("page", "state", "args", script);
    return (await execute(page, state, args)) as Record<string, unknown>;
  } finally {
    await browser.close();
  }
}

async function runWorkflowFunctionScriptOnContent(
  workflowPath: string,
  functionId: string,
  html: string,
  args: Record<string, unknown> = {},
  url = "https://auth.openai.com/create-account",
) {
  const browser = await chromium.launch({ headless: true, channel: "chrome" });
  try {
    const page = await browser.newPage();
    await page.goto(url);
    await page.setContent(html);
    const script = await loadWorkflowFunctionScript(workflowPath, functionId);
    const execute = new AsyncFunction("page", "args", script);
    return (await execute(page, args)) as Record<string, unknown>;
  } finally {
    await browser.close();
  }
}

async function runWorkflowFunctionScriptWithStub(
  workflowPath: string,
  functionId: string,
  page: Record<string, unknown>,
  state: Record<string, unknown> = {},
  args: Record<string, unknown> = {},
) {
  const script = await loadWorkflowFunctionScript(workflowPath, functionId);
  const execute = new AsyncFunction("page", "state", "args", script);
  return (await execute(page, state, args)) as Record<string, unknown>;
}

async function runWorkflowRunScript(
  workflowPath: string,
  stepId: string,
  args: Record<string, unknown> = {},
  state: Record<string, unknown> = {},
  env: Record<string, unknown> = {},
) {
  const script = await loadWorkflowStepScript(workflowPath, stepId);
  const execute = new AsyncFunction(
    "args",
    "state",
    "env",
    "process",
    "console",
    script,
  );
  return (await execute(args, state, env, process, console)) as Record<
    string,
    unknown
  >;
}

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

  test("resolves fast-browser skill files from the main worktree when the current worktree has no ai-rules sibling", () => {
    const fixtureRoot = mkdtempSync(join(tmpdir(), "codex-rotate-worktree-"));
    const worktreeRepoRoot = join(fixtureRoot, "worktrees", "e7ac", "ai-tools");
    const mainRepoRoot = join(fixtureRoot, "ai-tools");
    const mainSkillPath = join(
      fixtureRoot,
      "ai-rules",
      "skills",
      "fast-browser",
      "lib",
      "daemon",
      "client.mjs",
    );

    mkdirSync(
      join(fixtureRoot, "ai-rules", "skills", "fast-browser", "lib", "daemon"),
      {
        recursive: true,
      },
    );
    writeFileSync(mainSkillPath, "export {}");

    const resolved = resolveFastBrowserSkillPath(
      ["lib", "daemon", "client.mjs"],
      worktreeRepoRoot,
      mainRepoRoot,
    );

    expect(resolved).toBe(mainSkillPath);
  });

  test("respects CODEX_ROTATE_REPO_ROOT when resolving the automation repo root", () => {
    const automationModuleUrl = new URL("./automation.ts", import.meta.url)
      .href;
    const repoRoot = "/tmp/codex-rotate-worktree-root";
    const result = spawnSync(
      "node",
      [
        "--experimental-strip-types",
        "--input-type=module",
        "-e",
        `
import { resolveCodexRotateRepoRoot } from ${JSON.stringify(automationModuleUrl)};
console.log(resolveCodexRotateRepoRoot());
`,
      ],
      {
        encoding: "utf8",
        env: {
          ...process.env,
          CODEX_ROTATE_REPO_ROOT: repoRoot,
        },
      },
    );

    expect(result.status).toBe(0);
    expect(result.stdout.trim()).toBe(repoRoot);
  });
});

describe("optional secret locator fallback", () => {
  test("uses login selector lookup when resolving a secret ref from a login locator", () => {
    expect(
      buildFastBrowserSecretRefResolveSelector({
        kind: "login_lookup",
        store: "bitwarden-cli",
        username: "dev3astronlab+5@gmail.com",
        uris: ["https://auth.openai.com", "https://chatgpt.com"],
      }),
    ).toEqual({
      kind: "login",
      store: "bitwarden-cli",
      username: "dev3astronlab+5@gmail.com",
      uris: ["https://auth.openai.com", "https://chatgpt.com"],
    });
  });

  test("carries locator field_path onto a resolved secret ref when the CLI ref omits it", () => {
    expect(
      applyLocatorFieldPathToSecretRef(
        {
          type: "secret_ref",
          store: "bitwarden-cli",
          object_id: "abc123",
          field_path: null,
          version: null,
        },
        {
          kind: "login_lookup",
          store: "bitwarden-cli",
          username: "dev3astronlab+5@gmail.com",
          uris: ["https://auth.openai.com", "https://chatgpt.com"],
          field_path: "/password",
        },
      ),
    ).toEqual({
      type: "secret_ref",
      store: "bitwarden-cli",
      object_id: "abc123",
      field_path: "/password",
      version: null,
    });
  });

  test("defaults OpenAI account secret refs to the /password field path", () => {
    expect(
      applyAccountPasswordFieldPath({
        type: "secret_ref",
        store: "bitwarden-cli",
        object_id: "abc123",
        field_path: null,
        version: null,
      }),
    ).toEqual({
      type: "secret_ref",
      store: "bitwarden-cli",
      object_id: "abc123",
      field_path: "/password",
      version: null,
    });
  });

  test("extracts result payloads from public fast-browser CLI envelopes", () => {
    expect(
      extractFastBrowserCliResult({
        abiVersion: "1.0.0",
        command: "secrets.item",
        ok: true,
        result: {
          ref: {
            type: "secret-ref",
            store: "bitwarden-cli",
            objectId: "abc123",
          },
        },
      }),
    ).toEqual({
      ref: {
        type: "secret-ref",
        store: "bitwarden-cli",
        objectId: "abc123",
      },
    });
    expect(
      extractFastBrowserCliResult({
        abiVersion: "1.0.0",
        command: "secrets.item",
        ok: false,
        error: {
          code: "vault-locked",
          message: "Bitwarden CLI is locked.",
        },
      }),
    ).toBeUndefined();
  });

  test("treats locked or stalled Bitwarden preflight as optional", () => {
    expect(
      isUnavailableOptionalSecretLocatorError(
        new Error(
          "Bitwarden CLI is locked. Re-run this command interactively.",
        ),
      ),
    ).toBe(true);
    expect(
      isUnavailableOptionalSecretLocatorError(
        new Error(
          "Bitwarden CLI timed out while trying to read Bitwarden CLI status.",
        ),
      ),
    ).toBe(true);
    expect(
      isUnavailableOptionalSecretLocatorError(
        new Error("No Bitwarden item matched the exact name."),
      ),
    ).toBe(false);
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
      const response = JSON.parse(
        result.stdout.replace(/^__CODEX_ROTATE_BRIDGE__/u, ""),
      ) as {
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

  test("flushes large bridge responses before exiting", () => {
    const fixtureRoot = mkdtempSync(join(tmpdir(), "codex-rotate-bridge-big-"));
    const requestPath = join(fixtureRoot, "request.json");
    const oversizedCommand = `unsupported-command-${"x".repeat(250_000)}`;

    writeFileSync(
      requestPath,
      JSON.stringify({
        command: oversizedCommand,
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
          maxBuffer: 8 * 1024 * 1024,
        },
      );

      expect(result.status).toBe(1);
      expect(result.stdout.startsWith("__CODEX_ROTATE_BRIDGE__")).toBe(true);

      const response = JSON.parse(
        result.stdout.replace(/^__CODEX_ROTATE_BRIDGE__/u, ""),
      ) as {
        ok: boolean;
        error?: { message?: string | null };
      };

      expect(response.ok).toBe(false);
      expect(response.error?.message?.length ?? 0).toBeGreaterThan(200_000);
      expect(response.error?.message).toContain(
        "Unsupported automation bridge command: unsupported-command-",
      );
    } finally {
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });
});

describe("fast-browser daemon event visibility", () => {
  test("suppresses only daemon heartbeats", () => {
    expect(
      isSuppressedFastBrowserEventLine(
        '__FAST_BROWSER_EVENT__{"phase":"daemon","status":"heartbeat","message":"still alive"}',
      ),
    ).toBe(true);
    expect(
      isSuppressedFastBrowserEventLine(
        '__FAST_BROWSER_EVENT__{"phase":"daemon","status":"queued","message":"waiting"}',
      ),
    ).toBe(false);
    expect(
      isSuppressedFastBrowserEventLine(
        '__FAST_BROWSER_EVENT__{"phase":"daemon","status":"running","message":"starting"}',
      ),
    ).toBe(false);
  });

  test("treats running daemon heartbeats as liveness but not queued events", () => {
    expect(
      shouldResetFastBrowserBridgeInactivityTimer(
        '__FAST_BROWSER_EVENT__{"phase":"daemon","status":"heartbeat","message":"still alive"}',
      ),
    ).toBe(true);
    expect(
      shouldResetFastBrowserBridgeInactivityTimer(
        '__FAST_BROWSER_EVENT__{"phase":"daemon","status":"queued","message":"waiting"}',
      ),
    ).toBe(false);
    expect(
      shouldResetFastBrowserBridgeInactivityTimer(
        '__FAST_BROWSER_EVENT__{"phase":"daemon","status":"running","message":"starting"}',
      ),
    ).toBe(true);
    expect(
      shouldResetFastBrowserBridgeInactivityTimer(
        "plain stderr line from the automation bridge",
      ),
    ).toBe(true);
  });

  test("rejects reset-managed-runtime bridge commands", () => {
    const fixtureRoot = mkdtempSync(
      join(tmpdir(), "codex-rotate-bridge-reset-"),
    );
    const requestPath = join(fixtureRoot, "request.json");

    writeFileSync(
      requestPath,
      JSON.stringify({
        command: "reset-managed-runtime",
        payload: { profileName: "dev-1" },
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
      const response = JSON.parse(
        result.stdout.replace(/^__CODEX_ROTATE_BRIDGE__/u, ""),
      ) as {
        ok: boolean;
        error?: { message?: string | null };
      };
      expect(response.ok).toBe(false);
      expect(response.error?.message).toContain(
        "Unsupported automation bridge command: reset-managed-runtime",
      );
    } finally {
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });
});

describe("fast-browser workflow failures", () => {
  test("treats daemon results with failed status as workflow failures", () => {
    expect(
      isFastBrowserRunResultFailure({
        ok: true,
        status: "failed",
      }),
    ).toBe(true);
    expect(
      isFastBrowserRunResultFailure({
        ok: true,
        status: "completed",
        error: {
          message:
            "login-verification-code-rejected:https://auth.openai.com/email-verification",
        },
      }),
    ).toBe(true);
    expect(
      isFastBrowserRunResultFailure({
        ok: true,
        status: "completed",
      }),
    ).toBe(false);
  });

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

  test("hydrates snake_case final_url from the run artifact when daemon result omits finalUrl", () => {
    const fixtureRoot = mkdtempSync(
      join(tmpdir(), "codex-rotate-run-hydrate-"),
    );
    const runPath = join(fixtureRoot, "run.json");
    try {
      writeFileSync(
        runPath,
        JSON.stringify({
          final_url: "http://localhost:1455/success",
          page: {
            url: "http://localhost:1455/success",
            title: "Signed in to Codex",
            text: "Signed in to Codex\nYou may now close this page",
          },
        }),
      );

      const result = hydrateFastBrowserRunResultFromObservability({
        ok: true,
        status: "completed",
        finalUrl: null,
        observability: {
          runPath,
        },
      });

      expect(result.finalUrl).toBe("http://localhost:1455/success");
      expect(result.page).toEqual({
        url: "http://localhost:1455/success",
        title: "Signed in to Codex",
        text: "Signed in to Codex\nYou may now close this page",
      });
    } finally {
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });

  test("hydrates from snake_case observability paths emitted by the fast-browser daemon", () => {
    const fixtureRoot = mkdtempSync(
      join(tmpdir(), "codex-rotate-run-hydrate-snake-observability-"),
    );
    const runPath = join(fixtureRoot, "run.json");
    try {
      writeFileSync(
        runPath,
        JSON.stringify({
          final_url: "http://localhost:1455/success",
          page: {
            url: "http://localhost:1455/success",
            title: "Signed in to Codex",
            text: "Signed in to Codex\nYou may now close this page",
          },
        }),
      );

      const result = hydrateFastBrowserRunResultFromObservability({
        ok: true,
        status: "completed",
        finalUrl: null,
        observability: {
          run_path: runPath,
          status_path: runPath,
        },
      });

      expect(result.finalUrl).toBe("http://localhost:1455/success");
      expect(result.page).toEqual({
        url: "http://localhost:1455/success",
        title: "Signed in to Codex",
        text: "Signed in to Codex\nYou may now close this page",
      });
    } finally {
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });

  test("hydrates recent_events from the run artifact when the daemon result omits them", () => {
    const fixtureRoot = mkdtempSync(
      join(tmpdir(), "codex-rotate-run-hydrate-recent-events-"),
    );
    const runPath = join(fixtureRoot, "run.json");
    try {
      writeFileSync(
        runPath,
        JSON.stringify({
          final_url: "https://auth.openai.com/add-phone",
          recent_events: [
            {
              step_id: "finalize_flow_summary",
              phase: "action",
              status: "ok",
              details: {
                result: {
                  value: {
                    stage: "add_phone",
                    next_action: "skip_account",
                    replay_reason: "add_phone",
                  },
                },
              },
            },
          ],
        }),
      );

      const result = hydrateFastBrowserRunResultFromObservability({
        ok: true,
        status: "completed",
        finalUrl: null,
        observability: {
          runPath,
        },
      });

      expect(result.finalUrl).toBe("https://auth.openai.com/add-phone");
      expect(result.recent_events).toEqual([
        {
          step_id: "finalize_flow_summary",
          phase: "action",
          status: "ok",
          details: {
            result: {
              value: {
                stage: "add_phone",
                next_action: "skip_account",
                replay_reason: "add_phone",
              },
            },
          },
        },
      ]);
    } finally {
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });
});

describe("active auth workflows", () => {
  test("stepwise signup email submit treats /log-in/password as signup_password", async () => {
    const result = await runWorkflowFunctionScriptOnContent(
      stepwiseWorkflowPath,
      "submit_signup_email_form",
      `
        <form>
          <input type="email" value="devbench.23@astronlab.com" />
          <button
            type="button"
            onclick="
              history.replaceState({}, '', '/log-in/password');
              document.title = 'Password';
              document.body.innerHTML = '<main><h1>Password</h1><input type=\\'password\\' name=\\'password\\' autocomplete=\\'current-password\\'></main>';
            "
          >
            Continue
          </button>
        </form>
      `,
      { email: "devbench.23@astronlab.com" },
    );

    expect(result.ok).toBe(true);
    expect(result.next_stage).toBe("signup_password");
    expect(String(result.current_url || "")).toContain("/log-in/password");
  });

  test("all non-device flows normalize signup email handoff through /log-in/password", async () => {
    for (const workflowPath of [minimalWorkflowPath, stepwiseWorkflowPath]) {
      const script = await loadWorkflowFunctionScript(
        workflowPath,
        "submit_signup_email_form",
      );
      expect(script).toContain("/log-in/password");
      expect(script).toContain("normalizedStage");
      expect(script).toContain("signup_password");
      expect(() => new AsyncFunction("page", "args", script)).not.toThrow();
    }
  });

  test("minimal and stepwise accept the create-branch /log-in/password handoff as signup password entry", () => {
    const minimalText = readFileSync(minimalWorkflowPath, "utf8");
    const stepwiseText = readFileSync(stepwiseWorkflowPath, "utf8");

    for (const workflowText of [minimalText, stepwiseText]) {
      expect(workflowText).toContain(
        "stage === 'signup_password' || state.steps.classify_after_signup_email_gate?.action?.stage === 'login_password'",
      );
      expect(workflowText).toContain('input[name="password"]');
      expect(workflowText).toContain('autocomplete="current-password"');
      expect(workflowText).toContain("log in|sign in");
    }
  });

  test("all non-device flows arm consent click when the final surface is already oauth_consent", async () => {
    for (const workflowPath of [minimalWorkflowPath, stepwiseWorkflowPath]) {
      const workflow = await loadWorkflow(workflowPath);
      const acceptStep = workflow.do?.find(
        (entry) => "accept_oauth_consent" in entry,
      )?.accept_oauth_consent as
        | {
            if?: string;
          }
        | undefined;

      expect(acceptStep?.if).toContain("oauth_continue_visible === true");
      expect(acceptStep?.if).toContain("stage === 'oauth_consent'");
      expect(acceptStep?.if).toContain("sign-in-with-chatgpt");
      expect(acceptStep?.if).toContain("callback_complete !== true");
    }
  });

  test("all non-device flows classify the direct consent path after the consent click", async () => {
    for (const workflowPath of [
      originalWorkflowPath,
      minimalWorkflowPath,
      stepwiseWorkflowPath,
    ]) {
      const workflow = await loadWorkflow(workflowPath);
      const classifyAfterConsent = workflow.do?.find(
        (entry) => "classify_after_oauth_consent" in entry,
      )?.classify_after_oauth_consent as
        | {
            if?: string;
          }
        | undefined;
      const completeLoginOrConsent = workflow.do?.find(
        (entry) => "complete_login_or_consent" in entry,
      )?.complete_login_or_consent as
        | {
            if?: string;
          }
        | undefined;

      expect(classifyAfterConsent?.if).toContain(
        "state.steps.accept_oauth_consent?.action?.ok === true",
      );
      expect(classifyAfterConsent?.if).toContain("sign-in-with-chatgpt");
      expect(completeLoginOrConsent?.if).toContain(
        "state.steps.classify_after_oauth_consent?.action?.current_url",
      );
    }
  });

  test("minimal no longer detours back through Gmail after OTP collection", async () => {
    const workflow = await loadWorkflow(minimalWorkflowPath);
    const returnAfterDelete = workflow.do?.find(
      (entry) =>
        "return_to_login_after_delete_login_verification_artifact" in entry,
    );
    const returnAfterRetryDelete = workflow.do?.find(
      (entry) =>
        "return_to_login_after_delete_login_verification_artifact_after_submit_failure" in
        entry,
    );

    expect(returnAfterDelete).toBeUndefined();
    expect(returnAfterRetryDelete).toBeUndefined();
  });

  test("minimal tracks the replayed-login OTP success path from the OTP submit step itself", async () => {
    const workflow = await loadWorkflow(minimalWorkflowPath);
    const classifyAfterVerification = workflow.do?.find(
      (entry) => "classify_after_login_verification" in entry,
    )?.classify_after_login_verification as
      | {
          if?: string;
        }
      | undefined;
    const classifyAfterVerificationGate = workflow.do?.find(
      (entry) => "classify_after_login_verification_gate" in entry,
    )?.classify_after_login_verification_gate as
      | {
          if?: string;
        }
      | undefined;
    const resendAfterSubmitFailure = workflow.do?.find(
      (entry) =>
        "resend_login_verification_email_after_submit_failure" in entry,
    )?.resend_login_verification_email_after_submit_failure as
      | {
          if?: string;
        }
      | undefined;
    const acceptConsent = workflow.do?.find(
      (entry) => "accept_oauth_consent" in entry,
    )?.accept_oauth_consent as
      | {
          metadata?: { templateRef?: string };
        }
      | undefined;

    expect(classifyAfterVerification?.if).toBe(
      "${state.steps.submit_login_verification_code?.action?.ok === true}",
    );
    expect(classifyAfterVerificationGate?.if).toBe(
      "${state.steps.submit_login_verification_code?.action?.ok === true}",
    );
    expect(resendAfterSubmitFailure?.if).toContain(
      "state.steps.submit_login_verification_code?.action?.stage === 'email_verification'",
    );
    expect(acceptConsent?.metadata?.templateRef).toBe(
      "click_oauth_consent_continue",
    );
  });

  test("validate against current fast-browser digest pins", async () => {
    const repoRoot = join(import.meta.dir, "..", "..");
    const fastBrowserScript = resolveFastBrowserSkillPath([
      "scripts",
      "fast-browser.mjs",
    ]);
    expect(existsSync(fastBrowserScript)).toBe(true);
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
        "codex-rotate-account-flow-stepwise.yaml",
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

    for (const workflowPath of workflowPaths) {
      const result = spawnSync(
        "node",
        [fastBrowserScript, "workflows", "validate", workflowPath],
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
          `workflow validation failed for ${workflowPath}\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`,
        );
      }
      expect(result.stdout).toContain('"ok": true');
      expect(result.stdout).toContain('"command": "workflows.validate"');
      expect(result.stdout).toContain(workflowPath);
    }
  });

  test("runtime imports only the public fast-browser script boundary", () => {
    const automationSource = readFileSync(
      join(import.meta.dir, "automation.ts"),
      "utf8",
    );
    expect(automationSource).toContain("resolveFastBrowserSkillPath([");
    expect(automationSource).toContain('"fast-browser.mjs"');
    expect(automationSource).not.toContain("lib/daemon/client.mjs");
    expect(automationSource).not.toContain(
      "lib/secret-adapters/bitwarden-session.mjs",
    );
    expect(automationSource).not.toContain("workflows.mjs");
  });

  test("runtime uses only canonical fast-browser CLI command names", () => {
    const runtimeSources = [
      readFileSync(join(import.meta.dir, "automation.ts"), "utf8"),
      readFileSync(
        join(
          import.meta.dir,
          "crates",
          "codex-rotate-core",
          "src",
          "workflow.rs",
        ),
        "utf8",
      ),
      readFileSync(
        join(import.meta.dir, "crates", "codex-rotate-cli", "src", "main.rs"),
        "utf8",
      ),
    ].join("\n");

    expect(runtimeSources).not.toContain('"inspect-profiles"');
    expect(runtimeSources).not.toContain('"validate-global-workflows"');
    expect(runtimeSources).not.toContain('"debug-snapshot"');
    expect(runtimeSources).not.toContain('"secrets unlock"');
    expect(runtimeSources).not.toContain('"secrets status"');
    expect(runtimeSources).not.toContain('"secrets clear"');
  });

  test("main remains ordered stepwise-first with device-auth fallback after benchmarking", async () => {
    const workflow = await loadWorkflow(
      join(
        repoRoot,
        ".fast-browser",
        "workflows",
        "web",
        "auth.openai.com",
        "codex-rotate-account-flow-main.yaml",
      ),
    );

    const calls = (workflow.do || [])
      .flatMap((entry) =>
        Object.values(entry).map((step) => {
          const record = step as { call?: string; with?: { version?: string } };
          return { call: record.call, version: record.with?.version };
        }),
      )
      .filter((step) => typeof step.call === "string");

    expect(calls).toEqual([
      {
        call: "workflow.workspace.web.auth-openai-com.codex-rotate-account-flow-stepwise",
        version: "1.1.0",
      },
      {
        call: "workflow.workspace.web.auth-openai-com.codex-rotate-account-flow-device-auth",
        version: "1.3.13",
      },
    ]);
  });

  test("main forwards account_login_ref into both primary and fallback flows", async () => {
    const workflow = await loadWorkflow(
      join(
        repoRoot,
        ".fast-browser",
        "workflows",
        "web",
        "auth.openai.com",
        "codex-rotate-account-flow-main.yaml",
      ),
    );

    const runPrimary = findWorkflowStep<{
      with?: {
        input?: Record<string, unknown>;
      };
    }>(workflow, "run_primary_non_device_flow");
    const runFallback = findWorkflowStep<{
      with?: {
        input?: Record<string, unknown>;
      };
    }>(workflow, "run_device_auth_fallback");

    expect(runPrimary?.with?.input?.account_login_ref).toBe(
      "${inputs.account_login_ref}",
    );
    expect(runFallback?.with?.input?.account_login_ref).toBe(
      "${inputs.account_login_ref}",
    );
  });

  test("main keeps primary workflow-owned retry and skip outcomes on the stepwise path", () => {
    const workflowText = readFileSync(
      join(
        repoRoot,
        ".fast-browser",
        "workflows",
        "web",
        "auth.openai.com",
        "codex-rotate-account-flow-main.yaml",
      ),
      "utf8",
    );

    expect(workflowText).toContain("output?.next_action != 'skip_account'");
    expect(workflowText).toContain(
      "(state.steps.run_primary_non_device_flow?.action?.result?.output?.next_action != 'replay_auth_url' || state.steps.run_primary_non_device_flow?.action?.result?.output?.replay_reason == 'about_you')",
    );
    expect(workflowText).toContain("output?.next_action != 'retry_attempt'");
    expect(workflowText).toContain("const primarySkip =");
    expect(workflowText).toContain("const primarySetupBlocked =");
    expect(workflowText).toContain("const primaryRetryable =");
    expect(workflowText).toContain(
      "primaryComplete || primarySkip || primaryRetryable || !fallback",
    );
    expect(workflowText).toContain("!primarySkip");
    expect(workflowText).toContain("!primaryRetryable");
  });

  test("minimal flow waits after replayed-login resend before searching Gmail", async () => {
    const workflow = await loadWorkflow(minimalWorkflowPath);
    const stepIds = (workflow.do || []).flatMap((entry) => Object.keys(entry));
    const workflowText = readFileSync(minimalWorkflowPath, "utf8");

    expect(stepIds).toEqual(
      expect.arrayContaining([
        "resend_login_verification_email",
        "wait_after_resend_login_verification_email",
        "collect_login_verification_artifact",
      ]),
    );
    expect(workflowText).toContain(
      "wait_after_resend_login_verification_email",
    );
    expect(workflowText).toContain(
      "state.steps.resend_login_verification_email?.action?.skipped !== true",
    );
    expect(workflowText).toContain("waited_ms: 10000");
  });

  test("non-device flows treat direct replayed-login about-you as a first-class continuation", () => {
    for (const workflowPath of [
      originalWorkflowPath,
      minimalWorkflowPath,
      stepwiseWorkflowPath,
    ]) {
      const workflowText = readFileSync(workflowPath, "utf8");
      expect(workflowText).toContain(
        "state.steps.classify_after_login_password_gate?.action?.follow_up_step === true",
      );
    }
  });

  test("original flow keeps a two-step final add-phone reentry ladder", async () => {
    const workflow = await loadWorkflow(originalWorkflowPath);
    const stepIds = (workflow.do || []).flatMap((entry) => Object.keys(entry));
    const workflowText = readFileSync(originalWorkflowPath, "utf8");

    expect(stepIds).toEqual(
      expect.arrayContaining([
        "wait_before_consent_add_phone_retry_1",
        "reopen_auth_url_before_consent_retry_1",
        "classify_before_consent_retry_1",
        "wait_before_consent_add_phone_retry_2",
        "reopen_auth_url_before_consent_retry_2",
        "classify_before_consent_retry_2",
        "cache_effective_before_consent_surface",
      ]),
    );
    expect(workflowText).toContain(
      "wait_before_consent_add_phone_retry_1?.action?.skipped !== true",
    );
    expect(workflowText).toContain(
      "wait_before_consent_add_phone_retry_2?.action?.skipped !== true",
    );
    expect(workflowText).not.toContain("wait_before_consent_add_phone_retry_3");
  });

  test("all non-device flows clear OpenAI and ChatGPT auth state before detached codex login starts", async () => {
    for (const workflowPath of [
      originalWorkflowPath,
      minimalWorkflowPath,
      stepwiseWorkflowPath,
    ]) {
      const workflow = await loadWorkflow(workflowPath);
      const resetStep = findWorkflowStep<{
        call?: string;
        with?: {
          body?: {
            url?: string;
          };
        };
        metadata?: {
          browser?: {
            clearSiteDataForOrigins?: string[];
          };
        };
      }>(workflow, "reset_openai_auth_state_before_codex_login");
      const startStep = findWorkflowStep<{
        if?: string;
      }>(workflow, "start_codex_login_session");
      const openStep = findWorkflowStep<{
        metadata?: {
          browser?: {
            clearSiteDataForOrigins?: string[];
          };
        };
      }>(workflow, "open_codex_login_entry");

      expect(resetStep?.call).toBe("afn.driver.browser.navigate");
      expect(resetStep?.with?.body?.url).toBe("about:blank");
      expect(resetStep?.metadata?.browser?.clearSiteDataForOrigins).toEqual(
        expect.arrayContaining([
          "https://auth.openai.com",
          "https://chatgpt.com",
          "https://chat.openai.com",
        ]),
      );
      expect(startStep?.if).toContain(
        "state.steps.reset_openai_auth_state_before_codex_login?.action?.ok === true",
      );
      expect(startStep?.if).toContain(
        "state.steps.reset_openai_auth_state_before_codex_login?.action?.skipped !== true",
      );
      expect(
        openStep?.metadata?.browser?.clearSiteDataForOrigins,
      ).toBeUndefined();
    }
  });

  test("all non-device flows reclear auth state before each bounded final add-phone replay", async () => {
    for (const workflowPath of [
      originalWorkflowPath,
      minimalWorkflowPath,
      stepwiseWorkflowPath,
    ]) {
      const workflow = await loadWorkflow(workflowPath);
      const workflowText = readFileSync(workflowPath, "utf8");
      const waitStepIds = Array.from(
        workflowText.matchAll(/wait_before_consent_add_phone_retry_(\d+)/g),
        (match) => match[0],
      );

      expect(new Set(waitStepIds).size).toBe(2);

      for (let attempt = 1; attempt <= 2; attempt += 1) {
        const reopenStep = findWorkflowStep<{
          metadata?: {
            browser?: {
              clearSiteDataForOrigins?: string[];
            };
          };
        }>(workflow, `reopen_auth_url_before_consent_retry_${attempt}`);

        expect(reopenStep?.metadata?.browser?.clearSiteDataForOrigins).toEqual(
          expect.arrayContaining([
            "https://auth.openai.com",
            "https://chatgpt.com",
            "https://chat.openai.com",
          ]),
        );
      }
    }
  });

  test("all auth replay helpers tolerate OAuth navigation interrupted by a real OpenAI login redirect", async () => {
    for (const workflowPath of [
      originalWorkflowPath,
      minimalWorkflowPath,
      stepwiseWorkflowPath,
      deviceAuthWorkflowPath,
    ]) {
      let currentUrl = "about:blank";
      const fakePage = {
        url() {
          return currentUrl;
        },
        async title() {
          return "Log in";
        },
        async waitForTimeout() {
          return undefined;
        },
        async evaluate(_fn: unknown, url?: string) {
          if (typeof url === "string" && /oauth\/authorize/.test(url)) {
            return undefined;
          }
          return undefined;
        },
        async goto(url: string) {
          currentUrl = "https://auth.openai.com/log-in";
          throw new Error(
            `Navigation to "${url}" is interrupted by another navigation to "https://auth.openai.com/log-in"`,
          );
        },
      };

      const result = await runWorkflowFunctionScriptWithStub(
        workflowPath,
        "replay_captured_auth_url",
        fakePage,
        {},
        {
          auth_url:
            "https://auth.openai.com/oauth/authorize?client_id=test&redirect_uri=http%3A%2F%2Flocalhost%3A1455%2Fauth%2Fcallback",
        },
      );

      expect(result.ok).toBe(true);
      expect(result.current_url).toBe("https://auth.openai.com/log-in");
      expect(result.strategy).toContain("goto");
    }
  });

  test("all auth replay helpers tolerate interrupted OAuth replays that stay on the same OpenAI login page", async () => {
    for (const workflowPath of [
      originalWorkflowPath,
      minimalWorkflowPath,
      stepwiseWorkflowPath,
      deviceAuthWorkflowPath,
    ]) {
      let currentUrl = "https://auth.openai.com/log-in";
      const fakePage = {
        url() {
          return currentUrl;
        },
        async title() {
          return "Log in";
        },
        async waitForTimeout() {
          return undefined;
        },
        async evaluate(_fn: unknown, _url?: string) {
          return undefined;
        },
        async goto(url: string) {
          currentUrl = "https://auth.openai.com/log-in";
          throw new Error(
            `Navigation to "${url}" is interrupted by another navigation to "https://auth.openai.com/log-in"`,
          );
        },
      };

      const result = await runWorkflowFunctionScriptWithStub(
        workflowPath,
        "replay_captured_auth_url",
        fakePage,
        {},
        {
          auth_url:
            "https://auth.openai.com/oauth/authorize?client_id=test&redirect_uri=http%3A%2F%2Flocalhost%3A1455%2Fauth%2Fcallback",
        },
      );

      expect(result.ok).toBe(true);
      expect(result.current_url).toBe("https://auth.openai.com/log-in");
      expect(result.strategy).toContain("goto");
    }
  });

  test("auth workflows keep Gmail artifacts for caller-side cleanup instead of deleting them in-path", async () => {
    for (const workflowPath of [
      originalWorkflowPath,
      minimalWorkflowPath,
      stepwiseWorkflowPath,
      deviceAuthWorkflowPath,
    ]) {
      const workflow = await loadWorkflow(workflowPath);
      const stepIds = (workflow.do || []).flatMap((entry) =>
        Object.keys(entry),
      );
      const workflowText = readFileSync(workflowPath, "utf8");

      expect(workflowText).not.toContain(
        "workflow.sys.web.mail-google-com.delete-verification-artifact",
      );
      expect(
        stepIds.filter((stepId) => stepId.includes("_after_delete_")),
      ).toEqual([]);
    }
  });

  test("collects unique Gmail verification artifacts from workflow results for caller-side cleanup", () => {
    const artifacts = collectVerificationArtifactsForCleanup({
      ok: true,
      status: "ok",
      state: {
        steps: {
          collect_signup_verification_artifact: {
            action: {
              result: {
                result: {
                  output: {
                    gmail_message_id: "msg-signup",
                    gmail_thread_id: "thread-signup",
                    gmail_message_url:
                      "https://mail.google.com/mail/u/0/#inbox/msg-signup",
                    message_subject: "Your ChatGPT code is 112233",
                    selected_email: "1.dev.astronlab@gmail.com",
                  },
                },
              },
            },
          },
          collect_login_verification_artifact: {
            action: {
              output: {
                gmail_message_id: "msg-login",
                gmail_thread_id: "thread-login",
                gmail_message_url:
                  "https://mail.google.com/mail/u/0/#inbox/msg-login",
                message_preview:
                  "Enter this temporary verification code to continue",
                selected_email: "1.dev.astronlab@gmail.com",
              },
            },
          },
          recollect_login_verification_artifact_after_submit_failure: {
            action: {
              result: {
                output: {
                  gmail_message_id: "msg-login",
                  gmail_thread_id: "thread-login",
                  gmail_message_url:
                    "https://mail.google.com/mail/u/0/#inbox/msg-login",
                  selected_email: "1.dev.astronlab@gmail.com",
                },
              },
            },
          },
          unrelated_step: {
            action: {
              output: {
                current_url: "https://auth.openai.com/log-in",
              },
            },
          },
        },
      },
    });

    expect(artifacts).toEqual([
      {
        gmailMessageId: "msg-signup",
        gmailThreadId: "thread-signup",
        gmailMessageUrl: "https://mail.google.com/mail/u/0/#inbox/msg-signup",
        messageSubject: "Your ChatGPT code is 112233",
        messagePreview: null,
        selectedEmail: "1.dev.astronlab@gmail.com",
      },
      {
        gmailMessageId: "msg-login",
        gmailThreadId: "thread-login",
        gmailMessageUrl: "https://mail.google.com/mail/u/0/#inbox/msg-login",
        messageSubject: null,
        messagePreview: "Enter this temporary verification code to continue",
        selectedEmail: "1.dev.astronlab@gmail.com",
      },
    ]);
  });

  test("minimal keeps an in-workflow replayed-login OTP recollection path", async () => {
    const workflow = await loadWorkflow(minimalWorkflowPath);
    const stepIds = (workflow.do || []).flatMap((entry) => Object.keys(entry));
    const workflowText = readFileSync(minimalWorkflowPath, "utf8");

    expect(stepIds).toEqual(
      expect.arrayContaining([
        "resend_login_verification_email_after_submit_failure",
        "recollect_login_verification_artifact_after_submit_failure",
        "submit_login_verification_code_after_submit_failure",
        "cache_effective_login_verification_surface",
      ]),
    );
    expect(workflowText).toContain("login-verification-code-rejected");
    expect(workflowText).toContain(
      "state.steps.submit_login_verification_code_after_submit_failure?.action?.ok === true",
    );
  });

  test("minimal replacement OTP submit script reuses the proven login-stage detector literals", async () => {
    const script = await loadMinimalStepScript(
      "submit_login_verification_code_after_submit_failure",
    );

    expect(script).toContain("/^(localhost|127\\.0\\.0\\.1)$/i.test(host)");
    expect(script).toContain("/\\/log-in(?:\\/|$)/i.test(path)");
    expect(script).toContain('bodyText.split("\\n")');
    expect(script).not.toContain(
      "/^(localhost|127\\\\.0\\\\.0\\\\.1)$/i.test(host)",
    );
    expect(script).not.toContain("/\\\\/log-in(?:\\\\/|$)/i.test(path)");
    expect(script).not.toContain('bodyText.split("\\\\n")');
  });
});

describe("device-auth detached session seeding", () => {
  test("reuses a recoverable caller-provided detached device-auth challenge", async () => {
    const fixtureRoot = mkdtempSync(
      join(tmpdir(), "codex-rotate-device-auth-"),
    );
    try {
      const stdoutPath = join(fixtureRoot, "stdout.log");
      writeFileSync(
        stdoutPath,
        [
          "Open this URL in your browser:",
          "https://auth.openai.com/codex/device?user_code_flow=1",
          "Then enter code ABCD-12345",
        ].join("\n"),
      );

      const result = await runWorkflowRunScript(
        deviceAuthWorkflowPath,
        "seed_existing_codex_login_session",
        {
          codex_login_stdout_path: stdoutPath,
          codex_session_dir: join(fixtureRoot, "session"),
        },
        {},
        {},
      );

      expect(result.ok).toBe(true);
      expect(result.reusable_challenge).toBe(true);
      expect(result.auth_url).toBe(
        "https://auth.openai.com/codex/device?user_code_flow=1",
      );
      expect(result.device_code).toBe("ABCD-12345");
    } finally {
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });

  test("does not treat path-only stale detached session artifacts as reusable", async () => {
    const fixtureRoot = mkdtempSync(
      join(tmpdir(), "codex-rotate-device-auth-"),
    );
    try {
      const stdoutPath = join(fixtureRoot, "stdout.log");
      const stderrPath = join(fixtureRoot, "stderr.log");
      writeFileSync(stdoutPath, "still starting\n");
      writeFileSync(stderrPath, "");

      const result = await runWorkflowRunScript(
        deviceAuthWorkflowPath,
        "seed_existing_codex_login_session",
        {
          codex_login_stdout_path: stdoutPath,
          codex_login_stderr_path: stderrPath,
          codex_session_dir: join(fixtureRoot, "session"),
        },
        {},
        {},
      );

      expect(result.ok).toBe(true);
      expect(result.reusable_challenge).toBe(false);
      expect(result.auth_url).toBeNull();
      expect(result.device_code).toBeNull();
    } finally {
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });
});

describe("device-auth security toggle detection", () => {
  test("finds the Codex device-code toggle even when the container also includes the adjacent Codex CLI row", async () => {
    const result = await runWorkflowStepScriptOnContent(
      deviceAuthWorkflowPath,
      "ensure_device_code_authorization_enabled",
      `
        <html>
          <body>
            <div role="dialog" aria-label="Settings">
              <div>General</div>
              <div>Security</div>
              <section>
                <div>Trusted Devices</div>
                <div>Secure sign in with ChatGPT</div>
                <div>
                  <div>Codex CLI</div>
                  <div>Allow Codex CLI to use models from the API.</div>
                  <button type="button">Disconnect</button>
                  <div>
                    <div>Enable device code authorization for Codex</div>
                    <div>
                      Use device code sign-in for headless or remote environments
                      where the normal browser flow isn't available.
                    </div>
                    <label>
                      <input type="checkbox" />
                    </label>
                  </div>
                </div>
              </section>
            </div>
          </body>
        </html>
      `,
      {},
      {},
    );

    expect(result.ok).toBe(true);
    expect(result.enabled).toBe(true);
    expect(result.error).toBeNull();
  });
});

describe("device-auth recovery preference", () => {
  test("prefers manual one-time-code recovery on the prepare branch even when a stored-password locator exists", async () => {
    const result = await runWorkflowRunScript(
      deviceAuthWorkflowPath,
      "compute_prepare_one_time_code_recovery_flag",
      {
        prefer_password_login: "false",
        account_login_locator: {
          kind: "login_lookup",
          username: "dev.64@astronlab.com",
          uris: ["https://auth.openai.com"],
        },
      },
      {
        steps: {
          cache_effective_prepare_after_login_email_state: {
            action: {
              value: {
                effective_prepare_after_login_email_state: {
                  stage: "login_password",
                },
              },
            },
          },
        },
      },
      {},
    );

    expect(result.ok).toBe(true);
    expect(result.has_locator).toBe(true);
    expect(result.prefer_password_login).toBe(false);
    expect(result.should_recover).toBe(true);
  });

  test("does not force prepare one-time-code recovery from a plain direct login shell after signup invalid-state fallback", async () => {
    const result = await runWorkflowRunScript(
      deviceAuthWorkflowPath,
      "compute_prepare_one_time_code_recovery_flag",
      {},
      {
        steps: {
          classify_prepare_after_login_email_signup_invalid_state_fallback: {
            action: {
              ok: true,
              stage: "login_email",
              current_url: "https://auth.openai.com/log-in",
              one_time_code_cta: false,
            },
          },
          classify_prepare_login_entry_after_signup_invalid_state_fallback: {
            action: {
              ok: true,
              stage: "login_email",
              current_url: "https://auth.openai.com/log-in",
              one_time_code_cta: false,
            },
          },
          cache_effective_prepare_after_login_email_state: {
            action: {
              value: {
                effective_prepare_after_login_email_state: {
                  stage: "login_email",
                  current_url: "https://auth.openai.com/log-in",
                  one_time_code_cta: false,
                },
              },
            },
          },
        },
      },
      {},
    );

    expect(result.ok).toBe(true);
    expect(result.direct_login_shell).toBe(true);
    expect(result.one_time_code_visible).toBe(false);
    expect(result.should_recover).toBe(false);
  });

  test("prefers manual one-time-code recovery on the device-auth login branch even when a stored-password locator exists", async () => {
    const result = await runWorkflowRunScript(
      deviceAuthWorkflowPath,
      "compute_device_auth_one_time_code_recovery_flag",
      {
        prefer_password_login: "false",
        account_login_locator: {
          kind: "login_lookup",
          username: "dev.64@astronlab.com",
          uris: ["https://auth.openai.com"],
        },
      },
      {
        steps: {
          classify_device_auth_after_login_email: {
            action: {
              stage: "login_password",
            },
          },
          fill_device_auth_login_password: {
            action: {
              ok: true,
              skipped: false,
            },
          },
        },
      },
      {},
    );

    expect(result.ok).toBe(true);
    expect(result.has_locator).toBe(true);
    expect(result.prefer_password_login).toBe(false);
    expect(result.should_recover).toBe(true);
  });

  test("defaults to password-first on the device-auth login branch when a stored-password locator exists", async () => {
    const result = await runWorkflowRunScript(
      deviceAuthWorkflowPath,
      "compute_device_auth_one_time_code_recovery_flag",
      {
        account_login_locator: {
          kind: "login_lookup",
          username: "dev.64@astronlab.com",
          uris: ["https://auth.openai.com"],
        },
      },
      {
        steps: {
          classify_device_auth_after_login_email: {
            action: {
              stage: "login_password",
            },
          },
          fill_device_auth_login_password: {
            action: {
              ok: true,
              skipped: false,
            },
          },
        },
      },
      {},
    );

    expect(result.ok).toBe(true);
    expect(result.has_locator).toBe(true);
    expect(result.prefer_password_login).toBe(true);
    expect(result.should_recover).toBe(false);
  });

  test("recovers with one-time-code after the signup invalid_state fallback returns to the direct login shell", async () => {
    const result = await runWorkflowRunScript(
      deviceAuthWorkflowPath,
      "compute_prepare_one_time_code_recovery_flag",
      {
        prefer_password_login: "true",
      },
      {
        steps: {
          classify_prepare_after_login_email_signup_invalid_state_fallback: {
            action: {
              ok: true,
              stage: "unknown",
              current_url: "https://auth.openai.com/log-in",
            },
          },
          cache_effective_prepare_after_login_email_state: {
            action: {
              value: {
                effective_prepare_after_login_email_state: {
                  stage: "unknown",
                  current_url: "https://auth.openai.com/log-in",
                },
              },
            },
          },
        },
      },
      {},
    );

    expect(result.ok).toBe(true);
    expect(result.signup_invalid_state_fallback_active).toBe(true);
    expect(result.direct_login_shell).toBe(true);
    expect(result.should_recover).toBe(true);
  });
});

describe("minimal about-you helper", () => {
  test("does not contain raw workflow interpolation markers in the helper script", async () => {
    const script = await loadMinimalAboutYouHelperScript();
    expect(script.includes("${")).toBe(false);
  });

  test("fills the legacy month/day/year select layout", async () => {
    const result = await runMinimalAboutYouHelper(
      `
        <html>
          <body style="min-height: 100vh;">
            <label for="full-name">Full name</label>
            <input
              id="full-name"
              name="name"
              aria-label="Full name"
              placeholder="Full name"
            />
            <label for="birth-month">Birth month</label>
            <select id="birth-month" name="month">
              <option value="">Month</option>
              <option value="4">April</option>
            </select>
            <label for="birth-day">Birth day</label>
            <select id="birth-day" name="day">
              <option value="">Day</option>
              <option value="8">8</option>
            </select>
            <label for="birth-year">Birth year</label>
            <select id="birth-year" name="year">
              <option value="">Year</option>
              <option value="1990">1990</option>
            </select>
            <button type="submit">Finish creating account</button>
          </body>
        </html>
      `,
      {
        full_name: "Dev Astronlab",
        birth_month: "4",
        birth_day: "8",
        birth_year: "1990",
      },
    );

    expect(result.ok).toBe(true);
    expect(result.name_value).toBe("Dev Astronlab");
    expect(result.month_value).toBe("April");
    expect(result.day_value).toBe("8");
    expect(result.year_value).toBe("1990");
    expect(result.submit_disabled).toBe(false);
  });

  test("fills the compact birthday input layout", async () => {
    const result = await runMinimalAboutYouHelper(
      `
        <html>
          <body style="min-height: 100vh;">
            <label for="full-name">Full name</label>
            <input
              id="full-name"
              name="name"
              aria-label="Full name"
              placeholder="Full name"
            />
            <label for="birthday">Birthday</label>
            <input
              id="birthday"
              name="birthday"
              aria-label="Birthday"
              placeholder="MM/DD/YYYY"
              inputmode="numeric"
              value="04/08/2026"
            />
            <button type="submit">Finish creating account</button>
          </body>
        </html>
      `,
      {
        full_name: "Dev Astronlab",
        birth_month: "4",
        birth_day: "8",
        birth_year: "1990",
      },
    );

    expect(result.ok).toBe(true);
    expect(result.name_value).toBe("Dev Astronlab");
    expect(result.birthday_value).toBe("04/08/1990");
    expect(result.submit_disabled).toBe(false);
  });

  test("fills a floating-label two-input layout without useful input attributes", async () => {
    const result = await runMinimalAboutYouHelper(
      `
        <html>
          <body style="min-height: 100vh;">
            <div>Full name</div>
            <input />
            <div>Birthday</div>
            <input value="04/08/2026" />
            <button type="submit">Finish creating account</button>
          </body>
        </html>
      `,
      {
        full_name: "Dev Astronlab",
        birth_month: "4",
        birth_day: "8",
        birth_year: "1990",
      },
    );

    expect(result.ok).toBe(true);
    expect(result.name_value).toBe("Dev Astronlab");
    expect(result.birthday_value).toBe("04/08/1990");
    expect(result.submit_disabled).toBe(false);
  });

  test("fills the live full-name and age layout", async () => {
    const result = await runMinimalAboutYouHelper(
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>How old are you?</h1>
            <label for="full-name">Full name</label>
            <input id="full-name" name="name" aria-label="Full name" />
            <label for="age">Age</label>
            <input id="age" inputmode="numeric" aria-label="Age" />
            <button id="submit" type="submit" disabled>Finish creating account</button>
            <script>
              const fullName = document.getElementById("full-name");
              const age = document.getElementById("age");
              const submit = document.getElementById("submit");
              const sync = () => {
                submit.disabled =
                  fullName.value.trim().length === 0 || age.value.trim().length === 0;
              };
              fullName.addEventListener("input", sync);
              age.addEventListener("input", sync);
            </script>
          </body>
        </html>
      `,
      {
        full_name: "Dev Astronlab",
        birth_month: "4",
        birth_day: "8",
        birth_year: "1990",
      },
    );

    expect(result.ok).toBe(true);
    expect(result.name_value).toBe("Dev Astronlab");
    expect(result.age_value).toBe("36");
    expect(result.submit_disabled).toBe(false);
  });

  test("does not report success when the age layout keeps submit disabled", async () => {
    await expect(
      runMinimalAboutYouHelper(
        `
          <html>
            <body style="min-height: 100vh;">
              <h1>How old are you?</h1>
              <label for="full-name">Full name</label>
              <input id="full-name" name="name" aria-label="Full name" />
              <label for="age">Age</label>
              <input id="age" inputmode="numeric" aria-label="Age" />
              <button id="submit" type="submit" disabled>Finish creating account</button>
            </body>
          </html>
        `,
        {
          full_name: "Dev Astronlab",
          birth_month: "4",
          birth_day: "8",
          birth_year: "1990",
        },
      ),
    ).rejects.toThrow("about-you-fill-failed");
  });

  test("accepts a visible birthday shell when only the name input is directly editable", async () => {
    const result = await runMinimalAboutYouHelper(
      `
        <html>
          <body style="min-height: 100vh;">
            <div>Full name</div>
            <input id="name" />
            <div>Birthday</div>
            <div id="birthday-shell">04 / 09 / 2026</div>
            <button id="submit" type="submit" disabled>Finish creating account</button>
            <script>
              const name = document.getElementById("name");
              const submit = document.getElementById("submit");
              name.addEventListener("input", () => {
                submit.disabled = name.value.trim().length === 0;
              });
            </script>
          </body>
        </html>
      `,
      {
        full_name: "Dev Astronlab",
        birth_month: "4",
        birth_day: "9",
        birth_year: "1990",
      },
    );

    expect(result.ok).toBe(true);
    expect(result.name_value).toBe("Dev Astronlab");
    expect(result.birthday_value).toBe("04/09/2026");
    expect(result.submit_disabled).toBe(false);
  });

  test("treats an already-advanced ChatGPT shell as a successful about-you submit transition", async () => {
    const result = await runWorkflowStepScript(
      minimalWorkflowPath,
      "submit_signup_about_you",
      `
        <html>
          <body style="min-height: 100vh;">
            <div>New chat</div>
            <div>Projects</div>
            <button type="button">Log in</button>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/",
    );

    expect(result.ok).toBe(true);
    expect(result.next_stage).toBe("account_ready");
  });
});

describe("minimal verification helper", () => {
  test("does not treat the about-you age field as replayed email verification", async () => {
    const result = await runWorkflowStepScript(
      minimalWorkflowPath,
      "classify_after_login_password",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>How old are you?</h1>
            <label for="full-name">Full name</label>
            <input id="full-name" name="name" aria-label="Full name" />
            <label for="age">Age</label>
            <input id="age" inputmode="numeric" aria-label="Age" />
            <button type="submit">Finish creating account</button>
          </body>
        </html>
      `,
      {},
      {},
      "https://auth.openai.com/about-you",
    );

    expect(result.stage).toBe("about_you");
    expect(result.follow_up_step).toBe(true);
    expect(result.needs_email_verification).toBe(false);
  });

  test("retries the one-time-code recovery click when OpenAI stays on invalid credentials", async () => {
    const result = await runMinimalStepScript(
      "choose_login_one_time_code_recovery",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Enter your password</h1>
            <p>Incorrect email address or password</p>
            <input type="password" name="password" />
            <button id="recovery" type="button">Log in with a one-time code</button>
            <script>
              let attempts = 0;
              document.getElementById("recovery").addEventListener("click", () => {
                attempts += 1;
                if (attempts >= 2) {
                  document.body.innerHTML =
                    '<h1>Check your inbox</h1><input autocomplete="one-time-code" />';
                }
              });
            </script>
          </body>
        </html>
      `,
      {},
    );

    expect(result.ok).toBe(true);
    expect(result.stage).toBe("email_verification");
    expect(result.retried).toBe(true);
  });

  test("opens the intermediate chooser before selecting email-code recovery", async () => {
    const result = await runWorkflowStepScript(
      stepwiseWorkflowPath,
      "choose_login_one_time_code_recovery",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Enter your password</h1>
            <p>Incorrect email address or password</p>
            <input type="password" name="password" />
            <button id="chooser" type="button">Try another way</button>
            <div id="options" hidden>
              <button id="email-code" type="button">Email me a code</button>
            </div>
            <script>
              const chooser = document.getElementById("chooser");
              const options = document.getElementById("options");
              const emailCode = document.getElementById("email-code");
              chooser.addEventListener("click", () => {
                options.hidden = false;
              });
              emailCode.addEventListener("click", () => {
                document.body.innerHTML =
                  '<h1>Check your inbox</h1><input autocomplete="one-time-code" />';
                history.replaceState({}, "", "https://auth.openai.com/email-verification");
              });
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://auth.openai.com/log-in/password",
    );

    expect(result.ok).toBe(true);
    expect(result.stage).toBe("email_verification");
    expect(String(result.clicked_text || "")).toContain("Try another way");
    expect(String(result.clicked_text || "")).toContain("Email me a code");
  });

  test("non-device one-time-code recovery avoids workflow-template interpolation in its live error path", () => {
    for (const workflowPath of [
      originalWorkflowPath,
      minimalWorkflowPath,
      stepwiseWorkflowPath,
    ]) {
      const workflowText = readFileSync(workflowPath, "utf8");
      expect(workflowText).toContain(
        'throw new Error("one-time-code-recovery-did-not-advance:" + (after.current_url || page.url()));',
      );
      expect(workflowText).not.toContain(
        "one-time-code-recovery-did-not-advance:${after.current_url || page.url()}",
      );
    }
  });

  test("stepwise retries one-time-code recovery after an invalid_state timeout page", async () => {
    const result = await runWorkflowStepScript(
      stepwiseWorkflowPath,
      "choose_login_one_time_code_recovery",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Enter your password</h1>
            <p>Incorrect email address or password</p>
            <input type="password" name="password" />
            <button id="recovery" type="button">Log in with a one-time code</button>
            <script>
              document.getElementById("recovery").addEventListener("click", () => {
                document.body.innerHTML =
                  '<h1>Oops, an error occurred!</h1><p>An error occurred during authentication (invalid_state). Please try again.</p><button id="retry" type="button">Try again</button>';
                const retry = document.getElementById("retry");
                retry.addEventListener("click", () => {
                  document.body.innerHTML =
                    '<h1>Enter your password</h1><p>Incorrect email address or password</p><input type="password" name="password" /><button id="recovery-again" type="button">Log in with a one-time code</button>';
                  document.getElementById("recovery-again").addEventListener("click", () => {
                    document.body.innerHTML =
                      '<h1>Check your inbox</h1><input autocomplete="one-time-code" />';
                    history.replaceState({}, "", "https://auth.openai.com/email-verification");
                  });
                });
              });
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://auth.openai.com/log-in/password",
    );

    expect(result.ok).toBe(true);
    expect(result.stage).toBe("email_verification");
    expect(result.retried).toBe(true);
    expect(String(result.clicked_text || "")).toContain("Try again");
  });

  test("stepwise retries one-time-code recovery when the first click stays on the password page", async () => {
    const result = await runWorkflowStepScript(
      stepwiseWorkflowPath,
      "choose_login_one_time_code_recovery",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Enter your password</h1>
            <input type="password" name="password" />
            <button id="recovery" type="button">Log in with a one-time code</button>
            <script>
              let attempts = 0;
              document.getElementById("recovery").addEventListener("click", () => {
                attempts += 1;
                if (attempts < 2) {
                  return;
                }
                document.body.innerHTML =
                  '<h1>Check your inbox</h1><input autocomplete="one-time-code" />';
                history.replaceState({}, "", "https://auth.openai.com/email-verification");
              });
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://auth.openai.com/log-in/password",
    );

    expect(result.ok).toBe(true);
    expect(result.stage).toBe("email_verification");
    expect(result.retried).toBe(true);
    expect(String(result.clicked_text || "")).toContain(
      "Log in with a one-time code",
    );
  });

  test("non-device one-time-code recovery tolerates multiple password-page bounces before advancing", async () => {
    for (const workflowPath of [
      originalWorkflowPath,
      minimalWorkflowPath,
      stepwiseWorkflowPath,
    ]) {
      const result = await runWorkflowStepScript(
        workflowPath,
        "choose_login_one_time_code_recovery",
        `
          <html>
            <body style="min-height: 100vh;">
              <h1>Enter your password</h1>
              <input type="password" name="password" />
              <button id="recovery" type="button">Log in with a one-time code</button>
              <script>
                let attempts = 0;
                const installRecovery = () => {
                  document.getElementById("recovery")?.addEventListener("click", () => {
                    attempts += 1;
                    if (attempts < 3) {
                      document.body.innerHTML =
                        '<h1>Enter your password</h1><input type="password" name="password" /><button id="recovery" type="button">Log in with a one-time code</button>';
                      installRecovery();
                      return;
                    }
                    document.body.innerHTML =
                      '<h1>Check your inbox</h1><input autocomplete="one-time-code" />';
                    history.replaceState({}, "", "https://auth.openai.com/email-verification");
                  });
                };
                installRecovery();
              </script>
            </body>
          </html>
        `,
        {},
        {},
        "https://auth.openai.com/log-in/password",
      );

      expect(result.ok).toBe(true);
      expect(result.stage).toBe("email_verification");
      expect(result.retried).toBe(true);
      expect(String(result.clicked_text || "")).toContain(
        "Log in with a one-time code",
      );
    }
  });

  test("stepwise one-time-code recovery reports a stuck password page without throwing", async () => {
    const result = await runWorkflowStepScript(
      stepwiseWorkflowPath,
      "choose_login_one_time_code_recovery",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Enter your password</h1>
            <input type="password" name="password" />
            <button id="recovery" type="button">Log in with a one-time code</button>
            <script>
              const installRecovery = () => {
                document.getElementById("recovery")?.addEventListener("click", () => {
                  document.body.innerHTML =
                    '<h1>Enter your password</h1><input type="password" name="password" /><button id="recovery" type="button">Log in with a one-time code</button>';
                  installRecovery();
                });
              };
              installRecovery();
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://auth.openai.com/log-in/password",
    );

    expect(result.ok).toBe(true);
    expect(result.stage).toBe("login_password");
    expect(result.stayed_on_password_page).toBe(true);
    expect(String(result.current_url || "")).toContain("/log-in/password");
  });

  test("stepwise forgot-password recovery opens the email verification path", async () => {
    const result = await runWorkflowStepScript(
      stepwiseWorkflowPath,
      "choose_login_forgot_password_recovery",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Enter your password</h1>
            <input type="password" name="password" />
            <a id="forgot" href="#" role="link">Forgot password?</a>
            <script>
              document.getElementById("forgot").addEventListener("click", (event) => {
                event.preventDefault();
                document.body.innerHTML =
                  '<h1>Check your inbox</h1><input autocomplete="one-time-code" />';
                history.replaceState({}, "", "https://auth.openai.com/email-verification");
              });
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://auth.openai.com/log-in/password",
    );

    expect(result.ok).toBe(true);
    expect(String(result.clicked_text || "")).toContain("Forgot password");
  });

  test("stepwise forgot-password recovery clears a retryable timeout before opening email verification", async () => {
    const result = await runWorkflowStepScript(
      stepwiseWorkflowPath,
      "choose_login_forgot_password_recovery",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Oops, an error occurred!</h1>
            <button id="retry" type="button">Try again</button>
            <script>
              document.getElementById("retry").addEventListener("click", () => {
                document.body.innerHTML =
                  '<h1>Enter your password</h1><input type="password" name="password" /><a id="forgot" href="#" role="link">Forgot password?</a>';
                const forgot = document.getElementById("forgot");
                forgot.addEventListener("click", (event) => {
                  event.preventDefault();
                  document.body.innerHTML =
                    '<h1>Check your inbox</h1><input autocomplete="one-time-code" />';
                  history.replaceState({}, "", "https://auth.openai.com/email-verification");
                });
                history.replaceState({}, "", "https://auth.openai.com/log-in/password");
              });
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://auth.openai.com/log-in/password",
    );

    expect(result.ok).toBe(true);
    expect(String(result.clicked_text || "")).toContain("Try again");
    expect(String(result.clicked_text || "")).toContain("Forgot password");
  });

  test("stepwise forgot-password recovery keeps the retry branch alive even before the reset link appears", async () => {
    const result = await runWorkflowStepScript(
      stepwiseWorkflowPath,
      "choose_login_forgot_password_recovery",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Oops, an error occurred!</h1>
            <button id="retry" type="button">Try again</button>
            <script>
              document.getElementById("retry").addEventListener("click", () => {
                document.body.innerHTML =
                  '<h1>Enter your password</h1><input type="password" name="password" />';
                history.replaceState({}, "", "https://auth.openai.com/log-in/password");
                setTimeout(() => {
                  document.body.innerHTML =
                    '<h1>Reset password</h1><input type="email" name="email" /><button type="button">Continue</button>';
                  history.replaceState({}, "", "https://auth.openai.com/reset-password");
                }, 700);
              });
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://auth.openai.com/log-in/password",
    );

    expect(result.ok).toBe(true);
    expect(result.skipped).toBeUndefined();
    expect(result.retried).toBe(true);
    expect(String(result.clicked_text || "")).toContain("Try again");
  });

  test("stepwise forgot-password recovery stays alive on the password page even when recovery controls are not yet visible", async () => {
    const result = await runWorkflowStepScript(
      stepwiseWorkflowPath,
      "choose_login_forgot_password_recovery",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Enter your password</h1>
            <input type="password" name="password" />
          </body>
        </html>
      `,
      {},
      {},
      "https://auth.openai.com/log-in/password",
    );

    expect(result.ok).toBe(true);
    expect(result.skipped).toBeUndefined();
    expect(result.recovery_unavailable).toBe(true);
    expect(String(result.current_url || "")).toContain("/log-in/password");
  });

  test("stepwise submits the reset-password request and advances to email verification", async () => {
    const result = await runWorkflowStepScript(
      stepwiseWorkflowPath,
      "submit_login_reset_password_request",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Reset password</h1>
            <label>Email<input type="email" name="email" /></label>
            <button id="send" type="button">Continue</button>
            <script>
              document.getElementById("send").addEventListener("click", () => {
                document.body.innerHTML =
                  '<h1>Check your inbox</h1><input autocomplete="one-time-code" />';
                history.replaceState({}, "", "https://auth.openai.com/email-verification");
              });
            </script>
          </body>
        </html>
      `,
      {},
      {
        email: "dev3astronlab+5@gmail.com",
      },
      "https://auth.openai.com/reset-password",
    );

    expect(result.ok).toBe(true);
    expect(result.email_filled).toBe("dev3astronlab+5@gmail.com");
    expect(String(result.clicked_text || "")).toContain("Continue");
  });

  test("stepwise retries the reset-password submit once after a retryable timeout overlay", async () => {
    const result = await runWorkflowStepScript(
      stepwiseWorkflowPath,
      "submit_login_reset_password_request",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Reset password</h1>
            <label>Email<input type="email" name="email" /></label>
            <button id="send" type="button">Continue</button>
            <script>
              let first = true;
              document.getElementById("send").addEventListener("click", () => {
                if (first) {
                  first = false;
                  document.body.innerHTML =
                    '<h1>Oops, an error occurred!</h1><button id="retry" type="button">Try again</button><button id="send-again" type="button">Continue</button>';
                  history.replaceState({}, "", "https://auth.openai.com/reset-password");
                  document.getElementById("retry").addEventListener("click", () => {});
                  document.getElementById("send-again").addEventListener("click", () => {
                    document.body.innerHTML =
                      '<h1>Check your inbox</h1><input autocomplete="one-time-code" />';
                    history.replaceState({}, "", "https://auth.openai.com/email-verification");
                  });
                  return;
                }
              });
            </script>
          </body>
        </html>
      `,
      {},
      {
        email: "dev3astronlab+5@gmail.com",
      },
      "https://auth.openai.com/reset-password",
    );

    expect(result.ok).toBe(true);
    expect(result.timeout_retried).toBe(true);
    expect(String(result.retry_clicked_text || "")).toContain("Try again");
    expect(String(result.resubmit_clicked_text || "")).toContain("Continue");
    expect(String(result.current_url || "")).toContain("/email-verification");
  });

  test("stepwise routes recovery-unavailable password pages into the reset-password submit step", async () => {
    const workflow = await loadWorkflow(stepwiseWorkflowPath);
    const submitStep = workflow.do?.find(
      (entry) => "submit_login_reset_password_request" in entry,
    )?.submit_login_reset_password_request as { if?: string } | undefined;

    expect(submitStep?.if).toContain(
      "choose_login_forgot_password_recovery?.action?.recovery_unavailable === true",
    );
    expect(submitStep?.if).toContain(
      "classify_after_login_forgot_password_gate?.action?.stage === 'login_password'",
    );
  });

  test("stepwise retries reset-password after a delayed timeout classification on the reset page", async () => {
    const workflow = await loadWorkflow(stepwiseWorkflowPath);
    const retryStep = workflow.do?.find(
      (entry) => "retry_login_reset_password_request_after_timeout" in entry,
    )?.retry_login_reset_password_request_after_timeout as
      | { if?: string }
      | undefined;
    const resendStep = workflow.do?.find(
      (entry) => "resend_login_verification_email" in entry,
    )?.resend_login_verification_email as { if?: string } | undefined;

    expect(retryStep?.if).toContain(
      "classify_after_login_reset_password_request?.action?.retryable_timeout === true",
    );
    expect(retryStep?.if).toContain(
      "classify_after_login_reset_password_gate?.action?.retryable_timeout === true",
    );
    expect(retryStep?.if).toContain(
      "classify_after_login_reset_password_request?.action?.reset_password_prompt === true",
    );
    expect(resendStep?.if).toContain(
      "classify_after_login_reset_password_timeout_retry?.action?.stage === 'email_verification'",
    );
    expect(resendStep?.if).toContain(
      "classify_after_login_reset_password_timeout_retry_gate?.action?.stage === 'email_verification'",
    );
  });

  test("original opens the intermediate chooser before selecting email-code recovery", async () => {
    const result = await runWorkflowStepScript(
      originalWorkflowPath,
      "choose_login_one_time_code_recovery",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Enter your password</h1>
            <p>Incorrect email address or password</p>
            <input type="password" name="password" />
            <button id="chooser" type="button">Try another way</button>
            <div id="options" hidden>
              <button id="email-code" type="button">Email me a code</button>
            </div>
            <script>
              const chooser = document.getElementById("chooser");
              const options = document.getElementById("options");
              const emailCode = document.getElementById("email-code");
              chooser.addEventListener("click", () => {
                options.hidden = false;
              });
              emailCode.addEventListener("click", () => {
                document.body.innerHTML =
                  '<h1>Check your inbox</h1><input autocomplete="one-time-code" />';
                history.replaceState({}, "", "https://auth.openai.com/email-verification");
              });
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://auth.openai.com/log-in/password",
    );

    expect(result.ok).toBe(true);
    expect(result.stage).toBe("email_verification");
    expect(String(result.clicked_text || "")).toContain("Try another way");
    expect(String(result.clicked_text || "")).toContain("Email me a code");
  });

  test("original falls back to a DOM click when the one-time-code button is not pointer-clickable", async () => {
    const result = await runWorkflowStepScript(
      originalWorkflowPath,
      "choose_login_one_time_code_recovery",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Enter your password</h1>
            <input type="password" name="password" />
            <button id="recovery" type="button" style="pointer-events: none;">Log in with a one-time code</button>
            <script>
              document.getElementById("recovery").addEventListener("click", () => {
                document.body.innerHTML =
                  '<h1>Check your inbox</h1><input autocomplete="one-time-code" />';
                history.replaceState({}, "", "https://auth.openai.com/email-verification");
              });
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://auth.openai.com/log-in/password",
    );

    expect(result.ok).toBe(true);
    expect(result.stage).toBe("email_verification");
    expect(String(result.clicked_text || "")).toContain(
      "Log in with a one-time code",
    );
  });

  test("original forgot-password recovery opens the email verification path", async () => {
    const result = await runWorkflowStepScript(
      originalWorkflowPath,
      "choose_login_forgot_password_recovery",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Enter your password</h1>
            <input type="password" name="password" />
            <a id="forgot" href="#" role="link">Forgot password?</a>
            <script>
              document.getElementById("forgot").addEventListener("click", (event) => {
                event.preventDefault();
                document.body.innerHTML =
                  '<h1>Check your inbox</h1><input autocomplete="one-time-code" />';
                history.replaceState({}, "", "https://auth.openai.com/email-verification");
              });
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://auth.openai.com/log-in/password",
    );

    expect(result.ok).toBe(true);
    expect(String(result.clicked_text || "")).toContain("Forgot password");
  });

  test("original routes recovery-unavailable password pages into the reset-password submit step", async () => {
    const workflow = await loadWorkflow(originalWorkflowPath);
    const submitStep = workflow.do?.find(
      (entry) => "submit_login_reset_password_request" in entry,
    )?.submit_login_reset_password_request as { if?: string } | undefined;

    expect(submitStep?.if).toContain(
      "choose_login_forgot_password_recovery?.action?.recovery_unavailable === true",
    );
    expect(submitStep?.if).toContain(
      "classify_after_login_forgot_password_gate?.action?.stage === 'login_password'",
    );
  });

  test("original retries reset-password after a delayed timeout classification on the reset page", async () => {
    const workflow = await loadWorkflow(originalWorkflowPath);
    const retryStep = workflow.do?.find(
      (entry) => "retry_login_reset_password_request_after_timeout" in entry,
    )?.retry_login_reset_password_request_after_timeout as
      | { if?: string }
      | undefined;
    const resendStep = workflow.do?.find(
      (entry) => "resend_login_verification_email" in entry,
    )?.resend_login_verification_email as { if?: string } | undefined;

    expect(retryStep?.if).toContain(
      "classify_after_login_reset_password_request?.action?.retryable_timeout === true",
    );
    expect(retryStep?.if).toContain(
      "classify_after_login_reset_password_gate?.action?.retryable_timeout === true",
    );
    expect(retryStep?.if).toContain(
      "classify_after_login_reset_password_request?.action?.reset_password_prompt === true",
    );
    expect(resendStep?.if).toContain(
      "classify_after_login_reset_password_timeout_retry?.action?.stage === 'email_verification'",
    );
    expect(resendStep?.if).toContain(
      "classify_after_login_reset_password_timeout_retry_gate?.action?.stage === 'email_verification'",
    );
  });

  test("fills segmented OTP inputs and advances the flow", async () => {
    const result = await runMinimalStepScript(
      "submit_login_verification_code",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent.</p>
            <div id="otp" style="display: flex; gap: 8px;">
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
            </div>
            <button id="continue" type="button" disabled>Continue</button>
            <script>
              const inputs = Array.from(document.querySelectorAll("#otp input"));
              const button = document.getElementById("continue");
              const readCode = () => inputs.map((input) => input.value).join("");
              const sync = () => {
                button.disabled = readCode().length < 6;
              };
              inputs.forEach((input, index) => {
                input.addEventListener("input", () => {
                  input.value = String(input.value || "").replace(/\\D+/g, "").slice(-1);
                  sync();
                });
              });
              button.addEventListener("click", () => {
                if (readCode() === "632467") {
                  document.body.innerHTML =
                    '<h1>Sign in to Codex with ChatGPT</h1><button>Continue to Codex</button>';
                }
              });
              sync();
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          collect_login_verification_artifact: {
            action: {
              result: {
                output: {
                  code: "632467",
                },
              },
            },
          },
        },
      },
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("632467");
    expect(result.next_stage).toBe("oauth_consent");
  });

  test("extracts a replayed-login OTP from a deeply nested workflow artifact shape", async () => {
    const result = await runMinimalStepScript(
      "submit_login_verification_code",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent.</p>
            <div id="otp" style="display: flex; gap: 8px;">
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
            </div>
            <button id="continue" type="button" disabled>Continue</button>
            <script>
              const inputs = Array.from(document.querySelectorAll("#otp input"));
              const button = document.getElementById("continue");
              const readCode = () => inputs.map((input) => input.value).join("");
              const sync = () => {
                button.disabled = readCode().length < 6;
              };
              inputs.forEach((input) => {
                input.addEventListener("input", () => {
                  input.value = String(input.value || "").replace(/\\D+/g, "").slice(-1);
                  sync();
                });
              });
              button.addEventListener("click", () => {
                if (readCode() === "380393") {
                  document.body.innerHTML =
                    '<h1>Sign in to Codex with ChatGPT</h1><button>Continue to Codex</button>';
                }
              });
              sync();
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          collect_login_verification_artifact: {
            action: {
              result: {
                result: {
                  output: {
                    ok: true,
                    message_subject:
                      "Your OpenAI code is 380393 - Enter this temporary verification code to continue: 380393",
                  },
                  state: {
                    steps: {
                      collect_verification_artifact: {
                        action: {
                          current_url:
                            "https://mail.google.com/mail/u/0/#search/from%3Aopenai",
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("380393");
    expect(result.next_stage).toBe("oauth_consent");
  });

  test("extracts a signup OTP from a deeply nested workflow artifact shape", async () => {
    const result = await runMinimalStepScript(
      "submit_signup_verification_code",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent.</p>
            <div id="otp" style="display: flex; gap: 8px;">
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
            </div>
            <button id="continue" type="button" disabled>Continue</button>
            <script>
              const inputs = Array.from(document.querySelectorAll("#otp input"));
              const button = document.getElementById("continue");
              const readCode = () => inputs.map((input) => input.value).join("");
              const sync = () => {
                button.disabled = readCode().length < 6;
              };
              inputs.forEach((input) => {
                input.addEventListener("input", () => {
                  input.value = String(input.value || "").replace(/\\D+/g, "").slice(-1);
                  sync();
                });
              });
              button.addEventListener("click", () => {
                if (readCode() === "380393") {
                  document.body.innerHTML =
                    '<h1>Sign in to Codex with ChatGPT</h1><button>Continue to Codex</button>';
                }
              });
              sync();
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          collect_signup_verification_artifact: {
            action: {
              result: {
                result: {
                  output: {
                    ok: true,
                    message_subject:
                      "Your OpenAI code is 380393 - Enter this temporary verification code to continue: 380393",
                  },
                  state: {
                    steps: {
                      collect_verification_artifact: {
                        action: {
                          current_url:
                            "https://mail.google.com/mail/u/0/#search/from%3Aopenai",
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("380393");
    expect(result.next_stage).toBe("oauth_consent");
  });

  test("uses the cached signup OTP when the first Gmail collection loaded late", async () => {
    const result = await runMinimalStepScript(
      "submit_signup_verification_code",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent.</p>
            <div id="otp" style="display: flex; gap: 8px;">
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
            </div>
            <button id="continue" type="button" disabled>Continue</button>
            <script>
              const inputs = Array.from(document.querySelectorAll("#otp input"));
              const button = document.getElementById("continue");
              const readCode = () => inputs.map((input) => input.value).join("");
              const sync = () => {
                button.disabled = readCode().length < 6;
              };
              inputs.forEach((input) => {
                input.addEventListener("input", () => {
                  input.value = String(input.value || "").replace(/\\D+/g, "").slice(-1);
                  sync();
                });
              });
              button.addEventListener("click", () => {
                if (readCode() === "654321") {
                  document.body.innerHTML =
                    '<h1>Sign in to Codex with ChatGPT</h1><button>Continue to Codex</button>';
                }
              });
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          collect_signup_verification_artifact: {
            action: {
              result: {
                output: {
                  ok: true,
                },
              },
            },
          },
        },
        vars: {
          signup_verification_code: "654321",
        },
      },
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("654321");
    expect(result.next_stage).toBe("oauth_consent");
  });

  test("device-auth classifier does not treat the about-you age field as email verification", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "classify_prepare_after_login_verification_retry",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>How old are you?</h1>
            <label for="full-name">Full name</label>
            <input id="full-name" name="name" aria-label="Full name" />
            <label for="age">Age</label>
            <input id="age" inputmode="numeric" aria-label="Age" />
            <button type="submit">Finish creating account</button>
          </body>
        </html>
      `,
      {},
      {},
      "https://auth.openai.com/about-you",
    );

    expect(result.stage).toBe("about_you");
    expect(result.follow_up_step).toBe(true);
    expect(result.email_verification).toBe(false);
  });

  test("device-auth about-you helper fills the live full-name and age layout", async () => {
    const result = await runDeviceAuthAboutYouHelper(
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>How old are you?</h1>
            <label for="full-name">Full name</label>
            <input id="full-name" name="name" aria-label="Full name" />
            <label for="age">Age</label>
            <input id="age" inputmode="numeric" aria-label="Age" />
            <button id="submit" type="submit" disabled>Finish creating account</button>
            <script>
              const fullName = document.getElementById("full-name");
              const age = document.getElementById("age");
              const submit = document.getElementById("submit");
              const sync = () => {
                submit.disabled =
                  fullName.value.trim().length === 0 || age.value.trim().length === 0;
              };
              fullName.addEventListener("input", sync);
              age.addEventListener("input", sync);
            </script>
          </body>
        </html>
      `,
      {
        full_name: "Dev Astronlab",
        birth_month: "4",
        birth_day: "8",
        birth_year: "1990",
      },
      "https://auth.openai.com/about-you",
    );

    expect(result.ok).toBe(true);
    expect(result.name_value).toBe("Dev Astronlab");
    expect(result.age_value).toBe("36");
    expect(result.submit_disabled).toBe(false);
  });

  test("device-auth about-you helper accepts a public ChatGPT redirect as already advanced", async () => {
    const result = await runDeviceAuthAboutYouHelper(
      `
        <html>
          <body style="min-height: 100vh;">
            <div>New chat</div>
            <div>Search chats</div>
            <button type="button">Log in</button>
            <button type="button">Sign up for free</button>
            <div>What’s on the agenda today?</div>
          </body>
        </html>
      `,
      {
        full_name: "Dev Astronlab",
        birth_month: "4",
        birth_day: "8",
        birth_year: "1990",
      },
      "https://chatgpt.com/",
    );

    expect(result.ok).toBe(true);
    expect(result.next_stage).toBe("public_chatgpt");
  });

  test("device-auth about-you helper does not click the top-left ChatGPT logo while blurring fields", async () => {
    const result = await runDeviceAuthAboutYouHelper(
      `
        <html>
          <body style="min-height: 100vh; margin: 0;">
            <a
              href="https://chatgpt.com/"
              style="position: absolute; top: 0; left: 0; width: 120px; height: 40px;"
            >
              ChatGPT
            </a>
            <div style="padding-top: 80px;">
              <h1>How old are you?</h1>
              <label for="full-name">Full name</label>
              <input id="full-name" name="name" aria-label="Full name" />
              <label for="age">Age</label>
              <input id="age" inputmode="numeric" aria-label="Age" />
              <button id="submit" type="submit" disabled>Finish creating account</button>
            </div>
            <script>
              const fullName = document.getElementById("full-name");
              const age = document.getElementById("age");
              const submit = document.getElementById("submit");
              const sync = () => {
                submit.disabled =
                  fullName.value.trim().length === 0 || age.value.trim().length === 0;
              };
              fullName.addEventListener("input", sync);
              age.addEventListener("input", sync);
            </script>
          </body>
        </html>
      `,
      {
        full_name: "Dev Astronlab",
        birth_month: "4",
        birth_day: "8",
        birth_year: "1990",
      },
      "https://auth.openai.com/about-you",
    );

    expect(result.ok).toBe(true);
    expect(result.current_url).toBe("https://auth.openai.com/about-you");
    expect(result.name_value).toBe("Dev Astronlab");
    expect(result.age_value).toBe("36");
  });

  test("device-auth about-you submit survives a navigation race into the ChatGPT app shell", async () => {
    const browser = await chromium.launch({
      headless: true,
      channel: "chrome",
    });
    try {
      const page = await browser.newPage();
      await page.route("**/*", async (route) => {
        const url = route.request().url();
        const body = /chatgpt\.com/i.test(url)
          ? `
              <html>
                <body style="min-height: 100vh;">
                  <div>New chat</div>
                  <div>Projects</div>
                </body>
              </html>
            `
          : `
              <html>
                <body style="min-height: 100vh;">
                  <form>
                    <h1>How old are you?</h1>
                    <label for="full-name">Full name</label>
                    <input id="full-name" name="name" value="Dev Astronlab" />
                    <label for="age">Age</label>
                    <input id="age" value="36" />
                    <button id="submit" type="submit">Finish creating account</button>
                  </form>
                  <script>
                    document.getElementById("submit").addEventListener("click", (event) => {
                      event.preventDefault();
                      setTimeout(() => {
                        window.location.href = "https://chatgpt.com/";
                      }, 0);
                    });
                  </script>
                </body>
              </html>
            `;
        await route.fulfill({
          status: 200,
          contentType: "text/html",
          body,
        });
      });
      await page.goto("https://auth.openai.com/about-you");
      const script = await loadWorkflowStepScript(
        deviceAuthWorkflowPath,
        "submit_prepare_signup_about_you",
      );
      const execute = new AsyncFunction("page", "state", "args", script);
      const result = (await execute(page, {}, {})) as Record<string, unknown>;

      expect(result.ok).toBe(true);
      expect(result.next_stage).toBe("app_shell");
      expect(String(result.current_url || "")).toContain(
        "https://chatgpt.com/",
      );
    } finally {
      await browser.close();
    }
  });

  test("ignores unrelated nested six-digit strings when the workflow output has no OTP", async () => {
    const result = await runMinimalStepScript(
      "submit_login_verification_code",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent.</p>
            <input inputmode="numeric" aria-label="Code" />
            <button type="button">Continue</button>
          </body>
        </html>
      `,
      {
        steps: {
          collect_login_verification_artifact: {
            action: {
              ancestor_text:
                "Search results Your OpenAI code is 207750 Enter this temporary verification code to continue: 207750",
              result: {
                result: {
                  output: {
                    ok: true,
                    code: null,
                    message_subject: null,
                  },
                },
              },
            },
          },
        },
      },
    );

    expect(result.ok).toBe(false);
    expect(result.error).toBe("login-verification-code-missing");
  });

  test("waits for a delayed replayed-login OTP transition after clicking continue", async () => {
    const result = await runMinimalStepScript(
      "submit_login_verification_code",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent.</p>
            <input inputmode="numeric" aria-label="Code" />
            <button id="continue" type="button">Continue</button>
            <script>
              const input = document.querySelector("input");
              const button = document.getElementById("continue");
              button.addEventListener("click", () => {
                if (input.value === "111222") {
                  setTimeout(() => {
                    document.body.innerHTML =
                      '<h1>Sign in to Codex with ChatGPT</h1><button>Continue to Codex</button>';
                  }, 2500);
                }
              });
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          collect_login_verification_artifact: {
            action: {
              result: {
                output: {
                  code: "111222",
                },
              },
            },
          },
        },
      },
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("111222");
    expect(result.next_stage).toBe("oauth_consent");
  });
});

describe("original workflow verification helper", () => {
  test("fills the current floating-label about-you layout", async () => {
    const result = await runOriginalAboutYouHelper(
      `
        <html>
          <body style="min-height: 100vh;">
            <div>
              <label style="display:block;">
                <span>Full name</span>
                <input type="text" />
              </label>
              <label style="display:block;">
                <span>Birthday</span>
                <input type="text" value="04/09/2026" />
              </label>
              <button type="submit">Finish creating account</button>
            </div>
          </body>
        </html>
      `,
      {
        full_name: "Dev Astronlab",
        birth_month: "1",
        birth_day: "24",
        birth_year: "1990",
      },
    );

    expect(result.ok).toBe(true);
    expect(result.name_value).toBe("Dev Astronlab");
    expect(result.birthday_value).toBe("01/24/1990");
    expect(result.submit_text).toMatch(/finish creating account/i);
  });

  test("fills the live age-based about-you layout", async () => {
    const result = await runOriginalAboutYouHelper(
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>How old are you?</h1>
            <label for="full-name">Full name</label>
            <input id="full-name" name="name" aria-label="Full name" />
            <label for="age">Age</label>
            <input id="age" inputmode="numeric" aria-label="Age" />
            <button id="submit" type="submit" disabled>Finish creating account</button>
            <script>
              const fullName = document.getElementById("full-name");
              const age = document.getElementById("age");
              const submit = document.getElementById("submit");
              const sync = () => {
                submit.disabled =
                  fullName.value.trim().length === 0 || age.value.trim().length === 0;
              };
              fullName.addEventListener("input", sync);
              age.addEventListener("input", sync);
            </script>
          </body>
        </html>
      `,
      {
        full_name: "Dev Astronlab",
        birth_month: "4",
        birth_day: "8",
        birth_year: "1990",
      },
    );

    expect(result.ok).toBe(true);
    expect(result.name_value).toBe("Dev Astronlab");
    expect(result.age_value).toBe("36");
    expect(result.submit_text).toMatch(/finish creating account/i);
  });

  test("fills segmented signup OTP inputs and advances the flow", async () => {
    const result = await runOriginalStepScript(
      "submit_signup_verification_code",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent.</p>
            <div id="otp" style="display: flex; gap: 8px;">
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
            </div>
            <button id="continue" type="button" disabled>Continue</button>
            <script>
              const inputs = Array.from(document.querySelectorAll("#otp input"));
              const button = document.getElementById("continue");
              const readCode = () => inputs.map((input) => input.value).join("");
              const update = () => {
                const code = readCode();
                button.disabled = code.length !== 6;
              };
              inputs.forEach((input, index) => {
                input.addEventListener("input", () => {
                  if (input.value.length > 1) {
                    input.value = input.value.slice(-1);
                  }
                  update();
                  if (input.value && index + 1 < inputs.length) {
                    inputs[index + 1].focus();
                  }
                });
              });
              button.addEventListener("click", () => {
                if (readCode() === "123456") {
                  document.body.innerHTML =
                    '<h1>Sign in to Codex with ChatGPT</h1><button>Continue to Codex</button>';
                }
              });
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          collect_signup_verification_artifact: {
            action: {
              result: {
                output: {
                  code: "123456",
                },
              },
            },
          },
        },
      },
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("123456");
    expect(result.next_stage).toBe("oauth_consent");
  });

  test("fills otp-named segmented signup OTP inputs and advances the flow", async () => {
    const result = await runOriginalStepScript(
      "submit_signup_verification_code",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent.</p>
            <div id="otp" style="display: flex; gap: 8px;">
              <input name="otp-1" maxlength="1" />
              <input name="otp-2" maxlength="1" />
              <input name="otp-3" maxlength="1" />
              <input name="otp-4" maxlength="1" />
              <input name="otp-5" maxlength="1" />
              <input name="otp-6" maxlength="1" />
            </div>
            <button id="continue" type="button" disabled>Continue</button>
            <script>
              const inputs = Array.from(document.querySelectorAll("#otp input"));
              const button = document.getElementById("continue");
              const readCode = () => inputs.map((input) => input.value).join("");
              const update = () => {
                const code = readCode();
                button.disabled = code.length !== 6;
              };
              inputs.forEach((input, index) => {
                input.addEventListener("input", () => {
                  input.value = String(input.value || "").replace(/\\D+/g, "").slice(-1);
                  update();
                  if (input.value && index + 1 < inputs.length) {
                    inputs[index + 1].focus();
                  }
                });
              });
              button.addEventListener("click", () => {
                if (readCode() === "123456") {
                  document.body.innerHTML =
                    '<h1>Sign in to Codex with ChatGPT</h1><button>Continue to Codex</button>';
                }
              });
              update();
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          collect_signup_verification_artifact: {
            action: {
              result: {
                output: {
                  code: "123456",
                },
              },
            },
          },
        },
      },
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("123456");
    expect(result.next_stage).toBe("oauth_consent");
  });

  test("remembers final add-phone across bounded retries from stage/url evidence alone", async () => {
    const result = await runWorkflowRunScript(
      originalWorkflowPath,
      "finalize_flow_summary",
      {
        email: "devbench.25@astronlab.com",
      },
      {
        steps: {
          classify_before_consent: {
            action: {
              stage: "add_phone",
              current_url: "https://auth.openai.com/add-phone",
              headline: "Phone number required",
            },
          },
          classify_before_consent_retry_1: {
            action: {
              stage: "login_email",
              current_url: "https://auth.openai.com/log-in",
              headline: "Welcome back",
              auth_prompt: true,
            },
          },
          complete_login_or_consent: {
            action: {
              stage: "login_email",
              current_url: "https://auth.openai.com/log-in",
              headline: "Welcome back",
              auth_prompt: true,
            },
          },
        },
      },
    );

    expect(result.next_action).toBe("skip_account");
    expect(result.replay_reason).toBe("add_phone");
    expect(result.error_message).toContain("phone setup");
  });

  test("fills segmented replayed-login OTP inputs and advances the flow", async () => {
    const result = await runOriginalStepScript(
      "submit_login_verification_code",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent.</p>
            <div id="otp" style="display: flex; gap: 8px;">
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
            </div>
            <button id="continue" type="button" disabled>Continue</button>
            <script>
              const inputs = Array.from(document.querySelectorAll("#otp input"));
              const button = document.getElementById("continue");
              const readCode = () => inputs.map((input) => input.value).join("");
              const update = () => {
                const code = readCode();
                button.disabled = code.length !== 6;
              };
              inputs.forEach((input, index) => {
                input.addEventListener("input", () => {
                  if (input.value.length > 1) {
                    input.value = input.value.slice(-1);
                  }
                  update();
                  if (input.value && index + 1 < inputs.length) {
                    inputs[index + 1].focus();
                  }
                });
              });
              button.addEventListener("click", () => {
                if (readCode() === "654321") {
                  document.body.innerHTML =
                    '<h1>Sign in to Codex with ChatGPT</h1><button>Continue to Codex</button>';
                }
              });
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          collect_login_verification_artifact: {
            action: {
              result: {
                output: {
                  code: "654321",
                },
              },
            },
          },
        },
      },
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("654321");
    expect(result.next_stage).toBe("oauth_consent");
  });

  test("waits for a delayed replayed-login OTP transition after clicking continue", async () => {
    const result = await runOriginalStepScript(
      "submit_login_verification_code",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent.</p>
            <input inputmode="numeric" aria-label="Code" />
            <button id="continue" type="button">Continue</button>
            <script>
              const input = document.querySelector("input");
              const button = document.getElementById("continue");
              button.addEventListener("click", () => {
                if (input.value === "111222") {
                  setTimeout(() => {
                    document.body.innerHTML =
                      '<h1>Sign in to Codex with ChatGPT</h1><button>Continue to Codex</button>';
                  }, 2500);
                }
              });
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          collect_login_verification_artifact: {
            action: {
              result: {
                output: {
                  code: "111222",
                },
              },
            },
          },
        },
      },
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("111222");
    expect(result.next_stage).toBe("oauth_consent");
  });

  test("fills otp-named segmented replayed-login OTP inputs and advances the flow", async () => {
    const result = await runOriginalStepScript(
      "submit_login_verification_code",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent.</p>
            <div id="otp" style="display: flex; gap: 8px;">
              <input name="otp-1" maxlength="1" />
              <input name="otp-2" maxlength="1" />
              <input name="otp-3" maxlength="1" />
              <input name="otp-4" maxlength="1" />
              <input name="otp-5" maxlength="1" />
              <input name="otp-6" maxlength="1" />
            </div>
            <button id="continue" type="button" disabled>Continue</button>
            <script>
              const inputs = Array.from(document.querySelectorAll("#otp input"));
              const button = document.getElementById("continue");
              const readCode = () => inputs.map((input) => input.value).join("");
              const sync = () => {
                button.disabled = readCode().length < 6;
              };
              inputs.forEach((input, index) => {
                input.addEventListener("input", () => {
                  input.value = String(input.value || "").replace(/\\D+/g, "").slice(-1);
                  sync();
                  if (input.value && index + 1 < inputs.length) {
                    inputs[index + 1].focus();
                  }
                });
              });
              button.addEventListener("click", () => {
                if (readCode() === "111222") {
                  document.body.innerHTML =
                    '<h1>Sign in to Codex with ChatGPT</h1><button>Continue to Codex</button>';
                }
              });
              sync();
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          collect_login_verification_artifact: {
            action: {
              result: {
                output: {
                  code: "111222",
                },
              },
            },
          },
        },
      },
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("111222");
    expect(result.next_stage).toBe("oauth_consent");
  });

  test("original reports a replayed-login OTP rejection as structured workflow state", async () => {
    const result = await runOriginalStepScript(
      "submit_login_verification_code",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent.</p>
            <input inputmode="numeric" aria-label="Code" />
            <button id="continue" type="button">Continue</button>
            <script>
              const input = document.querySelector("input");
              document.getElementById("continue").addEventListener("click", () => {
                if (input.value === "654321") {
                  document.body.innerHTML =
                    '<h1>Check your inbox</h1><p>Incorrect code. Use the newest code.</p><input inputmode="numeric" aria-label="Code" /><button type="button">Continue</button>';
                }
              });
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          collect_login_verification_artifact: {
            action: {
              result: {
                output: {
                  code: "654321",
                },
              },
            },
          },
        },
      },
    );

    expect(result.ok).toBe(false);
    expect(result.error).toBe("login-verification-code-rejected");
    expect(result.stage).toBe("email_verification");
    expect(result.incorrect_code).toBe(true);
  });

  test("original uses a recollected replayed-login OTP on the second in-workflow submit", async () => {
    const result = await runOriginalStepScript(
      "submit_login_verification_code_after_submit_failure",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent.</p>
            <div id="otp" style="display: flex; gap: 8px;">
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
            </div>
            <button id="continue" type="button" disabled>Continue</button>
            <script>
              const inputs = Array.from(document.querySelectorAll("#otp input"));
              const button = document.getElementById("continue");
              const readCode = () => inputs.map((input) => input.value).join("");
              const sync = () => {
                button.disabled = readCode().length < 6;
              };
              inputs.forEach((input) => {
                input.addEventListener("input", () => {
                  input.value = String(input.value || "").replace(/\\D+/g, "").slice(-1);
                  sync();
                });
              });
              button.addEventListener("click", () => {
                if (readCode() === "111222") {
                  document.body.innerHTML =
                    '<h1>Sign in to Codex with ChatGPT</h1><button>Continue to Codex</button>';
                }
              });
              sync();
            </script>
          </body>
        </html>
      `,
      {
        vars: {
          login_verification_code_retry: "111222",
        },
        steps: {
          recollect_login_verification_artifact_after_submit_failure: {
            action: {
              result: {
                output: {
                  code: "111222",
                },
              },
            },
          },
        },
      },
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("111222");
    expect(result.next_stage).toBe("oauth_consent");
  });
});

describe("stepwise workflow verification helper", () => {
  test("minimal login email submit waits for the password transition after continue", async () => {
    const result = await runWorkflowFunctionScriptOnContent(
      minimalWorkflowPath,
      "submit_login_email_form",
      `
        <html>
          <body>
            <form id="login-form">
              <label>
                Email
                <input
                  type="email"
                  name="email"
                  autocomplete="email"
                  value="dev3astronlab+1@gmail.com"
                />
              </label>
              <button type="submit">Continue</button>
            </form>
            <script>
              document.getElementById("login-form").addEventListener("submit", (event) => {
                event.preventDefault();
                setTimeout(() => {
                  history.replaceState({}, "", "/log-in/password");
                  document.body.innerHTML =
                    '<form><label>Password<input type="password" name="password" autocomplete="current-password" /></label><button type="submit">Log in</button></form>';
                }, 100);
              });
            </script>
          </body>
        </html>
      `,
      {
        email: "dev3astronlab+1@gmail.com",
      },
      "https://auth.openai.com/log-in",
    );

    expect(result.ok).toBe(true);
    expect(result.next_stage).toBe("login_password");
    expect(String(result.current_url || "")).toContain("/log-in/password");
  });

  test("non-device replay-login gates stay aligned with ref-based password and stage-based verification routing", () => {
    const workflows = [
      readFileSync(originalWorkflowPath, "utf8"),
      readFileSync(minimalWorkflowPath, "utf8"),
      readFileSync(stepwiseWorkflowPath, "utf8"),
    ];

    for (const workflowText of workflows) {
      expect(workflowText).toContain("account_login_ref");
      expect(workflowText).toContain(
        "state.steps.classify_after_login_email_gate?.action?.stage === 'login_password' && inputs.account_login_ref != null",
      );
      expect(workflowText).toContain(
        "state.steps.classify_after_login_email_gate?.action?.stage === 'login_password' && inputs.account_login_ref == null",
      );
      expect(workflowText).toContain(
        "state.steps.classify_after_login_password_gate?.action?.stage === 'email_verification'",
      );
      expect(workflowText).toContain(
        "templateRef: fill_generated_openai_password_field",
      );
      expect(workflowText).not.toContain(
        "state.steps.classify_after_login_password_gate?.action?.needs_email_verification === true",
      );
    }

    const stepwiseWorkflowText = readFileSync(stepwiseWorkflowPath, "utf8");
    const minimalWorkflowText = readFileSync(minimalWorkflowPath, "utf8");
    expect(stepwiseWorkflowText).toContain(
      'reason: "forgot-password-link-not-found-or-not-on-password-page"',
    );
    expect(stepwiseWorkflowText).toContain(
      "state.steps.classify_after_signup_email_gate?.action?.stage === 'signup_password'",
    );
    expect(stepwiseWorkflowText).not.toContain(
      "state.steps.classify_after_signup_email_gate?.action?.stage === 'signup_password' || state.steps.classify_after_signup_email_gate?.action?.stage === 'login_password'",
    );
    expect(minimalWorkflowText).toContain(
      "state.steps.classify_after_signup_email_gate?.action?.stage === 'signup_password'",
    );
    expect(minimalWorkflowText).not.toContain(
      "state.steps.classify_after_signup_email_gate?.action?.stage === 'signup_password' || state.steps.classify_after_signup_email_gate?.action?.stage === 'login_password'",
    );
  });

  test("submits signup OTPs from segmented otp-named inputs", async () => {
    const result = await runWorkflowStepScriptOnContent(
      stepwiseWorkflowPath,
      "submit_signup_verification_code",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent.</p>
            <div id="otp" style="display: flex; gap: 8px;">
              <input name="otp-1" maxlength="1" />
              <input name="otp-2" maxlength="1" />
              <input name="otp-3" maxlength="1" />
              <input name="otp-4" maxlength="1" />
              <input name="otp-5" maxlength="1" />
              <input name="otp-6" maxlength="1" />
            </div>
            <button id="continue" type="button" disabled>Continue</button>
            <script>
              const inputs = Array.from(document.querySelectorAll("#otp input"));
              const button = document.getElementById("continue");
              const readCode = () => inputs.map((input) => input.value).join("");
              const sync = () => {
                button.disabled = readCode().length < 6;
              };
              inputs.forEach((input, index) => {
                input.addEventListener("input", () => {
                  input.value = String(input.value || "").replace(/\\D+/g, "").slice(-1);
                  sync();
                  if (input.value && index + 1 < inputs.length) {
                    inputs[index + 1].focus();
                  }
                });
              });
              button.addEventListener("click", () => {
                if (readCode() === "380393") {
                  document.body.innerHTML =
                    '<h1>What should we call you?</h1><label for="full-name">Full name</label><input id="full-name" name="name" aria-label="Full name" />';
                }
              });
              sync();
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          collect_signup_verification_artifact: {
            action: {
              result: {
                output: {
                  code: "380393",
                },
              },
            },
          },
        },
      },
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("380393");
    expect(result.next_stage).toBe("about_you");
  });

  test("reports a replayed-login OTP rejection as structured workflow state", async () => {
    const result = await runWorkflowStepScriptOnContent(
      stepwiseWorkflowPath,
      "submit_login_verification_code",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent.</p>
            <input inputmode="numeric" aria-label="Code" />
            <button id="continue" type="button">Continue</button>
            <script>
              const input = document.querySelector("input");
              document.getElementById("continue").addEventListener("click", () => {
                if (input.value === "654321") {
                  document.body.innerHTML =
                    '<h1>Check your inbox</h1><p>Incorrect code. Use the newest code.</p><input inputmode="numeric" aria-label="Code" /><button type="button">Continue</button>';
                }
              });
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          collect_login_verification_artifact: {
            action: {
              result: {
                output: {
                  code: "654321",
                },
              },
            },
          },
        },
      },
    );

    expect(result.ok).toBe(false);
    expect(result.error).toBe("login-verification-code-rejected");
    expect(result.stage).toBe("email_verification");
    expect(result.incorrect_code).toBe(true);
  });

  test("uses a recollected replayed-login OTP on the second in-workflow submit", async () => {
    const result = await runWorkflowStepScriptOnContent(
      stepwiseWorkflowPath,
      "submit_login_verification_code_after_submit_failure",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent.</p>
            <div id="otp" style="display: flex; gap: 8px;">
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
              <input inputmode="numeric" maxlength="1" />
            </div>
            <button id="continue" type="button" disabled>Continue</button>
            <script>
              const inputs = Array.from(document.querySelectorAll("#otp input"));
              const button = document.getElementById("continue");
              const readCode = () => inputs.map((input) => input.value).join("");
              const sync = () => {
                button.disabled = readCode().length < 6;
              };
              inputs.forEach((input) => {
                input.addEventListener("input", () => {
                  input.value = String(input.value || "").replace(/\\D+/g, "").slice(-1);
                  sync();
                });
              });
              button.addEventListener("click", () => {
                if (readCode() === "111222") {
                  document.body.innerHTML =
                    '<h1>Sign in to Codex with ChatGPT</h1><button>Continue to Codex</button>';
                }
              });
              sync();
            </script>
          </body>
        </html>
      `,
      {
        vars: {
          login_verification_code_retry: "111222",
        },
        steps: {
          recollect_login_verification_artifact_after_submit_failure: {
            action: {
              result: {
                output: {
                  code: "111222",
                },
              },
            },
          },
        },
      },
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("111222");
    expect(result.next_stage).toBe("oauth_consent");
  });

  test("stepwise retries the same account when bounded final add-phone bounces back to login", async () => {
    const result = await runWorkflowRunScript(
      stepwiseWorkflowPath,
      "finalize_flow_summary",
      {
        email: "devbench.15@astronlab.com",
      },
      {
        vars: {
          effective_before_consent_surface: {
            add_phone_prompt: true,
            current_url: "https://auth.openai.com/add-phone",
          },
        },
        steps: {
          complete_login_or_consent: {
            action: {
              stage: "login_email",
              current_url: "https://auth.openai.com/log-in",
              headline: "Welcome back",
              auth_prompt: true,
            },
          },
        },
      },
    );

    expect(result.next_action).toBe("retry_attempt");
    expect(result.retry_reason).toBe("final_add_phone");
    expect(result.error_message).toContain("phone setup");
  });

  test("stepwise remembers final add-phone across bounded retries before retrying the same account", async () => {
    const result = await runWorkflowRunScript(
      stepwiseWorkflowPath,
      "finalize_flow_summary",
      {
        email: "devbench.15@astronlab.com",
      },
      {
        steps: {
          classify_before_consent: {
            action: {
              add_phone_prompt: true,
              stage: "add_phone",
              current_url: "https://auth.openai.com/add-phone",
            },
          },
          classify_before_consent_retry_1: {
            action: {
              stage: "login_email",
              current_url: "https://auth.openai.com/log-in",
              headline: "Welcome back",
              auth_prompt: true,
            },
          },
          complete_login_or_consent: {
            action: {
              stage: "login_email",
              current_url: "https://auth.openai.com/log-in",
              headline: "Welcome back",
              auth_prompt: true,
            },
          },
        },
      },
    );

    expect(result.next_action).toBe("retry_attempt");
    expect(result.retry_reason).toBe("final_add_phone");
    expect(result.error_message).toContain("phone setup");
  });

  test("original retries the same account when bounded final add-phone bounces back to login", async () => {
    const result = await runWorkflowRunScript(
      originalWorkflowPath,
      "finalize_flow_summary",
      {
        email: "devbench.15@astronlab.com",
      },
      {
        vars: {
          effective_before_consent_surface: {
            add_phone_prompt: true,
            current_url: "https://auth.openai.com/add-phone",
          },
        },
        steps: {
          complete_login_or_consent: {
            action: {
              stage: "login_email",
              current_url: "https://auth.openai.com/log-in",
              headline: "Welcome back",
              auth_prompt: true,
            },
          },
        },
      },
    );

    expect(result.next_action).toBe("retry_attempt");
    expect(result.retry_reason).toBe("final_add_phone");
    expect(result.error_message).toContain("phone setup");
  });

  test("minimal retries the same account when bounded final add-phone bounces back to login", async () => {
    const result = await runWorkflowRunScript(
      minimalWorkflowPath,
      "finalize_flow_summary",
      {
        email: "devbench.15@astronlab.com",
      },
      {
        vars: {
          effective_before_consent_surface: {
            add_phone_prompt: true,
            current_url: "https://auth.openai.com/add-phone",
          },
        },
        steps: {
          complete_login_or_consent: {
            action: {
              stage: "login_email",
              current_url: "https://auth.openai.com/log-in",
              headline: "Welcome back",
              auth_prompt: true,
            },
          },
        },
      },
    );

    expect(result.next_action).toBe("retry_attempt");
    expect(result.retry_reason).toBe("final_add_phone");
    expect(result.error_message).toContain("phone setup");
  });

  test("non-device flows keep remembered final add-phone authoritative over generic retryable timeout metadata", async () => {
    for (const workflowPath of [minimalWorkflowPath, stepwiseWorkflowPath]) {
      const result = await runWorkflowRunScript(
        workflowPath,
        "finalize_flow_summary",
        {
          email: "dev3astronlab+6@gmail.com",
        },
        {
          vars: {
            effective_before_consent_surface: {
              add_phone_prompt: true,
              current_url: "https://auth.openai.com/add-phone",
            },
          },
          steps: {
            complete_login_or_consent: {
              action: {
                stage: "retryable_timeout",
                retryable_timeout: true,
                current_url: "https://auth.openai.com/log-in",
                headline: "Oops, an error occurred!",
                auth_prompt: true,
              },
            },
          },
        },
      );

      expect(result.next_action).toBe("retry_attempt");
      expect(result.retry_reason).toBe("final_add_phone");
      expect(result.error_message).toContain("phone setup");
    }
  });

  test("non-device flows keep about-you replay authoritative over generic retryable timeout metadata", async () => {
    for (const workflowPath of [
      originalWorkflowPath,
      minimalWorkflowPath,
      stepwiseWorkflowPath,
    ]) {
      const result = await runWorkflowRunScript(
        workflowPath,
        "finalize_flow_summary",
        {
          email: "dev3astronlab+5@gmail.com",
        },
        {
          steps: {
            classify_after_login_about_you: {
              action: {
                stage: "about_you",
                follow_up_step: true,
                current_url: "https://auth.openai.com/about-you",
                headline: "About you",
              },
            },
            complete_login_or_consent: {
              action: {
                stage: "retryable_timeout",
                retryable_timeout: true,
                current_url: "https://auth.openai.com/about-you",
                headline: "About you",
              },
            },
          },
        },
      );

      expect(result.next_action).toBe("replay_auth_url");
      expect(result.replay_reason).toBe("about_you");
      expect(result.retry_reason).toBeNull();
      expect(result.error_message).toContain("account setup");
    }
  });

  test("non-device flows keep replayed auth prompts authoritative over generic retryable timeout metadata", async () => {
    for (const workflowPath of [minimalWorkflowPath, stepwiseWorkflowPath]) {
      const result = await runWorkflowRunScript(
        workflowPath,
        "finalize_flow_summary",
        {
          email: "dev3astronlab+5@gmail.com",
        },
        {
          steps: {
            complete_login_or_consent: {
              action: {
                stage: "retryable_timeout",
                retryable_timeout: true,
                current_url: "https://auth.openai.com/log-in/password",
                headline: "Welcome back",
              },
            },
          },
        },
      );

      expect(result.next_action).toBe("replay_auth_url");
      expect(result.replay_reason).toBe("auth_prompt");
      expect(result.retry_reason).toBeNull();
      expect(result.error_message).toContain("auth prompt");
    }
  });

  test("stepwise keeps reset-password recovery on the primary non-device replay path", async () => {
    const result = await runWorkflowRunScript(
      stepwiseWorkflowPath,
      "finalize_flow_summary",
      {
        email: "dev3astronlab+5@gmail.com",
      },
      {
        steps: {
          complete_login_or_consent: {
            action: {
              current_url: "https://auth.openai.com/reset-password",
              headline: "Reset password",
              reset_password_prompt: true,
            },
          },
        },
      },
    );

    expect(result.next_action).toBe("replay_auth_url");
    expect(result.replay_reason).toBe("reset_password");
    expect(result.retry_reason).toBeNull();
    expect(result.error_message).toContain("password-reset recovery");
  });

  test("stepwise keeps reset-password recovery authoritative over a retryable-timeout overlay on the reset page", async () => {
    const result = await runWorkflowRunScript(
      stepwiseWorkflowPath,
      "finalize_flow_summary",
      {
        email: "dev3astronlab+5@gmail.com",
      },
      {
        steps: {
          complete_login_or_consent: {
            action: {
              current_url: "https://auth.openai.com/reset-password",
              headline: "Oops, an error occurred!",
              reset_password_prompt: true,
              retryable_timeout: true,
              stage: "retryable_timeout",
            },
          },
        },
      },
    );

    expect(result.next_action).toBe("replay_auth_url");
    expect(result.replay_reason).toBe("reset_password");
    expect(result.retry_reason).toBeNull();
  });

  test("non-device flows retry the replayed about-you form in-place before falling back to full auth replay", async () => {
    for (const workflowPath of [
      originalWorkflowPath,
      minimalWorkflowPath,
      stepwiseWorkflowPath,
    ]) {
      const workflowText = readFileSync(workflowPath, "utf8");
      expect(workflowText).toContain("fill_login_about_you_retry");
      expect(workflowText).toContain("submit_login_about_you_retry");
      expect(workflowText).toContain("classify_after_login_about_you_retry");
      expect(workflowText).toContain("loginAboutYouRetry.current_url");
      expect(workflowText).toContain("click_login_about_you_timeout_retry");
      expect(workflowText).toContain(
        "classify_after_login_about_you_timeout_retry",
      );
    }
  });

  test("non-device flows never route replayed about-you recovery on create-account password pages", async () => {
    for (const workflowPath of [
      originalWorkflowPath,
      minimalWorkflowPath,
      stepwiseWorkflowPath,
    ]) {
      const workflowText = readFileSync(workflowPath, "utf8");
      expect(workflowText).toContain(
        "!/\\\\/create-account(?:[/?#]|$)/i.test(",
      );
      expect(workflowText).toContain("fill_login_about_you");
      expect(workflowText).toContain("fill_login_about_you_retry");
    }
  });

  test("minimal retries signup about-you in-place before replaying the auth url", () => {
    const workflowText = readFileSync(minimalWorkflowPath, "utf8");
    expect(workflowText).toContain("click_signup_about_you_timeout_retry");
    expect(workflowText).toContain("fill_signup_about_you_retry");
    expect(workflowText).toContain("submit_signup_about_you_retry");
    expect(workflowText).toContain("classify_after_signup_about_you_retry");
  });
});

describe("gmail account capture workflow", () => {
  test("accepts a modern Gmail inbox shell when title and GLOBALS expose the active email", async () => {
    const result = await runWorkflowStepScript(
      gmailCaptureWorkflowPath,
      "scan_gmail_slots",
      `
        <html>
          <head>
            <title>Inbox (54) - 1.dev.astronlab@gmail.com - Gmail</title>
            <script>
              window.GLOBALS = ["1.dev.astronlab@gmail.com"];
            </script>
          </head>
          <body>
            <div>Inbox</div>
            <table role="main">
              <tr class="zA"><td>message row</td></tr>
            </table>
          </body>
        </html>
      `,
      {
        steps: {
          clear_google_auth_gate: {
            action: {
              result: {
                state: {
                  steps: {
                    classify_auth_gate_state: {
                      action: {
                        auth_cleared: true,
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        enabled: true,
        preferred_email: "1.dev.astronlab@gmail.com",
      },
      "https://mail.google.com/mail/u/0/#inbox",
    );

    expect(result.ok).toBe(true);
    expect(result.email).toBe("1.dev.astronlab@gmail.com");
    expect(result.gmail_shell_ready).toBe(true);
    expect(result.failure_reason).toBe(null);
  });

  test("does not treat the Google Workspace loading shell as a ready mailbox", async () => {
    const result = await runWorkflowStepScript(
      gmailCaptureWorkflowPath,
      "scan_gmail_slots",
      `
        <html>
          <head>
            <title>Inbox (54) - 1.dev.astronlab@gmail.com - Gmail</title>
            <script>
              window.GLOBALS = ["1.dev.astronlab@gmail.com"];
            </script>
          </head>
          <body>
            <div>Google Workspace</div>
            <a href="https://support.google.com/mail">Gmail help center</a>
          </body>
        </html>
      `,
      {
        steps: {
          clear_google_auth_gate: {
            action: {
              result: {
                state: {
                  steps: {
                    classify_auth_gate_state: {
                      action: {
                        auth_cleared: true,
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        enabled: true,
        preferred_email: "1.dev.astronlab@gmail.com",
      },
      "https://mail.google.com/mail/u/0/#inbox",
    );

    expect(result.ok).toBe(true);
    expect(result.email).toBe("1.dev.astronlab@gmail.com");
    expect(result.gmail_shell_ready).toBe(false);
    expect(result.failure_reason).toBe("gmail_shell_not_ready");
  });
});

describe("gmail verification artifact workflow", () => {
  test("waits for the Gmail app shell to finish loading before searching for the OTP", async () => {
    const result = await runWorkflowStepScriptOnContent(
      gmailVerificationWorkflowPath,
      "collect_verification_artifact",
      `
        <html>
          <head>
            <title>Gmail</title>
          </head>
          <body>
            <div>Google Workspace</div>
            <a href="https://support.google.com/mail">Gmail help center</a>
            <script>
              setTimeout(() => {
                document.title = "Search results - 1.dev.astronlab@gmail.com - Gmail";
                document.body.innerHTML =
                  '<input aria-label="Search mail" name="q" />' +
                  '<table role="main">' +
                    '<tr class="zA">' +
                      '<td>noreply Your ChatGPT code is 112233 Enter this temporary verification code to continue: 112233</td>' +
                    '</tr>' +
                  '</table>';
              }, 900);
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          ensure_gmail_shell: {
            action: {
              result: {
                state: {
                  steps: {
                    capture_active_account: {
                      action: {
                        skipped: false,
                        email: "1.dev.astronlab@gmail.com",
                        gmail_shell_ready: false,
                        needs_google_login: false,
                        failure_reason: null,
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        enabled: true,
        search_query: "from:openai to:dev.48@astronlab.com newer_than:7d",
        message_match_text: "dev.48@astronlab.com",
      },
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("112233");
    expect(result.selected_email).toBe("1.dev.astronlab@gmail.com");
    expect(result.gmail_shell_ready).toBe(true);
  });

  test("still waits for a slow Gmail shell even when account capture already knows the email", async () => {
    const result = await runWorkflowStepScript(
      gmailVerificationWorkflowPath,
      "collect_verification_artifact",
      `
        <html>
          <head>
            <title>Gmail</title>
          </head>
          <body>
            <div>Google Workspace</div>
            <a href="https://support.google.com/mail">Gmail help center</a>
            <script>
              setTimeout(() => {
                document.title = "Search results - 1.dev.astronlab@gmail.com - Gmail";
                document.body.innerHTML =
                  '<input aria-label="Search mail" name="q" />' +
                  '<table role="main">' +
                    '<tr class="zA">' +
                      '<td>noreply Your ChatGPT code is 445566 Enter this temporary verification code to continue: 445566</td>' +
                    '</tr>' +
                  '</table>';
              }, 6500);
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          ensure_gmail_shell: {
            action: {
              result: {
                state: {
                  steps: {
                    capture_active_account: {
                      action: {
                        skipped: false,
                        email: "1.dev.astronlab@gmail.com",
                        gmail_shell_ready: true,
                        needs_google_login: false,
                        failure_reason: null,
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        enabled: true,
        search_query: "from:openai to:dev.49@astronlab.com newer_than:7d",
        message_match_text: "dev.49@astronlab.com",
      },
      "https://mail.google.com/mail/u/0/#search/from%3Aopenai",
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("445566");
    expect(result.selected_email).toBe("1.dev.astronlab@gmail.com");
    expect(result.gmail_shell_ready).toBe(true);
  });

  test("does not extract OTPs from hidden stale rows while Gmail is still loading", async () => {
    const result = await runWorkflowStepScriptOnContent(
      gmailVerificationWorkflowPath,
      "collect_verification_artifact",
      `
        <html>
          <head>
            <title>Gmail</title>
          </head>
          <body>
            <div>Google Workspace</div>
            <a href="https://support.google.com/mail">Gmail help center</a>
            <table role="main" style="display: none;">
              <tr class="zA">
                <td>noreply Your ChatGPT code is 998877 Enter this temporary verification code to continue: 998877</td>
              </tr>
            </table>
            <script>
              setTimeout(() => {
                document.title = "Search results - 1.dev.astronlab@gmail.com - Gmail";
                document.body.innerHTML =
                  '<input aria-label="Search mail" name="q" />' +
                  '<div>No messages matched your search</div>' +
                  '<table role="main" style="display: none;">' +
                    '<tr class="zA">' +
                      '<td>noreply Your ChatGPT code is 998877 Enter this temporary verification code to continue: 998877</td>' +
                    '</tr>' +
                  '</table>';
              }, 900);
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          ensure_gmail_shell: {
            action: {
              result: {
                state: {
                  steps: {
                    capture_active_account: {
                      action: {
                        skipped: false,
                        email: "1.dev.astronlab@gmail.com",
                        gmail_shell_ready: false,
                        needs_google_login: false,
                        failure_reason: "gmail_shell_not_ready",
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        enabled: true,
        search_query: "from:openai to:dev.49@astronlab.com newer_than:7d",
        message_match_text: "dev.49@astronlab.com",
      },
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe(null);
    expect(result.gmail_shell_ready).toBe(true);
    expect(result.failure_reason).toBe(null);
  });

  test("falls back to the matched row preview when the opened Gmail body omits the OTP", async () => {
    const result = await runWorkflowStepScript(
      gmailVerificationWorkflowPath,
      "collect_verification_artifact",
      `
        <html>
          <head>
            <title>Search results - 1.dev.astronlab@gmail.com - Gmail</title>
          </head>
          <body>
            <input aria-label="Search mail" name="q" />
            <table role="main">
              <tr
                class="zA"
                onclick="
                  document.body.innerHTML =
                    '<input aria-label=&quot;Search mail&quot; name=&quot;q&quot; />' +
                    '<h2>OpenAI verification</h2>' +
                    '<div class=&quot;a3s aiL&quot;>Welcome back to ChatGPT. Continue in the app.</div>';
                "
              >
                <td>
                  noreply Your ChatGPT code is 046520 Enter this temporary verification code to continue: 046520
                </td>
              </tr>
            </table>
          </body>
        </html>
      `,
      {
        steps: {
          ensure_gmail_shell: {
            action: {
              result: {
                state: {
                  steps: {
                    capture_active_account: {
                      action: {
                        skipped: false,
                        email: "1.dev.astronlab@gmail.com",
                        gmail_shell_ready: true,
                        needs_google_login: false,
                        failure_reason: null,
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        enabled: true,
        search_query: "from:openai to:dev.48@astronlab.com newer_than:7d",
        message_match_text: "dev.48@astronlab.com",
      },
      "https://mail.google.com/mail/u/0/#search/from%3Aopenai",
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("046520");
    expect(result.selected_email).toBe("1.dev.astronlab@gmail.com");
    expect(result.gmail_shell_ready).toBe(true);
  });

  test("waits for the opened Gmail message body when the selected row preview has no OTP yet", async () => {
    const result = await runWorkflowStepScript(
      gmailVerificationWorkflowPath,
      "collect_verification_artifact",
      `
        <html>
          <head>
            <title>Search results - 1.dev.astronlab@gmail.com - Gmail</title>
          </head>
          <body>
            <input aria-label="Search mail" name="q" />
            <table role="main">
              <tr
                class="zA"
                onclick="
                  document.body.innerHTML =
                    '<input aria-label=&quot;Search mail&quot; name=&quot;q&quot; />' +
                    '<div id=&quot;spinner&quot;>Loading…</div>';
                  setTimeout(() => {
                    document.body.innerHTML =
                      '<input aria-label=&quot;Search mail&quot; name=&quot;q&quot; />' +
                      '<h2>OpenAI verification</h2>' +
                      '<div class=&quot;a3s aiL&quot;>Enter this temporary verification code to continue: 731902</div>';
                  }, 2600);
                "
              >
                <td>dev.101@astronlab.com OpenAI verification message</td>
              </tr>
            </table>
          </body>
        </html>
      `,
      {
        steps: {
          ensure_gmail_shell: {
            action: {
              result: {
                state: {
                  steps: {
                    capture_active_account: {
                      action: {
                        skipped: false,
                        email: "1.dev.astronlab@gmail.com",
                        gmail_shell_ready: true,
                        needs_google_login: false,
                        failure_reason: null,
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        enabled: true,
        search_query: "from:openai to:dev.101@astronlab.com newer_than:7d",
        message_match_text: "dev.101@astronlab.com",
      },
      "https://mail.google.com/mail/u/0/#search/from%3Aopenai",
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("731902");
    expect(result.selected_email).toBe("1.dev.astronlab@gmail.com");
    expect(result.gmail_shell_ready).toBe(true);
  });

  test("does not treat a loading preview pane as a ready Gmail message body", async () => {
    const result = await runWorkflowStepScript(
      gmailVerificationWorkflowPath,
      "collect_verification_artifact",
      `
        <html>
          <head>
            <title>Search results - 1.dev.astronlab@gmail.com - Gmail</title>
          </head>
          <body>
            <input aria-label="Search mail" name="q" />
            <table role="main">
              <tr
                class="zA"
                onclick="
                  const preview = document.getElementById('preview-pane');
                  if (preview) {
                    preview.innerHTML = '<div class=&quot;a3s aiL&quot;>Loading…</div>';
                    setTimeout(() => {
                      preview.innerHTML =
                        '<h2>OpenAI verification</h2>' +
                        '<div class=&quot;a3s aiL&quot;>Enter this temporary verification code to continue: 842197</div>';
                    }, 2600);
                  }
                "
              >
                <td>dev.101@astronlab.com OpenAI verification message</td>
              </tr>
            </table>
            <div id="preview-pane"></div>
          </body>
        </html>
      `,
      {
        steps: {
          ensure_gmail_shell: {
            action: {
              result: {
                state: {
                  steps: {
                    capture_active_account: {
                      action: {
                        skipped: false,
                        email: "1.dev.astronlab@gmail.com",
                        gmail_shell_ready: true,
                        needs_google_login: false,
                        failure_reason: null,
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        enabled: true,
        search_query: "from:openai to:dev.101@astronlab.com newer_than:7d",
        message_match_text: "dev.101@astronlab.com",
      },
      "https://mail.google.com/mail/u/0/#search/from%3Aopenai",
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("842197");
    expect(result.selected_email).toBe("1.dev.astronlab@gmail.com");
    expect(result.gmail_shell_ready).toBe(true);
  });

  test("forces a full Gmail thread open when the preview pane never yields verification content", async () => {
    const result = await runWorkflowStepScript(
      gmailVerificationWorkflowPath,
      "collect_verification_artifact",
      `
        <html>
          <head>
            <title>Search results - 1.dev.astronlab@gmail.com - Gmail</title>
          </head>
          <body>
            <input aria-label="Search mail" name="q" />
            <table role="main">
              <tr
                class="zA"
                tabindex="0"
                onclick="
                  document.getElementById('preview-pane').innerHTML =
                    '<div class=&quot;a3s aiL&quot;>Loading...</div>';
                "
                ondblclick="
                  document.body.innerHTML =
                    '<input aria-label=&quot;Search mail&quot; name=&quot;q&quot; />' +
                    '<h2>OpenAI verification</h2>' +
                    '<div class=&quot;a3s aiL&quot;>Enter this temporary verification code to continue: 661204</div>';
                "
              >
                <td>dev.101@astronlab.com OpenAI verification message</td>
              </tr>
            </table>
            <div id="preview-pane"></div>
          </body>
        </html>
      `,
      {
        steps: {
          ensure_gmail_shell: {
            action: {
              result: {
                state: {
                  steps: {
                    capture_active_account: {
                      action: {
                        skipped: false,
                        email: "1.dev.astronlab@gmail.com",
                        gmail_shell_ready: true,
                        needs_google_login: false,
                        failure_reason: null,
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        enabled: true,
        search_query: "from:openai to:dev.101@astronlab.com newer_than:7d",
        message_match_text: "dev.101@astronlab.com",
      },
      "https://mail.google.com/mail/u/0/#search/from%3Aopenai",
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("661204");
    expect(result.selected_email).toBe("1.dev.astronlab@gmail.com");
    expect(result.gmail_shell_ready).toBe(true);
  });

  test("prefers the selected row preview OTP over an older code later in the opened Gmail thread", async () => {
    const result = await runWorkflowStepScript(
      gmailVerificationWorkflowPath,
      "collect_verification_artifact",
      `
        <html>
          <head>
            <title>Search results - 1.dev.astronlab@gmail.com - Gmail</title>
          </head>
          <body>
            <input aria-label="Search mail" name="q" />
            <table role="main">
              <tr
                class="zA"
                onclick="
                  document.body.innerHTML =
                    '<input aria-label=&quot;Search mail&quot; name=&quot;q&quot; />' +
                    '<h2>OpenAI verification</h2>' +
                    '<div class=&quot;a3s aiL&quot;>' +
                      'Your latest OpenAI verification thread is below. ' +
                      'Current code summary: 140863. ' +
                      'Older code in this thread: 046520.' +
                    '</div>';
                "
              >
                <td>
                  dev.50@astronlab.com OpenAI Your latest verification code is 140863
                </td>
              </tr>
            </table>
          </body>
        </html>
      `,
      {
        steps: {
          ensure_gmail_shell: {
            action: {
              result: {
                state: {
                  steps: {
                    capture_active_account: {
                      action: {
                        skipped: false,
                        email: "1.dev.astronlab@gmail.com",
                        gmail_shell_ready: true,
                        needs_google_login: false,
                        failure_reason: null,
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        enabled: true,
        search_query: "from:openai to:dev.50@astronlab.com newer_than:7d",
        message_match_text: "dev.50@astronlab.com",
      },
      "https://mail.google.com/mail/u/0/#search/from%3Aopenai",
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("140863");
    expect(result.message_preview).toContain("140863");
    expect(result.selected_email).toBe("1.dev.astronlab@gmail.com");
  });

  test("prefers scored verification rows over non-message search header rows", async () => {
    const result = await runWorkflowStepScriptOnContent(
      gmailVerificationWorkflowPath,
      "collect_verification_artifact",
      `
        <html>
          <head>
            <title>Search results - 1.dev.astronlab@gmail.com - Gmail</title>
          </head>
          <body>
            <table role="main">
              <tr role="row">
                <td>dev.48@astronlab.com</td>
              </tr>
              <tr
                class="zA"
                onclick="
                  document.body.innerHTML =
                    '<h2>OpenAI verification</h2>' +
                    '<div class=&quot;a3s aiL&quot;>Continue in the app.</div>';
                "
              >
                <td>
                  noreply Your OpenAI code is 380393 Enter this temporary verification code to continue: 380393
                </td>
              </tr>
              <tr class="zA">
                <td>
                  OpenAI Your ChatGPT code is 803733 Enter this temporary verification code to continue: 803733
                </td>
              </tr>
            </table>
          </body>
        </html>
      `,
      {
        steps: {
          ensure_gmail_shell: {
            action: {
              result: {
                state: {
                  steps: {
                    capture_active_account: {
                      action: {
                        skipped: false,
                        email: "1.dev.astronlab@gmail.com",
                        gmail_shell_ready: true,
                        needs_google_login: false,
                        failure_reason: null,
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        enabled: true,
        search_query: "from:openai to:dev.48@astronlab.com newer_than:7d",
        message_match_text: "dev.48@astronlab.com",
      },
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("380393");
    expect(result.selected_email).toBe("1.dev.astronlab@gmail.com");
    expect(result.gmail_shell_ready).toBe(true);
  });
});

describe("device-auth workflow", () => {
  test("treats create-account password as signup progress after submitting a prepare login email", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "submit_prepare_login_email",
      `
        <html>
          <body style="min-height: 100vh;">
            <form>
              <label for="email">Email address</label>
              <input
                id="email"
                type="email"
                name="email"
                autocomplete="email"
                value="dev.51@astronlab.com"
              />
              <label for="password">Create a password</label>
              <input
                id="password"
                type="password"
                name="new-password"
                autocomplete="new-password"
                value=""
              />
              <button type="submit">Continue</button>
            </form>
          </body>
        </html>
      `,
      {},
      {
        email: "dev.51@astronlab.com",
      },
      "https://auth.openai.com/create-account/password",
    );

    expect(result.ok).toBe(true);
    expect(result.next_stage).toBe("signup_password");
    expect(String(result.current_url || "")).toContain(
      "https://auth.openai.com/create-account/password",
    );
  });

  test("routes prepare auth submit by the effective OpenAI stage instead of a skipped signup click", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const signupSubmitStep = workflow.do?.find(
      (entry) => "submit_prepare_signup_email" in entry,
    )?.submit_prepare_signup_email as { if?: string } | undefined;
    const submitStep = workflow.do?.find(
      (entry) => "submit_prepare_login_email" in entry,
    )?.submit_prepare_login_email as { if?: string } | undefined;

    expect(signupSubmitStep?.if).toContain(
      "effective_prepare_login_entry_state?.stage === 'signup_email'",
    );
    expect(signupSubmitStep?.if).toContain(
      "classify_prepare_after_choose_signup?.action?.stage === 'signup_email'",
    );
    expect(submitStep?.if).toContain(
      "effective_prepare_login_entry_state?.stage === 'login_email'",
    );
    expect(submitStep?.if).toContain(
      "classify_prepare_after_choose_signup?.action?.stage !== 'signup_email'",
    );
    expect(submitStep?.if).not.toContain("click_prepare_signup_button");
  });

  test("keeps password-first defaults aligned across device-auth guards", () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText.includes("prefer_password_login ?? 'false'")).toBe(
      false,
    );
  });

  test("keeps signup-recovery defaults aligned across device-auth guards", () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain(
      'prefer_signup_recovery:\n          default: "true"',
    );
    expect(workflowText.includes("prefer_signup_recovery ?? 'false'")).toBe(
      false,
    );
  });

  test("tracks locator availability and password attempts for device-auth recovery", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const computeStep = workflow.do?.find(
      (entry) => "compute_device_auth_one_time_code_recovery_flag" in entry,
    )?.compute_device_auth_one_time_code_recovery_flag as
      | {
          run?: {
            script?: {
              code?: string;
            };
          };
        }
      | undefined;

    expect(computeStep?.run?.script?.code).toContain(
      "const hasLocator = args.account_login_locator != null;",
    );
    expect(computeStep?.run?.script?.code).toContain(
      'const preferPasswordLogin = String(args.prefer_password_login ?? "true").trim().toLowerCase() !== "false";',
    );
    expect(computeStep?.run?.script?.code).toContain("passwordAttempted");
  });

  test("preserves an omitted preferPasswordLogin flag instead of coercing it to false", () => {
    const automationText = readFileSync(
      join(repoRoot, "packages", "codex-rotate", "automation.ts"),
      "utf8",
    );

    expect(automationText).toContain(
      "preferPasswordLogin: options?.preferPasswordLogin,",
    );
    expect(automationText).not.toContain(
      "preferPasswordLogin: options?.preferPasswordLogin === true,",
    );
  });

  test("retries unknown auth classifications before caching device-auth login state", () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain("classify_prepare_login_entry_retry");
    expect(workflowText).toContain("classify_device_auth_login_entry_retry");
  });

  test("clears prepare-login security verification before caching the effective device-auth login state", () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain("clear_prepare_login_security_gate");
    expect(workflowText).toContain(
      "classify_prepare_login_entry_after_security_gate",
    );
  });

  test("targets OTP-style inputs when refilling the replacement prepare verification code", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const step = workflow.do?.find(
      (entry) =>
        "fill_prepare_login_verification_code_after_incorrect_code" in entry,
    )?.fill_prepare_login_verification_code_after_incorrect_code as
      | {
          call?: string;
          with?: {
            body?: {
              selector?: string;
              text?: string;
              dispatch_events_after_fill?: boolean;
              blur_after_fill?: boolean;
              settle_ms?: number;
            };
          };
        }
      | undefined;

    expect(step?.call).toBe("afn.driver.browser.type");
    expect(step?.with?.body?.selector).toContain(
      'input[autocomplete="one-time-code"]',
    );
    expect(step?.with?.body?.selector).toContain('input[inputmode="numeric"]');
    expect(step?.with?.body?.text).toContain(
      "state.vars.prepare_login_verification_code",
    );
    expect(step?.with?.body?.text).toContain(
      "recollect_prepare_login_verification_artifact_after_incorrect_code_missing_code",
    );
    expect(step?.with?.body?.dispatch_events_after_fill).toBe(true);
    expect(step?.with?.body?.blur_after_fill).toBe(true);
    expect(step?.with?.body?.settle_ms).toBe(250);
  });

  test("waits and recollects the replacement prepare verification code when Gmail loads late", () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain(
      "wait_for_prepare_login_verification_email_delivery_after_incorrect_code",
    );
    expect(workflowText).toContain(
      "recollect_prepare_login_verification_artifact_after_incorrect_code_missing_code",
    );
    expect(workflowText).toContain(
      "cache_prepare_login_verification_artifact_after_incorrect_code_missing_code",
    );
    expect(workflowText).toContain(
      "state.vars.prepare_login_verification_code",
    );
  });

  test("waits and recollects the first prepare verification code when Gmail loads late", () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain(
      "wait_for_prepare_login_verification_email_delivery",
    );
    expect(workflowText).toContain(
      "recollect_prepare_login_verification_artifact_after_missing_code",
    );
    expect(workflowText).toContain(
      "cache_prepare_login_verification_artifact_after_missing_code",
    );
    expect(workflowText).toContain(
      "state.vars.prepare_login_verification_code",
    );
  });

  test("waits after resending the first device-auth login verification email before opening Gmail", () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain(
      "wait_after_resend_device_auth_login_verification_email",
    );
    expect(workflowText).toContain(
      "Allow Gmail to settle after resending the device-auth login verification email before collecting the OTP.",
    );
    expect(workflowText).toContain(
      "state.steps.resend_device_auth_login_verification_email?.action?.ok === true",
    );
  });

  test("waits after resending the replacement device-auth verification email before reopening Gmail", () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain(
      "wait_after_resend_device_auth_login_verification_email_after_incorrect_code",
    );
    expect(workflowText).toContain(
      "Allow Gmail to settle after resending the replacement device-auth verification email before recollecting the OTP.",
    );
    expect(workflowText).toContain(
      "state.steps.resend_device_auth_login_verification_email_after_incorrect_code?.action?.ok === true",
    );
  });

  test("keeps prepare add-phone as an authenticated-ready state instead of blocking device-auth preparation", () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain("latest.add_phone_prompt === true");
    expect(workflowText).toContain('stage === "add_phone"');
    expect(workflowText).toContain("prepare_flow_ready");
  });

  test("retries the final device-auth login when OpenAI blocks consent behind add phone", () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain(
      "surface.add_phone_prompt === true && authUrl",
    );
    expect(workflowText).toContain(
      "for (const waitMs of [2000, 4000, 8000, 12000])",
    );
    expect(workflowText).toContain('retryReason = "device_auth_add_phone"');
    expect(workflowText).toContain("add_phone_prompt: addPhonePrompt");
  });

  test("targets OTP-style inputs when filling the device-auth verification code", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const step = workflow.do?.find(
      (entry) => "fill_device_auth_login_verification_code" in entry,
    )?.fill_device_auth_login_verification_code as
      | {
          call?: string;
          with?: {
            body?: {
              selector?: string;
              text?: string;
              dispatch_events_after_fill?: boolean;
              blur_after_fill?: boolean;
              settle_ms?: number;
            };
          };
        }
      | undefined;

    expect(step?.call).toBe("afn.driver.browser.type");
    expect(step?.with?.body?.selector).toContain(
      'input[autocomplete="one-time-code"]',
    );
    expect(step?.with?.body?.selector).toContain('input[inputmode="numeric"]');
    expect(step?.with?.body?.text).toContain(
      "collect_device_auth_login_verification_artifact",
    );
    expect(step?.with?.body?.text).toContain(
      ".replace(/\\D+/g, '').slice(0, 6)",
    );
    expect(step?.with?.body?.dispatch_events_after_fill).toBe(true);
    expect(step?.with?.body?.blur_after_fill).toBe(true);
    expect(step?.with?.body?.settle_ms).toBe(250);
  });

  test("uses the robust segmented-input verification submitter during prepare", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const step = workflow.do?.find(
      (entry) => "submit_prepare_login_verification_code" in entry,
    )?.submit_prepare_login_verification_code as
      | {
          call?: string;
          with?: { body?: { script?: string } };
        }
      | undefined;

    expect(step?.call).toBe("afn.driver.browser.exec_script");
    expect(step?.with?.body?.script).toContain("pressSequentially(code");
    expect(step?.with?.body?.script).toMatch(
      /state\.vars\??\.prepare_login_verification_code/,
    );
    expect(step?.with?.body?.script).toContain(
      "recollect_prepare_login_verification_artifact_after_missing_code",
    );
    expect(step?.with?.body?.script).toContain(
      "collect_prepare_login_verification_artifact",
    );
    expect(step?.with?.body?.script).toContain(
      'after.stage !== "email_verification"',
    );
  });

  test("uses the robust segmented-input verification submitter during device auth", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const step = workflow.do?.find(
      (entry) => "submit_device_auth_login_verification_code" in entry,
    )?.submit_device_auth_login_verification_code as
      | {
          call?: string;
          with?: { body?: { script?: string } };
        }
      | undefined;

    expect(step?.call).toBe("afn.driver.browser.exec_script");
    expect(step?.with?.body?.script).toContain("pressSequentially(code");
    expect(step?.with?.body?.script).toContain(
      "collect_device_auth_login_verification_artifact",
    );
    expect(step?.with?.body?.script).toContain(
      'after.stage !== "email_verification"',
    );
  });

  test("accepts the ChatGPT settings dialog before activating Security for device auth", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "wait_for_chatgpt_settings_shell",
      `
        <html>
          <body style="min-height: 100vh;">
            <div role="dialog" aria-label="Settings">
              <button type="button">General</button>
              <button type="button">Notifications</button>
              <button type="button">Security</button>
              <button type="button">Account</button>
              <button type="button">Data controls</button>
            </div>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/#settings/Security",
    );

    expect(result.ok).toBe(true);
    expect(result.dialog_visible).toBe(true);
    expect(result.settings_shell).toBe(true);
  });

  test("warms an authenticated ChatGPT shell before opening Security settings for device auth", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "wait_for_authenticated_chatgpt_shell_before_security_settings",
      `
        <html>
          <body style="min-height: 100vh;">
            <main>
              <button type="button">New chat</button>
              <button type="button">Projects</button>
              <button type="button">Search chats</button>
            </main>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/",
    );

    expect(result.ok).toBe(true);
    expect(result.authenticated_chatgpt_shell).toBe(true);
    expect(result.app_markers).toBe(true);
    expect(result.public_shell).toBe(false);
  });

  test("rejects a generic ChatGPT settings shell that is not yet logged in for device auth", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "wait_for_chatgpt_settings_shell",
      `
        <html>
          <body style="min-height: 100vh;">
            <main>
              <div>General</div>
              <div>Data controls</div>
              <div>Appearance</div>
              <div>Language</div>
            </main>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/#settings/Security",
    );

    expect(result.ok).toBe(false);
    expect(result.logged_in_settings_shell).toBe(false);
    expect(result.settings_shell).toBe(true);
    expect(result.security_control_visible).toBe(false);
  });

  test("accepts the full-page ChatGPT settings shell even when it is not rendered as a dialog", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "wait_for_chatgpt_settings_shell",
      `
        <html>
          <body style="min-height: 100vh;">
            <main>
              <button type="button">General</button>
              <button type="button">Appearance</button>
              <button type="button">Data controls</button>
              <button type="button">Security</button>
              <button type="button">Account</button>
            </main>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/#settings/Security",
    );

    expect(result.ok).toBe(true);
    expect(result.dialog_visible).toBe(false);
    expect(result.settings_shell).toBe(true);
  });

  test("activates the Security tab before enabling device auth when settings opens on General", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "activate_chatgpt_security_settings_tab",
      `
        <html>
          <body style="min-height: 100vh;">
            <div role="dialog" aria-label="Settings">
              <button type="button" id="general">General</button>
              <button type="button" id="security">Security</button>
              <div id="panel">General Notifications Security Account Data controls</div>
            </div>
            <script>
              document.getElementById("security")?.addEventListener("click", () => {
                document.getElementById("panel").textContent =
                  "Security Trusted devices Enable device code authorization for Codex";
              });
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/#settings/Security",
    );

    expect(result.ok).toBe(true);
    expect(result.clicked).toBe(true);
    expect(result.security_open).toBe(true);
  });

  test("does not activate Security on a settings shell that is not logged in", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "activate_chatgpt_security_settings_tab",
      `
        <html>
          <body style="min-height: 100vh;">
            <main>
              <div>General</div>
              <div>Data controls</div>
              <div>Appearance</div>
              <div>Language</div>
            </main>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/#settings/Security",
    );

    expect(result.ok).toBe(false);
    expect(result.logged_in_settings_shell).toBe(false);
    expect(result.security_open).toBe(false);
  });

  test("opens an explicit ChatGPT login branch before Security settings when the warmup shell is not authenticated", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const openLoginStep = workflow.do?.find(
      (entry) => "open_chatgpt_login_entry_before_security_settings" in entry,
    )?.open_chatgpt_login_entry_before_security_settings as
      | {
          with?: {
            body?: {
              url?: string;
            };
          };
        }
      | undefined;
    const openSecurityStep = workflow.do?.find(
      (entry) => "open_chatgpt_security_settings" in entry,
    )?.open_chatgpt_security_settings as
      | {
          if?: string;
        }
      | undefined;

    expect(openLoginStep?.with?.body?.url).toContain(
      "chatgpt.com/auth/login?next=%2F%23settings%2FSecurity",
    );
    expect(openSecurityStep?.if).toContain(
      "cache_effective_authenticated_chatgpt_shell_before_security_settings",
    );
  });

  test("clicks through the ChatGPT auth-landing before Security settings", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const clickLoginStep = workflow.do?.find(
      (entry) =>
        "click_chatgpt_login_button_before_security_settings_from_auth_landing" in
        entry,
    )?.click_chatgpt_login_button_before_security_settings_from_auth_landing as
      | {
          if?: string;
        }
      | undefined;

    expect(clickLoginStep?.if).toContain("chatgpt\\.com\\/auth\\/login");
  });

  test("caches the reclassified ChatGPT auth entry after the auth-landing login click", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const cacheStep = workflow.do?.find(
      (entry) =>
        "cache_effective_chatgpt_login_entry_before_security_settings" in entry,
    )?.cache_effective_chatgpt_login_entry_before_security_settings as
      | {
          set?: { pre_settings_login_entry?: string };
        }
      | undefined;

    expect(cacheStep?.set?.pre_settings_login_entry).toContain(
      "classify_chatgpt_login_entry_before_security_settings_after_auth_landing",
    );
    expect(cacheStep?.set?.pre_settings_login_entry).toContain(
      "classify_chatgpt_login_entry_before_security_settings",
    );
  });

  test("activates the Security tab on the full-page ChatGPT settings shell for device auth", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "activate_chatgpt_security_settings_tab",
      `
        <html>
          <body style="min-height: 100vh;">
            <main>
              <div>New chat</div>
              <div>Projects</div>
              <button type="button" id="general">General</button>
              <button type="button" id="security">Security</button>
              <div id="panel">General Appearance Data controls Security Account</div>
            </main>
            <script>
              document.getElementById("security")?.addEventListener("click", () => {
                document.getElementById("panel").textContent =
                  "Security Trusted devices Enable device code authorization for Codex";
              });
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/#settings/Security",
    );

    expect(result.ok).toBe(true);
    expect(result.clicked).toBe(true);
    expect(result.dialog_visible).toBe(false);
    expect(result.settings_shell).toBe(true);
    expect(result.security_open).toBe(true);
  });

  test("reactivates the Security tab on the full-page ChatGPT settings shell after toggling device auth", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "reactivate_chatgpt_security_settings_tab_after_toggle",
      `
        <html>
          <body style="min-height: 100vh;">
            <main>
              <div>New chat</div>
              <div>Projects</div>
              <button type="button" id="general">General</button>
              <button type="button" id="security">Security</button>
              <div id="panel">General Appearance Data controls Security Account</div>
            </main>
            <script>
              document.getElementById("security")?.addEventListener("click", () => {
                document.getElementById("panel").textContent =
                  "Security Trusted devices Enable device code authorization for Codex";
              });
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/#settings/Security",
    );

    expect(result.ok).toBe(true);
    expect(result.clicked).toBe(true);
    expect(result.dialog_visible).toBe(false);
    expect(result.settings_shell).toBe(true);
    expect(result.security_open).toBe(true);
  });

  test("logs out via the ChatGPT security settings control during device-auth prepare", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "logout_prepare_chatgpt_app_shell",
      `
        <html>
          <body style="min-height: 100vh;">
            <div role="dialog" aria-label="Settings">
              <div>Security</div>
              <button type="button" id="logout">Log out</button>
            </div>
            <script>
              document.getElementById("logout")?.addEventListener("click", () => {
                document.body.innerHTML = "<h1>Logged out</h1>";
              });
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/#settings/Security",
    );

    expect(result.ok).toBe(true);
    expect(result.clicked_text).toBe("Log out");
  });

  test("device-auth keeps the legacy fallback step disabled and recovers through its own pre-security login branch", () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain("prepare_with_original_flow_fallback");
    expect(workflowText).toContain(
      "Legacy no-op kept only for step-id stability; device-auth must recover through its own pre-security login branch instead of borrowing the original non-device flow.",
    );
    expect(workflowText).toContain(
      'prepare_with_original_flow_fallback:\n      if: "${false}"',
    );
    expect(workflowText).not.toContain(
      "workflow.workspace.web.auth-openai-com.codex-rotate-account-flow",
    );
    expect(workflowText).toContain("prepare_security_login_recovery_required");
  });

  test("waits for the ChatGPT settings shell before the prepare logout click", () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain(
      "wait_for_prepare_chatgpt_security_settings_shell",
    );
    expect(workflowText).toContain(
      "activate_prepare_chatgpt_security_settings_tab",
    );
    expect(workflowText).toContain("log out of all devices");
    expect(workflowText).toContain(
      "clear_prepare_device_auth_site_state_after_ui_logout",
    );
  });

  test("activates the prepare Security tab before attempting the ChatGPT logout control", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "activate_prepare_chatgpt_security_settings_tab",
      `
        <html>
          <body style="min-height: 100vh;">
            <div role="dialog">
              <button type="button" id="general">General</button>
              <button type="button" id="security">Security</button>
              <div id="panel">General Data controls Appearance System Language Auto-detect</div>
            </div>
            <script>
              document.getElementById("security")?.addEventListener("click", () => {
                document.getElementById("panel").textContent =
                  "Security Secure sign in with ChatGPT Trusted devices Log out of all devices";
              });
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/#settings/Security",
    );

    expect(result.ok).toBe(true);
    expect(result.security_open).toBe(true);
  });

  test("tolerates a QUIC logout navigation error while clearing prepare device-auth site state", async () => {
    const script = await loadWorkflowStepScript(
      deviceAuthWorkflowPath,
      "clear_prepare_device_auth_site_state",
    );
    const execute = new AsyncFunction("page", "state", "args", script);
    const result = (await execute(
      {
        goto: async () => {
          throw new Error(
            "page.goto: net::ERR_QUIC_PROTOCOL_ERROR at https://chatgpt.com/auth/logout",
          );
        },
        url: () => "about:blank",
      },
      {},
      {},
    )) as Record<string, unknown>;

    expect(result.ok).toBe(true);
    expect(result.tolerated_logout_navigation_error).toBe(true);
    expect(String(result.logout_navigation_error_message || "")).toContain(
      "ERR_QUIC_PROTOCOL_ERROR",
    );
  });

  test("tolerates a QUIC logout navigation error after the settings-based logout path", async () => {
    const script = await loadWorkflowStepScript(
      deviceAuthWorkflowPath,
      "clear_prepare_device_auth_site_state_after_ui_logout",
    );
    const execute = new AsyncFunction("page", "state", "args", script);
    const result = (await execute(
      {
        goto: async () => {
          throw new Error(
            "page.goto: net::ERR_QUIC_PROTOCOL_ERROR at https://chatgpt.com/auth/logout",
          );
        },
        url: () => "about:blank",
      },
      {},
      {},
    )) as Record<string, unknown>;

    expect(result.ok).toBe(true);
    expect(result.tolerated_logout_navigation_error).toBe(true);
    expect(String(result.logout_navigation_error_message || "")).toContain(
      "ERR_QUIC_PROTOCOL_ERROR",
    );
  });

  test("uses the direct OpenAI login surface as the existing-account fallback during device-auth prepare", () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain("https://auth.openai.com/log-in");
  });

  test("classifies the logged-out ChatGPT shell with login CTAs as public, not authenticated", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "inspect_prepare_public_chatgpt_entry",
      `
        <html>
          <body style="min-height: 100vh;">
            <div>New chat</div>
            <div>Search chats</div>
            <div>Images</div>
            <div>See plans and pricing</div>
            <button type="button">Log in</button>
            <button type="button">Sign up for free</button>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/",
    );

    expect(result.stage).toBe("public_chatgpt");
    expect(result.account_ready).toBe(false);
    expect(result.login_cta_visible).toBe(true);
    expect(result.signup_cta_visible).toBe(true);
  });

  test("forces the direct auth fallback when prepare remains on an authenticated ChatGPT shell", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const fallbackStep = workflow.do?.find(
      (entry) => "open_prepare_direct_auth_entry_fallback" in entry,
    )?.open_prepare_direct_auth_entry_fallback as { if?: string } | undefined;

    expect(fallbackStep?.if).toContain("stage !== 'public_chatgpt'");
    expect(fallbackStep?.if).not.toContain("stage === 'blank_chatgpt'");
    expect(fallbackStep?.if).not.toContain("stage === 'unknown'");
    expect(fallbackStep?.if).not.toContain("stage === 'security_verification'");
  });

  test("reduces the effective prepare login state through the scripted candidate picker", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const cacheStep = workflow.do?.find(
      (entry) => "cache_effective_prepare_login_entry_state" in entry,
    )?.cache_effective_prepare_login_entry_state as
      | {
          run?: {
            script?: {
              code?: string;
            };
          };
        }
      | undefined;

    expect(cacheStep?.run?.script?.code).toContain("const candidates = [");
    expect(cacheStep?.run?.script?.code).toContain(
      "classify_prepare_after_choose_signup",
    );
    expect(cacheStep?.run?.script?.code).toContain(
      "classify_prepare_login_entry_after_security_gate",
    );
  });

  test("recovers the prepare email branch from retryable invalid-state shells", () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain("click_prepare_login_email_timeout_retry");
    expect(workflowText).toContain(
      "classify_prepare_after_login_email_timeout_retry_submit",
    );
    expect(workflowText).toContain(
      "force_prepare_signup_recovery_after_login_timeout",
    );
    expect(workflowText).toContain(
      "classify_prepare_signup_recovery_after_login_timeout",
    );
    expect(workflowText).toContain(
      "cache_effective_prepare_after_login_email_state",
    );
  });

  test("opens the direct prepare auth fallback on a clean site state", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const step = workflow.do?.find(
      (entry) => "open_prepare_direct_auth_entry_fallback" in entry,
    )?.open_prepare_direct_auth_entry_fallback as
      | {
          metadata?: {
            browser?: {
              clearSiteDataForOrigins?: string[];
            };
          };
        }
      | undefined;

    expect(step?.metadata?.browser?.clearSiteDataForOrigins).toEqual([
      "https://auth.openai.com",
      "https://chatgpt.com",
      "https://chat.openai.com",
    ]);
  });

  test("forces direct signup recovery after repeated prepare login invalid-state retries", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const forceStep = workflow.do?.find(
      (entry) => "force_prepare_signup_recovery_after_login_timeout" in entry,
    )?.force_prepare_signup_recovery_after_login_timeout as
      | {
          if?: string;
          with?: { body?: { url?: string } };
          metadata?: {
            browser?: {
              clearSiteDataForOrigins?: string[];
            };
          };
        }
      | undefined;
    const classifyStep = workflow.do?.find(
      (entry) =>
        "classify_prepare_signup_recovery_after_login_timeout" in entry,
    )?.classify_prepare_signup_recovery_after_login_timeout as
      | { if?: string }
      | undefined;
    const refillStep = workflow.do?.find(
      (entry) => "refill_prepare_email_after_forced_signup_recovery" in entry,
    )?.refill_prepare_email_after_forced_signup_recovery as
      | { if?: string }
      | undefined;
    const submitStep = workflow.do?.find(
      (entry) =>
        "submit_prepare_signup_email_after_forced_signup_recovery" in entry,
    )?.submit_prepare_signup_email_after_forced_signup_recovery as
      | { if?: string }
      | undefined;
    const cacheStep = workflow.do?.find(
      (entry) => "cache_effective_prepare_after_login_email_state" in entry,
    )?.cache_effective_prepare_after_login_email_state as
      | {
          run?: {
            script?: {
              code?: string;
            };
          };
        }
      | undefined;

    expect(forceStep?.if).toContain(
      "classify_prepare_after_login_email_timeout_retry_submit?.action?.stage === 'retryable_timeout'",
    );
    expect(forceStep?.with?.body?.url).toBe(
      "https://auth.openai.com/create-account",
    );
    expect(forceStep?.metadata?.browser?.clearSiteDataForOrigins).toEqual([
      "https://auth.openai.com",
      "https://chatgpt.com",
      "https://chat.openai.com",
    ]);
    expect(classifyStep?.if).toContain(
      "force_prepare_signup_recovery_after_login_timeout?.action?.ok === true",
    );
    expect(refillStep?.if).toContain(
      "classify_prepare_signup_recovery_after_login_timeout?.action?.stage === 'signup_email'",
    );
    expect(submitStep?.if).toContain(
      "/create-account/i.test(String(state.steps.classify_prepare_signup_recovery_after_login_timeout?.action?.current_url || ''))",
    );
    expect(cacheStep?.run?.script?.code).toContain(
      "classify_prepare_after_forced_signup_recovery_email_submit",
    );
    expect(cacheStep?.run?.script?.code).toContain(
      "classify_prepare_signup_recovery_after_login_timeout",
    );
  });

  test("retries the forced direct signup recovery branch when create-account stays on invalid_state", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const retryStep = workflow.do?.find(
      (entry) => "click_prepare_forced_signup_email_timeout_retry" in entry,
    )?.click_prepare_forced_signup_email_timeout_retry as
      | { if?: string }
      | undefined;
    const classifyRetryStep = workflow.do?.find(
      (entry) =>
        "classify_prepare_after_forced_signup_email_timeout_retry" in entry,
    )?.classify_prepare_after_forced_signup_email_timeout_retry as
      | { if?: string }
      | undefined;
    const refillRetryStep = workflow.do?.find(
      (entry) =>
        "refill_prepare_email_after_forced_signup_email_timeout_retry" in entry,
    )?.refill_prepare_email_after_forced_signup_email_timeout_retry as
      | { if?: string }
      | undefined;
    const submitRetryStep = workflow.do?.find(
      (entry) =>
        "submit_prepare_signup_email_after_forced_signup_email_timeout_retry" in
        entry,
    )?.submit_prepare_signup_email_after_forced_signup_email_timeout_retry as
      | { if?: string }
      | undefined;
    const cacheStep = workflow.do?.find(
      (entry) => "cache_effective_prepare_after_login_email_state" in entry,
    )?.cache_effective_prepare_after_login_email_state as
      | {
          run?: {
            script?: {
              code?: string;
            };
          };
        }
      | undefined;

    expect(retryStep?.if).toContain(
      "classify_prepare_after_forced_signup_recovery_email_submit?.action?.retryable_timeout === true",
    );
    expect(retryStep?.if).toContain(
      "/create-account/i.test(String(state.steps.classify_prepare_after_forced_signup_recovery_email_submit?.action?.current_url || ''))",
    );
    expect(classifyRetryStep?.if).toContain(
      "click_prepare_forced_signup_email_timeout_retry?.action?.ok === true",
    );
    expect(refillRetryStep?.if).toContain(
      "classify_prepare_after_forced_signup_email_timeout_retry?.action?.stage === 'signup_email'",
    );
    expect(submitRetryStep?.if).toContain(
      "/create-account/i.test(String(state.steps.classify_prepare_after_forced_signup_email_timeout_retry?.action?.current_url || ''))",
    );
    expect(cacheStep?.run?.script?.code).toContain(
      "classify_prepare_after_forced_signup_email_timeout_retry_submit",
    );
  });

  test("falls back to direct login after repeated signup invalid_state shells", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const fallbackStep = workflow.do?.find(
      (entry) => "fallback_prepare_login_after_signup_invalid_state" in entry,
    )?.fallback_prepare_login_after_signup_invalid_state as
      | { if?: string; with?: { body?: { url?: string } } }
      | undefined;
    const cacheStep = workflow.do?.find(
      (entry) => "cache_effective_prepare_after_login_email_state" in entry,
    )?.cache_effective_prepare_after_login_email_state as
      | { run?: { script?: { code?: string } } }
      | undefined;

    expect(fallbackStep?.if).toContain(
      "classify_prepare_after_forced_signup_email_timeout_retry_submit?.action?.retryable_timeout === true",
    );
    expect(fallbackStep?.if).toContain("/create-account/i.test");
    expect(fallbackStep?.if).toContain(
      "classify_prepare_after_forced_signup_recovery_email_submit?.action?.retryable_timeout === true",
    );
    expect(fallbackStep?.if).toContain(
      "click_prepare_forced_signup_email_timeout_retry?.action?.ok !== true",
    );
    expect(fallbackStep?.with?.body?.url).toBe(
      "https://auth.openai.com/log-in",
    );
    expect(cacheStep?.run?.script?.code).toContain(
      "classify_prepare_after_login_email_signup_invalid_state_fallback",
    );
    expect(cacheStep?.run?.script?.code).toContain(
      "classify_prepare_after_login_email_signup_invalid_state_fallback_retry",
    );
    expect(cacheStep?.run?.script?.code).toContain(
      "classify_prepare_login_entry_after_signup_invalid_state_fallback",
    );
  });

  test("reopens the direct login form when the signup invalid_state fallback lands on log-in-or-create-account", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const openStep = workflow.do?.find(
      (entry) =>
        "open_prepare_direct_login_after_signup_invalid_state_auth_landing" in
        entry,
    )?.open_prepare_direct_login_after_signup_invalid_state_auth_landing as
      | { if?: string; with?: { body?: { url?: string } } }
      | undefined;
    const classifyStep = workflow.do?.find(
      (entry) =>
        "classify_prepare_login_entry_after_signup_invalid_state_auth_landing" in
        entry,
    )?.classify_prepare_login_entry_after_signup_invalid_state_auth_landing as
      | { if?: string }
      | undefined;
    const cacheStep = workflow.do?.find(
      (entry) => "cache_effective_prepare_after_login_email_state" in entry,
    )?.cache_effective_prepare_after_login_email_state as
      | { run?: { script?: { code?: string } } }
      | undefined;

    expect(openStep?.if).toContain(
      "/auth\\.openai\\.com\\/log-in-or-create-account/i.test",
    );
    expect(openStep?.if).toContain(
      "classify_prepare_login_entry_after_signup_invalid_state_fallback?.action?.login_cta_visible === true",
    );
    expect(openStep?.with?.body?.url).toBe("https://auth.openai.com/log-in");
    expect(classifyStep?.if).toContain(
      "open_prepare_direct_login_after_signup_invalid_state_auth_landing?.action?.ok === true",
    );
    expect(cacheStep?.run?.script?.code).toContain(
      "classify_prepare_login_entry_after_signup_invalid_state_auth_landing",
    );
  });

  test("classifies the OpenAI log-in-or-create-account shell as a recoverable auth prompt", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "classify_prepare_login_entry_after_signup_invalid_state_fallback",
      `
        <html>
          <head>
            <title>OpenAI</title>
          </head>
          <body>
            <h1>Welcome back</h1>
            <button type="button">Log in</button>
            <button type="button">Sign up</button>
          </body>
        </html>
      `,
      {},
      {},
      "https://auth.openai.com/log-in-or-create-account",
    );

    expect(result.ok).toBe(true);
    expect(result.stage).toBe("auth_prompt");
    expect(result.login_cta_visible).toBe(true);
    expect(result.email_input).toBe(false);
  });

  test("retries the prepare login email once more after the signup invalid_state fallback still lands on the email step", async () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain(
      "retry_prepare_login_email_after_signup_invalid_state_fallback",
    );
    expect(workflowText).toContain(
      "continue_prepare_login_email_after_signup_invalid_state_fallback",
    );
    expect(workflowText).toContain(
      "classify_prepare_after_login_email_signup_invalid_state_fallback?.action?.stage === 'login_email'",
    );
    expect(workflowText).toContain(
      "classify_prepare_after_login_email_signup_invalid_state_fallback_retry",
    );
  });

  test("falls back to direct login after the signup password step reports an existing account", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const fallbackStep = workflow.do?.find(
      (entry) =>
        "fallback_prepare_login_after_existing_account_prompt" in entry,
    )?.fallback_prepare_login_after_existing_account_prompt as
      | { if?: string; with?: { body?: { url?: string } } }
      | undefined;
    const recoveryStep = workflow.do?.find(
      (entry) => "compute_prepare_one_time_code_recovery_flag" in entry,
    )?.compute_prepare_one_time_code_recovery_flag as
      | { run?: { script?: { code?: string } } }
      | undefined;

    expect(fallbackStep?.if).toContain(
      "classify_prepare_after_signup_password_gate?.action?.existing_account_prompt === true",
    );
    expect(fallbackStep?.with?.body?.url).toBe(
      "https://auth.openai.com/log-in",
    );
    expect(recoveryStep?.run?.script?.code).toContain(
      "classify_prepare_after_login_email_existing_account_prompt",
    );
    expect(recoveryStep?.run?.script?.code).toContain(
      "existingAccountFallbackActive",
    );
  });

  test("treats oauth consent as a successful authenticated prepare state for device auth", async () => {
    const result = await runWorkflowRunScript(
      deviceAuthWorkflowPath,
      "compute_prepare_flow_result",
      {},
      {
        vars: {
          reuse_device_auth_session: false,
        },
        steps: {
          classify_prepare_after_login_verification_submit: {
            action: {
              stage: "oauth_consent",
              oauth_continue_visible: true,
              current_url:
                "https://auth.openai.com/sign-in-with-chatgpt/codex/consent",
            },
          },
        },
      },
    );

    expect(result.prepare_flow_ready).toBe(true);
    expect((result.prepare_flow_output as Record<string, unknown>)?.stage).toBe(
      "oauth_consent",
    );
  });

  test("treats add_phone as a successful authenticated prepare state for device auth", async () => {
    const result = await runWorkflowRunScript(
      deviceAuthWorkflowPath,
      "compute_prepare_flow_result",
      {},
      {
        vars: {
          reuse_device_auth_session: false,
        },
        steps: {
          classify_prepare_after_signup_about_you: {
            action: {
              stage: "add_phone",
              add_phone_prompt: true,
              current_url: "https://auth.openai.com/add-phone",
            },
          },
        },
      },
    );

    expect(result.prepare_flow_ready).toBe(true);
    expect((result.prepare_flow_output as Record<string, unknown>)?.stage).toBe(
      "add_phone",
    );
  });

  test("captures an explicit device-auth challenge without requiring the settings shortcut first", async () => {
    const result = await runWorkflowRunScript(
      deviceAuthWorkflowPath,
      "capture_codex_login_url",
      {
        auth_url: "https://auth.openai.com/codex/device?user_code=ABCD-EFGHI",
        device_code: "ABCD-EFGHI",
      },
      {
        vars: {
          prepare_flow_ready: true,
          reuse_device_auth_session: false,
        },
        steps: {},
      },
    );

    expect(result.ok).toBe(true);
    expect(result.reused).toBe(true);
    expect(result.auth_url).toBe(
      "https://auth.openai.com/codex/device?user_code=ABCD-EFGHI",
    );
    expect(result.device_code).toBe("ABCD-EFGHI");
  });

  test("finalizes device-auth as success when the challenge completes even if settings Security was unavailable", async () => {
    const result = await runWorkflowRunScript(
      deviceAuthWorkflowPath,
      "finalize_device_auth_tail_summary",
      {
        email: "dev.115@astronlab.com",
      },
      {
        vars: {
          prepare_flow_ready: true,
          codex_login: {
            auth_url:
              "https://auth.openai.com/codex/device?user_code=ABCD-EFGHI",
            device_code: "ABCD-EFGHI",
          },
        },
        steps: {
          activate_chatgpt_security_settings_tab: {
            action: {
              ok: false,
            },
          },
          ensure_device_code_authorization_enabled: {
            action: {
              ok: false,
              enabled: false,
            },
          },
          inspect_device_authorization_surface: {
            action: {
              current_url: "https://auth.openai.com/codex/deviceauth/callback",
              success: true,
              headline: "Signed in to Codex",
            },
          },
          wait_for_codex_login_exit: {
            action: {
              value: {
                ok: true,
                exit_code: 0,
              },
            },
          },
        },
      },
    );

    expect(result.success).toBe(true);
    expect(result.next_action).toBe("complete");
    expect(result.error_message).toBeNull();
    expect(result.verified_account_email).toBe("dev.115@astronlab.com");
  });

  test("opens the captured device-auth challenge once preparation is ready without gating on the settings shortcut", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const openStep = workflow.do?.find(
      (entry) => "open_device_authorization_entry" in entry,
    )?.open_device_authorization_entry as
      | {
          if?: string;
        }
      | undefined;

    expect(openStep?.if).toContain("state.vars.prepare_flow_ready === true");
    expect(openStep?.if).not.toContain(
      "ensure_device_code_authorization_enabled",
    );
    expect(openStep?.if).not.toContain(
      "reconfirm_device_code_authorization_enabled",
    );
  });

  test("device-auth opens the pre-security ChatGPT recovery branch when prepare invalid_state poisoned the local login path", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const openWarmupStep = workflow.do?.find(
      (entry) =>
        "open_authenticated_chatgpt_shell_before_security_settings" in entry,
    )?.open_authenticated_chatgpt_shell_before_security_settings as
      | {
          if?: string;
        }
      | undefined;
    const cacheStep = workflow.do?.find(
      (entry) => "cache_prepare_flow_result" in entry,
    )?.cache_prepare_flow_result as
      | {
          set?: Record<string, string>;
        }
      | undefined;

    expect(cacheStep?.set?.prepare_security_login_recovery_required).toContain(
      "prepare_flow_output?.next_action === 'skip_account'",
    );
    expect(cacheStep?.set?.prepare_security_login_recovery_required).toContain(
      "prepare_flow_output?.retry_reason === 'prepare_invalid_state'",
    );
    expect(openWarmupStep?.if).toContain(
      "state.vars.prepare_security_login_recovery_required === true",
    );
  });

  test("device-auth reopens direct OpenAI login when the device-auth auth landing falls into log-in-or-create-account", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const openStep = workflow.do?.find(
      (entry) => "open_device_auth_direct_login_after_auth_landing" in entry,
    )?.open_device_auth_direct_login_after_auth_landing as
      | { if?: string; with?: { body?: { url?: string } } }
      | undefined;
    const classifyStep = workflow.do?.find(
      (entry) =>
        "classify_device_auth_login_entry_after_auth_landing_reopen" in entry,
    )?.classify_device_auth_login_entry_after_auth_landing_reopen as
      | { if?: string }
      | undefined;
    const cacheStep = workflow.do?.find(
      (entry) => "cache_effective_device_auth_login_entry_state" in entry,
    )?.cache_effective_device_auth_login_entry_state as
      | { set?: Record<string, string> }
      | undefined;

    expect(openStep?.if).toContain(
      "/auth\\.openai\\.com\\/log-in-or-create-account/i.test",
    );
    expect(openStep?.if).toContain(
      "classify_device_auth_login_entry_after_auth_landing?.action?.login_cta_visible === true",
    );
    expect(openStep?.with?.body?.url).toBe("https://auth.openai.com/log-in");
    expect(classifyStep?.if).toContain(
      "open_device_auth_direct_login_after_auth_landing?.action?.ok === true",
    );
    expect(cacheStep?.set?.effective_device_auth_login_entry_state).toContain(
      "classify_device_auth_login_entry_after_auth_landing_reopen",
    );
  });

  test("device-auth reopens direct OpenAI login when the initial device-auth login entry is already on log-in-or-create-account", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const openStep = workflow.do?.find(
      (entry) =>
        "open_device_auth_direct_login_from_initial_login_entry" in entry,
    )?.open_device_auth_direct_login_from_initial_login_entry as
      | { if?: string; with?: { body?: { url?: string } } }
      | undefined;
    const classifyStep = workflow.do?.find(
      (entry) =>
        "classify_device_auth_login_entry_after_initial_reopen" in entry,
    )?.classify_device_auth_login_entry_after_initial_reopen as
      | { if?: string }
      | undefined;
    const cacheStep = workflow.do?.find(
      (entry) => "cache_effective_device_auth_login_entry_state" in entry,
    )?.cache_effective_device_auth_login_entry_state as
      | { set?: Record<string, string> }
      | undefined;

    expect(openStep?.if).toContain(
      "/auth\\.openai\\.com\\/log-in-or-create-account/i.test",
    );
    expect(openStep?.if).toContain(
      "classify_device_auth_login_entry?.action?.login_cta_visible === true",
    );
    expect(openStep?.with?.body?.url).toBe("https://auth.openai.com/log-in");
    expect(classifyStep?.if).toContain(
      "open_device_auth_direct_login_from_initial_login_entry?.action?.ok === true",
    );
    expect(cacheStep?.set?.effective_device_auth_login_entry_state).toContain(
      "classify_device_auth_login_entry_after_initial_reopen",
    );
  });

  test("device-auth reopens direct OpenAI login when the pre-security ChatGPT auth landing falls into log-in-or-create-account", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const openStep = workflow.do?.find(
      (entry) =>
        "open_chatgpt_direct_login_before_security_settings_after_auth_landing" in
        entry,
    )?.open_chatgpt_direct_login_before_security_settings_after_auth_landing as
      | { if?: string; with?: { body?: { url?: string } } }
      | undefined;
    const classifyStep = workflow.do?.find(
      (entry) =>
        "classify_chatgpt_login_entry_before_security_settings_after_auth_landing_reopen" in
        entry,
    )
      ?.classify_chatgpt_login_entry_before_security_settings_after_auth_landing_reopen as
      | { if?: string }
      | undefined;
    const cacheStep = workflow.do?.find(
      (entry) =>
        "cache_effective_chatgpt_login_entry_before_security_settings" in entry,
    )?.cache_effective_chatgpt_login_entry_before_security_settings as
      | { set?: Record<string, string> }
      | undefined;

    expect(openStep?.if).toContain(
      "/auth\\.openai\\.com\\/log-in-or-create-account/i.test",
    );
    expect(openStep?.if).not.toContain("login_cta_visible === true");
    expect(openStep?.with?.body?.url).toBe("https://auth.openai.com/log-in");
    expect(classifyStep?.if).toContain(
      "open_chatgpt_direct_login_before_security_settings_after_auth_landing?.action?.ok === true",
    );
    expect(cacheStep?.set?.pre_settings_login_entry).toContain(
      "classify_chatgpt_login_entry_before_security_settings_after_auth_landing_reopen",
    );
  });

  test("device-auth pre-security ChatGPT login branch tolerates a plain direct log-in surface after reopen", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const fillStep = workflow.do?.find(
      (entry) => "fill_chatgpt_login_email_before_security_settings" in entry,
    )?.fill_chatgpt_login_email_before_security_settings as
      | { if?: string }
      | undefined;

    expect(fillStep?.if).toContain(
      "/auth\\.openai\\.com\\/log-in(?:$|[?#])/i.test",
    );
  });

  test("device-auth promotes prepare_flow_ready only after its own pre-security login branch proves an authenticated ChatGPT shell", async () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain(
      "promote_prepare_flow_ready_after_security_login_recovery",
    );
    expect(workflowText).toContain(
      "state.vars.prepare_flow_ready !== true && state.steps.cache_effective_authenticated_chatgpt_shell_before_security_settings?.action?.value?.authenticated_chatgpt_shell_before_security_settings?.ready === true",
    );
    expect(workflowText).toContain("candidate.ready === true");
    expect(workflowText).not.toContain('candidate.stage === "app_shell"');
  });

  test("submits device-auth verification using the OTP already present in the DOM when workflow state is empty", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "submit_device_auth_login_verification_code",
      `
        <html>
          <head>
            <title>Verify your email</title>
          </head>
          <body>
            <form onsubmit="document.body.innerHTML = '<div>Continue to Codex</div>'; return false;">
              <label>
                Verification code
                <input autocomplete="one-time-code" value="654321" />
              </label>
              <button type="submit">Continue</button>
            </form>
          </body>
        </html>
      `,
      {
        vars: {
          device_auth_login_verification_code: "",
        },
        steps: {
          collect_device_auth_login_verification_artifact: {
            action: {
              ok: true,
            },
          },
        },
      },
      {},
      "https://auth.openai.com/email-verification",
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("654321");
    expect(result.next_stage).toBe("oauth_consent");
  });

  test("keeps prepare verification on the recoverable email-verification path when OpenAI does not advance immediately after submit", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "submit_prepare_login_verification_code",
      `
        <html>
          <head>
            <title>Check your inbox - OpenAI</title>
          </head>
          <body>
            <form onsubmit="return false;">
              <label>
                Code
                <input autocomplete="one-time-code" />
              </label>
              <button type="submit">Continue</button>
            </form>
            <div>Check your inbox</div>
            <div>Enter the verification code we just sent to dev.101@astronlab.com</div>
          </body>
        </html>
      `,
      {
        steps: {
          collect_prepare_login_verification_artifact: {
            action: {
              result: {
                output: {
                  code: "654321",
                },
              },
            },
          },
        },
      },
      {},
      "https://auth.openai.com/email-verification",
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("654321");
    expect(result.next_stage).toBe("email_verification");
    expect(result.strategy).toBe("email-verification-still-open");
  });

  test("submit_prepare_login_verification_code uses the cached prepare OTP when the first Gmail collection loaded late", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "submit_prepare_login_verification_code",
      `
        <html>
          <head>
            <title>Continue to Codex</title>
          </head>
          <body>
            <form onsubmit="return false;">
              <label>
                Code
                <input autocomplete="one-time-code" />
              </label>
              <button
                type="submit"
                onclick="document.body.innerHTML = '<div>Continue to Codex</div><button>Continue to Codex</button>';"
              >
                Continue
              </button>
            </form>
          </body>
        </html>
      `,
      {
        vars: {
          prepare_login_verification_code: "654321",
        },
        steps: {
          recollect_prepare_login_verification_artifact_after_missing_code: {
            action: {
              ok: true,
            },
          },
          collect_prepare_login_verification_artifact: {
            action: {
              result: {
                output: {},
              },
            },
          },
        },
      },
      {},
      "https://auth.openai.com/email-verification",
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("654321");
    expect(result.next_stage).toBe("oauth_consent");
  });

  test("keeps device-auth verification on the recoverable email-verification path when OpenAI does not advance immediately after submit", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "submit_device_auth_login_verification_code",
      `
        <html>
          <head>
            <title>Check your inbox - OpenAI</title>
          </head>
          <body>
            <form onsubmit="return false;">
              <label>
                Code
                <input autocomplete="one-time-code" value="654321" />
              </label>
              <button type="submit">Continue</button>
            </form>
            <div>Check your inbox</div>
            <div>Enter the verification code we just sent to dev.101@astronlab.com</div>
          </body>
        </html>
      `,
      {
        vars: {
          device_auth_login_verification_code: "654321",
        },
      },
      {},
      "https://auth.openai.com/email-verification",
    );

    expect(result.ok).toBe(true);
    expect(result.code).toBe("654321");
    expect(result.next_stage).toBe("email_verification");
    expect(result.strategy).toBe("email-verification-still-open");
  });

  test("retries the prepare signup password shell when OpenAI returns a retryable invalid_auth_step page", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const retryStep = workflow.do?.find(
      (entry) => "click_prepare_signup_password_timeout_retry" in entry,
    )?.click_prepare_signup_password_timeout_retry as
      | {
          if?: string;
        }
      | undefined;
    const gateStep = workflow.do?.find(
      (entry) => "classify_prepare_after_signup_password_gate" in entry,
    )?.classify_prepare_after_signup_password_gate as
      | {
          if?: string;
        }
      | undefined;

    expect(retryStep?.if).toContain(
      "classify_prepare_after_signup_password?.action?.retryable_timeout === true",
    );
    expect(retryStep?.if).toContain("/create-account/i.test");
    expect(gateStep?.if).toContain(
      "click_prepare_signup_password_timeout_retry?.action?.ok === true",
    );
    expect(gateStep?.if).toContain(
      "submit_prepare_signup_password_after_timeout_retry?.action?.ok === true",
    );
  });

  test("marks repeated pending-account invalid_state as skip_account for device auth", async () => {
    const result = await runWorkflowRunScript(
      deviceAuthWorkflowPath,
      "compute_prepare_flow_result",
      {},
      {
        vars: {
          reuse_device_auth_session: false,
        },
        steps: {
          classify_prepare_after_login_email_signup_invalid_state_fallback: {
            action: {
              stage: "retryable_timeout",
              retryable_timeout: true,
              current_url: "https://auth.openai.com/log-in",
            },
          },
        },
      },
    );

    expect(result.prepare_flow_ready).toBe(false);
    expect(
      (result.prepare_flow_output as Record<string, unknown>)?.next_action,
    ).toBe("skip_account");
  });

  test("does not treat a public ChatGPT shell as a successful authenticated prepare state for device auth", async () => {
    const result = await runWorkflowRunScript(
      deviceAuthWorkflowPath,
      "compute_prepare_flow_result",
      {},
      {
        vars: {
          reuse_device_auth_session: false,
        },
        steps: {
          inspect_prepare_public_chatgpt_entry_after_ui_logout: {
            action: {
              stage: "public_chatgpt",
              chatgpt_public_shell: true,
              chatgpt_app_shell: false,
              account_ready: false,
              current_url: "https://chatgpt.com/",
            },
          },
        },
      },
    );

    expect(result.prepare_flow_ready).toBe(false);
    expect((result.prepare_flow_output as Record<string, unknown>)?.stage).toBe(
      "public_chatgpt",
    );
  });

  test("prefers the latest authenticated prepare result over older login-stage candidates for device auth", async () => {
    const result = await runWorkflowRunScript(
      deviceAuthWorkflowPath,
      "compute_prepare_flow_result",
      {},
      {
        vars: {
          reuse_device_auth_session: false,
        },
        steps: {
          classify_prepare_after_signup_about_you: {
            action: {
              stage: "app_shell",
              account_ready: true,
              chatgpt_app_shell: true,
              chatgpt_public_shell: false,
              current_url: "https://chatgpt.com/",
            },
          },
          classify_prepare_after_login_email: {
            action: {
              stage: "login_email",
              account_ready: false,
              chatgpt_app_shell: false,
              chatgpt_public_shell: false,
              current_url: "https://auth.openai.com/log-in",
            },
          },
        },
      },
    );

    expect(result.prepare_flow_ready).toBe(true);
    expect((result.prepare_flow_output as Record<string, unknown>)?.stage).toBe(
      "app_shell",
    );
    expect(
      (result.prepare_flow_output as Record<string, unknown>)?.current_url,
    ).toBe("https://chatgpt.com/");
  });

  test("classifies the authenticated ChatGPT onboarding shell as app_shell, not public_chatgpt", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "classify_prepare_after_login_email",
      `
        <html>
          <head>
            <title>ChatGPT</title>
          </head>
          <body>
            <div>Skip to content</div>
            <div>What brings you to ChatGPT?</div>
            <button type="button">School</button>
            <button type="button">Work</button>
            <button type="button">Next</button>
            <button type="button">Skip</button>
            <nav>
              <button type="button">New chat</button>
              <button type="button">Search chats</button>
              <button type="button">Images</button>
              <button type="button">Apps</button>
              <button type="button">Projects</button>
              <button type="button">Codex</button>
            </nav>
            <div>Dev Astronlab</div>
            <div>Where should we begin?</div>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/",
    );

    expect(result.ok).toBe(true);
    expect(result.stage).toBe("app_shell");
    expect(result.chatgpt_app_shell).toBe(true);
    expect(result.chatgpt_public_shell).toBe(false);
    expect(result.account_ready).toBe(true);
  });

  test("exposes login CTA visibility on the ChatGPT auth landing so the second-click recovery can run", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "classify_prepare_login_entry",
      `
        <html>
          <head>
            <title>ChatGPT</title>
          </head>
          <body>
            <div>Get started</div>
            <button type="button">Log in</button>
            <button type="button">Sign up for free</button>
            <div>Try it first</div>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/auth/login?next=%2F%3Fcodex_rotate_public_entry%3Ddirect_login",
    );

    expect(result.ok).toBe(true);
    expect(result.stage).toBe("public_chatgpt");
    expect(result.login_cta_visible).toBe(true);
    expect(result.signup_cta_visible).toBe(true);
  });

  test("prefers the logged-out ChatGPT public shell over sidebar chrome when login CTAs are visible", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "classify_prepare_login_entry",
      `
        <html>
          <head>
            <title>ChatGPT</title>
          </head>
          <body>
            <div>New chat</div>
            <div>Search chats</div>
            <div>Images</div>
            <div>Get responses tailored to you</div>
            <button type="button">Log in</button>
            <button type="button">Sign up for free</button>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/",
    );

    expect(result.ok).toBe(true);
    expect(result.stage).toBe("public_chatgpt");
    expect(result.chatgpt_public_shell).toBe(true);
    expect(result.chatgpt_app_shell).toBe(false);
    expect(result.login_cta_visible).toBe(true);
    expect(result.signup_cta_visible).toBe(true);
  });

  test("opens a clean direct OpenAI login entry from the public ChatGPT shell before choosing signup", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const step = workflow.do?.find(
      (entry) => "click_prepare_signup_button" in entry,
    )?.click_prepare_signup_button as
      | {
          call?: string;
          with?: { body?: { url?: string } };
          metadata?: {
            browser?: {
              clearSiteDataForOrigins?: string[];
            };
          };
        }
      | undefined;

    expect(step?.call).toBe("afn.driver.browser.navigate");
    expect(step?.with?.body?.url).toBe("https://auth.openai.com/log-in");
    expect(step?.metadata?.browser?.clearSiteDataForOrigins).toEqual([
      "https://auth.openai.com",
      "https://chatgpt.com",
      "https://chat.openai.com",
    ]);
  });

  test("opens a clean direct OpenAI login entry from the public ChatGPT shell", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const step = workflow.do?.find(
      (entry) => "click_prepare_login_button" in entry,
    )?.click_prepare_login_button as
      | {
          call?: string;
          with?: { body?: { url?: string } };
          metadata?: {
            browser?: {
              clearSiteDataForOrigins?: string[];
            };
          };
        }
      | undefined;

    expect(step?.call).toBe("afn.driver.browser.navigate");
    expect(step?.with?.body?.url).toBe("https://auth.openai.com/log-in");
    expect(step?.metadata?.browser?.clearSiteDataForOrigins).toEqual([
      "https://auth.openai.com",
      "https://chatgpt.com",
      "https://chat.openai.com",
    ]);
  });

  test("switches the prepare branch to signup from the clean OpenAI login shell", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const chooseStep = workflow.do?.find(
      (entry) => "choose_prepare_signup_branch" in entry,
    )?.choose_prepare_signup_branch as
      | { if?: string; metadata?: { templateRef?: string } }
      | undefined;
    const classifyStep = workflow.do?.find(
      (entry) => "classify_prepare_after_choose_signup" in entry,
    )?.classify_prepare_after_choose_signup as
      | { if?: string; metadata?: { templateRef?: string } }
      | undefined;

    expect(chooseStep?.metadata?.templateRef).toBe("click_signup_button");
    expect(chooseStep?.if).toContain(
      "effective_prepare_login_entry_state?.stage === 'login_email'",
    );
    expect(chooseStep?.if).toContain(
      "effective_prepare_login_entry_state?.signup_cta_visible === true",
    );
    expect(classifyStep?.metadata?.templateRef).toBe(
      "classify_openai_auth_surface",
    );
    expect(classifyStep?.if).toContain(
      "choose_prepare_signup_branch?.action?.ok === true",
    );
  });

  test("enables the device-auth security toggle when ChatGPT renders it as a plain button in the target row", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "ensure_device_code_authorization_enabled",
      `
        <html>
          <body>
            <div role="dialog">
              <div>Security</div>
              <section>
                <div>Enable device code authorization for Codex</div>
                <div>Use device code sign-in for headless or remote environments.</div>
                <button type="button" id="toggle"><span>toggle</span></button>
              </section>
            </div>
            <script>
              const toggle = document.getElementById("toggle");
              toggle?.setAttribute("data-state", "off");
              toggle?.addEventListener("click", () => {
                toggle.setAttribute("data-state", "checked");
              });
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/#settings/Security",
    );

    expect(result.ok).toBe(true);
    expect(result.enabled).toBe(true);
  });

  test("does not block device-auth start when the target security row is present but toggle state is unreadable", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "ensure_device_code_authorization_enabled",
      `
        <html>
          <body>
            <div role="dialog">
              <div>Security</div>
              <section>
                <div>Enable device code authorization for Codex</div>
                <div>Use device code sign-in for headless or remote environments.</div>
                <button type="button" id="toggle"><span>toggle</span></button>
              </section>
            </div>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/#settings/Security",
    );

    expect(result.ok).toBe(true);
    expect(result.enabled).toBe(true);
    expect(result.verification_pending).toBe(true);
    expect(result.ambiguous_enabled_state).toBe(true);
  });

  test("falls back to a row-edge click when the Codex security row is visible but exposes no semantic switch control", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "ensure_device_code_authorization_enabled",
      `
        <html>
          <body>
            <div role="dialog">
              <section id="row" style="position: relative; width: 520px; height: 120px;">
                <div>Enable device code authorization for Codex</div>
                <div>Use device code sign-in for headless or remote environments.</div>
                <div
                  id="toggle-hitbox"
                  style="position: absolute; right: 0; top: 36px; width: 56px; height: 28px; background: #ddd; border-radius: 999px;"
                ></div>
              </section>
            </div>
            <script>
              document.getElementById("toggle-hitbox")?.addEventListener("click", () => {
                document.getElementById("toggle-hitbox")?.setAttribute("data-toggled", "true");
              });
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/#settings/Security",
    );

    expect(result.ok).toBe(true);
    expect(result.enabled).toBe(true);
    expect(result.verification_pending).toBe(true);
  });

  test("returns to the exact device-auth verification URL after Gmail collection", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const returnStep = workflow.do?.find(
      (entry) => "return_to_device_auth_login_verification_page" in entry,
    )?.return_to_device_auth_login_verification_page as
      | {
          with?: {
            body?: {
              url?: string;
            };
          };
        }
      | undefined;

    expect(returnStep?.with?.body?.url).toContain(
      "state.steps.classify_device_auth_after_login_password_gate?.action?.current_url",
    );
    expect(returnStep?.with?.body?.url).not.toBe(
      "https://auth.openai.com/email-verification",
    );
  });

  test("returns to the stable prepare verification URL after Gmail collection", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const returnStep = workflow.do?.find(
      (entry) => "return_to_prepare_login_verification_page" in entry,
    )?.return_to_prepare_login_verification_page as
      | {
          with?: {
            body?: {
              url?: string;
            };
          };
        }
      | undefined;
    const retryReturnStep = workflow.do?.find(
      (entry) =>
        "return_to_prepare_login_verification_page_after_incorrect_code" in
        entry,
    )?.return_to_prepare_login_verification_page_after_incorrect_code as
      | {
          with?: {
            body?: {
              url?: string;
            };
          };
        }
      | undefined;

    expect(returnStep?.with?.body?.url).toBe(
      "https://auth.openai.com/email-verification",
    );
    expect(retryReturnStep?.with?.body?.url).toBe(
      "https://auth.openai.com/email-verification",
    );
  });

  test("device-auth retries the prepare Gmail-return invalid_state shell before falling back", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const recoverStep = workflow.do?.find(
      (entry) => "recover_prepare_login_verification_return_timeout" in entry,
    )?.recover_prepare_login_verification_return_timeout as
      | {
          if?: string;
          metadata?: {
            templateRef?: string;
          };
        }
      | undefined;
    const classifyAfterRecoverStep = workflow.do?.find(
      (entry) =>
        "classify_prepare_login_verification_ready_after_return_timeout" in
        entry,
    )?.classify_prepare_login_verification_ready_after_return_timeout as
      | {
          if?: string;
          metadata?: {
            templateRef?: string;
          };
        }
      | undefined;

    expect(recoverStep?.metadata?.templateRef).toBe(
      "click_retryable_try_again",
    );
    expect(recoverStep?.if).toContain(
      "classify_prepare_login_verification_ready?.action?.retryable_timeout === true",
    );
    expect(classifyAfterRecoverStep?.metadata?.templateRef).toBe(
      "classify_openai_auth_surface",
    );
    expect(classifyAfterRecoverStep?.if).toContain(
      "recover_prepare_login_verification_return_timeout?.action?.ok === true",
    );
  });

  test("device-auth reuses the recovered prepare Gmail-return classification in downstream recovery", async () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain(
      "classify_prepare_login_verification_ready_after_return_timeout?.action?.stage === 'login_email'",
    );
    expect(workflowText).toContain(
      "classify_prepare_login_verification_ready_after_return_timeout?.action?.stage === 'login_password'",
    );
    expect(workflowText).toContain(
      "classify_prepare_login_verification_ready_after_return_timeout?.action?.stage === 'email_verification'",
    );
    expect(workflowText).toContain(
      "state.steps?.classify_prepare_login_verification_ready_after_return_timeout?.action",
    );
  });

  test("device-auth retries the restored prepare email step once more after Gmail return before giving up on one-time-code recovery", async () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain(
      "refill_prepare_login_email_after_return_retry",
    );
    expect(workflowText).toContain(
      "continue_prepare_login_email_after_return_retry",
    );
    expect(workflowText).toContain(
      "submit_prepare_login_email_after_return_retry",
    );
    expect(workflowText).toContain("classify_prepare_login_after_return_retry");
    expect(workflowText).toContain(
      "state.steps.classify_prepare_login_after_return?.action?.stage === 'login_email'",
    );
    expect(workflowText).toContain(
      "state.steps.classify_prepare_login_after_return_retry?.action?.stage === 'login_password'",
    );
    expect(workflowText).toContain(
      "state.steps.classify_prepare_login_after_return_retry?.action?.stage === 'email_verification'",
    );
  });

  test("device-auth reopens prepare one-time-code recovery from a restored login shell when the CTA is visible", async () => {
    const workflowText = readFileSync(deviceAuthWorkflowPath, "utf8");

    expect(workflowText).toContain(
      "state.steps.classify_prepare_login_after_return?.action?.stage === 'login_email'",
    );
    expect(workflowText).toContain(
      "state.steps.classify_prepare_login_after_return?.action?.one_time_code_cta === true",
    );
    expect(workflowText).toContain(
      "state.steps.classify_prepare_login_after_return_retry?.action?.one_time_code_cta === true",
    );
  });

  test("original signup password submit treats direct navigation to email verification as progress", async () => {
    const result = await runWorkflowStepScript(
      originalWorkflowPath,
      "submit_signup_password",
      `
        <html>
          <body style="min-height: 100vh;">
            <script>
              const render = () => {
                if (/email-verification/i.test(location.pathname)) {
                  document.body.innerHTML = \`
                    <main>
                      <h1>Check your inbox</h1>
                      <input name="code" inputmode="numeric" />
                      <button type="button">Continue</button>
                    </main>
                  \`;
                  return;
                }
                document.body.innerHTML = \`
                  <form id="signup-form">
                    <input type="password" name="new-password" autocomplete="new-password" value="super-secret-password" />
                    <button type="submit" id="continue">Continue</button>
                  </form>
                \`;
                const goNext = (event) => {
                  event.preventDefault();
                  location.href = "https://auth.openai.com/email-verification";
                };
                document.getElementById("continue")?.addEventListener("click", goNext);
                document.getElementById("signup-form")?.addEventListener("submit", goNext);
              };
              render();
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://auth.openai.com/create-account/password",
    );

    expect(result.ok).toBe(true);
    expect(result.next_stage).toBe("email_verification");
    expect(String(result.current_url || "")).toContain("email-verification");
  });

  test("original signup password helper matches the strict stepwise navigation guard", async () => {
    const originalWorkflow = await loadWorkflow(originalWorkflowPath);
    const stepwiseWorkflow = await loadWorkflow(stepwiseWorkflowPath);

    const originalScript =
      originalWorkflow.use?.functions?.submit_openai_password_form?.with?.body
        ?.script;
    const stepwiseScript =
      stepwiseWorkflow.use?.functions?.submit_openai_password_form?.with?.body
        ?.script;

    expect(String(originalScript || "")).toContain(
      "Execution context was destroyed|Cannot find context|Target closed|Frame was detached",
    );
    expect(String(originalScript || "")).not.toContain(
      "Timeout \\\\d+ms exceeded",
    );
    expect(String(originalScript || "")).toContain("const passwordSelector =");
    expect(String(originalScript || "")).toContain(
      "await page.$(passwordSelector)",
    );
    expect(String(originalScript || "")).not.toContain(
      'page.locator(\'input[type="password"], input[name="password"], input[name="new-password"], input[autocomplete="current-password"], input[autocomplete="new-password"]\').first()',
    );
    expect(String(stepwiseScript || "")).toContain(
      "Execution context was destroyed|Cannot find context|Target closed|Frame was detached",
    );
    expect(String(stepwiseScript || "")).not.toContain(
      "Timeout \\\\d+ms exceeded",
    );
    expect(String(stepwiseScript || "")).toContain("const passwordSelector =");
    expect(String(stepwiseScript || "")).toContain(
      "await page.$(passwordSelector)",
    );
    expect(String(stepwiseScript || "")).not.toContain(
      'page.locator(\'input[type="password"], input[name="password"], input[name="new-password"], input[autocomplete="current-password"], input[autocomplete="new-password"]\').first()',
    );
  });

  test("device-auth signup password submit treats direct navigation to email verification as progress", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "submit_prepare_signup_password",
      `
        <html>
          <body style="min-height: 100vh;">
            <script>
              const render = () => {
                if (/email-verification/i.test(location.pathname)) {
                  document.body.innerHTML = \`
                    <main>
                      <h1>Check your inbox</h1>
                      <input name="code" inputmode="numeric" />
                      <button type="button">Continue</button>
                    </main>
                  \`;
                  return;
                }
                document.body.innerHTML = \`
                  <form id="signup-form">
                    <input type="password" name="new-password" autocomplete="new-password" value="super-secret-password" />
                    <button type="submit" id="continue">Continue</button>
                  </form>
                \`;
                const goNext = (event) => {
                  event.preventDefault();
                  location.href = "https://auth.openai.com/email-verification";
                };
                document.getElementById("continue")?.addEventListener("click", goNext);
                document.getElementById("signup-form")?.addEventListener("submit", goNext);
              };
              render();
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://auth.openai.com/create-account/password",
    );

    expect(result.ok).toBe(true);
    expect(result.next_stage).toBe("email_verification");
    expect(String(result.current_url || "")).toContain("email-verification");
  });

  test("device-auth prepare verification submit reuses the code already present in the page when workflow state lost it", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "submit_prepare_login_verification_code",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent to devbench.23@astronlab.com</p>
            <input inputmode="numeric" aria-label="Code" value="654321" />
            <button id="continue" type="button">Continue</button>
            <script>
              const input = document.querySelector("input");
              document.getElementById("continue").addEventListener("click", () => {
                if (String(input.value || "") === "654321") {
                  document.body.innerHTML =
                    '<h1>Phone number required</h1><p>To continue, please add a phone number.</p>';
                  history.replaceState({}, "", "https://auth.openai.com/add-phone");
                }
              });
            </script>
          </body>
        </html>
      `,
      {
        steps: {
          fill_prepare_login_verification_code: {
            action: {
              ok: true,
            },
          },
        },
      },
      {},
      "https://auth.openai.com/email-verification",
    );

    expect(result.ok).toBe(true);
    expect(result.next_stage).toBe("add_phone");
    expect(String(result.current_url || "")).toContain("add-phone");
  });

  test("device-auth replacement prepare verification submit treats direct navigation as progress", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "submit_prepare_login_verification_code_after_incorrect_code",
      `
        <html>
          <body style="min-height: 100vh;">
            <h1>Check your inbox</h1>
            <p>Enter the verification code we just sent to devbench.23@astronlab.com</p>
            <input inputmode="numeric" aria-label="Code" value="112233" />
            <button id="continue" type="button">Continue</button>
            <script>
              const input = document.querySelector("input");
              document.getElementById("continue").addEventListener("click", () => {
                if (String(input.value || "") === "112233") {
                  location.href = "https://auth.openai.com/add-phone";
                }
              });
            </script>
          </body>
        </html>
      `,
      {
        vars: {},
        steps: {
          fill_prepare_login_verification_code_after_incorrect_code: {
            action: {
              ok: true,
            },
          },
          recollect_prepare_login_verification_artifact_after_incorrect_code: {
            action: {
              result: {
                output: {
                  code: "112233",
                },
              },
            },
          },
        },
      },
      {},
      "https://auth.openai.com/email-verification",
    );

    expect(result.ok).toBe(true);
    expect(result.next_stage).toBe("add_phone");
    expect(String(result.current_url || "")).toContain("add-phone");
  });

  test("classifies an authenticated ChatGPT settings shell after reused login", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "inspect_device_authorization_after_login",
      `
        <html>
          <body style="min-height: 100vh;">
            <div role="dialog">
              <div>General</div>
              <div>Notifications</div>
              <div>Security</div>
              <div>Account</div>
              <div>Name</div>
              <div>Email</div>
              <div>1.dev.astronlab@gmail.com</div>
            </div>
            <div>New chat</div>
            <div>Projects</div>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/#settings/Account",
    );

    expect(result.ok).toBe(true);
    expect(result.chatgpt_app_shell).toBe(true);
    expect(result.chatgpt_security_shell).toBe(false);
    expect(result.chatgpt_public_shell).toBe(false);
    expect(result.auth_prompt).toBe(false);
  });

  test("reopens the saved device challenge when reused login lands on an authenticated ChatGPT shell", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const reopenStep = workflow.do?.find(
      (entry) => "reopen_device_authorization_entry_after_login" in entry,
    )?.reopen_device_authorization_entry_after_login as
      | {
          if?: string;
        }
      | undefined;

    expect(reopenStep?.if).toContain(
      "state.steps.inspect_device_authorization_after_login?.action?.chatgpt_app_shell === true",
    );
    expect(reopenStep?.if).toContain(
      "state.steps.inspect_device_authorization_after_login?.action?.chatgpt_public_shell === true",
    );
  });

  test("waits through a temporary ChatGPT settings shell until the device-auth consent surface appears", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "inspect_device_authorization_after_login",
      `
        <html>
          <body style="min-height: 100vh;">
            <div role="dialog" id="settings-shell">
              <div>General</div>
              <div>Notifications</div>
              <div>Security</div>
              <div>Account</div>
              <div>Email</div>
              <div>1.dev.astronlab@gmail.com</div>
            </div>
            <script>
              setTimeout(() => {
                document.body.innerHTML =
                  '<h1>Sign in to Codex with ChatGPT</h1><p>Share your name, email, and profile picture with Codex</p><button>Continue to Codex</button>';
              }, 1500);
            </script>
          </body>
        </html>
      `,
      {},
      {},
      "https://chatgpt.com/#settings/Account",
    );

    expect(result.ok).toBe(true);
    expect(result.saw_oauth_consent).toBe(true);
    expect(result.oauth_continue_visible).toBe(true);
    expect(result.chatgpt_app_shell).toBe(false);
  });

  test("waits through a temporary reopened login prompt until the device-auth consent surface appears", async () => {
    const result = await runWorkflowStepScript(
      deviceAuthWorkflowPath,
      "inspect_device_authorization_after_login_reopen",
      `
        <html>
          <body>
            <h1>Welcome back</h1>
            <label>
              Email address
              <input type="email" autocomplete="email" />
            </label>
            <button>Continue</button>
            <script>
              setTimeout(() => {
                document.body.innerHTML =
                  '<h1>Sign in to Codex with ChatGPT</h1><p>Share your name, email, and profile picture with Codex</p><button>Continue to Codex</button>';
              }, 1500);
            </script>
          </body>
        </html>
      `,
      {
        vars: {
          codex_login: {
            auth_url:
              "https://auth.openai.com/codex/device?user_code=ABCD-EFGHI",
          },
        },
      },
      {},
      "https://auth.openai.com/log-in",
    );

    expect(result.ok).toBe(true);
    expect(result.saw_oauth_consent).toBe(true);
    expect(result.oauth_continue_visible).toBe(true);
    expect(result.auth_prompt).toBe(false);
  });

  test("does not preserve a stale device-auth-disabled flag once the device-auth surface advances to consent", async () => {
    const workflow = await loadWorkflow(deviceAuthWorkflowPath);
    const cacheStep = workflow.do?.find(
      (entry) => "cache_effective_device_authorization_surface" in entry,
    )?.cache_effective_device_authorization_surface as
      | { set?: Record<string, string> }
      | undefined;
    const consentStep = workflow.do?.find(
      (entry) => "submit_device_authorization_consent" in entry,
    )?.submit_device_authorization_consent as { if?: string } | undefined;
    const exitStep = workflow.do?.find(
      (entry) => "wait_for_codex_login_exit" in entry,
    )?.wait_for_codex_login_exit as { if?: string } | undefined;

    expect(cacheStep?.set?.device_authorization_surface).toContain(
      "oauth_continue_visible === true",
    );
    expect(consentStep?.if).toContain(
      "state.steps.inspect_device_authorization_after_code?.action?.success === true",
    );
    expect(exitStep?.if).toContain(
      "state.vars.device_authorization_surface?.success === true",
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
