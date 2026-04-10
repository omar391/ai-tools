import { describe, expect, setDefaultTimeout, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { chromium } from "playwright";

import {
  buildFastBrowserWorkflowError,
  hydrateFastBrowserRunResultFromObservability,
  isFastBrowserRunResultFailure,
  shouldPromptForCodexRotateSecretUnlock,
} from "./automation.ts";

setDefaultTimeout(30_000);

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
    throw new Error(`${stepId} script was not found in ${workflowPath}`);
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
        body: html,
      });
    });
    await page.goto(url);
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
    expect(result.stdout).toContain("codex-rotate-account-flow-stepwise.yaml");
    expect(result.stdout).toContain("codex-rotate-account-flow-minimal.yaml");
    expect(result.stdout).toContain(
      "codex-rotate-account-flow-device-auth.yaml",
    );
  });

  test("main remains pinned to the single stepwise flow while benchmarking", async () => {
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
    ]);
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
    expect(result.current_url).toBe(
      "https://auth.openai.com/create-account/password",
    );
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
    expect(result.chatgpt_public_shell).toBe(false);
    expect(result.auth_prompt).toBe(false);
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
