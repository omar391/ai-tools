#!/usr/bin/env -S node --experimental-strip-types

import { execFile } from "node:child_process";
import { createRequire } from "node:module";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { promisify } from "node:util";
import { fileURLToPath, pathToFileURL } from "node:url";

const execFileAsync = promisify(execFile);
const MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(MODULE_DIR, "..", "..");
const FAST_BROWSER_CLIENT_MODULE = pathToFileURL(
  path.resolve(
    REPO_ROOT,
    "..",
    "ai-rules",
    "skills",
    "fast-browser",
    "lib",
    "daemon",
    "client.mjs",
  ),
).href;
const PROFILE_NAME =
  String(process.env.FAST_BROWSER_PROFILE || "dev-1").trim() || "dev-1";
const LOG_PATH =
  process.env.CODEX_ROTATE_BROWSER_SHIM_LOG ||
  path.join(os.tmpdir(), "codex-rotate-managed-browser-opener.log");
const USER_DATA_DIR = path.join(
  os.homedir(),
  ".fast-browser",
  "profiles",
  PROFILE_NAME,
);

function pickUrl(argv: string[]): string | null {
  for (const value of argv) {
    const trimmed = String(value || "").trim();
    if (/^https?:\/\//i.test(trimmed)) {
      return trimmed;
    }
  }
  return null;
}

async function appendLog(
  message: string,
  details: unknown = null,
): Promise<void> {
  const line = `[${new Date().toISOString()}] ${message}${details ? ` ${JSON.stringify(details)}` : ""}\n`;
  await fs.appendFile(LOG_PATH, line, "utf8").catch(() => {});
}

async function findManagedChromeDebugPort(): Promise<number> {
  const { stdout } = await execFileAsync("ps", ["-Ao", "pid=,command="], {
    encoding: "utf8",
    maxBuffer: 10 * 1024 * 1024,
  });
  const needle = `--user-data-dir=${USER_DATA_DIR}`;
  for (const line of stdout.split("\n")) {
    if (!line.includes(needle) || !line.includes("--remote-debugging-port=")) {
      continue;
    }
    if (!line.includes("Google Chrome") || line.includes("Helper")) {
      continue;
    }
    const portMatch = line.match(/--remote-debugging-port=(\d+)/);
    if (portMatch) {
      return Number.parseInt(portMatch[1], 10);
    }
  }
  throw new Error(
    `Could not find a remote debugging port for managed profile '${PROFILE_NAME}'.`,
  );
}

async function main(): Promise<void> {
  process.chdir(REPO_ROOT);

  const url = pickUrl(process.argv.slice(2));
  if (!url) {
    const argv = process.argv.slice(2);
    await appendLog("browser_shim_blocked_non_url_open", { argv });
    process.stderr.write(
      "Managed Codex browser opener refused a non-URL browser launch request.\n",
    );
    process.exit(1);
  }

  await appendLog("browser_shim_invoked", {
    profile: PROFILE_NAME,
    url,
    argv: process.argv.slice(2),
  });

  const client = await import(FAST_BROWSER_CLIENT_MODULE);
  await client.ensureProfileReady({ profileName: PROFILE_NAME, headed: false });

  const port = await findManagedChromeDebugPort();
  const requireFromWorkspace = createRequire(
    path.join(REPO_ROOT, "package.json"),
  );
  const { chromium } = requireFromWorkspace("playwright");
  const browser = await chromium.connectOverCDP(`http://127.0.0.1:${port}`);
  const context = browser.contexts()[0];
  if (!context) {
    throw new Error(
      `Managed profile '${PROFILE_NAME}' did not expose a default browser context.`,
    );
  }

  const page = await context.newPage();
  await page.goto(url, { waitUntil: "domcontentloaded" });
  const result = {
    ok: true,
    profile: PROFILE_NAME,
    port,
    final_url: page.url(),
    title: await page.title().catch(() => null),
    page_count: context.pages().length,
  };
  await appendLog("browser_shim_opened_url", result);
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

main().catch(async (error) => {
  const message = error instanceof Error ? error.message : String(error);
  await appendLog("browser_shim_failed", {
    message,
    stack: error instanceof Error ? error.stack : null,
  });
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
