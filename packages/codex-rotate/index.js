#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const MODULE_DIR = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(MODULE_DIR, "..", "..");
const CLI_BINARY_NAME =
  process.platform === "win32" ? "codex-rotate.exe" : "codex-rotate";

function resolveCliBinary() {
  const candidates = [
    process.env.CODEX_ROTATE_BIN,
    process.env.CODEX_ROTATE_CLI_BIN,
    join(MODULE_DIR, "bin", CLI_BINARY_NAME),
    join(MODULE_DIR, "dist", "bin", CLI_BINARY_NAME),
    join(REPO_ROOT, "target", "debug", CLI_BINARY_NAME),
    join(REPO_ROOT, "target", "release", CLI_BINARY_NAME),
  ].filter(Boolean);

  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }

  const checked = candidates.map((candidate) => `  - ${candidate}`).join("\n");
  throw new Error(
    [
      "Unable to find the codex-rotate CLI binary.",
      checked ? `Checked:\n${checked}` : "",
    ]
      .filter(Boolean)
      .join("\n"),
  );
}

function main() {
  const cliBinary = resolveCliBinary();
  const result = spawnSync(cliBinary, process.argv.slice(2), {
    stdio: "inherit",
    env: process.env,
  });

  if (result.error) {
    throw result.error;
  }

  process.exit(result.status ?? 1);
}

try {
  main();
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exit(1);
}
