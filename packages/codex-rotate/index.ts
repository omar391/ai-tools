#!/usr/bin/env bun

import { spawn, spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const MODULE_DIR = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(MODULE_DIR, "..", "..");
const IS_WINDOWS = process.platform === "win32";
const RUST_BIN =
  process.env.CODEX_ROTATE_RUST_BIN ??
  join(
    REPO_ROOT,
    "target",
    "debug",
    IS_WINDOWS ? "codex-rotate-cli.exe" : "codex-rotate-cli",
  );
const TRAY_BIN =
  process.env.CODEX_ROTATE_TRAY_BIN ??
  join(
    REPO_ROOT,
    "target",
    "debug",
    IS_WINDOWS ? "codex-rotate-tray.exe" : "codex-rotate-tray",
  );
const TRAY_MANIFEST = join(
  REPO_ROOT,
  "packages",
  "codex-rotate-app",
  "src-tauri",
  "Cargo.toml",
);
const CARGO_BIN = process.env.CARGO_BIN ?? "cargo";
const directBinaryExists = existsSync(RUST_BIN);

const args = process.argv.slice(2);
const command = args[0];

if (command === "tray") {
  process.exit(handleTrayCommand(args.slice(1)));
}

if (
  args.length === 0 ||
  command === "help" ||
  command === "--help" ||
  command === "-h"
) {
  process.exit(printHelp());
}

const runner = directBinaryExists ? RUST_BIN : CARGO_BIN;
const runnerArgs = directBinaryExists
  ? args
  : ["run", "--quiet", "--package", "codex-rotate-cli", "--", ...args];

const result = spawnSync(runner, runnerArgs, {
  cwd: REPO_ROOT,
  stdio: "inherit",
  env: process.env,
});

if (result.error) {
  console.error(result.error.message || `Failed to run ${runner}`);
  process.exit(1);
}

if (result.signal) {
  console.error(`codex-rotate-cli was interrupted by signal ${result.signal}.`);
  process.exit(1);
}

process.exit(typeof result.status === "number" ? result.status : 1);

function printHelp(): number {
  const helpResult = spawnSync(
    directBinaryExists ? RUST_BIN : CARGO_BIN,
    directBinaryExists
      ? ["help"]
      : ["run", "--quiet", "--package", "codex-rotate-cli", "--", "help"],
    {
      cwd: REPO_ROOT,
      env: process.env,
      encoding: "utf8",
    },
  );

  if (helpResult.error) {
    console.error(
      helpResult.error.message || "Failed to print codex-rotate help.",
    );
    return 1;
  }

  if (helpResult.stdout) {
    process.stdout.write(helpResult.stdout);
  }
  if (helpResult.stderr) {
    process.stderr.write(helpResult.stderr);
  }

  process.stdout.write(`
Wrapper Commands
  tray open           Start the Codex Rotate tray app
  tray quit           Stop the Codex Rotate tray app
  tray restart        Restart the Codex Rotate tray app
  tray status         Show whether the Codex Rotate tray app is running
`);

  return typeof helpResult.status === "number" ? helpResult.status : 0;
}

function handleTrayCommand(args: string[]): number {
  const subcommand = args[0] ?? "open";
  switch (subcommand) {
    case "open":
      return trayOpen();
    case "quit":
      return trayQuit();
    case "restart": {
      const quitStatus = trayQuit();
      if (quitStatus !== 0) {
        return quitStatus;
      }
      return trayOpen();
    }
    case "status":
      return trayStatus();
    case "help":
    case "--help":
    case "-h":
      process.stdout.write(
        `Usage: codex-rotate tray <open|quit|restart|status>\n`,
      );
      return 0;
    default:
      console.error(
        `Unknown tray command: "${subcommand}". Run "codex-rotate tray help" for usage.`,
      );
      return 1;
  }
}

function trayOpen(): number {
  if (isTrayRunning()) {
    process.stdout.write("Codex Rotate tray is already running.\n");
    return 0;
  }
  if (!ensureTrayBinary()) {
    return 1;
  }
  const child = spawn(TRAY_BIN, [], {
    cwd: REPO_ROOT,
    detached: true,
    env: process.env,
    stdio: "ignore",
  });
  child.unref();
  process.stdout.write("Started Codex Rotate tray.\n");
  return 0;
}

function trayQuit(): number {
  if (!isTrayRunning()) {
    process.stdout.write("Codex Rotate tray is not running.\n");
    return 0;
  }
  const result = spawnSync("pkill", ["-f", TRAY_BIN], {
    cwd: REPO_ROOT,
    env: process.env,
    encoding: "utf8",
  });
  if (result.error) {
    console.error(result.error.message || "Failed to stop Codex Rotate tray.");
    return 1;
  }
  if ((result.status ?? 1) !== 0) {
    const stderr = result.stderr?.trim();
    if (stderr) {
      console.error(stderr);
    } else {
      console.error("Failed to stop Codex Rotate tray.");
    }
    return 1;
  }
  process.stdout.write("Stopped Codex Rotate tray.\n");
  return 0;
}

function trayStatus(): number {
  process.stdout.write(
    `${isTrayRunning() ? "Codex Rotate tray is running." : "Codex Rotate tray is not running."}\n`,
  );
  return 0;
}

function isTrayRunning(): boolean {
  if (IS_WINDOWS) {
    return existsSync(TRAY_BIN);
  }
  const result = spawnSync("pgrep", ["-f", TRAY_BIN], {
    cwd: REPO_ROOT,
    env: process.env,
    stdio: "ignore",
  });
  return result.status === 0;
}

function ensureTrayBinary(): boolean {
  if (existsSync(TRAY_BIN)) {
    return true;
  }
  const result = spawnSync(
    CARGO_BIN,
    [
      "build",
      "--quiet",
      "--manifest-path",
      TRAY_MANIFEST,
      "--bin",
      "codex-rotate-tray",
    ],
    {
      cwd: REPO_ROOT,
      env: process.env,
      stdio: "inherit",
    },
  );
  if (result.error) {
    console.error(result.error.message || "Failed to build Codex Rotate tray.");
    return false;
  }
  if ((result.status ?? 1) !== 0) {
    return false;
  }
  return existsSync(TRAY_BIN);
}
