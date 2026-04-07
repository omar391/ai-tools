#!/usr/bin/env node

import { spawn, spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const MODULE_DIR = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(MODULE_DIR, "..", "..");
const IS_WINDOWS = process.platform === "win32";
const CLI_BINARY_NAME = IS_WINDOWS ? "codex-rotate.exe" : "codex-rotate";
const TRAY_BINARY_NAME = IS_WINDOWS
  ? "codex-rotate-tray.exe"
  : "codex-rotate-tray";
const HELP_FLAGS = new Set(["help", "--help", "-h"]);

function resolveBinaryCandidates(): string[] {
  return [
    process.env.CODEX_ROTATE_BIN,
    process.env.CODEX_ROTATE_CLI_BIN,
    join(MODULE_DIR, "bin", CLI_BINARY_NAME),
    join(MODULE_DIR, "dist", "bin", CLI_BINARY_NAME),
    join(REPO_ROOT, "target", "debug", CLI_BINARY_NAME),
    join(REPO_ROOT, "target", "release", CLI_BINARY_NAME),
  ].filter((value): value is string => Boolean(value));
}

function resolveTrayBinaryCandidates(): string[] {
  return [
    process.env.CODEX_ROTATE_TRAY_BIN,
    join(MODULE_DIR, "bin", TRAY_BINARY_NAME),
    join(MODULE_DIR, "dist", "bin", TRAY_BINARY_NAME),
    join(REPO_ROOT, "target", "debug", TRAY_BINARY_NAME),
    join(REPO_ROOT, "target", "release", TRAY_BINARY_NAME),
  ].filter((value): value is string => Boolean(value));
}

function resolveBinary(label: string, candidates: string[]): string {
  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }

  const checked = candidates.map((candidate) => `  - ${candidate}`).join("\n");
  throw new Error(
    [
      `Unable to find the ${label} binary.`,
      checked ? `Checked:\n${checked}` : "",
    ]
      .filter(Boolean)
      .join("\n"),
  );
}

function resolveCliBinary(): string {
  return resolveBinary("codex-rotate CLI", resolveBinaryCandidates());
}

function resolveTrayBinary(): string {
  return resolveBinary("codex-rotate tray", resolveTrayBinaryCandidates());
}

function printWrapperHelp(): void {
  process.stdout.write(`
Wrapper Commands
  tray [open]         Start the Codex Rotate tray app
  tray status         Show whether the Codex Rotate tray app is running
  tray quit           Stop the Codex Rotate tray app
  tray restart        Restart the Codex Rotate tray app
  tray path           Print the resolved Codex Rotate tray binary path
`);
}

function printHelp(): number {
  let status = 0;
  try {
    const cliBinary = resolveCliBinary();
    const result = spawnSync(cliBinary, ["help"], {
      stdio: "inherit",
      env: process.env,
    });
    if (result.error) {
      throw result.error;
    }
    if (typeof result.status === "number") {
      status = result.status;
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`${message}\n`);
    status = 1;
  }

  printWrapperHelp();
  return status;
}

function parseProcessId(raw: string): number | null {
  const value = Number.parseInt(raw.trim(), 10);
  return Number.isFinite(value) && value > 0 ? value : null;
}

function listRunningTrayProcessIds(trayBinary: string): number[] {
  if (IS_WINDOWS) {
    const result = spawnSync(
      "tasklist",
      ["/FO", "CSV", "/NH", "/FI", `IMAGENAME eq ${TRAY_BINARY_NAME}`],
      {
        env: process.env,
        encoding: "utf8",
      },
    );
    if (result.error) {
      throw result.error;
    }
    if ((result.status ?? 1) !== 0) {
      throw new Error(
        result.stderr || "Failed to query running tray processes.",
      );
    }
    return result.stdout
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line.length > 0 && !line.startsWith("INFO:"))
      .map((line) => {
        const columns = line
          .split('","')
          .map((value) => value.replace(/^"/, "").replace(/"$/, ""));
        return parseProcessId(columns[1] ?? "");
      })
      .filter((value): value is number => value !== null);
  }

  const result = spawnSync("ps", ["ax", "-o", "pid=,command="], {
    env: process.env,
    encoding: "utf8",
  });
  if (result.error) {
    throw result.error;
  }
  if ((result.status ?? 1) !== 0) {
    throw new Error(result.stderr || "Failed to query running tray processes.");
  }

  return result.stdout
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.includes(trayBinary))
    .map((line) => parseProcessId(line.split(/\s+/, 1)[0] ?? ""))
    .filter((value): value is number => value !== null);
}

function isTrayRunning(trayBinary: string): boolean {
  return listRunningTrayProcessIds(trayBinary).length > 0;
}

function waitForTrayState(trayBinary: string, running: boolean): boolean {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    if (isTrayRunning(trayBinary) === running) {
      return true;
    }
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
  }
  return isTrayRunning(trayBinary) === running;
}

function trayOpen(): number {
  const trayBinary = resolveTrayBinary();
  if (isTrayRunning(trayBinary)) {
    process.stdout.write("Codex Rotate tray is already running.\n");
    return 0;
  }

  let spawnError: Error | null = null;
  const child = spawn(trayBinary, [], {
    env: process.env,
    cwd: REPO_ROOT,
    detached: true,
    stdio: "ignore",
  });
  child.once("error", (error) => {
    spawnError = error;
  });
  child.unref();
  if (waitForTrayState(trayBinary, true)) {
    process.stdout.write(`Started Codex Rotate tray.\n`);
    return 0;
  }
  if (spawnError) {
    throw spawnError;
  }
  process.stderr.write(
    "Timed out waiting for the Codex Rotate tray to start.\n",
  );
  return 1;
}

function trayStatus(): number {
  const trayBinary = resolveTrayBinary();
  const running = isTrayRunning(trayBinary);
  process.stdout.write(
    running
      ? `Codex Rotate tray is running.\n`
      : `Codex Rotate tray is not running.\n`,
  );
  return running ? 0 : 1;
}

function trayQuit(): number {
  const trayBinary = resolveTrayBinary();
  const pids = listRunningTrayProcessIds(trayBinary);
  if (pids.length === 0) {
    process.stdout.write("Codex Rotate tray is not running.\n");
    return 0;
  }

  for (const pid of pids) {
    try {
      process.kill(pid, "SIGTERM");
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      process.stderr.write(`Failed to stop tray pid ${pid}: ${message}\n`);
      return 1;
    }
  }

  if (!waitForTrayState(trayBinary, false)) {
    process.stderr.write(
      "Timed out waiting for the Codex Rotate tray to stop.\n",
    );
    return 1;
  }

  process.stdout.write("Stopped Codex Rotate tray.\n");
  return 0;
}

function handleTrayCommand(args: string[]): number {
  const command = args[0] ?? "open";
  switch (command) {
    case "open":
      return trayOpen();
    case "status":
      return trayStatus();
    case "quit":
      return trayQuit();
    case "restart": {
      const stopped = trayQuit();
      if (stopped !== 0) {
        return stopped;
      }
      return trayOpen();
    }
    case "path":
      process.stdout.write(`${resolveTrayBinary()}\n`);
      return 0;
    case "help":
    case "--help":
    case "-h":
      process.stdout.write(
        "Usage: codex-rotate tray [open|status|quit|restart|path]\n",
      );
      return 0;
    default:
      process.stderr.write(
        `Unknown tray command: ${command}. Run "codex-rotate tray help".\n`,
      );
      return 1;
  }
}

function main(): never {
  const args = process.argv.slice(2);
  const command = args[0];
  if (command === "tray") {
    process.exit(handleTrayCommand(args.slice(1)));
  }
  if (!command || HELP_FLAGS.has(command)) {
    process.exit(printHelp());
  }

  const cliBinary = resolveCliBinary();
  const result = spawnSync(cliBinary, args, {
    stdio: "inherit",
    env: process.env,
  });

  if (result.error) {
    throw result.error;
  }
  if (result.signal) {
    process.stderr.write(
      `codex-rotate was interrupted by signal ${result.signal}.\n`,
    );
    process.exit(1);
  }
  process.exit(typeof result.status === "number" ? result.status : 1);
}

try {
  main();
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exit(1);
}
