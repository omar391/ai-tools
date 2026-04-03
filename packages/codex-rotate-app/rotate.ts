import { spawnSync, type SpawnSyncReturns } from "node:child_process";
import type { AuthSummary, RotationResult } from "./types.ts";
import { buildDeviceLoginPayload, loadCodexAuth, summarizeCodexAuth } from "./auth.ts";

export interface RotateCommandOptions {
  authFilePath: string;
  rotateEntrypoint: string;
  runtime: string;
  repoRoot: string;
  command?: "next" | "create";
  args?: string[];
  run?: typeof spawnSync;
}

export function runRotateCommand(options: RotateCommandOptions): RotationResult {
  const run = options.run ?? spawnSync;
  const command = options.command ?? "next";
  const result = run(
    options.runtime,
    [options.rotateEntrypoint, command, ...(options.args ?? [])],
    {
      cwd: options.repoRoot,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    },
  ) as SpawnSyncReturns<string>;

  if (result.status !== 0) {
    const detail = (result.stderr || result.stdout || `codex-rotate ${command} failed`).trim();
    throw new Error(detail);
  }

  const auth = loadCodexAuth(options.authFilePath);
  return {
    summary: summarizeCodexAuth(auth),
    loginPayload: buildDeviceLoginPayload(auth),
  };
}

export function formatRotationSummary(summary: AuthSummary): string {
  return `${summary.email} (${summary.planType}, ${summary.accountId})`;
}
