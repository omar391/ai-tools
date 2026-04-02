import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { isCdpReady } from "./cdp.ts";
import { resolveCodexRotateAppPaths } from "./paths.ts";

export interface CodexLaunchSession {
  appPath: string;
  port: number;
  profileDir: string;
  launchedAt: string;
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function ensureDirectory(path: string): void {
  if (!existsSync(path)) {
    mkdirSync(path, { recursive: true });
  }
}

export function readLaunchSession(): CodexLaunchSession | null {
  const paths = resolveCodexRotateAppPaths();
  if (!existsSync(paths.sessionFile)) {
    return null;
  }
  return JSON.parse(readFileSync(paths.sessionFile, "utf8")) as CodexLaunchSession;
}

function writeLaunchSession(session: CodexLaunchSession): void {
  const paths = resolveCodexRotateAppPaths();
  ensureDirectory(paths.rotateAppHome);
  writeFileSync(paths.sessionFile, JSON.stringify(session, null, 2), "utf8");
}

export async function ensureDebugCodexInstance(options?: {
  appPath?: string;
  port?: number;
  profileDir?: string;
  waitMs?: number;
}): Promise<CodexLaunchSession> {
  const paths = resolveCodexRotateAppPaths();
  const session: CodexLaunchSession = {
    appPath: options?.appPath ?? "/Applications/Codex.app",
    port: options?.port ?? 9333,
    profileDir: options?.profileDir ?? paths.debugProfileDir,
    launchedAt: new Date().toISOString(),
  };

  if (await isCdpReady(session.port)) {
    writeLaunchSession(session);
    return session;
  }

  ensureDirectory(paths.rotateAppHome);
  ensureDirectory(session.profileDir);

  const openResult = spawnSync(
    "open",
    [
      "-na",
      session.appPath,
      "--args",
      `--user-data-dir=${session.profileDir}`,
      `--remote-debugging-port=${session.port}`,
    ],
    {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  if (openResult.status !== 0) {
    throw new Error(openResult.stderr.trim() || `Failed to launch Codex from ${session.appPath}.`);
  }

  const deadline = Date.now() + (options?.waitMs ?? 15_000);
  while (Date.now() < deadline) {
    if (await isCdpReady(session.port)) {
      writeLaunchSession(session);
      return session;
    }
    await sleep(500);
  }

  throw new Error(`Codex did not expose a remote debugging target on port ${session.port}.`);
}
