import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const MODULE_DIR = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(MODULE_DIR, "..", "..");

export interface CodexRotateAppPaths {
  repoRoot: string;
  codexHome: string;
  codexAuthFile: string;
  codexLogsDbFile: string;
  codexRotateEntrypoint: string;
  rotateAppHome: string;
  debugProfileDir: string;
  sessionFile: string;
  runtime: string;
}

export function resolveCodexRotateAppPaths(): CodexRotateAppPaths {
  const codexHome = process.env.CODEX_HOME ?? join(homedir(), ".codex");
  const rotateAppHome = join(homedir(), ".codex-rotate-app");
  return {
    repoRoot: REPO_ROOT,
    codexHome,
    codexAuthFile: join(codexHome, "auth.json"),
    codexLogsDbFile: join(codexHome, "logs_1.sqlite"),
    codexRotateEntrypoint: join(REPO_ROOT, "packages", "codex-rotate", "index.ts"),
    rotateAppHome,
    debugProfileDir: join(rotateAppHome, "profile"),
    sessionFile: join(rotateAppHome, "session.json"),
    runtime: process.env.CODEX_ROTATE_APP_RUNTIME
      ?? (process.versions.bun ? process.execPath : "bun"),
  };
}
