import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  writeFileSync,
  chmodSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const REPO_ROOT = resolve(import.meta.dir, "..", "..");
const WRAPPER_PATH = join(REPO_ROOT, "packages", "codex-rotate", "index.ts");

async function waitForCondition(
  condition: () => boolean,
  timeoutMs = 5_000,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (condition()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  return condition();
}

describe("npm wrapper", () => {
  test("forwards create --force to the native CLI binary", () => {
    const fixtureRoot = mkdtempSync(join(tmpdir(), "codex-rotate-wrapper-"));
    const cliStubPath = join(fixtureRoot, "codex-rotate");
    const argsCapturePath = join(fixtureRoot, "args.txt");

    try {
      writeFileSync(
        cliStubPath,
        `#!/bin/sh
printf '%s\n' "$@" > "${argsCapturePath}"
printf 'wrapper-ok\n'
`,
      );
      chmodSync(cliStubPath, 0o755);

      const result = spawnSync("node", [WRAPPER_PATH, "create", "--force"], {
        cwd: REPO_ROOT,
        env: {
          ...process.env,
          CODEX_ROTATE_BIN: cliStubPath,
        },
        encoding: "utf8",
      });

      expect(result.status).toBe(0);
      expect(result.stdout).toContain("wrapper-ok");
      expect(readFileSync(argsCapturePath, "utf8")).toBe("create\n--force\n");
    } finally {
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });

  test("can launch and stop the tray via wrapper commands", async () => {
    if (process.platform === "win32") {
      return;
    }

    const fixtureRoot = mkdtempSync(join(tmpdir(), "codex-rotate-tray-"));
    const trayStubPath = join(fixtureRoot, "codex-rotate-tray");
    const startedPath = join(fixtureRoot, "started.txt");
    const env = {
      ...process.env,
      CODEX_ROTATE_TRAY_BIN: trayStubPath,
    };

    try {
      writeFileSync(
        trayStubPath,
        `#!/bin/sh
trap 'exit 0' TERM INT
printf 'started\n' > "${startedPath}"
while true; do
  sleep 1
done
`,
      );
      chmodSync(trayStubPath, 0o755);

      const openResult = spawnSync("node", [WRAPPER_PATH, "tray", "open"], {
        cwd: REPO_ROOT,
        env,
        encoding: "utf8",
      });
      expect(openResult.status).toBe(0);
      expect(openResult.stdout).toContain("Started Codex Rotate tray.");
      expect(await waitForCondition(() => existsSync(startedPath))).toBe(true);

      const statusResult = spawnSync("node", [WRAPPER_PATH, "tray", "status"], {
        cwd: REPO_ROOT,
        env,
        encoding: "utf8",
      });
      expect(statusResult.status).toBe(0);
      expect(statusResult.stdout).toContain("Codex Rotate tray is running.");

      const quitResult = spawnSync("node", [WRAPPER_PATH, "tray", "quit"], {
        cwd: REPO_ROOT,
        env,
        encoding: "utf8",
      });
      expect(quitResult.status).toBe(0);
      expect(quitResult.stdout).toContain("Stopped Codex Rotate tray.");

      const stopped = await waitForCondition(() => {
        const result = spawnSync("node", [WRAPPER_PATH, "tray", "status"], {
          cwd: REPO_ROOT,
          env,
          encoding: "utf8",
        });
        return result.status === 1;
      });
      expect(stopped).toBe(true);
    } finally {
      spawnSync("node", [WRAPPER_PATH, "tray", "quit"], {
        cwd: REPO_ROOT,
        env,
        encoding: "utf8",
      });
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });
});
