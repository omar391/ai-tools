import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import {
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

describe("npm wrapper", () => {
  test("forwards create --force to the native CLI binary", () => {
    const fixtureRoot = mkdtempSync(join(tmpdir(), "codex-rotate-wrapper-"));
    const cliStubPath = join(fixtureRoot, "codex-rotate-v2");
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
});
