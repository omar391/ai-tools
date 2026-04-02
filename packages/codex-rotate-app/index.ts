#!/usr/bin/env bun

import { buildCurrentLoginRequest, decideRotation, rotateNow } from "./controller.ts";
import { loadCodexAuth, summarizeCodexAuth } from "./auth.ts";
import { readLiveAccount, switchLiveAccountToCurrentAuth } from "./hook.ts";
import { ensureDebugCodexInstance } from "./launcher.ts";
import { resolveCodexRotateAppPaths } from "./paths.ts";
import { runWatchIteration, watchLive } from "./watch.ts";
import { sanitizeLoginStartRequest, sanitizeDeviceLoginPayload } from "./auth.ts";
import type { RotationResult } from "./types.ts";

function sanitizeRotationResult(result: RotationResult): RotationResult {
  return {
    ...result,
    loginPayload: sanitizeDeviceLoginPayload(result.loginPayload),
  };
}

function printUsage(): void {
  console.log(`codex-rotate-app

Usage:
  codex-rotate-app auth-summary
  codex-rotate-app build-login-request [--full]
  codex-rotate-app launch [--port <n>]
  codex-rotate-app account-read [--port <n>]
  codex-rotate-app switch-live [--port <n>]
  codex-rotate-app rotate-next-and-switch [--port <n>]
  codex-rotate-app watch-live [--port <n>] [--interval-ms <n>] [--cooldown-ms <n>] [--once]
  codex-rotate-app watch-once [--port <n>] [--after-id <n>]
  codex-rotate-app probe-signals [--after-id <n>]
  codex-rotate-app rotate-now
`);
}

function parseFlagNumber(args: string[], flag: string): number | null {
  for (let index = 0; index < args.length; index += 1) {
    if (args[index] === flag) {
      const value = args[index + 1];
      if (!value || Number.isNaN(Number(value))) {
        throw new Error(`Expected numeric value after ${flag}.`);
      }
      return Number(value);
    }
  }
  return null;
}

async function main(): Promise<void> {
  const [command, ...rest] = process.argv.slice(2);
  const paths = resolveCodexRotateAppPaths();

  switch (command) {
    case undefined:
    case "help":
    case "--help":
    case "-h":
      printUsage();
      return;
    case "auth-summary": {
      const summary = summarizeCodexAuth(loadCodexAuth(paths.codexAuthFile));
      console.log(JSON.stringify(summary, null, 2));
      return;
    }
    case "build-login-request": {
      const request = buildCurrentLoginRequest();
      console.log(JSON.stringify(rest.includes("--full") ? request : sanitizeLoginStartRequest(request), null, 2));
      return;
    }
    case "launch": {
      const session = await ensureDebugCodexInstance({ port: parseFlagNumber(rest, "--port") ?? 9333 });
      console.log(JSON.stringify(session, null, 2));
      return;
    }
    case "account-read": {
      const account = await readLiveAccount({ port: parseFlagNumber(rest, "--port") ?? 9333 });
      console.log(JSON.stringify(account, null, 2));
      return;
    }
    case "switch-live": {
      const result = await switchLiveAccountToCurrentAuth({ port: parseFlagNumber(rest, "--port") ?? 9333 });
      console.log(JSON.stringify(result, null, 2));
      return;
    }
    case "rotate-next-and-switch": {
      const rotation = rotateNow();
      const result = await switchLiveAccountToCurrentAuth({ port: parseFlagNumber(rest, "--port") ?? 9333 });
      console.log(JSON.stringify({ rotation: sanitizeRotationResult(rotation), live: result }, null, 2));
      return;
    }
    case "watch-live": {
      await watchLive({
        port: parseFlagNumber(rest, "--port") ?? 9333,
        intervalMs: parseFlagNumber(rest, "--interval-ms") ?? undefined,
        cooldownMs: parseFlagNumber(rest, "--cooldown-ms") ?? undefined,
        once: rest.includes("--once"),
      });
      return;
    }
    case "watch-once": {
      const result = await runWatchIteration({
        port: parseFlagNumber(rest, "--port") ?? 9333,
        afterSignalId: parseFlagNumber(rest, "--after-id"),
      });
      if (result.rotation) {
        result.rotation = sanitizeRotationResult(result.rotation);
      }
      console.log(JSON.stringify(result, null, 2));
      return;
    }
    case "probe-signals": {
      const decision = await decideRotation({ afterSignalId: parseFlagNumber(rest, "--after-id") });
      console.log(JSON.stringify(decision, null, 2));
      return;
    }
    case "rotate-now":
      console.log(JSON.stringify(sanitizeRotationResult(rotateNow()), null, 2));
      return;
    default:
      throw new Error(`Unknown command "${command}".`);
  }
}

await main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(message);
  process.exit(1);
});
