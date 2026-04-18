#!/usr/bin/env -S node --experimental-strip-types

import { readFileSync } from "node:fs";
import process from "node:process";
import type {
  CodexRotateSecretLocator,
  CodexRotateAuthFlowSession,
  BrowserOsFamily,
} from "./automation.ts";
import {
  completeCodexLoginViaWorkflowAttempt,
  deleteBitwardenCliAccountSecretRef,
  generateDeterministicFingerprint,
  prepareBitwardenCliAccountSecretRef,
} from "./automation.ts";

type BridgeRequest =
  | {
      command: "prepare-account-secret-ref";
      payload: {
        profileName: string;
        profileDir?: string;
        email: string;
        password: string;
      };
    }
  | {
      command: "delete-account-secret-ref";
      payload: { profileName: string; profileDir?: string; email: string };
    }
  | {
      command: "complete-codex-login-attempt";
      payload: {
        profileName: string;
        profileDir?: string;
        email: string;
        accountLoginLocator?: CodexRotateSecretLocator | null;
        options?: {
          codexBin?: string;
          workflowRef?: string;
          workflowRunStamp?: string;
          preferSignupRecovery?: boolean;
          preferPasswordLogin?: boolean;
          password?: string;
          fullName?: string;
          birthMonth?: number;
          birthDay?: number;
          birth_year?: number;
          skipLocatorPreflight?: boolean;
          codexSession?: CodexRotateAuthFlowSession | null;
          personaProfile?: {
            id: string;
            osFamily: BrowserOsFamily;
            userAgent: string;
            acceptLanguage: string;
            timezone: string;
            screenWidth: number;
            screenHeight: number;
            deviceScaleFactor: number;
            browserFingerprint?: Record<string, unknown> | null;
          } | null;
        };
      };
    }
  | {
      command: "generate-browser-fingerprint";
      payload: {
        personaId: string;
        options: {
          userAgent?: string;
          screenWidth?: number;
          screenHeight?: number;
          osFamily: BrowserOsFamily;
        };
      };
    };

type BridgeResponse =
  | { ok: true; result: unknown }
  | { ok: false; error: { message: string } };

const BRIDGE_RESPONSE_PREFIX = "__CODEX_ROTATE_BRIDGE__";
const OWNER_WATCHDOG_INTERVAL_MS = 1_000;

function parseOwnerPid(value: string | undefined): number | null {
  if (!value) {
    return null;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 1) {
    return null;
  }
  return parsed;
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function startOwnerWatchdog(ownerPid: number | null): () => void {
  if (!ownerPid) {
    return () => {};
  }
  const timer = setInterval(() => {
    if (isProcessAlive(ownerPid)) {
      return;
    }
    process.stderr.write(
      `[codex-rotate] bridge owner ${ownerPid} exited; stopping orphaned automation bridge.\n`,
    );
    process.exit(1);
  }, OWNER_WATCHDOG_INTERVAL_MS);
  timer.unref?.();
  return () => clearInterval(timer);
}

function readStdin(): string {
  return readFileSync(process.stdin.fd, "utf8");
}

function readRequestRaw(): string {
  const requestFileFlagIndex = process.argv.indexOf("--request-file");
  if (requestFileFlagIndex !== -1) {
    const requestFilePath = process.argv[requestFileFlagIndex + 1];
    if (!requestFilePath) {
      throw new Error(
        "Automation bridge expected a path after --request-file.",
      );
    }
    return readFileSync(requestFilePath, "utf8");
  }
  return readStdin();
}

async function respond(response: BridgeResponse): Promise<never> {
  await new Promise<void>((resolve, reject) => {
    process.stdout.write(
      `${BRIDGE_RESPONSE_PREFIX}${JSON.stringify(response)}\n`,
      (error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve();
      },
    );
  });
  process.exit(response.ok ? 0 : 1);
}

async function handleRequest(request: BridgeRequest): Promise<unknown> {
  switch (request.command) {
    case "prepare-account-secret-ref":
      return await prepareBitwardenCliAccountSecretRef(
        request.payload.profileName,
        request.payload.email,
        request.payload.password,
        request.payload.profileDir,
      );
    case "delete-account-secret-ref":
      return await deleteBitwardenCliAccountSecretRef(
        request.payload.profileName,
        request.payload.email,
        request.payload.profileDir,
      );
    case "complete-codex-login-attempt":
      return (await completeCodexLoginViaWorkflowAttempt(
        request.payload.profileName,
        request.payload.email,
        request.payload.accountLoginLocator ?? null,
        {
          ...request.payload.options,
          profileDir: request.payload.profileDir,
        },
      )) as unknown;
    case "generate-browser-fingerprint":
      return generateDeterministicFingerprint(
        request.payload.personaId,
        request.payload.options,
      );

    default: {
      const label =
        typeof (request as { command?: unknown }).command === "string"
          ? (request as { command: string }).command
          : String(request);
      throw new Error(`Unsupported automation bridge command: ${label}`);
    }
  }
}

async function main(): Promise<void> {
  const stopOwnerWatchdog = startOwnerWatchdog(
    parseOwnerPid(process.env.CODEX_ROTATE_BRIDGE_OWNER_PID) ?? process.ppid,
  );
  try {
    const raw = readRequestRaw().trim();
    if (!raw) {
      throw new Error(
        "Automation bridge expected a JSON request on stdin or --request-file.",
      );
    }
    const request = JSON.parse(raw) as BridgeRequest;
    const result = await handleRequest(request);
    await respond({ ok: true, result });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await respond({
      ok: false,
      error: { message },
    });
  } finally {
    stopOwnerWatchdog();
  }
}

await main();
