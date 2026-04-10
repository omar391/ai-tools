#!/usr/bin/env -S node --experimental-strip-types

import { readFileSync } from "node:fs";
import type {
  CodexRotateSecretLocator,
  CodexRotateAuthFlowSession,
  CodexRotateSecretRef,
} from "./automation.ts";
import {
  completeCodexLoginViaWorkflowAttempt,
  deleteBitwardenCliAccountSecretRef,
  prepareBitwardenCliAccountSecretRef,
  resetManagedProfileRuntime,
} from "./automation.ts";

type BridgeRequest =
  | {
      command: "prepare-account-secret-ref";
      payload: { profileName: string; email: string; password: string };
    }
  | {
      command: "delete-account-secret-ref";
      payload: { profileName: string; email: string };
    }
  | {
      command: "reset-managed-runtime";
      payload: { profileName: string; socketPath?: string | null };
    }
  | {
      command: "complete-codex-login-attempt";
      payload: {
        profileName: string;
        email: string;
        accountLoginLocator?: CodexRotateSecretLocator | null;
        options?: {
          codexBin?: string;
          workflowRef?: string;
          workflowRunStamp?: string;
          preferSignupRecovery?: boolean;
          fullName?: string;
          birthMonth?: number;
          birthDay?: number;
          birthYear?: number;
          skipLocatorPreflight?: boolean;
          codexSession?: CodexRotateAuthFlowSession | null;
        };
      };
    };

type BridgeResponse =
  | { ok: true; result: unknown }
  | { ok: false; error: { message: string } };

const BRIDGE_RESPONSE_PREFIX = "__CODEX_ROTATE_BRIDGE__";

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
      );
    case "delete-account-secret-ref":
      return await deleteBitwardenCliAccountSecretRef(
        request.payload.profileName,
        request.payload.email,
      );
    case "reset-managed-runtime":
      await resetManagedProfileRuntime(
        request.payload.profileName,
        request.payload.socketPath ?? null,
      );
      return { ok: true };
    case "complete-codex-login-attempt":
      return (await completeCodexLoginViaWorkflowAttempt(
        request.payload.profileName,
        request.payload.email,
        request.payload.accountLoginLocator ?? null,
        {
          ...request.payload.options,
        },
      )) as unknown;
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
  }
}

void main();
