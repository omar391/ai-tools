#!/usr/bin/env -S node --experimental-strip-types

import { readFileSync } from "node:fs";
import type {
  CodexRotateAuthFlowSummary,
  CodexRotateSecretLocator,
  CodexRotateSecretRef,
} from "./automation.ts";
import {
  completeCodexLoginViaWorkflow,
  deleteBitwardenCliAccountSecretRef,
  inspectManagedProfiles,
  prepareBitwardenCliAccountSecretRef,
} from "./automation.ts";

type BridgeRequest =
  | {
      command: "inspect-managed-profiles";
      payload?: Record<string, never> | null;
    }
  | {
      command: "prepare-account-secret-ref";
      payload: { profileName: string; email: string; password: string };
    }
  | {
      command: "delete-account-secret-ref";
      payload: { profileName: string; email: string };
    }
  | {
      command: "complete-codex-login";
      payload: {
        profileName: string;
        email: string;
        accountLoginLocator?: CodexRotateSecretLocator | null;
        accountLoginEnvVarName?: string | null;
        accountLoginEnvVarValue?: string | null;
        options?: {
          codexBin?: string;
          workflowFile?: string;
          workflowRef?: string;
          workflowRunStamp?: string;
          preferSignupRecovery?: boolean;
          fullName?: string;
          birthMonth?: number;
          birthDay?: number;
          birthYear?: number;
          maxAttempts?: number;
          maxReplayPasses?: number;
          retryDelaysMs?: number[];
          skipLocatorPreflight?: boolean;
        };
      };
    };

type BridgeResponse =
  | { ok: true; result: unknown }
  | { ok: false; error: { message: string } };

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

function respond(response: BridgeResponse): never {
  process.stdout.write(`${JSON.stringify(response)}\n`);
  process.exit(response.ok ? 0 : 1);
}

async function withTemporaryEnvVar<T>(
  name: string | null,
  value: string | null,
  operation: () => Promise<T>,
): Promise<T> {
  const normalizedName = typeof name === "string" ? name.trim() : "";
  if (!normalizedName) {
    return await operation();
  }
  const hadPrevious = Object.prototype.hasOwnProperty.call(
    process.env,
    normalizedName,
  );
  const previousValue = process.env[normalizedName];
  if (typeof value === "string") {
    process.env[normalizedName] = value;
  } else {
    delete process.env[normalizedName];
  }
  try {
    return await operation();
  } finally {
    if (hadPrevious && previousValue !== undefined) {
      process.env[normalizedName] = previousValue;
    } else {
      delete process.env[normalizedName];
    }
  }
}

async function handleRequest(request: BridgeRequest): Promise<unknown> {
  switch (request.command) {
    case "inspect-managed-profiles":
      return inspectManagedProfiles();
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
    case "complete-codex-login":
      return await withTemporaryEnvVar(
        request.payload.accountLoginEnvVarName ?? null,
        request.payload.accountLoginEnvVarValue ?? null,
        async () =>
          (await completeCodexLoginViaWorkflow(
            request.payload.profileName,
            request.payload.email,
            request.payload.accountLoginLocator ?? null,
            {
              ...request.payload.options,
              onNote: (message) => {
                process.stderr.write(`[codex-rotate] ${message}\n`);
              },
              restoreState: null,
            },
          )) as CodexRotateAuthFlowSummary,
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
  try {
    const raw = readRequestRaw().trim();
    if (!raw) {
      throw new Error(
        "Automation bridge expected a JSON request on stdin or --request-file.",
      );
    }
    const request = JSON.parse(raw) as BridgeRequest;
    const result = await handleRequest(request);
    respond({ ok: true, result });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    respond({
      ok: false,
      error: { message },
    });
  }
}

void main();
