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
  ensureBitwardenCliAccountSecretRef,
  findBitwardenCliAccountSecretRef,
  inspectManagedProfiles,
} from "./automation.ts";

type BridgeRequest =
  | {
      command: "inspect-managed-profiles";
      payload?: Record<string, never> | null;
    }
  | {
      command: "ensure-account-secret-ref";
      payload: { profileName: string; email: string; password: string };
    }
  | {
      command: "find-account-secret-ref";
      payload: { profileName: string; email: string };
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
        options?: {
          codexBin?: string;
          workflowFile?: string;
          workflowRunStamp?: string;
          preferSignupRecovery?: boolean;
          fullName?: string;
          birthMonth?: number;
          birthDay?: number;
          birthYear?: number;
          maxAttempts?: number;
          maxReplayPasses?: number;
          retryDelaysMs?: number[];
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

async function handleRequest(request: BridgeRequest): Promise<unknown> {
  switch (request.command) {
    case "inspect-managed-profiles":
      return inspectManagedProfiles();
    case "ensure-account-secret-ref":
      return await ensureBitwardenCliAccountSecretRef(
        request.payload.profileName,
        request.payload.email,
        request.payload.password,
      );
    case "find-account-secret-ref":
      return await findBitwardenCliAccountSecretRef(
        request.payload.profileName,
        request.payload.email,
      );
    case "delete-account-secret-ref":
      return await deleteBitwardenCliAccountSecretRef(
        request.payload.profileName,
        request.payload.email,
      );
    case "complete-codex-login":
      return (await completeCodexLoginViaWorkflow(
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
      )) as CodexRotateAuthFlowSummary;
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
