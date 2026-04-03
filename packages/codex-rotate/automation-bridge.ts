#!/usr/bin/env bun

import { readFileSync } from "node:fs";
import type {
  CodexRotateAuthFlowSummary,
  CodexRotateSecretRef,
} from "./automation.ts";
import {
  completeCodexLoginViaWorkflow,
  ensureBitwardenCliAccountSecretRef,
  findBitwardenCliAccountSecretRef,
  inspectManagedProfiles,
  readWorkflowFileMetadata,
} from "./automation.ts";

type BridgeRequest =
  | {
      command: "inspect-managed-profiles";
      payload?: Record<string, never> | null;
    }
  | { command: "read-workflow-metadata"; payload: { filePath: string } }
  | {
      command: "ensure-account-secret-ref";
      payload: { profileName: string; email: string; password: string };
    }
  | {
      command: "find-account-secret-ref";
      payload: { profileName: string; email: string };
    }
  | {
      command: "complete-codex-login";
      payload: {
        profileName: string;
        email: string;
        accountSecretRef: CodexRotateSecretRef;
        options?: {
          codexBin?: string;
          workflowRunStamp?: string;
          preferSignupRecovery?: boolean;
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

function respond(response: BridgeResponse): never {
  process.stdout.write(`${JSON.stringify(response)}\n`);
  process.exit(response.ok ? 0 : 1);
}

async function handleRequest(request: BridgeRequest): Promise<unknown> {
  switch (request.command) {
    case "inspect-managed-profiles":
      return inspectManagedProfiles();
    case "read-workflow-metadata":
      return readWorkflowFileMetadata(request.payload.filePath);
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
    case "complete-codex-login":
      return (await completeCodexLoginViaWorkflow(
        request.payload.profileName,
        request.payload.email,
        request.payload.accountSecretRef,
        {
          ...request.payload.options,
          onNote: (message) => {
            process.stderr.write(`[codex-rotate] ${message}\n`);
          },
          restoreState: null,
        },
      )) as CodexRotateAuthFlowSummary;
    default: {
      const exhaustive: never = request;
      throw new Error(
        `Unsupported automation bridge command: ${String(exhaustive)}`,
      );
    }
  }
}

async function main(): Promise<void> {
  try {
    const raw = readStdin().trim();
    if (!raw) {
      throw new Error("Automation bridge expected a JSON request on stdin.");
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
