import { buildLoginStartRequest, loadCodexAuth } from "./auth.ts";
import { connectToLocalCodexPage } from "./cdp.ts";
import { ensureDebugCodexInstance } from "./launcher.ts";
import { resolveCodexRotateAppPaths } from "./paths.ts";
import type { CodexDesktopMcpRequest, DeviceLoginPayload } from "./types.ts";

export interface CodexDesktopHookAdapter {
  readonly name: string;
  dispatch(request: CodexDesktopMcpRequest<DeviceLoginPayload>): Promise<void>;
}

export class PreviewHookAdapter implements CodexDesktopHookAdapter {
  readonly name = "preview";

  async dispatch(request: CodexDesktopMcpRequest<DeviceLoginPayload>): Promise<void> {
    const preview = {
      ...request,
      request: {
        ...request.request,
        params: {
          ...request.request.params,
          accessToken: `${request.request.params.accessToken.slice(0, 24)}...`,
        },
      },
    };
    console.log(JSON.stringify(preview, null, 2));
  }
}

export interface AccountReadResult {
  account?: {
    type?: string;
    email?: string;
    planType?: string;
  };
  requiresOpenaiAuth?: boolean;
}

export class RemoteDebugHookAdapter implements CodexDesktopHookAdapter {
  readonly name = "remote-debug";

  constructor(private readonly port: number) {}

  async dispatch(request: CodexDesktopMcpRequest<DeviceLoginPayload>): Promise<void> {
    const connection = await connectToLocalCodexPage(this.port);
    try {
      const expression = `new Promise(async (resolve) => {
        const request = ${JSON.stringify(request)};
        await window.electronBridge.sendMessageFromView(request);
        resolve({ sent: true });
      })`;
      await connection.evaluate(expression);
    } finally {
      connection.close();
    }
  }
}

export function buildCurrentHookRequest(): CodexDesktopMcpRequest<DeviceLoginPayload> {
  const paths = resolveCodexRotateAppPaths();
  return buildLoginStartRequest(loadCodexAuth(paths.codexAuthFile));
}

export async function dispatchCurrentAuthThroughHook(adapter: CodexDesktopHookAdapter): Promise<void> {
  await adapter.dispatch(buildCurrentHookRequest());
}

async function sendMcpRequest<TResult>(port: number, method: string, params: Record<string, unknown>): Promise<TResult> {
  const connection = await connectToLocalCodexPage(port);
  try {
    const request = {
      type: "mcp-request",
      hostId: "local",
      request: {
        jsonrpc: "2.0",
        id: `codex-rotate-app-${method}-${Date.now()}-${Math.random()}`,
        method,
        params,
      },
    };
    const expression = `new Promise(async (resolve) => {
      const request = ${JSON.stringify(request)};
      const timeout = setTimeout(() => {
        window.removeEventListener("message", handler);
        resolve({ timeout: true });
      }, 8000);
      const handler = (event) => {
        const data = event.data;
        if (data && data.type === "mcp-response" && data.message && data.message.id === request.request.id) {
          clearTimeout(timeout);
          window.removeEventListener("message", handler);
          resolve({ timeout: false, result: data.message.result });
        }
      };
      window.addEventListener("message", handler);
      await window.electronBridge.sendMessageFromView(request);
    })`;
    const value = await connection.evaluate<{ timeout: boolean; result?: TResult }>(expression);
    if (value.timeout) {
      throw new Error(`Timed out waiting for ${method} response from Codex.`);
    }
    return value.result as TResult;
  } finally {
    connection.close();
  }
}

export async function readLiveAccount(options?: { port?: number }): Promise<AccountReadResult> {
  const port = options?.port ?? 9333;
  return await sendMcpRequest<AccountReadResult>(port, "account/read", {});
}

export async function switchLiveAccountToCurrentAuth(options?: {
  port?: number;
  ensureLaunched?: boolean;
  timeoutMs?: number;
}): Promise<{ email: string; planType: string; accountId: string }> {
  const paths = resolveCodexRotateAppPaths();
  const auth = loadCodexAuth(paths.codexAuthFile);
  const expected = buildCurrentHookRequest();
  const expectedEmail = auth.tokens.access_token
    ? JSON.parse(Buffer.from(auth.tokens.access_token.split(".")[1]!, "base64url").toString("utf8"))["https://api.openai.com/profile"]?.email
    : null;
  const port = options?.port ?? 9333;
  if (options?.ensureLaunched !== false) {
    await ensureDebugCodexInstance({ port });
  }
  const adapter = new RemoteDebugHookAdapter(port);
  await adapter.dispatch(expected);

  const deadline = Date.now() + (options?.timeoutMs ?? 15_000);
  while (Date.now() < deadline) {
    const current = await readLiveAccount({ port });
    if (expectedEmail && current.account?.email === expectedEmail) {
      return {
        email: current.account.email ?? "unknown",
        planType: current.account.planType ?? "unknown",
        accountId: expected.request.params.chatgptAccountId,
      };
    }
    await new Promise((resolve) => setTimeout(resolve, 750));
  }

  throw new Error(`Codex did not switch to ${expectedEmail ?? "the expected account"} in time.`);
}
