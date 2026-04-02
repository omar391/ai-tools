export interface CdpTargetInfo {
  id: string;
  type: string;
  title: string;
  url: string;
  webSocketDebuggerUrl: string;
}

export interface CdpConnection {
  close(): void;
  evaluate<T>(expression: string): Promise<T>;
}

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`CDP endpoint failed (${response.status}): ${url}`);
  }
  return await response.json() as T;
}

export async function listCdpTargets(port: number): Promise<CdpTargetInfo[]> {
  return await fetchJson<CdpTargetInfo[]>(`http://127.0.0.1:${port}/json/list`);
}

export async function isCdpReady(port: number): Promise<boolean> {
  try {
    await fetchJson<Record<string, unknown>>(`http://127.0.0.1:${port}/json/version`);
    return true;
  } catch {
    return false;
  }
}

export async function connectToLocalCodexPage(port: number): Promise<CdpConnection> {
  const targets = await listCdpTargets(port);
  const page = targets.find((target) => target.type === "page" && target.url.startsWith("app://-/index.html"));
  if (!page) {
    throw new Error(`No Codex page target is available on port ${port}.`);
  }

  let nextId = 0;
  const pending = new Map<number, { resolve: (value: unknown) => void; reject: (reason: Error) => void }>();
  const ws = new WebSocket(page.webSocketDebuggerUrl);

  await new Promise<void>((resolve, reject) => {
    ws.addEventListener("open", () => resolve(), { once: true });
    ws.addEventListener("error", () => reject(new Error(`Failed to connect to Codex renderer on port ${port}.`)), { once: true });
  });

  ws.addEventListener("message", (event) => {
    const message = JSON.parse(String(event.data)) as { id?: number; error?: unknown; result?: unknown };
    const id = typeof message.id === "number" ? message.id : null;
    if (id === null || !pending.has(id)) {
      return;
    }
    const handlers = pending.get(id)!;
    pending.delete(id);
    if (message.error) {
      handlers.reject(new Error(JSON.stringify(message.error)));
    } else {
      handlers.resolve(message.result);
    }
  });

  const send = async <T>(method: string, params: Record<string, unknown> = {}): Promise<T> => {
    return await new Promise<T>((resolve, reject) => {
      const id = ++nextId;
      pending.set(id, { resolve: (value) => resolve(value as T), reject });
      ws.send(JSON.stringify({ id, method, params }));
    });
  };

  await send("Runtime.enable");

  return {
    close(): void {
      ws.close();
    },
    async evaluate<T>(expression: string): Promise<T> {
      const result = await send<{ result: { value: T } }>("Runtime.evaluate", {
        expression,
        returnByValue: true,
        awaitPromise: true,
      });
      return result.result.value;
    },
  };
}
