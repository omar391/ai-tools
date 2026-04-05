#!/usr/bin/env node

import { spawn } from "node:child_process";
import process from "node:process";

const CODEX_BIN =
  String(process.env.CODEX_ROTATE_REAL_CODEX || "codex").trim() || "codex";
const CLIENT_INFO = {
  name: "codex-rotate-managed-login",
  version: "1.0.0",
};
const helperArgs = process.argv.slice(2);

async function runDeviceAuthLogin() {
  const child = spawn(CODEX_BIN, ["login", "--device-auth"], {
    stdio: "inherit",
    env: process.env,
  });

  const exit = await new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      resolve({ code, signal });
    });
  });

  if (exit && typeof exit === "object") {
    const { code, signal } = exit;
    if (typeof code === "number") {
      process.exit(code);
    }
    if (signal === "SIGINT") {
      process.exit(130);
    }
    if (signal === "SIGTERM") {
      process.exit(143);
    }
  }

  process.exit(1);
}

function writeLine(stream, line = "") {
  stream.write(`${line}\n`);
}

function parseCallbackOrigin(authUrl) {
  try {
    const parsed = new URL(authUrl);
    const redirectUri = parsed.searchParams.get("redirect_uri");
    if (!redirectUri) {
      return null;
    }
    return new URL(redirectUri).origin;
  } catch {
    return null;
  }
}

class JsonRpcStdioClient {
  constructor(command, args) {
    this.command = command;
    this.args = args;
    this.nextId = 1;
    this.pending = new Map();
    this.notifications = new Map();
    this.buffer = "";
    this.closed = false;
    this.child = spawn(command, args, {
      stdio: ["pipe", "pipe", "inherit"],
      env: process.env,
    });
    this.ready = new Promise((resolve, reject) => {
      this.child.once("spawn", resolve);
      this.child.once("error", reject);
    });
    this.exit = new Promise((resolve) => {
      this.child.once("exit", (code, signal) => {
        this.closed = true;
        for (const { reject } of this.pending.values()) {
          reject(
            new Error(
              `codex app-server exited unexpectedly (${signal || code || 0}).`,
            ),
          );
        }
        this.pending.clear();
        resolve({ code, signal });
      });
    });
    this.child.stdout.setEncoding("utf8");
    this.child.stdout.on("data", (chunk) => this.#onData(chunk));
  }

  onNotification(method, handler) {
    const listeners = this.notifications.get(method) || [];
    listeners.push(handler);
    this.notifications.set(method, listeners);
  }

  async initialize() {
    await this.ready;
    await this.request("initialize", {
      clientInfo: CLIENT_INFO,
      capabilities: {},
    });
    this.notify("initialized", {});
  }

  request(method, params) {
    const id = this.nextId++;
    const payload = { jsonrpc: "2.0", id, method, params };
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.#write(payload);
    });
  }

  notify(method, params) {
    this.#write({ jsonrpc: "2.0", method, params });
  }

  async cancelLogin(loginId) {
    if (!loginId || this.closed) {
      return;
    }
    try {
      await this.request("account/login/cancel", { loginId });
    } catch {
      // Ignore cancellation failures during teardown.
    }
  }

  async close() {
    if (this.closed) {
      return;
    }
    this.child.kill("SIGTERM");
    const result = await Promise.race([
      this.exit,
      new Promise((resolve) =>
        setTimeout(() => {
          this.child.kill("SIGKILL");
          resolve({ code: null, signal: "SIGKILL" });
        }, 1500),
      ),
    ]);
    return result;
  }

  #write(payload) {
    if (this.closed) {
      throw new Error("codex app-server is not available.");
    }
    this.child.stdin.write(`${JSON.stringify(payload)}\n`);
  }

  #onData(chunk) {
    this.buffer += String(chunk);
    while (true) {
      const newlineIndex = this.buffer.indexOf("\n");
      if (newlineIndex === -1) {
        break;
      }
      const line = this.buffer.slice(0, newlineIndex).trim();
      this.buffer = this.buffer.slice(newlineIndex + 1);
      if (!line) {
        continue;
      }
      let message;
      try {
        message = JSON.parse(line);
      } catch {
        continue;
      }
      if (Object.prototype.hasOwnProperty.call(message, "id")) {
        const pending = this.pending.get(message.id);
        if (!pending) {
          continue;
        }
        this.pending.delete(message.id);
        if (message.error) {
          pending.reject(
            new Error(
              message.error.message ||
                `codex app-server request ${message.id} failed.`,
            ),
          );
          continue;
        }
        pending.resolve(message.result);
        continue;
      }
      if (message.method) {
        const listeners = this.notifications.get(message.method) || [];
        for (const listener of listeners) {
          listener(message.params ?? null);
        }
      }
    }
  }
}

async function main() {
  if (helperArgs.includes("--device-auth")) {
    await runDeviceAuthLogin();
    return;
  }

  const client = new JsonRpcStdioClient(CODEX_BIN, [
    "app-server",
    "--listen",
    "stdio://",
  ]);

  let loginId = null;
  let completionHandled = false;
  let pendingCompletion;
  const completionPromise = new Promise((resolve, reject) => {
    pendingCompletion = { resolve, reject };
  });

  client.onNotification("account/login/completed", (params) => {
    if (completionHandled) {
      return;
    }
    if (loginId && params?.loginId && params.loginId !== loginId) {
      return;
    }
    completionHandled = true;
    if (params?.success) {
      pendingCompletion.resolve();
      return;
    }
    pendingCompletion.reject(
      new Error(String(params?.error || "Login was not completed.")),
    );
  });

  const terminate = async (signal) => {
    if (!completionHandled) {
      completionHandled = true;
      await client.cancelLogin(loginId);
    }
    await client.close();
    process.exit(signal === "SIGINT" ? 130 : 143);
  };

  process.once("SIGINT", () => {
    void terminate("SIGINT");
  });
  process.once("SIGTERM", () => {
    void terminate("SIGTERM");
  });

  try {
    await client.initialize();
    const response = await client.request("account/login/start", {
      type: "chatgpt",
    });
    if (
      !response ||
      response.type !== "chatgpt" ||
      !response.authUrl ||
      !response.loginId
    ) {
      throw new Error("Unexpected account/login/start response.");
    }
    loginId = response.loginId;
    const callbackOrigin =
      parseCallbackOrigin(response.authUrl) || "http://localhost:1455";
    writeLine(
      process.stderr,
      `Starting local login server on ${callbackOrigin}.`,
    );
    writeLine(
      process.stderr,
      "If your browser did not open, navigate to this URL to authenticate:",
    );
    writeLine(process.stderr);
    writeLine(process.stderr, response.authUrl);
    await completionPromise;
    writeLine(process.stderr, "Successfully logged in");
    await client.close();
  } catch (error) {
    if (!completionHandled) {
      await client.cancelLogin(loginId);
    }
    await client.close();
    writeLine(
      process.stderr,
      error instanceof Error ? error.message : String(error),
    );
    process.exit(1);
  }
}

void main();
