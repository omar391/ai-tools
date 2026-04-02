import { spawnSync } from "node:child_process";
import type { CodexLogSignal, CodexSignalKind } from "./types.ts";

interface SqliteLogRow {
  id?: number;
  ts?: number;
  target?: string;
  feedback_log_body?: string;
}

function querySqliteJson<T>(sqliteBin: string, logsDbPath: string, query: string, fallbackMessage: string): T {
  const result = spawnSync(sqliteBin, ["-json", logsDbPath, query], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
  if (result.status !== 0) {
    throw new Error(`Failed to query Codex logs: ${result.stderr.trim() || "sqlite3 exited non-zero"}`);
  }
  return parseJson<T>(result.stdout || "[]", fallbackMessage);
}

function parseJson<T>(raw: string, fallbackMessage: string): T {
  try {
    return JSON.parse(raw) as T;
  } catch {
    throw new Error(fallbackMessage);
  }
}

function classifySignal(target: string, body: string): CodexSignalKind | null {
  if (target === "codex_app_server::outgoing_message" && body.startsWith("app-server event: account/rateLimits/updated")) {
    return "rate_limits_updated";
  }
  if (
    target === "log"
    && body.startsWith("Received message {\"type\":\"error\"")
    && body.includes("\"type\":\"usage_limit_reached\"")
    && body.includes("\"status_code\":429")
  ) {
    return "usage_limit_reached";
  }
  return null;
}

export function readCodexSignals(
  logsDbPath: string,
  options?: {
    afterId?: number | null;
    limit?: number;
    sqliteBin?: string;
  },
): CodexLogSignal[] {
  const sqliteBin = options?.sqliteBin ?? "sqlite3";
  const afterId = Math.max(0, Number(options?.afterId ?? 0));
  const limit = Math.max(1, Math.min(500, Number(options?.limit ?? 50)));
  const query = `
select id, ts, target, feedback_log_body
from logs
where id > ${afterId}
  and (
    (target = 'codex_app_server::outgoing_message' and feedback_log_body like 'app-server event: account/rateLimits/updated%')
    or
    (
      target = 'log'
      and feedback_log_body like 'Received message {"type":"error"%'
      and feedback_log_body like '%"type":"usage_limit_reached"%'
      and feedback_log_body like '%"status_code":429%'
    )
  )
order by id asc
limit ${limit};`.trim();
  const rows = querySqliteJson<SqliteLogRow[]>(sqliteBin, logsDbPath, query, "Failed to parse sqlite log rows");
  return rows.flatMap((row) => {
    const id = typeof row.id === "number" ? row.id : null;
    const ts = typeof row.ts === "number" ? row.ts : null;
    const target = typeof row.target === "string" ? row.target : null;
    const body = typeof row.feedback_log_body === "string" ? row.feedback_log_body : null;
    if (id === null || ts === null || !target || !body) {
      return [];
    }
    const kind = classifySignal(target, body);
    if (!kind) {
      return [];
    }
    return [{ id, ts, kind, target, body }];
  });
}

export function readLatestCodexSignalId(
  logsDbPath: string,
  options?: {
    sqliteBin?: string;
  },
): number | null {
  const sqliteBin = options?.sqliteBin ?? "sqlite3";
  const query = `
select id
from logs
where
  (
    (target = 'codex_app_server::outgoing_message' and feedback_log_body like 'app-server event: account/rateLimits/updated%')
    or
    (
      target = 'log'
      and feedback_log_body like 'Received message {"type":"error"%'
      and feedback_log_body like '%"type":"usage_limit_reached"%'
      and feedback_log_body like '%"status_code":429%'
    )
  )
order by id desc
limit 1;`.trim();
  const rows = querySqliteJson<Array<{ id?: number }>>(sqliteBin, logsDbPath, query, "Failed to parse latest sqlite log row");
  return typeof rows[0]?.id === "number" ? rows[0].id : null;
}
