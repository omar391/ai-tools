#!/usr/bin/env bun

import { spawn } from "node:child_process";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import process from "node:process";
import { fileURLToPath } from "node:url";

type Track = "non_device" | "device_auth";

type BenchmarkCandidate = {
  id: string;
  track: Track;
  filePath: string;
  workflowRef: string;
  baseEmail: string;
};

type RotatePoolAccount = {
  email?: string | null;
};

type RotateState = {
  accounts?: RotatePoolAccount[];
  pending?: Record<string, { email?: string | null }>;
  families?: Record<
    string,
    {
      profile_name?: string | null;
      base_email?: string | null;
      next_suffix?: number | null;
      last_created_email?: string | null;
    }
  >;
  active_index?: number | null;
};

type Snapshot = {
  authEmail: string | null;
  accountEmails: Set<string>;
  pendingEmails: Set<string>;
};

type BenchmarkRecord = {
  competitor: string;
  workflow_id: string;
  workflow_ref: string;
  workflow_file: string;
  track: Track;
  task: string;
  run_label: string;
  cold: boolean;
  success: boolean;
  latency_ms: number;
  exit_status: number | null;
  failure_mode: string | null;
  created_emails: string[];
  new_pending_emails: string[];
  auth_before: string | null;
  auth_after: string | null;
  auth_restored: boolean;
  base_email: string;
  notes: string | null;
  stdout_tail: string | null;
  stderr_tail: string | null;
  measured_at: string;
};

type GroupSummary = {
  workflowId: string;
  workflowRef: string;
  workflowFile: string;
  track: Track;
  runs: number;
  successes: number;
  successRate: number;
  medianLatencyMs: number | null;
  bestLatencyMs: number | null;
  failureModes: Record<string, number>;
  latestRunLabel: string | null;
};

const MODULE_DIR = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(MODULE_DIR, "..", "..");
const ROTATE_HOME = resolve(
  process.env.CODEX_ROTATE_HOME || join(homedir(), ".codex-rotate"),
);
const CODEX_HOME = resolve(process.env.CODEX_HOME || join(homedir(), ".codex"));
const WRAPPER = join(REPO_ROOT, "packages", "codex-rotate", "index.ts");
const WORKFLOW_ROOT = join(
  REPO_ROOT,
  ".fast-browser",
  "workflows",
  "web",
  "auth.openai.com",
);
const RESULTS_ROOT = join(
  REPO_ROOT,
  "packages",
  "codex-rotate",
  "benchmarks",
  "results",
  "account-flows",
);
const RAW_DIR = join(RESULTS_ROOT, "raw");
const ROTATE_STATE_PATH = join(ROTATE_HOME, "accounts.json");
const SUMMARY_SCRIPT =
  "/Volumes/Projects/business/AstronLab/omar391/ai-rules/skills/competitive-benchmark-loop/scripts/summarize_benchmarks.py";

const DEFAULT_PROFILE = "dev-1";
const DEFAULT_RUNS = 1;

const BENCHMARK_CANDIDATES: BenchmarkCandidate[] = [
  {
    id: "original",
    track: "non_device",
    filePath: join(WORKFLOW_ROOT, "codex-rotate-account-flow.yaml"),
    workflowRef: "workspace.web.auth-openai-com.codex-rotate-account-flow",
    baseEmail: "bench.original.{n}@astronlab.com",
  },
  {
    id: "stepwise",
    track: "non_device",
    filePath: join(WORKFLOW_ROOT, "codex-rotate-account-flow-stepwise.yaml"),
    workflowRef:
      "workspace.web.auth-openai-com.codex-rotate-account-flow-stepwise",
    baseEmail: "bench.stepwise.{n}@astronlab.com",
  },
  {
    id: "minimal",
    track: "non_device",
    filePath: join(WORKFLOW_ROOT, "codex-rotate-account-flow-minimal.yaml"),
    workflowRef:
      "workspace.web.auth-openai-com.codex-rotate-account-flow-minimal",
    baseEmail: "bench.minimal.{n}@astronlab.com",
  },
  {
    id: "device-auth",
    track: "device_auth",
    filePath: join(WORKFLOW_ROOT, "codex-rotate-account-flow-device-auth.yaml"),
    workflowRef:
      "workspace.web.auth-openai-com.codex-rotate-account-flow-device-auth",
    baseEmail: "bench.device.{n}@astronlab.com",
  },
];

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2));
  const benchmarkRunId = new Date().toISOString().replace(/[:.]/g, "-");
  mkdirSync(RAW_DIR, { recursive: true });

  const rawJsonlPath = join(RAW_DIR, `${benchmarkRunId}.jsonl`);
  const rawJsonPath = join(RAW_DIR, `${benchmarkRunId}.json`);
  const compatSummaryPath = join(
    RESULTS_ROOT,
    `${benchmarkRunId}.compat-summary.json`,
  );
  const summaryPath = join(RESULTS_ROOT, `${benchmarkRunId}.summary.json`);
  const selectionPath = join(RESULTS_ROOT, `${benchmarkRunId}.selection.json`);
  const reportPath = join(RESULTS_ROOT, `${benchmarkRunId}.report.md`);
  const latestRawJsonlPath = join(RAW_DIR, "latest.jsonl");
  const latestRawJsonPath = join(RAW_DIR, "latest.json");
  const latestCompatSummaryPath = join(
    RESULTS_ROOT,
    "latest.compat-summary.json",
  );
  const latestSummaryPath = join(RESULTS_ROOT, "latest.summary.json");
  const latestSelectionPath = join(RESULTS_ROOT, "latest.selection.json");
  const latestReportPath = join(RESULTS_ROOT, "latest.report.md");

  const selectedCandidates = BENCHMARK_CANDIDATES.filter((candidate) => {
    if (options.mode === "non_device") {
      return candidate.track === "non_device";
    }
    if (options.mode === "device_auth") {
      return candidate.track === "device_auth";
    }
    return true;
  });

  if (selectedCandidates.length === 0) {
    throw new Error("No benchmark candidates were selected.");
  }

  const records: BenchmarkRecord[] = [];
  for (const candidate of selectedCandidates) {
    for (let iteration = 1; iteration <= options.runs; iteration += 1) {
      const record = await benchmarkCandidate(candidate, {
        iteration,
        profileName: options.profileName,
        baseEmail:
          options.baseEmailOverride?.[candidate.id] ??
          options.baseEmailOverride?.[candidate.track] ??
          candidate.baseEmail,
      });
      records.push(record);
      process.stdout.write(
        `[benchmark] ${record.workflow_id} success=${record.success} latency_ms=${record.latency_ms} created=${record.created_emails.join(",") || "-"} failure=${record.failure_mode || "-"}\n`,
      );
    }
  }

  writeJson(rawJsonPath, records);
  writeJsonl(rawJsonlPath, records);

  const compatSummary = runSummaryScript(rawJsonlPath, compatSummaryPath);
  const summary = buildSummary(records);
  writeJson(summaryPath, summary);
  const selection = buildSelection(records, summary);
  writeJson(selectionPath, selection);

  const report = buildReport({
    benchmarkRunId,
    options,
    records,
    summary,
    selection,
  });
  writeFileSync(reportPath, report);

  copyFileSync(rawJsonPath, latestRawJsonPath);
  copyFileSync(rawJsonlPath, latestRawJsonlPath);
  if (compatSummary) {
    copyFileSync(compatSummaryPath, latestCompatSummaryPath);
  }
  copyFileSync(summaryPath, latestSummaryPath);
  copyFileSync(selectionPath, latestSelectionPath);
  copyFileSync(reportPath, latestReportPath);

  process.stdout.write(`Results:\n`);
  process.stdout.write(`- raw jsonl: ${rawJsonlPath}\n`);
  if (compatSummary) {
    process.stdout.write(`- compat summary: ${compatSummaryPath}\n`);
  }
  process.stdout.write(`- summary: ${summaryPath}\n`);
  process.stdout.write(`- selection: ${selectionPath}\n`);
  process.stdout.write(`- report: ${reportPath}\n`);
  process.stdout.write(
    `Selected non-device: ${selection.selected_non_device?.workflow_id || "none"}\n`,
  );
  process.stdout.write(
    `Selected device-auth: ${selection.selected_device_auth?.workflow_id || "none"}\n`,
  );
}

function parseArgs(args: string[]): {
  mode: "all" | "non_device" | "device_auth";
  runs: number;
  profileName: string;
  baseEmailOverride: Record<string, string>;
} {
  let mode: "all" | "non_device" | "device_auth" = "all";
  let runs = DEFAULT_RUNS;
  let profileName = DEFAULT_PROFILE;
  const baseEmailOverride: Record<string, string> = {};

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--non-device") {
      mode = "non_device";
      continue;
    }
    if (arg === "--device-auth") {
      mode = "device_auth";
      continue;
    }
    if (arg === "--all") {
      mode = "all";
      continue;
    }
    if (arg === "--profile") {
      const value = args[index + 1];
      if (!value) throw new Error("--profile requires a value");
      profileName = value;
      index += 1;
      continue;
    }
    if (arg.startsWith("--profile=")) {
      profileName = arg.slice("--profile=".length);
      continue;
    }
    if (arg === "--runs") {
      const value = args[index + 1];
      if (!value) throw new Error("--runs requires a value");
      runs = Number.parseInt(value, 10);
      index += 1;
      continue;
    }
    if (arg.startsWith("--runs=")) {
      runs = Number.parseInt(arg.slice("--runs=".length), 10);
      continue;
    }
    if (arg === "--base-email") {
      const value = args[index + 1];
      if (!value) throw new Error("--base-email requires workflow=value");
      assignBaseEmailOverride(baseEmailOverride, value);
      index += 1;
      continue;
    }
    if (arg.startsWith("--base-email=")) {
      assignBaseEmailOverride(
        baseEmailOverride,
        arg.slice("--base-email=".length),
      );
      continue;
    }
    throw new Error(`Unknown benchmark option: ${arg}`);
  }

  if (!Number.isInteger(runs) || runs < 1) {
    throw new Error("--runs must be a positive integer");
  }

  return {
    mode,
    runs,
    profileName,
    baseEmailOverride,
  };
}

function assignBaseEmailOverride(
  overrides: Record<string, string>,
  raw: string,
): void {
  const separatorIndex = raw.indexOf("=");
  if (separatorIndex === -1) {
    overrides.non_device = raw;
    overrides.device_auth = raw;
    return;
  }
  const key = raw.slice(0, separatorIndex).trim();
  const value = raw.slice(separatorIndex + 1).trim();
  if (!key || !value) {
    throw new Error(`Invalid --base-email override: ${raw}`);
  }
  overrides[key] = value;
}

async function benchmarkCandidate(
  candidate: BenchmarkCandidate,
  options: {
    iteration: number;
    profileName: string;
    baseEmail: string;
  },
): Promise<BenchmarkRecord> {
  const before = readSnapshot();
  const rotateStateSnapshot = snapshotRotateStateFile();
  const runLabel = `${candidate.id}-${new Date().toISOString()}`;
  const command = [
    "bun",
    WRAPPER,
    "create",
    "--force",
    "--restore-auth",
    "--profile",
    options.profileName,
    "--base-email",
    options.baseEmail,
  ];
  const env = {
    ...process.env,
    CODEX_ROTATE_ACCOUNT_FLOW_FILE: candidate.filePath,
  };

  process.stdout.write(
    `[benchmark] starting ${candidate.id} iteration=${options.iteration} flow=${candidate.filePath} base_email=${options.baseEmail}\n`,
  );
  const startedAt = new Date().toISOString();
  const startedMs = performance.now();
  let result: { exitStatus: number | null; stdout: string; stderr: string };
  let after: Snapshot;
  try {
    result = await runCommandWithCapture(command, env, candidate.id);
    after = readSnapshot();
  } finally {
    restoreRotateStateFile(rotateStateSnapshot);
  }
  const latencyMs = Math.round(performance.now() - startedMs);
  const createdEmails = [...after.accountEmails].filter(
    (email) => !before.accountEmails.has(email),
  );
  const newPendingEmails = [...after.pendingEmails].filter(
    (email) => !before.pendingEmails.has(email),
  );
  const success = result.exitStatus === 0;
  const notes = buildNotes(
    success,
    createdEmails,
    newPendingEmails,
    before,
    after,
  );
  const combinedOutput = [result.stdout, result.stderr]
    .filter(Boolean)
    .join("\n");

  return {
    competitor: candidate.id,
    workflow_id: candidate.id,
    workflow_ref: candidate.workflowRef,
    workflow_file: candidate.filePath,
    track: candidate.track,
    task:
      candidate.track === "device_auth"
        ? "openai-account-create-device-auth"
        : "openai-account-create-non-device",
    run_label: runLabel,
    cold: true,
    success,
    latency_ms: latencyMs,
    exit_status: result.exitStatus,
    failure_mode: success ? null : classifyFailureMode(combinedOutput),
    created_emails: createdEmails,
    new_pending_emails: newPendingEmails,
    auth_before: before.authEmail,
    auth_after: after.authEmail,
    auth_restored: before.authEmail === after.authEmail,
    base_email: options.baseEmail,
    notes,
    stdout_tail: tailText(result.stdout),
    stderr_tail: tailText(result.stderr),
    measured_at: startedAt,
  };
}

function buildNotes(
  success: boolean,
  createdEmails: string[],
  newPendingEmails: string[],
  before: Snapshot,
  after: Snapshot,
): string | null {
  const parts: string[] = [];
  if (success && createdEmails.length === 0) {
    parts.push(
      "create exited successfully but no new pooled account was detected",
    );
  }
  if (!success && newPendingEmails.length > 0) {
    parts.push(`left pending reservation(s): ${newPendingEmails.join(", ")}`);
  }
  if (before.authEmail !== after.authEmail) {
    parts.push(
      `live auth changed from ${before.authEmail || "none"} to ${after.authEmail || "none"}`,
    );
  }
  return parts.length > 0 ? parts.join("; ") : null;
}

function classifyFailureMode(output: string): string {
  const normalized = output.toLowerCase();
  if (normalized.includes("state mismatch")) return "state_mismatch";
  if (normalized.includes("did not reach the callback"))
    return "callback_missing";
  if (normalized.includes("verification is not ready"))
    return "verification_pending";
  if (normalized.includes("device authorization is rate limited"))
    return "device_auth_rate_limit";
  if (normalized.includes("too many requests")) return "rate_limit";
  if (normalized.includes("anti-bot") || normalized.includes("security gate"))
    return "anti_bot_gate";
  if (
    normalized.includes("add your phone number") ||
    normalized.includes("add_phone")
  )
    return "add_phone";
  if (normalized.includes("invalid credentials")) return "invalid_credentials";
  if (normalized.includes("not exit cleanly")) return "codex_login_exit";
  return "unknown";
}

async function runCommandWithCapture(
  command: string[],
  env: NodeJS.ProcessEnv,
  label: string,
): Promise<{ exitStatus: number | null; stdout: string; stderr: string }> {
  return await new Promise((resolve, reject) => {
    const child = spawn(command[0], command.slice(1), {
      cwd: REPO_ROOT,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";

    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      stdout += chunk;
      process.stdout.write(`[${label}] ${chunk}`);
    });

    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk: string) => {
      stderr += chunk;
      process.stderr.write(`[${label}] ${chunk}`);
    });

    child.once("error", reject);
    child.once("close", (code) => {
      resolve({
        exitStatus: typeof code === "number" ? code : null,
        stdout,
        stderr,
      });
    });
  });
}

function readSnapshot(): Snapshot {
  const authEmail = readAuthEmail();
  const state = readRotateState();
  const accountEmails = new Set<string>();
  const pendingEmails = new Set<string>();

  for (const entry of state.accounts || []) {
    const email = normalizeEmail(entry?.email);
    if (email) {
      accountEmails.add(email);
    }
  }
  for (const pending of Object.values(state.pending || {})) {
    const email = normalizeEmail(pending?.email);
    if (email) {
      pendingEmails.add(email);
    }
  }

  return {
    authEmail,
    accountEmails,
    pendingEmails,
  };
}

function readRotateState(): RotateState {
  if (!existsSync(ROTATE_STATE_PATH)) {
    return {};
  }
  try {
    return JSON.parse(readFileSync(ROTATE_STATE_PATH, "utf8")) as RotateState;
  } catch {
    return {};
  }
}

function snapshotRotateStateFile():
  | { exists: false }
  | { exists: true; content: string } {
  if (!existsSync(ROTATE_STATE_PATH)) {
    return { exists: false };
  }
  return {
    exists: true,
    content: readFileSync(ROTATE_STATE_PATH, "utf8"),
  };
}

function restoreRotateStateFile(
  snapshot: { exists: false } | { exists: true; content: string },
): void {
  if (snapshot.exists) {
    writeFileSync(ROTATE_STATE_PATH, snapshot.content);
    return;
  }
  if (existsSync(ROTATE_STATE_PATH)) {
    rmSync(ROTATE_STATE_PATH);
  }
}

function readAuthEmail(): string | null {
  const authPath = join(CODEX_HOME, "auth.json");
  if (!existsSync(authPath)) {
    return null;
  }
  try {
    const raw = JSON.parse(readFileSync(authPath, "utf8")) as Record<
      string,
      unknown
    >;
    const idToken = (raw?.tokens as { id_token?: string } | undefined)
      ?.id_token;
    if (typeof idToken === "string") {
      const payload = parseJwtPayload(idToken);
      const email = normalizeEmail(payload?.email);
      if (email) return email;
    }
    const accessToken = (raw?.tokens as { access_token?: string } | undefined)
      ?.access_token;
    if (typeof accessToken === "string") {
      const payload = parseJwtPayload(accessToken);
      const email = normalizeEmail(
        payload?.["https://api.openai.com/profile"]?.email,
      );
      if (email) return email;
    }
  } catch {}
  return null;
}

function parseJwtPayload(token: string): Record<string, any> | null {
  const segments = token.split(".");
  if (segments.length < 2) return null;
  try {
    const normalized = segments[1].replace(/-/g, "+").replace(/_/g, "/");
    const padded =
      normalized + "=".repeat((4 - (normalized.length % 4 || 4)) % 4);
    return JSON.parse(Buffer.from(padded, "base64").toString("utf8"));
  } catch {
    return null;
  }
}

function normalizeEmail(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim().toLowerCase();
  return trimmed || null;
}

function tailText(value: string, maxLines = 12): string | null {
  const lines = String(value || "")
    .split(/\r?\n/)
    .map((line) => line.trimEnd())
    .filter(Boolean);
  if (lines.length === 0) return null;
  return lines.slice(-maxLines).join("\n");
}

function writeJson(filePath: string, value: unknown): void {
  mkdirSync(dirname(filePath), { recursive: true });
  writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function writeJsonl(filePath: string, records: unknown[]): void {
  mkdirSync(dirname(filePath), { recursive: true });
  writeFileSync(
    filePath,
    records.map((record) => JSON.stringify(record)).join("\n") + "\n",
  );
}

function runSummaryScript(
  inputPath: string,
  outputPath: string,
): { groups: Record<string, unknown> } | null {
  const summaryProcess = Bun.spawnSync(
    ["python3", SUMMARY_SCRIPT, inputPath, "--output", outputPath],
    {
      cwd: REPO_ROOT,
      env: process.env,
      stdout: "pipe",
      stderr: "pipe",
    },
  );
  if (summaryProcess.exitCode !== 0) {
    process.stderr.write(
      `[benchmark] summarize_benchmarks.py failed: ${summaryProcess.stderr.toString()}\n`,
    );
    return null;
  }
  try {
    return JSON.parse(readFileSync(outputPath, "utf8")) as {
      groups: Record<string, unknown>;
    };
  } catch {
    return null;
  }
}

function buildSummary(records: BenchmarkRecord[]): {
  groups: Record<string, GroupSummary>;
} {
  const grouped = new Map<string, BenchmarkRecord[]>();
  for (const record of records) {
    const key = `${record.workflow_id}::${record.task}::cold`;
    const bucket = grouped.get(key) || [];
    bucket.push(record);
    grouped.set(key, bucket);
  }
  const groups: Record<string, GroupSummary> = {};
  for (const [key, bucket] of grouped) {
    const latencies = bucket
      .map((record) => record.latency_ms)
      .sort((a, b) => a - b);
    const failureModes: Record<string, number> = {};
    for (const record of bucket) {
      if (!record.success) {
        const failureMode = record.failure_mode || "unknown";
        failureModes[failureMode] = (failureModes[failureMode] || 0) + 1;
      }
    }
    const first = bucket[0];
    groups[key] = {
      workflowId: first.workflow_id,
      workflowRef: first.workflow_ref,
      workflowFile: first.workflow_file,
      track: first.track,
      runs: bucket.length,
      successes: bucket.filter((record) => record.success).length,
      successRate:
        bucket.length > 0
          ? bucket.filter((record) => record.success).length / bucket.length
          : 0,
      medianLatencyMs:
        latencies.length > 0
          ? latencies[Math.floor((latencies.length - 1) / 2)]
          : null,
      bestLatencyMs: latencies[0] ?? null,
      failureModes,
      latestRunLabel: bucket[bucket.length - 1]?.run_label ?? null,
    };
  }
  return { groups };
}

function buildSelection(
  _records: BenchmarkRecord[],
  summary: { groups: Record<string, GroupSummary> },
): {
  selected_non_device: null | {
    workflow_id: string;
    workflow_ref: string;
    workflow_file: string;
    median_latency_ms: number | null;
    success_rate: number;
  };
  selected_device_auth: null | {
    workflow_id: string;
    workflow_ref: string;
    workflow_file: string;
    median_latency_ms: number | null;
    success_rate: number;
  };
} {
  const groups = Object.values(summary.groups || {});
  const choose = (track: Track) => {
    const candidates = groups
      .filter((group) => group.track === track)
      .sort((left, right) => {
        const successDelta =
          Number(right.successRate || 0) - Number(left.successRate || 0);
        if (successDelta !== 0) return successDelta;
        return (
          Number(left.medianLatencyMs ?? Number.MAX_SAFE_INTEGER) -
          Number(right.medianLatencyMs ?? Number.MAX_SAFE_INTEGER)
        );
      });
    const winner = candidates.find(
      (group) => Number(group.successRate || 0) > 0,
    );
    if (!winner) return null;
    return {
      workflow_id: winner.workflowId,
      workflow_ref: winner.workflowRef,
      workflow_file: winner.workflowFile,
      median_latency_ms: winner.medianLatencyMs ?? null,
      success_rate: Number(winner.successRate || 0),
    };
  };

  return {
    selected_non_device: choose("non_device"),
    selected_device_auth: choose("device_auth"),
  };
}

function buildReport(input: {
  benchmarkRunId: string;
  options: { mode: string; runs: number; profileName: string };
  records: BenchmarkRecord[];
  summary: { groups: Record<string, any> };
  selection: {
    selected_non_device: {
      workflow_id: string;
      workflow_ref: string;
      workflow_file: string;
      median_latency_ms: number | null;
      success_rate: number;
    } | null;
    selected_device_auth: {
      workflow_id: string;
      workflow_ref: string;
      workflow_file: string;
      median_latency_ms: number | null;
      success_rate: number;
    } | null;
  };
}): string {
  const rows = input.records
    .map(
      (record) =>
        `| ${record.workflow_id} | ${record.track} | ${record.success ? "yes" : "no"} | ${record.latency_ms} | ${record.created_emails.join(", ") || "-"} | ${record.failure_mode || "-"} |`,
    )
    .join("\n");

  const groupRows = Object.values(input.summary.groups || {})
    .map((group: any) => {
      const failureModes = Object.entries(group.failureModes || {})
        .map(([name, count]) => `${name}:${count}`)
        .join(", ");
      return `| ${group.workflowId} | ${group.track} | ${group.successes}/${group.runs} | ${group.successRate} | ${group.medianLatencyMs ?? "-"} | ${failureModes || "-"} |`;
    })
    .join("\n");

  return (
    `# OpenAI Account Flow Benchmark\n\n` +
    `- run: \`${input.benchmarkRunId}\`\n` +
    `- mode: \`${input.options.mode}\`\n` +
    `- runs per workflow: \`${input.options.runs}\`\n` +
    `- managed profile: \`${input.options.profileName}\`\n\n` +
    `## Raw Runs\n\n` +
    `| workflow | track | success | latency_ms | created emails | failure |\n` +
    `| --- | --- | --- | ---: | --- | --- |\n` +
    `${rows || "| - | - | - | - | - | - |"}\n\n` +
    `## Summary\n\n` +
    `| workflow | track | successes | success rate | median latency_ms | failure modes |\n` +
    `| --- | --- | --- | ---: | ---: | --- |\n` +
    `${groupRows || "| - | - | - | - | - | - |"}\n\n` +
    `## Selection\n\n` +
    `- selected non-device: ${input.selection.selected_non_device ? `\`${input.selection.selected_non_device.workflow_id}\` (${input.selection.selected_non_device.median_latency_ms ?? "-"} ms, success rate ${input.selection.selected_non_device.success_rate})` : "none"}\n` +
    `- selected device-auth: ${input.selection.selected_device_auth ? `\`${input.selection.selected_device_auth.workflow_id}\` (${input.selection.selected_device_auth.median_latency_ms ?? "-"} ms, success rate ${input.selection.selected_device_auth.success_rate})` : "none"}\n`
  );
}

await main();
