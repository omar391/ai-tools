#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import path from "node:path";

const GROUPS = [
  {
    name: "runtime daemon",
    mode: "exact",
    base: ["packages/codex-rotate/crates/codex-rotate-runtime/src/daemon.rs"],
    current: [
      "packages/codex-rotate/crates/codex-rotate-runtime/src/daemon.rs",
    ],
  },
  {
    name: "runtime rotation_hygiene host split",
    filter: "runtimeHostParity",
    base: [
      "packages/codex-rotate/crates/codex-rotate-runtime/src/rotation_hygiene.rs",
    ],
    current: [
      "packages/codex-rotate/crates/codex-rotate-runtime/src/rotation_hygiene.rs",
      "packages/codex-rotate/crates/codex-rotate-runtime/src/rotation_hygiene",
    ],
  },
  {
    name: "runtime vm bootstrap relocation",
    filter: "productionOnly",
    base: [
      "packages/codex-rotate/crates/codex-rotate-runtime/src/vm_bootstrap.rs",
    ],
    current: ["packages/codex-rotate/crates/codex-rotate-vm/src/bootstrap.rs"],
  },
  {
    name: "core workflow",
    base: ["packages/codex-rotate/crates/codex-rotate-core/src/workflow.rs"],
    current: [
      "packages/codex-rotate/crates/codex-rotate-core/src/workflow.rs",
      "packages/codex-rotate/crates/codex-rotate-core/src/workflow",
    ],
  },
  {
    name: "core quota",
    mode: "exact",
    base: ["packages/codex-rotate/crates/codex-rotate-core/src/quota.rs"],
    current: ["packages/codex-rotate/crates/codex-rotate-core/src/quota.rs"],
  },
  {
    name: "core pool",
    base: ["packages/codex-rotate/crates/codex-rotate-core/src/pool.rs"],
    current: [
      "packages/codex-rotate/crates/codex-rotate-core/src/pool.rs",
      "packages/codex-rotate/crates/codex-rotate-core/src/pool",
    ],
  },
  {
    name: "cli main",
    base: ["packages/codex-rotate/crates/codex-rotate-cli/src/main.rs"],
    current: [
      "packages/codex-rotate/crates/codex-rotate-cli/src/main.rs",
      "packages/codex-rotate/crates/codex-rotate-cli/src/parsing.rs",
      "packages/codex-rotate/crates/codex-rotate-cli/src/tests.rs",
      "packages/codex-rotate/crates/codex-rotate-cli/src/commands",
    ],
  },
];

const FILTERS = {
  runtimeHostParity: {
    skipItemHeaders: [
      /^\s*fn select_rotation_backend\(/,
      /^\s*(?:pub\s+)?struct VmBackend\b/,
      /^\s*(?:pub\s+)?struct VmConversationTransport\b/,
      /^\s*(?:pub\s+)?struct GuestThreadHandoff(?:Export|Import)Result\b/,
      /^\s*(?:pub\s+)?struct IncomingGuestBridgeRequest\b/,
      /^\s*(?:pub\s+)?struct OutgoingGuestBridge(?:Error|Response)\b/,
      /^\s*impl RotationBackend for VmBackend\b/,
      /^\s*impl VmBackend\b/,
      /^\s*impl VmConversationTransport\b/,
      /^\s*impl ConversationTransport for VmConversationTransport\b/,
      /^\s*struct RecordingGuestBridge\b/,
      /^\s*impl RecordingGuestBridge\b/,
      /^\s*impl Drop for RecordingGuestBridge\b/,
      /^\s*(?:pub\s+)?fn (?:utmctl_binary|guest_bridge_request_url|guest_bridge_bind_addr|run_guest_bridge_server|handle_guest_bridge_stream|write_guest_bridge_response|handle_guest_bridge_command|send_guest_request|validate_vm_environment_config|require_absolute_existing_directory|require_absolute_directory|require_absolute_path|validate_vm_persona_id|ensure_apfs_filesystem|directory_size|ensure_clone_capacity|rollback_vm_relogin_auth_sync_failure|write_fake_utmctl|rotation_diagnostics_include_phase_and_context|rollback_after_failed_vm_activation_stops_target_vm|recording_utmctl_detects_simultaneous_active_regression|test_vm_backend(?:_invalid_relative_paths)?|vm_[A-Za-z0-9_]*|guest_bridge_[A-Za-z0-9_]*)\b/,
    ],
  },
  productionOnly: {
    stripCfgTestModules: true,
  },
};

function parseArgs(argv) {
  let base = null;
  let maxExamples = 12;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--base") {
      base = argv[++index] ?? null;
      continue;
    }
    if (arg === "--max-examples") {
      maxExamples = Number(argv[++index] ?? maxExamples);
      continue;
    }
    throw new Error(`Unknown argument: ${arg}`);
  }
  return { base, maxExamples };
}

function git(args) {
  return execFileSync("git", args, { encoding: "utf8" }).trim();
}

function resolveBase(explicitBase) {
  if (explicitBase) return explicitBase;
  for (const candidate of ["main", "origin/main"]) {
    try {
      const mergeBase = git(["merge-base", "HEAD", candidate]);
      if (mergeBase) return mergeBase;
    } catch {
      // Try the next common primary-branch spelling.
    }
  }
  throw new Error("Unable to resolve base. Pass --base <ref>.");
}

function readBaseFile(baseRef, file) {
  return execFileSync("git", ["show", `${baseRef}:${file}`], {
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
  });
}

function currentFiles(repoRoot, entries) {
  const files = [];
  for (const entry of entries) {
    const absolute = path.join(repoRoot, entry);
    if (!existsSync(absolute)) continue;
    if (absolute.endsWith(".rs")) {
      files.push(entry);
      continue;
    }
    for (const child of walkRustFiles(absolute)) {
      files.push(path.relative(repoRoot, child));
    }
  }
  return [...new Set(files)].sort();
}

function walkRustFiles(root) {
  const found = [];
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    const absolute = path.join(root, entry.name);
    if (entry.isDirectory()) {
      found.push(...walkRustFiles(absolute));
    } else if (entry.isFile() && entry.name.endsWith(".rs")) {
      found.push(absolute);
    }
  }
  return found;
}

function ignoredLine(trimmed) {
  if (!trimmed) return true;
  if (trimmed === "{" || trimmed === "}" || trimmed === "};") return true;
  if (trimmed.startsWith("//")) return true;
  if (trimmed.startsWith("#[")) return true;
  if (trimmed.startsWith("mod ") || trimmed.startsWith("pub mod ")) return true;
  return false;
}

function normalizeLine(line) {
  let normalized = line.trim();
  normalized = normalized.replace(/\bpub\(super\)\s+/g, "");
  normalized = normalized.replace(/\bpub\(crate\)\s+/g, "");
  normalized = normalized.replace(/\bpub\s+/g, "");
  normalized = normalized.replace(
    "- Host and VM execute the same sync semantics through the shared rotation engine; only transport wiring differs.",
    "- __lineage_sync_transport_boundary__",
  );
  normalized = normalized.replace(
    "- The default runtime executes host sync semantics; dormant VM transport/backend code lives in codex-rotate-vm.",
    "- __lineage_sync_transport_boundary__",
  );
  normalized = normalized.replace(
    '"same sync semantics through the shared rotation engine"',
    '"__lineage_sync_transport_boundary__"',
  );
  normalized = normalized.replace(
    '"codex-rotate-vm"',
    '"__lineage_sync_transport_boundary__"',
  );
  normalized = normalized.replace(
    'include_str!("../host_snapshot.rs")',
    'include_str!("host_snapshot.rs")',
  );
  normalized = normalized.replace(
    'include_str!("rotation_hygiene.rs")',
    'include_str!("__relocated_split_source__.rs")',
  );
  normalized = normalized.replace(
    'include_str!("host_snapshot.rs")',
    'include_str!("__relocated_split_source__.rs")',
  );
  return normalized;
}

function filterText(text, filterName) {
  const filter = FILTERS[filterName];
  if (!filter) return text;

  const lines = text.split(/\r?\n/);
  const kept = [];
  let skip = null;
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    const trimmed = line.trim();

    if (!skip && filter.stripCfgTestModules && trimmed === "#[cfg(test)]") {
      const nextIndex = nextNonEmptyLine(lines, index + 1);
      if (nextIndex !== -1 && /^mod tests\b/.test(lines[nextIndex].trim())) {
        skip = { depth: 0, seenBrace: false };
        continue;
      }
    }

    if (!skip && shouldSkipItem(trimmed, filter)) {
      skip = { depth: 0, seenBrace: false };
    }

    if (skip) {
      updateSkipState(skip, line);
      if (skip.seenBrace && skip.depth <= 0) skip = null;
      continue;
    }

    kept.push(line);
  }
  return kept.join("\n");
}

function nextNonEmptyLine(lines, startIndex) {
  for (let index = startIndex; index < lines.length; index += 1) {
    if (lines[index].trim()) return index;
  }
  return -1;
}

function shouldSkipItem(trimmed, filter) {
  return (filter.skipItemHeaders ?? []).some((pattern) =>
    pattern.test(trimmed),
  );
}

function updateSkipState(skip, line) {
  const delta = braceDelta(line);
  if (delta.open > 0 || delta.close > 0) skip.seenBrace = true;
  skip.depth += delta.open - delta.close;
}

function braceDelta(line) {
  const withoutStrings = line
    .replace(/r#+".*?"#+/g, '""')
    .replace(/"(?:\\.|[^"\\])*"/g, '""');
  return {
    open: (withoutStrings.match(/{/g) ?? []).length,
    close: (withoutStrings.match(/}/g) ?? []).length,
  };
}

function startsUseBlock(trimmed) {
  return (
    trimmed.startsWith("use ") ||
    trimmed.startsWith("pub use ") ||
    trimmed.startsWith("pub(crate) use ") ||
    trimmed.startsWith("pub(super) use ")
  );
}

function tokenize(line) {
  return (
    line.match(
      /[A-Za-z_][A-Za-z0-9_]*|r#".*?"#|"(?:\\.|[^"\\])*"|[0-9][0-9A-Za-z_]*|::|->|=>|==|!=|<=|>=|&&|\|\||[^\s]/g,
    ) ?? []
  );
}

function normalizedTokens(text, source) {
  const tokens = [];
  let skippingUseBlock = false;
  text.split(/\r?\n/).forEach((line, index) => {
    const normalized = normalizeLine(line);
    if (skippingUseBlock) {
      if (normalized.endsWith(";")) skippingUseBlock = false;
      return;
    }
    if (startsUseBlock(normalized)) {
      if (!normalized.endsWith(";")) skippingUseBlock = true;
      return;
    }
    if (ignoredLine(normalized)) return;
    for (const token of tokenize(normalized).filter((value) => value !== ",")) {
      tokens.push({ text: token, source, line: index + 1 });
    }
  });
  return tokens;
}

function multiset(items) {
  const counts = new Map();
  const examples = new Map();
  for (const item of items) {
    counts.set(item.text, (counts.get(item.text) ?? 0) + 1);
    if (!examples.has(item.text)) examples.set(item.text, item);
  }
  return { counts, examples };
}

function subtract(left, right) {
  const missing = [];
  for (const [line, count] of left.counts.entries()) {
    const delta = count - (right.counts.get(line) ?? 0);
    if (delta > 0) {
      missing.push({ line, count: delta, example: left.examples.get(line) });
    }
  }
  missing.sort((a, b) => b.count - a.count || a.line.localeCompare(b.line));
  return missing;
}

function groupLinesFromBase(baseRef, group) {
  return group.base.flatMap((file) =>
    normalizedTokens(
      filterText(readBaseFile(baseRef, file), group.filter),
      file,
    ),
  );
}

function groupLinesFromCurrent(repoRoot, group) {
  return currentFiles(repoRoot, group.current).flatMap((file) =>
    normalizedTokens(
      filterText(readFileSync(path.join(repoRoot, file), "utf8"), group.filter),
      file,
    ),
  );
}

function firstDifference(left, right) {
  const max = Math.min(left.length, right.length);
  let offset = 0;
  while (offset < max && left[offset] === right[offset]) offset += 1;
  const prefix = left.slice(0, offset);
  const line = prefix.split(/\r?\n/).length;
  const lastNewline = Math.max(
    prefix.lastIndexOf("\n"),
    prefix.lastIndexOf("\r"),
  );
  const column = offset - lastNewline;
  return { offset, line, column };
}

function formatExample(item) {
  const location = item.example
    ? `${item.example.source}:${item.example.line}`
    : "unknown";
  return `    x${item.count} ${location} :: ${item.line}`;
}

const { base: explicitBase, maxExamples } = parseArgs(process.argv.slice(2));
const repoRoot = git(["rev-parse", "--show-toplevel"]);
const baseRef = resolveBase(explicitBase);
let failed = false;

console.log(`split identity base: ${baseRef}`);
for (const group of GROUPS) {
  if (group.mode === "exact") {
    const baseFiles = group.base;
    const currentFileList = group.current;
    let ok = baseFiles.length === currentFileList.length;
    let exactBytes = 0;
    let mismatch = null;
    if (ok) {
      for (let index = 0; index < baseFiles.length; index += 1) {
        const baseFile = baseFiles[index];
        const currentFile = currentFileList[index];
        const baseText = readBaseFile(baseRef, baseFile);
        const currentText = readFileSync(
          path.join(repoRoot, currentFile),
          "utf8",
        );
        exactBytes += Buffer.byteLength(currentText);
        if (baseText !== currentText) {
          ok = false;
          mismatch = {
            baseFile,
            currentFile,
            baseBytes: Buffer.byteLength(baseText),
            currentBytes: Buffer.byteLength(currentText),
            diff: firstDifference(baseText, currentText),
          };
          break;
        }
      }
    }
    failed ||= !ok;
    console.log(
      `${ok ? "OK" : "FAIL"} ${group.name}: exact_files=${currentFileList.length} exact_bytes=${exactBytes}`,
    );
    if (!ok) {
      if (mismatch) {
        console.log(
          `  first exact mismatch: ${mismatch.baseFile} -> ${mismatch.currentFile} base_bytes=${mismatch.baseBytes} current_bytes=${mismatch.currentBytes} line=${mismatch.diff.line} column=${mismatch.diff.column}`,
        );
      } else {
        console.log(
          `  exact file count mismatch: base_files=${baseFiles.length} current_files=${currentFileList.length}`,
        );
      }
    }
    continue;
  }

  const baseLines = groupLinesFromBase(baseRef, group);
  const currentLines = groupLinesFromCurrent(repoRoot, group);
  const baseSet = multiset(baseLines);
  const currentSet = multiset(currentLines);
  const removed = subtract(baseSet, currentSet);
  const added = subtract(currentSet, baseSet);
  const ok = removed.length === 0 && added.length === 0;
  failed ||= !ok;

  console.log(
    `${ok ? "OK" : "FAIL"} ${group.name}: base_tokens=${baseLines.length} current_tokens=${currentLines.length} unique_base=${baseSet.counts.size} unique_current=${currentSet.counts.size}`,
  );
  if (!ok) {
    if (removed.length) {
      console.log(`  base-only normalized tokens (${removed.length} unique):`);
      for (const item of removed.slice(0, maxExamples))
        console.log(formatExample(item));
    }
    if (added.length) {
      console.log(`  current-only normalized tokens (${added.length} unique):`);
      for (const item of added.slice(0, maxExamples))
        console.log(formatExample(item));
    }
  }
}

if (failed) {
  console.error(
    "Split identity check failed: moved-code groups are not token-identical after relocation-only normalization.",
  );
  process.exit(1);
}
