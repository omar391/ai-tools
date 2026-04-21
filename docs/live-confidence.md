# Live Confidence Policy

This document defines the suite split, verification rules, artifact retention, and operator runbooks for shared persona rotation confidence.

## Suite Matrix

| Suite | Command | Purpose | Gate |
| --- | --- | --- | --- |
| Hermetic host | `make hermetic-host` | Host system coverage on isolated state only | Required for PR confidence |
| Hermetic VM | `make hermetic-vm` | VM bootstrap and runtime coverage on isolated state only | Required for PR confidence |
| Live host | `make host-live` | Real Codex Desktop + Chrome + staging accounts | Opt-in, live-gated |
| Live VM | `make vm-live` | Real UTM + guest bridge + staging accounts | Blocked until live prerequisites exist |

The live gates are the single source of truth for preflight checks:

- `cargo run -p codex-rotate-cli -- internal live-check host`
- `cargo run -p codex-rotate-cli -- internal live-check vm`

## Verification Rules

Host mode can only be claimed done and verified when both hermetic host coverage and live host acceptance pass, cleanup checks pass, and the managed-browser path never escapes the isolated profile.

VM mode can only be claimed done and verified when hermetic VM coverage passes and the live VM suite is runnable on a machine with real UTM prerequisites. If the live prerequisites are missing, the claim stays blocked.

Live suites must always run against dedicated staging accounts and dedicated isolated persona roots. They must never reuse the operator's default Codex state.

## Artifact Retention

Failing hermetic runs retain the artifacts needed to diagnose cleanup and rollback regressions:

- logs
- app-server transcripts
- symlink maps
- process snapshots
- filesystem snapshots

Failing live runs retain the same hermetic artifacts plus the relevant live-only data:

- browser logs
- screenshots when a browser step is involved
- UTM and `utmctl` output
- guest-bridge transcripts
- runtime logs

Artifact capture is opt-in for passing runs and automatic for failing runs. Artifacts are written to a per-scenario directory so concurrent failures do not overwrite each other.

## No Silent Retry Policy

Cleanup and leak failures are hard failures. They are not silently retried because they indicate a regression in the isolation contract.

Infrastructure flake may be retried only when it is clearly separated from cleanup regressions and is tracked explicitly.

## Staging And Secret Handling

Live suites must use staging credentials only.

- Never run live suites against personal or production accounts.
- Keep secrets in environment variables or dedicated staging JSON, not in test output.
- Redact or avoid printing sensitive values in failure artifacts.

## Host Live Runbook

1. Confirm host prerequisites with `cargo run -p codex-rotate-cli -- internal live-check host`.
2. Export isolated host roots and staging-account data.
3. Run `make host-live`.
4. Verify the run stayed inside isolated persona roots before and after the test.
5. Inspect retained artifacts if the suite fails.

## VM Live Runbook

1. Confirm VM prerequisites with `cargo run -p codex-rotate-cli -- internal live-check vm`.
2. Export the UTM, `utmctl`, base package, bridge root, persona root, and staging-account inputs.
3. Confirm the persona root is APFS-backed and isolated from the operator's normal data.
4. Run `make vm-live`.
5. Verify VM teardown, bridge teardown, and artifact retention after the test.

## Backlog Mapping

### Track A

- A02-A08 are implemented in `packages/codex-rotate/crates/codex-rotate-test-support/` and `packages/codex-rotate/crates/codex-rotate-refresh/`.
- A09 is covered by `FilesystemTracker` and `FilesystemLeakGuard`.
- A10 is covered by `FailureArtifactCapture` and `FailureArtifactBundle`.
- A11 is covered by the app-server and managed-browser failure hooks plus the VM guest-bridge rollback paths.
- A12 is covered by `packages/codex-rotate/crates/codex-rotate-runtime/src/live_checks.rs`.
- A13 is covered by `packages/codex-rotate/crates/codex-rotate-runtime/src/live_checks.rs`.
- A14 is covered by `Makefile`, `package.json`, and `packages/codex-rotate/crates/codex-rotate-cli/src/main.rs`.

### Track B

- B01/B02 are covered by `host_sandbox_dry_run_next_preserves_live_snapshot`.
- B06/B07 are covered by the live-root migration and checkpoint recovery tests in `rotation_hygiene.rs`.
- B10/B11 are covered by the host activation staging and commit-after-import tests.
- B12 is covered by `switch_host_persona_repoints_live_roots_to_target_persona`.
- B15 is covered by `host_activation_retains_target_state_when_relaunch_fails` and `rollback_after_failed_host_activation_restores_state_and_symlinks`.
- B17 is covered by `finalize_rotation_after_import_rejects_partial_import_without_committing_pool`.
- B18 is covered by `relogin_host_switches_persona_and_restores`.
- B21 is covered by the host leak-guarded runtime and daemon tests.
- The remaining host hermetic gaps stay pending until there is a named test for them.

### Track C

- The live host acceptance items remain pending because they require real Codex Desktop and Chrome execution.
- The current codebase only supplies the preflight gate and the cleanup primitives needed to support those suites.

### Track D And E

- The VM bootstrap and backend tests cover bootstrap assets, seal metadata, guest-bridge command handling, relogin rollback, and persona-package provisioning.
- The remaining dedicated VM fixture tasks stay pending because the shared fixture layer is still incomplete.
- The VM live acceptance items remain blocked until real UTM prerequisites are available.

### Existing Tests

- `packages/codex-rotate/crates/codex-rotate-cli/tests/daemon_e2e.rs`
- `packages/codex-rotate/crates/codex-rotate-cli/tests/managed_login_e2e.rs`
- `packages/codex-rotate-app/src-tauri/tests/ipc_e2e.rs`
- `packages/codex-rotate/crates/codex-rotate-runtime/src/rotation_hygiene.rs`
- `packages/codex-rotate/crates/codex-rotate-runtime/src/vm_bootstrap.rs`