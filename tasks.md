# Shared Persona Rotation Tasks

## Goal

Implement one shared rotation pipeline that defaults to host personas and can optionally use VM personas, while preserving per-account Codex/Desktop/browser state and minimal conversation continuity through Codex-native app-server APIs.

## Planning Baseline

- This backlog is derived from the current shared persona rotation plan.
- It is a forward-looking implementation task list, not a completion claim.
- Status values below should reflect the current `shared-persona-rotation` branch rather than a generic future plan.
- Public runtime command surface must remain unchanged: `next`, `prev`, `relogin`, and watch-triggered rotation only.
- Unit tasks are intentionally small and independently completable where possible.
- Corner cases should appear as explicit tasks rather than staying buried inside larger acceptance bullets.
- VM base preparation remains a support/bootstrap concern, not a product command.
- Host mode isolates app, CLI, and browser state per account, but does not attempt hardware fingerprint isolation.
- VM mode is the only path that should deliver hardware-level persona isolation.

## Status Legend

- `Pending`: Not started or not yet verified.
- `In Progress`: Implementation started but not yet complete.
- `Blocked`: Cannot proceed due to prerequisite or external dependency.
- `Done`: Implemented and verified against acceptance criteria.

## Constraints And Assumptions

- Default environment is `host`.
- Optional environment is `vm`.
- Cross-account continuity must use native Codex app-server APIs rather than UI scripting or SQLite cloning.
- VM persona storage assumes APFS-backed cheap cloning/snapshots for practical performance.
- Region-sensitive persona settings must remain coherent with expected egress when validation is enabled.
- Persona creation should use BrowserForge-style generation methods for realistic browser headers/fingerprints instead of ad hoc browser-profile fabrication.

## Current Branch Analysis

- The current worktree already implements a real shared host-side rotation path in [rotation_hygiene.rs](/Volumes/Projects/business/AstronLab/omar391/ai-tools/worktrees/shared-persona-rotation/packages/codex-rotate/crates/codex-rotate-runtime/src/rotation_hygiene.rs:1).
- Core staged rotation state, environment config, persona metadata, and commit helpers already exist in [pool.rs](/Volumes/Projects/business/AstronLab/omar391/ai-tools/worktrees/shared-persona-rotation/packages/codex-rotate/crates/codex-rotate-core/src/pool.rs:1).
- CLI, daemon manual rotation, and watch-triggered rotation already route through the shared runtime orchestrator.
- Host persona isolation already covers live `~/.codex`, Codex app-support state, `.fast-browser`, and the managed debug/browser profile through symlinked active roots.
- Native handoff already uses Codex app-server methods from runtime code: `thread/read`, `thread/start`, `thread/inject_items`, and `turn/start`.
- VM mode is still scaffold-only: config exists, backend selection exists, and the bootstrap asset exists, but `VmBackend` still returns guarded "not implemented" errors for activation/relogin paths.
- The bootstrap asset currently installs only a placeholder guest bridge LaunchAgent and does not provide autonomous UTM orchestration.
- Browser automation is already relevant to host personas in this repo because the existing login/browser flow uses managed Chrome profiles, Playwright, and `FAST_BROWSER_HOME`.

## Repo-Specific Implementation Notes

- Core schema/state work is centered in [pool.rs](/Volumes/Projects/business/AstronLab/omar391/ai-tools/worktrees/shared-persona-rotation/packages/codex-rotate/crates/codex-rotate-core/src/pool.rs:1), [state.rs](/Volumes/Projects/business/AstronLab/omar391/ai-tools/worktrees/shared-persona-rotation/packages/codex-rotate/crates/codex-rotate-core/src/state.rs:1), and [paths.rs](/Volumes/Projects/business/AstronLab/omar391/ai-tools/worktrees/shared-persona-rotation/packages/codex-rotate/crates/codex-rotate-core/src/paths.rs:1).
- Shared runtime orchestration lives in [rotation_hygiene.rs](/Volumes/Projects/business/AstronLab/omar391/ai-tools/worktrees/shared-persona-rotation/packages/codex-rotate/crates/codex-rotate-runtime/src/rotation_hygiene.rs:1), with command entry integration in [main.rs](/Volumes/Projects/business/AstronLab/omar391/ai-tools/worktrees/shared-persona-rotation/packages/codex-rotate/crates/codex-rotate-cli/src/main.rs:1), [daemon.rs](/Volumes/Projects/business/AstronLab/omar391/ai-tools/worktrees/shared-persona-rotation/packages/codex-rotate/crates/codex-rotate-runtime/src/daemon.rs:1), and [watch.rs](/Volumes/Projects/business/AstronLab/omar391/ai-tools/worktrees/shared-persona-rotation/packages/codex-rotate/crates/codex-rotate-runtime/src/watch.rs:1).
- Managed browser and Chrome/Playwright-related account automation is already part of the repo surface via [managed_browser.rs](/Volumes/Projects/business/AstronLab/omar391/ai-tools/worktrees/shared-persona-rotation/packages/codex-rotate/crates/codex-rotate-core/src/managed_browser.rs:1), [codex-login-managed-browser-opener.ts](/Volumes/Projects/business/AstronLab/omar391/ai-tools/worktrees/shared-persona-rotation/packages/codex-rotate/codex-login-managed-browser-opener.ts:1), and [automation.ts](/Volumes/Projects/business/AstronLab/omar391/ai-tools/worktrees/shared-persona-rotation/packages/codex-rotate/automation.ts:1).
- Because `.fast-browser` and the managed debug profile are already part of host persona switching, host persona work should explicitly cover Chrome-based Playwright/fast-browser automation isolation, not only relogin.

## Track A: State Schema And Shared Persona Metadata

### A01. Add top-level `environment` state field

Status: Done
Depends on: None
Acceptance criteria:

- `accounts.json` accepts `environment = "host" | "vm"`.
- Omitted `environment` defaults to `host`.
- Existing state files load without migration errors.

### A02. Add optional top-level `vm` config block

Status: Done
Depends on: A01
Acceptance criteria:

- `accounts.json` accepts an optional `vm` block.
- The block can persist `base_package_path`, `persona_root`, `utm_app_path`, `bridge_root`, and `expected_egress_mode`.
- Existing files without a `vm` block remain valid.

### A03. Extend `AccountEntry` with optional shared `persona` metadata

Status: Done
Depends on: None
Acceptance criteria:

- `AccountEntry` can persist an optional `persona` object.
- Missing persona metadata does not break older state files.
- Existing serialized account entries continue to deserialize successfully.

### A04. Add shared persona fields required by both backends

Status: Done
Depends on: A03
Acceptance criteria:

- Persona metadata supports `persona_id`, `persona_profile_id`, `expected_region_code`, and `ready_at`.
- Persona metadata supports backend-specific location fields without forcing both backends to use the same keys.
- Serialization uses stable field names suitable for long-lived state.

### A05. Normalize missing persona metadata with deterministic defaults

Status: Done
Depends on: A03, A04
Acceptance criteria:

- Missing persona metadata is filled deterministically during pool normalization.
- Re-running normalization does not generate new persona ids for unchanged accounts.
- Default persona ids are collision-resistant within the local pool.

### A06. Add deterministic default `persona_profile_id` assignment

Status: Done
Depends on: A05
Acceptance criteria:

- Every normalized account gets a coherent default `persona_profile_id`.
- The assignment is deterministic for a stable account identity.
- The assignment is independent of current active index.

### A07. Preserve optional region and readiness metadata without inventing values

Status: Done
Depends on: A04
Acceptance criteria:

- `expected_region_code` remains optional and round-trips correctly.
- `ready_at` remains optional and round-trips correctly.
- Missing optional values are not replaced with misleading placeholders.

### A08. Add schema round-trip and backward-compatibility tests

Status: Done
Depends on: A01, A02, A03, A04, A05, A06, A07
Acceptance criteria:

- Tests cover legacy state files with no environment and no persona metadata.
- Tests cover full environment/vm/persona round-trip.
- Tests prove default host selection when `environment` is omitted.

## Track B: Persona Profile Model And Coherence Rules

### B01. Define the `persona_profile_id` contract

Status: Done
Depends on: A06
Acceptance criteria:

- The codebase has one authoritative mapping from `persona_profile_id` to realism defaults.
- The mapping is usable by both host and VM backends.
- The contract is stable enough to persist in `accounts.json`.

### B02. Define non-hardware realism defaults for host mode

Status: Done
Depends on: B01
Acceptance criteria:

- Host-mode profile data defines locale, language, timezone, hostname style, and browser defaults.
- Host mode explicitly excludes hardware-only knobs such as RAM, vCPU, and display emulation.
- Host profile defaults are coherent with the profile id contract.

### B03. Define full realism defaults for VM mode

Status: Done
Depends on: B01
Acceptance criteria:

- VM-mode profile data defines locale, language, timezone, hostname style, browser defaults, RAM/vCPU pairing, and display size/scaling.
- Hardware settings are coherent rather than arbitrarily mixed.
- VM profile defaults are derived from the same `persona_profile_id` contract used in host mode.

### B04. Add region and egress validation rules

Status: Done
Depends on: A02, B02, B03
Acceptance criteria:

- `expected_egress_mode = "validate"` triggers validation logic.
- Region-sensitive defaults can be checked against `expected_region_code`.
- Validation failures are actionable and do not silently degrade to success.

### B05. Add tests for deterministic and coherent profile assignment

Status: Done
Depends on: B01, B02, B03, B04
Acceptance criteria:

- Tests prove deterministic assignment for stable account identities.
- Tests prove host profiles exclude hardware-only attributes.
- Tests prove VM profiles include hardware/display attributes.

### B06. Add BrowserForge as the browser-persona generation basis

Status: Done
Depends on: B01
Acceptance criteria:

- Persona creation is explicitly defined to use BrowserForge-backed generation methods rather than custom hand-built browser fingerprint defaults.
- The design identifies BrowserForge header generation and fingerprint generation as separate but coordinated outputs.
- The implementation boundary makes clear which persona attributes come from BrowserForge and which remain runtime-owned.

### B07. Map `persona_profile_id` to BrowserForge generation constraints

Status: Done
Depends on: B06
Acceptance criteria:

- Each `persona_profile_id` maps to an explicit BrowserForge constraint set.
- The mapping covers browser family/version policy, OS family, device class, locale, and HTTP version where relevant.
- Constraint sets are coherent and suitable for both host and VM persona provisioning.

### B08. Add BrowserForge-backed screen and browser realism synthesis

Status: Done
Depends on: B06, B07
Acceptance criteria:

- Persona synthesis uses BrowserForge-compatible screen constraints for display realism where applicable.
- Generated browser headers and fingerprint outputs are mutually coherent for the same persona.
- Host mode uses the non-hardware-realism subset while VM mode can consume the fuller persona output.
- Surgical plumbing in `automation.ts` ensures `fast-browser` receives persona overrides via `inputs`.

### B08a. Update fast-browser internals to consume persona inputs

Status: Done
Depends on: B08
Acceptance criteria:

- External `fast-browser` repository (local-chrome-cdp.mjs) is surgically updated.
- Playwright context launch correctly applies `browser_user_agent`, `browser_timezone`, and resolution inputs.
- Browser coherence is maintained between the OS and the reported User-Agent.

### B09. Define persisted vs generated BrowserForge-backed persona data

Status: Done
Depends on: B06, B07, B08
Acceptance criteria:

- The design specifies which browser-persona attributes are persisted in `accounts.json` and which are regenerated from stable inputs.
- Persisted state is minimal and stable.
- Re-generation from the same persona inputs remains deterministic enough for operational use.

### B10. Add fallback behavior when BrowserForge constraints are too strict or incompatible

Status: Done
Depends on: B06, B07, B08, B09
Acceptance criteria:

- The system defines how to handle BrowserForge strict-generation failures or incompatible constraint combinations.
- Fallbacks never silently generate a wildly different persona than requested.
- Errors identify whether the failure came from persona inputs, BrowserForge constraints, or unsupported combinations.

### B11. Add unit tests for BrowserForge-backed persona synthesis

Status: Done

Depends on: B06, B07, B08, B09, B10
Acceptance criteria:

- Tests cover deterministic mapping from `persona_profile_id` to BrowserForge constraints.
- Tests cover coherence between generated headers, user agent traits, locale, and fingerprint outputs.
- Tests cover strict/incompatible constraint failures and fallback behavior.

## Track C: Core Staged Rotation Prepare/Commit Flow

### C01. Introduce `PreparedRotation` and `PreparedRotationAction`

Status: Done
Depends on: None
Acceptance criteria:

- Core exposes a prepared-rotation struct that can be passed across runtime phases.
- Prepared rotations distinguish `switch`, `stay`, and `create_required`.
- Prepared rotations preserve previous and target account context.

### C02. Implement staged `prepare_next`

Status: Done
Depends on: C01
Acceptance criteria:

- `prepare_next` identifies the target account without immediately mutating live auth.
- It can still return `stay` and `create_required` when appropriate.
- It persists only the pool changes explicitly intended at prepare time.

### C03. Implement staged `prepare_prev`

Status: Done
Depends on: C01
Acceptance criteria:

- `prepare_prev` identifies the target account without immediately mutating live auth.
- It respects disabled domains and previous-account selection rules.
- It can be committed later by a separate step.

### C04. Add independent commit helper for prepared rotations

Status: Done
Depends on: C02, C03
Acceptance criteria:

- Pool-state commit is separate from auth-file mutation.
- Runtime can commit pool state after environment activation succeeds.
- The helper is idempotent for already-committed target selection.

### C05. Add explicit auth-write helper for selected accounts

Status: Done
Depends on: C04
Acceptance criteria:

- Runtime can write target auth without re-running selection logic.
- The helper creates missing parent directories safely.
- The helper is usable by host and VM activation flows.

### C06. Add relogin-side pool account resolution before activation

Status: Done
Depends on: C01
Acceptance criteria:

- Relogin can resolve a target account from the existing pool before backend switching.
- Missing selectors still fall back to the legacy relogin path.
- Runtime can distinguish pool-backed relogin from non-pool relogin.

### C07. Add rollback helpers for failed prepared flows

Status: Done
Depends on: C02, C03, C04, C05
Acceptance criteria:

- Core can restore active index when a prepared switch fails after prepare.
- Failed create-and-retry flows restore the previous active account cleanly.
- Rollback helpers do not leave auth and pool state pointing at different accounts.

### C08. Add unit tests for staged prepare/commit/rollback boundaries

Status: Done
Depends on: C01, C02, C03, C04, C05, C06, C07
Acceptance criteria:

- Tests prove prepare does not mutate live auth early.
- Tests prove commit mutates active state only after explicit invocation.
- Tests prove rollback restores the original selection on failure.

## Track D: Shared Runtime Orchestrator

### D01. Create `rotation_hygiene.rs` runtime module

Status: Done
Depends on: None
Acceptance criteria:

- Runtime has one orchestrator module dedicated to shared rotation hygiene.
- The module owns the shared transaction shape for host and VM modes.
- Host- and VM-specific activation logic remains isolated behind backend-specific functions/types.

### D02. Add runtime backend selection with default host behavior

Status: Done
Depends on: A01, A02, D01
Acceptance criteria:

- Runtime backend selection uses config or env override.
- Omitted selection defaults to host mode.
- Unsupported environment values return a clear error.

### D03. Implement shared `next` transaction flow

Status: Done
Depends on: C02, C04, C05, D01, D02
Acceptance criteria:

- `next` uses the shared prepare/export/activate/import/commit flow.
- `create_required` flows remain supported without adding public commands.
- The operator-facing `next` surface remains unchanged.

### D04. Implement shared `prev` transaction flow

Status: Done
Depends on: C03, C04, C05, D01, D02
Acceptance criteria:

- `prev` uses the same shared flow shape as `next`.
- The operator-facing `prev` surface remains unchanged.
- Backend selection is fully internal to runtime.

### D05. Implement shared `relogin` transaction flow

Status: Done
Depends on: C05, C06, D01, D02
Acceptance criteria:

- Relogin uses the shared backend activation path when the selector maps to a pooled account.
- The operator-facing `relogin` surface remains unchanged.
- Non-pooled relogin still works through the legacy-compatible path.

### D06. Route CLI commands through the shared orchestrator

Status: Done
Depends on: D03, D04, D05
Acceptance criteria:

- CLI `next`, `prev`, and `relogin` call the shared runtime orchestrator.
- CLI output remains compatible with existing user expectations.
- No new public CLI commands are introduced.

### D07. Route daemon manual rotation through the shared orchestrator

Status: Done
Depends on: D03, D04, D05
Acceptance criteria:

- Daemon-side manual `next` and `prev` use the shared runtime flow.
- Snapshot state continues to update correctly after manual rotation.
- User-visible daemon behavior remains stable.

### D08. Route watch-triggered rotation through the shared orchestrator

Status: Done
Depends on: D03
Acceptance criteria:

- Watch-triggered `next` uses the shared runtime flow.
- Existing watch-triggered create behavior is preserved.
- Backend selection stays internal and invisible to the caller.

## Track E: Shared Handoff Pipeline

### E01. Define the minimal shared handoff payload format

Status: Done
Depends on: None
Acceptance criteria:

- One shared handoff schema is used by both host and VM flows.
- The schema is minimal and excludes UI-specific or SQLite-specific data.
- The schema supports current active work plus recoverable pending work.

### E02. Collect active and recoverable thread ids from native sources

Status: Done
Depends on: E01
Acceptance criteria:

- Runtime can enumerate currently active threads.
- Runtime can include recoverable pending thread ids when available.
- The thread-id collection path does not require UI automation.

### E03. Export thread state from the source environment using `thread/read`

Status: Done
Depends on: E02
Acceptance criteria:

- Export logic reads thread content from the Codex app-server.
- Export supports current active work and recoverable pending work.
- Missing or deleted threads fail softly instead of aborting the whole export batch.

### E04. Normalize transferable items for cross-persona import

Status: Done
Depends on: E03
Acceptance criteria:

- Transfer logic keeps only supported user/assistant/plan/reasoning/command items.
- Exported text is truncated to safe size limits.
- Unsupported item types are ignored rather than causing hard failure.

### E05. Start target threads using native app-server APIs

Status: Done
Depends on: E04
Acceptance criteria:

- Runtime can create target threads natively before injecting prior context.
- Thread creation preserves working directory when available.
- No UI scripting is required to create target threads.

### E06. Import thread items using the native inject API

Status: Done
Depends on: E05
Acceptance criteria:

- Runtime can inject exported items into the target thread using the supported Codex app-server method.
- Import works with the same shared payload format used by both backends.
- Import failure is surfaced clearly for rollback handling.

### E07. Resume work using `turn/start`

Status: Done
Depends on: E06
Acceptance criteria:

- Runtime can append a continuation prompt after import.
- The continuation prompt avoids narrating the transfer itself.
- Resumed work starts in the target persona without extra operator steps.

### E08. Add handoff size, count, and truncation safeguards

Status: Done
Depends on: E04
Acceptance criteria:

- The runtime enforces per-thread item limits.
- The runtime enforces text truncation limits.
- Safeguards are explicit and test-covered.

### E09. Add rollback behavior for post-activation handoff failure

Status: Done
Depends on: E05, E06, E07, C07
Acceptance criteria:

- If target import fails after activation, runtime can restore the source environment.
- Partial target imports do not silently become the new active state.
- Error output identifies whether failure happened during export, activation, or import.

### E10. Add integration tests for active and recoverable handoff flows

Status: Done
Depends on: E01, E02, E03, E04, E05, E06, E07, E08, E09
Acceptance criteria:

- Tests cover active-thread export/import.
- Tests cover recoverable pending-thread export/import.
- Tests prove the same handoff format can be used by both host and VM backends.

## Track F: Host Backend

### F01. Define host persona directory layout under rotate home

Status: Done
Depends on: A03, A04, A05
Acceptance criteria:

- Each account maps to one persistent host persona root.
- The persona root contains isolated Codex home, Codex app support, fast-browser home, and managed browser/debug profile state.
- The layout is stable enough to survive repeated rotations.

### F02. Add runtime path support for live host roots

Status: Done
Depends on: F01
Acceptance criteria:

- Runtime can resolve the live host `~/.codex` root.
- Runtime can resolve the live host Codex app-support root.
- Runtime can resolve the live host fast-browser home and managed profile roots.

### F03. Implement first-run host migration for the active account

Status: Done
Depends on: F01, F02
Acceptance criteria:

- Existing live host state is moved into the current active account’s persona root on first migration.
- Migration is idempotent when run again.
- Migration preserves pre-existing user data instead of deleting it.

### F04. Create and repair live symlinks to the active persona

Status: Done
Depends on: F03
Acceptance criteria:

- Live host paths become symlinks to the active persona root.
- If symlinks already exist and are correct, the operation is a no-op.
- If symlinks are stale, runtime can repair them safely.

### F05. Provision missing target host persona roots on demand

Status: Done
Depends on: F01
Acceptance criteria:

- Rotating to a not-yet-provisioned host persona creates the required directory layout.
- Provisioning does not require an explicit pre-create command.
- Provisioning does not overwrite an existing persona root.

### F06. Seed first-time host persona content from an allowlisted source set

Status: Done
Depends on: F05
Acceptance criteria:

- Initial persona seeding copies only an explicit allowlist of configuration/state files.
- Seeding does not copy the entire source persona blindly.
- The allowlist is coherent with Codex Desktop, CLI, and browser needs.

### F06a. Materialize BrowserForge-backed browser persona defaults in host mode

Status: Done
Depends on: B06, B07, B08, F05, F06
Acceptance criteria:

- Host persona provisioning materializes BrowserForge-derived browser defaults needed by the local browser persona.
- Materialization avoids deprecated BrowserForge injection paths as the primary runtime contract.
- Generated browser persona data remains aligned with the account’s `persona_profile_id`.

### F07. Wait for handoff-safe state before host switch

Status: Done
Depends on: E02, F02
Acceptance criteria:

- Runtime can wait until active Codex work is handoff-safe.
- Managed Codex is stopped only after handoff-safe conditions are met.
- Timeout or failure produces a clear operator-visible error.

### F08. Repoint host symlinks to the target persona during activation

Status: Done
Depends on: F04, F05, F07
Acceptance criteria:

- Activation swaps live symlink targets to the selected target persona.
- Same-account activation is a no-op for already-correct symlinks.
- Failed repoint operations do not leave live roots partially switched.

### F09. Sync target auth after host activation

Status: Done
Depends on: C05, F08
Acceptance criteria:

- The selected target account’s auth becomes the live auth only after activation succeeds.
- Auth sync is explicit rather than incidental.
- Host rotation does not leave live auth pointing at the wrong persona.

### F10. Relaunch Codex after successful host activation

Status: Done
Depends on: F08, F09
Acceptance criteria:

- Managed Codex can be restarted after a successful host persona switch.
- Relaunch occurs only when it was running before the switch or when required by flow.
- Relaunch failures are propagated into rollback handling.

### F11. Preserve native history for same-account reopen

Status: Done
Depends on: F01, F04
Acceptance criteria:

- Returning to the same account uses its existing persona root and native history.
- Same-account reopen does not create unnecessary handoff threads.
- Native conversation history remains local to that account persona.

### F12. Use the target account’s browser persona during relogin

Status: Done
Depends on: F01, F05, D05
Acceptance criteria:

- Relogin runs against the target account’s managed browser/debug profile.
- Relogin does not reuse another account’s browser persona.
- After relogin, the source persona is restored when the target was only temporarily activated.

### F12a. Ensure host persona switching isolates Chrome-based Playwright and fast-browser automation state

Status: Done
Depends on: F01, F02, F04, F05, F12
Acceptance criteria:

- Host persona switching isolates `.fast-browser` state per account, not just Codex auth/app state.
- Managed Chrome/debug profile state used by Playwright-backed login automation stays account-local after rotation.
- Tasks and tests explicitly treat Chrome-based Playwright/fast-browser automation as part of the host persona contract.

### F13. Add host integration tests for migration, switching, and relogin

Status: Done
Depends on: F03, F04, F05, F06, F07, F08, F09, F10, F11, F12, F12a
Acceptance criteria:

- Tests cover first-run migration.
- Tests cover symlink target switching.
- Tests cover same-account reopen behavior.
- Tests cover relogin using the target browser persona.
- Tests cover account-local `.fast-browser` and managed Chrome/Playwright state isolation.

## Track G: VM Backend

### G00. Baseline-evaluate current VM scaffold against autonomous UTM goals

Status: Done
Depends on: A02, D02
Acceptance criteria:

- Produce an explicit verified assessment of the current VM backend state before deeper VM implementation continues.
- Confirm whether `VmBackend` entry points for `next`, `prev`, and `relogin` are guarded or functional.
- Confirm whether the current system can autonomously start UTM, boot/select per-persona VMs, run a real guest bridge, and complete full VM account rotation end to end.
- Record the outcome as a concrete gap statement rather than an implicit assumption.

### G00a. Verify guarded VM backend behavior in runtime

Status: Done
Depends on: G00
Acceptance criteria:

- Confirm from runtime code and tests that VM mode currently returns guarded "not implemented" errors rather than performing activation.
- Confirm the guard covers `rotate_next`, `rotate_prev`, and `relogin`.
- Confirm diagnostics mention the missing guest bridge/UTM activation flow.

### G00b. Verify current VM config surface only provides scaffolding

Status: Done
Depends on: G00
Acceptance criteria:

- Confirm that VM-related config fields exist for `base_package_path`, `persona_root`, `utm_app_path`, `bridge_root`, and `expected_egress_mode`.
- Confirm that having config support does not imply working VM activation.
- Document the gap between configuration shape and runtime implementation.

### G00c. Verify bootstrap script is provisioning-only, not autonomous VM rotation

Status: Done
Depends on: G00
Acceptance criteria:

- Confirm the bootstrap helper only prepares a base VM and does not provide a production guest bridge runtime.
- Confirm the current LaunchAgent or guest hook is placeholder behavior if that remains true.
- Confirm the bootstrap asset does not by itself provide autonomous UTM launch, guest-side Codex orchestration, or end-to-end account rotation.

### G00d. Verify missing autonomous UTM capabilities explicitly

Status: Done
Depends on: G00a, G00b, G00c
Acceptance criteria:

- Confirm whether the current code can start UTM automatically.
- Confirm whether the current code can boot/select per-account VM personas automatically.
- Confirm whether the current code can run guest-side bridge calls for activation/handoff/relogin.
- Confirm whether the current code can perform full end-to-end VM account rotation.
- Each capability is marked explicitly as implemented, partial, scaffold-only, or missing.

### G00e. Publish a concrete VM readiness gap summary

Status: Done
Depends on: G00d
Acceptance criteria:

- The backlog or linked design notes contain a concise summary of what is ready today versus what remains to be built for autonomous UTM rotation.
- The summary distinguishes scaffold/config/bootstrap readiness from true autonomous VM orchestration readiness.
- The summary is specific enough to be used as a go/no-go checkpoint before claiming VM support.

### G01. Define the runtime VM backend interface

Status: Done
Depends on: D01, A02
Acceptance criteria:

- Runtime has an explicit VM backend type behind the shared orchestrator.
- The interface covers activate, deactivate, relogin, and shared handoff calls.
- VM backend responsibilities are isolated from host backend logic.

### G02. Validate VM config before activation

Status: Done
Depends on: A02, G01
Acceptance criteria:

- VM backend validates required paths before use.
- Missing `base_package_path`, `persona_root`, or `utm_app_path` produce actionable errors.
- Validation happens before partial activation work starts.

### G03. Define persistent VM persona package layout

Status: Done
Depends on: A03, A04, G02
Acceptance criteria:

- Each account maps to one persistent VM persona package cloned from a sealed base.
- VM persona package naming is deterministic and collision-resistant.
- VM persona storage remains separate from host persona roots.

### G04. Provision missing VM persona packages from the sealed base

Status: Done
Depends on: G03
Acceptance criteria:

- First-time activation of a VM persona clones from the sealed base package.
- Provisioning is idempotent and does not overwrite an existing persona package.
- Provisioning supports APFS-friendly cloning semantics where available.

### G05. Enforce single-running-VM semantics

Status: Done
Depends on: G01
Acceptance criteria:

- VM backend ensures that only one persona VM is treated as active at a time.
- Switching VM personas stops or detaches the previous active VM cleanly.
- Operator-visible diagnostics explain VM contention or stale-running state.

### G06. Add UTM launch integration

Status: Done
Depends on: G02, G03, G04, G05
Acceptance criteria:

- VM backend can launch the target persona package via configured UTM tooling.
- Launch waits for guest readiness instead of assuming immediate availability.
- Launch failure is surfaced with enough detail for diagnosis.

### G07. Add UTM shutdown integration

Status: Done
Depends on: G05, G06
Acceptance criteria:

- VM backend can stop the currently active VM cleanly.
- Shutdown includes timeout handling and a fallback error path.
- Shutdown failure feeds into rollback/diagnostic logic.

### G08. Define host-to-guest bridge transport contract

Status: Done
Depends on: G01
Acceptance criteria:

- The host has one explicit transport contract for guest bridge communication.
- The contract is thin and limited to required control/data paths.
- Transport errors are separable from Codex app-server errors.

### G09. Implement guest bridge support for launching and stopping Codex

Status: Done
Depends on: G08
Acceptance criteria:

- The guest bridge can start Codex Desktop or managed Codex services in the guest.
- The guest bridge can stop guest Codex safely before persona switching.
- The bridge does not take on unrelated orchestration responsibilities.

### G10. Implement guest bridge support for native app-server calls

Status: Done
Depends on: G08
Acceptance criteria:

- The guest bridge can proxy required Codex app-server methods from host to guest.
- The shared handoff pipeline can use the same logical calls in VM mode as in host mode.
- The bridge remains thin and does not reimplement thread semantics.

### G11. Implement guest bridge support for managed relogin

Status: Done
Depends on: G08
Acceptance criteria:

- VM relogin runs locally inside the guest using the existing managed relogin flow.
- Guest relogin can reuse the isolated browser persona inside the VM.
- Guest relogin results are returned to the host for final state sync.

### G11a. Materialize BrowserForge-backed browser persona defaults in VM mode

Status: Done
Depends on: B06, B07, B08, G04, G11
Acceptance criteria:

- VM persona provisioning materializes BrowserForge-derived browser persona defaults inside the guest persona.
- VM browser persona materialization is coherent with the same `persona_profile_id` used by host mode.
- Guest-side relogin and browser automation consume the generated persona defaults without requiring deprecated BrowserForge injector usage as the primary model.

### G12. Activate the target VM persona through the shared orchestrator

Status: Done
Depends on: D03, D04, D05, G06, G07, G10
Acceptance criteria:

- VM mode uses the same prepare/export/activate/import/commit flow as host mode.
- The only mode-specific difference is environment activation/backend behavior.
- Operator-facing commands remain unchanged in VM mode.

### G13. Sync guest auth results back to host state

Status: Done
Depends on: C05, G11, G12
Acceptance criteria:

- Successful guest relogin updates host-side pool/auth state correctly.
- Host auth does not drift from the guest-selected account.
- Sync logic is explicit and test-covered.

### G14. Add VM rollback on activation or import failure

Status: Done
Depends on: C07, E09, G06, G07, G10, G12
Acceptance criteria:

- Failed VM activation can restore the source environment when possible.
- Failed handoff import after VM activation does not commit the broken target state.
- Rollback errors are reported separately from the original failure.

### G15. Add VM integration tests for boot, handoff, relogin, and rollback

Status: Done
Depends on: G06, G07, G08, G09, G10, G11, G12, G13, G14
Acceptance criteria:

- Tests cover target persona boot/shutdown through the backend abstraction.
- Tests cover guest bridge import of the shared handoff format.
- Tests cover relogin syncing auth back to host state.
- Tests cover rollback after target failure.

## Track H: Bootstrap Script And Base VM Preparation

### H01. Add bootstrap script asset outside the public product command surface

Status: Done
Depends on: None
Acceptance criteria:

- The repo contains one support bootstrap script for sealing/finalizing a base macOS VM.
- The script is clearly positioned as an ops/provisioning helper rather than a user-facing product command.
- The script can be versioned with the rest of the repo.

### H02. Install or verify Codex Desktop in the base VM

Status: Done
Depends on: H01
Acceptance criteria:

- Bootstrap flow verifies Codex Desktop is present or installs it.
- The script reports a clear failure when Codex Desktop is missing.
- The output tells the operator what was installed or verified.

### H03. Install or verify Codex CLI in the base VM

Status: Done
Depends on: H01
Acceptance criteria:

- Bootstrap flow verifies Codex CLI is present or installs it.
- CLI availability is testable after bootstrap.
- Failure output is actionable.

### H04. Install or verify Chrome in the base VM

Status: Done
Depends on: H01
Acceptance criteria:

- Bootstrap flow verifies Chrome is present or installs it.
- Chrome is ready for managed relogin/browser persona workflows.
- Failure output is actionable.

### H05. Install or verify Node in the base VM

Status: Done
Depends on: H01
Acceptance criteria:

- Bootstrap flow verifies Node is present or installs it.
- Node is sufficient for guest bridge assets that require it.
- Failure output is actionable.

### H06. Install guest bridge assets into the base VM

Status: Done
Depends on: H01, G08
Acceptance criteria:

- Bootstrap copies or prepares the guest bridge assets needed by VM mode.
- Asset installation is idempotent.
- The bootstrap output records where the bridge assets were installed.

### H07. Install guest auto-start hook in the base VM

Status: Done
Depends on: H01, H06
Acceptance criteria:

- Bootstrap installs a guest auto-start mechanism for the bridge.
- The hook is suitable for sealed-base cloning.
- The hook can be inspected or replaced later without manual reverse engineering.

### H08. Persist a seal marker and base metadata

Status: Done
Depends on: H01, H06, H07
Acceptance criteria:

- Bootstrap writes a clear seal marker indicating the base has been finalized.
- Stored metadata includes the bridge root and seal time.
- Operators can distinguish sealed vs unsealed bases quickly.

### H09. Document bootstrap prerequisites and APFS assumptions

Status: Done
Depends on: H01
Acceptance criteria:

- Bootstrap usage docs explain required host tooling and VM prerequisites.
- Documentation explicitly calls out APFS assumptions for cheap persona storage.
- Documentation explains that bootstrap is a one-time provisioning helper.

## Track I: Rollback, Diagnostics, And Failure Isolation

### I01. Define shared failure phases in the orchestrator

Status: Done
Depends on: D01, E09
Acceptance criteria:

- Failures are categorized as prepare, export, activate, import, commit, or rollback failures.
- Diagnostics name the failing phase explicitly.
- Phase labeling is consistent across host and VM flows.

### I02. Restore source auth and pool state after failed target activation

Status: Done
Depends on: C07, I01
Acceptance criteria:

- Failed target activation does not leave the target account committed as active.
- Pool state and auth state are restored together.
- Restoration behavior is test-covered.

### I03. Restore source host symlinks after failed host switch

Status: Done
Depends on: F04, F08, I01
Acceptance criteria:

- Failed host switch restores the original live symlink targets.
- The runtime does not leave half-switched live roots behind.
- Recovery is safe even when target persona creation partially completed.

### I04. Restore source VM after failed VM switch

Status: Done
Depends on: G07, G12, G14, I01
Acceptance criteria:

- Failed VM activation or import attempts to restore the source VM environment.
- The operator is told whether restoration fully succeeded.
- Partial rollback is surfaced explicitly rather than hidden.

### I05. Emit actionable operator diagnostics without corrupting state

Status: Done
Depends on: I01
Acceptance criteria:

- Errors explain what failed, at what phase, and what state may need inspection.
- Diagnostics do not require reading raw internals to understand the failure mode.
- Error reporting does not itself mutate active state.

### I06. Add rollback and failure-path tests across host and VM modes

Status: Done
Depends on: I02, I03, I04, I05
Acceptance criteria:

- Tests cover failure before commit and after activation.
- Tests cover host rollback.
- Tests cover VM rollback.
- Tests prove source state is preserved when target activation fails.

## Track J: End-To-End Verification And Documentation

### J01. Add unit tests for environment/persona schema round-trip

Status: Done
Depends on: A08
Acceptance criteria:

- Tests cover legacy, partial, and full schema forms.
- Tests prove default host selection.
- Tests prove persona metadata round-trips correctly.

### J02. Add unit tests for deterministic persona/profile assignment

Status: Done
Depends on: A05, A06, B05
Acceptance criteria:

- Tests prove stable assignment for the same account identity.
- Tests prove assignments do not drift due to unrelated pool changes.
- Tests prove host and VM use the same profile id contract.

### J03. Add unit tests for backend selection defaults and overrides

Status: Done
Depends on: D02
Acceptance criteria:

- Tests prove default backend selection is `host`.
- Tests prove env overrides work.
- Tests prove invalid override values fail clearly.

### J04. Add shared acceptance tests for unchanged public command behavior

Status: Done
Depends on: D06, D07, D08
Acceptance criteria:

- `next`, `prev`, `relogin`, and watch still work from the user’s point of view.
- Backend selection remains invisible in the public command surface.
- Output and failure behavior stay operator-usable.

### J05. Add host-mode acceptance tests

Status: Done
Depends on: F13, E10
Acceptance criteria:

- Host remains the default mode.
- Cross-account host switching recreates active/recoverable work in the target persona.
- Same-account reopen preserves native history.
- Host-mode acceptance explicitly covers account-local Chrome/Playwright automation state.

### J06. Add VM-mode acceptance tests

Status: Done
Depends on: G15, E10
Acceptance criteria:

- VM mode uses the same shared pipeline as host mode.
- VM mode adds hardware-level isolation without changing public commands.
- VM failures trigger rollback rather than partial commit.

### J07. Update operator documentation for environment and VM config

Status: Done
Depends on: A01, A02, G02
Acceptance criteria:

- Docs explain `environment` and `vm` config fields.
- Docs explain default host behavior.
- Docs explain when VM mode is appropriate and what it adds.

### J08. Update migration documentation from cleanup-only host design

Status: Done
Depends on: F03, F04
Acceptance criteria:

- Docs explain the shift from cleanup-only behavior to persistent host personas.
- Docs explain first-run host migration behavior.
- Docs explain how live symlink-based isolation works at a high level.

## Track K: Corner Cases, Recovery, And Operational Hardening

### K01. Add a shared rotation lock to prevent concurrent mutations

Status: Done
Depends on: D01
Acceptance criteria:

- CLI, daemon manual actions, relogin, and watch-triggered rotation cannot mutate active state concurrently.
- Lock acquisition failure produces a clear operator-facing message rather than corrupting state.
- Lock ownership is released safely after success, failure, or timeout.

### K02. Handle no-op target selection explicitly

Status: Done
Depends on: C01, D03, D04, D05
Acceptance criteria:

- When the selected target is already active, runtime takes a no-op or stay path intentionally.
- Same-account reopen does not perform unnecessary handoff/import work.
- Output distinguishes intentional no-op from failure.

### K03. Detect pool/auth drift before commit

Status: Done
Depends on: C04, C05
Acceptance criteria:

- Runtime validates that active pool selection and live auth still agree before final commit.
- Drift detected after prepare but before commit results in an explicit repair or rollback path.
- Commit never silently cements mismatched pool and auth state.

### K04. Handle missing or corrupt live auth files

Status: Done
Depends on: C05, I01
Acceptance criteria:

- Missing auth files produce a recoverable error where possible.
- Corrupt auth files do not crash the full runtime path with opaque errors.
- Diagnostics tell the operator whether the problem is missing auth, malformed auth, or account mismatch.

### K05. Handle removed, disabled, or invalid target accounts mid-flow

Status: Done
Depends on: C02, C03, C06
Acceptance criteria:

- If a target account becomes invalid after prepare, runtime aborts without partial activation.
- Disabled-domain rules are rechecked at commit-sensitive boundaries when needed.
- Operator-visible output names the invalidated target account.

### K06. Add explicit handling for `create_required` edge cases

Status: Done
Depends on: C02, C07, D03
Acceptance criteria:

- Auto-create retry budget is enforced explicitly.
- Failed create-and-retry restores the previous active account cleanly.
- Output distinguishes quota exhaustion from create-flow failure.

### K07. Make first-run host migration resumable after interruption

Status: Done
Depends on: F03, F04
Acceptance criteria:

- If migration is interrupted mid-run, re-running migration resumes or repairs state safely.
- Partially moved directories are detected rather than overwritten blindly.
- Live roots are never left in an ambiguous half-migrated state without repair guidance.

### K08. Repair broken symlinks and unexpected live-root shapes

Status: Done
Depends on: F04
Acceptance criteria:

- Runtime can detect broken symlinks, plain directories where symlinks are expected, and stale persona targets.
- Repair logic is explicit and idempotent.
- Unexpected filesystem shapes produce an actionable error instead of unsafe deletion.

### K09. Handle source Codex already stopped or app-server unavailable

Status: Done
Depends on: F07, E02
Acceptance criteria:

- If source Codex is not running, host rotation can skip shutdown-specific steps safely.
- Missing or unresponsive app-server is surfaced as a distinct failure mode.
- Rotation can still proceed when no handoff is required and safety conditions allow it.

### K10. Handle stale PIDs and stale managed-instance state

Status: Done
Depends on: F07, F10
Acceptance criteria:

- Managed-instance detection ignores stale PID records or stale socket state when possible.
- Runtime can recover from stale-process metadata without manual cleanup in common cases.
- Diagnostics identify when stale state blocked automatic recovery.

### K11. Guard against unsupported or drifting app-server API shapes

Status: Done
Depends on: E03, E05, E06, E07
Acceptance criteria:

- Runtime validates required response fields from `thread/read`, `thread/start`, inject, and `turn/start`.
- Unsupported API response shapes fail clearly instead of panicking.
- Parsing logic degrades safely when optional fields are absent.

### K12. Handle oversized, malformed, or unsupported handoff content

Status: Done
Depends on: E04, E08
Acceptance criteria:

- Oversized threads are truncated predictably.
- Unsupported attachment/item types are skipped deliberately.
- Malformed item payloads do not abort the entire multi-thread handoff batch unless safety requires it.

### K13. Handle import partial success without committing broken target state

Status: In Progress
Depends on: E06, E07, E09
Acceptance criteria:

- Runtime distinguishes full import success, partial import success, and total import failure.
- Partial success does not silently become the new active state.
- Diagnostics identify which threads imported successfully and which failed.

### K14. Handle guest bridge unavailability and version mismatch

Status: Done
Depends on: G08, G10, G11
Acceptance criteria:

- VM backend detects when the guest bridge is missing, unreachable, or running an incompatible protocol version.
- The failure is reported separately from Codex app-server errors inside the guest.
- VM mode does not partially commit target state when the bridge handshake fails.

### K15. Handle missing UTM app, missing base package, and invalid persona package paths

Status: Done
Depends on: G02, G03, G04, G06
Acceptance criteria:

- VM backend validates configured UTM and package paths before destructive work.
- Missing or invalid paths fail fast with actionable diagnostics.
- Path validation prevents accidental use of unrelated directories as persona packages.

### K16. Add explicit behavior when APFS cloning assumptions are unavailable

Status: Done
Depends on: G04, H09
Acceptance criteria:

- The system either provides a supported fallback or fails clearly when APFS-friendly cloning is unavailable.
- Docs and diagnostics explain the operational consequence.
- Runtime does not pretend cheap persona provisioning is available when it is not.

### K17. Handle guest boot success but guest Codex unavailability

Status: Done
Depends on: G06, G09, G10
Acceptance criteria:

- VM backend distinguishes VM boot readiness from Codex readiness inside the guest.
- Guest boot success with missing/unhealthy guest Codex triggers rollback rather than commit.
- Diagnostics identify whether the failure is VM boot, bridge, or guest Codex availability.

### K18. Handle relogin success with failed host-side auth sync

Status: Done
Depends on: G11, G13, F12
Acceptance criteria:

- If relogin succeeds but host-side auth persistence fails, runtime reports the mismatch explicitly.
- The active account is not falsely reported as healthy when sync failed.
- Recovery guidance explains whether guest state or host state is authoritative after failure.

### K19. Handle commit success followed by relaunch failure

Status: Done
Depends on: F10, G12, I01
Acceptance criteria:

- Runtime distinguishes successful activation/commit from failed post-commit relaunch.
- Diagnostics tell the operator that account activation succeeded even if Codex did not relaunch.
- The system avoids rolling back already-correct committed state unless rollback is safe and explicit.

### K20. Add crash-recovery logic for mid-rotation interruption

Status: In Progress
Depends on: C07, I01
Acceptance criteria:

- If the process crashes mid-rotation, the next run can detect incomplete work and repair or report it.
- Recovery logic can determine whether source or target should be treated as authoritative.
- Crash recovery does not require manual database surgery for common interruption cases.

### K21. Validate persona, bridge, and VM paths for safety

Status: Done
Depends on: A02, F01, G03, G08
Acceptance criteria:

- Path validation rejects empty, relative-when-unsupported, or traversal-prone paths where safety requires strictness.
- Runtime does not follow unexpected path shapes into unrelated user directories silently.
- Validation rules are documented and test-covered.

### K22. Add permission-error handling for persona roots and live paths

Status: In Progress
Depends on: F03, F04, G04, K21
Acceptance criteria:

- Permission failures surface the exact path that failed.
- Runtime avoids partial destructive changes after a permission error.
- Common permission failures are distinguishable from missing-path failures.

### K23. Add disk-space and capacity checks for persona provisioning

Status: Done
Depends on: F05, G04
Acceptance criteria:

- Persona provisioning checks for obvious insufficient-space conditions before large copy/clone steps when practical.
- Low-space failures are reported before partial provisioning corrupts expectations.
- Diagnostics identify which provisioning path ran out of space.

### K24. Add network and egress-mismatch handling

Status: Done
Depends on: B04, G12
Acceptance criteria:

- Validation mode can distinguish missing connectivity from region-mismatch failures.
- Region/egress mismatches fail clearly rather than silently downgrading persona realism guarantees.
- Operators can tell whether the failure is network reachability, egress mismatch, or validation plumbing.

### K24a. Handle BrowserForge generation drift or upstream output changes

Status: Done
Depends on: B06, B07, B08, B11
Acceptance criteria:

- The system detects when BrowserForge output shape or generation behavior changes enough to threaten persona coherence.
- Tests or validation catch incompatible BrowserForge-backed persona output before it silently enters provisioning flows.
- Diagnostics distinguish runtime bugs from upstream generation-model drift.

### K25. Add watch/manual/daemon contention tests

Status: Done
Depends on: K01
Acceptance criteria:

- Tests simulate overlapping rotation requests from different entry points.
- Tests prove only one mutation path can own the active rotation.
- Failed contenders leave no partial state behind.

### K26. Add interruption and recovery tests

Status: In Progress
Depends on: K07, K19, K20
Acceptance criteria:

- Tests cover interrupted host migration.
- Tests cover mid-rotation crash or forced termination.
- Tests prove next-run recovery can identify and repair common incomplete states.

### K27. Add API-drift and malformed-payload tests

Status: Done
Depends on: K11, K12, K13
Acceptance criteria:

- Tests cover missing response fields from native app-server methods.
- Tests cover malformed exported items and oversized payloads.
- Tests prove failures are explicit and non-panicking.

### K28. Add guest-bridge and VM-path failure tests

Status: Done
Depends on: K14, K15, K16, K17, K18
Acceptance criteria:

- Tests cover missing guest bridge, bridge protocol mismatch, missing UTM app, and invalid base/persona package paths.
- Tests cover guest boot success with guest Codex failure.
- Tests prove VM mode does not falsely commit broken target state.

## Suggested Execution Order

- Phase 1: A01-A08, B01-B05, C01-C08
- Phase 1a: B06-B11
- Phase 2: D01-D08, E01-E08
- Phase 3: F01-F13, I01-I03, J01-J05, J07-J08
- Phase 4: G01-G15, H01-H09, I04-I06, J06
- Phase 5: K01-K28
