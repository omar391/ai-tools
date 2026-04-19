# Shared Persona Rotation E2E And Live Confidence Tasks

## Goal

Define the backlog needed so host and VM rotation can be considered confidenceable through automated system coverage instead of ad hoc manual live testing.

## Planning Baseline

- This file tracks test confidence work only and does not replace the feature-delivery backlog in `tasks.md`.
- Hermetic system coverage and live acceptance are separate layers and should remain separately executable.
- No new public runtime commands are introduced for testing; the system under test stays `next`, `prev`, `relogin`, and watch-triggered rotation.
- Live suites must run only in isolated personas and staging accounts, never against the user's default Codex state.
- No task may be considered done if it leaks windows, background processes, temp profiles, mounts, sockets, or symlink state.
- Unit tasks are intentionally kept as independent as possible so a single helper, scenario, or failure mode can be landed and verified on its own.
- Execution priority is host-first: complete shared harness work and host confidence tracks before expanding into VM-specific coverage beyond prerequisite scaffolding.

## Status Legend

- `Pending`: Not started or not yet verified.
- `In Progress`: Implementation started but not yet complete.
- `Blocked`: Cannot proceed due to prerequisite or external dependency.
- `Done`: Implemented and verified against acceptance criteria.

## Constraints And Assumptions

- Host and VM confidence tracks both exist from day one even if their execution readiness differs.
- Host confidence work is the first implementation priority because it is runnable on the current machine shape and removes the largest amount of manual live validation earliest.
- Full-confidence standard is `staged + live`, meaning hermetic system coverage plus scripted live acceptance for the relevant backend.
- Host live work is runnable on machines with Codex Desktop and Chrome installed.
- VM live work is blocked until real UTM prerequisites exist on the executing machine.
- The current repo already has partial integration coverage but not full-system confidence.
- Live suites must use dedicated staging accounts, dedicated isolated persona roots, and dedicated test-only state paths.
- Browser isolation confidence must explicitly include managed Chrome and Playwright/fast-browser behavior, not only Codex Desktop behavior.
- VM live validation remains infrastructure-gated on machines without `UTM.app`, `utmctl`, a sealed base package, and guest-bridge prerequisites.

## Current Branch Analysis

- Existing automated coverage already includes CLI/app integration and E2E-named tests such as [daemon_e2e.rs](/Volumes/Projects/business/AstronLab/omar391/ai-tools/worktrees/shared-persona-rotation/packages/codex-rotate/crates/codex-rotate-cli/tests/daemon_e2e.rs:1), [managed_login_e2e.rs](/Volumes/Projects/business/AstronLab/omar391/ai-tools/worktrees/shared-persona-rotation/packages/codex-rotate/crates/codex-rotate-cli/tests/managed_login_e2e.rs:1), and [ipc_e2e.rs](/Volumes/Projects/business/AstronLab/omar391/ai-tools/worktrees/shared-persona-rotation/packages/codex-rotate-app/src-tauri/tests/ipc_e2e.rs:1).
- Current coverage does not fully prove real Codex Desktop lifecycle management end to end, especially around launch, reachability, and guaranteed cleanup after tests.
- Current coverage does not fully prove managed browser isolation end to end, especially preventing system-browser escape during relogin and browser automation flows.
- Current coverage does not fully prove real UTM and guest lifecycle orchestration end to end; current VM confidence is still primarily code-path and hermetic-test confidence.
- VM live validation is currently infrastructure-gated on machines without `UTM.app` and `utmctl`.
- The current branch already contains host/VM orchestration and bootstrap implementation in runtime code, so the remaining gap is confidence depth rather than missing backlog structure.

## Track A: Shared E2E Harness And Safety Rails

### A01. Record the current automated-confidence baseline

Status: Done
Depends on: None
Acceptance criteria:

- The backlog explicitly records the existing E2E/integration files already present in the branch.
- The backlog explicitly identifies the remaining host-live and VM-live confidence gaps instead of implying they are already covered.
- The baseline distinguishes hermetic integration/system coverage from real-infrastructure acceptance coverage.

### A02. Add a shared isolated-home fixture

Status: Done
Depends on: None
Acceptance criteria:

- The fixture provisions isolated `HOME`, `CODEX_ROTATE_HOME`, `CODEX_HOME`, `FAST_BROWSER_HOME`, and Codex app-support roots for every test run.
- The fixture can be reused by both hermetic and live suites without relying on ambient machine state.
- The fixture makes it impossible for a passing test to read or write the operator's default Codex state accidentally.

### A03. Add a shared isolated-account-state fixture

Status: Pending
Depends on: A02
Acceptance criteria:

- The fixture can seed a multi-account `accounts.json` with stable test accounts and active-index state.
- The fixture can materialize persona roots and backend-specific state roots without touching real user directories.
- The fixture supports per-test customization of environment mode, active account, and persona metadata.

### A04. Add a fake Codex app-server fixture

Status: Pending
Depends on: A02
Acceptance criteria:

- The fixture supports `thread/read`, `thread/start`, `thread/injectItems`, and `turn/start` with deterministic request and response control.
- The fixture supports login-related flows needed by relogin and managed-login scenarios.
- The fixture can inject targeted failures and timeouts without changing test-owned fixture code for every scenario.

### A05. Add a fake managed-browser opener fixture

Status: Pending
Depends on: A02
Acceptance criteria:

- The fixture proves which managed browser profile path was requested by the system under test.
- The fixture fails the test if a browser launch escapes the isolated managed profile or falls back to the system browser.
- The fixture can simulate success, launch failure, and post-launch callback behavior independently.

### A06. Add a watch-trigger harness

Status: Pending
Depends on: A02
Acceptance criteria:

- The harness can trigger watch-based rotation deterministically without depending on real file churn or operator timing.
- The harness exposes enough control to verify watch-triggered `next` and watch-triggered failure rollback behavior separately.
- The harness can be reused by both host and VM watch-path tests.

### A07. Add process-tracking helpers for Codex, Chrome, utmctl, guest bridge, and helper scripts

Status: Pending
Depends on: A02
Acceptance criteria:

- The helpers record child processes started during a test with enough metadata to assert ownership and cleanup.
- The helpers can distinguish test-managed processes from operator-owned processes already running on the machine.
- The helpers are reusable from both hermetic and live suites.

### A08. Add window/leak cleanup assertions

Status: Pending
Depends on: A07
Acceptance criteria:

- Tests fail if managed Codex, browser, or VM-related processes remain alive after completion.
- Cleanup assertions can run on both success and failure paths so leaks are not masked by earlier assertion failures.
- Cleanup assertions report the leaked executable, arguments, and owning test context.

### A09. Add temp-path, socket, mount, and symlink cleanup assertions

Status: Pending
Depends on: A02
Acceptance criteria:

- Tests fail if temp profiles, sockets, mounts, or live symlink targets are left behind after execution.
- Assertions cover both host persona roots and VM persona package side effects.
- Cleanup failure output identifies the remaining path type and concrete path so operators can diagnose leaks quickly.

### A10. Add artifact capture helpers for failure triage

Status: Pending
Depends on: A02
Acceptance criteria:

- Failing suites can retain logs, app-server transcripts, symlink maps, process snapshots, and screenshots when relevant.
- Artifact capture is opt-in for passing runs and automatic for failing runs.
- Artifact paths are isolated per test case so concurrent failures do not overwrite each other.

### A11. Add explicit failure-injection hooks for activation, handoff, relogin, bridge, and shutdown

Status: Pending
Depends on: A04, A05
Acceptance criteria:

- Each major failure mode can be triggered independently without editing production code.
- Failure hooks support at least immediate failure, timeout, and partial-progress failure shapes where applicable.
- The harness can assert that rollback behavior happened after the injected failure rather than before it.

### A12. Add host live-capability gating

Status: Pending
Depends on: A02
Acceptance criteria:

- Host live gating detects `Codex.app`, Chrome, and required staging-account environment variables before a live host suite starts.
- Missing prerequisites fail loudly as a skipped-or-blocked precondition rather than causing partial live execution.
- The gate verifies that the configured live paths are isolated and not equal to the operator's default state roots.

### A13. Add VM live-capability gating

Status: Pending
Depends on: A02
Acceptance criteria:

- VM live gating detects `UTM.app`, `utmctl`, base package path, bridge root, APFS-backed persona root, and staging-account requirements before a live VM suite starts.
- Missing prerequisites fail loudly as blocked prerequisites rather than producing false-negative VM failures.
- The gate verifies that VM live execution is targeting isolated persona roots and test-only state paths.

### A14. Add CI profile split for hermetic-host, hermetic-vm, live-host, and live-vm

Status: Pending
Depends on: A08, A12, A13
Acceptance criteria:

- PR-safe hermetic suites are separable from opt-in live suites through an explicit profile or command split.
- Host and VM live suites can be invoked independently without editing test code or test manifests.
- The split makes it unambiguous which suites are required for PR confidence and which suites require live infrastructure.

## Track B: Host Hermetic System Coverage

### B01. Add hermetic host next happy-path coverage

Status: Pending
Depends on: A03, A04, A08
Acceptance criteria:

- The test covers a successful `next` rotation across two host personas using only isolated state.
- The test proves target persona activation and active-account commit happen in the expected order.
- The test asserts cleanup before success.

### B02. Add hermetic host prev happy-path coverage

Status: Pending
Depends on: A03, A04, A08
Acceptance criteria:

- The test covers a successful `prev` rotation after an initial forward switch in host mode.
- The test proves the prior persona is reactivated without corrupting current pool state.
- The test asserts cleanup before success.

### B03. Add hermetic host relogin happy-path coverage

Status: Pending
Depends on: A03, A05, A08
Acceptance criteria:

- The test covers a successful host-mode `relogin` using the target account's managed browser persona.
- The test proves relogin state is applied to the intended account and not to the currently active source by mistake.
- The test asserts cleanup before success.

### B04. Add hermetic watch-triggered host rotation happy-path coverage

Status: Pending
Depends on: A03, A04, A06, A08
Acceptance criteria:

- The test proves watch-triggered rotation reaches the same shared host orchestrator path as manual rotation.
- The test proves the triggered rotation commits the correct target account and persona state.
- The test asserts cleanup before success.

### B05. Add hermetic same-account reopen coverage

Status: Pending
Depends on: A03, A04
Acceptance criteria:

- The test proves returning to the same account persona does not require synthetic cross-account handoff recreation.
- The test proves native persona history remains available when reopening the same account.
- The test remains isolated from live state.

### B06. Add hermetic first-run host migration coverage

Status: Pending
Depends on: A02, A03
Acceptance criteria:

- The test covers first-run migration from legacy live host state into the current account's persona root.
- The test proves live host paths become the expected symlinks after migration.
- The test proves migration preserves the original state instead of dropping it.

### B07. Add hermetic pre-migration compatibility coverage

Status: Pending
Depends on: A02, A03
Acceptance criteria:

- The test proves the system preserves existing behavior when migration has not been performed yet.
- The test covers loading older state without persona migration artifacts already present.
- The test proves pre-migration execution does not create partially migrated host state.

### B08. Add hermetic active-thread handoff coverage

Status: Pending
Depends on: A03, A04
Acceptance criteria:

- The test proves active-thread continuity uses only native app-server APIs for export and import.
- The test proves the recreated thread appears in the target persona with the expected minimal handoff payload.
- The test proves no custom registry or direct storage cloning is required.

### B09. Add hermetic recoverable-thread handoff coverage

Status: Pending
Depends on: A03, A04
Acceptance criteria:

- The test proves recoverable work continuity uses the same native app-server handoff path as active work.
- The test proves only recoverable state intended for continuity is transferred.
- The test verifies the target persona receives the recoverable handoff independently of same-account history.

### B10. Add hermetic staged-prepare no-early-mutation coverage

Status: Pending
Depends on: A03, A11
Acceptance criteria:

- The test proves prepare-stage logic does not mutate active account state before target activation succeeds.
- The test injects a failure during preparation or readiness gating and verifies the source remains authoritative.
- The test isolates state inspection to the test-owned pool and persona roots.

### B11. Add hermetic commit-only-after-target-ready coverage

Status: Pending
Depends on: A03, A11
Acceptance criteria:

- The test proves commit-stage mutation happens only after the target persona is ready to become active.
- The test injects a target-readiness failure and verifies active index and auth state remain unchanged.
- The test distinguishes staged preparation from committed activation in its assertions.

### B12. Add hermetic live-symlink swap coverage

Status: Pending
Depends on: A02, A03
Acceptance criteria:

- The test proves host live paths are repointed to the target account persona root during a switch.
- The test asserts the final symlink targets for Codex state, app-support state, and managed browser state.
- The test proves symlink updates are coherent across all host-isolated state roots.

### B13. Add hermetic browser-persona selection coverage for relogin

Status: Pending
Depends on: A03, A05
Acceptance criteria:

- The test proves relogin chooses the target account's managed browser persona rather than a global or source persona.
- The test asserts the exact managed browser root or profile path requested by the system under test.
- The test remains hermetic and does not open the system browser.

### B14. Add hermetic BrowserForge-derived browser-input coherence coverage

Status: Pending
Depends on: A03, A05
Acceptance criteria:

- The test proves host browser automation receives persona-derived inputs coherent with the assigned persona profile.
- The test covers at least locale, timezone, user agent, and viewport or screen-derived inputs.
- The test proves the browser input set is associated with the correct account persona.

### B15. Add hermetic rollback coverage for target Codex launch failure

Status: Pending
Depends on: A03, A04, A11
Acceptance criteria:

- The test injects a target Codex launch failure during host rotation.
- The test proves source persona state, active index, and live symlink targets are restored.
- The test asserts cleanup before success.

### B16. Add hermetic rollback coverage for handoff export failure

Status: Pending
Depends on: A03, A04, A11
Acceptance criteria:

- The test injects a failure while exporting handoff state from the source environment.
- The test proves no partial target activation is committed after export failure.
- The test asserts cleanup before success.

### B17. Add hermetic rollback coverage for handoff import failure

Status: Pending
Depends on: A03, A04, A11
Acceptance criteria:

- The test injects a failure while importing handoff state into the target environment.
- The test proves committed host state is restored to the source account after the failure.
- The test asserts cleanup before success.

### B18. Add hermetic rollback coverage for relogin failure

Status: Pending
Depends on: A03, A05, A11
Acceptance criteria:

- The test injects a relogin failure for a target account in host mode.
- The test proves account activation state and browser persona selection do not end in a half-switched condition.
- The test asserts cleanup before success.

### B19. Add hermetic rollback coverage for watch-triggered target failure

Status: Pending
Depends on: A03, A04, A06, A11
Acceptance criteria:

- The test injects a failure during a watch-triggered host rotation.
- The test proves watch-triggered failure uses the same rollback semantics as manual rotation.
- The test asserts cleanup before success.

### B20. Add hermetic host no-system-browser guarantee coverage

Status: Pending
Depends on: A05, A08
Acceptance criteria:

- The test proves host relogin and browser automation paths do not launch the system browser when managed-browser mode is expected.
- The test fails if the opener falls back to a non-isolated browser path.
- The test asserts cleanup before success.

### B21. Add hermetic host zero-leaked-resource coverage

Status: Pending
Depends on: A07, A08, A09
Acceptance criteria:

- The test suite asserts host hermetic scenarios leave no Codex windows, browser windows, helper processes, temp profiles, sockets, or stale symlinks behind.
- The test proves leak detection runs even when an earlier behavioral assertion fails.
- The test output identifies leaked resources precisely enough to diagnose cleanup regressions.

## Track C: Host Live Acceptance Coverage

### C01. Add real Codex Desktop smoke coverage in isolated host personas

Status: Pending
Depends on: A12, A08
Acceptance criteria:

- The live test launches real Codex Desktop against isolated host persona roots and verifies reachability.
- The test proves the operator's default Codex state is not reused.
- The test closes the launched Codex resources before success.

### C02. Add real Codex Desktop auto-close-on-test-exit coverage

Status: Pending
Depends on: A12, A08
Acceptance criteria:

- The live test proves test-managed Codex launches are tracked and closed automatically on test completion or abrupt test exit.
- The test distinguishes test-managed Codex instances from already running operator-owned instances.
- The test fails if a test-managed Codex instance remains open after teardown.

### C03. Add real managed-relogin smoke coverage

Status: Pending
Depends on: A12, A05, A08
Acceptance criteria:

- The live test performs a real managed relogin with isolated host persona state and staging credentials.
- The test proves the relogin flow completes through the managed browser path rather than a manual fallback.
- The test closes browser and Codex resources before success.

### C04. Add real managed-relogin no-system-browser guarantee coverage

Status: Pending
Depends on: A12, A05, A08
Acceptance criteria:

- The live test proves relogin does not open the system browser outside the managed isolated profile.
- The test fails if any browser window or process is attributable to a non-managed path.
- The test closes browser and Codex resources before success.

### C05. Add real host next acceptance across two staging accounts

Status: Pending
Depends on: C01
Acceptance criteria:

- The live test performs a real `next` rotation across two staging accounts in host mode.
- The test proves the target account becomes active with isolated persona state.
- The test closes test-managed resources before success.

### C06. Add real host prev acceptance across two staging accounts

Status: Pending
Depends on: C01
Acceptance criteria:

- The live test performs a real `prev` rotation after at least one forward host rotation.
- The test proves the prior staging account resumes correctly with its persona state intact.
- The test closes test-managed resources before success.

### C07. Add real same-account reopen acceptance

Status: Pending
Depends on: C01
Acceptance criteria:

- The live test proves reopening the same host account reuses its persistent persona state without synthetic cross-account recovery.
- The test verifies previously native account history remains available.
- The test closes test-managed resources before success.

### C08. Add real active-thread continuity acceptance

Status: Pending
Depends on: C05
Acceptance criteria:

- The live test proves active-thread continuity works across a real host rotation between staging accounts.
- The test verifies the target persona receives the expected active-thread handoff through native APIs.
- The test closes test-managed resources before success.

### C09. Add real recoverable-thread continuity acceptance

Status: Pending
Depends on: C05
Acceptance criteria:

- The live test proves recoverable-thread continuity works across a real host rotation between staging accounts.
- The test verifies the handoff payload is limited to recoverable work intended for continuity.
- The test closes test-managed resources before success.

### C10. Add real host rollback acceptance for induced target-start failure

Status: Pending
Depends on: C05
Acceptance criteria:

- The live test induces a target-start failure during a host rotation.
- The test proves the source account remains or is restored as the active persona after failure.
- The test closes test-managed resources before success.

### C11. Add real host rollback acceptance for induced relogin failure

Status: Pending
Depends on: C03
Acceptance criteria:

- The live test induces a relogin failure for a staging account in host mode.
- The test proves the account and persona state are not left partially switched.
- The test closes test-managed resources before success.

### C12. Add real host watch-triggered acceptance

Status: Pending
Depends on: C05
Acceptance criteria:

- The live test proves watch-triggered host rotation exercises the same production runtime path as manual host rotation.
- The test verifies the triggered switch lands on the expected account with isolated persona state.
- The test closes test-managed resources before success.

### C13. Add real host artifact-capture-on-failure coverage

Status: Pending
Depends on: A10, A12
Acceptance criteria:

- The live host suite can retain logs, screenshots, transcripts, and process snapshots on failure.
- Artifact capture does not require rerunning the failed scenario with extra flags after the fact.
- Artifact directories stay isolated per failing test case.

### C14. Add real host zero-residual-window-and-process coverage

Status: Pending
Depends on: A07, A08, A09, A12
Acceptance criteria:

- The live host suite fails if Codex windows, browser windows, helper processes, temp profiles, sockets, or stale symlinks remain after a run.
- The suite distinguishes between test-managed resources and unrelated operator-owned processes already present.
- Cleanup assertions run after both success and failure paths.

## Track D: VM Harness And Virtualization Test Infrastructure

### D01. Add a fake UTM app-bundle fixture

Status: Pending
Depends on: A02
Acceptance criteria:

- The fixture can stand in for `UTM.app` discovery logic without requiring real UTM installation.
- The fixture exposes deterministic paths and metadata needed by VM orchestration code.
- The fixture can be reused across hermetic VM runtime tests.

### D02. Add a fake utmctl fixture

Status: Pending
Depends on: A02
Acceptance criteria:

- The fixture simulates `utmctl` commands used by runtime activation and shutdown flows.
- The fixture supports success, timeout, and command-failure behaviors independently.
- The fixture records invocations for assertion by test code.

### D03. Add a fake guest-bridge fixture

Status: Pending
Depends on: A02
Acceptance criteria:

- The fixture simulates guest-bridge import, export, relogin, and Codex control commands.
- The fixture supports deterministic responses and injected failures per command.
- The fixture can be shared across both runtime and bootstrap-oriented VM tests.

### D04. Add a fake VM persona-package clone fixture

Status: Pending
Depends on: A02
Acceptance criteria:

- The fixture simulates per-account VM persona package creation from a base package.
- The fixture records which persona package was selected for each account.
- The fixture can simulate clone collisions and missing-base failures independently.

### D05. Add a mounted-guest-root fixture for bootstrap tests

Status: Pending
Depends on: A02
Acceptance criteria:

- The fixture provides an isolated mounted guest-root layout suitable for bootstrap asset installation tests.
- The fixture supports inspection of installed bridge assets, LaunchAgents, and seal metadata after execution.
- The fixture can be cleaned up fully after each test.

### D06. Add guest-bridge transcript capture helpers

Status: Pending
Depends on: D03
Acceptance criteria:

- The helpers capture guest-bridge command requests and responses in sequence for later assertion.
- The helpers can be attached to both successful and failing VM scenarios.
- Transcript capture remains isolated per test case.

### D07. Add VM process and package cleanup assertions

Status: Pending
Depends on: A07, A09
Acceptance criteria:

- The assertions fail if VM-related processes, persona packages, temp bridge state, or mount artifacts remain after a VM test.
- The assertions distinguish test-managed VM resources from unrelated operator-managed VM resources when relevant.
- Cleanup assertions can run after both success and failure paths.

### D08. Add one-active-VM invariant helpers

Status: Pending
Depends on: D01, D02, D04
Acceptance criteria:

- The helpers can assert that only one VM is considered active at a time during a rotation sequence.
- The helpers can detect simultaneous-active regression scenarios in hermetic tests.
- The invariant helpers work independently of real UTM presence.

### D09. Add VM failure-injection hooks for boot, shutdown, bridge connect, bridge command, import, export, and relogin

Status: Pending
Depends on: D01, D02, D03
Acceptance criteria:

- Each listed VM failure mode can be triggered independently by tests.
- Failure hooks support targeted phase timing so rollback assertions can verify correct sequencing.
- The injected failures do not require editing production code or fixture implementations per test case.

### D10. Add real-UTM launcher/closer helpers for live suites

Status: Pending
Depends on: A13
Acceptance criteria:

- The helpers can start and stop real UTM-managed VMs needed by live acceptance suites.
- The helpers track the exact VM instances they launched so cleanup can target only test-owned resources.
- The helpers fail loudly if real UTM prerequisites are missing or misconfigured.

### D11. Add bootstrap seal-validation helpers

Status: Pending
Depends on: D05
Acceptance criteria:

- The helpers can assert the presence and correctness of bootstrap-installed seal metadata.
- The helpers can validate installed LaunchAgent content and guest-bridge asset placement.
- The helpers are reusable across bootstrap asset-install, LaunchAgent, and seal-metadata tests.

### D12. Add VM live artifact-capture helpers

Status: Pending
Depends on: A10, D06
Acceptance criteria:

- The helpers capture UTM, `utmctl`, guest-bridge, and runtime logs needed for live VM failure diagnosis.
- Artifact capture includes enough context to reconstruct the active persona package and test-owned VM paths.
- Artifacts remain isolated per failing live VM scenario.

## Track E: VM Hermetic System Coverage

### E01. Add hermetic VM bootstrap asset-install coverage

Status: Pending
Depends on: D05, D11
Acceptance criteria:

- The test proves bootstrap installs the expected guest-bridge assets into the mounted guest root.
- The test asserts installed asset paths and names explicitly.
- The test cleans up the mounted guest-root fixture after completion.

### E02. Add hermetic VM bootstrap LaunchAgent-install coverage

Status: Pending
Depends on: D05, D11
Acceptance criteria:

- The test proves bootstrap installs the expected LaunchAgent into the mounted guest root.
- The test asserts the LaunchAgent points at the expected bridge assets or startup flow.
- The test cleans up the mounted guest-root fixture after completion.

### E03. Add hermetic VM bootstrap seal-metadata coverage

Status: Pending
Depends on: D05, D11
Acceptance criteria:

- The test proves bootstrap writes the expected seal metadata needed by runtime or operator validation.
- The test asserts seal metadata contents are coherent with the installed bootstrap assets.
- The test cleans up the mounted guest-root fixture after completion.

### E04. Add hermetic VM persona clone-and-select coverage

Status: Pending
Depends on: D04
Acceptance criteria:

- The test proves a per-account VM persona package is cloned or selected from the configured base package.
- The test asserts the correct persona package is chosen for the intended account.
- The test remains isolated from real APFS cloning or real UTM state.

### E05. Add hermetic VM next happy-path coverage

Status: Pending
Depends on: A03, A04, D01, D02, D03
Acceptance criteria:

- The test covers a successful `next` rotation in VM mode through fake UTM and fake guest bridge.
- The test proves shared handoff format and staged commit behavior match the intended VM path.
- The test asserts cleanup before success.

### E06. Add hermetic VM prev happy-path coverage

Status: Pending
Depends on: A03, A04, D01, D02, D03
Acceptance criteria:

- The test covers a successful `prev` rotation in VM mode after an initial forward switch.
- The test proves the previous VM persona becomes active again without corrupting pool state.
- The test asserts cleanup before success.

### E07. Add hermetic VM relogin happy-path coverage

Status: Pending
Depends on: A03, D01, D02, D03
Acceptance criteria:

- The test covers a successful VM relogin using guest-side orchestration and host-side state update.
- The test proves relogin results are applied to the intended target account.
- The test asserts cleanup before success.

### E08. Add hermetic VM same-account reopen coverage

Status: Pending
Depends on: A03, D01, D02, D03
Acceptance criteria:

- The test proves reopening the same VM account persona does not require synthetic cross-account recovery.
- The test verifies same-account return uses persistent persona state already present in that VM path.
- The test asserts cleanup before success.

### E09. Add hermetic VM active-thread handoff coverage

Status: Pending
Depends on: A03, D01, D02, D03
Acceptance criteria:

- The test proves active-thread continuity in VM mode uses the same shared handoff contract as host mode.
- The test verifies guest-bridge import receives the expected minimal active-thread payload.
- The test asserts cleanup before success.

### E10. Add hermetic VM recoverable-thread handoff coverage

Status: Pending
Depends on: A03, D01, D02, D03
Acceptance criteria:

- The test proves recoverable-thread continuity in VM mode uses the same shared handoff contract as host mode.
- The test verifies guest-bridge import receives the expected recoverable payload only.
- The test asserts cleanup before success.

### E11. Add hermetic one-active-VM invariant coverage

Status: Pending
Depends on: D08
Acceptance criteria:

- The test proves VM rotation never leaves two targetable personas active at the same time.
- The test covers at least one sequence where a new target becomes active after the previous source is deactivated.
- The test asserts cleanup before success.

### E12. Add hermetic VM rollback coverage for boot failure

Status: Pending
Depends on: D09
Acceptance criteria:

- The test injects a VM boot failure before the target becomes ready.
- The test proves source account and source environment remain or are restored as authoritative.
- The test asserts cleanup before success.

### E13. Add hermetic VM rollback coverage for bridge-connect timeout

Status: Pending
Depends on: D09
Acceptance criteria:

- The test injects a guest-bridge connection timeout after VM activation has started.
- The test proves the failed target VM does not remain the committed active environment.
- The test asserts cleanup before success.

### E14. Add hermetic VM rollback coverage for handoff export failure

Status: Pending
Depends on: D09
Acceptance criteria:

- The test injects a failure while exporting handoff state before or during VM rotation.
- The test proves no partial target activation is committed.
- The test asserts cleanup before success.

### E15. Add hermetic VM rollback coverage for handoff import failure

Status: Pending
Depends on: D09
Acceptance criteria:

- The test injects a failure while importing handoff state through the guest bridge.
- The test proves source state is restored after the failed target import.
- The test asserts cleanup before success.

### E16. Add hermetic VM rollback coverage for relogin failure

Status: Pending
Depends on: D09
Acceptance criteria:

- The test injects a relogin failure in the VM path.
- The test proves account activation, guest state, and host state do not end partially switched.
- The test asserts cleanup before success.

### E17. Add hermetic VM rollback coverage for shutdown failure

Status: Pending
Depends on: D09
Acceptance criteria:

- The test injects a failure while shutting down a no-longer-needed VM during or after a switch.
- The test proves the failure is surfaced without corrupting committed active-account state.
- The test asserts cleanup before success.

### E18. Add hermetic VM region-and-egress validation failure coverage

Status: Pending
Depends on: D09
Acceptance criteria:

- The test injects a region or egress mismatch under validation mode.
- The test proves the mismatch fails loudly and does not silently degrade to success.
- The test asserts cleanup before success.

### E19. Add hermetic VM zero-leaked-resource coverage

Status: Pending
Depends on: D07
Acceptance criteria:

- The VM hermetic suite fails if VM-related processes, bridge daemons, temp persona packages, mounts, sockets, or bridge artifacts remain after execution.
- Leak assertions run after both success and failure paths.
- Cleanup failure output identifies the concrete leaked resource for diagnosis.

## Track F: VM Live Acceptance Coverage

### F01. Add real-UTM prerequisite verification task

Status: Blocked
Depends on: A13
Acceptance criteria:

- The task remains blocked until `UTM.app`, `utmctl`, a sealed base package, a bridge root, APFS persona storage, and staging credentials are available.
- The prerequisite check enumerates every missing live VM dependency explicitly.
- The task does not degrade to hermetic execution when live prerequisites are absent.

### F02. Add sealed-base-package validation task

Status: Blocked
Depends on: F01
Acceptance criteria:

- The task remains blocked until a real sealed base VM package exists and passes bootstrap validation.
- The live validation must prove required guest assets and seal metadata are present in the base package.
- The task does not claim VM live readiness based on mock or fixture data.

### F03. Add real guest-bridge auto-start smoke coverage

Status: Blocked
Depends on: F02
Acceptance criteria:

- The task remains blocked until real UTM prerequisites are available.
- The live smoke test must prove the guest bridge auto-starts inside the booted guest without manual operator repair.
- The test must close or stop test-managed VM resources before success.

### F04. Add real VM boot smoke coverage

Status: Blocked
Depends on: F02
Acceptance criteria:

- The task remains blocked until real UTM prerequisites are available.
- The live smoke test must boot the target VM, reach the guest-side control path, and verify readiness.
- The test must close or stop test-managed VM resources before success.

### F05. Add real VM next acceptance across two staging accounts

Status: Blocked
Depends on: F04
Acceptance criteria:

- The task remains blocked until real UTM prerequisites are available.
- The live test must perform a real VM `next` rotation across two staging accounts.
- The test must close or stop test-managed VM resources before success.

### F06. Add real VM prev acceptance across two staging accounts

Status: Blocked
Depends on: F04
Acceptance criteria:

- The task remains blocked until real UTM prerequisites are available.
- The live test must perform a real VM `prev` rotation after at least one forward VM switch.
- The test must close or stop test-managed VM resources before success.

### F07. Add real VM relogin acceptance

Status: Blocked
Depends on: F04
Acceptance criteria:

- The task remains blocked until real UTM prerequisites are available.
- The live test must perform a real guest-side relogin and verify resulting host-side auth/state update.
- The test must close or stop test-managed VM resources before success.

### F08. Add real VM same-account reopen acceptance

Status: Blocked
Depends on: F04
Acceptance criteria:

- The task remains blocked until real UTM prerequisites are available.
- The live test must prove returning to the same VM persona reuses persistent persona state correctly.
- The test must close or stop test-managed VM resources before success.

### F09. Add real VM active-thread continuity acceptance

Status: Blocked
Depends on: F05
Acceptance criteria:

- The task remains blocked until real UTM prerequisites are available.
- The live test must prove active-thread continuity works across a real VM rotation.
- The test must close or stop test-managed VM resources before success.

### F10. Add real VM recoverable-thread continuity acceptance

Status: Blocked
Depends on: F05
Acceptance criteria:

- The task remains blocked until real UTM prerequisites are available.
- The live test must prove recoverable-thread continuity works across a real VM rotation.
- The test must close or stop test-managed VM resources before success.

### F11. Add real one-active-VM invariant acceptance

Status: Blocked
Depends on: F05
Acceptance criteria:

- The task remains blocked until real UTM prerequisites are available.
- The live test must prove only one VM remains active or targeted at a time during the tested rotation sequence.
- The test must close or stop test-managed VM resources before success.

### F12. Add real VM region-and-egress validation acceptance

Status: Blocked
Depends on: F04
Acceptance criteria:

- The task remains blocked until real UTM prerequisites are available.
- The live test must prove region-sensitive persona settings are validated against actual egress assumptions when validation mode is enabled.
- The test must close or stop test-managed VM resources before success.

### F13. Add real VM rollback acceptance for induced bridge failure

Status: Blocked
Depends on: F05
Acceptance criteria:

- The task remains blocked until real UTM prerequisites are available.
- The live test must induce a guest-bridge failure during a VM switch and verify source restoration.
- The test must close or stop test-managed VM resources before success.

### F14. Add real VM rollback acceptance for induced relogin failure

Status: Blocked
Depends on: F07
Acceptance criteria:

- The task remains blocked until real UTM prerequisites are available.
- The live test must induce a guest-side relogin failure and verify the system is not left partially switched.
- The test must close or stop test-managed VM resources before success.

### F15. Add real VM rollback acceptance for induced shutdown failure

Status: Blocked
Depends on: F05
Acceptance criteria:

- The task remains blocked until real UTM prerequisites are available.
- The live test must induce a shutdown failure and verify committed account state remains coherent.
- The test must close or stop test-managed VM resources before success.

### F16. Add real VM zero-residual-window-process-and-package coverage

Status: Blocked
Depends on: F04
Acceptance criteria:

- The task remains blocked until real UTM prerequisites are available.
- The live suite must fail if VM windows, VM processes, guest bridges, temp packages, mounts, or helper processes remain after execution.
- Cleanup assertions must run after both success and failure paths.

## Track G: CI Matrix, Artifacts, And Confidence Policy

### G01. Define the suite matrix for PR-safe versus live-gated coverage

Status: Pending
Depends on: A14
Acceptance criteria:

- The confidence model explicitly states which suites are required on PRs and which suites are live-gated.
- The matrix distinguishes host and VM requirements rather than using one combined status.
- The matrix can be referenced when deciding whether a backend is partially or fully verified.

### G02. Define the rule for claiming host mode “done and verified”

Status: Pending
Depends on: B21, C14
Acceptance criteria:

- The rule requires host hermetic coverage and host live acceptance, not one without the other.
- The rule states that cleanup, leak checks, and no-system-browser guarantees are part of verification, not optional extras.
- The rule makes it unambiguous when host mode can be claimed release-ready.

### G03. Define the rule for claiming VM mode “done and verified”

Status: Pending
Depends on: E19, F16
Acceptance criteria:

- The rule requires VM hermetic coverage and VM live acceptance once live prerequisites exist.
- The rule states that blocked live prerequisites prevent a full VM verification claim.
- The rule makes it unambiguous when VM mode can be claimed release-ready.

### G04. Define artifact-retention rules for failing hermetic suites

Status: Pending
Depends on: A10
Acceptance criteria:

- The rule defines which logs, transcripts, screenshots, and process maps must be retained on hermetic failures.
- The rule defines where retained artifacts live and how they are keyed per test case.
- The rule avoids retaining irrelevant passing-run artifacts by default.

### G05. Define artifact-retention rules for failing live suites

Status: Pending
Depends on: A10, D12
Acceptance criteria:

- The rule defines which Codex, browser, guest-bridge, UTM, and runtime artifacts must be retained on live failures.
- The rule defines how live artifact capture avoids leaking staging secrets into casual output.
- The rule makes retained artifacts sufficient for remote debugging without rerunning the scenario immediately.

### G06. Define a no-silent-retry flake policy for leak and cleanup failures

Status: Pending
Depends on: A08, A09
Acceptance criteria:

- The policy states that leak and cleanup failures are hard failures rather than silently retried noise.
- The policy distinguishes allowed reruns for infrastructure flake from disallowed reruns for cleanup regressions.
- The policy requires explicit tracking for any quarantined live or hermetic scenario.

### G07. Define staging-account and secret-handling rules for live suites

Status: Pending
Depends on: A12, A13
Acceptance criteria:

- The rule defines how staging credentials are provided to host and VM live suites.
- The rule forbids running live suites against personal or production accounts.
- The rule defines how logs and artifacts must redact or avoid exposing secrets.

### G08. Add an operator runbook task for live host execution

Status: Pending
Depends on: A12
Acceptance criteria:

- The task produces a runbook describing host live prerequisites, env vars, invocation, cleanup expectations, and failure triage.
- The runbook defines how to verify isolation before and after a live host run.
- The runbook can be followed without reverse-engineering test code.

### G09. Add an operator runbook task for live VM execution

Status: Pending
Depends on: A13
Acceptance criteria:

- The task produces a runbook describing VM live prerequisites, env vars, sealed base expectations, UTM requirements, and cleanup expectations.
- The runbook defines how to verify VM isolation before and after a live VM run.
- The runbook can be followed without reverse-engineering test code.

### G10. Map existing E2E/integration tests to the new backlog so overlap and remaining gaps are explicit

Status: Pending
Depends on: A01
Acceptance criteria:

- The mapping identifies which current tests satisfy or partially satisfy which `tasks_e2e.md` items.
- The mapping identifies remaining gaps without double-counting current tests as full-system confidence.
- The mapping makes future backlog pruning possible once new tasks are implemented.
