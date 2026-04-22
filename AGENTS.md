# AGENTS.md

This file has one managed section per active spec, followed by one local section:

- `rules:spec:*` are synced from the `agent-md` skill's specs collection and must be updated there rather than directly in this file.
- `rules:local` is reserved for repo-specific rules and is preserved by the sync script.

<!-- prettier-ignore-start -->
<!-- markdownlint-disable MD025 -->
<!-- BEGIN rules:spec:common -->
# Shared Rules

- Start each new task from the repository's primary branch.
- Unless already inside an appropriate task worktree, any task that will perform file edits must first create a dedicated repo-local task worktree under `<repo-root>/worktrees/<name>/` from the repository's primary branch when the repository uses worktree-based task isolation.
- Keep mutable task work in the task worktree rather than in the primary checkout.
- If a runtime skill defines coordinator/child worktree naming or reconcile semantics, treat that runtime contract as authoritative for naming and merge flow while keeping this baseline isolation policy.
- When the model determines the current worktree already contains one valid coherent change set that should be committed, auto-land that change before starting further unrelated code edits.
- After landing and verifying a task, delete any temporary repo-local worktrees and branches created during the current conversation whose contents are already represented on `main`.
- Use an isolated `bin/`, virtual environment, or equivalent tool environment per active worktree when the repository depends on local tooling.
- Keep those per-worktree tool artifacts rooted inside the worktree root or repo-local task directory, for example `<worktree-root>/bin/`, `<worktree-root>/.venv/`, or `<worktree-root>/.codex-rotate/bin/`, rather than in shared home-level directories.
- Do not run mutable tooling from a live shared environment when a repo-local isolated environment is expected.
- Validate relevant tests, builds, and checks before landing completed changes.
- Keep repo-specific constraints in the `rules:local` block instead of editing the shared baseline.

## Coding Baseline

- Keep edits within explicit task scope files when they are provided.
- Follow repository idioms and existing conventions.
- Prefer TDD-first red -> green -> refactor for behavior changes.
- Apply SOLID pragmatically to reduce future churn, not for abstraction theater.
<!-- END rules:spec:common -->

<!-- BEGIN rules:local -->
<!-- END rules:local -->
<!-- markdownlint-enable MD025 -->
<!-- prettier-ignore-end -->
