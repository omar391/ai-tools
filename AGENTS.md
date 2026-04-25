# AGENTS.md

This file has one managed section per active spec, followed by one local section:

- `rules:spec:*` are synced from the `agent-md` skill's specs collection and must be updated there rather than directly in this file.
- `rules:local` is reserved for repo-specific rules and is preserved by the sync script.

<!-- prettier-ignore-start -->
<!-- markdownlint-disable MD025 -->
<!-- BEGIN rules:spec:common -->
# Shared Rules

- Start from the repo's primary branch. In worktree-isolated repos, edit only in a dedicated repo-local task worktree under `<repo>/worktrees/<name>/` unless already in the right one.
- If a runtime skill defines worktree naming or reconcile flow, follow it within this isolation policy.
- If the current worktree already holds one coherent committable change, land it before unrelated edits.
- After verified landing, remove temporary worktrees/branches already represented on `main`.
- Use per-worktree tool envs (`bin/`, `.venv/`, `.codex-rotate/bin/`); avoid mutable shared tool environments.
- Run relevant tests/builds/checks before landing.
- Put repo-specific constraints in `rules:local`, not shared specs.

## Coding Baseline

- Keep edits scoped and follow repo idioms.
- Prefer TDD for behavior changes; apply SOLID only when it reduces churn.
- Optimize for agentic locality: prefer cohesive production files ~150-500 lines; treat 700+ as a smell and 1,000+ as a split candidate unless generated, declarative, or inherently cohesive.
- Split by semantic boundary, for example types, I/O, validation, domain logic, UI state/view, CLI parsing/execution, test helpers, or test scenarios.
- Avoid splits where the pieces must always be read or changed together.
<!-- END rules:spec:common -->

<!-- BEGIN rules:local -->
<!-- END rules:local -->
<!-- markdownlint-enable MD025 -->
<!-- prettier-ignore-end -->
