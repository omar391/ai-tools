<!-- markdownlint-disable MD025 -->
<!-- BEGIN rules:spec:common -->

# Shared Rules

## Worktree Isolation (Mandatory)

- **Never edit, stage, or commit directly on the primary branch (`main`).** The primary branch is read-only for all agents; it receives changes only through landing merges (fast-forward or merge commit from a validated branch).
- All code changes must happen in a dedicated task worktree under `<repo>/worktrees/<name>/` (or `<repo>.worktrees/<name>/` for external worktrees). If you are not already in the correct worktree, create or switch to one before making any edits.
- If a runtime skill (e.g. `loopo`) defines worktree naming, branch naming, or reconcile flow, follow its conventions within this isolation policy.
- One worktree = one coherent task. If the current worktree already holds one coherent committable change, land it before starting unrelated edits.
- After verified landing, remove temporary worktrees and branches already represented on the primary branch.
- Use per-worktree tool envs (`bin/`, `.venv/`, `.codex-rotate/bin/`); avoid mutable shared tool environments.
- Run relevant tests/builds/checks before landing.

<!-- END rules:spec:common -->
<!-- BEGIN rules:spec:coding -->

# Coding Baseline

- **Minimal diff**: aim for the smallest code change that satisfies the goal; avoid unnecessary refactors, reformats, or scope creep in the same changeset.
- Keep edits scoped and follow repo idioms.
- Prefer TDD/BDD: write or update tests before (or alongside) the implementation for behavior changes. Apply SOLID only when it reduces churn.
- **Integration tests must use isolated live environments** (sandboxed databases, test accounts, ephemeral services). Never run integration tests against a production runtime or data store.
- Optimize for agentic locality: prefer cohesive production files ~300-500 lines; treat 500+ as a smell and 1,000+ as a split candidate in multi-file modules unless generated, declarative, or inherently cohesive.
- Split by semantic boundary, for example types, I/O, validation, domain logic, UI state/view, CLI parsing/execution, test helpers, or test scenarios.
- Avoid splits where the pieces must always be read or changed together.

<!-- END rules:spec:coding -->
<!-- BEGIN rules:local -->
<!-- END rules:local -->

Load on-demand specs: [`code-review`](../ai-rules/skills/loopo/assets/specs/code-review.md), [`ts`](../ai-rules/skills/loopo/assets/specs/ts.md)
