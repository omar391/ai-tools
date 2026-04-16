# AGENTS.md

This file has two managed sections:

- `rules:shared` is synced from the `agent-md` active specs: `common`.
- `rules:local` is reserved for repo-specific rules and is preserved by the sync script.

<!-- prettier-ignore-start -->
<!-- markdownlint-disable MD025 -->
<!-- BEGIN rules:shared -->
# Shared Rules

- Start each new task from the repository's primary branch.
- Unless already inside an appropriate task worktree, any task that will perform file edits must first create a dedicated repo-local task worktree under `<repo-root>/worktrees/<name>/` from the repository's primary branch when the repository uses worktree-based task isolation.
- Keep mutable task work in the task worktree rather than in the primary checkout.
- After landing and verifying a task, delete any temporary repo-local worktrees and branches created during the current conversation whose contents are already represented on `main`.
- Use an isolated `bin/`, virtual environment, or equivalent tool environment per active worktree when the repository depends on local tooling.
- Keep those per-worktree tool artifacts rooted inside the worktree or repo-local task directory, for example `<repo-root>/bin/`, `<repo-root>/.venv/`, or `<repo-root>/.codex-rotate/bin/`, rather than in shared home-level directories.
- Do not run mutable tooling from a live shared environment when a repo-local isolated environment is expected.
- Validate relevant tests, builds, and checks before landing completed changes.
- Keep repo-specific constraints in the `rules:local` block instead of editing the shared baseline.
<!-- END rules:shared -->

<!-- BEGIN rules:local -->
<!-- END rules:local -->
<!-- markdownlint-enable MD025 -->
<!-- prettier-ignore-end -->
