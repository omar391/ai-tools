# AGENTS.md

This file has two managed sections:

- `rules:shared` is synced from the `agents-md-sync` `generic` template.
- `rules:local` is reserved for repo-specific rules and is preserved by the sync script.

<!-- prettier-ignore-start -->
<!-- markdownlint-disable MD025 -->
<!-- BEGIN rules:shared -->
# Shared Rules

- Start each new task from the repository's primary branch.
- Prefer a dedicated, repo-local task worktree under `<repo-root>/worktrees/<name>/` when the repository uses worktree-based task isolation.
- Keep mutable task work in the task worktree rather than in the primary checkout.
- Use an isolated `bin/`, virtual environment, or equivalent tool environment per active worktree when the repository depends on local tooling.
- Do not run mutable tooling from a live shared environment when a repo-local isolated environment is expected.
- Validate relevant tests, builds, and checks before landing completed changes.
- Keep repo-specific constraints in the `rules:local` block instead of editing the shared baseline for one repository.
<!-- END rules:shared -->

<!-- BEGIN rules:local -->
<!-- END rules:local -->
<!-- markdownlint-enable MD025 -->
<!-- prettier-ignore-end -->
