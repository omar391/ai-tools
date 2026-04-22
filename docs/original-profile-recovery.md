# Original Single-Profile Recovery Runbook

## Overview

This runbook describes how to migrate conversation history from a legacy single-profile setup to a unified lineage-based persona sync model.

## Prerequisites

1. Multiple accounts added to the pool via `codex-rotate add`.
2. The legacy history is located in the _currently active_ persona (the one used during the first rotation).

## Procedure

### 1. Dry Run

Report discovered conversations and planned bindings without making changes:

```bash
codex-rotate repair-host-history
```

Verify the output lists the expected number of conversations and planned updates for target accounts.

### 2. Apply Recovery

Execute the migration via app APIs:

```bash
codex-rotate repair-host-history --apply
```

This will:

- Iterate through all target accounts.
- Switch the host persona symlink to each target.
- Launch a managed Codex instance for each target.
- Import conversations from the source account via the app API.
- Persist lineage-to-local-ID bindings in `~/.codex-rotate/conversation_sync.sqlite`.

### 3. Verification

Run the report again to ensure all conversations are now bound:

```bash
codex-rotate report-duplicates
```

Check for "Bound threads" count.

## Post-Run Checklist

- [ ] Verify target-local IDs were created for each lineage.
- [ ] Confirm no logical duplicates appear on subsequent rotations.
- [ ] Historical duplicates (pre-existing) are reported but not deleted.
