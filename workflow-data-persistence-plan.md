# Workflow Data Persistence Plan

Status: Proposed

## Purpose

This document defines the R&D direction and shared architecture for workflow data persistence adapters in this repository.

This plan is about workflow-produced data only. It is not about engine control state such as paused sessions, diagnostics, semantic memory, or profile binding.

The core recommendation is:

- `SQLite` as the default primary workflow-data store
- `JSON` as a lightweight local adapter for simple or low-concurrency use
- `Google Sheets` as a projection and reporting adapter, not a source of truth

## What This Covers

This plan covers persistence for workflow data such as:

- records created by workflows
- normalized entities extracted from pages or documents
- append-only workflow outputs that later need lookup or reconciliation
- tabular exports and human-facing projections

Examples:

- a workflow writes extracted leads to a durable collection
- a workflow upserts a product catalog snapshot
- a workflow appends status records for later reconciliation
- a workflow projects selected records into a spreadsheet for review

## What This Does Not Cover

This plan does not change or absorb existing fast-browser control-state persistence surfaces.

Those remain separate concerns, including:

- workspace-local profile binding
- semantic memory
- diagnostics and run artifacts
- paused-session persistence
- daemon lifecycle state

Those surfaces are currently JSON-backed and local to fast-browser runtime behavior. They are not the persistence model this plan is standardizing.

## Current State

The current repo is file-first.

In fast-browser today:

- workflows are YAML files in global or local stores
- project runtime cache lives under `.fast-browser/cache/`
- diagnostics are written as JSON
- semantic memory is written as JSON
- local paused sessions are written as JSON
- CLI and daemon responses are JSON

This means two things:

1. the repo already has working local-file persistence patterns
2. the current `global|local` workflow "store" concept is about workflow definition location, not workflow data persistence

The new workflow-data persistence surface MUST therefore be introduced as a separate concept, not as an extension of workflow-definition store routing.

## Design Goals

- Robust local persistence without introducing a server dependency in v1
- One canonical workflow-data model across adapters
- Clear separation between primary persistence and human-facing projection
- Strong idempotency and replay safety
- Compatibility with both structured JSON records and tabular reporting needs
- Easy local development and inspection
- Reasonable alignment with industry-standard durability and API practices

## Non-Goals

- Replacing fast-browser run-state persistence in this pass
- Treating Google Sheets as an operational database
- Designing a distributed database layer
- Solving full schema migration governance across arbitrary external stores
- Multi-writer robustness on plain JSON files

## Architectural Recommendation

Use a two-layer model:

1. primary workflow-data store
2. secondary projection adapters

### Primary store

The primary store owns:

- canonical record durability
- read and query semantics
- idempotent upsert behavior
- conflict and replay handling
- internal metadata such as timestamps and source identity

Supported v1 primary stores:

- `sqlite`
- `json`

### Projection store

Projection stores own:

- human-facing exports
- reporting views
- review surfaces
- low-friction collaboration outputs

Supported v1 projection store:

- `google-sheets`

Google Sheets SHOULD NOT be used as the canonical write path in v1.

## Why SQLite Is the Default

`SQLite` is the recommended default primary store because it provides robust local durability without requiring a separate database server.

This plan specifically recommends:

- transactional batch writes
- WAL mode
- explicit indexes
- idempotent upsert semantics

References:

- [SQLite Documentation](https://sqlite.org/docs.html)
- [SQLite Write-Ahead Logging](https://www.sqlite.org/wal.html)

The practical reasoning is straightforward:

- it is much more robust than plain files for repeated appends and upserts
- it supports concurrent readers while a writer is active
- it provides transactions and crash recovery properties expected of a serious local store
- it avoids network dependencies for local workflow execution

## Why JSON Still Matters

`JSON` remains useful as a lightweight adapter when:

- the workflow is single-writer
- the dataset is small
- human readability matters more than concurrency
- local portability matters more than write throughput

`JSON` is not the robust default. It is the simple fallback.

The JSON adapter MUST make its limitations explicit:

- single-writer assumption
- no claim of robust multi-process concurrency
- atomic replace writes only

## Why Google Sheets Is Projection-Only

Google Sheets is useful as a review and reporting surface, but it is a poor primary source of truth for robust workflow persistence.

Reasons:

- request quotas
- shared-service limits
- timeout and request-complexity constraints
- weaker guarantees for application-style read/modify/write workflows
- spreadsheet complexity and operational drift as data volume grows

Google’s own guidance reinforces this:

- quotas are enforced per minute
- Google recommends keeping request payloads modest
- retry should use exponential backoff
- high-concurrency and high-complexity usage should be limited
- related updates should be batched carefully

References:

- [Google Sheets API usage limits](https://developers.google.com/workspace/sheets/api/limits)
- [Google Sheets API troubleshoot errors](https://developers.google.com/workspace/sheets/api/troubleshoot-api-errors)

This makes Sheets well-suited for projection, not canonical persistence.

## Canonical Workflow Data Model

All primary adapters MUST use the same logical record model.

The canonical envelope is:

```ts
type WorkflowRecordEnvelope = {
  namespace: string;
  collection: string;
  record_id: string;
  schema_version: string;
  payload: Record<string, unknown>;
  source: {
    workflow_ref?: string | null;
    run_id?: string | null;
    actor?: string | null;
    origin?: string | null;
  };
  created_at: string;
  updated_at: string;
  idempotency_key?: string | null;
};
```

### Field meaning

- `namespace`: logical project or domain boundary
- `collection`: logical dataset name, such as `leads`, `products`, `tickets`, or `page_extracts`
- `record_id`: stable application-level identifier
- `schema_version`: payload schema version for compatibility and migration
- `payload`: domain-specific record body
- `source`: optional write provenance
- `created_at`: first durable insert timestamp
- `updated_at`: last durable mutation timestamp
- `idempotency_key`: optional replay protection key for append and upsert flows

### Model rules

- `payload` is canonical JSON data
- adapters MUST NOT define alternative canonical models
- adapters MAY derive secondary forms from the envelope, such as flattened rows for Sheets
- `record_id` MUST be stable for upsertable entities
- `idempotency_key` SHOULD be used for replay-prone workflow writes

## Adapter Contracts

This plan defines two different contracts:

1. primary workflow-data store adapters
2. projection adapters

### Primary store contract

```ts
interface WorkflowDataStoreAdapter {
  ensureDataset(input: {
    namespace: string;
    collection: string;
    schema_version: string;
  }): Promise<void>;

  appendRecords(input: {
    namespace: string;
    collection: string;
    records: WorkflowRecordEnvelope[];
  }): Promise<{
    written: number;
    deduplicated: number;
  }>;

  upsertRecords(input: {
    namespace: string;
    collection: string;
    records: WorkflowRecordEnvelope[];
  }): Promise<{
    inserted: number;
    updated: number;
    deduplicated: number;
  }>;

  getRecord(input: {
    namespace: string;
    collection: string;
    record_id: string;
  }): Promise<WorkflowRecordEnvelope | null>;

  queryRecords(input: {
    namespace: string;
    collection: string;
    where?: Record<string, unknown>;
    limit?: number;
    cursor?: string | null;
    order_by?: "created_at" | "updated_at" | "record_id";
    order_direction?: "asc" | "desc";
  }): Promise<{
    records: WorkflowRecordEnvelope[];
    next_cursor?: string | null;
  }>;

  capabilities(): {
    transactions: boolean;
    concurrent_readers: boolean;
    concurrent_writers: boolean;
    idempotent_append: boolean;
    idempotent_upsert: boolean;
    secondary_indexes: boolean;
  };
}
```

### Projection contract

```ts
interface WorkflowProjectionAdapter {
  projectRecords(input: {
    namespace: string;
    collection: string;
    records: WorkflowRecordEnvelope[];
    projection: ProjectionSpec;
  }): Promise<{
    projected: number;
    skipped: number;
  }>;

  reconcileProjection(input: {
    namespace: string;
    collection: string;
    projection: ProjectionSpec;
  }): Promise<{
    scanned: number;
    projected: number;
    removed?: number;
  }>;

  capabilities(): {
    batch_write: boolean;
    schema_headers: boolean;
    row_update: boolean;
    row_delete: boolean;
  };
}
```

## Adapter Responsibilities

### SQLite adapter

The SQLite adapter SHOULD be the reference implementation.

It MUST:

- enable WAL mode
- write in transactions
- support batched append and batched upsert
- create indexes on at least:
  - `collection`
  - `record_id`
  - `updated_at`
  - `idempotency_key`
- preserve `created_at` on updates
- update `updated_at` on mutation
- enforce deterministic upsert semantics by `namespace + collection + record_id`

Recommended table shape:

```sql
CREATE TABLE workflow_records (
  namespace TEXT NOT NULL,
  collection TEXT NOT NULL,
  record_id TEXT NOT NULL,
  schema_version TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  source_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  idempotency_key TEXT,
  PRIMARY KEY (namespace, collection, record_id)
);
```

Recommended indexes:

```sql
CREATE INDEX idx_workflow_records_collection
  ON workflow_records(namespace, collection);

CREATE INDEX idx_workflow_records_updated_at
  ON workflow_records(namespace, collection, updated_at);

CREATE INDEX idx_workflow_records_idempotency
  ON workflow_records(namespace, collection, idempotency_key);
```

The exact SQL can vary at implementation time, but the behavior above is not optional.

### JSON adapter

The JSON adapter MUST optimize for simplicity and inspectability, not concurrency.

It MUST:

- write valid JSON envelopes only
- use atomic replace writes
- preserve stable ordering where practical
- support the same logical append and upsert contract as SQLite
- document that concurrent multi-process writes are not robustly supported

Recommended layout:

- one dataset file per `namespace + collection`
- newline-delimited JSON is allowed if the implementation explicitly standardizes it
- otherwise a deterministic object or array form is acceptable

The plan SHOULD prefer one JSON file per dataset rather than scattering records across many tiny files unless the workload clearly benefits from object-per-file storage.

### Google Sheets projection adapter

The Sheets adapter MUST be projection-only in v1.

It MUST:

- accept canonical record envelopes as input
- flatten selected fields into stable columns
- use one spreadsheet tab per dataset or projection
- batch related writes
- use conservative request pacing per spreadsheet
- retry quota and transient errors with truncated exponential backoff
- make projection column mapping explicit and deterministic

It MUST NOT claim:

- canonical-source semantics
- robust transactional read/modify/write
- robust high-frequency multi-writer coordination

Recommended projection columns:

- `namespace`
- `collection`
- `record_id`
- `schema_version`
- `created_at`
- `updated_at`
- selected flattened payload fields

The adapter SHOULD keep a stable header row and SHOULD fail loudly on schema drift unless a projection migration step is explicitly invoked.

## Query and Idempotency Semantics

### Append

`appendRecords` is for write-once or append-mostly flows.

If an `idempotency_key` is present, the adapter SHOULD deduplicate replayed writes for the same dataset.

### Upsert

`upsertRecords` is for entity-like data where `record_id` is stable.

The adapter MUST:

- insert when a record does not exist
- update when a record exists
- preserve original `created_at`
- replace or merge the canonical `payload` deterministically

This plan assumes full-record replacement by default. Partial patch semantics MAY be added later, but they are not part of the v1 contract.

### Query

`queryRecords` in v1 is intentionally limited.

It is not trying to be a general analytics query language. It is a practical retrieval API for workflow follow-up steps and projection pipelines.

SQLite MAY implement richer filtering internally. The shared contract SHOULD stay conservative until there is evidence for a broader common need.

## Data Shape and Projection Rules

The canonical model is record-oriented JSON.

That means:

- SQLite stores canonical JSON payloads durably
- JSON stores canonical JSON payloads directly
- Sheets receives a flattened projection of selected fields

Sheets MUST NOT define the canonical model.

Projection flattening MUST be explicit. A projection spec SHOULD define:

- target spreadsheet
- target tab or dataset sheet
- ordered column list
- field extraction paths
- null handling
- formatting hints if needed

Illustrative example:

```ts
type ProjectionSpec = {
  spreadsheet_id: string;
  sheet_name: string;
  columns: Array<{
    header: string;
    path: string;
  }>;
};
```

Paths MAY use JSON Pointer for consistency with the secret-ref design work already documented in this repo.

## Separation of Concerns

This plan intentionally separates three persistence domains:

### 1. Workflow data persistence

Canonical records, queryable entities, append/upsert datasets.

This is the subject of this plan.

### 2. Workflow run and control persistence

Paused sessions, diagnostics, semantic memory, replay state, debug artifacts.

This is not being redesigned here.

### 3. Reporting and export projection

Human-facing Sheets or similar downstream views built from canonical records.

This is part of this plan, but only as a secondary projection surface.

## Industry Alignment

### SQLite

This plan follows SQLite’s strengths as an embedded transactional database. WAL mode and transactional updates are industry-standard ways to improve local write durability and concurrency behavior for application-owned files.

References:

- [SQLite Documentation](https://sqlite.org/docs.html)
- [Write-Ahead Logging](https://www.sqlite.org/wal.html)

### Google Sheets

This plan follows Google’s own guidance by treating Sheets as a quota-limited shared API that benefits from batching, backoff, and conservative concurrency.

References:

- [Google Sheets API usage limits](https://developers.google.com/workspace/sheets/api/limits)
- [Google Sheets API troubleshoot API errors](https://developers.google.com/workspace/sheets/api/troubleshoot-api-errors)

The architecture therefore treats Sheets as a projection target, not as an operational primary database.

## Example Flows

### Example 1: SQLite primary write

1. A workflow extracts `lead` records from a page.
2. The runtime builds `WorkflowRecordEnvelope` objects.
3. The SQLite adapter `upsertRecords` writes them transactionally.
4. A later workflow queries the same dataset by `record_id` or `updated_at`.

### Example 2: JSON local dataset

1. A lightweight local workflow creates a small `page_extracts` dataset.
2. The JSON adapter writes one canonical dataset file atomically.
3. A later local step reads or queries the same dataset.
4. The system assumes single-writer discipline.

### Example 3: Google Sheets projection

1. Canonical records already exist in SQLite.
2. A projection step flattens selected fields into tabular rows.
3. The Sheets adapter writes batched updates to a dedicated tab.
4. Humans review or annotate the sheet, but the canonical source remains SQLite.

## Test Plan

The R&D and follow-up implementation work SHOULD validate the architecture with the following tests.

### Shared contract tests

Run the same fixture suite against both JSON and SQLite:

- `ensureDataset`
- `appendRecords`
- `upsertRecords`
- `getRecord`
- `queryRecords`
- idempotent replay handling

### SQLite robustness tests

- crash and restart after committed writes
- duplicate write replay using `idempotency_key`
- concurrent readers while a writer is active
- batched transactional upsert behavior
- index-backed retrieval for recent updates

### JSON safety tests

- atomic replace writes
- malformed-file recovery or explicit failure
- stable load/store shape
- explicit warning or rejection for unsupported concurrent-writer use

### Google Sheets projection tests

- stable column mapping from envelopes to rows
- batch projection of multiple records
- retry and backoff behavior for quota-related failures
- reconciliation after partial sync or interrupted projection
- explicit handling of schema drift in projection columns

### Boundary tests

Verify that the new workflow-data adapter model does not change or absorb:

- profile binding persistence
- semantic memory persistence
- paused-session persistence
- diagnostics and run artifact persistence

## Migration Strategy

### Phase 1: Publish the R&D spec

- Add this document
- Lock the conceptual split between workflow data and workflow control state
- Standardize the canonical record envelope and adapter contracts

### Phase 2: Build the reference store adapters

- implement `sqlite`
- implement `json`
- run the same contract tests against both

### Phase 3: Add projection support

- implement `google-sheets` as a projection adapter
- define projection specs and flattening rules
- add backoff and pacing behavior

### Phase 4: Integrate with workflow execution surfaces

- add an explicit workflow-data persistence surface to runtimes or workflow actions
- keep it separate from workflow-definition store resolution
- ensure projection flows read from canonical primary datasets

### Phase 5: Operationalize and tighten

- document recommended default selection
- default new robust workflow-data use cases to SQLite
- reserve JSON for simple local cases and Sheets for projections

## Decision Summary

The decisions in this plan are intentionally opinionated:

- `SQLite` is the default primary store in v1
- `JSON` is a lightweight fallback, not the robust default
- `Google Sheets` is projection-only in v1
- the canonical data model is record-oriented JSON
- workflow-definition store routing and workflow-data persistence are separate concepts
- fast-browser run-state persistence remains out of scope for this pass

These defaults are chosen because they are the simplest path to robust local persistence while preserving a clean adapter story and avoiding misuse of spreadsheets as operational databases.
