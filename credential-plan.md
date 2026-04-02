# Credential Plan

Status: Proposed

## Purpose

This document defines the shared secret reference architecture for this repository.

The goal is to make local `0600` files, Bitwarden CLI, LastPass CLI, HashiCorp Vault, and cloud secret managers interchangeable behind one runtime contract.

This document is normative. The key words `MUST`, `MUST NOT`, `SHOULD`, `SHOULD NOT`, and `MAY` are to be interpreted as described in RFC 2119.

## Problem Statement

Passing raw secret values through ordinary workflow inputs is an anti-pattern.

When a workflow accepts plaintext `password`, `token`, `api_key`, `client_secret`, or similar values as normal inputs, those secrets can leak across multiple seams:

- CLI arguments and shell history
- API request payloads
- daemon or worker IPC payloads
- runtime interpolation contexts
- logs and progress events
- screenshots, recordings, and DOM snapshots
- paused-session or resume payloads
- persisted semantic memory and run state
- error messages and diagnostics

The architecture MUST therefore treat secret values differently from normal user input.

Workflows MAY carry secret references. Workflows MUST NOT carry resolved secret values as durable configuration or ordinary string inputs.

## Goals

- Store neutrality across local files, CLI-backed vaults, and cloud secret managers
- Runtime-only secret resolution immediately before use
- No plaintext secrets in workflow definitions or durable artifacts
- One consistent authoring model across runtimes and stores
- Industry-aligned reference semantics
- A model that supports both scalar secrets and object-shaped secrets

## Non-Goals

- Browser-extension autofill
- Vendor-specific create, update, rotate, or delete operations in the core contract
- Full secret governance, approval, and rotation policy
- Eliminating the existence of secrets in process memory entirely

The architecture can reduce exposure and persistence. It cannot avoid the fact that a running process must hold a resolved value briefly in memory when it fills a form, signs a request, or authenticates to a remote system.

## Core Model

There is exactly one first-class primitive: `secret_ref`.

A secret MAY be:

- a scalar value such as an API key or password
- an object-shaped value such as `{ "email": "...", "password": "...", "totp": "..." }`

There is no separate `credential_ref` type.

A "credential" is a convention, not a primitive. It is simply an object-shaped secret whose fields match the needs of a login, form, or auth flow.

## Canonical `secret_ref`

The canonical `secret_ref` shape is:

```yaml
type: secret_ref
store: bitwarden-cli
object_id: team/openai/dev-account
field_path: /password
version: null
```

### Fields

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `type` | string | yes | MUST be `secret_ref` |
| `store` | string | yes | Adapter slug such as `local-file`, `bitwarden-cli`, `lastpass-cli`, `vault-kv`, `aws-secrets-manager`, `gcp-secret-manager`, or `azure-key-vault` |
| `object_id` | string | yes | Opaque adapter-owned identifier |
| `field_path` | string | no | JSON Pointer path for selecting a field from an object-shaped secret |
| `version` | string or null | no | Optional adapter-owned version selector |

### Rules

- `object_id` MUST be treated as opaque by workflow authors and runtime code outside the adapter.
- `object_id` MAY be a UUID, path, key, ARN, item id, or other adapter-native locator.
- `field_path` MUST use JSON Pointer syntax as defined by [RFC 6901](https://www.rfc-editor.org/rfc/rfc6901).
- If `field_path` is omitted, the runtime MUST resolve the whole scalar or object value.
- If `field_path` is present and the resolved secret is not object-addressable, the runtime MUST fail.
- `version` semantics are adapter-defined, but the field name is standardized so stores with explicit versioning can participate without changing workflow shape.

## Conventional Object Shapes

The architecture standardizes one primitive, but it also standardizes common object conventions so actions can validate expected fields.

### Recommended login/account fields

- `email`
- `username`
- `password`
- `totp`
- `phone_number`
- `client_id`
- `client_secret`

An action that accepts an object-shaped secret MUST declare which fields are required and which are optional.

An action MUST NOT assume that both `email` and `username` are present. It MUST declare the contract it expects.

## Unified Adapter Contract

Each secret store adapter MUST implement the following contract:

```ts
type SecretRef = {
  type: "secret_ref";
  store: string;
  object_id: string;
  field_path?: string | null;
  version?: string | null;
};

type ResolveContext = {
  purpose: string;
  workspace_id?: string | null;
  actor?: string | null;
  workflow_ref?: string | null;
  run_id?: string | null;
};

type ResolvedSecretMetadata = {
  store: string;
  object_id_redacted: string;
  field_path?: string | null;
  version_resolved?: string | null;
  content_shape: "scalar" | "object";
};

type ResolvedSecret =
  | { payload: string; metadata: ResolvedSecretMetadata }
  | { payload: Record<string, unknown> | unknown[]; metadata: ResolvedSecretMetadata };

interface SecretStoreAdapter {
  validateRef(ref: SecretRef): void;
  resolveRef(ref: SecretRef, context: ResolveContext): Promise<ResolvedSecret>;
  capabilities(): {
    object_values: boolean;
    field_selection: boolean;
    version_selection: boolean;
    write_support: boolean;
  };
  redactRef(ref: SecretRef): string;
}
```

### Contract rules

- `validateRef(ref)` MUST reject malformed refs before execution.
- `resolveRef(ref, context)` MUST return the resolved payload plus log-safe metadata.
- `capabilities()` MUST describe what the adapter can do without changing the shared authoring model.
- `redactRef(ref)` MUST return a log-safe representation and MUST NOT expose secret values.

### Initial adapter families

The shared architecture MUST be designed to support these adapter families:

- `local-file`
- `bitwarden-cli`
- `lastpass-cli`
- `vault-kv`
- `aws-secrets-manager`
- `gcp-secret-manager`
- `azure-key-vault`

Write and rotation operations MAY exist as adapter-specific extensions, but they are out of scope for the base contract.

## Workflow Authoring Contract

Workflows MUST use secret-aware bindings. They MUST NOT rely on generic string interpolation to turn a reference into a plaintext secret.

There are two standard consumption patterns.

### Pattern 1: Inline secret binding

The action embeds the `secret_ref` directly in a secret-aware field:

```yaml
steps:
  - id: call-api
    action:
      kind: http_request
      headers:
        Authorization:
          value_from:
            secret_ref:
              type: secret_ref
              store: aws-secrets-manager
              object_id: arn:aws:secretsmanager:us-east-1:123456789012:secret:vendor/api
              field_path: /api_key
              version: AWSCURRENT
```

### Pattern 2: Typed input carrying a `secret_ref`

The workflow input itself is a reference object, not a plaintext secret:

```yaml
inputs:
  - name: account_secret
    type: secret_ref
    required: true

steps:
  - id: login
    action:
      kind: login_form
      account:
        value_from:
          secret_ref_input: account_secret
```

### Rules

- New workflows MUST NOT declare plaintext secret-bearing string inputs.
- Secret-bearing inputs MUST use type `secret_ref`.
- Action fields that consume secrets MUST use `value_from.secret_ref` or `value_from.secret_ref_input`.
- Generic `${...}` interpolation MUST NOT dereference a `secret_ref`.
- Generic `${...}` interpolation MUST NOT stringify a `secret_ref` into a string.
- If interpolation encounters a `secret_ref` in a string context, the runtime MUST fail with a validation error.

## Runtime Resolution Model

Secret refs and resolved secret values have different allowed boundaries.

### Stable architecture seams

| Seam | `secret_ref` allowed | Resolved value allowed | Policy |
| --- | --- | --- | --- |
| Workflow ingress | yes | no | Workflows, API requests, and schedulers may submit refs only |
| Worker or daemon IPC | yes | no | Cross-process transport must carry refs, never plaintext |
| Interpolation boundary | yes | no | Interpolation may move refs as structured values but must not resolve them |
| Action execution boundary | yes | yes | Executor resolves immediately before use |
| Logging and telemetry | redacted only | no | Use `redactRef(ref)` only |
| Pause or resume payloads | yes | no | Session state may persist refs, never resolved values |
| Persisted run state | yes | no | Store refs only when needed for replay semantics |
| Semantic memory | no by default | no | Secret-related details should be excluded unless explicitly safe and redacted |
| Artifacts | no by default | no | Screenshots, video, DOM dumps, and traces must not persist secrets |

### Resolution lifecycle

The runtime MUST follow this lifecycle:

1. Parse the workflow.
2. Validate workflow structure and `secret_ref` structure.
3. Transport only refs across process and daemon boundaries.
4. Resolve refs only when a secret-aware action is about to execute.
5. Inject the resolved value into the action in memory only.
6. Discard the resolved value immediately after the action completes.

Implementations SHOULD overwrite mutable buffers when the language and runtime make that practical. Where explicit overwrite is not practical, implementations MUST still minimize value lifetime and MUST prevent serialization or logging.

### Secret-aware actions

An action is secret-aware if it explicitly declares that one or more fields may consume a `secret_ref`.

A secret-aware action MUST declare:

- which fields accept `value_from.secret_ref`
- which fields accept `value_from.secret_ref_input`
- whether it expects a scalar or an object-shaped secret
- which object fields are required if it accepts an object-shaped secret

If an action accepts a whole object-shaped secret, the runtime MUST validate the required fields before the action runs.

If an action needs only one field from an object secret, the preferred pattern is `field_path` selection in the `secret_ref`.

## Security, Redaction, and Artifacts

Resolved secret values MUST NOT appear in:

- logs
- progress events
- returned workflow state
- persisted semantic memory
- pause or resume payloads
- error messages
- artifact metadata
- screenshots
- video recordings
- DOM snapshots
- debug traces

### Sensitive-step defaults

Any step that consumes a `secret_ref` is sensitive by default.

Sensitive steps MUST default to:

- screenshots disabled
- video recording disabled
- DOM snapshots disabled or scrubbed before persistence
- debug traces redacted

A future implementation MAY support masked screenshots or masked video, but only if masking is applied before persistence and is guaranteed not to expose the secret value.

### Redaction rules

- Logs MUST use `redactRef(ref)` rather than raw refs.
- Error messages SHOULD identify the store and a redacted object id.
- Error messages MAY include `field_path` and resolved version metadata if those values are not sensitive in the target environment.
- No logging path may include the resolved payload.

## Standards Alignment

This design aligns with established reference-based secret injection patterns instead of plaintext config injection.

### Kubernetes

Kubernetes uses reference-based secret wiring through `secretKeyRef` rather than embedding resolved values in Pod specs. This architecture follows the same principle: configuration references secrets by id and key, and the runtime resolves them later.

Reference: [Kubernetes Secrets](https://kubernetes.io/docs/concepts/configuration/secret/)

### AWS Secrets Manager

AWS Secrets Manager supports a model built around secret id, optional JSON key selection, and optional version selection. This architecture mirrors that shape with `object_id`, `field_path`, and `version`.

References:

- [AWS Secrets Manager dynamic references](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/dynamic-references-secretsmanager.html)
- [What's in a Secrets Manager secret?](https://docs.aws.amazon.com/secretsmanager/latest/userguide/whats-in-a-secret.html)

### HashiCorp Vault

Vault KV v2 supports secret paths and field selection, which maps cleanly to the normalized `object_id` plus `field_path` model defined here.

References:

- [Vault KV v2 read data](https://developer.hashicorp.com/vault/docs/secrets/kv/kv-v2/cookbook/read-data)
- [Vault read command](https://developer.hashicorp.com/vault/docs/commands/read)

### JSON Pointer

This architecture standardizes field selection on JSON Pointer so adapters can normalize store-specific key-selection models into one shared cross-store contract.

Reference: [RFC 6901](https://www.rfc-editor.org/rfc/rfc6901)

## Examples

### Example 1: Whole login object secret

```yaml
inputs:
  - name: openai_account
    type: secret_ref
    required: true

steps:
  - id: openai-login
    action:
      kind: login_form
      account:
        value_from:
          secret_ref_input: openai_account
```

The referenced object secret might look like:

```json
{
  "email": "dev@example.com",
  "password": "********",
  "totp": "********"
}
```

The workflow never carries those values directly.

### Example 2: Single API key from an object secret

```yaml
steps:
  - id: vendor-call
    action:
      kind: http_request
      headers:
        X-API-Key:
          value_from:
            secret_ref:
              type: secret_ref
              store: vault-kv
              object_id: secret/data/vendors/acme
              field_path: /api_key
```

### Example 3: Workflow migration from plaintext input to `secret_ref`

Before:

```yaml
inputs:
  - name: email
    required: true
  - name: password
    required: true

steps:
  - id: login
    action:
      kind: login_form
      email: "${inputs.email}"
      password: "${inputs.password}"
```

After:

```yaml
inputs:
  - name: account_secret
    type: secret_ref
    required: true

steps:
  - id: login
    action:
      kind: login_form
      account:
        value_from:
          secret_ref_input: account_secret
```

## Migration Strategy

### Phase 1: Publish the shared spec

- Add this document
- Align runtime and workflow schema discussions around one primitive: `secret_ref`
- Ban new plaintext secret-bearing inputs in newly authored workflows

### Phase 2: Add secret-aware runtime boundaries

- Carry refs, not plaintext, across workflow ingress and IPC
- Prevent generic interpolation from dereferencing refs
- Mark secret-consuming steps as sensitive by default
- Redact refs consistently in logs and errors

### Phase 3: Add proof adapters

- Implement `local-file`
- Implement `bitwarden-cli`

The goal of these adapters is to prove the shared architecture, not to lock the system to those two stores.

### Phase 4: Migrate existing secret-bearing workflows

- Replace plaintext secret inputs with `secret_ref`
- Replace `${inputs.password}`-style flows with secret-aware action bindings
- Migrate one representative login workflow and one representative API workflow first

### Phase 5: Gate or remove legacy plaintext support

- Add warnings for legacy plaintext secret inputs
- Add policy or validation gates to block new plaintext secret inputs
- Remove compatibility paths once the necessary workflows have migrated

## Conformance Checklist

An implementation conforms to this plan only if all of the following are true:

- It uses `secret_ref` as the only core reference type.
- It supports both scalar and object-shaped secrets.
- It resolves secrets only at action execution time.
- It never serializes resolved values across IPC boundaries.
- It never logs resolved values.
- It never persists resolved values in returned state, semantic memory, or artifacts.
- It supports inline secret bindings and typed inputs carrying a `secret_ref`.
- It rejects string interpolation that would stringify or dereference a `secret_ref`.

## Final Notes

This architecture is intentionally store-agnostic.

The store-specific authentication story remains an adapter concern. A local file adapter may rely on OS file permissions. A CLI-backed adapter may rely on a local session. A cloud adapter may rely on IAM, workload identity, or machine credentials. The shared runtime contract does not change.

The core principle does not change either:

Configuration references secrets by opaque id and optional field selector. The runtime resolves them only at execution time, injects them only into the action that needs them, and avoids exposing them anywhere durable.
