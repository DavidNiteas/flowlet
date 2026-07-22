# Legacy Event Endpoint Version Policy

## Status

Draft awaiting package/product-owner approval. It is a decision record and
release checklist, not an implementation authorization.

## Scope

This policy covers the legacy `TxnEvent` SSE endpoints and their additive
standard `RuntimeEvent` replacements:

| Package surface | Legacy | Standard replacement |
| --- | --- | --- |
| MetaMSTools backend | `/api/jobs/{id}/events` | `/api/jobs/{id}/runtime-events` |
| MassLib4Search backend | `/api/jobs/{id}/events` | `/api/jobs/{id}/runtime-events` |
| MetaMSTools GUI proxy | `/api/v1/tasks/{id}/events` | `/api/v1/tasks/{id}/runtime-events` |
| MetaMSTools observation | no legacy equivalent | `/api/v1/tasks/{id}/runtime-observation` |

The MetaMSTools task WebSocket remains a business snapshot transport and is
outside this policy. A future standard WebSocket requires a separate decision.

## Proposed Decision

1. Keep every legacy `/events` endpoint supported and dual-written by default.
2. Mark standard endpoints as the required choice for new framework-level
   observability clients.
3. Do not attach a removal version or date until the external consumer census
   is signed off.
4. Do not make legacy endpoint use emit noisy runtime warnings; collect usage
   only through approved product telemetry, if any.
5. A later major-version proposal may introduce an opt-in standard-only writer
   for new runtimes, while historical legacy reading remains supported.

## Required External Consumer Census

Each package owner must record the following before approval:

| Consumer class | Owner | Known consumers | Contacted | Migration target | Status |
| --- | --- | --- | --- | --- | --- |
| Product GUI/browser clients | | | | | |
| External Python clients | | | | | |
| Notebooks and automation scripts | | | | | |
| Service-to-service integrations | | | | | |
| Persisted runtime archive readers | | | | | |

An empty table is not evidence of no consumers. It means the census is not
complete.

## Approval Gates

All gates must be satisfied before a legacy endpoint receives a deprecation
notice:

- [ ] MetaMSTools owner signs the external consumer census.
- [ ] MassLib4Search owner signs the external consumer census.
- [ ] Both owners confirm standard `runtime-events()` and
  `runtime_observation()` meet their intended client use cases.
- [ ] Live framework observation is available where a product needs it, without
  changing business controls or hierarchy.
- [ ] Fresh standard sidecar real-liver regression remains green for both
  packages.
- [ ] Legacy-only historical runtime readers remain green.
- [ ] Release owner approves a major/minor version and notice period.
- [ ] Rollback plan is documented and configuration-only.

## Future Deprecation Notice Template

Use only after all approval gates are checked.

```text
Status: deprecated in <release>; supported until <release/date>.
Affected endpoint: <legacy path>.
Replacement: <standard path>.
Event model change: TxnEvent -> complete RuntimeEvent SSE envelope.
Cursor change: numeric-only legacy cursor -> numeric or string standard cursor.
Historical runtime support: legacy readers remain available.
Rollback: set <approved compatibility switch> for new runtime writes; no
persisted event file is rewritten or deleted.
Owner/contact: <name/team>.
```

## Standard-Only Rollout Preconditions

A future compatibility switch may disable legacy event *writing* for a new
runtime only when:

1. All supported product clients can observe the standard stream or observation
   snapshot.
2. Standard endpoint replay, reconnect, unavailable-sidecar, success, failure,
   and cancellation cases are tested.
3. Both real liver fresh runs pass in standard-only mode.
4. The default remains dual-write until the announced major-version boundary.

No current package implements this switch. Do not add it as an unversioned
boolean in a business job config.

## Sign-Off

| Role | MetaMSTools | MassLib4Search | Date | Notes |
| --- | --- | --- | --- | --- |
| Package maintainer | | | | |
| Product/API owner | | | | |
| Framework maintainer | | | | |
| Release owner | | | | |
