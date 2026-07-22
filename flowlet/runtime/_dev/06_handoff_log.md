# Handoff Log

This file records implementation progress for the runtime backend redesign.

## 2026-07-22

### Completed

- Added initial design document set under `flowlet/flowlet/runtime/_dev`.
- Completed Phase 0 boundary audit:
  - See [05_boundary_audit.md](05_boundary_audit.md).
  - Current migration-support APIs are classified.
  - APIs that should not be expanded are explicitly listed.
- Started Phase 1 RuntimeEvent schema implementation:
  - `flowlet.runtime.schema.RuntimeEvent`
  - `RuntimeEventStatus`
  - `RuntimeStatusClass`
  - `RuntimeProgress`
  - `RuntimeErrorInfo`
  - `runtime_event_payload`
- Exported the new schema objects from:
  - `flowlet.runtime`
  - top-level `flowlet`
- Added tests for:
  - JSON-safe runtime event payloads.
  - Custom business status with framework `status_class`.
  - Standard error payload serialization.

### Validation

Commands run:

```bash
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py --fix
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py
pixi run -e dev-all-gpu pytest flowlet/tests/test_runtime.py -q
```

Result:

```text
All checks passed.
6 passed.
```

### Open Work

Phase 1 is not fully complete until the schema is reviewed against real MetaMSTools and MassLib4Search event payloads.

Phase 2 should start only after that review.

Recommended next steps:

1. Add a small compatibility fixture set from current `TxnEvent` payloads.
2. Implement `txn_event_payload_to_runtime_event(...)` as an adapter, not a business dependency.
3. Introduce a `RuntimeEventStore` protocol and JSONL implementation.
4. Decide whether standard events should initially use a sidecar file such as `runtime/events.runtime.jsonl`.

### Boundary Reminder

Do not add domain fields to `RuntimeEvent`.

Keep these in business packages:

- OpenMS stage names.
- Annotation search job types.
- Study/workspace paths as interpreted domain concepts.
- Business monitor summaries.
- Resume policy.

