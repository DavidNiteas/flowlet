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

### Continued Progress

Phase 2 RuntimeEvent store and adapters were started.

Added:

- `flowlet.runtime.event_store.RuntimeEventStore`
- `RuntimeEventJsonlStore`
- `flowlet.runtime.adapters.LEGACY_TXN_EVENT_TYPE_MAP`
- `txn_event_payload_to_runtime_event(...)`
- `runtime_event_to_txn_event_payload(...)`
- `manager_record_to_runtime_event(...)`
- `RuntimeFileLayout.runtime_events = "runtime/events.runtime.jsonl"`
- `RuntimeStore.runtime_event_store()`
- `RuntimeStore.append_runtime_event(...)`

Added tests for:

- JSONL append/load/list/wait behavior.
- TxnEvent-like dictionary to RuntimeEvent conversion.
- RuntimeEvent back to legacy dictionary conversion.
- Current-style MetaMSTools and MassLib4Search legacy event payload shapes.
- Flowlet manager record conversion for progress, signal, log, stream, and telemetry.
- Standard RuntimeEvent sidecar writing through `RuntimeStore`.
- Opt-in sidecar mirroring through `RuntimeEventSidecarWriter`.

Validation commands:

```bash
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py --fix
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py
pixi run -e dev-all-gpu pytest flowlet/tests/test_runtime.py -q
```

Result:

```text
All checks passed.
12 passed.
```

### Continued Progress: Sidecar Writer

Added:

- `flowlet.runtime.sidecar.RuntimeEventSidecarWriter`

The writer can append:

- already-normalized `RuntimeEvent` objects
- legacy `TxnEvent`-like dictionary payloads
- Flowlet manager records for progress, signal, log, stream, and telemetry

This keeps the current legacy `events.jsonl` stream unchanged while allowing
business packages to opt into the standard sidecar stream at
`runtime/events.runtime.jsonl`.

Added tests for:

- disabled sidecar writer no-op behavior
- legacy payload mirroring
- manager record mirroring
- artifact listing for the standard sidecar file

Validation commands:

```bash
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py --fix
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py
pixi run -e dev-all-gpu pytest flowlet/tests/test_runtime.py -q
```

Result:

```text
All checks passed.
15 passed.
```

### Open Work

Phase 1 is not fully complete until the schema is reviewed against real MetaMSTools and MassLib4Search event payloads.

Recommended next steps:

1. Integrate `RuntimeEventSidecarWriter` in current business backends without changing their legacy `events.jsonl`.
2. Add fixtures from actual runtime files once fixture ownership is decided.
3. Keep CLI/TUI readers on existing projections until event-derived projections are implemented.

### Boundary Reminder

Do not add domain fields to `RuntimeEvent`.

Keep these in business packages:

- OpenMS stage names.
- Annotation search job types.
- Study/workspace paths as interpreted domain concepts.
- Business monitor summaries.
- Resume policy.
