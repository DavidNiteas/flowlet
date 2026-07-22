# Legacy Event Consumer Census

## Status

Repository census completed on 2026-07-22. This is evidence for Wave 1 of
`09_legacy_migration_plan.md`; it does not cover callers outside this monorepo.

## Method and Limit

The census searched Python source and tests for `TxnEvent`, `EventBuffer`,
`events.jsonl`, `/events`, `stream_events`, `wait_events`, and `.events(`.
It establishes repository-owned consumers only. Package owners must identify
external services, notebooks, scripts, and API clients before changing public
endpoint defaults.

## MetaMSTools

| Consumer | Evidence | Current role | Migration target |
| --- | --- | --- | --- |
| Backend buffer/writer | `txn/backend/events.py:31`; `backend.py:271`, `355` | Writes and restores `events.jsonl`; sidecar is mirrored | Retain until a package monitor adapter is standard-event native |
| Backend manager mirror | `txn/backend/backend.py:206`, `215`, `425` | Converts manager records into legacy buffer events | Package presentation adapter consuming standard events |
| Backend HTTP endpoint | `txn/backend/app.py:138`, `182` | `GET /api/jobs/{job_id}/events` SSE | Versioned legacy endpoint policy |
| CLI HTTP client | `cli/backend_client.py:114` | Calls legacy `/events` | Switch new callers to `runtime_events()` after UI adapter exists |
| GUI HTTP proxy | `gui/api.py:273`, `282`, `335` | Streams legacy backend events to GUI task views | Package GUI standard-event presentation adapter |
| GUI WebSocket endpoint | `gui/ws.py:17` | Exposes task event stream | Decide WebSocket standard frame contract separately |
| Tests | `tests/txn/backend/test_txn_backend.py`, `tests/cli/test_backend_client.py`, `tests/gui/test_backend_proxy.py` | Assert legacy persistence, replay, and proxy behavior | Keep as compatibility coverage through removal review |

The package already has the additive standard HTTP client method at
`cli/backend_client.py:123`, and the backend standard endpoint at
`txn/backend/app.py:148`.

## MassLib4Search

| Consumer | Evidence | Current role | Migration target |
| --- | --- | --- | --- |
| Backend buffer/writer | `txn/backend/events.py:31`; `backend.py:306`, `392` | Writes and restores `events.jsonl`; sidecar is mirrored | Retain until a package monitor adapter is standard-event native |
| Backend wait/stream loop | `txn/backend/backend.py:179`, `196`, `213`, `252` | Waits for and replays legacy events, including terminal drain | Standard stream with a package-owned root-terminal predicate |
| Ray manager mirror | `txn/search/executor.py:760` | Sends Ray runtime records into existing business observation path | Keep business execution ownership; only adapt emitted observation envelope |
| Backend HTTP endpoint | `txn/backend/app.py:133`, `173` | `GET /api/jobs/{job_id}/events` SSE | Versioned legacy endpoint policy |
| CLI HTTP client | `cli/backend_client.py:120` | Calls legacy `/events` | Switch new callers to `runtime_events()` after UI adapter exists |
| Tests | `tests/txn/test_backend.py`, `tests/cli/test_backend_client.py` | Assert legacy persistence, restore, and streaming | Keep as compatibility coverage through removal review |

The package already has the additive standard HTTP client method at
`cli/backend_client.py:129`, and the backend standard endpoint at
`txn/backend/app.py:140`.

## Flowlet Compatibility Owners

| Component | Evidence | Constraint |
| --- | --- | --- |
| Legacy buffer base | `flowlet/runtime/events.py:15` | Do not expand with business fields |
| Legacy manager mirror | `flowlet/runtime/backend.py:65` | Keep until package adapters stop needing it |
| Standard bridge | `flowlet/runtime/sidecar.py:17` | Framework-only; no business imports |
| Standard SSE/JSONL | `flowlet/runtime/stream.py:24`, `68` | Remains the replacement transport surface |

## Wave 1 Result

The repository condition for Wave 1 is met: every direct in-repository legacy
reader/writer has a named owner and a target migration class. The remaining
Wave 1 requirements are non-code decisions:

1. MetaMSTools product owners must identify GUI/WebSocket and external clients
   outside the repository and approve an endpoint version policy.
2. MassLib4Search product owners must identify external API/CLI clients and
   approve the same policy.
3. Both packages must decide whether standard-event presentation adapters cover
   only read-only inspection first, or also live monitor views.

Until these decisions exist, Wave 2 implementation may add adapters but must
not disable legacy writes or change `/events` behavior.

## Wave 2 Baseline Evidence

Both package CLI adapters can now render root lifecycle aggregates from a
standard projection plus `status.json` alone. This is covered by package CLI
tests and does not use `events.jsonl`, `snapshot.json`, or
`monitor_snapshot.json`. The retained business record is still required for
identity, paths, planned counts, and package monitor schemas.
