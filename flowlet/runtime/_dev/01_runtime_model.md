# Runtime Model

## Objective

Flowlet runtime backend should be redesigned around event-driven observability and process-level orchestration.

The runtime backend is not a domain job backend. It is a framework-level executor that can run and observe Flowlet FSMs, workflows, executable units, and business-provided process implementations.

## Conceptual Layers

```text
Standard Event
  minimum observable unit

Standard Process
  minimum runtime operation unit
  composed from one event or many events

Runtime Backend
  event reporting system
  process orchestration executor
  projection and IO interface

Business Backend
  domain facade around runtime backend
  owns business schemas, workspace policy, config, API, and summary semantics
```

## Standard Event

A standard event is the smallest thing Flowlet can observe.

It does not imply retryability, cancellation, or independent execution. It only says that something observable happened.

Examples:

- A process started.
- A unit progressed.
- A FSM transition completed.
- A metric was sampled.
- A log record was emitted.
- An artifact was produced.
- An error was raised.

Events are append-only. They are the canonical runtime source.

## Standard Process

A standard process is the smallest runtime operation unit.

A process may be:

- A single function call wrapped as one observable operation.
- A Flowlet FSM execution.
- A Flowlet workflow.
- A group of executable units.
- A business-level operation such as "analyze study" or "annotate run".

A process can be composed from many events. It can also degenerate into a single event.

A process is addressable by the runtime backend. It can declare capabilities such as start, cancel, pause, resume, retry, cleanup, checkpoint, and resource requirements. Flowlet provides the interface; process implementations provide concrete behavior.

## Runtime Backend

The runtime backend is a framework execution system.

It is responsible for:

- Registering process specifications.
- Creating process contexts.
- Running process start hooks.
- Dispatching process control hooks when available.
- Receiving and storing standard events.
- Reducing event streams into runtime projections.
- Exposing standard runtime IO and observation interfaces.

It is not responsible for:

- Understanding business job types.
- Resolving domain config.
- Choosing workspace/study/annotation layout.
- Defining business snapshots.
- Implementing domain retry/cancel semantics.

## FSM Position

FSM is a local execution mechanism. It can be used inside a process.

```text
RuntimeBackend
  Process: analyze-study
    FSM: run-level processing
      Event: transition.started
      Event: transition.completed
      Event: artifact.produced
```

FSM events should enter the same runtime event stream when the process context is available. This gives Flowlet fine-grained observability without making FSM the top-level backend runtime model.

## Event Stream as Canonical Source

The long-term source of truth should be `events.jsonl`.

Existing files become projections or compatibility outputs:

```text
runtime_info.json        runtime/execution metadata
runtime/events.runtime.jsonl
                         standard RuntimeEvent sidecar stream during migration
events.jsonl             legacy compatibility event stream
status.json              compatibility process/execution state projection
snapshot.json            full runtime projection
monitor_snapshot.json    monitor-friendly projection
runtime/progress.json    progress projection
runtime/signals.json     signal projection
runtime/logs.jsonl       log projection
runtime/telemetry.jsonl  telemetry projection
```

The migration must keep existing files during compatibility phases.

During the compatibility phases, the canonical standard-event candidate is the sidecar file:

```text
runtime/events.runtime.jsonl
```

Existing `events.jsonl` remains the legacy TxnEvent-compatible stream until MetaMSTools and MassLib4Search readers migrate.

## Standard Runtime Layout Direction

The current layout remains valid:

```text
<runtime_dir>/
  runtime_info.json
  events.jsonl
  runtime/events.runtime.jsonl
  job_spec.json
  status.json
  snapshot.json
  monitor_snapshot.json
  runtime/
    progress.json
    signals.json
    logs.jsonl
    telemetry.jsonl
```

The future layout may add process-oriented shards without breaking current readers:

```text
<runtime_dir>/
  runtime_info.json
  events.jsonl
  processes/
    <process_id>/
      process_spec.json
      process_state.json
      events.jsonl
      artifacts.json
  projections/
    snapshot.json
    monitor_snapshot.json
    progress.json
    signals.json
    telemetry.jsonl
```

The current layout must remain readable throughout migration.
