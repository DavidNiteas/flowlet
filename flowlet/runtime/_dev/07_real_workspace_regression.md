# Real Workspace Regression

This note defines the current real-sample regression target for the runtime
redesign.

## Target Workspace

Use:

```text
/mnt/data/daiql/dev_repo/MetaEngine-mono/data/large_files/ms_exp_datas/900_human_metabolites_db/workspace/900_human_metabolites_liver
```

The workspace is treated as the study root.

Expected runtime directories:

```text
.metams/runtime
.annotation/spec_spec_unispec_pos/runtime
```

Historical directories may also exist:

```text
runtime
study/.searching/runtime
```

Those historical directories are useful for migration inspection, but they are
not the target layout.

## Current Historical-State Check

Existing runs in the target workspace predate standard sidecar writing. They
are expected to have legacy `events.jsonl` files and may not have
`runtime/events.runtime.jsonl`.

Use the validator in compatibility mode:

```bash
pixi run -e dev-all-gpu python flowlet/flowlet/runtime/_dev/validate_runtime_sidecar.py \
  data/large_files/ms_exp_datas/900_human_metabolites_db/workspace/900_human_metabolites_liver/.metams/runtime \
  data/large_files/ms_exp_datas/900_human_metabolites_db/workspace/900_human_metabolites_liver/.annotation/spec_spec_unispec_pos/runtime
```

Acceptance:

- Each target runtime directory has at least one legacy event.
- Every legacy event can be adapted to `RuntimeEvent`.
- Missing sidecar files are allowed for historical runs.

## Current-Layout New-Run Check

After rerunning MetaMSTools or MassLib4Search with the current code, use both
strict flags:

```bash
pixi run -e dev-all-gpu python flowlet/flowlet/runtime/_dev/validate_runtime_sidecar.py --require-sidecar --require-current-layout \
  data/large_files/ms_exp_datas/900_human_metabolites_db/workspace/900_human_metabolites_liver/.metams/runtime \
  data/large_files/ms_exp_datas/900_human_metabolites_db/workspace/900_human_metabolites_liver/.annotation/spec_spec_unispec_pos/runtime
```

Acceptance:

- Each target runtime directory has at least one legacy event.
- Every legacy event can be adapted to `RuntimeEvent`.
- Each target runtime directory has at least one standard sidecar event.
- Every standard sidecar event validates as `RuntimeEvent`.
- Each target runtime directory has `runtime/processes.json` with its root
  process declaration.
- The first standard sidecar event is `event_id == 0` and
  `event_type == "process.created"`; its `process_id` is declared in the
  manifest.
- Each target runtime directory has `runtime/projection.json`, whose root
  process is visible through the package runtime-snapshot reader. Its event
  count is positive and cannot exceed the sidecar count. Equality is not
  required because append-only log and stream events do not force a projection
  rewrite.

## Migrating-Layout Observation

Fresh workflows were executed against the target workspace after standard
sidecar and projection persistence were added:

```text
.metams/runtime: 386 legacy events, 94 sidecar events, succeeded projection.
.annotation/spec_spec_unispec_pos/runtime: 370 legacy events, 185 sidecar events, succeeded projection.
```

`--require-sidecar` validation passes for both directories. Their
projection-aware package readers report a completed job with 3 total, 3
completed, and 0 remaining.
The annotation results remain under the study root at:

```text
annotations/spec_spec_unispec_pos/search_annotation_results_lib
```

## Reader Compatibility Check

The existing CLI readers were exercised against the same historical target
directories after adding projection-aware Flowlet reads:

```bash
pixi run -e dev-all-gpu meta-ms-tools runtime-snapshot print \
  data/large_files/ms_exp_datas/900_human_metabolites_db/workspace/900_human_metabolites_liver/.metams/runtime \
  --format json
pixi run -e dev-all-gpu masslib4search runtime-snapshot print \
  data/large_files/ms_exp_datas/900_human_metabolites_db/workspace/900_human_metabolites_liver/.annotation/spec_spec_unispec_pos/runtime \
  --format json
```

Current result:

- Both commands succeed and retain `runtime_info`, `monitor`, and `snapshot`.
- The MetaMSTools monitor reports 3 completed runs.
- The MassLib4Search monitor reports 3 completed runs.
- Both runtime directories contain `runtime/projection.json`; the CLI readers
  therefore exercise their projection-aware business adapters while retaining
  the legacy monitor and snapshot detail.

## Current Revalidation

The sidecar validator and both CLI commands were rerun after the standard
event-stream, client, vocabulary, and resource-observation additions.

- Strict validation still reports 386 legacy / 94 standard events for
  `.metams/runtime`, and 370 legacy / 185 standard events for the annotation
  runtime.
- Both projection-aware CLI commands succeed with completed monitors showing
  3 total, 3 completed, and 0 remaining.
- This is a read-only regression of the persisted real sample; it does not
  rewrite its study output or runtime files.

## Declaration Regression Gap

On 2026-07-22, `--require-current-layout` correctly fails for both persisted
directories: each has a valid legacy stream, sidecar, and projection, but no
`runtime/processes.json` and no root declaration at sidecar event id `0`.
These runs were created before root declarations were introduced. They remain
valuable reader-compatibility fixtures, but are not evidence for the current
writer contract.

The next execution regression must use isolated runtime directories and retain
the existing output untouched. It must record the exact commands, output paths,
elapsed time, event counts, manifest content, and package snapshot-reader
results in this document.

## Isolated Execution Recipe

Run these commands from the monorepo root. The explicit MetaMSTools output
directory is intentionally outside the study layout: `--workdir` would force
the normal `.metams/runtime` path and overwrite the historical runtime. The
three source mzML files remain the real liver inputs.

```bash
workspace=data/large_files/ms_exp_datas/900_human_metabolites_db/workspace/900_human_metabolites_liver
metams_runtime="$workspace/.metams/runtime_regression_current"
metams_output="$workspace/.metams/regression_output_current"
metams_config="$workspace/.metams/config/900_human_metabolites_liver_20260722_215619.toml"

pixi run -e dev-all-gpu meta-ms-tools run-analysis liver_runtime_regression \
  /mnt/data/daiql/ms_exp_datas/900_human_metabolites_db/mzml/pos/liver/Liver-1.mzML \
  /mnt/data/daiql/ms_exp_datas/900_human_metabolites_db/mzml/pos/liver/Liver-2.mzML \
  /mnt/data/daiql/ms_exp_datas/900_human_metabolites_db/mzml/pos/liver/Liver-3.mzML \
  --output "$metams_output" \
  --runtime-dir "$metams_runtime" \
  --config-path "$metams_config" \
  --set batch_config.worker_type=synchronous
```

For MassLib4Search, the workspace is the real study root. A unique annotation
id preserves the existing `spec_spec_unispec_pos` result and writes the new
annotation result into the study as required by workspace mode:

```bash
workspace=data/large_files/ms_exp_datas/900_human_metabolites_db/workspace/900_human_metabolites_liver
annotation_id=runtime_regression_current
mass_runtime="$workspace/.annotation/$annotation_id/runtime"

pixi run -e dev-all-gpu masslib4search search annotation \
  data/large_files/ms_exp_datas/900_human_metabolites_db/database/pos/derivative/search_db \
  --workspace "$workspace" \
  --config-path "$workspace/masslib4search_spec_spec_unispec_config.toml" \
  --annotation-id "$annotation_id" \
  --annotation-write-policy error \
  --runtime-dir "$mass_runtime" \
  --annotation-runtime-dir "$mass_runtime"
```

Before either command, verify that its target runtime directory and output or
annotation id do not already exist. Do not remove an existing target as part of
the regression procedure; choose a new suffix instead.

Then validate and inspect both new runtime directories:

```bash
pixi run -e dev-all-gpu python flowlet/flowlet/runtime/_dev/validate_runtime_sidecar.py \
  --require-sidecar --require-current-layout "$metams_runtime" "$mass_runtime"
pixi run -e dev-all-gpu meta-ms-tools runtime-snapshot print "$metams_runtime" --format json
pixi run -e dev-all-gpu masslib4search runtime-snapshot print "$mass_runtime" --format json
```

## Execution Evidence

### MetaMSTools, 2026-07-22

The isolated MetaMSTools command above was executed with the three real liver
mzML files and `batch_config.worker_type=synchronous`.

```text
runtime: .metams/runtime_regression_current
output:  .metams/regression_output_current
job id:  90df7199c63f4567883a2ec2db15efc4
elapsed: 90.515 seconds
result:  completed, 3 of 3 runs completed, 0 failed
```

Observed artifacts:

- The output directory contains the persisted study result (149 MB, seven
  files) without modifying the existing study root artifacts.
- `events.jsonl` contains 334 legacy events.
- `runtime/events.runtime.jsonl` contains 335 standard events. Event `0` is
  the root `process.created` declaration.
- `runtime/processes.json` declares the root
  `metams.openms.analysis` process with `metadata.engine = MetaMSTools`.
- `runtime/projection.json` reports `succeeded` and contains 94 projected
  stateful events. It is intentionally smaller than the JSONL stream because
  subsequent append-only telemetry events do not force projection rewrites.
- `validate_runtime_sidecar.py --require-sidecar --require-current-layout`
  passes, and `meta-ms-tools runtime-snapshot print --format json` reports a
  completed monitor with 3 total, 3 completed, and 0 remaining.

### MassLib4Search, 2026-07-22

The workspace-mode MassLib4Search command above was executed against the same
real study root using the fresh `runtime_regression_current` annotation id.
The existing `spec_spec_unispec_pos` result was not modified.

```text
runtime: .annotation/runtime_regression_current/runtime
results: annotations/runtime_regression_current/search_annotation_results_lib
job id:  2d450a39a8a64f42b53a25da069b0e5c
elapsed: 17.043 seconds
result:  completed, 3 of 3 runs completed, 0 failed
```

Observed artifacts:

- The new annotation directory contains eight files (53 MB). The existing
  `annotations/spec_spec_unispec_pos/search_annotation_results_lib` remains
  present and unchanged.
- `events.jsonl` contains 185 legacy events; the standard sidecar contains 186
  events, beginning with the root `process.created` event at id `0`.
- `runtime/processes.json` declares `annotation.search` with
  `metadata.engine = MassLib4Search`.
- `runtime/projection.json` reports `succeeded` with 80 projected stateful
  events. The same append-only telemetry rule explains the lower projection
  count.
- `validate_runtime_sidecar.py --require-sidecar --require-current-layout`
  passes for both fresh runtimes, and both package
  `runtime-snapshot print --format json` commands succeed.

The process emitted a Transformers warning that a `unimol` model was being
instantiated as `clip`. It did not produce a runtime error and all annotation
summary counts were populated, but it is a separate model-configuration item
to investigate before treating the numerical annotation result as a model
quality benchmark. It is not a Flowlet runtime contract failure.

## RuntimeObservation Readback

After `RuntimeObservation` and `load_runtime_observation()` were introduced,
both fresh real runtimes were read through that framework-only loader without
using business snapshots:

| Runtime | Available | Projection | Declared root type |
| --- | --- | --- | --- |
| `.metams/runtime_regression_current` | yes | `succeeded` | `metams.openms.analysis` |
| `.annotation/runtime_regression_current/runtime` | yes | `succeeded` | `annotation.search` |

The strict sidecar/current-layout validator was rerun at the same time and
passed for both directories (334/335 MetaMSTools legacy/standard events;
185/186 MassLib4Search legacy/standard events).
