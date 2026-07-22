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

## New-Run Check

After rerunning MetaMSTools or MassLib4Search with the current code, use strict
mode:

```bash
pixi run -e dev-all-gpu python flowlet/flowlet/runtime/_dev/validate_runtime_sidecar.py --require-sidecar \
  data/large_files/ms_exp_datas/900_human_metabolites_db/workspace/900_human_metabolites_liver/.metams/runtime \
  data/large_files/ms_exp_datas/900_human_metabolites_db/workspace/900_human_metabolites_liver/.annotation/spec_spec_unispec_pos/runtime
```

Acceptance:

- Each target runtime directory has at least one legacy event.
- Every legacy event can be adapted to `RuntimeEvent`.
- Each target runtime directory has at least one standard sidecar event.
- Every standard sidecar event validates as `RuntimeEvent`.

## Current Observation

The current checked workspace state validates legacy compatibility:

```text
.metams/runtime: 292 legacy events, no sidecar events.
.annotation/spec_spec_unispec_pos/runtime: 185 legacy events, no sidecar events.
```

This means the existing historical runtime files are compatible with the
standard adapter, while a fresh run is still required to prove sidecar emission
on the real sample.

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
- Neither historical directory has `runtime/projection.json`, so these checks
  prove legacy fallback rather than projection-first behavior.
