from __future__ import annotations

import json

from flowlet.runtime import RuntimeInfo, RuntimeStore, list_runtime_artifacts, runtime_info_payload


def test_runtime_info_payload_is_json_safe(tmp_path):
    payload = runtime_info_payload(
        engine="ExampleEngine",
        job_type="example.job",
        job_id="job1",
        status="completed",
        runtime_dir=tmp_path,
        workspace_path=tmp_path / "workspace",
        output_path=tmp_path / "workspace",
        planned_unit_count=3,
        result={"summary": {"count": 3}},
    )

    restored = RuntimeInfo.model_validate(payload)

    assert restored.schema_version == 1
    assert restored.engine == "ExampleEngine"
    assert restored.job_type == "example.job"
    assert restored.runtime_files.progress == "runtime/progress.json"


def test_runtime_store_writes_standard_files_and_lists_artifacts(tmp_path):
    store = RuntimeStore(tmp_path / "runtime")
    info = runtime_info_payload(
        engine="ExampleEngine",
        job_type="example.job",
        job_id="job1",
        status="running",
        runtime_dir=store.runtime_dir,
    )

    store.write_runtime_info(info)
    store.write_status({"job_id": "job1", "status": "running"})
    store.write_progress({"task": {"current": 1}})

    runtime_info = json.loads((store.runtime_dir / "runtime_info.json").read_text(encoding="utf-8"))
    artifacts = list_runtime_artifacts(store.runtime_dir)

    assert runtime_info["engine"] == "ExampleEngine"
    assert {item["path"] for item in artifacts} == {
        "runtime_info.json",
        "runtime/progress.json",
        "status.json",
    }
