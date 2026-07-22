from __future__ import annotations

import json

from flowlet.runtime import (
    RuntimeEvent,
    RuntimeEventStatus,
    RuntimeProcessSpec,
    RuntimeProjection,
    RuntimeStatusClass,
    RuntimeStore,
)
from flowlet.runtime._dev.validate_runtime_sidecar import _validate_current_layout


def test_current_layout_accepts_root_declaration_after_lifecycle_events(tmp_path) -> None:
    runtime_id = "runtime-1"
    store = RuntimeStore(tmp_path)
    store.write_runtime_info({"job_id": runtime_id})
    store.write_process_specs(
        [RuntimeProcessSpec(process_id=runtime_id, process_type="test.root")]
    )
    store.write_projection(
        RuntimeProjection(
            runtime_id=runtime_id,
            event_count=2,
            processes={},
        ).model_dump(mode="json")
    )
    projection = json.loads((tmp_path / "runtime" / "projection.json").read_text())
    projection["processes"] = {
        runtime_id: {
            "process_id": runtime_id,
            "process_type": "test.root",
            "status": "succeeded",
            "status_class": "terminal_success",
        }
    }
    store.write_projection(projection)
    events = [
        RuntimeEvent(
            event_id=0,
            runtime_id=runtime_id,
            event_type="backend_session.acquired",
            timestamp=1.0,
        ),
        RuntimeEvent(
            event_id=1,
            runtime_id=runtime_id,
            process_id=runtime_id,
            event_type="process.created",
            timestamp=2.0,
            status=RuntimeEventStatus.PENDING,
            status_class=RuntimeStatusClass.NOT_STARTED,
        ),
    ]

    result = _validate_current_layout(tmp_path, events)

    assert all(result.values())
