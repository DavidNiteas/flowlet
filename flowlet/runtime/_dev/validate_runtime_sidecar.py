"""Validate legacy and standard RuntimeEvent files in runtime directories."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from flowlet.runtime import RuntimeEvent, RuntimeStore, txn_event_payload_to_runtime_event


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runtime_dir", nargs="+", type=Path)
    parser.add_argument(
        "--require-sidecar",
        action="store_true",
        help="Fail when runtime/events.runtime.jsonl is missing or empty.",
    )
    parser.add_argument(
        "--require-current-layout",
        action="store_true",
        help=(
            "Require artifacts produced by the current runtime integration: a "
            "process manifest, root process.created event id 0, and a consistent projection."
        ),
    )
    parser.add_argument(
        "--allow-standard-only",
        action="store_true",
        help="Allow native Flowlet runtimes that intentionally have no legacy events.jsonl stream.",
    )
    args = parser.parse_args()

    failed = False
    for runtime_dir in args.runtime_dir:
        result = validate_runtime_dir(
            runtime_dir,
            require_sidecar=args.require_sidecar,
            require_current_layout=args.require_current_layout,
            allow_standard_only=args.allow_standard_only,
        )
        failed = failed or not result
    return 1 if failed else 0


def validate_runtime_dir(
    runtime_dir: Path,
    *,
    require_sidecar: bool = False,
    require_current_layout: bool = False,
    allow_standard_only: bool = False,
) -> bool:
    legacy_path = runtime_dir / "events.jsonl"
    sidecar_path = runtime_dir / "runtime" / "events.runtime.jsonl"
    legacy_count = _validate_legacy_events(legacy_path)
    sidecar_events = _load_sidecar_events(sidecar_path)
    sidecar_count = len(sidecar_events)

    ok = legacy_count > 0 or (allow_standard_only and sidecar_count > 0)
    if require_sidecar:
        ok = ok and sidecar_count > 0
    current_layout: dict[str, bool] | None = None
    if require_current_layout:
        current_layout = _validate_current_layout(runtime_dir, sidecar_events)
        ok = ok and all(current_layout.values())
    print(
        json.dumps(
            {
                "runtime_dir": str(runtime_dir),
                "legacy_events": legacy_count,
                "sidecar_events": sidecar_count,
                "sidecar_required": require_sidecar,
                "current_layout_required": require_current_layout,
                "standard_only_allowed": allow_standard_only,
                "current_layout": current_layout,
                "ok": ok,
            },
            ensure_ascii=False,
        )
    )
    return ok


def _validate_legacy_events(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        txn_event_payload_to_runtime_event(payload, runtime_id=str(path.parent))
        count += 1
    return count


def _load_sidecar_events(path: Path) -> list[RuntimeEvent]:
    if not path.exists():
        return []
    events: list[RuntimeEvent] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        events.append(RuntimeEvent.model_validate_json(line))
    return events


def _validate_current_layout(runtime_dir: Path, events: list[RuntimeEvent]) -> dict[str, bool]:
    """Validate declarations and projection written by the current bridge."""
    store = RuntimeStore(runtime_dir)
    try:
        specs = store.load_process_specs()
    except (OSError, ValueError, json.JSONDecodeError):
        specs = []
    projection = store.load_projection()
    root_event = events[0] if events else None
    root_process_id = root_event.process_id if root_event is not None else None
    declared_root_ids = {spec.resolved_process_id() for spec in specs}
    return {
        "process_manifest": bool(specs),
        "root_process_declaration": bool(
            root_event is not None
            and root_event.event_id == 0
            and root_event.event_type == "process.created"
            and root_process_id in declared_root_ids
        ),
        "projection": projection is not None,
        # Log and stream events are deliberately append-only and need not
        # trigger a projection rewrite, so equality is not an invariant.
        "projection_event_count_bounded": projection is not None
        and 0 < projection.event_count <= len(events),
        "projection_root_process": projection is not None
        and root_process_id is not None
        and root_process_id in projection.processes,
    }


if __name__ == "__main__":
    raise SystemExit(main())
