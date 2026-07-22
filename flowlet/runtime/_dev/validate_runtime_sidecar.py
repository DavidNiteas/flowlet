"""Validate legacy and standard RuntimeEvent files in runtime directories."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from flowlet.runtime import RuntimeEvent, txn_event_payload_to_runtime_event


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runtime_dir", nargs="+", type=Path)
    parser.add_argument(
        "--require-sidecar",
        action="store_true",
        help="Fail when runtime/events.runtime.jsonl is missing or empty.",
    )
    args = parser.parse_args()

    failed = False
    for runtime_dir in args.runtime_dir:
        result = validate_runtime_dir(runtime_dir, require_sidecar=args.require_sidecar)
        failed = failed or not result
    return 1 if failed else 0


def validate_runtime_dir(runtime_dir: Path, *, require_sidecar: bool = False) -> bool:
    legacy_path = runtime_dir / "events.jsonl"
    sidecar_path = runtime_dir / "runtime" / "events.runtime.jsonl"
    legacy_count = _validate_legacy_events(legacy_path)
    sidecar_count = _validate_sidecar_events(sidecar_path)

    ok = legacy_count > 0
    if require_sidecar:
        ok = ok and sidecar_count > 0
    print(
        json.dumps(
            {
                "runtime_dir": str(runtime_dir),
                "legacy_events": legacy_count,
                "sidecar_events": sidecar_count,
                "sidecar_required": require_sidecar,
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


def _validate_sidecar_events(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        RuntimeEvent.model_validate_json(line)
        count += 1
    return count


if __name__ == "__main__":
    raise SystemExit(main())

