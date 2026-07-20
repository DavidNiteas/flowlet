"""Artifact listing helpers for Flowlet runtime directories."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def list_runtime_artifacts(runtime_dir: str | Path) -> list[dict[str, Any]]:
    """Return file artifacts under a runtime directory using stable relative paths."""
    root = Path(runtime_dir)
    if not root.exists():
        return []
    artifacts: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        stat = path.stat()
        artifacts.append(
            {
                "path": path.relative_to(root).as_posix(),
                "size": stat.st_size,
                "mtime": stat.st_mtime,
            }
        )
    return artifacts
