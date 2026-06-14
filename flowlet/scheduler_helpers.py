"""Importable helper targets for scheduler Ray tests and examples."""

from __future__ import annotations

from typing import Any


def add_values(x: int, y: int = 0) -> int:
    return x + y


def fail_target() -> None:
    msg = "scheduled failure"
    raise ValueError(msg)


def write_signal(value: int, signals: Any) -> int:
    signals.mark_completed("worker_signal", value=value)
    return value * 2
