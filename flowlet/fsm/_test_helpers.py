"""FSM test helpers that are importable by Ray workers."""

from __future__ import annotations

import asyncio
from typing import Any


def load_action(state: dict[str, Any], runtime, event) -> None:
    state["loaded"] = True


def add_action(state: dict[str, Any], runtime, event) -> None:
    state["value"] = state.get("value", 0) + 1


async def async_add_action(state: dict[str, Any], runtime, event) -> None:
    await asyncio.sleep(0.01)
    state["value"] = state.get("value", 0) + 1


def require_loaded(state: dict[str, Any], runtime, event) -> bool:
    return bool(state.get("loaded"))


def fail_action(state: dict[str, Any], runtime, event) -> None:
    msg = "fsm action failed"
    raise ValueError(msg)
