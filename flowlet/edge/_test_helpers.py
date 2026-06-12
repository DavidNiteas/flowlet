"""Edge 测试辅助函数与类。

放在 ``flowlet.edge`` 包内，确保 Ray worker 可以导入。
"""

from __future__ import annotations

import threading
import time
from typing import Any

from flowlet import Kernel, Workflow
from flowlet.config import BaseConfig, BaseConfigContainer


def make_lock_state() -> dict[str, Any]:
    return {"lock": threading.Lock()}


def add_value(state: dict[str, Any], value: int) -> int:
    state["value"] = state.get("value", 0) + value
    return state["value"]


def multiply(state: dict[str, Any], factor: int) -> int:
    return state["value"] * factor


def extract(state: dict[str, Any], key: str) -> Any:
    return state[key]


def lock_and_report(state: dict[str, Any]) -> str:
    acquired = state["lock"].acquire(blocking=False)
    return "acquired" if acquired else "busy"


def slow_increment(state: dict[str, Any]) -> int:
    time.sleep(0.1)
    state["value"] = state.get("value", 0) + 1
    return state["value"]


def raise_error(state: dict[str, Any]) -> None:
    msg = "boom"
    raise ValueError(msg)


def set_value(state: dict[str, Any], value: int) -> None:
    state["value"] = value


class AddKernel(Kernel):
    config = BaseConfigContainer()

    def __call__(self, state: dict[str, Any], value: int) -> int:
        return state.get("value", 0) + value


class DoubleWorkflow(Workflow):
    config = BaseConfig()

    def __call__(self, state: dict[str, Any]) -> int:
        return state.get("value", 0) * 2
