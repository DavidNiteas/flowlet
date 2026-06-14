"""Cross-process log collection utilities.

The design mirrors :mod:`flowlet.base.progress`: child workers use lightweight
proxies to send BaseConfig messages through multiprocessing or Ray queues, while
one manager in the driver process owns the canonical record store.
"""

from __future__ import annotations

import logging
import os
import threading
import time
import traceback
from contextlib import suppress
from copy import deepcopy
from multiprocessing import Queue as MPQueue
from queue import Empty
from typing import Any, Literal

from pydantic import Field

from ..config.base_config import BaseConfig

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class LogRecordMessage(BaseConfig):
    """Serializable log record transported from workers to the manager."""

    timestamp: float = 0.0
    level: LogLevel = "INFO"
    logger_name: str = "flowlet"
    message: str = ""
    process_id: int | None = None
    process_name: str | None = None
    thread_id: int | None = None
    thread_name: str | None = None
    node_id: str | None = None
    task_id: str | None = None
    run_id: str | None = None
    stage: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    exc_text: str | None = None


class BaseLogCollector:
    """Shared local API for log managers and proxies."""

    def __init__(self, max_records: int | None = None) -> None:
        self._records: list[LogRecordMessage] = []
        self._max_records = max_records
        self._lock = threading.RLock()

    def get_records(self) -> list[LogRecordMessage]:
        with self._lock:
            return deepcopy(self._records)

    def tail(self, n: int = 100) -> list[LogRecordMessage]:
        with self._lock:
            return deepcopy(self._records[-n:])

    def clear(self) -> None:
        with self._lock:
            self._records.clear()

    def log(
        self,
        level: LogLevel,
        message: str,
        *,
        logger_name: str = "flowlet",
        node_id: str | None = None,
        task_id: str | None = None,
        run_id: str | None = None,
        stage: str | None = None,
        metadata: dict[str, Any] | None = None,
        exc_info: BaseException | bool | None = None,
    ) -> None:
        exc_text = None
        if isinstance(exc_info, BaseException):
            exc_text = "".join(traceback.format_exception(type(exc_info), exc_info, exc_info.__traceback__))
        elif exc_info:
            exc_text = traceback.format_exc()
        record = LogRecordMessage(
            timestamp=time.time(),
            level=level,
            logger_name=logger_name,
            message=message,
            process_id=os.getpid(),
            process_name=None,
            thread_id=threading.get_ident(),
            thread_name=threading.current_thread().name,
            node_id=node_id,
            task_id=task_id,
            run_id=run_id,
            stage=stage,
            metadata=metadata or {},
            exc_text=exc_text,
        )
        self._append_local(record)
        self._send_record(record)

    def debug(self, message: str, **kwargs: Any) -> None:
        self.log("DEBUG", message, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        self.log("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        self.log("WARNING", message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        self.log("ERROR", message, **kwargs)

    def critical(self, message: str, **kwargs: Any) -> None:
        self.log("CRITICAL", message, **kwargs)

    def _append_local(self, record: LogRecordMessage) -> None:
        with self._lock:
            self._records.append(record)
            if self._max_records is not None and self._max_records >= 0:
                overflow = len(self._records) - self._max_records
                if overflow > 0:
                    del self._records[:overflow]

    def _send_record(self, record: LogRecordMessage) -> None:
        """Hook for proxies. Managers keep records locally only."""


class BaseLogProxy(BaseLogCollector):
    """Base class for cross-process log proxies."""

    def __init__(
        self,
        records_snapshot: list[LogRecordMessage] | None = None,
        max_records: int | None = None,
    ) -> None:
        super().__init__(max_records=max_records)
        if records_snapshot is not None:
            self._records = records_snapshot


class MPLogProxy(BaseLogProxy):
    """Multiprocessing log proxy."""

    def __init__(
        self,
        queue: MPQueue | None = None,
        records_snapshot: list[LogRecordMessage] | None = None,
        max_records: int | None = None,
    ) -> None:
        super().__init__(records_snapshot=records_snapshot, max_records=max_records)
        self._queue = queue

    def _send_record(self, record: LogRecordMessage) -> None:
        if self._queue is not None:
            self._queue.put(record)

    def __getstate__(self) -> dict[str, Any]:
        return {
            "records": [record.to_dict() for record in self._records],
            "max_records": self._max_records,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        BaseLogCollector.__init__(self, max_records=state.get("max_records"))
        self._records = [LogRecordMessage.from_dict(v) for v in state.get("records", [])]
        self._queue = None


class RayLogProxy(BaseLogProxy):
    """Ray log proxy using ray.util.queue.Queue."""

    def __init__(
        self,
        queue: Any | None = None,
        records_snapshot: list[LogRecordMessage] | None = None,
        max_records: int | None = None,
    ) -> None:
        super().__init__(records_snapshot=records_snapshot, max_records=max_records)
        self._queue = queue

    def _send_record(self, record: LogRecordMessage) -> None:
        if self._queue is not None:
            self._queue.put(record)

    def __getstate__(self) -> dict[str, Any]:
        return {
            "records": [record.to_dict() for record in self._records],
            "max_records": self._max_records,
            "queue": self._queue,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        BaseLogCollector.__init__(self, max_records=state.get("max_records"))
        self._records = [LogRecordMessage.from_dict(v) for v in state.get("records", [])]
        self._queue = state.get("queue")


class LogManager(BaseLogCollector):
    """Driver-side log record manager."""

    def __init__(self, max_records: int | None = None) -> None:
        super().__init__(max_records=max_records)
        self._mp_queue: MPQueue | None = None
        self._ray_queue: Any | None = None
        self._consumer_threads: list[threading.Thread] = []

    def get_mp_proxy(self) -> MPLogProxy:
        if self._mp_queue is None:
            self._mp_queue = MPQueue()
            thread = threading.Thread(target=self._consume_mp_loop, daemon=True)
            thread.start()
            self._consumer_threads.append(thread)
        with self._lock:
            snapshot = deepcopy(self._records)
        return MPLogProxy(self._mp_queue, snapshot, self._max_records)

    def get_ray_proxy(self) -> RayLogProxy:
        if self._ray_queue is None:
            try:
                from ray.util.queue import Queue as RayQueue
            except ImportError as exc:
                raise RuntimeError("Ray is not installed. Install ray to use get_ray_proxy().") from exc
            self._ray_queue = RayQueue()
            thread = threading.Thread(target=self._consume_ray_loop, daemon=True)
            thread.start()
            self._consumer_threads.append(thread)
        with self._lock:
            snapshot = deepcopy(self._records)
        return RayLogProxy(self._ray_queue, snapshot, self._max_records)

    def get_proxy(self) -> MPLogProxy:
        return self.get_mp_proxy()

    def to_jsonl(self, path: str | os.PathLike[str]) -> None:
        with open(path, "w", encoding="utf-8") as f:
            for record in self.get_records():
                f.write(record.model_dump_json() + "\n")

    def close(self) -> None:
        if self._mp_queue is not None:
            self._mp_queue.put(None)
            self._mp_queue = None
        if self._ray_queue is not None:
            with suppress(Exception):
                self._ray_queue.put(None)
            self._ray_queue = None
        for thread in self._consumer_threads:
            thread.join(timeout=2.0)
        self._consumer_threads.clear()

    def _consume_mp_loop(self) -> None:
        while True:
            try:
                msg = self._mp_queue.get(timeout=0.5)
            except Exception:
                continue
            if msg is None:
                break
            if isinstance(msg, LogRecordMessage):
                self._append_local(msg)

    def _consume_ray_loop(self) -> None:
        while True:
            try:
                msg = self._ray_queue.get(block=True, timeout=0.5)
            except Empty:
                continue
            except Exception:
                continue
            if msg is None:
                break
            if isinstance(msg, LogRecordMessage):
                self._append_local(msg)


class FlowletLogHandler(logging.Handler):
    """Standard logging handler that forwards records to a flowlet log collector."""

    def __init__(self, collector: BaseLogCollector, level: int = logging.NOTSET) -> None:
        super().__init__(level=level)
        self.collector = collector

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
            exc_text = None
            if record.exc_info:
                exc_text = "".join(traceback.format_exception(*record.exc_info))
            log_record = LogRecordMessage(
                timestamp=record.created,
                level=record.levelname,  # type: ignore[arg-type]
                logger_name=record.name,
                message=message,
                process_id=record.process,
                process_name=record.processName,
                thread_id=record.thread,
                thread_name=record.threadName,
                metadata={},
                exc_text=exc_text,
            )
            self.collector._append_local(log_record)
            self.collector._send_record(log_record)
        except Exception:
            self.handleError(record)


LogProxy = MPLogProxy
