"""Cross-process stdout/stderr stream collection utilities."""

from __future__ import annotations

import io
import os
import sys
import threading
import time
from contextlib import AbstractContextManager, suppress
from copy import deepcopy
from multiprocessing import Queue as MPQueue
from queue import Empty
from typing import Any, Literal, TextIO

from pydantic import Field

from ..config.base_config import BaseConfig

StreamName = Literal["stdout", "stderr"]
StreamMode = Literal["capture", "tee", "inherit", "silent"]


class StreamChunkMessage(BaseConfig):
    """Serializable stdout/stderr chunk."""

    timestamp: float = 0.0
    stream: StreamName = "stdout"
    text: str = ""
    source: str | None = None
    process_id: int | None = None
    thread_id: int | None = None
    node_id: str | None = None
    task_id: str | None = None
    run_id: str | None = None
    stage: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class BaseStreamCollector:
    """Shared local API for stream managers and proxies."""

    def __init__(self, max_chunks: int | None = None) -> None:
        self._chunks: list[StreamChunkMessage] = []
        self._max_chunks = max_chunks
        self._lock = threading.RLock()

    def get_chunks(self) -> list[StreamChunkMessage]:
        with self._lock:
            return deepcopy(self._chunks)

    def tail(self, n: int = 100) -> list[StreamChunkMessage]:
        with self._lock:
            return deepcopy(self._chunks[-n:])

    def clear(self) -> None:
        with self._lock:
            self._chunks.clear()

    def write_chunk(
        self,
        stream: StreamName,
        text: str,
        *,
        source: str | None = None,
        node_id: str | None = None,
        task_id: str | None = None,
        run_id: str | None = None,
        stage: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if text == "":
            return
        chunk = StreamChunkMessage(
            timestamp=time.time(),
            stream=stream,
            text=text,
            source=source,
            process_id=os.getpid(),
            thread_id=threading.get_ident(),
            node_id=node_id,
            task_id=task_id,
            run_id=run_id,
            stage=stage,
            metadata=metadata or {},
        )
        self._append_local(chunk)
        self._send_chunk(chunk)

    def capture(self, *, source: str | None = None, mode: StreamMode = "capture", **metadata: Any) -> StreamCapture:
        return StreamCapture(self, source=source, mode=mode, metadata=metadata)

    def _append_local(self, chunk: StreamChunkMessage) -> None:
        with self._lock:
            self._chunks.append(chunk)
            if self._max_chunks is not None and self._max_chunks >= 0:
                overflow = len(self._chunks) - self._max_chunks
                if overflow > 0:
                    del self._chunks[:overflow]

    def _send_chunk(self, chunk: StreamChunkMessage) -> None:
        """Hook for proxies. Managers keep chunks locally only."""


class _StreamWriter(io.TextIOBase):
    def __init__(
        self,
        collector: BaseStreamCollector,
        stream: StreamName,
        original: TextIO,
        *,
        source: str | None,
        mode: StreamMode,
        metadata: dict[str, Any],
    ) -> None:
        self.collector = collector
        self.stream = stream
        self.original = original
        self.source = source
        self.mode = mode
        self.metadata = metadata

    def writable(self) -> bool:
        return True

    def write(self, text: str) -> int:
        if self.mode != "inherit":
            self.collector.write_chunk(self.stream, text, source=self.source, **self.metadata)
        if self.mode in {"tee", "inherit"}:
            self.original.write(text)
        return len(text)

    def flush(self) -> None:
        if self.mode in {"tee", "inherit"}:
            self.original.flush()


class StreamCapture(AbstractContextManager["StreamCapture"]):
    """Context manager that captures Python-level sys.stdout/sys.stderr writes."""

    def __init__(
        self,
        collector: BaseStreamCollector,
        *,
        source: str | None = None,
        mode: StreamMode = "capture",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.collector = collector
        self.source = source
        self.mode = mode
        self.metadata = metadata or {}
        self._stdout: TextIO | None = None
        self._stderr: TextIO | None = None

    def __enter__(self) -> StreamCapture:
        if self.mode == "inherit":
            return self
        self._stdout = sys.stdout
        self._stderr = sys.stderr
        sys.stdout = _StreamWriter(
            self.collector,
            "stdout",
            self._stdout,
            source=self.source,
            mode=self.mode,
            metadata=self.metadata,
        )
        sys.stderr = _StreamWriter(
            self.collector,
            "stderr",
            self._stderr,
            source=self.source,
            mode=self.mode,
            metadata=self.metadata,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self._stdout is not None:
            sys.stdout = self._stdout
        if self._stderr is not None:
            sys.stderr = self._stderr
        return False


class BaseStreamProxy(BaseStreamCollector):
    def __init__(
        self,
        chunks_snapshot: list[StreamChunkMessage] | None = None,
        max_chunks: int | None = None,
    ) -> None:
        super().__init__(max_chunks=max_chunks)
        if chunks_snapshot is not None:
            self._chunks = chunks_snapshot


class MPStreamProxy(BaseStreamProxy):
    def __init__(
        self,
        queue: MPQueue | None = None,
        chunks_snapshot: list[StreamChunkMessage] | None = None,
        max_chunks: int | None = None,
    ) -> None:
        super().__init__(chunks_snapshot=chunks_snapshot, max_chunks=max_chunks)
        self._queue = queue

    def _send_chunk(self, chunk: StreamChunkMessage) -> None:
        if self._queue is not None:
            self._queue.put(chunk)

    def __getstate__(self) -> dict[str, Any]:
        return {
            "chunks": [chunk.to_dict() for chunk in self._chunks],
            "max_chunks": self._max_chunks,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        BaseStreamCollector.__init__(self, max_chunks=state.get("max_chunks"))
        self._chunks = [StreamChunkMessage.from_dict(v) for v in state.get("chunks", [])]
        self._queue = None


class RayStreamProxy(BaseStreamProxy):
    def __init__(
        self,
        queue: Any | None = None,
        chunks_snapshot: list[StreamChunkMessage] | None = None,
        max_chunks: int | None = None,
    ) -> None:
        super().__init__(chunks_snapshot=chunks_snapshot, max_chunks=max_chunks)
        self._queue = queue

    def _send_chunk(self, chunk: StreamChunkMessage) -> None:
        if self._queue is not None:
            self._queue.put(chunk)

    def __getstate__(self) -> dict[str, Any]:
        return {
            "chunks": [chunk.to_dict() for chunk in self._chunks],
            "max_chunks": self._max_chunks,
            "queue": self._queue,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        BaseStreamCollector.__init__(self, max_chunks=state.get("max_chunks"))
        self._chunks = [StreamChunkMessage.from_dict(v) for v in state.get("chunks", [])]
        self._queue = state.get("queue")


class StreamManager(BaseStreamCollector):
    def __init__(self, max_chunks: int | None = None) -> None:
        super().__init__(max_chunks=max_chunks)
        self._mp_queue: MPQueue | None = None
        self._ray_queue: Any | None = None
        self._consumer_threads: list[threading.Thread] = []

    def get_mp_proxy(self) -> MPStreamProxy:
        if self._mp_queue is None:
            self._mp_queue = MPQueue()
            thread = threading.Thread(target=self._consume_mp_loop, daemon=True)
            thread.start()
            self._consumer_threads.append(thread)
        with self._lock:
            snapshot = deepcopy(self._chunks)
        return MPStreamProxy(self._mp_queue, snapshot, self._max_chunks)

    def get_ray_proxy(self) -> RayStreamProxy:
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
            snapshot = deepcopy(self._chunks)
        return RayStreamProxy(self._ray_queue, snapshot, self._max_chunks)

    def get_proxy(self) -> MPStreamProxy:
        return self.get_mp_proxy()

    def to_text(self, path: str | os.PathLike[str], *, stream: StreamName | None = None) -> None:
        chunks = self.get_chunks()
        with open(path, "w", encoding="utf-8") as f:
            for chunk in chunks:
                if stream is None or chunk.stream == stream:
                    f.write(chunk.text)

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
            if isinstance(msg, StreamChunkMessage):
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
            if isinstance(msg, StreamChunkMessage):
                self._append_local(msg)


StreamProxy = MPStreamProxy
