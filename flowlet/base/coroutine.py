"""Coroutine execution helpers built on a private asyncio event loop thread."""

from __future__ import annotations

import asyncio
import inspect
import threading
from collections.abc import Awaitable, Callable, Iterable
from concurrent.futures import Future, wait
from typing import Any


class CoroutinePoolClosedError(RuntimeError):
    """Raised when submitting work to a closed CoroutinePool."""


class CoroutinePool:
    """Run asyncio tasks on an event loop owned by a dedicated thread.

    The pool gives synchronous code a Future-based API without touching the
    caller thread's event loop. It is intended for I/O concurrency, async edge
    backends, and lightweight async workflow wrappers.
    """

    def __init__(
        self,
        max_concurrency: int | None = None,
        name: str | None = None,
        daemon: bool = True,
    ) -> None:
        if max_concurrency is not None and max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1 or None")

        self.max_concurrency = max_concurrency
        self.name = name or "coroutine-pool"
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._closed = False
        self._pending: set[Future] = set()
        self._pending_lock = threading.Lock()
        self._semaphore: asyncio.Semaphore | None = None
        self._daemon = daemon
        self._start_loop_thread()

    @property
    def closed(self) -> bool:
        """Whether this pool has been closed."""
        return self._closed

    @property
    def loop_thread_id(self) -> int | None:
        """Identifier of the thread that owns the event loop."""
        if self._thread is None:
            return None
        return self._thread.ident

    def _start_loop_thread(self) -> None:
        self._thread = threading.Thread(target=self._run_loop, name=self.name, daemon=self._daemon)
        self._thread.start()
        self._ready.wait()

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        if self.max_concurrency is not None:
            self._semaphore = asyncio.Semaphore(self.max_concurrency)
        self._ready.set()
        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    def _check_open(self) -> asyncio.AbstractEventLoop:
        if self._closed or self._loop is None:
            raise CoroutinePoolClosedError("CoroutinePool has been closed")
        return self._loop

    def _track_future(self, future: Future) -> Future:
        with self._pending_lock:
            self._pending.add(future)

        def _discard(done: Future) -> None:
            with self._pending_lock:
                self._pending.discard(done)

        future.add_done_callback(_discard)
        return future

    async def _with_limit(self, awaitable: Awaitable[Any]) -> Any:
        started = False
        try:
            if self._semaphore is None:
                started = True
                return await awaitable
            async with self._semaphore:
                started = True
                return await awaitable
        except asyncio.CancelledError:
            if not started and inspect.iscoroutine(awaitable):
                awaitable.close()
            raise

    async def _call(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        blocking: bool,
    ) -> Any:
        if blocking:
            return await asyncio.to_thread(func, *args, **kwargs)
        result = func(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    def submit_awaitable(self, awaitable: Awaitable[Any]) -> Future:
        """Submit an awaitable object and return a concurrent Future."""
        try:
            loop = self._check_open()
        except Exception:
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            raise
        future = asyncio.run_coroutine_threadsafe(self._with_limit(awaitable), loop)

        def _close_unstarted(done: Future) -> None:
            if done.cancelled() and inspect.iscoroutine(awaitable):
                awaitable.close()

        future.add_done_callback(_close_unstarted)
        return self._track_future(future)

    def submit(self, func: Callable[..., Any], *args: Any, blocking: bool = False, **kwargs: Any) -> Future:
        """Submit a sync or async callable.

        Args:
            func: Callable to run in the pool event loop.
            *args: Positional callable arguments.
            blocking: If True, execute the callable through ``asyncio.to_thread``.
        **kwargs: Keyword callable arguments.
        """
        self._check_open()
        return self.submit_awaitable(self._call(func, args, kwargs, blocking))

    def map(
        self,
        func: Callable[[Any], Any],
        iterable: Iterable[Any],
        *,
        blocking: bool = False,
    ) -> list[Any]:
        """Apply a callable to all items and return results in input order."""
        futures = [self.submit(func, item, blocking=blocking) for item in iterable]
        return self.gather(futures)

    def gather(self, futures_or_awaitables: Iterable[Future | Awaitable[Any]]) -> list[Any]:
        """Wait for futures or awaitables and return results in input order."""
        futures: list[Future] = []
        for item in futures_or_awaitables:
            if isinstance(item, Future):
                futures.append(item)
            elif inspect.isawaitable(item):
                futures.append(self.submit_awaitable(item))
            else:
                raise TypeError(f"gather expects Future or awaitable, got {type(item).__name__}")
        return [future.result() for future in futures]

    def join(self, timeout: float | None = None) -> None:
        """Wait for all currently pending tasks and propagate the first error."""
        if threading.get_ident() == self.loop_thread_id:
            raise RuntimeError("CoroutinePool.join() cannot be called from its own event loop thread")
        with self._pending_lock:
            pending = list(self._pending)
        if not pending:
            return
        wait(pending, timeout=timeout)
        for future in pending:
            if future.done():
                future.result()

    def close(self, wait_tasks: bool = True) -> None:
        """Close the event loop thread."""
        if self._closed:
            return
        if wait_tasks:
            self.join()
        self._closed = True
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None and threading.get_ident() != self._thread.ident:
            self._thread.join()

    def kill(self) -> None:
        """Cancel pending tasks and stop the event loop thread."""
        if self._closed:
            return
        self._closed = True
        with self._pending_lock:
            pending = list(self._pending)
        for future in pending:
            future.cancel()
        if self._loop is not None:

            def _cancel_and_stop() -> None:
                for task in asyncio.all_tasks(self._loop):
                    task.cancel()
                self._loop.call_later(0.01, self._loop.stop)

            self._loop.call_soon_threadsafe(_cancel_and_stop)
        if self._thread is not None and threading.get_ident() != self._thread.ident:
            self._thread.join(timeout=1.0)

    def __enter__(self) -> CoroutinePool:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            self.close()
        else:
            self.kill()
