from __future__ import annotations

import asyncio
import time

import pytest
from flowlet.base import CoroutinePool, CoroutinePoolClosedError
from flowlet.parallel_unit import CoroutineParallelWorkflow, ParallelConfig


async def async_double(x: int) -> int:
    await asyncio.sleep(0.01)
    return x * 2


async def async_fail() -> None:
    await asyncio.sleep(0.01)
    msg = "async boom"
    raise ValueError(msg)


def sync_double(x: int) -> int:
    return x * 2


def blocking_sleep(value: int) -> int:
    time.sleep(0.02)
    return value


class TestCoroutinePool:
    def test_submit_async_callable(self):
        with CoroutinePool(max_concurrency=2) as pool:
            future = pool.submit(async_double, 21)
            assert future.result() == 42

    def test_submit_sync_callable(self):
        with CoroutinePool() as pool:
            future = pool.submit(sync_double, 21)
            assert future.result() == 42

    def test_gather_awaitables(self):
        with CoroutinePool(max_concurrency=2) as pool:
            results = pool.gather([async_double(1), async_double(2), async_double(3)])
            assert results == [2, 4, 6]

    def test_map_blocking_callable(self):
        with CoroutinePool(max_concurrency=2) as pool:
            assert pool.map(blocking_sleep, [1, 2, 3], blocking=True) == [1, 2, 3]

    def test_exception_propagates(self):
        with CoroutinePool() as pool:
            future = pool.submit_awaitable(async_fail())
            with pytest.raises(ValueError, match="async boom"):
                future.result()

    def test_submit_after_close_raises(self):
        pool = CoroutinePool()
        pool.close()
        with pytest.raises(CoroutinePoolClosedError):
            pool.submit(sync_double, 1)

    def test_max_concurrency_limits_runtime(self):
        async def sleeper(x: int) -> int:
            await asyncio.sleep(0.05)
            return x

        with CoroutinePool(max_concurrency=1) as serial_pool:
            start = time.perf_counter()
            assert serial_pool.gather([sleeper(1), sleeper(2)]) == [1, 2]
            serial_elapsed = time.perf_counter() - start

        with CoroutinePool(max_concurrency=2) as parallel_pool:
            start = time.perf_counter()
            assert parallel_pool.gather([sleeper(1), sleeper(2)]) == [1, 2]
            parallel_elapsed = time.perf_counter() - start

        assert serial_elapsed > parallel_elapsed


class TestCoroutineParallelWorkflow:
    def test_submit_and_fetch(self):
        workflow = CoroutineParallelWorkflow()
        future = workflow.submit(async_double, 21)
        assert workflow.fetch(future) == 42
        workflow.shutdown()

    def test_batch_operations(self):
        workflow = CoroutineParallelWorkflow(ParallelConfig(parallel_threshold=1))
        futures = workflow.submit_many(async_double, [1, 2, 3])
        assert workflow.gather(futures) == [2, 4, 6]
        assert workflow.map(async_double, [4, 5]) == [8, 10]
        workflow.shutdown()

    def test_sync_callable(self):
        with CoroutineParallelWorkflow() as workflow:
            assert workflow.map(sync_double, [1, 2, 3]) == [2, 4, 6]

    def test_context_manager_closes(self):
        with CoroutineParallelWorkflow() as workflow:
            assert workflow.fetch(workflow.submit(sync_double, 2)) == 4
        with pytest.raises(RuntimeError, match="Coroutine pool has been shutdown"):
            workflow.submit(sync_double, 2)
