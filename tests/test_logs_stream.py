from __future__ import annotations

import logging
import os
import time

import pytest
from flowlet.base.logs import FlowletLogHandler, LogManager, RayLogProxy
from flowlet.base.stream import RayStreamProxy, StreamManager


@pytest.fixture
def ray_cluster(monkeypatch):
    ray = pytest.importorskip("ray")
    monkeypatch.setenv("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")
    os.environ.setdefault("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")
    if ray.is_initialized():
        ray.shutdown()
    ray.init(num_cpus=2, include_dashboard=False, log_to_driver=False)
    try:
        yield ray
    finally:
        ray.shutdown()


def _wait_until(predicate, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def test_log_manager_local_and_logging_handler():
    manager = LogManager()
    manager.info("hello", run_id="r1", stage="load")

    logger = logging.getLogger("flowlet-test-log-manager")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.INFO)
    logger.addHandler(FlowletLogHandler(manager))
    logger.info("from std logging")

    records = manager.get_records()
    assert [record.message for record in records] == ["hello", "from std logging"]
    assert records[0].run_id == "r1"
    assert records[0].stage == "load"


def test_stream_manager_capture_stdout_stderr():
    manager = StreamManager()
    with manager.capture(source="unit", mode="capture"):
        print("hello stdout")
        print("hello stderr", file=__import__("sys").stderr)

    chunks = manager.get_chunks()
    assert any(chunk.stream == "stdout" and "hello stdout" in chunk.text for chunk in chunks)
    assert any(chunk.stream == "stderr" and "hello stderr" in chunk.text for chunk in chunks)
    assert all(chunk.source == "unit" for chunk in chunks)


def test_ray_log_and_stream_proxies_roundtrip(ray_cluster):
    ray = ray_cluster
    log_manager = LogManager()
    stream_manager = StreamManager()
    log_proxy = log_manager.get_ray_proxy()
    stream_proxy = stream_manager.get_ray_proxy()

    @ray.remote
    def worker(logs, streams):
        assert isinstance(logs, RayLogProxy)
        assert isinstance(streams, RayStreamProxy)
        logs.info("ray log", run_id="ray-run")
        with streams.capture(source="ray-worker", mode="capture"):
            print("ray stdout")
        return "ok"

    assert ray.get(worker.remote(log_proxy, stream_proxy)) == "ok"
    assert _wait_until(lambda: any(record.message == "ray log" for record in log_manager.get_records()))
    assert _wait_until(lambda: any("ray stdout" in chunk.text for chunk in stream_manager.get_chunks()))
    log_manager.close()
    stream_manager.close()


def test_ray_proxy_requires_ray_queue_serialization(ray_cluster):
    ray = ray_cluster
    manager = LogManager()
    proxy = manager.get_ray_proxy()
    restored = ray.get(ray.put(proxy))
    assert isinstance(restored, RayLogProxy)
    restored.warning("restored warning")
    assert _wait_until(lambda: any(record.message == "restored warning" for record in manager.get_records()))
    manager.close()
