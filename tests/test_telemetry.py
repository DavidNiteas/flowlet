from __future__ import annotations

import os
import time

import pytest
from flowlet.base.telemetry import RayTelemetryProxy, TelemetryManager


def _wait_until(predicate, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


@pytest.fixture
def ray_cluster(monkeypatch):
    ray = pytest.importorskip("ray")
    monkeypatch.setenv("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")
    os.environ.setdefault("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")
    if ray.is_initialized():
        ray.shutdown()
    _wait_until(lambda: not ray.is_initialized(), timeout=5.0)
    ray.init(num_cpus=2, include_dashboard=False, log_to_driver=False)
    try:
        yield ray
    finally:
        ray.shutdown()
        _wait_until(lambda: not ray.is_initialized(), timeout=5.0)


def test_telemetry_manager_metrics_checkpoints_and_resource_snapshots(tmp_path):
    manager = TelemetryManager()
    manager.metric("workers", 2, unit="count")
    manager.checkpoint("started", run_id="r1", stage="load")
    system = manager.sample_system()
    process = manager.sample_process(worker_id="main", run_id="r1")

    events = manager.get_events()
    assert [event.event_type for event in events[:2]] == ["metric", "checkpoint"]
    assert system.timestamp > 0
    assert process.process_id == os.getpid()

    output = tmp_path / "telemetry.jsonl"
    manager.to_jsonl(output)
    assert output.exists()
    assert "workers" in output.read_text()


def test_telemetry_sampler_threads_collect_events():
    manager = TelemetryManager()
    manager.start_system_sampler(interval=0.05)
    manager.start_process_sampler(interval=0.05, worker_id="main")
    assert _wait_until(lambda: len(manager.get_events()) >= 2, timeout=2.0)
    manager.close()
    assert any(event.event_type == "system_resource" for event in manager.get_events())
    assert any(event.event_type == "process_resource" for event in manager.get_events())


def test_ray_telemetry_proxy_roundtrip(ray_cluster):
    ray = ray_cluster
    manager = TelemetryManager()
    proxy = manager.get_ray_proxy()

    @ray.remote
    def worker(telemetry):
        assert isinstance(telemetry, RayTelemetryProxy)
        telemetry.metric("ray_metric", 7, worker_id="ray-worker")
        telemetry.checkpoint("ray_checkpoint", worker_id="ray-worker")
        telemetry.sample_process(worker_id="ray-worker")
        return "ok"

    assert ray.get(worker.remote(proxy)) == "ok"
    assert _wait_until(lambda: any(event.name == "ray_metric" for event in manager.get_events()))
    assert _wait_until(lambda: any(event.name == "ray_checkpoint" for event in manager.get_events()))
    assert _wait_until(lambda: any(event.event_type == "process_resource" for event in manager.get_events()))
    manager.close()
