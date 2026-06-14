from __future__ import annotations

import json
import urllib.request

from flowlet import (
    DashboardServer,
    DashboardState,
    LogManager,
    ProgressManager,
    Scheduler,
    SignalPool,
    TelemetryManager,
)
from flowlet.scheduler_helpers import add_values


def test_dashboard_state_snapshot_collects_runtime_data():
    progress = ProgressManager()
    progress.register_task("task1", "Process data", total=10)
    progress.update_progress("task1", current=4, status="running")

    telemetry = TelemetryManager()
    telemetry.metric("items", 4, unit="count")
    telemetry.checkpoint("loaded", run_id="r1", stage="load")
    telemetry.sample_system()
    telemetry.sample_process(worker_id="main")

    logs = LogManager()
    logs.info("started")

    signals = SignalPool()
    scheduler = Scheduler(signal_pool=signals, name="dash-scheduler")
    scheduler.schedule(add_values, name="add", args=(1,), kwargs={"y": 2}, lifecycle_signal="add_status")
    scheduler.run_until_idle()

    state = DashboardState(
        progress=progress,
        telemetry=telemetry,
        logs=logs,
        signals=signals,
        schedulers=[scheduler],
    )
    snapshot = state.snapshot()

    assert snapshot["summary"]["progress_running"] == 1
    assert snapshot["summary"]["actions_total"] == 1
    assert snapshot["progress"][0]["task_id"] == "task1"
    assert snapshot["signals"]
    assert snapshot["schedulers"][0]["actions"][0]["name"] == "add"
    assert snapshot["resources"]["system"] is not None
    assert snapshot["resources"]["processes"]
    assert snapshot["logs"][0]["message"] == "started"

    scheduler.close()
    telemetry.close()
    logs.close()


def test_dashboard_server_serves_html_and_snapshot():
    progress = ProgressManager()
    progress.register_task("task1", "Task", total=1)
    state = DashboardState(progress=progress)
    server = DashboardServer(state, port=0)

    try:
        url = server.start()
        with urllib.request.urlopen(f"{url}/api/snapshot", timeout=3) as response:
            payload = json.loads(response.read().decode("utf-8"))
        assert payload["progress"][0]["task_id"] == "task1"

        with urllib.request.urlopen(url, timeout=3) as response:
            html = response.read().decode("utf-8")
        assert "Flowlet Dashboard" in html
        assert "React.createElement" in html
    finally:
        server.close()
