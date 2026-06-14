"""Read-only HTTP dashboard server."""

# ruff: noqa: E501

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .state import DashboardState


class DashboardServer:
    """Small read-only web server for Flowlet dashboard snapshots."""

    def __init__(
        self,
        state: DashboardState,
        *,
        host: str = "127.0.0.1",
        port: int = 8765,
    ) -> None:
        self.state = state
        self.host = host
        self.port = port
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        if self._server is None:
            return f"http://{self.host}:{self.port}"
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def start(self) -> str:
        if self._server is not None:
            return self.url

        dashboard = self

        class Handler(_DashboardRequestHandler):
            dashboard_server = dashboard

        self._server = ThreadingHTTPServer((self.host, self.port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self.url

    def close(self) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        self._server = None
        self._thread = None

    def __enter__(self) -> DashboardServer:
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class _DashboardRequestHandler(BaseHTTPRequestHandler):
    dashboard_server: DashboardServer

    def do_GET(self) -> None:
        if self.path in {"/", "/index.html"}:
            self._send_text(_HTML, content_type="text/html; charset=utf-8")
            return
        if self.path == "/api/snapshot":
            self._send_json(self.dashboard_server.state.snapshot())
            return
        self.send_error(404, "Not Found")

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send_json(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=repr).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, text: str, *, content_type: str) -> None:
        body = text.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Flowlet Dashboard</title>
  <script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --text: #15171a;
      --muted: #667085;
      --line: #d9dee7;
      --blue: #2563eb;
      --green: #16815d;
      --red: #c24135;
      --amber: #a35b00;
      --violet: #6d28d9;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: 0;
    }
    header {
      height: 56px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 24px;
      border-bottom: 1px solid var(--line);
      background: rgba(255,255,255,.92);
      position: sticky;
      top: 0;
      z-index: 2;
    }
    h1 { font-size: 18px; margin: 0; font-weight: 700; }
    main { max-width: 1480px; margin: 0 auto; padding: 18px 24px 28px; }
    .muted { color: var(--muted); font-size: 12px; }
    .grid { display: grid; gap: 12px; }
    .summary { grid-template-columns: repeat(6, minmax(0, 1fr)); }
    .columns { grid-template-columns: 1.35fr 1fr; align-items: start; }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      min-width: 0;
    }
    .metric .value { font-size: 26px; font-weight: 750; margin-top: 4px; }
    .panel h2 { font-size: 14px; margin: 0 0 12px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { text-align: left; padding: 8px 6px; border-top: 1px solid #edf0f5; vertical-align: top; }
    th { color: var(--muted); font-weight: 650; font-size: 12px; }
    .pill {
      display: inline-flex;
      align-items: center;
      height: 22px;
      padding: 0 8px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 650;
      background: #eef2f7;
      color: #344054;
      white-space: nowrap;
    }
    .running { background: #e8f1ff; color: var(--blue); }
    .completed { background: #e9f8f2; color: var(--green); }
    .failed { background: #fff0ed; color: var(--red); }
    .pending { background: #f2f4f7; color: #475467; }
    .triggered { background: #f2ecff; color: var(--violet); }
    .bar { height: 7px; background: #eceff4; border-radius: 999px; overflow: hidden; min-width: 90px; }
    .bar > span { display: block; height: 100%; background: var(--blue); }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
    .scroll { max-height: 360px; overflow: auto; }
    .section { margin-top: 12px; }
    @media (max-width: 1100px) {
      .summary { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .columns { grid-template-columns: 1fr; }
    }
    @media (max-width: 720px) {
      header { padding: 0 14px; }
      main { padding: 14px; }
      .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
  </style>
</head>
<body>
  <div id="root"></div>
  <script>
    const e = React.createElement;
    const fmtTime = ts => ts ? new Date(ts * 1000).toLocaleTimeString() : "-";
    const statusClass = s => ["running","completed","failed","pending"].includes(s) ? s : "pending";
    const short = v => {
      if (v === null || v === undefined) return "";
      const s = typeof v === "string" ? v : JSON.stringify(v);
      return s.length > 80 ? s.slice(0, 77) + "..." : s;
    };
    function Pill({status, children}) {
      return e("span", {className: "pill " + statusClass(status)}, children || status);
    }
    function Metric({label, value}) {
      return e("div", {className: "panel metric"}, e("div", {className: "muted"}, label), e("div", {className: "value"}, value));
    }
    function ProgressTable({items}) {
      return e("div", {className: "panel scroll"}, e("h2", null, "任务进度"),
        e("table", null, e("thead", null, e("tr", null, ["Task","Status","Progress","Description"].map(h => e("th", {key:h}, h)))),
          e("tbody", null, items.map(t => {
            const pct = t.total ? Math.min(100, Math.round(t.current * 100 / t.total)) : 0;
            return e("tr", {key:t.task_id}, e("td", {className:"mono"}, t.task_id), e("td", null, e(Pill, {status:t.status})),
              e("td", null, e("div", {className:"bar"}, e("span", {style:{width:pct + "%"}})), e("div", {className:"muted"}, `${t.current}/${t.total || "?"}`)),
              e("td", null, t.description || ""));
          }))));
    }
    function Actions({schedulers}) {
      const rows = schedulers.flatMap(s => (s.actions || []).map(a => ({scheduler:s.name, ...a})));
      return e("div", {className:"panel scroll"}, e("h2", null, "Scheduler Actions"),
        e("table", null, e("thead", null, e("tr", null, ["Scheduler","Action","Backend","Status","Runs","Trigger"].map(h => e("th", {key:h}, h)))),
          e("tbody", null, rows.map(a => e("tr", {key:a.scheduler + a.name},
            e("td", {className:"mono"}, a.scheduler), e("td", {className:"mono"}, a.name), e("td", null, a.backend),
            e("td", null, e(Pill, {status:a.status})), e("td", null, `${a.runs}/${a.max_runs ?? "∞"}`),
            e("td", null, e("span", {className:"pill " + (a.triggered ? "triggered" : "pending")}, a.triggered ? "triggered" : "waiting")))))));
    }
    function Signals({items}) {
      return e("div", {className:"panel scroll"}, e("h2", null, "Signals"),
        e("table", null, e("thead", null, e("tr", null, ["Name","Status","Version","Value"].map(h => e("th", {key:h}, h)))),
          e("tbody", null, items.map(s => e("tr", {key:s.name}, e("td", {className:"mono"}, s.name),
            e("td", null, e(Pill, {status:s.status})), e("td", null, s.version), e("td", {className:"mono"}, short(s.value || s.error)))))));
    }
    function Resources({resources}) {
      const sys = resources.system || {};
      const processes = resources.processes || [];
      return e("div", {className:"panel scroll"}, e("h2", null, "资源占用"),
        e("div", {className:"grid summary"}, e(Metric,{label:"CPU %", value: sys.cpu_percent ?? "-"}), e(Metric,{label:"Memory %", value: sys.memory_percent ?? "-"})),
        e("table", {className:"section"}, e("thead", null, e("tr", null, ["Worker","PID","CPU %","RSS"].map(h => e("th", {key:h}, h)))),
          e("tbody", null, processes.map(p => e("tr", {key:(p.worker_id || "") + p.process_id},
            e("td", {className:"mono"}, p.worker_id || p.node_id || "main"), e("td", null, p.process_id),
            e("td", null, p.cpu_percent ?? "-"), e("td", null, p.memory_rss ? Math.round(p.memory_rss / 1048576) + " MB" : "-"))))));
    }
    function Events({logs, telemetry}) {
      const checkpoints = telemetry.filter(x => x.event_type === "checkpoint").slice(-20).reverse();
      return e("div", {className:"grid columns section"},
        e("div", {className:"panel scroll"}, e("h2", null, "Checkpoints"),
          e("table", null, e("tbody", null, checkpoints.map((c,i) => e("tr", {key:i}, e("td", null, fmtTime(c.timestamp)), e("td", {className:"mono"}, c.name), e("td", null, c.stage || ""))))),
        e("div", {className:"panel scroll"}, e("h2", null, "Logs"),
          e("table", null, e("tbody", null, logs.slice(-30).reverse().map((l,i) => e("tr", {key:i}, e("td", null, fmtTime(l.timestamp)), e("td", null, e(Pill,{status:l.level === "ERROR" ? "failed" : "pending"}, l.level)), e("td", null, l.message))))));
    }
    function App() {
      const [data, setData] = React.useState(null);
      React.useEffect(() => {
        let alive = true;
        const load = () => fetch("/api/snapshot").then(r => r.json()).then(d => alive && setData(d)).catch(() => {});
        load();
        const id = setInterval(load, 1000);
        return () => { alive = false; clearInterval(id); };
      }, []);
      if (!data) return e("main", null, e("div", {className:"panel"}, "Loading..."));
      const s = data.summary || {};
      return e(React.Fragment, null,
        e("header", null, e("h1", null, "Flowlet Dashboard"), e("div", {className:"muted"}, "Last update " + fmtTime(data.timestamp))),
        e("main", null,
          e("div", {className:"grid summary"}, e(Metric,{label:"Actions", value:s.actions_total || 0}), e(Metric,{label:"Running Actions", value:s.actions_running || 0}), e(Metric,{label:"Triggered", value:s.actions_triggered || 0}), e(Metric,{label:"Signals", value:s.signals_total || 0}), e(Metric,{label:"Progress", value:s.progress_total || 0}), e(Metric,{label:"Failures", value:(s.signals_failed || 0) + (s.progress_failed || 0)})),
          e("div", {className:"grid columns section"}, e(Actions,{schedulers:data.schedulers || []}), e(Signals,{items:data.signals || []})),
          e("div", {className:"grid columns section"}, e(ProgressTable,{items:data.progress || []}), e(Resources,{resources:data.resources || {}})),
          e(Events,{logs:data.logs || [], telemetry:data.telemetry || []})
        ));
    }
    ReactDOM.createRoot(document.getElementById("root")).render(e(App));
  </script>
</body>
</html>"""
