#!/usr/bin/env python3
"""
Market Data Downloader -- browser app
=====================================

The download window (eod_gui.py) as a local web page: same sections, choices, defaults and messages, opened in
the browser. The logic is shared with the Tk window through eod_app.py.

    python eod_web.py                 # starts http://127.0.0.1:8790/ and opens the browser
    python eod_web.py --port 0        # any free port
    python eod_web.py --no-browser

In the portable bundle this is EOD_Market_Data.exe. The console window shows the address and the log; closing
it stops the app. Port and browser opening default to [web] in eod_config.ini.

Security: the server listens on 127.0.0.1 only, rejects requests whose Host header is not this machine, and
every /api call must carry the random per-start token that is written into the page, so other web sites open
in the same browser cannot drive it.
"""

import argparse
import base64
import json
import logging
import os
import secrets
import socket
import string
import sys
import tempfile
import threading
import webbrowser
from collections import deque
from datetime import date
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import app_paths
import eod_app as appcore
import eod_downloader as core

WEB_DIR = app_paths.RES_DIR / "web"
STATIC_TYPES = {".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8",
                ".css": "text/css; charset=utf-8", ".svg": "image/svg+xml", ".ico": "image/x-icon"}
MAX_BODY = 50 * 1024 * 1024       # ticker-list uploads
LOG_KEEP = 5000                   # log lines kept for the page (a reloaded page gets these back)


class LogBuffer(logging.Handler):
    """Numbered log lines for the page to poll (same format as the Tk window's log)."""

    def __init__(self):
        super().__init__()
        self.setFormatter(appcore.LOG_FORMAT)
        self.lines = deque(maxlen=LOG_KEEP)
        self.seq = 0
        self.lock_ = threading.Lock()

    def emit(self, record):
        try:
            self.add(self.format(record))
        except Exception:
            pass

    def add(self, line: str):
        with self.lock_:
            self.seq += 1
            self.lines.append((self.seq, line))

    def since(self, n: int):
        with self.lock_:
            return [line for s, line in self.lines if s > n], self.seq


def default_settings() -> dict:
    paths = appcore.default_output_paths()
    return {"markets": [ex for ex in core.SUPPORTED_EXCHANGES if ex in appcore.DEFAULT_MARKETS], "all": True, "selected": {},
            "start": date.today().isoformat(), "end": date.today().isoformat(),      # quick range 1D
            "date_preset": appcore.DEFAULT_PRESET,
            "layout": "none", "interval": "daily", "metastock_ascii": True, "bhavcopy": True,
            "bse_bhavcopy": False, "bse_adjust": True, "us_source_alpaca": True, "nse_source_official": True,
            "output": paths["daily"], "output_hourly": paths["hourly"]}


def merge_settings(saved) -> dict:
    """The saved settings over the defaults, read the way the Tk window reads them (a saved 'today' end
    date stays today; unknown or broken values keep the default)."""
    s = default_settings()
    if not isinstance(saved, dict):
        return s
    if isinstance(saved.get("markets"), list):
        s["markets"] = [ex for ex in core.SUPPORTED_EXCHANGES if ex in saved["markets"]]
    if isinstance(saved.get("selected"), dict):
        s["selected"] = {str(k): [v[0] if len(v) > 0 else "", v[1] if len(v) > 1 else None]
                         for k, v in saved["selected"].items() if isinstance(v, (list, tuple))}
    for k in ("all", "metastock_ascii", "bhavcopy", "bse_bhavcopy", "bse_adjust", "us_source_alpaca",
              "nse_source_official"):
        if k in saved:
            s[k] = bool(saved[k])
    try:
        start, end, preset = appcore.saved_dates(saved)     # a saved quick range is recomputed from today
        s["start"], s["end"], s["date_preset"] = start.isoformat(), end.isoformat(), preset
    except Exception:
        pass
    if saved.get("layout") in {v for _t, v in appcore.LAYOUTS}:
        s["layout"] = saved["layout"]
    if saved.get("interval") in ("daily", "hourly"):
        s["interval"] = saved["interval"]
    for k in ("output", "output_hourly"):
        if saved.get(k):
            s[k] = str(saved[k])
    return s


def settings_to_save(form: dict) -> dict:
    """The page's form -> the gui_settings.json dict (same keys as the Tk window)."""
    s = merge_settings(form)
    s["end"] = form.get("end", s["end"])
    s["start"] = form.get("start", s["start"])
    s["date_preset"] = form.get("date_preset", "") if appcore.preset_range(form.get("date_preset", "")) else ""
    s["end_is_today"] = s["end"] == date.today().isoformat()   # so a saved "today" doesn't go stale
    return s


class App:
    """Server-side state: ticker lists, the one running job, log and messages for the page."""

    def __init__(self, cache_dir: Path = None, settings_path: Path = None):
        self.cache_dir = Path(cache_dir or appcore.CACHE_DIR)
        self.settings_path = Path(settings_path or self.cache_dir / "gui_settings.json")
        appcore.ensure_cache_dir(self.cache_dir)
        self.lock = threading.Lock()
        self.all_tickers = appcore.load_cached_tickers(self.cache_dir)
        self.tickers_version = 0
        self.logbuf = LogBuffer()
        core.log.addHandler(self.logbuf)
        core.log.setLevel(logging.INFO)
        self.runner = appcore.Runner(on_progress=self._on_progress, on_status=self._on_status,
                                     on_finished=self._on_finished, cache_dir=self.cache_dir)
        self.running = False
        self.refreshing = False
        self.status = "Ready."
        self.progress = {"done": 0, "total": 0}
        self.messages = deque(maxlen=50)    # dialogs for the page: {id, kind, title, message}
        self.msg_seq = 0
        self.thread = None

    def close(self):
        core.log.removeHandler(self.logbuf)

    # ------------------------------------------------------------------------------------------ messages --
    def _message(self, kind, title, message):
        with self.lock:
            self.msg_seq += 1
            self.messages.append({"id": self.msg_seq, "kind": kind, "title": title, "message": message})

    def _on_progress(self, done, total, failed, no_data):
        with self.lock:
            self.progress = {"done": done, "total": total}
            self.status = appcore.progress_text(done, total, failed, no_data)

    def _on_status(self, text):
        with self.lock:
            self.status = text

    def _on_finished(self, status, message):
        title, kind, text = {"success": ("Download complete", "info", "Done."),
                             "cancelled": ("Download cancelled", "info", "Cancelled.")}.get(
            status, ("Download failed", "error", "Failed -- see log."))
        with self.lock:
            self.running = False
            self.status = text
        self._message(kind, title, message)

    # ------------------------------------------------------------------------------------------- queries --
    def state(self) -> dict:
        return {"settings": merge_settings(appcore.load_settings(self.settings_path)),
                "defaults": default_settings(),
                "exchanges": list(core.SUPPORTED_EXCHANGES),
                "hourly_exchanges": ["NSE", "NASDAQ", "NYSE"],
                "layouts": appcore.LAYOUTS,
                "presets": [[label, d.isoformat()] for label, d in appcore.date_presets()],
                "today": date.today().isoformat(),
                "hourly_note": appcore.HOURLY_NOTE,
                "counts": {ex: len(t) for ex, t in self.all_tickers.items()},
                "config_file": str(app_paths.APP_DIR / app_paths.CONFIG_NAME),
                **self.poll(0, 0)}

    def poll(self, log_since: int, msg_since: int) -> dict:
        lines, seq = self.logbuf.since(log_since)
        with self.lock:
            return {"log": lines, "log_seq": seq, "running": self.running, "refreshing": self.refreshing,
                    "status": self.status, "progress": dict(self.progress),
                    "messages": [m for m in self.messages if m["id"] > msg_since], "msg_seq": self.msg_seq,
                    "tickers_version": self.tickers_version}

    def search(self, typed: str, markets) -> dict:
        markets = [m for m in markets if m in self.all_tickers]
        matches = appcore.search(self.all_tickers, markets, typed)
        return {"matches": [[s, n, ex, appcore.format_entry(s, n, ex)] for s, n, ex in matches],
                "loaded": appcore.ticker_count(self.all_tickers, markets)}

    def exchange_of(self, ticker: str):
        for ex in core.SUPPORTED_EXCHANGES:
            if ticker in self.all_tickers.get(ex, {}):
                return ex
        return None

    # ------------------------------------------------------------------------------------------- actions --
    def save_settings(self, form: dict):
        appcore.save_settings(settings_to_save(form), self.settings_path)

    def refresh_tickers(self, markets):
        if not markets:
            return {"ok": False, "kind": "warning", "title": "No market selected",
                    "message": "Please select at least one market (NASDAQ, NYSE, BSE, NSE)."}
        with self.lock:
            if self.refreshing or self.running:
                return {"ok": False, "kind": "warning", "title": "Busy",
                        "message": "Wait for the current download or refresh to finish."}
            self.refreshing = True

        def work():
            try:
                got, bse_empty = appcore.refresh_tickers(markets, self.cache_dir)
                with self.lock:
                    self.all_tickers.update(got)
                    self.tickers_version += 1
                if bse_empty:
                    self._message("info", "Couldn't fetch the BSE list", appcore.BSE_EMPTY_MESSAGE)
            except Exception as e:
                self._message("error", "Failed to fetch ticker list", str(e))
            finally:
                with self.lock:
                    self.refreshing = False

        threading.Thread(target=work, daemon=True).start()
        return {"ok": True}

    def load_tickers(self, exchange: str, filename: str, data: bytes):
        if exchange not in core.SUPPORTED_EXCHANGES:
            return {"ok": False, "kind": "warning", "title": "Pick one market first",
                    "message": "Check exactly one market checkbox above (the one this file's tickers belong "
                               "to -- e.g. BSE), then click 'Load tickers from file...' again."}
        suffix = Path(filename or "tickers.csv").suffix or ".csv"
        fd, tmp = tempfile.mkstemp(suffix=suffix)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            df = core.load_tickers_from_file(tmp, exchange)
        except Exception as e:
            return {"ok": False, "kind": "error", "title": "Couldn't load file", "message": str(e)}
        finally:
            try:
                os.remove(tmp)
            except OSError:
                pass
        with self.lock:
            self.all_tickers[exchange] = dict(zip(df["Symbol"], df["Name"]))
            self.tickers_version += 1
        return {"ok": True, "kind": "info", "title": "Loaded",
                "message": f"Loaded {len(df):,} {exchange} tickers from {Path(filename or tmp).name}"}

    def start_run(self, form: dict):
        markets = [m for m in form.get("markets", []) if m in core.SUPPORTED_EXCHANGES]
        all_mode = bool(form.get("all", True))
        selected = {str(s): (v[0] if v else "", v[1] if len(v) > 1 else None)
                    for s, v in (form.get("selected") or {}).items()}
        interval = form.get("interval", "daily") if form.get("interval") in ("daily", "hourly") else "daily"
        defaults = appcore.default_output_paths()
        output = form.get("output_hourly" if interval == "hourly" else "output", "")
        with self.lock:
            if self.running:
                return {"ok": False, "kind": "warning", "title": "Download in progress",
                        "message": "A download is already running. Wait for it to finish or cancel it."}
        problem, output_dir = appcore.validate_run(markets, all_mode, selected, form.get("start"),
                                                   form.get("end"), output, defaults[interval], interval)
        if problem:
            kind, title, msg = problem
            return {"ok": False, "kind": kind, "title": title, "message": msg}
        with self.lock:
            if self.running:
                return {"ok": False, "kind": "warning", "title": "Download in progress",
                        "message": "A download is already running. Wait for it to finish or cancel it."}
            self.running = True
            self.status = "Starting..."
            self.progress = {"done": 0, "total": 0}
        self.save_settings(form)
        self.runner.cancel_event.clear()

        if all_mode:
            ticker_selection, exchange_snapshot = "ALL", {}
        else:
            ticker_selection = list(selected)
            exchange_snapshot = {s: ex for s, (_n, ex) in selected.items()}
        start_s, end_s = form["start"], form["end"]
        layout = form.get("layout", "none")
        metastock_ascii = bool(form.get("metastock_ascii", True))
        sources = appcore.Runner.sources(bool(form.get("us_source_alpaca", True)),
                                         bool(form.get("nse_source_official", True)))
        self.runner.write_bhavcopy = bool(form.get("bhavcopy", True))
        ticker_desc = "ALL" if all_mode else f"{len(ticker_selection)} selected"
        self.logbuf.add(
            f"--- Starting run | markets={markets or '(n/a, specific tickers)'} | "
            f"tickers={ticker_desc} | {start_s} -> {end_s} | interval={interval} | layout={layout} | "
            f"metastock_ascii={metastock_ascii} | "
            + (f"sources: NASDAQ/NYSE={sources['NASDAQ']}, NSE={sources['NSE']}, others=yahoo | "
               if interval == "daily" else "source: yahoo | ")
            + f"output={output_dir} ---")
        if interval == "hourly":
            target, args = self.runner.run_hourly, (markets, ticker_selection, exchange_snapshot, start_s, end_s,
                                                    output_dir, metastock_ascii)
        else:
            target, args = self.runner.run_daily, (markets, ticker_selection, exchange_snapshot, start_s, end_s,
                                                   output_dir, layout, metastock_ascii,
                                                   "bhavcopy" if form.get("bse_bhavcopy") else "yahoo",
                                                   bool(form.get("bse_adjust", True)), sources)
        self.thread = threading.Thread(target=self._run_guarded, args=(target, args), daemon=True)
        self.thread.start()
        return {"ok": True}

    def _run_guarded(self, target, args):
        try:
            target(*args)
        except Exception as e:                       # the Runner reports its own errors; this is a last resort
            self._on_finished("error", str(e))
        finally:
            with self.lock:
                self.running = False

    def cancel(self):
        with self.lock:
            if not self.running:
                return {"ok": False}
            self.status = "Cancelling -- letting in-flight downloads finish..."
        self.runner.cancel_event.set()
        self.logbuf.add("--- Cancel requested; stopping queued downloads, letting in-flight ones finish ---")
        return {"ok": True}

    @staticmethod
    def open_folder(path: str):
        p = Path((path or "").strip() or ".")
        if not p.exists():
            return {"ok": False, "kind": "info", "title": "Folder not created yet",
                    "message": "This folder doesn't exist yet -- it's created on the first run."}
        appcore.open_folder(p)
        return {"ok": True}

    @staticmethod
    def listdir(path: str):
        """Folder picker: sub-folders of `path` (or of its nearest existing parent); no path = the drives."""
        roots = ([f"{d}:\\" for d in string.ascii_uppercase if os.path.exists(f"{d}:\\")]
                 if sys.platform.startswith("win") else ["/"])
        if not (path or "").strip():
            return {"path": "", "parent": None, "dirs": roots, "roots": roots, "home": str(Path.home())}
        p = Path(path).expanduser()
        while not p.is_dir() and p.parent != p:
            p = p.parent
        if not p.is_dir():
            return {"path": "", "parent": None, "dirs": roots, "roots": roots, "home": str(Path.home())}
        try:
            dirs = sorted((c.name for c in p.iterdir() if c.is_dir() and not c.name.startswith(("$", "."))),
                          key=str.lower)
        except OSError:
            dirs = []
        parent = str(p.parent) if p.parent != p else ""
        return {"path": str(p), "parent": parent, "dirs": dirs, "roots": roots, "home": str(Path.home())}


def make_handler(app: App, token: str, server_ref: dict):
    class Handler(BaseHTTPRequestHandler):
        server_version = "EODMarketData"

        def log_message(self, fmt, *args):      # keep the console for the download log
            pass

        # ------------------------------------------------------------------------------------- helpers --
        def _host_ok(self) -> bool:
            host = (self.headers.get("Host") or "").rsplit(":", 1)[0].strip("[]").lower()
            return host in ("127.0.0.1", "localhost")

        def _send(self, code, body: bytes, ctype="application/json; charset=utf-8"):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "DENY")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, obj, code=200):
            self._send(code, json.dumps(obj).encode("utf-8"))

        def _body(self) -> dict:
            n = int(self.headers.get("Content-Length") or 0)
            if n > MAX_BODY:
                raise ValueError("request too large")
            raw = self.rfile.read(n) if n else b""
            return json.loads(raw.decode("utf-8")) if raw else {}

        def _guard(self) -> bool:
            if not self._host_ok():
                self._send(HTTPStatus.FORBIDDEN, b"forbidden", "text/plain")
                return False
            if self.path.startswith("/api/") and not secrets.compare_digest(self.headers.get("X-Token", ""), token):
                self._json({"error": "bad token -- reload the page"}, HTTPStatus.FORBIDDEN)
                return False
            return True

        # ------------------------------------------------------------------------------------------ GET --
        def do_GET(self):
            if not self._guard():
                return
            url = urlparse(self.path)
            q = {k: v[-1] for k, v in parse_qs(url.query).items()}
            try:
                if url.path == "/api/state":
                    return self._json(app.state())
                if url.path == "/api/poll":
                    return self._json(app.poll(int(q.get("log", 0)), int(q.get("msg", 0))))
                if url.path == "/api/search":
                    markets = [m for m in q.get("markets", "").split(",") if m]
                    return self._json(app.search(q.get("q", ""), markets))
                if url.path == "/api/listdir":
                    return self._json(app.listdir(q.get("path", "")))
                if url.path.startswith("/api/"):
                    return self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)
                return self._static(url.path)
            except Exception as e:
                return self._json({"error": str(e)}, HTTPStatus.INTERNAL_SERVER_ERROR)

        def _static(self, path):
            name = "index.html" if path in ("/", "") else path.lstrip("/")
            f = (WEB_DIR / name).resolve()
            if WEB_DIR.resolve() not in f.parents or not f.is_file() or f.suffix not in STATIC_TYPES:
                return self._send(HTTPStatus.NOT_FOUND, b"not found", "text/plain")
            body = f.read_bytes()
            if f.name == "index.html":
                body = body.replace(b"__EOD_TOKEN__", token.encode("ascii"))
            self._send(200, body, STATIC_TYPES[f.suffix])

        # ----------------------------------------------------------------------------------------- POST --
        def do_POST(self):
            if not self._guard():
                return
            path = urlparse(self.path).path
            try:
                body = self._body()
                if path == "/api/settings":
                    app.save_settings(body)
                    return self._json({"ok": True})
                if path == "/api/run":
                    return self._json(app.start_run(body))
                if path == "/api/cancel":
                    return self._json(app.cancel())
                if path == "/api/refresh-tickers":
                    return self._json(app.refresh_tickers(body.get("markets") or []))
                if path == "/api/load-tickers":
                    data = base64.b64decode(body.get("data", ""))
                    return self._json(app.load_tickers(body.get("exchange", ""), body.get("filename", ""), data))
                if path == "/api/open-folder":
                    return self._json(app.open_folder(body.get("path", "")))
                if path == "/api/shutdown":
                    self._json({"ok": True})
                    srv = server_ref.get("server")
                    if srv is not None:
                        app.runner.cancel_event.set()
                        threading.Thread(target=srv.shutdown, daemon=True).start()
                    return
                return self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)
            except Exception as e:
                return self._json({"error": str(e)}, HTTPStatus.BAD_REQUEST)

    return Handler


class LocalServer(ThreadingHTTPServer):
    """Binds the port exclusively. Python's HTTPServer sets SO_REUSEADDR, which on Windows lets a second server
    bind a port that is already in use (the swingbt scan server uses 8765), so both would answer on it."""
    allow_reuse_address = not sys.platform.startswith("win")
    daemon_threads = True

    def server_bind(self):
        if sys.platform.startswith("win") and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        super().server_bind()


def make_server(port: int = None, app: App = None, token: str = None):
    """(server, app, token). port 0 = any free port; a busy port also falls back to a free one."""
    app = app or App()
    token = token or secrets.token_urlsafe(24)
    ref = {}
    handler = make_handler(app, token, ref)
    port = app_paths.web_port() if port is None else port
    try:
        server = LocalServer(("127.0.0.1", port), handler)
    except OSError:
        core.log.warning(f"Port {port} is in use (another copy already running?) -- using a free port instead.")
        server = LocalServer(("127.0.0.1", 0), handler)
    server.daemon_threads = True
    ref["server"] = server
    return server, app, token


def main():
    ap = argparse.ArgumentParser(description="EOD market data downloader in the browser.")
    ap.add_argument("--port", type=int, default=None, help="port (default: [web] port in eod_config.ini, 8790)")
    ap.add_argument("--no-browser", action="store_true", help="don't open the browser")
    args = ap.parse_args()
    if core.yf is None:
        print("yfinance is not installed. Run: pip install yfinance pandas requests")
        sys.exit(1)
    server, app, _token = make_server(args.port)
    url = f"http://127.0.0.1:{server.server_port}/"
    print("=" * 72)
    print(f"  EOD Market Data is running at {url}")
    print("  Keep this window open while you use the page; close it to stop the app.")
    print(f"  Settings file: {app_paths.APP_DIR / app_paths.CONFIG_NAME}")
    print("=" * 72, flush=True)
    if not args.no_browser and app_paths.web_open_browser():
        threading.Timer(0.6, webbrowser.open, [url]).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        app.runner.cancel_event.set()
        server.server_close()
        app.close()


if __name__ == "__main__":
    main()
