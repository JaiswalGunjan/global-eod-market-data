"""Offline tests for the browser app (eod_web.py + eod_app.py), the config file (app_paths.py) and the Alpaca ->
Yahoo fallback. The server runs on a free port in a thread; network calls are stubbed as in test_sources.

    venv\\Scripts\\python.exe -m unittest discover -s tests -v
"""
import base64
import http.client
import importlib
import json
import logging
import sys
import threading
import time
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import app_paths                   # noqa: E402
import eod_downloader as core      # noqa: E402
import eod_web                     # noqa: E402
import hourly_downloader as hourly  # noqa: E402
import test_sources as ts          # noqa: E402  (synthetic Alpaca / NSE / Yahoo data)

TOKEN = "test-token"


class WebTests(unittest.TestCase):
    def setUp(self):
        self._td = ts.tmpdir()
        self.tmp = Path(self._td.name)
        self.out = self.tmp / "out"
        ts.make_alpaca(self.out / "NYSE")
        ts.make_nse(self.out / "NSE", self.tmp / "unused.csv")
        ts.nb.update_corporate_actions(self.out / "NSE" / "nse_ca", download=False)
        ts.nb.build(self.out / "NSE")
        raw = self.out / "LSE" / "yahoo_adjusted"
        raw.mkdir(parents=True)
        df = ts.yf_frame(ts.ALL, ts.series(ts.ALL, 120)).reset_index()[["Date", "Open", "High", "Low", "Close",
                                                                         "Volume"]]
        df["Ticker"] = "VOD"
        df.to_csv(raw / "LSE.VOD.csv", index=False)
        self.cache = self.tmp / "cache"
        self.cache.mkdir()
        nse = [("TCS", "Tata Consultancy Services"), ("INFY", "Infosys"), ("ATCS", "Another TCS-ish Co"),
               ("RELIANCE", "Reliance Industries")] + [(f"S{i:03d}", f"Stock {i}") for i in range(400)]
        pd.DataFrame(nse, columns=["Symbol", "Name"]).assign(Exchange="NSE").to_csv(
            self.cache / "nse_tickers.csv", index=False)

        self.updates, self.downloads, self.hourly_calls = [], [], []
        self.update_error, self.cancel_in_update, self.block = None, False, None
        self.patches = [mock.patch.object(core, "update_source", self.fake_update),
                        mock.patch.object(core, "download_all", self.fake_download),
                        mock.patch.object(hourly, "download_all_hourly", self.fake_hourly_download),
                        mock.patch.object(hourly, "build_hour_files", self.fake_hour_files)]
        for p in self.patches:
            p.start()
        self.app = eod_web.App(cache_dir=self.cache, settings_path=self.tmp / "gui_settings.json")
        self.server, _, _ = eod_web.make_server(0, app=self.app, token=TOKEN)
        self.port = self.server.server_port
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.app.runner.cancel_event.set()
        if self.block is not None:
            self.block.set()
        if self.app.thread is not None:
            self.app.thread.join(10)
        self.server.shutdown()
        self.server.server_close()
        self.app.close()
        for p in self.patches:
            p.stop()
        ts.drop_file_handlers(self.tmp)
        logging.getLogger("market_dl").setLevel(logging.ERROR)
        self._td.cleanup()

    # ----------------------------------------------------------------------------------------------- fakes --
    def fake_update(self, ex, ex_dir, src, start, end, download=True, wanted=None, progress_callback=None,
                    cancel_event=None):
        self.updates.append((ex, src, None if wanted is None else sorted(wanted)))
        if progress_callback:
            progress_callback(1, 1, 0, 0)
        if self.block is not None:
            self.block.wait(20)
        if self.cancel_in_update:
            self.app.runner.cancel_event.set()
        if self.update_error:
            raise RuntimeError(self.update_error)

    def fake_download(self, tickers, start, end, raw_dir, **kw):
        self.downloads.append((Path(raw_dir).parent.name, sorted(tickers)))
        return []

    def fake_hourly_download(self, pairs, out, **kw):
        self.hourly_calls.append(("download", sorted(pairs), Path(out)))

    def fake_hour_files(self, pairs, out, metastock_ascii, start, end):
        self.hourly_calls.append(("build", sorted(pairs), Path(out)))

    # --------------------------------------------------------------------------------------------- helpers --
    def req(self, method, path, body=None, token=TOKEN, host=None):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=20)
        headers = {}
        if token is not None:
            headers["X-Token"] = token
        if host is not None:
            headers["Host"] = host
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        c.request(method, path, body=data, headers=headers)
        r = c.getresponse()
        raw = r.read()
        c.close()
        try:
            return r.status, json.loads(raw)
        except ValueError:
            return r.status, raw

    def get(self, path, **kw):
        return self.req("GET", path, **kw)

    def post(self, path, body=None, **kw):
        return self.req("POST", path, body if body is not None else {}, **kw)

    def form(self, **kw):
        f = dict(self.get("/api/state")[1]["settings"])
        f.update(output=str(self.out), start="2025-01-01", end="2026-11-10", layout="combined")
        f.update(kw)
        return f

    def run_and_wait(self, form, timeout=120):
        status, r = self.post("/api/run", form)
        self.assertEqual(status, 200)
        self.assertTrue(r["ok"], r)
        t0 = time.time()
        while time.time() - t0 < timeout:
            p = self.get("/api/poll?log=0&msg=0")[1]
            if not p["running"]:
                return p
            time.sleep(0.05)
        self.fail("run did not finish")

    # ----------------------------------------------------------------------------------------------- tests --
    def test_page_carries_token_and_api_requires_it(self):
        status, body = self.get("/", token=None)
        self.assertEqual(status, 200)
        self.assertIn(TOKEN.encode(), body)
        self.assertNotIn(b"__EOD_TOKEN__", body)
        self.assertEqual(self.get("/app.js", token=None)[0], 200)
        self.assertEqual(self.get("/api/state", token=None)[0], 403)
        self.assertEqual(self.get("/api/state", token="wrong")[0], 403)
        self.assertEqual(self.get("/api/state", host="evil.example:80")[0], 403)
        self.assertEqual(self.get("/api/state", host=f"localhost:{self.port}")[0], 200)
        self.assertEqual(self.get("/../eod_web.py", token=None)[0], 404)

    def test_state_defaults(self):
        s = self.get("/api/state")[1]
        st = s["settings"]
        self.assertEqual(st["markets"], ["NASDAQ", "NYSE", "NSE"])
        self.assertTrue(st["all"] and st["metastock_ascii"] and st["bhavcopy"] and st["us_source_alpaca"]
                        and st["nse_source_official"])
        self.assertFalse(st["bse_bhavcopy"])
        today = date.today().isoformat()
        self.assertEqual((st["layout"], st["interval"]), ("none", "daily"))
        self.assertEqual((st["start"], st["end"], st["date_preset"]), (today, today, "1D"))
        self.assertEqual([p[0] for p in s["presets"]], ["1D", "1W", "1Y", "5Y", "10Y", "YTD", "Max"])
        self.assertEqual(s["presets"][0][1], today)
        self.assertEqual(s["presets"][1][1], (date.today() - timedelta(days=7)).isoformat())
        self.assertEqual(len(s["exchanges"]), len(core.SUPPORTED_EXCHANGES))
        self.assertEqual(s["counts"]["NSE"], 404)
        self.assertFalse(s["running"])

    def test_search_order_prefix_name_and_cap(self):
        r = self.get("/api/search?q=tc&markets=NSE")[1]
        self.assertEqual([m[0] for m in r["matches"]], ["TCS", "ATCS"])   # symbol prefix first, then name match
        self.assertEqual(r["matches"][0][3], "NSE.TCS — Tata Consultancy Services")
        self.assertEqual(r["loaded"], 404)
        self.assertEqual([m[0] for m in self.get("/api/search?q=NSE.INF&markets=NSE")[1]["matches"]], ["INFY"])
        self.assertEqual(len(self.get("/api/search?q=&markets=NSE")[1]["matches"]), 300)
        self.assertEqual(self.get("/api/search?q=tc&markets=NYSE")[1]["matches"], [])

    def test_settings_round_trip(self):
        f = self.form(us_source_alpaca=False, metastock_ascii=False, markets=["NSE", "LSE"],
                      selected={"TCS": ["Tata", "NSE"]}, all=False, end=date.today().isoformat())
        self.assertEqual(self.post("/api/settings", f)[0], 200)
        saved = json.loads((self.tmp / "gui_settings.json").read_text())
        self.assertTrue(saved["end_is_today"])
        self.assertEqual(saved["selected"], {"TCS": ["Tata", "NSE"]})
        st = self.get("/api/state")[1]["settings"]
        self.assertEqual((st["us_source_alpaca"], st["metastock_ascii"], st["markets"], st["all"]),
                         (False, False, ["NSE", "LSE"], False))
        self.assertEqual(st["output"], str(self.out))

    def test_saved_quick_range_is_recomputed_custom_dates_kept(self):
        f = self.form(start="2020-01-01", end="2020-02-01", date_preset="1W")
        self.post("/api/settings", f)
        saved = json.loads((self.tmp / "gui_settings.json").read_text())
        self.assertEqual(saved["date_preset"], "1W")
        st = self.get("/api/state")[1]["settings"]
        self.assertEqual((st["start"], st["end"], st["date_preset"]),
                         ((date.today() - timedelta(days=7)).isoformat(), date.today().isoformat(), "1W"))
        self.post("/api/settings", self.form(start="2020-01-01", end="2020-02-01", date_preset=""))
        st = self.get("/api/state")[1]["settings"]
        self.assertEqual((st["start"], st["end"], st["date_preset"]), ("2020-01-01", "2020-02-01", ""))
        # an older settings file without date_preset opens on today (1D)
        saved = json.loads((self.tmp / "gui_settings.json").read_text())
        saved.pop("date_preset")
        (self.tmp / "gui_settings.json").write_text(json.dumps(saved))
        st = self.get("/api/state")[1]["settings"]
        self.assertEqual((st["start"], st["end"], st["date_preset"]),
                         (date.today().isoformat(), date.today().isoformat(), "1D"))

    def test_one_day_range_is_accepted(self):
        today = date.today().isoformat()
        p = self.run_and_wait(self.form(all=False, selected={"VOD": ["", "LSE"]}, start=today, end=today))
        self.assertEqual(p["messages"][-1]["title"], "Download complete")
        self.assertEqual(self.downloads, [("LSE", ["VOD"])])

    def test_validation_messages(self):
        cases = [(dict(all=True, markets=[]), "No market selected"),
                 (dict(all=False, selected={}), "No tickers selected"),
                 (dict(start="2026-13-01"), "Invalid date"),
                 (dict(start="2026-11-10", end="2026-01-01"), "Invalid date range"),
                 (dict(interval="hourly", all=True, markets=["LSE"]), "Hourly not available")]
        for overrides, title in cases:
            r = self.post("/api/run", self.form(**overrides))[1]
            self.assertFalse(r["ok"])
            self.assertEqual(r["title"], title, overrides)
        self.assertEqual(self.updates + self.downloads, [])

    def test_daily_run_all_mode_uses_sources(self):
        p = self.run_and_wait(self.form(markets=["NYSE", "NSE"], all=True, layout="combined"))
        self.assertEqual([m["title"] for m in p["messages"]], ["Download complete"], p["messages"])
        self.assertEqual(p["status"], "Done.")
        self.assertEqual(sorted(self.updates), [("NSE", "bhavcopy", None), ("NYSE", "alpaca", None)])
        for d in ("combined_adjusted", "bhavcopy_adjusted"):
            self.assertTrue((self.out / "NYSE" / d).is_dir(), d)
        for d in ("combined_adjusted", "bhavcopy_adjusted"):
            self.assertTrue((self.out / "NSE" / d).is_dir(), d)
        self.assertEqual(ts.header(self.out / "NSE" / "bhavcopy_adjusted" / "NSE_20260302.csv"), ts.METASTOCK_HEADER)
        self.assertTrue(any("--- Starting run" in line for line in p["log"]))

    def test_selected_tickers_split_and_alpaca_fallback(self):
        sel = {"AAA": ["", "NYSE"], "SPL": ["", "NSE"], "VOD": ["", "LSE"], "AAPL": ["Apple", "NASDAQ"]}
        p = self.run_and_wait(self.form(all=False, selected=sel))
        self.assertEqual(p["messages"][-1]["title"], "Download complete")
        self.assertIn("4 ticker(s)", p["messages"][-1]["message"])
        self.assertEqual(sorted(self.updates), [("NSE", "bhavcopy", ["SPL"]), ("NYSE", "alpaca", ["AAA"])])
        # NASDAQ has no alpaca_adjusted/ files here, so AAPL goes to Yahoo
        self.assertEqual(sorted(self.downloads), [("LSE", ["VOD"]), ("NASDAQ", ["AAPL"])])
        self.assertTrue(any("no Alpaca store" in line for line in p["log"]))
        self.assertTrue((self.out / "LSE" / "bhavcopy_adjusted" / "LSE_20261106.csv").exists())

    def test_no_bhavcopy_plain_format(self):
        self.run_and_wait(self.form(all=False, selected={"AAA": ["", "NYSE"]}, bhavcopy=False,
                                    metastock_ascii=False))
        self.assertFalse((self.out / "NYSE" / "bhavcopy_adjusted").exists())
        self.assertTrue(ts.header(self.out / "NYSE" / "combined_adjusted" / "all_data.csv").startswith("Symbol,"))

    def test_cancel_during_source_update(self):
        self.cancel_in_update = True
        p = self.run_and_wait(self.form(markets=["NYSE", "NSE"], all=True))
        self.assertEqual(p["messages"][-1]["title"], "Download cancelled")
        self.assertEqual(p["status"], "Cancelled.")
        self.assertEqual(len(self.updates), 1)

    def test_cancel_endpoint_and_second_run_refused(self):
        self.block = threading.Event()
        f = self.form(all=False, selected={"AAA": ["", "NYSE"]})
        self.assertTrue(self.post("/api/run", f)[1]["ok"])
        r = self.post("/api/run", f)[1]
        self.assertEqual((r["ok"], r["title"]), (False, "Download in progress"))
        self.assertTrue(self.post("/api/cancel")[1]["ok"])
        self.assertIn("Cancelling", self.get("/api/poll")[1]["status"])
        self.block.set()
        self.app.thread.join(20)
        p = self.get("/api/poll?log=0&msg=0")[1]
        self.assertFalse(p["running"])
        self.assertEqual(p["messages"][-1]["title"], "Download cancelled")
        self.assertTrue(any("Cancel requested" in line for line in p["log"]))

    def test_update_failure_still_builds(self):
        self.update_error = "no working Alpaca key"
        p = self.run_and_wait(self.form(all=False, selected={"AAA": ["", "NYSE"]}))
        self.assertEqual(p["messages"][-1]["title"], "Download complete")
        self.assertTrue((self.out / "NYSE" / "bhavcopy_adjusted" / "NYSE_20261106.csv").exists())
        self.assertTrue(any("no working Alpaca key" in line for line in p["log"]))

    def test_hourly_run_uses_hourly_folder(self):
        hourly_out = self.tmp / "hourly"
        p = self.run_and_wait(self.form(interval="hourly", all=False, output_hourly=str(hourly_out),
                                        selected={"RELIANCE": ["", "NSE"], "VOD": ["", "LSE"]}))
        self.assertEqual(p["messages"][-1]["title"], "Download complete", p["messages"])
        self.assertEqual(self.hourly_calls, [("download", [("RELIANCE", "NSE")], hourly_out),
                                             ("build", [("RELIANCE", "NSE")], hourly_out)])
        self.assertTrue(any("Skipping VOD" in line for line in p["log"]))
        self.assertEqual(self.updates + self.downloads, [])

    def test_listdir_and_open_folder(self):
        r = self.get(f"/api/listdir?path={self.tmp}")[1]
        self.assertEqual(Path(r["path"]), self.tmp)
        self.assertIn("out", r["dirs"])
        self.assertIn("cache", r["dirs"])
        r = self.get(f"/api/listdir?path={self.tmp / 'missing' / 'deeper'}")[1]
        self.assertEqual(Path(r["path"]), self.tmp)
        self.assertTrue(self.get("/api/listdir?path=")[1]["roots"])
        r = self.post("/api/open-folder", {"path": str(self.tmp / "nope")})[1]
        self.assertEqual((r["ok"], r["title"]), (False, "Folder not created yet"))

    def test_load_tickers_from_file(self):
        data = base64.b64encode(b"SYMBOL,NAME OF COMPANY\nabc,Alpha Beta\nXYZ,Xylo\n").decode()
        r = self.post("/api/load-tickers", {"exchange": "BSE", "filename": "bse.csv", "data": data})[1]
        self.assertTrue(r["ok"], r)
        self.assertEqual(r["message"], "Loaded 2 BSE tickers from bse.csv")
        self.assertEqual([m[0] for m in self.get("/api/search?q=alpha&markets=BSE")[1]["matches"]], ["ABC"])
        self.assertEqual(self.get("/api/poll")[1]["tickers_version"], 1)
        bad = base64.b64encode(b"foo,bar\n1,2\n").decode()
        r = self.post("/api/load-tickers", {"exchange": "BSE", "filename": "x.csv", "data": bad})[1]
        self.assertEqual((r["ok"], r["title"]), (False, "Couldn't load file"))
        r = self.post("/api/load-tickers", {"exchange": "", "filename": "x.csv", "data": data})[1]
        self.assertEqual(r["title"], "Pick one market first")

    def test_refresh_tickers(self):
        fake = pd.DataFrame({"Symbol": ["AAA", "BBB"], "Name": ["A Co", "B Co"], "Exchange": "NYSE"})
        with mock.patch.object(core, "fetch_ticker_directory", return_value=fake) as f:
            self.assertTrue(self.post("/api/refresh-tickers", {"markets": ["NYSE"]})[1]["ok"])
            t0 = time.time()
            while self.get("/api/poll")[1]["refreshing"] and time.time() - t0 < 10:
                time.sleep(0.05)
        self.assertTrue(f.call_args.kwargs["refresh"])
        self.assertEqual(self.get("/api/search?q=&markets=NYSE")[1]["loaded"], 2)
        r = self.post("/api/refresh-tickers", {"markets": []})[1]
        self.assertEqual(r["title"], "No market selected")

    def test_shutdown(self):
        self.assertTrue(self.post("/api/shutdown")[1]["ok"])
        self.thread.join(10)
        self.assertFalse(self.thread.is_alive())


class PortTests(unittest.TestCase):
    def test_busy_port_falls_back_to_a_free_one(self):
        import socket
        blocker = socket.socket()
        if hasattr(socket, "SO_REUSEADDR"):
            blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)     # like the swingbt server
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        busy = blocker.getsockname()[1]
        td = ts.tmpdir()
        app = eod_web.App(cache_dir=Path(td.name), settings_path=Path(td.name) / "s.json")
        try:
            server, _, _ = eod_web.make_server(busy, app=app, token=TOKEN)
            self.assertNotEqual(server.server_port, busy)
            server.server_close()
        finally:
            app.close()
            blocker.close()
            td.cleanup()

    def test_default_port_is_not_swingbts(self):
        self.assertEqual(app_paths.DEFAULTS["web"]["port"], "8790")


class AppPathsTests(unittest.TestCase):
    def setUp(self):
        self._td = ts.tmpdir()
        self.tmp = Path(self._td.name)

    def tearDown(self):
        app_paths.load_config(reload=True)
        self._td.cleanup()

    def test_defaults_without_config(self):
        cp = app_paths.load_config(self.tmp / "missing.ini")
        self.assertEqual(cp.get("paths", "daily_output"), "market_data_daily")
        with mock.patch.object(app_paths, "_config", cp):
            self.assertEqual(app_paths.daily_output(), app_paths.APP_DIR / "market_data_daily")
            self.assertEqual(app_paths.ticker_cache_dir(), app_paths.APP_DIR / ".ticker_cache")
            self.assertEqual(app_paths._path("alpaca_data"), app_paths.APP_DIR / "market_data_alpaca")
            self.assertEqual(app_paths.web_port(), 8790)
            self.assertTrue(app_paths.web_open_browser())

    def test_config_relative_and_absolute_paths(self):
        absolute = self.tmp / "data" / "daily"
        ini = self.tmp / "eod_config.ini"
        ini.write_text(f"[paths]\ndaily_output = {absolute}   ; a comment\nhourly_output = ..\\hourly_data\n"
                       f"ticker_cache = \"cache dir\"\n[web]\nport = 0\nopen_browser = no\n", encoding="utf-8")
        cp = app_paths.load_config(ini)
        with mock.patch.object(app_paths, "_config", cp):
            self.assertEqual(app_paths.daily_output(), absolute)
            self.assertEqual(app_paths.hourly_output(), app_paths.APP_DIR / ".." / "hourly_data")
            self.assertEqual(app_paths.ticker_cache_dir(), app_paths.APP_DIR / "cache dir")
            self.assertEqual(app_paths.web_port(), 0)
            self.assertFalse(app_paths.web_open_browser())

    def test_example_config_matches_defaults(self):
        cp = app_paths.load_config(ROOT / "eod_config.example.ini")
        for section, values in app_paths.DEFAULTS.items():
            for k, v in values.items():
                self.assertEqual(cp.get(section, k), v, (section, k))

    def test_frozen_app_dir_is_exe_folder(self):
        exe = self.tmp / "bundle" / "EOD_Market_Data.exe"
        with mock.patch.object(sys, "frozen", True, create=True), \
                mock.patch.object(sys, "executable", str(exe)), \
                mock.patch.object(sys, "_MEIPASS", str(self.tmp / "bundle" / "_internal"), create=True):
            mod = importlib.reload(app_paths)
            self.assertTrue(mod.FROZEN)
            self.assertEqual(mod.APP_DIR, exe.parent.resolve())
            self.assertEqual(mod.RES_DIR, self.tmp / "bundle" / "_internal")
        mod = importlib.reload(app_paths)
        self.assertFalse(mod.FROZEN)
        self.assertEqual(mod.APP_DIR, ROOT)


class EffectiveSourceTests(unittest.TestCase):
    def setUp(self):
        self._td = ts.tmpdir()
        self.out = Path(self._td.name)

    def tearDown(self):
        self._td.cleanup()

    def test_alpaca_needs_files(self):
        self.addCleanup(ts.mock.patch.stopall)
        ts.mock.patch.object(ts.alpaca_store, "store_dir", return_value=self.out / "no_store").start()
        self.assertEqual(core.effective_source("NASDAQ", self.out / "NASDAQ", "alpaca"), "yahoo")
        self.assertEqual(core.effective_source("NASDAQ", self.out / "NASDAQ", "alpaca"), "yahoo")
        ts.make_alpaca(self.out / "NYSE")
        self.assertEqual(core.effective_source("NYSE", self.out / "NYSE", "alpaca"), "alpaca")
        self.assertEqual(core.effective_source("NSE", self.out / "NSE", "bhavcopy"), "bhavcopy")
        self.assertEqual(core.effective_source("NYSE", self.out / "NYSE", "yahoo"), "yahoo")

    def test_cli_falls_back_per_exchange(self):
        self.addCleanup(ts.mock.patch.stopall)
        ts.make_alpaca(self.out / "NYSE")
        a = ts.parse([])
        self.assertEqual([core.source_of(e, a, self.out) for e in ("NASDAQ", "NYSE", "NSE")],
                         ["yahoo", "alpaca", "bhavcopy"])
        self.assertEqual([core.source_of(e, a) for e in ("NASDAQ", "NYSE")], ["alpaca", "alpaca"])



class InclusiveEndTests(unittest.TestCase):
    """The Yahoo daily fetch includes the end date (the 1D quick range asks for start == end == today)."""

    def setUp(self):
        self._td = ts.tmpdir()
        self.raw = Path(self._td.name) / "LSE" / "yahoo_adjusted"
        self.raw.mkdir(parents=True)

    def tearDown(self):
        self._td.cleanup()

    def test_yf_fetch_asks_yahoo_for_the_day_after(self):
        with mock.patch.object(core.yf, "download", return_value=pd.DataFrame()) as dl:
            core._yf_fetch("VOD.L", "2026-11-11", "2026-11-11")
        self.assertEqual((dl.call_args.kwargs["start"], dl.call_args.kwargs["end"]), ("2026-11-11", "2026-11-12"))

    def test_last_weekday(self):
        self.assertEqual(core._last_weekday(pd.Timestamp("2026-11-11")), pd.Timestamp("2026-11-11"))   # Wed
        self.assertEqual(core._last_weekday(pd.Timestamp("2026-11-14")), pd.Timestamp("2026-11-13"))   # Sat
        self.assertEqual(core._last_weekday(pd.Timestamp("2026-11-15")), pd.Timestamp("2026-11-13"))   # Sun

    def test_one_day_run_fetches_the_end_day(self):
        dates = pd.bdate_range("2026-11-02", "2026-11-10")
        df = ts.yf_frame(dates, 100 + np.arange(len(dates))).reset_index()[
            ["Date", "Open", "High", "Low", "Close", "Volume"]]
        df["Ticker"] = "TST"
        df.to_csv(self.raw / "LSE.TST.csv", index=False)
        truth = ts.yf_frame(pd.bdate_range("2026-11-02", "2026-11-11"), 100 + np.arange(8))
        calls = []

        def fake(sym, start, end):                       # _yf_fetch's contract: end inclusive
            calls.append((start, end))
            return truth[(truth.index >= pd.Timestamp(start)) & (truth.index <= pd.Timestamp(end))]
        with mock.patch.object(core, "_yf_fetch", fake), mock.patch.object(core.time, "sleep", lambda s: None):
            self.assertEqual(core.download_ticker("TST", "2026-11-11", "2026-11-11", self.raw, exchange="LSE"),
                             "ok")
            self.assertEqual(calls, [("2026-11-09", "2026-11-11")])
            got = pd.read_csv(self.raw / "LSE.TST.csv", parse_dates=["Date"])
            self.assertEqual(got["Date"].max(), pd.Timestamp("2026-11-11"))
            # now covered: the same 1D run again needs no request
            calls.clear()
            self.assertEqual(core.download_ticker("TST", "2026-11-11", "2026-11-11", self.raw, exchange="LSE"),
                             "cached")
            self.assertEqual(calls, [])


class SavedDatesTests(unittest.TestCase):
    def test_presets_and_saved_dates(self):
        import eod_app
        today = date.today()
        self.assertEqual(eod_app.preset_range("1D"), (today, today))
        self.assertEqual(eod_app.preset_range("1W"), (today - timedelta(days=7), today))
        self.assertIsNone(eod_app.preset_range(""))
        old = {"start": "2016-10-06", "end": "2026-11-10", "end_is_today": False}
        self.assertEqual(eod_app.saved_dates(old), (today, today, "1D"))                  # no key = default 1D
        self.assertEqual(eod_app.saved_dates({**old, "date_preset": "1W"})[2], "1W")
        self.assertEqual(eod_app.saved_dates({**old, "date_preset": ""}),
                         (date(2016, 10, 6), date(2026, 11, 10), ""))
        self.assertEqual(eod_app.saved_dates({**old, "date_preset": "", "end_is_today": True})[1], today)


if __name__ == "__main__":
    unittest.main(verbosity=2)
