"""Offline tests for the data sources (Alpaca US, NSE bhavcopy, Yahoo incremental), the CLI wiring and the GUI.

No network: Alpaca is a fake client, yfinance is patched, NSE data is a synthetic bhavcopy cache in a temp folder.

    venv\\Scripts\\python.exe -m unittest discover -s tests -v
"""
import contextlib
import io
import json
import logging
import math
import shutil
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import alpaca_store               # noqa: E402
import eod_downloader as core     # noqa: E402
import nse_bhavcopy as nb         # noqa: E402

logging.getLogger("market_dl").setLevel(logging.ERROR)
METASTOCK_HEADER = "<TICKER>,<PER>,<DTYYYYMMDD>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>"


def tmpdir():
    return tempfile.TemporaryDirectory(ignore_cleanup_errors=True)


def drop_file_handlers(under: Path):
    """eod_downloader.start_file_logging() adds a FileHandler per run; close the ones writing into `under`."""
    lg = logging.getLogger("market_dl")
    for h in list(lg.handlers):
        if isinstance(h, logging.FileHandler) and str(under) in str(getattr(h, "baseFilename", "")):
            lg.removeHandler(h)
            h.close()


# ======================================================================================================== Alpaca
def frame(dates, closes, vol=1000.0):
    closes = np.asarray(closes, dtype=float)
    vol = np.broadcast_to(np.asarray(vol, dtype=float), closes.shape)
    return pd.DataFrame({"Open": closes, "High": closes * 1.01, "Low": closes * 0.99, "Close": closes,
                         "Volume": vol}, index=pd.DatetimeIndex(pd.to_datetime(dates), name="Date"))


HIST = pd.bdate_range("2026-09-01", "2026-10-23")        # what the export stored
NEW = pd.bdate_range("2026-10-22", "2026-11-06")         # what Alpaca has from the second-to-last stored bar on
ALL = pd.bdate_range("2026-09-01", "2026-11-06")


def series(dates, base):
    return base * (1 + 0.01 * np.sin(np.arange(len(dates))))


# =========================================================================================================== NSE
NSE_DATES = [d for d in pd.bdate_range("2026-01-01", "2026-03-31") if d != pd.Timestamp("2026-01-26")]


def osc(i):
    return 1 + 0.004 * math.sin(i)


def make_nse(nse_dir: Path, demergers_csv: Path):
    """Synthetic NSE caches: day files, a corporate-actions quarter JSON and a demerger list."""
    cache = nse_dir / "official_raw" / "full"
    cache.mkdir(parents=True)
    rows = []

    def add(sym, isin, d, i, close, vol):
        rows.append(dict(Date=f"{d:%Y-%m-%d}", Symbol=sym, Series="EQ", Open=close, High=close * 1.002,
                         Low=close * 0.998, Close=close, Volume=int(vol), ISIN=isin))
    for i, d in enumerate(NSE_DATES):
        o = osc(i)
        add("SPL", "INE000SPL01", d, i, (1000 if d < pd.Timestamp("2026-02-02") else 100) * o,
            1000 if d < pd.Timestamp("2026-02-02") else 10000)
        add("BON", "INE000BON01", d, i, (500 if d < pd.Timestamp("2026-02-16") else 250) * o,
            2000 if d < pd.Timestamp("2026-02-16") else 4000)
        add("RTS", "INE000RTS01", d, i, (100 if d < pd.Timestamp("2026-03-02") else 90) * o, 100)
        if d <= pd.Timestamp("2026-02-27"):
            add("OLDN", "INE000OLD01", d, i, 200 * o, 300)
        else:
            add("NEWN", "INE000OLD01", d, i, 200 * o, 300)
        add("PAR", "INE000PAR01", d, i, (300 if d < pd.Timestamp("2026-03-10") else 210) * o, 400)
        if d >= pd.Timestamp("2026-03-20"):
            add("KID", "INE000KID01", d, i, 90 * o, 400)
        if d <= pd.Timestamp("2026-02-10"):
            add("DEL", "INE000DEL01", d, i, 50 * o, 100)
        add("NOG", "INE000NOG01", d, i, 80 * o, 100)
        add("ETFX", "INF000ETF01", d, i, 10 * o, 100)                         # fund units: left out
    df = pd.DataFrame(rows)
    for d, g in df.groupby("Date"):
        g.to_csv(cache / f"{d.replace('-', '')}.csv", index=False)
    (cache / "20260126.csv").write_text("Date,Symbol,Series,Open,High,Low,Close,Volume,ISIN\n")   # holiday marker
    ca = [dict(symbol="SPL", series="EQ", isin="INE000SPL01", comp="Spl Ltd", exDate="02-Feb-2026",
               subject="Face Value Split (Sub-Division) - From Rs 10/- Per Share To Re 1/- Per Share",
               faceVal="10", recDate="02-Feb-2026"),
          dict(symbol="SPL", series="EQ", isin="INE000SPL01", comp="Spl Ltd", exDate="05-Mar-2026",
               subject="Interim Dividend - Rs 2 Per Share", faceVal="1", recDate="05-Mar-2026"),
          dict(symbol="BON", series="EQ", isin="INE000BON01", comp="Bon Ltd", exDate="16-Feb-2026",
               subject="Bonus 1:1", faceVal="10", recDate="16-Feb-2026"),
          dict(symbol="RTS", series="EQ", isin="INE000RTS01", comp="Rts Ltd", exDate="02-Mar-2026",
               subject="Rights 1:4 @ Premium Rs 40/-", faceVal="10", recDate="02-Mar-2026"),
          dict(symbol="NOG", series="EQ", isin="INE000NOG01", comp="Nog Ltd", exDate="20-Feb-2026",
               subject="Bonus 1:1", faceVal="10", recDate="20-Feb-2026")]
    (nse_dir / "nse_ca").mkdir()
    (nse_dir / "nse_ca" / "2026-01-01_2026-03-31.json").write_text(json.dumps(ca), encoding="utf-8")
    demergers_csv.write_text("Date,Parent,Child,Ratio,Note,Verified\n2026-03-10,PAR,KID,1,test,\n")


def read_official(nse_dir, sym):
    return pd.read_csv(nse_dir / "official_adjusted" / f"NSE.{sym}.csv", parse_dates=["Date"], index_col="Date")


class NseParseTests(unittest.TestCase):
    def test_parse_events(self):
        ca = pd.DataFrame([
            dict(symbol="A", isin="I1", exDate=pd.Timestamp("2026-01-05"), faceVal="10",
                 subject="Face Value Split (Sub-Division) - From Rs 10/- Per Share To Re 1/- Per Share"),
            dict(symbol="B", isin="I2", exDate=pd.Timestamp("2026-01-05"), faceVal="10", subject="Bonus 1:1"),
            dict(symbol="C", isin="I3", exDate=pd.Timestamp("2026-01-05"), faceVal="2", subject="Bonus 4:1"),
            dict(symbol="C", isin="I3", exDate=pd.Timestamp("2026-01-05"), faceVal="2",
                 subject="Face Value Split (Sub-Division) - From Rs 2/- Per Share To Re 1/- Per Share"),
            dict(symbol="D", isin="I4", exDate=pd.Timestamp("2026-01-05"), faceVal="10",
                 subject="Rights 1:4 @ Premium Rs 40/-"),
            dict(symbol="E", isin="I5", exDate=pd.Timestamp("2026-01-05"), faceVal="10", subject="Demerger"),
            dict(symbol="F", isin="I6", exDate=pd.Timestamp("2026-01-05"), faceVal="10",
                 subject="Interim Dividend - Rs 2.50 Per Share"),
            dict(symbol="G", isin="I7", exDate=pd.NaT, faceVal="10", subject="Bonus 1:1")])
        ev = nb.parse_events(ca).set_index("symbol")
        self.assertAlmostEqual(ev.loc["A", "f"], 0.1)
        self.assertAlmostEqual(ev.loc["B", "f"], 0.5)
        self.assertAlmostEqual(ev.loc["C", "f"], 0.2 * 0.5)                   # bonus 4:1 x split 2->1, one event
        self.assertIn("+", ev.loc["C", "kind"])
        self.assertEqual((ev.loc["D", "a"], ev.loc["D", "b"], ev.loc["D", "price"]), (1, 4, 50.0))
        self.assertEqual(ev.loc["E", "kind"], "demerger")
        self.assertEqual(ev.loc["F", "price"], 2.5)
        self.assertNotIn("G", ev.index)

    def test_parse_events_empty(self):
        self.assertEqual(len(nb.parse_events(pd.DataFrame(columns=["exDate", "subject"]))), 0)


class NseBuildTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._td = tmpdir()
        cls.nse = Path(cls._td.name) / "NSE"
        cls.dm = Path(cls._td.name) / "nse_demergers.csv"
        make_nse(cls.nse, cls.dm)
        nb.update_corporate_actions(cls.nse / "nse_ca", download=False)
        cls.built = nb.build(cls.nse, demergers_csv=cls.dm)
        cls.events = pd.read_csv(cls.nse / "official_info" / "events.csv")
        cls.meta = pd.read_csv(cls.nse / "official_info" / "meta.csv").set_index("Symbol")

    @classmethod
    def tearDownClass(cls):
        cls._td.cleanup()

    def smooth(self, sym, tol=0.02):
        c = read_official(self.nse, sym)["Close"]
        return float(np.abs(np.diff(np.log(c.values))).max()) < tol

    def test_built(self):
        self.assertTrue(self.built)

    def test_split_adjusted_back_and_volume(self):
        s = read_official(self.nse, "SPL")
        self.assertTrue(self.smooth("SPL"))
        self.assertAlmostEqual(s["Close"].iloc[0], 100 * osc(0), places=3)
        self.assertEqual(s["Volume"].iloc[0], 10000)
        self.assertAlmostEqual(s["Close"].iloc[-1], 100 * osc(len(NSE_DATES) - 1), places=3)   # latest = as traded

    def test_bonus_and_rights(self):
        self.assertTrue(self.smooth("BON"))
        r = read_official(self.nse, "RTS")
        pc = 100 * osc(NSE_DATES.index(pd.Timestamp("2026-02-27")))
        f = (4 * pc + 50) / 5 / pc
        self.assertAlmostEqual(r["Close"].iloc[0], 100 * osc(0) * f, places=3)

    def test_small_dividend_not_adjusted(self):
        e = self.events[(self.events.symbol == "SPL")]
        self.assertFalse((e.kind.astype(str).str.contains("dividend")).any())

    def test_rename_joined_under_latest_symbol(self):
        self.assertFalse((self.nse / "official_adjusted" / "NSE.OLDN.csv").exists())
        n = read_official(self.nse, "NEWN")
        self.assertEqual(n.index[0], NSE_DATES[0])
        self.assertEqual(len(n), len(NSE_DATES))
        self.assertIn("OLDN", str(self.meta.loc["NEWN", "JoinedFrom"]))

    def test_demerger_from_hand_list(self):
        p = read_official(self.nse, "PAR")["Close"]
        k = p.index.get_loc(pd.Timestamp("2026-03-10"))
        self.assertAlmostEqual(p.iloc[k] / p.iloc[k - 1], 1.0, places=6)          # actual drop absorbed
        self.assertTrue(((self.events.symbol == "PAR") & (self.events.kind == "demerger")).any())

    def test_delisted_kept_and_flagged(self):
        d = read_official(self.nse, "DEL")
        self.assertEqual(d.index[-1], pd.Timestamp("2026-02-10"))
        self.assertFalse(bool(self.meta.loc["DEL", "Active"]))
        self.assertTrue(bool(self.meta.loc["SPL", "Active"]))

    def test_event_without_price_gap_not_applied(self):
        e = self.events[self.events.symbol == "NOG"]
        self.assertEqual(e.status.tolist(), ["no price gap"])
        self.assertAlmostEqual(read_official(self.nse, "NOG")["Close"].iloc[0], 80 * osc(0), places=3)

    def test_fund_units_and_holiday_marker_skipped(self):
        self.assertFalse((self.nse / "official_adjusted" / "NSE.ETFX.csv").exists())
        self.assertNotIn(pd.Timestamp("2026-01-26"), read_official(self.nse, "SPL").index)

    def test_layout_matches_raw_cache(self):
        cols = pd.read_csv(self.nse / "official_adjusted" / "NSE.SPL.csv", nrows=1).columns.tolist()
        self.assertEqual(cols, ["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"])

    def test_ca_csv_lf_only_and_stable(self):
        p = self.nse / "nse_ca" / nb.CA_CSV
        self.assertNotIn(b"\r", p.read_bytes())
        m = p.stat().st_mtime_ns
        nb.update_corporate_actions(self.nse / "nse_ca", download=False)
        self.assertEqual(p.stat().st_mtime_ns, m)


class NseBuildControlTests(unittest.TestCase):
    def setUp(self):
        self._td = tmpdir()
        self.nse = Path(self._td.name) / "NSE"
        self.dm = Path(self._td.name) / "nse_demergers.csv"
        make_nse(self.nse, self.dm)
        nb.update_corporate_actions(self.nse / "nse_ca", download=False)

    def tearDown(self):
        self._td.cleanup()

    def test_skip_when_unchanged_rebuild_when_input_changes(self):
        self.assertTrue(nb.build(self.nse, demergers_csv=self.dm))
        self.assertFalse(nb.build(self.nse, demergers_csv=self.dm))
        time.sleep(1.1)
        self.dm.write_text(self.dm.read_text() + "2026-03-10,PAR,,0.1,second child,\n")
        self.assertTrue(nb.build(self.nse, demergers_csv=self.dm))
        self.assertTrue(nb.build(self.nse, demergers_csv=self.dm, force=True))

    def test_cancel_leaves_official_untouched(self):
        nb.build(self.nse, demergers_csv=self.dm)
        f = self.nse / "official_adjusted" / "NSE.SPL.csv"
        before = f.read_bytes()
        ev = threading.Event()
        ev.set()
        self.assertFalse(nb.build(self.nse, demergers_csv=self.dm, force=True, cancel_event=ev))
        self.assertEqual(f.read_bytes(), before)

    def test_stale_symbol_files_removed(self):
        nb.build(self.nse, demergers_csv=self.dm)
        (self.nse / "official_adjusted" / "NSE.GHOST.csv").write_text("Date,Open,High,Low,Close,Volume,Ticker\n")
        nb.build(self.nse, demergers_csv=self.dm, force=True)
        self.assertFalse((self.nse / "official_adjusted" / "NSE.GHOST.csv").exists())

    def test_progress_reported(self):
        calls = []
        nb.build(self.nse, demergers_csv=self.dm, force=True, progress_callback=lambda *a: calls.append(a))
        self.assertEqual(calls[-1][0], calls[-1][1])


class NseDownloadTests(unittest.TestCase):
    def test_which_days_are_fetched_and_holiday_marking(self):
        import datetime as dt
        today = dt.date.today()
        calls = []

        def fake_fetch(d, cache, mark_holiday=True):
            calls.append((d, mark_holiday))
            return d, True
        with tmpdir() as td, mock.patch.object(nb, "fetch_day", fake_fetch):
            cache = Path(td)
            (cache / f"{today - dt.timedelta(days=60):%Y%m%d}.csv").write_text("x")    # already cached
            nb.download_days(cache, today - dt.timedelta(days=90), today + dt.timedelta(days=5), workers=2)
        days = {d for d, _ in calls}
        self.assertNotIn(today - dt.timedelta(days=60), days)
        self.assertTrue(all(d <= today for d in days))                                       # never the future
        old_weekends = [d for d in days if d.weekday() >= 5 and d < today - dt.timedelta(nb.WEEKEND_LOOKBACK)]
        self.assertEqual(old_weekends, [])
        recent_weekends = [d for d in days if d.weekday() >= 5]
        self.assertTrue(recent_weekends)
        for d, mark in calls:
            expect = d.weekday() < 5 and d < today - dt.timedelta(1)
            self.assertEqual(mark, expect, d)

    def test_cancel_skips_remaining_days(self):
        import datetime as dt
        calls = []
        ev = threading.Event()
        ev.set()
        with tmpdir() as td, mock.patch.object(nb, "fetch_day", lambda d, c, mark_holiday=True: calls.append(d)):
            failed = nb.download_days(Path(td), dt.date(2026, 1, 1), dt.date(2026, 1, 31), cancel_event=ev)
        self.assertEqual((calls, failed), ([], []))


# ========================================================================================================= Yahoo
def yf_frame(dates, closes):
    closes = np.asarray(closes, dtype=float)
    return pd.DataFrame({"Open": closes, "High": closes, "Low": closes, "Close": closes, "Adj Close": closes,
                         "Volume": 100}, index=pd.DatetimeIndex(pd.to_datetime(dates), name="Date"))


class YahooIncrementalTests(unittest.TestCase):
    def setUp(self):
        self._td = tmpdir()
        self.raw = Path(self._td.name) / "LSE" / "yahoo_adjusted"
        self.raw.mkdir(parents=True)
        self.dates = pd.bdate_range("2026-10-01", "2026-10-23")
        self.c = 100 + np.arange(len(self.dates), dtype=float)
        df = yf_frame(self.dates, self.c).reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
        df["Ticker"] = "TST"
        df.to_csv(self.raw / "LSE.TST.csv", index=False)
        self.calls = []

    def tearDown(self):
        self._td.cleanup()

    def run_dl(self, truth, end="2026-11-07"):
        def fake(sym, start, end_):
            self.calls.append((sym, start, end_))
            m = (truth.index >= pd.Timestamp(start)) & (truth.index < pd.Timestamp(end_))
            return truth[m]
        with mock.patch.object(core, "_yf_fetch", fake), mock.patch.object(core.time, "sleep", lambda s: None):
            return core.download_ticker("TST", "2026-10-01", end, self.raw, exchange="LSE")

    def cached(self):
        return pd.read_csv(self.raw / "LSE.TST.csv", parse_dates=["Date"]).set_index("Date")

    def test_extends_from_second_to_last_bar(self):
        all_d = pd.bdate_range("2026-10-01", "2026-11-06")
        truth = yf_frame(all_d, 100 + np.arange(len(all_d), dtype=float))
        truth.loc[pd.Timestamp("2026-10-23"), "Close"] = 999.0               # partial bar fixed on refetch
        self.assertEqual(self.run_dl(truth), "ok")
        self.assertEqual(self.calls[0][1], "2026-10-22")
        got = self.cached()
        self.assertEqual(got.index[-1], pd.Timestamp("2026-11-06"))
        self.assertEqual(got.at[pd.Timestamp("2026-10-23"), "Close"], 999.0)
        self.assertEqual(len(got), len(all_d))
        self.assertEqual(got["Ticker"].iloc[-1], "TST")

    def test_basis_change_refetches_full_history(self):
        all_d = pd.bdate_range("2026-10-01", "2026-11-06")
        truth = yf_frame(all_d, (100 + np.arange(len(all_d), dtype=float)) / 2)      # split since caching
        self.assertEqual(self.run_dl(truth), "ok")
        self.assertEqual([c[1] for c in self.calls], ["2026-10-22", "2026-10-01"])
        got = self.cached()
        self.assertTrue(np.allclose(got["Close"].values, truth["Close"].values))

    def test_only_overlap_back_is_cached_not_stale(self):
        truth = yf_frame(self.dates, self.c)
        self.assertEqual(self.run_dl(truth, end="2026-10-27"), "cached")     # Sat..Mon: one business day
        self.assertEqual(len(self.cached()), len(self.dates))

    def test_long_empty_gap_is_stale(self):
        truth = yf_frame(self.dates[:0], [])
        self.assertEqual(self.run_dl(truth, end="2026-11-07"), "stale")

    def test_fully_cached_makes_no_request(self):
        self.assertEqual(self.run_dl(yf_frame(self.dates, self.c), end="2026-10-24"), "cached")
        self.assertEqual(self.calls, [])

    def test_basis_changed_helper(self):
        ex = pd.DataFrame({"Date": pd.to_datetime(["2026-10-22"]), "Close": [100.0]})
        fr = pd.DataFrame({"Date": pd.to_datetime(["2026-10-22"]), "Close": [100.4]})
        self.assertFalse(core._basis_changed(ex, fr, pd.Timestamp("2026-10-22")))
        fr["Close"] = 50.0
        self.assertTrue(core._basis_changed(ex, fr, pd.Timestamp("2026-10-22")))
        fr["Close"] = np.nan
        self.assertFalse(core._basis_changed(ex, fr, pd.Timestamp("2026-10-22")))
        self.assertFalse(core._basis_changed(ex, fr, None))


# =========================================================================================================== CLI
def parse(argv):
    with mock.patch.object(sys, "argv", ["eod_downloader.py", *argv]):
        return core.parse_args()


class CliArgsTests(unittest.TestCase):
    def test_defaults(self):
        a = parse([])
        self.assertTrue(a.metastock_ascii)
        self.assertEqual((a.us_source, a.nse_source), ("alpaca", "bhavcopy"))

    def test_no_metastock(self):
        self.assertFalse(parse(["--no-metastock-ascii"]).metastock_ascii)
        self.assertTrue(parse(["--metastock-ascii"]).metastock_ascii)

    def test_raw_basis_keeps_alpaca_and_uses_yahoo_for_nse(self):
        a = parse(["--price-basis", "raw"])
        self.assertEqual((a.us_source, a.nse_source), ("alpaca", "yahoo"))

    def test_raw_basis_with_nse_bhavcopy_is_an_error(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parse(["--price-basis", "raw", "--nse-source", "bhavcopy"])

    def test_old_layouts_mean_combined_and_default_is_none(self):
        for v in ("daily", "per-symbol", "all", "combined"):
            self.assertEqual(parse(["--layout", v]).layout, "combined")
        self.assertEqual(parse(["--layout", "none"]).layout, "none")
        self.assertEqual(parse([]).layout, "none")

    def test_source_of(self):
        a = parse([])
        self.assertEqual([core.source_of(e, a) for e in ("NASDAQ", "NYSE", "NSE", "BSE", "LSE")],
                         ["alpaca", "alpaca", "bhavcopy", "yahoo", "yahoo"])

    def test_other_clis_default_to_metastock(self):
        import hourly_downloader
        import rebuild_combined
        for mod, argv in ((hourly_downloader, ["x"]), (rebuild_combined, ["x"])):
            src = Path(mod.__file__).read_text(encoding="utf-8")
            self.assertIn('"--metastock-ascii", action=argparse.BooleanOptionalAction, default=True', src)


META_COLS = ["key", "symbol", "status", "delisted", "start", "sig", "first", "last", "bars", "cut", "f_last",
             "last_close", "after_date", "after_close", "dup_of", "exit_price", "exit_why"]


def make_alpaca(ex_dir: Path):
    """A synthetic Alpaca store (market_data_alpaca/ layout: as-traded pickles + meta + corp_actions) next to
    `ex_dir`'s market_data_daily, and alpaca_store.store_dir() pointed at it (undone by mock.patch.stopall)."""
    ex = ex_dir.name
    store = ex_dir.parent.parent / "market_data_alpaca_test"
    (store / "daily" / ex).mkdir(parents=True, exist_ok=True)
    rows = []
    for key, dates, base in (("AAA", ALL, 10), ("BRK.B", ALL, 500), ("AAA@2026-09-30", HIST[:15], 3)):
        df = frame(dates, series(dates, base))
        df["f_split"] = df["f_spinoff"] = df["f_all"] = 1.0
        df.to_pickle(store / "daily" / ex / f"{key}.pkl")
        rows.append([key, key.split("@")[0], "active", "", str(dates[0].date()), "", dates[0], dates[-1], len(dates),
                     "", 1.0, float(df["Close"].iloc[-1]), "", np.nan, "", float(df["Close"].iloc[-1]), "last close"])
    pd.DataFrame(rows, columns=META_COLS).to_csv(store / f"meta_{ex}.csv", index=False)
    pd.DataFrame(columns=["year", "type", "symbol", "date", "record"]).to_csv(store / "corp_actions.csv", index=False)
    mock.patch.object(alpaca_store, "store_dir", return_value=store).start()


def header(p: Path):
    return p.read_bytes().split(b"\r\n" if b"\r\n" in p.read_bytes() else b"\n")[0].decode()


class CliEndToEndTests(unittest.TestCase):
    """main() with --skip-download on synthetic Alpaca / NSE / Yahoo caches."""

    def setUp(self):
        self._td = tmpdir()
        self.out = Path(self._td.name) / "out"
        make_alpaca(self.out / "NYSE")
        self.addCleanup(mock.patch.stopall)
        self.dm = ROOT / "nse_demergers.csv"
        make_nse(self.out / "NSE", Path(self._td.name) / "unused_demergers.csv")
        raw = self.out / "LSE" / "yahoo_adjusted"
        raw.mkdir(parents=True)
        df = yf_frame(ALL, series(ALL, 120)).reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
        df["Ticker"] = "VOD"
        df.to_csv(raw / "LSE.VOD.csv", index=False)

    def tearDown(self):
        drop_file_handlers(self.out)
        self._td.cleanup()

    def main(self, *argv):
        with mock.patch.object(sys, "argv", ["eod_downloader.py", "--output", str(self.out), "--skip-download",
                                             *argv]):
            core.main()
        drop_file_handlers(self.out)

    def test_alpaca_and_nse_sources_combined_and_bhavcopy_metastock(self):
        self.main("--exchanges", "NYSE", "NSE", "--layout", "combined", "--start", "2025-01-01")
        nyse, nse = self.out / "NYSE", self.out / "NSE"
        for ex_dir in (nyse, nse):
            self.assertTrue((ex_dir / "combined_adjusted" / "all_data.csv").is_file())
            for d in ("daily_adjusted", "by_symbol_adjusted", "yahoo_adjusted_never"):
                self.assertFalse((ex_dir / d).exists(), d)           # the per-day / per-symbol folders are gone
        self.assertTrue((nyse / "alpaca_exits.csv").is_file())
        self.assertTrue(any((nse / "bhavcopy_adjusted").glob("NSE_*.csv")))
        day = nyse / "bhavcopy_adjusted" / "NYSE_20261106.csv"
        self.assertEqual(header(day), METASTOCK_HEADER)
        self.assertIn(b"\r\n", day.read_bytes())
        body = pd.read_csv(day)
        self.assertEqual(sorted(body["<TICKER>"]), ["AAA", "BRK.B"])
        old = pd.read_csv(nyse / "bhavcopy_adjusted" / "NYSE_20260901.csv")
        self.assertEqual(sorted(old["<TICKER>"]), ["AAA", "AAA@2026-09-30", "BRK.B"])   # delisted key included
        nse_day = pd.read_csv(nse / "bhavcopy_adjusted" / "NSE_20260105.csv")
        self.assertIn("DEL", set(nse_day["<TICKER>"]))                                  # delisted included
        self.assertIn("NEWN", set(nse_day["<TICKER>"]))                                 # renamed, joined
        self.assertFalse((nyse / "yahoo_adjusted").exists())                            # Yahoo folders untouched

    def test_alpaca_raw_basis_is_as_traded(self):
        self.main("--exchanges", "NYSE", "--price-basis", "raw", "--layout", "combined", "--no-metastock-ascii",
                  "--start", "2025-01-01")
        nyse = self.out / "NYSE"
        self.assertTrue((nyse / "combined_raw" / "all_data.csv").is_file())
        self.assertTrue(any((nyse / "bhavcopy_raw").glob("NYSE_*.csv")))
        self.assertFalse((nyse / "combined_adjusted").exists())

    def test_plain_layout_and_single_ticker(self):
        self.main("--exchanges", "NYSE", "--ticker", "brk.b", "--ticker-exchange", "NYSE",
                  "--layout", "combined", "--no-metastock-ascii", "--no-bhavcopy", "--start", "2025-01-01")
        f = self.out / "NYSE" / "combined_adjusted" / "all_data.csv"
        self.assertEqual(header(f).split(",")[0], "Symbol")
        self.assertEqual(set(pd.read_csv(f)["Symbol"]), {"NYSE.BRK.B"})

    def test_default_writes_only_the_day_files(self):
        self.main("--exchanges", "NYSE", "NSE", "--start", "2025-01-01")
        for ex in ("NYSE", "NSE"):
            self.assertTrue(any((self.out / ex / "bhavcopy_adjusted").glob(f"{ex}_*.csv")))
            self.assertFalse((self.out / ex / "combined_adjusted").exists())
            self.assertFalse((self.out / ex / "daily_adjusted").exists())

    def test_yahoo_exchange_unchanged_path(self):
        self.main("--exchanges", "LSE", "--ticker", "VOD", "--ticker-exchange", "LSE", "--start", "2025-01-01")
        day = self.out / "LSE" / "bhavcopy_adjusted" / "LSE_20261106.csv"
        self.assertEqual(header(day), METASTOCK_HEADER)

    def test_yahoo_source_for_nse_when_chosen(self):
        self.main("--exchanges", "NSE", "--nse-source", "yahoo", "--ticker", "SPL", "--ticker-exchange", "NSE",
                  "--start", "2025-01-01")
        self.assertFalse((self.out / "NSE" / "official_adjusted").exists())                     # bhavcopy path not used

    def test_source_tickers(self):
        src_dir, t = core.source_tickers("NYSE", self.out / "NYSE", "alpaca", wanted=["aaa", "nope"])
        self.assertEqual(t, ["AAA"])
        _, t = core.source_tickers("NYSE", self.out / "NYSE", "alpaca", limit=1)
        self.assertEqual(len(t), 1)
        _, t = core.source_tickers("NASDAQ", self.out / "NASDAQ", "alpaca")          # no store for NASDAQ
        self.assertEqual(t, [])


# =========================================================================================================== GUI
class GuiTests(unittest.TestCase):
    """The real Tk window, driven programmatically. Network calls (update_source, download_all) are stubbed."""

    @classmethod
    def setUpClass(cls):
        import eod_gui
        cls.gui_mod = eod_gui

    def setUp(self):
        self._td = tmpdir()
        self.tmp = Path(self._td.name)
        self.out = self.tmp / "out"
        make_alpaca(self.out / "NYSE")
        self.addCleanup(mock.patch.stopall)
        make_nse(self.out / "NSE", self.tmp / "unused.csv")
        nb.update_corporate_actions(self.out / "NSE" / "nse_ca", download=False)
        nb.build(self.out / "NSE")
        raw = self.out / "LSE" / "yahoo_adjusted"
        raw.mkdir(parents=True)
        df = yf_frame(ALL, series(ALL, 120)).reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
        df["Ticker"] = "VOD"
        df.to_csv(raw / "LSE.VOD.csv", index=False)
        self.updates, self.downloads, self.boxes = [], [], []
        self.patches = [
            mock.patch.object(self.gui_mod, "SETTINGS_PATH", self.tmp / "gui_settings.json"),
            mock.patch.object(core, "update_source", self.fake_update),
            mock.patch.object(core, "download_all", self.fake_download),
            mock.patch.object(self.gui_mod.messagebox, "showinfo", lambda *a: self.boxes.append(("info",) + a)),
            mock.patch.object(self.gui_mod.messagebox, "showerror", lambda *a: self.boxes.append(("error",) + a)),
            mock.patch.object(self.gui_mod.messagebox, "showwarning", lambda *a: self.boxes.append(("warn",) + a)),
        ]
        for p in self.patches:
            p.start()
        self.update_error = None
        self.cancel_in_update = False
        self.app = self.gui_mod.MarketDataGUI()
        self.app.withdraw()

    def tearDown(self):
        try:
            self.app.destroy()
        except Exception:
            pass
        for p in self.patches:
            p.stop()
        lg = logging.getLogger("market_dl")
        for h in list(lg.handlers):
            if isinstance(h, self.gui_mod.QueueLogHandler):
                lg.removeHandler(h)
        drop_file_handlers(self.tmp)
        lg.setLevel(logging.ERROR)
        self._td.cleanup()

    def fake_update(self, ex, ex_dir, src, start, end, download=True, wanted=None, progress_callback=None,
                    cancel_event=None):
        self.updates.append((ex, src, None if wanted is None else sorted(wanted)))
        if progress_callback:
            progress_callback(1, 1, 0, 0)
        if self.cancel_in_update:
            self.app.cancel_event.set()
        if self.update_error:
            raise RuntimeError(self.update_error)

    def fake_download(self, tickers, start, end, raw_dir, **kw):
        self.downloads.append((Path(raw_dir).parent.name, sorted(tickers)))
        return []

    def configure(self, markets=(), selected=None, layout="combined", start="2025-01-01"):
        for ex, var in self.app.market_vars.items():
            var.set(ex in markets)
        if selected is None:
            self.app.ticker_all_var.set(True)
        else:
            self.app.ticker_all_var.set(False)
            self.app.selected_tickers = {s: ("", ex) for s, ex in selected.items()}
        self.app.layout_var.set(layout)
        self.app._set_dates(pd.Timestamp(start).date(), pd.Timestamp("2026-11-10").date())
        self.app.output_var.set(str(self.out))

    def run_gui(self, timeout=120):
        """Click Download and run Tk's real main loop (worker threads post results with after(), which needs
        it) until the run finishes."""
        t0 = time.time()

        def poll():
            if not self.app.is_running or time.time() - t0 > timeout:
                self.app.quit()
            else:
                self.app.after(50, poll)
        self.app.after(0, self.app._start_download)
        self.app.after(200, poll)
        self.app.mainloop()
        self.app.update()
        self.assertFalse(self.app.is_running, "GUI run did not finish")

    def test_defaults(self):
        self.assertEqual([ex for ex, v in self.app.market_vars.items() if v.get()], ["NASDAQ", "NYSE", "NSE"])
        self.assertTrue(self.app.metastock_ascii_var.get())
        self.assertTrue(self.app.us_alpaca_var.get())
        self.assertTrue(self.app.nse_official_var.get())

    def test_all_mode_uses_sources_and_builds_suffixed_folders(self):
        self.configure(markets=("NYSE", "NSE"), layout="combined")
        self.run_gui()
        self.assertEqual(self.boxes[-1][0], "info", self.boxes)
        self.assertEqual(sorted(self.updates), [("NSE", "bhavcopy", None), ("NYSE", "alpaca", None)])
        self.assertEqual(self.downloads, [])
        for d in ("combined_adjusted", "bhavcopy_adjusted"):
            self.assertTrue((self.out / "NYSE" / d).is_dir(), d)
        for d in ("combined_adjusted", "bhavcopy_adjusted"):
            self.assertTrue((self.out / "NSE" / d).is_dir(), d)
        day = self.out / "NSE" / "bhavcopy_adjusted" / "NSE_20260302.csv"
        self.assertEqual(header(day), METASTOCK_HEADER)
        self.assertIn("NEWN", set(pd.read_csv(day)["<TICKER>"]))
        self.assertFalse((self.out / "NYSE" / "yahoo_adjusted").exists())

    def test_selected_tickers_split_between_sources_and_yahoo(self):
        self.configure(selected={"AAA": "NYSE", "SPL": "NSE", "VOD": "LSE"})
        self.run_gui()
        self.assertEqual(self.boxes[-1][0], "info", self.boxes)
        self.assertEqual(sorted(self.updates), [("NSE", "bhavcopy", ["SPL"]), ("NYSE", "alpaca", ["AAA"])])
        self.assertEqual(self.downloads, [("LSE", ["VOD"])])
        self.assertEqual(set(pd.read_csv(self.out / "NYSE" / "bhavcopy_adjusted" / "NYSE_20261106.csv")["<TICKER>"]),
                         {"AAA"})
        self.assertEqual(set(pd.read_csv(self.out / "NSE" / "bhavcopy_adjusted" / "NSE_20260302.csv")["<TICKER>"]),
                         {"SPL"})
        self.assertTrue((self.out / "LSE" / "bhavcopy_adjusted" / "LSE_20261106.csv").exists())
        self.assertIn("3 ticker(s)", self.boxes[-1][2])

    def test_sources_unticked_fall_back_to_yahoo(self):
        self.app.us_alpaca_var.set(False)
        self.app.nse_official_var.set(False)
        self.configure(selected={"AAA": "NYSE", "SPL": "NSE"})
        self.run_gui()
        self.assertEqual(self.updates, [])
        self.assertEqual(sorted(self.downloads), [("NSE", ["SPL"]), ("NYSE", ["AAA"])])
        self.assertFalse((self.out / "NYSE" / "yahoo_adjusted").exists())

    def test_update_failure_still_builds_from_existing_files(self):
        self.update_error = "no working Alpaca key"
        self.configure(selected={"AAA": "NYSE"})
        self.run_gui()
        self.assertEqual(self.boxes[-1][0], "info")
        self.assertTrue((self.out / "NYSE" / "bhavcopy_adjusted" / "NYSE_20261106.csv").exists())
        self.assertIn("no working Alpaca key", self.app.log_text.get("1.0", "end"))

    def test_cancel_during_source_update(self):
        self.cancel_in_update = True
        self.configure(markets=("NYSE", "NSE"))
        self.run_gui()
        self.assertEqual(self.boxes[-1][1], "Download cancelled")
        self.assertEqual(len(self.updates), 1)                       # the second exchange never started
        self.assertFalse((self.out / "NYSE" / "yahoo_adjusted").exists())

    def test_no_bhavcopy_and_plain_format(self):
        self.app.bhavcopy_var.set(False)
        self.app.metastock_ascii_var.set(False)
        self.configure(selected={"AAA": "NYSE"})
        self.run_gui()
        self.assertFalse((self.out / "NYSE" / "bhavcopy_adjusted").exists())
        self.assertTrue(header(self.out / "NYSE" / "combined_adjusted" / "all_data.csv").startswith("Symbol,"))

    def test_settings_round_trip_and_defaults_for_old_files(self):
        self.app.us_alpaca_var.set(False)
        self.app.metastock_ascii_var.set(False)
        self.app._save_settings()
        s = json.loads((self.tmp / "gui_settings.json").read_text())
        self.assertEqual((s["us_source_alpaca"], s["nse_source_official"], s["metastock_ascii"]),
                         (False, True, False))
        self.app.destroy()
        self.app = self.gui_mod.MarketDataGUI()
        self.app.withdraw()
        self.assertFalse(self.app.us_alpaca_var.get())
        self.assertTrue(self.app.nse_official_var.get())
        self.assertFalse(self.app.metastock_ascii_var.get())
        for k in ("us_source_alpaca", "nse_source_official", "metastock_ascii"):     # an old settings file
            s.pop(k)
        (self.tmp / "gui_settings.json").write_text(json.dumps(s))
        self.app.destroy()
        self.app = self.gui_mod.MarketDataGUI()
        self.app.withdraw()
        self.assertTrue(self.app.us_alpaca_var.get() and self.app.nse_official_var.get()
                        and self.app.metastock_ascii_var.get())

    def test_dates_default_to_today_with_quick_ranges(self):
        from datetime import date as _date, timedelta as _td
        today = _date.today().isoformat()
        self.assertEqual(self.app.date_preset_var.get(), "1D")
        self.assertEqual((self.app._get_start_date_str(), self.app._get_end_date_str()), (today, today))
        self.app.date_preset_var.set("1W")
        self.app._apply_preset()
        self.assertEqual(self.app._get_start_date_str(), (_date.today() - _td(days=7)).isoformat())
        self.app._save_settings()
        self.app.destroy()
        self.app = self.gui_mod.MarketDataGUI()
        self.app.withdraw()
        self.assertEqual(self.app.date_preset_var.get(), "1W")
        self.app._set_dates(_date(2020, 1, 1), _date(2020, 2, 1))
        self.app._on_manual_date()
        self.assertEqual(self.app.date_preset_var.get(), "")

    def test_hourly_mode_greys_out_sources(self):
        self.app.interval_var.set("hourly")
        self.app._on_interval_change()
        self.assertIn("disabled", str(self.app.us_alpaca_check.cget("state")))
        self.assertIn("disabled", str(self.app.nse_official_check.cget("state")))
        self.app.interval_var.set("daily")
        self.app._on_interval_change()
        self.assertNotIn("disabled", str(self.app.us_alpaca_check.cget("state")))


if __name__ == "__main__":
    unittest.main(verbosity=2)
