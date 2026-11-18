"""NSE corporate actions are downloaded once and reused: nseindia.com is only asked again for trading days that
arrived since the last check (nse_bhavcopy.update_corporate_actions). No network: the API calls are stubbed.

    venv\\Scripts\\python.exe -m unittest discover -s tests -v
"""
import datetime as dt
import json
import logging
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import nse_bhavcopy as nb   # noqa: E402

logging.getLogger("market_dl").setLevel(logging.ERROR)
START = dt.date(2026, 4, 1)          # two quarters: Q2 (closed) and Q3 2026 (all dates in the past)


def ev(sym, ex_date, subject="Bonus 1:1"):
    return {"symbol": sym, "series": "EQ", "isin": "INE000000000", "comp": sym, "exDate": ex_date.strftime("%d-%b-%Y"),
            "subject": subject, "faceVal": "10", "recDate": ex_date.strftime("%d-%b-%Y")}


class CorporateActionCacheTests(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.ca = Path(self._td.name) / "nse_ca"
        self.calls = []
        self.api = {}                    # (index, a, b) -> rows; default: every event of `self.events` in [a, b]
        self.events = {"equities": [ev("AAA", dt.date(2026, 5, 4)), ev("BBB", dt.date(2026, 8, 3))],
                       "sme": [ev("SMEX", dt.date(2026, 7, 6))]}
        self.fail = False
        self.patches = [mock.patch.object(nb, "_ca_session", lambda: object()),
                        mock.patch.object(nb, "_ca_fetch", self.fake_fetch),
                        mock.patch.object(nb.time, "sleep", lambda s: None)]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()
        self._td.cleanup()

    def fake_fetch(self, s, a, b, index):
        self.calls.append((index, a, b))
        if self.fail:
            raise RuntimeError("HTTP 403")
        rows = [r for r in self.events[index] if a <= nb._ex_date(r) <= b]
        return rows, s

    def update(self, through, **kw):
        return nb.update_corporate_actions(self.ca, start=START, through=through, **kw)

    def csv_symbols(self):
        import pandas as pd
        return sorted(pd.read_csv(self.ca / nb.CA_CSV)["symbol"])

    def test_first_run_fetches_every_quarter_once(self):
        self.update(dt.date(2026, 8, 10))
        self.assertEqual(sorted(self.calls), [
            ("equities", dt.date(2026, 4, 1), dt.date(2026, 6, 30)),
            ("equities", dt.date(2026, 7, 1), dt.date(2026, 8, 10)),
            ("sme", dt.date(2026, 4, 1), dt.date(2026, 6, 30)),
            ("sme", dt.date(2026, 7, 1), dt.date(2026, 8, 10))])
        self.assertEqual(self.csv_symbols(), ["AAA", "BBB", "SMEX"])
        checked = json.loads((self.ca / nb.CA_CHECKED).read_text())
        self.assertEqual(checked["2026-07-01_2026-09-30.json"], "2026-08-10")
        self.assertEqual(checked["sme_2026-04-01_2026-06-30.json"], "2026-06-30")

    def test_no_new_trading_day_means_no_request(self):
        self.update(dt.date(2026, 8, 10))
        self.calls.clear()
        self.update(dt.date(2026, 8, 10))
        self.update(dt.date(2026, 8, 5))           # an older end date needs nothing either
        self.assertEqual(self.calls, [])

    def test_new_trading_day_fetches_only_the_new_days_and_merges(self):
        self.update(dt.date(2026, 8, 10))
        self.calls.clear()
        self.events["equities"].append(ev("CCC", dt.date(2026, 8, 12), "Face Value Split From Rs 10 To Rs 2"))
        self.events["equities"][1] = ev("BBB", dt.date(2026, 8, 3), "Bonus 2:1")         # corrected late
        self.update(dt.date(2026, 8, 13))
        a = dt.date(2026, 8, 10) - dt.timedelta(days=nb.CA_OVERLAP_DAYS)
        self.assertEqual(sorted(self.calls), [("equities", a, dt.date(2026, 8, 13)),
                                              ("sme", a, dt.date(2026, 8, 13))])
        rows = json.loads((self.ca / "2026-07-01_2026-09-30.json").read_text())
        self.assertEqual(sorted((r["symbol"], r["subject"]) for r in rows),
                         [("BBB", "Bonus 2:1"), ("CCC", "Face Value Split From Rs 10 To Rs 2")])
        self.assertEqual(self.csv_symbols(), ["AAA", "BBB", "CCC", "SMEX"])

    def test_rows_outside_the_window_are_kept(self):
        self.update(dt.date(2026, 8, 10))
        self.events["sme"] = []                          # the API no longer lists the Oct 6 event...
        self.update(dt.date(2026, 8, 13))
        self.assertIn("SMEX", self.csv_symbols())        # ...but it is before the re-checked window: kept

    def test_failed_fetch_keeps_cache_and_retries_next_time(self):
        self.update(dt.date(2026, 8, 10))
        self.fail = True
        self.calls.clear()
        self.update(dt.date(2026, 8, 13))
        self.assertEqual(len(self.calls), 1)             # the first list failed, the rest were not tried
        self.assertEqual(json.loads((self.ca / nb.CA_CHECKED).read_text())["2026-07-01_2026-09-30.json"],
                         "2026-08-10")
        self.assertEqual(self.csv_symbols(), ["AAA", "BBB", "SMEX"])
        self.fail = False
        self.calls.clear()
        self.update(dt.date(2026, 8, 13))
        self.assertEqual(len(self.calls), 2)

    def test_download_false_never_fetches(self):
        self.update(dt.date(2026, 8, 10), download=False)
        self.assertEqual(self.calls, [])

    def test_refresh_fetches_everything_again(self):
        self.update(dt.date(2026, 8, 10))
        self.calls.clear()
        self.update(dt.date(2026, 8, 10), refresh=True)
        self.assertEqual(len(self.calls), 4)
        self.assertTrue(all(a.day == 1 for _i, a, _b in self.calls))

    def test_older_cache_without_checked_file_uses_file_dates(self):
        self.ca.mkdir(parents=True)
        q3 = self.ca / "2026-04-01_2026-06-30.json"
        q4 = self.ca / "2026-07-01_2026-09-30.json"
        for f in (q3, q4, self.ca / ("sme_" + q3.name), self.ca / ("sme_" + q4.name)):
            f.write_text("[]")
        written = time.mktime(dt.datetime(2026, 8, 10, 12).timetuple())
        for f in self.ca.glob("*.json"):
            os.utime(f, (written, written))
        self.update(dt.date(2026, 8, 10))
        self.assertEqual(self.calls, [])                 # closed Q2 is complete; Q3 was fetched through Aug 10
        self.assertTrue((self.ca / nb.CA_CHECKED).exists())
        self.update(dt.date(2026, 8, 11))
        self.assertEqual(len(self.calls), 2)
        self.assertTrue(all(a == dt.date(2026, 8, 3) for _i, a, _b in self.calls))

    def make_seed(self, through):
        """A bundled seed like build_bundle.py writes: quarter lists + checked.json."""
        seed = Path(self._td.name) / "seed"
        real_ca, self.ca = self.ca, seed
        self.update(through)
        self.ca = real_ca
        self.calls.clear()
        return seed

    def test_bundled_seed_fills_an_empty_cache_without_requests(self):
        seed = self.make_seed(dt.date(2026, 8, 10))
        with mock.patch.object(nb, "SEED_CA_DIR", seed):
            self.update(dt.date(2026, 8, 10))
        self.assertEqual(self.calls, [])
        self.assertEqual(self.csv_symbols(), ["AAA", "BBB", "SMEX"])
        self.assertEqual(json.loads((self.ca / nb.CA_CHECKED).read_text())["2026-07-01_2026-09-30.json"],
                         "2026-08-10")
        with mock.patch.object(nb, "SEED_CA_DIR", seed):    # a later trading day: only the new days are asked
            self.update(dt.date(2026, 8, 13))
        self.assertEqual(len(self.calls), 2)
        self.assertTrue(all(a == dt.date(2026, 8, 3) for _i, a, _b in self.calls))

    def test_newer_local_lists_are_kept_older_ones_replaced(self):
        seed = self.make_seed(dt.date(2026, 8, 10))
        self.update(dt.date(2026, 8, 13))                    # local cache checked further than the seed
        self.calls.clear()
        local_q3 = (self.ca / "2026-07-01_2026-09-30.json").read_text()
        with mock.patch.object(nb, "SEED_CA_DIR", seed):
            self.assertEqual(nb.seed_corporate_actions(self.ca), 0)
        self.assertEqual((self.ca / "2026-07-01_2026-09-30.json").read_text(), local_q3)
        checked = json.loads((self.ca / nb.CA_CHECKED).read_text())
        checked["2026-07-01_2026-09-30.json"] = "2026-07-15"          # pretend the local one is older
        (self.ca / nb.CA_CHECKED).write_text(json.dumps(checked))
        with mock.patch.object(nb, "SEED_CA_DIR", seed):
            self.assertEqual(nb.seed_corporate_actions(self.ca), 1)
        self.assertEqual(json.loads((self.ca / nb.CA_CHECKED).read_text())["2026-07-01_2026-09-30.json"],
                         "2026-08-10")

    def test_no_seed_folder_is_a_no_op(self):
        self.assertEqual(nb.seed_corporate_actions(self.ca, Path(self._td.name) / "nope"), 0)

    def test_latest_trading_day_skips_holiday_markers(self):
        cache = Path(self._td.name) / "full"
        cache.mkdir()
        (cache / "20261106.csv").write_text("x" * 100)
        (cache / "20261109.csv").write_text("x" * 100)
        (cache / "20261110.csv").write_text("")         # holiday marker
        self.assertEqual(nb.latest_trading_day(cache), dt.date(2026, 11, 9))
        self.assertIsNone(nb.latest_trading_day(Path(self._td.name) / "missing"))



class OneDayBuildTests(unittest.TestCase):
    """A first NSE run over a single day (the 1D quick range) must still write every symbol."""

    def test_single_day_cache_builds_every_symbol(self):
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import pandas as pd
        import test_sources as ts
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
            nse, dm = Path(td) / "NSE", Path(td) / "nse_demergers.csv"
            ts.make_nse(nse, dm)
            days = sorted(f for f in (nse / "official_raw" / "full").glob("[0-9]*.csv") if f.stat().st_size > 60)
            for f in days[:-1]:
                f.unlink()
            nb.update_corporate_actions(nse / "nse_ca", download=False)
            self.assertTrue(nb.build(nse, demergers_csv=dm, force=True))
            files = sorted((nse / "official_adjusted").glob("NSE.*.csv"))
            self.assertTrue(files)
            self.assertTrue(all(len(pd.read_csv(f)) == 1 for f in files))
            meta = pd.read_csv(nse / "official_info" / "meta.csv")
            self.assertEqual(len(meta), len(files))
            self.assertTrue(meta["Active"].all())


if __name__ == "__main__":
    unittest.main(verbosity=2)
