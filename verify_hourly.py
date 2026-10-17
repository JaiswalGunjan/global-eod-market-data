#!/usr/bin/env python3
"""
Offline check of the hourly output written by hourly_downloader.py.

For each exchange folder under --root it verifies:
  1. Conversion  -- every bar in every raw_1h/<EX>.<TICKER>.csv file appears
                    in the matching <EX>_YYYYMMDD_HHMM.csv hour file (and the
                    hour files hold nothing that isn't in raw_1h/).
  2. Hourly only -- raw bars all start on the exchange's usual minute
                    (e.g. :15 for NSE, :30 for US), are never at midnight
                    (a sign of daily bars), and bars in the same day are a
                    whole number of hours apart. Each hour file holds one
                    bar time only, matching its filename, with one row per symbol.

Usage:
    python verify_hourly.py                         # all exchanges under ./market_data_hourly
    python verify_hourly.py --exchanges NSE --show 50
    python verify_hourly.py --progress-every 30     # progress line every 30s (default 60)
"""

import argparse
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

import eod_downloader as core
from hourly_downloader import HOURLY_EXCHANGES, _MinuteProgress

HOUR_FILE_RE = re.compile(r"^(?P<ex>[A-Z]+)_(?P<date>\d{8})_(?P<time>\d{4})\.csv$")
EPOCH = pd.Timestamp("1970-01-01")


def _minutes(ts) -> np.ndarray:
    """Timestamps -> int64 minutes since epoch (tz-naive, exchange-local)."""
    return ((pd.DatetimeIndex(ts) - EPOCH) // pd.Timedelta(minutes=1)).to_numpy(dtype="int64")


class _SymbolCodes:
    """Maps symbol strings to small ints so (symbol, bar) pairs can be packed
    into one int64 key -- keeps millions of rows cheap to compare."""

    def __init__(self):
        self.codes = {}

    def encode(self, symbols) -> np.ndarray:
        for s in pd.unique(symbols):
            if s not in self.codes:
                self.codes[s] = len(self.codes)
        return pd.Series(symbols).map(self.codes).to_numpy(dtype="int64")

    def decode(self, code: int) -> str:
        if not hasattr(self, "_rev"):
            self._rev = {v: k for k, v in self.codes.items()}
        return self._rev[code]


def _keys(sym_codes: np.ndarray, minutes: np.ndarray) -> np.ndarray:
    return (sym_codes << 32) | minutes


def _unkey(key: int, codes: _SymbolCodes) -> str:
    sym = codes.decode(int(key) >> 32)
    ts = EPOCH + pd.Timedelta(minutes=int(key) & 0xFFFFFFFF)
    return f"{sym} @ {ts:%Y-%m-%d %H:%M}"


def check_exchange(root: Path, ex: str, show: int, progress: _MinuteProgress) -> int:
    ex_dir = root / ex
    raw_dir = ex_dir / "raw_1h"
    problems = 0

    def report(title, items):
        nonlocal problems
        items = list(items)
        if not items:
            return
        problems += len(items)
        print(f"  [FAIL] {title}: {len(items):,}")
        for it in items[:show]:
            print(f"         - {it}")
        if len(items) > show:
            print(f"         ... and {len(items) - show:,} more")

    print(f"\n=== {ex} ===")
    if not raw_dir.is_dir():
        print(f"  no {raw_dir}, skipped")
        return 0

    codes = _SymbolCodes()
    metastock = None

    # ---- hour files -------------------------------------------------------
    hour_keys, bad_names, mismatched, dup_rows, mixed_layout = [], [], [], [], []
    hour_files = sorted(p for p in ex_dir.glob(f"{ex}_*.csv"))
    progress.label = f"{ex} hour files"
    for path in hour_files:
        progress.step("ok")
        m = HOUR_FILE_RE.match(path.name)
        if not m or m["ex"] != ex:
            bad_names.append(path.name)
            continue
        df = pd.read_csv(path, dtype=str)
        is_ms = df.columns[0] == "<TICKER>"
        if metastock is None:
            metastock = is_ms
        elif metastock != is_ms:
            mixed_layout.append(path.name)
        date_col, time_col = (2, 3) if is_ms else (1, 2)
        sym = df.iloc[:, 0]
        stamps = (df.iloc[:, date_col] + df.iloc[:, time_col].str.zfill(6).str[:4]).unique()
        if len(stamps) != 1 or stamps[0] != m["date"] + m["time"]:
            mismatched.append(f"{path.name}: rows have {', '.join(map(str, stamps[:5]))}")
        dups = sym[sym.duplicated()].unique()
        if len(dups):
            dup_rows.append(f"{path.name}: {', '.join(dups[:5])}")
        ts = pd.Timestamp(m["date"] + m["time"])
        hour_keys.append(_keys(codes.encode(sym.to_numpy()), np.full(len(df), _minutes([ts])[0])))

    hour_keys = np.concatenate(hour_keys) if hour_keys else np.empty(0, dtype="int64")
    print(f"  hour files: {len(hour_files):,} ({len(hour_keys):,} rows, "
          f"{'MetaStock ASCII' if metastock else 'standard'} layout)")
    report("hour files with unexpected names", bad_names)
    report("hour files whose rows don't match the filename's bar time", mismatched)
    report("hour files with a symbol listed more than once", dup_rows)
    report("hour files in a different layout from the rest", mixed_layout)

    # ---- raw per-symbol files --------------------------------------------
    raw_keys, empty, unreadable = [], [], []
    minute_counts = pd.Series(dtype="int64")
    midnight, bad_gaps, raw_dups = [], [], []
    raw_files = sorted(raw_dir.glob("*.csv"))
    progress.label = f"{ex} raw files"
    for path in raw_files:
        progress.step("ok")
        ticker = core._bare_ticker_from_filename(path.stem)
        try:
            df = pd.read_csv(path, parse_dates=["Datetime"])
        except pd.errors.EmptyDataError:
            empty.append(path.name)
            continue
        except Exception as e:
            unreadable.append(f"{path.name}: {e}")
            continue
        df = df.dropna(subset=["Open", "High", "Low", "Close"])  # same rows build_hour_files drops
        if df.empty:
            empty.append(path.name)
            continue
        dt = df["Datetime"]

        minute_counts = minute_counts.add(dt.dt.minute.value_counts(), fill_value=0)
        if (dt.dt.hour.eq(0) & dt.dt.minute.eq(0)).any():
            midnight.append(f"{path.name}: {int((dt.dt.hour.eq(0) & dt.dt.minute.eq(0)).sum())} bar(s) at 00:00")
        if dt.duplicated().any():
            raw_dups.append(f"{path.name}: {int(dt.duplicated().sum())} repeated timestamp(s)")
        s = dt.sort_values()
        gap = s.diff()
        same_day = s.dt.normalize().eq(s.dt.normalize().shift())
        off = same_day & gap.notna() & (gap % pd.Timedelta(hours=1) != pd.Timedelta(0))
        if off.any():
            first = s[off].iloc[0]
            bad_gaps.append(f"{path.name}: {int(off.sum())} bar(s) not a whole hour after the previous, "
                            f"first at {first:%Y-%m-%d %H:%M}")

        sym = core.metastock_symbol(ticker) if metastock else core.prefixed_symbol(ticker, ex)
        raw_keys.append(_keys(codes.encode(np.full(len(df), sym, dtype=object)), _minutes(dt)))

    raw_keys = np.concatenate(raw_keys) if raw_keys else np.empty(0, dtype="int64")
    print(f"  raw files : {len(raw_files):,} ({len(raw_keys):,} bars; "
          f"{len(empty):,} empty = Yahoo had no hourly data, nothing to convert)")
    report("unreadable raw files", unreadable)

    # Hourly-only checks
    if not minute_counts.empty:
        main_minute = int(minute_counts.idxmax())
        print(f"  bar start minute: :{main_minute:02d} "
              f"({int(minute_counts.max()):,} of {int(minute_counts.sum()):,} bars)")
        other = minute_counts.drop(main_minute)
        report("bars starting off the usual minute (not hourly-aligned)",
               [f":{int(k):02d} -> {int(v):,} bar(s)" for k, v in other.items()])
    report("raw files with bars at midnight (looks like daily data)", midnight)
    report("raw files with bars less/more than a whole hour apart", bad_gaps)
    report("raw files with duplicate timestamps", raw_dups)

    # Conversion check
    missing = np.setdiff1d(raw_keys, hour_keys, assume_unique=False)
    extra = np.setdiff1d(hour_keys, raw_keys, assume_unique=False)
    missing_syms = pd.Series([codes.decode(int(k) >> 32) for k in missing]).value_counts()
    report("raw bars NOT found in any hour file",
           [f"{sym}: {n:,} bar(s)" for sym, n in missing_syms.items()])
    if len(missing):
        print("         first missing bars: " + "; ".join(_unkey(k, codes) for k in missing[:3]))
    report("hour-file rows with no matching raw bar", [_unkey(k, codes) for k in extra])

    print(f"  {'OK' if problems == 0 else f'{problems:,} problem(s)'}")
    return problems


def main():
    p = argparse.ArgumentParser(description="Verify hourly hour-files against the raw_1h/ caches")
    p.add_argument("--root", default="./market_data_hourly", help="Hourly output root")
    p.add_argument("--exchanges", nargs="+", choices=HOURLY_EXCHANGES, default=HOURLY_EXCHANGES)
    p.add_argument("--show", type=int, default=10, help="Examples to print per problem type")
    p.add_argument("--progress-every", type=float, default=60, help="Seconds between progress lines")
    args = p.parse_args()

    root = Path(args.root)
    # Overall % across every exchange: one step per hour file and per raw file
    n_files = sum(len(list((root / ex).glob(f"{ex}_*.csv"))) + len(list((root / ex / "raw_1h").glob("*.csv")))
                  for ex in args.exchanges)
    progress = _MinuteProgress(n_files, args.progress_every, unit="files")
    total = sum(check_exchange(root, ex, args.show, progress) for ex in args.exchanges)
    print(f"\n{'ALL CHECKS PASSED' if total == 0 else f'TOTAL: {total:,} problem(s)'}")
    sys.exit(1 if total else 0)


if __name__ == "__main__":
    main()
