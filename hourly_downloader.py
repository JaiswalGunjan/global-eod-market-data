#!/usr/bin/env python3
"""
Hourly (1h) OHLCV downloader -- NSE, NYSE, NASDAQ -- free, no API key
======================================================================

Uses Yahoo Finance via yfinance, the same free/keyless source as
eod_downloader.py. HARD LIMIT: Yahoo only serves 1-hour bars for the most
recent ~730 days, so a first run gets ~2 years, not 10. No free keyless
source has 10 years of hourly data.

What this script does about that: the raw cache is never trimmed. Every
re-run only fetches bars newer than what's cached and appends them, so if
you run it regularly (at least once every ~700 days, ideally weekly) the
history keeps growing past Yahoo's 2-year window.

    python hourly_downloader.py                                  # NSE + NASDAQ + NYSE, every ticker
    python hourly_downloader.py --exchanges NSE --limit 20       # quick test
    python hourly_downloader.py --ticker RELIANCE --ticker-exchange NSE
    python hourly_downloader.py --metastock-ascii                # MetaStock intraday ASCII output
    python hourly_downloader.py --skip-download                  # rebuild output from cache only

Output (default --output ./market_data_hourly), one folder per exchange:
    <EX>/<EX>_YYYYMMDD_HHMM.csv           every ticker's bar for that hour, e.g.
                                          NSE/NSE_20261009_1015.csv:
                                          Symbol,Date,Time,Open,High,Low,Close,Volume
    <EX>/raw_1h/<EX>.<TICKER>.csv         download cache (Datetime in exchange local time)
    logs/run_<timestamp>.log

Bars are regular-session only, in exchange local time (NSE: 09:15, 10:15 ...
15:15 -- the last bar is the short 15:15-15:30 one; US: 09:30 ... 15:30).
Prices are unadjusted, like eod_downloader.py.

Progress (done/total, %, ok/cached/no data/failed, elapsed, ETA) is logged
every --progress-every seconds (default 60).
"""

import argparse
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

import eod_downloader as core

log = core.log

HOURLY_EXCHANGES = ["NSE", "NASDAQ", "NYSE"]
YAHOO_1H_MAX_DAYS = 729  # Yahoo rejects 1h requests reaching back more than 730 days
RAW_COLUMNS = ["Datetime", "Open", "High", "Low", "Close", "Volume", "Ticker"]
METASTOCK_HOURLY_HEADER = ["<TICKER>", "<PER>", "<DTYYYYMMDD>", "<TIME>",
                           "<OPEN>", "<HIGH>", "<LOW>", "<CLOSE>", "<VOL>"]
TICKER_CACHE_DIR = Path(__file__).resolve().parent / ".ticker_cache"


def _yf_fetch_hourly(yf_symbol: str, start: str):
    return core.yf.download(yf_symbol, start=start, interval="1h", progress=False,
                            auto_adjust=False, prepost=False, threads=False)


def download_ticker_hourly(ticker: str, exchange: str, raw_dir: Path, retries: int = 3) -> str:
    """Fetch 1h bars for one ticker into raw_dir/<EXCHANGE>.<TICKER>.csv.
    First run: the last ~730 days. Later runs: from the last cached day
    onward, merged into the cache (the newest cached bar is re-fetched since
    it may have been a still-forming bar). Returns ok/cached/no_data/failed."""
    raw_path = raw_dir / f"{core.prefixed_symbol(ticker, exchange)}.csv"
    yf_symbol = core.yf_download_symbol(ticker, exchange)
    earliest = (datetime.now() - timedelta(days=YAHOO_1H_MAX_DAYS)).strftime("%Y-%m-%d")

    existing = None
    fetch_start = earliest
    if raw_path.exists():
        try:
            existing = pd.read_csv(raw_path, parse_dates=["Datetime"], dtype={"Ticker": str})
        except Exception:
            existing = None  # corrupt cache -- full re-fetch
        if existing is not None and existing.empty:
            return "cached"  # no-data marker from an earlier run
        if existing is not None:
            fetch_start = max(earliest, existing["Datetime"].max().strftime("%Y-%m-%d"))

    for attempt in range(1, retries + 1):
        try:
            data = _yf_fetch_hourly(yf_symbol, fetch_start)
            if data is None or data.empty:
                if existing is not None:
                    return "cached"
                pd.DataFrame(columns=RAW_COLUMNS).to_csv(raw_path, index=False)
                return "no_data"

            if isinstance(data.columns, pd.MultiIndex):
                data.columns = [c[0] for c in data.columns]
            data = data.reset_index().rename(columns={"index": "Datetime", "Date": "Datetime"})
            # Keep exchange local time, drop the tz so it round-trips through CSV cleanly.
            if getattr(data["Datetime"].dt, "tz", None) is not None:
                data["Datetime"] = data["Datetime"].dt.tz_localize(None)
            data["Ticker"] = ticker
            data = data[RAW_COLUMNS].dropna(subset=["Open", "High", "Low", "Close"])

            if existing is not None:
                data = pd.concat([existing, data], ignore_index=True)
                data = data.drop_duplicates(subset="Datetime", keep="last")
            data.sort_values("Datetime").to_csv(raw_path, index=False, date_format="%Y-%m-%d %H:%M:%S")
            return "ok"
        except Exception as e:
            log.warning(f"[{yf_symbol}] attempt {attempt}/{retries} failed: {e}")
            time.sleep(attempt)

    log.error(f"[{yf_symbol}] giving up after {retries} attempts")
    return "failed"


class _MinuteProgress:
    """Thread-safe counter that logs done/total (%), status counts and ETA
    at most once every `every` seconds, plus once at the end."""

    def __init__(self, total: int, every: float, label: str = "", unit: str = "tickers"):
        self.total, self.every, self.label, self.unit = total, every, label, unit
        self.counts = {"ok": 0, "cached": 0, "no_data": 0, "failed": 0}
        self.done = 0
        self.t0 = self.t_last = time.time()
        self.lock = threading.Lock()

    def step(self, status: str):
        with self.lock:
            self.done += 1
            self.counts[status] = self.counts.get(status, 0) + 1
            now = time.time()
            if self.done < self.total and now - self.t_last < self.every:
                return
            self.t_last = now
            elapsed = now - self.t0
            pct = 100 * self.done / self.total if self.total else 100
            label = f"{self.label} " if self.label else ""
            msg = f"PROGRESS {pct:5.1f}% | {label}{self.done:,}/{self.total:,} {self.unit} | "
            if self.unit == "tickers":
                msg += (f"new {self.counts['ok']:,}, up-to-date {self.counts['cached']:,}, "
                        f"no data {self.counts['no_data']:,}, failed {self.counts['failed']:,} | ")
            msg += f"elapsed {_fmt_secs(elapsed)}"
            if self.done < self.total:
                msg += f" | ETA {_fmt_secs(elapsed / self.done * (self.total - self.done))}"
            log.info(msg)


def _fmt_secs(s: float) -> str:
    s = int(s)
    return f"{s // 3600}h{s % 3600 // 60:02d}m" if s >= 3600 else f"{s // 60}m{s % 60:02d}s"


def download_all_hourly(pairs, out_root: Path, workers: int = 8, progress_every: float = 60,
                        progress_callback=None, cancel_event: threading.Event = None):
    """pairs: list of (ticker, exchange); each ticker is cached under
    raw_dir_for(out_root, exchange). progress_callback(done, total, failed,
    no_data) is called after every ticker (the GUI uses it). Setting
    cancel_event skips tickers that haven't started yet."""
    for ex in {ex for _, ex in pairs}:
        raw_dir_for(out_root, ex).mkdir(parents=True, exist_ok=True)
    progress = _MinuteProgress(len(pairs), progress_every)
    log.info(f"Downloading 1h bars for {len(pairs):,} ticker(s) with {workers} worker(s) ...")
    failed = []

    def _work(ticker, exchange):
        if cancel_event is not None and cancel_event.is_set():
            return "cancelled"
        return download_ticker_hourly(ticker, exchange, raw_dir_for(out_root, exchange))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_work, t, ex): (t, ex) for t, ex in pairs}
        for fut in as_completed(futures):
            try:
                status = fut.result()
            except Exception as e:
                log.error(f"[{futures[fut][0]}] unexpected error: {e}")
                status = "failed"
            if status == "failed":
                failed.append(futures[fut])
            progress.step(status)
            if progress_callback:
                progress_callback(progress.done, progress.total,
                                  progress.counts["failed"], progress.counts["no_data"])
    if cancel_event is not None and cancel_event.is_set():
        log.info(f"Cancelled: {progress.counts.get('cancelled', 0):,} ticker(s) not started")
    if failed:
        log.warning(f"{len(failed)} ticker(s) failed; re-run to retry them: "
                    f"{[t for t, _ in failed[:15]]}{' ...' if len(failed) > 15 else ''}")
    return progress.counts


def raw_dir_for(out_root: Path, exchange: str) -> Path:
    """<out_root>/<EXCHANGE>/raw_1h -- each exchange keeps its own cache."""
    return out_root / exchange / "raw_1h"


def hour_file_path(out_root: Path, exchange: str, ts: pd.Timestamp) -> Path:
    """<out_root>/<EXCHANGE>/<EXCHANGE>_YYYYMMDD_HHMM.csv, named by the bar's start time."""
    return out_root / exchange / f"{exchange}_{ts.strftime('%Y%m%d_%H%M')}.csv"


def build_hour_files(pairs, out_root: Path, metastock_ascii: bool, start: str = None,
                     end: str = None, progress_every: float = 60):
    """Write one file per exchange per hourly bar, holding every ticker's bar
    for that hour: <out_root>/<EX>/<EX>_YYYYMMDD_HHMM.csv. start/end
    (YYYY-MM-DD, inclusive) optionally limit the bars written.

    An existing hour file is merged, not overwritten: rows for tickers in
    this run are replaced, other tickers' rows are kept -- so a run for a
    few tickers doesn't wipe the rest of the market out of the files."""
    by_exchange = {}
    for ticker, exchange in pairs:
        by_exchange.setdefault(exchange, []).append(ticker)

    for exchange, tickers in by_exchange.items():
        raw_dir = raw_dir_for(out_root, exchange)
        frames = []
        log.info(f"{exchange}: reading {len(tickers):,} cached ticker file(s) from {raw_dir} ...")
        for ticker in tickers:
            raw_path = raw_dir / f"{core.prefixed_symbol(ticker, exchange)}.csv"
            if not raw_path.exists():
                continue
            try:
                df = pd.read_csv(raw_path, parse_dates=["Datetime"], dtype={"Ticker": str})
            except Exception as e:
                log.warning(f"[{raw_path.name}] unreadable, skipped: {e}")
                continue
            if df.empty:
                continue
            df["Ticker"] = ticker
            frames.append(df)
        if not frames:
            log.warning(f"{exchange}: no cached hourly data to write")
            continue

        data = pd.concat(frames, ignore_index=True)
        del frames
        if start:
            data = data[data["Datetime"] >= pd.Timestamp(start)]
        if end:
            data = data[data["Datetime"] < pd.Timestamp(end) + pd.Timedelta(days=1)]
        data = data.dropna(subset=["Open", "High", "Low", "Close"])
        for c in ("Open", "High", "Low", "Close"):
            data[c] = data[c].round(core.PRICE_DECIMALS)
        data["Volume"] = pd.to_numeric(data["Volume"], errors="coerce").fillna(0).round().astype("int64")
        if metastock_ascii:
            data["Symbol"] = data["Ticker"].map(core.metastock_symbol)
        else:
            data["Symbol"] = data["Ticker"].map(lambda t: core.prefixed_symbol(t, exchange))
        run_symbols = set(data["Symbol"].unique())

        n_bars = data["Datetime"].nunique()
        log.info(f"{exchange}: writing {n_bars:,} hourly file(s) ({len(data):,} rows) to {out_root / exchange}")
        progress = _MinuteProgress(n_bars, progress_every, label=f"{exchange} hour files", unit="files")
        for ts, group in data.groupby("Datetime"):
            out = pd.DataFrame({"Symbol": group["Symbol"].values})
            if metastock_ascii:
                out["Period"] = "60"
            out["Date"] = ts.strftime("%Y%m%d")
            out["Time"] = ts.strftime("%H%M%S")
            for c in ("Open", "High", "Low", "Close", "Volume"):
                out[c] = group[c].values
            path = hour_file_path(out_root, exchange, ts)
            if path.exists():
                try:
                    old = pd.read_csv(path, dtype=str)
                    old.columns = out.columns
                    out = pd.concat([old[~old["Symbol"].isin(run_symbols)], out], ignore_index=True)
                except Exception as e:
                    log.warning(f"[{path.name}] couldn't merge with existing file, overwriting: {e}")
            out = out.sort_values("Symbol", kind="stable")
            if metastock_ascii:
                out.to_csv(path, index=False, header=METASTOCK_HOURLY_HEADER, lineterminator="\r\n")
            else:
                out.to_csv(path, index=False)
            progress.step("ok")


def resolve_tickers(args):
    if args.ticker:
        if not args.ticker_exchange:
            sys.exit("--ticker needs --ticker-exchange (NSE, NASDAQ or NYSE)")
        return [(core.normalize_ticker_input(args.ticker), args.ticker_exchange)]

    pairs = []
    for ex in args.exchanges:
        if args.ticker_file:
            df = core.load_tickers_from_file(args.ticker_file, ex)
        else:
            TICKER_CACHE_DIR.mkdir(exist_ok=True)
            df = core.fetch_ticker_directory([ex], include_etfs=not args.no_etfs,
                                             cache_path=TICKER_CACHE_DIR / f"{ex.lower()}_tickers.csv",
                                             refresh=args.refresh_tickers)
            df = df[df["Exchange"] == ex]
        syms = df["Symbol"].dropna().astype(str).tolist()
        if args.limit:
            syms = syms[:args.limit]
        pairs += [(s, ex) for s in syms]
        log.info(f"{ex}: {len(syms):,} ticker(s)")
    return pairs


def parse_args():
    p = argparse.ArgumentParser(description="Free hourly OHLCV downloader (Yahoo, ~730-day window, "
                                            "grows incrementally on re-runs)")
    p.add_argument("--exchanges", nargs="+", choices=HOURLY_EXCHANGES, default=HOURLY_EXCHANGES)
    p.add_argument("--ticker", help="Download a single ticker, e.g. RELIANCE or AAPL")
    p.add_argument("--ticker-exchange", choices=HOURLY_EXCHANGES)
    p.add_argument("--ticker-file", help="CSV with a Symbol column (applied to each --exchanges entry)")
    p.add_argument("--output", default="./market_data_hourly")
    p.add_argument("--limit", type=int, help="Only the first N tickers per exchange (testing)")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--progress-every", type=float, default=60, help="Seconds between progress lines")
    p.add_argument("--metastock-ascii", action="store_true",
                   help="Write <TICKER>,<PER>,<DTYYYYMMDD>,<TIME>,O,H,L,C,<VOL> with PER=60, CRLF")
    p.add_argument("--skip-download", action="store_true", help="Only rebuild the hour files from the raw_1h/ caches")
    p.add_argument("--refresh-tickers", action="store_true")
    p.add_argument("--no-etfs", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    if core.yf is None:
        sys.exit("yfinance is not installed: pip install -r requirements.txt")
    out_root = Path(args.output)
    out_root.mkdir(parents=True, exist_ok=True)
    core.start_file_logging(out_root)
    pairs = resolve_tickers(args)
    if not pairs:
        sys.exit("No tickers to process.")
    if not args.skip_download:
        download_all_hourly(pairs, out_root, args.workers, args.progress_every)
    build_hour_files(pairs, out_root, args.metastock_ascii, progress_every=args.progress_every)
    log.info("Done.")


if __name__ == "__main__":
    main()
