"""Backfill BSE scrips that are no longer listed from BSE's official daily bhavcopies (as-traded prices).

Downloads (and caches) every bhavcopy day file for [--start, today] into <root>/BSE/bhavcopy_cache/, then writes
  <root>/BSE/raw_delisted/BSE.<SYMBOL>.csv   Date,Open,High,Low,Close,Volume,Ticker   one file per delisted scrip
  <root>/BSE/raw_delisted/code_map.csv       Symbol,Code,Name,First,Last,Days
A scrip counts as delisted when its last trading day is more than LIVE_DAYS before the newest cached day. The
files only list scrips that traded that day, so a listed-but-dormant scrip lands here too.
Scrip codes are BSE's stable id (they survive renames). SYMBOL is the last ticker the scrip carried (tickers only
exist in files from 2024-07-08), else the scrip code itself; if that clashes with another scrip it becomes
<SYMBOL>_<CODE>. The files are as traded, so they only mix with --price-basis raw (eod_downloader.py
--include-delisted). Run bse_factors.py for the live scrips' as-traded factors.
"""
import argparse
import logging
import re
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

import bse_bhavcopy as bb

log = logging.getLogger("market_dl")
LIVE_DAYS = 30


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="market_data_daily")
    ap.add_argument("--start", default="2016-10-06")
    ap.add_argument("--end", default=None, help="last day to fetch, inclusive (default today)")
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    bse = Path(args.root) / "BSE"
    cache, out = bse / "bhavcopy_cache", bse / "raw_delisted"
    end = pd.Timestamp(args.end) if args.end else pd.Timestamp(date.today())
    bb.cache_days(args.start, (end + timedelta(days=1)).strftime("%Y-%m-%d"), cache, args.workers)

    log.info("Loading cached day files ...")
    off = bb.load_official(cache)
    if off.empty:
        raise SystemExit("no BSE bhavcopy data in the cache")
    ticker_of = bb.latest_ticker_by_code(off)
    span = off.groupby("Code").Date.agg(["min", "max", "count"])
    cutoff = off.Date.max() - pd.Timedelta(days=LIVE_DAYS)
    live = set(span.index[span["max"] >= cutoff])
    live_syms = {ticker_of[c] for c in live if c in ticker_of}
    gone = span.index.difference(list(live))
    log.info("%d scrips in the cache, %d live, %d delisted", len(span), len(live), len(gone))

    sym_of, used = {}, set(live_syms)
    for c in sorted(gone):
        sym = re.sub(r"[^A-Za-z0-9._&-]", "_", ticker_of.get(c) or c)
        if sym in used:
            sym = f"{sym}_{c}"
        used.add(sym)
        sym_of[c] = sym

    out.mkdir(parents=True, exist_ok=True)
    names = off.sort_values("Date").drop_duplicates("Code", keep="last").set_index("Code").Name
    rows = []
    for c, g in off[off.Code.isin(sym_of)].groupby("Code"):
        g = g.sort_values("Date").drop_duplicates("Date", keep="last")
        sym = sym_of[c]
        d = g[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
        d["Volume"] = d["Volume"].fillna(0).astype("int64")
        d["Ticker"] = sym
        d.to_csv(out / f"BSE.{sym}.csv", index=False, date_format="%Y-%m-%d")
        rows.append((sym, c, names.get(c, ""), g.Date.min().strftime("%Y-%m-%d"), g.Date.max().strftime("%Y-%m-%d"), len(g)))
    pd.DataFrame(rows, columns=["Symbol", "Code", "Name", "First", "Last", "Days"]).sort_values("Symbol").to_csv(
        out / "code_map.csv", index=False)
    log.info("wrote %d delisted scrip file(s) to %s", len(rows), out)


if __name__ == "__main__":
    main()
