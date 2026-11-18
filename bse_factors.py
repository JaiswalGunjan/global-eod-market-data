"""Derive as-traded scale factors for BSE by comparing the cached yahoo_adjusted/ closes with BSE's official (unadjusted) closes.

as_traded_price = raw_price * Factor          (volume: as_traded_volume = raw_volume / Factor)

Needs <root>/BSE/bhavcopy_cache/ (run bse_delisted.py first) and the live scrips' yahoo_adjusted/BSE.<TICKER>.csv, whether those
came from Yahoo or from the bhavcopy source. Writes under <root>/BSE/recon/ (same files as nse_factors.py):
scale_factors.csv, factor_outliers.csv, factor_check.csv. After this, eod_downloader.py --price-basis raw works for BSE.
Scrips are matched by their latest ticker, so symbols that were renamed before mid-2024 only get factors under the
current ticker (yahoo_adjusted/ is keyed by that anyway).
"""
import argparse
import logging
from pathlib import Path

import pandas as pd

import bse_bhavcopy as bb
from nse_factors import derive_factors

log = logging.getLogger("market_dl")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="market_data_daily")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    bse = Path(args.root) / "BSE"
    raw_dir, out = bse / "yahoo_adjusted", bse / "recon"
    out.mkdir(exist_ok=True)

    log.info("Loading official closes ...")
    allrows = bb.load_official(bse / "bhavcopy_cache")
    ticker_of = bb.latest_ticker_by_code(allrows)
    allrows["Symbol"] = allrows.Code.map(ticker_of)
    off = (allrows.dropna(subset=["Symbol"]).query("Close > 0")
           .assign(Date=lambda d: d.Date.dt.strftime("%Y-%m-%d"))
           .drop_duplicates(["Symbol", "Date"], keep="last").set_index(["Symbol", "Date"]).Close)

    log.info("Loading yahoo_adjusted/ ...")
    frames = []
    for p in raw_dir.glob("BSE.*.csv"):
        if p.stat().st_size < 30:
            continue
        d = pd.read_csv(p, usecols=["Date", "Close"])
        if d.empty:
            continue
        d["Symbol"] = p.stem.split(".", 1)[1]
        frames.append(d)
    yh = pd.concat(frames, ignore_index=True)
    yh["Date"] = yh.Date.astype(str).str[:10]
    yh = yh[yh.Close > 0].drop_duplicates(["Symbol", "Date"]).set_index(["Symbol", "Date"]).Close

    j = pd.concat([off.rename("Official"), yh.rename("Yahoo")], axis=1, join="inner").reset_index()
    j["Ratio"] = j.Official / j.Yahoo
    log.info("%d tickers, %d matched days", j.Symbol.nunique(), len(j))
    j = j.sort_values(["Symbol", "Date"])
    derive_factors(j, out)


if __name__ == "__main__":
    main()
