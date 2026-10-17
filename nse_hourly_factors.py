"""Scale factors for NSE hourly (raw_1h) caches, derived from the exact daily factors plus the hourly cache itself.

nse_factors.py gives exact split/bonus/rights ratios per date for the DAILY Yahoo cache. The hourly cache is a
separate Yahoo feed that is not always adjusted the same way (some splits are adjusted, some left as-traded, some
only partly adjusted), so applying the daily factors blindly would double-adjust or under-adjust those tickers.
For every daily factor step inside the hourly window this measures the price jump across the step in the hourly
cache and computes the multiplier m = ratio / jump that makes the pre-step bars continuous with the as-traded
post-step bars (m ~ ratio: cache adjusted; m ~ 1: already as-traded; otherwise partly adjusted).

Writes <hourly_root>/NSE/recon/scale_factors.csv (same layout as nse_factors.py) and split_evidence.csv.
  python nse_hourly_factors.py [--daily-root market_data_daily] [--hourly-root market_data_hourly]
"""
import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from reconcile_nse import is_clean_ratio
from us_factors import judge_split, segments_for, _read_bars

log = logging.getLogger("market_dl")
MIN_STEP = 0.05      # daily factor steps smaller than this are tick-rounding noise
NEAR = 0.25          # |ln| distance of the jump from 1 (adjusted) or from the ratio (as-traded) that still counts


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--daily-root", default="market_data_daily")
    ap.add_argument("--hourly-root", default="market_data_hourly")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    f = pd.read_csv(Path(args.daily_root) / "NSE" / "recon" / "scale_factors.csv", parse_dates=["From"])
    f = f.sort_values(["Symbol", "From"])
    f["Ratio"] = f.groupby("Symbol").Factor.shift() / f.Factor        # price drop at this step's From date
    steps = f[(f.Ratio - 1).abs() > MIN_STEP]

    raw_dir = Path(args.hourly_root) / "NSE" / "raw_1h"
    rows, evid = [], []
    for sym, g in steps.groupby("Symbol"):
        p = raw_dir / f"NSE.{sym}.csv"
        if not p.exists() or p.stat().st_size < 30:
            continue
        bars = _read_bars(p, hourly=True)
        if bars.empty:
            continue
        applied = []
        for r in g.itertuples():
            d = r.From.strftime("%Y-%m-%d")
            status, jump = judge_split(bars, d, float(r.Ratio))
            if status == "outside":
                evid.append((sym, d, r.Ratio, None, None, status))
                continue
            m = float(r.Ratio)                                       # no usable evidence: trust the daily ratio
            if status != "unknown":
                lj, lr = abs(np.log(jump)), abs(np.log(jump / r.Ratio))
                if lj < NEAR and lj <= lr:
                    status, m = "applied", float(r.Ratio)               # no jump in the cache -> it is adjusted
                elif lr < NEAR:
                    status, m = "already_traded", 1.0                   # jump ~ ratio -> cache already as-traded
                else:
                    m = float(r.Ratio) / jump
                    if is_clean_ratio(m, 0.03):
                        status, m = "partial", round(m, 3)              # cache only partly adjusted
                    else:
                        status, m = "ambiguous", float(r.Ratio)         # keep the daily ratio, flag for review
            evid.append((sym, d, r.Ratio, None if np.isnan(jump) else round(jump, 4), round(m, 4), status))
            if m != 1.0:
                applied.append((d, m))
        if applied:
            dates = bars["Date"].drop_duplicates().reset_index(drop=True)
            for fr, to, fac, days in segments_for(dates, applied):
                rows.append((sym, fr, to, fac, days, True))

    out = Path(args.hourly_root) / "NSE" / "recon"
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=["Symbol", "From", "To", "Factor", "Days", "Clean"]).to_csv(
        out / "scale_factors.csv", index=False)
    ev = pd.DataFrame(evid, columns=["Symbol", "SplitDate", "DailyRatio", "Jump", "Multiplier", "Status"])
    ev.to_csv(out / "split_evidence.csv", index=False)
    log.info(f"NSE hourly: {ev.Symbol.nunique()} tickers with factor steps; {ev.Status.value_counts().to_dict()}")
    log.info(f"-> {out / 'scale_factors.csv'}")


if __name__ == "__main__":
    main()
