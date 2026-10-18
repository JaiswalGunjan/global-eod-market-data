"""Derive exact split/bonus/rights scale factors for NSE by comparing every day's Yahoo close to NSE's official close.

as_traded_price = yahoo_price * Factor          (volume: as_traded_volume = yahoo_volume / Factor)

Needs official_cache/full/ (run nse_delisted.py first). Writes under <root>/NSE/recon/:
  scale_factors.csv   Symbol,From,To,Factor,Days,Clean   one row per constant-factor segment (To is inclusive)
  factor_outliers.csv Symbol,Date,Official,Yahoo,Ratio   days that fit no segment (bad Yahoo/official bar)
  factor_check.csv    one-line summary of how well the segments reproduce the official closes
Segments are changes of > TOL in the official/yahoo ratio that persist for >= CONFIRM days.
"""
import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from reconcile_nse import is_clean_ratio

log = logging.getLogger("market_dl")
TOL = 0.03        # ratio change that starts a new segment (tick rounding on cheap stocks stays below this)
CONFIRM = 3       # consecutive days that must agree on the new level


def segments(dates, logr):
    """Split a ratio series into constant segments. Returns (list of (i0, i1, median_logr), outlier index list)."""
    n, i, segs, outl = len(logr), 0, [], []
    while i < n:
        ref = np.median(logr[i:i + 5])
        j, last_ok = i, i
        while j < n:
            if abs(logr[j] - ref) <= TOL:
                last_ok = j
                j += 1
                continue
            nxt = logr[j:j + CONFIRM]
            if len(nxt) == CONFIRM and np.ptp(nxt) <= TOL and abs(np.median(nxt) - ref) > TOL:
                break                      # persistent shift -> new segment starts at j
            if j + CONFIRM >= n and abs(logr[j] - ref) > TOL and j == n - 1:
                break                      # lone last day that differs: treat as a segment of its own
            outl.append(j)
            j += 1
        idx = [k for k in range(i, j) if k not in set(outl)]
        seg_vals = logr[idx] if idx else logr[i:j]
        segs.append((i, j - 1, float(np.median(seg_vals))))
        i = j
    return segs, outl


def derive_factors(j, out: Path):
    """j: DataFrame(Symbol, Date, Official, Yahoo, Ratio) of matched days. Writes scale_factors.csv,
    factor_outliers.csv and factor_check.csv into `out` (shared with bse_factors.py)."""
    rows, outl = [], []
    for sym, g in j.sort_values(["Symbol", "Date"]).groupby("Symbol", sort=False):
        d, lr = g.Date.values, np.log(g.Ratio.values)
        segs, bad = segments(d, lr)
        for i0, i1, m in segs:
            f = round(float(np.exp(m)), 4)
            rows.append((sym, d[i0], d[i1], f, i1 - i0 + 1, is_clean_ratio(f)))
        outl.extend(g.iloc[bad][["Symbol", "Date", "Official", "Yahoo", "Ratio"]].itertuples(index=False))

    seg = pd.DataFrame(rows, columns=["Symbol", "From", "To", "Factor", "Days", "Clean"])
    seg.to_csv(out / "scale_factors.csv", index=False)
    pd.DataFrame(outl, columns=["Symbol", "Date", "Official", "Yahoo", "Ratio"]).to_csv(out / "factor_outliers.csv", index=False)

    # how well do the segments reproduce official closes?
    lookup = seg.sort_values(["Symbol", "From"])
    j = j.merge(lookup, on="Symbol")
    j = j[(j.Date >= j.From) & (j.Date <= j.To)]
    err = (j.Yahoo * j.Factor / j.Official - 1).abs()
    summary = pd.DataFrame([{
        "tickers": seg.Symbol.nunique(), "segments": len(seg),
        "tickers_with_factor_ne_1": int(seg[(seg.Factor - 1).abs() > 0.005].Symbol.nunique()),
        "non_clean_segments": int((~seg.Clean & ((seg.Factor - 1).abs() > 0.005)).sum()),
        "days_within_0.5pct": round(float((err <= 0.005).mean()), 5),
        "days_within_2pct": round(float((err <= 0.02).mean()), 5),
        "outlier_days": len(outl),
    }])
    summary.to_csv(out / "factor_check.csv", index=False)
    log.info("\n%s", summary.T.to_string(header=False))
    log.info("outputs in %s", out)



def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="market_data_daily")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    nse = Path(args.root) / "NSE"
    raw_dir, cache, out = nse / "raw", nse / "official_cache" / "full", nse / "recon"
    out.mkdir(exist_ok=True)

    log.info("Loading official closes ...")
    off = pd.concat([pd.read_csv(p, usecols=["Date", "Symbol", "Series", "Close"], dtype={"Symbol": str, "Series": str})
                     for p in sorted(cache.glob("*.csv")) if p.stat().st_size > 60], ignore_index=True)
    off["_eq"] = off.Series.eq("EQ")
    off = off.sort_values(["Symbol", "Date", "_eq"]).drop_duplicates(["Symbol", "Date"], keep="last")
    off = off[off.Close > 0].set_index(["Symbol", "Date"]).Close

    log.info("Loading Yahoo raw/ ...")
    frames = []
    for p in raw_dir.glob("NSE.*.csv"):
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

    derive_factors(j, out)


if __name__ == "__main__":
    main()
