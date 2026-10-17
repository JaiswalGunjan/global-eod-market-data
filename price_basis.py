"""Price-basis conversion for the build step (--price-basis raw).

raw/ caches hold Yahoo's retroactively split/bonus/rights-adjusted prices ("split" basis). For NSE,
nse_factors.py derives exact per-date scale factors (<EX>/recon/scale_factors.csv) so that
    as_traded_price  = yahoo_price  * Factor
    as_traded_volume = yahoo_volume / Factor
This module applies them to one ticker's frame. Only NSE has factors; other exchanges are returned unchanged.
"""
import logging
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger("market_dl")
FACTOR_EXCHANGES = {"NSE", "NYSE", "NASDAQ"}   # NSE: nse_factors.py; NYSE/NASDAQ: us_factors.py (Yahoo splits)
_cache = {}          # path -> (mtime, {symbol: (from_dates ndarray, factors ndarray)})


def factors_path(raw_dir: Path) -> Path:
    return Path(raw_dir).parent / "recon" / "scale_factors.csv"


def load_factors(raw_dir: Path):
    """{symbol: (sorted From dates as datetime64, factors)} from <EX>/recon/scale_factors.csv, or {} if absent."""
    p = factors_path(raw_dir)
    if not p.exists():
        return {}
    mtime = p.stat().st_mtime
    hit = _cache.get(p)
    if hit and hit[0] == mtime:
        return hit[1]
    df = pd.read_csv(p, parse_dates=["From"]).sort_values(["Symbol", "From"])
    table = {s: (g.From.values, g.Factor.values) for s, g in df.groupby("Symbol")}
    _cache[p] = (mtime, table)
    return table


def apply_raw_basis(df: pd.DataFrame, exchange, raw_dir: Path, date_col: str = "Date") -> pd.DataFrame:
    """Return df (Ticker, Date, Open, High, Low, Close, Volume) converted to as-traded prices.
    date_col="Datetime" for hourly frames (matched on the bar's calendar day).
    Dates before a ticker's first segment use its first factor; dates after its last segment use the last one."""
    if exchange not in FACTOR_EXCHANGES or df.empty:
        return df
    table = load_factors(raw_dir)
    sym = str(df["Ticker"].iloc[0])
    if sym not in table:
        return df                      # no factor derived (not in official data) -> leave unchanged
    froms, facs = table[sym]
    days = pd.to_datetime(df[date_col]).dt.normalize().values
    idx = np.clip(np.searchsorted(froms, days, side="right") - 1, 0, len(froms) - 1)
    f = facs[idx]
    out = df.copy()
    nd = 2 if exchange == "NSE" else 4
    for c in ("Open", "High", "Low", "Close"):
        out[c] = (out[c] * f).round(nd)
    out["Volume"] = (pd.to_numeric(out["Volume"], errors="coerce") / f).round()
    return out
