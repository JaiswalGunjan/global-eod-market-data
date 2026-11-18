"""Reconcile Yahoo (split-adjusted) NSE closes in yahoo_adjusted/ against NSE's official as-traded bhavcopy.

For each sampled date, factor = official_close / yahoo_close. Yahoo is adjusted for later splits/bonuses, so
factor is the cumulative scale needed to get back to the price that actually traded (1.0 = no later split).
Also lists symbols that traded officially on a sampled date but have no yahoo_adjusted/ file (delisted, renamed, never fetched).

Outputs (under <root>/NSE/recon/):
  implied_factors.csv   Symbol,Date,Official,Yahoo,Factor
  factor_steps.csv      per ticker: the distinct factor levels and the date range of each
  suspicious.csv        tickers whose factor is not a clean split ratio, or changes without a clean step
  missing_from_raw.csv  Date,Symbol,ISIN,Close for official EQ-type symbols with no raw file
Official zips are cached in <root>/NSE/official_raw/.
"""
import argparse
import datetime as dt
import io
import logging
import zipfile
from pathlib import Path

import pandas as pd
import requests

log = logging.getLogger("market_dl")
HEADERS = {"User-Agent": "Mozilla/5.0"}
UDIFF_FROM = dt.date(2024, 7, 8)
EQ_SERIES = {"EQ", "BE", "BZ", "SM", "ST"}


def official_url(d: dt.date) -> str:
    if d >= UDIFF_FROM:
        return f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{d:%Y%m%d}_F_0000.csv.zip"
    mon = d.strftime("%b").upper()
    return (f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{d.year}/"
            f"{mon}/cm{d:%d}{mon}{d.year}bhav.csv.zip")


def load_official(d: dt.date, cache: Path):
    """Return DataFrame[Symbol,Series,Close,ISIN] for date d, or None if d wasn't a trading day."""
    f = cache / f"{d:%Y%m%d}.csv"
    if f.exists():
        df = pd.read_csv(f, dtype=str)
        return None if df.empty else df.assign(Close=lambda x: x.Close.astype(float))
    try:
        r = requests.get(official_url(d), headers=HEADERS, timeout=30)
    except requests.RequestException as e:
        log.warning("%s: %s", d, e)
        return None
    if r.status_code != 200:
        f.write_text("Symbol,Series,Close,ISIN\n")   # marker: holiday / not published
        return None
    z = zipfile.ZipFile(io.BytesIO(r.content))
    raw = pd.read_csv(io.BytesIO(z.read(z.namelist()[0])), dtype=str)
    if "TckrSymb" in raw.columns:
        raw = raw[raw.FinInstrmTp.eq("STK")]
        df = pd.DataFrame({"Symbol": raw.TckrSymb, "Series": raw.SctySrs, "Close": raw.ClsPric, "ISIN": raw.ISIN})
    else:
        df = pd.DataFrame({"Symbol": raw.SYMBOL, "Series": raw.SERIES, "Close": raw.CLOSE, "ISIN": raw.ISIN})
    df = df[df.Series.isin(EQ_SERIES)].copy()
    df.to_csv(f, index=False)
    df["Close"] = df.Close.astype(float)
    return df


def sample_dates(start: dt.date, end: dt.date, every_days: int):
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=every_days)


def first_trading_day(d: dt.date, cache: Path, tries: int = 6):
    for i in range(tries):
        day = d + dt.timedelta(days=i)
        if day.weekday() >= 5:
            continue
        df = load_official(day, cache)
        if df is not None:
            return day, df
    return None, None


def is_clean_ratio(x: float, tol: float = 0.01) -> bool:
    """True if x is ~1 or ~p/q for small integers (splits 2:1, bonus 1:1 -> 2, 3:2 -> 1.5, 10:1 ...)."""
    if x <= 0:
        return False
    for q in range(1, 11):
        for p in range(1, 101):
            if abs(x - p / q) / x <= tol:
                return True
    return False


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="market_data_daily")
    ap.add_argument("--every-days", type=int, default=91, help="sample spacing (default ~quarterly)")
    ap.add_argument("--start", help="first sample date YYYY-MM-DD (default: oldest raw date)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    nse = Path(args.root) / "NSE"
    raw_dir, out = nse / "yahoo_adjusted", nse / "recon"
    cache = nse / "official_raw"
    out.mkdir(exist_ok=True)
    cache.mkdir(exist_ok=True)

    log.info("Loading yahoo_adjusted/ ...")
    yahoo = {}
    for p in raw_dir.glob("NSE.*.csv"):
        if p.stat().st_size < 30:
            continue
        df = pd.read_csv(p, usecols=["Date", "Close"], parse_dates=["Date"]).set_index("Date").Close
        if df.empty:
            continue
        yahoo[p.stem.split(".", 1)[1]] = df[~df.index.duplicated()]
    first = min(s.index.min() for s in yahoo.values()).date()
    last = max(s.index.max() for s in yahoo.values()).date()
    start = dt.date.fromisoformat(args.start) if args.start else first
    log.info("%d raw tickers, %s .. %s", len(yahoo), first, last)

    rows, missing, used = [], [], []
    for target in sample_dates(start, last, args.every_days):
        day, off = first_trading_day(target, cache)
        if day is None or day > last:
            continue
        used.append(day)
        ts = pd.Timestamp(day)
        log.info("sample %s: %d official symbols", day, len(off))
        for sym, ser, close, isin in off[["Symbol", "Series", "Close", "ISIN"]].itertuples(index=False):
            s = yahoo.get(sym)
            if s is None:
                missing.append((day, sym, isin, close))
                continue
            y = s.get(ts)
            if y is None or pd.isna(y) or y <= 0 or close <= 0:
                continue
            rows.append((sym, day, close, float(y), close / float(y)))

    f = pd.DataFrame(rows, columns=["Symbol", "Date", "Official", "Yahoo", "Factor"])
    f["Factor"] = f.Factor.round(4)
    f.to_csv(out / "implied_factors.csv", index=False)
    pd.DataFrame(missing, columns=["Date", "Symbol", "ISIN", "Close"]).to_csv(out / "missing_from_raw.csv", index=False)

    steps, sus = [], []
    for sym, g in f.sort_values("Date").groupby("Symbol"):
        lvl = (g.Factor.round(2) != g.Factor.round(2).shift()).cumsum()
        for _, h in g.groupby(lvl):
            steps.append((sym, h.Date.min(), h.Date.max(), h.Factor.median(), len(h)))
        bad = g[~g.Factor.map(is_clean_ratio)]
        if len(bad):
            sus.append((sym, "non-clean factor", len(bad), bad.Date.min(), bad.Factor.iloc[0]))
        # factor must be non-increasing over time (later date => fewer future splits left to adjust for)
        if (g.Factor.round(2).diff().dropna() > 0.01 * g.Factor.round(2).iloc[:-1].values).any():
            sus.append((sym, "factor rises over time (reverse split / bad data)", 0, g.Date.min(), g.Factor.iloc[0]))
    pd.DataFrame(steps, columns=["Symbol", "From", "To", "Factor", "Samples"]).to_csv(out / "factor_steps.csv", index=False)
    pd.DataFrame(sus, columns=["Symbol", "Issue", "Samples", "FirstDate", "Factor"]).to_csv(out / "suspicious.csv", index=False)

    adj = f.groupby("Symbol").Factor.apply(lambda s: (s.round(2) != 1).any())
    log.info("\n=== NSE reconciliation ===")
    log.info("sample dates: %d | tickers compared: %d", len(used), f.Symbol.nunique())
    log.info("tickers needing a scale factor (!= 1 on some date): %d", int(adj.sum()))
    log.info("suspicious tickers: %d", len({s[0] for s in sus}))
    m = pd.DataFrame(missing, columns=["Date", "Symbol", "ISIN", "Close"])
    log.info("official symbols with no raw file (distinct): %d  <- survivorship gap", m.Symbol.nunique())
    log.info("outputs in %s", out)


if __name__ == "__main__":
    main()
