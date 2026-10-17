"""Backfill NSE symbols that Yahoo no longer serves (delisted / merged / renamed) from NSE's official bhavcopies.

Downloads every trading day's official bhavcopy (cached in <root>/NSE/official_cache/full/), then writes one file per
symbol that has no usable file in <root>/NSE/raw/:

  <root>/NSE/raw_delisted/NSE.<SYMBOL>.csv   Date,Open,High,Low,Close,Volume,Ticker   (same layout as raw/)
  <root>/NSE/raw_delisted/isin_map.csv       Symbol,ISIN,First,Last,Days,Successor

Prices here are AS TRADED (not split/bonus adjusted), unlike raw/ which Yahoo adjusts. They are kept in a separate
folder so the two bases are never mixed by accident. Successor is the symbol that now carries the same ISIN
(rename/merger of the same listing), blank if none.
"""
import argparse
import concurrent.futures as cf
import datetime as dt
import io
import logging
import zipfile
from pathlib import Path

import pandas as pd
import requests

from reconcile_nse import EQ_SERIES, HEADERS, official_url

log = logging.getLogger("market_dl")
COLS = ["Date", "Symbol", "Series", "Open", "High", "Low", "Close", "Volume", "ISIN"]


def fetch_day(d: dt.date, cache: Path):
    f = cache / f"{d:%Y%m%d}.csv"
    if f.exists():
        return d, True
    for attempt in range(3):
        try:
            r = requests.get(official_url(d), headers=HEADERS, timeout=30)
        except requests.RequestException:
            continue
        if r.status_code == 404:
            f.write_text(",".join(COLS) + "\n")      # holiday marker
            return d, True
        if r.status_code != 200:
            continue
        z = zipfile.ZipFile(io.BytesIO(r.content))
        raw = pd.read_csv(io.BytesIO(z.read(z.namelist()[0])), dtype=str)
        if "TckrSymb" in raw.columns:
            raw = raw[raw.FinInstrmTp.eq("STK")]
            df = pd.DataFrame({"Symbol": raw.TckrSymb, "Series": raw.SctySrs, "Open": raw.OpnPric, "High": raw.HghPric,
                               "Low": raw.LwPric, "Close": raw.ClsPric, "Volume": raw.TtlTradgVol, "ISIN": raw.ISIN})
        else:
            df = pd.DataFrame({"Symbol": raw.SYMBOL, "Series": raw.SERIES, "Open": raw.OPEN, "High": raw.HIGH,
                               "Low": raw.LOW, "Close": raw.CLOSE, "Volume": raw.TOTTRDQTY, "ISIN": raw.ISIN})
        df = df[df.Series.isin(EQ_SERIES)].copy()
        df.insert(0, "Date", f"{d:%Y-%m-%d}")
        df[COLS].to_csv(f, index=False)
        return d, True
    return d, False


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="market_data_daily")
    ap.add_argument("--start", help="YYYY-MM-DD (default: oldest raw date)")
    ap.add_argument("--end", help="YYYY-MM-DD (default: newest raw date)")
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    nse = Path(args.root) / "NSE"
    raw_dir, cache, out = nse / "raw", nse / "official_cache" / "full", nse / "raw_delisted"
    cache.mkdir(parents=True, exist_ok=True)
    out.mkdir(exist_ok=True)

    have = {p.stem.split(".", 1)[1] for p in raw_dir.glob("NSE.*.csv") if p.stat().st_size > 30}
    if args.start and args.end:
        start, end = dt.date.fromisoformat(args.start), dt.date.fromisoformat(args.end)
    else:
        dates = [pd.read_csv(p, usecols=["Date"]).Date for p in list(raw_dir.glob("NSE.TCS.csv"))]
        start = dt.date.fromisoformat(args.start) if args.start else pd.to_datetime(dates[0]).min().date()
        end = dt.date.fromisoformat(args.end) if args.end else pd.to_datetime(dates[0]).max().date()
    days = [start + dt.timedelta(n) for n in range((end - start).days + 1)
            if (start + dt.timedelta(n)).weekday() < 5]
    log.info("%d existing raw tickers; fetching %d weekdays %s..%s", len(have), len(days), start, end)

    done = failed = 0
    with cf.ThreadPoolExecutor(args.workers) as ex:
        for d, ok in ex.map(lambda d: fetch_day(d, cache), days):
            done += 1
            failed += not ok
            if done % 100 == 0:
                log.info("  %d/%d days (failed %d)", done, len(days), failed)
    if failed:
        log.warning("%d days failed to download; re-run to retry them", failed)

    log.info("Assembling ...")
    frames = [pd.read_csv(p, dtype=str) for p in sorted(cache.glob("*.csv")) if p.stat().st_size > 60]
    allp = pd.concat(frames, ignore_index=True)
    allp = allp[(allp.Date >= f"{start}") & (allp.Date <= f"{end}")]
    for c in ("Open", "High", "Low", "Close", "Volume"):
        allp[c] = pd.to_numeric(allp[c], errors="coerce")
    allp["_eq"] = allp.Series.eq("EQ")
    # one row per symbol/day, preferring the EQ series
    allp = allp.sort_values(["Symbol", "Date", "_eq"]).drop_duplicates(["Symbol", "Date"], keep="last")

    missing = allp[~allp.Symbol.isin(have)]
    latest_isin = allp.sort_values("Date").drop_duplicates("ISIN", keep="last").set_index("ISIN").Symbol
    mapping = []
    for sym, g in missing.groupby("Symbol"):
        g = g.sort_values("Date")
        g = g[(g.Close > 0)]
        if g.empty:
            continue
        g[["Date", "Open", "High", "Low", "Close", "Volume"]].assign(Ticker=sym).to_csv(
            out / f"NSE.{sym}.csv", index=False)
        isin = g.ISIN.iloc[-1]
        succ = latest_isin.get(isin, "")
        mapping.append((sym, isin, g.Date.iloc[0], g.Date.iloc[-1], len(g), succ if succ != sym else ""))
    m = pd.DataFrame(mapping, columns=["Symbol", "ISIN", "First", "Last", "Days", "Successor"])
    m.to_csv(out / "isin_map.csv", index=False)

    log.info("\n=== NSE delisted backfill ===")
    log.info("symbols written to raw_delisted/: %d (%d price rows)", len(m), len(missing))
    log.info("of these, renamed/merged onto another live symbol (same ISIN): %d", int(m.Successor.ne("").sum()))
    log.info("last traded before %s (truly gone): %d", end - dt.timedelta(days=30), int((m.Last < f"{end - dt.timedelta(days=30)}").sum()))


if __name__ == "__main__":
    main()
