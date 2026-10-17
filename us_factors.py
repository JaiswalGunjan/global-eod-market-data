"""As-traded scale factors for NYSE / NASDAQ (daily and hourly) from Yahoo's own split history.

Yahoo's cached prices are retroactively split-adjusted (bonus/stock dividends are reported by Yahoo as splits too).
For each ticker this fetches Ticker.splits (cached in <root>/<EX>/recon/splits.json) and writes
<root>/<EX>/recon/scale_factors.csv in the same layout nse_factors.py uses, so price_basis.py applies it unchanged:
    as_traded_price = yahoo_price * Factor          (volume: as_traded_volume = yahoo_volume / Factor)
Factor for a date = product of the ratios of every split that happened AFTER that date.

The raw caches grow incrementally, so one file can mix bars fetched before and after a split. For each split the
cached prices around the split date are inspected: a price jump of about the split ratio means those bars are
already as-traded (split NOT applied); no jump means Yahoo adjusted them (split applied). Verdicts are listed in
<root>/<EX>/recon/split_evidence.csv. Splits outside the cached date range are ignored.

Delisted names: Yahoo has no delisted directory, so pass --delisted-file (a list of symbols). They are downloaded
into the same raw cache (when Yahoo still serves them), recorded in <root>/<EX>/delisted_tickers.csv, and then
included in --price-basis raw builds with --include-delisted. Spin-offs and mergers are not split events and are
not adjusted.

  python us_factors.py --root market_data_daily  --exchanges NYSE NASDAQ
  python us_factors.py --root market_data_hourly --interval hourly --exchanges NYSE
  python us_factors.py --exchanges NYSE --delisted-file my_delisted.txt
"""
import argparse
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

import eod_downloader as core

log = logging.getLogger("market_dl")
US_EXCHANGES = ["NYSE", "NASDAQ"]
WINDOW = 3            # bars each side of the split date used to judge the price jump
MAX_GAP_DAYS = 10     # bars further than this from the split date give no evidence


def fetch_splits(ticker: str, exchange: str):
    """[(YYYY-MM-DD, ratio)] from Yahoo; ratio 4.0 = 4-for-1, 0.1 = 1-for-10 reverse."""
    s = core.yf.Ticker(core.yf_download_symbol(ticker, exchange)).splits
    if s is None or len(s) == 0:
        return []
    return [(pd.Timestamp(d).strftime("%Y-%m-%d"), float(r)) for d, r in s.items() if r and r > 0]


def load_split_cache(path: Path):
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def refresh_splits(tickers, exchange, cache_path: Path, max_age_days: int, workers: int = 8):
    cache = load_split_cache(cache_path)
    cutoff = (date.today() - timedelta(days=max_age_days)).isoformat()
    todo = [t for t in tickers if cache.get(t, {}).get("fetched", "") < cutoff]
    log.info(f"{exchange}: split history cached for {len(tickers) - len(todo):,}, fetching {len(todo):,}")
    done, last = 0, time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_splits, t, exchange): t for t in todo}
        for fut in as_completed(futs):
            t = futs[fut]
            try:
                cache[t] = {"fetched": date.today().isoformat(), "splits": fut.result()}
            except Exception as e:
                log.warning(f"[{t}] couldn't fetch splits: {e}")
            done += 1
            if time.time() - last > 30:
                log.info(f"Splits: {done}/{len(todo)}")
                last = time.time()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache))
    return cache


def judge_split(bars: pd.DataFrame, split_date: str, ratio: float):
    """Returns (status, jump). bars: sorted frame with Date (day), Open, Close.
    'applied' = cache is adjusted for this split, 'already_traded' = cache shows the real jump,
    'outside' / 'unknown' = no usable evidence (treated as: do nothing for outside, applied for unknown)."""
    sd = pd.Timestamp(split_date)
    if abs(ratio - 1) < 1e-9:
        return "unknown", np.nan
    if bars.empty or sd <= bars["Date"].iloc[0] or sd > bars["Date"].iloc[-1]:
        return "outside", np.nan
    pos = int(np.searchsorted(bars["Date"].values, np.datetime64(sd), side="left"))
    before, after = bars.iloc[max(0, pos - WINDOW):pos], bars.iloc[pos:pos + WINDOW]
    if before.empty or after.empty:
        return "unknown", np.nan
    if (sd - before["Date"].iloc[-1]).days > MAX_GAP_DAYS or (after["Date"].iloc[0] - sd).days > MAX_GAP_DAYS:
        return "unknown", np.nan
    jump = float(before["Close"].median() / after["Open"].median())
    if not np.isfinite(jump) or jump <= 0:
        return "unknown", np.nan
    return ("already_traded" if abs(np.log(jump) - np.log(ratio)) < abs(np.log(jump)) else "applied"), jump


def segments_for(bars_dates: pd.Series, applied):
    """applied: sorted [(split_date, ratio)]. Rows (From, To, Factor, Days) for the constant-factor segments."""
    first, last = bars_dates.iloc[0], bars_dates.iloc[-1]
    ratios = [r for _, r in applied]
    starts = [first] + [pd.Timestamp(d) for d, _ in applied]
    out = []
    for i, st in enumerate(starts):
        fac = float(np.prod(ratios[i:])) if i < len(ratios) else 1.0
        en = (starts[i + 1] - pd.Timedelta(days=1)) if i + 1 < len(starts) else last
        days = int(((bars_dates >= st) & (bars_dates <= en)).sum())
        out.append((st.strftime("%Y-%m-%d"), en.strftime("%Y-%m-%d"), float(f"{fac:.10g}"), days))
    return out


def _read_bars(path: Path, hourly: bool):
    col = "Datetime" if hourly else "Date"
    df = pd.read_csv(path, usecols=[col, "Open", "Close"], parse_dates=[col])
    df = df.dropna(subset=["Open", "Close"])
    df = df[(df["Open"] > 0) & (df["Close"] > 0)]
    if df.empty:
        return df
    df["Date"] = df[col].dt.normalize()
    return df.sort_values(col).reset_index(drop=True)


def build_exchange(root: Path, exchange: str, hourly: bool, max_age_days: int, workers: int):
    ex_dir = root / exchange
    raw_dir = ex_dir / ("raw_1h" if hourly else "raw")
    prefix = f"{core.EXCHANGE_PREFIX[exchange]}."
    files = {p.stem[len(prefix):]: p for p in raw_dir.glob(f"{prefix}*.csv") if p.stat().st_size > 30}
    if not files:
        log.warning(f"{exchange}: no cached files in {raw_dir}")
        return
    recon = ex_dir / "recon"
    cache = refresh_splits(sorted(files), exchange, recon / "splits.json", max_age_days, workers)

    rows, evid = [], []
    for sym, p in sorted(files.items()):
        splits = sorted(cache.get(sym, {}).get("splits", []))
        if not splits:
            continue
        try:
            bars = _read_bars(p, hourly)
        except Exception as e:
            log.warning(f"[{p.name}] unreadable, skipped: {e}")
            continue
        if bars.empty:
            continue
        applied = []
        for d, r in splits:
            status, jump = judge_split(bars, d, r)
            evid.append((sym, d, r, None if np.isnan(jump) else round(jump, 4), status))
            if status in ("applied", "unknown"):
                applied.append((d, r))
        applied = [(d, r) for d, r in applied if d > bars["Date"].iloc[0].strftime("%Y-%m-%d")
                   and d <= bars["Date"].iloc[-1].strftime("%Y-%m-%d")]
        if not applied:
            continue
        for fr, to, fac, days in segments_for(bars["Date"].drop_duplicates().reset_index(drop=True), applied):
            rows.append((sym, fr, to, fac, days, True))

    recon.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=["Symbol", "From", "To", "Factor", "Days", "Clean"]).to_csv(
        recon / "scale_factors.csv", index=False)
    pd.DataFrame(evid, columns=["Symbol", "SplitDate", "Ratio", "Jump", "Status"]).to_csv(
        recon / "split_evidence.csv", index=False)
    ev = pd.DataFrame(evid, columns=["Symbol", "SplitDate", "Ratio", "Jump", "Status"])
    log.info(f"{exchange}: {len(files):,} files, {ev.Symbol.nunique():,} with splits in range "
             f"({ev.Status.value_counts().to_dict()}); factors -> {recon / 'scale_factors.csv'}")


def fetch_delisted(root: Path, exchange: str, hourly: bool, list_file: str, start: str, workers: int):
    """Download the symbols in list_file into the exchange's raw cache and remember them in delisted_tickers.csv."""
    df = core.load_tickers_from_file(list_file, exchange)
    syms = df["Symbol"].dropna().astype(str).tolist()
    log.info(f"{exchange}: downloading {len(syms):,} listed (possibly delisted) symbol(s) ...")
    if hourly:
        import hourly_downloader as hourly_mod
        hourly_mod.download_all_hourly([(s, exchange) for s in syms], root, workers)
        raw_dir = hourly_mod.raw_dir_for(root, exchange)
    else:
        raw_dir = root / exchange / "raw"
        core.download_all(syms, start, date.today().isoformat(), raw_dir, exchange_map={s: exchange for s in syms},
                          max_workers=workers)
    have = [s for s in syms if (raw_dir / f"{core.prefixed_symbol(s, exchange)}.csv").exists()
            and (raw_dir / f"{core.prefixed_symbol(s, exchange)}.csv").stat().st_size > 30]
    log.info(f"{exchange}: Yahoo returned data for {len(have):,}/{len(syms):,}")
    reg = root / exchange / "delisted_tickers.csv"
    old = pd.read_csv(reg)["Symbol"].astype(str).tolist() if reg.exists() else []
    pd.DataFrame({"Symbol": sorted(set(old) | set(have))}).to_csv(reg, index=False)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", help="default: market_data_daily (or market_data_hourly with --interval hourly)")
    ap.add_argument("--interval", choices=["daily", "hourly"], default="daily")
    ap.add_argument("--exchanges", nargs="+", choices=US_EXCHANGES, default=US_EXCHANGES)
    ap.add_argument("--delisted-file", help="CSV with a Symbol column to download and treat as delisted names; "
                                            "{ex} in the path is replaced by each exchange")
    ap.add_argument("--start", default="2000-01-01", help="Start date for --delisted-file downloads (daily)")
    ap.add_argument("--split-max-age", type=int, default=7, help="Re-fetch split history older than N days")
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    hourly = args.interval == "hourly"
    root = Path(args.root or ("market_data_hourly" if hourly else "market_data_daily"))
    for ex in args.exchanges:
        if args.delisted_file:
            fetch_delisted(root, ex, hourly, args.delisted_file.format(ex=ex), args.start, args.workers)
        build_exchange(root, ex, hourly, args.split_max_age, args.workers)


if __name__ == "__main__":
    main()
