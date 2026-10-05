#!/usr/bin/env python3
"""
BSE official bhavcopy -> per-symbol history
============================================

An alternative to Yahoo Finance for BSE tickers. BSE publishes one "bhavcopy"
file per trading day (every security's OHLCV); this module downloads those
day files, caches each one, and stitches them into the same per-symbol
`raw/BSE.<TICKER>.csv` files the rest of the pipeline already reads.

Why: Yahoo's BSE (.BO) history is patchy, and BSE-only stocks have no NSE
listing to fall back on. The official files cover every BSE security.

Two file formats, picked by date:
  * 2024-07-08 onward : BhavCopy_BSE_CM_0_0_0_<YYYYMMDD>_F_0000.CSV  (UDiFF)
  * earlier           : EQ<DDMMYY>_CSV.ZIP  (verified back to 2016)
Weekends/holidays return an HTML page instead of a file; that is recorded as
"no trading that day" rather than an error.

Prices in the files are NOT adjusted for splits/bonuses (BSE doesn't reset
the previous-close column on ex-dates either -- verified on Tata Steel's
2022 10:1 split). With adjust_splits=True, split/bonus events are taken from
Yahoo's corporate-actions data (the NSE listing first, then the BSE one;
cached in cache_dir/splits.json) and earlier prices/volumes are back-adjusted,
matching Yahoo's split-adjusted Close. Dividends are not adjusted. Any
remaining overnight gap that looks like an unrecorded split (a BSE-only stock
Yahoo knows nothing about) is NOT adjusted but is listed in the log.

Day files are cached under cache_dir/YYYYMMDD.csv, so re-runs only fetch new
days.
"""

import io
import json
import logging
import threading
import time
import zipfile
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

log = logging.getLogger("market_dl")  # same logger as the main module -> shows up in the GUI log

BASE_URL = "https://www.bseindia.com/download/BhavCopy/Equity/"
NEW_URL = BASE_URL + "BhavCopy_BSE_CM_0_0_0_{ymd}_F_0000.CSV"
OLD_URL = BASE_URL + "EQ{dmy}_CSV.ZIP"
NEW_FORMAT_FROM = date(2024, 7, 8)
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# Columns kept in the per-day cache files.
DAY_COLS = ["Code", "Ticker", "Name", "Open", "High", "Low", "Close", "PrevClose", "Volume"]

# After adjusting, an overnight close-to-close move beyond these ratios is
# reported as a possible unadjusted corporate action (never auto-adjusted).
SUSPECT_LOW, SUSPECT_HIGH = 0.55, 1.8
SPLITS_MAX_AGE_DAYS = 7  # how long cached Yahoo split data is trusted


# --------------------------------------------------------------------------
# One day's file
# --------------------------------------------------------------------------

def _num(s):
    return pd.to_numeric(s, errors="coerce")


def _parse_new(text: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(text), dtype=str)
    df.columns = [c.strip() for c in df.columns]
    if "FinInstrmTp" in df.columns:
        df = df[df["FinInstrmTp"].str.strip() == "STK"]
    return pd.DataFrame({
        "Code": df["FinInstrmId"].str.strip(),
        "Ticker": df["TckrSymb"].str.strip(),
        "Name": df["FinInstrmNm"].str.strip(),
        "Open": _num(df["OpnPric"]), "High": _num(df["HghPric"]),
        "Low": _num(df["LwPric"]), "Close": _num(df["ClsPric"]),
        "PrevClose": _num(df["PrvsClsgPric"]), "Volume": _num(df["TtlTradgVol"]),
    })


def _parse_old(raw: bytes) -> pd.DataFrame:
    z = zipfile.ZipFile(io.BytesIO(raw))
    df = pd.read_csv(z.open(z.namelist()[0]), dtype=str)
    df.columns = [c.strip() for c in df.columns]
    return pd.DataFrame({
        "Code": df["SC_CODE"].str.strip(),
        "Ticker": "",  # old files carry no ticker; mapped from the latest file instead
        "Name": df["SC_NAME"].str.strip(),
        "Open": _num(df["OPEN"]), "High": _num(df["HIGH"]),
        "Low": _num(df["LOW"]), "Close": _num(df["CLOSE"]),
        "PrevClose": _num(df["PREVCLOSE"]), "Volume": _num(df["NO_OF_SHRS"]),
    })


def fetch_day(d: date, retries: int = 3):
    """One trading day's bhavcopy as a DataFrame[DAY_COLS], or None if BSE
    published nothing for that date (holiday/weekend/not out yet). Network
    errors are retried, then raised."""
    url = (NEW_URL.format(ymd=d.strftime("%Y%m%d")) if d >= NEW_FORMAT_FROM
           else OLD_URL.format(dmy=d.strftime("%d%m%y")))
    last = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=45)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            body = resp.content
            if body[:200].lstrip().lower().startswith((b"<!doc", b"<html")):
                return None  # BSE's "no file for this date" page (served with HTTP 200)
            df = _parse_new(body.decode("utf-8-sig")) if d >= NEW_FORMAT_FROM else _parse_old(body)
            df = df[(df["Code"] != "") & (df["Close"] > 0)]
            return df.reset_index(drop=True)
        except Exception as e:
            last = e
            if attempt < retries:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"{d}: {last}")


def _cached_day(d: date, cache_dir: Path):
    """(status, df): status 'ok' (df has rows), 'holiday' (nothing published),
    or 'missing' (never fetched)."""
    p = cache_dir / f"{d:%Y%m%d}.csv"
    if not p.exists():
        return "missing", None
    try:
        df = pd.read_csv(p, dtype={"Code": str, "Ticker": str, "Name": str}).fillna({"Ticker": "", "Name": ""})
    except Exception:
        return "missing", None  # corrupt cache file: refetch
    return ("ok", df) if len(df) else ("holiday", None)


def _store_day(d: date, df, cache_dir: Path):
    p = cache_dir / f"{d:%Y%m%d}.csv"
    out = df if df is not None else pd.DataFrame(columns=DAY_COLS)
    out.to_csv(p, index=False)


def _latest_ticker_map(cache_dir: Path) -> dict:
    """{ticker: code} from the most recent published bhavcopy (new format
    carries tickers). Used to translate the tickers the rest of the tool
    works in into BSE scrip codes, which is what every file is keyed by."""
    for back in range(0, 11):
        d = date.today() - timedelta(days=back)
        if d < NEW_FORMAT_FROM:
            break
        status, df = _cached_day(d, cache_dir)
        if status == "missing":
            try:
                df = fetch_day(d)
            except Exception:
                continue
            _store_day(d, df, cache_dir)
        elif status == "holiday":
            continue
        if df is not None and len(df):
            m = df[df["Ticker"] != ""]
            return dict(zip(m["Ticker"], m["Code"]))
    return {}


# --------------------------------------------------------------------------
# Split / bonus adjustment
# --------------------------------------------------------------------------

def _yahoo_splits(ticker: str) -> list:
    """[[YYYY-MM-DD, ratio], ...] for one ticker from Yahoo (ratio = new
    shares per old, e.g. 10.0 for a 10:1 split; bonus issues appear here too).
    Tries the NSE listing first (much better populated), then BSE."""
    import yfinance as yf
    found = {}
    for suffix in (".NS", ".BO"):
        try:
            sp = yf.Ticker(ticker + suffix).splits
        except Exception:
            continue
        if sp is None:  # symbol Yahoo doesn't know (bonds, ETFs, BSE-only names)
            continue
        for ts, ratio in sp.items():
            if ratio and ratio > 0:
                found.setdefault(ts.strftime("%Y-%m-%d"), float(ratio))
    return sorted([d, r] for d, r in found.items())


def load_splits(tickers, cache_dir: Path, max_workers: int = 8, cancel_event=None) -> dict:
    """{ticker: [[date, ratio], ...]}, from cache_dir/splits.json where fresh
    (<SPLITS_MAX_AGE_DAYS old), otherwise fetched from Yahoo."""
    path = cache_dir / "splits.json"
    try:
        cache = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        cache = {}
    cutoff = (date.today() - timedelta(days=SPLITS_MAX_AGE_DAYS)).isoformat()
    todo = [t for t in tickers if cache.get(t, {}).get("fetched", "") < cutoff]
    if todo:
        log.info(f"Fetching split/bonus history from Yahoo for {len(todo)} ticker(s)...")
        done = 0
        with ThreadPoolExecutor(max_workers=max(1, max_workers)) as ex:
            futures = {ex.submit(_yahoo_splits, t): t for t in todo}
            for fut in as_completed(futures):
                if cancel_event is not None and cancel_event.is_set():
                    for f in futures:
                        f.cancel()
                    break
                try:
                    cache[futures[fut]] = {"fetched": date.today().isoformat(), "splits": fut.result()}
                except CancelledError:
                    continue
                except Exception as e:
                    log.warning(f"[{futures[fut]}] couldn't fetch splits: {e}")
                done += 1
                if done % 500 == 0:
                    log.info(f"Splits: {done}/{len(todo)}")
        try:
            path.write_text(json.dumps(cache), encoding="utf-8")
        except Exception:
            pass
    return {t: cache.get(t, {}).get("splits", []) for t in tickers}


def adjust_for_splits(g: pd.DataFrame, splits) -> tuple:
    """g: one scrip's rows sorted by Date. Divides prices (and multiplies
    volume) on every row BEFORE each split date. Returns (frame, n_applied)."""
    g = g.copy()
    first, last = g["Date"].iloc[0], g["Date"].iloc[-1]
    applied = 0
    for d, ratio in splits:
        ts = pd.Timestamp(d)
        if not (first < ts <= last) or ratio == 1:
            continue
        before = g["Date"] < ts
        for c in ("Open", "High", "Low", "Close"):
            g.loc[before, c] = g.loc[before, c] / ratio
        g.loc[before, "Volume"] = g.loc[before, "Volume"] * ratio
        applied += 1
    if applied:
        for c in ("Open", "High", "Low", "Close"):
            g[c] = g[c].round(4)
        g["Volume"] = g["Volume"].round(0)
    return g, applied


def suspect_gaps(g: pd.DataFrame) -> list:
    """[(date, ratio)] overnight close moves that look like an unadjusted
    split/bonus (see SUSPECT_LOW/HIGH)."""
    r = g["Close"] / g["Close"].shift(1)
    bad = (r < SUSPECT_LOW) | (r > SUSPECT_HIGH)
    return [(g["Date"].loc[i].strftime("%Y-%m-%d"), float(r.loc[i])) for i in g.index[bad]]


# --------------------------------------------------------------------------
# Public entry point
# --------------------------------------------------------------------------

def download_bse(tickers, start: str, end: str, raw_dir: Path, cache_dir: Path = None,
                 max_workers: int = 8, progress_callback=None, cancel_event=None,
                 adjust_splits: bool = True) -> dict:
    """Build raw/BSE.<TICKER>.csv for each ticker from BSE's official daily
    files, for [start, end) (end exclusive, like yfinance).

    progress_callback(done, total, failed, no_data) is called per day-file
    fetched (so `total` counts days, not tickers). Returns
    {"ok": n, "no_data": [...], "failed_days": [...]}."""
    raw_dir = Path(raw_dir)
    cache_dir = Path(cache_dir) if cache_dir else raw_dir.parent / "bhavcopy_cache"
    raw_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    start_d = datetime.strptime(start, "%Y-%m-%d").date()
    end_d = datetime.strptime(end, "%Y-%m-%d").date()
    days = [start_d + timedelta(days=i) for i in range((end_d - start_d).days)]

    tmap = _latest_ticker_map(cache_dir)
    if not tmap:
        raise RuntimeError("couldn't read BSE's latest bhavcopy to map tickers to scrip codes "
                           "(BSE site down or blocked?)")
    code_of = {t: tmap[t] for t in tickers if t in tmap}
    unknown = [t for t in tickers if t not in tmap]
    if unknown:
        log.warning(f"{len(unknown)} BSE ticker(s) not in BSE's latest bhavcopy (delisted or "
                    f"renamed?), skipping: {unknown[:10]}{' ...' if len(unknown) > 10 else ''}")
    wanted = set(code_of.values())
    ticker_of = {c: t for t, c in code_of.items()}

    log.info(f"BSE bhavcopy: {len(days)} calendar day(s) {start_d} -> {end_d}, "
             f"{len(code_of)} ticker(s); cache {cache_dir}")

    frames, failed_days = [], []
    holidays = done = 0
    lock = threading.Lock()
    today = date.today()
    t0 = time.time()

    def _work(d):
        status, df = _cached_day(d, cache_dir)
        if status == "missing":
            df = fetch_day(d)
            # Don't remember "nothing published" for the last day or two:
            # today's file simply may not be out yet.
            if df is not None or d < today - timedelta(days=1):
                _store_day(d, df, cache_dir)
        if df is None or not len(df):
            return d, None
        return d, df[df["Code"].isin(wanted)].assign(Date=pd.Timestamp(d))

    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as ex:
        futures = {ex.submit(_work, d): d for d in days}
        cancelled = False
        for fut in as_completed(futures):
            if cancel_event is not None and cancel_event.is_set() and not cancelled:
                cancelled = True
                for f in futures:
                    f.cancel()
            d = futures[fut]
            try:
                _, df = fut.result()
            except CancelledError:
                continue
            except Exception as e:
                log.error(f"BSE bhavcopy {d}: {e}")
                with lock:
                    failed_days.append(pd.Timestamp(d))
                    done += 1
                df = None
            else:
                with lock:
                    done += 1
                    if df is None:
                        holidays += 1
                    else:
                        frames.append(df)
            if progress_callback:
                progress_callback(done, len(days), len(failed_days), holidays)
            if done % 250 == 0:
                log.info(f"BSE bhavcopy {done}/{len(days)} days | failed={len(failed_days)} | "
                         f"no-file={holidays} | {time.time() - t0:.0f}s")

    if cancel_event is not None and cancel_event.is_set():
        log.warning("BSE bhavcopy download cancelled; fetched day files stay cached.")
        return {"ok": 0, "no_data": [], "failed_days": [d.date() for d in failed_days]}

    if failed_days:
        log.warning(f"{len(failed_days)} day file(s) failed to download; re-run to retry them "
                    f"(those days are missing from the output until you do).")

    all_rows = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=DAY_COLS + ["Date"])
    ok, no_data, adjusted_events = 0, [], 0
    suspects = []
    by_code = {c: g for c, g in all_rows.groupby("Code")}
    splits = {}
    if adjust_splits:
        splits = load_splits([ticker_of[c] for c in by_code if c in ticker_of], cache_dir,
                             cancel_event=cancel_event)
    for code, ticker in ticker_of.items():
        g = by_code.get(code)
        raw_path = raw_dir / f"BSE.{ticker}.csv"
        if g is None or g.empty:
            pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]).to_csv(
                raw_path, index=False)
            no_data.append(ticker)
            continue
        g = g.sort_values("Date").reset_index(drop=True)
        if adjust_splits:
            g, n = adjust_for_splits(g, splits.get(ticker, []))
            adjusted_events += n
            suspects.extend((ticker, d, r) for d, r in suspect_gaps(g))
        out = g[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
        out["Volume"] = out["Volume"].fillna(0).astype("int64")
        out["Ticker"] = ticker
        out.to_csv(raw_path, index=False, date_format="%Y-%m-%d")
        ok += 1

    if suspects:
        shown = ", ".join(f"{t} {d} x{r:.2f}" for t, d, r in suspects[:12])
        log.warning(f"{len(suspects)} overnight move(s) look like unadjusted splits/bonuses that "
                    f"Yahoo has no record of (NOT adjusted -- check before trusting returns "
                    f"there): {shown}{' ...' if len(suspects) > 12 else ''}")
    log.info(f"BSE bhavcopy done: {ok} ticker(s) written, {len(no_data)} with no rows in range"
             + (f", {adjusted_events} split/bonus adjustment(s) applied" if adjust_splits else ""))
    return {"ok": ok, "no_data": no_data, "failed_days": [d.date() for d in failed_days]}
