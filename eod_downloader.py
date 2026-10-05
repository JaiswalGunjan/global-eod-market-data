#!/usr/bin/env python3
"""
Global Market Historical Data Downloader (16 exchanges)
============================================================

Downloads daily OHLCV data for tickers on any of 16 exchanges -- NASDAQ,
NYSE (US), BSE, NSE (India), LSE, EURONEXT, XETRA (Europe), TSE, HKEX,
SSE, SZSE (East Asia), TSX, ASX, KRX, SGX, SIX (Canada/Australia/Korea/
Singapore/Switzerland) -- over a chosen date range (default: last 10
years), then reorganizes the data into CSV output formatted for import
into Metastock.

--------------------------------------------------------------------------
PIPELINE
--------------------------------------------------------------------------
1. Fetch the current list of ticker symbols for the selected exchange(s).
   NASDAQ/NYSE come from Nasdaq Trader's public directory, NSE from its
   public equity list, and BSE from its latest daily bhavcopy file --
   these four (BULK_FETCHABLE_EXCHANGES) are the only exchanges with a
   bulk list this tool can auto-fetch. The other 12 global markets have
   no equivalent public bulk endpoint -- see --ticker-file, or download
   individual tickers directly instead.
2. Download historical daily OHLCV data per ticker via yfinance, in
   parallel (--workers threads, default 10), caching each ticker's raw
   data to disk so the run can be resumed if interrupted. Re-runs only
   fetch the missing recent days, not the whole range again. BSE tickers
   fall back to the NSE (.NS) listing when Yahoo's BSE data is sparse, or
   can be built from BSE's official daily files instead (--bse-source
   bhavcopy, see bse_bhavcopy.py).
3. Combine all cached per-ticker data and re-split it by date, writing
   one CSV per trading day into the output "daily" folder (or per-symbol /
   combined files, depending on --layout).

--------------------------------------------------------------------------
USAGE
--------------------------------------------------------------------------
    # Default: last 10 years, NASDAQ + NYSE
    python eod_downloader.py

    # Choose your own date range
    python eod_downloader.py --start 2016-01-01 --end 2026-01-01

    # Only NASDAQ, custom output folder
    python eod_downloader.py --exchanges NASDAQ --output ./data

    # Re-run later just to rebuild daily CSVs from what's already cached
    python eod_downloader.py --skip-download

    # Refresh the ticker list (new listings/delistings happen
    # constantly, so re-run this periodically instead of reusing tickers.csv)
    python eod_downloader.py --refresh-tickers

    # Club everything into ONE single CSV instead of per-day files
    python eod_downloader.py --layout combined

    # Get daily files, per-symbol files, AND the combined file, all at once
    python eod_downloader.py --layout all

--------------------------------------------------------------------------
OUTPUT
--------------------------------------------------------------------------
    <output>/raw/<TICKER>.csv          one file per ticker (download cache)
    <output>/daily/<YYYYMMDD>.csv      one file per trading day
    <output>/by_symbol/<TICKER>.csv    one file per ticker (--layout per-symbol/all)
    <output>/combined/all_data.csv     ALL tickers, ALL dates, one file (--layout combined/all)
    <output>/tickers.csv               the ticker list used for this run

Each daily CSV has columns:
    Symbol,Date,Open,High,Low,Close,Volume
sorted alphabetically by Symbol, Date formatted as YYYYMMDD by default.
Symbol is exchange-prefixed when the exchange is known: NASDAQ.AAPL for NASDAQ,
NYSE.AAPL for NYSE, NSE.TCS for NSE, etc. It falls back to the bare ticker
(e.g. just "AAPL") when the exchange isn't known -- this happens for
--ticker mode without --ticker-exchange (CLI), or a manually typed ticker
that was never looked up via search/refresh (GUI). The raw cache files
under raw/ are named with the same prefix (NSE.TCS.csv), but the Ticker
column inside them is always the bare symbol. What's actually sent to
Yahoo Finance is yf_download_symbol(): the bare ticker plus the exchange's
suffix for non-US markets (TCS.NS, VOD.L, ...).

Pass --metastock-ascii to instead write the specific multi-symbol layout
EOD data vendors use for direct MetaStock import: Symbol,Period,Date,Open,
High,Low,Close,Volume with NO header row, Period always "D" (daily), and
dates as MM/DD/YYYY by default. This is the format to use if you're
actually importing into MetaStock itself, rather than just wanting tidy
CSVs; the default (non --metastock-ascii) layout above is friendlier for
spreadsheets/pandas/other tools but isn't a recognized MetaStock format.

--------------------------------------------------------------------------
REQUIREMENTS
--------------------------------------------------------------------------
    pip install yfinance pandas requests

--------------------------------------------------------------------------
NOTES / LIMITATIONS
--------------------------------------------------------------------------
- Data source is Yahoo Finance via yfinance (free, unofficial, rate
  limited). For ~6,000+ tickers x 10 years this WILL take a long time
  (expect several hours) and Yahoo may throttle you. If you need this
  reliably/quickly at scale, a paid bulk EOD data provider (e.g. Polygon,
  Tiingo, EOD Historical Data, Norgate, Alpaca) will be far faster and
  more robust than scraping Yahoo ticker-by-ticker.
- Delisted/defunct tickers from 10 years ago will NOT be in today's
  exchange symbol directories, so this script (like most free sources)
  will have "survivorship bias" -- it only covers companies currently
  listed. True point-in-time coverage needs a paid data vendor.
- Metastock traditionally imports data ONE FILE PER SYMBOL, not one file
  per day. If your Metastock Downloader/Converter setup expects that
  instead, see the `--layout per-symbol` option, which writes
  <output>/by_symbol/<TICKER>.csv instead of (or alongside) daily files.
"""

import argparse
import io
import logging
import sys
import threading
import time
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

try:
    import yfinance as yf
except ImportError:
    yf = None


NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

# NSE India publishes a public bulk equity list. BSE's own scrip-list API
# (api.bseindia.com/.../ListofScripData) returns 403 to scripts, but BSE's
# daily bhavcopy CSV is a plain download that carries every security traded
# that day (~5,000 rows: scrip code, ticker, company name, ISIN) -- so the
# BSE directory is built from the most recent trading day's bhavcopy. See
# _fetch_bse_directory(). --ticker-file remains the manual fallback.
NSE_EQUITY_LIST_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
BSE_BHAVCOPY_URL = ("https://www.bseindia.com/download/BhavCopy/Equity/"
                    "BhavCopy_BSE_CM_0_0_0_{date}_F_0000.CSV")
BSE_BHAVCOPY_LOOKBACK_DAYS = 10  # weekends/holidays have no file; walk back to the last trading day

# Well-known market indices, kept separately from the bulk equity-list
# fetch above since indices are never part of that CSV (they're not
# individual listed companies). Injected into the ticker directory for
# whichever exchange(s) below every time it's built or read from cache --
# see _inject_extra_tickers() -- so they always show up in tickers.csv /
# nse_tickers.csv without a manual edit. Symbol is Yahoo Finance's own
# index ticker (leading "^"); yf_download_symbol() special-cases these so
# no exchange suffix gets appended (e.g. never "^NSEI.NS").
EXTRA_INDEX_TICKERS = {
    "NSE": [
        {"Symbol": "^NSEI", "Name": "NIFTY 50 INDEX"},
    ],
}


def _inject_extra_tickers(df: pd.DataFrame, exchanges) -> pd.DataFrame:
    """Adds the known index tickers (EXTRA_INDEX_TICKERS) for any exchange
    in `exchanges` that has one, if not already present in `df`. Called on
    BOTH the cached-read path and the freshly-fetched path in
    fetch_ticker_directory(), so an index added here shows up immediately
    -- including for anyone re-running against an older cache file that
    predates this -- without needing --refresh-tickers."""
    rows = []
    for ex in exchanges:
        for extra in EXTRA_INDEX_TICKERS.get((ex or "").upper(), []):
            already = ((df["Symbol"] == extra["Symbol"]) & (df["Exchange"] == ex.upper())).any()
            if not already:
                rows.append({"Symbol": extra["Symbol"], "Name": extra["Name"], "Exchange": ex.upper()})
    if not rows:
        return df
    df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
    return df.sort_values(["Exchange", "Symbol"]).reset_index(drop=True)

# NSE (and some other Indian sites) reject requests with no User-Agent as a
# basic anti-scraping measure; Nasdaq Trader doesn't need this but it
# doesn't hurt there either.
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

SUPPORTED_EXCHANGES = [
    "NASDAQ", "NYSE",                    # US
    "BSE", "NSE",                        # India
    "LSE", "EURONEXT", "XETRA",          # Europe (London, Paris, Frankfurt)
    "TSE", "HKEX", "SSE", "SZSE",        # East Asia (Tokyo, Hong Kong, Shanghai, Shenzhen)
    "TSX", "ASX", "KRX", "SGX", "SIX",   # Canada, Australia, Korea, Singapore, Switzerland
]

# Exchanges with a plain-CSV/text bulk symbol list this tool can actually
# fetch automatically. Everything else in SUPPORTED_EXCHANGES has no such
# endpoint (most exchange websites require a form/session/login to export
# their full listing) -- use --ticker-file for those, or download
# individual tickers directly (no bulk list needed for that at all).
BULK_FETCHABLE_EXCHANGES = {"NASDAQ", "NYSE", "NSE", "BSE"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("market_dl")

# yfinance logs its own "possibly delisted / no data found" messages for
# every ticker with no history (common for warrants/units/rights, e.g.
# tickers ending in .U, W, R). That's expected and NOT a real failure --
# our code already handles it by writing an empty placeholder and moving
# on. Silence yfinance's own logger so it doesn't clutter the output; our
# code still reports real failures via `log` above.
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

_active_file_handler = None  # tracks the current run's log file handler


def start_file_logging(out_root: Path) -> Path:
    """Attach a file handler so this run's full log is saved to
    <out_root>/logs/run_<timestamp>.log, in addition to the console/GUI.
    Handy for reviewing what happened after a large overnight run. If a
    previous run in this same process already had one attached (the GUI
    can run multiple times without restarting), that one is detached first
    so log messages don't keep fanning out to every past run's file too."""
    global _active_file_handler
    if _active_file_handler is not None:
        log.removeHandler(_active_file_handler)
        _active_file_handler.close()
        _active_file_handler = None

    log_dir = out_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S"))
    log.addHandler(handler)
    _active_file_handler = handler
    log.info(f"Logging this run to {log_path}")
    return log_path


# --------------------------------------------------------------------------
# 1. Ticker list
# --------------------------------------------------------------------------

def _http_get_with_retry(url, headers=None, retries=3, timeout=30):
    """GET with a couple of retries and exponential backoff -- exchange
    symbol-directory endpoints (especially NSE, and occasionally Nasdaq
    Trader) intermittently 403/timeout on a single attempt but succeed on
    a retry a few seconds later."""
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            if attempt < retries:
                wait = 2 ** attempt
                log.warning(f"Request to {url} failed (attempt {attempt}/{retries}): {e} "
                            f"-- retrying in {wait}s")
                time.sleep(wait)
    raise last_exc


def _fetch_bse_directory() -> pd.DataFrame:
    """BSE-listed equities from the most recent daily bhavcopy.

    Symbol is the BSE ticker (e.g. TCS -> Yahoo "TCS.BO"). Yahoo does NOT
    resolve numeric scrip codes ("500325.BO" returns nothing, verified), so
    the code can't be the symbol. Name carries the scrip code, so the GUI's
    name search still finds a company by "500325" or its company name.
    Note Yahoo's own BSE coverage is partial: some tickers return a full
    history, others little or nothing -- those show up as no_data/failed in
    the run summary rather than as errors here."""
    last_exc = None
    for back in range(BSE_BHAVCOPY_LOOKBACK_DAYS + 1):
        day = datetime.now() - timedelta(days=back)
        if day.weekday() >= 5:  # Sat/Sun: BSE never publishes a file
            continue
        url = BSE_BHAVCOPY_URL.format(date=day.strftime("%Y%m%d"))
        try:
            resp = requests.get(url, headers=HTTP_HEADERS, timeout=30)
            if resp.status_code != 200:
                last_exc = RuntimeError(f"HTTP {resp.status_code} for {url}")
                continue
            df = pd.read_csv(io.StringIO(resp.text), dtype=str)
        except Exception as e:
            last_exc = e
            continue
        df.columns = [c.strip() for c in df.columns]
        if "FinInstrmId" not in df.columns:
            last_exc = RuntimeError(f"unexpected bhavcopy format at {url}: {list(df.columns)}")
            continue
        if "FinInstrmTp" in df.columns:
            df = df[df["FinInstrmTp"].str.strip() == "STK"]
        code = df["FinInstrmId"].astype(str).str.strip()
        ticker = df["TckrSymb"].astype(str).str.strip()
        company = df["FinInstrmNm"].astype(str).str.strip()
        out = pd.DataFrame({"Symbol": ticker, "Name": company + " (" + code + ")", "Exchange": "BSE"})
        out = out[out["Symbol"] != ""]
        log.info(f"BSE bhavcopy {day:%Y-%m-%d}: {len(out)} securities")
        return out
    raise RuntimeError(f"no BSE bhavcopy found in the last {BSE_BHAVCOPY_LOOKBACK_DAYS} days"
                       + (f" (last error: {last_exc})" if last_exc else ""))


def fetch_ticker_directory(exchanges, include_etfs=True, cache_path: Path = None,
                            refresh: bool = False) -> pd.DataFrame:
    """Fetch NASDAQ / NYSE ticker symbols, company names, AND exchange from
    Nasdaq Trader's directory. Returns a DataFrame with columns
    ['Symbol', 'Name', 'Exchange'], e.g. ('AAPL', 'Apple Inc. - Common
    Stock', 'NASDAQ') -- Name enables company-name search, Exchange enables
    prefixing output symbols as NASDAQ.AAPL / NYSE.AAPL.

    exchanges: iterable containing any of {"NASDAQ", "NYSE"}
    refresh: if True, ignore any cached copy and re-fetch fresh from Nasdaq
        Trader (use this periodically -- new IPOs/listings/delistings
        happen constantly, so an old cached copy slowly goes stale).
    """
    if cache_path and cache_path.exists() and not refresh:
        # dtype forced to str: a purely-numeric ticker (common on Tokyo
        # Stock Exchange, e.g. "7203") would otherwise get silently
        # auto-inferred as an integer on read-back, which breaks every
        # exchange_map/prefix lookup keyed on the string form downstream.
        df = pd.read_csv(cache_path, dtype={"Symbol": str})
        if "Name" not in df.columns:  # older cache from before name search existed
            df["Name"] = ""
        if "Exchange" not in df.columns:  # older cache from before exchange prefixing existed
            df["Exchange"] = ""
        cached_exchanges = set(df["Exchange"].dropna().unique())
        missing = set(exchanges) - cached_exchanges
        if not missing:
            log.info(f"Using cached ticker list: {cache_path} (use --refresh-tickers to update it)")
            df = _inject_extra_tickers(df, exchanges)
            if cache_path:
                df.to_csv(cache_path, index=False)  # persist injected index rows into the cache itself
            return df[["Symbol", "Name", "Exchange"]].dropna(subset=["Symbol"])
        else:
            # The cache exists but was built for a narrower/different set of
            # exchanges (e.g. an earlier run used --exchanges NASDAQ only,
            # and this run wants NASDAQ+NYSE) -- trusting it here would
            # silently drop whichever exchange it doesn't cover. Re-fetch
            # instead of returning an incomplete list.
            log.warning(f"Cached ticker list at {cache_path} doesn't cover {sorted(missing)} "
                        f"(it covers {sorted(cached_exchanges) or ['nothing recognized']}) -- "
                        f"re-fetching fresh instead of using that stale/partial cache.")

    if refresh:
        log.info("Refreshing ticker list from Nasdaq Trader (ignoring any cache)...")

    parts = []

    if "NASDAQ" in exchanges:
        log.info("Fetching NASDAQ-listed symbols...")
        resp = _http_get_with_retry(NASDAQ_LISTED_URL)
        df = pd.read_csv(io.StringIO(resp.text), sep="|")
        df = df[df["Test Issue"] == "N"]
        if not include_etfs and "ETF" in df.columns:
            df = df[df["ETF"] == "N"]
        sub = df[["Symbol", "Security Name"]].dropna(subset=["Symbol"]).copy()
        sub.columns = ["Symbol", "Name"]
        sub["Exchange"] = "NASDAQ"
        parts.append(sub)

    if "NYSE" in exchanges:
        log.info("Fetching NYSE-listed symbols (via 'otherlisted' directory)...")
        resp = _http_get_with_retry(OTHER_LISTED_URL)
        df = pd.read_csv(io.StringIO(resp.text), sep="|")
        df = df[df["Test Issue"] == "N"]
        # Exchange codes in this file: N=NYSE, A=NYSE American, P=NYSE Arca,
        # Z=BATS, V=IEX. We keep only "N" for pure NYSE.
        if "Exchange" in df.columns:
            df = df[df["Exchange"] == "N"]
        if not include_etfs and "ETF" in df.columns:
            df = df[df["ETF"] == "N"]
        symbol_col = "ACT Symbol" if "ACT Symbol" in df.columns else "Symbol"
        sub = df[[symbol_col, "Security Name"]].dropna(subset=[symbol_col]).copy()
        sub.columns = ["Symbol", "Name"]
        sub["Exchange"] = "NYSE"
        parts.append(sub)

    if "NSE" in exchanges:
        log.info("Fetching NSE (India)-listed symbols...")
        try:
            resp = _http_get_with_retry(NSE_EQUITY_LIST_URL, headers=HTTP_HEADERS)
            df = pd.read_csv(io.StringIO(resp.text))
            df.columns = [c.strip() for c in df.columns]  # NSE's CSV has stray whitespace in headers
            name_col = "NAME OF COMPANY" if "NAME OF COMPANY" in df.columns else df.columns[1]
            sub = df[["SYMBOL", name_col]].dropna(subset=["SYMBOL"]).copy()
            sub.columns = ["Symbol", "Name"]
            sub["Exchange"] = "NSE"
            parts.append(sub)
        except Exception as e:
            log.error(f"Couldn't fetch the NSE symbol list ({e}). NSE's site occasionally "
                      f"blocks automated requests or changes its URL. You can still download "
                      f"individual NSE tickers directly without the full directory: "
                      f"--ticker RELIANCE --ticker-exchange NSE, or load your own list via "
                      f"--ticker-file.")

    if "BSE" in exchanges:
        log.info("Fetching BSE (India)-listed symbols (from the latest daily bhavcopy)...")
        try:
            parts.append(_fetch_bse_directory())
        except Exception as e:
            log.error(f"Couldn't fetch the BSE symbol list ({e}). You can still download "
                      f"individual BSE tickers directly without the full directory: "
                      f"--ticker 500325 --ticker-exchange BSE, or load your own list via "
                      f"--ticker-file.")

    for ex in exchanges:
        if ex in BULK_FETCHABLE_EXCHANGES:
            continue  # handled above
        log.warning(f"{ex} has no public bulk symbol-list endpoint this tool can fetch "
                    f"automatically (most exchange sites require a form/session to export "
                    f"their full listing). Options: (1) download individual {ex} tickers "
                    f"directly with --ticker SYMBOL --ticker-exchange {ex} (no directory "
                    f"needed), or (2) export {ex}'s list yourself (from the exchange's own "
                    f"site, or a data vendor) and load it with --ticker-file.")

    if parts:
        combined = pd.concat(parts, ignore_index=True)
    else:
        combined = pd.DataFrame(columns=["Symbol", "Name", "Exchange"])

    combined["Symbol"] = combined["Symbol"].astype(str).str.strip().str.upper()
    combined["Name"] = combined["Name"].astype(str).str.strip()
    # Drop obviously bad rows (footer lines, blanks, symbols with weird chars)
    combined = combined[combined["Symbol"] != ""]
    combined = combined[~combined["Symbol"].str.contains("File Creation", na=False)]
    # Dedup on (Symbol, Exchange), NOT Symbol alone -- now that multiple
    # countries are in play, the same short ticker text can legitimately
    # exist on more than one exchange (e.g. a 3-letter NASDAQ ticker
    # coinciding with an NSE one); only an exact duplicate row should merge.
    combined = combined.drop_duplicates(subset=["Symbol", "Exchange"])
    combined = combined.sort_values(["Exchange", "Symbol"]).reset_index(drop=True)

    combined = _inject_extra_tickers(combined, exchanges)

    log.info(f"Total tickers found: {len(combined)}")

    if cache_path:
        combined.to_csv(cache_path, index=False)
        log.info(f"Saved ticker list to {cache_path}")

    return combined


def fetch_ticker_list(exchanges, include_etfs=True, cache_path: Path = None,
                       refresh: bool = False) -> list:
    """Backward-compatible helper: same as fetch_ticker_directory() but
    returns just the plain list of ticker symbols (used by the CLI, and
    anywhere company names aren't needed)."""
    df = fetch_ticker_directory(exchanges, include_etfs=include_etfs,
                                 cache_path=cache_path, refresh=refresh)
    return df["Symbol"].dropna().tolist()


def load_tickers_from_file(path, exchange: str) -> pd.DataFrame:
    """Load a ticker directory from a local CSV instead of fetching it from
    the web -- the fallback for exchanges with no scrapeable bulk endpoint
    (BSE), or for using your own curated/exported list for any exchange.

    Accepts flexible column names: looks for a symbol column named "Symbol"
    or "SYMBOL", and a name column named "Name", "NAME OF COMPANY", or
    "Security Name" if present (optional -- an empty Name is fine, you'll
    just lose company-name search for these tickers). Every row is tagged
    with the given `exchange`.
    """
    # dtype=str throughout: a purely-numeric symbol (common on some Asian
    # exchanges) would otherwise get silently auto-inferred as an integer,
    # which breaks every exchange_map/prefix lookup keyed on the string
    # form downstream. Every column actually used here is text anyway.
    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    symbol_col = next((c for c in df.columns if c.upper() == "SYMBOL"), None)
    if symbol_col is None:
        raise ValueError(f"{path}: couldn't find a Symbol column (looked for 'Symbol'/'SYMBOL'). "
                          f"Columns found: {list(df.columns)}")

    name_col = next(
        (c for c in df.columns if c.upper() in ("NAME", "NAME OF COMPANY", "SECURITY NAME")),
        None,
    )

    out = pd.DataFrame()
    out["Symbol"] = df[symbol_col].astype(str).str.strip().str.upper()
    out["Name"] = df[name_col].astype(str).str.strip() if name_col else ""
    out["Exchange"] = exchange.upper()
    out = out[out["Symbol"] != ""].drop_duplicates(subset="Symbol").reset_index(drop=True)

    log.info(f"Loaded {len(out)} {exchange.upper()} tickers from {path}")
    return out


# Exchange prefix used in output Symbol columns -- deliberately just each
# exchange's own standard name (the same one used everywhere else in this
# tool: --exchanges values, GUI checkboxes, --split-by-exchange folder
# names) rather than an invented abbreviation, e.g. NASDAQ.AAPL, NYSE.GE,
# BSE.RELIANCE, NSE.TCS, LSE.VOD, TSE.7203, HKEX.0700, SSE.600519, etc.
# NOTE: this is purely a display/output convention for the CSVs -- the raw
# ticker (e.g. "AAPL", "RELIANCE") is what's used for the raw/ download
# cache filename; only build_*() output uses this prefix.
EXCHANGE_PREFIX = {ex: ex for ex in SUPPORTED_EXCHANGES}

# The suffix Yahoo Finance requires ON THE ACTUAL DOWNLOAD REQUEST for
# non-US exchanges -- unlike EXCHANGE_PREFIX above (output-only), this is
# NOT optional: Yahoo can't find "RELIANCE" or "VOD" at all without it, it
# needs "RELIANCE.NS" / "VOD.L". NASDAQ/NYSE need no suffix (absent here).
# None of these collide with EXCHANGE_PREFIX above (e.g. "NSE." as a
# prefix vs Yahoo's own ".NS" suffix look and mean different things, so
# there's no ambiguity between our output and what gets sent to Yahoo).
YF_TICKER_SUFFIX = {
    "NSE": ".NS", "BSE": ".BO",
    "LSE": ".L", "EURONEXT": ".PA", "XETRA": ".DE",
    "TSE": ".T", "HKEX": ".HK", "SSE": ".SS", "SZSE": ".SZ",
    "TSX": ".TO", "ASX": ".AX", "KRX": ".KS", "SGX": ".SI", "SIX": ".SW",
}


def yf_download_symbol(ticker: str, exchange: str) -> str:
    """The actual string to send to Yahoo Finance -- appends the exchange's
    required suffix (NSE/BSE) if not already present; unchanged for
    exchanges that don't need one (NASDAQ/NYSE).

    Index tickers (Yahoo's own "^" prefix, e.g. "^NSEI" for Nifty 50) are
    never suffixed -- Yahoo's index symbols are global and don't take an
    exchange-specific suffix the way equities do. Sending "^NSEI.NS" would
    fail outright."""
    if ticker.startswith("^"):
        return ticker
    suffix = YF_TICKER_SUFFIX.get((exchange or "").upper())
    if suffix and not ticker.upper().endswith(suffix.upper()):
        return f"{ticker}{suffix}"
    return ticker


def normalize_ticker_input(ticker: str) -> str:
    """If someone types a ticker with Yahoo's own suffix already on it (e.g.
    'RELIANCE.NS'), strip it back off so the bare ticker stays the one
    canonical form used everywhere else in this tool (raw cache filenames,
    exchange_map keys, etc.) -- yf_download_symbol() re-adds it only for the
    actual API call."""
    t = ticker.strip().upper()
    for suffix in YF_TICKER_SUFFIX.values():
        if t.endswith(suffix.upper()):
            return t[: -len(suffix)]
    return t


def prefixed_symbol(ticker: str, exchange: str) -> str:
    """'AAPL' + 'NASDAQ' -> 'NASDAQ.AAPL'. Falls back to the bare ticker if the
    exchange is unknown/unrecognized."""
    prefix = EXCHANGE_PREFIX.get((exchange or "").upper())
    return f"{prefix}.{ticker}" if prefix else ticker


def _prefixed_symbols_series(tickers: pd.Series, exchange_map: dict) -> pd.Series:
    """Vectorized version of prefixed_symbol() for a whole column at once."""
    if not exchange_map:
        return tickers
    exchange = tickers.map(exchange_map)
    prefix = exchange.map(EXCHANGE_PREFIX)
    prefixed = prefix.str.cat(tickers, sep=".")
    return prefixed.fillna(tickers)



# --------------------------------------------------------------------------
# 2. Download per-ticker historical data (with local caching / resume)
# --------------------------------------------------------------------------

def load_cached_range(raw_path: Path):
    """Return (min_date, max_date) already cached for a ticker, or (None, None)."""
    if not raw_path.exists():
        return None, None
    try:
        df = pd.read_csv(raw_path, usecols=["Date"], parse_dates=["Date"])
        if df.empty:
            return None, None
        return df["Date"].min(), df["Date"].max()
    except Exception:
        return None, None


def _yf_fetch(yf_symbol: str, start: str, end: str):
    """One raw yfinance download (unadjusted, no threads)."""
    return yf.download(yf_symbol, start=start, end=end, progress=False,
                       auto_adjust=False, threads=False)


def _looks_sparse(data, start: str, end: str) -> bool:
    """True if `data` has far fewer rows than the date range implies (or none
    at all) -- i.e. Yahoo's feed for this symbol looks broken/partial.
    Expected rows = weekdays in the range; below 60% of that (generous for
    holidays) counts as sparse. Ranges under 3 weekdays can't be judged, so
    only an empty result counts."""
    n = 0 if data is None else len(data)
    if n == 0:
        return True
    expected = len(pd.bdate_range(start, pd.Timestamp(end) - pd.Timedelta(days=1)))
    return expected >= 3 and n < 0.6 * expected


def download_ticker(ticker: str, start: str, end: str, raw_dir: Path,
                     retries: int = 3, pause: float = 1.0, exchange: str = None) -> str:
    """Download one ticker's OHLCV data and cache it to raw_dir/<SYMBOL>.csv,
    where <SYMBOL> is the exchange-prefixed display symbol (NASDAQ.AAPL /
    NYSE.GE / BSE.RELIANCE / NSE.TCS) when `exchange` is known, or just the
    bare ticker otherwise.
    IMPORTANT: the prefix is filename-only. The actual request sent to
    Yahoo Finance uses yf_download_symbol(ticker, exchange) instead of the
    bare `ticker` -- for NASDAQ/NYSE that's identical to the bare ticker,
    but NSE/BSE need Yahoo's own ".NS"/".BO" suffix or the request fails.

    Skips tickers whose cache already fully covers [start, end]. If the
    cache covers `start` but not `end` -- the common case of re-running
    periodically to catch up to today -- only the missing tail is fetched
    and merged into the existing file, instead of re-downloading the whole
    range from scratch.

    Returns one of: "cached", "ok", "no_data", "failed".
    "no_data" means Yahoo has no history for this symbol at all (common
    for warrants/units/rights) -- this is expected and not a real error.
    """
    raw_symbol = prefixed_symbol(ticker, exchange)
    raw_path = raw_dir / f"{raw_symbol}.csv"
    yf_symbol = yf_download_symbol(ticker, exchange)
    start_dt = pd.Timestamp(start)
    end_dt = pd.Timestamp(end)

    cached_min, cached_max = load_cached_range(raw_path)
    if cached_min is not None and cached_min <= start_dt and cached_max >= end_dt - pd.Timedelta(days=1):
        return "cached"  # already have this range cached

    # Incremental case: cache already covers the start, just needs a newer
    # tail. Only fetch the delta and merge it into the existing file.
    fetch_start, fetch_end = start, end
    existing_df = None
    if cached_min is not None and cached_min <= start_dt and cached_max is not None:
        delta_start = cached_max + pd.Timedelta(days=1)
        if delta_start <= end_dt:
            try:
                existing_df = pd.read_csv(raw_path, parse_dates=["Date"], dtype={"Ticker": str})
                fetch_start = delta_start.strftime("%Y-%m-%d")
            except Exception:
                existing_df = None  # corrupt cache -- fall back to a full re-fetch

    for attempt in range(1, retries + 1):
        try:
            data = _yf_fetch(yf_symbol, fetch_start, fetch_end)
            if (exchange or "").upper() == "BSE" and _looks_sparse(data, fetch_start, fetch_end):
                # Yahoo's BSE (.BO) history is patchy: many tickers return
                # nothing, or a single row, while the same stock on NSE has a
                # full history. BSE and NSE prices for a dual-listed stock
                # are nearly identical, so fall back to the .NS listing --
                # but only if it actually has more data than the BSE one.
                nse_data = _yf_fetch(yf_download_symbol(ticker, "NSE"), fetch_start, fetch_end)
                n_bse = 0 if data is None else len(data)
                if nse_data is not None and len(nse_data) > n_bse:
                    log.info(f"[{ticker}] Yahoo has {n_bse} BSE row(s); using NSE ({ticker}.NS) "
                             f"data instead ({len(nse_data)} rows)")
                    data = nse_data
            if data is None or data.empty:
                if existing_df is not None:
                    # Incremental fetch found nothing new (e.g. no trading
                    # days in the delta window) -- what's already cached is
                    # still valid, nothing to do.
                    return "cached"
                # No data exists for this symbol (e.g. warrant/unit/rights
                # with no trading history). Write an empty marker so we
                # don't keep re-trying it on future runs, and don't retry
                # now -- retrying won't produce data that doesn't exist.
                pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close", "Volume"]).to_csv(
                    raw_path, index=False
                )
                return "no_data"

            data = data.reset_index()
            # yfinance sometimes returns MultiIndex columns for single tickers
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = [c[0] for c in data.columns]

            data = data[["Date", "Open", "High", "Low", "Close", "Volume"]]
            data["Ticker"] = ticker  # always the bare symbol, regardless of filename/yf_symbol

            if existing_df is not None:
                data = pd.concat([existing_df, data], ignore_index=True)
                data = data.drop_duplicates(subset="Date").sort_values("Date")

            data.to_csv(raw_path, index=False)
            return "ok"

        except Exception as e:
            log.warning(f"[{ticker}] attempt {attempt}/{retries} failed: {e}")
            time.sleep(pause * attempt)

    log.error(f"[{ticker}] giving up after {retries} attempts")
    return "failed"


def download_all(tickers, start, end, raw_dir: Path, pause: float = 0.3,
                  progress_callback=None, exchange_map=None, max_workers: int = 10,
                  cancel_event: threading.Event = None, bse_source: str = "yahoo",
                  bse_adjust_splits: bool = True):
    """Downloads every ticker in `tickers`, in parallel (max_workers threads
    at a time -- Yahoo Finance calls are I/O-bound, so threads work fine
    here without needing multiprocessing). All downloads finish before this
    function returns; callers should build output CSVs only after this
    returns, not per-ticker, to avoid rebuilding output repeatedly.

    progress_callback, if given, is called after every completed ticker as
    progress_callback(done, total, failed_count, no_data_count) -- handy for
    driving a GUI progress bar.

    exchange_map, if given, is used to name each raw cache file with its
    exchange prefix (NASDAQ.AAPL.csv / NYSE.GE.csv) -- purely a filename/display
    convention, doesn't affect what's actually requested from Yahoo.

    cancel_event, if given, is checked between tickers; once set, no new
    downloads are started (in-flight ones still finish) and this returns
    early. Whatever's already downloaded stays cached either way.

    bse_source: "yahoo" (default; with an NSE fallback, see download_ticker)
    or "bhavcopy" -- build BSE tickers' history from BSE's official daily
    files instead (see bse_bhavcopy.py); every other exchange still uses
    Yahoo. For the bhavcopy phase progress_callback's `total` counts DAYS
    (one per day-file), not tickers.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    exchange_map = exchange_map or {}

    if bse_source == "bhavcopy":
        bse_tickers = [t for t in tickers if exchange_map.get(t) == "BSE"]
        if bse_tickers:
            import bse_bhavcopy
            log.info(f"BSE: using official bhavcopy files for {len(bse_tickers)} ticker(s)")
            res = bse_bhavcopy.download_bse(
                bse_tickers, start, end, raw_dir, max_workers=max(1, min(max_workers, 8)),
                progress_callback=progress_callback, cancel_event=cancel_event,
                adjust_splits=bse_adjust_splits)
            if res["no_data"]:
                (raw_dir.parent / "no_data_tickers.txt").write_text("\n".join(res["no_data"]))
            skip = set(bse_tickers)
            tickers = [t for t in tickers if t not in skip]
            if not tickers or (cancel_event is not None and cancel_event.is_set()):
                return []
    failed = []
    no_data = []
    total = len(tickers)
    done = 0
    start_time = time.time()
    lock = threading.Lock()

    def _work(ticker):
        exchange = exchange_map.get(ticker)
        return ticker, download_ticker(ticker, start, end, raw_dir, pause=pause, exchange=exchange)

    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        # Submit everything upfront -- ThreadPoolExecutor queues tasks beyond
        # max_workers rather than blocking, so this returns immediately
        # regardless of ticker count. Cancellation below relies on that: it
        # cancels whichever futures haven't STARTED yet (still queued), which
        # actually stops work, unlike checking a flag in this loop (which
        # would finish submitting almost instantly, before cancel could matter).
        futures = {executor.submit(_work, ticker): ticker for ticker in tickers}
        cancelled_count = 0
        cancel_attempted = False

        for future in as_completed(futures):
            if cancel_event is not None and cancel_event.is_set() and not cancel_attempted:
                cancel_attempted = True
                # First time we notice cancellation: stop every not-yet-started
                # task. Already-running ones can't be interrupted mid-request,
                # they'll finish naturally and still get counted below.
                for f in futures:
                    if f.cancel():
                        cancelled_count += 1
                if cancelled_count:
                    log.warning(f"Cancelling {cancelled_count} not-yet-started ticker(s); "
                                f"letting in-flight ones finish.")

            ticker = futures[future]
            try:
                _, status = future.result()
            except CancelledError:
                continue  # never started -- don't count it as done/failed/no_data
            except Exception as e:
                status = "failed"
                log.error(f"[{ticker}] unexpected error: {e}")

            with lock:
                done += 1
                if status == "failed":
                    failed.append(ticker)
                elif status == "no_data":
                    no_data.append(ticker)
                snapshot = (done, len(failed), len(no_data))

            if progress_callback:
                progress_callback(snapshot[0], total, snapshot[1], snapshot[2])

            if snapshot[0] % 50 == 0 or snapshot[0] == total:
                elapsed = time.time() - start_time
                rate = elapsed / snapshot[0]
                eta_min = rate * (total - snapshot[0]) / 60
                log.info(
                    f"Progress {snapshot[0]}/{total} | failed={snapshot[1]} | "
                    f"no_data={snapshot[2]} | ETA ~{eta_min:.0f} min"
                )

    if cancel_event is not None and cancel_event.is_set():
        log.warning(f"Download cancelled -- {done}/{total} ticker(s) finished before stopping "
                    f"({cancelled_count} more were queued but never started). Whatever finished "
                    f"is cached; re-run the same settings later to resume.")

    if failed:
        fail_path = raw_dir.parent / "failed_tickers.txt"
        fail_path.write_text("\n".join(failed))
        log.warning(f"{len(failed)} tickers genuinely failed (network/API errors). See {fail_path}")

    if no_data:
        no_data_path = raw_dir.parent / "no_data_tickers.txt"
        no_data_path.write_text("\n".join(no_data))
        log.info(f"{len(no_data)} tickers had no data available on Yahoo (expected for many warrants/units/rights). See {no_data_path}")

    return failed


# --------------------------------------------------------------------------
# 3. Reorganize into daily CSVs (or per-symbol / combined CSVs)
# --------------------------------------------------------------------------

# The recognized multi-symbol "MetaStock ASCII (8 column)" convention used
# by EOD data vendors for direct MetaStock import: Symbol, Period ("D" for
# daily), Date, Open, High, Low, Close, Volume -- comma-separated, NO header
# row. (There's also a 7-column variant that drops Open entirely, but 8 is
# what you want since MetaStock needs Open for its own charts.)
METASTOCK_ASCII_COLUMNS = ["Symbol", "Period", "Date", "Open", "High", "Low", "Close", "Volume"]


def _finalize_output_columns(df: pd.DataFrame, metastock_ascii: bool) -> pd.DataFrame:
    """Apply strict MetaStock ASCII column layout (adds a Period='D' column,
    reorders to Symbol,Period,Date,O,H,L,C,V) when requested; otherwise keep
    the friendlier Symbol,Date,O,H,L,C,V layout used elsewhere in this tool."""
    if metastock_ascii:
        df = df.copy()
        df["Period"] = "D"
        return df[METASTOCK_ASCII_COLUMNS]
    return df[["Symbol", "Date", "Open", "High", "Low", "Close", "Volume"]]


def _bare_ticker_from_filename(stem: str) -> str:
    """Reverse of prefixed_symbol() for filenames, e.g. 'NASDAQ.AAPL' -> 'AAPL'.
    Only used as a defensive fallback when a raw cache file is somehow
    missing its 'Ticker' column -- normally we read the ticker from the
    CSV's own Ticker column instead, since that's always the bare symbol
    regardless of what the (possibly prefixed) filename looks like."""
    for prefix in EXCHANGE_PREFIX.values():
        lead = f"{prefix}."
        if stem.startswith(lead):
            return stem[len(lead):]
    return stem


def _select_raw_files(raw_dir: Path, tickers=None, exchange_map=None):
    """Pick which cached raw/<SYMBOL>.csv files to include when building
    output CSVs (filenames are exchange-prefixed when known, e.g.
    NASDAQ.AAPL.csv). If `tickers` is given, only include those specific
    symbols (skipping any that were never downloaded) -- this is what
    makes "pick one ticker" actually produce output with just that ticker,
    instead of clubbing in everything else ever cached in raw_dir. If
    `tickers` is None, every cached file is included (original behavior)."""
    if tickers:
        exchange_map = exchange_map or {}
        wanted = {t.upper() for t in tickers}
        files = []
        missing = []
        for t in wanted:
            p = raw_dir / f"{prefixed_symbol(t, exchange_map.get(t))}.csv"
            if p.exists():
                files.append(p)
            else:
                missing.append(t)
        if missing:
            log.warning(f"{len(missing)} requested ticker(s) have no cached data yet "
                        f"(not downloaded, or download failed): {sorted(missing)[:10]}"
                        f"{' ...' if len(missing) > 10 else ''}")
        return sorted(files)
    return sorted(raw_dir.glob("*.csv"))


def report_data_quality(raw_dir: Path, tickers, exchange_map=None):
    """Quick post-download sanity check: flag tickers whose cached row
    count is far below the rest of the batch's typical count -- often a
    sign of a partial network failure that still technically "succeeded"
    (e.g. Yahoo cut the response short) rather than cleanly failing.
    Purely informational -- logs a warning, changes nothing."""
    exchange_map = exchange_map or {}
    counts = {}
    for t in tickers:
        p = raw_dir / f"{prefixed_symbol(t, exchange_map.get(t))}.csv"
        if not p.exists():
            continue
        try:
            with open(p, encoding="utf-8") as f:
                n = sum(1 for _ in f) - 1  # minus header row
        except Exception:
            continue
        if n > 0:
            counts[t] = n

    if len(counts) < 5:
        return  # too few tickers to judge what's "typical" for this batch

    values = sorted(counts.values())
    median = values[len(values) // 2]
    threshold = median * 0.5
    suspicious = sorted(t for t, n in counts.items() if n < threshold)
    if suspicious:
        log.warning(
            f"{len(suspicious)} ticker(s) have far fewer rows than typical for this batch "
            f"(median {median}, threshold {threshold:.0f}) -- possibly partial data from a "
            f"network hiccup rather than a clean download. Worth a spot check, or just "
            f"re-run (already-good tickers are skipped, only these would re-fetch): "
            f"{suspicious[:15]}{' ...' if len(suspicious) > 15 else ''}"
        )




def build_daily_csvs(raw_dir: Path, daily_dir: Path, start: str, end: str,
                      date_format: str = None, tickers=None, exchange_map=None,
                      metastock_ascii: bool = False):
    date_format = date_format or ("%m/%d/%Y" if metastock_ascii else "%Y%m%d")
    daily_dir.mkdir(parents=True, exist_ok=True)
    start_dt = pd.Timestamp(start)
    end_dt = pd.Timestamp(end)

    raw_files = _select_raw_files(raw_dir, tickers, exchange_map)
    log.info(f"Combining {len(raw_files)} cached ticker files...")

    frames = []
    for f in raw_files:
        try:
            df = pd.read_csv(f, parse_dates=["Date"], dtype={"Ticker": str})
        except Exception as e:
            log.warning(f"Skipping unreadable file {f.name}: {e}")
            continue
        if df.empty:
            continue
        if "Ticker" not in df.columns:
            df["Ticker"] = _bare_ticker_from_filename(f.stem)
        df = df[(df["Date"] >= start_dt) & (df["Date"] <= end_dt)]
        if not df.empty:
            frames.append(df)

    if not frames:
        log.warning("No data found to build daily CSVs from.")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["Open", "High", "Low", "Close"])

    n_days = combined["Date"].nunique()
    log.info(f"Writing {n_days} daily CSV files to {daily_dir} ...")

    for date, group in combined.groupby("Date"):
        out = group[["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]].copy()
        out["Date"] = out["Date"].dt.strftime(date_format)
        out["Symbol"] = out["Ticker"] if metastock_ascii else _prefixed_symbols_series(out["Ticker"], exchange_map)
        out = out.sort_values("Symbol")
        out = _finalize_output_columns(out, metastock_ascii)
        fname = daily_dir / f"{date.strftime('%Y%m%d')}.csv"
        out.to_csv(fname, index=False, header=not metastock_ascii)

    log.info("Done building daily CSV files.")


def build_combined_csv(raw_dir: Path, out_path: Path, start: str, end: str,
                        date_format: str = None, tickers=None, exchange_map=None,
                        metastock_ascii: bool = False):
    """Club every ticker's data across the whole date range into ONE CSV.

    Columns: Symbol,Date,Open,High,Low,Close,Volume (or, with
    metastock_ascii=True: Symbol,Period,Date,Open,High,Low,Close,Volume,
    no header -- the recognized multi-symbol MetaStock ASCII layout).
    Sorted by Date then Symbol.

    NOTE: for the full NYSE+NASDAQ universe over 10 years, this single
    file can be very large (tens of millions of rows, likely 1GB+). If
    that's too unwieldy for your downstream tool, prefer --layout daily
    or --layout per-symbol instead.
    """
    date_format = date_format or ("%m/%d/%Y" if metastock_ascii else "%Y%m%d")
    start_dt = pd.Timestamp(start)
    end_dt = pd.Timestamp(end)

    raw_files = _select_raw_files(raw_dir, tickers, exchange_map)
    log.info(f"Building combined single CSV from {len(raw_files)} cached ticker files...")

    frames = []
    for f in raw_files:
        try:
            df = pd.read_csv(f, parse_dates=["Date"], dtype={"Ticker": str})
        except Exception as e:
            log.warning(f"Skipping unreadable file {f.name}: {e}")
            continue
        if df.empty:
            continue
        if "Ticker" not in df.columns:
            df["Ticker"] = _bare_ticker_from_filename(f.stem)
        df = df[(df["Date"] >= start_dt) & (df["Date"] <= end_dt)]
        if not df.empty:
            frames.append(df)

    if not frames:
        log.warning("No data found to build the combined CSV from.")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["Open", "High", "Low", "Close"])

    out = combined[["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]].copy()
    out["Date"] = out["Date"].dt.strftime(date_format)
    out["Symbol"] = out["Ticker"] if metastock_ascii else _prefixed_symbols_series(out["Ticker"], exchange_map)
    out = out.sort_values(["Date", "Symbol"])
    out = _finalize_output_columns(out, metastock_ascii)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, header=not metastock_ascii)
    log.info(f"Wrote combined CSV ({len(out):,} rows, {combined['Ticker'].nunique():,} symbols) to {out_path}")


def build_per_symbol_csvs(raw_dir: Path, out_dir: Path, start: str, end: str,
                           date_format: str = None, tickers=None, exchange_map=None,
                           metastock_ascii: bool = False):
    """Alternative layout: one CSV per symbol (classic Metastock ASCII import).
    Files are named after the (possibly exchange-prefixed) output symbol,
    e.g. NASDAQ.AAPL.csv, not the raw download ticker."""
    date_format = date_format or ("%m/%d/%Y" if metastock_ascii else "%Y%m%d")
    out_dir.mkdir(parents=True, exist_ok=True)
    start_dt = pd.Timestamp(start)
    end_dt = pd.Timestamp(end)

    raw_files = _select_raw_files(raw_dir, tickers, exchange_map)
    exchange_map = exchange_map or {}
    for f in raw_files:
        try:
            df = pd.read_csv(f, parse_dates=["Date"], dtype={"Ticker": str})
        except Exception:
            continue
        if df.empty:
            continue
        df = df[(df["Date"] >= start_dt) & (df["Date"] <= end_dt)]
        if df.empty:
            continue
        df = df.sort_values("Date")
        df["Date"] = df["Date"].dt.strftime(date_format)
        ticker = df["Ticker"].iloc[0] if "Ticker" in df.columns else _bare_ticker_from_filename(f.stem)
        # In strict MetaStock ASCII mode, use the bare ticker -- MetaStock
        # needs this to match your existing security codes, and a prefixed
        # symbol like "NASDAQ.AAPL" would just look like an unrelated new symbol.
        symbol = ticker if metastock_ascii else prefixed_symbol(ticker, exchange_map.get(ticker))
        df["Symbol"] = symbol
        df = _finalize_output_columns(df, metastock_ascii)
        df.to_csv(out_dir / f"{symbol}.csv", index=False, header=not metastock_ascii)

    log.info(f"Wrote per-symbol CSVs to {out_dir}")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def parse_args():
    today = datetime.today()
    ten_years_ago = today - timedelta(days=365 * 10)

    p = argparse.ArgumentParser(
        description="Download historical NASDAQ/NYSE/BSE/NSE data for Metastock."
    )
    p.add_argument("--start", default=ten_years_ago.strftime("%Y-%m-%d"),
                    help="Start date YYYY-MM-DD (default: 10 years ago)")
    p.add_argument("--end", default=today.strftime("%Y-%m-%d"),
                    help="End date YYYY-MM-DD (default: today)")
    p.add_argument("--exchanges", nargs="+", default=["NASDAQ", "NYSE"],
                    choices=SUPPORTED_EXCHANGES,
                    help="Which exchanges to include -- US: NASDAQ, NYSE. India: BSE, NSE. "
                         "Europe: LSE, EURONEXT, XETRA. East Asia: TSE, HKEX, SSE, SZSE. "
                         "Other: TSX, ASX, KRX, SGX, SIX. Note: only NASDAQ/NYSE/NSE have an "
                         "auto-fetchable bulk symbol list -- see --ticker-file for the rest.")
    p.add_argument("--ticker", default="ALL",
                    help="A single ticker symbol to download, or 'ALL' (default) for every "
                         "ticker in the selected --exchanges. Always use the bare symbol (e.g. "
                         "RELIANCE, not RELIANCE.NS, or VOD not VOD.L) -- the Yahoo suffix for "
                         "non-US exchanges is added automatically based on --ticker-exchange.")
    p.add_argument("--ticker-exchange", choices=SUPPORTED_EXCHANGES, default=None,
                    help="Which exchange --ticker belongs to. Prefixes the output Symbol column "
                         "(NASDAQ.AAPL / NSE.TCS / LSE.VOD / etc.), and for every non-US exchange "
                         "it's also REQUIRED to fetch the right security at all (Yahoo needs a "
                         "suffix like .NS/.L/.HK, which this adds automatically). Not needed for "
                         "--ticker ALL.")
    p.add_argument("--ticker-file", default=None,
                    help="Load the ticker directory from a local CSV instead of fetching it from "
                         "the web -- the workaround for exchanges with no auto-fetchable bulk list "
                         "(everything except NASDAQ/NYSE/NSE), or to use your own curated list for "
                         "any exchange. Needs a Symbol column (a Name column is optional). Use "
                         "with --exchanges to say which exchange these tickers belong to, e.g. "
                         "--ticker-file bse_list.csv --exchanges BSE")
    p.add_argument("--output", default="./market_data", help="Output root folder")
    p.add_argument("--layout", choices=["daily", "per-symbol", "combined", "all"], default="daily",
                    help="Output layout: one file per day, one per symbol, one single "
                         "clubbed CSV with everything, or all three")
    p.add_argument("--skip-download", action="store_true",
                    help="Skip downloading; just rebuild CSVs from existing raw cache")
    p.add_argument("--refresh-tickers", action="store_true",
                    help="Re-fetch the NASDAQ/NYSE ticker list instead of reusing tickers.csv "
                         "(do this periodically -- new listings/delistings happen constantly)")
    p.add_argument("--no-etfs", action="store_true", help="Exclude ETFs, keep common stocks only")
    p.add_argument("--limit", type=int, default=None,
                    help="Only process the first N tickers (useful for testing)")
    p.add_argument("--pause", type=float, default=0.3,
                    help="Seconds each worker pauses between its own requests "
                         "(politeness/rate-limiting -- lower this cautiously)")
    p.add_argument("--workers", type=int, default=10,
                    help="Number of tickers to download in parallel (default: 10). Downloads "
                         "are I/O-bound so threads help a lot; raise cautiously, or lower to 1 "
                         "for fully serial downloads if you start seeing rate-limit errors.")
    p.add_argument("--date-format", default=None,
                    help="strftime format for the Date column. Default: %%Y%%m%%d normally, "
                         "or %%m/%%d/%%Y automatically when --metastock-ascii is set.")
    p.add_argument("--metastock-ascii", action="store_true",
                    help="Write the recognized multi-symbol 'MetaStock ASCII' layout instead of "
                         "the default one: Symbol,Period,Date,Open,High,Low,Close,Volume with NO "
                         "header row and Period always 'D' (daily) -- this is the format EOD data "
                         "vendors use for direct MetaStock import. Applies to whichever --layout "
                         "you've chosen (daily/per-symbol/combined/all).")
    p.add_argument("--split-by-exchange", action="store_true",
                    help="Download and build output into separate NASDAQ/ and NYSE/ subfolders "
                         "under --output, each with its own raw/ cache and its own daily/by_symbol/"
                         "combined output -- rather than one shared set of folders with both "
                         "exchanges mixed together.")
    p.add_argument("--bse-source", choices=["yahoo", "bhavcopy"], default="yahoo",
                    help="Where BSE tickers' prices come from. 'yahoo' (default) is quick but "
                         "Yahoo's BSE data is patchy (empty/sparse BSE tickers fall back to the "
                         "same stock's NSE data). 'bhavcopy' builds history from BSE's official "
                         "daily files -- complete, covers BSE-only stocks, but downloads one file "
                         "per trading day (cached under <output>/bhavcopy_cache) so a first "
                         "10-year run takes a while.")
    p.add_argument("--no-split-adjust", action="store_true",
                    help="With --bse-source bhavcopy: leave prices unadjusted for splits/bonuses "
                         "(default adjusts them using Yahoo's corporate-actions data).")
    return p.parse_args()


def _download_and_build(tickers, exchange_map, raw_dir, daily_dir, per_symbol_dir, combined_path, args):
    """Runs the full download -> build pipeline for one set of tickers into
    one set of directories. Building only happens once, after every
    download in this batch has finished -- not incrementally per-ticker."""
    if not args.skip_download:
        download_all(tickers, args.start, args.end, raw_dir, pause=args.pause,
                     exchange_map=exchange_map, max_workers=args.workers,
                     bse_source=args.bse_source, bse_adjust_splits=not args.no_split_adjust)
        report_data_quality(raw_dir, tickers, exchange_map)
    else:
        log.info("Skipping download step (--skip-download); using existing raw cache")

    # Pass `tickers` through explicitly so the output only reflects what was
    # requested this run (e.g. a single --ticker, or a --limit'ed test run)
    # rather than everything that happens to be cached in raw_dir already.
    if args.layout in ("daily", "all"):
        build_daily_csvs(raw_dir, daily_dir, args.start, args.end, args.date_format,
                          tickers=tickers, exchange_map=exchange_map,
                          metastock_ascii=args.metastock_ascii)
    if args.layout in ("per-symbol", "all"):
        build_per_symbol_csvs(raw_dir, per_symbol_dir, args.start, args.end, args.date_format,
                               tickers=tickers, exchange_map=exchange_map,
                               metastock_ascii=args.metastock_ascii)
    if args.layout in ("combined", "all"):
        build_combined_csv(raw_dir, combined_path, args.start, args.end, args.date_format,
                            tickers=tickers, exchange_map=exchange_map,
                            metastock_ascii=args.metastock_ascii)


def main():
    args = parse_args()

    if yf is None and not args.skip_download:
        log.error("yfinance is not installed. Run: pip install yfinance")
        sys.exit(1)

    out_root = Path(args.output)
    out_root.mkdir(parents=True, exist_ok=True)
    start_file_logging(out_root)
    ticker_cache = out_root / "tickers.csv"

    log.info(f"Date range: {args.start} to {args.end}")
    log.info(f"Exchanges: {args.exchanges}")

    ticker_arg = normalize_ticker_input(args.ticker or "ALL")
    exchange_map = {}

    if ticker_arg == "ALL":
        if args.ticker_file:
            if len(args.exchanges) != 1:
                log.warning(f"--ticker-file doesn't fetch per-exchange -- tagging every row in "
                            f"the file as {args.exchanges[0]} (the first --exchanges given). "
                            f"Run it again separately per exchange if you need to mix a file "
                            f"with a web-fetched exchange.")
            directory_df = load_tickers_from_file(args.ticker_file, args.exchanges[0])
        else:
            directory_df = fetch_ticker_directory(
                args.exchanges, include_etfs=not args.no_etfs, cache_path=ticker_cache,
                refresh=args.refresh_tickers,
            )
        tickers = directory_df["Symbol"].tolist()
        exchange_map = dict(zip(directory_df["Symbol"], directory_df["Exchange"]))
        if args.limit:
            tickers = tickers[: args.limit]
            log.info(f"Limiting to first {args.limit} tickers for this run")
    else:
        tickers = [ticker_arg]
        log.info(f"Single ticker mode: {ticker_arg}")
        if args.ticker_exchange:
            exchange_map[ticker_arg] = args.ticker_exchange
        else:
            log.info(f"No --ticker-exchange given for {ticker_arg} -- its output Symbol will be "
                      f"unprefixed. If this is a non-US ticker (India, Europe, Asia, etc.), "
                      f"--ticker-exchange is actually REQUIRED (Yahoo needs an exchange-specific "
                      f"suffix to find it at all, not just for display) -- pass "
                      f"--ticker-exchange, e.g. NSE, LSE, HKEX (see --help for the full list).")

    if args.split_by_exchange:
        groups = {}
        for t in tickers:
            groups.setdefault(exchange_map.get(t, "UNKNOWN"), []).append(t)
        for ex, ex_tickers in groups.items():
            log.info(f"=== {ex}: {len(ex_tickers)} ticker(s) ===")
            ex_dir = out_root / ex
            ex_exchange_map = {t: exchange_map.get(t) for t in ex_tickers}
            _download_and_build(
                ex_tickers, ex_exchange_map,
                ex_dir / "raw", ex_dir / "daily", ex_dir / "by_symbol",
                ex_dir / "combined" / "all_data.csv", args,
            )
    else:
        _download_and_build(
            tickers, exchange_map,
            out_root / "raw", out_root / "daily", out_root / "by_symbol",
            out_root / "combined" / "all_data.csv", args,
        )

    log.info("All done.")


if __name__ == "__main__":
    main()
