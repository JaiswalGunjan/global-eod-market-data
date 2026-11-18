"""
The download window's logic without any UI: ticker lists and search, remembered settings, run validation, and
the download/build runner. Shared by the Tk window (eod_gui.py) and the browser app (eod_web.py), so both behave
the same. Nothing here touches a UI toolkit; results go out through callbacks, called from the worker thread.
"""

import json
import logging
import os
import subprocess
import sys
import threading
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

import app_paths
import eod_downloader as core
import hourly_downloader as hourly

CACHE_DIR = app_paths.ticker_cache_dir()
SETTINGS_PATH = CACHE_DIR / "gui_settings.json"   # remembered choices of the download window
EARLIEST_DATE = date(1990, 1, 1)                   # what the "Max" date preset starts from
LOG_FORMAT = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")
ENTRY_SEP = " — "                                  # separates "SYMBOL" from "Company Name" in list entries
SEARCH_LIMIT = 300

LAYOUTS = [("Day files only", "none"), ("Also one combined file", "combined")]   # day files = bhavcopy_*
HOURLY_NOTE = ("Hourly: Yahoo only has the last ~730 days of 1h bars. Output goes to "
               "<save location>/<EXCHANGE>/<EXCHANGE>_YYYYMMDD_HHMM.csv (all tickers for one hour per "
               "file). Re-run regularly -- each run "
               "appends new bars, so the cached history grows past 2 years. Only NSE, NASDAQ and NYSE "
               "tickers are downloaded, all from Yahoo (the Alpaca / NSE bhavcopy sources are daily only).")


def ensure_cache_dir(cache_dir: Path = None) -> Path:
    d = Path(cache_dir or CACHE_DIR)
    d.mkdir(parents=True, exist_ok=True)
    return d


def ticker_cache_path(ex: str, cache_dir: Path = None) -> Path:
    return Path(cache_dir or CACHE_DIR) / f"{ex.lower()}_tickers.csv"


def years_ago(years: int) -> date:
    today = date.today()
    try:
        return today.replace(year=today.year - years)
    except ValueError:  # Feb 29 -> a non-leap target year
        return today.replace(year=today.year - years, day=28)


DEFAULT_PRESET = "1D"   # the window opens on today .. today
DEFAULT_MARKETS = ("NASDAQ", "NYSE", "NSE")   # ticked on a first start (no saved settings)


def date_presets() -> list:
    """[(label, start date)] for the quick-range choices; each sets end = today (inclusive)."""
    today = date.today()
    return [("1D", today), ("1W", today - timedelta(days=7)), ("1Y", years_ago(1)), ("5Y", years_ago(5)),
            ("10Y", years_ago(10)), ("YTD", date(today.year, 1, 1)), ("Max", EARLIEST_DATE)]


def preset_range(label: str):
    """(start, end) dates for a quick-range label, or None for an unknown label (custom dates)."""
    start = dict(date_presets()).get(label)
    return None if start is None else (start, date.today())


def saved_dates(s: dict):
    """(start, end, preset) from a saved settings dict. A saved quick range is recomputed from today, so "1D"
    always opens on today; settings without the key (older files) get DEFAULT_PRESET; preset "" = the saved
    custom dates (a saved "today" end stays today). Raises on unreadable custom dates."""
    preset = s.get("date_preset", DEFAULT_PRESET)
    rng = preset_range(preset) if preset else None
    if rng:
        return rng[0], rng[1], preset
    start = datetime.strptime(s["start"], "%Y-%m-%d").date()
    end = date.today() if s.get("end_is_today") else datetime.strptime(s["end"], "%Y-%m-%d").date()
    return start, end, ""


def default_output_paths() -> dict:
    """One save location per interval, so daily and hourly data never land in the same folder."""
    return {"daily": str(app_paths.daily_output()), "hourly": str(app_paths.hourly_output())}


def open_folder(path: Path):
    """Open a folder in the OS file browser. Best-effort, never raises."""
    try:
        if sys.platform.startswith("win"):
            os.startfile(str(path))  # noqa: S606 (Windows-only API)
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])
    except Exception:
        pass


# ------------------------------------------------------------------------------------------------ tickers --

def load_cached_tickers(cache_dir: Path = None) -> dict:
    """{exchange: {symbol: name}} from the on-disk ticker caches, so search works at startup without a refresh."""
    out = {ex: {} for ex in core.SUPPORTED_EXCHANGES}
    for ex in core.SUPPORTED_EXCHANGES:
        path = ticker_cache_path(ex, cache_dir)
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path, dtype=str).fillna("")
            names = df["Name"] if "Name" in df.columns else [""] * len(df)
            out[ex] = dict(zip(df["Symbol"], names))
        except Exception:
            continue  # unreadable/odd cache: just skip, Refresh rebuilds it
    return out


def refresh_tickers(markets, cache_dir: Path = None):
    """Re-fetch the ticker directory of each market. Returns ({exchange: {symbol: name}}, bse_empty)."""
    ensure_cache_dir(cache_dir)
    got, bse_empty = {}, False
    for m in markets:
        df = core.fetch_ticker_directory([m], include_etfs=True, cache_path=ticker_cache_path(m, cache_dir),
                                         refresh=True)
        got[m] = dict(zip(df["Symbol"], df["Name"]))
        if m == "BSE" and df.empty:
            bse_empty = True
    return got, bse_empty


BSE_EMPTY_MESSAGE = (
    "The BSE ticker list couldn't be downloaded automatically (BSE's site may be "
    "down or have changed -- see the log for details).\n\n"
    "You can still use 'Load tickers from file...' with a BSE list you export "
    "yourself (bseindia.com/corporates/List_Scrips.aspx), or just search/type "
    "individual BSE tickers directly (e.g. TCS).")


def format_entry(symbol, name, exchange=None) -> str:
    display_symbol = core.prefixed_symbol(symbol, exchange) if exchange else symbol
    name = (name or "").strip()
    return f"{display_symbol}{ENTRY_SEP}{name}" if name else display_symbol


def display_text_to_symbol(text: str) -> str:
    """'NASDAQ.AAPL — Apple Inc. ...' -> 'AAPL' (the key in the selection)."""
    if ENTRY_SEP in text:
        text = text.split(ENTRY_SEP, 1)[0]
    text = text.strip()
    for prefix in core.EXCHANGE_PREFIX.values():
        lead = f"{prefix}."
        if text.startswith(lead):
            return text[len(lead):]
    return text


def ticker_entries(all_tickers: dict, markets) -> list:
    """[(symbol, name, exchange), ...] for the given markets."""
    entries = []
    for ex in markets:
        entries.extend((s, n, ex) for s, n in all_tickers.get(ex, {}).items())
    return entries


def ticker_count(all_tickers: dict, markets) -> int:
    """How many distinct symbols the markets hold (the status line's 'N tickers loaded')."""
    combined = {}
    for ex in markets:
        combined.update(all_tickers.get(ex, {}))
    return len(combined)


def search(all_tickers: dict, markets, typed: str, limit: int = SEARCH_LIMIT) -> list:
    """Live search: the bare symbol, the exchange-prefixed symbol (NASDAQ.AAPL) and the company name all match.
    Symbol-prefix matches come first, then alphabetical. Empty search = everything, alphabetical."""
    typed = (typed or "").strip().upper()
    entries = ticker_entries(all_tickers, markets)
    if typed:
        matches = [
            (s, n, ex) for s, n, ex in entries
            if s.upper().startswith(typed)
            or typed in (n or "").upper()
            or core.prefixed_symbol(s, ex).upper().startswith(typed)
        ]
        matches.sort(key=lambda e: (not e[0].upper().startswith(typed), e[0]))
    else:
        matches = sorted(entries, key=lambda e: e[0])
    return matches[:limit]


def resolve_search_return(typed: str, matches: list):
    """What Enter in the search box does. Returns ("select", (symbol, name, exchange)) for an exact or single
    match, or a typed custom ticker when nothing matches; ("multiple", n) when several match; None if empty."""
    typed = (typed or "").strip().upper()
    if not typed:
        return None
    exact = next((e for e in matches if e[0] == typed), None)
    if exact:
        return "select", exact
    if len(matches) == 1:
        return "select", matches[0]
    if matches:
        return "multiple", len(matches)
    return "select", (typed, "", None)   # not in the loaded directory (e.g. a brand-new IPO), exchange unknown


def multiple_matches_message(typed: str, n: int) -> str:
    return (f"{n} matches for '{typed.strip().upper()}' -- select the one(s) "
            "you want from the list on the left and click Add ▸ (or double-click one).")


# ----------------------------------------------------------------------------------------------- settings --

def load_settings(path: Path = None):
    """The saved settings dict, or None (first run / unreadable file)."""
    try:
        return json.loads(Path(path or SETTINGS_PATH).read_text(encoding="utf-8"))
    except Exception:
        return None


def save_settings(d: dict, path: Path = None) -> bool:
    try:
        p = Path(path or SETTINGS_PATH)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(d, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False  # remembering settings is a convenience; never block on it


# --------------------------------------------------------------------------------------------- validation --

def validate_run(markets, all_mode: bool, selected: dict, start_s: str, end_s: str, output: str,
                 default_output: str, interval: str):
    """The checks the Download button makes. Returns (None, output_dir) when the run may start, else
    ((kind, title, message), None) with kind 'warning' or 'error'. Creates the output folder."""
    if all_mode:
        if not markets:
            return ("warning", "No market selected",
                    "Please select at least one market (NASDAQ, NYSE, BSE, NSE), "
                    "or uncheck ALL and pick specific ticker(s) instead."), None
    elif not selected:
        return ("warning", "No tickers selected",
                "Please select at least one ticker: search, then click Add ▸ (or "
                "double-click a match) -- or check ALL instead."), None
    try:
        start_dt = datetime.strptime(start_s, "%Y-%m-%d")
        end_dt = datetime.strptime(end_s, "%Y-%m-%d")
    except (ValueError, TypeError):
        return ("error", "Invalid date", "Please enter dates as YYYY-MM-DD."), None
    if start_dt > end_dt:
        return ("error", "Invalid date range", "Start date must be before end date."), None
    output_dir = Path((output or "").strip() or default_output)
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        return ("error", "Can't create output folder", str(e)), None
    if interval == "hourly":
        chosen = markets if all_mode else {ex for (_n, ex) in selected.values()}
        if not set(chosen) & set(hourly.HOURLY_EXCHANGES):
            return ("warning", "Hourly not available",
                    "Hourly data is only available for NSE, NASDAQ and NYSE. "
                    "Select one of those markets (or tickers from them)."), None
    return None, output_dir


# ------------------------------------------------------------------------------------------------- runner --

class Runner:
    """Runs one download at a time on the caller's thread (start it on a worker thread).
    Callbacks (all optional, called from the worker thread):
        on_progress(done, total, failed, no_data), on_status(text), on_finished(status, message)
    with status 'success' / 'cancelled' / 'error'."""

    def __init__(self, on_progress=None, on_status=None, on_finished=None, cache_dir: Path = None):
        self.cancel_event = threading.Event()
        self.on_progress = on_progress or (lambda *a: None)
        self.on_status = on_status or (lambda *a: None)
        self.on_finished = on_finished or (lambda *a: None)
        self.cache_dir = cache_dir
        self.write_bhavcopy = True

    def _cache_path(self, ex):
        ensure_cache_dir(self.cache_dir)
        return ticker_cache_path(ex, self.cache_dir)

    @staticmethod
    def sources(us_alpaca: bool, nse_official: bool) -> dict:
        return {"NASDAQ": "alpaca" if us_alpaca else "yahoo",
                "NYSE": "alpaca" if us_alpaca else "yahoo",
                "NSE": "bhavcopy" if nse_official else "yahoo"}

    def run_daily(self, markets, ticker_selection, exchange_snapshot, start_s, end_s, output_dir, layout,
                  metastock_ascii, bse_source="yahoo", bse_adjust=True, sources=None):
        output_dir = Path(output_dir)
        try:
            core.start_file_logging(output_dir)
            exchange_map = {}
            # an Alpaca exchange with no alpaca_adjusted/ files yet uses Yahoo (core.effective_source logs why)
            sources = {ex: core.effective_source(ex, output_dir / ex, src) for ex, src in (sources or {}).items()}
            # Alpaca / NSE-bhavcopy exchanges bring their own symbol lists (delisted included):
            # exchange -> None (every symbol) or the selected tickers
            special = {}

            groups = None
            if ticker_selection == "ALL":
                # One group per market, straight from that market's own directory: the same symbol can
                # be listed on several (INFY/HAL on NSE and NYSE) and a symbol->exchange dict would let
                # one listing shadow the other.
                groups = {}
                for m in markets:
                    if sources.get(m, "yahoo") != "yahoo":
                        special[m] = None
                        continue
                    df = core.fetch_ticker_directory([m], include_etfs=True, cache_path=self._cache_path(m),
                                                     refresh=False)
                    groups[m] = sorted(set(df.loc[df["Exchange"] == m, "Symbol"]))
                tickers = sorted({t for g in groups.values() for t in g})
            else:
                tickers = list(ticker_selection)
                for t in tickers:
                    ex = exchange_snapshot.get(t)
                    if ex:
                        exchange_map[t] = ex
                    else:
                        core.log.warning(
                            f"Exchange unknown for {t}. Its output Symbol will be unprefixed, "
                            f"and if it's actually an NSE/BSE ticker the download will FAIL "
                            f"outright (Yahoo needs the .NS/.BO suffix to find it at all). "
                            f"Search for it via 'Refresh ticker list' first so its exchange is known."
                        )

            # Always one folder per exchange: output_dir/<EXCHANGE>/{yahoo_adjusted,daily_adjusted,
            # by_symbol,combined,bhavcopy}. Unknown-exchange tickers go to UNKNOWN/.
            if groups is None:
                groups = {}
                for t in tickers:
                    groups.setdefault(exchange_map.get(t) or "UNKNOWN", []).append(t)
            for ex in list(groups):
                if sources.get(ex, "yahoo") != "yahoo":
                    special[ex] = groups.pop(ex)
            if not tickers and not special:
                raise RuntimeError("No tickers resolved for the selected market(s)/ticker(s).")

            n_special = 0
            for ex, wanted in special.items():
                if self.cancel_event.is_set():
                    break
                n_special += self.run_source(ex, wanted, sources[ex], start_s, end_s, output_dir, layout,
                                             metastock_ascii)

            for ex, ex_tickers in groups.items():
                if self.cancel_event.is_set():
                    break
                core.log.info(f"=== {ex}: {len(ex_tickers)} ticker(s) ===")
                ex_dir = output_dir / ex
                ex_exchange_map = {t: (None if ex == "UNKNOWN" else ex) for t in ex_tickers}
                core.download_all(ex_tickers, start_s, end_s, ex_dir / "yahoo_adjusted", pause=0.3,
                                  progress_callback=self.on_progress,
                                  exchange_map=ex_exchange_map, max_workers=10,
                                  cancel_event=self.cancel_event,
                                  bse_source=bse_source, bse_adjust_splits=bse_adjust)
                if self.cancel_event.is_set():
                    break
                core.report_data_quality(ex_dir / "yahoo_adjusted", ex_tickers, ex_exchange_map)
                self.build_outputs(ex_dir, ex_tickers, ex_exchange_map, layout, metastock_ascii,
                                   start_s, end_s, output_dir)

            if self.cancel_event.is_set():
                self.on_finished("cancelled",
                                 f"Cancelled. Whatever finished downloading is cached under:\n{output_dir}\n"
                                 f"Re-run the same settings later to resume and build outputs.")
            else:
                n_yahoo = sum(len(g) for g in groups.values())
                self.on_finished("success",
                                 f"Done -- {n_yahoo + n_special:,} ticker(s) processed.\nSaved under:\n{output_dir}")
        except Exception as e:
            core.log.error(f"GUI run failed: {e}")
            self.on_finished("error", str(e))

    def run_hourly(self, markets, ticker_selection, exchange_snapshot, start_s, end_s, output_dir,
                   metastock_ascii):
        output_dir = Path(output_dir)
        try:
            core.start_file_logging(output_dir)
            pairs = []
            if ticker_selection == "ALL":
                for m in markets:
                    if m not in hourly.HOURLY_EXCHANGES:
                        core.log.warning(f"Hourly data isn't supported for {m}; skipping it.")
                        continue
                    df = core.fetch_ticker_directory([m], include_etfs=True, cache_path=self._cache_path(m),
                                                     refresh=False)
                    df = df[df["Exchange"] == m]
                    pairs += [(str(s), m) for s in df["Symbol"].dropna()]
            else:
                for t in ticker_selection:
                    ex = exchange_snapshot.get(t)
                    if ex in hourly.HOURLY_EXCHANGES:
                        pairs.append((t, ex))
                    else:
                        core.log.warning(f"Skipping {t}: hourly data needs a known NSE/NASDAQ/NYSE "
                                         f"ticker (exchange: {ex or 'unknown'}).")
            if not pairs:
                raise RuntimeError("No NSE/NASDAQ/NYSE tickers to download hourly data for.")

            hourly.download_all_hourly(pairs, output_dir, workers=8,
                                       progress_callback=self.on_progress,
                                       cancel_event=self.cancel_event)
            if not self.cancel_event.is_set():
                hourly.build_hour_files(pairs, output_dir, metastock_ascii, start_s, end_s)

            if self.cancel_event.is_set():
                self.on_finished("cancelled", f"Cancelled. Finished tickers are cached under:\n{output_dir}\n"
                                              f"Re-run the same settings later to resume.")
            else:
                self.on_finished("success", f"Done -- hourly data for {len(pairs):,} ticker(s).\nSaved under:\n"
                                            f"{output_dir}")
        except Exception as e:
            core.log.error(f"GUI hourly run failed: {e}")
            self.on_finished("error", str(e))

    def run_source(self, ex, wanted, src, start_s, end_s, output_dir, layout, metastock_ascii) -> int:
        """Alpaca (NASDAQ/NYSE) or NSE bhavcopy: update the exchange's source folder (all of it, or only
        `wanted` for Alpaca), then build the outputs from it into the *_adjusted folders.
        Returns how many tickers were built."""
        output_dir = Path(output_dir)
        ex_dir = output_dir / ex
        what = "every symbol" if wanted is None else f"{len(wanted)} selected ticker(s)"
        core.log.info(f"=== {ex}: {src} source, {what} ===")
        self.on_status(f"{ex}: updating the {src} data...")
        try:
            core.update_source(ex, ex_dir, src, start_s, end_s, download=True, wanted=wanted,
                               progress_callback=self.on_progress, cancel_event=self.cancel_event)
        except Exception as e:
            core.log.error(f"{ex}: updating the {src} data failed ({e}); building from the files already there")
        if self.cancel_event.is_set():
            return 0
        src_dir, tickers = core.source_tickers(ex, ex_dir, src, wanted)
        if not tickers:
            core.log.error(f"{ex}: nothing to build from {src_dir}.")
            return 0
        if src == "alpaca":
            core.write_alpaca_exits(ex, ex_dir)
        self.on_status(f"{ex}: building {len(tickers):,} ticker(s)...")
        self.build_outputs(ex_dir, tickers, {t: ex for t in tickers}, layout, metastock_ascii, start_s, end_s,
                           output_dir, src_dir=src_dir, suffix=core.SOURCE_FOLDERS[src][1])
        return len(tickers)

    def build_outputs(self, base_dir, tickers, exchange_map, layout, metastock_ascii, start_s, end_s,
                      output_root, src_dir=None, suffix="_adjusted"):
        """Runs once, after all downloads for `tickers` have finished -- never
        incrementally per-ticker, so combining a big batch only happens once.
        src_dir/suffix: build from an Alpaca / NSE-official folder into daily<suffix>/ etc."""
        base_dir = Path(base_dir)
        raw_dir = src_dir or base_dir / "yahoo_adjusted"
        if layout == "combined":
            core.build_combined_csv(raw_dir, base_dir / f"combined{suffix}" / "all_data.csv", start_s, end_s,
                                    tickers=tickers, exchange_map=exchange_map,
                                    metastock_ascii=metastock_ascii)
        if self.write_bhavcopy:
            core.build_bhavcopy_files(raw_dir, output_root, start_s, end_s,
                                      tickers=tickers, exchange_map=exchange_map, folder=f"bhavcopy{suffix}")


def progress_text(done, total, failed, no_data) -> str:
    pct = 100 * done / total if total else 100
    return f"{pct:.1f}% | {done}/{total} | failed={failed} | no_data={no_data}"
