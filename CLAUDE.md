# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Downloads daily OHLCV history for 16 exchanges (NASDAQ, NYSE, NSE, BSE, LSE, EURONEXT, XETRA, TSE, HKEX, SSE, SZSE, TSX, ASX, KRX, SGX, SIX) via `yfinance` and writes CSVs for MetaStock import. Plain Python scripts, no package/build step, no test suite, no linter config. `README.md` is the detailed user-facing manual.

## Commands

```bash
pip install -r requirements.txt          # yfinance, pandas, requests, tkcalendar (optional, GUI date picker)
python eod_gui.py                        # Tkinter GUI
python eod_downloader.py --help          # CLI

# Quick smoke tests (no test suite exists — verify with small real runs)
python eod_downloader.py --limit 3 --start 2024-01-01 --output ./scratch_out
python eod_downloader.py --ticker RELIANCE --ticker-exchange NSE --start 2024-01-01
python eod_downloader.py --skip-download --layout combined     # rebuild outputs from raw/ cache, no network
python rebuild_combined.py                                     # offline rebuild of combined/all_data.csv with progress/ETA
```

A `venv/` exists in the project root. `market_data/` and `.ticker_cache/` hold real downloaded data — don't delete them; use a separate `--output` dir for experiments.

## Architecture

**`eod_downloader.py` is the core library *and* the CLI.** `eod_gui.py` and `rebuild_combined.py` both `import eod_downloader as core` and reuse its functions, so all files must stay in the same folder and changes to core function signatures affect the GUI.

Pipeline (`main()` → `_download_and_build()`):
1. **Ticker directory** — `fetch_ticker_directory()` auto-fetches only for `BULK_FETCHABLE_EXCHANGES` (NASDAQ/NYSE from Nasdaq Trader, NSE from its equity list, BSE from the latest daily bhavcopy). Other exchanges need `--ticker-file` or single `--ticker`. Cached to `<output>/tickers.csv` (CLI) or `.ticker_cache/<ex>_tickers.csv` (GUI).
2. **Download** — `download_all()` runs `download_ticker()` in a `ThreadPoolExecutor` (default 10 workers) with a `cancel_event` and `progress_callback` (used by the GUI). Each ticker is cached to `raw/<EXCHANGE>.<TICKER>.csv`. Re-runs are resumable/incremental: fully-covered tickers return `"cached"`, tickers whose cache covers the start date only fetch the missing tail. Empty results write an empty marker file (`"no_data"`) so they aren't retried.
3. **Build** — `build_daily_csvs` / `build_per_symbol_csvs` / `build_combined_csv` read `raw/` and write `daily/`, `by_symbol/`, `combined/all_data.csv` per `--layout`. Only the tickers requested this run are included, not everything in `raw/`. `_format_output()` + `_write_csv()` handle formatting for every layout; `--metastock-ascii` means header `<TICKER>,<PER>,<DTYYYYMMDD>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>`, bare ticker (`^` stripped), `YYYYMMDD`, 4-dp prices, CRLF. Sort on Timestamps *before* formatting dates. `build_bhavcopy_files()` always runs (unless `--no-bhavcopy` / GUI checkbox) and merges into `<output>/bhavcopy/<EX>/<EX>_YYYYMMDD.csv`.
4. `report_data_quality()` flags tickers with suspiciously few rows. `--split-by-exchange` runs steps 2–3 separately per exchange under `<output>/<EXCHANGE>/`.

### Symbol conventions (easy to get wrong)
- `prefixed_symbol()` / `EXCHANGE_PREFIX`: `NSE.TCS` style — used for **raw cache filenames and the output Symbol column** only.
- `yf_download_symbol()` / `YF_TICKER_SUFFIX`: `TCS.NS` style — the **actual Yahoo request**; mandatory for all non-US exchanges. Index tickers (`^NSEI`) are never suffixed.
- The `Ticker` column inside raw CSVs is always the bare ticker.
- MetaStock ASCII output uses the bare ticker (no prefix).

### BSE specifics
- Yahoo's `.BO` data is patchy: `download_ticker()` falls back to the `.NS` listing when BSE data `_looks_sparse()`.
- `bse_bhavcopy.py` is an alternate BSE source (`--bse-source bhavcopy`, lazily imported inside `download_all()`): downloads BSE's official per-day bhavcopy files (UDiFF CSV from 2024-07-08, older `EQ<DDMMYY>_CSV.ZIP` before), caches them in `<output>/bhavcopy_cache/`, and stitches them into the same `raw/BSE.<TICKER>.csv` files. Split/bonus back-adjustment uses Yahoo's corporate actions (cached in `splits.json`), disable with `--no-split-adjust`; unexplained large gaps are only logged.

### GUI (`eod_gui.py`)
- `MarketDataGUI(tk.Tk)` runs downloads/ticker refreshes on daemon threads; results return to Tk via `self.after(0, ...)`, and logging goes through `QueueLogHandler` → polled queue. Never touch Tk widgets from worker threads.
- All modules log via the shared `logging.getLogger("market_dl")` logger, which is how core/bhavcopy messages reach the GUI log pane. `start_file_logging()` also writes `<output>/logs/run_<timestamp>.log`.
- Settings persist in `.ticker_cache/gui_settings.json` (`_save_settings` / load counterpart) — add new controls there too.
