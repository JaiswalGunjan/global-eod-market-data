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
python rebuild_combined.py --exchange NSE                      # offline rebuild of <root>/<EX>/combined/all_data.csv with progress/ETA (all exchanges if omitted)
python hourly_downloader.py --ticker RELIANCE --ticker-exchange NSE --output ./scratch_hourly
python hourly_downloader.py --skip-download --output ./market_data_hourly   # rebuild hour files from raw_1h/ caches
```

A `venv/` exists in the project root. `market_data_daily/` (daily), `market_data_hourly/` (hourly) and `.ticker_cache/` hold real downloaded data — don't delete them; use a separate `--output` dir for experiments.

## Architecture

**`eod_downloader.py` is the core library *and* the CLI.** `eod_gui.py` and `rebuild_combined.py` both `import eod_downloader as core` and reuse its functions, so all files must stay in the same folder and changes to core function signatures affect the GUI.

Pipeline (`main()` → `_download_and_build()`):
1. **Ticker directory** — `fetch_ticker_directory()` auto-fetches only for `BULK_FETCHABLE_EXCHANGES` (NASDAQ/NYSE from Nasdaq Trader, NSE from its equity list, BSE from the latest daily bhavcopy). Other exchanges need `--ticker-file` or single `--ticker`. Index tickers (e.g. `^NSEI`) are added via `EXTRA_INDEX_TICKERS` / `_inject_extra_tickers()` on both the fresh-fetch and cached-read paths, so new indices appear without `--refresh-tickers`. Cached to `<output>/tickers.csv` (CLI) or `.ticker_cache/<ex>_tickers.csv` (GUI).
2. **Download** — `download_all()` runs `download_ticker()` in a `ThreadPoolExecutor` (default 10 workers) with a `cancel_event` and `progress_callback` (used by the GUI). Each ticker is cached to `raw/<EXCHANGE>.<TICKER>.csv`. Re-runs are resumable/incremental: fully-covered tickers return `"cached"`, tickers whose cache covers the start date only fetch the missing tail. Empty results write an empty marker file (`"no_data"`) so they aren't retried.
3. **Build** — `build_daily_csvs` / `build_per_symbol_csvs` / `build_combined_csv` read `raw/` and write `daily/`, `by_symbol/`, `combined/all_data.csv` per `--layout`. Only the tickers requested this run are included, not everything in `raw/`. `_format_output()` + `_write_csv()` handle formatting for every layout; `--metastock-ascii` means header `<TICKER>,<PER>,<DTYYYYMMDD>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>`, bare ticker (`^` stripped), `YYYYMMDD`, 4-dp prices, CRLF. Sort on Timestamps *before* formatting dates. `build_bhavcopy_files()` always runs (unless `--no-bhavcopy` / GUI checkbox) and merges into `<output>/<EX>/bhavcopy/<EX>_YYYYMMDD.csv`.
4. `report_data_quality()` flags tickers with suspiciously few rows. Steps 2–3 always run separately per exchange under `<output>/<EXCHANGE>/` (raw, daily, by_symbol, combined, bhavcopy); `--split-by-exchange` is a hidden no-op kept for old scripts.

### Hourly data (`hourly_downloader.py`)
Separate CLI + module (imported by the GUI as `hourly`) for 1h bars, NSE/NASDAQ/NYSE only. Yahoo serves only the last ~730 days of 1h bars; the `raw_1h/` cache is never trimmed and re-runs append, so history grows over time. Datetimes are exchange-local, tz stripped. Layout is per exchange: `<out>/<EX>/raw_1h/<EX>.<TICKER>.csv` (cache) and `<out>/<EX>/<EX>_YYYYMMDD_HHMM.csv` (all tickers for one bar, named by bar start time; existing files are merged by symbol, not overwritten). MetaStock mode writes `<TICKER>,<PER>,<DTYYYYMMDD>,<TIME>,...` with PER=60. GUI "Data interval: Hourly" greys out the daily-only layout/bhavcopy options and keeps a separate save location per interval (`output` / `output_hourly` in settings; defaults `market_data_daily` / `market_data_hourly`).

### NSE as-traded / delisted / corporate actions (backtesting)
`raw/` is Yahoo's split/bonus/rights-**adjusted** data. The NSE-only scripts below correct it from NSE's official bhavcopy; full usage is in README section 6.
- `reconcile_nse.py` (diagnostic), `nse_delisted.py` (downloads bhavcopies to `NSE/official_cache/full/`, writes as-traded `NSE/raw_delisted/` + `isin_map.csv`), `nse_factors.py` (`NSE/recon/scale_factors.csv`: `as_traded = yahoo × Factor`, volume ÷ Factor), `nse_stitch.py` (`raw_delisted/stitch_report.csv`, same-ISIN renames, `Stitch=Y/N`), `nse_corporate_actions.py` (`NSE/corporate_actions.csv`, validates the hand-edited `nse_mergers.csv`).
- `eod_downloader.py --price-basis raw [--include-delisted]` applies `price_basis.py` in `_load_raw_frames()`/`_apply_basis()`; output goes to `daily_raw/`, `by_symbol_raw/`, `combined_raw/`, `bhavcopy_raw/` so adjusted files are never overwritten. Raw-basis is NSE only. `--include-delisted` requires raw basis (delisted files are as-traded) and stitches `Stitch=Y` renames (`_stitch_map()`); official rows win over Yahoo on overlapping dates. Builders gained optional `price_basis` / `include_delisted` kwargs (GUI unaffected, defaults unchanged). `build_bhavcopy_files` merges by symbol, so delete `bhavcopy_raw/` before a rebuild if symbol names changed.
- Mergers are events in `corporate_actions.csv`, never spliced (old and new trade concurrently). Seeded ratios in `nse_mergers.csv` are from memory; `recon/merger_check.csv` flags >5% disagreement with prices.
- Re-run `nse_factors.py` after new splits; factors are only as current as its last run.
- NYSE/NASDAQ (Yahoo-only): `us_factors.py` writes the same `recon/scale_factors.csv` from `Ticker.splits` (daily root, or `--interval hourly`), judging per split whether the cache is already as-traded. `hourly_downloader.py --price-basis raw` writes `<EX>/as_traded/`. Delisted = user-supplied symbol list (`--delisted-file`) registered in `<EX>/delisted_tickers.csv`.
- Symbol collisions: the same ticker can exist on NSE and NYSE (INFY, HAL). Always group by the directory's `Exchange` column (done in `main()` and the GUI "ALL" branch), never by a `symbol → exchange` dict. A known remaining quirk: `download_ticker()` returns `"cached"` when an incremental fetch comes back empty, so a transient empty Yahoo response leaves a ticker stale without an error.

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
- All modules log via the shared `logging.getLogger("market_dl")` logger, which is how core/bhavcopy messages reach the GUI log pane. yfinance's own logger is set to CRITICAL on purpose, because "possibly delisted" noise is expected for warrants and units. `start_file_logging()` also writes `<output>/logs/run_<timestamp>.log`.
- Settings persist in `.ticker_cache/gui_settings.json` (`_save_settings` / load counterpart) — add new controls there too.
