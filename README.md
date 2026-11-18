# EOD Market Data (16 global exchanges → Metastock CSVs)

Downloads daily OHLCV history for tickers on any of 16 exchanges across the
US, India, Europe, East Asia, and elsewhere, and writes it out as CSVs,
ready for Metastock import.

| Region | Exchanges |
|--------|-----------|
| US | NASDAQ, NYSE |
| India | BSE, NSE |
| Europe | LSE (London), EURONEXT (Paris), XETRA (Frankfurt) |
| East Asia | TSE (Tokyo), HKEX (Hong Kong), SSE (Shanghai), SZSE (Shenzhen) |
| Other | TSX (Toronto), ASX (Australia), KRX (Korea), SGX (Singapore), SIX (Switzerland) |

Two ways to run it:
- **`eod_gui.py`** — a point-and-click window (recommended for
  everyday use)
- **`eod_downloader.py`** — the command-line version (recommended for
  scripting/automation, or a full 8,000+ ticker overnight run)

Both files must stay in the same folder — the GUI imports the CLI script
directly and reuses all of its logic.

**A heads-up on most non-US exchanges**: only NASDAQ, NYSE, NSE, and BSE have
a bulk symbol list this tool can fetch automatically (BSE's comes from its
daily bhavcopy file). Everything else (the other 12 global markets) needs one extra step for *bulk*
downloads — see [section 2 below](#a-note-on-exchanges-without-a-bulk-list)
— but individual tickers on any of the 16 work immediately either way, no
extra step needed.

## Data sources and corporate actions (CLI default since 2026-11-10)

| Exchange | Source | Updated by | Adjusted for | Output folders |
|---|---|---|---|---|
| NASDAQ, NYSE | Alpaca (SIP feed), delisted companies included | the Alpaca store `market_data_alpaca/` (read by `alpaca_store.py`) | splits, reverse splits, spin-offs (not dividends) | `daily_adjusted/`, `by_symbol_adjusted/`, `combined_adjusted/`, `bhavcopy_adjusted/` |
| NSE | NSE's official bhavcopies + NSE's corporate-actions list, delisted/merged companies included | `nse_bhavcopy.py` → `NSE/official_adjusted/` | splits, bonuses, rights, demergers, big special dividends; renames joined (TATAMOTORS → TMPV) | `daily_adjusted/`, `by_symbol_adjusted/`, `combined_adjusted/`, `bhavcopy_adjusted/` |
| everything else | Yahoo Finance | `download_ticker()` → `<EX>/yahoo_adjusted/` | splits (Yahoo's own adjustment) | `daily_adjusted/`, `by_symbol_adjusted/`, `combined_adjusted/`, `bhavcopy_adjusted/` |

History is always on **today's** share basis, also when it was downloaded before the event:

* **NSE** is rebuilt from the day-file cache whenever a new day or corporate action arrives (about 2 minutes), so
  every earlier price is re-adjusted. The decisions are in `NSE/official_info/events.csv`; moves beyond x1.6 that no
  event explains are listed in `NSE/official_info/gaps.csv` (not adjusted). Hand-kept input: `nse_demergers.csv`.
  NSE's corporate-actions lists are downloaded once and kept in `NSE/nse_ca/`. nseindia.com is asked again only
  when a run brings in a new trading day, and then only for the days since the last check (plus a week of overlap
  for late corrections); new or changed actions are merged into the cache and trigger the rebuild. A run with no
  new day file makes no corporate-actions request. `python nse_bhavcopy.py --refresh-actions` downloads all
  quarters again.
* **Alpaca** files are extended from the second-to-last stored bar. If Alpaca's adjusted price for that day
  changed, and its corporate-actions list has a split or spin-off for the symbol, the stored history is rescaled
  (exact listed ratio). A change no listed event explains (usually a ticker now used by another company, or an ADS
  ratio change) is **not** appended. It is logged as `skipped` in `<EX>/alpaca_updates.csv`: check those by hand.
  Renames are followed; a reused ticker keeps the old company as `TICKER@date`.
* **Yahoo** caches are extended from the second-to-last cached bar too; if its close changed (a split since the
  last run), the whole history is downloaded again.

```bash
python eod_downloader.py --exchanges NSE NASDAQ NYSE --layout daily   # update + build (MetaStock ASCII by default)
python eod_downloader.py --exchanges NSE --skip-download                                 # offline rebuild
python nse_bhavcopy.py            # NSE source only (download + adjusted rebuild)
```

`--us-source yahoo` / `--nse-source yahoo` bring back the old Yahoo sources (and they stay the default with
`--price-basis raw`, the as-traded pipeline in section 6).

**GUI:** section 1 has two **Data sources** boxes, both ticked by default: *NASDAQ / NYSE from Alpaca* and *NSE from NSE's official bhavcopies*. With ALL ticked, every symbol of that source is built (delisted ones too); with picked tickers, only those are updated and built (a US ticker takes seconds; NSE still checks for new day files and rebuilds `NSE/official_adjusted/` when something changed, about 2 minutes). Untick a box to use Yahoo for that market instead. The progress bar and Cancel work during these steps. Hourly mode always uses Yahoo (the boxes are greyed out).

## 0. GUI (recommended)

```bash
pip install yfinance pandas requests tkcalendar
python eod_gui.py
```

A window opens with:

1. **Market** — checkboxes for the 16 exchanges (NASDAQ, NYSE and NSE
   checked on a first start; after that the last choice is remembered)
2. **Ticker(s)** — leave **ALL** checked (the default) for every ticker in
   the checked market(s), or uncheck it to pick specific ones:
   - Type in the search box — results filter live as you type, no need to
     click or open anything. Search matches symbol, company name, or the
     exchange-prefixed form (`AAPL`, `APPLE`, and `NASDAQ.AAPL` all find the
     same entry). For India, just type the bare symbol (`RELIANCE`, not
     `RELIANCE.NS`) — the Yahoo suffix is added automatically once you pick
     a match.
   - Select one or more matches (click, or shift/ctrl-click for multiple)
     and click **Add ▸**, or just double-click a single match.
   - Selected tickers land in the list on the right — select + **◂ Remove**
     (or double-click) to take one back out, **Clear all** to start over.
   - If you haven't loaded the ticker list yet, clicking 🔍 loads it
     automatically (needs at least one market checked).
   - **BSE**: "Refresh ticker list" builds the BSE list (~5,000 securities)
     from BSE's latest daily bhavcopy. Symbols are BSE tickers (`TCS`,
     `RELIANCE`); the company name column also carries the numeric scrip code,
     so searching `532540` works too. **Yahoo's own BSE coverage is partial**,
     so when a BSE ticker comes back empty or sparse the downloader
     automatically falls back to the same stock's NSE (`.NS`) data (prices are
     nearly identical; logged as "using NSE ... instead"). BSE-only stocks with
     no NSE listing may still end up `no_data`. Yahoo does not accept numeric
     scrip codes. If the fetch ever fails, use "Load tickers from
     file..." as a fallback.
   - **BSE official prices (optional)**: tick *"BSE prices from BSE's official
     daily files"* in the Market box to skip Yahoo for BSE tickers and build
     their history from BSE's own daily bhavcopy files instead -- complete,
     and it covers BSE-only stocks Yahoo can't. See
     [BSE official bhavcopy](#bse-official-bhavcopy-optional-source) below.
   - Cached ticker lists are loaded at startup, so search works immediately.
3. **Date range** — a calendar picker for start/end, plus **Quick range**
   buttons: **1D** (the default: start = end = today) / 1W / 1Y / 5Y / 10Y /
   YTD / Max. The active range is highlighted and is recomputed from today
   each time the window opens; picking dates by hand switches it off and those
   dates are remembered instead. The end date is inclusive. **If you're just seeing plain
   YYYY-MM-DD text boxes instead of a calendar dropdown, that means
   `tkcalendar` isn't installed** — run `pip install tkcalendar` and
   restart the app; the plain text fields still work fine in the meantime,
   they're just less convenient.
4. **CSV layout** — **Data interval** (Daily or Hourly), then single combined
   file (default), one file per day, one per symbol, or all three, plus:
   - **MetaStock ASCII format** — ticked by default (see
     [section 4](#4-about-the-metastock-format)); untick for the plain
     `Symbol,Date,...` layout
   - Data is always saved per exchange: each exchange gets its own
     `<save location>/<EXCHANGE>/` folder with its own `yahoo_adjusted/` cache and
     `daily_adjusted/`/`by_symbol_adjusted/`/`combined_adjusted/`/`bhavcopy_adjusted/` output
   - **Hourly** (NSE/NASDAQ/NYSE only, last ~2 years from Yahoo) writes one
     file per exchange per hour, `<EXCHANGE>/<EXCHANGE>_YYYYMMDD_HHMM.csv`,
     with every ticker's bar for that hour (see `hourly_downloader.py`)
5. **Save location** — any folder, via Browse; **Open folder** jumps there
   once files exist. Daily and Hourly each remember their own folder
   (defaults `market_data_daily` and `market_data_hourly`)

Hit **Download**. Progress, ETA, and any errors stream into the log box at
the bottom — it's the same information you'd see in the console when running
the CLI version. Downloads run **10 at a time in parallel** by default, so
even a large batch finishes well before it would running one at a time.
Click **Cancel** anytime to stop — whatever's already finished downloading
stays cached, so re-running the same settings later picks up where you left
off rather than starting over.

The window remembers your last settings (markets, tickers, dates, layout,
folder, size) between sessions. The Download/Cancel bar is pinned to the bottom,
the settings area scrolls, and the log pane is resizable, so nothing gets hidden
on small or full-screen windows.

Closing the window mid-download just stops it — whatever was already
downloaded stays cached under `yahoo_adjusted/`, so re-running later (GUI or CLI)
picks up where you left off.

### Browser version (`eod_web.py`) and the portable Windows bundle

The same window also runs as a local web page: same sections, defaults,
messages and remembered settings (`gui_settings.json` is shared).

```bash
python eod_web.py                # opens http://127.0.0.1:8790/ in the browser
python eod_web.py --port 0 --no-browser
```

Keep the console window open while using the page; **Quit app** (top right)
or closing the console stops it. Differences from the Tk window: dates use the
browser's date picker (no `tkcalendar` needed), **Browse...** opens an in-page
folder picker, and a refreshed or reopened tab reconnects to a running download.
The server only answers on `127.0.0.1` and checks a per-start token, so other
web sites can't drive it. The default port is 8790 (8765 belongs to the swingbt scan server); if
the port is busy a free one is used and shown in the console window.

**`eod_config.ini`** (optional, next to the scripts or the exe; template:
`eod_config.example.ini`) sets the default folders (`daily_output`,
`hourly_output`, `alpaca_data`, `ticker_cache`) and the web port. Without it
everything uses the folders next to the scripts, as before. The CLIs read it
too (their `--output` / `--root` defaults).

**Portable bundle for a PC without Python:**

```bash
venv\Scripts\python.exe -m pip install -r requirements-build.txt
venv\Scripts\python.exe build_bundle.py          # --no-keys leaves the Alpaca key files out
```

This writes `dist/EOD_Market_Data/` and `dist/EOD_Market_Data_<YYYYMMDD>.zip`
(about 70 MB): `EOD_Market_Data.exe` (the browser app), `cli_daily.exe`,
`cli_hourly.exe`, `eod_config.ini`, the NSE hand-maintained CSVs,
this README, `QUICKSTART.txt` (instructions for the recipient) and
plus NSE's corporate-actions lists (`seed_data/NSE/nse_ca`, about 8 MB,
copied from your `<daily_output>/NSE/nse_ca`; the first NSE run takes them into its cache, so nseindia.com is
only asked about later days). **No price data is included**; the recipient
points `eod_config.ini` at their own folders. NASDAQ/NYSE use Alpaca only when
the Alpaca store (`alpaca_data` in `eod_config.ini`, a `market_data_alpaca/` folder) exists;
otherwise those exchanges fall back to Yahoo automatically, with a log line.

## 1. Install requirements (CLI)

```bash
pip install yfinance pandas requests
```

## 2. Run it

Default run — last 10 years, both exchanges, one CSV per day:

```bash
python eod_downloader.py
```

Pick your own dates later, any time:

```bash
python eod_downloader.py --start 2016-01-01 --end 2026-01-01
```

Only NASDAQ, custom output folder, no ETFs:

```bash
python eod_downloader.py --exchanges NASDAQ --no-etfs --output ./data
```

Just one specific ticker (skips fetching the full symbol directory):

```bash
python eod_downloader.py --ticker AAPL --start 2016-01-01
```

Same, but with the exchange prefix in the output Symbol column too:

```bash
python eod_downloader.py --ticker AAPL --ticker-exchange NASDAQ
```

An NSE (India) stock -- note --ticker-exchange isn't just for prefixing
here, it's REQUIRED: Yahoo needs the .NS suffix (added automatically) to
find the security at all:

```bash
python eod_downloader.py --ticker RELIANCE --ticker-exchange NSE --start 2016-01-01
```

Every NSE-listed stock, using NSE's public bulk list:

```bash
python eod_downloader.py --exchanges NSE --layout combined
```

Every BSE-listed stock (list fetched automatically from BSE's daily bhavcopy;
note Yahoo's BSE coverage is partial):

```bash
python eod_downloader.py --exchanges BSE --layout combined
```

Or point at a CSV you've exported yourself, if the automatic fetch ever fails:

```bash
python eod_downloader.py --exchanges BSE --ticker-file bse_list.csv --layout combined
```

Every exchange is always downloaded into its own folder
(`market_data_daily/NASDAQ/...`, `market_data_daily/NYSE/...`, each with its own
`yahoo_adjusted/` cache and its own combined file) -- for any combination of exchanges:

```bash
python eod_downloader.py --exchanges NSE BSE --layout combined
```

Downloads run 10 at a time in parallel by default; dial it up/down if
needed:

```bash
python eod_downloader.py --workers 20   # faster, more aggressive
python eod_downloader.py --workers 1    # fully serial, gentlest on Yahoo's API
```

Test on a small sample first (recommended before a multi-hour full run):

```bash
python eod_downloader.py --limit 20 --start 2024-01-01
```

If a run gets interrupted, just re-run the same command — tickers that are
already fully cached for your date range are skipped automatically.

The ticker list itself (`tickers.csv`) is fetched once per exchange and then
reused on subsequent runs so you don't hit the exchange's site every time.
New listings and delistings happen constantly though, so refresh it
periodically:

```bash
python eod_downloader.py --refresh-tickers
```

If you only changed the date range and already have raw data downloaded,
you can skip re-downloading and just rebuild the CSVs:

```bash
python eod_downloader.py --skip-download
```

### A note on exchanges without a bulk list

NASDAQ, NYSE, NSE, and BSE (via its daily bhavcopy) provide a plain CSV/text
file listing every ticker, which this tool fetches automatically. **The other
12 exchanges don't** — LSE, EURONEXT, XETRA, TSE, HKEX, SSE, SZSE, TSX, ASX, KRX,
SGX, and SIX all require either a form/login on their site or a paid data
feed to get the full list, with no stable URL a script can just fetch. Two
ways around that:

1. **Individual tickers, no directory needed** — this doesn't require the
   bulk list at all, for any exchange:
   ```bash
   python eod_downloader.py --ticker VOD --ticker-exchange LSE
   python eod_downloader.py --ticker TATASTEEL --ticker-exchange BSE
   python eod_downloader.py --ticker 7203 --ticker-exchange TSE
   ```
   (GUI: type the ticker directly, check the matching market.)

2. **Bulk download from your own file** — export that exchange's list
   yourself (their website, or a data vendor) as a CSV with a `Symbol`
   column (a `Name` column is optional), then point this tool at that file
   instead of fetching from the web:
   ```bash
   python eod_downloader.py --exchanges BSE --ticker-file bse_list.csv --layout combined
   python eod_downloader.py --exchanges LSE --ticker-file lse_list.csv --layout combined
   ```
   (GUI: check that market as the only one selected, click **Load tickers
   from file...**.) BSE's list specifically is exportable from
   [bseindia.com/corporates/List_Scrips.aspx](https://www.bseindia.com/corporates/List_Scrips.aspx);
   for the others, check the exchange's own site or a market-data vendor.

### BSE official bhavcopy (optional source)

Yahoo's BSE data is patchy (see above), so BSE tickers can instead be built
from BSE's own daily files:

```bash
python eod_downloader.py --exchanges BSE --bse-source bhavcopy --layout combined
python eod_downloader.py --exchanges BSE --bse-source bhavcopy --no-split-adjust   # raw prices
```

(GUI: the checkbox in the Market box, plus a second one for split adjustment.)

- **How**: one file per trading day is downloaded and cached in
  `<output>/bhavcopy_cache/` (weekends/holidays are detected and skipped), then
  stitched into the usual `yahoo_adjusted/BSE.<TICKER>.csv` files, so every layout and the
  MetaStock format work unchanged. Works back to at least 2016.
- **Speed**: roughly 250 day-files per ~40 s on a cold cache, so a first
  10-year run takes about 6-7 minutes (plus ~10 min of Yahoo split look-ups for
  the full ~5,000-ticker list); later runs only fetch new days (a fully cached
  re-run takes seconds). During this phase the progress bar counts *days*, not
  tickers.
- **Split/bonus adjustment** (on by default): BSE's files are unadjusted, so a
  10:1 split looks like a 90% crash. Splits/bonuses are taken from Yahoo's
  corporate-actions data (NSE listing first, then BSE) and earlier prices are
  adjusted to match. Dividends are not adjusted. BSE-only stocks Yahoo knows
  nothing about can't be adjusted; any overnight move that looks like an
  unrecorded split (<0.55x or >1.8x) is listed in the log as a warning rather
  than silently changed -- check those before trusting returns.
- **Scope**: only tickers in the *current* BSE list are built (the symbol is
  the BSE ticker, mapped to BSE's scrip code from the latest file), so
  delisted securities still aren't included -- the survivorship caveat
  below still applies.
- **Volume** is shares traded on BSE only, unlike the NSE fallback.

## 3. Output layout

```
market_data_daily/
├── tickers.csv                  # the ticker list used
├── logs/                        # one log file per run
├── NASDAQ/
│   ├── yahoo_adjusted/                     # one cache file per ticker (download cache, keep this)
│   │   ├── NASDAQ.AAPL.csv
│   │   └── ...
│   ├── daily_adjusted/                   # one file per trading day (NASDAQ tickers only)
│   │   ├── 20160115.csv
│   │   └── ...
│   ├── by_symbol_adjusted/               # optional, see "Metastock format" below
│   ├── combined/all_data.csv    # optional: every NASDAQ ticker + date in ONE file
│   └── bhavcopy_adjusted/                # always built (unless --no-bhavcopy), MetaStock ASCII
│       ├── NASDAQ_20240102.csv  # every downloaded stock for that day
│       └── ...
├── NYSE/                        # same structure
└── NSE/                         # same structure
```

`bhavcopy_adjusted/` mirrors the BSE/NSE bhavcopy idea for every exchange: each day's
file holds all stocks for that day, already in MetaStock ASCII format, so you
can point MetaStock's Downloader at a folder and convert day by day. It
accumulates across runs -- downloading one more stock adds/updates just that
stock in each existing day file and keeps everything else.
Turn it off with `--no-bhavcopy` (CLI) or the checkbox in the GUI.

Hourly data (`hourly_downloader.py`, or Hourly in the GUI) uses the same
per-exchange idea under `market_data_hourly/`:

```
market_data_hourly/
├── NSE/
│   ├── raw_1h/NSE.TCS.csv ...   # download cache
│   ├── NSE_20261009_0915.csv    # every NSE ticker's 09:15 bar that day
│   ├── NSE_20261009_1015.csv
│   └── ...
├── NASDAQ/NASDAQ_20261009_0930.csv ...
└── NYSE/NYSE_20261009_0930.csv ...
```

Choose which of these get built with `--layout`:

```bash
python eod_downloader.py --layout daily        # default: one file per day
python eod_downloader.py --layout per-symbol   # one file per ticker
python eod_downloader.py --layout combined     # everything clubbed into one CSV
python eod_downloader.py --layout all          # build all three at once
```

`--layout combined` (or `all`) writes `combined/all_data.csv` containing every
ticker's OHLCV data across the entire date range, sorted by Date then Symbol.
**Heads up**: for the full NYSE+NASDAQ universe over 10 years this can be a
very large file (tens of millions of rows, likely 1GB+) — fine to generate,
but worth knowing before you try to open it in Excel. Because each exchange
gets its own combined file, NASDAQ and NYSE are never mixed together, which
is also the shape you'll want if MetaStock needs them imported as separate
batches.

Each `daily/YYYYMMDD.csv` looks like:

```
Symbol,Date,Open,High,Low,Close,Volume
NASDAQ.AAPL,20160115,24.10,24.55,23.90,24.30,185000000
NASDAQ.MSFT,20160115,50.10,50.80,49.95,50.60,32000000
NYSE.GE,20160115,28.40,28.90,28.10,28.75,41000000
...
```

`Symbol` is exchange-prefixed when the exchange is known — using each
exchange's own standard name, the same one used everywhere else in this
tool (`--exchanges` values, GUI checkboxes, per-exchange folder names):

| Exchange | Example         | Yahoo suffix (actual request) |
|----------|-----------------|--------------------------------|
| NASDAQ   | `NASDAQ.AAPL`   | (none)                         |
| NYSE     | `NYSE.GE`       | (none)                         |
| BSE      | `BSE.TATASTEEL` | `.BO`                          |
| NSE      | `NSE.RELIANCE`  | `.NS`                          |
| LSE      | `LSE.VOD`       | `.L`                           |
| EURONEXT | `EURONEXT.MC`   | `.PA`                          |
| XETRA    | `XETRA.SAP`     | `.DE`                          |
| TSE      | `TSE.7203`      | `.T`                           |
| HKEX     | `HKEX.0700`     | `.HK`                          |
| SSE      | `SSE.600519`    | `.SS`                          |
| SZSE     | `SZSE.000002`   | `.SZ`                          |
| TSX      | `TSX.SHOP`      | `.TO`                          |
| ASX      | `ASX.BHP`       | `.AX`                          |
| KRX      | `KRX.005930`    | `.KS`                          |
| SGX      | `SGX.D05`       | `.SI`                          |
| SIX      | `SIX.NESN`      | `.SW`                          |

(The "Yahoo suffix" column is a completely separate thing — what actually
gets sent to Yahoo Finance to fetch the data, added automatically. You
never type it yourself; it's not related to the output prefix on the
left, which is just this tool's own display convention.)

It falls back to the bare ticker (just `AAPL`) when the exchange isn't known:
- **CLI, `--ticker ALL`**: always prefixed — the full symbol directory fetch
  already knows every ticker's exchange.
- **CLI, `--ticker AAPL`**: prefixed only if you also pass `--ticker-exchange`;
  otherwise left bare, since picking one ticker deliberately skips the full
  directory fetch to stay fast. **For NSE/BSE, `--ticker-exchange` isn't just
  for prefixing — it's required**, since Yahoo needs the `.NS`/`.BO` suffix
  (added automatically) to find the security at all.
- **GUI**: prefixed if that ticker came from a company/symbol search (i.e.
  you clicked "Refresh ticker list" or "Load tickers from file...") or
  you're on ALL mode; if you typed a raw symbol without ever
  refreshing/searching, it's left bare (with a note in the log explaining
  why) — and for NSE/BSE, the download itself will fail without a known
  exchange, not just the prefix.

This only affects the *output* CSVs — the `yahoo_adjusted/` download cache always uses
the bare ticker internally as its canonical form (Yahoo's required
`.NS`/`.BO` suffix is added only for the actual API request, not stored
anywhere else).

## 4. About the Metastock format

Since 2026-11-10 every layout is written in **MetaStock ASCII** by default (CLI and GUI). Use
`--no-metastock-ascii` (CLI; also `hourly_downloader.py`, `rebuild_combined.py`, `update_us.py`) or untick
"MetaStock ASCII format" (GUI) for the friendly `Symbol,Date,Open,High,Low,Close,Volume` layout, which is easy to
open in Excel/pandas but **isn't itself a format MetaStock recognizes**. Note: daily files are rewritten only for the
dates in the run's range, so a folder built earlier in the other layout (e.g. `NASDAQ/daily/` before 2026-11-10)
holds both layouts until it is rebuilt over its whole range. MetaStock ASCII follows the specific convention EOD
data vendors (e.g. EODData) use for direct MetaStock import — confirmed via
their published format docs and MetaStock's own community forum:

```
<TICKER>,<PER>,<DTYYYYMMDD>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>
AAPL,D,20160115,24.1,24.55,23.9,24.3,185000000
GE,D,20160115,28.4,28.9,28.1,28.75,41000000
```

- **Header row of MetaStock field names** — MetaStock's Downloader
  (File > Convert, source "ASCII Text") reads these to identify the columns.
  Without it the first data line is taken as the header and the convert fails.
- **Symbol** is the bare ticker here (`AAPL`, not `NASDAQ.AAPL`) — deliberately
  *not* exchange-prefixed like the default layout is. MetaStock needs this
  to match your existing security codes; a prefixed symbol would just look
  like an unrelated, brand-new ticker to it. Yahoo's index caret is dropped
  (`^NSEI` -> `NSEI`).
- **Period** column is always `D` (daily)
- **Date** is always `YYYYMMDD` (`--date-format` is ignored in this mode)
- Records are in ascending date order per symbol (the combined file is sorted
  by Symbol, then Date), prices are rounded to 4 decimals, volume is a whole
  number, and lines end in CRLF
- Applies to whichever `--layout` you've picked — daily, per-symbol,
  combined, or all

This works with `--layout combined` (multiple symbols in one file) because
that's a genuinely recognized, well-established pattern — EOD vendors sell
bulk historical data in exactly this shape specifically so it can be
imported into MetaStock in one go, not something specific to this tool.
That said, MetaStock's own **native database** is still organized one
security at a time internally; getting a multi-symbol ASCII file into it
goes through MetaStock's own Import/ASCII wizard (or a dedicated converter),
where you'll map columns and confirm the date format. If your specific
MetaStock version's importer turns out to insist on one file per symbol
instead, that's `--layout per-symbol` — the traditional, unambiguous
fallback:

```bash
python eod_downloader.py --layout per-symbol --metastock-ascii
```

or `--layout all` to get every layout in the same run. The per-symbol files
land in `by_symbol_adjusted/<SYMBOL>.csv` (e.g. `NASDAQ.AAPL.csv`), named after the same
exchange-prefixed symbol used inside the file.

**Recommendation**: test with a small run first (`--limit 3` or a couple
of tickers, a short date range) and confirm it actually imports cleanly in
your MetaStock version before running the full multi-year, multi-thousand-
ticker download.

## 5. Things worth knowing

- **Data source**: free Yahoo Finance data via `yfinance`. It's unofficial
  and rate-limited, so downloading ~6,000+ tickers × 10 years will take
  hours, and Yahoo may occasionally throttle or return gaps. The script
  retries failed tickers a few times and logs any that still fail to
  `failed_tickers.txt` so you can re-run just those later.
- **Survivorship bias** (NSE can be fixed, see section 6): the ticker list comes from today's live NASDAQ/NYSE
  symbol directory, so companies that delisted or were acquired earlier in
  your 10-year window won't be included. Free sources generally can't avoid
  this — a paid EOD vendor (Polygon, Tiingo, EOD Historical Data, Norgate,
  Alpaca) would be needed for true point-in-time coverage.
- **Exchange coverage**: "NYSE" here means the core NYSE (Exchange code
  `N`) from Nasdaq Trader's directory — not NYSE American or NYSE Arca. Edit
  the `Exchange` filter in `fetch_ticker_list()` if you want those included
  too.
- **Rate limiting**: `--pause` controls the delay between ticker downloads
  (default 0.3s). Increase it if you start seeing a lot of failures.
- **Re-running to catch up to today is fast, not a full re-download**:
  if a ticker's cache already covers your start date but the end date has
  moved forward (the normal case of running this again next month), only
  the missing days are fetched and merged in — not the whole range again.
  Shrinking the start date to go further back in history still triggers a
  full re-fetch for that ticker, though.
- **Flaky exchange endpoints get retried**: NSE especially, and
  occasionally Nasdaq Trader, intermittently 403/timeout on a single
  request. Directory fetches now retry twice with backoff before giving up.
- **Every run writes a log file** under `<output>/logs/run_<timestamp>.log` — the full
  console/GUI log output, useful for checking what happened after a big
  overnight run.
- **A quick data-quality check runs after every download**: if some
  tickers came back with far fewer rows than the rest of the batch (a
  common sign of a partial network hiccup that still "succeeded" instead
  of cleanly failing), they're listed in a warning so you know to spot-check
  or re-run them. It's a rough heuristic, not a guarantee — a stock that
  genuinely IPO'd partway through your date range will also look
  "suspicious" by this measure even though its data is completely fine, so
  don't treat the warning as proof something's wrong, just as a hint of
  where to look first.

## 6. Backtesting-grade NSE data (as-traded prices, delisted names, corporate actions)

Yahoo's cached prices (`yahoo_adjusted/`) are **retroactively adjusted** for later splits, bonuses and rights
issues (e.g. HDFCBANK in 2016 shows ₹320 instead of the ₹1,282 that actually traded), and symbols
that were delisted or merged away are missing. For NSE this can be corrected from NSE's official
daily bhavcopy. Run the steps in order; all of them are offline-resumable (downloads are cached).

| Step | Command | Output (under `<output>/NSE/`) |
|---|---|---|
| 1. Download official bhavcopies, backfill symbols Yahoo no longer serves | `python nse_delisted.py` | `official_raw/full/`, `raw_delisted/NSE.<SYM>.csv`, `raw_delisted/isin_map.csv` |
| 2. Derive exact scale factors (Yahoo price → as-traded price) | `python nse_factors.py` | `recon/scale_factors.csv`, `factor_outliers.csv`, `factor_check.csv` |
| 3. Validate rename chains (same ISIN, new symbol) | `python nse_stitch.py` | `raw_delisted/stitch_report.csv` |
| 4. Build the corporate-actions table | `python nse_corporate_actions.py` | `corporate_actions.csv`, `recon/merger_check.csv` |
| 5. Build as-traded output | `python eod_downloader.py --exchanges NSE --skip-download --price-basis raw --include-delisted --metastock-ascii --layout daily` | `daily_raw/`, `bhavcopy_raw/` (or `by_symbol_raw/`, `combined_raw/`) |

(`reconcile_nse.py` is an optional diagnostic that samples ~quarterly dates and writes `recon/implied_factors.csv`,
`suspicious.csv` and `missing_from_raw.csv`.)

**Price bases.** `--price-basis split` (default) is Yahoo's adjusted prices. `--price-basis raw`
applies `as_traded_price = yahoo_price × Factor` and `volume ÷ Factor` using `recon/scale_factors.csv`
(NSE only; other exchanges are written unchanged with a warning). Raw-basis output goes to separate
`*_raw` folders so it never overwrites the adjusted files. Against the official close, ~99.7% of
symbols match within 0.5% (the rest is four-decimal factor rounding and the odd bad Yahoo bar).
Dividend-adjusted (total-return) prices are **not** implemented.

**NYSE / NASDAQ (daily and hourly), Yahoo only.** No official bhavcopy exists, so `us_factors.py` builds
`<EX>/recon/scale_factors.csv` from Yahoo's split history (bonus/stock dividends are reported as splits),
after checking the cached prices around each split date to see whether that part of the cache is already
as-traded (verdicts in `recon/split_evidence.csv`). Then build with `--price-basis raw`:

    python us_factors.py --exchanges NYSE NASDAQ                              # daily factors
    python us_factors.py --root market_data_hourly --interval hourly --exchanges NYSE NASDAQ
    python eod_downloader.py --skip-download --price-basis raw --exchanges NYSE NASDAQ
    python hourly_downloader.py --skip-download --price-basis raw --exchanges NYSE NASDAQ   # -> <EX>/as_traded/

**One-command refresh:** `python update_us.py` (download, re-derive factors, rebuild as-traded; daily and hourly; options `--exchanges`, `--daily-only`, `--hourly-only`, `--skip-download`, `--include-delisted`).

Delisted tickers: Yahoo has no delisted directory, so give `us_factors.py --delisted-file symbols.txt`
(a list you supply); those Yahoo still serves are downloaded into the raw cache and registered in
`<EX>/delisted_tickers.csv`, and `--include-delisted` adds them to the build. Spin-offs/mergers and
dividends are not adjusted. Re-run `us_factors.py` after new splits.

Free delisted list: `python us_delisted_list.py` (key via `--api-key` or env `ALPHAVANTAGE_API_KEY`) writes
`<EX>/delisted_symbols.csv` (NASDAQ 4,198, NYSE 3,314 symbols as of 2026-10-17). Then
`python us_factors.py --exchanges NYSE NASDAQ --delisted-file "market_data_daily/{ex}/delisted_symbols.csv"`
and `python update_us.py --skip-download --include-delisted`.
**Coverage caveat:** Yahoo serves history for only a small share of them (NASDAQ: 276 of 4,198, about 7%;
NYSE: 210 of 3,314, about 6%; the rest are empty markers, which builds skip). `delisted_tickers.csv` lists every symbol tried, not just those with data.
Alpha Vantage covers roughly 2010 onward. Complete delisted history needs a paid source (Norgate, Sharadar, CRSP).

**Delisted names.** `--include-delisted` (needs `--price-basis raw`) adds `raw_delisted/` to the build.
These are as-traded official prices, which is why they only mix with the raw basis. Symbols that kept
the same ISIN under a new symbol (e.g. L&TFH → LTF) are joined into one series when
`stitch_report.csv` says `Stitch=Y`; on overlapping dates NSE's official rows win over Yahoo's.
The 29 rejected chains (very long gaps, i.e. relistings or reused ISINs) stay separate.

**Corporate actions** (`corporate_actions.csv`: `Date,Symbol,Type,Ratio,NewSymbol,Source,Note`):
`split_bonus` and `adjustment` (price steps that aren't clean split ratios: rights issues, demergers or
bad data — review these) come from the factor steps; `rename` from the stitched chains; `delist` is a
symbol's last traded day; `merger` comes from the hand-maintained **`nse_mergers.csv`**
(`Old,New,Ratio,Note`; Ratio = new shares per old share; leave blank to use the ratio implied by prices).
Mergers are events, not spliced series, because old and new usually trade side by side until the merger date.
`recon/merger_check.csv` compares each declared ratio with the price-implied one and marks `REVIEW`
when they differ by more than 5%. The seeded ratios were entered from memory of the scheme terms and are
**not verified against the official scheme documents**; ALBK→INDIANB and GRUH→BANDHANBNK currently show `REVIEW`.

**Keep it fresh.** Factors only reflect splits up to the last time `nse_factors.py` ran, so re-run steps 1–5
after each daily update (step 1 only downloads the new days).

**Same symbol on two exchanges.** Tickers are now grouped by the directory's `Exchange` column, so symbols listed
on both NSE and NYSE (INFY, HAL, IEX …) are each downloaded under the right exchange. Before this fix the NYSE
listing shadowed the NSE one and the NSE cache silently went stale.

**Still missing:** BSE/US as-traded prices and corporate actions (no free official source for US delisted
data), mergers where the ISIN changed beyond those in `nse_mergers.csv`, and total-return prices.
