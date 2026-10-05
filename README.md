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

## 0. GUI (recommended)

```bash
pip install yfinance pandas requests tkcalendar
python eod_gui.py
```

A window opens with:

1. **Market** — checkboxes for NASDAQ / NYSE / BSE / NSE (NASDAQ+NYSE
   checked by default; BSE/NSE off by default since they're India-specific)
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
3. **Date range** — a calendar picker for start/end (default: last 10 years →
   today), plus **Quick range** buttons: 1Y / 5Y / 10Y / YTD / Max. **If you're just seeing plain
   YYYY-MM-DD text boxes instead of a calendar dropdown, that means
   `tkcalendar` isn't installed** — run `pip install tkcalendar` and
   restart the app; the plain text fields still work fine in the meantime,
   they're just less convenient.
4. **CSV layout** — single combined file (default), one file per day, one
   per symbol, or all three, plus two checkboxes:
   - **Strict MetaStock ASCII format** — check this if you're actually
     importing into MetaStock (see [section 4](#4-about-the-metastock-format))
   - **Split into separate NASDAQ/ and NYSE/ folders** — each exchange gets
     its own `raw/` cache and its own `daily/`/`by_symbol/`/`combined/`
     output, instead of one shared set of folders with both mixed together
5. **Save location** — any folder, via Browse; **Open folder** jumps there
   once files exist

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
downloaded stays cached under `raw/`, so re-running later (GUI or CLI)
picks up where you left off.

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

NASDAQ and NYSE downloaded and combined separately, each into its own folder
(`market_data/NASDAQ/...` and `market_data/NYSE/...`, each with its own
`raw/` cache and its own single combined file) -- works the same way for
any combination of exchanges, e.g. `--exchanges NSE BSE --split-by-exchange`:

```bash
python eod_downloader.py --split-by-exchange --layout combined
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
  stitched into the usual `raw/BSE.<TICKER>.csv` files, so every layout and the
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
market_data/
├── tickers.csv              # the ticker list used
├── raw/                     # one cache file per ticker (download cache, keep this)
│   ├── NASDAQ.AAPL.csv          # named with the exchange prefix when known
│   ├── NASDAQ.MSFT.csv
│   ├── NYSE.GE.csv
│   └── ...
├── daily/                   # one file per trading day
│   ├── 20160115.csv
│   ├── 20160119.csv
│   └── ...
├── by_symbol/                # optional, see "Metastock format" below
└── combined/
    └── all_data.csv          # optional: every ticker + every date in ONE file
```

Or, with `--split-by-exchange`:

```
market_data/
├── NASDAQ/
│   ├── raw/{NASDAQ.AAPL.csv, NASDAQ.MSFT.csv, ...}
│   ├── daily/, by_symbol/  (if requested)
│   └── combined/all_data.csv   # NASDAQ tickers only
└── NYSE/
    ├── raw/{NYSE.GE.csv, NYSE.JPM.csv, ...}
    ├── daily/, by_symbol/  (if requested)
    └── combined/all_data.csv   # NYSE tickers only
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
but worth knowing before you try to open it in Excel. `--split-by-exchange`
naturally cuts this in half (one combined file per exchange instead of one
file with both mixed together), which also happens to be the shape you'll
want if MetaStock ends up needing NASDAQ and NYSE imported as separate
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
tool (`--exchanges` values, GUI checkboxes, `--split-by-exchange` folder
names):

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

This only affects the *output* CSVs — the `raw/` download cache always uses
the bare ticker internally as its canonical form (Yahoo's required
`.NS`/`.BO` suffix is added only for the actual API request, not stored
anywhere else).

## 4. About the Metastock format

By default this tool writes a friendly CSV layout (`Symbol,Date,Open,High,
Low,Close,Volume`, with a header row) that's easy to open in Excel/pandas
but **isn't itself a format MetaStock recognizes**. To actually import into
MetaStock, add `--metastock-ascii` (CLI) or check "Strict MetaStock ASCII
format" (GUI). That switches every layout to the specific convention EOD
data vendors (e.g. EODData) use for direct MetaStock import — confirmed via
their published format docs and MetaStock's own community forum:

```
Symbol,Period,Date,Open,High,Low,Close,Volume
AAPL,D,01/15/2016,24.10,24.55,23.90,24.30,185000000
GE,D,01/15/2016,28.40,28.90,28.10,28.75,41000000
```

- **No header row** — the file starts directly with data
- **Symbol** is the bare ticker here (`AAPL`, not `NASDAQ.AAPL`) — deliberately
  *not* exchange-prefixed like the default layout is. MetaStock needs this
  to match your existing security codes; a prefixed symbol would just look
  like an unrelated, brand-new ticker to it.
- **Period** column is always `D` (daily)
- **Date** defaults to `MM/DD/YYYY` in this mode (override with
  `--date-format` if your version of MetaStock wants something else)
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
land in `by_symbol/<SYMBOL>.csv` (e.g. `NASDAQ.AAPL.csv`), named after the same
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
- **Survivorship bias**: the ticker list comes from today's live NASDAQ/NYSE
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
- **Every run writes a log file** under `<output>/logs/run_<timestamp>.log`
  (or `<output>/<EXCHANGE>/logs/...` with `--split-by-exchange`) — the full
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
