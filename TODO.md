# TODO

## NSE
- [x] Hourly as-traded: done via `nse_hourly_factors.py` (daily factor steps checked against the hourly cache). Re-run it after every NSE hourly download or `nse_factors.py` run.
- [ ] Hourly delisted: no source (Yahoo hourly only covers live symbols; NSE bhavcopies are daily). Probably not possible; confirm and document.

## NYSE / NASDAQ
- [x] Delisted: done 2026-10-17 via Alpha Vantage list -> `us_factors.py --delisted-file` -> `update_us.py --skip-download --include-delisted`. Yahoo served only ~7% (NASDAQ 276 of 4,198); the rest needs a paid source.
- [x] Checked: NYSE yield 210 of 3,314 (~6%); `_load_raw_frames()` skips empty-marker files, so they don't affect builds.
- [x] Spot-checked daily factors/prices against known splits (2026-10-17): AAPL 4:1, TSLA 5:1+3:1 (x15), NVDA 4:1+10:1 (x40), AMZN/GOOGL 20:1, CMG 50:1, GE 1:8 reverse. As-traded closes match the real pre-split prices. Hourly factors not spot-checked.

## All exchanges
- [ ] Dividend / total-return basis.
- [x] Spin-offs / renames (US): handled in the Alpaca source (`alpaca_us.py`, default since 2026-11-10); still not in the Yahoo US pipeline.
- [ ] Split-factor as-traded support for LSE, TSX, ASX and other Yahoo exchanges.
- [ ] GUI controls for `--price-basis raw` and `--include-delisted`.
- [x] Stale-cache quirk: now reported as "stale".
- [x] Cached history on an old split basis (Yahoo): `download_ticker()` checks an overlap bar and re-downloads the full history (2026-11-10).
- [x] GUI source choice (Alpaca / NSE bhavcopy boxes in section 1, on by default; 2026-11-10). MetaStock ASCII is the default everywhere.
- [ ] Rebuild NASDAQ/NYSE `daily/` and `daily_raw/` over their full range: they are still in the plain layout, and a partial-range run would mix layouts.
- [ ] `alpaca_us.py`: review `skipped` rows in `<EX>/alpaca_updates.csv` (CDLR 2026-11-05: new security under the ticker, x0.29 price step, likely an ADS ratio change); `alpaca_exits.csv` and `alpaca_1h/` are not extended.
- [ ] Hourly Yahoo caches (`download_ticker_hourly`) still append without the overlap/basis check.
