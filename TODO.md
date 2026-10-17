# TODO

## NSE
- [x] Hourly as-traded: done via `nse_hourly_factors.py` (daily factor steps checked against the hourly cache). Re-run it after every NSE hourly download or `nse_factors.py` run.
- [ ] Hourly delisted: no source (Yahoo hourly only covers live symbols; NSE bhavcopies are daily). Probably not possible; confirm and document.

## NYSE / NASDAQ
- [x] Delisted: done 2026-10-17 via Alpha Vantage list -> `us_factors.py --delisted-file` -> `update_us.py --skip-download --include-delisted`. Yahoo served only ~7% (NASDAQ 276 of 4,198); the rest needs a paid source.
- [x] Checked: NYSE yield 210 of 3,314 (~6%); `_load_raw_frames()` skips empty-marker files, so they don't affect builds.
- [x] Spot-checked daily factors/prices against known splits (2026-10-17): AAPL 4:1, TSLA 5:1+3:1 (x15), NVDA 4:1+10:1 (x40), AMZN/GOOGL 20:1, CMG 50:1, GE 1:8 reverse. As-traded closes match the real pre-split prices. Hourly factors not spot-checked.

## All exchanges
- [ ] Dividend / total-return basis; spin-offs and mergers (US).
- [ ] Split-factor as-traded support for LSE, TSX, ASX and other Yahoo exchanges.
- [ ] GUI controls for `--price-basis raw` and `--include-delisted`.
- [ ] Stale-cache quirk: `download_ticker()` returns "cached" when an incremental fetch comes back empty.
