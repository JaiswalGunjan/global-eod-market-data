# TODO

## NSE
- [x] Hourly as-traded: done via `nse_hourly_factors.py` (daily factor steps checked against the hourly cache). Re-run it after every NSE hourly download or `nse_factors.py` run.
- [ ] Hourly delisted: no source (Yahoo hourly only covers live symbols; NSE bhavcopies are daily). Probably not possible; confirm and document.

## NYSE / NASDAQ
- [ ] Delisted: needs a free Alpha Vantage key -> `us_delisted_list.py`, `us_factors.py --delisted-file`, `update_us.py --include-delisted` (daily and hourly).
- [ ] Verify the first full `update_us.py` run (all four daily/hourly steps) and spot-check factors against known splits.

## All exchanges
- [ ] Dividend / total-return basis; spin-offs and mergers (US).
- [ ] Split-factor as-traded support for LSE, TSX, ASX and other Yahoo exchanges.
- [ ] GUI controls for `--price-basis raw` and `--include-delisted`.
- [ ] Stale-cache quirk: `download_ticker()` returns "cached" when an incremental fetch comes back empty.
