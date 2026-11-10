"""One-command refresh of NYSE / NASDAQ data, adjusted and as-traded, daily and hourly.

For each interval and exchange it runs, in order:
  1. download new bars + rebuild the adjusted output   (eod_downloader.py / hourly_downloader.py)
  2. re-derive split factors from the fresh caches     (us_factors.py)
  3. rebuild the as-traded output                      (--price-basis raw --skip-download)

  python update_us.py                                  # NYSE + NASDAQ, daily + hourly
  python update_us.py --exchanges NYSE --daily-only
  python update_us.py --skip-download                  # re-derive factors and rebuild from existing caches
  python update_us.py --include-delisted               # also build the symbols in <EX>/delisted_tickers.csv
"""
import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

log = logging.getLogger("market_dl")
HERE = Path(__file__).resolve().parent


def run(script: str, *args: str):
    cmd = [sys.executable, str(HERE / script), *args]
    log.info("$ " + " ".join(cmd[1:]))
    t0 = time.time()
    if subprocess.run(cmd, cwd=HERE).returncode != 0:
        sys.exit(f"{script} failed -- stopping (later steps would use stale or partial data)")
    log.info(f"  done in {time.time() - t0:.0f}s")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--exchanges", nargs="+", choices=["NYSE", "NASDAQ"], default=["NYSE", "NASDAQ"])
    ap.add_argument("--daily-root", default="market_data_daily")
    ap.add_argument("--hourly-root", default="market_data_hourly")
    ap.add_argument("--daily-only", action="store_true")
    ap.add_argument("--hourly-only", action="store_true")
    ap.add_argument("--skip-download", action="store_true", help="No network downloads of bars; reuse the raw caches")
    ap.add_argument("--include-delisted", action="store_true")
    ap.add_argument("--metastock-ascii", action=argparse.BooleanOptionalAction, default=True,
                    help="MetaStock ASCII output (default); --no-metastock-ascii for the plain layout")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.daily_only and args.hourly_only:
        ap.error("--daily-only and --hourly-only are mutually exclusive")

    ex = list(args.exchanges)
    ms = ["--metastock-ascii"] if args.metastock_ascii else ["--no-metastock-ascii"]
    dl = ["--include-delisted"] if args.include_delisted else []
    skip = ["--skip-download"] if args.skip_download else []

    if not args.hourly_only:
        log.info("=== daily ===")
        # this pipeline is built on the Yahoo raw/ caches (the default US source is Alpaca now)
        run("eod_downloader.py", "--exchanges", *ex, "--output", args.daily_root, "--layout", "daily",
            "--us-source", "yahoo", *skip, *ms)
        run("us_factors.py", "--root", args.daily_root, "--exchanges", *ex)
        run("eod_downloader.py", "--exchanges", *ex, "--output", args.daily_root, "--layout", "daily",
            "--skip-download", "--price-basis", "raw", *dl, *ms)
    if not args.daily_only:
        log.info("=== hourly ===")
        run("hourly_downloader.py", "--exchanges", *ex, "--output", args.hourly_root, *skip, *ms)
        run("us_factors.py", "--root", args.hourly_root, "--interval", "hourly", "--exchanges", *ex)
        run("hourly_downloader.py", "--exchanges", *ex, "--output", args.hourly_root, "--skip-download",
            "--price-basis", "raw", *dl, *ms)
    log.info("Update finished.")


if __name__ == "__main__":
    main()
