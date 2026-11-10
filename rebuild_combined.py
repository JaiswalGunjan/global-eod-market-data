"""
rebuild_combined.py
--------------------
Rebuilds <EXCHANGE>/combined/all_data.csv from whatever is already cached
in <EXCHANGE>/raw/ under market_data_daily/ -- for every exchange folder
found, or just --exchange -- with no network access, no re-download.

Reimplements build_combined_csv()'s per-file loop locally (rather than
calling it as a black box) purely so progress/ETA can be reported as each
raw file is read -- the final formatting (symbol prefixing, column order,
Metastock ASCII vs headered layout) still delegates to your existing
eod_downloader.py helpers, so output is identical to the original
function's.

Usage:
    python rebuild_combined.py                      # every exchange folder under --root
    python rebuild_combined.py --exchange NSE
    python rebuild_combined.py --start 2015-01-01 --end 2026-09-26
    python rebuild_combined.py --metastock-ascii
    python rebuild_combined.py --log-every 50
"""

import argparse
import datetime as dt
import logging
import time
from pathlib import Path

import pandas as pd

import eod_downloader as core  # your existing module, must be importable (same folder)


def _format_eta(seconds: float) -> str:
    """Human-readable duration, e.g. '2m 14s' or '1h 03m'."""
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def rebuild_combined(raw_dir: Path, out_path: Path, start: str, end: str,
                      metastock_ascii: bool, log_every: int, log: logging.Logger):
    start_dt = pd.Timestamp(start)
    end_dt = pd.Timestamp(end)

    raw_files = core._select_raw_files(raw_dir, tickers=None, exchange_map=None)
    total = len(raw_files)
    if total == 0:
        log.warning(f"No cached files found in {raw_dir.resolve()} -- nothing to combine.")
        return

    log.info(f"Combining {total:,} cached ticker file(s) from {raw_dir.resolve()}")

    frames = []
    skipped = 0
    t_start = time.time()

    for i, f in enumerate(raw_files, start=1):
        try:
            # only an empty field is missing: tickers NA, NAN, INF, TRUE ... stay text (Alpaca export has them)
            df = pd.read_csv(f, parse_dates=["Date"], dtype={"Ticker": str}, keep_default_na=False, na_values=[""])
        except Exception as e:
            log.warning(f"Skipping unreadable file {f.name}: {e}")
            skipped += 1
            continue

        if df.empty:
            skipped += 1
        else:
            if "Ticker" not in df.columns:
                df["Ticker"] = core._bare_ticker_from_filename(f.stem)
            df = df[(df["Date"] >= start_dt) & (df["Date"] <= end_dt)]
            if df.empty:
                skipped += 1
            else:
                frames.append(df)

        if i % log_every == 0 or i == total:
            elapsed = time.time() - t_start
            avg_per_file = elapsed / i
            remaining = (total - i) * avg_per_file
            pct = (i / total) * 100
            log.info(
                f"[{i:,}/{total:,}] {pct:5.1f}% | "
                f"elapsed {_format_eta(elapsed)} | "
                f"ETA {_format_eta(remaining)} | "
                f"~{1/avg_per_file:,.0f} files/sec"
            )

    read_elapsed = time.time() - t_start
    log.info(f"Finished reading {total:,} files in {_format_eta(read_elapsed)} "
              f"({skipped:,} skipped: empty, unreadable, or out of date range)")

    if not frames:
        log.warning("No data found within the given date range -- nothing to write.")
        return

    log.info("Concatenating and formatting output...")
    t_concat = time.time()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["Open", "High", "Low", "Close"])

    sort_by = ("Symbol", "Date") if metastock_ascii else ("Date", "Symbol")
    out = core._format_output(combined, metastock_ascii, sort_by=sort_by)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    core._write_csv(out, out_path, metastock_ascii)

    log.info(f"Wrote combined CSV in {_format_eta(time.time() - t_concat)}: "
              f"{len(out):,} rows, {combined['Ticker'].nunique():,} symbols -> {out_path.resolve()}")
    log.info(f"Total time: {_format_eta(time.time() - t_start)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="./market_data_daily",
                         help="Daily data root holding one <EXCHANGE>/raw/ folder per exchange")
    parser.add_argument("--exchange", default=None,
                         help="Only rebuild this exchange (e.g. NSE). Default: every exchange "
                              "folder under --root that has a raw/ cache")
    parser.add_argument("--raw-dir", default=None,
                         help="Explicit raw/ folder (overrides --root/--exchange; needs --out)")
    parser.add_argument("--out", default=None,
                         help="Output path for the combined CSV when --raw-dir is given")
    parser.add_argument("--start", default="2000-01-01",
                         help="Earliest date to include (YYYY-MM-DD). Wide by default "
                              "so nothing cached gets filtered out unintentionally.")
    parser.add_argument("--end", default=None,
                         help="Latest date to include (YYYY-MM-DD). Default: today.")
    parser.add_argument("--metastock-ascii", action=argparse.BooleanOptionalAction, default=True,
                         help="MetaStock ASCII layout (<TICKER>,<PER>,<DTYYYYMMDD>,... header; the default). "
                              "--no-metastock-ascii writes the plain Symbol,Date,... layout")
    parser.add_argument("--log-every", type=int, default=25,
                         help="Log progress every N files (default: 25). Lower for smaller "
                              "batches, raise for very large raw/ directories.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("rebuild_combined")

    end = args.end or dt.date.today().strftime("%Y-%m-%d")
    if args.raw_dir:
        if not args.out:
            raise SystemExit("--raw-dir needs --out")
        jobs = [(Path(args.raw_dir), Path(args.out))]
    else:
        root = Path(args.root)
        ex_dirs = [root / args.exchange.upper()] if args.exchange else sorted(
            p for p in root.iterdir() if (p / "raw").is_dir())
        jobs = [(d / "raw", d / "combined" / "all_data.csv") for d in ex_dirs]
    if not jobs:
        raise SystemExit(f"No <EXCHANGE>/raw/ folders found under {Path(args.root).resolve()}")

    for raw_dir, out_path in jobs:
        if not raw_dir.exists():
            raise SystemExit(f"raw_dir not found: {raw_dir.resolve()}")
        log.info(f"=== {raw_dir.parent.name}: {raw_dir} -> {out_path} ===")
        rebuild_combined(
            raw_dir=raw_dir,
            out_path=out_path,
            start=args.start,
            end=end,
            metastock_ascii=args.metastock_ascii,
            log_every=max(1, args.log_every),
            log=log,
        )


if __name__ == "__main__":
    main()
