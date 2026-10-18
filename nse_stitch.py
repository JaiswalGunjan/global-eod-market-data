"""Decide which NSE rename/merger chains in raw_delisted/isin_map.csv are safe to stitch into one series.

A chain OLD -> NEW comes from both symbols carrying the same ISIN (nse_delisted.py). Before the builder
joins them (eod_downloader --price-basis raw --include-delisted), check the join looks like a rename:
  * the gap between OLD's last official day and NEW's first official day after it is small
  * the as-traded close does not jump by an implausible factor across the boundary
Writes <root>/NSE/raw_delisted/stitch_report.csv:
  Old,New,OldFirst,OldLast,NewFirstAfter,GapDays,OldLastClose,NewFirstClose,Ratio,Stitch,Why
The builder stitches only rows with Stitch == Y.
"""
import argparse
import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger("market_dl")
MAX_GAP_DAYS = 45
RATIO_RANGE = (0.6, 1.7)


def load_official_closes(cache: Path):
    """{symbol: Series(close, indexed by 'YYYY-MM-DD')} from official_cache/full/, EQ series preferred."""
    off = pd.concat([pd.read_csv(p, usecols=["Date", "Symbol", "Series", "Close"], dtype=str)
                     for p in sorted(cache.glob("*.csv")) if p.stat().st_size > 60], ignore_index=True)
    off["Close"] = pd.to_numeric(off.Close, errors="coerce")
    off["_eq"] = off.Series.eq("EQ")
    off = off.sort_values(["Symbol", "Date", "_eq"]).drop_duplicates(["Symbol", "Date"], keep="last")
    return {s: g.set_index("Date").Close for s, g in off.groupby("Symbol")}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="market_data_daily")
    ap.add_argument("--renames", default="nse_renames.csv",
                    help="Old,New pairs where the ISIN changed too (from nse_import_workbook.py); "
                         "checked like the ISIN chains")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    nse = Path(args.root) / "NSE"
    cache, dl = nse / "official_cache" / "full", nse / "raw_delisted"
    m = pd.read_csv(dl / "isin_map.csv", dtype=str).fillna("")
    chains = m[m.Successor != ""].copy()
    rp = Path(args.renames)
    if rp.exists():
        extra = pd.read_csv(rp, dtype=str).fillna("")
        extra = extra[~extra.Old.isin(chains.Symbol)]
        extra = extra.merge(m[["Symbol", "First", "Last"]], left_on="Old", right_on="Symbol")
        extra = extra.assign(Successor=extra.New, Why="ISIN change")[["Symbol", "First", "Last", "Successor", "Why"]]
        chains = pd.concat([chains.assign(Why=""), extra], ignore_index=True)
        log.info("%d extra rename(s) with a changed ISIN from %s", len(extra), rp)
    else:
        chains["Why"] = ""
    log.info("%d rename chains; loading official closes ...", len(chains))
    by_sym = load_official_closes(cache)

    rows = []
    for r in chains.itertuples():
        old, new = by_sym.get(r.Symbol), by_sym.get(r.Successor)
        if old is None or new is None:
            rows.append((r.Symbol, r.Successor, r.First, r.Last, "", "", "", "", "", "N", "no official data"))
            continue
        after = new[new.index > r.Last]
        if after.empty:
            rows.append((r.Symbol, r.Successor, r.First, r.Last, "", "", "", "", "", "N", "successor never trades after"))
            continue
        d1 = after.index[0]
        gap = (pd.Timestamp(d1) - pd.Timestamp(r.Last)).days
        c0, c1 = float(old.loc[r.Last]), float(after.iloc[0])
        ratio = c1 / c0 if c0 else float("nan")
        why = []
        if gap > MAX_GAP_DAYS:
            why.append(f"gap {gap}d")
        if not RATIO_RANGE[0] <= ratio <= RATIO_RANGE[1]:
            why.append(f"price jump x{ratio:.2f}")
        ok = not why
        if ok and r.Why:
            why = [r.Why]          # stitched; Why records the provenance (shown by nse_corporate_actions.py)
        rows.append((r.Symbol, r.Successor, r.First, r.Last, d1, gap, c0, c1, round(ratio, 3),
                     "Y" if ok else "N", "; ".join(why)))

    rep = pd.DataFrame(rows, columns=["Old", "New", "OldFirst", "OldLast", "NewFirstAfter", "GapDays",
                                      "OldLastClose", "NewFirstClose", "Ratio", "Stitch", "Why"])
    rep.to_csv(dl / "stitch_report.csv", index=False)
    log.info("stitch=Y: %d | stitch=N: %d", int((rep.Stitch == "Y").sum()), int((rep.Stitch == "N").sum()))
    log.info("report: %s", dl / "stitch_report.csv")


if __name__ == "__main__":
    main()
