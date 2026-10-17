"""Build the NSE corporate-actions table that backtests need alongside the as-traded prices.

Writes <root>/NSE/corporate_actions.csv  (Date,Symbol,Type,Ratio,NewSymbol,Source,Note), one row per event:
  split_bonus  shares multiply by Ratio (price / Ratio); derived from scale_factors.csv factor steps
  adjustment   price step that is not a clean split ratio (rights issue, demerger, bad data) -- review
  rename       same security, new symbol (same ISIN, stitched by nse_stitch.py); Ratio 1
  merger       Old holders receive Ratio shares of NewSymbol per share held; from nse_mergers.csv
  delist       last traded day (Date) of a symbol that is not a rename or merger; Note has the last close
and <root>/NSE/recon/merger_check.csv comparing each declared merger ratio with the ratio implied by prices.

Merger input (nse_mergers.csv, hand-maintained): Old,New,Ratio,Note. Ratio blank -> implied by prices. Mergers are
kept as events, NOT spliced into one series: Old and New usually trade side by side until the merger date.
"""
import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from nse_stitch import load_official_closes
from reconcile_nse import is_clean_ratio

log = logging.getLogger("market_dl")
REVIEW_PCT = 5.0          # declared vs implied merger ratio tolerance
SHORT_SEGMENT_DAYS = 5


def next_trading_day(dates, after):
    i = np.searchsorted(dates, after, side="right")
    return str(dates[i]) if i < len(dates) else ""


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="market_data_daily")
    ap.add_argument("--mergers", default="nse_mergers.csv")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    nse = Path(args.root) / "NSE"
    dl, recon = nse / "raw_delisted", nse / "recon"
    log.info("Loading official closes ...")
    closes = load_official_closes(nse / "official_cache" / "full")
    all_dates = np.array(sorted({d for s in closes.values() for d in s.index}))
    events = []

    # 1. splits / bonuses / other price steps, from the factor segments
    seg = pd.read_csv(recon / "scale_factors.csv").sort_values(["Symbol", "From"])
    for sym, g in seg.groupby("Symbol"):
        r = list(g.itertuples())
        for a, b in zip(r, r[1:]):
            ratio = a.Factor / b.Factor
            if abs(ratio - 1) < 0.005:
                continue
            clean = is_clean_ratio(ratio)
            note = f"factor {a.Factor:g} -> {b.Factor:g}"
            if min(a.Days, b.Days) < SHORT_SEGMENT_DAYS:
                note += f"; short segment ({min(a.Days, b.Days)} days), maybe noise"
            events.append((b.From, sym, "split_bonus" if clean else "adjustment", round(ratio, 4), "",
                           "derived: official/Yahoo close ratio", note))

    # 2. renames the stitcher approved
    st = pd.read_csv(dl / "stitch_report.csv", dtype=str).fillna("")
    for r in st[st.Stitch == "Y"].itertuples():
        events.append((r.NewFirstAfter, r.Old, "rename", 1, r.New, "same ISIN (nse_stitch.py)", ""))

    # 3. mergers (manual), validated against prices
    checks = []
    merged = set()
    mp = Path(args.mergers)
    if mp.exists():
        for r in pd.read_csv(mp, dtype=str).fillna("").itertuples():
            old, new = closes.get(r.Old), closes.get(r.New)
            if old is None or new is None:
                log.warning("merger %s -> %s: no official data for %s", r.Old, r.New, r.Old if old is None else r.New)
                continue
            common = old.index.intersection(new.index)
            if len(common) == 0:
                log.warning("merger %s -> %s: the two symbols never trade on the same day", r.Old, r.New)
                continue
            last = common[-5:]
            implied = float((old[last] / new[last]).median())
            declared = float(r.Ratio) if r.Ratio else None
            dev = None if declared is None else (declared / implied - 1) * 100
            if declared is None:
                ratio, src, status = implied, "implied from last closes", "IMPLIED"
            elif abs(dev) <= REVIEW_PCT:
                ratio, src, status = declared, "declared; price check ok", "OK"
            else:
                ratio, src, status = declared, f"declared; REVIEW (prices imply {implied:.4f})", "REVIEW"
            merged.add(r.Old)
            events.append((next_trading_day(all_dates, old.index.max()), r.Old, "merger", round(ratio, 4), r.New,
                           src, r.Note))
            checks.append((r.Old, r.New, declared, round(implied, 4), None if dev is None else round(dev, 1),
                           status, common.max()))
    pd.DataFrame(checks, columns=["Old", "New", "Declared", "Implied", "DeviationPct", "Status", "LastCommonDate"]
                 ).to_csv(recon / "merger_check.csv", index=False)

    # 4. delistings: gone for good and not explained by a rename or merger
    isin = pd.read_csv(dl / "isin_map.csv", dtype=str).fillna("")
    renamed = set(st[st.Stitch == "Y"].Old)
    cutoff = str(pd.Timestamp(str(all_dates[-1])) - pd.Timedelta(days=30))[:10]
    for r in isin.itertuples():
        if r.Symbol in renamed or r.Symbol in merged or r.Last >= cutoff:
            continue
        c = closes[r.Symbol].get(r.Last, float("nan")) if r.Symbol in closes else float("nan")
        note = f"last close {c:g}"
        if r.Successor:
            note += f"; same ISIN reappears as {r.Successor} after a long gap (not stitched)"
        events.append((r.Last, r.Symbol, "delist", "", "", "official bhavcopy: last traded day", note))

    out = pd.DataFrame(events, columns=["Date", "Symbol", "Type", "Ratio", "NewSymbol", "Source", "Note"])
    out = out.sort_values(["Date", "Symbol", "Type"]).reset_index(drop=True)
    out.to_csv(nse / "corporate_actions.csv", index=False)

    log.info("\n%s", out.Type.value_counts().to_string())
    if checks:
        log.info("\nmerger price check:\n%s", pd.DataFrame(checks, columns=[
            "Old", "New", "Declared", "Implied", "Dev%", "Status", "LastCommon"]).to_string(index=False))
    log.info("\nwrote %s (%d events)", nse / "corporate_actions.csv", len(out))


if __name__ == "__main__":
    main()
