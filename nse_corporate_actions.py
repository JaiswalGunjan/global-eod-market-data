"""Build the NSE corporate-actions table that backtests need alongside the as-traded prices.

Writes <root>/NSE/corporate_actions.csv  (Date,Symbol,Type,Ratio,NewSymbol,Source,Note), one row per event:
  split_bonus  shares multiply by Ratio (price / Ratio); derived from scale_factors.csv factor steps
  adjustment   price step that is not a clean split ratio (rights issue, demerger, bad data) -- review
  rename       same security, new symbol (same ISIN, stitched by nse_stitch.py); Ratio 1
  demerger     Parent holders receive Ratio shares of Child per share held and the parent price drops on the ex-date;
               from nse_demergers.csv (the parent's adjustment step on that date, if any, is folded into this event)
  merger       Old holders receive Ratio shares of NewSymbol per share held; from nse_mergers.csv
  delist       last traded day (Date) of a symbol that is not a rename or merger; Note has the last close
               (plus NSE's delisting route/date/exit price when nse_exit_info.csv knows the symbol)
  rights_entitlement_end  a rights-entitlement ticker (e.g. XYZ-RE) ceasing to trade; not a company exit
and <root>/NSE/recon/merger_check.csv comparing each declared merger ratio with the ratio implied by prices, and
<root>/NSE/recon/demerger_check.csv comparing each declared demerger with the parent's price drop.

Demerger input (nse_demergers.csv, hand-maintained): Date,Parent,Child,Ratio,Note,Verified (text = ratio checked
against a company notice/filing; the price check is then informational only). Date is the ex-date; the check looks
+-DEMERGER_WINDOW trading days for the parent's largest one-day drop and compares it with the drop the Ratio predicts
(Ratio x Child's first close / Parent's previous close). Child shares trade only after listing, often weeks later.

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
DEMERGER_WINDOW = 25      # trading days either side of the declared date (the source's Date is often not the ex-date)
DEMERGER_LOOSE_PTS = 20.0  # a larger gap is still PLAUSIBLE if it is under DEMERGER_LOOSE_FRAC of the drop (child price drifts
DEMERGER_LOOSE_FRAC = 0.35  # between the ex-date and the first close used as its value)
DEMERGER_MIN_DROP = 2.0   # a childless demerger needs a parent drop at least this big (percent) to count as seen
DEMERGER_TOL_PTS = 5.0    # allowed gap between actual and predicted parent drop, percentage points


def next_trading_day(dates, after):
    i = np.searchsorted(dates, after, side="right")
    return str(dates[i]) if i < len(dates) else ""


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="market_data_daily")
    ap.add_argument("--mergers", default="nse_mergers.csv")
    ap.add_argument("--demergers", default="nse_demergers.csv")
    ap.add_argument("--exit-info", default="nse_exit_info.csv",
                    help="from nse_import_workbook.py; annotates delist events, retypes rights entitlements")
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
        events.append((r.NewFirstAfter, r.Old, "rename", 1, r.New,
                       "ISIN changed, same issuer (nse_renames.csv)" if r.Why else "same ISIN (nse_stitch.py)",
                       f"close ratio across the join {r.Ratio}" if r.Why else ""))

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

    # 3b. demergers (manual), validated against the parent's price drop
    dchecks = []
    dp = Path(args.demergers)
    if dp.exists():
        for r in pd.read_csv(dp, dtype=str).fillna("").itertuples():
            par, kid = closes.get(r.Parent), closes.get(r.Child)
            if par is None:
                log.warning("demerger %s -> %s: no official data for %s", r.Parent, r.Child, r.Parent)
                continue
            if r.Child and kid is None:
                log.info("demerger %s -> %s: child has no NSE prices, judging by the parent's drop only", r.Parent, r.Child)
            ratio = float(r.Ratio)
            pos = int(np.searchsorted(par.index.values, r.Date))
            lo, hi = max(pos - DEMERGER_WINDOW, 1), min(pos + DEMERGER_WINDOW + 1, len(par))
            ret = (par.iloc[lo:hi].values / par.iloc[lo - 1:hi - 1].values - 1) * 100
            if len(ret) == 0:
                log.warning("demerger %s -> %s: %s is outside the data", r.Parent, r.Child, r.Date)
                continue
            j = lo + int(np.argmin(ret))
            seen_date, prev = par.index[j], par.iloc[j - 1]
            drop = float(ret.min())
            # the source's ratio direction is not reliable: test it both ways and keep the one the drop supports
            gaps = {}
            # child's price: the first close on/after the ex-date (an already-listed child) or its first-ever close
            kref = float(kid.iloc[int(np.searchsorted(kid.index.values, str(seen_date)))] if kid is not None
                         and len(kid) and np.searchsorted(kid.index.values, str(seen_date)) < len(kid) else float("nan"))
            if kid is not None and len(kid) and np.isfinite(kref):
                for cand in (ratio, 1 / ratio):
                    gaps[cand] = drop + cand * kref / prev * 100
            if not gaps:
                status, gap, exp = ("NO_CHILD" if drop <= -DEMERGER_MIN_DROP else "NO_DROP"), None, None
            else:
                best = min(gaps, key=lambda c: abs(gaps[c]))
                gap, exp = gaps[best], -best * kref / prev * 100
                if r.Verified:
                    status = "CONFIRMED"
                elif abs(gap) > DEMERGER_TOL_PTS:
                    status = ("PLAUSIBLE" if abs(gap) <= DEMERGER_LOOSE_PTS and abs(gap) <= DEMERGER_LOOSE_FRAC * abs(drop)
                              else "REVIEW")
                elif best == ratio:
                    status = "OK"
                else:
                    status, ratio = "INVERTED", best
            events.append((seen_date, r.Parent, "demerger", round(ratio, 6), r.Child,
                           "declared" if status in ("OK", "CONFIRMED") else f"declared; {status} (see recon/demerger_check.csv)",
                           r.Note))
            dchecks.append((r.Parent, r.Child, r.Date, seen_date, round(drop, 1),
                            None if exp is None else round(exp, 1), None if gap is None else round(gap, 1), status))
    dcols = ["Parent", "Child", "DeclaredDate", "BiggestDropDate", "DropPct", "ExpectedPct", "GapPts", "Status"]
    # several children demerged on the same day share one parent drop: judge them by the summed expectation
    dfc = pd.DataFrame(dchecks, columns=dcols)
    for _, g in dfc[dfc.ExpectedPct.notna()].groupby(["Parent", "BiggestDropDate"]):
        if len(g) > 1 and abs(g.DropPct.iloc[0] - g.ExpectedPct.sum()) <= DEMERGER_TOL_PTS:
            dfc.loc[g.index[g.Status.isin(["REVIEW", "PLAUSIBLE"])], "Status"] = "OK_GROUP"
    ok_group = {(r.Parent, r.Child) for r in dfc.itertuples() if r.Status == "OK_GROUP"}
    events = [e if not (e[2] == "demerger" and (e[1], e[4]) in ok_group)
              else e[:5] + ("declared; price check ok (children summed)",) + e[6:] for e in events]
    dfc.to_csv(recon / "demerger_check.csv", index=False)
    dchecks = [tuple(r) for r in dfc.itertuples(index=False)]
    # the parent's price step on the ex-date is the demerger itself, not a separate rights issue or bad data
    demerged = {(e[1], pd.Timestamp(e[0])) for e in events if e[2] == "demerger"}
    events = [e for e in events if not (e[2] == "adjustment" and any(
        e[1] == s and abs((pd.Timestamp(e[0]) - d).days) <= 2 * DEMERGER_WINDOW for s, d in demerged))]

    # 4. delistings: gone for good and not explained by a rename or merger
    isin = pd.read_csv(dl / "isin_map.csv", dtype=str).fillna("")
    exit_info = {}
    if Path(args.exit_info).exists():
        exit_info = {x.Symbol: x for x in pd.read_csv(args.exit_info, dtype=str).fillna("").itertuples()}
    renamed = set(st[st.Stitch == "Y"].Old)
    cutoff = str(pd.Timestamp(str(all_dates[-1])) - pd.Timedelta(days=30))[:10]
    for r in isin.itertuples():
        if r.Symbol in renamed or r.Symbol in merged or r.Last >= cutoff:
            continue
        c = closes[r.Symbol].get(r.Last, float("nan")) if r.Symbol in closes else float("nan")
        note = f"last close {c:g}"
        if r.Successor:
            note += f"; same ISIN reappears as {r.Successor} after a long gap (not stitched)"
        etype = "delist"
        x = exit_info.get(r.Symbol)
        if x is not None:
            if x.Category == "rights_entitlement":
                etype = "rights_entitlement_end"       # a rights-entitlement ticker expiring, not a company exit
            elif x.Category == "delisted":
                note += f"; NSE delisting via {x.Route} on {x.OfficialDate}"
                if x.ExitPrice:
                    note += f", exit price {x.ExitPrice}"
        events.append((r.Last, r.Symbol, etype, "", "", "official bhavcopy: last traded day", note))

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
