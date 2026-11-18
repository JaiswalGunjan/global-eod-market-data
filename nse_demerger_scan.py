"""Scan NSE official as-traded closes for one-day parent drops that look like demergers (no ratios, no child symbols).

A candidate is a permanent drop of at least --min-drop percent that no split/bonus, merger or rename in
<root>/NSE/corporate_actions.csv explains, and that is not already in nse_demergers.csv. Writes
<root>/NSE/recon/demerger_candidates.csv with the drop, the level it settled at, and any Yahoo "adjustment" step on
the same date (a hint that Yahoo treated it as a corporate action), and Flags for the ones that are probably
splits, circuit-limit hits or funds. The window defaults to before the Tradebrains table
starts (2018-10-25).
"""
import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from nse_stitch import load_official_closes

log = logging.getLogger("market_dl")
EXPLAINED = {"split_bonus", "merger", "rename", "demerger"}
NEAR_DAYS = 3             # a split/merger this close to the drop explains it
HOLD_DAYS = 10            # the price must stay down this long (rules out crashes that recover)
HOLD_TOL = 0.10           # ... within this fraction of the day's close
MIN_PRICE = 10.0          # ignore penny stocks (tick-size jumps look like drops)
SPLIT_RATIOS = (1.25, 4 / 3, 1.5, 2, 2.5, 3, 4, 5, 6, 10, 20, 100)   # common split/bonus ratios (prev / close)
SPLIT_TOL = 0.015
CIRCUIT_PCTS = (20.0, 10.0)                # price-band limits: a drop of exactly this is a circuit hit, not a demerger
ETF_WORDS = ("NIFTY", "BEES", "ETF", "SENSEX", "GOLD", "LIQUID")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="market_data_daily")
    ap.add_argument("--start", default="2016-10-06")
    ap.add_argument("--end", default="2018-10-24")
    ap.add_argument("--min-drop", type=float, default=12.0)
    ap.add_argument("--demergers", default="nse_demergers.csv")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    nse = Path(args.root) / "NSE"
    closes = load_official_closes(nse / "official_raw" / "full")
    ca = pd.read_csv(nse / "corporate_actions.csv", dtype=str).fillna("")
    explained = ca[ca.Type.isin(EXPLAINED)]
    expl = {s: pd.to_datetime(g.Date).values for s, g in explained.groupby("Symbol")}
    # a merger/rename names the new symbol too
    for r in explained[explained.NewSymbol != ""].itertuples():
        expl[r.NewSymbol] = np.append(expl.get(r.NewSymbol, np.array([], dtype="datetime64[ns]")),
                                      np.datetime64(r.Date))
    adj = {s: pd.to_datetime(g.Date).values for s, g in ca[ca.Type == "adjustment"].groupby("Symbol")}
    known = set()
    if Path(args.demergers).exists():
        known = set(pd.read_csv(args.demergers, dtype=str).Parent)

    def near(arr, d):
        return arr is not None and len(arr) and bool((np.abs((arr - np.datetime64(d)).astype("timedelta64[D]")
                                                              .astype(int)) <= NEAR_DAYS).any())

    # market move per day, so a crash day (2016-11-15, 2018-02-02) does not make every stock a candidate
    allret = pd.concat({k: v[(v.index >= args.start) & (v.index <= args.end)].pct_change() for k, v in closes.items()
                        if not k.endswith("-RE")}, axis=1)
    market = allret.median(axis=1) * 100

    rows = []
    for sym, s in closes.items():
        s = s[(s.index >= args.start) & (s.index <= args.end)]
        if len(s) < HOLD_DAYS + 2 or sym.endswith("-RE") or sym in known:
            continue
        v = s.values
        ret = (v[1:] / v[:-1] - 1) * 100
        for i in np.where(ret <= -args.min_drop)[0]:
            d, prev = s.index[i + 1], v[i]
            if prev < MIN_PRICE or near(expl.get(sym), d) or ret[i] - market.get(d, 0.0) > -args.min_drop:
                continue
            after = v[i + 1:i + 2 + HOLD_DAYS]
            if len(after) < HOLD_DAYS or np.abs(after / v[i + 1] - 1).max() > HOLD_TOL:
                continue
            ratio = prev / v[i + 1]
            flags = []
            if any(abs(ratio / r - 1) <= SPLIT_TOL for r in SPLIT_RATIOS):
                flags.append("looks_like_split")
            if any(abs(-ret[i] - c) <= 0.5 for c in CIRCUIT_PCTS):
                flags.append("circuit_band")
            if any(w in sym for w in ETF_WORDS):
                flags.append("etf_or_fund")
            rows.append((sym, d, round(prev, 2), round(v[i + 1], 2), round(ret[i], 1),
                         "Y" if near(adj.get(sym), d) else "", ";".join(flags)))
    out = pd.DataFrame(rows, columns=["Symbol", "Date", "PrevClose", "Close", "DropPct", "YahooAdjusted", "Flags"])
    out = out.sort_values(["Flags", "DropPct"]).reset_index(drop=True)
    out.to_csv(nse / "recon" / "demerger_candidates.csv", index=False)
    log.info("%d candidates, %d unflagged -> %s", len(out), (out.Flags == "").sum(), nse / "recon" / "demerger_candidates.csv")


if __name__ == "__main__":
    main()
