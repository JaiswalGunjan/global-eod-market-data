"""Audit (and optionally repair) NSE daily/hourly Yahoo caches against NSE's official bhavcopy.

Run from the EOD_Market_Data folder:
    python nse_data_audit.py                 # audit only -> market_data_daily/NSE/recon/audit_*.csv
    python nse_data_audit.py --repair        # also write corrected daily files to NSE/raw_fixed/

Needs: market_data_daily/NSE/yahoo_adjusted/, official_raw/full/ (from nse_delisted.py), recon/scale_factors.csv
(from nse_factors.py). Hourly checks use market_data_hourly/NSE/raw_1h/ if present.

Daily checks (Yahoo raw vs official, official converted to Yahoo's adjusted basis: price / Factor, volume * Factor):
  placeholder   Yahoo bar with Volume 0 and O=H=L=C on a real trading day (Yahoo had no data; e.g. 2025-03-18)
  holiday_bar   Yahoo bar on a date NSE published no bhavcopy for (empty marker file = holiday)
  no_official_file  Yahoo bar on a date with no file at all in official_raw/full (fetch it; not repaired)
  missing_day   official trading day where the symbol traded but Yahoo has no row
  ohlc_invalid  High < max(Open, Close) or Low > min(Open, Close)
  close_diff    |Yahoo close / official close - 1| > --tol (snapshot taken before the closing session, bad bar)
  ohl_diff      same test on Open / High / Low
  volume_diff   Yahoo volume differs from official by more than 10%
Hourly checks:
  hourly_missing_bar   day with fewer than 7 bars (09:15 ... 15:15), excluding known short sessions
  hourly_zero_volume   bar with Volume 0 (Yahoo drops the 09:15 bar's volume on most days)
  hourly_vs_daily      hourly-aggregated close differs from the daily close by > --tol (after adjustment basis)

--repair writes NSE/raw_fixed/NSE.<SYM>.csv (same columns as yahoo_adjusted/): holiday bars dropped, placeholder /
mismatching / missing days replaced by the official bar converted to Yahoo's basis. yahoo_adjusted/ is never modified.
"""
import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger("nse_audit")
PRICE = ["Open", "High", "Low", "Close"]
HOURS = ["09:15", "10:15", "11:15", "12:15", "13:15", "14:15", "15:15"]


def load_official(cache: Path):
    """Returns (bars, holidays): holidays are dates whose cache file is an empty marker (no bhavcopy published)."""
    frames, holidays = [], set()
    for p in sorted(cache.glob("*.csv")):
        if p.stat().st_size <= 60:
            holidays.add(f"{p.stem[:4]}-{p.stem[4:6]}-{p.stem[6:8]}")
            continue
        d = pd.read_csv(p, dtype={"Symbol": str, "Series": str})
        frames.append(d)
    off = pd.concat(frames, ignore_index=True)
    off["Date"] = off.Date.astype(str).str[:10]
    off["_eq"] = off.Series.eq("EQ")
    off = off.sort_values(["Symbol", "Date", "_eq"]).drop_duplicates(["Symbol", "Date"], keep="last")
    return off[off.Close > 0].drop(columns="_eq"), holidays


def factor_series(sf: pd.DataFrame, sym: str, dates: pd.Series) -> pd.Series:
    """Factor for each date (as_traded = yahoo * Factor). Dates outside every segment take the nearest one."""
    g = sf[sf.Symbol == sym].sort_values("From")
    if g.empty:
        return pd.Series(1.0, index=dates.index)
    idx = np.searchsorted(g.From.values, dates.values, side="right") - 1
    return pd.Series(g.Factor.values[np.clip(idx, 0, len(g) - 1)], index=dates.index)


def audit_daily(sym, y, o, sf, cal, holidays, tol):
    issues = []
    y = y.copy()
    y["Date"] = y.Date.astype(str).str[:10]
    y = y.drop_duplicates("Date", keep="last")

    flat = (y.Volume == 0) & (y.Open == y.High) & (y.High == y.Low) & (y.Low == y.Close)
    in_cal = y.Date.isin(cal)
    for d in y.Date[y.Date.isin(holidays)]:
        issues.append((sym, d, "holiday_bar", ""))
    unknown = ~in_cal & ~y.Date.isin(holidays) & (y.Date >= cal.min()) & (y.Date <= cal.max())
    for d in y.Date[unknown]:
        issues.append((sym, d, "no_official_file", "download official_raw/full for this date"))
    for d in y.Date[flat & ~y.Date.isin(holidays)]:
        issues.append((sym, d, "placeholder", ""))
    bad = (y.High < y[["Open", "Close"]].max(axis=1) - 1e-6) | (y.Low > y[["Open", "Close"]].min(axis=1) + 1e-6)
    for d in y.Date[bad]:
        issues.append((sym, d, "ohlc_invalid", ""))

    if o is None or o.empty:
        return issues, y, None
    o = o.copy()
    f = factor_series(sf, sym, o.Date)
    for c in PRICE:
        o[c] = o[c] / f
    o["Volume"] = (o.Volume * f).round()

    first = y.Date.min()
    for d in sorted(set(o.Date[o.Date >= first]) - set(y.Date)):
        issues.append((sym, d, "missing_day", ""))

    j = y.merge(o[["Date"] + PRICE + ["Volume"]], on="Date", suffixes=("", "_off"))
    j = j[~((j.Volume == 0) & (j.Open == j.Close) & (j.High == j.Low))]
    cd = (j.Close / j.Close_off - 1).abs()
    for d, v in zip(j.Date[cd > tol], cd[cd > tol]):
        issues.append((sym, d, "close_diff", f"{v:.4f}"))
    for c in ["Open", "High", "Low"]:
        dd = (j[c] / j[c + "_off"] - 1).abs()
        for d, v in zip(j.Date[dd > tol], dd[dd > tol]):
            issues.append((sym, d, "ohl_diff", f"{c}:{v:.4f}"))
    vd = (j.Volume / j.Volume_off.replace(0, np.nan) - 1).abs()
    for d, v in zip(j.Date[vd > 0.10], vd[vd > 0.10]):
        issues.append((sym, d, "volume_diff", f"{v:.3f}"))
    return issues, y, o


def repair(y, o, issues, holidays):
    """Drop holiday bars; replace bad / insert missing days with the official bar (Yahoo basis).
    A placeholder with no official bar for the symbol (it really did not trade) is kept as-is."""
    fix_dates = {d for _, d, kind, _ in issues if kind in ("placeholder", "missing_day", "close_diff", "ohl_diff")}
    out = y[~y.Date.isin(holidays)]
    if o is not None:
        repl = o[o.Date.isin(fix_dates)][["Date"] + PRICE + ["Volume"]].copy()
        repl["Volume"] = repl.Volume.astype("int64")
        repl["Ticker"] = y.Ticker.iloc[0] if "Ticker" in y and len(y) else ""
        out = pd.concat([out[~out.Date.isin(repl.Date)], repl], ignore_index=True)
    return out.sort_values("Date")[y.columns]


def audit_hourly(sym, h, daily, sf, hsf, tol, short_days):
    issues = []
    h = h.copy()
    h["dt"] = pd.to_datetime(h.Datetime)
    h["D"] = h.dt.dt.strftime("%Y-%m-%d")
    h["T"] = h.dt.dt.strftime("%H:%M")
    for d, g in h.groupby("D"):
        if d in short_days:
            continue
        miss = sorted(set(HOURS) - set(g["T"]))
        if miss:
            issues.append((sym, d, "hourly_missing_bar", " ".join(miss)))
    for dt, t in zip(h.Datetime[h.Volume == 0], h["T"][h.Volume == 0]):
        issues.append((sym, str(dt)[:10], "hourly_zero_volume", t))
    if daily is not None and not daily.empty:
        c = h.sort_values("dt").groupby("D").Close.last()
        # hourly as-traded via the hourly factor file (Yahoo's 1h feed is sometimes only partly adjusted)
        c_at = c * factor_series(hsf, sym, pd.Series(c.index, index=c.index))
        d = daily.assign(Date=daily.Date.astype(str).str[:10]).set_index("Date").Close
        # Yahoo daily is fully adjusted; hourly is usually as-traded -> compare on as-traded basis
        f = factor_series(sf, sym, pd.Series(d.index, index=d.index))
        d_as_traded = d * f
        j = pd.concat([c.rename("h"), c_at.rename("hat"), d.rename("d"), d_as_traded.rename("dt")],
                      axis=1, join="inner")
        best = np.minimum((j.h / j.d - 1).abs(), (j.hat / j.dt - 1).abs())
        for day, v in zip(j.index[best > tol], best[best > tol]):
            issues.append((sym, day, "hourly_vs_daily", f"{v:.4f}"))
    return issues


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="market_data_daily")
    ap.add_argument("--hourly-root", default="market_data_hourly")
    ap.add_argument("--tol", type=float, default=0.005, help="price tolerance (default 0.5%%)")
    ap.add_argument("--symbols", nargs="*", help="limit to these symbols")
    ap.add_argument("--repair", action="store_true", help="write corrected daily files to NSE/raw_fixed/")
    ap.add_argument("--short-days", nargs="*", default=["2024-11-01", "2025-02-01", "2025-10-21"],
                    help="known short sessions (muhurat, special Saturdays) skipped by the hourly bar-count check")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    nse = Path(args.root) / "NSE"
    out = nse / "recon"
    out.mkdir(exist_ok=True)
    log.info("Loading official bhavcopies ...")
    off, holidays = load_official(nse / "official_raw" / "full")
    cal = pd.Series(sorted(off.Date.unique()))
    by_sym = dict(tuple(off.groupby("Symbol")))
    sf_path = out / "scale_factors.csv"
    sf = pd.read_csv(sf_path) if sf_path.exists() else pd.DataFrame(columns=["Symbol", "From", "To", "Factor"])
    log.info("%d official days (%s .. %s), %d holiday markers, %d symbols",
             len(cal), cal.iloc[0], cal.iloc[-1], len(holidays), len(by_sym))

    files = sorted((nse / "yahoo_adjusted").glob("NSE.*.csv"))
    if args.symbols:
        files = [p for p in files if p.stem.split(".", 1)[1] in set(args.symbols)]
    fixed_dir = nse / "raw_fixed"
    if args.repair:
        fixed_dir.mkdir(exist_ok=True)

    all_issues, dailies = [], {}
    for i, p in enumerate(files, 1):
        sym = p.stem.split(".", 1)[1]
        if p.stat().st_size < 30 or sym.startswith("^"):
            continue
        y = pd.read_csv(p)
        if y.empty:
            continue
        iss, y_clean, o = audit_daily(sym, y, by_sym.get(sym), sf, cal, holidays, args.tol)
        all_issues += iss
        dailies[sym] = y
        if args.repair:
            repair(y_clean, o, iss, holidays).to_csv(fixed_dir / p.name, index=False)
        if i % 250 == 0:
            log.info("daily %d/%d", i, len(files))

    hdir = Path(args.hourly_root) / "NSE" / "raw_1h"
    hsf_path = Path(args.hourly_root) / "NSE" / "recon" / "scale_factors.csv"
    hsf = pd.read_csv(hsf_path) if hsf_path.exists() else sf.iloc[0:0]
    if hdir.exists():
        hfiles = sorted(hdir.glob("NSE.*.csv"))
        if args.symbols:
            hfiles = [p for p in hfiles if p.stem.split(".", 1)[1] in set(args.symbols)]
        for i, p in enumerate(hfiles, 1):
            sym = p.stem.split(".", 1)[1]
            if p.stat().st_size < 30:
                continue
            h = pd.read_csv(p)
            if not h.empty:
                all_issues += audit_hourly(sym, h, dailies.get(sym), sf, hsf, args.tol, set(args.short_days))
            if i % 250 == 0:
                log.info("hourly %d/%d", i, len(hfiles))

    rep = pd.DataFrame(all_issues, columns=["Symbol", "Date", "Issue", "Detail"])
    rep.to_csv(out / "audit_issues.csv", index=False)
    summ = rep.groupby("Issue").agg(rows=("Symbol", "size"), symbols=("Symbol", "nunique")).reset_index()
    summ.to_csv(out / "audit_summary.csv", index=False)
    bydate = (rep[rep.Issue.isin(["placeholder", "holiday_bar", "close_diff"])]
              .groupby(["Issue", "Date"]).Symbol.nunique().rename("symbols").reset_index()
              .sort_values("symbols", ascending=False))
    bydate.to_csv(out / "audit_by_date.csv", index=False)
    log.info("\n%s\n\nWorst dates:\n%s", summ.to_string(index=False), bydate.head(15).to_string(index=False))
    log.info("outputs: %s/audit_issues.csv, audit_summary.csv, audit_by_date.csv%s", out,
             f"; repaired files in {fixed_dir}" if args.repair else "")


if __name__ == "__main__":
    main()
