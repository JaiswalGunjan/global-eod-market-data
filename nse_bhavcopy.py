"""NSE daily prices from NSE's official bhavcopies, back-adjusted for corporate actions (eod_downloader --nse-source
bhavcopy, the default). The rules are ported from TranscriptExtract's swingbt/nse_official.py; every input is read
from this repo.

update() runs three steps:
  1. download  new day files into <root>/NSE/official_raw/full/YYYYMMDD.csv (as traded; the cache nse_delisted.py
               fills too). Weekend special sessions (Muhurat, Budget day) are tried for the last WEEKEND_LOOKBACK days.
  2. actions   NSE's corporate-actions lists (main board and SME; the API behind nseindia.com's Corporate Actions page)
               by calendar quarter into <root>/NSE/nse_ca/*.json -> nse_ca/nse_corporate_actions.csv. Downloaded
               once and reused: nse_ca/checked.json remembers up to which ex-date each quarter was asked for, and
               nseindia.com is only asked again when the day files reach a newer trading day -- then just the
               new days (plus CA_OVERLAP_DAYS for late corrections) are fetched and merged into the cache. No new
               day file = no request. --refresh-actions fetches every quarter again.
  3. build     every symbol, delisted and merged ones included -> <root>/NSE/official_adjusted/NSE.<SYM>.csv
               (Date,Open,High,Low,Close,Volume,Ticker, the yahoo_adjusted/ layout), adjusted to today's share basis. Renames are
               joined under the latest symbol (TATAMOTORS -> TMPV). Logs in <root>/NSE/official_info/. The build is
               skipped when no input changed since the last one.

Adjustment (every earlier price x f, volume / f; the latest prices stay as traded):
  split / consolidation 'From Rs X To Rs Y'   f = Y / X
  bonus a:b (a new shares for b held)          f = b / (a + b)
  rights a:b @ premium P (price = FV + P)      f = TERP / previous close, TERP = (b x close + a x price) / (a + b);
                                               only when the rights price is below the previous close
  demerger                                     nse_demergers.csv (hand-kept): the parent's expected drop from
                                               recon/demerger_check.csv, else its actual ex-date drop; demergers and
                                               'Scheme of Arrangement' rows (drop >= SCHEME_MIN_DROP) in NSE's list that
                                               nse_demergers.csv lacks: the parent's actual ex-date drop
  dividend >= BIG_DIV of the previous close    f = 1 - dividend / previous close (a return of capital), only if the
                                               ex-date open shows at least half of that drop; smaller dividends are
                                               not adjusted
A split, bonus or consolidation (several on one ex-date combined into one f) is applied on the bar (ex-date -2..+3
sessions) whose close-to-close move is closest to f, and only if that move is nearer f than 1; a ratio with
f >= SMALL_F is applied on the ex-date even without a visible step. Split/bonus steps derived from Yahoo
(corporate_actions.csv, nse_corporate_actions.py) that NSE's list lacks are applied under the same test, and so are
moves beyond x1.6 that Yahoo's yahoo_adjusted/ series shows as the same step, or that fall on an ISIN change and match a simple
ratio. Anything else beyond x1.6 is only reported (official_info/gaps.csv).

Mergers: the acquired company's series ends on its last trading day (it is kept, not spliced into the acquirer);
official_info/meta.csv gives the price a holder got after it (NSE delisting exit price, merger ratio x the acquirer's
next close, else the last close).

    python nse_bhavcopy.py                    # download new days + corporate actions, rebuild official_adjusted/
    python nse_bhavcopy.py --skip-download    # rebuild from the caches only
"""
import argparse
import datetime as dt
import json
import logging
import math
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import requests

import app_paths
from nse_delisted import fetch_day
from reconcile_nse import EQ_SERIES

log = logging.getLogger("market_dl")
HERE = app_paths.APP_DIR       # nse_demergers.csv sits next to the scripts, or next to the exe in the bundle

BUILD_VERSION = 1      # bump when the build rules change, so the next run rebuilds even if no input changed
COLS = ["Open", "High", "Low", "Close", "Volume"]
WEEKEND_LOOKBACK = 21  # days back in which Saturday/Sunday files are tried too (special sessions)
FIRST_DAY = dt.date(2016, 10, 6)   # oldest day in the official cache
SMALL_F = 0.8          # split/bonus ratios with f >= this (e.g. bonus 1:5 or smaller) are applied on the ex-date
BIG_DIV = 0.25         # a dividend this large a share of the previous close is treated as a return of capital
SCHEME_MIN_DROP = 0.30  # a 'Scheme of Arrangement' ex-date drop this deep is a demerger-type capital change
NON_EQUITY_ISIN = ("INF", "IN9")   # ETFs / mutual fund units, partly paid shares
HALT_DAYS = 30         # calendar days without a bar that count as a trading halt
RENAME_MAX_GAP_DAYS, RENAME_RATIO = 45, (0.6, 1.7)   # same checks as nse_stitch.py
SIMPLE_RATIOS = sorted({float(k) for k in (2, 3, 4, 5, 10, 20, 25, 50, 100)} |
                       {1.0 / k for k in (2, 3, 4, 5, 10, 20, 25, 50, 100)} | {1.5, 2 / 3})

RX_FV = re.compile(r"(?:split|sub-?division|consolidat).*?from\s*r[se]\.?\s*([\d.]+).*?to\s*r[se]\.?\s*([\d.]+)", re.I)
RX_BONUS = re.compile(r"bonus\s*-?\s*(\d+)\s*:\s*(\d+)", re.I)
RX_RIGHTS = re.compile(r"rights?(?:\s*issue)?\s*-?\s*(\d+)\s*:\s*(\d+)(?:[^/@]*@\s*(?:prem(?:ium)?\s*r[se]\.?\s*([\d.]+)|par))?",
                       re.I)
RX_DEMERGER = re.compile(r"demerger", re.I)
RX_SCHEME = re.compile(r"scheme\s+of\s+arrangement", re.I)
RX_DIV = re.compile(r"dividend\s*-?\s*r[se]\.?\s*([\d.]+)", re.I)

CA_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36"
CA_API = "https://www.nseindia.com/api/corporates-corporateActions?index={index}&from_date={a}&to_date={b}"
CA_BOARDS = {"equities": "", "sme": "sme_"}     # index -> cache file prefix
CA_CSV = "nse_corporate_actions.csv"
CA_CHECKED = "checked.json"     # {quarter file: last ex-date the API was asked for}
CA_OVERLAP_DAYS = 7             # an incremental fetch starts this many days before the last checked day
SEED_CA_DIR = app_paths.APP_DIR / "seed_data" / "NSE" / "nse_ca"   # lists shipped with the portable bundle
RX_CA_FILE = re.compile(r"^(?:sme_)?(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.json$")


def paths(nse_dir: Path) -> dict:
    nse_dir = Path(nse_dir)
    return dict(cache=nse_dir / "official_raw" / "full", ca=nse_dir / "nse_ca", out=nse_dir / "official_adjusted",
                info=nse_dir / "official_info")


# ------------------------------------------------------------------ 1. day files ------------------------------------
def download_days(cache: Path, start: dt.date, end: dt.date, workers: int = 6, progress_callback=None,
                  cancel_event=None) -> list:
    """Fetch every missing day file in [start, end] (weekdays; weekends too in the last WEEKEND_LOOKBACK days).
    Returns the days that failed (re-run to retry). progress_callback(done, total, failed, 0) per day; once
    cancel_event is set, the remaining days are skipped (fetched ones stay cached)."""
    cache.mkdir(parents=True, exist_ok=True)
    today = dt.date.today()
    end = min(end, today)
    days = [start + dt.timedelta(n) for n in range((end - start).days + 1)]
    days = [d for d in days if (d.weekday() < 5 or d >= today - dt.timedelta(WEEKEND_LOOKBACK))
            and not (cache / f"{d:%Y%m%d}.csv").exists()]
    if not days:
        log.info("NSE bhavcopy: day cache is up to date")
        return []
    log.info(f"NSE bhavcopy: fetching {len(days)} day file(s) {days[0]} .. {days[-1]} into {cache}")
    # a missing file is remembered as a holiday only once the day is safely past (today's may not be out yet;
    # a weekend usually has no session at all, so it is not remembered either and simply tried again next time)
    def work(d):
        if cancel_event is not None and cancel_event.is_set():
            return d, None
        return fetch_day(d, cache, mark_holiday=d.weekday() < 5 and d < today - dt.timedelta(1))
    failed = []
    with ThreadPoolExecutor(max(1, workers)) as ex:
        for n, (d, ok) in enumerate(ex.map(work, days), 1):
            if ok is False:
                failed.append(d)
            if progress_callback:
                progress_callback(n, len(days), len(failed), 0)
            if n % 100 == 0:
                log.info(f"  NSE bhavcopy {n}/{len(days)} days (failed {len(failed)})")
    new = sum((cache / f"{d:%Y%m%d}.csv").exists() and (cache / f"{d:%Y%m%d}.csv").stat().st_size > 60 for d in days)
    log.info(f"NSE bhavcopy: {new} new trading day(s)" + (f", {len(failed)} failed (re-run to retry)" if failed else ""))
    return failed


# ------------------------------------------------------------------ 2. corporate actions ----------------------------
def _ca_session():
    s = requests.Session()
    s.headers.update({"User-Agent": CA_UA, "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9",
                      "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-actions"})
    s.get("https://www.nseindia.com/", timeout=20)
    time.sleep(2)
    return s


def _quarters(start: dt.date, end: dt.date):
    q = dt.date(start.year, 3 * ((start.month - 1) // 3) + 1, 1)
    while q <= end:
        nxt = dt.date(q.year + (q.month + 3 > 12), (q.month + 2) % 12 + 1, 1)
        yield q, nxt - dt.timedelta(days=1)
        q = nxt


def _ca_fetch(s, a, b, index, tries=4):
    url = CA_API.format(index=index, a=a.strftime("%d-%m-%Y"), b=b.strftime("%d-%m-%Y"))
    for k in range(tries):
        try:
            r = s.get(url, timeout=60)
            if r.status_code == 200 and r.headers.get("content-type", "").startswith("application/json"):
                return r.json(), s
            log.info(f"  NSE corporate actions {index} {a}..{b}: HTTP {r.status_code}, retrying")
        except (requests.RequestException, ValueError) as e:
            log.info(f"  NSE corporate actions {index} {a}..{b}: {type(e).__name__}, retrying")
        time.sleep(10 * (k + 1))
        try:
            s = _ca_session()
        except requests.RequestException:
            pass
    raise RuntimeError(f"NSE corporate actions {index} {a}..{b}: no data after {tries} tries")


def _ca_checked(ca_dir: Path) -> dict:
    """{quarter file name: date} -- the last ex-date each cached quarter list was fetched through. Caches written
    before checked.json existed: a list covers its quarter up to the day the file was written (the fetch asked for
    min(quarter end, that day))."""
    try:
        got = json.loads((ca_dir / CA_CHECKED).read_text(encoding="utf-8"))
        return {k: dt.date.fromisoformat(v) for k, v in got.items()}
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    out = {}
    for f in ca_dir.glob("*.json"):
        m = RX_CA_FILE.match(f.name)
        if m:
            written = dt.date.fromtimestamp(f.stat().st_mtime)
            out[f.name] = min(dt.date.fromisoformat(m.group(2)), written)
    return out


def _save_ca_checked(ca_dir: Path, checked: dict):
    (ca_dir / CA_CHECKED).write_text(json.dumps({k: v.isoformat() for k, v in sorted(checked.items())}, indent=1),
                                     encoding="utf-8")


def seed_corporate_actions(ca_dir: Path, seed_dir: Path = None) -> int:
    """Take the corporate-actions lists shipped with the app (seed_data/NSE/nse_ca next to the exe, written by
    build_bundle.py) into the cache: every quarter file the cache lacks, or holds checked through an earlier day.
    Newer local files are kept. Returns how many lists were copied (0 when there is no seed)."""
    seed_dir = Path(seed_dir or SEED_CA_DIR)
    if not seed_dir.is_dir():
        return 0
    seed = _ca_checked(seed_dir)
    ca_dir.mkdir(parents=True, exist_ok=True)
    local = _ca_checked(ca_dir)
    copied = []
    for name, day in seed.items():
        if (seed_dir / name).exists() and (not (ca_dir / name).exists() or local.get(name, dt.date.min) < day):
            shutil.copy2(seed_dir / name, ca_dir / name)
            local[name] = day
            copied.append(day)
    if copied:
        _save_ca_checked(ca_dir, local)
        log.info(f"NSE corporate actions: {len(copied)} list(s) taken from the bundled data "
                 f"(checked through {max(copied)}); only later trading days are asked from nseindia.com")
    return len(copied)


def _ex_date(row):
    try:
        return dt.datetime.strptime(str(row.get("exDate", "")), "%d-%b-%Y").date()
    except ValueError:
        return None


def _merge_ca(old: list, new: list, a: dt.date, b: dt.date) -> list:
    """The cached rows with an ex-date outside [a, b], plus the fresh rows (which replace those inside it)."""
    keep = [r for r in old if not (_ex_date(r) is not None and a <= _ex_date(r) <= b)]
    seen, out = set(), []
    for r in keep + list(new):
        key = json.dumps(r, sort_keys=True)
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


def latest_trading_day(cache: Path):
    """The newest day file with trades in the bhavcopy cache (holiday markers are tiny), or None."""
    days = [f.stem for f in Path(cache).glob("[0-9]*.csv") if f.stat().st_size > 60] if Path(cache).exists() else []
    return dt.datetime.strptime(max(days), "%Y%m%d").date() if days else None


def update_corporate_actions(ca_dir: Path, start: dt.date = FIRST_DAY, download: bool = True,
                             through: dt.date = None, refresh: bool = False) -> Path:
    """Bring the per-quarter JSON caches up to `through` (the newest trading day the prices reach; default today)
    and rewrite nse_corporate_actions.csv. The lists are downloaded once and reused: only ex-dates after the last
    checked day (minus CA_OVERLAP_DAYS) are asked for, so with no new trading day nothing is fetched. A failed
    fetch leaves the checked day where it was, so the next run tries again. refresh=True fetches every quarter
    again; download=False only rewrites the CSV from the cache."""
    ca_dir.mkdir(parents=True, exist_ok=True)
    today = dt.date.today()
    through = min(through or today, today)
    if not refresh:
        seed_corporate_actions(ca_dir)
    checked = {} if refresh else _ca_checked(ca_dir)
    todo = []
    for index, pre in CA_BOARDS.items():
        for q0, q1 in _quarters(start, through):
            name = f"{pre}{q0}_{q1}.json"
            want = min(q1, through)
            done = checked.get(name) if (ca_dir / name).exists() else None
            if done is not None and done >= want:
                continue
            a = q0 if done is None else max(q0, done - dt.timedelta(days=CA_OVERLAP_DAYS))
            todo.append((index, name, q0, a, want))
    if download and todo:
        n_full = sum(a == q0 for _i, _n, q0, a, _w in todo)
        log.info(f"NSE corporate actions: checking nseindia.com for {len(todo)} list(s) up to {through}"
                 + (f" ({n_full} full quarter(s))" if n_full else " (new days only)"))
        changed = 0
        try:
            s = _ca_session()
            for index, name, q0, a, b in todo:
                rows, s = _ca_fetch(s, a, b, index)
                f = ca_dir / name
                old = json.loads(f.read_text(encoding="utf-8")) if f.exists() and a > q0 else []
                merged = rows if a == q0 else _merge_ca(old, rows, a, b)
                before = {json.dumps(r, sort_keys=True) for r in old}
                changed += len({json.dumps(r, sort_keys=True) for r in merged} - before)
                f.write_text(json.dumps(merged), encoding="utf-8")
                checked[name] = b
                _save_ca_checked(ca_dir, checked)          # after every list, so a later failure keeps this one
                log.info(f"  {index} {a}..{b}: {len(rows)} event(s)")
                time.sleep(3)
            log.info(f"NSE corporate actions: {changed} new or changed event(s)" if changed
                     else "NSE corporate actions: no new events")
        except (RuntimeError, requests.RequestException) as e:
            log.warning(f"NSE corporate actions download failed ({e}); using the cached lists -- the next run with "
                        f"downloading switched on tries again")
    elif download:
        log.info(f"NSE corporate actions: cached lists already checked through {through} (no new trading day)")
    if not (ca_dir / CA_CHECKED).exists() and checked:
        _save_ca_checked(ca_dir, checked)                   # record the dates taken from older caches
    rows = []
    for f in sorted(f for f in ca_dir.glob("*.json") if RX_CA_FILE.match(f.name)):
        board = "sme" if f.name.startswith("sme_") else "equities"
        rows += [dict(r, board=board) for r in json.loads(f.read_text(encoding="utf-8"))]
    out = ca_dir / CA_CSV
    if not rows:
        log.warning(f"No NSE corporate actions cached in {ca_dir}: prices will NOT be adjusted for splits/bonuses")
        pd.DataFrame(columns=["symbol", "series", "isin", "comp", "exDate", "subject", "faceVal", "recDate", "board"]
                     ).to_csv(out, index=False)
        return out
    df = pd.DataFrame(rows)
    df["exDate"] = pd.to_datetime(df["exDate"], format="%d-%b-%Y", errors="coerce")
    df = df[["symbol", "series", "isin", "comp", "exDate", "subject", "faceVal", "recDate", "board"]].drop_duplicates()
    df = df.sort_values(["exDate", "symbol"])
    new = df.to_csv(index=False, lineterminator="\n").encode("utf-8")
    if not out.exists() or out.read_bytes() != new:
        out.write_bytes(new)                        # unchanged content keeps the old mtime (build signature)
    log.info(f"NSE corporate actions: {len(df):,} events, {df['exDate'].min().date()} .. {df['exDate'].max().date()}")
    return out


def num(x):
    try:
        v = float(str(x).strip())
        return v if math.isfinite(v) else None
    except ValueError:
        return None


def parse_events(ca: pd.DataFrame) -> pd.DataFrame:
    """NSE corporate actions -> rows (symbol, isin, date, kind, a, b, price, f) with f known for splits/bonuses."""
    out = []
    for r in ca.itertuples(index=False):
        if pd.isna(r.exDate) or not isinstance(r.subject, str):
            continue
        s = r.subject
        for m in RX_FV.finditer(s):
            x, y = num(m.group(1)), num(m.group(2))
            if x and y and x != y:
                out.append(dict(symbol=r.symbol, isin=r.isin, date=r.exDate, kind="split" if y < x else "consolidation",
                                f=y / x, text=s))
        for m in RX_BONUS.finditer(s):
            a, b = int(m.group(1)), int(m.group(2))
            if a > 0 and b > 0:
                out.append(dict(symbol=r.symbol, isin=r.isin, date=r.exDate, kind="bonus", f=b / (a + b), text=s))
        for m in RX_RIGHTS.finditer(s):
            a, b = int(m.group(1)), int(m.group(2))
            fv = num(r.faceVal) or 0.0
            prem = num(m.group(3)) if m.group(3) else 0.0
            if a > 0 and b > 0:
                out.append(dict(symbol=r.symbol, isin=r.isin, date=r.exDate, kind="rights", a=a, b=b,
                                price=fv + (prem or 0.0), text=s))
        if RX_DEMERGER.search(s):
            out.append(dict(symbol=r.symbol, isin=r.isin, date=r.exDate, kind="demerger", text=s))
        elif RX_SCHEME.search(s):
            out.append(dict(symbol=r.symbol, isin=r.isin, date=r.exDate, kind="scheme", text=s))
        for m in RX_DIV.finditer(s):
            d = num(m.group(1).rstrip("."))
            if d:
                out.append(dict(symbol=r.symbol, isin=r.isin, date=r.exDate, kind="dividend", price=d, text=s))
    cols = ["symbol", "isin", "date", "kind", "f", "a", "b", "price", "text"]
    ev = pd.DataFrame(out).reindex(columns=cols)
    if not len(ev):
        return ev
    ev = ev.drop_duplicates(["symbol", "date", "kind", "f", "a", "b", "price"])
    # a split and a bonus on the same ex-date (e.g. BAJFINANCE 2025-06-16: bonus 4:1 + split 2 -> 1) show as one price
    # step, so they are tested and applied as one event with the product of their ratios
    cap = ev["kind"].isin(["split", "consolidation", "bonus"])
    grp = ev[cap].groupby(["symbol", "date"], as_index=False).agg(
        isin=("isin", "first"), kind=("kind", "+".join), f=("f", "prod"), text=("text", " | ".join))
    return pd.concat([ev[~cap], grp], ignore_index=True)


# ------------------------------------------------------------------ 3. build ----------------------------------------
def load_bhavcopies(cache: Path) -> pd.DataFrame:
    frames = []
    files = [f for f in sorted(cache.glob("[0-9]*.csv")) if f.stat().st_size > 60]
    for n, f in enumerate(files, 1):
        d = pd.read_csv(f, usecols=["Date", "Symbol", "Series", "Open", "High", "Low", "Close", "Volume", "ISIN"],
                        dtype={"Symbol": str, "Series": str, "ISIN": str}, keep_default_na=False, na_values=[""])
        frames.append(d[d["Series"].isin(EQ_SERIES)])
        if n % 500 == 0:
            log.info(f"  reading day files {n}/{len(files)}")
    a = pd.concat(frames, ignore_index=True)
    a["Date"] = pd.to_datetime(a["Date"])
    for c in COLS:
        a[c] = pd.to_numeric(a[c], errors="coerce")
    a = a[a["Symbol"].notna() & ~a["Symbol"].str.endswith("-RE")
          & ~a["ISIN"].fillna("").str.startswith(NON_EQUITY_ISIN)]
    a["_eq"] = a["Series"].eq("EQ")
    a = a.sort_values(["Symbol", "Date", "_eq"]).drop_duplicates(["Symbol", "Date"], keep="last")
    log.info(f"NSE official: {len(files)} days, {len(a):,} rows, {a['Symbol'].nunique():,} symbols")
    return a.drop(columns="_eq")


def clean(df):
    df = df[COLS].astype(float)
    df["Volume"] = df["Volume"].fillna(0).clip(lower=0)
    df = df[(df["Close"] > 0) & (df["High"] > 0) & (df["Low"] > 0)]
    df["Open"] = df["Open"].where(df["Open"] > 0, df["Close"])
    df["High"] = df[["High", "Open", "Close"]].max(axis=1)
    df["Low"] = df[["Low", "Open", "Close"]].min(axis=1)
    return df


def _read_csv_opt(p: Path, **kw):
    return pd.read_csv(p, **kw) if p.exists() and p.stat().st_size > 0 else None


def rename_map(a: pd.DataFrame, nse_dir: Path) -> dict:
    """{old: new} symbol renames to join: nse_stitch.py's approved chains (raw_delisted/stitch_report.csv, Stitch=Y,
    incl. hand-researched ones with a changed ISIN) plus same-ISIN chains found here with the same checks (successor
    = the symbol now carrying the old one's last ISIN, starting within RENAME_MAX_GAP_DAYS at a close ratio within
    RENAME_RATIO), so a rename after the last nse_stitch.py run is joined too."""
    out = {}
    st = _read_csv_opt(nse_dir / "raw_delisted" / "stitch_report.csv", dtype=str)
    if st is not None:
        out.update({r.Old: r.New for r in st[st["Stitch"] == "Y"].itertuples()})
    a = a.sort_values("Date")
    last = a.groupby("Symbol").tail(1).set_index("Symbol")
    holder = a.dropna(subset=["ISIN"]).drop_duplicates("ISIN", keep="last").set_index("ISIN")["Symbol"]
    by_sym = None
    for s, r in last.iterrows():
        n = holder.get(r["ISIN"]) if isinstance(r["ISIN"], str) else None
        if not n or n == s or s in out:
            continue
        if by_sym is None:
            by_sym = {k: g for k, g in a.groupby("Symbol")}
        after = by_sym[n][by_sym[n]["Date"] > r["Date"]]
        if after.empty:
            continue
        gap = (after["Date"].iat[0] - r["Date"]).days
        ratio = after["Close"].iat[0] / r["Close"] if r["Close"] else float("nan")
        if gap <= RENAME_MAX_GAP_DAYS and RENAME_RATIO[0] <= ratio <= RENAME_RATIO[1]:
            out[s] = n
    return out


def best_gap(df, d, f, before=2, after=3):
    """Bar index near ex-date d whose close / previous close is closest to f, if that move is nearer f than 1."""
    k = int(df.index.searchsorted(pd.Timestamp(d)))
    c = df["Close"].to_numpy()
    best, bk, bg = None, None, None
    for j in range(max(1, k - before), min(len(df), k + after + 1)):
        if c[j - 1] <= 0:
            continue
        g = c[j] / c[j - 1]
        dist = abs(math.log(g) - math.log(f))
        if best is None or dist < best:
            best, bk, bg = dist, j, g
    if bk is None or best >= 0.5 * abs(math.log(f)):
        return None, None
    return bk, bg


def _yahoo_close(nse_dir: Path, sym: str):
    p = nse_dir / "yahoo_adjusted" / f"NSE.{sym}.csv"
    if not p.exists() or p.stat().st_size < 30:
        return None
    try:
        y = pd.read_csv(p, usecols=["Date", "Close"])
    except (ValueError, pd.errors.EmptyDataError):
        return None
    y["Date"] = pd.to_datetime(y["Date"].astype(str).str[:10], errors="coerce")
    y = y.dropna().drop_duplicates("Date", keep="last").set_index("Date")["Close"].sort_index()
    y = y[y > 0]
    return y if len(y) else None


def yahoo_step(y, df, k, f, tol=0.03):
    """True if Yahoo's adjusted series shows the step f at bar k (as-traded / Yahoo close, 3 bars after vs before)."""
    if y is None:
        return False
    j = (df["Close"] / df["factor"]).to_frame("o").join(y.rename("y"), how="inner")
    rr = j["o"] / j["y"]
    b, a = rr[rr.index < df.index[k]].tail(3), rr[rr.index >= df.index[k]].head(3)
    if len(b) < 2 or len(a) < 2:
        return False
    return abs((a.mean() / b.mean()) / f - 1) < tol


def yahoo_confirmed_gaps(y, df, known, cut=math.log(1.6)):
    """Overnight moves beyond x1.6 with no event within 5 days that Yahoo's series shows as the same step (a capital
    reduction / consolidation / relisting missing from NSE's list) -> [(bar, f)]."""
    c = df["Close"].to_numpy()
    lg = np.diff(np.log(c))
    cand = [j + 1 for j in np.nonzero(np.abs(lg) > cut)[0]
            if not any(abs((df.index[j + 1] - x).days) <= 5 for x in known)]
    if not cand or y is None:
        return []
    out = []
    j = df["Close"].to_frame("o").join(y.rename("y"), how="inner")
    rr = j["o"] / j["y"]
    for k in cand:
        b, a = rr[rr.index < df.index[k]].tail(3), rr[rr.index >= df.index[k]].head(3)
        if len(b) < 1 or len(a) < 1:
            continue
        g = c[k] / c[k - 1]
        if abs(math.log(a.mean() / b.mean()) - math.log(g)) < 0.1:
            out.append((k, g))
    return sorted(out, reverse=True)


def isin_change_gaps(df, isin, known, cut=math.log(1.6), tol=0.08):
    """Overnight moves beyond x1.6 with no event within 5 days, on a bar where the ISIN changes and the move is within
    tol (log) of a simple split / consolidation / bonus ratio -> [(bar, ratio)] (latest first)."""
    c = df["Close"].to_numpy()
    lg = np.diff(np.log(c))
    isin = isin.reindex(df.index)
    out = []
    for j in np.nonzero(np.abs(lg) > cut)[0]:
        k = j + 1
        d = df.index[k]
        if any(abs((d - x).days) <= 5 for x in known) or isin.iat[k] == isin.iat[k - 1] or pd.isna(isin.iat[k]):
            continue
        g = c[k] / c[k - 1]
        f = min(SIMPLE_RATIOS, key=lambda r: abs(math.log(r) - math.log(g)))
        if abs(math.log(f) - math.log(g)) < tol:
            out.append((k, f))
    return sorted(out, reverse=True)


def halts(df, sym):
    d = df.index.to_series().diff().dt.days.to_numpy()
    return [dict(Symbol=sym, LastBar=df.index[k - 1].date(), Resumed=df.index[k].date(), Days=int(d[k]),
                 LastClose=round(float(df["Close"].iat[k - 1]), 4))
            for k in np.nonzero(d > HALT_DAYS)[0]]


def apply_factor(df, k, f):
    df.iloc[:k, df.columns.get_indexer(["Open", "High", "Low", "Close"])] *= f
    df.iloc[:k, df.columns.get_indexer(["Volume"])] /= f
    df.iloc[:k, df.columns.get_indexer(["factor"])] *= f


def load_demergers(nse_dir: Path, path: Path) -> dict:
    """{parent: {date: expected drop %}} from nse_demergers.csv, with recon/demerger_check.csv's expectation summed per
    parent and date (several children on one day share one drop); 0 = use the actual drop."""
    dm = _read_csv_opt(path, parse_dates=["Date"])
    if dm is None:
        return {}
    exp = {}
    ck = _read_csv_opt(nse_dir / "recon" / "demerger_check.csv", parse_dates=["DeclaredDate"])
    if ck is not None:
        exp = {(r.Parent, r.DeclaredDate): r.ExpectedPct for r in ck.itertuples() if pd.notna(r.ExpectedPct)}
    rows = {}
    for r in dm.itertuples():
        rows.setdefault(r.Parent, {}).setdefault(r.Date, 0.0)
        if (r.Parent, r.Date) in exp:
            rows[r.Parent][r.Date] += float(exp[(r.Parent, r.Date)])
    return rows


def adjust_demergers(df, sym, events, dlog):
    """Back-adjust a parent for its demergers (events {date: expected drop %}): earlier prices x (1 + drop), drop = the
    expected one, else (or when it is outside -90..0%) the parent's own close-to-close move on the ex-date; skipped
    when the series shows no step there (|move| < 3%)."""
    for d, exp_pct in sorted((events or {}).items()):
        k = df.index.searchsorted(pd.Timestamp(d))
        if not (0 < k < len(df)):
            continue
        move = df["Close"].iloc[k] / df["Close"].iloc[k - 1] - 1
        if abs(move) < 0.03:
            continue
        drop = exp_pct / 100 if exp_pct else move
        if not (-0.9 < drop < 0):
            drop = move
        if not (-0.9 < drop < 0):
            continue
        apply_factor(df, k, 1 + drop)
        dlog.append((sym, df.index[k].date(), round(1 + drop, 4), "demerger (nse_demergers.csv)"))
    return df


def adjust_extra_demergers(df, dates, dlog, sym):
    """dates: ex-date -> smallest drop that counts (3% for a listed demerger, SCHEME_MIN_DROP for a scheme)."""
    for d in sorted(dates):
        k = df.index.searchsorted(pd.Timestamp(d))
        if not (0 < k < len(df)):
            continue
        move = df["Close"].iat[k] / df["Close"].iat[k - 1] - 1
        if not (-0.95 < move < -dates[d]):
            continue
        apply_factor(df, k, 1 + move)
        what = "scheme of arrangement" if dates[d] > 0.03 else "demerger"
        dlog.append((sym, df.index[k].date(), round(1 + move, 4), f"{what} (NSE list; parent's ex-date drop)"))
    return df


def _exit_price_note(note):
    m = re.search(r"exit price\s*([\d.]+)", str(note))
    return float(m.group(1)) if m else None


def add_exits(meta, eod_ca, asis_close, owner):
    """Exit price after a symbol's last bar: NSE delisting exit price, merger value, or the last close."""
    px, why = [], []
    dl = eod_ca[eod_ca["Type"] == "delist"].set_index("Symbol")["Note"].to_dict()
    mg = eod_ca[(eod_ca["Type"] == "merger") & ~eod_ca["Source"].astype(str).str.contains("REVIEW")]
    mg = mg.drop_duplicates("Symbol").set_index("Symbol")[["NewSymbol", "Ratio"]].to_dict("index")
    for r in meta.itertuples():
        p, w = r.LastClose, "last close"
        if not r.Active:
            x = _exit_price_note(dl.get(r.Symbol))
            m = mg.get(r.Symbol)
            if x and 0.2 < x / r.LastClose < 5:
                p, w = x, "NSE delisting exit price"
            elif m and pd.notna(m["Ratio"]):
                acq = asis_close.get(owner.get(m["NewSymbol"], m["NewSymbol"]))
                if acq is not None:
                    after = acq[acq.index > pd.Timestamp(r.Last)]
                    if len(after):
                        p = float(m["Ratio"]) * after.iat[0]
                        w = f"merged into {m['NewSymbol']}: {m['Ratio']} x its close {after.iat[0]:.2f} on {after.index[0].date()}"
        px.append(p)
        why.append(w)
    meta["ExitPrice"], meta["ExitWhy"] = px, why
    return meta


def unexplained_gaps(frames, log_df, cut=math.log(1.6)):
    """Overnight moves beyond x1.6 / /1.6 left in the adjusted series with no event near them (reported only)."""
    near = {}
    if len(log_df):
        for r in log_df[log_df["status"].astype(str).str.startswith("applied")].itertuples():
            if pd.notna(r.bar):
                near.setdefault(r.symbol, []).append(pd.Timestamp(r.bar))
    rows = []
    for sym, df in frames.items():
        c = df["Close"].to_numpy()
        hits = np.nonzero(np.abs(np.diff(np.log(c))) > cut)[0]
        if not len(hits):
            continue
        days = df.index.to_series().diff().dt.days.to_numpy()
        for j in hits:
            d = df.index[j + 1]
            if any(abs((d - x).days) <= 5 for x in near.get(sym, [])):
                continue
            rows.append(dict(symbol=sym, date=d.date(), prev_close=round(c[j], 4), close=round(c[j + 1], 4),
                             move=round(c[j + 1] / c[j], 3), after_halt=bool(days[j + 1] > HALT_DAYS)))
    return pd.DataFrame(rows, columns=["symbol", "date", "prev_close", "close", "move", "after_halt"])


def _signature(nse_dir: Path, demergers_csv: Path) -> dict:
    p = paths(nse_dir)
    days = sorted(f.name for f in p["cache"].glob("[0-9]*.csv"))

    def stamp(f):
        f = Path(f)
        return [f.stat().st_size, int(f.stat().st_mtime)] if f.exists() else None
    return dict(version=BUILD_VERSION, days=len(days), last_day=days[-1] if days else "",
                ca=stamp(p["ca"] / CA_CSV), demergers=stamp(demergers_csv),
                demerger_check=stamp(nse_dir / "recon" / "demerger_check.csv"),
                stitch=stamp(nse_dir / "raw_delisted" / "stitch_report.csv"),
                eod_ca=stamp(nse_dir / "corporate_actions.csv"))


def build(nse_dir: Path, demergers_csv: Path = HERE / "nse_demergers.csv", force: bool = False,
          progress_callback=None, cancel_event=None) -> bool:
    """Rebuild <nse_dir>/official_adjusted/ from the caches. Returns False when skipped (nothing changed since the last build)
    or cancelled (cancel_event set before the files were written: official_adjusted/ is left as it was).
    progress_callback(done, total, 0, 0) per symbol."""
    nse_dir = Path(nse_dir)
    p = paths(nse_dir)
    sig = _signature(nse_dir, demergers_csv)
    sig_path = p["info"] / "build_signature.json"
    if not force and sig_path.exists() and any(p["out"].glob("NSE.*.csv")):
        try:
            if json.loads(sig_path.read_text(encoding="utf-8")) == sig:
                log.info(f"NSE official: {p['out']} is up to date (no new day files or corporate actions)")
                return False
        except ValueError:
            pass
    if not sig["days"]:
        raise RuntimeError(f"no NSE day files in {p['cache']} -- run without --skip-download first")
    t0 = time.time()
    a = load_bhavcopies(p["cache"])
    last_day = a["Date"].max()
    ca = _read_csv_opt(p["ca"] / CA_CSV, parse_dates=["exDate"])
    ev = parse_events(ca if ca is not None else pd.DataFrame(columns=["exDate", "subject"]))
    log.info(f"NSE corporate actions: {0 if ca is None else len(ca):,} rows -> {len(ev):,} price events "
             f"({ev['kind'].value_counts().to_dict() if len(ev) else {}})")
    eod_ca = _read_csv_opt(nse_dir / "corporate_actions.csv", parse_dates=["Date"])
    if eod_ca is None:
        eod_ca = pd.DataFrame(columns=["Date", "Symbol", "Type", "Ratio", "NewSymbol", "Source", "Note"])

    # renames: the old symbol's rows go in front of the final symbol (chains followed to the end)
    nxt = rename_map(a, nse_dir)

    def final(s, seen=()):
        return final(nxt[s], seen + (s,)) if s in nxt and nxt[s] not in seen else s
    owner = {s: final(s) for s in set(a["Symbol"])}
    a["Final"] = a["Symbol"].map(owner)
    # an old symbol's rows are kept only before the new symbol's own first day
    own_first = a[a["Symbol"] == a["Final"]].groupby("Symbol")["Date"].min()
    a = a[(a["Symbol"] == a["Final"]) | (a["Date"] < a["Final"].map(own_first).fillna(pd.Timestamp.max))]
    a = a.sort_values(["Final", "Date", "Symbol"]).drop_duplicates(["Final", "Date"], keep="last")
    joined = sorted(s for s in owner if owner[s] != s)
    log.info(f"NSE official: {len(joined)} renamed symbol(s) joined under their latest symbol")

    # which final symbol an event belongs to: by symbol, else by ISIN
    isin_sym = a.dropna(subset=["ISIN"]).groupby("ISIN")["Final"].agg(lambda x: x.iloc[-1]).to_dict()
    if len(ev):
        ev["final"] = [owner.get(r.symbol) or isin_sym.get(r.isin) for r in ev.itertuples()]
        ev = ev[ev["final"].notna()]
    ev_by = {s: g for s, g in ev.groupby("final")} if len(ev) else {}

    # split/bonus steps derived from Yahoo vs official prices (nse_corporate_actions.py) as a second source
    der = eod_ca[(eod_ca["Type"] == "split_bonus") & pd.to_numeric(eod_ca["Ratio"], errors="coerce").notna()].copy()
    der["final"] = der["Symbol"].map(lambda s: owner.get(s, s))
    der_by = {s: g for s, g in der.groupby("final")}
    dm_rows = load_demergers(nse_dir, Path(demergers_csv))

    asis_close, frames = {}, {}
    rows_log, meta, halt_rows = [], [], []
    n_syms = a["Final"].nunique()
    for n, (sym, g) in enumerate(a.groupby("Final", sort=True), 1):
        if cancel_event is not None and cancel_event.is_set():
            log.warning("NSE official: rebuild cancelled -- official_adjusted/ left as it was")
            return False
        if progress_callback and (n % 25 == 0 or n == n_syms):
            progress_callback(n, n_syms, 0, 0)
        df = clean(g.set_index("Date"))
        if len(df) < 2:
            # one bar (a first run over a single day, the 1D quick range, or a one-day listing): nothing to adjust
            if len(df) == 1:
                asis_close[sym] = df["Close"].copy()
                frames[sym] = df
                meta.append(dict(Symbol=sym, First=df.index[0].date(), Last=df.index[-1].date(), Bars=1,
                                 Active=df.index[-1] == last_day, LastClose=df["Close"].iat[-1], Events=0,
                                 JoinedFrom=" ".join(s for s in joined if owner[s] == sym)))
            continue
        isin = g.set_index("Date")["ISIN"]
        asis_close[sym] = df["Close"].copy()
        df["factor"] = 1.0
        applied = []
        y = None
        e = ev_by.get(sym)
        if e is not None:
            for r in e.sort_values("date", ascending=False).itertuples():
                if r.kind in ("demerger", "scheme"):
                    continue                                        # handled with the demergers below
                if r.kind not in ("rights", "dividend"):          # split / bonus / consolidation (combined)
                    k, gap = best_gap(df, r.date, r.f)
                    status = "applied"
                    if k is None:
                        k0 = int(df.index.searchsorted(r.date))
                        k0 = k0 if 0 < k0 < len(df) else None
                        if k0 is not None and r.f >= SMALL_F:
                            # a ratio this small hides inside a normal day's move: trust NSE's ex-date
                            k, status = k0, "applied (small ratio, NSE ex-date)"
                        elif k0 is not None:
                            y = _yahoo_close(nse_dir, sym) if y is None else y
                            if yahoo_step(y, df, k0, r.f):
                                k, status = k0, "applied (NSE ex-date, Yahoo shows the step)"
                    ok = k is not None and not any(abs((df.index[k] - x[0]).days) <= 5 and x[1] != "rights" and
                                                  abs(x[2] - r.f) < 1e-6 for x in applied)
                    if ok:
                        apply_factor(df, k, r.f)
                        applied.append((df.index[k], r.kind, r.f))
                    rows_log.append(dict(symbol=sym, date=r.date.date(), kind=r.kind, f=round(r.f, 6), source="NSE",
                                         bar=df.index[k].date() if k is not None else None,
                                         gap=round(gap, 4) if gap else None,
                                         status=status if ok else ("duplicate" if k is not None else "no price gap"),
                                         text=r.text[:120]))
                elif r.kind == "rights":
                    k = int(df.index.searchsorted(r.date))
                    if not (0 < k < len(df)):
                        continue
                    pc = df["Close"].iat[k - 1] / df["factor"].iat[k - 1]     # previous close as traded
                    status, f = "rights price >= previous close (no adjustment)", None
                    if r.price and r.price < pc:
                        f = ((r.b * pc + r.a * r.price) / (r.a + r.b)) / pc
                        apply_factor(df, k, f)
                        applied.append((df.index[k], "rights", f))
                        status = "applied"
                    rows_log.append(dict(symbol=sym, date=r.date.date(), kind="rights", f=round(f, 6) if f else None,
                                         source="NSE", bar=df.index[k].date(), gap=None, status=status,
                                         text=r.text[:120]))
                else:                                               # dividend
                    k = int(df.index.searchsorted(r.date))
                    if not (0 < k < len(df)):
                        continue
                    pc = df["Close"].iat[k - 1] / df["factor"].iat[k - 1]
                    share = r.price / pc if pc > 0 else 0
                    if share < BIG_DIV:
                        continue                                    # ordinary dividend: not adjusted
                    seen = df["Open"].iat[k] / df["factor"].iat[k] / pc - 1
                    f = 1 - share
                    ok = 0 < f and seen <= -0.5 * share
                    if ok:
                        apply_factor(df, k, f)
                        applied.append((df.index[k], "dividend", f))
                    rows_log.append(dict(symbol=sym, date=r.date.date(), kind="return of capital (dividend)",
                                         f=round(f, 6) if ok else None, source="NSE", bar=df.index[k].date(),
                                         gap=round(1 + seen, 4),
                                         status="applied" if ok else "no matching drop (not adjusted)",
                                         text=r.text[:120]))
        dg = der_by.get(sym)
        nse_dates = (list(e.loc[~e["kind"].isin(["dividend", "scheme"]), "date"]) if e is not None else []) + \
            [x[0] for x in applied if x[1] == "dividend"]
        if dg is not None:
            for r in dg.itertuples():
                f = 1.0 / float(r.Ratio)
                if any(abs((pd.Timestamp(x) - r.Date).days) <= 7 for x in nse_dates):
                    continue                    # NSE's list has an event there (split, bonus, rights or demerger)
                k, gap = best_gap(df, r.Date, f)
                if k is not None:
                    apply_factor(df, k, f)
                    applied.append((df.index[k], "split_bonus (Yahoo-derived)", f))
                rows_log.append(dict(symbol=sym, date=r.Date.date(), kind="split_bonus (Yahoo-derived)",
                                     f=round(f, 6), source="corporate_actions.csv",
                                     bar=df.index[k].date() if k is not None else None,
                                     gap=round(gap, 4) if gap else None,
                                     status="applied" if k is not None else "no price gap", text=str(r.Note)[:120]))
        # demergers: the hand-kept list, plus NSE-listed demergers / schemes it lacks (parent's actual ex-date drop)
        dlog = []
        extra = {}
        nse_dm = e[e["kind"].isin(["demerger", "scheme"])] if e is not None else None
        if nse_dm is not None:
            for r in nse_dm.itertuples():
                if not any(abs((pd.Timestamp(d) - r.date).days) <= 30 for d in dm_rows.get(sym, {})):
                    extra[r.date] = SCHEME_MIN_DROP if r.kind == "scheme" else 0.03
        df = adjust_demergers(df, sym, dm_rows.get(sym), dlog)
        if extra:
            df = adjust_extra_demergers(df, extra, dlog, sym)
        known = [x[0] for x in applied] + [pd.Timestamp(x[1]) for x in dlog]
        big = np.abs(np.diff(np.log(df["Close"].to_numpy()))) > math.log(1.6)
        if big.any():
            y = _yahoo_close(nse_dir, sym) if y is None else y
            for k, g_ in yahoo_confirmed_gaps(y, df, known):
                apply_factor(df, k, g_)
                applied.append((df.index[k], "capital change (Yahoo)", g_))
                rows_log.append(dict(symbol=sym, date=df.index[k].date(), kind="capital change (gap = Yahoo step)",
                                     f=round(g_, 6), source="price gap + Yahoo", bar=df.index[k].date(),
                                     gap=round(g_, 4), status="applied",
                                     text="overnight move beyond x1.6 that Yahoo's series shows as the same step"))
            known = [x[0] for x in applied] + [pd.Timestamp(x[1]) for x in dlog]
            for k, f in isin_change_gaps(df, isin, known):
                g_ = df["Close"].iat[k] / df["Close"].iat[k - 1]
                apply_factor(df, k, f)
                applied.append((df.index[k], "capital change (ISIN)", f))
                ii = isin.reindex(df.index)
                rows_log.append(dict(symbol=sym, date=df.index[k].date(), kind="capital change (ISIN change)",
                                     f=round(f, 6), source="price gap + new ISIN", bar=df.index[k].date(),
                                     gap=round(g_, 4), status="applied",
                                     text=f"ISIN {ii.iat[k - 1]} -> {ii.iat[k]}; move x{g_:.3f} taken as x{f:.4g}"))
        for x in dlog:
            rows_log.append(dict(symbol=sym, date=x[1], kind="demerger", f=x[2], source="nse_demergers.csv / NSE",
                                 bar=x[1], gap=None, status="applied", text=x[3]))
        halt_rows += halts(df, sym)
        frames[sym] = df
        meta.append(dict(Symbol=sym, First=df.index[0].date(), Last=df.index[-1].date(), Bars=len(df),
                         Active=df.index[-1] == last_day, LastClose=asis_close[sym].iat[-1],
                         Events=len(applied) + len(dlog),
                         JoinedFrom=" ".join(s for s in joined if owner[s] == sym)))
        if n % 1000 == 0:
            log.info(f"  adjusted {n} symbols | {time.time() - t0:.0f}s")

    # write the bars (yahoo_adjusted/ layout) and drop files of symbols that no longer exist as a series (renamed since)
    p["out"].mkdir(parents=True, exist_ok=True)
    p["info"].mkdir(parents=True, exist_ok=True)
    written = set()
    for sym, df in frames.items():
        out = df[["Open", "High", "Low", "Close"]].round(4)
        out["Volume"] = df["Volume"].round().astype("int64")
        out["Ticker"] = sym
        f = p["out"] / f"NSE.{sym}.csv"
        out.reset_index().to_csv(f, index=False, date_format="%Y-%m-%d")
        written.add(f.name)
    stale = [f for f in p["out"].glob("NSE.*.csv") if f.name not in written]
    for f in stale:
        f.unlink()
    meta = add_exits(pd.DataFrame(meta), eod_ca, asis_close, owner)
    meta.to_csv(p["info"] / "meta.csv", index=False)
    log_df = pd.DataFrame(rows_log, columns=["symbol", "date", "kind", "f", "source", "bar", "gap", "status", "text"])
    log_df.to_csv(p["info"] / "events.csv", index=False)
    unexplained_gaps(frames, log_df).to_csv(p["info"] / "gaps.csv", index=False)
    pd.DataFrame(halt_rows, columns=["Symbol", "LastBar", "Resumed", "Days", "LastClose"]).to_csv(
        p["info"] / "halts.csv", index=False)
    (p["info"] / "README.md").write_text(_readme(meta, log_df, last_day), encoding="utf-8")
    sig_path.write_text(json.dumps(sig), encoding="utf-8")
    applied_n = int(log_df["status"].astype(str).str.startswith("applied").sum())
    log.info(f"NSE official: {len(frames):,} symbols written to {p['out']} ({int(meta['Active'].sum()):,} trading on "
             f"{last_day.date()}), {applied_n:,} adjustments applied, {len(stale)} stale file(s) removed | "
             f"{time.time() - t0:.0f}s")
    return True


def _readme(meta, log_df, last_day):
    st = log_df["status"].value_counts().to_dict() if len(log_df) else {}
    return (f"# NSE official daily bars (nse_bhavcopy.py)\n\n"
            f"Built {dt.datetime.now():%Y-%m-%d %H:%M} from NSE's bhavcopies up to {last_day.date()}, back-adjusted "
            f"with NSE's corporate-actions lists (see nse_bhavcopy.py's docstring for the rules).\n\n"
            f"* `../official_adjusted/NSE.<SYM>.csv`: {len(meta):,} symbols ({int(meta['Active'].sum()):,} trading on the last "
            f"day; the rest are delisted or merged and end on their last trading day).\n"
            f"* `meta.csv`: first/last bar, rename chains joined in, exit price after the last bar.\n"
            f"* `events.csv`: every corporate action considered and what was done ({st}).\n"
            f"* `gaps.csv`: overnight moves beyond x1.6 with no event (not adjusted; check before trusting).\n"
            f"* `halts.csv`: gaps of more than {HALT_DAYS} days between bars (suspensions).\n")


# ------------------------------------------------------------------ entry point -------------------------------------
def update(nse_dir: Path, start: str = None, end: str = None, download: bool = True, workers: int = 6,
           force_build: bool = False, progress_callback=None, cancel_event=None, refresh_actions: bool = False) -> bool:
    """Download new day files (unless download=False), check NSE's corporate actions for the trading days that
    arrived since the last check (none new = no request), then rebuild official_adjusted/ if anything changed. Day files
    are fetched from max(start, FIRST_DAY) .. end (inclusive), or from the newest cached day."""
    nse_dir = Path(nse_dir)
    p = paths(nse_dir)
    if download:
        cached = sorted(f.stem for f in p["cache"].glob("[0-9]*.csv")) if p["cache"].exists() else []
        first = max(dt.date.fromisoformat(start) if start else FIRST_DAY, FIRST_DAY)
        last = dt.date.fromisoformat(end) if end else dt.date.today()
        download_days(p["cache"], first, last, workers, progress_callback, cancel_event)
        if not cached:
            log.info("NSE bhavcopy: first run -- the whole range was fetched")
    if cancel_event is not None and cancel_event.is_set():
        return False
    update_corporate_actions(p["ca"], download=download, through=latest_trading_day(p["cache"]),
                             refresh=refresh_actions)
    return build(nse_dir, force=force_build, progress_callback=progress_callback, cancel_event=cancel_event)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=str(app_paths.daily_output()))
    ap.add_argument("--start", default=None, help="first day to make sure is cached (default: oldest, 2016-10-06)")
    ap.add_argument("--skip-download", action="store_true", help="no network: rebuild from the caches")
    ap.add_argument("--force", action="store_true", help="rebuild even if no input changed")
    ap.add_argument("--refresh-actions", action="store_true",
                    help="download NSE's corporate-actions lists for every quarter again (normally only the trading "
                         "days since the last check are asked for)")
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    update(Path(args.root) / "NSE", start=args.start, download=not args.skip_download, workers=args.workers,
           force_build=args.force, refresh_actions=args.refresh_actions)


if __name__ == "__main__":
    main()
