"""Bring the Alpaca US daily files up to date from Alpaca's Market Data API (eod_downloader --us-source alpaca, the
default for NASDAQ/NYSE).

The files are <root>/<EX>/alpaca/<EX>.<KEY>.csv (Date,Open,High,Low,Close,Volume,Ticker; splits and spin-offs
back-adjusted, dividends not), first written by TranscriptExtract's `py -m swingbt.data_fetch.alpaca_export` from the
survivorship-free store in market_data_alpaca/. This module only extends them; it never touches market_data_alpaca/.

Per exchange, update_exchange():
  * still-trading keys (alpaca_exits.csv still_trading=True, or a recent last bar for keys added here) get the bars
    after their last one (adjustment=split,spin-off, SIP feed). Each request starts on the key's second-to-last stored
    bar, so the overlap shows whether Alpaca's adjusted price for an old day has changed. A change beyond BASIS_TOL
    is accepted only when Alpaca's corporate-actions list explains it: a split / reverse split (its exact listed
    ratio, if the measured one agrees within SPLIT_TOL) or a spin-off / stock dividend (the measured drop). The
    whole stored history is then rescaled (volume by the split ratio only, as Alpaca does), which keeps the export's
    own spin-off corrections. A change nothing explains is almost always a ticker that now belongs to another company
    (Alpaca maps symbols as of today): that file is NOT updated and the case is logged as 'skipped'.
    The overlapping bars are replaced (fixes a partial last bar).
  * name changes (Alpaca corporate actions): the old key's file is replaced by the new symbol's full history (Alpaca
    maps the old bars to the new symbol). If another company's file already holds the new ticker, that company keeps
    its bars as NEW@<change date> (topped up to its last day via Alpaca's as-of mapping); if the file already holds
    the same bars (the export used the new name), the old file is just removed.
  * new listings: active Alpaca assets on the exchange with no file that the original download did not know
    (market_data_alpaca/universe.csv) get their full history from START.
  * delisted keys (and TICKER@date keys) are left alone: they end at their delisting date.
Today's bar is left out until the New York day is over (no partial bars). Flat zero-volume no-trade bars are dropped,
as in the export. Every rescale / rename / new file is logged to <EX>/alpaca_updates.csv.
Not updated here: alpaca_exits.csv and the hourly alpaca_1h/ files (both from the export).

Keys: APCA_API_KEY_ID / APCA_API_SECRET_KEY in the environment, else the first working one of .alpaca3.env,
.alpaca2.env, .alpaca.env next to this script (KEY=VALUE lines; git-ignored, never print them).

    python alpaca_us.py                      # NASDAQ + NYSE
    python alpaca_us.py --exchanges NYSE --root market_data_daily
"""
import argparse
import logging
import os
import re
import threading
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests

log = logging.getLogger("market_dl")
HERE = Path(__file__).resolve().parent

DATA_URL = "https://data.alpaca.markets"
TRADE_URLS = ("https://paper-api.alpaca.markets", "https://api.alpaca.markets")
ENV_FILES = (".alpaca3.env", ".alpaca2.env", ".alpaca.env")
KEY_NAMES = ("APCA_API_KEY_ID", "ALPACA_API_KEY", "ALPACA_KEY_ID")
SECRET_NAMES = ("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY", "ALPACA_API_SECRET")
ADJUSTMENT = "split,spin-off"     # the export's basis: splits and spin-offs, no dividends
START = "2016-01-01"              # history start for new listings (SIP history begins 2016-01-04)
BATCH = 100                       # symbols per bars request
BASIS_TOL = 0.002                 # overlap close ratio beyond this = a corporate action since the last run
SPLIT_TOL = 0.02                  # measured vs listed split ratio
LOOKBACK_DAYS = 30                # corporate actions are read from this long before the newest stored bar
ACTIVE_DAYS = 30                  # a key not in alpaca_exits.csv counts as trading if its last bar is this recent
PAUSE = 0.31                      # seconds between requests (free plan: 200 a minute)
NY = "America/New_York"


# ------------------------------------------------------------------ client ------------------------------------------
def _env_file(path: Path) -> dict:
    vals = {}
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            vals[k.strip().removeprefix("export ").strip()] = v.strip().strip('"').strip("'")
    return vals


class Alpaca:
    def __init__(self, key, secret, pause=PAUSE):
        self.s = requests.Session()
        self.s.headers.update({"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret, "accept": "application/json"})
        self.pause, self.calls = pause, 0
        self._lock, self._next = threading.Lock(), 0.0

    def _wait(self):
        with self._lock:
            now = time.monotonic()
            t = max(now, self._next)
            self._next = t + self.pause
            self.calls += 1
        time.sleep(max(0.0, t - now))

    def get(self, url, params=None, tries=5):
        err = ""
        for _ in range(tries):
            self._wait()
            try:
                r = self.s.get(url, params=params, timeout=60)
            except requests.RequestException as e:
                err = repr(e)[:200]
                time.sleep(5)
                continue
            if r.status_code == 429:
                err = "429 rate limited"
                time.sleep(30)
                continue
            if r.status_code >= 400:
                return None, f"HTTP {r.status_code}: {r.text[:200]}"
            return r.json(), ""
        return None, err

    def bars(self, symbols, start, end=None, asof=None):
        """{symbol: DataFrame(Open..Volume, index Date)} for daily bars, adjustment ADJUSTMENT. A symbol Alpaca calls
        invalid is dropped from the request (and returned empty) instead of failing the whole batch. asof
        (YYYY-MM-DD) = Alpaca's symbol mapping as of that day (the company that had the ticker then)."""
        ok = [s for s in symbols if s]
        # the free plan serves SIP data only up to 15 minutes ago: end at the end of `end`'s New York day, or
        # 16 minutes ago, whichever is earlier (an end in the 'recent' window gets HTTP 403)
        cap = pd.Timestamp.now(tz="UTC") - pd.Timedelta(minutes=16)
        if end:   # daily bars are stamped at New York midnight: stop just before the next day's stamp
            cap = min(cap, ((pd.Timestamp(end) + pd.Timedelta(days=1)).tz_localize(NY)
                            - pd.Timedelta(minutes=1)).tz_convert("UTC"))
        while ok:
            params = dict(symbols=",".join(ok), timeframe="1Day", start=start, end=cap.strftime("%Y-%m-%dT%H:%M:%SZ"),
                          adjustment=ADJUSTMENT, feed="sip", limit=10000)
            if asof:
                params["asof"] = asof
            got, tok, js, err = {}, None, None, ""
            while True:
                if tok:
                    params["page_token"] = tok
                js, err = self.get(f"{DATA_URL}/v2/stocks/bars", params)
                if js is None:
                    break
                for s, bl in (js.get("bars") or {}).items():
                    got.setdefault(s, []).extend(bl or [])
                tok = js.get("next_page_token")
                if not tok:
                    break
            m = re.search(r"invalid symbol: ([^\"}]+)\"", err or "")
            if js is None and m and m.group(1) in ok:
                ok.remove(m.group(1))
                continue
            if js is None:
                raise RuntimeError(f"Alpaca bars request failed: {err}")
            return {s: _frame(got.get(s, [])) for s in symbols}
        return {s: _frame([]) for s in symbols}

    def active_assets(self):
        for base in TRADE_URLS:
            js, err = self.get(f"{base}/v2/assets", dict(status="active", asset_class="us_equity"), tries=2)
            if js is not None:
                return pd.DataFrame(js)
        log.warning(f"Alpaca asset list unavailable ({err}); new listings are not added this run")
        return None

    def corporate_actions(self, start, end):
        """(events, renames) between start and end, or (None, []) if Alpaca's list is unavailable.
        events: {symbol: [(ex_date, kind, price_ratio)]} for splits, reverse/unit splits, stock dividends and
        spin-offs (spin-offs under the source symbol); price_ratio = what earlier adjusted prices get multiplied by
        (old_rate / new_rate for splits), None when the list does not fix it (spin-offs, stock dividends).
        renames: [(old, new, date)]; events listed under the old symbol are copied to the new one."""
        events, renames, tok = {}, [], None
        params = dict(types="forward_split,reverse_split,unit_split,stock_dividend,spin_off,name_change",
                      start=start, end=end, limit=1000)
        while True:
            if tok:
                params["page_token"] = tok
            js, err = self.get(f"{DATA_URL}/v1/corporate-actions", params)
            if js is None:
                log.warning(f"Alpaca corporate actions unavailable ({err}): no history is rescaled and no rename "
                            f"followed this run")
                return None, []
            for kind, recs in (js.get("corporate_actions") or {}).items():
                for r in recs or []:
                    if kind == "name_changes":
                        if r.get("old_symbol") and r.get("new_symbol"):
                            renames.append((r["old_symbol"], r["new_symbol"], r.get("process_date", "")))
                        continue
                    sym = r.get("source_symbol") if kind == "spin_offs" else r.get("symbol")
                    d = r.get("ex_date") or r.get("process_date") or ""
                    ratio = None
                    if kind in ("forward_splits", "reverse_splits", "unit_splits") and r.get("new_rate") \
                            and r.get("old_rate"):
                        ratio = float(r["old_rate"]) / float(r["new_rate"])
                    if sym and d:
                        events.setdefault(sym, []).append((d, kind, ratio))
            tok = js.get("next_page_token")
            if not tok:
                break
        for old, new, _ in renames:
            events.setdefault(new, []).extend(events.get(old, []))
        return events, renames


_client = None


def connect(reuse: bool = True) -> Alpaca:
    """A client with the first working key (environment, then ENV_FILES), reused across calls in one process.
    Raises RuntimeError if none works."""
    global _client
    if reuse and _client is not None:
        return _client
    pools = [("environment", dict(os.environ))]
    pools += [(n, _env_file(HERE / n)) for n in ENV_FILES if (HERE / n).exists()]
    tried = []
    for name, pool in pools:
        key = next((pool[n] for n in KEY_NAMES if pool.get(n)), None)
        sec = next((pool[n] for n in SECRET_NAMES if pool.get(n)), None)
        if not (key and sec):
            continue
        api = Alpaca(key, sec)
        js, err = api.get(f"{DATA_URL}/v2/stocks/bars",
                          dict(symbols="AAPL", timeframe="1Day", start="2024-01-02", end="2024-01-03", feed="sip"),
                          tries=2)
        if js is not None:
            log.info(f"Alpaca: using the key from {name}")
            _client = api
            return api
        tried.append(f"{name}: {err}")
    raise RuntimeError("no working Alpaca key (set APCA_API_KEY_ID / APCA_API_SECRET_KEY or add .alpaca2.env); "
                       + "; ".join(tried))


def _frame(bars) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"], index=pd.DatetimeIndex([], name="Date"))
    df = pd.DataFrame(bars)
    t = pd.to_datetime(df["t"], utc=True).dt.tz_convert(NY)
    df.index = pd.DatetimeIndex(t.dt.normalize().dt.tz_localize(None), name="Date")
    return df.rename(columns=dict(o="Open", h="High", l="Low", c="Close", v="Volume"))[
        ["Open", "High", "Low", "Close", "Volume"]].astype(float)


# ------------------------------------------------------------------ files -------------------------------------------
def key_of(path: Path) -> str:
    return path.stem.split(".", 1)[1]


def symbol_of(key: str) -> str:
    """Alpaca symbol for a file key (the export writes '/' as '~' and Windows reserved names with a trailing '_')."""
    s = key.replace("~", "/")
    return s[:-1] if re.fullmatch(r"(CON|PRN|AUX|NUL|COM\d|LPT\d)_", s) else s


def _last_dates(paths, n=2) -> dict:
    """{path: [last n dates as 'YYYY-MM-DD']} read from each file's tail (fast; no full parse)."""
    out = {}
    for p in paths:
        try:
            with open(p, "rb") as f:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - 600))
                lines = [ln for ln in f.read().decode("utf-8", "replace").splitlines() if ln[:4].isdigit()]
            out[p] = [ln[:10] for ln in lines[-n:]]
        except OSError:
            out[p] = []
    return out


def _read(p: Path) -> pd.DataFrame:
    df = pd.read_csv(p, parse_dates=["Date"], dtype={"Ticker": str}, keep_default_na=False, na_values=[""])
    return df.set_index("Date").sort_index()


def _write(p: Path, df: pd.DataFrame, ticker: str):
    out = df[["Open", "High", "Low", "Close", "Volume"]].copy()
    out = out[~((out["Volume"] <= 0) & (out["High"] == out["Low"]))]          # no-trade bars, as in the export
    out["Volume"] = out["Volume"].fillna(0).round().astype("int64")
    out["Ticker"] = ticker
    tmp = p.with_suffix(".tmp")
    out.reset_index().to_csv(tmp, index=False, date_format="%Y-%m-%d")
    os.replace(tmp, p)


def _explain(measured, events, sym, ref):
    """(price_ratio, volume_ratio, why) for an overlap close that moved by `measured`, or (None, None, why) when
    Alpaca's corporate-actions list has nothing after `ref` that explains it. Splits must match their listed ratio
    within SPLIT_TOL (the listed ratio is then used exactly); a spin-off or stock dividend explains a further drop."""
    if events is None:
        return None, None, "Alpaca's corporate-actions list was unavailable"
    evs = [e for e in events.get(sym, []) if e[0] > ref.strftime("%Y-%m-%d")]
    if not evs:
        return None, None, "no split / spin-off listed for it"
    what = ", ".join(f"{k.rstrip('s').replace('_', ' ')} {d}" for d, k, _ in evs)
    exp = float(np.prod([e[2] for e in evs if e[2] is not None])) if any(e[2] for e in evs) else 1.0
    if all(e[2] is not None for e in evs):
        if abs(measured / exp - 1) <= SPLIT_TOL:
            return exp, 1.0 / exp, what
        return None, None, f"{what} listed but the price moved x{measured:.4g}, not x{exp:.4g}"
    rest = measured / exp                    # spin-off / stock dividend: earlier prices drop by the distribution
    if 0.05 < rest < 1.0 + BASIS_TOL:
        return measured, 1.0 / exp, what
    return None, None, f"{what} listed but the price moved x{measured:.4g}"


def _same_company(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    """True if two stored series show the same closes (median gap < 1%) on their last common days."""
    j = a[["Close"]].join(b[["Close"]], rsuffix="_b", how="inner").tail(10)
    return len(j) >= 3 and float((j["Close"] / j["Close_b"] - 1).abs().median()) < 0.01


def _freeze(ex, a_dir: Path, q: Path, sym: str, d: str, api) -> str:
    """Move the file of a company whose ticker passes to another one on day d to <EX>.<SYM>@<d>.csv, after adding its
    last bars before d (Alpaca's mapping as of the day before). Returns the new key."""
    df = _read(q)
    asof = (pd.Timestamp(d) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    tail = api.bars([sym], df.index[-1].strftime("%Y-%m-%d"), asof, asof=asof)[sym]
    tail = tail[tail.index < pd.Timestamp(d)]          # from day d on the ticker is the other company's
    last = df.index[-1]
    if len(tail) and last in tail.index and abs(tail.at[last, "Close"] / df.at[last, "Close"] - 1) <= BASIS_TOL:
        df = pd.concat([df[df.index < tail.index.min()], tail[["Open", "High", "Low", "Close", "Volume"]]])
    key = f"{sym}@{d}"
    _write(a_dir / f"{ex}.{key}.csv", df, key)     # Ticker = the key, as the export writes it: a separate security
    q.unlink()
    return key


def _drop_unfinished(df: pd.DataFrame) -> pd.DataFrame:
    """Leave out today's bar until the New York trading day (incl. after-hours) is over, and flat zero-volume
    no-trade bars (the export drops them too)."""
    df = df[~((df["Volume"] <= 0) & (df["High"] == df["Low"]))]
    now = pd.Timestamp.now(tz=NY)
    if now.hour >= 20:
        return df
    return df[df.index < now.normalize().tz_localize(None)]


# ------------------------------------------------------------------ update ------------------------------------------
def update_exchange(ex: str, a_dir: Path, api: Alpaca, end: str = None, universe_csv: Path = None, keys=None,
                    progress_callback=None, cancel_event=None) -> dict:
    """Extend <EX>/alpaca/ (see module docstring). Returns counts.
    keys: only these tickers (case-insensitive; renames touching them are still followed, and one with no file yet
    is fetched in full if Alpaca has it) -- the fast path for a few selected tickers; None = every still-trading key.
    progress_callback(done_batches, total_batches, skipped, unchanged); cancel_event stops between batches."""
    a_dir = Path(a_dir)
    want = {k.upper() for k in keys} if keys is not None else None
    files = sorted(a_dir.glob(f"{ex}.*.csv"))
    if not files:
        raise RuntimeError(f"no Alpaca files in {a_dir} (run TranscriptExtract's alpaca_export once to seed them)")
    t0 = time.time()
    tails = _last_dates(files)
    newest = max((t[-1] for t in tails.values() if t), default=START)
    exits = a_dir.parent / "alpaca_exits.csv"
    trading = {}
    if exits.exists():
        e = pd.read_csv(exits, dtype=str, keep_default_na=False)
        trading = dict(zip(e["key"], e["still_trading"].str.lower() == "true"))
    recent = (pd.Timestamp(newest) - pd.Timedelta(days=ACTIVE_DAYS)).strftime("%Y-%m-%d")
    live = {p: t for p, t in tails.items() if t and "@" not in key_of(p)
            and trading.get(key_of(p), t[-1] >= recent)}
    log.info(f"{ex} Alpaca: {len(files):,} files, {len(live):,} still trading (newest bar {newest})")
    changes = []
    run = datetime.now().strftime("%Y-%m-%d %H:%M")

    # 1. corporate actions since shortly before the newest bar: renames, and the events that justify a rescale
    today = pd.Timestamp.now(tz=NY).strftime("%Y-%m-%d")
    since = (pd.Timestamp(newest) - pd.Timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    events, renames = api.corporate_actions(since, today)
    by_key = {key_of(p): p for p in files}
    renamed = {}
    for old, new, d in sorted(renames, key=lambda r: r[2]):
        if old == new:
            continue                    # company name / CUSIP change only: same ticker, same file
        if want is not None and old.upper() not in want and new.upper() not in want:
            continue
        p = by_key.get(old)
        if p is None or p not in live or "/" in new:
            continue
        q = by_key.get(new)
        if q is not None and q != p and _same_company(_read(q), _read(p)):
            # the export already stored this company under its new name: the old file is a duplicate
            p.unlink()
            live.pop(p)
            changes.append((run, old, "dropped", f"{old} -> {new} on {d}; {new}'s file already holds the same bars"))
            log.info(f"{ex} Alpaca: {old} renamed {new} ({d}); {new} already has its bars -- duplicate {old} removed")
            continue
        first = _read(p).index[0].strftime("%Y-%m-%d")
        got = _drop_unfinished(api.bars([new], first, end)[new])
        if got.empty:
            continue
        if q is not None:
            # the ticker passes to the renamed company: the company that had it keeps its bars as NEW@date (the
            # export's key for an older company under a reused ticker), with its last bars before the change
            frozen = _freeze(ex, a_dir, q, new, d, api)
            live.pop(q, None)
            changes.append((run, frozen, "ticker reused", f"{new} passed to {old}'s company on {d}; the previous "
                                                          f"{new} company kept as {frozen}"))
            log.info(f"{ex} Alpaca: ticker {new} passed to {old}'s company on {d}; previous {new} kept as {frozen}")
        _write(a_dir / f"{ex}.{new}.csv", got, new)
        p.unlink()
        live.pop(p)
        renamed[new] = old
        changes.append((run, new, "renamed", f"{old} -> {new} on {d}; full history refetched from {first}"))
        log.info(f"{ex} Alpaca: {old} renamed {new} ({d}) -- file replaced with {new}'s full history")

    # 2. still-trading keys: bars from the second-to-last stored bar on, grouped by that date
    if want is not None:
        live = {p: t for p, t in live.items() if key_of(p).upper() in want}
    groups = {}
    for p, t in live.items():
        groups.setdefault(t[0], []).append(p)
    updated = rescaled = unchanged = skipped = 0
    batches = [(start, ps[i:i + BATCH]) for start, ps in sorted(groups.items()) for i in range(0, len(ps), BATCH)]
    cancelled = False
    for n, (start, ps) in enumerate(batches, 1):
        if cancel_event is not None and cancel_event.is_set():
            cancelled = True
            log.warning(f"{ex} Alpaca: cancelled after {n - 1}/{len(batches)} batches (files done so far are kept)")
            break
        syms = {p: symbol_of(key_of(p)) for p in ps}
        got = api.bars(list(dict.fromkeys(syms.values())), start, end)
        for p, sym in syms.items():
            new = _drop_unfinished(got.get(sym, _frame([])))
            if new.empty:
                unchanged += 1              # no bar since: delisted after the export, halted, or a holiday stretch
                continue
            old = _read(p)
            ticker = str(old["Ticker"].iloc[-1]) if "Ticker" in old and len(old) else sym
            ref, did_rescale = pd.Timestamp(start), False
            if ref in old.index and new.index.min() > ref:
                # the stored overlap day (a day the stock traded) is not in Alpaca's current history for this ticker:
                # a new security now uses it (ADS ratio change, reorganisation, another company). Can't verify the
                # basis -> don't append.
                skipped += 1
                why = (f"Alpaca's current {sym} bars start {new.index.min().date()}, after the stored "
                       f"{ref.date()}: a new security under this ticker")
                changes.append((run, key_of(p), "skipped", f"{why}; not updated, check by hand"))
                log.warning(f"{ex} Alpaca: {key_of(p)} NOT updated -- {why} (ADS ratio change, reorganisation or "
                            f"another company?)")
                continue
            if ref in old.index and ref in new.index and old.at[ref, "Close"] > 0:
                c0, c1 = old.at[ref, "Close"], new.at[ref, "Close"]
                if abs(c1 / c0 - 1) > BASIS_TOL:
                    ratio, vr, why = _explain(c1 / c0, events, sym, ref)
                    if ratio is None:
                        # no listed split/spin-off explains it: most likely the ticker now belongs to another
                        # company (Alpaca maps symbols as of today). Appending would mix two companies.
                        skipped += 1
                        changes.append((run, key_of(p), "skipped", f"close on {ref.date()} {c0:.4f} -> {c1:.4f}: "
                                                                     f"{why}; not updated, check by hand"))
                        log.warning(f"{ex} Alpaca: {key_of(p)} NOT updated -- Alpaca's close for {ref.date()} "
                                    f"changed {c0:.4f} -> {c1:.4f} and {why} (ticker reused by another company?)")
                        continue
                    old[["Open", "High", "Low", "Close"]] = old[["Open", "High", "Low", "Close"]] * ratio
                    old["Volume"] = old["Volume"] * vr
                    rescaled += 1
                    did_rescale = True
                    changes.append((run, key_of(p), "rescaled",
                                    f"close on {ref.date()} {c0:.4f} -> {c1:.4f}: {why}; history price x{ratio:.6g}, "
                                    f"volume x{vr:.6g}"))
                    log.info(f"{ex} Alpaca: {key_of(p)} history rescaled x{ratio:.6g} ({why})")
            if new.index.max() <= pd.Timestamp(tails[p][-1]) and not did_rescale:
                unchanged += 1
                continue
            merged = pd.concat([old[old.index < new.index.min()], new])
            _write(p, merged, ticker)
            updated += 1
        if progress_callback:
            progress_callback(n, len(batches), skipped, unchanged)
        if n % 20 == 0 or n == len(batches):
            log.info(f"{ex} Alpaca: {n}/{len(batches)} batches | updated {updated:,} | rescaled {rescaled} | "
                     f"{time.time() - t0:.0f}s")

    # 3. new listings: active assets on this exchange with no file that the original download did not know
    #    (keys mode: the wanted tickers that have no file)
    added = 0
    have = {symbol_of(key_of(p)).upper() for p in a_dir.glob(f"{ex}.*.csv")}
    if cancelled:
        new_syms = []
    elif want is not None:
        new_syms = sorted(k for k in want if k not in have and "/" not in k and "@" not in k)
        if new_syms:
            log.info(f"{ex} Alpaca: no file yet for {new_syms[:15]}; fetching their full history if Alpaca has them")
    else:
        new_syms = None
    assets = api.active_assets() if new_syms is None else None
    if new_syms is None and assets is not None and len(assets):
        known = set()
        if universe_csv and Path(universe_csv).exists():
            known = set(pd.read_csv(universe_csv, usecols=["symbol"], dtype=str, keep_default_na=False)["symbol"])
        new_syms = sorted(s for s in assets.loc[assets["exchange"] == ex, "symbol"]
                          if s.upper() not in have and s not in known and s not in renamed and "/" not in s)
        if new_syms:
            log.info(f"{ex} Alpaca: {len(new_syms)} new listing(s): {new_syms[:15]}{' ...' if len(new_syms) > 15 else ''}")
    for i in range(0, len(new_syms or []), BATCH):
        chunk = new_syms[i:i + BATCH]
        got = api.bars(chunk, START, end)
        for s in chunk:
            df = _drop_unfinished(got[s])
            if df.empty:
                continue
            _write(a_dir / f"{ex}.{s}.csv", df, s)
            added += 1
            changes.append((run, s, "new", f"new listing, {len(df)} bars from {df.index[0].date()}"))
    if changes:
        lp = a_dir.parent / "alpaca_updates.csv"
        pd.DataFrame(changes, columns=["run", "key", "action", "detail"]).to_csv(
            lp, mode="a", header=not lp.exists(), index=False)
    log.info(f"{ex} Alpaca done: {updated:,} file(s) extended, {rescaled} rescaled for a split/spin-off, "
             f"{len(renamed)} renamed, {added} new listing(s), {unchanged:,} with no new bar, {skipped} skipped "
             f"(unexplained price change, see alpaca_updates.csv) | {api.calls} requests, {time.time() - t0:.0f}s")
    return dict(updated=updated, rescaled=rescaled, renamed=len(renamed), added=added, unchanged=unchanged,
                skipped=skipped)


def update(root: Path, exchanges=("NASDAQ", "NYSE"), end: str = None, api: Alpaca = None) -> dict:
    api = api or connect()
    root = Path(root)
    uni = HERE / "market_data_alpaca" / "universe.csv"
    return {ex: update_exchange(ex, root / ex / "alpaca", api, end=end, universe_csv=uni) for ex in exchanges}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="market_data_daily")
    ap.add_argument("--exchanges", nargs="+", choices=["NASDAQ", "NYSE"], default=["NASDAQ", "NYSE"])
    ap.add_argument("--end", default=None, help="last day to fetch (default: up to now)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    update(Path(args.root), args.exchanges, args.end)


if __name__ == "__main__":
    main()
