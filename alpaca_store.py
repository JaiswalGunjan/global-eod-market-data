"""Read the US (NASDAQ/NYSE) daily bars straight from the Alpaca store, market_data_alpaca/ (no per-symbol CSV copy).

The store (downloaded and extended by TranscriptExtract's swingbt.data_fetch.alpaca_fetch, data card in
market_data_alpaca/README.md) keeps one pickle per key, AS TRADED, with Alpaca's split / spin-off factors:

    market_data_alpaca/daily/<EX>/<KEY>.pkl     Date index; Open High Low Close Volume (as traded), f_split, f_spinoff, f_all
    market_data_alpaca/meta_<EX>.csv            per key: start / cut (series window), dup_of, exit_price, exit_why, ...
    market_data_alpaca/corp_actions.csv         Alpaca's corporate actions (spin-offs are read from here)
    market_data_daily/<EX>/recon/scale_factors.csv   (optional) Yahoo's adjustment steps, to confirm spin-offs

load(ex, key) returns the series the old <EX>/alpaca_adjusted/ files held (a port of swingbt's data.load_alpaca_daily):
splits and spin-offs back-adjusted to the series' own last bar (dividends not), volume split-adjusted, cut at the
delisting date and after the last > 90-day gap (an older company under a reused ticker is not carried).
basis="raw" returns the same window as traded (no adjustment).

KEY is the ticker, or TICKER@YYYY-MM-DD for an older company under a reused ticker.
"""
import json
import logging
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

import app_paths

log = logging.getLogger("market_dl")

COLS = ["Open", "High", "Low", "Close", "Volume"]
WIN_RESERVED = {"CON", "PRN", "AUX", "NUL"} | {f"{d}{i}" for d in ("COM", "LPT") for i in range(10)}
EXCHANGES = ("NASDAQ", "NYSE")


def store_dir() -> Path:
    return app_paths._path("alpaca_data")


def kpath(ex: str, key: str, root: Path = None) -> Path:
    """File of a key: '/' -> '~', and a Windows reserved name (CON, PRN, NUL ...) gets a trailing '_'."""
    stem = key.replace("/", "~")
    if stem.split(".")[0].split("@")[0].upper() in WIN_RESERVED:
        stem += "_"
    return Path(root or store_dir()) / "daily" / ex / f"{stem}.pkl"


def _read_raw(ex, key, root=None):
    p = kpath(ex, key, root)
    return pd.read_pickle(p) if p.exists() else None


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df[COLS].astype(float)
    df["Volume"] = df["Volume"].fillna(0).clip(lower=0)
    df = df[(df["Close"] > 0) & (df["High"] > 0) & (df["Low"] > 0)]
    df["Open"] = df["Open"].where(df["Open"] > 0, df["Close"])
    df["High"] = df[["High", "Open", "Close"]].max(axis=1)
    df["Low"] = df[["Low", "Open", "Close"]].min(axis=1)
    return df


class AlpacaStore:
    """The Alpaca store of one machine: root = market_data_alpaca/, daily_root = market_data_daily/ (for the optional
    Yahoo spin-off confirmation)."""

    def __init__(self, root: Path = None, daily_root: Path = None):
        self.root = Path(root) if root else store_dir()
        self.daily_root = Path(daily_root) if daily_root else app_paths.daily_output()
        self._meta, self._spin, self._ysteps, self._children = {}, None, {}, {}

    # ---- tables ------------------------------------------------------------------------------------------------
    def available(self, ex: str) -> bool:
        return (self.root / f"meta_{ex}.csv").exists() and (self.root / "daily" / ex).is_dir()

    def meta(self, ex: str) -> pd.DataFrame:
        if ex not in self._meta:
            m = pd.read_csv(self.root / f"meta_{ex}.csv", keep_default_na=False, parse_dates=["first", "last"]) \
                .set_index("key")
            for c in ("f_last", "last_close", "after_close", "exit_price"):
                m[c] = pd.to_numeric(m[c], errors="coerce")
            self._meta[ex] = m
        return self._meta[ex]

    def keys(self, ex: str) -> list:
        """Every key with bars (delisted companies included), duplicates of another key dropped."""
        m = self.meta(ex)
        return sorted(m.index[m["dup_of"] == ""])

    def spinoffs(self) -> dict:
        """source symbol -> [(ex-date, new symbol, new shares per source share)] from Alpaca's corporate actions."""
        if self._spin is None:
            out = {}
            c = pd.read_csv(self.root / "corp_actions.csv", usecols=["type", "record"])
            for rec in c.loc[c.type == "spin_offs", "record"]:
                x = json.loads(rec)
                try:
                    out.setdefault(x["source_symbol"], []).append(
                        (pd.Timestamp(x["ex_date"]), x["new_symbol"],
                         float(x["new_rate"]) / float(x.get("source_rate") or 1)))
                except (KeyError, TypeError, ValueError):
                    continue
            self._spin = out
        return self._spin

    def yahoo_steps(self, ex: str) -> dict:
        """symbol -> [(date, step)] from <daily>/<EX>/recon/scale_factors.csv (as traded / Yahoo)."""
        if ex not in self._ysteps:
            out, p = {}, self.daily_root / ex / "recon" / "scale_factors.csv"
            if p.exists():
                sf = pd.read_csv(p, keep_default_na=False).sort_values(["Symbol", "From"])
                for s, g in sf.groupby("Symbol"):
                    f = g["Factor"].astype(float).to_numpy()
                    for k in range(1, len(g)):
                        if f[k] > 0 and abs(f[k - 1] / f[k] - 1) > 0.005:
                            out.setdefault(s, []).append((pd.Timestamp(g["From"].iloc[k]), float(f[k - 1] / f[k])))
            self._ysteps[ex] = out
        return self._ysteps[ex]

    # ---- bars --------------------------------------------------------------------------------------------------
    def _child_first_close(self, new_symbol, d):
        """As-traded close of a spun-off company on its first bar on or after the ex-date (within 5 days)."""
        for ex2 in EXCHANGES:
            for key in dict.fromkeys([new_symbol, new_symbol.split(".")[0]]):
                raw = _read_raw(ex2, key, self.root)
                if raw is None or raw.empty:
                    continue
                r = raw[(raw.index >= d) & (raw.index <= d + pd.Timedelta(days=5)) & (raw["Close"] > 0)]
                if len(r):
                    return float(r["Close"].iloc[0])
        return None

    def _spin_factor(self, ex, sym, raw):
        """Spin-off factor per bar (raw / adjusted for spin-offs only; 1 after the last event). Each Alpaca spin-off of
        the symbol: k = P / (P - ratio x C), P = the parent's last close before the ex-date, C = the new company's
        first close (Alpaca's own step if C is unknown); Yahoo's step for the same event (within 3 days) is preferred
        when it exists. A Yahoo step that is neither an Alpaca split nor one of these spin-offs is applied as well
        (not for a reused ticker, key SYMBOL@date)."""
        base = sym.split("@")[0]
        events = []
        split_steps = raw["f_split"].ffill().bfill()
        split_dates = raw.index[(split_steps.shift(1) / split_steps - 1).abs() > 0.005]
        ysteps = self.yahoo_steps(ex).get(base, []) if "@" not in sym else []
        for d, new, ratio in self.spinoffs().get(base, []):
            before = raw[(raw.index < d) & (raw["Close"] > 0)]
            if before.empty or d > raw.index[-1]:
                continue
            p = float(before["Close"].iloc[-1])
            c = self._child_first_close(new, d)
            ys = [st for yd, st in ysteps if abs((yd - d).days) <= 3]
            if ys and 1.0 < ys[0] < 2.0:
                events.append((d, ys[0]))
            elif c is not None and 0 < ratio * c < 0.9 * p:
                events.append((d, p / (p - ratio * c)))
            else:
                fs = raw["f_spinoff"].ffill().bfill()
                i = raw.index.searchsorted(d)
                if 0 < i < len(raw) and abs(fs.iloc[i - 1] / fs.iloc[i] - 1) > 0.005:
                    events.append((d, float(fs.iloc[i - 1] / fs.iloc[i])))
        if "@" not in sym:
            for d, step in self.yahoo_steps(ex).get(base, []):
                near = lambda ds: len(ds) and min(abs((x - d).days) for x in ds) <= 3     # noqa: E731
                if near(split_dates) or near([e[0] for e in events]) or not (raw.index[0] < d <= raw.index[-1]):
                    continue
                if 1.005 < step < 2.0:                     # a price drop the size of a spin-off, not a split remnant
                    events.append((d, step))
        f = pd.Series(1.0, index=raw.index)
        for d, k in events:
            f[f.index < d] *= k
        return f

    def load(self, ex: str, key: str, basis: str = "adjusted"):
        """Daily bars of one key (Date index; Open High Low Close Volume), or None when it has none.
        basis "adjusted": splits and spin-offs adjusted; "raw": as traded."""
        d = _read_raw(ex, key, self.root)
        if d is None or len(d) == 0:
            return None
        m = self.meta(ex)
        if key in m.index:
            if m.at[key, "cut"]:
                d = d[d.index <= pd.Timestamp(m.at[key, "cut"])]
            d = d[d.index >= pd.Timestamp(m.at[key, "start"])]
        if d.empty:
            return None
        if basis == "raw":
            out = clean(d[COLS].astype(float))
        else:
            fs = d["f_split"].ffill().bfill().fillna(1.0)
            fs = fs / fs.iloc[-1]                  # 1 at the series' own last bar (Alpaca adjusts to today)
            f = fs * self._spin_factor(ex, key, d)
            out = d[COLS].astype(float).copy()
            for k in ("Open", "High", "Low", "Close"):
                out[k] = out[k] / f
            out["Volume"] = out["Volume"] * fs
            out = clean(out)
        out.index.name = "Date"
        return out

    def frame(self, ex: str, key: str, basis: str = "adjusted"):
        """load() as a build-ready table: Date, Open, High, Low, Close, Volume, Ticker (None when empty)."""
        d = self.load(ex, key, basis)
        if d is None or d.empty:
            return None
        d = d.reset_index()
        d["Ticker"] = key
        return d

    # ---- exits -------------------------------------------------------------------------------------------------
    def write_exits(self, ex: str, path: Path):
        """<EX>/alpaca_exits.csv: what a holder received after each key's last bar (cash / stock merger value, 0 for a
        bankruptcy, else the last close). Same columns as the old export; still_trading = a bar within 7 days of the
        newest bar of the exchange."""
        m = self.meta(ex)
        m = m[m["dup_of"] == ""].copy()
        end = m["last"].max()
        m["still_trading"] = m["last"] >= end - pd.Timedelta(days=7)
        x = m.reset_index()[["key", "symbol", "status", "first", "last", "bars", "delisted", "cut", "still_trading",
                             "exit_price", "exit_why"]]
        x["first"], x["last"] = x["first"].dt.date, x["last"].dt.date
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        x.to_csv(path, index=False, lineterminator="\n")
        return len(x)
