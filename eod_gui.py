#!/usr/bin/env python3
"""
Market Data Downloader -- GUI
==============================

A simple desktop window on top of eod_downloader.py that lets you pick:

    1. Market(s)   - checkboxes for the supported exchanges (default:
                     NASDAQ + NYSE checked)
    2. Ticker      - a specific symbol, or "ALL" (default) for every ticker
                     in the selected market(s), with type-to-filter
    3. Date range  - start / end (default: last 10 years -> today)
    4. CSV layout  - one file per day / per symbol / one combined file / all
    5. Save folder - any location on disk, via a folder picker

--------------------------------------------------------------------------
SETUP
--------------------------------------------------------------------------
This file MUST sit in the same folder as eod_downloader.py -- it
imports and reuses all of its download/build logic directly.

    pip install yfinance pandas requests
    pip install tkcalendar   # optional: enables the calendar date picker;
                              # without it, dates fall back to typed YYYY-MM-DD fields

tkinter ships with the standard python.org installer on Windows and macOS,
so nothing extra to install there. On Linux you may need:
    sudo apt install python3-tk

--------------------------------------------------------------------------
RUN
--------------------------------------------------------------------------
    python eod_gui.py

--------------------------------------------------------------------------
NOTES
--------------------------------------------------------------------------
- Picking a specific ticker skips fetching the full symbol directory
  entirely -- it just downloads that one symbol, so it's fast.
- Picking "ALL" downloads every ticker in the checked market(s); this is
  the same multi-hour operation as running eod_downloader.py
  directly, just with a progress bar instead of a scrolling console.
- Ticker lists (used to populate the search box and for "ALL" runs) are
  cached per exchange in a .ticker_cache folder next to this script,
  separate from wherever you choose to save your data. Use "Refresh
  ticker list" periodically -- new listings/delistings happen constantly.
- Closing the window while a download is running stops it; whatever was
  already downloaded stays cached, so re-running later resumes from there.
"""

import json
import logging
import os
import queue
import subprocess
import sys
import threading
import tkinter as tk
from datetime import date, datetime, timedelta
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

import pandas as pd

import eod_downloader as core  # reuse all download/build logic

try:
    from tkcalendar import DateEntry
    HAS_TKCALENDAR = True
except ImportError:
    HAS_TKCALENDAR = False  # falls back to typed YYYY-MM-DD fields, see date frame below

CACHE_DIR = Path(__file__).resolve().parent / ".ticker_cache"
CACHE_DIR.mkdir(exist_ok=True)
SETTINGS_PATH = CACHE_DIR / "gui_settings.json"  # remembered GUI choices, see _save_settings()
EARLIEST_DATE = date(1990, 1, 1)  # what the "Max" date preset starts from


def _years_ago(years: int) -> date:
    today = date.today()
    try:
        return today.replace(year=today.year - years)
    except ValueError:  # Feb 29 -> a non-leap target year
        return today.replace(year=today.year - years, day=28)


# --------------------------------------------------------------------------
# Bridge: core module's logging -> GUI log box (thread-safe via a queue)
# --------------------------------------------------------------------------

class QueueLogHandler(logging.Handler):
    """Pushes formatted log records into a queue the GUI polls on a timer."""

    def __init__(self, log_queue: queue.Queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(self.format(record))


def _open_folder(path: Path):
    """Open a folder in the OS file browser. Best-effort, never raises."""
    try:
        if sys.platform.startswith("win"):
            os.startfile(str(path))  # noqa: S606 (Windows-only API)
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])
    except Exception:
        pass


# --------------------------------------------------------------------------
# Main window
# --------------------------------------------------------------------------

class MarketDataGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("NYSE / NASDAQ Market Data Downloader")
        # Clamp to the screen so the window (and its Download button) can
        # never start taller/wider than the display; the settings area
        # scrolls, so a small window is still fully usable.
        w = min(820, self.winfo_screenwidth() - 80)
        h = min(920, self.winfo_screenheight() - 120)
        self.geometry(f"{w}x{h}")
        self.minsize(640, 480)

        self.log_queue: queue.Queue = queue.Queue()
        # {"NASDAQ": {"AAPL": "Apple Inc. - Common Stock", ...}, "NYSE": {...},
        #  "BSE": {...}, "NSE": {...}} -- populated by "Refresh ticker list"
        # (or "Load tickers from file..." for BSE); used for both the
        # ALL-mode download and for symbol/company-name search.
        self.all_tickers = {ex: {} for ex in core.SUPPORTED_EXCHANGES}
        # Live search results, aligned by index with self.filter_listbox's rows:
        # [(symbol, name, exchange), ...]
        self.filtered_entries = []
        # The user's multi-selection, insertion-ordered: {symbol: (name, exchange)}.
        # exchange may be None for a manually-typed ticker we don't have data for.
        self.selected_tickers = {}
        self.is_running = False
        self.cancel_event = threading.Event()
        self.last_output_dir = None

        self._build_widgets()
        self._load_cached_tickers()
        self._load_settings()
        self._update_ticker_widgets_state()
        self._update_ticker_status()
        self._attach_log_handler()
        self._poll_log_queue()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    # ---------------------------------------------------------------- UI --

    def _build_widgets(self):
        pad = {"padx": 10, "pady": 6}

        # Window layout, top to bottom:
        #   - pinned bottom bar (Download / Cancel / progress / status): packed
        #     FIRST with side="bottom" so it keeps its space at any window size
        #   - a vertical paned window filling the rest: scrollable settings
        #     (sections 1-5) on top, the log underneath; drag the divider to
        #     resize, and both grow with the window.
        bottom_bar = ttk.Frame(self)
        bottom_bar.pack(side="bottom", fill="x")

        run_frame = ttk.Frame(bottom_bar)
        run_frame.pack(fill="x", **pad)
        self.run_button = ttk.Button(run_frame, text="Download", command=self._start_download)
        self.run_button.pack(side="left", padx=10)
        self.cancel_button = ttk.Button(run_frame, text="Cancel", command=self._cancel_download,
                                         state="disabled")
        self.cancel_button.pack(side="left")
        self.progress = ttk.Progressbar(run_frame, mode="determinate")
        self.progress.pack(side="left", fill="x", expand=True, padx=10)

        self.status_var = tk.StringVar(value="Ready.")
        ttk.Label(bottom_bar, textvariable=self.status_var).pack(fill="x", padx=20, pady=(0, 6))

        paned = ttk.PanedWindow(self, orient="vertical")
        paned.pack(fill="both", expand=True)

        settings_outer = ttk.Frame(paned)
        paned.add(settings_outer, weight=3)
        self._settings_canvas = tk.Canvas(settings_outer, highlightthickness=0)
        settings_scroll = ttk.Scrollbar(settings_outer, orient="vertical",
                                         command=self._settings_canvas.yview)
        self._settings_canvas.configure(yscrollcommand=settings_scroll.set)
        settings_scroll.pack(side="right", fill="y")
        self._settings_canvas.pack(side="left", fill="both", expand=True)
        body = ttk.Frame(self._settings_canvas)
        body_window = self._settings_canvas.create_window((0, 0), window=body, anchor="nw")
        body.bind("<Configure>", lambda e: self._settings_canvas.configure(
            scrollregion=self._settings_canvas.bbox("all")))
        self._settings_canvas.bind("<Configure>", lambda e: self._settings_canvas.itemconfigure(
            body_window, width=e.width))
        self.bind_all("<MouseWheel>", self._on_mousewheel)

        # 1. Market
        market_frame = ttk.LabelFrame(body, text="1. Market")
        market_frame.pack(fill="x", **pad)
        # US markets checked by default (matches prior behavior); everything
        # else off by default since most need extra setup (bulk fetch isn't
        # auto-fetchable -- see "Load tickers from file...") rather than
        # "just working" the way NASDAQ/NYSE/NSE do.
        self.market_vars = {ex: tk.BooleanVar(value=(ex in ("NASDAQ", "NYSE")))
                             for ex in core.SUPPORTED_EXCHANGES}
        PER_ROW = 6
        exchanges = list(self.market_vars.items())
        for row_start in range(0, len(exchanges), PER_ROW):
            row_frame = ttk.Frame(market_frame)
            row_frame.pack(fill="x")
            for ex, var in exchanges[row_start:row_start + PER_ROW]:
                ttk.Checkbutton(row_frame, text=ex, variable=var,
                                 command=self._on_market_change).pack(side="left", padx=10, pady=4)

        button_row = ttk.Frame(market_frame)
        button_row.pack(fill="x", pady=(4, 6))
        self.refresh_button = ttk.Button(button_row, text="Refresh ticker list",
                                          command=self._refresh_tickers_async)
        self.refresh_button.pack(side="right", padx=10)
        self.load_file_button = ttk.Button(button_row, text="Load tickers from file...",
                                            command=self._load_tickers_from_file)
        self.load_file_button.pack(side="right", padx=(10, 0))

        self.bse_bhavcopy_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            market_frame,
            text="BSE prices from BSE's official daily files (complete, incl. BSE-only stocks; "
                 "slower first run) instead of Yahoo",
            variable=self.bse_bhavcopy_var,
        ).pack(anchor="w", padx=10, pady=(0, 2))
        self.bse_adjust_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            market_frame,
            text="   ...and adjust those BSE prices for splits/bonuses (via Yahoo's corporate-actions data)",
            variable=self.bse_adjust_var,
        ).pack(anchor="w", padx=10, pady=(0, 6))

        # 2. Ticker(s)
        ticker_frame = ttk.LabelFrame(body, text="2. Ticker(s)")
        ticker_frame.pack(fill="x", **pad)

        top_row = ttk.Frame(ticker_frame)
        top_row.pack(fill="x", padx=10, pady=(8, 4))
        self.ticker_all_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(top_row, text="ALL (every ticker in the selected market(s))",
                         variable=self.ticker_all_var,
                         command=self._on_ticker_all_toggle).pack(side="left")
        self.ticker_status = ttk.Label(top_row, text="")
        self.ticker_status.pack(side="left", padx=20)

        search_row = ttk.Frame(ticker_frame)
        search_row.pack(fill="x", padx=10, pady=4)
        ttk.Label(search_row, text="Search symbol or company name:").pack(side="left")
        self.ticker_search_var = tk.StringVar()
        self.search_entry = ttk.Entry(search_row, textvariable=self.ticker_search_var, width=30)
        self.search_entry.pack(side="left", padx=(6, 4))
        self.search_entry.bind("<KeyRelease>", self._on_search_keyrelease)
        self.search_entry.bind("<Return>", self._on_search_entry_return)
        self.search_button = ttk.Button(search_row, text="🔍", width=3,
                                         command=self._search_ticker_button)
        self.search_button.pack(side="left")

        lists_row = ttk.Frame(ticker_frame)
        lists_row.pack(fill="x", padx=10, pady=(4, 8))

        left_col = ttk.Frame(lists_row)
        left_col.pack(side="left", fill="both", expand=True)
        ttk.Label(left_col, text="Matches -- select + Add, or double-click").pack(anchor="w")
        lf = ttk.Frame(left_col)
        lf.pack(fill="both", expand=True)
        self.filter_listbox = tk.Listbox(lf, height=6, selectmode="extended", exportselection=False)
        self.filter_listbox.pack(side="left", fill="both", expand=True)
        fscroll = ttk.Scrollbar(lf, command=self.filter_listbox.yview)
        fscroll.pack(side="left", fill="y")
        self.filter_listbox.config(yscrollcommand=fscroll.set)
        self.filter_listbox.bind("<Double-Button-1>", self._on_filter_double_click)

        mid_col = ttk.Frame(lists_row)
        mid_col.pack(side="left", padx=8)
        self.add_button = ttk.Button(mid_col, text="Add ▸", command=self._add_filter_selection)
        self.add_button.pack(pady=4, fill="x")
        self.remove_button = ttk.Button(mid_col, text="◂ Remove", command=self._remove_selected_selection)
        self.remove_button.pack(pady=4, fill="x")
        self.clear_button = ttk.Button(mid_col, text="Clear all", command=self._clear_selected)
        self.clear_button.pack(pady=4, fill="x")

        right_col = ttk.Frame(lists_row)
        right_col.pack(side="left", fill="both", expand=True)
        ttk.Label(right_col, text="Selected tickers").pack(anchor="w")
        rf = ttk.Frame(right_col)
        rf.pack(fill="both", expand=True)
        self.selected_listbox = tk.Listbox(rf, height=6, selectmode="extended", exportselection=False)
        self.selected_listbox.pack(side="left", fill="both", expand=True)
        sscroll = ttk.Scrollbar(rf, command=self.selected_listbox.yview)
        sscroll.pack(side="left", fill="y")
        self.selected_listbox.config(yscrollcommand=sscroll.set)
        self.selected_listbox.bind("<Double-Button-1>", self._on_selected_double_click)

        # 3. Date range
        date_frame = ttk.LabelFrame(body, text="3. Date range")
        date_frame.pack(fill="x", **pad)
        date_row = ttk.Frame(date_frame)
        date_row.pack(fill="x")
        today = datetime.today()
        default_start = _years_ago(10)  # matches the README's "last 10 years"

        # HAS_TKCALENDAR only means the import succeeded -- actually
        # constructing a DateEntry can still fail at runtime for reasons
        # like a missing 'babel' dependency (tkcalendar needs it for
        # locale-aware month/day names in some versions). Try it for real
        # here so a runtime failure falls back to plain text fields instead
        # of crashing the whole window.
        self.has_date_picker = False
        date_picker_error = None
        if HAS_TKCALENDAR:
            try:
                probe = DateEntry(date_frame)
                probe.destroy()
                self.has_date_picker = True
            except Exception as e:
                date_picker_error = str(e)

        ttk.Label(date_row, text="Start:").pack(side="left", padx=10)
        if self.has_date_picker:
            self.start_date_widget = DateEntry(date_row, width=12, date_pattern="yyyy-mm-dd")
            self.start_date_widget.set_date(default_start)
            self.start_date_widget.pack(side="left", pady=8)
            self.start_var = None
        else:
            self.start_var = tk.StringVar(value=default_start.strftime("%Y-%m-%d"))
            ttk.Entry(date_row, textvariable=self.start_var, width=12).pack(side="left", pady=8)

        ttk.Label(date_row, text="End:").pack(side="left", padx=10)
        if self.has_date_picker:
            self.end_date_widget = DateEntry(date_row, width=12, date_pattern="yyyy-mm-dd")
            self.end_date_widget.set_date(today.date())
            self.end_date_widget.pack(side="left", pady=8)
            self.end_var = None
        else:
            self.end_var = tk.StringVar(value=today.strftime("%Y-%m-%d"))
            ttk.Entry(date_row, textvariable=self.end_var, width=12).pack(side="left", pady=8)

        # Quick ranges: set start to N years back (or Jan 1 / the earliest
        # date) and end to today.
        preset_row = ttk.Frame(date_frame)
        preset_row.pack(fill="x", pady=(0, 6))
        ttk.Label(preset_row, text="Quick range:").pack(side="left", padx=10)
        presets = [
            ("1Y", lambda: _years_ago(1)),
            ("5Y", lambda: _years_ago(5)),
            ("10Y", lambda: _years_ago(10)),
            ("YTD", lambda: date(date.today().year, 1, 1)),
            ("Max", lambda: EARLIEST_DATE),
        ]
        for label, start_fn in presets:
            ttk.Button(preset_row, text=label, width=5,
                       command=lambda fn=start_fn: self._set_dates(fn(), date.today())
                       ).pack(side="left", padx=2)

        if not self.has_date_picker:
            msg = "(install 'tkcalendar' for a calendar picker: pip install tkcalendar)"
            if HAS_TKCALENDAR and date_picker_error:
                # It IS installed but failed to actually build -- surface why,
                # since "pip install tkcalendar" won't fix this case.
                msg = f"(calendar picker unavailable: {date_picker_error} -- using plain date fields)"
            ttk.Label(date_row, text=msg).pack(side="left", padx=10)

        # 4. CSV layout
        layout_frame = ttk.LabelFrame(body, text="4. CSV layout")
        layout_frame.pack(fill="x", **pad)
        self.layout_var = tk.StringVar(value="combined")
        options = [
            ("One file per day", "daily"),
            ("One file per symbol", "per-symbol"),
            ("Single combined file", "combined"),
            ("All of the above", "all"),
        ]
        radio_row = ttk.Frame(layout_frame)
        radio_row.pack(fill="x")
        for text, val in options:
            ttk.Radiobutton(radio_row, text=text, variable=self.layout_var, value=val).pack(
                side="left", padx=8, pady=(8, 2)
            )
        self.metastock_ascii_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            layout_frame,
            text="MetaStock ASCII format (<TICKER>,<PER>,<DTYYYYMMDD>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>)",
            variable=self.metastock_ascii_var,
        ).pack(anchor="w", padx=8, pady=(2, 2))
        self.bhavcopy_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            layout_frame,
            text="Also write per-day bhavcopy files for MetaStock (bhavcopy/<EXCHANGE>/<EXCHANGE>_YYYYMMDD.csv)",
            variable=self.bhavcopy_var,
        ).pack(anchor="w", padx=8, pady=(2, 2))
        self.split_by_exchange_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            layout_frame,
            text="Split into separate NASDAQ/ and NYSE/ folders (each with its own raw + combined output)",
            variable=self.split_by_exchange_var,
        ).pack(anchor="w", padx=8, pady=(2, 8))

        # 5. Save location
        out_frame = ttk.LabelFrame(body, text="5. Save location")
        out_frame.pack(fill="x", **pad)
        self.output_var = tk.StringVar(value=str(Path.cwd() / "market_data"))
        ttk.Entry(out_frame, textvariable=self.output_var).pack(
            side="left", padx=(10, 6), pady=8, fill="x", expand=True
        )
        ttk.Button(out_frame, text="Browse...", command=self._browse_output).pack(side="left", padx=4)
        ttk.Button(out_frame, text="Open folder", command=self._open_output_folder).pack(
            side="left", padx=(4, 10)
        )

        # Log output (bottom pane of the paned window; the Download/Cancel bar
        # is pinned separately at the very bottom, see top of this method)
        log_frame = ttk.LabelFrame(paned, text="Log")
        paned.add(log_frame, weight=1)
        log_scroll = ttk.Scrollbar(log_frame)
        log_scroll.pack(side="right", fill="y")
        self.log_text = tk.Text(log_frame, height=8, state="disabled", wrap="word",
                                 yscrollcommand=log_scroll.set)
        self.log_text.pack(fill="both", expand=True, padx=(6, 0), pady=6)
        log_scroll.config(command=self.log_text.yview)

    # ----------------------------------------------- Scrolling / settings --

    def _on_mousewheel(self, event):
        """Scroll the settings pane with the mouse wheel -- but only when the
        pointer is over it and not over a Listbox/Text, which scroll
        themselves (and would otherwise scroll twice)."""
        w = self.winfo_containing(event.x_root, event.y_root)
        if w is None or isinstance(w, (tk.Listbox, tk.Text)):
            return
        p = w
        while p is not None and p is not self._settings_canvas:
            p = getattr(p, "master", None)
        if p is None:
            return
        bbox = self._settings_canvas.bbox("all")
        if bbox and bbox[3] > self._settings_canvas.winfo_height():
            self._settings_canvas.yview_scroll(int(-event.delta / 120), "units")

    def _load_cached_tickers(self):
        """Populate the symbol/name search from the on-disk ticker caches at
        startup, so search works immediately (incl. BSE) instead of needing
        'Refresh ticker list' every session."""
        for ex in core.SUPPORTED_EXCHANGES:
            path = CACHE_DIR / f"{ex.lower()}_tickers.csv"
            if not path.exists():
                continue
            try:
                df = pd.read_csv(path, dtype=str).fillna("")
                names = df["Name"] if "Name" in df.columns else [""] * len(df)
                self.all_tickers[ex] = dict(zip(df["Symbol"], names))
            except Exception:
                continue  # unreadable/odd cache: just skip, Refresh rebuilds it

    def _collect_settings(self) -> dict:
        today_str = date.today().strftime("%Y-%m-%d")
        end_s = self._get_end_date_str()
        return {
            "markets": self._selected_markets(),
            "all": self.ticker_all_var.get(),
            "selected": {s: [n, ex] for s, (n, ex) in self.selected_tickers.items()},
            "start": self._get_start_date_str(),
            "end": end_s,
            "end_is_today": end_s == today_str,  # so a saved "today" doesn't go stale
            "layout": self.layout_var.get(),
            "metastock_ascii": self.metastock_ascii_var.get(),
            "bhavcopy": self.bhavcopy_var.get(),
            "split_by_exchange": self.split_by_exchange_var.get(),
            "bse_bhavcopy": self.bse_bhavcopy_var.get(),
            "bse_adjust": self.bse_adjust_var.get(),
            "output": self.output_var.get(),
            "size": [self.winfo_width(), self.winfo_height()],
        }

    def _save_settings(self):
        try:
            SETTINGS_PATH.write_text(json.dumps(self._collect_settings(), indent=2), encoding="utf-8")
        except Exception:
            pass  # remembering settings is a convenience; never block on it

    def _load_settings(self):
        try:
            s = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
        except Exception:
            return  # first run / unreadable file: keep the defaults
        try:
            for ex, var in self.market_vars.items():
                var.set(ex in s.get("markets", []))
            self.ticker_all_var.set(bool(s.get("all", True)))
            self.selected_tickers = {sym: (n, ex) for sym, (n, ex) in s.get("selected", {}).items()}
            self._refresh_selected_listbox()
            start = datetime.strptime(s["start"], "%Y-%m-%d").date()
            end = date.today() if s.get("end_is_today") else datetime.strptime(s["end"], "%Y-%m-%d").date()
            self._set_dates(start, end)
            self.layout_var.set(s.get("layout", "combined"))
            self.metastock_ascii_var.set(bool(s.get("metastock_ascii", False)))
            self.bhavcopy_var.set(bool(s.get("bhavcopy", True)))
            self.split_by_exchange_var.set(bool(s.get("split_by_exchange", False)))
            self.bse_bhavcopy_var.set(bool(s.get("bse_bhavcopy", False)))
            self.bse_adjust_var.set(bool(s.get("bse_adjust", True)))
            if s.get("output"):
                self.output_var.set(s["output"])
            if s.get("size"):
                w = min(int(s["size"][0]), self.winfo_screenwidth() - 40)
                h = min(int(s["size"][1]), self.winfo_screenheight() - 80)
                if w >= 640 and h >= 480:
                    self.geometry(f"{w}x{h}")
            self._apply_ticker_filter()
        except Exception:
            pass  # a stale/partial settings file must not stop the app starting

    # ---------------------------------------------------------- Logging --

    def _attach_log_handler(self):
        handler = QueueLogHandler(self.log_queue)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S"))
        core.log.addHandler(handler)
        core.log.setLevel(logging.INFO)

    def _poll_log_queue(self):
        while True:
            try:
                msg = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self._append_log(msg)
        self.after(200, self._poll_log_queue)

    def _append_log(self, msg: str):
        self.log_text.config(state="normal")
        self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")
        self.log_text.config(state="disabled")

    # ------------------------------------------------------------ Market --

    def _selected_markets(self):
        return [ex for ex, var in self.market_vars.items() if var.get()]

    def _on_market_change(self):
        self._apply_ticker_filter()
        self._update_ticker_status()

    # ------------------------------------------------------------ Ticker --

    ENTRY_SEP = " — "  # separates "SYMBOL" from "Company Name" in list entries

    def _format_entry(self, symbol, name, exchange=None):
        display_symbol = core.prefixed_symbol(symbol, exchange) if exchange else symbol
        name = (name or "").strip()
        return f"{display_symbol}{self.ENTRY_SEP}{name}" if name else display_symbol

    def _current_ticker_map(self):
        """{symbol: name} for every symbol in the currently checked market(s)."""
        combined = {}
        for ex in self._selected_markets():
            combined.update(self.all_tickers.get(ex, {}))
        return combined

    def _current_ticker_entries(self):
        """[(symbol, name, exchange), ...] for the currently checked market(s)
        -- unlike _current_ticker_map(), this keeps the exchange per entry so
        the list can show NASDAQ.AAPL / NSE.TCS instead of the bare symbol."""
        entries = []
        for ex in self._selected_markets():
            entries.extend((s, n, ex) for s, n in self.all_tickers.get(ex, {}).items())
        return entries

    def _exchange_for_ticker(self, ticker: str):
        """Which market a symbol belongs to, based on what's been loaded via
        search/refresh so far. Returns None if unknown (never refreshed /
        searched, or a genuinely unlisted symbol) -- callers should treat
        that as "leave the output Symbol unprefixed", not as an error."""
        for ex in core.SUPPORTED_EXCHANGES:
            if ticker in self.all_tickers.get(ex, {}):
                return ex
        return None

    def _update_ticker_status(self):
        m = self._current_ticker_map()
        parts = [f"{len(m):,} tickers loaded" if m else
                 "No tickers loaded yet -- click 'Refresh ticker list' to search"]
        if self.selected_tickers:
            parts.append(f"{len(self.selected_tickers)} selected")
        self.ticker_status.config(text=" | ".join(parts))

    def _on_ticker_all_toggle(self):
        self._update_ticker_widgets_state()
        self._apply_ticker_filter()  # the match list can't be filled while disabled, so fill it now

    def _update_ticker_widgets_state(self):
        """Search/select widgets are disabled while ALL is checked, or while
        a download is running -- whichever applies."""
        disable = self.is_running or self.ticker_all_var.get()
        state = "disabled" if disable else "normal"
        for w in (self.search_entry, self.filter_listbox, self.selected_listbox,
                  self.add_button, self.remove_button, self.clear_button, self.search_button):
            w.config(state=state)

    # -- live search (updates as you type, no dropdown/click needed) ------

    def _apply_ticker_filter(self):
        typed = self.ticker_search_var.get().strip().upper()
        entries = self._current_ticker_entries()

        if typed:
            # Search the bare symbol, the exchange-prefixed symbol (so typing
            # "NASDAQ.AAPL" works too), and the company name -- so "APPLE",
            # "AAPL", and "NASDAQ.AAPL" all find the same entry.
            matches = [
                (s, n, ex) for s, n, ex in entries
                if s.upper().startswith(typed)
                or typed in (n or "").upper()
                or core.prefixed_symbol(s, ex).upper().startswith(typed)
            ]
            matches.sort(key=lambda e: (not e[0].upper().startswith(typed), e[0]))
        else:
            matches = sorted(entries, key=lambda e: e[0])

        self.filtered_entries = matches[:300]
        self.filter_listbox.delete(0, "end")
        for s, n, ex in self.filtered_entries:
            self.filter_listbox.insert("end", self._format_entry(s, n, ex))

    def _on_search_keyrelease(self, event):
        if event.keysym in ("Up", "Down", "Return", "Escape", "Tab"):
            return
        self._apply_ticker_filter()  # live: fires on every keystroke

    def _on_search_entry_return(self, event=None):
        typed = self.ticker_search_var.get().strip().upper()
        if not typed:
            return
        exact = next((e for e in self.filtered_entries if e[0] == typed), None)
        if exact:
            self._select_ticker(*exact)
        elif len(self.filtered_entries) == 1:
            self._select_ticker(*self.filtered_entries[0])
        elif self.filtered_entries:
            messagebox.showinfo(
                "Multiple matches",
                f"{len(self.filtered_entries)} matches for '{typed}' -- select the one(s) "
                "you want from the list on the left and click Add ▸ (or double-click one)."
            )
            return
        else:
            # No matches in our loaded directory -- treat as a raw custom
            # ticker (e.g. a brand-new IPO we haven't cached), exchange unknown.
            self._select_ticker(typed, "", None)
        self.ticker_search_var.set("")
        self._apply_ticker_filter()
        self._refresh_selected_listbox()

    def _search_ticker_button(self):
        """Handler for the 🔍 button next to the search field."""
        if not self._current_ticker_map():
            # Nothing loaded yet -- load it now so search has something to
            # search against (needs at least one market checked).
            markets = self._selected_markets()
            if not markets:
                messagebox.showwarning("No market selected",
                                        "Please select at least one market (NASDAQ, NYSE, BSE, NSE) "
                                        "first, then search again.")
                return
            self._refresh_tickers_async()
            return
        self._apply_ticker_filter()

    # -- multi-select: move entries between "matches" and "selected" ------

    def _select_ticker(self, symbol, name, exchange):
        self.selected_tickers[symbol] = (name, exchange)

    def _refresh_selected_listbox(self):
        self.selected_listbox.delete(0, "end")
        for s, (n, ex) in self.selected_tickers.items():
            self.selected_listbox.insert("end", self._format_entry(s, n, ex))
        self._update_ticker_status()

    def _add_filter_selection(self):
        idxs = self.filter_listbox.curselection()
        if not idxs:
            return
        for i in idxs:
            self._select_ticker(*self.filtered_entries[i])
        self._refresh_selected_listbox()

    def _on_filter_double_click(self, event):
        idx = self.filter_listbox.nearest(event.y)
        if 0 <= idx < len(self.filtered_entries):
            self._select_ticker(*self.filtered_entries[idx])
            self._refresh_selected_listbox()

    def _display_text_to_symbol(self, text: str) -> str:
        """Recover the bare ticker (the actual key in self.selected_tickers)
        from a listbox row's display text, e.g. 'NASDAQ.AAPL — Apple Inc. ...'
        -> 'AAPL'. Reading straight from the widget instead of indexing into
        a separately-tracked list of dict keys avoids any chance of the two
        getting out of sync."""
        if self.ENTRY_SEP in text:
            text = text.split(self.ENTRY_SEP, 1)[0]
        text = text.strip()
        for prefix in core.EXCHANGE_PREFIX.values():
            lead = f"{prefix}."
            if text.startswith(lead):
                return text[len(lead):]
        return text

    def _remove_selected_selection(self):
        idxs = self.selected_listbox.curselection()
        if not idxs:
            return
        to_remove = [self._display_text_to_symbol(self.selected_listbox.get(i)) for i in idxs]
        for sym in to_remove:
            self.selected_tickers.pop(sym, None)
        self._refresh_selected_listbox()

    def _on_selected_double_click(self, event):
        idx = self.selected_listbox.nearest(event.y)
        if idx < 0 or idx >= self.selected_listbox.size():
            return
        sym = self._display_text_to_symbol(self.selected_listbox.get(idx))
        self.selected_tickers.pop(sym, None)
        self._refresh_selected_listbox()

    def _clear_selected(self):
        self.selected_tickers.clear()
        self._refresh_selected_listbox()

    def _load_tickers_from_file(self):
        markets = self._selected_markets()
        if len(markets) != 1:
            messagebox.showwarning(
                "Pick one market first",
                "Check exactly one market checkbox above (the one this file's tickers belong "
                "to -- e.g. BSE), then click 'Load tickers from file...' again."
            )
            return
        exchange = markets[0]
        path = filedialog.askopenfilename(
            title=f"Load {exchange} ticker list",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not path:
            return
        try:
            df = core.load_tickers_from_file(path, exchange)
        except Exception as e:
            messagebox.showerror("Couldn't load file", str(e))
            return
        self.all_tickers[exchange] = dict(zip(df["Symbol"], df["Name"]))
        self._apply_ticker_filter()
        self._refresh_selected_listbox()
        self._update_ticker_status()
        messagebox.showinfo("Loaded", f"Loaded {len(df):,} {exchange} tickers from "
                                       f"{Path(path).name}")

    def _refresh_tickers_async(self):
        markets = self._selected_markets()
        if not markets:
            messagebox.showwarning("No market selected",
                                    "Please select at least one market (NASDAQ, NYSE, BSE, NSE).")
            return
        self.refresh_button.config(state="disabled", text="Refreshing...")
        threading.Thread(target=self._do_refresh_tickers, args=(markets,), daemon=True).start()

    def _do_refresh_tickers(self, markets):
        try:
            bse_empty = False
            for m in markets:
                cache_path = CACHE_DIR / f"{m.lower()}_tickers.csv"
                df = core.fetch_ticker_directory(
                    [m], include_etfs=True, cache_path=cache_path, refresh=True
                )
                self.all_tickers[m] = dict(zip(df["Symbol"], df["Name"]))
                if m == "BSE" and df.empty:
                    bse_empty = True
            self.after(0, self._on_tickers_refreshed, bse_empty)
        except Exception as e:
            self.after(0, self._on_tickers_refresh_failed, str(e))

    def _on_tickers_refreshed(self, bse_empty=False):
        self.refresh_button.config(state="normal", text="Refresh ticker list")
        self._apply_ticker_filter()
        self._refresh_selected_listbox()
        self._update_ticker_status()
        if bse_empty:
            messagebox.showinfo(
                "Couldn't fetch the BSE list",
                "The BSE ticker list couldn't be downloaded automatically (BSE's site may be "
                "down or have changed -- see the log for details).\n\n"
                "You can still use 'Load tickers from file...' with a BSE list you export "
                "yourself (bseindia.com/corporates/List_Scrips.aspx), or just search/type "
                "individual BSE tickers directly (e.g. TCS)."
            )

    def _on_tickers_refresh_failed(self, msg):
        self.refresh_button.config(state="normal", text="Refresh ticker list")
        messagebox.showerror("Failed to fetch ticker list", str(msg))

    # --------------------------------------------------------- Date range --

    def _set_dates(self, start: date, end: date):
        for d, widget_attr, var in ((start, "start_date_widget", self.start_var),
                                     (end, "end_date_widget", self.end_var)):
            if self.has_date_picker:
                getattr(self, widget_attr).set_date(d)
            else:
                var.set(d.strftime("%Y-%m-%d"))

    def _get_start_date_str(self) -> str:
        if self.has_date_picker:
            return self.start_date_widget.get_date().strftime("%Y-%m-%d")
        return self.start_var.get().strip()

    def _get_end_date_str(self) -> str:
        if self.has_date_picker:
            return self.end_date_widget.get_date().strftime("%Y-%m-%d")
        return self.end_var.get().strip()

    # -------------------------------------------------------- Save path --

    def _browse_output(self):
        chosen = filedialog.askdirectory(initialdir=self.output_var.get() or str(Path.cwd()))
        if chosen:
            self.output_var.set(chosen)

    def _open_output_folder(self):
        path = Path(self.output_var.get().strip() or ".")
        if path.exists():
            _open_folder(path)
        else:
            messagebox.showinfo("Folder not created yet",
                                 "This folder doesn't exist yet -- it's created on the first run.")

    # -------------------------------------------------------------- Run --

    def _set_running_state(self, running: bool):
        self.is_running = running
        state = "disabled" if running else "normal"
        self.run_button.config(state=state, text="Downloading..." if running else "Download")
        self.cancel_button.config(state=("normal" if running else "disabled"), text="Cancel")
        self.refresh_button.config(state=state)
        self._update_ticker_widgets_state()
        if running:
            self.progress["value"] = 0
            self.status_var.set("Starting...")

    def _cancel_download(self):
        self.cancel_event.set()
        self.cancel_button.config(state="disabled", text="Cancelling...")
        self.status_var.set("Cancelling -- letting in-flight downloads finish...")
        self._append_log("--- Cancel requested; stopping queued downloads, letting in-flight ones finish ---")

    def _start_download(self):
        markets = self._selected_markets()
        all_mode = self.ticker_all_var.get()

        if all_mode:
            if not markets:
                messagebox.showwarning("No market selected",
                                        "Please select at least one market (NASDAQ, NYSE, BSE, NSE), "
                                        "or uncheck ALL and pick specific ticker(s) instead.")
                return
            ticker_selection = "ALL"
            exchange_snapshot = {}
        else:
            if not self.selected_tickers:
                messagebox.showwarning(
                    "No tickers selected",
                    "Please select at least one ticker: search, then click Add ▸ (or "
                    "double-click a match) -- or check ALL instead."
                )
                return
            ticker_selection = list(self.selected_tickers.keys())
            exchange_snapshot = {s: ex for s, (n, ex) in self.selected_tickers.items()}

        start_s, end_s = self._get_start_date_str(), self._get_end_date_str()
        try:
            start_dt = datetime.strptime(start_s, "%Y-%m-%d")
            end_dt = datetime.strptime(end_s, "%Y-%m-%d")
        except ValueError:
            messagebox.showerror("Invalid date", "Please enter dates as YYYY-MM-DD.")
            return
        if start_dt > end_dt:
            messagebox.showerror("Invalid date range", "Start date must be before end date.")
            return

        output_dir = Path(self.output_var.get().strip() or (Path.cwd() / "market_data"))
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            messagebox.showerror("Can't create output folder", str(e))
            return

        layout = self.layout_var.get()
        metastock_ascii = self.metastock_ascii_var.get()
        split_by_exchange = self.split_by_exchange_var.get()
        bse_source = "bhavcopy" if self.bse_bhavcopy_var.get() else "yahoo"
        bse_adjust = self.bse_adjust_var.get()
        self.write_bhavcopy = self.bhavcopy_var.get()  # snapshot: worker thread must not read Tk vars
        self.last_output_dir = output_dir
        self._save_settings()
        self.cancel_event.clear()

        self._set_running_state(True)
        ticker_desc = "ALL" if ticker_selection == "ALL" else f"{len(ticker_selection)} selected"
        self._append_log(
            f"--- Starting run | markets={markets or '(n/a, specific tickers)'} | "
            f"tickers={ticker_desc} | {start_s} -> {end_s} | layout={layout} | "
            f"metastock_ascii={metastock_ascii} | split_by_exchange={split_by_exchange} | "
            f"output={output_dir} ---"
        )

        threading.Thread(
            target=self._run_download_worker,
            args=(markets, ticker_selection, exchange_snapshot, start_s, end_s, output_dir,
                  layout, metastock_ascii, split_by_exchange, bse_source, bse_adjust),
            daemon=True,
        ).start()

    def _run_download_worker(self, markets, ticker_selection, exchange_snapshot,
                              start_s, end_s, output_dir, layout, metastock_ascii,
                              split_by_exchange, bse_source="yahoo", bse_adjust=True):
        try:
            core.start_file_logging(output_dir)
            exchange_map = {}

            if ticker_selection == "ALL":
                tickers = set()
                for m in markets:
                    cache_path = CACHE_DIR / f"{m.lower()}_tickers.csv"
                    df = core.fetch_ticker_directory([m], include_etfs=True, cache_path=cache_path,
                                                       refresh=False)
                    tickers.update(df["Symbol"].tolist())
                    exchange_map.update(dict(zip(df["Symbol"], df["Exchange"])))
                tickers = sorted(tickers)
            else:
                tickers = list(ticker_selection)
                for t in tickers:
                    ex = exchange_snapshot.get(t)
                    if ex:
                        exchange_map[t] = ex
                    else:
                        core.log.warning(
                            f"Exchange unknown for {t}. Its output Symbol will be unprefixed, "
                            f"and if it's actually an NSE/BSE ticker the download will FAIL "
                            f"outright (Yahoo needs the .NS/.BO suffix to find it at all). "
                            f"Search for it via 'Refresh ticker list' first so its exchange is known."
                        )

            if not tickers:
                raise RuntimeError("No tickers resolved for the selected market(s)/ticker(s).")

            if split_by_exchange:
                # Separate raw/ cache AND separate combined/daily/per-symbol
                # output per exchange, e.g. output_dir/NASDAQ/..., output_dir/NYSE/...
                groups = {}
                for t in tickers:
                    groups.setdefault(exchange_map.get(t, "UNKNOWN"), []).append(t)
                for ex, ex_tickers in groups.items():
                    core.log.info(f"=== {ex}: {len(ex_tickers)} ticker(s) ===")
                    ex_dir = output_dir / ex
                    ex_exchange_map = {t: exchange_map.get(t) for t in ex_tickers}
                    core.download_all(ex_tickers, start_s, end_s, ex_dir / "raw", pause=0.3,
                                       progress_callback=self._on_progress,
                                       exchange_map=ex_exchange_map, max_workers=10,
                                       cancel_event=self.cancel_event,
                                       bse_source=bse_source, bse_adjust_splits=bse_adjust)
                    if self.cancel_event.is_set():
                        break
                    core.report_data_quality(ex_dir / "raw", ex_tickers, ex_exchange_map)
                    self._build_outputs(ex_dir, ex_tickers, ex_exchange_map, layout, metastock_ascii,
                                         start_s, end_s, output_dir / "bhavcopy")
            else:
                core.download_all(tickers, start_s, end_s, output_dir / "raw", pause=0.3,
                                   progress_callback=self._on_progress, exchange_map=exchange_map,
                                   max_workers=10, cancel_event=self.cancel_event,
                                   bse_source=bse_source, bse_adjust_splits=bse_adjust)
                if not self.cancel_event.is_set():
                    core.report_data_quality(output_dir / "raw", tickers, exchange_map)
                    self._build_outputs(output_dir, tickers, exchange_map, layout, metastock_ascii,
                                         start_s, end_s, output_dir / "bhavcopy")

            if self.cancel_event.is_set():
                self.after(0, self._download_finished, "cancelled",
                           f"Cancelled. Whatever finished downloading is cached under:\n{output_dir}\n"
                           f"Re-run the same settings later to resume and build outputs.")
            else:
                self.after(0, self._download_finished, "success",
                           f"Done -- {len(tickers):,} ticker(s) processed.\nSaved under:\n{output_dir}")
        except Exception as e:
            core.log.error(f"GUI run failed: {e}")
            self.after(0, self._download_finished, "error", str(e))

    def _build_outputs(self, base_dir, tickers, exchange_map, layout, metastock_ascii, start_s, end_s,
                       bhavcopy_dir):
        """Runs once, after all downloads for `tickers` have finished -- never
        incrementally per-ticker, so combining a big batch only happens once."""
        raw_dir = base_dir / "raw"
        if layout in ("daily", "all"):
            core.build_daily_csvs(raw_dir, base_dir / "daily", start_s, end_s,
                                   tickers=tickers, exchange_map=exchange_map,
                                   metastock_ascii=metastock_ascii)
        if layout in ("per-symbol", "all"):
            core.build_per_symbol_csvs(raw_dir, base_dir / "by_symbol", start_s, end_s,
                                        tickers=tickers, exchange_map=exchange_map,
                                        metastock_ascii=metastock_ascii)
        if layout in ("combined", "all"):
            core.build_combined_csv(raw_dir, base_dir / "combined" / "all_data.csv", start_s, end_s,
                                     tickers=tickers, exchange_map=exchange_map,
                                     metastock_ascii=metastock_ascii)
        if self.write_bhavcopy:
            core.build_bhavcopy_files(raw_dir, bhavcopy_dir, start_s, end_s,
                                      tickers=tickers, exchange_map=exchange_map)

    def _on_progress(self, done, total, failed, no_data):
        self.after(0, self._update_progress_ui, done, total, failed, no_data)

    def _update_progress_ui(self, done, total, failed, no_data):
        self.progress["maximum"] = max(total, 1)
        self.progress["value"] = done
        self.status_var.set(f"{done}/{total} | failed={failed} | no_data={no_data}")

    def _download_finished(self, status: str, message: str):
        self._set_running_state(False)
        if status == "success":
            self.status_var.set("Done.")
            messagebox.showinfo("Download complete", message)
        elif status == "cancelled":
            self.status_var.set("Cancelled.")
            messagebox.showinfo("Download cancelled", message)
        else:
            self.status_var.set("Failed -- see log.")
            messagebox.showerror("Download failed", message)

    # ------------------------------------------------------------ Close --

    def _on_close(self):
        if self.is_running:
            if not messagebox.askyesno(
                "Download in progress",
                "A download is still running. Quit anyway?\n\n"
                "(Whatever's already been downloaded stays cached, so you "
                "can resume later by running again with the same settings.)",
            ):
                return
        self._save_settings()
        self.destroy()


def main():
    if core.yf is None:
        # Show this in a message box too, since a GUI user may never see a
        # console. GUI still opens so they can read the message clearly.
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(
            "Missing dependency",
            "yfinance is not installed.\n\nRun this in your terminal, then restart the app:\n\n"
            "    pip install yfinance pandas requests",
        )
        root.destroy()
        return
    app = MarketDataGUI()
    app.mainloop()


if __name__ == "__main__":
    main()
