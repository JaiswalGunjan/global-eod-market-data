"""
Where the app lives and where its data goes.

APP_DIR  -- the folder holding the scripts, or the exe in the portable bundle (PyInstaller). Writable things
            that belong to the install sit here: eod_config.ini,
            nse_demergers.csv.
RES_DIR  -- read-only files bundled inside the exe (the web page in web/); the same as APP_DIR when run
            from source.

eod_config.ini (optional, next to the scripts / exe) points at the data folders, so data never has to live
inside the app folder. Relative paths are taken relative to APP_DIR. A missing file or key keeps the
defaults below, which are the folders this repo has always used. See eod_config.example.ini.
"""

import configparser
import sys
from pathlib import Path

FROZEN = bool(getattr(sys, "frozen", False))
APP_DIR = Path(sys.executable).resolve().parent if FROZEN else Path(__file__).resolve().parent
RES_DIR = Path(getattr(sys, "_MEIPASS", APP_DIR))
CONFIG_NAME = "eod_config.ini"

DEFAULTS = {
    "paths": {
        "daily_output": "market_data_daily",
        "hourly_output": "market_data_hourly",
        "alpaca_data": "market_data_alpaca",
        "ticker_cache": ".ticker_cache",
    },
    "web": {
        "port": "8790",
        "open_browser": "yes",
    },
}

_config = None


def load_config(path: Path = None, reload: bool = False) -> configparser.ConfigParser:
    """The parsed config (defaults merged with eod_config.ini), cached after the first call."""
    global _config
    if _config is not None and not reload and path is None:
        return _config
    cp = configparser.ConfigParser(inline_comment_prefixes=(";", "#"))
    cp.read_dict(DEFAULTS)
    cfg_path = Path(path) if path is not None else APP_DIR / CONFIG_NAME
    if cfg_path.exists():
        cp.read(cfg_path, encoding="utf-8")
    if path is None:
        _config = cp
    return cp


def _path(key: str) -> Path:
    raw = load_config().get("paths", key, fallback=DEFAULTS["paths"][key]).strip().strip('"')
    p = Path(raw or DEFAULTS["paths"][key]).expanduser()
    return p if p.is_absolute() else APP_DIR / p


def daily_output() -> Path:
    return _path("daily_output")


def hourly_output() -> Path:
    return _path("hourly_output")


def ticker_cache_dir() -> Path:
    return _path("ticker_cache")


def web_port() -> int:
    try:
        return int(load_config().get("web", "port", fallback="8790"))
    except ValueError:
        return 8790


def web_open_browser() -> bool:
    try:
        return load_config().getboolean("web", "open_browser", fallback=True)
    except ValueError:
        return True
