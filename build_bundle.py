#!/usr/bin/env python3
"""
Build the portable Windows bundle: dist/EOD_Market_Data/ and dist/EOD_Market_Data_<YYYYMMDD>.zip.

    venv\\Scripts\\python.exe -m pip install -r requirements-build.txt
    venv\\Scripts\\python.exe build_bundle.py            # add --no-keys to leave the Alpaca key files out

The folder holds EOD_Market_Data.exe (browser app), cli_daily.exe and cli_hourly.exe sharing one
_internal/ (Python and every library), plus eod_config.ini, the NSE hand-maintained CSVs, README.md,
QUICKSTART.txt, the Alpaca key files (.alpaca3.env, .alpaca2.env) and NSE's corporate-actions lists
(seed_data/NSE/nse_ca, ~10 MB, copied from <daily_output>/NSE/nse_ca; the app takes them into its own cache on
the first NSE run, so it never downloads 2016..today from nseindia.com). No price data is copied; the build fails
if any ends up in the folder. The recipient points eod_config.ini at their data folders.
"""

import argparse
import shutil
import subprocess
import sys
import zipfile
from datetime import date
from pathlib import Path

HERE = Path(__file__).resolve().parent
DIST = HERE / "dist"
APP = DIST / "EOD_Market_Data"
EXTRA_FILES = ["nse_demergers.csv", "nse_mergers.csv", "nse_renames.csv", "nse_exit_info.csv",
               "README.md", "QUICKSTART.txt"]
KEY_FILES = [".alpaca3.env", ".alpaca2.env"]          # .alpaca.env is dead (see CLAUDE.md)
FORBIDDEN = ("market_data_", ".ticker_cache", "raw_1h", "official_cache", "bhavcopy_cache")


def build(with_keys: bool = True) -> Path:
    subprocess.run([sys.executable, "-m", "PyInstaller", "--noconfirm", "--clean",
                    "--distpath", str(DIST), "--workpath", str(HERE / "build"), str(HERE / "eod_bundle.spec")],
                   cwd=HERE, check=True)
    shutil.copy2(HERE / "eod_config.example.ini", APP / "eod_config.ini")
    for name in EXTRA_FILES:
        shutil.copy2(HERE / name, APP / name)
    seed_nse_actions(APP / "seed_data" / "NSE" / "nse_ca")
    if with_keys:
        for name in KEY_FILES:
            if (HERE / name).exists():
                shutil.copy2(HERE / name, APP / name)
            else:
                print(f"warning: {name} not found, not bundled")
    check(APP)
    zip_path = DIST / f"EOD_Market_Data_{date.today():%Y%m%d}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for f in sorted(APP.rglob("*")):
            if f.is_file():
                z.write(f, Path("EOD_Market_Data") / f.relative_to(APP))
    size = sum(f.stat().st_size for f in APP.rglob("*") if f.is_file())
    print(f"\nBundle folder: {APP} ({size / 1e6:.0f} MB)")
    print(f"Zip:           {zip_path} ({zip_path.stat().st_size / 1e6:.0f} MB)")
    return zip_path


def seed_nse_actions(out: Path):
    """Copy the quarter lists of NSE's corporate actions plus the day each was checked through."""
    import app_paths
    import nse_bhavcopy as nb
    src = app_paths.daily_output() / "NSE" / "nse_ca"
    files = sorted(f for f in src.glob("*.json") if nb.RX_CA_FILE.match(f.name)) if src.is_dir() else []
    if not files:
        print(f"warning: no NSE corporate-actions lists in {src}, not bundled")
        return
    out.mkdir(parents=True, exist_ok=True)
    for f in files:
        shutil.copy2(f, out / f.name)
    checked = {k: v for k, v in nb._ca_checked(src).items() if (out / k).exists()}
    nb._save_ca_checked(out, checked)
    size = sum(f.stat().st_size for f in files)
    print(f"NSE corporate actions: {len(files)} lists ({size / 1e6:.1f} MB) bundled, checked through "
          f"{max(checked.values())}")


def check(folder: Path):
    """No market data, logs or caches in the bundle."""
    bad = [p for p in folder.rglob("*")
           if any(part.startswith(FORBIDDEN) for part in p.relative_to(folder).parts) or p.suffix == ".log"]
    if bad:
        raise SystemExit(f"refusing to package data/log files: {[str(b) for b in bad[:10]]}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-keys", action="store_true", help="leave the Alpaca key files out")
    build(with_keys=not ap.parse_args().no_keys)


if __name__ == "__main__":
    main()
