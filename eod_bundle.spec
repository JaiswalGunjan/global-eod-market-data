# PyInstaller spec for the portable Windows bundle. Build with:  python build_bundle.py
# Three programs share one _internal/ folder:
#   EOD_Market_Data.exe    the browser app (eod_web.py); a console window shows the address and the log
#   cli_daily.exe     the daily CLI
#   cli_hourly.exe  the hourly CLI
from PyInstaller.utils.hooks import collect_all, collect_data_files

datas = [("web", "web")]
binaries = []
hiddenimports = [
    # imported lazily inside functions
    "nse_bhavcopy", "alpaca_us", "bse_bhavcopy", "nse_delisted", "reconcile_nse", "price_basis",
]
for pkg in ("curl_cffi", "yfinance"):           # curl_cffi ships yfinance's native libcurl
    d, b, h = collect_all(pkg)
    datas += d
    binaries += b
    hiddenimports += h
datas += collect_data_files("certifi")

excludes = ["tkinter", "_tkinter", "tkcalendar", "openpyxl", "matplotlib", "IPython", "pytest", "tests",
            "eod_gui"]


def analysis(script):
    return Analysis([script], pathex=["."], binaries=binaries, datas=datas, hiddenimports=hiddenimports,
                    excludes=excludes, noarchive=False)


def exe(a, name):
    return EXE(PYZ(a.pure), a.scripts, [], exclude_binaries=True, name=name, console=True,
               debug=False, strip=False, upx=False)


a_web = analysis("eod_web.py")
a_cli = analysis("eod_downloader.py")
a_hourly = analysis("hourly_downloader.py")

coll = COLLECT(
    exe(a_web, "EOD_Market_Data"), a_web.binaries, a_web.datas,
    exe(a_cli, "cli_daily"), a_cli.binaries, a_cli.datas,
    exe(a_hourly, "cli_hourly"), a_hourly.binaries, a_hourly.datas,
    strip=False, upx=False, name="EOD_Market_Data",
)
