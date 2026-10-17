"""Build NYSE / NASDAQ delisted-symbol lists from Alpha Vantage's free LISTING_STATUS endpoint.

Needs a free API key (https://www.alphavantage.co/support/#api-key): --api-key or env ALPHAVANTAGE_API_KEY.
Writes <root>/<EX>/delisted_symbols.csv (Symbol, Name, IpoDate, DelistingDate) for each exchange, ready for
  python us_factors.py --exchanges NYSE NASDAQ --delisted-file "market_data_daily/{ex}/delisted_symbols.csv"
Alpha Vantage only covers stocks delisted since about 2010, and Yahoo serves history for only some of them;
us_factors.py registers whichever ones Yahoo returns. Use --input to read a CSV you already saved.
"""
import argparse
import io
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import requests

log = logging.getLogger("market_dl")
URL = "https://www.alphavantage.co/query"
EXCHANGE_MAP = {"NYSE": "NYSE", "NYSE MKT": "NYSE", "NYSE ARCA": "NYSE", "NYSE AMERICAN": "NYSE", "NASDAQ": "NASDAQ"}


def fetch(api_key: str) -> pd.DataFrame:
    r = requests.get(URL, params={"function": "LISTING_STATUS", "state": "delisted", "apikey": api_key}, timeout=60)
    r.raise_for_status()
    if not r.text.lstrip().lower().startswith("symbol"):
        sys.exit(f"Alpha Vantage did not return a listing (bad key or rate limit?): {r.text[:200]}")
    return pd.read_csv(io.StringIO(r.text), dtype=str)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api-key", default=os.environ.get("ALPHAVANTAGE_API_KEY"))
    ap.add_argument("--input", help="Use this saved LISTING_STATUS CSV instead of calling the API")
    ap.add_argument("--root", default="market_data_daily")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.input:
        df = pd.read_csv(args.input, dtype=str)
    elif args.api_key:
        df = fetch(args.api_key)
    else:
        sys.exit("Need --api-key / ALPHAVANTAGE_API_KEY (free) or --input <saved csv>")

    df.columns = [c.strip() for c in df.columns]
    df = df[df["assetType"].str.lower() == "stock"]
    df["Exchange"] = df["exchange"].str.upper().map(EXCHANGE_MAP)
    df = df.dropna(subset=["Exchange"])
    for ex, g in df.groupby("Exchange"):
        out = pd.DataFrame({"Symbol": g["symbol"].str.strip().str.upper(), "Name": g["name"],
                            "IpoDate": g["ipoDate"], "DelistingDate": g["delistingDate"]})
        out = out.drop_duplicates("Symbol")
        path = Path(args.root) / ex / "delisted_symbols.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(path, index=False)
        log.info(f"{ex}: {len(out):,} delisted symbols -> {path}")


if __name__ == "__main__":
    main()
