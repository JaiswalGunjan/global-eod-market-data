"""Turn the hand-researched NSE delistings/mergers workbook into the CSV inputs the NSE scripts read.

Reads (needs `pip install openpyxl`, only for this script):
  sheet "Symbol changes (not exits)" -> nse_renames.csv    Old,New   (ISIN changed, same issuer; read by nse_stitch.py)
  sheets "Delistings" + "Exits not in NSE list" -> nse_exit_info.csv
        Symbol,Category,Route,OfficialDate,ExitPrice   (read by nse_corporate_actions.py to annotate delist events;
        Category is 'rights_entitlement' for rights-entitlement tickers, which are not company exits)
Existing files are overwritten; nse_mergers.csv stays hand-maintained.
"""
import argparse

import openpyxl
import pandas as pd


def sheet(wb, name):
    rows = list(wb[name].iter_rows(values_only=True))
    return pd.DataFrame(rows[1:], columns=rows[0])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("workbook")
    ap.add_argument("--renames", default="nse_renames.csv")
    ap.add_argument("--exit-info", default="nse_exit_info.csv")
    args = ap.parse_args()

    wb = openpyxl.load_workbook(args.workbook, read_only=True)

    sc = sheet(wb, "Symbol changes (not exits)")
    sc = sc.rename(columns={"Old symbol": "Old", "New symbol": "New"})[["Old", "New"]].dropna()
    sc.to_csv(args.renames, index=False)
    print(f"{len(sc)} symbol changes -> {args.renames}")

    dl = sheet(wb, "Delistings").dropna(subset=["Symbol"])
    info = pd.DataFrame({"Symbol": dl["Symbol"], "Category": "delisted", "Route": dl["Route"],
                         "OfficialDate": dl["Date"], "ExitPrice": dl["Exit price (Rs)"]})
    ex = sheet(wb, "Exits not in NSE list").dropna(subset=["Symbol"])
    cat = ex["Category"].fillna("")
    kind = pd.Series("exit_not_in_list", index=ex.index)
    kind[cat.str.startswith("Rights entitlement")] = "rights_entitlement"
    kind[cat.str.startswith("Not an exit")] = "not_an_exit"
    info = pd.concat([info, pd.DataFrame({"Symbol": ex["Symbol"], "Category": kind, "Route": cat,
                                          "OfficialDate": "", "ExitPrice": ""})], ignore_index=True)
    info = info.drop_duplicates("Symbol", keep="first")      # official delisting list wins over the "not in list" sheet
    info.to_csv(args.exit_info, index=False)
    print(f"{len(info)} symbols -> {args.exit_info}\n{info.Category.value_counts().to_string()}")


if __name__ == "__main__":
    main()
