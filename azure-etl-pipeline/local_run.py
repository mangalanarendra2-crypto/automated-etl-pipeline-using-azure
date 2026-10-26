"""Run the full ETL locally (local folder + SQLite) - no Azure account required.

    python local_run.py                 # drops sample_data/*.csv into raw/ and processes them
    python local_run.py --no-sample     # only process whatever is already in .local/blobs/raw
"""
import argparse
import logging
from pathlib import Path

from etl.load import SqliteLoader
from etl.pipeline import RAW, process_all
from etl.storage import LocalBlobStore


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-sample", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    store, loader = LocalBlobStore(".local/blobs"), SqliteLoader(".local/etl.db")
    if not a.no_sample:
        for f in sorted(Path("sample_data").glob("*.csv")):
            store.write(RAW, f.name, f.read_bytes())  # simulates a file landing in Blob Storage

    print(f"\n{'blob':<26}{'status':<14}{'in':>5}{'loaded':>8}{'rejected':>10}")
    for r in process_all(store, loader):
        print(f"{r['blob_name']:<26}{r['status']:<14}{r['rows_in']:>5}{r['rows_loaded']:>8}{r['rows_rejected']:>10}")
        if r["message"]:
            print(f"    -> {r['message']}")

    print(f"\nRows in sales table: {loader.count_sales()}")
    print("Revenue by country:")
    for country, orders, revenue in loader.query(
            "SELECT country, COUNT(*), ROUND(SUM(total_amount),2) FROM sales GROUP BY country ORDER BY 3 DESC"):
        print(f"  {country}  {orders:>3} orders  {revenue:>12,.2f}")
    for c in ("processed", "rejected", "archive", "quarantine"):
        print(f"{c:<11}: {store.list(c)}")


if __name__ == "__main__":
    main()
