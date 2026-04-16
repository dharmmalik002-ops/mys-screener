
import asyncio
import json
from pathlib import Path
from typing import Any

# Mocking the Environment to run the diagnostic
class MockSettings:
    market_cap_min_crore = 800.0
    backend_root = Path("./backend")

def diag_load():
    snapshot_path = Path("backend/data/free_snapshots.json")
    if not snapshot_path.exists():
        print(f"FAILED: {snapshot_path} does not exist")
        return

    print(f"File exists: {snapshot_path}, size: {snapshot_path.stat().st_size} bytes")
    
    try:
        with open(snapshot_path, "r") as f:
            data = json.load(f)
    except Exception as e:
        print(f"FAILED: JSON Load error: {e}")
        return

    if not isinstance(data, list):
        print(f"FAILED: JSON is not a list, it is {type(data)}")
        return

    print(f"Total rows in JSON: {len(data)}")
    if not data:
        return

    # Check version
    ver = data[0].get("snapshot_cache_version")
    print(f"Snapshot version in file: {ver}")

    # Check first row fields
    fields = set(data[0].keys())
    expected = {
        "snapshot_cache_version", "previous_close", "week_high_prev", "month_high_prev",
        "ath_prev", "high_52w_prev", "range_high_prev_20d", "avg_volume_30d", "ema10",
        "benchmark_return_1d", "benchmark_return_5d", "benchmark_return_60d",
        "benchmark_return_126d", "benchmark_return_252d", "stock_return_5d",
        "stock_return_126d", "stock_return_189d", "stock_return_12m", "stock_return_504d",
        "rs_line_today", "rs_rating", "listing_date"
    }
    missing = expected - fields
    if missing:
        print(f"FAILED: Row 0 is missing fields: {missing}")
    else:
        print("Row 0 has all expected fields.")

    # Check market cap filter
    min_cap = 800.0
    passed = [r for r in data if float(r.get("market_cap_crore", 0) or 0) >= min_cap]
    print(f"Rows passing market_cap >= {min_cap}: {len(passed)}")
    
    if len(passed) == 0:
        caps = [float(r.get("market_cap_crore", 0) or 0) for r in data[:10]]
        print(f"Sample market caps: {caps}")
        if "market_cap_crore" not in data[0]:
            print(f"WARNING: 'market_cap_crore' field missing. Available: {list(data[0].keys())[:10]}")

if __name__ == "__main__":
    diag_load()
