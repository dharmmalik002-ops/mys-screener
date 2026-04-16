
import asyncio
import json
import logging
import sys
from pathlib import Path
from pydantic import BaseModel, Field
from datetime import date, datetime

# Simple StockSnapshot mock to match the live one
class StockSnapshot(BaseModel):
    symbol: str
    last_price: float
    market_cap_crore: float
    snapshot_cache_version: int
    listing_date: date | str | None = None
    # Add other fields as needed or just test a few

def test_validation():
    snapshot_path = Path("backend/data/free_snapshots.json")
    with open(snapshot_path, "r") as f:
        data = json.load(f)
    
    print(f"Testing validation on {len(data)} rows...")
    success = 0
    failure = 0
    for i, row in enumerate(data):
        try:
            # We don't have the full model here, but we can check for common issues
            # like invalid dates or nulls.
            if row.get("listing_date") == "":
                # If it's empty string, Pydantic 'date' might fail depending on version
                pass
            success += 1
        except Exception as e:
            if failure < 5:
                print(f"Row {i} ({row.get('symbol')}) failed: {e}")
            failure += 1
    
    print(f"Validation complete. Success: {success}, Failure: {failure}")

if __name__ == "__main__":
    test_validation()
