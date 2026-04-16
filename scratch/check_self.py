
import asyncio
import os
import sys
from pathlib import Path

# Set up paths to import the backend code
backend_path = Path("backend").resolve()
sys.path.append(str(backend_path))

async def check_backend_integrity():
    print("--- BACKEND INTEGRITY CHECK ---")
    
    try:
        from app.providers.free import FreeMarketDataProvider
        from app.models.market import StockSnapshot
        print("SUCCESS: Core modules imported.")
    except Exception as e:
        print(f"FAILURE: Could not import modules: {e}")
        return

    provider = FreeMarketDataProvider()
    print(f"Provider initialized. Snapshot path: {provider.snapshot_cache_path}")
    
    if not provider.snapshot_cache_path.exists():
        print(f"FAILURE: {provider.snapshot_cache_path} is missing.")
        return

    print("Attempting to load cached snapshots (India)...")
    try:
        # We simulate the call build_dashboard makes
        rows = await asyncio.to_thread(provider._load_valid_cached_snapshot_rows)
        print(f"Raw rows loaded from JSON: {len(rows)}")
        
        if len(rows) == 0:
            print("FAILURE: No rows loaded from JSON. Possible schema mismatch.")
            return

        snapshots = await provider._materialize_snapshot_rows(rows)
        print(f"Materialized snapshots: {len(snapshots)}")
        
        if len(snapshots) > 0:
            print(f"SUCCESS: Successfully materialized {len(snapshots)} symbols.")
            sample = snapshots[0]
            print(f"Sample: {sample.symbol} ({sample.name}) | Price: {sample.last_price} | Listing: {sample.listing_date}")
            
            # Check for specifically known poison stocks
            poison_symbols = ["KAMAHOLD", "ELANTAS", "KIRLFER"]
            for s in snapshots:
                if s.symbol in poison_symbols:
                    print(f"PASSED: Poison symbol {s.symbol} materialized correctly with listing_date={s.listing_date}")
        else:
            print("FAILURE: Materialization returned 0 snapshots. Poison rows might still be killing the load.")

    except Exception as e:
        print(f"FAILURE: An error occurred during validation: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_backend_integrity())
