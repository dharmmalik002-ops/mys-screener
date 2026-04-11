import sys
import os
import asyncio
from datetime import date
from zoneinfo import ZoneInfo

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), "backend"))

from app.providers.free import FreeMarketDataProvider
from app.core.config import Settings

async def test_chart_filtering():
    settings = Settings()
    # Mock settings if needed
    provider = FreeMarketDataProvider(settings)
    
    # Check if Friday April 3rd (Holiday) and Saturday April 4th (Today) are filtered
    symbol = "RELIANCE.NS"
    try:
        # We need to mock some dependencies for a standalone test if possible,
        # but the provider needs a data/ directory and cached snapshots to work fully.
        # Let's just check the method directly with mock data.
        import pandas as pd
        
        # Create a dataframe with a holiday and a weekend
        # April 2: Thur (Trading)
        # April 3: Fri (Holiday - Good Friday)
        # April 4: Sat (Weekend)
        data = {
            "Open": [100.0, 101.0, 101.0],
            "High": [102.0, 101.0, 101.0],
            "Low": [99.0, 101.0, 101.0],
            "Close": [101.0, 101.0, 101.0],
            "Volume": [1000, 0, 0]
        }
        dates = [
            pd.Timestamp("2026-04-02"),
            pd.Timestamp("2026-04-03"), 
            pd.Timestamp("2026-04-04")
        ]
        history = pd.DataFrame(data, index=dates)
        
        bars = provider._history_to_chart_bars(history)
        
        print(f"Original history rows: {len(history)}")
        print(f"Filtered chart bars: {len(bars)}")
        for bar in bars:
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(bar.time, tz=timezone.utc)
            print(f"Bar: {dt.date()} Close: {bar.close} Vol: {bar.volume}")
            
        if len(bars) == 1 and bars[0].close == 101.0:
            print("SUCCESS: Holiday and weekend bars were filtered out!")
        else:
            print("FAILURE: Some holiday/weekend bars survived.")
            
    except Exception as e:
        print(f"Test error: {e}")

if __name__ == "__main__":
    asyncio.run(test_chart_filtering())
