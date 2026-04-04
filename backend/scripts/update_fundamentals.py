import asyncio
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import yfinance as yf

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BACKEND_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BACKEND_DIR / "data"

def load_universe(filepath: Path) -> list[dict[str, Any]]:
    if not filepath.exists():
        logger.warning(f"Universe file {filepath} not found.")
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

def fetch_yfinance_info(symbol: str) -> dict[str, Any]:
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        # We extract safe defaults if they don't jump exist
        return {
            "symbol": symbol,
            "eps_growth_yoy": info.get("earningsGrowth", None),          # often presented as a decimal (0.30 = 30%)
            "revenue_growth_yoy": info.get("revenueGrowth", None),       # often as a decimal
            "operating_margin": info.get("operatingMargins", None),
            "profit_margin": info.get("profitMargins", None),
            "roe": info.get("returnOnEquity", None),
            "roa": info.get("returnOnAssets", None),
            "trailing_eps": info.get("trailingEps", None),
            "forward_eps": info.get("forwardEps", None),
            "peg_ratio": info.get("pegRatio", None),
            "price_to_book": info.get("priceToBook", None),
            "debt_to_equity": info.get("debtToEquity", None),
        }
    except Exception as e:
        logger.debug(f"Failed to fetch {symbol}: {e}")
        return {"symbol": symbol}

def fetch_batch_fundamentals(symbols: list[str], max_workers: int = 10) -> dict[str, dict[str, Any]]:
    results = {}
    completed = 0
    total = len(symbols)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(fetch_yfinance_info, sym): sym for sym in symbols}
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            completed += 1
            if completed % 100 == 0 or completed == total:
                logger.info(f"Progress: {completed} / {total} processed.")
            try:
                data = future.result()
                if any(v is not None for k, v in data.items() if k != "symbol"):
                    results[symbol] = data
            except Exception as e:
                logger.debug(f"Exception for {symbol}: {e}")
                
    return results

def construct_instrument_key(row: dict[str, Any]) -> str:
    # India expects .NS suffix, US doesn't need a suffix for Yahoo
    exchange = str(row.get("exchange", "")).upper()
    symbol = str(row.get("symbol", ""))
    if exchange == "NSE" and not symbol.endswith(".NS"):
        return f"{symbol}.NS"
    elif exchange == "BSE" and not symbol.endswith(".BO"):
        return f"{symbol}.BO"
    return symbol

def main():
    logger.info("Starting Quarterly Fundamentals Update")
    
    # 1. INDIA
    india_universe_path = DATA_DIR / "free_universe.json"
    india_cache_path = DATA_DIR / "free_fundamental_cache.json"
    india_universe = load_universe(india_universe_path)
    
    if india_universe:
        logger.info(f"Fetching India Fundamentals for {len(india_universe)} symbols...")
        india_symbols = [construct_instrument_key(item) for item in india_universe if item.get("symbol")]
        india_results = fetch_batch_fundamentals(india_symbols, max_workers=8)
        
        # map back to root symbol (Reliance instead of RELIANCE.NS)
        output = {}
        for item in india_universe:
            raw_sym = str(item.get("symbol"))
            yf_sym = construct_instrument_key(item)
            if yf_sym in india_results:
                output[raw_sym] = india_results[yf_sym]
                
        with open(india_cache_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)
        logger.info(f"Saved {len(output)} India fundamentals to {india_cache_path.name}")
        
    # 2. USA
    us_universe_path = DATA_DIR / "free_universe_us.json"
    us_cache_path = DATA_DIR / "free_fundamental_cache_us.json"
    us_universe = load_universe(us_universe_path)
    
    if us_universe:
        logger.info(f"Fetching US Fundamentals for {len(us_universe)} symbols...")
        us_symbols = [str(item.get("symbol", "")) for item in us_universe if item.get("symbol")]
        us_results = fetch_batch_fundamentals(us_symbols, max_workers=8)
        
        output = {}
        for item in us_universe:
            raw_sym = str(item.get("symbol"))
            if raw_sym in us_results:
                output[raw_sym] = us_results[raw_sym]
                
        with open(us_cache_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)
        logger.info(f"Saved {len(output)} US fundamentals to {us_cache_path.name}")

    logger.info("Quarterly Fundamentals Update Complete! You can now commit the JSON files and upload to Hugging Face.")

if __name__ == "__main__":
    main()
