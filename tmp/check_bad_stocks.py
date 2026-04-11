import yfinance as yf
import pandas as pd

def check_stock(symbol):
    print(f"\nChecking {symbol}:")
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="1mo")
    if hist.empty:
        print("No history found.")
        return
    
    print(hist[["Open", "High", "Low", "Close", "Adj Close", "Dividends", "Stock Splits"]].tail())
    
    # Check for splits/dividends in last 12 months
    hist_1y = ticker.history(period="1y")
    splits = hist_1y[hist_1y["Stock Splits"] > 0]
    divs = hist_1y[hist_1y["Dividends"] > 0]
    
    if not splits.empty:
        print("\nRecent Splits:")
        print(splits[["Stock Splits"]])
    else:
        print("\nNo splits in last 12 months.")
        
    if not divs.empty:
        print(f"\nFound {len(divs)} dividends in last 12 months.")
    else:
        print("\nNo dividends in last 12 months.")

if __name__ == "__main__":
    # Indian stocks usually need .NS or .BO
    check_stock("TNPL.NS")
    check_stock("APOLLOHOSP.NS")
    check_stock("NESCO.NS")
    check_stock("WONDERELE.NS")
