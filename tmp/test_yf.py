import yfinance as yf
print(yf.Ticker("AAPL").fast_info.last_price)
