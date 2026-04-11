import yfinance as yf
print("yfinance testing fast quotes...")
df = yf.download(tickers=["AAPL", "MSFT", "TSLA"], period="1d", interval="1d", group_by="ticker", threads=True, progress=False)
print("DF columns:", df.columns)
print("AAPL close:", df.get('AAPL', df)['Close'].iloc[-1])
