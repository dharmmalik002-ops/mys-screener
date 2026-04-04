import yfinance as yf
from yfinance.data import YfData
data = YfData()
res = data.get("https://query1.finance.yahoo.com/v7/finance/quote", params={"symbols": "RELIANCE.NS,TCS.NS"})
print(res.status_code)
print(res.json() if res.status_code == 200 else res.text)
