import yfinance as yf
print([x for x in dir(yf.utils) if 'crumb' in x.lower() or 'cookie' in x.lower()])
