import httpx
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}
try:
    with httpx.Client(timeout=10, headers=HEADERS, follow_redirects=True) as client:
        resp = client.get("https://query1.finance.yahoo.com/v7/finance/quote", params={"symbols": "AAPL,MSFT,TSLA"}, timeout=10)
        print("Status", resp.status_code)
        print("Data length", len(resp.content))
except Exception as e:
    print(e)
