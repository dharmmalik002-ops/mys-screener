import requests

url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20TOTAL%20MARKET"
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
}

session = requests.Session()
session.get("https://www.nseindia.com", headers=headers)
response = session.get(url, headers=headers)
print(response.status_code)
if response.status_code == 200:
    data = response.json()
    print("Found data for", len(data.get("data", [])), "stocks")
    if data.get("data"):
        print(data["data"][0])
else:
    print(response.text[:200])

