"""Temporary: which earnings sources answer from a GitHub runner."""
import io, json, zipfile, datetime as dt
import requests
from curl_cffi import requests as creq

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
BSE_H = {"User-Agent": UA, "Accept": "application/json, text/plain, */*", "Referer": "https://www.bseindia.com/", "Origin": "https://www.bseindia.com"}
today = dt.date.today()

def show(name, r):
    try:
        body = r.text[:300].replace("\n", " ")
    except Exception as e:
        body = repr(e)
    print(f"== {name}: {getattr(r,'status_code',None)} len={len(getattr(r,'content',b'') or b'')} :: {body}", flush=True)

def tryit(name, fn):
    try:
        show(name, fn())
    except Exception as e:
        print(f"== {name}: EXC {e!r}"[:300], flush=True)

ann = ("https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?strCat=Result&strPrevDate="
       + (today - dt.timedelta(days=7)).strftime("%Y%m%d") + "&strToDate=" + today.strftime("%Y%m%d")
       + "&strSearch=P&strscrip=&strType=C&pageno=1")
fth = "https://api.bseindia.com/BseIndiaAPI/api/Corpforthresults/w?scripcode="
tryit("bse ann requests", lambda: requests.get(ann, headers=BSE_H, timeout=25))
for imp in ("chrome", "chrome124", "safari17_0", "firefox133"):
    def f(imp=imp):
        s = creq.Session(impersonate=imp)
        s.get("https://www.bseindia.com/corporates/ann.html", timeout=20)
        return s.get(ann, headers={"Referer": "https://www.bseindia.com/", "Origin": "https://www.bseindia.com"}, timeout=25)
    tryit(f"bse ann curl_cffi {imp}", f)
def f2():
    s = creq.Session(impersonate="chrome")
    return s.get(fth, headers={"Referer": "https://www.bseindia.com/", "Origin": "https://www.bseindia.com"}, timeout=25)
tryit("bse forthcoming curl_cffi chrome", f2)

# NSE
def nse(url):
    s = creq.Session(impersonate="chrome")
    s.get("https://www.nseindia.com/", timeout=20)
    s.get("https://www.nseindia.com/companies-listing/corporate-filings-board-meetings", timeout=20)
    return s.get(url, headers={"Referer": "https://www.nseindia.com/"}, timeout=25)
tryit("nse board meetings", lambda: nse("https://www.nseindia.com/api/corporate-board-meetings?index=equities"))
tryit("nse event calendar", lambda: nse("https://www.nseindia.com/api/event-calendar"))
fd = (today - dt.timedelta(days=10)).strftime("%d-%m-%Y"); td = today.strftime("%d-%m-%Y")
tryit("nse fin results", lambda: nse(f"https://www.nseindia.com/api/corporates-financial-results?index=equities&period=Quarterly&from_date={fd}&to_date={td}"))
tryit("nse announcements", lambda: nse(f"https://www.nseindia.com/api/corporate-announcements?index=equities&from_date={fd}&to_date={td}"))

# NSE archives PR zip
for back in range(0, 6):
    d = today - dt.timedelta(days=back)
    url = f"https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR{d:%d%m%y}.zip"
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=25)
        print("== PR", d, r.status_code, len(r.content), flush=True)
        if r.status_code == 200 and r.content[:2] == b"PK":
            z = zipfile.ZipFile(io.BytesIO(r.content))
            print("   files:", z.namelist())
            for n in z.namelist():
                low = n.lower()
                if low.startswith(("bm", "an", "bc")):
                    txt = z.read(n).decode("latin-1")
                    print(f"   --- {n} ({len(txt)} chars)\n" + txt[:2500])
            break
    except Exception as e:
        print("== PR EXC", e)

# Yahoo
import yfinance as yf
for t in ("HDFCBANK.NS", "TATAELXSI.NS", "ANGELONE.NS"):
    try:
        print("== yf calendar", t, yf.Ticker(t).calendar)
    except Exception as e:
        print("== yf calendar EXC", t, e)
    try:
        ed = yf.Ticker(t).earnings_dates
        print("== yf earnings_dates", t, None if ed is None else ed.head(4).to_string())
    except Exception as e:
        print("== yf earnings_dates EXC", t, e)
