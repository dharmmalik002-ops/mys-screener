"""Quarterly standalone results (sales, net profit, EPS lines) per BSE scrip, 2009+.

BSE's own feeds: `CorprateResultbeta` lists every quarter a company has filed
with its quarter code (back to 2000); `Corp_detailedResult_Transpose_ng`
returns that quarter's line items. One gzipped JSON per symbol in
data/quarterly_results/ (gitignored raw material), resumable. About an hour
for the universe at 10 threads; BSE throttles above that. Measured use:
CLAUDE.md gotcha 120.

    python3 scripts/build_quarterly_results.py 10
"""
import gzip, json, os, sys, threading, time
from concurrent.futures import ThreadPoolExecutor
import requests
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
from app.services.earnings_metrics import BSE_HEADERS
OUT = str(__import__("pathlib").Path(__file__).resolve().parents[1] / "data" / "quarterly_results")
os.makedirs(OUT, exist_ok=True)
API = "https://api.bseindia.com/BseIndiaAPI/api/"
loc = threading.local()
def sess():
    if not hasattr(loc, "s"):
        loc.s = requests.Session(); loc.s.headers.update(BSE_HEADERS)
    return loc.s
def get(ep, params):
    for a in range(5):
        try:
            r = sess().get(API + ep, params=params, timeout=40)
            if r.status_code == 200 and r.text.strip().startswith(("{", "[")): return r.json()
        except Exception: pass
        time.sleep(2 * (a + 1))
    return None
def one(sym_code):
    sym, code = sym_code
    path = f"{OUT}/{sym}.json.gz"
    if os.path.exists(path): return "skip"
    lst = get("CorprateResultbeta/w", {"scripcode": code})
    if lst is None: return f"{sym} FAIL list"
    qs = []
    for row in lst.get("Table") or []:
        for k in ("Q1", "Q2", "Q3", "Q4"):
            v = row.get(k)
            if not v: continue
            parts = v.split(";")
            if len(parts) >= 3:
                lab, qc = parts[0], parts[2]
                try: yr = 2000 + int(lab[-2:])
                except ValueError: continue
                if yr >= 2009: qs.append((lab, qc))
    out, fails = [], 0
    for lab, qc in qs:
        j = get("Corp_detailedResult_Transpose_ng/w", {"Scrip_cd": code, "Qtr": qc})
        if j is None: fails += 1; continue
        rows = [[x.get("fld_desc"), x.get("Value")] for x in (j.get("table1") or [])]
        out.append({"q": lab, "code": qc, "rows": rows})
    with gzip.open(path, "wt") as fh: json.dump({"symbol": sym, "bse": code, "quarters": out, "fails": fails}, fh)
    return f"{sym} {len(out)}q fails {fails}"
uni = json.load(open(__import__("pathlib").Path(__file__).resolve().parents[1] / "data" / "free_universe.json"))
pairs = [(r["symbol"].upper(), str(r["bse_code"])) for r in uni if r.get("bse_code")]
with ThreadPoolExecutor(int(sys.argv[1]) if len(sys.argv) > 1 else 6) as ex:
    for i, m in enumerate(ex.map(one, pairs)):
        if m != "skip": print(i, len(pairs), m, flush=True)
print("DONE", flush=True)
