#!/usr/bin/env python3
"""Last 12 quarters of standalone results per company, from BSE, committed.

    python3 scripts/build_quarterly_summary.py            # incremental (default)
    python3 scripts/build_quarterly_summary.py --limit 20 # smoke test
    python3 scripts/build_quarterly_summary.py --refresh  # re-fetch every quarter

Writes `data/quarterly_summary.json`, which `app/services/bse_quarterly.py`
serves whenever Screener fails (it refuses the HF Space for every symbol).
Incremental by design: each run lists every company's filed quarters (one
request each) and fetches only quarter codes the file does not already hold,
so after the first build a nightly run costs ~1,550 list calls plus that
day's new results. Refuses to shrink the file, so a run BSE blocks cannot
empty the widget.
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services import bse_quarterly as bq  # noqa: E402
from app.services.earnings_metrics import BSE_HEADERS  # noqa: E402

API = "https://api.bseindia.com/BseIndiaAPI/api/"
_local = threading.local()


def _session() -> requests.Session:
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
        _local.session.headers.update(BSE_HEADERS)
    return _local.session


def _get(endpoint: str, params: dict) -> dict | None:
    for attempt in range(4):
        try:
            response = _session().get(API + endpoint, params=params, timeout=30)
            if response.status_code == 200 and response.text.strip().startswith(("{", "[")):
                return response.json()
        except Exception:  # noqa: BLE001 - one bad company must not stop the run
            pass
        time.sleep(1.5 * (attempt + 1))
    return None


def update_company(symbol: str, code: str, existing: list[dict], refresh: bool) -> tuple[str, list[dict] | None, int]:
    listing = _get("CorprateResultbeta/w", {"scripcode": code})
    if listing is None:
        return symbol, None, 0
    wanted = bq.quarter_codes(listing)[: bq.QUARTERS_KEPT]
    have = {row["period"]: row for row in existing} if not refresh else {}
    rows: list[dict] = []
    fetched = 0
    for period, quarter_code, url in wanted:
        if period in have:
            rows.append(have[period])
            continue
        detail = _get("Corp_detailedResult_Transpose_ng/w", {"Scrip_cd": code, "Qtr": quarter_code})
        fetched += 1
        if detail is None:
            continue
        parsed = bq.parse_quarter(detail.get("table1") or [], period, url)
        if parsed is not None:
            parsed.pop("result_document_url", None)
            parsed["qtr"] = quarter_code
            rows.append(parsed)
    return symbol, rows, fetched


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()

    universe = json.loads((BACKEND_ROOT / "data" / "free_universe.json").read_text(encoding="utf-8"))
    pairs = [(str(r["symbol"]).upper(), str(r["bse_code"])) for r in universe if r.get("symbol") and r.get("bse_code")]
    if args.limit:
        pairs = pairs[: args.limit]

    previous: dict = {}
    if bq.ARTIFACT_PATH.exists():
        try:
            previous = json.loads(bq.ARTIFACT_PATH.read_text(encoding="utf-8"))
        except ValueError:
            previous = {}
    symbols: dict[str, dict] = dict(previous.get("symbols") or {})

    started = time.time()
    done = failed = fetched_total = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(update_company, sym, code, (symbols.get(sym) or {}).get("quarters") or [], args.refresh)
            for sym, code in pairs
        ]
        for n, future in enumerate(as_completed(futures), start=1):
            symbol, rows, fetched = future.result()
            fetched_total += fetched
            if rows:
                code = next(c for s, c in pairs if s == symbol)
                symbols[symbol] = {"bse": code, "quarters": [bq.compact_row(r) for r in rows]}
                done += 1
            elif rows is None:
                failed += 1
            if n % 100 == 0 or n == len(pairs):
                print(f"[{n}/{len(pairs)}] ok={done} failed={failed} quarters fetched={fetched_total} "
                      f"({time.time() - started:.0f}s)", flush=True)

    # Rows kept from an earlier run may predate the compact form.
    for entry in symbols.values():
        entry["quarters"] = [bq.compact_row(r) for r in entry.get("quarters") or []]
    before = len(previous.get("symbols") or {})
    if len(symbols) < before:
        print(f"refusing to shrink the file: {len(symbols)} companies < {before} before", flush=True)
        return 1
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "BSE standalone results (Rs million / 10 = crore)",
        "quarters_kept": bq.QUARTERS_KEPT,
        "symbols": dict(sorted(symbols.items())),
    }
    bq.ARTIFACT_PATH.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    size_kb = bq.ARTIFACT_PATH.stat().st_size / 1024
    print(f"wrote {bq.ARTIFACT_PATH.name}: {len(symbols)} companies, {size_kb:.0f} KB", flush=True)
    return 0 if done or not pairs else 1


if __name__ == "__main__":
    raise SystemExit(main())
