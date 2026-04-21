import asyncio
import json
from datetime import datetime, timezone
import sys

sys.path.insert(0, 'backend')
from app.models.market import StockSnapshot
from app.scanners.definitions import SCANS, run_scan

def main():
    rows = json.load(open("backend/data/free_snapshots.json"))
    snaps = [StockSnapshot(**r) for r in rows if r.get('market_cap_crore') and float(r['market_cap_crore']) >= 800]
    print(f"Loaded {len(snaps)} eligible snapshots")
    
    for scan in SCANS:
        matches = run_scan(scan, snaps)
        print(f"Scan {scan.id}: {len(matches)} matches")

if __name__ == '__main__':
    main()
