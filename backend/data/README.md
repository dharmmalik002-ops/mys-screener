`sample_universe.json` powers the demo mode so the app stays usable before live credentials are added.

For free live mode:

1. Download the quarterly NSE market-cap sheet from the official NSE page and export it as CSV.
2. Download `NSE.json.gz` and `BSE.json.gz` from the Upstox instruments feed.
3. Build the filtered universe:

```bash
cd backend
python3 -m app.services.universe_builder \
  --market-cap-csv data/market_caps.csv \
  --nse-json-gz data/NSE.json.gz \
  --bse-json-gz data/BSE.json.gz \
  --threshold 1000 \
  --output data/live_universe.json
```

The generated `live_universe.json` is the handoff point for the next live-cache step.
