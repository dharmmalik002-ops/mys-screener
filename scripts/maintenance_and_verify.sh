#!/usr/bin/env bash
set -u

cd "$(dirname "$0")/.." || exit 1

HOST=dharmmalik-stock-scanner-backend.hf.space
MAINT_TOKEN=$(grep MAINTENANCE_TRIGGER_TOKEN backend/.env | cut -d'=' -f2- || true)

echo "Waiting for Space health..."
hc="000"
for i in $(seq 1 30); do
  hc=$(curl -sS -o /dev/null -w "%{http_code}" "https://${HOST}/api/health" || echo "000")
  echo "Attempt $i: $hc"
  if [ "$hc" = "200" ]; then
    echo "Space healthy"
    break
  fi
  sleep 6
done

if [ "$hc" != "200" ]; then
  echo "Space did not become healthy in time (status=$hc); attempting maintenance anyway"
fi

echo "Calling maintenance endpoint..."
curl -sS -X POST "https://${HOST}/api/maintenance/eod-refresh" -H "x-maintenance-token: $MAINT_TOKEN" -H "Content-Type: application/json" -d '{}' -w '\nMAINT_STATUS:%{http_code}\n' || true

echo "Calling watchdog/fix..."
curl -sS -X POST "https://${HOST}/api/watchdog/fix" -H "Content-Type: application/json" -d '{}' -w '\nWATCHDOG_STATUS:%{http_code}\n' || true

echo "Saving test watchlist..."
PAYLOAD='{"market":"india","active_watchlist_id":"wl-1","watchlists":[{"id":"wl-1","name":"Core","color":"#4f8cff","symbols":["INFY","TCS"]}]}'
echo "$PAYLOAD" | curl -sS -X PUT "https://${HOST}/api/watchlists" -H "Content-Type: application/json" -d @- | python3 -m json.tool || true

echo "Fetching watchlists..."
curl -sS "https://${HOST}/api/watchlists" | python3 -m json.tool || true

echo "Fetching groups (india)..."
curl -sS "https://${HOST}/api/india/groups" | python3 -m json.tool || true

echo "Chart INFY (1D):"
curl -s -w '\nCHART_STATUS:%{http_code} TIME:%{time_total}\n' "https://${HOST}/api/chart/INFY?timeframe=1D" -o /tmp/chart_infy.json || true
echo "Chart file size:"
wc -c /tmp/chart_infy.json || true

echo "Done."
