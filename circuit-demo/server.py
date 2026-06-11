"""
Circuit Limit demo — standalone FastAPI backend.

A self-contained teaching/demo service (separate from the main scanner backend)
that computes daily Upper/Lower Circuit bands for a few Indian stocks and serves
mock OHLC candles for the standalone TradingView Lightweight Charts front-end in
index.html.

Run:
    pip install fastapi uvicorn
    python circuit-demo/server.py          # serves on http://127.0.0.1:8100
    # then open circuit-demo/index.html in a browser (it fetches from :8100)

Math:
    Upper Circuit = PrevClose * (1 + C)
    Lower Circuit = PrevClose * (1 - C)
For "Dynamic" (F&O) stocks the bands shift intraday, so we return null bands and
the front-end draws no static lines.
"""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# --- Mock "database" -------------------------------------------------------
# circuit_pct is the fractional band (0.20 = 20%); None => dynamic (F&O).
STOCKS: dict[str, dict] = {
    "ZOMATO": {
        "name": "Zomato Ltd",
        "previous_close": 180.50,
        "circuit_pct": 0.20,
        "circuit_type": "Fixed",
        # 5 daily candles; the last one rallies right into the upper circuit.
        "candles": [
            {"time": "2026-06-02", "open": 171.0, "high": 175.2, "low": 169.5, "close": 174.0},
            {"time": "2026-06-03", "open": 174.0, "high": 179.0, "low": 173.2, "close": 178.4},
            {"time": "2026-06-04", "open": 178.4, "high": 181.0, "low": 176.8, "close": 180.5},
            {"time": "2026-06-05", "open": 181.0, "high": 199.5, "low": 180.2, "close": 198.7},
            # Approaches/hits the 216.60 upper circuit (within 0.5%).
            {"time": "2026-06-08", "open": 205.0, "high": 216.4, "low": 204.0, "close": 215.9},
        ],
    },
    "SUZLON": {
        "name": "Suzlon Energy Ltd",
        "previous_close": 42.10,
        "circuit_pct": 0.05,
        "circuit_type": "Fixed",
        "candles": [
            {"time": "2026-06-02", "open": 41.0, "high": 41.8, "low": 40.6, "close": 41.5},
            {"time": "2026-06-03", "open": 41.5, "high": 42.0, "low": 41.1, "close": 41.9},
            {"time": "2026-06-04", "open": 41.9, "high": 42.3, "low": 41.6, "close": 42.1},
            # Slides toward the 39.995 lower circuit.
            {"time": "2026-06-05", "open": 42.1, "high": 42.2, "low": 40.3, "close": 40.5},
            {"time": "2026-06-08", "open": 40.5, "high": 40.7, "low": 40.0, "close": 40.05},
        ],
    },
    "RELIANCE": {
        "name": "Reliance Industries Ltd",
        "previous_close": 2450.00,
        "circuit_pct": None,  # F&O -> dynamic bands, no static lines
        "circuit_type": "Dynamic (F&O)",
        "candles": [
            {"time": "2026-06-02", "open": 2410.0, "high": 2440.0, "low": 2400.0, "close": 2432.0},
            {"time": "2026-06-03", "open": 2432.0, "high": 2460.0, "low": 2425.0, "close": 2451.0},
            {"time": "2026-06-04", "open": 2451.0, "high": 2475.0, "low": 2448.0, "close": 2469.0},
            {"time": "2026-06-05", "open": 2469.0, "high": 2488.0, "low": 2455.0, "close": 2460.0},
            {"time": "2026-06-08", "open": 2460.0, "high": 2470.0, "low": 2438.0, "close": 2445.0},
        ],
    },
}

app = FastAPI(title="Circuit Limit Demo")

# Allow the static front-end (opened via file:// or any localhost port) to fetch.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _round2(value: float | None) -> float | None:
    return round(value, 2) if value is not None else None


@app.get("/api/stock/{ticker}")
def get_stock(ticker: str):
    key = ticker.strip().upper()
    stock = STOCKS.get(key)
    if stock is None:
        raise HTTPException(status_code=404, detail=f"Unknown ticker: {ticker}")

    prev_close = stock["previous_close"]
    pct = stock["circuit_pct"]
    if pct is None:
        upper = lower = None
        band_text = "Dynamic (F&O)"
    else:
        upper = _round2(prev_close * (1 + pct))
        lower = _round2(prev_close * (1 - pct))
        band_text = f"{stock['circuit_type']} {int(round(pct * 100))}%"

    return {
        "ticker": key,
        "name": stock["name"],
        "previous_close": prev_close,
        "circuit_type": stock["circuit_type"],
        "circuit_pct": pct,
        "circuit_band_text": band_text,
        "upper_circuit": upper,
        "lower_circuit": lower,
        "is_dynamic": pct is None,
        "candles": stock["candles"],
    }


@app.get("/api/stocks")
def list_stocks():
    """Tickers for the front-end dropdown."""
    return [{"ticker": k, "name": v["name"]} for k, v in STOCKS.items()]


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8100)
