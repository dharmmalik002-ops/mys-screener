# Circuit Limit Demo (standalone)

A self-contained illustration of daily Upper/Lower Circuit band indicators on an
Indian stock chart. It is **independent** of the main scanner app.

## Run

```bash
pip install fastapi uvicorn
python circuit-demo/server.py        # http://127.0.0.1:8100
```

Then open `circuit-demo/index.html` in a browser (double-click, or use any static
server). The page fetches from `http://127.0.0.1:8100` (CORS is enabled).

## What it shows

- Dropdown over three mock stocks:
  - **ZOMATO** — fixed 20% band, prev close ₹180.50 (UC ₹216.60 / LC ₹144.40); last candle rallies into the upper circuit.
  - **SUZLON** — fixed 5% band, prev close ₹42.10 (UC ₹44.21 / LC ₹40.00); last candle slides into the lower circuit.
  - **RELIANCE** — F&O "Dynamic" band, prev close ₹2450.00; no static lines drawn.
- Candlestick series (TradingView Lightweight Charts via CDN).
- Dashed Upper (red) / Lower (green) circuit price lines for fixed-band stocks.
- Info panel; its border turns **yellow/red** and a status warning appears when
  the latest candle's high/low is within 0.5% of a band.

## Math

```
Upper Circuit = PrevClose * (1 + C)
Lower Circuit = PrevClose * (1 - C)
```

Dynamic (F&O) names return `null` bands (they shift intraday), so the front-end
draws no static lines.
