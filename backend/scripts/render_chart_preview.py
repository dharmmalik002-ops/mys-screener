#!/usr/bin/env python3
"""Render digest chart PNGs to disk for eyeballing.

The appearance loop for chart_render.py: no server, no HTTP, no Telegram.
Reads bars through the real provider path (so the EOD gap-fill and the disk
cache are exercised), or with --synthetic to work fully offline.

    cd backend
    python scripts/render_chart_preview.py RELIANCE TITAN CDSL --out /tmp/preview
    python scripts/render_chart_preview.py --synthetic --out /tmp/preview
"""

from __future__ import annotations

import argparse
import asyncio
import math
import struct
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.chart_render import (  # noqa: E402
    ChartAnnotation,
    RenderSpec,
    render_candles_png,
)


def synthetic_bars(count: int = 520, *, seed: float = 1400.0) -> list[dict]:
    """Deterministic pseudo-random walk with a visible base-and-breakout shape,
    so the chart exercises MAs, a volume spike and both candle colours."""
    bars: list[dict] = []
    price = seed
    start = 1_700_000_000
    for i in range(count):
        # Deterministic wobble plus a late uptrend.
        wobble = math.sin(i / 7.0) * 0.012 + math.sin(i / 31.0) * 0.02
        drift = 0.0018 if i > count - 90 else 0.0002
        price = max(1.0, price * (1.0 + wobble * 0.35 + drift))
        open_ = price * (1 - wobble * 0.15)
        close = price
        high = max(open_, close) * (1 + abs(wobble) * 0.4 + 0.002)
        low = min(open_, close) * (1 - abs(wobble) * 0.4 - 0.002)
        volume = int(500_000 * (1 + abs(math.sin(i / 5.0))) * (3.0 if i == count - 3 else 1.0))
        bars.append(
            {
                "time": start + i * 86_400,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
        )
    return bars


async def load_bars(symbol: str, bar_count: int) -> list:
    from app.core.config import get_settings
    from app.providers.factory import build_provider

    settings = get_settings()
    provider = build_provider(settings, market="india")
    return await provider.get_chart(symbol, "1D", bars=bar_count)


def png_dimensions(data: bytes) -> tuple[int, int]:
    return struct.unpack(">II", data[16:24])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("symbols", nargs="*", default=[], help="NSE symbols to render")
    parser.add_argument("--out", default="/tmp/chart-preview", help="output directory")
    parser.add_argument("--bars", type=int, default=520, help="bars to FETCH (compute window)")
    parser.add_argument("--display", type=int, default=130, help="bars to DISPLAY")
    parser.add_argument("--width", type=int, default=1280)
    parser.add_argument("--height", type=int, default=800)
    parser.add_argument("--synthetic", action="store_true", help="offline synthetic series")
    parser.add_argument("--no-levels", action="store_true", help="hide trade level lines")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    spec = RenderSpec(
        width_px=args.width,
        height_px=args.height,
        display_bars=args.display,
        show_trade_levels=not args.no_levels,
    )

    jobs: list[tuple[str, list]] = []
    if args.synthetic or not args.symbols:
        jobs.append(("SYNTH", synthetic_bars(args.bars)))
        jobs.append(("SYNTH_SHORT", synthetic_bars(40)))
    for symbol in args.symbols:
        try:
            jobs.append((symbol, asyncio.run(load_bars(symbol, args.bars))))
        except Exception as exc:
            print(f"  {symbol:14} FETCH FAILED: {exc}")

    failures = 0
    for symbol, bars in jobs:
        note = ChartAnnotation(
            symbol=symbol,
            name="Preview Industries Ltd." if symbol.startswith("SYNTH") else None,
            exchange="NSE",
            sector="Refineries",
            market_cap_crore=1_942_100,
            last_price=float(bars[-1]["close"] if isinstance(bars[-1], dict) else bars[-1].close),
            change_pct=3.24,
            rs_rating=92,
            relative_volume=3.2,
            scanner_name="VCP Contraction",
            is_new=True,
            also_in=("Minervini 5M", "3 Tight Closes"),
            session_date="2026-08-20",
        )
        try:
            png = render_candles_png(bars, note, spec)
        except Exception as exc:
            print(f"  {symbol:14} RENDER FAILED: {exc}")
            failures += 1
            continue
        path = out_dir / f"{symbol}.png"
        path.write_bytes(png)
        width, height = png_dimensions(png)
        print(f"  {symbol:14} {width}x{height}  {len(png) / 1024:7.1f} KB  -> {path}")

    print(f"\n{len(jobs) - failures}/{len(jobs)} rendered into {out_dir}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
