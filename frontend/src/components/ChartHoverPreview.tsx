import { useEffect, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";
import type { ChartBar } from "../lib/api";
import "./ChartHoverPreview.css";

/* A glance at a stock's daily chart while the pointer rests on its row, so a
   scan can be skimmed without opening the full chart for every name. Drawn as
   plain SVG from bars the app already caches (the hover prefetch usually has
   them before the preview appears) — mounting the charting library per hover
   would cost more than the glance is worth. Not interactive: pointer-events are
   off so it can never sit between the cursor and the next row. */

export type ChartPreviewAnchor = { symbol: string; rect: DOMRect };

const SESSIONS = 90;
const SMA_PERIOD = 50;
const WIDTH = 300;
const PRICE_H = 128;
const VOL_H = 30;
const GAP = 6;
const HEIGHT = PRICE_H + GAP + VOL_H;
const PAD_X = 2;

function sma(closes: number[], period: number, index: number): number | null {
  if (index + 1 < period) return null;
  let sum = 0;
  for (let i = index - period + 1; i <= index; i += 1) sum += closes[i];
  return sum / period;
}

export function ChartHoverPreview({
  anchor,
  loadBars,
}: {
  anchor: ChartPreviewAnchor | null;
  loadBars: (symbol: string) => Promise<ChartBar[] | null>;
}) {
  const [state, setState] = useState<{ symbol: string; bars: ChartBar[] | null; failed: boolean } | null>(null);

  useEffect(() => {
    if (!anchor) {
      setState(null);
      return;
    }
    let live = true;
    setState({ symbol: anchor.symbol, bars: null, failed: false });
    loadBars(anchor.symbol).then(
      (bars) => {
        if (live) setState({ symbol: anchor.symbol, bars, failed: !bars || bars.length < 2 });
      },
      () => {
        if (live) setState({ symbol: anchor.symbol, bars: null, failed: true });
      },
    );
    return () => {
      live = false;
    };
  }, [anchor, loadBars]);

  if (!anchor || !state || state.symbol !== anchor.symbol) return null;

  // Beside the row's name cell, flipped above when it would run off the bottom.
  const cardH = HEIGHT + 64;
  const left = Math.min(anchor.rect.right + 12, window.innerWidth - WIDTH - 40);
  const below = anchor.rect.top;
  const top = below + cardH > window.innerHeight - 12 ? Math.max(12, anchor.rect.bottom - cardH) : below;

  let body: ReactNode;
  let header: ReactNode = null;
  if (!state.bars) {
    body = <div className="chp-status">{state.failed ? "Chart unavailable" : "Loading chart…"}</div>;
  } else {
    const all = state.bars;
    const closes = all.map((bar) => bar.close);
    const start = Math.max(0, all.length - SESSIONS);
    const bars = all.slice(start);
    const smaPoints = bars.map((_, i) => sma(closes, SMA_PERIOD, start + i));
    const lows = bars.map((bar) => bar.low);
    const highs = bars.map((bar) => bar.high);
    smaPoints.forEach((value) => {
      if (value != null) {
        lows.push(value);
        highs.push(value);
      }
    });
    const lo = Math.min(...lows);
    const hi = Math.max(...highs);
    const span = hi - lo || 1;
    const y = (price: number) => PRICE_H - ((price - lo) / span) * (PRICE_H - 4) - 2;
    const step = (WIDTH - PAD_X * 2) / bars.length;
    const x = (i: number) => PAD_X + i * step + step / 2;
    const bodyW = Math.max(1, step * 0.62);
    const maxVol = Math.max(1, ...bars.map((bar) => bar.volume || 0));
    const smaPath = smaPoints
      .map((value, i) => (value == null ? null : `${x(i).toFixed(1)},${y(value).toFixed(1)}`))
      .filter(Boolean)
      .join(" ");
    const first = bars[0].close;
    const last = bars[bars.length - 1].close;
    const changePct = first ? ((last - first) / first) * 100 : 0;
    header = (
      <div className="chp-figures">
        <span className="chp-last">{last.toLocaleString("en-IN", { maximumFractionDigits: 2 })}</span>
        <span className={changePct >= 0 ? "chp-chg pos" : "chp-chg neg"}>
          {changePct >= 0 ? "+" : ""}
          {changePct.toFixed(1)}% · {bars.length} sessions
        </span>
      </div>
    );
    body = (
      <svg className="chp-svg" viewBox={`0 0 ${WIDTH} ${HEIGHT}`} width={WIDTH} height={HEIGHT} aria-hidden="true">
        {bars.map((bar, i) => {
          const up = bar.close >= bar.open;
          const bodyTop = y(Math.max(bar.open, bar.close));
          const bottom = y(Math.min(bar.open, bar.close));
          const vol = ((bar.volume || 0) / maxVol) * VOL_H;
          return (
            <g key={bar.time} className={up ? "chp-up" : "chp-down"}>
              <line x1={x(i)} x2={x(i)} y1={y(bar.high)} y2={y(bar.low)} />
              <rect x={x(i) - bodyW / 2} y={bodyTop} width={bodyW} height={Math.max(0.8, bottom - bodyTop)} />
              <rect className="chp-vol" x={x(i) - bodyW / 2} y={HEIGHT - vol} width={bodyW} height={vol} />
            </g>
          );
        })}
        {smaPath ? <polyline className="chp-sma" points={smaPath} /> : null}
      </svg>
    );
  }

  return createPortal(
    <div className="chp-card" style={{ left, top, width: WIDTH + 28 }} role="presentation">
      <div className="chp-head">
        <strong>{anchor.symbol}</strong>
        {header}
      </div>
      {body}
      <div className="chp-legend">
        <span className="chp-legend-sma" /> 50-day average · daily · click for the full chart
      </div>
    </div>,
    document.body,
  );
}
