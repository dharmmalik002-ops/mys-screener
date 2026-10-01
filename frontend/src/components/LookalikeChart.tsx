import { useEffect, useRef } from "react";

import { CANDLE_DOWN, CANDLE_UP } from "../lib/marketColors";

/* One canvas chart for every look-alike view.

   Draws a setup the way the model saw it — 120 sessions, candles over the
   50-day average, volume underneath — and, when the data carries them, the
   sessions that followed, on a shaded background after a dashed line at the
   setup date. That is the "take me to the date when it was similar" view: the
   chart sits at the setup date, and the shading shows what happened next.

   Prices arrive normalised to the setup window's own range (0..1); the sessions
   after may run outside it, so the vertical scale is fitted to whatever is
   drawn. `lo` / `hi` turn the scale back into prices for the axis labels.
   Canvas cannot read CSS variables, so the colours here are fixed. */

export type LookalikeSeries = {
  o: number[];
  h: number[];
  l: number[];
  c: number[];
  sma?: number[];
  v?: number[];
  setup_index?: number;
  lo?: number;
  hi?: number;
  dates?: { start: string; setup: string; end: string };
};

const SMA_COLOUR = "#5a78c8";
const VOLUME_COLOUR = "rgba(128, 128, 128, 0.35)";
const AFTER_SHADE = "rgba(120, 120, 120, 0.08)";
const SETUP_LINE = "rgba(80, 80, 80, 0.55)";
const LABEL_COLOUR = "#6b6f72";

function fmtDate(iso?: string) {
  if (!iso) return "";
  const d = new Date(`${iso}T00:00:00`);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "2-digit" });
}

function fmtPrice(v: number) {
  if (!Number.isFinite(v)) return "";
  return v >= 1000 ? v.toFixed(0) : v >= 100 ? v.toFixed(1) : v.toFixed(2);
}

export function LookalikeChart({
  data,
  height = 150,
  showAfter = true,
  labels = false,
  ariaLabel,
}: {
  data: LookalikeSeries | null | undefined;
  height?: number;
  showAfter?: boolean;
  labels?: boolean;
  ariaLabel: string;
}) {
  const ref = useRef<HTMLCanvasElement | null>(null);

  useEffect(() => {
    const canvas = ref.current;
    if (!canvas || !data?.c?.length) return;
    const dpr = window.devicePixelRatio || 1;
    const cssW = canvas.clientWidth || 300;
    const cssH = canvas.clientHeight || height;
    canvas.width = Math.round(cssW * dpr);
    canvas.height = Math.round(cssH * dpr);
    const g = canvas.getContext("2d");
    if (!g) return;
    g.setTransform(dpr, 0, 0, dpr, 0, 0);
    g.clearRect(0, 0, cssW, cssH);

    const setup = data.setup_index ?? data.c.length - 1;
    const n = showAfter ? data.c.length : Math.min(data.c.length, setup + 1);
    const slice = <T,>(a: T[] | undefined) => (a ?? []).slice(0, n);
    const o = slice(data.o), h = slice(data.h), l = slice(data.l), c = slice(data.c);
    const sma = slice(data.sma), v = slice(data.v);

    const padL = labels ? 4 : 0;
    const padR = labels ? 46 : 0;
    const padB = labels ? 16 : 0;
    const plotW = cssW - padL - padR;
    const priceH = (cssH - padB) * 0.78;
    const volTop = (cssH - padB) * 0.8;
    const volH = cssH - padB - volTop;
    const vMin = Math.min(...l, ...(sma.length ? sma : l));
    const vMax = Math.max(...h, ...(sma.length ? sma : h));
    const span = vMax - vMin || 1;
    const step = plotW / n;
    const body = Math.max(1, step * 0.6);
    const y = (value: number) => 3 + (1 - (value - vMin) / span) * (priceH - 6);
    const x = (i: number) => padL + (i + 0.5) * step;

    if (showAfter && n > setup + 1) {
      g.fillStyle = AFTER_SHADE;
      g.fillRect(x(setup) + step / 2, 0, plotW - (x(setup) + step / 2 - padL), cssH - padB);
    }

    for (let i = 0; i < n; i += 1) {
      g.fillStyle = VOLUME_COLOUR;
      const vh = (v[i] ?? 0) * volH;
      g.fillRect(x(i) - body / 2, cssH - padB - vh, body, vh);
      const up = c[i] >= o[i];
      g.strokeStyle = up ? CANDLE_UP : CANDLE_DOWN;
      g.fillStyle = up ? CANDLE_UP : CANDLE_DOWN;
      g.lineWidth = 1;
      g.beginPath();
      g.moveTo(x(i), y(h[i]));
      g.lineTo(x(i), y(l[i]));
      g.stroke();
      const top = Math.min(y(o[i]), y(c[i]));
      g.fillRect(x(i) - body / 2, top, body, Math.max(1, Math.abs(y(o[i]) - y(c[i]))));
    }

    if (sma.length) {
      g.strokeStyle = SMA_COLOUR;
      g.lineWidth = 1.5;
      g.beginPath();
      sma.forEach((value, i) => (i === 0 ? g.moveTo(x(i), y(value)) : g.lineTo(x(i), y(value))));
      g.stroke();
    }

    if (showAfter && n > setup + 1) {
      g.strokeStyle = SETUP_LINE;
      g.setLineDash([4, 3]);
      g.beginPath();
      g.moveTo(x(setup) + step / 2, 0);
      g.lineTo(x(setup) + step / 2, cssH - padB);
      g.stroke();
      g.setLineDash([]);
    }

    if (labels) {
      g.fillStyle = LABEL_COLOUR;
      g.font = "10px -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif";
      if (data.lo != null && data.hi != null) {
        const toPrice = (u: number) => (data.lo as number) + u * ((data.hi as number) - (data.lo as number));
        g.textAlign = "left";
        g.fillText(fmtPrice(toPrice(vMax)), padL + plotW + 4, 10);
        g.fillText(fmtPrice(toPrice(vMin)), padL + plotW + 4, priceH - 2);
      }
      if (data.dates) {
        g.textAlign = "left";
        g.fillText(fmtDate(data.dates.start), padL, cssH - 3);
        if (showAfter && n > setup + 1) {
          // The setup date sits at the top, beside the dashed line, so it can
          // never collide with the start and end dates along the bottom.
          g.textAlign = "right";
          g.fillText(fmtDate(data.dates.setup), x(setup) + step / 2 - 3, 10);
          g.fillText(fmtDate(data.dates.end), padL + plotW, cssH - 3);
        } else {
          g.textAlign = "right";
          g.fillText(fmtDate(data.dates.setup), padL + plotW, cssH - 3);
        }
      }
    }
  }, [data, height, showAfter, labels]);

  if (!data?.c?.length) {
    return (
      <div className="lookalike-canvas lookalike-missing" style={{ height }}>
        Chart unavailable
      </div>
    );
  }
  return <canvas ref={ref} className="lookalike-canvas" style={{ height }} role="img" aria-label={ariaLabel} />;
}
