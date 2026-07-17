import { useEffect, useMemo, useRef, useState } from "react";
import { ColorType, createChart, type IChartApi, type ISeriesApi, type UTCTimestamp } from "lightweight-charts";
import type { ChartBar } from "../lib/api";

import "./IndexCandleChart.css";

/** Exponential moving average over closes, same length as input. */
function emaSeries(closes: number[], span: number): number[] {
  if (!closes.length) return [];
  const k = 2 / (span + 1);
  const out: number[] = [closes[0]];
  for (let i = 1; i < closes.length; i += 1) out.push(closes[i] * k + out[i - 1] * (1 - k));
  return out;
}

function smaAt(closes: number[], idx: number, span: number): number | null {
  if (idx + 1 < span) return null;
  let sum = 0;
  for (let i = idx - span + 1; i <= idx; i += 1) sum += closes[i];
  return sum / span;
}

const OVERLAYS = [
  { key: "ema10", label: "EMA10", color: "#22d3ee" },
  { key: "ema21", label: "EMA21", color: "#f59e0b" },
  { key: "ema50", label: "EMA50", color: "#8b5cf6" },
  { key: "sma200", label: "SMA200", color: "#64748b" },
] as const;

type OverlayKey = (typeof OVERLAYS)[number]["key"];

type LegendState = {
  o: number;
  h: number;
  l: number;
  c: number;
  changePct: number | null;
  overlays: Record<OverlayKey, number | null>;
};

const fmt = (v: number | null | undefined, digits = 2) =>
  v == null || !Number.isFinite(v) ? "—" : v.toLocaleString("en-IN", { minimumFractionDigits: digits, maximumFractionDigits: digits });

/**
 * Interactive daily candlestick chart for an index: candles + EMA10/21/50 +
 * SMA200 overlays, crosshair OHLC legend with live overlay values, zoom/pan.
 */
export function IndexCandleChart({ bars, height = 320 }: { bars: ChartBar[]; height?: number }) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const [legend, setLegend] = useState<LegendState | null>(null);

  const computed = useMemo(() => {
    const clean = bars.filter((b) => Number.isFinite(b.close) && b.close > 0);
    const closes = clean.map((b) => b.close);
    return {
      clean,
      closes,
      ema10: emaSeries(closes, 10),
      ema21: emaSeries(closes, 21),
      ema50: emaSeries(closes, 50),
      sma200: closes.map((_, i) => smaAt(closes, i, 200)),
    };
  }, [bars]);

  useEffect(() => {
    const node = containerRef.current;
    if (!node || computed.clean.length < 2) return;

    const styles = getComputedStyle(document.documentElement);
    const textColor = styles.getPropertyValue("--text-muted").trim() || "#64748b";
    const lineColor = styles.getPropertyValue("--line").trim() || "rgba(100,140,200,0.15)";

    const chart = createChart(node, {
      height,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor,
        fontSize: 11,
        fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace",
      },
      grid: {
        vertLines: { color: lineColor },
        horzLines: { color: lineColor },
      },
      rightPriceScale: { borderVisible: false },
      timeScale: { borderVisible: false, rightOffset: 4 },
      crosshair: { horzLine: { labelVisible: true }, vertLine: { labelVisible: true } },
      autoSize: true,
    });
    chartRef.current = chart;

    const candles = chart.addCandlestickSeries({
      upColor: "#22c55e",
      downColor: "#ef4444",
      wickUpColor: "#22c55e",
      wickDownColor: "#ef4444",
      borderVisible: false,
      priceFormat: { type: "price", precision: 2, minMove: 0.05 },
    });
    candles.setData(
      computed.clean.map((b) => ({
        time: b.time as UTCTimestamp,
        open: b.open,
        high: b.high,
        low: b.low,
        close: b.close,
      })),
    );

    const overlaySeries: Partial<Record<OverlayKey, ISeriesApi<"Line">>> = {};
    const overlayValues: Record<OverlayKey, number[] | (number | null)[]> = {
      ema10: computed.ema10,
      ema21: computed.ema21,
      ema50: computed.ema50,
      sma200: computed.sma200,
    };
    for (const overlay of OVERLAYS) {
      const series = chart.addLineSeries({
        color: overlay.color,
        lineWidth: overlay.key === "sma200" ? 2 : 1,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      });
      series.setData(
        computed.clean
          .map((b, i) => ({ time: b.time as UTCTimestamp, value: overlayValues[overlay.key][i] }))
          .filter((p): p is { time: UTCTimestamp; value: number } => p.value != null && Number.isFinite(p.value)),
      );
      overlaySeries[overlay.key] = series;
    }

    const timeToIdx = new Map<number, number>(computed.clean.map((b, i) => [b.time, i]));
    const legendFor = (idx: number): LegendState => {
      const b = computed.clean[idx];
      const prev = idx > 0 ? computed.clean[idx - 1].close : null;
      return {
        o: b.open,
        h: b.high,
        l: b.low,
        c: b.close,
        changePct: prev ? ((b.close / prev) - 1) * 100 : null,
        overlays: {
          ema10: computed.ema10[idx] ?? null,
          ema21: computed.ema21[idx] ?? null,
          ema50: computed.ema50[idx] ?? null,
          sma200: computed.sma200[idx] ?? null,
        },
      };
    };
    setLegend(legendFor(computed.clean.length - 1));

    chart.subscribeCrosshairMove((param) => {
      const t = param.time as number | undefined;
      const idx = t != null ? timeToIdx.get(t) : undefined;
      setLegend(legendFor(idx ?? computed.clean.length - 1));
    });

    chart.timeScale().setVisibleLogicalRange({
      from: Math.max(0, computed.clean.length - 130),
      to: computed.clean.length + 3,
    });

    return () => {
      chartRef.current = null;
      chart.remove();
    };
  }, [computed, height]);

  if (computed.clean.length < 2) return null;

  return (
    <div className="icc-wrap">
      {legend ? (
        <div className="icc-legend" aria-live="off">
          <span className="icc-ohlc">
            O <em>{fmt(legend.o)}</em> H <em>{fmt(legend.h)}</em> L <em>{fmt(legend.l)}</em> C{" "}
            <em className={legend.changePct != null && legend.changePct < 0 ? "neg" : "pos"}>{fmt(legend.c)}</em>
            {legend.changePct != null ? (
              <em className={legend.changePct < 0 ? "neg" : "pos"}>
                {" "}({legend.changePct >= 0 ? "+" : ""}{legend.changePct.toFixed(2)}%)
              </em>
            ) : null}
          </span>
          <span className="icc-mas">
            {OVERLAYS.map((o) => (
              <span key={o.key} style={{ color: o.color }}>
                {o.label} <em>{fmt(legend.overlays[o.key], 0)}</em>
              </span>
            ))}
          </span>
        </div>
      ) : null}
      <div ref={containerRef} className="icc-chart" style={{ height }} />
    </div>
  );
}
