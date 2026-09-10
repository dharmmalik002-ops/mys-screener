import { useEffect, useMemo, useRef } from "react";
import {
  ColorType,
  CrosshairMode,
  LineStyle,
  createChart,
  type IChartApi,
  type IPriceLine,
  type ISeriesApi,
  type UTCTimestamp,
} from "lightweight-charts";
import type { StudyBar } from "../lib/api";

/** Exponential moving average over closes, same length as input. */
function emaSeries(closes: number[], span: number): (number | null)[] {
  if (!closes.length) return [];
  const k = 2 / (span + 1);
  const out: number[] = [closes[0]];
  for (let i = 1; i < closes.length; i += 1) out.push(closes[i] * k + out[i - 1] * (1 - k));
  // The first `span` values are seeded rather than averaged; drawing them would
  // put a fake EMA on the left edge of every card.
  return out.map((v, i) => (i < span - 1 ? null : v));
}

const OVERLAYS = [
  { key: "ema10", span: 10, color: "#22d3ee", width: 1 },
  { key: "ema21", span: 21, color: "#f59e0b", width: 1 },
  { key: "ema50", span: 50, color: "#8b5cf6", width: 2 },
] as const;

type Props = {
  /** Bars up to and including the trigger session. */
  contextBars: StudyBar[];
  /** Bars after the trigger. Only the first `revealed` of them are drawn. */
  forwardBars: StudyBar[];
  revealed: number;
  entry: number;
  /** The stop the user placed, or null while they have not placed one. */
  stop: number | null;
  /** Called with the price under the click, so the panel can set the stop. */
  onPickPrice?: (price: number) => void;
  height?: number;
};

/**
 * The drill chart. Draws candles truncated at the trigger bar, then extends
 * them one session at a time as the user steps forward.
 *
 * Chart creation and data updates are deliberately split across two effects:
 * placing a stop or revealing a bar must not tear down and rebuild the chart,
 * which would reset the user's zoom on every keypress.
 */
export function StudyChart({
  contextBars,
  forwardBars,
  revealed,
  entry,
  stop,
  onPickPrice,
  height = 420,
}: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candlesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  const overlayRefs = useRef<Record<string, ISeriesApi<"Line">>>({});
  const entryLineRef = useRef<IPriceLine | null>(null);
  const stopLineRef = useRef<IPriceLine | null>(null);
  const pickRef = useRef(onPickPrice);
  pickRef.current = onPickPrice;

  const visible = useMemo(() => {
    const context = contextBars.filter((b) => Number.isFinite(b.close) && b.close > 0);
    const lastContextTime = context.length ? context[context.length - 1].time : -Infinity;
    // Moving to the next card swaps `contextBars` a render before the panel's
    // reset effect clears `forwardBars`, so for one frame the two belong to
    // different symbols. Appending them blindly hands lightweight-charts a
    // series that jumps backwards in time, which it turns into a hard assertion
    // and a blank page. Only bars that actually follow the trigger are drawn.
    const forward = forwardBars
      .filter((b) => Number.isFinite(b.close) && b.close > 0 && b.time > lastContextTime)
      .slice(0, Math.max(0, revealed));
    const clean = [...context, ...forward];
    const closes = clean.map((b) => b.close);
    return {
      clean,
      triggerIndex: contextBars.length - 1,
      emas: Object.fromEntries(OVERLAYS.map((o) => [o.key, emaSeries(closes, o.span)])) as Record<
        string,
        (number | null)[]
      >,
    };
  }, [contextBars, forwardBars, revealed]);

  // --- Create once per mount -------------------------------------------------
  useEffect(() => {
    const node = containerRef.current;
    if (!node) return;

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
      grid: { vertLines: { color: lineColor }, horzLines: { color: lineColor } },
      rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.08, bottom: 0.28 } },
      timeScale: { borderVisible: false, rightOffset: 6 },
      crosshair: { mode: CrosshairMode.Normal },
      autoSize: true,
    });
    chartRef.current = chart;

    candlesRef.current = chart.addCandlestickSeries({
      upColor: "#22c55e",
      downColor: "#ef4444",
      wickUpColor: "#22c55e",
      wickDownColor: "#ef4444",
      borderVisible: false,
      priceFormat: { type: "price", precision: 2, minMove: 0.05 },
    });

    volumeRef.current = chart.addHistogramSeries({
      priceFormat: { type: "volume" },
      priceScaleId: "vol",
      priceLineVisible: false,
      lastValueVisible: false,
    });
    chart.priceScale("vol").applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });

    for (const overlay of OVERLAYS) {
      overlayRefs.current[overlay.key] = chart.addLineSeries({
        color: overlay.color,
        lineWidth: overlay.width,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      });
    }

    // Clicking the chart places the stop. Converted through the candle series so
    // the price matches the axis the user is actually looking at.
    const unsubscribe = chart.subscribeClick((param) => {
      const handler = pickRef.current;
      const series = candlesRef.current;
      if (!handler || !series || !param.point) return;
      const price = series.coordinateToPrice(param.point.y);
      if (price != null && Number.isFinite(price)) handler(Number(price));
    });

    return () => {
      // subscribeClick returns void in v4; unsubscribing is by handler identity.
      void unsubscribe;
      chartRef.current = null;
      candlesRef.current = null;
      volumeRef.current = null;
      overlayRefs.current = {};
      entryLineRef.current = null;
      stopLineRef.current = null;
      chart.remove();
    };
  }, [height]);

  // --- Data ------------------------------------------------------------------
  useEffect(() => {
    const candles = candlesRef.current;
    const volume = volumeRef.current;
    if (!candles || !volume || visible.clean.length < 2) return;

    candles.setData(
      visible.clean.map((b) => ({
        time: b.time as UTCTimestamp,
        open: b.open,
        high: b.high,
        low: b.low,
        close: b.close,
      })),
    );

    volume.setData(
      visible.clean.map((b, i) => ({
        time: b.time as UTCTimestamp,
        value: Number(b.volume ?? 0),
        // Revealed bars are tinted so the eye can tell the future from the past
        // at a glance once stepping starts.
        color:
          i > visible.triggerIndex
            ? "rgba(59,130,246,0.55)"
            : b.close >= b.open
              ? "rgba(34,197,94,0.35)"
              : "rgba(239,68,68,0.35)",
      })),
    );

    for (const overlay of OVERLAYS) {
      const series = overlayRefs.current[overlay.key];
      if (!series) continue;
      series.setData(
        visible.clean
          .map((b, i) => ({ time: b.time as UTCTimestamp, value: visible.emas[overlay.key][i] }))
          .filter((p): p is { time: UTCTimestamp; value: number } => p.value != null && Number.isFinite(p.value)),
      );
    }

    const trigger = visible.clean[visible.triggerIndex];
    if (trigger) {
      candles.setMarkers([
        {
          time: trigger.time as UTCTimestamp,
          position: "belowBar",
          color: "#3b82f6",
          shape: "arrowUp",
          text: "signal",
        },
      ]);
    }
  }, [visible]);

  // --- Entry / stop price lines ---------------------------------------------
  useEffect(() => {
    const candles = candlesRef.current;
    if (!candles) return;

    if (entryLineRef.current) candles.removePriceLine(entryLineRef.current);
    entryLineRef.current = candles.createPriceLine({
      price: entry,
      color: "#3b82f6",
      lineWidth: 1,
      lineStyle: LineStyle.Dashed,
      axisLabelVisible: true,
      title: "entry",
    });

    if (stopLineRef.current) {
      candles.removePriceLine(stopLineRef.current);
      stopLineRef.current = null;
    }
    if (stop != null && Number.isFinite(stop)) {
      stopLineRef.current = candles.createPriceLine({
        price: stop,
        color: "#ef4444",
        lineWidth: 2,
        lineStyle: LineStyle.Solid,
        axisLabelVisible: true,
        title: "stop",
      });
    }
  }, [entry, stop, visible]);

  // Keep the base in view as forward bars extend the series to the right.
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || visible.clean.length < 2) return;
    chart.timeScale().setVisibleLogicalRange({
      from: Math.max(0, visible.clean.length - 120),
      to: visible.clean.length + 4,
    });
  }, [visible.clean.length]);

  return <div ref={containerRef} className="study-chart" style={{ height }} />;
}
