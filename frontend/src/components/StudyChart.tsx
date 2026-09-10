import { forwardRef, useCallback, useEffect, useImperativeHandle, useMemo, useRef, useState } from "react";
import {
  ColorType,
  CrosshairMode,
  LineStyle,
  createChart,
  type IChartApi,
  type IPriceLine,
  type ISeriesApi,
  type MouseEventParams,
  type SeriesType,
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

export type StudyChartStyle = "candles" | "bars" | "hlc";
export type StudyTool = "cursor" | "stop" | "trendline" | "measure" | "snip";

/** A two-point drawing anchored in (time, price) so it survives pan and zoom. */
export type StudyDrawing = {
  id: string;
  kind: "trendline" | "measure";
  from: { time: number; price: number };
  to: { time: number; price: number };
};

type Props = {
  contextBars: StudyBar[];
  forwardBars: StudyBar[];
  revealed: number;
  /** Index into the combined series where the position was opened, or null. */
  entryIndex: number | null;
  /** Label for the bar the replay starts from, or null to draw no marker. */
  signalLabel?: string | null;
  entryPrice: number | null;
  stop: number | null;
  style: StudyChartStyle;
  tool: StudyTool;
  drawings: StudyDrawing[];
  onDrawingsChange: (drawings: StudyDrawing[]) => void;
  onPickPrice?: (price: number) => void;
  /** Fired on a click in cursor mode, with the session that was clicked. */
  onPickEntry?: (time: number) => void;
  /** Pixel rect of a finished region drag, for cropping a PNG out of the chart. */
  onSnip?: (rect: { x0: number; y0: number; x1: number; y1: number }) => void;
  /** Reports the visible window so a study can be reopened where it was left. */
  onRangeChange?: (range: { from: number; to: number } | null) => void;
  /** Applied once when a saved study is opened. */
  initialRange?: { from: number; to: number } | null;
  height?: number;
};

export type StudyChartHandle = {
  /** PNG of the chart with drawings composited in. */
  capture: () => Promise<Blob | null>;
};

const fmt = (v: number) => v.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

/**
 * The drill chart: candles (or bars) truncated at the session the user has
 * stepped to, plus the two measuring tools that matter for placing a stop.
 *
 * Drawings live in (time, price) space and are re-projected to pixels on every
 * pan, zoom and data change — anchoring them to screen coordinates would slide
 * them off the bars they were drawn against the moment the chart moved.
 */
export const StudyChart = forwardRef<StudyChartHandle, Props>(function StudyChart({
  contextBars,
  forwardBars,
  revealed,
  entryIndex,
  signalLabel,
  entryPrice,
  stop,
  style,
  tool,
  drawings,
  onDrawingsChange,
  onPickPrice,
  onPickEntry,
  onSnip,
  onRangeChange,
  initialRange,
  height = 460,
}: Props, handleRef) {
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const overlayRef = useRef<HTMLCanvasElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const priceRef = useRef<ISeriesApi<SeriesType> | null>(null);
  const volumeRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  const maRefs = useRef<Record<string, ISeriesApi<"Line">>>({});
  const entryLineRef = useRef<IPriceLine | null>(null);
  const stopLineRef = useRef<IPriceLine | null>(null);
  const [draft, setDraft] = useState<StudyDrawing["from"] | null>(null);
  const [hover, setHover] = useState<StudyDrawing["from"] | null>(null);
  // Pixel rect for the region snip, and the live stop drag.
  const [snip, setSnip] = useState<{ x0: number; y0: number; x1: number; y1: number } | null>(null);
  // Mirrored for the window-level drag handlers, which are bound once.
  const draggingStopRef = useRef(false);
  const snipRef = useRef<{ x0: number; y0: number; x1: number; y1: number } | null>(null);
  const stopRef = useRef<number | null>(stop);
  stopRef.current = stop;
  // The auto-fit must run once per chart, not on every new bar — otherwise
  // stepping the replay yanks the view back and undoes the user's zoom.
  const fittedForRef = useRef<string | null>(null);

  // Handlers change every render; the chart subscribes once. Refs keep the
  // live versions reachable without tearing the chart down on each keystroke.
  const cb = useRef({ tool, drawings, onDrawingsChange, onPickPrice, onPickEntry, onSnip, draft });
  cb.current = { tool, drawings, onDrawingsChange, onPickPrice, onPickEntry, onSnip, draft };

  const visible = useMemo(() => {
    const context = contextBars.filter((b) => Number.isFinite(b.close) && b.close > 0);
    const lastContextTime = context.length ? context[context.length - 1].time : -Infinity;
    // Moving to the next card swaps `contextBars` a render before the panel's
    // reset clears `forwardBars`, so for one frame the two belong to different
    // symbols. Appending blindly hands lightweight-charts a series that jumps
    // backwards in time, which it turns into a hard assertion and a blank page.
    const forward = forwardBars
      .filter((b) => Number.isFinite(b.close) && b.close > 0 && b.time > lastContextTime)
      .slice(0, Math.max(0, revealed));
    const clean = [...context, ...forward];
    const closes = clean.map((b) => b.close);
    return {
      clean,
      triggerIndex: context.length - 1,
      emas: Object.fromEntries(OVERLAYS.map((o) => [o.key, emaSeries(closes, o.span)])) as Record<
        string,
        (number | null)[]
      >,
    };
  }, [contextBars, forwardBars, revealed]);

  // --- Chart creation. Re-runs on style change: lightweight-charts has no
  //     "convert this series to bars", so the series is rebuilt instead. ------
  useEffect(() => {
    const node = containerRef.current;
    if (!node) return;

    const styles = getComputedStyle(document.documentElement);
    const textColor = styles.getPropertyValue("--text-muted").trim() || "#64748b";

    const chart = createChart(node, {
      height,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor,
        fontSize: 11,
        fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace",
      },
      // No grid: it competes with the trendlines and measurements drawn on top.
      grid: { vertLines: { visible: false }, horzLines: { visible: false } },
      rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.08, bottom: 0.28 } },
      timeScale: { borderVisible: false, rightOffset: 6 },
      crosshair: { mode: CrosshairMode.Normal },
      autoSize: true,
    });
    chartRef.current = chart;

    const up = "#22c55e";
    const down = "#ef4444";
    priceRef.current =
      style === "candles"
        ? chart.addCandlestickSeries({
            upColor: up,
            downColor: down,
            wickUpColor: up,
            wickDownColor: down,
            borderVisible: false,
            priceFormat: { type: "price", precision: 2, minMove: 0.05 },
          })
        : chart.addBarSeries({
            upColor: up,
            downColor: down,
            thinBars: style === "hlc",
            openVisible: style !== "hlc",
            priceFormat: { type: "price", precision: 2, minMove: 0.05 },
          });

    volumeRef.current = chart.addHistogramSeries({
      priceFormat: { type: "volume" },
      priceScaleId: "vol",
      priceLineVisible: false,
      lastValueVisible: false,
    });
    chart.priceScale("vol").applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });

    for (const o of OVERLAYS) {
      maRefs.current[o.key] = chart.addLineSeries({
        color: o.color,
        lineWidth: o.width,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      });
    }

    const anchorFrom = (param: MouseEventParams): StudyDrawing["from"] | null => {
      const series = priceRef.current;
      if (!series || !param.point || param.time == null) return null;
      const price = series.coordinateToPrice(param.point.y);
      if (price == null || !Number.isFinite(price)) return null;
      return { time: Number(param.time), price: Number(price) };
    };

    chart.subscribeClick((param) => {
      const { tool: t, drawings: d, onDrawingsChange, onPickPrice, onPickEntry, draft: pending } = cb.current;
      const anchor = anchorFrom(param);
      if (!anchor) return;
      if (t === "stop") {
        onPickPrice?.(anchor.price);
        return;
      }
      if (t === "cursor") {
        onPickEntry?.(anchor.time);
        return;
      }
      // The snip is a drag, not a two-click drawing — handled on the wrapper.
      if (t !== "trendline" && t !== "measure") return;
      // Two-click tools: first click sets the anchor, second commits.
      if (!pending) {
        setDraft(anchor);
        return;
      }
      onDrawingsChange([
        ...d,
        { id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`, kind: t, from: pending, to: anchor },
      ]);
      setDraft(null);
    });

    chart.subscribeCrosshairMove((param) => setHover(anchorFrom(param)));

    return () => {
      chartRef.current = null;
      priceRef.current = null;
      volumeRef.current = null;
      maRefs.current = {};
      entryLineRef.current = null;
      stopLineRef.current = null;
      chart.remove();
    };
  }, [height, style]);

  // --- Data --------------------------------------------------------------
  useEffect(() => {
    const price = priceRef.current;
    const volume = volumeRef.current;
    if (!price || !volume || visible.clean.length < 2) return;

    price.setData(
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
        color:
          entryIndex != null && i >= entryIndex
            ? "rgba(59,130,246,0.55)"
            : i > visible.triggerIndex
              ? "rgba(148,163,184,0.40)"
              : b.close >= b.open
                ? "rgba(34,197,94,0.35)"
                : "rgba(239,68,68,0.35)",
      })),
    );

    for (const o of OVERLAYS) {
      maRefs.current[o.key]?.setData(
        visible.clean
          .map((b, i) => ({ time: b.time as UTCTimestamp, value: visible.emas[o.key][i] }))
          .filter((p): p is { time: UTCTimestamp; value: number } => p.value != null && Number.isFinite(p.value)),
      );
    }

    const markers = [];
    const trigger = visible.clean[visible.triggerIndex];
    // A free study with no replay anchor has no signal bar — its last candle is
    // just the last candle, and marking it "signal" invented a scan that never
    // happened.
    if (trigger && signalLabel) {
      markers.push({
        time: trigger.time as UTCTimestamp,
        position: "belowBar" as const,
        color: "#64748b",
        shape: "arrowUp" as const,
        text: signalLabel,
      });
    }
    const entryBar = entryIndex != null ? visible.clean[entryIndex] : null;
    if (entryBar) {
      markers.push({
        time: entryBar.time as UTCTimestamp,
        position: "belowBar" as const,
        color: "#3b82f6",
        shape: "arrowUp" as const,
        text: "entry",
      });
    }
    price.setMarkers(markers);
    // `style` is a dependency because switching candles/bars tears the series
    // down and builds a new one — lightweight-charts cannot convert in place.
    // Without it the rebuilt series keeps whatever data it was born with, which
    // is none, and the chart goes blank.
  }, [visible, entryIndex, style, signalLabel]);

  // --- Entry / stop price lines -------------------------------------------
  useEffect(() => {
    const price = priceRef.current;
    if (!price) return;
    if (entryLineRef.current) {
      price.removePriceLine(entryLineRef.current);
      entryLineRef.current = null;
    }
    if (entryPrice != null && Number.isFinite(entryPrice)) {
      entryLineRef.current = price.createPriceLine({
        price: entryPrice,
        color: "#3b82f6",
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: "entry",
      });
    }
    if (stopLineRef.current) {
      price.removePriceLine(stopLineRef.current);
      stopLineRef.current = null;
    }
    if (stop != null && Number.isFinite(stop)) {
      stopLineRef.current = price.createPriceLine({
        price: stop,
        color: "#ef4444",
        lineWidth: 2,
        lineStyle: LineStyle.Solid,
        axisLabelVisible: true,
        title: "stop",
      });
    }
  }, [entryPrice, stop, visible, style]);

  // --- Drawing overlay ----------------------------------------------------
  // A separate canvas over the chart. lightweight-charts has no drawing
  // primitives, and re-projecting on every render keeps the lines welded to the
  // bars rather than to the viewport.
  const paint = useCallback(() => {
    const canvas = overlayRef.current;
    const chart = chartRef.current;
    const series = priceRef.current;
    const node = containerRef.current;
    if (!canvas || !chart || !series || !node) return;

    const dpr = window.devicePixelRatio || 1;
    const width = node.clientWidth;
    const heightPx = node.clientHeight;
    if (canvas.width !== width * dpr || canvas.height !== heightPx * dpr) {
      canvas.width = width * dpr;
      canvas.height = heightPx * dpr;
      canvas.style.width = `${width}px`;
      canvas.style.height = `${heightPx}px`;
    }
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, heightPx);

    const project = (a: StudyDrawing["from"]) => {
      const x = chart.timeScale().timeToCoordinate(a.time as UTCTimestamp);
      const y = series.priceToCoordinate(a.price);
      return x == null || y == null ? null : { x, y };
    };

    const barsBetween = (a: number, b: number) => {
      const times = visible.clean.map((bar) => bar.time);
      const ia = times.findIndex((t) => t >= Math.min(a, b));
      const ib = times.findIndex((t) => t >= Math.max(a, b));
      return ia < 0 || ib < 0 ? 0 : Math.abs(ib - ia);
    };

    const drawOne = (d: StudyDrawing, ghost: boolean) => {
      const p1 = project(d.from);
      const p2 = project(d.to);
      if (!p1 || !p2) return;
      const rising = d.to.price >= d.from.price;
      const color = d.kind === "measure" ? (rising ? "#22c55e" : "#ef4444") : "#ffd36f";
      ctx.save();
      ctx.globalAlpha = ghost ? 0.6 : 1;
      ctx.strokeStyle = color;
      ctx.lineWidth = 1.5;
      if (d.kind === "measure") {
        ctx.fillStyle = rising ? "rgba(34,197,94,0.12)" : "rgba(239,68,68,0.12)";
        ctx.fillRect(p1.x, p1.y, p2.x - p1.x, p2.y - p1.y);
        ctx.setLineDash([4, 3]);
      }
      ctx.beginPath();
      ctx.moveTo(p1.x, p1.y);
      ctx.lineTo(p2.x, p2.y);
      ctx.stroke();
      ctx.setLineDash([]);

      if (d.kind === "measure") {
        const move = ((d.to.price - d.from.price) / d.from.price) * 100;
        const label = `${move >= 0 ? "+" : ""}${move.toFixed(2)}%  ${fmt(Math.abs(d.to.price - d.from.price))}  ${barsBetween(d.from.time, d.to.time)} bars`;
        ctx.font = "11px 'JetBrains Mono', Menlo, monospace";
        const w = ctx.measureText(label).width + 10;
        const bx = Math.min(p1.x, p2.x) + Math.abs(p2.x - p1.x) / 2 - w / 2;
        const by = Math.min(p1.y, p2.y) - 20;
        ctx.fillStyle = "rgba(15,23,42,0.92)";
        ctx.fillRect(bx, by, w, 17);
        ctx.strokeStyle = color;
        ctx.strokeRect(bx, by, w, 17);
        ctx.fillStyle = color;
        ctx.fillText(label, bx + 5, by + 12);
      }
      ctx.restore();
    };

    for (const d of drawings) drawOne(d, false);
    if (draft && hover && (tool === "measure" || tool === "trendline")) {
      drawOne({ id: "draft", kind: tool, from: draft, to: hover }, true);
    }

    if (snip) {
      const x = Math.min(snip.x0, snip.x1);
      const y = Math.min(snip.y0, snip.y1);
      const w = Math.abs(snip.x1 - snip.x0);
      const h = Math.abs(snip.y1 - snip.y0);
      ctx.save();
      ctx.fillStyle = "rgba(59,130,246,0.12)";
      ctx.fillRect(x, y, w, h);
      ctx.strokeStyle = "#3b82f6";
      ctx.setLineDash([5, 4]);
      ctx.lineWidth = 1;
      ctx.strokeRect(x, y, w, h);
      ctx.restore();
    }
  }, [drawings, draft, hover, tool, visible, snip]);

  useEffect(() => {
    paint();
    const chart = chartRef.current;
    if (!chart) return;
    void style; // repaint after a series rebuild swaps the price scale under us
    const handler = () => paint();
    chart.timeScale().subscribeVisibleLogicalRangeChange(handler);
    window.addEventListener("resize", handler);
    return () => {
      chart.timeScale().unsubscribeVisibleLogicalRangeChange(handler);
      window.removeEventListener("resize", handler);
    };
  }, [paint, style]);

  // Grab-and-drag the stop line, and rubber-band a region for the snip tool.
  // Both live on the wrapper in the capture phase so they can swallow the event
  // before lightweight-charts starts panning the chart under the cursor.
  useEffect(() => {
    const wrap = wrapRef.current;
    if (!wrap) return;

    const localY = (event: MouseEvent) => event.clientY - wrap.getBoundingClientRect().top;
    const localX = (event: MouseEvent) => event.clientX - wrap.getBoundingClientRect().left;

    const stopY = () => {
      const series = priceRef.current;
      const value = stopRef.current;
      if (!series || value == null) return null;
      const y = series.priceToCoordinate(value);
      return y == null ? null : Number(y);
    };

    const onDown = (event: MouseEvent) => {
      if (event.button !== 0) return;
      if (cb.current.tool === "snip") {
        const rect = { x0: localX(event), y0: localY(event), x1: localX(event), y1: localY(event) };
        snipRef.current = rect;
        setSnip(rect);
        event.stopPropagation();
        event.preventDefault();
        return;
      }
      const y = stopY();
      if (y != null && Math.abs(localY(event) - y) <= 6) {
        draggingStopRef.current = true;
        event.stopPropagation();
        event.preventDefault();
      }
    };

    const onMove = (event: MouseEvent) => {
      if (snipRef.current) {
        const rect = { ...snipRef.current, x1: localX(event), y1: localY(event) };
        snipRef.current = rect;
        setSnip(rect);
        event.stopPropagation();
        return;
      }
      if (!draggingStopRef.current) return;
      const series = priceRef.current;
      const price = series?.coordinateToPrice(localY(event));
      if (price != null && Number.isFinite(price)) cb.current.onPickPrice?.(Number(price));
      event.stopPropagation();
    };

    const onUp = () => {
      draggingStopRef.current = false;
      const rect = snipRef.current;
      if (rect) {
        snipRef.current = null;
        setSnip(null);
        if (Math.abs(rect.x1 - rect.x0) > 8 && Math.abs(rect.y1 - rect.y0) > 8) cb.current.onSnip?.(rect);
      }
    };

    wrap.addEventListener("mousedown", onDown, true);
    window.addEventListener("mousemove", onMove, true);
    window.addEventListener("mouseup", onUp, true);
    return () => {
      wrap.removeEventListener("mousedown", onDown, true);
      window.removeEventListener("mousemove", onMove, true);
      window.removeEventListener("mouseup", onUp, true);
    };
  }, []);

  // Clear a half-finished drawing when the tool changes under the user.
  useEffect(() => setDraft(null), [tool]);

  // Frame the chart once, then leave the viewport alone. Zoom and pan are the
  // user's; re-fitting on every revealed bar was silently undoing them mid-replay.
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || visible.clean.length < 2) return;
    const key = `${contextBars[0]?.time ?? 0}:${contextBars.length}:${style}`;
    if (fittedForRef.current === key) return;
    fittedForRef.current = key;
    if (initialRange && initialRange.from < initialRange.to) {
      chart.timeScale().setVisibleRange({
        from: initialRange.from as UTCTimestamp,
        to: initialRange.to as UTCTimestamp,
      });
      return;
    }
    chart.timeScale().setVisibleLogicalRange({
      from: Math.max(0, visible.clean.length - 130),
      to: visible.clean.length + 4,
    });
  }, [visible.clean.length, contextBars, style, initialRange]);

  // Report the visible window so a saved study can reopen on the same dates.
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || !onRangeChange) return;
    const emit = () => {
      const range = chart.timeScale().getVisibleRange();
      onRangeChange(range ? { from: Number(range.from), to: Number(range.to) } : null);
    };
    emit();
    chart.timeScale().subscribeVisibleTimeRangeChange(emit);
    return () => chart.timeScale().unsubscribeVisibleTimeRangeChange(emit);
  }, [onRangeChange, visible.clean.length]);

  // --- Capture -------------------------------------------------------------
  useImperativeHandle(handleRef, () => ({
    async capture() {
      const chart = chartRef.current;
      const overlay = overlayRef.current;
      if (!chart) return null;
      // takeScreenshot only knows about the chart's own canvases, so the
      // drawings layer is composited on top by hand.
      const shot = chart.takeScreenshot();
      const out = document.createElement("canvas");
      out.width = shot.width;
      out.height = shot.height;
      const ctx = out.getContext("2d");
      if (!ctx) return null;
      ctx.fillStyle = getComputedStyle(document.documentElement).getPropertyValue("--surface").trim() || "#0f172a";
      ctx.fillRect(0, 0, out.width, out.height);
      ctx.drawImage(shot, 0, 0);
      if (overlay) ctx.drawImage(overlay, 0, 0, out.width, out.height);
      return await new Promise<Blob | null>((resolve) => out.toBlob(resolve, "image/png"));
    },
  }), []);

  return (
    <div ref={wrapRef} className="study-chart-wrap" style={{ height }}>
      <div ref={containerRef} className={`study-chart tool-${tool}`} style={{ height }} />
      <canvas ref={overlayRef} className="study-chart-overlay" />
    </div>
  );
});
