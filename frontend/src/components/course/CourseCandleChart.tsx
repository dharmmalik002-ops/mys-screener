import { useEffect, useMemo, useRef } from "react";
import {
  ColorType,
  CrosshairMode,
  createChart,
  type IChartApi,
  type ISeriesApi,
  type SeriesMarker,
  type UTCTimestamp,
} from "lightweight-charts";
import type { StudyBar } from "../../lib/api";
import { CHART_PAPER, CHART_PAPER_TEXT, premiumCrosshair } from "../../lib/chartDefaults";
import { MA_DEFS, courseChartColors, useCourseChartOptions, type MaKey } from "./CourseChartOptions";

/* A real daily chart in the site's own look: white paper, no grid, the main
   chart's candle and moving-average colours, volume underneath. Style, moving
   averages and the swing chart come from the shared Course toolbar.

   `revealThrough` hides every bar after that time (the quiz and the trade
   replay). Moving averages are computed over ALL bars passed in, including
   the warm-up before `showFrom`, so a 200-day line is real from the first
   drawn bar; only bars up to `revealThrough` are ever drawn or used for
   swing points. The view is framed once per bar set and only scrolled after
   that, so a zoom survives each reveal (CLAUDE.md gotcha 21). */

const SWING_WINDOW = 5;
const SWING_COLOUR = "#5b5f63";

type Point = { time: UTCTimestamp; value: number };
const NO_MARKERS: SeriesMarker<UTCTimestamp>[] = [];
/** A volume bar in the candle colour at ~35% opacity (hex colours only; anything else as is). */
const tint = (hex: string) => (/^#[0-9a-f]{6}$/i.test(hex) ? `${hex}59` : hex);

function movingAverage(bars: StudyBar[], span: number, kind: "ema" | "sma"): (number | null)[] {
  const out: (number | null)[] = [];
  if (kind === "sma") {
    let sum = 0;
    bars.forEach((b, i) => {
      sum += b.close;
      if (i >= span) sum -= bars[i - span].close;
      out.push(i >= span - 1 ? sum / span : null);
    });
    return out;
  }
  const k = 2 / (span + 1);
  let ema = 0;
  bars.forEach((b, i) => {
    ema = i === 0 ? b.close : b.close * k + ema * (1 - k);
    // Seeded values are not an average yet; drawing them fakes a line.
    out.push(i >= span - 1 ? ema : null);
  });
  return out;
}

/** Alternating swing highs and lows (a bar beyond the `w` bars either side). */
export function swingLine(bars: StudyBar[], w = SWING_WINDOW): { index: number; price: number; high: boolean }[] {
  const pivots: { index: number; price: number; high: boolean }[] = [];
  for (let i = w; i < bars.length - w; i += 1) {
    let isHigh = true;
    let isLow = true;
    for (let j = i - w; j <= i + w; j += 1) {
      if (j === i) continue;
      if (bars[j].high > bars[i].high) isHigh = false;
      if (bars[j].low < bars[i].low) isLow = false;
    }
    if (isHigh) pivots.push({ index: i, price: bars[i].high, high: true });
    if (isLow) pivots.push({ index: i, price: bars[i].low, high: false });
  }
  // Two highs in a row keep the higher, two lows the lower: a swing chart alternates.
  const out: typeof pivots = [];
  for (const p of pivots) {
    const last = out[out.length - 1];
    if (last && last.high === p.high) {
      if ((p.high && p.price > last.price) || (!p.high && p.price < last.price)) out[out.length - 1] = p;
    } else {
      out.push(p);
    }
  }
  return out;
}

export function CourseCandleChart({
  bars,
  height,
  showFrom = 0,
  revealThrough,
  markers = NO_MARKERS,
  ariaLabel,
}: {
  bars: StudyBar[];
  height: number;
  /** First bar index drawn; earlier bars only warm up the moving averages. */
  showFrom?: number;
  revealThrough?: number;
  markers?: SeriesMarker<UTCTimestamp>[];
  ariaLabel: string;
}) {
  const opts = useCourseChartOptions();
  const node = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const priceRef = useRef<ISeriesApi<"Candlestick"> | ISeriesApi<"Bar"> | null>(null);
  const volRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  const maRefs = useRef<Partial<Record<MaKey, ISeriesApi<"Line">>>>({});
  const swingRef = useRef<ISeriesApi<"Line"> | null>(null);
  const framedFor = useRef<StudyBar[] | null>(null);
  const colors = useMemo(courseChartColors, []);

  const averages = useMemo(() => {
    const out: Partial<Record<MaKey, (number | null)[]>> = {};
    for (const m of MA_DEFS) out[m.key] = movingAverage(bars, m.span, m.kind);
    return out;
  }, [bars]);

  // Built per style: lightweight-charts cannot turn candles into bars in place,
  // and a rebuilt series needs the data effect to run again (gotcha 19).
  useEffect(() => {
    const el = node.current;
    if (!el) return;
    const chart = createChart(el, {
      height,
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: CHART_PAPER },
        textColor: CHART_PAPER_TEXT,
        fontSize: 11,
        fontFamily: "-apple-system, BlinkMacSystemFont, 'SF Pro Text', system-ui, 'Segoe UI', Roboto, sans-serif",
      },
      grid: { vertLines: { visible: false }, horzLines: { visible: false } },
      rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.08, bottom: 0.24 } },
      // Room on the right for the last bar's marker text ("Setup", "Sell 230").
      timeScale: { borderVisible: false, rightOffset: 8 },
      crosshair: { mode: CrosshairMode.Normal, ...premiumCrosshair() },
      handleScroll: { mouseWheel: false, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: false },
    });
    const priceFormat = { type: "price" as const, precision: 2, minMove: 0.05 };
    priceRef.current =
      opts.style === "candles"
        ? chart.addCandlestickSeries({ upColor: colors.up, downColor: colors.down, wickUpColor: colors.up, wickDownColor: colors.down, borderVisible: false, priceFormat })
        : chart.addBarSeries({ upColor: colors.up, downColor: colors.down, thinBars: opts.style === "hlc", openVisible: opts.style !== "hlc", priceFormat });
    volRef.current = chart.addHistogramSeries({ priceFormat: { type: "volume" }, priceScaleId: "vol", priceLineVisible: false, lastValueVisible: false });
    chart.priceScale("vol").applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
    maRefs.current = {};
    for (const m of MA_DEFS) {
      maRefs.current[m.key] = chart.addLineSeries({
        color: colors.ma[m.key],
        lineWidth: m.key === "ema200" ? 2 : 1,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      });
    }
    swingRef.current = chart.addLineSeries({ color: SWING_COLOUR, lineWidth: 1, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false });
    chartRef.current = chart;
    framedFor.current = null;
    return () => {
      chart.remove();
      chartRef.current = null;
      priceRef.current = null;
    };
  }, [opts.style, height, colors]);

  useEffect(() => {
    const chart = chartRef.current;
    const price = priceRef.current;
    if (!chart || !price || !bars.length) return;
    const limit = revealThrough ?? Infinity;
    let end = bars.length;
    while (end > 0 && bars[end - 1].time > limit) end -= 1;
    const from = Math.min(showFrom, Math.max(0, end - 1));
    const shown = bars.slice(from, end);
    const t = (b: StudyBar) => b.time as UTCTimestamp;

    price.setData(shown.map((b) => ({ time: t(b), open: b.open, high: b.high, low: b.low, close: b.close })));
    volRef.current?.setData(
      shown.map((b) => ({ time: t(b), value: Number(b.volume ?? 0), color: b.close >= b.open ? tint(colors.up) : tint(colors.down) })),
    );
    for (const m of MA_DEFS) {
      const series = maRefs.current[m.key];
      if (!series) continue;
      const values = averages[m.key] ?? [];
      const points: Point[] = [];
      if (opts.mas.includes(m.key)) {
        for (let i = from; i < end; i += 1) {
          const v = values[i];
          if (v != null) points.push({ time: t(bars[i]), value: v });
        }
      }
      series.setData(points);
    }
    const swings = opts.swings ? swingLine(shown) : [];
    swingRef.current?.setData(swings.map((p) => ({ time: t(shown[p.index]), value: p.price })));

    const all: SeriesMarker<UTCTimestamp>[] = markers.filter((m) => Number(m.time) <= limit);
    for (const p of swings) {
      all.push({
        time: t(shown[p.index]),
        position: p.high ? "aboveBar" : "belowBar",
        shape: "circle",
        color: SWING_COLOUR,
        size: 0.4,
        text: p.price >= 1000 ? p.price.toFixed(0) : p.price.toFixed(1),
      });
    }
    all.sort((a, b) => Number(a.time) - Number(b.time));
    price.setMarkers(all);

    if (framedFor.current !== bars) {
      chart.timeScale().fitContent();
      framedFor.current = bars;
    } else {
      chart.timeScale().scrollToRealTime();
    }
  }, [bars, averages, showFrom, revealThrough, markers, opts.mas, opts.swings, opts.style, colors]);

  return <div ref={node} className="course-candle-chart" style={{ height }} role="img" aria-label={ariaLabel} />;
}
