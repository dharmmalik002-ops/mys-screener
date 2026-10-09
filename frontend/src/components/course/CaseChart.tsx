import { useEffect, useMemo, useRef, useState } from "react";
import {
  ColorType,
  CrosshairMode,
  createChart,
  type IChartApi,
  type ISeriesApi,
  type SeriesMarker,
  type UTCTimestamp,
} from "lightweight-charts";
import { getCourseBars, type StudyBar } from "../../lib/api";
import { CHART_PAPER, CHART_PAPER_TEXT, premiumCrosshair } from "../../lib/chartDefaults";
import { caseTicker, type CaseStudy } from "./courseData";

/* A case study on a real daily chart, with his own buys, adds and sells
   marked where he logged them.

   `revealThrough` hides every bar after that date, so the trade replay can
   step through his log without the chart giving the ending away. The view is
   framed once per trade and only scrolled to the newest bar after that — never
   re-fitted, so a zoom the reader chose survives each step (the same rule as
   Chart Gym, CLAUDE.md gotcha 21).

   Yahoo's history is restated for later splits and bonuses, so a 2021 price
   can sit at a fraction of what he quoted. When his entry price and the
   chart's close that day disagree by more than ~15%, the bars are rescaled
   onto his numbers and the chart says so. */

const UP = "#1f9e70";
const DOWN = "#dc4b48";
const BUY = "#1f6fd1";
const SELL = "#c2410c";
const NOTE = "#8b877d";
const SMA = "#5a78c8";

const DAY = 86_400;
const toTime = (iso: string) => Math.floor(Date.parse(`${iso}T00:00:00Z`) / 1000);
const shiftDays = (iso: string, days: number) => new Date(Date.parse(`${iso}T00:00:00Z`) + days * DAY * 1000).toISOString().slice(0, 10);

type Kind = "buy" | "sell" | "short" | "cover" | "note";

export function eventKind(action: string, isShort: boolean): Kind {
  const a = action.toLowerCase();
  if (/^cover/.test(a)) return "cover";
  if (/short/.test(a)) return "short";
  if (/^(sold|closed|exit|booked)/.test(a)) return isShort ? "cover" : "sell";
  if (/^(bought|added|re-entered|buy|entry|long)/.test(a)) return "buy";
  return "note";
}

const barCache = new Map<string, Promise<StudyBar[]>>();

function loadBars(symbol: string, start: string, end: string): Promise<StudyBar[]> {
  const key = `${symbol}|${start}|${end}`;
  let hit = barCache.get(key);
  if (!hit) {
    hit = getCourseBars(symbol, start, end).then((r) => r?.bars ?? []);
    hit.catch(() => barCache.delete(key));
    barCache.set(key, hit);
  }
  return hit;
}

export function caseWindow(c: CaseStudy) {
  const dates = [c.entry_date, ...c.timeline.map((t) => t.date)].filter(Boolean).sort();
  return { start: shiftDays(dates[0], -160), end: shiftDays(dates[dates.length - 1], 60), first: dates[0], last: dates[dates.length - 1] };
}

/** Index of the last bar on or before `t` (or the first bar after it, if none). */
function barAtOrBefore(bars: StudyBar[], t: number) {
  let found = -1;
  for (let i = 0; i < bars.length; i += 1) {
    if (bars[i].time <= t) found = i;
    else break;
  }
  return found >= 0 ? found : bars.length ? 0 : -1;
}

function sma(bars: StudyBar[], span: number) {
  const out: { time: UTCTimestamp; value: number }[] = [];
  let sum = 0;
  bars.forEach((b, i) => {
    sum += b.close;
    if (i >= span) sum -= bars[i - span].close;
    if (i >= span - 1) out.push({ time: b.time as UTCTimestamp, value: sum / span });
  });
  return out;
}

export function CaseChart({
  c,
  revealThrough,
  revealEvents,
  height = 320,
}: {
  c: CaseStudy;
  /** Show bars through this date only; omit to show the whole window. */
  revealThrough?: string;
  /** Mark only the first N log entries; omit to mark all. */
  revealEvents?: number;
  height?: number;
}) {
  const node = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const series = useRef<{ price: ISeriesApi<"Candlestick">; volume: ISeriesApi<"Histogram">; avg: ISeriesApi<"Line"> } | null>(null);
  const framed = useRef(false);
  const [raw, setRaw] = useState<StudyBar[] | null>(null);
  const [failed, setFailed] = useState(false);
  const ticker = caseTicker(c.symbol);
  const isShort = /short/i.test(c.symbol) || /short/i.test(c.timeline[0]?.action ?? "");
  const win = useMemo(() => caseWindow(c), [c]);

  useEffect(() => {
    let live = true;
    setRaw(null);
    setFailed(false);
    framed.current = false;
    if (!ticker) {
      setFailed(true);
      return;
    }
    loadBars(ticker, win.start, win.end)
      .then((bars) => {
        if (!live) return;
        if (bars.length < 10) setFailed(true);
        else setRaw(bars);
      })
      .catch(() => live && setFailed(true));
    return () => {
      live = false;
    };
  }, [ticker, win.start, win.end]);

  // Put the bars on his price scale when a later split/bonus restated them.
  const scaled = useMemo(() => {
    if (!raw) return null;
    const anchor = c.timeline.find((t) => typeof t.price === "number" && eventKind(t.action, isShort) !== "note");
    let factor = 1;
    if (anchor && typeof anchor.price === "number") {
      const i = barAtOrBefore(raw, toTime(anchor.date));
      const close = i >= 0 ? raw[i].close : 0;
      const ratio = close > 0 ? anchor.price / close : 1;
      if (ratio > 1.15 || ratio < 0.87) factor = ratio;
    }
    const bars =
      factor === 1
        ? raw
        : raw.map((b) => ({ ...b, open: b.open * factor, high: b.high * factor, low: b.low * factor, close: b.close * factor }));
    return { bars, factor };
  }, [raw, c, isShort]);

  useEffect(() => {
    const el = node.current;
    if (!el || !scaled) return;
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
      timeScale: { borderVisible: false, rightOffset: 4 },
      crosshair: { mode: CrosshairMode.Normal, ...premiumCrosshair() },
      handleScroll: { mouseWheel: false, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: false },
    });
    const price = chart.addCandlestickSeries({
      upColor: UP,
      downColor: DOWN,
      wickUpColor: UP,
      wickDownColor: DOWN,
      borderVisible: false,
      priceFormat: { type: "price", precision: 2, minMove: 0.05 },
    });
    const volume = chart.addHistogramSeries({ priceFormat: { type: "volume" }, priceScaleId: "vol", priceLineVisible: false, lastValueVisible: false });
    chart.priceScale("vol").applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
    const avg = chart.addLineSeries({ color: SMA, lineWidth: 1, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false });
    chartRef.current = chart;
    series.current = { price, volume, avg };
    framed.current = false;
    return () => {
      chart.remove();
      chartRef.current = null;
      series.current = null;
    };
  }, [scaled, height]);

  useEffect(() => {
    const s = series.current;
    const chart = chartRef.current;
    if (!s || !chart || !scaled) return;
    const limit = revealThrough ? toTime(revealThrough) : Infinity;
    const shown = scaled.bars.filter((b) => b.time <= limit);
    const t = (b: StudyBar) => b.time as UTCTimestamp;
    s.price.setData(shown.map((b) => ({ time: t(b), open: b.open, high: b.high, low: b.low, close: b.close })));
    s.volume.setData(
      shown.map((b) => ({ time: t(b), value: b.volume ?? 0, color: b.close >= b.open ? "rgba(31,158,112,0.35)" : "rgba(220,75,72,0.35)" })),
    );
    s.avg.setData(sma(shown, 50));

    const events = c.timeline.slice(0, revealEvents ?? c.timeline.length);
    const markers: SeriesMarker<UTCTimestamp>[] = [];
    for (const e of events) {
      const i = barAtOrBefore(shown, toTime(e.date));
      if (i < 0) continue;
      const kind = eventKind(e.action, isShort);
      const priced = typeof e.price === "number" ? ` ${e.price}` : "";
      const time = t(shown[i]);
      if (kind === "buy" || kind === "cover") {
        markers.push({ time, position: "belowBar", shape: "arrowUp", color: kind === "buy" ? BUY : SELL, text: `${kind === "buy" ? "Buy" : "Cover"}${priced}` });
      } else if (kind === "sell" || kind === "short") {
        markers.push({ time, position: "aboveBar", shape: "arrowDown", color: kind === "sell" ? SELL : BUY, text: `${kind === "sell" ? "Sell" : "Short"}${priced}` });
      } else if (priced) {
        markers.push({ time, position: "aboveBar", shape: "circle", color: NOTE, text: priced.trim() });
      }
    }
    markers.sort((a, b) => Number(a.time) - Number(b.time));
    s.price.setMarkers(markers);

    if (!framed.current && shown.length) {
      const entry = barAtOrBefore(shown, toTime(win.first));
      const from = Math.max(0, entry - 90);
      chart.timeScale().setVisibleLogicalRange({ from, to: Math.max(from + 60, shown.length + 3) });
      framed.current = true;
    } else {
      chart.timeScale().scrollToRealTime();
    }
  }, [scaled, revealThrough, revealEvents, c, isShort, win.first]);

  if (failed) {
    return (
      <p className="course-casechart-missing">
        No price history for {ticker || c.symbol} in this window (delisted, renamed or not on Yahoo). His own chart is above.
      </p>
    );
  }
  return (
    <div className="course-casechart">
      <div ref={node} style={{ height }} aria-label={`${ticker} daily chart with his trades marked`} role="img" />
      {!scaled ? <p className="course-casechart-loading">Loading {ticker} prices…</p> : null}
      <p className="course-casechart-legend">
        <span className="is-buy">▲ buy / add</span> <span className="is-sell">▼ sell</span> <span className="is-note">● update</span> · blue line: 50-day average
        {scaled && scaled.factor !== 1 ? ` · prices restated ×${scaled.factor.toFixed(2)} to the levels he quoted (later split/bonus)` : ""}
      </p>
    </div>
  );
}
