import { useEffect, useMemo, useRef, useState } from "react";
import { ColorType, LineStyle, createChart, type IChartApi, type ISeriesApi } from "lightweight-charts";
import type { MfBenchmarkLine, MfSeriesLine } from "../lib/api";

import "./FundNavChart.css";

/**
 * NAV chart for a fund, with its benchmark and any compared funds overlaid.
 *
 * Two display modes, because they answer different questions:
 *
 * - **Growth of ₹100** rebases every line to 100 at the left edge of the
 *   visible range. This is the only way to put a fund whose NAV is ₹90 next to
 *   an index sitting at 24,000 on one axis, and rebasing *inside the window*
 *   (rather than at inception) is what makes the 1-year view answer "how did
 *   this year go" instead of "how did 2013 go".
 * - **NAV** shows the fund's actual unit value, which is what a statement
 *   shows and what a purchase is priced at.
 *
 * A fund's NAV is a single value per day — there is no intraday high or low to
 * draw, so a *daily* candlestick would be a row of doji and tell you nothing.
 * The candlestick mode therefore aggregates: each candle is one week or one
 * month, opening at that period's first NAV, closing at its last, with the
 * high and low taken across the period. That is a real OHLC bar built from
 * real data, and it answers the question candles are actually good for — how
 * wide was the swing inside the period, not just where it ended.
 */

const COMPARE_COLORS = ["#8b5cf6", "#f59e0b", "#06b6d4", "#ec4899"];

export type FundNavChartMode = "growth" | "nav" | "candles";

/** Aggregation period for candlestick mode. Daily is deliberately absent:
 *  one NAV a day cannot make a meaningful open/high/low/close. */
export type CandlePeriod = "weekly" | "monthly";

type LegendEntry = {
  key: string;
  label: string;
  color: string;
  value: number | null;
  changePct: number | null;
  dashed: boolean;
};

const fmtNumber = (value: number | null | undefined, digits = 2) =>
  value == null || !Number.isFinite(value)
    ? "—"
    : value.toLocaleString("en-IN", { minimumFractionDigits: digits, maximumFractionDigits: digits });

const fmtSignedPct = (value: number | null | undefined) =>
  value == null || !Number.isFinite(value) ? "—" : `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;

type Candle = { time: string; open: number; high: number; low: number; close: number };

/**
 * Collapse a daily NAV series into weekly or monthly OHLC candles.
 *
 * The period key is the bucket a date falls in; the candle is stamped with the
 * period's *first trading date* so the time axis stays on real dates the fund
 * was actually priced on. Open is the first NAV in the bucket and close the
 * last, so consecutive candles join up the way a price series should — the
 * close of one week is the NAV the next week opens from, give or take the
 * weekend.
 */
function toCandles(dates: string[], values: number[], period: CandlePeriod): Candle[] {
  const buckets = new Map<string, Candle>();

  for (let index = 0; index < dates.length; index += 1) {
    const value = values[index];
    if (!Number.isFinite(value)) continue;
    const date = dates[index];
    const key = period === "monthly" ? date.slice(0, 7) : isoWeekKey(date);

    const existing = buckets.get(key);
    if (!existing) {
      buckets.set(key, { time: date, open: value, high: value, low: value, close: value });
      continue;
    }
    existing.high = Math.max(existing.high, value);
    existing.low = Math.min(existing.low, value);
    existing.close = value;
  }

  return [...buckets.values()].sort((a, b) => a.time.localeCompare(b.time));
}

/** Year + ISO week number, so a week spanning a month or year boundary stays
 *  one bucket rather than splitting into two stub candles. */
function isoWeekKey(iso: string): string {
  const date = new Date(`${iso}T00:00:00Z`);
  const day = date.getUTCDay() || 7;          // Monday = 1 … Sunday = 7
  date.setUTCDate(date.getUTCDate() + 4 - day); // Thursday decides the ISO year
  const yearStart = Date.UTC(date.getUTCFullYear(), 0, 1);
  const week = Math.ceil(((date.getTime() - yearStart) / 86400000 + 1) / 7);
  return `${date.getUTCFullYear()}-W${String(week).padStart(2, "0")}`;
}

export function FundNavChart({
  series,
  benchmark,
  drawdown,
  mode = "growth",
  candlePeriod = "weekly",
  height = 340,
}: {
  series: MfSeriesLine[];
  benchmark?: MfBenchmarkLine | null;
  drawdown?: number[] | null;
  mode?: FundNavChartMode;
  candlePeriod?: CandlePeriod;
  height?: number;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [legend, setLegend] = useState<{ date: string; entries: LegendEntry[] } | null>(null);

  const candles = useMemo(() => {
    if (mode !== "candles") return [];
    const fund = series.find((line) => line.kind === "fund");
    if (!fund || fund.dates.length < 2) return [];
    return toCandles(fund.dates, fund.values, candlePeriod);
  }, [series, mode, candlePeriod]);

  const lines = useMemo(() => {
    if (mode === "candles") return [];
    const fund = series.find((line) => line.kind === "fund");
    if (!fund || fund.dates.length < 2) return [];

    const out: { key: string; label: string; color: string; dashed: boolean; width: number; points: { time: string; value: number }[] }[] = [];

    if (mode === "nav") {
      // Absolute NAV: only the fund's own series is meaningful on this axis.
      out.push({
        key: fund.key,
        label: fund.label ?? "Fund",
        color: "#22c55e",
        dashed: false,
        width: 2,
        points: fund.dates.map((date, index) => ({ time: date, value: fund.values[index] })),
      });
      return out;
    }

    // Growth mode. The benchmark payload carries its own copy of the fund leg
    // (`fund_rebased`) already inner-joined to the benchmark's calendar — using
    // it keeps the two lines starting at exactly 100 on the same date. Without
    // that, a missing NAV day on either side makes the fund appear to start
    // slightly above or below the benchmark.
    const fundPoints = benchmark
      ? benchmark.dates.map((date, index) => ({ time: date, value: benchmark.fund_rebased[index] }))
      : fund.dates.map((date, index) => ({ time: date, value: fund.rebased[index] }));

    out.push({
      key: fund.key,
      label: fund.label ?? "Fund",
      color: "#22c55e",
      dashed: false,
      width: 2,
      points: fundPoints.filter((point) => Number.isFinite(point.value)),
    });

    if (benchmark && benchmark.rebased.length) {
      out.push({
        key: `bm-${benchmark.key}`,
        label: benchmark.label ?? "Benchmark",
        color: "#94a3b8",
        dashed: true,
        width: 2,
        points: benchmark.dates
          .map((date, index) => ({ time: date, value: benchmark.rebased[index] }))
          .filter((point) => Number.isFinite(point.value)),
      });
    }

    series
      .filter((line) => line.kind === "compare")
      .forEach((line, index) => {
        out.push({
          key: line.key,
          label: line.label ?? line.key,
          color: COMPARE_COLORS[index % COMPARE_COLORS.length],
          dashed: false,
          width: 1,
          points: line.dates
            .map((date, position) => ({ time: date, value: line.rebased[position] }))
            .filter((point) => Number.isFinite(point.value)),
        });
      });

    return out;
  }, [series, benchmark, mode]);

  useEffect(() => {
    const node = containerRef.current;
    if (!node || (!lines.length && !candles.length)) return;

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
      rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.12, bottom: drawdown?.length ? 0.28 : 0.08 } },
      timeScale: { borderVisible: false, rightOffset: 2, fixLeftEdge: true, fixRightEdge: true },
      crosshair: { horzLine: { labelVisible: true }, vertLine: { labelVisible: true } },
      handleScale: { axisPressedMouseMove: { time: true, price: false } },
      autoSize: true,
    });

    if (mode === "candles") {
      const up = styles.getPropertyValue("--pfd-up").trim() || "#22c55e";
      const down = styles.getPropertyValue("--pfd-down").trim() || "#ef4444";
      const candleSeries = chart.addCandlestickSeries({
        upColor: up,
        downColor: down,
        borderUpColor: up,
        borderDownColor: down,
        wickUpColor: up,
        wickDownColor: down,
        priceFormat: { type: "price", precision: 2, minMove: 0.01 },
      });
      candleSeries.setData(candles);

      const byDate = new Map(candles.map((candle) => [candle.time, candle]));
      const lastCandle = candles[candles.length - 1];
      const legendForCandle = (date: string) => {
        const candle = byDate.get(date) ?? lastCandle;
        if (!candle) return null;
        const change = candle.open > 0 ? (candle.close / candle.open - 1) * 100 : null;
        return {
          date: candle.time,
          entries: [
            // Empty colour = inherit the legend's own text colour. Only the
            // close is tinted, because only the close has a direction.
            { key: "o", label: "O", color: "", dashed: false, value: candle.open, changePct: null },
            { key: "h", label: "H", color: "", dashed: false, value: candle.high, changePct: null },
            { key: "l", label: "L", color: "", dashed: false, value: candle.low, changePct: null },
            {
              key: "c",
              label: "C",
              color: (change ?? 0) < 0 ? down : up,
              dashed: false,
              value: candle.close,
              changePct: change,
            },
          ],
        };
      };
      setLegend(legendForCandle(lastCandle?.time ?? ""));
      chart.subscribeCrosshairMove((param) => {
        const date = typeof param.time === "string" ? param.time : null;
        setLegend(legendForCandle(date ?? lastCandle?.time ?? ""));
      });
      chart.timeScale().fitContent();
      return () => { chart.remove(); };
    }

    const drawn: { key: string; label: string; color: string; dashed: boolean; series: ISeriesApi<"Line">; byDate: Map<string, number>; base: number }[] = [];

    lines.forEach((line) => {
      const api = chart.addLineSeries({
        color: line.color,
        lineWidth: line.width as 1 | 2 | 3,
        lineStyle: line.dashed ? LineStyle.Dashed : LineStyle.Solid,
        priceLineVisible: false,
        lastValueVisible: true,
        crosshairMarkerVisible: true,
        crosshairMarkerRadius: 3,
        priceFormat:
          mode === "growth"
            ? { type: "price", precision: 1, minMove: 0.1 }
            : { type: "price", precision: 2, minMove: 0.01 },
      });
      api.setData(line.points);
      drawn.push({
        key: line.key,
        label: line.label,
        color: line.color,
        dashed: line.dashed,
        series: api,
        byDate: new Map(line.points.map((point) => [point.time, point.value])),
        base: line.points[0]?.value ?? 0,
      });
    });

    // Drawdown ribbon on its own scale beneath the price lines — the "how bad
    // did it get along the way" view that a rising equity curve hides.
    if (drawdown?.length) {
      const fund = series.find((line) => line.kind === "fund");
      if (fund) {
        const ribbon = chart.addAreaSeries({
          priceScaleId: "drawdown",
          lineColor: "#ef4444",
          topColor: "rgba(239,68,68,0.02)",
          bottomColor: "rgba(239,68,68,0.28)",
          lineWidth: 1,
          priceLineVisible: false,
          lastValueVisible: false,
          crosshairMarkerVisible: false,
        });
        ribbon.setData(
          fund.dates
            .map((date, index) => ({ time: date, value: drawdown[index] }))
            .filter((point) => Number.isFinite(point.value)),
        );
        chart.priceScale("drawdown").applyOptions({
          scaleMargins: { top: 0.78, bottom: 0 },
          borderVisible: false,
        });
      }
    }

    const lastDate = lines[0].points[lines[0].points.length - 1]?.time ?? "";
    const legendFor = (date: string) => ({
      date,
      entries: drawn.map((entry) => {
        const value = entry.byDate.get(date) ?? null;
        return {
          key: entry.key,
          label: entry.label,
          color: entry.color,
          dashed: entry.dashed,
          value,
          // In growth mode every line starts at 100, so "value - 100" is the
          // window return; in NAV mode it is the move from the window start.
          changePct:
            value != null && entry.base > 0 ? (value / entry.base - 1) * 100 : null,
        };
      }),
    });
    setLegend(legendFor(lastDate));

    chart.subscribeCrosshairMove((param) => {
      const date = typeof param.time === "string" ? param.time : null;
      setLegend(legendFor(date ?? lastDate));
    });

    chart.timeScale().fitContent();

    return () => {
      chart.remove();
    };
  }, [lines, candles, height, drawdown, series, mode]);

  if (!lines.length && !candles.length) {
    return <div className="fnc-empty">No NAV history available for this range.</div>;
  }

  return (
    <div className="fnc-wrap">
      {legend ? (
        <div className="fnc-legend" aria-live="off">
          <span className="fnc-legend-date">{legend.date}</span>
          {legend.entries.map((entry) => (
            <span className="fnc-legend-item" key={entry.key}>
              {mode === "candles" ? null : (
                <i className={entry.dashed ? "fnc-swatch dashed" : "fnc-swatch"} style={{ background: entry.color }} />
              )}
              <span className="fnc-legend-label">{entry.label}</span>
              <em style={mode === "candles" && entry.color ? { color: entry.color } : undefined}>
                {mode === "growth" ? fmtNumber(entry.value, 1) : fmtNumber(entry.value, 2)}
              </em>
              {entry.changePct != null ? (
                <em className={entry.changePct < 0 ? "neg" : "pos"}>{fmtSignedPct(entry.changePct)}</em>
              ) : null}
            </span>
          ))}
        </div>
      ) : null}
      <div ref={containerRef} className="fnc-chart" style={{ height }} />
      {mode === "growth" ? (
        <p className="fnc-foot">
          Every line rebased to 100 at the start of this range — the value is what ₹100 became.
          {drawdown?.length ? " The red ribbon below is the fund's drawdown from its running peak." : ""}
        </p>
      ) : null}
      {mode === "candles" ? (
        <p className="fnc-foot">
          Each candle is one {candlePeriod === "monthly" ? "month" : "week"} of NAV: it opens at the
          period's first NAV and closes at its last, with the wick spanning the highest and lowest
          NAV in between. A fund is priced once a day, so there is no intraday high or low — the
          range you see is the swing across the {candlePeriod === "monthly" ? "month" : "week"},
          which is the thing a daily candle could not show.
        </p>
      ) : null}
    </div>
  );
}
