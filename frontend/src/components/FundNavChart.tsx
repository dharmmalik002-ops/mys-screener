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
 * NAV is a single daily value, not an OHLC bar, so this is a line/area chart
 * rather than the candlesticks used for equities — there is no intraday high
 * or low for a mutual fund to draw.
 */

const COMPARE_COLORS = ["#8b5cf6", "#f59e0b", "#06b6d4", "#ec4899"];

export type FundNavChartMode = "growth" | "nav";

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

export function FundNavChart({
  series,
  benchmark,
  drawdown,
  mode = "growth",
  height = 340,
}: {
  series: MfSeriesLine[];
  benchmark?: MfBenchmarkLine | null;
  drawdown?: number[] | null;
  mode?: FundNavChartMode;
  height?: number;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [legend, setLegend] = useState<{ date: string; entries: LegendEntry[] } | null>(null);

  const lines = useMemo(() => {
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
    if (!node || !lines.length) return;

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
  }, [lines, height, drawdown, series, mode]);

  if (!lines.length) {
    return <div className="fnc-empty">No NAV history available for this range.</div>;
  }

  return (
    <div className="fnc-wrap">
      {legend ? (
        <div className="fnc-legend" aria-live="off">
          <span className="fnc-legend-date">{legend.date}</span>
          {legend.entries.map((entry) => (
            <span className="fnc-legend-item" key={entry.key}>
              <i className={entry.dashed ? "fnc-swatch dashed" : "fnc-swatch"} style={{ background: entry.color }} />
              <span className="fnc-legend-label">{entry.label}</span>
              <em>{mode === "growth" ? fmtNumber(entry.value, 1) : fmtNumber(entry.value, 2)}</em>
              <em className={(entry.changePct ?? 0) < 0 ? "neg" : "pos"}>{fmtSignedPct(entry.changePct)}</em>
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
    </div>
  );
}
