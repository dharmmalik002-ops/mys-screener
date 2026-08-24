import { useEffect, useRef } from "react";
import { ColorType, LineStyle, createChart } from "lightweight-charts";
import type { MfSectorSeries } from "../lib/api";

/**
 * A sector index, as a line or as candles.
 *
 * Unlike a fund NAV, an index really is priced continuously, so these are true
 * daily open/high/low/close bars from the feed rather than an aggregation.
 * Weekly candles are offered alongside because stage analysis is a weekly
 * framework — the 30-week average that decides the stage is drawn on both, so
 * what the classification saw is what the chart shows.
 */

export type SectorChartMode = "line" | "candles";
export type SectorCandlePeriod = "daily" | "weekly";

export function SectorChart({
  series,
  mode,
  period,
  height = 300,
}: {
  series: MfSectorSeries;
  mode: SectorChartMode;
  period: SectorCandlePeriod;
  height?: number;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const node = containerRef.current;
    if (!node || !series.dates.length) return;

    const styles = getComputedStyle(document.documentElement);
    const read = (token: string, fallback: string) =>
      styles.getPropertyValue(token).trim() || fallback;
    const textColor = read("--text-muted", "#64748b");
    const gridColor = read("--line", "rgba(100,140,200,0.15)");
    const up = read("--pfd-up", "#22c55e");
    const down = read("--pfd-down", "#ef4444");

    const chart = createChart(node, {
      height,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor,
        fontSize: 11,
        fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace",
      },
      grid: { vertLines: { color: gridColor }, horzLines: { color: gridColor } },
      rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.1, bottom: 0.08 } },
      timeScale: { borderVisible: false, rightOffset: 2, fixLeftEdge: true, fixRightEdge: true },
      crosshair: { horzLine: { labelVisible: true }, vertLine: { labelVisible: true } },
      autoSize: true,
    });

    if (mode === "candles") {
      const candles = chart.addCandlestickSeries({
        upColor: up, downColor: down,
        borderUpColor: up, borderDownColor: down,
        wickUpColor: up, wickDownColor: down,
        priceFormat: { type: "price", precision: 2, minMove: 0.01 },
      });
      if (period === "weekly") {
        const w = series.weekly;
        candles.setData(
          w.dates.map((time, i) => ({
            time, open: w.opens[i], high: w.highs[i], low: w.lows[i], close: w.closes[i],
          })),
        );
      } else if (series.opens && series.highs && series.lows) {
        candles.setData(
          series.dates.map((time, i) => ({
            time,
            open: series.opens![i],
            high: series.highs![i],
            low: series.lows![i],
            close: series.closes[i],
          })),
        );
      } else {
        // No OHLC in the payload: a flat bar at the close is honest, a fake
        // range is not.
        candles.setData(
          series.dates.map((time, i) => ({
            time, open: series.closes[i], high: series.closes[i],
            low: series.closes[i], close: series.closes[i],
          })),
        );
      }
    } else {
      const line = chart.addAreaSeries({
        lineColor: read("--pfd-line", "#38bdf8"),
        topColor: read("--pfd-line-soft", "rgba(56,189,248,0.25)"),
        bottomColor: read("--pfd-line-faint", "rgba(56,189,248,0.01)"),
        lineWidth: 2,
        priceLineVisible: false,
        priceFormat: { type: "price", precision: 2, minMove: 0.01 },
      });
      line.setData(series.dates.map((time, i) => ({ time, value: series.closes[i] })));
    }

    // The 30-week average, on both modes — it is the line the stage was
    // decided on, so leaving it off would show a different chart from the one
    // the classification read.
    const maPoints = series.dates
      .map((time, i) => ({ time, value: series.ma30w[i] }))
      .filter((point): point is { time: string; value: number } => typeof point.value === "number");
    if (maPoints.length > 5) {
      const ma = chart.addLineSeries({
        color: read("--pfd-cost-line", "rgba(148,163,184,0.75)"),
        lineWidth: 2,
        lineStyle: LineStyle.Dashed,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      });
      ma.setData(maPoints);
    }

    chart.timeScale().fitContent();
    return () => { chart.remove(); };
  }, [series, mode, period, height]);

  if (!series.dates.length) {
    return <div className="fnc-empty">No history for this index.</div>;
  }

  return <div ref={containerRef} style={{ height }} />;
}
