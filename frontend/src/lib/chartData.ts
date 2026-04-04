import type { ChartBar, ChartLineMarker, ChartLinePoint, HistoricalBreadthDataPoint, IndexPeHistoryResponse } from "./api";

export function sanitizeChartBars(bars: ChartBar[]) {
  const deduped = new Map<number, ChartBar>();
  for (const bar of bars) {
    if (
      !Number.isFinite(bar.time)
      || !Number.isFinite(bar.open)
      || !Number.isFinite(bar.high)
      || !Number.isFinite(bar.low)
      || !Number.isFinite(bar.close)
      || !Number.isFinite(bar.volume)
      || bar.close <= 0
    ) {
      continue;
    }
    // Enforce OHLC consistency for rendering
    const high = Math.max(bar.high, bar.open, bar.close);
    const low = Math.min(bar.low, bar.open, bar.close);
    deduped.set(bar.time, { ...bar, high, low });
  }
  return Array.from(deduped.values()).sort((left, right) => left.time - right.time);
}

export function sanitizeLinePoints(points: ChartLinePoint[]) {
  const deduped = new Map<number, ChartLinePoint>();
  for (const point of points) {
    if (!Number.isFinite(point.time) || !Number.isFinite(point.value)) {
      continue;
    }
    deduped.set(point.time, point);
  }
  return Array.from(deduped.values()).sort((left, right) => left.time - right.time);
}

export function sanitizeLineMarkers(markers: ChartLineMarker[]) {
  const deduped = new Map<string, ChartLineMarker>();
  for (const marker of markers) {
    if (!Number.isFinite(marker.time) || !Number.isFinite(marker.value)) {
      continue;
    }
    deduped.set(`${marker.time}:${marker.label}`, marker);
  }
  return Array.from(deduped.values()).sort((left, right) => left.time - right.time);
}

export function sanitizeIndexPePoints(points: IndexPeHistoryResponse["points"]) {
  const deduped = new Map<string, { date: string; pe: number }>();
  for (const point of points) {
    const date = typeof point.date === "string" ? point.date.split("T")[0] : "";
    const pe = Number(point.pe);
    if (!date || !Number.isFinite(pe)) {
      continue;
    }
    deduped.set(date, { date, pe });
  }
  return Array.from(deduped.values()).sort((left, right) => left.date.localeCompare(right.date));
}

export function sanitizeHistoricalBreadth(history?: HistoricalBreadthDataPoint[]) {
  if (!history?.length) {
    return [];
  }

  const deduped = new Map<string, HistoricalBreadthDataPoint>();
  for (const point of history) {
    const date = typeof point.date === "string" ? point.date.split("T")[0] : "";
    if (!date) {
      continue;
    }

    deduped.set(date, {
      date,
      above_ma20_pct: Number.isFinite(Number(point.above_ma20_pct)) ? Number(point.above_ma20_pct) : 0,
      above_ma50_pct: Number.isFinite(Number(point.above_ma50_pct)) ? Number(point.above_ma50_pct) : 0,
      above_sma200_pct: Number.isFinite(Number(point.above_sma200_pct)) ? Number(point.above_sma200_pct) : 0,
      new_high_52w_pct: Number.isFinite(Number(point.new_high_52w_pct)) ? Number(point.new_high_52w_pct) : 0,
      new_low_52w_pct: Number.isFinite(Number(point.new_low_52w_pct)) ? Number(point.new_low_52w_pct) : 0,
    });
  }

  return Array.from(deduped.values()).sort((left, right) => left.date.localeCompare(right.date));
}