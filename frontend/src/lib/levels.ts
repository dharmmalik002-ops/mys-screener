// Automatic, professional-grade chart levels: strong support/resistance lines,
// higher-timeframe (weekly/monthly) demand/supply zones near price, and clean
// trendlines. Pure functions over daily ChartBar[] — computed client-side and
// shared by the main chart (ChartPanel) and the grid (ChartGridModal).
//
// Guiding rule: draw like a discretionary trader, or draw nothing. Every level
// must clear a quality gate; counts are hard-capped so charts stay light.

import type { ChartBar } from "./api";

export type SRLevel = {
  price: number;
  kind: "support" | "resistance";
  touches: number;
  strength: number;
};

export type Zone = {
  low: number;
  high: number;
  kind: "demand" | "supply";
  timeframe: "W" | "M";
  startTime: number; // seconds (first bar of the base)
  strength: number;
};

export type Trendline = {
  kind: "up" | "down";
  // Two anchor points (time in seconds, price) — draw a line between them and
  // extend to the latest bar using `slope` (price per second).
  t1: number;
  p1: number;
  t2: number;
  p2: number;
  slope: number;
  touches: number;
};

export type AutoLevels = {
  srLevels: SRLevel[];
  zones: Zone[];
  trendlines: Trendline[];
};

const DAY = 86_400;
const WEEK = 7 * DAY;

// ---- tunable defaults (sensible, professional-grade) ----------------------
const MIN_BARS = 60; // need a meaningful history before drawing anything
const NEAR_PCT = 30; // only surface levels within ±30% of the last close
const SR_TOL_PCT = 0.0075; // cluster swing prices within 0.75%
const SR_MIN_TOUCHES = 3; // a strong daily level needs 3+ touches…
const SR_WEEKLY_MIN_TOUCHES = 2; // …or 2 if it's confirmed on the weekly
const MAX_SR_PER_SIDE = 2;
const MAX_ZONES_PER_SIDE = 2;

type Pivot = { index: number; time: number; price: number; weekly: boolean };

export function aggregateBars(daily: ChartBar[], tf: "W" | "M"): ChartBar[] {
  const buckets = new Map<number, ChartBar>();
  for (const bar of daily) {
    if (!Number.isFinite(bar.close) || bar.close <= 0) continue;
    let key: number;
    if (tf === "M") {
      const d = new Date(bar.time * 1000);
      key = d.getUTCFullYear() * 12 + d.getUTCMonth();
    } else {
      key = Math.floor(bar.time / WEEK);
    }
    const existing = buckets.get(key);
    if (!existing) {
      buckets.set(key, { time: bar.time, open: bar.open, high: bar.high, low: bar.low, close: bar.close, volume: bar.volume });
    } else {
      existing.high = Math.max(existing.high, bar.high);
      existing.low = Math.min(existing.low, bar.low);
      existing.close = bar.close;
      existing.volume += bar.volume;
    }
  }
  return [...buckets.values()].sort((a, b) => a.time - b.time);
}

export function swingPivots(bars: ChartBar[], window: number, weekly = false): { highs: Pivot[]; lows: Pivot[] } {
  const highs: Pivot[] = [];
  const lows: Pivot[] = [];
  for (let i = window; i < bars.length - window; i += 1) {
    const h = bars[i].high;
    const l = bars[i].low;
    let isHigh = true;
    let isLow = true;
    for (let j = i - window; j <= i + window; j += 1) {
      if (j === i) continue;
      if (bars[j].high > h) isHigh = false;
      if (bars[j].low < l) isLow = false;
    }
    if (isHigh) highs.push({ index: i, time: bars[i].time, price: h, weekly });
    if (isLow) lows.push({ index: i, time: bars[i].time, price: l, weekly });
  }
  return { highs, lows };
}

function computeStrongSR(weekly: ChartBar[], daily: ChartBar[], lastClose: number): SRLevel[] {
  const d = swingPivots(daily, 3, false);
  const w = swingPivots(weekly, 2, true);
  const all: Pivot[] = [...d.highs, ...d.lows, ...w.highs, ...w.lows].sort((a, b) => a.price - b.price);
  if (!all.length) return [];

  const latestTime = daily[daily.length - 1]?.time ?? 0;
  const earliestTime = daily[0]?.time ?? 0;
  const span = Math.max(latestTime - earliestTime, 1);

  const levels: SRLevel[] = [];
  let cluster: Pivot[] = [];
  const flush = () => {
    if (!cluster.length) return;
    const price = cluster.reduce((s, p) => s + p.price, 0) / cluster.length;
    const touches = cluster.length;
    const hasWeekly = cluster.some((p) => p.weekly);
    const strongEnough = touches >= SR_MIN_TOUCHES || (hasWeekly && touches >= SR_WEEKLY_MIN_TOUCHES);
    if (strongEnough) {
      const lastTouch = Math.max(...cluster.map((p) => p.time));
      const recency = (lastTouch - earliestTime) / span; // 0..1, newer = stronger
      const strength = touches + (hasWeekly ? 2 : 0) + recency;
      levels.push({ price, kind: price < lastClose ? "support" : "resistance", touches, strength });
    }
    cluster = [];
  };
  for (const p of all) {
    if (!cluster.length) {
      cluster.push(p);
      continue;
    }
    const mean = cluster.reduce((s, c) => s + c.price, 0) / cluster.length;
    if (Math.abs(p.price - mean) <= mean * SR_TOL_PCT) cluster.push(p);
    else {
      flush();
      cluster.push(p);
    }
  }
  flush();
  return levels;
}

export function computeZones(bars: ChartBar[], tf: "W" | "M"): Zone[] {
  // Pivot-anchored demand/supply: a swing low that launched a strong up-move is
  // a demand zone (band = the base candle(s) at that low); a swing high that
  // launched a strong down-move is a supply zone. This surfaces the bases a
  // trader actually marks — including recent ones near price — instead of only
  // the untested origin at the very bottom of a trend.
  const n = bars.length;
  const window = tf === "M" ? 1 : 2;
  const impulsePct = tf === "M" ? 15 : 10;
  const impulseBars = tf === "M" ? 4 : 6;
  const tfWeight = tf === "M" ? 1.6 : 1;
  const breakTol = 0.03; // a zone is only "consumed" once a close pierces it by >3%
  if (n < window * 2 + impulseBars + 1) return [];

  const { highs, lows } = swingPivots(bars, window);
  const raw: Zone[] = [];

  const baseAt = (i: number) => {
    const lo = Math.min(bars[i].low, bars[i - 1]?.low ?? bars[i].low);
    const hi = Math.max(bars[i].high, bars[i - 1]?.high ?? bars[i].high);
    const bodyHi = Math.max(bars[i].open, bars[i].close, bars[i - 1]?.open ?? -Infinity, bars[i - 1]?.close ?? -Infinity);
    const bodyLo = Math.min(bars[i].open, bars[i].close, bars[i - 1]?.open ?? Infinity, bars[i - 1]?.close ?? Infinity);
    return { lo, hi, bodyHi, bodyLo };
  };

  for (const piv of lows) {
    const end = Math.min(piv.index + impulseBars, n - 1);
    let maxClose = -Infinity;
    for (let k = piv.index + 1; k <= end; k += 1) maxClose = Math.max(maxClose, bars[k].close);
    const mag = ((maxClose - bars[piv.index].close) / bars[piv.index].close) * 100;
    if (mag < impulsePct) continue;
    const base = baseAt(piv.index);
    const low = base.lo;
    const high = Math.max(base.bodyHi, low * 1.001);
    let broken = false;
    let retests = 0;
    for (let k = end + 1; k < n; k += 1) {
      if (bars[k].close < low * (1 - breakTol)) { broken = true; break; }
      if (bars[k].low <= high && bars[k].low >= low) retests += 1;
    }
    if (broken) continue;
    const recency = piv.index / n;
    raw.push({ low, high, kind: "demand", timeframe: tf, startTime: bars[piv.index].time, strength: mag * tfWeight * (1 + recency) + retests });
  }

  for (const piv of highs) {
    const end = Math.min(piv.index + impulseBars, n - 1);
    let minClose = Infinity;
    for (let k = piv.index + 1; k <= end; k += 1) minClose = Math.min(minClose, bars[k].close);
    const mag = ((bars[piv.index].close - minClose) / bars[piv.index].close) * 100;
    if (mag < impulsePct) continue;
    const base = baseAt(piv.index);
    const high = base.hi;
    const low = Math.min(base.bodyLo, high * 0.999);
    let broken = false;
    let retests = 0;
    for (let k = end + 1; k < n; k += 1) {
      if (bars[k].close > high * (1 + breakTol)) { broken = true; break; }
      if (bars[k].high >= low && bars[k].high <= high) retests += 1;
    }
    if (broken) continue;
    const recency = piv.index / n;
    raw.push({ low, high, kind: "supply", timeframe: tf, startTime: bars[piv.index].time, strength: mag * tfWeight * (1 + recency) + retests });
  }

  // Dedupe overlapping same-kind zones — keep the stronger.
  raw.sort((a, b) => b.strength - a.strength);
  const kept: Zone[] = [];
  for (const z of raw) {
    const overlaps = kept.some((k) => k.kind === z.kind && z.low <= k.high && z.high >= k.low);
    if (!overlaps) kept.push(z);
  }
  return kept;
}

function fitTrendline(
  bars: ChartBar[],
  pivots: Pivot[],
  kind: "up" | "down",
): Trendline | null {
  if (pivots.length < 3 || bars.length < 12) return null;
  const n = bars.length;
  const lastBar = bars[n - 1];
  const tolPct = 0.02; // a touch must sit within 2% of the line
  const breakTol = 0.03; // closes may pierce the line by up to 3% (wicks happen)
  const maxBreaksFrac = 0.06; // tolerate a few violating closes, not zero
  const span = Math.max((bars[n - 1].time - bars[0].time) / DAY, 1);
  const minSpan = span * 0.3; // touches must cover ≥30% of the window

  let best: Trendline | null = null;
  for (let i = 0; i < pivots.length - 1; i += 1) {
    for (let j = i + 1; j < pivots.length; j += 1) {
      const a = pivots[i];
      const b = pivots[j];
      if (b.time <= a.time) continue;
      const slope = (b.price - a.price) / (b.time - a.time);
      if (kind === "up" && slope <= 0) continue;
      if (kind === "down" && slope >= 0) continue;
      const priceAt = (t: number) => a.price + slope * (t - a.time);

      // Touches near the line.
      let touches = 0;
      let firstTouch = Infinity;
      let lastTouch = -Infinity;
      for (const p of pivots) {
        const lp = priceAt(p.time);
        if (lp <= 0) continue;
        if (Math.abs(p.price - lp) <= lp * tolPct) {
          touches += 1;
          firstTouch = Math.min(firstTouch, p.time);
          lastTouch = Math.max(lastTouch, p.time);
        }
      }
      if (touches < 3 || lastTouch - firstTouch < minSpan * DAY) continue;

      // Tolerate a few violating closes (wicks/noise), not a full break.
      let breaks = 0;
      let considered = 0;
      for (let k = 0; k < n; k += 1) {
        if (bars[k].time < firstTouch) continue;
        const lp = priceAt(bars[k].time);
        if (lp <= 0) continue;
        considered += 1;
        if (kind === "up" && bars[k].close < lp * (1 - breakTol)) breaks += 1;
        if (kind === "down" && bars[k].close > lp * (1 + breakTol)) breaks += 1;
      }
      if (considered === 0 || breaks / considered > maxBreaksFrac) continue;

      // Still active: the line should sit near recent price, not miles away.
      const lpNow = priceAt(lastBar.time);
      if (lpNow <= 0 || Math.abs(lastBar.close - lpNow) / lastBar.close > 0.4) continue;

      const candidate: Trendline = {
        kind,
        t1: a.time,
        p1: a.price,
        t2: lastBar.time,
        p2: lpNow,
        slope,
        touches,
      };
      // Prefer more touches, then the more recent anchor.
      if (!best || candidate.touches > best.touches || (candidate.touches === best.touches && a.time > best.t1)) {
        best = candidate;
      }
    }
  }
  return best;
}

export function computeTrendlines(daily: ChartBar[]): Trendline[] {
  // Fit on the WEEKLY series: fewer, more significant pivots → cleaner,
  // professional-looking lines (and far fewer false positives than daily).
  const weekly = aggregateBars(daily, "W");
  const { highs, lows } = swingPivots(weekly, 1, false);
  const out: Trendline[] = [];
  const up = fitTrendline(weekly, lows, "up");
  if (up) out.push(up);
  const down = fitTrendline(weekly, highs, "down");
  if (down) out.push(down);
  return out;
}

/** Main entry: compute strong, near-price S/R + W/M zones + trendlines from daily bars. */
export function computeAutoLevels(daily: ChartBar[]): AutoLevels {
  const empty: AutoLevels = { srLevels: [], zones: [], trendlines: [] };
  if (!Array.isArray(daily) || daily.length < MIN_BARS) return empty;
  // Keep it light: use ~2 years of daily bars at most.
  const bars = daily.slice(-520);
  const lastClose = bars[bars.length - 1]?.close;
  if (!Number.isFinite(lastClose) || lastClose <= 0) return empty;

  const weekly = aggregateBars(bars, "W");
  const monthly = aggregateBars(bars, "M");

  // ---- S/R: strong only, nearest few each side ----
  const near = (price: number) => Math.abs(price - lastClose) / lastClose <= NEAR_PCT / 100;
  const sr = computeStrongSR(weekly, bars, lastClose).filter((l) => near(l.price));
  const supports = sr.filter((l) => l.kind === "support").sort((a, b) => b.price - a.price).slice(0, MAX_SR_PER_SIDE);
  const resistances = sr.filter((l) => l.kind === "resistance").sort((a, b) => a.price - b.price).slice(0, MAX_SR_PER_SIDE);

  // ---- Zones: weekly + monthly, near price, strongest few each side ----
  const allZones = [...computeZones(weekly, "W"), ...computeZones(monthly, "M")].filter(
    (z) => near(z.high) || near(z.low),
  );
  const demand = allZones
    .filter((z) => z.kind === "demand" && z.high <= lastClose * 1.02)
    .sort((a, b) => b.high - a.high || b.strength - a.strength) // nearest below first
    .slice(0, MAX_ZONES_PER_SIDE);
  const supply = allZones
    .filter((z) => z.kind === "supply" && z.low >= lastClose * 0.98)
    .sort((a, b) => a.low - b.low || b.strength - a.strength) // nearest above first
    .slice(0, MAX_ZONES_PER_SIDE);

  return {
    srLevels: [...supports, ...resistances],
    zones: [...demand, ...supply],
    trendlines: computeTrendlines(bars),
  };
}
