/**
 * Relative Rotation Graph geometry.
 *
 * A real RRG plots relative strength (the level) against its own momentum (the
 * rate of change of that level), so a group traces a clockwise loop through
 * four quadrants: Improving -> Leading -> Weakening -> Lagging.
 *
 * Both axes here come from the group score already stored per session in
 * /api/groups/rank-history -- a measured, 3-month-anchored relative-strength
 * composite. Nothing is synthesised: the tail is the periods the backend
 * actually recorded, and if a group has too few of them it gets no tail rather
 * than an invented one.
 *
 * The level axis is centred on the CROSS-SECTIONAL MEDIAN score of the groups
 * being plotted, measured at the as-of period being viewed, which is the RRG
 * convention (the benchmark sits at the origin): x > 0 means "stronger than the
 * median group on that date", not "stronger than some absolute number".
 */

export type RotationPoint = { date: string; x: number; y: number };

export type Quadrant = "leading" | "weakening" | "lagging" | "improving";

export type RotationTrail = {
  groupId: string;
  label: string;
  parentSector: string;
  stockCount: number;
  /** Oldest first; the last entry is the as-of period. */
  points: RotationPoint[];
  quadrant: Quadrant;
  /** Consecutive periods (including the as-of one) spent in `quadrant`. */
  periodsInQuadrant: number;
  /** Compass heading of the last leg, degrees clockwise from north. */
  heading: number | null;
  /** Length of the last leg in axis units — how fast it is travelling. */
  speed: number;
};

export type Timeframe = "daily" | "weekly";

/**
 * Per-timeframe smoothing and momentum lag.
 *
 * The daily group score is genuinely noisy -- it can move several points in a
 * session -- and differencing a noisy series amplifies that noise, so the
 * unsmoothed chart was crossing spaghetti with no readable rotation. Smoothing
 * the level before differencing is what a JdK RS-Ratio / RS-Momentum pair does
 * too. It is a stated average of measured values, not a reshaping of them.
 *
 * Weekly uses a shorter window in its own units: 3 weeks of smoothing is ~15
 * sessions, already heavier than the daily setting, and the recorded history is
 * ten-odd weeks deep so a 5-week lag would leave almost no tail.
 */
export const TIMEFRAME_PARAMS: Record<
  Timeframe,
  { smooth: number; lag: number; unit: string; unitShort: string; maxTail: number }
> = {
  daily: { smooth: 5, lag: 5, unit: "session", unitShort: "d", maxTail: 20 },
  weekly: { smooth: 3, lag: 3, unit: "week", unitShort: "w", maxTail: 12 },
};

/** Back-compat aliases for the daily defaults. */
export const MOMENTUM_LAG = TIMEFRAME_PARAMS.daily.lag;
export const SMOOTH_WINDOW = TIMEFRAME_PARAMS.daily.smooth;

export const QUADRANT_LABEL: Record<Quadrant, string> = {
  leading: "Leading",
  weakening: "Weakening",
  lagging: "Lagging",
  improving: "Improving",
};

export const QUADRANT_ORDER: Quadrant[] = ["leading", "weakening", "lagging", "improving"];

export function quadrantOf(x: number, y: number): Quadrant {
  if (x >= 0) return y >= 0 ? "leading" : "weakening";
  return y >= 0 ? "improving" : "lagging";
}

type Score = { date: string; score: number };

function median(values: number[]): number {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function smooth(values: Score[], window: number): Score[] {
  if (window <= 1) return values;
  const out: Score[] = [];
  for (let i = 0; i < values.length; i += 1) {
    const from = Math.max(0, i - window + 1);
    let sum = 0;
    for (let j = from; j <= i; j += 1) sum += values[j].score;
    out.push({ date: values[i].date, score: sum / (i - from + 1) });
  }
  return out;
}

/**
 * ISO week key for a yyyy-mm-dd date, so Monday-to-Friday sessions collapse to
 * one weekly observation. Weeks are keyed rather than counted, so a market
 * holiday shortens a week instead of shifting every later one.
 */
export function isoWeekKey(date: string): string {
  const d = new Date(`${date}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return date;
  const day = d.getUTCDay() || 7; // Mon=1 .. Sun=7
  d.setUTCDate(d.getUTCDate() + 4 - day); // Thursday of this ISO week
  const year = d.getUTCFullYear();
  const jan1 = Date.UTC(year, 0, 1);
  const week = Math.ceil(((d.getTime() - jan1) / 86400000 + 1) / 7);
  return `${year}-W${String(week).padStart(2, "0")}`;
}

/**
 * Collapses a daily series to one observation per ISO week, keeping the last
 * session of each week -- the weekly close, not an average of the week, so a
 * weekly RRG reads the same way a weekly chart does.
 *
 * `weekEnd` maps an ISO week to the date every group should stamp that week
 * with. Without it each group labels its weeks with its own last recorded
 * session, and a group that missed a Friday ends up on a period date no other
 * group shares -- which showed up as 18 "weeks" in 12 weeks of history and
 * would have made the time slider step through phantom periods.
 */
export function toWeekly(scores: Score[], weekEnd?: Map<string, string>): Score[] {
  const out: Score[] = [];
  let currentKey = "";
  for (const point of scores) {
    const key = isoWeekKey(point.date);
    const stamped = { date: weekEnd?.get(key) ?? point.date, score: point.score };
    if (key === currentKey) out[out.length - 1] = stamped;
    else {
      out.push(stamped);
      currentKey = key;
    }
  }
  return out;
}

export type SeriesInput = {
  groupId: string;
  label: string;
  parentSector: string;
  stockCount: number;
  /** Oldest first. `null` scores are dropped, not interpolated. */
  scores: Array<{ date: string; score: number | null }>;
};

export type PreparedSeries = SeriesInput & { scores: Score[] };

/**
 * Cleans, optionally weekly-aggregates and smooths every series once, and
 * returns the ordered list of period end-dates present across all of them.
 *
 * Split out from `buildRotation` because the time slider re-derives the chart
 * on every step: the expensive part (per-group smoothing) does not depend on
 * which period you are looking at, so it is computed once and re-sliced.
 */
export function prepareSeries(
  series: SeriesInput[],
  timeframe: Timeframe,
): { prepared: PreparedSeries[]; periods: string[] } {
  const { smooth: window } = TIMEFRAME_PARAMS[timeframe];
  const finiteSeries = series.map((s) => ({
    ...s,
    scores: s.scores.filter(
      (p): p is Score => typeof p.score === "number" && Number.isFinite(p.score),
    ),
  }));

  // One shared calendar for every group, built from the union of recorded
  // dates, so the slider steps through periods that all groups agree on.
  const allDates = new Set<string>();
  for (const s of finiteSeries) for (const p of s.scores) allDates.add(p.date);
  const calendar = [...allDates].sort();

  let weekEnd: Map<string, string> | undefined;
  if (timeframe === "weekly") {
    weekEnd = new Map();
    for (const date of calendar) weekEnd.set(isoWeekKey(date), date); // last date wins
  }

  const prepared = finiteSeries.map((s) => ({
    ...s,
    scores: smooth(timeframe === "weekly" ? toWeekly(s.scores, weekEnd) : s.scores, window),
  }));

  const periods = weekEnd ? [...new Set(weekEnd.values())].sort() : calendar;
  return { prepared, periods };
}

function countPeriodsInQuadrant(points: RotationPoint[], quadrant: Quadrant): number {
  let n = 0;
  for (let i = points.length - 1; i >= 0; i -= 1) {
    if (quadrantOf(points[i].x, points[i].y) !== quadrant) break;
    n += 1;
  }
  return n;
}

/**
 * Builds one trail per group as of `asOfDate` (default: the latest period).
 *
 * `tailLength` is how many periods of history to draw. A group needs lag + 1
 * usable observations at or before the as-of date to place even a single
 * point, because the momentum axis is a difference over `lag` periods.
 */
export function buildRotation(
  prepared: PreparedSeries[],
  options: { timeframe: Timeframe; tailLength?: number; asOfDate?: string } = {
    timeframe: "daily",
  },
): { trails: RotationTrail[]; centre: number; asOfDate: string | null } {
  const { lag } = TIMEFRAME_PARAMS[options.timeframe];
  const tailLength = options.tailLength ?? 8;
  const asOf = options.asOfDate;

  // Each group is indexed against its own array, not a global calendar, so a
  // group that started recording late keeps a correct (shorter) tail rather
  // than borrowing a neighbour's dates.
  const heads = prepared.map((s) => {
    let idx = s.scores.length - 1;
    if (asOf) {
      idx = -1;
      for (let i = 0; i < s.scores.length; i += 1) {
        if (s.scores[i].date <= asOf) idx = i;
        else break;
      }
    }
    return { series: s, idx };
  });

  const centre = median(
    heads.filter((h) => h.idx >= 0).map((h) => h.series.scores[h.idx].score),
  );

  const trails: RotationTrail[] = [];
  for (const { series: s, idx } of heads) {
    if (idx < lag) continue; // not enough measured history at this as-of date

    const points: RotationPoint[] = [];
    const first = Math.max(lag, idx - tailLength + 1);
    for (let i = first; i <= idx; i += 1) {
      const now = s.scores[i];
      const prev = s.scores[i - lag];
      points.push({ date: now.date, x: now.score - centre, y: now.score - prev.score });
    }
    if (!points.length) continue;

    const head = points[points.length - 1];
    const quadrant = quadrantOf(head.x, head.y);
    const prev = points.length > 1 ? points[points.length - 2] : null;
    const dx = prev ? head.x - prev.x : 0;
    const dy = prev ? head.y - prev.y : 0;
    const speed = Math.hypot(dx, dy);
    // Compass heading, clockwise from north, so "into Leading" reads as ~45deg.
    const heading = prev && speed > 1e-9 ? (((Math.atan2(dx, dy) * 180) / Math.PI) + 360) % 360 : null;

    trails.push({
      groupId: s.groupId,
      label: s.label,
      parentSector: s.parentSector,
      stockCount: s.stockCount,
      points,
      quadrant,
      periodsInQuadrant: countPeriodsInQuadrant(points, quadrant),
      heading,
      speed,
    });
  }

  return { trails, centre, asOfDate: asOf ?? prepared.reduce<string | null>((acc, s) => {
    const last = s.scores.at(-1)?.date ?? null;
    return last && (!acc || last > acc) ? last : acc;
  }, null) };
}

/**
 * Rolls group series up to one series per parent sector, weighting each group
 * by its stock count.
 *
 * The backend records rank history per group, not per sector, so the sector
 * view is derived here rather than fetched. Weighting by constituents means a
 * 40-stock group moves its sector more than a 5-stock one, which is what
 * "how is this sector behaving" means; an unweighted mean would let the
 * smallest groups shout.
 */
export function aggregateBySector(series: SeriesInput[]): SeriesInput[] {
  const bySector = new Map<
    string,
    { weightSum: Map<string, number>; scoreSum: Map<string, number>; stocks: number; groups: number }
  >();
  for (const s of series) {
    const key = s.parentSector || "Unclassified";
    let bucket = bySector.get(key);
    if (!bucket) {
      bucket = { weightSum: new Map(), scoreSum: new Map(), stocks: 0, groups: 0 };
      bySector.set(key, bucket);
    }
    bucket.stocks += s.stockCount;
    bucket.groups += 1;
    const weight = Math.max(1, s.stockCount);
    for (const p of s.scores) {
      if (typeof p.score !== "number" || !Number.isFinite(p.score)) continue;
      bucket.scoreSum.set(p.date, (bucket.scoreSum.get(p.date) ?? 0) + p.score * weight);
      bucket.weightSum.set(p.date, (bucket.weightSum.get(p.date) ?? 0) + weight);
    }
  }

  const out: SeriesInput[] = [];
  for (const [sector, bucket] of bySector) {
    const dates = [...bucket.weightSum.keys()].sort();
    out.push({
      groupId: `__sector__${sector}`,
      label: sector,
      parentSector: sector,
      stockCount: bucket.stocks,
      scores: dates.map((date) => ({
        date,
        score: bucket.scoreSum.get(date)! / bucket.weightSum.get(date)!,
      })),
    });
  }
  return out.sort((a, b) => a.label.localeCompare(b.label));
}

/** Symmetric axis bound so the origin sits dead centre and quadrants are equal. */
export function axisBound(trails: RotationTrail[], key: "x" | "y"): number {
  let max = 0;
  for (const t of trails) for (const p of t.points) max = Math.max(max, Math.abs(p[key]));
  return max > 0 ? max * 1.12 : 1;
}

/**
 * Quadrant population per period, for the history strip under the chart.
 *
 * Written as a single sweep with a moving cursor per group rather than calling
 * `buildRotation` once per period: the naive version was 47 periods x 91 groups
 * of trail-and-point allocation and cost ~1.3s to switch the chart to "All".
 * The cursor only ever moves forward, so this is one pass over each series.
 */
export function quadrantHistory(
  prepared: PreparedSeries[],
  timeframe: Timeframe,
  periods: string[],
): Array<{ date: string; counts: Record<Quadrant, number>; total: number }> {
  const { lag } = TIMEFRAME_PARAMS[timeframe];
  const cursors = prepared.map(() => -1);
  const out: Array<{ date: string; counts: Record<Quadrant, number>; total: number }> = [];
  const levels: number[] = [];

  for (const date of periods) {
    levels.length = 0;
    for (let g = 0; g < prepared.length; g += 1) {
      const scores = prepared[g].scores;
      let i = cursors[g];
      while (i + 1 < scores.length && scores[i + 1].date <= date) i += 1;
      cursors[g] = i;
      if (i >= 0) levels.push(scores[i].score);
    }
    const centre = median(levels);

    const counts: Record<Quadrant, number> = { leading: 0, weakening: 0, lagging: 0, improving: 0 };
    let total = 0;
    for (let g = 0; g < prepared.length; g += 1) {
      const i = cursors[g];
      if (i < lag) continue;
      const scores = prepared[g].scores;
      counts[quadrantOf(scores[i].score - centre, scores[i].score - scores[i - lag].score)] += 1;
      total += 1;
    }
    out.push({ date, counts, total });
  }
  return out;
}
