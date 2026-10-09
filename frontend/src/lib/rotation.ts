/**
 * Relative Rotation Graph geometry.
 *
 * Each group is placed on two axes, and the quadrant is read straight off
 * them: Improving -> Leading -> Weakening -> Lagging, clockwise.
 *
 * - Across (x): the group's ranking score minus the median score of EVERY
 *   group on that date. Right of centre therefore always means "top half of
 *   the Rankings table", whichever subset is being drawn.
 * - Up (y): price momentum against the Nifty 500, computed on the backend
 *   (services/group_rotation.py): the % change of the smoothed
 *   group/benchmark line over 10 sessions (15 on the weekly view). Above zero
 *   means the group has actually been gaining on the market.
 *
 * The up axis used to be the change in the ranking score. That score is a
 * percentile of 1-6 month returns, so it rose when an old day dropped out of
 * its window or another group fell — half the groups it called Improving had
 * been losing to the market for two weeks. Its centre was also the median of
 * only the groups drawn, so with "Top 20" the groups ranked 11-20 of ~94 were
 * filed under Improving or Lagging.
 *
 * Nothing is synthesised: a period with no recorded score, or no measured
 * momentum, is simply not a point.
 */

export type RotationPoint = { date: string; x: number; y: number; rsChange5d: number | null };

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
  /**
   * The quadrant it came from, when the as-of period is the one it crossed in.
   * Null when it has been sitting in the same quadrant for longer than the
   * tail, or when the tail is too short to know.
   */
  fromQuadrant: Quadrant | null;
  /** Compass heading of the last leg, degrees clockwise from north. */
  heading: number | null;
  /** Length of the last leg in axis units — how fast it is travelling. */
  speed: number;
};

export type Timeframe = "daily" | "weekly";

/**
 * Per-timeframe wording. `momentumSessions` is the lookback the backend uses
 * for that timeframe's momentum; nothing is smoothed or differenced here, so
 * the score on the x axis is exactly the one the Rankings table shows.
 */
export const TIMEFRAME_PARAMS: Record<
  Timeframe,
  { momentumSessions: number; unit: string; unitShort: string; maxTail: number }
> = {
  daily: { momentumSessions: 10, unit: "session", unitShort: "d", maxTail: 20 },
  weekly: { momentumSessions: 15, unit: "week", unitShort: "w", maxTail: 12 },
};

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

type Score = {
  date: string;
  score: number;
  momentum: number | null;
  momentumW: number | null;
  rsChange5d: number | null;
};

function median(values: number[]): number {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
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
    const stamped = { ...point, date: weekEnd?.get(key) ?? point.date };
    if (key === currentKey) out[out.length - 1] = stamped;
    else {
      out.push(stamped);
      currentKey = key;
    }
  }
  return out;
}

export type SeriesPointInput = {
  date: string;
  score: number | null;
  momentum?: number | null;
  momentumW?: number | null;
  rsChange5d?: number | null;
};

export type SeriesInput = {
  groupId: string;
  label: string;
  parentSector: string;
  stockCount: number;
  /** Oldest first. A `null` score is dropped, not interpolated. */
  scores: SeriesPointInput[];
};

export type PreparedSeries = Omit<SeriesInput, "scores"> & { scores: Score[] };

const finite = (v: number | null | undefined): number | null =>
  typeof v === "number" && Number.isFinite(v) ? v : null;

/**
 * Cleans and optionally weekly-aggregates every series once, and returns the
 * ordered list of period end-dates present across all of them.
 *
 * Split out from `buildRotation` because the time slider re-derives the chart
 * on every step: the per-group work does not depend on which period you are
 * looking at, so it is computed once and re-sliced.
 */
export function prepareSeries(
  series: SeriesInput[],
  timeframe: Timeframe,
): { prepared: PreparedSeries[]; periods: string[] } {
  const cleaned = series.map((s) => ({
    ...s,
    scores: s.scores
      .filter((p) => finite(p.score) !== null)
      .map((p) => ({
        date: p.date,
        score: p.score as number,
        momentum: finite(p.momentum),
        momentumW: finite(p.momentumW),
        rsChange5d: finite(p.rsChange5d),
      })),
  }));

  // One shared calendar for every group, built from the union of recorded
  // dates, so the slider steps through periods that all groups agree on.
  const allDates = new Set<string>();
  for (const s of cleaned) for (const p of s.scores) allDates.add(p.date);
  const calendar = [...allDates].sort();

  let weekEnd: Map<string, string> | undefined;
  if (timeframe === "weekly") {
    weekEnd = new Map();
    for (const date of calendar) weekEnd.set(isoWeekKey(date), date); // last date wins
  }

  const prepared = cleaned.map((s) => ({
    ...s,
    scores: timeframe === "weekly" ? toWeekly(s.scores, weekEnd) : s.scores,
  }));

  const periods = weekEnd ? [...new Set(weekEnd.values())].sort() : calendar;
  return { prepared, periods };
}

function momentumOf(point: Score, timeframe: Timeframe): number | null {
  return timeframe === "weekly" ? point.momentumW : point.momentum;
}

/** Index of the last point at or before `asOf` (the last point when undefined). */
function headIndex(scores: Score[], asOf: string | undefined): number {
  if (!asOf) return scores.length - 1;
  let idx = -1;
  for (let i = 0; i < scores.length; i += 1) {
    if (scores[i].date <= asOf) idx = i;
    else break;
  }
  return idx;
}

/**
 * The x-axis centre on `asOf`: the median score across the whole universe, so
 * a group's side of the line does not depend on which subset is drawn.
 */
function centreAt(universe: PreparedSeries[], asOf: string | undefined): number {
  const levels: number[] = [];
  for (const s of universe) {
    const idx = headIndex(s.scores, asOf);
    if (idx >= 0) levels.push(s.scores[idx].score);
  }
  return median(levels);
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
 * `centreFrom` is the whole universe the median is taken over; it defaults to
 * the series being drawn, which is right only when every group is drawn.
 * Each tail point's x is centred on that period's own median.
 */
export function buildRotation(
  prepared: PreparedSeries[],
  options: { timeframe: Timeframe; tailLength?: number; asOfDate?: string; centreFrom?: PreparedSeries[] } = {
    timeframe: "daily",
  },
): { trails: RotationTrail[]; centre: number; asOfDate: string | null } {
  const tailLength = options.tailLength ?? 8;
  const asOf = options.asOfDate;
  const universe = options.centreFrom ?? prepared;
  const centres = new Map<string, number>();
  const centreFor = (date: string) => {
    let c = centres.get(date);
    if (c === undefined) {
      c = centreAt(universe, date);
      centres.set(date, c);
    }
    return c;
  };

  const trails: RotationTrail[] = [];
  for (const s of prepared) {
    // Each group is indexed against its own array, not a global calendar, so a
    // group that started recording late keeps a correct (shorter) tail rather
    // than borrowing a neighbour's dates.
    const idx = headIndex(s.scores, asOf);
    if (idx < 0) continue;
    // A head that is not the as-of period would draw the group where it stood
    // days ago as if it were today.
    if (asOf && s.scores[idx].date !== asOf) continue;

    const points: RotationPoint[] = [];
    for (let i = Math.max(0, idx - tailLength + 1); i <= idx; i += 1) {
      const now = s.scores[i];
      const y = momentumOf(now, options.timeframe);
      if (y === null) continue;
      points.push({ date: now.date, x: now.score - centreFor(now.date), y, rsChange5d: now.rsChange5d });
    }
    if (!points.length || points[points.length - 1].date !== s.scores[idx].date) continue;

    const head = points[points.length - 1];
    const quadrant = quadrantOf(head.x, head.y);
    const prev = points.length > 1 ? points[points.length - 2] : null;
    const dx = prev ? head.x - prev.x : 0;
    const dy = prev ? head.y - prev.y : 0;
    const speed = Math.hypot(dx, dy);
    // Compass heading, clockwise from north, so "into Leading" reads as ~45deg.
    const heading = prev && speed > 1e-9 ? (((Math.atan2(dx, dy) * 180) / Math.PI) + 360) % 360 : null;
    const previous = prev ? quadrantOf(prev.x, prev.y) : null;

    trails.push({
      groupId: s.groupId,
      label: s.label,
      parentSector: s.parentSector,
      stockCount: s.stockCount,
      points,
      quadrant,
      periodsInQuadrant: countPeriodsInQuadrant(points, quadrant),
      fromQuadrant: previous && previous !== quadrant ? previous : null,
      heading,
      speed,
    });
  }

  const latest = asOf ?? prepared.reduce<string | null>((acc, s) => {
    const last = s.scores.at(-1)?.date ?? null;
    return last && (!acc || last > acc) ? last : acc;
  }, null);
  return { trails, centre: latest ? centreFor(latest) : 0, asOfDate: latest };
}

/**
 * Rolls group scores up to one series per parent sector, weighting each group
 * by its stock count, and attaches the sector's own price momentum.
 *
 * The backend records rank history per group, not per sector, so the sector
 * level is derived here: a 40-stock group moves its sector more than a 5-stock
 * one. Momentum is NOT averaged from the groups -- it is measured on the
 * sector's own equal-weight index (`sectorMomentum`, keyed by sector name).
 */
export function aggregateBySector(
  series: SeriesInput[],
  sectorMomentum: Record<string, Array<{ date: string; momentum: number; momentum_w: number; rs_change_5d: number }>> = {},
): SeriesInput[] {
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
    const momentum = new Map((sectorMomentum[sector] ?? []).map((p) => [p.date, p]));
    out.push({
      groupId: `__sector__${sector}`,
      label: sector,
      parentSector: sector,
      stockCount: bucket.stocks,
      scores: dates.map((date) => ({
        date,
        score: bucket.scoreSum.get(date)! / bucket.weightSum.get(date)!,
        momentum: momentum.get(date)?.momentum ?? null,
        momentumW: momentum.get(date)?.momentum_w ?? null,
        rsChange5d: momentum.get(date)?.rs_change_5d ?? null,
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
 * The centre comes from `centreFrom` (the whole universe), as in buildRotation.
 */
export function quadrantHistory(
  prepared: PreparedSeries[],
  timeframe: Timeframe,
  periods: string[],
  centreFrom: PreparedSeries[] = prepared,
): Array<{ date: string; counts: Record<Quadrant, number>; total: number }> {
  const cursors = prepared.map(() => -1);
  const universeCursors = centreFrom.map(() => -1);
  const out: Array<{ date: string; counts: Record<Quadrant, number>; total: number }> = [];
  const levels: number[] = [];

  for (const date of periods) {
    levels.length = 0;
    for (let g = 0; g < centreFrom.length; g += 1) {
      const scores = centreFrom[g].scores;
      let i = universeCursors[g];
      while (i + 1 < scores.length && scores[i + 1].date <= date) i += 1;
      universeCursors[g] = i;
      if (i >= 0) levels.push(scores[i].score);
    }
    const centre = median(levels);

    const counts: Record<Quadrant, number> = { leading: 0, weakening: 0, lagging: 0, improving: 0 };
    let total = 0;
    for (let g = 0; g < prepared.length; g += 1) {
      const scores = prepared[g].scores;
      let i = cursors[g];
      while (i + 1 < scores.length && scores[i + 1].date <= date) i += 1;
      cursors[g] = i;
      if (i < 0 || scores[i].date !== date) continue;
      const y = momentumOf(scores[i], timeframe);
      if (y === null) continue;
      counts[quadrantOf(scores[i].score - centre, y)] += 1;
      total += 1;
    }
    out.push({ date, counts, total });
  }
  return out;
}
