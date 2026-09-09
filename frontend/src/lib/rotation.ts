/**
 * Relative Rotation Graph geometry.
 *
 * A real RRG plots relative strength (the level) against its own momentum (the
 * rate of change of that level), so a group traces a clockwise loop through
 * four quadrants: Improving -> Leading -> Weakening -> Lagging.
 *
 * Both axes here come from the group score already stored per session in
 * /api/groups/rank-history -- a measured, 3-month-anchored relative-strength
 * composite. Nothing is synthesised: the tail is the last N sessions the
 * backend actually recorded, and if a group has too few sessions it gets no
 * tail rather than an invented one.
 *
 * The level axis is centred on the CROSS-SECTIONAL MEDIAN score of the groups
 * being plotted, which is the RRG convention (the benchmark sits at the
 * origin): x > 0 means "stronger than the median group today", not "stronger
 * than some absolute number".
 */

export type RotationPoint = { date: string; x: number; y: number };

export type RotationTrail = {
  groupId: string;
  label: string;
  parentSector: string;
  stockCount: number;
  /** Oldest first; the last entry is today. */
  points: RotationPoint[];
  quadrant: "leading" | "weakening" | "lagging" | "improving";
};

/** Sessions between the score and the earlier score it is compared against. */
export const MOMENTUM_LAG = 5;
/**
 * Sessions averaged before either axis is derived.
 *
 * The daily group score is genuinely noisy -- it can move several points in a
 * session -- and differencing a noisy series amplifies that noise, so the
 * unsmoothed chart was crossing spaghetti with no readable rotation. Smoothing
 * the level first is what a JdK RS-Ratio / RS-Momentum pair does too. It is a
 * stated average of measured values, not a reshaping of them.
 */
export const SMOOTH_WINDOW = 5;

function smooth(values: Array<{ date: string; score: number }>, window: number) {
  if (window <= 1) return values;
  const out: Array<{ date: string; score: number }> = [];
  for (let i = 0; i < values.length; i += 1) {
    const from = Math.max(0, i - window + 1);
    const slice = values.slice(from, i + 1);
    const mean = slice.reduce((sum, p) => sum + p.score, 0) / slice.length;
    out.push({ date: values[i].date, score: mean });
  }
  return out;
}

export function quadrantOf(x: number, y: number): RotationTrail["quadrant"] {
  if (x >= 0) return y >= 0 ? "leading" : "weakening";
  return y >= 0 ? "improving" : "lagging";
}

export const QUADRANT_LABEL: Record<RotationTrail["quadrant"], string> = {
  leading: "Leading",
  weakening: "Weakening",
  lagging: "Lagging",
  improving: "Improving",
};

function median(values: number[]): number {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

type SeriesInput = {
  groupId: string;
  label: string;
  parentSector: string;
  stockCount: number;
  /** Oldest first. `null` scores are dropped, not interpolated. */
  scores: Array<{ date: string; score: number | null }>;
};

/**
 * Builds one trail per group.
 *
 * `tailLength` is how many sessions of history to draw. A group needs
 * MOMENTUM_LAG + 1 usable scores to place even a single point, because the
 * momentum axis is a difference over MOMENTUM_LAG sessions.
 */
export function buildRotation(
  series: SeriesInput[],
  tailLength = 8,
): { trails: RotationTrail[]; centre: number; sessions: number } {
  const clean = series.map((s) => ({
    ...s,
    scores: smooth(
      s.scores.filter(
        (p): p is { date: string; score: number } =>
          typeof p.score === "number" && Number.isFinite(p.score),
      ),
      SMOOTH_WINDOW,
    ),
  }));

  // The origin is today's median score across the plotted groups, so the
  // chart answers "relative to the pack" rather than "relative to 50".
  const latest = clean
    .map((s) => s.scores.at(-1)?.score)
    .filter((v): v is number => typeof v === "number");
  const centre = median(latest);

  const trails: RotationTrail[] = [];
  for (const s of clean) {
    const n = s.scores.length;
    if (n < MOMENTUM_LAG + 1) continue; // not enough measured history

    const points: RotationPoint[] = [];
    const first = Math.max(MOMENTUM_LAG, n - tailLength);
    for (let i = first; i < n; i += 1) {
      const now = s.scores[i];
      const prev = s.scores[i - MOMENTUM_LAG];
      points.push({ date: now.date, x: now.score - centre, y: now.score - prev.score });
    }
    if (!points.length) continue;
    const head = points[points.length - 1];
    trails.push({
      groupId: s.groupId,
      label: s.label,
      parentSector: s.parentSector,
      stockCount: s.stockCount,
      points,
      quadrant: quadrantOf(head.x, head.y),
    });
  }

  const sessions = Math.max(0, ...clean.map((s) => s.scores.length));
  return { trails, centre, sessions };
}

/** Symmetric axis bound so the origin sits dead centre and quadrants are equal. */
export function axisBound(trails: RotationTrail[], key: "x" | "y"): number {
  let max = 0;
  for (const t of trails) for (const p of t.points) max = Math.max(max, Math.abs(p[key]));
  return max > 0 ? max * 1.12 : 1;
}
