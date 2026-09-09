/**
 * Histogram maths for the screener's distribution strip.
 *
 * The point of the strip is context: a scan returning 40 names tells you
 * nothing about whether those 40 are unusual. Binning the whole universe and
 * overlaying the hits answers "where do these sit" — which turns a list into a
 * judgement.
 *
 * Both series are binned on the SAME edges, or the two shapes are not
 * comparable. Values are used as measured; rows missing a metric are excluded
 * from that metric only, and the excluded count is reported rather than hidden.
 */

export type Bin = { from: number; to: number; universe: number; hits: number };

export type Distribution = {
  key: string;
  label: string;
  unit: string;
  bins: Bin[];
  universeMedian: number | null;
  hitsMedian: number | null;
  universeCount: number;
  hitsCount: number;
  /** Rows dropped because the metric was absent, not zero. */
  missing: number;
};

function median(values: number[]): number | null {
  if (!values.length) return null;
  const s = [...values].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

function usable(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

export type MetricSpec<T> = {
  key: string;
  label: string;
  unit: string;
  pick: (row: T) => number | null | undefined;
  /** Fixed domain when the metric has a natural one (RS rating is 0-100). */
  domain?: [number, number];
  /** Clamp a long tail so the shape stays readable (relative volume, returns). */
  clampTo?: [number, number];
};

export function buildDistribution<T>(
  spec: MetricSpec<T>,
  universeRows: T[],
  hitRows: T[],
  binCount = 18,
): Distribution | null {
  const raw = universeRows.map((r) => usable(spec.pick(r)));
  const universe = raw.filter((v): v is number => v !== null);
  const missing = raw.length - universe.length;
  if (universe.length < 20) return null; // too little to describe a distribution

  const hits = hitRows.map((r) => usable(spec.pick(r))).filter((v): v is number => v !== null);

  const clamp = (v: number) =>
    spec.clampTo ? Math.min(Math.max(v, spec.clampTo[0]), spec.clampTo[1]) : v;

  let lo: number;
  let hi: number;
  if (spec.domain) {
    [lo, hi] = spec.domain;
  } else {
    const all = universe.map(clamp);
    lo = Math.min(...all);
    hi = Math.max(...all);
  }
  if (!(hi > lo)) return null;

  const width = (hi - lo) / binCount;
  const bins: Bin[] = Array.from({ length: binCount }, (_, i) => ({
    from: lo + i * width,
    to: lo + (i + 1) * width,
    universe: 0,
    hits: 0,
  }));
  const index = (v: number) =>
    Math.min(binCount - 1, Math.max(0, Math.floor((clamp(v) - lo) / width)));

  for (const v of universe) bins[index(v)].universe += 1;
  for (const v of hits) bins[index(v)].hits += 1;

  return {
    key: spec.key,
    label: spec.label,
    unit: spec.unit,
    bins,
    universeMedian: median(universe),
    hitsMedian: median(hits),
    universeCount: universe.length,
    hitsCount: hits.length,
    missing,
  };
}

/** Percentile of `value` within `rows`, 0-100, or null when undecidable. */
export function percentileOf(value: number | null, values: number[]): number | null {
  if (value === null || !values.length) return null;
  const below = values.filter((v) => v < value).length;
  return Math.round((below / values.length) * 100);
}
