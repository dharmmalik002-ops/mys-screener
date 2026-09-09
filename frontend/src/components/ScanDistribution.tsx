import { useMemo, useState } from "react";

import { buildDistribution, percentileOf, type Distribution, type MetricSpec } from "../lib/distribution";
import type { ScanMatch } from "../lib/api";

import "./ScanDistribution.css";

/**
 * Metrics worth the context. Each has a natural domain or a clamp, because an
 * unclamped relative-volume tail (one stock at 40x) squeezes every other bar
 * to nothing and the shape stops being readable.
 */
const METRICS: Array<MetricSpec<ScanMatch>> = [
  // Exact on both sides: the hits and the universe read the same field.
  { key: "rs", label: "RS Rating", unit: "", pick: (r) => r.rs_rating, domain: [0, 100] },
  { key: "chg", label: "Day Change", unit: "%", pick: (r) => r.change_pct, clampTo: [-10, 10] },
  { key: "mcap", label: "Market Cap", unit: "k Cr", pick: (r) => (r.market_cap_crore ? r.market_cap_crore / 1000 : null), clampTo: [0, 60] },
  // Approximate: the universe side comes from the groups payload's 1-month
  // return, the hits from a 20-session return. Near-identical windows, and the
  // strip says so rather than implying they are the same measure.
  { key: "r20", label: "1M Return", unit: "%", pick: (r) => r.stock_return_20d, clampTo: [-30, 60] },
];

function fmt(value: number | null, unit: string): string {
  if (value === null) return "—";
  const rounded = Math.abs(value) >= 100 ? Math.round(value) : Math.round(value * 10) / 10;
  return `${rounded}${unit}`;
}

function Histogram({ dist, universeValues }: { dist: Distribution; universeValues: number[] }) {
  const maxUniverse = Math.max(1, ...dist.bins.map((b) => b.universe));
  const maxHits = Math.max(1, ...dist.bins.map((b) => b.hits));
  const pct = percentileOf(dist.hitsMedian, universeValues);

  return (
    <div className="sdist-card">
      <div className="sdist-head">
        <span className="sdist-label">{dist.label}</span>
        {pct !== null ? (
          <span className={`sdist-pct${pct >= 70 ? " hi" : pct <= 30 ? " lo" : ""}`}>
            {pct}th pct
          </span>
        ) : null}
      </div>

      <div className="sdist-bars" role="img"
        aria-label={`${dist.label}: these ${dist.hitsCount} hits have a median of ${fmt(dist.hitsMedian, dist.unit)} against a universe median of ${fmt(dist.universeMedian, dist.unit)}`}>
        {dist.bins.map((b) => (
          <span
            key={b.from}
            className="sdist-bin"
            title={`${fmt(b.from, dist.unit)} to ${fmt(b.to, dist.unit)} — ${b.hits} of ${b.universe}`}
          >
            {/* universe shape sits behind, hits in front, same edges */}
            <span className="sdist-bar-universe" style={{ height: `${(b.universe / maxUniverse) * 100}%` }} />
            <span className="sdist-bar-hits" style={{ height: `${(b.hits / maxHits) * 100}%` }} />
          </span>
        ))}
      </div>

      <div className="sdist-foot">
        <span className="sdist-median hits">these {fmt(dist.hitsMedian, dist.unit)}</span>
        <span className="sdist-median uni">universe {fmt(dist.universeMedian, dist.unit)}</span>
      </div>
    </div>
  );
}

type Props = { items: ScanMatch[]; universe: ScanMatch[] };

export function ScanDistribution({ items, universe }: Props) {
  const [open, setOpen] = useState(false);

  const dists = useMemo(() => {
    if (!open || !items.length || universe.length < 20) return [];
    return METRICS
      .map((spec) => {
        const dist = buildDistribution(spec, universe, items);
        if (!dist || !dist.hitsCount) return null;
        const values = universe
          .map((r) => spec.pick(r))
          .filter((v): v is number => typeof v === "number" && Number.isFinite(v));
        return { dist, values };
      })
      .filter((x): x is { dist: Distribution; values: number[] } => x !== null);
  }, [open, items, universe]);

  if (!items.length || universe.length < 20) return null;

  return (
    <section className="sdist-root">
      <button
        type="button"
        className="sdist-toggle"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
      >
        {open ? "Hide" : "Show"} distribution · where these {items.length} sit in {universe.length} stocks
      </button>

      {open ? (
        dists.length ? (
          <>
            <div className="sdist-grid">
              {dists.map(({ dist, values }) => (
                <Histogram key={dist.key} dist={dist} universeValues={values} />
              ))}
            </div>
            <p className="sdist-note">
              Pale bars are the whole universe, solid bars these results, binned on the same edges.
              Each panel is scaled to its own tallest bar, so compare shape and position, not height.
              RS, day change and market cap read the same field on both sides; the return panel
              compares a 20-session return against the universe's 1-month return — near-identical
              windows, not the same number.
            </p>
          </>
        ) : (
          <p className="sdist-note">These results carry none of the metrics needed to plot a distribution.</p>
        )
      ) : null}
    </section>
  );
}
