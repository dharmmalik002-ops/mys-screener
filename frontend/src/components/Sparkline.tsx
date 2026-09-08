/**
 * Shared sparkline.
 *
 * Replaces five hand-rolled copies (HomePanel ×2, ScanTable, MarketsPanel ×2)
 * and, more importantly, the `genMockSparkline` sine wave that used to feed the
 * home page. Every consumer must pass REAL values — if a series is missing,
 * render the `empty` state rather than inventing a shape.
 */

type SparklineProps = {
  values: number[];
  color: string;
  fill?: string;
  height?: number;
  width?: number;
  /**
   * For series where LOWER is better (group rank #1 is the top). Flips the y
   * mapping so an improving rank visually rises.
   */
  invert?: boolean;
  /**
   * Minimum displayed range, as a percentage of the series midpoint.
   *
   * Without this, every series is normalised to its own min/max, so a stock
   * that drifted 1.5% across 20 sessions fills the full height and reads as
   * violent chop — the opposite of the truth, and actively misleading in a
   * screener whose job includes finding tight bases.
   *
   * Set it and the value becomes "the move that fills the height": at 10, a
   * 10%+ swing uses the full box, 3% uses about a third, 1% is nearly flat.
   * That also makes rows COMPARABLE to one another — with per-series
   * normalisation, two adjacent sparklines at the same visual amplitude can be
   * a 40% trend and a 1% drift, which is worse than showing nothing.
   *
   * Left undefined for non-price series (ranks, breadth) where the reader
   * wants maximum detail and there is no meaningful "percent move".
   */
  minRangePct?: number;
  /** Accessible description, e.g. "Rank trend: 12 to 3 over 10 sessions". */
  label?: string;
  className?: string;
};

export function Sparkline({
  values,
  color,
  fill,
  height = 36,
  width = 100,
  invert = false,
  minRangePct,
  label,
  className,
}: SparklineProps) {
  const clean = (values ?? []).filter((v) => Number.isFinite(v));

  // Fewer than two real points is not a trend. Show a flat hairline rather than
  // fabricating a curve — the whole point of this component.
  if (clean.length < 2) {
    return (
      <svg
        className={className}
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="none"
        role="img"
        aria-label={label ?? "No trend data yet"}
        style={{ height }}
      >
        <line
          x1="0"
          y1={height / 2}
          x2={width}
          y2={height / 2}
          stroke={color}
          strokeOpacity="0.25"
          strokeWidth="1.5"
          strokeDasharray="3 3"
        />
      </svg>
    );
  }

  let min = Math.min(...clean);
  let max = Math.max(...clean);

  if (minRangePct && minRangePct > 0) {
    const mid = (max + min) / 2;
    const floor = Math.abs(mid) * (minRangePct / 100);
    if (max - min < floor) {
      // Widen symmetrically about the midpoint so the line keeps its shape and
      // simply occupies less of the height.
      min = mid - floor / 2;
      max = mid + floor / 2;
    }
  }

  const range = max - min || 1;
  const step = width / (clean.length - 1);
  const pad = 2;
  const usable = height - pad * 2;

  const y = (v: number) => {
    const t = (v - min) / range; // 0 = min, 1 = max
    const norm = invert ? t : 1 - t; // invert => larger value sits lower
    return (norm * usable + pad).toFixed(2);
  };

  const points = clean.map((v, i) => `${(i * step).toFixed(2)},${y(v)}`);
  const pathD = `M ${points.join(" L ")}`;
  const areaD = `${pathD} L ${width},${height} L 0,${height} Z`;

  return (
    <svg
      className={className}
      viewBox={`0 0 ${width} ${height}`}
      preserveAspectRatio="none"
      role="img"
      aria-label={label ?? `Trend from ${clean[0]} to ${clean[clean.length - 1]}`}
      style={{ height }}
    >
      {fill ? <path d={areaD} fill={fill} /> : null}
      <path
        d={pathD}
        stroke={color}
        strokeWidth="1.8"
        fill="none"
        strokeLinejoin="round"
        strokeLinecap="round"
      />
    </svg>
  );
}
