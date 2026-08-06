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

  const min = Math.min(...clean);
  const max = Math.max(...clean);
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
