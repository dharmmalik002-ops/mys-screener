import { useCallback, useMemo, useState } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { HistoricalBreadthDataPoint } from "../../lib/api";
import { EmptyState } from "../EmptyState";
import "./BreadthTimeline.css";

type Props = {
  points: HistoricalBreadthDataPoint[];
  /** Which universe the backend actually served — it walks its sources best-first. */
  universeLabel?: string;
};

const RANGES = [
  { key: "1y", label: "1Y", sessions: 250 },
  { key: "3y", label: "3Y", sessions: 10_000 },
] as const;

type SeriesKey =
  | "above_ema20_pct"
  | "above_ema21_pct"
  | "above_ma50_pct"
  | "above_sma200_pct"
  | "above_ma20_pct";

/**
 * Every series the chart knows how to draw, in plot order. Only the ones the
 * served universe actually carries are rendered — the ₹1,000 cr+ file has the
 * first four, the XP fallback only has its own 20-EMA.
 *
 * The 20-EMA ships hidden by default. It tracks the 21-EMA within a couple of
 * points, so drawn together they read as one thick line rather than two
 * signals; it is here because it is the average the posture strip counts off,
 * and being able to overlay the two is the only way to check them against each
 * other.
 */
const SERIES: Array<{ key: SeriesKey; name: string; short: string; stroke: string; width: number }> = [
  { key: "above_ema21_pct", name: "% above 21-EMA", short: "21 EMA", stroke: "var(--accent)", width: 1.6 },
  { key: "above_ema20_pct", name: "% above 20-EMA", short: "20 EMA", stroke: "var(--accent-3)", width: 1.3 },
  { key: "above_ma50_pct", name: "% above 50-DMA", short: "50 DMA", stroke: "var(--amber)", width: 1.5 },
  { key: "above_sma200_pct", name: "% above 200-DMA", short: "200 DMA", stroke: "var(--text-muted)", width: 1.3 },
  { key: "above_ma20_pct", name: "% above 20-EMA", short: "20 EMA", stroke: "var(--accent)", width: 1.6 },
];

const DEFAULT_HIDDEN: SeriesKey[] = ["above_ema20_pct"];

const HIDDEN_KEY = "stockScanner.breadthHiddenSeries";

function loadHidden(): Set<SeriesKey> {
  try {
    const raw = window.localStorage.getItem(HIDDEN_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    return new Set(Array.isArray(parsed) ? (parsed as SeriesKey[]) : DEFAULT_HIDDEN);
  } catch {
    return new Set(DEFAULT_HIDDEN);
  }
}

/**
 * Breadth over years, not days.
 *
 * Everything else on this page describes today. This is the only place the
 * regime can be seen turning, which is the whole reason it is here.
 *
 * Null values are passed through to Recharts deliberately: the first ~200
 * sessions of a 200-DMA series do not exist, and the backend sends null rather
 * than 0 for exactly that reason. `connectNulls={false}` makes the line stop
 * where the data stops instead of drawing a plunge to zero that never happened.
 *
 * The header entries double as the legend and the show/hide control — three
 * lines on one 0-100 axis is a lot to read at once, and being able to drop the
 * 200-DMA to compare the two faster averages is most of the point.
 */
export function BreadthTimeline({ points, universeLabel }: Props) {
  const [range, setRange] = useState<(typeof RANGES)[number]["key"]>("3y");
  const [hidden, setHidden] = useState<Set<SeriesKey>>(loadHidden);

  const data = useMemo(() => {
    const chosen = RANGES.find((r) => r.key === range) ?? RANGES[1];
    return points.slice(-chosen.sessions);
  }, [points, range]);

  // Which series actually carry data in the window on screen.
  const live = useMemo(() => {
    const has = (key: SeriesKey) => data.some((p) => typeof p[key] === "number");
    const carried = SERIES.filter((s) => has(s.key));
    // The 20-EMA is the XP fallback's only series; never show it beside the
    // 21-EMA, where two near-identical lines would read as a real divergence.
    return carried.some((s) => s.key === "above_ema21_pct")
      ? carried.filter((s) => s.key !== "above_ma20_pct")
      : carried;
  }, [data]);

  const toggle = useCallback((key: SeriesKey) => {
    setHidden((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      try {
        window.localStorage.setItem(HIDDEN_KEY, JSON.stringify([...next]));
      } catch {
        /* private mode — the toggle still works for this session */
      }
      return next;
    });
  }, []);

  // Hiding every line would leave an empty axis with no way back except the
  // buttons themselves, so the last visible series stays visible.
  const visible = live.filter((s) => !hidden.has(s.key));
  const shown = visible.length ? visible : live;

  if (!points.length) {
    return (
      <EmptyState
        title="No breadth history yet"
        body="The multi-year breadth series has not been generated for this market."
      />
    );
  }

  const latest = points[points.length - 1];

  return (
    <div className="mkb">
      <div className="mkb-head">
        <div className="mkb-now" role="group" aria-label="Series shown">
          {live.map((s) => {
            const off = !shown.some((v) => v.key === s.key);
            return (
              <button
                key={s.key}
                type="button"
                className={`mkb-series${off ? " off" : ""}`}
                aria-pressed={!off}
                title={`${off ? "Show" : "Hide"} the ${s.short} line`}
                onClick={() => toggle(s.key)}
              >
                <span className="mkb-swatch" style={{ background: s.stroke }} aria-hidden="true" />
                <strong>{(latest[s.key] as number | null)?.toFixed(0) ?? "—"}%</strong>
                <span className="mkb-series-name">above {s.short}</span>
              </button>
            );
          })}
          <span className="mkb-span">
            {data.length} sessions · {data[0]?.date} → {latest.date}
          </span>
        </div>
        <div className="mkb-ranges" role="group" aria-label="Chart range">
          {RANGES.map((r) => (
            <button
              key={r.key}
              type="button"
              className={`mkb-range${range === r.key ? " active" : ""}`}
              aria-pressed={range === r.key}
              onClick={() => setRange(r.key)}
            >
              {r.label}
            </button>
          ))}
        </div>
      </div>

      <div className="mkb-chart">
        <ResponsiveContainer width="100%" height={240}>
          <LineChart data={data} margin={{ top: 6, right: 8, bottom: 4, left: -18 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--line)" vertical={false} />
            <XAxis
              dataKey="date"
              tick={{ fontSize: 10, fill: "var(--text-muted)" }}
              minTickGap={48}
              tickLine={false}
              axisLine={{ stroke: "var(--line)" }}
            />
            <YAxis
              domain={[0, 100]}
              ticks={[0, 25, 50, 75, 100]}
              tick={{ fontSize: 10, fill: "var(--text-muted)" }}
              tickLine={false}
              axisLine={false}
              unit="%"
            />
            <Tooltip
              contentStyle={{
                background: "var(--card-flat)",
                border: "1px solid var(--line-strong)",
                borderRadius: 8,
                fontSize: 12,
              }}
              labelStyle={{ color: "var(--text-muted)" }}
            />
            {shown.map((s) => (
              <Line
                key={s.key}
                type="monotone"
                dataKey={s.key}
                name={s.name}
                stroke={s.stroke}
                strokeWidth={s.width}
                dot={false}
                connectNulls={false}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      <p className="mkb-note">
        {universeLabel ?? "NSE stocks over Rs 1,000 cr"}. Click a reading above to hide or show
        its line. Gaps at the start are real: a moving average does not exist until it has
        enough history, and the backend sends those sessions as null rather than zero. Shown
        as context — bucketed against forward index returns and drawdowns over ~500 sessions,
        breadth did not rank monotonically, so it does not feed the exposure verdict.
      </p>
    </div>
  );
}
