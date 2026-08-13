import { useMemo, useState } from "react";
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
  /** Which universe the backend actually served — it falls back when the Nifty 500 file is absent. */
  universeLabel?: string;
};

const RANGES = [
  { key: "1y", label: "1Y", sessions: 250 },
  { key: "3y", label: "3Y", sessions: 10_000 },
] as const;

/**
 * Breadth over years, not days.
 *
 * Everything else on this page describes today. This is the only place the
 * regime can be seen turning, which is the whole reason it is here.
 *
 * Null values are passed through to Recharts deliberately: the first ~190
 * sessions have a 20-DMA but no 200-SMA yet, and the backend sends null rather
 * than 0 for exactly that reason. `connectNulls={false}` makes the line stop
 * where the data stops instead of drawing a plunge to zero that never happened.
 */
export function BreadthTimeline({ points, universeLabel = "Nifty 500" }: Props) {
  const [range, setRange] = useState<(typeof RANGES)[number]["key"]>("3y");

  const data = useMemo(() => {
    const chosen = RANGES.find((r) => r.key === range) ?? RANGES[1];
    return points.slice(-chosen.sessions);
  }, [points, range]);

  // Which series actually carry data. On the Nifty 500 file this is the 50-DMA
  // and 200-SMA; when that file is absent the backend falls back to the XP
  // universe, which only has a 20-EMA — plotting the hardcoded pair there drew
  // two empty lines and a header of dashes.
  const series = useMemo(() => {
    const has = (key: keyof HistoricalBreadthDataPoint) =>
      data.some((p) => typeof p[key] === "number");
    const candidates = [
      { key: "above_ma50_pct" as const, name: "% above 50-DMA", stroke: "var(--accent)", width: 1.6 },
      { key: "above_sma200_pct" as const, name: "% above 200-SMA", stroke: "var(--text-muted)", width: 1.3 },
      { key: "above_ma20_pct" as const, name: "% above 20-EMA", stroke: "var(--accent)", width: 1.6 },
    ];
    const live = candidates.filter((c) => has(c.key));
    // Prefer the 50/200 pair; only fall back to the 20-EMA when neither exists.
    const preferred = live.filter((c) => c.key !== "above_ma20_pct");
    return preferred.length ? preferred : live;
  }, [data]);

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
        <div className="mkb-now">
          {series.map((s) => (
            <span key={s.key}>
              <strong>{(latest[s.key] as number | null)?.toFixed(0) ?? "—"}%</strong>{" "}
              {s.name.replace("% above ", "above ")}
            </span>
          ))}
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
                background: "var(--bg-alt)",
                border: "1px solid var(--line-strong)",
                borderRadius: 8,
                fontSize: 12,
              }}
              labelStyle={{ color: "var(--text-muted)" }}
            />
            {series.map((s) => (
              <Line
                key={s.key}
                type="monotone"
                dataKey={s.key}
                name={s.name}
                stroke={s.stroke}
                strokeWidth={s.width}
                dot={false}
                connectNulls={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      <p className="mkb-note">
        {universeLabel}. Gaps at the start are real: a moving average does not exist until it
        has enough history, and the backend sends those sessions as null rather than
        zero. Shown as context — bucketed against forward index returns and drawdowns
        over ~500 sessions, breadth did not rank monotonically, so it does not feed the
        exposure verdict.
      </p>
    </div>
  );
}
