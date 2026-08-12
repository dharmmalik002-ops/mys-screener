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

type Props = { points: HistoricalBreadthDataPoint[] };

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
export function BreadthTimeline({ points }: Props) {
  const [range, setRange] = useState<(typeof RANGES)[number]["key"]>("3y");

  const data = useMemo(() => {
    const chosen = RANGES.find((r) => r.key === range) ?? RANGES[1];
    return points.slice(-chosen.sessions);
  }, [points, range]);

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
          <span>
            <strong>{latest.above_ma50_pct?.toFixed(0) ?? "—"}%</strong> above 50-DMA
          </span>
          <span>
            <strong>{latest.above_sma200_pct?.toFixed(0) ?? "—"}%</strong> above 200-SMA
          </span>
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
            <Line
              type="monotone"
              dataKey="above_ma50_pct"
              name="% above 50-DMA"
              stroke="var(--accent)"
              strokeWidth={1.6}
              dot={false}
              connectNulls={false}
            />
            <Line
              type="monotone"
              dataKey="above_sma200_pct"
              name="% above 200-SMA"
              stroke="var(--text-muted)"
              strokeWidth={1.3}
              dot={false}
              connectNulls={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <p className="mkb-note">
        Nifty 500. Gaps at the start are real: a moving average does not exist until it
        has enough history, and the backend sends those sessions as null rather than
        zero. Shown as context — bucketed against forward index returns and drawdowns
        over ~500 sessions, breadth did not rank monotonically, so it does not feed the
        exposure verdict.
      </p>
    </div>
  );
}
