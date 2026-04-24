/**
 * QuarterlyChart — Quarterly revenue + net profit grouped bars with YoY% line overlay.
 * Uses Recharts ComposedChart.
 */
import {
  ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, ReferenceLine, Cell,
} from "recharts";
import type { QuarterlyResultItem } from "../../lib/api";

type Props = {
  data: QuarterlyResultItem[];
  market: string;
};

const CustomTooltip = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null;
  const currSymbol = payload[0]?.payload?._curr ?? "₹";
  const suffix = payload[0]?.payload?._suffix ?? " Cr";
  return (
    <div style={{
      background: "var(--surface-strong, #1a1a2e)",
      border: "1px solid rgba(255,255,255,0.12)",
      borderRadius: 8, padding: "10px 14px", fontSize: "0.77rem", minWidth: 160,
    }}>
      <div style={{ fontWeight: 700, marginBottom: 8, color: "#fff" }}>{label}</div>
      {payload.map((p: any) => {
        const isYoY = p.dataKey === "yoy";
        return (
          <div key={p.dataKey} style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 3 }}>
            <div style={{ width: 8, height: 8, borderRadius: isYoY ? "50%" : 2, background: p.color || p.fill }} />
            <span style={{ color: "rgba(255,255,255,0.65)" }}>{p.name}:</span>
            <span style={{ color: p.color || p.fill, fontWeight: 700 }}>
              {isYoY
                ? (p.value != null ? `${p.value > 0 ? "+" : ""}${p.value.toFixed(1)}%` : "—")
                : (p.value != null ? `${currSymbol}${p.value.toFixed(0)}${suffix}` : "—")}
            </span>
          </div>
        );
      })}
    </div>
  );
};

export function QuarterlyChart({ data, market }: Props) {
  if (!data || data.length === 0) {
    return (
      <div style={{ textAlign: "center", color: "rgba(255,255,255,0.4)", padding: "30px 0", fontSize: "0.8rem" }}>
        No quarterly data available
      </div>
    );
  }

  const currSymbol = "₹";
  const suffix = " Cr";

  const sorted = [...data]
    .sort((a, b) => a.period.localeCompare(b.period))
    .slice(-12); // last 12 quarters

  const chartData = sorted.map(q => ({
    quarter: q.period,
    revenue: q.sales_crore ?? null,
    profit: q.net_profit_crore ?? null,
    yoy: q.yoy_change_pct ?? null,
    _curr: currSymbol,
    _suffix: suffix,
  }));

  const maxRevenue = Math.max(...chartData.map(d => d.revenue ?? 0));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <span style={{ fontSize: "0.78rem", fontWeight: 700, color: "var(--text, #fff)", letterSpacing: "0.04em", textTransform: "uppercase" }}>
          Quarterly Revenue &amp; Profit ({sorted.length}Q)
        </span>
        <span style={{ fontSize: "0.68rem", color: "rgba(255,255,255,0.4)" }}>
          Values in ₹ Crores
        </span>
      </div>

      <div style={{ height: 240 }}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={chartData} margin={{ top: 4, right: 20, bottom: 4, left: -10 }} barCategoryGap="25%">
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.07)" />
            <XAxis
              dataKey="quarter"
              tick={{ fontSize: 9, fill: "rgba(255,255,255,0.4)" }}
              axisLine={false} tickLine={false}
              interval={sorted.length > 8 ? 1 : 0}
            />
            <YAxis
              yAxisId="left"
              tick={{ fontSize: 9, fill: "rgba(255,255,255,0.4)" }}
              axisLine={false} tickLine={false}
              tickFormatter={(v) => maxRevenue >= 1000 ? `${(v / 1000).toFixed(0)}K` : String(v)}
            />
            <YAxis
              yAxisId="right"
              orientation="right"
              tick={{ fontSize: 9, fill: "rgba(255,255,255,0.4)" }}
              axisLine={false} tickLine={false}
              tickFormatter={(v) => `${v}%`}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 10, color: "rgba(255,255,255,0.55)", paddingTop: 6 }} />
            <ReferenceLine yAxisId="right" y={0} stroke="rgba(255,255,255,0.18)" strokeDasharray="4 2" />

            <Bar yAxisId="left" dataKey="revenue" name="Revenue" radius={[3, 3, 0, 0]} maxBarSize={28}>
              {chartData.map((entry, i) => (
                <Cell
                  key={`rev-${i}`}
                  fill={entry.yoy != null && entry.yoy < 0 ? "rgba(239,68,68,0.55)" : "rgba(79,140,255,0.65)"}
                />
              ))}
            </Bar>
            <Bar yAxisId="left" dataKey="profit" name="Net Profit" fill="rgba(34,217,138,0.55)" radius={[3, 3, 0, 0]} maxBarSize={28} />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="yoy"
              name="Revenue YoY %"
              stroke="#f59e0b"
              strokeWidth={2}
              dot={{ r: 3, fill: "#f59e0b" }}
              connectNulls
              activeDot={{ r: 5 }}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
