/**
 * AnnualPLChart — Annual revenue + net profit as an area/bar chart.
 * Overlays operating margin as a line on the secondary axis.
 */
import {
  ComposedChart, Area, Bar, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, ReferenceLine,
} from "recharts";
import type { ProfitLossItem } from "../../lib/api";

type Props = {
  data: ProfitLossItem[];
  market: string;
};

const CustomTooltip = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null;
  const curr = payload[0]?.payload?._curr ?? "₹";
  const suf = payload[0]?.payload?._suf ?? " Cr";
  return (
    <div style={{
      background: "var(--surface-strong, #1a1a2e)",
      border: "1px solid rgba(255,255,255,0.12)",
      borderRadius: 8, padding: "10px 14px", fontSize: "0.77rem", minWidth: 170,
    }}>
      <div style={{ fontWeight: 700, marginBottom: 8, color: "#fff" }}>{label}</div>
      {payload.map((p: any) => {
        const isPct = ["opm", "npm"].includes(p.dataKey);
        return (
          <div key={p.dataKey} style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 3 }}>
            <div style={{ width: 8, height: 8, borderRadius: 2, background: p.color || p.fill }} />
            <span style={{ color: "rgba(255,255,255,0.65)" }}>{p.name}:</span>
            <span style={{ color: p.color || p.fill, fontWeight: 700 }}>
              {p.value != null
                ? isPct ? `${p.value.toFixed(1)}%` : `${curr}${p.value.toFixed(0)}${suf}`
                : "—"}
            </span>
          </div>
        );
      })}
    </div>
  );
};

export function AnnualPLChart({ data, market }: Props) {
  if (!data || data.length === 0) {
    return (
      <div style={{ textAlign: "center", color: "rgba(255,255,255,0.4)", padding: "30px 0", fontSize: "0.8rem" }}>
        No annual P&amp;L data available
      </div>
    );
  }

  const curr = market === "us" ? "$" : "₹";
  const suf = market === "us" ? " M" : " Cr";

  const sorted = [...data]
    .sort((a, b) => a.period.localeCompare(b.period))
    .slice(-10); // last 10 years

  const chartData = sorted.map(p => ({
    year: p.period,
    revenue: p.sales_crore ?? null,
    profit: p.net_profit_crore ?? null,
    opm: p.operating_margin_pct ?? p.ebitda_margin_pct ?? null,
    npm: (p.net_profit_crore != null && p.sales_crore && p.sales_crore > 0)
      ? +((p.net_profit_crore / p.sales_crore) * 100).toFixed(1) : null,
    _curr: curr,
    _suf: suf,
  }));

  const maxRevenue = Math.max(...chartData.map(d => d.revenue ?? 0));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <span style={{ fontSize: "0.78rem", fontWeight: 700, color: "var(--text, #fff)", letterSpacing: "0.04em", textTransform: "uppercase" }}>
          Annual Revenue &amp; Profit ({sorted.length}Y)
        </span>
        <span style={{ fontSize: "0.68rem", color: "rgba(255,255,255,0.4)" }}>
          Values in {market === "us" ? "$ Millions" : "₹ Crores"}
        </span>
      </div>

      <div style={{ height: 260 }}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={chartData} margin={{ top: 4, right: 20, bottom: 4, left: -10 }} barCategoryGap="30%">
            <defs>
              <linearGradient id="revenue-grad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#4f8cff" stopOpacity={0.35} />
                <stop offset="95%" stopColor="#4f8cff" stopOpacity={0.04} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.07)" />
            <XAxis
              dataKey="year"
              tick={{ fontSize: 9, fill: "rgba(255,255,255,0.4)" }}
              axisLine={false} tickLine={false}
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
            <ReferenceLine yAxisId="left" y={0} stroke="rgba(255,255,255,0.15)" />

            <Area
              yAxisId="left"
              type="monotone"
              dataKey="revenue"
              name="Revenue"
              fill="url(#revenue-grad)"
              stroke="#4f8cff"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: "#4f8cff" }}
            />
            <Bar yAxisId="left" dataKey="profit" name="Net Profit" fill="rgba(34,217,138,0.6)" radius={[3, 3, 0, 0]} maxBarSize={22} />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="opm"
              name="OPM %"
              stroke="#f59e0b"
              strokeWidth={2}
              dot={{ r: 3, fill: "#f59e0b" }}
              connectNulls
              activeDot={{ r: 5 }}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="npm"
              name="Net Margin %"
              stroke="#a855f7"
              strokeWidth={1.5}
              strokeDasharray="5 3"
              dot={false}
              connectNulls
              activeDot={{ r: 4 }}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
