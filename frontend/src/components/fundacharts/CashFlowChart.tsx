/**
 * CashFlowChart — OCF / FCF / Capex grouped bars + FCF trend line
 */
import {
  ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, ReferenceLine,
} from "recharts";
import type { CashFlowItem } from "../../lib/api";

type Props = { cashFlow: CashFlowItem[] };

const CustomTooltip = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: "var(--surface-strong,#1a1a2e)", border: "1px solid rgba(255,255,255,0.12)", borderRadius: 8, padding: "10px 14px", fontSize: "0.77rem" }}>
      <div style={{ fontWeight: 700, color: "#fff", marginBottom: 6 }}>{label}</div>
      {payload.map((p: any) => (
        <div key={p.dataKey} style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 2 }}>
          <div style={{ width: 8, height: 8, borderRadius: "50%", background: p.color || p.fill }} />
          <span style={{ color: "rgba(255,255,255,0.7)" }}>{p.name}:</span>
          <span style={{ color: p.color || p.fill, fontWeight: 700 }}>
            {p.value != null ? `₹${p.value >= 0 ? "" : ""}${p.value.toFixed(0)} Cr` : "—"}
          </span>
        </div>
      ))}
    </div>
  );
};

function MetricCard({ label, value, color }: { label: string; value: number | null | undefined; color: string }) {
  const fmt = (v: number | null | undefined) => {
    if (v == null) return "—";
    const abs = Math.abs(v);
    if (abs >= 1000) return `₹${(v / 1000).toFixed(1)}K Cr`;
    return `₹${v.toFixed(0)} Cr`;
  };
  const sign = value != null && value >= 0;
  return (
    <div style={{ background: "rgba(255,255,255,0.04)", border: "1px solid rgba(255,255,255,0.09)", borderRadius: 8, padding: "7px 12px", textAlign: "center", flex: 1, minWidth: 100 }}>
      <div style={{ fontSize: "0.65rem", color: "rgba(255,255,255,0.5)", textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 2 }}>{label}</div>
      <div style={{ fontSize: "0.9rem", fontWeight: 700, color: value == null ? "rgba(255,255,255,0.3)" : (sign ? color : "#ef4444"), fontFamily: "monospace" }}>
        {fmt(value)}
      </div>
    </div>
  );
}

export function CashFlowChart({ cashFlow }: Props) {
  if (!cashFlow || cashFlow.length === 0) {
    return (
      <div style={{ textAlign: "center", color: "rgba(255,255,255,0.4)", padding: "30px 0", fontSize: "0.8rem" }}>
        No cash flow data available
      </div>
    );
  }

  const sorted = [...cashFlow].sort((a, b) => a.period.localeCompare(b.period));
  const latest = sorted[sorted.length - 1];

  const chartData = sorted.map(d => ({
    year: d.period,
    ocf: d.operating_cash_flow_crore ?? null,
    fcf: d.free_cash_flow_crore ?? null,
    capex: d.capital_expenditure_crore != null ? -Math.abs(d.capital_expenditure_crore) : null,
  }));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      <span style={{ fontSize: "0.78rem", fontWeight: 700, color: "var(--text,#fff)", letterSpacing: "0.04em", textTransform: "uppercase" }}>
        Cash Flow Quality ({sorted.length}Y)
      </span>

      {/* Metric cards */}
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
        <MetricCard label="Latest OCF" value={latest.operating_cash_flow_crore} color="#22c55e" />
        <MetricCard label="Latest FCF" value={latest.free_cash_flow_crore} color="#00d2ff" />
        <MetricCard label="Capex" value={latest.capital_expenditure_crore != null ? -Math.abs(latest.capital_expenditure_crore) : null} color="#f59e0b" />
      </div>

      <div style={{ height: 260 }}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={chartData} margin={{ top: 5, right: 10, bottom: 5, left: -5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.07)" />
            <XAxis dataKey="year" tick={{ fontSize: 10, fill: "rgba(255,255,255,0.45)" }} axisLine={false} tickLine={false} />
            <YAxis tick={{ fontSize: 10, fill: "rgba(255,255,255,0.45)" }} axisLine={false} tickLine={false} />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 11, color: "rgba(255,255,255,0.6)" }} />
            <ReferenceLine y={0} stroke="rgba(255,255,255,0.2)" />
            <Bar dataKey="ocf" name="Oper. CF" fill="#22c55e" opacity={0.8} radius={[2, 2, 0, 0]} />
            <Bar dataKey="capex" name="Capex" fill="#ef444466" radius={[2, 2, 0, 0]} />
            <Line type="monotone" dataKey="fcf" name="Free CF" stroke="#00d2ff"
              strokeWidth={2.5} dot={{ r: 3, fill: "#00d2ff" }} connectNulls activeDot={{ r: 5 }} />
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      {/* FCF quality note */}
      {latest.operating_cash_flow_crore != null && latest.free_cash_flow_crore != null && (
        <div style={{ fontSize: "0.72rem", color: "rgba(255,255,255,0.45)", borderTop: "1px solid rgba(255,255,255,0.08)", paddingTop: 8 }}>
          FCF Conversion:{" "}
          <span style={{
            fontWeight: 700,
            color: latest.operating_cash_flow_crore > 0
              ? (latest.free_cash_flow_crore / latest.operating_cash_flow_crore >= 0.7 ? "#22c55e" : "#f59e0b")
              : "rgba(255,255,255,0.4)",
          }}>
            {latest.operating_cash_flow_crore > 0
              ? `${((latest.free_cash_flow_crore / latest.operating_cash_flow_crore) * 100).toFixed(0)}%`
              : "N/A"}
          </span>
          <span style={{ marginLeft: 12 }}>
            Capex Intensity:{" "}
            <span style={{ fontWeight: 700, color: "#f59e0b" }}>
              {latest.capital_expenditure_crore != null && latest.operating_cash_flow_crore > 0
                ? `${((Math.abs(latest.capital_expenditure_crore) / latest.operating_cash_flow_crore) * 100).toFixed(0)}%`
                : "—"}
            </span>
          </span>
        </div>
      )}
    </div>
  );
}
