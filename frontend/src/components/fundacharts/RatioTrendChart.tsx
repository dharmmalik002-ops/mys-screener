/**
 * RatioTrendChart — 10-year financial ratios plotted with Recharts.
 * Metrics: ROCE, ROE, Net Margin, EBITDA Margin, D/E, Interest Coverage
 */
import { useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  Legend, ReferenceLine, ResponsiveContainer,
} from "recharts";
import type { FinancialRatios, ProfitLossItem } from "../../lib/api";

type Props = {
  ratios: FinancialRatios[];
  profitLoss: ProfitLossItem[];
  market: string;
};

type MetricDef = {
  key: string; label: string; color: string;
  good: number; bad: number; invert: boolean; unit: string;
};

const METRICS: MetricDef[] = [
  { key: "roce_pct",           label: "ROCE",          color: "#7c6aff", good: 15, bad: 8,  invert: false, unit: "%" },
  { key: "roe_pct",            label: "ROE",            color: "#00d2ff", good: 15, bad: 8,  invert: false, unit: "%" },
  { key: "ebitda_margin_pct",  label: "EBITDA Margin",  color: "#f59e0b", good: 20, bad: 10, invert: false, unit: "%" },
  { key: "npm_pct",            label: "Net Margin",     color: "#22c55e", good: 12, bad: 5,  invert: false, unit: "%" },
  { key: "de_ratio",           label: "D/E Ratio",      color: "#ef4444", good: 0.5, bad: 1, invert: true,  unit: "x" },
  { key: "interest_coverage",  label: "Int. Coverage",  color: "#a855f7", good: 5,  bad: 2,  invert: false, unit: "x" },
];

function colorForValue(val: number, good: number, bad: number, invert: boolean): string {
  if (invert) return val <= good ? "#22c55e" : val >= bad ? "#ef4444" : "#e5e7eb";
  return val >= good ? "#22c55e" : val <= bad ? "#ef4444" : "#e5e7eb";
}

const CustomTooltip = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: "var(--surface-strong, #1a1a2e)",
      border: "1px solid rgba(255,255,255,0.12)",
      borderRadius: 8, padding: "10px 14px", fontSize: "0.77rem",
    }}>
      <div style={{ fontWeight: 700, marginBottom: 6, color: "#fff" }}>{label}</div>
      {payload.map((p: any) => (
        <div key={p.dataKey} style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 3 }}>
          <div style={{ width: 8, height: 8, borderRadius: "50%", background: p.color }} />
          <span style={{ color: "rgba(255,255,255,0.7)" }}>{p.name}:</span>
          <span style={{ color: p.color, fontWeight: 700 }}>{p.value}{METRICS.find(m => m.key === p.dataKey)?.unit || ""}</span>
        </div>
      ))}
    </div>
  );
};

export function RatioTrendChart({ ratios, profitLoss, market }: Props) {
  const [selectedMetrics, setSelectedMetrics] = useState<string[]>(["roce_pct", "roe_pct", "ebitda_margin_pct"]);
  const [view, setView] = useState<"chart" | "table">("chart");

  // Merge ratio rows with net margin from profit_loss
  const plByPeriod = Object.fromEntries(profitLoss.map(p => [p.period, p]));
  const chartData = [...ratios]
    .sort((a, b) => a.period.localeCompare(b.period))
    .map(r => {
      const pl = plByPeriod[r.period];
      return {
        year: r.period,
        roce_pct: r.roce_pct ?? null,
        roe_pct: r.roe_pct ?? null,
        ebitda_margin_pct: pl?.ebitda_margin_pct ?? null,
        npm_pct: pl?.net_profit_crore != null && pl?.sales_crore
          ? +((pl.net_profit_crore / pl.sales_crore) * 100).toFixed(1) : null,
        de_ratio: r.debt_to_equity_ratio ?? null,
        interest_coverage: r.interest_coverage ?? null,
      };
    });

  const toggle = (key: string) =>
    setSelectedMetrics(prev => prev.includes(key) ? prev.filter(k => k !== key) : [...prev, key]);

  const latest = chartData[chartData.length - 1];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      {/* Header row */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 8 }}>
        <span style={{ fontSize: "0.78rem", fontWeight: 700, color: "var(--text, #fff)", letterSpacing: "0.04em", textTransform: "uppercase" }}>
          Ratio Trends ({chartData.length}Y)
        </span>
        <div style={{ display: "flex", gap: 4 }}>
          {(["chart", "table"] as const).map(v => (
            <button key={v} onClick={() => setView(v)} style={{
              fontSize: "0.7rem", padding: "3px 10px", borderRadius: 6, cursor: "pointer",
              background: view === v ? "var(--accent, #7c6aff)" : "rgba(255,255,255,0.07)",
              border: `1px solid ${view === v ? "var(--accent, #7c6aff)" : "rgba(255,255,255,0.12)"}`,
              color: view === v ? "#fff" : "rgba(255,255,255,0.6)",
            }}>
              {v.charAt(0).toUpperCase() + v.slice(1)}
            </button>
          ))}
        </div>
      </div>

      {/* Metric selector chips */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
        {METRICS.map(m => {
          const active = selectedMetrics.includes(m.key);
          return (
            <button key={m.key} onClick={() => toggle(m.key)} style={{
              fontSize: "0.68rem", padding: "3px 10px", borderRadius: 100, cursor: "pointer",
              background: active ? `${m.color}22` : "rgba(255,255,255,0.05)",
              border: `1px solid ${active ? m.color + "66" : "rgba(255,255,255,0.1)"}`,
              color: active ? m.color : "rgba(255,255,255,0.5)",
              display: "flex", alignItems: "center", gap: 5,
            }}>
              <span style={{ width: 6, height: 6, borderRadius: "50%", background: active ? m.color : "rgba(255,255,255,0.3)", display: "inline-block" }} />
              {m.label}
            </button>
          );
        })}
      </div>

      {chartData.length === 0 ? (
        <div style={{ textAlign: "center", color: "rgba(255,255,255,0.4)", padding: "30px 0", fontSize: "0.8rem" }}>No ratio data available</div>
      ) : view === "chart" ? (
        <div style={{ height: 260 }}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData} margin={{ top: 5, right: 10, bottom: 5, left: -10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.07)" />
              <XAxis dataKey="year" tick={{ fontSize: 10, fill: "rgba(255,255,255,0.45)" }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontSize: 10, fill: "rgba(255,255,255,0.45)" }} axisLine={false} tickLine={false} />
              <Tooltip content={<CustomTooltip />} />
              <Legend wrapperStyle={{ fontSize: 11, color: "rgba(255,255,255,0.6)" }} />
              {METRICS.filter(m => selectedMetrics.includes(m.key)).map(m => (
                <Line key={m.key} type="monotone" dataKey={m.key} stroke={m.color}
                  strokeWidth={2} dot={{ r: 3, fill: m.color }} name={m.label}
                  connectNulls activeDot={{ r: 5 }} />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.75rem" }}>
            <thead>
              <tr style={{ borderBottom: "1px solid rgba(255,255,255,0.1)" }}>
                <th style={{ textAlign: "left", padding: "6px 10px", color: "rgba(255,255,255,0.5)", fontWeight: 600, fontSize: "0.7rem" }}>Period</th>
                {METRICS.filter(m => selectedMetrics.includes(m.key)).map(m => (
                  <th key={m.key} style={{ textAlign: "right", padding: "6px 10px", color: m.color, fontWeight: 600, fontSize: "0.7rem" }}>{m.label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[...chartData].reverse().map((r, i) => (
                <tr key={i} style={{ borderBottom: "1px solid rgba(255,255,255,0.06)" }}>
                  <td style={{ padding: "5px 10px", color: "rgba(255,255,255,0.85)", fontWeight: 600 }}>{r.year}</td>
                  {METRICS.filter(m => selectedMetrics.includes(m.key)).map(m => {
                    const val = (r as any)[m.key];
                    const color = val != null ? colorForValue(val, m.good, m.bad, m.invert) : "rgba(255,255,255,0.3)";
                    return (
                      <td key={m.key} style={{ textAlign: "right", padding: "5px 10px", color, fontFamily: "monospace", fontWeight: 600 }}>
                        {val != null ? `${val}${m.unit}` : "—"}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Current snapshot */}
      {latest && (
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
          {METRICS.map(m => {
            const val = (latest as any)[m.key];
            if (val == null) return null;
            const color = colorForValue(val, m.good, m.bad, m.invert);
            return (
              <div key={m.key} style={{
                background: "rgba(255,255,255,0.04)", border: "1px solid rgba(255,255,255,0.09)",
                borderRadius: 8, padding: "7px 12px", textAlign: "center", minWidth: 80,
              }}>
                <div style={{ fontSize: "0.65rem", color: "rgba(255,255,255,0.5)", textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 2 }}>{m.label}</div>
                <div style={{ fontSize: "0.95rem", fontWeight: 700, color, fontFamily: "monospace" }}>{val}{m.unit}</div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
