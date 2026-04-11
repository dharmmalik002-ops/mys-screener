/**
 * ShareholdingChart — Donut (latest) + stacked bar trend + change indicators
 */
import { useState } from "react";
import {
  PieChart, Pie, Cell, Tooltip as RTooltip,
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Legend,
  ResponsiveContainer, AreaChart, Area,
} from "recharts";
import type { ShareholdingPatternItem } from "../../lib/api";

type Props = {
  data: ShareholdingPatternItem[];
};

const COLORS: Record<"promoter_pct" | "fii_pct" | "dii_pct" | "public_pct", string> = {
  promoter_pct: "#7c6aff",
  fii_pct: "#00d2ff",
  dii_pct: "#f59e0b",
  public_pct: "#6b7280",
};

const LABELS: Record<string, string> = {
  promoter_pct: "Promoter",
  fii_pct: "FII",
  dii_pct: "DII",
  public_pct: "Public",
};

const KEYS = ["promoter_pct", "fii_pct", "dii_pct", "public_pct"] as const;

const CustomPieTooltip = ({ active, payload }: any) => {
  if (!active || !payload?.length) return null;
  const { name, value } = payload[0];
  return (
    <div style={{ background: "var(--surface-strong,#1a1a2e)", border: "1px solid rgba(255,255,255,0.12)", borderRadius: 8, padding: "8px 12px", fontSize: "0.77rem" }}>
      <span style={{ color: "#fff", fontWeight: 700 }}>{name}: </span>
      <span style={{ color: payload[0].payload.color || "#fff", fontWeight: 700 }}>{value?.toFixed(1)}%</span>
    </div>
  );
};

const CustomBarTooltip = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: "var(--surface-strong,#1a1a2e)", border: "1px solid rgba(255,255,255,0.12)", borderRadius: 8, padding: "10px 14px", fontSize: "0.77rem" }}>
      <div style={{ fontWeight: 700, color: "#fff", marginBottom: 6 }}>{label}</div>
      {payload.map((p: any) => (
        <div key={p.dataKey} style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 2 }}>
          <div style={{ width: 8, height: 8, borderRadius: "50%", background: p.fill }} />
          <span style={{ color: "rgba(255,255,255,0.7)" }}>{LABELS[p.dataKey]}:</span>
          <span style={{ color: p.fill, fontWeight: 700 }}>{p.value?.toFixed(1)}%</span>
        </div>
      ))}
    </div>
  );
};

export function ShareholdingChart({ data }: Props) {
  const [view, setView] = useState<"bar" | "area">("bar");

  if (!data || data.length === 0) {
    return (
      <div style={{ textAlign: "center", color: "rgba(255,255,255,0.4)", padding: "30px 0", fontSize: "0.8rem" }}>
        No shareholding data available
      </div>
    );
  }

  const sorted = [...data].sort((a, b) => a.period.localeCompare(b.period));
  const latest = sorted[sorted.length - 1];
  const prev = sorted.length > 1 ? sorted[sorted.length - 2] : null;

  const pieData = KEYS.map(k => ({
    name: LABELS[k],
    value: latest[k] ?? 0,
    color: COLORS[k],
  }));

  const chartData = sorted.map(d => ({
    period: d.period,
    promoter_pct: d.promoter_pct ?? 0,
    fii_pct: d.fii_pct ?? 0,
    dii_pct: d.dii_pct ?? 0,
    public_pct: d.public_pct ?? 0,
  }));

  const delta = prev ? {
    promoter_pct: (latest.promoter_pct ?? 0) - (prev.promoter_pct ?? 0),
    fii_pct: (latest.fii_pct ?? 0) - (prev.fii_pct ?? 0),
    dii_pct: (latest.dii_pct ?? 0) - (prev.dii_pct ?? 0),
    public_pct: (latest.public_pct ?? 0) - (prev.public_pct ?? 0),
  } : null;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 8 }}>
        <span style={{ fontSize: "0.78rem", fontWeight: 700, color: "var(--text,#fff)", letterSpacing: "0.04em", textTransform: "uppercase" }}>
          Shareholding Pattern — {latest.period}
        </span>
        <div style={{ display: "flex", gap: 4 }}>
          {(["bar", "area"] as const).map(v => (
            <button key={v} onClick={() => setView(v)} style={{
              fontSize: "0.7rem", padding: "3px 10px", borderRadius: 6, cursor: "pointer",
              background: view === v ? "var(--accent,#7c6aff)" : "rgba(255,255,255,0.07)",
              border: `1px solid ${view === v ? "var(--accent,#7c6aff)" : "rgba(255,255,255,0.12)"}`,
              color: view === v ? "#fff" : "rgba(255,255,255,0.6)",
            }}>
              {v === "bar" ? "Stacked Bar" : "Area Trend"}
            </button>
          ))}
        </div>
      </div>

      {/* Two-column layout: donut left, trend right */}
      <div style={{ display: "grid", gridTemplateColumns: "200px 1fr", gap: 16, alignItems: "start" }}>
        {/* Donut */}
        <div>
          <div style={{ height: 160 }}>
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={pieData} cx="50%" cy="50%" innerRadius={45} outerRadius={70}
                  dataKey="value" stroke="none">
                  {pieData.map((entry, i) => (
                    <Cell key={i} fill={entry.color} />
                  ))}
                </Pie>
                <RTooltip content={<CustomPieTooltip />} />
              </PieChart>
            </ResponsiveContainer>
          </div>
          {/* Legend with delta */}
          <div style={{ display: "flex", flexDirection: "column", gap: 5, marginTop: 4 }}>
            {KEYS.map(k => {
              const val = latest[k] ?? 0;
              const d = delta ? (delta as any)[k] : null;
              const arrow = d != null ? (d > 0.1 ? "▲" : d < -0.1 ? "▼" : "—") : "";
              const dColor = d != null ? (k === "promoter_pct" ? (d > 0 ? "#22c55e" : d < 0 ? "#ef4444" : "rgba(255,255,255,0.4)") : "rgba(255,255,255,0.4)") : "rgba(255,255,255,0.4)";
              return (
                <div key={k} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 6 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
                    <div style={{ width: 8, height: 8, borderRadius: "50%", background: COLORS[k], flexShrink: 0 }} />
                    <span style={{ fontSize: "0.7rem", color: "rgba(255,255,255,0.7)" }}>{LABELS[k]}</span>
                  </div>
                  <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
                    <span style={{ fontSize: "0.75rem", color: COLORS[k], fontWeight: 700, fontFamily: "monospace" }}>{val.toFixed(1)}%</span>
                    {arrow && d != null && Math.abs(d) > 0.05 && (
                      <span style={{ fontSize: "0.65rem", color: dColor, fontWeight: 700 }}>{arrow}{Math.abs(d).toFixed(1)}</span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Trend Chart */}
        <div style={{ height: 230 }}>
          <ResponsiveContainer width="100%" height="100%">
            {view === "bar" ? (
              <BarChart data={chartData} margin={{ top: 5, right: 5, bottom: 5, left: -15 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.07)" />
                <XAxis dataKey="period" tick={{ fontSize: 9, fill: "rgba(255,255,255,0.4)" }} axisLine={false} tickLine={false} />
                <YAxis domain={[0, 100]} tick={{ fontSize: 9, fill: "rgba(255,255,255,0.4)" }} axisLine={false} tickLine={false} />
                <RTooltip content={<CustomBarTooltip />} />
                <Legend wrapperStyle={{ fontSize: 10, color: "rgba(255,255,255,0.5)" }} />
                {KEYS.map(k => (
                  <Bar key={k} dataKey={k} name={LABELS[k]} stackId="a" fill={COLORS[k]} />
                ))}
              </BarChart>
            ) : (
              <AreaChart data={chartData} margin={{ top: 5, right: 5, bottom: 5, left: -15 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.07)" />
                <XAxis dataKey="period" tick={{ fontSize: 9, fill: "rgba(255,255,255,0.4)" }} axisLine={false} tickLine={false} />
                <YAxis domain={[0, 100]} tick={{ fontSize: 9, fill: "rgba(255,255,255,0.4)" }} axisLine={false} tickLine={false} />
                <RTooltip content={<CustomBarTooltip />} />
                <Legend wrapperStyle={{ fontSize: 10, color: "rgba(255,255,255,0.5)" }} />
                {KEYS.map(k => (
                  <Area key={k} type="monotone" dataKey={k} name={LABELS[k]}
                    stackId="a" fill={COLORS[k] + "66"} stroke={COLORS[k]} strokeWidth={1.5} />
                ))}
              </AreaChart>
            )}
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
