/**
 * DCFCalculator — 2-stage DCF, WACC helper, sensitivity table, chart
 */
import { useState, useMemo } from "react";
import {
  ComposedChart, Bar, Area, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, ReferenceLine,
  BarChart,
} from "recharts";
import type { CompanyFundamentals } from "../../lib/api";

type Props = {
  fundamentals: CompanyFundamentals;
  market: string;
};

const CCY = (_mkt: string) => "₹";

function roundSf(n: number, sf = 3) {
  if (!isFinite(n) || n === 0) return n;
  const factor = Math.pow(10, sf - Math.floor(Math.log10(Math.abs(n))) - 1);
  return Math.round(n * factor) / factor;
}

function fmtCr(val: number, ccy: string) {
  const abs = Math.abs(val);
  if (abs >= 1_00_000) return `${ccy}${(val / 1_00_000).toFixed(1)}L Cr`;
  if (abs >= 1_000) return `${ccy}${(val / 1000).toFixed(1)}K Cr`;
  return `${ccy}${val.toFixed(0)} Cr`;
}

const CustomTooltip = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: "var(--surface-strong,#1a1a2e)", border: "1px solid rgba(255,255,255,0.12)", borderRadius: 8, padding: "10px 14px", fontSize: "0.77rem" }}>
      <div style={{ fontWeight: 700, color: "#fff", marginBottom: 5 }}>{label}</div>
      {payload.map((p: any) => (
        <div key={p.dataKey} style={{ display: "flex", gap: 6, marginBottom: 2 }}>
          <span style={{ color: "rgba(255,255,255,0.6)" }}>{p.name}:</span>
          <span style={{ color: p.color || p.fill, fontWeight: 700 }}>{p.value != null ? fmtCr(p.value, "") : "—"}</span>
        </div>
      ))}
    </div>
  );
};

function Slider({ label, min, max, step, value, onChange, unit, color }: {
  label: string; min: number; max: number; step: number;
  value: number; onChange: (v: number) => void; unit: string; color?: string;
}) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <span style={{ fontSize: "0.7rem", color: "rgba(255,255,255,0.6)" }}>{label}</span>
        <span style={{ fontSize: "0.75rem", fontWeight: 700, color: color || "var(--accent,#7c6aff)", fontFamily: "monospace" }}>{value}{unit}</span>
      </div>
      <input type="range" min={min} max={max} step={step} value={value}
        onChange={e => onChange(parseFloat(e.target.value))}
        style={{ accentColor: color || "var(--accent,#7c6aff)", width: "100%" }} />
    </div>
  );
}

export function DCFCalculator({ fundamentals, market }: Props) {
  const ccy = CCY(market);

  // Seed values from data
  const sortedCf = [...(fundamentals.cash_flow ?? [])].sort((a, b) => b.period.localeCompare(a.period));
  const sortedBs = [...(fundamentals.balance_sheet ?? [])].sort((a, b) => b.period.localeCompare(a.period));
  const latestCf = sortedCf[0];
  const latestBs = sortedBs[0];

  const seedFcf = latestCf?.free_cash_flow_crore ?? 0;
  const seedDebt = (latestBs?.debt_crore ?? 0) - (latestBs?.cash_and_equivalents_crore ?? 0);
  const seedMarketCap = fundamentals.valuation?.market_cap_crore ?? 0;

  // Parameters
  const [baseFcf, setBaseFcf] = useState(+seedFcf.toFixed(0));
  const [g1, setG1] = useState(15);       // stage-1 growth %
  const [stage1Years, setStage1Years] = useState(10);
  const [g2, setG2] = useState(10);       // stage-2 growth %
  const [stage2Years, setStage2Years] = useState(5);
  const [tg, setTg] = useState(3);        // terminal growth %
  const [wacc, setWacc] = useState(12);   // WACC %
  const [netDebt, setNetDebt] = useState(+seedDebt.toFixed(0));
  const [marketCap, setMarketCap] = useState(+seedMarketCap.toFixed(0));
  const [activeTab, setActiveTab] = useState<"chart" | "sensitivity">("chart");

  // Historical FCF for chart
  const historicalFcf = sortedCf.reverse().map(c => ({
    period: c.period,
    fcf: c.free_cash_flow_crore ?? null,
  }));

  // DCF computation
  const dcf = useMemo(() => {
    if (!baseFcf || wacc <= tg) return null;
    const waccD = wacc / 100;
    const g1D = g1 / 100;
    const g2D = g2 / 100;
    const tgD = tg / 100;

    let pv = 0;
    let fcf = baseFcf;
    const projections: { period: string; fcf: number; pv: number }[] = [];

    // Stage 1
    for (let y = 1; y <= stage1Years; y++) {
      fcf *= (1 + g1D);
      const disc = fcf / Math.pow(1 + waccD, y);
      pv += disc;
      projections.push({ period: `Y+${y}`, fcf: +fcf.toFixed(0), pv: +disc.toFixed(0) });
    }

    // Stage 2
    for (let y = stage1Years + 1; y <= stage1Years + stage2Years; y++) {
      fcf *= (1 + g2D);
      const disc = fcf / Math.pow(1 + waccD, y);
      pv += disc;
      projections.push({ period: `Y+${y}`, fcf: +fcf.toFixed(0), pv: +disc.toFixed(0) });
    }

    // Terminal value
    const tv = (fcf * (1 + tgD)) / (waccD - tgD);
    const tvPv = tv / Math.pow(1 + waccD, stage1Years + stage2Years);
    pv += tvPv;

    const intrinsic = pv - netDebt;
    const mos = marketCap > 0 ? ((intrinsic - marketCap) / marketCap * 100) : null;

    // Reverse DCF: implied growth rate
    // pv ≈ baseFcf * (1+g) / (wacc-g) → simplified single-stage reverse
    const impliedG = marketCap > 0 && netDebt != null
      ? ((marketCap + netDebt) * waccD - baseFcf) / ((marketCap + netDebt) + baseFcf) * 100 : null;

    return { pv: +pv.toFixed(0), tv: +tv.toFixed(0), tvPv: +tvPv.toFixed(0), intrinsic: +intrinsic.toFixed(0), mos, impliedG: impliedG != null ? +impliedG.toFixed(1) : null, projections };
  }, [baseFcf, g1, stage1Years, g2, stage2Years, tg, wacc, netDebt, marketCap]);

  // Sensitivity table (wacc vs growth)
  const sensitivityRows = useMemo(() => {
    const waccRange = [wacc - 2, wacc - 1, wacc, wacc + 1, wacc + 2];
    const gRange = [g1 - 4, g1 - 2, g1, g1 + 2, g1 + 4];
    return gRange.map(g => ({
      g,
      values: waccRange.map(w => {
        if (w <= tg) return null;
        const wd = w / 100; const gd = g / 100; const tgd = tg / 100;
        let pv = 0; let fcf = baseFcf;
        for (let y = 1; y <= stage1Years; y++) { fcf *= (1 + gd); pv += fcf / Math.pow(1 + wd, y); }
        for (let y = stage1Years + 1; y <= stage1Years + stage2Years; y++) { fcf *= (1 + g2 / 100); pv += fcf / Math.pow(1 + wd, y); }
        const tv = (fcf * (1 + tgd)) / (wd - tgd);
        pv += tv / Math.pow(1 + wd, stage1Years + stage2Years);
        return +(pv - netDebt).toFixed(0);
      }),
      waccRange,
    }));
  }, [baseFcf, g1, stage1Years, g2, stage2Years, tg, wacc, netDebt]);

  const chartData = [
    ...historicalFcf.map(h => ({ period: h.period, hist: h.fcf })),
    ...(dcf?.projections.slice(0, 10).map(p => ({ period: p.period, proj: p.fcf, pv: p.pv })) ?? []),
  ];

  const mosColor = dcf?.mos != null ? (dcf.mos >= 20 ? "#22c55e" : dcf.mos >= 0 ? "#f59e0b" : "#ef4444") : "rgba(255,255,255,0.4)";

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      <span style={{ fontSize: "0.78rem", fontWeight: 700, color: "var(--text,#fff)", letterSpacing: "0.04em", textTransform: "uppercase" }}>
        DCF Intrinsic Value Calculator
      </span>

      {/* Two-column: sliders | output */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
        {/* Left: sliders */}
        <div style={{ display: "flex", flexDirection: "column", gap: 10, background: "rgba(255,255,255,0.02)", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 10, padding: "12px 14px" }}>
          <div style={{ fontSize: "0.68rem", fontWeight: 700, color: "rgba(255,255,255,0.5)", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 }}>Inputs</div>
          <Slider label={`Base FCF (${ccy} Cr)`} min={-10000} max={50000} step={100} value={baseFcf} onChange={setBaseFcf} unit="" color="#22c55e" />
          <Slider label="Stage 1 Growth (%)" min={0} max={50} step={0.5} value={g1} onChange={setG1} unit="%" color="#7c6aff" />
          <Slider label="Stage 1 Years" min={3} max={15} step={1} value={stage1Years} onChange={setStage1Years} unit="Y" />
          <Slider label="Stage 2 Growth (%)" min={0} max={30} step={0.5} value={g2} onChange={setG2} unit="%" color="#00d2ff" />
          <Slider label="Stage 2 Years" min={1} max={10} step={1} value={stage2Years} onChange={setStage2Years} unit="Y" />
          <Slider label="Terminal Growth (%)" min={0} max={8} step={0.25} value={tg} onChange={setTg} unit="%" color="#f59e0b" />
          <Slider label="WACC (%)" min={6} max={25} step={0.25} value={wacc} onChange={setWacc} unit="%" color="#ef4444" />
          <Slider label={`Net Debt (${ccy} Cr)`} min={-50000} max={200000} step={100} value={netDebt} onChange={setNetDebt} unit="" color="#6b7280" />
          <Slider label={`Market Cap (${ccy} Cr)`} min={100} max={2000000} step={100} value={marketCap} onChange={setMarketCap} unit="" color="#a855f7" />
        </div>

        {/* Right: output cards */}
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {[
            { label: "Intrinsic Value", val: dcf?.intrinsic != null ? fmtCr(dcf.intrinsic, ccy) : "—", color: "#22c55e" },
            { label: "Market Cap", val: marketCap > 0 ? fmtCr(marketCap, ccy) : "—", color: "rgba(255,255,255,0.7)" },
            { label: "Margin of Safety", val: dcf?.mos != null ? `${dcf.mos.toFixed(1)}%` : "—", color: mosColor },
            { label: "Terminal Value (PV)", val: dcf?.tvPv != null ? fmtCr(dcf.tvPv, ccy) : "—", color: "#f59e0b" },
            { label: "Implied Growth (Market)", val: dcf?.impliedG != null ? `${dcf.impliedG}%` : "—", color: "#a855f7" },
          ].map(item => (
            <div key={item.label} style={{ background: "rgba(255,255,255,0.03)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: 8, padding: "10px 14px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <span style={{ fontSize: "0.72rem", color: "rgba(255,255,255,0.55)" }}>{item.label}</span>
              <span style={{ fontSize: "0.95rem", fontWeight: 700, color: item.color, fontFamily: "monospace" }}>{item.val}</span>
            </div>
          ))}

          {dcf?.mos != null && (
            <div style={{ marginTop: 4, fontSize: "0.7rem", color: "rgba(255,255,255,0.4)", lineHeight: 1.5, padding: "8px 10px", background: "rgba(255,255,255,0.02)", borderRadius: 7, border: "1px solid rgba(255,255,255,0.06)" }}>
              {dcf.mos >= 20
                ? "Undervalued — large margin of safety."
                : dcf.mos >= 0
                ? "Slight undervaluation — limited margin of safety."
                : "Overvalued based on these assumptions."}
            </div>
          )}
        </div>
      </div>

      {/* Chart / Sensitivity tab toggle */}
      <div style={{ display: "flex", gap: 4 }}>
        {(["chart", "sensitivity"] as const).map(v => (
          <button key={v} onClick={() => setActiveTab(v)} style={{
            fontSize: "0.7rem", padding: "3px 10px", borderRadius: 6, cursor: "pointer",
            background: activeTab === v ? "var(--accent,#7c6aff)" : "rgba(255,255,255,0.07)",
            border: `1px solid ${activeTab === v ? "var(--accent,#7c6aff)" : "rgba(255,255,255,0.12)"}`,
            color: activeTab === v ? "#fff" : "rgba(255,255,255,0.6)",
          }}>
            {v === "chart" ? "FCF Projection" : "Sensitivity Table"}
          </button>
        ))}
      </div>

      {activeTab === "chart" && (
        <div style={{ height: 240 }}>
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={chartData} margin={{ top: 5, right: 10, bottom: 5, left: -5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.07)" />
              <XAxis dataKey="period" tick={{ fontSize: 9, fill: "rgba(255,255,255,0.4)" }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontSize: 9, fill: "rgba(255,255,255,0.4)" }} axisLine={false} tickLine={false} />
              <Tooltip content={<CustomTooltip />} />
              <Legend wrapperStyle={{ fontSize: 10, color: "rgba(255,255,255,0.5)" }} />
              <ReferenceLine x={historicalFcf[historicalFcf.length - 1]?.period} stroke="rgba(255,255,255,0.25)" strokeDasharray="6 3" label={{ value: "Today", fill: "rgba(255,255,255,0.4)", fontSize: 9 }} />
              <Bar dataKey="hist" name="Historical FCF" fill="#22c55e66" radius={[2, 2, 0, 0]} />
              <Area type="monotone" dataKey="proj" name="Projected FCF" stroke="#7c6aff" fill="rgba(124,106,255,0.15)" strokeWidth={2} />
              <Line type="monotone" dataKey="pv" name="Present Value" stroke="#f59e0b" strokeWidth={1.5} dot={false} strokeDasharray="5 3" />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      )}

      {activeTab === "sensitivity" && (
        <div style={{ overflowX: "auto" }}>
          <div style={{ fontSize: "0.68rem", color: "rgba(255,255,255,0.4)", marginBottom: 8 }}>
            Intrinsic Value ({ccy} Cr) — rows = Stage 1 Growth, columns = WACC
          </div>
          {sensitivityRows.length > 0 && (
            <table style={{ borderCollapse: "collapse", fontSize: "0.71rem", width: "100%" }}>
              <thead>
                <tr style={{ borderBottom: "1px solid rgba(255,255,255,0.1)" }}>
                  <th style={{ padding: "5px 8px", color: "rgba(255,255,255,0.5)", textAlign: "left", fontWeight: 600 }}>G↓ / WACC→</th>
                  {sensitivityRows[0].waccRange.map(w => (
                    <th key={w} style={{ padding: "5px 8px", textAlign: "right", color: w === wacc ? "var(--accent,#7c6aff)" : "rgba(255,255,255,0.5)", fontWeight: 600 }}>
                      {w}%
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sensitivityRows.map((row, i) => (
                  <tr key={i} style={{ borderBottom: "1px solid rgba(255,255,255,0.05)" }}>
                    <td style={{ padding: "5px 8px", color: row.g === g1 ? "var(--accent,#7c6aff)" : "rgba(255,255,255,0.7)", fontWeight: 600 }}>{row.g}%</td>
                    {row.values.map((v, j) => {
                      if (v == null) return <td key={j} style={{ padding: "5px 8px", textAlign: "right", color: "rgba(255,255,255,0.25)" }}>—</td>;
                      const mos = marketCap > 0 ? ((v - marketCap) / marketCap * 100) : 0;
                      const cellColor = mos >= 20 ? "#22c55e" : mos >= 0 ? "#f59e0b" : "#ef4444";
                      return (
                        <td key={j} style={{ padding: "5px 8px", textAlign: "right", fontFamily: "monospace", fontWeight: 600,
                          color: row.g === g1 && row.waccRange[j] === wacc ? "#fff" : cellColor,
                          background: row.g === g1 && row.waccRange[j] === wacc ? "rgba(124,106,255,0.15)" : "transparent",
                        }}>
                          {fmtCr(v, "")}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}

      <div style={{ fontSize: "0.66rem", color: "rgba(255,255,255,0.3)", borderTop: "1px solid rgba(255,255,255,0.07)", paddingTop: 8 }}>
        Disclaimer: DCF valuations are sensitive to assumptions. This calculator is for educational purposes only, not investment advice.
      </div>
    </div>
  );
}
