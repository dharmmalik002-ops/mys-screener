/**
 * FundamentalScores — Piotroski F-Score, Altman Z-Score, DuPont decomposition, Radar overview
 */
import { useState } from "react";
import {
  RadarChart, PolarGrid, PolarAngleAxis, Radar,
  ResponsiveContainer, Tooltip,
} from "recharts";
import type {
  BalanceSheetItem, CashFlowItem, ProfitLossItem,
  FinancialRatios, ValuationSnapshot,
} from "../../lib/api";

type Props = {
  balanceSheet: BalanceSheetItem[];
  cashFlow: CashFlowItem[];
  profitLoss: ProfitLossItem[];
  ratios: FinancialRatios[];
  valuation: ValuationSnapshot | null;
  market: string;
};

type ScoreTab = "piotroski" | "altman" | "dupont" | "radar";

// ── Piotroski F-Score (9-pt) ────────────────────────────────────────────────

function calcPiotroski(
  bs: BalanceSheetItem[],
  cf: CashFlowItem[],
  pl: ProfitLossItem[],
  ra: FinancialRatios[],
): { criteria: { name: string; pass: boolean; detail: string }[]; score: number } {
  const latestPl = pl[0] ?? null;
  const prevPl   = pl[1] ?? null;
  const latestBs = bs[0] ?? null;
  const prevBs   = bs[1] ?? null;
  const latestCf = cf[0] ?? null;
  const latestRa = ra[0] ?? null;
  const prevRa   = ra[1] ?? null;

  // ROA = net_profit / total_assets
  const roa = latestPl && latestBs && latestBs.total_assets_crore && latestPl.net_profit_crore != null
    ? latestPl.net_profit_crore / latestBs.total_assets_crore : null;
  const roaPrev = prevPl && prevBs && prevBs.total_assets_crore && prevPl.net_profit_crore != null
    ? prevPl.net_profit_crore / prevBs.total_assets_crore : null;

  // OCF from cashFlow
  const ocf = latestCf?.operating_cash_flow_crore ?? null;

  // Leverage = debt / total_assets  (lower is better)
  const leverage = latestBs && latestBs.total_assets_crore && latestBs.debt_crore != null
    ? latestBs.debt_crore / latestBs.total_assets_crore : null;
  const leveragePrev = prevBs && prevBs.total_assets_crore && prevBs.debt_crore != null
    ? prevBs.debt_crore / prevBs.total_assets_crore : null;

  // Current ratio
  const cr = latestRa?.current_ratio ?? null;
  const crPrev = prevRa?.current_ratio ?? null;

  // Shares (use equity proxy — we don't have share count so skip dilution check)
  const equity = latestBs?.shareholders_equity_crore ?? null;
  const equityPrev = prevBs?.shareholders_equity_crore ?? null;

  // Gross margin proxy = operating_margin_pct
  const gm = latestPl?.operating_margin_pct ?? null;
  const gmPrev = prevPl?.operating_margin_pct ?? null;

  // Asset turnover
  const at = latestRa?.asset_turnover ?? null;
  const atPrev = prevRa?.asset_turnover ?? null;

  const criteria = [
    { name: "ROA > 0",               pass: roa != null ? roa > 0 : false,                         detail: roa != null ? `ROA = ${(roa * 100).toFixed(1)}%` : "N/A" },
    { name: "OCF > 0",               pass: ocf != null ? ocf > 0 : false,                         detail: ocf != null ? `OCF = ₹${ocf.toFixed(0)} Cr` : "N/A" },
    { name: "ROA improving",          pass: roa != null && roaPrev != null ? roa > roaPrev : false, detail: roaPrev != null && roa != null ? `${(roa*100).toFixed(1)}% vs ${(roaPrev*100).toFixed(1)}%` : "N/A" },
    { name: "OCF > Net Income",       pass: ocf != null && latestPl && latestPl.net_profit_crore != null ? ocf > latestPl.net_profit_crore : false, detail: "Accrual check" },
    { name: "Debt ratio decreasing",  pass: leverage != null && leveragePrev != null ? leverage < leveragePrev : false, detail: leveragePrev != null && leverage != null ? `${(leverage*100).toFixed(1)}% vs ${(leveragePrev*100).toFixed(1)}%` : "N/A" },
    { name: "Current ratio improving",pass: cr != null && crPrev != null ? cr > crPrev : false,   detail: cr != null ? `CR = ${cr.toFixed(2)}x` : "N/A" },
    { name: "No share dilution",      pass: equity != null && equityPrev != null ? equity <= equityPrev * 1.1 : false, detail: "Equity check" },
    { name: "Gross margin improving", pass: gm != null && gmPrev != null ? gm > gmPrev : false,   detail: gm != null ? `Margin = ${gm.toFixed(1)}%` : "N/A" },
    { name: "Asset turnover improving",pass: at != null && atPrev != null ? at > atPrev : false,  detail: at != null ? `AT = ${at.toFixed(2)}x` : "N/A" },
  ];

  return { criteria, score: criteria.filter(c => c.pass).length };
}

// ── Altman Z-Score ───────────────────────────────────────────────────────────

function calcAltman(
  bs: BalanceSheetItem[],
  pl: ProfitLossItem[],
  val: ValuationSnapshot | null,
) {
  const latestBs = bs[0] ?? null;
  const latestPl = pl[0] ?? null;

  if (!latestBs || !latestBs.total_assets_crore || latestBs.total_assets_crore === 0) return null;

  const ta = latestBs.total_assets_crore;
  const wc = (latestBs.current_assets_crore ?? 0) - (latestBs.current_liabilities_crore ?? 0);
  const re = (latestBs.shareholders_equity_crore ?? 0) - ((latestBs.shareholders_equity_crore ?? 0) - (latestPl?.net_profit_crore ?? 0)); // retained approximation
  const ebit = latestPl ? (latestPl.ebitda_crore ?? latestPl.net_profit_crore ?? 0) : 0;
  const mve = (val?.market_cap_crore ?? 0);
  const bvd = latestBs.debt_crore ?? 0;
  const sales = latestPl?.sales_crore ?? 0;

  const x1 = wc / ta;
  const x2 = (latestBs.shareholders_equity_crore ?? 0) / ta;  // retained earnings approx
  const x3 = ebit / ta;
  const x4 = bvd > 0 ? mve / bvd : 5;
  const x5 = sales / ta;

  // Manufacturing model (Altman 1968): Z = 1.2X1 + 1.4X2 + 3.3X3 + 0.6X4 + X5
  const z = 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + 1.0 * x5;

  return {
    z: +z.toFixed(2),
    components: [
      { label: "X1 (Working Capital/TA)",    value: +x1.toFixed(3), coeff: 1.2 },
      { label: "X2 (Retained Earnings/TA)",  value: +x2.toFixed(3), coeff: 1.4 },
      { label: "X3 (EBIT/TA)",               value: +x3.toFixed(3), coeff: 3.3 },
      { label: "X4 (Mkt Cap/Book Debt)",     value: +x4.toFixed(3), coeff: 0.6 },
      { label: "X5 (Sales/TA)",              value: +x5.toFixed(3), coeff: 1.0 },
    ],
  };
}

// ── DuPont decomposition ─────────────────────────────────────────────────────

function calcDuPont(
  pl: ProfitLossItem[],
  bs: BalanceSheetItem[],
  ra: FinancialRatios[],
) {
  return pl.slice(0, 5).map(p => {
    const bsRow = bs.find(b => b.period === p.period);
    const raRow = ra.find(r => r.period === p.period);
    const npm = p.sales_crore && p.net_profit_crore != null ? +(p.net_profit_crore / p.sales_crore * 100).toFixed(1) : null;
    const at = raRow?.asset_turnover ?? (bsRow?.total_assets_crore && p.sales_crore ? +(p.sales_crore / bsRow.total_assets_crore).toFixed(2) : null);
    const lev = bsRow && bsRow.shareholders_equity_crore && bsRow.total_assets_crore
      ? +(bsRow.total_assets_crore / bsRow.shareholders_equity_crore).toFixed(2) : null;
    const roe = npm != null && at != null && lev != null ? +(npm / 100 * at * lev * 100).toFixed(1) : (raRow?.roe_pct ?? null);
    return { period: p.period, npm, at, lev, roe };
  }).reverse();
}

// ── Radar data ───────────────────────────────────────────────────────────────

function buildRadar(
  ra: FinancialRatios[],
  pl: ProfitLossItem[],
  val: ValuationSnapshot | null,
  altman: { z: number } | null,
) {
  const latest = ra[0] ?? {};
  const latestPl = pl[0] ?? {};

  function score(val: number | null | undefined, low: number, high: number): number {
    if (val == null) return 50;
    const clamped = Math.max(low, Math.min(high, val));
    return Math.round(((clamped - low) / (high - low)) * 100);
  }

  return [
    { subject: "Profitability",  value: score((latest as any).roce_pct, 0, 30) },
    { subject: "Efficiency",     value: score((latest as any).asset_turnover, 0, 2) },
    { subject: "Leverage",       value: 100 - score((latest as any).debt_to_equity_ratio ?? 0, 0, 3) },
    { subject: "Liquidity",      value: score((latest as any).current_ratio, 0, 3) },
    { subject: "Cash Quality",   value: score((latest as any).interest_coverage, 0, 15) },
    { subject: "Growth",         value: score((latestPl as any).net_profit_crore, 0, 5000) },
    { subject: "Solvency",       value: altman ? score(altman.z, 0, 3) : 50 },
  ];
}

const CustomRadarTooltip = ({ active, payload }: any) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: "var(--surface-strong,#1a1a2e)", border: "1px solid rgba(255,255,255,0.12)", borderRadius: 8, padding: "8px 12px", fontSize: "0.77rem" }}>
      <span style={{ color: "#fff" }}>{payload[0]?.payload?.subject}: </span>
      <span style={{ color: "var(--accent,#7c6aff)", fontWeight: 700 }}>{payload[0]?.value}</span>
    </div>
  );
};

// ── Main component ───────────────────────────────────────────────────────────

export function FundamentalScores({ balanceSheet, cashFlow, profitLoss, ratios, valuation, market }: Props) {
  const [tab, setTab] = useState<ScoreTab>("piotroski");

  const { criteria, score } = calcPiotroski(balanceSheet, cashFlow, profitLoss, ratios);
  const altman = calcAltman(balanceSheet, profitLoss, valuation);
  const dupont = calcDuPont(profitLoss, balanceSheet, ratios);
  const radarData = buildRadar(ratios, profitLoss, valuation, altman);

  const piotroskiColor = score >= 7 ? "#22c55e" : score >= 5 ? "#f59e0b" : "#ef4444";
  const piotroskiLabel = score >= 7 ? "Strong" : score >= 5 ? "Average" : "Weak";

  const zColor = altman ? (altman.z >= 2.99 ? "#22c55e" : altman.z >= 1.81 ? "#f59e0b" : "#ef4444") : "rgba(255,255,255,0.4)";
  const zLabel = altman ? (altman.z >= 2.99 ? "Safe Zone" : altman.z >= 1.81 ? "Grey Zone" : "Distress Zone") : "N/A";

  const TABS: { key: ScoreTab; label: string }[] = [
    { key: "piotroski", label: "Piotroski F" },
    { key: "altman",    label: "Altman Z" },
    { key: "dupont",    label: "DuPont" },
    { key: "radar",     label: "Radar" },
  ];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 8 }}>
        <span style={{ fontSize: "0.78rem", fontWeight: 700, color: "var(--text,#fff)", letterSpacing: "0.04em", textTransform: "uppercase" }}>
          Fundamental Quality Scores
        </span>
        <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
          {TABS.map(t => (
            <button key={t.key} onClick={() => setTab(t.key)} style={{
              fontSize: "0.7rem", padding: "3px 10px", borderRadius: 6, cursor: "pointer",
              background: tab === t.key ? "var(--accent,#7c6aff)" : "rgba(255,255,255,0.07)",
              border: `1px solid ${tab === t.key ? "var(--accent,#7c6aff)" : "rgba(255,255,255,0.12)"}`,
              color: tab === t.key ? "#fff" : "rgba(255,255,255,0.6)",
            }}>
              {t.label}
            </button>
          ))}
        </div>
      </div>

      {/* ── Piotroski ── */}
      {tab === "piotroski" && (
        <div>
          {/* Score display */}
          <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 14 }}>
            <div style={{ textAlign: "center" }}>
              <div style={{ fontSize: "2.5rem", fontWeight: 900, color: piotroskiColor, lineHeight: 1, fontFamily: "monospace" }}>{score}</div>
              <div style={{ fontSize: "0.65rem", color: "rgba(255,255,255,0.5)" }}>/ 9</div>
            </div>
            <div>
              <div style={{ fontWeight: 700, color: piotroskiColor, fontSize: "0.9rem" }}>{piotroskiLabel}</div>
              <div style={{ fontSize: "0.72rem", color: "rgba(255,255,255,0.5)", marginTop: 2 }}>
                {score >= 7 ? "Strong financial health signal" : score >= 5 ? "Average – watch improving signals" : "Weak – multiple concerns flagged"}
              </div>
            </div>
            {/* Score bar */}
            <div style={{ flex: 1, height: 8, background: "rgba(255,255,255,0.08)", borderRadius: 4, overflow: "hidden" }}>
              <div style={{ width: `${(score / 9) * 100}%`, height: "100%", background: piotroskiColor, borderRadius: 4, transition: "width 0.4s ease" }} />
            </div>
          </div>

          {/* Criteria grid */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6 }}>
            {criteria.map((c, i) => (
              <div key={i} style={{
                display: "flex", alignItems: "center", gap: 8, padding: "7px 10px",
                background: c.pass ? "rgba(34,197,94,0.08)" : "rgba(239,68,68,0.06)",
                border: `1px solid ${c.pass ? "rgba(34,197,94,0.2)" : "rgba(239,68,68,0.15)"}`,
                borderRadius: 7,
              }}>
                <span style={{ fontSize: "1rem", flexShrink: 0 }}>{c.pass ? "✅" : "❌"}</span>
                <div>
                  <div style={{ fontSize: "0.72rem", fontWeight: 600, color: "#fff" }}>{c.name}</div>
                  <div style={{ fontSize: "0.65rem", color: "rgba(255,255,255,0.45)" }}>{c.detail}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Altman Z ── */}
      {tab === "altman" && (
        <div>
          {/* Z display */}
          <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 14 }}>
            <div style={{ textAlign: "center" }}>
              <div style={{ fontSize: "2.5rem", fontWeight: 900, color: zColor, lineHeight: 1, fontFamily: "monospace" }}>
                {altman?.z ?? "—"}
              </div>
              <div style={{ fontSize: "0.65rem", color: "rgba(255,255,255,0.5)" }}>Z-Score</div>
            </div>
            <div>
              <div style={{ fontWeight: 700, color: zColor, fontSize: "0.9rem" }}>{zLabel}</div>
              <div style={{ fontSize: "0.72rem", color: "rgba(255,255,255,0.5)", maxWidth: 240, marginTop: 2 }}>
                Z &gt; 2.99 = Safe · 1.81–2.99 = Grey · &lt; 1.81 = Distress
              </div>
            </div>
          </div>

          {altman && (
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              {altman.components.map((c, i) => (
                <div key={i} style={{ display: "flex", alignItems: "center", gap: 10, padding: "6px 10px", background: "rgba(255,255,255,0.03)", borderRadius: 6, border: "1px solid rgba(255,255,255,0.07)" }}>
                  <span style={{ fontSize: "0.65rem", color: "rgba(255,255,255,0.4)", width: 60, flexShrink: 0, fontFamily: "monospace" }}>×{c.coeff}</span>
                  <span style={{ flex: 1, fontSize: "0.73rem", color: "rgba(255,255,255,0.8)" }}>{c.label}</span>
                  <span style={{ fontSize: "0.8rem", fontWeight: 700, color: "var(--teal,#00d2ff)", fontFamily: "monospace" }}>{c.value}</span>
                  <span style={{ fontSize: "0.7rem", color: "rgba(255,255,255,0.35)", fontFamily: "monospace" }}>→ {+(c.value * c.coeff).toFixed(3)}</span>
                </div>
              ))}
              <div style={{ textAlign: "right", fontSize: "0.75rem", fontWeight: 700, color: zColor, paddingTop: 4, fontFamily: "monospace" }}>
                Total Z = {altman.z}
              </div>
            </div>
          )}

          {!altman && (
            <div style={{ textAlign: "center", color: "rgba(255,255,255,0.4)", fontSize: "0.8rem", padding: "20px 0" }}>
              Insufficient data for Altman Z-Score calculation
            </div>
          )}
        </div>
      )}

      {/* ── DuPont ── */}
      {tab === "dupont" && (
        <div>
          <div style={{ fontSize: "0.72rem", color: "rgba(255,255,255,0.5)", marginBottom: 10 }}>
            ROE = Net Profit Margin × Asset Turnover × Leverage
          </div>
          {dupont.length === 0 ? (
            <div style={{ textAlign: "center", color: "rgba(255,255,255,0.4)", fontSize: "0.8rem" }}>No data</div>
          ) : (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.75rem" }}>
                <thead>
                  <tr style={{ borderBottom: "1px solid rgba(255,255,255,0.1)" }}>
                    {["Period", "Net Margin", "Asset Turn.", "Leverage", "→ ROE"].map(h => (
                      <th key={h} style={{ padding: "5px 10px", textAlign: h === "Period" ? "left" : "right", color: "rgba(255,255,255,0.5)", fontWeight: 600, fontSize: "0.68rem", textTransform: "uppercase" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {dupont.map((d, i) => (
                    <tr key={i} style={{ borderBottom: "1px solid rgba(255,255,255,0.05)" }}>
                      <td style={{ padding: "6px 10px", fontWeight: 600, color: "rgba(255,255,255,0.85)" }}>{d.period}</td>
                      <td style={{ padding: "6px 10px", textAlign: "right", fontFamily: "monospace", color: d.npm != null && d.npm > 10 ? "#22c55e" : "rgba(255,255,255,0.7)" }}>{d.npm != null ? `${d.npm}%` : "—"}</td>
                      <td style={{ padding: "6px 10px", textAlign: "right", fontFamily: "monospace", color: "rgba(255,255,255,0.7)" }}>{d.at ?? "—"}x</td>
                      <td style={{ padding: "6px 10px", textAlign: "right", fontFamily: "monospace", color: "rgba(255,255,255,0.7)" }}>{d.lev ?? "—"}x</td>
                      <td style={{ padding: "6px 10px", textAlign: "right", fontFamily: "monospace", fontWeight: 700, color: d.roe != null && d.roe > 15 ? "#22c55e" : d.roe != null && d.roe > 8 ? "#f59e0b" : "#ef4444" }}>
                        {d.roe != null ? `${d.roe}%` : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* ── Radar ── */}
      {tab === "radar" && (
        <div style={{ height: 280 }}>
          <ResponsiveContainer width="100%" height="100%">
            <RadarChart data={radarData}>
              <PolarGrid stroke="rgba(255,255,255,0.1)" />
              <PolarAngleAxis dataKey="subject" tick={{ fontSize: 10, fill: "rgba(255,255,255,0.6)" }} />
              <Radar dataKey="value" stroke="var(--accent,#7c6aff)" fill="var(--accent,#7c6aff)" fillOpacity={0.25} strokeWidth={2} />
              <Tooltip content={<CustomRadarTooltip />} />
            </RadarChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}
