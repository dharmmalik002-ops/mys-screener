import { useEffect, useMemo, useState } from "react";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis, Legend } from "recharts";

import { getMarketEnvironment, type MarketEnvironmentResponse, type MarketEnvDay } from "../lib/api";
import { Panel } from "./Panel";

import "./MarketsPanel.css";

// Focus-list learning: which sectors the user keeps vs removes, persisted so
// suggestions gradually bias toward the kinds of stocks the user actually wants.
const FOCUS_REMOVED_KEY = "stockScanner.marketsFocusRemoved.v1";
const FOCUS_SECTOR_STATS_KEY = "stockScanner.marketsFocusSectorStats.v1";

function readRemoved(): Set<string> {
  try {
    const raw = JSON.parse(window.localStorage.getItem(FOCUS_REMOVED_KEY) ?? "[]");
    return new Set(Array.isArray(raw) ? raw.map(String) : []);
  } catch {
    return new Set();
  }
}
function readSectorStats(): Record<string, { kept: number; removed: number }> {
  try {
    const raw = JSON.parse(window.localStorage.getItem(FOCUS_SECTOR_STATS_KEY) ?? "{}");
    return raw && typeof raw === "object" ? raw : {};
  } catch {
    return {};
  }
}

type FetchState = "loading" | "ready" | "error";

function num(v: number | null | undefined, digits = 0, suffix = ""): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "—";
  return v.toFixed(digits) + suffix;
}

function delta(today: number | null | undefined, prev: number | null | undefined): string | null {
  if (today === null || today === undefined || prev === null || prev === undefined) return null;
  const d = today - prev;
  return `${d >= 0 ? "+" : ""}${d.toFixed(1)}`;
}

function verdictClass(verdict: string | undefined): string {
  switch (verdict) {
    case "Press": return "press";
    case "Selective": return "selective";
    case "Protect": return "protect";
    case "Stand Aside": return "aside";
    default: return "";
  }
}

function Spark({ values }: { values: Array<number | null> }) {
  const points = values.filter((v): v is number => v !== null && Number.isFinite(v));
  if (points.length < 3) return null;
  const min = Math.min(...points);
  const max = Math.max(...points);
  const span = max - min || 1;
  const w = 120;
  const h = 30;
  const step = w / (points.length - 1);
  const path = points.map((v, i) => `${i === 0 ? "M" : "L"}${(i * step).toFixed(1)},${(h - ((v - min) / span) * h).toFixed(1)}`).join(" ");
  return (
    <svg className="mk-spark" viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" aria-hidden>
      <path d={path} fill="none" stroke="currentColor" strokeWidth="1.6" />
    </svg>
  );
}

// One-line interpretation per metric — words, not numbers.
function ftMeaning(pct: number | null): string {
  if (pct === null) return "Not enough recent breakouts to judge.";
  if (pct >= 65) return "Breakouts are being defended — fresh entries are getting paid.";
  if (pct >= 45) return "Mixed follow-through — be selective, demand the best setups.";
  return "Breakouts are being sold into — fresh breakout buys are fighting the tape.";
}

function qualityMeaning(strong: number | null, faded: number | null): string {
  if (strong === null) return "No breakout attempts today.";
  if ((faded ?? 0) > strong) return "Sellers are using strength — most attempts faded off their highs.";
  if (strong >= 55) return "Strong closes — demand is absorbing supply at the highs.";
  return "Average close quality — no edge either way today.";
}

function emaMeaning(above21: number | null, bouncePct: number | null): string {
  if (above21 === null) return "Leader list too small to judge.";
  const holding = above21 >= 75 ? "Leaders are respecting their EMAs" : above21 >= 55 ? "Leaders are slipping toward their EMAs" : "Leaders are losing their EMAs";
  if (bouncePct !== null) {
    return `${holding}; ${bouncePct >= 60 ? "21 EMA tests are being bought" : "21 EMA tests are failing"}.`;
  }
  return holding + ".";
}

function pressureMeaning(share: number | null): string {
  if (share === null) return "No high-volume moves among leaders today.";
  if (share >= 60) return "Institutions are accumulating the leaders.";
  if (share >= 40) return "Balanced — no clear institutional footprint.";
  return "Distribution — leaders are being sold on volume.";
}

import { SymbolGridModal, type SymbolGridItem } from "./SymbolGridModal";

export function MarketsPanel({
  onOpenSymbolChart,
  onOpenChartWithList,
}: {
  onOpenSymbolChart?: (symbol: string) => void;
  onOpenChartWithList?: (symbol: string, symbols: string[]) => void;
}) {
  const [state, setState] = useState<FetchState>("loading");
  const [data, setData] = useState<MarketEnvironmentResponse | null>(null);
  const [removed, setRemoved] = useState<Set<string>>(() => readRemoved());
  const [sectorStats, setSectorStats] = useState<Record<string, { kept: number; removed: number }>>(() => readSectorStats());

  const load = () => {
    setState("loading");
    getMarketEnvironment()
      .then((resp) => {
        setData(resp);
        setState("ready");
      })
      .catch(() => setState("error"));
  };

  useEffect(load, []);

  const bumpSector = (sector: string | undefined, field: "kept" | "removed") => {
    if (!sector) return;
    setSectorStats((prev) => {
      const next = { ...prev, [sector]: { ...(prev[sector] ?? { kept: 0, removed: 0 }) } };
      next[sector][field] += 1;
      try {
        window.localStorage.setItem(FOCUS_SECTOR_STATS_KEY, JSON.stringify(next));
      } catch { /* ignore */ }
      return next;
    });
  };

  const [gridModal, setGridModal] = useState<{ title: string; subtitle?: string; items: SymbolGridItem[] } | null>(null);

  // Open the full app chart, arming ↑/↓ navigation through the given list when
  // the host provides it; falls back to a plain single-symbol open.
  const openChart = (symbol: string, list?: string[]) => {
    if (list && list.length && onOpenChartWithList) onOpenChartWithList(symbol, list);
    else onOpenSymbolChart?.(symbol);
  };

  const removeFocus = (symbol: string, sector?: string) => {
    setRemoved((prev) => {
      const next = new Set(prev);
      next.add(symbol);
      try {
        window.localStorage.setItem(FOCUS_REMOVED_KEY, JSON.stringify([...next]));
      } catch { /* ignore */ }
      return next;
    });
    bumpSector(sector, "removed");
  };

  const restoreFocus = (symbol: string) => {
    setRemoved((prev) => {
      const next = new Set(prev);
      next.delete(symbol);
      try {
        window.localStorage.setItem(FOCUS_REMOVED_KEY, JSON.stringify([...next]));
      } catch { /* ignore */ }
      return next;
    });
  };

  // Learned sector affinity: keep-rate per sector, blended toward neutral until
  // there's enough signal. >0 = user tends to keep this sector, <0 = removes it.
  const sectorAffinity = useMemo(() => {
    const out: Record<string, number> = {};
    for (const [sector, s] of Object.entries(sectorStats)) {
      const total = s.kept + s.removed;
      if (total < 2) continue;
      out[sector] = Math.round(((s.kept - s.removed) / total) * 100);
    }
    return out;
  }, [sectorStats]);

  const today: MarketEnvDay | null = data?.today ?? null;
  const yesterday = data?.yesterday ?? null;

  const compareRows = useMemo(() => {
    if (!today) return [];
    const weekRow = (key: (d: MarketEnvDay) => number | null) => {
      const values = (data?.history ?? [])
        .slice(0, -1)
        .slice(-5)
        .map(() => null); // per-metric weekly averages come from slim history below
      void values;
      return null;
    };
    void weekRow;
    const week = (data?.history ?? []).slice(0, -1).slice(-5);
    const weekAvg = (pick: (r: { structural_held_pct?: number | null; ft3_held_pct: number | null; above_ema21_pct: number | null; score: number | null }) => number | null) => {
      const vals = week.map(pick).filter((v): v is number => v !== null && Number.isFinite(v));
      return vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
    };
    return [
      {
        label: "Base breakouts still above pivot %",
        today: (today.structural ?? {}).held_pct ?? null,
        yesterday: (yesterday?.structural ?? {}).held_pct ?? null,
        week: weekAvg((r) => r.structural_held_pct ?? null),
      },
      {
        label: "Leaders above 21 EMA %",
        today: today.ema_health?.above_ema21_pct ?? null,
        yesterday: yesterday?.ema_health?.above_ema21_pct ?? null,
        week: weekAvg((r) => r.above_ema21_pct),
      },
      {
        label: "Environment score",
        today: today.score,
        yesterday: yesterday?.score ?? null,
        week: weekAvg((r) => r.score),
      },
    ];
  }, [data, today, yesterday]);

  // Learn once per weekly review cycle: names suggested last week that the
  // user did NOT remove are credited to their sectors as "kept".
  useEffect(() => {
    const reviewed = data?.focus_review?.reviewed_date;
    if (!reviewed) return;
    const marker = `stockScanner.marketsFocusLearned.${reviewed}`;
    try {
      if (window.localStorage.getItem(marker)) return;
    } catch { return; }
    const rows = data?.focus_review?.rows ?? [];
    if (!rows.length) return;
    setSectorStats((prev) => {
      const next = { ...prev };
      for (const r of rows) {
        if (!r.sector || removed.has(r.symbol)) continue;
        next[r.sector] = { ...(next[r.sector] ?? { kept: 0, removed: 0 }) };
        next[r.sector].kept += 1;
      }
      try {
        window.localStorage.setItem(FOCUS_SECTOR_STATS_KEY, JSON.stringify(next));
        window.localStorage.setItem(marker, "1");
      } catch { /* ignore */ }
      return next;
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data?.focus_review?.reviewed_date]);

  // Display focus: drop removed, re-rank with learned sector affinity, split
  // into kept vs removed so the user can restore.
  const focusRaw = data?.focus ?? [];
  const focusVisible = useMemo(() => {
    return focusRaw
      .filter((f) => !removed.has(f.symbol))
      .map((f) => ({ ...f, adjScore: f.score + (sectorAffinity[f.sector] ?? 0) * 0.15 }))
      .sort((a, b) => b.adjScore - a.adjScore);
  }, [focusRaw, removed, sectorAffinity]);
  const focusRemovedRows = focusRaw.filter((f) => removed.has(f.symbol));

  if (state === "loading" && !data) {
    return (
      <Panel title="Markets" subtitle="Daily follow-through health of the tape" className="markets-panel">
        <div className="mk-loading">Reading the tape…</div>
      </Panel>
    );
  }
  if (state === "error" && !data) {
    return (
      <Panel title="Markets" subtitle="Daily follow-through health of the tape" className="markets-panel">
        <div className="mk-loading">
          Could not load market environment. <button type="button" onClick={load}>Retry</button>
        </div>
      </Panel>
    );
  }
  if (!today) return null;

  const structural = (today.structural ?? {}) as Record<string, number | null>;
  const ft3 = (today.followthrough?.d3 ?? {}) as Record<string, number | null>;
  const ft1 = (today.followthrough?.d1 ?? {}) as Record<string, number | null>;
  const ft5 = (today.followthrough?.d5 ?? {}) as Record<string, number | null>;
  const quality = today.close_quality ?? {};
  const ema = today.ema_health ?? {};
  const pressure = today.volume_pressure ?? {};
  const expansion = today.range_expansion ?? {};
  const thrust = today.thrust ?? {};
  const scoreDelta = delta(today.score, yesterday?.score ?? null);
  const ai = data?.ai ?? null;
  const week = data?.week_review;

  return (
    <Panel
      title="Markets"
      subtitle={`Follow-through health · ${data?.date ?? ""} · ${today.universe} liquid stocks measured`}
      className="markets-panel"
    >
      {/* Verdict header */}
      <div className="mk-header">
        <div className={`mk-score ${verdictClass(today.verdict)}`}>
          <strong>{num(today.score, 1)}</strong>
          <span className="mk-verdict">{today.verdict}</span>
        </div>
        <div className="mk-header-context">
          <div>
            {scoreDelta ? (
              <span className={Number(scoreDelta) >= 0 ? "pos" : "neg"}>{scoreDelta} vs yesterday</span>
            ) : (
              <span className="mk-muted">first recorded session</span>
            )}
            {data?.week_avg_score !== null && data?.week_avg_score !== undefined ? (
              <span className="mk-muted"> · last-week avg {num(data.week_avg_score, 1)}</span>
            ) : null}
          </div>
          <Spark values={(data?.history ?? []).map((h) => h.score)} />
        </div>
        {ai?.one_rule_today ? <div className="mk-rule">Rule today: {ai.one_rule_today}</div> : null}
      </div>

      {/* Market posture strip */}
      {data?.posture ? (
        <div className="mk-posture">
          <div className="mk-posture-item">
            <span>Adv / Dec</span>
            <strong><em className="pos">{data.posture.advances}</em> / <em className="neg">{data.posture.declines}</em></strong>
          </div>
          <div className="mk-posture-item">
            <span>52w High / Low today</span>
            <strong><em className="pos">{data.posture.new_52w_highs}</em> / <em className="neg">{data.posture.new_52w_lows}</em></strong>
          </div>
          <div className="mk-posture-item">
            <span>&gt; 21 EMA</span>
            <strong>{num(data.posture.above_ema21_pct, 0, "%")}</strong>
          </div>
          <div className="mk-posture-item">
            <span>&gt; 50 SMA</span>
            <strong>{num(data.posture.above_sma50_pct, 0, "%")}</strong>
          </div>
          <div className="mk-posture-item">
            <span>&gt; 200 SMA</span>
            <strong>{num(data.posture.above_sma200_pct, 0, "%")}</strong>
          </div>
          <div className="mk-posture-note">
            52w/MA stats measured on {data.posture.leveled_universe ?? data.posture.universe} stocks with
            verified-fresh levels; stale-history names are excluded, not guessed.
          </div>
        </div>
      ) : null}

      {/* Breadth trend — is participation improving? */}
      {(() => {
        const series = (data?.history ?? [])
          .filter((h) => h.date)
          .map((h) => ({
            date: (h.date ?? "").slice(5),
            "> 21 EMA": h.above_ema21_pct ?? null,
            "> 50 SMA": h.above_sma50_pct ?? null,
            "> 200 SMA": h.above_sma200_pct ?? null,
          }));
        if (series.length < 2) {
          return (
            <div className="mk-breadth">
              <div className="mk-week-hdr">Breadth Trend</div>
              <div className="mk-muted">Building — the multi-day breadth chart needs a few sessions of history. Today's snapshot is in the posture strip above.</div>
            </div>
          );
        }
        return (
          <div className="mk-breadth">
            <div className="mk-week-hdr">Breadth Trend — % of stocks above key moving averages</div>
            <div className="mk-breadth-chart">
              <ResponsiveContainer width="100%" height={220}>
                <LineChart data={series} margin={{ top: 8, right: 12, bottom: 4, left: -18 }}>
                  <CartesianGrid stroke="var(--line)" strokeDasharray="3 3" vertical={false} />
                  <XAxis dataKey="date" tick={{ fontSize: 11, fill: "var(--text-muted)" }} />
                  <YAxis domain={[0, 100]} tick={{ fontSize: 11, fill: "var(--text-muted)" }} />
                  <Tooltip contentStyle={{ fontSize: 12 }} />
                  <Legend wrapperStyle={{ fontSize: 12 }} />
                  <Line type="monotone" dataKey="> 21 EMA" stroke="#00d2ff" strokeWidth={2} dot={false} connectNulls />
                  <Line type="monotone" dataKey="> 50 SMA" stroke="#f7b955" strokeWidth={2} dot={false} connectNulls />
                  <Line type="monotone" dataKey="> 200 SMA" stroke="#089981" strokeWidth={2} dot={false} connectNulls />
                </LineChart>
              </ResponsiveContainer>
            </div>
            <div className="mk-footnote">
              Rising lines = broadening participation (healthy); falling while the index holds = a narrowing,
              distribution-prone tape. The 200 SMA line is the slow, structural one; the 21 EMA line is the fast swing gauge.
            </div>
          </div>
        );
      })()}

      {/* AI daily read */}
      {ai ? (
        <div className="mk-ai">
          {ai.headline ? <div className="mk-ai-headline">{ai.headline}</div> : null}
          {(ai.narrative ?? []).map((p, i) => (
            <p key={i}>{p}</p>
          ))}
        </div>
      ) : (
        <div className="mk-ai mk-muted">AI read unavailable right now — the counted metrics below stand on their own.</div>
      )}

      {/* Today vs yesterday vs week */}
      <div className="mk-compare">
        <div className="mk-compare-head">
          <span>Metric</span><span>Today</span><span>Yesterday</span><span>Week avg</span>
        </div>
        {compareRows.map((row) => (
          <div key={row.label} className="mk-compare-row">
            <span>{row.label}</span>
            <strong>{num(row.today, 1)}</strong>
            <span>{num(row.yesterday, 1)}</span>
            <span>{row.week !== null ? num(row.week, 1) : "—"}</span>
          </div>
        ))}
      </div>

      {/* Component cards */}
      <div className="mk-grid">
        <div className="mk-card">
          <div className="mk-card-hdr">Base Breakout Follow-Through</div>
          <div className="mk-big">{num(structural.held_pct ?? null, 0, "%")}<small> of {structural.events ?? 0} base breakouts (last ~12 sessions) still above pivot</small></div>
          <div className="mk-sub">
            back inside base: {num(structural.back_in_base_pct ?? null, 0, "%")} · short-term clears held (1d/3d/5d): {num(ft1.held_pct, 0, "%")} / {num(ft3.held_pct, 0, "%")} / {num(ft5.held_pct, 0, "%")}
          </div>
          <div className="mk-meaning">{ftMeaning(structural.held_pct ?? null)}</div>
          <Spark values={(data?.history ?? []).map((h) => h.structural_held_pct ?? null)} />
        </div>
        <div className="mk-card">
          <div className="mk-card-hdr">Today's Breakout Quality</div>
          <div className="mk-big">{num(quality.strong_pct, 0, "%")}<small> strong closes of {quality.count ?? 0} attempts</small></div>
          <div className="mk-sub">faded below midpoint: {num(quality.faded_pct, 0, "%")}</div>
          <div className="mk-meaning">{qualityMeaning(quality.strong_pct ?? null, quality.faded_pct ?? null)}</div>
        </div>
        <div className="mk-card">
          <div className="mk-card-hdr">Leader EMA Health</div>
          <div className="mk-big">{num(ema.above_ema21_pct, 0, "%")}<small> of {ema.leaders ?? 0} leaders above 21 EMA</small></div>
          <div className="mk-sub">
            above 10 EMA: {num(ema.above_ema10_pct, 0, "%")} · 21 EMA tests bought: {num(ema.ema21_bounce_pct, 0, "%")} of {ema.ema21_touches ?? 0}
          </div>
          <div className="mk-meaning">{emaMeaning(ema.above_ema21_pct ?? null, ema.ema21_bounce_pct ?? null)}</div>
          <Spark values={(data?.history ?? []).map((h) => h.above_ema21_pct)} />
        </div>
        <div className="mk-card">
          <div className="mk-card-hdr">Leader Volume Pressure</div>
          <div className="mk-big">
            {num(pressure.accumulation_share_pct, 0, "%")}
            <small> accumulation share ({pressure.accumulation ?? 0} up / {pressure.distribution ?? 0} down on volume)</small>
          </div>
          <div className="mk-meaning">{pressureMeaning(pressure.accumulation_share_pct ?? null)}</div>
        </div>
        <div className="mk-card">
          <div className="mk-card-hdr">Range Expansion Direction</div>
          <div className="mk-big">{num(expansion.up_share_pct, 0, "%")}<small> of wide-range days closed up ({expansion.up ?? 0} vs {expansion.down ?? 0})</small></div>
          <div className="mk-meaning">
            {expansion.up_share_pct === null ? "No unusually wide days today." : (expansion.up_share_pct ?? 0) >= 60 ? "The big candles belong to buyers." : (expansion.up_share_pct ?? 0) <= 40 ? "The big candles belong to sellers." : "Big-range days are split — no side in control."}
          </div>
        </div>
        <div className="mk-card">
          <div className="mk-card-hdr">Thrust &amp; Tape</div>
          <div className="mk-big">
            {thrust.up_4pct ?? 0} <small>up 4%+</small> / {thrust.down_4pct ?? 0} <small>down 4%+</small>
          </div>
          <div className="mk-sub">
            up/down volume {num(thrust.updown_volume_ratio, 2, "x")} · fresh 20d highs {thrust.fresh_20d_highs ?? 0} vs lows {thrust.fresh_20d_lows ?? 0}
          </div>
          <div className="mk-meaning">
            {(thrust.up_4pct ?? 0) >= (thrust.down_4pct ?? 0) * 2 ? "Momentum aggression is one-sided to the upside." : (thrust.down_4pct ?? 0) >= (thrust.up_4pct ?? 0) * 2 ? "Downside aggression dominates — momentum longs are swimming upstream." : "Two-way tape — aggression is balanced."}
          </div>
        </div>
      </div>

      {/* Open positions health */}
      {(data?.positions ?? []).length ? (
        <div className="mk-week">
          <div className="mk-week-hdr">Your Open Positions — health check</div>
          <div className="mk-pos-list">
            {(data?.positions ?? []).map((p) => (
              <div key={p.symbol + String(p.avg_px)} className={`mk-pos-row cat-${p.category.toLowerCase().replace(/[^a-z]+/g, "-")}`}>
                <button type="button" className="mk-symbol" onClick={() => p.mapped && onOpenSymbolChart?.(p.symbol)}>
                  {p.symbol}
                </button>
                <span className="mk-pos-cat">{p.category}</span>
                {p.pnl_pct !== null && p.pnl_pct !== undefined ? (
                  <strong className={p.pnl_pct >= 0 ? "pos" : "neg"}>
                    {p.pnl_pct >= 0 ? "+" : ""}{p.pnl_pct.toFixed(1)}%
                  </strong>
                ) : <strong>—</strong>}
                <small>
                  {p.mapped ? `avg ${p.avg_px} → ${p.last_price}` : `avg ${p.avg_px}`}
                  {p.rs_rating ? ` · RS ${p.rs_rating}` : ""}
                </small>
                <div className="mk-pos-advice">{p.advice}</div>
              </div>
            ))}
          </div>
          <div className="mk-footnote">
            Positions are netted from your journal's buy/sell entries and re-classified daily against the same
            rules as the market metrics. Worst conditions listed first.
          </div>
        </div>
      ) : (
        <div className="mk-week">
          <div className="mk-week-hdr">Your Open Positions</div>
          <div className="mk-muted">
            No open positions synced yet — open the Journal page once (it syncs your positions to the backend), then revisit.
          </div>
        </div>
      )}

      {/* Leaders + sector-breakout cards */}
      <div className="mk-cardrow">
        {(data?.leaders ?? []).length ? (
          <div className="mk-bigcard">
            <div className="mk-bigcard-num">{data?.leaders?.length ?? 0}</div>
            <div className="mk-bigcard-label">Market Leaders</div>
            <div className="mk-bigcard-sub">
              {(data?.leaders ?? []).filter((l) => l.above_ema21).length} above their 21 EMA · 2%/5% circuit-band names excluded
            </div>
            <div className="mk-bigcard-actions">
              <button
                type="button"
                onClick={() => {
                  const syms = (data?.leaders ?? []).map((l) => l.symbol);
                  openChart(syms[0], syms);
                }}
              >
                Full chart (↑/↓ steps all)
              </button>
              <button
                type="button"
                onClick={() =>
                  setGridModal({
                    title: "Market Leaders",
                    subtitle: `${data?.leaders?.length ?? 0} Stage-2 leaders · click any chart to open it full`,
                    items: (data?.leaders ?? []).map((l) => ({
                      symbol: l.symbol,
                      name: l.name,
                      badge: l.rs_rating ? `RS ${l.rs_rating}` : undefined,
                      badgeTone: "pos",
                      note: `${l.above_ema21 ? "above" : "below"} 21 EMA · ${l.pct_from_52w_high.toFixed(1)}% off high`,
                    })),
                  })
                }
              >
                ⊞ Grid view
              </button>
            </div>
          </div>
        ) : null}
        {(data?.sector_breakouts ?? []).length ? (
          <div className="mk-bigcard">
            <div className="mk-bigcard-num">{data?.sector_breakouts?.length ?? 0}</div>
            <div className="mk-bigcard-label">Sector Breakouts Setting Up</div>
            <div className="mk-bigcard-sub">Leading-sector names 0–5% under a pivot — the next to fire</div>
            <div className="mk-bigcard-actions">
              <button
                type="button"
                onClick={() => {
                  const syms = (data?.sector_breakouts ?? []).map((b) => b.symbol);
                  openChart(syms[0], syms);
                }}
              >
                Full chart (↑/↓ steps all)
              </button>
              <button
                type="button"
                onClick={() =>
                  setGridModal({
                    title: "Leading-sector breakouts, about to fire",
                    subtitle: "Names in the strongest sectors coiled 0–5% under a base pivot",
                    items: (data?.sector_breakouts ?? []).map((b) => ({
                      symbol: b.symbol,
                      name: b.name,
                      badge: `${b.pct_below_pivot.toFixed(1)}% to pivot`,
                      badgeTone: "muted",
                      note: `${b.sector} · pivot ${b.pivot}`,
                    })),
                  })
                }
              >
                ⊞ Grid view
              </button>
            </div>
          </div>
        ) : null}
      </div>

      {/* Focus list — 40+ names with a buy plan, removable, learns your taste */}
      {focusRaw.length ? (
        <div className="mk-week">
          <div className="mk-week-hdr">Focus for the coming week — {focusVisible.length} names, each with a plan</div>
          {Object.keys(sectorAffinity).length ? (
            <div className="mk-affinity">
              Learned from your edits:
              {Object.entries(sectorAffinity)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 6)
                .map(([sector, v]) => (
                  <span key={sector} className={`mk-tag ${v >= 0 ? "pos-tag" : "neg-tag"}`}>
                    {sector} {v >= 0 ? "↑" : "↓"}
                  </span>
                ))}
            </div>
          ) : null}
          <div className="mk-focus-grid">
            {focusVisible.map((f) => (
              <div key={f.symbol} className="mk-focus-card">
                <div className="mk-focus-top">
                  <button
                    type="button"
                    className="mk-symbol"
                    onClick={() => openChart(f.symbol, focusVisible.map((x) => x.symbol))}
                  >
                    {f.symbol}
                  </button>
                  <span className={f.change_pct >= 0 ? "pos" : "neg"}>
                    {f.change_pct >= 0 ? "+" : ""}{f.change_pct.toFixed(1)}%
                  </span>
                  <button
                    type="button"
                    className="mk-focus-remove"
                    aria-label={`Remove ${f.symbol}`}
                    title="Remove — the page learns your preference"
                    onClick={() => removeFocus(f.symbol, f.sector)}
                  >
                    ×
                  </button>
                </div>
                <div className="mk-focus-setup">
                  {f.setup ?? "Setup"} · <span className="mk-muted">{f.sector}</span>
                </div>
                {f.entry ? (
                  <div className="mk-focus-plan">
                    <div><em>Buy:</em> {f.entry}</div>
                    <div><em>Stop:</em> {f.stop}</div>
                    {f.buy_note ? <div className="mk-muted">{f.buy_note}</div> : null}
                  </div>
                ) : null}
                <div className="mk-focus-tags">
                  {f.reasons.map((r) => <span key={r} className="mk-tag">{r}</span>)}
                </div>
              </div>
            ))}
          </div>
          {focusRemovedRows.length ? (
            <div className="mk-removed">
              Removed ({focusRemovedRows.length}):
              {focusRemovedRows.map((f) => (
                <button key={f.symbol} type="button" className="mk-tag mk-restore" onClick={() => restoreFocus(f.symbol)}>
                  {f.symbol} ↺
                </button>
              ))}
            </div>
          ) : null}
          <div className="mk-footnote">
            Selection: RS ≥ 72–80, above a stacked 50/200 SMA, within 18% of the 52-week high, liquid. Each card shows
            the setup and a concrete plan — a watch list, not a buy list. Remove any you don't want; the page learns
            which sectors you keep and re-ranks future lists toward them.
          </div>
        </div>
      ) : null}

      {/* Weekly focus review — did last week's picks do what we thought? */}
      {data?.focus_review?.summary ? (
        <div className="mk-week">
          <div className="mk-week-hdr">
            Focus scorecard — the list from {data.focus_review.reviewed_date}, graded
          </div>
          <div className="mk-review-summary">
            <strong className={data.focus_review.summary.avg_return_pct >= 0 ? "pos" : "neg"}>
              {data.focus_review.summary.avg_return_pct >= 0 ? "+" : ""}
              {data.focus_review.summary.avg_return_pct.toFixed(1)}% avg
            </strong>
            <span>{data.focus_review.summary.worked}/{data.focus_review.summary.count} behaved as expected (≥3%) · {data.focus_review.summary.hit_rate_pct.toFixed(0)}% hit rate</span>
          </div>
          <div className="mk-review-grid">
            {data.focus_review.rows.slice(0, 20).map((r) => (
              <div key={r.symbol} className={`mk-review-row ${r.worked ? "won" : "lost"}`}>
                <button type="button" className="mk-symbol" onClick={() => onOpenSymbolChart?.(r.symbol)}>{r.symbol}</button>
                <strong className={r.return_pct >= 0 ? "pos" : "neg"}>{r.return_pct >= 0 ? "+" : ""}{r.return_pct.toFixed(1)}%</strong>
                <small>{r.setup}</small>
              </div>
            ))}
          </div>
          <div className="mk-footnote">
            Return since the suggestion day, unmanaged. "Behaved as expected" = a tradable follow-through of +3% or
            more — the goal is that the setups fire, not that every one is green.
          </div>
        </div>
      ) : null}

      {/* Named evidence */}
      <div className="mk-week">
        <div className="mk-week-hdr">The Evidence — names, not claims</div>
        <div className="mk-week-grid">
          <div>
            <div className="mk-week-sub pos-hdr">Breakouts working ({(data?.evidence?.breakouts_working ?? []).length})</div>
            {(data?.evidence?.breakouts_working ?? []).map((e) => (
              <div key={e.symbol} className="mk-week-row">
                <button type="button" className="mk-symbol" onClick={() => onOpenSymbolChart?.(e.symbol)}>{e.symbol}</button>
                <strong className="pos">+{e.pct_vs_pivot.toFixed(1)}%</strong>
                <small>vs pivot {e.pivot} · broke {e.sessions_ago}s ago · base {e.base_len_label}</small>
              </div>
            ))}
            {(data?.evidence?.breakouts_working ?? []).length === 0 ? <div className="mk-muted">None in the last ~12 sessions.</div> : null}
          </div>
          <div>
            <div className="mk-week-sub neg-hdr">Back inside the base ({(data?.evidence?.breakouts_failed ?? []).length})</div>
            {(data?.evidence?.breakouts_failed ?? []).map((e) => (
              <div key={e.symbol} className="mk-week-row">
                <button type="button" className="mk-symbol" onClick={() => onOpenSymbolChart?.(e.symbol)}>{e.symbol}</button>
                <strong className="neg">{e.pct_vs_pivot.toFixed(1)}%</strong>
                <small>vs pivot {e.pivot} · broke {e.sessions_ago}s ago · base {e.base_len_label}</small>
              </div>
            ))}
            {(data?.evidence?.breakouts_failed ?? []).length === 0 ? <div className="mk-muted">None — breakouts are holding.</div> : null}
          </div>
        </div>
        <div className="mk-week-grid mk-ema-tests">
          <div>
            <div className="mk-week-sub pos-hdr">21 EMA tests bought</div>
            <div className="mk-chip-row">
              {(data?.evidence?.ema_tests?.bounced ?? []).map((e) => (
                <button key={e.symbol} type="button" className="mk-chip pos-chip" onClick={() => onOpenSymbolChart?.(e.symbol)}>
                  {e.symbol} <small>+{e.pct_vs_ema21.toFixed(1)}%</small>
                </button>
              ))}
            </div>
          </div>
          <div>
            <div className="mk-week-sub neg-hdr">21 EMA tests failed</div>
            <div className="mk-chip-row">
              {(data?.evidence?.ema_tests?.sliced ?? []).map((e) => (
                <button key={e.symbol} type="button" className="mk-chip neg-chip" onClick={() => onOpenSymbolChart?.(e.symbol)}>
                  {e.symbol} <small>{e.pct_vs_ema21.toFixed(1)}%</small>
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Last week review */}
      <div className="mk-week">
        <div className="mk-week-hdr">Last Week — what worked, what didn't</div>
        <div className="mk-week-grid">
          <div>
            <div className="mk-week-sub pos-hdr">Worked</div>
            {ai?.what_worked?.length ? (
              <ul>{ai.what_worked.map((w, i) => <li key={i}>{w}</li>)}</ul>
            ) : null}
            {(week?.top_sectors ?? []).map((s) => (
              <div key={s.sector} className="mk-week-row">
                <span>{s.sector}</span>
                <strong className={s.median_return_5d_pct >= 0 ? "pos" : "neg"}>
                  {s.median_return_5d_pct >= 0 ? "+" : ""}{s.median_return_5d_pct.toFixed(1)}%
                </strong>
                <small>sector median 5d</small>
              </div>
            ))}
          </div>
          <div>
            <div className="mk-week-sub neg-hdr">Didn't</div>
            {ai?.what_didnt?.length ? (
              <ul>{ai.what_didnt.map((w, i) => <li key={i}>{w}</li>)}</ul>
            ) : null}
            {(week?.bottom_sectors ?? []).map((s) => (
              <div key={s.sector} className="mk-week-row">
                <span>{s.sector}</span>
                <strong className={s.median_return_5d_pct >= 0 ? "pos" : "neg"}>
                  {s.median_return_5d_pct >= 0 ? "+" : ""}{s.median_return_5d_pct.toFixed(1)}%
                </strong>
                <small>sector median 5d</small>
              </div>
            ))}
          </div>
        </div>
        <div className="mk-footnote">
          Sector rows are the median 5-day return across each sector's liquid stocks. Day-vs-day and week
          comparisons deepen automatically as daily history accumulates.
        </div>
      </div>

      {gridModal ? (
        <SymbolGridModal
          title={gridModal.title}
          subtitle={gridModal.subtitle}
          items={gridModal.items}
          market="india"
          onOpenSymbolChart={(sym) => {
            setGridModal(null);
            onOpenSymbolChart?.(sym);
          }}
          onClose={() => setGridModal(null)}
        />
      ) : null}
    </Panel>
  );
}
