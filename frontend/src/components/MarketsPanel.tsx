import { useEffect, useMemo, useState } from "react";

import { getMarketEnvironment, type MarketEnvironmentResponse, type MarketEnvDay } from "../lib/api";
import { Panel } from "./Panel";

import "./MarketsPanel.css";

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

export function MarketsPanel() {
  const [state, setState] = useState<FetchState>("loading");
  const [data, setData] = useState<MarketEnvironmentResponse | null>(null);

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
    const weekAvg = (pick: (r: { ft3_held_pct: number | null; above_ema21_pct: number | null; score: number | null }) => number | null) => {
      const vals = week.map(pick).filter((v): v is number => v !== null && Number.isFinite(v));
      return vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
    };
    return [
      {
        label: "Breakout follow-through (3d held %)",
        today: (today.followthrough?.d3 ?? {}).held_pct ?? null,
        yesterday: (yesterday?.followthrough?.d3 ?? {}).held_pct ?? null,
        week: weekAvg((r) => r.ft3_held_pct),
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
          <div className="mk-card-hdr">Breakout Follow-Through</div>
          <div className="mk-big">{num(ft3.held_pct, 0, "%")}<small> of 3-day-old breakouts held ({ft3.count ?? 0})</small></div>
          <div className="mk-sub">1d: {num(ft1.held_pct, 0, "%")} ({ft1.count ?? 0}) · 5d: {num(ft5.held_pct, 0, "%")} ({ft5.count ?? 0})</div>
          <div className="mk-meaning">{ftMeaning(ft3.held_pct ?? null)}</div>
          <Spark values={(data?.history ?? []).map((h) => h.ft3_held_pct)} />
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

      {/* Last week review */}
      <div className="mk-week">
        <div className="mk-week-hdr">Last Week — what worked, what didn't</div>
        <div className="mk-week-grid">
          <div>
            <div className="mk-week-sub pos-hdr">Worked</div>
            {ai?.what_worked?.length ? (
              <ul>{ai.what_worked.map((w, i) => <li key={i}>{w}</li>)}</ul>
            ) : null}
            {(week?.scanners ?? []).filter((s) => s.avg_return_pct > 0).slice(0, 4).map((s) => (
              <div key={s.scan_id} className="mk-week-row">
                <span>{s.name}</span>
                <strong className="pos">+{s.avg_return_pct.toFixed(1)}%</strong>
                <small>{s.win_rate_pct.toFixed(0)}% win · {s.picks} picks</small>
              </div>
            ))}
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
            {(week?.scanners ?? []).filter((s) => s.avg_return_pct <= 0).slice(-4).map((s) => (
              <div key={s.scan_id} className="mk-week-row">
                <span>{s.name}</span>
                <strong className="neg">{s.avg_return_pct.toFixed(1)}%</strong>
                <small>{s.win_rate_pct.toFixed(0)}% win · {s.picks} picks</small>
              </div>
            ))}
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
          Scanner rows measure the forward return of every pick recorded in the last 5 sessions, unmanaged since its
          scan day. History deepens daily — the first days after launch will show fewer comparisons.
        </div>
      </div>
    </Panel>
  );
}
