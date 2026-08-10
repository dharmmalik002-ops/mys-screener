import { useCallback, useEffect, useRef, useState } from "react";
import { Activity, AlertTriangle, RefreshCw, TrendingDown, TrendingUp } from "lucide-react";
import {
  getMarketRegimeAnalysis,
  type MarketKey,
  type RegimeAnalysis,
  type RegimeDelta,
} from "../lib/api";
import "./RegimeBrief.css";

type Props = { market: MarketKey };

const STANCE_ICON = {
  constructive: TrendingUp,
  mixed: Activity,
  defensive: TrendingDown,
} as const;

function pct(value: number | null | undefined, digits = 1): string {
  return value === null || value === undefined ? "—" : `${value.toFixed(digits)}%`;
}

function delta(value: RegimeDelta | null | undefined): { text: string; tone: string } | null {
  if (!value || value.direction === "unchanged") return null;
  const up = value.direction === "improved";
  return {
    text: `${up ? "+" : "−"}${value.magnitude.toFixed(1)} pts vs last week`,
    tone: up ? "pos" : "neg",
  };
}

/**
 * Market regime brief.
 *
 * The narrative and the table are shown together on purpose: the prose is
 * written by a model, so every figure it can mention is also rendered as data
 * right beside it. A reader who distrusts the paragraph can check it without
 * leaving the page.
 */
export function RegimeBrief({ market }: Props) {
  const [data, setData] = useState<RegimeAnalysis | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  // Monotonic request id: a slow first load must not overwrite a newer refresh,
  // and switching market mid-flight must not resurrect the old market's brief.
  const requestId = useRef(0);

  const load = useCallback(
    async (marketKey: MarketKey, refresh: boolean) => {
      const id = ++requestId.current;
      if (refresh) setRefreshing(true);
      else setLoading(true);
      try {
        const result = await getMarketRegimeAnalysis(marketKey, refresh);
        if (id !== requestId.current) return;
        setData(result);
        setError(null);
      } catch (err) {
        if (id !== requestId.current) return;
        setError(err instanceof Error ? err.message : "Could not load the regime brief.");
      } finally {
        if (id === requestId.current) {
          setLoading(false);
          setRefreshing(false);
        }
      }
    },
    [],
  );

  useEffect(() => {
    void load(market, false);
  }, [market, load]);

  if (loading) {
    return (
      <section className="rb" aria-busy="true">
        <div className="rb-skeleton" role="status" aria-label="Loading market regime brief">
          <span /><span /><span />
        </div>
      </section>
    );
  }

  if (error) {
    return (
      <section className="rb rb-empty">
        <AlertTriangle size={16} aria-hidden />
        <p>{error}</p>
      </section>
    );
  }

  if (!data?.available || !data.facts || !data.brief) {
    return (
      <section className="rb rb-empty">
        <AlertTriangle size={16} aria-hidden />
        <p>{data?.reason ?? "Breakout statistics are not available yet."}</p>
      </section>
    );
  }

  const { facts, brief, rules } = data;
  const week = facts.this_week;
  const StanceIcon = STANCE_ICON[brief.stance];
  const winDelta = delta(facts.vs_prior_week.win_rate);
  const moveDelta = delta(facts.vs_prior_week.median_max_move_pct);

  // A week whose signals are mostly unresolved cannot carry a confident read.
  // Fall back to resolved+open when the total is missing, so the banner never
  // renders a null count.
  const totalSignals =
    week.signals ?? ((week.resolved ?? 0) + (week.still_open ?? 0) || null);
  const openShare =
    totalSignals && week.still_open ? week.still_open / totalSignals : 0;
  const provisional = openShare > 0.35;

  const stop = typeof rules?.stop_pct === "number" ? rules.stop_pct : null;
  const target = typeof rules?.win_pct === "number" ? rules.win_pct : null;
  const horizon = typeof rules?.horizon_sessions === "number" ? rules.horizon_sessions : null;
  const bigMove = typeof rules?.big_move_pct === "number" ? rules.big_move_pct : null;

  const cohortRows = [
    { key: "ipo", label: "Recent IPOs" },
    { key: "leading_groups", label: "Top-decile groups" },
    { key: "lagging_groups", label: "Everything else" },
  ];

  return (
    <section className="rb" aria-label="Market breakout conditions">
      <header className="rb-head">
        <div className={`rb-stance rb-${brief.stance}`}>
          <StanceIcon size={14} strokeWidth={2.2} aria-hidden />
          <span>{brief.stance}</span>
        </div>
        <h3 className="rb-headline">{brief.headline}</h3>
        <button
          type="button"
          className="rb-refresh"
          onClick={() => void load(market, true)}
          disabled={refreshing}
          aria-label="Regenerate the brief"
          title="Regenerate the brief"
        >
          <RefreshCw size={13} className={refreshing ? "rb-spin" : undefined} aria-hidden />
        </button>
      </header>

      {provisional ? (
        <p className="rb-provisional">
          {week.still_open} of {totalSignals} signals this week have not resolved yet — this read is
          provisional.
        </p>
      ) : null}

      <div className="rb-kpis">
        <div className="rb-kpi">
          <span className="rb-kpi-label">Win rate</span>
          <strong>{pct(week.win_rate)}</strong>
          {winDelta ? <em className={winDelta.tone}>{winDelta.text}</em> : <em className="rb-flat">vs last week</em>}
        </div>
        <div className="rb-kpi">
          <span className="rb-kpi-label">Median best move</span>
          <strong>{pct(week.median_max_move_pct)}</strong>
          {moveDelta ? <em className={moveDelta.tone}>{moveDelta.text}</em> : <em className="rb-flat">vs last week</em>}
        </div>
        <div className="rb-kpi">
          <span className="rb-kpi-label">Median hold</span>
          <strong>{week.median_sessions_held ?? "—"}</strong>
          <em className="rb-flat">sessions to resolve</em>
        </div>
        <div className="rb-kpi">
          <span className="rb-kpi-label">Closed strong</span>
          <strong>{pct(week.pct_closed_near_high)}</strong>
          <em className="rb-flat">top quartile of range</em>
        </div>
        <div className="rb-kpi">
          <span className="rb-kpi-label">Reached {bigMove ?? 10}%</span>
          <strong>{pct(week.pct_reached_big_move)}</strong>
          <em className="rb-flat">of resolved signals</em>
        </div>
        <div className="rb-kpi">
          <span className="rb-kpi-label">…and held it</span>
          <strong>{pct(week.pct_of_big_movers_that_closed_near_high)}</strong>
          <em className="rb-flat">of them closed strong</em>
        </div>
      </div>

      {facts.expectancy ? (
        <div className={`rb-edge ${facts.expectancy.clears_breakeven ? "rb-edge-ok" : "rb-edge-short"}`}>
          <span className="rb-edge-label">The arithmetic</span>
          <span>
            {facts.expectancy.wins_per_100_trades} wins and{" "}
            {facts.expectancy.losses_per_100_trades} losses per 100 trades. Break-even needs{" "}
            <strong>{pct(facts.expectancy.breakeven_win_rate)}</strong>; you are at{" "}
            <strong>{pct(facts.expectancy.observed_win_rate)}</strong>
            {facts.expectancy.clears_breakeven
              ? " — clearing it."
              : ` — ${facts.expectancy.shortfall_pts.toFixed(1)} points short.`}{" "}
            Expected <strong>{facts.expectancy.expected_pct_per_trade.toFixed(2)}%</strong> per trade.
          </span>
          <span className="rb-edge-caveat">{facts.expectancy.assumption}</span>
        </div>
      ) : null}

      <div className="rb-narrative">
        {brief.narrative.split("\n\n").map((para, i) => (
          <p key={i}>{para}</p>
        ))}
      </div>

      <div className="rb-tables">
        <div className="rb-table-wrap">
          <h4>
            Setups this week
            <span className="rb-note">
              {facts.setups_below_sample_threshold > 0
                ? `${facts.setups_below_sample_threshold} setup${facts.setups_below_sample_threshold === 1 ? "" : "s"} hidden — fewer than ${facts.min_sample_for_setup} signals`
                : `minimum ${facts.min_sample_for_setup} signals to appear`}
            </span>
          </h4>
          <div className="rb-scroll">
            <table className="rb-table">
              <thead>
                <tr>
                  <th scope="col">Setup</th>
                  <th scope="col" className="num">n</th>
                  <th scope="col" className="num">Win</th>
                  <th scope="col" className="num">Med. best</th>
                  <th scope="col" className="num">Hold</th>
                  <th scope="col" className="num">Closed strong</th>
                </tr>
              </thead>
              <tbody>
                {facts.setups.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="rb-none">
                      No setup reached {facts.min_sample_for_setup} signals this week.
                    </td>
                  </tr>
                ) : (
                  facts.setups.map((row) => (
                    <tr key={row.label}>
                      <td>{row.label}</td>
                      <td className="num">{row.signals}</td>
                      <td className={`num ${row.win_rate >= 50 ? "pos" : "neg"}`}>{pct(row.win_rate)}</td>
                      <td className="num">{pct(row.median_max_move_pct)}</td>
                      <td className="num">{row.median_sessions_held}</td>
                      <td className="num">{pct(row.pct_closed_near_high)}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div className="rb-table-wrap">
          <h4>Cohorts</h4>
          <div className="rb-scroll">
            <table className="rb-table">
              <thead>
                <tr>
                  <th scope="col">Cohort</th>
                  <th scope="col" className="num">n</th>
                  <th scope="col" className="num">Win</th>
                  <th scope="col" className="num">Med. best</th>
                </tr>
              </thead>
              <tbody>
                {cohortRows.map(({ key, label }) => {
                  const row = facts.cohorts[key];
                  if (!row) return null;
                  const win = row.win_rate;
                  return (
                    <tr key={key}>
                      <td>{label}</td>
                      <td className="num">{row.signals ?? "—"}</td>
                      <td className={`num ${win !== null && win >= 50 ? "pos" : "neg"}`}>{pct(win)}</td>
                      <td className="num">{pct(row.median_max_move_pct)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <footer className="rb-foot">
        <p>
          {facts.universe.signals?.toLocaleString("en-IN")} signals reconstructed by replaying the
          scanners across {facts.universe.symbols_replayed?.toLocaleString("en-IN")} stocks over{" "}
          {facts.universe.sessions_replayed} sessions. Entry at the trigger session's close;
          {stop !== null ? ` a ${stop}% adverse move counts as a loss,` : ""}
          {target !== null ? ` a close ${target}% up counts as a win,` : ""}
          {horizon !== null ? ` giving up after ${horizon} sessions.` : ""}
          {facts.comparison_is_like_for_like && facts.comparison_horizon_sessions
            ? ` Week-on-week changes are measured over ${facts.comparison_horizon_sessions} sessions for both weeks, so a partly-open week is not compared against a finished one.`
            : ""}
        </p>
        <p className="rb-source">
          {brief.source === "ai"
            ? "Narrative written by AI from the measured figures above — it cannot introduce numbers of its own."
            : "Narrative generated from the measured figures (AI unavailable)."}
          {data.stats_generated_at ? ` Statistics built ${new Date(data.stats_generated_at).toLocaleString("en-IN")}.` : ""}
        </p>
      </footer>
    </section>
  );
}
