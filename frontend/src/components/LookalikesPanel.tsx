import { useEffect, useMemo, useRef, useState } from "react";
import { AlertTriangle, ExternalLink, RefreshCw } from "lucide-react";

import {
  getLookalikePicks,
  getLookalikes,
  type LookalikePicksSummary,
  type LookalikeMatch,
  type LookalikeReference,
  type LookalikeLibrary,
  type LookalikeWindow,
  type Lookalikes,
} from "../lib/api";
import { CANDLE_DOWN, CANDLE_UP } from "../lib/marketColors";
import { Panel } from "./Panel";
import { CalendarView, ReviewsView } from "./LookalikePicks";
import { CompareModal } from "./LookalikeModals";
import { fullChartUrl } from "../lib/chartLink";

import "./LookalikesPanel.css";

/* Chart look-alikes.

   Each match is drawn beside the reference setup it most resembles, in the
   same standard style the model saw: the last 120 sessions, candles over a
   50-day average, volume underneath. The windows arrive as 0..1 shapes rather
   than prices, so the page can show a reference without republishing it.

   The library's own test result sits above the matches, warnings included,
   because a list of look-alikes reads as a recommendation whether or not the
   evidence behind it is strong enough to be one. */

type Props = {
  onOpenSymbolChart?: (symbol: string) => void;
};

type OutcomeFilter = "any" | "worked" | "template";
type PageView = "today" | "calendar" | "reviews";

// Fewer than this many decided setups on either side of a rule and its
// worked-rate comparison is noise; the table shows a dash instead.
const MIN_RULE_SAMPLE = 20;

const LABEL_TEXT: Record<LookalikeReference["label"], string> = {
  worked: "Worked",
  failed: "Failed",
  pending: "Still running",
};

// Canvas cannot read CSS variables, so the non-candle colours are fixed here.
const SMA_COLOUR = "#5a78c8";
const VOLUME_COLOUR = "rgba(128, 128, 128, 0.35)";

function MiniChart({ window: w, label }: { window: LookalikeWindow; label: string }) {
  const ref = useRef<HTMLCanvasElement | null>(null);

  useEffect(() => {
    const canvas = ref.current;
    if (!canvas || !w?.c?.length) return;
    const dpr = window.devicePixelRatio || 1;
    const cssW = canvas.clientWidth || 300;
    const cssH = canvas.clientHeight || 150;
    canvas.width = Math.round(cssW * dpr);
    canvas.height = Math.round(cssH * dpr);
    const g = canvas.getContext("2d");
    if (!g) return;
    g.setTransform(dpr, 0, 0, dpr, 0, 0);
    g.clearRect(0, 0, cssW, cssH);

    const n = w.c.length;
    const priceH = cssH * 0.78;
    const volTop = cssH * 0.8;
    const volH = cssH - volTop;
    const step = cssW / n;
    const body = Math.max(1, step * 0.6);
    const y = (value: number) => 2 + (1 - value) * (priceH - 4);

    for (let i = 0; i < n; i += 1) {
      const x = (i + 0.5) * step;
      g.fillStyle = VOLUME_COLOUR;
      const vh = (w.v[i] ?? 0) * volH;
      g.fillRect(x - body / 2, cssH - vh, body, vh);

      const up = w.c[i] >= w.o[i];
      g.strokeStyle = up ? CANDLE_UP : CANDLE_DOWN;
      g.fillStyle = up ? CANDLE_UP : CANDLE_DOWN;
      g.lineWidth = 1;
      g.beginPath();
      g.moveTo(x, y(w.h[i]));
      g.lineTo(x, y(w.l[i]));
      g.stroke();
      const top = Math.min(y(w.o[i]), y(w.c[i]));
      const height = Math.max(1, Math.abs(y(w.o[i]) - y(w.c[i])));
      g.fillRect(x - body / 2, top, body, height);
    }

    g.strokeStyle = SMA_COLOUR;
    g.lineWidth = 1.5;
    g.beginPath();
    w.sma.forEach((value, i) => {
      const x = (i + 0.5) * step;
      if (i === 0) g.moveTo(x, y(value));
      else g.lineTo(x, y(value));
    });
    g.stroke();
  }, [w]);

  return <canvas ref={ref} className="lookalike-canvas" role="img" aria-label={label} />;
}

function formatDate(iso: string | undefined) {
  if (!iso) return "—";
  const d = new Date(`${iso}T00:00:00`);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric" });
}

function ReferenceOutcome({ reference }: { reference: LookalikeReference }) {
  const detail =
    reference.label === "pending"
      ? `best +${reference.max_gain_pct.toFixed(1)}%, worst ${reference.max_loss_pct.toFixed(1)}% so far`
      : `${reference.label === "worked" ? `+${reference.max_gain_pct.toFixed(1)}%` : `${reference.max_loss_pct.toFixed(1)}%`}` +
        (reference.days_to_result != null ? ` in ${reference.days_to_result} sessions` : "");
  return (
    <span className="lookalike-outcome">
      <span className={`lookalike-chip is-${reference.label}`}>{LABEL_TEXT[reference.label]}</span>
      <span className="lookalike-outcome-detail">{detail}</span>
    </span>
  );
}

function styleName(style: string) {
  return style ? style.charAt(0).toUpperCase() + style.slice(1) : "Library";
}

/* Out-of-sample separation against how many charts the model learned from.
   A line still rising at its last point says more charts would help. */
function LearningCurve({ curve }: { curve: LookalikeLibrary["evaluation"]["learning_curve"] }) {
  if (!curve?.length) {
    return (
      <p className="lookalike-curve-empty">
        No learning curve yet — it needs at least 60 charts spread over more than a year, so the model can learn on older
        charts and be tested on newer ones.
      </p>
    );
  }
  const lo = 0.5;
  const hi = Math.max(0.75, ...curve.map((p) => p.auc));
  return (
    <div className="lookalike-curve" role="table" aria-label="Learning curve">
      {curve.map((point) => {
        const width = Math.max(0, Math.min(1, (point.auc - lo) / (hi - lo))) * 100;
        return (
          <div className="lookalike-curve-row" role="row" key={point.charts}>
            <span role="cell" className="lookalike-curve-n">
              {point.charts.toLocaleString("en-IN")} charts
            </span>
            <span role="cell" className="lookalike-curve-bar">
              <span style={{ width: `${width}%` }} />
            </span>
            <span role="cell" className="lookalike-curve-v">
              {(point.auc * 100).toFixed(0)}%
            </span>
          </div>
        );
      })}
    </div>
  );
}

function RuleChecklist({ flags, labels }: { flags: Record<string, boolean> | null; labels: Record<string, string> }) {
  if (!flags) return <p className="lookalike-rules-none">Not enough history to check the rules.</p>;
  return (
    <ul className="lookalike-rules-list">
      {Object.entries(labels).map(([key, label]) => (
        <li key={key} className={flags[key] ? "is-pass" : "is-fail"} title={label}>
          {flags[key] ? "✓" : "✗"} {label}
        </li>
      ))}
    </ul>
  );
}

function RulesTable({ rows }: { rows: NonNullable<LookalikeLibrary["rules"]> }) {
  const pct = (v: number | null) => (v == null ? "—" : `${v.toFixed(0)}%`);
  return (
    <div className="lookalike-rules-table-wrap">
      <table className="lookalike-rules-table">
        <thead>
          <tr>
            <th>Rule</th>
            <th>His setups pass</th>
            <th>Ordinary days pass</th>
            <th>Worked when passed</th>
            <th>Worked when failed</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const enough = row.decided_pass >= MIN_RULE_SAMPLE && row.decided_fail >= MIN_RULE_SAMPLE;
            return (
              <tr key={row.rule}>
                <td>{row.label}</td>
                <td>{pct(row.setups_pass_pct)}</td>
                <td>{pct(row.ordinary_pass_pct)}</td>
                <td title={`${row.decided_pass} finished setups`}>{enough ? pct(row.worked_when_pass_pct) : "—"}</td>
                <td title={`${row.decided_fail} finished setups`}>{enough ? pct(row.worked_when_fail_pct) : "—"}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function MatchCard({
  match,
  references,
  ruleLabels,
  onOpen,
}: {
  match: LookalikeMatch;
  references: Record<string, LookalikeReference>;
  ruleLabels: Record<string, string>;
  onOpen?: (symbol: string) => void;
}) {
  const [showRules, setShowRules] = useState(false);
  const [comparing, setComparing] = useState(false);
  const [pick, setPick] = useState(0);
  const nearest = match.nearest ?? [];
  const chosen = nearest[Math.min(pick, nearest.length - 1)];
  const reference = chosen ? references[chosen.key] : undefined;

  return (
    <article className="lookalike-card">
      <header className="lookalike-card-head">
        <span className="lookalike-rank">#{match.rank}</span>
        <a className="lookalike-symbol" href={fullChartUrl(match.symbol)} target="_blank" rel="noreferrer noopener" title="Open on my site in a new tab">
          {match.symbol}
        </a>
        <span className="lookalike-meta">
          ₹{match.close.toLocaleString("en-IN")} · ₹{match.turnover_crore.toFixed(1)} cr/day
        </span>
        {match.template != null ? (
          <button
            type="button"
            className={`lookalike-template${match.template === 8 ? " is-full" : ""}`}
            onClick={() => setShowRules((v) => !v)}
            aria-expanded={showRules}
          >
            Trend Template {match.template}/8
          </button>
        ) : null}
        <span className="lookalike-pct" title="Share of ordinary charts (random days the model never trained on) this chart out-scores">
          Beats {match.percentile.toFixed(0)}% of ordinary charts
        </span>
      </header>
      <div className="lookalike-pair">
        <figure>
          <MiniChart window={match.window} label={`${match.symbol}, last 120 sessions`} />
          <figcaption>
            {match.symbol} today · {formatDate(match.session)}
          </figcaption>
        </figure>
        <figure>
          {reference ? (
            <MiniChart window={reference.window} label={`${reference.name}, 120 sessions before the setup`} />
          ) : (
            <div className="lookalike-canvas lookalike-missing">Reference unavailable</div>
          )}
          <figcaption>
            {reference ? (
              <>
                <span>{reference.name}</span>
                <ReferenceOutcome reference={reference} />
              </>
            ) : (
              "—"
            )}
          </figcaption>
        </figure>
      </div>
      {match.reason ? <p className="lookalike-reason">{match.reason}</p> : null}
      <div className="lookalike-links">
        <a className="lookalike-link" href={fullChartUrl(match.symbol)} target="_blank" rel="noreferrer noopener">
          Open {match.symbol} on my site <ExternalLink size={12} />
        </a>
        {reference?.chart ? (
          <button type="button" className="lookalike-link" onClick={() => setComparing(true)}>
            Compare with {reference.name} at its date
          </button>
        ) : null}
      </div>
      {comparing && reference?.chart && reference.ticker && reference.date && reference.link ? (
        <CompareModal
          symbol={match.symbol}
          chart={match.window}
          session={match.session}
          reference={{
            name: reference.name,
            ticker: reference.ticker,
            date: reference.date,
            style: reference.style,
            label: reference.label,
            max_gain_pct: reference.max_gain_pct,
            max_loss_pct: reference.max_loss_pct,
            days_to_result: reference.days_to_result,
            link: reference.link,
            chart: reference.chart,
          }}
          onClose={() => setComparing(false)}
        />
      ) : null}
      {showRules ? <RuleChecklist flags={match.rules} labels={ruleLabels} /> : null}
      {nearest.length > 1 ? (
        <div className="lookalike-nearest" role="tablist" aria-label="Closest reference setups">
          {nearest.map((near, index) => {
            const ref = references[near.key];
            return (
              <button
                key={near.key}
                type="button"
                role="tab"
                aria-selected={index === pick}
                className={`lookalike-near-tab${index === pick ? " is-active" : ""}`}
                onClick={() => setPick(index)}
              >
                {index + 1}. {ref ? LABEL_TEXT[ref.label] : "?"} · {(near.similarity * 100).toFixed(0)}% alike
              </button>
            );
          })}
        </div>
      ) : null}
    </article>
  );
}

export function LookalikesPanel({ onOpenSymbolChart }: Props) {
  const [data, setData] = useState<Lookalikes | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState<OutcomeFilter>("any");
  const [style, setStyle] = useState<string | null>(null);
  const [view, setView] = useState<PageView>("today");
  const [picks, setPicks] = useState<LookalikePicksSummary | null>(null);
  const [picksError, setPicksError] = useState<string | null>(null);
  const [reloadKey, setReloadKey] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    getLookalikes()
      .then((payload) => {
        if (!cancelled) setData(payload);
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : "Could not load the look-alike scan.");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    getLookalikePicks()
      .then((payload) => {
        if (!cancelled) setPicks(payload);
      })
      .catch((err) => {
        if (!cancelled) setPicksError(err instanceof Error ? err.message : "Could not load the pick history.");
      });
    return () => {
      cancelled = true;
    };
  }, [reloadKey]);

  const available = data && data.available ? data : null;
  const styleKeys = useMemo(() => {
    const styles = available?.styles ?? {};
    // Largest library first: it is the one whose numbers mean the most.
    return Object.keys(styles).sort((a, b) => (styles[b]?.library?.references ?? 0) - (styles[a]?.library?.references ?? 0));
  }, [available]);
  const activeStyle = style && styleKeys.includes(style) ? style : styleKeys[0] ?? null;
  const block = activeStyle ? available?.styles?.[activeStyle] : undefined;

  const matches = useMemo(() => {
    if (!available || !block) return [];
    const list = block.matches ?? [];
    if (filter === "any") return list;
    if (filter === "template") return list.filter((m) => m.template === 8);
    return list.filter((m) => available.references?.[m.nearest?.[0]?.key ?? ""]?.label === "worked");
  }, [available, block, filter]);

  const refresh = (
    <button type="button" className="lookalike-refresh" onClick={() => setReloadKey((k) => k + 1)} disabled={loading}>
      <RefreshCw size={14} /> Refresh
    </button>
  );

  if (loading && !data) {
    return (
      <Panel title="Chart Look-alikes" subtitle="Setups that resemble your reference library">
        <p className="lookalike-empty">Loading today's scan…</p>
      </Panel>
    );
  }

  if (error || !available || !block) {
    return (
      <Panel title="Chart Look-alikes" subtitle="Setups that resemble your reference library" actions={refresh}>
        <p className="lookalike-empty">
          {error ?? (data && !data.available ? data.reason : "No scan available.")}
        </p>
      </Panel>
    );
  }

  const lib = block.library;
  const ev = lib?.evaluation;
  const auc = ev?.setup_vs_random_auc;

  return (
    <Panel
      title="Chart Look-alikes"
      subtitle={`Indian charts on ${formatDate(available.session)} that resemble your reference setups`}
      actions={refresh}
    >
      <div className="lookalike-views" role="tablist" aria-label="Look-alike views">
        {(
          [
            ["today", "Today"],
            ["calendar", "Calendar"],
            ["reviews", "Reviews & learning"],
          ] as Array<[PageView, string]>
        ).map(([key, label]) => (
          <button
            key={key}
            type="button"
            role="tab"
            aria-selected={view === key}
            className={`bot-view-tab${view === key ? " is-active" : ""}`}
            onClick={() => setView(key)}
          >
            {label}
          </button>
        ))}
      </div>

      {view !== "today" ? (
        picks && picks.available ? (
          view === "calendar" ? (
            <CalendarView summary={picks} onOpen={onOpenSymbolChart} />
          ) : (
            <ReviewsView summary={picks} />
          )
        ) : (
          <p className="lookalike-empty">
            {picksError ?? (picks && !picks.available ? picks.reason : "Loading the pick history…")}
          </p>
        )
      ) : null}

      {view === "today" && styleKeys.length > 1 ? (
        <div className="lookalike-styles" role="tablist" aria-label="Whose setups">
          {styleKeys.map((key) => (
            <button
              key={key}
              type="button"
              role="tab"
              aria-selected={key === activeStyle}
              className={`lookalike-filter${key === activeStyle ? " is-active" : ""}`}
              onClick={() => setStyle(key)}
            >
              {styleName(key)} style · {available.styles[key]?.library?.references ?? 0} charts
            </button>
          ))}
        </div>
      ) : null}

      {view === "today" ? (
      <>
      <section className="lookalike-summary">
        <div className="lookalike-stat">
          <span className="lookalike-stat-label">Reference charts</span>
          <strong>
            {lib?.references ?? 0}
            {lib?.submitted && lib.submitted > lib.references ? (
              <span className="lookalike-stat-vs"> of {lib.submitted}</span>
            ) : null}
          </strong>
          <span className="lookalike-stat-sub">
            {formatDate(lib?.first_date)}
            {lib?.first_date !== lib?.last_date ? ` – ${formatDate(lib?.last_date)}` : ""}
          </span>
        </div>
        <div className="lookalike-stat">
          <span className="lookalike-stat-label">How often the setups worked</span>
          <strong>
            {lib?.worked_rate_pct?.setups == null ? "—" : `${lib.worked_rate_pct.setups.toFixed(0)}%`}
            <span className="lookalike-stat-vs">
              {" "}vs {lib?.worked_rate_pct?.ordinary_days == null ? "—" : `${lib.worked_rate_pct.ordinary_days.toFixed(0)}%`} on ordinary days
            </span>
          </strong>
          <span className="lookalike-stat-sub">
            {lib?.outcomes?.worked ?? 0} worked · {lib?.outcomes?.failed ?? 0} failed · {lib?.outcomes?.pending ?? 0} still running · +
            {lib?.outcome_rule?.target_pct}% before −{lib?.outcome_rule?.stop_pct}% within {lib?.outcome_rule?.horizon_sessions} sessions
          </span>
        </div>
        <div className="lookalike-stat">
          <span className="lookalike-stat-label">Tells setups from ordinary days</span>
          <strong>{auc == null ? "—" : `${(auc * 100).toFixed(0)}%`}</strong>
          <span className="lookalike-stat-sub">50% is a coin flip · tested on charts it did not learn from</span>
        </div>
        <div className="lookalike-stat">
          <span className="lookalike-stat-label">Picks the winners among the setups</span>
          <strong>
            {ev?.outcome_auc == null || Math.min(ev.worked ?? 0, ev.failed ?? 0) < 20 ? "—" : `${(ev.outcome_auc * 100).toFixed(0)}%`}
          </strong>
          <span className="lookalike-stat-sub">
            {ev?.outcome_auc == null || Math.min(ev.worked ?? 0, ev.failed ?? 0) < 20
              ? "not enough finished setups to measure"
              : ev.outcome_auc < 0.55
                ? "no better than a coin flip — looking more like the style does not mean more likely to work"
                : `50% is a coin flip · ${ev.worked} worked vs ${ev.failed} failed, unseen`}
          </span>
        </div>
        <div className="lookalike-stat">
          <span className="lookalike-stat-label">Indian charts scanned</span>
          <strong>{available.scanned.toLocaleString("en-IN")}</strong>
          <span className="lookalike-stat-sub">turnover ≥ ₹{available.filters?.min_turnover_crore} cr/day</span>
        </div>
      </section>

      <section className="lookalike-curve-wrap">
        <h3>Learning curve</h3>
        <p className="lookalike-stat-sub">
          How well it tells this style's setups from ordinary days, by number of charts learned from — tested on the newest
          30% of charts, which it never saw. 50% is a coin flip.
        </p>
        <LearningCurve curve={ev?.learning_curve ?? []} />
      </section>

      {lib?.rules?.length ? (
        <section className="lookalike-curve-wrap">
          <h3>His rules, tested on his own charts</h3>
          <p className="lookalike-stat-sub">
            A rule describes his style if his setups pass it far more often than ordinary days do. It picks winners only
            if setups that passed it worked more often than setups that failed it.
          </p>
          <RulesTable rows={lib.rules} />
        </section>
      ) : null}

      {ev?.warnings?.length ? (
        <div className="lookalike-warning" role="note">
          <AlertTriangle size={16} />
          <ul>
            {ev.warnings.map((w) => (
              <li key={w}>{w}</li>
            ))}
          </ul>
        </div>
      ) : null}

      <div className="lookalike-toolbar">
        <span>Show:</span>
        {(["any", "worked", "template"] as OutcomeFilter[]).map((option) => (
          <button
            key={option}
            type="button"
            className={`lookalike-filter${filter === option ? " is-active" : ""}`}
            onClick={() => setFilter(option)}
          >
            {option === "any" ? "Any match" : option === "worked" ? "Closest setup worked" : "Passes all 8 Trend Template rules"}
          </button>
        ))}
        <span className="lookalike-count">{matches.length} shown</span>
      </div>

      {matches.length ? (
        <div className="lookalike-grid">
          {matches.map((match) => (
            <MatchCard
              key={match.symbol}
              match={match}
              references={available.references ?? {}}
              ruleLabels={available.rule_labels ?? {}}
              onOpen={onOpenSymbolChart}
            />
          ))}
        </div>
      ) : (
        <p className="lookalike-empty">No matches for this filter.</p>
      )}
      </>
      ) : null}
    </Panel>
  );
}
