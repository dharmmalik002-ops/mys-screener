import { useMemo } from "react";
import { AlertTriangle, ArrowRight, CheckCircle2, Info, TrendingDown, TrendingUp } from "lucide-react";

import type {
  BotBacktest,
  BotCellStatus,
  BotConditionStudy,
  BotEvolutionChange,
  BotLearning,
} from "../lib/api";

/* The two learning views.

   `LearningView` answers "what kind of trade works, and what did the losers
   have in common". `EvolutionView` answers "has the bot's view changed, and
   when" — which is the only honest evidence that it adapts at all.

   Both are rendered from numbers computed in Python. Nothing on this page is
   generated prose, and where a finding was rejected the rejection is shown
   rather than the finding being dropped. */

function formatR(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}R`;
}

const STATUS_TONE: Record<string, string> = {
  confirmed: "good",
  watch: "warn",
  retired: "bad",
  candidate: "flat",
  rejected: "flat",
};

function StatusPill({ status, label }: { status: string; label: string }) {
  return <span className={`bot-pill bot-pill-${STATUS_TONE[status] ?? "flat"}`}>{label}</span>;
}

/* --- Learning -------------------------------------------------------------- */

function ConditionCard({ study }: { study: BotConditionStudy }) {
  const best = Math.max(...study.buckets.map((b) => Math.abs(b.avg_r)), 0.001);
  const real = study.monotone && study.verdict.includes("holds out-of-sample");

  return (
    <article className={`bot-condition ${real ? "bot-condition-real" : ""}`}>
      <header>
        <div>
          <h5>{study.label}</h5>
          <p className="bot-condition-q">{study.question}</p>
        </div>
        <span className={`bot-pill bot-pill-${study.duplicates ? "warn" : real ? "good" : "flat"}`}>
          {study.duplicates ? "Same effect, restated" : real ? "Real effect" : "Not supported"}
        </span>
      </header>

      <table className="bot-table bot-table-compact">
        <thead>
          <tr>
            <th>Bucket</th>
            <th className="num">Trades</th>
            <th className="num">Win</th>
            <th className="num">Avg R</th>
            <th className="num">Payoff</th>
            <th className="num">Held-out</th>
            <th aria-label="magnitude" />
          </tr>
        </thead>
        <tbody>
          {study.buckets.map((bucket) => (
            <tr key={bucket.label}>
              <td>{bucket.label}</td>
              <td className="num">{bucket.trades.toLocaleString("en-IN")}</td>
              <td className="num">{bucket.win_rate.toFixed(0)}%</td>
              <td className={`num ${bucket.avg_r > 0 ? "bot-expected" : "bot-negative"}`}>
                {formatR(bucket.avg_r)}
              </td>
              <td className="num">{bucket.payoff.toFixed(2)}</td>
              <td className="num">
                {bucket.oos_avg_r === null
                  ? <span className="bot-cell-thin">too few</span>
                  : formatR(bucket.oos_avg_r)}
              </td>
              <td className="bot-bar-cell">
                <div
                  className={`bot-bar ${bucket.avg_r >= 0 ? "bot-bar-pos" : "bot-bar-neg"}`}
                  style={{ width: `${Math.min(100, (Math.abs(bucket.avg_r) / best) * 100)}%` }}
                />
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      <p className={`bot-condition-verdict ${real ? "bot-verdict-good" : ""}`}>
        {real ? <CheckCircle2 size={14} aria-hidden /> : <AlertTriangle size={14} aria-hidden />}
        <span>{study.verdict}</span>
      </p>
    </article>
  );
}

export function LearningView({ learning }: { learning: BotLearning }) {
  const summary = learning.review_summary;
  // A study flagged `duplicates` measures the same underlying quantity as
  // another (stop width is ATR rescaled), so it is excluded from the count.
  // Counting it would report one finding as two pieces of evidence.
  const real = learning.condition_studies.filter(
    (s) => s.monotone && s.verdict.includes("holds out-of-sample") && !s.duplicates,
  );

  return (
    <div className="bot-learning">
      <div className="bot-alert bot-alert-info">
        <Info size={15} aria-hidden />
        <span>
          Every trade in the book is judged on its own path — how close it came to the stop, how
          much of its best move it kept — not just on whether it made money. A win taken from a
          position that was nearly stopped out is not the same decision as a clean one, and
          sorting on P&amp;L alone cannot tell them apart.
        </span>
      </div>

      <section>
        <h4>What kind of trades these were</h4>
        <p className="bot-section-note">
          {summary.trades.toLocaleString("en-IN")} closed trades, book average{" "}
          <strong>{formatR(summary.book_avg_r)}</strong>.
        </p>
        <table className="bot-table bot-table-compact">
          <thead>
            <tr>
              <th>Shape</th><th className="num">Trades</th><th className="num">Share</th>
              <th className="num">Avg R</th><th>What it means</th>
            </tr>
          </thead>
          <tbody>
            {summary.verdicts.map((row) => (
              <tr key={row.verdict}>
                <td>{row.label}</td>
                <td className="num">{row.trades.toLocaleString("en-IN")}</td>
                <td className="num">{row.pct_of_trades.toFixed(1)}%</td>
                <td className={`num ${row.avg_r >= 0 ? "bot-expected" : "bot-negative"}`}>
                  {formatR(row.avg_r)}
                </td>
                <td className="bot-verdict-note">{row.note}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {summary.structural_note ? (
          <p className="bot-verdict bot-verdict-warn">
            <AlertTriangle size={15} aria-hidden />
            <span>{summary.structural_note}</span>
          </p>
        ) : null}
      </section>

      <section>
        <h4>Lessons the book supports ({summary.lessons.length})</h4>
        <p className="bot-section-note">{summary.lesson_basis}</p>
        {summary.lessons.length ? (
          <ul className="bot-lessons">
            {summary.lessons.map((lesson) => (
              <li key={lesson.tag} className={`bot-lesson bot-lesson-${lesson.direction}`}>
                {lesson.direction === "better"
                  ? <TrendingUp size={14} aria-hidden />
                  : <TrendingDown size={14} aria-hidden />}
                <span>{lesson.text}</span>
              </li>
            ))}
          </ul>
        ) : (
          <p className="bot-playbook-empty">
            No entry-time condition separated results by enough to be worth acting on.
          </p>
        )}

        {summary.outcome_tags.length ? (
          <details className="bot-details">
            <summary>
              Why {summary.outcome_tags.length} other patterns are excluded
            </summary>
            <p className="bot-section-note">
              These describe what a trade did after it was taken, so averaging R over them
              restates the outcome. "Trades that came within a whisker of the stop averaged
              +1.88R" cannot include a trade that was stopped out — it is the definition talking
              back, not a finding. They are kept for the shapes above and barred from lessons.
            </p>
            <table className="bot-table bot-table-compact">
              <thead>
                <tr><th>Pattern</th><th className="num">Trades</th><th className="num">Avg R</th></tr>
              </thead>
              <tbody>
                {summary.outcome_tags.map((tag) => (
                  <tr key={tag.tag} className="bot-row-muted">
                    <td>{tag.tag.replace(/_/g, " ")}</td>
                    <td className="num">{tag.trades.toLocaleString("en-IN")}</td>
                    <td className="num">{formatR(tag.avg_r)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </details>
        ) : null}
      </section>

      <section>
        <h4>What conditions actually matter</h4>
        <p className="bot-section-note">
          Each condition measured across its whole range rather than at one cutoff, then
          re-checked on the held-out period from {learning.condition_split}. A real effect
          moves in one direction across every bucket <em>and</em> survives out-of-sample —{" "}
          {real.length} of {learning.condition_studies.length} did. One further study passes the
          tests but measures the same quantity a different way, and is marked rather than counted
          twice.
        </p>
        <div className="bot-conditions">
          {learning.condition_studies.map((study) => (
            <ConditionCard key={study.condition} study={study} />
          ))}
        </div>
      </section>
    </div>
  );
}

/* --- Evolution -------------------------------------------------------------- */

function TimelineBand({ learning }: { learning: BotLearning }) {
  const points = learning.evolution_timeline;
  const max = Math.max(...points.map((p) => Object.values(p.counts).reduce((a, b) => a + b, 0)), 1);

  return (
    <div className="bot-timeline" role="img" aria-label="Tradeable strategy cells over time">
      {points.map((point) => {
        const total = Object.values(point.counts).reduce((a, b) => a + b, 0) || 1;
        const height = (total / max) * 100;
        const confirmed = ((point.counts.confirmed ?? 0) / total) * height;
        const watch = ((point.counts.watch ?? 0) / total) * height;
        const retired = ((point.counts.retired ?? 0) / total) * height;
        return (
          <div
            key={point.as_of}
            className="bot-timeline-col"
            title={`${point.as_of}: ${point.tradeable} tradeable of ${total} scored`}
          >
            <div className="bot-timeline-seg bot-seg-retired" style={{ height: `${retired}%` }} />
            <div className="bot-timeline-seg bot-seg-watch" style={{ height: `${watch}%` }} />
            <div className="bot-timeline-seg bot-seg-confirmed" style={{ height: `${confirmed}%` }} />
          </div>
        );
      })}
    </div>
  );
}

export function EvolutionView({ learning }: { learning: BotLearning }) {
  const byStatus = useMemo(() => {
    const groups: Record<string, BotCellStatus[]> = {};
    learning.cell_status.forEach((cell) => {
      (groups[cell.status] ??= []).push(cell);
    });
    return groups;
  }, [learning.cell_status]);

  const order = ["confirmed", "watch", "candidate", "retired", "rejected"];

  return (
    <div className="bot-evolution-view">
      <div className="bot-alert bot-alert-info">
        <Info size={15} aria-hidden />
        <span>{learning.method_note}</span>
      </div>

      <section>
        <h4>What the bot believed, quarter by quarter</h4>
        <p className="bot-section-note">
          {learning.evolution_timeline.length} checkpoints from{" "}
          {learning.evolution_timeline[0]?.as_of} to {learning.cell_status_as_of}. Each column is
          every strategy × regime pairing scored on trades closed by that date only. Green is
          cleared to trade, amber is on watch, red is stood down.
        </p>
        <TimelineBand learning={learning} />
        <div className="bot-legend">
          <span><i className="bot-swatch bot-seg-confirmed" /> Confirmed</span>
          <span><i className="bot-swatch bot-seg-watch" /> On watch</span>
          <span><i className="bot-swatch bot-seg-retired" /> Retired</span>
        </div>
      </section>

      <section>
        <h4>Where it changed its mind ({learning.evolution_changes_total})</h4>
        <p className="bot-section-note">
          Every point a strategy was promoted or stood down, most recent first. A system that
          never appears in this table is not adapting, whatever it claims.
        </p>
        <table className="bot-table bot-table-compact">
          <thead>
            <tr>
              <th>Date</th><th>Setup</th><th>Regime</th><th>Change</th>
              <th className="num">Lifetime</th><th className="num">Recent</th>
            </tr>
          </thead>
          <tbody>
            {learning.evolution_changes.slice(0, 40).map((change: BotEvolutionChange, index) => (
              <tr key={`${change.as_of}-${change.strategy}-${change.regime}-${index}`}>
                <td className="bot-mono">{change.as_of}</td>
                <td>{change.strategy.replace(/_/g, " ")}</td>
                <td>{change.regime.replace(/_/g, " ")}</td>
                <td className="bot-change">
                  <StatusPill status={change.from_status} label={change.from_label} />
                  <ArrowRight size={12} aria-hidden />
                  <StatusPill status={change.to_status} label={change.to_label} />
                </td>
                <td className="num">{formatR(change.avg_r)}</td>
                <td className={`num ${(change.recent_avg_r ?? 0) < 0 ? "bot-negative" : ""}`}>
                  {change.recent_avg_r === null ? "—" : formatR(change.recent_avg_r)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section>
        <h4>Where every strategy stands now</h4>
        <p className="bot-section-note">
          As at {learning.cell_status_as_of}. "Recent" is the trailing two years; a cell that is
          positive lifetime and negative recently is retired rather than repaired.
        </p>
        {order.map((status) => {
          const cells = byStatus[status];
          if (!cells?.length) return null;
          const meta = learning.status_catalogue.find((s) => s.id === status);
          return (
            <div key={status} className="bot-status-group">
              <h5>
                <StatusPill status={status} label={meta?.label ?? status} />
                <span className="bot-status-count">{cells.length}</span>
              </h5>
              <p className="bot-section-note">{meta?.note}</p>
              <table className="bot-table bot-table-compact">
                <thead>
                  <tr>
                    <th>Setup</th><th>Regime</th><th className="num">Trades</th>
                    <th className="num">Win</th><th className="num">Payoff</th>
                    <th className="num">Lifetime</th><th className="num">Recent</th>
                  </tr>
                </thead>
                <tbody>
                  {cells.map((cell) => (
                    <tr key={`${cell.strategy}-${cell.regime}`} title={cell.note}>
                      <td>{cell.strategy.replace(/_/g, " ")}</td>
                      <td>{cell.regime.replace(/_/g, " ")}</td>
                      <td className="num">{cell.trades.toLocaleString("en-IN")}</td>
                      <td className="num">{cell.win_rate.toFixed(0)}%</td>
                      <td className="num">{cell.payoff.toFixed(2)}</td>
                      <td className={`num ${cell.avg_r >= 0 ? "bot-expected" : "bot-negative"}`}>
                        {formatR(cell.avg_r)}
                      </td>
                      <td className={`num ${(cell.recent_avg_r ?? 0) < 0 ? "bot-negative" : ""}`}>
                        {cell.recent_avg_r === null ? "—" : formatR(cell.recent_avg_r)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          );
        })}
      </section>
    </div>
  );
}
