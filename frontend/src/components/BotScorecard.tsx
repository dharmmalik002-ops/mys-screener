import { AlertTriangle, Check, Info, Minus, X } from "lucide-react";

import type { BotBenchmark, BotLearning, BotPortfolioRun, BotSensitivity } from "../lib/api";

/* The scorecard: the bot as an account, ranked against real fund managers.

   Everything else in this tab is trade-level. That is a statement about a
   population of signals, and it is not the same as a result: no account can
   take every signal, capital is finite, and the trades you are forced to skip
   are not a random sample of the ones you wanted. This view closes that gap —
   the same trades run through finite capital and eight slots, then placed
   inside the distribution of real Indian equity funds over a window the
   system never saw.

   The caveats are rendered with the numbers rather than under them, because a
   verdict that travels without them will be quoted without them. */

function formatPct(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  return `${value >= 0 ? "" : ""}${value.toFixed(digits)}%`;
}

function VerdictMark({ verdict }: { verdict: boolean | null }) {
  // null is "inside the noise band", not "unknown" — a 0.01-point win over a
  // 3.8-year window is a tie, and calling it a win is how a measurement turns
  // into marketing.
  if (verdict === null) {
    return <span className="bot-mark bot-mark-flat"><Minus size={13} aria-hidden /> Level</span>;
  }
  return verdict ? (
    <span className="bot-mark bot-mark-good"><Check size={13} aria-hidden /> Better</span>
  ) : (
    <span className="bot-mark bot-mark-bad"><X size={13} aria-hidden /> Worse</span>
  );
}

function EquityCurve({ run }: { run: BotPortfolioRun }) {
  const points = run.equity_curve;
  if (points.length < 2) return null;
  const values = points.map((p) => p.equity);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const path = points
    .map((p, i) => {
      const x = (i / (points.length - 1)) * 100;
      const y = 100 - ((p.equity - min) / span) * 100;
      return `${i === 0 ? "M" : "L"}${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
  const ended = run.ending_equity >= run.starting_equity;

  return (
    <svg
      className="bot-equity"
      viewBox="0 0 100 100"
      preserveAspectRatio="none"
      role="img"
      aria-label={`Equity curve, ${run.start} to ${run.end}`}
    >
      <path d={path} className={ended ? "bot-equity-up" : "bot-equity-down"} vectorEffect="non-scaling-stroke" />
    </svg>
  );
}

export function ScorecardView({ learning }: { learning: BotLearning }) {
  const benchmark: BotBenchmark | null = learning.benchmark ?? null;
  const runs = learning.portfolio_runs ?? [];
  const headline = benchmark?.headline ?? null;
  const answer = benchmark?.answer ?? null;
  const headlineRun = runs.find((r) => r.label === "playbook_held_out") ?? runs[0];
  const sensitivity: BotSensitivity | null = learning.config_sensitivity ?? null;

  const RUN_LABELS: Record<string, string> = {
    playbook_held_out: "The system, on data it never saw",
    playbook_no_vol_adjust: "Same, without the volatility adjustment",
    reactive_held_out: "Re-deciding eligibility every quarter",
    no_gating: "Every strategy, full history (upper bound)",
  };

  return (
    <div className="bot-scorecard">
      <div className="bot-alert bot-alert-info">
        <Info size={15} aria-hidden />
        <span>
          Everything else on this page is trade-level. An account is different: capital is
          finite, positions compete for it, and the trades it can take are not a random sample
          of the ones it wanted. This is the same trade record run as an account, then compared
          with what the money could have done elsewhere.
        </span>
      </div>

      {answer && headline ? (
        <section>
          <h4>Is it better than a professional?</h4>
          <p className="bot-section-note">
            Measured over {headline.window_years} years the system never saw, against{" "}
            <strong>{headline.funds_counted.toLocaleString("en-IN")}</strong> real Indian equity
            funds — actual money, actual managers, returns computed from AMFI NAV.
          </p>
          <table className="bot-table">
            <thead>
              <tr>
                <th>Dimension</th>
                <th className="num">Bot</th>
                <th className="num">Reference</th>
                <th>Verdict</th>
              </tr>
            </thead>
            <tbody>
              {headline.scorecard.map((row) => (
                <tr key={row.dimension}>
                  <td>{row.dimension}</td>
                  <td className="num bot-mono-cell">{row.bot}</td>
                  <td className="num bot-mono-cell">{row.reference}</td>
                  <td><VerdictMark verdict={row.verdict} /></td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className={`bot-verdict ${answer.wins > answer.losses ? "bot-verdict-good" : "bot-verdict-warn"}`}>
            {answer.wins > answer.losses
              ? <Check size={15} aria-hidden />
              : <AlertTriangle size={15} aria-hidden />}
            <span>{answer.summary}</span>
          </p>
          <p className="bot-footnote">
            It sits at the {headline.percentile.toFixed(0)}th percentile of those funds on raw
            return. The median fund returned {formatPct(headline.fund_median_cagr)} through a{" "}
            {formatPct(headline.fund_median_drawdown, 1)} worst drawdown; this account returned{" "}
            {formatPct(headline.bot_cagr_pct)} through {formatPct(headline.bot_max_drawdown_pct, 1)}.
            Which of those you prefer is a question about what you can hold, not about arithmetic.
          </p>
        </section>
      ) : null}

      {headlineRun ? (
        <section>
          <h4>The account</h4>
          <p className="bot-section-note">
            {headlineRun.start} → {headlineRun.end}, starting from{" "}
            ₹{(headlineRun.starting_equity / 100000).toFixed(0)} lakh.{" "}
            {headlineRun.trades_taken.toLocaleString("en-IN")} trades taken,{" "}
            {headlineRun.signals_declined.toLocaleString("en-IN")} signals declined for want of a
            slot — which is the honest reason trade-level returns and account returns differ.
          </p>
          <EquityCurve run={headlineRun} />
          {headline?.uncertainty ? (
            <div className="bot-uncertainty">
              <p className="bot-kicker">How much of this is the edge, and how much is the draw</p>
              <p>{headline.uncertainty.note}</p>
              <div className="bot-uncertainty-stats">
                <div>
                  <span>90% range</span>
                  <strong>
                    {formatPct(headline.uncertainty.ci_low_cagr, 1)} to{" "}
                    {formatPct(headline.uncertainty.ci_high_cagr, 1)}
                  </strong>
                </div>
                <div>
                  <span>Resamples beating Nifty</span>
                  <strong>{headline.uncertainty.share_beating_index.toFixed(0)}%</strong>
                </div>
                <div>
                  <span>Resamples beating the fund median</span>
                  <strong>{headline.uncertainty.share_beating_fund_median.toFixed(0)}%</strong>
                </div>
              </div>
            </div>
          ) : null}
          <div className="bot-hero-stats bot-run-stats">
            <div><span>CAGR</span><strong>{formatPct(headlineRun.cagr_pct)}</strong></div>
            <div><span>Worst drawdown</span><strong>{formatPct(headlineRun.max_drawdown_pct, 1)}</strong></div>
            <div><span>Sharpe</span><strong>{headlineRun.sharpe.toFixed(2)}</strong></div>
            <div><span>Avg per trade</span><strong>{headlineRun.avg_r >= 0 ? "+" : ""}{headlineRun.avg_r.toFixed(2)}R</strong></div>
            <div><span>Payoff</span><strong>{headlineRun.payoff.toFixed(2)}</strong></div>
            <div><span>Win rate</span><strong>{headlineRun.win_rate.toFixed(0)}%</strong></div>
          </div>
        </section>
      ) : null}

      {sensitivity ? (
        <section>
          <h4>Across every defensible book structure, not just this one</h4>
          <p className="bot-section-note">{sensitivity.note}</p>
          <div className="bot-hero-stats bot-run-stats">
            <div>
              <span>Median CAGR</span>
              <strong>{formatPct(sensitivity.median_cagr)}</strong>
            </div>
            <div>
              <span>Middle half</span>
              <strong>
                {formatPct(sensitivity.p25_cagr, 1)}–{formatPct(sensitivity.p75_cagr, 1)}
              </strong>
            </div>
            <div>
              <span>Median drawdown</span>
              <strong>{formatPct(sensitivity.median_drawdown, 1)}</strong>
            </div>
            <div>
              <span>Beat Nifty</span>
              <strong>{sensitivity.share_beating_index.toFixed(0)}%</strong>
            </div>
            <div>
              <span>Beat the median fund</span>
              <strong>{sensitivity.share_beating_fund_median.toFixed(0)}%</strong>
            </div>
            <div>
              <span>Configurations</span>
              <strong>{sensitivity.configs.length}</strong>
            </div>
          </div>
          <p className="bot-footnote">
            Held-out return ranges from {formatPct(sensitivity.min_cagr, 1)} to{" "}
            {formatPct(sensitivity.max_cagr, 1)} across structures that are all defensible, so
            any figure quoted to a tenth of a point is describing one draw and calling it a
            measurement. Every structure beat the index; {sensitivity.share_beating_fund_median.toFixed(0)}%
            beat the median fund.
          </p>
        </section>
      ) : null}

      <section>
        <h4>What each design choice was worth</h4>
        <p className="bot-section-note">
          The same trade record, run four ways. The gap between the first two rows is the entire
          value of ranking candidates by the one validated entry-time condition; the third row is
          what happens when eligibility chases recent performance.
        </p>
        <table className="bot-table bot-table-compact">
          <thead>
            <tr>
              <th>Run</th><th className="num">Years</th><th className="num">CAGR</th>
              <th className="num">Max DD</th><th className="num">Sharpe</th>
              <th className="num">Trades</th><th className="num">Avg R</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((run) => (
              <tr key={run.label} className={run.label === "playbook_held_out" ? "bot-row-highlight" : ""}>
                <td>{RUN_LABELS[run.label] ?? run.label}</td>
                <td className="num">{run.years.toFixed(1)}</td>
                <td className={`num ${run.cagr_pct >= 0 ? "bot-expected" : "bot-negative"}`}>
                  {formatPct(run.cagr_pct)}
                </td>
                <td className="num">{formatPct(run.max_drawdown_pct, 1)}</td>
                <td className="num">{run.sharpe.toFixed(2)}</td>
                <td className="num">{run.trades_taken.toLocaleString("en-IN")}</td>
                <td className={`num ${run.avg_r >= 0 ? "bot-expected" : "bot-negative"}`}>
                  {run.avg_r >= 0 ? "+" : ""}{run.avg_r.toFixed(3)}R
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      {benchmark ? (
        <section>
          <h4>Why this is still not a fair fight</h4>
          <p className="bot-section-note">{benchmark.method}</p>
          <ul className="bot-caveats">
            {benchmark.caveats.map((caveat) => (
              <li key={caveat}>{caveat}</li>
            ))}
            <li>
              <strong>Three and a half years is a short window.</strong> It contains one broad
              uptrend and one correction. A result measured over it is a reading, not a track
              record, and the next bear market is not in this sample.
            </li>
          </ul>
        </section>
      ) : null}
    </div>
  );
}
