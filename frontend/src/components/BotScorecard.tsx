import { useEffect, useState } from "react";
import { AlertTriangle, Check, Info, Minus, X } from "lucide-react";

import {
  getBotCombined,
  getBotWalkforward,
  type BotCombined,
  type BotBenchmark,
  type BotLearning,
  type BotPortfolioRun,
  type BotRegimeTiming,
  type BotSensitivity,
  type BotWalkforward,
} from "../lib/api";

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
  const timing: BotRegimeTiming | null =
    learning.regime_timing?.available ? learning.regime_timing : null;
  const [walkforward, setWalkforward] = useState<BotWalkforward | null>(null);
  const [combined, setCombined] = useState<BotCombined | null>(null);

  useEffect(() => {
    let cancelled = false;
    getBotWalkforward()
      .then((result) => { if (!cancelled) setWalkforward(result); })
      // Absent on a host that has not run the evaluation. The rest of the page
      // still renders; it just loses the number that matters most.
      .catch(() => undefined);
    getBotCombined()
      .then((result) => { if (!cancelled) setCombined(result); })
      // Absent until scripts/combined_product.py has run. The page still
      // renders without it, minus the headline.
      .catch(() => undefined);
    return () => { cancelled = true; };
  }, []);

  // The fund comparison is only computed on the window cut to match the funds'
  // own, so read that one and nothing else. Any other window would be
  // comparing a different span against the same fund returns, which is the
  // mistake that produced this project's second false positive.
  const matched = combined
    ? Object.entries(combined).find(([name]) => name.startsWith("exact 3y"))?.[1]
    : undefined;
  const blend = matched?.blends?.["50/50"];
  const fundStats = matched?.benchmark;
  const timingAlt = matched?.alternatives?.["timing sleeve alone"];
  const bookAlt = matched?.alternatives?.["stock book alone"];

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

      {matched && blend && fundStats && timingAlt ? (
        <section className="bot-headline-good">
          <h4>Against 691 real fund managers, over the same three years</h4>
          <p className="bot-section-note">
            Every configuration is measured separately against the same funds. Reporting
            only one of them would let the choice be made after seeing which came out
            best — the same error as picking an exit rule off a grid.
          </p>

          <table className="bot-table">
            <thead>
              <tr>
                <th>Configuration</th>
                <th>Return</th>
                <th>Worst drawdown</th>
                <th>Funds beating it on <em>both</em></th>
              </tr>
            </thead>
            <tbody>
              <tr className="bot-row-highlight">
                <td>Regime timing — <em>when</em> to be exposed</td>
                <td>{formatPct(timingAlt.stats.cagr_pct)}</td>
                <td>{formatPct(timingAlt.stats.max_drawdown_pct)}</td>
                <td>{timingAlt.benchmark.funds_dominating} of {timingAlt.benchmark.funds_counted}</td>
              </tr>
              <tr>
                <td>Both sleeves, 50/50</td>
                <td>{formatPct(blend.cagr_pct)}</td>
                <td>{formatPct(blend.max_drawdown_pct)}</td>
                <td>{fundStats.funds_dominating} of {fundStats.funds_counted}</td>
              </tr>
              {bookAlt ? (
                <tr>
                  <td>Stock selection alone</td>
                  <td>{formatPct(bookAlt.stats.cagr_pct)}</td>
                  <td>{formatPct(bookAlt.stats.max_drawdown_pct)}</td>
                  <td>{bookAlt.benchmark.funds_dominating} of {bookAlt.benchmark.funds_counted}</td>
                </tr>
              ) : null}
              <tr>
                <td>Median professional fund</td>
                <td>{formatPct(fundStats.fund_median_cagr)}</td>
                <td>{formatPct(fundStats.fund_median_drawdown)}</td>
                <td>—</td>
              </tr>
            </tbody>
          </table>

          <p className="bot-verdict bot-verdict-good">
            <span>
              Regime timing beats the median fund on <strong>both</strong> return and
              drawdown, and {timingAlt.benchmark.funds_dominating} funds of{" "}
              {timingAlt.benchmark.funds_counted} beat it on both. That is the claim this
              system supports — and it belongs to deciding <em>when</em> to be exposed.
            </span>
          </p>

          {/* Selection is the weak half and gets stated at the same size as the
              strong one. A reader who leaves thinking the stock picking works
              has been misled by the layout rather than the number. */}
          <p className="bot-verdict bot-verdict-warn">
            <span>
              Stock selection does not. On its own it returns{" "}
              {bookAlt ? formatPct(bookAlt.stats.cagr_pct) : "—"} and{" "}
              {bookAlt ? bookAlt.benchmark.funds_dominating : "many"} funds beat it on both
              axes. The 50/50 blend is not the return-maximising choice either: it gives up{" "}
              {formatPct(timingAlt.stats.cagr_pct - blend.cagr_pct, 2)} of return to buy a
              shallower drawdown. Which you want depends on what you can sit through.
            </span>
          </p>

          {matched.blend_breaker_off ? (
            <p className="bot-footnote">
              The learning's share: the identical blend with the breaker switched off gives{" "}
              {formatPct(matched.blend_breaker_off.max_drawdown_pct)} drawdown against{" "}
              {formatPct(blend.max_drawdown_pct)}, on return within a rounding error. The
              self-improving part makes the holes shallower. It does not make more money.
            </p>
          ) : null}
          <p className="bot-footnote">
            Two things this cannot control for: the bot may sit in cash and a fund may not,
            which is a large structural advantage in a falling market and is not skill; and
            the bot's returns are simulated while the funds' are realised money, net of fees
            actually charged.
          </p>
        </section>
      ) : null}

      {timing ? (
        <section className="bot-headline-good">
          <h4>The one thing here that beats a professional</h4>
          <p className="bot-section-note">
            Not by picking stocks — that side of this system has no edge at all. By deciding{" "}
            <em>when to be exposed</em>: hold the Nifty 500 while the regime is{" "}
            {timing.rule.map((r) => r.replace(/_/g, " ")).join(", ")}, and park cash otherwise.
            The regime set was chosen on the first half of history from five candidates declared
            in advance; the held-out half was run once.
          </p>
          <table className="bot-table">
            <thead>
              <tr>
                <th>Strategy</th><th className="num">CAGR</th><th className="num">Worst drawdown</th>
                <th className="num">Return per drawdown</th>
              </tr>
            </thead>
            <tbody>
              <tr className="bot-row-highlight">
                <td><strong>Regime timing</strong></td>
                <td className="num bot-expected">{formatPct(timing.timed.cagr_pct)}</td>
                <td className="num bot-expected">{formatPct(timing.timed.max_drawdown_pct, 1)}</td>
                <td className="num bot-expected">{timing.timed.return_per_drawdown.toFixed(2)}</td>
              </tr>
              <tr>
                <td>Buy and hold Nifty 500</td>
                <td className="num">{formatPct(timing.buy_and_hold.cagr_pct)}</td>
                <td className="num">{formatPct(timing.buy_and_hold.max_drawdown_pct, 1)}</td>
                <td className="num">{timing.buy_and_hold.return_per_drawdown.toFixed(2)}</td>
              </tr>
              <tr>
                <td>Median professional fund</td>
                <td className="num">
                  {timing.fund_median_cagr === null ? "—" : formatPct(timing.fund_median_cagr)}
                </td>
                <td className="num">
                  {timing.fund_median_drawdown === null ? "—" : formatPct(timing.fund_median_drawdown, 1)}
                </td>
                <td className="num">
                  {timing.fund_median_cagr && timing.fund_median_drawdown
                    ? (timing.fund_median_cagr / Math.abs(timing.fund_median_drawdown)).toFixed(2)
                    : "—"}
                </td>
              </tr>
            </tbody>
          </table>
          <div className="bot-hero-stats bot-run-stats">
            <div><span>Time invested</span><strong>{timing.timed.exposure_pct.toFixed(0)}%</strong></div>
            <div><span>Switches</span><strong>{timing.timed.switches}</strong></div>
            <div><span>Average hold</span><strong>{timing.timed.mean_hold_days.toFixed(0)} days</strong></div>
            <div><span>Cash earns</span><strong>{timing.timed.cash_rate_pct.toFixed(0)}%</strong></div>
            <div><span>Tax charged</span><strong>{timing.timed.tax_pct.toFixed(0)}%</strong></div>
          </div>
          <details className="bot-details">
            <summary>Does it survive the assumptions? ({timing.sensitivity.length} combinations)</summary>
            <p className="bot-section-note">
              The cash rate is the number most open to argument, so it is varied alongside tax.
              An advantage that exists at only one assumption is not an advantage.
            </p>
            <table className="bot-table bot-table-compact">
              <thead>
                <tr>
                  <th className="num">Cash</th><th className="num">Tax</th><th className="num">CAGR</th>
                  <th className="num">Max DD</th><th>Beats fund</th><th>Beats index</th>
                </tr>
              </thead>
              <tbody>
                {timing.sensitivity.map((row) => (
                  <tr key={`${row.cash_rate_pct}-${row.tax_pct}`}>
                    <td className="num">{row.cash_rate_pct.toFixed(0)}%</td>
                    <td className="num">{row.tax_pct.toFixed(0)}%</td>
                    <td className={`num ${row.cagr_pct >= 0 ? "bot-expected" : "bot-negative"}`}>
                      {formatPct(row.cagr_pct)}
                    </td>
                    <td className="num">{formatPct(row.max_drawdown_pct, 1)}</td>
                    <td><VerdictMark verdict={row.beats_fund_median} /></td>
                    <td><VerdictMark verdict={row.beats_buy_and_hold} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </details>
          {timing.health ? (
            <div className={`bot-health bot-health-${timing.health.status}`}>
              <p className="bot-kicker">
                Is the rule still working? — the learning loop, pointed at the part that earns
              </p>
              <p>{timing.health.note}</p>
              <div className="bot-hero-stats bot-run-stats">
                <div>
                  <span>Days invested</span>
                  <strong>{timing.health.invested_avg_daily_pct.toFixed(3)}%</strong>
                </div>
                <div>
                  <span>Days out</span>
                  <strong>{timing.health.cash_avg_daily_pct.toFixed(3)}%</strong>
                </div>
                <div>
                  <span>Gap per day</span>
                  <strong className={timing.health.discrimination_pp > 0 ? "bot-expected" : "bot-negative"}>
                    {timing.health.discrimination_pp >= 0 ? "+" : ""}
                    {timing.health.discrimination_pp.toFixed(3)}pp
                  </strong>
                </div>
                <div>
                  <span>Switches judged</span>
                  <strong>{timing.health.completed_switches}</strong>
                </div>
              </div>
              <p className="bot-footnote">
                It watches whether the classifier still separates good tape from bad — not
                recent P&amp;L, which over a handful of switches says nothing. It can stand the
                rule down; it can never promote it for a good run, because a good stretch on a
                few decisions is the most seductive noise there is.
              </p>
            </div>
          ) : null}
          <p className="bot-verdict bot-verdict-warn">
            <AlertTriangle size={15} aria-hidden />
            <span>{timing.caveat}</span>
          </p>
        </section>
      ) : null}

      {walkforward ? (
        <section className="bot-headline-warning">
          <h4>Where stock selection stands: rebuilt every year, traded the next</h4>
          <p className="bot-section-note">
            Everything below this rests on a single train/test split, and its 3.8-year test
            window happens to contain 2023 — the one year this system worked. That year
            dominates every figure on the page. This is the honest alternative: the playbook
            rebuilt each January from prior data only, then used to trade that year, repeated{" "}
            {walkforward.years_evaluated} times.
          </p>
          <div className="bot-hero-stats bot-run-stats">
            <div><span>Bot, compounded</span><strong className="bot-negative">{formatPct(walkforward.bot_cagr)}</strong></div>
            <div><span>Index, compounded</span><strong>{walkforward.index_cagr === null ? "—" : formatPct(walkforward.index_cagr)}</strong></div>
            <div>
              <span>Years beating the index</span>
              <strong className="bot-negative">
                {walkforward.years_beating_index} of {walkforward.years_evaluated}
              </strong>
            </div>
          </div>
          <table className="bot-table bot-table-compact">
            <thead>
              <tr>
                <th>Year</th><th className="num">Cells</th><th className="num">Signals</th>
                <th className="num">Signal edge</th>
                <th className="num">Bot</th><th className="num">Index</th><th className="num">Excess</th>
              </tr>
            </thead>
            <tbody>
              {walkforward.years.map((row) => (
                <tr key={row.year}>
                  <td className="bot-mono">{row.year}</td>
                  <td className="num">{row.cells}</td>
                  <td className="num">{row.signals.toLocaleString("en-IN")}</td>
                  <td className={`num ${(row.signal_avg_r ?? 0) >= 0 ? "bot-expected" : "bot-negative"}`}>
                    {row.signal_avg_r === null
                      ? "—"
                      : `${row.signal_avg_r >= 0 ? "+" : ""}${row.signal_avg_r.toFixed(2)}R`}
                  </td>
                  <td className={`num ${row.bot_return_pct >= 0 ? "bot-expected" : "bot-negative"}`}>
                    {formatPct(row.bot_return_pct)}
                  </td>
                  <td className="num">{row.index_return_pct === null ? "—" : formatPct(row.index_return_pct)}</td>
                  <td className={`num ${(row.excess_pct ?? 0) >= 0 ? "bot-expected" : "bot-negative"}`}>
                    {row.excess_pct === null ? "—" : `${row.excess_pct >= 0 ? "+" : ""}${row.excess_pct.toFixed(1)}`}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="bot-verdict bot-verdict-warn">
            <AlertTriangle size={15} aria-hidden />
            <span>
              Under this evaluation the system does not have a durable edge: it compounds at{" "}
              {formatPct(walkforward.bot_cagr)} a year against the market's{" "}
              {walkforward.index_cagr === null ? "—" : formatPct(walkforward.index_cagr)}, and
              beats the index in {walkforward.years_beating_index} of{" "}
              {walkforward.years_evaluated} years. The <strong>signal edge</strong> column is the
              clinching one: it ignores slots, sizing and capital entirely, so it cannot be blamed
              on how the book is run — and it is negative in{" "}
              {walkforward.years_evaluated - walkforward.positive_signal_years} of{" "}
              {walkforward.years_evaluated} years, averaging{" "}
              {walkforward.mean_signal_edge === null
                ? "—"
                : `${walkforward.mean_signal_edge >= 0 ? "+" : ""}${walkforward.mean_signal_edge.toFixed(2)}R`}
              . The cells the playbook selects go on to lose money. Read everything below with
              that in front of it.
            </span>
          </p>
        </section>
      ) : null}

      {answer && headline ? (
        <section>
          <h4>The single-split view, for contrast</h4>
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

          {sensitivity.by_period?.length ? (
            <>
              <h5 className="bot-subhead">Year by year, which matters more than the headline</h5>
              <p className="bot-section-note">
                The account is lumpy. A single CAGR is mostly a statement about which years the
                window contains — the figure above covers 3.8 years, and over the three years
                the fund comparison actually uses it is closer to 5.6%. Both are shown because
                neither is "the" number.
              </p>
              <table className="bot-table bot-table-compact">
                <thead>
                  <tr>
                    <th>Twelve months to</th>
                    <th className="num">Bot (median)</th>
                    <th className="num">Middle half</th>
                    <th className="num">Nifty</th>
                    <th className="num">Difference</th>
                  </tr>
                </thead>
                <tbody>
                  {sensitivity.by_period.map((row) => (
                    <tr key={row.period}>
                      <td>{row.period}</td>
                      <td className={`num ${row.bot_median_cagr >= 0 ? "bot-expected" : "bot-negative"}`}>
                        {formatPct(row.bot_median_cagr)}
                      </td>
                      <td className="num bot-cell-thin">
                        {formatPct(row.bot_p25_cagr, 1)} – {formatPct(row.bot_p75_cagr, 1)}
                      </td>
                      <td className="num">{row.index_cagr === null ? "—" : formatPct(row.index_cagr)}</td>
                      <td className={`num ${(row.excess_vs_index ?? 0) >= 0 ? "bot-expected" : "bot-negative"}`}>
                        {row.excess_vs_index === null
                          ? "—"
                          : `${row.excess_vs_index >= 0 ? "+" : ""}${row.excess_vs_index.toFixed(1)}`}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          ) : null}
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
