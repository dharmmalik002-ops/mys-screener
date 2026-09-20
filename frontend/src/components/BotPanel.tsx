import { useCallback, useEffect, useMemo, useState } from "react";
import {
  AlertTriangle,
  Ban,
  CheckCircle2,
  CircleDashed,
  Info,
  RefreshCw,
  ShieldAlert,
  TrendingDown,
  TrendingUp,
} from "lucide-react";

import {
  getBotBacktest,
  getBotLearning,
  getBotSignals,
  type BotBacktest,
  type BotCandidate,
  type BotLearning,
  type BotPlaybook,
  type BotSignals,
} from "../lib/api";
import { EvolutionView, LearningView } from "./BotLearning";
import { ScorecardView } from "./BotScorecard";
import { Panel } from "./Panel";

import "./BotPanel.css";

/* The Bot page.

   Four views, in the order a decision actually gets made: what the tape is
   doing today and what that implies, the regime->strategy map behind it, the
   evidence the map was built from, and what the evidence cannot tell you.

   The last view is not an appendix. Everything here is a backtest, and a
   backtest is a statement about the past wearing the costume of a prediction.
   The limitations view is what keeps the costume visible, so it gets equal
   billing with the numbers rather than a footnote nobody scrolls to. */

type BotView = "today" | "scorecard" | "playbook" | "learning" | "evolution" | "evidence" | "limits";

// Ordered the way a decision gets made: what to do now, the rules behind it,
// what the trade record taught, how that view has shifted, the underlying
// study, and finally what none of it can tell you.
const VIEWS: Array<{ id: BotView; label: string; hint: string }> = [
  { id: "today", label: "Today", hint: "Current regime, stance and candidates" },
  // Second on purpose: "is this any good?" is the question everything else
  // only supports, and it is answered against real fund managers.
  { id: "scorecard", label: "Scorecard", hint: "The account, measured against real fund managers" },
  { id: "playbook", label: "Playbook", hint: "Which setups are cleared in which regime" },
  { id: "learning", label: "Learning", hint: "What the trade record says works, and what it cost" },
  { id: "evolution", label: "Evolution", hint: "How the bot's view of each strategy has changed" },
  { id: "evidence", label: "Evidence", hint: "The strategy × regime study behind the playbook" },
  { id: "limits", label: "What this can't tell you", hint: "Survivorship, macro and the honest caveats" },
];

const DEFAULT_EQUITY = 1_000_000;

function formatR(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}R`;
}

function formatMoney(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  return `₹${Math.round(value).toLocaleString("en-IN")}`;
}

function stanceTone(stance: string | undefined): string {
  if (stance === "engaged") return "good";
  if (stance === "selective") return "warn";
  return "flat";
}

function StanceBadge({ stance }: { stance: string | undefined }) {
  const tone = stanceTone(stance);
  const Icon = stance === "engaged" ? TrendingUp : stance === "selective" ? CircleDashed : Ban;
  const label = stance === "engaged" ? "Engaged" : stance === "selective" ? "Selective" : "Stand down";
  return (
    <span className={`bot-badge bot-badge-${tone}`}>
      <Icon size={13} aria-hidden /> {label}
    </span>
  );
}

function VerdictPill({ verdict }: { verdict: string }) {
  const tone =
    verdict === "confirmed" ? "good"
      : verdict === "confirmed_weak" ? "warn"
        : verdict === "decayed" ? "bad"
          : "flat";
  const label =
    verdict === "confirmed" ? "Confirmed"
      : verdict === "confirmed_weak" ? "Confirmed (weak)"
        : verdict === "decayed" ? "Decayed"
          : verdict === "negative" ? "No edge"
            : "Too few trades";
  return <span className={`bot-pill bot-pill-${tone}`}>{label}</span>;
}

/* --- Today ---------------------------------------------------------------- */

function TodayView({
  signals,
  equity,
  onEquityChange,
  onRefresh,
  refreshing,
}: {
  signals: BotSignals | null;
  equity: number;
  onEquityChange: (value: number) => void;
  onRefresh: () => void;
  refreshing: boolean;
}) {
  // A live scan rebuilds indicators for ~1,600 symbols and takes about 20
  // seconds cold. Showing "no data" during that is a lie the user acts on, so
  // the in-flight state is its own branch.
  if (!signals && refreshing) {
    return (
      <p className="bot-empty">
        <RefreshCw size={14} aria-hidden className="bot-spin" /> Scanning the universe for
        today's candidates…
      </p>
    );
  }
  if (!signals) {
    return (
      <div className="bot-alert bot-alert-warn">
        <AlertTriangle size={15} aria-hidden />
        <span>
          Could not load today's signals. The study on the other tabs is unaffected.
        </span>
      </div>
    );
  }

  const regime = signals.regime;
  const macro = signals.macro;
  const standDown = signals.stance === "stand_down" || (signals.candidates?.length ?? 0) === 0;

  return (
    <div className="bot-today">
      {signals.stale ? (
        <div className="bot-alert bot-alert-warn">
          <AlertTriangle size={15} aria-hidden />
          <span>
            These signals are {signals.age_days} days old. They describe a market that has
            moved on — rebuild before acting on them.
          </span>
        </div>
      ) : null}

      <div className="bot-hero">
        <div className="bot-hero-main">
          <p className="bot-kicker">Market regime · {regime?.day ?? "—"}</p>
          <h3>{regime?.regime_label ?? "Unknown"}</h3>
          <p className="bot-hero-note">
            {signals.playbook?.note ?? ""}
          </p>
          <div className="bot-hero-stats">
            <div>
              <span>Breadth above 200 DMA</span>
              <strong>{regime ? `${regime.breadth_above_200dma.toFixed(0)}%` : "—"}</strong>
            </div>
            <div>
              <span>From 52-week high</span>
              <strong>{regime ? `${regime.pct_from_52w_high.toFixed(1)}%` : "—"}</strong>
            </div>
            <div>
              <span>Volatility</span>
              <strong>{regime?.volatility_band ?? "—"}</strong>
            </div>
            <div>
              <span>In this regime</span>
              <strong>{regime ? `${regime.regime_age} sessions` : "—"}</strong>
            </div>
          </div>
        </div>
        <div className="bot-hero-side">
          <StanceBadge stance={signals.stance} />
          {macro ? (
            <div className={`bot-macro-gate bot-macro-${macro.stance}`}>
              <p className="bot-kicker">External tape</p>
              <strong>{macro.stance}</strong>
              <p className="bot-macro-size">Position size × {macro.size_multiplier.toFixed(2)}</p>
              {macro.headwinds.length ? (
                <p className="bot-macro-list">
                  <TrendingDown size={12} aria-hidden /> {macro.headwinds.join(", ")}
                </p>
              ) : null}
              {macro.tailwinds.length ? (
                <p className="bot-macro-list bot-macro-good">
                  <TrendingUp size={12} aria-hidden /> {macro.tailwinds.join(", ")}
                </p>
              ) : null}
            </div>
          ) : null}
        </div>
      </div>

      <p className="bot-message">{signals.message}</p>

      <div className="bot-controls">
        <label>
          Book size
          <input
            type="number"
            min={10000}
            step={50000}
            value={equity}
            onChange={(event) => onEquityChange(Number(event.target.value) || DEFAULT_EQUITY)}
          />
        </label>
        <button type="button" onClick={onRefresh} disabled={refreshing} className="bot-refresh">
          <RefreshCw size={14} aria-hidden className={refreshing ? "bot-spin" : undefined} />
          {refreshing ? "Scanning…" : "Rescan"}
        </button>
        {signals.source ? (
          <span className="bot-source">
            {signals.source === "live" ? "Live scan" : "Pre-computed"}
          </span>
        ) : null}
      </div>

      {standDown ? (
        <div className="bot-standdown">
          <ShieldAlert size={22} aria-hidden />
          <div>
            <h4>No trades today</h4>
            <p>{signals.playbook?.rationale ?? signals.message}</p>
            <p className="bot-standdown-note">
              Sitting out is the position the evidence supports. A bot that always finds
              something to buy is not being clever — it is being agreeable.
            </p>
          </div>
        </div>
      ) : (
        <div className="bot-table-wrap">
          <table className="bot-table">
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Setup</th>
                <th className="num">Close</th>
                <th className="num">Stop</th>
                <th className="num">Risk</th>
                <th className="num">Qty</th>
                <th className="num">Position</th>
                <th className="num">Vol adj</th>
                <th className="num">Edge</th>
              </tr>
            </thead>
            <tbody>
              {signals.candidates.map((candidate: BotCandidate) => (
                <tr key={`${candidate.symbol}-${candidate.strategy}`}>
                  <td className="bot-symbol">{candidate.symbol}</td>
                  <td>{candidate.strategy_label}</td>
                  <td className="num">{candidate.close.toFixed(2)}</td>
                  <td className="num bot-stop">{candidate.stop.toFixed(2)}</td>
                  <td className="num">{candidate.risk_pct.toFixed(1)}%</td>
                  <td className="num">{candidate.sizing.shares.toLocaleString("en-IN")}</td>
                  <td className="num">{formatMoney(candidate.sizing.position_value)}</td>
                  <td className="num" title={candidate.volatility_bucket ? `ATR bucket: ${candidate.volatility_bucket}` : undefined}>
                    {candidate.volatility_adjustment_r === undefined
                      ? "—"
                      : formatR(candidate.volatility_adjustment_r)}
                  </td>
                  <td className="num bot-expected">
                    {formatR(candidate.edge_score_r ?? candidate.expected_r)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="bot-footnote">
            "Edge" is the held-out average for that setup in this regime, plus the volatility
            adjustment — the one entry-time condition that survived validation. Both parts are
            shown so the order can be checked. None of it forecasts <em>this</em> trade:
            outcomes scatter enormously around the average, which only means something across
            many trades.
          </p>
        </div>
      )}
    </div>
  );
}

/* --- Playbook ------------------------------------------------------------- */

function PlaybookView({ backtest }: { backtest: BotBacktest }) {
  const current = backtest.current_regime?.regime;
  return (
    <div className="bot-playbooks">
      {backtest.playbooks.map((book: BotPlaybook) => (
        <article
          key={book.regime}
          className={`bot-playbook ${book.regime === current ? "bot-playbook-current" : ""}`}
        >
          <header>
            <div>
              <h4>
                {book.label}
                {book.regime === current ? <span className="bot-now">now</span> : null}
              </h4>
              <p>{book.note}</p>
            </div>
            <StanceBadge stance={book.stance} />
          </header>
          <p className="bot-playbook-rationale">{book.rationale}</p>
          {book.entries.length ? (
            <table className="bot-table bot-table-compact">
              <thead>
                <tr>
                  <th>Setup</th>
                  <th>Evidence</th>
                  <th className="num">Held-out</th>
                  <th className="num">Trades</th>
                  <th className="num">Win</th>
                  <th className="num">Risk/trade</th>
                </tr>
              </thead>
              <tbody>
                {book.entries.map((entry) => (
                  <tr key={entry.strategy}>
                    <td>{entry.label}</td>
                    <td><VerdictPill verdict={entry.verdict} /></td>
                    <td className="num bot-expected">{formatR(entry.out_sample_r)}</td>
                    <td className="num">{entry.out_sample_trades.toLocaleString("en-IN")}</td>
                    <td className="num">{entry.win_rate.toFixed(0)}%</td>
                    <td className="num">{entry.risk_per_trade_pct.toFixed(2)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <p className="bot-playbook-empty">
              Nothing cleared validation here. The book stays in cash.
            </p>
          )}
        </article>
      ))}
    </div>
  );
}

/* --- Evidence ------------------------------------------------------------- */

function EvidenceView({ backtest }: { backtest: BotBacktest }) {
  const regimes = backtest.regime_catalogue.map((r) => r.id);
  const strategies = backtest.strategy_catalogue;
  const cellFor = useMemo(() => {
    const map = new Map<string, (typeof backtest.matrix)[number]>();
    backtest.matrix.forEach((cell) => map.set(`${cell.strategy}|${cell.regime}`, cell));
    return map;
  }, [backtest.matrix]);

  const confirmed = backtest.validated.filter((v) => v.verdict === "confirmed" || v.verdict === "confirmed_weak");
  const decayed = backtest.validated.filter((v) => v.verdict === "decayed");

  return (
    <div className="bot-evidence">
      <section>
        <h4>How the study was run</h4>
        <ul className="bot-facts">
          <li>
            <strong>{backtest.coverage.trades_resolved.toLocaleString("en-IN")}</strong> simulated trades
            across <strong>{backtest.coverage.symbols_with_trades.toLocaleString("en-IN")}</strong> stocks
          </li>
          <li>
            <strong>{backtest.coverage.sessions.toLocaleString("en-IN")}</strong> sessions
            ({backtest.coverage.first_session} → {backtest.coverage.last_session})
          </li>
          <li>
            Held-out period begins <strong>{backtest.coverage.validation_split}</strong> — everything
            after that date was never used to choose anything
          </li>
          <li>Entries fill at the next session's open; stops that gap through fill at the open</li>
          <li>Costs charged on every trade: STT, stamp duty, exchange, GST and 15 bps slippage each way</li>
        </ul>
      </section>

      <section>
        <h4>Average R by setup and regime</h4>
        <p className="bot-section-note">
          Full-sample averages. A dot means fewer than 30 trades — too few to say anything.
          An asterisk means the cell survived multiple-testing correction across all 60 cells.
        </p>
        <div className="bot-matrix-wrap">
          <table className="bot-matrix">
            <thead>
              <tr>
                <th>Setup</th>
                {regimes.map((regime) => (
                  <th key={regime} className="num">
                    {backtest.regime_catalogue.find((r) => r.id === regime)?.label ?? regime}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {strategies.map((strategy) => (
                <tr key={strategy.id}>
                  <td className="bot-matrix-label" title={strategy.thesis}>{strategy.label}</td>
                  {regimes.map((regime) => {
                    const cell = cellFor.get(`${strategy.id}|${regime}`);
                    if (!cell || !cell.reportable) {
                      return <td key={regime} className="num bot-cell-thin">·</td>;
                    }
                    const tone = cell.avg_r > 0.15 ? "pos" : cell.avg_r < -0.05 ? "neg" : "flat";
                    return (
                      <td key={regime} className={`num bot-cell bot-cell-${tone}`} title={`${cell.trades} trades`}>
                        {cell.avg_r >= 0 ? "+" : ""}{cell.avg_r.toFixed(2)}
                        {cell.significant ? <sup>*</sup> : null}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section>
        <h4>What survived the held-out period ({confirmed.length})</h4>
        <p className="bot-section-note">
          Positive before the split date and still positive after it. Only these earn capital.
        </p>
        <table className="bot-table bot-table-compact">
          <thead>
            <tr>
              <th>Setup</th><th>Regime</th><th>Evidence</th>
              <th className="num">In-sample</th><th className="num">Held-out</th><th className="num">Trades</th>
            </tr>
          </thead>
          <tbody>
            {confirmed.map((cell) => (
              <tr key={`${cell.strategy}-${cell.regime}`}>
                <td>{strategies.find((s) => s.id === cell.strategy)?.label ?? cell.strategy}</td>
                <td>{backtest.regime_catalogue.find((r) => r.id === cell.regime)?.label ?? cell.regime}</td>
                <td><VerdictPill verdict={cell.verdict} /></td>
                <td className="num">{formatR(cell.in_sample?.avg_r)}</td>
                <td className="num bot-expected">{formatR(cell.out_sample?.avg_r)}</td>
                <td className="num">{cell.out_sample?.trades.toLocaleString("en-IN") ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section>
        <h4>What fell apart ({decayed.length})</h4>
        <p className="bot-section-note">
          Strong before the split, negative after it. These are the ones a backtest without a
          held-out period would have sold you, and they are shown for exactly that reason.
        </p>
        <table className="bot-table bot-table-compact">
          <thead>
            <tr>
              <th>Setup</th><th>Regime</th>
              <th className="num">In-sample</th><th className="num">Held-out</th><th className="num">Trades</th>
            </tr>
          </thead>
          <tbody>
            {decayed.map((cell) => (
              <tr key={`${cell.strategy}-${cell.regime}`}>
                <td>{strategies.find((s) => s.id === cell.strategy)?.label ?? cell.strategy}</td>
                <td>{backtest.regime_catalogue.find((r) => r.id === cell.regime)?.label ?? cell.regime}</td>
                <td className="num">{formatR(cell.in_sample?.avg_r)}</td>
                <td className="num bot-negative">{formatR(cell.out_sample?.avg_r)}</td>
                <td className="num">{cell.out_sample?.trades.toLocaleString("en-IN") ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section>
        <h4>How often each regime occurs</h4>
        <div className="bot-regime-bars">
          {backtest.regime_distribution.map((row) => (
            <div key={row.regime} className="bot-regime-bar">
              <span className="bot-regime-name">{row.label}</span>
              <div className="bot-regime-track">
                <div className={`bot-regime-fill bot-regime-${row.regime}`} style={{ width: `${row.pct_of_history}%` }} />
              </div>
              <span className="bot-regime-pct">{row.pct_of_history.toFixed(1)}%</span>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

/* --- Limits --------------------------------------------------------------- */

function LimitsView({ backtest }: { backtest: BotBacktest }) {
  const survivorship = backtest.survivorship;
  const macro = backtest.macro;
  const material = macro.filter((m) => m.material);

  return (
    <div className="bot-limits">
      <div className="bot-alert bot-alert-info">
        <Info size={15} aria-hidden />
        <span>
          Everything on the other three tabs is a measurement of the past. None of it is a
          forecast, and none of it is advice. What follows is what the measurement cannot see.
        </span>
      </div>

      <section>
        <h4>Survivorship</h4>
        <p className="bot-section-note">{survivorship.limitation}</p>
        <table className="bot-table bot-table-compact">
          <thead>
            <tr>
              <th>Era</th><th className="num">Universe coverage</th>
              <th className="num">Trades</th><th className="num">Avg R</th>
            </tr>
          </thead>
          <tbody>
            {survivorship.eras.map((era) => (
              <tr key={era.era}>
                <td>{era.era}</td>
                <td className="num">{era.coverage_pct.toFixed(0)}%</td>
                <td className="num">{era.trades.toLocaleString("en-IN")}</td>
                <td className="num">{formatR(era.avg_r)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <p className={`bot-verdict ${(survivorship.coverage_performance_correlation ?? 0) > 0 ? "bot-verdict-good" : "bot-verdict-warn"}`}>
          {(survivorship.coverage_performance_correlation ?? 0) > 0
            ? <CheckCircle2 size={15} aria-hidden />
            : <AlertTriangle size={15} aria-hidden />}
          <span>{survivorship.verdict}</span>
        </p>
      </section>

      <section>
        <h4>Does the outside world matter?</h4>
        <p className="bot-section-note">
          Each external series split into headwind and tailwind sessions, holding the regime
          constant so the comparison is not just rediscovering that bull markets pay better.
          {material.length
            ? ` ${material.length} of ${macro.length} carry information the regime label does not.`
            : " None of them carried information beyond the regime label."}
        </p>
        <table className="bot-table bot-table-compact">
          <thead>
            <tr>
              <th>Series</th><th className="num">Tailwind</th><th className="num">Headwind</th>
              <th className="num">Difference</th><th>Reads</th>
            </tr>
          </thead>
          <tbody>
            {macro.map((finding) => (
              <tr key={finding.series} className={finding.material ? "" : "bot-row-muted"}>
                <td>{finding.label}</td>
                <td className="num">{formatR(finding.avg_r_tailwind)}</td>
                <td className="num">{formatR(finding.avg_r_headwind)}</td>
                <td className={`num ${finding.material ? "bot-expected" : ""}`}>
                  {finding.difference >= 0 ? "+" : ""}{finding.difference.toFixed(2)}
                </td>
                <td className="bot-macro-verdict">{finding.material ? "Matters" : "No material effect"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section>
        <h4>The things that will still bite you</h4>
        <ul className="bot-caveats">
          <li>
            <strong>A backtest is not a track record.</strong> Every number here was produced by
            code that knew the rules in advance. Live, you will hesitate, size differently, and
            skip the trade that turns out to be the one that paid for the month.
          </li>
          <li>
            <strong>Regimes are labelled with hindsight-free rules, but they still lag.</strong>
            The classifier needs a few sessions to confirm a change, so the turn from bull to
            correction is recognised after it has begun, not as it begins.
          </li>
          <li>
            <strong>The edge is thin and slow.</strong> A held-out average near +0.4R with a ~30%
            win rate means long losing streaks are normal, not a sign something broke. Seven
            consecutive losers is an ordinary week in this profile.
          </li>
          <li>
            <strong>Costs were modelled, not incurred.</strong> Real slippage on a gap-up entry in
            a mid cap is worse than 15 bps, and the thinner the stock the worse it gets.
          </li>
          <li>
            <strong>Nothing here accounts for you.</strong> Position sizing assumes you take every
            signal the playbook clears, at the size it states, without exception.
          </li>
        </ul>
      </section>
    </div>
  );
}

/* --- Shell ---------------------------------------------------------------- */

export function BotPanel() {
  const [view, setView] = useState<BotView>("today");
  const [backtest, setBacktest] = useState<BotBacktest | null>(null);
  const [learning, setLearning] = useState<BotLearning | null>(null);
  const [signals, setSignals] = useState<BotSignals | null>(null);
  const [equity, setEquity] = useState(DEFAULT_EQUITY);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadSignals = useCallback(async (book: number) => {
    setRefreshing(true);
    try {
      setSignals(await getBotSignals(book));
    } catch (err) {
      // The study is the more important half of this page; a signals failure
      // must not blank the evidence the user came to read.
      setSignals(null);
      setError((previous) => previous ?? (err instanceof Error ? err.message : "Could not load signals."));
    } finally {
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setError(null);
      try {
        const study = await getBotBacktest();
        if (cancelled) return;
        setBacktest(study);
        // The learning payload is a slice of the same artifact. Fetched
        // separately so an older artifact without it still renders the rest
        // of the page rather than failing the whole load.
        setLearning(study.learning?.available ? study.learning : null);
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Could not load the backtest.");
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
      if (!cancelled) await loadSignals(DEFAULT_EQUITY);
    })();
    return () => {
      cancelled = true;
    };
  }, [loadSignals]);

  const subtitle = backtest
    ? `${backtest.coverage.trades_resolved.toLocaleString("en-IN")} simulated trades · ${backtest.coverage.first_session} → ${backtest.coverage.last_session}`
    : "Regime-aware strategy selection";

  return (
    <div className="workspace-grid-solo">
      <Panel title="Trading Bot" subtitle={subtitle}>
        <nav className="bot-views" role="tablist">
          {VIEWS.map((item) => (
            <button
              key={item.id}
              type="button"
              role="tab"
              aria-selected={view === item.id}
              className={`bot-view-tab ${view === item.id ? "is-active" : ""}`}
              onClick={() => setView(item.id)}
              title={item.hint}
            >
              {item.label}
            </button>
          ))}
        </nav>

        {loading ? <p className="bot-empty">Loading the study…</p> : null}
        {error && !backtest ? (
          <div className="bot-alert bot-alert-warn">
            <AlertTriangle size={15} aria-hidden />
            <span>{error}</span>
          </div>
        ) : null}

        {!loading && backtest ? (
          <>
            {view === "today" ? (
              <TodayView
                signals={signals}
                equity={equity}
                onEquityChange={(value) => {
                  setEquity(value);
                  void loadSignals(value);
                }}
                onRefresh={() => void loadSignals(equity)}
                refreshing={refreshing}
              />
            ) : null}
            {view === "playbook" ? <PlaybookView backtest={backtest} /> : null}
            {view === "scorecard" ? (
              learning
                ? <ScorecardView learning={learning} />
                : <p className="bot-empty">No account simulation in this artifact yet.</p>
            ) : null}
            {view === "learning" ? (
              learning
                ? <LearningView learning={learning} />
                : <p className="bot-empty">This backtest predates the learning layer. Rerun the backtest to populate it.</p>
            ) : null}
            {view === "evolution" ? (
              learning
                ? <EvolutionView learning={learning} />
                : <p className="bot-empty">No evolution history in this artifact yet.</p>
            ) : null}
            {view === "evidence" ? <EvidenceView backtest={backtest} /> : null}
            {view === "limits" ? <LimitsView backtest={backtest} /> : null}
          </>
        ) : null}
      </Panel>
    </div>
  );
}

export default BotPanel;
