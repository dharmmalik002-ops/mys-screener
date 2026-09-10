import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  getStudyBars,
  getStudyDeck,
  getStudyReveal,
  type StudyBar,
  type StudyCard,
  type StudyDeckResponse,
  type StudyReveal,
} from "../lib/api";
import { StudyChart } from "./StudyChart";

import "./StudyPanel.css";

const LOG_KEY = "study-drill-log:v1";

type Action = "buy" | "pass";
type Phase = "call" | "stepping" | "done";

type LogEntry = {
  cardId: string;
  setup: string;
  symbol: string;
  gradedAt: string;
  action: Action;
  stop: number | null;
  riskPct: number | null;
  /** R-multiple against the user's own stop. Null for a pass. */
  r: number | null;
  officialResult: string;
};

function readLog(): LogEntry[] {
  if (typeof window === "undefined") return [];
  try {
    const raw = window.localStorage.getItem(LOG_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? (parsed as LogEntry[]) : [];
  } catch {
    return [];
  }
}

function writeLog(entries: LogEntry[]): void {
  try {
    window.localStorage.setItem(LOG_KEY, JSON.stringify(entries.slice(-2000)));
  } catch {
    /* quota or private mode — the drill still works, it just stops scoring */
  }
}

/**
 * Grade the trade the user actually placed, against their own stop.
 *
 * Deliberately not the deck's `result`: that is the fixed 3%/5%/10-session rule
 * the regime brief uses, which says nothing about whether *this* stop was well
 * chosen. Walking the same bars against the user's line is the whole point of
 * making them place one.
 */
function gradeBuy(
  entry: number,
  stop: number,
  forward: StudyBar[],
): { r: number; exit: number; exitIndex: number; stoppedOut: boolean } | null {
  const risk = entry - stop;
  if (!(risk > 0) || !forward.length) return null;
  for (let i = 0; i < forward.length; i += 1) {
    if (forward[i].low <= stop) {
      return { r: -1, exit: stop, exitIndex: i, stoppedOut: true };
    }
  }
  const last = forward[forward.length - 1];
  return { r: (last.close - entry) / risk, exit: last.close, exitIndex: forward.length - 1, stoppedOut: false };
}

const fmt = (v: number | null | undefined, digits = 2) =>
  v == null || !Number.isFinite(v) ? "—" : v.toLocaleString("en-IN", { minimumFractionDigits: digits, maximumFractionDigits: digits });

const signed = (v: number, digits = 2) => `${v >= 0 ? "+" : ""}${v.toFixed(digits)}`;

export function StudyPanel() {
  const [deck, setDeck] = useState<StudyDeckResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [setupFilter, setSetupFilter] = useState<string | null>(null);

  const [index, setIndex] = useState(0);
  const [phase, setPhase] = useState<Phase>("call");
  const [stop, setStop] = useState<number | null>(null);
  const [revealed, setRevealed] = useState(0);
  const [reveal, setReveal] = useState<StudyReveal | null>(null);
  const [action, setAction] = useState<Action | null>(null);
  const [log, setLog] = useState<LogEntry[]>(() => readLog());
  const loggedRef = useRef<Set<string>>(new Set());
  // Bars are fetched per card rather than with the deal. Keyed by card id so
  // stepping back to an already-seen card does not refetch, and so a slow
  // response for card 3 can never paint itself onto card 4.
  const [barsByCard, setBarsByCard] = useState<Record<string, StudyBar[]>>({});

  const load = useCallback((setup: string | null) => {
    setLoading(true);
    setError(null);
    getStudyDeck({ count: 20, setup })
      .then((payload) => {
        setDeck(payload);
        setIndex(0);
      })
      .catch((err) => setError(err instanceof Error ? err.message : String(err)))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    load(setupFilter);
  }, [load, setupFilter]);

  const cards: StudyCard[] = deck?.cards ?? [];
  const card = cards[index] ?? null;

  // Reset the per-card state during render rather than in an effect. An effect
  // runs after paint, which leaves one frame where the new card's context bars
  // are drawn against the previous card's forward bars — different symbols,
  // timestamps that jump backwards, and a chart library assertion that blanks
  // the page. Adjusting state during render means that frame never exists.
  const [shownCardId, setShownCardId] = useState<string | null>(card?.id ?? null);
  if ((card?.id ?? null) !== shownCardId) {
    setShownCardId(card?.id ?? null);
    setPhase("call");
    setStop(null);
    setRevealed(0);
    setReveal(null);
    setAction(null);
  }

  const fetchBars = useCallback((cardId: string) => {
    setBarsByCard((prev) => {
      if (prev[cardId]) return prev;
      getStudyBars(cardId)
        .then((payload) => setBarsByCard((current) => ({ ...current, [cardId]: payload.bars })))
        .catch(() => {
          /* one unreadable card should not take the drill down */
        });
      return prev;
    });
  }, []);

  useEffect(() => {
    if (card) fetchBars(card.id);
    // Warm the next card while this one is being read, so the drill does not
    // stall for a network round trip on every "next".
    const upcoming = cards[index + 1];
    if (upcoming) fetchBars(upcoming.id);
  }, [card, cards, index, fetchBars]);

  const bars = card ? barsByCard[card.id] ?? [] : [];
  const forward = reveal?.forward_bars ?? [];
  const grade = useMemo(
    () => (card && stop != null && forward.length ? gradeBuy(card.entry, stop, forward) : null),
    [card, stop, forward],
  );

  // Once stepping passes the bar that broke the stop, the trade is over.
  const stoppedAt = grade?.stoppedOut ? grade.exitIndex : null;
  const finished =
    phase === "stepping" && (revealed >= forward.length || (stoppedAt != null && revealed > stoppedAt));

  useEffect(() => {
    if (finished) setPhase("done");
  }, [finished]);

  const riskPct = card && stop != null && stop < card.entry ? ((card.entry - stop) / card.entry) * 100 : null;

  const commit = useCallback(
    (chosen: Action) => {
      if (!card || phase !== "call") return;
      if (chosen === "buy" && (stop == null || stop >= card.entry)) return;
      setAction(chosen);
      getStudyReveal(card.id)
        .then((payload) => {
          setReveal(payload);
          // A pass has nothing to step through — show the whole outcome at once.
          if (chosen === "pass") {
            setRevealed(payload.forward_bars.length);
            setPhase("done");
          } else {
            setRevealed(0);
            setPhase("stepping");
          }
        })
        .catch((err) => setError(err instanceof Error ? err.message : String(err)));
    },
    [card, phase, stop],
  );

  // Record the grade exactly once per card, when the answer is fully known.
  useEffect(() => {
    if (phase !== "done" || !card || !reveal || !action) return;
    if (loggedRef.current.has(card.id)) return;
    loggedRef.current.add(card.id);
    const entry: LogEntry = {
      cardId: card.id,
      setup: card.setup,
      symbol: card.symbol,
      gradedAt: new Date().toISOString(),
      action,
      stop: action === "buy" ? stop : null,
      riskPct: action === "buy" ? riskPct : null,
      r: action === "buy" && grade ? Number(grade.r.toFixed(3)) : null,
      officialResult: reveal.result,
    };
    setLog((prev) => {
      const next = [...prev.filter((e) => e.cardId !== entry.cardId), entry];
      writeLog(next);
      return next;
    });
  }, [phase, card, reveal, action, stop, riskPct, grade]);

  const step = useCallback(() => {
    if (phase !== "stepping") return;
    setRevealed((n) => Math.min(n + 1, forward.length));
  }, [phase, forward.length]);

  const next = useCallback(() => {
    setIndex((i) => Math.min(i + 1, Math.max(0, cards.length - 1)));
  }, [cards.length]);

  const prev = useCallback(() => setIndex((i) => Math.max(0, i - 1)), []);

  // Keyboard: the drill is meant to run at 20 cards in ten minutes, which does
  // not happen if every action needs the mouse.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (target && ["INPUT", "TEXTAREA", "SELECT"].includes(target.tagName)) return;
      if (event.key === "ArrowRight" || event.key === " ") {
        event.preventDefault();
        if (phase === "stepping") step();
        else if (phase === "done") next();
      } else if (event.key.toLowerCase() === "b" && phase === "call") {
        commit("buy");
      } else if (event.key.toLowerCase() === "p" && phase === "call") {
        commit("pass");
      } else if (event.key.toLowerCase() === "n") {
        next();
      } else if (event.key === "ArrowLeft") {
        prev();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [phase, step, next, prev, commit]);

  const stats = useMemo(() => {
    const buys = log.filter((e) => e.action === "buy" && e.r != null);
    const passes = log.filter((e) => e.action === "pass");
    const totalR = buys.reduce((sum, e) => sum + (e.r ?? 0), 0);
    const wins = buys.filter((e) => (e.r ?? 0) > 0).length;
    return {
      graded: log.length,
      buys: buys.length,
      avgR: buys.length ? totalR / buys.length : null,
      hitRate: buys.length ? (wins / buys.length) * 100 : null,
      passes: passes.length,
      // A pass is "right" when the card would not have paid under the deck rules.
      passesRight: passes.filter((e) => e.officialResult !== "win").length,
    };
  }, [log]);

  const perSetup = useMemo(() => {
    const rows = new Map<string, { setup: string; n: number; totalR: number }>();
    for (const entry of log) {
      if (entry.action !== "buy" || entry.r == null) continue;
      const row = rows.get(entry.setup) ?? { setup: entry.setup, n: 0, totalR: 0 };
      row.n += 1;
      row.totalR += entry.r;
      rows.set(entry.setup, row);
    }
    return [...rows.values()].map((r) => ({ ...r, avgR: r.totalR / r.n }));
  }, [log]);

  if (loading) return <div className="study-state">Dealing today's cards…</div>;
  if (error) return <div className="study-state study-error">Could not load the deck: {error}</div>;
  if (!deck || !cards.length) {
    return (
      <div className="study-state">
        No cards in the deck yet. Build one with{" "}
        <code>python3 scripts/generate_study_deck.py --weeks 52</code>.
      </div>
    );
  }

  return (
    <div className="study-panel">
      <header className="study-head">
        <div className="study-head-left">
          <h2>Chart Gym</h2>
          <p>
            Read the chart, place a stop, call it. The outcome is hidden until you do.
            <span className="study-hint"> B buy · P pass · → step · N next</span>
          </p>
        </div>
        <div className="study-head-right">
          <div className="study-filters">
            {[null, "vcp", "high-tight-flag"].map((key) => (
              <button
                key={key ?? "all"}
                type="button"
                className={setupFilter === key ? "active" : ""}
                onClick={() => setSetupFilter(key)}
              >
                {key === null ? "Both" : key === "vcp" ? "VCP" : "Flags"}
              </button>
            ))}
          </div>
          <div className="study-progress">
            Card {index + 1} / {cards.length}
          </div>
        </div>
      </header>

      {card ? (
        <div className="study-body">
          <div className="study-chart-col">
            <div className="study-card-title">
              {phase === "call" ? (
                <span className="study-masked" title="Revealed once you have called it">
                  {card.label} · hidden symbol
                </span>
              ) : (
                <span>
                  <strong>{card.symbol}</strong> · {card.name} · {card.label} · {card.trigger_date}
                </span>
              )}
              <span className="study-entry">Entry (signal close) {fmt(card.entry)}</span>
            </div>

            {bars.length ? (
            <StudyChart
              contextBars={bars}
              forwardBars={forward}
              revealed={revealed}
              entry={card.entry}
              stop={stop}
              onPickPrice={phase === "call" ? (price) => setStop(Number(price.toFixed(2))) : undefined}
            />
            ) : (
              <div className="study-chart-loading">Loading {card.symbol} history…</div>
            )}

            {phase === "stepping" ? (
              <div className="study-stepper">
                <button type="button" onClick={step} disabled={revealed >= forward.length}>
                  Step forward ({revealed} / {forward.length})
                </button>
                <span className="study-running">
                  {revealed > 0 && forward[revealed - 1] ? (
                    <>
                      Close {fmt(forward[revealed - 1].close)} ·{" "}
                      <em className={forward[revealed - 1].close >= card.entry ? "pos" : "neg"}>
                        {signed(((forward[revealed - 1].close - card.entry) / card.entry) * 100)}%
                      </em>
                      {stoppedAt != null && revealed > stoppedAt ? <strong className="neg"> · stopped out</strong> : null}
                    </>
                  ) : (
                    "Press → to advance one session at a time"
                  )}
                </span>
              </div>
            ) : null}
          </div>

          <aside className="study-side">
            {phase === "call" ? (
              <div className="study-call">
                <h3>Your call</h3>
                <label className="study-stop-field">
                  Stop
                  <input
                    type="number"
                    step="0.05"
                    value={stop ?? ""}
                    placeholder="click the chart"
                    onChange={(event) => {
                      const value = Number(event.target.value);
                      setStop(event.target.value === "" ? null : Number.isFinite(value) ? value : null);
                    }}
                  />
                </label>
                <p className="study-risk">
                  {stop == null
                    ? "Click the chart where your stop belongs."
                    : stop >= card.entry
                      ? "Stop must sit below the entry."
                      : `Risk ${riskPct?.toFixed(2)}% — a 1% account risk sizes this at ${(100 / (riskPct ?? 1)).toFixed(0)}% of the account`}
                </p>
                <div className="study-actions">
                  <button
                    type="button"
                    className="study-buy"
                    disabled={stop == null || stop >= card.entry || !bars.length}
                    onClick={() => commit("buy")}
                  >
                    Buy
                  </button>
                  <button type="button" className="study-pass" onClick={() => commit("pass")}>
                    Pass
                  </button>
                </div>
                <p className="study-note">
                  A pass is a real answer. Most setups should be passed — that is the skill.
                </p>
              </div>
            ) : null}

            {phase !== "call" && reveal ? (
              <div className="study-reveal">
                <h3>
                  {action === "pass" ? (
                    <>You passed</>
                  ) : phase === "done" && grade ? (
                    <>
                      Your trade: <em className={grade.r >= 0 ? "pos" : "neg"}>{signed(grade.r)}R</em>
                    </>
                  ) : (
                    // The R is known the moment the reveal lands, but showing it
                    // here would hand over the answer before the user has walked
                    // a single bar — which is the whole exercise.
                    <>Your trade is running</>
                  )}
                </h3>

                {phase === "done" ? (
                  <>
                    <dl className="study-facts">
                      <div>
                        <dt>Under the deck rules</dt>
                        <dd className={reveal.result === "win" ? "pos" : reveal.result === "loss" ? "neg" : ""}>
                          {reveal.result} · best {signed(reveal.max_favourable_pct)}% · closed{" "}
                          {signed(reveal.final_pct)}% after {reveal.sessions_held} sessions
                        </dd>
                      </div>
                      {reveal.scanner_stop != null ? (
                        <div>
                          <dt>Scanner's stop</dt>
                          <dd>
                            {fmt(reveal.scanner_stop)} (risk {reveal.scanner_risk_pct?.toFixed(1)}%)
                            {stop != null ? (
                              <span className="study-compare">
                                {" "}
                                — yours {stop < reveal.scanner_stop ? "wider" : "tighter"}
                              </span>
                            ) : null}
                          </dd>
                        </div>
                      ) : null}
                      <div>
                        <dt>RS rating</dt>
                        <dd>
                          {reveal.rs_rating || "—"}
                          {reveal.group_top_decile ? " · top-decile industry group" : ""}
                        </dd>
                      </div>
                    </dl>

                    <h4>What the scanner saw</h4>
                    <ul className="study-reasons">
                      {reveal.reasons.map((reason) => (
                        <li key={reason}>{reason}</li>
                      ))}
                    </ul>

                    <div className="study-nav">
                      <button type="button" onClick={prev} disabled={index === 0}>
                        Previous
                      </button>
                      <button type="button" className="study-next" onClick={next} disabled={index >= cards.length - 1}>
                        Next card →
                      </button>
                    </div>
                  </>
                ) : (
                  <p className="study-note">Step through the sessions to see how it resolved.</p>
                )}
              </div>
            ) : null}

            <div className="study-score">
              <h4>Your record</h4>
              <div className="study-score-grid">
                <span>Graded</span>
                <strong>{stats.graded}</strong>
                <span>Bought</span>
                <strong>{stats.buys}</strong>
                <span>Avg R</span>
                <strong className={stats.avgR != null && stats.avgR >= 0 ? "pos" : "neg"}>
                  {stats.avgR != null ? signed(stats.avgR) : "—"}
                </strong>
                <span>Hit rate</span>
                <strong>{stats.hitRate != null ? `${stats.hitRate.toFixed(0)}%` : "—"}</strong>
                <span>Passed</span>
                <strong>
                  {stats.passes}
                  {stats.passes ? ` (${Math.round((stats.passesRight / stats.passes) * 100)}% right)` : ""}
                </strong>
              </div>
              {perSetup.length > 1 ? (
                <ul className="study-per-setup">
                  {perSetup.map((row) => (
                    <li key={row.setup}>
                      {row.setup} <em className={row.avgR >= 0 ? "pos" : "neg"}>{signed(row.avgR)}R</em>{" "}
                      <span>({row.n})</span>
                    </li>
                  ))}
                </ul>
              ) : null}
            </div>
          </aside>
        </div>
      ) : null}

      <footer className="study-foot">
        Deck: {deck.meta.total_cards ?? 0} cards ({deck.meta.wins ?? 0} win / {deck.meta.losses ?? 0} loss) from{" "}
        {deck.meta.window_start} to {deck.meta.window_end}. Wins and losses are dealt in equal measure on purpose.
      </footer>
    </div>
  );
}
