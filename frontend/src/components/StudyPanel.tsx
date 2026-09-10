import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  getStudyBars,
  getStudyDeck,
  getStudyForward,
  getStudyReveal,
  type StudyBar,
  type StudyCard,
  type StudyDeckResponse,
  type StudyReveal,
} from "../lib/api";
import { StudyChart, type StudyChartStyle, type StudyDrawing, type StudyTool } from "./StudyChart";

import "./StudyPanel.css";

const LOG_KEY = "study-drill-log:v1";
const STYLE_KEY = "study-drill-style:v1";
const CHUNK = 8;

type Action = "entered" | "passed";
type Phase = "watching" | "holding" | "done";

type LogEntry = {
  cardId: string;
  setup: string;
  symbol: string;
  gradedAt: string;
  action: Action;
  /** Sessions waited after the signal before buying. */
  waited: number | null;
  entry: number | null;
  stop: number | null;
  riskPct: number | null;
  r: number | null;
  officialResult: string;
};

function readLog(): LogEntry[] {
  if (typeof window === "undefined") return [];
  try {
    const parsed = JSON.parse(window.localStorage.getItem(LOG_KEY) ?? "[]");
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
 * Grade the trade the user actually placed: their entry bar, their stop.
 *
 * Deliberately not the deck's `result`, which is the fixed 3%/5%/10-session
 * rule measured from the signal close. That says nothing about whether waiting
 * three days and stopping under the last contraction was the better trade —
 * which is the entire thing being practised here.
 */
function grade(
  entry: number,
  stop: number,
  held: StudyBar[],
): { r: number; exitIndex: number; stoppedOut: boolean } | null {
  const risk = entry - stop;
  if (!(risk > 0) || !held.length) return null;
  for (let i = 0; i < held.length; i += 1) {
    if (held[i].low <= stop) return { r: -1, exitIndex: i, stoppedOut: true };
  }
  const last = held[held.length - 1];
  return { r: (last.close - entry) / risk, exitIndex: held.length - 1, stoppedOut: false };
}

const fmt = (v: number | null | undefined, digits = 2) =>
  v == null || !Number.isFinite(v) ? "—" : v.toLocaleString("en-IN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
const signed = (v: number, digits = 2) => `${v >= 0 ? "+" : ""}${v.toFixed(digits)}`;

const TOOLS: Array<{ key: StudyTool; label: string; hint: string }> = [
  { key: "cursor", label: "Enter", hint: "Click the chart to buy the newest session" },
  { key: "stop", label: "Stop", hint: "Click a price to place your stop" },
  { key: "trendline", label: "Trendline", hint: "Click start, then end" },
  { key: "measure", label: "Measure", hint: "Click start, then end — shows % move and bars" },
];

const STYLES: Array<{ key: StudyChartStyle; label: string }> = [
  { key: "candles", label: "Candles" },
  { key: "bars", label: "Bars" },
  { key: "hlc", label: "HLC" },
];

export function StudyPanel() {
  const [deck, setDeck] = useState<StudyDeckResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [setupFilter, setSetupFilter] = useState<string | null>(null);
  const [index, setIndex] = useState(0);

  const [phase, setPhase] = useState<Phase>("watching");
  const [revealed, setRevealed] = useState(0);
  const [entryAt, setEntryAt] = useState<number | null>(null);
  const [stop, setStop] = useState<number | null>(null);
  const [reveal, setReveal] = useState<StudyReveal | null>(null);
  const [action, setAction] = useState<Action | null>(null);

  const [tool, setTool] = useState<StudyTool>("stop");
  const [drawings, setDrawings] = useState<StudyDrawing[]>([]);
  const [style, setStyle] = useState<StudyChartStyle>(() => {
    try {
      const saved = window.localStorage.getItem(STYLE_KEY);
      return saved === "bars" || saved === "hlc" ? saved : "candles";
    } catch {
      return "candles";
    }
  });

  const [log, setLog] = useState<LogEntry[]>(() => readLog());
  const loggedRef = useRef<Set<string>>(new Set());
  const [barsByCard, setBarsByCard] = useState<Record<string, StudyBar[]>>({});
  const [fwdByCard, setFwdByCard] = useState<Record<string, StudyBar[]>>({});
  const fetchingRef = useRef<Set<string>>(new Set());

  useEffect(() => {
    try {
      window.localStorage.setItem(STYLE_KEY, style);
    } catch {
      /* ignore */
    }
  }, [style]);

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
  const waitBars = deck?.meta.wait_bars ?? 15;
  const holdBars = deck?.meta.hold_bars ?? 15;

  // Reset per-card state during render, not in an effect: an effect runs after
  // paint, leaving one frame where the new card's context bars are drawn
  // against the previous card's forward bars — timestamps that jump backwards,
  // and a chart-library assertion that blanks the page.
  const [shownCardId, setShownCardId] = useState<string | null>(card?.id ?? null);
  if ((card?.id ?? null) !== shownCardId) {
    setShownCardId(card?.id ?? null);
    setPhase("watching");
    setRevealed(0);
    setEntryAt(null);
    setStop(null);
    setReveal(null);
    setAction(null);
    setDrawings([]);
    setTool("stop");
  }

  const bars = card ? barsByCard[card.id] ?? [] : [];
  const forward = card ? fwdByCard[card.id] ?? [] : [];

  const fetchBars = useCallback((cardId: string) => {
    setBarsByCard((prev) => {
      if (prev[cardId]) return prev;
      getStudyBars(cardId)
        .then((p) => setBarsByCard((cur) => ({ ...cur, [cardId]: p.bars })))
        .catch(() => {});
      return prev;
    });
  }, []);

  useEffect(() => {
    if (card) fetchBars(card.id);
    const upcoming = cards[index + 1];
    if (upcoming) fetchBars(upcoming.id);
  }, [card, cards, index, fetchBars]);

  // Keep a small lookahead of forward bars loaded. Only a few unseen sessions
  // are ever in the browser, so the outcome still has to be stepped into.
  useEffect(() => {
    if (!card) return;
    const have = forward.length;
    const want = revealed + CHUNK;
    const key = `${card.id}@${have}`;
    if (have >= want || fetchingRef.current.has(key)) return;
    fetchingRef.current.add(key);
    getStudyForward(card.id, have, CHUNK)
      .then((p) => {
        if (!p.bars.length) return;
        setFwdByCard((cur) => ({ ...cur, [card.id]: [...(cur[card.id] ?? []), ...p.bars] }));
      })
      .catch(() => {})
      .finally(() => fetchingRef.current.delete(key));
  }, [card, forward.length, revealed]);

  const entryIndex = entryAt != null && bars.length ? bars.length - 1 + entryAt : null;
  const entryPrice =
    entryAt == null ? null : entryAt === 0 ? (bars.length ? bars[bars.length - 1].close : null) : forward[entryAt - 1]?.close ?? null;

  const held = entryAt != null ? forward.slice(entryAt, revealed) : [];
  const result = entryPrice != null && stop != null ? grade(entryPrice, stop, held) : null;
  const riskPct = entryPrice != null && stop != null && stop < entryPrice ? ((entryPrice - stop) / entryPrice) * 100 : null;

  // Two different ceilings, and conflating them is what made the page claim the
  // waiting room was full before a single bar had loaded. `windowCeiling` is the
  // rule — how long you are allowed to wait, or to hold. `stepCeiling` also
  // respects how much data has actually arrived, which is a loading state, not
  // a decision point.
  const windowCeiling = entryAt == null ? waitBars : entryAt + holdBars;
  const stepCeiling = Math.min(windowCeiling, forward.length);
  const atCeiling = revealed >= stepCeiling;
  const outOfRoom = forward.length > 0 && revealed >= windowCeiling;

  useEffect(() => {
    if (phase !== "holding" || entryAt == null) return;
    const doneStepping = revealed >= Math.min(entryAt + holdBars, forward.length) && forward.length > 0;
    const stoppedOut = result?.stoppedOut && revealed > entryAt + result.exitIndex;
    if (doneStepping || stoppedOut) setPhase("done");
  }, [phase, entryAt, revealed, holdBars, forward.length, result]);

  useEffect(() => {
    if (phase !== "done" || !card || reveal) return;
    getStudyReveal(card.id)
      .then(setReveal)
      .catch((err) => setError(err instanceof Error ? err.message : String(err)));
  }, [phase, card, reveal]);

  useEffect(() => {
    if (phase !== "done" || !card || !action) return;
    if (loggedRef.current.has(card.id)) return;
    loggedRef.current.add(card.id);
    const entryLog: LogEntry = {
      cardId: card.id,
      setup: card.setup,
      symbol: card.symbol,
      gradedAt: new Date().toISOString(),
      action,
      waited: action === "entered" ? entryAt : null,
      entry: action === "entered" ? entryPrice : null,
      stop: action === "entered" ? stop : null,
      riskPct: action === "entered" ? riskPct : null,
      r: action === "entered" && result ? Number(result.r.toFixed(3)) : null,
      // Filled in below once the reveal lands. Everything above is already
      // known from the user's own trade, so the record is written now rather
      // than waiting on a request the user can outrun by pressing Next.
      officialResult: "pending",
    };
    setLog((prev) => {
      const next = [...prev.filter((e) => e.cardId !== entryLog.cardId), entryLog];
      writeLog(next);
      return next;
    });
  }, [phase, card, action, entryAt, entryPrice, stop, riskPct, result]);

  // The deck's own verdict arrives separately; fold it into the record when it does.
  useEffect(() => {
    if (!reveal) return;
    setLog((prev) => {
      const idx = prev.findIndex((e) => e.cardId === reveal.id && e.officialResult === "pending");
      if (idx < 0) return prev;
      const next = [...prev];
      next[idx] = { ...next[idx], officialResult: reveal.result };
      writeLog(next);
      return next;
    });
  }, [reveal]);

  const step = useCallback(() => {
    if (phase === "done") return;
    setRevealed((n) => (n < stepCeiling ? n + 1 : n));
  }, [phase, stepCeiling]);

  const enterHere = useCallback(() => {
    if (phase !== "watching" || stop == null) return;
    const price = revealed === 0 ? bars[bars.length - 1]?.close : forward[revealed - 1]?.close;
    if (price == null || stop >= price) return;
    setEntryAt(revealed);
    setAction("entered");
    setPhase("holding");
  }, [phase, stop, revealed, bars, forward]);

  const pass = useCallback(() => {
    if (phase !== "watching") return;
    setAction("passed");
    setPhase("done");
  }, [phase]);

  const next = useCallback(() => setIndex((i) => Math.min(i + 1, Math.max(0, cards.length - 1))), [cards.length]);
  const prev = useCallback(() => setIndex((i) => Math.max(0, i - 1)), []);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (target && ["INPUT", "TEXTAREA", "SELECT"].includes(target.tagName)) return;
      const key = event.key.toLowerCase();
      if (event.key === "ArrowRight" || event.key === " ") {
        event.preventDefault();
        // Deliberately does NOT advance the card when the trade is over: the
        // step key held down through the last session would otherwise blow
        // straight past the result. Moving on is N, or the button.
        step();
      } else if (key === "e" && phase === "watching") enterHere();
      else if (key === "p" && phase === "watching") pass();
      else if (key === "n") next();
      else if (event.key === "ArrowLeft") prev();
      else if (key === "u") setDrawings((d) => d.slice(0, -1));
      else if (key === "1") setTool("cursor");
      else if (key === "2") setTool("stop");
      else if (key === "3") setTool("trendline");
      else if (key === "4") setTool("measure");
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [phase, step, next, prev, enterHere, pass]);

  const stats = useMemo(() => {
    const taken = log.filter((e) => e.action === "entered" && e.r != null);
    const passes = log.filter((e) => e.action === "passed");
    const total = taken.reduce((sum, e) => sum + (e.r ?? 0), 0);
    const waits = taken.filter((e) => e.waited != null).map((e) => e.waited as number);
    return {
      graded: log.length,
      taken: taken.length,
      avgR: taken.length ? total / taken.length : null,
      hitRate: taken.length ? (taken.filter((e) => (e.r ?? 0) > 0).length / taken.length) * 100 : null,
      passes: passes.length,
      passesRight: passes.filter((e) => e.officialResult === "loss" || e.officialResult === "timeout").length,
      avgWait: waits.length ? waits.reduce((a, b) => a + b, 0) / waits.length : null,
    };
  }, [log]);

  if (loading) return <div className="study-state">Dealing today's cards…</div>;
  if (error) {
    return (
      <div className="study-state study-error">
        <p>Could not load the deck: {error}</p>
        <button type="button" className="study-retry" onClick={() => load(setupFilter)}>Try again</button>
      </div>
    );
  }
  if (!deck || !cards.length) {
    return (
      <div className="study-state">
        No cards in the deck yet. Build one with <code>python3 scripts/generate_study_deck.py --weeks 52</code>.
      </div>
    );
  }

  const latestClose = revealed === 0 ? bars[bars.length - 1]?.close : forward[revealed - 1]?.close;
  const canEnter = phase === "watching" && stop != null && latestClose != null && stop < latestClose;

  return (
    <div className="study-panel">
      <header className="study-head">
        <div className="study-head-left">
          <h2>Chart Gym</h2>
          <p>
            Step the tape, pick your own entry, place your own stop.
            <span className="study-hint"> → step · E enter · P pass · U undo · 1-4 tools · N next</span>
          </p>
        </div>
        <div className="study-head-right">
          <div className="study-filters">
            {[null, "vcp", "high-tight-flag"].map((key) => (
              <button key={key ?? "all"} type="button" className={setupFilter === key ? "active" : ""} onClick={() => setSetupFilter(key)}>
                {key === null ? "Both" : key === "vcp" ? "VCP" : "Flags"}
              </button>
            ))}
          </div>
          <div className="study-progress">Card {index + 1} / {cards.length}</div>
        </div>
      </header>

      {card ? (
        <div className="study-body">
          <div className="study-chart-col">
            <div className="study-toolbar">
              <div className="study-tools">
                {TOOLS.map((t, i) => (
                  <button key={t.key} type="button" title={`${t.hint} (${i + 1})`} className={tool === t.key ? "active" : ""} onClick={() => setTool(t.key)}>
                    {t.label}
                  </button>
                ))}
                <button type="button" className="study-undo" onClick={() => setDrawings((d) => d.slice(0, -1))} disabled={!drawings.length}>
                  Undo
                </button>
              </div>
              <div className="study-styles">
                {STYLES.map((s) => (
                  <button key={s.key} type="button" className={style === s.key ? "active" : ""} onClick={() => setStyle(s.key)}>
                    {s.label}
                  </button>
                ))}
              </div>
            </div>

            <div className="study-card-title">
              {phase === "done" ? (
                <span><strong>{card.symbol}</strong> · {card.name} · {card.label} · signal {card.trigger_date}</span>
              ) : (
                <span className="study-masked">{card.label} · hidden symbol</span>
              )}
              <span className="study-entry">
                {entryPrice != null ? `Entry ${fmt(entryPrice)}` : latestClose != null ? `Last close ${fmt(latestClose)}` : "—"}
              </span>
            </div>

            {bars.length ? (
              <StudyChart
                contextBars={bars}
                forwardBars={forward}
                revealed={revealed}
                entryIndex={entryIndex}
                entryPrice={entryPrice}
                stop={stop}
                style={style}
                tool={tool}
                drawings={drawings}
                onDrawingsChange={setDrawings}
                onPickPrice={(price) => phase === "watching" && setStop(Number(price.toFixed(2)))}
                onPickEntry={enterHere}
              />
            ) : (
              <div className="study-chart-loading">Loading {card.symbol} history…</div>
            )}

            <div className="study-stepper">
              <button type="button" onClick={step} disabled={phase === "done" || atCeiling}>
                {phase === "watching" ? `Wait a day (${revealed} / ${waitBars})` : `Hold a day (${entryAt != null ? revealed - entryAt : 0} / ${holdBars})`}
              </button>
              <span className="study-running">
                {phase === "watching" && outOfRoom ? (
                  <>Out of waiting room — enter or pass.</>
                ) : phase === "watching" && atCeiling && !forward.length ? (
                  <>Loading the next sessions…</>
                ) : phase === "holding" && result ? (
                  <>
                    Open: <em className={result.r >= 0 ? "pos" : "neg"}>{signed(result.r)}R</em>
                    {result.stoppedOut && revealed > (entryAt ?? 0) + result.exitIndex ? <strong className="neg"> · stopped out</strong> : null}
                  </>
                ) : phase === "watching" ? (
                  <>{revealed} session{revealed === 1 ? "" : "s"} since the signal — press → to wait, E to enter here.</>
                ) : null}
              </span>
            </div>
          </div>

          <aside className="study-side">
            {phase === "watching" ? (
              <div className="study-call">
                <h3>Your call</h3>
                <label className="study-stop-field">
                  Stop
                  <input
                    type="number"
                    step="0.05"
                    value={stop ?? ""}
                    placeholder="click the chart"
                    onChange={(e) => {
                      const v = Number(e.target.value);
                      setStop(e.target.value === "" ? null : Number.isFinite(v) ? v : null);
                    }}
                  />
                </label>
                <p className="study-risk">
                  {stop == null
                    ? "Pick the Stop tool and click where your stop belongs."
                    : latestClose != null && stop >= latestClose
                      ? "Stop must sit below the last close."
                      : `Risk ${riskPct?.toFixed(2) ?? (latestClose ? (((latestClose - stop) / latestClose) * 100).toFixed(2) : "—")}% at today's close`}
                </p>
                <div className="study-actions">
                  <button type="button" className="study-buy" disabled={!canEnter} onClick={enterHere}>Enter here</button>
                  <button type="button" className="study-pass" onClick={pass}>Pass</button>
                </div>
                <p className="study-note">
                  You can only buy the newest session — entering on a day you have already seen the future of
                  would grade hindsight, not judgement.
                </p>
              </div>
            ) : null}

            {phase !== "watching" ? (
              <div className="study-reveal">
                <h3>
                  {action === "passed" ? <>You passed</> : phase === "done" && result ? (
                    <>Your trade: <em className={result.r >= 0 ? "pos" : "neg"}>{signed(result.r)}R</em></>
                  ) : <>Position open</>}
                </h3>

                {phase === "done" && reveal ? (
                  <>
                    <dl className="study-facts">
                      {action === "entered" ? (
                        <div>
                          <dt>Your trade</dt>
                          <dd>
                            Waited {entryAt} session{entryAt === 1 ? "" : "s"}, entered {fmt(entryPrice)}, stop {fmt(stop)} (risk {riskPct?.toFixed(1)}%)
                          </dd>
                        </div>
                      ) : null}
                      <div>
                        <dt>Deck rules, from the signal close</dt>
                        <dd className={reveal.result === "win" ? "pos" : reveal.result === "loss" ? "neg" : ""}>
                          {reveal.result} · best {signed(reveal.max_favourable_pct)}% · closed {signed(reveal.final_pct)}% after {reveal.sessions_held} sessions
                        </dd>
                      </div>
                      {reveal.scanner_stop != null ? (
                        <div>
                          <dt>Scanner's plan</dt>
                          <dd>
                            stop {fmt(reveal.scanner_stop)} (risk {reveal.scanner_risk_pct?.toFixed(1)}%)
                            {stop != null ? <span className="study-compare"> — yours {stop < reveal.scanner_stop ? "wider" : "tighter"}</span> : null}
                          </dd>
                        </div>
                      ) : null}
                      <div>
                        <dt>RS rating</dt>
                        <dd>{reveal.rs_rating || "—"}{reveal.group_top_decile ? " · top-decile group" : ""}</dd>
                      </div>
                    </dl>
                    <h4>What the scanner saw</h4>
                    <ul className="study-reasons">
                      {reveal.reasons.map((r) => <li key={r}>{r}</li>)}
                    </ul>
                    <div className="study-nav">
                      <button type="button" onClick={prev} disabled={index === 0}>Previous</button>
                      <button type="button" className="study-next" onClick={next} disabled={index >= cards.length - 1}>Next card →</button>
                    </div>
                  </>
                ) : (
                  <p className="study-note">Step the sessions to see how it resolves.</p>
                )}
              </div>
            ) : null}

            <div className="study-score">
              <h4>Your record</h4>
              <div className="study-score-grid">
                <span>Graded</span><strong>{stats.graded}</strong>
                <span>Taken</span><strong>{stats.taken}</strong>
                <span>Avg R</span>
                <strong className={stats.avgR != null && stats.avgR >= 0 ? "pos" : "neg"}>{stats.avgR != null ? signed(stats.avgR) : "—"}</strong>
                <span>Hit rate</span><strong>{stats.hitRate != null ? `${stats.hitRate.toFixed(0)}%` : "—"}</strong>
                <span>Avg wait</span><strong>{stats.avgWait != null ? `${stats.avgWait.toFixed(1)}d` : "—"}</strong>
                <span>Passed</span>
                <strong>{stats.passes}{stats.passes ? ` (${Math.round((stats.passesRight / stats.passes) * 100)}% right)` : ""}</strong>
              </div>
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
