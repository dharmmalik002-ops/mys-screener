import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  getChart,
  getStudyBars,
  getStudyDeck,
  getStudyForward,
  getStudyLibrary,
  getStudyReveal,
  getStudyLog,
  saveStudyLibrary,
  saveStudyLog,
  searchStudySymbols,
  type StudyBar,
  type StudyCard,
  type StudyDeckResponse,
  type StudyRecord,
  type StudyReveal,
} from "../lib/api";
import { StudyCoach } from "./StudyCoach";
import {
  StudyChart,
  type StudyChartHandle,
  type StudyChartStyle,
  type StudyDrawing,
  type StudyTool,
} from "./StudyChart";

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
  { key: "snip", label: "Snip", hint: "Drag a box to save that part of the chart as an image" },
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
  const [selectedDrawing, setSelectedDrawing] = useState<string | null>(null);
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
  const chartHandle = useRef<StudyChartHandle | null>(null);
  const [range, setRange] = useState<{ from: number; to: number } | null>(null);
  const [initialRange, setInitialRange] = useState<{ from: number; to: number } | null>(null);

  // Free study: any searched symbol, outside the deck. `null` means the drill.
  const [freeStudy, setFreeStudy] = useState<{ symbol: string; name: string; bars: StudyBar[] } | null>(null);
  // Index into freeStudy.bars where replay starts; null = show the whole chart.
  const [freeAnchor, setFreeAnchor] = useState<number | null>(null);
  const [query, setQuery] = useState("");
  const [matches, setMatches] = useState<Array<{ symbol: string; name: string }>>([]);
  const [library, setLibrary] = useState<StudyRecord[]>([]);
  const [libraryOpen, setLibraryOpen] = useState(false);
  const [note, setNote] = useState("");
  const [toast, setToast] = useState<string | null>(null);
  const [coachOpen, setCoachOpen] = useState(false);
  // Bumped whenever a card is graded, so the coach knows its review is stale.
  const [coachVersion, setCoachVersion] = useState(0);
  const logSyncedRef = useRef(false);
  const [fwdByCard, setFwdByCard] = useState<Record<string, StudyBar[]>>({});
  const fetchingRef = useRef<Set<string>>(new Set());

  useEffect(() => {
    try {
      window.localStorage.setItem(STYLE_KEY, style);
    } catch {
      /* ignore */
    }
  }, [style]);

  const flash = useCallback((message: string) => {
    setToast(message);
    window.setTimeout(() => setToast(null), 3200);
  }, []);

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
    setSelectedDrawing(null);
    setTool("stop");
  }

  // A free study splits its own bars at the replay anchor; a deck card gets its
  // halves from the server, which is what keeps the deck's outcome hidden.
  const deckBars = card ? barsByCard[card.id] ?? [] : [];
  const deckForward = card ? fwdByCard[card.id] ?? [] : [];
  const bars = freeStudy
    ? freeAnchor == null
      ? freeStudy.bars
      : freeStudy.bars.slice(0, freeAnchor + 1)
    : deckBars;
  const forward = freeStudy ? (freeAnchor == null ? [] : freeStudy.bars.slice(freeAnchor + 1)) : deckForward;

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
    if (freeStudy) return;
    if (card) fetchBars(card.id);
    const upcoming = cards[index + 1];
    if (upcoming) fetchBars(upcoming.id);
  }, [freeStudy, card, cards, index, fetchBars]);

  // Keep a small lookahead of forward bars loaded. Only a few unseen sessions
  // are ever in the browser, so the outcome still has to be stepped into.
  useEffect(() => {
    if (!card || freeStudy) return;
    const have = deckForward.length;
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
  }, [card, freeStudy, deckForward.length, revealed]);

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
      // Only push once the server's copy has been merged in, or a fresh browser
      // would overwrite the whole history with its single new card.
      if (logSyncedRef.current) saveStudyLog(next as never).catch(() => {});
      return next;
    });
    setCoachVersion((v) => v + 1);
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

  // --- Library, search, capture ---------------------------------------------

  // The server holds the real record; localStorage is only a fast first paint.
  // Merging rather than replacing means a card graded offline is not lost.
  useEffect(() => {
    getStudyLog()
      .then((payload) => {
        const remote = Array.isArray(payload.entries) ? payload.entries : [];
        setLog((local) => {
          const byId = new Map<string, LogEntry>();
          for (const row of remote as unknown as LogEntry[]) byId.set(row.cardId, row);
          for (const row of local) byId.set(row.cardId, row);
          const merged = [...byId.values()].sort((a, b) => a.gradedAt.localeCompare(b.gradedAt));
          for (const row of merged) loggedRef.current.add(row.cardId);
          writeLog(merged);
          logSyncedRef.current = true;
          if (merged.length !== remote.length) {
            saveStudyLog(merged as never).catch(() => {});
          }
          return merged;
        });
      })
      .catch(() => {
        // Offline or backend down: keep drilling against localStorage.
        logSyncedRef.current = true;
      });
  }, []);

  useEffect(() => {
    getStudyLibrary()
      .then((payload) => setLibrary(Array.isArray(payload.studies) ? payload.studies : []))
      .catch(() => {
        /* an unreachable library must not take the drill down */
      });
  }, []);

  const persist = useCallback(
    (studies: StudyRecord[]) => {
      setLibrary(studies);
      saveStudyLibrary(studies).catch(() => flash("Could not save — the backend did not accept it."));
    },
    [flash],
  );

  useEffect(() => {
    const needle = query.trim();
    if (needle.length < 2) {
      setMatches([]);
      return;
    }
    const timer = window.setTimeout(() => {
      searchStudySymbols(needle).then((r) => setMatches(r.results)).catch(() => setMatches([]));
    }, 200);
    return () => window.clearTimeout(timer);
  }, [query]);

  const openSymbol = useCallback(
    (symbol: string, name: string, restore?: StudyRecord) => {
      setQuery("");
      setMatches([]);
      setLibraryOpen(false);
      getChart(symbol, "1D", "india")
        .then((chart) => {
          const bars = (chart.bars ?? []) as unknown as StudyBar[];
          if (!bars.length) {
            flash(`No history for ${symbol}.`);
            return;
          }
          setFreeStudy({ symbol, name, bars });
          setDrawings((restore?.drawings as StudyDrawing[]) ?? []);
          setStop(restore?.stop ?? null);
          setNote(restore?.note ?? "");
          setInitialRange(restore?.from && restore?.to ? { from: restore.from, to: restore.to } : null);
          setPhase("watching");
          setRevealed(0);
          setEntryAt(null);
          setReveal(null);
          setAction(null);
          if (restore?.style) setStyle(restore.style as StudyChartStyle);
        })
        .catch((err) => flash(err instanceof Error ? err.message : String(err)));
    },
    [flash],
  );

  const saveStudy = useCallback(() => {
    const symbol = freeStudy?.symbol ?? card?.symbol;
    if (!symbol) return;
    const record: StudyRecord = {
      id: `${symbol}-${Date.now()}`,
      symbol,
      name: freeStudy?.name ?? card?.name,
      note: note.trim() || undefined,
      savedAt: new Date().toISOString(),
      cardId: freeStudy ? null : card?.id ?? null,
      triggerDate: freeStudy ? null : card?.trigger_date ?? null,
      from: range?.from ?? null,
      to: range?.to ?? null,
      style,
      stop,
      drawings,
    };
    persist([record, ...library]);
    flash(`Saved ${symbol} to your library.`);
  }, [freeStudy, card, note, range, style, stop, drawings, library, persist, flash]);

  const download = useCallback(
    async (crop?: { x0: number; y0: number; x1: number; y1: number }) => {
      const blob = await chartHandle.current?.capture();
      if (!blob) return;
      const symbol = freeStudy?.symbol ?? card?.symbol ?? "chart";
      let out = blob;
      if (crop) {
        // The capture is at device-pixel scale while the rect is in CSS pixels,
        // so the crop is taken as a proportion of the image rather than raw px.
        const bitmap = await createImageBitmap(blob);
        const node = document.querySelector(".study-chart-wrap") as HTMLElement | null;
        const sx = bitmap.width / (node?.clientWidth || bitmap.width);
        const sy = bitmap.height / (node?.clientHeight || bitmap.height);
        const x = Math.min(crop.x0, crop.x1) * sx;
        const y = Math.min(crop.y0, crop.y1) * sy;
        const w = Math.abs(crop.x1 - crop.x0) * sx;
        const h = Math.abs(crop.y1 - crop.y0) * sy;
        const canvas = document.createElement("canvas");
        canvas.width = Math.max(1, Math.round(w));
        canvas.height = Math.max(1, Math.round(h));
        const ctx = canvas.getContext("2d");
        if (!ctx) return;
        ctx.drawImage(bitmap, x, y, w, h, 0, 0, canvas.width, canvas.height);
        const cropped = await new Promise<Blob | null>((resolve) => canvas.toBlob(resolve, "image/png"));
        if (!cropped) return;
        out = cropped;
      }
      const url = URL.createObjectURL(out);
      const link = document.createElement("a");
      link.href = url;
      link.download = `${symbol}-${new Date().toISOString().slice(0, 10)}.png`;
      link.click();
      URL.revokeObjectURL(url);
      flash(crop ? "Region saved as PNG." : "Chart saved as PNG.");
    },
    [freeStudy, card, flash],
  );

  const step = useCallback(() => {
    if (phase === "done") return;
    setRevealed((n) => (n < stepCeiling ? n + 1 : n));
  }, [phase, stepCeiling]);

  const enterHere = useCallback(() => {
    if (phase !== "watching") return;
    const price = revealed === 0 ? bars[bars.length - 1]?.close : forward[revealed - 1]?.close;
    // Every refusal below used to be a silent no-op, which is indistinguishable
    // from the page being broken. Say what is wrong, where the user is looking.
    if (stop == null) {
      flash("Place a stop first — pick the Stop tool and click the chart, or type a price.");
      return;
    }
    if (price == null) {
      flash("Still loading this chart.");
      return;
    }
    if (stop >= price) {
      flash(`Your stop (${fmt(stop)}) is above the last close (${fmt(price)}). Move it below.`);
      return;
    }
    setEntryAt(revealed);
    setAction("entered");
    setPhase("holding");
  }, [phase, stop, revealed, bars, forward, flash]);

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
      else if (event.key === "Delete" || event.key === "Backspace") {
        if (!selectedDrawing) return;
        event.preventDefault();
        setDrawings((d) => d.filter((x) => x.id !== selectedDrawing));
        setSelectedDrawing(null);
      } else if (event.key === "Escape") {
        setTool("stop");
        setSelectedDrawing(null);
      }
      else if (key === "s") saveStudy();
      else if ("12345".includes(key)) {
        const picked = TOOLS[Number(key) - 1]?.key;
        if (picked) setTool((current) => (current === picked ? "cursor" : picked));
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [phase, step, next, prev, enterHere, pass, saveStudy, selectedDrawing]);

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
            <span className="study-hint"> → step · E enter · P pass · U undo · Del delete · Esc drop tool · 1-5 tools · N next</span>
          </p>
        </div>
        <div className="study-head-right">
          <div className="study-search">
            <input
              type="search"
              value={query}
              placeholder="Search any stock…"
              onChange={(e) => setQuery(e.target.value)}
            />
            {matches.length ? (
              <ul className="study-search-results">
                {matches.map((m) => (
                  <li key={m.symbol}>
                    <button type="button" onClick={() => openSymbol(m.symbol, m.name)}>
                      <strong>{m.symbol}</strong> <span>{m.name}</span>
                    </button>
                  </li>
                ))}
              </ul>
            ) : null}
          </div>
          <button type="button" className={`study-lib-toggle ${libraryOpen ? "active" : ""}`} onClick={() => setLibraryOpen((v) => !v)}>
            Library {library.length ? `(${library.length})` : ""}
          </button>
          <button type="button" className={`study-lib-toggle ${coachOpen ? "active" : ""}`} onClick={() => setCoachOpen((v) => !v)}>
            Coach
          </button>
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
                  <button
                    key={t.key}
                    type="button"
                    title={`${t.hint} (${i + 1}${tool === t.key ? " — click again or Esc to drop it" : ""})`}
                    className={tool === t.key ? "active" : ""}
                    // Clicking the active tool releases it. Being stuck in a
                    // drawing tool with no way out was the complaint.
                    onClick={() => setTool((current) => (current === t.key ? "cursor" : t.key))}
                  >
                    {t.label}
                  </button>
                ))}
                <button
                  type="button"
                  className="study-undo"
                  title="Remove the last drawing (U)"
                  onClick={() => setDrawings((d) => d.slice(0, -1))}
                  disabled={!drawings.length}
                >
                  Undo
                </button>
                <button
                  type="button"
                  className="study-undo"
                  title={selectedDrawing ? "Delete the selected drawing (Del)" : "Remove every drawing"}
                  onClick={() => {
                    if (selectedDrawing) {
                      setDrawings((d) => d.filter((x) => x.id !== selectedDrawing));
                      setSelectedDrawing(null);
                    } else {
                      setDrawings([]);
                    }
                  }}
                  disabled={!drawings.length}
                >
                  {selectedDrawing ? "Delete" : "Clear"}
                </button>
              </div>
              <div className="study-styles">
                {STYLES.map((s) => (
                  <button key={s.key} type="button" className={style === s.key ? "active" : ""} onClick={() => setStyle(s.key)}>
                    {s.label}
                  </button>
                ))}
                <button type="button" title="Save this chart to your library (S)" onClick={saveStudy}>Save</button>
                <button type="button" title="Download the whole chart as PNG" onClick={() => void download()}>PNG</button>
              </div>
            </div>

            <div className="study-card-title">
              {freeStudy ? (
                <span>
                  <strong>{freeStudy.symbol}</strong> · {freeStudy.name}
                  {freeAnchor != null ? " · replaying" : " · free study"}
                  <button type="button" className="study-back" onClick={() => { setFreeStudy(null); setFreeAnchor(null); setDrawings([]); setStop(null); setNote(""); setInitialRange(null); }}>
                    back to deck
                  </button>
                </span>
              ) : phase === "done" ? (
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
                signalLabel={freeStudy ? (freeAnchor == null ? null : "replay start") : "signal"}
                entryPrice={entryPrice}
                stop={stop}
                style={style}
                tool={tool}
                drawings={drawings}
                onDrawingsChange={setDrawings}
                onPickPrice={(price) => phase === "watching" && setStop(Number(price.toFixed(2)))}
                onPickEntry={(time) => {
                  if (freeStudy && freeAnchor == null) {
                    const idx = freeStudy.bars.findIndex((b) => b.time >= time);
                    if (idx > 20) {
                      setFreeAnchor(idx);
                      setRevealed(0);
                    }
                    return;
                  }
                  enterHere();
                }}
                onSnip={(rect) => void download(rect)}
                onDrawingDone={() => setTool("stop")}
                selectedDrawingId={selectedDrawing}
                onSelectDrawing={setSelectedDrawing}
                onRangeChange={setRange}
                initialRange={initialRange}
                ref={chartHandle}
              />
            ) : (
              <div className="study-chart-loading">Loading {card.symbol} history…</div>
            )}

            {tool === "trendline" || tool === "measure" || tool === "snip" ? (
              <div className="study-tool-banner">
                <strong>{TOOLS.find((t) => t.key === tool)?.label}</strong> tool is on — clicks draw instead of
                placing your stop. Press <kbd>Esc</kbd> or click the tool again when you're done.
              </div>
            ) : selectedDrawing ? (
              <div className="study-tool-banner">
                Drawing selected — press <kbd>Del</kbd> or the Delete button to remove it.
              </div>
            ) : null}

            <div className="study-stepper">
              {freeStudy && freeAnchor == null ? (
                <span className="study-running">
                  Whole chart. Pick the Enter tool and click a session to replay forward from there.
                </span>
              ) : (
              <>
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
              </>
              )}
            </div>
          </div>

          <aside className="study-side">
            {coachOpen ? <StudyCoach version={coachVersion} /> : null}
            {libraryOpen ? (
              <div className="study-library">
                <h3>Saved studies</h3>
                {library.length ? (
                  <ul>
                    {library.map((item) => (
                      <li key={item.id}>
                        <button type="button" onClick={() => openSymbol(item.symbol, item.name ?? item.symbol, item)}>
                          <strong>{item.symbol}</strong>
                          <span>{item.note || new Date(item.savedAt).toLocaleDateString("en-IN")}</span>
                        </button>
                        <button
                          type="button"
                          className="study-lib-del"
                          title="Delete this study"
                          onClick={() => persist(library.filter((x) => x.id !== item.id))}
                        >
                          ×
                        </button>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="study-note">Nothing saved yet. Open a chart, draw on it, then press Save.</p>
                )}
              </div>
            ) : null}

            {freeStudy ? (
              <div className="study-call">
                <h3>Study notes</h3>
                <textarea
                  className="study-note-field"
                  value={note}
                  placeholder="What are you looking at here?"
                  onChange={(e) => setNote(e.target.value)}
                />
                <div className="study-actions">
                  <button type="button" className="study-buy" onClick={saveStudy}>Save study</button>
                  <button type="button" className="study-pass" onClick={() => void download()}>PNG</button>
                </div>
                <p className="study-note">
                  Saved studies keep the symbol, the visible dates, your drawings and this note. Reopen one from
                  Library and you land exactly where you left it.
                </p>
              </div>
            ) : null}
            {phase === "watching" && (!freeStudy || freeAnchor != null) ? (
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

      {toast ? <div className="study-toast">{toast}</div> : null}

      <footer className="study-foot">
        Deck: {deck.meta.total_cards ?? 0} cards ({deck.meta.wins ?? 0} win / {deck.meta.losses ?? 0} loss) from{" "}
        {deck.meta.window_start} to {deck.meta.window_end}. Wins and losses are dealt in equal measure on purpose.
      </footer>
    </div>
  );
}
