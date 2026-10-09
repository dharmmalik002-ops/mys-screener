import { useEffect, useMemo, useState } from "react";
import type { SeriesMarker, UTCTimestamp } from "lightweight-charts";
import { getGalleryIndianHistory, getSignalArchive, type ArchiveRow, type GalleryHistoryRow, type StudyBar } from "../../lib/api";
import { fullChartUrl } from "../../lib/chartLink";
import { ChartsPerRow, useChartHeight, useChartsPerRow } from "../ChartsPerRow";
import { CourseCandleChart } from "./CourseCandleChart";
import { CourseChartToolbar } from "./CourseChartOptions";
import { barAtOrBefore, isoShift, loadBars, toTime } from "./courseBars";
import { readJson, shuffle, writeJson } from "./courseData";
import type { ExampleSource } from "./courseLinks";

/* Real Indian charts for a setup lesson — winners AND failures, because a
   library of winners trains the eye to see a breakout in every base (the same
   reason the Chart Gym deck is balanced, CLAUDE.md gotcha 15).

   Which stocks and dates come from two graded sets the site already keeps —
   Chart Gym signals (study_deck.json, via /api/study/archive, which leaves
   out today's Gym hand) and the look-alike gallery history. Every chart is
   then drawn from REAL daily bars for that stock and window
   (/api/course/bars), in the site's own chart look, with the shared toolbar's
   style, moving averages and swing chart. The gallery's stored 0..1 shapes are
   not used for drawing.

   "Call it" hides everything after the setup day until you say whether it
   worked, and deals half winners, half failures, so 50% is guessing. */

const COUNT_KEY = "mr-malik-course-examples-count:v1";
const COLS_KEY = "mr-malik-course-examples-cols:v1";
const MAX_COUNT = 60;
const SHOW_BEFORE = 120; // sessions drawn up to and including the setup day
const SHOW_AFTER = 60; // sessions drawn after it

type Outcome = "all" | "worked" | "failed";
type Example = {
  key: string;
  symbol: string;
  /** The setup session: the last bar the trader would have seen. */
  setup: string;
  /** null while the result is still open (look-alike charts under 40 sessions old). */
  worked: boolean | null;
  resultText: string;
  detail: string;
};

export type ExampleScore = { right: number; total: number };

function fromArchive(r: ArchiveRow): Example {
  const sign = r.final_pct > 0 ? "+" : "";
  return {
    key: r.id,
    symbol: r.symbol,
    setup: r.trigger_date,
    // A timeout never reached +5%, so for "did it work" it counts as no.
    worked: r.result === "win",
    resultText: r.result === "win" ? "Hit +5%" : r.result === "loss" ? "Stopped −3%" : `Neither in 10 sessions (${sign}${r.final_pct.toFixed(1)}%)`,
    detail: r.reasons.slice(0, 2).join(" · "),
  };
}

function fromGallery(r: GalleryHistoryRow): Example {
  const worked = r.label === "worked" ? true : r.label === "failed" ? false : null;
  return {
    key: `${r.symbol}@${r.date}`,
    symbol: r.symbol,
    setup: r.chart?.dates?.setup ?? r.session ?? r.date,
    worked,
    resultText: worked === true ? "Ran +20% first" : worked === false ? "Fell −8% first" : "Still open",
    detail: `More alike than ${Math.min(99, Math.round(r.pct))}% of ordinary charts`,
  };
}

async function fetchPage(source: ExampleSource, outcome: Outcome, page: number, size: number) {
  if (source.kind === "deck") {
    const res = await getSignalArchive({
      setup: source.setup,
      result: outcome === "worked" ? "win" : outcome === "failed" ? "loss" : null,
      sort: "recent",
      limit: size,
      offset: page * size,
    });
    if (!res.available) throw new Error(res.reason || "The signal archive is not available.");
    const base = res.baseline;
    return { rows: res.rows.map(fromArchive), total: res.total, rate: base && base.count ? base.wins / base.count : null };
  }
  const res = await getGalleryIndianHistory(source.style, page, size, outcome);
  return { rows: res.rows.map(fromGallery), total: res.total, rate: null };
}

/** Quiz sets are half winners, half failures, shuffled — so 50% is what
    guessing scores, and a chart still running (no result yet) never appears. */
async function fetchBalanced(source: ExampleSource, page: number, size: number) {
  const half = Math.max(1, Math.ceil(size / 2));
  const [won, lost] = await Promise.all([fetchPage(source, "worked", page, half), fetchPage(source, "failed", page, half)]);
  const n = Math.min(won.rows.length, lost.rows.length);
  return { rows: shuffle([...won.rows.slice(0, n), ...lost.rows.slice(0, n)]), total: 2 * Math.min(won.total, lost.total), rate: null };
}

function useExamples(source: ExampleSource, outcome: Outcome, page: number, balanced: boolean, size: number) {
  const [rows, setRows] = useState<Example[] | null>(null);
  const [total, setTotal] = useState<number | null>(null);
  const [baseRate, setBaseRate] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    setError(null);
    (balanced ? fetchBalanced(source, page, size) : fetchPage(source, outcome, page, size))
      .then((r) => {
        if (!live) return;
        setRows((prev) => (page === 0 || !prev ? r.rows : [...prev, ...r.rows]));
        setTotal(r.total);
        setBaseRate(r.rate);
      })
      .catch((e) => live && setError(e instanceof Error ? e.message : String(e)));
    return () => {
      live = false;
    };
  }, [source, outcome, page, balanced, size]);

  return { rows, total, baseRate, error };
}

/** The example's real bars: enough history before the setup for a 200-day
    average, the setup window, and the sessions that followed. */
function useExampleBars(ex: Example) {
  const [state, setState] = useState<{ bars: StudyBar[]; trigger: number } | "missing" | null>(null);
  useEffect(() => {
    let live = true;
    setState(null);
    loadBars(ex.symbol, isoShift(ex.setup, -420), isoShift(ex.setup, 100))
      .then((all) => {
        if (!live) return;
        const trigger = barAtOrBefore(all, toTime(ex.setup));
        if (trigger < 30) {
          setState("missing");
          return;
        }
        setState({ bars: all.slice(0, trigger + 1 + SHOW_AFTER), trigger });
      })
      .catch(() => live && setState("missing"));
    return () => {
      live = false;
    };
  }, [ex.symbol, ex.setup]);
  return state;
}

function ExampleCard({ ex, quiz, height, onCall }: { ex: Example; quiz: boolean; height: number; onCall: (right: boolean) => void }) {
  const data = useExampleBars(ex);
  const [called, setCalled] = useState<boolean | null>(null);
  const canCall = quiz && ex.worked !== null;
  const hidden = canCall && called === null;
  const right = called !== null && called === ex.worked;
  const loaded = data && data !== "missing" ? data : null;

  const markers = useMemo<SeriesMarker<UTCTimestamp>[]>(
    () =>
      loaded
        ? [{ time: loaded.bars[loaded.trigger].time as UTCTimestamp, position: "belowBar", shape: "arrowUp", color: "#222222", text: "Setup" }]
        : [],
    [loaded],
  );

  return (
    <figure className={`course-example${called === null ? "" : right ? " is-right" : " is-wrong"}`}>
      <figcaption className="course-example-head">
        <a href={fullChartUrl(ex.symbol)} target="_blank" rel="noreferrer noopener" className="course-mono" title="Chart today, on this site">
          {ex.symbol}
        </a>
        <span className="course-mono">setup {ex.setup}</span>
        {hidden ? null : (
          <span className={`course-result${ex.worked === true ? " is-up" : ex.worked === false ? " is-down" : ""}`}>{ex.resultText}</span>
        )}
      </figcaption>
      {loaded ? (
        <CourseCandleChart
          bars={loaded.bars}
          height={height}
          showFrom={Math.max(0, loaded.trigger - SHOW_BEFORE + 1)}
          revealThrough={hidden ? loaded.bars[loaded.trigger].time : undefined}
          markers={markers}
          ariaLabel={`${ex.symbol} daily chart around ${ex.setup}`}
        />
      ) : (
        <div className="course-example-blank" style={{ height }}>
          {data === "missing" ? "No price history for this stock and date" : "Loading chart…"}
        </div>
      )}
      {hidden ? (
        <span className="course-grade is-compact">
          <button
            type="button"
            onClick={() => {
              setCalled(true);
              onCall(ex.worked === true);
            }}
          >
            It worked
          </button>
          <button
            type="button"
            onClick={() => {
              setCalled(false);
              onCall(ex.worked === false);
            }}
          >
            It failed
          </button>
        </span>
      ) : (
        <span className="course-example-detail">
          {called !== null ? <strong>{right ? "Right call. " : "Wrong call. "}</strong> : null}
          {ex.detail}
        </span>
      )}
    </figure>
  );
}

export function SetupExamples({
  sources,
  defaultQuiz = false,
  onScore,
}: {
  sources: ExampleSource[];
  defaultQuiz?: boolean;
  onScore?: (right: boolean) => void;
}) {
  const [which, setWhich] = useState(0);
  const [outcome, setOutcome] = useState<Outcome>("all");
  const [quiz, setQuiz] = useState(defaultQuiz);
  const [score, setScore] = useState<ExampleScore>({ right: 0, total: 0 });
  const [count, setCountState] = useState<number>(() => {
    const v = Number(readJson<number>(COUNT_KEY, 6));
    return v >= 1 && v <= MAX_COUNT ? v : 6;
  });
  const [countDraft, setCountDraft] = useState(String(count));
  const [cols, setCols] = useChartsPerRow(COLS_KEY, 2);
  // Below 900px the stylesheet stacks the grid into one column, so size the
  // charts for one; useChartHeight re-renders this on resize.
  const shownCols = typeof window !== "undefined" && window.innerWidth <= 900 ? 1 : cols;
  const height = useChartHeight(shownCols);
  const source = sources[Math.min(which, sources.length - 1)];

  const setCount = (n: number) => {
    const v = Math.min(MAX_COUNT, Math.max(1, Math.round(n)));
    setCountState(v);
    setCountDraft(String(v));
    writeJson(COUNT_KEY, v);
  };

  return (
    <div className="course-setup-examples">
      <div className="course-gallery-filters">
        {sources.length > 1
          ? sources.map((s, i) => (
              <button key={i} type="button" className={`course-pill${i === which ? " is-active" : ""}`} onClick={() => setWhich(i)}>
                {s.label}
              </button>
            ))
          : null}
        <select value={outcome} onChange={(e) => setOutcome(e.target.value as Outcome)} aria-label="Filter by outcome" disabled={quiz}>
          <option value="all">Winners and failures</option>
          <option value="worked">Only ones that worked</option>
          <option value="failed">Only ones that failed</option>
        </select>
        <label className="course-toggle">
          <input
            type="checkbox"
            checked={quiz}
            onChange={(e) => {
              setQuiz(e.target.checked);
              setOutcome("all");
            }}
          />{" "}
          Call it: hide what happened next
        </label>
        {quiz && score.total ? (
          <span className="course-progress course-mono">
            {score.right}/{score.total} right · {Math.round((100 * score.right) / score.total)}%
          </span>
        ) : null}
      </div>
      <div className="course-gallery-filters">
        <CourseChartToolbar />
      </div>
      <div className="course-gallery-filters">
        <label className="course-count">
          Stocks shown{" "}
          {[2, 4, 6, 12, 24].map((n) => (
            <button key={n} type="button" className={`lookalike-filter${count === n ? " is-active" : ""}`} aria-pressed={count === n} onClick={() => setCount(n)}>
              {n}
            </button>
          ))}
          <input
            type="number"
            min={1}
            max={MAX_COUNT}
            value={countDraft}
            aria-label={`Stocks shown, 1 to ${MAX_COUNT}`}
            onChange={(e) => {
              setCountDraft(e.target.value);
              const n = Number(e.target.value);
              if (n >= 1 && n <= MAX_COUNT) setCount(n);
            }}
            onBlur={() => setCountDraft(String(count))}
          />
        </label>
        <ChartsPerRow value={cols} onChange={setCols} />
      </div>
      {/* Keyed so a new source, filter, mode or count starts again from page 0 in one fetch. */}
      <ExampleSet
        key={`${which}:${outcome}:${quiz ? "q" : "s"}:${count}`}
        source={source}
        outcome={outcome}
        quiz={quiz}
        count={count}
        cols={cols}
        height={height}
        onCall={(right) => {
          setScore((s) => ({ right: s.right + (right ? 1 : 0), total: s.total + 1 }));
          onScore?.(right);
        }}
      />
    </div>
  );
}

function ExampleSet({
  source,
  outcome,
  quiz,
  count,
  cols,
  height,
  onCall,
}: {
  source: ExampleSource;
  outcome: Outcome;
  quiz: boolean;
  count: number;
  cols: number;
  height: number;
  onCall: (right: boolean) => void;
}) {
  const [page, setPage] = useState(0);
  const { rows, total, baseRate, error } = useExamples(source, outcome, page, quiz, count);
  const rule =
    source.kind === "deck"
      ? "Graded: +5% before −3% within 10 sessions, from the setup-day close."
      : "Graded: +20% before −8% within 40 sessions, from the next open.";

  return (
    <>
      <p className="course-muted course-small">
        {source.kind === "deck"
          ? "Real signals this site's scanner found, failures included. Our scanner's rules, not his: use them to train your eye, then compare with his charts above."
          : "Indian charts the look-alike model matched to this setup. A resemblance, not his definition."}{" "}
        Each is drawn from the stock's real daily prices, the arrow marking the setup day. {rule}
        {quiz
          ? " Quiz sets are dealt half winners and half failures, so guessing scores 50%: beat that and you are reading something."
          : baseRate !== null
            ? ` Across the whole set ${Math.round(baseRate * 100)}% worked.`
            : ""}
        {total !== null && !quiz ? ` ${total.toLocaleString("en-IN")} charts in this set.` : ""}
      </p>
      {error ? <p className="course-empty">{error}</p> : null}
      {!rows && !error ? <p className="course-empty">Loading examples…</p> : null}
      {rows ? (
        <div className="course-example-grid" style={{ ["--cols" as string]: cols }}>
          {rows.map((ex) => (
            <ExampleCard key={ex.key} ex={ex} quiz={quiz} height={height} onCall={onCall} />
          ))}
        </div>
      ) : null}
      {rows && total !== null && rows.length < total ? (
        <button type="button" className="course-link-button" onClick={() => setPage((p) => p + 1)}>
          Show {count} more
        </button>
      ) : null}
    </>
  );
}
