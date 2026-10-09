import { useEffect, useState } from "react";
import {
  getCourseExampleBars,
  getGalleryIndianHistory,
  getSignalArchive,
  type ArchiveRow,
  type GalleryHistoryRow,
  type StudyBar,
} from "../../lib/api";
import { fullChartUrl } from "../../lib/chartLink";
import { LookalikeChart, type LookalikeSeries } from "../LookalikeChart";
import { shuffle } from "./courseData";
import type { ExampleSource } from "./courseLinks";

/* Real Indian charts for a setup lesson — winners AND failures, because a
   library of winners trains the eye to see a breakout in every base (the same
   reason the Chart Gym deck is balanced, CLAUDE.md gotcha 15).

   Two sources, both already graded by the site:
   - deck: Chart Gym signals (study_deck.json) drawn whole through
     /api/course/example-bars, which refuses today's Gym hand;
   - style: look-alike gallery history, charts the image model matched to a
     trader's setup.

   "Call it" mode hides what happened next on every chart until you say
   whether it worked, and keeps score against the set's own base rate — a
   75% hit rate means nothing if 75% of the set worked anyway. */

const PAGE = 6;

type Outcome = "all" | "worked" | "failed";
type Example = {
  key: string;
  symbol: string;
  date: string;
  /** null while the result is still open (look-alike charts under 40 sessions old). */
  worked: boolean | null;
  resultText: string;
  detail: string;
  chart: LookalikeSeries | null;
  cardId?: string;
};

export type ExampleScore = { right: number; total: number };

function seriesFromBars(bars: StudyBar[], triggerIndex: number, show = 90): LookalikeSeries | null {
  if (bars.length < 20) return null;
  const closes = bars.map((b) => b.close);
  const sma = closes.map((_, i) => {
    const from = Math.max(0, i - 49);
    const window = closes.slice(from, i + 1);
    return window.reduce((a, b) => a + b, 0) / window.length;
  });
  const start = Math.max(0, triggerIndex - show + 1);
  const cut = <T,>(a: T[]) => a.slice(start);
  const vols = cut(bars.map((b) => Number(b.volume ?? 0)));
  const vmax = Math.max(1, ...vols);
  const iso = (t: number) => new Date(t * 1000).toISOString().slice(0, 10);
  const kept = cut(bars);
  return {
    o: kept.map((b) => b.open),
    h: kept.map((b) => b.high),
    l: kept.map((b) => b.low),
    c: kept.map((b) => b.close),
    sma: cut(sma),
    v: vols.map((v) => v / vmax),
    setup_index: triggerIndex - start,
    lo: 0,
    hi: 1,
    dates: { start: iso(kept[0].time), setup: iso(bars[triggerIndex].time), end: iso(kept[kept.length - 1].time) },
  };
}

function fromArchive(r: ArchiveRow): Example {
  const worked = r.result === "win" ? true : r.result === "loss" ? false : null;
  const sign = r.final_pct > 0 ? "+" : "";
  return {
    key: r.id,
    symbol: r.symbol,
    date: r.trigger_date,
    worked: r.result === "timeout" ? false : worked,
    resultText: r.result === "win" ? "Hit +5%" : r.result === "loss" ? "Stopped −3%" : `Neither in 10 sessions (${sign}${r.final_pct.toFixed(1)}%)`,
    detail: r.reasons.slice(0, 2).join(" · "),
    chart: null,
    cardId: r.id,
  };
}

function fromGallery(r: GalleryHistoryRow): Example {
  const worked = r.label === "worked" ? true : r.label === "failed" ? false : null;
  return {
    key: `${r.symbol}@${r.date}`,
    symbol: r.symbol,
    date: r.date,
    worked,
    resultText: worked === true ? "Ran +20% first" : worked === false ? "Fell −8% first" : "Still open",
    detail: `More alike than ${Math.min(99, Math.round(r.pct))}% of ordinary charts`,
    chart: r.chart ? { ...r.chart } : null,
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

/** Quiz mode deals half winners, half failures, shuffled — so 50% is what
    guessing scores, and a chart still running (no result yet) never appears. */
async function fetchBalanced(source: ExampleSource, page: number) {
  const half = PAGE / 2;
  const [won, lost] = await Promise.all([fetchPage(source, "worked", page, half), fetchPage(source, "failed", page, half)]);
  const n = Math.min(won.rows.length, lost.rows.length);
  return { rows: shuffle([...won.rows.slice(0, n), ...lost.rows.slice(0, n)]), total: 2 * Math.min(won.total, lost.total), rate: null };
}

function useExamples(source: ExampleSource, outcome: Outcome, page: number, balanced: boolean) {
  const [rows, setRows] = useState<Example[] | null>(null);
  const [total, setTotal] = useState<number | null>(null);
  const [baseRate, setBaseRate] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    setError(null);
    (balanced ? fetchBalanced(source, page) : fetchPage(source, outcome, page, PAGE))
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
  }, [source, outcome, page, balanced]);

  return { rows, total, baseRate, error };
}

function ExampleCard({
  ex,
  quiz,
  onCall,
}: {
  ex: Example;
  quiz: boolean;
  onCall: (right: boolean) => void;
}) {
  const [chart, setChart] = useState<LookalikeSeries | null>(ex.chart);
  const [missing, setMissing] = useState(false);
  const [called, setCalled] = useState<boolean | null>(null);

  useEffect(() => {
    if (ex.chart || !ex.cardId) return;
    let live = true;
    getCourseExampleBars(ex.cardId)
      .then((r) => {
        if (!live) return;
        const s = seriesFromBars(r.bars, r.trigger_index);
        if (s) setChart(s);
        else setMissing(true);
      })
      .catch(() => live && setMissing(true));
    return () => {
      live = false;
    };
  }, [ex]);

  const canCall = quiz && ex.worked !== null;
  const hidden = canCall && called === null;
  const right = called !== null && called === ex.worked;

  return (
    <figure className={`course-example${called === null ? "" : right ? " is-right" : " is-wrong"}`}>
      {chart ? (
        <LookalikeChart data={chart} height={150} labels showAfter={!hidden} ariaLabel={`${ex.symbol} around ${ex.date}`} />
      ) : (
        <div className="course-example-blank">{missing ? "Chart unavailable" : "Loading chart…"}</div>
      )}
      <figcaption>
        <span className="course-example-head">
          <a href={fullChartUrl(ex.symbol)} target="_blank" rel="noreferrer noopener" className="course-mono" title="Chart today, on this site">
            {ex.symbol}
          </a>
          <span className="course-mono">{ex.date}</span>
          {hidden ? null : (
            <span className={`course-result${ex.worked === true ? " is-up" : ex.worked === false ? " is-down" : ""}`}>{ex.resultText}</span>
          )}
        </span>
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
      </figcaption>
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
  const source = sources[Math.min(which, sources.length - 1)];

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
      {/* Keyed so a new source or filter starts from page 0 in one fetch. */}
      <ExampleSet
        key={`${which}:${outcome}:${quiz ? "q" : "s"}`}
        source={source}
        outcome={outcome}
        quiz={quiz}
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
  onCall,
}: {
  source: ExampleSource;
  outcome: Outcome;
  quiz: boolean;
  onCall: (right: boolean) => void;
}) {
  const [page, setPage] = useState(0);
  const { rows, total, baseRate, error } = useExamples(source, outcome, page, quiz);
  const rule =
    source.kind === "deck"
      ? "Graded: +5% before −3% within 10 sessions, from the trigger close."
      : "Graded: +20% before −8% within 40 sessions, from the next open.";

  return (
    <>
      <p className="course-muted course-small">
        {source.kind === "deck"
          ? "Real signals this site's scanner found, failures included. Our scanner's rules, not his: use them to train your eye, then compare with his charts above."
          : "Indian charts the look-alike model matched to this setup. A resemblance, not his definition."}{" "}
        {rule}
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
        <div className="course-example-grid">
          {rows.map((ex) => (
            <ExampleCard key={ex.key} ex={ex} quiz={quiz} onCall={onCall} />
          ))}
        </div>
      ) : null}
      {rows && total !== null && rows.length < total ? (
        <button type="button" className="course-link-button" onClick={() => setPage((p) => p + 1)}>
          Show {PAGE} more
        </button>
      ) : null}
    </>
  );
}
