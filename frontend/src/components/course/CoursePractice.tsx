import { useCallback, useEffect, useMemo, useState } from "react";
import { fullChartUrl } from "../../lib/chartLink";
import {
  caseTicker,
  chartSrc,
  pick,
  readJson,
  shuffle,
  tweetUrl,
  writeJson,
  type Bullet,
  type CaseStudy,
  type ChartItem,
  type CourseData,
  type Lesson,
} from "./courseData";

/* Practice: four drills built only from what the course already verified.
   Nothing here is generated — every question is a lesson's own rule, a
   bullet he wrote as "how" against one he warned about, a chart he used for
   a lesson, or a trade he logged. So a right answer is always checkable
   against the cited tweets. */

type Drill = "cards" | "spot" | "chart" | "replay";
type CardState = Record<string, { box: number; due: string }>;
type Score = { right: number; total: number };
type Scores = Record<"spot" | "chart" | "replay", Score>;

const CARDS_KEY = "mr-malik-course-cards:v1";
const SCORES_KEY = "mr-malik-course-scores:v1";
const DRILL_KEY = "mr-malik-course-drill:v1";
/** Leitner intervals in days for boxes 1..5; box 0 is "again today". */
const INTERVALS = [0, 1, 3, 7, 16, 35];
const EMPTY_SCORES: Scores = { spot: { right: 0, total: 0 }, chart: { right: 0, total: 0 }, replay: { right: 0, total: 0 } };

const today = () => new Date().toISOString().slice(0, 10);
const addDays = (days: number) => new Date(Date.now() + days * 86_400_000).toISOString().slice(0, 10);
const pct = (s: Score) => (s.total ? `${Math.round((100 * s.right) / s.total)}%` : "—");

export function CoursePractice({
  data,
  lessons,
  charts,
  onOpenLesson,
  onZoom,
}: {
  data: CourseData;
  lessons: (Lesson & { moduleKey: string; moduleTitle: string })[];
  charts: ChartItem[];
  onOpenLesson: (lessonId: string) => void;
  onZoom: (items: ChartItem[], index: number) => void;
}) {
  const [drill, setDrill] = useState<Drill>(() => (readJson<string>(DRILL_KEY, "cards") as Drill) || "cards");
  const [scores, setScores] = useState<Scores>(() => ({ ...EMPTY_SCORES, ...readJson<Partial<Scores>>(SCORES_KEY, {}) }));

  useEffect(() => writeJson(DRILL_KEY, drill), [drill]);

  const record = useCallback((kind: keyof Scores, right: boolean) => {
    setScores((prev) => {
      const next = { ...prev, [kind]: { right: prev[kind].right + (right ? 1 : 0), total: prev[kind].total + 1 } };
      writeJson(SCORES_KEY, next);
      return next;
    });
  }, []);

  const cases = useMemo(() => data.modules.flatMap((m) => m.cases ?? []), [data]);
  const tabs: { key: Drill; label: string; blurb: string; score?: Score }[] = [
    { key: "cards", label: "Flashcards", blurb: "Recall each lesson's rule; spaced repetition brings back the ones you miss." },
    { key: "spot", label: "Spot the mistake", blurb: "Three things he does, one he warns against. Find the warning.", score: scores.spot },
    { key: "chart", label: "Read the chart", blurb: "Which lesson did he post this chart to make?", score: scores.chart },
    { key: "replay", label: "Trade replay", blurb: "Step through a real trade log one decision at a time.", score: scores.replay },
  ];

  return (
    <div className="course-practice">
      <div className="course-drill-tabs" role="tablist" aria-label="Practice drills">
        {tabs.map((t) => (
          <button
            key={t.key}
            type="button"
            role="tab"
            aria-selected={drill === t.key}
            className={`course-drill-tab${drill === t.key ? " is-active" : ""}`}
            onClick={() => setDrill(t.key)}
          >
            <strong>{t.label}</strong>
            <span>{t.blurb}</span>
            {t.score && t.score.total ? (
              <em className="course-mono">
                {t.score.right}/{t.score.total} · {pct(t.score)}
              </em>
            ) : null}
          </button>
        ))}
      </div>
      {drill === "cards" ? <Flashcards lessons={lessons} onOpenLesson={onOpenLesson} /> : null}
      {drill === "spot" ? <SpotTheMistake lessons={lessons} dates={data.tweet_dates} onOpenLesson={onOpenLesson} onAnswer={(r) => record("spot", r)} /> : null}
      {drill === "chart" ? (
        <ReadTheChart lessons={lessons} charts={charts} onOpenLesson={onOpenLesson} onZoom={onZoom} onAnswer={(r) => record("chart", r)} />
      ) : null}
      {drill === "replay" ? (
        <TradeReplay
          cases={cases}
          dates={data.tweet_dates}
          titleOf={(id) => lessons.find((l) => l.id === id)?.title ?? id}
          onOpenLesson={onOpenLesson}
          onZoom={onZoom}
          onAnswer={(r) => record("replay", r)}
        />
      ) : null}
      {scores.spot.total + scores.chart.total + scores.replay.total > 0 ? (
        <button
          type="button"
          className="course-link-button course-reset"
          onClick={() => {
            setScores(EMPTY_SCORES);
            writeJson(SCORES_KEY, EMPTY_SCORES);
          }}
        >
          Reset drill scores
        </button>
      ) : null}
    </div>
  );
}

/* ---------- Flashcards ---------- */

function Flashcards({
  lessons,
  onOpenLesson,
}: {
  lessons: (Lesson & { moduleKey: string; moduleTitle: string })[];
  onOpenLesson: (lessonId: string) => void;
}) {
  const [state, setState] = useState<CardState>(() => readJson<CardState>(CARDS_KEY, {}));
  const [moduleKey, setModuleKey] = useState("all");
  const [flipped, setFlipped] = useState(false);
  const [skipped, setSkipped] = useState<string[]>([]);

  const modules = useMemo(() => {
    const seen = new Map<string, string>();
    lessons.forEach((l) => seen.set(l.moduleKey, l.moduleTitle));
    return [...seen.entries()];
  }, [lessons]);

  const pool = lessons.filter((l) => moduleKey === "all" || l.moduleKey === moduleKey);
  const now = today();
  // Due reviews first (oldest due first), then cards never seen, in course order.
  const due = pool.filter((l) => state[l.id] && state[l.id].due <= now).sort((a, b) => state[a.id].due.localeCompare(state[b.id].due));
  const fresh = pool.filter((l) => !state[l.id]);
  const queue = [...due, ...fresh].filter((l) => !skipped.includes(l.id));
  const card = queue[0];
  const learned = pool.filter((l) => (state[l.id]?.box ?? 0) >= 3).length;

  const grade = useCallback(
    (knew: boolean) => {
      if (!card) return;
      setState((prev) => {
        const box = knew ? Math.min((prev[card.id]?.box ?? 0) + 1, INTERVALS.length - 1) : 0;
        const next = { ...prev, [card.id]: { box, due: addDays(INTERVALS[box]) } };
        writeJson(CARDS_KEY, next);
        return next;
      });
      // "Again" stays due today; push it behind the rest of this session's queue.
      if (!knew) setSkipped((s) => [...s, card.id]);
      setFlipped(false);
    },
    [card],
  );

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement | null;
      if (target && /^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName)) return;
      if (e.key === " ") {
        e.preventDefault();
        setFlipped((f) => !f);
      } else if (flipped && e.key === "1") grade(false);
      else if (flipped && e.key === "2") grade(true);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [flipped, grade]);

  return (
    <section className="course-drill">
      <div className="course-toolbar">
        <select
          value={moduleKey}
          onChange={(e) => {
            setModuleKey(e.target.value);
            setSkipped([]);
            setFlipped(false);
          }}
          aria-label="Flashcards module"
        >
          <option value="all">All modules</option>
          {modules.map(([key, title]) => (
            <option key={key} value={key}>
              {title}
            </option>
          ))}
        </select>
        <span className="course-progress">
          {due.length} due · {fresh.length} new · {learned} of {pool.length} learned
          <span className="course-bar">
            <i style={{ width: `${pool.length ? (100 * learned) / pool.length : 0}%` }} />
          </span>
        </span>
      </div>
      {card ? (
        <article className={`course-flashcard${flipped ? " is-flipped" : ""}`}>
          <div className="course-chips">
            <span className="course-chip">{card.moduleTitle}</span>
            {card.setup ? <span className="course-chip is-setup">{card.setup}</span> : null}
            {state[card.id] ? <span className="course-chip">box {state[card.id].box}</span> : <span className="course-chip">new</span>}
          </div>
          <h3>{card.title}</h3>
          {!flipped ? (
            <>
              <p className="course-muted">State the rule in your own words, and one way he applies it. Then reveal.</p>
              <button type="button" className="course-primary" onClick={() => setFlipped(true)}>
                Reveal <kbd>Space</kbd>
              </button>
            </>
          ) : (
            <>
              <p className="course-rule">{card.rule}</p>
              <ul className="course-list">
                {card.how.slice(0, 3).map((b, i) => (
                  <li key={i}>{b.text}</li>
                ))}
              </ul>
              {card.examples[0] ? (
                <img className="course-flash-chart" src={chartSrc(card.examples[0].local, card.examples[0].image, "small")} alt={card.examples[0].caption} loading="lazy" referrerPolicy="no-referrer" />
              ) : null}
              <div className="course-grade">
                <button type="button" onClick={() => grade(false)}>
                  Missed it — show again <kbd>1</kbd>
                </button>
                <button type="button" className="course-primary" onClick={() => grade(true)}>
                  Knew it <kbd>2</kbd>
                </button>
                <button type="button" className="course-link-button" onClick={() => onOpenLesson(card.id)}>
                  Open the full lesson
                </button>
              </div>
            </>
          )}
        </article>
      ) : (
        <div className="course-flashcard is-empty">
          <h3>Nothing due here.</h3>
          <p className="course-muted">
            Every card in this set is scheduled for a later day. Cards you knew come back after 1, 3, 7, 16 and 35 days; cards you
            missed come back the same day.
          </p>
          {skipped.length ? (
            <button type="button" className="course-primary" onClick={() => setSkipped([])}>
              Go through the {skipped.length} missed card{skipped.length === 1 ? "" : "s"} again
            </button>
          ) : null}
        </div>
      )}
    </section>
  );
}

/* ---------- Spot the mistake ---------- */

type SpotQuestion = { lesson: Lesson & { moduleTitle: string }; options: { bullet: Bullet; mistake: boolean }[] };

function makeSpot(pool: (Lesson & { moduleTitle: string })[]): SpotQuestion | null {
  if (!pool.length) return null;
  const lesson = pick(pool);
  const options = shuffle([
    { bullet: pick(lesson.mistakes), mistake: true },
    ...shuffle(lesson.how).slice(0, 3).map((bullet) => ({ bullet, mistake: false })),
  ]);
  return { lesson, options };
}

function SpotTheMistake({
  lessons,
  dates,
  onOpenLesson,
  onAnswer,
}: {
  lessons: (Lesson & { moduleTitle: string })[];
  dates: Record<string, string>;
  onOpenLesson: (lessonId: string) => void;
  onAnswer: (right: boolean) => void;
}) {
  const pool = useMemo(() => lessons.filter((l) => l.mistakes.length >= 1 && l.how.length >= 3), [lessons]);
  const [q, setQ] = useState<SpotQuestion | null>(() => makeSpot(pool));
  const [chosen, setChosen] = useState<number | null>(null);
  const [streak, setStreak] = useState(0);

  if (!q) return <p className="course-empty">No lesson has enough material for this drill.</p>;
  const answered = chosen !== null;

  return (
    <section className="course-drill">
      <p className="course-muted">
        From <strong>{q.lesson.title}</strong> <span className="course-chip">{q.lesson.moduleTitle}</span> — three of these are how he
        does it; one is a mistake he warns about. Which is the mistake?
      </p>
      <ol className="course-options">
        {q.options.map((o, i) => {
          const state = !answered ? "" : o.mistake ? " is-right" : i === chosen ? " is-wrong" : " is-dim";
          return (
            <li key={i}>
              <button
                type="button"
                className={`course-option${state}`}
                disabled={answered}
                onClick={() => {
                  setChosen(i);
                  onAnswer(o.mistake);
                  setStreak((s) => (o.mistake ? s + 1 : 0));
                }}
              >
                <span className="course-mono">{String.fromCharCode(65 + i)}</span> {o.bullet.text}
              </button>
              {answered ? (
                <span className="course-option-note">
                  {o.mistake ? "Mistake he warns about" : "How he does it"}
                  {o.bullet.ids.slice(0, 2).map((id) => (
                    <a key={id} className="course-src" href={tweetUrl(id)} target="_blank" rel="noreferrer noopener">
                      {dates[id] ?? "tweet"}
                    </a>
                  ))}
                </span>
              ) : null}
            </li>
          );
        })}
      </ol>
      {answered ? (
        <div className="course-grade">
          <span className="course-mono">{streak ? `Streak ${streak}` : "Streak reset"}</span>
          <button
            type="button"
            className="course-primary"
            onClick={() => {
              setQ(makeSpot(pool));
              setChosen(null);
            }}
          >
            Next question
          </button>
          <button type="button" className="course-link-button" onClick={() => onOpenLesson(q.lesson.id)}>
            Read the lesson
          </button>
        </div>
      ) : null}
    </section>
  );
}

/* ---------- Read the chart ---------- */

type ChartQuestion = { chart: ChartItem; options: { id: string; title: string }[] };

function makeChartQuestion(charts: ChartItem[], lessons: (Lesson & { moduleKey: string })[]): ChartQuestion | null {
  // Lesson examples only: case charts are tied to a trade, not a lesson.
  const pool = charts.filter((c) => c.lessonId && !c.lessonId.startsWith("case-"));
  if (!pool.length) return null;
  const chart = pick(pool);
  const correct = lessons.find((l) => l.id === chart.lessonId);
  if (!correct) return null;
  // Distractors from the same module first, so the choice is about the idea rather than the topic.
  const usesChart = new Set(charts.filter((c) => (c.local ?? c.image) === (chart.local ?? chart.image)).map((c) => c.lessonId));
  const others = lessons.filter((l) => !usesChart.has(l.id));
  const sameModule = shuffle(others.filter((l) => l.moduleKey === correct.moduleKey));
  const rest = shuffle(others.filter((l) => l.moduleKey !== correct.moduleKey));
  const distractors = [...sameModule, ...rest].slice(0, 3);
  return { chart, options: shuffle([correct, ...distractors]).map((l) => ({ id: l.id, title: l.title })) };
}

function ReadTheChart({
  lessons,
  charts,
  onOpenLesson,
  onZoom,
  onAnswer,
}: {
  lessons: (Lesson & { moduleKey: string })[];
  charts: ChartItem[];
  onOpenLesson: (lessonId: string) => void;
  onZoom: (items: ChartItem[], index: number) => void;
  onAnswer: (right: boolean) => void;
}) {
  const [q, setQ] = useState<ChartQuestion | null>(() => makeChartQuestion(charts, lessons));
  const [chosen, setChosen] = useState<string | null>(null);
  if (!q) return <p className="course-empty">No charts to quiz on.</p>;
  const answered = chosen !== null;

  return (
    <section className="course-drill course-chart-drill">
      <button type="button" className="course-zoom is-large" onClick={() => onZoom([q.chart], 0)} aria-label="Enlarge chart">
        <img src={chartSrc(q.chart.local, q.chart.image, "large")} alt="Chart to identify" referrerPolicy="no-referrer" />
      </button>
      <div>
        <p className="course-muted">He posted this to make one of these points. Which one?</p>
        <ol className="course-options">
          {q.options.map((o, i) => {
            const right = o.id === q.chart.lessonId;
            const state = !answered ? "" : right ? " is-right" : o.id === chosen ? " is-wrong" : " is-dim";
            return (
              <li key={o.id}>
                <button
                  type="button"
                  className={`course-option${state}`}
                  disabled={answered}
                  onClick={() => {
                    setChosen(o.id);
                    onAnswer(right);
                  }}
                >
                  <span className="course-mono">{String.fromCharCode(65 + i)}</span> {o.title}
                </button>
              </li>
            );
          })}
        </ol>
        {answered ? (
          <>
            <p className="course-takeaway">
              {q.chart.date ? <span className="course-mono">{q.chart.date} · </span> : null}
              {q.chart.caption}
            </p>
            <div className="course-grade">
              <button
                type="button"
                className="course-primary"
                onClick={() => {
                  setQ(makeChartQuestion(charts, lessons));
                  setChosen(null);
                }}
              >
                Next chart
              </button>
              <button type="button" className="course-link-button" onClick={() => onOpenLesson(q.chart.lessonId!)}>
                Read the lesson
              </button>
              {q.chart.tweet ? (
                <a className="course-link-button" href={tweetUrl(q.chart.tweet)} target="_blank" rel="noreferrer noopener">
                  Source tweet
                </a>
              ) : null}
            </div>
          </>
        ) : null}
      </div>
    </section>
  );
}

/* ---------- Trade replay ---------- */

function TradeReplay({
  cases,
  dates,
  titleOf,
  onOpenLesson,
  onZoom,
  onAnswer,
}: {
  cases: CaseStudy[];
  dates: Record<string, string>;
  titleOf: (lessonId: string) => string;
  onOpenLesson: (lessonId: string) => void;
  onZoom: (items: ChartItem[], index: number) => void;
  onAnswer: (right: boolean) => void;
}) {
  const [index, setIndex] = useState(0);
  const [step, setStep] = useState(0);
  const [guess, setGuess] = useState<"up" | "down" | null>(null);
  const c = cases[index];
  if (!c) return <p className="course-empty">No case studies.</p>;

  const total = c.timeline.length;
  // Step 0 shows only the first entry; the story and the result wait for the end.
  const shown = c.timeline.slice(0, Math.max(1, step + 1));
  const finished = step >= total - 1;
  const canGuess = c.result_pct != null && c.result_pct !== 0;
  const needsGuess = canGuess && guess === null;
  const ticker = caseTicker(c.symbol);
  const chart: ChartItem | null =
    c.local || c.image ? { key: c.root, local: c.local, image: c.image, caption: `${c.symbol} entry chart`, tweet: c.root, date: c.entry_date } : null;

  const load = (next: number) => {
    setIndex(next);
    setStep(0);
    setGuess(null);
  };

  return (
    <section className="course-drill">
      <div className="course-toolbar">
        <select value={index} onChange={(e) => load(Number(e.target.value))} aria-label="Choose a trade">
          {cases.map((x, i) => (
            <option key={x.root} value={i}>
              {x.entry_date} · {x.symbol}
            </option>
          ))}
        </select>
        <button type="button" className="course-link-button" onClick={() => load(Math.floor(Math.random() * cases.length))}>
          Random trade
        </button>
      </div>
      <div className="course-case">
        {chart ? (
          <figure className="course-figure">
            <button type="button" className="course-zoom" onClick={() => onZoom([chart], 0)} aria-label="Enlarge chart">
              <img src={chartSrc(chart.local, chart.image, "small")} alt={chart.caption} referrerPolicy="no-referrer" />
            </button>
            <figcaption>
              The chart he posted at entry, {c.entry_date}.{" "}
              {ticker ? (
                <a className="course-src" href={fullChartUrl(ticker)} target="_blank" rel="noreferrer noopener" title="The stock's chart today on this site">
                  chart today
                </a>
              ) : null}
            </figcaption>
          </figure>
        ) : null}
        <div className="course-case-body">
          <h3 className="course-replay-title">
            <span className="course-mono">{c.symbol}</span> · {c.setup}
          </h3>
          {c.context ? <p className="course-muted">{c.context}</p> : null}
          <h4>Trade log</h4>
          <ol className="course-list course-replay-log">
            {shown.map((t, j) => (
              <li key={j} className={j === shown.length - 1 ? "is-new" : ""}>
                <span className="course-mono">{t.date}</span> <strong>{t.action}</strong>
                {t.price != null && t.price !== "" ? <span className="course-mono"> @ {t.price}</span> : null} — {t.text}{" "}
                {dates[t.tweet] ? (
                  <a className="course-src" href={tweetUrl(t.tweet)} target="_blank" rel="noreferrer noopener">
                    {dates[t.tweet]}
                  </a>
                ) : null}
              </li>
            ))}
          </ol>
          {step === 0 && needsGuess ? (
            <div className="course-guess">
              <p>Before you step through it: did this trade end in profit?</p>
              <div className="course-grade">
                <button type="button" onClick={() => setGuess("up")}>
                  Profit
                </button>
                <button type="button" onClick={() => setGuess("down")}>
                  Loss
                </button>
                <button type="button" className="course-link-button" onClick={() => setStep(1)}>
                  Skip the guess
                </button>
              </div>
            </div>
          ) : !finished ? (
            <div className="course-grade">
              <span className="course-mono">
                {shown.length} / {total} events
              </span>
              <button type="button" className="course-primary" onClick={() => setStep((s) => Math.min(s + 1, total - 1))}>
                What did he do next?
              </button>
              <button type="button" className="course-link-button" onClick={() => setStep(total - 1)}>
                Show all
              </button>
            </div>
          ) : (
            <Outcome key={c.root} c={c} guess={guess} titleOf={titleOf} onAnswer={onAnswer} onOpenLesson={onOpenLesson} onNext={() => load((index + 1) % cases.length)} />
          )}
        </div>
      </div>
    </section>
  );
}

function Outcome({
  c,
  guess,
  titleOf,
  onAnswer,
  onOpenLesson,
  onNext,
}: {
  c: CaseStudy;
  guess: "up" | "down" | null;
  titleOf: (lessonId: string) => string;
  onAnswer: (right: boolean) => void;
  onOpenLesson: (lessonId: string) => void;
  onNext: () => void;
}) {
  const up = (c.result_pct ?? 0) > 0;
  const right = guess !== null && c.result_pct != null && (guess === "up") === up;
  // Score once per finished replay, when it first shows (Outcome is keyed per trade).
  useEffect(() => {
    if (guess !== null && c.result_pct != null && c.result_pct !== 0) onAnswer(right);
  }, []);
  return (
    <div className="course-outcome">
      <p>
        <strong className={`course-result${c.result_pct == null ? "" : up ? " is-up" : " is-down"}`}>
          {c.result_pct == null ? "Result not stated" : `${up ? "+" : ""}${c.result_pct.toFixed(1)}%`}
        </strong>{" "}
        {guess !== null && c.result_pct != null ? <span>{right ? "— your call was right." : "— your call was wrong."}</span> : null}
      </p>
      {c.result_note ? <p className="course-muted">{c.result_note}</p> : null}
      <p>{c.story}</p>
      {c.takeaway ? (
        <p className="course-takeaway">
          <strong>Takeaway:</strong> {c.takeaway}
        </p>
      ) : null}
      {c.lessons.length ? (
        <p className="course-related">
          Lessons this trade shows:{" "}
          {c.lessons.map((id) => (
            <button key={id} type="button" className="course-link-button" onClick={() => onOpenLesson(id)}>
              {titleOf(id)}
            </button>
          ))}
        </p>
      ) : null}
      <div className="course-grade">
        <button type="button" className="course-primary" onClick={onNext}>
          Next trade
        </button>
      </div>
    </div>
  );
}
