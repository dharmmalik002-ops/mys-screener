import { useCallback, useEffect, useMemo, useState } from "react";
import { BookOpen, Dumbbell, Images } from "lucide-react";
import { Panel } from "./Panel";
import { fullChartUrl } from "../lib/chartLink";
import { CourseGallery } from "./course/CourseGallery";
import { CourseLightbox } from "./course/CourseLightbox";
import { CoursePractice } from "./course/CoursePractice";
import {
  HANDLE,
  allCharts,
  caseAnchor,
  caseTicker,
  chartSrc,
  moduleOfLesson,
  readJson,
  tweetUrl,
  writeJson,
  type CaseStudy,
  type ChartItem,
  type CourseData,
  type Lesson,
  type Module,
} from "./course/courseData";
import "./CoursePanel.css";

/* The Course page: @iManasArora's approach, distilled from his 2021-2026 posts.
   Data is a static file written by backend/scripts/export_course.py — every
   bullet carries the ids of the tweets it rests on. Charts are served from our
   own copies (frontend/public/course/img) so they outlive a deleted tweet;
   X's CDN is only the fallback. */

type View = "lessons" | "gallery" | "practice";

const DONE_KEY = "mr-malik-course-done:v1";
const MODULE_KEY = "mr-malik-course-module:v1";
const VIEW_KEY = "mr-malik-course-view:v1";
const NOTES_KEY = "mr-malik-course-notes:v1";

/** The module key is stored as a bare string, not JSON. */
function readModule(): string {
  try {
    return localStorage.getItem(MODULE_KEY) || "philosophy";
  } catch {
    return "philosophy";
  }
}

function Sources({ ids, dates, max = 4 }: { ids: string[]; dates: Record<string, string>; max?: number }) {
  if (!ids?.length) return null;
  return (
    <span className="course-sources">
      {ids.slice(0, max).map((id) => (
        <a key={id} className="course-src" href={tweetUrl(id)} target="_blank" rel="noreferrer noopener" title="Open the source tweet">
          {dates[id] ?? "tweet"}
        </a>
      ))}
      {ids.length > max ? <span className="course-src-more">+{ids.length - max}</span> : null}
    </span>
  );
}

function Chart({ item, onZoom }: { item: ChartItem; onZoom: () => void }) {
  const [failed, setFailed] = useState(false);
  const src = failed ? chartSrc(null, item.image, "small") : chartSrc(item.local, item.image, "small");
  if (!src) return null;
  return (
    <figure className="course-figure">
      <button type="button" className="course-zoom" onClick={onZoom} aria-label="Enlarge chart">
        <img
          src={src}
          alt={item.caption}
          loading="lazy"
          referrerPolicy="no-referrer"
          onError={() => {
            if (!failed && item.local && item.image) setFailed(true);
          }}
        />
      </button>
      <figcaption>
        {item.caption}{" "}
        {item.tweet ? (
          <a className="course-src" href={tweetUrl(item.tweet)} target="_blank" rel="noreferrer noopener">
            {item.date ?? "tweet"}
          </a>
        ) : null}
      </figcaption>
    </figure>
  );
}

function LessonCard({
  lesson,
  index,
  dates,
  done,
  note,
  trades,
  onToggle,
  onNote,
  onZoom,
  onOpenCase,
}: {
  lesson: Lesson;
  index: number;
  dates: Record<string, string>;
  done: boolean;
  note: string;
  trades: CaseStudy[];
  onToggle: () => void;
  onNote: (text: string) => void;
  onZoom: (items: ChartItem[], index: number) => void;
  onOpenCase: (c: CaseStudy) => void;
}) {
  const charts = useMemo<ChartItem[]>(
    () =>
      lesson.examples.map((e) => ({
        key: `${lesson.id}:${e.local ?? e.image}`,
        local: e.local,
        image: e.image,
        caption: e.caption,
        tweet: e.tweet,
        date: dates[e.tweet],
      })),
    [lesson, dates],
  );
  return (
    <article className={`course-lesson${done ? " is-done" : ""}`} id={lesson.id}>
      <header className="course-lesson-head">
        <span className="course-num">{index}</span>
        <h3>{lesson.title}</h3>
        <label className="course-done">
          <input type="checkbox" checked={done} onChange={onToggle} /> Studied
        </label>
      </header>
      <div className="course-chips">
        {lesson.setup ? <span className="course-chip is-setup">{lesson.setup}</span> : null}
        <span className={`course-chip${lesson.strength === "core" ? " is-core" : ""}`}>
          {lesson.strength === "core" ? "Core idea" : "Supporting idea"}
        </span>
        <span className="course-chip">
          {lesson.evidence.length} tweets · {lesson.years.join(", ") || "—"}
        </span>
        {charts.length ? <span className="course-chip">{charts.length} chart{charts.length === 1 ? "" : "s"}</span> : null}
      </div>
      <p className="course-rule"><Linked text={lesson.rule} dates={dates} /></p>
      {lesson.how.length ? (
        <>
          <h4>How he does it</h4>
          <ul className="course-list">
            {lesson.how.map((b, i) => (
              <li key={i}>
                <Linked text={b.text} dates={dates} /> <Sources ids={b.ids} dates={dates} />
              </li>
            ))}
          </ul>
        </>
      ) : null}
      {lesson.when ? (
        <>
          <h4>When it applies</h4>
          <p><Linked text={lesson.when} dates={dates} /></p>
        </>
      ) : null}
      {lesson.mistakes.length ? (
        <>
          <h4>Mistakes he warns about</h4>
          <ul className="course-list is-mistakes">
            {lesson.mistakes.map((b, i) => (
              <li key={i}>
                <Linked text={b.text} dates={dates} /> <Sources ids={b.ids} dates={dates} max={3} />
              </li>
            ))}
          </ul>
        </>
      ) : null}
      {lesson.evolution ? (
        <>
          <h4>How it changed over time</h4>
          <p><Linked text={lesson.evolution} dates={dates} /></p>
        </>
      ) : null}
      {charts.length ? (
        <div className="course-examples">
          {charts.map((c, i) => (
            <Chart key={c.key} item={c} onZoom={() => onZoom(charts, i)} />
          ))}
        </div>
      ) : null}
      {trades.length ? (
        <p className="course-related">
          In his trade logs:{" "}
          {trades.map((c) => (
            <button key={c.root} type="button" className="course-link-button" onClick={() => onOpenCase(c)}>
              {c.symbol} ({c.entry_date.slice(0, 4)})
            </button>
          ))}
        </p>
      ) : null}
      <details className="course-notes" open={note ? true : undefined}>
        <summary>{note ? "My notes" : "Add my notes"}</summary>
        <textarea
          defaultValue={note}
          placeholder="What will you do differently? A stock of yours this applies to? Saved in this browser."
          onBlur={(e) => onNote(e.target.value)}
          rows={3}
        />
      </details>
      {lesson.evidence.length ? (
        <details className="course-evidence">
          <summary>All {lesson.evidence.length} source tweets</summary>
          <Sources ids={lesson.evidence} dates={dates} max={lesson.evidence.length} />
        </details>
      ) : null}
    </article>
  );
}

/** Text with tweet ids written inline ("(2022: 1569264938648240131)") — the
    verifiers cite sources that way — gets the ids turned into dated links. */
function Linked({ text, dates }: { text: string; dates: Record<string, string> }) {
  const parts = text.split(/(\d{17,20})/g);
  if (parts.length === 1) return <>{text}</>;
  return (
    <>
      {parts.map((part, i) =>
        /^\d{17,20}$/.test(part) ? (
          <a key={i} className="course-src" href={tweetUrl(part)} target="_blank" rel="noreferrer noopener" title="Open the source tweet">
            {dates[part] ?? "tweet"}
          </a>
        ) : (
          <span key={i}>{part}</span>
        ),
      )}
    </>
  );
}

function matches(term: string, ...parts: Array<string | null | undefined>) {
  return parts.some((p) => (p ?? "").toLowerCase().includes(term));
}

function lessonMatches(l: Lesson, term: string, note?: string) {
  return matches(term, l.title, l.rule, l.when, l.evolution, l.setup, note, ...l.how.map((b) => b.text), ...l.mistakes.map((b) => b.text));
}

export function CoursePanel() {
  const [data, setData] = useState<CourseData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [view, setView] = useState<View>(() => readJson<View>(VIEW_KEY, "lessons"));
  const [active, setActive] = useState<string>(readModule);
  const [query, setQuery] = useState("");
  const [coreOnly, setCoreOnly] = useState(false);
  const [done, setDone] = useState<Record<string, true>>(() => readJson(DONE_KEY, {}));
  const [notes, setNotes] = useState<Record<string, string>>(() => readJson(NOTES_KEY, {}));
  const [zoom, setZoom] = useState<{ items: ChartItem[]; index: number } | null>(null);
  const [scrollTo, setScrollTo] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetch(`${import.meta.env.BASE_URL}course/course.json`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json() as Promise<CourseData>;
      })
      .then((d) => {
        if (!cancelled) setData(d);
      })
      .catch((e) => {
        if (!cancelled) setError(`The course file could not be loaded (${e instanceof Error ? e.message : String(e)}).`);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    try {
      localStorage.setItem(MODULE_KEY, active);
    } catch {
      /* per-viewer convenience only */
    }
  }, [active]);
  useEffect(() => writeJson(VIEW_KEY, view), [view]);

  // Scroll once the target lesson or case has rendered in its module.
  useEffect(() => {
    if (!scrollTo || view !== "lessons") return;
    const frame = requestAnimationFrame(() => {
      const el = document.getElementById(scrollTo);
      if (el) {
        el.scrollIntoView({ behavior: "smooth", block: "start" });
        el.classList.add("is-flash");
        window.setTimeout(() => el.classList.remove("is-flash"), 1400);
      }
      setScrollTo(null);
    });
    return () => cancelAnimationFrame(frame);
  }, [scrollTo, view, active]);

  const toggle = (id: string) =>
    setDone((prev) => {
      const next = { ...prev };
      if (next[id]) delete next[id];
      else next[id] = true;
      writeJson(DONE_KEY, next);
      return next;
    });

  const saveNote = (id: string, text: string) =>
    setNotes((prev) => {
      const trimmed = text.trim();
      if ((prev[id] ?? "") === trimmed) return prev;
      const next = { ...prev };
      if (trimmed) next[id] = trimmed;
      else delete next[id];
      writeJson(NOTES_KEY, next);
      return next;
    });

  const modules = useMemo(() => data?.modules ?? [], [data]);
  const allLessons = useMemo(
    () => modules.flatMap((m) => (m.lessons ?? []).map((l) => ({ ...l, moduleKey: m.key, moduleTitle: m.title }))),
    [modules],
  );
  const charts = useMemo(() => (data ? allCharts(data) : []), [data]);
  const casesByLesson = useMemo(() => {
    const out: Record<string, CaseStudy[]> = {};
    for (const m of modules) for (const c of m.cases ?? []) for (const id of c.lessons) (out[id] ??= []).push(c);
    return out;
  }, [modules]);
  const titleOf = useCallback((id: string) => allLessons.find((l) => l.id === id)?.title ?? id, [allLessons]);

  const goTo = useCallback((anchor: string) => {
    setZoom(null);
    setQuery("");
    setView("lessons");
    setActive(anchor.startsWith("case-") ? "cases" : moduleOfLesson(anchor));
    setScrollTo(anchor);
  }, []);
  const onZoom = useCallback((items: ChartItem[], index: number) => setZoom({ items, index }), []);

  const doneCount = allLessons.filter((l) => done[l.id]).length;
  const nextUp = allLessons.find((l) => !done[l.id]);
  const term = query.trim().toLowerCase();
  const dates = data?.tweet_dates ?? {};

  if (error || !data) {
    return (
      <Panel title="Course" subtitle="Lessons from @iManasArora's posts">
        <p className="course-empty">{error ?? "Loading the course…"}</p>
      </Panel>
    );
  }

  const current = modules.find((m) => m.key === active) ?? modules[0];
  const searching = term.length >= 2;
  const filterLesson = (l: Lesson) => (!coreOnly || l.strength === "core") && (!searching || lessonMatches(l, term, notes[l.id]));

  const renderModule = (m: Module) => {
    if (m.lessons) {
      const shown = m.lessons.map((l, i) => ({ l, i })).filter(({ l }) => filterLesson(l));
      if (searching && !shown.length) return null;
      return (
        <section key={m.key} className="course-module">
          {searching ? <h3 className="course-module-title">{m.title}</h3> : <p className="course-intro">{m.intro}</p>}
          {shown.map(({ l, i }) => (
            <LessonCard
              key={l.id}
              lesson={l}
              index={i + 1}
              dates={dates}
              done={!!done[l.id]}
              note={notes[l.id] ?? ""}
              trades={casesByLesson[l.id] ?? []}
              onToggle={() => toggle(l.id)}
              onNote={(text) => saveNote(l.id, text)}
              onZoom={onZoom}
              onOpenCase={(c) => goTo(caseAnchor(c))}
            />
          ))}
          {!shown.length ? <p className="course-empty">No core ideas in this module.</p> : null}
        </section>
      );
    }
    if (m.conditions) {
      const shown = m.conditions.filter((c) => !searching || matches(term, c.name, c.reads, c.does));
      if (searching && !shown.length) return null;
      return (
        <section key={m.key} className="course-module">
          {searching ? <h3 className="course-module-title">{m.title}</h3> : <p className="course-intro">{m.intro}</p>}
          {shown.map((c, i) => (
            <article key={c.name} className="course-lesson">
              <header className="course-lesson-head">
                <span className="course-num">P{i + 1}</span>
                <h3>{c.name}</h3>
              </header>
              <div className="course-play">
                <div>
                  <h4>How he reads it</h4>
                  <p>{c.reads}</p>
                </div>
                <div>
                  <h4>What he does</h4>
                  <p>{c.does}</p>
                </div>
              </div>
              {c.periods.length ? (
                <>
                  <h4>When he called it</h4>
                  <ul className="course-list">
                    {c.periods.map((p, j) => (
                      <li key={j}>
                        <span className="course-mono">
                          {p.from} → {p.to}
                        </span>{" "}
                        {p.note} <Sources ids={p.ids} dates={dates} max={3} />
                      </li>
                    ))}
                  </ul>
                </>
              ) : null}
              {c.lessons.length ? (
                <p className="course-related">
                  Related lessons:{" "}
                  {c.lessons.map((id) => (
                    <button key={id} type="button" className="course-link-button" onClick={() => goTo(id)}>
                      {titleOf(id)}
                    </button>
                  ))}
                </p>
              ) : null}
            </article>
          ))}
        </section>
      );
    }
    if (m.cases) {
      const shown = m.cases.filter((c) => !searching || matches(term, c.symbol, c.setup, c.story, c.takeaway));
      if (searching && !shown.length) return null;
      return (
        <section key={m.key} className="course-module">
          {searching ? (
            <h3 className="course-module-title">{m.title}</h3>
          ) : (
            <p className="course-intro">
              {m.intro} Want to test yourself first?{" "}
              <button type="button" className="course-link-button" onClick={() => setView("practice")}>
                Replay them one decision at a time in Practice
              </button>
            </p>
          )}
          {shown.map((c, i) => {
            const chart: ChartItem | null =
              c.image || c.local
                ? { key: c.root, local: c.local, image: c.image, caption: `${c.symbol} entry chart`, tweet: c.root, date: c.entry_date }
                : null;
            const ticker = caseTicker(c.symbol);
            return (
              <article key={`${c.symbol}-${c.root}`} className="course-lesson" id={caseAnchor(c)}>
                <header className="course-lesson-head">
                  <span className="course-num">C{i + 1}</span>
                  <h3>
                    <span className="course-mono">{c.symbol}</span> · {c.setup}
                  </h3>
                  <span
                    className={`course-result${c.result_pct == null ? "" : c.result_pct > 0 ? " is-up" : c.result_pct < 0 ? " is-down" : ""}`}
                    title={c.result_note || undefined}
                  >
                    {c.result_pct == null ? "result not stated" : `${c.result_pct > 0 ? "+" : ""}${c.result_pct.toFixed(1)}%`}
                  </span>
                </header>
                <div className="course-chips">
                  <span className="course-chip">Entered {c.entry_date}</span>
                  {c.context ? <span className="course-chip">{c.context}</span> : null}
                </div>
                <div className="course-case">
                  {chart ? <Chart item={chart} onZoom={() => onZoom([chart], 0)} /> : null}
                  <div className="course-case-body">
                    <p>{c.story}</p>
                    {c.timeline.length ? (
                      <>
                        <h4>Trade log</h4>
                        <ol className="course-list">
                          {c.timeline.map((t, j) => (
                            <li key={j}>
                              <span className="course-mono">{t.date}</span> <strong>{t.action}</strong>
                              {t.price != null && t.price !== "" ? <span className="course-mono"> @ {t.price}</span> : null} — {t.text}{" "}
                              {dates[t.tweet] ? <Sources ids={[t.tweet]} dates={dates} /> : null}
                            </li>
                          ))}
                        </ol>
                      </>
                    ) : null}
                    {c.takeaway ? (
                      <p className="course-takeaway">
                        <strong>Takeaway:</strong> {c.takeaway}
                      </p>
                    ) : null}
                    <p className="course-related">
                      {c.lessons.length ? "Lessons it shows: " : null}
                      {c.lessons.map((id) => (
                        <button key={id} type="button" className="course-link-button" onClick={() => goTo(id)}>
                          {titleOf(id)}
                        </button>
                      ))}
                      {ticker ? (
                        <a className="course-link-button" href={fullChartUrl(ticker)} target="_blank" rel="noreferrer noopener">
                          {ticker} chart today
                        </a>
                      ) : null}
                    </p>
                  </div>
                </div>
              </article>
            );
          })}
        </section>
      );
    }
    return null;
  };

  const views: { key: View; label: string; Icon: typeof BookOpen; count: string }[] = [
    { key: "lessons", label: "Lessons", Icon: BookOpen, count: `${doneCount}/${allLessons.length}` },
    { key: "gallery", label: "Chart gallery", Icon: Images, count: String(charts.length) },
    { key: "practice", label: "Practice", Icon: Dumbbell, count: "4 drills" },
  ];

  return (
    <Panel
      title="Course"
      subtitle={`How @${HANDLE} trades, from his posts · ${data.span[0].slice(0, 4)}–${data.span[1].slice(0, 4)}`}
      className="course-panel"
    >
      <p className="course-notice">
        Unofficial study notes compiled from his public posts and checked against the tweets each point cites. He did not write or review them.
        Nothing here is a recommendation to buy or sell anything.
      </p>
      <div className="course-views" role="tablist" aria-label="Course views">
        {views.map(({ key, label, Icon, count }) => (
          <button
            key={key}
            type="button"
            role="tab"
            aria-selected={view === key}
            className={`course-view${view === key ? " is-active" : ""}`}
            onClick={() => setView(key)}
          >
            <Icon size={15} aria-hidden /> {label} <span className="course-mono">{count}</span>
          </button>
        ))}
      </div>
      {view === "gallery" ? <CourseGallery data={data} charts={charts} onZoom={onZoom} /> : null}
      {view === "practice" ? (
        <CoursePractice data={data} lessons={allLessons} charts={charts} onOpenLesson={goTo} onZoom={onZoom} />
      ) : null}
      {view === "lessons" ? (
        <>
          <div className="course-toolbar">
            <input
              id="course-search"
              type="search"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search all lessons and your notes: VCP, breadth, stop, pyramiding…"
              aria-label="Search lessons"
            />
            <label className="course-toggle">
              <input type="checkbox" checked={coreOnly} onChange={(e) => setCoreOnly(e.target.checked)} /> Core ideas only
            </label>
            <span className="course-progress">
              {doneCount} of {allLessons.length} studied
              <span className="course-bar">
                <i style={{ width: `${allLessons.length ? (100 * doneCount) / allLessons.length : 0}%` }} />
              </span>
            </span>
            {nextUp ? (
              <button type="button" className="course-link-button" onClick={() => goTo(nextUp.id)} title={nextUp.title}>
                Continue: {nextUp.title.length > 42 ? `${nextUp.title.slice(0, 40)}…` : nextUp.title}
              </button>
            ) : null}
          </div>
          <div className="course-layout">
            <nav className="course-rail" aria-label="Course modules">
              {modules.map((m, i) => {
                const count = m.lessons?.length ?? m.conditions?.length ?? m.cases?.length ?? 0;
                const studied = (m.lessons ?? []).filter((l) => done[l.id]).length;
                return (
                  <button
                    key={m.key}
                    type="button"
                    className={`course-rail-item${!searching && m.key === current.key ? " is-active" : ""}`}
                    onClick={() => {
                      setQuery("");
                      setActive(m.key);
                    }}
                  >
                    <span className="course-mono">{String(i + 1).padStart(2, "0")}</span>
                    <span className="course-rail-label">{m.title}</span>
                    <span className="course-rail-count">{m.lessons ? `${studied}/${count}` : count}</span>
                  </button>
                );
              })}
            </nav>
            <div className="course-content">
              {searching ? (
                <>
                  {modules.map(renderModule)}
                  {!modules.some((m) => renderModule(m)) ? <p className="course-empty">Nothing matches “{query}”.</p> : null}
                </>
              ) : (
                <>
                  <h3 className="course-module-title">{current.title}</h3>
                  {renderModule(current)}
                  <ModuleNav modules={modules} current={current} onGo={(key) => {
                    setActive(key);
                    window.scrollTo({ top: 0, behavior: "smooth" });
                  }} />
                </>
              )}
            </div>
          </div>
        </>
      ) : null}
      {zoom ? (
        <CourseLightbox
          items={zoom.items}
          index={zoom.index}
          onIndex={(index) => setZoom((z) => (z ? { ...z, index } : z))}
          onClose={() => setZoom(null)}
          onOpenLesson={goTo}
        />
      ) : null}
    </Panel>
  );
}

function ModuleNav({ modules, current, onGo }: { modules: Module[]; current: Module; onGo: (key: string) => void }) {
  const i = modules.findIndex((m) => m.key === current.key);
  const prev = modules[i - 1];
  const next = modules[i + 1];
  return (
    <nav className="course-module-nav" aria-label="Previous and next module">
      {prev ? (
        <button type="button" onClick={() => onGo(prev.key)}>
          <span>Previous</span> {prev.title}
        </button>
      ) : (
        <span />
      )}
      {next ? (
        <button type="button" className="is-next" onClick={() => onGo(next.key)}>
          <span>Next</span> {next.title}
        </button>
      ) : null}
    </nav>
  );
}
