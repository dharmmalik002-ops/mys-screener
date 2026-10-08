import { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import { ExternalLink, X as CloseIcon } from "lucide-react";
import { Panel } from "./Panel";
import "./CoursePanel.css";

/* The Course page: @iManasArora's approach, distilled from his 2021-2026 posts.
   Data is a static file written by backend/scripts/export_course.py — every
   bullet carries the ids of the tweets it rests on. Charts are served from our
   own copies (frontend/public/course/img) so they outlive a deleted tweet;
   X's CDN is only the fallback. */

type Bullet = { text: string; ids: string[] };
type Example = { tweet: string; image: string; local: string | null; caption: string };
type Lesson = {
  id: string;
  setup: string | null;
  title: string;
  rule: string;
  how: Bullet[];
  when: string;
  mistakes: Bullet[];
  evolution: string;
  examples: Example[];
  evidence: string[];
  years: number[];
  strength: "core" | "supporting";
};
type Condition = {
  name: string;
  reads: string;
  does: string;
  periods: { from: string; to: string; note: string; ids: string[] }[];
  lessons: string[];
  evidence: string[];
};
type CaseStudy = {
  symbol: string;
  setup: string;
  entry_date: string;
  context: string;
  story: string;
  takeaway: string;
  result_pct: number | null;
  result_note: string;
  image: string | null;
  local: string | null;
  root: string;
  lessons: string[];
  timeline: { date: string; action: string; price: number | string | null; text: string; tweet: string }[];
};
type Module = {
  key: string;
  title: string;
  intro: string;
  lessons?: Lesson[];
  conditions?: Condition[];
  cases?: CaseStudy[];
};
type CourseData = { source: string; span: [string, string]; tweet_dates: Record<string, string>; modules: Module[] };

const DONE_KEY = "mr-malik-course-done:v1";
const MODULE_KEY = "mr-malik-course-module:v1";
const HANDLE = "iManasArora";

function readDone(): Record<string, true> {
  try {
    return JSON.parse(localStorage.getItem(DONE_KEY) || "{}") || {};
  } catch {
    return {};
  }
}

function readModule(): string | null {
  try {
    return localStorage.getItem(MODULE_KEY);
  } catch {
    return null;
  }
}

const tweetUrl = (id: string) => `https://x.com/${HANDLE}/status/${id}`;
const sized = (url: string, size: "small" | "large") => `${url}&name=${size}`;
/** Our stored copy first; X's CDN only if the copy is missing. */
const chartSrc = (local: string | null | undefined, cdn: string | null | undefined, size: "small" | "large") =>
  local ? `${import.meta.env.BASE_URL}${local}` : cdn ? sized(cdn, size) : "";

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

function Chart({
  image,
  local,
  caption,
  tweet,
  onZoom,
}: {
  image: string | null;
  local: string | null;
  caption: string;
  tweet?: string;
  onZoom: (src: string, alt: string) => void;
}) {
  const [failed, setFailed] = useState(false);
  const src = failed ? chartSrc(null, image, "small") : chartSrc(local, image, "small");
  const full = failed ? chartSrc(null, image, "large") : chartSrc(local, image, "large");
  if (!src) return null;
  return (
    <figure className="course-figure">
      <button type="button" className="course-zoom" onClick={() => onZoom(full, caption)} aria-label="Enlarge chart">
        <img
          src={src}
          alt={caption}
          loading="lazy"
          referrerPolicy="no-referrer"
          onError={() => {
            if (!failed && local && image) setFailed(true);
          }}
        />
      </button>
      <figcaption>
        {caption}{" "}
        {tweet ? (
          <a className="course-src" href={tweetUrl(tweet)} target="_blank" rel="noreferrer noopener">
            tweet
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
  onToggle,
  onZoom,
}: {
  lesson: Lesson;
  index: number;
  dates: Record<string, string>;
  done: boolean;
  onToggle: () => void;
  onZoom: (src: string, alt: string) => void;
}) {
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
      {lesson.examples.length ? (
        <div className="course-examples">
          {lesson.examples.map((e) => (
            <Chart key={e.image} image={e.image} local={e.local} caption={e.caption} tweet={e.tweet} onZoom={onZoom} />
          ))}
        </div>
      ) : null}
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

function lessonMatches(l: Lesson, term: string) {
  return matches(term, l.title, l.rule, l.when, l.evolution, l.setup, ...l.how.map((b) => b.text), ...l.mistakes.map((b) => b.text));
}

export function CoursePanel() {
  const [data, setData] = useState<CourseData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [active, setActive] = useState<string>(() => readModule() ?? "philosophy");
  const [query, setQuery] = useState("");
  const [coreOnly, setCoreOnly] = useState(false);
  const [done, setDone] = useState<Record<string, true>>(readDone);
  const [zoom, setZoom] = useState<{ src: string; alt: string } | null>(null);

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

  useEffect(() => {
    if (!zoom) return;
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && setZoom(null);
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [zoom]);

  const toggle = (id: string) =>
    setDone((prev) => {
      const next = { ...prev };
      if (next[id]) delete next[id];
      else next[id] = true;
      try {
        localStorage.setItem(DONE_KEY, JSON.stringify(next));
      } catch {
        /* per-viewer convenience only */
      }
      return next;
    });

  const modules = data?.modules ?? [];
  const allLessons = useMemo(() => modules.flatMap((m) => m.lessons ?? []), [modules]);
  const doneCount = allLessons.filter((l) => done[l.id]).length;
  const term = query.trim().toLowerCase();
  const onZoom = (src: string, alt: string) => setZoom({ src, alt });
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
  const filterLesson = (l: Lesson) => (!coreOnly || l.strength === "core") && (!searching || lessonMatches(l, term));

  const renderModule = (m: Module) => {
    if (m.lessons) {
      const shown = m.lessons.map((l, i) => ({ l, i })).filter(({ l }) => filterLesson(l));
      if (searching && !shown.length) return null;
      return (
        <section key={m.key} className="course-module">
          {searching ? <h3 className="course-module-title">{m.title}</h3> : <p className="course-intro">{m.intro}</p>}
          {shown.map(({ l, i }) => (
            <LessonCard key={l.id} lesson={l} index={i + 1} dates={dates} done={!!done[l.id]} onToggle={() => toggle(l.id)} onZoom={onZoom} />
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
                    <a key={id} href={`#${id}`} onClick={() => setActive(id.replace(/-\d+$/, ""))}>
                      {id}
                    </a>
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
          {searching ? <h3 className="course-module-title">{m.title}</h3> : <p className="course-intro">{m.intro}</p>}
          {shown.map((c, i) => (
            <article key={`${c.symbol}-${c.root}`} className="course-lesson">
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
                {c.image || c.local ? <Chart image={c.image} local={c.local} caption={`${c.symbol} entry chart`} tweet={c.root} onZoom={onZoom} /> : null}
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
                </div>
              </div>
            </article>
          ))}
        </section>
      );
    }
    return null;
  };

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
      <div className="course-toolbar">
        <input
          id="course-search"
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search all lessons: VCP, breadth, stop, pyramiding…"
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
            </>
          )}
        </div>
      </div>
      {zoom
        ? createPortal(
            <div className="course-lightbox" role="dialog" aria-modal="true" aria-label="Enlarged chart" onClick={() => setZoom(null)}>
              <button type="button" className="course-lightbox-close" onClick={() => setZoom(null)} aria-label="Close">
                <CloseIcon size={18} />
              </button>
              <img src={zoom.src} alt={zoom.alt} referrerPolicy="no-referrer" onClick={(e) => e.stopPropagation()} />
              <p onClick={(e) => e.stopPropagation()}>
                {zoom.alt}{" "}
                <a href={zoom.src} target="_blank" rel="noreferrer noopener">
                  Open image <ExternalLink size={12} />
                </a>
              </p>
            </div>,
            document.body,
          )
        : null}
    </Panel>
  );
}
