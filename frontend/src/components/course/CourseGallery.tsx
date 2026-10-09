import { useMemo, useState } from "react";
import { chartSrc, type ChartItem, type CourseData } from "./courseData";

/* Every chart the course shows, in one browsable wall. "Hide captions" turns
   it into a reading drill: look at the chart, decide what it shows, then
   reveal his note. */
export function CourseGallery({
  data,
  charts,
  onZoom,
}: {
  data: CourseData;
  charts: ChartItem[];
  onZoom: (items: ChartItem[], index: number) => void;
}) {
  const [moduleKey, setModuleKey] = useState<string>("all");
  const [query, setQuery] = useState("");
  const [order, setOrder] = useState<"newest" | "oldest" | "course">("course");
  const [hideCaptions, setHideCaptions] = useState(false);
  const [revealed, setRevealed] = useState<Record<string, true>>({});

  const counts = useMemo(() => {
    const out: Record<string, number> = {};
    for (const c of charts) out[c.moduleKey ?? ""] = (out[c.moduleKey ?? ""] ?? 0) + 1;
    return out;
  }, [charts]);

  const shown = useMemo(() => {
    const term = query.trim().toLowerCase();
    let list = charts.filter(
      (c) =>
        (moduleKey === "all" || c.moduleKey === moduleKey) &&
        (term.length < 2 || `${c.caption} ${c.lessonTitle ?? ""}`.toLowerCase().includes(term)),
    );
    if (order !== "course") {
      const sign = order === "newest" ? -1 : 1;
      list = list.slice().sort((a, b) => sign * (a.date ?? "").localeCompare(b.date ?? ""));
    }
    return list;
  }, [charts, moduleKey, query, order]);

  return (
    <div className="course-gallery">
      <p className="course-intro">
        All {charts.length} charts and screenshots the course uses, each tied to the lesson it illustrates. Click one to step through
        the set with ← and →.
      </p>
      <div className="course-gallery-filters">
        <button type="button" className={`course-pill${moduleKey === "all" ? " is-active" : ""}`} onClick={() => setModuleKey("all")}>
          All <span>{charts.length}</span>
        </button>
        {data.modules
          .filter((m) => counts[m.key])
          .map((m) => (
            <button
              key={m.key}
              type="button"
              className={`course-pill${moduleKey === m.key ? " is-active" : ""}`}
              onClick={() => setModuleKey(m.key)}
            >
              {m.title} <span>{counts[m.key]}</span>
            </button>
          ))}
      </div>
      <div className="course-toolbar">
        <input
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search captions: breadth, VCP, weekly, gap…"
          aria-label="Search charts"
        />
        <select value={order} onChange={(e) => setOrder(e.target.value as typeof order)} aria-label="Order charts">
          <option value="course">Course order</option>
          <option value="newest">Newest first</option>
          <option value="oldest">Oldest first</option>
        </select>
        <label className="course-toggle">
          <input
            type="checkbox"
            checked={hideCaptions}
            onChange={(e) => {
              setHideCaptions(e.target.checked);
              setRevealed({});
            }}
          />{" "}
          Hide captions (read the chart first)
        </label>
        <span className="course-progress">{shown.length} shown</span>
      </div>
      {shown.length ? (
        <div className="course-gallery-grid">
          {shown.map((c, i) => {
            const hidden = hideCaptions && !revealed[c.key];
            return (
              <figure key={c.key} className="course-figure">
                <button type="button" className="course-zoom" onClick={() => onZoom(shown, i)} aria-label="Enlarge chart">
                  <GalleryImage item={c} />
                </button>
                <figcaption>
                  {hidden ? (
                    <button type="button" className="course-reveal" onClick={() => setRevealed((r) => ({ ...r, [c.key]: true }))}>
                      What does this chart show? Reveal
                    </button>
                  ) : (
                    <>
                      {c.date ? <span className="course-mono">{c.date} · </span> : null}
                      {c.caption}
                      <span className="course-gallery-lesson">{c.lessonTitle}</span>
                    </>
                  )}
                </figcaption>
              </figure>
            );
          })}
        </div>
      ) : (
        <p className="course-empty">No chart matches.</p>
      )}
    </div>
  );
}

function GalleryImage({ item }: { item: ChartItem }) {
  const [failed, setFailed] = useState(false);
  const src = failed ? chartSrc(null, item.image, "small") : chartSrc(item.local, item.image, "small");
  return (
    <img
      src={src}
      alt={item.caption}
      loading="lazy"
      referrerPolicy="no-referrer"
      onError={() => {
        if (!failed && item.local && item.image) setFailed(true);
      }}
    />
  );
}
