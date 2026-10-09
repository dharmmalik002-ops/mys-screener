/* Shared shapes and helpers for the Course page. The data is the static
   frontend/public/course/course.json written by backend/scripts/export_course.py. */

export type Bullet = { text: string; ids: string[] };
export type Example = { tweet: string; image: string; local: string | null; caption: string };
export type Lesson = {
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
export type Condition = {
  name: string;
  reads: string;
  does: string;
  periods: { from: string; to: string; note: string; ids: string[] }[];
  lessons: string[];
  evidence: string[];
};
export type CaseStudy = {
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
export type Module = {
  key: string;
  title: string;
  intro: string;
  lessons?: Lesson[];
  conditions?: Condition[];
  cases?: CaseStudy[];
};
export type CourseData = { source: string; span: [string, string]; tweet_dates: Record<string, string>; modules: Module[] };

/** One chart as the gallery and the lightbox see it, wherever it came from. */
export type ChartItem = {
  key: string;
  local: string | null;
  image: string | null;
  caption: string;
  tweet?: string;
  date?: string;
  moduleKey?: string;
  moduleTitle?: string;
  lessonId?: string;
  lessonTitle?: string;
};

export const HANDLE = "iManasArora";

export const tweetUrl = (id: string) => `https://x.com/${HANDLE}/status/${id}`;
const sized = (url: string, size: "small" | "large") => `${url}${url.includes("?") ? "&" : "?"}name=${size}`;

/** Our stored copy first; X's CDN only if the copy is missing. */
export const chartSrc = (local: string | null | undefined, cdn: string | null | undefined, size: "small" | "large") =>
  local ? `${import.meta.env.BASE_URL}${local}` : cdn ? sized(cdn, size) : "";

/** Lesson ids are `<module key>-<n>`; module keys may contain underscores. */
export const moduleOfLesson = (lessonId: string) => lessonId.replace(/-\d+$/, "");

export const caseAnchor = (c: CaseStudy) => `case-${c.root}`;

/** "TVSMOTOR (short, futures)" / "IRCTC FEB FUT" -> the bare ticker. */
export const caseTicker = (symbol: string) => (symbol.trim().match(/^[A-Z0-9&-]+/)?.[0] ?? "").toUpperCase();

export function readJson<T>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key);
    return raw ? (JSON.parse(raw) as T) ?? fallback : fallback;
  } catch {
    return fallback;
  }
}

export function writeJson(key: string, value: unknown) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {
    /* per-viewer convenience only */
  }
}

export function shuffle<T>(items: T[]): T[] {
  const out = items.slice();
  for (let i = out.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

export const pick = <T,>(items: T[]): T => items[Math.floor(Math.random() * items.length)];

/** Every chart in the course: lesson examples first, then the case-study entry charts. */
export function allCharts(data: CourseData): ChartItem[] {
  const out: ChartItem[] = [];
  const seen = new Set<string>();
  for (const m of data.modules) {
    for (const l of m.lessons ?? []) {
      for (const e of l.examples) {
        const key = `${l.id}:${e.local ?? e.image}`;
        if (seen.has(key)) continue;
        seen.add(key);
        out.push({
          key,
          local: e.local,
          image: e.image,
          caption: e.caption,
          tweet: e.tweet,
          date: data.tweet_dates[e.tweet],
          moduleKey: m.key,
          moduleTitle: m.title,
          lessonId: l.id,
          lessonTitle: l.title,
        });
      }
    }
    for (const c of m.cases ?? []) {
      if (!c.local && !c.image) continue;
      out.push({
        key: caseAnchor(c),
        local: c.local,
        image: c.image,
        caption: `${c.symbol} entry chart — ${c.setup}`,
        tweet: c.root,
        date: c.entry_date,
        moduleKey: m.key,
        moduleTitle: m.title,
        lessonId: caseAnchor(c),
        lessonTitle: `${c.symbol} case study`,
      });
    }
  }
  return out;
}
