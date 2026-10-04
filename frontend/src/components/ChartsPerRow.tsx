import { useEffect, useState } from "react";

/* "Charts per row" for the look-alike pages: quick buttons plus any number,
   and chart heights that grow as the row empties — one chart a row is a big
   chart to study. The choice is remembered per page in this browser only. */

export const MAX_PER_ROW = 12;
const QUICK = [1, 2, 3, 4, 6];

function clamp(n: number) {
  return Math.min(MAX_PER_ROW, Math.max(1, Math.round(n) || 1));
}

export function useChartsPerRow(storageKey: string, fallback: number): [number, (n: number) => void] {
  const [cols, setColsState] = useState<number>(() => {
    try {
      const v = Number(window.localStorage.getItem(storageKey));
      return v >= 1 && v <= MAX_PER_ROW ? v : fallback;
    } catch {
      return fallback;
    }
  });
  const setCols = (n: number) => {
    const v = clamp(n);
    setColsState(v);
    try {
      window.localStorage.setItem(storageKey, String(v));
    } catch {
      /* a per-viewer convenience only */
    }
  };
  return [cols, setCols];
}

/** Chart height for `cols` charts across: about a 3:2 chart at the column's
    width, never taller than most of the screen nor shorter than readable. */
export function useChartHeight(cols: number, perCard = 1): number {
  const [size, setSize] = useState(() => ({
    w: typeof window === "undefined" ? 1200 : window.innerWidth,
    h: typeof window === "undefined" ? 800 : window.innerHeight,
  }));
  useEffect(() => {
    const on = () => setSize({ w: window.innerWidth, h: window.innerHeight });
    window.addEventListener("resize", on);
    return () => window.removeEventListener("resize", on);
  }, []);
  const colWidth = Math.min(size.w, 1400) / (cols * perCard);
  return Math.round(Math.max(140, Math.min(colWidth * 0.62, size.h * 0.78)));
}

export function ChartsPerRow({ value, onChange }: { value: number; onChange: (n: number) => void }) {
  const [draft, setDraft] = useState(String(value));
  useEffect(() => setDraft(String(value)), [value]);
  return (
    <span className="charts-per-row">
      Charts per row{" "}
      {QUICK.map((n) => (
        <button
          key={n}
          type="button"
          className={`lookalike-filter${value === n ? " is-active" : ""}`}
          onClick={() => onChange(n)}
          aria-pressed={value === n}
        >
          {n}
        </button>
      ))}
      <input
        type="number"
        min={1}
        max={MAX_PER_ROW}
        value={draft}
        aria-label={`Charts per row, 1 to ${MAX_PER_ROW}`}
        onChange={(e) => {
          setDraft(e.target.value);
          const n = Number(e.target.value);
          if (n >= 1 && n <= MAX_PER_ROW) onChange(n);
        }}
        onBlur={() => setDraft(String(value))}
      />
    </span>
  );
}
