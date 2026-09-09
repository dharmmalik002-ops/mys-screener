import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  MOMENTUM_LAG,
  QUADRANT_LABEL,
  SMOOTH_WINDOW,
  axisBound,
  buildRotation,
  type RotationTrail,
} from "../lib/rotation";
import {
  getGroupRankHistory,
  type GroupRankHistoryResponse,
  type IndustryGroupsResponse,
  type MarketKey,
} from "../lib/api";

import "./RotationGraph.css";

const TAIL_OPTIONS = [4, 8, 12] as const;
const TOP_OPTIONS = [10, 20, 40] as const;

type Props = {
  market: MarketKey;
  data: IndustryGroupsResponse | null;
  onOpenGroup: (groupId: string) => void;
};

export function RotationGraph({ market, data, onOpenGroup }: Props) {
  const [history, setHistory] = useState<GroupRankHistoryResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tail, setTail] = useState<number>(8);
  // 20, not 10: ranking by score means a small N is all leaders, so every
  // group lands top-right and there is nothing to rotate against. 20 populates
  // all four quadrants on live data while staying legible.
  const [topN, setTopN] = useState<number>(20);
  const [hover, setHover] = useState<{ trail: RotationTrail; x: number; y: number } | null>(null);

  const wrapRef = useRef<HTMLDivElement | null>(null);
  const roRef = useRef<ResizeObserver | null>(null);
  const [size, setSize] = useState({ w: 0, h: 0 });

  /**
   * Callback ref rather than useLayoutEffect: this component early-returns a
   * loading state while the rank history is in flight, so an effect with an
   * empty dependency list runs before the canvas node exists and then never
   * runs again -- the chart stayed 2px tall. Attaching the observer when the
   * node itself appears cannot miss it.
   */
  const attachWrap = useCallback((el: HTMLDivElement | null) => {
    wrapRef.current = el;
    roRef.current?.disconnect();
    roRef.current = null;
    if (!el) return;
    const apply = (w: number) => {
      if (w <= 0) return;
      // Near-square: the four quadrants must read as equal area, or the eye
      // mis-judges which one a group sits in.
      setSize({ w, h: Math.round(Math.min(680, Math.max(360, w * 0.72))) });
    };
    apply(el.getBoundingClientRect().width);
    const ro = new ResizeObserver((entries) => apply(entries[0]?.contentRect.width ?? 0));
    ro.observe(el);
    roRef.current = ro;
  }, []);

  useEffect(() => () => roRef.current?.disconnect(), []);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setError(null);
    getGroupRankHistory(market, 40)
      .then((r) => { if (alive) setHistory(r); })
      .catch((e: unknown) => { if (alive) setError(e instanceof Error ? e.message : "Could not load rank history"); })
      .finally(() => { if (alive) setLoading(false); });
    return () => { alive = false; };
  }, [market]);

  const { trails, centre, sessions } = useMemo(() => {
    if (!history || !data?.groups?.length) return { trails: [], centre: 0, sessions: 0 };
    // Rank the candidates by today's score and keep the top N, so 94 groups do
    // not overlap into noise. The score is the same measure the axes use.
    const ranked = [...data.groups].sort((a, b) => b.score - a.score).slice(0, topN);
    return buildRotation(
      ranked.map((g) => ({
        groupId: g.group_id,
        label: g.group_name,
        parentSector: g.parent_sector,
        stockCount: g.stock_count,
        scores: (history.groups[g.group_id] ?? []).map((p) => ({ date: p.date, score: p.score })),
      })),
      tail,
    );
  }, [history, data, tail, topN]);

  const bx = useMemo(() => axisBound(trails, "x"), [trails]);
  const by = useMemo(() => axisBound(trails, "y"), [trails]);

  const PAD = { l: 46, r: 16, t: 18, b: 34 };
  const innerW = Math.max(10, size.w - PAD.l - PAD.r);
  const innerH = Math.max(10, size.h - PAD.t - PAD.b);
  const sx = useCallback((v: number) => PAD.l + ((v + bx) / (2 * bx)) * innerW, [bx, innerW]);
  const sy = useCallback((v: number) => PAD.t + innerH - ((v + by) / (2 * by)) * innerH, [by, innerH]);

  const showTip = useCallback((trail: RotationTrail, ev: React.MouseEvent) => {
    const box = wrapRef.current?.getBoundingClientRect();
    setHover({ trail, x: ev.clientX - (box?.left ?? 0), y: ev.clientY - (box?.top ?? 0) });
  }, []);

  const counts = useMemo(() => {
    const c: Record<string, number> = { leading: 0, weakening: 0, lagging: 0, improving: 0 };
    for (const t of trails) c[t.quadrant] += 1;
    return c;
  }, [trails]);

  if (error) return <div className="rrg-empty">{error}</div>;
  if (!data?.groups?.length) return <div className="rrg-empty">No group data yet.</div>;
  if (loading && !history) {
    return (
      <div className="rrg-empty">
        Loading rank history…
        <br />
        <small>The first load can take a moment; it is shared with the Home page afterwards.</small>
      </div>
    );
  }
  if (!trails.length) {
    return (
      <div className="rrg-empty">
        Not enough recorded sessions yet — a group needs {MOMENTUM_LAG + 1} daily
        snapshots before its rotation can be plotted.
      </div>
    );
  }

  const zeroX = sx(0);
  const zeroY = sy(0);

  return (
    <div className="rrg-root">
      <div className="rrg-controls">
        <p className="rrg-note">
          {SMOOTH_WINDOW}-session smoothed strength against the median group, plotted against its
          own {MOMENTUM_LAG}-session momentum. Groups rotate clockwise: Improving → Leading →
          Weakening → Lagging.
        </p>
        <div className="rrg-control-group">
          <div className="rrg-seg" role="group" aria-label="Tail length">
            {TAIL_OPTIONS.map((t) => (
              <button
                key={t}
                type="button"
                className={`rrg-seg-btn${tail === t ? " active" : ""}`}
                onClick={() => setTail(t)}
                aria-pressed={tail === t}
              >
                {t}d tail
              </button>
            ))}
          </div>
          <div className="rrg-seg" role="group" aria-label="How many groups">
            {TOP_OPTIONS.map((n) => (
              <button
                key={n}
                type="button"
                className={`rrg-seg-btn${topN === n ? " active" : ""}`}
                onClick={() => setTopN(n)}
                aria-pressed={topN === n}
              >
                Top {n}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="rrg-canvas-wrap" ref={attachWrap}>
        {size.w > 0 ? (
          <svg
            className="rrg-canvas"
            width={size.w}
            height={size.h}
            viewBox={`0 0 ${size.w} ${size.h}`}
            role="img"
            aria-label={`Rotation graph: ${trails.length} groups. ${counts.leading} leading, ${counts.improving} improving, ${counts.weakening} weakening, ${counts.lagging} lagging.`}
          >
            {/* quadrant grounds */}
            <rect className="rrg-q leading" x={zeroX} y={PAD.t} width={Math.max(0, PAD.l + innerW - zeroX)} height={Math.max(0, zeroY - PAD.t)} />
            <rect className="rrg-q weakening" x={zeroX} y={zeroY} width={Math.max(0, PAD.l + innerW - zeroX)} height={Math.max(0, PAD.t + innerH - zeroY)} />
            <rect className="rrg-q lagging" x={PAD.l} y={zeroY} width={Math.max(0, zeroX - PAD.l)} height={Math.max(0, PAD.t + innerH - zeroY)} />
            <rect className="rrg-q improving" x={PAD.l} y={PAD.t} width={Math.max(0, zeroX - PAD.l)} height={Math.max(0, zeroY - PAD.t)} />

            <text className="rrg-q-label" x={PAD.l + innerW - 8} y={PAD.t + 14} textAnchor="end">LEADING</text>
            <text className="rrg-q-label" x={PAD.l + innerW - 8} y={PAD.t + innerH - 6} textAnchor="end">WEAKENING</text>
            <text className="rrg-q-label" x={PAD.l + 8} y={PAD.t + innerH - 6}>LAGGING</text>
            <text className="rrg-q-label" x={PAD.l + 8} y={PAD.t + 14}>IMPROVING</text>

            <line className="rrg-axis" x1={PAD.l} x2={PAD.l + innerW} y1={zeroY} y2={zeroY} />
            <line className="rrg-axis" x1={zeroX} x2={zeroX} y1={PAD.t} y2={PAD.t + innerH} />
            <text className="rrg-axis-title" x={PAD.l + innerW / 2} y={size.h - 8} textAnchor="middle">
              relative strength vs median group →
            </text>
            <text className="rrg-axis-title" x={14} y={PAD.t + innerH / 2} textAnchor="middle" transform={`rotate(-90 14 ${PAD.t + innerH / 2})`}>
              momentum →
            </text>

            {trails.map((t) => {
              const head = t.points[t.points.length - 1];
              const d = t.points.map((p, i) => `${i ? "L" : "M"} ${sx(p.x).toFixed(1)} ${sy(p.y).toFixed(1)}`).join(" ");
              const isHot = hover?.trail.groupId === t.groupId;
              return (
                <g
                  key={t.groupId}
                  className={`rrg-trail ${t.quadrant}${isHot ? " is-hot" : ""}`}
                  role="button"
                  tabIndex={0}
                  aria-label={`${t.label}, ${QUADRANT_LABEL[t.quadrant]}`}
                  onClick={() => onOpenGroup(t.groupId)}
                  onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onOpenGroup(t.groupId); } }}
                  onMouseMove={(e) => showTip(t, e)}
                  onMouseLeave={() => setHover(null)}
                >
                  {t.points.length > 1 ? <path className="rrg-tail" d={d} /> : null}
                  {t.points.slice(0, -1).map((p, i) => (
                    <circle
                      key={p.date}
                      className="rrg-dot"
                      cx={sx(p.x)}
                      cy={sy(p.y)}
                      r={1.8}
                      opacity={0.25 + (0.5 * (i + 1)) / t.points.length}
                    />
                  ))}
                  <circle className="rrg-head" cx={sx(head.x)} cy={sy(head.y)} r={5} />
                  {(isHot || trails.length <= 12) ? (
                    <text className="rrg-head-label" x={sx(head.x) + 8} y={sy(head.y) + 3.5}>
                      {t.label.length > 20 ? `${t.label.slice(0, 19)}…` : t.label}
                    </text>
                  ) : null}
                </g>
              );
            })}
          </svg>
        ) : null}

        {hover ? (
          <div
            className="rrg-tooltip"
            style={{
              left: Math.min(Math.max(hover.x + 14, 8), Math.max(8, size.w - 224)),
              top: Math.max(8, hover.y - 12),
            }}
            role="presentation"
          >
            <strong>{hover.trail.label}</strong>
            <span className={`rrg-tt-q ${hover.trail.quadrant}`}>{QUADRANT_LABEL[hover.trail.quadrant]}</span>
            <span className="rrg-tt-meta">{hover.trail.parentSector} · {hover.trail.stockCount} stocks</span>
            <span className="rrg-tt-meta">
              strength {hover.trail.points.at(-1)!.x >= 0 ? "+" : ""}
              {hover.trail.points.at(-1)!.x.toFixed(1)} · momentum{" "}
              {hover.trail.points.at(-1)!.y >= 0 ? "+" : ""}
              {hover.trail.points.at(-1)!.y.toFixed(1)}
            </span>
            <span className="rrg-tt-hint">Click to open the group</span>
          </div>
        ) : null}
      </div>

      <div className="rrg-footer">
        <div className="rrg-legend">
          {(["leading", "improving", "weakening", "lagging"] as const).map((q) => (
            <span key={q} className="rrg-legend-item">
              <span className={`rrg-legend-dot ${q}`} />
              {QUADRANT_LABEL[q]} <em>{counts[q]}</em>
            </span>
          ))}
        </div>
        <p className="rrg-note">
          {trails.length} of {data.groups.length} groups · {sessions} recorded sessions · centre = median score {centre.toFixed(1)}
        </p>
      </div>
    </div>
  );
}
