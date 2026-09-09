import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  QUADRANT_LABEL,
  QUADRANT_ORDER,
  TIMEFRAME_PARAMS,
  aggregateBySector,
  axisBound,
  buildRotation,
  prepareSeries,
  quadrantHistory,
  type Quadrant,
  type RotationTrail,
  type SeriesInput,
  type Timeframe,
} from "../lib/rotation";
import {
  getGroupRankHistory,
  type GroupRankHistoryResponse,
  type IndustryGroupsResponse,
  type MarketKey,
} from "../lib/api";

import "./RotationGraph.css";

/**
 * Tail choices per timeframe, in that timeframe's own units.
 *
 * Weekly gets shorter tails on purpose: the recorded history is ~11 weeks
 * deep, so an 8-week tail draws almost every observation for every group at
 * once and the chart turns into spaghetti. 5 weeks is a quarter of a rotation
 * and stays readable.
 */
const TAIL_OPTIONS: Record<Timeframe, readonly number[]> = {
  daily: [4, 8, 12],
  weekly: [3, 5, 8],
};
const DEFAULT_TAIL: Record<Timeframe, number> = { daily: 8, weekly: 5 };
const TOP_OPTIONS = [10, 20, 40, 0] as const; // 0 = every group
const PLAY_MS = 420;

type Universe = "groups" | "sectors";
type SortKey = "label" | "x" | "y" | "dwell";

type Props = {
  market: MarketKey;
  data: IndustryGroupsResponse | null;
  onOpenGroup: (groupId: string) => void;
};

function fmtSigned(value: number, digits = 1) {
  return `${value >= 0 ? "+" : ""}${value.toFixed(digits)}`;
}

function fmtDate(date: string | null) {
  if (!date) return "—";
  const d = new Date(`${date}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return date;
  return d.toLocaleDateString(undefined, { day: "2-digit", month: "short", year: "numeric", timeZone: "UTC" });
}

/** Catmull-Rom through the measured points, so a tail curves instead of kinking. */
function smoothPath(pts: Array<{ x: number; y: number }>): string {
  if (pts.length < 2) return "";
  if (pts.length === 2) return `M ${pts[0].x} ${pts[0].y} L ${pts[1].x} ${pts[1].y}`;
  let d = `M ${pts[0].x} ${pts[0].y}`;
  for (let i = 0; i < pts.length - 1; i += 1) {
    const p0 = pts[i - 1] ?? pts[i];
    const p1 = pts[i];
    const p2 = pts[i + 1];
    const p3 = pts[i + 2] ?? p2;
    const c1x = p1.x + (p2.x - p0.x) / 6;
    const c1y = p1.y + (p2.y - p0.y) / 6;
    const c2x = p2.x - (p3.x - p1.x) / 6;
    const c2y = p2.y - (p3.y - p1.y) / 6;
    d += ` C ${c1x.toFixed(1)} ${c1y.toFixed(1)} ${c2x.toFixed(1)} ${c2y.toFixed(1)} ${p2.x.toFixed(1)} ${p2.y.toFixed(1)}`;
  }
  return d;
}

export function RotationGraph({ market, data, onOpenGroup }: Props) {
  const [history, setHistory] = useState<GroupRankHistoryResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [timeframe, setTimeframe] = useState<Timeframe>("daily");
  const [universe, setUniverse] = useState<Universe>("groups");
  const [tail, setTail] = useState<number>(DEFAULT_TAIL.daily);
  // 20, not 10: ranking by score means a small N is all leaders, so every
  // group lands top-right and there is nothing to rotate against. 20 populates
  // all four quadrants on live data while staying legible.
  const [topN, setTopN] = useState<number>(20);
  const [hidden, setHidden] = useState<Set<Quadrant>>(() => new Set());
  const [sort, setSort] = useState<{ key: SortKey; desc: boolean }>({ key: "x", desc: true });
  const [step, setStep] = useState<number | null>(null); // null = latest period
  const [playing, setPlaying] = useState(false);
  const [hover, setHover] = useState<{ trail: RotationTrail; x: number; y: number } | null>(null);
  const [pinned, setPinned] = useState<string | null>(null);

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
      setSize({ w, h: Math.round(Math.min(700, Math.max(380, w * 0.82))) });
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
    getGroupRankHistory(market)
      .then((r) => { if (alive) setHistory(r); })
      .catch((e: unknown) => { if (alive) setError(e instanceof Error ? e.message : "Could not load rank history"); })
      .finally(() => { if (alive) setLoading(false); });
    return () => { alive = false; };
  }, [market]);

  // Raw per-group series, before any timeframe or as-of choice.
  const series = useMemo<SeriesInput[]>(() => {
    if (!history || !data?.groups?.length) return [];
    const ranked = [...data.groups].sort((a, b) => b.score - a.score);
    const groupSeries = ranked.map((g) => ({
      groupId: g.group_id,
      label: g.group_name,
      parentSector: g.parent_sector,
      stockCount: g.stock_count,
      scores: (history.groups[g.group_id] ?? []).map((p) => ({ date: p.date, score: p.score })),
    }));
    // Sectors roll up every group, not just the top N -- a sector is only
    // itself if all of its groups are in it.
    if (universe === "sectors") return aggregateBySector(groupSeries);
    return topN > 0 ? groupSeries.slice(0, topN) : groupSeries;
  }, [history, data, universe, topN]);

  const { prepared, periods } = useMemo(() => prepareSeries(series, timeframe), [series, timeframe]);

  // Clamp the slider whenever the calendar changes under it (switching daily to
  // weekly turns 47 periods into 10).
  useEffect(() => { setStep(null); setPlaying(false); }, [timeframe, universe]);
  useEffect(() => { setTail(DEFAULT_TAIL[timeframe]); }, [timeframe]);

  const maxStep = Math.max(0, periods.length - 1);
  const stepIndex = step === null ? maxStep : Math.min(step, maxStep);
  const asOfDate = periods[stepIndex] ?? null;
  const isLive = step === null || stepIndex >= maxStep;

  const { trails: allTrails, centre } = useMemo(
    () => buildRotation(prepared, { timeframe, tailLength: tail, asOfDate: asOfDate ?? undefined }),
    [prepared, timeframe, tail, asOfDate],
  );

  const counts = useMemo(() => {
    const c: Record<Quadrant, number> = { leading: 0, weakening: 0, lagging: 0, improving: 0 };
    for (const t of allTrails) c[t.quadrant] += 1;
    return c;
  }, [allTrails]);

  /**
   * Hidden quadrants are hidden with a class, not filtered out of the rendered
   * array: filtering changed the array identity and made React tear down and
   * rebuild every trail's ~10 SVG nodes, which measured 1.0s on the 91-group
   * view. Toggling a class on stable nodes is instant.
   */
  const visibleCount = useMemo(
    () => allTrails.reduce((n, t) => (hidden.has(t.quadrant) ? n : n + 1), 0),
    [allTrails, hidden],
  );
  const trails = useMemo(
    () => allTrails.filter((t) => !hidden.has(t.quadrant)),
    [allTrails, hidden],
  );

  const rows = useMemo(() => {
    const dir = sort.desc ? -1 : 1;
    const value = (t: RotationTrail) => {
      const head = t.points[t.points.length - 1];
      if (sort.key === "label") return t.label.toLowerCase();
      if (sort.key === "x") return head.x;
      if (sort.key === "y") return head.y;
      return t.periodsInQuadrant;
    };
    return [...trails].sort((a, b) => {
      const va = value(a);
      const vb = value(b);
      if (typeof va === "string" || typeof vb === "string") {
        return String(va).localeCompare(String(vb)) * dir;
      }
      return (va - vb) * dir;
    });
  }, [trails, sort]);

  // Bounds come from every trail, hidden ones included, so toggling a quadrant
  // off hides dots without sliding the remaining ones across the chart.
  const bx = useMemo(() => axisBound(allTrails, "x"), [allTrails]);
  const by = useMemo(() => axisBound(allTrails, "y"), [allTrails]);

  const strip = useMemo(() => {
    if (!periods.length) return [];
    // One point per period is cheap at this scale (<=90 periods x <=94 groups)
    // and it is the only way to show where today sits in the cycle.
    return quadrantHistory(prepared, timeframe, periods);
  }, [prepared, timeframe, periods]);

  /**
   * Playback advances one period per timeout, keyed on the period it is
   * showing, rather than an interval that mutates state from inside a setState
   * updater. The first version did the latter -- and stopping playback from
   * inside the updater made it impure, so React's double-invocation ran the
   * whole animation in a single tick and it looked like the button did nothing.
   */
  useEffect(() => {
    if (!playing) return;
    if (!periods.length || stepIndex >= maxStep) { setPlaying(false); return; }
    const id = window.setTimeout(() => {
      const next = stepIndex + 1;
      setStep(next >= maxStep ? null : next);
    }, PLAY_MS);
    return () => window.clearTimeout(id);
  }, [playing, stepIndex, maxStep, periods.length]);

  const startPlay = useCallback(() => {
    // Replaying from the live edge rewinds first, so there is something to play.
    if (step === null || step >= maxStep) setStep(Math.max(0, maxStep - Math.min(maxStep, 24)));
    setPlaying(true);
  }, [step, maxStep]);

  const toggleQuadrant = useCallback((q: Quadrant) => {
    setHidden((prev) => {
      const next = new Set(prev);
      if (next.has(q)) next.delete(q);
      else next.add(q);
      return next;
    });
  }, []);

  const PAD = { l: 52, r: 18, t: 20, b: 40 };
  const innerW = Math.max(10, size.w - PAD.l - PAD.r);
  const innerH = Math.max(10, size.h - PAD.t - PAD.b);
  const sx = useCallback((v: number) => PAD.l + ((v + bx) / (2 * bx)) * innerW, [bx, innerW]);
  const sy = useCallback((v: number) => PAD.t + innerH - ((v + by) / (2 * by)) * innerH, [by, innerH]);

  const showTip = useCallback((trail: RotationTrail, ev: React.MouseEvent) => {
    const box = wrapRef.current?.getBoundingClientRect();
    setHover({ trail, x: ev.clientX - (box?.left ?? 0), y: ev.clientY - (box?.top ?? 0) });
  }, []);

  const params = TIMEFRAME_PARAMS[timeframe];
  // Sector trails are rolled up here, not recorded by the backend, so there is
  // no group page behind them: they read, they do not click through.
  const canOpen = universe === "groups";
  const open = useCallback((groupId: string) => { if (canOpen) onOpenGroup(groupId); }, [canOpen, onOpenGroup]);

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
  if (!allTrails.length) {
    return (
      <div className="rrg-empty">
        Not enough recorded {params.unit}s yet — a {universe === "sectors" ? "sector" : "group"} needs{" "}
        {params.lag + 1} {timeframe === "weekly" ? "weekly" : "daily"} snapshots before its rotation can
        be plotted.
        {timeframe === "weekly" ? (
          <>
            <br />
            <button type="button" className="rrg-inline-btn" onClick={() => setTimeframe("daily")}>
              Switch to daily
            </button>
          </>
        ) : null}
      </div>
    );
  }

  const zeroX = sx(0);
  const zeroY = sy(0);
  const activeId = pinned ?? hover?.trail.groupId ?? null;

  return (
    <div className="rrg-root">
      <div className="rrg-controls">
        <div className="rrg-control-group">
          <div className="rrg-seg" role="group" aria-label="What to plot">
            {(["groups", "sectors"] as const).map((u) => (
              <button
                key={u}
                type="button"
                className={`rrg-seg-btn${universe === u ? " active" : ""}`}
                onClick={() => setUniverse(u)}
                aria-pressed={universe === u}
              >
                {u === "groups" ? "Groups" : "Sectors"}
              </button>
            ))}
          </div>
          <div className="rrg-seg" role="group" aria-label="Timeframe">
            {(["daily", "weekly"] as const).map((t) => (
              <button
                key={t}
                type="button"
                className={`rrg-seg-btn${timeframe === t ? " active" : ""}`}
                onClick={() => setTimeframe(t)}
                aria-pressed={timeframe === t}
              >
                {t === "daily" ? "Daily" : "Weekly"}
              </button>
            ))}
          </div>
          {universe === "groups" ? (
            <div className="rrg-seg" role="group" aria-label="How many groups">
              {TOP_OPTIONS.map((n) => (
                <button
                  key={n}
                  type="button"
                  className={`rrg-seg-btn${topN === n ? " active" : ""}`}
                  onClick={() => setTopN(n)}
                  aria-pressed={topN === n}
                >
                  {n === 0 ? "All" : `Top ${n}`}
                </button>
              ))}
            </div>
          ) : null}
          <div className="rrg-seg" role="group" aria-label="Tail length">
            {TAIL_OPTIONS[timeframe].map((t) => (
              <button
                key={t}
                type="button"
                className={`rrg-seg-btn${tail === t ? " active" : ""}`}
                onClick={() => setTail(t)}
                aria-pressed={tail === t}
              >
                {t}
                {params.unitShort} tail
              </button>
            ))}
          </div>
        </div>
        <p className="rrg-note">
          {params.smooth}-{params.unit} smoothed strength against the median{" "}
          {universe === "sectors" ? "sector" : "group"}, plotted against its own {params.lag}-
          {params.unit} momentum. Rotation runs clockwise: Improving → Leading → Weakening → Lagging.
        </p>
      </div>

      <div className="rrg-body">
        <div className="rrg-canvas-wrap" ref={attachWrap}>
          {!isLive ? (
            <div className="rrg-asof-badge">
              As of {fmtDate(asOfDate)}
              <button type="button" onClick={() => { setStep(null); setPlaying(false); }}>
                back to latest
              </button>
            </div>
          ) : null}
          {size.w > 0 ? (
            <svg
              className="rrg-canvas"
              width={size.w}
              height={size.h}
              viewBox={`0 0 ${size.w} ${size.h}`}
              role="img"
              aria-label={`Rotation graph as of ${fmtDate(asOfDate)}: ${allTrails.length} ${universe}. ${counts.leading} leading, ${counts.improving} improving, ${counts.weakening} weakening, ${counts.lagging} lagging.`}
            >
              {/* quadrant grounds */}
              <rect className="rrg-q leading" x={zeroX} y={PAD.t} width={Math.max(0, PAD.l + innerW - zeroX)} height={Math.max(0, zeroY - PAD.t)} />
              <rect className="rrg-q weakening" x={zeroX} y={zeroY} width={Math.max(0, PAD.l + innerW - zeroX)} height={Math.max(0, PAD.t + innerH - zeroY)} />
              <rect className="rrg-q lagging" x={PAD.l} y={zeroY} width={Math.max(0, zeroX - PAD.l)} height={Math.max(0, PAD.t + innerH - zeroY)} />
              <rect className="rrg-q improving" x={PAD.l} y={PAD.t} width={Math.max(0, zeroX - PAD.l)} height={Math.max(0, zeroY - PAD.t)} />

              {/* gridlines at half-scale, so distance from the origin is readable */}
              {[-0.5, 0.5].map((f) => (
                <g key={f} className="rrg-grid">
                  <line x1={sx(bx * f)} x2={sx(bx * f)} y1={PAD.t} y2={PAD.t + innerH} />
                  <line y1={sy(by * f)} y2={sy(by * f)} x1={PAD.l} x2={PAD.l + innerW} />
                </g>
              ))}

              <text className="rrg-q-label" x={PAD.l + innerW - 10} y={PAD.t + 16} textAnchor="end">LEADING</text>
              <text className="rrg-q-label" x={PAD.l + innerW - 10} y={PAD.t + innerH - 8} textAnchor="end">WEAKENING</text>
              <text className="rrg-q-label" x={PAD.l + 10} y={PAD.t + innerH - 8}>LAGGING</text>
              <text className="rrg-q-label" x={PAD.l + 10} y={PAD.t + 16}>IMPROVING</text>

              <line className="rrg-axis" x1={PAD.l} x2={PAD.l + innerW} y1={zeroY} y2={zeroY} />
              <line className="rrg-axis" x1={zeroX} x2={zeroX} y1={PAD.t} y2={PAD.t + innerH} />
              <text className="rrg-axis-title" x={PAD.l + innerW / 2} y={size.h - 10} textAnchor="middle">
                relative strength vs median {universe === "sectors" ? "sector" : "group"} →
              </text>
              <text className="rrg-axis-title" x={16} y={PAD.t + innerH / 2} textAnchor="middle" transform={`rotate(-90 16 ${PAD.t + innerH / 2})`}>
                {params.lag}-{params.unit} momentum →
              </text>

              {allTrails.map((t) => {
                const head = t.points[t.points.length - 1];
                const screen = t.points.map((p) => ({ x: sx(p.x), y: sy(p.y) }));
                const isHot = activeId === t.groupId;
                const dim = activeId !== null && !isHot;
                const off = hidden.has(t.quadrant);
                return (
                  <g
                    key={t.groupId}
                    className={`rrg-trail ${t.quadrant}${isHot ? " is-hot" : ""}${dim ? " is-dim" : ""}${off ? " is-hidden" : ""}`}
                    role={canOpen ? "button" : "img"}
                    tabIndex={0}
                    aria-label={`${t.label}, ${QUADRANT_LABEL[t.quadrant]}`}
                    onClick={() => open(t.groupId)}
                    onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(t.groupId); } }}
                    onMouseMove={(e) => showTip(t, e)}
                    onMouseLeave={() => setHover(null)}
                  >
                    {screen.length > 1 ? (
                      <>
                        <path className="rrg-tail-halo" d={smoothPath(screen)} />
                        <path className="rrg-tail" d={smoothPath(screen)} />
                      </>
                    ) : null}
                    {t.points.slice(0, -1).map((p, i) => (
                      <circle
                        key={p.date}
                        className="rrg-dot"
                        cx={screen[i].x}
                        cy={screen[i].y}
                        r={1.9}
                        opacity={0.2 + (0.55 * (i + 1)) / t.points.length}
                      />
                    ))}
                    <circle className="rrg-head-ring" cx={sx(head.x)} cy={sy(head.y)} r={8} />
                    <circle className="rrg-head" cx={sx(head.x)} cy={sy(head.y)} r={5} />
                    {(isHot || visibleCount <= 12) ? (
                      (() => {
                        // Flip the label inside the plot near the right edge --
                        // the wrap clips overflow, so "Unclassified" lost its
                        // tail off the side of the chart.
                        const hx = sx(head.x);
                        const flip = hx > PAD.l + innerW * 0.78;
                        return (
                          <text
                            className="rrg-head-label"
                            x={flip ? hx - 9 : hx + 9}
                            y={sy(head.y) + 3.5}
                            textAnchor={flip ? "end" : "start"}
                          >
                            {t.label.length > 22 ? `${t.label.slice(0, 21)}…` : t.label}
                          </text>
                        );
                      })()
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
                left: Math.min(Math.max(hover.x + 14, 8), Math.max(8, size.w - 244)),
                top: Math.max(8, hover.y - 12),
              }}
              role="presentation"
            >
              <strong>{hover.trail.label}</strong>
              <span className={`rrg-tt-q ${hover.trail.quadrant}`}>{QUADRANT_LABEL[hover.trail.quadrant]}</span>
              <span className="rrg-tt-meta">
                {universe === "sectors"
                  ? `${hover.trail.stockCount} stocks`
                  : `${hover.trail.parentSector} · ${hover.trail.stockCount} stocks`}
              </span>
              <span className="rrg-tt-meta">
                strength {fmtSigned(hover.trail.points.at(-1)!.x)} · momentum{" "}
                {fmtSigned(hover.trail.points.at(-1)!.y)}
              </span>
              <span className="rrg-tt-meta">
                {hover.trail.periodsInQuadrant} {params.unit}
                {hover.trail.periodsInQuadrant === 1 ? "" : "s"} in {QUADRANT_LABEL[hover.trail.quadrant].toLowerCase()}
              </span>
              {canOpen ? <span className="rrg-tt-hint">Click to open the group</span> : null}
            </div>
          ) : null}
        </div>

        <div className="rrg-side">
          <div className="rrg-legend" role="group" aria-label="Show or hide quadrants">
            {QUADRANT_ORDER.map((q) => {
              const off = hidden.has(q);
              return (
                <button
                  key={q}
                  type="button"
                  className={`rrg-legend-btn ${q}${off ? " is-off" : ""}`}
                  onClick={() => toggleQuadrant(q)}
                  aria-pressed={!off}
                  title={off ? `Show ${QUADRANT_LABEL[q]}` : `Hide ${QUADRANT_LABEL[q]}`}
                >
                  <span className={`rrg-legend-dot ${q}`} />
                  <span className="rrg-legend-name">{QUADRANT_LABEL[q]}</span>
                  <em>{counts[q]}</em>
                </button>
              );
            })}
          </div>

          <div className="rrg-table-wrap">
            <table className={`rrg-table${canOpen ? " is-clickable" : ""}`}>
              <thead>
                <tr>
                  {([
                    ["label", universe === "sectors" ? "Sector" : "Group", "left"],
                    ["x", "RS", "right"],
                    ["y", "Mom", "right"],
                    ["dwell", `In Q`, "right"],
                  ] as const).map(([key, label, align]) => (
                    <th
                      key={key}
                      className={`${align === "right" ? "num" : ""}${sort.key === key ? " sorted" : ""}`}
                      onClick={() =>
                        setSort((prev) =>
                          prev.key === key ? { key, desc: !prev.desc } : { key, desc: key !== "label" },
                        )
                      }
                      aria-sort={sort.key === key ? (sort.desc ? "descending" : "ascending") : "none"}
                    >
                      {label}
                      {sort.key === key ? <span className="rrg-sort">{sort.desc ? "▾" : "▴"}</span> : null}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((t) => {
                  const head = t.points[t.points.length - 1];
                  return (
                    <tr
                      key={t.groupId}
                      className={activeId === t.groupId ? "is-hot" : ""}
                      onMouseEnter={() => setPinned(t.groupId)}
                      onMouseLeave={() => setPinned(null)}
                      onClick={() => open(t.groupId)}
                    >
                      <td>
                        <span className={`rrg-row-dot ${t.quadrant}`} />
                        <span className="rrg-row-label">{t.label}</span>
                        <small>{t.stockCount}</small>
                      </td>
                      <td className={`num ${head.x >= 0 ? "up" : "down"}`}>{fmtSigned(head.x)}</td>
                      <td className={`num ${head.y >= 0 ? "up" : "down"}`}>{fmtSigned(head.y)}</td>
                      <td className="num">{t.periodsInQuadrant}</td>
                    </tr>
                  );
                })}
                {!rows.length ? (
                  <tr><td colSpan={4} className="rrg-table-empty">Every quadrant is hidden.</td></tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="rrg-timeline">
        <div className="rrg-timeline-head">
          <div className="rrg-player">
            <button
              type="button"
              className="rrg-icon-btn"
              onClick={() => { setPlaying(false); setStep(Math.max(0, stepIndex - 1)); }}
              disabled={stepIndex <= 0}
              aria-label={`One ${params.unit} back`}
            >
              ‹
            </button>
            <button
              type="button"
              className="rrg-icon-btn primary"
              onClick={() => (playing ? setPlaying(false) : startPlay())}
              aria-label={playing ? "Pause" : "Play the rotation"}
            >
              {playing ? "❚❚" : "▶"}
            </button>
            <button
              type="button"
              className="rrg-icon-btn"
              onClick={() => { setPlaying(false); setStep(stepIndex + 1 >= maxStep ? null : stepIndex + 1); }}
              disabled={isLive}
              aria-label={`One ${params.unit} forward`}
            >
              ›
            </button>
          </div>
          <label className="rrg-slider-wrap">
            <span className="sr-only">As-of {params.unit}</span>
            <input
              type="range"
              className="rrg-slider"
              min={0}
              max={maxStep}
              step={1}
              value={stepIndex}
              onChange={(e) => {
                setPlaying(false);
                const next = Number(e.target.value);
                setStep(next >= maxStep ? null : next);
              }}
              aria-valuetext={fmtDate(asOfDate)}
            />
          </label>
          <span className={`rrg-asof${isLive ? " is-live" : ""}`}>
            {isLive ? "Latest" : fmtDate(asOfDate)}
            <small>
              {stepIndex + 1}/{periods.length} {params.unit}s
            </small>
          </span>
        </div>

        {strip.length > 1 ? (
          <div className="rrg-strip" aria-hidden="true">
            {strip.map((s, i) => {
              const total = Math.max(1, s.total);
              return (
                <button
                  key={s.date}
                  type="button"
                  className={`rrg-strip-col${i === stepIndex ? " is-now" : ""}`}
                  title={`${fmtDate(s.date)} — ${s.counts.leading} leading, ${s.counts.improving} improving, ${s.counts.weakening} weakening, ${s.counts.lagging} lagging`}
                  onClick={() => { setPlaying(false); setStep(i >= maxStep ? null : i); }}
                  tabIndex={-1}
                >
                  {QUADRANT_ORDER.map((q) => (
                    <span
                      key={q}
                      className={`rrg-strip-seg ${q}`}
                      style={{ height: `${(s.counts[q] / total) * 100}%` }}
                    />
                  ))}
                </button>
              );
            })}
          </div>
        ) : null}
        <p className="rrg-note rrg-strip-note">
          Quadrant population per {params.unit} — leading (top) through lagging (bottom). Click a column
          or drag the slider to rewind the chart to that {params.unit}.
        </p>
      </div>

      <div className="rrg-footer">
        <p className="rrg-note">
          {allTrails.length} of {universe === "sectors" ? series.length : data.groups.length}{" "}
          {universe === "sectors" ? "sectors" : "groups"} · {periods.length} recorded {params.unit}s ·
          centre = median score {centre.toFixed(1)} as of {fmtDate(asOfDate)}
        </p>
      </div>
    </div>
  );
}
