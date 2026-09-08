import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { ChevronLeft } from "lucide-react";

import { squarify, type TreemapRect } from "../lib/treemap";
import type { IndustryGroupsResponse, IndustryGroupStockItem } from "../lib/api";

import "./SectorTreemap.css";

/* ============================== configuration ============================== */

type ReturnWindow = "1d" | "1w" | "1m" | "3m" | "6m";
type SizingMode = "cap" | "equal";

const WINDOWS: Array<{ key: ReturnWindow; label: string; full: string }> = [
  { key: "1d", label: "1D", full: "1 day" },
  { key: "1w", label: "1W", full: "1 week" },
  { key: "1m", label: "1M", full: "1 month" },
  { key: "3m", label: "3M", full: "3 months" },
  { key: "6m", label: "6M", full: "6 months" },
];

/**
 * Colour buckets are ABSOLUTE, not relative to today's spread. A scale
 * normalised to the day's min/max would paint a dead flat session in full
 * red and green and make every day look identical — the opposite of what a
 * heatmap is for. These thresholds are fixed per window so the same colour
 * means the same move on Monday as on Friday.
 */
const BANDS: Record<ReturnWindow, [number, number, number]> = {
  "1d": [0.5, 1.5, 3],
  "1w": [1, 3, 6],
  "1m": [2, 6, 12],
  "3m": [4, 12, 22],
  "6m": [5, 18, 35],
};

/** Below these pixel sizes a label is illegible, so it is dropped entirely.
 *  Kept deliberately low because truncate() already clips to the tile width —
 *  a clipped "Telecom…" is more use than a blank tile, and at 17px an 11px
 *  label still sits inside its box. */
const LABEL_MIN_W = 38;
const LABEL_MIN_H = 17;
const SUB_LABEL_MIN_H = 34;
const SECTOR_HEADER_H = 19;
/** Below this canvas width the map drops to a single level (see the tiles memo). */
const NARROW_WIDTH = 520;
/** Children projected below this many square pixels get folded into "+N more". */
const MIN_LABELLED_AREA = 620;
/**
 * Average pixels per parent needed before nesting a second level inside it.
 *
 * Measured against the live universe (29 sectors): at 1138x592 the root map
 * labels 60% of its tiles, which reads fine; at 636x369 -- the width the Groups
 * page actually gives when the chart pane is alongside -- two levels drop to
 * 41% labelled with 11 sub-pixel slivers, while ONE level puts 29 tiles at
 * ~90x90px each and labels all of them. So the level count is a measurement,
 * not a breakpoint: phone-vs-desktop was never the real variable, available
 * area per sector was.
 */
const MIN_PARENT_AREA_FOR_NESTING = 12000;

function bandOf(value: number, window: ReturnWindow): string {
  if (!Number.isFinite(value)) return "flat";
  const [t1, t2, t3] = BANDS[window];
  const magnitude = Math.abs(value);
  if (magnitude < t1) return "flat";
  const step = magnitude < t2 ? 1 : magnitude < t3 ? 2 : 3;
  return `${value >= 0 ? "pos" : "neg"}-${step}`;
}

function returnOf(stock: IndustryGroupStockItem, window: ReturnWindow): number {
  switch (window) {
    case "1d": return stock.change_pct;
    case "1w": return stock.return_1w;
    case "1m": return stock.return_1m;
    case "3m": return stock.return_3m;
    default: return stock.return_6m;
  }
}

function formatPct(value: number): string {
  if (!Number.isFinite(value)) return "—";
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatCap(crore: number): string {
  if (!Number.isFinite(crore)) return "—";
  if (crore >= 100000) return `₹${(crore / 100000).toFixed(2)} L Cr`;
  if (crore >= 1000) return `₹${(crore / 1000).toFixed(1)}k Cr`;
  return `₹${Math.round(crore)} Cr`;
}

/** Clips a label to what the tile can actually hold. ~6.1px per character at
 *  11px and ~7.2px at 13px in the UI font — measured, and deliberately
 *  conservative so a long group name never bleeds over the tile edge. */
function truncate(text: string, availableWidth: number, perChar = 6.1): string {
  const maxChars = Math.floor(availableWidth / perChar);
  if (maxChars <= 1) return "";
  if (text.length <= maxChars) return text;
  return `${text.slice(0, Math.max(1, maxChars - 1))}…`;
}

/* ============================== tree building ============================== */

type Leaf = {
  kind: "stock";
  id: string;
  label: string;
  title: string;
  weight: number;
  ret: number;
  cap: number;
  symbol: string;
};

type Branch = {
  kind: "sector" | "group" | "other";
  id: string;
  label: string;
  title: string;
  weight: number;
  ret: number;
  cap: number;
  children: Node[];
  count: number;
};

type Node = Branch | Leaf;

/** Weighted mean of children, using the same weights that drive tile area, so
 *  colour and size always answer the same question. */
function rollUp(children: Node[]): { weight: number; ret: number; cap: number; count: number } {
  let weight = 0;
  let weighted = 0;
  let cap = 0;
  let count = 0;
  for (const child of children) {
    const childCount = child.kind === "stock" ? 1 : child.count;
    count += childCount;
    cap += child.cap;
    if (!Number.isFinite(child.weight) || child.weight <= 0) continue;
    weight += child.weight;
    if (Number.isFinite(child.ret)) weighted += child.ret * child.weight;
  }
  return { weight, ret: weight > 0 ? weighted / weight : Number.NaN, cap, count };
}

/**
 * Folds children too small to carry a label into one aggregate tile.
 *
 * Measured on the live universe: 29 sectors and 94 groups over ~1,600 stocks
 * put 210 tiles on the root map, of which 144 were too small to label and 108
 * were under 18x14px. Half the map was unlabelled slivers. Synthetic mock data
 * hid this completely, because real market cap is far more concentrated than a
 * tidy power law.
 *
 * Area stays honest: the aggregate's weight is exactly the sum of what it
 * replaces, so the sector's total footprint is unchanged. It is deliberately
 * NOT clickable — "+7 more" is not a thing you can drill into — and the parent
 * header is still the way into the full sector.
 */
function foldSmallChildren(children: Node[], availableArea: number, minArea: number): Node[] {
  const total = children.reduce((sum, child) => sum + (child.weight > 0 ? child.weight : 0), 0);
  if (total <= 0 || availableArea <= 0) return children;

  const sorted = [...children].sort((a, b) => b.weight - a.weight);
  const keep: Node[] = [];
  const fold: Node[] = [];
  for (const child of sorted) {
    const projected = availableArea * (child.weight / total);
    // Always keep the two largest, however cramped the parent is, so a sector
    // never collapses to a single "+N more".
    if (projected >= minArea || keep.length < 2) keep.push(child);
    else fold.push(child);
  }

  // Folding one child gains nothing and loses its name.
  if (fold.length < 2) return sorted;

  const rolled = rollUp(fold);
  keep.push({
    kind: "other",
    id: "__other__",
    label: `+${fold.length} more`,
    title: `${fold.length} smaller holdings, aggregated`,
    children: fold,
    ...rolled,
  });
  return keep;
}

function buildTree(
  stocks: IndustryGroupStockItem[],
  window: ReturnWindow,
  sizing: SizingMode,
  /** group_id -> the group master's curated parent sector. */
  sectorByGroupId: Map<string, string>,
): Branch[] {
  const sectors = new Map<string, Map<string, { name: string; leaves: Leaf[] }>>();

  for (const stock of stocks) {
    const cap = Number.isFinite(stock.market_cap_cr) ? stock.market_cap_cr : 0;
    if (cap <= 0) continue;
    const ret = returnOf(stock, window);
    // Group by the group master's `parent_sector`, not the stock's own
    // `sector`. The two are different taxonomies: `parent_sector` is the
    // curated 15-sector scheme the Groups page is built on, while `sector`
    // comes from whichever vendor labelled the stock and carried 29 distinct
    // values on the live universe -- seven of them GICS leftovers duplicating
    // a real sector. Grouping on the curated one is why this map reads as 15
    // coherent blocks instead of 22 blocks plus 7 slivers.
    const sectorName =
      sectorByGroupId.get(stock.final_group_id) || stock.sector?.trim() || "Unclassified";
    const groupId = stock.final_group_id || `${sectorName}::ungrouped`;

    let groups = sectors.get(sectorName);
    if (!groups) {
      groups = new Map();
      sectors.set(sectorName, groups);
    }
    let bucket = groups.get(groupId);
    if (!bucket) {
      bucket = { name: stock.final_group_name || "Other", leaves: [] };
      groups.set(groupId, bucket);
    }
    bucket.leaves.push({
      kind: "stock",
      id: stock.symbol,
      label: stock.symbol,
      title: stock.company_name || stock.symbol,
      weight: sizing === "cap" ? cap : 1,
      ret,
      cap,
      symbol: stock.symbol,
    });
  }

  const out: Branch[] = [];
  for (const [sectorName, groups] of sectors) {
    const groupNodes: Branch[] = [];
    for (const [groupId, bucket] of groups) {
      const rolled = rollUp(bucket.leaves);
      groupNodes.push({
        kind: "group",
        id: groupId,
        label: bucket.name,
        title: bucket.name,
        children: bucket.leaves,
        ...rolled,
      });
    }
    const rolled = rollUp(groupNodes);
    out.push({
      kind: "sector",
      id: sectorName,
      label: sectorName,
      title: sectorName,
      children: groupNodes,
      ...rolled,
    });
  }
  return out;
}

/* ============================== the component ============================== */

type Tile = TreemapRect<Node> & { depth: number; parentId: string | null };

type SectorTreemapProps = {
  data: IndustryGroupsResponse | null;
  loading?: boolean;
  onPickSymbolWithContext: (symbol: string, contextSymbols: string[]) => void;
};

export function SectorTreemap({ data, loading, onPickSymbolWithContext }: SectorTreemapProps) {
  const [window_, setWindow] = useState<ReturnWindow>("1d");
  const [sizing, setSizing] = useState<SizingMode>("cap");
  /** null = all sectors; [sector] = one sector's groups; [sector, group] = one group. */
  const [path, setPath] = useState<string[]>([]);
  const [hover, setHover] = useState<{ node: Node; x: number; y: number } | null>(null);

  const wrapRef = useRef<HTMLDivElement | null>(null);
  const [size, setSize] = useState({ w: 0, h: 0 });

  useLayoutEffect(() => {
    const element = wrapRef.current;
    if (!element) return;

    const apply = (width: number) => {
      if (width <= 0) return;
      // Portrait box on a phone (one level of large tiles), landscape on a
      // desktop (two nested levels).
      // 0.58 rather than 0.52 on desktop: the live universe puts ~210 tiles on
      // the root map and the extra height is the cheapest legibility win.
      const ratio = width < NARROW_WIDTH ? 1.15 : 0.58;
      setSize({ w: width, h: Math.round(Math.min(760, Math.max(360, width * ratio))) });
    };

    // Measure once up front. ResizeObserver alone is not enough: a container
    // that is laid out but never resized again — or one whose first callback
    // is deferred — would leave the canvas at zero width and draw nothing.
    apply(element.getBoundingClientRect().width);

    const observer = new ResizeObserver((entries) => {
      apply(entries[0]?.contentRect.width ?? 0);
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const sectorByGroupId = useMemo(() => {
    const map = new Map<string, string>();
    for (const group of data?.groups ?? []) {
      const parent = group.parent_sector?.trim();
      if (parent) map.set(group.group_id, parent);
    }
    return map;
  }, [data]);

  const tree = useMemo(
    () => buildTree(data?.stocks ?? [], window_, sizing, sectorByGroupId),
    [data, window_, sizing, sectorByGroupId],
  );

  // A stale drill-down path (sector renamed, filters changed) must not blank
  // the canvas — fall back to the root instead.
  const activeSector = path[0] ? tree.find((s) => s.id === path[0]) ?? null : null;
  const activeGroup =
    activeSector && path[1]
      ? (activeSector.children.find((g) => g.id === path[1]) as Branch | undefined) ?? null
      : null;
  useEffect(() => {
    if (path.length && !activeSector) setPath([]);
    else if (path.length > 1 && !activeGroup) setPath((p) => p.slice(0, 1));
  }, [path, activeSector, activeGroup]);

  /** Lays out the visible two levels. Showing a parent and its children at once
   *  is what makes a treemap readable — one level alone is just a bar chart. */
  const tiles = useMemo<Tile[]>(() => {
    if (!size.w || !size.h) return [];
    const out: Tile[] = [];

    const layoutLeaves = (parent: Branch, x: number, y: number, w: number, h: number) => {
      const rects = squarify(
        parent.children.map((child) => ({ value: child.weight, datum: child })),
        x, y, w, h,
      );
      for (const rect of rects) out.push({ ...rect, depth: 2, parentId: parent.id });
    };

    if (activeGroup) {
      layoutLeaves(activeGroup, 0, 0, size.w, size.h);
      return out;
    }

    const parents: Branch[] = activeGroup
      ? []
      : activeSector
        ? (activeSector.children as Branch[])
        : tree;

    const outer = squarify(
      parents.map((parent) => ({ value: parent.weight, datum: parent as Node })),
      0, 0, size.w, size.h,
    );

    // One level, fully coloured and labelled, with a click to go deeper —
    // either because the canvas is phone-narrow, or because there is not
    // enough room per sector to nest anything legible inside it.
    const areaPerParent = parents.length ? (size.w * size.h) / parents.length : 0;
    if (size.w < NARROW_WIDTH || areaPerParent < MIN_PARENT_AREA_FOR_NESTING) {
      for (const rect of outer) out.push({ ...rect, depth: 2, parentId: null });
      return out;
    }

    for (const rect of outer) {
      out.push({ ...rect, depth: 1, parentId: null });
      const parent = rect.datum as Branch;
      // Reserve the header strip, then nest the children below it. Too small
      // to hold a header plus a readable child? Leave it as one flat tile.
      const innerY = rect.y + SECTOR_HEADER_H;
      const innerH = rect.h - SECTOR_HEADER_H - 2;
      if (rect.w < 56 || innerH < 26) continue;
      const inner = foldSmallChildren(
        parent.children,
        Math.max(0, rect.w - 2) * innerH,
        MIN_LABELLED_AREA,
      );
      const nested = squarify(
        inner.map((child) => ({ value: child.weight, datum: child })),
        rect.x + 1, innerY, rect.w - 2, innerH,
      );
      for (const inner of nested) out.push({ ...inner, depth: 2, parentId: parent.id });
    }
    return out;
  }, [tree, activeSector, activeGroup, size]);

  /**
   * A treemap is a grid, and grids get ONE tab stop plus arrow keys — not one
   * tab stop per cell. The first version made all 102 tiles tabbable, which
   * technically passed "keyboard accessible" while making the footer 102 tabs
   * away. This is the roving-tabindex model instead.
   */
  const navTiles = useMemo(
    () =>
      tiles
        .map((tile, index) => ({ tile, index }))
        .filter(({ tile }) => {
          if (tile.w < 10 || tile.h < 8) return false;
          return tile.depth === 1 || tile.depth === 2;
        }),
    [tiles],
  );

  const [navPos, setNavPos] = useState(0);
  const tileRefs = useRef<Array<SVGGElement | null>>([]);
  const shouldRefocus = useRef(false);

  // A new scope or window rebuilds the tiles, so the cursor goes home.
  useEffect(() => {
    setNavPos(0);
    shouldRefocus.current = false;
  }, [path, window_, sizing, size.w]);

  useEffect(() => {
    if (!shouldRefocus.current) return;
    shouldRefocus.current = false;
    tileRefs.current[navPos]?.focus();
  }, [navPos]);

  /** Nearest tile in the requested direction, by centre-to-centre distance with
   *  the off-axis component weighted up so the move stays visually sensible. */
  const moveFocus = useCallback(
    (direction: "up" | "down" | "left" | "right") => {
      const current = navTiles[navPos];
      if (!current) return;
      const cx = current.tile.x + current.tile.w / 2;
      const cy = current.tile.y + current.tile.h / 2;

      let bestPos = -1;
      let bestCost = Number.POSITIVE_INFINITY;
      navTiles.forEach((candidate, pos) => {
        if (pos === navPos) return;
        const dx = candidate.tile.x + candidate.tile.w / 2 - cx;
        const dy = candidate.tile.y + candidate.tile.h / 2 - cy;
        const along = direction === "left" ? -dx : direction === "right" ? dx : direction === "up" ? -dy : dy;
        if (along <= 1) return;
        const across = Math.abs(direction === "left" || direction === "right" ? dy : dx);
        const cost = along + across * 2.5;
        if (cost < bestCost) {
          bestCost = cost;
          bestPos = pos;
        }
      });
      if (bestPos >= 0) {
        shouldRefocus.current = true;
        setNavPos(bestPos);
      }
    },
    [navTiles, navPos],
  );

  const handleCanvasKeyDown = useCallback(
    (event: React.KeyboardEvent<SVGSVGElement>) => {
      const map: Record<string, "up" | "down" | "left" | "right"> = {
        ArrowUp: "up", ArrowDown: "down", ArrowLeft: "left", ArrowRight: "right",
      };
      const direction = map[event.key];
      if (direction) {
        event.preventDefault();
        moveFocus(direction);
        return;
      }
      if (event.key === "Escape" && path.length) {
        event.preventDefault();
        setPath((previous) => previous.slice(0, -1));
      }
    },
    [moveFocus, path.length],
  );

  const navPosByTileIndex = useMemo(() => {
    const lookup = new Map<number, number>();
    navTiles.forEach((entry, pos) => lookup.set(entry.index, pos));
    return lookup;
  }, [navTiles]);

  const contextSymbols = useMemo(() => {
    const collect = (node: Node, into: string[]) => {
      if (node.kind === "stock") into.push(node.symbol);
      else for (const child of node.children) collect(child, into);
    };
    const into: string[] = [];
    const roots: Node[] = activeGroup ? [activeGroup] : activeSector ? [activeSector] : tree;
    for (const root of roots) collect(root, into);
    return into;
  }, [tree, activeSector, activeGroup]);

  const handleTile = useCallback(
    (node: Node, parentId: string | null) => {
      if (node.kind === "stock") {
        onPickSymbolWithContext(node.symbol, contextSymbols);
        return;
      }
      if (node.kind === "other") return;
      if (node.kind === "sector") {
        setPath([node.id]);
        return;
      }
      // A group tile is reachable from two places: nested inside a sector on
      // the root map (where `parentId` is that sector) and as a top-level tile
      // once a sector is open (where the sector is `activeSector`). Both have
      // to produce a two-segment path, or the click silently does nothing.
      const sectorId = parentId ?? activeSector?.id ?? null;
      if (sectorId) setPath([sectorId, node.id]);
    },
    [activeSector, contextSymbols, onPickSymbolWithContext],
  );

  const showTooltip = useCallback((node: Node, event: React.MouseEvent) => {
    const box = wrapRef.current?.getBoundingClientRect();
    setHover({
      node,
      x: event.clientX - (box?.left ?? 0),
      y: event.clientY - (box?.top ?? 0),
    });
  }, []);

  const windowLabel = WINDOWS.find((w) => w.key === window_)?.full ?? "";
  const rootTotalCap = useMemo(() => tree.reduce((sum, sector) => sum + sector.cap, 0), [tree]);
  const scopeCap = activeGroup?.cap ?? activeSector?.cap ?? rootTotalCap;
  const scopeCount = activeGroup?.count ?? activeSector?.count ?? tree.reduce((s, x) => s + x.count, 0);

  if (!data?.stocks?.length) {
    return (
      <div className="tm-empty">
        {loading ? "Loading market map…" : "No constituent data available for the map."}
      </div>
    );
  }

  return (
    <div className="tm-root">
      <div className="tm-controls">
        <div className="tm-crumbs">
          {path.length ? (
            <button
              type="button"
              className="tm-crumb-back"
              onClick={() => setPath((p) => p.slice(0, -1))}
              aria-label="Back one level"
            >
              <ChevronLeft size={13} aria-hidden="true" />
            </button>
          ) : null}
          <button
            type="button"
            className={`tm-crumb${path.length ? "" : " active"}`}
            onClick={() => setPath([])}
            disabled={!path.length}
          >
            All sectors
          </button>
          {activeSector ? (
            <>
              <span className="tm-crumb-sep" aria-hidden="true">/</span>
              <button
                type="button"
                className={`tm-crumb${activeGroup ? "" : " active"}`}
                onClick={() => setPath([activeSector.id])}
                disabled={!activeGroup}
              >
                {activeSector.label}
              </button>
            </>
          ) : null}
          {activeGroup ? (
            <>
              <span className="tm-crumb-sep" aria-hidden="true">/</span>
              <span className="tm-crumb active">{activeGroup.label}</span>
            </>
          ) : null}
        </div>

        <div className="tm-control-group">
          <div className="tm-seg" role="group" aria-label="Return window">
            {WINDOWS.map((w) => (
              <button
                key={w.key}
                type="button"
                className={`tm-seg-btn${window_ === w.key ? " active" : ""}`}
                onClick={() => setWindow(w.key)}
                aria-pressed={window_ === w.key}
              >
                {w.label}
              </button>
            ))}
          </div>
          <div className="tm-seg" role="group" aria-label="Tile size">
            <button
              type="button"
              className={`tm-seg-btn${sizing === "cap" ? " active" : ""}`}
              onClick={() => setSizing("cap")}
              aria-pressed={sizing === "cap"}
              aria-label="Size tiles by market cap"
              title="Tile area ∝ market cap — how much money moved"
            >
              Market cap
            </button>
            <button
              type="button"
              className={`tm-seg-btn${sizing === "equal" ? " active" : ""}`}
              onClick={() => setSizing("equal")}
              aria-pressed={sizing === "equal"}
              aria-label="Size tiles equally"
              title="Every stock the same size — how broad the move is"
            >
              Equal weight
            </button>
          </div>
        </div>
      </div>

      <div className="tm-canvas-wrap" ref={wrapRef}>
        {size.w > 0 ? (
          <svg
            className="tm-canvas"
            width={size.w}
            height={size.h}
            viewBox={`0 0 ${size.w} ${size.h}`}
            onKeyDown={handleCanvasKeyDown}
            role="img"
            aria-label={`Market map: ${scopeCount} stocks sized by ${
              sizing === "cap" ? "market capitalisation" : "equal weight"
            }, coloured by ${windowLabel} return`}
          >
            {tiles.map((tile, index) => {
              const node = tile.datum;
              const band = bandOf(node.ret, window_);
              const showLabel = tile.w >= LABEL_MIN_W && tile.h >= LABEL_MIN_H;
              const showValue = tile.w >= LABEL_MIN_W && tile.h >= SUB_LABEL_MIN_H;
              const big = tile.w >= 150 && tile.h >= 64;
              // "other" is a bucket, not a place — nothing to drill into.
              const interactive =
                node.kind !== "other" && (tile.depth === 2 || node.kind === "sector");

              if (tile.depth === 1) {
                // Parent frame + header strip only; children are drawn on top,
                // so the strip is what stays clickable.
                return (
                  <g
                    key={`p-${node.id}-${index}`}
                    className="tm-parent tm-clickable"
                    role="button"
                    ref={(element) => {
                      const pos = navPosByTileIndex.get(index);
                      if (pos !== undefined) tileRefs.current[pos] = element;
                    }}
                    tabIndex={navPosByTileIndex.get(index) === navPos ? 0 : -1}
                    aria-label={`${node.title}, ${formatPct(node.ret)} over ${windowLabel}. Open`}
                    onClick={() => handleTile(node, null)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter" || event.key === " ") {
                        event.preventDefault();
                        handleTile(node, null);
                      }
                    }}
                    onMouseMove={(event) => showTooltip(node, event)}
                    onMouseLeave={() => setHover(null)}
                  >
                    <rect
                      x={tile.x} y={tile.y} width={Math.max(0, tile.w - 1)} height={Math.max(0, tile.h - 1)}
                      rx={4} className="tm-parent-rect"
                    />
                    {tile.w >= 56 ? (
                      <text x={tile.x + 6} y={tile.y + 13} className="tm-parent-label">
                        <tspan>{truncate(node.label, tile.w - (tile.w >= 132 ? 62 : 12))}</tspan>
                        {tile.w >= 132 ? (
                          <tspan className={`tm-parent-ret ${band}`} dx={6}>{formatPct(node.ret)}</tspan>
                        ) : null}
                      </text>
                    ) : null}
                  </g>
                );
              }

              return (
                <g
                  key={`t-${tile.parentId}-${node.id}-${index}`}
                  className={`tm-tile ${band}${interactive ? " tm-clickable" : ""}`}
                  role={interactive ? "button" : undefined}
                  ref={(element) => {
                    const pos = navPosByTileIndex.get(index);
                    if (pos !== undefined) tileRefs.current[pos] = element;
                  }}
                  tabIndex={
                    navPosByTileIndex.get(index) === undefined
                      ? undefined
                      : navPosByTileIndex.get(index) === navPos
                        ? 0
                        : -1
                  }
                  aria-label={`${node.title}, ${formatPct(node.ret)} over ${windowLabel}`}
                  onClick={interactive ? () => handleTile(node, tile.parentId) : undefined}
                  onKeyDown={
                    interactive
                      ? (event) => {
                          if (event.key === "Enter" || event.key === " ") {
                            event.preventDefault();
                            handleTile(node, tile.parentId);
                          }
                        }
                      : undefined
                  }
                  onMouseMove={(event) => showTooltip(node, event)}
                  onMouseLeave={() => setHover(null)}
                >
                  <rect
                    x={tile.x} y={tile.y}
                    width={Math.max(0, tile.w - 1)} height={Math.max(0, tile.h - 1)}
                    rx={2} className="tm-tile-rect"
                  />
                  {showLabel ? (
                    <text
                      x={tile.x + 6}
                      y={tile.y + (big ? 20 : 13)}
                      className={`tm-tile-label${big ? " lg" : ""}`}
                    >
                      {truncate(node.label, tile.w - 10, big ? 7.2 : 6.1)}
                    </text>
                  ) : null}
                  {showValue ? (
                    <text
                      x={tile.x + 6}
                      y={tile.y + (big ? 36 : 26)}
                      className={`tm-tile-value${big ? " lg" : ""}`}
                    >
                      {formatPct(node.ret)}
                    </text>
                  ) : null}
                </g>
              );
            })}
          </svg>
        ) : null}

        {hover ? (
          <div
            className="tm-tooltip"
            style={{
              left: Math.min(Math.max(hover.x + 14, 8), Math.max(8, size.w - 214)),
              top: Math.max(8, hover.y - 12),
            }}
            role="presentation"
          >
            <strong>{hover.node.title}</strong>
            <span className={`tm-tt-ret ${bandOf(hover.node.ret, window_)}`}>
              {formatPct(hover.node.ret)} · {windowLabel}
            </span>
            <span className="tm-tt-meta">
              {formatCap(hover.node.cap)}
              {hover.node.kind === "stock"
                ? ""
                : ` · ${hover.node.count} stock${hover.node.count === 1 ? "" : "s"}`}
            </span>
            {hover.node.kind === "stock" ? (
              <span className="tm-tt-hint">Click to open chart</span>
            ) : (
              <span className="tm-tt-hint">Click to drill in</span>
            )}
          </div>
        ) : null}
      </div>

      <div className="tm-footer">
        <div className="tm-legend" aria-hidden="true">
          <span className="tm-legend-label">{formatPct(-BANDS[window_][2])}</span>
          {["neg-3", "neg-2", "neg-1", "flat", "pos-1", "pos-2", "pos-3"].map((band) => (
            <span key={band} className={`tm-legend-swatch ${band}`} />
          ))}
          <span className="tm-legend-label">{formatPct(BANDS[window_][2])}</span>
        </div>
        <p className="tm-note">
          Area ∝ {sizing === "cap" ? "market cap" : "equal weight"} · colour ={" "}
          {sizing === "cap" ? "cap-weighted" : "average"} {windowLabel} return ·{" "}
          {scopeCount} stocks, {formatCap(scopeCap)}
        </p>
      </div>
    </div>
  );
}
