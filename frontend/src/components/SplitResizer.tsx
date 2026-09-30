import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import type { KeyboardEvent as ReactKeyboardEvent, PointerEvent as ReactPointerEvent } from "react";
import "./SplitResizer.css";

/**
 * A drag handle that lets the user resize two side-by-side columns of a CSS grid.
 *
 * Drop it in as a child of the grid container. It is absolutely positioned, so it
 * takes no grid cell, and it finds the container through its own parent — no ref
 * plumbing. The layout's stylesheet stays in charge: the handle reads the grid's
 * natural columns, and only when they really are side by side (the desktop
 * layout, and both panes present) does it take over the two tracks either side
 * of `boundary`. When a media query collapses the grid to one column the inline
 * template is dropped and the handle hides, so the responsive rules still win.
 *
 * The split is stored as the left pane's share of the pair, per `storageKey`, so
 * it follows the window width instead of freezing a pixel size. Double-click (or
 * Enter) returns to the layout's own proportions.
 */
type SplitResizerProps = {
  /** Remembered per layout, e.g. "groups-rankings". */
  storageKey: string;
  /** Index of the left-hand column of the pair being resized. */
  boundary?: number;
  /** Neither pane may be dragged narrower than this. */
  minPx?: number;
  /** Per-side floors, for a pane whose content has a fixed width (a table row template). */
  minBefore?: number;
  minAfter?: number;
  label?: string;
};

const STORAGE_PREFIX = "mr-malik-split:v1:";
const KEY_STEP = 0.02;

function readRatio(key: string): number | null {
  try {
    const raw = window.localStorage.getItem(STORAGE_PREFIX + key);
    if (raw == null) return null;
    const value = Number(raw);
    return Number.isFinite(value) && value > 0 && value < 1 ? value : null;
  } catch {
    return null;
  }
}

function writeRatio(key: string, ratio: number | null) {
  try {
    if (ratio == null) window.localStorage.removeItem(STORAGE_PREFIX + key);
    else window.localStorage.setItem(STORAGE_PREFIX + key, ratio.toFixed(4));
  } catch {
    // storage unavailable (private window): the split just isn't remembered
  }
}

function parseTracks(template: string): number[] {
  if (!template || template === "none") return [];
  return template
    .trim()
    .split(/\s+/)
    .map((token) => Number.parseFloat(token))
    .filter((value) => Number.isFinite(value));
}

type Geometry = {
  tracks: number[];
  gap: number;
  paddingLeft: number;
};

export function SplitResizer({
  storageKey,
  boundary = 0,
  minPx = 260,
  minBefore,
  minAfter,
  label = "Resize panels",
}: SplitResizerProps) {
  const handleRef = useRef<HTMLDivElement | null>(null);
  const ratioRef = useRef<number | null>(readRatio(storageKey));
  const geometryRef = useRef<Geometry | null>(null);
  const dragRef = useRef<{ pointerId: number } | null>(null);
  const frameRef = useRef<number | null>(null);
  const [active, setActive] = useState(false);
  const [dragging, setDragging] = useState(false);
  const [ratioLabel, setRatioLabel] = useState(50);

  const containerOf = () => handleRef.current?.parentElement ?? null;

  const clampRatio = useCallback(
    (leftWidth: number, pair: number) => {
      let floorBefore = minBefore ?? minPx;
      let floorAfter = minAfter ?? minPx;
      // A window too narrow for both floors splits the shortfall evenly.
      if (floorBefore + floorAfter > pair) {
        const scale = pair / (floorBefore + floorAfter);
        floorBefore *= scale;
        floorAfter *= scale;
      }
      return Math.min(Math.max(leftWidth, floorBefore), pair - floorAfter) / pair;
    },
    [minPx, minBefore, minAfter],
  );

  /** Re-derive the layout from the stylesheet, then re-apply the saved split. */
  const apply = useCallback(() => {
    const container = containerOf();
    const handle = handleRef.current;
    if (!container || !handle) return;

    container.style.gridTemplateColumns = "";
    const style = window.getComputedStyle(container);
    const natural = parseTracks(style.gridTemplateColumns);
    const panes = Array.from(container.children).filter((child) => {
      if (child === handle) return false;
      const childStyle = window.getComputedStyle(child);
      return childStyle.display !== "none" && childStyle.position !== "absolute" && childStyle.position !== "fixed";
    });
    const sideBySide =
      style.display.includes("grid") && natural.length >= boundary + 2 && panes.length >= boundary + 2;
    if (!sideBySide) {
      geometryRef.current = null;
      setActive(false);
      return;
    }

    if (style.position === "static") container.style.position = "relative";
    const gap = Number.parseFloat(style.columnGap) || 0;
    const paddingLeft = Number.parseFloat(style.paddingLeft) || 0;
    let tracks = natural;
    const naturalPair = natural[boundary] + natural[boundary + 1];
    // A split saved on a wider window must still respect the floors on this one.
    const ratio =
      ratioRef.current != null && naturalPair > 0 ? clampRatio(ratioRef.current * naturalPair, naturalPair) : null;
    if (ratio != null) {
      const template = natural
        .map((width, index) => {
          if (index === boundary) return `minmax(0, ${ratio.toFixed(4)}fr)`;
          if (index === boundary + 1) return `minmax(0, ${(1 - ratio).toFixed(4)}fr)`;
          return `${width}px`;
        })
        .join(" ");
      container.style.gridTemplateColumns = template;
      tracks = parseTracks(window.getComputedStyle(container).gridTemplateColumns);
    }
    geometryRef.current = { tracks, gap, paddingLeft };

    const left = paddingLeft + tracks.slice(0, boundary + 1).reduce((sum, width) => sum + width, 0) + gap * boundary + gap / 2;
    handle.style.left = `${left}px`;
    const pair = tracks[boundary] + tracks[boundary + 1];
    setRatioLabel(pair > 0 ? Math.round((tracks[boundary] / pair) * 100) : 50);
    setActive(true);
  }, [boundary, clampRatio]);

  // A new layout key brings its own remembered split.
  useLayoutEffect(() => {
    ratioRef.current = readRatio(storageKey);
    apply();
  }, [storageKey, apply]);

  useEffect(() => {
    const container = containerOf();
    if (!container) return;
    let lastWidth = container.clientWidth;
    const schedule = () => {
      if (frameRef.current != null) cancelAnimationFrame(frameRef.current);
      frameRef.current = requestAnimationFrame(() => {
        frameRef.current = null;
        apply();
      });
    };
    const resizeObserver =
      typeof ResizeObserver !== "undefined"
        ? new ResizeObserver(() => {
            const width = container.clientWidth;
            if (width === lastWidth) return;
            lastWidth = width;
            schedule();
          })
        : null;
    resizeObserver?.observe(container);
    // Pages swap the container's class (e.g. Groups -> Rotation goes one column)
    // and panes come and go; both change whether a split exists at all.
    const mutationObserver = new MutationObserver(schedule);
    mutationObserver.observe(container, { attributes: true, attributeFilter: ["class"], childList: true });
    window.addEventListener("resize", schedule);
    // This effect's cleanup clears the template, and when `apply` changes it runs
    // after the layout effect above has already re-applied it — so apply again.
    apply();
    return () => {
      resizeObserver?.disconnect();
      mutationObserver.disconnect();
      window.removeEventListener("resize", schedule);
      if (frameRef.current != null) cancelAnimationFrame(frameRef.current);
      container.style.gridTemplateColumns = "";
    };
  }, [apply]);

  const setRatio = useCallback(
    (ratio: number | null, persist: boolean) => {
      ratioRef.current = ratio;
      if (persist) writeRatio(storageKey, ratio);
      apply();
    },
    [storageKey, apply],
  );


  const handlePointerDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.button !== 0 || !geometryRef.current) return;
    event.preventDefault();
    event.currentTarget.setPointerCapture(event.pointerId);
    dragRef.current = { pointerId: event.pointerId };
    setDragging(true);
  };

  const handlePointerMove = (event: ReactPointerEvent<HTMLDivElement>) => {
    const geometry = geometryRef.current;
    const container = containerOf();
    if (!dragRef.current || dragRef.current.pointerId !== event.pointerId || !geometry || !container) return;
    const { tracks, gap, paddingLeft } = geometry;
    const rect = container.getBoundingClientRect();
    const pairStart =
      rect.left + container.clientLeft + paddingLeft + tracks.slice(0, boundary).reduce((sum, width) => sum + width, 0) + gap * boundary;
    const pair = tracks[boundary] + tracks[boundary + 1];
    if (pair <= 0) return;
    setRatio(clampRatio(event.clientX - pairStart - gap / 2, pair), false);
  };

  const finishDrag = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (!dragRef.current || dragRef.current.pointerId !== event.pointerId) return;
    try {
      event.currentTarget.releasePointerCapture(event.pointerId);
    } catch {
      // already released
    }
    dragRef.current = null;
    setDragging(false);
    writeRatio(storageKey, ratioRef.current);
  };

  const handleKeyDown = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    const geometry = geometryRef.current;
    if (!geometry) return;
    const pair = geometry.tracks[boundary] + geometry.tracks[boundary + 1];
    const current = pair > 0 ? geometry.tracks[boundary] / pair : 0.5;
    if (event.key === "ArrowLeft" || event.key === "ArrowRight") {
      event.preventDefault();
      const next = current + (event.key === "ArrowLeft" ? -KEY_STEP : KEY_STEP);
      setRatio(clampRatio(next * pair, pair), true);
    } else if (event.key === "Enter") {
      event.preventDefault();
      setRatio(null, true);
    }
  };

  // While dragging, the whole page shows the resize cursor and text can't be
  // selected — the pointer is often over a chart canvas or a table, not the bar.
  useEffect(() => {
    if (!dragging) return;
    const body = document.body;
    const previousCursor = body.style.cursor;
    const previousSelect = body.style.userSelect;
    body.style.cursor = "col-resize";
    body.style.userSelect = "none";
    body.classList.add("split-resizing");
    return () => {
      body.style.cursor = previousCursor;
      body.style.userSelect = previousSelect;
      body.classList.remove("split-resizing");
    };
  }, [dragging]);

  return (
    <div
      ref={handleRef}
      className={`split-resizer${active ? " is-active" : ""}${dragging ? " is-dragging" : ""}`}
      role="separator"
      aria-orientation="vertical"
      aria-label={label}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={ratioLabel}
      aria-hidden={active ? undefined : true}
      tabIndex={active ? 0 : -1}
      title="Drag to resize · double-click to reset"
      onPointerDown={handlePointerDown}
      onPointerMove={handlePointerMove}
      onPointerUp={finishDrag}
      onPointerCancel={finishDrag}
      onDoubleClick={() => setRatio(null, true)}
      onKeyDown={handleKeyDown}
    >
      <span className="split-resizer-line" aria-hidden="true" />
      <span className="split-resizer-grip" aria-hidden="true" />
    </div>
  );
}

export default SplitResizer;
