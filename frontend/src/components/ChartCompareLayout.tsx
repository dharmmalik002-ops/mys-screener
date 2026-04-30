import { useCallback, useEffect, useRef, useState, type PointerEvent as ReactPointerEvent, type ReactNode } from "react";

import "./ChartCompareLayout.css";

export type CompareLayout = "horizontal" | "vertical";
export type ComparePane = "A" | "B";

type ChartCompareLayoutProps = {
  compareMode: boolean;
  layout: CompareLayout;
  activePane: ComparePane;
  dividerRatio: number;
  onDividerRatioChange: (ratio: number) => void;
  onActivePaneChange: (pane: ComparePane) => void;
  paneA: ReactNode;
  paneB: ReactNode;
};

const MIN_RATIO = 0.2;
const MAX_RATIO = 0.8;

export function ChartCompareLayout({
  compareMode,
  layout,
  activePane,
  dividerRatio,
  onDividerRatioChange,
  onActivePaneChange,
  paneA,
  paneB,
}: ChartCompareLayoutProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const dragStateRef = useRef<{ pointerId: number } | null>(null);

  const clamp = (value: number) => Math.max(MIN_RATIO, Math.min(MAX_RATIO, value));

  const handlePointerDown = useCallback(
    (event: ReactPointerEvent<HTMLDivElement>) => {
      event.preventDefault();
      event.stopPropagation();
      dragStateRef.current = { pointerId: event.pointerId };
      setIsDragging(true);
      try {
        event.currentTarget.setPointerCapture(event.pointerId);
      } catch {
        // ignore — capture is best-effort
      }
    },
    [],
  );

  const handlePointerMove = useCallback(
    (event: ReactPointerEvent<HTMLDivElement>) => {
      if (!dragStateRef.current || dragStateRef.current.pointerId !== event.pointerId) return;
      const container = containerRef.current;
      if (!container) return;
      const rect = container.getBoundingClientRect();
      const ratio =
        layout === "horizontal"
          ? (event.clientX - rect.left) / rect.width
          : (event.clientY - rect.top) / rect.height;
      onDividerRatioChange(clamp(ratio));
    },
    [layout, onDividerRatioChange],
  );

  const finishDrag = useCallback(
    (event: ReactPointerEvent<HTMLDivElement>) => {
      if (!dragStateRef.current || dragStateRef.current.pointerId !== event.pointerId) return;
      try {
        event.currentTarget.releasePointerCapture(event.pointerId);
      } catch {
        // ignore
      }
      dragStateRef.current = null;
      setIsDragging(false);
    },
    [],
  );

  useEffect(() => {
    if (!isDragging) return;
    const previousCursor = document.body.style.cursor;
    document.body.style.cursor = layout === "horizontal" ? "col-resize" : "row-resize";
    return () => {
      document.body.style.cursor = previousCursor;
    };
  }, [isDragging, layout]);

  if (!compareMode) {
    return <div className="chart-compare-single">{paneA}</div>;
  }

  const paneAStyle =
    layout === "horizontal"
      ? { width: `${dividerRatio * 100}%`, height: "100%" }
      : { height: `${dividerRatio * 100}%`, width: "100%" };
  const paneBStyle =
    layout === "horizontal"
      ? { width: `${(1 - dividerRatio) * 100}%`, height: "100%" }
      : { height: `${(1 - dividerRatio) * 100}%`, width: "100%" };

  const containerClass = `chart-compare-layout ${layout} ${isDragging ? "is-dragging" : ""}`;

  return (
    <div ref={containerRef} className={containerClass}>
      <div
        className={`chart-compare-pane pane-a ${activePane === "A" ? "is-active" : ""}`}
        style={paneAStyle}
        onMouseDown={() => onActivePaneChange("A")}
        onTouchStart={() => onActivePaneChange("A")}
      >
        {paneA}
      </div>
      <div
        className="chart-compare-divider"
        role="separator"
        aria-orientation={layout === "horizontal" ? "vertical" : "horizontal"}
        aria-valuenow={Math.round(dividerRatio * 100)}
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={finishDrag}
        onPointerCancel={finishDrag}
      >
        <div className="chart-compare-divider-grip" />
      </div>
      <div
        className={`chart-compare-pane pane-b ${activePane === "B" ? "is-active" : ""}`}
        style={paneBStyle}
        onMouseDown={() => onActivePaneChange("B")}
        onTouchStart={() => onActivePaneChange("B")}
      >
        {paneB}
      </div>
    </div>
  );
}

export default ChartCompareLayout;
