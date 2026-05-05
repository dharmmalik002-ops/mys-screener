import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type PointerEvent as ReactPointerEvent,
} from "react";

import type { IndustryGroupStockItem, MarketKey } from "../lib/api";
import { useLatestQuarters, type QuarterInfo } from "../lib/latestQuarter";
import "./GroupStocksWidget.css";

type GroupMember = IndustryGroupStockItem & { group_member_rank: number };

export type GroupStocksWidgetContext = {
  groupId: string;
  groupName: string;
  parentSector: string;
  groupRankLabel: string;
  stockCount: number;
  members: GroupMember[];
};

type CompareLayout = "horizontal" | "vertical";
type PaneId = "A" | "B";

export type WidgetRect = {
  x: number;
  y: number;
  width: number;
  height: number;
};

type GroupStocksWidgetProps = {
  market: MarketKey;
  context: GroupStocksWidgetContext;
  selectedSymbolA: string | null;
  selectedSymbolB: string | null;
  activePane: PaneId;
  compareMode: boolean;
  compareLayout: CompareLayout;
  rect: WidgetRect;
  onRectChange: (rect: WidgetRect) => void;
  onClose: () => void;
  onSelectMember: (symbol: string) => void;
  onToggleCompare: () => void;
  onLayoutChange: (layout: CompareLayout) => void;
};

const MIN_WIDTH = 240;
const MIN_HEIGHT = 320;
const MAX_WIDTH = 640;
const MAX_HEIGHT = 1200;

function fmtChange(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return "--";
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function changeClass(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return "neutral";
  return value >= 0 ? "positive" : "negative";
}

function rsClass(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return "rs-neutral";
  if (value >= 80) return "rs-strong";
  if (value >= 60) return "rs-medium";
  return "rs-weak";
}

export function GroupStocksWidget({
  market,
  context,
  selectedSymbolA,
  selectedSymbolB,
  activePane,
  compareMode,
  compareLayout,
  rect,
  onRectChange,
  onClose,
  onSelectMember,
  onToggleCompare,
  onLayoutChange,
}: GroupStocksWidgetProps) {
  const memberSymbols = useMemo(() => context.members.map((m) => m.symbol), [context.members]);
  const quarterMap = useLatestQuarters(memberSymbols, market, { concurrency: 4 });
  const groupMaxRank = useMemo(() => {
    let maxRank = 0;
    Object.values(quarterMap).forEach((q) => {
      if (q && q.rank > maxRank) maxRank = q.rank;
    });
    return maxRank;
  }, [quarterMap]);

  const [query, setQuery] = useState("");
  const listRef = useRef<HTMLDivElement | null>(null);
  const dragStateRef = useRef<{
    mode: "move" | "resize";
    pointerId: number;
    startX: number;
    startY: number;
    initial: WidgetRect;
  } | null>(null);
  const [isInteracting, setIsInteracting] = useState(false);

  const clampRect = useCallback((next: WidgetRect): WidgetRect => {
    const vw = typeof window !== "undefined" ? window.innerWidth : 1280;
    const vh = typeof window !== "undefined" ? window.innerHeight : 800;
    const width = Math.max(MIN_WIDTH, Math.min(MAX_WIDTH, Math.min(next.width, vw - 16)));
    const height = Math.max(MIN_HEIGHT, Math.min(MAX_HEIGHT, Math.min(next.height, vh - 16)));
    const x = Math.max(8, Math.min(next.x, vw - width - 8));
    const y = Math.max(8, Math.min(next.y, vh - height - 8));
    return { x, y, width, height };
  }, []);

  const startDrag = useCallback(
    (event: ReactPointerEvent<HTMLDivElement>, mode: "move" | "resize") => {
      event.preventDefault();
      event.stopPropagation();
      dragStateRef.current = {
        mode,
        pointerId: event.pointerId,
        startX: event.clientX,
        startY: event.clientY,
        initial: { ...rect },
      };
      setIsInteracting(true);
      try {
        event.currentTarget.setPointerCapture(event.pointerId);
      } catch {
        // capture is best-effort
      }
    },
    [rect],
  );

  const handlePointerMove = useCallback(
    (event: ReactPointerEvent<HTMLDivElement>) => {
      const state = dragStateRef.current;
      if (!state || state.pointerId !== event.pointerId) return;
      const dx = event.clientX - state.startX;
      const dy = event.clientY - state.startY;
      if (state.mode === "move") {
        onRectChange(clampRect({ ...state.initial, x: state.initial.x + dx, y: state.initial.y + dy }));
      } else {
        onRectChange(
          clampRect({
            ...state.initial,
            width: state.initial.width + dx,
            height: state.initial.height + dy,
          }),
        );
      }
    },
    [clampRect, onRectChange],
  );

  const finishDrag = useCallback((event: ReactPointerEvent<HTMLDivElement>) => {
    const state = dragStateRef.current;
    if (!state || state.pointerId !== event.pointerId) return;
    try {
      event.currentTarget.releasePointerCapture(event.pointerId);
    } catch {
      // ignore
    }
    dragStateRef.current = null;
    setIsInteracting(false);
  }, []);

  const filteredMembers = useMemo(() => {
    const trimmed = query.trim().toUpperCase();
    if (!trimmed) return context.members;
    return context.members.filter((member) => {
      return (
        member.symbol.toUpperCase().includes(trimmed) ||
        (member.company_name ?? "").toUpperCase().includes(trimmed)
      );
    });
  }, [context.members, query]);

  const activeSymbol = activePane === "A" ? selectedSymbolA : selectedSymbolB;

  useEffect(() => {
    if (!activeSymbol || !listRef.current) return;
    const target = listRef.current.querySelector<HTMLDivElement>(
      `[data-symbol="${activeSymbol}"]`,
    );
    if (target) {
      target.scrollIntoView({ block: "nearest", behavior: "smooth" });
    }
  }, [activeSymbol]);

  const handleSearchChange = (event: ChangeEvent<HTMLInputElement>) => {
    setQuery(event.target.value);
  };

  const widgetStyle = {
    left: `${rect.x}px`,
    top: `${rect.y}px`,
    width: `${rect.width}px`,
    height: `${rect.height}px`,
  };
  const widgetClass = `group-stocks-widget ${isInteracting ? "is-interacting" : ""}`;

  return (
    <div
      className={widgetClass}
      role="complementary"
      aria-label="Group stocks"
      style={widgetStyle}
    >
      <div
        className="gsw-header"
        onPointerDown={(event) => startDrag(event, "move")}
        onPointerMove={handlePointerMove}
        onPointerUp={finishDrag}
        onPointerCancel={finishDrag}
      >
        <div className="gsw-drag-handle" aria-hidden="true">⋮⋮</div>
        <div className="gsw-title-block">
          <span className="gsw-eyebrow">{context.parentSector}</span>
          <h4 className="gsw-title">{context.groupName}</h4>
          <div className="gsw-meta">
            <span>{context.groupRankLabel}</span>
            <span aria-hidden="true">•</span>
            <span>{context.stockCount} stocks</span>
          </div>
        </div>
        <button
          type="button"
          className="gsw-close"
          onClick={onClose}
          onPointerDown={(event) => event.stopPropagation()}
          aria-label="Close group stocks"
          title="Close"
        >
          ×
        </button>
      </div>

      <div className="gsw-actions">
        <button
          type="button"
          className={compareMode ? "gsw-compare-btn active" : "gsw-compare-btn"}
          onClick={onToggleCompare}
          title={compareMode ? "Exit compare mode" : "Compare with group leader"}
        >
          {compareMode ? "Exit Compare" : "Compare"}
        </button>
        {compareMode ? (
          <div className="gsw-layout-toggle" role="group" aria-label="Layout">
            <button
              type="button"
              className={compareLayout === "horizontal" ? "active" : ""}
              onClick={() => onLayoutChange("horizontal")}
              title="Side by side"
              aria-pressed={compareLayout === "horizontal"}
            >
              ⬌
            </button>
            <button
              type="button"
              className={compareLayout === "vertical" ? "active" : ""}
              onClick={() => onLayoutChange("vertical")}
              title="Stacked"
              aria-pressed={compareLayout === "vertical"}
            >
              ⬍
            </button>
          </div>
        ) : null}
      </div>

      <div className="gsw-search-row">
        <input
          type="search"
          className="gsw-search"
          placeholder="Search in group..."
          value={query}
          onChange={handleSearchChange}
          aria-label="Search stocks in this group"
        />
      </div>

      {compareMode ? (
        <div className="gsw-pane-hint">
          Active: <strong>Pane {activePane}</strong> — arrow keys / clicks update this side
        </div>
      ) : (
        <div className="gsw-pane-hint">↑ ↓ to navigate</div>
      )}

      <div className="gsw-table-head">
        <span>#</span>
        <span>Symbol</span>
        <span title="Latest reported quarter">Last Q</span>
        <span>1D</span>
        <span>RS</span>
      </div>

      <div className="gsw-list" ref={listRef}>
        {filteredMembers.length === 0 ? (
          <div className="gsw-empty">No matches</div>
        ) : (
          filteredMembers.map((member) => {
            const isPaneA = member.symbol === selectedSymbolA;
            const isPaneB = compareMode && member.symbol === selectedSymbolB;
            const isActive = activePane === "A" ? isPaneA : isPaneB;
            const rowClass = [
              "gsw-row",
              isPaneA ? "is-pane-a" : "",
              isPaneB ? "is-pane-b" : "",
              isActive ? "is-active" : "",
            ]
              .filter(Boolean)
              .join(" ");

            return (
              <div
                key={`${context.groupId}:${member.symbol}`}
                className={rowClass}
                data-symbol={member.symbol}
                role="button"
                tabIndex={0}
                onClick={() => onSelectMember(member.symbol)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    onSelectMember(member.symbol);
                  }
                }}
                title={member.company_name ?? member.symbol}
              >
                <span className="gsw-rank">{member.group_member_rank}</span>
                <span className="gsw-symbol">
                  <strong>{member.symbol}</strong>
                  {member.company_name ? (
                    <small>{member.company_name}</small>
                  ) : null}
                </span>
                {(() => {
                  const q = quarterMap[member.symbol] as QuarterInfo | null | undefined;
                  if (q === undefined) {
                    return <span className="gsw-quarter loading" title="Loading…">…</span>;
                  }
                  if (!q) {
                    return <span className="gsw-quarter na" title="No quarterly data">—</span>;
                  }
                  const isLatest = groupMaxRank > 0 && q.rank === groupMaxRank;
                  const isBehind = groupMaxRank > 0 && q.rank < groupMaxRank;
                  const cls = isLatest ? "latest" : isBehind ? "behind" : "neutral";
                  return (
                    <span className={`gsw-quarter ${cls}`} title={q.raw}>
                      {q.short}
                    </span>
                  );
                })()}
                <span className={`gsw-change ${changeClass(member.change_pct)}`}>
                  {fmtChange(member.change_pct)}
                </span>
                <span className={`gsw-rs ${rsClass(member.rs_rating)}`}>
                  {member.rs_rating ?? "--"}
                </span>
              </div>
            );
          })
        )}
      </div>

      <div
        className="gsw-resize-handle"
        onPointerDown={(event) => startDrag(event, "resize")}
        onPointerMove={handlePointerMove}
        onPointerUp={finishDrag}
        onPointerCancel={finishDrag}
        role="slider"
        aria-label="Resize widget"
      >
        <span aria-hidden="true">⤡</span>
      </div>
    </div>
  );
}

export default GroupStocksWidget;
