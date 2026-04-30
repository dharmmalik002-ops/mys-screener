import { useEffect, useMemo, useRef, useState, type ChangeEvent } from "react";

import type { IndustryGroupStockItem, MarketKey } from "../lib/api";
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

type GroupStocksWidgetProps = {
  market: MarketKey;
  context: GroupStocksWidgetContext;
  selectedSymbolA: string | null;
  selectedSymbolB: string | null;
  activePane: PaneId;
  compareMode: boolean;
  compareLayout: CompareLayout;
  onClose: () => void;
  onSelectMember: (symbol: string) => void;
  onToggleCompare: () => void;
  onLayoutChange: (layout: CompareLayout) => void;
};

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
  onClose,
  onSelectMember,
  onToggleCompare,
  onLayoutChange,
}: GroupStocksWidgetProps) {
  void market;

  const [query, setQuery] = useState("");
  const listRef = useRef<HTMLDivElement | null>(null);

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

  return (
    <div className="group-stocks-widget" role="complementary" aria-label="Group stocks">
      <div className="gsw-header">
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
    </div>
  );
}

export default GroupStocksWidget;
