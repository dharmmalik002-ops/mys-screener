import { useEffect, useRef } from "react";

import type { ImprovingRsItem, ImprovingRsResponse, MarketKey } from "../lib/api";
import { useMinWidth, useVirtualRows } from "../lib/virtualRows";
import { Panel } from "./Panel";

const IMPROVING_RS_ROW_HEIGHT = 76;

type ImprovingRsPanelProps = {
  market: MarketKey;
  data: ImprovingRsResponse | null;
  loading?: boolean;
  onPickSymbol: (symbol: string) => void;
  onRequestAddToWatchlist: (symbol: string) => void;
  selectedSymbol: string | null;
};

function formatPrice(value: number, market: MarketKey) {
  const locale = "en-IN";
  const symbol = "₹";
  return `${symbol}${value.toLocaleString(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

export function ImprovingRsPanel({
  market,
  data,
  loading = false,
  onPickSymbol,
  onRequestAddToWatchlist,
  selectedSymbol,
}: ImprovingRsPanelProps) {
  const rowRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const hasWideTableLayout = useMinWidth(1180);
  const items = data?.items ?? [];
  const shouldVirtualize = hasWideTableLayout && items.length > 120;
  const { containerRef, scrollToKey, totalHeight, visibleRows } = useVirtualRows({
    items,
    getKey: (item) => item.symbol,
    getHeight: () => IMPROVING_RS_ROW_HEIGHT,
  });

  useEffect(() => {
    if (!selectedSymbol) {
      return;
    }

    if (shouldVirtualize) {
      scrollToKey(selectedSymbol);
      return;
    }

    const activeRow = rowRefs.current[selectedSymbol];
    activeRow?.scrollIntoView({ block: "nearest", inline: "nearest" });
  }, [items, scrollToKey, selectedSymbol, shouldVirtualize]);

  const renderRow = (item: ImprovingRsItem, virtualHeight?: number) => (
    <div
      key={`high-rs-${item.symbol}`}
      className={selectedSymbol === item.symbol ? "scan-row improving-rs-row active" : "scan-row improving-rs-row"}
      ref={
        shouldVirtualize
          ? undefined
          : (element) => {
              rowRefs.current[item.symbol] = element;
            }
      }
      style={virtualHeight ? { height: `${virtualHeight}px` } : undefined}
    >
      <button type="button" className="scan-row-main" onClick={() => onPickSymbol(item.symbol)}>
        <span>
          <strong>{item.symbol}</strong>
          <small>{item.sub_sector || item.sector}</small>
        </span>
      </button>
      <span>{formatPrice(item.last_price, market)}</span>
      <span className={item.change_pct >= 0 ? "positive-text" : "negative-text"}>{item.change_pct.toFixed(2)}%</span>
      <span>{item.rs_rating}</span>
      <span>{item.rs_rating_1d_ago}</span>
      <span>{item.rs_rating_1w_ago}</span>
      <span>{item.rs_rating_1m_ago}</span>
      <span className="positive-text">52W High</span>
      <button type="button" className="tool-pill small" onClick={() => onRequestAddToWatchlist(item.symbol)}>
        Add
      </button>
    </div>
  );

  return (
    <Panel
      title="52 Week High RS"
      subtitle={data ? `${data.total_hits} stocks printed a fresh 52-week high in RS rating today` : "Loading 52-week RS highs"}
      className="improving-rs-panel"
    >
      <div className="scan-table">
        <div className="scan-table-head improving-rs-head">
          <span>Stock</span>
          <span>Price</span>
          <span>Change</span>
          <span>RS Now</span>
          <span>1D Ago</span>
          <span>1W Ago</span>
          <span>1M Ago</span>
          <span>Signal</span>
          <span>Watch</span>
        </div>
        <div ref={shouldVirtualize ? containerRef : undefined} className={shouldVirtualize ? "scan-table-body scan-table-body-virtual" : "scan-table-body"}>
          {!items.length ? (
            <div className="empty-state">{loading ? "Loading 52-week RS highs..." : "No stocks printed a 52-week high RS rating today."}</div>
          ) : shouldVirtualize ? (
            <div className="scan-table-virtual-spacer" style={{ height: `${totalHeight}px` }}>
              {visibleRows.map((row) => (
                <div key={row.key} className="scan-table-virtual-slot" style={{ top: `${row.top}px`, height: `${row.height}px` }}>
                  {renderRow(row.item, Math.max(0, row.height - 6))}
                </div>
              ))}
            </div>
          ) : (
            items.map((item) => renderRow(item))
          )}
        </div>
      </div>
    </Panel>
  );
}
