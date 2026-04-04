import { Suspense, lazy, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";

import { getChartGridSeries, type ChartBar, type ChartGridTimeframe, type MarketKey, type ScanMatch } from "../lib/api";
import { buildSymbolSuggestions } from "../lib/searchSuggestions";
import { useMinWidth, useVirtualRows } from "../lib/virtualRows";
import type { ChartGridChartStyle, ChartGridDisplayCard, ChartGridDisplayMode, ChartGridSortBy, ChartGridStat } from "./ChartGridModal";
import { Panel } from "./Panel";

const ChartGridModal = lazy(() => import("./ChartGridModal").then((module) => ({ default: module.ChartGridModal })));
const WATCHLIST_SLOT_GAP = 6;
const WATCHLIST_ROW_SLOT_HEIGHT = 64;

export type LocalWatchlist = {
  id: string;
  name: string;
  color: string;
  symbols: string[];
};

type WatchlistsPanelProps = {
  market: MarketKey;
  watchlists: LocalWatchlist[];
  activeWatchlistId: string | null;
  onSelectWatchlist: (id: string) => void;
  onCreateWatchlist: (name: string) => void;
  onRenameWatchlist: (id: string, name: string) => void;
  onDeleteWatchlist: (id: string) => void;
  onExportWatchlist: (id: string) => void;
  onSetWatchlistColor: (id: string, color: string) => void;
  onRemoveFromWatchlist: (watchlistId: string, symbol: string) => void;
  onMoveSymbols: (fromWatchlistId: string, toWatchlistId: string, symbols: string[]) => void;
  onRequestAddToWatchlist: (symbol: string) => void;
  onPickSymbol: (symbol: string) => void;
  universeItems: ScanMatch[];
  selectedSymbol: string | null;
};

type WatchlistDisplayItem = {
  symbol: string;
  name: string;
  last_price: number;
  change_pct: number;
  rs_rating: number | null;
  market_cap_crore: number | null;
  stock_return_20d: number | null;
  stock_return_60d: number | null;
  stock_return_12m: number | null;
  isKnown: boolean;
};

function formatReturn(value: number) {
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function metricClass(value: number) {
  return value >= 0 ? "positive-text" : "negative-text";
}

function formatPrice(value: number, market: MarketKey) {
  const locale = market === "us" ? "en-US" : "en-IN";
  const symbol = market === "us" ? "$" : "₹";
  return `${symbol}${value.toLocaleString(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function selectedReturnForGrid(item: WatchlistDisplayItem, timeframe: ChartGridTimeframe) {
  if (timeframe === "3M") {
    return item.stock_return_60d ?? item.stock_return_20d ?? item.change_pct;
  }
  if (timeframe === "6M") {
    return item.stock_return_12m ?? item.stock_return_60d ?? item.change_pct;
  }
  if (timeframe === "1Y") {
    return item.stock_return_12m ?? item.stock_return_60d ?? item.change_pct;
  }
  return item.stock_return_12m ?? item.stock_return_60d ?? item.change_pct;
}

function fallbackSparkline(returnPct: number) {
  const now = Math.floor(Date.now() / 1000);
  const baseline = 100;
  const current = baseline * (1 + (returnPct / 100));
  return [
    { time: now - (63 * 24 * 60 * 60), value: Number(baseline.toFixed(4)) },
    { time: now, value: Number(current.toFixed(4)) },
  ];
}

export function WatchlistsPanel({
  market,
  watchlists,
  activeWatchlistId,
  onSelectWatchlist,
  onCreateWatchlist,
  onRenameWatchlist,
  onDeleteWatchlist,
  onExportWatchlist,
  onSetWatchlistColor,
  onRemoveFromWatchlist,
  onMoveSymbols,
  onRequestAddToWatchlist,
  onPickSymbol,
  universeItems,
  selectedSymbol,
}: WatchlistsPanelProps) {
  const marketLabel = market === "india" ? "India" : "US";
  const hasWideTableLayout = useMinWidth(1180);
  const [newWatchlistName, setNewWatchlistName] = useState("");
  const [renameDraft, setRenameDraft] = useState("");
  const [quickAddSymbol, setQuickAddSymbol] = useState("");
  const deferredQuickAddSymbol = useDeferredValue(quickAddSymbol);
  const [selectedSymbols, setSelectedSymbols] = useState<string[]>([]);
  const [bulkTargetWatchlistId, setBulkTargetWatchlistId] = useState<string>("");
  const [rowMoveTargets, setRowMoveTargets] = useState<Record<string, string>>({});
  const [gridOpen, setGridOpen] = useState(false);
  const [gridColumns, setGridColumns] = useState(4);
  const [gridRows, setGridRows] = useState(3);
  const [gridTimeframe, setGridTimeframe] = useState<ChartGridTimeframe>("6M");
  const [gridSortBy, setGridSortBy] = useState<ChartGridSortBy>("selected_return");
  const [gridChartStyle, setGridChartStyle] = useState<ChartGridChartStyle>("line");
  const [gridDisplayMode, setGridDisplayMode] = useState<ChartGridDisplayMode>("compact");

  const activeWatchlist = useMemo(
    () => watchlists.find((watchlist) => watchlist.id === activeWatchlistId) ?? watchlists[0] ?? null,
    [activeWatchlistId, watchlists],
  );

  const lookup = useMemo(() => {
    const map = new Map<string, ScanMatch>();
    for (const item of universeItems) {
      map.set(item.symbol, item);
    }
    return map;
  }, [universeItems]);

  const activeItems = useMemo(
    (): WatchlistDisplayItem[] =>
      (activeWatchlist?.symbols ?? [])
        .map((symbol) => {
          const match = lookup.get(symbol);
          if (match) {
            return {
              symbol: match.symbol,
              name: match.name,
              last_price: match.last_price,
              change_pct: match.change_pct,
              rs_rating: match.rs_rating ?? null,
              market_cap_crore: match.market_cap_crore ?? null,
              stock_return_20d: match.stock_return_20d ?? null,
              stock_return_60d: match.stock_return_60d ?? null,
              stock_return_12m: match.stock_return_12m ?? null,
              isKnown: true,
            } satisfies WatchlistDisplayItem;
          }

          return {
            symbol,
            name: "Saved symbol",
            last_price: 0,
            change_pct: 0,
            rs_rating: null,
            market_cap_crore: null,
            stock_return_20d: null,
            stock_return_60d: null,
            stock_return_12m: null,
            isKnown: false,
          } satisfies WatchlistDisplayItem;
        })
        .sort((left, right) => {
          if (left.isKnown !== right.isKnown) {
            return left.isKnown ? -1 : 1;
          }
          return (right.rs_rating ?? 0) - (left.rs_rating ?? 0);
        }),
    [activeWatchlist?.symbols, lookup],
  );

  const availableMoveTargets = useMemo(
    () => watchlists.filter((watchlist) => watchlist.id !== activeWatchlist?.id),
    [activeWatchlist?.id, watchlists],
  );
  const quickAddSuggestions = useMemo(
    () => buildSymbolSuggestions(universeItems, deferredQuickAddSymbol, 80),
    [deferredQuickAddSymbol, universeItems],
  );

  useEffect(() => {
    setRenameDraft(activeWatchlist?.name ?? "");
  }, [activeWatchlist?.id, activeWatchlist?.name]);

  useEffect(() => {
    setSelectedSymbols([]);
    setRowMoveTargets({});
    setBulkTargetWatchlistId((current) => {
      if (current && availableMoveTargets.some((watchlist) => watchlist.id === current)) {
        return current;
      }
      return availableMoveTargets[0]?.id ?? "";
    });
  }, [activeWatchlist?.id, availableMoveTargets]);

  useEffect(() => {
    setSelectedSymbols((current) => {
      const activeSymbols = new Set(activeWatchlist?.symbols ?? []);
      return current.filter((symbol) => activeSymbols.has(symbol));
    });
  }, [activeWatchlist?.symbols]);

  const shouldVirtualize = hasWideTableLayout && activeItems.length > 60;
  const { containerRef, scrollToKey, totalHeight, visibleRows } = useVirtualRows({
    items: activeItems,
    getKey: (item) => `watchlist:${activeWatchlist?.id ?? "none"}:${item.symbol}`,
    getHeight: () => WATCHLIST_ROW_SLOT_HEIGHT,
  });
  const lastAutoScrolledRowKeyRef = useRef<string | null>(null);

  useEffect(() => {
    if (!shouldVirtualize || !selectedSymbol) {
      if (!selectedSymbol) {
        lastAutoScrolledRowKeyRef.current = null;
      }
      return;
    }

    if (!activeItems.some((item) => item.symbol === selectedSymbol)) {
      return;
    }

    const rowKey = `watchlist:${activeWatchlist?.id ?? "none"}:${selectedSymbol}`;
    if (lastAutoScrolledRowKeyRef.current === rowKey) {
      return;
    }

    lastAutoScrolledRowKeyRef.current = rowKey;
    scrollToKey(rowKey);
  }, [activeItems, activeWatchlist?.id, scrollToKey, selectedSymbol, shouldVirtualize]);

  const toggleSymbolSelection = (symbol: string) => {
    setSelectedSymbols((current) =>
      current.includes(symbol) ? current.filter((item) => item !== symbol) : [...current, symbol],
    );
  };

  const handleMoveOne = (symbol: string) => {
    if (!activeWatchlist) {
      return;
    }
    const targetId = rowMoveTargets[symbol] || bulkTargetWatchlistId || availableMoveTargets[0]?.id;
    if (!targetId) {
      return;
    }
    onMoveSymbols(activeWatchlist.id, targetId, [symbol]);
    setSelectedSymbols((current) => current.filter((item) => item !== symbol));
  };

  const handleBulkMove = () => {
    if (!activeWatchlist || !bulkTargetWatchlistId || selectedSymbols.length === 0) {
      return;
    }
    onMoveSymbols(activeWatchlist.id, bulkTargetWatchlistId, selectedSymbols);
    setSelectedSymbols([]);
  };

  const handleBulkRemove = () => {
    if (!activeWatchlist || selectedSymbols.length === 0) {
      return;
    }
    for (const symbol of selectedSymbols) {
      onRemoveFromWatchlist(activeWatchlist.id, symbol);
    }
    setSelectedSymbols([]);
  };

  const gridCards = useMemo<ChartGridDisplayCard[]>(() => {
    return activeItems.map((item) => {
      const selectedReturn = selectedReturnForGrid(item, gridTimeframe);
      return {
        id: `watchlist:${activeWatchlist?.id ?? "none"}:${item.symbol}`,
        symbol: item.symbol,
        entityLabel: "Stock",
        title: item.symbol,
        subtitle: item.name,
        footerLabel: "Price",
        footerValue: item.isKnown ? formatPrice(item.last_price, market) : "--",
        primaryBadge: {
          label: `${gridTimeframe} ${selectedReturn >= 0 ? "+" : ""}${selectedReturn.toFixed(2)}%`,
          tone: selectedReturn >= 0 ? "positive" : "negative",
        },
        secondaryBadge: {
          label: `1D ${item.change_pct >= 0 ? "+" : ""}${item.change_pct.toFixed(2)}%`,
          tone: item.change_pct >= 0 ? "positive" : "negative",
        },
        points: fallbackSparkline(selectedReturn),
        selectedReturn,
        dayReturn: item.change_pct,
        rsRating: item.rs_rating,
        marketCapCrore: item.market_cap_crore,
        constituents: null,
        onClick: () => onPickSymbol(item.symbol),
      };
    });
  }, [activeItems, activeWatchlist?.id, gridTimeframe, market, onPickSymbol]);

  const gridStats = useMemo<ChartGridStat[]>(() => {
    const advancing = activeItems.filter((item) => item.change_pct > 0).length;
    const declining = activeItems.filter((item) => item.change_pct < 0).length;
    return [
      { label: "Stocks", value: `${activeItems.length}` },
      { label: "Advancing", value: `${advancing}`, tone: advancing >= declining ? "positive" : "neutral" },
      { label: "Declining", value: `${declining}`, tone: declining > advancing ? "negative" : "neutral" },
      { label: "Known", value: `${activeItems.filter((item) => item.isKnown).length}` },
    ];
  }, [activeItems]);

  async function loadGridSeries(symbols: string[], timeframe: ChartGridTimeframe): Promise<Record<string, ChartBar[]>> {
    const payload = await getChartGridSeries(symbols, timeframe, market);
    return payload.items.reduce<Record<string, ChartBar[]>>((accumulator, item) => {
      accumulator[item.symbol] = item.bars;
      return accumulator;
    }, {});
  }

  const renderWatchlistRow = (item: WatchlistDisplayItem, virtualHeight?: number) => (
    <div
      key={`watchlist-${activeWatchlist?.id ?? "none"}-${item.symbol}`}
      className={selectedSymbol === item.symbol ? "scan-row watchlist-row active" : "scan-row watchlist-row"}
      style={virtualHeight ? { height: `${virtualHeight}px` } : undefined}
    >
      <span>
        <input
          type="checkbox"
          checked={selectedSymbols.includes(item.symbol)}
          onChange={() => toggleSymbolSelection(item.symbol)}
          aria-label={`Select ${item.symbol}`}
        />
      </span>
      <button
        type="button"
        className="watchlist-stock-button watchlist-stock-button--compact"
        onClick={() => onPickSymbol(item.symbol)}
      >
        <span>
          <strong>{item.symbol}</strong>
          <small>{item.name}</small>
        </span>
      </button>
      <span>{item.isKnown ? formatPrice(item.last_price, market) : "--"}</span>
      <span className={item.isKnown ? metricClass(item.change_pct) : ""}>{item.isKnown ? formatReturn(item.change_pct) : "--"}</span>
      <span>{item.isKnown ? item.rs_rating ?? "--" : "--"}</span>
      <div className="watchlist-row-actions">
        <button
          type="button"
          className="tool-pill"
          onClick={() => {
            if (activeWatchlist) {
              onRemoveFromWatchlist(activeWatchlist.id, item.symbol);
            }
          }}
        >
          Remove
        </button>
        <select
          value={rowMoveTargets[item.symbol] || bulkTargetWatchlistId || availableMoveTargets[0]?.id || ""}
          onChange={(event) =>
            setRowMoveTargets((current) => ({
              ...current,
              [item.symbol]: event.target.value,
            }))
          }
          disabled={availableMoveTargets.length === 0}
        >
          {availableMoveTargets.length === 0 ? <option value="">No target</option> : null}
          {availableMoveTargets.map((watchlist) => (
            <option key={`row-target-${item.symbol}-${watchlist.id}`} value={watchlist.id}>
              {watchlist.name}
            </option>
          ))}
        </select>
        <button
          type="button"
          className="tool-pill"
          disabled={availableMoveTargets.length === 0}
          onClick={() => handleMoveOne(item.symbol)}
        >
          Move
        </button>
      </div>
    </div>
  );

  return (
    <div className="watchlists-layout">
      <Panel
        title="Watchlists"
        subtitle={`${marketLabel} watchlists and saved collections`}
        className="watchlists-sidebar"
      >
        <div className="watchlists-create">
          <input
            value={newWatchlistName}
            onChange={(event) => setNewWatchlistName(event.target.value)}
            placeholder="New watchlist name"
          />
          <button
            type="button"
            className="nav-button primary"
            onClick={() => {
              const value = newWatchlistName.trim();
              if (!value) {
                return;
              }
              onCreateWatchlist(value);
              setNewWatchlistName("");
            }}
          >
            Create
          </button>
        </div>

        <div className="watchlists-nav">
          {watchlists.map((watchlist) => (
            <div key={watchlist.id} className="watchlist-link-row">
              <button
                type="button"
                className={watchlist.id === activeWatchlist?.id ? "watchlist-link active" : "watchlist-link"}
                style={{
                  borderLeftColor: watchlist.color,
                  boxShadow: watchlist.id === activeWatchlist?.id ? `inset 3px 0 0 ${watchlist.color}` : undefined,
                }}
                onClick={() => onSelectWatchlist(watchlist.id)}
              >
                <span className="watchlist-link-color" style={{ backgroundColor: watchlist.color }} aria-hidden="true" />
                <span>
                  <strong>{watchlist.name}</strong>
                  <small>{watchlist.symbols.length} stocks</small>
                </span>
              </button>
              <label className="watchlist-inline-color" title={`Change color for ${watchlist.name}`}>
                <input
                  type="color"
                  value={watchlist.color}
                  onChange={(event) => onSetWatchlistColor(watchlist.id, event.target.value)}
                  aria-label={`Change color for ${watchlist.name}`}
                />
              </label>
            </div>
          ))}
        </div>
      </Panel>

      <Panel
        title={activeWatchlist?.name ?? "No watchlist selected"}
        subtitle={
          activeWatchlist
            ? `${activeWatchlist.symbols.length} ${market === "india" ? "Indian" : "US"} stocks saved here`
            : `Create a ${marketLabel} watchlist to start collecting names`
        }
        actions={
          activeWatchlist ? (
            <div className="watchlist-actions">
              <input value={renameDraft} onChange={(event) => setRenameDraft(event.target.value)} placeholder="Rename watchlist" />
              <label className="watchlist-color-picker" title="Watchlist color">
                <span>Change color</span>
                <input
                  type="color"
                  value={activeWatchlist.color}
                  onChange={(event) => onSetWatchlistColor(activeWatchlist.id, event.target.value)}
                  aria-label="Watchlist color"
                />
              </label>
              <button
                type="button"
                className="tool-pill"
                onClick={() => {
                  const value = renameDraft.trim();
                  if (!value || !activeWatchlist) {
                    return;
                  }
                  onRenameWatchlist(activeWatchlist.id, value);
                }}
              >
                Rename
              </button>
              <button type="button" className="tool-pill" onClick={() => onExportWatchlist(activeWatchlist.id)}>
                Export .txt
              </button>
              <button type="button" className="tool-pill" onClick={() => setGridOpen(true)} disabled={activeItems.length === 0}>
                Open Grid
              </button>
              <button type="button" className="tool-pill" onClick={() => onDeleteWatchlist(activeWatchlist.id)}>
                Delete
              </button>
            </div>
          ) : null
        }
        className="watchlists-main"
      >
        {activeWatchlist ? (
          <>
            <div className="watchlist-quick-add">
              <input
                list="watchlist-symbols"
                value={quickAddSymbol}
                onChange={(event) => setQuickAddSymbol(event.target.value.toUpperCase())}
                placeholder={`Type ${market === "us" ? "ticker" : "symbol"} to add`}
              />
              <datalist id="watchlist-symbols">
                {quickAddSuggestions.map((item) => (
                  <option key={`watch-option-${item.symbol}`} value={item.symbol}>
                    {item.name}
                  </option>
                ))}
              </datalist>
              <button
                type="button"
                className="nav-button"
                onClick={() => {
                  const symbol = quickAddSymbol.trim().toUpperCase();
                  if (!symbol) {
                    return;
                  }
                  onRequestAddToWatchlist(symbol);
                  setQuickAddSymbol("");
                }}
              >
                Add by Symbol
              </button>
            </div>

            <div className="watchlist-bulk-toolbar">
              <button
                type="button"
                className="tool-pill"
                onClick={() => {
                  if (!activeWatchlist?.symbols?.length) {
                    return;
                  }
                  const allSelected = selectedSymbols.length === activeWatchlist.symbols.length;
                  setSelectedSymbols(allSelected ? [] : [...activeWatchlist.symbols]);
                }}
              >
                {selectedSymbols.length === activeWatchlist.symbols.length && activeWatchlist.symbols.length > 0 ? "Unselect All" : "Select All"}
              </button>
              <span className="watchlist-selection-count">{selectedSymbols.length} selected</span>
              <select
                value={bulkTargetWatchlistId}
                onChange={(event) => setBulkTargetWatchlistId(event.target.value)}
                disabled={availableMoveTargets.length === 0}
              >
                {availableMoveTargets.length === 0 ? <option value="">No other watchlist</option> : null}
                {availableMoveTargets.map((watchlist) => (
                  <option key={`bulk-target-${watchlist.id}`} value={watchlist.id}>
                    {watchlist.name}
                  </option>
                ))}
              </select>
              <button
                type="button"
                className="tool-pill"
                disabled={!bulkTargetWatchlistId || selectedSymbols.length === 0}
                onClick={handleBulkMove}
              >
                Move Selected
              </button>
              <button
                type="button"
                className="tool-pill"
                disabled={selectedSymbols.length === 0}
                onClick={handleBulkRemove}
              >
                Remove Selected
              </button>
            </div>

            <div className="scan-table">
              <div className="scan-table-head watchlist-head">
                <span>Select</span>
                <span>Stock</span>
                <span>Price</span>
                <span>Change</span>
                <span>RS Rating</span>
                <span>Action</span>
              </div>
              <div ref={shouldVirtualize ? containerRef : undefined} className={shouldVirtualize ? "scan-table-body scan-table-body-virtual" : "scan-table-body"}>
                {activeItems.length === 0 ? (
                  <div className="empty-state">This watchlist is empty. Add {market === "us" ? "tickers" : "stocks"} from scanners, sectors, or the chart.</div>
                ) : shouldVirtualize ? (
                  <div className="scan-table-virtual-spacer" style={{ height: `${totalHeight}px` }}>
                    {visibleRows.map((row) => (
                      <div key={row.key} className="scan-table-virtual-slot" style={{ top: `${row.top}px`, height: `${row.height}px` }}>
                        {renderWatchlistRow(row.item, Math.max(0, row.height - WATCHLIST_SLOT_GAP))}
                      </div>
                    ))}
                  </div>
                ) : (
                  activeItems.map((item) => renderWatchlistRow(item))
                )}
              </div>
            </div>
          </>
        ) : (
          <div className="empty-state">Create a watchlist from the left to begin.</div>
        )}
      </Panel>
      {gridOpen ? (
        <Suspense fallback={null}>
          <ChartGridModal
            contextLabel="Watchlist"
            title={activeWatchlist?.name ?? "Watchlist Grid"}
            subtitle={`${activeItems.length} saved ${market === "us" ? "US" : "Indian"} stocks`}
            cards={gridCards}
            stats={gridStats}
            columns={gridColumns}
            rows={gridRows}
            timeframe={gridTimeframe}
            sortBy={gridSortBy}
            chartStyle={gridChartStyle}
            displayMode={gridDisplayMode}
            onColumnsChange={setGridColumns}
            onRowsChange={setGridRows}
            onTimeframeChange={setGridTimeframe}
            onSortByChange={setGridSortBy}
            onChartStyleChange={setGridChartStyle}
            onDisplayModeChange={setGridDisplayMode}
            onLoadSeries={loadGridSeries}
            onClose={() => setGridOpen(false)}
          />
        </Suspense>
      ) : null}
    </div>
  );
}
