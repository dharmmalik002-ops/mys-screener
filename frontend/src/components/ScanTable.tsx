import { Suspense, lazy, type CSSProperties, useEffect, useMemo, useRef, useState } from "react";

import { getChartGridSeries, type ChartBar, type ChartGridTimeframe, type MarketKey, type ScanDescriptor, type ScanMatch, type ScanSectorSummary } from "../lib/api";
import { useMinWidth, useVirtualRows } from "../lib/virtualRows";
import type { ChartGridChartStyle, ChartGridDisplayCard, ChartGridDisplayMode, ChartGridSortBy, ChartGridStat } from "./ChartGridModal";
import { Panel } from "./Panel";

const ChartGridModal = lazy(() => import("./ChartGridModal").then((module) => ({ default: module.ChartGridModal })));
const SCAN_SLOT_GAP = 6;
const SCAN_ROW_SLOT_HEIGHT = 82;
const SCAN_HEADER_SLOT_HEIGHT = 64;

type ScanTableEntry =
  | {
      key: string;
      type: "header";
      sector: string;
      accent: string;
      summary: ScanSectorSummary | undefined;
      count: number;
      isFirst: boolean;
    }
  | {
      key: string;
      type: "row";
      item: ScanMatch;
    };

function formatListingDate(value: string | null | undefined) {
  if (!value) {
    return null;
  }

  const [year, month, day] = value.split("-");
  if (!year || !month || !day) {
    return value;
  }

  return `${day}-${month}-${year}`;
}

type ScanTableProps = {
  market: MarketKey;
  loading: boolean;
  sectorSummaryLoading: boolean;
  scan: ScanDescriptor | null;
  items: ScanMatch[];
  sectorSummaries: ScanSectorSummary[];
  onPickSymbol: (symbol: string) => void;
  onRequestAddToWatchlist: (symbol: string) => void;
  selectedSymbol: string | null;
  sortMode: "change" | "rs";
  onSortModeChange: (mode: "change" | "rs") => void;
  arrangementMode: "flat" | "sector";
  onArrangementModeChange: (mode: "flat" | "sector") => void;
  sectorSortMode: "1W" | "1M" | "count-desc" | "count-asc";
  onSectorSortModeChange: (mode: "1W" | "1M" | "count-desc" | "count-asc") => void;
  onExport: () => void;
};

function formatPrice(value: number, market: MarketKey) {
  const locale = market === "us" ? "en-US" : "en-IN";
  const symbol = market === "us" ? "$" : "₹";
  return `${symbol}${value.toLocaleString(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function sectorAccentColor(label: string): string {
  const palette = ["#5dd6a2", "#58a6ff", "#f7b955", "#ff8a65", "#c792ea", "#5eead4", "#f472b6", "#a3e635"];
  let hash = 0;
  for (const character of label) {
    hash = (hash * 31 + character.charCodeAt(0)) >>> 0;
  }
  return palette[hash % palette.length];
}

function sortScanItems(items: ScanMatch[], sortMode: "change" | "rs") {
  return [...items].sort((left, right) => {
    if (sortMode === "change") {
      return right.change_pct - left.change_pct;
    }
    return (right.rs_rating ?? Number.NEGATIVE_INFINITY) - (left.rs_rating ?? Number.NEGATIVE_INFINITY);
  });
}

function sectorSortValue(
  summary: ScanSectorSummary | undefined,
  sectorItems: ScanMatch[],
  sectorSortMode: "1W" | "1M" | "count-desc" | "count-asc",
) {
  if (!summary) {
    return sectorSortMode === "count-desc" || sectorSortMode === "count-asc"
      ? sectorItems.length
      : Number.NEGATIVE_INFINITY;
  }
  if (sectorSortMode === "count-desc" || sectorSortMode === "count-asc") {
    return summary.current_hits;
  }
  return sectorSortMode === "1W" ? summary.sector_return_1w : summary.sector_return_1m;
}

function formatSectorLine(
  summary: ScanSectorSummary | undefined,
  sectorSortMode: "1W" | "1M" | "count-desc" | "count-asc",
) {
  if (!summary) {
    return "";
  }
  const label =
    sectorSortMode === "1W"
      ? `1W ${summary.sector_return_1w >= 0 ? "+" : ""}${summary.sector_return_1w.toFixed(2)}%`
      : sectorSortMode === "1M"
        ? `1M ${summary.sector_return_1m >= 0 ? "+" : ""}${summary.sector_return_1m.toFixed(2)}%`
        : sectorSortMode === "count-desc"
          ? "Most stocks first"
          : "Fewest stocks first";
  return `${label} · Last week ${summary.prior_week_hits} · Last month ${summary.prior_month_hits}`;
}

function selectedReturnForGrid(item: ScanMatch, timeframe: ChartGridTimeframe) {
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

function scanRowDetail(item: ScanMatch) {
  const listingDate = formatListingDate(item.listing_date);
  const categoryLabel = item.sub_sector && item.sub_sector !== item.sector ? item.sub_sector : item.sector;
  const baseLabel =
    item.gap_pct !== null && item.gap_pct !== undefined
      ? `RS ${item.rs_rating ?? "--"} · Gap ${item.gap_pct.toFixed(2)}%`
      : `RS ${item.rs_rating ?? "--"} · ${categoryLabel}`;
  return listingDate ? `${baseLabel} · Listed ${listingDate}` : baseLabel;
}

function scanEntryHeight(entry: ScanTableEntry) {
  return entry.type === "header" ? SCAN_HEADER_SLOT_HEIGHT : SCAN_ROW_SLOT_HEIGHT;
}

export function ScanTable({
  market,
  loading,
  sectorSummaryLoading,
  scan,
  items,
  sectorSummaries,
  onPickSymbol,
  onRequestAddToWatchlist,
  selectedSymbol,
  sortMode,
  onSortModeChange,
  arrangementMode,
  onArrangementModeChange,
  sectorSortMode,
  onSectorSortModeChange,
  onExport,
}: ScanTableProps) {
  const rowRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const [gridOpen, setGridOpen] = useState(false);
  const [gridColumns, setGridColumns] = useState(4);
  const [gridRows, setGridRows] = useState(3);
  const [gridTimeframe, setGridTimeframe] = useState<ChartGridTimeframe>("6M");
  const [gridSortBy, setGridSortBy] = useState<ChartGridSortBy>("selected_return");
  const [gridChartStyle, setGridChartStyle] = useState<ChartGridChartStyle>("line");
  const [gridDisplayMode, setGridDisplayMode] = useState<ChartGridDisplayMode>("compact");
  const hasWideTableLayout = useMinWidth(1180);
  const showSortToggle = scan?.id !== "custom-scan";
  const summaryBySector = useMemo(
    () => Object.fromEntries(sectorSummaries.map((summary) => [summary.sector, summary])),
    [sectorSummaries],
  );
  const sortedItems = useMemo(() => (showSortToggle ? sortScanItems(items, sortMode) : items), [items, showSortToggle, sortMode]);
  const sectorGroups = useMemo(() => {
    const grouped = sortedItems.reduce<Record<string, ScanMatch[]>>((accumulator, item) => {
      accumulator[item.sector] = [...(accumulator[item.sector] ?? []), item];
      return accumulator;
    }, {});

    return Object.entries(grouped).sort((left, right) => {
      const leftValue = sectorSortValue(summaryBySector[left[0]], left[1], sectorSortMode);
      const rightValue = sectorSortValue(summaryBySector[right[0]], right[1], sectorSortMode);
      if (leftValue !== rightValue) {
        return sectorSortMode === "count-asc" ? leftValue - rightValue : rightValue - leftValue;
      }
      return right[1].length - left[1].length;
    });
  }, [sectorSortMode, sortedItems, summaryBySector]);

  const tableEntries = useMemo<ScanTableEntry[]>(() => {
    if (arrangementMode !== "sector") {
      return sortedItems.map((item) => ({
        key: `row:${item.scan_id}:${item.symbol}`,
        type: "row",
        item,
      }));
    }

    return sectorGroups.flatMap(([sector, sectorItems], index) => {
      const summary = summaryBySector[sector];
      const accent = sectorAccentColor(sector);
      const header = {
        key: `header:${sector}`,
        type: "header",
        sector,
        accent,
        summary,
        count: summary?.current_hits ?? sectorItems.length,
        isFirst: index === 0,
      } satisfies ScanTableEntry;
      const rows = sectorItems.map((item) => ({
        key: `row:${item.scan_id}:${item.symbol}`,
        type: "row",
        item,
      }) satisfies ScanTableEntry);
      return [header, ...rows];
    });
  }, [arrangementMode, sectorGroups, sortedItems, summaryBySector]);

  const shouldVirtualize = hasWideTableLayout && tableEntries.length > 120;
  const { containerRef, scrollToKey, totalHeight, visibleRows } = useVirtualRows({
    items: tableEntries,
    getKey: (entry) => entry.key,
    getHeight: scanEntryHeight,
  });
  const lastAutoScrolledEntryKeyRef = useRef<string | null>(null);

  useEffect(() => {
    if (!selectedSymbol) {
      lastAutoScrolledEntryKeyRef.current = null;
      return;
    }

    const selectedEntry = tableEntries.find((entry) => entry.type === "row" && entry.item.symbol === selectedSymbol);
    const selectedEntryKey = selectedEntry?.key ?? null;
    if (!selectedEntryKey) {
      return;
    }

    if (lastAutoScrolledEntryKeyRef.current === selectedEntryKey) {
      return;
    }

    lastAutoScrolledEntryKeyRef.current = selectedEntryKey;

    if (shouldVirtualize) {
      scrollToKey(selectedEntryKey);
      return;
    }

    const activeRow = rowRefs.current[selectedSymbol];
    activeRow?.scrollIntoView({ block: "nearest", inline: "nearest" });
  }, [arrangementMode, scrollToKey, sectorSortMode, selectedSymbol, shouldVirtualize, sortMode, sortedItems, tableEntries]);

  const gridCards = useMemo<ChartGridDisplayCard[]>(() => {
    return sortedItems.map((item) => {
      const selectedReturn = selectedReturnForGrid(item, gridTimeframe);
      return {
        id: `${item.scan_id}:${item.symbol}`,
        symbol: item.symbol,
        entityLabel: "Stock",
        title: item.symbol,
        subtitle: item.name,
        footerLabel: "Price",
        footerValue: formatPrice(item.last_price, market),
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
        rsRating: item.rs_rating ?? null,
        marketCapCrore: item.market_cap_crore,
        constituents: null,
        onClick: () => onPickSymbol(item.symbol),
      };
    });
  }, [gridTimeframe, market, onPickSymbol, sortedItems]);

  const gridStats = useMemo<ChartGridStat[]>(() => {
    const advancing = sortedItems.filter((item) => item.change_pct > 0).length;
    const declining = sortedItems.filter((item) => item.change_pct < 0).length;
    const topSector = sortedItems[0]?.sector ?? "--";
    return [
      { label: "Stocks", value: `${sortedItems.length}` },
      { label: "Advancing", value: `${advancing}`, tone: advancing >= declining ? "positive" : "neutral" },
      { label: "Declining", value: `${declining}`, tone: declining > advancing ? "negative" : "neutral" },
      { label: "Top Sector", value: topSector },
    ];
  }, [sortedItems]);

  async function loadGridSeries(symbols: string[], timeframe: ChartGridTimeframe): Promise<Record<string, ChartBar[]>> {
    const payload = await getChartGridSeries(symbols, timeframe, market);
    return payload.items.reduce<Record<string, ChartBar[]>>((accumulator, item) => {
      accumulator[item.symbol] = item.bars;
      return accumulator;
    }, {});
  }

  const renderEntry = (entry: ScanTableEntry, virtualHeight?: number) => {
    if (entry.type === "header") {
      return (
        <div
          key={entry.key}
          className={entry.isFirst ? "scan-sector-header scan-sector-header-first" : "scan-sector-header"}
          style={{
            "--sector-accent": entry.accent,
            ...(virtualHeight ? { height: `${virtualHeight}px` } : {}),
          } as CSSProperties}
        >
          <div>
            <strong>{entry.sector} ({entry.count})</strong>
            <small>{formatSectorLine(entry.summary, sectorSortMode)}</small>
          </div>
        </div>
      );
    }

    const { item } = entry;
    return (
      <div
        key={entry.key}
        className={selectedSymbol === item.symbol ? "scan-row active" : "scan-row"}
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
            <small>{scanRowDetail(item)}</small>
          </span>
        </button>
        <span>{formatPrice(item.last_price, market)}</span>
        <span className={item.change_pct >= 0 ? "positive-text" : "negative-text"}>{item.change_pct.toFixed(2)}%</span>
        <span>{item.rs_rating ?? "--"}</span>
        <span>{item.rs_rating_1m_ago ?? "--"}</span>
        <span>{item.relative_volume.toFixed(2)}x</span>
        <span>{item.gap_pct !== null && item.gap_pct !== undefined ? `${item.gap_pct.toFixed(2)}%` : "--"}</span>
        <button type="button" className="tool-pill small" onClick={() => onRequestAddToWatchlist(item.symbol)}>
          Add
        </button>
      </div>
    );
  };

  return (
    <Panel
      title={scan?.name ?? "Loading scan"}
      subtitle={
        scan
          ? `${scan.hit_count} matches across the filtered universe${sectorSummaryLoading ? " · Updating sector history…" : ""} · Use Up/Down arrows to move between charts`
          : "Fetching results"
      }
      actions={(
        <div className="scan-sort-toggle">
          {showSortToggle ? (
            <>
              <button
                type="button"
                className={sortMode === "change" ? "tool-pill active" : "tool-pill"}
                onClick={() => onSortModeChange("change")}
              >
                Change %
              </button>
              <button
                type="button"
                className={sortMode === "rs" ? "tool-pill active" : "tool-pill"}
                onClick={() => onSortModeChange("rs")}
              >
                RS
              </button>
            </>
          ) : null}
          <button
            type="button"
            className={arrangementMode === "flat" ? "tool-pill active" : "tool-pill"}
            onClick={() => onArrangementModeChange("flat")}
          >
            Flat
          </button>
          <button
            type="button"
            className={arrangementMode === "sector" ? "tool-pill active" : "tool-pill"}
            onClick={() => onArrangementModeChange("sector")}
          >
            By Sector
          </button>
          {arrangementMode === "sector" ? (
            <>
              <button
                type="button"
                className={sectorSortMode === "1W" ? "tool-pill active" : "tool-pill"}
                onClick={() => onSectorSortModeChange("1W")}
              >
                Best 1W
              </button>
              <button
                type="button"
                className={sectorSortMode === "1M" ? "tool-pill active" : "tool-pill"}
                onClick={() => onSectorSortModeChange("1M")}
              >
                Best 1M
              </button>
              <button
                type="button"
                className={sectorSortMode === "count-desc" ? "tool-pill active" : "tool-pill"}
                onClick={() => onSectorSortModeChange("count-desc")}
              >
                Most Stocks
              </button>
              <button
                type="button"
                className={sectorSortMode === "count-asc" ? "tool-pill active" : "tool-pill"}
                onClick={() => onSectorSortModeChange("count-asc")}
              >
                Fewest Stocks
              </button>
            </>
          ) : null}
          <button type="button" className="tool-pill" onClick={onExport} disabled={items.length === 0}>
            Export TXT
          </button>
          <button type="button" className="tool-pill" onClick={() => setGridOpen(true)} disabled={items.length === 0}>
            Open Grid
          </button>
        </div>
      )}
    >
      <div className="scan-table">
        <div className="scan-table-head">
          <span>Stock</span>
          <span>Price</span>
          <span>Change</span>
          <span>RS Rating</span>
          <span>RS 1M Ago</span>
          <span>RVOL</span>
          <span>Gap</span>
          <span>Watch</span>
        </div>
        <div ref={shouldVirtualize ? containerRef : undefined} className={shouldVirtualize ? "scan-table-body scan-table-body-virtual" : "scan-table-body"}>
          {items.length === 0 ? (
            <div className="empty-state">{loading ? "Fetching results for this screener..." : "No symbols match this scan at the current filter."}</div>
          ) : shouldVirtualize ? (
            <div className="scan-table-virtual-spacer" style={{ height: `${totalHeight}px` }}>
              {visibleRows.map((row) => (
                <div key={row.key} className="scan-table-virtual-slot" style={{ top: `${row.top}px`, height: `${row.height}px` }}>
                  {renderEntry(row.item, Math.max(0, row.height - SCAN_SLOT_GAP))}
                </div>
              ))}
            </div>
          ) : (
            tableEntries.map((entry) => renderEntry(entry))
          )}
        </div>
      </div>
      {gridOpen ? (
        <Suspense fallback={null}>
          <ChartGridModal
            contextLabel="Scan"
            title={scan?.name ?? "Scan Grid"}
            subtitle={`${sortedItems.length} stocks from the active screener`}
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
    </Panel>
  );
}
