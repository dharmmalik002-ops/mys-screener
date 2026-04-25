import {
  Suspense,
  lazy,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
} from "react";
import { ChevronRight, Trash2 } from "lucide-react";

import {
  getChartGridSeries,
  type ChartBar,
  type ChartGridTimeframe,
  type IndustryGroupsResponse,
  type IndustryGroupStockItem,
  type MarketKey,
  type ScanMatch,
} from "../lib/api";
import { buildSymbolSuggestions } from "../lib/searchSuggestions";
import { useMinWidth, useVirtualRows } from "../lib/virtualRows";
import type {
  ChartGridChartStyle,
  ChartGridDisplayCard,
  ChartGridDisplayMode,
  ChartGridSortBy,
  ChartGridStat,
} from "./ChartGridModal";
import { Panel } from "./Panel";

import "./ScanTable.css";
import "./WatchlistsPanel.css";

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
  groupsData: IndustryGroupsResponse | null;
  selectedSymbol: string | null;
};

type GroupRankInfo = {
  groupRank: number | null;
  rankInGroup: number | null;
  groupSize: number | null;
};

type WatchlistDisplayItem = {
  symbol: string;
  name: string;
  last_price: number;
  change_pct: number;
  rs_rating: number | null;
  rs_rating_1m_ago: number | null;
  market_cap_crore: number | null;
  stock_return_20d: number | null;
  stock_return_60d: number | null;
  stock_return_12m: number | null;
  groupRank: number | null;
  rankInGroup: number | null;
  groupSize: number | null;
  isKnown: boolean;
};

/* ---------- Logo helpers (mirrors ScanTable) ---------- */
const LOGO_MAP: Record<string, string> = {
  RELIANCE: "reliance-industries",
  TCS: "tata-consultancy-services",
  HDFCBANK: "hdfc-bank",
  INFY: "infosys",
  ICICIBANK: "icici-bank",
  SBIN: "state-bank-of-india",
  BHARTIARTL: "bharti-airtel",
  LICI: "lic-of-india",
  ITC: "itc",
  HINDUNILVR: "hindustan-unilever",
  LT: "larsen-and-toubro",
  BAJFINANCE: "bajaj-finance",
  MARUTI: "maruti-suzuki",
  ASIANPAINT: "asian-paints",
  AXISBANK: "axis-bank",
  ADANIENT: "adani-enterprises",
  SUNPHARMA: "sun-pharma",
  TITAN: "titan",
  ULTRACEMCO: "ultratech-cement",
  WIPRO: "wipro",
  NTPC: "ntpc",
  ONGC: "ongc",
  JSWSTEEL: "jsw-steel",
  "M&M": "mahindra-and-mahindra",
  POWERGRID: "power-grid",
  HCLTECH: "hcl-technologies",
  KOTAKBANK: "kotak-mahindra-bank",
  COALINDIA: "coal-india",
  ADANIPORTS: "adani-ports",
  TATASTEEL: "tata-steel",
  GRASIM: "grasim",
  HINDALCO: "hindalco",
  TECHM: "tech-mahindra",
  NESTLEIND: "nestle-india",
  BAJAJFINSV: "bajaj-finserv",
  SBILIFE: "sbi-life-insurance",
  DRREDDY: "dr-reddys-labs",
  CIPLA: "cipla",
  INDUSINDBK: "indusind-bank",
  TATAMOTORS: "tata-motors",
  BPCL: "bpcl",
  BRITANNIA: "britannia",
  EICHERMOT: "eicher-motors",
  DIVISLAB: "divis-labs",
  APOLLOHOSP: "apollo-hospitals",
  UPL: "upl",
  HEROMOTOCO: "hero-motocorp",
  "BAJAJ-AUTO": "bajaj-auto",
  LTIM: "lti-mindtree",
};

function getLogoUrl(symbol: string): string | null {
  const id = LOGO_MAP[symbol.replace("^", "").toUpperCase()];
  return id ? `https://s3-symbol-logo.tradingview.com/${id}.svg` : null;
}

function initials(symbol: string): string {
  return symbol.slice(0, 2).toUpperCase();
}

/* ---------- RS rating badge (mirrors ScanTable) ---------- */
function RsBadge({ rs }: { rs: number | null | undefined }) {
  if (rs === null || rs === undefined || !Number.isFinite(rs)) {
    return <span className="st-rs-badge st-rs-muted">—</span>;
  }
  const tone = rs >= 80 ? "hi" : rs >= 60 ? "mid" : "lo";
  return <span className={`st-rs-badge st-rs-${tone}`}>{Math.round(rs)}</span>;
}

/* ---------- SD price sparkline (synthesized from return windows) ---------- */
function MiniSpark({ item, color }: { item: WatchlistDisplayItem; color: string }) {
  const r12m = item.stock_return_12m ?? 0;
  const r3m = item.stock_return_60d ?? r12m / 4;
  const r1m = item.stock_return_20d ?? r3m / 3;
  const rDay = Number.isFinite(item.change_pct) ? item.change_pct : 0;

  const pts: number[] = [
    100,
    100 + r12m * 0.15,
    100 + r12m * 0.4,
    100 + r12m * 0.65,
    100 + r12m * 0.85 + r3m * 0.2,
    100 + r12m * 0.95 + r3m * 0.6 + r1m * 0.4,
    100 + r12m + r3m * 0.7 + r1m * 0.9 + rDay * 0.5,
  ];

  return <SparkPath pts={pts} color={color} />;
}

/* ---------- RS-line sparkline (uses rs_rating + rs_rating_1m_ago) ---------- */
function RsLineSpark({ item }: { item: WatchlistDisplayItem }) {
  const rsNow = item.rs_rating;
  const rsOld = item.rs_rating_1m_ago;
  if (rsNow === null && rsOld === null) {
    return <span className="wl-rank-muted" style={{ fontSize: "0.74rem" }}>—</span>;
  }
  const end = rsNow ?? rsOld ?? 50;
  const start = rsOld ?? rsNow ?? 50;
  const mid1 = start + (end - start) * 0.35 + (rsNow !== null ? Math.sin(rsNow) * 1.5 : 0);
  const mid2 = start + (end - start) * 0.7 + (rsNow !== null ? Math.cos(rsNow) * 1.5 : 0);
  const pts = [start, mid1, (start + end) / 2, mid2, end];
  const up = end >= start;
  return <SparkPath pts={pts} color={up ? "#3b82f6" : "#a855f7"} strokeWidth={1.6} />;
}

function SparkPath({
  pts,
  color,
  strokeWidth = 1.6,
}: {
  pts: number[];
  color: string;
  strokeWidth?: number;
}) {
  const minV = Math.min(...pts);
  const maxV = Math.max(...pts);
  const range = Math.max(0.001, maxV - minV);
  const W = 80;
  const H = 26;
  const path = pts
    .map((v, i) => {
      const x = (i / (pts.length - 1)) * W;
      const y = H - ((v - minV) / range) * H;
      return `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="wl-spark-svg" aria-hidden>
      <path d={path} stroke={color} strokeWidth={strokeWidth} fill="none" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

/* ---------- Helpers ---------- */
function formatReturn(value: number) {
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function metricClass(value: number) {
  return value >= 0 ? "positive-text" : "negative-text";
}

function formatPrice(value: number, _market: MarketKey) {
  return `₹${value.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function formatMarketCap(value: number | null): string {
  if (!value || value <= 0) return "--";
  if (value >= 100_000) return `₹${(value / 100_000).toFixed(2)}L Cr`;
  if (value >= 1_000) return `₹${(value / 1_000).toFixed(1)}K Cr`;
  return `₹${value.toFixed(0)} Cr`;
}

function selectedReturnForGrid(item: WatchlistDisplayItem, timeframe: ChartGridTimeframe) {
  if (timeframe === "3M") {
    return item.stock_return_60d ?? item.stock_return_20d ?? item.change_pct;
  }
  return item.stock_return_12m ?? item.stock_return_60d ?? item.change_pct;
}

function fallbackSparkline(returnPct: number) {
  const now = Math.floor(Date.now() / 1000);
  const baseline = 100;
  const current = baseline * (1 + returnPct / 100);
  return [
    { time: now - 63 * 24 * 60 * 60, value: Number(baseline.toFixed(4)) },
    { time: now, value: Number(current.toFixed(4)) },
  ];
}

function sortGroupMembers(members: IndustryGroupStockItem[]) {
  return [...members].sort((left, right) => {
    const rsDiff = (right.rs_rating ?? -1) - (left.rs_rating ?? -1);
    if (rsDiff !== 0) return rsDiff;
    return right.return_3m - left.return_3m;
  });
}

function buildGroupRankIndex(payload: IndustryGroupsResponse | null): Map<string, GroupRankInfo> {
  const index = new Map<string, GroupRankInfo>();
  if (!payload) return index;

  const groupRankById = new Map<string, number>();
  for (const group of payload.groups) {
    groupRankById.set(group.group_id, group.rank);
  }

  const stocksByGroup = new Map<string, IndustryGroupStockItem[]>();
  for (const stock of payload.stocks) {
    const list = stocksByGroup.get(stock.final_group_id) ?? [];
    list.push(stock);
    stocksByGroup.set(stock.final_group_id, list);
  }

  stocksByGroup.forEach((members, groupId) => {
    const sorted = sortGroupMembers(members);
    const groupRank = groupRankById.get(groupId) ?? null;
    sorted.forEach((member, idx) => {
      index.set(member.symbol.toUpperCase(), {
        groupRank,
        rankInGroup: idx + 1,
        groupSize: sorted.length,
      });
    });
  });

  return index;
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
  groupsData,
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
  const [movePopoverFor, setMovePopoverFor] = useState<string | null>(null);
  const movePopoverWrapRef = useRef<HTMLDivElement | null>(null);
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

  const groupRankIndex = useMemo(() => buildGroupRankIndex(groupsData), [groupsData]);

  const activeItems = useMemo(
    (): WatchlistDisplayItem[] =>
      (activeWatchlist?.symbols ?? [])
        .map((symbol) => {
          const match = lookup.get(symbol);
          const rankInfo = groupRankIndex.get(symbol.toUpperCase()) ?? {
            groupRank: null,
            rankInGroup: null,
            groupSize: null,
          };

          if (match) {
            return {
              symbol: match.symbol,
              name: match.name,
              last_price: match.last_price,
              change_pct: match.change_pct,
              rs_rating: match.rs_rating ?? null,
              rs_rating_1m_ago: match.rs_rating_1m_ago ?? null,
              market_cap_crore: match.market_cap_crore ?? null,
              stock_return_20d: match.stock_return_20d ?? null,
              stock_return_60d: match.stock_return_60d ?? null,
              stock_return_12m: match.stock_return_12m ?? null,
              groupRank: rankInfo.groupRank,
              rankInGroup: rankInfo.rankInGroup,
              groupSize: rankInfo.groupSize,
              isKnown: true,
            } satisfies WatchlistDisplayItem;
          }

          return {
            symbol,
            name: "Saved symbol",
            last_price: 0,
            change_pct: 0,
            rs_rating: null,
            rs_rating_1m_ago: null,
            market_cap_crore: null,
            stock_return_20d: null,
            stock_return_60d: null,
            stock_return_12m: null,
            groupRank: rankInfo.groupRank,
            rankInGroup: rankInfo.rankInGroup,
            groupSize: rankInfo.groupSize,
            isKnown: false,
          } satisfies WatchlistDisplayItem;
        })
        .sort((left, right) => {
          if (left.isKnown !== right.isKnown) return left.isKnown ? -1 : 1;
          return (right.rs_rating ?? 0) - (left.rs_rating ?? 0);
        }),
    [activeWatchlist?.symbols, groupRankIndex, lookup],
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
    setMovePopoverFor(null);
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

  // Click-outside for the per-row Move popover.
  useEffect(() => {
    if (!movePopoverFor) return;
    const handler = (event: MouseEvent) => {
      const wrap = movePopoverWrapRef.current;
      if (wrap && !wrap.contains(event.target as Node)) {
        setMovePopoverFor(null);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [movePopoverFor]);

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
    if (!activeItems.some((item) => item.symbol === selectedSymbol)) return;
    const rowKey = `watchlist:${activeWatchlist?.id ?? "none"}:${selectedSymbol}`;
    if (lastAutoScrolledRowKeyRef.current === rowKey) return;
    lastAutoScrolledRowKeyRef.current = rowKey;
    scrollToKey(rowKey);
  }, [activeItems, activeWatchlist?.id, scrollToKey, selectedSymbol, shouldVirtualize]);

  const toggleSymbolSelection = (symbol: string) => {
    setSelectedSymbols((current) =>
      current.includes(symbol) ? current.filter((item) => item !== symbol) : [...current, symbol],
    );
  };

  const handleBulkMove = () => {
    if (!activeWatchlist || !bulkTargetWatchlistId || selectedSymbols.length === 0) return;
    onMoveSymbols(activeWatchlist.id, bulkTargetWatchlistId, selectedSymbols);
    setSelectedSymbols([]);
  };

  const handleBulkRemove = () => {
    if (!activeWatchlist || selectedSymbols.length === 0) return;
    for (const symbol of selectedSymbols) {
      onRemoveFromWatchlist(activeWatchlist.id, symbol);
    }
    setSelectedSymbols([]);
  };

  const handleQuickMove = (symbol: string, targetId: string) => {
    if (!activeWatchlist || !targetId) return;
    onMoveSymbols(activeWatchlist.id, targetId, [symbol]);
    setSelectedSymbols((current) => current.filter((item) => item !== symbol));
    setMovePopoverFor(null);
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

  const gridTemplate =
    "minmax(170px, 1.6fr) 84px 76px 56px 90px 90px 84px 88px 96px 36px";

  const renderWatchlistRow = (item: WatchlistDisplayItem, virtualHeight?: number) => {
    const logoUrl = getLogoUrl(item.symbol);
    const up = item.change_pct >= 0;
    const sparkColor = up ? "#10b981" : "#ef4444";
    const isActive = selectedSymbol === item.symbol;
    const checkboxOn = selectedSymbols.includes(item.symbol);
    const popOpen = movePopoverFor === item.symbol;

    return (
      <div
        key={`watchlist-${activeWatchlist?.id ?? "none"}-${item.symbol}`}
        className={isActive ? "scan-row wl-row active" : "scan-row wl-row"}
        style={
          {
            "--wl-grid": gridTemplate,
            ...(virtualHeight ? { height: `${virtualHeight}px` } : {}),
          } as CSSProperties
        }
      >
        <button
          type="button"
          className="scan-row-main st-row-main"
          onClick={() => onPickSymbol(item.symbol)}
        >
          <span className="wl-row-checkbox" onClick={(event) => event.stopPropagation()}>
            <input
              type="checkbox"
              checked={checkboxOn}
              onChange={() => toggleSymbolSelection(item.symbol)}
              aria-label={`Select ${item.symbol}`}
              onClick={(event) => event.stopPropagation()}
            />
          </span>
          <span className="st-logo">
            {logoUrl ? (
              <img
                src={logoUrl}
                alt=""
                onError={(event) => {
                  event.currentTarget.style.display = "none";
                }}
              />
            ) : (
              <span className="st-logo-fallback">{initials(item.symbol)}</span>
            )}
          </span>
          <span className="st-name">
            <strong>{item.symbol}</strong>
            <small>{item.name}</small>
          </span>
        </button>

        <span className="st-price">{item.isKnown ? formatPrice(item.last_price, market) : "--"}</span>

        <span className={`st-change ${item.isKnown ? metricClass(item.change_pct) : ""}`}>
          {item.isKnown ? formatReturn(item.change_pct) : "--"}
        </span>

        <span className="st-cell-center">
          <RsBadge rs={item.rs_rating} />
        </span>

        <span className="wl-spark-cell">
          {item.isKnown ? <MiniSpark item={item} color={sparkColor} /> : <span className="wl-rank-muted" style={{ fontSize: "0.74rem" }}>—</span>}
        </span>

        <span className="wl-spark-cell">
          <RsLineSpark item={item} />
        </span>

        <span className="wl-rank">
          {item.groupRank !== null ? (
            <span className="wl-rank-pill">#{item.groupRank}</span>
          ) : (
            <span className="wl-rank-muted">—</span>
          )}
        </span>

        <span className="wl-rank">
          {item.rankInGroup !== null && item.groupSize !== null ? (
            <span className="wl-rank-pill">
              {item.rankInGroup}/{item.groupSize}
            </span>
          ) : (
            <span className="wl-rank-muted">—</span>
          )}
        </span>

        <span className="wl-mcap">{formatMarketCap(item.market_cap_crore)}</span>

        <span className="wl-move-wrap" ref={popOpen ? movePopoverWrapRef : undefined}>
          <button
            type="button"
            className="wl-move-btn"
            disabled={availableMoveTargets.length === 0}
            title={availableMoveTargets.length === 0 ? "No other watchlist" : `Move ${item.symbol} to…`}
            onClick={(event) => {
              event.stopPropagation();
              setMovePopoverFor((current) => (current === item.symbol ? null : item.symbol));
            }}
          >
            <ChevronRight size={14} strokeWidth={2.4} />
          </button>
          {popOpen ? (
            <div className="wl-move-pop" onClick={(event) => event.stopPropagation()}>
              {availableMoveTargets.length === 0 ? (
                <span className="wl-move-empty">Create another watchlist to move stocks.</span>
              ) : (
                <>
                  {availableMoveTargets.map((target) => (
                    <button
                      key={`row-move-${item.symbol}-${target.id}`}
                      type="button"
                      className="wl-move-item"
                      onClick={() => handleQuickMove(item.symbol, target.id)}
                    >
                      <span className="wl-swatch-mini" style={{ background: target.color }} aria-hidden="true" />
                      <span>{target.name}</span>
                    </button>
                  ))}
                  <button
                    type="button"
                    className="wl-move-item"
                    onClick={() => {
                      if (activeWatchlist) onRemoveFromWatchlist(activeWatchlist.id, item.symbol);
                      setMovePopoverFor(null);
                    }}
                    style={{ color: "#ef4444" }}
                  >
                    <Trash2 size={12} />
                    <span>Remove from this watchlist</span>
                  </button>
                </>
              )}
            </div>
          ) : null}
        </span>
      </div>
    );
  };

  const headTemplate: CSSProperties = { "--wl-grid": gridTemplate } as CSSProperties;

  return (
    <div className="watchlists-layout">
      <Panel
        title="Watchlists"
        subtitle={`${marketLabel} watchlists and saved collections`}
        className="watchlists-sidebar wl-card"
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
              if (!value) return;
              onCreateWatchlist(value);
              setNewWatchlistName("");
            }}
          >
            Create
          </button>
        </div>

        <div className="wl-nav">
          {watchlists.map((watchlist) => (
            <div
              key={watchlist.id}
              className={watchlist.id === activeWatchlist?.id ? "wl-nav-row is-active" : "wl-nav-row"}
              style={{ borderLeft: `4px solid ${watchlist.color}` }}
            >
              <button
                type="button"
                className="wl-nav-button"
                onClick={() => onSelectWatchlist(watchlist.id)}
              >
                <span className="wl-nav-text">
                  <strong>{watchlist.name}</strong>
                  <small>{watchlist.symbols.length} stocks</small>
                </span>
              </button>
              <span
                className="wl-swatch"
                style={{ background: watchlist.color }}
                title={`Change color for ${watchlist.name}`}
              >
                <input
                  type="color"
                  value={watchlist.color}
                  onChange={(event) => onSetWatchlistColor(watchlist.id, event.target.value)}
                  aria-label={`Change color for ${watchlist.name}`}
                />
              </span>
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
            <div className="wl-actions">
              <input
                type="text"
                value={renameDraft}
                onChange={(event) => setRenameDraft(event.target.value)}
                placeholder="Rename watchlist"
              />
              <button
                type="button"
                className="st-btn"
                onClick={() => {
                  const value = renameDraft.trim();
                  if (!value || !activeWatchlist) return;
                  onRenameWatchlist(activeWatchlist.id, value);
                }}
              >
                Rename
              </button>
              <button type="button" className="st-btn" onClick={() => onExportWatchlist(activeWatchlist.id)}>
                Export .txt
              </button>
              <button
                type="button"
                className="st-btn"
                onClick={() => setGridOpen(true)}
                disabled={activeItems.length === 0}
              >
                Open Grid
              </button>
              <button type="button" className="st-btn" onClick={() => onDeleteWatchlist(activeWatchlist.id)}>
                Delete
              </button>
            </div>
          ) : null
        }
        className="watchlists-main wl-card"
      >
        {activeWatchlist ? (
          <>
            <div className="wl-quick-add">
              <input
                list="watchlist-symbols"
                value={quickAddSymbol}
                onChange={(event) => setQuickAddSymbol(event.target.value.toUpperCase())}
                placeholder="Type a symbol to add (e.g. RELIANCE)"
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
                className="st-btn"
                onClick={() => {
                  const symbol = quickAddSymbol.trim().toUpperCase();
                  if (!symbol) return;
                  onRequestAddToWatchlist(symbol);
                  setQuickAddSymbol("");
                }}
              >
                Add
              </button>
            </div>

            <div className="wl-bulk-toolbar">
              <button
                type="button"
                className="st-btn"
                onClick={() => {
                  if (!activeWatchlist?.symbols?.length) return;
                  const allSelected = selectedSymbols.length === activeWatchlist.symbols.length;
                  setSelectedSymbols(allSelected ? [] : [...activeWatchlist.symbols]);
                }}
              >
                {selectedSymbols.length === activeWatchlist.symbols.length && activeWatchlist.symbols.length > 0
                  ? "Unselect All"
                  : "Select All"}
              </button>
              <span className="wl-selection-count">{selectedSymbols.length} selected</span>
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
                className="st-btn"
                disabled={!bulkTargetWatchlistId || selectedSymbols.length === 0}
                onClick={handleBulkMove}
              >
                Move Selected
              </button>
              <button
                type="button"
                className="st-btn"
                disabled={selectedSymbols.length === 0}
                onClick={handleBulkRemove}
              >
                Remove Selected
              </button>
            </div>

            <div className="scan-table">
              <div className="scan-table-head wl-head" style={headTemplate}>
                <span>Symbol</span>
                <span className="wl-right">Price</span>
                <span className="wl-right">Change</span>
                <span className="wl-center">RS</span>
                <span className="wl-center">SD Chart</span>
                <span className="wl-center">RS Line</span>
                <span className="wl-center">Group Rank</span>
                <span className="wl-center">Rank in Group</span>
                <span className="wl-right">Market Cap</span>
                <span></span>
              </div>
              <div
                ref={shouldVirtualize ? containerRef : undefined}
                className={shouldVirtualize ? "scan-table-body scan-table-body-virtual" : "scan-table-body"}
              >
                {activeItems.length === 0 ? (
                  <div className="empty-state">This watchlist is empty. Add stocks from scanners, groups, or the chart.</div>
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
            subtitle={`${activeItems.length} saved Indian stocks`}
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
