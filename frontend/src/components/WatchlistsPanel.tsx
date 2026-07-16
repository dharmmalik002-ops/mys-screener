import {
  Suspense,
  lazy,
  useCallback,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
} from "react";
import { createPortal } from "react-dom";
import { LayoutGrid, Newspaper, Plus, Trash2 } from "lucide-react";

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
  ChartGridGroupSection,
  ChartGridSortBy,
  ChartGridStat,
} from "./ChartGridModal";
import { Panel } from "./Panel";
import { NewsModal } from "./NewsModal";

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
  onPrefetchSymbol?: (symbol: string) => void;
  onImportSymbols?: (watchlistId: string, raw: string) => { added: number; duplicates: number };
  universeItems: ScanMatch[];
  groupsData: IndustryGroupsResponse | null;
  selectedSymbol: string | null;
};

type GroupRankInfo = {
  groupId: string | null;
  groupName: string | null;
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
  groupId: string | null;
  groupName: string | null;
  groupRank: number | null;
  rankInGroup: number | null;
  groupSize: number | null;
  isKnown: boolean;
};

/* ---------- Pipeline stages + staleness ---------- */
const WATCHLIST_STAGES = ["Watching", "Near Pivot", "Triggered", "Position"] as const;
type WatchlistStage = (typeof WATCHLIST_STAGES)[number];
const STAGES_STORAGE_KEY = "stockScanner.watchlistStages.v1";

function readStoredStages(): Record<string, WatchlistStage> {
  if (typeof window === "undefined") return {};
  try {
    const raw = window.localStorage.getItem(STAGES_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : {};
    if (!parsed || typeof parsed !== "object") return {};
    const result: Record<string, WatchlistStage> = {};
    for (const [symbol, stage] of Object.entries(parsed)) {
      if ((WATCHLIST_STAGES as readonly string[]).includes(String(stage))) {
        result[symbol] = stage as WatchlistStage;
      }
    }
    return result;
  } catch {
    return {};
  }
}

// Dead-momentum hygiene: a watchlist symbol whose RS rating has dropped
// below the momentum floor is flagged so stale names don't linger unnoticed.
function stalenessFor(item: WatchlistDisplayItem): "dead" | "fading" | null {
  if (item.rs_rating === null) return null;
  if (item.rs_rating < 50) return "dead";
  if (item.rs_rating < 70) return "fading";
  return null;
}

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

  const groupMetaById = new Map<string, { rank: number; name: string }>();
  for (const group of payload.groups) {
    groupMetaById.set(group.group_id, { rank: group.rank, name: group.group_name });
  }

  const stocksByGroup = new Map<string, IndustryGroupStockItem[]>();
  for (const stock of payload.stocks) {
    const list = stocksByGroup.get(stock.final_group_id) ?? [];
    list.push(stock);
    stocksByGroup.set(stock.final_group_id, list);
  }

  stocksByGroup.forEach((members, groupId) => {
    const sorted = sortGroupMembers(members);
    const meta = groupMetaById.get(groupId);
    sorted.forEach((member, idx) => {
      index.set(member.symbol.toUpperCase(), {
        groupId,
        groupName: meta?.name ?? null,
        groupRank: meta?.rank ?? null,
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
  onPrefetchSymbol,
  onImportSymbols,
  universeItems,
  groupsData,
  selectedSymbol,
}: WatchlistsPanelProps) {
  const [importOpen, setImportOpen] = useState(false);
  const [importText, setImportText] = useState("");
  const [importMessage, setImportMessage] = useState<string | null>(null);
  const marketLabel = market === "india" ? "India" : "US";
  const hasWideTableLayout = useMinWidth(1180);
  const [newWatchlistName, setNewWatchlistName] = useState("");
  const [renameDraft, setRenameDraft] = useState("");
  const [quickAddSymbol, setQuickAddSymbol] = useState("");
  const deferredQuickAddSymbol = useDeferredValue(quickAddSymbol);
  const [selectedSymbols, setSelectedSymbols] = useState<string[]>([]);
  const [bulkTargetWatchlistId, setBulkTargetWatchlistId] = useState<string>("");
  const [movePopoverFor, setMovePopoverFor] = useState<string | null>(null);
  const [stages, setStages] = useState<Record<string, WatchlistStage>>(() => readStoredStages());

  useEffect(() => {
    try {
      window.localStorage.setItem(STAGES_STORAGE_KEY, JSON.stringify(stages));
    } catch {
      // Ignore storage failures.
    }
  }, [stages]);

  const cycleStage = (symbol: string) => {
    setStages((current) => {
      const stage = current[symbol] ?? WATCHLIST_STAGES[0];
      const next = WATCHLIST_STAGES[(WATCHLIST_STAGES.indexOf(stage) + 1) % WATCHLIST_STAGES.length];
      return { ...current, [symbol]: next };
    });
  };
  const [movePopoverAnchor, setMovePopoverAnchor] = useState<{ top: number; left: number } | null>(null);
  const moveButtonRefs = useRef<Record<string, HTMLButtonElement | null>>({});
  const movePopoverEl = useRef<HTMLDivElement | null>(null);
  const prefetchTimerRef = useRef<number | null>(null);
  const requestPrefetchSymbol = useCallback(
    (symbol: string) => {
      if (!onPrefetchSymbol) {
        return;
      }
      if (prefetchTimerRef.current !== null) {
        window.clearTimeout(prefetchTimerRef.current);
      }
      prefetchTimerRef.current = window.setTimeout(() => {
        prefetchTimerRef.current = null;
        onPrefetchSymbol(symbol);
      }, 150);
    },
    [onPrefetchSymbol],
  );
  const cancelPrefetch = useCallback(() => {
    if (prefetchTimerRef.current !== null) {
      window.clearTimeout(prefetchTimerRef.current);
      prefetchTimerRef.current = null;
    }
  }, []);
  useEffect(() => () => cancelPrefetch(), [cancelPrefetch]);
  const [gridOpen, setGridOpen] = useState(false);
  const [newsOpen, setNewsOpen] = useState(false);
  const [newsContext, setNewsContext] = useState<{ title: string; symbols: string[]; accent: string } | null>(null);
  const openNewsForWatchlist = useCallback((wl: LocalWatchlist) => {
    setNewsContext({ title: `News · ${wl.name}`, symbols: wl.symbols, accent: wl.color || "#ff6b6b" });
    setNewsOpen(true);
  }, []);
  // Defaults per the owner's spec: big side-by-side charts — 2 per row, one
  // row filling the screen, 3 months of daily bars.
  const [gridColumns, setGridColumns] = useState(2);
  const [gridRows, setGridRows] = useState(1);
  const [gridTimeframe, setGridTimeframe] = useState<ChartGridTimeframe>("6M");
  const [gridSortBy, setGridSortBy] = useState<ChartGridSortBy>("rs_rating");
  const [gridChartStyle, setGridChartStyle] = useState<ChartGridChartStyle>("candles");
  const [gridDisplayMode, setGridDisplayMode] = useState<ChartGridDisplayMode>("normal");
  const [arrangementMode, setArrangementMode] = useState<"flat" | "group">("flat");

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
            groupId: null,
            groupName: null,
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
              groupId: rankInfo.groupId,
              groupName: rankInfo.groupName,
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
            groupId: rankInfo.groupId,
            groupName: rankInfo.groupName,
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

  // Bucket the watchlist by industry group for the "By Group" arrangement.
  // Groups are sorted by group_rank ascending (#1 first); anything without
  // a resolved group falls into a trailing "Ungrouped" bucket.
  const groupedView = useMemo(() => {
    type Bucket = { groupId: string; groupName: string; rank: number; items: WatchlistDisplayItem[] };
    const buckets = new Map<string, Bucket>();
    const ungrouped: WatchlistDisplayItem[] = [];
    for (const item of activeItems) {
      if (!item.groupId || item.groupRank === null || !item.groupName) {
        ungrouped.push(item);
        continue;
      }
      const existing = buckets.get(item.groupId);
      if (existing) {
        existing.items.push(item);
      } else {
        buckets.set(item.groupId, {
          groupId: item.groupId,
          groupName: item.groupName,
          rank: item.groupRank,
          items: [item],
        });
      }
    }
    const ordered = Array.from(buckets.values()).sort((a, b) => a.rank - b.rank);
    return { ordered, ungrouped };
  }, [activeItems]);

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
    setMovePopoverAnchor(null);
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

  // Close per-row Add popover on outside click, scroll, or resize.
  useEffect(() => {
    if (!movePopoverFor) return;
    const close = () => {
      setMovePopoverFor(null);
      setMovePopoverAnchor(null);
    };
    const onMouseDown = (event: MouseEvent) => {
      const target = event.target as Node;
      const trigger = moveButtonRefs.current[movePopoverFor];
      const pop = movePopoverEl.current;
      if (trigger?.contains(target)) return;
      if (pop?.contains(target)) return;
      close();
    };
    document.addEventListener("mousedown", onMouseDown);
    window.addEventListener("scroll", close, true);
    window.addEventListener("resize", close);
    return () => {
      document.removeEventListener("mousedown", onMouseDown);
      window.removeEventListener("scroll", close, true);
      window.removeEventListener("resize", close);
    };
  }, [movePopoverFor]);

  const openMovePopover = (symbol: string) => {
    if (movePopoverFor === symbol) {
      setMovePopoverFor(null);
      setMovePopoverAnchor(null);
      return;
    }
    const trigger = moveButtonRefs.current[symbol];
    if (!trigger) return;
    const rect = trigger.getBoundingClientRect();
    const POP_WIDTH = 220;
    setMovePopoverAnchor({
      top: rect.bottom + 6,
      left: Math.max(8, rect.right - POP_WIDTH),
    });
    setMovePopoverFor(symbol);
  };

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
    setMovePopoverAnchor(null);
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

  const gridGroupSections = useMemo<ChartGridGroupSection[]>(() => {
    if (!groupsData) return [];
    const cardBySymbol = new Map(gridCards.map((card) => [card.symbol?.toUpperCase() ?? "", card]));
    const cardsByGroup = new Map<string, ChartGridDisplayCard[]>();
    const assigned = new Set<string>();

    for (const stock of groupsData.stocks) {
      const symbol = stock.symbol.toUpperCase();
      const card = cardBySymbol.get(symbol);
      if (!card || !stock.final_group_id) continue;
      const current = cardsByGroup.get(stock.final_group_id) ?? [];
      current.push({
        ...card,
        entityLabel: stock.final_group_name || card.entityLabel,
        subtitle: `${card.subtitle} · ${stock.final_group_name}`,
      });
      cardsByGroup.set(stock.final_group_id, current);
      assigned.add(symbol);
    }

    const sections = groupsData.groups
      .map((group) => {
        const sectionCards = cardsByGroup.get(group.group_id) ?? [];
        if (!sectionCards.length) return null;
        return {
          id: group.group_id,
          title: group.group_name,
          subtitle: `${group.parent_sector} · base rank #${group.rank} · ${sectionCards.length} watchlist stock${sectionCards.length === 1 ? "" : "s"}`,
          baseRank: group.rank,
          stockCount: group.stock_count,
          returns: {
            "1W": group.return_1w,
            "1M": group.return_1m,
            "3M": group.return_3m,
            "6M": group.return_6m,
          },
          cards: sectionCards,
        } satisfies ChartGridGroupSection;
      })
      .filter((section): section is ChartGridGroupSection => Boolean(section));

    const ungrouped = gridCards.filter((card) => card.symbol && !assigned.has(card.symbol.toUpperCase()));
    if (ungrouped.length) {
      sections.push({
        id: "ungrouped",
        title: "Ungrouped",
        subtitle: `${ungrouped.length} watchlist stock${ungrouped.length === 1 ? "" : "s"} without a resolved industry group`,
        baseRank: 9999,
        stockCount: ungrouped.length,
        returns: { "1W": -999, "1M": -999, "3M": -999, "6M": -999 },
        cards: ungrouped,
      });
    }

    return sections;
  }, [gridCards, groupsData]);

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

  // Flexible tracks: fixed-px columns overflow (and get clipped at) the card
  // edge when the middle panel is squeezed by the chart panel — "Rank in
  // Group" was rendering as "RANK IN G…" with its values cut off.
  const gridTemplate =
    "minmax(150px, 1.8fr) minmax(62px, 86px) minmax(70px, 88px) minmax(58px, 76px) minmax(42px, 56px) minmax(62px, 88px) minmax(70px, 96px) 32px";

  const renderWatchlistRow = (item: WatchlistDisplayItem, virtualHeight?: number) => {
    const logoUrl = getLogoUrl(item.symbol);
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
        onMouseEnter={() => requestPrefetchSymbol(item.symbol)}
        onMouseLeave={cancelPrefetch}
        onFocus={() => requestPrefetchSymbol(item.symbol)}
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
            <strong>
              {item.symbol}
              {stalenessFor(item) ? (
                <span
                  className={`wl-stale-chip ${stalenessFor(item)}`}
                  title={
                    stalenessFor(item) === "dead"
                      ? `RS rating ${item.rs_rating} — momentum is gone. Why is this still on the list?`
                      : `RS rating ${item.rs_rating} — momentum fading below the 70 floor.`
                  }
                >
                  {stalenessFor(item) === "dead" ? "DEAD" : "FADING"}
                </span>
              ) : null}
            </strong>
            <small>{item.name}</small>
          </span>
        </button>

        <button
          type="button"
          className={`wl-stage-chip stage-${(stages[item.symbol] ?? "Watching").replace(/\s+/g, "-").toLowerCase()}`}
          title="Pipeline stage — click to cycle: Watching → Near Pivot → Triggered → Position"
          onClick={(event) => {
            event.stopPropagation();
            cycleStage(item.symbol);
          }}
        >
          {stages[item.symbol] ?? "Watching"}
        </button>

        <span className="st-price">{item.isKnown ? formatPrice(item.last_price, market) : "--"}</span>

        <span className={`st-change ${item.isKnown ? metricClass(item.change_pct) : ""}`}>
          {item.isKnown ? formatReturn(item.change_pct) : "--"}
        </span>

        <span className="st-cell-center">
          <RsBadge rs={item.rs_rating} />
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

        <span className="wl-move-wrap">
          <button
            type="button"
            ref={(el) => {
              moveButtonRefs.current[item.symbol] = el;
            }}
            className={popOpen ? "wl-move-btn is-open" : "wl-move-btn"}
            disabled={availableMoveTargets.length === 0}
            title={availableMoveTargets.length === 0 ? "No other watchlist" : `Add ${item.symbol} to…`}
            onClick={(event) => {
              event.stopPropagation();
              openMovePopover(item.symbol);
            }}
          >
            <Plus size={14} strokeWidth={2.6} />
          </button>
        </span>
      </div>
    );
  };

  const renderMovePopover = () => {
    if (!movePopoverFor || !movePopoverAnchor) return null;
    const symbol = movePopoverFor;
    return createPortal(
      <div
        ref={movePopoverEl}
        className="wl-move-pop wl-move-pop--portal"
        style={{ top: `${movePopoverAnchor.top}px`, left: `${movePopoverAnchor.left}px` }}
        onClick={(event) => event.stopPropagation()}
        role="menu"
      >
        {availableMoveTargets.length === 0 ? (
          <span className="wl-move-empty">Create another watchlist to add stocks.</span>
        ) : (
          <>
            <span className="wl-move-heading">Add {symbol} to…</span>
            {availableMoveTargets.map((target) => (
              <button
                key={`row-move-${symbol}-${target.id}`}
                type="button"
                className="wl-move-item"
                onClick={() => handleQuickMove(symbol, target.id)}
              >
                <span className="wl-swatch-mini" style={{ background: target.color }} aria-hidden="true" />
                <span className="wl-move-item-label">{target.name}</span>
              </button>
            ))}
            <button
              type="button"
              className="wl-move-item wl-move-item--danger"
              onClick={() => {
                if (activeWatchlist) onRemoveFromWatchlist(activeWatchlist.id, symbol);
                setMovePopoverFor(null);
                setMovePopoverAnchor(null);
              }}
            >
              <Trash2 size={12} />
              <span className="wl-move-item-label">Remove from this watchlist</span>
            </button>
          </>
        )}
      </div>,
      document.body,
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
              {onImportSymbols ? (
                <button
                  type="button"
                  className="st-btn"
                  onClick={() => {
                    setImportMessage(null);
                    setImportText("");
                    setImportOpen(true);
                  }}
                  title={`Paste tickers (NSE:RELIANCE, BSE:RELIANCE, RELIANCE) to add into ${activeWatchlist.name}`}
                >
                  Import
                </button>
              ) : null}
              <button
                type="button"
                className="st-btn wl-grid-btn"
                onClick={() => setGridOpen(true)}
                disabled={activeItems.length === 0}
                title="See every stock in this watchlist as side-by-side charts"
              >
                <LayoutGrid size={14} />
                Grid View
              </button>
              <button
                type="button"
                className="news-trigger-btn"
                onClick={() => openNewsForWatchlist(activeWatchlist)}
                disabled={activeWatchlist.symbols.length === 0}
                title={activeWatchlist.symbols.length === 0 ? "Add stocks to see news" : `Open news for ${activeWatchlist.name}`}
              >
                <Newspaper size={14} strokeWidth={2.2} aria-hidden="true" /> News
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
              <span className="wl-arrange-spacer" aria-hidden="true" />
              <div className="st-seg" role="tablist" aria-label="Arrange watchlist">
                <button
                  type="button"
                  className={`st-seg-btn${arrangementMode === "flat" ? " is-active" : ""}`}
                  onClick={() => setArrangementMode("flat")}
                  title="Flat list (sorted by RS rating)"
                >
                  Flat
                </button>
                <button
                  type="button"
                  className={`st-seg-btn${arrangementMode === "group" ? " is-active" : ""}`}
                  onClick={() => setArrangementMode("group")}
                  disabled={!groupsData}
                  title="Bucket by industry group, ordered by group rank (#1 first)"
                >
                  By Group
                </button>
              </div>
            </div>

            <div className="scan-table">
              <div className="scan-table-head wl-head" style={headTemplate}>
                <span>Symbol</span>
                <span className="wl-center">Stage</span>
                <span className="wl-right">Price</span>
                <span className="wl-right">Change</span>
                <span className="wl-center">RS</span>
                <span className="wl-center">Group Rank</span>
                <span className="wl-center">Rank in Group</span>
                <span></span>
              </div>
              <div
                ref={arrangementMode === "flat" && shouldVirtualize ? containerRef : undefined}
                className={
                  arrangementMode === "flat" && shouldVirtualize
                    ? "scan-table-body scan-table-body-virtual"
                    : "scan-table-body"
                }
              >
                {activeItems.length === 0 ? (
                  <div className="empty-state">This watchlist is empty. Add stocks from scanners, groups, or the chart.</div>
                ) : arrangementMode === "group" ? (
                  <>
                    {groupedView.ordered.map((bucket, idx) => (
                      <div key={`gh:${bucket.groupId}`} className="wl-group-section">
                        <div className={idx === 0 ? "wl-group-header is-first" : "wl-group-header"}>
                          <strong>
                            {bucket.groupName} <span className="wl-group-rank">#{bucket.rank}</span>
                          </strong>
                          <small>
                            {bucket.items.length} stock{bucket.items.length === 1 ? "" : "s"} from this watchlist
                          </small>
                        </div>
                        {bucket.items.map((item) => renderWatchlistRow(item))}
                      </div>
                    ))}
                    {groupedView.ungrouped.length > 0 ? (
                      <div className="wl-group-section">
                        <div className={groupedView.ordered.length === 0 ? "wl-group-header is-first" : "wl-group-header"}>
                          <strong>Ungrouped</strong>
                          <small>
                            {groupedView.ungrouped.length} stock{groupedView.ungrouped.length === 1 ? "" : "s"} without a resolved industry group
                          </small>
                        </div>
                        {groupedView.ungrouped.map((item) => renderWatchlistRow(item))}
                      </div>
                    ) : null}
                  </>
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
            groupSections={gridGroupSections}
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
            onAddToWatchlist={onRequestAddToWatchlist}
            onClose={() => setGridOpen(false)}
          />
        </Suspense>
      ) : null}
      {renderMovePopover()}
      {importOpen && activeWatchlist ? (
        <div
          className="watchlist-import-overlay"
          onClick={(event) => {
            if (event.target === event.currentTarget) {
              setImportOpen(false);
            }
          }}
        >
          <div className="watchlist-import-modal" onClick={(event) => event.stopPropagation()}>
            <header>
              <strong>Import to "{activeWatchlist.name}"</strong>
              <button
                type="button"
                className="st-btn"
                onClick={() => setImportOpen(false)}
                aria-label="Close import"
              >
                x
              </button>
            </header>
            <p className="watchlist-import-hint">
              Paste tickers separated by commas, spaces, or newlines. Accepts <code>NSE:RELIANCE</code>,{" "}
              <code>BSE:RELIANCE</code>, <code>NSE/BSE:RELIANCE</code>, <code>RELIANCE-EQ</code>, or just <code>RELIANCE</code>.
            </p>
            <textarea
              value={importText}
              onChange={(event) => setImportText(event.target.value)}
              placeholder={"NSE:RELIANCE, NSE:TCS\nBSE:HDFCBANK\nINFY"}
              rows={8}
              autoFocus
            />
            {importMessage ? <div className="watchlist-import-message">{importMessage}</div> : null}
            <footer>
              <button
                type="button"
                className="st-btn"
                onClick={() => setImportOpen(false)}
              >
                Cancel
              </button>
              <button
                type="button"
                className="st-btn primary"
                onClick={() => {
                  if (!onImportSymbols || !activeWatchlist) return;
                  const result = onImportSymbols(activeWatchlist.id, importText);
                  if (result.added === 0 && result.duplicates === 0) {
                    setImportMessage("No valid tickers found in the pasted text.");
                    return;
                  }
                  setImportMessage(
                    `Added ${result.added} ticker${result.added === 1 ? "" : "s"}` +
                      (result.duplicates > 0
                        ? `, skipped ${result.duplicates} already in this watchlist.`
                        : "."),
                  );
                  setImportText("");
                }}
              >
                Add to watchlist
              </button>
            </footer>
          </div>
        </div>
      ) : null}
      <NewsModal
        isOpen={newsOpen}
        onClose={() => setNewsOpen(false)}
        title={newsContext?.title ?? "News"}
        symbols={newsContext?.symbols ?? []}
        market={market}
        accentColor={newsContext?.accent}
      />
    </div>
  );
}
