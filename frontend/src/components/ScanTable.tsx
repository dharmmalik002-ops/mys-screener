import {
  Suspense,
  lazy,
  type CSSProperties,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  Table2,
  LayoutGrid,
  LineChart as LineChartIcon,
  ArrowUpDown,
  Columns3,
  Download,
  Eye,
  EyeOff,
  Plus,
  ChevronDown,
  Search as SearchIcon,
  SearchX,
} from "lucide-react";

import {
  getChartGridSeries,
  type ChartBar,
  type ChartGridTimeframe,
  type IndustryGroupsResponse,
  type MarketKey,
  type ScanDescriptor,
  type ScanMatch,
  type ScanSectorSummary,
} from "../lib/api";
import { useMinWidth, useVirtualRows } from "../lib/virtualRows";
import type {
  ChartGridChartStyle,
  ChartGridDisplayCard,
  ChartGridDisplayMode,
  ChartGridGroupSection,
  ChartGridSortBy,
  ChartGridStat,
} from "./ChartGridModal";
import { EmptyState } from "./EmptyState";
import { Panel } from "./Panel";
import { SortableHeader } from "./SortableTh";

import "./ScanTable.css";

const ChartGridModal = lazy(() =>
  import("./ChartGridModal").then((module) => ({ default: module.ChartGridModal })),
);
const SCAN_SLOT_GAP = 6;
const SCAN_ROW_SLOT_HEIGHT = 76;
const SCAN_HEADER_SLOT_HEIGHT = 64;

type ScanTableEntry =
  | {
      key: string;
      type: "header";
      sector: string;
      accent: string;
      summary: ScanSectorSummary | undefined;
      subtitle?: string;
      count: number;
      isFirst: boolean;
    }
  | {
      key: string;
      type: "row";
      item: ScanMatch;
    };

type ViewMode = "table" | "grid" | "chart";

type SortBy =
  | "change_desc"
  | "change_asc"
  | "rs_desc"
  | "rs_asc"
  | "rvol_desc"
  | "price_desc"
  | "price_asc"
  | "mcap_desc"
  | "listing_desc"
  | "listing_asc"
  | "volume_date_desc"
  | "expansion_date_desc"
  | "earnings_date_desc";

type ColumnKey = "spark" | "rs" | "rs1m" | "rvol" | "vdate" | "sdate" | "edate" | "gap";

/**
 * Which SortBy modes each header column maps to. Direction is encoded in the
 * key here (change_desc / change_asc), so a column with no `asc` variant simply
 * stays descending when re-clicked rather than inventing a mode.
 */
const COLUMN_SORTS = {
  price: { desc: "price_desc", asc: "price_asc" },
  change: { desc: "change_desc", asc: "change_asc" },
  rs: { desc: "rs_desc", asc: "rs_asc" },
  rvol: { desc: "rvol_desc" },
  vdate: { desc: "volume_date_desc" },
  edate: { desc: "earnings_date_desc" },
} as const satisfies Record<string, { desc: SortBy; asc?: SortBy }>;

type SortColumn = keyof typeof COLUMN_SORTS;

function isColumnActive(column: SortColumn, sortBy: SortBy): boolean {
  const pair = COLUMN_SORTS[column] as { desc: SortBy; asc?: SortBy };
  return sortBy === pair.desc || sortBy === pair.asc;
}

function nextColumnSort(column: SortColumn, sortBy: SortBy): SortBy {
  const pair = COLUMN_SORTS[column] as { desc: SortBy; asc?: SortBy };
  if (sortBy === pair.desc && pair.asc) return pair.asc;
  return pair.desc;
}

const SORT_OPTIONS: Array<{ value: SortBy; label: string }> = [
  { value: "change_desc", label: "Change % (high → low)" },
  { value: "change_asc", label: "Change % (low → high)" },
  { value: "rs_desc", label: "RS Rating (high → low)" },
  { value: "rs_asc", label: "RS Rating (low → high)" },
  { value: "rvol_desc", label: "Relative Volume" },
  { value: "price_desc", label: "Price (high → low)" },
  { value: "price_asc", label: "Price (low → high)" },
  { value: "mcap_desc", label: "Market Cap" },
  { value: "listing_desc", label: "IPO Debut (newest first)" },
  { value: "listing_asc", label: "IPO Debut (oldest first)" },
];

const COLUMN_DEFS: Array<{ key: ColumnKey; label: string }> = [
  { key: "spark", label: "Sparkline" },
  { key: "rs", label: "RS Rating" },
  { key: "rs1m", label: "RS 1M Ago" },
  { key: "rvol", label: "Rel Volume" },
  { key: "gap", label: "Gap %" },
];

/* ---------- Logo helpers (mirrors GroupsPanel) ---------- */
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

/* ---------- Inline sparkline (synthesized from change/3M/1Y) ---------- */
function MiniSpark({ item, color }: { item: ScanMatch; color: string }) {
  // Synthesize a 7-point trajectory using available return windows so each
  // ticker has a deterministic shape that reflects its momentum.
  const r12m = item.stock_return_12m ?? 0;
  const r3m = item.stock_return_60d ?? r12m / 4;
  const r1m = item.stock_return_20d ?? r3m / 3;
  const rDay = Number.isFinite(item.change_pct) ? item.change_pct : 0;

  // Build 7 normalized points (0=baseline 100, then weighted blend of windows).
  const pts: number[] = [
    100,
    100 + r12m * 0.15,
    100 + r12m * 0.4,
    100 + r12m * 0.65,
    100 + r12m * 0.85 + r3m * 0.2,
    100 + r12m * 0.95 + r3m * 0.6 + r1m * 0.4,
    100 + r12m + r3m * 0.7 + r1m * 0.9 + rDay * 0.5,
  ];

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
    <svg viewBox={`0 0 ${W} ${H}`} className="st-spark-svg" aria-hidden>
      <path d={path} stroke={color} strokeWidth={1.6} fill="none" strokeLinecap="round" />
    </svg>
  );
}

/* ---------- RS rating badge ---------- */
function RsBadge({ rs }: { rs: number | null | undefined }) {
  if (rs === null || rs === undefined || !Number.isFinite(rs)) {
    return <span className="st-rs-badge st-rs-muted">—</span>;
  }
  const tone = rs >= 80 ? "hi" : rs >= 60 ? "mid" : "lo";
  return <span className={`st-rs-badge st-rs-${tone}`}>{Math.round(rs)}</span>;
}

/* ---------- Existing helpers (kept) ---------- */
function formatListingDate(value: string | null | undefined) {
  if (!value) return null;
  const [year, month, day] = value.split("-");
  if (!year || !month || !day) return value;
  return `${day}-${month}-${year}`;
}

function formatPrice(value: number, market: MarketKey) {
  void market;
  return `₹${value.toLocaleString("en-IN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function sectorAccentColor(label: string): string {
  const palette = [
    "#5dd6a2",
    "#58a6ff",
    "#f7b955",
    "#ff8a65",
    "#c792ea",
    "#5eead4",
    "#f472b6",
    "#a3e635",
  ];
  let hash = 0;
  for (const character of label) {
    hash = (hash * 31 + character.charCodeAt(0)) >>> 0;
  }
  return palette[hash % palette.length];
}

function applySort(items: ScanMatch[], sortBy: SortBy): ScanMatch[] {
  const sorted = [...items];
  sorted.sort((left, right) => {
    switch (sortBy) {
      case "change_desc":
        return right.change_pct - left.change_pct;
      case "change_asc":
        return left.change_pct - right.change_pct;
      case "rs_desc":
        return (right.rs_rating ?? -Infinity) - (left.rs_rating ?? -Infinity);
      case "rs_asc":
        return (left.rs_rating ?? Infinity) - (right.rs_rating ?? Infinity);
      case "rvol_desc":
        return (right.relative_volume ?? 0) - (left.relative_volume ?? 0);
      case "price_desc":
        return right.last_price - left.last_price;
      case "price_asc":
        return left.last_price - right.last_price;
      case "mcap_desc":
        return (right.market_cap_crore ?? 0) - (left.market_cap_crore ?? 0);
      case "listing_desc": {
        const lt = listingTimestamp(left.listing_date);
        const rt = listingTimestamp(right.listing_date);
        return rt - lt;
      }
      case "listing_asc": {
        const lt = listingTimestamp(left.listing_date);
        const rt = listingTimestamp(right.listing_date);
        return lt - rt;
      }
      case "volume_date_desc": {
        // Newest high-volume push first; tie-break by score (recency→tier→breakout).
        const lt = listingTimestamp(left.volume_push_date);
        const rt = listingTimestamp(right.volume_push_date);
        if (rt !== lt) return rt - lt;
        return right.score - left.score;
      }
      case "expansion_date_desc": {
        // Newest Expansion trigger first; tie-break by score within the same session.
        const lt = listingTimestamp(left.session_date);
        const rt = listingTimestamp(right.session_date);
        if (rt !== lt) return rt - lt;
        return right.score - left.score;
      }
      case "earnings_date_desc": {
        // Latest result at the top; tie-break by grade/score within a day.
        const lt = listingTimestamp(left.earnings_date);
        const rt = listingTimestamp(right.earnings_date);
        if (rt !== lt) return rt - lt;
        return right.score - left.score;
      }
    }
  });
  return sorted;
}

function listingTimestamp(value: string | null | undefined): number {
  if (!value) return 0;
  const t = Date.parse(value);
  return Number.isFinite(t) ? t : 0;
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
  if (!summary) return "";
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
  if (timeframe === "3M") return item.stock_return_60d ?? item.stock_return_20d ?? item.change_pct;
  if (timeframe === "6M") return item.stock_return_12m ?? item.stock_return_60d ?? item.change_pct;
  if (timeframe === "1Y") return item.stock_return_12m ?? item.stock_return_60d ?? item.change_pct;
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

// Volume screener tier badges. The backend's first reason encodes the longest
// window the volume push cleared; map it to a short code + color for display
// on the RVOL (volume-increase) cell.
const VOLUME_TIER_BADGES: Record<string, { code: string; title: string; color: string }> = {
  "Monthly volume high": { code: "HMV", title: "Highest Monthly Volume", color: "#dc2626" },
  "Quarterly volume high": { code: "HQV", title: "Highest Quarterly Volume", color: "#16a34a" },
  "Half-yearly volume high": { code: "HHV", title: "Highest Half-yearly Volume", color: "#2563eb" },
  "Yearly volume high": { code: "HYV", title: "Highest Yearly Volume", color: "#111827" },
};

function formatVolumeDate(value: string | null | undefined): string {
  if (!value) return "";
  const dt = new Date(value);
  if (isNaN(dt.getTime())) return value;
  return dt.toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "2-digit" });
}

function volumeTierBadge(item: ScanMatch): { code: string; title: string; color: string } | null {
  const first = item.reasons?.[0];
  return first ? VOLUME_TIER_BADGES[first] ?? null : null;
}

function scanRowSubtitle(item: ScanMatch) {
  const listingDate = formatListingDate(item.listing_date);
  const categoryLabel =
    item.sub_sector && item.sub_sector !== item.sector ? item.sub_sector : item.sector;
  const baseLabel =
    item.gap_pct !== null && item.gap_pct !== undefined
      ? `${categoryLabel} · Gap ${item.gap_pct.toFixed(2)}%`
      : categoryLabel;
  return listingDate ? `${baseLabel} · Listed ${listingDate}` : baseLabel;
}

function scanEntryHeight(entry: ScanTableEntry) {
  return entry.type === "header" ? SCAN_HEADER_SLOT_HEIGHT : SCAN_ROW_SLOT_HEIGHT;
}

type ArrangementMode = "flat" | "sector" | "group";

type ScanTableProps = {
  market: MarketKey;
  loading: boolean;
  sectorSummaryLoading: boolean;
  scan: ScanDescriptor | null;
  items: ScanMatch[];
  sectorSummaries: ScanSectorSummary[];
  onPickSymbol: (symbol: string) => void;
  // Optional best-effort chart prewarm on row hover (no-op if omitted).
  onPrefetchSymbol?: (symbol: string) => void;
  onRequestAddToWatchlist: (symbol: string) => void;
  selectedSymbol: string | null;
  sortMode: "change" | "rs";
  onSortModeChange: (mode: "change" | "rs") => void;
  arrangementMode: ArrangementMode;
  onArrangementModeChange: (mode: ArrangementMode) => void;
  sectorSortMode: "1W" | "1M" | "count-desc" | "count-asc";
  onSectorSortModeChange: (mode: "1W" | "1M" | "count-desc" | "count-asc") => void;
  groupsData?: IndustryGroupsResponse | null;
  onExport: () => void;
  // Reports the exact top-to-bottom order of the currently displayed rows
  // (after search filter + sort + sector/group arrangement) so the parent's
  // ArrowUp/ArrowDown chart navigation walks stocks in the SAME order the user
  // sees — not the parent's flat pre-arrangement list.
  onVisibleOrderChange?: (symbols: string[]) => void;
};

export function ScanTable({
  market,
  loading,
  sectorSummaryLoading,
  scan,
  items,
  sectorSummaries,
  onPickSymbol,
  onPrefetchSymbol,
  onRequestAddToWatchlist,
  selectedSymbol,
  sortMode,
  onSortModeChange,
  arrangementMode,
  onArrangementModeChange,
  sectorSortMode,
  onSectorSortModeChange,
  groupsData = null,
  onExport,
  onVisibleOrderChange,
}: ScanTableProps) {
  /* ----- Refs ----- */
  const rowRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const sortMenuRef = useRef<HTMLDivElement | null>(null);
  const colsMenuRef = useRef<HTMLDivElement | null>(null);

  /* ----- Existing grid-modal state (kept) ----- */
  const [gridOpen, setGridOpen] = useState(false);
  // Defaults per the owner's spec: big side-by-side charts — 2 per row, one
  // row filling the screen, 3 months of daily bars.
  const [gridColumns, setGridColumns] = useState(2);
  const [gridRows, setGridRows] = useState(1);
  const [gridTimeframe, setGridTimeframe] = useState<ChartGridTimeframe>("6M");
  const [gridSortBy, setGridSortBy] = useState<ChartGridSortBy>("rs_rating");
  const [gridChartStyle, setGridChartStyle] = useState<ChartGridChartStyle>("candles");
  const [gridDisplayMode, setGridDisplayMode] = useState<ChartGridDisplayMode>("normal");

  /* ----- Stage 4 new state ----- */
  const [viewMode, setViewMode] = useState<ViewMode>("table");
  const [sortBy, setSortBy] = useState<SortBy>(
    scan?.id === "volume"
      ? "volume_date_desc"
      : scan?.id === "ema-expansion"
        ? "expansion_date_desc"
        : scan?.id === "positive-earnings"
          ? "earnings_date_desc"
          : sortMode === "rs"
            ? "rs_desc"
            : "change_desc",
  );
  // Date-tracked scanners are meant to read newest signal first, so force
  // their date sort whenever that scanner is (re)opened.
  useEffect(() => {
    if (scan?.id === "volume") setSortBy("volume_date_desc");
    if (scan?.id === "ema-expansion") setSortBy("expansion_date_desc");
    if (scan?.id === "positive-earnings") setSortBy("earnings_date_desc");
  }, [scan?.id]);
  const [sortMenuOpen, setSortMenuOpen] = useState(false);
  const [colsMenuOpen, setColsMenuOpen] = useState(false);
  // Symbol-filter applied to the scan result list itself ("does Screener A's
  // 200 hits contain ABCD?"). Empty = show everything. Matches against symbol
  // OR company name, case-insensitive.
  const [symbolFilter, setSymbolFilter] = useState("");

  // Reset the symbol filter whenever the underlying result set changes (a
  // different scanner ran), so the input doesn't silently hide rows from the
  // next scan.
  useEffect(() => {
    setSymbolFilter("");
  }, [scan?.id]);
  const [visibleCols, setVisibleCols] = useState<Set<ColumnKey>>(
    () =>
      new Set<ColumnKey>(
        scan?.id === "volume"
          ? ["spark", "rs", "rvol", "vdate"]
          : scan?.id === "ema-expansion"
            ? ["spark", "rs", "rvol", "sdate"]
            : scan?.id === "positive-earnings"
              ? ["spark", "rs", "rvol", "edate"]
              : ["spark", "rs", "rvol"],
      ),
  );
  // Mode-specific date columns: high-volume date on the volume screener,
  // expansion session date on the rolling Expansion tracker.
  useEffect(() => {
    setVisibleCols((current) => {
      const next = new Set(current);
      if (scan?.id === "volume") next.add("vdate");
      else next.delete("vdate");
      if (scan?.id === "ema-expansion") next.add("sdate");
      else next.delete("sdate");
      if (scan?.id === "positive-earnings") next.add("edate");
      else next.delete("edate");
      return next;
    });
  }, [scan?.id]);

  /* Keep parent's sortMode in sync with the new sortBy when feasible */
  useEffect(() => {
    if (sortBy === "change_desc" || sortBy === "change_asc") onSortModeChange("change");
    else if (sortBy === "rs_desc" || sortBy === "rs_asc") onSortModeChange("rs");
  }, [sortBy, onSortModeChange]);

  /* Click-outside for popovers */
  useEffect(() => {
    if (!sortMenuOpen && !colsMenuOpen) return;
    const handler = (e: MouseEvent) => {
      const t = e.target as Node;
      if (sortMenuRef.current && !sortMenuRef.current.contains(t)) setSortMenuOpen(false);
      if (colsMenuRef.current && !colsMenuRef.current.contains(t)) setColsMenuOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [sortMenuOpen, colsMenuOpen]);

  /* ----- Layout ----- */
  const hasWideTableLayout = useMinWidth(1180);
  const showSortToggle = scan?.id !== "custom-scan";

  const summaryBySector = useMemo(
    () => Object.fromEntries(sectorSummaries.map((summary) => [summary.sector, summary])),
    [sectorSummaries],
  );

  // Pre-filter step: narrow the scan items to whatever matches the user's
  // symbol/company-name typed query. Then the existing sort/group/sector
  // pipeline operates on the filtered subset.
  const filteredItems = useMemo(() => {
    const q = symbolFilter.trim().toLowerCase();
    if (!q) return items;
    return items.filter((m) => {
      const sym = (m.symbol ?? "").toLowerCase();
      const name = (m.name ?? "").toLowerCase();
      return sym.includes(q) || name.includes(q);
    });
  }, [items, symbolFilter]);

  const sortedItems = useMemo(
    () => (showSortToggle ? applySort(filteredItems, sortBy) : filteredItems),
    [filteredItems, showSortToggle, sortBy],
  );

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

  // Group bucketing: resolve each item's industry group via groupsData and
  // sort buckets by rank ascending (rank #1 = top of list).
  const groupContextBySymbol = useMemo(() => {
    const map = new Map<string, { groupId: string; groupName: string; rank: number; totalGroups: number }>();
    if (!groupsData) return map;
    const total = groupsData.total_groups || groupsData.groups.length;
    const rankByGroupId = new Map<string, { rank: number; name: string }>();
    for (const g of groupsData.groups) {
      rankByGroupId.set(g.group_id, { rank: g.rank, name: g.group_name });
    }
    for (const stock of groupsData.stocks) {
      const meta = rankByGroupId.get(stock.final_group_id);
      if (!meta) continue;
      map.set(stock.symbol.toUpperCase(), {
        groupId: stock.final_group_id,
        groupName: meta.name,
        rank: meta.rank,
        totalGroups: total,
      });
    }
    return map;
  }, [groupsData]);

  const groupBuckets = useMemo(() => {
    const grouped = new Map<string, { rank: number; name: string; total: number; items: ScanMatch[] }>();
    const unranked: ScanMatch[] = [];
    for (const item of sortedItems) {
      const ctx = groupContextBySymbol.get(item.symbol.toUpperCase());
      if (!ctx) {
        unranked.push(item);
        continue;
      }
      const bucket = grouped.get(ctx.groupId) ?? { rank: ctx.rank, name: ctx.groupName, total: ctx.totalGroups, items: [] };
      bucket.items.push(item);
      grouped.set(ctx.groupId, bucket);
    }
    const ordered = Array.from(grouped.entries()).sort((a, b) => a[1].rank - b[1].rank);
    return { ordered, unranked };
  }, [groupContextBySymbol, sortedItems]);

  const tableEntries = useMemo<ScanTableEntry[]>(() => {
    if (arrangementMode === "group" && groupsData) {
      const entries: ScanTableEntry[] = [];
      groupBuckets.ordered.forEach(([groupId, bucket], index) => {
        entries.push({
          key: `gheader:${groupId}`,
          type: "header",
          sector: `${bucket.name} · #${bucket.rank}`,
          accent: sectorAccentColor(bucket.name),
          summary: undefined,
          subtitle: `Group rank ${bucket.rank} of ${bucket.total} · ${bucket.items.length} stock${bucket.items.length === 1 ? "" : "s"} in this scan`,
          count: bucket.items.length,
          isFirst: index === 0,
        });
        for (const item of bucket.items) {
          entries.push({
            key: `row:${item.scan_id}:${item.symbol}`,
            type: "row",
            item,
          });
        }
      });
      if (groupBuckets.unranked.length > 0) {
        entries.push({
          key: "gheader:unranked",
          type: "header",
          sector: "Ungrouped",
          accent: "#94a3b8",
          summary: undefined,
          subtitle: `${groupBuckets.unranked.length} stock${groupBuckets.unranked.length === 1 ? "" : "s"} without a resolved group`,
          count: groupBuckets.unranked.length,
          isFirst: groupBuckets.ordered.length === 0,
        });
        for (const item of groupBuckets.unranked) {
          entries.push({
            key: `row:${item.scan_id}:${item.symbol}`,
            type: "row",
            item,
          });
        }
      }
      return entries;
    }
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
      const rows = sectorItems.map(
        (item) =>
          ({
            key: `row:${item.scan_id}:${item.symbol}`,
            type: "row",
            item,
          }) satisfies ScanTableEntry,
      );
      return [header, ...rows];
    });
  }, [arrangementMode, groupBuckets, groupsData, sectorGroups, sortedItems, summaryBySector]);

  // Lift the displayed row order to the parent for keyboard chart navigation.
  // Keyed on the joined symbol string so we only notify when the sequence
  // actually changes (not on every unrelated re-render), avoiding update loops.
  const visibleRowSymbols = useMemo(
    () =>
      tableEntries
        .filter((entry): entry is Extract<ScanTableEntry, { type: "row" }> => entry.type === "row")
        .map((entry) => entry.item.symbol),
    [tableEntries],
  );
  const visibleOrderKey = visibleRowSymbols.join("|");
  useEffect(() => {
    onVisibleOrderChange?.(visibleRowSymbols);
    // visibleRowSymbols is derived from visibleOrderKey; depending on the key
    // alone keeps this to one call per real order change.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [visibleOrderKey]);

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
    const selectedEntry = tableEntries.find(
      (entry) => entry.type === "row" && entry.item.symbol === selectedSymbol,
    );
    const selectedEntryKey = selectedEntry?.key ?? null;
    if (!selectedEntryKey) return;
    if (lastAutoScrolledEntryKeyRef.current === selectedEntryKey) return;
    lastAutoScrolledEntryKeyRef.current = selectedEntryKey;
    if (shouldVirtualize) {
      scrollToKey(selectedEntryKey);
      return;
    }
    const activeRow = rowRefs.current[selectedSymbol];
    activeRow?.scrollIntoView({ block: "nearest", inline: "nearest" });
  }, [
    arrangementMode,
    scrollToKey,
    sectorSortMode,
    selectedSymbol,
    shouldVirtualize,
    sortBy,
    sortedItems,
    tableEntries,
  ]);

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
          subtitle: `${group.parent_sector} · base rank #${group.rank} · ${sectionCards.length} stock${sectionCards.length === 1 ? "" : "s"}`,
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
        subtitle: `${ungrouped.length} stock${ungrouped.length === 1 ? "" : "s"} without a resolved industry group`,
        baseRank: 9999,
        stockCount: ungrouped.length,
        returns: { "1W": -999, "1M": -999, "3M": -999, "6M": -999 },
        cards: ungrouped,
      });
    }

    return sections;
  }, [gridCards, groupsData]);

  const gridStats = useMemo<ChartGridStat[]>(() => {
    const advancing = sortedItems.filter((item) => item.change_pct > 0).length;
    const declining = sortedItems.filter((item) => item.change_pct < 0).length;
    const topSector = sortedItems[0]?.sector ?? "--";
    return [
      { label: "Stocks", value: `${sortedItems.length}` },
      {
        label: "Advancing",
        value: `${advancing}`,
        tone: advancing >= declining ? "positive" : "neutral",
      },
      {
        label: "Declining",
        value: `${declining}`,
        tone: declining > advancing ? "negative" : "neutral",
      },
      { label: "Top Sector", value: topSector },
    ];
  }, [sortedItems]);

  async function loadGridSeries(
    symbols: string[],
    timeframe: ChartGridTimeframe,
  ): Promise<Record<string, ChartBar[]>> {
    const payload = await getChartGridSeries(symbols, timeframe, market);
    return payload.items.reduce<Record<string, ChartBar[]>>((accumulator, item) => {
      accumulator[item.symbol] = item.bars;
      return accumulator;
    }, {});
  }

  /* Reactive grid template based on column visibility.
     Tight column widths so the Stock column keeps enough room in the
     narrow middle of the screener page (sidebar | main | chart layout). */
  const gridTemplate = useMemo(() => {
    const cols: string[] = ["minmax(160px, 1.8fr)"]; // Stock + logo (flex-grow)
    cols.push("72px"); // Price
    cols.push("64px"); // Change %
    if (visibleCols.has("spark")) cols.push("56px");
    if (visibleCols.has("rs")) cols.push("44px");
    if (visibleCols.has("rs1m")) cols.push("44px");
    if (visibleCols.has("rvol")) cols.push("48px");
    if (visibleCols.has("vdate")) cols.push("82px");
    if (visibleCols.has("sdate")) cols.push("82px");
    if (visibleCols.has("edate")) cols.push("82px");
    if (visibleCols.has("gap")) cols.push("52px");
    cols.push("32px"); // Watch
    return cols.join(" ");
  }, [visibleCols]);

  const handleViewMode = (mode: ViewMode) => {
    setViewMode(mode);
    if (mode === "grid") {
      setGridOpen(true);
    } else if (mode === "chart") {
      // Ensure first item is selected so chart panel shows something useful
      const first = sortedItems[0];
      if (first) onPickSymbol(first.symbol);
    }
  };

  const renderEntry = (entry: ScanTableEntry, virtualHeight?: number) => {
    if (entry.type === "header") {
      return (
        <div
          key={entry.key}
          className={
            entry.isFirst ? "scan-sector-header scan-sector-header-first" : "scan-sector-header"
          }
          style={
            {
              "--sector-accent": entry.accent,
              ...(virtualHeight ? { height: `${virtualHeight}px` } : {}),
            } as CSSProperties
          }
        >
          <div>
            <strong>
              {entry.sector} ({entry.count})
            </strong>
            <small>{entry.subtitle ?? formatSectorLine(entry.summary, sectorSortMode)}</small>
          </div>
        </div>
      );
    }

    const { item } = entry;
    const logoUrl = getLogoUrl(item.symbol);
    const up = item.change_pct >= 0;
    const sparkColor = up ? "#10b981" : "#ef4444";
    const isActive = selectedSymbol === item.symbol;
    const volBadge = scan?.id === "volume" ? volumeTierBadge(item) : null;

    return (
      <div
        key={entry.key}
        className={`scan-row st-row${isActive ? " active" : ""}`}
        ref={
          shouldVirtualize
            ? undefined
            : (element) => {
                rowRefs.current[item.symbol] = element;
              }
        }
        style={
          {
            "--st-grid": gridTemplate,
            ...(virtualHeight ? { height: `${virtualHeight}px` } : {}),
          } as CSSProperties
        }
      >
        <button
          type="button"
          className="scan-row-main st-row-main"
          onMouseEnter={onPrefetchSymbol ? () => onPrefetchSymbol(item.symbol) : undefined}
          onFocus={onPrefetchSymbol ? () => onPrefetchSymbol(item.symbol) : undefined}
          onClick={() => onPickSymbol(item.symbol)}
        >
          <span className="st-logo">
            {logoUrl ? (
              <img
                src={logoUrl}
                alt=""
                onError={(e) => {
                  e.currentTarget.style.display = "none";
                }}
              />
            ) : (
              <span className="st-logo-fallback">{initials(item.symbol)}</span>
            )}
          </span>
          <span className="st-name">
            <strong>
              {item.symbol}
              {volBadge ? (
                <span
                  title={volBadge.title}
                  style={{
                    display: "inline-block",
                    marginLeft: 6,
                    fontSize: 9.5,
                    fontWeight: 700,
                    letterSpacing: "0.04em",
                    lineHeight: "14px",
                    color: "#fff",
                    background: volBadge.color,
                    border: "1px solid rgba(255,255,255,0.4)",
                    borderRadius: 4,
                    padding: "0 5px",
                    verticalAlign: "middle",
                  }}
                >
                  {volBadge.code}
                </span>
              ) : null}
              {item.new_since_prev ? (
                <span className="st-new-chip" title="Entered this scanner today (was not in the previous session's results)">
                  NEW
                </span>
              ) : null}
            </strong>
            <small>
              {scanRowSubtitle(item)}
              {item.also_in && item.also_in.length > 0 ? (
                <span className="st-confluence" title={`Also flagged today by: ${item.also_in.join(", ")}`}>
                  {" · also in "}
                  {item.also_in.join(", ")}
                </span>
              ) : null}
            </small>
          </span>
        </button>

        <span className="st-price">{formatPrice(item.last_price, market)}</span>

        <span className={`st-change ${up ? "positive-text" : "negative-text"}`}>
          {item.change_pct >= 0 ? "+" : ""}
          {item.change_pct.toFixed(2)}%
        </span>

        {visibleCols.has("spark") ? (
          <span className="st-spark">
            <MiniSpark item={item} color={sparkColor} />
          </span>
        ) : null}

        {visibleCols.has("rs") ? (
          <span className="st-cell-center">
            <RsBadge rs={item.rs_rating} />
          </span>
        ) : null}

        {visibleCols.has("rs1m") ? (
          <span className="st-cell-center">
            <RsBadge rs={item.rs_rating_1m_ago} />
          </span>
        ) : null}

        {visibleCols.has("rvol") ? (
          <span className="st-cell-center st-rvol" style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 1, lineHeight: 1.1 }}>
            {volBadge ? (
              <span
                title={volBadge.title}
                style={{
                  fontSize: 8.5,
                  fontWeight: 700,
                  letterSpacing: "0.03em",
                  color: "#fff",
                  background: volBadge.color,
                  border: "1px solid rgba(255,255,255,0.4)",
                  borderRadius: 3,
                  padding: "0 3px",
                }}
              >
                {volBadge.code}
              </span>
            ) : null}
            <span>{item.relative_volume.toFixed(2)}×</span>
          </span>
        ) : null}

        {visibleCols.has("vdate") ? (
          <span className="st-cell-center st-vdate" title={item.volume_push_date ? `High-volume push on ${item.volume_push_date}` : undefined}>
            {item.volume_push_date ? formatVolumeDate(item.volume_push_date) : "—"}
          </span>
        ) : null}

        {visibleCols.has("sdate") ? (
          <span className="st-cell-center st-vdate" title={item.session_date ? `Showed expansion on ${item.session_date}` : undefined}>
            {item.session_date ? formatVolumeDate(item.session_date) : "—"}
          </span>
        ) : null}

        {visibleCols.has("edate") ? (
          <span className="st-cell-center st-vdate" title={item.earnings_date ? `Result announced on ${item.earnings_date}` : undefined}>
            {item.earnings_date ? formatVolumeDate(item.earnings_date) : "—"}
          </span>
        ) : null}

        {visibleCols.has("gap") ? (
          <span className="st-cell-center">
            {item.gap_pct !== null && item.gap_pct !== undefined
              ? `${item.gap_pct.toFixed(2)}%`
              : "—"}
          </span>
        ) : null}

        <button
          type="button"
          className="st-watch-btn"
          onClick={(e) => {
            e.stopPropagation();
            onRequestAddToWatchlist(item.symbol);
          }}
          title={`Add ${item.symbol} to watchlist`}
        >
          <Plus size={14} strokeWidth={2.4} />
        </button>
      </div>
    );
  };

  const headTemplate: CSSProperties = { "--st-grid": gridTemplate } as CSSProperties;

  /* ----- Suppress legacy showSortToggle UI; new dropdown owns sort ----- */
  void sortMode;
  void onSortModeChange;

  return (
    <Panel
      title={scan?.name ?? "Loading scan"}
      subtitle={
        scan
          ? `${scan.hit_count} matches across the filtered universe${
              sectorSummaryLoading ? " · Updating sector history…" : ""
            }`
          : "Fetching results"
      }
      actions={
        <div className="st-actions">
          {/* Symbol filter — "is stock X in the current scan results?" */}
          <div className="st-filter" style={{ position: "relative", display: "inline-flex", alignItems: "center" }}>
            <input
              type="search"
              value={symbolFilter}
              onChange={(event) => setSymbolFilter(event.target.value)}
              placeholder={`Find symbol in ${items.length} results…`}
              aria-label="Filter scan results by symbol or company name"
              className="st-filter-input"
              style={{
                height: 26,
                padding: "0 10px 0 26px",
                fontSize: 12,
                lineHeight: "24px",
                borderRadius: 6,
                border: "1px solid var(--border, #2d3340)",
                background: "var(--input-bg, rgba(13,17,23,0.6))",
                color: "inherit",
                width: 200,
                outline: "none",
              }}
            />
            <SearchIcon
              size={12}
              strokeWidth={2.2}
              style={{ position: "absolute", left: 8, opacity: 0.55, pointerEvents: "none" }}
            />
            {symbolFilter ? (
              <span
                title={`${sortedItems.length} of ${items.length} match`}
                style={{ marginLeft: 6, fontSize: 11, opacity: 0.7, whiteSpace: "nowrap" }}
              >
                {sortedItems.length}/{items.length}
              </span>
            ) : null}
          </div>

          {/* View tabs */}
          <div className="st-view-tabs" role="tablist" aria-label="Result view mode">
            <button
              type="button"
              role="tab"
              aria-selected={viewMode === "table"}
              className={`st-view-tab${viewMode === "table" ? " is-active" : ""}`}
              onClick={() => handleViewMode("table")}
              title="Table view"
            >
              <Table2 size={13} strokeWidth={2.2} />
              <span>Table</span>
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={viewMode === "grid"}
              className={`st-view-tab${viewMode === "grid" ? " is-active" : ""}`}
              onClick={() => handleViewMode("grid")}
              disabled={items.length === 0}
              title="Grid view"
            >
              <LayoutGrid size={13} strokeWidth={2.2} />
              <span>Grid</span>
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={viewMode === "chart"}
              className={`st-view-tab${viewMode === "chart" ? " is-active" : ""}`}
              onClick={() => handleViewMode("chart")}
              disabled={items.length === 0}
              title="Open first stock chart"
            >
              <LineChartIcon size={13} strokeWidth={2.2} />
              <span>Chart</span>
            </button>
          </div>

          {/* Sort By dropdown */}
          <div className="st-pop-wrap" ref={sortMenuRef}>
            <button
              type="button"
              className="st-btn"
              onClick={() => {
                setSortMenuOpen((o) => !o);
                setColsMenuOpen(false);
              }}
              title="Sort by"
            >
              <ArrowUpDown size={13} strokeWidth={2.2} />
              <span>Sort By</span>
              <ChevronDown size={12} />
            </button>
            {sortMenuOpen ? (
              <div className="st-pop">
                {(scan?.id === "volume"
                  ? [{ value: "volume_date_desc" as SortBy, label: "High-volume date (newest first)" }, ...SORT_OPTIONS]
                  : scan?.id === "ema-expansion"
                    ? [{ value: "expansion_date_desc" as SortBy, label: "Expansion date (newest first)" }, ...SORT_OPTIONS]
                  : scan?.id === "positive-earnings"
                    ? [{ value: "earnings_date_desc" as SortBy, label: "Earnings date (newest first)" }, ...SORT_OPTIONS]
                  : SORT_OPTIONS
                ).map((opt) => (
                  <button
                    key={opt.value}
                    type="button"
                    className={`st-pop-item${sortBy === opt.value ? " is-active" : ""}`}
                    onClick={() => {
                      setSortBy(opt.value);
                      setSortMenuOpen(false);
                    }}
                  >
                    {opt.label}
                  </button>
                ))}
              </div>
            ) : null}
          </div>

          {/* Columns dropdown */}
          <div className="st-pop-wrap" ref={colsMenuRef}>
            <button
              type="button"
              className="st-btn"
              onClick={() => {
                setColsMenuOpen((o) => !o);
                setSortMenuOpen(false);
              }}
              title="Choose columns"
            >
              <Columns3 size={13} strokeWidth={2.2} />
              <span>Columns</span>
              <ChevronDown size={12} />
            </button>
            {colsMenuOpen ? (
              <div className="st-pop">
                {(scan?.id === "volume"
                  ? [...COLUMN_DEFS, { key: "vdate" as ColumnKey, label: "High-Vol Date" }]
                  : scan?.id === "ema-expansion"
                    ? [...COLUMN_DEFS, { key: "sdate" as ColumnKey, label: "Expansion Day" }]
                    : scan?.id === "positive-earnings"
                      ? [...COLUMN_DEFS, { key: "edate" as ColumnKey, label: "Earnings Date" }]
                      : COLUMN_DEFS
                ).map((c) => {
                  const on = visibleCols.has(c.key);
                  return (
                    <button
                      key={c.key}
                      type="button"
                      className="st-pop-item st-pop-toggle"
                      onClick={() =>
                        setVisibleCols((current) => {
                          const next = new Set(current);
                          if (next.has(c.key)) next.delete(c.key);
                          else next.add(c.key);
                          return next;
                        })
                      }
                    >
                      {on ? <Eye size={12} /> : <EyeOff size={12} />}
                      <span>{c.label}</span>
                    </button>
                  );
                })}
              </div>
            ) : null}
          </div>

          {/* Layout (flat / sector / industry group) */}
          <div className="st-seg">
            <button
              type="button"
              className={`st-seg-btn${arrangementMode === "flat" ? " is-active" : ""}`}
              onClick={() => onArrangementModeChange("flat")}
              title="Flat list"
            >
              Flat
            </button>
            <button
              type="button"
              className={`st-seg-btn${arrangementMode === "sector" ? " is-active" : ""}`}
              onClick={() => onArrangementModeChange("sector")}
              title="Group by sector"
            >
              By Sector
            </button>
            <button
              type="button"
              className={`st-seg-btn${arrangementMode === "group" ? " is-active" : ""}`}
              onClick={() => onArrangementModeChange("group")}
              title="Group by industry group, ordered by group rank (#1 first)"
              disabled={!groupsData}
            >
              By Group
            </button>
          </div>

          {arrangementMode === "sector" ? (
            <select
              className="st-select"
              value={sectorSortMode}
              onChange={(e) =>
                onSectorSortModeChange(
                  e.target.value as "1W" | "1M" | "count-desc" | "count-asc",
                )
              }
              aria-label="Sector ordering"
            >
              <option value="1W">Sectors: Best 1W</option>
              <option value="1M">Sectors: Best 1M</option>
              <option value="count-desc">Sectors: Most stocks</option>
              <option value="count-asc">Sectors: Fewest stocks</option>
            </select>
          ) : null}

          {/* Export */}
          <button
            type="button"
            className="st-btn"
            onClick={onExport}
            disabled={items.length === 0}
          >
            <Download size={13} strokeWidth={2.2} />
            <span>Export</span>
          </button>
        </div>
      }
    >
      <div className="scan-table st-card">
        <div className="scan-table-head st-head" style={headTemplate}>
          <span>Stock</span>
          {/* Columns backed by a SortBy mode are click-to-sort; the rest stay
              plain labels rather than pretending to be interactive. */}
          {(() => {
            const header = (column: SortColumn, label: string, title?: string) => (
              <SortableHeader
                numeric
                active={isColumnActive(column, sortBy)}
                direction={sortBy.endsWith("_asc") ? "asc" : "desc"}
                onSort={() => setSortBy(nextColumnSort(column, sortBy))}
                title={title ?? `Sort by ${label}`}
              >
                {label}
              </SortableHeader>
            );
            return (
              <>
                {header("price", "Price")}
                {header("change", "Change")}
                {visibleCols.has("spark") ? <span className="st-num">Trend</span> : null}
                {visibleCols.has("rs") ? header("rs", "RS") : null}
                {visibleCols.has("rs1m") ? <span className="st-num">RS 1M</span> : null}
                {visibleCols.has("rvol") ? header("rvol", "RVOL") : null}
                {visibleCols.has("vdate") ? header("vdate", "Vol Date") : null}
                {visibleCols.has("sdate") ? <span className="st-num">Day</span> : null}
                {visibleCols.has("edate") ? header("edate", "Earnings") : null}
                {visibleCols.has("gap") ? <span className="st-num">Gap</span> : null}
              </>
            );
          })()}
          <span></span>
        </div>
        <div
          ref={shouldVirtualize ? containerRef : undefined}
          className={
            shouldVirtualize ? "scan-table-body scan-table-body-virtual" : "scan-table-body"
          }
        >
          {/* Test the FILTERED list: the old condition used `items` (pre-filter),
              so filtering every row away rendered a blank area with no message. */}
          {sortedItems.length === 0 ? (
            loading ? (
              <div className="empty-state">Fetching results for this screener…</div>
            ) : symbolFilter.trim() ? (
              // A row filter is hiding everything — say so and offer the undo,
              // rather than implying the scan itself found nothing.
              <EmptyState
                icon={<SearchX size={20} strokeWidth={2.1} />}
                title={`No rows match “${symbolFilter.trim()}”`}
                // `items` is the PRE-filter list; sortedItems derives from the
                // filtered one and would always read 0 here.
                body={`The scan returned ${items.length} stock${items.length === 1 ? "" : "s"}; your row filter is hiding ${items.length === 1 ? "it" : "them"}.`}
                action={{ label: "Clear filter", onClick: () => setSymbolFilter("") }}
              />
            ) : (
              <EmptyState
                icon={<SearchX size={20} strokeWidth={2.1} />}
                title="No stocks passed this scan today"
                body="That is a result, not an error — these setups do not print every session. Try another scanner, or loosen its thresholds in the settings panel."
              />
            )
          ) : shouldVirtualize ? (
            <div
              className="scan-table-virtual-spacer"
              style={{ height: `${totalHeight}px` }}
            >
              {visibleRows.map((row) => (
                <div
                  key={row.key}
                  className="scan-table-virtual-slot"
                  style={{ top: `${row.top}px`, height: `${row.height}px` }}
                >
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
            onClose={() => {
              setGridOpen(false);
              if (viewMode === "grid") setViewMode("table");
            }}
          />
        </Suspense>
      ) : null}
    </Panel>
  );
}
