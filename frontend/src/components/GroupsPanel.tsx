import { Suspense, lazy, useEffect, useMemo, useRef, useState } from "react";
import { LayoutGrid } from "lucide-react";

import {
  getChartGridSeries,
  type ChartBar,
  type ChartGridTimeframe,
  IndustryGroupRankItem,
  IndustryGroupsResponse,
  IndustryGroupStockItem,
  MarketKey,
} from "../lib/api";
import type {
  ChartGridChartStyle,
  ChartGridDisplayCard,
  ChartGridDisplayMode,
  ChartGridGroupSection,
  ChartGridSortBy,
  ChartGridStat,
} from "./ChartGridModal";
import { Panel } from "./Panel";

import "./GroupsPanel.css";

const ChartGridModal = lazy(() =>
  import("./ChartGridModal").then((module) => ({ default: module.ChartGridModal })),
);

type GroupsPanelProps = {
  market: MarketKey;
  data: IndustryGroupsResponse | null;
  loading?: boolean;
  selectedSymbol: string | null;
  focusRequest?: { groupId?: string | null; symbol?: string | null; nonce: number } | null;
  onPickSymbolWithContext: (symbol: string, contextSymbols: string[]) => void;
  onRequestAddToWatchlist: (symbol: string) => void;
  onVisibleSymbolsChange: (symbols: string[]) => void;
};

type GroupSortBy = "rank" | "score" | "return_1w" | "return_1m" | "return_3m" | "return_6m";
type GroupStrengthFilter = "all" | "top40" | "top10";

const SORT_OPTIONS: Array<{ value: GroupSortBy; label: string }> = [
  { value: "rank", label: "Fast Rank" },
  { value: "score", label: "Fast Score" },
  { value: "return_1w", label: "1W Return" },
  { value: "return_1m", label: "1M Return" },
  { value: "return_3m", label: "3M Return" },
  { value: "return_6m", label: "6M Return" },
];

/* ---------- formatters ---------- */

function formatReturn(value: number) {
  if (!Number.isFinite(value)) return "—";
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatScore(value: number) {
  return value.toFixed(1);
}

function formatPrice(value: number) {
  return `₹${value.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function metricClass(value: number) {
  return value >= 0 ? "gp-pos" : "gp-neg";
}

function csvValue(value: string | number | null | undefined) {
  const text = value === null || value === undefined ? "" : String(value);
  return `"${text.replace(/"/g, '""')}"`;
}

function initials(symbol: string) {
  return symbol.slice(0, 2).toUpperCase();
}

function getLogoUrl(symbol: string) {
  const clean = symbol.replace("^", "").toUpperCase();
  const mapping: Record<string, string> = {
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
  const id = mapping[clean];
  if (id) return `https://s3-symbol-logo.tradingview.com/${id}.svg`;
  return null;
}

function RankBadge({ rank }: { rank: number }) {
  if (rank === 1) return <span className="gp-rank-badge gp-rank-gold">🥇</span>;
  if (rank === 2) return <span className="gp-rank-badge gp-rank-silver">🥈</span>;
  if (rank === 3) return <span className="gp-rank-badge gp-rank-bronze">🥉</span>;
  return <span className="gp-rank-badge gp-rank-num">{rank}</span>;
}

function RankDelta({ change }: { change: number | null }) {
  if (change === null || !Number.isFinite(change) || change === 0) {
    return null;
  }
  const improving = change > 0;
  return (
    <span
      className={`gp-rank-delta${improving ? " up" : " down"}`}
      title={`${improving ? "Up" : "Down"} ${Math.abs(change)} rank${Math.abs(change) === 1 ? "" : "s"} vs 1 week ago — ${improving ? "improving" : "fading"} group`}
    >
      {improving ? "▲" : "▼"}
      {Math.abs(change)}
    </span>
  );
}

function RsCircle({ rs }: { rs: number | null }) {
  if (rs === null || !Number.isFinite(rs)) {
    return <span className="gp-rs-circle gp-rs-muted">—</span>;
  }
  const tone = rs >= 80 ? "hi" : rs >= 60 ? "mid" : "lo";
  return <span className={`gp-rs-circle gp-rs-${tone}`}>{Math.round(rs)}</span>;
}

function selectedReturnForGrid(stock: IndustryGroupStockItem, timeframe: ChartGridTimeframe) {
  if (timeframe === "3M") return stock.return_3m;
  if (timeframe === "6M") return stock.return_6m;
  if (timeframe === "1Y") return stock.return_1y;
  return stock.return_1y;
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

/* ---------- Component ---------- */

export function GroupsPanel({
  market: _market,
  data,
  loading = false,
  selectedSymbol,
  focusRequest = null,
  onPickSymbolWithContext,
  onRequestAddToWatchlist,
  onVisibleSymbolsChange,
}: GroupsPanelProps) {
  const [searchQuery, setSearchQuery] = useState("");
  const [sortBy, setSortBy] = useState<GroupSortBy>("rank");
  const [strengthFilter, setStrengthFilter] = useState<GroupStrengthFilter>("all");
  const [focusedGroupId, setFocusedGroupId] = useState<string | null>(null);
  const [gridGroupId, setGridGroupId] = useState<string | null>(null);
  const [gridColumns, setGridColumns] = useState(2);
  const [gridRows, setGridRows] = useState(1);
  const [gridTimeframe, setGridTimeframe] = useState<ChartGridTimeframe>("6M");
  const [gridSortBy, setGridSortBy] = useState<ChartGridSortBy>("rs_rating");
  const [gridChartStyle, setGridChartStyle] = useState<ChartGridChartStyle>("candles");
  const [gridDisplayMode, setGridDisplayMode] = useState<ChartGridDisplayMode>("normal");
  const groupRowRefs = useRef<Record<string, HTMLElement | null>>({});

  useEffect(() => {
    setFocusedGroupId(null);
    setGridGroupId(null);
  }, [data, _market]);

  const stocksByGroup = useMemo(() => {
    const grouped = new Map<string, IndustryGroupStockItem[]>();
    for (const stock of data?.stocks ?? []) {
      const current = grouped.get(stock.final_group_id);
      if (current) current.push(stock);
      else grouped.set(stock.final_group_id, [stock]);
    }
    for (const members of grouped.values()) {
      members.sort((a, b) => {
        const rs = (b.rs_rating ?? -1) - (a.rs_rating ?? -1);
        if (rs !== 0) return rs;
        const oneWeek = b.return_1w - a.return_1w;
        if (oneWeek !== 0) return oneWeek;
        return b.return_1m - a.return_1m;
      });
    }
    return grouped;
  }, [data]);

  const searchMatches = useMemo(() => {
    const normalized = searchQuery.trim().toLowerCase();
    const matches = new Set<string>();
    if (!normalized) return matches;
    for (const group of data?.groups ?? []) {
      if (
        group.group_name.toLowerCase().includes(normalized) ||
        group.parent_sector.toLowerCase().includes(normalized) ||
        group.description.toLowerCase().includes(normalized)
      ) {
        matches.add(group.group_id);
        continue;
      }
      const members = stocksByGroup.get(group.group_id) ?? [];
      if (members.some((m) => m.symbol.toLowerCase().includes(normalized) || m.company_name.toLowerCase().includes(normalized))) {
        matches.add(group.group_id);
      }
    }
    return matches;
  }, [data, searchQuery, stocksByGroup]);

  const filteredGroups = useMemo(() => {
    const groups = [...(data?.groups ?? [])].filter((group) => {
      if (strengthFilter === "top10" && group.rank > 10) return false;
      if (strengthFilter === "top40" && group.rank > 40) return false;
      if (!searchQuery.trim()) return true;
      return searchMatches.has(group.group_id);
    });
    groups.sort((a, b) => {
      if (sortBy === "rank") return a.rank - b.rank;
      if (sortBy === "return_1w") return b.return_1w - a.return_1w;
      if (sortBy === "return_1m") return b.return_1m - a.return_1m;
      if (sortBy === "return_3m") return b.return_3m - a.return_3m;
      if (sortBy === "return_6m") return b.return_6m - a.return_6m;
      return b.score - a.score;
    });
    return groups;
  }, [data, searchMatches, searchQuery, sortBy, strengthFilter]);

  useEffect(() => {
    onVisibleSymbolsChange(filteredGroups.flatMap((g) => g.symbols));
  }, [filteredGroups, onVisibleSymbolsChange]);

  const activeGridGroup = useMemo(
    () => (gridGroupId ? data?.groups.find((group) => group.group_id === gridGroupId) ?? null : null),
    [data, gridGroupId],
  );
  const activeGridIndex = useMemo(
    () => (gridGroupId ? filteredGroups.findIndex((group) => group.group_id === gridGroupId) : -1),
    [filteredGroups, gridGroupId],
  );
  const previousGridGroup = activeGridIndex > 0 ? filteredGroups[activeGridIndex - 1] : null;
  const nextGridGroup = activeGridIndex >= 0 && activeGridIndex < filteredGroups.length - 1 ? filteredGroups[activeGridIndex + 1] : null;
  const activeGridStocks = useMemo(() => {
    if (!activeGridGroup) return [];
    return stocksByGroup.get(activeGridGroup.group_id) ?? [];
  }, [activeGridGroup, stocksByGroup]);
  const activeGridSymbols = useMemo(() => {
    if (!activeGridGroup) return [];
    if (activeGridGroup.symbols.length) return activeGridGroup.symbols;
    return activeGridStocks.map((stock) => stock.symbol);
  }, [activeGridGroup, activeGridStocks]);
  const gridCards = useMemo<ChartGridDisplayCard[]>(() => {
    if (!activeGridGroup) return [];
    return activeGridStocks.map((stock) => {
      const selectedReturn = selectedReturnForGrid(stock, gridTimeframe);
      return {
        id: `${activeGridGroup.group_id}:${stock.symbol}`,
        symbol: stock.symbol,
        entityLabel: "Stock",
        title: stock.symbol,
        subtitle: stock.company_name || stock.final_group_name || activeGridGroup.group_name,
        footerLabel: "Price",
        footerValue: stock.last_price > 0 ? formatPrice(stock.last_price) : "—",
        primaryBadge: {
          label: `${gridTimeframe} ${formatReturn(selectedReturn)}`,
          tone: selectedReturn >= 0 ? "positive" : "negative",
        },
        secondaryBadge: {
          label: `1D ${formatReturn(stock.change_pct)}`,
          tone: stock.change_pct >= 0 ? "positive" : "negative",
        },
        points: fallbackSparkline(selectedReturn),
        selectedReturn,
        dayReturn: stock.change_pct,
        rsRating: stock.rs_rating ?? null,
        marketCapCrore: stock.market_cap_cr,
        constituents: null,
        onClick: () => onPickSymbolWithContext(stock.symbol, activeGridSymbols),
      };
    });
  }, [activeGridGroup, activeGridStocks, activeGridSymbols, gridTimeframe, onPickSymbolWithContext]);

  const gridGroupSections = useMemo<ChartGridGroupSection[]>(() => {
    if (!activeGridGroup || !gridCards.length) return [];
    return [
      {
        id: activeGridGroup.group_id,
        title: activeGridGroup.group_name,
        subtitle: `${activeGridGroup.parent_sector} · base rank #${activeGridGroup.rank} · ${gridCards.length} stock${gridCards.length === 1 ? "" : "s"}`,
        baseRank: activeGridGroup.rank,
        stockCount: activeGridGroup.stock_count,
        returns: {
          "1W": activeGridGroup.return_1w,
          "1M": activeGridGroup.return_1m,
          "3M": activeGridGroup.return_3m,
          "6M": activeGridGroup.return_6m,
        },
        cards: gridCards,
      },
    ];
  }, [activeGridGroup, gridCards]);

  const gridStats = useMemo<ChartGridStat[]>(() => {
    if (!activeGridGroup) return [];
    const advancing = activeGridStocks.filter((stock) => stock.change_pct > 0).length;
    const declining = activeGridStocks.filter((stock) => stock.change_pct < 0).length;
    return [
      { label: "Stocks", value: `${activeGridStocks.length || activeGridGroup.stock_count}` },
      { label: "Group Rank", value: `#${activeGridGroup.rank}` },
      { label: "1M Group", value: formatReturn(activeGridGroup.return_1m), tone: activeGridGroup.return_1m >= 0 ? "positive" : "negative" },
      { label: "Advancing", value: `${advancing}`, tone: advancing >= declining ? "positive" : "neutral" },
      { label: "Declining", value: `${declining}`, tone: declining > advancing ? "negative" : "neutral" },
    ];
  }, [activeGridGroup, activeGridStocks]);

  async function loadGroupGridSeries(
    symbols: string[],
    timeframe: ChartGridTimeframe,
  ): Promise<Record<string, ChartBar[]>> {
    const payload = await getChartGridSeries(symbols, timeframe, _market);
    return payload.items.reduce<Record<string, ChartBar[]>>((accumulator, item) => {
      accumulator[item.symbol] = item.bars;
      return accumulator;
    }, {});
  }

  useEffect(() => {
    if (!focusRequest || !data) return;
    const symbolMatch = focusRequest.symbol
      ? data.stocks.find((s) => s.symbol.toUpperCase() === focusRequest.symbol?.trim().toUpperCase())
      : null;
    const targetGroupId = focusRequest.groupId ?? symbolMatch?.final_group_id ?? null;
    if (!targetGroupId) return;
    setStrengthFilter("all");
    setSearchQuery(focusRequest.symbol ?? "");
    setFocusedGroupId(targetGroupId);
    const scrollId = window.setTimeout(() => {
      groupRowRefs.current[targetGroupId]?.scrollIntoView({ behavior: "smooth", block: "center" });
    }, 80);
    const clearId = window.setTimeout(() => {
      setFocusedGroupId((c) => (c === targetGroupId ? null : c));
    }, 2600);
    return () => {
      window.clearTimeout(scrollId);
      window.clearTimeout(clearId);
    };
  }, [data, focusRequest]);

  const allGroups = data?.groups ?? [];
  const totalGroups = data?.total_groups ?? allGroups.length;

  const pageSubtitle = data
    ? `${data.total_groups} fast swing groups · ${data.benchmark} · EOD ${data.as_of_date ?? ""}`
    : "Loading ranked industry groups";

  function handleGroupClick(group: IndustryGroupRankItem) {
    const members = stocksByGroup.get(group.group_id) ?? [];
    const first = members[0]?.symbol ?? group.symbols[0];
    if (first) {
      onPickSymbolWithContext(first, group.symbols.length ? group.symbols : members.map((m) => m.symbol));
    }
  }

  function handleExportTop20Stocks() {
    const topGroups = [...(data?.groups ?? [])].sort((a, b) => a.rank - b.rank).slice(0, 20);
    if (!topGroups.length) return;

    const rows = [
      [
        "Group Rank",
        "Group Name",
        "Parent Sector",
        "Symbol",
        "Company Name",
        "Last Price",
        "Day Change %",
        "RS Rating",
        "Above 50DMA",
        "Above 200DMA",
      ],
    ];
    const seen = new Set<string>();
    topGroups.forEach((group) => {
      const members = stocksByGroup.get(group.group_id) ?? [];
      const fallbackSymbols = group.symbols.map((symbol) => ({ symbol, company_name: "", last_price: "", change_pct: "", rs_rating: "" }));
      const source = members.length ? members : fallbackSymbols;
      source.forEach((stock) => {
        const key = `${group.group_id}:${stock.symbol}`;
        if (seen.has(key)) return;
        seen.add(key);
        rows.push([
          group.rank,
          group.group_name,
          group.parent_sector,
          stock.symbol,
          stock.company_name,
          stock.last_price,
          stock.change_pct,
          stock.rs_rating ?? "",
          group.pct_above_50dma,
          group.pct_above_200dma,
        ]);
      });
    });

    const csv = rows.map((row) => row.map(csvValue).join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `top-20-industry-group-stocks-${data?.as_of_date ?? "latest"}.csv`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  }

  return (
    <Panel title="Industry Groups" subtitle={pageSubtitle}>
      <div className="gp-root">
        {/* ===== TOOLBAR ===== */}
        <div className="gp-toolbar">
          <div className="gp-toolbar-left">
            <input
              type="search"
              className="gp-search"
              placeholder="Search group, sector or symbol…"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          </div>
          <div className="gp-toolbar-right">
            <button
              type="button"
              className="gp-export-btn"
              onClick={handleExportTop20Stocks}
              disabled={!data?.groups?.length}
            >
              Export Top 20 Stocks
            </button>
            <div className="gp-tabs">
              {(["all", "top40", "top10"] as const).map((s) => (
                <button
                  key={s}
                  type="button"
                  className={`gp-tab${strengthFilter === s ? " active" : ""}`}
                  onClick={() => setStrengthFilter(s)}
                >
                  {s === "all" ? "All" : s === "top40" ? "Top 40" : "Top 10"}
                </button>
              ))}
            </div>
            <select
              className="gp-select"
              value={sortBy}
              onChange={(e) => setSortBy(e.target.value as GroupSortBy)}
              aria-label="Sort"
            >
              {SORT_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>Sort: {o.label}</option>
              ))}
            </select>
          </div>
        </div>

        {/* ===== MASTER TABLE ===== */}
        <section className="gp-card gp-card-table">
          <div className="gp-card-head">
            <div>
              <h3>Fast Group Rankings</h3>
              <p className="gp-card-sub">
                {filteredGroups.length} of {totalGroups} groups
                {searchQuery.trim() ? ` · matching "${searchQuery.trim()}"` : ""}
              </p>
            </div>
          </div>

          {!filteredGroups.length ? (
            <div className="gp-empty">
              {loading ? "Loading group ranks…" : searchQuery.trim() ? "No groups match that search." : "No group data available yet."}
            </div>
          ) : (
            <div className="gp-table-scroll">
              <table className="gp-table">
                <thead>
                  <tr>
                    <th style={{ width: 56 }}>#</th>
                    <th>Industry Group</th>
                    <th className="gp-num">Score</th>
                    <th className="gp-num" title="Leadership breadth: % of group members above their 50DMA. Broad participation beats two stocks dragging an index.">&gt;50D</th>
                    <th>Top Stocks</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredGroups.map((group) => {
                    const members = stocksByGroup.get(group.group_id) ?? [];
                    const stockSource = group.top_constituents.slice(0, 3);
                    const displayedStocks = stockSource.map((src) => {
                      const isMember = "change_pct" in src;
                      const match = isMember ? (src as IndustryGroupStockItem) : members.find((m) => m.symbol === src.symbol);
                      return {
                        symbol: src.symbol,
                        company: (src as { company_name?: string }).company_name ?? match?.company_name ?? "",
                        rs: isMember ? (src as IndustryGroupStockItem).rs_rating : (src as { rs_rating: number | null }).rs_rating,
                        change_pct: match?.change_pct ?? (src as { return_1m?: number }).return_1m ?? 0,
                      };
                    });
                    const groupSymbols = group.symbols.length ? group.symbols : members.map((m) => m.symbol);
                    const isGridActive = gridGroupId === group.group_id;

                    return (
                      <tr
                        key={group.group_id}
                        ref={(el) => {
                          groupRowRefs.current[group.group_id] = el;
                        }}
                        className={`gp-row${focusedGroupId === group.group_id ? " is-focused" : ""}${isGridActive ? " is-expanded" : ""}`}
                      >
                        <td>
                          <RankBadge rank={group.rank} />
                          <RankDelta change={group.rank_change_1w} />
                        </td>
                        <td className="gp-cell-name">
                          <div className="gp-group-title-row">
                            <button
                              type="button"
                              className="gp-group-name-btn"
                              onClick={() => handleGroupClick(group)}
                            >
                              <strong>{group.group_name}</strong>
                              <small>{group.parent_sector} · {group.stock_count} stocks</small>
                            </button>
                            <button
                              type="button"
                              className={`gp-group-grid-toggle${isGridActive ? " active" : ""}`}
                              onClick={() => setGridGroupId(group.group_id)}
                              disabled={!members.length}
                              title={members.length ? `Open ${group.group_name} chart grid` : `No stock chart data for ${group.group_name}`}
                              aria-label={members.length ? `Open ${group.group_name} chart grid` : `No stock chart data for ${group.group_name}`}
                            >
                              <LayoutGrid size={16} strokeWidth={2.2} />
                            </button>
                          </div>
                        </td>
                        <td className="gp-num">
                          <span className="gp-score-chip">{formatScore(group.score)}</span>
                        </td>
                        <td
                          className="gp-num"
                          title={`Leadership breadth — ${Math.round(group.pct_above_50dma)}% of members above the 50DMA, ${Math.round(group.pct_above_200dma)}% above the 200DMA. Breadth score ${Math.round(group.breadth_score)}.`}
                        >
                          <span className={`gp-breadth${group.pct_above_50dma >= 70 ? " hi" : group.pct_above_50dma >= 40 ? " mid" : " lo"}`}>
                            {Math.round(group.pct_above_50dma)}%
                          </span>
                        </td>
                        <td className="gp-cell-stocks">
                          <div className="gp-top3">
                            {displayedStocks.length === 0 ? (
                              <span className="gp-muted">No leaders</span>
                            ) : displayedStocks.map((s) => {
                              const logo = getLogoUrl(s.symbol);
                              const up = s.change_pct >= 0;
                              return (
                                <button
                                  key={`${group.group_id}-${s.symbol}`}
                                  type="button"
                                  className={`gp-stock-chip${selectedSymbol === s.symbol ? " active" : ""}`}
                                  onClick={() => onPickSymbolWithContext(s.symbol, groupSymbols)}
                                  onContextMenu={(e) => {
                                    e.preventDefault();
                                    onRequestAddToWatchlist(s.symbol);
                                  }}
                                  title={`${s.company} · right-click to add to watchlist`}
                                >
                                  {logo ? (
                                    <img src={logo} alt="" className="gp-stock-logo" onError={(e) => { e.currentTarget.style.display = "none"; }} />
                                  ) : (
                                    <span className="gp-stock-avatar">{initials(s.symbol)}</span>
                                  )}
                                  <span className="gp-stock-text">
                                    <span className="gp-stock-sym">{s.symbol}</span>
                                    <span className={`gp-stock-chg ${up ? "gp-pos" : "gp-neg"}`}>{formatReturn(s.change_pct)}</span>
                                  </span>
                                  <RsCircle rs={s.rs} />
                                </button>
                              );
                            })}
                          </div>
                          {members[0] ? (
                            <div className="gp-lead-price">
                              <span>Lead:</span> <strong>{members[0].symbol}</strong> @ {formatPrice(members[0].last_price)}
                            </div>
                          ) : null}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </div>
      {activeGridGroup ? (
        <Suspense fallback={null}>
          <ChartGridModal
            contextLabel="Group"
            title={activeGridGroup.group_name}
            subtitle={`${gridCards.length || activeGridGroup.stock_count} stocks · rank #${activeGridGroup.rank} · ${activeGridGroup.parent_sector}`}
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
            onLoadSeries={loadGroupGridSeries}
            onAddToWatchlist={onRequestAddToWatchlist}
            previousAction={{
              label: previousGridGroup ? `Previous group: ${previousGridGroup.group_name}` : "No previous group",
              disabled: !previousGridGroup,
              onClick: () => {
                if (previousGridGroup) setGridGroupId(previousGridGroup.group_id);
              },
            }}
            nextAction={{
              label: nextGridGroup ? `Next group: ${nextGridGroup.group_name}` : "No next group",
              disabled: !nextGridGroup,
              onClick: () => {
                if (nextGridGroup) setGridGroupId(nextGridGroup.group_id);
              },
            }}
            onClose={() => setGridGroupId(null)}
          />
        </Suspense>
      ) : null}
    </Panel>
  );
}
