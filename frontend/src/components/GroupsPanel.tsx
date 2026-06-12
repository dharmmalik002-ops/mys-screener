import { useEffect, useMemo, useRef, useState } from "react";

import type {
  IndustryGroupRankItem,
  IndustryGroupsResponse,
  IndustryGroupStockItem,
  MarketKey,
} from "../lib/api";
import { Panel } from "./Panel";

import "./GroupsPanel.css";

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

type GroupSortBy = "rank" | "score" | "1m" | "3m" | "6m";
type GroupStrengthFilter = "all" | "top40" | "top10";

const SORT_OPTIONS: Array<{ value: GroupSortBy; label: string }> = [
  { value: "rank", label: "Rank" },
  { value: "score", label: "Score" },
  { value: "1m", label: "1M" },
  { value: "3m", label: "3M" },
  { value: "6m", label: "6M" },
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
  const groupRowRefs = useRef<Record<string, HTMLElement | null>>({});

  useEffect(() => {
    setFocusedGroupId(null);
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
        return b.return_3m - a.return_3m;
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
      if (sortBy === "score") return b.score - a.score;
      if (sortBy === "1m") return b.relative_return_1m - a.relative_return_1m;
      if (sortBy === "3m") return b.relative_return_3m - a.relative_return_3m;
      return b.relative_return_6m - a.relative_return_6m;
    });
    return groups;
  }, [data, searchMatches, searchQuery, sortBy, strengthFilter]);

  useEffect(() => {
    onVisibleSymbolsChange(filteredGroups.flatMap((g) => g.symbols));
  }, [filteredGroups, onVisibleSymbolsChange]);

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
    ? `${data.total_groups} ranked groups · ${data.benchmark} · EOD ${data.as_of_date ?? ""}`
    : "Loading ranked industry groups";

  function handleGroupClick(group: IndustryGroupRankItem) {
    const members = stocksByGroup.get(group.group_id) ?? [];
    const first = members[0]?.symbol ?? group.symbols[0];
    if (first) {
      onPickSymbolWithContext(first, group.symbols.length ? group.symbols : members.map((m) => m.symbol));
    }
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
              <h3>Group Rankings</h3>
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
                    <th className="gp-num">1M</th>
                    <th className="gp-num">3M</th>
                    <th className="gp-num">6M</th>
                    <th className="gp-num" title="Leadership breadth: % of group members above their 50DMA. Broad participation beats two stocks dragging an index.">&gt;50D</th>
                    <th>Stocks <small style={{ opacity: 0.6, fontWeight: 500 }}>(all members for Top 20, else top 3)</small></th>
                  </tr>
                </thead>
                <tbody>
                  {filteredGroups.map((group) => {
                    const members = stocksByGroup.get(group.group_id) ?? [];
                    // For the top 20 ranked groups, expand the stock list to ALL
                    // constituents (already sorted by rs_rating desc in
                    // stocksByGroup). For deeper-ranked groups, fall back to the
                    // condensed "top 3 constituents" view so the table doesn't
                    // explode in height for hundreds of low-rank groups.
                    const showAllStocks = group.rank <= 20;
                    const stockSource = showAllStocks ? members : group.top_constituents.slice(0, 3);
                    const displayedStocks = stockSource.map((src) => {
                      // members[] rows already carry change_pct & rs_rating; top_constituents
                      // rows carry rs_rating & return_1m. Normalize both into one shape.
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

                    return (
                      <tr
                        key={group.group_id}
                        ref={(el) => {
                          groupRowRefs.current[group.group_id] = el;
                        }}
                        className={`gp-row${focusedGroupId === group.group_id ? " is-focused" : ""}`}
                      >
                        <td>
                          <RankBadge rank={group.rank} />
                          <RankDelta change={group.rank_change_1w} />
                        </td>
                        <td className="gp-cell-name">
                          <button
                            type="button"
                            className="gp-group-name-btn"
                            onClick={() => handleGroupClick(group)}
                          >
                            <strong>{group.group_name}</strong>
                            <small>{group.parent_sector} · {group.stock_count} stocks</small>
                          </button>
                        </td>
                        <td className="gp-num">
                          <span className="gp-score-chip">{formatScore(group.score)}</span>
                        </td>
                        <td className={`gp-num ${metricClass(group.return_1m)}`}>{formatReturn(group.return_1m)}</td>
                        <td className={`gp-num ${metricClass(group.return_3m)}`}>{formatReturn(group.return_3m)}</td>
                        <td className={`gp-num ${metricClass(group.return_6m)}`}>{formatReturn(group.return_6m)}</td>
                        <td
                          className="gp-num"
                          title={`Leadership breadth — ${Math.round(group.pct_above_50dma)}% of members above the 50DMA, ${Math.round(group.pct_above_200dma)}% above the 200DMA. Breadth score ${Math.round(group.breadth_score)}.`}
                        >
                          <span className={`gp-breadth${group.pct_above_50dma >= 70 ? " hi" : group.pct_above_50dma >= 40 ? " mid" : " lo"}`}>
                            {Math.round(group.pct_above_50dma)}%
                          </span>
                        </td>
                        <td className="gp-cell-stocks">
                          <div className={`gp-top3${showAllStocks ? " gp-all-members" : ""}`}>
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
    </Panel>
  );
}
