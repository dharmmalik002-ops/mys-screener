import { Suspense, lazy, useEffect, useMemo, useRef, useState } from "react";

import {
  getChartGridSeries,
  type ChartBar,
  type ChartGridTimeframe,
  type IndustryGroupRankItem,
  type IndustryGroupsResponse,
  type IndustryGroupStockItem,
  type MarketKey,
} from "../lib/api";
import type { ChartGridChartStyle, ChartGridDisplayCard, ChartGridDisplayMode, ChartGridSortBy, ChartGridStat } from "./ChartGridModal";
import { Panel } from "./Panel";

const ChartGridModal = lazy(() => import("./ChartGridModal").then((module) => ({ default: module.ChartGridModal })));

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

type GroupSortBy = "rank" | "score" | "1m" | "3m" | "6m" | "breadth" | "trend";
type GroupStrengthFilter = "all" | "top40" | "top10";

const SORT_OPTIONS: Array<{ value: GroupSortBy; label: string }> = [
  { value: "rank", label: "Rank" },
  { value: "score", label: "Score" },
  { value: "1m", label: "1M" },
  { value: "3m", label: "3M" },
  { value: "6m", label: "6M" },
  { value: "breadth", label: "Breadth" },
  { value: "trend", label: "Trend" },
];

function formatReturn(value: number) {
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatScore(value: number) {
  return value.toFixed(1);
}

function formatPrice(value: number, market: MarketKey) {
  const locale = market === "us" ? "en-US" : "en-IN";
  const symbol = market === "us" ? "$" : "₹";
  return `${symbol}${value.toLocaleString(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function formatMarketCap(value: number, market: MarketKey) {
  const locale = market === "us" ? "en-US" : "en-IN";
  const suffix = market === "us" ? "Bn" : "Cr";
  return `${value.toLocaleString(locale, { maximumFractionDigits: 0 })} ${suffix}`;
}

function metricClass(value: number) {
  return value >= 0 ? "positive-text" : "negative-text";
}

function formatRankChange(value: number | null) {
  if (value === null) {
    return "New";
  }
  if (value === 0) {
    return "0";
  }
  return value > 0 ? `+${value}` : String(value);
}

function formatScoreChange(value: number | null) {
  if (value === null) {
    return "--";
  }
  return `${value > 0 ? "+" : ""}${value.toFixed(1)}`;
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

function memberReturn(member: IndustryGroupStockItem, timeframe: ChartGridTimeframe) {
  if (timeframe === "3M") {
    return member.return_3m;
  }
  if (timeframe === "6M") {
    return member.return_6m;
  }
  return member.return_1y;
}

function buildMemberGridCards(
  members: IndustryGroupStockItem[],
  timeframe: ChartGridTimeframe,
  market: MarketKey,
  onPickSymbolWithContext: (symbol: string, contextSymbols: string[]) => void,
): ChartGridDisplayCard[] {
  const contextSymbols = members.map((member) => member.symbol);
  return members.map((member) => {
    const selectedReturn = memberReturn(member, timeframe);
    return {
      id: `group:${member.final_group_id}:${member.symbol}`,
      symbol: member.symbol,
      entityLabel: "Stock",
      title: member.symbol,
      subtitle: member.company_name,
      footerLabel: "Mcap",
      footerValue: formatMarketCap(member.market_cap_cr, market),
      primaryBadge: {
        label: `${timeframe} ${formatReturn(selectedReturn)}`,
        tone: selectedReturn >= 0 ? "positive" : "negative",
      },
      secondaryBadge: {
        label: `1D ${formatReturn(member.change_pct)}`,
        tone: member.change_pct >= 0 ? "positive" : "negative",
      },
      points: fallbackSparkline(selectedReturn),
      selectedReturn,
      dayReturn: member.change_pct,
      rsRating: member.rs_rating,
      marketCapCrore: member.market_cap_cr,
      constituents: null,
      onClick: () => onPickSymbolWithContext(member.symbol, contextSymbols),
    };
  });
}

function buildModalStats(group: IndustryGroupRankItem, members: IndustryGroupStockItem[]): ChartGridStat[] {
  const advances = members.filter((member) => member.change_pct > 0).length;
  const declines = members.filter((member) => member.change_pct < 0).length;
  return [
    { label: "Rank", value: group.rank_label },
    { label: "Stocks", value: `${group.stock_count}` },
    { label: "Breadth", value: `${group.breadth_score.toFixed(1)}`, tone: group.breadth_score >= 50 ? "positive" : "negative" },
    { label: "Advancing", value: `${advances}`, tone: advances >= declines ? "positive" : "neutral" },
    { label: "Declining", value: `${declines}`, tone: declines > advances ? "negative" : "neutral" },
    { label: "Leaders", value: group.leaders.slice(0, 3).join(", ") || "--" },
  ];
}

export function GroupsPanel({
  market,
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
  const [expandedGroupIds, setExpandedGroupIds] = useState<string[]>([]);
  const [gridGroupId, setGridGroupId] = useState<string | null>(null);
  const [gridColumns, setGridColumns] = useState(4);
  const [gridRows, setGridRows] = useState(3);
  const [gridTimeframe, setGridTimeframe] = useState<ChartGridTimeframe>("6M");
  const [gridSortBy, setGridSortBy] = useState<ChartGridSortBy>("selected_return");
  const [gridChartStyle, setGridChartStyle] = useState<ChartGridChartStyle>("line");
  const [gridDisplayMode, setGridDisplayMode] = useState<ChartGridDisplayMode>("compact");
  const [memberGridSeries, setMemberGridSeries] = useState<Record<string, ChartBar[]>>({});
  const [focusedGroupId, setFocusedGroupId] = useState<string | null>(null);
  const groupCardRefs = useRef<Record<string, HTMLElement | null>>({});

  useEffect(() => {
    setExpandedGroupIds([]);
    setGridGroupId(null);
    setMemberGridSeries({});
    setFocusedGroupId(null);
  }, [data, market]);

  const stocksByGroup = useMemo(() => {
    const grouped = new Map<string, IndustryGroupStockItem[]>();
    for (const stock of data?.stocks ?? []) {
      const current = grouped.get(stock.final_group_id);
      if (current) {
        current.push(stock);
      } else {
        grouped.set(stock.final_group_id, [stock]);
      }
    }
    for (const members of grouped.values()) {
      members.sort((left, right) => {
        const rsDiff = (right.rs_rating ?? -1) - (left.rs_rating ?? -1);
        if (rsDiff !== 0) {
          return rsDiff;
        }
        return right.return_3m - left.return_3m;
      });
    }
    return grouped;
  }, [data]);

  const searchMatches = useMemo(() => {
    const normalized = searchQuery.trim().toLowerCase();
    const matches = new Map<string, string>();
    if (!normalized) {
      return matches;
    }

    for (const group of data?.groups ?? []) {
      if (
        group.group_name.toLowerCase().includes(normalized)
        || group.parent_sector.toLowerCase().includes(normalized)
        || group.description.toLowerCase().includes(normalized)
      ) {
        matches.set(group.group_id, "Group match");
        continue;
      }
      const stockMatch = (stocksByGroup.get(group.group_id) ?? []).find((stock) => (
        stock.symbol.toLowerCase().includes(normalized)
        || stock.company_name.toLowerCase().includes(normalized)
      ));
      if (stockMatch) {
        matches.set(group.group_id, `Found via ${stockMatch.symbol}`);
      }
    }

    return matches;
  }, [data, searchQuery, stocksByGroup]);

  const filteredGroups = useMemo(() => {
    const groups = [...(data?.groups ?? [])].filter((group) => {
      if (strengthFilter === "top10" && group.rank > 10) {
        return false;
      }
      if (strengthFilter === "top40" && group.rank > 40) {
        return false;
      }
      if (!searchQuery.trim()) {
        return true;
      }
      return searchMatches.has(group.group_id);
    });

    groups.sort((left, right) => {
      if (sortBy === "rank") {
        return left.rank - right.rank;
      }
      if (sortBy === "score") {
        return right.score - left.score;
      }
      if (sortBy === "1m") {
        return right.relative_return_1m - left.relative_return_1m;
      }
      if (sortBy === "3m") {
        return right.relative_return_3m - left.relative_return_3m;
      }
      if (sortBy === "6m") {
        return right.relative_return_6m - left.relative_return_6m;
      }
      if (sortBy === "breadth") {
        return right.breadth_score - left.breadth_score;
      }
      return right.trend_health_score - left.trend_health_score;
    });
    return groups;
  }, [data, searchMatches, searchQuery, sortBy, strengthFilter]);

  useEffect(() => {
    onVisibleSymbolsChange(filteredGroups.flatMap((group) => group.symbols));
  }, [filteredGroups, onVisibleSymbolsChange]);

  useEffect(() => {
    if (!focusRequest || !data) {
      return;
    }

    const symbolMatch = focusRequest.symbol
      ? data.stocks.find((item) => item.symbol.toUpperCase() === focusRequest.symbol?.trim().toUpperCase())
      : null;
    const targetGroupId = focusRequest.groupId ?? symbolMatch?.final_group_id ?? null;
    if (!targetGroupId) {
      return;
    }

    setStrengthFilter("all");
    setSearchQuery(focusRequest.symbol ?? "");
    setExpandedGroupIds((current) => (current.includes(targetGroupId) ? current : [...current, targetGroupId]));
    setFocusedGroupId(targetGroupId);

    const timeoutId = window.setTimeout(() => {
      groupCardRefs.current[targetGroupId]?.scrollIntoView({ behavior: "smooth", block: "center" });
    }, 80);
    const clearHighlightId = window.setTimeout(() => {
      setFocusedGroupId((current) => (current === targetGroupId ? null : current));
    }, 2400);

    return () => {
      window.clearTimeout(timeoutId);
      window.clearTimeout(clearHighlightId);
    };
  }, [data, focusRequest]);

  const activeGridGroup = useMemo(
    () => data?.groups.find((group) => group.group_id === gridGroupId) ?? null,
    [data, gridGroupId],
  );

  const activeGridMembers = useMemo(
    () => (activeGridGroup ? (stocksByGroup.get(activeGridGroup.group_id) ?? []) : []),
    [activeGridGroup, stocksByGroup],
  );

  async function loadMemberGridSeries(symbols: string[], timeframe: ChartGridTimeframe) {
    const missingSymbols = symbols.filter((symbol) => !memberGridSeries[`${timeframe}:${symbol}`]);
    if (!missingSymbols.length) {
      return symbols.reduce<Record<string, ChartBar[]>>((accumulator, symbol) => {
        accumulator[symbol] = memberGridSeries[`${timeframe}:${symbol}`] ?? [];
        return accumulator;
      }, {});
    }

    const payload = await getChartGridSeries(missingSymbols, timeframe, market);
    const loaded = payload.items.reduce<Record<string, ChartBar[]>>((accumulator, item) => {
      accumulator[item.symbol] = item.bars;
      return accumulator;
    }, {});
    setMemberGridSeries((current) => {
      const next = { ...current };
      Object.entries(loaded).forEach(([symbol, bars]) => {
        next[`${timeframe}:${symbol}`] = bars;
      });
      return next;
    });
    return symbols.reduce<Record<string, ChartBar[]>>((accumulator, symbol) => {
      accumulator[symbol] = loaded[symbol] ?? memberGridSeries[`${timeframe}:${symbol}`] ?? [];
      return accumulator;
    }, {});
  }

  const pageSubtitle = data
    ? `${data.total_groups} custom groups ranked versus ${data.benchmark}`
    : market === "india"
      ? "Loading ranked industry groups for India"
      : "Loading ranked industry groups";

  return (
    <Panel
      title="Groups"
      subtitle={pageSubtitle}
      className="groups-panel"
      actions={
        <div className="groups-toolbar">
          <form className="groups-search" onSubmit={(event) => event.preventDefault()}>
            <input
              value={searchQuery}
              onChange={(event) => setSearchQuery(event.target.value)}
              placeholder="Search group, symbol, or company"
            />
          </form>

          <div className="sector-sort-pills">
            <button
              type="button"
              className={strengthFilter === "all" ? "tool-pill active" : "tool-pill"}
              onClick={() => setStrengthFilter("all")}
            >
              All
            </button>
            <button
              type="button"
              className={strengthFilter === "top40" ? "tool-pill active" : "tool-pill"}
              onClick={() => setStrengthFilter("top40")}
            >
              Top 40
            </button>
            <button
              type="button"
              className={strengthFilter === "top10" ? "tool-pill active" : "tool-pill"}
              onClick={() => setStrengthFilter("top10")}
            >
              Top 10
            </button>
          </div>

          <label className="nav-select groups-sort-select">
            <span>Sort</span>
            <select value={sortBy} onChange={(event) => setSortBy(event.target.value as GroupSortBy)}>
              {SORT_OPTIONS.map((option) => (
                <option key={`group-sort-${option.value}`} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
        </div>
      }
    >
      {data ? (
        <div className="sector-summary-strip groups-summary-strip">
          <div className="sector-summary-card">
            <span>Tracked Groups</span>
            <strong>{data.total_groups}</strong>
          </div>
          <div className="sector-summary-card">
            <span>Highlighted</span>
            <strong>{data.groups.filter((group) => group.rank <= 40).length} Top 40</strong>
          </div>
          <div className="sector-summary-card">
            <span>Benchmark</span>
            <strong>{data.benchmark}</strong>
          </div>
          <div className="sector-summary-card">
            <span>Universe Filter</span>
            <strong>{`>${data.filters.min_market_cap_cr} Cr · >${data.filters.min_avg_daily_value_cr} Cr ADV`}</strong>
          </div>
        </div>
      ) : null}

      <div className="groups-grid">
        {!filteredGroups.length ? (
          <div className="empty-state">
            {loading ? "Loading group ranks..." : searchQuery.trim() ? "No groups match that symbol or company." : "No group data is available yet."}
          </div>
        ) : null}

        {filteredGroups.map((group) => {
          const expanded = expandedGroupIds.includes(group.group_id);
          const members = stocksByGroup.get(group.group_id) ?? [];
          const leaderRows = group.top_constituents
            .map((leader) => members.find((member) => member.symbol === leader.symbol))
            .filter((member): member is IndustryGroupStockItem => Boolean(member));
          const groupSymbols = group.symbols.length > 0 ? group.symbols : members.map((member) => member.symbol);
          const searchHint = searchMatches.get(group.group_id);

          return (
            <article
              key={group.group_id}
              ref={(element) => {
                groupCardRefs.current[group.group_id] = element;
              }}
              className={[
                "group-card",
                expanded ? "expanded" : "",
                group.rank <= 40 ? "is-top40" : "",
                group.rank <= 10 ? "is-top10" : "",
                focusedGroupId === group.group_id ? "is-focused" : "",
              ].filter(Boolean).join(" ")}
            >
              <div className="group-card-header">
                <button
                  type="button"
                  className="group-card-header-button"
                  onClick={() => setExpandedGroupIds((current) => (
                    current.includes(group.group_id)
                      ? current.filter((item) => item !== group.group_id)
                      : [...current, group.group_id]
                  ))}
                >
                  <div className="group-card-chip-row">
                    <span className="group-chip group-chip-rank">{group.rank_label}</span>
                    <span className="group-chip">{group.parent_sector}</span>
                    <span className={`group-chip ${group.rank <= 40 ? "group-chip-top" : ""}`}>{group.strength_bucket}</span>
                    <span className={`group-chip ${group.trend_label === "Improving" ? "group-chip-improving" : group.trend_label === "Weakening" ? "group-chip-weakening" : ""}`}>
                      {group.trend_label}
                    </span>
                  </div>
                  <h3>{group.group_name}</h3>
                  <p className="group-card-subtitle">
                    {group.stock_count} stocks · {group.leaders.slice(0, 3).join(", ") || "No leaders yet"}
                  </p>
                  {searchHint ? <p className="group-card-search-hit">{searchHint}</p> : null}
                </button>

                <div className="group-card-actions">
                  <button type="button" className="tool-pill small" onClick={() => setGridGroupId(group.group_id)}>
                    View Grid
                  </button>
                  <button
                    type="button"
                    className="tool-pill small"
                    onClick={() => setExpandedGroupIds((current) => (
                      current.includes(group.group_id)
                        ? current.filter((item) => item !== group.group_id)
                        : [...current, group.group_id]
                    ))}
                  >
                    {expanded ? "Collapse" : "Expand"}
                  </button>
                </div>
              </div>

              <div className="group-metrics-row">
                <div className="group-metric-card group-metric-card-score">
                  <span>Score</span>
                  <strong>{formatScore(group.score)}</strong>
                  <small className={group.score_change_1w !== null && group.score_change_1w >= 0 ? "positive-text" : "negative-text"}>
                    {`1W ${formatScoreChange(group.score_change_1w)}`}
                  </small>
                </div>
                <div className="group-metric-card">
                  <span>1M Rel</span>
                  <strong className={metricClass(group.relative_return_1m)}>{formatReturn(group.relative_return_1m)}</strong>
                </div>
                <div className="group-metric-card">
                  <span>3M Rel</span>
                  <strong className={metricClass(group.relative_return_3m)}>{formatReturn(group.relative_return_3m)}</strong>
                </div>
                <div className="group-metric-card">
                  <span>6M Rel</span>
                  <strong className={metricClass(group.relative_return_6m)}>{formatReturn(group.relative_return_6m)}</strong>
                </div>
                <div className="group-metric-card">
                  <span>Breadth</span>
                  <strong className={group.breadth_score >= 50 ? "positive-text" : "negative-text"}>{group.breadth_score.toFixed(1)}</strong>
                </div>
                <div className="group-metric-card">
                  <span>Trend</span>
                  <strong className={group.trend_health_score >= 50 ? "positive-text" : "negative-text"}>{group.trend_health_score.toFixed(1)}</strong>
                </div>
                <div className="group-metric-card">
                  <span>Rank Δ 1W</span>
                  <strong className={group.rank_change_1w !== null && group.rank_change_1w >= 0 ? "positive-text" : "negative-text"}>
                    {formatRankChange(group.rank_change_1w)}
                  </strong>
                </div>
              </div>

              {expanded ? (
                <div className="group-card-details">
                  <div className="group-card-detail-strip">
                    <article className="group-detail-card">
                      <span>Above 50 DMA</span>
                      <strong className={group.pct_above_50dma >= 50 ? "positive-text" : "negative-text"}>{group.pct_above_50dma.toFixed(1)}%</strong>
                    </article>
                    <article className="group-detail-card">
                      <span>Above 200 DMA</span>
                      <strong className={group.pct_above_200dma >= 50 ? "positive-text" : "negative-text"}>{group.pct_above_200dma.toFixed(1)}%</strong>
                    </article>
                    <article className="group-detail-card">
                      <span>Beat Benchmark 3M</span>
                      <strong className={group.pct_outperform_benchmark_3m >= 50 ? "positive-text" : "negative-text"}>{group.pct_outperform_benchmark_3m.toFixed(1)}%</strong>
                    </article>
                    <article className="group-detail-card">
                      <span>Beat Benchmark 6M</span>
                      <strong className={group.pct_outperform_benchmark_6m >= 50 ? "positive-text" : "negative-text"}>{group.pct_outperform_benchmark_6m.toFixed(1)}%</strong>
                    </article>
                  </div>

                  <p className="group-card-description">{group.description}</p>

                  <div className="group-symbol-chip-row">
                    {group.leaders.slice(0, 4).map((symbol) => (
                      <button key={`${group.group_id}:leader:${symbol}`} type="button" className="group-symbol-chip" onClick={() => onPickSymbolWithContext(symbol, groupSymbols)}>
                        {symbol}
                      </button>
                    ))}
                    {group.laggards.slice(0, 2).map((symbol) => (
                      <button key={`${group.group_id}:laggard:${symbol}`} type="button" className="group-symbol-chip muted" onClick={() => onPickSymbolWithContext(symbol, groupSymbols)}>
                        {symbol}
                      </button>
                    ))}
                  </div>

                  <div className="group-stock-list">
                    <div className="group-stock-head">
                      <span>Stock</span>
                      <span>Price</span>
                      <span>1D</span>
                      <span>1M</span>
                      <span>3M</span>
                      <span>6M</span>
                      <span>RS</span>
                      <span>Watch</span>
                    </div>
                    {(leaderRows.length ? leaderRows : members.slice(0, 8)).map((member) => (
                      <div key={`${group.group_id}:${member.symbol}`} className={selectedSymbol === member.symbol ? "group-stock-row active" : "group-stock-row"}>
                        <button type="button" className="group-stock-main" onClick={() => onPickSymbolWithContext(member.symbol, groupSymbols)}>
                          <span>
                            <strong>{member.symbol}</strong>
                            <small>{member.company_name} · {member.exchange}</small>
                          </span>
                        </button>
                        <span>{formatPrice(member.last_price, market)}</span>
                        <span className={metricClass(member.change_pct)}>{formatReturn(member.change_pct)}</span>
                        <span className={metricClass(member.return_1m)}>{formatReturn(member.return_1m)}</span>
                        <span className={metricClass(member.return_3m)}>{formatReturn(member.return_3m)}</span>
                        <span className={metricClass(member.return_6m)}>{formatReturn(member.return_6m)}</span>
                        <span>{member.rs_rating ?? "--"}</span>
                        <button type="button" className="tool-pill small" onClick={() => onRequestAddToWatchlist(member.symbol)}>
                          Add
                        </button>
                      </div>
                    ))}
                  </div>
                </div>
              ) : null}
            </article>
          );
        })}
      </div>

      {activeGridGroup ? (
        <Suspense fallback={null}>
          <ChartGridModal
            contextLabel="Group"
            title={activeGridGroup.group_name}
            subtitle={`${activeGridGroup.stock_count} stocks · ${activeGridGroup.rank_label} · ${activeGridGroup.strength_bucket}`}
            cards={buildMemberGridCards(activeGridMembers, gridTimeframe, market, onPickSymbolWithContext)}
            stats={buildModalStats(activeGridGroup, activeGridMembers)}
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
            onLoadSeries={loadMemberGridSeries}
            onClose={() => setGridGroupId(null)}
          />
        </Suspense>
      ) : null}
    </Panel>
  );
}