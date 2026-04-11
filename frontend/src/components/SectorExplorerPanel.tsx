import { Suspense, lazy, useEffect, useMemo, useState } from "react";

import {
  getChartGrid,
  getChartGridSeries,
  type ChartBar,
  type ChartGridResponse,
  type ChartGridTimeframe,
  type MarketKey,
  type SectorCard,
  type SectorCompanyItem,
  type SectorSortBy,
  type SectorTabResponse,
} from "../lib/api";
import type { ChartGridChartStyle, ChartGridDisplayCard, ChartGridDisplayMode, ChartGridSortBy, ChartGridStat } from "./ChartGridModal";
import { Panel } from "./Panel";

const ChartGridModal = lazy(() => import("./ChartGridModal").then((module) => ({ default: module.ChartGridModal })));

type SectorExplorerPanelProps = {
  market: MarketKey;
  data: SectorTabResponse | null;
  loading?: boolean;
  sortBy: SectorSortBy;
  sortOrder: "asc" | "desc";
  onSortByChange: (sortBy: SectorSortBy) => void;
  onSortOrderChange: (sortOrder: "asc" | "desc") => void;
  onPickSymbol: (symbol: string) => void;
  onPickSymbolWithContext: (symbol: string, contextSymbols: string[]) => void;
  onRequestAddToWatchlist: (symbol: string) => void;
  onVisibleSymbolsChange: (symbols: string[]) => void;
  selectedSymbol: string | null;
};

const SORT_OPTIONS: Array<{ value: SectorSortBy; label: string }> = [
  { value: "1D", label: "1D" },
  { value: "1W", label: "1W" },
  { value: "1M", label: "1M" },
  { value: "3M", label: "3M" },
  { value: "6M", label: "6M" },
  { value: "1Y", label: "1Y" },
  { value: "2Y", label: "2Y" },
];

type CompanySortBy = "rs_rating" | SectorSortBy;
type GridTarget =
  | { type: "section" }
  | { type: "members"; name: string; groupKind: "sector" | "index" };

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

function topCompanyCount(card: SectorCard) {
  return card.sub_sectors.reduce((count, group) => count + group.companies.length, 0);
}

function companyKey(company: SectorCompanyItem) {
  return `${company.symbol}:${company.sub_sector}`;
}

function companyReturn(company: SectorCompanyItem, sortBy: SectorSortBy) {
  if (sortBy === "1D") {
    return company.return_1d;
  }
  if (sortBy === "1W") {
    return company.return_1w;
  }
  if (sortBy === "1M") {
    return company.return_1m;
  }
  if (sortBy === "3M") {
    return company.return_3m;
  }
  if (sortBy === "6M") {
    return company.return_6m;
  }
  if (sortBy === "1Y") {
    return company.return_1y;
  }
  return company.return_2y;
}

function sectorReturn(card: SectorCard, sortBy: SectorSortBy) {
  if (sortBy === "1D") {
    return card.return_1d;
  }
  if (sortBy === "1W") {
    return card.return_1w;
  }
  if (sortBy === "1M") {
    return card.return_1m;
  }
  if (sortBy === "3M") {
    return card.return_3m;
  }
  if (sortBy === "6M") {
    return card.return_6m;
  }
  if (sortBy === "1Y") {
    return card.return_1y;
  }
  return card.return_2y;
}

function getGridReturn(card: SectorCard, timeframe: ChartGridTimeframe) {
  if (timeframe === "3M") {
    return card.return_3m;
  }
  if (timeframe === "6M") {
    return card.return_6m;
  }
  if (timeframe === "1Y") {
    return card.return_1y;
  }
  return card.return_2y;
}

function getMemberGridReturn(card: ChartGridResponse["cards"][number], timeframe: ChartGridTimeframe) {
  if (timeframe === "3M") {
    return card.return_3m;
  }
  if (timeframe === "6M") {
    return card.return_6m;
  }
  if (timeframe === "1Y") {
    return card.return_1y;
  }
  return card.return_2y;
}

function sortCompanies(companies: SectorCompanyItem[], sortBy: CompanySortBy, sortOrder: "asc" | "desc") {
  const factor = sortOrder === "asc" ? 1 : -1;
  return [...companies].sort((left, right) => {
    const leftValue = sortBy === "rs_rating" ? (left.rs_rating ?? 0) : companyReturn(left, sortBy);
    const rightValue = sortBy === "rs_rating" ? (right.rs_rating ?? 0) : companyReturn(right, sortBy);

    if (leftValue === rightValue) {
      return sortOrder === "asc"
        ? left.market_cap_crore - right.market_cap_crore
        : right.market_cap_crore - left.market_cap_crore;
    }
    return factor * (leftValue - rightValue);
  });
}

function downsamplePoints(points: SectorCard["sparkline"], limit: number) {
  if (points.length <= limit) {
    return points;
  }
  const step = Math.max(points.length / limit, 1);
  const sampled = Array.from({ length: limit }, (_, index) => points[Math.min(points.length - 1, Math.floor(index * step))]);
  if (sampled[sampled.length - 1]?.time !== points[points.length - 1]?.time) {
    sampled[sampled.length - 1] = points[points.length - 1];
  }
  return sampled;
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

function scopeSparkline(points: SectorCard["sparkline"], timeframe: ChartGridTimeframe) {
  if (!points.length) {
    return [];
  }
  const latestTime = points[points.length - 1]?.time ?? Math.floor(Date.now() / 1000);
  const lookbackDays = {
    "3M": 95,
    "6M": 190,
    "1Y": 380,
    "2Y": 760,
  }[timeframe];
  const pointLimit = {
    "3M": 60,
    "6M": 72,
    "1Y": 96,
    "2Y": 120,
  }[timeframe];
  const threshold = latestTime - (lookbackDays * 24 * 60 * 60);
  const scoped = points.filter((point) => point.time >= threshold);
  return downsamplePoints(scoped.length > 1 ? scoped : points, pointLimit);
}

function buildSectionCards(
  cards: SectorCard[],
  market: MarketKey,
  timeframe: ChartGridTimeframe,
  onOpenMembers: (card: SectorCard) => void,
): ChartGridDisplayCard[] {
  return cards.map((card) => {
    const selectedReturn = getGridReturn(card, timeframe);
    const footerLabel = card.group_kind === "index" && typeof card.last_price === "number" ? "Price" : "Constituents";
    const footerValue = card.group_kind === "index" && typeof card.last_price === "number"
      ? formatPrice(card.last_price, market)
      : `${card.company_count}`;
    return {
      id: `${card.group_kind}:${card.sector}`,
      entityLabel: card.group_kind === "index" ? "Index" : "Sector",
      title: card.sector,
      subtitle: `${card.sub_sector_count} groups`,
      footerLabel,
      footerValue,
      primaryBadge: {
        label: `${timeframe} ${formatReturn(selectedReturn)}`,
        tone: selectedReturn >= 0 ? "positive" : "negative",
      },
      secondaryBadge: {
        label: `1D ${formatReturn(card.return_1d)}`,
        tone: card.return_1d >= 0 ? "positive" : "negative",
      },
      points: scopeSparkline(card.sparkline, timeframe).length > 0 ? scopeSparkline(card.sparkline, timeframe) : fallbackSparkline(selectedReturn),
      selectedReturn,
      dayReturn: card.return_1d,
      rsRating: null,
      marketCapCrore: null,
      constituents: card.company_count,
      onClick: () => onOpenMembers(card),
    };
  });
}

function buildMemberCards(
  payload: ChartGridResponse | null,
  timeframe: ChartGridTimeframe,
  onPickSymbolWithContext: (symbol: string, contextSymbols: string[]) => void,
): ChartGridDisplayCard[] {
  const contextSymbols = Array.from(new Set((payload?.cards ?? []).map((card) => card.symbol).filter(Boolean)));
  return (payload?.cards ?? []).map((card) => {
    const selectedReturn = getMemberGridReturn(card, timeframe);
    return {
      id: `${payload?.group_kind ?? "sector"}:${card.symbol}`,
      symbol: card.symbol,
      entityLabel: "Stock",
      title: card.symbol,
      subtitle: card.name,
      footerLabel: "Price",
      footerValue: card.last_price.toFixed(2),
      primaryBadge: {
        label: `${timeframe} ${formatReturn(selectedReturn)}`,
        tone: selectedReturn >= 0 ? "positive" : "negative",
      },
      secondaryBadge: {
        label: `1D ${formatReturn(card.change_pct)}`,
        tone: card.change_pct >= 0 ? "positive" : "negative",
      },
      points: card.sparkline,
      selectedReturn,
      dayReturn: card.change_pct,
      rsRating: card.rs_rating,
      marketCapCrore: card.market_cap_crore,
      constituents: null,
      onClick: () => onPickSymbolWithContext(card.symbol, contextSymbols),
    };
  });
}

function gridStatsForMembers(payload: ChartGridResponse | null, timeframe: ChartGridTimeframe): ChartGridStat[] {
  const cards = payload?.cards ?? [];
  const advances = cards.filter((card) => getMemberGridReturn(card, timeframe) > 0).length;
  const declines = cards.filter((card) => getMemberGridReturn(card, timeframe) < 0).length;
  const topWeight = cards.slice(0, 3).map((card) => card.symbol).join(", ");

  return [
    { label: "Stocks", value: `${cards.length}` },
    { label: "Advancing", value: `${advances}`, tone: advances >= declines ? "positive" : "neutral" },
    { label: "Declining", value: `${declines}`, tone: declines > advances ? "negative" : "neutral" },
    { label: "Top Weight", value: topWeight || "--" },
  ];
}

function gridStatsForSection(cards: SectorCard[], timeframe: ChartGridTimeframe): ChartGridStat[] {
  const advances = cards.filter((card) => getGridReturn(card, timeframe) > 0).length;
  const declines = cards.filter((card) => getGridReturn(card, timeframe) < 0).length;
  const leaders = cards.slice(0, 3).map((card) => card.sector).join(", ");

  return [
    { label: "Cards", value: `${cards.length}` },
    { label: "Advancing", value: `${advances}`, tone: advances >= declines ? "positive" : "neutral" },
    { label: "Declining", value: `${declines}`, tone: declines > advances ? "negative" : "neutral" },
    { label: "Leaders", value: leaders || "--" },
  ];
}

export function SectorExplorerPanel({
  market,
  data,
  loading = false,
  sortBy,
  sortOrder,
  onSortByChange,
  onSortOrderChange,
  onPickSymbol,
  onPickSymbolWithContext,
  onRequestAddToWatchlist,
  onVisibleSymbolsChange,
  selectedSymbol,
}: SectorExplorerPanelProps) {
  const [expandedSectors, setExpandedSectors] = useState<string[]>([]);
  const [expandedSubSectors, setExpandedSubSectors] = useState<string[]>([]);
  const [companySortBy, setCompanySortBy] = useState<CompanySortBy>("rs_rating");
  const [companySortOrder, setCompanySortOrder] = useState<"asc" | "desc">("desc");
  const [gridTarget, setGridTarget] = useState<GridTarget | null>(null);
  const [gridColumns, setGridColumns] = useState(4);
  const [gridRows, setGridRows] = useState(3);
  const [gridTimeframe, setGridTimeframe] = useState<ChartGridTimeframe>("6M");
  const [gridSortBy, setGridSortBy] = useState<ChartGridSortBy>("selected_return");
  const [gridChartStyle, setGridChartStyle] = useState<ChartGridChartStyle>("line");
  const [gridDisplayMode, setGridDisplayMode] = useState<ChartGridDisplayMode>("compact");
  const [memberGridData, setMemberGridData] = useState<ChartGridResponse | null>(null);
  const [memberGridLoading, setMemberGridLoading] = useState(false);
  const [memberGridError, setMemberGridError] = useState<string | null>(null);
  const [memberGridSeries, setMemberGridSeries] = useState<Record<string, ChartBar[]>>({});
  const indexCount = data?.sectors.filter((item) => item.group_kind === "index").length ?? 0;
  const groupLabel = indexCount > 0 ? "Groups" : "Sectors";
  const pageTitle = market === "us" && indexCount > 0 ? "Sectors & Indices" : "Sectors";
  const pageSubtitle = data
    ? `${data.total_sectors} ${groupLabel.toLowerCase()} ranked by ${sortBy} return`
    : market === "us"
      ? "Loading US sector and index map"
      : "Loading sector map";

  useEffect(() => {
    setExpandedSectors([]);
    setExpandedSubSectors([]);
    setGridTarget(null);
    setMemberGridData(null);
    setMemberGridError(null);
    setMemberGridSeries({});
  }, [data]);

  const toggleSector = (sector: string) => {
    setExpandedSectors((current) => (current.includes(sector) ? current.filter((item) => item !== sector) : [...current, sector]));
  };

  const toggleSubSector = (key: string) => {
    setExpandedSubSectors((current) => (current.includes(key) ? current.filter((item) => item !== key) : [...current, key]));
  };

  useEffect(() => {
    const symbols =
      data?.sectors.flatMap((sectorCard) => {
        if (!expandedSectors.includes(sectorCard.sector)) {
          return [];
        }
        return sectorCard.sub_sectors.flatMap((group) => {
          const key = `${sectorCard.sector}:${group.sub_sector}`;
          if (!expandedSubSectors.includes(key)) {
            return [];
          }
          return sortCompanies(group.companies, companySortBy, companySortOrder).map((company) => company.symbol);
        });
      }) ?? [];
    onVisibleSymbolsChange(symbols);
  }, [companySortBy, companySortOrder, data, expandedSectors, expandedSubSectors, onVisibleSymbolsChange]);

  useEffect(() => {
    if (!gridTarget || gridTarget.type !== "members") {
      return;
    }

    let active = true;
    setMemberGridLoading(true);
    setMemberGridError(null);
    void getChartGrid(gridTarget.name, gridTarget.groupKind, gridTimeframe, market)
      .then((payload) => {
        if (!active) {
          return;
        }
        setMemberGridData(payload);
      })
      .catch((error) => {
        if (!active) {
          return;
        }
        setMemberGridData(null);
        setMemberGridError(error instanceof Error ? error.message : "Failed to load chart grid.");
      })
      .finally(() => {
        if (active) {
          setMemberGridLoading(false);
        }
      });

    return () => {
      active = false;
    };
  }, [gridTarget, gridTimeframe, market]);

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

  const modalCards = useMemo(() => {
    if (!gridTarget) {
      return [];
    }
    if (gridTarget.type === "section") {
      return buildSectionCards(data?.sectors ?? [], market, gridTimeframe, (card) =>
        setGridTarget({ type: "members", name: card.sector, groupKind: card.group_kind }),
      );
    }
    return buildMemberCards(memberGridData, gridTimeframe, onPickSymbolWithContext);
  }, [data?.sectors, gridTarget, gridTimeframe, market, memberGridData, onPickSymbolWithContext]);

  const modalStats = useMemo(() => {
    if (!gridTarget) {
      return [];
    }
    return gridTarget.type === "section"
      ? gridStatsForSection(data?.sectors ?? [], gridTimeframe)
      : gridStatsForMembers(memberGridData, gridTimeframe);
  }, [data?.sectors, gridTarget, gridTimeframe, memberGridData]);

  const modalContextLabel = gridTarget?.type === "section" ? pageTitle : (memberGridData?.group_kind === "index" ? "Index" : "Sector");
  const modalTitle = gridTarget?.type === "section" ? pageTitle : (memberGridData?.name ?? gridTarget?.name ?? pageTitle);
  const modalSubtitle =
    gridTarget?.type === "section"
      ? `${data?.total_sectors ?? 0} ${groupLabel.toLowerCase()} across the current ranking window`
      : memberGridData
        ? `${memberGridData.total_items} members arranged by ${gridTimeframe}`
        : "Loading chart grid";

  return (
    <Panel
      title={pageTitle}
      subtitle={pageSubtitle}
      actions={
        <div className="sector-toolbar">
          <div className="sector-sort-pills">
            {SORT_OPTIONS.map((option) => (
              <button
                key={option.value}
                type="button"
                className={sortBy === option.value ? "tool-pill active" : "tool-pill"}
                onClick={() => onSortByChange(option.value)}
              >
                {option.label}
              </button>
            ))}
          </div>
          <button
            type="button"
            className="tool-pill"
            onClick={() => onSortOrderChange(sortOrder === "desc" ? "asc" : "desc")}
          >
            {sortOrder === "desc" ? "High to Low" : "Low to High"}
          </button>
          <label className="nav-select sector-company-sort">
            <span>Companies</span>
            <select value={companySortBy} onChange={(event) => setCompanySortBy(event.target.value as CompanySortBy)}>
              <option value="rs_rating">RS Rating</option>
              <option value="1D">1D Return</option>
              <option value="1W">1W Return</option>
              <option value="1M">1M Return</option>
              <option value="3M">3M Return</option>
              <option value="6M">6M Return</option>
              <option value="1Y">1Y Return</option>
              <option value="2Y">2Y Return</option>
            </select>
          </label>
          <button
            type="button"
            className="tool-pill"
            onClick={() => setCompanySortOrder((current) => (current === "desc" ? "asc" : "desc"))}
          >
            {companySortOrder === "desc" ? "Companies: High to Low" : "Companies: Low to High"}
          </button>
          {data?.sectors.length ? (
            <button type="button" className="tool-pill" onClick={() => setGridTarget({ type: "section" })}>
              Open Grid
            </button>
          ) : null}
        </div>
      }
      className="sector-panel"
    >
      {data ? (
        <div className="sector-summary-strip">
          <div className="sector-summary-card">
            <span>{indexCount > 0 ? "Tracked Groups" : "Tracked Sectors"}</span>
            <strong>{data.total_sectors}</strong>
          </div>
          <div className="sector-summary-card">
            <span>Sort Window</span>
            <strong>{sortBy}</strong>
          </div>
          <div className="sector-summary-card">
            <span>Ranking Mode</span>
            <strong>{sortOrder === "desc" ? "Leaders First" : "Laggards First"}</strong>
          </div>
        </div>
      ) : null}

      <div className="sector-grid">
        {!data?.sectors.length ? (
          <div className="empty-state">{loading ? `Loading ${market === "us" ? "US " : ""}sector market map...` : `No eligible ${indexCount > 0 ? "groups" : "sectors"} were found.`}</div>
        ) : null}
        {(data?.sectors ?? []).map((sectorCard) => {
          const expanded = expandedSectors.includes(sectorCard.sector);
          const groupKindLabel = sectorCard.group_kind === "index" ? "Index" : "Sector";

          return (
            <article key={sectorCard.sector} className={expanded ? "sector-card expanded" : "sector-card"}>
              <button type="button" className="sector-card-header" onClick={() => toggleSector(sectorCard.sector)}>
                <div>
                  <p className="sector-card-eyebrow">
                    {groupKindLabel} · {sectorCard.sub_sector_count} sub sectors · {topCompanyCount(sectorCard)} companies
                  </p>
                  <h3>{sectorCard.sector}</h3>
                </div>
                <div className="sector-card-main-metric">
                  <span>{sortBy} return</span>
                  <strong className={metricClass(sectorReturn(sectorCard, sortBy))}>
                    {formatReturn(sectorReturn(sectorCard, sortBy))}
                  </strong>
                </div>
                <span className="sector-card-toggle">{expanded ? "Collapse" : "Expand"}</span>
              </button>

              <div className="sector-metrics-row">
                <button
                  type="button"
                  className="tool-pill small"
                  onClick={() => setGridTarget({ type: "members", name: sectorCard.sector, groupKind: sectorCard.group_kind })}
                >
                  View Grid
                </button>
                <div className="sector-metric">
                  <span>1D</span>
                  <strong className={metricClass(sectorCard.return_1d)}>{formatReturn(sectorCard.return_1d)}</strong>
                </div>
                <div className="sector-metric">
                  <span>1W</span>
                  <strong className={metricClass(sectorCard.return_1w)}>{formatReturn(sectorCard.return_1w)}</strong>
                </div>
                <div className="sector-metric">
                  <span>1M</span>
                  <strong className={metricClass(sectorCard.return_1m)}>{formatReturn(sectorCard.return_1m)}</strong>
                </div>
                <div className="sector-metric">
                  <span>3M</span>
                  <strong className={metricClass(sectorCard.return_3m)}>{formatReturn(sectorCard.return_3m)}</strong>
                </div>
                <div className="sector-metric">
                  <span>6M</span>
                  <strong className={metricClass(sectorCard.return_6m)}>{formatReturn(sectorCard.return_6m)}</strong>
                </div>
                <div className="sector-metric">
                  <span>1Y</span>
                  <strong className={metricClass(sectorCard.return_1y)}>{formatReturn(sectorCard.return_1y)}</strong>
                </div>
                <div className="sector-metric">
                  <span>2Y</span>
                  <strong className={metricClass(sectorCard.return_2y)}>{formatReturn(sectorCard.return_2y)}</strong>
                </div>
              </div>

              {expanded ? (
                <div className="sector-details">
                  {sectorCard.sub_sectors.map((group) => (
                    <section key={`${sectorCard.sector}:${group.sub_sector}`} className="sub-sector-block">
                      <button
                        type="button"
                        className="sub-sector-header"
                        onClick={() => toggleSubSector(`${sectorCard.sector}:${group.sub_sector}`)}
                      >
                        <div>
                          <p className="sector-card-eyebrow">Sub sector</p>
                          <h4>{group.sub_sector}</h4>
                        </div>
                        <span>{group.company_count} companies</span>
                        <span className="sector-card-toggle">
                          {expandedSubSectors.includes(`${sectorCard.sector}:${group.sub_sector}`) ? "Collapse" : "Expand"}
                        </span>
                      </button>

                      {expandedSubSectors.includes(`${sectorCard.sector}:${group.sub_sector}`) ? (
                        <div className="sector-company-list">
                          <div className="sector-company-head">
                            <span>Company</span>
                            <span>Price</span>
                            <span>1D</span>
                            <span>1W</span>
                            <span>1M</span>
                            <span>3M</span>
                            <span>6M</span>
                            <span>1Y</span>
                            <span>2Y</span>
                            <span>RS</span>
                            <span>Watch</span>
                          </div>
                          {sortCompanies(group.companies, companySortBy, companySortOrder).map((company, _index, sortedCompanies) => (
                            <div
                              key={companyKey(company)}
                              className={selectedSymbol === company.symbol ? "sector-company-row active" : "sector-company-row"}
                            >
                              <button
                                type="button"
                                className="sector-company-main"
                                onClick={() => onPickSymbolWithContext(company.symbol, sortedCompanies.map((item) => item.symbol))}
                              >
                                <span>
                                  <strong>{company.symbol}</strong>
                                  <small>{company.name} · {company.exchange}</small>
                                </span>
                              </button>
                              <span>{formatPrice(company.last_price, market)}</span>
                              <span className={metricClass(company.return_1d)}>{formatReturn(company.return_1d)}</span>
                              <span className={metricClass(company.return_1w)}>{formatReturn(company.return_1w)}</span>
                              <span className={metricClass(company.return_1m)}>{formatReturn(company.return_1m)}</span>
                              <span className={metricClass(company.return_3m)}>{formatReturn(company.return_3m)}</span>
                              <span className={metricClass(company.return_6m)}>{formatReturn(company.return_6m)}</span>
                              <span className={metricClass(company.return_1y)}>{formatReturn(company.return_1y)}</span>
                              <span className={metricClass(company.return_2y)}>{formatReturn(company.return_2y)}</span>
                              <span>{company.rs_rating ?? "--"}</span>
                              <button
                                type="button"
                                className="tool-pill small"
                                onClick={() => onRequestAddToWatchlist(company.symbol)}
                              >
                                Add
                              </button>
                            </div>
                          ))}
                        </div>
                      ) : null}
                    </section>
                  ))}
                </div>
              ) : null}
            </article>
          );
        })}
      </div>

      {gridTarget ? (
        <Suspense fallback={null}>
          <ChartGridModal
            contextLabel={modalContextLabel}
            title={modalTitle}
            subtitle={modalSubtitle}
            cards={modalCards}
            stats={modalStats}
            columns={gridColumns}
            rows={gridRows}
            timeframe={gridTimeframe}
            sortBy={gridSortBy}
            chartStyle={gridChartStyle}
            displayMode={gridDisplayMode}
            loading={gridTarget.type === "members" ? memberGridLoading : false}
            error={gridTarget.type === "members" ? memberGridError : null}
            onColumnsChange={setGridColumns}
            onRowsChange={setGridRows}
            onTimeframeChange={setGridTimeframe}
            onSortByChange={setGridSortBy}
            onChartStyleChange={setGridChartStyle}
            onDisplayModeChange={setGridDisplayMode}
            onLoadSeries={gridTarget.type === "members" ? loadMemberGridSeries : undefined}
            onClose={() => setGridTarget(null)}
          />
        </Suspense>
      ) : null}
    </Panel>
  );
}
