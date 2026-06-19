import { useMemo } from "react";

import type { DashboardResponse, IndustryGroupsResponse, ScanMatch } from "../../lib/api";
import type { VisualMode } from "../../lib/visualMode";
import type { LocalWatchlist } from "../WatchlistsPanel";
import { ThreeSceneShell, type SceneDatum, type ThreeSceneVariant } from "./ThreeSceneShell";

import "./Tab3DHeader.css";

type Tab3DHeaderProps = {
  page: "home" | "screener" | "groups" | "watchlists" | "journal";
  visualMode: VisualMode;
  dashboard: DashboardResponse | null;
  groups: IndustryGroupsResponse | null;
  watchlists?: LocalWatchlist[];
  scannerLabel?: string;
  scannerCount?: number;
  visibleItems?: ScanMatch[];
  chartSymbol?: string | null;
};

function compact(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return "0";
  if (Math.abs(value) >= 1000) return `${(value / 1000).toFixed(1)}k`;
  return String(Math.round(value));
}

function groupData(groups: IndustryGroupsResponse | null): SceneDatum[] {
  return (groups?.groups ?? []).slice(0, 10).map((group) => ({
    label: group.group_name,
    value: Math.max(1, 120 - group.rank + Math.max(0, group.rank_change_1w ?? 0) * 3),
    change: group.return_1w,
    color: group.return_1w >= 0 ? "strength" : "weakness",
  }));
}

function moverData(items: ScanMatch[] | undefined, dashboard: DashboardResponse | null): SceneDatum[] {
  const source = items?.length ? items : dashboard?.top_gainers ?? [];
  return source.slice(0, 10).map((item) => ({
    label: item.symbol,
    value: Math.max(1, Math.abs(item.change_pct || item.score || 1)),
    change: item.change_pct,
    color: item.change_pct >= 0 ? "strength" : "weakness",
  }));
}

function watchlistData(watchlists: LocalWatchlist[] | undefined): SceneDatum[] {
  return (watchlists ?? []).flatMap((watchlist, listIndex) =>
    watchlist.symbols.slice(0, 8).map((symbol, symbolIndex) => ({
      label: symbol,
      value: 10 + symbolIndex * 5 + watchlist.symbols.length,
      color: (["liquidity", "strength", "insight", "caution"] as const)[listIndex % 4],
    })),
  );
}

function journalData(dashboard: DashboardResponse | null): SceneDatum[] {
  const breadth = dashboard?.breadth_history?.slice(-10) ?? [];
  if (breadth.length) {
    return breadth.map((day) => ({
      label: day.date,
      value: day.advances - day.declines,
      color: day.advances >= day.declines ? "strength" : "weakness",
    }));
  }
  return [
    { label: "Discipline", value: 28, color: "insight" },
    { label: "Risk", value: -14, color: "caution" },
    { label: "Edge", value: 18, color: "strength" },
  ];
}

function metaForPage(props: Tab3DHeaderProps): { variant: ThreeSceneVariant; title: string; kicker: string; stat: string; data: SceneDatum[]; positiveRatio?: number } {
  const { page, dashboard, groups, watchlists, scannerLabel, scannerCount, visibleItems } = props;
  if (page === "home") {
    const advances = dashboard?.breadth_today?.advances ?? 0;
    const declines = dashboard?.breadth_today?.declines ?? 0;
    const total = advances + declines;
    return {
      variant: "home",
      title: "Market Cockpit",
      kicker: "Breadth, leaders and flow in one glance",
      stat: total ? `${advances} adv / ${declines} dec` : `${compact(dashboard?.universe_count)} stocks`,
      data: groupData(groups).length ? groupData(groups) : moverData(undefined, dashboard),
      positiveRatio: total ? advances / total : 0.55,
    };
  }
  if (page === "screener") {
    return {
      variant: "scanner",
      title: scannerLabel || "Signal Engine",
      kicker: "Filters, momentum and volume pressure",
      stat: `${compact(scannerCount)} matches`,
      data: moverData(visibleItems, dashboard),
      positiveRatio: Math.min(0.9, Math.max(0.12, (scannerCount ?? 0) / 60)),
    };
  }
  if (page === "groups") {
    return {
      variant: "groups",
      title: "Sector City",
      kicker: "Fast groups rise higher; weak groups dim out",
      stat: `${compact(groups?.total_groups)} groups`,
      data: groupData(groups),
    };
  }
  if (page === "watchlists") {
    const count = (watchlists ?? []).reduce((sum, item) => sum + item.symbols.length, 0);
    return {
      variant: "watchlists",
      title: "Portfolio Constellation",
      kicker: "Watchlists as clusters of tradable focus",
      stat: `${compact(count)} symbols`,
      data: watchlistData(watchlists),
    };
  }
  return {
    variant: "journal",
    title: "Risk & Edge Path",
    kicker: "Discipline, drawdown and process pressure",
    stat: props.chartSymbol ? `Chart: ${props.chartSymbol}` : "Journal cockpit",
    data: journalData(dashboard),
  };
}

export function Tab3DHeader(props: Tab3DHeaderProps) {
  const meta = useMemo(
    () => metaForPage(props),
    [props.page, props.dashboard, props.groups, props.watchlists, props.scannerLabel, props.scannerCount, props.visibleItems, props.chartSymbol],
  );

  return (
    <section className={`tab-3d-header tab-3d-header-${meta.variant} visual-mode-${props.visualMode}`}>
      <div className="tab-3d-copy">
        <span className="tab-3d-eyebrow">{meta.kicker}</span>
        <strong>{meta.title}</strong>
        <small>{meta.stat}</small>
      </div>
      <ThreeSceneShell
        variant={meta.variant}
        visualMode={props.visualMode}
        data={meta.data}
        positiveRatio={meta.positiveRatio}
      />
      <div className="tab-3d-sheen" aria-hidden="true" />
    </section>
  );
}

export default Tab3DHeader;
