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

type GroupSortBy = "rank" | "score" | "1m" | "3m" | "6m" | "breadth" | "trend";
type GroupStrengthFilter = "all" | "top40" | "top10";
type TrendTimeframe = "1M" | "3M" | "6M" | "1Y";

const SORT_OPTIONS: Array<{ value: GroupSortBy; label: string }> = [
  { value: "rank", label: "Rank" },
  { value: "score", label: "Score" },
  { value: "1m", label: "1M" },
  { value: "3m", label: "3M" },
  { value: "6m", label: "6M" },
  { value: "breadth", label: "Breadth" },
  { value: "trend", label: "Trend" },
];

const TREND_TIMEFRAMES: TrendTimeframe[] = ["1M", "3M", "6M", "1Y"];

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

/* ---------- deterministic mock sparkline (no real history in response) ---------- */

function genMockSparkline(seed: number, endPct: number, length = 24): number[] {
  const out: number[] = [];
  const drift = endPct / length;
  let v = 100;
  for (let i = 0; i < length; i++) {
    const noise = Math.sin((seed + i) * 0.7) * 1.3 + Math.cos((seed + i) * 0.33) * 0.9;
    v += drift + noise * 0.25;
    out.push(v);
  }
  return out;
}

function genMockVolumeBars(seed: number, length = 12): number[] {
  const out: number[] = [];
  for (let i = 0; i < length; i++) {
    const base = 0.5 + Math.abs(Math.sin((seed + i) * 1.3)) * 0.5;
    out.push(Math.max(0.12, base));
  }
  return out;
}

/* ---------- SVG primitives ---------- */

function Sparkline({ values, color, fill, height = 34, width = 110 }: { values: number[]; color: string; fill?: string; height?: number; width?: number }) {
  if (!values || values.length < 2) {
    return (
      <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" style={{ width: "100%", height }}>
        <line x1="0" y1={height / 2} x2={width} y2={height / 2} stroke={color} strokeOpacity="0.25" strokeWidth="1.5" />
      </svg>
    );
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const step = width / (values.length - 1);
  const pts = values.map((v, i) => `${(i * step).toFixed(2)},${(height - ((v - min) / range) * (height - 4) - 2).toFixed(2)}`);
  const d = `M ${pts.join(" L ")}`;
  const areaD = `${d} L ${width},${height} L 0,${height} Z`;
  return (
    <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" style={{ width: "100%", height }} aria-hidden="true">
      {fill ? <path d={areaD} fill={fill} /> : null}
      <path d={d} stroke={color} strokeWidth="1.8" fill="none" strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

function BreadthDonut({ score, size = 40 }: { score: number; size?: number }) {
  const radius = size / 2 - 4;
  const circumference = 2 * Math.PI * radius;
  const clamped = Math.max(0, Math.min(100, score));
  const dash = (clamped / 100) * circumference;
  const gap = circumference - dash;
  const color = clamped >= 60 ? "#10b981" : clamped >= 40 ? "#f59e0b" : "#ef4444";
  const cx = size / 2;
  const cy = size / 2;
  return (
    <div style={{ position: "relative", width: size, height: size, flex: "none" }}>
      <svg viewBox={`0 0 ${size} ${size}`} width={size} height={size} style={{ transform: "rotate(-90deg)" }} aria-hidden="true">
        <circle cx={cx} cy={cy} r={radius} stroke="rgba(15,23,42,0.08)" strokeWidth="5" fill="none" />
        <circle
          cx={cx}
          cy={cy}
          r={radius}
          stroke={color}
          strokeWidth="5"
          fill="none"
          strokeDasharray={`${dash} ${gap}`}
          strokeLinecap="round"
        />
      </svg>
      <div className="gp-donut-label" style={{ color }}>{Math.round(clamped)}</div>
    </div>
  );
}

function VolumeBars({ values, seed }: { values: number[]; seed: number }) {
  const max = Math.max(...values);
  return (
    <div className="gp-volbars" aria-hidden="true">
      {values.map((v, i) => {
        const h = (v / max) * 100;
        const color = (seed + i) % 3 === 0 ? "#8b5cf6" : "#6366f1";
        return <span key={i} style={{ height: `${h}%`, background: color }} />;
      })}
    </div>
  );
}

function RankBadge({ rank }: { rank: number }) {
  if (rank === 1) return <span className="gp-rank-badge gp-rank-gold">🥇</span>;
  if (rank === 2) return <span className="gp-rank-badge gp-rank-silver">🥈</span>;
  if (rank === 3) return <span className="gp-rank-badge gp-rank-bronze">🥉</span>;
  return <span className="gp-rank-badge gp-rank-num">{rank}</span>;
}

function RsCircle({ rs }: { rs: number | null }) {
  if (rs === null || !Number.isFinite(rs)) {
    return <span className="gp-rs-circle gp-rs-muted">—</span>;
  }
  const tone = rs >= 80 ? "hi" : rs >= 60 ? "mid" : "lo";
  return <span className={`gp-rs-circle gp-rs-${tone}`}>{Math.round(rs)}</span>;
}

/* ---------- treemap layout (simple squarified) ---------- */

type TreemapNode = {
  group: IndustryGroupRankItem;
  x: number;
  y: number;
  w: number;
  h: number;
};

function layoutTreemap(
  groups: IndustryGroupRankItem[],
  width: number,
  height: number,
): TreemapNode[] {
  if (!groups.length || width <= 0 || height <= 0) return [];
  const totalValue = groups.reduce((sum, g) => sum + Math.max(1, g.stock_count), 0);
  const scale = (width * height) / totalValue;
  const items = groups.map((g) => ({ group: g, value: Math.max(1, g.stock_count) * scale }));

  const nodes: TreemapNode[] = [];
  let cursorX = 0;
  let cursorY = 0;
  let remainingW = width;
  let remainingH = height;

  let i = 0;
  while (i < items.length) {
    const horizontal = remainingW >= remainingH;
    const rowLen = horizontal ? remainingH : remainingW;
    let row: typeof items = [];
    let rowSum = 0;
    let bestRatio = Infinity;

    for (let j = i; j < items.length; j++) {
      const newSum = rowSum + items[j].value;
      const lengthSide = newSum / rowLen;
      let worst = 0;
      for (const it of [...row, items[j]]) {
        const ratio = Math.max(lengthSide / (it.value / lengthSide), (it.value / lengthSide) / lengthSide);
        worst = Math.max(worst, ratio);
      }
      if (worst > bestRatio && row.length > 0) {
        break;
      }
      row.push(items[j]);
      rowSum = newSum;
      bestRatio = worst;
    }

    const rowThickness = rowSum / rowLen;
    let offset = 0;
    for (const it of row) {
      const side = it.value / rowThickness;
      if (horizontal) {
        nodes.push({ group: it.group, x: cursorX, y: cursorY + offset, w: rowThickness, h: side });
      } else {
        nodes.push({ group: it.group, x: cursorX + offset, y: cursorY, w: side, h: rowThickness });
      }
      offset += side;
    }

    if (horizontal) {
      cursorX += rowThickness;
      remainingW -= rowThickness;
    } else {
      cursorY += rowThickness;
      remainingH -= rowThickness;
    }
    i += row.length;
  }

  return nodes;
}

function treemapColor(returnPct: number) {
  const clamp = Math.max(-10, Math.min(10, returnPct));
  if (clamp >= 0) {
    const intensity = clamp / 10;
    const a = 0.3 + intensity * 0.6;
    return `rgba(16, 185, 129, ${a.toFixed(3)})`;
  }
  const intensity = -clamp / 10;
  const a = 0.3 + intensity * 0.6;
  return `rgba(239, 68, 68, ${a.toFixed(3)})`;
}

/* ---------- Performance Trend (top 5) ---------- */

function TrendChart({ groups, timeframe }: { groups: IndustryGroupRankItem[]; timeframe: TrendTimeframe }) {
  const width = 720;
  const height = 230;
  const padL = 40;
  const padR = 16;
  const padT = 16;
  const padB = 28;
  const innerW = width - padL - padR;
  const innerH = height - padT - padB;

  const palette = ["#8b5cf6", "#10b981", "#3b82f6", "#f59e0b", "#ec4899"];

  const length = timeframe === "1M" ? 22 : timeframe === "3M" ? 66 : timeframe === "6M" ? 132 : 252;
  const series = groups.map((g, idx) => {
    const endPct = timeframe === "1M" ? g.return_1m : timeframe === "3M" ? g.return_3m : timeframe === "6M" ? g.return_6m : g.return_6m * 1.5;
    return {
      group: g,
      color: palette[idx % palette.length],
      values: genMockSparkline(idx * 13 + g.rank, endPct, length),
      endPct,
    };
  });

  const allVals = series.flatMap((s) => s.values);
  const min = Math.min(...allVals, 100);
  const max = Math.max(...allVals, 100);
  const range = max - min || 1;
  const step = innerW / (length - 1);

  return (
    <div className="gp-trend-wrap">
      <svg viewBox={`0 0 ${width} ${height}`} className="gp-trend-svg" aria-hidden="true">
        {[0, 0.25, 0.5, 0.75, 1].map((frac) => {
          const y = padT + innerH * frac;
          const label = (max - range * frac).toFixed(0);
          return (
            <g key={frac}>
              <line x1={padL} x2={width - padR} y1={y} y2={y} stroke="rgba(15,23,42,0.05)" strokeDasharray="2 4" />
              <text x={padL - 6} y={y + 3} textAnchor="end" className="gp-trend-axis">{label}</text>
            </g>
          );
        })}
        <line x1={padL} x2={width - padR} y1={padT + innerH} y2={padT + innerH} stroke="rgba(15,23,42,0.2)" />
        {series.map((s) => {
          const pts = s.values.map((v, i) => `${(padL + i * step).toFixed(1)},${(padT + innerH - ((v - min) / range) * innerH).toFixed(1)}`);
          return (
            <path
              key={s.group.group_id}
              d={`M ${pts.join(" L ")}`}
              stroke={s.color}
              strokeWidth="2"
              fill="none"
              strokeLinejoin="round"
              strokeLinecap="round"
            />
          );
        })}
      </svg>
      <div className="gp-trend-legend">
        {series.map((s) => (
          <span key={s.group.group_id} className="gp-trend-legend-item">
            <span className="gp-trend-swatch" style={{ background: s.color }} />
            <span className="gp-trend-legend-name">{s.group.group_name}</span>
            <span className={`gp-trend-legend-pct ${s.endPct >= 0 ? "gp-pos" : "gp-neg"}`}>{formatReturn(s.endPct)}</span>
          </span>
        ))}
      </div>
    </div>
  );
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
  const [trendTF, setTrendTF] = useState<TrendTimeframe>("3M");
  const [treemapMetric, setTreemapMetric] = useState<"1m" | "3m" | "6m">("1m");
  const groupRowRefs = useRef<Record<string, HTMLElement | null>>({});
  const treemapWrapRef = useRef<HTMLDivElement | null>(null);
  const [treemapSize, setTreemapSize] = useState({ w: 1000, h: 360 });

  useEffect(() => {
    setFocusedGroupId(null);
  }, [data, _market]);

  useEffect(() => {
    if (!treemapWrapRef.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const cr = entry.contentRect;
        setTreemapSize({ w: Math.max(320, cr.width), h: Math.max(260, Math.min(460, cr.width * 0.36)) });
      }
    });
    ro.observe(treemapWrapRef.current);
    return () => ro.disconnect();
  }, []);

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
      if (sortBy === "6m") return b.relative_return_6m - a.relative_return_6m;
      if (sortBy === "breadth") return b.breadth_score - a.breadth_score;
      return b.trend_health_score - a.trend_health_score;
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

  /* KPIs */
  const allGroups = data?.groups ?? [];
  const totalGroups = data?.total_groups ?? allGroups.length;
  const topPerformer = useMemo(() => {
    return [...allGroups].sort((a, b) => b.return_1m - a.return_1m)[0] ?? null;
  }, [allGroups]);
  const topLoser = useMemo(() => {
    return [...allGroups].sort((a, b) => a.return_1m - b.return_1m)[0] ?? null;
  }, [allGroups]);
  const avgChange = useMemo(() => {
    if (!allGroups.length) return 0;
    return allGroups.reduce((sum, g) => sum + g.return_1m, 0) / allGroups.length;
  }, [allGroups]);
  const advancingGroups = useMemo(() => allGroups.filter((g) => g.return_1m >= 0).length, [allGroups]);

  const treemapGroups = useMemo(() => {
    return [...allGroups].sort((a, b) => b.stock_count - a.stock_count);
  }, [allGroups]);

  const treemapNodes = useMemo(
    () => layoutTreemap(treemapGroups, treemapSize.w, treemapSize.h),
    [treemapGroups, treemapSize],
  );

  const top5Trend = useMemo(() => {
    return [...allGroups].sort((a, b) => a.rank - b.rank).slice(0, 5);
  }, [allGroups]);

  const pageSubtitle = data
    ? `${data.total_groups} custom groups · ${data.benchmark} · EOD ${data.as_of_date ?? ""}`
    : "Loading ranked industry groups";

  function handleGroupClick(group: IndustryGroupRankItem) {
    const members = stocksByGroup.get(group.group_id) ?? [];
    const first = members[0]?.symbol ?? group.symbols[0];
    if (first) {
      onPickSymbolWithContext(first, group.symbols.length ? group.symbols : members.map((m) => m.symbol));
    }
  }

  function treemapMetricValue(g: IndustryGroupRankItem) {
    if (treemapMetric === "1m") return g.return_1m;
    if (treemapMetric === "3m") return g.return_3m;
    return g.return_6m;
  }

  return (
    <Panel
      title="Groups Overview"
      subtitle={pageSubtitle}
      className="groups-panel-pro"
      actions={
        <div className="gp-toolbar">
          <form className="gp-search" onSubmit={(e) => e.preventDefault()}>
            <span className="gp-search-icon" aria-hidden="true">🔍</span>
            <input
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Search group, symbol, or company"
            />
          </form>
          <div className="gp-pill-row">
            {(["all", "top40", "top10"] as GroupStrengthFilter[]).map((f) => (
              <button
                key={f}
                type="button"
                className={`gp-pill${strengthFilter === f ? " active" : ""}`}
                onClick={() => setStrengthFilter(f)}
              >
                {f === "all" ? "All" : f === "top40" ? "Top 40" : "Top 10"}
              </button>
            ))}
          </div>
          <label className="gp-sort">
            <span>Sort</span>
            <select value={sortBy} onChange={(e) => setSortBy(e.target.value as GroupSortBy)}>
              {SORT_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>{o.label}</option>
              ))}
            </select>
          </label>
        </div>
      }
    >
      <div className="gp">
        {/* ===== KPI ROW ===== */}
        <div className="gp-kpis">
          <div className="gp-kpi gp-kpi-total">
            <div className="gp-kpi-icon">📊</div>
            <div className="gp-kpi-main">
              <div className="gp-kpi-label">Total Groups</div>
              <div className="gp-kpi-value">{totalGroups}</div>
              <Sparkline values={genMockSparkline(101, 2)} color="#8b5cf6" fill="rgba(139,92,246,0.15)" />
            </div>
          </div>

          <div className="gp-kpi gp-kpi-top">
            <div className="gp-kpi-icon">🚀</div>
            <div className="gp-kpi-main">
              <div className="gp-kpi-label">Top Performer</div>
              <div className="gp-kpi-value gp-kpi-value-sm">{topPerformer?.group_name ?? "—"}</div>
              <div className={`gp-kpi-sub ${topPerformer && topPerformer.return_1m >= 0 ? "gp-pos" : "gp-neg"}`}>
                {topPerformer ? formatReturn(topPerformer.return_1m) : "—"}
              </div>
              <Sparkline
                values={genMockSparkline(102, topPerformer?.return_1m ?? 4)}
                color="#10b981"
                fill="rgba(16,185,129,0.18)"
              />
            </div>
          </div>

          <div className="gp-kpi gp-kpi-low">
            <div className="gp-kpi-icon">📉</div>
            <div className="gp-kpi-main">
              <div className="gp-kpi-label">Top Loser</div>
              <div className="gp-kpi-value gp-kpi-value-sm">{topLoser?.group_name ?? "—"}</div>
              <div className={`gp-kpi-sub ${topLoser && topLoser.return_1m >= 0 ? "gp-pos" : "gp-neg"}`}>
                {topLoser ? formatReturn(topLoser.return_1m) : "—"}
              </div>
              <Sparkline
                values={genMockSparkline(103, topLoser?.return_1m ?? -3)}
                color="#ef4444"
                fill="rgba(239,68,68,0.16)"
              />
            </div>
          </div>

          <div className="gp-kpi gp-kpi-avg">
            <div className="gp-kpi-icon">⚖️</div>
            <div className="gp-kpi-main">
              <div className="gp-kpi-label">Avg Change (1M)</div>
              <div className={`gp-kpi-value ${avgChange >= 0 ? "gp-pos" : "gp-neg"}`}>{formatReturn(avgChange)}</div>
              <Sparkline
                values={genMockSparkline(104, avgChange)}
                color="#3b82f6"
                fill="rgba(59,130,246,0.16)"
              />
            </div>
          </div>

          <div className="gp-kpi gp-kpi-adv">
            <div className="gp-kpi-icon">✅</div>
            <div className="gp-kpi-main">
              <div className="gp-kpi-label">Advancing Groups</div>
              <div className="gp-kpi-value">{advancingGroups} / {totalGroups}</div>
              <div className="gp-kpi-sub gp-pos">
                {totalGroups > 0 ? `${Math.round((advancingGroups / totalGroups) * 100)}%` : "—"}
              </div>
              <Sparkline
                values={genMockSparkline(105, advancingGroups - totalGroups / 2)}
                color="#0ea5e9"
                fill="rgba(14,165,233,0.15)"
              />
            </div>
          </div>
        </div>

        {/* ===== TREEMAP ===== */}
        <section className="gp-card gp-card-treemap">
          <div className="gp-card-head">
            <div>
              <h3>Performance Heatmap</h3>
              <p className="gp-card-sub">All industry groups · sized by stock count · colored by return</p>
            </div>
            <div className="gp-tabs">
              {(["1m", "3m", "6m"] as const).map((m) => (
                <button
                  key={m}
                  type="button"
                  className={`gp-tab${treemapMetric === m ? " active" : ""}`}
                  onClick={() => setTreemapMetric(m)}
                >
                  {m.toUpperCase()}
                </button>
              ))}
            </div>
          </div>
          <div className="gp-treemap-wrap" ref={treemapWrapRef}>
            {allGroups.length === 0 ? (
              <div className="gp-empty">{loading ? "Loading heatmap…" : "No groups available."}</div>
            ) : (
              <svg
                viewBox={`0 0 ${treemapSize.w} ${treemapSize.h}`}
                width="100%"
                height={treemapSize.h}
                preserveAspectRatio="none"
                style={{ display: "block" }}
              >
                {treemapNodes.map((node) => {
                  const val = treemapMetricValue(node.group);
                  const color = treemapColor(val);
                  const showLabel = node.w > 60 && node.h > 30;
                  const showPct = node.w > 70 && node.h > 50;
                  return (
                    <g
                      key={node.group.group_id}
                      className="gp-treemap-cell"
                      onClick={() => handleGroupClick(node.group)}
                    >
                      <rect
                        x={node.x}
                        y={node.y}
                        width={node.w - 2}
                        height={node.h - 2}
                        rx={6}
                        fill={color}
                        stroke="rgba(255,255,255,0.6)"
                        strokeWidth="1"
                      />
                      {showLabel ? (
                        <text
                          x={node.x + 8}
                          y={node.y + 16}
                          className="gp-treemap-label"
                        >
                          {node.group.group_name.length > Math.floor(node.w / 7)
                            ? `${node.group.group_name.slice(0, Math.floor(node.w / 7))}…`
                            : node.group.group_name}
                        </text>
                      ) : null}
                      {showPct ? (
                        <text x={node.x + 8} y={node.y + 34} className="gp-treemap-pct">
                          {formatReturn(val)}
                        </text>
                      ) : null}
                    </g>
                  );
                })}
              </svg>
            )}
          </div>
          <div className="gp-treemap-legend">
            <span className="gp-treemap-legend-item"><span className="gp-swatch" style={{ background: "rgba(239,68,68,0.85)" }} />Strong Down</span>
            <span className="gp-treemap-legend-item"><span className="gp-swatch" style={{ background: "rgba(239,68,68,0.35)" }} />Weak</span>
            <span className="gp-treemap-legend-item"><span className="gp-swatch" style={{ background: "rgba(16,185,129,0.35)" }} />Gain</span>
            <span className="gp-treemap-legend-item"><span className="gp-swatch" style={{ background: "rgba(16,185,129,0.85)" }} />Strong Up</span>
          </div>
        </section>

        {/* ===== PERFORMANCE TREND (TOP 5) ===== */}
        <section className="gp-card gp-card-trend">
          <div className="gp-card-head">
            <div>
              <h3>Performance Trend</h3>
              <p className="gp-card-sub">Top 5 ranked groups · EOD history</p>
            </div>
            <div className="gp-tabs">
              {TREND_TIMEFRAMES.map((tf) => (
                <button
                  key={tf}
                  type="button"
                  className={`gp-tab${trendTF === tf ? " active" : ""}`}
                  onClick={() => setTrendTF(tf)}
                >
                  {tf}
                </button>
              ))}
            </div>
          </div>
          {top5Trend.length === 0 ? (
            <div className="gp-empty">{loading ? "Loading trend…" : "No groups available."}</div>
          ) : (
            <TrendChart groups={top5Trend} timeframe={trendTF} />
          )}
        </section>

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
                    <th>Trend</th>
                    <th>Breadth</th>
                    <th>Volume</th>
                    <th>Top 3 Stocks</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredGroups.map((group) => {
                    const members = stocksByGroup.get(group.group_id) ?? [];
                    const topThree = group.top_constituents.slice(0, 3).map((tc) => {
                      const match = members.find((m) => m.symbol === tc.symbol);
                      return {
                        symbol: tc.symbol,
                        company: tc.company_name,
                        rs: tc.rs_rating,
                        change_pct: match?.change_pct ?? tc.return_1m,
                      };
                    });
                    const groupSymbols = group.symbols.length ? group.symbols : members.map((m) => m.symbol);
                    const up1m = group.return_1m >= 0;
                    const volBars = genMockVolumeBars(group.rank + 17);

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
                        <td className="gp-cell-spark">
                          <Sparkline
                            values={genMockSparkline(group.rank * 7, group.return_1m, 30)}
                            color={up1m ? "#10b981" : "#ef4444"}
                            fill={up1m ? "rgba(16,185,129,0.18)" : "rgba(239,68,68,0.18)"}
                            height={30}
                            width={90}
                          />
                        </td>
                        <td>
                          <BreadthDonut score={group.breadth_score} />
                        </td>
                        <td>
                          <VolumeBars values={volBars} seed={group.rank} />
                        </td>
                        <td className="gp-cell-stocks">
                          <div className="gp-top3">
                            {topThree.length === 0 ? (
                              <span className="gp-muted">No leaders</span>
                            ) : topThree.map((s) => {
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
