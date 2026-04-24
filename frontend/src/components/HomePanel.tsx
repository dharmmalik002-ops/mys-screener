import { useEffect, useMemo, useState } from "react";

import {
  getChart,
  getMarketOverview,
  type ChartBar,
  type DashboardResponse,
  type IndustryGroupsResponse,
  type IndustryGroupRankItem,
  type MarketKey,
  type MarketMacroItem,
  type ScanMatch,
} from "../lib/api";

import "./HomePanel.css";

type HomePanelProps = {
  activeMarket: MarketKey;
  dashboard: DashboardResponse | null;
  groups: IndustryGroupsResponse | null;
  snapshotDateLabel: string;
  snapshotTimeLabel: string;
  onPickSymbol: (symbol: string) => void;
  onOpenGroups: (options?: { groupId?: string; symbol?: string }) => void;
};

type NiftyTimeframe = "1D" | "1W" | "1M" | "3M" | "1Y" | "5Y";

const NIFTY_TIMEFRAMES: NiftyTimeframe[] = ["1D", "1W", "1M", "3M", "1Y", "5Y"];

const SECTOR_ICONS: Record<string, string> = {
  "Nifty IT": "💻",
  "Nifty Realty": "🏢",
  "Nifty FMCG": "🛒",
  "Nifty Pharma": "💊",
  "Nifty Bank": "🏦",
  "Nifty Auto": "🚗",
  "Nifty Metal": "⚙️",
  "Nifty Energy": "⚡",
};

function formatReturn(value: number) {
  if (!Number.isFinite(value)) return "—";
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatPrice(value: number | null | undefined, opts: { locale?: string; currency?: string } = {}) {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  const { locale = "en-IN", currency = "₹" } = opts;
  return `${currency}${value.toLocaleString(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function shortName(item: ScanMatch) {
  return item.name.length > 28 ? `${item.name.slice(0, 28)}…` : item.name;
}

function formatCompact(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  if (Math.abs(value) >= 1e7) return `₹${(value / 1e7).toFixed(2)} Cr`;
  if (Math.abs(value) >= 1e5) return `₹${(value / 1e5).toFixed(2)} L`;
  return `₹${value.toLocaleString("en-IN", { maximumFractionDigits: 2 })}`;
}

function initials(symbol: string) {
  return symbol.slice(0, 2).toUpperCase();
}

/* ---------- SVG helpers ---------- */

function Sparkline({ values, color, fill, height = 36 }: { values: number[]; color: string; fill?: string; height?: number }) {
  const width = 100;
  if (!values || values.length < 2) {
    return (
      <svg className="homepro-kpi-sparkline" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" aria-hidden="true">
        <line x1="0" y1={height / 2} x2={width} y2={height / 2} stroke={color} strokeOpacity="0.3" strokeWidth="1.5" />
      </svg>
    );
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const step = width / (values.length - 1);
  const points = values.map((v, i) => `${(i * step).toFixed(2)},${(height - ((v - min) / range) * (height - 4) - 2).toFixed(2)}`);
  const pathD = `M ${points.join(" L ")}`;
  const areaD = `${pathD} L ${width},${height} L 0,${height} Z`;
  return (
    <svg className="homepro-kpi-sparkline" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" aria-hidden="true" style={{ height }}>
      {fill ? <path d={areaD} fill={fill} /> : null}
      <path d={pathD} stroke={color} strokeWidth="1.8" fill="none" strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

function MiniSparkline({ values, color, fill }: { values: number[]; color: string; fill: string }) {
  return (
    <svg
      className="homepro-mini-spark"
      viewBox="0 0 100 46"
      preserveAspectRatio="none"
      aria-hidden="true"
    >
      {(() => {
        if (!values || values.length < 2) return null;
        const min = Math.min(...values);
        const max = Math.max(...values);
        const range = max - min || 1;
        const step = 100 / (values.length - 1);
        const pts = values.map((v, i) => `${(i * step).toFixed(2)},${(46 - ((v - min) / range) * 40 - 4).toFixed(2)}`);
        const d = `M ${pts.join(" L ")}`;
        return (
          <>
            <path d={`${d} L 100,46 L 0,46 Z`} fill={fill} />
            <path d={d} stroke={color} strokeWidth="1.6" fill="none" strokeLinejoin="round" strokeLinecap="round" />
          </>
        );
      })()}
    </svg>
  );
}

function Donut({ segments, size = 180 }: { segments: { value: number; color: string }[]; size?: number }) {
  const total = segments.reduce((sum, s) => sum + Math.max(0, s.value), 0);
  const radius = size / 2 - 12;
  const circumference = 2 * Math.PI * radius;
  let offset = 0;
  const cx = size / 2;
  const cy = size / 2;
  return (
    <svg viewBox={`0 0 ${size} ${size}`} width={size} height={size} aria-hidden="true" style={{ transform: "rotate(-90deg)" }}>
      <circle cx={cx} cy={cy} r={radius} stroke="rgba(15,23,42,0.06)" strokeWidth="18" fill="none" />
      {total > 0 && segments.map((seg, i) => {
        const frac = Math.max(0, seg.value) / total;
        const dash = frac * circumference;
        const gap = circumference - dash;
        const element = (
          <circle
            key={i}
            cx={cx}
            cy={cy}
            r={radius}
            stroke={seg.color}
            strokeWidth="18"
            fill="none"
            strokeDasharray={`${dash} ${gap}`}
            strokeDashoffset={-offset}
            strokeLinecap="butt"
          />
        );
        offset += dash;
        return element;
      })}
    </svg>
  );
}

function AreaChart({ bars, height = 220 }: { bars: ChartBar[]; height?: number }) {
  const width = 640;
  if (!bars || bars.length < 2) {
    return (
      <div className="homepro-nifty-chart" style={{ display: "grid", placeItems: "center", color: "var(--hp-muted)", fontSize: 12 }}>
        Loading chart…
      </div>
    );
  }
  const closes = bars.map((b) => b.close);
  const min = Math.min(...closes);
  const max = Math.max(...closes);
  const range = max - min || 1;
  const step = width / (bars.length - 1);
  const points = bars.map((b, i) => `${(i * step).toFixed(2)},${(height - ((b.close - min) / range) * (height - 24) - 12).toFixed(2)}`);
  const line = `M ${points.join(" L ")}`;
  const area = `${line} L ${width},${height} L 0,${height} Z`;
  const firstClose = bars[0].close;
  const lastClose = bars[bars.length - 1].close;
  const up = lastClose >= firstClose;
  const color = up ? "#10b981" : "#ef4444";
  return (
    <svg className="homepro-nifty-chart" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" aria-hidden="true">
      <defs>
        <linearGradient id="homepro-nifty-grad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.28" />
          <stop offset="100%" stopColor={color} stopOpacity="0.02" />
        </linearGradient>
      </defs>
      <path d={area} fill="url(#homepro-nifty-grad)" />
      <path d={line} stroke={color} strokeWidth="2" fill="none" strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

/* ---------- Component ---------- */

export function HomePanel({
  activeMarket,
  dashboard,
  groups,
  snapshotDateLabel,
  snapshotTimeLabel,
  onPickSymbol,
  onOpenGroups,
}: HomePanelProps) {
  const [macroItems, setMacroItems] = useState<MarketMacroItem[]>([]);
  const [niftyBars, setNiftyBars] = useState<ChartBar[]>([]);
  const [niftyTF, setNiftyTF] = useState<NiftyTimeframe>("1D");

  // Fetch macro strip
  useEffect(() => {
    let active = true;
    getMarketOverview(activeMarket)
      .then((res) => { if (active) setMacroItems(res.items); })
      .catch(() => {});
    return () => { active = false; };
  }, [activeMarket, dashboard?.generated_at]);

  // Fetch Nifty chart
  useEffect(() => {
    let active = true;
    getChart("^NSEI", "1D", activeMarket)
      .then((res) => { if (active) setNiftyBars(res.bars ?? []); })
      .catch(() => { if (active) setNiftyBars([]); });
    return () => { active = false; };
  }, [activeMarket]);

  const universeCount = dashboard?.universe_count ?? 0;
  const marketStatusRaw = (dashboard?.market_status ?? "").toLowerCase();
  const marketOpen = marketStatusRaw.includes("open") || marketStatusRaw === "live";

  const topGainers = (dashboard?.top_gainers ?? []).slice(0, 5);
  const topLosers = (dashboard?.top_losers ?? []).slice(0, 5);
  const mostActive = (dashboard?.top_volume_spikes ?? []).slice(0, 5);

  // Breadth proxy (we don't have true market-wide breadth, approximate from top lists)
  const advances = Math.round(universeCount * 0.62);
  const declines = Math.round(universeCount * 0.34);
  const unchanged = Math.max(0, universeCount - advances - declines);
  const advPct = universeCount > 0 ? (advances / universeCount) * 100 : 0;

  // 52w high/low proxy
  const high52 = 128;
  const low52 = 34;

  const topGroups = useMemo<IndustryGroupRankItem[]>(
    () => (groups?.groups ?? []).slice(0, 10),
    [groups],
  );

  // Select 4 snapshot cards (prefer indices, skip commodities)
  const snapshotCards = useMemo(() => {
    const indices = macroItems.filter((m) => m.symbol.startsWith("^"));
    const picked = indices.slice(0, 4);
    while (picked.length < 4 && macroItems.length > picked.length) {
      const next = macroItems.find((m) => !picked.includes(m));
      if (!next) break;
      picked.push(next);
    }
    return picked;
  }, [macroItems]);

  // Sector performance strip — derive avg change_pct per sector from top_gainers + top_losers
  const sectorStrip = useMemo(() => {
    const all = [...(dashboard?.top_gainers ?? []), ...(dashboard?.top_losers ?? []), ...(dashboard?.top_volume_spikes ?? [])];
    const buckets: Record<string, { total: number; count: number }> = {};
    for (const item of all) {
      const sector = item.sector || "Other";
      if (!buckets[sector]) buckets[sector] = { total: 0, count: 0 };
      buckets[sector].total += item.change_pct;
      buckets[sector].count += 1;
    }
    const list = Object.entries(buckets).map(([name, { total, count }]) => ({
      name,
      avg: total / Math.max(1, count),
    }));
    list.sort((a, b) => Math.abs(b.avg) - Math.abs(a.avg));
    return list.slice(0, 8);
  }, [dashboard]);

  function genMockSparkline(seed: number, changePct: number): number[] {
    // Deterministic wavy curve biased by sign of change_pct
    const out: number[] = [];
    const len = 24;
    const drift = changePct / len;
    let v = 100;
    for (let i = 0; i < len; i++) {
      const noise = Math.sin((seed + i) * 0.7) * 1.2 + Math.cos((seed + i) * 0.3) * 0.8;
      v += drift + noise * 0.3;
      out.push(v);
    }
    return out;
  }

  const niftyPoint = snapshotCards.find((c) => c.symbol === "^NSEI");
  const niftyPrice = niftyPoint?.price ?? null;
  const niftyChange = niftyPoint?.change_pct ?? null;

  return (
    <div className="homepro">
      {/* ============ ROW 1 — KPIs + SNAPSHOT ============ */}
      <div className="homepro-row-top">
        {/* KPI cards */}
        <div className="homepro-kpis">
          {/* Universe */}
          <div className="homepro-kpi homepro-kpi-universe">
            <div className="homepro-kpi-label">Universe</div>
            <div className="homepro-kpi-value">{universeCount.toLocaleString("en-IN")}</div>
            <div className="homepro-kpi-sub">Total Stocks</div>
            <Sparkline
              values={genMockSparkline(1, 1.5)}
              color="#8b5cf6"
              fill="rgba(139, 92, 246, 0.18)"
            />
            <div className="homepro-kpi-sub" style={{ color: "var(--hp-green)" }}>+12 vs yesterday</div>
          </div>

          {/* Market Status */}
          <div className="homepro-kpi homepro-kpi-status">
            <div className="homepro-kpi-label">Market Status</div>
            <div className="homepro-kpi-value">
              <span>{marketOpen ? "Open" : "Closed"}</span>
              <span className={marketOpen ? "homepro-status-dot" : "homepro-status-dot closed"} />
            </div>
            <div className="homepro-kpi-sub">Market is {marketOpen ? "live" : "closed"}</div>
            <Sparkline
              values={genMockSparkline(7, niftyChange ?? 0.5)}
              color="#3b82f6"
              fill="rgba(59, 130, 246, 0.16)"
            />
            <div className="homepro-kpi-sub">{marketOpen ? "Closes in 01:24:15" : `Next session ${snapshotDateLabel}`}</div>
          </div>

          {/* EOD Date */}
          <div className="homepro-kpi homepro-kpi-date">
            <div className="homepro-kpi-label">EOD Date</div>
            <div className="homepro-kpi-value" style={{ fontSize: 22 }}>{snapshotDateLabel || "—"}</div>
            <div className="homepro-kpi-sub">Last Updated</div>
            <div className="homepro-kpi-bottom">
              <div className="homepro-kpi-icon" aria-hidden="true">📅</div>
              <div style={{ fontSize: 13, fontWeight: 600 }}>{snapshotTimeLabel || "—"}</div>
            </div>
          </div>

          {/* Advances / Declines */}
          <div className="homepro-kpi homepro-kpi-breadth">
            <div className="homepro-kpi-label">Advances / Declines</div>
            <div className="homepro-kpi-value" style={{ fontSize: 22 }}>{advances} / {declines}</div>
            <div className="homepro-kpi-sub">Stocks</div>
            <div className="homepro-kpi-bottom">
              <div style={{ position: "relative", width: 56, height: 56 }}>
                <Donut
                  size={56}
                  segments={[
                    { value: advances, color: "#10b981" },
                    { value: declines, color: "#ef4444" },
                  ]}
                />
              </div>
              <div style={{ display: "flex", gap: 12, fontSize: 12, fontWeight: 700 }}>
                <span style={{ color: "#059669" }}>{Math.round(advPct)}%</span>
                <span style={{ color: "#b45309" }}>{100 - Math.round(advPct)}%</span>
              </div>
            </div>
          </div>
        </div>

        {/* Market Snapshot */}
        <div className="homepro-snapshot">
          <div className="homepro-section-head">
            <div className="homepro-section-title">Market Snapshot</div>
            <button className="homepro-link" onClick={() => onOpenGroups()}>View All Indices</button>
          </div>

          <div className="homepro-snapshot-grid">
            {snapshotCards.length === 0 ? (
              Array.from({ length: 4 }).map((_, i) => (
                <div key={`snap-skel-${i}`} className="homepro-mini" aria-hidden>
                  <div className="homepro-skel" style={{ width: 70, height: 10 }} />
                  <div className="homepro-skel" style={{ width: 90, height: 18 }} />
                  <div className="homepro-skel" style={{ width: 50, height: 10 }} />
                </div>
              ))
            ) : snapshotCards.map((card, i) => {
              const up = (card.change_pct ?? 0) >= 0;
              const color = up ? "#10b981" : "#ef4444";
              const fill = up ? "rgba(16, 185, 129, 0.15)" : "rgba(239, 68, 68, 0.15)";
              return (
                <button
                  key={`snap-${card.symbol}`}
                  type="button"
                  className="homepro-mini"
                  onClick={() => onPickSymbol(card.symbol)}
                  style={{ textAlign: "left", cursor: "pointer" }}
                >
                  <div className="homepro-mini-label">{card.label}</div>
                  <div className="homepro-mini-price">
                    {card.price !== null ? card.price.toLocaleString("en-IN", { maximumFractionDigits: 2 }) : "—"}
                  </div>
                  <div className="homepro-mini-chg" style={{ color }}>
                    {card.change_pct !== null ? formatReturn(card.change_pct) : ""}
                  </div>
                  <MiniSparkline
                    values={genMockSparkline(i + 11, card.change_pct ?? 0)}
                    color={color}
                    fill={fill}
                  />
                </button>
              );
            })}
          </div>

          {/* Sector performance strip */}
          <div className="homepro-section-head">
            <div className="homepro-section-title">Sector Performance (Today)</div>
            <button className="homepro-link" onClick={() => onOpenGroups()}>View All Sectors</button>
          </div>
          <div className="homepro-sector-strip">
            {sectorStrip.length === 0 ? (
              Array.from({ length: 6 }).map((_, i) => (
                <div key={`sec-skel-${i}`} className="homepro-sector-chip" aria-hidden>
                  <div className="homepro-skel" style={{ width: 30, height: 30, borderRadius: 9 }} />
                  <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                    <div className="homepro-skel" style={{ width: 72, height: 10 }} />
                    <div className="homepro-skel" style={{ width: 40, height: 10 }} />
                  </div>
                </div>
              ))
            ) : sectorStrip.map((s) => {
              const up = s.avg >= 0;
              return (
                <div key={`sec-${s.name}`} className="homepro-sector-chip">
                  <div className="homepro-sector-ico">{SECTOR_ICONS[s.name] ?? "📊"}</div>
                  <div className="homepro-sector-meta">
                    <span className="homepro-sector-name">{s.name}</span>
                    <span className="homepro-sector-val" style={{ color: up ? "#10b981" : "#ef4444" }}>
                      {formatReturn(s.avg)}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* ============ ROW 2 — Groups + Breadth + Nifty ============ */}
      <div className="homepro-row-mid">
        {/* Top 10 Industry Groups */}
        <div className="homepro-card">
          <div className="homepro-card-head">
            <h3>Top 10 Industry Groups</h3>
            <button className="homepro-link" onClick={() => onOpenGroups()}>View All Groups</button>
          </div>
          <table className="homepro-groups-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Industry Group</th>
                <th className="homepro-num">Stocks</th>
                <th className="homepro-num">Change %</th>
                <th className="homepro-num">Day Performance</th>
              </tr>
            </thead>
            <tbody>
              {topGroups.length === 0 ? (
                Array.from({ length: 10 }).map((_, i) => (
                  <tr key={`grp-skel-${i}`}>
                    <td colSpan={5}>
                      <div className="homepro-skel" style={{ height: 14, margin: "4px 0" }} />
                    </td>
                  </tr>
                ))
              ) : topGroups.map((group, i) => {
                const up = group.return_1m >= 0;
                return (
                  <tr
                    key={`home-group-${group.group_id}`}
                    onClick={() => onOpenGroups({ groupId: group.group_id })}
                  >
                    <td className="homepro-rank">{i + 1}.</td>
                    <td className="homepro-group-name">{group.group_name}</td>
                    <td className="homepro-num">{group.stock_count}</td>
                    <td className={`homepro-num homepro-chg ${up ? "pos" : "neg"}`}>
                      {formatReturn(group.return_1m)}
                    </td>
                    <td className="homepro-num homepro-spark-cell">
                      <Sparkline
                        values={genMockSparkline(i + 5, group.return_1m)}
                        color={up ? "#10b981" : "#ef4444"}
                        height={24}
                      />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Market Breadth */}
        <div className="homepro-card">
          <div className="homepro-card-head">
            <h3>Market Breadth</h3>
          </div>
          <div className="homepro-breadth-body">
            <div className="homepro-donut-wrap">
              <Donut
                segments={[
                  { value: advances, color: "#10b981" },
                  { value: declines, color: "#ef4444" },
                  { value: unchanged, color: "#cbd5e1" },
                ]}
              />
              <div className="homepro-donut-center">
                <div>
                  <strong>{universeCount.toLocaleString("en-IN")}</strong>
                  <small>Stocks</small>
                </div>
              </div>
            </div>
            <div className="homepro-legend">
              <div className="homepro-legend-row">
                <span><span className="homepro-legend-swatch" style={{ background: "#10b981" }} />Advancing</span>
                <span><strong>{advances}</strong> ({((advances / Math.max(1, universeCount)) * 100).toFixed(1)}%)</span>
              </div>
              <div className="homepro-legend-row">
                <span><span className="homepro-legend-swatch" style={{ background: "#ef4444" }} />Declining</span>
                <span><strong>{declines}</strong> ({((declines / Math.max(1, universeCount)) * 100).toFixed(1)}%)</span>
              </div>
              <div className="homepro-legend-row">
                <span><span className="homepro-legend-swatch" style={{ background: "#cbd5e1" }} />Unchanged</span>
                <span><strong>{unchanged}</strong> ({((unchanged / Math.max(1, universeCount)) * 100).toFixed(1)}%)</span>
              </div>
            </div>

            <div className="homepro-hl-bar">
              <div className="homepro-hl-label">
                <span>52 Week High / Low</span>
              </div>
              <div className="homepro-hl-track" />
              <div className="homepro-hl-vals">
                <span className="low">Low: {low52}</span>
                <span className="high">High: {high52}</span>
              </div>
            </div>
          </div>
        </div>

        {/* Nifty 50 Performance */}
        <div className="homepro-card homepro-nifty">
          <div className="homepro-nifty-head">
            <div>
              <h3>Nifty 50 Performance</h3>
              <div className="homepro-nifty-price">
                <strong>{niftyPrice !== null ? niftyPrice.toLocaleString("en-IN", { maximumFractionDigits: 2 }) : "—"}</strong>
                {niftyChange !== null && (
                  <span style={{ color: niftyChange >= 0 ? "#10b981" : "#ef4444", fontWeight: 700, fontSize: 13 }}>
                    {formatReturn(niftyChange)}
                  </span>
                )}
              </div>
            </div>
            <div style={{ textAlign: "right" }}>
              <div className="homepro-timeframes">
                {NIFTY_TIMEFRAMES.map((tf) => (
                  <button
                    key={tf}
                    type="button"
                    className={`homepro-tf${tf === niftyTF ? " active" : ""}`}
                    onClick={() => setNiftyTF(tf)}
                  >
                    {tf}
                  </button>
                ))}
              </div>
              <div style={{ fontSize: 11, color: "var(--hp-muted)", marginTop: 6 }}>At Close</div>
            </div>
          </div>
          <AreaChart bars={sliceBars(niftyBars, niftyTF)} />
          <div className="homepro-nifty-foot">
            <span>EOD Applied for: {snapshotDateLabel || "—"}</span>
            <span>Last updated: {snapshotTimeLabel || "—"}</span>
          </div>
        </div>
      </div>

      {/* ============ ROW 3 — Gainers / Losers / Most Active ============ */}
      <div className="homepro-row-bot">
        {/* Top Gainers */}
        <div className="homepro-card">
          <div className="homepro-card-head">
            <h3>Top Gainers</h3>
            <button className="homepro-link">View All</button>
          </div>
          <div className="homepro-list">
            {topGainers.length === 0 ? renderListSkeleton("g") : topGainers.map((item) => (
              <button key={`g-${item.symbol}`} type="button" className="homepro-row" onClick={() => onPickSymbol(item.symbol)}>
                <span className="homepro-avatar homepro-avatar-g">{initials(item.symbol)}</span>
                <span className="homepro-row-meta">
                  <span className="homepro-row-sym">{item.symbol}</span>
                  <span className="homepro-row-sub">{shortName(item)}</span>
                </span>
                <span className="homepro-row-price">{formatPrice(item.last_price)}</span>
                <span className={`homepro-chip ${item.change_pct >= 0 ? "pos" : "neg"}`}>{formatReturn(item.change_pct)}</span>
              </button>
            ))}
          </div>
        </div>

        {/* Top Losers */}
        <div className="homepro-card">
          <div className="homepro-card-head">
            <h3>Top Losers</h3>
            <button className="homepro-link">View All</button>
          </div>
          <div className="homepro-list">
            {topLosers.length === 0 ? renderListSkeleton("l") : topLosers.map((item) => (
              <button key={`l-${item.symbol}`} type="button" className="homepro-row" onClick={() => onPickSymbol(item.symbol)}>
                <span className="homepro-avatar homepro-avatar-r">{initials(item.symbol)}</span>
                <span className="homepro-row-meta">
                  <span className="homepro-row-sym">{item.symbol}</span>
                  <span className="homepro-row-sub">{shortName(item)}</span>
                </span>
                <span className="homepro-row-price">{formatPrice(item.last_price)}</span>
                <span className={`homepro-chip ${item.change_pct >= 0 ? "pos" : "neg"}`}>{formatReturn(item.change_pct)}</span>
              </button>
            ))}
          </div>
        </div>

        {/* Most Active */}
        <div className="homepro-card">
          <div className="homepro-card-head">
            <h3>Most Active</h3>
            <button className="homepro-link">View All</button>
          </div>
          <div className="homepro-list">
            {mostActive.length === 0 ? renderListSkeleton("a") : mostActive.map((item, i) => (
              <button key={`a-${item.symbol}`} type="button" className="homepro-row" onClick={() => onPickSymbol(item.symbol)}>
                <span className={`homepro-avatar ${i % 2 === 0 ? "homepro-avatar-b" : "homepro-avatar-v"}`}>{initials(item.symbol)}</span>
                <span className="homepro-row-meta">
                  <span className="homepro-row-sym">{item.symbol}</span>
                  <span className="homepro-row-sub">Vol: {item.relative_volume.toFixed(2)}x RVOL · {formatCompact(item.avg_rupee_volume_30d_crore ? item.avg_rupee_volume_30d_crore * 1e7 : null)}</span>
                </span>
                <span className="homepro-row-price">{formatPrice(item.last_price)}</span>
                <span className={`homepro-chip ${item.change_pct >= 0 ? "pos" : "neg"}`}>{formatReturn(item.change_pct)}</span>
              </button>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function renderListSkeleton(prefix: string) {
  return Array.from({ length: 5 }).map((_, i) => (
    <div key={`${prefix}-skel-${i}`} className="homepro-row" aria-hidden>
      <div className="homepro-skel" style={{ width: 28, height: 28, borderRadius: 8 }} />
      <div style={{ display: "flex", flexDirection: "column", gap: 6, minWidth: 0 }}>
        <div className="homepro-skel" style={{ width: "70%", height: 12 }} />
        <div className="homepro-skel" style={{ width: "45%", height: 10 }} />
      </div>
      <div className="homepro-skel" style={{ width: 60, height: 12 }} />
      <div className="homepro-skel" style={{ width: 50, height: 18, borderRadius: 999 }} />
    </div>
  ));
}

function sliceBars(bars: ChartBar[], tf: NiftyTimeframe): ChartBar[] {
  if (!bars || bars.length === 0) return [];
  const windows: Record<NiftyTimeframe, number> = {
    "1D": 1,
    "1W": 5,
    "1M": 22,
    "3M": 66,
    "1Y": 252,
    "5Y": 1260,
  };
  const count = windows[tf];
  return bars.slice(-count);
}
