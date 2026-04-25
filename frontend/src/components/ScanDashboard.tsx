import { useMemo } from "react";
import {
  Bar,
  BarChart,
  Cell,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Activity, Layers, PieChart as PieChartIcon, BarChart3 } from "lucide-react";

import type { IndustryGroupsResponse, ScanMatch } from "../lib/api";

import "./ScanDashboard.css";

type ScanDashboardProps = {
  items: ScanMatch[];
  groupsData: IndustryGroupsResponse | null;
};

/* ---------- Market Cap buckets (in crore) ---------- */
type CapBucket = { key: string; label: string; min: number; max: number; color: string };

const CAP_BUCKETS: CapBucket[] = [
  { key: "mega", label: "Mega (> ₹2L Cr)", min: 200000, max: Infinity, color: "#6366f1" },
  { key: "large", label: "Large (₹50K–2L Cr)", min: 50000, max: 200000, color: "#8b5cf6" },
  { key: "mid", label: "Mid (₹10K–50K Cr)", min: 10000, max: 50000, color: "#ec4899" },
  { key: "small", label: "Small (₹1K–10K Cr)", min: 1000, max: 10000, color: "#f59e0b" },
  { key: "micro", label: "Micro (< ₹1K Cr)", min: 0, max: 1000, color: "#10b981" },
];

/* ---------- Sub-components ---------- */

function Sparkline({ data, color }: { data: number[]; color: string }) {
  if (data.length === 0) {
    return <div className="sd-spark-empty">—</div>;
  }
  const series = data.map((v, i) => ({ idx: i, val: v }));
  return (
    <ResponsiveContainer width="100%" height={48}>
      <LineChart data={series} margin={{ top: 4, right: 0, bottom: 0, left: 0 }}>
        <Line
          type="monotone"
          dataKey="val"
          stroke={color}
          strokeWidth={2}
          dot={false}
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}

function MiniDonut({
  data,
  total,
  centerLabel,
}: {
  data: Array<{ name: string; value: number; color: string }>;
  total: number;
  centerLabel: string;
}) {
  if (total === 0) {
    return <div className="sd-mini-empty">—</div>;
  }
  return (
    <div className="sd-mini-donut">
      <ResponsiveContainer width="100%" height={84}>
        <PieChart>
          <Pie
            data={data}
            dataKey="value"
            nameKey="name"
            cx="50%"
            cy="50%"
            innerRadius={26}
            outerRadius={40}
            paddingAngle={2}
            isAnimationActive={false}
          >
            {data.map((entry, idx) => (
              <Cell key={idx} fill={entry.color} stroke="none" />
            ))}
          </Pie>
        </PieChart>
      </ResponsiveContainer>
      <div className="sd-mini-donut-center">
        <strong>{total}</strong>
        <small>{centerLabel}</small>
      </div>
    </div>
  );
}

/* ---------- Main component ---------- */

export function ScanDashboard({ items, groupsData }: ScanDashboardProps) {
  /* Sparkline series: cumulative change% across results (proxy "trend") */
  const sparkData = useMemo(() => {
    return items.slice(0, 40).map((m) => Number.isFinite(m.change_pct) ? m.change_pct : 0);
  }, [items]);

  /* Symbol → final_group_id mapping (from IndustryGroupsResponse.stocks) */
  const symbolToGroup = useMemo(() => {
    const map = new Map<string, { groupId: string; groupName: string; parent: string }>();
    if (!groupsData) return map;
    const groupMeta = new Map<string, { name: string; parent: string }>();
    for (const g of groupsData.groups) {
      groupMeta.set(g.group_id, { name: g.group_name, parent: g.parent_sector });
    }
    for (const s of groupsData.stocks) {
      const meta = groupMeta.get(s.final_group_id);
      if (meta) {
        map.set(s.symbol.toUpperCase(), {
          groupId: s.final_group_id,
          groupName: meta.name,
          parent: meta.parent,
        });
      }
    }
    return map;
  }, [groupsData]);

  /* Group counts (for "Groups" card and Group Distribution bar) */
  const groupCounts = useMemo(() => {
    const counts = new Map<string, { name: string; count: number; parent: string }>();
    for (const m of items) {
      const hit = symbolToGroup.get(m.symbol.toUpperCase());
      if (hit) {
        const existing = counts.get(hit.groupId);
        if (existing) existing.count += 1;
        else counts.set(hit.groupId, { name: hit.groupName, count: 1, parent: hit.parent });
      } else {
        // Fallback: use sector field directly if present
        const sec = (m.sector || "Unclassified").trim();
        const key = `__sec_${sec}`;
        const existing = counts.get(key);
        if (existing) existing.count += 1;
        else counts.set(key, { name: sec, count: 1, parent: sec });
      }
    }
    return Array.from(counts.entries())
      .map(([id, v]) => ({ id, ...v }))
      .sort((a, b) => b.count - a.count);
  }, [items, symbolToGroup]);

  const totalGroups = groupCounts.length;

  /* Mini donut for "Groups" summary card — top 5 groups + "Other" */
  const groupsDonutData = useMemo(() => {
    const palette = ["#6366f1", "#8b5cf6", "#ec4899", "#f59e0b", "#10b981"];
    const top = groupCounts.slice(0, 5).map((g, i) => ({
      name: g.name,
      value: g.count,
      color: palette[i],
    }));
    const otherCount = groupCounts.slice(5).reduce((sum, g) => sum + g.count, 0);
    if (otherCount > 0) {
      top.push({ name: "Other", value: otherCount, color: "#cbd5e1" });
    }
    return top;
  }, [groupCounts]);

  /* Market cap distribution */
  const capData = useMemo(() => {
    const counts = CAP_BUCKETS.map((b) => ({ ...b, value: 0 }));
    for (const m of items) {
      const cap = m.market_cap_crore || 0;
      const bucket = counts.find((b) => cap >= b.min && cap < b.max);
      if (bucket) bucket.value += 1;
    }
    return counts;
  }, [items]);

  const capTotal = capData.reduce((s, b) => s + b.value, 0);

  /* Group distribution — top 10 horizontal bars */
  const groupBarData = useMemo(() => {
    return groupCounts.slice(0, 10).map((g, i) => ({
      name: g.name.length > 26 ? `${g.name.slice(0, 24)}…` : g.name,
      fullName: g.name,
      count: g.count,
      // Diminishing fill intensity by rank
      fill: i === 0 ? "#6366f1" : i < 3 ? "#818cf8" : "#a5b4fc",
    }));
  }, [groupCounts]);

  /* Sparkline trend direction */
  const sparkAvg = sparkData.length
    ? sparkData.reduce((s, v) => s + v, 0) / sparkData.length
    : 0;
  const sparkColor = sparkAvg >= 0 ? "#10b981" : "#ef4444";

  return (
    <div className="sd-root">
      {/* ===== Summary Row ===== */}
      <div className="sd-row sd-row-summary">
        {/* Total Results */}
        <div className="sd-card sd-card-summary">
          <div className="sd-card-head">
            <span className="sd-card-icon sd-icon-indigo">
              <Activity size={14} strokeWidth={2.4} />
            </span>
            <span className="sd-card-label">Total Results</span>
          </div>
          <div className="sd-card-body">
            <div className="sd-stat">
              <strong>{items.length}</strong>
              <small className={sparkAvg >= 0 ? "sd-pos" : "sd-neg"}>
                Avg {sparkAvg >= 0 ? "+" : ""}
                {sparkAvg.toFixed(2)}%
              </small>
            </div>
            <div className="sd-spark">
              <Sparkline data={sparkData} color={sparkColor} />
            </div>
          </div>
        </div>

        {/* Groups */}
        <div className="sd-card sd-card-summary">
          <div className="sd-card-head">
            <span className="sd-card-icon sd-icon-violet">
              <Layers size={14} strokeWidth={2.4} />
            </span>
            <span className="sd-card-label">Groups</span>
          </div>
          <div className="sd-card-body">
            <div className="sd-stat">
              <strong>{totalGroups}</strong>
              <small>
                {groupCounts[0]
                  ? `Top: ${groupCounts[0].name}`
                  : "No groups"}
              </small>
            </div>
            <MiniDonut data={groupsDonutData} total={items.length} centerLabel="stocks" />
          </div>
        </div>
      </div>

      {/* ===== Distribution Row ===== */}
      <div className="sd-row sd-row-dist">
        {/* Market Cap Distribution */}
        <div className="sd-card sd-card-dist">
          <div className="sd-card-head">
            <span className="sd-card-icon sd-icon-pink">
              <PieChartIcon size={14} strokeWidth={2.4} />
            </span>
            <span className="sd-card-label">Market Cap Distribution</span>
          </div>
          {capTotal === 0 ? (
            <div className="sd-empty">No data</div>
          ) : (
            <div className="sd-dist-body">
              <div className="sd-donut-wrap">
                <ResponsiveContainer width="100%" height={170}>
                  <PieChart>
                    <Pie
                      data={capData}
                      dataKey="value"
                      nameKey="label"
                      cx="50%"
                      cy="50%"
                      innerRadius={48}
                      outerRadius={75}
                      paddingAngle={1.5}
                      isAnimationActive={false}
                    >
                      {capData.map((entry, idx) => (
                        <Cell key={idx} fill={entry.color} stroke="none" />
                      ))}
                    </Pie>
                    <Tooltip
                      formatter={(value: number, _name, props) => [
                        `${value} stocks`,
                        props.payload.label,
                      ]}
                      contentStyle={{
                        background: "#fff",
                        border: "1px solid #e5e7eb",
                        borderRadius: 8,
                        fontSize: 12,
                      }}
                    />
                  </PieChart>
                </ResponsiveContainer>
                <div className="sd-donut-center">
                  <strong>{capTotal}</strong>
                  <small>total</small>
                </div>
              </div>
              <ul className="sd-legend">
                {capData.map((b) => {
                  const pct = capTotal > 0 ? (b.value / capTotal) * 100 : 0;
                  return (
                    <li key={b.key}>
                      <span className="sd-legend-dot" style={{ background: b.color }} />
                      <span className="sd-legend-label">{b.label}</span>
                      <span className="sd-legend-value">
                        {b.value} <small>· {pct.toFixed(0)}%</small>
                      </span>
                    </li>
                  );
                })}
              </ul>
            </div>
          )}
        </div>

        {/* Group Distribution */}
        <div className="sd-card sd-card-dist">
          <div className="sd-card-head">
            <span className="sd-card-icon sd-icon-amber">
              <BarChart3 size={14} strokeWidth={2.4} />
            </span>
            <span className="sd-card-label">Group Distribution</span>
            <span className="sd-card-sub">Top 10 industry groups</span>
          </div>
          {groupBarData.length === 0 ? (
            <div className="sd-empty">No grouped data</div>
          ) : (
            <div className="sd-bar-wrap">
              <ResponsiveContainer width="100%" height={Math.max(170, groupBarData.length * 26)}>
                <BarChart
                  data={groupBarData}
                  layout="vertical"
                  margin={{ top: 4, right: 24, bottom: 4, left: 8 }}
                >
                  <XAxis
                    type="number"
                    hide
                    domain={[0, "dataMax"]}
                  />
                  <YAxis
                    dataKey="name"
                    type="category"
                    width={150}
                    tick={{ fill: "var(--sd-text-soft)", fontSize: 11 }}
                    tickLine={false}
                    axisLine={false}
                  />
                  <Tooltip
                    cursor={{ fill: "rgba(99, 102, 241, 0.08)" }}
                    formatter={(value: number) => [`${value} stocks`, "Count"]}
                    labelFormatter={(_l, payload) =>
                      (payload?.[0]?.payload as { fullName?: string })?.fullName ?? _l
                    }
                    contentStyle={{
                      background: "#fff",
                      border: "1px solid #e5e7eb",
                      borderRadius: 8,
                      fontSize: 12,
                    }}
                  />
                  <Bar dataKey="count" radius={[0, 6, 6, 0]} barSize={14}>
                    {groupBarData.map((entry, idx) => (
                      <Cell key={idx} fill={entry.fill} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
