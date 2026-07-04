import { useCallback, useEffect, useState } from "react";
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import { getChartGridSeries, type ChartBar, type MarketKey } from "../lib/api";

import "./ChartLightbox.css";

export type LightboxItem = {
  symbol: string;
  name?: string;
  setup?: string;
  entry?: string;
  stop?: string;
  buy_note?: string;
  reasons?: string[];
};

type Props = {
  items: LightboxItem[];
  startIndex: number;
  market: MarketKey;
  onOpenFullChart?: (symbol: string) => void;
  onClose: () => void;
};

const TIMEFRAMES = ["3M", "6M", "1Y"] as const;
type TF = (typeof TIMEFRAMES)[number];

export function ChartLightbox({ items, startIndex, market, onOpenFullChart, onClose }: Props) {
  const [index, setIndex] = useState(startIndex);
  const [timeframe, setTimeframe] = useState<TF>("6M");
  const [cache, setCache] = useState<Record<string, ChartBar[]>>({});
  const [loading, setLoading] = useState(false);

  const clampedIndex = Math.max(0, Math.min(index, items.length - 1));
  const item = items[clampedIndex];
  const symbol = item?.symbol;
  const cacheKey = symbol ? `${symbol}:${timeframe}` : "";

  const move = useCallback(
    (delta: number) => setIndex((i) => Math.max(0, Math.min(items.length - 1, i + delta))),
    [items.length],
  );

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
      else if (e.key === "ArrowDown" || e.key === "ArrowRight") {
        e.preventDefault();
        move(1);
      } else if (e.key === "ArrowUp" || e.key === "ArrowLeft") {
        e.preventDefault();
        move(-1);
      }
    };
    window.addEventListener("keydown", handler);
    document.body.style.overflow = "hidden";
    return () => {
      window.removeEventListener("keydown", handler);
      document.body.style.overflow = "";
    };
  }, [move, onClose]);

  useEffect(() => {
    if (!symbol) return;
    let active = true;
    let settled = false;
    setLoading(true);
    getChartGridSeries([symbol], timeframe, market)
      .then((r) => {
        if (!active) return;
        const items = r.items ?? [];
        const bars = (items.find((it) => it.symbol === symbol) ?? items[0])?.bars ?? [];
        if (bars.length) {
          settled = true;
          setCache((prev) => ({ ...prev, [cacheKey]: bars }));
        }
      })
      .catch(() => { /* leave blank; user can re-navigate to retry */ })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
      void settled;
    };
    // Keyed on the symbol+timeframe only — not on `cache` — so it fetches once
    // per view and an in-flight result isn't discarded by a cache re-render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cacheKey]);

  if (!item) return null;

  const bars = cache[cacheKey] ?? [];
  const data = bars.map((b) => ({ t: b.time, c: b.close }));
  const first = data[0]?.c;
  const last = data[data.length - 1]?.c;
  const up = first !== undefined && last !== undefined ? last >= first : true;
  const color = up ? "#089981" : "#f23645";

  return (
    <div className="clb-overlay" onClick={onClose}>
      <div className="clb-modal" onClick={(e) => e.stopPropagation()}>
        <div className="clb-head">
          <div>
            <h2>
              {symbol} <span className="clb-count">{clampedIndex + 1} / {items.length}</span>
            </h2>
            <p>{item.setup ? `${item.setup} · ` : ""}{item.name ?? ""}</p>
          </div>
          <div className="clb-controls">
            <div className="clb-tf">
              {TIMEFRAMES.map((tf) => (
                <button key={tf} type="button" className={tf === timeframe ? "active" : ""} onClick={() => setTimeframe(tf)}>
                  {tf}
                </button>
              ))}
            </div>
            <button type="button" className="clb-close" onClick={onClose} aria-label="Close">×</button>
          </div>
        </div>

        <div className="clb-nav-hint">Use ↑ / ↓ (or ← / →) to move through the focus list</div>

        <div className="clb-chart">
          {data.length > 1 ? (
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={data} margin={{ top: 8, right: 8, bottom: 4, left: -12 }}>
                <defs>
                  <linearGradient id="clb-fill" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={color} stopOpacity={0.25} />
                    <stop offset="100%" stopColor={color} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid stroke="var(--line)" strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="t" hide />
                <YAxis domain={["dataMin", "dataMax"]} tick={{ fontSize: 11, fill: "var(--text-muted)" }} width={52} />
                <Tooltip contentStyle={{ fontSize: 12 }} labelFormatter={() => ""} formatter={(v) => [Number(v).toFixed(2), "Close"]} />
                <Area type="monotone" dataKey="c" stroke={color} strokeWidth={2} fill="url(#clb-fill)" />
              </AreaChart>
            </ResponsiveContainer>
          ) : (
            <div className="clb-nochart">{loading ? "Loading chart…" : "No chart data"}</div>
          )}
        </div>

        {(item.entry || item.reasons?.length) ? (
          <div className="clb-plan">
            {item.entry ? <div><em>Buy:</em> {item.entry}</div> : null}
            {item.stop ? <div><em>Stop:</em> {item.stop}</div> : null}
            {item.buy_note ? <div className="clb-muted">{item.buy_note}</div> : null}
            {item.reasons?.length ? (
              <div className="clb-tags">{item.reasons.map((r) => <span key={r} className="clb-tag">{r}</span>)}</div>
            ) : null}
          </div>
        ) : null}

        <div className="clb-footer">
          <button type="button" onClick={() => move(-1)} disabled={clampedIndex === 0}>↑ Prev</button>
          {onOpenFullChart ? (
            <button type="button" className="clb-full" onClick={() => { onClose(); onOpenFullChart(symbol); }}>
              Open full chart →
            </button>
          ) : null}
          <button type="button" onClick={() => move(1)} disabled={clampedIndex === items.length - 1}>Next ↓</button>
        </div>
      </div>
    </div>
  );
}
